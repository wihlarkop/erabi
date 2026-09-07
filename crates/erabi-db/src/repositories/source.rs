use std::fmt;

use erabi_domain::{
    CanonicalizationPolicy, CollectionId, Source, SourceId, SourceStatus, SourceTargetType,
};
use turso::{Connection, Row, transaction::TransactionBehavior};
use url::Url;
use uuid::Uuid;

const MAX_SOURCE_NAME_CHARS: usize = 512;
const MAX_SOURCE_URL_CHARS: usize = 4_096;

/// The validated durable identity and initial classification for a new Source.
#[derive(Clone)]
pub struct NewSource {
    pub collection_id: Option<CollectionId>,
    pub name: String,
    pub original_url: String,
    pub canonical_url: Url,
    pub target_type: SourceTargetType,
}

impl fmt::Debug for NewSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewSource")
            .field("collection_id", &self.collection_id)
            .field("name", &self.name)
            .field("original_url", &safe_url_identity(&self.original_url))
            .field(
                "canonical_url",
                &safe_url_identity(self.canonical_url.as_str()),
            )
            .field("target_type", &self.target_type)
            .finish()
    }
}

/// Typed persistence failures for Source identity and classification state.
#[derive(Debug, thiserror::Error)]
pub enum SourceRepositoryError {
    #[error("the Source input is invalid: {0}")]
    InvalidInput(&'static str),
    #[error("the Source's collection was not found")]
    CollectionNotFound,
    #[error("the Source was not found")]
    NotFound,
    #[error("the persisted Source state is corrupt")]
    CorruptState,
    #[error("database operation failed")]
    Database(#[source] crate::DbError),
}

impl SourceRepositoryError {
    fn database(error: impl Into<crate::DbError>) -> Self {
        Self::Database(error.into())
    }
}

/// Persistence for durable Source target/history identity.
#[derive(Clone, Copy, Debug)]
pub struct SourceRepository<'database> {
    database: &'database crate::ErabiDatabase,
}

impl<'database> SourceRepository<'database> {
    #[must_use]
    pub const fn new(database: &'database crate::ErabiDatabase) -> Self {
        Self { database }
    }

    /// Creates a Source or reuses the unique Source for its collection and
    /// canonical URL. All matching rows are inspected; duplicate identity is
    /// never resolved by row order.
    ///
    /// # Errors
    /// Returns a typed input, collection, corruption, or database error.
    pub async fn create_or_reuse(
        &self,
        input: &NewSource,
    ) -> Result<Source, SourceRepositoryError> {
        validate_new_source(input)?;
        let mut connection = self
            .database
            .connection()
            .await
            .map_err(SourceRepositoryError::database)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(SourceRepositoryError::database)?;

        let result = create_or_reuse_in_transaction(&transaction, input).await;
        match result {
            Ok(source) => transaction
                .commit()
                .await
                .map(|()| source)
                .map_err(SourceRepositoryError::database),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Reads and validates one complete persisted Source row.
    ///
    /// # Errors
    /// Returns `NotFound` for a missing Source and `CorruptState` for any
    /// malformed durable field or broken collection relationship.
    pub async fn read(&self, id: SourceId) -> Result<Source, SourceRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(SourceRepositoryError::database)?;
        let row = connection
            .prepare(
                "SELECT id, collection_id, name, original_url, canonical_url, target_type, status FROM sources WHERE id = ?1",
            )
            .await
            .map_err(SourceRepositoryError::database)?
            .query_row([id.to_string()])
            .await
            .map_err(|error| match error {
                turso::Error::QueryReturnedNoRows => SourceRepositoryError::NotFound,
                other => SourceRepositoryError::database(other),
            })?;
        read_source(&connection, &row).await
    }

    /// Marks a Source as a direct file target without changing its identity,
    /// original URL, collection, status, or any Crawler configuration.
    /// Existing `FileAsset` state is idempotently preserved.
    ///
    /// # Errors
    /// Returns `NotFound`, `CorruptState`, or a database error.
    pub async fn mark_file_asset(&self, id: SourceId) -> Result<Source, SourceRepositoryError> {
        let mut connection = self
            .database
            .connection()
            .await
            .map_err(SourceRepositoryError::database)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(SourceRepositoryError::database)?;
        let result = mark_file_asset_in_transaction(&transaction, id).await;
        match result {
            Ok(source) => transaction
                .commit()
                .await
                .map(|()| source)
                .map_err(SourceRepositoryError::database),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }
}

async fn create_or_reuse_in_transaction(
    connection: &Connection,
    input: &NewSource,
) -> Result<Source, SourceRepositoryError> {
    if let Some(collection_id) = input.collection_id {
        ensure_collection_exists(connection, collection_id).await?;
    }

    let mut rows = if let Some(collection_id) = input.collection_id {
        connection
            .query(
                "SELECT id, collection_id, name, original_url, canonical_url, target_type, status FROM sources WHERE collection_id = ?1 AND canonical_url = ?2",
                (collection_id.to_string(), input.canonical_url.as_str()),
            )
            .await
            .map_err(SourceRepositoryError::database)?
    } else {
        connection
            .query(
                "SELECT id, collection_id, name, original_url, canonical_url, target_type, status FROM sources WHERE collection_id IS NULL AND canonical_url = ?1",
                [input.canonical_url.as_str()],
            )
            .await
            .map_err(SourceRepositoryError::database)?
    };

    let mut matches = Vec::new();
    while let Some(row) = rows.next().await.map_err(SourceRepositoryError::database)? {
        matches.push(read_source(connection, &row).await?);
    }
    if matches.len() > 1 {
        return Err(SourceRepositoryError::CorruptState);
    }
    if let Some(source) = matches.pop() {
        if source.canonical_url != input.canonical_url
            || source.collection_id != input.collection_id
        {
            return Err(SourceRepositoryError::CorruptState);
        }
        return Ok(source);
    }

    let original_url = input
        .original_url
        .parse::<Url>()
        .map_err(|_| SourceRepositoryError::InvalidInput("Source URL is invalid"))?;
    let source = Source {
        id: SourceId::new(),
        collection_id: input.collection_id,
        name: input.name.clone(),
        original_url,
        canonical_url: input.canonical_url.clone(),
        target_type: input.target_type,
        status: SourceStatus::Active,
        run_ids: Vec::new(),
        artifact_ids: Vec::new(),
    };
    connection
        .execute(
            "INSERT INTO sources (id, collection_id, name, original_url, canonical_url, target_type, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (
                source.id.to_string(),
                source
                    .collection_id
                    .map_or(turso::Value::Null, |value| turso::Value::Text(value.to_string())),
                source.name.as_str(),
                input.original_url.as_str(),
                source.canonical_url.as_str(),
                target_type_name(source.target_type),
                source_status_name(source.status),
            ),
        )
        .await
        .map_err(SourceRepositoryError::database)?;
    Ok(source)
}

async fn mark_file_asset_in_transaction(
    connection: &Connection,
    id: SourceId,
) -> Result<Source, SourceRepositoryError> {
    let row = connection
        .prepare(
            "SELECT id, collection_id, name, original_url, canonical_url, target_type, status FROM sources WHERE id = ?1",
        )
        .await
        .map_err(SourceRepositoryError::database)?
        .query_row([id.to_string()])
        .await
        .map_err(|error| match error {
            turso::Error::QueryReturnedNoRows => SourceRepositoryError::NotFound,
            other => SourceRepositoryError::database(other),
        })?;
    let source = read_source(connection, &row).await?;
    if source.target_type == SourceTargetType::WebPage {
        connection
            .execute(
                "UPDATE sources SET target_type = 'FILE_ASSET' WHERE id = ?1",
                [id.to_string()],
            )
            .await
            .map_err(SourceRepositoryError::database)?;
        return Ok(Source {
            target_type: SourceTargetType::FileAsset,
            ..source
        });
    }
    Ok(source)
}

async fn ensure_collection_exists(
    connection: &Connection,
    collection_id: CollectionId,
) -> Result<(), SourceRepositoryError> {
    let row = connection
        .prepare("SELECT id FROM collections WHERE id = ?1")
        .await
        .map_err(SourceRepositoryError::database)?
        .query_row([collection_id.to_string()])
        .await
        .map_err(|error| match error {
            turso::Error::QueryReturnedNoRows => SourceRepositoryError::CollectionNotFound,
            other => SourceRepositoryError::database(other),
        })?;
    let stored_id: String = row
        .get(0)
        .map_err(|_| SourceRepositoryError::CorruptState)?;
    if stored_id != collection_id.to_string() {
        return Err(SourceRepositoryError::CorruptState);
    }
    Ok(())
}

async fn read_source(connection: &Connection, row: &Row) -> Result<Source, SourceRepositoryError> {
    let id_text: String = row
        .get(0)
        .map_err(|_| SourceRepositoryError::CorruptState)?;
    let collection_text: Option<String> = row
        .get(1)
        .map_err(|_| SourceRepositoryError::CorruptState)?;
    let name: String = row
        .get(2)
        .map_err(|_| SourceRepositoryError::CorruptState)?;
    let original_text: String = row
        .get(3)
        .map_err(|_| SourceRepositoryError::CorruptState)?;
    let canonical_text: String = row
        .get(4)
        .map_err(|_| SourceRepositoryError::CorruptState)?;
    let target_type_text: String = row
        .get(5)
        .map_err(|_| SourceRepositoryError::CorruptState)?;
    let status_text: String = row
        .get(6)
        .map_err(|_| SourceRepositoryError::CorruptState)?;

    let id = parse_uuid_v7::<SourceId>(&id_text).ok_or(SourceRepositoryError::CorruptState)?;
    let collection_id = match collection_text.as_deref() {
        None => None,
        Some(value) => {
            Some(parse_uuid_v7::<CollectionId>(value).ok_or(SourceRepositoryError::CorruptState)?)
        }
    };
    validate_name(&name).map_err(|_| SourceRepositoryError::CorruptState)?;
    if original_text.chars().count() > MAX_SOURCE_URL_CHARS
        || canonical_text.chars().count() > MAX_SOURCE_URL_CHARS
    {
        return Err(SourceRepositoryError::CorruptState);
    }
    let original_url = original_text
        .parse::<Url>()
        .map_err(|_| SourceRepositoryError::CorruptState)?;
    let canonical_url = canonical_text
        .parse::<Url>()
        .map_err(|_| SourceRepositoryError::CorruptState)?;
    if !original_url.username().is_empty()
        || original_url.password().is_some()
        || original_url.fragment().is_some()
    {
        return Err(SourceRepositoryError::CorruptState);
    }
    let expected_canonical = CanonicalizationPolicy::default()
        .canonicalize(&original_text)
        .map_err(|_| SourceRepositoryError::CorruptState)?
        .canonical_url;
    if expected_canonical != canonical_url {
        return Err(SourceRepositoryError::CorruptState);
    }
    if let Some(collection_id) = collection_id {
        ensure_collection_exists(connection, collection_id)
            .await
            .map_err(|error| match error {
                SourceRepositoryError::CollectionNotFound => SourceRepositoryError::CorruptState,
                other => other,
            })?;
    }
    let target_type =
        parse_target_type(&target_type_text).ok_or(SourceRepositoryError::CorruptState)?;
    let status = parse_source_status(&status_text).ok_or(SourceRepositoryError::CorruptState)?;

    Ok(Source {
        id,
        collection_id,
        name,
        original_url,
        canonical_url,
        target_type,
        status,
        run_ids: Vec::new(),
        artifact_ids: Vec::new(),
    })
}

fn validate_new_source(input: &NewSource) -> Result<(), SourceRepositoryError> {
    validate_name(&input.name)?;
    let original_url = input
        .original_url
        .parse::<Url>()
        .map_err(|_| SourceRepositoryError::InvalidInput("Source URL is invalid"))?;
    if !original_url.username().is_empty()
        || original_url.password().is_some()
        || original_url.fragment().is_some()
    {
        return Err(SourceRepositoryError::InvalidInput(
            "Source URL credentials and fragments are not allowed",
        ));
    }
    if input.original_url.chars().count() > MAX_SOURCE_URL_CHARS
        || input.canonical_url.as_str().chars().count() > MAX_SOURCE_URL_CHARS
    {
        return Err(SourceRepositoryError::InvalidInput("URL is too long"));
    }
    let expected_canonical = CanonicalizationPolicy::default()
        .canonicalize(&input.original_url)
        .map_err(|_| SourceRepositoryError::InvalidInput("URL canonicalization is invalid"))?
        .canonical_url;
    if expected_canonical != input.canonical_url {
        return Err(SourceRepositoryError::InvalidInput(
            "canonical URL does not match the original URL",
        ));
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<(), SourceRepositoryError> {
    if name.trim().is_empty()
        || name.chars().count() > MAX_SOURCE_NAME_CHARS
        || name.chars().any(char::is_control)
    {
        return Err(SourceRepositoryError::InvalidInput(
            "Source name is invalid",
        ));
    }
    Ok(())
}

fn safe_url_identity(value: &str) -> String {
    let Ok(mut url) = Url::parse(value) else {
        return "<invalid-url>".to_owned();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

fn parse_uuid_v7<T>(value: &str) -> Option<T>
where
    T: FromUuidV7,
{
    T::from_uuid(Uuid::parse_str(value).ok()?)
}

trait FromUuidV7: Sized {
    fn from_uuid(value: Uuid) -> Option<Self>;
}

impl FromUuidV7 for SourceId {
    fn from_uuid(value: Uuid) -> Option<Self> {
        Self::from_uuid(value)
    }
}

impl FromUuidV7 for CollectionId {
    fn from_uuid(value: Uuid) -> Option<Self> {
        Self::from_uuid(value)
    }
}

fn target_type_name(target_type: SourceTargetType) -> &'static str {
    match target_type {
        SourceTargetType::WebPage => "WEB_PAGE",
        SourceTargetType::FileAsset => "FILE_ASSET",
    }
}

fn parse_target_type(value: &str) -> Option<SourceTargetType> {
    match value {
        "WEB_PAGE" => Some(SourceTargetType::WebPage),
        "FILE_ASSET" => Some(SourceTargetType::FileAsset),
        _ => None,
    }
}

fn source_status_name(status: SourceStatus) -> &'static str {
    match status {
        SourceStatus::Active => "ACTIVE",
        SourceStatus::Archived => "ARCHIVED",
        SourceStatus::Trashed => "TRASHED",
    }
}

fn parse_source_status(value: &str) -> Option<SourceStatus> {
    match value {
        "ACTIVE" => Some(SourceStatus::Active),
        "ARCHIVED" => Some(SourceStatus::Archived),
        "TRASHED" => Some(SourceStatus::Trashed),
        _ => None,
    }
}
