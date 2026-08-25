use std::collections::{BTreeMap, BTreeSet};

use erabi_domain::{
    Crawler, CrawlerId, CrawlerVersion, CrawlerVersionId, CrawlerVersionState,
    OperationalOverrides, canonical_sha256,
};
use serde::Serialize;
use serde_json::{Map, Value};
use turso::{Connection, Row, transaction::TransactionBehavior};
use uuid::Uuid;

use crate::{DbError, ErabiDatabase};

macro_rules! finish_transaction {
    ($transaction:expr, $result:expr) => {{
        match $result {
            Ok(value) => $transaction
                .commit()
                .await
                .map_err(CrawlerRepositoryError::database)
                .map(|()| value),
            Err(error) => {
                let _ = $transaction.rollback().await;
                Err(error)
            }
        }
    }};
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CrawlerPointers {
    pub active_published_version_id: Option<String>,
    pub active_draft_version_id: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum CrawlerRepositoryError {
    #[error("Crawler was not found")]
    CrawlerNotFound,
    #[error("CrawlerVersion was not found")]
    CrawlerVersionNotFound,
    #[error("CrawlerVersion does not belong to the requested Crawler")]
    VersionNotOwnedByCrawler,
    #[error("Crawler already has an active Draft")]
    ActiveDraftExists,
    #[error("CrawlerVersion is not a Draft")]
    VersionNotDraft,
    #[error("CrawlerVersion is not Published")]
    VersionNotPublished,
    #[error("Published CrawlerVersion is immutable")]
    PublishedVersionImmutable,
    #[error("CrawlerVersion lifecycle transition is invalid")]
    InvalidLifecycleTransition,
    #[error("CrawlerVersion lifecycle transition conflicted with another request")]
    ConcurrentVersionTransition,
    #[error("durable Crawler state is invalid")]
    CorruptState,
    #[error("database operation failed")]
    Database(#[source] DbError),
}

impl CrawlerRepositoryError {
    fn database(error: impl Into<DbError>) -> Self {
        Self::Database(error.into())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct CrawlerAuditMetadata {
    pub actor: Option<String>,
    pub occurred_at: Option<String>,
    pub config_hash: Option<String>,
    pub warning_summary: Vec<String>,
    pub base_version_id: Option<CrawlerVersionId>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CrawlerVersionRecord {
    pub version: CrawlerVersion,
    pub audit: CrawlerAuditMetadata,
}

#[derive(Clone, Copy, Debug)]
pub struct CrawlerRepository<'database> {
    database: &'database ErabiDatabase,
}

#[allow(clippy::missing_errors_doc)]
impl<'database> CrawlerRepository<'database> {
    #[must_use]
    pub const fn new(database: &'database ErabiDatabase) -> Self {
        Self { database }
    }

    fn database(error: impl Into<DbError>) -> CrawlerRepositoryError {
        CrawlerRepositoryError::database(error)
    }

    pub async fn create(&self, crawler: &Crawler) -> Result<(), DbError> {
        let defaults = serialize(crawler.operational_defaults())?;
        let connection = self.database.connection().await?;
        connection
            .execute(
                "INSERT INTO crawlers (id, name, collection_id, operational_defaults_json, active_published_version_id, active_draft_version_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                (
                    crawler.id().to_string(),
                    crawler.name.clone(),
                    turso::Value::Null,
                    defaults,
                    turso::Value::Null,
                    turso::Value::Null,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<Crawler>, CrawlerRepositoryError> {
        let connection = self.database.connection().await.map_err(Self::database)?;
        let mut rows = connection
            .query(
                "SELECT id, name, collection_id, operational_defaults_json, active_published_version_id, active_draft_version_id FROM crawlers ORDER BY name COLLATE BINARY, id",
                (),
            )
            .await
            .map_err(Self::database)?;
        let mut crawlers = Vec::new();
        while let Some(row) = rows.next().await.map_err(Self::database)? {
            let crawler = crawler_from_row(&row)?;
            ensure_pointer_consistency(&connection, crawler.id()).await?;
            crawlers.push(crawler);
        }
        Ok(crawlers)
    }

    pub async fn get(&self, crawler_id: CrawlerId) -> Result<Crawler, CrawlerRepositoryError> {
        let connection = self.database.connection().await.map_err(Self::database)?;
        let mut rows = connection
            .query(
                "SELECT id, name, collection_id, operational_defaults_json, active_published_version_id, active_draft_version_id FROM crawlers WHERE id = ?1",
                [crawler_id.to_string()],
            )
            .await
            .map_err(Self::database)?;
        let Some(row) = rows.next().await.map_err(Self::database)? else {
            return Err(CrawlerRepositoryError::CrawlerNotFound);
        };
        let crawler = crawler_from_row(&row)?;
        ensure_pointer_consistency(&connection, crawler.id()).await?;
        Ok(crawler)
    }

    pub async fn list_versions(
        &self,
        crawler_id: CrawlerId,
    ) -> Result<Vec<CrawlerVersionRecord>, CrawlerRepositoryError> {
        let connection = self.database.connection().await.map_err(Self::database)?;
        ensure_crawler_exists(&connection, crawler_id).await?;
        ensure_pointer_consistency(&connection, crawler_id).await?;
        let mut rows = connection
            .query(
                "SELECT id, crawler_id, state, semantic_configuration_json FROM crawler_versions WHERE crawler_id = ?1 ORDER BY id",
                [crawler_id.to_string()],
            )
            .await
            .map_err(Self::database)?;
        let mut versions = Vec::new();
        while let Some(row) = rows.next().await.map_err(Self::database)? {
            let version = version_from_row(&row)?;
            let audit = audit_metadata(&connection, version.id()).await?;
            versions.push(CrawlerVersionRecord { version, audit });
        }
        Ok(versions)
    }

    pub async fn version(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
    ) -> Result<CrawlerVersionRecord, CrawlerRepositoryError> {
        let connection = self.database.connection().await.map_err(Self::database)?;
        ensure_crawler_exists(&connection, crawler_id).await?;
        ensure_pointer_consistency(&connection, crawler_id).await?;
        let mut rows = connection
            .query(
                "SELECT id, crawler_id, state, semantic_configuration_json FROM crawler_versions WHERE id = ?1",
                [version_id.to_string()],
            )
            .await
            .map_err(Self::database)?;
        let Some(row) = rows.next().await.map_err(Self::database)? else {
            return Err(CrawlerRepositoryError::CrawlerVersionNotFound);
        };
        let version = version_from_row(&row)?;
        if version.crawler_id() != crawler_id {
            return Err(CrawlerRepositoryError::VersionNotOwnedByCrawler);
        }
        let audit = audit_metadata(&connection, version.id()).await?;
        Ok(CrawlerVersionRecord { version, audit })
    }

    pub async fn create_draft(
        &self,
        crawler_id: CrawlerId,
        actor: &str,
        occurred_at: &str,
    ) -> Result<CrawlerVersion, CrawlerRepositoryError> {
        let version = CrawlerVersion::draft(crawler_id);
        let mut connection = self.database.connection().await.map_err(Self::database)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(Self::database)?;
        let result =
            insert_draft_in_transaction(&transaction, &version, None, actor, occurred_at).await;
        finish_transaction!(transaction, result).map(|()| version)
    }

    pub async fn create_draft_from_published(
        &self,
        crawler_id: CrawlerId,
        source_version_id: CrawlerVersionId,
        actor: &str,
        occurred_at: &str,
    ) -> Result<CrawlerVersion, CrawlerRepositoryError> {
        let mut connection = self.database.connection().await.map_err(Self::database)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(Self::database)?;
        let result = clone_draft_in_transaction(
            &transaction,
            crawler_id,
            source_version_id,
            actor,
            occurred_at,
        )
        .await;
        finish_transaction!(transaction, result)
    }

    pub async fn save_draft(
        &self,
        version: &CrawlerVersion,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), DbError> {
        match self.save_draft_typed(version, actor, occurred_at).await {
            Err(CrawlerRepositoryError::CrawlerNotFound) => {
                // Preserve the original compatibility seam's FK behavior for
                // callers that use `save_draft` directly. The typed authoring
                // APIs above reject this case before attempting a write.
                let configuration = serialize(version)?;
                let connection = self.database.connection().await?;
                connection
                    .execute(
                        "INSERT INTO crawler_versions (id, crawler_id, state, semantic_configuration_json) VALUES (?1, ?2, 'DRAFT', ?3)",
                        (version.id().to_string(), version.crawler_id().to_string(), configuration),
                    )
                    .await
                    .map(|_| ())
                    .map_err(DbError::from)
            }
            result => result.map_err(repository_error_as_db),
        }
    }

    async fn save_draft_typed(
        &self,
        version: &CrawlerVersion,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), CrawlerRepositoryError> {
        if version.state() != CrawlerVersionState::Draft {
            return Err(CrawlerRepositoryError::PublishedVersionImmutable);
        }
        let configuration = serialize(version).map_err(CrawlerRepositoryError::database)?;
        let mut connection = self.database.connection().await.map_err(Self::database)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(Self::database)?;
        let result = save_draft_in_transaction(
            &transaction,
            version,
            configuration.as_str(),
            actor,
            occurred_at,
        )
        .await;
        finish_transaction!(transaction, result)
    }

    pub async fn publish(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        actor: &str,
        occurred_at: &str,
    ) -> Result<CrawlerVersionRecord, CrawlerRepositoryError> {
        let mut connection = self.database.connection().await.map_err(Self::database)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(Self::database)?;
        let result =
            publish_in_transaction(&transaction, crawler_id, version_id, actor, occurred_at).await;
        finish_transaction!(transaction, result)
    }

    pub async fn publish_and_activate(
        &self,
        crawler: &Crawler,
        version: &CrawlerVersion,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), DbError> {
        if version.crawler_id() != crawler.id() || version.state() != CrawlerVersionState::Published
        {
            return Err(DbError::Invariant(
                "only a published version belonging to the Crawler can be activated".into(),
            ));
        }
        self.publish(crawler.id(), version.id(), actor, occurred_at)
            .await
            .map(|_| ())
            .map_err(repository_error_as_db)
    }

    pub async fn reactivate_published_typed(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), CrawlerRepositoryError> {
        let mut connection = self.database.connection().await.map_err(Self::database)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(Self::database)?;
        let result =
            reactivate_in_transaction(&transaction, crawler_id, version_id, actor, occurred_at)
                .await;
        finish_transaction!(transaction, result)
    }

    pub async fn reactivate_published(
        &self,
        crawler: &Crawler,
        version: &CrawlerVersion,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), DbError> {
        if version.state() != CrawlerVersionState::Published || version.crawler_id() != crawler.id()
        {
            return Err(DbError::Invariant(
                "only a published version belonging to the Crawler can be reactivated".into(),
            ));
        }
        self.reactivate_published_typed(crawler.id(), version.id(), actor, occurred_at)
            .await
            .map_err(repository_error_as_db)
    }

    pub async fn pointers(&self, crawler: &Crawler) -> Result<CrawlerPointers, DbError> {
        let connection = self.database.connection().await?;
        let row = connection
            .prepare(
                "SELECT active_published_version_id, active_draft_version_id FROM crawlers WHERE id = ?1",
            )
            .await?
            .query_row([crawler.id().to_string()])
            .await?;
        Ok(CrawlerPointers {
            active_published_version_id: row.get(0)?,
            active_draft_version_id: row.get(1)?,
        })
    }

    pub async fn audit_event_count(&self, entity_id: &str) -> Result<i64, DbError> {
        let connection = self.database.connection().await?;
        let row = connection
            .prepare("SELECT COUNT(*) FROM audit_events WHERE entity_id = ?1")
            .await?
            .query_row([entity_id])
            .await?;
        Ok(row.get(0)?)
    }

    pub async fn configuration_hash(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
    ) -> Result<String, CrawlerRepositoryError> {
        let connection = self.database.connection().await.map_err(Self::database)?;
        let record = self.version(crawler_id, version_id).await?;
        semantic_hash(&connection, &record.version).await
    }
}

async fn insert_draft_in_transaction(
    connection: &Connection,
    version: &CrawlerVersion,
    base_version_id: Option<CrawlerVersionId>,
    actor: &str,
    occurred_at: &str,
) -> Result<(), CrawlerRepositoryError> {
    let crawler_id = version.crawler_id().to_string();
    let mut pointers = connection
        .query(
            "SELECT active_draft_version_id FROM crawlers WHERE id = ?1",
            [crawler_id.as_str()],
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    let Some(pointer_row) = pointers
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?
    else {
        return Err(CrawlerRepositoryError::CrawlerNotFound);
    };
    let active_draft: Option<String> = pointer_row
        .get(0)
        .map_err(CrawlerRepositoryError::database)?;
    if active_draft.is_some() {
        return Err(CrawlerRepositoryError::ActiveDraftExists);
    }

    let configuration = serialize(version).map_err(CrawlerRepositoryError::database)?;
    connection
        .execute(
            "INSERT INTO crawler_versions (id, crawler_id, state, semantic_configuration_json) VALUES (?1, ?2, 'DRAFT', ?3)",
            (version.id().to_string(), crawler_id.as_str(), configuration),
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    sync_seed_rows(connection, version).await?;
    let activated = connection
        .execute(
            "UPDATE crawlers SET active_draft_version_id = ?1 WHERE id = ?2 AND active_draft_version_id IS NULL",
            (version.id().to_string(), crawler_id.as_str()),
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    if activated != 1 {
        return Err(CrawlerRepositoryError::ConcurrentVersionTransition);
    }
    insert_audit_event(
        connection,
        format!("draft:{}:{}", version.id(), occurred_at),
        "CRAWLER_DRAFT_CREATED",
        actor,
        occurred_at,
        &version.id().to_string(),
        audit_payload(version.id(), base_version_id, None),
    )
    .await
}

async fn save_draft_in_transaction(
    connection: &Connection,
    version: &CrawlerVersion,
    configuration: &str,
    actor: &str,
    occurred_at: &str,
) -> Result<(), CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT crawler_id, state FROM crawler_versions WHERE id = ?1",
            [version.id().to_string()],
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    let existing = rows
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?;
    if let Some(row) = existing {
        let owner: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        let state: String = row.get(1).map_err(CrawlerRepositoryError::database)?;
        if owner != version.crawler_id().to_string() {
            return Err(CrawlerRepositoryError::VersionNotOwnedByCrawler);
        }
        if state == "PUBLISHED" {
            return Err(CrawlerRepositoryError::PublishedVersionImmutable);
        }
        if state != "DRAFT" {
            return Err(CrawlerRepositoryError::InvalidLifecycleTransition);
        }
        let pointer = active_draft_for(connection, version.crawler_id()).await?;
        if pointer.as_deref() != Some(version.id().to_string().as_str()) {
            return Err(CrawlerRepositoryError::InvalidLifecycleTransition);
        }
        connection
            .execute(
                "UPDATE crawler_versions SET semantic_configuration_json = ?1 WHERE id = ?2 AND state = 'DRAFT'",
                (configuration, version.id().to_string()),
            )
            .await
            .map_err(CrawlerRepositoryError::database)?;
        sync_seed_rows(connection, version).await?;
        return Ok(());
    }

    insert_draft_in_transaction(connection, version, None, actor, occurred_at).await
}

#[allow(clippy::too_many_lines)]
async fn clone_draft_in_transaction(
    connection: &Connection,
    crawler_id: CrawlerId,
    source_version_id: CrawlerVersionId,
    actor: &str,
    occurred_at: &str,
) -> Result<CrawlerVersion, CrawlerRepositoryError> {
    let mut crawler_rows = connection
        .query(
            "SELECT active_draft_version_id FROM crawlers WHERE id = ?1",
            [crawler_id.to_string()],
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    let Some(crawler_row) = crawler_rows
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?
    else {
        return Err(CrawlerRepositoryError::CrawlerNotFound);
    };
    let active_draft: Option<String> = crawler_row
        .get(0)
        .map_err(CrawlerRepositoryError::database)?;
    if active_draft.is_some() {
        return Err(CrawlerRepositoryError::ActiveDraftExists);
    }

    let source = load_version(connection, source_version_id).await?;
    if source.crawler_id() != crawler_id {
        return Err(CrawlerRepositoryError::VersionNotOwnedByCrawler);
    }
    if source.state() != CrawlerVersionState::Published {
        return Err(CrawlerRepositoryError::VersionNotPublished);
    }
    let clone = source
        .draft_from_published()
        .map_err(|_| CrawlerRepositoryError::InvalidLifecycleTransition)?;

    let mut page_map = declared_child_id_map(
        source
            .page_type_ids()
            .iter()
            .zip(clone.page_type_ids())
            .map(|(source, target)| (source.to_string(), target.to_string())),
    );
    let mut transition_map = declared_child_id_map(
        source
            .transition_ids()
            .iter()
            .zip(clone.transition_ids())
            .map(|(source, target)| (source.to_string(), target.to_string())),
    );
    augment_child_id_map(
        connection,
        source_version_id,
        &mut page_map,
        "SELECT id FROM page_types WHERE crawler_version_id = ?1",
    )
    .await?;
    augment_child_id_map(
        connection,
        source_version_id,
        &mut transition_map,
        "SELECT id FROM discovery_transitions WHERE crawler_version_id = ?1",
    )
    .await?;
    let seed_map = source
        .seeds()
        .iter()
        .zip(clone.seeds())
        .map(|(source, target)| (source.id.to_string(), target.id.to_string()))
        .collect::<BTreeMap<_, _>>();

    let configuration = serialize(&clone).map_err(CrawlerRepositoryError::database)?;
    connection
        .execute(
            "INSERT INTO crawler_versions (id, crawler_id, state, semantic_configuration_json) VALUES (?1, ?2, 'DRAFT', ?3)",
            (clone.id().to_string(), crawler_id.to_string(), configuration),
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    clone_child_rows(
        connection,
        source_version_id,
        clone.id(),
        &seed_map,
        &page_map,
        &transition_map,
    )
    .await?;
    let activated = connection
        .execute(
            "UPDATE crawlers SET active_draft_version_id = ?1 WHERE id = ?2 AND active_draft_version_id IS NULL",
            (clone.id().to_string(), crawler_id.to_string()),
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    if activated != 1 {
        return Err(CrawlerRepositoryError::ConcurrentVersionTransition);
    }
    let hash = semantic_hash(connection, &clone).await?;
    insert_audit_event(
        connection,
        format!("draft:{}:{}", clone.id(), occurred_at),
        "CRAWLER_DRAFT_CREATED",
        actor,
        occurred_at,
        &clone.id().to_string(),
        audit_payload(clone.id(), Some(source_version_id), Some(hash.as_str())),
    )
    .await?;
    Ok(clone)
}

async fn publish_in_transaction(
    connection: &Connection,
    crawler_id: CrawlerId,
    version_id: CrawlerVersionId,
    actor: &str,
    occurred_at: &str,
) -> Result<CrawlerVersionRecord, CrawlerRepositoryError> {
    let draft = load_version(connection, version_id).await?;
    if draft.crawler_id() != crawler_id {
        return Err(CrawlerRepositoryError::VersionNotOwnedByCrawler);
    }
    if draft.state() != CrawlerVersionState::Draft {
        return Err(CrawlerRepositoryError::VersionNotDraft);
    }
    let active_draft = active_draft_for(connection, crawler_id).await?;
    if active_draft.as_deref() != Some(version_id.to_string().as_str()) {
        return Err(if active_draft.is_some() {
            CrawlerRepositoryError::InvalidLifecycleTransition
        } else {
            CrawlerRepositoryError::VersionNotDraft
        });
    }
    let hash = semantic_hash(connection, &draft).await?;
    let mut published = draft.clone();
    published
        .publish()
        .map_err(|_| CrawlerRepositoryError::PublishedVersionImmutable)?;
    let configuration = serialize(&published).map_err(CrawlerRepositoryError::database)?;
    let updated = connection
        .execute(
            "UPDATE crawler_versions SET state = 'PUBLISHED', semantic_configuration_json = ?1 WHERE id = ?2 AND crawler_id = ?3 AND state = 'DRAFT'",
            (configuration, version_id.to_string(), crawler_id.to_string()),
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    if updated != 1 {
        return Err(CrawlerRepositoryError::ConcurrentVersionTransition);
    }
    let pointers = connection
        .execute(
            "UPDATE crawlers SET active_published_version_id = ?1, active_draft_version_id = NULL WHERE id = ?2 AND active_draft_version_id = ?1",
            (version_id.to_string(), crawler_id.to_string()),
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    if pointers != 1 {
        return Err(CrawlerRepositoryError::ConcurrentVersionTransition);
    }
    let base_version_id = base_version_from_audit(connection, version_id).await?;
    insert_audit_event(
        connection,
        format!("publish:{version_id}:{occurred_at}"),
        "CRAWLER_VERSION_PUBLISHED",
        actor,
        occurred_at,
        &version_id.to_string(),
        audit_payload(version_id, base_version_id, Some(hash.as_str())),
    )
    .await?;
    Ok(CrawlerVersionRecord {
        version: published,
        audit: CrawlerAuditMetadata {
            actor: Some(actor.to_owned()),
            occurred_at: Some(occurred_at.to_owned()),
            config_hash: Some(hash),
            warning_summary: Vec::new(),
            base_version_id,
        },
    })
}

async fn reactivate_in_transaction(
    connection: &Connection,
    crawler_id: CrawlerId,
    version_id: CrawlerVersionId,
    actor: &str,
    occurred_at: &str,
) -> Result<(), CrawlerRepositoryError> {
    let version = load_version(connection, version_id).await?;
    if version.crawler_id() != crawler_id {
        return Err(CrawlerRepositoryError::VersionNotOwnedByCrawler);
    }
    if version.state() != CrawlerVersionState::Published {
        return Err(CrawlerRepositoryError::VersionNotPublished);
    }
    let changed = connection
        .execute(
            "UPDATE crawlers SET active_published_version_id = ?1 WHERE id = ?2 AND EXISTS (SELECT 1 FROM crawler_versions WHERE id = ?1 AND crawler_id = ?2 AND state = 'PUBLISHED')",
            (version_id.to_string(), crawler_id.to_string()),
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    if changed != 1 {
        return Err(CrawlerRepositoryError::ConcurrentVersionTransition);
    }
    insert_audit_event(
        connection,
        format!("reactivate:{version_id}:{occurred_at}"),
        "CRAWLER_VERSION_REACTIVATED",
        actor,
        occurred_at,
        &version_id.to_string(),
        audit_payload(version_id, None, None),
    )
    .await
}

async fn ensure_crawler_exists(
    connection: &Connection,
    crawler_id: CrawlerId,
) -> Result<(), CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT 1 FROM crawlers WHERE id = ?1",
            [crawler_id.to_string()],
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    if rows
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?
        .is_none()
    {
        return Err(CrawlerRepositoryError::CrawlerNotFound);
    }
    Ok(())
}

async fn ensure_pointer_consistency(
    connection: &Connection,
    crawler_id: CrawlerId,
) -> Result<(), CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT 1 FROM crawlers AS crawler WHERE crawler.id = ?1 AND ((crawler.active_draft_version_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM crawler_versions AS version WHERE version.id = crawler.active_draft_version_id AND version.crawler_id = crawler.id AND version.state = 'DRAFT')) OR (crawler.active_published_version_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM crawler_versions AS version WHERE version.id = crawler.active_published_version_id AND version.crawler_id = crawler.id AND version.state = 'PUBLISHED'))) LIMIT 1",
            [crawler_id.to_string()],
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    if rows
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?
        .is_some()
    {
        return Err(CrawlerRepositoryError::CorruptState);
    }
    Ok(())
}

async fn active_draft_for(
    connection: &Connection,
    crawler_id: CrawlerId,
) -> Result<Option<String>, CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT active_draft_version_id FROM crawlers WHERE id = ?1",
            [crawler_id.to_string()],
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    let Some(row) = rows
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?
    else {
        return Err(CrawlerRepositoryError::CrawlerNotFound);
    };
    row.get(0).map_err(CrawlerRepositoryError::database)
}

async fn load_version(
    connection: &Connection,
    version_id: CrawlerVersionId,
) -> Result<CrawlerVersion, CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT id, crawler_id, state, semantic_configuration_json FROM crawler_versions WHERE id = ?1",
            [version_id.to_string()],
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    let Some(row) = rows
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?
    else {
        return Err(CrawlerRepositoryError::CrawlerVersionNotFound);
    };
    version_from_row(&row)
}

fn crawler_from_row(row: &Row) -> Result<Crawler, CrawlerRepositoryError> {
    let id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
    let id = parse_crawler_id(&id)?;
    let name: String = row.get(1).map_err(CrawlerRepositoryError::database)?;
    let collection_id: Option<String> = row.get(2).map_err(CrawlerRepositoryError::database)?;
    let defaults_json: String = row.get(3).map_err(CrawlerRepositoryError::database)?;
    let defaults: OperationalOverrides =
        serde_json::from_str(&defaults_json).map_err(|_| CrawlerRepositoryError::CorruptState)?;
    let active_published: Option<String> = row.get(4).map_err(CrawlerRepositoryError::database)?;
    let active_draft: Option<String> = row.get(5).map_err(CrawlerRepositoryError::database)?;
    Ok(Crawler::from_persisted(
        id,
        name,
        collection_id
            .as_deref()
            .map(parse_collection_id)
            .transpose()?,
        defaults,
        active_published
            .as_deref()
            .map(parse_version_id)
            .transpose()?,
        active_draft.as_deref().map(parse_version_id).transpose()?,
    ))
}

fn version_from_row(row: &Row) -> Result<CrawlerVersion, CrawlerRepositoryError> {
    let row_id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
    let crawler_id: String = row.get(1).map_err(CrawlerRepositoryError::database)?;
    let state: String = row.get(2).map_err(CrawlerRepositoryError::database)?;
    let configuration: String = row.get(3).map_err(CrawlerRepositoryError::database)?;
    let version: CrawlerVersion =
        serde_json::from_str(&configuration).map_err(|_| CrawlerRepositoryError::CorruptState)?;
    let id = parse_version_id(&row_id)?;
    let owner = parse_crawler_id(&crawler_id)?;
    let expected_state = match state.as_str() {
        "DRAFT" => CrawlerVersionState::Draft,
        "PUBLISHED" => CrawlerVersionState::Published,
        _ => return Err(CrawlerRepositoryError::CorruptState),
    };
    if version.id() != id || version.crawler_id() != owner || version.state() != expected_state {
        return Err(CrawlerRepositoryError::CorruptState);
    }
    Ok(version)
}

async fn audit_metadata(
    connection: &Connection,
    version_id: CrawlerVersionId,
) -> Result<CrawlerAuditMetadata, CrawlerRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT event_type, actor, occurred_at, payload_json FROM audit_events WHERE entity_type = 'CRAWLER_VERSION' AND entity_id = ?1 ORDER BY occurred_at, id",
            [version_id.to_string()],
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    let mut metadata = CrawlerAuditMetadata::default();
    while let Some(row) = rows
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?
    {
        let event_type: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        let actor: String = row.get(1).map_err(CrawlerRepositoryError::database)?;
        let occurred_at: String = row.get(2).map_err(CrawlerRepositoryError::database)?;
        let payload_json: String = row.get(3).map_err(CrawlerRepositoryError::database)?;
        let payload: Value = serde_json::from_str(&payload_json)
            .map_err(|_| CrawlerRepositoryError::CorruptState)?;
        metadata.actor = Some(actor);
        metadata.occurred_at = Some(occurred_at);
        if event_type == "CRAWLER_DRAFT_CREATED" || event_type == "CRAWLER_VERSION_PUBLISHED" {
            metadata.config_hash = payload
                .get("config_hash")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or(metadata.config_hash);
            metadata.base_version_id = payload
                .get("base_version_id")
                .and_then(Value::as_str)
                .map(parse_version_id)
                .transpose()?;
            metadata.warning_summary = payload
                .get("warning_summary")
                .and_then(Value::as_array)
                .map(|warnings| {
                    warnings
                        .iter()
                        .filter_map(Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect()
                })
                .unwrap_or_default();
        }
    }
    Ok(metadata)
}

async fn base_version_from_audit(
    connection: &Connection,
    version_id: CrawlerVersionId,
) -> Result<Option<CrawlerVersionId>, CrawlerRepositoryError> {
    Ok(audit_metadata(connection, version_id)
        .await?
        .base_version_id)
}

fn audit_payload(
    version_id: CrawlerVersionId,
    base_version_id: Option<CrawlerVersionId>,
    config_hash: Option<&str>,
) -> String {
    serde_json::json!({
        "version_id": version_id.to_string(),
        "base_version_id": base_version_id.map(|id| id.to_string()),
        "config_hash": config_hash,
        "warning_summary": [],
    })
    .to_string()
}

async fn insert_audit_event(
    connection: &Connection,
    id: String,
    event_type: &str,
    actor: &str,
    occurred_at: &str,
    entity_id: &str,
    payload_json: String,
) -> Result<(), CrawlerRepositoryError> {
    connection
        .execute(
            "INSERT INTO audit_events (id, event_type, actor, occurred_at, entity_type, entity_id, payload_json) VALUES (?1, ?2, ?3, ?4, 'CRAWLER_VERSION', ?5, ?6)",
            (id, event_type, actor, occurred_at, entity_id, payload_json),
        )
        .await
        .map_err(CrawlerRepositoryError::database)
        .map(|_| ())
}

async fn sync_seed_rows(
    connection: &Connection,
    version: &CrawlerVersion,
) -> Result<(), CrawlerRepositoryError> {
    connection
        .execute(
            "DELETE FROM seeds WHERE crawler_version_id = ?1",
            [version.id().to_string()],
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    for seed in version.seeds() {
        connection
            .execute(
                "INSERT INTO seeds (id, crawler_version_id, original_url, canonical_url, enabled, label, entry_page_type_hint_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                (
                    seed.id.to_string(),
                    version.id().to_string(),
                    seed.original_url.as_str(),
                    seed.canonical_url.as_str(),
                    i64::from(seed.enabled),
                    seed.label.clone(),
                    seed.entry_page_type_hint.map(|id| id.to_string()),
                ),
            )
            .await
            .map_err(CrawlerRepositoryError::database)?;
    }
    Ok(())
}

fn declared_child_id_map(
    pairs: impl Iterator<Item = (String, String)>,
) -> BTreeMap<String, String> {
    pairs.collect()
}

async fn augment_child_id_map(
    connection: &Connection,
    source_version_id: CrawlerVersionId,
    map: &mut BTreeMap<String, String>,
    sql: &str,
) -> Result<(), CrawlerRepositoryError> {
    let mut rows = connection
        .query(sql, [source_version_id.to_string()])
        .await
        .map_err(CrawlerRepositoryError::database)?;
    while let Some(row) = rows
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?
    {
        let id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        map.entry(id).or_insert_with(new_opaque_id);
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn clone_child_rows(
    connection: &Connection,
    source_version_id: CrawlerVersionId,
    target_version_id: CrawlerVersionId,
    seed_map: &BTreeMap<String, String>,
    page_map: &BTreeMap<String, String>,
    transition_map: &BTreeMap<String, String>,
) -> Result<(), CrawlerRepositoryError> {
    let mut seeds = connection
        .query(
            "SELECT id, original_url, canonical_url, enabled, label, entry_page_type_hint_id FROM seeds WHERE crawler_version_id = ?1 ORDER BY id",
            [source_version_id.to_string()],
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    let mut seed_rows = Vec::new();
    while let Some(row) = seeds
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?
    {
        let old_id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        let hint: Option<String> = row.get(5).map_err(CrawlerRepositoryError::database)?;
        let hint = hint
            .map(|id| {
                page_map
                    .get(&id)
                    .cloned()
                    .ok_or(CrawlerRepositoryError::CorruptState)
            })
            .transpose()?;
        seed_rows.push((
            seed_map.get(&old_id).cloned().unwrap_or_else(new_opaque_id),
            row.get::<String>(1)
                .map_err(CrawlerRepositoryError::database)?,
            row.get::<String>(2)
                .map_err(CrawlerRepositoryError::database)?,
            row.get::<i64>(3)
                .map_err(CrawlerRepositoryError::database)?,
            row.get::<Option<String>>(4)
                .map_err(CrawlerRepositoryError::database)?,
            hint,
        ));
    }
    for (id, original_url, canonical_url, enabled, label, hint) in seed_rows {
        connection
            .execute(
                "INSERT INTO seeds (id, crawler_version_id, original_url, canonical_url, enabled, label, entry_page_type_hint_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                (id, target_version_id.to_string(), original_url, canonical_url, enabled, label, hint),
            )
            .await
            .map_err(CrawlerRepositoryError::database)?;
    }

    let mut pages = connection
        .query(
            "SELECT id, name, priority, configuration_json FROM page_types WHERE crawler_version_id = ?1 ORDER BY id",
            [source_version_id.to_string()],
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    while let Some(row) = pages
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?
    {
        let old_id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        let configuration: String = row.get(3).map_err(CrawlerRepositoryError::database)?;
        let configuration =
            remap_json_references(&configuration, &[seed_map, page_map, transition_map])?;
        let new_id = page_map.get(&old_id).cloned().unwrap_or_else(new_opaque_id);
        connection
            .execute(
                "INSERT INTO page_types (id, crawler_version_id, name, priority, configuration_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    new_id,
                    target_version_id.to_string(),
                    row.get::<String>(1).map_err(CrawlerRepositoryError::database)?,
                    row.get::<i64>(2).map_err(CrawlerRepositoryError::database)?,
                    configuration,
                ),
            )
            .await
            .map_err(CrawlerRepositoryError::database)?;
    }

    let mut matchers = connection
        .query(
            "SELECT url_matchers.page_type_id, url_matchers.ordinal, url_matchers.matcher_json FROM url_matchers JOIN page_types ON page_types.id = url_matchers.page_type_id WHERE page_types.crawler_version_id = ?1 ORDER BY url_matchers.page_type_id, url_matchers.ordinal, url_matchers.id",
            [source_version_id.to_string()],
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    while let Some(row) = matchers
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?
    {
        let page_id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        let matcher_json: String = row.get(2).map_err(CrawlerRepositoryError::database)?;
        let matcher_json =
            remap_json_references(&matcher_json, &[seed_map, page_map, transition_map])?;
        let target_page = page_map
            .get(&page_id)
            .cloned()
            .ok_or(CrawlerRepositoryError::CorruptState)?;
        connection
            .execute(
                "INSERT INTO url_matchers (id, page_type_id, ordinal, matcher_json) VALUES (?1, ?2, ?3, ?4)",
                (
                    new_opaque_id(),
                    target_page,
                    row.get::<i64>(1).map_err(CrawlerRepositoryError::database)?,
                    matcher_json,
                ),
            )
            .await
            .map_err(CrawlerRepositoryError::database)?;
    }

    let mut transitions = connection
        .query(
            "SELECT id, configuration_json FROM discovery_transitions WHERE crawler_version_id = ?1 ORDER BY id",
            [source_version_id.to_string()],
        )
        .await
        .map_err(CrawlerRepositoryError::database)?;
    while let Some(row) = transitions
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?
    {
        let old_id: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
        let configuration: String = row.get(1).map_err(CrawlerRepositoryError::database)?;
        let configuration =
            remap_json_references(&configuration, &[seed_map, page_map, transition_map])?;
        let new_id = transition_map
            .get(&old_id)
            .cloned()
            .unwrap_or_else(new_opaque_id);
        connection
            .execute(
                "INSERT INTO discovery_transitions (id, crawler_version_id, configuration_json) VALUES (?1, ?2, ?3)",
                (new_id, target_version_id.to_string(), configuration),
            )
            .await
            .map_err(CrawlerRepositoryError::database)?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn semantic_hash(
    connection: &Connection,
    version: &CrawlerVersion,
) -> Result<String, CrawlerRepositoryError> {
    let mut identity_ids = BTreeSet::new();
    identity_ids.insert(version.id().to_string());
    identity_ids.insert(version.crawler_id().to_string());
    identity_ids.extend(version.seeds().iter().map(|seed| seed.id.to_string()));
    identity_ids.extend(version.page_type_ids().iter().map(ToString::to_string));
    identity_ids.extend(version.transition_ids().iter().map(ToString::to_string));
    collect_child_identity_ids(
        connection,
        "SELECT id FROM seeds WHERE crawler_version_id = ?1",
        version.id(),
        &mut identity_ids,
    )
    .await?;
    collect_child_identity_ids(
        connection,
        "SELECT id FROM page_types WHERE crawler_version_id = ?1",
        version.id(),
        &mut identity_ids,
    )
    .await?;
    collect_child_identity_ids(
        connection,
        "SELECT url_matchers.id FROM url_matchers JOIN page_types ON page_types.id = url_matchers.page_type_id WHERE page_types.crawler_version_id = ?1",
        version.id(),
        &mut identity_ids,
    )
    .await?;
    collect_child_identity_ids(
        connection,
        "SELECT id FROM discovery_transitions WHERE crawler_version_id = ?1",
        version.id(),
        &mut identity_ids,
    )
    .await?;
    let mut version_json = serde_json::to_value(version).map_err(|error| {
        CrawlerRepositoryError::database(DbError::Serialization(error.to_string()))
    })?;
    if let Value::Object(object) = &mut version_json {
        object.remove("id");
        object.remove("crawler_id");
        object.remove("state");
        object.remove("page_type_ids");
        object.remove("transition_ids");
        if let Some(Value::Array(seeds)) = object.get_mut("seeds") {
            for seed in &mut *seeds {
                if let Value::Object(seed) = seed {
                    seed.remove("id");
                    if seed.contains_key("entry_page_type_hint") {
                        seed.insert("entry_page_type_hint".into(), Value::Bool(true));
                    }
                }
            }
            seeds.sort_by_key(canonical_sort_key);
        }
        object.insert(
            "declared_page_type_count".into(),
            Value::from(version.page_type_ids().len()),
        );
        object.insert(
            "declared_transition_count".into(),
            Value::from(version.transition_ids().len()),
        );
        object.insert(
            "canonicalization_configured".into(),
            Value::Bool(version.canonicalization_policy_id().is_some()),
        );
        object.insert(
            "domain_scope_configured".into(),
            Value::Bool(version.domain_scope_id().is_some()),
        );
    }

    let seeds = child_values(
        connection,
        "SELECT original_url, canonical_url, enabled, label, entry_page_type_hint_id FROM seeds WHERE crawler_version_id = ?1 ORDER BY id",
        version.id(),
        |row| {
            let mut object = Map::new();
            object.insert(
                "original_url".into(),
                Value::String(row.get(0).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert(
                "canonical_url".into(),
                Value::String(row.get(1).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert(
                "enabled".into(),
                Value::from(row.get::<i64>(2).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert(
                "label".into(),
                row.get::<Option<String>>(3)
                    .map_err(CrawlerRepositoryError::database)?
                    .map_or(Value::Null, Value::String),
            );
            object.insert(
                "entry_page_type_hint".into(),
                Value::Bool(
                    row.get::<Option<String>>(4)
                        .map_err(CrawlerRepositoryError::database)?
                        .is_some(),
                ),
            );
            Ok(Value::Object(object))
        },
    )
    .await?;
    let pages = child_values(
        connection,
        "SELECT name, priority, configuration_json FROM page_types WHERE crawler_version_id = ?1 ORDER BY id",
        version.id(),
        |row| {
            let configuration: String = row.get(2).map_err(CrawlerRepositoryError::database)?;
            let configuration = serde_json::from_str::<Value>(&configuration)
                .map_err(|_| CrawlerRepositoryError::CorruptState)?;
            let mut configuration = configuration;
            normalize_identity_values(&mut configuration, &identity_ids);
            let mut object = Map::new();
            object.insert(
                "name".into(),
                Value::String(row.get(0).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert(
                "priority".into(),
                Value::from(row.get::<i64>(1).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert("configuration".into(), configuration);
            Ok(Value::Object(object))
        },
    )
    .await?;
    let matchers = child_values(
        connection,
        "SELECT page_types.name, page_types.priority, url_matchers.ordinal, url_matchers.matcher_json FROM url_matchers JOIN page_types ON page_types.id = url_matchers.page_type_id WHERE page_types.crawler_version_id = ?1 ORDER BY url_matchers.id",
        version.id(),
        |row| {
            let matcher: String = row
                .get(3)
                .map_err(CrawlerRepositoryError::database)?;
            let matcher = serde_json::from_str::<Value>(&matcher)
                .map_err(|_| CrawlerRepositoryError::CorruptState)?;
            let mut matcher = matcher;
            normalize_identity_values(&mut matcher, &identity_ids);
            let mut object = Map::new();
            object.insert(
                "page_type_name".into(),
                Value::String(row.get(0).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert(
                "page_type_priority".into(),
                Value::from(row.get::<i64>(1).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert(
                "ordinal".into(),
                Value::from(row.get::<i64>(2).map_err(CrawlerRepositoryError::database)?),
            );
            object.insert("matcher".into(), matcher);
            Ok(Value::Object(object))
        },
    )
    .await?;
    let transitions = child_values(
        connection,
        "SELECT configuration_json FROM discovery_transitions WHERE crawler_version_id = ?1 ORDER BY id",
        version.id(),
        |row| {
            let configuration: String = row.get(0).map_err(CrawlerRepositoryError::database)?;
            let mut configuration: Value = serde_json::from_str(&configuration)
                .map_err(|_| CrawlerRepositoryError::CorruptState)?;
            normalize_identity_values(&mut configuration, &identity_ids);
            Ok(configuration)
        },
    )
    .await?;
    let mut payload = BTreeMap::new();
    payload.insert("version", version_json);
    payload.insert("seeds", sorted_array(seeds));
    payload.insert("page_types", sorted_array(pages));
    payload.insert("url_matchers", sorted_array(matchers));
    payload.insert("transitions", sorted_array(transitions));
    canonical_sha256(&payload).map_err(|error| {
        CrawlerRepositoryError::database(DbError::Serialization(error.to_string()))
    })
}

async fn child_values<F>(
    connection: &Connection,
    sql: &str,
    version_id: CrawlerVersionId,
    mut map: F,
) -> Result<Vec<Value>, CrawlerRepositoryError>
where
    F: FnMut(&Row) -> Result<Value, CrawlerRepositoryError>,
{
    let mut rows = connection
        .query(sql, [version_id.to_string()])
        .await
        .map_err(CrawlerRepositoryError::database)?;
    let mut values = Vec::new();
    while let Some(row) = rows
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?
    {
        values.push(map(&row)?);
    }
    Ok(values)
}

async fn collect_child_identity_ids(
    connection: &Connection,
    sql: &str,
    version_id: CrawlerVersionId,
    ids: &mut BTreeSet<String>,
) -> Result<(), CrawlerRepositoryError> {
    let mut rows = connection
        .query(sql, [version_id.to_string()])
        .await
        .map_err(CrawlerRepositoryError::database)?;
    while let Some(row) = rows
        .next()
        .await
        .map_err(CrawlerRepositoryError::database)?
    {
        ids.insert(row.get(0).map_err(CrawlerRepositoryError::database)?);
    }
    Ok(())
}

fn remap_json_references(
    serialized: &str,
    maps: &[&BTreeMap<String, String>],
) -> Result<String, CrawlerRepositoryError> {
    let mut value: Value =
        serde_json::from_str(serialized).map_err(|_| CrawlerRepositoryError::CorruptState)?;
    remap_json_value(&mut value, maps);
    serde_json::to_string(&value).map_err(|_| CrawlerRepositoryError::CorruptState)
}

fn remap_json_value(value: &mut Value, maps: &[&BTreeMap<String, String>]) {
    match value {
        Value::Array(values) => values
            .iter_mut()
            .for_each(|value| remap_json_value(value, maps)),
        Value::Object(values) => values
            .values_mut()
            .for_each(|value| remap_json_value(value, maps)),
        Value::String(string) => {
            for map in maps {
                if let Some(replacement) = map.get(string) {
                    *string = replacement.clone();
                    break;
                }
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn normalize_identity_values(value: &mut Value, identity_ids: &BTreeSet<String>) {
    match value {
        Value::Array(values) => values
            .iter_mut()
            .for_each(|value| normalize_identity_values(value, identity_ids)),
        Value::Object(values) => values
            .values_mut()
            .for_each(|value| normalize_identity_values(value, identity_ids)),
        Value::String(string) if identity_ids.contains(string) => *string = "@identity".into(),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn sorted_array(mut values: Vec<Value>) -> Value {
    values.sort_by_key(canonical_sort_key);
    Value::Array(values)
}

fn canonical_sort_key(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

fn repository_error_as_db(error: CrawlerRepositoryError) -> DbError {
    match error {
        CrawlerRepositoryError::Database(error) => error,
        other => DbError::Invariant(other.to_string()),
    }
}

fn new_opaque_id() -> String {
    Uuid::now_v7().to_string()
}

fn parse_crawler_id(value: &str) -> Result<CrawlerId, CrawlerRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(CrawlerId::from_uuid)
        .ok_or(CrawlerRepositoryError::CorruptState)
}

fn parse_version_id(value: &str) -> Result<CrawlerVersionId, CrawlerRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(CrawlerVersionId::from_uuid)
        .ok_or(CrawlerRepositoryError::CorruptState)
}

fn parse_collection_id(value: &str) -> Result<erabi_domain::CollectionId, CrawlerRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(erabi_domain::CollectionId::from_uuid)
        .ok_or(CrawlerRepositoryError::CorruptState)
}

fn serialize(value: &impl serde::Serialize) -> Result<String, DbError> {
    serde_json::to_string(value).map_err(|error| DbError::Serialization(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MigrationRunner;
    use erabi_domain::{CrawlerVersion, DiscoveryTransitionId, PageTypeId, Seed};

    async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
        let database = ErabiDatabase::in_memory().await?;
        MigrationRunner::default().apply(&database).await?;
        Ok(database)
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn crawler_authoring_lifecycle_clones_children_and_preserves_history()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let crawler = Crawler::new("Catalog");
        repository.create(&crawler).await?;

        let page_type_id = PageTypeId::new();
        let transition_id = DiscoveryTransitionId::new();
        let mut initial = CrawlerVersion::draft(crawler.id());
        initial.set_page_type_ids(vec![page_type_id])?;
        initial.set_transition_ids(vec![transition_id])?;
        initial.add_seed(Seed::new(
            "https://example.test/catalog".parse()?,
            "https://example.test/catalog".parse()?,
        ))?;
        repository
            .save_draft(&initial, "operator", "2026-08-25T00:00:00Z")
            .await?;
        let connection = database.connection().await?;
        connection
            .execute(
                "INSERT INTO page_types (id, crawler_version_id, name, priority, configuration_json) VALUES (?1, ?2, 'catalog', 10, '{\"extract\":\"links\"}')",
                (page_type_id.to_string(), initial.id().to_string()),
            )
            .await?;
        connection
            .execute(
                "UPDATE page_types SET configuration_json = ?1 WHERE id = ?2",
                (
                    serde_json::json!({"extract":"links", "page_type_id": page_type_id.to_string()}).to_string(),
                    page_type_id.to_string(),
                ),
            )
            .await?;
        connection
            .execute(
                "INSERT INTO url_matchers (id, page_type_id, ordinal, matcher_json) VALUES (?1, ?2, 0, '{\"kind\":\"prefix\"}')",
                (new_opaque_id(), page_type_id.to_string()),
            )
            .await?;
        connection
            .execute(
                "INSERT INTO discovery_transitions (id, crawler_version_id, configuration_json) VALUES (?1, ?2, '{\"target\":\"catalog\"}')",
                (transition_id.to_string(), initial.id().to_string()),
            )
            .await?;

        initial.publish()?;
        repository
            .publish_and_activate(&crawler, &initial, "operator", "2026-08-25T00:01:00Z")
            .await?;
        let source_hash = repository
            .configuration_hash(crawler.id(), initial.id())
            .await?;
        assert_eq!(source_hash.len(), 64);

        let cloned = repository
            .create_draft_from_published(
                crawler.id(),
                initial.id(),
                "operator",
                "2026-08-25T00:02:00Z",
            )
            .await?;
        assert_ne!(cloned.id(), initial.id());
        assert_ne!(cloned.page_type_ids(), initial.page_type_ids());
        assert_ne!(cloned.transition_ids(), initial.transition_ids());
        assert_ne!(cloned.seeds()[0].id, initial.seeds()[0].id);
        assert_eq!(
            source_hash,
            repository
                .configuration_hash(crawler.id(), cloned.id())
                .await?
        );

        let child_counts: (i64, i64, i64, i64) = (
            connection
                .prepare("SELECT COUNT(*) FROM seeds WHERE crawler_version_id = ?1")
                .await?
                .query_row([cloned.id().to_string()])
                .await?
                .get(0)?,
            connection
                .prepare("SELECT COUNT(*) FROM page_types WHERE crawler_version_id = ?1")
                .await?
                .query_row([cloned.id().to_string()])
                .await?
                .get(0)?,
            connection
                .prepare("SELECT COUNT(*) FROM url_matchers WHERE page_type_id = ?1")
                .await?
                .query_row([cloned.page_type_ids()[0].to_string()])
                .await?
                .get(0)?,
            connection
                .prepare("SELECT COUNT(*) FROM discovery_transitions WHERE crawler_version_id = ?1")
                .await?
                .query_row([cloned.id().to_string()])
                .await?
                .get(0)?,
        );
        assert_eq!(child_counts, (1, 1, 1, 1));
        let cloned_page_configuration: String = connection
            .prepare("SELECT configuration_json FROM page_types WHERE crawler_version_id = ?1")
            .await?
            .query_row([cloned.id().to_string()])
            .await?
            .get(0)?;
        assert!(cloned_page_configuration.contains(&cloned.page_type_ids()[0].to_string()));
        assert!(!cloned_page_configuration.contains(&page_type_id.to_string()));

        let mut edited = cloned.clone();
        edited.add_seed(Seed::new(
            "https://example.test/second".parse()?,
            "https://example.test/second".parse()?,
        ))?;
        repository
            .save_draft(&edited, "operator", "2026-08-25T00:03:00Z")
            .await?;
        let source_seed_count: i64 = connection
            .prepare("SELECT COUNT(*) FROM seeds WHERE crawler_version_id = ?1")
            .await?
            .query_row([initial.id().to_string()])
            .await?
            .get(0)?;
        assert_eq!(source_seed_count, 1);

        let published_clone = repository
            .publish(
                crawler.id(),
                cloned.id(),
                "operator",
                "2026-08-25T00:03:30Z",
            )
            .await?;
        assert_eq!(published_clone.audit.base_version_id, Some(initial.id()));
        let older_draft = repository
            .create_draft_from_published(
                crawler.id(),
                initial.id(),
                "operator",
                "2026-08-25T00:03:45Z",
            )
            .await?;
        assert_ne!(older_draft.id(), published_clone.version.id());
        assert_eq!(
            repository
                .version(crawler.id(), older_draft.id())
                .await?
                .audit
                .base_version_id,
            Some(initial.id())
        );

        repository
            .reactivate_published_typed(
                crawler.id(),
                initial.id(),
                "operator",
                "2026-08-25T00:04:00Z",
            )
            .await?;
        let pointers = repository.pointers(&crawler).await?;
        assert_eq!(
            pointers.active_published_version_id,
            Some(initial.id().to_string())
        );
        assert_eq!(
            pointers.active_draft_version_id,
            Some(older_draft.id().to_string())
        );
        assert!(
            connection
                .execute(
                    "UPDATE seeds SET label = 'published-edit' WHERE crawler_version_id = ?1",
                    [initial.id().to_string()],
                )
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_initial_draft_creation_has_one_winner()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let crawler = Crawler::new("Concurrent");
        CrawlerRepository::new(&database).create(&crawler).await?;
        let left_database = database.clone();
        let right_database = database;
        let left = CrawlerRepository::new(&left_database);
        let right = CrawlerRepository::new(&right_database);
        let (left, right) = tokio::join!(
            left.create_draft(crawler.id(), "left", "2026-08-25T00:00:00Z"),
            right.create_draft(crawler.id(), "right", "2026-08-25T00:00:01Z"),
        );
        assert_eq!(
            usize::from(u8::from(left.is_ok())) + usize::from(u8::from(right.is_ok())),
            1
        );
        assert!(
            left.is_ok()
                || matches!(
                    left,
                    Err(CrawlerRepositoryError::ActiveDraftExists
                        | CrawlerRepositoryError::ConcurrentVersionTransition
                        | CrawlerRepositoryError::Database(_))
                )
        );
        assert!(
            right.is_ok()
                || matches!(
                    right,
                    Err(CrawlerRepositoryError::ActiveDraftExists
                        | CrawlerRepositoryError::ConcurrentVersionTransition
                        | CrawlerRepositoryError::Database(_))
                )
        );
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_draft_from_published_creation_has_one_winner()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let crawler = Crawler::new("Clone concurrency");
        let repository = CrawlerRepository::new(&database);
        repository.create(&crawler).await?;
        let initial = repository
            .create_draft(crawler.id(), "operator", "2026-08-25T00:00:00Z")
            .await?;
        repository
            .publish(
                crawler.id(),
                initial.id(),
                "operator",
                "2026-08-25T00:00:01Z",
            )
            .await?;

        let left_database = database.clone();
        let right_database = database.clone();
        let left_repository = CrawlerRepository::new(&left_database);
        let right_repository = CrawlerRepository::new(&right_database);
        let (left, right) = tokio::join!(
            left_repository.create_draft_from_published(
                crawler.id(),
                initial.id(),
                "left",
                "2026-08-25T00:00:02Z",
            ),
            right_repository.create_draft_from_published(
                crawler.id(),
                initial.id(),
                "right",
                "2026-08-25T00:00:03Z",
            ),
        );
        assert_eq!(
            usize::from(u8::from(left.is_ok())) + usize::from(u8::from(right.is_ok())),
            1
        );
        assert!(
            left.is_ok()
                || matches!(
                    left,
                    Err(CrawlerRepositoryError::ActiveDraftExists
                        | CrawlerRepositoryError::ConcurrentVersionTransition
                        | CrawlerRepositoryError::Database(_))
                )
        );
        assert!(
            right.is_ok()
                || matches!(
                    right,
                    Err(CrawlerRepositoryError::ActiveDraftExists
                        | CrawlerRepositoryError::ConcurrentVersionTransition
                        | CrawlerRepositoryError::Database(_))
                )
        );

        let pointers = repository.pointers(&crawler).await?;
        assert!(pointers.active_draft_version_id.is_some());
        let version_count: i64 = database
            .connection()
            .await?
            .prepare("SELECT COUNT(*) FROM crawler_versions WHERE crawler_id = ?1")
            .await?
            .query_row([crawler.id().to_string()])
            .await?
            .get(0)?;
        assert_eq!(version_count, 2);
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_semantic_configuration_is_not_not_found()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let repository = CrawlerRepository::new(&database);
        let crawler = Crawler::new("Corrupt");
        repository.create(&crawler).await?;
        let draft = repository
            .create_draft(crawler.id(), "operator", "2026-08-25T00:00:00Z")
            .await?;
        let connection = database.connection().await?;
        connection
            .execute(
                "UPDATE crawler_versions SET semantic_configuration_json = '{bad-json}' WHERE id = ?1",
                [draft.id().to_string()],
            )
            .await?;
        assert!(matches!(
            repository.version(crawler.id(), draft.id()).await,
            Err(CrawlerRepositoryError::CorruptState)
        ));
        Ok(())
    }
}
