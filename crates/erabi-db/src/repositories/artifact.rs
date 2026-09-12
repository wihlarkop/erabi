use erabi_domain::{CrawlRunId, SourceId};

use crate::{DbError, ErabiDatabase, SqliteValue, StoredArtifact};

/// Metadata persistence for filesystem artifacts; artifact bytes never enter SQLite.
#[derive(Clone, Copy, Debug)]
pub struct ArtifactRepository<'database> {
    database: &'database ErabiDatabase,
}

impl<'database> ArtifactRepository<'database> {
    #[must_use]
    pub const fn new(database: &'database ErabiDatabase) -> Self {
        Self { database }
    }

    /// Stores artifact identity and metadata without storing artifact bytes.
    ///
    /// # Errors
    /// Returns an error when metadata cannot be serialized or persisted.
    pub async fn record(
        &self,
        artifact: &StoredArtifact,
        crawl_run_id: Option<CrawlRunId>,
        source_id: Option<SourceId>,
        media_type: Option<&str>,
        created_at: &str,
        metadata: &serde_json::Value,
    ) -> Result<(), DbError> {
        let metadata = serde_json::to_string(metadata)
            .map_err(|error| DbError::Serialization(error.to_string()))?;
        let artifact = artifact.clone();
        let media_type = media_type.map(str::to_owned);
        let created_at = created_at.to_owned();
        let byte_size = i64::try_from(artifact.byte_size).map_err(|_| {
            DbError::Invariant("artifact byte size exceeds SQLite INTEGER range".into())
        })?;
        self.database
            .call(move |raw| -> Result<(), DbError> {
                let connection = crate::SqliteConnection::new(raw);
                connection.execute(
                "INSERT INTO artifacts (id, crawl_run_id, source_id, content_hash, byte_size, media_type, safe_relative_path, created_at, metadata_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                (
                    artifact.id.to_string(),
                    crawl_run_id.map_or(SqliteValue::Null, |id| SqliteValue::Text(id.to_string())),
                    source_id.map_or(SqliteValue::Null, |id| SqliteValue::Text(id.to_string())),
                    artifact.content_hash.as_str(),
                    byte_size,
                    media_type.map_or(SqliteValue::Null, SqliteValue::Text),
                    artifact.safe_relative_path.to_string_lossy().into_owned(),
                    created_at,
                    metadata,
                ),
                )
                .map(|_| ())
                .map_err(DbError::from)
            })
            .await
    }

    /// Reads the safe relative path recorded for an artifact.
    ///
    /// # Errors
    /// Returns an error when the artifact does not exist or cannot be read.
    pub async fn safe_relative_path(
        &self,
        id: erabi_domain::ArtifactId,
    ) -> Result<String, DbError> {
        self.database
            .call(move |raw| -> Result<String, DbError> {
                let connection = crate::SqliteConnection::new(raw);
                let mut statement =
                    connection.prepare("SELECT safe_relative_path FROM artifacts WHERE id = ?1")?;
                let row = statement.query_row([id.to_string()])?;
                Ok(row.get(0)?)
            })
            .await
    }
}
