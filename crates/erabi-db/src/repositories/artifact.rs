use erabi_domain::{CrawlRunId, SourceId};

use crate::{DbError, ErabiDatabase, StoredArtifact};

/// Metadata persistence for filesystem artifacts; artifact bytes never enter Turso.
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
        let connection = self.database.connection().await?;
        connection
            .execute(
                "INSERT INTO artifacts (id, crawl_run_id, source_id, content_hash, byte_size, media_type, safe_relative_path, created_at, metadata_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                (
                    artifact.id.to_string(),
                    crawl_run_id.map_or(turso::Value::Null, |id| turso::Value::Text(id.to_string())),
                    source_id.map_or(turso::Value::Null, |id| turso::Value::Text(id.to_string())),
                    artifact.content_hash.as_str(),
                    i64::try_from(artifact.byte_size).map_err(|_| {
                        DbError::Invariant("artifact byte size exceeds Turso INTEGER range".into())
                    })?,
                    media_type.map_or(turso::Value::Null, |value| turso::Value::Text(value.to_owned())),
                    artifact.safe_relative_path.to_string_lossy().into_owned(),
                    created_at,
                    metadata,
                ),
            )
            .await?;
        Ok(())
    }

    /// Reads the safe relative path recorded for an artifact.
    ///
    /// # Errors
    /// Returns an error when the artifact does not exist or cannot be read.
    pub async fn safe_relative_path(
        &self,
        id: erabi_domain::ArtifactId,
    ) -> Result<String, DbError> {
        let connection = self.database.connection().await?;
        let row = connection
            .prepare("SELECT safe_relative_path FROM artifacts WHERE id = ?1")
            .await?
            .query_row([id.to_string()])
            .await?;
        Ok(row.get(0)?)
    }
}
