use erabi_domain::{Crawler, CrawlerVersion, CrawlerVersionState};
use turso::{Connection, transaction::TransactionBehavior};

use crate::{DbError, ErabiDatabase};

/// Current persisted Crawler pointers used to verify atomic version activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlerPointers {
    pub active_published_version_id: Option<String>,
    pub active_draft_version_id: Option<String>,
}

/// Persistence operations for Crawler identity and immutable versions.
#[derive(Clone, Copy, Debug)]
pub struct CrawlerRepository<'database> {
    database: &'database ErabiDatabase,
}

impl<'database> CrawlerRepository<'database> {
    #[must_use]
    pub const fn new(database: &'database ErabiDatabase) -> Self {
        Self { database }
    }

    /// Persists a new Crawler and its operational settings layer.
    ///
    /// # Errors
    /// Returns an error if the Crawler cannot be stored.
    pub async fn create(&self, crawler: &Crawler) -> Result<(), DbError> {
        let defaults = serialize(crawler.operational_defaults())?;
        let connection = self.database.connection()?;
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

    /// Persists an editable Draft version only.
    ///
    /// A Published version has no normal repository write path.
    ///
    /// # Errors
    /// Returns an invariant error if the input is Published or a database error
    /// if the Draft cannot be stored.
    pub async fn save_draft(&self, version: &CrawlerVersion) -> Result<(), DbError> {
        if version.state() != CrawlerVersionState::Draft {
            return Err(DbError::Invariant(
                "published crawler versions cannot be saved through the Draft repository path"
                    .into(),
            ));
        }
        let configuration = serialize(version)?;
        let connection = self.database.connection()?;
        connection
            .execute(
                "INSERT INTO crawler_versions (id, crawler_id, state, semantic_configuration_json) VALUES (?1, ?2, ?3, ?4)",
                (
                    version.id().to_string(),
                    version.crawler_id().to_string(),
                    "DRAFT",
                    configuration,
                ),
            )
            .await?;
        Ok(())
    }

    /// Atomically publishes a Draft, switches the Crawler pointer, and records audit history.
    ///
    /// # Errors
    /// Returns an invariant error when ownership/state is invalid or a database
    /// error when the transaction cannot be completed.
    pub async fn publish_and_activate(
        &self,
        crawler: &Crawler,
        version: &CrawlerVersion,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), DbError> {
        if version.state() != CrawlerVersionState::Published || version.crawler_id() != crawler.id()
        {
            return Err(DbError::Invariant(
                "only a published version belonging to the Crawler can be activated".into(),
            ));
        }
        let configuration = serialize(version)?;
        let mut connection = self.database.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;
        let result = publish_in_transaction(
            &transaction,
            crawler,
            version,
            configuration.as_str(),
            actor,
            occurred_at,
        )
        .await;
        match result {
            Ok(()) => transaction.commit().await.map_err(DbError::from),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Reads persisted Crawler pointers for integrity checks and tests.
    ///
    /// # Errors
    /// Returns an error when the Crawler does not exist or cannot be read.
    pub async fn pointers(&self, crawler: &Crawler) -> Result<CrawlerPointers, DbError> {
        let connection = self.database.connection()?;
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

    /// Counts audit events recorded for an entity.
    ///
    /// # Errors
    /// Returns a database error when the audit history cannot be read.
    pub async fn audit_event_count(&self, entity_id: &str) -> Result<i64, DbError> {
        let connection = self.database.connection()?;
        let row = connection
            .prepare("SELECT COUNT(*) FROM audit_events WHERE entity_id = ?1")
            .await?
            .query_row([entity_id])
            .await?;
        Ok(row.get(0)?)
    }
}

async fn publish_in_transaction(
    connection: &Connection,
    crawler: &Crawler,
    version: &CrawlerVersion,
    configuration: &str,
    actor: &str,
    occurred_at: &str,
) -> Result<(), DbError> {
    let updated = connection
        .execute(
            "UPDATE crawler_versions SET state = ?1, semantic_configuration_json = ?2 WHERE id = ?3 AND crawler_id = ?4 AND state = 'DRAFT'",
            (
                "PUBLISHED",
                configuration,
                version.id().to_string(),
                crawler.id().to_string(),
            ),
        )
        .await?;
    if updated != 1 {
        return Err(DbError::Invariant(
            "the persisted CrawlerVersion was not an activatable Draft".into(),
        ));
    }
    connection
        .execute(
            "UPDATE crawlers SET active_published_version_id = ?1, active_draft_version_id = NULL WHERE id = ?2",
            (version.id().to_string(), crawler.id().to_string()),
        )
        .await?;
    connection
        .execute(
            "INSERT INTO audit_events (id, event_type, actor, occurred_at, entity_type, entity_id, payload_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (
                format!("publish:{}", version.id()),
                "CRAWLER_VERSION_PUBLISHED",
                actor,
                occurred_at,
                "CRAWLER_VERSION",
                version.id().to_string(),
                "{}",
            ),
        )
        .await?;
    Ok(())
}

fn serialize(value: &impl serde::Serialize) -> Result<String, DbError> {
    serde_json::to_string(value).map_err(|error| DbError::Serialization(error.to_string()))
}
