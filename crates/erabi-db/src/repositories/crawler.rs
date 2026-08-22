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

    /// Persists and activates an editable Draft version atomically.
    ///
    /// The Draft configuration, Crawler pointer, and audit event form one
    /// lifecycle action. A Published version has no normal repository write path.
    ///
    /// # Errors
    /// Returns an invariant error if the input is Published or a database error
    /// if the Draft cannot be stored.
    pub async fn save_draft(
        &self,
        version: &CrawlerVersion,
        actor: &str,
        occurred_at: &str,
    ) -> Result<(), DbError> {
        if version.state() != CrawlerVersionState::Draft {
            return Err(DbError::Invariant(
                "published crawler versions cannot be saved through the Draft repository path"
                    .into(),
            ));
        }
        let configuration = serialize(version)?;
        let mut connection = self.database.connection().await?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;
        let result = save_draft_in_transaction(
            &transaction,
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
        let mut connection = self.database.connection().await?;
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

    /// Counts audit events recorded for an entity.
    ///
    /// # Errors
    /// Returns a database error when the audit history cannot be read.
    pub async fn audit_event_count(&self, entity_id: &str) -> Result<i64, DbError> {
        let connection = self.database.connection().await?;
        let row = connection
            .prepare("SELECT COUNT(*) FROM audit_events WHERE entity_id = ?1")
            .await?
            .query_row([entity_id])
            .await?;
        Ok(row.get(0)?)
    }

    /// Atomically reactivates a historical Published version without mutating it.
    ///
    /// An unrelated active Draft remains active while the historical Published
    /// version becomes the active published pointer.
    ///
    /// # Errors
    /// Returns an invariant error when the version is not a Published version
    /// belonging to `crawler`, or a database error if the action cannot commit.
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
        let mut connection = self.database.connection().await?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;
        let result =
            reactivate_in_transaction(&transaction, crawler, version, actor, occurred_at).await;
        match result {
            Ok(()) => transaction.commit().await.map_err(DbError::from),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }
}

async fn save_draft_in_transaction(
    connection: &Connection,
    version: &CrawlerVersion,
    configuration: &str,
    actor: &str,
    occurred_at: &str,
) -> Result<(), DbError> {
    let saved = connection
        .execute(
            "INSERT INTO crawler_versions (id, crawler_id, state, semantic_configuration_json) VALUES (?1, ?2, 'DRAFT', ?3) ON CONFLICT(id) DO UPDATE SET semantic_configuration_json = excluded.semantic_configuration_json WHERE crawler_versions.state = 'DRAFT' AND crawler_versions.crawler_id = excluded.crawler_id",
            (
                version.id().to_string(),
                version.crawler_id().to_string(),
                configuration,
            ),
        )
        .await?;
    if saved != 1 {
        return Err(DbError::Invariant(
            "the persisted CrawlerVersion was not an editable Draft for this Crawler".into(),
        ));
    }
    let activated = connection
        .execute(
            "UPDATE crawlers SET active_draft_version_id = ?1 WHERE id = ?2",
            (version.id().to_string(), version.crawler_id().to_string()),
        )
        .await?;
    if activated != 1 {
        return Err(DbError::Invariant(
            "the Draft cannot be activated because its Crawler does not exist".into(),
        ));
    }
    insert_audit_event(
        connection,
        format!("draft-activate:{}:{occurred_at}", version.id()),
        "CRAWLER_DRAFT_ACTIVATED",
        actor,
        occurred_at,
        "CRAWLER_VERSION",
        &version.id().to_string(),
    )
    .await
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
            "UPDATE crawlers SET active_published_version_id = ?1, active_draft_version_id = CASE WHEN active_draft_version_id = ?1 THEN NULL ELSE active_draft_version_id END WHERE id = ?2",
            (version.id().to_string(), crawler.id().to_string()),
        )
        .await?;
    insert_audit_event(
        connection,
        format!("publish:{}", version.id()),
        "CRAWLER_VERSION_PUBLISHED",
        actor,
        occurred_at,
        "CRAWLER_VERSION",
        &version.id().to_string(),
    )
    .await
}

async fn insert_audit_event(
    connection: &Connection,
    id: String,
    event_type: &str,
    actor: &str,
    occurred_at: &str,
    entity_type: &str,
    entity_id: &str,
) -> Result<(), DbError> {
    connection
        .execute(
            "INSERT INTO audit_events (id, event_type, actor, occurred_at, entity_type, entity_id, payload_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (id, event_type, actor, occurred_at, entity_type, entity_id, "{}"),
        )
        .await?;
    Ok(())
}

async fn reactivate_in_transaction(
    connection: &Connection,
    crawler: &Crawler,
    version: &CrawlerVersion,
    actor: &str,
    occurred_at: &str,
) -> Result<(), DbError> {
    let reactivated = connection
        .execute(
            "UPDATE crawlers SET active_published_version_id = ?1 WHERE id = ?2 AND EXISTS (SELECT 1 FROM crawler_versions WHERE id = ?1 AND crawler_id = ?2 AND state = 'PUBLISHED')",
            (version.id().to_string(), crawler.id().to_string()),
        )
        .await?;
    if reactivated != 1 {
        return Err(DbError::Invariant(
            "the persisted CrawlerVersion was not a Published version for this Crawler".into(),
        ));
    }
    insert_audit_event(
        connection,
        format!("reactivate:{}:{occurred_at}", version.id()),
        "CRAWLER_VERSION_REACTIVATED",
        actor,
        occurred_at,
        "CRAWLER_VERSION",
        &version.id().to_string(),
    )
    .await
}

fn serialize(value: &impl serde::Serialize) -> Result<String, DbError> {
    serde_json::to_string(value).map_err(|error| DbError::Serialization(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MigrationRunner;

    #[tokio::test]
    async fn published_versions_freeze_semantic_child_rows_but_not_test_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = ErabiDatabase::in_memory().await?;
        MigrationRunner::default().apply(&database).await?;
        let repository = CrawlerRepository::new(&database);
        let crawler = Crawler::new("Catalog");
        repository.create(&crawler).await?;
        let mut version = CrawlerVersion::draft(crawler.id());
        repository
            .save_draft(&version, "operator", "2026-08-23T00:00:00Z")
            .await?;

        let connection = database.connection().await?;
        connection
            .execute(
                "INSERT INTO seeds (id, crawler_version_id, original_url, canonical_url, enabled, label, entry_page_type_hint_id) VALUES ('seed-1', ?1, 'https://example.test', 'https://example.test', 1, 'before', NULL)",
                [version.id().to_string()],
            )
            .await?;
        connection
            .execute(
                "INSERT INTO page_types (id, crawler_version_id, name, priority, configuration_json) VALUES ('page-type-1', ?1, 'product', 1, '{}')",
                [version.id().to_string()],
            )
            .await?;
        connection
            .execute(
                "INSERT INTO url_matchers (id, page_type_id, ordinal, matcher_json) VALUES ('matcher-1', 'page-type-1', 0, '{}')",
                (),
            )
            .await?;
        connection
            .execute(
                "INSERT INTO discovery_transitions (id, crawler_version_id, configuration_json) VALUES ('transition-1', ?1, '{}')",
                [version.id().to_string()],
            )
            .await?;

        for update in [
            "UPDATE seeds SET label = 'draft-edit' WHERE id = 'seed-1'",
            "UPDATE page_types SET name = 'draft-edit' WHERE id = 'page-type-1'",
            "UPDATE url_matchers SET ordinal = 1 WHERE id = 'matcher-1'",
            "UPDATE discovery_transitions SET configuration_json = '{\"draft\":true}' WHERE id = 'transition-1'",
        ] {
            connection.execute(update, ()).await?;
        }

        version.publish()?;
        repository
            .publish_and_activate(&crawler, &version, "operator", "2026-08-23T00:01:00Z")
            .await?;

        for statement in [
            "INSERT INTO seeds (id, crawler_version_id, original_url, canonical_url, enabled, label, entry_page_type_hint_id) VALUES ('seed-2', (SELECT crawler_version_id FROM seeds WHERE id = 'seed-1'), 'https://example.test/2', 'https://example.test/2', 1, NULL, NULL)",
            "UPDATE seeds SET label = 'published-edit' WHERE id = 'seed-1'",
            "DELETE FROM seeds WHERE id = 'seed-1'",
            "INSERT INTO page_types (id, crawler_version_id, name, priority, configuration_json) VALUES ('page-type-2', (SELECT crawler_version_id FROM page_types WHERE id = 'page-type-1'), 'other', 2, '{}')",
            "UPDATE page_types SET name = 'published-edit' WHERE id = 'page-type-1'",
            "DELETE FROM page_types WHERE id = 'page-type-1'",
            "INSERT INTO url_matchers (id, page_type_id, ordinal, matcher_json) VALUES ('matcher-2', 'page-type-1', 2, '{}')",
            "UPDATE url_matchers SET ordinal = 2 WHERE id = 'matcher-1'",
            "DELETE FROM url_matchers WHERE id = 'matcher-1'",
            "INSERT INTO discovery_transitions (id, crawler_version_id, configuration_json) VALUES ('transition-2', (SELECT crawler_version_id FROM discovery_transitions WHERE id = 'transition-1'), '{}')",
            "UPDATE discovery_transitions SET configuration_json = '{\"published\":true}' WHERE id = 'transition-1'",
            "DELETE FROM discovery_transitions WHERE id = 'transition-1'",
        ] {
            assert!(connection.execute(statement, ()).await.is_err());
        }
        connection
            .execute(
                "INSERT INTO test_evidence (id, crawler_version_id, evidence_json, executed_at) VALUES ('evidence-1', ?1, '{}', '2026-08-23T00:02:00Z')",
                [version.id().to_string()],
            )
            .await?;
        Ok(())
    }
}
