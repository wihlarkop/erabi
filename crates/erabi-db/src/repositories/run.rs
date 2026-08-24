use erabi_domain::{CrawlRunId, CrawlRunSnapshot, CrawlRunStatus, CrawlRunType, RunConfiguration};
use turso::{Connection, transaction::TransactionBehavior};

use crate::{DbError, ErabiDatabase};

/// Persistence operations for immutable Crawl Run snapshots.
#[derive(Clone, Copy, Debug)]
pub struct CrawlRunRepository<'database> {
    database: &'database ErabiDatabase,
}

impl<'database> CrawlRunRepository<'database> {
    #[must_use]
    pub const fn new(database: &'database ErabiDatabase) -> Self {
        Self { database }
    }

    /// Persists a new Crawl Run snapshot and its audit event atomically.
    ///
    /// # Errors
    /// Returns an error if the run/audit transaction cannot be committed.
    pub async fn create(
        &self,
        id: CrawlRunId,
        status: CrawlRunStatus,
        snapshot: &CrawlRunSnapshot,
    ) -> Result<(), DbError> {
        let serialized = serde_json::to_string(snapshot)
            .map_err(|error| DbError::Serialization(error.to_string()))?;
        let mut connection = self.database.connection().await?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;
        let result =
            insert_run_in_transaction(&transaction, id, status, snapshot, serialized.as_str())
                .await;
        match result {
            Ok(()) => transaction.commit().await.map_err(DbError::from),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Loads the snapshot originally stored for a run.
    ///
    /// # Errors
    /// Returns an error when the run does not exist, cannot be read, or contains
    /// an invalid snapshot payload.
    pub async fn snapshot(&self, id: CrawlRunId) -> Result<CrawlRunSnapshot, DbError> {
        self.snapshot_by_stored_id(&id.to_string()).await
    }

    /// Loads a snapshot using a durable foreign-key value from another
    /// repository. The stored identifier is kept opaque at this boundary.
    ///
    /// # Errors
    /// Returns an error when the run does not exist, cannot be read, or
    /// contains an invalid immutable snapshot.
    pub async fn snapshot_by_stored_id(
        &self,
        stored_id: &str,
    ) -> Result<CrawlRunSnapshot, DbError> {
        let connection = self.database.connection().await?;
        let row = connection
            .prepare(
                "SELECT snapshot_json, snapshot_hash, checkpoint_compatibility_hash FROM crawl_runs WHERE id = ?1",
            )
            .await?
            .query_row([stored_id])
            .await?;
        let snapshot_json: String = row.get(0)?;
        let stored_snapshot_hash: String = row.get(1)?;
        let stored_checkpoint_compatibility_hash: String = row.get(2)?;
        let snapshot: CrawlRunSnapshot = serde_json::from_str(&snapshot_json).map_err(|error| {
            DbError::Invariant(format!("stored CrawlRunSnapshot is invalid: {error}"))
        })?;
        if snapshot.snapshot_hash() != stored_snapshot_hash {
            return Err(DbError::Invariant(
                "stored CrawlRunSnapshot hash column does not match snapshot JSON".into(),
            ));
        }
        if snapshot.checkpoint_compatibility_hash() != stored_checkpoint_compatibility_hash {
            return Err(DbError::Invariant(
                "stored CrawlRunSnapshot checkpoint hash column does not match snapshot JSON"
                    .into(),
            ));
        }
        Ok(snapshot)
    }

    /// Reads the durable creation audit payload for one Crawl Run.
    ///
    /// This narrowly exposes the structured event for diagnostics without
    /// exposing a raw database connection.
    ///
    /// # Errors
    /// Returns an error when the run/audit event does not exist or contains
    /// malformed structured JSON.
    pub async fn created_audit_payload(
        &self,
        id: CrawlRunId,
    ) -> Result<serde_json::Value, DbError> {
        let connection = self.database.connection().await?;
        let row = connection
            .prepare(
                "SELECT payload_json FROM audit_events WHERE id = ?1 AND event_type = 'CRAWL_RUN_CREATED'",
            )
            .await?
            .query_row([format!("run:{id}")])
            .await?;
        let payload: String = row.get(0)?;
        serde_json::from_str(&payload).map_err(|error| {
            DbError::Invariant(format!(
                "stored Crawl Run audit payload is invalid: {error}"
            ))
        })
    }
}

pub(crate) async fn insert_run_in_transaction(
    connection: &Connection,
    id: CrawlRunId,
    status: CrawlRunStatus,
    snapshot: &CrawlRunSnapshot,
    serialized: &str,
) -> Result<(), DbError> {
    let (crawler_id, crawler_version_id) = match snapshot.configuration() {
        RunConfiguration::CrawlerVersion {
            crawler_id,
            crawler_version_id,
            ..
        } => (
            turso::Value::Text(crawler_id.to_string()),
            turso::Value::Text(crawler_version_id.to_string()),
        ),
        RunConfiguration::QuickScrape { .. } => (turso::Value::Null, turso::Value::Null),
    };
    let audit_payload = robots_audit_payload(snapshot)?;
    connection
        .execute(
            "INSERT INTO crawl_runs (id, run_type, status, crawler_id, crawler_version_id, snapshot_json, snapshot_hash, checkpoint_compatibility_hash, actor, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            (
                id.to_string(),
                run_type_name(snapshot.run_type()),
                run_status_name(status),
                crawler_id,
                crawler_version_id,
                serialized,
                snapshot.snapshot_hash(),
                snapshot.checkpoint_compatibility_hash(),
                snapshot.actor(),
                snapshot.created_at(),
            ),
        )
        .await?;
    connection
        .execute(
            "INSERT INTO audit_events (id, event_type, actor, occurred_at, entity_type, entity_id, payload_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (
                format!("run:{id}"),
                "CRAWL_RUN_CREATED",
                snapshot.actor(),
                snapshot.created_at(),
                "CRAWL_RUN",
                id.to_string(),
                audit_payload,
            ),
        )
        .await?;
    Ok(())
}

fn robots_audit_payload(snapshot: &CrawlRunSnapshot) -> Result<String, DbError> {
    let robots = snapshot.robots();
    let mut payload = serde_json::Map::new();
    payload.insert(
        "robots".into(),
        serde_json::to_value(robots.decision())
            .map_err(|error| DbError::Serialization(error.to_string()))?,
    );
    payload.insert("actor".into(), serde_json::json!(robots.actor()));
    payload.insert("decision_at".into(), serde_json::json!(robots.decided_at()));
    payload.insert(
        "affected_scope".into(),
        serde_json::json!(robots.affected_scope()),
    );
    payload.insert("user_agent".into(), serde_json::json!(robots.user_agent()));
    if let RunConfiguration::CrawlerVersion {
        crawler_id,
        crawler_version_id,
        ..
    } = snapshot.configuration()
    {
        payload.insert(
            "crawler_id".into(),
            serde_json::json!(crawler_id.to_string()),
        );
        payload.insert(
            "crawler_version_id".into(),
            serde_json::json!(crawler_version_id.to_string()),
        );
    }
    serde_json::to_string(&serde_json::Value::Object(payload))
        .map_err(|error| DbError::Serialization(error.to_string()))
}

const fn run_type_name(run_type: CrawlRunType) -> &'static str {
    match run_type {
        CrawlRunType::QuickScrape => "QUICK_SCRAPE",
        CrawlRunType::TestRun => "TEST_RUN",
        CrawlRunType::DiscoveryPreview => "DISCOVERY_PREVIEW",
        CrawlRunType::ProductionRun => "PRODUCTION_RUN",
    }
}

const fn run_status_name(status: CrawlRunStatus) -> &'static str {
    match status {
        CrawlRunStatus::Queued => "QUEUED",
        CrawlRunStatus::Running => "RUNNING",
        CrawlRunStatus::Succeeded => "SUCCEEDED",
        CrawlRunStatus::PartialResult => "PARTIAL_RESULT",
        CrawlRunStatus::Failed => "FAILED",
        CrawlRunStatus::Cancelled => "CANCELLED",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::MigrationRunner;
    use erabi_domain::{
        CrawlRunSnapshotDraft, ResolvedValue, RobotsAudit, SettingSource,
        SnapshotOperationalSettings,
    };

    fn resolved<T>(value: T) -> ResolvedValue<T> {
        ResolvedValue {
            value,
            source: SettingSource::BuiltInDefault,
        }
    }

    fn snapshot() -> Result<CrawlRunSnapshot, Box<dyn std::error::Error>> {
        Ok(CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
            run_type: CrawlRunType::QuickScrape,
            configuration: RunConfiguration::QuickScrape {
                target_url: "https://example.test/item".parse()?,
                ad_hoc_configuration: BTreeMap::new(),
            },
            selected_seed_ids: Vec::new(),
            run_profile_id: None,
            settings: SnapshotOperationalSettings {
                max_pages: resolved(100),
                max_depth: resolved(3),
                max_duration_seconds: resolved(60),
                concurrency: resolved(2),
                request_delay_ms: resolved(250),
                timeout_ms: resolved(30_000),
                screenshot: resolved(false),
                asset_download_limit_bytes: resolved(1_000_000),
                retain_artifacts: resolved(true),
                user_agent: resolved("Erabi/0.1".into()),
            },
            robots: RobotsAudit::respect(
                "operator",
                "2026-08-23T00:00:00Z",
                "https://example.test",
                "Erabi/0.1",
                None,
            ),
            actor: "operator".into(),
            created_at: "2026-08-23T00:00:00Z".into(),
        })?)
    }

    #[tokio::test]
    async fn stored_snapshots_reject_json_and_projection_tampering()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = ErabiDatabase::in_memory().await?;
        MigrationRunner::default().apply(&database).await?;
        let repository = CrawlRunRepository::new(&database);
        let snapshot = snapshot()?;
        let run_id = CrawlRunId::new();
        repository
            .create(run_id, CrawlRunStatus::Queued, &snapshot)
            .await?;

        let connection = database.connection().await?;
        for assignment in [
            "run_type = run_type",
            "crawler_id = crawler_id",
            "crawler_version_id = crawler_version_id",
            "snapshot_json = snapshot_json",
            "snapshot_hash = snapshot_hash",
            "checkpoint_compatibility_hash = checkpoint_compatibility_hash",
            "actor = actor",
            "created_at = created_at",
        ] {
            assert!(
                connection
                    .execute(
                        format!("UPDATE crawl_runs SET {assignment} WHERE id = ?1"),
                        [run_id.to_string()],
                    )
                    .await
                    .is_err()
            );
        }
        connection
            .execute(
                "UPDATE crawl_runs SET status = 'RUNNING' WHERE id = ?1",
                [run_id.to_string()],
            )
            .await?;

        connection
            .execute_batch("DROP TRIGGER crawl_runs_snapshot_immutable")
            .await?;
        connection
            .execute(
                "UPDATE crawl_runs SET snapshot_hash = ?1 WHERE id = ?2",
                ("0".repeat(64), run_id.to_string()),
            )
            .await?;
        assert!(matches!(
            repository.snapshot(run_id).await,
            Err(DbError::Invariant(_))
        ));

        let invalid_run_id = CrawlRunId::new();
        repository
            .create(invalid_run_id, CrawlRunStatus::Queued, &snapshot)
            .await?;
        let mut invalid_snapshot = serde_json::to_value(&snapshot)?;
        invalid_snapshot["robots"]["decision"] = serde_json::json!({
            "decision": "OVERRIDE",
            "reason": " "
        });
        connection
            .execute(
                "UPDATE crawl_runs SET snapshot_json = ?1 WHERE id = ?2",
                (
                    serde_json::to_string(&invalid_snapshot)?,
                    invalid_run_id.to_string(),
                ),
            )
            .await?;
        assert!(matches!(
            repository.snapshot(invalid_run_id).await,
            Err(DbError::Invariant(_))
        ));

        let checkpoint_run_id = CrawlRunId::new();
        repository
            .create(checkpoint_run_id, CrawlRunStatus::Queued, &snapshot)
            .await?;
        connection
            .execute(
                "UPDATE crawl_runs SET checkpoint_compatibility_hash = ?1 WHERE id = ?2",
                ("f".repeat(64), checkpoint_run_id.to_string()),
            )
            .await?;
        assert!(matches!(
            repository.snapshot(checkpoint_run_id).await,
            Err(DbError::Invariant(_))
        ));
        Ok(())
    }
}
