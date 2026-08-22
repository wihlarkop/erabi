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
        let mut connection = self.database.connection()?;
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
        let connection = self.database.connection()?;
        let row = connection
            .prepare("SELECT snapshot_json FROM crawl_runs WHERE id = ?1")
            .await?
            .query_row([id.to_string()])
            .await?;
        let snapshot_json: String = row.get(0)?;
        serde_json::from_str(&snapshot_json)
            .map_err(|error| DbError::Serialization(error.to_string()))
    }
}

async fn insert_run_in_transaction(
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
                "{}",
            ),
        )
        .await?;
    Ok(())
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
