use erabi_domain::{
    CrawlRunId, CrawlRunSnapshot, CrawlRunStatus, CrawlRunType, RunConfiguration, SourceId,
};
use serde_json::Value;
use turso::{Connection, transaction::TransactionBehavior};
use uuid::Uuid;

use crate::{DbError, ErabiDatabase};

/// Typed immutable Crawl Run read failures. Missing rows are distinct from
/// malformed or inconsistent durable snapshot evidence.
#[derive(Debug, thiserror::Error)]
pub enum CrawlRunRepositoryError {
    #[error("the Crawl Run does not exist")]
    NotFound,
    #[error("durable Crawl Run operation failed")]
    Database(#[source] DbError),
}

/// Persistence operations for immutable Crawl Run snapshots.
#[derive(Clone, Copy, Debug)]
pub struct CrawlRunRepository<'database> {
    database: &'database ErabiDatabase,
}

/// Immutable preserved evidence for one discovered URL decision. `detail` is
/// bounded structured provenance, not provider body content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveredUrlRecord {
    pub id: String,
    pub crawl_run_id: CrawlRunId,
    pub source_id: Option<SourceId>,
    pub raw_href: Option<String>,
    pub original_url: String,
    pub canonical_url: String,
    pub status: String,
    pub discovered_at: String,
    pub detail: Value,
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
    pub async fn snapshot(
        &self,
        id: CrawlRunId,
    ) -> Result<CrawlRunSnapshot, CrawlRunRepositoryError> {
        self.snapshot_by_stored_id(&id.to_string()).await
    }

    /// Reads the current durable lifecycle status without exposing a raw
    /// connection. This is intentionally a narrow diagnostic/read boundary:
    /// execution workers still use `transition_execution_status` for every
    /// lifecycle mutation.
    ///
    /// # Errors
    /// Returns `NotFound` for an absent run or a typed corruption error for an
    /// unrecognized durable status.
    pub async fn status(&self, id: CrawlRunId) -> Result<CrawlRunStatus, CrawlRunRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(CrawlRunRepositoryError::Database)?;
        let row = connection
            .prepare("SELECT status FROM crawl_runs WHERE id = ?1")
            .await
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?
            .query_row([id.to_string()])
            .await
            .map_err(|error| match error {
                turso::Error::QueryReturnedNoRows => CrawlRunRepositoryError::NotFound,
                other => CrawlRunRepositoryError::Database(DbError::from(other)),
            })?;
        parse_run_status(
            &row.get::<String>(0)
                .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?,
        )
    }

    /// Moves a run through the execution lifecycle without ever modifying its
    /// immutable snapshot. A recovered leased job may observe `RUNNING` again,
    /// so that transition is intentionally idempotent.
    ///
    /// # Errors
    /// Returns a typed error when the run is missing, already terminal, or the
    /// requested transition is not part of the canonical run lifecycle.
    pub async fn transition_execution_status(
        &self,
        id: CrawlRunId,
        status: CrawlRunStatus,
    ) -> Result<(), CrawlRunRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(CrawlRunRepositoryError::Database)?;
        transition_execution_status_in_transaction(&connection, id, status).await
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
    ) -> Result<CrawlRunSnapshot, CrawlRunRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(CrawlRunRepositoryError::Database)?;
        let row = connection
            .prepare(
                "SELECT snapshot_json, snapshot_hash, checkpoint_compatibility_hash FROM crawl_runs WHERE id = ?1",
            )
            .await
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?
            .query_row([stored_id])
            .await
            .map_err(|error| match error {
                turso::Error::QueryReturnedNoRows => CrawlRunRepositoryError::NotFound,
                other => CrawlRunRepositoryError::Database(DbError::from(other)),
            })?;
        let snapshot_json: String = row
            .get(0)
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?;
        let stored_snapshot_hash: String = row
            .get(1)
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?;
        let stored_checkpoint_compatibility_hash: String = row
            .get(2)
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?;
        let snapshot: CrawlRunSnapshot = serde_json::from_str(&snapshot_json).map_err(|error| {
            CrawlRunRepositoryError::Database(DbError::Invariant(format!(
                "stored CrawlRunSnapshot is invalid: {error}"
            )))
        })?;
        if snapshot.snapshot_hash() != stored_snapshot_hash {
            return Err(CrawlRunRepositoryError::Database(DbError::Invariant(
                "stored CrawlRunSnapshot hash column does not match snapshot JSON".into(),
            )));
        }
        if snapshot.checkpoint_compatibility_hash() != stored_checkpoint_compatibility_hash {
            return Err(CrawlRunRepositoryError::Database(DbError::Invariant(
                "stored CrawlRunSnapshot checkpoint hash column does not match snapshot JSON"
                    .into(),
            )));
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

    /// Reads the recorded timestamp for a Crawl Run's durable creation audit
    /// event using the opaque run identifier stored by related repositories.
    ///
    /// # Errors
    /// Returns `NotFound` only when the corresponding audit event is absent;
    /// other durable failures remain distinct repository errors.
    pub async fn created_audit_occurred_at_by_stored_id(
        &self,
        stored_id: &str,
    ) -> Result<String, CrawlRunRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(CrawlRunRepositoryError::Database)?;
        let row = connection
            .prepare(
                "SELECT occurred_at FROM audit_events WHERE id = ?1 AND event_type = 'CRAWL_RUN_CREATED'",
            )
            .await
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?
            .query_row([format!("run:{stored_id}")])
            .await
            .map_err(|error| match error {
                turso::Error::QueryReturnedNoRows => CrawlRunRepositoryError::NotFound,
                other => CrawlRunRepositoryError::Database(DbError::from(other)),
            })?;
        row.get(0)
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))
    }

    /// Persists one immutable discovery/provenance decision. The existing
    /// `discovered_urls` contract intentionally retains non-admitted links as
    /// well as scheduled work, so later finalization can distinguish a bounded
    /// exclusion from missing evidence.
    ///
    /// # Errors
    /// Returns a typed durable failure without modifying a prior discovery row.
    pub async fn record_discovered_url(
        &self,
        record: &DiscoveredUrlRecord,
    ) -> Result<(), CrawlRunRepositoryError> {
        validate_discovered_url_record(record).map_err(CrawlRunRepositoryError::Database)?;
        let connection = self
            .database
            .connection()
            .await
            .map_err(CrawlRunRepositoryError::Database)?;
        let run_exists = connection
            .query(
                "SELECT 1 FROM crawl_runs WHERE id = ?1",
                [record.crawl_run_id.to_string()],
            )
            .await
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?
            .next()
            .await
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?
            .is_some();
        if !run_exists {
            return Err(CrawlRunRepositoryError::NotFound);
        }
        let detail_json = serde_json::to_string(&record.detail).map_err(|error| {
            CrawlRunRepositoryError::Database(DbError::Serialization(error.to_string()))
        })?;
        connection
            .execute(
                "INSERT INTO discovered_urls (id, crawl_run_id, source_id, raw_href, original_url, canonical_url, status, discovered_at, detail_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                (
                    record.id.as_str(),
                    record.crawl_run_id.to_string(),
                    record.source_id.map_or(turso::Value::Null, |id| turso::Value::Text(id.to_string())),
                    record.raw_href.as_deref().map_or(turso::Value::Null, |value| turso::Value::Text(value.to_owned())),
                    record.original_url.as_str(),
                    record.canonical_url.as_str(),
                    record.status.as_str(),
                    record.discovered_at.as_str(),
                    detail_json,
                ),
            )
            .await
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?;
        Ok(())
    }

    /// Reads durable discovery/provenance decisions in deterministic creation
    /// order. Production uses the same `discovered_urls` table for admitted
    /// and preserve-only evidence; callers never need a raw connection to
    /// verify either category.
    ///
    /// # Errors
    /// Returns `NotFound` for an absent run and a typed corruption error for
    /// malformed durable provenance.
    pub async fn discovered_urls(
        &self,
        id: CrawlRunId,
    ) -> Result<Vec<DiscoveredUrlRecord>, CrawlRunRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(CrawlRunRepositoryError::Database)?;
        let exists = connection
            .query("SELECT 1 FROM crawl_runs WHERE id = ?1", [id.to_string()])
            .await
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?
            .next()
            .await
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?
            .is_some();
        if !exists {
            return Err(CrawlRunRepositoryError::NotFound);
        }
        let mut rows = connection
            .query(
                "SELECT id, source_id, raw_href, original_url, canonical_url, status, discovered_at, detail_json FROM discovered_urls WHERE crawl_run_id = ?1 ORDER BY discovered_at COLLATE BINARY, id COLLATE BINARY",
                [id.to_string()],
            )
            .await
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?;
        let mut records = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?
        {
            let source_id = row
                .get::<Option<String>>(1)
                .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?
                .map(|value| {
                    uuid::Uuid::parse_str(&value)
                        .ok()
                        .and_then(SourceId::from_uuid)
                        .ok_or_else(|| {
                            CrawlRunRepositoryError::Database(DbError::Invariant(
                                "stored discovered URL source identity is invalid".into(),
                            ))
                        })
                })
                .transpose()?;
            let detail: String = row
                .get(7)
                .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?;
            let record = DiscoveredUrlRecord {
                id: row
                    .get(0)
                    .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?,
                crawl_run_id: id,
                source_id,
                raw_href: row
                    .get(2)
                    .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?,
                original_url: row
                    .get(3)
                    .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?,
                canonical_url: row
                    .get(4)
                    .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?,
                status: row
                    .get(5)
                    .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?,
                discovered_at: row
                    .get(6)
                    .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?,
                detail: serde_json::from_str(&detail).map_err(|error| {
                    CrawlRunRepositoryError::Database(DbError::Invariant(format!(
                        "stored discovered URL detail is invalid: {error}"
                    )))
                })?,
            };
            validate_discovered_url_record(&record).map_err(CrawlRunRepositoryError::Database)?;
            records.push(record);
        }
        Ok(records)
    }
}

/// Applies the canonical worker-owned Crawl Run lifecycle transition on an
/// existing transaction. Job queue failure synchronization uses this exact
/// boundary so it cannot bypass the run repository's transition rules.
pub(crate) async fn transition_execution_status_in_transaction(
    connection: &Connection,
    id: CrawlRunId,
    status: CrawlRunStatus,
) -> Result<(), CrawlRunRepositoryError> {
    let changed = match status {
        CrawlRunStatus::Running => connection
            .execute(
                "UPDATE crawl_runs SET status = 'RUNNING' WHERE id = ?1 AND status IN ('QUEUED', 'RUNNING')",
                [id.to_string()],
            )
            .await,
        CrawlRunStatus::Succeeded => connection
            .execute(
                "UPDATE crawl_runs SET status = 'SUCCEEDED' WHERE id = ?1 AND status IN ('QUEUED', 'RUNNING', 'SUCCEEDED')",
                [id.to_string()],
            )
            .await,
        CrawlRunStatus::PartialResult => connection
            .execute(
                "UPDATE crawl_runs SET status = 'PARTIAL_RESULT' WHERE id = ?1 AND status IN ('QUEUED', 'RUNNING', 'PARTIAL_RESULT')",
                [id.to_string()],
            )
            .await,
        CrawlRunStatus::Failed => connection
            .execute(
                "UPDATE crawl_runs SET status = 'FAILED' WHERE id = ?1 AND status IN ('QUEUED', 'RUNNING', 'FAILED')",
                [id.to_string()],
            )
            .await,
        CrawlRunStatus::Queued | CrawlRunStatus::Cancelled => {
            return Err(CrawlRunRepositoryError::Database(DbError::Invariant(
                "execution workers cannot directly set queued or cancelled run status".into(),
            )));
        }
    }
    .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?;
    if changed != 1 {
        let exists = connection
            .query("SELECT 1 FROM crawl_runs WHERE id = ?1", [id.to_string()])
            .await
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?
            .next()
            .await
            .map_err(|error| CrawlRunRepositoryError::Database(DbError::from(error)))?
            .is_some();
        return Err(if exists {
            CrawlRunRepositoryError::Database(DbError::Invariant(
                "Crawl Run lifecycle transition is not legal".into(),
            ))
        } else {
            CrawlRunRepositoryError::NotFound
        });
    }
    Ok(())
}

fn validate_discovered_url_record(record: &DiscoveredUrlRecord) -> Result<(), DbError> {
    if Uuid::parse_str(&record.id).map_or(true, |id| id.get_version_num() != 7)
        || record.status.is_empty()
        || record.status.len() > 128
        || record
            .status
            .bytes()
            .any(|byte| !byte.is_ascii_uppercase() && byte != b'_')
        || record.discovered_at.trim().is_empty()
        || record.discovered_at.chars().count() > 256
    {
        return Err(DbError::Invariant(
            "discovered URL evidence is invalid".into(),
        ));
    }
    for value in [&record.original_url, &record.canonical_url] {
        let parsed = url::Url::parse(value)
            .map_err(|_| DbError::Invariant("discovered URL is invalid".into()))?;
        if value.chars().count() > 4_096
            || value.chars().any(char::is_control)
            || !matches!(parsed.scheme(), "http" | "https")
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
        {
            return Err(DbError::Invariant("discovered URL is invalid".into()));
        }
    }
    if record
        .raw_href
        .as_ref()
        .is_some_and(|value| value.chars().count() > 4_096 || value.chars().any(char::is_control))
    {
        return Err(DbError::Invariant("discovered href is invalid".into()));
    }
    Ok(())
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

fn parse_run_status(value: &str) -> Result<CrawlRunStatus, CrawlRunRepositoryError> {
    match value {
        "QUEUED" => Ok(CrawlRunStatus::Queued),
        "RUNNING" => Ok(CrawlRunStatus::Running),
        "SUCCEEDED" => Ok(CrawlRunStatus::Succeeded),
        "PARTIAL_RESULT" => Ok(CrawlRunStatus::PartialResult),
        "FAILED" => Ok(CrawlRunStatus::Failed),
        "CANCELLED" => Ok(CrawlRunStatus::Cancelled),
        _ => Err(CrawlRunRepositoryError::Database(DbError::Invariant(
            "stored Crawl Run status is invalid".into(),
        ))),
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
            Err(CrawlRunRepositoryError::Database(DbError::Invariant(_)))
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
            Err(CrawlRunRepositoryError::Database(DbError::Invariant(_)))
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
            Err(CrawlRunRepositoryError::Database(DbError::Invariant(_)))
        ));
        Ok(())
    }
}
