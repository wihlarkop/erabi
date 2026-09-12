use std::{
    collections::{BTreeSet, HashSet},
    fmt,
};

use crate::{
    SqliteConnection as Connection, SqliteRow as Row, SqliteTransaction as Transaction,
    SqliteValue, SqliteValue as Value,
};
use erabi_domain::{
    CrawlExecutionErrorCode, CrawlExecutionId, CrawlExecutionOutcome, CrawlRunId, CrawlRunSnapshot,
    CrawlRunType, CrawlerId, CrawlerVersion, CrawlerVersionId, CrawlerVersionState,
    DiscoveryTransition, DiscoveryTransitionId, PageTypeId, RunConfiguration, SourceId,
};
use rusqlite::TransactionBehavior;
use rusqlite::params_from_iter;
use url::Url;
use uuid::Uuid;

use super::JobId;
use super::crawl_traversal::CrawlWorkState;
use super::run::{
    CrawlRunRepositoryError, cancel_execution_status_in_transaction,
    transition_execution_status_in_transaction,
};

const MAX_EXECUTION_URL_CHARS: usize = 4_096;
const MAX_EXECUTION_MEDIA_TYPE_CHARS: usize = 256;
const MAX_DISCOVERED_URL_ID_CHARS: usize = 256;

/// The persisted role of one artifact referenced by a page execution.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CrawlExecutionArtifactKind {
    RawHtml,
    CleanedHtml,
    RenderedHtml,
    Markdown,
    Screenshot,
}

/// One artifact identity associated with a page execution. Artifact metadata
/// and bytes remain owned by the existing `artifacts` table and `ArtifactStore`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CrawlExecutionArtifact {
    pub artifact_id: erabi_domain::ArtifactId,
    pub kind: CrawlExecutionArtifactKind,
}

/// Durable provider-neutral evidence for one page execution.
#[derive(Clone, Eq, PartialEq)]
pub struct CrawlExecutionRecord {
    pub id: CrawlExecutionId,
    pub crawl_run_id: CrawlRunId,
    pub requested_url: String,
    pub canonical_url: String,
    pub observed_final_url: Option<String>,
    pub source_id: Option<SourceId>,
    pub page_type_id: Option<PageTypeId>,
    pub transition_id: Option<DiscoveryTransitionId>,
    pub discovered_url_id: Option<String>,
    pub outcome: CrawlExecutionOutcome,
    pub error_code: Option<CrawlExecutionErrorCode>,
    pub http_status: Option<u16>,
    pub media_type: Option<String>,
    pub content_length_bytes: Option<u64>,
    pub provider_elapsed_ms: Option<u64>,
    pub artifacts: Vec<CrawlExecutionArtifact>,
}

impl fmt::Debug for CrawlExecutionRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CrawlExecutionRecord")
            .field("id", &self.id)
            .field("crawl_run_id", &self.crawl_run_id)
            .field("requested_url", &safe_url_identity(&self.requested_url))
            .field("canonical_url", &safe_url_identity(&self.canonical_url))
            .field(
                "observed_final_url",
                &self.observed_final_url.as_deref().map(safe_url_identity),
            )
            .field("source_id", &self.source_id)
            .field("page_type_id", &self.page_type_id)
            .field("transition_id", &self.transition_id)
            .field("discovered_url_id", &self.discovered_url_id)
            .field("outcome", &self.outcome)
            .field("error_code", &self.error_code)
            .field("http_status", &self.http_status)
            .field("media_type", &self.media_type)
            .field("content_length_bytes", &self.content_length_bytes)
            .field("provider_elapsed_ms", &self.provider_elapsed_ms)
            .field("artifacts", &self.artifacts)
            .finish()
    }
}

/// Durable structural facts consumed by later run finalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CrawlExecutionSummary {
    pub crawl_run_id: CrawlRunId,
    pub in_scope_pages_planned: u64,
    pub in_scope_pages_completed: u64,
    pub pagination_truncation_count: u64,
    pub unresolved_partial_work_count: u64,
    pub page_type_ambiguity_count: u64,
}

/// Typed persistence errors for execution results and summaries.
#[derive(Debug, thiserror::Error)]
pub enum CrawlExecutionRepositoryError {
    #[error("the Crawl Run was not found")]
    CrawlRunNotFound,
    #[error("the page execution result was not found")]
    NotFound,
    #[error("the page execution identity already exists")]
    DuplicateExecution,
    #[error("the artifact was not found")]
    ArtifactNotFound,
    #[error("the artifact is not owned by the Crawl Run")]
    ArtifactNotOwnedByRun,
    #[error("the Source was not found")]
    SourceNotFound,
    #[error("the Source is not related to the Crawl Run and URL")]
    SourceNotOwnedByRun,
    #[error("the PageType reference is not applicable to this Crawl Run")]
    PageTypeNotApplicable,
    #[error("the PageType was not found")]
    PageTypeNotFound,
    #[error("the PageType is not owned by the Crawl Run's CrawlerVersion")]
    PageTypeNotOwnedByRun,
    #[error("the DiscoveryTransition reference is not applicable to this Crawl Run")]
    TransitionNotApplicable,
    #[error("the DiscoveryTransition was not found")]
    TransitionNotFound,
    #[error("the DiscoveryTransition is not owned by the Crawl Run's CrawlerVersion")]
    TransitionNotOwnedByRun,
    #[error("the DiscoveryTransition does not produce the recorded PageType")]
    TransitionDoesNotMatchPageType,
    #[error("the discovered URL is not owned by the Crawl Run")]
    DiscoveredUrlNotOwnedByRun,
    #[error("the reference is not applicable to the page execution")]
    InvalidReference,
    #[error("the execution input is invalid: {0}")]
    InvalidInput(&'static str),
    #[error("an execution counter is outside the supported integer range")]
    CounterOutOfRange,
    #[error("completed in-scope pages cannot exceed planned pages")]
    CompletedExceedsPlanned,
    #[error("the durable crawl execution state is corrupt")]
    CorruptState,
    #[error("the execution summary was not found")]
    SummaryNotFound,
    #[error("database operation failed")]
    Database(#[source] crate::DbError),
}

impl CrawlExecutionRepositoryError {
    fn database(error: impl Into<crate::DbError>) -> Self {
        Self::Database(error.into())
    }

    fn corrupt_on_read(self) -> Self {
        match self {
            Self::Database(error) => Self::Database(error),
            _ => Self::CorruptState,
        }
    }
}

impl From<crate::DbError> for CrawlExecutionRepositoryError {
    fn from(error: crate::DbError) -> Self {
        Self::Database(error)
    }
}

impl From<rusqlite::Error> for CrawlExecutionRepositoryError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(crate::DbError::from(error))
    }
}

/// Persistence operations for provider-neutral page execution evidence.
#[derive(Clone, Copy, Debug)]
pub struct CrawlExecutionRepository<'database> {
    database: &'database crate::ErabiDatabase,
}

impl<'database> CrawlExecutionRepository<'database> {
    #[must_use]
    pub const fn new(database: &'database crate::ErabiDatabase) -> Self {
        Self { database }
    }

    /// Persists one page execution and its artifact references atomically.
    ///
    /// # Errors
    /// Returns a typed ownership, input, duplicate, corruption, or database
    /// error. No result or artifact-reference row is left after a failure.
    pub async fn persist(
        &self,
        record: &CrawlExecutionRecord,
    ) -> Result<(), CrawlExecutionRepositoryError> {
        validate_record_input(record)?;
        let record = record.clone();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(CrawlExecutionRepositoryError::database)?;
                match persist_in_transaction(&transaction, &record) {
                    Ok(()) => transaction
                        .commit()
                        .map_err(CrawlExecutionRepositoryError::database),
                    Err(error) => {
                        let _ = transaction.rollback();
                        Err(error)
                    }
                }
            })
            .await
    }

    /// Atomically records append-only execution evidence and advances the
    /// current logical work projection. A crash before commit exposes neither
    /// half; a crash after commit cannot make a completed work item callable
    /// again merely because a checkpoint append was delayed.
    ///
    /// # Errors
    /// Returns a typed validation, ownership, execution, or transaction
    /// failure without exposing a half-advanced logical work state.
    #[allow(clippy::too_many_arguments)]
    pub async fn persist_current_work(
        &self,
        record: &CrawlExecutionRecord,
        crawl_url_state_id: &str,
        job_id: &JobId,
        job_attempt_id: &str,
        state: CrawlWorkState,
        expected_work_generation: u64,
        now: i64,
    ) -> Result<(), CrawlExecutionRepositoryError> {
        validate_record_input(record)?;
        if crawl_url_state_id.is_empty()
            || job_attempt_id.is_empty()
            || !outcome_matches_work_state(record.outcome, state)
        {
            return Err(CrawlExecutionRepositoryError::InvalidInput(
                "logical work lineage",
            ));
        }
        let record = record.clone();
        let crawl_url_state_id = crawl_url_state_id.to_owned();
        let job_id = job_id.clone();
        let job_attempt_id = job_attempt_id.to_owned();
        self.database
            .call(move |raw| {
        let mut connection = Connection::new(raw);
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(CrawlExecutionRepositoryError::database)?;
        let result = (|| {
            validate_current_work_owner(
                &transaction,
                &record,
                &crawl_url_state_id,
                &job_id,
                &job_attempt_id,
                expected_work_generation,
                now,
            )
            ?;
            persist_in_transaction_with_lineage(
                &transaction,
                &record,
                &crawl_url_state_id,
                &job_attempt_id,
                expected_work_generation,
            )
            ?;
            let changed = transaction.execute("UPDATE crawl_url_state SET current_work_state = ?1, current_execution_id = ?2 WHERE id = ?3 AND crawl_run_id = ?4 AND work_generation = ?5 AND current_work_state IN ('PENDING', 'RUNNING')", (work_state_name(state), record.id.to_string(), crawl_url_state_id.as_str(), record.crawl_run_id.to_string(), i64::try_from(expected_work_generation).map_err(|_| CrawlExecutionRepositoryError::CounterOutOfRange)?)).map_err(CrawlExecutionRepositoryError::database)?;
            if changed != 1 { return Err(CrawlExecutionRepositoryError::CorruptState); }
            Ok(())
        })();
        match result {
            Ok(()) => transaction
                .commit()

                .map_err(CrawlExecutionRepositoryError::database),
            Err(error) => {
                let _ = transaction.rollback();
                Err(error)
            }
        }
            })
            .await
    }

    /// Marks one logical work generation active before provider IO. The
    /// current job attempt and work generation are both verified in the same
    /// transaction, so a re-leased retry cannot revive a newer generation or
    /// let a stale attempt replace its current execution.
    ///
    /// # Errors
    /// Returns a typed ownership, lineage, or transition failure.
    #[allow(clippy::too_many_arguments)]
    pub async fn activate_current_work(
        &self,
        crawl_run_id: CrawlRunId,
        crawl_url_state_id: &str,
        job_id: &JobId,
        job_attempt_id: &str,
        expected_work_generation: u64,
        now: i64,
    ) -> Result<(), CrawlExecutionRepositoryError> {
        if crawl_url_state_id.is_empty() || job_attempt_id.is_empty() {
            return Err(CrawlExecutionRepositoryError::InvalidInput(
                "logical work activation",
            ));
        }
        let job_id = job_id.clone();
        let crawl_url_state_id = crawl_url_state_id.to_owned();
        let job_attempt_id = job_attempt_id.to_owned();
        self.database
            .call(move |raw| {
        let mut connection = Connection::new(raw);
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(CrawlExecutionRepositoryError::database)?;
        let result = (|| {
            validate_active_attempt_owner(
                &transaction,
                crawl_run_id,
                &job_id,
                &job_attempt_id,
                now,
            )
            ?;
            let changed = transaction
                .execute(
                    "UPDATE crawl_url_state SET current_work_state = 'RUNNING', current_execution_id = NULL WHERE id = ?1 AND crawl_run_id = ?2 AND admission_state = 'ADMITTED' AND work_generation = ?3 AND current_work_state IN ('PENDING', 'RUNNING', 'FAILED')",
                    (
                        crawl_url_state_id.as_str(),
                        crawl_run_id.to_string(),
                        i64::try_from(expected_work_generation)
                            .map_err(|_| CrawlExecutionRepositoryError::CounterOutOfRange)?,
                    ),
                )

                .map_err(CrawlExecutionRepositoryError::database)?;
            if changed != 1 {
                return Err(CrawlExecutionRepositoryError::InvalidReference);
            }
            Ok(())
        })();
        match result {
            Ok(()) => transaction
                .commit()

                .map_err(CrawlExecutionRepositoryError::database),
            Err(error) => {
                let _ = transaction.rollback();
                Err(error)
            }
        }
            })
            .await
    }

    /// Appends a physical execution that is known to be a historical alias
    /// observation while leaving the already completed canonical work unit
    /// authoritative. This is used only when an alias is discovered after an
    /// independently authored final-canonical Seed has already completed.
    ///
    /// # Errors
    /// Returns a typed ownership, execution, or transaction failure. The
    /// method never changes current logical work or current execution
    /// linkage.
    pub async fn persist_historical_work(
        &self,
        record: &CrawlExecutionRecord,
        crawl_url_state_id: &str,
        job_id: &JobId,
        job_attempt_id: &str,
        expected_work_generation: u64,
        now: i64,
    ) -> Result<(), CrawlExecutionRepositoryError> {
        validate_record_input(record)?;
        if crawl_url_state_id.is_empty() || job_attempt_id.is_empty() {
            return Err(CrawlExecutionRepositoryError::InvalidInput(
                "historical logical work lineage",
            ));
        }
        let job_id = job_id.clone();
        let record = record.clone();
        let crawl_url_state_id = crawl_url_state_id.to_owned();
        let job_attempt_id = job_attempt_id.to_owned();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(CrawlExecutionRepositoryError::database)?;
                let result = (|| {
                    validate_historical_work_owner(
                        &transaction,
                        &record,
                        &crawl_url_state_id,
                        &job_id,
                        &job_attempt_id,
                        expected_work_generation,
                        now,
                    )?;
                    persist_in_transaction_with_lineage(
                        &transaction,
                        &record,
                        &crawl_url_state_id,
                        &job_attempt_id,
                        expected_work_generation,
                    )
                })();
                match result {
                    Ok(()) => transaction
                        .commit()
                        .map_err(CrawlExecutionRepositoryError::database),
                    Err(error) => {
                        let _ = transaction.rollback();
                        Err(error)
                    }
                }
            })
            .await
    }

    /// Reads one page execution and validates every durable relationship.
    ///
    /// # Errors
    /// Returns `NotFound` for a missing result and `CorruptState` when a
    /// persisted row or reference no longer satisfies the execution contract.
    pub async fn read(
        &self,
        id: CrawlExecutionId,
    ) -> Result<CrawlExecutionRecord, CrawlExecutionRepositoryError> {
        self.database
            .call(move |raw| {
        let connection = Connection::new(raw);
        let row = connection
            .prepare(
                "SELECT id, crawl_run_id, requested_url, canonical_url, observed_final_url, source_id, page_type_id, transition_id, discovered_url_id, outcome, error_code, http_status, media_type, content_length_bytes, provider_elapsed_ms FROM crawl_execution_results WHERE id = ?1",
            )

            .map_err(CrawlExecutionRepositoryError::database)?
            .query_row([id.to_string()])

            .map_err(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => CrawlExecutionRepositoryError::NotFound,
                other => CrawlExecutionRepositoryError::database(other),
            })?;
        let record = read_record(&connection, &row)

            .map_err(CrawlExecutionRepositoryError::corrupt_on_read)?;
        Ok(record)
            })
            .await
    }

    /// Reads all page executions for a run in canonical URL and execution-ID
    /// order, validating each result and its references.
    ///
    /// # Errors
    /// Returns `CrawlRunNotFound` for a missing run or `CorruptState` for any
    /// inconsistent durable result in the run.
    pub async fn list_for_run(
        &self,
        crawl_run_id: CrawlRunId,
    ) -> Result<Vec<CrawlExecutionRecord>, CrawlExecutionRepositoryError> {
        self.database
            .call(move |raw| {
        let connection = Connection::new(raw);
        load_run_context(&connection, crawl_run_id)?;
        let mut rows = connection
            .query(
                "SELECT id, crawl_run_id, requested_url, canonical_url, observed_final_url, source_id, page_type_id, transition_id, discovered_url_id, outcome, error_code, http_status, media_type, content_length_bytes, provider_elapsed_ms FROM crawl_execution_results WHERE crawl_run_id = ?1 ORDER BY canonical_url COLLATE BINARY, id COLLATE BINARY",
                [crawl_run_id.to_string()],
            )

            .map_err(CrawlExecutionRepositoryError::database)?;
        let mut records = Vec::new();
        while let Some(row) = rows
            .next()

            .map_err(CrawlExecutionRepositoryError::database)?
        {
            records.push(
                read_record(&connection, row)

                    .map_err(CrawlExecutionRepositoryError::corrupt_on_read)?,
            );
        }
        Ok(records)
            })
            .await
    }

    /// Saves the current durable structural summary for one run.
    ///
    /// The summary is intentionally replaceable as execution progresses, but
    /// an existing malformed row is never repaired implicitly.
    ///
    /// # Errors
    /// Returns a typed counter, ownership, corruption, or database error.
    pub async fn save_summary(
        &self,
        summary: &CrawlExecutionSummary,
    ) -> Result<(), CrawlExecutionRepositoryError> {
        let summary = *summary;
        let values = summary_sql_values(&summary)?;
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(CrawlExecutionRepositoryError::database)?;
                let result = save_summary_in_transaction(&transaction, &summary, values);
                match result {
                    Ok(()) => transaction
                        .commit()
                        .map_err(CrawlExecutionRepositoryError::database),
                    Err(error) => {
                        let _ = transaction.rollback();
                        Err(error)
                    }
                }
            })
            .await
    }

    /// Atomically persists the current structural summary with its terminal
    /// run status. Finalization callers must not expose a status that was
    /// committed without the counters used to justify it.
    ///
    /// # Errors
    /// Returns a typed summary, run-lifecycle, or database error.
    pub async fn finalize(
        &self,
        summary: &CrawlExecutionSummary,
        status: erabi_domain::CrawlRunStatus,
    ) -> Result<(), CrawlExecutionRepositoryError> {
        let summary = *summary;
        let values = summary_sql_values(&summary)?;
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(CrawlExecutionRepositoryError::database)?;
                let result = (|| {
                    save_summary_in_transaction(&transaction, &summary, values)?;
                    if status == erabi_domain::CrawlRunStatus::Cancelled {
                        cancel_execution_status_in_transaction(&transaction, summary.crawl_run_id)
                            .map_err(map_run_error)
                    } else {
                        transition_execution_status_in_transaction(
                            &transaction,
                            summary.crawl_run_id,
                            status,
                        )
                        .map_err(map_run_error)
                    }
                })();
                match result {
                    Ok(()) => transaction
                        .commit()
                        .map_err(CrawlExecutionRepositoryError::database),
                    Err(error) => {
                        let _ = transaction.rollback();
                        Err(error)
                    }
                }
            })
            .await
    }

    /// Reads one durable run execution summary.
    ///
    /// # Errors
    /// Returns `SummaryNotFound` when no summary has been persisted for the
    /// valid run, and `CorruptState` for malformed counters or identity.
    pub async fn summary(
        &self,
        crawl_run_id: CrawlRunId,
    ) -> Result<CrawlExecutionSummary, CrawlExecutionRepositoryError> {
        self.database
            .call(move |raw| {
        let connection = Connection::new(raw);
        load_run_context(&connection, crawl_run_id)?;
        let mut rows = connection
            .query(
                "SELECT crawl_run_id, in_scope_pages_planned, in_scope_pages_completed, pagination_truncation_count, unresolved_partial_work_count, page_type_ambiguity_count FROM crawl_execution_summaries WHERE crawl_run_id = ?1",
                [crawl_run_id.to_string()],
            )

            .map_err(CrawlExecutionRepositoryError::database)?;
        let Some(row) = rows
            .next()

            .map_err(CrawlExecutionRepositoryError::database)?
            .cloned()
        else {
            return Err(CrawlExecutionRepositoryError::SummaryNotFound);
        };
        if rows
            .next()

            .map_err(CrawlExecutionRepositoryError::database)?
            .is_some()
        {
            return Err(CrawlExecutionRepositoryError::CorruptState);
        }
        summary_from_row(&row).map_err(CrawlExecutionRepositoryError::corrupt_on_read)
            })
            .await
    }
}

fn persist_in_transaction(
    connection: &impl crate::SqliteExecutor,
    record: &CrawlExecutionRecord,
) -> Result<(), CrawlExecutionRepositoryError> {
    let content_length_bytes = optional_counter(record.content_length_bytes)?;
    let provider_elapsed_ms = optional_counter(record.provider_elapsed_ms)?;
    let run = load_run_context(connection, record.crawl_run_id)?;
    validate_references(connection, record, &run)?;
    validate_artifact_references(connection, record)?;
    insert_execution_row(
        connection,
        record,
        content_length_bytes,
        provider_elapsed_ms,
        None,
    )
}

fn persist_in_transaction_with_lineage(
    connection: &impl crate::SqliteExecutor,
    record: &CrawlExecutionRecord,
    crawl_url_state_id: &str,
    job_attempt_id: &str,
    work_generation: u64,
) -> Result<(), CrawlExecutionRepositoryError> {
    if row_exists(
        connection,
        "SELECT 1 FROM crawl_execution_results WHERE id = ?1",
        [record.id.to_string()],
    )? {
        return Err(CrawlExecutionRepositoryError::DuplicateExecution);
    }

    let content_length_bytes = optional_counter(record.content_length_bytes)?;
    let provider_elapsed_ms = optional_counter(record.provider_elapsed_ms)?;
    let run = load_run_context(connection, record.crawl_run_id)?;
    validate_references(connection, record, &run)?;
    validate_artifact_references(connection, record)?;
    insert_execution_row(
        connection,
        record,
        content_length_bytes,
        provider_elapsed_ms,
        Some((crawl_url_state_id, job_attempt_id, work_generation)),
    )
}

fn insert_execution_row(
    connection: &impl crate::SqliteExecutor,
    record: &CrawlExecutionRecord,
    content_length_bytes: Value,
    provider_elapsed_ms: Value,
    lineage: Option<(&str, &str, u64)>,
) -> Result<(), CrawlExecutionRepositoryError> {
    if row_exists(
        connection,
        "SELECT 1 FROM crawl_execution_results WHERE id = ?1",
        [record.id.to_string()],
    )? {
        return Err(CrawlExecutionRepositoryError::DuplicateExecution);
    }

    let mut values = vec![
        Value::Text(record.id.to_string()),
        Value::Text(record.crawl_run_id.to_string()),
        Value::Text(record.requested_url.clone()),
        Value::Text(record.canonical_url.clone()),
        optional_text(record.observed_final_url.as_deref()),
        optional_id(record.source_id),
        optional_id(record.page_type_id),
        optional_id(record.transition_id),
        optional_text(record.discovered_url_id.as_deref()),
        optional_text(Some(outcome_name(record.outcome))),
        optional_error_code(record.error_code),
        optional_i64(record.http_status.map(i64::from)),
        optional_text(record.media_type.as_deref()),
        content_length_bytes,
        provider_elapsed_ms,
    ];
    let sql = if let Some((crawl_url_state_id, job_attempt_id, work_generation)) = lineage {
        values.extend([
            Value::Text(crawl_url_state_id.to_owned()),
            Value::Text(job_attempt_id.to_owned()),
            Value::Integer(
                i64::try_from(work_generation)
                    .map_err(|_| CrawlExecutionRepositoryError::CounterOutOfRange)?,
            ),
        ]);
        "INSERT INTO crawl_execution_results (id, crawl_run_id, requested_url, canonical_url, observed_final_url, source_id, page_type_id, transition_id, discovered_url_id, outcome, error_code, http_status, media_type, content_length_bytes, provider_elapsed_ms, crawl_url_state_id, job_attempt_id, work_generation) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)"
    } else {
        "INSERT INTO crawl_execution_results (id, crawl_run_id, requested_url, canonical_url, observed_final_url, source_id, page_type_id, transition_id, discovered_url_id, outcome, error_code, http_status, media_type, content_length_bytes, provider_elapsed_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"
    };
    connection
        .execute(sql, params_from_iter(values))
        .map_err(CrawlExecutionRepositoryError::database)?;

    for artifact in &record.artifacts {
        connection
            .execute(
                "INSERT INTO crawl_execution_artifacts (crawl_execution_id, artifact_id, artifact_kind) VALUES (?1, ?2, ?3)",
                (
                    record.id.to_string(),
                    artifact.artifact_id.to_string(),
                    artifact_kind_name(artifact.kind),
                ),
            )

            .map_err(CrawlExecutionRepositoryError::database)?;
    }
    Ok(())
}

fn validate_current_work_owner(
    connection: &Transaction<'_>,
    record: &CrawlExecutionRecord,
    crawl_url_state_id: &str,
    job_id: &JobId,
    job_attempt_id: &str,
    expected_work_generation: u64,
    now: i64,
) -> Result<(), CrawlExecutionRepositoryError> {
    let state = connection
        .prepare(
            "SELECT canonical_url, admission_state, current_work_state, work_generation FROM crawl_url_state WHERE id = ?1 AND crawl_run_id = ?2",
        )

        .map_err(CrawlExecutionRepositoryError::database)?
        .query_row((crawl_url_state_id, record.crawl_run_id.to_string()))

        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => CrawlExecutionRepositoryError::InvalidReference,
            other => CrawlExecutionRepositoryError::database(other),
        })?;
    let canonical_url: String = state
        .get(0)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let admission_state: String = state
        .get(1)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let current_work_state: Option<String> = state
        .get(2)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let generation: i64 = state
        .get(3)
        .map_err(CrawlExecutionRepositoryError::database)?;
    if canonical_url != record.canonical_url
        || admission_state != "ADMITTED"
        || !matches!(current_work_state.as_deref(), Some("PENDING" | "RUNNING"))
        || generation != i64::try_from(expected_work_generation).unwrap_or(i64::MIN)
    {
        return Err(CrawlExecutionRepositoryError::InvalidReference);
    }

    validate_active_attempt_owner(connection, record.crawl_run_id, job_id, job_attempt_id, now)
}

fn validate_historical_work_owner(
    connection: &Transaction<'_>,
    record: &CrawlExecutionRecord,
    crawl_url_state_id: &str,
    job_id: &JobId,
    job_attempt_id: &str,
    expected_work_generation: u64,
    now: i64,
) -> Result<(), CrawlExecutionRepositoryError> {
    let state = connection
        .prepare(
            "SELECT canonical_url, admission_state, current_work_state, work_generation, current_execution_id FROM crawl_url_state WHERE id = ?1 AND crawl_run_id = ?2",
        )

        .map_err(CrawlExecutionRepositoryError::database)?
        .query_row((crawl_url_state_id, record.crawl_run_id.to_string()))

        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => CrawlExecutionRepositoryError::InvalidReference,
            other => CrawlExecutionRepositoryError::database(other),
        })?;
    let canonical_url: String = state
        .get(0)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let admission_state: String = state
        .get(1)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let current_work_state: Option<String> = state
        .get(2)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let generation: i64 = state
        .get(3)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let current_execution_id: Option<String> = state
        .get(4)
        .map_err(CrawlExecutionRepositoryError::database)?;
    if canonical_url != record.canonical_url
        || admission_state != "ADMITTED"
        || !matches!(
            current_work_state.as_deref(),
            Some("COMPLETED" | "PARTIAL" | "FAILED" | "CANCELLED")
        )
        || current_execution_id.is_none()
        || generation != i64::try_from(expected_work_generation).unwrap_or(i64::MIN)
    {
        return Err(CrawlExecutionRepositoryError::InvalidReference);
    }

    validate_active_attempt_owner(connection, record.crawl_run_id, job_id, job_attempt_id, now)
}

fn validate_active_attempt_owner(
    connection: &Transaction<'_>,
    crawl_run_id: CrawlRunId,
    job_id: &JobId,
    job_attempt_id: &str,
    now: i64,
) -> Result<(), CrawlExecutionRepositoryError> {
    let attempt = connection
        .prepare(
            "SELECT job.crawl_run_id, job.state, job.current_attempt, job.lease_id, job.lease_owner, job.lease_generation, job.lease_expires_at, attempt.attempt_number, attempt.lease_id, attempt.lease_generation, attempt.worker_id, attempt.outcome FROM job_attempts AS attempt JOIN jobs AS job ON job.id = attempt.job_id WHERE attempt.id = ?1 AND job.id = ?2",
        )

        .map_err(CrawlExecutionRepositoryError::database)?
        .query_row((job_attempt_id, job_id.to_string()))

        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => CrawlExecutionRepositoryError::InvalidReference,
            other => CrawlExecutionRepositoryError::database(other),
        })?;
    let attempt_run_id: Option<String> = attempt
        .get(0)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let job_state: String = attempt
        .get(1)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let current_attempt: i64 = attempt
        .get(2)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let job_lease_id: Option<String> = attempt
        .get(3)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let job_lease_owner: Option<String> = attempt
        .get(4)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let job_lease_generation: i64 = attempt
        .get(5)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let job_lease_expires_at: Option<i64> = attempt
        .get(6)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let attempt_number: i64 = attempt
        .get(7)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let attempt_lease_id: String = attempt
        .get(8)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let attempt_lease_generation: i64 = attempt
        .get(9)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let attempt_worker_id: String = attempt
        .get(10)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let attempt_outcome: String = attempt
        .get(11)
        .map_err(CrawlExecutionRepositoryError::database)?;
    if attempt_run_id.as_deref() != Some(crawl_run_id.to_string().as_str())
        || job_state != "RUNNING"
        || current_attempt != attempt_number
        || job_lease_id.as_deref() != Some(attempt_lease_id.as_str())
        || job_lease_owner.as_deref() != Some(attempt_worker_id.as_str())
        || job_lease_generation != attempt_lease_generation
        || attempt_outcome != "RUNNING"
        || job_lease_expires_at.is_none_or(|expires_at| expires_at <= now)
    {
        return Err(CrawlExecutionRepositoryError::InvalidReference);
    }
    Ok(())
}

fn save_summary_in_transaction(
    connection: &Transaction<'_>,
    summary: &CrawlExecutionSummary,
    values: [i64; 5],
) -> Result<(), CrawlExecutionRepositoryError> {
    load_run_context(connection, summary.crawl_run_id)?;
    let mut existing_rows = connection
        .query(
            "SELECT crawl_run_id, in_scope_pages_planned, in_scope_pages_completed, pagination_truncation_count, unresolved_partial_work_count, page_type_ambiguity_count FROM crawl_execution_summaries WHERE crawl_run_id = ?1",
            [summary.crawl_run_id.to_string()],
        )

        .map_err(CrawlExecutionRepositoryError::database)?;
    let existing = existing_rows
        .next()
        .map_err(CrawlExecutionRepositoryError::database)?
        .cloned();
    if existing_rows
        .next()
        .map_err(CrawlExecutionRepositoryError::database)?
        .is_some()
    {
        return Err(CrawlExecutionRepositoryError::CorruptState);
    }
    if let Some(row) = existing {
        summary_from_row(&row)?;
        let updated = connection
            .execute(
                "UPDATE crawl_execution_summaries SET in_scope_pages_planned = ?1, in_scope_pages_completed = ?2, pagination_truncation_count = ?3, unresolved_partial_work_count = ?4, page_type_ambiguity_count = ?5 WHERE crawl_run_id = ?6",
                (values[0], values[1], values[2], values[3], values[4], summary.crawl_run_id.to_string()),
            )

            .map_err(CrawlExecutionRepositoryError::database)?;
        if updated != 1 {
            return Err(CrawlExecutionRepositoryError::CorruptState);
        }
    } else {
        connection
            .execute(
                "INSERT INTO crawl_execution_summaries (crawl_run_id, in_scope_pages_planned, in_scope_pages_completed, pagination_truncation_count, unresolved_partial_work_count, page_type_ambiguity_count) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                (summary.crawl_run_id.to_string(), values[0], values[1], values[2], values[3], values[4]),
            )

            .map_err(CrawlExecutionRepositoryError::database)?;
    }
    Ok(())
}

fn read_record(
    connection: &impl crate::SqliteExecutor,
    row: &Row,
) -> Result<CrawlExecutionRecord, CrawlExecutionRepositoryError> {
    let id = parse_execution_id(
        &row.get::<String>(0)
            .map_err(CrawlExecutionRepositoryError::database)?,
    )?;
    let crawl_run_id = parse_run_id(
        &row.get::<String>(1)
            .map_err(CrawlExecutionRepositoryError::database)?,
    )?;
    let record = CrawlExecutionRecord {
        id,
        crawl_run_id,
        requested_url: row
            .get(2)
            .map_err(CrawlExecutionRepositoryError::database)?,
        canonical_url: row
            .get(3)
            .map_err(CrawlExecutionRepositoryError::database)?,
        observed_final_url: row
            .get(4)
            .map_err(CrawlExecutionRepositoryError::database)?,
        source_id: parse_optional_source_id(
            row.get(5)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?,
        page_type_id: parse_optional_page_type_id(
            row.get(6)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?,
        transition_id: parse_optional_transition_id(
            row.get(7)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?,
        discovered_url_id: row
            .get(8)
            .map_err(CrawlExecutionRepositoryError::database)?,
        outcome: parse_outcome(
            &row.get::<String>(9)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?,
        error_code: parse_optional_error_code(
            row.get(10)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?,
        http_status: parse_optional_status(
            row.get(11)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?,
        media_type: row
            .get(12)
            .map_err(CrawlExecutionRepositoryError::database)?,
        content_length_bytes: parse_optional_counter(
            row.get(13)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?,
        provider_elapsed_ms: parse_optional_counter(
            row.get(14)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?,
        artifacts: read_artifacts(connection, id)?,
    };
    validate_record_input(&record)?;
    let run = load_run_context(connection, crawl_run_id)?;
    validate_references(connection, &record, &run)?;
    validate_artifact_references(connection, &record)?;
    Ok(record)
}

fn read_artifacts(
    connection: &impl crate::SqliteExecutor,
    execution_id: CrawlExecutionId,
) -> Result<Vec<CrawlExecutionArtifact>, CrawlExecutionRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT artifact_id, artifact_kind FROM crawl_execution_artifacts WHERE crawl_execution_id = ?1 ORDER BY artifact_kind COLLATE BINARY, artifact_id COLLATE BINARY",
            [execution_id.to_string()],
        )

        .map_err(CrawlExecutionRepositoryError::database)?;
    let mut seen_kinds = BTreeSet::new();
    let mut seen_artifacts = HashSet::new();
    let mut artifacts = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(CrawlExecutionRepositoryError::database)?
    {
        let artifact_id = parse_artifact_id(
            &row.get::<String>(0)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?;
        let kind = parse_artifact_kind(
            &row.get::<String>(1)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?;
        if !seen_kinds.insert(kind) || !seen_artifacts.insert(artifact_id) {
            return Err(CrawlExecutionRepositoryError::CorruptState);
        }
        artifacts.push(CrawlExecutionArtifact { artifact_id, kind });
    }
    Ok(artifacts)
}

fn validate_references(
    connection: &impl crate::SqliteExecutor,
    record: &CrawlExecutionRecord,
    run: &RunContext,
) -> Result<(), CrawlExecutionRepositoryError> {
    if let RunConfiguration::QuickScrape { target_url, .. } = run.snapshot.configuration()
        && record.requested_url != target_url.as_str()
    {
        return Err(CrawlExecutionRepositoryError::InvalidReference);
    }

    if let Some(source_id) = record.source_id {
        validate_source_reference(connection, record, source_id)?;
    }
    if let Some(discovered_url_id) = record.discovered_url_id.as_deref() {
        validate_discovered_url_reference(connection, record, discovered_url_id)?;
    }

    let Some(version_id) = run.crawler_version_id else {
        if record.page_type_id.is_some() {
            return Err(CrawlExecutionRepositoryError::PageTypeNotApplicable);
        }
        if record.transition_id.is_some() {
            return Err(CrawlExecutionRepositoryError::TransitionNotApplicable);
        }
        if record.discovered_url_id.is_some() {
            return Err(CrawlExecutionRepositoryError::InvalidReference);
        }
        return Ok(());
    };

    let version = load_version(connection, version_id)?;
    if run.crawler_id != Some(version.crawler_id()) {
        return Err(CrawlExecutionRepositoryError::CorruptState);
    }
    match (run.snapshot.run_type(), version.state()) {
        (CrawlRunType::ProductionRun, CrawlerVersionState::Published)
        | (CrawlRunType::TestRun | CrawlRunType::DiscoveryPreview, CrawlerVersionState::Draft) => {}
        _ => return Err(CrawlExecutionRepositoryError::CorruptState),
    }
    if let Some(page_type_id) = record.page_type_id {
        validate_page_type_reference(connection, &version, page_type_id)?;
    }
    if let Some(transition_id) = record.transition_id {
        let target_page_type_id =
            validate_transition_reference(connection, &version, transition_id)?;
        if record
            .page_type_id
            .is_some_and(|page_type_id| Some(page_type_id) != target_page_type_id)
        {
            return Err(CrawlExecutionRepositoryError::TransitionDoesNotMatchPageType);
        }
    }
    Ok(())
}

fn validate_source_reference(
    connection: &impl crate::SqliteExecutor,
    record: &CrawlExecutionRecord,
    source_id: SourceId,
) -> Result<(), CrawlExecutionRepositoryError> {
    let row = connection
        .prepare("SELECT id, original_url, canonical_url FROM sources WHERE id = ?1")
        .map_err(CrawlExecutionRepositoryError::database)?
        .query_row([source_id.to_string()])
        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => CrawlExecutionRepositoryError::SourceNotFound,
            other => CrawlExecutionRepositoryError::database(other),
        })?;
    let stored_id = parse_source_id(
        &row.get::<String>(0)
            .map_err(CrawlExecutionRepositoryError::database)?,
    )?;
    if stored_id != source_id {
        return Err(CrawlExecutionRepositoryError::CorruptState);
    }
    let source_original_url: String = row
        .get(1)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let source_canonical_url: String = row
        .get(2)
        .map_err(CrawlExecutionRepositoryError::database)?;
    validate_http_url(&source_original_url)
        .and_then(|()| validate_http_url(&source_canonical_url))
        .map_err(|_| CrawlExecutionRepositoryError::CorruptState)?;
    if source_canonical_url == record.canonical_url {
        return Ok(());
    }
    let related = connection
        .prepare(
            "SELECT 1 FROM discovered_urls WHERE crawl_run_id = ?1 AND source_id = ?2 AND canonical_url = ?3 LIMIT 1",
        )

        .map_err(CrawlExecutionRepositoryError::database)?
        .query_row((
            record.crawl_run_id.to_string(),
            source_id.to_string(),
            record.canonical_url.as_str(),
        ))
        ;
    match related {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            Err(CrawlExecutionRepositoryError::SourceNotOwnedByRun)
        }
        Err(error) => Err(CrawlExecutionRepositoryError::database(error)),
    }
}

fn validate_page_type_reference(
    connection: &impl crate::SqliteExecutor,
    version: &CrawlerVersion,
    page_type_id: PageTypeId,
) -> Result<(), CrawlExecutionRepositoryError> {
    let row = connection
        .prepare("SELECT id, crawler_version_id FROM page_types WHERE id = ?1")
        .map_err(CrawlExecutionRepositoryError::database)?
        .query_row([page_type_id.to_string()])
        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => CrawlExecutionRepositoryError::PageTypeNotFound,
            other => CrawlExecutionRepositoryError::database(other),
        })?;
    let stored_id = parse_page_type_id(
        &row.get::<String>(0)
            .map_err(CrawlExecutionRepositoryError::database)?,
    )?;
    let owner = parse_version_id(
        &row.get::<String>(1)
            .map_err(CrawlExecutionRepositoryError::database)?,
    )?;
    if stored_id != page_type_id {
        return Err(CrawlExecutionRepositoryError::CorruptState);
    }
    if owner != version.id() {
        return Err(CrawlExecutionRepositoryError::PageTypeNotOwnedByRun);
    }
    if !version.page_type_ids().contains(&page_type_id) {
        return Err(CrawlExecutionRepositoryError::CorruptState);
    }
    Ok(())
}

fn validate_transition_reference(
    connection: &impl crate::SqliteExecutor,
    version: &CrawlerVersion,
    transition_id: DiscoveryTransitionId,
) -> Result<Option<PageTypeId>, CrawlExecutionRepositoryError> {
    let row = connection
        .prepare(
            "SELECT id, crawler_version_id, configuration_json FROM discovery_transitions WHERE id = ?1",
        )

        .map_err(CrawlExecutionRepositoryError::database)?
        .query_row([transition_id.to_string()])

        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => CrawlExecutionRepositoryError::TransitionNotFound,
            other => CrawlExecutionRepositoryError::database(other),
        })?;
    let stored_id = parse_transition_id(
        &row.get::<String>(0)
            .map_err(CrawlExecutionRepositoryError::database)?,
    )?;
    let owner = parse_version_id(
        &row.get::<String>(1)
            .map_err(CrawlExecutionRepositoryError::database)?,
    )?;
    let configuration: String = row
        .get(2)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let transition: DiscoveryTransition = serde_json::from_str(&configuration)
        .map_err(|_| CrawlExecutionRepositoryError::CorruptState)?;
    if stored_id != transition_id || transition.id != transition_id {
        return Err(CrawlExecutionRepositoryError::CorruptState);
    }
    if owner != version.id() {
        return Err(CrawlExecutionRepositoryError::TransitionNotOwnedByRun);
    }
    if !version.transition_ids().contains(&transition_id) {
        return Err(CrawlExecutionRepositoryError::CorruptState);
    }
    transition
        .validate()
        .map_err(|_| CrawlExecutionRepositoryError::CorruptState)?;
    for page_type_id in [
        transition.source_page_type_id,
        transition.target_page_type_id,
    ] {
        validate_page_type_reference(connection, version, page_type_id)
            .map_err(|_| CrawlExecutionRepositoryError::CorruptState)?;
    }
    Ok(Some(transition.target_page_type_id))
}

fn validate_discovered_url_reference(
    connection: &impl crate::SqliteExecutor,
    record: &CrawlExecutionRecord,
    discovered_url_id: &str,
) -> Result<(), CrawlExecutionRepositoryError> {
    let row = connection
        .prepare(
            "SELECT id, crawl_run_id, source_id, original_url, canonical_url FROM discovered_urls WHERE id = ?1",
        )

        .map_err(CrawlExecutionRepositoryError::database)?
        .query_row([discovered_url_id])

        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => {
                CrawlExecutionRepositoryError::DiscoveredUrlNotOwnedByRun
            }
            other => CrawlExecutionRepositoryError::database(other),
        })?;
    let stored_id: String = row
        .get(0)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let stored_run = parse_run_id(
        &row.get::<String>(1)
            .map_err(CrawlExecutionRepositoryError::database)?,
    )?;
    let stored_source = parse_optional_source_id(
        row.get(2)
            .map_err(CrawlExecutionRepositoryError::database)?,
    )?;
    let original_url: String = row
        .get(3)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let canonical_url: String = row
        .get(4)
        .map_err(CrawlExecutionRepositoryError::database)?;
    if stored_id != discovered_url_id
        || stored_run != record.crawl_run_id
        || stored_source != record.source_id
        || original_url != record.requested_url
        || canonical_url != record.canonical_url
    {
        return Err(CrawlExecutionRepositoryError::DiscoveredUrlNotOwnedByRun);
    }
    validate_http_url(&original_url)
        .and_then(|()| validate_http_url(&canonical_url))
        .map_err(|_| CrawlExecutionRepositoryError::CorruptState)
}

fn validate_artifact_references(
    connection: &impl crate::SqliteExecutor,
    record: &CrawlExecutionRecord,
) -> Result<(), CrawlExecutionRepositoryError> {
    for artifact in &record.artifacts {
        let row = connection
            .prepare("SELECT id, crawl_run_id, source_id FROM artifacts WHERE id = ?1")
            .map_err(CrawlExecutionRepositoryError::database)?
            .query_row([artifact.artifact_id.to_string()])
            .map_err(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => {
                    CrawlExecutionRepositoryError::ArtifactNotFound
                }
                other => CrawlExecutionRepositoryError::database(other),
            })?;
        let stored_id = parse_artifact_id(
            &row.get::<String>(0)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?;
        let stored_run = row
            .get::<Option<String>>(1)
            .map_err(CrawlExecutionRepositoryError::database)?
            .map(|value| parse_run_id(&value))
            .transpose()?;
        let stored_source = parse_optional_source_id(
            row.get(2)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?;
        if stored_id != artifact.artifact_id {
            return Err(CrawlExecutionRepositoryError::CorruptState);
        }
        if stored_run != Some(record.crawl_run_id) {
            return Err(CrawlExecutionRepositoryError::ArtifactNotOwnedByRun);
        }
        if stored_source.is_some() && stored_source != record.source_id {
            return Err(CrawlExecutionRepositoryError::ArtifactNotOwnedByRun);
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct RunContext {
    snapshot: CrawlRunSnapshot,
    crawler_id: Option<CrawlerId>,
    crawler_version_id: Option<CrawlerVersionId>,
}

fn load_run_context(
    connection: &impl crate::SqliteExecutor,
    run_id: CrawlRunId,
) -> Result<RunContext, CrawlExecutionRepositoryError> {
    let row = connection
        .prepare(
            "SELECT id, run_type, crawler_id, crawler_version_id, snapshot_json, snapshot_hash, checkpoint_compatibility_hash FROM crawl_runs WHERE id = ?1",
        )

        .map_err(CrawlExecutionRepositoryError::database)?
        .query_row([run_id.to_string()])

        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => CrawlExecutionRepositoryError::CrawlRunNotFound,
            other => CrawlExecutionRepositoryError::database(other),
        })?;
    let stored_id = parse_run_id(
        &row.get::<String>(0)
            .map_err(CrawlExecutionRepositoryError::database)?,
    )?;
    if stored_id != run_id {
        return Err(CrawlExecutionRepositoryError::CorruptState);
    }
    let run_type = parse_run_type(
        &row.get::<String>(1)
            .map_err(CrawlExecutionRepositoryError::database)?,
    )?;
    let crawler_id = row
        .get::<Option<String>>(2)
        .map_err(CrawlExecutionRepositoryError::database)?
        .map(|value| parse_crawler_id(&value))
        .transpose()?;
    let crawler_version_id = row
        .get::<Option<String>>(3)
        .map_err(CrawlExecutionRepositoryError::database)?
        .map(|value| parse_version_id(&value))
        .transpose()?;
    let snapshot_json: String = row
        .get(4)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let snapshot: CrawlRunSnapshot = serde_json::from_str(&snapshot_json)
        .map_err(|_| CrawlExecutionRepositoryError::CorruptState)?;
    let stored_snapshot_hash: String = row
        .get(5)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let stored_checkpoint_compatibility_hash: String = row
        .get(6)
        .map_err(CrawlExecutionRepositoryError::database)?;
    if snapshot.snapshot_hash() != stored_snapshot_hash
        || snapshot.checkpoint_compatibility_hash() != stored_checkpoint_compatibility_hash
    {
        return Err(CrawlExecutionRepositoryError::CorruptState);
    }
    if snapshot.run_type() != run_type {
        return Err(CrawlExecutionRepositoryError::CorruptState);
    }
    match snapshot.configuration() {
        RunConfiguration::QuickScrape { .. } => {
            if crawler_id.is_some() || crawler_version_id.is_some() {
                return Err(CrawlExecutionRepositoryError::CorruptState);
            }
        }
        RunConfiguration::CrawlerVersion {
            crawler_id: snapshot_crawler_id,
            crawler_version_id: snapshot_version_id,
            ..
        } => {
            if crawler_id != Some(*snapshot_crawler_id)
                || crawler_version_id != Some(*snapshot_version_id)
            {
                return Err(CrawlExecutionRepositoryError::CorruptState);
            }
        }
    }
    Ok(RunContext {
        snapshot,
        crawler_id,
        crawler_version_id,
    })
}

fn load_version(
    connection: &impl crate::SqliteExecutor,
    version_id: CrawlerVersionId,
) -> Result<CrawlerVersion, CrawlExecutionRepositoryError> {
    let row = connection
        .prepare(
            "SELECT id, crawler_id, state, semantic_configuration_json FROM crawler_versions WHERE id = ?1",
        )

        .map_err(CrawlExecutionRepositoryError::database)?
        .query_row([version_id.to_string()])

        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => CrawlExecutionRepositoryError::CorruptState,
            other => CrawlExecutionRepositoryError::database(other),
        })?;
    let stored_id = parse_version_id(
        &row.get::<String>(0)
            .map_err(CrawlExecutionRepositoryError::database)?,
    )?;
    let crawler_id = parse_crawler_id(
        &row.get::<String>(1)
            .map_err(CrawlExecutionRepositoryError::database)?,
    )?;
    let state = parse_version_state(
        &row.get::<String>(2)
            .map_err(CrawlExecutionRepositoryError::database)?,
    )?;
    let configuration: String = row
        .get(3)
        .map_err(CrawlExecutionRepositoryError::database)?;
    let version: CrawlerVersion = serde_json::from_str(&configuration)
        .map_err(|_| CrawlExecutionRepositoryError::CorruptState)?;
    if stored_id != version_id
        || version.id() != version_id
        || version.crawler_id() != crawler_id
        || version.state() != state
    {
        return Err(CrawlExecutionRepositoryError::CorruptState);
    }
    version
        .validate_semantic_contract()
        .map_err(|_| CrawlExecutionRepositoryError::CorruptState)?;
    Ok(version)
}

fn validate_record_input(
    record: &CrawlExecutionRecord,
) -> Result<(), CrawlExecutionRepositoryError> {
    if record.id.as_uuid().get_version_num() != 7
        || record.crawl_run_id.as_uuid().get_version_num() != 7
    {
        return Err(CrawlExecutionRepositoryError::InvalidInput(
            "execution identity is not a UUIDv7",
        ));
    }
    validate_http_url(&record.requested_url)?;
    validate_http_url(&record.canonical_url)?;
    if let Some(final_url) = record.observed_final_url.as_deref() {
        validate_http_url(final_url)?;
    }
    if let Some(media_type) = record.media_type.as_deref()
        && (media_type.is_empty()
            || media_type.trim() != media_type
            || media_type.chars().count() > MAX_EXECUTION_MEDIA_TYPE_CHARS
            || media_type.chars().any(char::is_control))
    {
        return Err(CrawlExecutionRepositoryError::InvalidInput(
            "media type is outside its bounded normalized form",
        ));
    }
    if let Some(discovered_url_id) = record.discovered_url_id.as_deref()
        && (discovered_url_id.is_empty()
            || discovered_url_id.chars().count() > MAX_DISCOVERED_URL_ID_CHARS
            || discovered_url_id.chars().any(char::is_control))
    {
        return Err(CrawlExecutionRepositoryError::InvalidInput(
            "discovered URL identity is invalid",
        ));
    }
    if record
        .http_status
        .is_some_and(|status| !(100..=599).contains(&status))
    {
        return Err(CrawlExecutionRepositoryError::InvalidInput(
            "HTTP status is outside the valid range",
        ));
    }
    validate_outcome_error(record.outcome, record.error_code)?;
    let mut kinds = BTreeSet::new();
    let mut artifacts = HashSet::new();
    for artifact in &record.artifacts {
        if !kinds.insert(artifact.kind) || !artifacts.insert(artifact.artifact_id) {
            return Err(CrawlExecutionRepositoryError::InvalidReference);
        }
    }
    Ok(())
}

fn validate_http_url(value: &str) -> Result<(), CrawlExecutionRepositoryError> {
    if value.is_empty()
        || value.chars().count() > MAX_EXECUTION_URL_CHARS
        || value.chars().any(char::is_control)
    {
        return Err(CrawlExecutionRepositoryError::InvalidInput(
            "URL is outside its bounded form",
        ));
    }
    let parsed = Url::parse(value).map_err(|_| {
        CrawlExecutionRepositoryError::InvalidInput("URL is not valid HTTP(S) syntax")
    })?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err(CrawlExecutionRepositoryError::InvalidInput(
            "URL is not a valid crawl identity",
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

fn validate_outcome_error(
    outcome: CrawlExecutionOutcome,
    error_code: Option<CrawlExecutionErrorCode>,
) -> Result<(), CrawlExecutionRepositoryError> {
    let valid = match (outcome, error_code) {
        (CrawlExecutionOutcome::Completed, None)
        | (CrawlExecutionOutcome::Partial, Some(CrawlExecutionErrorCode::PartialResult))
        | (CrawlExecutionOutcome::Cancelled, Some(CrawlExecutionErrorCode::Cancelled)) => true,
        (CrawlExecutionOutcome::Failed, Some(code)) => !matches!(
            code,
            CrawlExecutionErrorCode::PartialResult | CrawlExecutionErrorCode::Cancelled
        ),
        _ => false,
    };
    valid
        .then_some(())
        .ok_or(CrawlExecutionRepositoryError::InvalidInput(
            "outcome and error code are incoherent",
        ))
}

fn validate_summary(summary: &CrawlExecutionSummary) -> Result<(), CrawlExecutionRepositoryError> {
    if summary.crawl_run_id.as_uuid().get_version_num() != 7 {
        return Err(CrawlExecutionRepositoryError::InvalidInput(
            "summary run identity is not a UUIDv7",
        ));
    }
    if summary.in_scope_pages_completed > summary.in_scope_pages_planned {
        return Err(CrawlExecutionRepositoryError::CompletedExceedsPlanned);
    }
    Ok(())
}

fn summary_sql_values(
    summary: &CrawlExecutionSummary,
) -> Result<[i64; 5], CrawlExecutionRepositoryError> {
    validate_summary(summary)?;
    Ok([
        checked_counter(summary.in_scope_pages_planned)?,
        checked_counter(summary.in_scope_pages_completed)?,
        checked_counter(summary.pagination_truncation_count)?,
        checked_counter(summary.unresolved_partial_work_count)?,
        checked_counter(summary.page_type_ambiguity_count)?,
    ])
}

fn checked_counter(value: u64) -> Result<i64, CrawlExecutionRepositoryError> {
    i64::try_from(value).map_err(|_| CrawlExecutionRepositoryError::CounterOutOfRange)
}

fn map_run_error(error: CrawlRunRepositoryError) -> CrawlExecutionRepositoryError {
    match error {
        CrawlRunRepositoryError::NotFound => CrawlExecutionRepositoryError::NotFound,
        CrawlRunRepositoryError::Database(error) => CrawlExecutionRepositoryError::Database(error),
    }
}

fn summary_from_row(row: &Row) -> Result<CrawlExecutionSummary, CrawlExecutionRepositoryError> {
    let summary = CrawlExecutionSummary {
        crawl_run_id: parse_run_id(
            &row.get::<String>(0)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?,
        in_scope_pages_planned: parse_counter(
            row.get(1)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?,
        in_scope_pages_completed: parse_counter(
            row.get(2)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?,
        pagination_truncation_count: parse_counter(
            row.get(3)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?,
        unresolved_partial_work_count: parse_counter(
            row.get(4)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?,
        page_type_ambiguity_count: parse_counter(
            row.get(5)
                .map_err(CrawlExecutionRepositoryError::database)?,
        )?,
    };
    validate_summary(&summary).map_err(|error| match error {
        CrawlExecutionRepositoryError::CompletedExceedsPlanned
        | CrawlExecutionRepositoryError::CounterOutOfRange
        | CrawlExecutionRepositoryError::InvalidInput(_) => {
            CrawlExecutionRepositoryError::CorruptState
        }
        other => other,
    })?;
    Ok(summary)
}

fn parse_optional_counter(
    value: Option<i64>,
) -> Result<Option<u64>, CrawlExecutionRepositoryError> {
    value
        .map(|value| u64::try_from(value).map_err(|_| CrawlExecutionRepositoryError::CorruptState))
        .transpose()
}

fn parse_counter(value: i64) -> Result<u64, CrawlExecutionRepositoryError> {
    u64::try_from(value).map_err(|_| CrawlExecutionRepositoryError::CorruptState)
}

fn parse_optional_status(value: Option<i64>) -> Result<Option<u16>, CrawlExecutionRepositoryError> {
    value
        .map(|value| {
            let status =
                u16::try_from(value).map_err(|_| CrawlExecutionRepositoryError::CorruptState)?;
            (100..=599)
                .contains(&status)
                .then_some(status)
                .ok_or(CrawlExecutionRepositoryError::CorruptState)
        })
        .transpose()
}

fn row_exists(
    connection: &impl crate::SqliteExecutor,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<bool, CrawlExecutionRepositoryError> {
    let mut rows = connection
        .query(sql, params)
        .map_err(CrawlExecutionRepositoryError::database)?;
    Ok(rows
        .next()
        .map_err(CrawlExecutionRepositoryError::database)?
        .is_some())
}

fn optional_text(value: Option<&str>) -> SqliteValue {
    value.map_or(SqliteValue::Null, |value| {
        SqliteValue::Text(value.to_owned())
    })
}

fn optional_id<T: ToString>(value: Option<T>) -> SqliteValue {
    value.map_or(SqliteValue::Null, |value| {
        SqliteValue::Text(value.to_string())
    })
}

fn optional_i64(value: Option<i64>) -> SqliteValue {
    value.map_or(SqliteValue::Null, SqliteValue::Integer)
}

fn optional_counter(value: Option<u64>) -> Result<SqliteValue, CrawlExecutionRepositoryError> {
    value
        .map(checked_counter)
        .transpose()
        .map(|value| value.map_or(SqliteValue::Null, SqliteValue::Integer))
}

fn optional_error_code(value: Option<CrawlExecutionErrorCode>) -> SqliteValue {
    value.map_or(SqliteValue::Null, |value| {
        SqliteValue::Text(error_code_name(value).to_owned())
    })
}

fn outcome_name(value: CrawlExecutionOutcome) -> &'static str {
    match value {
        CrawlExecutionOutcome::Completed => "COMPLETED",
        CrawlExecutionOutcome::Partial => "PARTIAL",
        CrawlExecutionOutcome::Failed => "FAILED",
        CrawlExecutionOutcome::Cancelled => "CANCELLED",
    }
}

fn work_state_name(state: CrawlWorkState) -> &'static str {
    match state {
        CrawlWorkState::Pending => "PENDING",
        CrawlWorkState::Running => "RUNNING",
        CrawlWorkState::Completed => "COMPLETED",
        CrawlWorkState::Partial => "PARTIAL",
        CrawlWorkState::Failed => "FAILED",
        CrawlWorkState::Cancelled => "CANCELLED",
    }
}

fn outcome_matches_work_state(outcome: CrawlExecutionOutcome, state: CrawlWorkState) -> bool {
    matches!(
        (outcome, state),
        (CrawlExecutionOutcome::Completed, CrawlWorkState::Completed)
            | (CrawlExecutionOutcome::Partial, CrawlWorkState::Partial)
            | (CrawlExecutionOutcome::Failed, CrawlWorkState::Failed)
            | (CrawlExecutionOutcome::Cancelled, CrawlWorkState::Cancelled)
    )
}

fn error_code_name(value: CrawlExecutionErrorCode) -> &'static str {
    match value {
        CrawlExecutionErrorCode::AccessDenied => "ACCESS_DENIED",
        CrawlExecutionErrorCode::NotFound => "NOT_FOUND",
        CrawlExecutionErrorCode::Timeout => "TIMEOUT",
        CrawlExecutionErrorCode::ProviderUnavailable => "PROVIDER_UNAVAILABLE",
        CrawlExecutionErrorCode::InvalidResponse => "INVALID_RESPONSE",
        CrawlExecutionErrorCode::RateLimited => "RATE_LIMITED",
        CrawlExecutionErrorCode::RemoteFailure => "REMOTE_FAILURE",
        CrawlExecutionErrorCode::UnsupportedCapability => "UNSUPPORTED_CAPABILITY",
        CrawlExecutionErrorCode::PartialResult => "PARTIAL_RESULT",
        CrawlExecutionErrorCode::Cancelled => "CANCELLED",
        CrawlExecutionErrorCode::RobotsExcluded => "ROBOTS_EXCLUDED",
        CrawlExecutionErrorCode::PageTypeAmbiguous => "PAGE_TYPE_AMBIGUOUS",
        CrawlExecutionErrorCode::StoragePressure => "STORAGE_PRESSURE",
    }
}

fn artifact_kind_name(value: CrawlExecutionArtifactKind) -> &'static str {
    match value {
        CrawlExecutionArtifactKind::RawHtml => "RAW_HTML",
        CrawlExecutionArtifactKind::CleanedHtml => "CLEANED_HTML",
        CrawlExecutionArtifactKind::RenderedHtml => "RENDERED_HTML",
        CrawlExecutionArtifactKind::Markdown => "MARKDOWN",
        CrawlExecutionArtifactKind::Screenshot => "SCREENSHOT",
    }
}

fn parse_outcome(value: &str) -> Result<CrawlExecutionOutcome, CrawlExecutionRepositoryError> {
    match value {
        "COMPLETED" => Ok(CrawlExecutionOutcome::Completed),
        "PARTIAL" => Ok(CrawlExecutionOutcome::Partial),
        "FAILED" => Ok(CrawlExecutionOutcome::Failed),
        "CANCELLED" => Ok(CrawlExecutionOutcome::Cancelled),
        _ => Err(CrawlExecutionRepositoryError::CorruptState),
    }
}

fn parse_optional_error_code(
    value: Option<String>,
) -> Result<Option<CrawlExecutionErrorCode>, CrawlExecutionRepositoryError> {
    value
        .map(|value| match value.as_str() {
            "ACCESS_DENIED" => Ok(CrawlExecutionErrorCode::AccessDenied),
            "NOT_FOUND" => Ok(CrawlExecutionErrorCode::NotFound),
            "TIMEOUT" => Ok(CrawlExecutionErrorCode::Timeout),
            "PROVIDER_UNAVAILABLE" => Ok(CrawlExecutionErrorCode::ProviderUnavailable),
            "INVALID_RESPONSE" => Ok(CrawlExecutionErrorCode::InvalidResponse),
            "RATE_LIMITED" => Ok(CrawlExecutionErrorCode::RateLimited),
            "REMOTE_FAILURE" => Ok(CrawlExecutionErrorCode::RemoteFailure),
            "UNSUPPORTED_CAPABILITY" => Ok(CrawlExecutionErrorCode::UnsupportedCapability),
            "PARTIAL_RESULT" => Ok(CrawlExecutionErrorCode::PartialResult),
            "CANCELLED" => Ok(CrawlExecutionErrorCode::Cancelled),
            "ROBOTS_EXCLUDED" => Ok(CrawlExecutionErrorCode::RobotsExcluded),
            "PAGE_TYPE_AMBIGUOUS" => Ok(CrawlExecutionErrorCode::PageTypeAmbiguous),
            "STORAGE_PRESSURE" => Ok(CrawlExecutionErrorCode::StoragePressure),
            _ => Err(CrawlExecutionRepositoryError::CorruptState),
        })
        .transpose()
}

fn parse_artifact_kind(
    value: &str,
) -> Result<CrawlExecutionArtifactKind, CrawlExecutionRepositoryError> {
    match value {
        "RAW_HTML" => Ok(CrawlExecutionArtifactKind::RawHtml),
        "CLEANED_HTML" => Ok(CrawlExecutionArtifactKind::CleanedHtml),
        "RENDERED_HTML" => Ok(CrawlExecutionArtifactKind::RenderedHtml),
        "MARKDOWN" => Ok(CrawlExecutionArtifactKind::Markdown),
        "SCREENSHOT" => Ok(CrawlExecutionArtifactKind::Screenshot),
        _ => Err(CrawlExecutionRepositoryError::CorruptState),
    }
}

fn parse_run_type(value: &str) -> Result<CrawlRunType, CrawlExecutionRepositoryError> {
    match value {
        "QUICK_SCRAPE" => Ok(CrawlRunType::QuickScrape),
        "TEST_RUN" => Ok(CrawlRunType::TestRun),
        "DISCOVERY_PREVIEW" => Ok(CrawlRunType::DiscoveryPreview),
        "PRODUCTION_RUN" => Ok(CrawlRunType::ProductionRun),
        _ => Err(CrawlExecutionRepositoryError::CorruptState),
    }
}

fn parse_version_state(value: &str) -> Result<CrawlerVersionState, CrawlExecutionRepositoryError> {
    match value {
        "DRAFT" => Ok(CrawlerVersionState::Draft),
        "PUBLISHED" => Ok(CrawlerVersionState::Published),
        _ => Err(CrawlExecutionRepositoryError::CorruptState),
    }
}

fn parse_execution_id(value: &str) -> Result<CrawlExecutionId, CrawlExecutionRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(CrawlExecutionId::from_uuid)
        .ok_or(CrawlExecutionRepositoryError::CorruptState)
}

fn parse_run_id(value: &str) -> Result<CrawlRunId, CrawlExecutionRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(CrawlRunId::from_uuid)
        .ok_or(CrawlExecutionRepositoryError::CorruptState)
}

fn parse_crawler_id(value: &str) -> Result<CrawlerId, CrawlExecutionRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(CrawlerId::from_uuid)
        .ok_or(CrawlExecutionRepositoryError::CorruptState)
}

fn parse_version_id(value: &str) -> Result<CrawlerVersionId, CrawlExecutionRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(CrawlerVersionId::from_uuid)
        .ok_or(CrawlExecutionRepositoryError::CorruptState)
}

fn parse_page_type_id(value: &str) -> Result<PageTypeId, CrawlExecutionRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(PageTypeId::from_uuid)
        .ok_or(CrawlExecutionRepositoryError::CorruptState)
}

fn parse_transition_id(
    value: &str,
) -> Result<DiscoveryTransitionId, CrawlExecutionRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(DiscoveryTransitionId::from_uuid)
        .ok_or(CrawlExecutionRepositoryError::CorruptState)
}

fn parse_source_id(value: &str) -> Result<SourceId, CrawlExecutionRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(SourceId::from_uuid)
        .ok_or(CrawlExecutionRepositoryError::CorruptState)
}

fn parse_artifact_id(
    value: &str,
) -> Result<erabi_domain::ArtifactId, CrawlExecutionRepositoryError> {
    Uuid::parse_str(value)
        .ok()
        .and_then(erabi_domain::ArtifactId::from_uuid)
        .ok_or(CrawlExecutionRepositoryError::CorruptState)
}

fn parse_optional_source_id(
    value: Option<String>,
) -> Result<Option<SourceId>, CrawlExecutionRepositoryError> {
    value.map(|value| parse_source_id(&value)).transpose()
}

fn parse_optional_page_type_id(
    value: Option<String>,
) -> Result<Option<PageTypeId>, CrawlExecutionRepositoryError> {
    value.map(|value| parse_page_type_id(&value)).transpose()
}

fn parse_optional_transition_id(
    value: Option<String>,
) -> Result<Option<DiscoveryTransitionId>, CrawlExecutionRepositoryError> {
    value.map(|value| parse_transition_id(&value)).transpose()
}
