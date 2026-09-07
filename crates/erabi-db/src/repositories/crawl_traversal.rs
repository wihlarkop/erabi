//! Authoritative, bounded logical crawl work for Task 9 recovery.
//!
//! This repository stores semantic decisions made by `SemanticTraversal`; it
//! deliberately does not canonicalize URLs or select `PageType` transitions.

use erabi_domain::CrawlRunId;
use turso::{
    Value, params_from_iter,
    transaction::{Transaction, TransactionBehavior},
};

use crate::{DbError, ErabiDatabase};

use super::{
    CheckpointEnvelope, CheckpointRecord, CheckpointRepository, CheckpointRepositoryError, JobId,
    JobLease,
    checkpoint::append_in_transaction,
    run::{CrawlRunRepositoryError, DiscoveredUrlRecord, record_discovered_url_in_transaction},
};

const MAX_URL_CHARS: usize = 4_096;
type ControlSqlValues = (
    String,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CrawlAdmissionState {
    Admitted,
    PreserveOnly,
    Resolved,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CrawlWorkState {
    Pending,
    Running,
    Completed,
    Partial,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CrawlRecoveryActionKind {
    Retry,
    RetryFailedParts,
    RestartFromBeginning,
}

impl CrawlRecoveryActionKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Retry => "RETRY",
            Self::RetryFailedParts => "RETRY_FAILED_PARTS",
            Self::RestartFromBeginning => "RESTART_FROM_BEGINNING",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CrawlPageTypeMatchState {
    Matched,
    Unmatched,
    Ambiguous,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlUrlStateRecord {
    pub id: String,
    pub crawl_run_id: CrawlRunId,
    pub canonical_url: String,
    pub first_discovered_url_id: Option<String>,
    pub requested_url: String,
    pub parent_url_state_id: Option<String>,
    pub parent_discovered_url_id: Option<String>,
    pub admission_state: CrawlAdmissionState,
    pub preserve_reason: Option<String>,
    pub resolved_to_url_state_id: Option<String>,
    pub admission_sequence: Option<u64>,
    pub depth: Option<u32>,
    pub target_page_type_id: Option<String>,
    pub transition_id: Option<String>,
    pub pagination: bool,
    pub final_canonical_url: Option<String>,
    pub current_work_state: Option<CrawlWorkState>,
    pub work_generation: u64,
    pub current_execution_id: Option<String>,
    pub seed_provenance: Vec<String>,
    pub seen: bool,
    pub sampled: bool,
    pub expanded: bool,
    pub in_scope: bool,
    pub page_type_match_state: Option<CrawlPageTypeMatchState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlTraversalControl {
    pub crawl_run_id: CrawlRunId,
    pub consumed_bytes: u64,
    pub raw_link_count: u64,
    pub duplicate_count: u64,
    pub robots_excluded_count: u64,
    pub provider_error_count: u64,
    pub external_url_count: u64,
    pub blocked_url_count: u64,
    pub peak_expansion_count: u64,
    pub elapsed_millis: u64,
    pub time_budget_hit: bool,
    pub duration_work_not_expanded: bool,
    pub pagination_truncation_count: u64,
    pub next_admission_sequence: u64,
}

/// One source-page contribution to an accepted transition budget. The source
/// is the logical URL-state identity, never a timestamp or execution UUID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlTransitionSourceCount {
    pub transition_id: String,
    pub source_url_state_id: String,
    pub eligible_edge_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlTraversalUrlSemanticState {
    pub canonical_url: String,
    pub sampled: bool,
    pub expanded: bool,
    pub in_scope: bool,
    pub page_type_match_state: Option<CrawlPageTypeMatchState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlTraversalPageTypeCounts {
    pub page_type_id: String,
    pub sampled_count: u64,
    pub discovered_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlTraversalSemanticProjection {
    pub url_states: Vec<CrawlTraversalUrlSemanticState>,
    pub page_type_counts: Vec<CrawlTraversalPageTypeCounts>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlRedirectReconciliation {
    pub alias_url_state_id: String,
    pub final_url_state_id: String,
    pub alias_canonical_url: String,
    pub final_canonical_url: String,
}

/// A provider call whose semantic discovery result is being committed before
/// its append-only execution row. The logical state is marked RUNNING in the
/// same discovery transaction so recovery cannot mistake sampled work for a
/// fresh pending frontier item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlInFlightWork {
    pub state_id: String,
    pub expected_work_generation: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconstructedTraversalState {
    pub control: CrawlTraversalControl,
    /// Ordered by semantic recovery order: depth, admission sequence,
    /// canonical URL, requested URL. Never by UUID or insertion time.
    pub work: Vec<CrawlUrlStateRecord>,
    pub transition_source_counts: Vec<CrawlTransitionSourceCount>,
    pub page_type_counts: Vec<CrawlTraversalPageTypeCounts>,
}

/// The durable executable selection for one logical recovery action. The
/// selection is keyed by the action job, while the current state rows remain
/// authoritative for whether an already-selected unit still needs provider
/// work after a crash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlRecoveryActionSelection {
    pub action_job_id: String,
    pub crawl_run_id: CrawlRunId,
    pub action_kind: CrawlRecoveryActionKind,
    pub state_ids: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum CrawlTraversalRepositoryError {
    #[error("the Crawl Run was not found")]
    CrawlRunNotFound,
    #[error("the durable crawl traversal state is invalid")]
    InvalidState,
    #[error("the durable crawl traversal state is corrupt")]
    CorruptState,
    #[error("the durable crawl traversal database operation failed")]
    Database(#[source] DbError),
    #[error("the durable crawl traversal checkpoint operation failed")]
    Checkpoint(#[source] CheckpointRepositoryError),
    #[error("the durable crawl traversal discovery operation failed")]
    Discovery(#[source] CrawlRunRepositoryError),
}

#[derive(Clone, Copy, Debug)]
pub struct CrawlTraversalRepository<'database> {
    database: &'database ErabiDatabase,
}

impl<'database> CrawlTraversalRepository<'database> {
    #[must_use]
    pub const fn new(database: &'database ErabiDatabase) -> Self {
        Self { database }
    }

    /// Atomically installs all initial logical work, exact seed provenance,
    /// and the scalar control row. Callers append the compact generic
    /// checkpoint in the same safe boundary before provider IO.
    ///
    /// # Errors
    /// Returns a typed validation, foreign-key, or transaction error and
    /// commits none of the initialization facts on failure.
    pub async fn initialize_run_state(
        &self,
        run_id: CrawlRunId,
        roots: &[CrawlUrlStateRecord],
        control: &CrawlTraversalControl,
    ) -> Result<(), CrawlTraversalRepositoryError> {
        if control.crawl_run_id != run_id || roots.is_empty() {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        let mut connection = self.database.connection().await.map_err(db)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(db)?;
        let result = async {
            ensure_run(&tx, run_id).await?;
            let initialized = tx
                .prepare(
                    "SELECT EXISTS(SELECT 1 FROM crawl_traversal_control WHERE crawl_run_id = ?1)",
                )
                .await
                .map_err(db)?
                .query_row([run_id.to_string()])
                .await
                .map_err(db)?
                .get::<i64>(0)
                .map_err(db)?
                == 1;
            if initialized {
                if read_control(&tx, run_id).await? != *control {
                    return Err(CrawlTraversalRepositoryError::InvalidState);
                }
                for root in roots {
                    let Some(persisted) =
                        read_state_by_canonical(&tx, run_id, &root.canonical_url).await?
                    else {
                        return Err(CrawlTraversalRepositoryError::InvalidState);
                    };
                    if !initial_state_matches(&persisted, root) {
                        return Err(CrawlTraversalRepositoryError::InvalidState);
                    }
                }
                return Ok(());
            }
            insert_control(&tx, control).await?;
            for root in roots {
                validate_state_ownership(&tx, root, run_id).await?;
                insert_state(&tx, root).await?;
            }
            Ok(())
        }
        .await;
        finish(tx, result).await
    }

    /// Atomically installs first logical work, scalar control, and the compact
    /// compatible checkpoint under the current verified job attempt and lease.
    ///
    /// # Errors
    /// Returns a typed validation, lease, checkpoint, or transaction error;
    /// roots and recovery control are rolled back together on failure.
    #[allow(clippy::too_many_arguments)]
    pub async fn initialize_run_state_with_checkpoint(
        &self,
        run_id: CrawlRunId,
        roots: &[CrawlUrlStateRecord],
        control: &CrawlTraversalControl,
        job_id: &JobId,
        attempt_id: &str,
        lease: &JobLease,
        checkpoint: &CheckpointEnvelope,
        created_at: i64,
    ) -> Result<CheckpointRecord, CrawlTraversalRepositoryError> {
        if control.crawl_run_id != run_id || roots.is_empty() {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        let mut connection = self.database.connection().await.map_err(db)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(db)?;
        let result = async {
            ensure_run(&tx, run_id).await?;
            insert_control(&tx, control).await?;
            for root in roots {
                validate_state_ownership(&tx, root, run_id).await?;
                insert_state(&tx, root).await?;
            }
            append_in_transaction(&tx, job_id, attempt_id, lease, checkpoint, created_at)
                .await
                .map_err(CrawlTraversalRepositoryError::Checkpoint)
        }
        .await;
        finish(tx, result).await
    }

    /// Atomically initializes the logical work projection, exact discovery
    /// evidence, semantic sets, scalar control, and compact checkpoint. The
    /// evidence is supplied by the semantic traversal and is replay-safe by
    /// its carried identity.
    ///
    /// # Errors
    /// Returns a typed validation, checkpoint, foreign-key, or transaction
    /// error without exposing partial initialization state.
    #[allow(clippy::too_many_arguments)]
    pub async fn initialize_run_state_with_checkpoint_and_evidence(
        &self,
        run_id: CrawlRunId,
        roots: &[CrawlUrlStateRecord],
        evidence: &[DiscoveredUrlRecord],
        control: &CrawlTraversalControl,
        semantic_projection: &CrawlTraversalSemanticProjection,
        job_id: &JobId,
        attempt_id: &str,
        lease: &JobLease,
        checkpoint: &CheckpointEnvelope,
        created_at: i64,
    ) -> Result<CheckpointRecord, CrawlTraversalRepositoryError> {
        if control.crawl_run_id != run_id
            || roots.is_empty()
            || evidence.iter().any(|record| record.crawl_run_id != run_id)
        {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        let mut connection = self.database.connection().await.map_err(db)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(db)?;
        let result = async {
            ensure_run(&tx, run_id).await?;
            let initialized = tx
                .prepare(
                    "SELECT EXISTS(SELECT 1 FROM crawl_traversal_control WHERE crawl_run_id = ?1)",
                )
                .await
                .map_err(db)?
                .query_row([run_id.to_string()])
                .await
                .map_err(db)?
                .get::<i64>(0)
                .map_err(db)?
                == 1;
            if initialized {
                let persisted_control = read_control(&tx, run_id).await?;
                if persisted_control != *control {
                    return Err(CrawlTraversalRepositoryError::InvalidState);
                }
                for record in evidence {
                    record_discovered_url_in_transaction(&tx, record)
                        .await
                        .map_err(CrawlTraversalRepositoryError::Discovery)?;
                }
                for root in roots {
                    let Some(persisted) =
                        read_state_by_canonical(&tx, run_id, &root.canonical_url).await?
                    else {
                        return Err(CrawlTraversalRepositoryError::InvalidState);
                    };
                    if !initial_state_matches(&persisted, root) {
                        return Err(CrawlTraversalRepositoryError::InvalidState);
                    }
                }
                return Ok(None);
            }
            for record in evidence {
                record_discovered_url_in_transaction(&tx, record)
                    .await
                    .map_err(CrawlTraversalRepositoryError::Discovery)?;
            }
            insert_control(&tx, control).await?;
            for root in roots {
                validate_state_ownership(&tx, root, run_id).await?;
                insert_state(&tx, root).await?;
            }
            apply_semantic_projection(&tx, run_id, semantic_projection).await?;
            append_in_transaction(&tx, job_id, attempt_id, lease, checkpoint, created_at)
                .await
                .map(Some)
                .map_err(CrawlTraversalRepositoryError::Checkpoint)
        }
        .await;
        let checkpoint_record = finish(tx, result).await?;
        if let Some(checkpoint_record) = checkpoint_record {
            return Ok(checkpoint_record);
        }
        CheckpointRepository::new(self.database)
            .latest(job_id)
            .await
            .map_err(CrawlTraversalRepositoryError::Checkpoint)?
            .ok_or(CrawlTraversalRepositoryError::InvalidState)
    }

    /// Records one semantic discovery boundary. The records supplied here are
    /// already classified by `SemanticTraversal`; the repository only stores
    /// their facts and advances the control row atomically.
    ///
    /// # Errors
    /// Returns a typed validation, foreign-key, or transaction error and
    /// commits none of this discovery boundary on failure.
    pub async fn apply_discovery_delta(
        &self,
        run_id: CrawlRunId,
        new_or_merged_work: &[CrawlUrlStateRecord],
        control: &CrawlTraversalControl,
        transition_source_counts: &[CrawlTransitionSourceCount],
    ) -> Result<(), CrawlTraversalRepositoryError> {
        if control.crawl_run_id != run_id {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        let mut connection = self.database.connection().await.map_err(db)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(db)?;
        let result = async {
            ensure_run(&tx, run_id).await?;
            for state in new_or_merged_work {
                validate_state_ownership(&tx, state, run_id).await?;
                upsert_discovery_state(&tx, state).await?;
            }
            update_control(&tx, control).await?;
            replace_transition_counts(&tx, run_id, transition_source_counts).await
        }
        .await;
        finish(tx, result).await
    }

    /// Atomically appends immutable discovery evidence and advances the
    /// logical-work/control projection decided by `SemanticTraversal`.
    ///
    /// # Errors
    /// Returns a typed evidence, validation, foreign-key, or transaction
    /// error and commits neither evidence nor logical work on failure.
    pub async fn apply_discovery_delta_with_evidence(
        &self,
        run_id: CrawlRunId,
        evidence: &[DiscoveredUrlRecord],
        new_or_merged_work: &[CrawlUrlStateRecord],
        control: &CrawlTraversalControl,
        transition_source_counts: &[CrawlTransitionSourceCount],
    ) -> Result<(), CrawlTraversalRepositoryError> {
        if control.crawl_run_id != run_id
            || evidence.iter().any(|record| record.crawl_run_id != run_id)
        {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        let mut connection = self.database.connection().await.map_err(db)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(db)?;
        let result = async {
            ensure_run(&tx, run_id).await?;
            for record in evidence {
                record_discovered_url_in_transaction(&tx, record)
                    .await
                    .map_err(CrawlTraversalRepositoryError::Discovery)?;
            }
            for state in new_or_merged_work {
                validate_state_ownership(&tx, state, run_id).await?;
                upsert_discovery_state(&tx, state).await?;
            }
            update_control(&tx, control).await?;
            replace_transition_counts(&tx, run_id, transition_source_counts).await
        }
        .await;
        finish(tx, result).await
    }

    /// Applies evidence, logical work, semantic state, redirect resolution,
    /// scalar control, and transition budgets as one discovery transaction.
    /// The semantic projection is supplied by `SemanticTraversal`; this
    /// repository never derives a semantic decision of its own.
    ///
    /// # Errors
    /// Returns a typed validation, evidence, foreign-key, or transaction
    /// error and commits none of the discovery boundary on failure.
    #[allow(clippy::too_many_arguments)]
    pub async fn apply_discovery_delta_with_projection(
        &self,
        run_id: CrawlRunId,
        evidence: &[DiscoveredUrlRecord],
        new_or_merged_work: &[CrawlUrlStateRecord],
        control: &CrawlTraversalControl,
        transition_source_counts: &[CrawlTransitionSourceCount],
        semantic_projection: &CrawlTraversalSemanticProjection,
        redirects: &[CrawlRedirectReconciliation],
        in_flight_work: &[CrawlInFlightWork],
    ) -> Result<(), CrawlTraversalRepositoryError> {
        if control.crawl_run_id != run_id
            || evidence.iter().any(|record| record.crawl_run_id != run_id)
        {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        let mut connection = self.database.connection().await.map_err(db)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(db)?;
        let result = async {
            ensure_run(&tx, run_id).await?;
            for record in evidence {
                record_discovered_url_in_transaction(&tx, record)
                    .await
                    .map_err(CrawlTraversalRepositoryError::Discovery)?;
            }
            for state in new_or_merged_work {
                validate_state_ownership(&tx, state, run_id).await?;
                upsert_discovery_state(&tx, state).await?;
            }
            // A redirect's final logical state may be newly admitted in this
            // same delta. Materialize that state before resolving the alias;
            // both mutations remain part of this transaction.
            for redirect in redirects {
                reconcile_redirect(&tx, run_id, redirect).await?;
            }
            apply_semantic_projection(&tx, run_id, semantic_projection).await?;
            mark_in_flight_work(&tx, run_id, in_flight_work).await?;
            update_control(&tx, control).await?;
            replace_transition_counts(&tx, run_id, transition_source_counts).await
        }
        .await;
        finish(tx, result).await
    }

    /// Replaces the compact per-source transition budget projection from the
    /// current semantic traversal state. It is called at the same safe
    /// boundary as an already durable discovery delta.
    ///
    /// # Errors
    /// Returns a typed validation or database error without leaving a partial
    /// transition-counter replacement visible.
    pub async fn replace_transition_source_counts(
        &self,
        run_id: CrawlRunId,
        counts: &[CrawlTransitionSourceCount],
    ) -> Result<(), CrawlTraversalRepositoryError> {
        let mut connection = self.database.connection().await.map_err(db)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(db)?;
        let result = async {
            ensure_run(&tx, run_id).await?;
            replace_transition_counts(&tx, run_id, counts).await
        }
        .await;
        finish(tx, result).await
    }

    /// Returns the deterministic recovery model. Historical execution rows
    /// are not sorted or ranked here: `current_work_state` and
    /// `current_execution_id` are the semantic current answer.
    ///
    /// # Errors
    /// Returns a typed read/corruption error when authoritative control or
    /// logical work cannot be reconstructed coherently.
    pub async fn reconstruct_recovery_state(
        &self,
        run_id: CrawlRunId,
    ) -> Result<ReconstructedTraversalState, CrawlTraversalRepositoryError> {
        let connection = self.database.connection().await.map_err(db)?;
        let control = read_control(&connection, run_id).await?;
        let mut rows = connection.query(
            "SELECT id, canonical_url, first_discovered_url_id, requested_url, parent_url_state_id, parent_discovered_url_id, admission_state, preserve_reason, resolved_to_url_state_id, admission_sequence, depth, target_page_type_id, transition_id, pagination, final_canonical_url, current_work_state, work_generation, current_execution_id, seen, sampled, expanded, in_scope, page_type_match_state FROM crawl_url_state WHERE crawl_run_id = ?1 ORDER BY depth ASC NULLS LAST, admission_sequence ASC NULLS LAST, canonical_url COLLATE BINARY ASC, requested_url COLLATE BINARY ASC",
            [run_id.to_string()],
        ).await.map_err(db)?;
        let mut work = Vec::new();
        while let Some(row) = rows.next().await.map_err(db)? {
            let id: String = row.get(0).map_err(db)?;
            let state = CrawlUrlStateRecord {
                id: id.clone(),
                crawl_run_id: run_id,
                canonical_url: row.get(1).map_err(db)?,
                first_discovered_url_id: row.get(2).map_err(db)?,
                requested_url: row.get(3).map_err(db)?,
                parent_url_state_id: row.get(4).map_err(db)?,
                parent_discovered_url_id: row.get(5).map_err(db)?,
                admission_state: admission_from_db(&row.get::<String>(6).map_err(db)?)?,
                preserve_reason: row.get(7).map_err(db)?,
                resolved_to_url_state_id: row.get(8).map_err(db)?,
                admission_sequence: optional_u64(row.get(9).map_err(db)?)?,
                depth: optional_u32(row.get(10).map_err(db)?)?,
                target_page_type_id: row.get(11).map_err(db)?,
                transition_id: row.get(12).map_err(db)?,
                pagination: row.get::<i64>(13).map_err(db)? == 1,
                final_canonical_url: row.get(14).map_err(db)?,
                current_work_state: optional_work_state(row.get(15).map_err(db)?)?,
                work_generation: as_u64(row.get::<i64>(16).map_err(db)?)?,
                current_execution_id: row.get(17).map_err(db)?,
                seed_provenance: read_seed_provenance(&connection, &id).await?,
                seen: row.get::<i64>(18).map_err(db)? == 1,
                sampled: row.get::<i64>(19).map_err(db)? == 1,
                expanded: row.get::<i64>(20).map_err(db)? == 1,
                in_scope: row.get::<i64>(21).map_err(db)? == 1,
                page_type_match_state: optional_page_type_match_state(row.get(22).map_err(db)?)?,
            };
            validate_state(&state)?;
            work.push(state);
        }
        let transition_source_counts = read_transition_counts(&connection, run_id).await?;
        let page_type_counts = read_page_type_counts(&connection, run_id).await?;
        Ok(ReconstructedTraversalState {
            control,
            work,
            transition_source_counts,
            page_type_counts,
        })
    }

    /// Prepares the executable selection for one recovery action. The action
    /// job is the durable logical identity: its first preparation records the
    /// exact state IDs and generation transitions in one transaction, while a
    /// replay validates and returns that same selection without incrementing a
    /// generation again.
    ///
    /// # Errors
    /// Returns a typed ownership, lineage, corruption, or transaction error
    /// without exposing a partially advanced selection.
    #[allow(clippy::too_many_lines)]
    pub async fn prepare_recovery_action(
        &self,
        action_job_id: &JobId,
        action_attempt_id: &str,
        run_id: CrawlRunId,
        action_kind: CrawlRecoveryActionKind,
        now: i64,
    ) -> Result<CrawlRecoveryActionSelection, CrawlTraversalRepositoryError> {
        if action_job_id.as_str().is_empty() || action_attempt_id.is_empty() || now < 0 {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        let mut connection = self.database.connection().await.map_err(db)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(db)?;
        let result = async {
            let action = tx
                .prepare(
                    "SELECT action_job.parent_job_id, action_job.crawl_run_id, action_job.kind, action_job.state, action_job.current_attempt, action_job.lease_id, action_job.lease_generation, action_job.lease_expires_at, attempt.outcome, attempt.attempt_number, attempt.lease_id, attempt.lease_generation FROM jobs AS action_job JOIN job_attempts AS attempt ON attempt.job_id = action_job.id WHERE action_job.id = ?1 AND attempt.id = ?2",
                )
                .await
                .map_err(db)?
                .query_row((action_job_id.as_str(), action_attempt_id))
                .await
                .map_err(|error| match error {
                    turso::Error::QueryReturnedNoRows => {
                        CrawlTraversalRepositoryError::InvalidState
                    }
                    other => db(other),
                })?;
            let source_job_id: Option<String> = action.get(0).map_err(db)?;
            let source_job_id = source_job_id.ok_or(CrawlTraversalRepositoryError::InvalidState)?;
            let action_run_id: Option<String> = action.get(1).map_err(db)?;
            let kind: String = action.get(2).map_err(db)?;
            let job_state: String = action.get(3).map_err(db)?;
            let current_attempt: i64 = action.get(4).map_err(db)?;
            let lease_id: Option<String> = action.get(5).map_err(db)?;
            let lease_generation: i64 = action.get(6).map_err(db)?;
            let lease_expires_at: Option<i64> = action.get(7).map_err(db)?;
            let attempt_outcome: String = action.get(8).map_err(db)?;
            let attempt_number: i64 = action.get(9).map_err(db)?;
            let attempt_lease_id: String = action.get(10).map_err(db)?;
            let attempt_lease_generation: i64 = action.get(11).map_err(db)?;
            if action_run_id.as_deref() != Some(run_id.to_string().as_str())
                || kind != action_kind.as_str()
                || job_state != "RUNNING"
                || current_attempt != attempt_number
                || lease_id.as_deref() != Some(attempt_lease_id.as_str())
                || lease_generation != attempt_lease_generation
                || attempt_outcome != "RUNNING"
                || lease_expires_at.is_none_or(|expires_at| expires_at <= now)
            {
                return Err(CrawlTraversalRepositoryError::InvalidState);
            }
            let source_run_id: Option<String> = tx
                .prepare("SELECT crawl_run_id FROM jobs WHERE id = ?1")
                .await
                .map_err(db)?
                .query_row([source_job_id.as_str()])
                .await
                .map_err(|error| match error {
                    turso::Error::QueryReturnedNoRows => {
                        CrawlTraversalRepositoryError::InvalidState
                    }
                    other => db(other),
                })?
                .get(0)
                .map_err(db)?;
            if source_run_id.as_deref() != Some(run_id.to_string().as_str()) {
                return Err(CrawlTraversalRepositoryError::InvalidState);
            }

            let existing = tx
                .query(
                    "SELECT source_job_id, crawl_run_id, action_kind FROM crawl_recovery_actions WHERE action_job_id = ?1",
                    [action_job_id.as_str()],
                )
                .await
                .map_err(db)?
                .next()
                .await
                .map_err(db)?;
            if let Some(existing) = existing {
                let existing_source: String = existing.get(0).map_err(db)?;
                let existing_run: String = existing.get(1).map_err(db)?;
                let existing_kind: String = existing.get(2).map_err(db)?;
                if existing_source != source_job_id
                    || existing_run != run_id.to_string()
                    || existing_kind != action_kind.as_str()
                {
                    return Err(CrawlTraversalRepositoryError::InvalidState);
                }
                let state_ids = read_recovery_action_items(&tx, action_job_id, run_id).await?;
                return Ok(CrawlRecoveryActionSelection {
                    action_job_id: action_job_id.to_string(),
                    crawl_run_id: run_id,
                    action_kind,
                    state_ids,
                });
            }

            tx.execute(
                "INSERT INTO crawl_recovery_actions (action_job_id, source_job_id, crawl_run_id, action_kind, prepared_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    action_job_id.as_str(),
                    source_job_id.as_str(),
                    run_id.to_string(),
                    action_kind.as_str(),
                    now,
                ),
            )
            .await
            .map_err(db)?;

            let control_exists = tx
                .prepare(
                    "SELECT EXISTS(SELECT 1 FROM crawl_traversal_control WHERE crawl_run_id = ?1)",
                )
                .await
                .map_err(db)?
                .query_row([run_id.to_string()])
                .await
                .map_err(db)?
                .get::<i64>(0)
                .map_err(db)?
                == 1;
            if !control_exists {
                return Ok(CrawlRecoveryActionSelection {
                    action_job_id: action_job_id.to_string(),
                    crawl_run_id: run_id,
                    action_kind,
                    state_ids: Vec::new(),
                });
            }

            let work_predicate = match action_kind {
                CrawlRecoveryActionKind::Retry => {
                    "current_work_state IN ('PENDING', 'FAILED', 'PARTIAL', 'CANCELLED')"
                }
                CrawlRecoveryActionKind::RetryFailedParts => {
                    "current_work_state IN ('FAILED', 'PARTIAL')"
                }
                CrawlRecoveryActionKind::RestartFromBeginning => "current_work_state IS NOT NULL",
            };
            let mut rows = tx
                .query(
                    format!("SELECT id, work_generation FROM crawl_url_state WHERE crawl_run_id = ?1 AND admission_state = 'ADMITTED' AND {work_predicate} ORDER BY depth ASC NULLS LAST, admission_sequence ASC NULLS LAST, canonical_url COLLATE BINARY ASC, requested_url COLLATE BINARY ASC"),
                    [run_id.to_string()],
                )
                .await
                .map_err(db)?;
            let mut state_ids = Vec::new();
            while let Some(row) = rows.next().await.map_err(db)? {
                let state_id: String = row.get(0).map_err(db)?;
                let previous_generation = as_u64(row.get::<i64>(1).map_err(db)?)?;
                let prepared_generation = previous_generation
                    .checked_add(1)
                    .ok_or(CrawlTraversalRepositoryError::InvalidState)?;
                let changed = tx
                    .execute(
                        "UPDATE crawl_url_state SET work_generation = ?1, current_work_state = 'PENDING', current_execution_id = NULL WHERE crawl_run_id = ?2 AND id = ?3 AND admission_state = 'ADMITTED' AND work_generation = ?4",
                        (
                            i64_from(prepared_generation)?,
                            run_id.to_string(),
                            state_id.as_str(),
                            i64_from(previous_generation)?,
                        ),
                    )
                    .await
                    .map_err(db)?;
                if changed != 1 {
                    return Err(CrawlTraversalRepositoryError::CorruptState);
                }
                tx.execute(
                    "INSERT INTO crawl_recovery_action_items (action_job_id, crawl_run_id, crawl_url_state_id, previous_work_generation, prepared_work_generation) VALUES (?1, ?2, ?3, ?4, ?5)",
                    (
                        action_job_id.as_str(),
                        run_id.to_string(),
                        state_id.as_str(),
                        i64_from(previous_generation)?,
                        i64_from(prepared_generation)?,
                    ),
                )
                .await
                .map_err(db)?;
                state_ids.push(state_id);
            }
            Ok(CrawlRecoveryActionSelection {
                action_job_id: action_job_id.to_string(),
                crawl_run_id: run_id,
                action_kind,
                state_ids,
            })
        }
        .await;
        finish(tx, result).await
    }

    /// # Errors
    /// Returns a typed missing/corrupt-control or database error.
    pub async fn read_traversal_control(
        &self,
        run_id: CrawlRunId,
    ) -> Result<CrawlTraversalControl, CrawlTraversalRepositoryError> {
        let connection = self.database.connection().await.map_err(db)?;
        read_control(&connection, run_id).await
    }

    /// Reads the generation of one current logical work identity before
    /// provider IO. The caller must pass this captured value back to the
    /// guarded execution write; it is not a substitute for that guard.
    ///
    /// # Errors
    /// Returns a typed missing-state, corrupt-generation, or database error.
    pub async fn read_work_generation(
        &self,
        run_id: CrawlRunId,
        state_id: &str,
    ) -> Result<u64, CrawlTraversalRepositoryError> {
        let connection = self.database.connection().await.map_err(db)?;
        let row = connection
            .prepare(
                "SELECT work_generation FROM crawl_url_state WHERE crawl_run_id = ?1 AND id = ?2 AND admission_state = 'ADMITTED'",
            )
            .await
            .map_err(db)?
            .query_row((run_id.to_string(), state_id))
            .await
            .map_err(|error| match error {
                turso::Error::QueryReturnedNoRows => CrawlTraversalRepositoryError::InvalidState,
                other => db(other),
            })?;
        as_u64(row.get(0).map_err(db)?)
    }
}

fn db(error: impl Into<DbError>) -> CrawlTraversalRepositoryError {
    CrawlTraversalRepositoryError::Database(error.into())
}
async fn finish<T>(
    tx: Transaction<'_>,
    result: Result<T, CrawlTraversalRepositoryError>,
) -> Result<T, CrawlTraversalRepositoryError> {
    match result {
        Ok(value) => tx.commit().await.map(|()| value).map_err(db),
        Err(error) => {
            let _ = tx.rollback().await;
            Err(error)
        }
    }
}
fn as_u64(value: i64) -> Result<u64, CrawlTraversalRepositoryError> {
    u64::try_from(value).map_err(|_| CrawlTraversalRepositoryError::CorruptState)
}
fn optional_u64(value: Option<i64>) -> Result<Option<u64>, CrawlTraversalRepositoryError> {
    value.map(as_u64).transpose()
}
fn optional_u32(value: Option<i64>) -> Result<Option<u32>, CrawlTraversalRepositoryError> {
    value
        .map(|v| u32::try_from(v).map_err(|_| CrawlTraversalRepositoryError::CorruptState))
        .transpose()
}
fn admission_name(value: CrawlAdmissionState) -> &'static str {
    match value {
        CrawlAdmissionState::Admitted => "ADMITTED",
        CrawlAdmissionState::PreserveOnly => "PRESERVE_ONLY",
        CrawlAdmissionState::Resolved => "RESOLVED",
    }
}
fn admission_from_db(value: &str) -> Result<CrawlAdmissionState, CrawlTraversalRepositoryError> {
    match value {
        "ADMITTED" => Ok(CrawlAdmissionState::Admitted),
        "PRESERVE_ONLY" => Ok(CrawlAdmissionState::PreserveOnly),
        "RESOLVED" => Ok(CrawlAdmissionState::Resolved),
        _ => Err(CrawlTraversalRepositoryError::CorruptState),
    }
}
fn work_name(value: CrawlWorkState) -> &'static str {
    match value {
        CrawlWorkState::Pending => "PENDING",
        CrawlWorkState::Running => "RUNNING",
        CrawlWorkState::Completed => "COMPLETED",
        CrawlWorkState::Partial => "PARTIAL",
        CrawlWorkState::Failed => "FAILED",
        CrawlWorkState::Cancelled => "CANCELLED",
    }
}
fn optional_work_state(
    value: Option<String>,
) -> Result<Option<CrawlWorkState>, CrawlTraversalRepositoryError> {
    value
        .map(|value| match value.as_str() {
            "PENDING" => Ok(CrawlWorkState::Pending),
            "RUNNING" => Ok(CrawlWorkState::Running),
            "COMPLETED" => Ok(CrawlWorkState::Completed),
            "PARTIAL" => Ok(CrawlWorkState::Partial),
            "FAILED" => Ok(CrawlWorkState::Failed),
            "CANCELLED" => Ok(CrawlWorkState::Cancelled),
            _ => Err(CrawlTraversalRepositoryError::CorruptState),
        })
        .transpose()
}
fn i64_from(value: u64) -> Result<i64, CrawlTraversalRepositoryError> {
    i64::try_from(value).map_err(|_| CrawlTraversalRepositoryError::InvalidState)
}
fn nullable_text(value: Option<&str>) -> Value {
    value.map_or(Value::Null, |value| Value::Text(value.to_owned()))
}

fn page_type_match_name(value: CrawlPageTypeMatchState) -> &'static str {
    match value {
        CrawlPageTypeMatchState::Matched => "MATCHED",
        CrawlPageTypeMatchState::Unmatched => "UNMATCHED",
        CrawlPageTypeMatchState::Ambiguous => "AMBIGUOUS",
    }
}

fn optional_page_type_match_state(
    value: Option<String>,
) -> Result<Option<CrawlPageTypeMatchState>, CrawlTraversalRepositoryError> {
    value
        .map(|value| match value.as_str() {
            "MATCHED" => Ok(CrawlPageTypeMatchState::Matched),
            "UNMATCHED" => Ok(CrawlPageTypeMatchState::Unmatched),
            "AMBIGUOUS" => Ok(CrawlPageTypeMatchState::Ambiguous),
            _ => Err(CrawlTraversalRepositoryError::CorruptState),
        })
        .transpose()
}

fn validate_state(state: &CrawlUrlStateRecord) -> Result<(), CrawlTraversalRepositoryError> {
    if state.id.is_empty()
        || state.canonical_url.is_empty()
        || state.canonical_url.len() > MAX_URL_CHARS
        || state.requested_url.is_empty()
        || state.requested_url.len() > MAX_URL_CHARS
    {
        return Err(CrawlTraversalRepositoryError::InvalidState);
    }
    if !state.seen || (state.expanded && !state.sampled) {
        return Err(CrawlTraversalRepositoryError::InvalidState);
    }
    if state.in_scope != state.page_type_match_state.is_some() {
        return Err(CrawlTraversalRepositoryError::InvalidState);
    }
    match state.admission_state {
        CrawlAdmissionState::Admitted
            if state.preserve_reason.is_none()
                && state.resolved_to_url_state_id.is_none()
                && state.admission_sequence.is_some()
                && state.depth.is_some()
                && state.current_work_state.is_some() =>
        {
            Ok(())
        }
        CrawlAdmissionState::PreserveOnly
            if state.preserve_reason.is_some()
                && state.resolved_to_url_state_id.is_none()
                && state.admission_sequence.is_none()
                && state.depth.is_none()
                && state.target_page_type_id.is_none()
                && state.transition_id.is_none()
                && state.current_work_state.is_none()
                && state.current_execution_id.is_none()
                && !state.sampled
                && !state.expanded =>
        {
            Ok(())
        }
        CrawlAdmissionState::Resolved
            if state.preserve_reason.as_deref() == Some("CANONICAL_REDIRECT")
                && state.resolved_to_url_state_id.is_some()
                && state.admission_sequence.is_none()
                && state.depth.is_none()
                && state.target_page_type_id.is_none()
                && state.transition_id.is_none()
                && state.current_work_state.is_none()
                && state.current_execution_id.is_none() =>
        {
            Ok(())
        }
        _ => Err(CrawlTraversalRepositoryError::InvalidState),
    }
}

fn initial_state_matches(persisted: &CrawlUrlStateRecord, expected: &CrawlUrlStateRecord) -> bool {
    persisted.id == expected.id
        && persisted.crawl_run_id == expected.crawl_run_id
        && persisted.canonical_url == expected.canonical_url
        && persisted.requested_url == expected.requested_url
        && persisted.first_discovered_url_id == expected.first_discovered_url_id
        && persisted.parent_url_state_id == expected.parent_url_state_id
        && persisted.parent_discovered_url_id == expected.parent_discovered_url_id
        && persisted.admission_state == expected.admission_state
        && persisted.preserve_reason == expected.preserve_reason
        && persisted.resolved_to_url_state_id == expected.resolved_to_url_state_id
        && persisted.admission_sequence == expected.admission_sequence
        && persisted.depth == expected.depth
        && persisted.target_page_type_id == expected.target_page_type_id
        && persisted.transition_id == expected.transition_id
        && persisted.pagination == expected.pagination
        && persisted.final_canonical_url == expected.final_canonical_url
        && expected
            .seed_provenance
            .iter()
            .all(|seed_id| persisted.seed_provenance.contains(seed_id))
}

async fn ensure_run(
    connection: &turso::Connection,
    run_id: CrawlRunId,
) -> Result<(), CrawlTraversalRepositoryError> {
    let row = connection
        .prepare("SELECT EXISTS(SELECT 1 FROM crawl_runs WHERE id = ?1)")
        .await
        .map_err(db)?
        .query_row([run_id.to_string()])
        .await
        .map_err(db)?;
    let found: i64 = row.get(0).map_err(db)?;
    if found == 1 {
        Ok(())
    } else {
        Err(CrawlTraversalRepositoryError::CrawlRunNotFound)
    }
}

async fn validate_state_ownership(
    connection: &turso::Connection,
    state: &CrawlUrlStateRecord,
    run_id: CrawlRunId,
) -> Result<(), CrawlTraversalRepositoryError> {
    if state.crawl_run_id != run_id {
        return Err(CrawlTraversalRepositoryError::InvalidState);
    }
    let run = connection
        .prepare("SELECT crawler_version_id, snapshot_json FROM crawl_runs WHERE id = ?1")
        .await
        .map_err(db)?
        .query_row([run_id.to_string()])
        .await
        .map_err(db)?;
    let version = run.get::<Option<String>>(0).map_err(db)?;
    let selected_seed_ids = if state.seed_provenance.is_empty() {
        None
    } else {
        let snapshot_json: String = run.get(1).map_err(db)?;
        let snapshot: erabi_domain::CrawlRunSnapshot = serde_json::from_str(&snapshot_json)
            .map_err(|_| CrawlTraversalRepositoryError::InvalidState)?;
        Some(
            snapshot
                .selected_seed_ids()
                .iter()
                .map(ToString::to_string)
                .collect::<std::collections::BTreeSet<_>>(),
        )
    };
    if let Some(target_page_type_id) = state.target_page_type_id.as_deref() {
        let owned = version.as_deref().is_some_and(|version_id| {
            // The query is performed below; this closure only keeps the
            // nullable Quick Scrape version from being treated as owned.
            !version_id.is_empty() && !target_page_type_id.is_empty()
        });
        if !owned
            || connection
                .prepare("SELECT EXISTS(SELECT 1 FROM page_types WHERE id = ?1 AND crawler_version_id = ?2)")
                .await
                .map_err(db)?
                .query_row((target_page_type_id, version.as_deref().unwrap_or_default()))
                .await
                .map_err(db)?
                .get::<i64>(0)
                .map_err(db)?
                .ne(&1)
        {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
    }
    if let Some(transition_id) = state.transition_id.as_deref()
        && (version.is_none()
            || connection
                .prepare("SELECT EXISTS(SELECT 1 FROM discovery_transitions WHERE id = ?1 AND crawler_version_id = ?2)")
                .await
                .map_err(db)?
                .query_row((transition_id, version.as_deref().unwrap_or_default()))
                .await
                .map_err(db)?
                .get::<i64>(0)
                .map_err(db)?
                .ne(&1))
    {
        return Err(CrawlTraversalRepositoryError::InvalidState);
    }
    for seed_id in &state.seed_provenance {
        if selected_seed_ids
            .as_ref()
            .is_some_and(|selected| !selected.contains(seed_id))
        {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        if version.is_none()
            || connection
                .prepare(
                    "SELECT EXISTS(SELECT 1 FROM seeds WHERE id = ?1 AND crawler_version_id = ?2)",
                )
                .await
                .map_err(db)?
                .query_row((seed_id.as_str(), version.as_deref().unwrap_or_default()))
                .await
                .map_err(db)?
                .get::<i64>(0)
                .map_err(db)?
                .ne(&1)
        {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
    }
    validate_state_relationships(connection, state, run_id).await?;
    Ok(())
}

async fn validate_state_relationships(
    connection: &turso::Connection,
    state: &CrawlUrlStateRecord,
    run_id: CrawlRunId,
) -> Result<(), CrawlTraversalRepositoryError> {
    if let Some(first_discovered_url_id) = state.first_discovered_url_id.as_deref() {
        require_same_run_reference(
            connection,
            "SELECT EXISTS(SELECT 1 FROM discovered_urls WHERE crawl_run_id = ?1 AND id = ?2)",
            run_id,
            first_discovered_url_id,
        )
        .await?;
    }
    if let Some(parent_discovered_url_id) = state.parent_discovered_url_id.as_deref() {
        require_same_run_reference(
            connection,
            "SELECT EXISTS(SELECT 1 FROM discovered_urls WHERE crawl_run_id = ?1 AND id = ?2)",
            run_id,
            parent_discovered_url_id,
        )
        .await?;
    }
    if let Some(parent_url_state_id) = state.parent_url_state_id.as_deref() {
        if parent_url_state_id == state.id {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        require_same_run_reference(
            connection,
            "SELECT EXISTS(SELECT 1 FROM crawl_url_state WHERE crawl_run_id = ?1 AND id = ?2)",
            run_id,
            parent_url_state_id,
        )
        .await?;
    }
    if let Some(resolved_to_url_state_id) = state.resolved_to_url_state_id.as_deref() {
        if resolved_to_url_state_id == state.id {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        require_same_run_reference(
            connection,
            "SELECT EXISTS(SELECT 1 FROM crawl_url_state WHERE crawl_run_id = ?1 AND id = ?2)",
            run_id,
            resolved_to_url_state_id,
        )
        .await?;
    }
    if let Some(current_execution_id) = state.current_execution_id.as_deref() {
        let row = connection
            .prepare(
                "SELECT EXISTS(SELECT 1 FROM crawl_execution_results WHERE crawl_run_id = ?1 AND id = ?2 AND crawl_url_state_id = ?3 AND work_generation = ?4)",
            )
            .await
            .map_err(db)?
            .query_row((
                run_id.to_string(),
                current_execution_id,
                state.id.as_str(),
                i64_from(state.work_generation)?,
            ))
            .await
            .map_err(db)?;
        if row.get::<i64>(0).map_err(db)? != 1 {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
    }
    Ok(())
}

async fn require_same_run_reference(
    connection: &turso::Connection,
    query: &str,
    run_id: CrawlRunId,
    reference_id: &str,
) -> Result<(), CrawlTraversalRepositoryError> {
    let row = connection
        .prepare(query)
        .await
        .map_err(db)?
        .query_row((run_id.to_string(), reference_id))
        .await
        .map_err(db)?;
    if row.get::<i64>(0).map_err(db)? == 1 {
        Ok(())
    } else {
        Err(CrawlTraversalRepositoryError::InvalidState)
    }
}

async fn apply_semantic_projection(
    connection: &turso::Connection,
    run_id: CrawlRunId,
    projection: &CrawlTraversalSemanticProjection,
) -> Result<(), CrawlTraversalRepositoryError> {
    if projection.url_states.is_empty() {
        return Ok(());
    }
    let mut seen = std::collections::BTreeSet::new();
    for value in &projection.url_states {
        if value.canonical_url.is_empty() || !seen.insert(value.canonical_url.as_str()) {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        if value.expanded && !value.sampled {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        if value.in_scope != value.page_type_match_state.is_some() {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        let changed = connection
            .execute(
                "UPDATE crawl_url_state SET sampled = ?1, expanded = ?2, in_scope = ?3, page_type_match_state = ?4 WHERE crawl_run_id = ?5 AND canonical_url = ?6 AND seen = 1",
                (
                    i64::from(value.sampled),
                    i64::from(value.expanded),
                    i64::from(value.in_scope),
                    value
                        .page_type_match_state
                        .map(page_type_match_name)
                        .map_or(Value::Null, |value| Value::Text(value.to_owned())),
                    run_id.to_string(),
                    value.canonical_url.as_str(),
                ),
            )
            .await
            .map_err(db)?;
        if changed != 1 {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
    }
    let mut rows = connection
        .query(
            "SELECT canonical_url FROM crawl_url_state WHERE crawl_run_id = ?1",
            [run_id.to_string()],
        )
        .await
        .map_err(db)?;
    while let Some(row) = rows.next().await.map_err(db)? {
        let canonical_url: String = row.get(0).map_err(db)?;
        if !seen.contains(canonical_url.as_str()) {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
    }

    connection
        .execute(
            "DELETE FROM crawl_traversal_page_type_counts WHERE crawl_run_id = ?1",
            [run_id.to_string()],
        )
        .await
        .map_err(db)?;
    let mut page_types = std::collections::BTreeSet::new();
    for count in &projection.page_type_counts {
        if !page_types.insert(count.page_type_id.as_str()) {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        let owned = connection
            .prepare(
                "SELECT EXISTS(SELECT 1 FROM page_types AS page_type JOIN crawl_runs AS run ON run.id = ?1 WHERE page_type.id = ?2 AND page_type.crawler_version_id = run.crawler_version_id)",
            )
            .await
            .map_err(db)?
            .query_row((run_id.to_string(), count.page_type_id.as_str()))
            .await
            .map_err(db)?
            .get::<i64>(0)
            .map_err(db)?;
        if owned != 1 {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        connection
            .execute(
                "INSERT INTO crawl_traversal_page_type_counts (crawl_run_id, page_type_id, sampled_count, discovered_count) VALUES (?1, ?2, ?3, ?4)",
                (
                    run_id.to_string(),
                    count.page_type_id.as_str(),
                    i64_from(count.sampled_count)?,
                    i64_from(count.discovered_count)?,
                ),
            )
            .await
            .map_err(db)?;
    }
    Ok(())
}

async fn read_page_type_counts(
    connection: &turso::Connection,
    run_id: CrawlRunId,
) -> Result<Vec<CrawlTraversalPageTypeCounts>, CrawlTraversalRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT page_type_id, sampled_count, discovered_count FROM crawl_traversal_page_type_counts WHERE crawl_run_id = ?1 ORDER BY page_type_id COLLATE BINARY",
            [run_id.to_string()],
        )
        .await
        .map_err(db)?;
    let mut counts = Vec::new();
    while let Some(row) = rows.next().await.map_err(db)? {
        counts.push(CrawlTraversalPageTypeCounts {
            page_type_id: row.get(0).map_err(db)?,
            sampled_count: as_u64(row.get::<i64>(1).map_err(db)?)?,
            discovered_count: as_u64(row.get::<i64>(2).map_err(db)?)?,
        });
    }
    Ok(counts)
}

async fn read_recovery_action_items(
    connection: &Transaction<'_>,
    action_job_id: &JobId,
    run_id: CrawlRunId,
) -> Result<Vec<String>, CrawlTraversalRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT item.crawl_url_state_id, item.prepared_work_generation, state.work_generation FROM crawl_recovery_action_items AS item JOIN crawl_url_state AS state ON state.crawl_run_id = item.crawl_run_id AND state.id = item.crawl_url_state_id WHERE item.action_job_id = ?1 AND item.crawl_run_id = ?2 ORDER BY state.depth ASC NULLS LAST, state.admission_sequence ASC NULLS LAST, state.canonical_url COLLATE BINARY ASC, state.requested_url COLLATE BINARY ASC",
            (action_job_id.as_str(), run_id.to_string()),
        )
        .await
        .map_err(db)?;
    let mut state_ids = Vec::new();
    while let Some(row) = rows.next().await.map_err(db)? {
        let state_id: String = row.get(0).map_err(db)?;
        let prepared_generation = as_u64(row.get::<i64>(1).map_err(db)?)?;
        let current_generation = as_u64(row.get::<i64>(2).map_err(db)?)?;
        if prepared_generation != current_generation
            || !state_ids.is_empty() && state_ids.last() == Some(&state_id)
        {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        state_ids.push(state_id);
    }
    Ok(state_ids)
}

async fn mark_in_flight_work(
    connection: &Transaction<'_>,
    run_id: CrawlRunId,
    in_flight_work: &[CrawlInFlightWork],
) -> Result<(), CrawlTraversalRepositoryError> {
    let mut state_ids = std::collections::BTreeSet::new();
    for work in in_flight_work {
        if work.state_id.is_empty() || !state_ids.insert(work.state_id.as_str()) {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        let row = connection
            .prepare(
                "SELECT admission_state, current_work_state, work_generation, current_execution_id FROM crawl_url_state WHERE crawl_run_id = ?1 AND id = ?2",
            )
            .await
            .map_err(db)?
            .query_row((run_id.to_string(), work.state_id.as_str()))
            .await
            .map_err(|error| match error {
                turso::Error::QueryReturnedNoRows => CrawlTraversalRepositoryError::InvalidState,
                other => db(other),
            })?;
        let admission_state: String = row.get(0).map_err(db)?;
        let current_work_state: Option<String> = row.get(1).map_err(db)?;
        let generation = as_u64(row.get::<i64>(2).map_err(db)?)?;
        let current_execution_id: Option<String> = row.get(3).map_err(db)?;
        if admission_state != "ADMITTED"
            || work
                .expected_work_generation
                .is_some_and(|expected| expected != generation)
        {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        if current_work_state.as_deref() == Some("COMPLETED") && current_execution_id.is_some() {
            // The discovery transaction may be replayed after the guarded
            // execution transaction has already committed. That is a valid
            // idempotent completion, not permission to move a newer result.
            continue;
        }
        if !matches!(current_work_state.as_deref(), Some("PENDING" | "RUNNING")) {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        let changed = connection
            .execute(
                "UPDATE crawl_url_state SET current_work_state = 'RUNNING' WHERE crawl_run_id = ?1 AND id = ?2 AND admission_state = 'ADMITTED' AND work_generation = ?3 AND current_work_state IN ('PENDING', 'RUNNING')",
                (
                    run_id.to_string(),
                    work.state_id.as_str(),
                    i64_from(generation)?,
                ),
            )
            .await
            .map_err(db)?;
        if changed != 1 {
            return Err(CrawlTraversalRepositoryError::CorruptState);
        }
    }
    Ok(())
}

async fn read_state_by_canonical(
    connection: &turso::Connection,
    run_id: CrawlRunId,
    canonical_url: &str,
) -> Result<Option<CrawlUrlStateRecord>, CrawlTraversalRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT id, canonical_url, first_discovered_url_id, requested_url, parent_url_state_id, parent_discovered_url_id, admission_state, preserve_reason, resolved_to_url_state_id, admission_sequence, depth, target_page_type_id, transition_id, pagination, final_canonical_url, current_work_state, work_generation, current_execution_id, seen, sampled, expanded, in_scope, page_type_match_state FROM crawl_url_state WHERE crawl_run_id = ?1 AND canonical_url = ?2",
            (run_id.to_string(), canonical_url),
        )
        .await
        .map_err(db)?;
    let Some(row) = rows.next().await.map_err(db)? else {
        return Ok(None);
    };
    let id: String = row.get(0).map_err(db)?;
    let state = CrawlUrlStateRecord {
        id: id.clone(),
        crawl_run_id: run_id,
        canonical_url: row.get(1).map_err(db)?,
        first_discovered_url_id: row.get(2).map_err(db)?,
        requested_url: row.get(3).map_err(db)?,
        parent_url_state_id: row.get(4).map_err(db)?,
        parent_discovered_url_id: row.get(5).map_err(db)?,
        admission_state: admission_from_db(&row.get::<String>(6).map_err(db)?)?,
        preserve_reason: row.get(7).map_err(db)?,
        resolved_to_url_state_id: row.get(8).map_err(db)?,
        admission_sequence: optional_u64(row.get(9).map_err(db)?)?,
        depth: optional_u32(row.get(10).map_err(db)?)?,
        target_page_type_id: row.get(11).map_err(db)?,
        transition_id: row.get(12).map_err(db)?,
        pagination: row.get::<i64>(13).map_err(db)? == 1,
        final_canonical_url: row.get(14).map_err(db)?,
        current_work_state: optional_work_state(row.get(15).map_err(db)?)?,
        work_generation: as_u64(row.get::<i64>(16).map_err(db)?)?,
        current_execution_id: row.get(17).map_err(db)?,
        seed_provenance: read_seed_provenance(connection, &id).await?,
        seen: row.get::<i64>(18).map_err(db)? == 1,
        sampled: row.get::<i64>(19).map_err(db)? == 1,
        expanded: row.get::<i64>(20).map_err(db)? == 1,
        in_scope: row.get::<i64>(21).map_err(db)? == 1,
        page_type_match_state: optional_page_type_match_state(row.get(22).map_err(db)?)?,
    };
    validate_state(&state)?;
    Ok(Some(state))
}

async fn reconcile_redirect(
    connection: &turso::Connection,
    run_id: CrawlRunId,
    redirect: &CrawlRedirectReconciliation,
) -> Result<(), CrawlTraversalRepositoryError> {
    if redirect.alias_canonical_url.is_empty()
        || redirect.final_canonical_url.is_empty()
        || redirect.alias_canonical_url == redirect.final_canonical_url
        || redirect.alias_url_state_id.is_empty()
        || redirect.final_url_state_id.is_empty()
    {
        return Err(CrawlTraversalRepositoryError::InvalidState);
    }
    let Some(alias) =
        read_state_by_canonical(connection, run_id, &redirect.alias_canonical_url).await?
    else {
        return Err(CrawlTraversalRepositoryError::InvalidState);
    };
    if alias.id != redirect.alias_url_state_id {
        return Err(CrawlTraversalRepositoryError::InvalidState);
    }
    if alias.admission_state == CrawlAdmissionState::Resolved {
        if alias.resolved_to_url_state_id.as_deref() == Some(redirect.final_url_state_id.as_str()) {
            return Ok(());
        }
        return Err(CrawlTraversalRepositoryError::InvalidState);
    }
    if alias.admission_state != CrawlAdmissionState::Admitted {
        return Err(CrawlTraversalRepositoryError::InvalidState);
    }

    let final_state =
        read_state_by_canonical(connection, run_id, &redirect.final_canonical_url).await?;
    if let Some(final_state) = &final_state {
        if final_state.id != redirect.final_url_state_id
            || final_state.admission_state != CrawlAdmissionState::Admitted
        {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        append_seed_provenance(connection, final_state, &alias.seed_provenance).await?;
    } else {
        let mut final_state = alias.clone();
        final_state.id.clone_from(&redirect.final_url_state_id);
        final_state
            .canonical_url
            .clone_from(&redirect.final_canonical_url);
        final_state.final_canonical_url = None;
        final_state.current_execution_id = None;
        final_state.seen = true;
        insert_state(connection, &final_state).await?;
    }
    let changed = connection
        .execute(
            "UPDATE crawl_url_state SET admission_state = 'RESOLVED', preserve_reason = 'CANONICAL_REDIRECT', resolved_to_url_state_id = ?1, admission_sequence = NULL, depth = NULL, target_page_type_id = NULL, transition_id = NULL, current_work_state = NULL, current_execution_id = NULL, final_canonical_url = ?2, sampled = 0, expanded = 0, in_scope = 0, page_type_match_state = NULL WHERE id = ?3 AND crawl_run_id = ?4 AND admission_state = 'ADMITTED'",
            (
                redirect.final_url_state_id.as_str(),
                redirect.final_canonical_url.as_str(),
                alias.id.as_str(),
                run_id.to_string(),
            ),
        )
        .await
        .map_err(db)?;
    if changed != 1 {
        return Err(CrawlTraversalRepositoryError::CorruptState);
    }
    Ok(())
}

async fn append_seed_provenance(
    connection: &turso::Connection,
    state: &CrawlUrlStateRecord,
    seed_ids: &[String],
) -> Result<(), CrawlTraversalRepositoryError> {
    let mut merged = state.seed_provenance.clone();
    for seed_id in seed_ids {
        if !merged.contains(seed_id) {
            merged.push(seed_id.clone());
        }
    }
    for seed_id in merged.into_iter().skip(state.seed_provenance.len()) {
        insert_seed_provenance_value(connection, state, &seed_id).await?;
    }
    Ok(())
}
async fn insert_state(
    connection: &turso::Connection,
    state: &CrawlUrlStateRecord,
) -> Result<(), CrawlTraversalRepositoryError> {
    validate_state(state)?;
    validate_state_ownership(connection, state, state.crawl_run_id).await?;
    connection.execute(
        "INSERT INTO crawl_url_state (id,crawl_run_id,canonical_url,first_discovered_url_id,requested_url,parent_url_state_id,parent_discovered_url_id,admission_state,preserve_reason,resolved_to_url_state_id,admission_sequence,depth,target_page_type_id,transition_id,pagination,final_canonical_url,current_work_state,work_generation,current_execution_id,seen,sampled,expanded,in_scope,page_type_match_state) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24)",
        params_from_iter(vec![
            Value::Text(state.id.clone()),
            Value::Text(state.crawl_run_id.to_string()),
            Value::Text(state.canonical_url.clone()),
            nullable_text(state.first_discovered_url_id.as_deref()),
            Value::Text(state.requested_url.clone()),
            nullable_text(state.parent_url_state_id.as_deref()),
            nullable_text(state.parent_discovered_url_id.as_deref()),
            Value::Text(admission_name(state.admission_state).to_owned()),
            nullable_text(state.preserve_reason.as_deref()),
            nullable_text(state.resolved_to_url_state_id.as_deref()),
            state
                .admission_sequence
                .map(i64_from)
                .transpose()?
                .map_or(Value::Null, Value::Integer),
            state
                .depth
                .map(i64::from)
                .map_or(Value::Null, Value::Integer),
            nullable_text(state.target_page_type_id.as_deref()),
            nullable_text(state.transition_id.as_deref()),
            Value::Integer(i64::from(state.pagination)),
            nullable_text(state.final_canonical_url.as_deref()),
            state
                .current_work_state
                .map(work_name)
                .map_or(Value::Null, |value| Value::Text(value.to_owned())),
            Value::Integer(i64_from(state.work_generation)?),
            nullable_text(state.current_execution_id.as_deref()),
            Value::Integer(1),
            Value::Integer(i64::from(state.sampled)),
            Value::Integer(i64::from(state.expanded)),
            Value::Integer(i64::from(state.in_scope)),
            state
                .page_type_match_state
                .map(page_type_match_name)
                .map_or(Value::Null, |value| Value::Text(value.to_owned())),
        ]),
    )
    .await
    .map_err(db)?;
    insert_seed_provenance(connection, state).await
}
async fn upsert_discovery_state(
    connection: &turso::Connection,
    state: &CrawlUrlStateRecord,
) -> Result<(), CrawlTraversalRepositoryError> {
    validate_state(state)?;
    validate_state_ownership(connection, state, state.crawl_run_id).await?;
    let row = connection.prepare("SELECT EXISTS(SELECT 1 FROM crawl_url_state WHERE crawl_run_id=?1 AND canonical_url=?2)").await.map_err(db)?.query_row((state.crawl_run_id.to_string(),state.canonical_url.as_str())).await.map_err(db)?;
    let exists: i64 = row.get(0).map_err(db)?;
    if exists == 0 {
        insert_state(connection, state).await
    } else {
        let existing = connection
            .prepare(
                "SELECT id, admission_state, current_work_state, work_generation FROM crawl_url_state WHERE crawl_run_id = ?1 AND canonical_url = ?2",
            )
            .await
            .map_err(db)?
            .query_row((state.crawl_run_id.to_string(), state.canonical_url.as_str()))
            .await
            .map_err(db)?;
        let existing_id: String = existing.get(0).map_err(db)?;
        let existing_admission: String = existing.get(1).map_err(db)?;
        let existing_work_state: Option<String> = existing.get(2).map_err(db)?;
        let _existing_generation: i64 = existing.get(3).map_err(db)?;
        if state.id != existing_id {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        if existing_admission == "RESOLVED"
            && state.admission_state != CrawlAdmissionState::PreserveOnly
        {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        if existing_admission == "PRESERVE_ONLY"
            && state.admission_state == CrawlAdmissionState::Admitted
        {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        if existing_admission == "ADMITTED"
            && state.admission_state == CrawlAdmissionState::Admitted
            && existing_work_state.as_deref() == Some("PENDING")
        {
            connection
                .execute(
                    "UPDATE crawl_url_state SET requested_url = ?1, first_discovered_url_id = COALESCE(first_discovered_url_id, ?2), parent_url_state_id = COALESCE(?3, parent_url_state_id), parent_discovered_url_id = COALESCE(parent_discovered_url_id, ?4), admission_sequence = ?5, depth = ?6, target_page_type_id = ?7, transition_id = ?8, pagination = ?9, final_canonical_url = COALESCE(?10, final_canonical_url) WHERE id = ?11 AND crawl_run_id = ?12 AND current_work_state = 'PENDING'",
                    (
                        state.requested_url.as_str(),
                        nullable_text(state.first_discovered_url_id.as_deref()),
                        nullable_text(state.parent_url_state_id.as_deref()),
                        nullable_text(state.parent_discovered_url_id.as_deref()),
                        i64_from(state.admission_sequence.ok_or(CrawlTraversalRepositoryError::InvalidState)?)?,
                        i64::from(state.depth.ok_or(CrawlTraversalRepositoryError::InvalidState)?),
                        nullable_text(state.target_page_type_id.as_deref()),
                        nullable_text(state.transition_id.as_deref()),
                        i64::from(state.pagination),
                        nullable_text(state.final_canonical_url.as_deref()),
                        existing_id.as_str(),
                        state.crawl_run_id.to_string(),
                    ),
                )
                .await
                .map_err(db)?;
        } else {
            connection
                .execute(
                    "UPDATE crawl_url_state SET first_discovered_url_id = COALESCE(first_discovered_url_id, ?1), parent_discovered_url_id = COALESCE(parent_discovered_url_id, ?2), final_canonical_url = COALESCE(final_canonical_url, ?3) WHERE id = ?4 AND crawl_run_id = ?5",
                    (
                        nullable_text(state.first_discovered_url_id.as_deref()),
                        nullable_text(state.parent_discovered_url_id.as_deref()),
                        nullable_text(state.final_canonical_url.as_deref()),
                        existing_id.as_str(),
                        state.crawl_run_id.to_string(),
                    ),
                )
                .await
                .map_err(db)?;
        }
        insert_seed_provenance(connection, state).await
    }
}
async fn insert_seed_provenance(
    connection: &turso::Connection,
    state: &CrawlUrlStateRecord,
) -> Result<(), CrawlTraversalRepositoryError> {
    for seed_id in &state.seed_provenance {
        insert_seed_provenance_value(connection, state, seed_id).await?;
    }
    Ok(())
}

async fn insert_seed_provenance_value(
    connection: &turso::Connection,
    state: &CrawlUrlStateRecord,
    seed_id: &str,
) -> Result<(), CrawlTraversalRepositoryError> {
    let existing = connection
        .prepare(
            "SELECT 1 FROM seeds AS seed JOIN crawl_runs AS run ON run.id = ?1 WHERE seed.id = ?2 AND run.crawler_version_id = seed.crawler_version_id",
        )
        .await
        .map_err(db)?
        .query_row((state.crawl_run_id.to_string(), seed_id))
        .await;
    match existing {
        Ok(_) => {}
        Err(turso::Error::QueryReturnedNoRows) => {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        Err(error) => return Err(db(error)),
    }

    let exists = connection
        .prepare(
            "SELECT 1 FROM crawl_url_seed_provenance WHERE crawl_url_state_id = ?1 AND seed_id = ?2",
        )
        .await
        .map_err(db)?
        .query_row((state.id.as_str(), seed_id))
        .await;
    match exists {
        Ok(_) => return Ok(()),
        Err(turso::Error::QueryReturnedNoRows) => {}
        Err(error) => return Err(db(error)),
    }
    let row = connection
        .prepare(
            "SELECT COALESCE(MAX(ordinal), -1) + 1 FROM crawl_url_seed_provenance WHERE crawl_url_state_id = ?1",
        )
        .await
        .map_err(db)?
        .query_row([state.id.as_str()])
        .await
        .map_err(db)?;
    let ordinal: i64 = row.get(0).map_err(db)?;
    connection
        .execute(
            "INSERT INTO crawl_url_seed_provenance (crawl_url_state_id, seed_id, ordinal) VALUES (?1, ?2, ?3)",
            (state.id.as_str(), seed_id, ordinal),
        )
        .await
        .map_err(db)?;
    Ok(())
}
async fn insert_control(
    connection: &turso::Connection,
    control: &CrawlTraversalControl,
) -> Result<(), CrawlTraversalRepositoryError> {
    connection.execute("INSERT INTO crawl_traversal_control (crawl_run_id,consumed_bytes,raw_link_count,duplicate_count,robots_excluded_count,provider_error_count,external_url_count,blocked_url_count,peak_expansion_count,elapsed_millis,time_budget_hit,duration_work_not_expanded,pagination_truncation_count,next_admission_sequence) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)", control_values(control)?).await.map_err(db)?;
    Ok(())
}
async fn update_control(
    connection: &turso::Connection,
    control: &CrawlTraversalControl,
) -> Result<(), CrawlTraversalRepositoryError> {
    let changed = connection.execute("UPDATE crawl_traversal_control SET consumed_bytes=?1,raw_link_count=?2,duplicate_count=?3,robots_excluded_count=?4,provider_error_count=?5,external_url_count=?6,blocked_url_count=?7,peak_expansion_count=?8,elapsed_millis=?9,time_budget_hit=?10,duration_work_not_expanded=?11,pagination_truncation_count=?12,next_admission_sequence=?13 WHERE crawl_run_id=?14", (i64_from(control.consumed_bytes)?,i64_from(control.raw_link_count)?,i64_from(control.duplicate_count)?,i64_from(control.robots_excluded_count)?,i64_from(control.provider_error_count)?,i64_from(control.external_url_count)?,i64_from(control.blocked_url_count)?,i64_from(control.peak_expansion_count)?,i64_from(control.elapsed_millis)?,i64::from(control.time_budget_hit),i64::from(control.duration_work_not_expanded),i64_from(control.pagination_truncation_count)?,i64_from(control.next_admission_sequence)?,control.crawl_run_id.to_string())).await.map_err(db)?;
    if changed == 1 {
        Ok(())
    } else {
        Err(CrawlTraversalRepositoryError::CorruptState)
    }
}

async fn replace_transition_counts(
    connection: &turso::Connection,
    run_id: CrawlRunId,
    counts: &[CrawlTransitionSourceCount],
) -> Result<(), CrawlTraversalRepositoryError> {
    let version = connection
        .prepare("SELECT crawler_version_id FROM crawl_runs WHERE id = ?1")
        .await
        .map_err(db)?
        .query_row([run_id.to_string()])
        .await
        .map_err(db)?
        .get::<Option<String>>(0)
        .map_err(db)?;
    for count in counts {
        if count.transition_id.is_empty() || count.source_url_state_id.is_empty() {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        let source_exists = connection
            .prepare(
                "SELECT EXISTS(SELECT 1 FROM crawl_url_state WHERE crawl_run_id = ?1 AND id = ?2 AND admission_state = 'ADMITTED')",
            )
            .await
            .map_err(db)?
            .query_row((run_id.to_string(), count.source_url_state_id.as_str()))
            .await
            .map_err(db)?
            .get::<i64>(0)
            .map_err(db)?;
        if source_exists != 1 {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
        let transition_exists = if let Some(version_id) = version.as_deref() {
            connection
            .prepare("SELECT EXISTS(SELECT 1 FROM discovery_transitions WHERE id = ?1 AND crawler_version_id = ?2)")
            .await
            .map_err(db)?
            .query_row((count.transition_id.as_str(), version_id))
            .await
            .map_err(db)?
            .get::<i64>(0)
            .map_err(db)?
            == 1
        } else {
            false
        };
        if !transition_exists {
            return Err(CrawlTraversalRepositoryError::InvalidState);
        }
    }
    connection
        .execute(
            "DELETE FROM crawl_transition_source_counts WHERE crawl_run_id = ?1",
            [run_id.to_string()],
        )
        .await
        .map_err(db)?;
    for count in counts {
        connection
            .execute(
                "INSERT INTO crawl_transition_source_counts (crawl_run_id, transition_id, source_url_state_id, eligible_edge_count) VALUES (?1, ?2, ?3, ?4)",
                (
                    run_id.to_string(),
                    count.transition_id.as_str(),
                    count.source_url_state_id.as_str(),
                    i64_from(count.eligible_edge_count)?,
                ),
            )
            .await
            .map_err(db)?;
    }
    Ok(())
}

async fn read_transition_counts(
    connection: &turso::Connection,
    run_id: CrawlRunId,
) -> Result<Vec<CrawlTransitionSourceCount>, CrawlTraversalRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT transition_id, source_url_state_id, eligible_edge_count FROM crawl_transition_source_counts WHERE crawl_run_id = ?1 ORDER BY transition_id COLLATE BINARY, source_url_state_id COLLATE BINARY",
            [run_id.to_string()],
        )
        .await
        .map_err(db)?;
    let mut counts = Vec::new();
    while let Some(row) = rows.next().await.map_err(db)? {
        counts.push(CrawlTransitionSourceCount {
            transition_id: row.get(0).map_err(db)?,
            source_url_state_id: row.get(1).map_err(db)?,
            eligible_edge_count: as_u64(row.get::<i64>(2).map_err(db)?)?,
        });
    }
    Ok(counts)
}
fn control_values(
    control: &CrawlTraversalControl,
) -> Result<ControlSqlValues, CrawlTraversalRepositoryError> {
    Ok((
        control.crawl_run_id.to_string(),
        i64_from(control.consumed_bytes)?,
        i64_from(control.raw_link_count)?,
        i64_from(control.duplicate_count)?,
        i64_from(control.robots_excluded_count)?,
        i64_from(control.provider_error_count)?,
        i64_from(control.external_url_count)?,
        i64_from(control.blocked_url_count)?,
        i64_from(control.peak_expansion_count)?,
        i64_from(control.elapsed_millis)?,
        i64::from(control.time_budget_hit),
        i64::from(control.duration_work_not_expanded),
        i64_from(control.pagination_truncation_count)?,
        i64_from(control.next_admission_sequence)?,
    ))
}
async fn read_control(
    connection: &turso::Connection,
    run_id: CrawlRunId,
) -> Result<CrawlTraversalControl, CrawlTraversalRepositoryError> {
    let row = connection.prepare("SELECT consumed_bytes,raw_link_count,duplicate_count,robots_excluded_count,provider_error_count,external_url_count,blocked_url_count,peak_expansion_count,elapsed_millis,time_budget_hit,duration_work_not_expanded,pagination_truncation_count,next_admission_sequence FROM crawl_traversal_control WHERE crawl_run_id=?1").await.map_err(db)?.query_row([run_id.to_string()]).await.map_err(|e| match e { turso::Error::QueryReturnedNoRows => CrawlTraversalRepositoryError::CrawlRunNotFound, other => db(other) })?;
    Ok(CrawlTraversalControl {
        crawl_run_id: run_id,
        consumed_bytes: as_u64(row.get(0).map_err(db)?)?,
        raw_link_count: as_u64(row.get(1).map_err(db)?)?,
        duplicate_count: as_u64(row.get(2).map_err(db)?)?,
        robots_excluded_count: as_u64(row.get(3).map_err(db)?)?,
        provider_error_count: as_u64(row.get(4).map_err(db)?)?,
        external_url_count: as_u64(row.get(5).map_err(db)?)?,
        blocked_url_count: as_u64(row.get(6).map_err(db)?)?,
        peak_expansion_count: as_u64(row.get(7).map_err(db)?)?,
        elapsed_millis: as_u64(row.get(8).map_err(db)?)?,
        time_budget_hit: row.get::<i64>(9).map_err(db)? == 1,
        duration_work_not_expanded: row.get::<i64>(10).map_err(db)? == 1,
        pagination_truncation_count: as_u64(row.get(11).map_err(db)?)?,
        next_admission_sequence: as_u64(row.get(12).map_err(db)?)?,
    })
}
async fn read_seed_provenance(
    connection: &turso::Connection,
    state_id: &str,
) -> Result<Vec<String>, CrawlTraversalRepositoryError> {
    let mut rows = connection.query("SELECT seed_id FROM crawl_url_seed_provenance WHERE crawl_url_state_id=?1 ORDER BY ordinal ASC, seed_id COLLATE BINARY ASC", [state_id]).await.map_err(db)?;
    let mut values = Vec::new();
    while let Some(row) = rows.next().await.map_err(db)? {
        values.push(row.get(0).map_err(db)?);
    }
    Ok(values)
}
