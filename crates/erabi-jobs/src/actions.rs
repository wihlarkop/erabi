//! Explicit retry, recovery, cancellation, and queue actions.

use erabi_db::{
    ErabiDatabase,
    repositories::{
        ActionRunAssociation, CheckpointCompatibility, CheckpointIdentity,
        CheckpointRepositoryError, CrawlRunRepository, CrawlRunRepositoryError, JobId, JobKind,
        JobRecord, JobRepository, JobRepositoryError, JobState,
    },
};
use erabi_domain::{
    CrawlRunSnapshot, CrawlRunSnapshotDraft, RobotsAudit, RobotsDecision, SnapshotError,
};

use crate::{CancellationController, JobRuntimeError, request_job_cancellation};

/// A durable user-requested operation with distinct recovery semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobAction {
    RetryFailedParts,
    RerunFullCrawl,
    ResumeCheckpoint,
    RestartFromBeginning,
    Retry,
    Cancel,
    Reprioritize,
    Remove,
}

/// Safe result metadata for one action. Checkpoint contents and database
/// internals are intentionally absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobActionResult {
    pub action: JobAction,
    pub job_id: JobId,
    pub parent_job_id: Option<JobId>,
    pub crawl_run_id: Option<String>,
    pub state: JobState,
    pub failed_part_count: Option<usize>,
}

/// Explicit evidence for one independent full-crawl rerun.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RerunFullCrawlInput {
    pub robots_override_reason: Option<String>,
}

/// Typed semantic failures at the action boundary.
#[derive(Debug, thiserror::Error)]
pub enum JobActionError {
    #[error("the requested job or run does not exist")]
    NotFound,
    #[error("the requested action is not legal for the current lifecycle state")]
    IllegalLifecycleState,
    #[error("the job has exhausted its bounded attempts")]
    AttemptsExhausted,
    #[error("the job already has an explicit retry continuation")]
    RetryAlreadyContinued,
    #[error("Rerun Full Crawl requires a durable Crawl Run")]
    CrawlRunRequired,
    #[error("an independent rerun with a robots override requires a new reason")]
    RobotsOverrideReasonRequired,
    #[error("the submitted robots override reason is invalid")]
    RobotsOverrideReasonInvalid,
    #[error("no durable checkpoint is available")]
    CheckpointMissing,
    #[error("the durable checkpoint is unsafe to use")]
    CheckpointUnsafe,
    #[error("the durable checkpoint does not match the immutable run snapshot")]
    CheckpointIncompatible,
    #[error("the job is not safe to remove")]
    NotRemovable,
    #[error("the job is not currently reprioritizable")]
    NotReprioritizable,
    #[error("a concurrent action already owns this transition")]
    ConcurrentTransition,
    #[error("durable job action operation failed")]
    Repository(#[source] JobRepositoryError),
    #[error("durable cancellation operation failed")]
    Cancellation(#[source] JobRuntimeError),
}

/// Service boundary for explicit job actions. It owns no lifecycle state: all
/// durable decisions remain in the transactional repository primitives.
#[derive(Clone, Debug)]
pub struct JobActionService {
    database: ErabiDatabase,
    cancellation: CancellationController,
}

impl JobActionService {
    #[must_use]
    pub fn new(database: ErabiDatabase, cancellation: CancellationController) -> Self {
        Self {
            database,
            cancellation,
        }
    }

    /// Retries one bounded execution through a new child job without rewriting
    /// any prior attempt evidence. The remaining attempt budget is carried to
    /// the child so an explicit retry cannot bypass the source limit.
    ///
    /// # Errors
    /// Returns a typed lifecycle, attempt, lineage, or persistence error.
    pub async fn retry(&self, job_id: &JobId, now: i64) -> Result<JobActionResult, JobActionError> {
        let source = self.recoverable_source(job_id).await?;
        if source.current_attempt == 0 {
            return Err(JobActionError::IllegalLifecycleState);
        }
        if source.current_attempt >= source.max_attempts {
            return Err(JobActionError::AttemptsExhausted);
        }
        self.snapshot_for(&source).await?;
        let remaining_attempts = source.max_attempts - source.current_attempt;
        let job = JobRepository::new(&self.database)
            .enqueue_action_child(
                job_id,
                action_kind(JobAction::Retry)?,
                now,
                same_run_association(&source),
                Some(remaining_attempts),
            )
            .await
            .map_err(action_repository_error)?;
        Ok(result(JobAction::Retry, job, None))
    }

    /// Queues a typed failed-parts action only when the latest compatible
    /// checkpoint identifies at least one failed unit. Successful units are
    /// never rewritten or copied into the child as failures.
    ///
    /// # Errors
    /// Returns a typed checkpoint, lifecycle, attempt, lineage, or persistence
    /// error.
    pub async fn retry_failed_parts(
        &self,
        job_id: &JobId,
        now: i64,
    ) -> Result<JobActionResult, JobActionError> {
        let source = self.recoverable_source(job_id).await?;
        if source.current_attempt >= source.max_attempts {
            return Err(JobActionError::AttemptsExhausted);
        }
        let (_snapshot, failed_part_count) = self.compatible_checkpoint(&source).await?;
        if failed_part_count == 0 {
            return Err(JobActionError::IllegalLifecycleState);
        }
        let job = JobRepository::new(&self.database)
            .enqueue_action_child(
                job_id,
                action_kind(JobAction::RetryFailedParts)?,
                now,
                ActionRunAssociation::SameSourceRun,
                Some(source.max_attempts - source.current_attempt),
            )
            .await
            .map_err(action_repository_error)?;
        Ok(result(
            JobAction::RetryFailedParts,
            job,
            Some(failed_part_count),
        ))
    }

    /// Creates an independent run/job child using explicit robots override
    /// evidence where the source run used an override.
    ///
    /// # Errors
    /// Returns a typed lifecycle, lineage, or persistence error.
    pub async fn rerun_full_crawl(
        &self,
        job_id: &JobId,
        now: i64,
        input: RerunFullCrawlInput,
    ) -> Result<JobActionResult, JobActionError> {
        let source = self.terminal_source(job_id).await?;
        let snapshot = self
            .snapshot_for(&source)
            .await?
            .ok_or(JobActionError::CrawlRunRequired)?;
        let rerun_snapshot = independent_rerun_snapshot(&snapshot, input, now)?;
        let job = JobRepository::new(&self.database)
            .enqueue_action_child(
                job_id,
                action_kind(JobAction::RerunFullCrawl)?,
                now,
                ActionRunAssociation::NewIndependentRun(&rerun_snapshot),
                None,
            )
            .await
            .map_err(action_repository_error)?;
        Ok(result(JobAction::RerunFullCrawl, job, None))
    }

    /// Resumes only from a current, structurally valid, snapshot-compatible
    /// durable checkpoint.
    ///
    /// # Errors
    /// Returns a typed checkpoint, lifecycle, lineage, or persistence error.
    pub async fn resume(
        &self,
        job_id: &JobId,
        now: i64,
    ) -> Result<JobActionResult, JobActionError> {
        let source = self.recoverable_source(job_id).await?;
        let (_snapshot, _) = self.compatible_checkpoint(&source).await?;
        let job = JobRepository::new(&self.database)
            .enqueue_action_child(
                job_id,
                action_kind(JobAction::ResumeCheckpoint)?,
                now,
                ActionRunAssociation::SameSourceRun,
                None,
            )
            .await
            .map_err(action_repository_error)?;
        Ok(result(JobAction::ResumeCheckpoint, job, None))
    }

    /// Restarts the same durable run from the beginning without loading,
    /// validating, or copying checkpoint evidence.
    ///
    /// # Errors
    /// Returns a typed lifecycle, lineage, or persistence error.
    pub async fn restart_from_beginning(
        &self,
        job_id: &JobId,
        now: i64,
    ) -> Result<JobActionResult, JobActionError> {
        let source = self.recoverable_source(job_id).await?;
        self.snapshot_for(&source).await?;
        let job = JobRepository::new(&self.database)
            .enqueue_action_child(
                job_id,
                action_kind(JobAction::RestartFromBeginning)?,
                now,
                same_run_association(&source),
                None,
            )
            .await
            .map_err(action_repository_error)?;
        Ok(result(JobAction::RestartFromBeginning, job, None))
    }

    /// Reuses the Task 3 cooperative cancellation boundary for queued and
    /// active work. Terminal requests are handled idempotently by the existing
    /// repository semantics.
    ///
    /// # Errors
    /// Returns a typed cancellation or persistence error.
    pub async fn cancel(
        &self,
        job_id: &JobId,
        now: i64,
    ) -> Result<JobActionResult, JobActionError> {
        let state = request_job_cancellation(&self.database, &self.cancellation, job_id, now)
            .await
            .map_err(JobActionError::Cancellation)?;
        let job = JobRepository::new(&self.database)
            .job(job_id)
            .await
            .map_err(action_repository_error)?;
        Ok(result(JobAction::Cancel, job, None).with_state(state))
    }

    /// Moves queued work while preserving deterministic queue tie-breakers.
    ///
    /// # Errors
    /// Returns a typed queue-state or persistence error.
    pub async fn reprioritize(
        &self,
        job_id: &JobId,
        priority: i32,
        scheduled_at: Option<i64>,
        now: i64,
    ) -> Result<JobActionResult, JobActionError> {
        let existing = JobRepository::new(&self.database)
            .job(job_id)
            .await
            .map_err(action_repository_error)?;
        let job = JobRepository::new(&self.database)
            .reprioritize(
                job_id,
                priority,
                scheduled_at.unwrap_or(existing.scheduled_at),
                now,
            )
            .await
            .map_err(action_repository_error)?;
        Ok(result(JobAction::Reprioritize, job, None))
    }

    /// Removes only safe, never-started, unlinked queue work.
    ///
    /// # Errors
    /// Returns a typed removal-safety or persistence error.
    pub async fn remove(&self, job_id: &JobId) -> Result<JobActionResult, JobActionError> {
        JobRepository::new(&self.database)
            .remove_if_safe(job_id)
            .await
            .map_err(action_repository_error)?;
        Ok(JobActionResult {
            action: JobAction::Remove,
            job_id: job_id.clone(),
            parent_job_id: None,
            crawl_run_id: None,
            state: JobState::Cancelled,
            failed_part_count: None,
        })
    }

    /// Returns a terminal job eligible for an independent full rerun.
    async fn terminal_source(&self, job_id: &JobId) -> Result<JobRecord, JobActionError> {
        let job = JobRepository::new(&self.database)
            .job(job_id)
            .await
            .map_err(action_repository_error)?;
        if matches!(job.state, JobState::Queued | JobState::Running) {
            return Err(JobActionError::IllegalLifecycleState);
        }
        Ok(job)
    }

    /// Returns failed/cancelled work eligible for in-place recovery. A
    /// successful job keeps its completed evidence; a new full crawl is the
    /// explicit action for running it again.
    async fn recoverable_source(&self, job_id: &JobId) -> Result<JobRecord, JobActionError> {
        let job = self.terminal_source(job_id).await?;
        if !matches!(job.state, JobState::Failed | JobState::Cancelled) {
            return Err(JobActionError::IllegalLifecycleState);
        }
        Ok(job)
    }

    async fn snapshot_for(
        &self,
        source: &JobRecord,
    ) -> Result<Option<CrawlRunSnapshot>, JobActionError> {
        match source.crawl_run_id.as_deref() {
            Some(run_id) => Ok(Some(
                CrawlRunRepository::new(&self.database)
                    .snapshot_by_stored_id(run_id)
                    .await
                    .map_err(run_repository_error)?,
            )),
            None => Ok(None),
        }
    }

    async fn compatible_checkpoint(
        &self,
        source: &JobRecord,
    ) -> Result<(CrawlRunSnapshot, usize), JobActionError> {
        let run_id = source
            .crawl_run_id
            .as_deref()
            .ok_or(JobActionError::CheckpointIncompatible)?;
        let snapshot = CrawlRunRepository::new(&self.database)
            .snapshot_by_stored_id(run_id)
            .await
            .map_err(run_repository_error)?;
        let identity = CheckpointIdentity::new(
            run_id,
            snapshot.snapshot_hash(),
            snapshot.checkpoint_compatibility_hash(),
        )
        .map_err(|_| JobActionError::CheckpointUnsafe)?;
        let checkpoint = JobRepository::new(&self.database)
            .latest_checkpoint(&source.id)
            .await
            .map_err(checkpoint_error)?
            .ok_or(JobActionError::CheckpointMissing)?;
        if checkpoint.checkpoint.compatibility_with(&identity)
            != CheckpointCompatibility::Compatible
        {
            return Err(JobActionError::CheckpointIncompatible);
        }
        Ok((snapshot, checkpoint.checkpoint.failed_units.len()))
    }
}

fn same_run_association(source: &JobRecord) -> ActionRunAssociation<'static> {
    if source.crawl_run_id.is_some() {
        ActionRunAssociation::SameSourceRun
    } else {
        ActionRunAssociation::None
    }
}

fn independent_rerun_snapshot(
    source: &CrawlRunSnapshot,
    input: RerunFullCrawlInput,
    now: i64,
) -> Result<CrawlRunSnapshot, JobActionError> {
    let occurred_at = now.to_string();
    let robots = match source.robots().decision() {
        RobotsDecision::Respect => RobotsAudit::respect(
            source.robots().actor(),
            &occurred_at,
            source.robots().affected_scope(),
            source.robots().user_agent(),
            source.robots().crawler_version_id(),
        ),
        RobotsDecision::Override { .. } => {
            let reason = input
                .robots_override_reason
                .ok_or(JobActionError::RobotsOverrideReasonRequired)?;
            RobotsAudit::override_with_reason(
                reason,
                source.robots().actor(),
                &occurred_at,
                source.robots().affected_scope(),
                source.robots().user_agent(),
                source.robots().crawler_version_id(),
            )
            .map_err(robots_override_reason_error)?
        }
    };
    CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
        run_type: source.run_type(),
        configuration: source.configuration().clone(),
        selected_seed_ids: source.selected_seed_ids().to_vec(),
        run_profile_id: source.run_profile_id(),
        settings: source.settings().clone(),
        robots,
        actor: source.actor().to_owned(),
        created_at: occurred_at,
    })
    .map_err(robots_override_reason_error)
}

fn robots_override_reason_error(_: SnapshotError) -> JobActionError {
    JobActionError::RobotsOverrideReasonInvalid
}

fn run_repository_error(error: CrawlRunRepositoryError) -> JobActionError {
    match error {
        CrawlRunRepositoryError::NotFound => JobActionError::NotFound,
        CrawlRunRepositoryError::Database(error) => {
            JobActionError::Repository(JobRepositoryError::Database(error))
        }
    }
}

impl JobActionResult {
    fn with_state(mut self, state: JobState) -> Self {
        self.state = state;
        self
    }
}

fn result(action: JobAction, job: JobRecord, failed_part_count: Option<usize>) -> JobActionResult {
    JobActionResult {
        action,
        job_id: job.id,
        parent_job_id: job.parent_job_id,
        crawl_run_id: job.crawl_run_id,
        state: job.state,
        failed_part_count,
    }
}

fn action_kind(action: JobAction) -> Result<JobKind, JobActionError> {
    let value = match action {
        JobAction::RetryFailedParts => "RETRY_FAILED_PARTS",
        JobAction::RerunFullCrawl => "RERUN_FULL_CRAWL",
        JobAction::ResumeCheckpoint => "RESUME_CHECKPOINT",
        JobAction::RestartFromBeginning => "RESTART_FROM_BEGINNING",
        JobAction::Retry => "RETRY",
        JobAction::Cancel => "CANCEL",
        JobAction::Reprioritize => "REPRIORITIZE",
        JobAction::Remove => "REMOVE",
    };
    JobKind::new(value).map_err(JobActionError::Repository)
}

fn checkpoint_error(error: JobRepositoryError) -> JobActionError {
    match error {
        JobRepositoryError::Checkpoint(
            CheckpointRepositoryError::Malformed
            | CheckpointRepositoryError::Inconsistent
            | CheckpointRepositoryError::InvalidEnvelope
            | CheckpointRepositoryError::PayloadTooLarge
            | CheckpointRepositoryError::Serialization,
        ) => JobActionError::CheckpointUnsafe,
        JobRepositoryError::Checkpoint(CheckpointRepositoryError::NotFound) => {
            JobActionError::NotFound
        }
        other => action_repository_error(other),
    }
}

fn action_repository_error(error: JobRepositoryError) -> JobActionError {
    match error {
        JobRepositoryError::NotFound => JobActionError::NotFound,
        JobRepositoryError::AttemptsExhausted => JobActionError::AttemptsExhausted,
        JobRepositoryError::RetryAlreadyContinued => JobActionError::RetryAlreadyContinued,
        JobRepositoryError::RemovalUnsafe => JobActionError::NotRemovable,
        JobRepositoryError::NotReprioritizable => JobActionError::NotReprioritizable,
        JobRepositoryError::ActionAlreadyActive | JobRepositoryError::LeaseLost => {
            JobActionError::ConcurrentTransition
        }
        JobRepositoryError::IllegalTransition => JobActionError::IllegalLifecycleState,
        other => JobActionError::Repository(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use erabi_db::DbError;

    #[test]
    fn malformed_checkpoint_evidence_maps_to_the_unsafe_action_error() {
        assert!(matches!(
            checkpoint_error(JobRepositoryError::Checkpoint(
                CheckpointRepositoryError::Malformed
            )),
            JobActionError::CheckpointUnsafe
        ));
    }

    #[test]
    fn durable_run_invariant_evidence_is_not_mapped_to_not_found() {
        assert!(matches!(
            run_repository_error(CrawlRunRepositoryError::Database(DbError::Invariant(
                "snapshot mismatch".into()
            ))),
            JobActionError::Repository(JobRepositoryError::Database(DbError::Invariant(_)))
        ));
    }
}
