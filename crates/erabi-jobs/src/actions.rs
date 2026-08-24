//! Explicit retry, recovery, cancellation, and queue actions.

use erabi_db::{
    ErabiDatabase,
    repositories::{
        CheckpointCompatibility, CheckpointIdentity, CheckpointRepositoryError, CrawlRunRepository,
        JobId, JobKind, JobRecord, JobRepository, JobRepositoryError, JobState,
    },
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

/// Typed semantic failures at the action boundary.
#[derive(Debug, thiserror::Error)]
pub enum JobActionError {
    #[error("the requested job or run does not exist")]
    NotFound,
    #[error("the requested action is not legal for the current lifecycle state")]
    IllegalLifecycleState,
    #[error("the job has exhausted its bounded attempts")]
    AttemptsExhausted,
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
        let source = self.source(job_id).await?;
        if source.current_attempt == 0 {
            return Err(JobActionError::IllegalLifecycleState);
        }
        if source.current_attempt >= source.max_attempts {
            return Err(JobActionError::AttemptsExhausted);
        }
        let snapshot = self.snapshot_for(&source).await?;
        let remaining_attempts = source.max_attempts - source.current_attempt;
        let job = JobRepository::new(&self.database)
            .enqueue_action_child(
                job_id,
                action_kind(JobAction::Retry)?,
                now,
                snapshot.as_ref(),
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
        let source = self.source(job_id).await?;
        if source.current_attempt >= source.max_attempts {
            return Err(JobActionError::AttemptsExhausted);
        }
        let (snapshot, failed_part_count) = self.compatible_checkpoint(&source).await?;
        if failed_part_count == 0 {
            return Err(JobActionError::IllegalLifecycleState);
        }
        let job = JobRepository::new(&self.database)
            .enqueue_action_child(
                job_id,
                action_kind(JobAction::RetryFailedParts)?,
                now,
                Some(&snapshot),
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

    /// Creates a new run/job child using the original immutable snapshot and
    /// explicit parent lineage.
    ///
    /// # Errors
    /// Returns a typed lifecycle, lineage, or persistence error.
    pub async fn rerun_full_crawl(
        &self,
        job_id: &JobId,
        now: i64,
    ) -> Result<JobActionResult, JobActionError> {
        self.new_snapshot_child(job_id, now, JobAction::RerunFullCrawl)
            .await
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
        let source = self.source(job_id).await?;
        let (snapshot, _) = self.compatible_checkpoint(&source).await?;
        let job = JobRepository::new(&self.database)
            .enqueue_action_child(
                job_id,
                action_kind(JobAction::ResumeCheckpoint)?,
                now,
                Some(&snapshot),
                None,
            )
            .await
            .map_err(action_repository_error)?;
        Ok(result(JobAction::ResumeCheckpoint, job, None))
    }

    /// Starts a new child from the beginning. It deliberately does not load,
    /// validate, or copy checkpoint evidence.
    ///
    /// # Errors
    /// Returns a typed lifecycle, lineage, or persistence error.
    pub async fn restart_from_beginning(
        &self,
        job_id: &JobId,
        now: i64,
    ) -> Result<JobActionResult, JobActionError> {
        self.new_snapshot_child(job_id, now, JobAction::RestartFromBeginning)
            .await
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

    async fn source(&self, job_id: &JobId) -> Result<JobRecord, JobActionError> {
        let job = JobRepository::new(&self.database)
            .job(job_id)
            .await
            .map_err(action_repository_error)?;
        if matches!(job.state, JobState::Queued | JobState::Running) {
            return Err(JobActionError::IllegalLifecycleState);
        }
        Ok(job)
    }

    async fn new_snapshot_child(
        &self,
        job_id: &JobId,
        now: i64,
        action: JobAction,
    ) -> Result<JobActionResult, JobActionError> {
        let source = self.source(job_id).await?;
        let snapshot = self.snapshot_for(&source).await?;
        let job = JobRepository::new(&self.database)
            .enqueue_action_child(job_id, action_kind(action)?, now, snapshot.as_ref(), None)
            .await
            .map_err(action_repository_error)?;
        Ok(result(action, job, None))
    }

    async fn snapshot_for(
        &self,
        source: &JobRecord,
    ) -> Result<Option<erabi_domain::CrawlRunSnapshot>, JobActionError> {
        match source.crawl_run_id.as_deref() {
            Some(run_id) => Ok(Some(
                CrawlRunRepository::new(&self.database)
                    .snapshot_by_stored_id(run_id)
                    .await
                    .map_err(|_| JobActionError::NotFound)?,
            )),
            None => Ok(None),
        }
    }

    async fn compatible_checkpoint(
        &self,
        source: &JobRecord,
    ) -> Result<(erabi_domain::CrawlRunSnapshot, usize), JobActionError> {
        let run_id = source
            .crawl_run_id
            .as_deref()
            .ok_or(JobActionError::CheckpointIncompatible)?;
        let snapshot = CrawlRunRepository::new(&self.database)
            .snapshot_by_stored_id(run_id)
            .await
            .map_err(|_| JobActionError::NotFound)?;
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

    #[test]
    fn malformed_checkpoint_evidence_maps_to_the_unsafe_action_error() {
        assert!(matches!(
            checkpoint_error(JobRepositoryError::Checkpoint(
                CheckpointRepositoryError::Malformed
            )),
            JobActionError::CheckpointUnsafe
        ));
    }
}
