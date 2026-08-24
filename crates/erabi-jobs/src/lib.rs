//! Generic Tokio worker and durable progress boundaries for Erabi jobs.
//!
//! Generic leased execution, cooperative cancellation, bounded checkpoints,
//! and replayable progress services for Erabi jobs.

use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use erabi_db::{
    ErabiDatabase,
    repositories::{
        ConcurrencyState, JobFailureCode, JobId, JobKind, JobLease, JobRepository,
        JobRepositoryError, JobState, StaleJobRecovery,
    },
};
use futures_util::FutureExt;
use tokio::{
    sync::RwLock,
    time::{Instant, interval_at},
};

mod actions;
mod cancellation;
mod progress;

pub use actions::{
    JobAction, JobActionError, JobActionResult, JobActionService, RerunFullCrawlInput,
};
pub use cancellation::{CancellationController, CancellationToken};
pub use progress::{
    ProgressLiveHub, ProgressLiveHubError, ProgressPublication, ProgressPublisher,
    ProgressPublisherError, ProgressService, ProgressServiceError,
};

pub use erabi_db::repositories::{AcquiredJob, AttemptOutcome, JobAttempt, JobRecord, NewJob};
pub use erabi_db::repositories::{
    CURRENT_CHECKPOINT_SCHEMA_VERSION, CheckpointArtifactReference, CheckpointCompatibility,
    CheckpointEnvelope, CheckpointIdentity, CheckpointPosition, CheckpointRecord,
    CheckpointRecoveryAssessment, CheckpointRecoveryDisposition, CheckpointRepository,
    CheckpointRepositoryError, CheckpointUnitId, ExtractionResumePhase, ExtractionResumeState,
    MAX_CHECKPOINT_ARTIFACTS, MAX_CHECKPOINT_BYTES, MAX_CHECKPOINT_UNITS,
};
pub use erabi_db::repositories::{
    NewProgressEvent, ProgressAttemptId, ProgressEvent, ProgressEventId, ProgressKey,
    ProgressMetadata, ProgressMetadataCode, ProgressMetadataKey, ProgressMetadataValue,
    ProgressReplayPage, ProgressReplayRequest, ProgressRepository, ProgressRepositoryError,
    ProgressSequence, ProgressTerminalState,
};

/// Fixed bounded retry and lease policy for one generic worker runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerPolicy {
    /// Duration of one acquired lease. Must be at least two seconds so the
    /// whole-second durable timestamps leave time to renew before expiry.
    pub lease_duration_seconds: i64,
    /// Delay before a retry. The job's `max_attempts` supplies the hard bound.
    pub retry_delay_seconds: i64,
}

impl WorkerPolicy {
    /// A conservative deterministic policy suitable for local worker polling.
    #[must_use]
    pub const fn conservative() -> Self {
        Self {
            lease_duration_seconds: 30,
            retry_delay_seconds: 5,
        }
    }

    fn valid(self) -> bool {
        self.lease_duration_seconds >= 2 && self.retry_delay_seconds >= 0
    }

    fn heartbeat_interval(self) -> Duration {
        let seconds = (self.lease_duration_seconds / 3).max(1).unsigned_abs();
        Duration::from_secs(seconds)
    }
}

/// Context supplied to an individual handler. It contains only durable queue
/// identity and ownership evidence, never request bodies or scraped content.
#[derive(Clone, Debug)]
pub struct JobExecutionContext {
    job_id: JobId,
    kind: JobKind,
    attempt_number: u32,
    worker_id: String,
    lease: JobLease,
    cancellation: CancellationToken,
    checkpoint_writer: CheckpointWriter,
}

impl JobExecutionContext {
    #[must_use]
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    #[must_use]
    pub fn kind(&self) -> &JobKind {
        &self.kind
    }

    #[must_use]
    pub const fn attempt_number(&self) -> u32 {
        self.attempt_number
    }

    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    #[must_use]
    pub fn lease(&self) -> &JobLease {
        &self.lease
    }

    /// Returns the cooperative cancellation signal for this active turn.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Persists a bounded checkpoint while this worker still owns the attempt.
    ///
    /// # Errors
    /// Returns a typed validation, ownership, or durable persistence failure.
    pub async fn checkpoint(
        &self,
        checkpoint: &CheckpointEnvelope,
    ) -> Result<CheckpointRecord, JobRepositoryError> {
        self.checkpoint_writer.append(checkpoint).await
    }
}

#[derive(Clone, Debug)]
struct CheckpointWriter {
    database: ErabiDatabase,
    job_id: JobId,
    attempt_id: String,
    lease: Arc<RwLock<JobLease>>,
    persisted: Arc<AtomicBool>,
    initial_now: i64,
    started: Instant,
}

impl CheckpointWriter {
    async fn append(
        &self,
        checkpoint: &CheckpointEnvelope,
    ) -> Result<CheckpointRecord, JobRepositoryError> {
        let lease = self.lease.read().await.clone();
        let elapsed = i64::try_from(self.started.elapsed().as_secs())
            .map_err(|_| JobRepositoryError::QueueInvariant)?;
        let created_at = self
            .initial_now
            .checked_add(elapsed)
            .ok_or(JobRepositoryError::QueueInvariant)?;
        let record = JobRepository::new(&self.database)
            .append_checkpoint(
                &self.job_id,
                &self.attempt_id,
                &lease,
                checkpoint,
                created_at,
            )
            .await?;
        self.persisted.store(true, Ordering::Release);
        Ok(record)
    }

    async fn update_lease(&self, lease: JobLease) {
        *self.lease.write().await = lease;
    }

    fn persisted(&self) -> bool {
        self.persisted.load(Ordering::Acquire)
    }
}

/// A sanitized expected handler error. Error payloads are deliberately not
/// persisted; only [`JobFailureCode::HandlerFailed`] is durable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobExecutionError;

/// A typed generic future-work handler. The runtime catches panics at this
/// boundary so a single handler cannot terminate Axum or unrelated workers.
pub trait JobHandler: Send + Sync {
    /// Executes a leased job. Implementations must treat the context as
    /// immutable ownership evidence and return an expected failure rather than
    /// panicking for normal operational errors.
    fn execute(
        &self,
        context: JobExecutionContext,
    ) -> impl Future<Output = Result<(), JobExecutionError>> + Send;
}

/// Outcome of one non-blocking `JobRuntime::execute_next_at` worker turn.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerTurn {
    Idle,
    Succeeded {
        job_id: JobId,
    },
    RetryScheduled {
        job_id: JobId,
        failure: JobFailureCode,
    },
    Failed {
        job_id: JobId,
        failure: JobFailureCode,
    },
    Cancelled {
        job_id: JobId,
        checkpoint_persisted: bool,
    },
}

/// Failure that prevents the generic worker boundary from safely proceeding.
#[derive(Debug, thiserror::Error)]
pub enum JobRuntimeError {
    #[error("worker policy is invalid")]
    InvalidPolicy,
    #[error("durable job queue operation failed")]
    Repository(#[source] JobRepositoryError),
}

/// Generic Tokio-ready single-worker runtime. A caller can run one turn from a
/// supervisor/poll loop without coupling job execution to HTTP route lifetime.
#[derive(Clone, Debug)]
pub struct JobRuntime<'database> {
    database: ErabiDatabase,
    repository: JobRepository<'database>,
    worker_id: String,
    policy: WorkerPolicy,
    cancellation: CancellationController,
}

impl<'database> JobRuntime<'database> {
    /// Creates a worker whose identity is also persisted in every lease and
    /// attempt. The identity is durable evidence, not a transient task id.
    ///
    /// # Errors
    /// Returns an error when the worker identity or bounded lease/retry policy
    /// is invalid.
    pub fn new(
        database: &'database ErabiDatabase,
        worker_id: impl Into<String>,
        policy: WorkerPolicy,
    ) -> Result<Self, JobRuntimeError> {
        Self::with_cancellation_controller(
            database,
            worker_id,
            policy,
            CancellationController::default(),
        )
    }

    /// Creates a worker joined to a process/runtime cancellation controller.
    /// The controller is the bridge used by graceful shutdown to signal active
    /// handlers without aborting them.
    ///
    /// # Errors
    /// Returns an error when the worker identity or bounded lease/retry policy
    /// is invalid.
    pub fn with_cancellation_controller(
        database: &'database ErabiDatabase,
        worker_id: impl Into<String>,
        policy: WorkerPolicy,
        cancellation: CancellationController,
    ) -> Result<Self, JobRuntimeError> {
        let worker_id = worker_id.into();
        if !policy.valid() || worker_id.is_empty() || worker_id.len() > 128 {
            return Err(JobRuntimeError::InvalidPolicy);
        }
        Ok(Self {
            database: database.clone(),
            repository: JobRepository::new(database),
            worker_id,
            policy,
            cancellation,
        })
    }

    /// Returns the controller used by handlers and process shutdown to signal
    /// active work without aborting its task.
    #[must_use]
    pub fn cancellation_controller(&self) -> CancellationController {
        self.cancellation.clone()
    }

    /// Requests cancellation for one job. Queued work is durably cancelled so
    /// it cannot be scheduled; active work receives the cooperative signal.
    ///
    /// # Errors
    /// Returns an error when the durable queue cannot inspect or update the job.
    pub async fn request_cancellation(
        &self,
        job_id: &JobId,
        now: i64,
    ) -> Result<JobState, JobRuntimeError> {
        request_job_cancellation(&self.database, &self.cancellation, job_id, now).await
    }

    /// Executes at most one eligible job using supplied deterministic time.
    /// Panics become a bounded retry/failure outcome rather than escaping the
    /// worker boundary.
    ///
    /// # Errors
    /// Returns an error when durable lease/attempt state cannot be read or
    /// updated safely, or when the configured retry time overflows.
    pub async fn execute_next_at<H: JobHandler>(
        &self,
        handler: &H,
        now: i64,
    ) -> Result<WorkerTurn, JobRuntimeError> {
        if self.cancellation.shutdown_requested() {
            return Ok(WorkerTurn::Idle);
        }
        let Some(acquired) = self
            .repository
            .acquire_next(&self.worker_id, now, self.policy.lease_duration_seconds)
            .await
            .map_err(JobRuntimeError::Repository)?
        else {
            return Ok(WorkerTurn::Idle);
        };
        let cancellation = self.cancellation.register(&acquired.job.id);
        let current_lease = acquired
            .job
            .lease
            .clone()
            .ok_or(JobRuntimeError::Repository(
                JobRepositoryError::QueueInvariant,
            ));
        let current_lease = match current_lease {
            Ok(lease) => lease,
            Err(error) => {
                self.cancellation.release(&acquired.job.id, false);
                return Err(error);
            }
        };
        let started = Instant::now();
        let checkpoint_writer = CheckpointWriter {
            database: self.database.clone(),
            job_id: acquired.job.id.clone(),
            attempt_id: acquired.attempt.id.clone(),
            lease: Arc::new(RwLock::new(current_lease.clone())),
            persisted: Arc::new(AtomicBool::new(false)),
            initial_now: now,
            started,
        };
        let context = JobExecutionContext {
            job_id: acquired.job.id.clone(),
            kind: acquired.job.kind.clone(),
            attempt_number: acquired.attempt.attempt_number,
            worker_id: self.worker_id.clone(),
            lease: current_lease,
            cancellation,
            checkpoint_writer,
        };
        let outcome = self.execute_acquired(handler, context, now, started).await;
        self.cancellation.release(
            &acquired.job.id,
            matches!(
                outcome,
                Ok(WorkerTurn::Succeeded { .. }
                    | WorkerTurn::Failed { .. }
                    | WorkerTurn::Cancelled { .. })
            ),
        );
        outcome
    }

    async fn execute_acquired<H: JobHandler>(
        &self,
        handler: &H,
        context: JobExecutionContext,
        now: i64,
        started: Instant,
    ) -> Result<WorkerTurn, JobRuntimeError> {
        let mut current_lease = context.lease.clone();
        let mut heartbeat = interval_at(
            started + self.policy.heartbeat_interval(),
            self.policy.heartbeat_interval(),
        );
        let handler = AssertUnwindSafe(handler.execute(context.clone())).catch_unwind();
        tokio::pin!(handler);

        let result = loop {
            tokio::select! {
                result = &mut handler => break result,
                _ = heartbeat.tick() => {
                    let heartbeat_now = current_queue_time(now, started)?;
                    match self
                        .repository
                        .heartbeat(
                            &context.job_id,
                            &current_lease,
                            heartbeat_now,
                            self.policy.lease_duration_seconds,
                        )
                        .await
                    {
                        Ok(renewed_lease) => {
                            current_lease = renewed_lease.clone();
                            context.checkpoint_writer.update_lease(renewed_lease).await;
                        }
                        Err(error) => {
                            // Lease loss revokes durable authority first, then signals
                            // the handler to reach its existing cooperative boundary.
                            // Its eventual result is intentionally discarded.
                            context.cancellation.cancel();
                            let _ = handler.await;
                            return Err(JobRuntimeError::Repository(error));
                        }
                    }
                }
            }
        };
        let completed_at = current_queue_time(now, started)?;
        if context.cancellation.is_cancelled() {
            let checkpoint_persisted = context.checkpoint_writer.persisted();
            self.repository
                .cancel(&context.job_id, &current_lease, completed_at)
                .await
                .map_err(JobRuntimeError::Repository)?;
            return Ok(WorkerTurn::Cancelled {
                job_id: context.job_id,
                checkpoint_persisted,
            });
        }
        match result {
            Ok(Ok(())) => {
                self.repository
                    .succeed(&context.job_id, &current_lease, completed_at)
                    .await
                    .map_err(JobRuntimeError::Repository)?;
                Ok(WorkerTurn::Succeeded {
                    job_id: context.job_id,
                })
            }
            Ok(Err(JobExecutionError)) => {
                self.record_failure(
                    &context,
                    &current_lease,
                    completed_at,
                    JobFailureCode::HandlerFailed,
                )
                .await
            }
            Err(_) => {
                self.record_failure(
                    &context,
                    &current_lease,
                    completed_at,
                    JobFailureCode::HandlerPanicked,
                )
                .await
            }
        }
    }

    async fn record_failure(
        &self,
        context: &JobExecutionContext,
        lease: &JobLease,
        now: i64,
        failure: JobFailureCode,
    ) -> Result<WorkerTurn, JobRuntimeError> {
        let retry_at = now
            .checked_add(self.policy.retry_delay_seconds)
            .ok_or(JobRuntimeError::InvalidPolicy)?;
        match self
            .repository
            .fail(&context.job_id, lease, now, failure, retry_at)
            .await
            .map_err(JobRuntimeError::Repository)?
        {
            JobState::Queued => Ok(WorkerTurn::RetryScheduled {
                job_id: context.job_id.clone(),
                failure,
            }),
            JobState::Failed => Ok(WorkerTurn::Failed {
                job_id: context.job_id.clone(),
                failure,
            }),
            _ => Err(JobRuntimeError::Repository(
                JobRepositoryError::QueueInvariant,
            )),
        }
    }
}

/// Shared Task 3 cancellation boundary used by both workers and explicit API
/// actions. It durably cancels queued work and only signals active work.
///
/// # Errors
/// Returns a typed runtime error when the durable state cannot be inspected or
/// updated safely.
pub async fn request_job_cancellation(
    database: &ErabiDatabase,
    cancellation: &CancellationController,
    job_id: &JobId,
    now: i64,
) -> Result<JobState, JobRuntimeError> {
    cancellation.request(job_id);
    let state = JobRepository::new(database)
        .cancel_queued(job_id, now)
        .await
        .map_err(JobRuntimeError::Repository)?;
    if matches!(
        state,
        JobState::Succeeded | JobState::Failed | JobState::Cancelled
    ) {
        cancellation.retire_after_terminal_boundary(job_id);
    }
    Ok(state)
}

fn current_queue_time(initial_now: i64, started: Instant) -> Result<i64, JobRuntimeError> {
    let elapsed =
        i64::try_from(started.elapsed().as_secs()).map_err(|_| JobRuntimeError::InvalidPolicy)?;
    initial_now
        .checked_add(elapsed)
        .ok_or(JobRuntimeError::InvalidPolicy)
}

/// Performs Plan 03's startup hooks with durable queue state as authority.
/// This does not start a handler or introduce cancellation semantics.
///
/// # Errors
/// Returns an error when stale-job recovery or durable concurrency rebuilding
/// detects queue corruption or cannot complete its database work.
pub async fn recover_and_rebuild_at(
    database: &ErabiDatabase,
    now: i64,
) -> Result<(StaleJobRecovery, ConcurrencyState), JobRepositoryError> {
    let repository = JobRepository::new(database);
    let recovery = repository.recover_stale_jobs(now).await?;
    let concurrency = repository.rebuild_concurrency_state(now).await?;
    Ok((recovery, concurrency))
}
