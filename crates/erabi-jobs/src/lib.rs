//! Generic Tokio worker boundary over Erabi's durable leased queue.
//!
//! This crate intentionally has no crawl, extraction, export, progress, or
//! cancellation implementation. Future plans supply handlers behind the typed
//! [`JobHandler`] boundary.

use std::{panic::AssertUnwindSafe, time::Duration};

use erabi_db::{
    ErabiDatabase,
    repositories::{
        ConcurrencyState, JobFailureCode, JobId, JobKind, JobLease, JobRepository,
        JobRepositoryError, JobState, StaleJobRecovery,
    },
};
use futures_util::FutureExt;
use tokio::time::{Instant, interval_at};

pub use erabi_db::repositories::{AcquiredJob, AttemptOutcome, JobAttempt, JobRecord, NewJob};

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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobExecutionContext {
    job_id: JobId,
    kind: JobKind,
    attempt_number: u32,
    worker_id: String,
    lease: JobLease,
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
    repository: JobRepository<'database>,
    worker_id: String,
    policy: WorkerPolicy,
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
        let worker_id = worker_id.into();
        if !policy.valid() || worker_id.is_empty() || worker_id.len() > 128 {
            return Err(JobRuntimeError::InvalidPolicy);
        }
        Ok(Self {
            repository: JobRepository::new(database),
            worker_id,
            policy,
        })
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
        let Some(acquired) = self
            .repository
            .acquire_next(&self.worker_id, now, self.policy.lease_duration_seconds)
            .await
            .map_err(JobRuntimeError::Repository)?
        else {
            return Ok(WorkerTurn::Idle);
        };
        let context = JobExecutionContext {
            job_id: acquired.job.id.clone(),
            kind: acquired.job.kind.clone(),
            attempt_number: acquired.attempt.attempt_number,
            worker_id: self.worker_id.clone(),
            lease: acquired
                .job
                .lease
                .clone()
                .ok_or(JobRuntimeError::Repository(
                    JobRepositoryError::QueueInvariant,
                ))?,
        };
        let started = Instant::now();
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
                        Ok(renewed_lease) => current_lease = renewed_lease,
                        Err(error) => {
                            // Task 1 has no cancellation/checkpoint protocol. Let the
                            // handler reach its own boundary, but never let its stale
                            // result mutate durable queue state.
                            let _ = handler.await;
                            return Err(JobRuntimeError::Repository(error));
                        }
                    }
                }
            }
        };
        let completed_at = current_queue_time(now, started)?;
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

fn current_queue_time(initial_now: i64, started: Instant) -> Result<i64, JobRuntimeError> {
    let elapsed =
        i64::try_from(started.elapsed().as_secs()).map_err(|_| JobRuntimeError::InvalidPolicy)?;
    initial_now
        .checked_add(elapsed)
        .ok_or(JobRuntimeError::InvalidPolicy)
}

/// Performs Plan 03's startup hooks with durable queue state as authority.
/// This does not start a handler or introduce progress/cancellation semantics.
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
