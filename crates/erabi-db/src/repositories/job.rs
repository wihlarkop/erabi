//! Durable job queue persistence with transactional ownership rules.

use std::fmt;

use turso::{Connection, transaction::TransactionBehavior};
use uuid::Uuid;

use crate::{DbError, ErabiDatabase};

/// Stable durable identity for a queued unit of work.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct JobId(String);

impl JobId {
    /// Creates a new time-sortable job identity.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// The lifecycle controlled exclusively by [`JobRepository`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobState {
    const fn as_sql(self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Running => "RUNNING",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }

    fn parse(value: &str) -> Result<Self, JobRepositoryError> {
        match value {
            "QUEUED" => Ok(Self::Queued),
            "RUNNING" => Ok(Self::Running),
            "SUCCEEDED" => Ok(Self::Succeeded),
            "FAILED" => Ok(Self::Failed),
            "CANCELLED" => Ok(Self::Cancelled),
            _ => Err(JobRepositoryError::QueueInvariant),
        }
    }
}

/// Sanitized, stable categories of worker failure persisted in queue history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobFailureCode {
    HandlerFailed,
    HandlerPanicked,
    LeaseExpired,
}

impl JobFailureCode {
    const fn as_sql(self) -> &'static str {
        match self {
            Self::HandlerFailed => "HANDLER_FAILED",
            Self::HandlerPanicked => "HANDLER_PANICKED",
            Self::LeaseExpired => "LEASE_EXPIRED",
        }
    }

    fn parse(value: &str) -> Result<Self, JobRepositoryError> {
        match value {
            "HANDLER_FAILED" => Ok(Self::HandlerFailed),
            "HANDLER_PANICKED" => Ok(Self::HandlerPanicked),
            "LEASE_EXPIRED" => Ok(Self::LeaseExpired),
            _ => Err(JobRepositoryError::QueueInvariant),
        }
    }
}

/// A validated, opaque future-worker category such as `CRAWL_PAGE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobKind(String);

impl JobKind {
    /// Creates a stable job kind without accepting arbitrary content.
    ///
    /// # Errors
    /// Returns an input error unless the name is upper-case ASCII, digits, or
    /// underscores and is at most 64 bytes.
    pub fn new(value: impl Into<String>) -> Result<Self, JobRepositoryError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(JobRepositoryError::InvalidJobKind);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A durable job creation request. `max_attempts` counts total executions,
/// including the first execution; it is never a number of retries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewJob {
    pub id: JobId,
    pub kind: JobKind,
    pub priority: i32,
    pub parent_job_id: Option<JobId>,
    pub crawl_run_id: Option<String>,
    pub scheduled_at: i64,
    pub max_attempts: u32,
}

impl NewJob {
    /// Constructs a queued job with bounded total execution attempts.
    ///
    /// # Errors
    /// A job requires at least one total execution attempt.
    pub fn new(
        kind: JobKind,
        priority: i32,
        scheduled_at: i64,
        max_attempts: u32,
    ) -> Result<Self, JobRepositoryError> {
        if max_attempts == 0 {
            return Err(JobRepositoryError::InvalidMaxAttempts);
        }
        Ok(Self {
            id: JobId::new(),
            kind,
            priority,
            parent_job_id: None,
            crawl_run_id: None,
            scheduled_at,
            max_attempts,
        })
    }
}

/// Current durable job state, including the active lease only while running.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobRecord {
    pub id: JobId,
    pub kind: JobKind,
    pub priority: i32,
    pub state: JobState,
    pub parent_job_id: Option<JobId>,
    pub crawl_run_id: Option<String>,
    pub scheduled_at: i64,
    pub current_attempt: u32,
    pub max_attempts: u32,
    /// Monotonically increases for each lease, even after a lease is released.
    pub lease_generation: u64,
    pub lease: Option<JobLease>,
    pub failure_code: Option<JobFailureCode>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Unforgeable-at-the-repository-boundary current ownership evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobLease {
    pub id: String,
    pub owner: String,
    pub generation: u64,
    pub acquired_at: i64,
    pub expires_at: i64,
    pub heartbeat_at: i64,
}

/// The immutable identity and terminal outcome of one execution attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobAttempt {
    pub id: String,
    pub job_id: JobId,
    pub attempt_number: u32,
    pub lease_id: String,
    pub lease_generation: u64,
    pub worker_id: String,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub outcome: AttemptOutcome,
    pub failure_code: Option<JobFailureCode>,
}

/// Outcome for one durable attempt; terminal attempts cannot be modified.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptOutcome {
    Running,
    Succeeded,
    Failed,
    LeaseExpired,
}

impl AttemptOutcome {
    const fn as_sql(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::LeaseExpired => "LEASE_EXPIRED",
        }
    }

    fn parse(value: &str) -> Result<Self, JobRepositoryError> {
        match value {
            "RUNNING" => Ok(Self::Running),
            "SUCCEEDED" => Ok(Self::Succeeded),
            "FAILED" => Ok(Self::Failed),
            "LEASE_EXPIRED" => Ok(Self::LeaseExpired),
            _ => Err(JobRepositoryError::QueueInvariant),
        }
    }
}

/// A job atomically leased to a worker together with its current attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcquiredJob {
    pub job: JobRecord,
    pub attempt: JobAttempt,
}

/// Result of stale-job recovery; recovery never deletes or rewrites history.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StaleJobRecovery {
    pub requeued: u32,
    pub failed: u32,
}

/// A rebuilt in-memory scheduling view derived only from durable queue rows.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConcurrencyState {
    pub running_jobs: Vec<JobId>,
}

/// Typed queue failures without raw SQL, panic payloads, or job content.
#[derive(Debug, thiserror::Error)]
pub enum JobRepositoryError {
    #[error("the durable queue database operation failed")]
    Database(#[source] DbError),
    #[error("job kinds must be stable upper-case identifiers")]
    InvalidJobKind,
    #[error("max_attempts counts total attempts and must be at least one")]
    InvalidMaxAttempts,
    #[error("the job does not exist")]
    NotFound,
    #[error("the job lifecycle transition is not legal")]
    IllegalTransition,
    #[error("the current worker no longer owns this lease")]
    LeaseLost,
    #[error("a critical durable queue invariant is inconsistent")]
    QueueInvariant,
}

impl JobRepositoryError {
    fn database(error: turso::Error) -> Self {
        Self::Database(DbError::from(error))
    }
}

/// Repository that is the sole normal mutation boundary for job lifecycle,
/// attempt history, leases, and stale recovery.
#[derive(Clone, Copy, Debug)]
pub struct JobRepository<'database> {
    database: &'database ErabiDatabase,
}

impl<'database> JobRepository<'database> {
    #[must_use]
    pub const fn new(database: &'database ErabiDatabase) -> Self {
        Self { database }
    }

    /// Creates a job in its only valid initial state: `QUEUED`.
    ///
    /// # Errors
    /// Returns an error when the durable insert cannot be completed.
    pub async fn enqueue(&self, job: &NewJob, now: i64) -> Result<(), JobRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(JobRepositoryError::from_db)?;
        connection
            .execute(
                "INSERT INTO jobs (id, kind, priority, state, parent_job_id, crawl_run_id, scheduled_at, current_attempt, max_attempts, lease_id, lease_owner, lease_generation, lease_acquired_at, lease_expires_at, heartbeat_at, failure_code, created_at, updated_at) VALUES (?1, ?2, ?3, 'QUEUED', ?4, ?5, ?6, 0, ?7, NULL, NULL, 0, NULL, NULL, NULL, NULL, ?8, ?8)",
                (
                    job.id.as_str(),
                    job.kind.as_str(),
                    job.priority,
                    job.parent_job_id.as_ref().map(JobId::as_str),
                    job.crawl_run_id.as_deref(),
                    job.scheduled_at,
                    i64::from(job.max_attempts),
                    now,
                ),
            )
            .await
            .map_err(JobRepositoryError::database)?;
        Ok(())
    }

    /// Acquires one eligible job and creates its attempt in the same immediate
    /// transaction. Expired leases are finalized first, so stale owners never
    /// retain authority through an ABA-style re-lease.
    ///
    /// # Errors
    /// Returns an error for invalid worker/lease input, an inconsistent queue,
    /// or a failed durable transaction.
    pub async fn acquire_next(
        &self,
        worker_id: &str,
        now: i64,
        lease_duration_seconds: i64,
    ) -> Result<Option<AcquiredJob>, JobRepositoryError> {
        if !valid_worker_id(worker_id) || lease_duration_seconds <= 0 {
            return Err(JobRepositoryError::QueueInvariant);
        }
        let mut connection = self
            .database
            .connection()
            .await
            .map_err(JobRepositoryError::from_db)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(JobRepositoryError::database)?;
        let result = async {
            ensure_queue_invariants(&transaction).await?;
            recover_expired_in_transaction(&transaction, now).await?;
            let Some(id) = select_eligible_job(&transaction, now).await? else {
                return Ok(None);
            };
            let acquired =
                lease_queued_job(&transaction, &id, worker_id, now, lease_duration_seconds).await?;
            Ok(Some(acquired))
        }
        .await;
        match result {
            Ok(value) => transaction
                .commit()
                .await
                .map(|()| value)
                .map_err(JobRepositoryError::database),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Renews a lease only when every durable ownership field still matches and
    /// the lease has not already expired.
    ///
    /// # Errors
    /// Returns an error for an invalid duration, missing job, stale lease, or
    /// failed durable update.
    pub async fn heartbeat(
        &self,
        job_id: &JobId,
        lease: &JobLease,
        now: i64,
        lease_duration_seconds: i64,
    ) -> Result<JobLease, JobRepositoryError> {
        if lease_duration_seconds <= 0 {
            return Err(JobRepositoryError::QueueInvariant);
        }
        let expires_at = now
            .checked_add(lease_duration_seconds)
            .ok_or(JobRepositoryError::QueueInvariant)?;
        let connection = self
            .database
            .connection()
            .await
            .map_err(JobRepositoryError::from_db)?;
        let changed = connection
            .execute(
                "UPDATE jobs SET heartbeat_at = ?1, lease_expires_at = ?2, updated_at = ?1 WHERE id = ?3 AND state = 'RUNNING' AND lease_id = ?4 AND lease_owner = ?5 AND lease_generation = ?6 AND lease_expires_at = ?7 AND heartbeat_at = ?8 AND lease_expires_at > ?1",
                (
                    now,
                    expires_at,
                    job_id.as_str(),
                    lease.id.as_str(),
                    lease.owner.as_str(),
                    i64::try_from(lease.generation).map_err(|_| JobRepositoryError::QueueInvariant)?,
                    lease.expires_at,
                    lease.heartbeat_at,
                ),
            )
            .await
            .map_err(JobRepositoryError::database)?;
        if changed != 1 {
            return Err(self.ownership_error(job_id).await);
        }
        Ok(JobLease {
            heartbeat_at: now,
            expires_at,
            ..lease.clone()
        })
    }

    /// Completes the active attempt, releasing its lease, only for its current
    /// durable owner. The attempt becomes immutable in the same transaction.
    ///
    /// # Errors
    /// Returns an error when the job is absent, no longer owned by `lease`, or
    /// its durable lifecycle transition cannot be committed.
    pub async fn succeed(
        &self,
        job_id: &JobId,
        lease: &JobLease,
        now: i64,
    ) -> Result<(), JobRepositoryError> {
        self.finish(job_id, lease, now, AttemptOutcome::Succeeded, None)
            .await
    }

    /// Fails the active attempt. If attempts remain, it requeues the job at
    /// `retry_at`; otherwise it transitions it permanently to `FAILED`.
    ///
    /// # Errors
    /// Returns an error when the job is absent, no longer owned by `lease`, or
    /// its durable attempt/lifecycle update cannot be committed.
    pub async fn fail(
        &self,
        job_id: &JobId,
        lease: &JobLease,
        now: i64,
        failure: JobFailureCode,
        retry_at: i64,
    ) -> Result<JobState, JobRepositoryError> {
        let mut connection = self
            .database
            .connection()
            .await
            .map_err(JobRepositoryError::from_db)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(JobRepositoryError::database)?;
        let result = async {
            let job = select_owned_running_job(&transaction, job_id, lease, now).await?;
            let next_state = if job.current_attempt < job.max_attempts {
                JobState::Queued
            } else {
                JobState::Failed
            };
            finish_attempt_in_transaction(&transaction, job_id, lease, now, AttemptOutcome::Failed, Some(failure)).await?;
            let changed = transaction
                .execute(
                    "UPDATE jobs SET state = ?1, scheduled_at = ?2, lease_id = NULL, lease_owner = NULL, lease_acquired_at = NULL, lease_expires_at = NULL, heartbeat_at = NULL, failure_code = ?3, updated_at = ?4 WHERE id = ?5 AND state = 'RUNNING' AND lease_id = ?6 AND lease_owner = ?7 AND lease_generation = ?8",
                    (
                        next_state.as_sql(),
                        if next_state == JobState::Queued { retry_at } else { job.scheduled_at },
                        failure.as_sql(),
                        now,
                        job_id.as_str(),
                        lease.id.as_str(),
                        lease.owner.as_str(),
                        i64::try_from(lease.generation).map_err(|_| JobRepositoryError::QueueInvariant)?,
                    ),
                )
                .await
                .map_err(JobRepositoryError::database)?;
            if changed != 1 {
                return Err(JobRepositoryError::LeaseLost);
            }
            Ok(next_state)
        }
        .await;
        match result {
            Ok(state) => transaction
                .commit()
                .await
                .map(|()| state)
                .map_err(JobRepositoryError::database),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Inspects every expired running lease and only requeues work when bounded
    /// total attempts allow another execution. It never repairs corruption.
    ///
    /// # Errors
    /// Returns an error instead of modifying queue evidence when a lease or
    /// attempt invariant is inconsistent, or when recovery cannot commit.
    pub async fn recover_stale_jobs(
        &self,
        now: i64,
    ) -> Result<StaleJobRecovery, JobRepositoryError> {
        let mut connection = self
            .database
            .connection()
            .await
            .map_err(JobRepositoryError::from_db)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(JobRepositoryError::database)?;
        let result = async {
            ensure_queue_invariants(&transaction).await?;
            recover_expired_in_transaction(&transaction, now).await
        }
        .await;
        match result {
            Ok(recovery) => transaction
                .commit()
                .await
                .map(|()| recovery)
                .map_err(JobRepositoryError::database),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Reconstructs scheduler occupancy from authoritative current leases.
    ///
    /// # Errors
    /// Returns an error if durable queue invariants are inconsistent or the
    /// current leased-job view cannot be read.
    pub async fn rebuild_concurrency_state(
        &self,
        now: i64,
    ) -> Result<ConcurrencyState, JobRepositoryError> {
        self.check_invariants().await?;
        let connection = self
            .database
            .connection()
            .await
            .map_err(JobRepositoryError::from_db)?;
        let mut rows = connection
            .query(
                "SELECT id FROM jobs WHERE state = 'RUNNING' AND lease_expires_at > ?1 ORDER BY id",
                [now],
            )
            .await
            .map_err(JobRepositoryError::database)?;
        let mut running_jobs = Vec::new();
        while let Some(row) = rows.next().await.map_err(JobRepositoryError::database)? {
            running_jobs.push(JobId(row.get(0).map_err(JobRepositoryError::database)?));
        }
        Ok(ConcurrencyState { running_jobs })
    }

    /// Reads one job without exposing a raw mutable database handle.
    ///
    /// # Errors
    /// Returns `NotFound` when no job has this identity, or an error when its
    /// persisted state cannot be read safely.
    pub async fn job(&self, job_id: &JobId) -> Result<JobRecord, JobRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(JobRepositoryError::from_db)?;
        select_job(&connection, job_id).await
    }

    /// Returns durable attempt history in execution order.
    ///
    /// # Errors
    /// Returns an error when attempt history cannot be read or is invalid.
    pub async fn attempts(&self, job_id: &JobId) -> Result<Vec<JobAttempt>, JobRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(JobRepositoryError::from_db)?;
        let mut rows = connection
            .query(
                "SELECT id, job_id, attempt_number, lease_id, lease_generation, worker_id, started_at, finished_at, outcome, failure_code FROM job_attempts WHERE job_id = ?1 ORDER BY attempt_number",
                [job_id.as_str()],
            )
            .await
            .map_err(JobRepositoryError::database)?;
        let mut attempts = Vec::new();
        while let Some(row) = rows.next().await.map_err(JobRepositoryError::database)? {
            attempts.push(attempt_from_row(&row)?);
        }
        Ok(attempts)
    }

    /// Validates queue ownership/history relationships without altering data.
    ///
    /// # Errors
    /// Returns `QueueInvariant` when durable ownership/history is inconsistent,
    /// or an error when the database cannot be inspected.
    pub async fn check_invariants(&self) -> Result<(), JobRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(JobRepositoryError::from_db)?;
        ensure_queue_invariants(&connection).await
    }

    async fn ownership_error(&self, job_id: &JobId) -> JobRepositoryError {
        match self.job(job_id).await {
            Ok(job) if job.state != JobState::Running => JobRepositoryError::IllegalTransition,
            Err(JobRepositoryError::NotFound) => JobRepositoryError::NotFound,
            Ok(_) | Err(_) => JobRepositoryError::LeaseLost,
        }
    }

    async fn finish(
        &self,
        job_id: &JobId,
        lease: &JobLease,
        now: i64,
        outcome: AttemptOutcome,
        failure: Option<JobFailureCode>,
    ) -> Result<(), JobRepositoryError> {
        let mut connection = self
            .database
            .connection()
            .await
            .map_err(JobRepositoryError::from_db)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(JobRepositoryError::database)?;
        let result = async {
            select_owned_running_job(&transaction, job_id, lease, now).await?;
            finish_attempt_in_transaction(&transaction, job_id, lease, now, outcome, failure).await?;
            let changed = transaction
                .execute(
                    "UPDATE jobs SET state = 'SUCCEEDED', lease_id = NULL, lease_owner = NULL, lease_acquired_at = NULL, lease_expires_at = NULL, heartbeat_at = NULL, failure_code = NULL, updated_at = ?1 WHERE id = ?2 AND state = 'RUNNING' AND lease_id = ?3 AND lease_owner = ?4 AND lease_generation = ?5",
                    (
                        now,
                        job_id.as_str(),
                        lease.id.as_str(),
                        lease.owner.as_str(),
                        i64::try_from(lease.generation).map_err(|_| JobRepositoryError::QueueInvariant)?,
                    ),
                )
                .await
                .map_err(JobRepositoryError::database)?;
            if changed != 1 {
                return Err(JobRepositoryError::LeaseLost);
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => transaction
                .commit()
                .await
                .map_err(JobRepositoryError::database),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }
}

impl JobRepositoryError {
    fn from_db(error: DbError) -> Self {
        Self::Database(error)
    }
}

async fn select_eligible_job(
    connection: &Connection,
    now: i64,
) -> Result<Option<JobId>, JobRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT id FROM jobs WHERE state = 'QUEUED' AND scheduled_at <= ?1 ORDER BY priority DESC, scheduled_at, created_at, id LIMIT 1",
            [now],
        )
        .await
        .map_err(JobRepositoryError::database)?;
    rows.next()
        .await
        .map_err(JobRepositoryError::database)?
        .map(|row| row.get(0).map(JobId).map_err(JobRepositoryError::database))
        .transpose()
}

async fn lease_queued_job(
    connection: &Connection,
    job_id: &JobId,
    worker_id: &str,
    now: i64,
    lease_duration_seconds: i64,
) -> Result<AcquiredJob, JobRepositoryError> {
    let job = select_job(connection, job_id).await?;
    if job.state != JobState::Queued
        || job.scheduled_at > now
        || job.current_attempt >= job.max_attempts
    {
        return Err(JobRepositoryError::IllegalTransition);
    }
    let lease_id = Uuid::now_v7().to_string();
    let lease_generation = job.lease_generation.saturating_add(1);
    let expires_at = now
        .checked_add(lease_duration_seconds)
        .ok_or(JobRepositoryError::QueueInvariant)?;
    let attempt_number = job.current_attempt.saturating_add(1);
    let changed = connection
        .execute(
            "UPDATE jobs SET state = 'RUNNING', current_attempt = ?1, lease_id = ?2, lease_owner = ?3, lease_generation = ?4, lease_acquired_at = ?5, lease_expires_at = ?6, heartbeat_at = ?5, failure_code = NULL, updated_at = ?5 WHERE id = ?7 AND state = 'QUEUED' AND scheduled_at <= ?5 AND current_attempt = ?8",
            (
                i64::from(attempt_number),
                lease_id.as_str(),
                worker_id,
                i64::try_from(lease_generation).map_err(|_| JobRepositoryError::QueueInvariant)?,
                now,
                expires_at,
                job_id.as_str(),
                i64::from(job.current_attempt),
            ),
        )
        .await
        .map_err(JobRepositoryError::database)?;
    if changed != 1 {
        return Err(JobRepositoryError::IllegalTransition);
    }
    let attempt = JobAttempt {
        id: Uuid::now_v7().to_string(),
        job_id: job_id.clone(),
        attempt_number,
        lease_id: lease_id.clone(),
        lease_generation,
        worker_id: worker_id.to_owned(),
        started_at: now,
        finished_at: None,
        outcome: AttemptOutcome::Running,
        failure_code: None,
    };
    connection
        .execute(
            "INSERT INTO job_attempts (id, job_id, attempt_number, lease_id, lease_generation, worker_id, started_at, finished_at, outcome, failure_code) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, 'RUNNING', NULL)",
            (
                attempt.id.as_str(),
                job_id.as_str(),
                i64::from(attempt_number),
                lease_id.as_str(),
                i64::try_from(lease_generation).map_err(|_| JobRepositoryError::QueueInvariant)?,
                worker_id,
                now,
            ),
        )
        .await
        .map_err(JobRepositoryError::database)?;
    let job = select_job(connection, job_id).await?;
    Ok(AcquiredJob { job, attempt })
}

async fn select_owned_running_job(
    connection: &Connection,
    job_id: &JobId,
    lease: &JobLease,
    now: i64,
) -> Result<JobRecord, JobRepositoryError> {
    let job = select_job(connection, job_id).await?;
    if job.state != JobState::Running {
        return Err(JobRepositoryError::IllegalTransition);
    }
    if job.lease.as_ref() != Some(lease) || lease.expires_at <= now {
        return Err(JobRepositoryError::LeaseLost);
    }
    Ok(job)
}

async fn finish_attempt_in_transaction(
    connection: &Connection,
    job_id: &JobId,
    lease: &JobLease,
    now: i64,
    outcome: AttemptOutcome,
    failure: Option<JobFailureCode>,
) -> Result<(), JobRepositoryError> {
    let changed = connection
        .execute(
            "UPDATE job_attempts SET finished_at = ?1, outcome = ?2, failure_code = ?3 WHERE job_id = ?4 AND lease_id = ?5 AND lease_generation = ?6 AND worker_id = ?7 AND outcome = 'RUNNING'",
            (
                now,
                outcome.as_sql(),
                failure.map(JobFailureCode::as_sql),
                job_id.as_str(),
                lease.id.as_str(),
                i64::try_from(lease.generation).map_err(|_| JobRepositoryError::QueueInvariant)?,
                lease.owner.as_str(),
            ),
        )
        .await
        .map_err(JobRepositoryError::database)?;
    if changed != 1 {
        return Err(JobRepositoryError::LeaseLost);
    }
    Ok(())
}

async fn recover_expired_in_transaction(
    connection: &Connection,
    now: i64,
) -> Result<StaleJobRecovery, JobRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT id FROM jobs WHERE state = 'RUNNING' AND lease_expires_at <= ?1 ORDER BY id",
            [now],
        )
        .await
        .map_err(JobRepositoryError::database)?;
    let mut ids = Vec::new();
    while let Some(row) = rows.next().await.map_err(JobRepositoryError::database)? {
        ids.push(JobId(row.get(0).map_err(JobRepositoryError::database)?));
    }
    drop(rows);

    let mut recovery = StaleJobRecovery::default();
    for id in ids {
        let job = select_job(connection, &id).await?;
        let lease = job
            .lease
            .clone()
            .ok_or(JobRepositoryError::QueueInvariant)?;
        if lease.expires_at > now {
            continue;
        }
        finish_attempt_in_transaction(
            connection,
            &id,
            &lease,
            now,
            AttemptOutcome::LeaseExpired,
            Some(JobFailureCode::LeaseExpired),
        )
        .await?;
        let next_state = if job.current_attempt < job.max_attempts {
            recovery.requeued = recovery.requeued.saturating_add(1);
            JobState::Queued
        } else {
            recovery.failed = recovery.failed.saturating_add(1);
            JobState::Failed
        };
        let changed = connection
            .execute(
                "UPDATE jobs SET state = ?1, scheduled_at = ?2, lease_id = NULL, lease_owner = NULL, lease_acquired_at = NULL, lease_expires_at = NULL, heartbeat_at = NULL, failure_code = 'LEASE_EXPIRED', updated_at = ?2 WHERE id = ?3 AND state = 'RUNNING' AND lease_id = ?4 AND lease_owner = ?5 AND lease_generation = ?6 AND lease_expires_at <= ?2",
                (
                    next_state.as_sql(),
                    now,
                    id.as_str(),
                    lease.id.as_str(),
                    lease.owner.as_str(),
                    i64::try_from(lease.generation).map_err(|_| JobRepositoryError::QueueInvariant)?,
                ),
            )
            .await
            .map_err(JobRepositoryError::database)?;
        if changed != 1 {
            return Err(JobRepositoryError::LeaseLost);
        }
    }
    Ok(recovery)
}

async fn select_job(
    connection: &Connection,
    job_id: &JobId,
) -> Result<JobRecord, JobRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT id, kind, priority, state, parent_job_id, crawl_run_id, scheduled_at, current_attempt, max_attempts, lease_id, lease_owner, lease_generation, lease_acquired_at, lease_expires_at, heartbeat_at, failure_code, created_at, updated_at FROM jobs WHERE id = ?1",
            [job_id.as_str()],
        )
        .await
        .map_err(JobRepositoryError::database)?;
    let row = rows
        .next()
        .await
        .map_err(JobRepositoryError::database)?
        .ok_or(JobRepositoryError::NotFound)?;
    job_from_row(&row)
}

fn job_from_row(row: &turso::Row) -> Result<JobRecord, JobRepositoryError> {
    let state = JobState::parse(&row.get::<String>(3).map_err(JobRepositoryError::database)?)?;
    let lease_id: Option<String> = row.get(9).map_err(JobRepositoryError::database)?;
    let lease = match lease_id {
        Some(id) => Some(JobLease {
            id,
            owner: row.get(10).map_err(JobRepositoryError::database)?,
            generation: positive_u64(row.get::<i64>(11).map_err(JobRepositoryError::database)?)?,
            acquired_at: row.get(12).map_err(JobRepositoryError::database)?,
            expires_at: row.get(13).map_err(JobRepositoryError::database)?,
            heartbeat_at: row.get(14).map_err(JobRepositoryError::database)?,
        }),
        None => None,
    };
    let failure: Option<String> = row.get(15).map_err(JobRepositoryError::database)?;
    Ok(JobRecord {
        id: JobId(row.get(0).map_err(JobRepositoryError::database)?),
        kind: JobKind::new(row.get::<String>(1).map_err(JobRepositoryError::database)?)?,
        priority: row.get(2).map_err(JobRepositoryError::database)?,
        state,
        parent_job_id: row
            .get::<Option<String>>(4)
            .map_err(JobRepositoryError::database)?
            .map(JobId),
        crawl_run_id: row.get(5).map_err(JobRepositoryError::database)?,
        scheduled_at: row.get(6).map_err(JobRepositoryError::database)?,
        current_attempt: positive_u32_or_zero(
            row.get::<i64>(7).map_err(JobRepositoryError::database)?,
        )?,
        max_attempts: positive_u32(row.get::<i64>(8).map_err(JobRepositoryError::database)?)?,
        lease_generation: nonnegative_u64(
            row.get::<i64>(11).map_err(JobRepositoryError::database)?,
        )?,
        lease,
        failure_code: failure
            .map(|value| JobFailureCode::parse(&value))
            .transpose()?,
        created_at: row.get(16).map_err(JobRepositoryError::database)?,
        updated_at: row.get(17).map_err(JobRepositoryError::database)?,
    })
}

fn attempt_from_row(row: &turso::Row) -> Result<JobAttempt, JobRepositoryError> {
    let failure: Option<String> = row.get(9).map_err(JobRepositoryError::database)?;
    Ok(JobAttempt {
        id: row.get(0).map_err(JobRepositoryError::database)?,
        job_id: JobId(row.get(1).map_err(JobRepositoryError::database)?),
        attempt_number: positive_u32(row.get::<i64>(2).map_err(JobRepositoryError::database)?)?,
        lease_id: row.get(3).map_err(JobRepositoryError::database)?,
        lease_generation: positive_u64(row.get::<i64>(4).map_err(JobRepositoryError::database)?)?,
        worker_id: row.get(5).map_err(JobRepositoryError::database)?,
        started_at: row.get(6).map_err(JobRepositoryError::database)?,
        finished_at: row.get(7).map_err(JobRepositoryError::database)?,
        outcome: AttemptOutcome::parse(
            &row.get::<String>(8).map_err(JobRepositoryError::database)?,
        )?,
        failure_code: failure
            .map(|value| JobFailureCode::parse(&value))
            .transpose()?,
    })
}

async fn ensure_queue_invariants(connection: &Connection) -> Result<(), JobRepositoryError> {
    const INCONSISTENCIES: [&str; 5] = [
        "SELECT 1 FROM jobs AS job WHERE (job.state = 'RUNNING' AND (job.current_attempt = 0 OR job.lease_id IS NULL OR job.lease_owner IS NULL OR job.lease_generation = 0 OR job.lease_acquired_at IS NULL OR job.lease_expires_at IS NULL OR job.heartbeat_at IS NULL)) OR (job.state <> 'RUNNING' AND (job.lease_id IS NOT NULL OR job.lease_owner IS NOT NULL OR job.lease_acquired_at IS NOT NULL OR job.lease_expires_at IS NOT NULL OR job.heartbeat_at IS NOT NULL)) LIMIT 1",
        "SELECT 1 FROM jobs AS job LEFT JOIN job_attempts AS attempt ON attempt.job_id = job.id AND attempt.attempt_number = job.current_attempt AND attempt.outcome = 'RUNNING' WHERE job.state = 'RUNNING' AND (attempt.id IS NULL OR attempt.lease_id <> job.lease_id OR attempt.lease_generation <> job.lease_generation OR attempt.worker_id <> job.lease_owner) LIMIT 1",
        "SELECT 1 FROM jobs AS job JOIN job_attempts AS attempt ON attempt.job_id = job.id WHERE job.state <> 'RUNNING' AND attempt.outcome = 'RUNNING' LIMIT 1",
        "SELECT 1 FROM jobs AS job WHERE job.current_attempt <> COALESCE((SELECT MAX(attempt.attempt_number) FROM job_attempts AS attempt WHERE attempt.job_id = job.id), 0) LIMIT 1",
        "SELECT 1 FROM job_attempts AS attempt LEFT JOIN jobs AS job ON job.id = attempt.job_id WHERE job.id IS NULL OR attempt.attempt_number > job.max_attempts LIMIT 1",
    ];
    for query in INCONSISTENCIES {
        let mut rows = connection
            .query(query, ())
            .await
            .map_err(JobRepositoryError::database)?;
        if rows
            .next()
            .await
            .map_err(JobRepositoryError::database)?
            .is_some()
        {
            return Err(JobRepositoryError::QueueInvariant);
        }
    }
    Ok(())
}

fn positive_u32(value: i64) -> Result<u32, JobRepositoryError> {
    u32::try_from(value)
        .map_err(|_| JobRepositoryError::QueueInvariant)
        .and_then(|value| {
            if value == 0 {
                Err(JobRepositoryError::QueueInvariant)
            } else {
                Ok(value)
            }
        })
}

fn positive_u32_or_zero(value: i64) -> Result<u32, JobRepositoryError> {
    u32::try_from(value).map_err(|_| JobRepositoryError::QueueInvariant)
}

fn positive_u64(value: i64) -> Result<u64, JobRepositoryError> {
    u64::try_from(value)
        .map_err(|_| JobRepositoryError::QueueInvariant)
        .and_then(|value| {
            if value == 0 {
                Err(JobRepositoryError::QueueInvariant)
            } else {
                Ok(value)
            }
        })
}

fn nonnegative_u64(value: i64) -> Result<u64, JobRepositoryError> {
    u64::try_from(value).map_err(|_| JobRepositoryError::QueueInvariant)
}

fn valid_worker_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}
