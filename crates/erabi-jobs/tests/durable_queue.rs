use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use erabi_db::{
    ErabiDatabase, MigrationRunner,
    repositories::{
        AttemptOutcome, JobFailureCode, JobKind, JobRepository, JobRepositoryError, JobState,
        NewJob,
    },
};
use erabi_jobs::{
    CheckpointEnvelope, CheckpointIdentity, CheckpointUnitId, JobExecutionContext,
    JobExecutionError, JobHandler, JobRuntime, JobRuntimeError, WorkerPolicy, WorkerTurn,
    recover_and_rebuild_at,
};
use tokio::sync::Notify;

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

fn new_job(max_attempts: u32, scheduled_at: i64) -> Result<NewJob, JobRepositoryError> {
    NewJob::new(JobKind::new("TEST_WORK")?, 10, scheduled_at, max_attempts)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_workers_race_for_one_job_and_exactly_one_wins()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(2, 100)?;
    repository.enqueue(&job, 1).await?;

    let left = repository.acquire_next("worker-left", 100, 10);
    let right = repository.acquire_next("worker-right", 100, 10);
    let (left, right) = tokio::join!(left, right);
    let winners = [left?, right?].into_iter().flatten().count();
    assert_eq!(winners, 1);
    assert_eq!(repository.attempts(&job.id).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn heartbeats_require_current_owner_and_current_lease_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(2, 10)?;
    repository.enqueue(&job, 1).await?;
    let acquired = repository
        .acquire_next("worker-a", 10, 20)
        .await?
        .ok_or("job was not acquired")?;
    let lease = acquired.job.lease.ok_or("missing lease")?;

    let renewed = repository.heartbeat(&job.id, &lease, 15, 20).await?;
    assert_eq!(renewed.heartbeat_at, 15);
    assert_eq!(renewed.expires_at, 35);
    assert!(matches!(
        repository.heartbeat(&job.id, &lease, 16, 20).await,
        Err(JobRepositoryError::LeaseLost)
    ));
    let mut non_owner = renewed.clone();
    non_owner.owner = "worker-b".to_owned();
    assert!(matches!(
        repository.heartbeat(&job.id, &non_owner, 16, 20).await,
        Err(JobRepositoryError::LeaseLost)
    ));
    assert!(matches!(
        repository.succeed(&job.id, &non_owner, 16).await,
        Err(JobRepositoryError::LeaseLost)
    ));
    repository.succeed(&job.id, &renewed, 16).await?;
    Ok(())
}

#[tokio::test]
async fn expired_leases_revoke_stale_owner_authority_without_aba_reuse()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(2, 0)?;
    repository.enqueue(&job, 0).await?;
    let first = repository
        .acquire_next("worker-a", 0, 10)
        .await?
        .ok_or("first lease missing")?;
    let first_lease = first.job.lease.ok_or("missing first lease")?;

    let second = repository
        .acquire_next("worker-b", 10, 10)
        .await?
        .ok_or("expired job was not recovered and leased")?;
    let second_lease = second.job.lease.ok_or("missing second lease")?;
    assert_ne!(first_lease.id, second_lease.id);
    assert!(second_lease.generation > first_lease.generation);
    assert!(matches!(
        repository.heartbeat(&job.id, &first_lease, 10, 10).await,
        Err(JobRepositoryError::LeaseLost)
    ));
    assert!(matches!(
        repository.succeed(&job.id, &first_lease, 10).await,
        Err(JobRepositoryError::LeaseLost)
    ));
    let attempts = repository.attempts(&job.id).await?;
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].outcome, AttemptOutcome::LeaseExpired);
    assert_eq!(attempts[0].failure_code, Some(JobFailureCode::LeaseExpired));
    Ok(())
}

#[tokio::test]
async fn stale_lease_owner_cannot_commit_final_cancellation_state()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(2, 0)?;
    repository.enqueue(&job, 0).await?;
    let first = repository
        .acquire_next("worker-stale-a", 0, 5)
        .await?
        .ok_or("first lease missing")?;
    let first_lease = first.job.lease.ok_or("first lease missing")?;
    let second = repository
        .acquire_next("worker-stale-b", 5, 5)
        .await?
        .ok_or("second lease missing")?;

    assert!(matches!(
        repository.cancel(&job.id, &first_lease, 5).await,
        Err(JobRepositoryError::LeaseLost)
    ));
    assert_eq!(repository.job(&job.id).await?.state, JobState::Running);
    repository
        .cancel(&job.id, &second.job.lease.ok_or("second lease missing")?, 5)
        .await?;
    assert_eq!(repository.job(&job.id).await?.state, JobState::Cancelled);
    Ok(())
}

#[tokio::test]
async fn max_attempts_counts_total_executions_and_preserves_prior_attempts()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(2, 0)?;
    repository.enqueue(&job, 0).await?;
    let first = repository
        .acquire_next("worker-a", 0, 30)
        .await?
        .ok_or("first lease missing")?;
    repository
        .fail(
            &job.id,
            &first.job.lease.ok_or("missing first lease")?,
            1,
            JobFailureCode::HandlerFailed,
            5,
        )
        .await?;
    let after_first_failure = repository.attempts(&job.id).await?;
    assert_eq!(after_first_failure.len(), 1);
    assert_eq!(after_first_failure[0].outcome, AttemptOutcome::Failed);

    let second = repository
        .acquire_next("worker-b", 5, 30)
        .await?
        .ok_or("second lease missing")?;
    assert_eq!(second.attempt.attempt_number, 2);
    assert_eq!(
        repository
            .fail(
                &job.id,
                &second.job.lease.ok_or("missing second lease")?,
                6,
                JobFailureCode::HandlerFailed,
                10,
            )
            .await?,
        JobState::Failed
    );
    assert!(repository.acquire_next("worker-c", 10, 30).await?.is_none());
    let attempts = repository.attempts(&job.id).await?;
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].outcome, AttemptOutcome::Failed);
    assert_eq!(attempts[1].outcome, AttemptOutcome::Failed);

    let one_attempt = new_job(1, 20)?;
    repository.enqueue(&one_attempt, 20).await?;
    let only = repository
        .acquire_next("worker-d", 20, 30)
        .await?
        .ok_or("one attempt lease missing")?;
    assert_eq!(
        repository
            .fail(
                &one_attempt.id,
                &only.job.lease.ok_or("missing lease")?,
                21,
                JobFailureCode::HandlerFailed,
                30,
            )
            .await?,
        JobState::Failed
    );
    assert_eq!(repository.attempts(&one_attempt.id).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn startup_recovery_requeues_safe_stale_work_and_fails_exhausted_work()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let retryable = new_job(2, 0)?;
    let exhausted = new_job(1, 0)?;
    repository.enqueue(&retryable, 0).await?;
    repository.enqueue(&exhausted, 0).await?;
    repository
        .acquire_next("worker-a", 0, 10)
        .await?
        .ok_or("retryable lease missing")?;
    repository
        .acquire_next("worker-b", 0, 10)
        .await?
        .ok_or("exhausted lease missing")?;

    let (recovery, concurrency) = recover_and_rebuild_at(&database, 10).await?;
    assert_eq!(recovery.requeued, 1);
    assert_eq!(recovery.failed, 1);
    assert!(concurrency.running_jobs.is_empty());
    assert_eq!(repository.job(&retryable.id).await?.state, JobState::Queued);
    assert_eq!(repository.job(&exhausted.id).await?.state, JobState::Failed);
    assert_eq!(
        repository.attempts(&retryable.id).await?[0].outcome,
        AttemptOutcome::LeaseExpired
    );
    Ok(())
}

#[tokio::test]
async fn durable_concurrency_state_rebuilds_only_unexpired_running_jobs()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(2, 0)?;
    repository.enqueue(&job, 0).await?;
    repository
        .acquire_next("worker-a", 0, 30)
        .await?
        .ok_or("lease missing")?;
    let (_, concurrency) = recover_and_rebuild_at(&database, 10).await?;
    assert_eq!(concurrency.running_jobs, vec![job.id]);
    Ok(())
}

struct PanicThenSucceed {
    calls: AtomicUsize,
}

impl JobHandler for PanicThenSucceed {
    fn execute(
        &self,
        _context: JobExecutionContext,
    ) -> impl Future<Output = Result<(), JobExecutionError>> + Send {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        async move {
            assert!(call != 0, "handler panic must be isolated");
            Ok(())
        }
    }
}

struct DelayedSuccess;

impl JobHandler for DelayedSuccess {
    async fn execute(&self, _context: JobExecutionContext) -> Result<(), JobExecutionError> {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        Ok(())
    }
}

struct BarrierThenSuccess {
    barrier: Arc<tokio::sync::Barrier>,
}

fn checkpoint(unit: &str) -> Result<CheckpointEnvelope, Box<dyn std::error::Error>> {
    let identity = CheckpointIdentity::new("generic-job", "a".repeat(64), "b".repeat(64))?;
    let mut checkpoint = CheckpointEnvelope::new(identity);
    checkpoint
        .completed_units
        .push(CheckpointUnitId::new(unit)?);
    Ok(checkpoint)
}

struct CheckpointThenWait {
    started: Arc<Notify>,
    checkpoint: CheckpointEnvelope,
    observed_cancellation: Arc<AtomicBool>,
}

impl JobHandler for CheckpointThenWait {
    fn execute(
        &self,
        context: JobExecutionContext,
    ) -> impl Future<Output = Result<(), JobExecutionError>> + Send {
        let started = Arc::clone(&self.started);
        let checkpoint = self.checkpoint.clone();
        let observed_cancellation = Arc::clone(&self.observed_cancellation);
        async move {
            context
                .checkpoint(&checkpoint)
                .await
                .map_err(|_| JobExecutionError)?;
            started.notify_one();
            context.cancellation().cancelled().await;
            observed_cancellation.store(true, Ordering::Release);
            Ok(())
        }
    }
}

struct InvalidCheckpointThenWait {
    started: Arc<Notify>,
    checkpoint: CheckpointEnvelope,
    checkpoint_failed: Arc<AtomicBool>,
}

struct CheckpointAfterSignal {
    started: Arc<Notify>,
    persist: Arc<Notify>,
    checkpoint: CheckpointEnvelope,
    persisted: Arc<AtomicBool>,
}

impl JobHandler for CheckpointAfterSignal {
    fn execute(
        &self,
        context: JobExecutionContext,
    ) -> impl Future<Output = Result<(), JobExecutionError>> + Send {
        let started = Arc::clone(&self.started);
        let persist = Arc::clone(&self.persist);
        let checkpoint = self.checkpoint.clone();
        let persisted = Arc::clone(&self.persisted);
        async move {
            started.notify_one();
            persist.notified().await;
            if context.checkpoint(&checkpoint).await.is_ok() {
                persisted.store(true, Ordering::Release);
            }
            Ok(())
        }
    }
}

struct TwoCheckpointsAtSignals {
    first_persisted: Arc<Notify>,
    persist_second: Arc<Notify>,
    first: CheckpointEnvelope,
    second: CheckpointEnvelope,
}

impl JobHandler for TwoCheckpointsAtSignals {
    fn execute(
        &self,
        context: JobExecutionContext,
    ) -> impl Future<Output = Result<(), JobExecutionError>> + Send {
        let first_persisted = Arc::clone(&self.first_persisted);
        let persist_second = Arc::clone(&self.persist_second);
        let first = self.first.clone();
        let second = self.second.clone();
        async move {
            context
                .checkpoint(&first)
                .await
                .map_err(|_| JobExecutionError)?;
            first_persisted.notify_one();
            persist_second.notified().await;
            context
                .checkpoint(&second)
                .await
                .map(|_| ())
                .map_err(|_| JobExecutionError)
        }
    }
}

struct WaitForLeaseLossCancellation {
    started: Arc<Notify>,
    observed_cancellation: Arc<AtomicBool>,
}

impl JobHandler for WaitForLeaseLossCancellation {
    fn execute(
        &self,
        context: JobExecutionContext,
    ) -> impl Future<Output = Result<(), JobExecutionError>> + Send {
        let started = Arc::clone(&self.started);
        let observed_cancellation = Arc::clone(&self.observed_cancellation);
        async move {
            started.notify_one();
            context.cancellation().cancelled().await;
            observed_cancellation.store(true, Ordering::Release);
            Ok(())
        }
    }
}

impl JobHandler for InvalidCheckpointThenWait {
    fn execute(
        &self,
        context: JobExecutionContext,
    ) -> impl Future<Output = Result<(), JobExecutionError>> + Send {
        let started = Arc::clone(&self.started);
        let mut checkpoint = self.checkpoint.clone();
        checkpoint
            .pending_units
            .clone_from(&checkpoint.completed_units);
        let checkpoint_failed = Arc::clone(&self.checkpoint_failed);
        async move {
            if context.checkpoint(&checkpoint).await.is_err() {
                checkpoint_failed.store(true, Ordering::Release);
            }
            started.notify_one();
            context.cancellation().cancelled().await;
            Err(JobExecutionError)
        }
    }
}

impl JobHandler for BarrierThenSuccess {
    fn execute(
        &self,
        _context: JobExecutionContext,
    ) -> impl Future<Output = Result<(), JobExecutionError>> + Send {
        let barrier = Arc::clone(&self.barrier);
        async move {
            barrier.wait().await;
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            Ok(())
        }
    }
}

#[tokio::test(start_paused = true)]
async fn runtime_heartbeats_long_handler_and_completes_with_the_renewed_lease()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(1, 0)?;
    repository.enqueue(&job, 0).await?;
    let runtime = JobRuntime::new(
        &database,
        "worker-runtime",
        WorkerPolicy {
            lease_duration_seconds: 3,
            retry_delay_seconds: 0,
        },
    )?;

    let handler = DelayedSuccess;
    let advance_time = async {
        for _ in 0..5 {
            tokio::task::yield_now().await;
            tokio::time::advance(std::time::Duration::from_secs(1)).await;
        }
    };
    let (turn, ()) = tokio::join!(runtime.execute_next_at(&handler, 0), advance_time);

    assert!(matches!(turn?, WorkerTurn::Succeeded { .. }));
    assert_eq!(repository.job(&job.id).await?.state, JobState::Succeeded);
    assert_eq!(repository.attempts(&job.id).await?[0].finished_at, Some(5));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn lost_heartbeat_ownership_never_commits_a_stale_handler_result()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(2, 0)?;
    repository.enqueue(&job, 0).await?;
    let runtime = JobRuntime::new(
        &database,
        "worker-runtime",
        WorkerPolicy {
            lease_duration_seconds: 3,
            retry_delay_seconds: 0,
        },
    )?;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let handler = BarrierThenSuccess {
        barrier: Arc::clone(&barrier),
    };

    let invalidate_lease = async {
        barrier.wait().await;
        assert_eq!(repository.recover_stale_jobs(3).await?.requeued, 1);
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(4)).await;
        tokio::task::yield_now().await;
        Ok::<(), JobRepositoryError>(())
    };
    let (turn, invalidation) = tokio::join!(runtime.execute_next_at(&handler, 0), invalidate_lease);

    invalidation?;
    assert!(matches!(
        turn,
        Err(JobRuntimeError::Repository(
            JobRepositoryError::LeaseLost | JobRepositoryError::IllegalTransition
        ))
    ));
    assert_eq!(repository.job(&job.id).await?.state, JobState::Queued);
    assert_eq!(
        repository.attempts(&job.id).await?[0].outcome,
        AttemptOutcome::LeaseExpired
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn expired_lease_rejects_checkpoint_without_handler_control_of_time()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(1, 0)?;
    repository.enqueue(&job, 0).await?;
    let runtime = JobRuntime::new(
        &database,
        "worker-checkpoint-time",
        WorkerPolicy {
            lease_duration_seconds: 30,
            retry_delay_seconds: 0,
        },
    )?;
    let started = Arc::new(Notify::new());
    let persist = Arc::new(Notify::new());
    let persisted = Arc::new(AtomicBool::new(false));
    let handler = CheckpointAfterSignal {
        started: Arc::clone(&started),
        persist: Arc::clone(&persist),
        checkpoint: checkpoint("expired")?,
        persisted: Arc::clone(&persisted),
    };
    let execution = runtime.execute_next_at(&handler, 0);
    tokio::pin!(execution);

    tokio::select! {
        () = started.notified() => {}
        result = &mut execution => return Err(format!("handler ended before lease expiry: {result:?}").into()),
    }
    let lease = repository
        .job(&job.id)
        .await?
        .lease
        .ok_or("lease missing")?;
    repository.heartbeat(&job.id, &lease, 0, 1).await?;
    tokio::time::advance(Duration::from_secs(1)).await;
    persist.notify_one();

    assert!(matches!(
        execution.await,
        Err(JobRuntimeError::Repository(JobRepositoryError::LeaseLost))
    ));
    assert!(!persisted.load(Ordering::Acquire));
    assert!(repository.checkpoints(&job.id).await?.is_empty());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn runtime_owned_checkpoint_time_preserves_history_and_latest_order()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(1, 0)?;
    repository.enqueue(&job, 0).await?;
    let runtime = JobRuntime::new(
        &database,
        "worker-checkpoint-order",
        WorkerPolicy {
            lease_duration_seconds: 30,
            retry_delay_seconds: 0,
        },
    )?;
    let first_persisted = Arc::new(Notify::new());
    let persist_second = Arc::new(Notify::new());
    let handler = TwoCheckpointsAtSignals {
        first_persisted: Arc::clone(&first_persisted),
        persist_second: Arc::clone(&persist_second),
        first: checkpoint("first")?,
        second: checkpoint("second")?,
    };
    let execution = runtime.execute_next_at(&handler, 0);
    tokio::pin!(execution);

    tokio::select! {
        () = first_persisted.notified() => {}
        result = &mut execution => return Err(format!("handler ended before second checkpoint: {result:?}").into()),
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    persist_second.notify_one();
    assert!(matches!(execution.await?, WorkerTurn::Succeeded { .. }));

    let records = repository.checkpoints(&job.id).await?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].created_at, 0);
    assert_eq!(records[1].created_at, 1);
    assert_eq!(records[0].checkpoint.completed_units[0].as_str(), "first");
    assert_eq!(records[1].checkpoint.completed_units[0].as_str(), "second");
    assert_eq!(
        repository
            .latest_checkpoint(&job.id)
            .await?
            .ok_or("latest checkpoint missing")?
            .checkpoint
            .completed_units[0]
            .as_str(),
        "second"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn lease_loss_signals_cooperative_handler_before_waiting_for_its_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(2, 0)?;
    repository.enqueue(&job, 0).await?;
    let runtime = JobRuntime::new(
        &database,
        "worker-lease-loss-cancel",
        WorkerPolicy {
            lease_duration_seconds: 3,
            retry_delay_seconds: 0,
        },
    )?;
    let started = Arc::new(Notify::new());
    let observed = Arc::new(AtomicBool::new(false));
    let handler = WaitForLeaseLossCancellation {
        started: Arc::clone(&started),
        observed_cancellation: Arc::clone(&observed),
    };
    let execution = runtime.execute_next_at(&handler, 0);
    tokio::pin!(execution);

    tokio::select! {
        () = started.notified() => {}
        result = &mut execution => return Err(format!("handler ended before lease loss: {result:?}").into()),
    }
    assert_eq!(repository.recover_stale_jobs(3).await?.requeued, 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;

    tokio::select! {
        biased;
        result = &mut execution => assert!(matches!(
            result,
            Err(JobRuntimeError::Repository(JobRepositoryError::LeaseLost | JobRepositoryError::IllegalTransition))
        )),
        () = tokio::time::advance(Duration::from_secs(1)) => panic!("lease loss did not signal the waiting handler"),
    }
    assert!(observed.load(Ordering::Acquire));
    assert_eq!(repository.job(&job.id).await?.state, JobState::Queued);
    Ok(())
}

#[tokio::test]
async fn panicking_handler_fails_only_its_job_and_next_job_still_runs()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let panicking = new_job(1, 0)?;
    let succeeding = new_job(1, 0)?;
    repository.enqueue(&panicking, 0).await?;
    repository.enqueue(&succeeding, 0).await?;
    let runtime = JobRuntime::new(
        &database,
        "worker-runtime",
        WorkerPolicy {
            lease_duration_seconds: 30,
            retry_delay_seconds: 0,
        },
    )?;
    let handler = PanicThenSucceed {
        calls: AtomicUsize::new(0),
    };
    assert!(matches!(
        runtime.execute_next_at(&handler, 0).await?,
        WorkerTurn::Failed {
            failure: JobFailureCode::HandlerPanicked,
            ..
        }
    ));
    assert!(matches!(
        runtime.execute_next_at(&handler, 0).await?,
        WorkerTurn::Succeeded { .. }
    ));
    assert_eq!(repository.job(&panicking.id).await?.state, JobState::Failed);
    assert_eq!(
        repository.job(&succeeding.id).await?.state,
        JobState::Succeeded
    );
    assert_eq!(
        repository.attempts(&panicking.id).await?[0].failure_code,
        Some(JobFailureCode::HandlerPanicked)
    );
    Ok(())
}

#[tokio::test]
async fn queued_cancellation_stops_scheduling_new_units() -> Result<(), Box<dyn std::error::Error>>
{
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(1, 0)?;
    repository.enqueue(&job, 0).await?;
    let runtime = JobRuntime::new(&database, "worker-cancel", WorkerPolicy::conservative())?;

    assert_eq!(
        runtime.request_cancellation(&job.id, 0).await?,
        JobState::Cancelled
    );
    assert!(matches!(
        runtime.execute_next_at(&DelayedSuccess, 0).await?,
        WorkerTurn::Idle
    ));
    assert_eq!(repository.job(&job.id).await?.state, JobState::Cancelled);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_handler_observes_cooperative_cancellation_and_finishes_cancelled()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(1, 0)?;
    repository.enqueue(&job, 0).await?;
    let runtime = JobRuntime::new(
        &database,
        "worker-cancel-active",
        WorkerPolicy::conservative(),
    )?;
    let started = Arc::new(Notify::new());
    let observed = Arc::new(AtomicBool::new(false));
    let handler = CheckpointThenWait {
        started: Arc::clone(&started),
        checkpoint: checkpoint("unit-1")?,
        observed_cancellation: Arc::clone(&observed),
    };
    let (execution, cancellation) = tokio::join!(runtime.execute_next_at(&handler, 0), async {
        started.notified().await;
        runtime.request_cancellation(&job.id, 0).await
    });
    assert_eq!(cancellation?, JobState::Running);
    assert!(matches!(
        execution?,
        WorkerTurn::Cancelled {
            checkpoint_persisted: true,
            ..
        }
    ));
    assert!(observed.load(Ordering::Acquire));
    assert_eq!(repository.job(&job.id).await?.state, JobState::Cancelled);
    assert_eq!(repository.checkpoints(&job.id).await?.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_persistence_failure_does_not_advertise_resumability()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(1, 0)?;
    repository.enqueue(&job, 0).await?;
    let runtime = JobRuntime::new(
        &database,
        "worker-cancel-invalid-checkpoint",
        WorkerPolicy::conservative(),
    )?;
    let started = Arc::new(Notify::new());
    let checkpoint_failed = Arc::new(AtomicBool::new(false));
    let handler = InvalidCheckpointThenWait {
        started: Arc::clone(&started),
        checkpoint: checkpoint("unit-invalid")?,
        checkpoint_failed: Arc::clone(&checkpoint_failed),
    };
    let (execution, cancellation) = tokio::join!(runtime.execute_next_at(&handler, 0), async {
        started.notified().await;
        runtime.request_cancellation(&job.id, 0).await
    });
    assert_eq!(cancellation?, JobState::Running);
    assert!(matches!(
        execution?,
        WorkerTurn::Cancelled {
            checkpoint_persisted: false,
            ..
        }
    ));
    assert!(checkpoint_failed.load(Ordering::Acquire));
    assert!(repository.checkpoints(&job.id).await?.is_empty());
    assert_eq!(repository.job(&job.id).await?.state, JobState::Cancelled);
    Ok(())
}

#[tokio::test]
async fn cancelling_one_job_leaves_unrelated_queued_work_executable()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let cancelled = new_job(1, 0)?;
    let unrelated = new_job(1, 0)?;
    repository.enqueue(&cancelled, 0).await?;
    repository.enqueue(&unrelated, 0).await?;
    let runtime = JobRuntime::new(
        &database,
        "worker-independent",
        WorkerPolicy::conservative(),
    )?;
    runtime.request_cancellation(&cancelled.id, 0).await?;

    assert!(matches!(
        runtime.execute_next_at(&DelayedSuccess, 0).await?,
        WorkerTurn::Succeeded { job_id } if job_id == unrelated.id
    ));
    assert_eq!(
        repository.job(&cancelled.id).await?.state,
        JobState::Cancelled
    );
    assert_eq!(
        repository.job(&unrelated.id).await?.state,
        JobState::Succeeded
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_cancellation_controller_prevents_new_worker_turns()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let repository = JobRepository::new(&database);
    let job = new_job(1, 0)?;
    repository.enqueue(&job, 0).await?;
    let runtime = JobRuntime::new(&database, "worker-shutdown", WorkerPolicy::conservative())?;
    runtime.cancellation_controller().cancel_all();

    assert!(matches!(
        runtime.execute_next_at(&DelayedSuccess, 0).await?,
        WorkerTurn::Idle
    ));
    assert_eq!(repository.job(&job.id).await?.state, JobState::Queued);
    Ok(())
}
