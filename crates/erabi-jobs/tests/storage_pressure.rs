use std::{
    collections::BTreeMap,
    fs,
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use erabi_db::repositories::{
    CheckpointEnvelope, CheckpointIdentity, CheckpointUnitId, CrawlRunRepository, JobFailureCode,
    JobKind, JobRepository, JobRepositoryError, JobState, NewJob,
};
use erabi_db::{ErabiDatabase, MigrationRunner};
use erabi_domain::{
    CrawlRunId, CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunStatus, CrawlRunType,
    ResolvedValue, RobotsAudit, RunConfiguration, SettingSource, SnapshotOperationalSettings,
};
use erabi_jobs::{
    CancellationController, JobActionService, JobExecutionContext, JobExecutionError, JobHandler,
    JobRuntime, JobStorageClass, RerunFullCrawlInput, StoragePressureController,
    StoragePressureLevel, StoragePressureMonitor, StoragePressurePolicy,
    StoragePressurePolicyError, StorageProbe, StorageProbeError, WorkerPolicy, WorkerTurn,
};
use tokio::sync::Barrier;

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

fn policy() -> Result<StoragePressurePolicy, StoragePressurePolicyError> {
    StoragePressurePolicy::new(100, 50)
}

fn checkpoint(
    run_id: &str,
    snapshot: &CrawlRunSnapshot,
) -> Result<CheckpointEnvelope, Box<dyn std::error::Error>> {
    let identity = CheckpointIdentity::new(
        run_id,
        snapshot.snapshot_hash(),
        snapshot.checkpoint_compatibility_hash(),
    )?;
    let mut checkpoint = CheckpointEnvelope::new(identity);
    checkpoint
        .completed_units
        .push(CheckpointUnitId::new("unit-1")?);
    Ok(checkpoint)
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
            "2026-08-25T00:00:00Z",
            "https://example.test",
            "Erabi/0.1",
            None,
        ),
        actor: "operator".into(),
        created_at: "2026-08-25T00:00:00Z".into(),
    })?)
}

fn resolved<T>(value: T) -> ResolvedValue<T> {
    ResolvedValue {
        value,
        source: SettingSource::BuiltInDefault,
    }
}

async fn heavy_job(
    database: &ErabiDatabase,
    priority: i32,
    max_attempts: u32,
) -> Result<NewJob, Box<dyn std::error::Error>> {
    let run_id = CrawlRunId::new();
    let snapshot = snapshot()?;
    CrawlRunRepository::new(database)
        .create(run_id, CrawlRunStatus::Queued, &snapshot)
        .await?;
    let mut job = NewJob::new(JobKind::new("TEST_WORK")?, priority, 0, max_attempts)?;
    job.crawl_run_id = Some(run_id.to_string());
    JobRepository::new(database).enqueue(&job, 0).await?;
    Ok(job)
}

async fn light_job(
    database: &ErabiDatabase,
    priority: i32,
    scheduled_at: i64,
) -> Result<NewJob, Box<dyn std::error::Error>> {
    let job = NewJob::new(JobKind::new("TEST_WORK")?, priority, scheduled_at, 2)?;
    JobRepository::new(database).enqueue(&job, 0).await?;
    Ok(job)
}

#[derive(Clone, Copy)]
struct FakeProbe {
    result: Result<u64, StorageProbeError>,
}

#[derive(Clone)]
struct MutableProbe {
    result: Arc<Mutex<Result<u64, StorageProbeError>>>,
    observations: Arc<AtomicUsize>,
}

impl MutableProbe {
    fn new(result: Result<u64, StorageProbeError>) -> Self {
        Self {
            result: Arc::new(Mutex::new(result)),
            observations: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn set(&self, result: Result<u64, StorageProbeError>) {
        if let Ok(mut current) = self.result.lock() {
            *current = result;
        }
    }
}

impl StorageProbe for MutableProbe {
    fn free_bytes(&self, _path: &Path) -> Result<u64, StorageProbeError> {
        self.observations.fetch_add(1, Ordering::AcqRel);
        self.result
            .lock()
            .map_or(Err(StorageProbeError::Unavailable), |result| *result)
    }
}

impl StorageProbe for FakeProbe {
    fn free_bytes(&self, _path: &Path) -> Result<u64, StorageProbeError> {
        self.result
    }
}

#[derive(Clone)]
struct PathRecordingProbe {
    result: Result<u64, StorageProbeError>,
    path: Arc<Mutex<Option<PathBuf>>>,
}

impl StorageProbe for PathRecordingProbe {
    fn free_bytes(&self, path: &Path) -> Result<u64, StorageProbeError> {
        if let Ok(mut observed) = self.path.lock() {
            *observed = Some(path.to_path_buf());
        }
        self.result
    }
}

struct RecordingSuccess {
    ids: Arc<Mutex<Vec<String>>>,
}

impl JobHandler for RecordingSuccess {
    fn execute(
        &self,
        context: JobExecutionContext,
    ) -> impl Future<Output = Result<(), JobExecutionError>> {
        if let Ok(mut ids) = self.ids.lock() {
            ids.push(context.job_id().to_string());
        }
        std::future::ready(Ok(()))
    }
}

struct PressureCheckpoint {
    barrier: Arc<Barrier>,
    checkpoint: CheckpointEnvelope,
}

impl JobHandler for PressureCheckpoint {
    async fn execute(&self, context: JobExecutionContext) -> Result<(), JobExecutionError> {
        self.barrier.wait().await;
        context.storage_pressure().signalled().await;
        context
            .checkpoint(&self.checkpoint)
            .await
            .map_err(|_| JobExecutionError)?;
        Ok(())
    }
}

struct WaitForCancellation {
    barrier: Arc<Barrier>,
    pressure_seen: Arc<AtomicBool>,
}

struct PressureWithoutCheckpoint {
    barrier: Arc<Barrier>,
}

impl JobHandler for PressureWithoutCheckpoint {
    async fn execute(&self, context: JobExecutionContext) -> Result<(), JobExecutionError> {
        self.barrier.wait().await;
        context.storage_pressure().signalled().await;
        Ok(())
    }
}

struct FailedPressureCheckpoint {
    barrier: Arc<Barrier>,
    checkpoint: CheckpointEnvelope,
    write_failed: Arc<AtomicBool>,
}

impl JobHandler for FailedPressureCheckpoint {
    async fn execute(&self, context: JobExecutionContext) -> Result<(), JobExecutionError> {
        self.barrier.wait().await;
        context.storage_pressure().signalled().await;
        self.write_failed.store(
            context.checkpoint(&self.checkpoint).await.is_err(),
            Ordering::Release,
        );
        Ok(())
    }
}

impl JobHandler for WaitForCancellation {
    async fn execute(&self, context: JobExecutionContext) -> Result<(), JobExecutionError> {
        self.barrier.wait().await;
        tokio::select! {
            () = context.storage_pressure().signalled() => {
                self.pressure_seen.store(true, Ordering::Release);
            }
            () = context.cancellation().cancelled() => {}
        }
        Ok(())
    }
}

fn runtime(
    database: &ErabiDatabase,
    monitor: StoragePressureMonitor,
    cancellation: CancellationController,
) -> Result<JobRuntime<'_>, Box<dyn std::error::Error>> {
    runtime_with_policy(
        database,
        monitor,
        cancellation,
        WorkerPolicy {
            lease_duration_seconds: 30,
            retry_delay_seconds: 0,
        },
    )
}

fn runtime_with_policy(
    database: &ErabiDatabase,
    monitor: StoragePressureMonitor,
    cancellation: CancellationController,
    worker_policy: WorkerPolicy,
) -> Result<JobRuntime<'_>, Box<dyn std::error::Error>> {
    Ok(JobRuntime::with_storage_pressure_monitor(
        database,
        "storage-pressure-worker",
        worker_policy,
        cancellation,
        monitor,
    )?)
}

async fn execute_after_probe_becomes_critical<H: JobHandler>(
    runtime: &JobRuntime<'_>,
    handler: &H,
    probe: &MutableProbe,
    barrier: Arc<Barrier>,
    now: i64,
) -> Result<WorkerTurn, Box<dyn std::error::Error>> {
    let execution = runtime.execute_next_at(handler, now);
    let transition = async {
        barrier.wait().await;
        probe.set(Ok(50));
        tokio::time::advance(Duration::from_secs(1)).await;
    };
    let (turn, ()) = tokio::join!(execution, transition);
    Ok(turn?)
}

#[test]
fn threshold_boundaries_are_inclusive_and_ordering_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let policy = policy()?;
    assert_eq!(policy.classify(101).level, StoragePressureLevel::Healthy);
    assert_eq!(policy.classify(100).level, StoragePressureLevel::Warning);
    assert_eq!(policy.classify(50).level, StoragePressureLevel::Critical);
    assert_eq!(policy.classify(49).level, StoragePressureLevel::Critical);
    assert_eq!(
        StoragePressurePolicy::new(50, 50),
        Err(StoragePressurePolicyError::InvalidThresholdOrdering)
    );
    Ok(())
}

#[test]
fn pressure_transitions_are_observable_and_deterministic() -> Result<(), Box<dyn std::error::Error>>
{
    let policy = policy()?;
    let controller = StoragePressureController::new(policy);
    controller.update(policy.classify(101));
    assert_eq!(controller.state().level, StoragePressureLevel::Healthy);
    controller.update(policy.classify(100));
    assert_eq!(controller.state().level, StoragePressureLevel::Warning);
    controller.update(policy.classify(50));
    assert_eq!(controller.state().level, StoragePressureLevel::Critical);
    Ok(())
}

#[test]
fn fake_probe_is_deterministic_and_is_bound_to_the_authoritative_path()
-> Result<(), Box<dyn std::error::Error>> {
    let path = Arc::new(Mutex::new(None));
    let monitor = StoragePressureMonitor::new(
        PathRecordingProbe {
            result: Ok(50),
            path: Arc::clone(&path),
        },
        "C:\\erabi-data",
        policy()?,
    );
    let state = monitor.refresh();
    assert_eq!(state.level, StoragePressureLevel::Critical);
    assert_eq!(state.free_bytes, Some(50));
    assert_eq!(state.warning_threshold, 100);
    assert_eq!(state.critical_threshold, 50);
    assert_eq!(
        path.lock().ok().and_then(|path| path.clone()),
        Some(PathBuf::from("C:\\erabi-data"))
    );
    Ok(())
}

#[test]
fn probe_failure_is_unavailable_and_never_healthy() -> Result<(), Box<dyn std::error::Error>> {
    let monitor = StoragePressureMonitor::new(
        FakeProbe {
            result: Err(StorageProbeError::Unavailable),
        },
        PathBuf::from("C:\\erabi-data"),
        policy()?,
    );
    let state = monitor.refresh();
    assert_eq!(state.level, StoragePressureLevel::Unavailable);
    assert_eq!(state.free_bytes, None);
    assert!(!state.allows_artifact_heavy());
    Ok(())
}

#[tokio::test]
async fn critical_admission_blocks_heavy_without_attempt_or_lease_but_runs_light_in_order()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let heavy = heavy_job(&database, 100, 2).await?;
    let first_light = light_job(&database, 10, 0).await?;
    let second_light = light_job(&database, 10, 1).await?;
    let monitor = StoragePressureMonitor::new(
        FakeProbe { result: Ok(50) },
        PathBuf::from("C:\\erabi-data"),
        policy()?,
    );
    let ids = Arc::new(Mutex::new(Vec::new()));
    let runtime = runtime(&database, monitor, CancellationController::default())?;
    let handler = RecordingSuccess {
        ids: Arc::clone(&ids),
    };

    assert!(
        matches!(runtime.execute_next_at(&handler, 1).await?, WorkerTurn::Succeeded { job_id } if job_id == first_light.id)
    );
    assert!(
        matches!(runtime.execute_next_at(&handler, 1).await?, WorkerTurn::Succeeded { job_id } if job_id == second_light.id)
    );
    assert!(matches!(
        runtime.execute_next_at(&handler, 1).await?,
        WorkerTurn::Idle
    ));
    let queued_heavy = JobRepository::new(&database).job(&heavy.id).await?;
    assert_eq!(queued_heavy.state, JobState::Queued);
    assert_eq!(queued_heavy.current_attempt, 0);
    assert!(queued_heavy.lease.is_none());
    assert!(
        JobRepository::new(&database)
            .attempts(&heavy.id)
            .await?
            .is_empty()
    );
    assert_eq!(ids.lock().ok().map_or(0, |ids| ids.len()), 2);
    Ok(())
}

#[tokio::test]
async fn critical_pressure_signals_active_heavy_work_and_requeues_after_durable_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let heavy = heavy_job(&database, 0, 2).await?;
    let policy = policy()?;
    let monitor = StoragePressureMonitor::new(
        FakeProbe { result: Ok(101) },
        PathBuf::from("C:\\erabi-data"),
        policy,
    );
    let controller = monitor.controller().clone();
    let runtime = runtime(&database, monitor, CancellationController::default())?;
    let barrier = Arc::new(Barrier::new(2));
    let run_id = heavy
        .crawl_run_id
        .as_deref()
        .ok_or("heavy job is missing its run id")?;
    let run_snapshot = CrawlRunRepository::new(&database)
        .snapshot_by_stored_id(run_id)
        .await?;
    let handler = PressureCheckpoint {
        barrier: Arc::clone(&barrier),
        checkpoint: checkpoint(run_id, &run_snapshot)?,
    };
    let execution = runtime.execute_next_at(&handler, 0);
    let signal = async {
        barrier.wait().await;
        controller.update(policy.classify(50));
    };
    let (turn, ()) = tokio::join!(execution, signal);
    let turn = turn?;
    assert!(matches!(
        turn,
        WorkerTurn::StoragePressure {
            state: JobState::Queued,
            checkpoint_persisted: true,
            ..
        }
    ));
    let repository = JobRepository::new(&database);
    let queued = repository.job(&heavy.id).await?;
    assert_eq!(queued.state, JobState::Queued);
    assert_eq!(
        repository.attempts(&heavy.id).await?[0].failure_code,
        Some(JobFailureCode::StoragePressure)
    );
    assert_eq!(repository.checkpoints(&heavy.id).await?.len(), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn active_heavy_work_observes_a_real_probe_transition_on_its_heartbeat()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let heavy = heavy_job(&database, 0, 2).await?;
    let probe = MutableProbe::new(Ok(101));
    let monitor =
        StoragePressureMonitor::new(probe.clone(), PathBuf::from("C:\\erabi-data"), policy()?);
    let runtime = runtime_with_policy(
        &database,
        monitor,
        CancellationController::default(),
        WorkerPolicy {
            lease_duration_seconds: 2,
            retry_delay_seconds: 0,
        },
    )?;
    let barrier = Arc::new(Barrier::new(2));
    let run_id = heavy
        .crawl_run_id
        .as_deref()
        .ok_or("heavy job is missing its run id")?;
    let snapshot = CrawlRunRepository::new(&database)
        .snapshot_by_stored_id(run_id)
        .await?;
    let turn = execute_after_probe_becomes_critical(
        &runtime,
        &PressureCheckpoint {
            barrier: Arc::clone(&barrier),
            checkpoint: checkpoint(run_id, &snapshot)?,
        },
        &probe,
        barrier,
        0,
    )
    .await?;

    assert!(matches!(
        turn,
        WorkerTurn::StoragePressure {
            state: JobState::Queued,
            checkpoint_persisted: true,
            ..
        }
    ));
    assert!(probe.observations.load(Ordering::Acquire) >= 2);
    let repository = JobRepository::new(&database);
    assert_eq!(repository.job(&heavy.id).await?.state, JobState::Queued);
    assert_eq!(repository.checkpoints(&heavy.id).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn pressure_failed_work_remains_usable_by_task_four_resume_action()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let heavy = heavy_job(&database, 0, 1).await?;
    let policy = policy()?;
    let monitor = StoragePressureMonitor::new(
        FakeProbe { result: Ok(101) },
        PathBuf::from("C:\\erabi-data"),
        policy,
    );
    let controller = monitor.controller().clone();
    let runtime = runtime(&database, monitor, CancellationController::default())?;
    let barrier = Arc::new(Barrier::new(2));
    let run_id = heavy
        .crawl_run_id
        .as_deref()
        .ok_or("heavy job is missing its run id")?;
    let run_snapshot = CrawlRunRepository::new(&database)
        .snapshot_by_stored_id(run_id)
        .await?;
    let handler = PressureCheckpoint {
        barrier: Arc::clone(&barrier),
        checkpoint: checkpoint(run_id, &run_snapshot)?,
    };
    let execution = runtime.execute_next_at(&handler, 0);
    let signal = async {
        barrier.wait().await;
        controller.update(policy.classify(50));
    };
    let (turn, ()) = tokio::join!(execution, signal);
    assert!(matches!(
        turn?,
        WorkerTurn::StoragePressure {
            state: JobState::Failed,
            checkpoint_persisted: true,
            ..
        }
    ));

    let action = JobActionService::new(database.clone(), CancellationController::default());
    let resumed = action.resume(&heavy.id, 2).await?;
    assert_eq!(resumed.state, JobState::Queued);
    assert_eq!(resumed.parent_job_id, Some(heavy.id));
    Ok(())
}

#[tokio::test]
async fn storage_light_active_work_is_not_signalled_and_user_cancellation_remains_independent()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let light = light_job(&database, 0, 0).await?;
    let policy = policy()?;
    let monitor = StoragePressureMonitor::new(
        FakeProbe { result: Ok(101) },
        PathBuf::from("C:\\erabi-data"),
        policy,
    );
    let controller = monitor.controller().clone();
    let cancellation = CancellationController::default();
    let runtime = runtime(&database, monitor, cancellation)?;
    let barrier = Arc::new(Barrier::new(2));
    let pressure_seen = Arc::new(AtomicBool::new(false));
    let handler = WaitForCancellation {
        barrier: Arc::clone(&barrier),
        pressure_seen: Arc::clone(&pressure_seen),
    };
    let execution = runtime.execute_next_at(&handler, 0);
    let cancel = async {
        barrier.wait().await;
        controller.update(policy.classify(50));
        runtime.request_cancellation(&light.id, 1).await
    };
    let (turn, cancellation_result) = tokio::join!(execution, cancel);
    assert_eq!(cancellation_result?, JobState::Running);
    assert!(matches!(
        turn?,
        WorkerTurn::Cancelled {
            checkpoint_persisted: false,
            ..
        }
    ));
    assert!(!pressure_seen.load(Ordering::Acquire));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn pressure_without_a_current_attempt_checkpoint_fails_instead_of_requeueing()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let heavy = heavy_job(&database, 0, 2).await?;
    let probe = MutableProbe::new(Ok(101));
    let monitor =
        StoragePressureMonitor::new(probe.clone(), PathBuf::from("C:\\erabi-data"), policy()?);
    let runtime = runtime_with_policy(
        &database,
        monitor,
        CancellationController::default(),
        WorkerPolicy {
            lease_duration_seconds: 2,
            retry_delay_seconds: 0,
        },
    )?;
    let barrier = Arc::new(Barrier::new(2));
    let turn = execute_after_probe_becomes_critical(
        &runtime,
        &PressureWithoutCheckpoint {
            barrier: Arc::clone(&barrier),
        },
        &probe,
        barrier,
        0,
    )
    .await?;

    assert!(matches!(
        turn,
        WorkerTurn::StoragePressure {
            state: JobState::Failed,
            checkpoint_persisted: false,
            ..
        }
    ));
    let repository = JobRepository::new(&database);
    assert_eq!(repository.job(&heavy.id).await?.state, JobState::Failed);
    assert_eq!(repository.attempts(&heavy.id).await?.len(), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn failed_checkpoint_write_does_not_make_pressure_interruption_resumable()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let heavy = heavy_job(&database, 0, 2).await?;
    let run_id = heavy
        .crawl_run_id
        .as_deref()
        .ok_or("heavy job is missing its run id")?;
    let snapshot = CrawlRunRepository::new(&database)
        .snapshot_by_stored_id(run_id)
        .await?;
    let mut invalid = checkpoint(run_id, &snapshot)?;
    invalid
        .completed_units
        .push(invalid.completed_units[0].clone());
    let probe = MutableProbe::new(Ok(101));
    let monitor =
        StoragePressureMonitor::new(probe.clone(), PathBuf::from("C:\\erabi-data"), policy()?);
    let runtime = runtime_with_policy(
        &database,
        monitor,
        CancellationController::default(),
        WorkerPolicy {
            lease_duration_seconds: 2,
            retry_delay_seconds: 0,
        },
    )?;
    let barrier = Arc::new(Barrier::new(2));
    let write_failed = Arc::new(AtomicBool::new(false));
    let turn = execute_after_probe_becomes_critical(
        &runtime,
        &FailedPressureCheckpoint {
            barrier: Arc::clone(&barrier),
            checkpoint: invalid,
            write_failed: Arc::clone(&write_failed),
        },
        &probe,
        barrier,
        0,
    )
    .await?;

    assert!(write_failed.load(Ordering::Acquire));
    assert!(matches!(
        turn,
        WorkerTurn::StoragePressure {
            state: JobState::Failed,
            checkpoint_persisted: false,
            ..
        }
    ));
    let repository = JobRepository::new(&database);
    assert_eq!(repository.job(&heavy.id).await?.state, JobState::Failed);
    assert!(repository.checkpoints(&heavy.id).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn stale_lease_cannot_commit_a_pressure_transition_even_with_a_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let heavy = heavy_job(&database, 0, 2).await?;
    let repository = JobRepository::new(&database);
    let acquired = repository
        .acquire_next("checkpoint-owner", 0, 30)
        .await?
        .ok_or("heavy job was not acquired")?;
    let lease = acquired
        .job
        .lease
        .clone()
        .ok_or("acquired job has no lease")?;
    let run_id = heavy
        .crawl_run_id
        .as_deref()
        .ok_or("heavy job is missing its run id")?;
    let snapshot = CrawlRunRepository::new(&database)
        .snapshot_by_stored_id(run_id)
        .await?;
    repository
        .append_checkpoint(
            &heavy.id,
            &acquired.attempt.id,
            &lease,
            &checkpoint(run_id, &snapshot)?,
            1,
        )
        .await?;
    let mut stale = lease.clone();
    stale.owner = "stale-owner".to_owned();

    assert!(matches!(
        repository
            .requeue_after_storage_pressure(&heavy.id, &stale, 1)
            .await,
        Err(JobRepositoryError::LeaseLost)
    ));
    assert_eq!(repository.job(&heavy.id).await?.state, JobState::Running);
    Ok(())
}

#[tokio::test]
async fn critical_pressure_blocks_run_backed_task_four_children_but_not_control_actions_or_light_work()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let source = heavy_job(&database, 100, 1).await?;
    let repository = JobRepository::new(&database);
    let acquired = repository
        .acquire_next("source-worker", 0, 30)
        .await?
        .ok_or("source job was not acquired")?;
    let lease = acquired
        .job
        .lease
        .clone()
        .ok_or("acquired job has no lease")?;
    let run_id = source
        .crawl_run_id
        .as_deref()
        .ok_or("source job is missing its run id")?;
    let snapshot = CrawlRunRepository::new(&database)
        .snapshot_by_stored_id(run_id)
        .await?;
    repository
        .append_checkpoint(
            &source.id,
            &acquired.attempt.id,
            &lease,
            &checkpoint(run_id, &snapshot)?,
            1,
        )
        .await?;
    assert_eq!(
        repository
            .fail(&source.id, &lease, 1, JobFailureCode::HandlerFailed, 1)
            .await?,
        JobState::Failed
    );

    let actions = JobActionService::new(database.clone(), CancellationController::default());
    let resumed = actions.resume(&source.id, 2).await?;
    let rerun = actions
        .rerun_full_crawl(&source.id, 2, RerunFullCrawlInput::default())
        .await?;
    assert_eq!(resumed.state, JobState::Queued);
    assert_eq!(rerun.state, JobState::Queued);
    assert_eq!(resumed.parent_job_id, Some(source.id.clone()));
    assert_eq!(rerun.parent_job_id, Some(source.id.clone()));

    let first_light = light_job(&database, 10, 2).await?;
    let second_light = light_job(&database, 10, 2).await?;
    let monitor = StoragePressureMonitor::new(
        FakeProbe { result: Ok(50) },
        PathBuf::from("C:\\erabi-data"),
        policy()?,
    );
    let ids = Arc::new(Mutex::new(Vec::new()));
    let runtime = runtime(&database, monitor, CancellationController::default())?;
    let handler = RecordingSuccess {
        ids: Arc::clone(&ids),
    };

    assert!(matches!(
        runtime.execute_next_at(&handler, 2).await?,
        WorkerTurn::Succeeded { job_id } if job_id == first_light.id
    ));
    assert!(matches!(
        runtime.execute_next_at(&handler, 2).await?,
        WorkerTurn::Succeeded { job_id } if job_id == second_light.id
    ));
    assert!(matches!(
        runtime.execute_next_at(&handler, 2).await?,
        WorkerTurn::Idle
    ));
    for child in [&resumed, &rerun] {
        let queued = repository.job(&child.job_id).await?;
        assert_eq!(queued.state, JobState::Queued);
        assert_eq!(queued.storage_class(), JobStorageClass::ArtifactHeavy);
        assert_eq!(queued.current_attempt, 0);
        assert!(queued.lease.is_none());
        assert!(repository.attempts(&child.job_id).await?.is_empty());
    }
    assert_eq!(
        ids.lock().ok().map(|ids| ids.clone()),
        Some(vec![
            first_light.id.to_string(),
            second_light.id.to_string()
        ])
    );
    Ok(())
}

#[test]
fn warning_and_critical_pressure_never_delete_user_artifacts()
-> Result<(), Box<dyn std::error::Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!("erabi-storage-pressure-{nonce}"));
    fs::create_dir_all(&root)?;
    let artifact = root.join("user-artifact.bin");
    fs::write(&artifact, b"preserve")?;
    let policy = policy()?;
    for free_bytes in [100, 50] {
        let monitor = StoragePressureMonitor::new(
            FakeProbe {
                result: Ok(free_bytes),
            },
            root.clone(),
            policy,
        );
        let level = monitor.refresh().level;
        assert!(matches!(
            level,
            StoragePressureLevel::Warning | StoragePressureLevel::Critical
        ));
        assert!(artifact.exists());
    }
    fs::remove_file(&artifact)?;
    fs::remove_dir_all(&root)?;
    Ok(())
}

#[test]
fn storage_class_keeps_all_run_backed_execution_heavy_and_control_plane_jobs_light()
-> Result<(), Box<dyn std::error::Error>> {
    let policy = policy()?;
    let heavy_record = erabi_db::repositories::JobRecord {
        id: erabi_db::repositories::JobId::new(),
        kind: JobKind::new("TEST_WORK")?,
        priority: 0,
        state: JobState::Queued,
        parent_job_id: None,
        crawl_run_id: Some("run".to_owned()),
        scheduled_at: 0,
        current_attempt: 0,
        max_attempts: 1,
        lease_generation: 0,
        lease: None,
        failure_code: None,
        created_at: 0,
        updated_at: 0,
    };
    let light_record = erabi_db::repositories::JobRecord {
        parent_job_id: Some(heavy_record.id.clone()),
        crawl_run_id: Some("run".to_owned()),
        ..heavy_record.clone()
    };
    assert_eq!(heavy_record.storage_class(), JobStorageClass::ArtifactHeavy);
    assert_eq!(light_record.storage_class(), JobStorageClass::ArtifactHeavy);
    assert!(!policy.classify(50).allows_artifact_heavy());
    Ok(())
}
