use std::{collections::BTreeMap, sync::Arc};

use erabi_db::repositories::{
    CheckpointEnvelope, CheckpointIdentity, CheckpointUnitId, CrawlRunRepository, JobFailureCode,
    JobKind, JobRepository, JobState, NewJob,
};
use erabi_db::{ErabiDatabase, MigrationRunner};
use erabi_domain::{
    CrawlRunId, CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunStatus, CrawlRunType,
    ResolvedValue, RobotsAudit, RobotsDecision, RunConfiguration, SettingSource,
    SnapshotOperationalSettings,
};
use erabi_jobs::{
    CancellationController, JobAction, JobActionError, JobActionService, JobExecutionContext,
    JobExecutionError, JobHandler, JobRuntime, RerunFullCrawlInput, WorkerPolicy, WorkerTurn,
};
use tokio::sync::Notify;

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

fn snapshot() -> Result<CrawlRunSnapshot, Box<dyn std::error::Error>> {
    snapshot_with_robots(RobotsAudit::respect(
        "operator",
        "2026-08-23T00:00:00Z",
        "https://example.test",
        "Erabi/0.1",
        None,
    ))
}

fn snapshot_with_robots(
    robots: RobotsAudit,
) -> Result<CrawlRunSnapshot, Box<dyn std::error::Error>> {
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
        robots,
        actor: "operator".into(),
        created_at: "2026-08-23T00:00:00Z".into(),
    })?)
}

fn resolved<T>(value: T) -> ResolvedValue<T> {
    ResolvedValue {
        value,
        source: SettingSource::BuiltInDefault,
    }
}

async fn queued_job(
    database: &ErabiDatabase,
    max_attempts: u32,
) -> Result<NewJob, Box<dyn std::error::Error>> {
    let job = NewJob::new(JobKind::new("TEST_WORK")?, 0, 0, max_attempts)?;
    JobRepository::new(database).enqueue(&job, 0).await?;
    Ok(job)
}

async fn run_backed_job(
    database: &ErabiDatabase,
    max_attempts: u32,
) -> Result<(NewJob, CrawlRunId, CrawlRunSnapshot), Box<dyn std::error::Error>> {
    let snapshot = snapshot()?;
    run_backed_job_with_snapshot(database, max_attempts, snapshot).await
}

async fn run_backed_job_with_snapshot(
    database: &ErabiDatabase,
    max_attempts: u32,
    snapshot: CrawlRunSnapshot,
) -> Result<(NewJob, CrawlRunId, CrawlRunSnapshot), Box<dyn std::error::Error>> {
    let run_id = CrawlRunId::new();
    CrawlRunRepository::new(database)
        .create(run_id, CrawlRunStatus::Queued, &snapshot)
        .await?;
    let mut job = NewJob::new(JobKind::new("TEST_WORK")?, 0, 0, max_attempts)?;
    job.crawl_run_id = Some(run_id.to_string());
    JobRepository::new(database).enqueue(&job, 0).await?;
    Ok((job, run_id, snapshot))
}

async fn cancel_active(
    database: &ErabiDatabase,
    job: &NewJob,
    checkpoint: Option<CheckpointEnvelope>,
) -> Result<(), Box<dyn std::error::Error>> {
    let repository = JobRepository::new(database);
    let acquired = repository
        .acquire_next("action-test-worker", 0, 30)
        .await?
        .ok_or("job was not acquired")?;
    let lease = acquired.job.lease.clone().ok_or("lease missing")?;
    if let Some(checkpoint) = checkpoint {
        repository
            .append_checkpoint(&job.id, &acquired.attempt.id, &lease, &checkpoint, 1)
            .await?;
    }
    repository.cancel(&job.id, &lease, 2).await?;
    Ok(())
}

async fn succeed_active(
    database: &ErabiDatabase,
    job: &NewJob,
    checkpoint: Option<CheckpointEnvelope>,
) -> Result<(), Box<dyn std::error::Error>> {
    let repository = JobRepository::new(database);
    let acquired = repository
        .acquire_next("action-test-worker", 0, 30)
        .await?
        .ok_or("job was not acquired")?;
    let lease = acquired.job.lease.clone().ok_or("lease missing")?;
    if let Some(checkpoint) = checkpoint {
        repository
            .append_checkpoint(&job.id, &acquired.attempt.id, &lease, &checkpoint, 1)
            .await?;
    }
    repository.succeed(&job.id, &lease, 2).await?;
    Ok(())
}

fn compatible_checkpoint(
    run_id: CrawlRunId,
    snapshot: &CrawlRunSnapshot,
) -> Result<CheckpointEnvelope, Box<dyn std::error::Error>> {
    Ok(CheckpointEnvelope::new(CheckpointIdentity::new(
        run_id.to_string(),
        snapshot.snapshot_hash(),
        snapshot.checkpoint_compatibility_hash(),
    )?))
}

#[tokio::test]
async fn retry_preserves_attempt_history_and_creates_a_new_attempt()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let snapshot = snapshot_with_robots(RobotsAudit::override_with_reason(
        "frozen retry approval",
        "operator",
        "2026-08-23T00:00:00Z",
        "https://example.test",
        "Erabi/0.1",
        None,
    )?)?;
    let (job, run_id, snapshot) = run_backed_job_with_snapshot(&database, 3, snapshot).await?;
    let repository = JobRepository::new(&database);
    let acquired = repository
        .acquire_next("retry-worker", 0, 30)
        .await?
        .ok_or("not acquired")?;
    let lease = acquired.job.lease.clone().ok_or("lease missing")?;
    repository.cancel(&job.id, &lease, 1).await?;

    let service = JobActionService::new(database.clone(), CancellationController::default());
    let result = service.retry(&job.id, 2).await?;
    assert_eq!(result.action, JobAction::Retry);
    assert_eq!(repository.attempts(&job.id).await?.len(), 1);
    assert_eq!(result.parent_job_id, Some(job.id.clone()));
    assert_eq!(result.crawl_run_id, Some(run_id.to_string()));
    assert_eq!(
        CrawlRunRepository::new(&database)
            .snapshot(run_id)
            .await?
            .snapshot_hash(),
        snapshot.snapshot_hash()
    );
    assert!(matches!(
        CrawlRunRepository::new(&database)
            .snapshot(run_id)
            .await?
            .robots()
            .decision(),
        RobotsDecision::Override { reason } if reason == "frozen retry approval"
    ));
    assert_eq!(repository.job(&job.id).await?.state, JobState::Cancelled);
    let second = repository
        .acquire_next("retry-worker-2", 2, 30)
        .await?
        .ok_or("not reacquired")?;
    assert_eq!(second.job.id, result.job_id);
    assert_eq!(second.attempt.attempt_number, 1);
    assert_eq!(repository.attempts(&job.id).await?.len(), 1);
    assert_eq!(repository.attempts(&result.job_id).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn retry_cannot_exceed_max_attempts() -> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let job = queued_job(&database, 1).await?;
    let repository = JobRepository::new(&database);
    let acquired = repository
        .acquire_next("bounded-worker", 0, 30)
        .await?
        .ok_or("not acquired")?;
    let lease = acquired.job.lease.clone().ok_or("lease missing")?;
    repository
        .fail(&job.id, &lease, 1, JobFailureCode::HandlerFailed, 1)
        .await?;
    let service = JobActionService::new(database.clone(), CancellationController::default());
    assert!(matches!(
        service.retry(&job.id, 2).await,
        Err(JobActionError::AttemptsExhausted)
    ));
    assert_eq!(repository.attempts(&job.id).await?.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_retry_requests_create_one_execution_lineage()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let job = queued_job(&database, 3).await?;
    let repository = JobRepository::new(&database);
    let acquired = repository
        .acquire_next("concurrent-worker", 0, 30)
        .await?
        .ok_or("not acquired")?;
    let lease = acquired.job.lease.clone().ok_or("lease missing")?;
    repository.cancel(&job.id, &lease, 1).await?;
    let service = JobActionService::new(database.clone(), CancellationController::default());
    let (left, right) = tokio::join!(service.retry(&job.id, 2), service.retry(&job.id, 2));
    assert_eq!(u8::from(left.is_ok()) + u8::from(right.is_ok()), 1);
    assert_eq!(repository.attempts(&job.id).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn resume_requires_a_current_compatible_checkpoint() -> Result<(), Box<dyn std::error::Error>>
{
    let database = database().await?;
    let (job, run_id, snapshot) = run_backed_job(&database, 3).await?;
    cancel_active(
        &database,
        &job,
        Some(compatible_checkpoint(run_id, &snapshot)?),
    )
    .await?;
    let service = JobActionService::new(database.clone(), CancellationController::default());
    let result = service.resume(&job.id, 3).await?;
    assert_eq!(result.action, JobAction::ResumeCheckpoint);
    assert_eq!(result.parent_job_id, Some(job.id.clone()));
    assert_eq!(result.crawl_run_id, job.crawl_run_id);
    assert_eq!(
        CrawlRunRepository::new(&database)
            .snapshot(run_id)
            .await?
            .snapshot_hash(),
        snapshot.snapshot_hash()
    );
    let child = JobRepository::new(&database).job(&result.job_id).await?;
    assert!(
        JobRepository::new(&database)
            .latest_checkpoint(&child.id)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn resume_rejects_missing_checkpoint() -> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (job, _, _) = run_backed_job(&database, 3).await?;
    cancel_active(&database, &job, None).await?;
    let service = JobActionService::new(database, CancellationController::default());
    assert!(matches!(
        service.resume(&job.id, 3).await,
        Err(JobActionError::CheckpointMissing)
    ));
    Ok(())
}

#[tokio::test]
async fn resume_rejects_incompatible_checkpoint() -> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (job, _, _) = run_backed_job(&database, 3).await?;
    let wrong = CheckpointEnvelope::new(CheckpointIdentity::new(
        "other-run",
        "a".repeat(64),
        "b".repeat(64),
    )?);
    cancel_active(&database, &job, Some(wrong)).await?;
    let service = JobActionService::new(database, CancellationController::default());
    assert!(matches!(
        service.resume(&job.id, 3).await,
        Err(JobActionError::CheckpointIncompatible)
    ));
    Ok(())
}

#[tokio::test]
async fn restart_reuses_the_same_run_and_rerun_creates_an_independent_run()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (job, run_id, snapshot) = run_backed_job(&database, 3).await?;
    cancel_active(
        &database,
        &job,
        Some(compatible_checkpoint(run_id, &snapshot)?),
    )
    .await?;
    let service = JobActionService::new(database.clone(), CancellationController::default());
    let restart = service.restart_from_beginning(&job.id, 3).await?;
    let restart_record = JobRepository::new(&database).job(&restart.job_id).await?;
    assert_eq!(restart_record.kind.as_str(), "RESTART_FROM_BEGINNING");
    assert_eq!(restart.crawl_run_id, Some(run_id.to_string()));
    assert!(
        JobRepository::new(&database)
            .latest_checkpoint(&restart.job_id)
            .await?
            .is_none()
    );
    let rerun = service
        .rerun_full_crawl(&job.id, 3, RerunFullCrawlInput::default())
        .await?;
    let rerun_run_id = rerun.crawl_run_id.as_deref().ok_or("run missing")?;
    let rerun_snapshot = CrawlRunRepository::new(&database)
        .snapshot_by_stored_id(rerun_run_id)
        .await?;
    assert_ne!(rerun_snapshot.snapshot_hash(), snapshot.snapshot_hash());
    assert_eq!(rerun_snapshot.run_type(), snapshot.run_type());
    assert_eq!(rerun_snapshot.configuration(), snapshot.configuration());
    assert_eq!(
        rerun_snapshot.selected_seed_ids(),
        snapshot.selected_seed_ids()
    );
    assert_eq!(rerun_snapshot.run_profile_id(), snapshot.run_profile_id());
    assert_eq!(rerun_snapshot.settings(), snapshot.settings());
    assert_eq!(rerun_snapshot.actor(), snapshot.actor());
    assert_eq!(rerun_snapshot.robots().actor(), snapshot.robots().actor());
    assert_eq!(
        rerun_snapshot.checkpoint_compatibility_hash(),
        snapshot.checkpoint_compatibility_hash()
    );
    assert_eq!(rerun_snapshot.created_at(), "3");
    assert_eq!(rerun_snapshot.robots().decided_at(), "3");
    assert!(matches!(
        rerun_snapshot.robots().decision(),
        RobotsDecision::Respect
    ));
    assert_eq!(
        CrawlRunRepository::new(&database)
            .created_audit_occurred_at_by_stored_id(rerun_run_id)
            .await?,
        "3"
    );
    assert_ne!(rerun.crawl_run_id, Some(run_id.to_string()));
    assert_eq!(rerun.parent_job_id, Some(job.id));
    Ok(())
}

#[tokio::test]
async fn retry_failed_parts_preserves_successful_checkpoint_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (job, run_id, snapshot) = run_backed_job(&database, 3).await?;
    let mut checkpoint = compatible_checkpoint(run_id, &snapshot)?;
    checkpoint
        .completed_units
        .push(CheckpointUnitId::new("success-1")?);
    checkpoint
        .failed_units
        .push(CheckpointUnitId::new("failed-1")?);
    cancel_active(&database, &job, Some(checkpoint)).await?;
    let service = JobActionService::new(database.clone(), CancellationController::default());
    let result = service.retry_failed_parts(&job.id, 3).await?;
    assert_eq!(result.failed_part_count, Some(1));
    assert_eq!(result.crawl_run_id, job.crawl_run_id);
    let records = JobRepository::new(&database).checkpoints(&job.id).await?;
    assert_eq!(
        records[0].checkpoint.completed_units[0].as_str(),
        "success-1"
    );
    assert_eq!(records[0].checkpoint.failed_units[0].as_str(), "failed-1");
    Ok(())
}

#[tokio::test]
async fn independent_rerun_requires_fresh_robots_override_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let source_snapshot = snapshot_with_robots(RobotsAudit::override_with_reason(
        "initial approval",
        "operator",
        "2026-08-23T00:00:00Z",
        "https://example.test",
        "Erabi/0.1",
        None,
    )?)?;
    let (job, run_id, source_snapshot) =
        run_backed_job_with_snapshot(&database, 3, source_snapshot).await?;
    cancel_active(
        &database,
        &job,
        Some(compatible_checkpoint(run_id, &source_snapshot)?),
    )
    .await?;
    let service = JobActionService::new(database.clone(), CancellationController::default());
    let resumed = service.resume(&job.id, 3).await?;
    assert_eq!(resumed.crawl_run_id, Some(run_id.to_string()));
    assert!(matches!(
        CrawlRunRepository::new(&database)
            .snapshot(run_id)
            .await?
            .robots()
            .decision(),
        RobotsDecision::Override { reason } if reason == "initial approval"
    ));
    assert!(matches!(
        service
            .rerun_full_crawl(&job.id, 4, RerunFullCrawlInput::default())
            .await,
        Err(JobActionError::RobotsOverrideReasonRequired)
    ));

    let result = service
        .rerun_full_crawl(
            &job.id,
            5,
            RerunFullCrawlInput {
                robots_override_reason: Some("renewed approval".into()),
            },
        )
        .await?;
    assert_ne!(result.crawl_run_id, Some(run_id.to_string()));
    let rerun = CrawlRunRepository::new(&database)
        .snapshot_by_stored_id(result.crawl_run_id.as_deref().ok_or("run missing")?)
        .await?;
    assert!(matches!(
        rerun.robots().decision(),
        RobotsDecision::Override { reason } if reason == "renewed approval"
    ));
    assert_eq!(rerun.created_at(), "5");
    assert_eq!(rerun.robots().decided_at(), "5");
    assert_eq!(rerun.actor(), source_snapshot.actor());
    assert_eq!(rerun.robots().actor(), source_snapshot.robots().actor());
    assert_ne!(rerun.snapshot_hash(), source_snapshot.snapshot_hash());
    assert_eq!(
        CrawlRunRepository::new(&database)
            .created_audit_occurred_at_by_stored_id(
                result.crawl_run_id.as_deref().ok_or("run missing")?
            )
            .await?,
        "5"
    );
    Ok(())
}

#[tokio::test]
async fn succeeded_work_rejects_in_place_recovery_but_allows_independent_rerun()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (job, run_id, snapshot) = run_backed_job(&database, 3).await?;
    succeed_active(
        &database,
        &job,
        Some(compatible_checkpoint(run_id, &snapshot)?),
    )
    .await?;
    let service = JobActionService::new(database.clone(), CancellationController::default());

    assert!(matches!(
        service.retry(&job.id, 3).await,
        Err(JobActionError::IllegalLifecycleState)
    ));
    assert!(matches!(
        service.retry_failed_parts(&job.id, 3).await,
        Err(JobActionError::IllegalLifecycleState)
    ));
    assert!(matches!(
        service.resume(&job.id, 3).await,
        Err(JobActionError::IllegalLifecycleState)
    ));
    assert!(matches!(
        service.restart_from_beginning(&job.id, 3).await,
        Err(JobActionError::IllegalLifecycleState)
    ));

    let rerun = service
        .rerun_full_crawl(&job.id, 4, RerunFullCrawlInput::default())
        .await?;
    assert_ne!(rerun.crawl_run_id, Some(run_id.to_string()));
    assert_eq!(rerun.parent_job_id, Some(job.id));
    Ok(())
}

#[tokio::test]
async fn rerun_full_crawl_rejects_a_generic_job_without_a_crawl_run()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let job = queued_job(&database, 2).await?;
    cancel_active(&database, &job, None).await?;
    let service = JobActionService::new(database, CancellationController::default());
    assert!(matches!(
        service
            .rerun_full_crawl(&job.id, 3, RerunFullCrawlInput::default())
            .await,
        Err(JobActionError::CrawlRunRequired)
    ));
    Ok(())
}

#[tokio::test]
async fn retry_continuation_cannot_reset_budget_from_a_terminal_ancestor()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let source = queued_job(&database, 3).await?;
    cancel_active(&database, &source, None).await?;
    let repository = JobRepository::new(&database);
    let service = JobActionService::new(database.clone(), CancellationController::default());
    let child = service.retry(&source.id, 3).await?;
    let acquired = repository
        .acquire_next("retry-child-worker", 3, 30)
        .await?
        .ok_or("child was not acquired")?;
    assert_eq!(acquired.job.id, child.job_id);
    let lease = acquired.job.lease.clone().ok_or("lease missing")?;
    repository.cancel(&child.job_id, &lease, 4).await?;

    assert!(matches!(
        service.retry(&source.id, 5).await,
        Err(JobActionError::RetryAlreadyContinued)
    ));
    let grandchild = service.retry(&child.job_id, 5).await?;
    let grandchild_record = repository.job(&grandchild.job_id).await?;
    assert_eq!(grandchild_record.parent_job_id, Some(child.job_id));
    assert_eq!(grandchild_record.max_attempts, 1);
    Ok(())
}

#[tokio::test]
async fn parent_linked_queued_action_child_is_not_removable()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let source = queued_job(&database, 2).await?;
    cancel_active(&database, &source, None).await?;
    let service = JobActionService::new(database.clone(), CancellationController::default());
    let child = service.retry(&source.id, 3).await?;
    assert!(matches!(
        service.remove(&child.job_id).await,
        Err(JobActionError::NotRemovable)
    ));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_same_run_recovery_actions_create_one_active_continuation()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (job, run_id, snapshot) = run_backed_job(&database, 3).await?;
    cancel_active(
        &database,
        &job,
        Some(compatible_checkpoint(run_id, &snapshot)?),
    )
    .await?;
    let service = JobActionService::new(database, CancellationController::default());
    let (resume, restart) = tokio::join!(
        service.resume(&job.id, 3),
        service.restart_from_beginning(&job.id, 3)
    );
    assert_eq!(u8::from(resume.is_ok()) + u8::from(restart.is_ok()), 1);
    let error = resume.err().or(restart.err()).ok_or("missing conflict")?;
    assert!(matches!(error, JobActionError::ConcurrentTransition));
    Ok(())
}

#[tokio::test]
async fn queue_move_and_safe_removal_use_state_and_history_predicates()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let queued = queued_job(&database, 1).await?;
    let other = queued_job(&database, 1).await?;
    let service = JobActionService::new(database.clone(), CancellationController::default());
    service.reprioritize(&queued.id, 10, None, 1).await?;
    assert_eq!(
        JobRepository::new(&database)
            .job(&queued.id)
            .await?
            .priority,
        10
    );
    service.remove(&queued.id).await?;
    assert!(matches!(
        JobRepository::new(&database).job(&queued.id).await,
        Err(erabi_db::repositories::JobRepositoryError::NotFound)
    ));

    let repository = JobRepository::new(&database);
    let acquired = repository
        .acquire_next("queue-worker", 0, 30)
        .await?
        .ok_or("not acquired")?;
    let lease = acquired.job.lease.clone().ok_or("lease missing")?;
    assert!(matches!(
        service.reprioritize(&other.id, 1, None, 1).await,
        Err(JobActionError::NotReprioritizable)
    ));
    repository.cancel(&other.id, &lease, 2).await?;
    assert!(matches!(
        service.remove(&other.id).await,
        Err(JobActionError::NotRemovable)
    ));
    assert!(matches!(
        service.reprioritize(&other.id, 1, None, 3).await,
        Err(JobActionError::NotReprioritizable)
    ));
    Ok(())
}

#[tokio::test]
async fn cancellation_action_uses_the_existing_queued_cancellation_path()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let job = queued_job(&database, 1).await?;
    let service = JobActionService::new(database.clone(), CancellationController::default());
    let result = service.cancel(&job.id, 4).await?;
    assert_eq!(result.state, JobState::Cancelled);
    assert_eq!(
        JobRepository::new(&database).job(&job.id).await?.state,
        JobState::Cancelled
    );
    Ok(())
}

struct WaitForActionCancellation {
    started: Arc<Notify>,
}

impl JobHandler for WaitForActionCancellation {
    async fn execute(&self, context: JobExecutionContext) -> Result<(), JobExecutionError> {
        self.started.notify_one();
        context.cancellation().cancelled().await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_cancel_action_remains_cooperative_and_uses_task_3_runtime_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let job = queued_job(&database, 1).await?;
    let cancellation = CancellationController::default();
    let runtime = JobRuntime::with_cancellation_controller(
        &database,
        "action-cancel-worker",
        WorkerPolicy::conservative(),
        cancellation.clone(),
    )?;
    let service = JobActionService::new(database.clone(), cancellation);
    let started = Arc::new(Notify::new());
    let handler = WaitForActionCancellation {
        started: Arc::clone(&started),
    };
    let (execution, action) = tokio::join!(runtime.execute_next_at(&handler, 0), async {
        started.notified().await;
        service.cancel(&job.id, 0).await
    });
    assert_eq!(action?.state, JobState::Running);
    assert!(matches!(execution?, WorkerTurn::Cancelled { .. }));
    Ok(())
}

#[test]
fn unsafe_checkpoint_errors_have_a_stable_action_classification() {
    let error = erabi_jobs::JobActionError::CheckpointUnsafe;
    assert_eq!(error.to_string(), "the durable checkpoint is unsafe to use");
}
