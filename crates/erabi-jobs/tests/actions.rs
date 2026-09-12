use std::{collections::BTreeMap, path::Path, sync::Arc};

use erabi_crawler::{
    CrawlRecoveryCheckpoint, CrawlRecoveryPhase, PRODUCTION_ROOT_MAX_ATTEMPTS,
    ProductionRunSubmissionRequest, ProductionRunSubmissionService,
};
use erabi_db::repositories::{
    AttemptOutcome, CheckpointEnvelope, CrawlAdmissionState, CrawlExecutionRecord,
    CrawlExecutionRepository, CrawlExecutionSummary, CrawlRunRepository, CrawlTraversalControl,
    CrawlTraversalRepository, CrawlUrlStateRecord, CrawlWorkState, CrawlerRepository,
    JobFailureCode, JobId, JobKind, JobRepository, JobState, NewJob, NewProgressEvent,
    ProgressMetadata, ProgressReplayRequest, ProgressRepository, ProgressTerminalState,
};
use erabi_db::{ErabiDatabase, MigrationRunner};
use erabi_domain::{
    CrawlExecutionId, CrawlExecutionOutcome, CrawlRunId, CrawlRunSnapshot, CrawlRunSnapshotDraft,
    CrawlRunStatus, CrawlRunType, Crawler, ResolvedValue, RobotsAudit, RobotsDecision,
    RunConfiguration, Seed, SettingSource, SnapshotOperationalSettings,
};
use erabi_jobs::{
    CancellationController, JobAction, JobActionError, JobActionService, JobExecutionContext,
    JobExecutionError, JobHandler, JobRuntime, RerunFullCrawlInput, StoragePressureMonitor,
    StoragePressurePolicy, StorageProbe, StorageProbeError, WorkerPolicy, WorkerTurn,
};
use tokio::sync::Notify;

#[derive(Clone, Copy)]
struct HealthyStorageProbe;

impl StorageProbe for HealthyStorageProbe {
    fn free_bytes(&self, _path: &Path) -> Result<u64, StorageProbeError> {
        Ok(u64::MAX)
    }
}

struct SystemFailureHandler {
    database: ErabiDatabase,
    run_id: CrawlRunId,
    mark_running: bool,
}

impl JobHandler for SystemFailureHandler {
    fn execute(
        &self,
        _context: JobExecutionContext,
    ) -> impl std::future::Future<Output = Result<(), JobExecutionError>> + Send {
        let database = self.database.clone();
        let run_id = self.run_id;
        let mark_running = self.mark_running;
        async move {
            if mark_running
                && CrawlRunRepository::new(&database)
                    .transition_execution_status(run_id, CrawlRunStatus::Running)
                    .await
                    .is_err()
            {
                return Err(JobExecutionError);
            }
            Err(JobExecutionError)
        }
    }
}

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

async fn production_root_job(
    database: &ErabiDatabase,
) -> Result<(JobId, CrawlRunId), Box<dyn std::error::Error>> {
    let crawler_repository = CrawlerRepository::new(database);
    let crawler = Crawler::new("Production recovery action fixture");
    crawler_repository.create(&crawler).await?;
    let draft = crawler_repository
        .create_draft(crawler.id(), "operator", "2026-08-23T00:00:00Z")
        .await?;
    let mut draft = crawler_repository
        .version(crawler.id(), draft.id())
        .await?
        .version;
    draft.add_seed(Seed::new(
        "https://example.test/listing".parse()?,
        "https://example.test/listing".parse()?,
    ))?;
    crawler_repository
        .save_draft(&draft, "operator", "2026-08-23T00:00:01Z")
        .await?;
    let published = crawler_repository
        .publish(crawler.id(), draft.id(), "operator", "2026-08-23T00:00:02Z")
        .await?;
    let accepted = ProductionRunSubmissionService::new(database.clone())
        .submit(
            ProductionRunSubmissionRequest {
                crawler_id: crawler.id(),
                crawler_version_id: published.version.id(),
                selected_seed_ids: None,
                settings: SnapshotOperationalSettings {
                    max_pages: resolved(100),
                    max_depth: resolved(3),
                    max_duration_seconds: resolved(60),
                    concurrency: resolved(1),
                    request_delay_ms: resolved(250),
                    timeout_ms: resolved(30_000),
                    screenshot: resolved(false),
                    asset_download_limit_bytes: resolved(1_000_000),
                    retain_artifacts: resolved(true),
                    user_agent: resolved("Erabi/0.1".into()),
                },
                robots: RobotsAudit::respect(
                    "operator",
                    "2026-08-23T00:00:03Z",
                    "https://example.test",
                    "Erabi/0.1",
                    Some(published.version.id()),
                ),
                actor: "operator".into(),
                created_at: "2026-08-23T00:00:03Z".into(),
                priority: 0,
            },
            0,
        )
        .await?;
    Ok((accepted.job_id.parse()?, accepted.run_id))
}

async fn cancel_active(
    database: &ErabiDatabase,
    job: &NewJob,
    checkpoint: Option<CheckpointEnvelope>,
) -> Result<(), Box<dyn std::error::Error>> {
    cancel_active_by_id(database, &job.id, checkpoint).await
}

async fn cancel_active_by_id(
    database: &ErabiDatabase,
    job_id: &JobId,
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
            .append_checkpoint(job_id, &acquired.attempt.id, &lease, &checkpoint, 1)
            .await?;
    }
    repository.cancel(job_id, &lease, 2).await?;
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
    Ok(
        CrawlRecoveryCheckpoint::new(run_id, snapshot, CrawlRecoveryPhase::Traversing)?
            .to_envelope()?,
    )
}

async fn initialize_recovery_work(
    database: &ErabiDatabase,
    run_id: CrawlRunId,
    states: Vec<CrawlUrlStateRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    CrawlTraversalRepository::new(database)
        .initialize_run_state(
            run_id,
            &states,
            &CrawlTraversalControl {
                crawl_run_id: run_id,
                consumed_bytes: 0,
                raw_link_count: 0,
                duplicate_count: 0,
                robots_excluded_count: 0,
                provider_error_count: 0,
                external_url_count: 0,
                blocked_url_count: 0,
                peak_expansion_count: 0,
                elapsed_millis: 0,
                time_budget_hit: false,
                duration_work_not_expanded: false,
                pagination_truncation_count: 0,
                next_admission_sequence: states.len() as u64,
            },
        )
        .await?;
    Ok(())
}

fn recovery_work_state(
    run_id: CrawlRunId,
    canonical_url: &str,
    state: CrawlWorkState,
    order: u64,
) -> CrawlUrlStateRecord {
    let digest = erabi_domain::canonical_sha256(&format!("{run_id}:{canonical_url}"))
        .unwrap_or_else(|_| unreachable!("test URL identity is hashable"));
    CrawlUrlStateRecord {
        id: format!("crawl:{digest}"),
        crawl_run_id: run_id,
        canonical_url: canonical_url.to_owned(),
        first_discovered_url_id: None,
        requested_url: canonical_url.to_owned(),
        parent_url_state_id: None,
        parent_discovered_url_id: None,
        admission_state: CrawlAdmissionState::Admitted,
        preserve_reason: None,
        resolved_to_url_state_id: None,
        admission_sequence: Some(order),
        depth: Some(0),
        target_page_type_id: None,
        transition_id: None,
        pagination: false,
        final_canonical_url: None,
        current_work_state: Some(state),
        work_generation: 0,
        current_execution_id: None,
        seed_provenance: Vec::new(),
        seen: true,
        sampled: false,
        expanded: false,
        in_scope: false,
        page_type_match_state: None,
    }
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
    initialize_recovery_work(
        &database,
        run_id,
        vec![recovery_work_state(
            run_id,
            "https://example.test/item",
            CrawlWorkState::Pending,
            0,
        )],
    )
    .await?;
    cancel_active(
        &database,
        &job,
        Some(compatible_checkpoint(run_id, &snapshot)?),
    )
    .await?;
    let repository = JobRepository::new(&database);

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
async fn production_recovery_actions_fail_closed_without_a_frontier_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (job_id, run_id) = production_root_job(&database).await?;
    let root = JobRepository::new(&database).job(&job_id).await?;
    assert_eq!(root.kind.as_str(), "PRODUCTION_CRAWL");
    assert_eq!(root.crawl_run_id, Some(run_id.to_string()));
    assert_eq!(root.max_attempts, PRODUCTION_ROOT_MAX_ATTEMPTS);
    assert_eq!(
        CrawlRunRepository::new(&database)
            .snapshot(run_id)
            .await?
            .run_type(),
        CrawlRunType::ProductionRun
    );
    cancel_active_by_id(&database, &job_id, None).await?;
    let service = JobActionService::new(database.clone(), CancellationController::default());

    assert!(matches!(
        service.retry(&job_id, 3).await,
        Err(JobActionError::CheckpointMissing)
    ));
    assert!(matches!(
        service.retry_failed_parts(&job_id, 3).await,
        Err(JobActionError::CheckpointMissing)
    ));
    assert!(matches!(
        service.resume(&job_id, 3).await,
        Err(JobActionError::CheckpointMissing)
    ));
    assert!(matches!(
        service.restart_from_beginning(&job_id, 3).await,
        Err(JobActionError::IllegalLifecycleState)
    ));
    let rerun = service
        .rerun_full_crawl(&job_id, 3, RerunFullCrawlInput::default())
        .await?;
    assert_ne!(rerun.crawl_run_id, Some(run_id.to_string()));
    assert_eq!(
        JobRepository::new(&database).attempts(&job_id).await?.len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn stale_single_attempt_production_root_is_terminal_and_never_requeues()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (job_id, run_id) = production_root_job(&database).await?;
    assert_eq!(
        JobRepository::new(&database)
            .job(&job_id)
            .await?
            .max_attempts,
        PRODUCTION_ROOT_MAX_ATTEMPTS
    );
    CrawlExecutionRepository::new(&database)
        .persist(&CrawlExecutionRecord {
            id: CrawlExecutionId::new(),
            crawl_run_id: run_id,
            requested_url: "https://example.test/listing/already-persisted".to_owned(),
            canonical_url: "https://example.test/listing/already-persisted".to_owned(),
            observed_final_url: Some("https://example.test/listing/already-persisted".to_owned()),
            source_id: None,
            page_type_id: None,
            transition_id: None,
            discovered_url_id: None,
            outcome: CrawlExecutionOutcome::Completed,
            error_code: None,
            http_status: Some(200),
            media_type: Some("text/html".to_owned()),
            content_length_bytes: Some(1),
            provider_elapsed_ms: Some(1),
            artifacts: Vec::new(),
        })
        .await?;
    let repository = JobRepository::new(&database);
    let acquired = repository
        .acquire_next("production-stale-worker", 0, 2)
        .await?
        .ok_or("Production root was not acquired")?;
    assert_eq!(acquired.job.id, job_id);

    let recovery = repository.recover_stale_jobs(2).await?;
    assert_eq!(recovery.requeued, 0);
    assert_eq!(recovery.failed, 1);
    let recovered = repository.job(&job_id).await?;
    assert_eq!(recovered.state, JobState::Failed);
    assert_eq!(recovered.current_attempt, 1);
    assert_eq!(recovered.max_attempts, 1);
    assert_eq!(
        CrawlRunRepository::new(&database).status(run_id).await?,
        CrawlRunStatus::Failed
    );
    let progress = ProgressRepository::new(&database)
        .replay(&job_id, ProgressReplayRequest::new(None, 16)?)
        .await?;
    assert!(
        progress
            .events
            .iter()
            .any(|event| { event.terminal == Some(ProgressTerminalState::Failed) })
    );
    assert_eq!(
        CrawlExecutionRepository::new(&database)
            .list_for_run(run_id)
            .await?
            .len(),
        1
    );
    assert!(
        repository
            .acquire_next("production-reentry-worker", 3, 2)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn stale_running_job_reconciles_each_terminal_crawl_run_status_idempotently()
-> Result<(), Box<dyn std::error::Error>> {
    let cases = [
        (
            CrawlRunStatus::Succeeded,
            JobState::Succeeded,
            ProgressTerminalState::Succeeded,
        ),
        (
            CrawlRunStatus::PartialResult,
            JobState::Succeeded,
            ProgressTerminalState::Succeeded,
        ),
        (
            CrawlRunStatus::Failed,
            JobState::Failed,
            ProgressTerminalState::Failed,
        ),
        (
            CrawlRunStatus::Cancelled,
            JobState::Cancelled,
            ProgressTerminalState::Cancelled,
        ),
    ];
    for (terminal_run_status, expected_job_state, expected_terminal) in cases {
        let database = database().await?;
        let (job_id, run_id) = production_root_job(&database).await?;
        let repository = JobRepository::new(&database);
        let acquired = repository
            .acquire_next("terminal-reconciliation-worker", 0, 2)
            .await?
            .ok_or("terminal reconciliation job was not acquired")?;
        CrawlExecutionRepository::new(&database)
            .finalize(
                &CrawlExecutionSummary {
                    crawl_run_id: run_id,
                    in_scope_pages_planned: 0,
                    in_scope_pages_completed: 0,
                    pagination_truncation_count: 0,
                    unresolved_partial_work_count: 0,
                    page_type_ambiguity_count: 0,
                },
                terminal_run_status,
            )
            .await?;
        assert_eq!(
            CrawlRunRepository::new(&database).status(run_id).await?,
            terminal_run_status
        );

        let first = repository.recover_stale_jobs(2).await?;
        assert_eq!(first.requeued, 0);
        assert_eq!(repository.job(&job_id).await?.state, expected_job_state);
        assert_eq!(
            CrawlRunRepository::new(&database).status(run_id).await?,
            terminal_run_status
        );
        let progress = ProgressRepository::new(&database)
            .replay(&job_id, ProgressReplayRequest::new(None, 32)?)
            .await?;
        assert_eq!(
            progress
                .events
                .iter()
                .filter(|event| event.terminal == Some(expected_terminal))
                .count(),
            1
        );

        let second = repository.recover_stale_jobs(3).await?;
        assert_eq!(second.requeued, 0);
        assert_eq!(second.failed, 0);
        let progress_after_replay = ProgressRepository::new(&database)
            .replay(&job_id, ProgressReplayRequest::new(None, 32)?)
            .await?;
        assert_eq!(
            progress_after_replay
                .events
                .iter()
                .filter(|event| event.terminal == Some(expected_terminal))
                .count(),
            1
        );
        let _ = acquired;
    }
    Ok(())
}

#[tokio::test]
async fn cancel_terminal_crawl_run_commits_job_and_attempt_reconciliation()
-> Result<(), Box<dyn std::error::Error>> {
    let cases = [
        (
            CrawlRunStatus::Succeeded,
            JobState::Succeeded,
            AttemptOutcome::Succeeded,
        ),
        (
            CrawlRunStatus::Failed,
            JobState::Failed,
            AttemptOutcome::Failed,
        ),
    ];
    for (terminal_run_status, expected_job_state, expected_attempt_outcome) in cases {
        let database = database().await?;
        let (job, run_id, _) = run_backed_job(&database, 1).await?;
        let repository = JobRepository::new(&database);
        let acquired = repository
            .acquire_next("terminal-cancel-worker", 0, 30)
            .await?
            .ok_or("terminal cancellation job was not acquired")?;
        let lease = acquired
            .job
            .lease
            .clone()
            .ok_or("terminal cancellation lease missing")?;
        CrawlExecutionRepository::new(&database)
            .finalize(
                &CrawlExecutionSummary {
                    crawl_run_id: run_id,
                    in_scope_pages_planned: 0,
                    in_scope_pages_completed: 0,
                    pagination_truncation_count: 0,
                    unresolved_partial_work_count: 0,
                    page_type_ambiguity_count: 0,
                },
                terminal_run_status,
            )
            .await?;

        repository.cancel(&job.id, &lease, 1).await?;

        assert_eq!(repository.job(&job.id).await?.state, expected_job_state);
        let attempts = repository.attempts(&job.id).await?;
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].outcome, expected_attempt_outcome);
        assert_eq!(attempts[0].finished_at, Some(1));
    }
    Ok(())
}

#[tokio::test]
async fn db_derived_terminal_progress_repair_is_idempotent_and_does_not_execute_work()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (job, run_id, _) = run_backed_job(&database, 1).await?;
    let repository = JobRepository::new(&database);
    let acquired = repository
        .acquire_next("terminal-repair-worker", 0, 30)
        .await?
        .ok_or("terminal repair job was not acquired")?;
    let lease = acquired.job.lease.clone().ok_or("lease missing")?;

    CrawlExecutionRepository::new(&database)
        .finalize(
            &CrawlExecutionSummary {
                crawl_run_id: run_id,
                in_scope_pages_planned: 0,
                in_scope_pages_completed: 0,
                pagination_truncation_count: 0,
                unresolved_partial_work_count: 0,
                page_type_ambiguity_count: 0,
            },
            CrawlRunStatus::Succeeded,
        )
        .await?;
    let reconciliation = repository
        .reconcile_terminal_crawl_run(&job.id, &lease, &run_id.to_string(), 1)
        .await?;
    assert_eq!(reconciliation.state, JobState::Succeeded);
    assert!(
        ProgressRepository::new(&database)
            .replay(&job.id, ProgressReplayRequest::new(None, 32)?)
            .await?
            .events
            .iter()
            .all(|event| event.terminal.is_none())
    );
    assert!(
        CrawlExecutionRepository::new(&database)
            .list_for_run(run_id)
            .await?
            .is_empty()
    );

    repository.reconcile_terminal_crawl_runs(2).await?;
    repository.reconcile_terminal_crawl_runs(3).await?;
    let progress = ProgressRepository::new(&database)
        .replay(&job.id, ProgressReplayRequest::new(None, 32)?)
        .await?;
    assert_eq!(
        progress
            .events
            .iter()
            .filter(|event| event.terminal == Some(ProgressTerminalState::Succeeded))
            .count(),
        1
    );
    assert_eq!(repository.job(&job.id).await?.state, JobState::Succeeded);
    Ok(())
}

#[tokio::test]
async fn terminal_progress_contradiction_is_an_invariant_after_business_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (job, run_id, _) = run_backed_job(&database, 1).await?;
    let repository = JobRepository::new(&database);
    let acquired = repository
        .acquire_next("terminal-contradiction-worker", 0, 30)
        .await?
        .ok_or("terminal contradiction job was not acquired")?;
    let lease = acquired.job.lease.clone().ok_or("lease missing")?;

    CrawlExecutionRepository::new(&database)
        .finalize(
            &CrawlExecutionSummary {
                crawl_run_id: run_id,
                in_scope_pages_planned: 0,
                in_scope_pages_completed: 0,
                pagination_truncation_count: 0,
                unresolved_partial_work_count: 0,
                page_type_ambiguity_count: 0,
            },
            CrawlRunStatus::Succeeded,
        )
        .await?;
    ProgressRepository::new(&database)
        .append_at(
            &NewProgressEvent::terminal(
                job.id.clone(),
                ProgressTerminalState::Failed,
                ProgressMetadata::default(),
            )?,
            1,
        )
        .await?;

    repository
        .reconcile_terminal_crawl_run(&job.id, &lease, &run_id.to_string(), 2)
        .await?;
    assert_eq!(repository.job(&job.id).await?.state, JobState::Succeeded);
    assert!(matches!(
        repository
            .append_terminal_progress_if_missing(&job.id, ProgressTerminalState::Succeeded, 3)
            .await,
        Err(erabi_db::repositories::JobRepositoryError::QueueInvariant)
    ));
    assert_eq!(
        ProgressRepository::new(&database)
            .replay(&job.id, ProgressReplayRequest::new(None, 32)?)
            .await?
            .events
            .iter()
            .filter(|event| event.terminal.is_some())
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn terminal_production_system_failure_fails_run_from_queued_or_running()
-> Result<(), Box<dyn std::error::Error>> {
    for mark_running in [false, true] {
        let database = database().await?;
        let (job_id, run_id) = production_root_job(&database).await?;
        let runtime = JobRuntime::with_storage_pressure_monitor(
            &database,
            "production-system-failure",
            WorkerPolicy::conservative(),
            CancellationController::default(),
            StoragePressureMonitor::new(
                HealthyStorageProbe,
                "production-system-failure-data",
                StoragePressurePolicy::default(),
            ),
        )?;
        let turn = runtime
            .execute_next_at(
                &SystemFailureHandler {
                    database: database.clone(),
                    run_id,
                    mark_running,
                },
                0,
            )
            .await?;
        assert_eq!(
            turn,
            WorkerTurn::Failed {
                job_id: job_id.clone(),
                failure: JobFailureCode::HandlerFailed,
                diagnostics: None,
            }
        );
        assert_eq!(
            JobRepository::new(&database).job(&job_id).await?.state,
            JobState::Failed
        );
        assert_eq!(
            CrawlRunRepository::new(&database).status(run_id).await?,
            CrawlRunStatus::Failed
        );
        let progress = ProgressRepository::new(&database)
            .replay(&job_id, ProgressReplayRequest::new(None, 16)?)
            .await?;
        assert!(progress.events.iter().any(|event| {
            event.terminal == Some(ProgressTerminalState::Failed)
                && event.key.as_str() == "COMPLETED"
        }));
        assert_eq!(
            JobRepository::new(&database).attempts(&job_id).await?.len(),
            1
        );
        assert!(
            JobRepository::new(&database)
                .acquire_next("production-no-retry", 1, 2)
                .await?
                .is_none()
        );
    }
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
    initialize_recovery_work(
        &database,
        run_id,
        vec![recovery_work_state(
            run_id,
            "https://example.test/item",
            CrawlWorkState::Pending,
            0,
        )],
    )
    .await?;
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
    let (job, run_id, snapshot) = run_backed_job(&database, 3).await?;
    initialize_recovery_work(
        &database,
        run_id,
        vec![recovery_work_state(
            run_id,
            "https://example.test/item",
            CrawlWorkState::Pending,
            0,
        )],
    )
    .await?;
    let mut wrong = compatible_checkpoint(run_id, &snapshot)?;
    wrong.identity.snapshot_id = "other-run".to_owned();
    cancel_active(&database, &job, Some(wrong)).await?;
    let service = JobActionService::new(database, CancellationController::default());
    assert!(matches!(
        service.resume(&job.id, 3).await,
        Err(JobActionError::CheckpointIdentityMismatch)
    ));
    Ok(())
}

#[tokio::test]
async fn resume_rejects_unsupported_crawl_recovery_format() -> Result<(), Box<dyn std::error::Error>>
{
    let database = database().await?;
    let (job, run_id, snapshot) = run_backed_job(&database, 3).await?;
    initialize_recovery_work(
        &database,
        run_id,
        vec![recovery_work_state(
            run_id,
            "https://example.test/item",
            CrawlWorkState::Pending,
            0,
        )],
    )
    .await?;
    let mut unsupported = compatible_checkpoint(run_id, &snapshot)?;
    unsupported.payload["format_version"] = serde_json::json!(2);
    cancel_active(&database, &job, Some(unsupported)).await?;
    let service = JobActionService::new(database, CancellationController::default());
    assert!(matches!(
        service.resume(&job.id, 3).await,
        Err(JobActionError::CheckpointFormatUnsupported)
    ));
    Ok(())
}

#[tokio::test]
async fn resume_rejects_malformed_crawl_recovery_payload() -> Result<(), Box<dyn std::error::Error>>
{
    let database = database().await?;
    let (job, run_id, snapshot) = run_backed_job(&database, 3).await?;
    initialize_recovery_work(
        &database,
        run_id,
        vec![recovery_work_state(
            run_id,
            "https://example.test/item",
            CrawlWorkState::Pending,
            0,
        )],
    )
    .await?;
    let mut malformed = compatible_checkpoint(run_id, &snapshot)?;
    malformed.payload["recovery_phase"] = serde_json::json!(42);
    cancel_active(&database, &job, Some(malformed)).await?;
    let service = JobActionService::new(database, CancellationController::default());
    assert!(matches!(
        service.resume(&job.id, 3).await,
        Err(JobActionError::CheckpointMalformed)
    ));
    Ok(())
}

#[tokio::test]
async fn retry_rejects_invalid_recovery_without_enqueuing_a_continuation()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (job, run_id, snapshot) = run_backed_job(&database, 3).await?;
    initialize_recovery_work(
        &database,
        run_id,
        vec![recovery_work_state(
            run_id,
            "https://example.test/item",
            CrawlWorkState::Pending,
            0,
        )],
    )
    .await?;
    let mut unsupported = compatible_checkpoint(run_id, &snapshot)?;
    unsupported.payload["format_version"] = serde_json::json!(2);
    cancel_active(&database, &job, Some(unsupported)).await?;

    let repository = JobRepository::new(&database);
    let attempts_before = repository.attempts(&job.id).await?;
    let checkpoints_before = repository.checkpoints(&job.id).await?;
    let service = JobActionService::new(database.clone(), CancellationController::default());
    assert!(matches!(
        service.retry(&job.id, 3).await,
        Err(JobActionError::CheckpointFormatUnsupported)
    ));
    assert_eq!(repository.attempts(&job.id).await?, attempts_before);
    assert_eq!(repository.checkpoints(&job.id).await?, checkpoints_before);
    assert!(
        repository
            .acquire_next("retry-invalid-follow-up", 3, 30)
            .await?
            .is_none()
    );
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
    initialize_recovery_work(
        &database,
        run_id,
        vec![
            recovery_work_state(
                run_id,
                "https://example.test/success-1",
                CrawlWorkState::Completed,
                0,
            ),
            recovery_work_state(
                run_id,
                "https://example.test/failed-1",
                CrawlWorkState::Failed,
                1,
            ),
            recovery_work_state(
                run_id,
                "https://example.test/partial-1",
                CrawlWorkState::Partial,
                2,
            ),
        ],
    )
    .await?;
    cancel_active(
        &database,
        &job,
        Some(compatible_checkpoint(run_id, &snapshot)?),
    )
    .await?;
    let service = JobActionService::new(database.clone(), CancellationController::default());
    let result = service.retry_failed_parts(&job.id, 3).await?;
    assert_eq!(result.failed_part_count, Some(2));
    assert_eq!(result.crawl_run_id, job.crawl_run_id);
    let records = JobRepository::new(&database).checkpoints(&job.id).await?;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].checkpoint.payload_kind.as_str(),
        "CRAWL_RECOVERY"
    );
    assert!(
        !records[0]
            .checkpoint
            .payload
            .to_string()
            .contains("failed_units")
    );
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
    initialize_recovery_work(
        &database,
        run_id,
        vec![recovery_work_state(
            run_id,
            "https://example.test/item",
            CrawlWorkState::Pending,
            0,
        )],
    )
    .await?;
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

    let second_retry = service.retry(&source.id, 5).await?;
    let second_retry_record = repository.job(&second_retry.job_id).await?;
    assert_eq!(second_retry_record.parent_job_id, Some(source.id.clone()));
    assert_eq!(second_retry_record.max_attempts, 2);
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
    initialize_recovery_work(
        &database,
        run_id,
        vec![recovery_work_state(
            run_id,
            "https://example.test/item",
            CrawlWorkState::Pending,
            0,
        )],
    )
    .await?;
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
fn checkpoint_errors_have_stable_precise_action_classification() {
    assert_eq!(
        erabi_jobs::JobActionError::CheckpointMalformed.to_string(),
        "the durable checkpoint is malformed"
    );
    assert_eq!(
        erabi_jobs::JobActionError::RecoveryStateInvalid.to_string(),
        "the durable crawl recovery state is invalid"
    );
}
