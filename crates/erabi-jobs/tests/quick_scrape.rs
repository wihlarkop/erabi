use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use erabi_crawler::{
    ContentEvidence, ContentProbeDecision, ContentProbeExecutor, CrawlerAdapter,
    CrawlerAdapterError, CrawlerArtifactEvidence, CrawlerCapabilities, CrawlerExecuteRequest,
    CrawlerExecuteResult, CrawlerFuture, CrawlerHealth, CrawlerHealthStatus, CrawlerMediaType,
    CrawlerResponseMetadata, DirectFileKind, NetworkTargetPolicy, PacingService,
    QuickScrapeSubmissionRequest, QuickScrapeSubmissionService, RetryAfterTiming,
    RobotsHttpResponse, RobotsPolicyService, RobotsTransport, StaticNetworkResolver,
    ValidatedNetworkTarget,
};
use erabi_db::{
    ArtifactStore, ErabiDatabase, MigrationRunner,
    repositories::{CrawlExecutionRepository, CrawlRunRepository, JobRepository},
};
use erabi_domain::{
    CrawlExecutionErrorCode, CrawlExecutionOutcome, ResolvedValue, RobotsAudit, SettingSource,
    SnapshotOperationalSettings,
};
use erabi_jobs::{
    CancellationController, JobRuntime, QuickScrapeJobHandler, StoragePressureMonitor,
    StoragePressurePolicy, StorageProbe, StorageProbeError, WorkerPolicy, WorkerTurn,
};

#[derive(Clone)]
struct FixedProbe(ContentProbeDecision);

impl ContentProbeExecutor for FixedProbe {
    fn probe<'probe>(
        &'probe self,
        _target: &'probe ValidatedNetworkTarget,
    ) -> erabi_crawler::ContentProbeFuture<'probe> {
        let decision = self.0.clone();
        Box::pin(async move { decision })
    }
}

#[derive(Clone, Copy)]
enum AdapterMode {
    Complete,
    Unavailable,
    AccessDenied,
    Cancelled,
}

struct FixtureAdapter {
    mode: AdapterMode,
    calls: Arc<AtomicUsize>,
}

impl CrawlerAdapter for FixtureAdapter {
    fn health(&self) -> CrawlerFuture<'_, CrawlerHealth> {
        Box::pin(async {
            Ok(CrawlerHealth::new(
                CrawlerHealthStatus::Healthy,
                None,
                CrawlerCapabilities {
                    rendered_html: true,
                    cleaned_html: true,
                    markdown: true,
                    screenshot: true,
                    wait_for_selector: false,
                    bounded_auto_scroll: false,
                    discovered_links: false,
                },
            ))
        })
    }

    fn execute(&self, request: CrawlerExecuteRequest) -> CrawlerFuture<'_, CrawlerExecuteResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mode = self.mode;
        Box::pin(async move {
            match mode {
                AdapterMode::Unavailable => Err(CrawlerAdapterError::Unavailable),
                AdapterMode::AccessDenied => Err(CrawlerAdapterError::AccessDenied),
                AdapterMode::Cancelled => Err(CrawlerAdapterError::Cancelled),
                AdapterMode::Complete => CrawlerExecuteResult::try_new(
                    &request,
                    erabi_crawler::PageObservation {
                        requested_url: request.target_url().to_string(),
                        final_url: Some(request.target_url().to_string()),
                        artifact_ids: Vec::new(),
                        discovered_links: Vec::new(),
                        selector_observations: Vec::new(),
                        pagination_observations: Vec::new(),
                    },
                    CrawlerResponseMetadata::try_new(
                        Some(200),
                        Some(
                            CrawlerMediaType::new("text/html")
                                .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?,
                        ),
                        Some(42),
                        Some(10),
                    )?,
                    vec![
                        CrawlerArtifactEvidence::cleaned_html("<main>clean</main>")?,
                        CrawlerArtifactEvidence::rendered_html("<main>rendered</main>")?,
                        CrawlerArtifactEvidence::markdown("# page")?,
                    ],
                    false,
                ),
            }
        })
    }
}

struct AllowRobots;

impl RobotsTransport for AllowRobots {
    fn fetch<'transport>(
        &'transport self,
        _target: &'transport ValidatedNetworkTarget,
        _user_agent: &'transport str,
    ) -> erabi_crawler::RobotsFetchFuture<'transport> {
        Box::pin(async {
            Ok(RobotsHttpResponse::new(
                404,
                Vec::new(),
                RetryAfterTiming::Absent,
            ))
        })
    }
}

#[derive(Clone, Copy)]
struct HealthyStorageProbe;

impl StorageProbe for HealthyStorageProbe {
    fn free_bytes(&self, _path: &Path) -> Result<u64, StorageProbeError> {
        Ok(u64::MAX)
    }
}

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

fn policy() -> NetworkTargetPolicy {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)), 443);
    NetworkTargetPolicy::new(Arc::new(StaticNetworkResolver::single(
        "example.test",
        address,
    )))
}

fn settings() -> SnapshotOperationalSettings {
    fn resolved<T>(value: T) -> ResolvedValue<T> {
        ResolvedValue {
            value,
            source: SettingSource::BuiltInDefault,
        }
    }
    SnapshotOperationalSettings {
        max_pages: resolved(1),
        max_depth: resolved(0),
        max_duration_seconds: resolved(60),
        concurrency: resolved(1),
        request_delay_ms: resolved(0),
        timeout_ms: resolved(1_000),
        screenshot: resolved(false),
        asset_download_limit_bytes: resolved(1_000_000),
        retain_artifacts: resolved(true),
        user_agent: resolved("Erabi/0.1".to_owned()),
    }
}

fn submission_request(target_url: &str, max_attempts: u32) -> QuickScrapeSubmissionRequest {
    QuickScrapeSubmissionRequest {
        target_url: target_url.to_owned(),
        collection_id: None,
        source_name: None,
        settings: settings(),
        robots: RobotsAudit::respect(
            "operator",
            "unix:100",
            "https://example.test:443",
            "Erabi/0.1",
            None,
        ),
        actor: "operator".to_owned(),
        created_at: "unix:100".to_owned(),
        priority: 0,
        max_attempts,
    }
}

async fn submit(
    database: &ErabiDatabase,
    probe: ContentProbeDecision,
    max_attempts: u32,
) -> Result<erabi_crawler::QuickScrapeSubmission, Box<dyn std::error::Error>> {
    let service = QuickScrapeSubmissionService::new(database.clone(), policy())
        .with_probe_executor(Arc::new(FixedProbe(probe)));
    Ok(service
        .submit(
            submission_request("https://example.test/page", max_attempts),
            100,
        )
        .await?)
}

fn handler(
    database: ErabiDatabase,
    adapter: Arc<dyn CrawlerAdapter>,
    artifact_store: ArtifactStore,
) -> QuickScrapeJobHandler {
    let pacing = PacingService::new();
    QuickScrapeJobHandler::new(
        database,
        adapter,
        RobotsPolicyService::with_transport(policy(), pacing.clone(), Arc::new(AllowRobots)),
        pacing,
        policy(),
        artifact_store,
    )
}

fn runtime<'database>(
    database: &'database ErabiDatabase,
    worker_id: &str,
) -> Result<JobRuntime<'database>, Box<dyn std::error::Error>> {
    Ok(JobRuntime::with_storage_pressure_monitor(
        database,
        worker_id,
        WorkerPolicy::conservative(),
        CancellationController::default(),
        StoragePressureMonitor::new(
            HealthyStorageProbe,
            "quick-scrape-test-data",
            StoragePressurePolicy::default(),
        ),
    )?)
}

#[tokio::test]
async fn normal_execution_persists_provider_evidence_through_the_adapter()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let accepted = submit(&database, ContentProbeDecision::NormalWebCrawl, 2).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let temporary = tempfile::tempdir()?;
    let handler = handler(
        database.clone(),
        Arc::new(FixtureAdapter {
            mode: AdapterMode::Complete,
            calls: Arc::clone(&calls),
        }),
        ArtifactStore::new(temporary.path())?,
    );
    let runtime = runtime(&database, "quick-scrape-test")?;

    let job_id: erabi_db::repositories::JobId = accepted.job_id.parse()?;
    let jobs = JobRepository::new(&database);
    let before = jobs.job(&job_id).await?;
    let turn = runtime.execute_next_at(&handler, 100).await?;
    let after = jobs.job(&job_id).await?;
    assert!(
        matches!(turn, WorkerTurn::Succeeded { .. }),
        "unexpected worker turn: {turn:?}; job before: {before:?}; job after: {after:?}; adapter calls: {}",
        calls.load(Ordering::SeqCst),
    );
    let records = CrawlExecutionRepository::new(&database)
        .list_for_run(accepted.run_id)
        .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].outcome, CrawlExecutionOutcome::Completed);
    assert_eq!(records[0].artifacts.len(), 3);
    assert_eq!(
        records[0].observed_final_url.as_deref(),
        Some("https://example.test/page")
    );
    Ok(())
}

#[tokio::test]
async fn confident_file_asset_completes_history_without_html_provider_execution()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let accepted = submit(
        &database,
        ContentProbeDecision::FileAsset {
            kind: DirectFileKind::Pdf,
            media_type: Some("application/pdf".to_owned()),
            evidence: ContentEvidence::ContentType,
        },
        2,
    )
    .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let temporary = tempfile::tempdir()?;
    let handler = handler(
        database.clone(),
        Arc::new(FixtureAdapter {
            mode: AdapterMode::Complete,
            calls: Arc::clone(&calls),
        }),
        ArtifactStore::new(temporary.path())?,
    );
    let runtime = runtime(&database, "quick-file-test")?;

    let job_id: erabi_db::repositories::JobId = accepted.job_id.parse()?;
    let jobs = JobRepository::new(&database);
    let before = jobs.job(&job_id).await?;
    let turn = runtime.execute_next_at(&handler, 100).await?;
    let after = jobs.job(&job_id).await?;
    assert!(
        matches!(turn, WorkerTurn::Succeeded { .. }),
        "unexpected worker turn: {turn:?}; job before: {before:?}; job after: {after:?}; adapter calls: {}",
        calls.load(Ordering::SeqCst),
    );
    let records = CrawlExecutionRepository::new(&database)
        .list_for_run(accepted.run_id)
        .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(records[0].outcome, CrawlExecutionOutcome::Completed);
    assert_eq!(records[0].artifacts.len(), 0);
    assert_eq!(records[0].media_type.as_deref(), Some("application/pdf"));
    Ok(())
}

#[tokio::test]
async fn provider_unavailability_retries_same_run_then_preserves_terminal_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let accepted = submit(&database, ContentProbeDecision::NormalWebCrawl, 2).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let temporary = tempfile::tempdir()?;
    let handler = handler(
        database.clone(),
        Arc::new(FixtureAdapter {
            mode: AdapterMode::Unavailable,
            calls: Arc::clone(&calls),
        }),
        ArtifactStore::new(temporary.path())?,
    );
    let runtime = runtime(&database, "quick-retry-test")?;

    let job_id: erabi_db::repositories::JobId = accepted.job_id.parse()?;
    let jobs = JobRepository::new(&database);
    let before_first = jobs.job(&job_id).await?;
    let first_turn = runtime.execute_next_at(&handler, 100).await?;
    let after_first = jobs.job(&job_id).await?;
    assert!(
        matches!(first_turn, WorkerTurn::RetryScheduled { .. }),
        "unexpected first worker turn: {first_turn:?}; job before: {before_first:?}; job after: {after_first:?}; adapter calls: {}",
        calls.load(Ordering::SeqCst),
    );
    let second_turn = runtime.execute_next_at(&handler, 105).await?;
    let job = jobs.job(&job_id).await?;
    assert!(
        matches!(second_turn, WorkerTurn::Failed { .. }),
        "unexpected second worker turn: {second_turn:?}; job after: {job:?}; adapter calls: {}",
        calls.load(Ordering::SeqCst),
    );
    let snapshot = CrawlRunRepository::new(&database)
        .snapshot(accepted.run_id)
        .await?;
    let records = CrawlExecutionRepository::new(&database)
        .list_for_run(accepted.run_id)
        .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(job.current_attempt, 2);
    assert_eq!(
        records[0].error_code,
        Some(CrawlExecutionErrorCode::ProviderUnavailable)
    );
    assert_eq!(snapshot.robots().actor(), "operator");
    Ok(())
}

#[tokio::test]
async fn permanent_provider_denial_fails_once_without_consuming_retry_budget()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let accepted = submit(&database, ContentProbeDecision::NormalWebCrawl, 3).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let temporary = tempfile::tempdir()?;
    let handler = handler(
        database.clone(),
        Arc::new(FixtureAdapter {
            mode: AdapterMode::AccessDenied,
            calls: Arc::clone(&calls),
        }),
        ArtifactStore::new(temporary.path())?,
    );
    let runtime = runtime(&database, "quick-denial-test")?;

    let job_id: erabi_db::repositories::JobId = accepted.job_id.parse()?;
    let turn = runtime.execute_next_at(&handler, 100).await?;
    let job = JobRepository::new(&database).job(&job_id).await?;
    let records = CrawlExecutionRepository::new(&database)
        .list_for_run(accepted.run_id)
        .await?;

    assert!(matches!(turn, WorkerTurn::Failed { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(job.current_attempt, 1);
    assert_eq!(job.state, erabi_db::repositories::JobState::Failed);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].outcome, CrawlExecutionOutcome::Failed);
    assert_eq!(
        records[0].error_code,
        Some(CrawlExecutionErrorCode::AccessDenied)
    );
    Ok(())
}

#[tokio::test]
async fn provider_cancellation_finishes_the_durable_job_as_cancelled_without_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let accepted = submit(&database, ContentProbeDecision::NormalWebCrawl, 3).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let temporary = tempfile::tempdir()?;
    let handler = handler(
        database.clone(),
        Arc::new(FixtureAdapter {
            mode: AdapterMode::Cancelled,
            calls: Arc::clone(&calls),
        }),
        ArtifactStore::new(temporary.path())?,
    );
    let runtime = runtime(&database, "quick-cancelled-test")?;

    let job_id: erabi_db::repositories::JobId = accepted.job_id.parse()?;
    let turn = runtime.execute_next_at(&handler, 100).await?;
    let job = JobRepository::new(&database).job(&job_id).await?;

    assert!(matches!(turn, WorkerTurn::Cancelled { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(job.current_attempt, 1);
    assert_eq!(job.state, erabi_db::repositories::JobState::Cancelled);
    Ok(())
}
