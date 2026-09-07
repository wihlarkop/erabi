use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use erabi_crawler::{
    CrawlCheckpointV2, CrawlRecoveryPhase, CrawlerAdapter, CrawlerAdapterError,
    CrawlerArtifactEvidence, CrawlerCapabilities, CrawlerExecuteRequest, CrawlerExecuteResult,
    CrawlerFuture, CrawlerHealth, CrawlerHealthStatus, CrawlerMediaType, CrawlerResponseMetadata,
    ManualPreviewClock, NetworkTargetPolicy, ObservedLink, PacingService, PaginationObservation,
    ProductionRunSubmissionRequest, ProductionRunSubmissionService, RetryAfterTiming,
    RobotsHttpResponse, RobotsPolicyService, RobotsTransport, StaticNetworkResolver,
    ValidatedNetworkTarget,
};
use erabi_db::{
    ArtifactStore, ErabiDatabase, MigrationRunner,
    repositories::{
        ActionRunAssociation, CrawlAdmissionState, CrawlExecutionRepository,
        CrawlPageTypeMatchState, CrawlRunRepository, CrawlTraversalControl,
        CrawlTraversalRepository, CrawlUrlStateRecord, CrawlWorkState, CrawlerRepository,
        DiscoveredUrlRecord, JobKind, JobRepository,
    },
};
use erabi_domain::{
    CrawlExecutionOutcome, CrawlRunStatus, Crawler, CrawlerVersionId, DiscoveryTransition,
    PageTypeDiscoveryGuardrails, PaginationKind, ResolvedValue, RobotsAudit, Seed, SettingSource,
    SnapshotOperationalSettings, TransitionBudget, UrlMatcher,
};
use erabi_jobs::{
    CancellationController, JobActionError, JobActionService, JobRuntime,
    ProductionCrawlJobHandler, ProgressReplayRequest, ProgressRepository, StoragePressureMonitor,
    StoragePressurePolicy, StorageProbe, StorageProbeError, WorkerPolicy, WorkerTurn,
};

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

fn settings() -> SnapshotOperationalSettings {
    fn resolved<T>(value: T) -> ResolvedValue<T> {
        ResolvedValue {
            value,
            source: SettingSource::BuiltInDefault,
        }
    }
    SnapshotOperationalSettings {
        max_pages: resolved(10),
        max_depth: resolved(4),
        max_duration_seconds: resolved(60),
        concurrency: resolved(1),
        request_delay_ms: resolved(100),
        timeout_ms: resolved(30_000),
        screenshot: resolved(false),
        asset_download_limit_bytes: resolved(1_000_000),
        retain_artifacts: resolved(true),
        user_agent: resolved("Erabi/0.1".to_owned()),
    }
}

fn request(
    crawler: &Crawler,
    version_id: CrawlerVersionId,
    max_pages: u64,
    max_duration_seconds: u64,
    timeout_ms: u64,
) -> ProductionRunSubmissionRequest {
    let mut settings = settings();
    settings.max_pages.value = max_pages;
    settings.max_duration_seconds.value = max_duration_seconds;
    settings.timeout_ms.value = timeout_ms;
    ProductionRunSubmissionRequest {
        crawler_id: crawler.id(),
        crawler_version_id: version_id,
        selected_seed_ids: None,
        settings,
        robots: RobotsAudit::respect(
            "operator",
            "unix:1",
            "crawler scope",
            "Erabi/0.1",
            Some(version_id),
        ),
        actor: "operator".to_owned(),
        created_at: "unix:1".to_owned(),
        priority: 0,
    }
}

#[derive(Clone)]
struct FixturePage {
    final_url: Option<String>,
    links: Vec<ObservedLink>,
    pagination: Vec<PaginationObservation>,
    failure: Option<CrawlerAdapterError>,
    advance_clock_millis: u64,
    provider_reported_partial: bool,
}

impl FixturePage {
    fn html(links: Vec<ObservedLink>) -> Self {
        Self {
            final_url: None,
            links,
            pagination: Vec::new(),
            failure: None,
            advance_clock_millis: 0,
            provider_reported_partial: false,
        }
    }

    fn partial_html(links: Vec<ObservedLink>) -> Self {
        Self {
            provider_reported_partial: true,
            ..Self::html(links)
        }
    }
}

struct FixtureAdapter {
    pages: BTreeMap<String, FixturePage>,
    calls: Arc<Mutex<Vec<(String, Duration)>>>,
    clock: Option<Arc<ManualPreviewClock>>,
}

#[derive(Clone, Copy, Default)]
struct GraphOptions {
    hint_first_seed_as_product: bool,
    add_runtime_ambiguous_match: bool,
    add_recovery_transition: bool,
    product_page_budget: Option<u64>,
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
                    discovered_links: true,
                },
            ))
        })
    }

    fn execute(&self, request: CrawlerExecuteRequest) -> CrawlerFuture<'_, CrawlerExecuteResult> {
        let target = request.target_url().to_string();
        match self.calls.lock() {
            Ok(mut calls) => calls.push((target.clone(), request.timeout())),
            Err(poisoned) => poisoned
                .into_inner()
                .push((target.clone(), request.timeout())),
        }
        let page = self.pages.get(&target).cloned();
        let clock = self.clock.clone();
        Box::pin(async move {
            let page = page.ok_or(CrawlerAdapterError::InvalidProviderResponse)?;
            if let Some(error) = page.failure {
                return Err(error);
            }
            if let Some(clock) = clock {
                clock.advance_millis(page.advance_clock_millis);
            }
            CrawlerExecuteResult::try_new(
                &request,
                erabi_crawler::PageObservation {
                    requested_url: target.clone(),
                    final_url: page.final_url.or(Some(target)),
                    artifact_ids: Vec::new(),
                    discovered_links: page.links,
                    selector_observations: Vec::new(),
                    pagination_observations: page.pagination,
                },
                CrawlerResponseMetadata::try_new(
                    Some(200),
                    Some(
                        CrawlerMediaType::new("text/html")
                            .map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?,
                    ),
                    Some(42),
                    Some(1),
                )?,
                vec![
                    CrawlerArtifactEvidence::cleaned_html("<main>clean</main>")?,
                    CrawlerArtifactEvidence::rendered_html("<main>rendered</main>")?,
                    CrawlerArtifactEvidence::markdown("# page")?,
                ],
                page.provider_reported_partial,
            )
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

fn policy() -> NetworkTargetPolicy {
    NetworkTargetPolicy::new(Arc::new(StaticNetworkResolver::single(
        "example.test",
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)), 443),
    )))
}

fn handler(
    database: ErabiDatabase,
    adapter: Arc<dyn CrawlerAdapter>,
    artifact_store: ArtifactStore,
    clock: Option<Arc<ManualPreviewClock>>,
) -> ProductionCrawlJobHandler {
    let pacing = PacingService::new();
    let value = ProductionCrawlJobHandler::new(
        database,
        adapter,
        RobotsPolicyService::with_transport(policy(), pacing.clone(), Arc::new(AllowRobots)),
        pacing,
        policy(),
        artifact_store,
    );
    match clock {
        Some(clock) => value.with_clock(clock),
        None => value,
    }
}

fn runtime(database: &ErabiDatabase) -> Result<JobRuntime<'_>, Box<dyn std::error::Error>> {
    Ok(JobRuntime::with_storage_pressure_monitor(
        database,
        "production-handler-test",
        WorkerPolicy::conservative(),
        CancellationController::default(),
        StoragePressureMonitor::new(
            HealthyStorageProbe,
            "production-handler-test-data",
            StoragePressurePolicy::default(),
        ),
    )?)
}

#[allow(clippy::too_many_lines)]
async fn published_graph(
    database: &ErabiDatabase,
    mut seeds: Vec<Seed>,
    options: GraphOptions,
) -> Result<(Crawler, CrawlerVersionId, erabi_domain::PageTypeId), Box<dyn std::error::Error>> {
    let repository = CrawlerRepository::new(database);
    let crawler = Crawler::new("Production handler fixture");
    repository.create(&crawler).await?;
    let version = repository
        .create_draft(crawler.id(), "operator", "unix:1")
        .await?;
    let listing = repository
        .create_page_type(
            crawler.id(),
            version.id(),
            "Listing",
            10,
            "operator",
            "unix:2",
        )
        .await?;
    let product = repository
        .create_page_type(
            crawler.id(),
            version.id(),
            "Product",
            10,
            "operator",
            "unix:3",
        )
        .await?;
    repository
        .create_url_matcher(
            crawler.id(),
            version.id(),
            listing.id,
            &UrlMatcher::path_prefix(Some("example.test".to_owned()), "/listing"),
            "operator",
            "unix:4",
        )
        .await?;
    if options.add_runtime_ambiguous_match {
        let ambiguous = repository
            .create_page_type(
                crawler.id(),
                version.id(),
                "Ambiguous Product",
                10,
                "operator",
                "unix:5b",
            )
            .await?;
        repository
            .create_url_matcher(
                crawler.id(),
                version.id(),
                product.id,
                &UrlMatcher::regex(r"^https://example\.test/ambiguous/.*$")?,
                "operator",
                "unix:5c",
            )
            .await?;
        repository
            .create_url_matcher(
                crawler.id(),
                version.id(),
                ambiguous.id,
                &UrlMatcher::regex(r"^https://example\.test/ambiguous/.*$")?,
                "operator",
                "unix:5d",
            )
            .await?;
    }
    if options.hint_first_seed_as_product
        && let Some(seed) = seeds.first_mut()
    {
        seed.entry_page_type_hint = Some(product.id);
    }
    repository
        .create_url_matcher(
            crawler.id(),
            version.id(),
            product.id,
            &UrlMatcher::path_prefix(Some("example.test".to_owned()), "/product"),
            "operator",
            "unix:5",
        )
        .await?;
    let mut current = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    for seed in seeds {
        current.add_seed(seed)?;
    }
    if let Some(page_budget) = options.product_page_budget {
        let mut guardrails = current.guardrails().clone();
        guardrails.page_types.push(PageTypeDiscoveryGuardrails {
            page_type_id: product.id,
            page_budget: Some(page_budget),
            health_threshold: None,
        });
        current.set_guardrails(guardrails)?;
    }
    repository
        .save_draft(&current, "operator", "unix:6")
        .await?;
    repository
        .create_discovery_transition(
            crawler.id(),
            version.id(),
            &DiscoveryTransition {
                id: erabi_domain::DiscoveryTransitionId::new(),
                source_page_type_id: listing.id,
                target_page_type_id: product.id,
                name: "listing products".to_owned(),
                enabled: true,
                link_selector: "a.product".to_owned(),
                url_constraints: None,
                priority: 10,
                budget: TransitionBudget {
                    max_links_per_source_page: 10,
                    total_budget: Some(20),
                    depth_contribution: 1,
                },
                deduplicate: false,
                latest_test_evidence_id: None,
            },
            "operator",
            "unix:7",
        )
        .await?;
    if options.add_recovery_transition {
        repository
            .create_discovery_transition(
                crawler.id(),
                version.id(),
                &DiscoveryTransition {
                    id: erabi_domain::DiscoveryTransitionId::new(),
                    source_page_type_id: product.id,
                    target_page_type_id: product.id,
                    name: "product related products".to_owned(),
                    enabled: true,
                    link_selector: "a.product".to_owned(),
                    url_constraints: None,
                    priority: 10,
                    budget: TransitionBudget {
                        max_links_per_source_page: 10,
                        total_budget: Some(20),
                        depth_contribution: 1,
                    },
                    deduplicate: false,
                    latest_test_evidence_id: None,
                },
                "operator",
                "unix:7b",
            )
            .await?;
    }
    let published = repository
        .publish(crawler.id(), version.id(), "operator", "unix:8")
        .await?;
    Ok((crawler, published.version.id(), product.id))
}

fn seed(value: &str) -> Result<Seed, Box<dyn std::error::Error>> {
    Ok(Seed::new(value.parse()?, value.parse()?))
}

#[allow(clippy::too_many_arguments)]
async fn submit_and_execute(
    database: &ErabiDatabase,
    crawler: &Crawler,
    version_id: CrawlerVersionId,
    max_pages: u64,
    max_duration_seconds: u64,
    timeout_ms: u64,
    adapter: Arc<dyn CrawlerAdapter>,
    clock: Option<Arc<ManualPreviewClock>>,
) -> Result<erabi_crawler::ProductionRunSubmission, Box<dyn std::error::Error>> {
    let accepted = ProductionRunSubmissionService::new(database.clone())
        .submit(
            request(
                crawler,
                version_id,
                max_pages,
                max_duration_seconds,
                timeout_ms,
            ),
            100,
        )
        .await?;
    let temporary = tempfile::tempdir()?;
    let root = handler(
        database.clone(),
        adapter,
        ArtifactStore::new(temporary.path())?,
        clock,
    );
    let turn = runtime(database)?.execute_next_at(&root, 100).await?;
    assert!(matches!(turn, WorkerTurn::Succeeded { .. }), "{turn:?}");
    Ok(accepted)
}

#[tokio::test]
async fn production_handler_executes_two_pages_and_emits_durable_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, _) = published_graph(
        &database,
        vec![seed("https://example.test/listing/a")?],
        GraphOptions::default(),
    )
    .await?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([
            (
                "https://example.test/listing/a".to_owned(),
                FixturePage::html(vec![ObservedLink {
                    raw_href: "/product/b".to_owned(),
                    selector: Some("a.product".to_owned()),
                }]),
            ),
            (
                "https://example.test/product/b".to_owned(),
                FixturePage::html(Vec::new()),
            ),
        ]),
        calls: Arc::clone(&calls),
        clock: None,
    };
    let accepted = submit_and_execute(
        &database,
        &crawler,
        version_id,
        10,
        60,
        30_000,
        Arc::new(adapter),
        None,
    )
    .await?;

    let records = CrawlExecutionRepository::new(&database)
        .list_for_run(accepted.run_id)
        .await?;
    let job_id = accepted.job_id.parse()?;
    assert_eq!(
        JobRepository::new(&database)
            .job(&job_id)
            .await?
            .max_attempts,
        1
    );
    assert_eq!(records.len(), 2);
    assert!(
        records
            .iter()
            .all(|record| record.outcome == CrawlExecutionOutcome::Completed)
    );
    let call_count = match calls.lock() {
        Ok(calls) => calls.len(),
        Err(poisoned) => poisoned.into_inner().len(),
    };
    assert_eq!(call_count, 2);
    assert_eq!(
        CrawlRunRepository::new(&database)
            .status(accepted.run_id)
            .await?,
        CrawlRunStatus::Succeeded
    );
    let progress = ProgressRepository::new(&database)
        .replay(&job_id, ProgressReplayRequest::new(None, 32)?)
        .await?;
    assert!(
        progress
            .events
            .iter()
            .any(|event| event.key.as_str() == "PRODUCTION_STARTED")
    );
    assert!(
        progress
            .events
            .iter()
            .any(|event| event.key.as_str() == "PAGE_COMPLETED")
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn retry_failed_parts_dispatches_only_current_failed_and_partial_work()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, _) = published_graph(
        &database,
        vec![seed("https://example.test/listing/a")?],
        GraphOptions::default(),
    )
    .await?;
    let accepted = ProductionRunSubmissionService::new(database.clone())
        .submit(request(&crawler, version_id, 10, 60, 30_000), 100)
        .await?;
    let snapshot = CrawlRunRepository::new(&database)
        .snapshot(accepted.run_id)
        .await?;
    let version = CrawlerRepository::new(&database)
        .version(crawler.id(), version_id)
        .await?
        .version;
    let seed_id = version
        .seeds()
        .first()
        .ok_or("published recovery fixture seed missing")?
        .id;
    let state_id = |canonical_url: &str| -> Result<String, Box<dyn std::error::Error>> {
        let identity = format!("{}:{canonical_url}", accepted.run_id);
        let digest = erabi_domain::canonical_sha256(&identity)?;
        Ok(format!("crawl:{digest}"))
    };
    let state = |canonical_url: &str,
                 work_state: CrawlWorkState,
                 generation: u64,
                 order: u64|
     -> Result<CrawlUrlStateRecord, Box<dyn std::error::Error>> {
        Ok(CrawlUrlStateRecord {
            id: state_id(canonical_url)?,
            crawl_run_id: accepted.run_id,
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
            current_work_state: Some(work_state),
            work_generation: generation,
            current_execution_id: None,
            seed_provenance: vec![seed_id.to_string()],
            seen: true,
            sampled: false,
            expanded: false,
            in_scope: false,
            page_type_match_state: None,
        })
    };
    let durable_states = vec![
        state(
            "https://example.test/listing/a",
            CrawlWorkState::Completed,
            0,
            0,
        )?,
        state(
            "https://example.test/product/b",
            CrawlWorkState::Failed,
            0,
            1,
        )?,
        state(
            "https://example.test/product/c",
            CrawlWorkState::Partial,
            0,
            2,
        )?,
        state(
            "https://example.test/product/d",
            CrawlWorkState::Pending,
            0,
            3,
        )?,
    ];
    let control = CrawlTraversalControl {
        crawl_run_id: accepted.run_id,
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
        next_admission_sequence: 4,
    };
    CrawlTraversalRepository::new(&database)
        .initialize_run_state(accepted.run_id, &durable_states, &control)
        .await?;

    let root_job_id: erabi_db::repositories::JobId = accepted.job_id.parse()?;
    let jobs = JobRepository::new(&database);
    let root_acquired = jobs
        .acquire_next("failed-parts-source-worker", 100, 30)
        .await?
        .ok_or("failed-parts source job was not acquired")?;
    let root_lease = root_acquired
        .job
        .lease
        .clone()
        .ok_or("failed-parts source lease missing")?;
    let checkpoint =
        CrawlCheckpointV2::new(accepted.run_id, &snapshot, CrawlRecoveryPhase::Traversing)?
            .to_envelope()?;
    jobs.append_checkpoint(
        &root_job_id,
        &root_acquired.attempt.id,
        &root_lease,
        &checkpoint,
        1,
    )
    .await?;
    jobs.cancel(&root_job_id, &root_lease, 2).await?;
    let action = JobActionService::new(database.clone(), CancellationController::default())
        .retry_failed_parts(&root_job_id, 3)
        .await?;
    assert_eq!(action.failed_part_count, Some(2));

    let calls = Arc::new(Mutex::new(Vec::new()));
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([
            (
                "https://example.test/product/b".to_owned(),
                FixturePage::html(Vec::new()),
            ),
            (
                "https://example.test/product/c".to_owned(),
                FixturePage::html(Vec::new()),
            ),
        ]),
        calls: Arc::clone(&calls),
        clock: None,
    };
    let temporary = tempfile::tempdir()?;
    let action_handler = handler(
        database.clone(),
        Arc::new(adapter),
        ArtifactStore::new(temporary.path())?,
        None,
    );
    let turn = runtime(&database)?
        .execute_next_at(&action_handler, 3)
        .await?;
    assert!(matches!(turn, WorkerTurn::Succeeded { job_id } if job_id == action.job_id));
    let calls = match calls.lock() {
        Ok(calls) => calls.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(
        calls
            .iter()
            .map(|(url, _)| url.as_str())
            .collect::<Vec<_>>(),
        vec![
            "https://example.test/product/b",
            "https://example.test/product/c"
        ]
    );
    let durable = CrawlTraversalRepository::new(&database)
        .reconstruct_recovery_state(accepted.run_id)
        .await?;
    for (url, expected_state, expected_generation) in [
        (
            "https://example.test/listing/a",
            CrawlWorkState::Completed,
            0,
        ),
        (
            "https://example.test/product/b",
            CrawlWorkState::Completed,
            1,
        ),
        (
            "https://example.test/product/c",
            CrawlWorkState::Completed,
            1,
        ),
        ("https://example.test/product/d", CrawlWorkState::Pending, 0),
    ] {
        let work = durable
            .work
            .iter()
            .find(|work| work.canonical_url == url)
            .ok_or("recovery work state missing")?;
        assert_eq!(work.current_work_state, Some(expected_state));
        assert_eq!(work.work_generation, expected_generation);
    }
    assert_eq!(
        CrawlRunRepository::new(&database)
            .status(accepted.run_id)
            .await?,
        CrawlRunStatus::PartialResult
    );
    assert!(matches!(
        JobActionService::new(database.clone(), CancellationController::default())
            .retry_failed_parts(&root_job_id, 4)
            .await,
        Err(JobActionError::IllegalLifecycleState)
    ));
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn frontier_newer_than_checkpoint_is_recovered_from_durable_state()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, _) = published_graph(
        &database,
        vec![seed("https://example.test/listing/root")?],
        GraphOptions::default(),
    )
    .await?;
    let accepted = ProductionRunSubmissionService::new(database.clone())
        .submit(request(&crawler, version_id, 10, 60, 30_000), 100)
        .await?;
    let snapshot = CrawlRunRepository::new(&database)
        .snapshot(accepted.run_id)
        .await?;
    let version = CrawlerRepository::new(&database)
        .version(crawler.id(), version_id)
        .await?
        .version;
    let seed_id = version
        .seeds()
        .first()
        .ok_or("frontier recovery fixture seed missing")?
        .id
        .to_string();
    let root_url = "https://example.test/listing/root";
    let child_url = "https://example.test/product/new-child";
    let state_id = |canonical_url: &str| -> Result<String, Box<dyn std::error::Error>> {
        let identity = format!("{}:{canonical_url}", accepted.run_id);
        let digest = erabi_domain::canonical_sha256(&identity)?;
        Ok(format!("crawl:{digest}"))
    };
    let root_state_id = state_id(root_url)?;
    let child_state_id = state_id(child_url)?;
    let root = CrawlUrlStateRecord {
        id: root_state_id.clone(),
        crawl_run_id: accepted.run_id,
        canonical_url: root_url.to_owned(),
        first_discovered_url_id: None,
        requested_url: root_url.to_owned(),
        parent_url_state_id: None,
        parent_discovered_url_id: None,
        admission_state: CrawlAdmissionState::Admitted,
        preserve_reason: None,
        resolved_to_url_state_id: None,
        admission_sequence: Some(0),
        depth: Some(0),
        target_page_type_id: None,
        transition_id: None,
        pagination: false,
        final_canonical_url: None,
        current_work_state: Some(CrawlWorkState::Completed),
        work_generation: 0,
        current_execution_id: None,
        seed_provenance: vec![seed_id.clone()],
        seen: true,
        sampled: false,
        expanded: false,
        in_scope: false,
        page_type_match_state: None,
    };
    let child = CrawlUrlStateRecord {
        id: child_state_id,
        crawl_run_id: accepted.run_id,
        canonical_url: child_url.to_owned(),
        first_discovered_url_id: None,
        requested_url: child_url.to_owned(),
        parent_url_state_id: Some(root_state_id),
        parent_discovered_url_id: None,
        admission_state: CrawlAdmissionState::Admitted,
        preserve_reason: None,
        resolved_to_url_state_id: None,
        admission_sequence: Some(1),
        depth: Some(1),
        target_page_type_id: None,
        transition_id: None,
        pagination: false,
        final_canonical_url: None,
        current_work_state: Some(CrawlWorkState::Pending),
        work_generation: 0,
        current_execution_id: None,
        seed_provenance: vec![seed_id],
        seen: true,
        sampled: false,
        expanded: false,
        in_scope: false,
        page_type_match_state: None,
    };
    let control = CrawlTraversalControl {
        crawl_run_id: accepted.run_id,
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
        next_admission_sequence: 2,
    };
    CrawlTraversalRepository::new(&database)
        .initialize_run_state(accepted.run_id, &[root, child], &control)
        .await?;

    let root_job_id: erabi_db::repositories::JobId = accepted.job_id.parse()?;
    let jobs = JobRepository::new(&database);
    let root_acquired = jobs
        .acquire_next("frontier-checkpoint-source", 100, 30)
        .await?
        .ok_or("frontier source job was not acquired")?;
    let root_lease = root_acquired
        .job
        .lease
        .clone()
        .ok_or("frontier source lease missing")?;
    let checkpoint =
        CrawlCheckpointV2::new(accepted.run_id, &snapshot, CrawlRecoveryPhase::Traversing)?
            .to_envelope()?;
    jobs.append_checkpoint(
        &root_job_id,
        &root_acquired.attempt.id,
        &root_lease,
        &checkpoint,
        1,
    )
    .await?;
    jobs.cancel(&root_job_id, &root_lease, 2).await?;
    let recovery_action = jobs
        .enqueue_action_child(
            &root_job_id,
            JobKind::new("RESUME_CHECKPOINT")?,
            3,
            ActionRunAssociation::SameSourceRun,
            Some(1),
        )
        .await?;

    let calls = Arc::new(Mutex::new(Vec::new()));
    let temporary = tempfile::tempdir()?;
    let recovery_handler = handler(
        database.clone(),
        Arc::new(FixtureAdapter {
            pages: BTreeMap::from([(child_url.to_owned(), FixturePage::html(Vec::new()))]),
            calls: Arc::clone(&calls),
            clock: None,
        }),
        ArtifactStore::new(temporary.path())?,
        None,
    );
    let turn = runtime(&database)?
        .execute_next_at(&recovery_handler, 3)
        .await?;
    assert!(matches!(
        turn,
        WorkerTurn::Succeeded { job_id } if job_id == recovery_action.id
    ));
    let calls = match calls.lock() {
        Ok(calls) => calls.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(
        calls
            .iter()
            .map(|(url, _)| url.as_str())
            .collect::<Vec<_>>(),
        vec![child_url]
    );
    let durable = CrawlTraversalRepository::new(&database)
        .reconstruct_recovery_state(accepted.run_id)
        .await?;
    assert_eq!(
        durable
            .work
            .iter()
            .find(|work| work.canonical_url == child_url)
            .and_then(|work| work.current_work_state),
        Some(CrawlWorkState::Completed)
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn production_recovery_restores_zero_transition_page_schedule_and_preserve_only_seen_state()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, product_id) = published_graph(
        &database,
        vec![seed("https://example.test/listing/root")?],
        GraphOptions {
            hint_first_seed_as_product: false,
            add_runtime_ambiguous_match: false,
            add_recovery_transition: true,
            product_page_budget: Some(1),
        },
    )
    .await?;
    let transitions = CrawlerRepository::new(&database)
        .list_discovery_transitions(crawler.id(), version_id)
        .await?;
    let consumed_transition_id = transitions
        .iter()
        .find(|record| record.transition.name == "listing products")
        .map(|record| record.transition.id)
        .ok_or("counted recovery transition missing")?;
    let zero_transition_id = transitions
        .iter()
        .find(|record| record.transition.name == "product related products")
        .map(|record| record.transition.id)
        .ok_or("zero-consumption recovery transition missing")?;
    assert_ne!(consumed_transition_id, zero_transition_id);
    let consumed_transition = consumed_transition_id.to_string();
    let zero_transition = zero_transition_id.to_string();
    let product_page_type = product_id.to_string();

    let accepted = ProductionRunSubmissionService::new(database.clone())
        .submit(request(&crawler, version_id, 10, 60, 30_000), 100)
        .await?;
    let snapshot = CrawlRunRepository::new(&database)
        .snapshot(accepted.run_id)
        .await?;
    let version = CrawlerRepository::new(&database)
        .version(crawler.id(), version_id)
        .await?
        .version;
    let seed_id = version
        .seeds()
        .first()
        .ok_or("integrated recovery fixture seed missing")?
        .id
        .to_string();
    let root_url = "https://example.test/listing/root";
    let pending_product_url = "https://example.test/product/pending";
    let blocked_product_url = "https://example.test/product/blocked";
    let preserve_only_url = "https://example.test/preserve-only";
    let state_id = |canonical_url: &str| -> Result<String, Box<dyn std::error::Error>> {
        let identity = format!("{}:{canonical_url}", accepted.run_id);
        let digest = erabi_domain::canonical_sha256(&identity)?;
        Ok(format!("crawl:{digest}"))
    };
    let root_state_id = state_id(root_url)?;
    let preserve_evidence = DiscoveredUrlRecord {
        id: uuid::Uuid::now_v7().to_string(),
        crawl_run_id: accepted.run_id,
        source_id: None,
        raw_href: Some("/preserve-only".to_owned()),
        original_url: preserve_only_url.to_owned(),
        canonical_url: preserve_only_url.to_owned(),
        status: "CANONICAL_DUPLICATE".to_owned(),
        discovered_at: "unix:2".to_owned(),
        detail: serde_json::json!({
            "origin": "DISCOVERY_PATH",
            "seed_ids": [seed_id.clone()],
            "source_canonical_url": root_url,
            "duplicate_of_canonical_url": preserve_only_url,
        }),
    };
    CrawlRunRepository::new(&database)
        .record_discovered_url(&preserve_evidence)
        .await?;
    let root = CrawlUrlStateRecord {
        id: root_state_id.clone(),
        crawl_run_id: accepted.run_id,
        canonical_url: root_url.to_owned(),
        first_discovered_url_id: None,
        requested_url: root_url.to_owned(),
        parent_url_state_id: None,
        parent_discovered_url_id: None,
        admission_state: CrawlAdmissionState::Admitted,
        preserve_reason: None,
        resolved_to_url_state_id: None,
        admission_sequence: Some(0),
        depth: Some(0),
        target_page_type_id: None,
        transition_id: None,
        pagination: false,
        final_canonical_url: None,
        current_work_state: Some(CrawlWorkState::Completed),
        work_generation: 0,
        current_execution_id: None,
        seed_provenance: vec![seed_id.clone()],
        seen: true,
        sampled: true,
        expanded: true,
        in_scope: true,
        page_type_match_state: Some(CrawlPageTypeMatchState::Matched),
    };
    let pending_product = CrawlUrlStateRecord {
        id: state_id(pending_product_url)?,
        crawl_run_id: accepted.run_id,
        canonical_url: pending_product_url.to_owned(),
        first_discovered_url_id: None,
        requested_url: pending_product_url.to_owned(),
        parent_url_state_id: Some(root_state_id.clone()),
        parent_discovered_url_id: None,
        admission_state: CrawlAdmissionState::Admitted,
        preserve_reason: None,
        resolved_to_url_state_id: None,
        admission_sequence: Some(1),
        depth: Some(1),
        target_page_type_id: Some(product_page_type.clone()),
        transition_id: Some(zero_transition.clone()),
        pagination: false,
        final_canonical_url: None,
        current_work_state: Some(CrawlWorkState::Pending),
        work_generation: 0,
        current_execution_id: None,
        seed_provenance: vec![seed_id.clone()],
        seen: true,
        sampled: false,
        expanded: false,
        in_scope: true,
        page_type_match_state: Some(CrawlPageTypeMatchState::Matched),
    };
    let preserve_only = CrawlUrlStateRecord {
        id: state_id(preserve_only_url)?,
        crawl_run_id: accepted.run_id,
        canonical_url: preserve_only_url.to_owned(),
        first_discovered_url_id: Some(preserve_evidence.id.clone()),
        requested_url: preserve_only_url.to_owned(),
        parent_url_state_id: None,
        parent_discovered_url_id: None,
        admission_state: CrawlAdmissionState::PreserveOnly,
        preserve_reason: Some("CANONICAL_DUPLICATE".to_owned()),
        resolved_to_url_state_id: None,
        admission_sequence: None,
        depth: None,
        target_page_type_id: None,
        transition_id: None,
        pagination: false,
        final_canonical_url: None,
        current_work_state: None,
        work_generation: 0,
        current_execution_id: None,
        seed_provenance: vec![seed_id.clone()],
        seen: true,
        sampled: false,
        expanded: false,
        in_scope: false,
        page_type_match_state: None,
    };
    let control = CrawlTraversalControl {
        crawl_run_id: accepted.run_id,
        consumed_bytes: 42,
        raw_link_count: 1,
        duplicate_count: 0,
        robots_excluded_count: 0,
        provider_error_count: 0,
        external_url_count: 0,
        blocked_url_count: 0,
        peak_expansion_count: 1,
        elapsed_millis: 0,
        time_budget_hit: false,
        duration_work_not_expanded: false,
        pagination_truncation_count: 0,
        next_admission_sequence: 2,
    };
    let traversal_repository = CrawlTraversalRepository::new(&database);
    traversal_repository
        .initialize_run_state(
            accepted.run_id,
            &[root, pending_product, preserve_only],
            &control,
        )
        .await?;
    traversal_repository
        .replace_transition_source_counts(
            accepted.run_id,
            &[erabi_db::repositories::CrawlTransitionSourceCount {
                transition_id: consumed_transition.clone(),
                source_url_state_id: root_state_id,
                eligible_edge_count: 1,
            }],
        )
        .await?;

    let durable_before = traversal_repository
        .reconstruct_recovery_state(accepted.run_id)
        .await?;
    assert_eq!(
        durable_before
            .transition_source_counts
            .iter()
            .find(|count| count.transition_id == consumed_transition)
            .map(|count| count.eligible_edge_count),
        Some(1)
    );
    assert!(
        !durable_before
            .transition_source_counts
            .iter()
            .any(|count| count.transition_id == zero_transition)
    );
    let durable_preserve = durable_before
        .work
        .iter()
        .find(|work| work.canonical_url == preserve_only_url)
        .ok_or("preserve-only fixture state missing")?;
    assert_eq!(
        durable_preserve.admission_state,
        CrawlAdmissionState::PreserveOnly
    );
    assert!(durable_preserve.seen);
    assert!(durable_preserve.current_work_state.is_none());

    let root_job_id: erabi_db::repositories::JobId = accepted.job_id.parse()?;
    let jobs = JobRepository::new(&database);
    let root_acquired = jobs
        .acquire_next("integrated-recovery-source", 100, 30)
        .await?
        .ok_or("integrated recovery source job was not acquired")?;
    let root_lease = root_acquired
        .job
        .lease
        .clone()
        .ok_or("integrated recovery source lease missing")?;
    let checkpoint =
        CrawlCheckpointV2::new(accepted.run_id, &snapshot, CrawlRecoveryPhase::Traversing)?
            .to_envelope()?;
    jobs.append_checkpoint(
        &root_job_id,
        &root_acquired.attempt.id,
        &root_lease,
        &checkpoint,
        101,
    )
    .await?;
    jobs.cancel(&root_job_id, &root_lease, 102).await?;
    let recovery_action = jobs
        .enqueue_action_child(
            &root_job_id,
            JobKind::new("RESUME_CHECKPOINT")?,
            200,
            ActionRunAssociation::SameSourceRun,
            Some(1),
        )
        .await?;

    let calls = Arc::new(Mutex::new(Vec::new()));
    let recovery_temporary = tempfile::tempdir()?;
    let recovery_handler = handler(
        database.clone(),
        Arc::new(FixtureAdapter {
            pages: BTreeMap::from([(
                pending_product_url.to_owned(),
                FixturePage::html(vec![
                    ObservedLink {
                        raw_href: "/product/blocked".to_owned(),
                        selector: Some("a.product".to_owned()),
                    },
                    ObservedLink {
                        raw_href: "/preserve-only".to_owned(),
                        selector: Some("a.product".to_owned()),
                    },
                ]),
            )]),
            calls: Arc::clone(&calls),
            clock: None,
        }),
        ArtifactStore::new(recovery_temporary.path())?,
        None,
    );
    let turn = runtime(&database)?
        .execute_next_at(&recovery_handler, 200)
        .await?;
    assert!(
        matches!(
            &turn,
            WorkerTurn::Succeeded { job_id } if job_id == &recovery_action.id
        ),
        "{turn:?}"
    );

    let calls = match calls.lock() {
        Ok(calls) => calls.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(
        calls
            .iter()
            .map(|(url, _)| url.as_str())
            .collect::<Vec<_>>(),
        vec![pending_product_url]
    );

    let latest_checkpoint = jobs
        .latest_checkpoint_for_lineage(&recovery_action.id)
        .await?
        .ok_or("recovery checkpoint was not persisted")?;
    CrawlCheckpointV2::from_envelope(&latest_checkpoint.checkpoint, &snapshot, accepted.run_id)?;
    let durable_after = traversal_repository
        .reconstruct_recovery_state(accepted.run_id)
        .await?;
    assert_eq!(
        durable_after
            .transition_source_counts
            .iter()
            .find(|count| count.transition_id == consumed_transition)
            .map(|count| count.eligible_edge_count),
        Some(1)
    );
    assert!(
        !durable_after
            .transition_source_counts
            .iter()
            .any(|count| count.transition_id == zero_transition)
    );
    assert_eq!(
        durable_after
            .work
            .iter()
            .find(|work| work.canonical_url == pending_product_url)
            .and_then(|work| work.current_work_state),
        Some(CrawlWorkState::Completed)
    );

    let blocked_state = durable_after
        .work
        .iter()
        .find(|work| work.canonical_url == blocked_product_url)
        .ok_or("budget-excluded continuation state missing")?;
    assert_eq!(
        blocked_state.admission_state,
        CrawlAdmissionState::PreserveOnly
    );
    assert_eq!(
        blocked_state.preserve_reason.as_deref(),
        Some("BUDGET_EXCLUDED")
    );
    assert!(blocked_state.current_work_state.is_none());

    let recovered_preserve = durable_after
        .work
        .iter()
        .filter(|work| work.canonical_url == preserve_only_url)
        .collect::<Vec<_>>();
    assert_eq!(recovered_preserve.len(), 1);
    assert_eq!(
        recovered_preserve[0].admission_state,
        CrawlAdmissionState::PreserveOnly
    );
    assert!(recovered_preserve[0].seen);
    assert!(recovered_preserve[0].current_work_state.is_none());
    assert!(recovered_preserve[0].target_page_type_id.is_none());
    assert!(recovered_preserve[0].transition_id.is_none());

    let discoveries = CrawlRunRepository::new(&database)
        .discovered_urls(accepted.run_id)
        .await?;
    let blocked_discovery = discoveries
        .iter()
        .find(|record| {
            record.canonical_url == blocked_product_url
                && record.raw_href.as_deref() == Some("/product/blocked")
        })
        .ok_or("budget-excluded continuation evidence missing")?;
    assert_eq!(blocked_discovery.status, "BUDGET_EXCLUDED");
    let transition_evaluations = blocked_discovery.detail["transition_evaluations"]
        .as_array()
        .ok_or("transition evaluation evidence missing")?;
    let zero_evaluation = transition_evaluations
        .iter()
        .find(|evaluation| evaluation["transition_id"].as_str() == Some(zero_transition.as_str()))
        .ok_or("zero-consumption transition evaluation missing")?;
    assert_eq!(zero_evaluation["eligible"].as_bool(), Some(false));
    let budget_hits = blocked_discovery.detail["budget_hits"]
        .as_array()
        .ok_or("PageType budget evidence missing")?;
    assert!(budget_hits.iter().any(|hit| {
        hit["kind"].as_str() == Some("PAGE_TYPE_PAGE_BUDGET")
            && hit["page_type_id"].as_str() == Some(product_page_type.as_str())
            && hit["observed"].as_u64() == Some(1)
            && hit["limit"].as_u64() == Some(1)
    }));
    let preserve_discoveries = discoveries
        .iter()
        .filter(|record| {
            record.canonical_url == preserve_only_url
                && record.raw_href.as_deref() == Some("/preserve-only")
        })
        .collect::<Vec<_>>();
    assert_eq!(preserve_discoveries.len(), 2);
    assert!(
        preserve_discoveries
            .iter()
            .all(|record| record.status == "CANONICAL_DUPLICATE")
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn canonical_duplicate_and_preserve_only_provenance_are_durable()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, _) = published_graph(
        &database,
        vec![
            seed("https://example.test/listing/a")?,
            seed("https://example.test/listing/a")?,
        ],
        GraphOptions::default(),
    )
    .await?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([
            (
                "https://example.test/listing/a".to_owned(),
                FixturePage::html(vec![
                    ObservedLink {
                        raw_href: "/product/b".to_owned(),
                        selector: Some("a.product".to_owned()),
                    },
                    ObservedLink {
                        raw_href: "/product/b?utm_source=duplicate#duplicate".to_owned(),
                        selector: Some("a.product".to_owned()),
                    },
                    ObservedLink {
                        raw_href: "/unmatched/c".to_owned(),
                        selector: Some("a.product".to_owned()),
                    },
                    ObservedLink {
                        raw_href: "/product/ineligible".to_owned(),
                        selector: Some("a.other".to_owned()),
                    },
                ]),
            ),
            (
                "https://example.test/product/b".to_owned(),
                FixturePage {
                    final_url: None,
                    links: Vec::new(),
                    pagination: Vec::new(),
                    failure: Some(CrawlerAdapterError::Unavailable),
                    advance_clock_millis: 0,
                    provider_reported_partial: false,
                },
            ),
        ]),
        calls: Arc::clone(&calls),
        clock: None,
    };
    let accepted = submit_and_execute(
        &database,
        &crawler,
        version_id,
        10,
        60,
        30_000,
        Arc::new(adapter),
        None,
    )
    .await?;

    let calls = match calls.lock() {
        Ok(calls) => calls.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(
        calls
            .iter()
            .filter(|(url, _)| url == "https://example.test/product/b")
            .count(),
        1
    );
    let discoveries = CrawlRunRepository::new(&database)
        .discovered_urls(accepted.run_id)
        .await?;
    let fragment_duplicate = discoveries
        .iter()
        .find(|record| {
            record.status == "CANONICAL_DUPLICATE"
                && record.raw_href.as_deref() == Some("/product/b?utm_source=duplicate#duplicate")
        })
        .ok_or("fragment-bearing duplicate evidence missing")?;
    assert_eq!(
        fragment_duplicate.original_url,
        "https://example.test/product/b?utm_source=duplicate"
    );
    assert_eq!(
        fragment_duplicate.canonical_url,
        "https://example.test/product/b"
    );
    assert_eq!(
        fragment_duplicate.detail["resolved_observation_url"],
        "https://example.test/product/b?utm_source=duplicate#duplicate"
    );
    assert!(
        discoveries
            .iter()
            .any(|record| record.status == "UNMATCHED")
    );
    assert!(
        discoveries
            .iter()
            .any(|record| record.status == "TRANSITION_INELIGIBLE")
    );
    assert!(discoveries.iter().all(|record| record.source_id.is_none()));
    let executions = CrawlExecutionRepository::new(&database)
        .list_for_run(accepted.run_id)
        .await?;
    assert!(
        executions
            .iter()
            .any(|record| record.outcome == CrawlExecutionOutcome::Completed)
    );
    assert!(
        executions
            .iter()
            .any(|record| record.outcome == CrawlExecutionOutcome::Failed)
    );
    assert_eq!(
        CrawlRunRepository::new(&database)
            .status(accepted.run_id)
            .await?,
        CrawlRunStatus::PartialResult
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn redirect_final_url_is_authoritative_for_children_and_deduplication()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, _) = published_graph(
        &database,
        vec![
            seed("https://example.test/listing/alias/path")?,
            seed("https://example.test/listing/final")?,
        ],
        GraphOptions::default(),
    )
    .await?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([
            (
                "https://example.test/listing/alias/path".to_owned(),
                FixturePage {
                    final_url: Some("https://example.test/listing/final".to_owned()),
                    links: vec![ObservedLink {
                        raw_href: "../product/child".to_owned(),
                        selector: Some("a.product".to_owned()),
                    }],
                    pagination: Vec::new(),
                    failure: None,
                    advance_clock_millis: 0,
                    provider_reported_partial: false,
                },
            ),
            (
                "https://example.test/product/child".to_owned(),
                FixturePage::html(Vec::new()),
            ),
        ]),
        calls: Arc::clone(&calls),
        clock: None,
    };
    let accepted = submit_and_execute(
        &database,
        &crawler,
        version_id,
        10,
        60,
        30_000,
        Arc::new(adapter),
        None,
    )
    .await?;

    let call_snapshot = match calls.lock() {
        Ok(calls) => calls.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(
        call_snapshot
            .iter()
            .filter(|(url, _)| url == "https://example.test/listing/final")
            .count(),
        0
    );
    assert!(
        call_snapshot
            .iter()
            .any(|(url, _)| url == "https://example.test/product/child")
    );
    let discoveries = CrawlRunRepository::new(&database)
        .discovered_urls(accepted.run_id)
        .await?;
    let executions = CrawlExecutionRepository::new(&database)
        .list_for_run(accepted.run_id)
        .await?;
    let alias_execution = executions
        .iter()
        .find(|record| record.requested_url == "https://example.test/listing/alias/path")
        .ok_or("alias execution missing")?;
    assert_eq!(
        alias_execution.canonical_url,
        "https://example.test/listing/final"
    );
    assert_eq!(
        alias_execution.observed_final_url.as_deref(),
        Some("https://example.test/listing/final")
    );
    let execution_provenance_id = alias_execution
        .discovered_url_id
        .as_deref()
        .ok_or("alias execution provenance missing")?;
    let execution_provenance = discoveries
        .iter()
        .find(|record| record.id == execution_provenance_id)
        .ok_or("alias execution provenance not durable")?;
    assert_eq!(
        execution_provenance.original_url,
        "https://example.test/listing/alias/path"
    );
    assert_eq!(
        execution_provenance.canonical_url,
        "https://example.test/listing/final"
    );
    assert_eq!(execution_provenance.status, "EXECUTION_RECONCILED");
    let independent_final_seed = discoveries
        .iter()
        .find(|record| {
            record.detail["origin"] == "SEED"
                && record.original_url == "https://example.test/listing/final"
                && record.canonical_url == "https://example.test/listing/final"
        })
        .ok_or("independent final Seed provenance missing")?;
    assert_ne!(independent_final_seed.id, execution_provenance_id);
    let child = discoveries
        .iter()
        .find(|record| record.original_url == "https://example.test/product/child")
        .ok_or("child discovery missing")?;
    assert_eq!(
        child.detail["source_canonical_url"],
        "https://example.test/listing/final"
    );

    let root_job_id: erabi_db::repositories::JobId = accepted.job_id.parse()?;
    let recovery_action = JobRepository::new(&database)
        .enqueue_action_child(
            &root_job_id,
            JobKind::new("RESUME_CHECKPOINT")?,
            120,
            ActionRunAssociation::SameSourceRun,
            Some(1),
        )
        .await?;
    let recovery_temporary = tempfile::tempdir()?;
    let recovery_handler = handler(
        database.clone(),
        Arc::new(FixtureAdapter {
            pages: BTreeMap::new(),
            calls: Arc::clone(&calls),
            clock: None,
        }),
        ArtifactStore::new(recovery_temporary.path())?,
        None,
    );
    let recovery_turn = runtime(&database)?
        .execute_next_at(&recovery_handler, 120)
        .await?;
    assert!(matches!(
        recovery_turn,
        WorkerTurn::Succeeded { job_id } if job_id == recovery_action.id
    ));
    let calls_after_recovery = match calls.lock() {
        Ok(calls) => calls.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(calls_after_recovery, call_snapshot);
    let recovered = CrawlTraversalRepository::new(&database)
        .reconstruct_recovery_state(accepted.run_id)
        .await?;
    let alias_state = recovered
        .work
        .iter()
        .find(|work| work.canonical_url == "https://example.test/listing/alias/path")
        .ok_or("redirect alias logical state missing")?;
    assert_eq!(alias_state.admission_state, CrawlAdmissionState::Resolved);
    assert!(alias_state.current_work_state.is_none());
    Ok(())
}

#[tokio::test]
async fn unsafe_final_url_and_page_failure_are_partial_without_child_execution()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, _) = published_graph(
        &database,
        vec![seed("https://example.test/listing/a")?],
        GraphOptions::default(),
    )
    .await?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([(
            "https://example.test/listing/a".to_owned(),
            FixturePage {
                final_url: Some("http://127.0.0.1/private".to_owned()),
                links: vec![ObservedLink {
                    raw_href: "/product/never".to_owned(),
                    selector: Some("a.product".to_owned()),
                }],
                pagination: Vec::new(),
                failure: None,
                advance_clock_millis: 0,
                provider_reported_partial: false,
            },
        )]),
        calls: Arc::clone(&calls),
        clock: None,
    };
    let accepted = submit_and_execute(
        &database,
        &crawler,
        version_id,
        10,
        60,
        30_000,
        Arc::new(adapter),
        None,
    )
    .await?;

    assert_eq!(
        CrawlRunRepository::new(&database)
            .status(accepted.run_id)
            .await?,
        CrawlRunStatus::PartialResult
    );
    let call_count = match calls.lock() {
        Ok(calls) => calls.len(),
        Err(poisoned) => poisoned.into_inner().len(),
    };
    assert_eq!(call_count, 1);
    let records = CrawlExecutionRepository::new(&database)
        .list_for_run(accepted.run_id)
        .await?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].outcome, CrawlExecutionOutcome::Failed);
    Ok(())
}

#[tokio::test]
async fn seed_page_type_hint_does_not_override_observed_plan_five_matching()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, product_id) = published_graph(
        &database,
        vec![seed("https://example.test/listing/a")?],
        GraphOptions {
            hint_first_seed_as_product: true,
            add_runtime_ambiguous_match: false,
            add_recovery_transition: false,
            product_page_budget: None,
        },
    )
    .await?;
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([
            (
                "https://example.test/listing/a".to_owned(),
                FixturePage::html(vec![ObservedLink {
                    raw_href: "/product/b".to_owned(),
                    selector: Some("a.product".to_owned()),
                }]),
            ),
            (
                "https://example.test/product/b".to_owned(),
                FixturePage::html(Vec::new()),
            ),
        ]),
        calls: Arc::new(Mutex::new(Vec::new())),
        clock: None,
    };
    let accepted = submit_and_execute(
        &database,
        &crawler,
        version_id,
        10,
        60,
        30_000,
        Arc::new(adapter),
        None,
    )
    .await?;

    let discoveries = CrawlRunRepository::new(&database)
        .discovered_urls(accepted.run_id)
        .await?;
    let root = discoveries
        .iter()
        .find(|record| record.detail["origin"] == "SEED")
        .ok_or("seed provenance missing")?;
    assert_eq!(root.detail["entry_page_type_hint"], product_id.to_string());
    let executions = CrawlExecutionRepository::new(&database)
        .list_for_run(accepted.run_id)
        .await?;
    assert_eq!(executions.len(), 2);
    assert!(executions.iter().any(|record| {
        record.requested_url == "https://example.test/listing/a"
            && record.page_type_id != Some(product_id)
    }));
    assert_eq!(
        CrawlRunRepository::new(&database)
            .status(accepted.run_id)
            .await?,
        CrawlRunStatus::Succeeded
    );
    Ok(())
}

#[tokio::test]
async fn unresolved_page_type_ambiguity_is_a_partial_result()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, _) = published_graph(
        &database,
        vec![seed("https://example.test/listing/a")?],
        GraphOptions {
            hint_first_seed_as_product: false,
            add_runtime_ambiguous_match: true,
            add_recovery_transition: false,
            product_page_budget: None,
        },
    )
    .await?;
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([(
            "https://example.test/listing/a".to_owned(),
            FixturePage::html(vec![ObservedLink {
                raw_href: "/ambiguous/runtime-only".to_owned(),
                selector: Some("a.product".to_owned()),
            }]),
        )]),
        calls: Arc::new(Mutex::new(Vec::new())),
        clock: None,
    };
    let accepted = submit_and_execute(
        &database,
        &crawler,
        version_id,
        10,
        60,
        30_000,
        Arc::new(adapter),
        None,
    )
    .await?;

    assert_eq!(
        CrawlRunRepository::new(&database)
            .status(accepted.run_id)
            .await?,
        CrawlRunStatus::PartialResult
    );
    assert!(
        CrawlExecutionRepository::new(&database)
            .summary(accepted.run_id)
            .await?
            .page_type_ambiguity_count
            > 0
    );
    Ok(())
}

#[tokio::test]
async fn duration_caps_provider_timeout_and_prevents_a_new_provider_call()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, _) = published_graph(
        &database,
        vec![seed("https://example.test/listing/a")?],
        GraphOptions::default(),
    )
    .await?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let clock = Arc::new(ManualPreviewClock::new());
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([
            (
                "https://example.test/listing/a".to_owned(),
                FixturePage {
                    final_url: None,
                    links: vec![ObservedLink {
                        raw_href: "/product/next".to_owned(),
                        selector: Some("a.product".to_owned()),
                    }],
                    pagination: Vec::new(),
                    failure: None,
                    advance_clock_millis: 1_000,
                    provider_reported_partial: false,
                },
            ),
            (
                "https://example.test/product/next".to_owned(),
                FixturePage::html(Vec::new()),
            ),
        ]),
        calls: Arc::clone(&calls),
        clock: Some(Arc::clone(&clock)),
    };
    let accepted = submit_and_execute(
        &database,
        &crawler,
        version_id,
        10,
        1,
        30_000,
        Arc::new(adapter),
        Some(clock),
    )
    .await?;

    let calls = match calls.lock() {
        Ok(calls) => calls.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].1, Duration::from_millis(1_000));
    assert_eq!(
        CrawlRunRepository::new(&database)
            .status(accepted.run_id)
            .await?,
        CrawlRunStatus::PartialResult
    );
    let summary = CrawlExecutionRepository::new(&database)
        .summary(accepted.run_id)
        .await?;
    assert!(summary.unresolved_partial_work_count > 0);
    let discoveries = CrawlRunRepository::new(&database)
        .discovered_urls(accepted.run_id)
        .await?;
    assert!(discoveries.iter().any(|record| {
        record.detail["origin"] == "SEED" && record.discovered_at == "unix-ms:0"
    }));
    Ok(())
}

#[tokio::test]
async fn pagination_observation_uses_the_shared_bounded_discovery_pipeline()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, _) = published_graph(
        &database,
        vec![seed("https://example.test/listing/a")?],
        GraphOptions::default(),
    )
    .await?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([
            (
                "https://example.test/listing/a".to_owned(),
                FixturePage {
                    final_url: None,
                    links: Vec::new(),
                    pagination: vec![PaginationObservation {
                        kind: PaginationKind::RelNext,
                        selector: Some("a.product".to_owned()),
                        target_url: Some("/product/next".to_owned()),
                    }],
                    failure: None,
                    advance_clock_millis: 0,
                    provider_reported_partial: false,
                },
            ),
            (
                "https://example.test/product/next".to_owned(),
                FixturePage::html(Vec::new()),
            ),
        ]),
        calls: Arc::clone(&calls),
        clock: None,
    };
    let accepted = submit_and_execute(
        &database,
        &crawler,
        version_id,
        10,
        60,
        30_000,
        Arc::new(adapter),
        None,
    )
    .await?;

    let calls = match calls.lock() {
        Ok(calls) => calls.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert!(
        calls
            .iter()
            .any(|(url, _)| url == "https://example.test/product/next")
    );
    let discoveries = CrawlRunRepository::new(&database)
        .discovered_urls(accepted.run_id)
        .await?;
    assert!(discoveries.iter().any(|record| {
        record.original_url == "https://example.test/product/next" && record.status == "ADMITTED"
    }));
    Ok(())
}

#[tokio::test]
async fn targetless_pagination_is_durable_incomplete_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, _) = published_graph(
        &database,
        vec![seed("https://example.test/listing/a")?],
        GraphOptions::default(),
    )
    .await?;
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([(
            "https://example.test/listing/a".to_owned(),
            FixturePage {
                final_url: None,
                links: Vec::new(),
                pagination: vec![PaginationObservation {
                    kind: PaginationKind::RelNext,
                    selector: Some("a.product".to_owned()),
                    target_url: None,
                }],
                failure: None,
                advance_clock_millis: 0,
                provider_reported_partial: false,
            },
        )]),
        calls: Arc::new(Mutex::new(Vec::new())),
        clock: None,
    };
    let accepted = submit_and_execute(
        &database,
        &crawler,
        version_id,
        10,
        60,
        30_000,
        Arc::new(adapter),
        None,
    )
    .await?;
    let summary = CrawlExecutionRepository::new(&database)
        .summary(accepted.run_id)
        .await?;
    assert_eq!(summary.pagination_truncation_count, 1);
    // Targetless pagination is represented independently by the structural
    // counter; it must not be counted again as generic unresolved work.
    assert_eq!(summary.unresolved_partial_work_count, 0);
    assert_eq!(
        CrawlRunRepository::new(&database)
            .status(accepted.run_id)
            .await?,
        CrawlRunStatus::PartialResult
    );
    Ok(())
}

#[tokio::test]
async fn targetful_pagination_budget_rejection_is_durable_truncation()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, _) = published_graph(
        &database,
        vec![seed("https://example.test/listing/a")?],
        GraphOptions::default(),
    )
    .await?;
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([(
            "https://example.test/listing/a".to_owned(),
            FixturePage {
                final_url: None,
                links: Vec::new(),
                pagination: vec![PaginationObservation {
                    kind: PaginationKind::RelNext,
                    selector: Some("a.product".to_owned()),
                    target_url: Some("/product/next".to_owned()),
                }],
                failure: None,
                advance_clock_millis: 0,
                provider_reported_partial: false,
            },
        )]),
        calls: Arc::new(Mutex::new(Vec::new())),
        clock: None,
    };
    let accepted = submit_and_execute(
        &database,
        &crawler,
        version_id,
        1,
        60,
        30_000,
        Arc::new(adapter),
        None,
    )
    .await?;
    let summary = CrawlExecutionRepository::new(&database)
        .summary(accepted.run_id)
        .await?;
    assert_eq!(summary.pagination_truncation_count, 1);
    // The rejected pagination target remains independently represented by
    // `pagination_truncation_count`; it must not be counted twice.
    assert_eq!(summary.unresolved_partial_work_count, 0);
    let discoveries = CrawlRunRepository::new(&database)
        .discovered_urls(accepted.run_id)
        .await?;
    assert!(discoveries.iter().any(|record| {
        record.raw_href.as_deref() == Some("/product/next") && record.status == "BUDGET_EXCLUDED"
    }));
    assert_eq!(
        CrawlRunRepository::new(&database)
            .status(accepted.run_id)
            .await?,
        CrawlRunStatus::PartialResult
    );
    Ok(())
}

#[tokio::test]
async fn clean_duration_boundary_succeeds_when_frontier_is_empty()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, _) = published_graph(
        &database,
        vec![seed("https://example.test/listing/a")?],
        GraphOptions::default(),
    )
    .await?;
    let clock = Arc::new(ManualPreviewClock::new());
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([(
            "https://example.test/listing/a".to_owned(),
            FixturePage {
                final_url: None,
                links: Vec::new(),
                pagination: Vec::new(),
                failure: None,
                advance_clock_millis: 1_000,
                provider_reported_partial: false,
            },
        )]),
        calls: Arc::new(Mutex::new(Vec::new())),
        clock: Some(Arc::clone(&clock)),
    };
    let accepted = submit_and_execute(
        &database,
        &crawler,
        version_id,
        10,
        1,
        30_000,
        Arc::new(adapter),
        Some(clock),
    )
    .await?;
    let summary = CrawlExecutionRepository::new(&database)
        .summary(accepted.run_id)
        .await?;
    assert_eq!(summary.in_scope_pages_planned, 1);
    assert_eq!(summary.in_scope_pages_completed, 1);
    assert_eq!(summary.unresolved_partial_work_count, 0);
    assert_eq!(
        CrawlRunRepository::new(&database)
            .status(accepted.run_id)
            .await?,
        CrawlRunStatus::Succeeded
    );
    Ok(())
}

#[tokio::test]
async fn pagination_only_duration_work_is_partial() -> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, _) = published_graph(
        &database,
        vec![seed("https://example.test/listing/a")?],
        GraphOptions::default(),
    )
    .await?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let clock = Arc::new(ManualPreviewClock::new());
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([(
            "https://example.test/listing/a".to_owned(),
            FixturePage {
                final_url: None,
                links: Vec::new(),
                pagination: vec![PaginationObservation {
                    kind: PaginationKind::RelNext,
                    selector: Some("a.product".to_owned()),
                    target_url: Some("/product/next".to_owned()),
                }],
                failure: None,
                advance_clock_millis: 1_000,
                provider_reported_partial: false,
            },
        )]),
        calls: Arc::clone(&calls),
        clock: Some(Arc::clone(&clock)),
    };
    let accepted = submit_and_execute(
        &database,
        &crawler,
        version_id,
        10,
        1,
        30_000,
        Arc::new(adapter),
        Some(clock),
    )
    .await?;
    let call_count = match calls.lock() {
        Ok(calls) => calls.len(),
        Err(poisoned) => poisoned.into_inner().len(),
    };
    assert_eq!(call_count, 1);
    let summary = CrawlExecutionRepository::new(&database)
        .summary(accepted.run_id)
        .await?;
    assert_eq!(summary.pagination_truncation_count, 1);
    // A pagination-only duration boundary is represented by the pagination
    // structural counter, not generic unexpanded work.
    assert_eq!(summary.unresolved_partial_work_count, 0);
    assert_eq!(
        CrawlRunRepository::new(&database)
            .status(accepted.run_id)
            .await?,
        CrawlRunStatus::PartialResult
    );
    Ok(())
}

#[tokio::test]
async fn provider_partial_page_counts_one_attempt_without_double_counting()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version_id, _) = published_graph(
        &database,
        vec![seed("https://example.test/listing/a")?],
        GraphOptions::default(),
    )
    .await?;
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([(
            "https://example.test/listing/a".to_owned(),
            FixturePage::partial_html(Vec::new()),
        )]),
        calls: Arc::new(Mutex::new(Vec::new())),
        clock: None,
    };
    let accepted = submit_and_execute(
        &database,
        &crawler,
        version_id,
        10,
        60,
        30_000,
        Arc::new(adapter),
        None,
    )
    .await?;
    let summary = CrawlExecutionRepository::new(&database)
        .summary(accepted.run_id)
        .await?;
    assert_eq!(summary.in_scope_pages_planned, 1);
    assert_eq!(summary.in_scope_pages_completed, 1);
    assert_eq!(summary.pagination_truncation_count, 0);
    assert_eq!(summary.page_type_ambiguity_count, 0);
    assert_eq!(summary.unresolved_partial_work_count, 1);
    let executions = CrawlExecutionRepository::new(&database)
        .list_for_run(accepted.run_id)
        .await?;
    assert_eq!(executions.len(), 1);
    assert_eq!(executions[0].outcome, CrawlExecutionOutcome::Partial);
    assert_eq!(
        CrawlRunRepository::new(&database)
            .status(accepted.run_id)
            .await?,
        CrawlRunStatus::PartialResult
    );
    Ok(())
}

#[tokio::test]
async fn fragment_bearing_seeds_fetch_fragment_free_once_and_retain_authorship()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let first = Seed::new(
        "https://example.test/listing/a#section".parse()?,
        "https://example.test/listing/a".parse()?,
    );
    let second = Seed::new(
        "https://example.test/listing/a#other".parse()?,
        "https://example.test/listing/a".parse()?,
    );
    let first_id = first.id;
    let (crawler, version_id, _) =
        published_graph(&database, vec![first, second], GraphOptions::default()).await?;
    let authored = CrawlerRepository::new(&database)
        .version(crawler.id(), version_id)
        .await?
        .version;
    assert!(authored.seeds().iter().any(|seed| seed.id == first_id
        && seed.original_url.as_str() == "https://example.test/listing/a#section"));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let adapter = FixtureAdapter {
        pages: BTreeMap::from([(
            "https://example.test/listing/a".to_owned(),
            FixturePage::html(Vec::new()),
        )]),
        calls: Arc::clone(&calls),
        clock: None,
    };
    let accepted = submit_and_execute(
        &database,
        &crawler,
        version_id,
        10,
        60,
        30_000,
        Arc::new(adapter),
        None,
    )
    .await?;
    let calls = match calls.lock() {
        Ok(calls) => calls.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "https://example.test/listing/a");
    let discoveries = CrawlRunRepository::new(&database)
        .discovered_urls(accepted.run_id)
        .await?;
    assert!(discoveries.iter().all(|record| {
        !record.original_url.contains('#') && !record.canonical_url.contains('#')
    }));
    Ok(())
}
