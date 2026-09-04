use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use erabi_crawler::{
    CrawlerAdapter, CrawlerAdapterError, CrawlerArtifactEvidence, CrawlerCapabilities,
    CrawlerExecuteRequest, CrawlerExecuteResult, CrawlerFuture, CrawlerHealth, CrawlerHealthStatus,
    CrawlerMediaType, CrawlerResponseMetadata, ManualPreviewClock, NetworkTargetPolicy,
    ObservedLink, PacingService, PaginationObservation, ProductionRunSubmissionRequest,
    ProductionRunSubmissionService, RetryAfterTiming, RobotsHttpResponse, RobotsPolicyService,
    RobotsTransport, StaticNetworkResolver, ValidatedNetworkTarget,
};
use erabi_db::{
    ArtifactStore, ErabiDatabase, MigrationRunner,
    repositories::{
        CrawlExecutionRepository, CrawlRunRepository, CrawlerRepository, JobRepository,
    },
};
use erabi_domain::{
    CrawlExecutionOutcome, CrawlRunStatus, Crawler, CrawlerVersionId, DiscoveryTransition,
    PaginationKind, ResolvedValue, RobotsAudit, Seed, SettingSource, SnapshotOperationalSettings,
    TransitionBudget, UrlMatcher,
};
use erabi_jobs::{
    CancellationController, JobRuntime, ProductionCrawlJobHandler, ProgressReplayRequest,
    ProgressRepository, StoragePressureMonitor, StoragePressurePolicy, StorageProbe,
    StorageProbeError, WorkerPolicy, WorkerTurn,
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

    let calls = match calls.lock() {
        Ok(calls) => calls.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(
        calls
            .iter()
            .filter(|(url, _)| url == "https://example.test/listing/final")
            .count(),
        0
    );
    assert!(
        calls
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
        record.detail["origin"] == "SEED" && record.discovered_at == "unix-ms:1000"
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
    assert!(summary.unresolved_partial_work_count > 0);
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
    assert!(summary.unresolved_partial_work_count > 0);
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
    assert!(summary.unresolved_partial_work_count > 0);
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
