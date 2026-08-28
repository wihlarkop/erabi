use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use erabi_crawler::{
    ExtractionTestHook, ExtractionTestRequest, FixtureTestLabProvider, ObservedLink,
    PageObservation, TestLabError, TestLabObservationRequest, TestLabProvider,
    TestLabProviderError, TestLabRequest, TestLabService,
};
use erabi_db::{ErabiDatabase, MigrationRunner, repositories::CrawlerRepository};
use erabi_domain::{
    Crawler, CrawlerVersionId, DiscoveryTransition, DiscoveryTransitionId, ExtractionFieldEvidence,
    ExtractionObservation, PageTypeId, Seed, TestKind, TransitionBudget, UrlMatcher,
};

struct TransitionFixture {
    database: ErabiDatabase,
    crawler: Crawler,
    version_id: CrawlerVersionId,
    source_page_type_id: PageTypeId,
    transition_id: DiscoveryTransitionId,
}

async fn transition_fixture(
    per_page_limit: u32,
) -> Result<TransitionFixture, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("Test Lab regressions");
    repository.create(&crawler).await?;
    let version = repository
        .create_draft(crawler.id(), "operator", "unix:1")
        .await?;
    let source = repository
        .create_page_type(
            crawler.id(),
            version.id(),
            "Listing",
            1,
            "operator",
            "unix:2",
        )
        .await?;
    let target = repository
        .create_page_type(
            crawler.id(),
            version.id(),
            "Product",
            1,
            "operator",
            "unix:3",
        )
        .await?;
    for (page_type_id, path, time) in [
        (source.id, "/listing", "unix:4"),
        (target.id, "/products", "unix:5"),
    ] {
        repository
            .create_url_matcher(
                crawler.id(),
                version.id(),
                page_type_id,
                &UrlMatcher::path_prefix(None, path),
                "operator",
                time,
            )
            .await?;
    }
    let mut configured = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    configured.add_seed(Seed::new(
        "https://example.test/listing".parse()?,
        "https://example.test/listing".parse()?,
    ))?;
    repository
        .save_draft(&configured, "operator", "unix:6")
        .await?;
    let transition = DiscoveryTransition {
        id: DiscoveryTransitionId::new(),
        source_page_type_id: source.id,
        target_page_type_id: target.id,
        name: "Product links".to_owned(),
        enabled: true,
        link_selector: "a.product".to_owned(),
        url_constraints: None,
        priority: 1,
        budget: TransitionBudget {
            max_links_per_source_page: per_page_limit,
            total_budget: Some(3),
            depth_contribution: 1,
        },
        deduplicate: true,
        latest_test_evidence_id: None,
    };
    repository
        .create_discovery_transition(
            crawler.id(),
            configured.id(),
            &transition,
            "operator",
            "unix:7",
        )
        .await?;
    Ok(TransitionFixture {
        database,
        crawler,
        version_id: configured.id(),
        source_page_type_id: source.id,
        transition_id: transition.id,
    })
}

fn observation(url: &str, discovered_links: Vec<ObservedLink>) -> PageObservation {
    PageObservation {
        requested_url: url.to_owned(),
        final_url: None,
        artifact_ids: Vec::new(),
        discovered_links,
        selector_observations: Vec::new(),
        pagination_observations: Vec::new(),
    }
}

fn transition_request(transition_id: DiscoveryTransitionId, compare: bool) -> TestLabRequest {
    TestLabRequest {
        test_kind: TestKind::DiscoveryTransition,
        input_urls: vec!["https://example.test/listing".to_owned()],
        page_type_id: None,
        transition_id: Some(transition_id),
        compare_with_active_published: compare,
        reuse_artifact_ids: Vec::new(),
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn transition_source_page_type_is_a_required_applicability_gate()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = transition_fixture(2).await?;
    let link = ObservedLink {
        raw_href: "/products/1".to_owned(),
        selector: Some("a.product".to_owned()),
    };
    let service = TestLabService::new(
        fixture.database.clone(),
        Some(Arc::new(FixtureTestLabProvider::new([observation(
            "https://example.test/listing",
            vec![link.clone()],
        )]))),
        None,
    );
    let eligible = service
        .execute(
            fixture.crawler.id(),
            fixture.version_id,
            transition_request(fixture.transition_id, false),
        )
        .await?
        .evidence
        .discovery
        .ok_or("missing transition evidence")?;
    assert_eq!(eligible.eligible_link_count, 1);
    assert_eq!(
        eligible
            .source_match
            .and_then(|source| source.winner)
            .map(|winner| winner.page_type_id),
        Some(fixture.source_page_type_id)
    );

    let repository = CrawlerRepository::new(&fixture.database);
    let article = repository
        .create_page_type(
            fixture.crawler.id(),
            fixture.version_id,
            "Article",
            1,
            "operator",
            "unix:8",
        )
        .await?;
    repository
        .create_url_matcher(
            fixture.crawler.id(),
            fixture.version_id,
            article.id,
            &UrlMatcher::path_prefix(None, "/article"),
            "operator",
            "unix:9",
        )
        .await?;
    let wrong_source = TestLabService::new(
        fixture.database.clone(),
        Some(Arc::new(FixtureTestLabProvider::new([observation(
            "https://example.test/article",
            vec![link.clone()],
        )]))),
        None,
    )
    .execute(
        fixture.crawler.id(),
        fixture.version_id,
        TestLabRequest {
            input_urls: vec!["https://example.test/article".to_owned()],
            ..transition_request(fixture.transition_id, false)
        },
    )
    .await?
    .evidence;
    assert_eq!(
        wrong_source
            .discovery
            .as_ref()
            .map(|discovery| discovery.eligible_link_count),
        Some(0)
    );
    assert!(
        wrong_source
            .warnings
            .iter()
            .any(|warning| warning.code == "TRANSITION_SOURCE_PAGE_TYPE_MISMATCH")
    );

    let tied = repository
        .create_page_type(
            fixture.crawler.id(),
            fixture.version_id,
            "Tied listing",
            1,
            "operator",
            "unix:10",
        )
        .await?;
    repository
        .create_url_matcher(
            fixture.crawler.id(),
            fixture.version_id,
            tied.id,
            &UrlMatcher::path_prefix(None, "/listing"),
            "operator",
            "unix:11",
        )
        .await?;
    let ambiguous = TestLabService::new(
        fixture.database.clone(),
        Some(Arc::new(FixtureTestLabProvider::new([observation(
            "https://example.test/listing",
            vec![link.clone()],
        )]))),
        None,
    )
    .execute(
        fixture.crawler.id(),
        fixture.version_id,
        transition_request(fixture.transition_id, false),
    )
    .await?
    .evidence;
    assert_eq!(
        ambiguous
            .discovery
            .as_ref()
            .map(|discovery| discovery.eligible_link_count),
        Some(0)
    );
    assert!(matches!(
        ambiguous
            .discovery
            .and_then(|discovery| discovery.source_match)
            .map(|source| source.decision),
        Some(erabi_domain::PageTypeMatchStatus::Ambiguous)
    ));

    let unmatched = TestLabService::new(
        fixture.database.clone(),
        Some(Arc::new(FixtureTestLabProvider::new([observation(
            "https://example.test/unmatched",
            vec![link],
        )]))),
        None,
    )
    .execute(
        fixture.crawler.id(),
        fixture.version_id,
        TestLabRequest {
            input_urls: vec!["https://example.test/unmatched".to_owned()],
            ..transition_request(fixture.transition_id, false)
        },
    )
    .await?
    .evidence;
    assert_eq!(
        unmatched
            .discovery
            .as_ref()
            .map(|discovery| discovery.eligible_link_count),
        Some(0)
    );
    assert!(
        unmatched
            .warnings
            .iter()
            .any(|warning| warning.code == "UNMATCHED_TRANSITION_SOURCE_PAGE_TYPE")
    );
    Ok(())
}

struct CountingProvider {
    calls: Arc<AtomicUsize>,
    page: PageObservation,
}

impl TestLabProvider for CountingProvider {
    fn observe(
        &self,
        _request: TestLabObservationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<PageObservation, TestLabProviderError>>
                + Send
                + '_,
        >,
    > {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let page = self.page.clone();
        Box::pin(async move { Ok(page) })
    }

    fn validate_reuse(
        &self,
        _artifact_id: erabi_domain::ArtifactId,
    ) -> Result<(), TestLabProviderError> {
        Ok(())
    }
}

#[tokio::test]
async fn selector_provenance_and_transition_local_budget_are_strict()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = transition_fixture(1).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let links = vec![
        ObservedLink {
            raw_href: "%zz".to_owned(),
            selector: Some("a.product".to_owned()),
        },
        ObservedLink {
            raw_href: "/products/0".to_owned(),
            selector: Some("a.other".to_owned()),
        },
        ObservedLink {
            raw_href: "/products/1".to_owned(),
            selector: Some("a.product".to_owned()),
        },
        ObservedLink {
            raw_href: "/products/1#duplicate".to_owned(),
            selector: Some("a.product".to_owned()),
        },
        ObservedLink {
            raw_href: "/products/2".to_owned(),
            selector: Some("a.product".to_owned()),
        },
        ObservedLink {
            raw_href: "/products/3".to_owned(),
            selector: None,
        },
        ObservedLink {
            raw_href: "https://external.test/products/4".to_owned(),
            selector: Some("a.product".to_owned()),
        },
    ];
    let service = TestLabService::new(
        fixture.database.clone(),
        Some(Arc::new(CountingProvider {
            calls: Arc::clone(&calls),
            page: observation("https://example.test/listing", links),
        })),
        None,
    );
    let evidence = service
        .execute(
            fixture.crawler.id(),
            fixture.version_id,
            transition_request(fixture.transition_id, false),
        )
        .await?
        .evidence;
    let discovery = evidence.discovery.ok_or("missing transition evidence")?;
    let item = |raw_href: &str| {
        discovery
            .discovered_urls
            .iter()
            .find(|item| item.raw_href == raw_href)
            .ok_or("missing discovered URL")
    };
    assert!(item("/products/1")?.transition_eligible);
    assert!(!item("/products/0")?.transition_eligible);
    assert!(!item("/products/1#duplicate")?.transition_eligible);
    assert!(item("/products/1#duplicate")?.duplicate);
    assert!(!item("/products/3")?.transition_eligible);
    assert!(!item("https://external.test/products/4")?.transition_eligible);
    assert!(!item("/products/2")?.transition_eligible);
    assert_eq!(
        item("/products/2")?
            .budget
            .as_ref()
            .and_then(|budget| budget.exclusion),
        Some(erabi_domain::TransitionBudgetExclusionEvidence::TransitionPerPageLinkLimit)
    );
    assert_eq!(discovery.eligible_link_count, 1);
    assert!(discovery.per_page_limit_reached);
    assert!(
        evidence
            .warnings
            .iter()
            .any(|warning| warning.code == "TRANSITION_SELECTOR_PROVENANCE_UNAVAILABLE")
    );
    assert!(
        evidence
            .warnings
            .iter()
            .any(|warning| warning.code == "FOCUSED_TRANSITION_TOTAL_BASELINE")
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "Test Lab must not navigate recursively"
    );
    Ok(())
}

async fn matching_clone_fixture()
-> Result<(ErabiDatabase, Crawler, CrawlerVersionId, CrawlerVersionId), Box<dyn std::error::Error>>
{
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("clone comparison");
    repository.create(&crawler).await?;
    let version = repository
        .create_draft(crawler.id(), "operator", "unix:1")
        .await?;
    for (name, priority, time) in [("Listing", 2, "unix:2"), ("Other", 1, "unix:3")] {
        let page_type = repository
            .create_page_type(crawler.id(), version.id(), name, priority, "operator", time)
            .await?;
        repository
            .create_url_matcher(
                crawler.id(),
                version.id(),
                page_type.id,
                &UrlMatcher::path_prefix(None, "/items"),
                "operator",
                time,
            )
            .await?;
    }
    let published = repository
        .publish(crawler.id(), version.id(), "operator", "unix:4")
        .await?;
    let draft = repository
        .create_draft_from_published(crawler.id(), published.version.id(), "operator", "unix:5")
        .await?;
    Ok((database, crawler, published.version.id(), draft.id()))
}

#[tokio::test]
async fn page_type_comparison_is_clone_safe_but_detects_real_behavior_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let (database, crawler, published_id, draft_id) = matching_clone_fixture().await?;
    let repository = CrawlerRepository::new(&database);
    let service = TestLabService::new(database.clone(), None, None);
    let request = TestLabRequest {
        test_kind: TestKind::PageTypeMatching,
        input_urls: vec!["https://example.test/items/1".to_owned()],
        page_type_id: None,
        transition_id: None,
        compare_with_active_published: true,
        reuse_artifact_ids: Vec::new(),
    };
    let unchanged = service
        .execute(crawler.id(), draft_id, request.clone())
        .await?
        .evidence;
    let comparison = unchanged.published_comparison.ok_or("missing comparison")?;
    assert!(!comparison.page_type_match_difference);
    let draft_candidate = &comparison.draft_page_type_match[0].candidates[0];
    let published_candidate = &comparison.published_page_type_match[0].candidates[0];
    assert_ne!(
        draft_candidate.page_type_id,
        published_candidate.page_type_id
    );
    assert_eq!(
        draft_candidate.page_type_id,
        repository
            .list_page_types(crawler.id(), draft_id)
            .await?
            .into_iter()
            .find(|page_type| page_type.name == "Listing")
            .ok_or("missing Draft Listing")?
            .id
    );
    assert_eq!(
        published_candidate.page_type_id,
        repository
            .list_page_types(crawler.id(), published_id)
            .await?
            .into_iter()
            .find(|page_type| page_type.name == "Listing")
            .ok_or("missing Published Listing")?
            .id
    );
    let other = repository
        .list_page_types(crawler.id(), draft_id)
        .await?
        .into_iter()
        .find(|page_type| page_type.name == "Other")
        .ok_or("missing Draft Other")?;
    repository
        .update_page_type(
            crawler.id(),
            draft_id,
            other.id,
            "Other",
            3,
            "operator",
            "unix:6",
        )
        .await?;
    let changed = service
        .execute(crawler.id(), draft_id, request)
        .await?
        .evidence;
    assert!(
        changed
            .published_comparison
            .as_ref()
            .is_some_and(|comparison| comparison.page_type_match_difference)
    );
    Ok(())
}

#[tokio::test]
async fn ambiguous_clone_comparison_is_semantic_and_order_independent()
-> Result<(), Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("ambiguous clone comparison");
    repository.create(&crawler).await?;
    let version = repository
        .create_draft(crawler.id(), "operator", "unix:1")
        .await?;
    for name in ["First", "Second"] {
        let page_type = repository
            .create_page_type(crawler.id(), version.id(), name, 1, "operator", "unix:2")
            .await?;
        repository
            .create_url_matcher(
                crawler.id(),
                version.id(),
                page_type.id,
                &UrlMatcher::path_prefix(None, "/items"),
                "operator",
                "unix:3",
            )
            .await?;
    }
    let published = repository
        .publish(crawler.id(), version.id(), "operator", "unix:4")
        .await?;
    let draft = repository
        .create_draft_from_published(crawler.id(), published.version.id(), "operator", "unix:5")
        .await?;
    let evidence = TestLabService::new(database, None, None)
        .execute(
            crawler.id(),
            draft.id(),
            TestLabRequest {
                test_kind: TestKind::PageTypeMatching,
                input_urls: vec!["https://example.test/items/1".to_owned()],
                page_type_id: None,
                transition_id: None,
                compare_with_active_published: true,
                reuse_artifact_ids: Vec::new(),
            },
        )
        .await?
        .evidence;
    let comparison = evidence.published_comparison.ok_or("missing comparison")?;
    assert!(!comparison.page_type_match_difference);
    assert_eq!(comparison.draft_page_type_match[0].candidates.len(), 2);
    assert!(comparison.draft_page_type_match[0].winner.is_none());
    Ok(())
}

#[tokio::test]
async fn transition_comparison_uses_unique_semantic_counterpart()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = transition_fixture(2).await?;
    let repository = CrawlerRepository::new(&fixture.database);
    let published = repository
        .publish(
            fixture.crawler.id(),
            fixture.version_id,
            "operator",
            "unix:8",
        )
        .await?;
    let draft = repository
        .create_draft_from_published(
            fixture.crawler.id(),
            published.version.id(),
            "operator",
            "unix:9",
        )
        .await?;
    let draft_transition = repository
        .list_discovery_transitions(fixture.crawler.id(), draft.id())
        .await?
        .into_iter()
        .find(|record| record.transition.name == "Product links")
        .ok_or("missing cloned transition")?
        .transition;
    let evidence = TestLabService::new(
        fixture.database,
        Some(Arc::new(FixtureTestLabProvider::new([observation(
            "https://example.test/listing",
            vec![ObservedLink {
                raw_href: "/products/1".to_owned(),
                selector: Some("a.product".to_owned()),
            }],
        )]))),
        None,
    )
    .execute(
        fixture.crawler.id(),
        draft.id(),
        transition_request(draft_transition.id, true),
    )
    .await?
    .evidence;
    assert_eq!(
        evidence
            .published_comparison
            .as_ref()
            .and_then(|comparison| comparison.discovery_difference),
        Some(false)
    );
    assert!(
        !evidence
            .warnings
            .iter()
            .any(|warning| { warning.code == "PUBLISHED_TRANSITION_CORRESPONDENCE_UNAVAILABLE" })
    );
    Ok(())
}

struct RecordingExtractionHook {
    observations: Arc<Mutex<Vec<(PageTypeId, PageObservation)>>>,
    field_values: BTreeMap<String, bool>,
}

impl ExtractionTestHook for RecordingExtractionHook {
    fn evaluate(
        &self,
        request: ExtractionTestRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ExtractionObservation> + Send + '_>>
    {
        self.observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((request.page_type_id, request.observation));
        let observed = self
            .field_values
            .get(&request.page_type_id.to_string())
            .copied()
            .unwrap_or(true);
        Box::pin(async move {
            ExtractionObservation::Available {
                fields: vec![ExtractionFieldEvidence {
                    name: "title".to_owned(),
                    observed,
                }],
            }
        })
    }
}

async fn extraction_clone_fixture(
    duplicate_published_page_type: bool,
) -> Result<
    (
        ErabiDatabase,
        Crawler,
        CrawlerVersionId,
        CrawlerVersionId,
        PageTypeId,
    ),
    Box<dyn std::error::Error>,
> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("extraction comparison");
    repository.create(&crawler).await?;
    let version = repository
        .create_draft(crawler.id(), "operator", "unix:1")
        .await?;
    let page_type = repository
        .create_page_type(
            crawler.id(),
            version.id(),
            "Product",
            1,
            "operator",
            "unix:2",
        )
        .await?;
    repository
        .create_url_matcher(
            crawler.id(),
            version.id(),
            page_type.id,
            &UrlMatcher::path_prefix(None, "/products"),
            "operator",
            "unix:3",
        )
        .await?;
    if duplicate_published_page_type {
        let duplicate = repository
            .create_page_type(
                crawler.id(),
                version.id(),
                "Product",
                1,
                "operator",
                "unix:4",
            )
            .await?;
        repository
            .create_url_matcher(
                crawler.id(),
                version.id(),
                duplicate.id,
                &UrlMatcher::path_prefix(None, "/products"),
                "operator",
                "unix:5",
            )
            .await?;
    }
    let published = repository
        .publish(crawler.id(), version.id(), "operator", "unix:6")
        .await?;
    let draft = repository
        .create_draft_from_published(crawler.id(), published.version.id(), "operator", "unix:7")
        .await?;
    let draft_page_type = repository
        .list_page_types(crawler.id(), draft.id())
        .await?
        .into_iter()
        .next()
        .ok_or("missing cloned PageType")?;
    Ok((
        database,
        crawler,
        published.version.id(),
        draft.id(),
        draft_page_type.id,
    ))
}

#[tokio::test]
async fn extraction_comparison_reuses_one_observation_and_compares_stably()
-> Result<(), Box<dyn std::error::Error>> {
    let (database, crawler, _published_id, draft_id, draft_page_type_id) =
        extraction_clone_fixture(false).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let service = TestLabService::new(
        database,
        Some(Arc::new(CountingProvider {
            calls: Arc::clone(&calls),
            page: observation("https://example.test/products/1", Vec::new()),
        })),
        Some(Arc::new(RecordingExtractionHook {
            observations: Arc::clone(&observed),
            field_values: BTreeMap::new(),
        })),
    );
    let evidence = service
        .execute(
            crawler.id(),
            draft_id,
            TestLabRequest {
                test_kind: TestKind::Extraction,
                input_urls: vec!["https://example.test/products/1".to_owned()],
                page_type_id: Some(draft_page_type_id),
                transition_id: None,
                compare_with_active_published: true,
                reuse_artifact_ids: Vec::new(),
            },
        )
        .await?
        .evidence;
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        evidence
            .published_comparison
            .as_ref()
            .and_then(|comparison| comparison.extraction_difference),
        Some(false)
    );
    let observed = observed.lock().map_err(|_| "recording hook lock")?;
    assert_eq!(observed.len(), 2);
    assert_ne!(observed[0].0, observed[1].0);
    assert_eq!(observed[0].1, observed[1].1);
    Ok(())
}

#[tokio::test]
async fn extraction_difference_and_unresolved_counterpart_are_explicit()
-> Result<(), Box<dyn std::error::Error>> {
    let (database, crawler, published_id, draft_id, draft_page_type_id) =
        extraction_clone_fixture(false).await?;
    let repository = CrawlerRepository::new(&database);
    let published_page_type_id = repository
        .list_page_types(crawler.id(), published_id)
        .await?
        .into_iter()
        .next()
        .ok_or("missing Published PageType")?
        .id;
    let service = TestLabService::new(
        database.clone(),
        Some(Arc::new(FixtureTestLabProvider::new([observation(
            "https://example.test/products/1",
            Vec::new(),
        )]))),
        Some(Arc::new(RecordingExtractionHook {
            observations: Arc::new(Mutex::new(Vec::new())),
            field_values: BTreeMap::from([(published_page_type_id.to_string(), false)]),
        })),
    );
    let request = TestLabRequest {
        test_kind: TestKind::Extraction,
        input_urls: vec!["https://example.test/products/1".to_owned()],
        page_type_id: Some(draft_page_type_id),
        transition_id: None,
        compare_with_active_published: true,
        reuse_artifact_ids: Vec::new(),
    };
    let different = service
        .execute(crawler.id(), draft_id, request.clone())
        .await?
        .evidence;
    assert_eq!(
        different
            .published_comparison
            .as_ref()
            .and_then(|comparison| comparison.extraction_difference),
        Some(true)
    );
    let (
        unresolved_database,
        unresolved_crawler,
        _published_id,
        unresolved_draft_id,
        unresolved_page_type_id,
    ) = extraction_clone_fixture(true).await?;
    let unresolved = TestLabService::new(
        unresolved_database,
        Some(Arc::new(FixtureTestLabProvider::new([observation(
            "https://example.test/products/1",
            Vec::new(),
        )]))),
        Some(Arc::new(RecordingExtractionHook {
            observations: Arc::new(Mutex::new(Vec::new())),
            field_values: BTreeMap::new(),
        })),
    )
    .execute(
        unresolved_crawler.id(),
        unresolved_draft_id,
        TestLabRequest {
            page_type_id: Some(unresolved_page_type_id),
            ..request
        },
    )
    .await?
    .evidence;
    assert_eq!(
        unresolved
            .published_comparison
            .as_ref()
            .and_then(|comparison| comparison.extraction_difference),
        None
    );
    assert!(
        unresolved
            .warnings
            .iter()
            .any(|warning| { warning.code == "PUBLISHED_PAGE_TYPE_CORRESPONDENCE_UNAVAILABLE" })
    );
    Ok(())
}

struct MismatchedProvider;

impl TestLabProvider for MismatchedProvider {
    fn observe(
        &self,
        _request: TestLabObservationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<PageObservation, TestLabProviderError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async { Ok(observation("https://example.test/other", Vec::new())) })
    }

    fn validate_reuse(
        &self,
        _artifact_id: erabi_domain::ArtifactId,
    ) -> Result<(), TestLabProviderError> {
        Ok(())
    }
}

#[tokio::test]
async fn provider_observation_cannot_be_attributed_to_another_requested_url()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = transition_fixture(1).await?;
    let service = TestLabService::new(
        fixture.database.clone(),
        Some(Arc::new(MismatchedProvider)),
        None,
    );
    let result = service
        .execute(
            fixture.crawler.id(),
            fixture.version_id,
            transition_request(fixture.transition_id, false),
        )
        .await;
    assert!(matches!(
        result,
        Err(TestLabError::ProviderObservationRequestMismatch)
    ));
    assert!(
        erabi_db::repositories::TestEvidenceRepository::new(&fixture.database)
            .list(fixture.crawler.id(), fixture.version_id)
            .await?
            .is_empty()
    );
    Ok(())
}
