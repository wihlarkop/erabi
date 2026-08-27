use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use erabi_crawler::{
    ExtractionTestHook, FixtureTestLabProvider, ObservedLink, PageObservation,
    PaginationObservation, SelectorObservation, TestLabError, TestLabRequest, TestLabService,
};
use erabi_db::{ErabiDatabase, MigrationRunner, repositories::CrawlerRepository};
use erabi_domain::{
    CanonicalizationPolicy, Crawler, CrawlerVersion, DiscoveryTransition, Seed, TestKind,
    TransitionBudget, UrlMatcher,
};
use std::collections::BTreeSet;
use tokio::sync::Notify;

async fn setup() -> Result<(ErabiDatabase, Crawler, CrawlerVersion), Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("Test Lab crawler");
    repository.create(&crawler).await?;
    let version = repository
        .create_draft(crawler.id(), "operator", "unix:1")
        .await?;
    Ok((database, crawler, version))
}

#[tokio::test]
async fn canonicalization_test_persists_typed_historical_evidence_without_provider()
-> Result<(), Box<dyn std::error::Error>> {
    let (database, crawler, version) = setup().await?;
    let service = TestLabService::new(database, None, None);
    let record = service
        .execute(
            crawler.id(),
            version.id(),
            TestLabRequest {
                test_kind: TestKind::UrlCanonicalization,
                input_urls: vec!["HTTPS://EXAMPLE.TEST:443/items#fragment".to_owned()],
                page_type_id: None,
                transition_id: None,
                compare_with_active_published: false,
                reuse_artifact_ids: Vec::new(),
            },
        )
        .await?;
    assert_eq!(record.evidence.test_kind, TestKind::UrlCanonicalization);
    assert_eq!(record.evidence.canonicalization.len(), 1);
    assert_eq!(
        record.evidence.canonicalization[0].original_url,
        "HTTPS://EXAMPLE.TEST:443/items#fragment"
    );
    assert_eq!(
        record.evidence.canonicalization[0].canonical_url.as_deref(),
        Some("https://example.test/items")
    );
    assert_eq!(record.evidence.config_hash.len(), 64);
    assert!(record.matches_current_configuration);
    Ok(())
}

#[tokio::test]
async fn provider_dependent_test_is_explicitly_unavailable_without_provider()
-> Result<(), Box<dyn std::error::Error>> {
    let (database, crawler, version) = setup().await?;
    let repository = CrawlerRepository::new(&database);
    let page = repository
        .create_page_type(crawler.id(), version.id(), "Item", 1, "operator", "unix:2")
        .await?;
    let service = TestLabService::new(database, None, None);
    let error = service
        .execute(
            crawler.id(),
            version.id(),
            TestLabRequest {
                test_kind: TestKind::SelectorCoverage,
                input_urls: vec!["https://example.test/items/1".to_owned()],
                page_type_id: Some(page.id),
                transition_id: None,
                compare_with_active_published: false,
                reuse_artifact_ids: Vec::new(),
            },
        )
        .await;
    assert!(matches!(error, Err(TestLabError::ProviderUnavailable)));
    Ok(())
}

struct StaticExtractionHook;

impl ExtractionTestHook for StaticExtractionHook {
    fn evaluate(
        &self,
        _request: erabi_crawler::ExtractionTestRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = erabi_domain::ExtractionObservation> + Send + '_>,
    > {
        Box::pin(async {
            erabi_domain::ExtractionObservation::Available {
                fields: vec![erabi_domain::ExtractionFieldEvidence {
                    name: "title".to_owned(),
                    observed: true,
                }],
            }
        })
    }
}

#[tokio::test]
async fn selector_pagination_and_extraction_observations_are_typed()
-> Result<(), Box<dyn std::error::Error>> {
    let (database, crawler, version) = setup().await?;
    let repository = CrawlerRepository::new(&database);
    let page_type = repository
        .create_page_type(crawler.id(), version.id(), "Item", 1, "operator", "unix:2")
        .await?;
    let provider = FixtureTestLabProvider::new([PageObservation {
        requested_url: "https://example.test/items".to_owned(),
        final_url: None,
        artifact_ids: Vec::new(),
        discovered_links: Vec::new(),
        selector_observations: vec![SelectorObservation {
            selector: ".missing".to_owned(),
            matches_found: 0,
        }],
        pagination_observations: vec![PaginationObservation {
            kind: erabi_domain::PaginationKind::RelNext,
            selector: Some("link[rel=next]".to_owned()),
            target_url: Some("https://example.test/items?page=2".to_owned()),
        }],
    }]);
    let service = TestLabService::new(
        database,
        Some(Arc::new(provider)),
        Some(Arc::new(StaticExtractionHook)),
    );
    let selector = service
        .execute(
            crawler.id(),
            version.id(),
            TestLabRequest {
                test_kind: TestKind::SelectorCoverage,
                input_urls: vec!["https://example.test/items".to_owned()],
                page_type_id: Some(page_type.id),
                transition_id: None,
                compare_with_active_published: false,
                reuse_artifact_ids: Vec::new(),
            },
        )
        .await?;
    assert_eq!(
        selector.evidence.selector_coverage[0].status,
        erabi_domain::SelectorCoverageStatus::NoMatches
    );
    assert!(
        selector
            .evidence
            .warnings
            .iter()
            .any(|warning| warning.code == "SELECTOR_NO_MATCHES")
    );

    let pagination = service
        .execute(
            crawler.id(),
            version.id(),
            TestLabRequest {
                test_kind: TestKind::Pagination,
                input_urls: vec!["https://example.test/items".to_owned()],
                page_type_id: None,
                transition_id: None,
                compare_with_active_published: false,
                reuse_artifact_ids: Vec::new(),
            },
        )
        .await?;
    assert_eq!(
        pagination
            .evidence
            .pagination
            .as_ref()
            .map(|value| value.kind),
        Some(erabi_domain::PaginationKind::RelNext)
    );

    let extraction = service
        .execute(
            crawler.id(),
            version.id(),
            TestLabRequest {
                test_kind: TestKind::Extraction,
                input_urls: vec!["https://example.test/items".to_owned()],
                page_type_id: Some(page_type.id),
                transition_id: None,
                compare_with_active_published: false,
                reuse_artifact_ids: Vec::new(),
            },
        )
        .await?;
    assert!(matches!(
        extraction.evidence.extraction,
        Some(erabi_domain::ExtractionObservation::Available { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn fixture_discovery_is_bounded_and_reuses_the_same_observation_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let (database, crawler, version) = setup().await?;
    let repository = CrawlerRepository::new(&database);
    let page = repository
        .create_page_type(crawler.id(), version.id(), "Item", 1, "operator", "unix:2")
        .await?;
    repository
        .create_url_matcher(
            crawler.id(),
            version.id(),
            page.id,
            &UrlMatcher::path_prefix(None, "/items"),
            "operator",
            "unix:3",
        )
        .await?;
    let mut current = repository
        .version(crawler.id(), version.id())
        .await
        .map_err(|error| format!("read version: {error:?}"))?
        .version;
    current.add_seed(Seed::new(
        "https://example.test/items".parse()?,
        "https://example.test/items".parse()?,
    ))?;
    repository
        .save_draft(&current, "operator", "unix:4")
        .await
        .map_err(|error| format!("save draft: {error:?}"))?;
    let observation = PageObservation {
        requested_url: "https://example.test/items".to_owned(),
        final_url: None,
        artifact_ids: Vec::new(),
        discovered_links: vec![
            ObservedLink {
                raw_href: "/items/2".to_owned(),
                selector: Some("a.item".to_owned()),
            },
            ObservedLink {
                raw_href: "items/2#top".to_owned(),
                selector: Some("a.item".to_owned()),
            },
        ],
        selector_observations: vec![SelectorObservation {
            selector: "a.item".to_owned(),
            matches_found: 2,
        }],
        pagination_observations: Vec::new(),
    };
    let provider = FixtureTestLabProvider::new([observation]);
    let service = TestLabService::new(database, Some(Arc::new(provider)), None);
    let record = service
        .execute(
            crawler.id(),
            current.id(),
            TestLabRequest {
                test_kind: TestKind::DiscoveredUrlPreview,
                input_urls: vec!["https://example.test/items".to_owned()],
                page_type_id: None,
                transition_id: None,
                compare_with_active_published: false,
                reuse_artifact_ids: Vec::new(),
            },
        )
        .await?;
    let discovery = record
        .evidence
        .discovery
        .ok_or("missing discovery evidence")?;
    assert_eq!(discovery.discovered_urls.len(), 2);
    assert!(discovery.discovered_urls[1].duplicate);
    assert_eq!(
        discovery.discovered_urls[0]
            .resolved_original_url
            .as_deref(),
        Some("https://example.test/items/2")
    );
    Ok(())
}

#[tokio::test]
async fn comparison_captures_the_active_published_identity_and_hash()
-> Result<(), Box<dyn std::error::Error>> {
    let (database, crawler, version) = setup().await?;
    let repository = CrawlerRepository::new(&database);
    let published = repository
        .publish(crawler.id(), version.id(), "operator", "unix:2")
        .await?;
    let draft = repository
        .create_draft_from_published(crawler.id(), published.version.id(), "operator", "unix:3")
        .await?;
    let changed_policy =
        CanonicalizationPolicy::new(BTreeSet::new(), BTreeSet::from(["campaign".to_owned()]))?;
    repository
        .update_canonicalization_policy(
            crawler.id(),
            draft.id(),
            &changed_policy,
            "operator",
            "unix:4",
        )
        .await?;
    let published_hash = repository
        .configuration_hash(crawler.id(), published.version.id())
        .await?;
    let service = TestLabService::new(database, None, None);
    let record = service
        .execute(
            crawler.id(),
            draft.id(),
            TestLabRequest {
                test_kind: TestKind::UrlCanonicalization,
                input_urls: vec!["https://example.test/items?campaign=spring".to_owned()],
                page_type_id: None,
                transition_id: None,
                compare_with_active_published: true,
                reuse_artifact_ids: Vec::new(),
            },
        )
        .await?;
    let comparison = record
        .evidence
        .published_comparison
        .ok_or("missing Published comparison")?;
    assert_eq!(
        comparison.status,
        erabi_domain::PublishedComparisonStatus::Compared
    );
    assert_eq!(
        comparison.published_version_id,
        Some(published.version.id())
    );
    assert_eq!(
        comparison.published_config_hash.as_deref(),
        Some(published_hash.as_str())
    );
    assert!(comparison.canonicalization_difference);
    Ok(())
}

#[tokio::test]
async fn comparison_without_published_version_is_explicit_and_durable()
-> Result<(), Box<dyn std::error::Error>> {
    let (database, crawler, version) = setup().await?;
    let service = TestLabService::new(database, None, None);
    let record = service
        .execute(
            crawler.id(),
            version.id(),
            TestLabRequest {
                test_kind: TestKind::UrlCanonicalization,
                input_urls: vec!["https://example.test/items".to_owned()],
                page_type_id: None,
                transition_id: None,
                compare_with_active_published: true,
                reuse_artifact_ids: Vec::new(),
            },
        )
        .await?;
    let comparison = record
        .evidence
        .published_comparison
        .ok_or("missing Published comparison")?;
    assert_eq!(
        comparison.status,
        erabi_domain::PublishedComparisonStatus::NoActivePublishedVersion
    );
    assert!(comparison.published_version_id.is_none());
    assert!(record.matches_current_configuration);
    Ok(())
}

struct CountingProvider {
    calls: Arc<AtomicUsize>,
}

impl erabi_crawler::TestLabProvider for CountingProvider {
    fn observe(
        &self,
        _request: erabi_crawler::TestLabObservationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<PageObservation, erabi_crawler::TestLabProviderError>,
                > + Send
                + '_,
        >,
    > {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {
            Ok(PageObservation {
                requested_url: "https://example.test/items".to_owned(),
                final_url: None,
                artifact_ids: Vec::new(),
                discovered_links: Vec::new(),
                selector_observations: Vec::new(),
                pagination_observations: Vec::new(),
            })
        })
    }

    fn validate_reuse(
        &self,
        _artifact_id: erabi_domain::ArtifactId,
    ) -> Result<(), erabi_crawler::TestLabProviderError> {
        Ok(())
    }
}

#[tokio::test]
async fn provider_backed_comparison_observes_one_page_once()
-> Result<(), Box<dyn std::error::Error>> {
    let (database, crawler, version) = setup().await?;
    let repository = CrawlerRepository::new(&database);
    let published = repository
        .publish(crawler.id(), version.id(), "operator", "unix:2")
        .await?;
    let draft = repository
        .create_draft_from_published(crawler.id(), published.version.id(), "operator", "unix:3")
        .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let service = TestLabService::new(
        database,
        Some(Arc::new(CountingProvider {
            calls: Arc::clone(&calls),
        })),
        None,
    );
    service
        .execute(
            crawler.id(),
            draft.id(),
            TestLabRequest {
                test_kind: TestKind::Pagination,
                input_urls: vec!["https://example.test/items".to_owned()],
                page_type_id: None,
                transition_id: None,
                compare_with_active_published: true,
                reuse_artifact_ids: Vec::new(),
            },
        )
        .await?;
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn transition_evidence_attaches_without_changing_hash_and_clone_clears_it()
-> Result<(), Box<dyn std::error::Error>> {
    let (database, crawler, version) = setup().await?;
    let repository = CrawlerRepository::new(&database);
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
    repository
        .create_url_matcher(
            crawler.id(),
            version.id(),
            source.id,
            &UrlMatcher::path_prefix(None, "/items"),
            "operator",
            "unix:4",
        )
        .await?;
    repository
        .create_url_matcher(
            crawler.id(),
            version.id(),
            target.id,
            &UrlMatcher::path_prefix(None, "/products"),
            "operator",
            "unix:5",
        )
        .await?;
    let mut current = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    current.add_seed(Seed::new(
        "https://example.test/items".parse()?,
        "https://example.test/items".parse()?,
    ))?;
    repository
        .save_draft(&current, "operator", "unix:6")
        .await?;
    let transition = DiscoveryTransition {
        id: erabi_domain::DiscoveryTransitionId::new(),
        source_page_type_id: source.id,
        target_page_type_id: target.id,
        name: "Product links".to_owned(),
        enabled: true,
        link_selector: "a.next".to_owned(),
        url_constraints: None,
        priority: 1,
        budget: TransitionBudget {
            max_links_per_source_page: 2,
            total_budget: None,
            depth_contribution: 1,
        },
        deduplicate: true,
        latest_test_evidence_id: None,
    };
    repository
        .create_discovery_transition(
            crawler.id(),
            current.id(),
            &transition,
            "operator",
            "unix:7",
        )
        .await?;
    let hash_before = repository
        .configuration_hash(crawler.id(), current.id())
        .await?;
    let provider = FixtureTestLabProvider::new([PageObservation {
        requested_url: "https://example.test/items".to_owned(),
        final_url: None,
        artifact_ids: Vec::new(),
        discovered_links: vec![ObservedLink {
            raw_href: "/products/1".to_owned(),
            selector: Some("a.next".to_owned()),
        }],
        selector_observations: vec![SelectorObservation {
            selector: "a.next".to_owned(),
            matches_found: 1,
        }],
        pagination_observations: Vec::new(),
    }]);
    let service = TestLabService::new(database.clone(), Some(Arc::new(provider)), None);
    let evidence = service
        .execute(
            crawler.id(),
            current.id(),
            TestLabRequest {
                test_kind: TestKind::DiscoveryTransition,
                input_urls: vec!["https://example.test/items".to_owned()],
                page_type_id: None,
                transition_id: Some(transition.id),
                compare_with_active_published: false,
                reuse_artifact_ids: Vec::new(),
            },
        )
        .await?;
    let attached = repository
        .discovery_transition(crawler.id(), current.id(), transition.id)
        .await?;
    assert_eq!(
        attached.transition.latest_test_evidence_id,
        Some(evidence.evidence.id)
    );
    assert_eq!(
        repository
            .configuration_hash(crawler.id(), current.id())
            .await?,
        hash_before
    );
    let published = repository
        .publish(crawler.id(), current.id(), "operator", "unix:8")
        .await?;
    let source_transition = repository
        .discovery_transition(crawler.id(), published.version.id(), transition.id)
        .await?;
    assert_eq!(
        source_transition.transition.latest_test_evidence_id,
        Some(evidence.evidence.id)
    );
    let cloned = repository
        .create_draft_from_published(crawler.id(), published.version.id(), "operator", "unix:9")
        .await?;
    let cloned_transition = repository
        .list_discovery_transitions(crawler.id(), cloned.id())
        .await?
        .into_iter()
        .next()
        .ok_or("missing cloned transition")?;
    assert!(
        cloned_transition
            .transition
            .latest_test_evidence_id
            .is_none()
    );
    assert_eq!(
        repository
            .configuration_hash(crawler.id(), published.version.id())
            .await?,
        repository
            .configuration_hash(crawler.id(), cloned.id())
            .await?
    );
    Ok(())
}

struct BlockingProvider {
    started: Arc<Notify>,
    release: Arc<Notify>,
    observation: PageObservation,
}

impl erabi_crawler::TestLabProvider for BlockingProvider {
    fn observe(
        &self,
        _request: erabi_crawler::TestLabObservationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<PageObservation, erabi_crawler::TestLabProviderError>,
                > + Send
                + '_,
        >,
    > {
        let started = Arc::clone(&self.started);
        let release = Arc::clone(&self.release);
        let observation = self.observation.clone();
        Box::pin(async move {
            started.notify_one();
            release.notified().await;
            Ok(observation)
        })
    }

    fn validate_reuse(
        &self,
        _artifact_id: erabi_domain::ArtifactId,
    ) -> Result<(), erabi_crawler::TestLabProviderError> {
        Ok(())
    }
}

#[tokio::test]
async fn draft_change_during_provider_execution_persists_no_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let (database, crawler, version) = setup().await?;
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let provider = BlockingProvider {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        observation: PageObservation {
            requested_url: "https://example.test/items".to_owned(),
            final_url: None,
            artifact_ids: Vec::new(),
            discovered_links: Vec::new(),
            selector_observations: Vec::new(),
            pagination_observations: Vec::new(),
        },
    };
    let service = Arc::new(TestLabService::new(
        database.clone(),
        Some(Arc::new(provider)),
        None,
    ));
    let crawler_id = crawler.id();
    let version_id = version.id();
    let task = tokio::spawn({
        let service = Arc::clone(&service);
        async move {
            service
                .execute(
                    crawler_id,
                    version_id,
                    TestLabRequest {
                        test_kind: TestKind::Pagination,
                        input_urls: vec!["https://example.test/items".to_owned()],
                        page_type_id: None,
                        transition_id: None,
                        compare_with_active_published: false,
                        reuse_artifact_ids: Vec::new(),
                    },
                )
                .await
        }
    });
    started.notified().await;
    let repository = CrawlerRepository::new(&database);
    let changed = repository
        .update_canonicalization_policy(
            crawler.id(),
            version.id(),
            &CanonicalizationPolicy::new(BTreeSet::new(), BTreeSet::from(["campaign".to_owned()]))?,
            "operator",
            "unix:5",
        )
        .await;
    assert!(changed.is_ok());
    release.notify_one();
    let result = task.await?;
    assert!(matches!(result, Err(TestLabError::ConfigurationChanged)));
    let evidence = erabi_db::repositories::TestEvidenceRepository::new(&database)
        .list(crawler.id(), version.id())
        .await?;
    assert!(evidence.is_empty());
    Ok(())
}
