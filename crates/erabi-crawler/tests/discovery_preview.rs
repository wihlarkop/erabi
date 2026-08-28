#![allow(clippy::too_many_lines)]

use std::sync::{Arc, Mutex};

use erabi_crawler::{
    DiscoveryPreviewObservationRequest, DiscoveryPreviewProvider, DiscoveryPreviewProviderError,
    DiscoveryPreviewProviderOutcome, DiscoveryPreviewService, FixtureDiscoveryPreviewProvider,
    ManualPreviewClock, ObservedLink, PageObservation,
};
use erabi_db::{ErabiDatabase, MigrationRunner, repositories::CrawlerRepository};
use erabi_domain::{
    Crawler, CrawlerVersion, DiscoveryPreviewLimits, DiscoveryPreviewRequest,
    DiscoveryPreviewResultSemantics, DiscoveryTransition, DiscoveryTransitionId,
    PageTypeDiscoveryGuardrails, Seed, TestDiagnostic, TransitionBudget,
    TransitionPreviewTotalLimit, UrlMatcher,
};

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

fn limits() -> DiscoveryPreviewLimits {
    DiscoveryPreviewLimits {
        max_pages: 20,
        max_depth: 8,
        max_duration_ms: 10_000,
        default_transition_total_limit: 20,
        transition_total_limits: Vec::new(),
    }
}

async fn graph_fixture(
    database: &ErabiDatabase,
) -> Result<
    (
        Crawler,
        CrawlerVersion,
        erabi_domain::Seed,
        erabi_domain::Seed,
        erabi_domain::PageTypeId,
        erabi_domain::PageTypeId,
        erabi_domain::DiscoveryTransitionId,
        erabi_domain::DiscoveryTransitionId,
    ),
    Box<dyn std::error::Error>,
> {
    let repository = CrawlerRepository::new(database);
    let crawler = Crawler::new("Discovery Preview crawler");
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
    let mut seed_a = Seed::new(
        "https://example.test/listing/a".parse()?,
        "https://example.test/listing/a".parse()?,
    );
    let seed_b = Seed::new(
        "https://example.test/listing/b".parse()?,
        "https://example.test/listing/b".parse()?,
    );
    seed_a.entry_page_type_hint = Some(listing.id);
    let mut current = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    current.add_seed(seed_a.clone())?;
    current.add_seed(seed_b.clone())?;
    repository
        .save_draft(&current, "operator", "unix:6")
        .await?;
    let listing_to_product = DiscoveryTransition {
        id: erabi_domain::DiscoveryTransitionId::new(),
        source_page_type_id: listing.id,
        target_page_type_id: product.id,
        name: "listing products".to_owned(),
        enabled: true,
        link_selector: "a.product".to_owned(),
        url_constraints: None,
        priority: 20,
        budget: TransitionBudget {
            max_links_per_source_page: 10,
            total_budget: Some(20),
            depth_contribution: 1,
        },
        deduplicate: false,
        latest_test_evidence_id: None,
    };
    let listing_to_listing = DiscoveryTransition {
        id: erabi_domain::DiscoveryTransitionId::new(),
        source_page_type_id: listing.id,
        target_page_type_id: listing.id,
        name: "listing cycle".to_owned(),
        enabled: true,
        link_selector: "a.next".to_owned(),
        url_constraints: None,
        priority: 10,
        budget: TransitionBudget {
            max_links_per_source_page: 10,
            total_budget: Some(20),
            depth_contribution: 0,
        },
        deduplicate: true,
        latest_test_evidence_id: None,
    };
    repository
        .create_discovery_transition(
            crawler.id(),
            version.id(),
            &listing_to_product,
            "operator",
            "unix:7",
        )
        .await?;
    repository
        .create_discovery_transition(
            crawler.id(),
            version.id(),
            &listing_to_listing,
            "operator",
            "unix:8",
        )
        .await?;
    let version = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    Ok((
        crawler,
        version,
        seed_a,
        seed_b,
        listing.id,
        product.id,
        listing_to_product.id,
        listing_to_listing.id,
    ))
}

fn page(requested_url: &str, links: Vec<ObservedLink>) -> PageObservation {
    PageObservation {
        requested_url: requested_url.to_owned(),
        final_url: None,
        artifact_ids: Vec::new(),
        discovered_links: links,
        selector_observations: Vec::new(),
        pagination_observations: Vec::new(),
    }
}

fn preview_request(seed_ids: Vec<erabi_domain::SeedId>) -> DiscoveryPreviewRequest {
    DiscoveryPreviewRequest {
        seed_ids,
        limits: limits(),
    }
}

struct AdvancingProvider {
    inner: FixtureDiscoveryPreviewProvider,
    clock: Arc<ManualPreviewClock>,
}

struct RecordingProvider {
    inner: FixtureDiscoveryPreviewProvider,
    requested_urls: Mutex<Vec<String>>,
}

impl RecordingProvider {
    fn observed(pages: impl IntoIterator<Item = PageObservation>, downloaded_bytes: u64) -> Self {
        Self {
            inner: FixtureDiscoveryPreviewProvider::observed(pages, downloaded_bytes),
            requested_urls: Mutex::new(Vec::new()),
        }
    }

    fn requested_urls(&self) -> Vec<String> {
        match self.requested_urls.lock() {
            Ok(requested_urls) => requested_urls.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

impl DiscoveryPreviewProvider for RecordingProvider {
    fn observe(
        &self,
        request: DiscoveryPreviewObservationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<DiscoveryPreviewProviderOutcome, DiscoveryPreviewProviderError>,
                > + Send
                + '_,
        >,
    > {
        match self.requested_urls.lock() {
            Ok(mut requested_urls) => requested_urls.push(request.requested_url.clone()),
            Err(poisoned) => poisoned.into_inner().push(request.requested_url.clone()),
        }
        self.inner.observe(request)
    }
}

fn transition(
    source_page_type_id: erabi_domain::PageTypeId,
    target_page_type_id: erabi_domain::PageTypeId,
    name: &str,
    selector: &str,
    depth_contribution: u32,
) -> DiscoveryTransition {
    DiscoveryTransition {
        id: DiscoveryTransitionId::new(),
        source_page_type_id,
        target_page_type_id,
        name: name.to_owned(),
        enabled: true,
        link_selector: selector.to_owned(),
        url_constraints: None,
        priority: 0,
        budget: TransitionBudget {
            max_links_per_source_page: 20,
            total_budget: Some(20),
            depth_contribution,
        },
        deduplicate: true,
        latest_test_evidence_id: None,
    }
}

impl DiscoveryPreviewProvider for AdvancingProvider {
    fn observe(
        &self,
        request: DiscoveryPreviewObservationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<DiscoveryPreviewProviderOutcome, DiscoveryPreviewProviderError>,
                > + Send
                + '_,
        >,
    > {
        self.clock.advance_millis(100);
        self.inner.observe(request)
    }
}

#[tokio::test]
async fn multi_seed_bfs_is_bounded_and_cycles_do_not_resample()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, seed_b, _listing, _product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let provider = Arc::new(RecordingProvider::observed(
        [
            page(
                seed_a.original_url.as_str(),
                vec![
                    ObservedLink {
                        raw_href: "/product/1".to_owned(),
                        selector: Some("a.product".to_owned()),
                    },
                    ObservedLink {
                        raw_href: "/listing/b".to_owned(),
                        selector: Some("a.next".to_owned()),
                    },
                ],
            ),
            page(
                seed_b.original_url.as_str(),
                vec![ObservedLink {
                    raw_href: "/listing/a".to_owned(),
                    selector: Some("a.next".to_owned()),
                }],
            ),
            page("https://example.test/product/1", vec![]),
        ],
        10,
    ));
    let service = DiscoveryPreviewService::new(database, Some(provider.clone()));
    let result = service
        .execute(
            crawler.id(),
            version.id(),
            preview_request(vec![seed_a.id, seed_b.id]),
        )
        .await?;
    assert_eq!(
        result.result_semantics,
        DiscoveryPreviewResultSemantics::PreviewOnly
    );
    assert_eq!(result.selected_seed_ids, vec![seed_a.id, seed_b.id]);
    assert_eq!(result.summary.pages_sampled, 3);
    assert!(result.summary.duplicates_prevented >= 2);
    assert!(result.summary.canonical_unique_urls >= 3);
    assert_eq!(result.summary.frontier_remaining, 0);
    assert!(
        result
            .discovery_paths
            .iter()
            .any(|path| path.state == erabi_domain::PreviewUrlState::CanonicalDuplicate)
    );
    assert_eq!(
        provider
            .requested_urls()
            .iter()
            .filter(|url| url.as_str() == seed_a.original_url.as_str())
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn canonical_equivalent_selected_roots_share_one_admitted_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, _listing, _product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let alias_seed = Seed::new(
        "https://example.test/listing/a#selected-alias".parse()?,
        seed_a.canonical_url.clone(),
    );
    let mut edited = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    edited.add_seed(alias_seed.clone())?;
    repository
        .save_draft(&edited, "operator", "unix:11")
        .await?;
    let version = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    let provider =
        FixtureDiscoveryPreviewProvider::observed([page(seed_a.original_url.as_str(), vec![])], 10);
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(provider)));
    let result = service
        .execute(
            crawler.id(),
            version.id(),
            preview_request(vec![seed_a.id, alias_seed.id]),
        )
        .await?;
    assert_eq!(result.summary.pages_sampled, 1);
    assert_eq!(result.pages[0].seed_ids, vec![seed_a.id, alias_seed.id]);
    assert!(
        result
            .seeds
            .iter()
            .all(|seed| { seed.state == erabi_domain::PreviewUrlState::Sampled })
    );
    Ok(())
}

#[tokio::test]
async fn robots_external_unmatched_and_ambiguous_are_preserved_without_traversal()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, listing, product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let ambiguous = repository
        .create_page_type(
            crawler.id(),
            version.id(),
            "Also Listing",
            10,
            "operator",
            "unix:9",
        )
        .await?;
    repository
        .create_url_matcher(
            crawler.id(),
            version.id(),
            listing,
            &UrlMatcher::path_prefix(Some("example.test".to_owned()), "/ambiguous"),
            "operator",
            "unix:10b",
        )
        .await?;
    repository
        .create_url_matcher(
            crawler.id(),
            version.id(),
            product,
            &UrlMatcher::exact_url("https://example.test/robots".parse()?),
            "operator",
            "unix:11",
        )
        .await?;
    repository
        .create_url_matcher(
            crawler.id(),
            version.id(),
            ambiguous.id,
            &UrlMatcher::path_prefix(Some("example.test".to_owned()), "/ambiguous"),
            "operator",
            "unix:10",
        )
        .await?;
    let provider = FixtureDiscoveryPreviewProvider::observed(
        [page(
            seed_a.original_url.as_str(),
            vec![
                ObservedLink {
                    raw_href: "https://outside.test/item".to_owned(),
                    selector: Some("a.product".to_owned()),
                },
                ObservedLink {
                    raw_href: "/unmatched".to_owned(),
                    selector: Some("a.product".to_owned()),
                },
                ObservedLink {
                    raw_href: "/ambiguous".to_owned(),
                    selector: Some("a.product".to_owned()),
                },
                ObservedLink {
                    raw_href: "/robots".to_owned(),
                    selector: Some("a.product".to_owned()),
                },
            ],
        )],
        10,
    )
    .with_robots_excluded("https://example.test/robots", "robots policy");
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(provider)));
    let result = service
        .execute(crawler.id(), version.id(), preview_request(vec![seed_a.id]))
        .await?;
    assert_eq!(result.summary.pages_sampled, 1);
    assert_eq!(result.summary.external_urls, 1);
    assert_eq!(result.summary.robots_excluded, 1);
    assert!(result.summary.unmatched_urls >= 1);
    assert!(result.summary.ambiguous_urls >= 1);
    assert!(
        result
            .pages
            .iter()
            .all(|page| page.requested_url != "https://outside.test/item")
    );
    assert!(
        result
            .discovery_paths
            .iter()
            .any(|path| path.state == erabi_domain::PreviewUrlState::External)
    );
    assert!(
        result
            .discovery_paths
            .iter()
            .any(|path| path.state == erabi_domain::PreviewUrlState::AmbiguousPageType)
    );
    assert!(
        result
            .discovery_paths
            .iter()
            .any(|path| path.state == erabi_domain::PreviewUrlState::Unmatched)
    );
    assert_eq!(
        result.pages[0]
            .page_type_match
            .as_ref()
            .and_then(|page_match| page_match.winner.as_ref())
            .map(|winner| winner.page_type_id),
        Some(listing)
    );
    Ok(())
}

#[tokio::test]
async fn preview_limits_and_time_clock_are_explicit_and_safe()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(
        DiscoveryPreviewLimits {
            max_pages: 0,
            ..limits()
        }
        .validate()
        .is_err()
    );
    assert!(
        DiscoveryPreviewLimits {
            max_duration_ms: 0,
            ..limits()
        }
        .validate()
        .is_err()
    );
    assert!(
        DiscoveryPreviewLimits {
            default_transition_total_limit: 0,
            ..limits()
        }
        .validate()
        .is_err()
    );
    assert!(
        DiscoveryPreviewLimits {
            max_depth: 0,
            ..limits()
        }
        .validate()
        .is_ok()
    );
    let duplicate_id = DiscoveryTransitionId::new();
    assert!(
        DiscoveryPreviewLimits {
            transition_total_limits: vec![
                TransitionPreviewTotalLimit {
                    transition_id: duplicate_id,
                    max_total_links: 1,
                },
                TransitionPreviewTotalLimit {
                    transition_id: duplicate_id,
                    max_total_links: 2,
                },
            ],
            ..limits()
        }
        .validate()
        .is_err()
    );

    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, _listing, _product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let clock = Arc::new(ManualPreviewClock::new());
    let provider = FixtureDiscoveryPreviewProvider::observed(
        [page(
            seed_a.original_url.as_str(),
            vec![ObservedLink {
                raw_href: "/product/not-enqueued-after-time-cap".to_owned(),
                selector: Some("a.product".to_owned()),
            }],
        )],
        1,
    );
    let service = DiscoveryPreviewService::with_clock(
        database,
        Some(Arc::new(AdvancingProvider {
            inner: provider,
            clock: clock.clone(),
        })),
        clock,
    );
    let mut request = preview_request(vec![seed_a.id]);
    request.limits.max_duration_ms = 50;
    let result = service.execute(crawler.id(), version.id(), request).await?;
    assert_eq!(result.summary.pages_sampled, 1);
    assert_eq!(
        result
            .summary
            .budget_hit_counts
            .get(&erabi_domain::PreviewBudgetKind::MaxDuration),
        Some(&1)
    );
    assert_eq!(result.summary.frontier_remaining, 0);
    assert!(
        result
            .warnings
            .iter()
            .any(|diagnostic| { diagnostic.code == "PREVIEW_TIME_BUDGET_LINKS_NOT_EXPANDED" })
    );
    Ok(())
}

#[tokio::test]
async fn semantic_guardrails_tighten_preview_and_provider_receives_remaining_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, listing, _product, transition, _cycle) =
        graph_fixture(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let mut guardrails = repository
        .crawler_version_guardrails(crawler.id(), version.id())
        .await?;
    guardrails.max_pages = 1;
    guardrails.max_depth = 1;
    guardrails.max_duration_seconds = 1;
    guardrails.max_downloaded_bytes = 5;
    repository
        .update_crawler_version_guardrails(
            crawler.id(),
            version.id(),
            &guardrails,
            "operator",
            "unix:12",
        )
        .await?;
    let provider = FixtureDiscoveryPreviewProvider::observed(
        [page(seed_a.original_url.as_str(), Vec::new())],
        1,
    );
    let service = DiscoveryPreviewService::new(database.clone(), Some(Arc::new(provider)));
    let mut request = preview_request(vec![seed_a.id]);
    request.limits.max_pages = 10;
    request.limits.max_depth = 10;
    request.limits.max_duration_ms = 10_000;
    request.limits.default_transition_total_limit = 10;
    request.limits.transition_total_limits = vec![TransitionPreviewTotalLimit {
        transition_id: transition,
        max_total_links: 20,
    }];
    let result = service.execute(crawler.id(), version.id(), request).await?;
    assert_eq!(result.effective_limits.max_pages, 1);
    assert_eq!(result.effective_limits.max_depth, 1);
    assert_eq!(result.effective_limits.max_duration_ms, 1_000);
    assert_eq!(result.effective_limits.max_downloaded_bytes, 5);
    assert_eq!(
        result
            .effective_limits
            .transition_total_limits
            .iter()
            .find(|limit| limit.transition_id == transition)
            .map(|limit| limit.effective_total_limit),
        Some(20)
    );
    let listing_distribution = result
        .summary
        .page_type_distribution
        .iter()
        .find(|distribution| distribution.page_type_id == listing)
        .ok_or("missing Listing distribution")?;
    assert_eq!(listing_distribution.sampled_pages, 1);
    assert_eq!(listing_distribution.discovered_unique_urls, 0);
    let mut foreign_request = preview_request(vec![seed_a.id]);
    foreign_request.limits.transition_total_limits = vec![TransitionPreviewTotalLimit {
        transition_id: DiscoveryTransitionId::new(),
        max_total_links: 1,
    }];
    assert!(matches!(
        service
            .execute(crawler.id(), version.id(), foreign_request)
            .await,
        Err(erabi_crawler::DiscoveryPreviewError::TransitionNotOwnedByVersion)
    ));
    assert!(matches!(
        service
            .execute(
                crawler.id(),
                version.id(),
                preview_request(vec![seed_a.id, seed_a.id])
            )
            .await,
        Err(erabi_crawler::DiscoveryPreviewError::DuplicateSeedSelection)
    ));

    let invalid_provider = FixtureDiscoveryPreviewProvider::observed(
        [page(seed_a.original_url.as_str(), Vec::new())],
        6,
    );
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(invalid_provider)));
    assert!(matches!(
        service
            .execute(crawler.id(), version.id(), preview_request(vec![seed_a.id]))
            .await,
        Err(erabi_crawler::DiscoveryPreviewError::ProviderObservationInvalid)
    ));
    Ok(())
}

#[tokio::test]
async fn redirect_final_url_is_the_source_of_truth_and_aliases_do_not_expand_twice()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, listing, _product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let product = repository
        .list_page_types(crawler.id(), version.id())
        .await?
        .into_iter()
        .find(|page_type| page_type.name == "Product")
        .ok_or("missing Product PageType")?;
    let redirect_identity: url::Url = "https://example.test/product/redirect".parse()?;
    let redirect_seed = Seed::new(redirect_identity.clone(), redirect_identity.clone());
    let mut updated_version = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    updated_version.add_seed(redirect_seed.clone())?;
    repository
        .save_draft(&updated_version, "operator", "unix:12b")
        .await?;
    let redirect_transition = DiscoveryTransition {
        id: DiscoveryTransitionId::new(),
        source_page_type_id: product.id,
        target_page_type_id: listing,
        name: "redirected product back link".to_owned(),
        enabled: true,
        link_selector: "a.back".to_owned(),
        url_constraints: None,
        priority: 5,
        budget: TransitionBudget {
            max_links_per_source_page: 10,
            total_budget: Some(10),
            depth_contribution: 1,
        },
        deduplicate: true,
        latest_test_evidence_id: None,
    };
    repository
        .create_discovery_transition(
            crawler.id(),
            version.id(),
            &redirect_transition,
            "operator",
            "unix:13",
        )
        .await?;
    let root = PageObservation {
        requested_url: seed_a.original_url.to_string(),
        final_url: Some(redirect_identity.to_string()),
        artifact_ids: Vec::new(),
        discovered_links: vec![ObservedLink {
            raw_href: "/listing/b".to_owned(),
            selector: Some("a.back".to_owned()),
        }],
        selector_observations: Vec::new(),
        pagination_observations: Vec::new(),
    };
    let provider = FixtureDiscoveryPreviewProvider::observed([root], 1);
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(provider)));
    let result = service
        .execute(
            crawler.id(),
            version.id(),
            preview_request(vec![seed_a.id, redirect_seed.id]),
        )
        .await?;
    assert_eq!(result.summary.pages_sampled, 1);
    assert_eq!(
        result.pages[0].final_url.as_deref(),
        Some("https://example.test/product/redirect")
    );
    assert_eq!(
        result.pages[0]
            .page_type_match
            .as_ref()
            .and_then(|page_match| page_match.winner.as_ref())
            .map(|winner| winner.page_type_id),
        Some(product.id)
    );
    assert!(result.summary.duplicates_prevented >= 1);
    Ok(())
}

#[tokio::test]
async fn discovered_page_type_reservations_are_counted_once_at_the_budget_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, listing, product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let product_to_product = transition(product, product, "related products", "a.related", 1);
    repository
        .create_discovery_transition(
            crawler.id(),
            version.id(),
            &product_to_product,
            "operator",
            "unix:14",
        )
        .await?;
    let mut guardrails = repository
        .crawler_version_guardrails(crawler.id(), version.id())
        .await?;
    guardrails.page_types = vec![PageTypeDiscoveryGuardrails {
        page_type_id: product,
        page_budget: Some(2),
        health_threshold: None,
    }];
    repository
        .update_crawler_version_guardrails(
            crawler.id(),
            version.id(),
            &guardrails,
            "operator",
            "unix:15",
        )
        .await?;
    let provider = FixtureDiscoveryPreviewProvider::observed(
        [
            page(
                seed_a.original_url.as_str(),
                vec![ObservedLink {
                    raw_href: "/product/1".to_owned(),
                    selector: Some("a.product".to_owned()),
                }],
            ),
            page(
                "https://example.test/product/1",
                vec![ObservedLink {
                    raw_href: "/product/2".to_owned(),
                    selector: Some("a.related".to_owned()),
                }],
            ),
            page(
                "https://example.test/product/2",
                vec![ObservedLink {
                    raw_href: "/product/3".to_owned(),
                    selector: Some("a.related".to_owned()),
                }],
            ),
        ],
        1,
    );
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(provider)));
    let result = service
        .execute(crawler.id(), version.id(), preview_request(vec![seed_a.id]))
        .await?;

    assert_eq!(result.summary.pages_sampled, 3);
    let product_distribution = result
        .summary
        .page_type_distribution
        .iter()
        .find(|distribution| distribution.page_type_id == product)
        .ok_or("missing Product distribution")?;
    assert_eq!(product_distribution.discovered_unique_urls, 3);
    assert_eq!(product_distribution.sampled_pages, 2);
    assert!(result.discovery_paths.iter().any(|path| {
        path.canonical_url.as_deref() == Some("https://example.test/product/2")
            && path.state == erabi_domain::PreviewUrlState::InScopeMatched
    }));
    assert!(result.discovery_paths.iter().any(|path| {
        path.canonical_url.as_deref() == Some("https://example.test/product/3")
            && path.state == erabi_domain::PreviewUrlState::BudgetExcluded
    }));
    assert_eq!(
        result
            .summary
            .budget_hit_counts
            .get(&erabi_domain::PreviewBudgetKind::PageTypePageBudget),
        Some(&1)
    );
    assert_eq!(
        listing,
        result.pages[0]
            .page_type_match
            .as_ref()
            .and_then(|matched| { matched.winner.as_ref().map(|winner| winner.page_type_id) })
            .ok_or("root has no Listing match")?
    );
    Ok(())
}

#[tokio::test]
async fn selected_roots_are_sampled_not_discovered_and_do_not_reserve_their_hint()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, _seed_a, _seed_b, listing, product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let mut hinted_root = Seed::new(
        "https://example.test/listing/hinted-root".parse()?,
        "https://example.test/listing/hinted-root".parse()?,
    );
    hinted_root.entry_page_type_hint = Some(product);
    let mut current = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    current.add_seed(hinted_root.clone())?;
    repository
        .save_draft(&current, "operator", "unix:16")
        .await?;
    let version = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    let mut guardrails = repository
        .crawler_version_guardrails(crawler.id(), version.id())
        .await?;
    guardrails.page_types = vec![PageTypeDiscoveryGuardrails {
        page_type_id: product,
        page_budget: Some(1),
        health_threshold: None,
    }];
    repository
        .update_crawler_version_guardrails(
            crawler.id(),
            version.id(),
            &guardrails,
            "operator",
            "unix:17",
        )
        .await?;
    let provider = FixtureDiscoveryPreviewProvider::observed(
        [
            page(
                hinted_root.original_url.as_str(),
                vec![ObservedLink {
                    raw_href: "/product/from-root".to_owned(),
                    selector: Some("a.product".to_owned()),
                }],
            ),
            page("https://example.test/product/from-root", Vec::new()),
        ],
        1,
    );
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(provider)));
    let result = service
        .execute(
            crawler.id(),
            version.id(),
            preview_request(vec![hinted_root.id]),
        )
        .await?;

    assert_eq!(result.summary.pages_sampled, 2);
    let listing_distribution = result
        .summary
        .page_type_distribution
        .iter()
        .find(|distribution| distribution.page_type_id == listing)
        .ok_or("missing Listing distribution")?;
    assert_eq!(listing_distribution.sampled_pages, 1);
    assert_eq!(listing_distribution.discovered_unique_urls, 0);
    let product_distribution = result
        .summary
        .page_type_distribution
        .iter()
        .find(|distribution| distribution.page_type_id == product)
        .ok_or("missing Product distribution")?;
    assert_eq!(product_distribution.discovered_unique_urls, 1);
    assert_eq!(product_distribution.sampled_pages, 1);
    Ok(())
}

#[tokio::test]
async fn discovered_page_type_distribution_tracks_unique_hrefs_separately_from_samples()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, listing, product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let provider = FixtureDiscoveryPreviewProvider::observed(
        [
            page(
                seed_a.original_url.as_str(),
                vec![
                    ObservedLink {
                        raw_href: "/product/one".to_owned(),
                        selector: Some("a.product".to_owned()),
                    },
                    ObservedLink {
                        raw_href: "/product/two".to_owned(),
                        selector: Some("a.product".to_owned()),
                    },
                    ObservedLink {
                        raw_href: "/product/two#canonical-duplicate".to_owned(),
                        selector: Some("a.product".to_owned()),
                    },
                ],
            ),
            page("https://example.test/product/one", Vec::new()),
            page("https://example.test/product/two", Vec::new()),
        ],
        1,
    );
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(provider)));
    let result = service
        .execute(crawler.id(), version.id(), preview_request(vec![seed_a.id]))
        .await?;

    let listing_distribution = result
        .summary
        .page_type_distribution
        .iter()
        .find(|distribution| distribution.page_type_id == listing)
        .ok_or("missing Listing distribution")?;
    assert_eq!(listing_distribution.sampled_pages, 1);
    assert_eq!(listing_distribution.discovered_unique_urls, 0);
    let product_distribution = result
        .summary
        .page_type_distribution
        .iter()
        .find(|distribution| distribution.page_type_id == product)
        .ok_or("missing Product distribution")?;
    assert_eq!(product_distribution.discovered_unique_urls, 2);
    assert_eq!(product_distribution.sampled_pages, 2);
    assert_eq!(result.summary.duplicates_prevented, 1);
    Ok(())
}

#[tokio::test]
async fn redirect_final_page_type_samples_without_transferring_the_admission_reservation()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, listing, product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let mut guardrails = repository
        .crawler_version_guardrails(crawler.id(), version.id())
        .await?;
    guardrails.page_types = vec![PageTypeDiscoveryGuardrails {
        page_type_id: listing,
        page_budget: Some(1),
        health_threshold: None,
    }];
    repository
        .update_crawler_version_guardrails(
            crawler.id(),
            version.id(),
            &guardrails,
            "operator",
            "unix:18",
        )
        .await?;
    let redirected_product = PageObservation {
        requested_url: "https://example.test/product/redirected".to_owned(),
        final_url: Some("https://example.test/listing/final".to_owned()),
        artifact_ids: Vec::new(),
        discovered_links: vec![ObservedLink {
            raw_href: "/listing/child".to_owned(),
            selector: Some("a.next".to_owned()),
        }],
        selector_observations: Vec::new(),
        pagination_observations: Vec::new(),
    };
    let provider = FixtureDiscoveryPreviewProvider::observed(
        [
            page(
                seed_a.original_url.as_str(),
                vec![ObservedLink {
                    raw_href: "/product/redirected".to_owned(),
                    selector: Some("a.product".to_owned()),
                }],
            ),
            redirected_product,
            page("https://example.test/listing/child", Vec::new()),
        ],
        1,
    );
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(provider)));
    let result = service
        .execute(crawler.id(), version.id(), preview_request(vec![seed_a.id]))
        .await?;

    assert_eq!(result.summary.pages_sampled, 3);
    assert!(result.pages.iter().any(|page| {
        page.canonical_url.as_deref() == Some("https://example.test/listing/child")
            && page.state == erabi_domain::PreviewUrlState::Sampled
    }));
    assert!(
        !result
            .summary
            .budget_hit_counts
            .contains_key(&erabi_domain::PreviewBudgetKind::PageTypePageBudget)
    );
    let product_distribution = result
        .summary
        .page_type_distribution
        .iter()
        .find(|distribution| distribution.page_type_id == product)
        .ok_or("missing Product distribution")?;
    assert_eq!(product_distribution.discovered_unique_urls, 1);
    assert_eq!(product_distribution.sampled_pages, 0);
    let listing_distribution = result
        .summary
        .page_type_distribution
        .iter()
        .find(|distribution| distribution.page_type_id == listing)
        .ok_or("missing Listing distribution")?;
    assert_eq!(listing_distribution.discovered_unique_urls, 1);
    assert_eq!(listing_distribution.sampled_pages, 3);
    Ok(())
}

#[tokio::test]
async fn redirected_paths_retain_the_observed_source_identity_everywhere()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, _seed_a, _seed_b, listing, product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let redirect_url: url::Url = "https://example.test/redirect".parse()?;
    let redirect_seed = Seed::new(redirect_url.clone(), redirect_url.clone());
    let mut current = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    current.add_seed(redirect_seed.clone())?;
    repository
        .save_draft(&current, "operator", "unix:19")
        .await?;
    let version = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    let product_to_listing = transition(product, listing, "product listings", "a.listing", 1);
    repository
        .create_discovery_transition(
            crawler.id(),
            version.id(),
            &product_to_listing,
            "operator",
            "unix:20",
        )
        .await?;
    let root = PageObservation {
        requested_url: redirect_seed.original_url.to_string(),
        final_url: Some("https://example.test/product/final".to_owned()),
        artifact_ids: Vec::new(),
        discovered_links: vec![
            ObservedLink {
                raw_href: "/listing/1".to_owned(),
                selector: Some("a.listing".to_owned()),
            },
            ObservedLink {
                raw_href: "/listing/1#duplicate".to_owned(),
                selector: Some("a.listing".to_owned()),
            },
            ObservedLink {
                raw_href: "https://[::1".to_owned(),
                selector: Some("a.listing".to_owned()),
            },
        ],
        selector_observations: Vec::new(),
        pagination_observations: Vec::new(),
    };
    let provider = FixtureDiscoveryPreviewProvider::observed(
        [root, page("https://example.test/listing/1", Vec::new())],
        1,
    );
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(provider)));
    let result = service
        .execute(
            crawler.id(),
            version.id(),
            preview_request(vec![redirect_seed.id]),
        )
        .await?;

    let redirected_paths = result
        .discovery_paths
        .iter()
        .filter(|path| path.source_requested_url == "https://example.test/redirect")
        .collect::<Vec<_>>();
    assert_eq!(redirected_paths.len(), 3);
    for path in &redirected_paths {
        assert_eq!(
            path.source_final_url.as_deref(),
            Some("https://example.test/product/final")
        );
        assert_eq!(
            path.source_canonical_url,
            "https://example.test/product/final"
        );
    }
    assert!(redirected_paths.iter().any(|path| {
        path.canonical_url.as_deref() == Some("https://example.test/listing/1")
            && path.state == erabi_domain::PreviewUrlState::InScopeMatched
    }));
    assert!(
        redirected_paths
            .iter()
            .any(|path| { path.state == erabi_domain::PreviewUrlState::CanonicalDuplicate })
    );
    assert!(
        redirected_paths
            .iter()
            .any(|path| path.state == erabi_domain::PreviewUrlState::InvalidUrl)
    );
    Ok(())
}

async fn effective_transition_total_limit_case(
    configured_total_budget: Option<u64>,
    preview_default: u64,
    preview_override: Option<u64>,
) -> Result<u64, Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, listing, product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let mut configured = transition(
        listing,
        product,
        "configured total limit",
        "a.configured",
        1,
    );
    configured.budget.total_budget = configured_total_budget;
    repository
        .create_discovery_transition(
            crawler.id(),
            version.id(),
            &configured,
            "operator",
            "unix:21",
        )
        .await?;
    let provider = FixtureDiscoveryPreviewProvider::observed(
        [page(seed_a.original_url.as_str(), Vec::new())],
        1,
    );
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(provider)));
    let mut request = preview_request(vec![seed_a.id]);
    request.limits.default_transition_total_limit = preview_default;
    if let Some(max_total_links) = preview_override {
        request.limits.transition_total_limits = vec![TransitionPreviewTotalLimit {
            transition_id: configured.id,
            max_total_links,
        }];
    }
    let result = service.execute(crawler.id(), version.id(), request).await?;
    result
        .effective_limits
        .transition_total_limits
        .iter()
        .find(|limit| limit.transition_id == configured.id)
        .map(|limit| limit.effective_total_limit)
        .ok_or_else(|| "missing configured transition effective limit".into())
}

#[tokio::test]
async fn effective_transition_totals_include_configured_caps_and_preview_overrides()
-> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        effective_transition_total_limit_case(Some(5), 20, None).await?,
        5
    );
    assert_eq!(
        effective_transition_total_limit_case(Some(5), 20, Some(3)).await?,
        3
    );
    assert_eq!(
        effective_transition_total_limit_case(Some(5), 20, Some(10)).await?,
        5
    );
    assert_eq!(
        effective_transition_total_limit_case(None, 7, None).await?,
        7
    );
    Ok(())
}

#[tokio::test]
async fn page_failure_is_bounded_and_provider_contract_failure_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, _listing, _product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let provider = FixtureDiscoveryPreviewProvider::default().with_page_failure(
        seed_a.original_url.as_str(),
        TestDiagnostic {
            code: "PAGE_FAILED".to_owned(),
            message: "fixture failure".to_owned(),
        },
    );
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(provider)));
    let result = service
        .execute(crawler.id(), version.id(), preview_request(vec![seed_a.id]))
        .await?;
    assert_eq!(result.summary.provider_errors, 1);
    assert_eq!(result.summary.pages_sampled, 0);
    Ok(())
}

async fn queued_duplicate_depth_case(
    first_depth: u32,
    later_depth: u32,
    reverse_link_input: bool,
    max_pages: u64,
) -> Result<(erabi_domain::DiscoveryPreviewResult, Vec<String>), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, listing, product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let repository = CrawlerRepository::new(&database);
    for item in [
        transition(listing, product, "deep route", "a.deep", first_depth),
        transition(
            listing,
            product,
            "shallower route",
            "a.shallow",
            later_depth,
        ),
    ] {
        repository
            .create_discovery_transition(crawler.id(), version.id(), &item, "operator", "unix:20")
            .await?;
    }
    let deep_link = ObservedLink {
        raw_href: "/product/x?utm_source=first".to_owned(),
        selector: Some("a.deep".to_owned()),
    };
    let shallow_link = ObservedLink {
        raw_href: "/product/x?utm_source=later".to_owned(),
        selector: Some("a.shallow".to_owned()),
    };
    let links = if reverse_link_input {
        vec![shallow_link, deep_link]
    } else {
        vec![deep_link, shallow_link]
    };
    let mut pages = vec![
        page(seed_a.original_url.as_str(), links),
        page("https://example.test/product/x", Vec::new()),
    ];
    if reverse_link_input {
        pages.reverse();
    }
    let provider = Arc::new(RecordingProvider::observed(pages, 1));
    let service = DiscoveryPreviewService::new(database, Some(provider.clone()));
    let mut request = preview_request(vec![seed_a.id]);
    request.limits.max_pages = max_pages;
    let result = service.execute(crawler.id(), version.id(), request).await?;
    Ok((result, provider.requested_urls()))
}

type QueuedDuplicateSemantics = (
    u32,
    u64,
    u64,
    u64,
    Vec<(erabi_domain::PreviewUrlState, Option<u32>)>,
);

fn queued_duplicate_semantics(
    result: &erabi_domain::DiscoveryPreviewResult,
) -> Option<QueuedDuplicateSemantics> {
    let target = result
        .pages
        .iter()
        .find(|page| page.canonical_url.as_deref() == Some("https://example.test/product/x"))?;
    let paths = result
        .discovery_paths
        .iter()
        .filter(|path| path.canonical_url.as_deref() == Some("https://example.test/product/x"))
        .map(|path| (path.state, path.prospective_depth))
        .collect();
    Some((
        target.depth,
        result.summary.pages_sampled,
        result.summary.duplicates_prevented,
        result.summary.newly_enqueued_urls,
        paths,
    ))
}

#[tokio::test]
async fn queued_duplicate_uses_minimum_known_depth_without_another_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let (result, calls) = queued_duplicate_depth_case(3, 1, false, 2).await?;
    let semantics = queued_duplicate_semantics(&result).ok_or("missing queued target page")?;
    assert_eq!(
        calls,
        vec![
            "https://example.test/listing/a".to_owned(),
            "https://example.test/product/x".to_owned(),
        ]
    );
    assert_eq!(semantics.0, 1);
    assert_eq!(result.summary.pages_sampled, 2);
    assert_eq!(result.summary.newly_enqueued_urls, 2);
    assert_eq!(result.summary.duplicates_prevented, 1);
    assert!(
        !result
            .summary
            .budget_hit_counts
            .contains_key(&erabi_domain::PreviewBudgetKind::MaxPages)
    );
    let paths = &semantics.4;
    assert_eq!(
        paths,
        &vec![
            (erabi_domain::PreviewUrlState::InScopeMatched, Some(3)),
            (erabi_domain::PreviewUrlState::CanonicalDuplicate, Some(1)),
        ]
    );
    assert!(
        result
            .discovery_paths
            .iter()
            .filter(|path| path.canonical_url.as_deref() == Some("https://example.test/product/x"))
            .all(|path| path.seed_ids.len() == 1)
    );

    let (reversed, reversed_calls) = queued_duplicate_depth_case(3, 1, true, 2).await?;
    assert_eq!(reversed_calls, calls);
    let reversed_semantics =
        queued_duplicate_semantics(&reversed).ok_or("missing reversed queued target page")?;
    assert_eq!(reversed_semantics, semantics);
    Ok(())
}

#[tokio::test]
async fn equal_or_deeper_queued_duplicates_do_not_create_another_entry()
-> Result<(), Box<dyn std::error::Error>> {
    let (equal, equal_calls) = queued_duplicate_depth_case(3, 3, false, 2).await?;
    assert_eq!(
        queued_duplicate_semantics(&equal)
            .ok_or("missing equal-depth queued target page")?
            .0,
        3
    );
    assert_eq!(equal_calls.len(), 2);
    assert_eq!(equal.summary.newly_enqueued_urls, 2);

    let (deeper, deeper_calls) = queued_duplicate_depth_case(3, 4, false, 2).await?;
    assert_eq!(
        queued_duplicate_semantics(&deeper)
            .ok_or("missing deeper queued target page")?
            .0,
        3
    );
    assert_eq!(deeper_calls.len(), 2);
    assert_eq!(deeper.summary.newly_enqueued_urls, 2);
    Ok(())
}

#[tokio::test]
async fn queued_duplicate_retains_each_seed_provenance_path()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, seed_b, listing, product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let repository = CrawlerRepository::new(&database);
    for item in [
        transition(listing, product, "deep route", "a.deep", 3),
        transition(listing, product, "shallower route", "a.shallow", 1),
    ] {
        repository
            .create_discovery_transition(crawler.id(), version.id(), &item, "operator", "unix:20b")
            .await?;
    }
    let provider = Arc::new(RecordingProvider::observed(
        [
            page(
                seed_a.original_url.as_str(),
                vec![ObservedLink {
                    raw_href: "/product/x?utm_source=first".to_owned(),
                    selector: Some("a.deep".to_owned()),
                }],
            ),
            page(
                seed_b.original_url.as_str(),
                vec![ObservedLink {
                    raw_href: "/product/x?utm_source=later".to_owned(),
                    selector: Some("a.shallow".to_owned()),
                }],
            ),
            page("https://example.test/product/x", Vec::new()),
        ],
        1,
    ));
    let service = DiscoveryPreviewService::new(database, Some(provider.clone()));
    let result = service
        .execute(
            crawler.id(),
            version.id(),
            preview_request(vec![seed_a.id, seed_b.id]),
        )
        .await?;
    let target = result
        .pages
        .iter()
        .find(|page| page.canonical_url.as_deref() == Some("https://example.test/product/x"))
        .ok_or("missing queued target page")?;
    assert_eq!(target.depth, 1);
    assert_eq!(target.seed_ids, vec![seed_a.id, seed_b.id]);
    assert_eq!(
        provider
            .requested_urls()
            .iter()
            .filter(|url| url.as_str() == "https://example.test/product/x")
            .count(),
        1
    );
    let source_seed_ids = result
        .discovery_paths
        .iter()
        .filter(|path| path.canonical_url.as_deref() == Some("https://example.test/product/x"))
        .map(|path| path.seed_ids.clone())
        .collect::<Vec<_>>();
    assert_eq!(source_seed_ids, vec![vec![seed_a.id], vec![seed_b.id]]);
    Ok(())
}

async fn query_explosion_case(
    query_variants: u64,
    queryless_identities: u64,
    reverse_link_input: bool,
) -> Result<erabi_domain::DiscoveryPreviewResult, Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, _listing, _product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let mut links = (0..query_variants)
        .map(|index| ObservedLink {
            raw_href: format!("/listing/query?variant={index}"),
            selector: Some("a.next".to_owned()),
        })
        .collect::<Vec<_>>();
    for index in 0..queryless_identities {
        let raw_href = if index == 0 {
            "/listing/query".to_owned()
        } else {
            format!("https://example.test:{}/listing/query", 4_430 + index)
        };
        links.push(ObservedLink {
            raw_href,
            selector: Some("a.next".to_owned()),
        });
    }
    if reverse_link_input {
        links.reverse();
    }
    let provider =
        FixtureDiscoveryPreviewProvider::observed([page(seed_a.original_url.as_str(), links)], 1);
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(provider)));
    service
        .execute(crawler.id(), version.id(), preview_request(vec![seed_a.id]))
        .await
        .map_err(Into::into)
}

fn query_group(
    result: &erabi_domain::DiscoveryPreviewResult,
) -> Option<&erabi_domain::PreviewQueryVariantGroup> {
    result
        .growth_indicators
        .query_variant_groups
        .iter()
        .find(|group| group.host == "example.test" && group.path == "/listing/query")
}

fn has_growth_warning(
    result: &erabi_domain::DiscoveryPreviewResult,
    code: erabi_domain::PreviewGrowthWarningCode,
) -> bool {
    result
        .growth_warnings
        .iter()
        .any(|warning| warning.code == code)
}

#[tokio::test]
async fn query_explosion_requires_variant_threshold_and_query_bearing_ratio()
-> Result<(), Box<dyn std::error::Error>> {
    let all_query_bearing = query_explosion_case(8, 0, false).await?;
    assert!(has_growth_warning(
        &all_query_bearing,
        erabi_domain::PreviewGrowthWarningCode::QueryParameterExplosion
    ));
    let all_query_group = query_group(&all_query_bearing).ok_or("missing all-query group")?;
    assert_eq!(all_query_group.canonical_query_variants, 8);
    assert_eq!(all_query_group.query_bearing_identities, 8);
    assert_eq!(all_query_group.total_identities, 8);

    let below_ratio = query_explosion_case(8, 4, false).await?;
    assert!(!has_growth_warning(
        &below_ratio,
        erabi_domain::PreviewGrowthWarningCode::QueryParameterExplosion
    ));
    let below_ratio_group = query_group(&below_ratio).ok_or("missing below-ratio group")?;
    assert_eq!(below_ratio_group.canonical_query_variants, 8);
    assert_eq!(below_ratio_group.query_bearing_identities, 8);
    assert_eq!(below_ratio_group.total_identities, 12);

    let threshold_ratio = query_explosion_case(9, 3, false).await?;
    assert!(has_growth_warning(
        &threshold_ratio,
        erabi_domain::PreviewGrowthWarningCode::QueryParameterExplosion
    ));
    let threshold_ratio_group =
        query_group(&threshold_ratio).ok_or("missing threshold-ratio group")?;
    assert_eq!(threshold_ratio_group.canonical_query_variants, 9);
    assert_eq!(threshold_ratio_group.query_bearing_identities, 9);
    assert_eq!(threshold_ratio_group.total_identities, 12);

    let reversed = query_explosion_case(9, 3, true).await?;
    assert_eq!(
        query_group(&reversed).ok_or("missing reversed query group")?,
        threshold_ratio_group
    );
    assert_eq!(reversed.growth_warnings, threshold_ratio.growth_warnings,);
    Ok(())
}

async fn dominance_case(
    include_product_tie: bool,
    reverse_link_input: bool,
) -> Result<
    (
        erabi_domain::DiscoveryPreviewResult,
        erabi_domain::DiscoveryTransitionId,
    ),
    Box<dyn std::error::Error>,
> {
    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, _listing, _product, to_product, to_listing) =
        graph_fixture(&database).await?;
    let mut links = (0..8)
        .map(|index| ObservedLink {
            raw_href: format!("/listing/cycle-{index}"),
            selector: Some("a.next".to_owned()),
        })
        .collect::<Vec<_>>();
    if include_product_tie {
        links.extend((0..8).map(|index| ObservedLink {
            raw_href: format!("/product/tie-{index}"),
            selector: Some("a.product".to_owned()),
        }));
    }
    if reverse_link_input {
        links.reverse();
    }
    let provider =
        FixtureDiscoveryPreviewProvider::observed([page(seed_a.original_url.as_str(), links)], 1);
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(provider)));
    let result = service
        .execute(crawler.id(), version.id(), preview_request(vec![seed_a.id]))
        .await?;
    if include_product_tie {
        assert_eq!(
            result
                .summary
                .transition_counts
                .iter()
                .find(|count| count.transition_id == to_product)
                .map(|count| count.eligible_edges),
            Some(8)
        );
    }
    assert_eq!(
        result
            .summary
            .transition_counts
            .iter()
            .find(|count| count.transition_id == to_listing)
            .map(|count| count.eligible_edges),
        Some(8)
    );
    Ok((result, to_listing))
}

#[tokio::test]
async fn cyclic_dominance_requires_a_unique_observed_transition()
-> Result<(), Box<dyn std::error::Error>> {
    let (unique, cycle_id) = dominance_case(false, false).await?;
    assert_eq!(
        unique.growth_indicators.dominant_transition_id,
        Some(cycle_id)
    );
    assert!(has_growth_warning(
        &unique,
        erabi_domain::PreviewGrowthWarningCode::CyclicTransitionDominance
    ));

    let (tied, _) = dominance_case(true, false).await?;
    assert_eq!(tied.growth_indicators.dominant_transition_id, None);
    assert_eq!(
        tied.growth_indicators.dominant_transition_share_percent,
        None
    );
    assert!(!has_growth_warning(
        &tied,
        erabi_domain::PreviewGrowthWarningCode::CyclicTransitionDominance
    ));

    let (regenerated_and_reversed, _) = dominance_case(true, true).await?;
    assert_eq!(
        regenerated_and_reversed
            .growth_indicators
            .dominant_transition_id,
        None
    );
    assert_eq!(
        regenerated_and_reversed
            .growth_indicators
            .dominant_transition_share_percent,
        None
    );
    assert_eq!(
        regenerated_and_reversed.growth_warnings,
        tied.growth_warnings
    );
    Ok(())
}

#[tokio::test]
async fn two_node_cycle_dominance_warns_but_acyclic_dominance_does_not()
-> Result<(), Box<dyn std::error::Error>> {
    let cycle_database = database().await?;
    let (crawler, version, seed_a, _seed_b, listing, product, to_product, _to_listing) =
        graph_fixture(&cycle_database).await?;
    let repository = CrawlerRepository::new(&cycle_database);
    let product_to_listing = transition(product, listing, "product return", "a.return", 1);
    repository
        .create_discovery_transition(
            crawler.id(),
            version.id(),
            &product_to_listing,
            "operator",
            "unix:22",
        )
        .await?;
    let two_node_provider = FixtureDiscoveryPreviewProvider::observed(
        [page(
            seed_a.original_url.as_str(),
            (0..8)
                .map(|index| ObservedLink {
                    raw_href: format!("/product/cycle-{index}"),
                    selector: Some("a.product".to_owned()),
                })
                .collect::<Vec<_>>(),
        )],
        1,
    );
    let two_node = DiscoveryPreviewService::new(cycle_database, Some(Arc::new(two_node_provider)))
        .execute(crawler.id(), version.id(), preview_request(vec![seed_a.id]))
        .await?;
    assert_eq!(
        two_node.growth_indicators.dominant_transition_id,
        Some(to_product)
    );
    assert!(has_growth_warning(
        &two_node,
        erabi_domain::PreviewGrowthWarningCode::CyclicTransitionDominance
    ));

    let acyclic_database = database().await?;
    let (crawler, version, seed_a, _seed_b, _listing, _product, to_product, _to_listing) =
        graph_fixture(&acyclic_database).await?;
    let acyclic_provider = FixtureDiscoveryPreviewProvider::observed(
        [page(
            seed_a.original_url.as_str(),
            (0..8)
                .map(|index| ObservedLink {
                    raw_href: format!("/product/acyclic-{index}"),
                    selector: Some("a.product".to_owned()),
                })
                .collect::<Vec<_>>(),
        )],
        1,
    );
    let acyclic = DiscoveryPreviewService::new(acyclic_database, Some(Arc::new(acyclic_provider)))
        .execute(crawler.id(), version.id(), preview_request(vec![seed_a.id]))
        .await?;
    assert_eq!(
        acyclic.growth_indicators.dominant_transition_id,
        Some(to_product)
    );
    assert_eq!(
        acyclic.growth_indicators.dominant_transition_share_percent,
        Some(100)
    );
    assert!(!has_growth_warning(
        &acyclic,
        erabi_domain::PreviewGrowthWarningCode::CyclicTransitionDominance
    ));
    Ok(())
}

#[tokio::test]
async fn retained_frontier_with_download_budget_hit_reports_budget_pressure()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, _listing, _product, _to_product, _to_listing) =
        graph_fixture(&database).await?;
    let repository = CrawlerRepository::new(&database);
    let mut guardrails = repository
        .crawler_version_guardrails(crawler.id(), version.id())
        .await?;
    guardrails.max_downloaded_bytes = 1;
    repository
        .update_crawler_version_guardrails(
            crawler.id(),
            version.id(),
            &guardrails,
            "operator",
            "unix:21",
        )
        .await?;
    let provider = FixtureDiscoveryPreviewProvider::observed(
        [page(
            seed_a.original_url.as_str(),
            vec![ObservedLink {
                raw_href: "/product/queued".to_owned(),
                selector: Some("a.product".to_owned()),
            }],
        )],
        1,
    );
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(provider)));
    let result = service
        .execute(crawler.id(), version.id(), preview_request(vec![seed_a.id]))
        .await?;
    assert_eq!(result.summary.frontier_remaining, 1);
    assert!(
        result
            .summary
            .budget_hit_counts
            .contains_key(&erabi_domain::PreviewBudgetKind::MaxDownloadedBytes)
    );
    assert!(has_growth_warning(
        &result,
        erabi_domain::PreviewGrowthWarningCode::BudgetPressure
    ));
    Ok(())
}
