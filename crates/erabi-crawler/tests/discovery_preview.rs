#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use erabi_crawler::{
    DiscoveryPreviewObservationRequest, DiscoveryPreviewProvider, DiscoveryPreviewProviderError,
    DiscoveryPreviewProviderOutcome, DiscoveryPreviewService, FixtureDiscoveryPreviewProvider,
    ManualPreviewClock, ObservedLink, PageObservation,
};
use erabi_db::{ErabiDatabase, MigrationRunner, repositories::CrawlerRepository};
use erabi_domain::{
    Crawler, CrawlerVersion, DiscoveryPreviewLimits, DiscoveryPreviewRequest,
    DiscoveryPreviewResultSemantics, DiscoveryTransition, DiscoveryTransitionId, Seed,
    TestDiagnostic, TransitionBudget, TransitionPreviewTotalLimit, UrlMatcher,
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
    let provider = FixtureDiscoveryPreviewProvider::observed(
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
    );
    let service = DiscoveryPreviewService::new(database, Some(Arc::new(provider)));
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
        [page(seed_a.original_url.as_str(), Vec::new())],
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
    assert!(
        result
            .summary
            .budget_hit_counts
            .contains_key(&erabi_domain::PreviewBudgetKind::MaxDuration)
    );
    Ok(())
}

#[tokio::test]
async fn semantic_guardrails_tighten_preview_and_provider_receives_remaining_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (crawler, version, seed_a, _seed_b, _listing, _product, transition, _cycle) =
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
        result.effective_limits.transition_total_limits[0].max_total_links,
        10
    );
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
