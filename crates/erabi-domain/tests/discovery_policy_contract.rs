#![allow(
    clippy::expect_used,
    clippy::field_reassign_with_default,
    clippy::too_many_lines,
    clippy::unwrap_used
)]

use std::collections::BTreeSet;

use erabi_domain::{
    CanonicalizationDecision, CanonicalizationPolicy, CrawlerVersionGuardrails,
    DiscoveryBudgetCandidate, DiscoveryBudgetDecision, DiscoveryBudgetError,
    DiscoveryBudgetEvaluator, DiscoveryBudgetExclusion, DiscoveryTransition,
    DomainScopeClassification, DomainScopeHostRule, DomainScopeKind, DomainScopePolicy,
    PageTypeDiscoveryGuardrails, ResolvedOperationalLimits, Seed, TransitionBudget,
    TransitionGraph,
};

fn seed(url: &str) -> Seed {
    let url: url::Url = url.parse().expect("valid fixture URL");
    Seed::new(url.clone(), url)
}

fn canonical_policy(keep: &[&str], drop: &[&str]) -> CanonicalizationPolicy {
    CanonicalizationPolicy::new(
        keep.iter().map(|value| (*value).to_owned()).collect(),
        drop.iter().map(|value| (*value).to_owned()).collect(),
    )
    .expect("valid canonicalization policy")
}

fn transition(
    source_page_type_id: erabi_domain::PageTypeId,
    target_page_type_id: erabi_domain::PageTypeId,
) -> DiscoveryTransition {
    DiscoveryTransition {
        id: erabi_domain::DiscoveryTransitionId::new(),
        source_page_type_id,
        target_page_type_id,
        name: "links".into(),
        enabled: true,
        link_selector: "a[href]".into(),
        url_constraints: None,
        priority: 0,
        budget: TransitionBudget {
            max_links_per_source_page: 2,
            total_budget: Some(3),
            depth_contribution: 1,
        },
        deduplicate: true,
        latest_test_evidence_id: None,
    }
}

#[test]
fn canonicalization_is_safe_deterministic_and_explainable() {
    let policy = canonical_policy(&[], &[]);
    let result = policy
        .canonicalize("HTTPS://EXAMPLE.test:443/product?b=2&a=1&utm_source=news#reviews")
        .expect("valid URL");
    assert_eq!(
        result.original_url,
        "HTTPS://EXAMPLE.test:443/product?b=2&a=1&utm_source=news#reviews"
    );
    assert_eq!(
        result.canonical_url.as_str(),
        "https://example.test/product?a=1&b=2"
    );
    assert!(
        result
            .decisions
            .contains(&CanonicalizationDecision::DefaultPortRemoved)
    );
    assert!(
        result
            .decisions
            .contains(&CanonicalizationDecision::FragmentRemoved)
    );
    assert!(
        result
            .decisions
            .contains(&CanonicalizationDecision::QuerySorted)
    );
    assert!(result.decisions.iter().any(|decision| matches!(
        decision,
        CanonicalizationDecision::TrackingParameterRemoved { parameter }
            if parameter == "utm_source"
    )));

    for (input, expected) in [
        ("http://example.test:80/", "http://example.test/"),
        (
            "https://example.test:444/product",
            "https://example.test:444/product",
        ),
        ("https://example.test", "https://example.test/"),
        (
            "https://example.test//product///",
            "https://example.test//product///",
        ),
        (
            "https://example.test/product/",
            "https://example.test/product/",
        ),
        (
            "https://example.test?a=2&a=1",
            "https://example.test/?a=1&a=2",
        ),
    ] {
        assert_eq!(
            policy.canonicalize(input).unwrap().canonical_url.as_str(),
            expected
        );
    }
}

#[test]
fn canonicalization_preserves_unknown_query_semantics_and_raw_encoding() {
    let policy = canonical_policy(&[], &[]);
    let result = policy
        .canonicalize("https://example.test/product?b=%2F&a=%26&session_mode=compact")
        .unwrap();
    assert_eq!(
        result.canonical_url.as_str(),
        "https://example.test/product?a=%26&b=%2F&session_mode=compact"
    );
    assert_eq!(
        policy
            .canonicalize("https://example.test/product?session_mode=expanded")
            .unwrap()
            .canonical_url,
        "https://example.test/product?session_mode=expanded"
            .parse()
            .unwrap()
    );
    assert_eq!(
        policy
            .canonicalize(
                "https://example.test/product?utm_source=x&fbclid=ad&gclid=click&product=1&session_mode=compact"
            )
            .unwrap()
            .canonical_url
            .as_str(),
        "https://example.test/product?product=1&session_mode=compact"
    );
}

#[test]
fn canonicalization_keep_drop_precedence_is_frozen_and_idempotent() {
    let policy = canonical_policy(&["utm_source"], &["session_mode"]);
    let result = policy
        .canonicalize("https://example.test/product?session_mode=compact&utm_source=x&gclid=y")
        .unwrap();
    assert_eq!(
        result.canonical_url.as_str(),
        "https://example.test/product?utm_source=x"
    );
    assert!(result.decisions.iter().any(|decision| matches!(
        decision,
        CanonicalizationDecision::ExplicitParameterKept { parameter }
            if parameter == "utm_source"
    )));
    assert!(result.decisions.iter().any(|decision| matches!(
        decision,
        CanonicalizationDecision::CustomParameterDropped { parameter }
            if parameter == "session_mode"
    )));
    assert!(
        CanonicalizationPolicy::new(
            BTreeSet::from(["utm_source".to_owned()]),
            BTreeSet::from(["utm_source".to_owned()]),
        )
        .is_err()
    );

    let canonical = result.canonical_url.to_string();
    let second = policy.canonicalize(&canonical).unwrap();
    assert_eq!(second.canonical_url, result.canonical_url);
    assert_eq!(
        policy
            .canonicalize("https://example.test/product?product=1&utm_source=x#one")
            .unwrap()
            .canonical_url,
        policy
            .canonicalize("https://example.test/product?utm_source=x&product=1#two")
            .unwrap()
            .canonical_url
    );
}

#[test]
fn invalid_and_non_web_urls_are_rejected_as_distinct_errors() {
    let policy = canonical_policy(&[], &[]);
    assert_eq!(
        policy.canonicalize("not a URL").unwrap_err().code,
        erabi_domain::ErrorCode::InvalidUrl
    );
    assert_eq!(
        policy.canonicalize("javascript:alert(1)").unwrap_err().code,
        erabi_domain::ErrorCode::UnsupportedUrlScheme
    );
}

#[test]
fn domain_scope_is_boundary_aware_and_preserves_external_classification() {
    let seeds = vec![seed("https://EXAMPLE.com/start")];
    let policy = DomainScopePolicy::default();
    assert!(matches!(
        policy
            .classify(&"https://example.com/next".parse().unwrap(), &seeds)
            .unwrap(),
        DomainScopeClassification::InScope { .. }
    ));
    for external in [
        "https://other.test/",
        "https://example.com.attacker.test/",
        "https://evil-example.com/",
    ] {
        assert!(matches!(
            policy.classify(&external.parse().unwrap(), &seeds).unwrap(),
            DomainScopeClassification::External { .. }
        ));
    }
}

#[test]
fn domain_scope_variants_use_explicit_subdomains_and_psl_registrable_domains() {
    let seeds = vec![seed("https://shop.example.co.uk/start")];
    let policy = DomainScopePolicy {
        version: 1,
        policy: DomainScopeKind::SameRegistrableDomain {
            explicit_subdomains: BTreeSet::from(["catalog.example.co.uk".into()]),
        },
    };
    assert!(matches!(
        policy
            .classify(&"https://example.co.uk/".parse().unwrap(), &seeds)
            .unwrap(),
        DomainScopeClassification::InScope { .. }
    ));
    assert!(matches!(
        policy
            .classify(&"https://catalog.example.co.uk/".parse().unwrap(), &seeds)
            .unwrap(),
        DomainScopeClassification::InScope { .. }
    ));
    assert!(matches!(
        policy
            .classify(
                &"https://unselected.example.co.uk/".parse().unwrap(),
                &seeds
            )
            .unwrap(),
        DomainScopeClassification::External { .. }
    ));
    let unrelated_explicit_host = DomainScopePolicy {
        version: 1,
        policy: DomainScopeKind::SameRegistrableDomain {
            explicit_subdomains: BTreeSet::from(["other.example".into()]),
        },
    };
    assert!(matches!(
        unrelated_explicit_host
            .classify(&"https://other.example/".parse().unwrap(), &seeds)
            .unwrap(),
        DomainScopeClassification::External { .. }
    ));

    let allowlist = DomainScopePolicy {
        version: 1,
        policy: DomainScopeKind::ExplicitAllowlist {
            hosts: BTreeSet::from(["public.example".into()]),
        },
    };
    assert!(matches!(
        allowlist
            .classify(&"https://public.example/".parse().unwrap(), &[])
            .unwrap(),
        DomainScopeClassification::InScope { .. }
    ));
}

#[test]
fn custom_scope_block_always_wins_and_invalid_scope_fails_closed() {
    let seeds = vec![seed("https://example.test/")];
    let allow = DomainScopeHostRule::subdomains("example.test").unwrap();
    let block = DomainScopeHostRule::exact("private.example.test").unwrap();
    let policy = DomainScopePolicy {
        version: 1,
        policy: DomainScopeKind::Custom {
            allow: BTreeSet::from([allow]),
            block: BTreeSet::from([block]),
        },
    };
    assert!(matches!(
        policy
            .classify(&"https://private.example.test/".parse().unwrap(), &seeds)
            .unwrap(),
        DomainScopeClassification::Blocked { .. }
    ));
    assert!(matches!(
        policy
            .classify(&"https://shop.example.test/".parse().unwrap(), &seeds)
            .unwrap(),
        DomainScopeClassification::InScope { .. }
    ));

    let invalid = DomainScopePolicy {
        version: 999,
        policy: DomainScopeKind::SeedDomainsOnly,
    };
    assert!(
        invalid
            .classify(&"https://example.test/".parse().unwrap(), &seeds)
            .is_err()
    );
    assert!(
        DomainScopePolicy::default()
            .classify(&"https://example.test/".parse().unwrap(), &[])
            .is_err()
    );
}

#[test]
fn transition_graph_accepts_self_edges_and_cycles() {
    let first = erabi_domain::PageTypeId::new();
    let second = erabi_domain::PageTypeId::new();
    let self_edge = transition(first, first);
    let mut reverse = transition(second, first);
    reverse.name = "return".into();
    let graph = TransitionGraph::new(
        &[first, second],
        vec![self_edge, transition(first, second), reverse],
    )
    .unwrap();
    assert_eq!(graph.transitions().len(), 3);
}

#[test]
fn discovery_budgets_enforce_limits_and_operational_baseline_is_separate() {
    let mut guardrails = CrawlerVersionGuardrails::default();
    guardrails.max_pages = 2;
    guardrails.max_depth = 2;
    guardrails.max_duration_seconds = 10;
    guardrails.max_downloaded_bytes = 100;
    guardrails.max_concurrent_requests_per_domain = 2;
    guardrails.min_request_delay_ms = 50;
    let page_type = PageTypeDiscoveryGuardrails {
        page_type_id: erabi_domain::PageTypeId::new(),
        page_budget: Some(1),
        health_threshold: None,
    };
    guardrails.page_types.push(page_type.clone());
    let transition_budget = TransitionBudget {
        max_links_per_source_page: 1,
        total_budget: Some(2),
        depth_contribution: 1,
    };
    let evaluator =
        DiscoveryBudgetEvaluator::new(&guardrails, Some(&page_type), Some(&transition_budget));
    assert_eq!(
        evaluator.evaluate(DiscoveryBudgetCandidate::default()),
        Ok(DiscoveryBudgetDecision::Allowed)
    );
    assert_eq!(
        evaluator.evaluate(DiscoveryBudgetCandidate {
            pages_already_scheduled: 2,
            ..DiscoveryBudgetCandidate::default()
        }),
        Ok(DiscoveryBudgetDecision::Excluded(
            DiscoveryBudgetExclusion::MaxPages
        ))
    );
    assert_eq!(
        evaluator.evaluate(DiscoveryBudgetCandidate {
            current_depth: 2,
            depth_contribution: 1,
            ..DiscoveryBudgetCandidate::default()
        }),
        Ok(DiscoveryBudgetDecision::Excluded(
            DiscoveryBudgetExclusion::MaxDepth
        ))
    );
    assert_eq!(
        evaluator.evaluate(DiscoveryBudgetCandidate {
            elapsed_duration_seconds: 10,
            ..DiscoveryBudgetCandidate::default()
        }),
        Ok(DiscoveryBudgetDecision::Excluded(
            DiscoveryBudgetExclusion::MaxDuration
        ))
    );
    assert_eq!(
        evaluator.evaluate(DiscoveryBudgetCandidate {
            downloaded_bytes: 99,
            prospective_download_bytes: 2,
            ..DiscoveryBudgetCandidate::default()
        }),
        Ok(DiscoveryBudgetDecision::Excluded(
            DiscoveryBudgetExclusion::MaxDownloadedBytes
        ))
    );
    assert_eq!(
        evaluator.evaluate(DiscoveryBudgetCandidate {
            page_type_pages: 1,
            ..DiscoveryBudgetCandidate::default()
        }),
        Ok(DiscoveryBudgetDecision::Excluded(
            DiscoveryBudgetExclusion::PageTypePageBudget
        ))
    );
    assert_eq!(
        evaluator.evaluate(DiscoveryBudgetCandidate {
            transition_links_on_source_page: 1,
            ..DiscoveryBudgetCandidate::default()
        }),
        Ok(DiscoveryBudgetDecision::Excluded(
            DiscoveryBudgetExclusion::TransitionPerPageLinkLimit
        ))
    );
    assert_eq!(
        evaluator.evaluate(DiscoveryBudgetCandidate {
            transition_total_links: 2,
            ..DiscoveryBudgetCandidate::default()
        }),
        Ok(DiscoveryBudgetDecision::Excluded(
            DiscoveryBudgetExclusion::TransitionTotalBudget
        ))
    );
    assert!(matches!(
        CrawlerVersionGuardrails {
            max_depth: 0,
            ..guardrails.clone()
        }
        .validate(),
        Err(error) if error.code == erabi_domain::ErrorCode::InvalidCrawlGuardrails
    ));

    let safe_limits = ResolvedOperationalLimits {
        max_pages: 1,
        max_depth: 1,
        max_duration_seconds: 5,
        max_downloaded_bytes: 50,
        concurrency: 1,
        request_delay_ms: 100,
    };
    guardrails
        .validate_effective_operational_limits(&safe_limits)
        .unwrap();
    let unsafe_limits = ResolvedOperationalLimits {
        max_pages: 3,
        ..safe_limits
    };
    assert!(
        guardrails
            .validate_effective_operational_limits(&unsafe_limits)
            .is_err()
    );
}

#[test]
fn discovery_budget_overflow_and_cycle_bounds_are_deterministic() {
    let mut depth_guardrails = CrawlerVersionGuardrails::default();
    depth_guardrails.max_pages = u64::MAX;
    depth_guardrails.max_depth = u32::MAX;
    let depth_evaluator = DiscoveryBudgetEvaluator::new(&depth_guardrails, None, None);
    assert_eq!(
        depth_evaluator.evaluate(DiscoveryBudgetCandidate {
            current_depth: u32::MAX,
            depth_contribution: 1,
            ..DiscoveryBudgetCandidate::default()
        }),
        Err(DiscoveryBudgetError::Overflow)
    );

    let mut bytes_guardrails = CrawlerVersionGuardrails::default();
    bytes_guardrails.max_pages = u64::MAX;
    bytes_guardrails.max_downloaded_bytes = u64::MAX;
    let bytes_evaluator = DiscoveryBudgetEvaluator::new(&bytes_guardrails, None, None);
    assert_eq!(
        bytes_evaluator.evaluate(DiscoveryBudgetCandidate {
            downloaded_bytes: u64::MAX,
            prospective_download_bytes: 1,
            ..DiscoveryBudgetCandidate::default()
        }),
        Err(DiscoveryBudgetError::Overflow)
    );

    let first = erabi_domain::PageTypeId::new();
    let second = erabi_domain::PageTypeId::new();
    let graph = TransitionGraph::new(
        &[first, second],
        vec![transition(first, second), transition(second, first)],
    )
    .unwrap();
    let guardrails = CrawlerVersionGuardrails {
        max_pages: 3,
        max_depth: 10,
        ..CrawlerVersionGuardrails::default()
    };
    let transition_budget = TransitionBudget {
        max_links_per_source_page: 10,
        total_budget: Some(10),
        depth_contribution: 1,
    };
    let evaluator = DiscoveryBudgetEvaluator::new(&guardrails, None, Some(&transition_budget));
    let mut excluded = None;
    for pages in 0..=guardrails.max_pages {
        let decision = evaluator
            .evaluate(DiscoveryBudgetCandidate {
                pages_already_scheduled: pages,
                transition_total_links: pages,
                ..DiscoveryBudgetCandidate::default()
            })
            .unwrap();
        if let DiscoveryBudgetDecision::Excluded(reason) = decision {
            excluded = Some(reason);
            break;
        }
    }
    assert_eq!(excluded, Some(DiscoveryBudgetExclusion::MaxPages));
    assert_eq!(graph.transitions().len(), 2);
}
