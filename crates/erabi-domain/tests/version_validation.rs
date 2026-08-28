#![allow(clippy::unwrap_used)]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use erabi_domain::{
    CrawlerId, CrawlerVersion, DiscoveryTransition, DiscoveryTransitionEvidence,
    DiscoveryTransitionId, MAX_VALIDATION_DETAIL_CHARS, PageType, Seed, SelectorCoverageEvidence,
    SelectorCoverageStatus, TestEvidence, TestKind, TransitionBudget, UrlMatcher,
    ValidationIssueCode, VersionValidationContext, VersionValidationContribution,
    VersionValidationContributor, VersionValidationContributorError, VersionValidationError,
    VersionValidationRegistry, VersionValidationSeverity, VersionValidationSubject,
};

fn url(value: &str) -> url::Url {
    url::Url::parse(value).unwrap_or_else(|_| url::Url::parse("https://invalid.test/").unwrap())
}

fn version_with_seed() -> CrawlerVersion {
    let mut version = CrawlerVersion::draft(CrawlerId::new());
    let seed_url = url("https://example.test/");
    version
        .add_seed(Seed::new(seed_url.clone(), seed_url))
        .unwrap_or_default();
    version
}

fn context(
    version: CrawlerVersion,
    page_types: Vec<PageType>,
    transitions: Vec<DiscoveryTransition>,
    evidence: Vec<TestEvidence>,
) -> VersionValidationContext {
    VersionValidationContext::new(version, page_types, transitions, evidence, "a".repeat(64))
}

fn issue(code: &str, severity: VersionValidationSeverity) -> erabi_domain::VersionValidationIssue {
    erabi_domain::VersionValidationIssue::new(
        ValidationIssueCode::new(code)
            .unwrap_or_else(|_| ValidationIssueCode::new("TEST").unwrap()),
        severity,
        "bounded test issue",
    )
}

#[test]
fn core_reports_seed_and_semantic_blockers() {
    let version = CrawlerVersion::draft(CrawlerId::new());
    let report = VersionValidationRegistry::new()
        .validate(&context(version, Vec::new(), Vec::new(), Vec::new()))
        .unwrap();
    assert!(
        report
            .blockers
            .iter()
            .any(|issue| issue.code.as_str() == "NO_ENABLED_SEED")
    );

    let mut disabled = CrawlerVersion::draft(CrawlerId::new());
    let mut seed = Seed::new(url("https://example.test/"), url("https://example.test/"));
    seed.enabled = false;
    disabled.add_seed(seed).unwrap_or_default();
    let report = VersionValidationRegistry::new()
        .validate(&context(disabled, Vec::new(), Vec::new(), Vec::new()))
        .unwrap();
    assert!(
        report
            .blockers
            .iter()
            .any(|issue| issue.code.as_str() == "NO_ENABLED_SEED")
    );
}

#[test]
fn equal_priority_identical_exact_matchers_are_proven_ambiguous() {
    let mut version = version_with_seed();
    let exact = UrlMatcher::exact_url(url("https://example.test/"));
    let left = PageType::new("left", 10, vec![exact.clone()]);
    let right = PageType::new("right", 10, vec![exact]);
    version.set_page_type_ids(vec![left.id, right.id]).unwrap();
    let report = VersionValidationRegistry::new()
        .validate(&context(version, vec![left, right], Vec::new(), Vec::new()))
        .unwrap();
    assert!(report.blockers.iter().any(|issue| {
        issue.code.as_str() == "UNRESOLVED_PAGE_TYPE_AMBIGUITY"
            && issue.details.get("witness_url") == Some(&"https://example.test/".to_owned())
    }));
}

#[test]
fn matcher_witness_tracking_parameter_is_canonicalized_before_resolution() {
    let mut version = version_with_seed();
    let matcher = UrlMatcher::exact_url(url("https://example.test/item?utm_source=x"));
    let left = PageType::new("left", 10, vec![matcher.clone()]);
    let right = PageType::new("right", 10, vec![matcher]);
    version.set_page_type_ids(vec![left.id, right.id]).unwrap();

    let report = VersionValidationRegistry::new()
        .validate(&context(version, vec![left, right], Vec::new(), Vec::new()))
        .unwrap();

    assert!(
        !report
            .blockers
            .iter()
            .any(|issue| issue.code.as_str() == "UNRESOLVED_PAGE_TYPE_AMBIGUITY")
    );
}

#[test]
fn matcher_witness_fragment_is_canonicalized_before_resolution() {
    let mut version = version_with_seed();
    let matcher = UrlMatcher::exact_url(url("https://example.test/item#details"));
    let left = PageType::new("left", 10, vec![matcher.clone()]);
    let right = PageType::new("right", 10, vec![matcher]);
    version.set_page_type_ids(vec![left.id, right.id]).unwrap();

    let report = VersionValidationRegistry::new()
        .validate(&context(version, vec![left, right], Vec::new(), Vec::new()))
        .unwrap();

    assert!(
        !report
            .blockers
            .iter()
            .any(|issue| issue.code.as_str() == "UNRESOLVED_PAGE_TYPE_AMBIGUITY")
    );
}

#[test]
fn matcher_witness_that_remains_ambiguous_after_canonicalization_blocks() {
    let mut version = version_with_seed();
    let matcher = UrlMatcher::try_path_prefix(Some("example.test".into()), "/item").unwrap();
    let left = PageType::new("left", 10, vec![matcher.clone()]);
    let right = PageType::new("right", 10, vec![matcher]);
    version.set_page_type_ids(vec![left.id, right.id]).unwrap();

    let report = VersionValidationRegistry::new()
        .validate(&context(version, vec![left, right], Vec::new(), Vec::new()))
        .unwrap();

    assert!(report.blockers.iter().any(|issue| {
        issue.code.as_str() == "UNRESOLVED_PAGE_TYPE_AMBIGUITY"
            && issue.details.get("witness_url") == Some(&"https://example.test/item".to_owned())
    }));
}

#[test]
fn enabled_seed_ambiguity_still_uses_its_canonical_identity() {
    let mut version = CrawlerVersion::draft(CrawlerId::new());
    version
        .add_seed(Seed::new(
            url("https://example.test/item?utm_source=x"),
            url("https://example.test/item"),
        ))
        .unwrap();
    let matcher = UrlMatcher::regex(r"^https://example\.test/item$").unwrap();
    let left = PageType::new("left", 10, vec![matcher.clone()]);
    let right = PageType::new("right", 10, vec![matcher]);
    version.set_page_type_ids(vec![left.id, right.id]).unwrap();

    let report = VersionValidationRegistry::new()
        .validate(&context(version, vec![left, right], Vec::new(), Vec::new()))
        .unwrap();

    assert!(report.blockers.iter().any(|issue| {
        issue.code.as_str() == "UNRESOLVED_PAGE_TYPE_AMBIGUITY"
            && issue.details.get("witness_url") == Some(&"https://example.test/item".to_owned())
    }));
}

#[test]
fn canonical_matcher_ambiguity_is_order_and_id_independent() {
    fn ambiguity_details(reverse: bool) -> Vec<std::collections::BTreeMap<String, String>> {
        let mut version = version_with_seed();
        let matcher = UrlMatcher::try_path_prefix(Some("example.test".into()), "/item").unwrap();
        let left = PageType::new("left", 10, vec![matcher.clone()]);
        let right = PageType::new("right", 10, vec![matcher]);
        let mut page_types = vec![left, right];
        if reverse {
            page_types.reverse();
        }
        version
            .set_page_type_ids(page_types.iter().map(|page_type| page_type.id).collect())
            .unwrap();
        VersionValidationRegistry::new()
            .validate(&context(version, page_types, Vec::new(), Vec::new()))
            .unwrap()
            .blockers
            .into_iter()
            .filter(|issue| issue.code.as_str() == "UNRESOLVED_PAGE_TYPE_AMBIGUITY")
            .map(|issue| issue.details)
            .collect()
    }

    assert_eq!(ambiguity_details(false), ambiguity_details(true));
}

#[test]
fn long_canonical_ambiguity_witness_remains_a_bounded_core_blocker() {
    let mut version = version_with_seed();
    let witness = format!(
        "https://example.test/item?query={}",
        "x".repeat(MAX_VALIDATION_DETAIL_CHARS + 1)
    );
    let matcher = UrlMatcher::exact_url(url(&witness));
    let left = PageType::new("left", 10, vec![matcher.clone()]);
    let right = PageType::new("right", 10, vec![matcher]);
    version.set_page_type_ids(vec![left.id, right.id]).unwrap();

    let report = VersionValidationRegistry::new()
        .validate(&context(version, vec![left, right], Vec::new(), Vec::new()))
        .unwrap();
    assert!(!report.is_publishable());
    let issue = report
        .blockers
        .iter()
        .find(|issue| issue.code.as_str() == "UNRESOLVED_PAGE_TYPE_AMBIGUITY")
        .unwrap();
    assert!(!issue.details.contains_key("witness_url"));
    assert!(
        witness.starts_with(
            issue
                .details
                .get("witness_url_prefix")
                .unwrap_or(&String::new())
        )
    );
    assert_eq!(
        issue.details.get("witness_url_sha256").map(String::len),
        Some(64)
    );
    assert!(issue.details.iter().all(|(key, value)| {
        key.chars().count() <= MAX_VALIDATION_DETAIL_CHARS
            && value.chars().count() <= MAX_VALIDATION_DETAIL_CHARS
    }));
}

#[test]
fn semantic_ambiguity_blocker_is_stable_across_order_and_regenerated_ids() {
    fn blocker_codes(reverse: bool) -> Vec<String> {
        let mut version = version_with_seed();
        let exact = UrlMatcher::exact_url(url("https://example.test/"));
        let left = PageType::new("left", 10, vec![exact.clone()]);
        let right = PageType::new("right", 10, vec![exact]);
        let mut page_types = vec![left, right];
        if reverse {
            page_types.reverse();
        }
        version
            .set_page_type_ids(page_types.iter().map(|page_type| page_type.id).collect())
            .unwrap();
        VersionValidationRegistry::new()
            .validate(&context(version, page_types, Vec::new(), Vec::new()))
            .unwrap()
            .blockers
            .into_iter()
            .map(|issue| issue.code.as_str().to_owned())
            .collect()
    }

    let forward = blocker_codes(false);
    let reverse = blocker_codes(true);
    assert_eq!(forward, reverse);
    assert_eq!(forward, vec!["UNRESOLVED_PAGE_TYPE_AMBIGUITY"]);
}

#[test]
fn priority_and_authoritative_winner_prevent_false_ambiguity() {
    let mut version = version_with_seed();
    let exact = PageType::new(
        "exact",
        10,
        vec![UrlMatcher::exact_url(url("https://example.test/"))],
    );
    let prefix = PageType::new(
        "prefix",
        9,
        vec![UrlMatcher::try_path_prefix(None, "/").unwrap()],
    );
    version
        .set_page_type_ids(vec![exact.id, prefix.id])
        .unwrap();
    let report = VersionValidationRegistry::new()
        .validate(&context(
            version,
            vec![prefix, exact],
            Vec::new(),
            Vec::new(),
        ))
        .unwrap();
    assert!(
        !report
            .blockers
            .iter()
            .any(|issue| issue.code.as_str() == "UNRESOLVED_PAGE_TYPE_AMBIGUITY")
    );
}

#[test]
fn identical_matchers_with_different_priorities_are_not_ambiguous() {
    let mut version = version_with_seed();
    let matcher = UrlMatcher::exact_url(url("https://example.test/"));
    let higher = PageType::new("higher", 10, vec![matcher.clone()]);
    let lower = PageType::new("lower", 9, vec![matcher]);
    version
        .set_page_type_ids(vec![higher.id, lower.id])
        .unwrap();
    let report = VersionValidationRegistry::new()
        .validate(&context(
            version,
            vec![lower, higher],
            Vec::new(),
            Vec::new(),
        ))
        .unwrap();
    assert!(
        !report
            .blockers
            .iter()
            .any(|issue| issue.code.as_str() == "UNRESOLVED_PAGE_TYPE_AMBIGUITY")
    );
}

#[test]
fn regex_overlap_without_a_proof_is_not_declared_ambiguous() {
    let mut version = version_with_seed();
    let left = PageType::new(
        "left",
        10,
        vec![UrlMatcher::regex(r"^https://example\.test/items/.*$").unwrap()],
    );
    let right = PageType::new(
        "right",
        10,
        vec![UrlMatcher::regex(r"^https://example\.test/items/.*$").unwrap()],
    );
    version.set_page_type_ids(vec![left.id, right.id]).unwrap();
    let report = VersionValidationRegistry::new()
        .validate(&context(version, vec![right, left], Vec::new(), Vec::new()))
        .unwrap();
    assert!(
        !report
            .blockers
            .iter()
            .any(|issue| issue.code.as_str() == "UNRESOLVED_PAGE_TYPE_AMBIGUITY")
    );
}

#[test]
fn evidence_warnings_are_exact_hash_and_subject_qualified() {
    let mut version = version_with_seed();
    let page_type = PageType::new(
        "catalog",
        1,
        vec![UrlMatcher::exact_url(url("https://example.test/"))],
    );
    version.set_page_type_ids(vec![page_type.id]).unwrap();
    let stale = evidence(
        &version,
        page_type.id,
        "b".repeat(64),
        TestKind::PageTypeMatching,
    );
    let report = VersionValidationRegistry::new()
        .validate(&context(
            version.clone(),
            vec![page_type.clone()],
            Vec::new(),
            vec![stale],
        ))
        .unwrap();
    assert!(
        report
            .warnings
            .iter()
            .any(|issue| issue.code.as_str() == "PAGE_TYPE_TEST_EVIDENCE_MISSING")
    );

    let current = evidence(
        &version,
        page_type.id,
        "a".repeat(64),
        TestKind::PageTypeMatching,
    );
    let report = VersionValidationRegistry::new()
        .validate(&context(
            version,
            vec![page_type],
            Vec::new(),
            vec![current],
        ))
        .unwrap();
    assert!(
        !report
            .warnings
            .iter()
            .any(|issue| issue.code.as_str() == "PAGE_TYPE_TEST_EVIDENCE_MISSING")
    );
}

#[test]
fn selector_no_matches_is_a_warning_but_positive_observation_clears_it() {
    let mut version = version_with_seed();
    let page_type = PageType::new("catalog", 1, Vec::new());
    version.set_page_type_ids(vec![page_type.id]).unwrap();
    let mut no_matches = evidence(
        &version,
        page_type.id,
        "a".repeat(64),
        TestKind::SelectorCoverage,
    );
    no_matches.selector_coverage = vec![erabi_domain::SelectorCoverageEvidence {
        selector: "a.next".into(),
        matches_found: 0,
        status: SelectorCoverageStatus::NoMatches,
    }];
    let report = VersionValidationRegistry::new()
        .validate(&context(
            version.clone(),
            vec![page_type.clone()],
            Vec::new(),
            vec![no_matches],
        ))
        .unwrap();
    assert!(
        report
            .warnings
            .iter()
            .any(|issue| issue.code.as_str() == "SELECTOR_COVERAGE_UNUSABLE")
    );

    let mut positive = evidence(
        &version,
        page_type.id,
        "a".repeat(64),
        TestKind::SelectorCoverage,
    );
    positive.selector_coverage = vec![erabi_domain::SelectorCoverageEvidence {
        selector: "a.next".into(),
        matches_found: 1,
        status: SelectorCoverageStatus::Observed,
    }];
    let report = VersionValidationRegistry::new()
        .validate(&context(
            version,
            vec![page_type],
            Vec::new(),
            vec![positive],
        ))
        .unwrap();
    assert!(
        !report
            .warnings
            .iter()
            .any(|issue| issue.code.as_str() == "SELECTOR_COVERAGE_UNUSABLE")
    );
}

#[test]
fn unavailable_selector_observations_do_not_claim_no_matches() {
    let mut version = version_with_seed();
    let page_type = PageType::new("catalog", 1, Vec::new());
    version.set_page_type_ids(vec![page_type.id]).unwrap();
    let mut unavailable = evidence(
        &version,
        page_type.id,
        "a".repeat(64),
        TestKind::SelectorCoverage,
    );
    unavailable.selector_coverage = vec![SelectorCoverageEvidence {
        selector: "a.next".into(),
        matches_found: 0,
        status: SelectorCoverageStatus::Unavailable,
    }];
    let report = VersionValidationRegistry::new()
        .validate(&context(
            version,
            vec![page_type],
            Vec::new(),
            vec![unavailable],
        ))
        .unwrap();
    assert!(
        !report
            .warnings
            .iter()
            .any(|issue| issue.code.as_str() == "SELECTOR_COVERAGE_UNUSABLE")
    );
}

#[test]
fn transition_confidence_requires_current_transition_evidence() {
    let mut version = version_with_seed();
    let page_type = PageType::new("catalog", 1, Vec::new());
    version.set_page_type_ids(vec![page_type.id]).unwrap();
    let transition = DiscoveryTransition {
        id: DiscoveryTransitionId::new(),
        source_page_type_id: page_type.id,
        target_page_type_id: page_type.id,
        name: "self-link".into(),
        enabled: true,
        link_selector: "a.next".into(),
        url_constraints: None,
        priority: 1,
        budget: TransitionBudget {
            max_links_per_source_page: 1,
            total_budget: Some(1),
            depth_contribution: 1,
        },
        deduplicate: true,
        latest_test_evidence_id: None,
    };
    version.set_transition_ids(vec![transition.id]).unwrap();
    let report = VersionValidationRegistry::new()
        .validate(&context(
            version.clone(),
            vec![page_type.clone()],
            vec![transition.clone()],
            Vec::new(),
        ))
        .unwrap();
    assert!(
        report
            .warnings
            .iter()
            .any(|issue| issue.code.as_str() == "TRANSITION_TEST_EVIDENCE_MISSING")
    );

    let current = transition_evidence(&version, transition.id, "a".repeat(64));
    let report = VersionValidationRegistry::new()
        .validate(&context(
            version,
            vec![page_type],
            vec![transition],
            vec![current],
        ))
        .unwrap();
    assert!(
        !report
            .warnings
            .iter()
            .any(|issue| issue.code.as_str() == "TRANSITION_TEST_EVIDENCE_MISSING")
    );
}

struct FakeContributor {
    key: &'static str,
    issue: Option<erabi_domain::VersionValidationIssue>,
    calls: Arc<AtomicUsize>,
    failure: bool,
}

impl VersionValidationContributor for FakeContributor {
    fn key(&self) -> &'static str {
        self.key
    }

    fn validate(
        &self,
        _context: &VersionValidationContext,
    ) -> Result<VersionValidationContribution, VersionValidationContributorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.failure {
            return Err(VersionValidationContributorError::InternalFailure);
        }
        Ok(VersionValidationContribution::new(
            self.issue.clone().into_iter().collect(),
        ))
    }
}

#[test]
fn contributors_aggregate_order_independently_and_fail_closed() {
    let context = context(version_with_seed(), Vec::new(), Vec::new(), Vec::new());
    let calls_a = Arc::new(AtomicUsize::new(0));
    let calls_b = Arc::new(AtomicUsize::new(0));
    let mut first = VersionValidationRegistry::new();
    first
        .register(Arc::new(FakeContributor {
            key: "alpha",
            issue: Some(issue("ALPHA_WARNING", VersionValidationSeverity::Warning)),
            calls: Arc::clone(&calls_a),
            failure: false,
        }))
        .unwrap();
    first
        .register(Arc::new(FakeContributor {
            key: "beta",
            issue: Some(issue("BETA_BLOCKER", VersionValidationSeverity::Blocker)),
            calls: Arc::clone(&calls_b),
            failure: false,
        }))
        .unwrap();
    let report = first.validate(&context).unwrap();
    assert!(!report.is_publishable());
    assert_eq!(calls_a.load(Ordering::SeqCst), 1);
    assert_eq!(calls_b.load(Ordering::SeqCst), 1);

    let duplicate = first.register(Arc::new(FakeContributor {
        key: "alpha",
        issue: None,
        calls: Arc::new(AtomicUsize::new(0)),
        failure: false,
    }));
    assert!(matches!(
        duplicate,
        Err(VersionValidationError::DuplicateContributorKey(_))
    ));

    let mut failing = VersionValidationRegistry::new();
    failing
        .register(Arc::new(FakeContributor {
            key: "failure",
            issue: None,
            calls: Arc::new(AtomicUsize::new(0)),
            failure: true,
        }))
        .unwrap();
    assert!(matches!(
        failing.validate(&context),
        Err(VersionValidationError::ContributorFailed(_))
    ));
}

#[test]
fn contributor_issue_bounds_are_enforced() {
    let context = context(version_with_seed(), Vec::new(), Vec::new(), Vec::new());
    let mut registry = VersionValidationRegistry::new();
    registry
        .register(Arc::new(FakeContributor {
            key: "bounded",
            issue: Some(
                erabi_domain::VersionValidationIssue::new(
                    ValidationIssueCode::new("BOUNDED").unwrap(),
                    VersionValidationSeverity::Warning,
                    "x".repeat(513),
                )
                .with_subject(VersionValidationSubject::new(
                    erabi_domain::ValidationSubjectKind::new("TEST").unwrap(),
                    None,
                )),
            ),
            calls: Arc::new(AtomicUsize::new(0)),
            failure: false,
        }))
        .unwrap();
    assert!(matches!(
        registry.validate(&context),
        Err(VersionValidationError::InvalidContribution(_))
    ));
}

fn evidence(
    version: &CrawlerVersion,
    page_type_id: erabi_domain::PageTypeId,
    config_hash: String,
    test_kind: TestKind,
) -> TestEvidence {
    TestEvidence {
        schema_version: 1,
        id: erabi_domain::TestEvidenceId::new(),
        crawler_version_id: version.id(),
        test_kind,
        input_urls: vec!["https://example.test/".into()],
        evaluated_page_type_id: Some(page_type_id),
        tested_transition_id: None,
        canonicalization: Vec::new(),
        page_type_match: Vec::new(),
        extraction: None,
        selector_coverage: Vec::new(),
        pagination: None,
        discovery: None,
        warnings: Vec::new(),
        errors: Vec::new(),
        artifact_ids: Vec::new(),
        config_hash,
        executed_at: "unix:1".into(),
        published_comparison: None,
    }
}

fn transition_evidence(
    version: &CrawlerVersion,
    transition_id: DiscoveryTransitionId,
    config_hash: String,
) -> TestEvidence {
    TestEvidence {
        schema_version: 1,
        id: erabi_domain::TestEvidenceId::new(),
        crawler_version_id: version.id(),
        test_kind: TestKind::DiscoveryTransition,
        input_urls: vec!["https://example.test/".into()],
        evaluated_page_type_id: None,
        tested_transition_id: Some(transition_id),
        canonicalization: Vec::new(),
        page_type_match: Vec::new(),
        extraction: None,
        selector_coverage: Vec::new(),
        pagination: None,
        discovery: Some(DiscoveryTransitionEvidence {
            transition_id: Some(transition_id),
            transition_name: Some("self-link".into()),
            source_page_type_id: None,
            target_page_type_id: None,
            source_match: None,
            selector: SelectorCoverageEvidence {
                selector: "a.next".into(),
                matches_found: 1,
                status: SelectorCoverageStatus::Observed,
            },
            discovered_urls: Vec::new(),
            eligible_link_count: 1,
            per_page_limit: 1,
            per_page_limit_reached: false,
        }),
        warnings: Vec::new(),
        errors: Vec::new(),
        artifact_ids: Vec::new(),
        config_hash,
        executed_at: "unix:1".into(),
        published_comparison: None,
    }
}
