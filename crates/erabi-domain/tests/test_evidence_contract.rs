use erabi_domain::{
    CanonicalizationDecisionCode, CanonicalizationDecisionEvidence, CanonicalizationEvidence,
    CanonicalizationOutcome, MatcherKindEvidence, MatcherSpecificityEvidence,
    PageTypeCandidateEvidence, PageTypeMatchEvidence, PageTypeMatchStatus,
    PublishedComparisonStatus, TEST_EVIDENCE_SCHEMA_VERSION, TestDiagnostic, TestEvidence,
    TestEvidenceId, TestKind, TestLabComparison,
};

fn evidence() -> TestEvidence {
    TestEvidence {
        schema_version: TEST_EVIDENCE_SCHEMA_VERSION,
        id: TestEvidenceId::new(),
        crawler_version_id: erabi_domain::CrawlerVersionId::new(),
        test_kind: TestKind::PageTypeMatching,
        input_urls: vec!["https://example.test/items?id=1".to_owned()],
        evaluated_page_type_id: None,
        tested_transition_id: None,
        canonicalization: vec![CanonicalizationEvidence {
            original_url: "HTTPS://EXAMPLE.TEST:443/items#fragment".to_owned(),
            canonical_url: Some("https://example.test/items".to_owned()),
            outcome: CanonicalizationOutcome::Canonicalized,
            decisions: vec![CanonicalizationDecisionEvidence {
                code: CanonicalizationDecisionCode::HostNormalized,
                parameter: None,
            }],
        }],
        page_type_match: Vec::new(),
        extraction: None,
        selector_coverage: Vec::new(),
        pagination: None,
        discovery: None,
        warnings: vec![TestDiagnostic {
            code: "UNMATCHED_PAGE_TYPE".to_owned(),
            message: "No PageType matched.".to_owned(),
        }],
        errors: Vec::new(),
        artifact_ids: Vec::new(),
        config_hash: "00".repeat(32),
        executed_at: "unix:1".to_owned(),
        published_comparison: None,
    }
}

fn candidate(id: erabi_domain::PageTypeId, name: &str) -> PageTypeCandidateEvidence {
    PageTypeCandidateEvidence {
        page_type_id: id,
        page_type_name: name.to_owned(),
        priority: 10,
        matcher_kind: MatcherKindEvidence::ExactUrl,
        specificity: MatcherSpecificityEvidence {
            matcher_kind_rank: 4,
            literal_path_segments: 1,
            explicit_query_constraints: 0,
            literal_characters: 24,
            wildcard_capture_count: 0,
        },
        matched_patterns: vec!["https://example.test/items".to_owned()],
    }
}

#[test]
fn typed_evidence_round_trips_and_keeps_uuid_v7_identity() -> Result<(), Box<dyn std::error::Error>>
{
    let evidence = evidence();
    assert_eq!(evidence.id.as_uuid().get_version_num(), 7);
    let encoded = serde_json::to_string(&evidence)?;
    let decoded: TestEvidence = serde_json::from_str(&encoded)?;
    assert_eq!(decoded, evidence);
    Ok(())
}

#[test]
fn evidence_preserves_match_winner_ambiguity_and_unmatched_states() {
    let first = candidate(erabi_domain::PageTypeId::new(), "First");
    let second = candidate(erabi_domain::PageTypeId::new(), "Second");
    let matched = PageTypeMatchEvidence {
        decision: PageTypeMatchStatus::Matched,
        winner: Some(first.clone()),
        candidates: vec![first.clone()],
    };
    assert_eq!(matched.winner, Some(first.clone()));
    let ambiguous = PageTypeMatchEvidence {
        decision: PageTypeMatchStatus::Ambiguous,
        winner: None,
        candidates: vec![first, second],
    };
    assert_eq!(ambiguous.candidates.len(), 2);
    assert!(ambiguous.winner.is_none());
    let unmatched = PageTypeMatchEvidence {
        decision: PageTypeMatchStatus::Unmatched,
        winner: None,
        candidates: Vec::new(),
    };
    assert!(unmatched.candidates.is_empty());
}

#[test]
fn unsupported_schema_and_unknown_test_kind_fail_typed_validation()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = serde_json::to_value(evidence())?;
    value["schema_version"] = serde_json::json!(2);
    assert!(serde_json::from_value::<TestEvidence>(value).is_err());
    let mut value = serde_json::to_value(evidence())?;
    value["test_kind"] = serde_json::json!("NOT_A_TEST");
    assert!(serde_json::from_value::<TestEvidence>(value).is_err());
    Ok(())
}

#[test]
fn invalid_evidence_diagnostics_and_unsorted_artifacts_are_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = serde_json::to_value(evidence())?;
    value["warnings"][0]["message"] = serde_json::json!("line\nleak");
    assert!(serde_json::from_value::<TestEvidence>(value).is_err());
    let mut value = serde_json::to_value(evidence())?;
    let mut artifact_ids = [
        erabi_domain::ArtifactId::new(),
        erabi_domain::ArtifactId::new(),
    ];
    artifact_ids.sort_by_key(ToString::to_string);
    artifact_ids.reverse();
    value["artifact_ids"] =
        serde_json::json!([artifact_ids[0].to_string(), artifact_ids[1].to_string()]);
    assert!(serde_json::from_value::<TestEvidence>(value).is_err());
    Ok(())
}

#[test]
fn unrelated_evidence_cannot_claim_a_tested_transition() {
    let mut evidence = evidence();
    evidence.test_kind = TestKind::UrlCanonicalization;
    evidence.tested_transition_id = Some(erabi_domain::DiscoveryTransitionId::new());
    assert!(evidence.validate().is_err());
}

#[test]
fn page_type_comparison_ignores_regenerated_ids_and_ambiguity_order() {
    let published_first = candidate(erabi_domain::PageTypeId::new(), "Duplicate");
    let published_second = candidate(erabi_domain::PageTypeId::new(), "Duplicate");
    let draft_first = candidate(erabi_domain::PageTypeId::new(), "Duplicate");
    let draft_second = candidate(erabi_domain::PageTypeId::new(), "Duplicate");
    let published = PageTypeMatchEvidence {
        decision: PageTypeMatchStatus::Ambiguous,
        winner: None,
        candidates: vec![published_first, published_second],
    };
    let draft = PageTypeMatchEvidence {
        decision: PageTypeMatchStatus::Ambiguous,
        winner: None,
        candidates: vec![draft_second, draft_first],
    };
    assert!(draft.behaviorally_equivalent(&published));
}

#[test]
fn persisted_comparison_rejects_contradictory_redundant_booleans() {
    let mut evidence = evidence();
    let published_id = erabi_domain::CrawlerVersionId::new();
    evidence.published_comparison = Some(TestLabComparison {
        status: PublishedComparisonStatus::Compared,
        draft_version_id: evidence.crawler_version_id,
        draft_config_hash: evidence.config_hash.clone(),
        published_version_id: Some(published_id),
        published_config_hash: Some("11".repeat(32)),
        canonicalization_difference: false,
        draft_canonicalization: evidence.canonicalization.clone(),
        published_canonicalization: vec![CanonicalizationEvidence {
            original_url: "https://example.test/changed".to_owned(),
            canonical_url: Some("https://example.test/changed".to_owned()),
            outcome: CanonicalizationOutcome::Canonicalized,
            decisions: Vec::new(),
        }],
        page_type_match_difference: false,
        draft_page_type_match: Vec::new(),
        published_page_type_match: Vec::new(),
        discovery_difference: None,
        extraction_difference: None,
        warnings: Vec::new(),
    });
    assert!(evidence.validate().is_err());
}
