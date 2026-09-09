//! API-local schemas for domain-owned values that cross the HTTP boundary.
//!
//! These types intentionally mirror the domain serde representation without
//! adding documentation dependencies to the domain crate.

use utoipa::ToSchema;

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = TestKind)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TestKindSchema {
    UrlCanonicalization,
    PageTypeMatching,
    Extraction,
    SelectorCoverage,
    Pagination,
    DiscoveryTransition,
    DiscoveredUrlPreview,
    CombinedUrlEvaluation,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = CanonicalizationOutcome)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum CanonicalizationOutcomeSchema {
    Canonicalized,
    InvalidUrl,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = CanonicalizationDecisionCode)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum CanonicalizationDecisionCodeSchema {
    SchemeNormalized,
    HostNormalized,
    DefaultPortRemoved,
    FragmentRemoved,
    PathNormalized,
    QuerySorted,
    TrackingParameterRemoved,
    CustomParameterDropped,
    ExplicitParameterKept,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = CanonicalizationDecisionEvidence)]
pub(crate) struct CanonicalizationDecisionEvidenceSchema {
    pub(crate) code: CanonicalizationDecisionCodeSchema,
    #[schema(required = true)]
    pub(crate) parameter: Option<String>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = CanonicalizationEvidence)]
pub(crate) struct CanonicalizationEvidenceSchema {
    pub(crate) original_url: String,
    #[schema(required = true)]
    pub(crate) canonical_url: Option<String>,
    pub(crate) outcome: CanonicalizationOutcomeSchema,
    pub(crate) decisions: Vec<CanonicalizationDecisionEvidenceSchema>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = MatcherKindEvidence)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum MatcherKindEvidenceSchema {
    ExactUrl,
    ExactHostPathTemplate,
    PathPrefixOrGlob,
    Regex,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = MatcherSpecificityEvidence)]
pub(crate) struct MatcherSpecificityEvidenceSchema {
    pub(crate) matcher_kind_rank: u8,
    pub(crate) literal_path_segments: u32,
    pub(crate) explicit_query_constraints: u32,
    pub(crate) literal_characters: u32,
    pub(crate) wildcard_capture_count: u32,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PageTypeCandidateEvidence)]
pub(crate) struct PageTypeCandidateEvidenceSchema {
    pub(crate) page_type_id: String,
    pub(crate) page_type_name: String,
    pub(crate) priority: i32,
    pub(crate) matcher_kind: MatcherKindEvidenceSchema,
    pub(crate) specificity: MatcherSpecificityEvidenceSchema,
    pub(crate) matched_patterns: Vec<String>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PageTypeMatchStatus)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum PageTypeMatchStatusSchema {
    Matched,
    Ambiguous,
    Unmatched,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PageTypeMatchEvidence)]
pub(crate) struct PageTypeMatchEvidenceSchema {
    pub(crate) decision: PageTypeMatchStatusSchema,
    #[schema(required = true)]
    pub(crate) winner: Option<PageTypeCandidateEvidenceSchema>,
    pub(crate) candidates: Vec<PageTypeCandidateEvidenceSchema>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = DomainScopeStatus)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum DomainScopeStatusSchema {
    InScope,
    External,
    Blocked,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = DomainScopeRationaleEvidence)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum DomainScopeRationaleEvidenceSchema {
    SeedHost,
    RegistrableDomain,
    ExplicitSubdomain,
    UnselectedSubdomain,
    ExplicitAllowlist,
    OutsideSeedDomains,
    OutsideAllowlist,
    ExplicitBlock,
    CustomAllow,
    OutsideCustomAllow,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = DomainScopeEvidence)]
pub(crate) struct DomainScopeEvidenceSchema {
    pub(crate) classification: DomainScopeStatusSchema,
    pub(crate) host: String,
    pub(crate) rationale: DomainScopeRationaleEvidenceSchema,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = SelectorCoverageStatus)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum SelectorCoverageStatusSchema {
    Observed,
    NoMatches,
    Unavailable,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = SelectorCoverageEvidence)]
pub(crate) struct SelectorCoverageEvidenceSchema {
    pub(crate) selector: String,
    pub(crate) matches_found: u32,
    pub(crate) status: SelectorCoverageStatusSchema,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = ExtractionFieldEvidence)]
pub(crate) struct ExtractionFieldEvidenceSchema {
    pub(crate) name: String,
    pub(crate) observed: bool,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = TestDiagnostic)]
pub(crate) struct TestDiagnosticSchema {
    pub(crate) code: String,
    pub(crate) message: String,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = ExtractionObservation)]
#[serde(
    tag = "status",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
pub(crate) enum ExtractionObservationSchema {
    Available {
        fields: Vec<ExtractionFieldEvidenceSchema>,
    },
    Unavailable {
        reason: String,
    },
    Error {
        diagnostic: TestDiagnosticSchema,
    },
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PaginationKind)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum PaginationKindSchema {
    RelNext,
    NextOlderMoreLink,
    NumberedPagination,
    UrlPageNumber,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PaginationEvidence)]
pub(crate) struct PaginationEvidenceSchema {
    pub(crate) kind: PaginationKindSchema,
    pub(crate) selector: Option<String>,
    pub(crate) target_url: Option<String>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = TransitionBudgetExclusionEvidence)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TransitionBudgetExclusionEvidenceSchema {
    MaxPages,
    MaxDuration,
    MaxDepth,
    MaxDownloadedBytes,
    PageTypePageBudget,
    TransitionPerPageLinkLimit,
    TransitionTotalBudget,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = TransitionBudgetEvidence)]
pub(crate) struct TransitionBudgetEvidenceSchema {
    pub(crate) allowed: bool,
    pub(crate) exclusion: Option<TransitionBudgetExclusionEvidenceSchema>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = DiscoveredUrlEvidence)]
pub(crate) struct DiscoveredUrlEvidenceSchema {
    pub(crate) raw_href: String,
    pub(crate) resolved_original_url: Option<String>,
    pub(crate) canonical_url: Option<String>,
    pub(crate) canonicalization: Option<CanonicalizationEvidenceSchema>,
    pub(crate) scope: Option<DomainScopeEvidenceSchema>,
    pub(crate) duplicate: bool,
    pub(crate) duplicate_of_canonical_url: Option<String>,
    pub(crate) page_type_match: Option<PageTypeMatchEvidenceSchema>,
    pub(crate) transition_eligible: bool,
    pub(crate) budget: Option<TransitionBudgetEvidenceSchema>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = DiscoveryTransitionEvidence)]
pub(crate) struct DiscoveryTransitionEvidenceSchema {
    pub(crate) transition_id: Option<String>,
    pub(crate) transition_name: Option<String>,
    pub(crate) source_page_type_id: Option<String>,
    pub(crate) target_page_type_id: Option<String>,
    pub(crate) source_match: Option<PageTypeMatchEvidenceSchema>,
    pub(crate) selector: SelectorCoverageEvidenceSchema,
    pub(crate) discovered_urls: Vec<DiscoveredUrlEvidenceSchema>,
    pub(crate) eligible_link_count: u32,
    pub(crate) per_page_limit: u32,
    pub(crate) per_page_limit_reached: bool,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PublishedComparisonStatus)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum PublishedComparisonStatusSchema {
    Compared,
    NoActivePublishedVersion,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = TestLabComparison)]
pub(crate) struct TestLabComparisonSchema {
    pub(crate) status: PublishedComparisonStatusSchema,
    pub(crate) draft_version_id: String,
    pub(crate) draft_config_hash: String,
    pub(crate) published_version_id: Option<String>,
    pub(crate) published_config_hash: Option<String>,
    pub(crate) canonicalization_difference: bool,
    pub(crate) draft_canonicalization: Vec<CanonicalizationEvidenceSchema>,
    pub(crate) published_canonicalization: Vec<CanonicalizationEvidenceSchema>,
    pub(crate) page_type_match_difference: bool,
    pub(crate) draft_page_type_match: Vec<PageTypeMatchEvidenceSchema>,
    pub(crate) published_page_type_match: Vec<PageTypeMatchEvidenceSchema>,
    pub(crate) discovery_difference: Option<bool>,
    pub(crate) extraction_difference: Option<bool>,
    pub(crate) warnings: Vec<TestDiagnosticSchema>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = TestEvidence)]
pub(crate) struct TestEvidenceSchema {
    pub(crate) schema_version: u16,
    pub(crate) id: String,
    pub(crate) crawler_version_id: String,
    pub(crate) test_kind: TestKindSchema,
    pub(crate) input_urls: Vec<String>,
    pub(crate) evaluated_page_type_id: Option<String>,
    pub(crate) tested_transition_id: Option<String>,
    pub(crate) canonicalization: Vec<CanonicalizationEvidenceSchema>,
    pub(crate) page_type_match: Vec<PageTypeMatchEvidenceSchema>,
    pub(crate) extraction: Option<ExtractionObservationSchema>,
    pub(crate) selector_coverage: Vec<SelectorCoverageEvidenceSchema>,
    pub(crate) pagination: Option<PaginationEvidenceSchema>,
    pub(crate) discovery: Option<DiscoveryTransitionEvidenceSchema>,
    pub(crate) warnings: Vec<TestDiagnosticSchema>,
    pub(crate) errors: Vec<TestDiagnosticSchema>,
    pub(crate) artifact_ids: Vec<String>,
    pub(crate) config_hash: String,
    pub(crate) executed_at: String,
    pub(crate) published_comparison: Option<TestLabComparisonSchema>,
}
