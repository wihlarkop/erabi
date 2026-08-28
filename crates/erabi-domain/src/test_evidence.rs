use std::collections::BTreeSet;

use crate::{
    ArtifactId, CrawlerVersionId, DiscoveryTransitionId, DomainScopeClassification,
    DomainScopeRationale, PageTypeCandidate, PageTypeId, PageTypeMatchDecision, SpecificityKey,
    TestEvidenceId, UrlMatcherKind,
};

/// Version of the durable historical `TestEvidence` contract.
pub const TEST_EVIDENCE_SCHEMA_VERSION: u16 = 1;
pub const MAX_TEST_EVIDENCE_INPUT_URLS: usize = 8;
pub const MAX_TEST_EVIDENCE_DISCOVERED_URLS: usize = 64;
pub const MAX_TEST_EVIDENCE_ARTIFACTS: usize = 16;
pub const MAX_TEST_EVIDENCE_DIAGNOSTICS: usize = 32;
pub const MAX_TEST_EVIDENCE_URL_CHARS: usize = 4_096;
pub const MAX_TEST_EVIDENCE_DIAGNOSTIC_CODE_CHARS: usize = 128;
pub const MAX_TEST_EVIDENCE_DIAGNOSTIC_MESSAGE_CHARS: usize = 1_024;

/// Focused Test Lab concerns supported by the versioned evidence contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TestKind {
    UrlCanonicalization,
    PageTypeMatching,
    Extraction,
    SelectorCoverage,
    Pagination,
    DiscoveryTransition,
    DiscoveredUrlPreview,
    CombinedUrlEvaluation,
}

/// Stable historical representation of a canonicalization pipeline result.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalizationEvidence {
    pub original_url: String,
    pub canonical_url: Option<String>,
    pub outcome: CanonicalizationOutcome,
    pub decisions: Vec<CanonicalizationDecisionEvidence>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CanonicalizationOutcome {
    Canonicalized,
    InvalidUrl,
}

/// Stable copy of a canonicalization decision. This intentionally does not
/// serialize the executable policy enum directly.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalizationDecisionEvidence {
    pub code: CanonicalizationDecisionCode,
    pub parameter: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CanonicalizationDecisionCode {
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

/// Stable historical representation of one `PageType` candidate.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageTypeCandidateEvidence {
    pub page_type_id: PageTypeId,
    pub page_type_name: String,
    pub priority: i32,
    pub matcher_kind: MatcherKindEvidence,
    pub specificity: MatcherSpecificityEvidence,
    pub matched_patterns: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MatcherKindEvidence {
    ExactUrl,
    ExactHostPathTemplate,
    PathPrefixOrGlob,
    Regex,
}

/// Stable copy of the deterministic matcher specificity tuple.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatcherSpecificityEvidence {
    pub matcher_kind_rank: u8,
    pub literal_path_segments: u32,
    pub explicit_query_constraints: u32,
    pub literal_characters: u32,
    pub wildcard_capture_count: u32,
}

/// Stable historical `PageType` decision, including every ambiguity candidate.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageTypeMatchEvidence {
    pub decision: PageTypeMatchStatus,
    pub winner: Option<PageTypeCandidateEvidence>,
    pub candidates: Vec<PageTypeCandidateEvidence>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PageTypeMatchStatus {
    Matched,
    Ambiguous,
    Unmatched,
}

impl PageTypeMatchEvidence {
    #[must_use]
    pub fn from_decision(decision: &PageTypeMatchDecision) -> Self {
        match decision {
            PageTypeMatchDecision::Matched(candidate) => {
                let candidate = PageTypeCandidateEvidence::from_candidate(candidate);
                Self {
                    decision: PageTypeMatchStatus::Matched,
                    winner: Some(candidate.clone()),
                    candidates: vec![candidate],
                }
            }
            PageTypeMatchDecision::Ambiguous { candidates } => Self {
                decision: PageTypeMatchStatus::Ambiguous,
                winner: None,
                candidates: candidates
                    .iter()
                    .map(PageTypeCandidateEvidence::from_candidate)
                    .collect(),
            },
            PageTypeMatchDecision::Unmatched => Self {
                decision: PageTypeMatchStatus::Unmatched,
                winner: None,
                candidates: Vec::new(),
            },
        }
    }

    /// Compares PageType-match behavior without treating version-local
    /// `PageType` identities as behavior. Historical evidence keeps the exact
    /// IDs for provenance; a Published-to-Draft clone deliberately has new
    /// IDs and must still compare by its complete resolution semantics.
    #[must_use]
    pub fn behaviorally_equivalent(&self, other: &Self) -> bool {
        self.decision == other.decision
            && semantic_candidate_multiset(&self.candidates)
                == semantic_candidate_multiset(&other.candidates)
            && semantic_winner(self.winner.as_ref()) == semantic_winner(other.winner.as_ref())
    }
}

/// Compares URL-by-URL `PageType` matching results by behavior. URL-batch order
/// remains significant, while ambiguity-candidate order does not.
#[must_use]
pub fn page_type_match_sequences_behaviorally_equivalent(
    left: &[PageTypeMatchEvidence],
    right: &[PageTypeMatchEvidence],
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.behaviorally_equivalent(right))
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SemanticPageTypeCandidate {
    page_type_name: String,
    priority: i32,
    matcher_kind_rank: u8,
    specificity: (u8, u32, u32, u32, u32),
    matched_patterns: Vec<String>,
}

fn semantic_candidate_multiset(
    candidates: &[PageTypeCandidateEvidence],
) -> Vec<SemanticPageTypeCandidate> {
    let mut candidates = candidates
        .iter()
        .map(SemanticPageTypeCandidate::from_evidence)
        .collect::<Vec<_>>();
    candidates.sort();
    candidates
}

fn semantic_winner(
    winner: Option<&PageTypeCandidateEvidence>,
) -> Option<SemanticPageTypeCandidate> {
    winner.map(SemanticPageTypeCandidate::from_evidence)
}

impl SemanticPageTypeCandidate {
    fn from_evidence(candidate: &PageTypeCandidateEvidence) -> Self {
        let mut matched_patterns = candidate.matched_patterns.clone();
        matched_patterns.sort();
        Self {
            page_type_name: candidate.page_type_name.clone(),
            priority: candidate.priority,
            matcher_kind_rank: matcher_kind_rank(candidate.matcher_kind),
            specificity: (
                candidate.specificity.matcher_kind_rank,
                candidate.specificity.literal_path_segments,
                candidate.specificity.explicit_query_constraints,
                candidate.specificity.literal_characters,
                candidate.specificity.wildcard_capture_count,
            ),
            matched_patterns,
        }
    }
}

const fn matcher_kind_rank(kind: MatcherKindEvidence) -> u8 {
    match kind {
        MatcherKindEvidence::ExactUrl => 4,
        MatcherKindEvidence::ExactHostPathTemplate => 3,
        MatcherKindEvidence::PathPrefixOrGlob => 2,
        MatcherKindEvidence::Regex => 1,
    }
}

impl PageTypeCandidateEvidence {
    #[must_use]
    pub fn from_candidate(candidate: &PageTypeCandidate) -> Self {
        Self {
            page_type_id: candidate.page_type_id,
            page_type_name: candidate.page_type_name.clone(),
            priority: candidate.priority,
            matcher_kind: MatcherKindEvidence::from_kind(candidate.matcher_kind),
            specificity: MatcherSpecificityEvidence::from_key(candidate.specificity),
            matched_patterns: candidate.matched_patterns.clone(),
        }
    }
}

impl MatcherKindEvidence {
    #[must_use]
    pub const fn from_kind(kind: UrlMatcherKind) -> Self {
        match kind {
            UrlMatcherKind::ExactUrl => Self::ExactUrl,
            UrlMatcherKind::ExactHostPathTemplate => Self::ExactHostPathTemplate,
            UrlMatcherKind::PathPrefixOrGlob => Self::PathPrefixOrGlob,
            UrlMatcherKind::Regex => Self::Regex,
        }
    }
}

impl MatcherSpecificityEvidence {
    #[must_use]
    pub const fn from_key(key: SpecificityKey) -> Self {
        Self {
            matcher_kind_rank: key.matcher_kind_rank,
            literal_path_segments: key.literal_path_segments,
            explicit_query_constraints: key.explicit_query_constraints,
            literal_characters: key.literal_characters,
            wildcard_capture_count: key.wildcard_capture_count(),
        }
    }
}

/// Stable copy of Domain Scope classification used for a discovered URL.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainScopeEvidence {
    pub classification: DomainScopeStatus,
    pub host: String,
    pub rationale: DomainScopeRationaleEvidence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DomainScopeStatus {
    InScope,
    External,
    Blocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DomainScopeRationaleEvidence {
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

impl DomainScopeEvidence {
    #[must_use]
    pub fn from_classification(classification: &DomainScopeClassification) -> Self {
        let (classification, host, rationale) = match classification {
            DomainScopeClassification::InScope { host, rationale } => {
                (DomainScopeStatus::InScope, host, rationale)
            }
            DomainScopeClassification::External { host, rationale } => {
                (DomainScopeStatus::External, host, rationale)
            }
            DomainScopeClassification::Blocked { host, rationale } => {
                (DomainScopeStatus::Blocked, host, rationale)
            }
        };
        Self {
            classification,
            host: host.clone(),
            rationale: DomainScopeRationaleEvidence::from_rationale(rationale),
        }
    }
}

impl DomainScopeRationaleEvidence {
    #[must_use]
    pub const fn from_rationale(rationale: &DomainScopeRationale) -> Self {
        match rationale {
            DomainScopeRationale::SeedHost => Self::SeedHost,
            DomainScopeRationale::RegistrableDomain => Self::RegistrableDomain,
            DomainScopeRationale::ExplicitSubdomain => Self::ExplicitSubdomain,
            DomainScopeRationale::UnselectedSubdomain => Self::UnselectedSubdomain,
            DomainScopeRationale::ExplicitAllowlist => Self::ExplicitAllowlist,
            DomainScopeRationale::OutsideSeedDomains => Self::OutsideSeedDomains,
            DomainScopeRationale::OutsideAllowlist => Self::OutsideAllowlist,
            DomainScopeRationale::ExplicitBlock => Self::ExplicitBlock,
            DomainScopeRationale::CustomAllow => Self::CustomAllow,
            DomainScopeRationale::OutsideCustomAllow => Self::OutsideCustomAllow,
        }
    }
}

/// Stable selector observation supplied by a provider or extraction hook.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectorCoverageEvidence {
    pub selector: String,
    pub matches_found: u32,
    pub status: SelectorCoverageStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SelectorCoverageStatus {
    Observed,
    NoMatches,
    Unavailable,
}

/// Stable extraction-hook result. It contains observations only and does not
/// define Plan 07 schemas, identity, Dataset, or record semantics.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "status",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
pub enum ExtractionObservation {
    Available {
        fields: Vec<ExtractionFieldEvidence>,
    },
    Unavailable {
        reason: String,
    },
    Error {
        diagnostic: TestDiagnostic,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtractionFieldEvidence {
    pub name: String,
    pub observed: bool,
}

/// Focused MVP pagination observations; no traversal is implied.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaginationEvidence {
    pub kind: PaginationKind,
    pub selector: Option<String>,
    pub target_url: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaginationKind {
    RelNext,
    NextOlderMoreLink,
    NumberedPagination,
    UrlPageNumber,
}

/// Bounded outcome of one discovered href in a focused Test Lab observation.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveredUrlEvidence {
    pub raw_href: String,
    pub resolved_original_url: Option<String>,
    pub canonical_url: Option<String>,
    pub canonicalization: Option<CanonicalizationEvidence>,
    pub scope: Option<DomainScopeEvidence>,
    pub duplicate: bool,
    pub duplicate_of_canonical_url: Option<String>,
    pub page_type_match: Option<PageTypeMatchEvidence>,
    pub transition_eligible: bool,
    pub budget: Option<TransitionBudgetEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionBudgetEvidence {
    pub allowed: bool,
    pub exclusion: Option<TransitionBudgetExclusionEvidence>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransitionBudgetExclusionEvidence {
    MaxPages,
    MaxDuration,
    MaxDepth,
    MaxDownloadedBytes,
    PageTypePageBudget,
    TransitionPerPageLinkLimit,
    TransitionTotalBudget,
}

/// Evidence for one explicitly selected transition. Links are observations;
/// none are followed or enqueued by Test Lab.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryTransitionEvidence {
    pub transition_id: Option<DiscoveryTransitionId>,
    pub transition_name: Option<String>,
    pub source_page_type_id: Option<PageTypeId>,
    pub target_page_type_id: Option<PageTypeId>,
    pub source_match: Option<PageTypeMatchEvidence>,
    pub selector: SelectorCoverageEvidence,
    pub discovered_urls: Vec<DiscoveredUrlEvidence>,
    pub eligible_link_count: u32,
    pub per_page_limit: u32,
    pub per_page_limit_reached: bool,
}

/// Typed comparison against the exact Published version captured at start.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestLabComparison {
    pub status: PublishedComparisonStatus,
    pub draft_version_id: CrawlerVersionId,
    pub draft_config_hash: String,
    pub published_version_id: Option<CrawlerVersionId>,
    pub published_config_hash: Option<String>,
    pub canonicalization_difference: bool,
    pub draft_canonicalization: Vec<CanonicalizationEvidence>,
    pub published_canonicalization: Vec<CanonicalizationEvidence>,
    pub page_type_match_difference: bool,
    pub draft_page_type_match: Vec<PageTypeMatchEvidence>,
    pub published_page_type_match: Vec<PageTypeMatchEvidence>,
    pub discovery_difference: Option<bool>,
    pub extraction_difference: Option<bool>,
    pub warnings: Vec<TestDiagnostic>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublishedComparisonStatus {
    Compared,
    NoActivePublishedVersion,
}

/// A sanitized, structured diagnostic retained with evidence.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestDiagnostic {
    pub code: String,
    pub message: String,
}

/// Durable historical Test Lab evidence. This is never an approval record.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct TestEvidence {
    pub schema_version: u16,
    pub id: TestEvidenceId,
    pub crawler_version_id: CrawlerVersionId,
    pub test_kind: TestKind,
    pub input_urls: Vec<String>,
    pub evaluated_page_type_id: Option<PageTypeId>,
    pub tested_transition_id: Option<DiscoveryTransitionId>,
    pub canonicalization: Vec<CanonicalizationEvidence>,
    pub page_type_match: Vec<PageTypeMatchEvidence>,
    pub extraction: Option<ExtractionObservation>,
    pub selector_coverage: Vec<SelectorCoverageEvidence>,
    pub pagination: Option<PaginationEvidence>,
    pub discovery: Option<DiscoveryTransitionEvidence>,
    pub warnings: Vec<TestDiagnostic>,
    pub errors: Vec<TestDiagnostic>,
    pub artifact_ids: Vec<ArtifactId>,
    pub config_hash: String,
    pub executed_at: String,
    pub published_comparison: Option<TestLabComparison>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TestEvidenceWire {
    schema_version: u16,
    id: TestEvidenceId,
    crawler_version_id: CrawlerVersionId,
    test_kind: TestKind,
    input_urls: Vec<String>,
    evaluated_page_type_id: Option<PageTypeId>,
    tested_transition_id: Option<DiscoveryTransitionId>,
    canonicalization: Vec<CanonicalizationEvidence>,
    page_type_match: Vec<PageTypeMatchEvidence>,
    extraction: Option<ExtractionObservation>,
    selector_coverage: Vec<SelectorCoverageEvidence>,
    pagination: Option<PaginationEvidence>,
    discovery: Option<DiscoveryTransitionEvidence>,
    warnings: Vec<TestDiagnostic>,
    errors: Vec<TestDiagnostic>,
    artifact_ids: Vec<ArtifactId>,
    config_hash: String,
    executed_at: String,
    published_comparison: Option<TestLabComparison>,
}

impl From<TestEvidenceWire> for TestEvidence {
    fn from(wire: TestEvidenceWire) -> Self {
        Self {
            schema_version: wire.schema_version,
            id: wire.id,
            crawler_version_id: wire.crawler_version_id,
            test_kind: wire.test_kind,
            input_urls: wire.input_urls,
            evaluated_page_type_id: wire.evaluated_page_type_id,
            tested_transition_id: wire.tested_transition_id,
            canonicalization: wire.canonicalization,
            page_type_match: wire.page_type_match,
            extraction: wire.extraction,
            selector_coverage: wire.selector_coverage,
            pagination: wire.pagination,
            discovery: wire.discovery,
            warnings: wire.warnings,
            errors: wire.errors,
            artifact_ids: wire.artifact_ids,
            config_hash: wire.config_hash,
            executed_at: wire.executed_at,
            published_comparison: wire.published_comparison,
        }
    }
}

impl<'de> serde::Deserialize<'de> for TestEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let evidence = TestEvidence::from(TestEvidenceWire::deserialize(deserializer)?);
        evidence.validate().map_err(serde::de::Error::custom)?;
        Ok(evidence)
    }
}

impl TestEvidence {
    /// Validates the bounded, versioned historical evidence contract.
    ///
    /// # Errors
    /// Returns a sanitized validation message when the evidence is malformed,
    /// outside its bounds, or uses an unsupported schema version.
    #[allow(clippy::too_many_lines)]
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != TEST_EVIDENCE_SCHEMA_VERSION {
            return Err("unsupported TestEvidence schema version".into());
        }
        if self.id.as_uuid().get_version_num() != 7
            || self.crawler_version_id.as_uuid().get_version_num() != 7
            || self
                .evaluated_page_type_id
                .is_some_and(|id| id.as_uuid().get_version_num() != 7)
            || self
                .tested_transition_id
                .is_some_and(|id| id.as_uuid().get_version_num() != 7)
            || self
                .artifact_ids
                .iter()
                .any(|id| id.as_uuid().get_version_num() != 7)
        {
            return Err("TestEvidence contains an invalid UUIDv7 identity".into());
        }
        if self.input_urls.is_empty() || self.input_urls.len() > MAX_TEST_EVIDENCE_INPUT_URLS {
            return Err("TestEvidence input URL count is outside its bounded range".into());
        }
        if self.input_urls.iter().any(|url| {
            url.is_empty()
                || url.chars().count() > MAX_TEST_EVIDENCE_URL_CHARS
                || url.chars().any(char::is_control)
        }) {
            return Err("TestEvidence contains an invalid input URL".into());
        }
        if self.config_hash.len() != 64
            || !self
                .config_hash
                .chars()
                .all(|character| character.is_ascii_hexdigit())
        {
            return Err("TestEvidence config hash is not a SHA-256 hex value".into());
        }
        if self.executed_at.trim().is_empty() || self.executed_at.chars().any(char::is_control) {
            return Err("TestEvidence execution timestamp is invalid".into());
        }
        if self.artifact_ids.len() > MAX_TEST_EVIDENCE_ARTIFACTS
            || !is_sorted_unique(&self.artifact_ids)
        {
            return Err("TestEvidence artifact references are not deterministic".into());
        }
        match self.test_kind {
            TestKind::Extraction
                if self.evaluated_page_type_id.is_none() || self.extraction.is_none() =>
            {
                return Err("extraction evidence is incomplete".into());
            }
            TestKind::DiscoveryTransition
                if self.tested_transition_id.is_none()
                    || self.discovery.as_ref().is_none_or(|discovery| {
                        discovery.transition_id != self.tested_transition_id
                    }) =>
            {
                return Err("transition evidence is incomplete".into());
            }
            TestKind::DiscoveredUrlPreview
                if self.discovery.as_ref().is_none_or(|discovery| {
                    discovery.transition_id.is_some()
                        || discovery.transition_name.is_some()
                        || discovery.source_page_type_id.is_some()
                        || discovery.target_page_type_id.is_some()
                }) =>
            {
                return Err("discovered URL preview evidence is incomplete".into());
            }
            _ => {}
        }
        if self.tested_transition_id.is_some()
            && self
                .discovery
                .as_ref()
                .is_some_and(|discovery| discovery.transition_id != self.tested_transition_id)
        {
            return Err("TestEvidence transition identity is inconsistent".into());
        }
        if self.canonicalization.len() > MAX_TEST_EVIDENCE_INPUT_URLS
            || self.page_type_match.len() > MAX_TEST_EVIDENCE_INPUT_URLS
            || self.selector_coverage.len() > MAX_TEST_EVIDENCE_INPUT_URLS
        {
            return Err("TestEvidence observation count is outside its bounded range".into());
        }
        if self.warnings.len() > MAX_TEST_EVIDENCE_DIAGNOSTICS
            || self.errors.len() > MAX_TEST_EVIDENCE_DIAGNOSTICS
        {
            return Err("TestEvidence diagnostic count is outside its bounded range".into());
        }
        for diagnostic in self.warnings.iter().chain(&self.errors) {
            validate_diagnostic(diagnostic)?;
        }
        for evidence in &self.canonicalization {
            validate_canonicalization(evidence)?;
        }
        for evidence in &self.page_type_match {
            validate_page_type_match(evidence)?;
        }
        for evidence in &self.selector_coverage {
            validate_selector_coverage(evidence)?;
        }
        if let Some(pagination) = &self.pagination {
            validate_pagination(pagination)?;
        }
        if let Some(discovery) = &self.discovery {
            validate_discovery(discovery)?;
        }
        if let Some(extraction) = &self.extraction {
            validate_extraction(extraction)?;
        }
        if let Some(comparison) = &self.published_comparison {
            validate_comparison(comparison, self)?;
        }
        Ok(())
    }
}

fn validate_canonicalization(evidence: &CanonicalizationEvidence) -> Result<(), String> {
    if evidence.original_url.is_empty()
        || evidence.original_url.chars().count() > MAX_TEST_EVIDENCE_URL_CHARS
        || evidence.original_url.chars().any(char::is_control)
    {
        return Err("canonicalization evidence contains an invalid original URL".into());
    }
    match (evidence.outcome, &evidence.canonical_url) {
        (CanonicalizationOutcome::Canonicalized, Some(url)) => validate_url_string(url)?,
        (CanonicalizationOutcome::InvalidUrl, None) => {}
        _ => return Err("canonicalization evidence outcome is inconsistent".into()),
    }
    for decision in &evidence.decisions {
        if decision.parameter.as_ref().is_some_and(|parameter| {
            parameter.is_empty()
                || parameter.chars().count() > MAX_TEST_EVIDENCE_URL_CHARS
                || parameter.chars().any(char::is_control)
        }) {
            return Err("canonicalization evidence contains an invalid parameter".into());
        }
    }
    Ok(())
}

fn validate_page_type_match(evidence: &PageTypeMatchEvidence) -> Result<(), String> {
    for candidate in &evidence.candidates {
        validate_page_type_candidate(candidate)?;
    }
    if let Some(winner) = &evidence.winner {
        validate_page_type_candidate(winner)?;
    }
    match evidence.decision {
        PageTypeMatchStatus::Matched => {
            let Some(winner) = &evidence.winner else {
                return Err("matched PageType evidence has no winner".into());
            };
            if evidence.candidates.len() != 1 || evidence.candidates[0] != *winner {
                return Err("matched PageType evidence has inconsistent candidates".into());
            }
        }
        PageTypeMatchStatus::Ambiguous => {
            if evidence.winner.is_some() || evidence.candidates.len() < 2 {
                return Err("ambiguous PageType evidence is incomplete".into());
            }
        }
        PageTypeMatchStatus::Unmatched => {
            if evidence.winner.is_some() || !evidence.candidates.is_empty() {
                return Err("unmatched PageType evidence has candidates".into());
            }
        }
    }
    Ok(())
}

fn validate_page_type_candidate(candidate: &PageTypeCandidateEvidence) -> Result<(), String> {
    if candidate.page_type_id.as_uuid().get_version_num() != 7
        || candidate.page_type_name.is_empty()
        || candidate.page_type_name.chars().count() > MAX_TEST_EVIDENCE_DIAGNOSTIC_MESSAGE_CHARS
        || candidate.page_type_name.chars().any(char::is_control)
        || candidate.matched_patterns.len() > MAX_TEST_EVIDENCE_INPUT_URLS
        || candidate.matched_patterns.iter().any(|pattern| {
            pattern.is_empty()
                || pattern.chars().count() > MAX_TEST_EVIDENCE_URL_CHARS
                || pattern.chars().any(char::is_control)
        })
    {
        return Err("PageType candidate evidence is invalid".into());
    }
    Ok(())
}

fn validate_selector_coverage(evidence: &SelectorCoverageEvidence) -> Result<(), String> {
    if evidence.selector.is_empty()
        || evidence.selector.chars().count() > MAX_TEST_EVIDENCE_URL_CHARS
        || evidence.selector.chars().any(char::is_control)
        || (evidence.matches_found == 0 && evidence.status == SelectorCoverageStatus::Observed)
        || (evidence.matches_found > 0 && evidence.status != SelectorCoverageStatus::Observed)
    {
        return Err("selector coverage evidence is invalid".into());
    }
    Ok(())
}

fn validate_pagination(evidence: &PaginationEvidence) -> Result<(), String> {
    if evidence.selector.as_ref().is_some_and(|selector| {
        selector.is_empty()
            || selector.chars().count() > MAX_TEST_EVIDENCE_URL_CHARS
            || selector.chars().any(char::is_control)
    }) {
        return Err("pagination selector evidence is invalid".into());
    }
    if let Some(url) = &evidence.target_url {
        validate_url_string(url)?;
    }
    Ok(())
}

fn validate_extraction(evidence: &ExtractionObservation) -> Result<(), String> {
    match evidence {
        ExtractionObservation::Available { fields } => {
            if fields.len() > MAX_TEST_EVIDENCE_DISCOVERED_URLS {
                return Err("extraction observation is outside its bounded range".into());
            }
            for field in fields {
                if field.name.is_empty()
                    || field.name.chars().count() > MAX_TEST_EVIDENCE_URL_CHARS
                    || field.name.chars().any(char::is_control)
                {
                    return Err("extraction field evidence is invalid".into());
                }
            }
        }
        ExtractionObservation::Unavailable { reason } => {
            if reason.is_empty()
                || reason.chars().count() > MAX_TEST_EVIDENCE_DIAGNOSTIC_MESSAGE_CHARS
                || reason.chars().any(char::is_control)
            {
                return Err("extraction unavailable observation is invalid".into());
            }
        }
        ExtractionObservation::Error { diagnostic } => validate_diagnostic(diagnostic)?,
    }
    Ok(())
}

fn validate_discovery(evidence: &DiscoveryTransitionEvidence) -> Result<(), String> {
    let has_transition = evidence.transition_id.is_some();
    if evidence
        .transition_id
        .is_some_and(|id| id.as_uuid().get_version_num() != 7)
        || has_transition != evidence.transition_name.is_some()
        || has_transition != evidence.source_page_type_id.is_some()
        || has_transition != evidence.target_page_type_id.is_some()
        || evidence.transition_name.as_ref().is_some_and(|name| {
            name.is_empty()
                || name.chars().count() > MAX_TEST_EVIDENCE_DIAGNOSTIC_MESSAGE_CHARS
                || name.chars().any(char::is_control)
        })
        || evidence
            .source_page_type_id
            .is_some_and(|id| id.as_uuid().get_version_num() != 7)
        || evidence
            .target_page_type_id
            .is_some_and(|id| id.as_uuid().get_version_num() != 7)
        || evidence.discovered_urls.len() > MAX_TEST_EVIDENCE_DISCOVERED_URLS
        || usize::try_from(evidence.eligible_link_count).unwrap_or(usize::MAX)
            > evidence.discovered_urls.len()
        || (!has_transition && (evidence.per_page_limit != 0 || evidence.per_page_limit_reached))
        || (has_transition && evidence.per_page_limit == 0)
    {
        return Err("discovery transition evidence is invalid".into());
    }
    if let Some(source_match) = &evidence.source_match {
        validate_page_type_match(source_match)?;
    }
    validate_selector_coverage(&evidence.selector)?;
    for discovered in &evidence.discovered_urls {
        if discovered.raw_href.is_empty()
            || discovered.raw_href.chars().count() > MAX_TEST_EVIDENCE_URL_CHARS
            || discovered.raw_href.chars().any(char::is_control)
            || discovered
                .duplicate_of_canonical_url
                .as_ref()
                .is_some_and(|url| validate_url_string(url).is_err())
            || (discovered.duplicate && discovered.duplicate_of_canonical_url.is_none())
            || (!discovered.duplicate && discovered.duplicate_of_canonical_url.is_some())
        {
            return Err("discovered URL evidence is invalid".into());
        }
        if let Some(url) = &discovered.resolved_original_url {
            validate_url_string(url)?;
        }
        if let Some(url) = &discovered.canonical_url {
            validate_url_string(url)?;
        }
        if let Some(canonicalization) = &discovered.canonicalization {
            validate_canonicalization(canonicalization)?;
        }
        if let Some(page_match) = &discovered.page_type_match {
            validate_page_type_match(page_match)?;
        }
        if discovered.transition_eligible
            && discovered
                .budget
                .as_ref()
                .is_none_or(|budget| !budget.allowed)
        {
            return Err("eligible discovered URL has no allowed budget evidence".into());
        }
        if let Some(budget) = &discovered.budget
            && (budget.allowed != discovered.transition_eligible
                || (!budget.allowed && budget.exclusion.is_none())
                || (budget.allowed && budget.exclusion.is_some()))
        {
            return Err("discovered URL budget evidence is invalid".into());
        }
    }
    Ok(())
}

fn validate_comparison(
    comparison: &TestLabComparison,
    evidence: &TestEvidence,
) -> Result<(), String> {
    if comparison.draft_version_id != evidence.crawler_version_id
        || comparison.draft_config_hash != evidence.config_hash
        || comparison.draft_canonicalization != evidence.canonicalization
        || comparison.draft_page_type_match != evidence.page_type_match
        || comparison.draft_config_hash.len() != 64
        || !comparison
            .draft_config_hash
            .chars()
            .all(|character| character.is_ascii_hexdigit())
        || comparison.draft_canonicalization.len() > MAX_TEST_EVIDENCE_INPUT_URLS
        || comparison.published_canonicalization.len() > MAX_TEST_EVIDENCE_INPUT_URLS
        || comparison.draft_page_type_match.len() > MAX_TEST_EVIDENCE_INPUT_URLS
        || comparison.published_page_type_match.len() > MAX_TEST_EVIDENCE_INPUT_URLS
        || comparison.warnings.len() > MAX_TEST_EVIDENCE_DIAGNOSTICS
    {
        return Err("Published comparison does not match evidence identity".into());
    }
    for canonicalization in comparison
        .draft_canonicalization
        .iter()
        .chain(&comparison.published_canonicalization)
    {
        validate_canonicalization(canonicalization)?;
    }
    for page_match in comparison
        .draft_page_type_match
        .iter()
        .chain(&comparison.published_page_type_match)
    {
        validate_page_type_match(page_match)?;
    }
    for diagnostic in &comparison.warnings {
        validate_diagnostic(diagnostic)?;
    }
    match comparison.status {
        PublishedComparisonStatus::Compared => {
            if comparison
                .published_version_id
                .is_none_or(|id| id.as_uuid().get_version_num() != 7)
                || comparison
                    .published_config_hash
                    .as_ref()
                    .is_none_or(|hash| {
                        hash.len() != 64
                            || !hash.chars().all(|character| character.is_ascii_hexdigit())
                    })
            {
                return Err("Published comparison identity is incomplete".into());
            }
            if comparison.canonicalization_difference
                != (comparison.draft_canonicalization != comparison.published_canonicalization)
                || comparison.page_type_match_difference
                    == page_type_match_sequences_behaviorally_equivalent(
                        &comparison.draft_page_type_match,
                        &comparison.published_page_type_match,
                    )
            {
                return Err("Published comparison differences are inconsistent".into());
            }
        }
        PublishedComparisonStatus::NoActivePublishedVersion => {
            if comparison.published_version_id.is_some()
                || comparison.published_config_hash.is_some()
                || !comparison.published_canonicalization.is_empty()
                || !comparison.published_page_type_match.is_empty()
                || comparison.canonicalization_difference
                || comparison.page_type_match_difference
                || comparison.discovery_difference.is_some()
                || comparison.extraction_difference.is_some()
            {
                return Err("no-Published comparison has Published identity".into());
            }
        }
    }
    Ok(())
}

fn validate_diagnostic(diagnostic: &TestDiagnostic) -> Result<(), String> {
    if diagnostic.code.is_empty()
        || diagnostic.code.chars().count() > MAX_TEST_EVIDENCE_DIAGNOSTIC_CODE_CHARS
        || diagnostic.code.chars().any(char::is_control)
        || diagnostic.message.is_empty()
        || diagnostic.message.chars().count() > MAX_TEST_EVIDENCE_DIAGNOSTIC_MESSAGE_CHARS
        || diagnostic.message.chars().any(char::is_control)
    {
        return Err("TestEvidence diagnostic is invalid".into());
    }
    Ok(())
}

fn validate_url_string(value: &str) -> Result<(), String> {
    if value.chars().count() > MAX_TEST_EVIDENCE_URL_CHARS
        || value.chars().any(char::is_control)
        || url::Url::parse(value).is_err()
    {
        return Err("TestEvidence contains an invalid URL".into());
    }
    Ok(())
}

fn is_sorted_unique<T: ToString>(values: &[T]) -> bool {
    let strings = values.iter().map(ToString::to_string).collect::<Vec<_>>();
    strings.windows(2).all(|window| window[0] < window[1])
        && strings.iter().collect::<BTreeSet<_>>().len() == strings.len()
}
