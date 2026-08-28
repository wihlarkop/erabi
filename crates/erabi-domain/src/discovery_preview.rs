use std::collections::BTreeMap;

use crate::{
    CanonicalizationEvidence, CrawlerVersionId, DiscoveryTransitionId, DomainScopeEvidence,
    PageTypeCandidateEvidence, PageTypeId, PageTypeMatchEvidence, SeedId, TestDiagnostic,
};

/// Conservative process-safety bounds for one synchronous Discovery Preview.
pub const MAX_PREVIEW_SELECTED_SEEDS: usize = 32;
pub const MAX_PREVIEW_LINKS_PER_OBSERVATION: usize = 256;
pub const MAX_PREVIEW_PROVENANCE_EDGES: usize = 4_096;
pub const MAX_PREVIEW_DIAGNOSTICS: usize = 256;
pub const MAX_PREVIEW_TRANSITION_LIMITS: usize = 128;
pub const MAX_PREVIEW_URL_CHARS: usize = 4_096;

/// Initial deterministic advisory thresholds for bounded growth diagnostics.
pub const DOMINANT_TRANSITION_MIN_EDGES: u64 = 8;
pub const DOMINANT_TRANSITION_SHARE_PERCENT: u64 = 70;
pub const QUERY_EXPLOSION_MIN_VARIANTS: u64 = 8;
pub const QUERY_EXPLOSION_QUERY_BEARING_PERCENT: u64 = 75;
pub const HIGH_UNMATCHED_MIN_DENOMINATOR: u64 = 8;
pub const HIGH_UNMATCHED_SHARE_PERCENT: u64 = 50;
pub const WIDESPREAD_AMBIGUITY_MIN_DENOMINATOR: u64 = 4;
pub const WIDESPREAD_AMBIGUITY_SHARE_PERCENT: u64 = 40;
pub const BUDGET_PRESSURE_PERCENT: u64 = 80;

/// The complete request for one ephemeral Discovery Preview.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryPreviewRequest {
    pub seed_ids: Vec<SeedId>,
    pub limits: DiscoveryPreviewLimits,
}

/// A synchronous request-time sample bound. `max_depth = 0` samples roots
/// only and is intentionally valid; all other caps that represent work must
/// be positive.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryPreviewLimits {
    pub max_pages: u64,
    pub max_depth: u32,
    pub max_duration_ms: u64,
    /// Preview-wide total eligible-edge cap applied independently per
    /// transition. The configured per-source-page cap remains authoritative.
    pub default_transition_total_limit: u64,
    #[serde(default)]
    pub transition_total_limits: Vec<TransitionPreviewTotalLimit>,
}

impl DiscoveryPreviewLimits {
    /// Validates request-local limits before any persisted state is read.
    ///
    /// # Errors
    /// Returns a stable limit validation error. The semantic `CrawlerVersion`
    /// guardrail is checked separately by the crawler service.
    pub fn validate(&self) -> Result<(), DiscoveryPreviewLimitError> {
        if self.max_pages == 0 {
            return Err(DiscoveryPreviewLimitError::MaxPagesMustBePositive);
        }
        if self.max_duration_ms == 0 {
            return Err(DiscoveryPreviewLimitError::MaxDurationMustBePositive);
        }
        if self.default_transition_total_limit == 0 {
            return Err(DiscoveryPreviewLimitError::DefaultTransitionTotalLimitMustBePositive);
        }
        if self.transition_total_limits.len() > MAX_PREVIEW_TRANSITION_LIMITS {
            return Err(DiscoveryPreviewLimitError::TooManyTransitionLimits);
        }
        let mut ids = std::collections::BTreeSet::new();
        for limit in &self.transition_total_limits {
            if limit.max_total_links == 0 {
                return Err(DiscoveryPreviewLimitError::TransitionTotalLimitMustBePositive);
            }
            if !ids.insert(limit.transition_id.to_string()) {
                return Err(DiscoveryPreviewLimitError::DuplicateTransitionLimit);
            }
        }
        Ok(())
    }
}

/// One optional Preview-wide total override for a version-local transition.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionPreviewTotalLimit {
    pub transition_id: DiscoveryTransitionId,
    pub max_total_links: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DiscoveryPreviewLimitError {
    #[error("Preview max_pages must be positive")]
    MaxPagesMustBePositive,
    #[error("Preview max_duration_ms must be positive")]
    MaxDurationMustBePositive,
    #[error("Preview default transition total limit must be positive")]
    DefaultTransitionTotalLimitMustBePositive,
    #[error("Preview transition total limit must be positive")]
    TransitionTotalLimitMustBePositive,
    #[error("Preview contains duplicate transition total limits")]
    DuplicateTransitionLimit,
    #[error("Preview contains too many transition total limits")]
    TooManyTransitionLimits,
}

/// Limits after tightening the request against the immutable semantic
/// `CrawlerVersion` baseline. Downloaded bytes are always the semantic budget;
/// Preview has no widening byte override. Transition totals are resolved for
/// every version-local transition so the response never implies that one
/// Preview default is the effective cap for a transition with a lower semantic
/// `total_budget`.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct EffectiveDiscoveryPreviewLimits {
    pub max_pages: u64,
    pub max_depth: u32,
    pub max_duration_ms: u64,
    pub max_downloaded_bytes: u64,
    pub transition_total_limits: Vec<EffectiveTransitionPreviewTotalLimit>,
}

/// One authoritative Preview total cap after applying both the Preview and
/// configured transition budgets.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct EffectiveTransitionPreviewTotalLimit {
    pub transition_id: DiscoveryTransitionId,
    pub effective_total_limit: u64,
}

/// The Preview result is categorically advisory and never a complete
/// production snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DiscoveryPreviewResultSemantics {
    PreviewOnly,
}

/// Explicit preserve-only state for one root, page, or discovery edge.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PreviewUrlState {
    Sampled,
    InScopeMatched,
    AmbiguousPageType,
    Unmatched,
    External,
    Blocked,
    CanonicalDuplicate,
    RobotsExcluded,
    BudgetExcluded,
    ProviderError,
    InvalidUrl,
}

/// Stable categories used by summary budget-hit accounting.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PreviewBudgetKind {
    MaxPages,
    MaxDepth,
    MaxDuration,
    MaxDownloadedBytes,
    PageTypePageBudget,
    TransitionPerSourcePage,
    TransitionTotal,
    ProvenanceRetention,
    DiagnosticRetention,
}

/// A bounded explanation for one preserve-only budget decision.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct PreviewBudgetHit {
    pub kind: PreviewBudgetKind,
    pub transition_id: Option<DiscoveryTransitionId>,
    pub page_type_id: Option<PageTypeId>,
    pub observed: u64,
    pub limit: u64,
}

/// A typed diagnostic for one growth or provider observation condition.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct PreviewDiagnostic {
    pub code: String,
    pub message: String,
    pub observed: Option<u64>,
    pub threshold: Option<u64>,
}

/// A selected root and its independently retained provenance outcome.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct DiscoveryPreviewSeed {
    pub seed_id: SeedId,
    pub requested_url: String,
    pub canonical_url: String,
    pub entry_page_type_hint: Option<PageTypeId>,
    pub state: PreviewUrlState,
    pub duplicate_of_canonical_url: Option<String>,
    pub scope: Option<DomainScopeEvidence>,
    pub page_type_match: Option<PageTypeMatchEvidence>,
    pub budget_hits: Vec<PreviewBudgetHit>,
}

/// A successful, robots-excluded, or failed provider page admission. The
/// service retains the provider's one authoritative `PageObservation.final_url`
/// value when an observation succeeded.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct DiscoveryPreviewPage {
    pub requested_url: String,
    pub final_url: Option<String>,
    pub canonical_url: Option<String>,
    pub depth: u32,
    pub state: PreviewUrlState,
    pub seed_ids: Vec<SeedId>,
    pub scope: Option<DomainScopeEvidence>,
    pub page_type_match: Option<PageTypeMatchEvidence>,
    pub downloaded_bytes: Option<u64>,
    pub robots_reason: Option<String>,
    pub diagnostic: Option<TestDiagnostic>,
    pub budget_hits: Vec<PreviewBudgetHit>,
}

/// Result of evaluating one transition on a first-unique discovery event.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct PreviewTransitionEvaluation {
    pub transition_id: DiscoveryTransitionId,
    pub transition_name: String,
    pub source_page_type_id: PageTypeId,
    pub target_page_type_id: PageTypeId,
    pub priority: i32,
    pub selector_eligible: bool,
    pub target_page_type_eligible: bool,
    pub constraints_eligible: bool,
    pub eligible: bool,
    pub budget_hits: Vec<PreviewBudgetHit>,
    pub diagnostic: Option<PreviewDiagnostic>,
}

/// Full explanation of one discovered href. Duplicate discoveries retain the
/// edge but intentionally have no transition evaluations or new budget use.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct DiscoveryPath {
    pub seed_id: SeedId,
    /// All selected roots represented by the source queue entry. `seed_id`
    /// remains the stable primary provenance entry for compatibility.
    pub seed_ids: Vec<SeedId>,
    pub source_requested_url: String,
    pub source_final_url: Option<String>,
    pub source_canonical_url: String,
    pub source_page_type_match: PageTypeMatchEvidence,
    pub selector: Option<String>,
    pub raw_href: String,
    pub resolved_original_url: Option<String>,
    pub canonical_url: Option<String>,
    pub canonicalization: Option<CanonicalizationEvidence>,
    pub scope: Option<DomainScopeEvidence>,
    pub state: PreviewUrlState,
    pub duplicate_of_canonical_url: Option<String>,
    pub target_page_type_match: Option<PageTypeMatchEvidence>,
    pub source_depth: u32,
    pub prospective_depth: Option<u32>,
    pub transition_evaluations: Vec<PreviewTransitionEvaluation>,
    pub budget_hits: Vec<PreviewBudgetHit>,
}

/// Per-transition eligible-edge and per-source-page counters.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct PreviewTransitionCount {
    pub transition_id: DiscoveryTransitionId,
    pub transition_name: String,
    pub eligible_edges: u64,
    pub source_pages_with_eligible_edges: u64,
}

/// Separate `PageType` distributions avoid overloading one ambiguous metric.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct PreviewPageTypeDistribution {
    pub page_type_id: PageTypeId,
    pub page_type_name: String,
    pub discovered_unique_urls: u64,
    pub sampled_pages: u64,
}

/// Stable summary definitions for Studio/API consumers.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct DiscoveryPreviewSummary {
    /// Successful accepted provider observations.
    pub pages_sampled: u64,
    /// Raw href observations emitted by sampled pages.
    pub urls_discovered: u64,
    /// Unique canonical identities retained from selected roots, discoveries,
    /// and observed redirect final URLs. A redirect alias and a distinct final
    /// canonical URL are both identities, but only one provider observation is
    /// admitted for the final identity.
    pub canonical_unique_urls: u64,
    /// Canonical scheduling attempts prevented by the global dedupe registry.
    pub duplicates_prevented: u64,
    pub page_type_distribution: Vec<PreviewPageTypeDistribution>,
    pub ambiguous_urls: u64,
    pub unmatched_urls: u64,
    pub external_urls: u64,
    pub blocked_urls: u64,
    /// Provider robots outcomes; never included in `pages_sampled`.
    pub robots_excluded: u64,
    /// Ordinary page-specific provider failures; never included in
    /// `pages_sampled` and never release an admitted page slot.
    pub provider_errors: u64,
    pub transition_counts: Vec<PreviewTransitionCount>,
    pub budget_hit_counts: BTreeMap<PreviewBudgetKind, u64>,
    pub frontier_remaining: u64,
    pub newly_enqueued_urls: u64,
}

/// Integer/count growth evidence. It is advisory and never a site-size claim.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct PreviewGrowthIndicators {
    pub peak_new_canonical_urls_from_one_page: u64,
    pub total_newly_enqueued_urls: u64,
    pub frontier_remaining: u64,
    pub dominant_transition_id: Option<DiscoveryTransitionId>,
    pub dominant_transition_eligible_edges: u64,
    pub total_eligible_transition_edges: u64,
    pub dominant_transition_share_percent: Option<u64>,
    pub query_variant_groups: Vec<PreviewQueryVariantGroup>,
    pub unmatched_denominator: u64,
    pub ambiguity_denominator: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct PreviewQueryVariantGroup {
    pub host: String,
    pub path: String,
    /// Every unique in-scope canonical identity sharing this normalized
    /// host/path, including identities without a query component.
    pub total_identities: u64,
    /// Unique identities in this group whose canonical URL has a query.
    pub query_bearing_identities: u64,
    /// Distinct canonical query strings observed among the query-bearing
    /// identities. This is intentionally separate from the denominator.
    pub canonical_query_variants: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PreviewGrowthWarningCode {
    CyclicTransitionDominance,
    QueryParameterExplosion,
    HighUnmatchedRate,
    WidespreadPageTypeAmbiguity,
    BudgetPressure,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct PreviewGrowthWarning {
    pub code: PreviewGrowthWarningCode,
    pub message: String,
    pub observed: u64,
    pub threshold: u64,
}

/// Complete synchronous Preview payload. It contains no run identity and no
/// completeness or missing-record semantics.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct DiscoveryPreviewResult {
    pub result_semantics: DiscoveryPreviewResultSemantics,
    pub crawler_version_id: CrawlerVersionId,
    pub config_hash: String,
    pub selected_seed_ids: Vec<SeedId>,
    pub effective_limits: EffectiveDiscoveryPreviewLimits,
    pub seeds: Vec<DiscoveryPreviewSeed>,
    pub pages: Vec<DiscoveryPreviewPage>,
    pub discovery_paths: Vec<DiscoveryPath>,
    pub summary: DiscoveryPreviewSummary,
    pub growth_indicators: PreviewGrowthIndicators,
    pub growth_warnings: Vec<PreviewGrowthWarning>,
    pub warnings: Vec<PreviewDiagnostic>,
}

/// Converts the existing Test Lab diagnostic shape when an advisory warning
/// does not need numeric evidence.
#[must_use]
pub fn preview_diagnostic(
    code: impl Into<String>,
    message: impl Into<String>,
) -> PreviewDiagnostic {
    PreviewDiagnostic {
        code: code.into(),
        message: message.into(),
        observed: None,
        threshold: None,
    }
}

/// Keeps the candidate type available to API layers that need to inspect all
/// ambiguity candidates without inventing a Preview-specific matcher.
pub type PreviewPageTypeCandidate = PageTypeCandidateEvidence;
