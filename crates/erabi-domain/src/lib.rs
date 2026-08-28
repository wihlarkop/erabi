//! Core, dependency-light Crawler Studio domain contracts.

mod budget;
mod canonicalization;
mod collection;
mod complete_snapshot;
mod crawl_snapshot;
mod crawler;
mod crawler_version;
mod discovery_preview;
mod domain_scope;
mod error;
mod guardrails;
mod id;
mod matching;
mod naming;
mod page_type;
mod run_profile;
mod seed;
mod settings;
mod source;
mod status;
mod test_evidence;
mod transition;
mod url_matcher;
mod version_validation;

pub use budget::{
    DiscoveryBudgetCandidate, DiscoveryBudgetDecision, DiscoveryBudgetError,
    DiscoveryBudgetEvaluator, DiscoveryBudgetExclusion,
};
pub use canonicalization::{
    CANONICALIZATION_POLICY_VERSION, CanonicalizationDecision, CanonicalizationPolicy,
    CanonicalizationResult,
};
pub use collection::Collection;
pub use complete_snapshot::{
    CompleteSnapshotReason, CompleteSnapshotStructuralDecision, CompleteSnapshotStructuralInput,
    CompleteSnapshotStructuralInputError, ExtractionHealth,
};
pub use crawl_snapshot::{
    CrawlRunSnapshot, CrawlRunSnapshotDraft, MAX_ROBOTS_OVERRIDE_REASON_CHARS, RobotsAudit,
    RobotsDecision, RunConfiguration, SnapshotError, SnapshotOperationalSettings, canonical_sha256,
};
pub use crawler::Crawler;
pub use crawler_version::{CrawlerVersion, CrawlerVersionState};
pub use discovery_preview::*;
pub use domain_scope::{
    DOMAIN_SCOPE_POLICY_VERSION, DomainScopeClassification, DomainScopeHostRule, DomainScopeKind,
    DomainScopePolicy, DomainScopeRationale,
};
pub use error::{ErrorCode, ProductError, SuggestedAction};
pub use guardrails::{
    CrawlerVersionGuardrails, DeferredPageTypeHealth, GUARDRAIL_POLICY_VERSION,
    PageTypeDiscoveryGuardrails, ResolvedOperationalLimits,
};
pub use id::{
    ArtifactId, CanonicalizationPolicyId, CollectionId, CrawlRunId, CrawlerId, CrawlerVersionId,
    DiscoveryTransitionId, DomainScopeId, PageTypeId, RunProfileId, SeedId, SourceId,
    TestEvidenceId,
};
pub use matching::{PageTypeCandidate, PageTypeMatchDecision, resolve_page_type};
pub use naming::{derive_dataset_name, derive_source_name};
pub use page_type::PageType;
pub use run_profile::{OperationalOverrides, RunProfile};
pub use seed::Seed;
pub use settings::{LayerValue, ResolvedValue, SettingLayers, SettingSource};
pub use source::Source;
pub use status::{CrawlRunStatus, CrawlRunType, SourceStatus, SourceTargetType};
pub use test_evidence::{
    CanonicalizationDecisionCode, CanonicalizationDecisionEvidence, CanonicalizationEvidence,
    CanonicalizationOutcome, DiscoveredUrlEvidence, DiscoveryTransitionEvidence,
    DomainScopeEvidence, DomainScopeRationaleEvidence, DomainScopeStatus, ExtractionFieldEvidence,
    ExtractionObservation, MAX_TEST_EVIDENCE_ARTIFACTS, MAX_TEST_EVIDENCE_DIAGNOSTICS,
    MAX_TEST_EVIDENCE_DISCOVERED_URLS, MAX_TEST_EVIDENCE_INPUT_URLS, MAX_TEST_EVIDENCE_URL_CHARS,
    MatcherKindEvidence, MatcherSpecificityEvidence, PageTypeCandidateEvidence,
    PageTypeMatchEvidence, PageTypeMatchStatus, PaginationEvidence, PaginationKind,
    PublishedComparisonStatus, SelectorCoverageEvidence, SelectorCoverageStatus,
    TEST_EVIDENCE_SCHEMA_VERSION, TestDiagnostic, TestEvidence, TestKind, TestLabComparison,
    TransitionBudgetEvidence, TransitionBudgetExclusionEvidence,
    page_type_match_sequences_behaviorally_equivalent,
};
pub use transition::{DiscoveryTransition, TransitionBudget, TransitionGraph};
pub use url_matcher::{SpecificityKey, UrlMatcher, UrlMatcherDefinition, UrlMatcherKind};
pub use version_validation::{
    CoreVersionValidationContributor, InvalidValidationIdentifier, MAX_VALIDATION_DETAIL_CHARS,
    MAX_VALIDATION_DETAILS, MAX_VALIDATION_IDENTIFIER_CHARS, MAX_VALIDATION_ISSUES,
    MAX_VALIDATION_ISSUES_PER_CONTRIBUTOR, MAX_VALIDATION_MESSAGE_CHARS, ValidationContributorKey,
    ValidationIssueCode, ValidationSubjectKind, VersionValidationContext,
    VersionValidationContribution, VersionValidationContributor, VersionValidationContributorError,
    VersionValidationError, VersionValidationIssue, VersionValidationRegistry,
    VersionValidationReport, VersionValidationSeverity, VersionValidationSubject,
};
