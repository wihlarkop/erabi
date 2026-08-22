//! Core, dependency-light Crawler Studio domain contracts.

mod collection;
mod crawler;
mod crawler_version;
mod error;
mod id;
mod matching;
mod naming;
mod page_type;
mod run_profile;
mod seed;
mod source;
mod status;
mod test_evidence;
mod transition;
mod url_matcher;

pub use collection::Collection;
pub use crawler::Crawler;
pub use crawler_version::{CrawlerVersion, CrawlerVersionState};
pub use error::{ErrorCode, ProductError, SuggestedAction};
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
pub use source::Source;
pub use status::{CrawlRunStatus, CrawlRunType, SourceStatus, SourceTargetType};
pub use test_evidence::TestEvidence;
pub use transition::{DiscoveryTransition, TransitionBudget};
pub use url_matcher::{SpecificityKey, UrlMatcher, UrlMatcherKind};
