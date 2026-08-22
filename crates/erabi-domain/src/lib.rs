//! Core, dependency-light Crawler Studio domain contracts.

mod crawler;
mod crawler_version;
mod error;
mod id;
mod matching;
mod page_type;
mod run_profile;
mod seed;
mod status;
mod test_evidence;
mod url_matcher;

pub use crawler::Crawler;
pub use crawler_version::{CrawlerVersion, CrawlerVersionState};
pub use error::{ErrorCode, ProductError, SuggestedAction};
pub use id::EntityId;
pub use matching::{PageTypeCandidate, PageTypeMatchDecision, resolve_page_type};
pub use page_type::PageType;
pub use run_profile::{OperationalOverrides, RunProfile};
pub use seed::Seed;
pub use status::{CrawlRunStatus, CrawlRunType, SourceStatus, SourceTargetType};
pub use test_evidence::TestEvidence;
pub use url_matcher::{SpecificityKey, UrlMatcher, UrlMatcherKind};
