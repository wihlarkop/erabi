//! Core, dependency-light Crawler Studio domain contracts.

mod crawler;
mod crawler_version;
mod error;
mod id;
mod run_profile;
mod seed;
mod status;
mod test_evidence;

pub use crawler::Crawler;
pub use crawler_version::{CrawlerVersion, CrawlerVersionState};
pub use error::{ErrorCode, ProductError, SuggestedAction};
pub use id::EntityId;
pub use run_profile::{OperationalOverrides, RunProfile};
pub use seed::Seed;
pub use status::{CrawlRunStatus, CrawlRunType, SourceStatus, SourceTargetType};
pub use test_evidence::TestEvidence;
