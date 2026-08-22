//! Core, dependency-light Crawler Studio domain contracts.

mod error;
mod id;
mod status;

pub use error::{ErrorCode, ProductError, SuggestedAction};
pub use id::EntityId;
pub use status::{CrawlRunStatus, CrawlRunType, SourceStatus, SourceTargetType};
