//! Crawler application-service wiring.
//!
//! Test Lab orchestration lives in focused modules so future crawler services
//! do not accumulate in this crate-root surface.

mod adapter;
mod content_probe;
mod discovery_preview;
mod mock_adapter;
mod network_policy;
mod observation;
mod pacing;
mod quick_scrape;
mod robots;
mod source_intake;
mod test_lab;

#[cfg(test)]
#[path = "../tests/robots_pacing.rs"]
mod robots_pacing_tests;

pub use adapter::*;
pub use content_probe::*;
pub use discovery_preview::*;
pub use mock_adapter::*;
pub use network_policy::*;
pub use pacing::*;
pub use quick_scrape::*;
pub use robots::*;
pub use source_intake::*;
pub use test_lab::*;
