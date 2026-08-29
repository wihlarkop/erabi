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
mod source_intake;
mod test_lab;

pub use adapter::*;
pub use content_probe::*;
pub use discovery_preview::*;
pub use mock_adapter::*;
pub use network_policy::*;
pub use source_intake::*;
pub use test_lab::*;
