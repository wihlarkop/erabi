//! Crawler application-service wiring.
//!
//! Test Lab orchestration lives in focused modules so future crawler services
//! do not accumulate in this crate-root surface.

mod adapter;
mod discovery_preview;
mod mock_adapter;
mod observation;
mod test_lab;

pub use adapter::*;
pub use discovery_preview::*;
pub use mock_adapter::*;
pub use test_lab::*;
