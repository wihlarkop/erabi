//! Crawler application-service wiring.
//!
//! Test Lab orchestration lives in focused modules so future crawler services
//! do not accumulate in this crate-root surface.

mod discovery_preview;
mod observation;
mod test_lab;

pub use discovery_preview::*;
pub use test_lab::*;
