//! Crawler application-service wiring.
//!
//! Test Lab orchestration lives in focused modules so future crawler services
//! do not accumulate in this crate-root surface.

mod test_lab;

pub use test_lab::*;
