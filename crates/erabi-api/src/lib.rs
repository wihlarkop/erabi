//! HTTP API boundary for the Erabi modular monolith.
//!
//! This crate owns transport security, stable API presentation, and runtime
//! extension points. Domain and persistence invariants remain in their owning
//! crates rather than being recreated in HTTP handlers.

mod app;
mod error;
mod progress;
mod redaction;
mod run_safety;
mod state;

pub mod security;

pub use app::build_router;
pub use error::{ApiErrorEnvelope, Recoverability};
pub use redaction::{REDACTED, redact_header, redact_headers, redact_json, redact_url};
pub use run_safety::{
    RobotsDecisionContext, RobotsOverrideInput, new_run_robots_decision,
    reuse_frozen_robots_decision,
};
pub use security::{SecurityConfig, SecurityConfigError};
pub use state::{AppState, Crawl4AiAvailability, MutationAdmission, RuntimeMode};
