//! HTTP API boundary for the Erabi modular monolith.
//!
//! This crate owns transport security, stable API presentation, and runtime
//! extension points. Domain and persistence invariants remain in their owning
//! crates rather than being recreated in HTTP handlers.

mod app;
mod error;
mod state;

pub mod security;

pub use app::build_router;
pub use error::{ApiErrorEnvelope, Recoverability};
pub use security::{SecurityConfig, SecurityConfigError};
pub use state::AppState;
