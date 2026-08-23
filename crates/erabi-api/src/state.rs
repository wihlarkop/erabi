//! Minimal runtime state shared by API routes.

use std::sync::{Arc, RwLock};

/// Runtime service mode used to protect mutable surfaces.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "mode")]
pub enum RuntimeMode {
    /// Startup completed without an integrity risk.
    Normal,
    /// Mutations are unsafe until the stated recovery condition is addressed.
    Recovery { code: String, message: String },
}

/// Crawl engine availability that does not itself imply data corruption.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Crawl4AiAvailability {
    /// The adapter health check succeeded.
    Available,
    /// The adapter is unavailable; UI, data, and diagnostics remain usable.
    Degraded { message: String },
}

#[derive(Clone, Debug)]
struct RuntimeSnapshot {
    ready: bool,
    mode: RuntimeMode,
    crawl4ai: Crawl4AiAvailability,
}

/// Shared state used by the hardened shell and extended by later runtime tasks.
#[derive(Clone, Debug)]
pub struct AppState {
    runtime: Arc<RwLock<RuntimeSnapshot>>,
}

impl AppState {
    /// Creates state for a process that has completed startup.
    #[must_use]
    pub fn ready() -> Self {
        Self::with_readiness(true)
    }

    /// Creates state for tests and startup orchestration before readiness.
    #[must_use]
    pub fn with_readiness(ready: bool) -> Self {
        Self {
            runtime: Arc::new(RwLock::new(RuntimeSnapshot {
                ready,
                mode: RuntimeMode::Normal,
                crawl4ai: Crawl4AiAvailability::Available,
            })),
        }
    }

    /// Reads the current readiness state.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.runtime.read().is_ok_and(|runtime| runtime.ready)
    }

    /// Updates readiness after runtime orchestration has reached its route boundary.
    pub fn set_ready(&self, ready: bool) {
        if let Ok(mut runtime) = self.runtime.write() {
            runtime.ready = ready;
        }
    }

    /// Enters Recovery Mode and prevents normal mutations/new job submission.
    pub fn enter_recovery(&self, code: impl Into<String>, message: impl Into<String>) {
        if let Ok(mut runtime) = self.runtime.write() {
            runtime.mode = RuntimeMode::Recovery {
                code: code.into(),
                message: message.into(),
            };
            runtime.ready = false;
        }
    }

    /// Marks Crawl4AI as unavailable without entering Recovery Mode.
    pub fn set_crawl4ai_degraded(&self, message: impl Into<String>) {
        if let Ok(mut runtime) = self.runtime.write() {
            runtime.crawl4ai = Crawl4AiAvailability::Degraded {
                message: message.into(),
            };
        }
    }

    /// Returns whether normal state-changing routes may proceed.
    #[must_use]
    pub fn mutations_allowed(&self) -> bool {
        self.runtime
            .read()
            .is_ok_and(|runtime| matches!(runtime.mode, RuntimeMode::Normal))
    }

    /// Provides only typed, safe runtime diagnostics.
    #[must_use]
    pub fn runtime_mode(&self) -> RuntimeMode {
        self.runtime.read().map_or_else(
            |_| RuntimeMode::Recovery {
                code: "RUNTIME_STATE_UNAVAILABLE".to_owned(),
                message: "Runtime state could not be inspected safely.".to_owned(),
            },
            |runtime| runtime.mode.clone(),
        )
    }

    /// Provides the Crawl4AI availability used by readiness and diagnostics.
    #[must_use]
    pub fn crawl4ai_availability(&self) -> Crawl4AiAvailability {
        self.runtime.read().map_or_else(
            |_| Crawl4AiAvailability::Degraded {
                message: "Crawl4AI availability could not be inspected safely.".to_owned(),
            },
            |runtime| runtime.crawl4ai.clone(),
        )
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::ready()
    }
}
