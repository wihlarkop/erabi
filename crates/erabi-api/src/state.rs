//! Minimal runtime state shared by API routes.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Shared state used by the hardened shell and extended by later runtime tasks.
#[derive(Clone, Debug)]
pub struct AppState {
    ready: Arc<AtomicBool>,
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
            ready: Arc::new(AtomicBool::new(ready)),
        }
    }

    /// Reads the current readiness state.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Updates readiness after runtime orchestration has reached its route boundary.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::ready()
    }
}
