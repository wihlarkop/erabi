use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Monotonic clock seam used only at safe Preview orchestration boundaries.
pub trait PreviewClock: Send + Sync {
    fn now_millis(&self) -> u64;
}

#[derive(Debug)]
pub struct MonotonicPreviewClock {
    started_at: Instant,
}

impl MonotonicPreviewClock {
    #[must_use]
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
        }
    }
}

impl Default for MonotonicPreviewClock {
    fn default() -> Self {
        Self::new()
    }
}

impl PreviewClock for MonotonicPreviewClock {
    fn now_millis(&self) -> u64 {
        self.started_at
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

/// Deterministic clock for service tests; it never sleeps or races the wall
/// clock.
#[derive(Debug, Default)]
pub struct ManualPreviewClock {
    now_millis: AtomicU64,
}

impl ManualPreviewClock {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            now_millis: AtomicU64::new(0),
        }
    }

    pub fn advance_millis(&self, millis: u64) {
        self.now_millis.fetch_add(millis, Ordering::SeqCst);
    }

    pub fn set_millis(&self, millis: u64) {
        self.now_millis.store(millis, Ordering::SeqCst);
    }
}

impl PreviewClock for ManualPreviewClock {
    fn now_millis(&self) -> u64 {
        self.now_millis.load(Ordering::SeqCst)
    }
}
