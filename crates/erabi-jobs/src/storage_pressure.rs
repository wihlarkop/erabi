//! Typed free-storage pressure policy and cooperative active-work signalling.

use std::{
    fmt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use erabi_db::repositories::{JobId, JobStorageClass};
use fs4::available_space;
use tokio::sync::Notify;

/// Default free-space level at which Erabi starts warning operators.
pub const DEFAULT_WARNING_FREE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
/// Default free-space level at which new artifact-heavy work is blocked.
pub const DEFAULT_CRITICAL_FREE_BYTES: u64 = 1024 * 1024 * 1024;

/// Observable storage-pressure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StoragePressureLevel {
    Healthy,
    Warning,
    Critical,
    /// The filesystem could not be inspected safely. This is never healthy.
    Unavailable,
}

/// The policy thresholds used to classify free bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoragePressurePolicy {
    warning_free_bytes: u64,
    critical_free_bytes: u64,
}

impl Default for StoragePressurePolicy {
    fn default() -> Self {
        Self {
            warning_free_bytes: DEFAULT_WARNING_FREE_BYTES,
            critical_free_bytes: DEFAULT_CRITICAL_FREE_BYTES,
        }
    }
}

impl StoragePressurePolicy {
    /// Creates a policy with deterministic inclusive boundaries.
    ///
    /// A free-space value equal to the critical threshold is critical. A
    /// value equal to the warning threshold is warning unless it is also
    /// critical. Thresholds are absolute bytes and no arithmetic is done on
    /// observed values.
    ///
    /// # Errors
    /// Returns [`StoragePressurePolicyError::InvalidThresholdOrdering`] when
    /// the critical threshold is not lower than the warning threshold.
    pub fn new(
        warning_free_bytes: u64,
        critical_free_bytes: u64,
    ) -> Result<Self, StoragePressurePolicyError> {
        if critical_free_bytes >= warning_free_bytes {
            return Err(StoragePressurePolicyError::InvalidThresholdOrdering);
        }
        Ok(Self {
            warning_free_bytes,
            critical_free_bytes,
        })
    }

    #[must_use]
    pub const fn warning_free_bytes(self) -> u64 {
        self.warning_free_bytes
    }

    #[must_use]
    pub const fn critical_free_bytes(self) -> u64 {
        self.critical_free_bytes
    }

    /// Classifies one deterministic free-byte observation.
    #[must_use]
    pub const fn classify(self, free_bytes: u64) -> StoragePressureState {
        let level = if free_bytes <= self.critical_free_bytes {
            StoragePressureLevel::Critical
        } else if free_bytes <= self.warning_free_bytes {
            StoragePressureLevel::Warning
        } else {
            StoragePressureLevel::Healthy
        };
        StoragePressureState {
            level,
            free_bytes: Some(free_bytes),
            warning_threshold: self.warning_free_bytes,
            critical_threshold: self.critical_free_bytes,
        }
    }

    #[must_use]
    pub const fn unavailable(self) -> StoragePressureState {
        StoragePressureState {
            level: StoragePressureLevel::Unavailable,
            free_bytes: None,
            warning_threshold: self.warning_free_bytes,
            critical_threshold: self.critical_free_bytes,
        }
    }
}

/// Typed policy-construction failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StoragePressurePolicyError {
    #[error("critical free-space threshold must be lower than warning threshold")]
    InvalidThresholdOrdering,
}

/// Safe state exposed to runtime/API diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub struct StoragePressureState {
    pub level: StoragePressureLevel,
    pub free_bytes: Option<u64>,
    pub warning_threshold: u64,
    pub critical_threshold: u64,
}

impl StoragePressureState {
    #[must_use]
    pub const fn unavailable(policy: StoragePressurePolicy) -> Self {
        policy.unavailable()
    }

    /// Critical and unavailable states conservatively refuse new artifact
    /// growth; warning and healthy states do not.
    #[must_use]
    pub const fn allows_artifact_heavy(self) -> bool {
        !matches!(
            self.level,
            StoragePressureLevel::Critical | StoragePressureLevel::Unavailable
        )
    }
}

/// Sanitized failures from a free-space adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StorageProbeError {
    #[error("storage free-space probe is unavailable")]
    Unavailable,
}

/// Injectable seam for obtaining free bytes on the filesystem owning Erabi data.
pub trait StorageProbe: Send + Sync {
    /// Returns available bytes for non-privileged users on the path's volume.
    ///
    /// # Errors
    /// Returns a sanitized typed error when the filesystem cannot be queried.
    fn free_bytes(&self, path: &Path) -> Result<u64, StorageProbeError>;
}

/// Cross-platform filesystem adapter for supported local development hosts.
#[derive(Clone, Copy, Debug, Default)]
pub struct FileSystemStorageProbe;

impl StorageProbe for FileSystemStorageProbe {
    fn free_bytes(&self, path: &Path) -> Result<u64, StorageProbeError> {
        available_space(path).map_err(|_| StorageProbeError::Unavailable)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct UnavailableStorageProbe;

impl StorageProbe for UnavailableStorageProbe {
    fn free_bytes(&self, _path: &Path) -> Result<u64, StorageProbeError> {
        Err(StorageProbeError::Unavailable)
    }
}

/// Shared pressure state and active artifact-heavy work signals.
#[derive(Clone, Debug)]
pub struct StoragePressureController {
    policy: StoragePressurePolicy,
    state: Arc<RwLock<StoragePressureState>>,
    active_heavy: Arc<Mutex<std::collections::HashMap<JobId, StoragePressureToken>>>,
}

impl StoragePressureController {
    #[must_use]
    pub fn new(policy: StoragePressurePolicy) -> Self {
        Self {
            policy,
            state: Arc::new(RwLock::new(policy.unavailable())),
            active_heavy: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    #[must_use]
    pub const fn policy(&self) -> StoragePressurePolicy {
        self.policy
    }

    /// Returns safe current state; a poisoned state lock is unavailable rather
    /// than incorrectly healthy.
    #[must_use]
    pub fn state(&self) -> StoragePressureState {
        self.state
            .read()
            .map_or_else(|_| self.policy.unavailable(), |state| *state)
    }

    /// Publishes a new observation and signals active heavy work on critical
    /// observations. Signalling is cooperative and never aborts a task.
    pub fn update(&self, next: StoragePressureState) {
        if let Ok(mut state) = self.state.write() {
            *state = next;
        } else {
            return;
        }
        if next.level != StoragePressureLevel::Critical {
            return;
        }
        let active = self
            .active_heavy
            .lock()
            .map_or_else(|_| Vec::new(), |active| active.values().cloned().collect());
        for token in active {
            token.signal();
        }
    }

    /// Registers active work with a typed storage-growth capability.
    #[must_use]
    pub fn register(&self, job_id: &JobId, class: JobStorageClass) -> StoragePressureToken {
        let token = StoragePressureToken::new(false);
        if class == JobStorageClass::ArtifactHeavy {
            if let Ok(mut active) = self.active_heavy.lock() {
                active.insert(job_id.clone(), token.clone());
            }
            // Recheck after registration so a transition concurrent with the
            // initial admission cannot leave active heavy work unsignalled.
            if self.state().level == StoragePressureLevel::Critical {
                token.signal();
            }
        }
        token
    }

    pub fn release(&self, job_id: &JobId) {
        if let Ok(mut active) = self.active_heavy.lock() {
            active.remove(job_id);
        }
    }
}

/// Cooperative signal that is distinct from user cancellation semantics.
#[derive(Clone, Debug)]
pub struct StoragePressureToken {
    state: Arc<StoragePressureTokenState>,
}

#[derive(Debug)]
struct StoragePressureTokenState {
    signalled: AtomicBool,
    notify: Notify,
}

impl StoragePressureToken {
    fn new(signalled: bool) -> Self {
        let token = Self {
            state: Arc::new(StoragePressureTokenState {
                signalled: AtomicBool::new(signalled),
                notify: Notify::new(),
            }),
        };
        if signalled {
            token.state.notify.notify_waiters();
        }
        token
    }

    fn signal(&self) {
        if !self.state.signalled.swap(true, Ordering::Release) {
            self.state.notify.notify_waiters();
        }
    }

    #[must_use]
    pub fn is_signalled(&self) -> bool {
        self.state.signalled.load(Ordering::Acquire)
    }

    pub async fn signalled(&self) {
        loop {
            let notified = self.state.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_signalled() {
                return;
            }
            notified.await;
        }
    }
}

/// Pressure monitor bound to the authoritative canonical data directory.
#[derive(Clone)]
pub struct StoragePressureMonitor {
    probe: Arc<dyn StorageProbe>,
    data_path: PathBuf,
    policy: StoragePressurePolicy,
    controller: StoragePressureController,
}

impl fmt::Debug for StoragePressureMonitor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoragePressureMonitor")
            .field("data_path", &self.data_path)
            .field("policy", &self.policy)
            .field("state", &self.controller.state())
            .finish_non_exhaustive()
    }
}

impl StoragePressureMonitor {
    /// Binds a probe to the canonical Erabi data directory.
    #[must_use]
    pub fn new(
        probe: impl StorageProbe + 'static,
        data_path: impl Into<PathBuf>,
        policy: StoragePressurePolicy,
    ) -> Self {
        Self {
            probe: Arc::new(probe),
            data_path: data_path.into(),
            policy,
            controller: StoragePressureController::new(policy),
        }
    }

    #[must_use]
    pub fn filesystem(data_path: impl Into<PathBuf>, policy: StoragePressurePolicy) -> Self {
        Self::new(FileSystemStorageProbe, data_path, policy)
    }

    #[must_use]
    pub fn unavailable(policy: StoragePressurePolicy) -> Self {
        Self::new(UnavailableStorageProbe, PathBuf::new(), policy)
    }

    #[must_use]
    pub const fn controller(&self) -> &StoragePressureController {
        &self.controller
    }

    /// Reads one observation and publishes it to runtime/worker consumers.
    #[must_use]
    pub fn refresh(&self) -> StoragePressureState {
        let state = self.probe.free_bytes(&self.data_path).map_or_else(
            |_| self.policy.unavailable(),
            |free_bytes| self.policy.classify(free_bytes),
        );
        self.controller.update(state);
        state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_thresholds_are_rejected_without_arithmetic() {
        assert_eq!(
            StoragePressurePolicy::new(10, 10),
            Err(StoragePressurePolicyError::InvalidThresholdOrdering)
        );
        assert_eq!(
            StoragePressurePolicy::new(u64::MAX, u64::MAX - 1)
                .map(|policy| policy.classify(u64::MAX)),
            Ok(StoragePressureState {
                level: StoragePressureLevel::Warning,
                free_bytes: Some(u64::MAX),
                warning_threshold: u64::MAX,
                critical_threshold: u64::MAX - 1,
            })
        );
    }
}
