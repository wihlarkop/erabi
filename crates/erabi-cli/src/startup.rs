//! Ordered startup orchestration with bounded Plan 04 extension hooks.

use erabi_db::{LightweightIntegrityError, repositories::JobRepositoryError};

/// Canonical ordered startup stages; tests can assert every boundary directly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupStage {
    ResolveDataDirectory,
    AcquireProcessLock,
    ValidateBootstrap,
    OpenDatabase,
    ApplyMigrations,
    CheckIntegrity,
    VerifyArtifactDirectories,
    RecoverStaleJobsHook,
    RebuildConcurrencyHook,
    CheckCrawl4Ai,
    StartRoutesAndWorkers,
    ReportReadiness,
}

/// `Crawl4AI` startup health is distinct from critical integrity state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Crawl4AiStartupHealth {
    /// Health check succeeded.
    Available,
    /// The process remains usable but crawling is unavailable.
    Degraded { message: String },
}

/// Typed recovery state that keeps mutation surfaces disabled.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryState {
    /// Stable condition code.
    pub code: String,
    /// Safe diagnostic message.
    pub message: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveryClassification {
    MigrationFailure,
    QueueInvariantViolation,
    JobRecoveryUnavailable,
    CheckpointInvariantViolation,
}

impl RecoveryClassification {
    const fn details(self) -> (&'static str, &'static str) {
        match self {
            Self::MigrationFailure => (
                "MIGRATION_FAILURE",
                "Database migration could not complete safely. Recovery Mode is active.",
            ),
            Self::QueueInvariantViolation => (
                "QUEUE_INVARIANT_VIOLATION",
                "Durable job ownership or attempt history is inconsistent. Recovery Mode is active.",
            ),
            Self::JobRecoveryUnavailable => (
                "JOB_RECOVERY_UNAVAILABLE",
                "Durable job recovery could not complete safely. Recovery Mode is active.",
            ),
            Self::CheckpointInvariantViolation => (
                "CHECKPOINT_INVARIANT_VIOLATION",
                "Durable checkpoint evidence is inconsistent. Recovery Mode is active.",
            ),
        }
    }
}

impl From<RecoveryClassification> for RecoveryState {
    fn from(classification: RecoveryClassification) -> Self {
        let (code, message) = classification.details();
        Self {
            code: code.to_owned(),
            message: message.to_owned(),
        }
    }
}

impl RecoveryState {
    pub(crate) fn migration_failure() -> Self {
        RecoveryClassification::MigrationFailure.into()
    }

    pub(crate) fn checkpoint_invariant_violation() -> Self {
        RecoveryClassification::CheckpointInvariantViolation.into()
    }
}

impl From<LightweightIntegrityError> for RecoveryState {
    fn from(error: LightweightIntegrityError) -> Self {
        Self {
            code: error.code().to_owned(),
            message: error.safe_message().to_owned(),
        }
    }
}

impl From<JobRepositoryError> for RecoveryState {
    fn from(error: JobRepositoryError) -> Self {
        match error {
            JobRepositoryError::QueueInvariant => {
                RecoveryClassification::QueueInvariantViolation.into()
            }
            _ => RecoveryClassification::JobRecoveryUnavailable.into(),
        }
    }
}

/// A startup failure that cannot safely expose an Erabi recovery surface.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{code}: {message}")]
pub struct StartupFatalError {
    /// Stable machine-readable reason for startup refusal.
    pub code: String,
    /// Safe operator-facing message without configuration values or secrets.
    pub message: String,
}

impl StartupFatalError {
    /// Builds a typed fatal startup error from already-sanitized text.
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

/// Classification returned by an individual startup boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartupFailure {
    /// The process must refuse startup because no safe service can be exposed.
    Fatal(StartupFatalError),
    /// Migration, integrity, or invariant risk requires limited Recovery Mode.
    Recovery(RecoveryState),
}

/// Result of ordered process bootstrap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartupOutcome {
    /// Normal startup, optionally with degraded `Crawl4AI` availability.
    Ready { crawl4ai: Crawl4AiStartupHealth },
    /// Migration/integrity failure selected Recovery Mode before route startup.
    Recovery(RecoveryState),
    /// A non-recoverable bootstrap, filesystem, lock, or listener failure refused startup.
    Fatal(StartupFatalError),
}

/// Dependencies supplied by the runtime startup sequence.
pub trait StartupHooks {
    /// Performs the named startup action.
    ///
    /// # Errors
    /// Classifies a stage failure as fatal or recovery-relevant.
    fn run_stage(
        &mut self,
        stage: StartupStage,
    ) -> Result<Option<Crawl4AiStartupHealth>, StartupFailure>;
}

/// Executes startup in the canonical order.
///
/// # Errors
/// Only explicitly classified migration/integrity errors produce `Recovery`.
/// Bootstrap, lock, listener, and other unsafe-to-serve failures remain fatal.
pub fn run_startup(hooks: &mut impl StartupHooks) -> StartupOutcome {
    let stages = [
        StartupStage::ResolveDataDirectory,
        StartupStage::AcquireProcessLock,
        StartupStage::ValidateBootstrap,
        StartupStage::OpenDatabase,
        StartupStage::ApplyMigrations,
        StartupStage::CheckIntegrity,
        StartupStage::VerifyArtifactDirectories,
        StartupStage::RecoverStaleJobsHook,
        StartupStage::RebuildConcurrencyHook,
        StartupStage::CheckCrawl4Ai,
        StartupStage::StartRoutesAndWorkers,
        StartupStage::ReportReadiness,
    ];
    let mut crawl4ai = Crawl4AiStartupHealth::Available;
    for stage in stages {
        match hooks.run_stage(stage) {
            Ok(Some(health)) if stage == StartupStage::CheckCrawl4Ai => crawl4ai = health,
            Ok(_) => {}
            Err(StartupFailure::Recovery(recovery)) if recovery_is_safe_at(stage) => {
                return StartupOutcome::Recovery(recovery);
            }
            Err(StartupFailure::Recovery(_)) => {
                return StartupOutcome::Fatal(StartupFatalError::new(
                    "STARTUP_FAILURE_MISCLASSIFIED",
                    "Only migration or integrity risk may enter Erabi Recovery Mode.",
                ));
            }
            Err(StartupFailure::Fatal(fatal)) => return StartupOutcome::Fatal(fatal),
        }
    }
    StartupOutcome::Ready { crawl4ai }
}

fn recovery_is_safe_at(stage: StartupStage) -> bool {
    matches!(
        stage,
        StartupStage::ApplyMigrations
            | StartupStage::CheckIntegrity
            | StartupStage::RecoverStaleJobsHook
            | StartupStage::RebuildConcurrencyHook
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_recovery_details(state: &RecoveryState, code: &str, message: &str) {
        assert_eq!(state.code, code);
        assert_eq!(state.message, message);
        assert!(!state.message.contains("SQL"));
    }

    #[test]
    fn boundary_recovery_classifications_keep_stable_sanitized_details() {
        assert_recovery_details(
            &RecoveryState::migration_failure(),
            "MIGRATION_FAILURE",
            "Database migration could not complete safely. Recovery Mode is active.",
        );
        assert_recovery_details(
            &RecoveryState::from(JobRepositoryError::QueueInvariant),
            "QUEUE_INVARIANT_VIOLATION",
            "Durable job ownership or attempt history is inconsistent. Recovery Mode is active.",
        );
        assert_recovery_details(
            &RecoveryState::from(JobRepositoryError::InvalidJobKind),
            "JOB_RECOVERY_UNAVAILABLE",
            "Durable job recovery could not complete safely. Recovery Mode is active.",
        );
        assert_recovery_details(
            &RecoveryState::checkpoint_invariant_violation(),
            "CHECKPOINT_INVARIANT_VIOLATION",
            "Durable checkpoint evidence is inconsistent. Recovery Mode is active.",
        );
    }

    #[test]
    fn integrity_error_conversion_preserves_its_typed_sanitized_details() {
        let state = RecoveryState::from(LightweightIntegrityError::DatabaseUnreadable);

        assert_recovery_details(
            &state,
            "DATABASE_UNREADABLE",
            "The internal database could not complete a required read-only check.",
        );
    }
}
