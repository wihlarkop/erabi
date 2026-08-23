//! Ordered startup orchestration with bounded Plan 04 extension hooks.

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
