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

/// Result of ordered process bootstrap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartupOutcome {
    /// Normal startup, optionally with degraded `Crawl4AI` availability.
    Ready { crawl4ai: Crawl4AiStartupHealth },
    /// Migration/integrity failure selected Recovery Mode before route startup.
    Recovery(RecoveryState),
}

/// Dependencies supplied by the runtime without pulling Plan 04 job types forward.
pub trait StartupHooks {
    /// Performs the named startup action.
    ///
    /// `RecoverStaleJobsHook` and `RebuildConcurrencyHook` are intentionally
    /// hook-only until Plan 04 supplies durable jobs/concurrency state.
    ///
    /// # Errors
    /// Returns a typed Recovery Mode condition when the stage cannot proceed safely.
    fn run_stage(
        &mut self,
        stage: StartupStage,
    ) -> Result<Option<Crawl4AiStartupHealth>, RecoveryState>;
}

/// Executes startup in the canonical order.
///
/// # Errors
/// Only migration and integrity errors produce `Recovery`; other hook failures
/// are represented by the caller's typed `RecoveryState` and likewise avoid
/// exposing normal mutable routes.
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
            Err(recovery) => return StartupOutcome::Recovery(recovery),
        }
    }
    StartupOutcome::Ready { crawl4ai }
}
