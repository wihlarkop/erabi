//! Bootstrap and runtime composition for the Erabi process.

pub mod config;
pub mod process_lock;
pub mod runtime;
pub mod shutdown;
pub mod startup;

pub use config::{
    BindMode, BootstrapConfig, BootstrapConfigError, Crawl4AiBootstrapConfig, SafeUrl,
    TursoBootstrapConfig,
};
pub use process_lock::{ProcessLock, ProcessLockError, ProcessLockMetadata};
pub use runtime::{RunningRuntime, RuntimeError, RuntimeOptions};
pub use shutdown::{
    GRACEFUL_SHUTDOWN_DEADLINE, ShutdownCoordinator, ShutdownFuture, ShutdownHooks, ShutdownReport,
    ShutdownStage,
};
pub use startup::{
    Crawl4AiStartupHealth, RecoveryState, StartupFailure, StartupFatalError, StartupHooks,
    StartupOutcome, StartupStage, run_startup,
};
