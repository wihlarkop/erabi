//! Bootstrap and runtime composition for the Erabi process.

pub mod config;
pub mod process_lock;
pub mod startup;

pub use config::{
    BindMode, BootstrapConfig, BootstrapConfigError, Crawl4AiBootstrapConfig, SafeUrl,
    TursoBootstrapConfig,
};
pub use process_lock::{ProcessLock, ProcessLockError, ProcessLockMetadata};
pub use startup::{
    Crawl4AiStartupHealth, RecoveryState, StartupHooks, StartupOutcome, StartupStage, run_startup,
};
