//! Real Plan 03 process composition for the Erabi server.

use std::{
    future::Future,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use axum::Router;
use erabi_api::{AppState, RuntimeMode, SecurityConfig, SecurityConfigError, build_router};
use erabi_db::repositories::JobRepositoryError;
use erabi_db::{
    ArtifactStore, ErabiDatabase, LightweightIntegrityChecker, MigrationRunner,
    repositories::ConcurrencyState,
};
use erabi_jobs::recover_and_rebuild_at;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
    time::{timeout, timeout_at},
};

use crate::{
    BindMode, BootstrapConfig, BootstrapConfigError, Crawl4AiStartupHealth, ProcessLock,
    ProcessLockMetadata, RecoveryState, ShutdownCoordinator, ShutdownFuture, ShutdownHooks,
    ShutdownReport, StartupFatalError, StartupOutcome,
};

/// Errors from composing or running the real Erabi process.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// Bootstrap, filesystem, process-lock, database-open, or listener failure.
    #[error("Erabi startup failed: {0}")]
    Fatal(StartupFatalError),
    /// The operating-system shutdown signal could not be observed.
    #[error("Erabi shutdown signal could not be received")]
    ShutdownSignal(#[source] io::Error),
    /// The HTTP server stopped unexpectedly before requested shutdown.
    #[error("Erabi HTTP server stopped unexpectedly")]
    Server,
}

/// Injectable Plan 03 runtime dependencies; no durable-job implementation is implied.
#[derive(Clone, Debug, Default)]
pub struct RuntimeOptions {
    migration_runner: MigrationRunner,
    crawl4ai_health: Option<Crawl4AiStartupHealth>,
}

impl RuntimeOptions {
    /// Replaces the migration runner for a bounded startup/recovery integration test.
    #[must_use]
    pub fn with_migration_runner(mut self, migration_runner: MigrationRunner) -> Self {
        self.migration_runner = migration_runner;
        self
    }

    /// Supplies an already-normalized `Crawl4AI` health result for a runtime integration.
    #[must_use]
    pub fn with_crawl4ai_health(mut self, crawl4ai_health: Crawl4AiStartupHealth) -> Self {
        self.crawl4ai_health = Some(crawl4ai_health);
        self
    }
}

/// A bound, serving Erabi process with its database and process lock retained.
pub struct RunningRuntime {
    local_address: SocketAddr,
    app_state: AppState,
    startup_outcome: StartupOutcome,
    shutdown: ShutdownCoordinator,
    stop_server: watch::Sender<bool>,
    server_task: JoinHandle<io::Result<()>>,
    _database: ErabiDatabase,
    _concurrency_state: ConcurrencyState,
}

impl RunningRuntime {
    /// Starts Erabi using the production migration runner and `Crawl4AI` probe.
    ///
    /// # Errors
    /// Returns a fatal startup error when no safe HTTP process can be exposed.
    pub async fn start(config: BootstrapConfig) -> Result<Self, RuntimeError> {
        Self::start_with_options(config, RuntimeOptions::default()).await
    }

    /// Starts Erabi from bootstrap loading so invalid configuration is classified as fatal.
    ///
    /// # Errors
    /// Returns `Fatal(BOOTSTRAP_CONFIGURATION_INVALID)` without exposing values or secrets.
    pub async fn start_from_bootstrap(
        config: Result<BootstrapConfig, BootstrapConfigError>,
    ) -> Result<Self, RuntimeError> {
        let config = config.map_err(|_| {
            fatal(
                "BOOTSTRAP_CONFIGURATION_INVALID",
                "Bootstrap configuration is invalid; Erabi did not start.",
            )
        })?;
        Self::start(config).await
    }

    /// Starts Erabi with bounded, injectable Plan 03 startup dependencies.
    ///
    /// The only supported recovery transition is migration or lightweight
    /// integrity risk. All pre-service failures remain fatal.
    ///
    /// # Errors
    /// Returns a typed fatal error for pre-service, database-open, or listener
    /// failures. Migration and integrity risk instead construct a limited
    /// Recovery Mode runtime.
    pub async fn start_with_options(
        config: BootstrapConfig,
        options: RuntimeOptions,
    ) -> Result<Self, RuntimeError> {
        let data_dir = prepare_data_directory(config.data_dir())?;
        let metadata =
            ProcessLockMetadata::current(startup_timestamp(), config.bind_address().to_string());
        let process_lock = ProcessLock::acquire(&data_dir, &metadata).map_err(|_| {
            fatal(
                "PROCESS_LOCK_UNAVAILABLE",
                "The Erabi data directory is already in use or cannot be locked.",
            )
        })?;
        let shutdown = ShutdownCoordinator::with_process_lock(process_lock);

        let database = open_database(&data_dir).await?;
        let mut recovery = None;
        if options.migration_runner.apply(&database).await.is_err() {
            recovery = Some(RecoveryState {
                code: "MIGRATION_FAILURE".to_owned(),
                message: "Database migration could not complete safely. Recovery Mode is active."
                    .to_owned(),
            });
        } else if let Err(error) =
            LightweightIntegrityChecker::new(&database, &options.migration_runner, &data_dir)
                .check()
                .await
        {
            recovery = Some(RecoveryState {
                code: error.code().to_owned(),
                message: error.safe_message().to_owned(),
            });
        }

        let concurrency_state = if recovery.is_none() {
            prepare_artifact_directories(&data_dir)?;
            match run_plan_four_startup_hooks(&database).await {
                Ok(concurrency_state) => concurrency_state,
                Err(recovery_state) => {
                    recovery = Some(recovery_state);
                    ConcurrencyState::default()
                }
            }
        } else {
            ConcurrencyState::default()
        };

        let crawl4ai = if let Some(health) = options.crawl4ai_health {
            health
        } else {
            probe_crawl4ai(&config).await
        };
        let startup_outcome = match recovery {
            Some(recovery) => StartupOutcome::Recovery(recovery),
            None => StartupOutcome::Ready {
                crawl4ai: crawl4ai.clone(),
            },
        };

        let app_state = AppState::with_readiness(false);
        match &startup_outcome {
            StartupOutcome::Recovery(recovery) => {
                app_state.enter_recovery(recovery.code.clone(), recovery.message.clone());
            }
            StartupOutcome::Ready { crawl4ai } => apply_crawl4ai_health(&app_state, crawl4ai),
            StartupOutcome::Fatal(_) => unreachable!("fatal outcomes never create a runtime"),
        }

        // Binding before constructing the policy is necessary for port `0`:
        // Host validation must use the actual listener port, never an unsafe
        // any-port fallback.
        let listener = TcpListener::bind(config.bind_address())
            .await
            .map_err(|_| {
                fatal(
                    "LISTENER_BIND_FAILED",
                    "Erabi could not bind the configured network listener.",
                )
            })?;
        let local_address = listener.local_addr().map_err(|_| {
            fatal(
                "LISTENER_ADDRESS_UNAVAILABLE",
                "Erabi could not inspect the bound network listener.",
            )
        })?;
        let security = security_from_bootstrap(&config, local_address)?;
        let router = build_router(app_state.clone(), security);

        if matches!(&startup_outcome, StartupOutcome::Ready { .. }) {
            app_state.set_ready(true);
        }
        let (stop_server, stop_receiver) = watch::channel(false);
        let server_task = spawn_server(listener, router, stop_receiver);

        Ok(Self {
            local_address,
            app_state,
            startup_outcome,
            shutdown,
            stop_server,
            server_task,
            _database: database,
            _concurrency_state: concurrency_state,
        })
    }

    /// Returns the address selected by the bound TCP listener.
    #[must_use]
    pub const fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    /// Returns the startup classification that created this server.
    #[must_use]
    pub fn startup_outcome(&self) -> &StartupOutcome {
        &self.startup_outcome
    }

    /// Returns the current safe runtime mode for diagnostics/tests.
    #[must_use]
    pub fn runtime_mode(&self) -> RuntimeMode {
        self.app_state.runtime_mode()
    }

    /// Waits for an operating-system termination signal, then shuts down safely.
    ///
    /// # Errors
    /// Returns a sanitized signal or server error.
    pub async fn serve_until_signal(self) -> Result<ShutdownReport, RuntimeError> {
        wait_for_os_shutdown_signal().await?;
        self.shutdown().await
    }

    /// Awaits a runtime-owned shutdown signal and then applies the same bounded
    /// shutdown path used for an operating-system termination signal.
    ///
    /// This is the injectable signal seam used by process integration tests;
    /// it does not introduce a worker or job implementation.
    ///
    /// # Errors
    /// Returns only a sanitized unexpected-server failure.
    pub async fn serve_until_shutdown_signal<F>(
        self,
        shutdown_signal: F,
    ) -> Result<ShutdownReport, RuntimeError>
    where
        F: Future<Output = ()>,
    {
        shutdown_signal.await;
        self.shutdown().await
    }

    /// Stops serving and releases runtime-owned resources by the fixed deadline.
    ///
    /// # Errors
    /// Returns only a sanitized unexpected-server failure.
    pub async fn shutdown(mut self) -> Result<ShutdownReport, RuntimeError> {
        let deadline = self.shutdown.begin_shutdown();
        self.app_state.stop_accepting_mutations();
        let _ = self.stop_server.send(true);

        let hooks = RuntimeShutdownHooks {
            app_state: self.app_state.clone(),
        };
        let report = self.shutdown.shutdown_by(deadline, &hooks).await;
        match timeout_at(deadline, &mut self.server_task).await {
            Ok(Ok(Ok(()))) => Ok(report),
            Ok(Ok(Err(_)) | Err(_)) => Err(RuntimeError::Server),
            Err(_) => {
                self.server_task.abort();
                Ok(report)
            }
        }
    }
}

async fn wait_for_os_shutdown_signal() -> Result<(), RuntimeError> {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(RuntimeError::ShutdownSignal)?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.map_err(RuntimeError::ShutdownSignal),
            _ = sigterm.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(RuntimeError::ShutdownSignal)
    }
}

fn spawn_server(
    listener: TcpListener,
    router: Router,
    mut stop_receiver: watch::Receiver<bool>,
) -> JoinHandle<io::Result<()>> {
    tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                if !*stop_receiver.borrow() {
                    let _ = stop_receiver.changed().await;
                }
            })
            .await
    })
}

fn prepare_data_directory(configured: &Path) -> Result<PathBuf, RuntimeError> {
    std::fs::create_dir_all(configured).map_err(|_| {
        fatal(
            "DATA_DIRECTORY_UNAVAILABLE",
            "Erabi could not create the configured data directory.",
        )
    })?;
    let canonical = configured.canonicalize().map_err(|_| {
        fatal(
            "DATA_DIRECTORY_UNAVAILABLE",
            "Erabi could not canonicalize the configured data directory.",
        )
    })?;
    if !canonical.is_dir() {
        return Err(fatal(
            "DATA_DIRECTORY_UNAVAILABLE",
            "The configured Erabi data path is not a directory.",
        ));
    }
    Ok(canonical)
}

async fn open_database(data_dir: &Path) -> Result<ErabiDatabase, RuntimeError> {
    let database_dir = data_dir.join("database");
    std::fs::create_dir_all(&database_dir).map_err(|_| {
        fatal(
            "DATABASE_DIRECTORY_UNAVAILABLE",
            "Erabi could not prepare its internal database directory.",
        )
    })?;
    ErabiDatabase::open_local(database_dir.join("erabi.db"))
        .await
        .map_err(|_| {
            fatal(
                "DATABASE_OPEN_FAILED",
                "Erabi could not open its internal database.",
            )
        })
}

fn prepare_artifact_directories(data_dir: &Path) -> Result<(), RuntimeError> {
    ArtifactStore::new(data_dir.join("artifacts")).map_err(|_| {
        fatal(
            "ARTIFACT_DIRECTORY_UNAVAILABLE",
            "Erabi could not prepare its controlled artifact directory.",
        )
    })?;
    for directory in ["assets", "exports", "backups"] {
        std::fs::create_dir_all(data_dir.join(directory)).map_err(|_| {
            fatal(
                "ARTIFACT_DIRECTORY_UNAVAILABLE",
                "Erabi could not prepare a required data directory.",
            )
        })?;
    }
    Ok(())
}

/// Executes the Plan 04 durable stale-job recovery and scheduler-state rebuild
/// inside the existing Plan 03 startup boundary. No handler is started here;
/// later plans register concrete work only after this durable state is safe.
async fn run_plan_four_startup_hooks(
    database: &ErabiDatabase,
) -> Result<ConcurrencyState, RecoveryState> {
    recover_and_rebuild_at(database, startup_epoch_seconds())
        .await
        .map(|(_, concurrency_state)| concurrency_state)
        .map_err(|error| match error {
            JobRepositoryError::QueueInvariant => RecoveryState {
                code: "QUEUE_INVARIANT_VIOLATION".to_owned(),
                message: "Durable job ownership or attempt history is inconsistent. Recovery Mode is active."
                    .to_owned(),
            },
            _ => RecoveryState {
                code: "JOB_RECOVERY_UNAVAILABLE".to_owned(),
                message: "Durable job recovery could not complete safely. Recovery Mode is active."
                    .to_owned(),
            },
        })
}

async fn probe_crawl4ai(config: &BootstrapConfig) -> Crawl4AiStartupHealth {
    let Some(url) = config.crawl4ai().base_url() else {
        return Crawl4AiStartupHealth::Degraded {
            message: "Crawl4AI is not configured.".to_owned(),
        };
    };
    let Some(host) = url.as_url().host_str() else {
        return Crawl4AiStartupHealth::Degraded {
            message: "Crawl4AI endpoint is unavailable.".to_owned(),
        };
    };
    let Some(port) = url.as_url().port_or_known_default() else {
        return Crawl4AiStartupHealth::Degraded {
            message: "Crawl4AI endpoint is unavailable.".to_owned(),
        };
    };
    match timeout(Duration::from_secs(1), TcpStream::connect((host, port))).await {
        Ok(Ok(_)) => Crawl4AiStartupHealth::Available,
        _ => Crawl4AiStartupHealth::Degraded {
            message: "Crawl4AI is unavailable; crawling is degraded.".to_owned(),
        },
    }
}

fn apply_crawl4ai_health(app_state: &AppState, health: &Crawl4AiStartupHealth) {
    if let Crawl4AiStartupHealth::Degraded { message } = health {
        app_state.set_crawl4ai_degraded(message.clone());
    }
}

fn security_from_bootstrap(
    config: &BootstrapConfig,
    listener_address: SocketAddr,
) -> Result<SecurityConfig, RuntimeError> {
    let security = match config.bind_mode() {
        BindMode::Loopback => SecurityConfig::loopback(listener_address),
        BindMode::Remote => {
            let token = config.access_token().cloned().ok_or_else(|| {
                fatal(
                    "BOOTSTRAP_CONFIGURATION_INVALID",
                    "Remote Erabi startup requires an access token.",
                )
            })?;
            let origins = config
                .cors_allowed_origins()
                .iter()
                .map(|origin| origin.as_url().origin().ascii_serialization());
            SecurityConfig::remote(listener_address, token, origins)
        }
    }
    .map_err(security_error)?
    .with_openapi_enabled(config.openapi_enabled());
    Ok(security)
}

fn security_error(_error: SecurityConfigError) -> RuntimeError {
    fatal(
        "SECURITY_CONFIGURATION_INVALID",
        "Erabi security configuration is invalid; the server did not start.",
    )
}

fn fatal(code: &str, message: &str) -> RuntimeError {
    RuntimeError::Fatal(StartupFatalError::new(code, message))
}

fn startup_timestamp() -> String {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => format!("unix:{}", duration.as_secs()),
        Err(_) => "unix:0".to_owned(),
    }
}

fn startup_epoch_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
        })
}

struct RuntimeShutdownHooks {
    app_state: AppState,
}

impl ShutdownHooks for RuntimeShutdownHooks {
    fn stop_accepting_mutations_and_jobs(&self) -> ShutdownFuture<'_> {
        Box::pin(async move {
            self.app_state.stop_accepting_mutations();
        })
    }

    fn mark_shutting_down(&self) -> ShutdownFuture<'_> {
        Box::pin(async move {
            self.app_state.mark_shutting_down();
        })
    }

    fn signal_cooperative_cancellation(&self) -> ShutdownFuture<'_> {
        Box::pin(async {})
    }

    fn settle_or_rollback_transactions(&self) -> ShutdownFuture<'_> {
        Box::pin(async {})
    }

    fn flush_critical_state(&self) -> ShutdownFuture<'_> {
        Box::pin(async {})
    }
}
