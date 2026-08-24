//! Minimal runtime state shared by API routes.

use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, Ordering},
};

use erabi_db::ErabiDatabase;
use erabi_jobs::{CancellationController, JobActionService, ProgressLiveHub};

/// Runtime service mode used to protect mutable surfaces.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "mode")]
pub enum RuntimeMode {
    /// Startup completed without an integrity risk.
    Normal,
    /// Mutations are unsafe until the stated recovery condition is addressed.
    Recovery { code: String, message: String },
    /// The process has closed admission and is completing bounded shutdown.
    ShuttingDown,
}

/// The reason a state-changing request is currently admitted or rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationAdmission {
    /// Normal mutation handling may continue.
    Allowed,
    /// Recovery Mode protects integrity evidence and normal mutation surfaces.
    Recovery,
    /// Graceful process shutdown has closed new admission.
    ShuttingDown,
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

/// Runtime-owned progress dependencies shared by the authenticated SSE route.
#[derive(Clone, Debug)]
pub(crate) struct ProgressRuntimeState {
    database: ErabiDatabase,
    live_hub: ProgressLiveHub,
}

/// Runtime-owned durable action service shared by protected mutation routes.
#[derive(Clone, Debug)]
pub(crate) struct JobActionRuntimeState {
    service: JobActionService,
}

impl JobActionRuntimeState {
    #[must_use]
    pub(crate) const fn service(&self) -> &JobActionService {
        &self.service
    }
}

impl ProgressRuntimeState {
    #[must_use]
    pub(crate) const fn database(&self) -> &ErabiDatabase {
        &self.database
    }

    #[must_use]
    pub(crate) const fn live_hub(&self) -> &ProgressLiveHub {
        &self.live_hub
    }
}

/// Shared state used by the hardened shell and extended by later runtime tasks.
#[derive(Clone, Debug)]
pub struct AppState {
    runtime: Arc<RwLock<RuntimeSnapshot>>,
    accepting_mutations: Arc<AtomicBool>,
    progress: Option<Arc<ProgressRuntimeState>>,
    job_actions: Option<Arc<JobActionRuntimeState>>,
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
            accepting_mutations: Arc::new(AtomicBool::new(true)),
            progress: None,
            job_actions: None,
        }
    }

    /// Attaches the durable progress store and live fan-out used by Task 2B.
    #[must_use]
    pub fn with_progress_runtime(
        mut self,
        database: ErabiDatabase,
        live_hub: ProgressLiveHub,
    ) -> Self {
        self.progress = Some(Arc::new(ProgressRuntimeState { database, live_hub }));
        self
    }

    #[must_use]
    pub(crate) fn progress_runtime(&self) -> Option<Arc<ProgressRuntimeState>> {
        self.progress.clone()
    }

    /// Attaches the durable Task 4 action service and process cancellation
    /// controller used by protected job mutation routes.
    #[must_use]
    pub fn with_job_actions_runtime(
        mut self,
        database: ErabiDatabase,
        cancellation: CancellationController,
    ) -> Self {
        self.job_actions = Some(Arc::new(JobActionRuntimeState {
            service: JobActionService::new(database, cancellation),
        }));
        self
    }

    #[must_use]
    pub(crate) fn job_actions_runtime(&self) -> Option<Arc<JobActionRuntimeState>> {
        self.job_actions.clone()
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

    /// Stops normal mutation admission during graceful process shutdown.
    pub fn stop_accepting_mutations(&self) {
        self.accepting_mutations.store(false, Ordering::Release);
    }

    /// Makes the graceful-shutdown state observable after admission closes.
    pub fn mark_shutting_down(&self) {
        if let Ok(mut runtime) = self.runtime.write() {
            runtime.mode = RuntimeMode::ShuttingDown;
            runtime.ready = false;
        }
    }

    /// Returns whether the runtime still admits normal mutation requests.
    #[must_use]
    pub fn accepts_mutations(&self) -> bool {
        self.accepting_mutations.load(Ordering::Acquire)
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

    /// Marks `Crawl4AI` as unavailable without entering Recovery Mode.
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
        self.mutation_admission() == MutationAdmission::Allowed
    }

    /// Determines a distinct safe response for each normal mutation boundary.
    #[must_use]
    pub fn mutation_admission(&self) -> MutationAdmission {
        let mode = self.runtime.read().map_or(
            RuntimeMode::Recovery {
                code: "RUNTIME_STATE_UNAVAILABLE".to_owned(),
                message: "Runtime state could not be inspected safely.".to_owned(),
            },
            |runtime| runtime.mode.clone(),
        );
        match mode {
            RuntimeMode::Recovery { .. } => MutationAdmission::Recovery,
            RuntimeMode::Normal if self.accepts_mutations() => MutationAdmission::Allowed,
            RuntimeMode::ShuttingDown | RuntimeMode::Normal => MutationAdmission::ShuttingDown,
        }
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

    /// Provides the `Crawl4AI` availability used by readiness and diagnostics.
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
