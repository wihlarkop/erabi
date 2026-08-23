//! Bounded, ordered graceful-shutdown coordination for the Erabi process.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::time::{Instant, timeout_at};

/// The fixed MVP graceful-shutdown deadline.
pub const GRACEFUL_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(3);

/// Ordered shutdown boundaries. Plan 04 extends the hooks, not this deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownStage {
    /// Refuse new state-changing requests and new job submissions.
    StopAcceptingMutationsAndJobs,
    /// Make the process-wide shutting-down state visible to integrations.
    MarkShuttingDown,
    /// Request cooperative cancellation and checkpointing of active work.
    SignalCooperativeCancellation,
    /// Commit or roll back in-flight atomic database work safely.
    SettleOrRollbackTransactions,
    /// Flush critical audit and error records.
    FlushCriticalState,
    /// Release the process lock and remaining owned resources.
    ReleaseResources,
}

/// Borrowed asynchronous work supplied by a shutdown integration.
pub type ShutdownFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Runtime extension points for later worker, persistence, and audit subsystems.
///
/// Hooks must be cooperative and non-blocking. Each asynchronous hook is
/// bounded by the one process-wide deadline; no hook receives extra time.
/// Resource release is synchronous so an expired deadline cannot strand the
/// local process lock.
pub trait ShutdownHooks {
    /// Stops new mutation and job admission at the owning subsystem boundary.
    fn stop_accepting_mutations_and_jobs(&self) -> ShutdownFuture<'_>;

    /// Marks the participating subsystem as shutting down.
    fn mark_shutting_down(&self) -> ShutdownFuture<'_>;

    /// Signals cancellation and allows bounded checkpoint work.
    fn signal_cooperative_cancellation(&self) -> ShutdownFuture<'_>;

    /// Safely completes or rolls back active atomic database work.
    fn settle_or_rollback_transactions(&self) -> ShutdownFuture<'_>;

    /// Flushes critical audit and error state.
    fn flush_critical_state(&self) -> ShutdownFuture<'_>;

    /// Releases resources such as the process lock without waiting past the deadline.
    fn release_resources(&self);
}

/// A safe summary containing only stage identities and elapsed time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShutdownReport {
    /// Fixed deadline used for this shutdown.
    pub deadline: Duration,
    /// Time consumed before resource release. Paused-time tests can assert this exactly.
    pub elapsed: Duration,
    /// Stages that completed before the fixed deadline.
    pub completed_stages: Vec<ShutdownStage>,
    /// Cooperative stages that did not complete before the fixed deadline.
    pub timed_out_stages: Vec<ShutdownStage>,
}

impl ShutdownReport {
    /// Returns true only when every cooperative stage completed by the deadline.
    #[must_use]
    pub fn completed_cleanly(&self) -> bool {
        self.timed_out_stages.is_empty()
    }
}

/// Shared process shutdown state with a fixed deadline coordinator.
#[derive(Clone, Debug)]
pub struct ShutdownCoordinator {
    accepting_mutations: Arc<AtomicBool>,
    shutting_down: Arc<AtomicBool>,
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownCoordinator {
    /// Creates a coordinator that accepts normal work until shutdown starts.
    #[must_use]
    pub fn new() -> Self {
        Self {
            accepting_mutations: Arc::new(AtomicBool::new(true)),
            shutting_down: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns whether the runtime may accept new mutations or job submissions.
    #[must_use]
    pub fn accepting_mutations(&self) -> bool {
        self.accepting_mutations.load(Ordering::Acquire)
    }

    /// Returns whether graceful shutdown has begun.
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    /// Runs the canonical shutdown sequence within exactly three seconds.
    ///
    /// The admission gate changes before any cooperative hook is called. Once
    /// the single deadline expires, pending hooks are cancelled and resources
    /// are released immediately rather than waiting for a crawl or download.
    pub async fn shutdown(&self, hooks: &impl ShutdownHooks) -> ShutdownReport {
        let started = Instant::now();
        let deadline = started + GRACEFUL_SHUTDOWN_DEADLINE;
        let mut completed_stages = Vec::new();
        let mut timed_out_stages = Vec::new();

        self.accepting_mutations.store(false, Ordering::Release);
        run_stage(
            hooks,
            ShutdownStage::StopAcceptingMutationsAndJobs,
            deadline,
            &mut completed_stages,
            &mut timed_out_stages,
        )
        .await;

        self.shutting_down.store(true, Ordering::Release);
        for stage in [
            ShutdownStage::MarkShuttingDown,
            ShutdownStage::SignalCooperativeCancellation,
            ShutdownStage::SettleOrRollbackTransactions,
            ShutdownStage::FlushCriticalState,
        ] {
            run_stage(
                hooks,
                stage,
                deadline,
                &mut completed_stages,
                &mut timed_out_stages,
            )
            .await;
        }

        hooks.release_resources();
        completed_stages.push(ShutdownStage::ReleaseResources);

        ShutdownReport {
            deadline: GRACEFUL_SHUTDOWN_DEADLINE,
            elapsed: Instant::now().saturating_duration_since(started),
            completed_stages,
            timed_out_stages,
        }
    }
}

async fn run_stage(
    hooks: &impl ShutdownHooks,
    stage: ShutdownStage,
    deadline: Instant,
    completed_stages: &mut Vec<ShutdownStage>,
    timed_out_stages: &mut Vec<ShutdownStage>,
) {
    let future = match stage {
        ShutdownStage::StopAcceptingMutationsAndJobs => hooks.stop_accepting_mutations_and_jobs(),
        ShutdownStage::MarkShuttingDown => hooks.mark_shutting_down(),
        ShutdownStage::SignalCooperativeCancellation => hooks.signal_cooperative_cancellation(),
        ShutdownStage::SettleOrRollbackTransactions => hooks.settle_or_rollback_transactions(),
        ShutdownStage::FlushCriticalState => hooks.flush_critical_state(),
        ShutdownStage::ReleaseResources => return,
    };
    if timeout_at(deadline, future).await.is_ok() {
        completed_stages.push(stage);
    } else {
        timed_out_stages.push(stage);
    }
}
