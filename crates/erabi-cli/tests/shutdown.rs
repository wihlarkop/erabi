use std::{
    fs,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use erabi::{
    GRACEFUL_SHUTDOWN_DEADLINE, ProcessLock, ProcessLockMetadata, ShutdownCoordinator,
    ShutdownFuture, ShutdownHooks, ShutdownStage,
};

#[derive(Clone)]
struct RecordingHooks {
    coordinator: ShutdownCoordinator,
    stages: Arc<Mutex<Vec<ShutdownStage>>>,
    saw_admission_disabled_before_cancellation: Arc<AtomicBool>,
    hang_cancellation: bool,
}

impl RecordingHooks {
    fn new(coordinator: ShutdownCoordinator) -> Self {
        Self {
            coordinator,
            stages: Arc::new(Mutex::new(Vec::new())),
            saw_admission_disabled_before_cancellation: Arc::new(AtomicBool::new(false)),
            hang_cancellation: false,
        }
    }

    fn with_hung_cancellation(mut self) -> Self {
        self.hang_cancellation = true;
        self
    }

    fn record(&self, stage: ShutdownStage) {
        if let Ok(mut stages) = self.stages.lock() {
            stages.push(stage);
        }
    }

    fn immediate(&self, stage: ShutdownStage) -> ShutdownFuture<'_> {
        Box::pin(async move {
            self.record(stage);
        })
    }
}

impl ShutdownHooks for RecordingHooks {
    fn stop_accepting_mutations_and_jobs(&self) -> ShutdownFuture<'_> {
        self.immediate(ShutdownStage::StopAcceptingMutationsAndJobs)
    }

    fn mark_shutting_down(&self) -> ShutdownFuture<'_> {
        self.immediate(ShutdownStage::MarkShuttingDown)
    }

    fn signal_cooperative_cancellation(&self) -> ShutdownFuture<'_> {
        Box::pin(async move {
            self.saw_admission_disabled_before_cancellation
                .store(!self.coordinator.accepting_mutations(), Ordering::Release);
            self.record(ShutdownStage::SignalCooperativeCancellation);
            if self.hang_cancellation {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        })
    }

    fn settle_or_rollback_transactions(&self) -> ShutdownFuture<'_> {
        self.immediate(ShutdownStage::SettleOrRollbackTransactions)
    }

    fn flush_critical_state(&self) -> ShutdownFuture<'_> {
        self.immediate(ShutdownStage::FlushCriticalState)
    }
}

#[tokio::test]
async fn shutdown_runs_the_canonical_order_and_completes_cleanly() {
    let coordinator = ShutdownCoordinator::new();
    let hooks = RecordingHooks::new(coordinator.clone());

    let report = coordinator.shutdown(&hooks).await;

    assert!(report.completed_cleanly());
    assert_eq!(report.deadline, GRACEFUL_SHUTDOWN_DEADLINE);
    assert!(coordinator.is_shutting_down());
    assert!(!coordinator.accepting_mutations());
    assert!(
        hooks
            .saw_admission_disabled_before_cancellation
            .load(Ordering::Acquire)
    );
    assert_eq!(
        report.completed_stages,
        vec![
            ShutdownStage::StopAcceptingMutationsAndJobs,
            ShutdownStage::MarkShuttingDown,
            ShutdownStage::SignalCooperativeCancellation,
            ShutdownStage::SettleOrRollbackTransactions,
            ShutdownStage::FlushCriticalState,
            ShutdownStage::ReleaseResources,
        ]
    );
}

/// Represents a legacy user-supplied cleanup callback that never cooperates.
/// It intentionally does not implement `ShutdownHooks`: the coordinator has
/// no extension point that could synchronously invoke it after the deadline.
struct NonCooperativeCleanup {
    attempted: Arc<AtomicBool>,
}

impl NonCooperativeCleanup {
    fn release_resources(&self) {
        self.attempted.store(true, Ordering::Release);
        std::thread::sleep(Duration::from_secs(60));
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn arbitrary_blocking_cleanup_is_excluded_from_the_deadline_critical_path() {
    let coordinator = ShutdownCoordinator::new();
    let hooks = RecordingHooks::new(coordinator.clone());
    let legacy_cleanup = NonCooperativeCleanup {
        attempted: Arc::new(AtomicBool::new(false)),
    };
    std::hint::black_box(NonCooperativeCleanup::release_resources as fn(&NonCooperativeCleanup));

    let report = coordinator.shutdown(&hooks).await;

    assert!(report.completed_cleanly());
    assert_eq!(report.elapsed, Duration::ZERO);
    assert!(!legacy_cleanup.attempted.load(Ordering::Acquire));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_hung_hook_cannot_extend_the_exact_deadline_and_releases_the_process_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let directory = std::env::temp_dir().join(format!("erabi-shutdown-test-{nonce}"));
    fs::create_dir_all(&directory)?;
    let metadata = ProcessLockMetadata::current("2026-08-23T12:00:00Z", "127.0.0.1:7878");
    let coordinator =
        ShutdownCoordinator::with_process_lock(ProcessLock::acquire(&directory, &metadata)?);
    let hooks = RecordingHooks::new(coordinator.clone()).with_hung_cancellation();

    let (report, ()) = tokio::join!(
        coordinator.shutdown(&hooks),
        tokio::time::advance(GRACEFUL_SHUTDOWN_DEADLINE),
    );

    assert_eq!(report.deadline, Duration::from_secs(3));
    assert_eq!(report.elapsed, Duration::from_secs(3));
    assert!(!report.completed_cleanly());
    assert!(
        report
            .timed_out_stages
            .contains(&ShutdownStage::SignalCooperativeCancellation)
    );
    assert_eq!(
        report.completed_stages.last(),
        Some(&ShutdownStage::ReleaseResources)
    );
    assert!(!format!("{report:?}").contains("test-shared-bearer-token"));

    let reclaimed = ProcessLock::acquire(&directory, &metadata)?;
    drop(reclaimed);
    fs::remove_dir_all(&directory)?;
    Ok(())
}
