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
    lock: Arc<Mutex<Option<ProcessLock>>>,
}

impl RecordingHooks {
    fn new(coordinator: ShutdownCoordinator) -> Self {
        Self {
            coordinator,
            stages: Arc::new(Mutex::new(Vec::new())),
            saw_admission_disabled_before_cancellation: Arc::new(AtomicBool::new(false)),
            hang_cancellation: false,
            lock: Arc::new(Mutex::new(None)),
        }
    }

    fn with_hung_cancellation(mut self) -> Self {
        self.hang_cancellation = true;
        self
    }

    fn with_process_lock(mut self, lock: ProcessLock) -> Self {
        self.lock = Arc::new(Mutex::new(Some(lock)));
        self
    }

    fn stages(&self) -> Vec<ShutdownStage> {
        self.stages
            .lock()
            .map_or_else(|_| Vec::new(), |stages| stages.clone())
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

    fn release_resources(&self) {
        self.record(ShutdownStage::ReleaseResources);
        if let Ok(mut lock) = self.lock.lock() {
            drop(lock.take());
        }
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
        hooks.stages(),
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

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_hung_hook_cannot_extend_the_exact_deadline_and_releases_the_process_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let directory = std::env::temp_dir().join(format!("erabi-shutdown-test-{nonce}"));
    fs::create_dir_all(&directory)?;
    let metadata = ProcessLockMetadata::current("2026-08-23T12:00:00Z", "127.0.0.1:7878");
    let coordinator = ShutdownCoordinator::new();
    let hooks = RecordingHooks::new(coordinator.clone())
        .with_hung_cancellation()
        .with_process_lock(ProcessLock::acquire(&directory, &metadata)?);

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
        hooks.stages().last(),
        Some(&ShutdownStage::ReleaseResources)
    );
    assert!(!format!("{report:?}").contains("test-shared-bearer-token"));

    let reclaimed = ProcessLock::acquire(&directory, &metadata)?;
    drop(reclaimed);
    fs::remove_dir_all(&directory)?;
    Ok(())
}
