use std::{
    fs,
    time::{SystemTime, UNIX_EPOCH},
};

use erabi::{
    Crawl4AiStartupHealth, ProcessLock, ProcessLockError, ProcessLockMetadata, RecoveryState,
    StartupFailure, StartupFatalError, StartupHooks, StartupOutcome, StartupStage, run_startup,
};

struct RecordingHooks {
    stages: Vec<StartupStage>,
    failure: Option<(StartupStage, StartupFailure)>,
    crawl4ai: Crawl4AiStartupHealth,
}

impl StartupHooks for RecordingHooks {
    fn run_stage(
        &mut self,
        stage: StartupStage,
    ) -> Result<Option<Crawl4AiStartupHealth>, StartupFailure> {
        self.stages.push(stage);
        if let Some((failure_stage, failure)) = &self.failure
            && *failure_stage == stage
        {
            return Err(failure.clone());
        }
        Ok((stage == StartupStage::CheckCrawl4Ai).then(|| self.crawl4ai.clone()))
    }
}

#[test]
fn startup_order_preserves_plan_four_recovery_and_concurrency_boundaries() {
    let mut hooks = RecordingHooks {
        stages: Vec::new(),
        failure: None,
        crawl4ai: Crawl4AiStartupHealth::Degraded {
            message: "adapter unavailable".to_owned(),
        },
    };
    assert!(matches!(
        run_startup(&mut hooks),
        StartupOutcome::Ready {
            crawl4ai: Crawl4AiStartupHealth::Degraded { .. }
        }
    ));
    assert_eq!(
        hooks.stages,
        vec![
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
        ]
    );
}

#[test]
fn migration_or_integrity_failure_enters_recovery_before_routes_start() {
    let mut hooks = RecordingHooks {
        stages: Vec::new(),
        failure: Some((
            StartupStage::CheckIntegrity,
            StartupFailure::Recovery(RecoveryState {
                code: "INTEGRITY_FAILURE".to_owned(),
                message: "Integrity check failed safely.".to_owned(),
            }),
        )),
        crawl4ai: Crawl4AiStartupHealth::Available,
    };
    assert!(matches!(
        run_startup(&mut hooks),
        StartupOutcome::Recovery(_)
    ));
    assert!(!hooks.stages.contains(&StartupStage::StartRoutesAndWorkers));
}

#[test]
fn bootstrap_and_live_lock_failures_refuse_startup_instead_of_entering_recovery() {
    for (stage, code) in [
        (
            StartupStage::ValidateBootstrap,
            "BOOTSTRAP_CONFIGURATION_INVALID",
        ),
        (StartupStage::AcquireProcessLock, "PROCESS_LOCK_UNAVAILABLE"),
    ] {
        let mut hooks = RecordingHooks {
            stages: Vec::new(),
            failure: Some((
                stage,
                StartupFailure::Fatal(StartupFatalError::new(code, "Startup must stop safely.")),
            )),
            crawl4ai: Crawl4AiStartupHealth::Available,
        };
        assert!(matches!(
            run_startup(&mut hooks),
            StartupOutcome::Fatal(StartupFatalError { code: observed, .. }) if observed == code
        ));
        assert!(!hooks.stages.contains(&StartupStage::StartRoutesAndWorkers));
    }
}

#[test]
fn recovery_classification_is_rejected_outside_migration_and_integrity_boundaries() {
    let mut hooks = RecordingHooks {
        stages: Vec::new(),
        failure: Some((
            StartupStage::ValidateBootstrap,
            StartupFailure::Recovery(RecoveryState {
                code: "INCORRECTLY_CLASSIFIED".to_owned(),
                message: "This must not expose a recovery server.".to_owned(),
            }),
        )),
        crawl4ai: Crawl4AiStartupHealth::Available,
    };
    assert!(matches!(
        run_startup(&mut hooks),
        StartupOutcome::Fatal(StartupFatalError { code, .. }) if code == "STARTUP_FAILURE_MISCLASSIFIED"
    ));
}

#[test]
fn process_lock_rejects_a_live_owner_and_reclaims_only_after_release()
-> Result<(), Box<dyn std::error::Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let directory = std::env::temp_dir().join(format!("erabi-lock-test-{nonce}"));
    fs::create_dir_all(&directory)?;
    let metadata = ProcessLockMetadata::current("2026-08-23T12:00:00Z", "127.0.0.1:7878");
    let first = ProcessLock::acquire(&directory, &metadata)?;
    assert!(matches!(
        ProcessLock::acquire(&directory, &metadata),
        Err(ProcessLockError::Contended { .. })
    ));
    drop(first);
    let second = ProcessLock::acquire(&directory, &metadata)?;
    assert!(second.path().ends_with(".erabi-process.lock"));
    drop(second);
    fs::remove_dir_all(&directory)?;
    Ok(())
}
