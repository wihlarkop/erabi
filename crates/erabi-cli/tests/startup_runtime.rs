use std::{
    fs,
    time::{SystemTime, UNIX_EPOCH},
};

use erabi::{
    Crawl4AiStartupHealth, ProcessLock, ProcessLockError, ProcessLockMetadata, RecoveryState,
    StartupHooks, StartupOutcome, StartupStage, run_startup,
};

struct RecordingHooks {
    stages: Vec<StartupStage>,
    failure: Option<StartupStage>,
    crawl4ai: Crawl4AiStartupHealth,
}

impl StartupHooks for RecordingHooks {
    fn run_stage(
        &mut self,
        stage: StartupStage,
    ) -> Result<Option<Crawl4AiStartupHealth>, RecoveryState> {
        self.stages.push(stage);
        if self.failure == Some(stage) {
            return Err(RecoveryState {
                code: "INTEGRITY_FAILURE".to_owned(),
                message: "Integrity check failed safely.".to_owned(),
            });
        }
        Ok((stage == StartupStage::CheckCrawl4Ai).then(|| self.crawl4ai.clone()))
    }
}

#[test]
fn startup_order_includes_only_plan_four_hooks_not_a_job_implementation() {
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
        failure: Some(StartupStage::CheckIntegrity),
        crawl4ai: Crawl4AiStartupHealth::Available,
    };
    assert!(matches!(
        run_startup(&mut hooks),
        StartupOutcome::Recovery(_)
    ));
    assert!(!hooks.stages.contains(&StartupStage::StartRoutesAndWorkers));
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
