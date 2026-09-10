//! Durable execution handler for provider-neutral one-page Quick Scrapes.

use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use erabi_crawler::{
    AdmissionError, CrawlRecoveryCheckpoint, CrawlRecoveryPhase, CrawlerAdapter,
    CrawlerAdapterError, CrawlerArtifactEvidence, CrawlerArtifactKind, CrawlerEvidencePolicy,
    CrawlerExecuteRequest, CrawlerResultCompleteness, NetworkTargetPolicy, OriginKey,
    PacingCancellation, PacingOutcome, PacingService, RenderingRequirement,
    RobotsAdmissionDecision, RobotsPolicyError, RobotsPolicyService, ScreenshotPolicy,
    quick_scrape_snapshot_target,
};
use erabi_db::{
    ArtifactStore, ErabiDatabase,
    repositories::{
        ArtifactRepository, CrawlAdmissionState, CrawlExecutionArtifact,
        CrawlExecutionArtifactKind, CrawlExecutionRecord, CrawlExecutionRepository,
        CrawlExecutionRepositoryError, CrawlExecutionSummary, CrawlRecoveryActionKind,
        CrawlRunRepository, CrawlTraversalControl, CrawlTraversalRepository,
        CrawlTraversalRepositoryError, CrawlUrlStateRecord, CrawlWorkState, JobRepository,
        SourceRepository,
    },
};
use erabi_domain::{
    CrawlExecutionErrorCode, CrawlExecutionId, CrawlExecutionOutcome, CrawlRunId, CrawlRunStatus,
    SourceId, SourceTargetType,
};
use erabi_observability::{
    ArtifactKind, CrawlExecutionSpan, EventOutcome, ProviderToken, SemanticEvent, TelemetryCode,
    emit,
};
use uuid::Uuid;

use crate::{
    ExecutionAction, ExecutionDiagnostic, ExecutionDiagnostics, ExecutionOperation,
    JobExecutionContext, JobExecutionError, JobHandler, NewProgressEvent,
    OrchestrationErrorCategory, ProgressAttemptId, ProgressKey, ProgressLiveHub, ProgressMetadata,
    ProgressPublication, ProgressService, ProgressTerminalState,
    recovery::{
        CrawlRecoveryValidationError, checkpoint_error_code, map_checkpoint_repository_error,
        validate_crawl_recovery,
    },
};

/// A Quick Scrape root performs its initial attempt and one automatic retry.
/// Further configured attempt budget remains available to an explicit durable
/// Retry child, preserving the operator-visible generation boundary.
const QUICK_SCRAPE_AUTOMATIC_MAX_ATTEMPTS: u32 = 2;

#[derive(Clone, Debug)]
struct QuickScrapeError {
    diagnostics: ExecutionDiagnostics,
}

type QuickScrapeResult<T> = Result<T, QuickScrapeError>;

impl QuickScrapeError {
    fn new(diagnostic: ExecutionDiagnostic) -> Self {
        let mut diagnostics = ExecutionDiagnostics::new();
        diagnostics.add_primary(diagnostic);
        Self { diagnostics }
    }

    fn progress(operation: ExecutionOperation, code: &'static str) -> Self {
        Self::new(ExecutionDiagnostic::new(
            OrchestrationErrorCategory::ProgressPublication,
            operation,
            ExecutionAction::Reconcile,
            code,
        ))
    }

    fn repository(operation: ExecutionOperation, code: &'static str) -> Self {
        Self::new(ExecutionDiagnostic::new(
            OrchestrationErrorCategory::Repository,
            operation,
            ExecutionAction::Retry,
            code,
        ))
    }

    fn checkpoint(operation: ExecutionOperation, code: &'static str) -> Self {
        Self::new(ExecutionDiagnostic::new(
            OrchestrationErrorCategory::CheckpointRecovery,
            operation,
            ExecutionAction::Retry,
            code,
        ))
    }

    fn artifact(operation: ExecutionOperation, code: &'static str) -> Self {
        Self::new(ExecutionDiagnostic::new(
            OrchestrationErrorCategory::Artifact,
            operation,
            ExecutionAction::Retry,
            code,
        ))
    }
}

/// Focused Plan 06 handler wired into the existing generic durable runtime.
/// The adapter remains provider-neutral; no `Crawl4AI` DTO or handle enters this
/// module's durable state.
#[derive(Clone)]
pub struct QuickScrapeJobHandler {
    database: ErabiDatabase,
    adapter: Arc<dyn CrawlerAdapter>,
    robots: RobotsPolicyService,
    pacing: PacingService,
    network_policy: NetworkTargetPolicy,
    artifact_store: ArtifactStore,
    progress_live_hub: Option<ProgressLiveHub>,
    fail_terminal_progress_append_for_test: bool,
}

impl std::fmt::Debug for QuickScrapeJobHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuickScrapeJobHandler")
            .field("adapter", &"configured")
            .field("robots", &self.robots)
            .field("pacing", &self.pacing)
            .finish_non_exhaustive()
    }
}

#[allow(clippy::result_large_err)]
impl QuickScrapeJobHandler {
    #[must_use]
    pub fn new(
        database: ErabiDatabase,
        adapter: Arc<dyn CrawlerAdapter>,
        robots: RobotsPolicyService,
        pacing: PacingService,
        network_policy: NetworkTargetPolicy,
        artifact_store: ArtifactStore,
    ) -> Self {
        Self {
            database,
            adapter,
            robots,
            pacing,
            network_policy,
            artifact_store,
            progress_live_hub: None,
            fail_terminal_progress_append_for_test: false,
        }
    }

    pub(crate) fn database(&self) -> &ErabiDatabase {
        &self.database
    }

    #[must_use]
    pub fn with_progress_live_hub(mut self, progress_live_hub: ProgressLiveHub) -> Self {
        self.progress_live_hub = Some(progress_live_hub);
        self
    }

    /// Injects one terminal durable-progress append failure for a handler
    /// boundary test. The CrawlRun business commit remains unaffected.
    #[doc(hidden)]
    #[must_use]
    pub fn with_terminal_progress_append_failure_for_test(mut self) -> Self {
        self.fail_terminal_progress_append_for_test = true;
        self
    }

    // This is the intentionally linear durable attempt lifecycle. Keeping the
    // admissions, provider call, and durable completion in one visible order
    // makes RAII release and crash boundaries auditable.
    #[allow(clippy::too_many_lines)]
    async fn execute_inner(&self, context: JobExecutionContext) -> QuickScrapeResult<()> {
        if !matches!(
            context.kind().as_str(),
            "QUICK_SCRAPE"
                | "RETRY"
                | "RETRY_FAILED_PARTS"
                | "RESUME_CHECKPOINT"
                | "RERUN_FULL_CRAWL"
                | "RESTART_FROM_BEGINNING"
        ) {
            return Err(QuickScrapeError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Invariant,
                ExecutionOperation::QueueLifecycle,
                ExecutionAction::Fail,
                "JOB_KIND_UNSUPPORTED",
            )));
        }
        let job = JobRepository::new(&self.database)
            .job(context.job_id())
            .await
            .map_err(|_| {
                QuickScrapeError::repository(ExecutionOperation::LoadJob, "JOB_LOAD_FAILED")
            })?;
        let terminal_attempt = quick_scrape_terminal_attempt(&context, &job);
        let stored_run_id = job.crawl_run_id.as_deref().ok_or_else(|| {
            QuickScrapeError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Invariant,
                ExecutionOperation::LoadJob,
                ExecutionAction::Fail,
                "JOB_RUN_ID_MISSING",
            ))
        })?;
        let run_id = parse_run_id(stored_run_id).map_err(|()| {
            QuickScrapeError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Invariant,
                ExecutionOperation::LoadRunSnapshot,
                ExecutionAction::Fail,
                "RUN_ID_INVALID",
            ))
        })?;
        let snapshot = CrawlRunRepository::new(&self.database)
            .snapshot(run_id)
            .await
            .map_err(|_| {
                QuickScrapeError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "RUN_SNAPSHOT_LOAD_FAILED",
                )
            })?;
        let target = quick_scrape_snapshot_target(&snapshot).map_err(|_| {
            QuickScrapeError::checkpoint(
                ExecutionOperation::LoadRunSnapshot,
                "SNAPSHOT_TARGET_INVALID",
            )
        })?;
        let source_id = parse_source_id(&target.source_id).map_err(|()| {
            QuickScrapeError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Invariant,
                ExecutionOperation::LoadRunSnapshot,
                ExecutionAction::Fail,
                "SOURCE_ID_INVALID",
            ))
        })?;
        let source = SourceRepository::new(&self.database)
            .read(source_id)
            .await
            .map_err(|_| {
                QuickScrapeError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "SOURCE_LOAD_FAILED",
                )
            })?;
        if source.canonical_url != target.target_url {
            return Err(QuickScrapeError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Invariant,
                ExecutionOperation::LoadRunSnapshot,
                ExecutionAction::Fail,
                "SOURCE_TARGET_MISMATCH",
            )));
        }
        let latest_checkpoint = if matches!(
            context.kind().as_str(),
            "RERUN_FULL_CRAWL" | "RESTART_FROM_BEGINNING"
        ) {
            None
        } else {
            JobRepository::new(&self.database)
                .latest_checkpoint_for_lineage(context.job_id())
                .await
                .map_err(|error| {
                    quick_checkpoint_load_error(ExecutionOperation::LoadCheckpoint, &error)
                })?
        };
        let recovery_kind = match context.kind().as_str() {
            "RETRY" => Some(CrawlRecoveryActionKind::Retry),
            "RETRY_FAILED_PARTS" => Some(CrawlRecoveryActionKind::RetryFailedParts),
            "RESTART_FROM_BEGINNING" => Some(CrawlRecoveryActionKind::RestartFromBeginning),
            _ => None,
        };
        let durable_state = match CrawlTraversalRepository::new(&self.database)
            .reconstruct_recovery_state(run_id)
            .await
        {
            Ok(state) => Some(state),
            Err(CrawlTraversalRepositoryError::CrawlRunNotFound) => None,
            Err(error) => {
                return Err(quick_traversal_error(
                    ExecutionOperation::LoadCheckpoint,
                    error,
                ));
            }
        };
        let recovery_requires_validation = latest_checkpoint.is_some()
            || matches!(
                context.kind().as_str(),
                "RETRY" | "RETRY_FAILED_PARTS" | "RESUME_CHECKPOINT"
            );
        if recovery_requires_validation {
            let current_status = CrawlRunRepository::new(&self.database)
                .status(run_id)
                .await
                .map_err(|_| {
                    QuickScrapeError::repository(
                        ExecutionOperation::LoadRunSnapshot,
                        "RUN_STATUS_LOAD_FAILED",
                    )
                })?;
            let executions = CrawlExecutionRepository::new(&self.database)
                .list_for_run(run_id)
                .await
                .map_err(|_| {
                    QuickScrapeError::repository(
                        ExecutionOperation::LoadRunSnapshot,
                        "EXECUTIONS_LOAD_FAILED",
                    )
                })?;
            let discovered = CrawlRunRepository::new(&self.database)
                .discovered_urls(run_id)
                .await
                .map_err(|_| {
                    QuickScrapeError::repository(
                        ExecutionOperation::LoadRunSnapshot,
                        "DISCOVERED_URLS_LOAD_FAILED",
                    )
                })?;
            validate_crawl_recovery(
                latest_checkpoint.as_ref(),
                &snapshot,
                run_id,
                current_status,
                &executions,
                &discovered,
                durable_state.as_ref(),
            )
            .map_err(|error| quick_recovery_error(ExecutionOperation::LoadCheckpoint, error))?;
        }
        if let Some(kind) = recovery_kind {
            // Persist a compact action marker only after canonical checkpoint
            // and durable-state validation has accepted this recovery. This
            // keeps an action child replayable if the process dies between
            // action preparation and its next ordinary checkpoint. A fresh
            // Restart action may have no prior checkpoint; its marker remains
            // bounded and is followed by normal initialization.
            let action_checkpoint = latest_checkpoint
                .as_ref()
                .map(|record| record.checkpoint.clone())
                .unwrap_or(
                    CrawlRecoveryCheckpoint::new(run_id, &snapshot, CrawlRecoveryPhase::Traversing)
                        .map_err(|_| {
                            QuickScrapeError::checkpoint(
                                ExecutionOperation::Serialization,
                                "CHECKPOINT_BUILD_FAILED",
                            )
                        })?
                        .to_envelope()
                        .map_err(|_| {
                            QuickScrapeError::checkpoint(
                                ExecutionOperation::Serialization,
                                "CHECKPOINT_ENVELOPE_FAILED",
                            )
                        })?,
                );
            context.checkpoint(&action_checkpoint).await.map_err(|_| {
                QuickScrapeError::checkpoint(
                    ExecutionOperation::LoadCheckpoint,
                    "CHECKPOINT_PERSIST_FAILED",
                )
            })?;
            CrawlTraversalRepository::new(&self.database)
                .prepare_recovery_action(
                    context.job_id(),
                    context.attempt_id(),
                    run_id,
                    kind,
                    context.ownership_now(),
                )
                .await
                .map_err(|_| {
                    QuickScrapeError::repository(
                        ExecutionOperation::QueueLifecycle,
                        "RECOVERY_ACTION_PERSIST_FAILED",
                    )
                })?;
        }
        let current_work_completed = durable_state.as_ref().is_some_and(|state| {
            state.work.iter().any(|work| {
                work.id == quick_url_state_id(run_id)
                    && work.admission_state == CrawlAdmissionState::Admitted
                    && work.current_work_state == Some(CrawlWorkState::Completed)
            })
        }) && recovery_kind
            != Some(CrawlRecoveryActionKind::RestartFromBeginning);
        if current_work_completed {
            return self
                .finish_recovered_execution(&context, run_id, snapshot, CrawlRunStatus::Running)
                .await;
        }
        let mut execution_id = execution_id_for_job(context.job_id().as_str()).map_err(|()| {
            QuickScrapeError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Invariant,
                ExecutionOperation::PersistExecution,
                ExecutionAction::Fail,
                "EXECUTION_ID_INVALID",
            ))
        })?;
        let executions = CrawlExecutionRepository::new(&self.database);
        match executions.read(execution_id).await {
            Ok(existing) => {
                if existing.outcome == CrawlExecutionOutcome::Completed {
                    return Err(QuickScrapeError::repository(
                        ExecutionOperation::PersistExecution,
                        "EXECUTION_ALREADY_COMPLETED",
                    ));
                }
                execution_id = CrawlExecutionId::new();
            }
            Err(CrawlExecutionRepositoryError::NotFound) => {}
            Err(_) => {
                return Err(QuickScrapeError::repository(
                    ExecutionOperation::LoadJob,
                    "EXECUTION_LOAD_FAILED",
                ));
            }
        }
        let run_repository = CrawlRunRepository::new(&self.database);
        if let Some(CrawlRecoveryActionKind::RestartFromBeginning) = recovery_kind {
            run_repository
                .transition_restart_status(run_id)
                .await
                .map_err(|_| {
                    QuickScrapeError::repository(
                        ExecutionOperation::TransitionRun,
                        "RUN_RESTART_TRANSITION_FAILED",
                    )
                })?;
        } else if recovery_kind.is_some() {
            run_repository
                .transition_recovery_status(run_id)
                .await
                .map_err(|_| {
                    QuickScrapeError::repository(
                        ExecutionOperation::TransitionRun,
                        "RUN_RECOVERY_TRANSITION_FAILED",
                    )
                })?;
        } else {
            run_repository
                .transition_execution_status(run_id, CrawlRunStatus::Running)
                .await
                .map_err(|_| {
                    QuickScrapeError::repository(
                        ExecutionOperation::TransitionRun,
                        "RUN_EXECUTION_TRANSITION_FAILED",
                    )
                })?;
        }
        self.progress(&context, "STARTED", None).await?;
        if latest_checkpoint.is_none() {
            if durable_state.is_none() {
                self.initialize_quick_work(run_id, target.target_url.as_str())
                    .await?;
            }
            self.save_quick_checkpoint(
                &context,
                &snapshot,
                run_id,
                CrawlRecoveryPhase::Initialized,
            )
            .await?;
        }
        if context.cancellation().is_cancelled() {
            return self
                .finish_recovered_execution(&context, run_id, snapshot, CrawlRunStatus::Cancelled)
                .await;
        }
        if context.storage_pressure().is_signalled() {
            return Ok(());
        }
        let expected_work_generation = CrawlTraversalRepository::new(&self.database)
            .read_work_generation(run_id, &quick_url_state_id(run_id))
            .await
            .map_err(|_| {
                QuickScrapeError::repository(
                    ExecutionOperation::PersistExecution,
                    "WORK_GENERATION_LOAD_FAILED",
                )
            })?;
        CrawlExecutionRepository::new(&self.database)
            .activate_current_work(
                run_id,
                &quick_url_state_id(run_id),
                context.job_id(),
                context.attempt_id(),
                expected_work_generation,
                context.ownership_now(),
            )
            .await
            .map_err(|_| {
                QuickScrapeError::repository(
                    ExecutionOperation::PersistExecution,
                    "EXECUTION_ACTIVATION_FAILED",
                )
            })?;

        // A confident Task 4 FileAsset classification is a durable completed
        // Quick Scrape without an HTML-provider request or Plan 08 download.
        if target.source_target_type == SourceTargetType::FileAsset {
            self.progress(&context, "DIRECT_FILE_CLASSIFIED", None)
                .await?;
            let record = CrawlExecutionRecord {
                id: execution_id,
                crawl_run_id: run_id,
                requested_url: target.target_url.to_string(),
                canonical_url: target.target_url.to_string(),
                observed_final_url: None,
                source_id: Some(source_id),
                page_type_id: None,
                transition_id: None,
                discovered_url_id: None,
                outcome: CrawlExecutionOutcome::Completed,
                error_code: None,
                http_status: None,
                media_type: direct_file_media_type(&snapshot),
                content_length_bytes: None,
                provider_elapsed_ms: None,
                artifacts: Vec::new(),
            };
            self.persist_record(&record, &context, Some(expected_work_generation))
                .await?;
            self.save_quick_checkpoint(&context, &snapshot, run_id, CrawlRecoveryPhase::Finalizing)
                .await?;
            return self
                .finish_recovered_execution(&context, run_id, snapshot, CrawlRunStatus::Running)
                .await;
        }

        let origin = OriginKey::from_url(&target.target_url).map_err(|_| {
            QuickScrapeError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::NetworkAdmission,
                ExecutionOperation::AcquireAdmission,
                ExecutionAction::Fail,
                "ORIGIN_INVALID",
            ))
        })?;
        // Registration is a runtime-only RAII contribution to Task 5's
        // process-wide same-origin registry. It cannot cross attempts/restarts.
        let registration = match self.pacing.register(origin, &snapshot) {
            Ok(registration) => registration,
            Err(AdmissionError::Cancelled) => {
                context.cancellation().cancel();
                return self
                    .finish_recovered_execution(
                        &context,
                        run_id,
                        snapshot.clone(),
                        CrawlRunStatus::Cancelled,
                    )
                    .await;
            }
            Err(error) => {
                return self
                    .terminal_failure(
                        &context,
                        &FailureContext {
                            kind: TerminalFailureKind::Pacing,
                            run_id,
                            execution_id,
                            source_id,
                            target_url: &target.target_url,
                            error_code: CrawlExecutionErrorCode::RemoteFailure,
                            http_status: None,
                            retryable: pacing_failure_is_retryable(error),
                            terminal_attempt,
                            expected_work_generation,
                        },
                    )
                    .await;
            }
        };
        let pacing_cancellation = PacingCancellation::new();
        let robots = tokio::select! {
            value = self.robots.evaluate(&target.target_url, &snapshot, &pacing_cancellation) => value,
            () = context.storage_pressure().signalled() => {
                pacing_cancellation.cancel();
                return Ok(());
            }
            () = context.cancellation().cancelled() => {
                pacing_cancellation.cancel();
                return self
                    .finish_recovered_execution(
                        &context,
                        run_id,
                        snapshot.clone(),
                        CrawlRunStatus::Cancelled,
                    )
                    .await;
            }
        };
        let robots = match robots {
            Ok(robots) => robots,
            Err(RobotsPolicyError::Admission(AdmissionError::Cancelled)) => {
                context.cancellation().cancel();
                return self
                    .finish_recovered_execution(
                        &context,
                        run_id,
                        snapshot.clone(),
                        CrawlRunStatus::Cancelled,
                    )
                    .await;
            }
            Err(error) => {
                if let Some(diagnostic) = robots_pacing_diagnostic(&error) {
                    context.record_secondary_diagnostic(
                        diagnostic.with_run(run_id).with_execution(execution_id),
                    );
                }
                return self
                    .terminal_failure(
                        &context,
                        &FailureContext {
                            kind: TerminalFailureKind::NetworkAdmission,
                            run_id,
                            execution_id,
                            source_id,
                            target_url: &target.target_url,
                            error_code: CrawlExecutionErrorCode::RemoteFailure,
                            http_status: None,
                            retryable: robots_failure_is_retryable(&error),
                            terminal_attempt,
                            expected_work_generation,
                        },
                    )
                    .await;
            }
        };
        if robots.decision() == RobotsAdmissionDecision::Disallowed {
            return self
                .terminal_failure(
                    &context,
                    &FailureContext {
                        kind: TerminalFailureKind::NetworkAdmission,
                        run_id,
                        execution_id,
                        source_id,
                        target_url: &target.target_url,
                        error_code: CrawlExecutionErrorCode::RobotsExcluded,
                        http_status: None,
                        retryable: false,
                        terminal_attempt: true,
                        expected_work_generation,
                    },
                )
                .await;
        }
        let permit = tokio::select! {
            value = registration.acquire(&robots, &pacing_cancellation) => value,
            () = context.storage_pressure().signalled() => {
                pacing_cancellation.cancel();
                return Ok(());
            }
            () = context.cancellation().cancelled() => {
                pacing_cancellation.cancel();
                return self
                    .finish_recovered_execution(
                        &context,
                        run_id,
                        snapshot.clone(),
                        CrawlRunStatus::Cancelled,
                    )
                    .await;
            }
        };
        let permit = match permit {
            Ok(permit) => permit,
            Err(AdmissionError::Cancelled) => {
                context.cancellation().cancel();
                return self
                    .finish_recovered_execution(
                        &context,
                        run_id,
                        snapshot.clone(),
                        CrawlRunStatus::Cancelled,
                    )
                    .await;
            }
            Err(error) => {
                return self
                    .terminal_failure(
                        &context,
                        &FailureContext {
                            kind: TerminalFailureKind::Pacing,
                            run_id,
                            execution_id,
                            source_id,
                            target_url: &target.target_url,
                            error_code: CrawlExecutionErrorCode::RemoteFailure,
                            http_status: None,
                            retryable: pacing_failure_is_retryable(error),
                            terminal_attempt,
                            expected_work_generation,
                        },
                    )
                    .await;
            }
        };
        self.progress(&context, "LOADING", None).await?;
        let request = CrawlerExecuteRequest::try_new(
            target.target_url.clone(),
            Duration::from_millis(snapshot.settings().timeout_ms.value),
            snapshot.settings().user_agent.value.clone(),
            RenderingRequirement::RenderedHtml,
            None,
            None,
            CrawlerEvidencePolicy {
                raw_html: false,
                cleaned_html: true,
                rendered_html: true,
                markdown: true,
                screenshot: if snapshot.settings().screenshot.value {
                    ScreenshotPolicy::Viewport
                } else {
                    ScreenshotPolicy::None
                },
                ..CrawlerEvidencePolicy::default()
            },
        )
        .map_err(|_| {
            QuickScrapeError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Invariant,
                ExecutionOperation::ProviderExecution,
                ExecutionAction::Fail,
                "PROVIDER_REQUEST_INVALID",
            ))
        })?;
        let provider = tokio::select! {
            value = async {
                let started = Instant::now();
                let result = self.adapter.execute(request).await;
                (started.elapsed(), result)
            } => value,
            () = context.storage_pressure().signalled() => {
                pacing_cancellation.cancel();
                return Ok(());
            }
            () = context.cancellation().cancelled() => {
                pacing_cancellation.cancel();
                return self
                    .finish_recovered_execution(
                        &context,
                        run_id,
                        snapshot.clone(),
                        CrawlRunStatus::Cancelled,
                    )
                    .await;
            }
        };
        let (provider_duration, provider) = provider;
        emit(SemanticEvent::ProviderExecuteCompleted {
            context: crate::telemetry_crawl_context(
                &context,
                Some(&run_id.to_string()),
                Some(&execution_id.to_string()),
            ),
            provider: ProviderToken::Crawl4Ai,
            outcome: if provider.is_ok() {
                EventOutcome::Success
            } else {
                EventOutcome::Failure
            },
            duration_ms: u64::try_from(provider_duration.as_millis()).unwrap_or(u64::MAX),
            code: provider.as_ref().err().map(|error| {
                TelemetryCode::from_static(crawl_execution_code_name(adapter_error_code(error)))
            }),
        });
        let result = match provider {
            Ok(result) => {
                if permit.record_outcome(PacingOutcome::Success).is_err() {
                    context.record_secondary_diagnostic(
                        ExecutionDiagnostic::new(
                            OrchestrationErrorCategory::Pacing,
                            ExecutionOperation::RecordOutcome,
                            ExecutionAction::Continue,
                            "PACING_OUTCOME_RECORD_FAILED",
                        )
                        .with_run(run_id)
                        .with_execution(execution_id),
                    );
                }
                result
            }
            Err(error) => {
                if permit
                    .record_outcome(PacingOutcome::from_adapter_error(&error))
                    .is_err()
                {
                    context.record_secondary_diagnostic(
                        ExecutionDiagnostic::new(
                            OrchestrationErrorCategory::Pacing,
                            ExecutionOperation::RecordOutcome,
                            ExecutionAction::Continue,
                            "PACING_OUTCOME_RECORD_FAILED",
                        )
                        .with_run(run_id)
                        .with_execution(execution_id),
                    );
                }
                if matches!(error, CrawlerAdapterError::Cancelled) {
                    context.cancellation().cancel();
                    return self
                        .finish_recovered_execution(
                            &context,
                            run_id,
                            snapshot.clone(),
                            CrawlRunStatus::Cancelled,
                        )
                        .await;
                }
                return self
                    .terminal_failure(
                        &context,
                        &FailureContext {
                            kind: TerminalFailureKind::Provider,
                            run_id,
                            execution_id,
                            source_id,
                            target_url: &target.target_url,
                            error_code: adapter_error_code(&error),
                            http_status: adapter_error_status(&error),
                            retryable: adapter_error_is_retryable(&error),
                            terminal_attempt,
                            expected_work_generation,
                        },
                    )
                    .await;
            }
        };
        let (observation, response, artifacts, completeness) = result.into_parts();
        let observed_final_url = match observation.final_url {
            Some(value) => {
                let Ok(final_url) = value.parse() else {
                    return self
                        .terminal_failure(
                            &context,
                            &FailureContext {
                                kind: TerminalFailureKind::Provider,
                                run_id,
                                execution_id,
                                source_id,
                                target_url: &target.target_url,
                                error_code: CrawlExecutionErrorCode::InvalidResponse,
                                http_status: None,
                                retryable: false,
                                terminal_attempt: true,
                                expected_work_generation,
                            },
                        )
                        .await;
                };
                // Final URLs are only evidence after Task 4's policy accepts
                // them. The Source canonical identity never follows redirects.
                if self
                    .network_policy
                    .validate_and_resolve(&final_url)
                    .await
                    .is_err()
                {
                    return self
                        .terminal_failure(
                            &context,
                            &FailureContext {
                                kind: TerminalFailureKind::Provider,
                                run_id,
                                execution_id,
                                source_id,
                                target_url: &target.target_url,
                                error_code: CrawlExecutionErrorCode::InvalidResponse,
                                http_status: None,
                                retryable: false,
                                terminal_attempt: true,
                                expected_work_generation,
                            },
                        )
                        .await;
                }
                Some(final_url.to_string())
            }
            None => None,
        };
        let artifacts = self
            .persist_artifacts(
                &context,
                run_id,
                source_id,
                snapshot.created_at(),
                artifacts,
                snapshot.settings().retain_artifacts.value,
            )
            .await?;
        self.progress(&context, "EVIDENCE_SAVED", None).await?;
        let partial = matches!(completeness, CrawlerResultCompleteness::Partial { .. });
        let record = CrawlExecutionRecord {
            id: execution_id,
            crawl_run_id: run_id,
            requested_url: target.target_url.to_string(),
            canonical_url: target.target_url.to_string(),
            observed_final_url,
            source_id: Some(source_id),
            page_type_id: None,
            transition_id: None,
            discovered_url_id: None,
            outcome: if partial {
                CrawlExecutionOutcome::Partial
            } else {
                CrawlExecutionOutcome::Completed
            },
            error_code: partial.then_some(CrawlExecutionErrorCode::PartialResult),
            http_status: response.status_code(),
            media_type: response.media_type().map(|value| value.as_str().to_owned()),
            content_length_bytes: response.content_length_bytes(),
            provider_elapsed_ms: response.provider_elapsed_ms(),
            artifacts,
        };
        self.persist_record(&record, &context, Some(expected_work_generation))
            .await?;
        self.save_quick_checkpoint(&context, &snapshot, run_id, CrawlRecoveryPhase::Finalizing)
            .await?;
        self.finish_recovered_execution(&context, run_id, snapshot, CrawlRunStatus::Running)
            .await
    }

    #[allow(clippy::too_many_lines)]
    async fn finish_recovered_execution(
        &self,
        context: &JobExecutionContext,
        run_id: CrawlRunId,
        snapshot: erabi_domain::CrawlRunSnapshot,
        current_status: CrawlRunStatus,
    ) -> QuickScrapeResult<()> {
        let executions = CrawlExecutionRepository::new(&self.database)
            .list_for_run(run_id)
            .await
            .map_err(|_| {
                QuickScrapeError::repository(ExecutionOperation::LoadJob, "EXECUTIONS_LOAD_FAILED")
            })?;
        let discovered = CrawlRunRepository::new(&self.database)
            .discovered_urls(run_id)
            .await
            .map_err(|_| {
                QuickScrapeError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "DISCOVERED_URLS_LOAD_FAILED",
                )
            })?;
        let checkpoint = JobRepository::new(&self.database)
            .latest_checkpoint_for_lineage(context.job_id())
            .await
            .map_err(|error| {
                quick_checkpoint_load_error(ExecutionOperation::LoadCheckpoint, &error)
            })?;
        let durable = CrawlTraversalRepository::new(&self.database)
            .reconstruct_recovery_state(run_id)
            .await
            .map_err(|error| quick_traversal_error(ExecutionOperation::LoadCheckpoint, error))?;
        let authoritative_status = durable
            .work
            .iter()
            .find(|work| work.id == quick_url_state_id(run_id))
            .and_then(|work| work.current_work_state)
            .map_or(current_status, |work_state| match work_state {
                CrawlWorkState::Failed
                    if matches!(
                        current_status,
                        CrawlRunStatus::Queued | CrawlRunStatus::Running
                    ) =>
                {
                    CrawlRunStatus::Failed
                }
                CrawlWorkState::Cancelled
                    if matches!(
                        current_status,
                        CrawlRunStatus::Queued | CrawlRunStatus::Running
                    ) =>
                {
                    CrawlRunStatus::Cancelled
                }
                CrawlWorkState::Partial
                    if matches!(
                        current_status,
                        CrawlRunStatus::Queued | CrawlRunStatus::Running
                    ) =>
                {
                    CrawlRunStatus::PartialResult
                }
                _ => current_status,
            });
        let validated_recovery = checkpoint
            .as_ref()
            .map(|record| {
                validate_crawl_recovery(
                    Some(record),
                    &snapshot,
                    run_id,
                    authoritative_status,
                    &executions,
                    &discovered,
                    Some(&durable),
                )
                .map_err(|error| quick_recovery_error(ExecutionOperation::LoadCheckpoint, error))
            })
            .transpose()?;
        if let Some(record) = checkpoint.as_ref() {
            emit(SemanticEvent::CheckpointRecovered {
                context: crate::telemetry_crawl_context(context, Some(&run_id.to_string()), None),
                version: erabi_crawler::CRAWL_RECOVERY_FORMAT_VERSION,
                phase: erabi_observability::CheckpointPhase::Recovery,
                bytes: serde_json::to_vec(&record.checkpoint.payload)
                    .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
                work_generation: 0,
                outcome: EventOutcome::Reconstructed,
            });
        }
        emit(SemanticEvent::RecoveryReconstructed {
            context: crate::telemetry_crawl_context(context, Some(&run_id.to_string()), None),
            action: erabi_observability::RecoveryAction::Reconstructed,
            generation: 0,
            recovered_count: u64::try_from(durable.work.len()).unwrap_or(u64::MAX),
            outcome: EventOutcome::Reconstructed,
        });
        let facts = validated_recovery.map_or_else(
            || {
                erabi_crawler::reconstruct_crawl_structural_facts(
                    &snapshot,
                    authoritative_status,
                    &executions,
                    &discovered,
                    Some(&durable.control),
                    Some(&durable.work),
                )
                .map_err(|_| {
                    quick_recovery_error(
                        ExecutionOperation::FinalizeRun,
                        CrawlRecoveryValidationError::StateInvalid,
                    )
                })
            },
            |validated| Ok(validated.facts),
        )?;
        let summary = CrawlExecutionSummary {
            crawl_run_id: run_id,
            in_scope_pages_planned: facts.in_scope_pages_planned,
            in_scope_pages_completed: facts.in_scope_pages_completed,
            pagination_truncation_count: facts.pagination_truncation_count,
            unresolved_partial_work_count: facts.unresolved_partial_work_count,
            page_type_ambiguity_count: facts.page_type_ambiguity_count,
        };
        CrawlExecutionRepository::new(&self.database)
            .finalize(&summary, facts.status)
            .await
            .map_err(|_| {
                QuickScrapeError::new(
                    ExecutionDiagnostic::new(
                        OrchestrationErrorCategory::Finalization,
                        ExecutionOperation::FinalizeRun,
                        ExecutionAction::Retry,
                        "CRAWL_RUN_FINALIZATION_FAILED",
                    )
                    .with_run(run_id),
                )
            })?;
        context.mark_terminal_crawl_run(run_id, facts.status);
        if facts.status == CrawlRunStatus::Cancelled
            && let Err(error) = self
                .progress(context, "CANCELLATION_SAFE_BOUNDARY", None)
                .await
        {
            context.record_secondary_diagnostics(error.diagnostics);
        }
        if let Err(error) = self.progress(context, "FINALIZATION_COMPLETED", None).await {
            context.record_secondary_diagnostics(error.diagnostics);
        }
        let (key, terminal) = match facts.status {
            CrawlRunStatus::Failed => ("FAILED", ProgressTerminalState::Failed),
            CrawlRunStatus::Cancelled => ("CANCELLED", ProgressTerminalState::Cancelled),
            CrawlRunStatus::PartialResult => ("PARTIAL_RESULT", ProgressTerminalState::Succeeded),
            CrawlRunStatus::Succeeded => ("COMPLETED", ProgressTerminalState::Succeeded),
            CrawlRunStatus::Queued | CrawlRunStatus::Running => {
                return Err(QuickScrapeError::new(
                    ExecutionDiagnostic::new(
                        OrchestrationErrorCategory::Finalization,
                        ExecutionOperation::FinalizeRun,
                        ExecutionAction::Fail,
                        "CRAWL_RUN_NOT_TERMINAL",
                    )
                    .with_run(run_id),
                ));
            }
        };
        if let Err(error) = self.progress(context, key, Some(terminal)).await {
            // The CrawlRun has already committed its business outcome. A
            // terminal progress append is a repairable projection failure.
            context.record_secondary_diagnostics(error.diagnostics);
        }
        Ok(())
    }

    async fn save_quick_checkpoint(
        &self,
        context: &JobExecutionContext,
        snapshot: &erabi_domain::CrawlRunSnapshot,
        run_id: CrawlRunId,
        phase: CrawlRecoveryPhase,
    ) -> QuickScrapeResult<()> {
        let checkpoint = CrawlRecoveryCheckpoint::new(run_id, snapshot, phase).map_err(|_| {
            QuickScrapeError::checkpoint(
                ExecutionOperation::Serialization,
                "CHECKPOINT_BUILD_FAILED",
            )
        })?;
        let envelope = checkpoint.to_envelope().map_err(|_| {
            QuickScrapeError::checkpoint(
                ExecutionOperation::Serialization,
                "CHECKPOINT_ENVELOPE_FAILED",
            )
        })?;
        context.checkpoint(&envelope).await.map_err(|_| {
            QuickScrapeError::checkpoint(
                ExecutionOperation::LoadCheckpoint,
                "CHECKPOINT_PERSIST_FAILED",
            )
        })?;
        emit(SemanticEvent::CheckpointPersisted {
            context: crate::telemetry_crawl_context(context, Some(&run_id.to_string()), None),
            version: erabi_crawler::CRAWL_RECOVERY_FORMAT_VERSION,
            phase: crate::telemetry_checkpoint_phase(phase),
            bytes: serde_json::to_vec(&envelope.payload)
                .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
            work_generation: 0,
            outcome: EventOutcome::Durable,
        });
        self.progress(context, "CHECKPOINT_SAVED", None).await
    }

    async fn initialize_quick_work(
        &self,
        run_id: CrawlRunId,
        target_url: &str,
    ) -> QuickScrapeResult<()> {
        let state = CrawlUrlStateRecord {
            id: quick_url_state_id(run_id),
            crawl_run_id: run_id,
            canonical_url: target_url.to_owned(),
            first_discovered_url_id: None,
            requested_url: target_url.to_owned(),
            parent_url_state_id: None,
            parent_discovered_url_id: None,
            admission_state: CrawlAdmissionState::Admitted,
            preserve_reason: None,
            resolved_to_url_state_id: None,
            admission_sequence: Some(0),
            depth: Some(0),
            target_page_type_id: None,
            transition_id: None,
            pagination: false,
            final_canonical_url: None,
            current_work_state: Some(CrawlWorkState::Pending),
            work_generation: 0,
            current_execution_id: None,
            seed_provenance: Vec::new(),
            seen: true,
            sampled: false,
            expanded: false,
            in_scope: false,
            page_type_match_state: None,
        };
        let control = CrawlTraversalControl {
            crawl_run_id: run_id,
            consumed_bytes: 0,
            raw_link_count: 0,
            duplicate_count: 0,
            robots_excluded_count: 0,
            provider_error_count: 0,
            external_url_count: 0,
            blocked_url_count: 0,
            peak_expansion_count: 0,
            elapsed_millis: 0,
            time_budget_hit: false,
            duration_work_not_expanded: false,
            pagination_truncation_count: 0,
            next_admission_sequence: 1,
        };
        CrawlTraversalRepository::new(&self.database)
            .initialize_run_state(run_id, &[state], &control)
            .await
            .map_err(|_| {
                QuickScrapeError::repository(
                    ExecutionOperation::QueueLifecycle,
                    "QUICK_WORK_INITIALIZATION_FAILED",
                )
            })
    }

    async fn persist_record(
        &self,
        record: &CrawlExecutionRecord,
        context: &JobExecutionContext,
        expected_work_generation: Option<u64>,
    ) -> QuickScrapeResult<()> {
        let state = match record.outcome {
            CrawlExecutionOutcome::Completed => CrawlWorkState::Completed,
            CrawlExecutionOutcome::Partial => CrawlWorkState::Partial,
            CrawlExecutionOutcome::Failed => CrawlWorkState::Failed,
            CrawlExecutionOutcome::Cancelled => CrawlWorkState::Cancelled,
        };
        let executions = CrawlExecutionRepository::new(&self.database);
        let expected_work_generation = match expected_work_generation {
            Some(generation) => generation,
            None => CrawlTraversalRepository::new(&self.database)
                .read_work_generation(
                    record.crawl_run_id,
                    &quick_url_state_id(record.crawl_run_id),
                )
                .await
                .map_err(|_| {
                    QuickScrapeError::repository(
                        ExecutionOperation::PersistExecution,
                        "WORK_GENERATION_LOAD_FAILED",
                    )
                })?,
        };
        executions
            .persist_current_work(
                record,
                &quick_url_state_id(record.crawl_run_id),
                context.job_id(),
                context.attempt_id(),
                state,
                expected_work_generation,
                context.ownership_now(),
            )
            .await
            .or_else(duplicate_execution_is_ok)
            .map_err(|_| {
                QuickScrapeError::repository(
                    ExecutionOperation::PersistExecution,
                    "EXECUTION_PERSIST_FAILED",
                )
            })
    }

    #[allow(clippy::too_many_lines)]
    async fn terminal_failure(
        &self,
        context: &JobExecutionContext,
        failure: &FailureContext<'_>,
    ) -> QuickScrapeResult<()> {
        if context.storage_pressure().is_signalled() {
            return Ok(());
        }
        let (category, operation, provider) = match failure.kind {
            TerminalFailureKind::Provider => (
                OrchestrationErrorCategory::Provider,
                ExecutionOperation::ProviderExecution,
                Some("crawler-adapter"),
            ),
            TerminalFailureKind::NetworkAdmission => (
                OrchestrationErrorCategory::NetworkAdmission,
                ExecutionOperation::AcquireAdmission,
                None,
            ),
            TerminalFailureKind::Pacing => (
                OrchestrationErrorCategory::Pacing,
                ExecutionOperation::AcquireAdmission,
                None,
            ),
        };
        let primary = ExecutionDiagnostic::new(
            category,
            operation,
            if failure.retryable {
                ExecutionAction::Retry
            } else {
                ExecutionAction::Fail
            },
            crawl_execution_code_name(failure.error_code),
        )
        .with_run(failure.run_id)
        .with_execution(failure.execution_id)
        .with_work_generation(failure.expected_work_generation);
        let primary = provider.map_or(primary.clone(), |provider| primary.with_provider(provider));
        // Establish the provider/business failure before any retry progress,
        // checkpoint, or finalization projection can fail secondarily.
        context.record_primary_diagnostic(primary.clone());
        if context.cancellation().is_cancelled() {
            let snapshot = CrawlRunRepository::new(&self.database)
                .snapshot(failure.run_id)
                .await
                .map_err(|_| {
                    QuickScrapeError::repository(
                        ExecutionOperation::LoadRunSnapshot,
                        "RUN_SNAPSHOT_LOAD_FAILED",
                    )
                })?;
            self.persist_record(
                &CrawlExecutionRecord {
                    id: failure.execution_id,
                    crawl_run_id: failure.run_id,
                    requested_url: failure.target_url.to_string(),
                    canonical_url: failure.target_url.to_string(),
                    observed_final_url: None,
                    source_id: Some(failure.source_id),
                    page_type_id: None,
                    transition_id: None,
                    discovered_url_id: None,
                    outcome: CrawlExecutionOutcome::Cancelled,
                    error_code: Some(CrawlExecutionErrorCode::Cancelled),
                    http_status: failure.http_status,
                    media_type: None,
                    content_length_bytes: None,
                    provider_elapsed_ms: None,
                    artifacts: Vec::new(),
                },
                context,
                Some(failure.expected_work_generation),
            )
            .await?;
            self.save_quick_checkpoint(
                context,
                &snapshot,
                failure.run_id,
                CrawlRecoveryPhase::Finalizing,
            )
            .await?;
            return self
                .finish_recovered_execution(
                    context,
                    failure.run_id,
                    snapshot,
                    CrawlRunStatus::Cancelled,
                )
                .await;
        }
        let record = CrawlExecutionRecord {
            id: failure.execution_id,
            crawl_run_id: failure.run_id,
            requested_url: failure.target_url.to_string(),
            canonical_url: failure.target_url.to_string(),
            observed_final_url: None,
            source_id: Some(failure.source_id),
            page_type_id: None,
            transition_id: None,
            discovered_url_id: None,
            outcome: CrawlExecutionOutcome::Failed,
            error_code: Some(failure.error_code),
            http_status: failure.http_status,
            media_type: None,
            content_length_bytes: None,
            provider_elapsed_ms: None,
            artifacts: Vec::new(),
        };
        self.persist_record(&record, context, Some(failure.expected_work_generation))
            .await?;
        let snapshot = CrawlRunRepository::new(&self.database)
            .snapshot(failure.run_id)
            .await
            .map_err(|_| {
                QuickScrapeError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "RUN_SNAPSHOT_LOAD_FAILED",
                )
            })?;
        self.save_quick_checkpoint(
            context,
            &snapshot,
            failure.run_id,
            CrawlRecoveryPhase::Traversing,
        )
        .await?;
        if failure.retryable && !failure.terminal_attempt {
            self.progress(context, "RETRY_SCHEDULED", None).await?;
            return Err(QuickScrapeError::new(primary));
        }
        self.finish_recovered_execution(context, failure.run_id, snapshot, CrawlRunStatus::Failed)
            .await?;
        if !failure.retryable || failure.terminal_attempt {
            context.mark_terminal_failure();
        }
        Err(QuickScrapeError::new(primary))
    }

    #[allow(dead_code)]
    async fn persist_success(
        &self,
        run_id: CrawlRunId,
        record: &CrawlExecutionRecord,
        partial: bool,
    ) -> QuickScrapeResult<()> {
        CrawlExecutionRepository::new(&self.database)
            .persist(record)
            .await
            .or_else(duplicate_execution_is_ok)
            .map_err(|_| {
                QuickScrapeError::repository(
                    ExecutionOperation::PersistExecution,
                    "EXECUTION_PERSIST_FAILED",
                )
            })?;
        CrawlExecutionRepository::new(&self.database)
            .save_summary(&CrawlExecutionSummary {
                crawl_run_id: run_id,
                in_scope_pages_planned: 1,
                in_scope_pages_completed: 1,
                pagination_truncation_count: 0,
                unresolved_partial_work_count: u64::from(partial),
                page_type_ambiguity_count: 0,
            })
            .await
            .map_err(|_| {
                QuickScrapeError::repository(
                    ExecutionOperation::PersistExecution,
                    "EXECUTION_SUMMARY_PERSIST_FAILED",
                )
            })?;
        CrawlRunRepository::new(&self.database)
            .transition_execution_status(
                run_id,
                if partial {
                    CrawlRunStatus::PartialResult
                } else {
                    CrawlRunStatus::Succeeded
                },
            )
            .await
            .map_err(|_| {
                QuickScrapeError::repository(
                    ExecutionOperation::TransitionRun,
                    "RUN_TRANSITION_FAILED",
                )
            })
    }

    async fn persist_artifacts(
        &self,
        context: &JobExecutionContext,
        run_id: CrawlRunId,
        source_id: SourceId,
        created_at: &str,
        artifacts: Vec<CrawlerArtifactEvidence>,
        retain: bool,
    ) -> QuickScrapeResult<Vec<CrawlExecutionArtifact>> {
        if !retain {
            return Ok(Vec::new());
        }
        let mut persisted = Vec::new();
        for artifact in artifacts {
            let (kind, file_name, media_type, bytes) = artifact_bytes(&artifact);
            let stored = self
                .artifact_store
                .write_bytes(format!("quick-scrape/{run_id}"), file_name, bytes)
                .map_err(|_| {
                    QuickScrapeError::artifact(
                        ExecutionOperation::PersistArtifact,
                        "ARTIFACT_WRITE_FAILED",
                    )
                })?;
            ArtifactRepository::new(&self.database)
                .record(
                    &stored,
                    Some(run_id),
                    Some(source_id),
                    media_type,
                    created_at,
                    &serde_json::json!({"kind": artifact_kind_name(kind)}),
                )
                .await
                .map_err(|_| {
                    QuickScrapeError::artifact(
                        ExecutionOperation::PersistArtifact,
                        "ARTIFACT_RECORD_FAILED",
                    )
                })?;
            emit(SemanticEvent::ArtifactPersisted {
                context: crate::telemetry_crawl_context(context, Some(&run_id.to_string()), None),
                kind: telemetry_artifact_kind(kind),
                count: 1,
                bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                outcome: EventOutcome::Success,
            });
            persisted.push(CrawlExecutionArtifact {
                artifact_id: stored.id,
                kind: execution_artifact_kind(kind),
            });
        }
        Ok(persisted)
    }

    async fn progress(
        &self,
        context: &JobExecutionContext,
        key: &str,
        terminal: Option<ProgressTerminalState>,
    ) -> QuickScrapeResult<()> {
        let attempt = ProgressAttemptId::new(context.attempt_id().to_owned()).map_err(|_| {
            QuickScrapeError::progress(
                ExecutionOperation::Serialization,
                "PROGRESS_ATTEMPT_INVALID",
            )
        })?;
        let terminal_event = terminal.is_some();
        let metadata = ProgressMetadata::default();
        let event = match terminal {
            Some(terminal) => {
                NewProgressEvent::terminal(context.job_id().clone(), terminal, metadata).map_err(
                    |_| {
                        QuickScrapeError::progress(
                            ExecutionOperation::Serialization,
                            "PROGRESS_EVENT_INVALID",
                        )
                    },
                )?
            }
            None => NewProgressEvent::new(
                context.job_id().clone(),
                ProgressKey::new(key).map_err(|_| {
                    QuickScrapeError::progress(
                        ExecutionOperation::Serialization,
                        "PROGRESS_KEY_INVALID",
                    )
                })?,
                metadata,
            ),
        }
        .with_attempt(attempt);
        if terminal_event && self.fail_terminal_progress_append_for_test {
            return Err(QuickScrapeError::progress(
                ExecutionOperation::AppendProgress,
                "PROGRESS_DURABLE_APPEND_FAILED",
            ));
        }
        let service = ProgressService::new(&self.database);
        let now = epoch_seconds();
        match &self.progress_live_hub {
            Some(hub) => match service.append_and_publish_at(hub, &event, now).await {
                Ok(ProgressPublication::Published(_)) => {
                    if terminal_event {
                        context.mark_terminal_progress_durable();
                        if let Some(terminal) = terminal {
                            emit(SemanticEvent::ProgressTerminalPublished {
                                context: crate::telemetry_job_context(context),
                                status: crate::telemetry_progress_status(terminal),
                            });
                        }
                    }
                    Ok(())
                }
                Ok(ProgressPublication::DurableOnly { .. }) => {
                    if terminal_event {
                        context.mark_terminal_progress_durable();
                        if let Some(terminal) = terminal {
                            emit(SemanticEvent::ProgressTerminalDurableOnly {
                                context: crate::telemetry_job_context(context),
                                status: crate::telemetry_progress_status(terminal),
                            });
                        }
                    }
                    context.record_secondary_diagnostic(ExecutionDiagnostic::new(
                        OrchestrationErrorCategory::ProgressPublication,
                        ExecutionOperation::PublishProgress,
                        ExecutionAction::Publish,
                        "PROGRESS_LIVE_PUBLICATION_FAILED",
                    ));
                    Ok(())
                }
                Err(_) => Err(QuickScrapeError::progress(
                    ExecutionOperation::AppendProgress,
                    "PROGRESS_DURABLE_APPEND_FAILED",
                )),
            },
            None => service
                .append_at(&event, now)
                .await
                .map(|_| {
                    if terminal_event {
                        context.mark_terminal_progress_durable();
                    }
                })
                .map_err(|_| {
                    QuickScrapeError::progress(
                        ExecutionOperation::AppendProgress,
                        "PROGRESS_DURABLE_APPEND_FAILED",
                    )
                }),
        }
    }
}

fn telemetry_artifact_kind(kind: CrawlerArtifactKind) -> ArtifactKind {
    match kind {
        CrawlerArtifactKind::RawHtml
        | CrawlerArtifactKind::CleanedHtml
        | CrawlerArtifactKind::RenderedHtml => ArtifactKind::Html,
        CrawlerArtifactKind::Screenshot => ArtifactKind::Screenshot,
        CrawlerArtifactKind::Markdown => ArtifactKind::Other,
    }
}

struct FailureContext<'url> {
    kind: TerminalFailureKind,
    run_id: CrawlRunId,
    execution_id: CrawlExecutionId,
    source_id: SourceId,
    target_url: &'url url::Url,
    error_code: CrawlExecutionErrorCode,
    http_status: Option<u16>,
    retryable: bool,
    terminal_attempt: bool,
    expected_work_generation: u64,
}

#[derive(Clone, Copy)]
enum TerminalFailureKind {
    Provider,
    NetworkAdmission,
    Pacing,
}

impl JobHandler for QuickScrapeJobHandler {
    fn execute(
        &self,
        context: JobExecutionContext,
    ) -> impl Future<Output = Result<(), JobExecutionError>> + Send {
        let handler = self.clone();
        // The handler owns provider/artifact response values and now the
        // compact durable-work transaction. Keep that state off Tokio's
        // default worker stack; this is an execution-boundary allocation, not
        // a change to retry or recovery semantics.
        Box::pin(async move {
            let telemetry_context = crate::telemetry_job_context(&context);
            let span = CrawlExecutionSpan::new(&telemetry_context);
            match span
                .run(Box::pin(handler.execute_inner(context.clone())))
                .await
            {
                Ok(()) => Ok(()),
                Err(error) => {
                    context.record_diagnostics(error.diagnostics);
                    Err(JobExecutionError)
                }
            }
        })
    }
}

fn parse_run_id(value: &str) -> Result<CrawlRunId, ()> {
    Uuid::parse_str(value)
        .ok()
        .and_then(CrawlRunId::from_uuid)
        .ok_or(())
}

fn parse_source_id(value: &str) -> Result<SourceId, ()> {
    Uuid::parse_str(value)
        .ok()
        .and_then(SourceId::from_uuid)
        .ok_or(())
}

fn execution_id_for_job(value: &str) -> Result<CrawlExecutionId, ()> {
    Uuid::parse_str(value)
        .ok()
        .and_then(CrawlExecutionId::from_uuid)
        .ok_or(())
}

fn quick_url_state_id(run_id: CrawlRunId) -> String {
    format!("quick:{run_id}")
}

fn adapter_error_code(error: &CrawlerAdapterError) -> CrawlExecutionErrorCode {
    match error {
        CrawlerAdapterError::Unavailable => CrawlExecutionErrorCode::ProviderUnavailable,
        CrawlerAdapterError::Timeout => CrawlExecutionErrorCode::Timeout,
        CrawlerAdapterError::AccessDenied => CrawlExecutionErrorCode::AccessDenied,
        CrawlerAdapterError::NotFound => CrawlExecutionErrorCode::NotFound,
        CrawlerAdapterError::RateLimited { .. } => CrawlExecutionErrorCode::RateLimited,
        CrawlerAdapterError::RemoteFailure { .. } => CrawlExecutionErrorCode::RemoteFailure,
        CrawlerAdapterError::UnsupportedCapability => {
            CrawlExecutionErrorCode::UnsupportedCapability
        }
        CrawlerAdapterError::InvalidProviderResponse => CrawlExecutionErrorCode::InvalidResponse,
        CrawlerAdapterError::Cancelled => CrawlExecutionErrorCode::Cancelled,
    }
}

fn crawl_execution_code_name(code: CrawlExecutionErrorCode) -> &'static str {
    match code {
        CrawlExecutionErrorCode::AccessDenied => "ACCESS_DENIED",
        CrawlExecutionErrorCode::NotFound => "NOT_FOUND",
        CrawlExecutionErrorCode::Timeout => "TIMEOUT",
        CrawlExecutionErrorCode::ProviderUnavailable => "PROVIDER_UNAVAILABLE",
        CrawlExecutionErrorCode::InvalidResponse => "INVALID_RESPONSE",
        CrawlExecutionErrorCode::RateLimited => "RATE_LIMITED",
        CrawlExecutionErrorCode::RemoteFailure => "REMOTE_FAILURE",
        CrawlExecutionErrorCode::UnsupportedCapability => "UNSUPPORTED_CAPABILITY",
        CrawlExecutionErrorCode::PartialResult => "PARTIAL_RESULT",
        CrawlExecutionErrorCode::Cancelled => "CANCELLED",
        CrawlExecutionErrorCode::RobotsExcluded => "ROBOTS_EXCLUDED",
        CrawlExecutionErrorCode::PageTypeAmbiguous => "PAGE_TYPE_AMBIGUOUS",
        CrawlExecutionErrorCode::StoragePressure => "STORAGE_PRESSURE",
    }
}

fn adapter_error_status(error: &CrawlerAdapterError) -> Option<u16> {
    match error {
        CrawlerAdapterError::RemoteFailure { status_code } => *status_code,
        _ => None,
    }
}

fn adapter_error_is_retryable(error: &CrawlerAdapterError) -> bool {
    match error {
        CrawlerAdapterError::Unavailable
        | CrawlerAdapterError::Timeout
        | CrawlerAdapterError::RateLimited { .. } => true,
        CrawlerAdapterError::RemoteFailure { status_code } => {
            status_code.is_none_or(|status| (500..=599).contains(&status))
        }
        CrawlerAdapterError::AccessDenied
        | CrawlerAdapterError::NotFound
        | CrawlerAdapterError::UnsupportedCapability
        | CrawlerAdapterError::InvalidProviderResponse
        | CrawlerAdapterError::Cancelled => false,
    }
}

fn robots_failure_is_retryable(error: &RobotsPolicyError) -> bool {
    matches!(
        error,
        RobotsPolicyError::Unavailable(_) | RobotsPolicyError::UnavailableWithPacing { .. }
    )
}

fn robots_pacing_diagnostic(error: &RobotsPolicyError) -> Option<ExecutionDiagnostic> {
    matches!(error, RobotsPolicyError::UnavailableWithPacing { .. }).then(|| {
        ExecutionDiagnostic::new(
            OrchestrationErrorCategory::Pacing,
            ExecutionOperation::RecordOutcome,
            ExecutionAction::Continue,
            "ROBOTS_PACING_OUTCOME_RECORD_FAILED",
        )
    })
}

fn quick_scrape_terminal_attempt(context: &JobExecutionContext, job: &crate::JobRecord) -> bool {
    job.current_attempt >= job.max_attempts
        || (context.kind().as_str() == "QUICK_SCRAPE"
            && job.current_attempt >= job.max_attempts.min(QUICK_SCRAPE_AUTOMATIC_MAX_ATTEMPTS))
}

fn pacing_failure_is_retryable(error: AdmissionError) -> bool {
    matches!(error, AdmissionError::OriginCapacityExhausted)
}

fn duplicate_execution_is_ok(
    error: CrawlExecutionRepositoryError,
) -> Result<(), CrawlExecutionRepositoryError> {
    match error {
        CrawlExecutionRepositoryError::DuplicateExecution => Ok(()),
        other => Err(other),
    }
}

fn quick_checkpoint_load_error(
    operation: ExecutionOperation,
    error: &erabi_db::repositories::JobRepositoryError,
) -> QuickScrapeError {
    QuickScrapeError::checkpoint(
        operation,
        checkpoint_error_code(error).unwrap_or("CHECKPOINT_LOAD_FAILED"),
    )
}

fn quick_traversal_error(
    operation: ExecutionOperation,
    error: CrawlTraversalRepositoryError,
) -> QuickScrapeError {
    match error {
        CrawlTraversalRepositoryError::CrawlRunNotFound
        | CrawlTraversalRepositoryError::InvalidState
        | CrawlTraversalRepositoryError::CorruptState => {
            quick_recovery_error(operation, CrawlRecoveryValidationError::StateInvalid)
        }
        CrawlTraversalRepositoryError::Checkpoint(error) => {
            if let Some(mapped) = map_checkpoint_repository_error(&error) {
                quick_recovery_error(operation, mapped)
            } else {
                QuickScrapeError::checkpoint(operation, "CHECKPOINT_LOAD_FAILED")
            }
        }
        CrawlTraversalRepositoryError::Database(_) => {
            QuickScrapeError::repository(operation, "TRAVERSAL_STATE_LOAD_FAILED")
        }
        CrawlTraversalRepositoryError::Discovery(_) => {
            QuickScrapeError::repository(operation, "DISCOVERY_LOAD_FAILED")
        }
    }
}

fn quick_recovery_error(
    operation: ExecutionOperation,
    error: CrawlRecoveryValidationError,
) -> QuickScrapeError {
    QuickScrapeError::checkpoint(operation, error.diagnostic_code())
}

fn direct_file_media_type(snapshot: &erabi_domain::CrawlRunSnapshot) -> Option<String> {
    let erabi_domain::RunConfiguration::QuickScrape {
        ad_hoc_configuration,
        ..
    } = snapshot.configuration()
    else {
        return None;
    };
    ad_hoc_configuration
        .get("source_intake_classification")
        .and_then(|value| value.get("media_type"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
}

fn artifact_bytes(
    artifact: &CrawlerArtifactEvidence,
) -> (CrawlerArtifactKind, &'static str, Option<&str>, &[u8]) {
    match artifact {
        CrawlerArtifactEvidence::RawHtml(value) => (
            CrawlerArtifactKind::RawHtml,
            "raw.html",
            Some("text/html"),
            value.as_bytes(),
        ),
        CrawlerArtifactEvidence::CleanedHtml(value) => (
            CrawlerArtifactKind::CleanedHtml,
            "cleaned.html",
            Some("text/html"),
            value.as_bytes(),
        ),
        CrawlerArtifactEvidence::RenderedHtml(value) => (
            CrawlerArtifactKind::RenderedHtml,
            "rendered.html",
            Some("text/html"),
            value.as_bytes(),
        ),
        CrawlerArtifactEvidence::Markdown(value) => (
            CrawlerArtifactKind::Markdown,
            "page.md",
            Some("text/markdown"),
            value.as_bytes(),
        ),
        CrawlerArtifactEvidence::Screenshot { media_type, bytes } => (
            CrawlerArtifactKind::Screenshot,
            "screenshot.bin",
            Some(media_type.as_str()),
            bytes,
        ),
    }
}

fn execution_artifact_kind(value: CrawlerArtifactKind) -> CrawlExecutionArtifactKind {
    match value {
        CrawlerArtifactKind::RawHtml => CrawlExecutionArtifactKind::RawHtml,
        CrawlerArtifactKind::CleanedHtml => CrawlExecutionArtifactKind::CleanedHtml,
        CrawlerArtifactKind::RenderedHtml => CrawlExecutionArtifactKind::RenderedHtml,
        CrawlerArtifactKind::Markdown => CrawlExecutionArtifactKind::Markdown,
        CrawlerArtifactKind::Screenshot => CrawlExecutionArtifactKind::Screenshot,
    }
}

fn artifact_kind_name(value: CrawlerArtifactKind) -> &'static str {
    match value {
        CrawlerArtifactKind::RawHtml => "RAW_HTML",
        CrawlerArtifactKind::CleanedHtml => "CLEANED_HTML",
        CrawlerArtifactKind::RenderedHtml => "RENDERED_HTML",
        CrawlerArtifactKind::Markdown => "MARKDOWN",
        CrawlerArtifactKind::Screenshot => "SCREENSHOT",
    }
}

fn epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn robots_pacing_accounting_failure_remains_secondary_to_admission() {
        let error = RobotsPolicyError::UnavailableWithPacing {
            failure: erabi_crawler::RobotsUnavailable::ServerFailure,
            pacing: AdmissionError::ClockOverflow,
        };
        let Some(pacing) = robots_pacing_diagnostic(&error) else {
            panic!("pacing diagnostic was not produced")
        };
        let mut diagnostics = ExecutionDiagnostics::new();
        diagnostics.add_secondary(pacing);
        diagnostics.add_primary(ExecutionDiagnostic::new(
            OrchestrationErrorCategory::NetworkAdmission,
            ExecutionOperation::AcquireAdmission,
            ExecutionAction::Retry,
            "ROBOTS_POLICY_FAILED",
        ));

        assert_eq!(
            diagnostics
                .primary
                .as_ref()
                .map(|diagnostic| diagnostic.category),
            Some(OrchestrationErrorCategory::NetworkAdmission)
        );
        assert_eq!(
            diagnostics
                .secondary
                .first()
                .map(|diagnostic| diagnostic.category),
            Some(OrchestrationErrorCategory::Pacing)
        );
        assert_eq!(
            diagnostics
                .secondary
                .first()
                .map(|diagnostic| diagnostic.operation),
            Some(ExecutionOperation::RecordOutcome)
        );
    }
}
