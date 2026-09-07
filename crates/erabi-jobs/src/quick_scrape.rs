//! Durable execution handler for provider-neutral one-page Quick Scrapes.

use std::{
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use erabi_crawler::{
    AdmissionError, CrawlCheckpointV2, CrawlRecoveryPhase, CrawlerAdapter, CrawlerAdapterError,
    CrawlerArtifactEvidence, CrawlerArtifactKind, CrawlerEvidencePolicy, CrawlerExecuteRequest,
    CrawlerResultCompleteness, NetworkTargetPolicy, OriginKey, PacingCancellation, PacingOutcome,
    PacingService, RenderingRequirement, RobotsAdmissionDecision, RobotsPolicyError,
    RobotsPolicyService, ScreenshotPolicy, quick_scrape_snapshot_target,
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
use uuid::Uuid;

use crate::{
    JobExecutionContext, JobExecutionError, JobHandler, NewProgressEvent, ProgressAttemptId,
    ProgressKey, ProgressLiveHub, ProgressMetadata, ProgressService, ProgressTerminalState,
};

/// A Quick Scrape root performs its initial attempt and one automatic retry.
/// Further configured attempt budget remains available to an explicit durable
/// Retry child, preserving the operator-visible generation boundary.
const QUICK_SCRAPE_AUTOMATIC_MAX_ATTEMPTS: u32 = 2;

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

    // This is the intentionally linear durable attempt lifecycle. Keeping the
    // admissions, provider call, and durable completion in one visible order
    // makes RAII release and crash boundaries auditable.
    #[allow(clippy::too_many_lines)]
    async fn execute_inner(&self, context: JobExecutionContext) -> Result<(), ()> {
        if !matches!(
            context.kind().as_str(),
            "QUICK_SCRAPE"
                | "RETRY"
                | "RETRY_FAILED_PARTS"
                | "RESUME_CHECKPOINT"
                | "RERUN_FULL_CRAWL"
                | "RESTART_FROM_BEGINNING"
        ) {
            return Err(());
        }
        let job = JobRepository::new(&self.database)
            .job(context.job_id())
            .await
            .map_err(|_| ())?;
        let terminal_attempt = quick_scrape_terminal_attempt(&context, &job);
        let stored_run_id = job.crawl_run_id.as_deref().ok_or(())?;
        let run_id = parse_run_id(stored_run_id)?;
        let snapshot = CrawlRunRepository::new(&self.database)
            .snapshot(run_id)
            .await
            .map_err(|_| ())?;
        let target = quick_scrape_snapshot_target(&snapshot).map_err(|_| ())?;
        let source_id = parse_source_id(&target.source_id)?;
        let source = SourceRepository::new(&self.database)
            .read(source_id)
            .await
            .map_err(|_| ())?;
        if source.canonical_url != target.target_url {
            return Err(());
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
                .map_err(|_| ())?
        };
        if let Some(record) = latest_checkpoint.as_ref() {
            CrawlCheckpointV2::from_envelope(&record.checkpoint, &snapshot, run_id)
                .map_err(|_| ())?;
        }
        let recovery_kind = match context.kind().as_str() {
            "RETRY" => Some(CrawlRecoveryActionKind::Retry),
            "RETRY_FAILED_PARTS" => Some(CrawlRecoveryActionKind::RetryFailedParts),
            "RESTART_FROM_BEGINNING" => Some(CrawlRecoveryActionKind::RestartFromBeginning),
            _ => None,
        };
        if let Some(kind) = recovery_kind {
            // Persist a compact action marker before mutating logical
            // generation state. This keeps an action child replayable if the
            // process dies between action preparation and its next ordinary
            // checkpoint. A fresh Restart action may have no prior checkpoint;
            // its marker still remains bounded and is followed by the normal
            // initialization checkpoint when the durable state is created.
            let action_checkpoint = latest_checkpoint
                .as_ref()
                .map(|record| record.checkpoint.clone())
                .unwrap_or(
                    CrawlCheckpointV2::new(run_id, &snapshot, CrawlRecoveryPhase::Traversing)
                        .map_err(|_| ())?
                        .to_envelope()
                        .map_err(|_| ())?,
                );
            context
                .checkpoint(&action_checkpoint)
                .await
                .map_err(|_| ())?;
            CrawlTraversalRepository::new(&self.database)
                .prepare_recovery_action(
                    context.job_id(),
                    context.attempt_id(),
                    run_id,
                    kind,
                    context.ownership_now(),
                )
                .await
                .map_err(|_| ())?;
        }
        let durable_state = match CrawlTraversalRepository::new(&self.database)
            .reconstruct_recovery_state(run_id)
            .await
        {
            Ok(state) => Some(state),
            Err(CrawlTraversalRepositoryError::CrawlRunNotFound) => None,
            Err(_) => return Err(()),
        };
        let current_work_completed = durable_state.as_ref().is_some_and(|state| {
            state.work.iter().any(|work| {
                work.id == quick_url_state_id(run_id)
                    && work.admission_state == CrawlAdmissionState::Admitted
                    && work.current_work_state == Some(CrawlWorkState::Completed)
            })
        });
        if current_work_completed {
            return self
                .finish_recovered_execution(&context, run_id, snapshot, CrawlRunStatus::Running)
                .await;
        }
        let mut execution_id = execution_id_for_job(context.job_id().as_str())?;
        let executions = CrawlExecutionRepository::new(&self.database);
        match executions.read(execution_id).await {
            Ok(existing) => {
                if existing.outcome == CrawlExecutionOutcome::Completed {
                    return Err(());
                }
                execution_id = CrawlExecutionId::new();
            }
            Err(CrawlExecutionRepositoryError::NotFound) => {}
            Err(_) => return Err(()),
        }
        let run_repository = CrawlRunRepository::new(&self.database);
        if let Some(CrawlRecoveryActionKind::RestartFromBeginning) = recovery_kind {
            run_repository
                .transition_restart_status(run_id)
                .await
                .map_err(|_| ())?;
        } else if recovery_kind.is_some() {
            run_repository
                .transition_recovery_status(run_id)
                .await
                .map_err(|_| ())?;
        } else {
            run_repository
                .transition_execution_status(run_id, CrawlRunStatus::Running)
                .await
                .map_err(|_| ())?;
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
            .map_err(|_| ())?;
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
            .map_err(|_| ())?;

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

        let origin = OriginKey::from_url(&target.target_url).map_err(|_| ())?;
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
                return self
                    .terminal_failure(
                        &context,
                        &FailureContext {
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
        .map_err(|_| ())?;
        let provider = tokio::select! {
            value = self.adapter.execute(request) => value,
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
        let result = match provider {
            Ok(result) => {
                permit
                    .record_outcome(PacingOutcome::Success)
                    .map_err(|_| ())?;
                result
            }
            Err(error) => {
                let _ = permit.record_outcome(PacingOutcome::from_adapter_error(&error));
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

    async fn finish_recovered_execution(
        &self,
        context: &JobExecutionContext,
        run_id: CrawlRunId,
        snapshot: erabi_domain::CrawlRunSnapshot,
        current_status: CrawlRunStatus,
    ) -> Result<(), ()> {
        let executions = CrawlExecutionRepository::new(&self.database)
            .list_for_run(run_id)
            .await
            .map_err(|_| ())?;
        let discovered = CrawlRunRepository::new(&self.database)
            .discovered_urls(run_id)
            .await
            .map_err(|_| ())?;
        let checkpoint = JobRepository::new(&self.database)
            .latest_checkpoint_for_lineage(context.job_id())
            .await
            .map_err(|_| ())?;
        let _checkpoint = checkpoint
            .as_ref()
            .map(|record| CrawlCheckpointV2::from_envelope(&record.checkpoint, &snapshot, run_id))
            .transpose()
            .map_err(|_| ())?;
        let durable = CrawlTraversalRepository::new(&self.database)
            .reconstruct_recovery_state(run_id)
            .await
            .map_err(|_| ())?;
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
        let finalization = erabi_crawler::finalize_durable_state_with_traversal(
            &snapshot,
            authoritative_status,
            &executions,
            &discovered,
            None,
            Some(&durable.control),
            Some(&durable.work),
        )
        .map_err(|_| ())?;
        let summary = CrawlExecutionSummary {
            crawl_run_id: run_id,
            in_scope_pages_planned: finalization.structural_input.in_scope_pages_planned,
            in_scope_pages_completed: finalization.structural_input.in_scope_pages_completed,
            pagination_truncation_count: finalization.structural_input.pagination_truncation_count,
            unresolved_partial_work_count: finalization
                .structural_input
                .unresolved_partial_work_count,
            page_type_ambiguity_count: finalization.structural_input.page_type_ambiguity_count,
        };
        CrawlExecutionRepository::new(&self.database)
            .finalize(&summary, finalization.status)
            .await
            .map_err(|_| ())?;
        if finalization.status == CrawlRunStatus::Cancelled {
            let _ = self
                .progress(context, "CANCELLATION_SAFE_BOUNDARY", None)
                .await;
        }
        let _ = self.progress(context, "FINALIZATION_COMPLETED", None).await;
        let (key, terminal) = match finalization.status {
            CrawlRunStatus::Failed => ("FAILED", ProgressTerminalState::Failed),
            CrawlRunStatus::Cancelled => ("CANCELLED", ProgressTerminalState::Cancelled),
            CrawlRunStatus::PartialResult => ("PARTIAL_RESULT", ProgressTerminalState::Succeeded),
            CrawlRunStatus::Succeeded => ("COMPLETED", ProgressTerminalState::Succeeded),
            CrawlRunStatus::Queued | CrawlRunStatus::Running => return Err(()),
        };
        let _ = self.progress(context, key, Some(terminal)).await;
        Ok(())
    }

    async fn save_quick_checkpoint(
        &self,
        context: &JobExecutionContext,
        snapshot: &erabi_domain::CrawlRunSnapshot,
        run_id: CrawlRunId,
        phase: CrawlRecoveryPhase,
    ) -> Result<(), ()> {
        let checkpoint = CrawlCheckpointV2::new(run_id, snapshot, phase).map_err(|_| ())?;
        context
            .checkpoint(&checkpoint.to_envelope().map_err(|_| ())?)
            .await
            .map_err(|_| ())?;
        self.progress(context, "CHECKPOINT_SAVED", None).await
    }

    async fn initialize_quick_work(&self, run_id: CrawlRunId, target_url: &str) -> Result<(), ()> {
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
            .map_err(|_| ())
    }

    async fn persist_record(
        &self,
        record: &CrawlExecutionRecord,
        context: &JobExecutionContext,
        expected_work_generation: Option<u64>,
    ) -> Result<(), ()> {
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
                .map_err(|_| ())?,
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
            .map_err(|_| ())
    }

    async fn terminal_failure(
        &self,
        context: &JobExecutionContext,
        failure: &FailureContext<'_>,
    ) -> Result<(), ()> {
        if context.storage_pressure().is_signalled() {
            return Ok(());
        }
        if context.cancellation().is_cancelled() {
            let snapshot = CrawlRunRepository::new(&self.database)
                .snapshot(failure.run_id)
                .await
                .map_err(|_| ())?;
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
            .map_err(|_| ())?;
        self.save_quick_checkpoint(
            context,
            &snapshot,
            failure.run_id,
            CrawlRecoveryPhase::Traversing,
        )
        .await?;
        if failure.retryable && !failure.terminal_attempt {
            self.progress(context, "RETRY_SCHEDULED", None).await?;
            return Err(());
        }
        self.finish_recovered_execution(context, failure.run_id, snapshot, CrawlRunStatus::Failed)
            .await?;
        if !failure.retryable || failure.terminal_attempt {
            context.mark_terminal_failure();
        }
        Err(())
    }

    #[allow(dead_code)]
    async fn persist_success(
        &self,
        run_id: CrawlRunId,
        record: &CrawlExecutionRecord,
        partial: bool,
    ) -> Result<(), ()> {
        CrawlExecutionRepository::new(&self.database)
            .persist(record)
            .await
            .or_else(duplicate_execution_is_ok)
            .map_err(|_| ())?;
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
            .map_err(|_| ())?;
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
            .map_err(|_| ())
    }

    async fn persist_artifacts(
        &self,
        run_id: CrawlRunId,
        source_id: SourceId,
        created_at: &str,
        artifacts: Vec<CrawlerArtifactEvidence>,
        retain: bool,
    ) -> Result<Vec<CrawlExecutionArtifact>, ()> {
        if !retain {
            return Ok(Vec::new());
        }
        let mut persisted = Vec::new();
        for artifact in artifacts {
            let (kind, file_name, media_type, bytes) = artifact_bytes(&artifact);
            let stored = self
                .artifact_store
                .write_bytes(format!("quick-scrape/{run_id}"), file_name, bytes)
                .map_err(|_| ())?;
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
                .map_err(|_| ())?;
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
    ) -> Result<(), ()> {
        let attempt = ProgressAttemptId::new(context.attempt_id().to_owned()).map_err(|_| ())?;
        let metadata = ProgressMetadata::default();
        let event = match terminal {
            Some(terminal) => {
                NewProgressEvent::terminal(context.job_id().clone(), terminal, metadata)
                    .map_err(|_| ())?
            }
            None => NewProgressEvent::new(
                context.job_id().clone(),
                ProgressKey::new(key).map_err(|_| ())?,
                metadata,
            ),
        }
        .with_attempt(attempt);
        let service = ProgressService::new(&self.database);
        let now = epoch_seconds();
        match &self.progress_live_hub {
            Some(hub) => service
                .append_and_publish_at(hub, &event, now)
                .await
                .map(|_| ())
                .map_err(|_| ()),
            None => service
                .append_at(&event, now)
                .await
                .map(|_| ())
                .map_err(|_| ()),
        }
    }
}

struct FailureContext<'url> {
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
            handler
                .execute_inner(context)
                .await
                .map_err(|()| JobExecutionError)
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
    matches!(error, RobotsPolicyError::Unavailable(_))
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
