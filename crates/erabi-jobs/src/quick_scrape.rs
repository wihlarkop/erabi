//! Durable execution handler for provider-neutral one-page Quick Scrapes.

use std::{
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use erabi_crawler::{
    AdmissionError, CrawlerAdapter, CrawlerAdapterError, CrawlerArtifactEvidence,
    CrawlerArtifactKind, CrawlerEvidencePolicy, CrawlerExecuteRequest, CrawlerResultCompleteness,
    NetworkTargetPolicy, OriginKey, PacingCancellation, PacingOutcome, PacingService,
    RenderingRequirement, RobotsAdmissionDecision, RobotsPolicyError, RobotsPolicyService,
    ScreenshotPolicy, quick_scrape_snapshot_target,
};
use erabi_db::{
    ArtifactStore, ErabiDatabase,
    repositories::{
        ArtifactRepository, CrawlExecutionArtifact, CrawlExecutionArtifactKind,
        CrawlExecutionRecord, CrawlExecutionRepository, CrawlExecutionRepositoryError,
        CrawlExecutionSummary, CrawlRunRepository, JobRepository, SourceRepository,
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
        if !matches!(context.kind().as_str(), "QUICK_SCRAPE" | "RETRY") {
            return Err(());
        }
        let job = JobRepository::new(&self.database)
            .job(context.job_id())
            .await
            .map_err(|_| ())?;
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
        let execution_id = execution_id_for_job(context.job_id().as_str())?;
        let executions = CrawlExecutionRepository::new(&self.database);
        match executions.read(execution_id).await {
            Ok(existing) => {
                return self
                    .finish_recovered_execution(&context, run_id, existing)
                    .await;
            }
            Err(CrawlExecutionRepositoryError::NotFound) => {}
            Err(_) => return Err(()),
        }
        CrawlRunRepository::new(&self.database)
            .transition_execution_status(run_id, CrawlRunStatus::Running)
            .await
            .map_err(|_| ())?;
        self.progress(&context, "STARTED", None).await?;

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
            self.persist_success(run_id, &record, false).await?;
            self.progress(
                &context,
                "COMPLETED",
                Some(ProgressTerminalState::Succeeded),
            )
            .await?;
            return Ok(());
        }

        let origin = OriginKey::from_url(&target.target_url).map_err(|_| ())?;
        // Registration is a runtime-only RAII contribution to Task 5's
        // process-wide same-origin registry. It cannot cross attempts/restarts.
        let registration = match self.pacing.register(origin, &snapshot) {
            Ok(registration) => registration,
            Err(AdmissionError::Cancelled) => {
                context.cancellation().cancel();
                let _ = self
                    .progress(
                        &context,
                        "CANCELLED",
                        Some(ProgressTerminalState::Cancelled),
                    )
                    .await;
                return Err(());
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
                            terminal_attempt: job.current_attempt >= job.max_attempts,
                        },
                    )
                    .await;
            }
        };
        let pacing_cancellation = PacingCancellation::new();
        let robots = tokio::select! {
            value = self.robots.evaluate(&target.target_url, &snapshot, &pacing_cancellation) => value,
            () = context.cancellation().cancelled() => {
                pacing_cancellation.cancel();
                let _ = self.progress(&context, "CANCELLED", Some(ProgressTerminalState::Cancelled)).await;
                return Err(());
            }
        };
        let robots = match robots {
            Ok(robots) => robots,
            Err(RobotsPolicyError::Admission(AdmissionError::Cancelled)) => {
                context.cancellation().cancel();
                let _ = self
                    .progress(
                        &context,
                        "CANCELLED",
                        Some(ProgressTerminalState::Cancelled),
                    )
                    .await;
                return Err(());
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
                            terminal_attempt: job.current_attempt >= job.max_attempts,
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
                    },
                )
                .await;
        }
        let permit = tokio::select! {
            value = registration.acquire(&robots, &pacing_cancellation) => value,
            () = context.cancellation().cancelled() => {
                pacing_cancellation.cancel();
                let _ = self.progress(&context, "CANCELLED", Some(ProgressTerminalState::Cancelled)).await;
                return Err(());
            }
        };
        let permit = match permit {
            Ok(permit) => permit,
            Err(AdmissionError::Cancelled) => {
                context.cancellation().cancel();
                let _ = self
                    .progress(
                        &context,
                        "CANCELLED",
                        Some(ProgressTerminalState::Cancelled),
                    )
                    .await;
                return Err(());
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
                            terminal_attempt: job.current_attempt >= job.max_attempts,
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
            () = context.cancellation().cancelled() => {
                pacing_cancellation.cancel();
                let _ = self.progress(&context, "CANCELLED", Some(ProgressTerminalState::Cancelled)).await;
                return Err(());
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
                    let _ = self
                        .progress(
                            &context,
                            "CANCELLED",
                            Some(ProgressTerminalState::Cancelled),
                        )
                        .await;
                    return Err(());
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
                            terminal_attempt: job.current_attempt >= job.max_attempts,
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
        self.persist_success(run_id, &record, partial).await?;
        self.progress(
            &context,
            if partial {
                "PARTIAL_RESULT"
            } else {
                "COMPLETED"
            },
            Some(ProgressTerminalState::Succeeded),
        )
        .await?;
        Ok(())
    }

    async fn finish_recovered_execution(
        &self,
        context: &JobExecutionContext,
        run_id: CrawlRunId,
        record: CrawlExecutionRecord,
    ) -> Result<(), ()> {
        match record.outcome {
            CrawlExecutionOutcome::Completed => {
                CrawlExecutionRepository::new(&self.database)
                    .save_summary(&CrawlExecutionSummary {
                        crawl_run_id: run_id,
                        in_scope_pages_planned: 1,
                        in_scope_pages_completed: 1,
                        pagination_truncation_count: 0,
                        unresolved_partial_work_count: 0,
                        page_type_ambiguity_count: 0,
                    })
                    .await
                    .map_err(|_| ())?;
                CrawlRunRepository::new(&self.database)
                    .transition_execution_status(run_id, CrawlRunStatus::Succeeded)
                    .await
                    .map_err(|_| ())?;
                let _ = self
                    .progress(context, "COMPLETED", Some(ProgressTerminalState::Succeeded))
                    .await;
                Ok(())
            }
            CrawlExecutionOutcome::Partial => {
                CrawlExecutionRepository::new(&self.database)
                    .save_summary(&CrawlExecutionSummary {
                        crawl_run_id: run_id,
                        in_scope_pages_planned: 1,
                        in_scope_pages_completed: 1,
                        pagination_truncation_count: 0,
                        unresolved_partial_work_count: 1,
                        page_type_ambiguity_count: 0,
                    })
                    .await
                    .map_err(|_| ())?;
                CrawlRunRepository::new(&self.database)
                    .transition_execution_status(run_id, CrawlRunStatus::PartialResult)
                    .await
                    .map_err(|_| ())?;
                let _ = self
                    .progress(
                        context,
                        "PARTIAL_RESULT",
                        Some(ProgressTerminalState::Succeeded),
                    )
                    .await;
                Ok(())
            }
            CrawlExecutionOutcome::Failed => {
                CrawlExecutionRepository::new(&self.database)
                    .save_summary(&CrawlExecutionSummary {
                        crawl_run_id: run_id,
                        in_scope_pages_planned: 1,
                        in_scope_pages_completed: 0,
                        pagination_truncation_count: 0,
                        unresolved_partial_work_count: 1,
                        page_type_ambiguity_count: 0,
                    })
                    .await
                    .map_err(|_| ())?;
                CrawlRunRepository::new(&self.database)
                    .transition_execution_status(run_id, CrawlRunStatus::Failed)
                    .await
                    .map_err(|_| ())?;
                context.mark_terminal_failure();
                Err(())
            }
            CrawlExecutionOutcome::Cancelled => Err(()),
        }
    }

    async fn terminal_failure(
        &self,
        context: &JobExecutionContext,
        failure: &FailureContext<'_>,
    ) -> Result<(), ()> {
        if failure.retryable && !failure.terminal_attempt {
            self.progress(context, "RETRY_SCHEDULED", None).await?;
            return Err(());
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
        CrawlExecutionRepository::new(&self.database)
            .persist(&record)
            .await
            .or_else(duplicate_execution_is_ok)
            .map_err(|_| ())?;
        CrawlExecutionRepository::new(&self.database)
            .save_summary(&CrawlExecutionSummary {
                crawl_run_id: failure.run_id,
                in_scope_pages_planned: 1,
                in_scope_pages_completed: 0,
                pagination_truncation_count: 0,
                unresolved_partial_work_count: 1,
                page_type_ambiguity_count: 0,
            })
            .await
            .map_err(|_| ())?;
        CrawlRunRepository::new(&self.database)
            .transition_execution_status(failure.run_id, CrawlRunStatus::Failed)
            .await
            .map_err(|_| ())?;
        self.progress(context, "FAILED", Some(ProgressTerminalState::Failed))
            .await?;
        if !failure.retryable {
            context.mark_terminal_failure();
        }
        Err(())
    }

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
}

impl JobHandler for QuickScrapeJobHandler {
    fn execute(
        &self,
        context: JobExecutionContext,
    ) -> impl Future<Output = Result<(), JobExecutionError>> + Send {
        let handler = self.clone();
        async move {
            handler
                .execute_inner(context)
                .await
                .map_err(|()| JobExecutionError)
        }
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
