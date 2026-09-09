//! Bounded physical Production page-attempt execution.
//!
//! This module coordinates one provider attempt and its directly coupled
//! artifact/execution persistence helpers. Crawl policies remain owned by
//! `erabi-crawler`; this module only preserves their runtime ordering.

#[allow(clippy::wildcard_imports)]
use super::*;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

#[allow(clippy::result_large_err)]
impl ProductionCrawlJobHandler {
    #[allow(clippy::too_many_lines)]
    async fn execute_page(
        &self,
        context: &JobExecutionContext,
        snapshot: &CrawlRunSnapshot,
        requested_url: &str,
        deadline: &ProductionDeadline,
    ) -> Result<PageResult, PageFailure> {
        let target = requested_url
            .parse::<url::Url>()
            .map_err(|_| PageFailure::normal(CrawlExecutionErrorCode::InvalidResponse))?;
        self.network_policy
            .validate_and_resolve(&target)
            .await
            .map_err(|_| {
                context.record_primary_diagnostic(ExecutionDiagnostic::new(
                    OrchestrationErrorCategory::NetworkAdmission,
                    ExecutionOperation::AcquireAdmission,
                    ExecutionAction::Fail,
                    "NETWORK_TARGET_REJECTED",
                ));
                PageFailure::normal(CrawlExecutionErrorCode::InvalidResponse)
            })?;
        let origin = OriginKey::from_url(&target).map_err(|_| {
            context.record_primary_diagnostic(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::NetworkAdmission,
                ExecutionOperation::AcquireAdmission,
                ExecutionAction::Fail,
                "ORIGIN_INVALID",
            ));
            PageFailure::normal(CrawlExecutionErrorCode::InvalidResponse)
        })?;
        let registration = self.pacing.register(origin, snapshot).map_err(|_| {
            context.record_primary_diagnostic(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Pacing,
                ExecutionOperation::AcquireAdmission,
                ExecutionAction::Retry,
                "PACING_REGISTRATION_FAILED",
            ));
            PageFailure::normal(CrawlExecutionErrorCode::RemoteFailure)
        })?;
        let pacing_cancel = PacingCancellation::new();
        let admission = tokio::select! {
            value = self.robots.evaluate(&target, snapshot, &pacing_cancel) => value.map_err(|error| {
                context.record_primary_diagnostic(ExecutionDiagnostic::new(
                    OrchestrationErrorCategory::NetworkAdmission,
                    ExecutionOperation::AcquireAdmission,
                    ExecutionAction::Retry,
                    "ROBOTS_POLICY_FAILED",
                ));
                if matches!(error, erabi_crawler::RobotsPolicyError::UnavailableWithPacing { .. }) {
                    context.record_secondary_diagnostic(ExecutionDiagnostic::new(
                        OrchestrationErrorCategory::Pacing,
                        ExecutionOperation::RecordOutcome,
                        ExecutionAction::Continue,
                        "ROBOTS_PACING_OUTCOME_RECORD_FAILED",
                    ));
                }
                PageFailure::normal(CrawlExecutionErrorCode::RobotsExcluded)
            }),
            () = context.storage_pressure().signalled() => { pacing_cancel.cancel(); return Err(PageFailure::normal(CrawlExecutionErrorCode::StoragePressure)); }
            () = context.cancellation().cancelled() => { pacing_cancel.cancel(); return Err(PageFailure::normal(CrawlExecutionErrorCode::Cancelled)); }
        }?;
        if admission.decision() == RobotsAdmissionDecision::Disallowed {
            context.record_primary_diagnostic(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::NetworkAdmission,
                ExecutionOperation::AcquireAdmission,
                ExecutionAction::Fail,
                "ROBOTS_EXCLUDED",
            ));
            return Err(PageFailure::normal(CrawlExecutionErrorCode::RobotsExcluded));
        }
        let permit = tokio::select! {
            value = registration.acquire(&admission, &pacing_cancel) => value.map_err(|_| {
                context.record_primary_diagnostic(ExecutionDiagnostic::new(
                    OrchestrationErrorCategory::Pacing,
                    ExecutionOperation::AcquireAdmission,
                    ExecutionAction::Retry,
                    "PACING_PERMIT_ACQUISITION_FAILED",
                ));
                PageFailure::normal(CrawlExecutionErrorCode::RemoteFailure)
            }),
            () = context.storage_pressure().signalled() => { pacing_cancel.cancel(); return Err(PageFailure::normal(CrawlExecutionErrorCode::StoragePressure)); }
            () = context.cancellation().cancelled() => { pacing_cancel.cancel(); return Err(PageFailure::normal(CrawlExecutionErrorCode::Cancelled)); }
        }?;
        // Recompute immediately before the provider call so pacing/robots
        // work cannot let an in-flight request exceed the frozen run cap.
        let timeout = deadline
            .remaining_timeout(snapshot.settings().timeout_ms.value)
            .ok_or_else(PageFailure::duration_exhausted)?;
        self.progress(context, "PAGE_LOADING", None)
            .await
            .map_err(|_| PageFailure::normal(CrawlExecutionErrorCode::RemoteFailure))?;
        let request = CrawlerExecuteRequest::try_new(
            target,
            timeout,
            snapshot.settings().user_agent.value.clone(),
            RenderingRequirement::RenderedHtml,
            None,
            None,
            CrawlerEvidencePolicy {
                cleaned_html: true,
                rendered_html: true,
                markdown: true,
                discovered_links: true,
                selector_observations: true,
                pagination_observations: true,
                screenshot: if snapshot.settings().screenshot.value {
                    ScreenshotPolicy::Viewport
                } else {
                    ScreenshotPolicy::None
                },
                ..CrawlerEvidencePolicy::default()
            },
        )
        .map_err(|_| PageFailure::normal(CrawlExecutionErrorCode::InvalidResponse))?;
        let result = tokio::select! {
            value = async {
                let started = Instant::now();
                let result = self.adapter.execute(request).await;
                (started.elapsed(), result)
            } => value,
            () = context.storage_pressure().signalled() => { pacing_cancel.cancel(); return Err(PageFailure::normal(CrawlExecutionErrorCode::StoragePressure)); }
            () = context.cancellation().cancelled() => { pacing_cancel.cancel(); return Err(PageFailure::normal(CrawlExecutionErrorCode::Cancelled)); }
        };
        let (provider_duration, result) = result;
        emit(SemanticEvent::ProviderExecuteCompleted {
            context: crate::telemetry_job_context(context),
            provider: ProviderToken::Crawl4Ai,
            outcome: if result.is_ok() {
                EventOutcome::Success
            } else {
                EventOutcome::Failure
            },
            duration_ms: u64::try_from(provider_duration.as_millis()).unwrap_or(u64::MAX),
            code: result.as_ref().err().map(|error| {
                TelemetryCode::from_static(crawl_execution_code_name(adapter_error_code(error)))
            }),
        });
        let result = match result {
            Ok(result) => {
                if permit.record_outcome(PacingOutcome::Success).is_err() {
                    context.record_secondary_diagnostic(ExecutionDiagnostic::new(
                        OrchestrationErrorCategory::Pacing,
                        ExecutionOperation::RecordOutcome,
                        ExecutionAction::Continue,
                        "PACING_OUTCOME_RECORD_FAILED",
                    ));
                }
                result
            }
            Err(error) => {
                context.record_primary_diagnostic(
                    ExecutionDiagnostic::new(
                        OrchestrationErrorCategory::Provider,
                        ExecutionOperation::ProviderExecution,
                        ExecutionAction::Retry,
                        crawl_execution_code_name(adapter_error_code(&error)),
                    )
                    .with_provider("crawler-adapter"),
                );
                if permit
                    .record_outcome(PacingOutcome::from_adapter_error(&error))
                    .is_err()
                {
                    context.record_secondary_diagnostic(ExecutionDiagnostic::new(
                        OrchestrationErrorCategory::Pacing,
                        ExecutionOperation::RecordOutcome,
                        ExecutionAction::Continue,
                        "PACING_OUTCOME_RECORD_FAILED",
                    ));
                }
                return Err(PageFailure {
                    code: adapter_error_code(&error),
                    status: adapter_error_status(&error),
                    duration_exhausted: false,
                });
            }
        };
        let (observation, response, artifacts, completeness) = result.into_parts();
        if observation.requested_url != requested_url {
            return Err(PageFailure::normal(
                CrawlExecutionErrorCode::InvalidResponse,
            ));
        }
        let final_url = observation
            .final_url
            .as_deref()
            .unwrap_or(&observation.requested_url);
        let final_target = final_url
            .parse::<url::Url>()
            .map_err(|_| PageFailure::normal(CrawlExecutionErrorCode::InvalidResponse))?;
        self.network_policy
            .validate_and_resolve(&final_target)
            .await
            .map_err(|_| PageFailure::normal(CrawlExecutionErrorCode::InvalidResponse))?;
        Ok(PageResult {
            observation,
            status: response.status_code(),
            media_type: response.media_type().map(|value| value.as_str().to_owned()),
            content_length: response.content_length_bytes(),
            elapsed_ms: response.provider_elapsed_ms(),
            artifacts,
            completeness,
        })
    }

    pub(super) async fn persist_execution(
        &self,
        record: CrawlExecutionRecord,
        context: &JobExecutionContext,
        expected_work_generation: Option<u64>,
        historical_alias_if_current: bool,
    ) -> ProductionResult<()> {
        let work_state = match record.outcome {
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
                    &crawl_url_state_id(record.crawl_run_id, &record.canonical_url),
                )
                .await
                .map_err(|_| {
                    ProductionError::repository(
                        ExecutionOperation::PersistExecution,
                        "WORK_GENERATION_LOAD_FAILED",
                    )
                })?,
        };
        let result = executions
            .persist_current_work(
                &record,
                &crawl_url_state_id(record.crawl_run_id, &record.canonical_url),
                context.job_id(),
                context.attempt_id(),
                work_state,
                expected_work_generation,
                context.ownership_now(),
            )
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(CrawlExecutionRepositoryError::InvalidReference) if historical_alias_if_current => {
                executions
                    .persist_historical_work(
                        &record,
                        &crawl_url_state_id(record.crawl_run_id, &record.canonical_url),
                        context.job_id(),
                        context.attempt_id(),
                        expected_work_generation,
                        context.ownership_now(),
                    )
                    .await
                    .map_err(|_| {
                        ProductionError::repository(
                            ExecutionOperation::PersistExecution,
                            "HISTORICAL_EXECUTION_PERSIST_FAILED",
                        )
                    })
            }
            Err(_) => Err(ProductionError::repository(
                ExecutionOperation::PersistExecution,
                "EXECUTION_PERSIST_FAILED",
            )),
        }
    }

    pub(super) async fn persist_artifacts(
        &self,
        context: &JobExecutionContext,
        run_id: CrawlRunId,
        created_at: &str,
        artifacts: Vec<CrawlerArtifactEvidence>,
        retain: bool,
    ) -> ProductionResult<Vec<CrawlExecutionArtifact>> {
        if !retain {
            return Ok(Vec::new());
        }
        let mut saved = Vec::new();
        for artifact in artifacts {
            let (kind, name, media_type, bytes) = artifact_bytes(&artifact);
            let stored = self
                .artifact_store
                .write_bytes(format!("production/{run_id}"), name, bytes)
                .map_err(|_| {
                    ProductionError::artifact(
                        ExecutionOperation::PersistArtifact,
                        "ARTIFACT_WRITE_FAILED",
                    )
                })?;
            ArtifactRepository::new(&self.database)
                .record(
                    &stored,
                    Some(run_id),
                    None,
                    media_type,
                    created_at,
                    &serde_json::json!({"kind":artifact_kind_name(kind)}),
                )
                .await
                .map_err(|_| {
                    ProductionError::artifact(
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
            saved.push(CrawlExecutionArtifact {
                artifact_id: stored.id,
                kind: execution_artifact_kind(kind),
            });
        }
        Ok(saved)
    }
}

#[derive(Clone)]
pub(super) struct ProductionTraversalProvider {
    handler: ProductionCrawlJobHandler,
    context: JobExecutionContext,
    snapshot: CrawlRunSnapshot,
    deadline: ProductionDeadline,
    attempts: Arc<Mutex<BTreeMap<String, ProductionPageAttempt>>>,
}

impl ProductionTraversalProvider {
    pub(super) fn new(
        handler: ProductionCrawlJobHandler,
        context: JobExecutionContext,
        snapshot: CrawlRunSnapshot,
        deadline: ProductionDeadline,
    ) -> Self {
        Self {
            handler,
            context,
            snapshot,
            deadline,
            attempts: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(super) async fn take_attempts(&self) -> BTreeMap<String, ProductionPageAttempt> {
        std::mem::take(&mut *self.attempts.lock().await)
    }
}

impl DiscoveryPreviewProvider for ProductionTraversalProvider {
    fn observe(
        &self,
        request: DiscoveryPreviewObservationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn Future<
                    Output = Result<DiscoveryPreviewProviderOutcome, DiscoveryPreviewProviderError>,
                > + Send
                + '_,
        >,
    > {
        let provider = self.clone();
        Box::pin(async move {
            let requested_url = request.requested_url;
            if provider.context.cancellation().is_cancelled() {
                return Ok(DiscoveryPreviewProviderOutcome::Interrupted {
                    reason: erabi_crawler::DiscoveryPreviewInterruption::Cancelled,
                });
            }
            if provider.context.storage_pressure().is_signalled() {
                return Ok(DiscoveryPreviewProviderOutcome::Interrupted {
                    reason: erabi_crawler::DiscoveryPreviewInterruption::StoragePressure,
                });
            }
            let outcome = match provider
                .handler
                .execute_page(
                    &provider.context,
                    &provider.snapshot,
                    &requested_url,
                    &provider.deadline,
                )
                .await
            {
                Ok(page) => {
                    let downloaded_bytes = page.content_length.unwrap_or(0);
                    let observation = page.semantic_observation();
                    provider.attempts.lock().await.insert(
                        requested_url,
                        ProductionPageAttempt::Observed {
                            page: Box::new(page),
                            observed_at_millis: provider.handler.clock.now_millis(),
                        },
                    );
                    DiscoveryPreviewProviderOutcome::Observed {
                        observation,
                        downloaded_bytes,
                    }
                }
                Err(failure) => {
                    if failure.code == CrawlExecutionErrorCode::Cancelled
                        && provider.context.cancellation().is_cancelled()
                    {
                        return Ok(DiscoveryPreviewProviderOutcome::Interrupted {
                            reason: erabi_crawler::DiscoveryPreviewInterruption::Cancelled,
                        });
                    }
                    if failure.code == CrawlExecutionErrorCode::StoragePressure
                        && provider.context.storage_pressure().is_signalled()
                    {
                        return Ok(DiscoveryPreviewProviderOutcome::Interrupted {
                            reason: erabi_crawler::DiscoveryPreviewInterruption::StoragePressure,
                        });
                    }
                    provider.attempts.lock().await.insert(
                        requested_url,
                        ProductionPageAttempt::Failed {
                            failure: failure.clone(),
                            observed_at_millis: provider.handler.clock.now_millis(),
                        },
                    );
                    if failure.code == CrawlExecutionErrorCode::RobotsExcluded {
                        DiscoveryPreviewProviderOutcome::RobotsExcluded {
                            reason: "ROBOTS_EXCLUDED".to_owned(),
                        }
                    } else {
                        DiscoveryPreviewProviderOutcome::PageFailed {
                            diagnostic: TestDiagnostic {
                                code: if failure.duration_exhausted {
                                    "PRODUCTION_DURATION_EXHAUSTED".to_owned()
                                } else {
                                    "PRODUCTION_PAGE_FAILED".to_owned()
                                },
                                message: "The bounded Production page operation did not complete."
                                    .to_owned(),
                            },
                        }
                    }
                }
            };
            Ok(outcome)
        })
    }
}

#[derive(Clone)]
pub(super) struct ProductionDeadline {
    clock: Arc<dyn PreviewClock>,
    started_at_millis: u64,
    max_duration_millis: u64,
}

impl ProductionDeadline {
    pub(super) fn new(
        clock: Arc<dyn PreviewClock>,
        started_at_millis: u64,
        max_duration_millis: u64,
    ) -> Self {
        Self {
            clock,
            started_at_millis,
            max_duration_millis,
        }
    }

    pub(super) fn remaining_timeout(&self, per_page_timeout_millis: u64) -> Option<Duration> {
        let elapsed = self
            .clock
            .now_millis()
            .saturating_sub(self.started_at_millis);
        let remaining = self.max_duration_millis.checked_sub(elapsed)?;
        (remaining > 0).then(|| Duration::from_millis(remaining.min(per_page_timeout_millis)))
    }
}

pub(super) struct SystemProductionClock;

impl PreviewClock for SystemProductionClock {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
            })
    }
}

#[derive(Clone)]
pub(super) enum ProductionPageAttempt {
    Observed {
        page: Box<PageResult>,
        observed_at_millis: u64,
    },
    Failed {
        failure: PageFailure,
        observed_at_millis: u64,
    },
}

#[derive(Clone)]
pub(super) struct PageResult {
    pub(super) observation: erabi_crawler::PageObservation,
    pub(super) status: Option<u16>,
    pub(super) media_type: Option<String>,
    pub(super) content_length: Option<u64>,
    pub(super) elapsed_ms: Option<u64>,
    pub(super) artifacts: Vec<CrawlerArtifactEvidence>,
    pub(super) completeness: CrawlerResultCompleteness,
}

/// One physical provider invocation contributes exactly one attempt. An
/// observed provider-partial page is completed in the sense used by the
/// Task 3 summary contract, while its partial evidence contributes one
/// unresolved-work unit; it is never counted as a second attempt.
#[derive(Clone, Copy, Debug, Default)]
#[allow(dead_code)]
pub(super) struct PageAttemptCounts {
    pub(super) attempted: u64,
    pub(super) completed: u64,
    pub(super) unresolved_partial_work: u64,
}

impl PageResult {
    fn semantic_observation(&self) -> erabi_crawler::PageObservation {
        let mut observation = self.observation.clone();
        // Direct non-HTML responses are evidence only; they never enter HTML
        // discovery/extraction semantics in this task.
        if !self.media_type.as_deref().is_some_and(is_html) {
            observation.discovered_links.clear();
            observation.pagination_observations.clear();
        }
        observation
    }
}

#[derive(Clone)]
pub(super) struct PageFailure {
    pub(super) code: CrawlExecutionErrorCode,
    pub(super) status: Option<u16>,
    pub(super) duration_exhausted: bool,
}

impl PageFailure {
    pub(super) const fn normal(code: CrawlExecutionErrorCode) -> Self {
        Self {
            code,
            status: None,
            duration_exhausted: false,
        }
    }

    pub(super) const fn duration_exhausted() -> Self {
        Self {
            code: CrawlExecutionErrorCode::Timeout,
            status: None,
            duration_exhausted: true,
        }
    }
}

fn is_html(media_type: &str) -> bool {
    media_type
        .split(';')
        .next()
        .is_some_and(|value| value.eq_ignore_ascii_case("text/html"))
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
    if let CrawlerAdapterError::RemoteFailure { status_code } = error {
        *status_code
    } else {
        None
    }
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

fn execution_artifact_kind(kind: CrawlerArtifactKind) -> CrawlExecutionArtifactKind {
    match kind {
        CrawlerArtifactKind::RawHtml => CrawlExecutionArtifactKind::RawHtml,
        CrawlerArtifactKind::CleanedHtml => CrawlExecutionArtifactKind::CleanedHtml,
        CrawlerArtifactKind::RenderedHtml => CrawlExecutionArtifactKind::RenderedHtml,
        CrawlerArtifactKind::Markdown => CrawlExecutionArtifactKind::Markdown,
        CrawlerArtifactKind::Screenshot => CrawlExecutionArtifactKind::Screenshot,
    }
}

fn artifact_kind_name(kind: CrawlerArtifactKind) -> &'static str {
    match kind {
        CrawlerArtifactKind::RawHtml => "RAW_HTML",
        CrawlerArtifactKind::CleanedHtml => "CLEANED_HTML",
        CrawlerArtifactKind::RenderedHtml => "RENDERED_HTML",
        CrawlerArtifactKind::Markdown => "MARKDOWN",
        CrawlerArtifactKind::Screenshot => "SCREENSHOT",
    }
}

impl ProductionPageAttempt {
    pub(super) const fn observed_at_millis(&self) -> u64 {
        match self {
            Self::Observed {
                observed_at_millis, ..
            }
            | Self::Failed {
                observed_at_millis, ..
            } => *observed_at_millis,
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
