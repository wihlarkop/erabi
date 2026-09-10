//! Bounded execution for one frozen Production Crawl Run.
//!
//! It delegates discovery semantics to `erabi_crawler::SemanticTraversal` and
//! owns provider execution, durable evidence, progress, checkpoint-backed
//! recovery, and final status.

use std::{
    future::Future,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use erabi_crawler::{
    CrawlRecoveryCheckpoint, CrawlRecoveryPhase, CrawlerAdapter, CrawlerAdapterError,
    CrawlerArtifactEvidence, CrawlerArtifactKind, CrawlerEvidencePolicy, CrawlerExecuteRequest,
    CrawlerResultCompleteness, DiscoveryPreviewObservationRequest, DiscoveryPreviewProvider,
    DiscoveryPreviewProviderError, DiscoveryPreviewProviderOutcome, NetworkTargetPolicy, OriginKey,
    PacingCancellation, PacingOutcome, PacingService, PreviewClock, RenderingRequirement,
    RobotsAdmissionDecision, RobotsPolicyService, ScreenshotPolicy, SemanticTraversal,
    SemanticTraversalCheckpoint, SemanticTraversalQueueEntry, SemanticTraversalStep,
    SemanticTraversalTransitionState, load_frozen_production_semantics,
};
use erabi_db::{
    ArtifactStore, ErabiDatabase,
    repositories::{
        ArtifactRepository, CrawlAdmissionState, CrawlExecutionArtifact,
        CrawlExecutionArtifactKind, CrawlExecutionRecord, CrawlExecutionRepository,
        CrawlExecutionRepositoryError, CrawlExecutionSummary, CrawlInFlightWork,
        CrawlPageTypeMatchState, CrawlRedirectReconciliation, CrawlRunRepository,
        CrawlTransitionSourceCount, CrawlTraversalControl, CrawlTraversalPageTypeCounts,
        CrawlTraversalRepository, CrawlTraversalRepositoryError, CrawlTraversalSemanticProjection,
        CrawlTraversalUrlSemanticState, CrawlUrlStateRecord, CrawlWorkState, DiscoveredUrlRecord,
        JobRepository,
    },
};
use erabi_domain::{
    CrawlExecutionErrorCode, CrawlExecutionId, CrawlExecutionOutcome, CrawlRunId, CrawlRunSnapshot,
    CrawlRunStatus, DiscoveryPath, DiscoveryPreviewPage, DiscoveryPreviewResult,
    DiscoveryPreviewSeed, DiscoveryTransitionId, EffectiveDiscoveryPreviewLimits,
    EffectiveTransitionPreviewTotalLimit, PageTypeId, PreviewBudgetKind, PreviewUrlState,
    TestDiagnostic,
};
use erabi_observability::{
    ArtifactKind, CrawlExecutionSpan, EventOutcome, ProviderToken, SemanticEvent, TelemetryCode,
    emit,
};
use tokio::sync::Mutex;
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

const PRODUCTION_CRAWL_JOB_KIND: &str = "PRODUCTION_CRAWL";

mod crawl_stage;
mod finalization;
mod page_execution;
use page_execution::{
    PageAttemptCounts, ProductionDeadline, ProductionPageAttempt, ProductionTraversalProvider,
    SystemProductionClock,
};

#[derive(Clone, Debug)]
struct ProductionError {
    diagnostics: ExecutionDiagnostics,
}

type ProductionResult<T> = Result<T, ProductionError>;

impl ProductionError {
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

    fn projection(operation: ExecutionOperation, code: &'static str) -> Self {
        Self::new(ExecutionDiagnostic::new(
            OrchestrationErrorCategory::SerializationProjection,
            operation,
            ExecutionAction::Fail,
            code,
        ))
    }
}

fn is_production_job_kind(kind: &str) -> bool {
    matches!(
        kind,
        PRODUCTION_CRAWL_JOB_KIND
            | "RETRY"
            | "RETRY_FAILED_PARTS"
            | "RESUME_CHECKPOINT"
            | "RERUN_FULL_CRAWL"
    )
}

fn production_recovery_error(
    operation: ExecutionOperation,
    error: CrawlRecoveryValidationError,
) -> ProductionError {
    ProductionError::checkpoint(operation, error.diagnostic_code())
}

fn production_checkpoint_load_error(
    operation: ExecutionOperation,
    error: &erabi_db::repositories::JobRepositoryError,
) -> ProductionError {
    ProductionError::checkpoint(
        operation,
        checkpoint_error_code(error).unwrap_or("CHECKPOINT_LOAD_FAILED"),
    )
}

fn production_traversal_error(
    operation: ExecutionOperation,
    error: CrawlTraversalRepositoryError,
) -> ProductionError {
    match error {
        CrawlTraversalRepositoryError::CrawlRunNotFound
        | CrawlTraversalRepositoryError::InvalidState
        | CrawlTraversalRepositoryError::CorruptState => {
            production_recovery_error(operation, CrawlRecoveryValidationError::StateInvalid)
        }
        CrawlTraversalRepositoryError::Checkpoint(error) => {
            if let Some(mapped) = map_checkpoint_repository_error(&error) {
                production_recovery_error(operation, mapped)
            } else {
                ProductionError::checkpoint(operation, "CHECKPOINT_LOAD_FAILED")
            }
        }
        CrawlTraversalRepositoryError::Database(_) => {
            ProductionError::repository(operation, "TRAVERSAL_STATE_LOAD_FAILED")
        }
        CrawlTraversalRepositoryError::Discovery(_) => {
            ProductionError::repository(operation, "DISCOVERY_LOAD_FAILED")
        }
    }
}

/// Runtime dependencies are injected by the process composition root. In
/// particular, `pacing` is the one process-wide Task 5 service also used by
/// robots and Quick Scrape; this handler never constructs a limiter.
#[derive(Clone)]
pub struct ProductionCrawlJobHandler {
    database: ErabiDatabase,
    adapter: Arc<dyn CrawlerAdapter>,
    robots: RobotsPolicyService,
    pacing: PacingService,
    network_policy: NetworkTargetPolicy,
    artifact_store: ArtifactStore,
    progress_live_hub: Option<ProgressLiveHub>,
    clock: Arc<dyn PreviewClock>,
}

#[allow(clippy::result_large_err)]
impl ProductionCrawlJobHandler {
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
            clock: Arc::new(SystemProductionClock),
        }
    }

    #[must_use]
    pub fn with_progress_live_hub(mut self, hub: ProgressLiveHub) -> Self {
        self.progress_live_hub = Some(hub);
        self
    }

    /// Shares Preview's existing deterministic clock seam with production
    /// duration enforcement and observation timestamps.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn PreviewClock>) -> Self {
        self.clock = clock;
        self
    }

    async fn execute_inner(&self, context: JobExecutionContext) -> ProductionResult<()> {
        match self.run_crawl_stage(context.clone()).await? {
            crawl_stage::CrawlStageOutcome::ReadyForPostCrawl(ready) => {
                let final_status = self.finalize_ready_for_post_crawl(&context, &ready).await?;
                if let Err(error) = self
                    .progress(&context, "FINALIZATION_COMPLETED", None)
                    .await
                {
                    context.record_secondary_diagnostics(error.diagnostics);
                }
                if let Err(error) = self
                    .progress(
                        &context,
                        if final_status == CrawlRunStatus::PartialResult {
                            "PRODUCTION_PARTIAL_RESULT"
                        } else {
                            "PRODUCTION_BOUNDED_COMPLETE"
                        },
                        Some(ProgressTerminalState::Succeeded),
                    )
                    .await
                {
                    context.record_secondary_diagnostics(error.diagnostics);
                }
                Ok(())
            }
            crawl_stage::CrawlStageOutcome::DeferredNoPostCrawl => Ok(()),
        }
    }
    async fn progress(
        &self,
        context: &JobExecutionContext,
        key: &str,
        terminal: Option<ProgressTerminalState>,
    ) -> ProductionResult<()> {
        let attempt = ProgressAttemptId::new(context.attempt_id().to_owned()).map_err(|_| {
            ProductionError::progress(
                ExecutionOperation::Serialization,
                "PROGRESS_ATTEMPT_INVALID",
            )
        })?;
        let terminal_event = terminal.is_some();
        let event = match terminal {
            Some(state) => NewProgressEvent::terminal(
                context.job_id().clone(),
                state,
                ProgressMetadata::default(),
            )
            .map_err(|_| {
                ProductionError::progress(
                    ExecutionOperation::Serialization,
                    "PROGRESS_EVENT_INVALID",
                )
            })?,
            None => NewProgressEvent::new(
                context.job_id().clone(),
                ProgressKey::new(key).map_err(|_| {
                    ProductionError::progress(
                        ExecutionOperation::Serialization,
                        "PROGRESS_KEY_INVALID",
                    )
                })?,
                ProgressMetadata::default(),
            ),
        }
        .with_attempt(attempt);
        let service = ProgressService::new(&self.database);
        match &self.progress_live_hub {
            Some(hub) => match service
                .append_and_publish_at(hub, &event, epoch_seconds())
                .await
            {
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
                Err(_) => Err(ProductionError::progress(
                    ExecutionOperation::AppendProgress,
                    "PROGRESS_DURABLE_APPEND_FAILED",
                )),
            },
            None => service
                .append_at(&event, epoch_seconds())
                .await
                .map(|_| {
                    if terminal_event {
                        context.mark_terminal_progress_durable();
                    }
                })
                .map_err(|_| {
                    ProductionError::progress(
                        ExecutionOperation::AppendProgress,
                        "PROGRESS_DURABLE_APPEND_FAILED",
                    )
                }),
        }
    }
}

impl JobHandler for ProductionCrawlJobHandler {
    fn execute(
        &self,
        context: JobExecutionContext,
    ) -> impl Future<Output = Result<(), JobExecutionError>> + Send {
        let handler = self.clone();
        async move {
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
        }
    }
}

fn crawl_url_state_id(run_id: CrawlRunId, canonical_url: &str) -> String {
    let identity = format!("{run_id}:{canonical_url}");
    let digest = erabi_domain::canonical_sha256(&identity).unwrap_or_else(|_| "invalid".to_owned());
    format!("crawl:{digest}")
}

fn epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |value| {
            i64::try_from(value.as_secs()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::{ProductionDeadline, is_production_job_kind};
    use erabi_crawler::{ManualPreviewClock, PreviewClock};
    use std::{sync::Arc, time::Duration};
    use uuid::Uuid;

    #[test]
    fn discovered_url_identity_is_a_bare_uuid_v7() {
        let id = super::crawl_stage::discovered_id();
        assert_eq!(
            Uuid::parse_str(&id)
                .ok()
                .map(|parsed| parsed.get_version_num()),
            Some(7)
        );
    }

    #[test]
    fn duration_timeout_is_capped_and_expires_without_sleeping() {
        let clock = Arc::new(ManualPreviewClock::new());
        let deadline = ProductionDeadline::new(clock.clone(), 0, 500);
        assert_eq!(
            deadline.remaining_timeout(2_000),
            Some(Duration::from_millis(500))
        );
        clock.advance_millis(500);
        assert_eq!(deadline.remaining_timeout(2_000), None);
        assert_eq!(clock.now_millis(), 500);
    }

    #[test]
    fn production_recovery_kinds_are_explicitly_routed() {
        for kind in [
            "PRODUCTION_CRAWL",
            "RETRY",
            "RETRY_FAILED_PARTS",
            "RESUME_CHECKPOINT",
            "RERUN_FULL_CRAWL",
        ] {
            assert!(is_production_job_kind(kind), "{kind} must use production");
        }
        assert!(!is_production_job_kind("QUICK_SCRAPE"));
        assert!(!is_production_job_kind("RESTART_FROM_BEGINNING"));
    }
}
