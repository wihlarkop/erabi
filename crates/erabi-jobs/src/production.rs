//! Bounded execution for one frozen Production Crawl Run.
//!
//! It delegates discovery semantics to `erabi_crawler::SemanticTraversal` and
//! owns provider execution, durable evidence, progress, checkpoint-backed
//! recovery, and final status.

use serde::de::DeserializeOwned;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use erabi_crawler::{
    CrawlCheckpointV2, CrawlRecoveryPhase, CrawlerAdapter, CrawlerAdapterError,
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
        CrawlTraversalRepository, CrawlTraversalSemanticProjection, CrawlTraversalUrlSemanticState,
        CrawlUrlStateRecord, CrawlWorkState, DiscoveredUrlRecord, JobRepository,
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
};

const PRODUCTION_CRAWL_JOB_KIND: &str = "PRODUCTION_CRAWL";

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

type ExecutionProvenanceKey = (String, String);
type ExecutionProvenanceIds = BTreeMap<ExecutionProvenanceKey, String>;

fn transition_source_counts(
    run_id: CrawlRunId,
    traversal: &SemanticTraversal,
) -> Vec<CrawlTransitionSourceCount> {
    traversal
        .checkpoint_state()
        .transition_page_counts
        .into_iter()
        .map(
            |(transition_id, source_canonical_url, eligible_edge_count)| {
                CrawlTransitionSourceCount {
                    transition_id: transition_id.to_string(),
                    source_url_state_id: crawl_url_state_id(run_id, &source_canonical_url),
                    eligible_edge_count: u64::from(eligible_edge_count),
                }
            },
        )
        .collect()
}

fn semantic_projection(state: &SemanticTraversalCheckpoint) -> CrawlTraversalSemanticProjection {
    let mut urls = BTreeSet::new();
    for values in [
        &state.admitted_canonical_urls,
        &state.seen_canonical_urls,
        &state.sampled_canonical_urls,
        &state.expanded_canonical_urls,
        &state.matching_canonical_urls,
        &state.unmatched_canonical_urls,
        &state.ambiguous_canonical_urls,
        &state.in_scope_canonical_urls,
    ] {
        urls.extend(values.iter().cloned());
    }
    let url_states = urls
        .into_iter()
        .map(|canonical_url| CrawlTraversalUrlSemanticState {
            sampled: state.sampled_canonical_urls.contains(&canonical_url),
            expanded: state.expanded_canonical_urls.contains(&canonical_url),
            in_scope: state.in_scope_canonical_urls.contains(&canonical_url),
            page_type_match_state: if state.ambiguous_canonical_urls.contains(&canonical_url) {
                Some(CrawlPageTypeMatchState::Ambiguous)
            } else if state.unmatched_canonical_urls.contains(&canonical_url) {
                Some(CrawlPageTypeMatchState::Unmatched)
            } else if state.matching_canonical_urls.contains(&canonical_url) {
                Some(CrawlPageTypeMatchState::Matched)
            } else {
                None
            },
            canonical_url,
        })
        .collect();
    let mut page_type_counts = BTreeMap::<String, (u64, u64)>::new();
    for (page_type_id, count) in &state.page_type_sampled {
        page_type_counts
            .entry(page_type_id.to_string())
            .or_default()
            .0 = *count;
    }
    for (page_type_id, count) in &state.page_type_discovered {
        page_type_counts
            .entry(page_type_id.to_string())
            .or_default()
            .1 = *count;
    }
    let page_type_counts = page_type_counts
        .into_iter()
        .map(
            |(page_type_id, (sampled_count, discovered_count))| CrawlTraversalPageTypeCounts {
                page_type_id,
                sampled_count,
                discovered_count,
            },
        )
        .collect();
    CrawlTraversalSemanticProjection {
        url_states,
        page_type_counts,
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

    #[allow(clippy::too_many_lines)]
    async fn execute_inner(&self, context: JobExecutionContext) -> ProductionResult<()> {
        if !is_production_job_kind(context.kind().as_str()) {
            return Err(ProductionError::new(ExecutionDiagnostic::new(
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
                ProductionError::repository(ExecutionOperation::LoadJob, "JOB_LOAD_FAILED")
            })?;
        let run_id = job
            .crawl_run_id
            .as_deref()
            .and_then(parse_run_id)
            .ok_or_else(|| {
                ProductionError::new(ExecutionDiagnostic::new(
                    OrchestrationErrorCategory::Invariant,
                    ExecutionOperation::LoadJob,
                    ExecutionAction::Fail,
                    "RUN_ID_INVALID",
                ))
            })?;
        let snapshot = CrawlRunRepository::new(&self.database)
            .snapshot(run_id)
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "RUN_SNAPSHOT_LOAD_FAILED",
                )
            })?;
        let semantic = load_frozen_production_semantics(&self.database, &snapshot)
            .await
            .map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::LoadRunSnapshot,
                    "FROZEN_SEMANTICS_LOAD_FAILED",
                )
            })?;
        let limits = production_limits(&snapshot, &semantic.version).map_err(|()| {
            ProductionError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Invariant,
                ExecutionOperation::LoadRunSnapshot,
                ExecutionAction::Fail,
                "PRODUCTION_LIMITS_INVALID",
            ))
        })?;
        let deadline = ProductionDeadline::new(
            self.clock.clone(),
            self.clock.now_millis(),
            limits.max_duration_ms,
        );
        let executions = CrawlExecutionRepository::new(&self.database)
            .list_for_run(run_id)
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "EXECUTION_LIST_FAILED",
                )
            })?;
        let discovered = CrawlRunRepository::new(&self.database)
            .discovered_urls(run_id)
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "DISCOVERY_LOAD_FAILED",
                )
            })?;
        let checkpoint = if context.kind().as_str() == "RERUN_FULL_CRAWL" {
            None
        } else {
            JobRepository::new(&self.database)
                .latest_checkpoint_for_lineage(context.job_id())
                .await
                .map_err(|_| {
                    ProductionError::checkpoint(
                        ExecutionOperation::LoadCheckpoint,
                        "CHECKPOINT_LOAD_FAILED",
                    )
                })?
        };

        if checkpoint.is_none() && (!executions.is_empty() || !discovered.is_empty()) {
            if !durable_production_completion_without_checkpoint(&executions, &discovered) {
                // Durable discovery without a compatible recovery frontier is
                // not permission to rediscover Seeds.  This is an interrupted
                // run and must fail closed.
                return Err(ProductionError::checkpoint(
                    ExecutionOperation::LoadCheckpoint,
                    "RECOVERY_FRONTIER_MISSING",
                ));
            }
            let current_status = CrawlRunRepository::new(&self.database)
                .status(run_id)
                .await
                .map_err(|_| {
                    ProductionError::repository(
                        ExecutionOperation::LoadRunSnapshot,
                        "RUN_STATUS_LOAD_FAILED",
                    )
                })?;
            let final_status = self
                .finalize_durable_run(&context, &snapshot, run_id, current_status)
                .await?;
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
            return Ok(());
        }

        let run_repository = CrawlRunRepository::new(&self.database);
        if matches!(
            context.kind().as_str(),
            "RETRY" | "RETRY_FAILED_PARTS" | "RESUME_CHECKPOINT"
        ) {
            run_repository
                .transition_recovery_status(run_id)
                .await
                .map_err(|_| {
                    ProductionError::repository(
                        ExecutionOperation::TransitionRun,
                        "RUN_RECOVERY_TRANSITION_FAILED",
                    )
                })?;
        } else {
            run_repository
                .transition_execution_status(run_id, CrawlRunStatus::Running)
                .await
                .map_err(|_| {
                    ProductionError::repository(
                        ExecutionOperation::TransitionRun,
                        "RUN_EXECUTION_TRANSITION_FAILED",
                    )
                })?;
        }
        self.progress(
            &context,
            if checkpoint.is_some() {
                "RECOVERY_ACCEPTED"
            } else {
                "PRODUCTION_STARTED"
            },
            None,
        )
        .await?;

        let provider = Arc::new(ProductionTraversalProvider::new(
            self.clone(),
            context.clone(),
            snapshot.clone(),
            deadline,
        ));
        let mut provenance = self.load_provenance_ids(run_id).await?;
        let mut traversal = if let Some(record) = checkpoint.as_ref() {
            CrawlCheckpointV2::from_envelope(&record.checkpoint, &snapshot, run_id).map_err(
                |_| {
                    ProductionError::checkpoint(
                        ExecutionOperation::LoadCheckpoint,
                        "CHECKPOINT_INVALID",
                    )
                },
            )?;
            // A provider result can be durably committed immediately before a
            // process crash and before the following checkpoint append.  The
            // immutable execution rows win over that stale work partition.
            let recovery_selection = match context.kind().as_str() {
                "RETRY" | "RETRY_FAILED_PARTS" => {
                    // Make the action child recoverable before its durable
                    // generation selection. If the process dies after the
                    // selection transaction commits but before the next
                    // checkpoint, queue recovery can replay this same action
                    // instead of exhausting the child as checkpoint-less.
                    context.checkpoint(&record.checkpoint).await.map_err(|_| {
                        ProductionError::checkpoint(
                            ExecutionOperation::LoadCheckpoint,
                            "CHECKPOINT_PERSIST_FAILED",
                        )
                    })?;
                    let action_kind = if context.kind().as_str() == "RETRY" {
                        erabi_db::repositories::CrawlRecoveryActionKind::Retry
                    } else {
                        erabi_db::repositories::CrawlRecoveryActionKind::RetryFailedParts
                    };
                    Some(
                        CrawlTraversalRepository::new(&self.database)
                            .prepare_recovery_action(
                                context.job_id(),
                                context.attempt_id(),
                                run_id,
                                action_kind,
                                context.ownership_now(),
                            )
                            .await
                            .map_err(|_| {
                                ProductionError::repository(
                                    ExecutionOperation::QueueLifecycle,
                                    "RECOVERY_ACTION_PERSIST_FAILED",
                                )
                            })?,
                    )
                }
                _ => None,
            };
            let selected_state_ids = recovery_selection
                .as_ref()
                .map(|selection| selection.state_ids.iter().cloned().collect::<BTreeSet<_>>());
            let (durable, recovery_entries) = self
                .reconstruct_traversal_checkpoint(
                    run_id,
                    &snapshot,
                    &semantic,
                    selected_state_ids.as_ref(),
                )
                .await?;
            if recovery_selection.is_some() {
                SemanticTraversal::restore_for_recovery(
                    semantic,
                    snapshot.selected_seed_ids().to_vec(),
                    limits,
                    provider.clone(),
                    self.clock.clone(),
                    durable,
                    recovery_entries,
                )
                .map_err(|_| {
                    ProductionError::checkpoint(
                        ExecutionOperation::Serialization,
                        "TRAVERSAL_RESTORE_FAILED",
                    )
                })?
            } else {
                SemanticTraversal::restore_from_checkpoint(
                    semantic,
                    snapshot.selected_seed_ids().to_vec(),
                    limits,
                    provider.clone(),
                    self.clock.clone(),
                    durable,
                )
                .map_err(|_| {
                    ProductionError::checkpoint(
                        ExecutionOperation::Serialization,
                        "TRAVERSAL_RESTORE_FAILED",
                    )
                })?
            }
        } else {
            if !executions.is_empty()
                || !discovered.is_empty()
                || !matches!(
                    context.kind().as_str(),
                    PRODUCTION_CRAWL_JOB_KIND | "RERUN_FULL_CRAWL"
                )
            {
                return Err(ProductionError::checkpoint(
                    ExecutionOperation::LoadCheckpoint,
                    "RECOVERY_STATE_INCOMPATIBLE",
                ));
            }
            SemanticTraversal::for_frozen_snapshot(
                semantic,
                snapshot.selected_seed_ids().to_vec(),
                limits,
                provider.clone(),
                self.clock.clone(),
            )
            .map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::LoadRunSnapshot,
                    "TRAVERSAL_INITIALIZATION_FAILED",
                )
            })?
        };

        // Initialization atomically commits the exact root evidence, logical
        // work, traversal control, and compact checkpoint before dispatch.
        let mut seeds_persisted = checkpoint.is_some();
        if checkpoint.is_some() {
            self.synchronize_traversal_state(run_id, &traversal)
                .await
                .map_err(|_| {
                    ProductionError::repository(
                        ExecutionOperation::PersistExecution,
                        "TRAVERSAL_STATE_PERSIST_FAILED",
                    )
                })?;
            self.save_checkpoint(&context, &snapshot, run_id, &traversal, &provenance)
                .await?;
        } else {
            self.initialize_traversal_state(
                &context,
                &snapshot,
                run_id,
                &traversal,
                &mut provenance,
            )
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::PersistExecution,
                    "TRAVERSAL_STATE_INITIALIZATION_FAILED",
                )
            })?;
            seeds_persisted = true;
        }

        loop {
            let pending_canonical_url = traversal
                .next_pending_canonical_url()
                .map(ToOwned::to_owned);
            let expected_work_generation = if let Some(canonical_url) =
                pending_canonical_url.as_deref()
            {
                Some(
                    CrawlTraversalRepository::new(&self.database)
                        .read_work_generation(run_id, &crawl_url_state_id(run_id, canonical_url))
                        .await
                        .map_err(|_| {
                            ProductionError::repository(
                                ExecutionOperation::PersistExecution,
                                "WORK_GENERATION_LOAD_FAILED",
                            )
                        })?,
                )
            } else {
                None
            };
            match traversal.step().await.map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::Serialization,
                    "TRAVERSAL_STEP_FAILED",
                )
            })? {
                SemanticTraversalStep::Processed {
                    pages,
                    discovery_paths,
                } => {
                    let attempts = provider.take_attempts().await;
                    let seeds = if seeds_persisted {
                        Vec::new()
                    } else {
                        traversal.seed_evidence()
                    };
                    let evidence = self.collect_discovery_delta(
                        run_id,
                        &seeds,
                        &discovery_paths,
                        &pages,
                        &attempts,
                        expected_work_generation.unwrap_or(0),
                        &mut provenance,
                    )?;
                    self.synchronize_traversal_state_with_evidence(
                        run_id,
                        &traversal,
                        &evidence,
                        &pages,
                        expected_work_generation,
                    )
                    .await?;
                    seeds_persisted = true;
                    self.persist_page_delta(
                        run_id,
                        &snapshot,
                        &pages,
                        &discovery_paths,
                        &provenance,
                        attempts,
                        expected_work_generation,
                        &context,
                    )
                    .await?;
                    self.save_checkpoint(&context, &snapshot, run_id, &traversal, &provenance)
                        .await?;
                    if context.cancellation().is_cancelled() {
                        return self
                            .cancellation_boundary(&context, &snapshot, run_id)
                            .await;
                    }
                    if context.storage_pressure().is_signalled() {
                        return Ok(());
                    }
                }
                SemanticTraversalStep::Interrupted(reason) => {
                    if !seeds_persisted {
                        let seeds = traversal.seed_evidence();
                        let evidence = self.collect_discovery_delta(
                            run_id,
                            &seeds,
                            &[],
                            &[],
                            &BTreeMap::new(),
                            0,
                            &mut provenance,
                        )?;
                        self.synchronize_traversal_state_with_evidence(
                            run_id,
                            &traversal,
                            &evidence,
                            &[],
                            None,
                        )
                        .await?;
                    }
                    // The interruption itself can advance durable traversal
                    // control (notably duration/pagination structural
                    // evidence) even when it produces no page delta. Persist
                    // that semantic result before the compact checkpoint.
                    self.synchronize_traversal_state(run_id, &traversal)
                        .await
                        .map_err(|_| {
                            ProductionError::repository(
                                ExecutionOperation::PersistExecution,
                                "TRAVERSAL_STATE_PERSIST_FAILED",
                            )
                        })?;
                    self.save_checkpoint(&context, &snapshot, run_id, &traversal, &provenance)
                        .await?;
                    match reason {
                        erabi_crawler::DiscoveryPreviewInterruption::Cancelled => {
                            return self
                                .cancellation_boundary(&context, &snapshot, run_id)
                                .await;
                        }
                        erabi_crawler::DiscoveryPreviewInterruption::StoragePressure => {
                            return Ok(());
                        }
                    }
                }
                SemanticTraversalStep::Complete => {
                    if !seeds_persisted {
                        let seeds = traversal.seed_evidence();
                        let evidence = self.collect_discovery_delta(
                            run_id,
                            &seeds,
                            &[],
                            &[],
                            &BTreeMap::new(),
                            0,
                            &mut provenance,
                        )?;
                        self.synchronize_traversal_state_with_evidence(
                            run_id,
                            &traversal,
                            &evidence,
                            &[],
                            None,
                        )
                        .await?;
                    }
                    // Completion may follow a budget decision with no further
                    // page delta. The scalar control row, not the compact
                    // checkpoint, owns those final traversal facts.
                    self.synchronize_traversal_state(run_id, &traversal)
                        .await
                        .map_err(|_| {
                            ProductionError::repository(
                                ExecutionOperation::PersistExecution,
                                "TRAVERSAL_STATE_PERSIST_FAILED",
                            )
                        })?;
                    self.save_checkpoint(&context, &snapshot, run_id, &traversal, &provenance)
                        .await?;
                    break;
                }
            }
        }

        let current_status = CrawlRunRepository::new(&self.database)
            .status(run_id)
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "RUN_STATUS_LOAD_FAILED",
                )
            })?;
        let final_status = self
            .finalize_durable_run(&context, &snapshot, run_id, current_status)
            .await?;
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

    async fn cancellation_boundary(
        &self,
        context: &JobExecutionContext,
        snapshot: &CrawlRunSnapshot,
        run_id: CrawlRunId,
    ) -> ProductionResult<()> {
        self.finalize_durable_run(context, snapshot, run_id, CrawlRunStatus::Cancelled)
            .await?;
        if let Err(error) = self
            .progress(context, "CANCELLATION_SAFE_BOUNDARY", None)
            .await
        {
            context.record_secondary_diagnostics(error.diagnostics);
        }
        if let Err(error) = self
            .progress(
                context,
                "PRODUCTION_CANCELLED",
                Some(ProgressTerminalState::Cancelled),
            )
            .await
        {
            context.record_secondary_diagnostics(error.diagnostics);
        }
        Err(ProductionError::new(ExecutionDiagnostic::new(
            OrchestrationErrorCategory::Finalization,
            ExecutionOperation::FinalizeRun,
            ExecutionAction::Fail,
            "CRAWL_RUN_CANCELLED",
        )))
    }

    async fn finalize_durable_run(
        &self,
        context: &JobExecutionContext,
        snapshot: &CrawlRunSnapshot,
        run_id: CrawlRunId,
        current_status: CrawlRunStatus,
    ) -> ProductionResult<CrawlRunStatus> {
        let executions = CrawlExecutionRepository::new(&self.database)
            .list_for_run(run_id)
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::PersistExecution,
                    "EXECUTIONS_LOAD_FAILED",
                )
            })?;
        let discovered = CrawlRunRepository::new(&self.database)
            .discovered_urls(run_id)
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "DISCOVERY_LOAD_FAILED",
                )
            })?;
        let latest = JobRepository::new(&self.database)
            .latest_checkpoint_for_lineage(context.job_id())
            .await
            .map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::LoadCheckpoint,
                    "CHECKPOINT_LOAD_FAILED",
                )
            })?;
        let _checkpoint = latest
            .as_ref()
            .map(|record| CrawlCheckpointV2::from_envelope(&record.checkpoint, snapshot, run_id))
            .transpose()
            .map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::LoadCheckpoint,
                    "CHECKPOINT_INVALID",
                )
            })?;
        let durable = CrawlTraversalRepository::new(&self.database)
            .reconstruct_recovery_state(run_id)
            .await
            .map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::LoadCheckpoint,
                    "TRAVERSAL_STATE_RECONSTRUCTION_FAILED",
                )
            })?;
        let finalization = erabi_crawler::finalize_durable_state_with_traversal(
            snapshot,
            current_status,
            &executions,
            &discovered,
            None,
            Some(&durable.control),
            Some(&durable.work),
        )
        .map_err(|_| {
            ProductionError::new(
                ExecutionDiagnostic::new(
                    OrchestrationErrorCategory::Finalization,
                    ExecutionOperation::FinalizeRun,
                    ExecutionAction::Retry,
                    "CRAWL_RUN_FINALIZATION_FAILED",
                )
                .with_run(run_id),
            )
        })?;
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
            .map_err(|_| {
                ProductionError::new(
                    ExecutionDiagnostic::new(
                        OrchestrationErrorCategory::Finalization,
                        ExecutionOperation::FinalizeRun,
                        ExecutionAction::Retry,
                        "CRAWL_RUN_FINALIZATION_FAILED",
                    )
                    .with_run(run_id),
                )
            })?;
        context.mark_terminal_crawl_run(run_id, finalization.status);
        Ok(finalization.status)
    }

    async fn load_provenance_ids(
        &self,
        run_id: CrawlRunId,
    ) -> ProductionResult<ExecutionProvenanceIds> {
        let mut latest = BTreeMap::<ExecutionProvenanceKey, (u64, String)>::new();
        let records = CrawlRunRepository::new(&self.database)
            .discovered_urls(run_id)
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "PROVENANCE_LOAD_FAILED",
                )
            })?;
        for record in records {
            if matches!(record.status.as_str(), "ADMITTED" | "EXECUTION_RECONCILED")
                || record
                    .detail
                    .get("origin")
                    .and_then(serde_json::Value::as_str)
                    == Some("SEED")
            {
                let generation = record
                    .detail
                    .get("work_generation")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let key = (record.original_url, record.canonical_url);
                if latest
                    .get(&key)
                    .is_none_or(|(known_generation, _)| generation >= *known_generation)
                {
                    latest.insert(key, (generation, record.id));
                }
            }
        }
        Ok(latest.into_iter().map(|(key, (_, id))| (key, id)).collect())
    }

    /// Persists only facts already decided by the one `SemanticTraversal`. The
    /// DB sees queue ordering/provenance; it never re-evaluates selectors,
    /// canonicalization, scope, or transition eligibility.
    async fn synchronize_traversal_state(
        &self,
        run_id: CrawlRunId,
        traversal: &SemanticTraversal,
    ) -> ProductionResult<()> {
        let (work, control) = Self::durable_traversal_snapshot(run_id, traversal);
        let semantic_state = semantic_projection(&traversal.checkpoint_state());
        let transition_source_counts = transition_source_counts(run_id, traversal);
        let repository = CrawlTraversalRepository::new(&self.database);
        match repository.read_traversal_control(run_id).await {
            Ok(_) => repository
                .apply_discovery_delta_with_projection(
                    run_id,
                    &[],
                    &work,
                    &control,
                    &transition_source_counts,
                    &semantic_state,
                    &[],
                    &[],
                )
                .await
                .map_err(|_| {
                    ProductionError::repository(
                        ExecutionOperation::PersistExecution,
                        "TRAVERSAL_STATE_PERSIST_FAILED",
                    )
                }),
            // A checkpoint without its coupled traversal-control row is an
            // interrupted or pre-Task-9 state, not proof that initialization
            // completed. Do not recreate roots here: that would allow
            // recovery to proceed without the atomic Seed evidence phase.
            Err(_) => Err(ProductionError::checkpoint(
                ExecutionOperation::LoadCheckpoint,
                "TRAVERSAL_CONTROL_MISSING",
            )),
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn synchronize_traversal_state_with_evidence(
        &self,
        run_id: CrawlRunId,
        traversal: &SemanticTraversal,
        evidence: &[DiscoveredUrlRecord],
        pages: &[DiscoveryPreviewPage],
        expected_work_generation: Option<u64>,
    ) -> ProductionResult<()> {
        let (mut work, control) = Self::durable_traversal_snapshot(run_id, traversal);
        for state in &mut work {
            for record in evidence.iter().filter(|record| {
                record.status == "ADMITTED" && record.canonical_url == state.canonical_url
            }) {
                if state.first_discovered_url_id.is_none() {
                    state.first_discovered_url_id = Some(record.id.clone());
                }
                let source_state_id = record
                    .detail
                    .get("source_canonical_url")
                    .and_then(serde_json::Value::as_str)
                    .map(|source| crawl_url_state_id(run_id, source));
                if source_state_id.as_deref() == state.parent_url_state_id.as_deref() {
                    state.parent_discovered_url_id = Some(record.id.clone());
                }
            }
        }
        for record in evidence {
            let Some(reason) = preserve_reason_for_discovery_status(&record.status) else {
                continue;
            };
            if work
                .iter()
                .any(|state| state.canonical_url == record.canonical_url)
            {
                continue;
            }
            let seed_provenance = record
                .detail
                .get("seed_ids")
                .and_then(serde_json::Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect()
                })
                .unwrap_or_default();
            work.push(CrawlUrlStateRecord {
                id: crawl_url_state_id(run_id, &record.canonical_url),
                crawl_run_id: run_id,
                canonical_url: record.canonical_url.clone(),
                first_discovered_url_id: Some(record.id.clone()),
                requested_url: record.original_url.clone(),
                parent_url_state_id: None,
                parent_discovered_url_id: None,
                admission_state: CrawlAdmissionState::PreserveOnly,
                preserve_reason: Some(reason.to_owned()),
                resolved_to_url_state_id: None,
                admission_sequence: None,
                depth: None,
                target_page_type_id: None,
                transition_id: None,
                pagination: false,
                final_canonical_url: None,
                current_work_state: None,
                work_generation: 0,
                current_execution_id: None,
                seed_provenance,
                seen: true,
                sampled: false,
                expanded: false,
                in_scope: false,
                page_type_match_state: None,
            });
        }
        let repository = CrawlTraversalRepository::new(&self.database);
        let transition_source_counts = transition_source_counts(run_id, traversal);
        let redirects = pages
            .iter()
            .filter_map(|page| {
                let final_canonical_url = page.canonical_url.as_ref()?;
                (page.requested_canonical_url != *final_canonical_url).then(|| {
                    CrawlRedirectReconciliation {
                        alias_url_state_id: crawl_url_state_id(
                            run_id,
                            &page.requested_canonical_url,
                        ),
                        final_url_state_id: crawl_url_state_id(run_id, final_canonical_url),
                        alias_canonical_url: page.requested_canonical_url.clone(),
                        final_canonical_url: final_canonical_url.clone(),
                    }
                })
            })
            .collect::<Vec<_>>();
        // SemanticTraversal has already committed the observation to its
        // in-memory sets by this point. Keep that source logical unit
        // non-pending in the same discovery transaction until the guarded
        // execution write can attach its result. A sampled PENDING row would
        // be an impossible checkpoint-equivalent state: strict restore would
        // reject it, while recovery could otherwise select it again.
        // Provider failures and robots exclusions are deliberately left
        // pending; they have no sampled semantic observation and remain
        // eligible for the existing retry/recovery actions.
        let in_flight_work = pages
            .iter()
            .filter(|page| {
                !matches!(
                    page.state,
                    PreviewUrlState::ProviderError | PreviewUrlState::RobotsExcluded
                )
            })
            .map(|page| {
                let canonical_url = page
                    .canonical_url
                    .as_deref()
                    .unwrap_or(&page.requested_canonical_url);
                CrawlInFlightWork {
                    state_id: crawl_url_state_id(run_id, canonical_url),
                    expected_work_generation: (page.requested_canonical_url == canonical_url)
                        .then_some(expected_work_generation)
                        .flatten(),
                }
            })
            .collect::<Vec<_>>();
        let semantic_state = semantic_projection(&traversal.checkpoint_state());
        repository
            .apply_discovery_delta_with_projection(
                run_id,
                evidence,
                &work,
                &control,
                &transition_source_counts,
                &semantic_state,
                &redirects,
                &in_flight_work,
            )
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::PersistExecution,
                    "TRAVERSAL_STATE_PERSIST_FAILED",
                )
            })
    }

    fn durable_traversal_snapshot(
        run_id: CrawlRunId,
        traversal: &SemanticTraversal,
    ) -> (Vec<CrawlUrlStateRecord>, CrawlTraversalControl) {
        let state = traversal.checkpoint_state();
        let projection = semantic_projection(&state);
        let work = state
            .pending
            .iter()
            .map(|entry| CrawlUrlStateRecord {
                id: crawl_url_state_id(run_id, &entry.canonical_url),
                crawl_run_id: run_id,
                canonical_url: entry.canonical_url.clone(),
                first_discovered_url_id: entry.discovered_url_id.clone(),
                requested_url: entry.requested_url.clone(),
                parent_url_state_id: entry
                    .parent_canonical_url
                    .as_deref()
                    .map(|url| crawl_url_state_id(run_id, url)),
                parent_discovered_url_id: None,
                admission_state: CrawlAdmissionState::Admitted,
                preserve_reason: None,
                resolved_to_url_state_id: None,
                admission_sequence: Some(entry.order),
                depth: Some(entry.depth),
                target_page_type_id: entry.target_page_type_id.map(|id| id.to_string()),
                transition_id: entry.transition_id.map(|id| id.to_string()),
                pagination: entry.pagination,
                final_canonical_url: None,
                current_work_state: Some(CrawlWorkState::Pending),
                work_generation: 0,
                current_execution_id: None,
                seed_provenance: entry.seed_ids.iter().map(ToString::to_string).collect(),
                seen: true,
                sampled: projection
                    .url_states
                    .iter()
                    .find(|value| value.canonical_url == entry.canonical_url)
                    .is_some_and(|value| value.sampled),
                expanded: projection
                    .url_states
                    .iter()
                    .find(|value| value.canonical_url == entry.canonical_url)
                    .is_some_and(|value| value.expanded),
                in_scope: projection
                    .url_states
                    .iter()
                    .find(|value| value.canonical_url == entry.canonical_url)
                    .is_some_and(|value| value.in_scope),
                page_type_match_state: projection
                    .url_states
                    .iter()
                    .find(|value| value.canonical_url == entry.canonical_url)
                    .and_then(|value| value.page_type_match_state),
            })
            .collect::<Vec<_>>();
        let control = CrawlTraversalControl {
            crawl_run_id: run_id,
            consumed_bytes: state.consumed_bytes,
            raw_link_count: state.urls_discovered,
            duplicate_count: state.duplicates_prevented,
            robots_excluded_count: state.robots_excluded,
            provider_error_count: state.provider_errors,
            external_url_count: state.external_urls,
            blocked_url_count: state.blocked_urls,
            peak_expansion_count: state.peak_new_from_page,
            elapsed_millis: state.elapsed_millis,
            time_budget_hit: state.time_budget_hit,
            duration_work_not_expanded: state.duration_work_not_expanded,
            pagination_truncation_count: state.pagination_truncation_count,
            next_admission_sequence: state
                .newly_enqueued_urls
                .saturating_add(u64::try_from(state.selected_seed_ids.len()).unwrap_or(u64::MAX)),
        };
        (work, control)
    }

    #[allow(clippy::too_many_lines)]
    async fn initialize_traversal_state(
        &self,
        context: &JobExecutionContext,
        snapshot: &CrawlRunSnapshot,
        run_id: CrawlRunId,
        traversal: &SemanticTraversal,
        provenance: &mut ExecutionProvenanceIds,
    ) -> ProductionResult<()> {
        let state = traversal.checkpoint_state();
        let seed_evidence = self.collect_discovery_delta(
            run_id,
            &traversal.seed_evidence(),
            &[],
            &[],
            &BTreeMap::new(),
            0,
            provenance,
        )?;
        let (mut work, control) = Self::durable_traversal_snapshot(run_id, traversal);
        for record in &seed_evidence {
            let seed_ids = record
                .detail
                .get("seed_ids")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>();
            if record.status == "ADMITTED" {
                if let Some(state) = work
                    .iter_mut()
                    .find(|state| state.canonical_url == record.canonical_url)
                {
                    if state.first_discovered_url_id.is_none() {
                        state.first_discovered_url_id = Some(record.id.clone());
                    }
                    for seed_id in seed_ids {
                        if !state.seed_provenance.contains(&seed_id) {
                            state.seed_provenance.push(seed_id);
                        }
                    }
                }
            } else if let Some(reason) = preserve_reason_for_discovery_status(&record.status) {
                if let Some(state) = work
                    .iter_mut()
                    .find(|state| state.canonical_url == record.canonical_url)
                {
                    for seed_id in seed_ids {
                        if !state.seed_provenance.contains(&seed_id) {
                            state.seed_provenance.push(seed_id);
                        }
                    }
                } else {
                    work.push(CrawlUrlStateRecord {
                        id: crawl_url_state_id(run_id, &record.canonical_url),
                        crawl_run_id: run_id,
                        canonical_url: record.canonical_url.clone(),
                        first_discovered_url_id: Some(record.id.clone()),
                        requested_url: record.original_url.clone(),
                        parent_url_state_id: None,
                        parent_discovered_url_id: None,
                        admission_state: CrawlAdmissionState::PreserveOnly,
                        preserve_reason: Some(reason.to_owned()),
                        resolved_to_url_state_id: None,
                        admission_sequence: None,
                        depth: None,
                        target_page_type_id: None,
                        transition_id: None,
                        pagination: false,
                        final_canonical_url: None,
                        current_work_state: None,
                        work_generation: 0,
                        current_execution_id: None,
                        seed_provenance: seed_ids,
                        seen: true,
                        sampled: false,
                        expanded: false,
                        in_scope: false,
                        page_type_match_state: None,
                    });
                }
            }
        }
        let checkpoint = CrawlCheckpointV2::new(run_id, snapshot, CrawlRecoveryPhase::Traversing)
            .map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::Serialization,
                    "CHECKPOINT_BUILD_FAILED",
                )
            })?
            .to_envelope()
            .map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::Serialization,
                    "CHECKPOINT_ENVELOPE_FAILED",
                )
            })?;
        let (job_id, attempt_id, lease, created_at) =
            context.checkpoint_lineage().await.map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::LoadCheckpoint,
                    "CHECKPOINT_LINEAGE_LOAD_FAILED",
                )
            })?;
        CrawlTraversalRepository::new(&self.database)
            .initialize_run_state_with_checkpoint_and_evidence(
                run_id,
                &work,
                &seed_evidence,
                &control,
                &semantic_projection(&state),
                &job_id,
                &attempt_id,
                &lease,
                &checkpoint,
                created_at,
            )
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::PersistExecution,
                    "TRAVERSAL_STATE_INITIALIZATION_FAILED",
                )
            })?;
        context.mark_checkpoint_persisted();
        self.progress(context, "CHECKPOINT_SAVED", None).await
    }

    #[allow(clippy::too_many_lines)]
    async fn reconstruct_traversal_checkpoint(
        &self,
        run_id: CrawlRunId,
        snapshot: &CrawlRunSnapshot,
        semantic: &erabi_db::repositories::CrawlerSemanticSnapshot,
        selected_state_ids: Option<&BTreeSet<String>>,
    ) -> ProductionResult<(
        SemanticTraversalCheckpoint,
        Vec<SemanticTraversalQueueEntry>,
    )> {
        let durable = CrawlTraversalRepository::new(&self.database)
            .reconstruct_recovery_state(run_id)
            .await
            .map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::LoadCheckpoint,
                    "TRAVERSAL_STATE_RECONSTRUCTION_FAILED",
                )
            })?;
        emit(SemanticEvent::RecoveryReconstructed {
            context: crate::telemetry_id(&run_id.to_string())
                .map_or(erabi_observability::CorrelationContext::new(), |id| {
                    erabi_observability::CorrelationContext::new().with_crawl_run_id(id)
                }),
            action: erabi_observability::RecoveryAction::Reconstructed,
            generation: 0,
            recovered_count: u64::try_from(durable.work.len()).unwrap_or(u64::MAX),
            outcome: EventOutcome::Reconstructed,
        });
        let checkpoint_error =
            |code| ProductionError::checkpoint(ExecutionOperation::LoadCheckpoint, code);
        let queue_entry = |work: &CrawlUrlStateRecord| -> Result<_, ProductionError> {
            Ok(SemanticTraversalQueueEntry {
                requested_url: work.requested_url.clone(),
                canonical_url: work.canonical_url.clone(),
                depth: work.depth.ok_or_else(|| {
                    ProductionError::checkpoint(
                        ExecutionOperation::LoadCheckpoint,
                        "TRAVERSAL_DEPTH_MISSING",
                    )
                })?,
                seed_ids: work
                    .seed_provenance
                    .iter()
                    .map(|id| {
                        decode_id(id).map_err(|()| {
                            ProductionError::checkpoint(
                                ExecutionOperation::LoadCheckpoint,
                                "TRAVERSAL_SEED_ID_INVALID",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                target_page_type_id: work
                    .target_page_type_id
                    .as_deref()
                    .map(|id| {
                        decode_id(id).map_err(|()| {
                            ProductionError::checkpoint(
                                ExecutionOperation::LoadCheckpoint,
                                "TRAVERSAL_PAGE_TYPE_ID_INVALID",
                            )
                        })
                    })
                    .transpose()?,
                transition_id: work
                    .transition_id
                    .as_deref()
                    .map(|id| {
                        decode_id(id)
                            .map_err(|()| checkpoint_error("TRAVERSAL_TRANSITION_ID_INVALID"))
                    })
                    .transpose()?,
                parent_canonical_url: work
                    .parent_url_state_id
                    .as_deref()
                    .and_then(|parent| durable.work.iter().find(|candidate| candidate.id == parent))
                    .map(|parent| parent.canonical_url.clone()),
                pagination: work.pagination,
                discovered_url_id: work.first_discovered_url_id.clone(),
                order: work.admission_sequence.ok_or_else(|| {
                    ProductionError::checkpoint(
                        ExecutionOperation::LoadCheckpoint,
                        "TRAVERSAL_ADMISSION_SEQUENCE_MISSING",
                    )
                })?,
            })
        };
        let pending = durable
            .work
            .iter()
            .filter(|work| {
                work.current_work_state == Some(CrawlWorkState::Pending)
                    && selected_state_ids.is_none_or(|ids| ids.contains(&work.id))
            })
            .map(queue_entry)
            .collect::<Result<Vec<_>, ProductionError>>()?;
        let recovery_entries = selected_state_ids
            .map(|ids| {
                durable
                    .work
                    .iter()
                    .filter(|work| {
                        ids.contains(&work.id)
                            && work.current_work_state != Some(CrawlWorkState::Completed)
                    })
                    .map(queue_entry)
                    .collect::<Result<Vec<_>, ProductionError>>()
            })
            .transpose()?
            .unwrap_or_default();
        let seen = durable
            .work
            .iter()
            .filter(|work| work.seen)
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let admitted = durable
            .work
            .iter()
            .filter(|work| work.admission_state == CrawlAdmissionState::Admitted)
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let sampled = durable
            .work
            .iter()
            .filter(|work| work.sampled)
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let expanded = durable
            .work
            .iter()
            .filter(|work| work.expanded)
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let matching = durable
            .work
            .iter()
            .filter(|work| work.page_type_match_state == Some(CrawlPageTypeMatchState::Matched))
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let unmatched = durable
            .work
            .iter()
            .filter(|work| work.page_type_match_state == Some(CrawlPageTypeMatchState::Unmatched))
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let ambiguous = durable
            .work
            .iter()
            .filter(|work| work.page_type_match_state == Some(CrawlPageTypeMatchState::Ambiguous))
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let in_scope = durable
            .work
            .iter()
            .filter(|work| work.in_scope)
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let mut transition_page_counts = Vec::new();
        let mut transition_counts = semantic
            .transitions
            .iter()
            .map(|transition| {
                (
                    transition.transition.id.to_string(),
                    (transition.transition.id, 0_u64, BTreeSet::new()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for count in &durable.transition_source_counts {
            let source = durable
                .work
                .iter()
                .find(|work| work.id == count.source_url_state_id)
                .ok_or_else(|| checkpoint_error("TRAVERSAL_SOURCE_STATE_MISSING"))?;
            let transition_id = decode_id(&count.transition_id)
                .map_err(|()| checkpoint_error("TRAVERSAL_TRANSITION_ID_INVALID"))?;
            transition_page_counts.push((
                transition_id,
                source.canonical_url.clone(),
                u32::try_from(count.eligible_edge_count)
                    .map_err(|_| checkpoint_error("TRAVERSAL_EDGE_COUNT_INVALID"))?,
            ));
            let entry = transition_counts
                .entry(count.transition_id.clone())
                .or_insert((transition_id, 0, BTreeSet::new()));
            entry.1 = entry.1.saturating_add(count.eligible_edge_count);
            entry.2.insert(source.canonical_url.clone());
        }
        let transition_counts = semantic
            .transitions
            .iter()
            .map(|transition| {
                let (_, eligible_edges, source_pages) = transition_counts
                    .remove(&transition.transition.id.to_string())
                    .ok_or_else(|| checkpoint_error("TRAVERSAL_TRANSITION_COUNT_MISSING"))?;
                Ok(SemanticTraversalTransitionState {
                    transition_id: transition.transition.id,
                    name: transition.transition.name.clone(),
                    eligible_edges,
                    source_pages: source_pages.into_iter().collect(),
                })
            })
            .collect::<Result<Vec<_>, ProductionError>>()?;
        let mut page_type_scheduled = BTreeMap::<String, u64>::new();
        for state in &durable.work {
            if state.admission_state == CrawlAdmissionState::Admitted
                && let Some(page_type_id) = state.target_page_type_id.as_deref()
            {
                *page_type_scheduled
                    .entry(page_type_id.to_owned())
                    .or_default() += 1;
            }
        }
        let page_type_sampled = durable
            .page_type_counts
            .iter()
            .map(|count| {
                Ok((
                    decode_id(count.page_type_id.as_str())
                        .map_err(|()| checkpoint_error("TRAVERSAL_PAGE_TYPE_ID_INVALID"))?,
                    count.sampled_count,
                ))
            })
            .collect::<Result<Vec<_>, ProductionError>>()?;
        let page_type_discovered = durable
            .page_type_counts
            .iter()
            .map(|count| {
                Ok((
                    decode_id(count.page_type_id.as_str())
                        .map_err(|()| checkpoint_error("TRAVERSAL_PAGE_TYPE_ID_INVALID"))?,
                    count.discovered_count,
                ))
            })
            .collect::<Result<Vec<_>, ProductionError>>()?;
        let page_type_scheduled = page_type_scheduled
            .into_iter()
            .map(|(id, count)| {
                Ok((
                    decode_id(id.as_str())
                        .map_err(|()| checkpoint_error("TRAVERSAL_PAGE_TYPE_ID_INVALID"))?,
                    count,
                ))
            })
            .collect::<Result<Vec<_>, ProductionError>>()?;
        Ok((
            SemanticTraversalCheckpoint {
                selected_seed_ids: snapshot.selected_seed_ids().to_vec(),
                pending,
                admitted_canonical_urls: admitted.clone(),
                seen_canonical_urls: seen,
                sampled_canonical_urls: sampled.clone(),
                expanded_canonical_urls: expanded,
                matching_canonical_urls: matching,
                unmatched_canonical_urls: unmatched,
                ambiguous_canonical_urls: ambiguous,
                in_scope_canonical_urls: in_scope,
                consumed_bytes: durable.control.consumed_bytes,
                pages_sampled: u64::try_from(sampled.len())
                    .map_err(|_| checkpoint_error("TRAVERSAL_SAMPLE_COUNT_INVALID"))?,
                urls_discovered: durable.control.raw_link_count,
                duplicates_prevented: durable.control.duplicate_count,
                robots_excluded: durable.control.robots_excluded_count,
                provider_errors: durable.control.provider_error_count,
                external_urls: durable.control.external_url_count,
                blocked_urls: durable.control.blocked_url_count,
                newly_enqueued_urls: u64::try_from(admitted.len())
                    .map_err(|_| checkpoint_error("TRAVERSAL_ADMISSION_COUNT_INVALID"))?,
                peak_new_from_page: durable.control.peak_expansion_count,
                time_budget_hit: durable.control.time_budget_hit,
                pagination_truncation_count: durable.control.pagination_truncation_count,
                duration_work_not_expanded: durable.control.duration_work_not_expanded,
                page_type_sampled,
                page_type_discovered,
                page_type_scheduled,
                transition_counts,
                transition_page_counts,
                elapsed_millis: durable.control.elapsed_millis,
            },
            recovery_entries,
        ))
    }

    async fn save_checkpoint(
        &self,
        context: &JobExecutionContext,
        snapshot: &CrawlRunSnapshot,
        run_id: CrawlRunId,
        _traversal: &SemanticTraversal,
        _provenance: &ExecutionProvenanceIds,
    ) -> ProductionResult<()> {
        let checkpoint = CrawlCheckpointV2::new(run_id, snapshot, CrawlRecoveryPhase::Traversing)
            .map_err(|_| {
            ProductionError::checkpoint(
                ExecutionOperation::Serialization,
                "CHECKPOINT_BUILD_FAILED",
            )
        })?;
        let envelope = checkpoint.to_envelope().map_err(|_| {
            ProductionError::checkpoint(
                ExecutionOperation::Serialization,
                "CHECKPOINT_ENVELOPE_FAILED",
            )
        })?;
        context.checkpoint(&envelope).await.map_err(|_| {
            ProductionError::checkpoint(
                ExecutionOperation::LoadCheckpoint,
                "CHECKPOINT_PERSIST_FAILED",
            )
        })?;
        emit(SemanticEvent::CheckpointPersisted {
            context: crate::telemetry_crawl_context(context, Some(&run_id.to_string()), None),
            version: checkpoint.payload_version,
            phase: crate::telemetry_checkpoint_phase(CrawlRecoveryPhase::Traversing),
            bytes: envelope
                .payload
                .as_ref()
                .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
            work_generation: 0,
            outcome: EventOutcome::Durable,
        });
        self.progress(context, "CHECKPOINT_SAVED", None).await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn collect_discovery_delta(
        &self,
        run_id: CrawlRunId,
        seeds: &[DiscoveryPreviewSeed],
        paths: &[DiscoveryPath],
        pages: &[DiscoveryPreviewPage],
        attempts: &BTreeMap<String, ProductionPageAttempt>,
        work_generation: u64,
        ids: &mut ExecutionProvenanceIds,
    ) -> ProductionResult<Vec<DiscoveredUrlRecord>> {
        let mut evidence = Vec::new();
        for seed in seeds {
            let original_url = fragment_free_fetch_url(&seed.requested_url).map_err(|()| {
                ProductionError::projection(
                    ExecutionOperation::Serialization,
                    "DISCOVERY_URL_SERIALIZATION_FAILED",
                )
            })?;
            let status = if seed.duplicate_of_canonical_url.is_some() {
                "CANONICAL_DUPLICATE"
            } else if seed.state == PreviewUrlState::InScopeMatched {
                "ADMITTED"
            } else {
                discovery_seed_status(seed.state)
            };
            let detail = serde_json::json!({
                "origin": "SEED",
                "work_generation": work_generation,
                "seed_ids": [seed.seed_id.to_string()],
                "entry_page_type_hint": seed.entry_page_type_hint.map(|id| id.to_string()),
                "duplicate_of_canonical_url": seed.duplicate_of_canonical_url,
                "scope": seed.scope,
                "page_type_match": seed.page_type_match,
                "budget_hits": seed.budget_hits,
            });
            let id = semantic_discovered_id(
                run_id,
                "SEED",
                work_generation,
                &(
                    seed.seed_id.to_string(),
                    seed.canonical_url.clone(),
                    status,
                    &detail,
                ),
            )
            .map_err(|()| {
                ProductionError::projection(
                    ExecutionOperation::Serialization,
                    "DISCOVERY_ID_BUILD_FAILED",
                )
            })?;
            evidence.push(DiscoveredUrlRecord {
                id: id.clone(),
                crawl_run_id: run_id,
                source_id: None,
                raw_href: None,
                original_url: original_url.clone(),
                canonical_url: seed.canonical_url.clone(),
                status: status.to_owned(),
                discovered_at: discovered_at(
                    attempts,
                    &seed.requested_url,
                    self.clock.now_millis(),
                ),
                detail,
            });
            if seed.duplicate_of_canonical_url.is_none() {
                insert_execution_provenance(ids, original_url, seed.canonical_url.clone(), id);
            }
        }
        for path in paths {
            let canonical_url = path
                .canonical_url
                .clone()
                .unwrap_or_else(|| path.source_canonical_url.clone());
            let resolved_original_url = path
                .resolved_original_url
                .clone()
                .unwrap_or_else(|| path.source_canonical_url.clone());
            let original_url = fragment_free_fetch_url(&resolved_original_url).map_err(|()| {
                ProductionError::projection(
                    ExecutionOperation::Serialization,
                    "DISCOVERY_URL_SERIALIZATION_FAILED",
                )
            })?;
            let status = discovery_status(path);
            let detail = serde_json::json!({
                "seed_ids": path.seed_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "work_generation": work_generation,
                "source_requested_url": path.source_requested_url,
                "source_final_url": path.source_final_url,
                "source_canonical_url": path.source_canonical_url,
                "source_depth": path.source_depth,
                "selector": path.selector,
                "resolved_observation_url": path.resolved_original_url,
                "duplicate_of_canonical_url": path.duplicate_of_canonical_url,
                "transition_evaluations": path.transition_evaluations,
                "budget_hits": path.budget_hits,
            });
            let id = semantic_discovered_id(
                run_id,
                "DISCOVERY_PATH",
                work_generation,
                &(
                    path.source_requested_url.clone(),
                    path.source_canonical_url.clone(),
                    path.raw_href.clone(),
                    path.selector.clone(),
                    original_url.clone(),
                    canonical_url.clone(),
                    status,
                    &detail,
                ),
            )
            .map_err(|()| {
                ProductionError::projection(
                    ExecutionOperation::Serialization,
                    "DISCOVERY_ID_BUILD_FAILED",
                )
            })?;
            evidence.push(DiscoveredUrlRecord {
                id: id.clone(),
                crawl_run_id: run_id,
                source_id: None,
                raw_href: Some(path.raw_href.clone()),
                original_url: original_url.clone(),
                canonical_url: canonical_url.clone(),
                status: status.to_owned(),
                discovered_at: discovered_at(
                    attempts,
                    &path.source_requested_url,
                    self.clock.now_millis(),
                ),
                detail,
            });
            if status == "ADMITTED" {
                insert_execution_provenance(ids, original_url, canonical_url.clone(), id);
            }
        }
        for page in pages {
            let canonical_url = page
                .canonical_url
                .clone()
                .unwrap_or_else(|| page.requested_url.clone());
            let original_url = fragment_free_fetch_url(&page.requested_url).map_err(|()| {
                ProductionError::projection(
                    ExecutionOperation::Serialization,
                    "DISCOVERY_URL_SERIALIZATION_FAILED",
                )
            })?;
            let key = (original_url.clone(), canonical_url.clone());
            if !ids.contains_key(&key) {
                let detail = serde_json::json!({
                    "origin": "EXECUTION_RECONCILIATION",
                    "work_generation": work_generation,
                    "requested_url": page.requested_url,
                    "observed_final_url": page.final_url,
                    "authoritative_canonical_url": canonical_url.clone(),
                    "seed_ids": page.seed_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                });
                let id = semantic_discovered_id(
                    run_id,
                    "EXECUTION_RECONCILIATION",
                    work_generation,
                    &(original_url.clone(), canonical_url.clone(), &detail),
                )
                .map_err(|()| {
                    ProductionError::projection(
                        ExecutionOperation::Serialization,
                        "DISCOVERY_ID_BUILD_FAILED",
                    )
                })?;
                evidence.push(DiscoveredUrlRecord {
                    id: id.clone(),
                    crawl_run_id: run_id,
                    source_id: None,
                    raw_href: None,
                    original_url: original_url.clone(),
                    canonical_url: canonical_url.clone(),
                    status: "EXECUTION_RECONCILED".to_owned(),
                    discovered_at: discovered_at(
                        attempts,
                        &page.requested_url,
                        self.clock.now_millis(),
                    ),
                    detail,
                });
                insert_execution_provenance(ids, original_url, canonical_url.clone(), id);
            }
            if page.state == PreviewUrlState::AmbiguousPageType {
                let detail = serde_json::json!({
                    "origin": "PAGE_TYPE_EVALUATION",
                    "work_generation": work_generation,
                    "requested_url": page.requested_url,
                });
                let ambiguity_id = semantic_discovered_id(
                    run_id,
                    "PAGE_TYPE_EVALUATION",
                    work_generation,
                    &(page.requested_url.clone(), canonical_url.clone(), &detail),
                )
                .map_err(|()| {
                    ProductionError::projection(
                        ExecutionOperation::Serialization,
                        "DISCOVERY_ID_BUILD_FAILED",
                    )
                })?;
                evidence.push(DiscoveredUrlRecord {
                    id: ambiguity_id,
                    crawl_run_id: run_id,
                    source_id: None,
                    raw_href: None,
                    original_url: page.requested_url.clone(),
                    canonical_url: page
                        .canonical_url
                        .clone()
                        .unwrap_or_else(|| page.requested_url.clone()),
                    status: "AMBIGUOUS_PAGE_TYPE".to_owned(),
                    discovered_at: discovered_at(
                        attempts,
                        &page.requested_url,
                        self.clock.now_millis(),
                    ),
                    detail,
                });
            }
        }
        Ok(evidence)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn persist_page_delta(
        &self,
        run_id: CrawlRunId,
        snapshot: &CrawlRunSnapshot,
        pages: &[DiscoveryPreviewPage],
        paths: &[DiscoveryPath],
        discovered_ids: &ExecutionProvenanceIds,
        mut attempts: BTreeMap<String, ProductionPageAttempt>,
        expected_work_generation: Option<u64>,
        context: &JobExecutionContext,
    ) -> ProductionResult<()> {
        for page in pages {
            let attempt = attempts.remove(&page.requested_url).ok_or_else(|| {
                ProductionError::new(ExecutionDiagnostic::new(
                    OrchestrationErrorCategory::Invariant,
                    ExecutionOperation::PersistExecution,
                    ExecutionAction::Fail,
                    "PAGE_ATTEMPT_MISSING",
                ))
            })?;
            match attempt {
                ProductionPageAttempt::Observed { page: result, .. } => {
                    let canonical_url = page
                        .canonical_url
                        .clone()
                        .unwrap_or_else(|| result.observation.requested_url.clone());
                    let page_type_id = page
                        .page_type_match
                        .as_ref()
                        .and_then(|evidence| evidence.winner.as_ref())
                        .map(|winner| winner.page_type_id);
                    let page_is_partial = matches!(
                        result.completeness,
                        CrawlerResultCompleteness::Partial { .. }
                    );
                    let artifacts = self
                        .persist_artifacts(
                            context,
                            run_id,
                            snapshot.created_at(),
                            result.artifacts.clone(),
                            snapshot.settings().retain_artifacts.value,
                        )
                        .await?;
                    let expected_generation = self
                        .expected_generation_for_page(
                            run_id,
                            page,
                            &canonical_url,
                            expected_work_generation,
                        )
                        .await?;
                    self.persist_execution(
                        CrawlExecutionRecord {
                            id: CrawlExecutionId::new(),
                            crawl_run_id: run_id,
                            requested_url: page.requested_url.clone(),
                            canonical_url: canonical_url.clone(),
                            observed_final_url: result.observation.final_url.clone(),
                            source_id: None,
                            page_type_id,
                            transition_id: page_type_id
                                .and_then(|id| transition_for_paths(&canonical_url, id, paths)),
                            discovered_url_id: discovered_ids
                                .get(&(page.requested_url.clone(), canonical_url.clone()))
                                .cloned(),
                            outcome: if page_is_partial {
                                CrawlExecutionOutcome::Partial
                            } else {
                                CrawlExecutionOutcome::Completed
                            },
                            error_code: page_is_partial
                                .then_some(CrawlExecutionErrorCode::PartialResult),
                            http_status: result.status,
                            media_type: result.media_type.clone(),
                            content_length_bytes: result.content_length,
                            provider_elapsed_ms: result.elapsed_ms,
                            artifacts,
                        },
                        context,
                        Some(expected_generation),
                        page.requested_canonical_url != canonical_url,
                    )
                    .await?;
                    self.progress(
                        context,
                        if page_is_partial {
                            "PAGE_PARTIAL"
                        } else {
                            "PAGE_COMPLETED"
                        },
                        None,
                    )
                    .await?;
                }
                ProductionPageAttempt::Failed { failure, .. } => {
                    let canonical_url = page
                        .canonical_url
                        .clone()
                        .unwrap_or_else(|| page.requested_url.clone());
                    let expected_generation = self
                        .expected_generation_for_page(
                            run_id,
                            page,
                            &canonical_url,
                            expected_work_generation,
                        )
                        .await?;
                    self.persist_execution(
                        CrawlExecutionRecord {
                            id: CrawlExecutionId::new(),
                            crawl_run_id: run_id,
                            requested_url: page.requested_url.clone(),
                            canonical_url: canonical_url.clone(),
                            observed_final_url: None,
                            source_id: None,
                            page_type_id: None,
                            transition_id: None,
                            discovered_url_id: discovered_ids
                                .get(&(page.requested_url.clone(), canonical_url.clone()))
                                .cloned(),
                            outcome: if failure.code == CrawlExecutionErrorCode::Cancelled {
                                CrawlExecutionOutcome::Cancelled
                            } else {
                                CrawlExecutionOutcome::Failed
                            },
                            error_code: Some(failure.code),
                            http_status: failure.status,
                            media_type: None,
                            content_length_bytes: None,
                            provider_elapsed_ms: None,
                            artifacts: Vec::new(),
                        },
                        context,
                        Some(expected_generation),
                        page.requested_canonical_url != canonical_url,
                    )
                    .await?;
                    self.progress(context, "PAGE_FAILED", None).await?;
                }
            }
        }
        if attempts.is_empty() {
            Ok(())
        } else {
            Err(ProductionError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Invariant,
                ExecutionOperation::PersistExecution,
                ExecutionAction::Fail,
                "PAGE_ATTEMPTS_UNCONSUMED",
            )))
        }
    }

    #[allow(dead_code, clippy::too_many_arguments, clippy::too_many_lines)]
    async fn persist_page_attempts(
        &self,
        run_id: CrawlRunId,
        snapshot: &CrawlRunSnapshot,
        traversal: &DiscoveryPreviewResult,
        discovered_ids: &ExecutionProvenanceIds,
        attempts: &mut BTreeMap<String, ProductionPageAttempt>,
        context: &JobExecutionContext,
    ) -> ProductionResult<PageAttemptCounts> {
        let mut counts = PageAttemptCounts::default();
        for page in &traversal.pages {
            let attempt = attempts.remove(&page.requested_url).ok_or_else(|| {
                ProductionError::new(ExecutionDiagnostic::new(
                    OrchestrationErrorCategory::Invariant,
                    ExecutionOperation::PersistExecution,
                    ExecutionAction::Fail,
                    "PAGE_ATTEMPT_MISSING",
                ))
            })?;
            counts.attempted = counts.attempted.saturating_add(1);
            match attempt {
                ProductionPageAttempt::Observed { page: result, .. } => {
                    let canonical_url = page
                        .canonical_url
                        .clone()
                        .unwrap_or_else(|| result.observation.requested_url.clone());
                    let page_type_id = page
                        .page_type_match
                        .as_ref()
                        .and_then(|evidence| evidence.winner.as_ref())
                        .map(|winner| winner.page_type_id);
                    let transition_id =
                        page_type_id.and_then(|id| transition_for(&canonical_url, id, traversal));
                    let artifacts = self
                        .persist_artifacts(
                            context,
                            run_id,
                            snapshot.created_at(),
                            result.artifacts.clone(),
                            snapshot.settings().retain_artifacts.value,
                        )
                        .await?;
                    let page_is_partial = matches!(
                        result.completeness,
                        CrawlerResultCompleteness::Partial { .. }
                    );
                    self.persist_execution(
                        CrawlExecutionRecord {
                            id: CrawlExecutionId::new(),
                            crawl_run_id: run_id,
                            requested_url: page.requested_url.clone(),
                            canonical_url: canonical_url.clone(),
                            observed_final_url: result.observation.final_url.clone(),
                            source_id: None,
                            page_type_id,
                            transition_id,
                            discovered_url_id: discovered_ids
                                .get(&(page.requested_url.clone(), canonical_url.clone()))
                                .cloned(),
                            outcome: if page_is_partial {
                                CrawlExecutionOutcome::Partial
                            } else {
                                CrawlExecutionOutcome::Completed
                            },
                            error_code: page_is_partial
                                .then_some(CrawlExecutionErrorCode::PartialResult),
                            http_status: result.status,
                            media_type: result.media_type.clone(),
                            content_length_bytes: result.content_length,
                            provider_elapsed_ms: result.elapsed_ms,
                            artifacts,
                        },
                        context,
                        None,
                        false,
                    )
                    .await?;
                    counts.completed = counts.completed.saturating_add(1);
                    if page_is_partial {
                        counts.unresolved_partial_work =
                            counts.unresolved_partial_work.saturating_add(1);
                    }
                    self.progress(
                        context,
                        if page_is_partial {
                            "PAGE_PARTIAL"
                        } else {
                            "PAGE_COMPLETED"
                        },
                        None,
                    )
                    .await?;
                }
                ProductionPageAttempt::Failed { failure, .. } => {
                    let canonical_url = page
                        .canonical_url
                        .clone()
                        .unwrap_or_else(|| page.requested_url.clone());
                    self.persist_execution(
                        CrawlExecutionRecord {
                            id: CrawlExecutionId::new(),
                            crawl_run_id: run_id,
                            requested_url: page.requested_url.clone(),
                            canonical_url: canonical_url.clone(),
                            observed_final_url: None,
                            source_id: None,
                            page_type_id: None,
                            transition_id: None,
                            discovered_url_id: discovered_ids
                                .get(&(page.requested_url.clone(), canonical_url.clone()))
                                .cloned(),
                            outcome: CrawlExecutionOutcome::Failed,
                            error_code: Some(failure.code),
                            http_status: failure.status,
                            media_type: None,
                            content_length_bytes: None,
                            provider_elapsed_ms: None,
                            artifacts: Vec::new(),
                        },
                        context,
                        None,
                        false,
                    )
                    .await?;
                    counts.unresolved_partial_work =
                        counts.unresolved_partial_work.saturating_add(1);
                    self.progress(context, "PAGE_FAILED", None).await?;
                }
            }
        }
        attempts.is_empty().then_some(counts).ok_or_else(|| {
            ProductionError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Invariant,
                ExecutionOperation::PersistExecution,
                ExecutionAction::Fail,
                "PAGE_ATTEMPTS_UNCONSUMED",
            ))
        })
    }

    async fn expected_generation_for_page(
        &self,
        run_id: CrawlRunId,
        page: &DiscoveryPreviewPage,
        canonical_url: &str,
        expected_work_generation: Option<u64>,
    ) -> ProductionResult<u64> {
        if page.requested_canonical_url == canonical_url {
            expected_work_generation.ok_or_else(|| {
                ProductionError::new(ExecutionDiagnostic::new(
                    OrchestrationErrorCategory::Invariant,
                    ExecutionOperation::PersistExecution,
                    ExecutionAction::Fail,
                    "WORK_GENERATION_MISSING",
                ))
            })
        } else {
            CrawlTraversalRepository::new(&self.database)
                .read_work_generation(run_id, &crawl_url_state_id(run_id, canonical_url))
                .await
                .map_err(|_| {
                    ProductionError::repository(
                        ExecutionOperation::PersistExecution,
                        "WORK_GENERATION_LOAD_FAILED",
                    )
                })
        }
    }

    #[allow(dead_code)]
    async fn persist_discovery_paths(
        &self,
        run_id: CrawlRunId,
        traversal: &DiscoveryPreviewResult,
        attempts: &BTreeMap<String, ProductionPageAttempt>,
    ) -> Result<ExecutionProvenanceIds, ()> {
        let repository = CrawlRunRepository::new(&self.database);
        let mut execution_ids = BTreeMap::new();
        for seed in &traversal.seeds {
            let id = discovered_id();
            let original_url = fragment_free_fetch_url(&seed.requested_url)?;
            let status = if seed.duplicate_of_canonical_url.is_some() {
                "CANONICAL_DUPLICATE"
            } else if seed.state == PreviewUrlState::InScopeMatched {
                "ADMITTED"
            } else {
                discovery_seed_status(seed.state)
            };
            repository
                .record_discovered_url(&DiscoveredUrlRecord {
                    id: id.clone(),
                    crawl_run_id: run_id,
                    source_id: None,
                    raw_href: None,
                    original_url: original_url.clone(),
                    canonical_url: seed.canonical_url.clone(),
                    status: status.to_owned(),
                    discovered_at: discovered_at(
                        attempts,
                        &seed.requested_url,
                        self.clock.now_millis(),
                    ),
                    detail: serde_json::json!({
                        "origin": "SEED",
                        "seed_ids": [seed.seed_id.to_string()],
                        "entry_page_type_hint": seed.entry_page_type_hint.map(|id| id.to_string()),
                        "duplicate_of_canonical_url": seed.duplicate_of_canonical_url,
                        "scope": seed.scope,
                        "page_type_match": seed.page_type_match,
                        "budget_hits": seed.budget_hits,
                    }),
                })
                .await
                .map_err(|_| ())?;
            if seed.duplicate_of_canonical_url.is_none() {
                insert_execution_provenance(
                    &mut execution_ids,
                    original_url,
                    seed.canonical_url.clone(),
                    id,
                );
            }
        }
        for path in &traversal.discovery_paths {
            let id = discovered_id();
            let canonical_url = path
                .canonical_url
                .clone()
                .unwrap_or_else(|| path.source_canonical_url.clone());
            let resolved_original_url = path
                .resolved_original_url
                .clone()
                .unwrap_or_else(|| path.source_canonical_url.clone());
            let original_url = fragment_free_fetch_url(&resolved_original_url)?;
            let status = discovery_status(path);
            repository
                .record_discovered_url(&DiscoveredUrlRecord {
                    id: id.clone(),
                    crawl_run_id: run_id,
                    source_id: None,
                    raw_href: Some(path.raw_href.clone()),
                    original_url: original_url.clone(),
                    canonical_url: canonical_url.clone(),
                    status: status.to_owned(),
                    // This is captured with the observed source page rather
                    // than copying frozen run submission metadata.
                    discovered_at: discovered_at(
                        attempts,
                        &path.source_requested_url,
                        self.clock.now_millis(),
                    ),
                    detail: serde_json::json!({
                        "seed_ids": path.seed_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                        "source_requested_url": path.source_requested_url,
                        "source_final_url": path.source_final_url,
                        "source_canonical_url": path.source_canonical_url,
                        "source_depth": path.source_depth,
                        "selector": path.selector,
                        "resolved_observation_url": path.resolved_original_url,
                        "duplicate_of_canonical_url": path.duplicate_of_canonical_url,
                        "transition_evaluations": path.transition_evaluations,
                        "budget_hits": path.budget_hits,
                    }),
                })
                .await
                .map_err(|_| ())?;
            if status == "ADMITTED" {
                insert_execution_provenance(&mut execution_ids, original_url, canonical_url, id);
            }
        }
        self.persist_execution_reconciliations(run_id, traversal, attempts, &mut execution_ids)
            .await?;
        Ok(execution_ids)
    }

    #[allow(dead_code)]
    async fn persist_execution_reconciliations(
        &self,
        run_id: CrawlRunId,
        traversal: &DiscoveryPreviewResult,
        attempts: &BTreeMap<String, ProductionPageAttempt>,
        execution_ids: &mut ExecutionProvenanceIds,
    ) -> Result<(), ()> {
        let repository = CrawlRunRepository::new(&self.database);
        for page in &traversal.pages {
            let canonical_url = page
                .canonical_url
                .clone()
                .unwrap_or_else(|| page.requested_url.clone());
            let original_url = fragment_free_fetch_url(&page.requested_url)?;
            let key = (original_url.clone(), canonical_url.clone());
            if execution_ids.contains_key(&key) {
                continue;
            }
            let id = discovered_id();
            repository
                .record_discovered_url(&DiscoveredUrlRecord {
                    id: id.clone(),
                    crawl_run_id: run_id,
                    source_id: None,
                    raw_href: None,
                    original_url: original_url.clone(),
                    canonical_url: canonical_url.clone(),
                    status: "EXECUTION_RECONCILED".to_owned(),
                    discovered_at: discovered_at(
                        attempts,
                        &page.requested_url,
                        self.clock.now_millis(),
                    ),
                    detail: serde_json::json!({
                        "origin": "EXECUTION_RECONCILIATION",
                        "requested_url": page.requested_url,
                        "observed_final_url": page.final_url,
                        "authoritative_canonical_url": canonical_url.clone(),
                        "seed_ids": page.seed_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                    }),
                })
                .await
                .map_err(|_| ())?;
            insert_execution_provenance(execution_ids, original_url, canonical_url, id);
        }
        Ok(())
    }

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

    async fn persist_execution(
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

    async fn persist_artifacts(
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

fn telemetry_artifact_kind(kind: CrawlerArtifactKind) -> ArtifactKind {
    match kind {
        CrawlerArtifactKind::RawHtml
        | CrawlerArtifactKind::CleanedHtml
        | CrawlerArtifactKind::RenderedHtml => ArtifactKind::Html,
        CrawlerArtifactKind::Screenshot => ArtifactKind::Screenshot,
        CrawlerArtifactKind::Markdown => ArtifactKind::Other,
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

#[derive(Clone)]
struct ProductionTraversalProvider {
    handler: ProductionCrawlJobHandler,
    context: JobExecutionContext,
    snapshot: CrawlRunSnapshot,
    deadline: ProductionDeadline,
    attempts: Arc<Mutex<BTreeMap<String, ProductionPageAttempt>>>,
}

impl ProductionTraversalProvider {
    fn new(
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

    async fn take_attempts(&self) -> BTreeMap<String, ProductionPageAttempt> {
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
struct ProductionDeadline {
    clock: Arc<dyn PreviewClock>,
    started_at_millis: u64,
    max_duration_millis: u64,
}

impl ProductionDeadline {
    fn new(clock: Arc<dyn PreviewClock>, started_at_millis: u64, max_duration_millis: u64) -> Self {
        Self {
            clock,
            started_at_millis,
            max_duration_millis,
        }
    }

    fn remaining_timeout(&self, per_page_timeout_millis: u64) -> Option<Duration> {
        let elapsed = self
            .clock
            .now_millis()
            .saturating_sub(self.started_at_millis);
        let remaining = self.max_duration_millis.checked_sub(elapsed)?;
        (remaining > 0).then(|| Duration::from_millis(remaining.min(per_page_timeout_millis)))
    }
}

struct SystemProductionClock;

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
enum ProductionPageAttempt {
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
struct PageResult {
    observation: erabi_crawler::PageObservation,
    status: Option<u16>,
    media_type: Option<String>,
    content_length: Option<u64>,
    elapsed_ms: Option<u64>,
    artifacts: Vec<CrawlerArtifactEvidence>,
    completeness: CrawlerResultCompleteness,
}

/// One physical provider invocation contributes exactly one attempt. An
/// observed provider-partial page is completed in the sense used by the
/// Task 3 summary contract, while its partial evidence contributes one
/// unresolved-work unit; it is never counted as a second attempt.
#[derive(Clone, Copy, Debug, Default)]
#[allow(dead_code)]
struct PageAttemptCounts {
    attempted: u64,
    completed: u64,
    unresolved_partial_work: u64,
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
struct PageFailure {
    code: CrawlExecutionErrorCode,
    status: Option<u16>,
    duration_exhausted: bool,
}

impl PageFailure {
    const fn normal(code: CrawlExecutionErrorCode) -> Self {
        Self {
            code,
            status: None,
            duration_exhausted: false,
        }
    }

    const fn duration_exhausted() -> Self {
        Self {
            code: CrawlExecutionErrorCode::Timeout,
            status: None,
            duration_exhausted: true,
        }
    }
}

fn production_limits(
    snapshot: &CrawlRunSnapshot,
    version: &erabi_domain::CrawlerVersion,
) -> Result<EffectiveDiscoveryPreviewLimits, ()> {
    let max_duration_ms = snapshot
        .settings()
        .max_duration_seconds
        .value
        .checked_mul(1_000)
        .ok_or(())?;
    let mut transition_total_limits = version
        .transition_ids()
        .iter()
        .copied()
        .map(|transition_id| EffectiveTransitionPreviewTotalLimit {
            transition_id,
            // The shared traversal independently evaluates configured total
            // budgets. Production adds no Preview-only artificial cap.
            effective_total_limit: u64::MAX,
        })
        .collect::<Vec<_>>();
    transition_total_limits.sort_by(|left, right| {
        left.transition_id
            .to_string()
            .cmp(&right.transition_id.to_string())
    });
    Ok(EffectiveDiscoveryPreviewLimits {
        max_pages: snapshot.settings().max_pages.value,
        max_depth: snapshot.settings().max_depth.value,
        max_duration_ms,
        max_downloaded_bytes: version.guardrails().max_downloaded_bytes,
        transition_total_limits,
    })
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

/// A checkpoint is required to recover interrupted frontier state, not to
/// repeat a run whose every admitted unit is already represented by durable
/// discovery and execution evidence.  The provenance tuple prevents a
/// redirect/canonical collision from treating a different admission as done.
fn durable_production_completion_without_checkpoint(
    executions: &[CrawlExecutionRecord],
    discovered: &[DiscoveredUrlRecord],
) -> bool {
    let admitted = discovered
        .iter()
        .filter(|record| matches!(record.status.as_str(), "ADMITTED" | "EXECUTION_RECONCILED"))
        .map(|record| {
            (
                record.id.as_str(),
                record.original_url.as_str(),
                record.canonical_url.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    !admitted.is_empty()
        && admitted.iter().all(|(id, requested, canonical)| {
            executions.iter().any(|execution| {
                execution.discovered_url_id.as_deref() == Some(*id)
                    && execution.requested_url == *requested
                    && execution.canonical_url == *canonical
            })
        })
}

#[allow(dead_code)]
fn transition_for(
    canonical_url: &str,
    page_type_id: PageTypeId,
    traversal: &DiscoveryPreviewResult,
) -> Option<DiscoveryTransitionId> {
    traversal
        .discovery_paths
        .iter()
        .find(|path| path.canonical_url.as_deref() == Some(canonical_url))
        .and_then(|path| {
            path.transition_evaluations.iter().find_map(|evaluation| {
                (evaluation.eligible && evaluation.target_page_type_id == page_type_id)
                    .then_some(evaluation.transition_id)
            })
        })
}

fn transition_for_paths(
    canonical_url: &str,
    page_type_id: PageTypeId,
    paths: &[DiscoveryPath],
) -> Option<DiscoveryTransitionId> {
    paths
        .iter()
        .find(|path| path.canonical_url.as_deref() == Some(canonical_url))
        .and_then(|path| {
            path.transition_evaluations.iter().find_map(|evaluation| {
                (evaluation.eligible && evaluation.target_page_type_id == page_type_id)
                    .then_some(evaluation.transition_id)
            })
        })
}

#[allow(dead_code)]
fn count_ambiguities(traversal: &DiscoveryPreviewResult) -> u64 {
    let pages = traversal
        .pages
        .iter()
        .filter(|page| page.state == PreviewUrlState::AmbiguousPageType)
        .count();
    let paths = traversal
        .discovery_paths
        .iter()
        .filter(|path| path.state == PreviewUrlState::AmbiguousPageType)
        .count();
    u64::try_from(pages.saturating_add(paths)).unwrap_or(u64::MAX)
}

#[allow(dead_code)]
fn duration_left_known_incomplete(traversal: &DiscoveryPreviewResult) -> bool {
    let duration_hit = traversal
        .summary
        .budget_hit_counts
        .get(&PreviewBudgetKind::MaxDuration)
        .is_some_and(|count| *count > 0);
    duration_hit
        && (traversal.summary.frontier_remaining > 0
            || traversal.summary.duration_work_not_expanded)
}

fn fragment_free_fetch_url(value: &str) -> Result<String, ()> {
    let mut parsed = url::Url::parse(value).map_err(|_| ())?;
    parsed.set_fragment(None);
    Ok(parsed.to_string())
}

fn crawl_url_state_id(run_id: CrawlRunId, canonical_url: &str) -> String {
    let identity = format!("{run_id}:{canonical_url}");
    let digest = erabi_domain::canonical_sha256(&identity).unwrap_or_else(|_| "invalid".to_owned());
    format!("crawl:{digest}")
}

fn decode_id<T: DeserializeOwned>(value: &str) -> Result<T, ()> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).map_err(|_| ())
}

fn insert_execution_provenance(
    ids: &mut ExecutionProvenanceIds,
    original_url: String,
    canonical_url: String,
    id: String,
) {
    ids.insert((original_url, canonical_url), id);
}

fn discovered_at(
    attempts: &BTreeMap<String, ProductionPageAttempt>,
    source_requested_url: &str,
    fallback_millis: u64,
) -> String {
    let millis = attempts
        .get(source_requested_url)
        .map_or(fallback_millis, ProductionPageAttempt::observed_at_millis);
    format!("unix-ms:{millis}")
}

impl ProductionPageAttempt {
    const fn observed_at_millis(&self) -> u64 {
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

fn discovery_seed_status(state: PreviewUrlState) -> &'static str {
    match state {
        PreviewUrlState::InScopeMatched => "ADMITTED",
        PreviewUrlState::AmbiguousPageType => "AMBIGUOUS_PAGE_TYPE",
        PreviewUrlState::Unmatched => "UNMATCHED",
        PreviewUrlState::External => "EXTERNAL",
        PreviewUrlState::Blocked => "BLOCKED",
        PreviewUrlState::CanonicalDuplicate => "CANONICAL_DUPLICATE",
        PreviewUrlState::BudgetExcluded => "BUDGET_EXCLUDED",
        PreviewUrlState::InvalidUrl => "INVALID",
        PreviewUrlState::RobotsExcluded => "ROBOTS_EXCLUDED",
        PreviewUrlState::ProviderError => "PROVIDER_ERROR",
        PreviewUrlState::Sampled => "SAMPLED",
    }
}

fn discovery_status(path: &erabi_domain::DiscoveryPath) -> &'static str {
    match path.state {
        PreviewUrlState::InScopeMatched => {
            if path
                .transition_evaluations
                .iter()
                .any(|evaluation| evaluation.eligible)
            {
                "ADMITTED"
            } else {
                "TRANSITION_INELIGIBLE"
            }
        }
        PreviewUrlState::AmbiguousPageType => "AMBIGUOUS_PAGE_TYPE",
        PreviewUrlState::Unmatched => "UNMATCHED",
        PreviewUrlState::External => "EXTERNAL",
        PreviewUrlState::Blocked => "BLOCKED",
        PreviewUrlState::CanonicalDuplicate => "CANONICAL_DUPLICATE",
        PreviewUrlState::BudgetExcluded => "BUDGET_EXCLUDED",
        PreviewUrlState::InvalidUrl => "INVALID",
        PreviewUrlState::RobotsExcluded => "ROBOTS_EXCLUDED",
        PreviewUrlState::ProviderError => "PROVIDER_ERROR",
        PreviewUrlState::Sampled => "SAMPLED",
    }
}

fn preserve_reason_for_discovery_status(status: &str) -> Option<&'static str> {
    match status {
        "CANONICAL_DUPLICATE" => Some("CANONICAL_DUPLICATE"),
        "AMBIGUOUS_PAGE_TYPE" => Some("AMBIGUOUS_PAGE_TYPE"),
        "UNMATCHED" => Some("UNMATCHED"),
        "EXTERNAL" => Some("EXTERNAL"),
        "BLOCKED" => Some("BLOCKED"),
        "BUDGET_EXCLUDED" => Some("BUDGET_EXCLUDED"),
        "INVALID" => Some("INVALID"),
        "ROBOTS_EXCLUDED" => Some("ROBOTS_EXCLUDED"),
        "PROVIDER_ERROR" => Some("PROVIDER_ERROR"),
        "TRANSITION_INELIGIBLE" => Some("TRANSITION_INELIGIBLE"),
        _ => None,
    }
}

fn is_html(media_type: &str) -> bool {
    media_type
        .split(';')
        .next()
        .is_some_and(|value| value.eq_ignore_ascii_case("text/html"))
}

fn parse_run_id(value: &str) -> Option<CrawlRunId> {
    Uuid::parse_str(value).ok().and_then(CrawlRunId::from_uuid)
}

fn discovered_id() -> String {
    Uuid::now_v7().to_string()
}

fn semantic_discovered_id<T: serde::Serialize>(
    run_id: CrawlRunId,
    kind: &str,
    work_generation: u64,
    identity: &T,
) -> Result<String, ()> {
    let digest =
        erabi_domain::canonical_sha256(&(run_id.to_string(), kind, work_generation, identity))
            .map_err(|_| ())?;
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&digest[index * 2..index * 2 + 2], 16).map_err(|_| ())?;
    }
    // Keep the existing UUID-shaped DiscoveredUrlId contract while making the
    // semantic observation identity stable across an uncertain transaction
    // replay. The work generation remains part of the identity, so a retry is
    // retained as a distinct physical discovery observation.
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(bytes).to_string())
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

fn epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |value| {
            i64::try_from(value.as_secs()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::{ProductionDeadline, discovered_id, is_production_job_kind};
    use erabi_crawler::{ManualPreviewClock, PreviewClock};
    use std::{sync::Arc, time::Duration};
    use uuid::Uuid;

    #[test]
    fn discovered_url_identity_is_a_bare_uuid_v7() {
        let id = discovered_id();
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
