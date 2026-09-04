//! Bounded normal-path execution for one frozen Production Crawl Run.
//!
//! Task 8 deliberately has no durable frontier checkpoint or recovery payload.
//! It delegates discovery semantics to `erabi_crawler::SemanticTraversal` and
//! owns only provider execution, durable evidence, progress, and normal status.
//! Task 9 owns durable reconstruction and re-entry.

use std::{
    collections::BTreeMap,
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use erabi_crawler::{
    CrawlerAdapter, CrawlerAdapterError, CrawlerArtifactEvidence, CrawlerArtifactKind,
    CrawlerEvidencePolicy, CrawlerExecuteRequest, CrawlerResultCompleteness,
    DiscoveryPreviewObservationRequest, DiscoveryPreviewProvider, DiscoveryPreviewProviderError,
    DiscoveryPreviewProviderOutcome, NetworkTargetPolicy, OriginKey, PacingCancellation,
    PacingOutcome, PacingService, PreviewClock, RenderingRequirement, RobotsAdmissionDecision,
    RobotsPolicyService, ScreenshotPolicy, SemanticTraversal, load_frozen_production_semantics,
};
use erabi_db::{
    ArtifactStore, ErabiDatabase,
    repositories::{
        ArtifactRepository, CrawlExecutionArtifact, CrawlExecutionArtifactKind,
        CrawlExecutionRecord, CrawlExecutionRepository, CrawlExecutionSummary, CrawlRunRepository,
        DiscoveredUrlRecord, JobRepository,
    },
};
use erabi_domain::{
    CrawlExecutionErrorCode, CrawlExecutionId, CrawlExecutionOutcome, CrawlRunId, CrawlRunSnapshot,
    CrawlRunStatus, DiscoveryPreviewResult, DiscoveryTransitionId, EffectiveDiscoveryPreviewLimits,
    EffectiveTransitionPreviewTotalLimit, PageTypeId, PreviewBudgetKind, PreviewUrlState,
    TestDiagnostic,
};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    JobExecutionContext, JobExecutionError, JobHandler, NewProgressEvent, ProgressAttemptId,
    ProgressKey, ProgressLiveHub, ProgressMetadata, ProgressService, ProgressTerminalState,
};

const PRODUCTION_CRAWL_JOB_KIND: &str = "PRODUCTION_CRAWL";

type ExecutionProvenanceKey = (String, String);
type ExecutionProvenanceIds = BTreeMap<ExecutionProvenanceKey, String>;

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
    async fn execute_inner(&self, context: JobExecutionContext) -> Result<(), ()> {
        if context.kind().as_str() != PRODUCTION_CRAWL_JOB_KIND {
            return Err(());
        }
        let job = JobRepository::new(&self.database)
            .job(context.job_id())
            .await
            .map_err(|_| ())?;
        let run_id = job
            .crawl_run_id
            .as_deref()
            .and_then(parse_run_id)
            .ok_or(())?;
        let snapshot = CrawlRunRepository::new(&self.database)
            .snapshot(run_id)
            .await
            .map_err(|_| ())?;
        let semantic = load_frozen_production_semantics(&self.database, &snapshot)
            .await
            .map_err(|_| ())?;
        let limits = production_limits(&snapshot, &semantic.version)?;
        let deadline = ProductionDeadline::new(
            self.clock.clone(),
            self.clock.now_millis(),
            limits.max_duration_ms,
        );

        CrawlRunRepository::new(&self.database)
            .transition_execution_status(run_id, CrawlRunStatus::Running)
            .await
            .map_err(|_| ())?;
        self.progress(&context, "PRODUCTION_STARTED", None).await?;
        self.progress(&context, "FRONTIER_PLANNED", None).await?;

        let provider = Arc::new(ProductionTraversalProvider::new(
            self.clone(),
            context.clone(),
            snapshot.clone(),
            deadline,
        ));
        let traversal = SemanticTraversal::for_frozen_snapshot(
            semantic,
            snapshot.selected_seed_ids().to_vec(),
            limits,
            provider.clone(),
            self.clock.clone(),
        )
        .map_err(|_| ())?;
        let result = traversal.execute().await.map_err(|_| ())?;
        if context.cancellation().is_cancelled() {
            return Err(());
        }

        let mut attempts = provider.take_attempts().await;
        let discovered_ids = self
            .persist_discovery_paths(run_id, &result, &attempts)
            .await?;
        let pagination_truncation_count = result.summary.pagination_truncation_count;
        let page_type_ambiguity_count = count_ambiguities(&result);
        let duration_incomplete = duration_left_known_incomplete(&result);
        let page_attempts = self
            .persist_page_attempts(
                run_id,
                &snapshot,
                &result,
                &discovered_ids,
                &mut attempts,
                &context,
            )
            .await?;
        let pending = result.summary.frontier_remaining;
        let unresolved_partial_work_count = page_attempts
            .unresolved_partial_work
            .saturating_add(page_type_ambiguity_count)
            .saturating_add(pagination_truncation_count)
            .saturating_add(pending)
            .saturating_add(u64::from(duration_incomplete));
        let summary = CrawlExecutionSummary {
            crawl_run_id: run_id,
            in_scope_pages_planned: page_attempts.attempted.saturating_add(pending),
            in_scope_pages_completed: page_attempts.completed,
            pagination_truncation_count,
            unresolved_partial_work_count,
            page_type_ambiguity_count,
        };
        CrawlExecutionRepository::new(&self.database)
            .save_summary(&summary)
            .await
            .map_err(|_| ())?;
        let status = if unresolved_partial_work_count > 0 {
            CrawlRunStatus::PartialResult
        } else {
            CrawlRunStatus::Succeeded
        };
        CrawlRunRepository::new(&self.database)
            .transition_execution_status(run_id, status)
            .await
            .map_err(|_| ())?;
        self.progress(
            &context,
            if status == CrawlRunStatus::PartialResult {
                "PRODUCTION_PARTIAL_RESULT"
            } else {
                "PRODUCTION_BOUNDED_COMPLETE"
            },
            Some(ProgressTerminalState::Succeeded),
        )
        .await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn persist_page_attempts(
        &self,
        run_id: CrawlRunId,
        snapshot: &CrawlRunSnapshot,
        traversal: &DiscoveryPreviewResult,
        discovered_ids: &ExecutionProvenanceIds,
        attempts: &mut BTreeMap<String, ProductionPageAttempt>,
        context: &JobExecutionContext,
    ) -> Result<PageAttemptCounts, ()> {
        let mut counts = PageAttemptCounts::default();
        for page in &traversal.pages {
            let attempt = attempts.remove(&page.requested_url).ok_or(())?;
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
                    self.persist_execution(CrawlExecutionRecord {
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
                    })
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
                    self.persist_execution(CrawlExecutionRecord {
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
                    })
                    .await?;
                    counts.unresolved_partial_work =
                        counts.unresolved_partial_work.saturating_add(1);
                    self.progress(context, "PAGE_FAILED", None).await?;
                }
            }
        }
        attempts.is_empty().then_some(counts).ok_or(())
    }

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
                )?;
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
                insert_execution_provenance(&mut execution_ids, original_url, canonical_url, id)?;
            }
        }
        self.persist_execution_reconciliations(run_id, traversal, attempts, &mut execution_ids)
            .await?;
        Ok(execution_ids)
    }

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
            insert_execution_provenance(execution_ids, original_url, canonical_url, id)?;
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
            .map_err(|_| PageFailure::normal(CrawlExecutionErrorCode::InvalidResponse))?;
        let origin = OriginKey::from_url(&target)
            .map_err(|_| PageFailure::normal(CrawlExecutionErrorCode::InvalidResponse))?;
        let registration = self
            .pacing
            .register(origin, snapshot)
            .map_err(|_| PageFailure::normal(CrawlExecutionErrorCode::RemoteFailure))?;
        let pacing_cancel = PacingCancellation::new();
        let admission = tokio::select! {
            value = self.robots.evaluate(&target, snapshot, &pacing_cancel) => value.map_err(|_| PageFailure::normal(CrawlExecutionErrorCode::RobotsExcluded)),
            () = context.cancellation().cancelled() => { pacing_cancel.cancel(); return Err(PageFailure::normal(CrawlExecutionErrorCode::Cancelled)); }
        }?;
        if admission.decision() == RobotsAdmissionDecision::Disallowed {
            return Err(PageFailure::normal(CrawlExecutionErrorCode::RobotsExcluded));
        }
        let permit = tokio::select! {
            value = registration.acquire(&admission, &pacing_cancel) => value.map_err(|_| PageFailure::normal(CrawlExecutionErrorCode::RemoteFailure)),
            () = context.cancellation().cancelled() => { pacing_cancel.cancel(); return Err(PageFailure::normal(CrawlExecutionErrorCode::Cancelled)); }
        }?;
        // Recompute immediately before the provider call so pacing/robots
        // work cannot let an in-flight request exceed the frozen run cap.
        let timeout = deadline
            .remaining_timeout(snapshot.settings().timeout_ms.value)
            .ok_or_else(PageFailure::duration_exhausted)?;
        self.progress(context, "PAGE_LOADING", None)
            .await
            .map_err(|()| PageFailure::normal(CrawlExecutionErrorCode::RemoteFailure))?;
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
            value = self.adapter.execute(request) => value,
            () = context.cancellation().cancelled() => { pacing_cancel.cancel(); return Err(PageFailure::normal(CrawlExecutionErrorCode::Cancelled)); }
        };
        let result = match result {
            Ok(result) => {
                permit
                    .record_outcome(PacingOutcome::Success)
                    .map_err(|_| PageFailure::normal(CrawlExecutionErrorCode::RemoteFailure))?;
                result
            }
            Err(error) => {
                let _ = permit.record_outcome(PacingOutcome::from_adapter_error(&error));
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

    async fn persist_execution(&self, record: CrawlExecutionRecord) -> Result<(), ()> {
        CrawlExecutionRepository::new(&self.database)
            .persist(&record)
            .await
            .map_err(|_| ())
    }

    async fn persist_artifacts(
        &self,
        run_id: CrawlRunId,
        created_at: &str,
        artifacts: Vec<CrawlerArtifactEvidence>,
        retain: bool,
    ) -> Result<Vec<CrawlExecutionArtifact>, ()> {
        if !retain {
            return Ok(Vec::new());
        }
        let mut saved = Vec::new();
        for artifact in artifacts {
            let (kind, name, media_type, bytes) = artifact_bytes(&artifact);
            let stored = self
                .artifact_store
                .write_bytes(format!("production/{run_id}"), name, bytes)
                .map_err(|_| ())?;
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
                .map_err(|_| ())?;
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
    ) -> Result<(), ()> {
        let attempt = ProgressAttemptId::new(context.attempt_id().to_owned()).map_err(|_| ())?;
        let event = match terminal {
            Some(state) => NewProgressEvent::terminal(
                context.job_id().clone(),
                state,
                ProgressMetadata::default(),
            )
            .map_err(|_| ())?,
            None => NewProgressEvent::new(
                context.job_id().clone(),
                ProgressKey::new(key).map_err(|_| ())?,
                ProgressMetadata::default(),
            ),
        }
        .with_attempt(attempt);
        let service = ProgressService::new(&self.database);
        match &self.progress_live_hub {
            Some(hub) => service
                .append_and_publish_at(hub, &event, epoch_seconds())
                .await
                .map(|_| ())
                .map_err(|_| ()),
            None => service
                .append_at(&event, epoch_seconds())
                .await
                .map(|_| ())
                .map_err(|_| ()),
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
            handler
                .execute_inner(context)
                .await
                .map_err(|()| JobExecutionError)
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

fn insert_execution_provenance(
    ids: &mut ExecutionProvenanceIds,
    original_url: String,
    canonical_url: String,
    id: String,
) -> Result<(), ()> {
    if ids.contains_key(&(original_url.clone(), canonical_url.clone())) {
        return Err(());
    }
    ids.insert((original_url, canonical_url), id);
    Ok(())
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
    use super::{ProductionDeadline, discovered_id};
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
}
