//! Generic Tokio worker and durable progress boundaries for Erabi jobs.
//!
//! Generic leased execution, cooperative cancellation, bounded checkpoints,
//! and replayable progress services for Erabi jobs.

use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use erabi_db::{
    DbError, ErabiDatabase,
    repositories::{
        CheckpointRepositoryError as DbCheckpointRepositoryError, ConcurrencyState,
        CrawlRunRepository, JobFailureCode, JobId, JobKind, JobLease, JobRepository,
        JobRepositoryError, JobState, StaleJobRecovery,
    },
};
use erabi_domain::{CrawlExecutionId, CrawlRunId, CrawlRunStatus};
use erabi_observability::{
    CheckpointPhase, CorrelationContext, DiagnosticFields,
    ExecutionAction as TelemetryExecutionAction, ExecutionCategory as TelemetryExecutionCategory,
    ExecutionOperation as TelemetryExecutionOperation, JobAttemptSpan, JobStateToken,
    SemanticEvent, TelemetryCode, TelemetryId, TerminalStatus,
    WorkerRuntimeDisposition as TelemetryWorkerRuntimeDisposition,
    WorkerTurnOutcome as TelemetryWorkerTurnOutcome, emit,
};
use futures_util::FutureExt;
use tokio::{
    sync::RwLock,
    time::{Instant, interval_at},
};

mod actions;
mod cancellation;
mod production;
mod progress;
mod quick_scrape;
mod recovery;
mod storage_pressure;

pub use actions::{
    JobAction, JobActionError, JobActionResult, JobActionService, RerunFullCrawlInput,
};
pub use cancellation::{CancellationController, CancellationToken};
pub use progress::{
    ProgressLiveHub, ProgressLiveHubError, ProgressPublication, ProgressPublisher,
    ProgressPublisherError, ProgressService, ProgressServiceError,
};
pub use storage_pressure::{
    DEFAULT_CRITICAL_FREE_BYTES, DEFAULT_WARNING_FREE_BYTES, FileSystemStorageProbe,
    StoragePressureController, StoragePressureLevel, StoragePressureMonitor, StoragePressurePolicy,
    StoragePressurePolicyError, StoragePressureState, StoragePressureToken, StorageProbe,
    StorageProbeError,
};

pub use erabi_db::repositories::JobStorageClass;
pub use erabi_db::repositories::{
    AcquiredJob, AttemptOutcome, JobAttempt, JobRecord, NewJob, QuickScrapeRunJob,
};
pub use erabi_db::repositories::{
    CHECKPOINT_ENVELOPE_FORMAT_VERSION, CheckpointEnvelope, CheckpointIdentity,
    CheckpointPayloadKind, CheckpointRecord, CheckpointRepository, CheckpointRepositoryError,
    MAX_CHECKPOINT_BYTES, MAX_CHECKPOINT_PAYLOAD_KIND_BYTES,
};
pub use erabi_db::repositories::{
    NewProgressEvent, ProgressAttemptId, ProgressEvent, ProgressEventId, ProgressKey,
    ProgressMetadata, ProgressMetadataCode, ProgressMetadataKey, ProgressMetadataValue,
    ProgressReplayPage, ProgressReplayRequest, ProgressRepository, ProgressRepositoryError,
    ProgressSequence, ProgressTerminalState,
};

/// Bounded categories for execution diagnostics. These are intentionally
/// local to the jobs/runtime boundary rather than a workspace-wide error
/// hierarchy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrchestrationErrorCategory {
    Repository,
    LeaseClaim,
    CheckpointRecovery,
    Provider,
    NetworkAdmission,
    Pacing,
    Artifact,
    SerializationProjection,
    Finalization,
    ProgressPublication,
    Invariant,
}

pub(crate) const fn telemetry_operation(
    operation: ExecutionOperation,
) -> TelemetryExecutionOperation {
    match operation {
        ExecutionOperation::LoadJob => TelemetryExecutionOperation::LoadJob,
        ExecutionOperation::LoadRunSnapshot => TelemetryExecutionOperation::LoadRunSnapshot,
        ExecutionOperation::LoadCheckpoint => TelemetryExecutionOperation::LoadCheckpoint,
        ExecutionOperation::TransitionRun => TelemetryExecutionOperation::TransitionRun,
        ExecutionOperation::AcquireAdmission => TelemetryExecutionOperation::AcquireAdmission,
        ExecutionOperation::ProviderExecution => TelemetryExecutionOperation::ProviderExecution,
        ExecutionOperation::RecordOutcome => TelemetryExecutionOperation::RecordOutcome,
        ExecutionOperation::PersistArtifact => TelemetryExecutionOperation::PersistArtifact,
        ExecutionOperation::PersistExecution => TelemetryExecutionOperation::PersistExecution,
        ExecutionOperation::FinalizeRun => TelemetryExecutionOperation::FinalizeRun,
        ExecutionOperation::AppendProgress => TelemetryExecutionOperation::AppendProgress,
        ExecutionOperation::PublishProgress => TelemetryExecutionOperation::PublishProgress,
        ExecutionOperation::ReconcileTerminality => {
            TelemetryExecutionOperation::ReconcileTerminality
        }
        ExecutionOperation::QueueLifecycle => TelemetryExecutionOperation::QueueLifecycle,
        ExecutionOperation::Serialization => TelemetryExecutionOperation::Serialization,
    }
}

pub(crate) const fn telemetry_category(
    category: OrchestrationErrorCategory,
) -> TelemetryExecutionCategory {
    match category {
        OrchestrationErrorCategory::Repository => TelemetryExecutionCategory::Repository,
        OrchestrationErrorCategory::LeaseClaim => TelemetryExecutionCategory::LeaseClaim,
        OrchestrationErrorCategory::CheckpointRecovery => {
            TelemetryExecutionCategory::CheckpointRecovery
        }
        OrchestrationErrorCategory::Provider => TelemetryExecutionCategory::Provider,
        OrchestrationErrorCategory::NetworkAdmission => {
            TelemetryExecutionCategory::NetworkAdmission
        }
        OrchestrationErrorCategory::Pacing => TelemetryExecutionCategory::Pacing,
        OrchestrationErrorCategory::Artifact => TelemetryExecutionCategory::Artifact,
        OrchestrationErrorCategory::SerializationProjection => {
            TelemetryExecutionCategory::SerializationProjection
        }
        OrchestrationErrorCategory::Finalization => TelemetryExecutionCategory::Finalization,
        OrchestrationErrorCategory::ProgressPublication => {
            TelemetryExecutionCategory::ProgressPublication
        }
        OrchestrationErrorCategory::Invariant => TelemetryExecutionCategory::Invariant,
    }
}

pub(crate) const fn telemetry_action(action: ExecutionAction) -> TelemetryExecutionAction {
    match action {
        ExecutionAction::Continue => TelemetryExecutionAction::Continue,
        ExecutionAction::Retry => TelemetryExecutionAction::Retry,
        ExecutionAction::Fail => TelemetryExecutionAction::Fail,
        ExecutionAction::Reconcile => TelemetryExecutionAction::Reconcile,
        ExecutionAction::Publish => TelemetryExecutionAction::Publish,
    }
}

pub(crate) const fn telemetry_runtime_disposition(
    disposition: WorkerRuntimeDisposition,
) -> TelemetryWorkerRuntimeDisposition {
    match disposition {
        WorkerRuntimeDisposition::Continue => TelemetryWorkerRuntimeDisposition::Continue,
        WorkerRuntimeDisposition::LeaseLost => TelemetryWorkerRuntimeDisposition::LeaseLost,
        WorkerRuntimeDisposition::Fatal => TelemetryWorkerRuntimeDisposition::Fatal,
    }
}

pub(crate) fn telemetry_code(code: &'static str) -> TelemetryCode {
    TelemetryCode::from_static(code)
}

pub(crate) fn telemetry_id(value: &str) -> Option<TelemetryId> {
    TelemetryId::parse(value).ok()
}

pub(crate) fn telemetry_job_context(context: &JobExecutionContext) -> CorrelationContext {
    let telemetry = CorrelationContext::new();
    let telemetry =
        telemetry_id(context.job_id.as_str()).map_or(telemetry, |id| telemetry.with_job_id(id));
    telemetry_id(context.attempt_id.as_str()).map_or(telemetry, |id| telemetry.with_attempt_id(id))
}

pub(crate) fn telemetry_crawl_context(
    context: &JobExecutionContext,
    run_id: Option<&str>,
    execution_id: Option<&str>,
) -> CorrelationContext {
    let mut telemetry = telemetry_job_context(context);
    if let Some(value) = run_id.and_then(telemetry_id) {
        telemetry = telemetry.with_crawl_run_id(value);
    }
    if let Some(value) = execution_id.and_then(telemetry_id) {
        telemetry = telemetry.with_crawl_execution_id(value);
    }
    telemetry
}

fn telemetry_job_context_from_acquired(acquired: &AcquiredJob) -> CorrelationContext {
    let telemetry = telemetry_id(acquired.job.id.as_str())
        .map_or(CorrelationContext::new(), |id| {
            CorrelationContext::new().with_job_id(id)
        });
    telemetry_id(&acquired.attempt.id).map_or(telemetry, |id| telemetry.with_attempt_id(id))
}

pub(crate) fn telemetry_diagnostic(diagnostic: &ExecutionDiagnostic) -> DiagnosticFields {
    let telemetry = DiagnosticFields::new(
        telemetry_code(diagnostic.code),
        telemetry_operation(diagnostic.operation),
        telemetry_category(diagnostic.category),
        telemetry_action(diagnostic.action),
    );
    let telemetry = match diagnostic.provider {
        Some("crawler-adapter" | "crawl4ai" | "CRAWL4AI") => {
            telemetry.with_provider(erabi_observability::ProviderToken::Crawl4Ai)
        }
        Some(_) => telemetry.with_provider(erabi_observability::ProviderToken::Unavailable),
        None => telemetry,
    };
    match diagnostic
        .terminal_outcome
        .and_then(telemetry_terminal_status)
    {
        Some(status) => telemetry.with_terminal_status(status),
        None => telemetry,
    }
}

pub(crate) fn telemetry_terminal_status(status: CrawlRunStatus) -> Option<TerminalStatus> {
    match status {
        CrawlRunStatus::Succeeded => Some(TerminalStatus::Succeeded),
        CrawlRunStatus::PartialResult => Some(TerminalStatus::PartialResult),
        CrawlRunStatus::Failed => Some(TerminalStatus::Failed),
        CrawlRunStatus::Cancelled => Some(TerminalStatus::Cancelled),
        CrawlRunStatus::Queued | CrawlRunStatus::Running => None,
    }
}

pub(crate) const fn telemetry_job_state(state: JobState) -> JobStateToken {
    match state {
        JobState::Queued => JobStateToken::Queued,
        JobState::Running => JobStateToken::Running,
        JobState::Succeeded => JobStateToken::Succeeded,
        JobState::Failed => JobStateToken::Failed,
        JobState::Cancelled => JobStateToken::Cancelled,
    }
}

pub(crate) const fn telemetry_progress_status(status: ProgressTerminalState) -> TerminalStatus {
    match status {
        ProgressTerminalState::Succeeded => TerminalStatus::Succeeded,
        ProgressTerminalState::Failed => TerminalStatus::Failed,
        ProgressTerminalState::Cancelled => TerminalStatus::Cancelled,
    }
}

pub(crate) const fn telemetry_checkpoint_phase(
    phase: erabi_crawler::CrawlRecoveryPhase,
) -> CheckpointPhase {
    match phase {
        erabi_crawler::CrawlRecoveryPhase::Initialized => CheckpointPhase::Initialized,
        erabi_crawler::CrawlRecoveryPhase::Traversing => CheckpointPhase::Running,
        erabi_crawler::CrawlRecoveryPhase::Finalizing => CheckpointPhase::Finalizing,
    }
}

pub(crate) const fn telemetry_turn_outcome(turn: &WorkerTurn) -> TelemetryWorkerTurnOutcome {
    match turn {
        WorkerTurn::Idle => TelemetryWorkerTurnOutcome::Idle,
        WorkerTurn::Succeeded { .. } => TelemetryWorkerTurnOutcome::Succeeded,
        WorkerTurn::RetryScheduled { .. } => TelemetryWorkerTurnOutcome::RetryScheduled,
        WorkerTurn::Failed { .. } => TelemetryWorkerTurnOutcome::Failed,
        WorkerTurn::Cancelled { .. } => TelemetryWorkerTurnOutcome::Cancelled,
        WorkerTurn::StoragePressure { .. } => TelemetryWorkerTurnOutcome::StoragePressure,
    }
}

pub(crate) fn telemetry_failure_code(failure: JobFailureCode) -> TelemetryCode {
    telemetry_code(match failure {
        JobFailureCode::HandlerFailed => "HANDLER_FAILED",
        JobFailureCode::HandlerPanicked => "HANDLER_PANICKED",
        JobFailureCode::LeaseExpired => "LEASE_EXPIRED",
        JobFailureCode::Cancelled => "CANCELLED",
        JobFailureCode::StoragePressure => "STORAGE_PRESSURE",
    })
}

pub(crate) fn emit_turn_telemetry(context: &CorrelationContext, turn: &WorkerTurn) {
    let (failure_code, diagnostics) = match turn {
        WorkerTurn::RetryScheduled {
            failure,
            diagnostics,
            ..
        }
        | WorkerTurn::Failed {
            failure,
            diagnostics,
            ..
        } => (Some(telemetry_failure_code(*failure)), diagnostics.as_ref()),
        WorkerTurn::Succeeded { diagnostics, .. } | WorkerTurn::Cancelled { diagnostics, .. } => {
            (None, diagnostics.as_ref())
        }
        WorkerTurn::Idle | WorkerTurn::StoragePressure { .. } => (None, None),
    };
    let primary = diagnostics.and_then(|value| value.primary.as_ref().map(telemetry_diagnostic));
    let secondary_count = diagnostics.map_or(0, |value| value.secondary.len());
    emit(SemanticEvent::WorkerTurnCompleted {
        context: *context,
        outcome: telemetry_turn_outcome(turn),
        failure_code,
        primary,
        secondary_count: u8::try_from(secondary_count).unwrap_or(u8::MAX),
    });
    if let Some(diagnostics) = diagnostics {
        for diagnostic in &diagnostics.secondary {
            let diagnostic_context = diagnostic_context(context, diagnostic);
            emit(SemanticEvent::ExecutionSecondaryFailure {
                context: diagnostic_context,
                diagnostic: telemetry_diagnostic(diagnostic),
            });
        }
    }
    match turn {
        WorkerTurn::RetryScheduled { failure, .. } => emit(SemanticEvent::JobRetryScheduled {
            context: *context,
            failure_code: telemetry_failure_code(*failure),
        }),
        WorkerTurn::Cancelled { .. } => emit(SemanticEvent::JobCancelled { context: *context }),
        _ => {}
    }
}

fn diagnostic_context(
    base: &CorrelationContext,
    diagnostic: &ExecutionDiagnostic,
) -> CorrelationContext {
    let mut context = *base;
    if let Some(value) = diagnostic
        .run_id
        .and_then(|value| telemetry_id(&value.to_string()))
    {
        context = context.with_crawl_run_id(value);
    }
    if let Some(value) = diagnostic
        .execution_id
        .and_then(|value| telemetry_id(&value.to_string()))
    {
        context = context.with_crawl_execution_id(value);
    }
    context
}

/// Safe operation names attached to a bounded execution diagnostic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionOperation {
    LoadJob,
    LoadRunSnapshot,
    LoadCheckpoint,
    TransitionRun,
    AcquireAdmission,
    ProviderExecution,
    RecordOutcome,
    PersistArtifact,
    PersistExecution,
    FinalizeRun,
    AppendProgress,
    PublishProgress,
    ReconcileTerminality,
    QueueLifecycle,
    Serialization,
}

/// Safe action selected by the orchestration boundary after an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionAction {
    Continue,
    Retry,
    Fail,
    Reconcile,
    Publish,
}

/// Bounded, non-content diagnostic data for one orchestration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionDiagnostic {
    pub run_id: Option<CrawlRunId>,
    pub job_id: Option<JobId>,
    pub job_attempt_id: Option<String>,
    pub execution_id: Option<CrawlExecutionId>,
    pub work_generation: Option<u64>,
    pub operation: ExecutionOperation,
    pub category: OrchestrationErrorCategory,
    pub action: ExecutionAction,
    pub provider: Option<&'static str>,
    pub attempt: Option<u32>,
    pub terminal_outcome: Option<CrawlRunStatus>,
    pub code: &'static str,
}

impl ExecutionDiagnostic {
    #[must_use]
    pub const fn new(
        category: OrchestrationErrorCategory,
        operation: ExecutionOperation,
        action: ExecutionAction,
        code: &'static str,
    ) -> Self {
        Self {
            run_id: None,
            job_id: None,
            job_attempt_id: None,
            execution_id: None,
            work_generation: None,
            operation,
            category,
            action,
            provider: None,
            attempt: None,
            terminal_outcome: None,
            code,
        }
    }

    #[must_use]
    pub fn with_context(mut self, context: &JobExecutionContext) -> Self {
        self.job_id = Some(context.job_id.clone());
        self.job_attempt_id = Some(context.attempt_id.clone());
        self.attempt = Some(context.attempt_number);
        self
    }

    #[must_use]
    pub const fn with_run(mut self, run_id: CrawlRunId) -> Self {
        self.run_id = Some(run_id);
        self
    }

    #[must_use]
    pub const fn with_execution(mut self, execution_id: CrawlExecutionId) -> Self {
        self.execution_id = Some(execution_id);
        self
    }

    #[must_use]
    pub const fn with_work_generation(mut self, work_generation: u64) -> Self {
        self.work_generation = Some(work_generation);
        self
    }

    #[must_use]
    pub const fn with_provider(mut self, provider: &'static str) -> Self {
        self.provider = Some(provider);
        self
    }

    #[must_use]
    pub const fn with_terminal_outcome(mut self, outcome: CrawlRunStatus) -> Self {
        self.terminal_outcome = Some(outcome);
        self
    }
}

/// Primary and secondary typed diagnostics for one worker outcome. Secondary
/// diagnostics never replace the primary business/execution failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionDiagnostics {
    pub primary: Option<ExecutionDiagnostic>,
    pub secondary: Vec<ExecutionDiagnostic>,
}

impl ExecutionDiagnostics {
    const MAX_SECONDARY: usize = 8;

    #[must_use]
    pub const fn new() -> Self {
        Self {
            primary: None,
            secondary: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.primary.is_none() && self.secondary.is_empty()
    }

    fn add_primary(&mut self, diagnostic: ExecutionDiagnostic) {
        if self.primary.is_none() {
            self.primary = Some(diagnostic);
        } else {
            self.add_secondary(diagnostic);
        }
    }

    fn add_secondary(&mut self, diagnostic: ExecutionDiagnostic) {
        if self.secondary.len() < Self::MAX_SECONDARY {
            self.secondary.push(diagnostic);
        }
    }
}

impl Default for ExecutionDiagnostics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug)]
struct TerminalCrawlRunCommit {
    run_id: CrawlRunId,
    status: CrawlRunStatus,
    terminal_progress_durable: bool,
}
pub use production::ProductionCrawlJobHandler;
pub use quick_scrape::QuickScrapeJobHandler;

/// One durable runtime dispatcher for the two Plan 06 crawl root-job kinds.
/// The queue leases by priority, not handler kind, so separate polling loops
/// would be able to lease and incorrectly fail one another's work.
#[derive(Clone)]
pub struct CrawlRootJobHandler {
    quick_scrape: QuickScrapeJobHandler,
    production: ProductionCrawlJobHandler,
}

impl CrawlRootJobHandler {
    #[must_use]
    pub const fn new(
        quick_scrape: QuickScrapeJobHandler,
        production: ProductionCrawlJobHandler,
    ) -> Self {
        Self {
            quick_scrape,
            production,
        }
    }
}

impl JobHandler for CrawlRootJobHandler {
    fn execute(
        &self,
        context: JobExecutionContext,
    ) -> impl Future<Output = Result<(), JobExecutionError>> + Send {
        let handler = self.clone();
        async move {
            match context.kind().as_str() {
                "QUICK_SCRAPE" => handler.quick_scrape.execute(context).await,
                "PRODUCTION_CRAWL" => handler.production.execute(context).await,
                "RETRY"
                | "RETRY_FAILED_PARTS"
                | "RESUME_CHECKPOINT"
                | "RERUN_FULL_CRAWL"
                | "RESTART_FROM_BEGINNING" => {
                    let database = handler.quick_scrape.database();
                    let job = JobRepository::new(database)
                        .job(context.job_id())
                        .await
                        .map_err(|_| JobExecutionError)?;
                    let run_id = job
                        .crawl_run_id
                        .as_deref()
                        .and_then(|value| uuid::Uuid::parse_str(value).ok())
                        .and_then(erabi_domain::CrawlRunId::from_uuid)
                        .ok_or(JobExecutionError)?;
                    let snapshot = erabi_db::repositories::CrawlRunRepository::new(database)
                        .snapshot(run_id)
                        .await
                        .map_err(|_| JobExecutionError)?;
                    match snapshot.run_type() {
                        erabi_domain::CrawlRunType::QuickScrape => {
                            handler.quick_scrape.execute(context).await
                        }
                        erabi_domain::CrawlRunType::ProductionRun => {
                            handler.production.execute(context).await
                        }
                        erabi_domain::CrawlRunType::TestRun
                        | erabi_domain::CrawlRunType::DiscoveryPreview => Err(JobExecutionError),
                    }
                }
                _ => Err(JobExecutionError),
            }
        }
    }
}

/// Fixed bounded retry and lease policy for one generic worker runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerPolicy {
    /// Duration of one acquired lease. Must be at least two seconds so the
    /// whole-second durable timestamps leave time to renew before expiry.
    pub lease_duration_seconds: i64,
    /// Delay before a retry. The job's `max_attempts` supplies the hard bound.
    pub retry_delay_seconds: i64,
}

impl WorkerPolicy {
    /// A conservative deterministic policy suitable for local worker polling.
    #[must_use]
    pub const fn conservative() -> Self {
        Self {
            lease_duration_seconds: 30,
            retry_delay_seconds: 5,
        }
    }

    fn valid(self) -> bool {
        self.lease_duration_seconds >= 2 && self.retry_delay_seconds >= 0
    }

    fn heartbeat_interval(self) -> Duration {
        let seconds = (self.lease_duration_seconds / 3).max(1).unsigned_abs();
        Duration::from_secs(seconds)
    }
}

/// Context supplied to an individual handler. It contains only durable queue
/// identity and ownership evidence, never request bodies or scraped content.
#[derive(Clone, Debug)]
pub struct JobExecutionContext {
    job_id: JobId,
    kind: JobKind,
    attempt_id: String,
    attempt_number: u32,
    worker_id: String,
    lease: JobLease,
    cancellation: CancellationToken,
    storage_pressure: StoragePressureToken,
    checkpoint_writer: CheckpointWriter,
    terminal_failure: Arc<AtomicBool>,
    diagnostics: Arc<Mutex<ExecutionDiagnostics>>,
    terminal_crawl_run: Arc<Mutex<Option<TerminalCrawlRunCommit>>>,
}

impl JobExecutionContext {
    #[must_use]
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    #[must_use]
    pub fn kind(&self) -> &JobKind {
        &self.kind
    }

    /// Returns the durable `UUIDv7` identity of the current attempt. Progress
    /// events use this reference instead of inventing an in-memory sequence.
    #[must_use]
    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }

    #[must_use]
    pub const fn attempt_number(&self) -> u32 {
        self.attempt_number
    }

    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    #[must_use]
    pub fn lease(&self) -> &JobLease {
        &self.lease
    }

    /// Returns the cooperative cancellation signal for this active turn.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Returns the cooperative signal for critical storage pressure. This is
    /// distinct from user-requested cancellation.
    #[must_use]
    pub const fn storage_pressure(&self) -> &StoragePressureToken {
        &self.storage_pressure
    }

    /// Persists a bounded checkpoint while this worker still owns the attempt.
    ///
    /// # Errors
    /// Returns a typed validation, ownership, or durable persistence failure.
    pub async fn checkpoint(
        &self,
        checkpoint: &CheckpointEnvelope,
    ) -> Result<CheckpointRecord, JobRepositoryError> {
        self.checkpoint_writer.append(checkpoint).await
    }

    /// Returns the verified ownership material needed by the narrowly scoped
    /// Task 9 initialization transaction. Callers must mark the writer after
    /// the repository commits its coupled checkpoint append.
    pub(crate) async fn checkpoint_lineage(
        &self,
    ) -> Result<(JobId, String, JobLease, i64), JobRepositoryError> {
        self.checkpoint_writer.lineage().await
    }

    /// Returns the queue clock value associated with this owned turn. Durable
    /// result writes must use the same seconds-based clock as lease creation;
    /// handlers must not substitute a provider or wall-clock timestamp.
    pub(crate) fn ownership_now(&self) -> i64 {
        self.checkpoint_writer.initial_now.saturating_add(
            i64::try_from(self.checkpoint_writer.started.elapsed().as_secs()).unwrap_or(i64::MAX),
        )
    }

    pub(crate) fn mark_checkpoint_persisted(&self) {
        self.checkpoint_writer.mark_persisted();
    }

    /// Marks the current expected handler error as permanently terminal after
    /// the handler has durably recorded its stable outcome. Generic handlers
    /// remain retryable by default; this narrow signal prevents a known
    /// permanent external result from consuming unrelated retry attempts.
    pub(crate) fn mark_terminal_failure(&self) {
        self.terminal_failure.store(true, Ordering::Release);
    }

    fn terminal_failure_requested(&self) -> bool {
        self.terminal_failure.load(Ordering::Acquire)
    }

    pub(crate) fn record_primary_diagnostic(&self, diagnostic: ExecutionDiagnostic) {
        if let Ok(mut diagnostics) = self.diagnostics.lock() {
            diagnostics.add_primary(diagnostic.with_context(self));
        }
    }

    pub(crate) fn record_secondary_diagnostic(&self, diagnostic: ExecutionDiagnostic) {
        if let Ok(mut diagnostics) = self.diagnostics.lock() {
            diagnostics.add_secondary(diagnostic.with_context(self));
        }
    }

    pub(crate) fn record_diagnostics(&self, mut incoming: ExecutionDiagnostics) {
        if let Ok(mut diagnostics) = self.diagnostics.lock() {
            if let Some(primary) = incoming.primary.take() {
                let primary = primary.with_context(self);
                if diagnostics.primary.as_ref() != Some(&primary) {
                    diagnostics.add_primary(primary);
                }
            }
            for secondary in incoming.secondary {
                diagnostics.add_secondary(secondary.with_context(self));
            }
        }
    }

    pub(crate) fn record_secondary_diagnostics(&self, incoming: ExecutionDiagnostics) {
        if let Ok(mut diagnostics) = self.diagnostics.lock() {
            if let Some(primary) = incoming.primary {
                let primary = primary.with_context(self);
                if diagnostics.primary.as_ref() != Some(&primary) {
                    diagnostics.add_secondary(primary);
                }
            }
            for secondary in incoming.secondary {
                diagnostics.add_secondary(secondary.with_context(self));
            }
        }
    }

    fn take_diagnostics(&self) -> Option<ExecutionDiagnostics> {
        self.diagnostics.lock().ok().and_then(|mut diagnostics| {
            let taken = std::mem::take(&mut *diagnostics);
            (!taken.is_empty()).then_some(taken)
        })
    }

    pub(crate) fn mark_terminal_crawl_run(&self, run_id: CrawlRunId, status: CrawlRunStatus) {
        if let Ok(mut terminal) = self.terminal_crawl_run.lock() {
            *terminal = Some(TerminalCrawlRunCommit {
                run_id,
                status,
                terminal_progress_durable: false,
            });
        }
    }

    pub(crate) fn mark_terminal_progress_durable(&self) {
        if let Ok(mut terminal) = self.terminal_crawl_run.lock()
            && let Some(terminal) = terminal.as_mut()
        {
            terminal.terminal_progress_durable = true;
        }
    }

    fn take_terminal_crawl_run(&self) -> Option<TerminalCrawlRunCommit> {
        self.terminal_crawl_run
            .lock()
            .ok()
            .and_then(|mut terminal| terminal.take())
    }
}

#[derive(Clone, Debug)]
struct CheckpointWriter {
    database: ErabiDatabase,
    job_id: JobId,
    attempt_id: String,
    lease: Arc<RwLock<JobLease>>,
    persisted: Arc<AtomicBool>,
    initial_now: i64,
    started: Instant,
}

impl CheckpointWriter {
    async fn lineage(&self) -> Result<(JobId, String, JobLease, i64), JobRepositoryError> {
        let lease = self.lease.read().await.clone();
        let elapsed = i64::try_from(self.started.elapsed().as_secs())
            .map_err(|_| JobRepositoryError::QueueInvariant)?;
        let created_at = self
            .initial_now
            .checked_add(elapsed)
            .ok_or(JobRepositoryError::QueueInvariant)?;
        Ok((
            self.job_id.clone(),
            self.attempt_id.clone(),
            lease,
            created_at,
        ))
    }

    fn mark_persisted(&self) {
        self.persisted.store(true, Ordering::Release);
    }

    async fn append(
        &self,
        checkpoint: &CheckpointEnvelope,
    ) -> Result<CheckpointRecord, JobRepositoryError> {
        let (job_id, attempt_id, lease, created_at) = self.lineage().await?;
        let record = JobRepository::new(&self.database)
            .append_checkpoint(&job_id, &attempt_id, &lease, checkpoint, created_at)
            .await?;
        self.mark_persisted();
        Ok(record)
    }

    async fn update_lease(&self, lease: JobLease) {
        *self.lease.write().await = lease;
    }

    fn persisted(&self) -> bool {
        self.persisted.load(Ordering::Acquire)
    }
}

/// A sanitized expected handler error. Error payloads are deliberately not
/// persisted; only stable typed failure codes are durable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobExecutionError;

/// A typed generic future-work handler. The runtime catches panics at this
/// boundary so a single handler cannot terminate Axum or unrelated workers.
pub trait JobHandler: Send + Sync {
    /// Executes a leased job. Implementations must treat the context as
    /// immutable ownership evidence and return an expected failure rather than
    /// panicking for normal operational errors.
    fn execute(
        &self,
        context: JobExecutionContext,
    ) -> impl Future<Output = Result<(), JobExecutionError>> + Send;
}

/// Outcome of one non-blocking `JobRuntime::execute_next_at` worker turn.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerTurn {
    Idle,
    Succeeded {
        job_id: JobId,
        diagnostics: Option<ExecutionDiagnostics>,
    },
    RetryScheduled {
        job_id: JobId,
        failure: JobFailureCode,
        diagnostics: Option<ExecutionDiagnostics>,
    },
    Failed {
        job_id: JobId,
        failure: JobFailureCode,
        diagnostics: Option<ExecutionDiagnostics>,
    },
    Cancelled {
        job_id: JobId,
        checkpoint_persisted: bool,
        diagnostics: Option<ExecutionDiagnostics>,
    },
    StoragePressure {
        job_id: JobId,
        state: JobState,
        checkpoint_persisted: bool,
    },
}

/// Control disposition for an error at the generic worker boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerRuntimeDisposition {
    /// The worker may continue polling after a transient infrastructure issue.
    Continue,
    /// Another owner won the lease; the worker may continue polling normally.
    LeaseLost,
    /// Durable invariants or runtime policy are unsafe; the worker must stop.
    Fatal,
}

/// Failure that prevents the generic worker boundary from safely proceeding.
#[derive(Debug, thiserror::Error)]
pub enum JobRuntimeError {
    #[error("worker policy is invalid")]
    InvalidPolicy,
    #[error("durable job queue operation failed")]
    Repository(#[source] JobRepositoryError),
}

impl JobRuntimeError {
    #[must_use]
    pub fn disposition(&self) -> WorkerRuntimeDisposition {
        match self {
            Self::InvalidPolicy => WorkerRuntimeDisposition::Fatal,
            Self::Repository(error) => match error {
                JobRepositoryError::LeaseLost
                | JobRepositoryError::Checkpoint(DbCheckpointRepositoryError::LeaseLost) => {
                    WorkerRuntimeDisposition::LeaseLost
                }
                JobRepositoryError::Database(error)
                | JobRepositoryError::Checkpoint(DbCheckpointRepositoryError::Database(error)) => {
                    db_error_disposition(error)
                }
                JobRepositoryError::InvalidJobKind
                | JobRepositoryError::InvalidMaxAttempts
                | JobRepositoryError::NotFound
                | JobRepositoryError::IllegalTransition
                | JobRepositoryError::AttemptsExhausted
                | JobRepositoryError::RemovalUnsafe
                | JobRepositoryError::NotReprioritizable
                | JobRepositoryError::ActionAlreadyActive
                | JobRepositoryError::RetryAlreadyContinued
                | JobRepositoryError::QueueInvariant
                | JobRepositoryError::Checkpoint(
                    DbCheckpointRepositoryError::UnsupportedFormatVersion
                    | DbCheckpointRepositoryError::InvalidEnvelope
                    | DbCheckpointRepositoryError::PayloadTooLarge
                    | DbCheckpointRepositoryError::Malformed
                    | DbCheckpointRepositoryError::Serialization
                    | DbCheckpointRepositoryError::NotFound,
                ) => WorkerRuntimeDisposition::Fatal,
                JobRepositoryError::StorageAdmissionBlocked => WorkerRuntimeDisposition::Continue,
            },
        }
    }

    #[must_use]
    pub const fn safe_code(&self) -> &'static str {
        match self {
            Self::InvalidPolicy => "WORKER_POLICY_INVALID",
            Self::Repository(error) => match error {
                JobRepositoryError::Database(error) if error.is_durable_invariant() => {
                    "DATABASE_INVARIANT"
                }
                JobRepositoryError::Checkpoint(DbCheckpointRepositoryError::Database(error)) => {
                    if error.is_durable_invariant() {
                        "DATABASE_INVARIANT"
                    } else {
                        "JOB_REPOSITORY_RUNTIME_ERROR"
                    }
                }
                JobRepositoryError::QueueInvariant => "QUEUE_INVARIANT",
                JobRepositoryError::InvalidJobKind | JobRepositoryError::InvalidMaxAttempts => {
                    "JOB_REPOSITORY_INVARIANT"
                }
                JobRepositoryError::LeaseLost
                | JobRepositoryError::Checkpoint(DbCheckpointRepositoryError::LeaseLost) => {
                    "LEASE_LOST"
                }
                JobRepositoryError::Checkpoint(
                    DbCheckpointRepositoryError::UnsupportedFormatVersion
                    | DbCheckpointRepositoryError::InvalidEnvelope
                    | DbCheckpointRepositoryError::PayloadTooLarge
                    | DbCheckpointRepositoryError::Malformed
                    | DbCheckpointRepositoryError::Serialization
                    | DbCheckpointRepositoryError::NotFound,
                ) => "CHECKPOINT_RUNTIME_ERROR",
                JobRepositoryError::StorageAdmissionBlocked
                | JobRepositoryError::NotFound
                | JobRepositoryError::IllegalTransition
                | JobRepositoryError::AttemptsExhausted
                | JobRepositoryError::RemovalUnsafe
                | JobRepositoryError::NotReprioritizable
                | JobRepositoryError::ActionAlreadyActive
                | JobRepositoryError::RetryAlreadyContinued
                | JobRepositoryError::Database(_) => "JOB_REPOSITORY_RUNTIME_ERROR",
            },
        }
    }

    #[must_use]
    pub fn telemetry_disposition(&self) -> TelemetryWorkerRuntimeDisposition {
        telemetry_runtime_disposition(self.disposition())
    }
}

fn db_error_disposition(error: &DbError) -> WorkerRuntimeDisposition {
    if error.is_durable_invariant() {
        WorkerRuntimeDisposition::Fatal
    } else {
        WorkerRuntimeDisposition::Continue
    }
}

/// Generic Tokio-ready single-worker runtime. A caller can run one turn from a
/// supervisor/poll loop without coupling job execution to HTTP route lifetime.
#[derive(Clone, Debug)]
pub struct JobRuntime<'database> {
    database: ErabiDatabase,
    repository: JobRepository<'database>,
    worker_id: String,
    policy: WorkerPolicy,
    cancellation: CancellationController,
    storage_pressure: StoragePressureMonitor,
    terminal_progress_repair_failure_for_test: Arc<Mutex<Option<JobRepositoryError>>>,
}

impl<'database> JobRuntime<'database> {
    /// Creates a worker whose identity is also persisted in every lease and
    /// attempt. The identity is durable evidence, not a transient task id.
    ///
    /// # Errors
    /// Returns an error when the worker identity or bounded lease/retry policy
    /// is invalid.
    pub fn new(
        database: &'database ErabiDatabase,
        worker_id: impl Into<String>,
        policy: WorkerPolicy,
    ) -> Result<Self, JobRuntimeError> {
        Self::with_cancellation_controller(
            database,
            worker_id,
            policy,
            CancellationController::default(),
        )
    }

    /// Creates a worker joined to a process/runtime cancellation controller.
    /// The controller is the bridge used by graceful shutdown to signal active
    /// handlers without aborting them.
    ///
    /// # Errors
    /// Returns an error when the worker identity or bounded lease/retry policy
    /// is invalid.
    pub fn with_cancellation_controller(
        database: &'database ErabiDatabase,
        worker_id: impl Into<String>,
        policy: WorkerPolicy,
        cancellation: CancellationController,
    ) -> Result<Self, JobRuntimeError> {
        Self::with_storage_pressure_monitor(
            database,
            worker_id,
            policy,
            cancellation,
            StoragePressureMonitor::unavailable(StoragePressurePolicy::default()),
        )
    }

    /// Creates a worker with a probe bound to Erabi's authoritative data path.
    /// The monitor is shared with runtime/API state through its controller.
    ///
    /// # Errors
    /// Returns an error when the worker identity or bounded lease/retry policy
    /// is invalid.
    pub fn with_storage_pressure_monitor(
        database: &'database ErabiDatabase,
        worker_id: impl Into<String>,
        policy: WorkerPolicy,
        cancellation: CancellationController,
        storage_pressure: StoragePressureMonitor,
    ) -> Result<Self, JobRuntimeError> {
        let worker_id = worker_id.into();
        if !policy.valid() || worker_id.is_empty() || worker_id.len() > 128 {
            return Err(JobRuntimeError::InvalidPolicy);
        }
        Ok(Self {
            database: database.clone(),
            repository: JobRepository::new(database),
            worker_id,
            policy,
            cancellation,
            storage_pressure,
            terminal_progress_repair_failure_for_test: Arc::new(Mutex::new(None)),
        })
    }

    /// Returns the controller used by handlers and process shutdown to signal
    /// active work without aborting its task.
    #[must_use]
    pub fn cancellation_controller(&self) -> CancellationController {
        self.cancellation.clone()
    }

    /// Injects one typed terminal-progress repair result for an integration
    /// boundary test. It does not alter the durable CrawlRun or queue truth.
    #[doc(hidden)]
    #[must_use]
    pub fn with_terminal_progress_repair_failure_for_test(
        mut self,
        error: JobRepositoryError,
    ) -> Self {
        self.terminal_progress_repair_failure_for_test = Arc::new(Mutex::new(Some(error)));
        self
    }

    /// Requests cancellation for one job. Queued work is durably cancelled so
    /// it cannot be scheduled; active work receives the cooperative signal.
    ///
    /// # Errors
    /// Returns an error when the durable queue cannot inspect or update the job.
    pub async fn request_cancellation(
        &self,
        job_id: &JobId,
        now: i64,
    ) -> Result<JobState, JobRuntimeError> {
        request_job_cancellation(&self.database, &self.cancellation, job_id, now).await
    }

    /// Executes at most one eligible job using supplied deterministic time.
    /// Panics become a bounded retry/failure outcome rather than escaping the
    /// worker boundary.
    ///
    /// # Errors
    /// Returns an error when durable lease/attempt state cannot be read or
    /// updated safely, or when the configured retry time overflows.
    pub async fn execute_next_at<H: JobHandler>(
        &self,
        handler: &H,
        now: i64,
    ) -> Result<WorkerTurn, JobRuntimeError> {
        if self.cancellation.shutdown_requested() {
            return Ok(WorkerTurn::Idle);
        }
        let pressure_state = self.storage_pressure.refresh();
        let Some(acquired) = self
            .repository
            .acquire_next_with_storage_admission(
                &self.worker_id,
                now,
                self.policy.lease_duration_seconds,
                pressure_state.allows_artifact_heavy(),
            )
            .await
            .map_err(JobRuntimeError::Repository)?
        else {
            emit(SemanticEvent::WorkerPollIdle);
            return Ok(WorkerTurn::Idle);
        };
        let telemetry_context = telemetry_job_context_from_acquired(&acquired);
        emit(SemanticEvent::WorkerJobAcquired {
            context: telemetry_context,
            attempt_number: acquired.attempt.attempt_number,
            lease_generation: acquired.attempt.lease_generation,
        });
        let cancellation = self.cancellation.register(&acquired.job.id);
        let storage_pressure = self
            .storage_pressure
            .controller()
            .register(&acquired.job.id, acquired.job.storage_class());
        let current_lease = acquired
            .job
            .lease
            .clone()
            .ok_or(JobRuntimeError::Repository(
                JobRepositoryError::QueueInvariant,
            ));
        let current_lease = match current_lease {
            Ok(lease) => lease,
            Err(error) => {
                self.cancellation.release(&acquired.job.id, false);
                self.storage_pressure.controller().release(&acquired.job.id);
                return Err(error);
            }
        };
        let started = Instant::now();
        let checkpoint_writer = CheckpointWriter {
            database: self.database.clone(),
            job_id: acquired.job.id.clone(),
            attempt_id: acquired.attempt.id.clone(),
            lease: Arc::new(RwLock::new(current_lease.clone())),
            persisted: Arc::new(AtomicBool::new(false)),
            initial_now: now,
            started,
        };
        let context = JobExecutionContext {
            job_id: acquired.job.id.clone(),
            kind: acquired.job.kind.clone(),
            attempt_id: acquired.attempt.id.clone(),
            attempt_number: acquired.attempt.attempt_number,
            worker_id: self.worker_id.clone(),
            lease: current_lease,
            cancellation,
            storage_pressure,
            checkpoint_writer,
            terminal_failure: Arc::new(AtomicBool::new(false)),
            diagnostics: Arc::new(Mutex::new(ExecutionDiagnostics::new())),
            terminal_crawl_run: Arc::new(Mutex::new(None)),
        };
        let attempt_span = JobAttemptSpan::new(&telemetry_context, acquired.attempt.attempt_number);
        let outcome = attempt_span
            .run(Box::pin(
                self.execute_acquired(handler, context, now, started),
            ))
            .await;
        if let Ok(turn) = &outcome {
            emit_turn_telemetry(&telemetry_context, turn);
        }
        self.cancellation.release(
            &acquired.job.id,
            matches!(
                outcome,
                Ok(WorkerTurn::Succeeded { .. }
                    | WorkerTurn::Failed { .. }
                    | WorkerTurn::Cancelled { .. })
            ),
        );
        self.storage_pressure.controller().release(&acquired.job.id);
        outcome
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_acquired<H: JobHandler>(
        &self,
        handler: &H,
        context: JobExecutionContext,
        now: i64,
        started: Instant,
    ) -> Result<WorkerTurn, JobRuntimeError> {
        let mut current_lease = context.lease.clone();
        let mut heartbeat = interval_at(
            started + self.policy.heartbeat_interval(),
            self.policy.heartbeat_interval(),
        );
        let handler = AssertUnwindSafe(handler.execute(context.clone())).catch_unwind();
        tokio::pin!(handler);

        let result = loop {
            tokio::select! {
                result = &mut handler => break result,
                _ = heartbeat.tick() => {
                    let heartbeat_now = current_queue_time(now, started)?;
                    // Active artifact-heavy work must observe real filesystem
                    // transitions without depending on another worker or a
                    // diagnostics request. The monitor owns the probe and the
                    // controller publishes its cooperative token.
                    let _ = self.storage_pressure.refresh();
                    match self
                        .repository
                        .heartbeat(
                            &context.job_id,
                            &current_lease,
                            heartbeat_now,
                            self.policy.lease_duration_seconds,
                        )
                        .await
                    {
                        Ok(renewed_lease) => {
                            current_lease = renewed_lease.clone();
                            context.checkpoint_writer.update_lease(renewed_lease).await;
                        }
                        Err(error) => {
                            // Lease loss revokes durable authority first, then signals
                            // the handler to reach its existing cooperative boundary.
                            // The lease is already revoked, so the handler result
                            // cannot change durable state. Observe every completion
                            // branch explicitly rather than silently discarding it.
                            context.cancellation.cancel();
                            match handler.await {
                                Ok(Ok(()) | Err(JobExecutionError)) | Err(_) => {}
                            }
                            return Err(JobRuntimeError::Repository(error));
                        }
                    }
                }
            }
        };
        let completed_at = current_queue_time(now, started)?;
        let diagnostics = context.take_diagnostics();
        if let Some(commit) = context.take_terminal_crawl_run() {
            return self
                .reconcile_terminal_commit(
                    &context,
                    &current_lease,
                    completed_at,
                    commit,
                    diagnostics,
                    result,
                )
                .await;
        }
        if context.cancellation.is_cancelled() {
            let checkpoint_persisted = context.checkpoint_writer.persisted();
            self.repository
                .cancel(&context.job_id, &current_lease, completed_at)
                .await
                .map_err(JobRuntimeError::Repository)?;
            return Ok(WorkerTurn::Cancelled {
                job_id: context.job_id,
                checkpoint_persisted,
                diagnostics,
            });
        }
        if context.storage_pressure.is_signalled() {
            let checkpoint_persisted = context.checkpoint_writer.persisted();
            let state = self
                .repository
                .requeue_after_storage_pressure(&context.job_id, &current_lease, completed_at)
                .await
                .map_err(JobRuntimeError::Repository)?;
            return Ok(WorkerTurn::StoragePressure {
                job_id: context.job_id,
                state,
                checkpoint_persisted,
            });
        }
        match result {
            Ok(Ok(())) => {
                self.repository
                    .succeed(&context.job_id, &current_lease, completed_at)
                    .await
                    .map_err(JobRuntimeError::Repository)?;
                Ok(WorkerTurn::Succeeded {
                    job_id: context.job_id,
                    diagnostics,
                })
            }
            Ok(Err(JobExecutionError)) => {
                self.record_failure(
                    &context,
                    &current_lease,
                    completed_at,
                    JobFailureCode::HandlerFailed,
                    context.terminal_failure_requested(),
                    diagnostics,
                )
                .await
            }
            Err(_) => {
                self.record_failure(
                    &context,
                    &current_lease,
                    completed_at,
                    JobFailureCode::HandlerPanicked,
                    false,
                    diagnostics,
                )
                .await
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn reconcile_terminal_commit(
        &self,
        context: &JobExecutionContext,
        lease: &JobLease,
        now: i64,
        commit: TerminalCrawlRunCommit,
        diagnostics: Option<ExecutionDiagnostics>,
        result: Result<Result<(), JobExecutionError>, Box<dyn std::any::Any + Send>>,
    ) -> Result<WorkerTurn, JobRuntimeError> {
        let mut diagnostics = diagnostics.unwrap_or_default();
        if matches!(result, Ok(Err(_)) | Err(_)) && diagnostics.primary.is_none() {
            diagnostics.add_primary(
                ExecutionDiagnostic::new(
                    OrchestrationErrorCategory::Finalization,
                    ExecutionOperation::ReconcileTerminality,
                    ExecutionAction::Reconcile,
                    "HANDLER_FAILED_AFTER_TERMINAL_COMMIT",
                )
                .with_run(commit.run_id),
            );
        }
        let reconciliation = self
            .repository
            .reconcile_terminal_crawl_run(&context.job_id, lease, &commit.run_id.to_string(), now)
            .await
            .map_err(JobRuntimeError::Repository)?;
        let telemetry_context = telemetry_id(&commit.run_id.to_string())
            .map_or(telemetry_job_context(context), |id| {
                telemetry_job_context(context).with_crawl_run_id(id)
            });
        if let Some(status) = telemetry_terminal_status(commit.status) {
            emit(SemanticEvent::CrawlRunTerminalCommitted {
                context: telemetry_context,
                status,
            });
            emit(SemanticEvent::JobTerminalReconciled {
                context: telemetry_context,
                job_state: telemetry_job_state(reconciliation.state),
                run_status: status,
            });
        }
        if !commit.terminal_progress_durable {
            if let Some(status) = telemetry_terminal_status(commit.status) {
                emit(SemanticEvent::ProgressTerminalRepairPending {
                    context: telemetry_context,
                    status,
                });
            }
            let injected_failure = self
                .terminal_progress_repair_failure_for_test
                .lock()
                .ok()
                .and_then(|mut failure| failure.take());
            let repair = if let Some(error) = injected_failure {
                Err(error)
            } else {
                self.repository
                    .append_terminal_progress_if_missing(
                        &context.job_id,
                        reconciliation.progress,
                        now,
                    )
                    .await
            };
            match repair {
                Ok(()) => {
                    if let Some(status) = telemetry_terminal_status(commit.status) {
                        emit(SemanticEvent::ProgressTerminalRepaired {
                            context: telemetry_context,
                            status,
                        });
                    }
                }
                Err(error) => {
                    if matches!(error, JobRepositoryError::QueueInvariant)
                        && terminal_progress_contradiction(
                            &self.database,
                            &context.job_id,
                            reconciliation.progress,
                        )
                        .await
                    {
                        emit(SemanticEvent::ProgressTerminalContradiction {
                            context: telemetry_context,
                            code: telemetry_code("TERMINAL_PROGRESS_CONTRADICTION"),
                        });
                    }
                    let runtime_error = JobRuntimeError::Repository(error);
                    if runtime_error.disposition() == WorkerRuntimeDisposition::Fatal {
                        // The CrawlRun and Job/Attempt reconciliation above is
                        // already committed and remains authoritative. A fatal
                        // repair error is a worker invariant, not a projection
                        // diagnostic to demote or retry through provider work.
                        return Err(runtime_error);
                    }
                    diagnostics.add_secondary(
                        ExecutionDiagnostic::new(
                            OrchestrationErrorCategory::ProgressPublication,
                            ExecutionOperation::AppendProgress,
                            ExecutionAction::Reconcile,
                            "TERMINAL_PROGRESS_REPAIR_FAILED",
                        )
                        .with_run(commit.run_id)
                        .with_terminal_outcome(commit.status),
                    );
                }
            }
        }
        let diagnostics = (!diagnostics.is_empty()).then_some(diagnostics);
        match reconciliation.state {
            JobState::Succeeded => Ok(WorkerTurn::Succeeded {
                job_id: context.job_id.clone(),
                diagnostics,
            }),
            JobState::Failed => Ok(WorkerTurn::Failed {
                job_id: context.job_id.clone(),
                failure: JobFailureCode::HandlerFailed,
                diagnostics,
            }),
            JobState::Cancelled => Ok(WorkerTurn::Cancelled {
                job_id: context.job_id.clone(),
                checkpoint_persisted: context.checkpoint_writer.persisted(),
                diagnostics,
            }),
            JobState::Queued | JobState::Running => Err(JobRuntimeError::Repository(
                JobRepositoryError::QueueInvariant,
            )),
        }
    }

    async fn record_failure(
        &self,
        context: &JobExecutionContext,
        lease: &JobLease,
        now: i64,
        failure: JobFailureCode,
        terminal: bool,
        diagnostics: Option<ExecutionDiagnostics>,
    ) -> Result<WorkerTurn, JobRuntimeError> {
        if self.is_production_context(context).await? {
            self.repository
                .fail_terminal_production(&context.job_id, lease, now, failure)
                .await
                .map_err(JobRuntimeError::Repository)?;
            return Ok(WorkerTurn::Failed {
                job_id: context.job_id.clone(),
                failure,
                diagnostics,
            });
        }
        if terminal {
            self.repository
                .fail_terminal(&context.job_id, lease, now, failure)
                .await
                .map_err(JobRuntimeError::Repository)?;
            return Ok(WorkerTurn::Failed {
                job_id: context.job_id.clone(),
                failure,
                diagnostics,
            });
        }
        let retry_at = now
            .checked_add(self.policy.retry_delay_seconds)
            .ok_or(JobRuntimeError::InvalidPolicy)?;
        match self
            .repository
            .fail(&context.job_id, lease, now, failure, retry_at)
            .await
            .map_err(JobRuntimeError::Repository)?
        {
            JobState::Queued => Ok(WorkerTurn::RetryScheduled {
                job_id: context.job_id.clone(),
                failure,
                diagnostics,
            }),
            JobState::Failed => Ok(WorkerTurn::Failed {
                job_id: context.job_id.clone(),
                failure,
                diagnostics,
            }),
            _ => Err(JobRuntimeError::Repository(
                JobRepositoryError::QueueInvariant,
            )),
        }
    }

    async fn is_production_context(
        &self,
        context: &JobExecutionContext,
    ) -> Result<bool, JobRuntimeError> {
        if context.kind().as_str() == "PRODUCTION_CRAWL" {
            return Ok(true);
        }
        if !matches!(
            context.kind().as_str(),
            "RETRY"
                | "RETRY_FAILED_PARTS"
                | "RESUME_CHECKPOINT"
                | "RERUN_FULL_CRAWL"
                | "RESTART_FROM_BEGINNING"
        ) {
            return Ok(false);
        }
        let job = self
            .repository
            .job(&context.job_id)
            .await
            .map_err(JobRuntimeError::Repository)?;
        let Some(run_id) = job.crawl_run_id.as_deref() else {
            return Ok(false);
        };
        let Ok(uuid) = uuid::Uuid::parse_str(run_id) else {
            return Err(JobRuntimeError::Repository(
                JobRepositoryError::QueueInvariant,
            ));
        };
        let Some(run_id) = erabi_domain::CrawlRunId::from_uuid(uuid) else {
            return Err(JobRuntimeError::Repository(
                JobRepositoryError::QueueInvariant,
            ));
        };
        let snapshot = CrawlRunRepository::new(&self.database)
            .snapshot(run_id)
            .await
            .map_err(|_| JobRuntimeError::Repository(JobRepositoryError::QueueInvariant))?;
        Ok(snapshot.run_type() == erabi_domain::CrawlRunType::ProductionRun)
    }
}

async fn terminal_progress_contradiction(
    database: &ErabiDatabase,
    job_id: &JobId,
    expected: ProgressTerminalState,
) -> bool {
    let Ok(request) = ProgressReplayRequest::new(None, 256) else {
        return false;
    };
    let Ok(page) = ProgressRepository::new(database)
        .replay(job_id, request)
        .await
    else {
        return false;
    };
    let mut terminal = None;
    for event in page.events {
        if let Some(status) = event.terminal
            && (terminal.replace(status).is_some() || status != expected)
        {
            return true;
        }
    }
    false
}

/// Shared Task 3 cancellation boundary used by both workers and explicit API
/// actions. It durably cancels queued work and only signals active work.
///
/// # Errors
/// Returns a typed runtime error when the durable state cannot be inspected or
/// updated safely.
pub async fn request_job_cancellation(
    database: &ErabiDatabase,
    cancellation: &CancellationController,
    job_id: &JobId,
    now: i64,
) -> Result<JobState, JobRuntimeError> {
    cancellation.request(job_id);
    let state = JobRepository::new(database)
        .cancel_queued(job_id, now)
        .await
        .map_err(JobRuntimeError::Repository)?;
    if matches!(
        state,
        JobState::Succeeded | JobState::Failed | JobState::Cancelled
    ) {
        cancellation.retire_after_terminal_boundary(job_id);
    }
    Ok(state)
}

fn current_queue_time(initial_now: i64, started: Instant) -> Result<i64, JobRuntimeError> {
    let elapsed =
        i64::try_from(started.elapsed().as_secs()).map_err(|_| JobRuntimeError::InvalidPolicy)?;
    initial_now
        .checked_add(elapsed)
        .ok_or(JobRuntimeError::InvalidPolicy)
}

/// Performs Plan 03's startup hooks with durable queue state as authority.
/// This does not start a handler or introduce cancellation semantics.
///
/// # Errors
/// Returns an error when stale-job recovery or durable concurrency rebuilding
/// detects queue corruption or cannot complete its database work.
pub async fn recover_and_rebuild_at(
    database: &ErabiDatabase,
    now: i64,
) -> Result<(StaleJobRecovery, ConcurrencyState), JobRepositoryError> {
    let repository = JobRepository::new(database);
    let recovery = repository.recover_stale_jobs(now).await?;
    let concurrency = repository.rebuild_concurrency_state(now).await?;
    Ok((recovery, concurrency))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, future::Future, path::Path};

    use erabi_db::{
        DbError, ErabiDatabase, MigrationFailure, MigrationFailureState, MigrationRunner,
        repositories::{
            CheckpointRepositoryError, CrawlExecutionRepository, CrawlExecutionSummary,
            CrawlRunRepository, JobKind, JobRepository, JobState, NewJob, ProgressRepository,
        },
    };
    use erabi_domain::{
        CrawlRunId, CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunStatus, CrawlRunType,
        ResolvedValue, RobotsAudit, RunConfiguration, SettingSource, SnapshotOperationalSettings,
    };

    use super::*;

    #[test]
    fn worker_runtime_dispositions_keep_transient_errors_pollable() {
        assert_eq!(
            JobRuntimeError::Repository(JobRepositoryError::Database(DbError::Invariant(
                "test".to_owned(),
            )))
            .disposition(),
            WorkerRuntimeDisposition::Fatal
        );
        assert_eq!(
            JobRuntimeError::Repository(JobRepositoryError::StorageAdmissionBlocked).disposition(),
            WorkerRuntimeDisposition::Continue
        );
        assert_eq!(
            JobRuntimeError::Repository(JobRepositoryError::LeaseLost).disposition(),
            WorkerRuntimeDisposition::LeaseLost
        );
        assert_eq!(
            JobRuntimeError::Repository(JobRepositoryError::Checkpoint(
                CheckpointRepositoryError::LeaseLost,
            ))
            .disposition(),
            WorkerRuntimeDisposition::LeaseLost
        );
        assert_eq!(
            JobRuntimeError::Repository(JobRepositoryError::QueueInvariant).disposition(),
            WorkerRuntimeDisposition::Fatal
        );
        assert_eq!(
            JobRuntimeError::Repository(JobRepositoryError::Checkpoint(
                CheckpointRepositoryError::Malformed,
            ))
            .disposition(),
            WorkerRuntimeDisposition::Fatal
        );
        assert_eq!(
            JobRuntimeError::Repository(JobRepositoryError::Database(DbError::Serialization(
                "test".to_owned()
            ),))
            .disposition(),
            WorkerRuntimeDisposition::Fatal
        );
        assert_eq!(
            JobRuntimeError::Repository(JobRepositoryError::Database(DbError::MigrationFailure {
                failure: MigrationFailure {
                    version: None,
                    state: MigrationFailureState::ChecksumMismatch,
                    message: "test".to_owned(),
                },
            },))
            .disposition(),
            WorkerRuntimeDisposition::Fatal
        );
    }

    #[tokio::test]
    async fn duplicate_enqueue_raw_constraint_is_a_fatal_database_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = ErabiDatabase::in_memory().await?;
        MigrationRunner::default().apply(&database).await?;
        let job = NewJob::new(JobKind::new("TYPED_TRANSIENT")?, 0, 0, 1)?;
        let repository = JobRepository::new(&database);
        repository.enqueue(&job, 0).await?;
        let error = match repository.enqueue(&job, 1).await {
            Ok(()) => return Err("duplicate enqueue unexpectedly succeeded".into()),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            JobRepositoryError::Database(DbError::Turso(_))
        ));
        assert_eq!(
            JobRuntimeError::Repository(error).disposition(),
            WorkerRuntimeDisposition::Fatal
        );
        Ok(())
    }

    #[derive(Clone, Copy)]
    struct HealthyStorageProbe;

    impl StorageProbe for HealthyStorageProbe {
        fn free_bytes(&self, _path: &Path) -> Result<u64, StorageProbeError> {
            Ok(u64::MAX)
        }
    }

    struct ContradictoryTerminalHandler {
        database: ErabiDatabase,
        run_id: CrawlRunId,
        contradictory: bool,
    }

    impl JobHandler for ContradictoryTerminalHandler {
        fn execute(
            &self,
            context: JobExecutionContext,
        ) -> impl Future<Output = Result<(), JobExecutionError>> + Send {
            let database = self.database.clone();
            let run_id = self.run_id;
            let contradictory = self.contradictory;
            async move {
                CrawlExecutionRepository::new(&database)
                    .finalize(
                        &CrawlExecutionSummary {
                            crawl_run_id: run_id,
                            in_scope_pages_planned: 0,
                            in_scope_pages_completed: 0,
                            pagination_truncation_count: 0,
                            unresolved_partial_work_count: 0,
                            page_type_ambiguity_count: 0,
                        },
                        CrawlRunStatus::Succeeded,
                    )
                    .await
                    .map_err(|_| JobExecutionError)?;
                if contradictory {
                    ProgressRepository::new(&database)
                        .append_at(
                            &NewProgressEvent::terminal(
                                context.job_id().clone(),
                                ProgressTerminalState::Failed,
                                ProgressMetadata::default(),
                            )
                            .map_err(|_| JobExecutionError)?,
                            1,
                        )
                        .await
                        .map_err(|_| JobExecutionError)?;
                }
                context.mark_terminal_crawl_run(run_id, CrawlRunStatus::Succeeded);
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn runtime_turn_fails_fatally_on_contradictory_terminal_progress()
    -> Result<(), Box<dyn std::error::Error>> {
        fn resolved<T>(value: T) -> ResolvedValue<T> {
            ResolvedValue {
                value,
                source: SettingSource::BuiltInDefault,
            }
        }

        let database = ErabiDatabase::in_memory().await?;
        MigrationRunner::default().apply(&database).await?;
        let run_id = CrawlRunId::new();
        let snapshot = CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
            run_type: CrawlRunType::QuickScrape,
            configuration: RunConfiguration::QuickScrape {
                target_url: "https://example.test/item".parse()?,
                ad_hoc_configuration: BTreeMap::new(),
            },
            selected_seed_ids: Vec::new(),
            run_profile_id: None,
            settings: SnapshotOperationalSettings {
                max_pages: resolved(1),
                max_depth: resolved(0),
                max_duration_seconds: resolved(60),
                concurrency: resolved(1),
                request_delay_ms: resolved(0),
                timeout_ms: resolved(1_000),
                screenshot: resolved(false),
                asset_download_limit_bytes: resolved(1_000_000),
                retain_artifacts: resolved(false),
                user_agent: resolved("Erabi/0.1".to_owned()),
            },
            robots: RobotsAudit::respect(
                "operator",
                "unix:1",
                "https://example.test",
                "Erabi/0.1",
                None,
            ),
            actor: "operator".to_owned(),
            created_at: "unix:1".to_owned(),
        })?;
        CrawlRunRepository::new(&database)
            .create(run_id, CrawlRunStatus::Queued, &snapshot)
            .await?;
        let mut job = NewJob::new(JobKind::new("TEST_WORK")?, 0, 0, 1)?;
        job.crawl_run_id = Some(run_id.to_string());
        JobRepository::new(&database).enqueue(&job, 0).await?;

        let runtime = JobRuntime::with_storage_pressure_monitor(
            &database,
            "contradictory-terminal-worker",
            WorkerPolicy::conservative(),
            CancellationController::default(),
            StoragePressureMonitor::new(
                HealthyStorageProbe,
                "contradictory-terminal-worker-data",
                StoragePressurePolicy::default(),
            ),
        )?;
        let result = runtime
            .execute_next_at(
                &ContradictoryTerminalHandler {
                    database: database.clone(),
                    run_id,
                    contradictory: true,
                },
                2,
            )
            .await;
        assert!(matches!(
            result,
            Err(JobRuntimeError::Repository(
                JobRepositoryError::QueueInvariant
            ))
        ));
        assert_eq!(
            CrawlRunRepository::new(&database).status(run_id).await?,
            CrawlRunStatus::Succeeded
        );
        assert_eq!(
            JobRepository::new(&database).job(&job.id).await?.state,
            JobState::Succeeded
        );
        let progress = ProgressRepository::new(&database)
            .replay(&job.id, ProgressReplayRequest::new(None, 16)?)
            .await?;
        assert_eq!(
            progress
                .events
                .iter()
                .filter(|event| event.terminal.is_some())
                .count(),
            1
        );
        assert_eq!(
            progress.events[0].terminal,
            Some(ProgressTerminalState::Failed)
        );
        Ok(())
    }

    #[tokio::test]
    async fn runtime_turn_fails_fatally_on_database_invariant_during_terminal_repair()
    -> Result<(), Box<dyn std::error::Error>> {
        fn resolved<T>(value: T) -> ResolvedValue<T> {
            ResolvedValue {
                value,
                source: SettingSource::BuiltInDefault,
            }
        }

        let database = ErabiDatabase::in_memory().await?;
        MigrationRunner::default().apply(&database).await?;
        let run_id = CrawlRunId::new();
        let snapshot = CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
            run_type: CrawlRunType::QuickScrape,
            configuration: RunConfiguration::QuickScrape {
                target_url: "https://example.test/item".parse()?,
                ad_hoc_configuration: BTreeMap::new(),
            },
            selected_seed_ids: Vec::new(),
            run_profile_id: None,
            settings: SnapshotOperationalSettings {
                max_pages: resolved(1),
                max_depth: resolved(0),
                max_duration_seconds: resolved(60),
                concurrency: resolved(1),
                request_delay_ms: resolved(0),
                timeout_ms: resolved(1_000),
                screenshot: resolved(false),
                asset_download_limit_bytes: resolved(1_000_000),
                retain_artifacts: resolved(false),
                user_agent: resolved("Erabi/0.1".to_owned()),
            },
            robots: RobotsAudit::respect(
                "operator",
                "unix:1",
                "https://example.test",
                "Erabi/0.1",
                None,
            ),
            actor: "operator".to_owned(),
            created_at: "unix:1".to_owned(),
        })?;
        CrawlRunRepository::new(&database)
            .create(run_id, CrawlRunStatus::Queued, &snapshot)
            .await?;
        let mut job = NewJob::new(JobKind::new("TEST_WORK")?, 0, 0, 1)?;
        job.crawl_run_id = Some(run_id.to_string());
        JobRepository::new(&database).enqueue(&job, 0).await?;

        let runtime = JobRuntime::with_storage_pressure_monitor(
            &database,
            "fatal-terminal-repair-worker",
            WorkerPolicy::conservative(),
            CancellationController::default(),
            StoragePressureMonitor::new(
                HealthyStorageProbe,
                "fatal-terminal-repair-worker-data",
                StoragePressurePolicy::default(),
            ),
        )?
        .with_terminal_progress_repair_failure_for_test(JobRepositoryError::Database(
            DbError::Invariant("terminal repair invariant".to_owned()),
        ));
        let result = runtime
            .execute_next_at(
                &ContradictoryTerminalHandler {
                    database: database.clone(),
                    run_id,
                    contradictory: false,
                },
                2,
            )
            .await;
        assert!(matches!(
            result,
            Err(JobRuntimeError::Repository(JobRepositoryError::Database(
                DbError::Invariant(_)
            )))
        ));
        assert_eq!(
            CrawlRunRepository::new(&database).status(run_id).await?,
            CrawlRunStatus::Succeeded
        );
        assert_eq!(
            JobRepository::new(&database).job(&job.id).await?.state,
            JobState::Succeeded
        );
        assert!(
            ProgressRepository::new(&database)
                .replay(&job.id, ProgressReplayRequest::new(None, 16)?)
                .await?
                .events
                .iter()
                .all(|event| event.terminal.is_none())
        );
        Ok(())
    }

    #[test]
    fn primary_diagnostics_are_preserved_when_secondary_failures_follow() {
        let provider = ExecutionDiagnostic::new(
            OrchestrationErrorCategory::Provider,
            ExecutionOperation::ProviderExecution,
            ExecutionAction::Retry,
            "PROVIDER_REMOTE_FAILURE",
        )
        .with_provider("crawler-adapter");
        let pacing = ExecutionDiagnostic::new(
            OrchestrationErrorCategory::Pacing,
            ExecutionOperation::RecordOutcome,
            ExecutionAction::Continue,
            "PACING_OUTCOME_RECORD_FAILED",
        );
        let progress = ExecutionDiagnostic::new(
            OrchestrationErrorCategory::ProgressPublication,
            ExecutionOperation::AppendProgress,
            ExecutionAction::Reconcile,
            "PROGRESS_DURABLE_APPEND_FAILED",
        );
        let mut diagnostics = ExecutionDiagnostics::new();
        diagnostics.add_primary(provider.clone());
        diagnostics.add_secondary(pacing.clone());
        diagnostics.add_primary(progress.clone());

        assert_eq!(diagnostics.primary, Some(provider));
        assert_eq!(diagnostics.secondary, vec![pacing, progress]);
        assert_eq!(diagnostics.secondary.len(), 2);
        assert!(
            diagnostics
                .secondary
                .iter()
                .all(|diagnostic| diagnostic.code != "PROVIDER_REMOTE_FAILURE")
        );
    }
}
