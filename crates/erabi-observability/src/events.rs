use crate::CorrelationContext;

/// A short, validated, non-content telemetry code.
#[derive(Clone, Eq, PartialEq)]
pub struct TelemetryCode(String);

impl TelemetryCode {
    /// Validates the bounded code vocabulary accepted by semantic events.
    #[must_use]
    pub fn new(value: &str) -> Option<Self> {
        if (1..=64).contains(&value.len())
            && value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            Some(Self(value.to_owned()))
        } else {
            None
        }
    }

    /// Converts a trusted static business code without allowing invalid text
    /// to enter telemetry. Invalid implementation codes become one fixed
    /// bounded fallback and are never echoed.
    #[must_use]
    pub fn from_static(value: &'static str) -> Self {
        Self::new(value).unwrap_or_else(|| Self("UNSAFE_CODE".to_owned()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

macro_rules! bounded_enum {
    ($(#[$meta:meta])* $name:ident { $($(#[$variant_meta:meta])* $variant:ident => $value:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Eq, PartialEq)]
        pub enum $name {
            $($(#[$variant_meta])* $variant),+
        }

        impl $name {
            pub(crate) const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }
    };
}

bounded_enum! {
    /// Bounded execution operation vocabulary mirrored from DX-D01.
    ExecutionOperation {
        LoadJob => "LOAD_JOB",
        LoadRunSnapshot => "LOAD_RUN_SNAPSHOT",
        LoadCheckpoint => "LOAD_CHECKPOINT",
        TransitionRun => "TRANSITION_RUN",
        AcquireAdmission => "ACQUIRE_ADMISSION",
        ProviderExecution => "PROVIDER_EXECUTION",
        RecordOutcome => "RECORD_OUTCOME",
        PersistArtifact => "PERSIST_ARTIFACT",
        PersistExecution => "PERSIST_EXECUTION",
        FinalizeRun => "FINALIZE_RUN",
        AppendProgress => "APPEND_PROGRESS",
        PublishProgress => "PUBLISH_PROGRESS",
        ReconcileTerminality => "RECONCILE_TERMINALITY",
        QueueLifecycle => "QUEUE_LIFECYCLE",
        Serialization => "SERIALIZATION"
    }
}

bounded_enum! {
    /// Bounded diagnostic category vocabulary mirrored from DX-D01.
    ExecutionCategory {
        Repository => "REPOSITORY",
        LeaseClaim => "LEASE_CLAIM",
        CheckpointRecovery => "CHECKPOINT_RECOVERY",
        Provider => "PROVIDER",
        NetworkAdmission => "NETWORK_ADMISSION",
        Pacing => "PACING",
        Artifact => "ARTIFACT",
        SerializationProjection => "SERIALIZATION_PROJECTION",
        Finalization => "FINALIZATION",
        ProgressPublication => "PROGRESS_PUBLICATION",
        Invariant => "INVARIANT"
    }
}

bounded_enum! {
    /// Bounded action vocabulary mirrored from DX-D01.
    ExecutionAction {
        Continue => "CONTINUE",
        Retry => "RETRY",
        Fail => "FAIL",
        Reconcile => "RECONCILE",
        Publish => "PUBLISH"
    }
}

bounded_enum! {
    /// Bounded worker disposition vocabulary mirrored from DX-D01.
    WorkerRuntimeDisposition {
        Continue => "CONTINUE",
        LeaseLost => "LEASE_LOST",
        Fatal => "FATAL"
    }
}

bounded_enum! {
    WorkerTurnOutcome {
        Idle => "IDLE",
        Succeeded => "SUCCEEDED",
        RetryScheduled => "RETRY_SCHEDULED",
        Failed => "FAILED",
        Cancelled => "CANCELLED",
        StoragePressure => "STORAGE_PRESSURE"
    }
}

bounded_enum! {
    EventOutcome {
        Success => "SUCCESS",
        Failure => "FAILURE",
        Accepted => "ACCEPTED",
        Rejected => "REJECTED",
        Durable => "DURABLE",
        Reconstructed => "RECONSTRUCTED",
        Missing => "MISSING",
        Halted => "HALTED",
        NotProcessed => "NOT_PROCESSED"
    }
}

bounded_enum! {
    AdmissionOutcome {
        Allowed => "ALLOWED",
        Rejected => "REJECTED",
        Failed => "FAILED"
    }
}

bounded_enum! {
    RobotsDecisionToken {
        Allowed => "ALLOWED",
        Disallowed => "DISALLOWED",
        Overridden => "OVERRIDDEN",
        Failed => "FAILED"
    }
}

bounded_enum! {
    RobotsEvidence {
        NetworkPolicy => "NETWORK_POLICY",
        Cache => "CACHE",
        NotFound => "NOT_FOUND",
        AccessDenied => "ACCESS_DENIED",
        Evaluated => "EVALUATED"
    }
}

bounded_enum! {
    ArtifactKind {
        Html => "HTML",
        Screenshot => "SCREENSHOT",
        Pdf => "PDF",
        Other => "OTHER"
    }
}

bounded_enum! {
    CheckpointPhase {
        Initialized => "INITIALIZED",
        Running => "RUNNING",
        Finalizing => "FINALIZING",
        Completed => "COMPLETED",
        Recovery => "RECOVERY"
    }
}

bounded_enum! {
    RecoveryAction {
        Prepared => "PREPARED",
        Reconstructed => "RECONSTRUCTED",
        Repaired => "REPAIRED",
        Continue => "CONTINUE",
        FailClosed => "FAIL_CLOSED"
    }
}

bounded_enum! {
    ProviderToken {
        Crawl4Ai => "CRAWL4AI",
        Unavailable => "UNAVAILABLE"
    }
}

bounded_enum! {
    JobActionToken {
        RetryFailedParts => "RETRY_FAILED_PARTS",
        RerunFullCrawl => "RERUN_FULL_CRAWL",
        ResumeCheckpoint => "RESUME_CHECKPOINT",
        RestartFromBeginning => "RESTART_FROM_BEGINNING",
        Retry => "RETRY",
        Cancel => "CANCEL",
        Reprioritize => "REPRIORITIZE",
        Remove => "REMOVE"
    }
}

bounded_enum! {
    JobStateToken {
        Queued => "QUEUED",
        Running => "RUNNING",
        Succeeded => "SUCCEEDED",
        Failed => "FAILED",
        Cancelled => "CANCELLED"
    }
}

bounded_enum! {
    TerminalStatus {
        Succeeded => "SUCCEEDED",
        PartialResult => "PARTIAL_RESULT",
        Failed => "FAILED",
        Cancelled => "CANCELLED"
    }
}

bounded_enum! {
    RuntimeMode {
        Server => "SERVER",
        Test => "TEST"
    }
}

/// Bounded DX-D01 diagnostic fields carried by completion and secondary events.
#[derive(Clone, Eq, PartialEq)]
pub struct DiagnosticFields {
    pub code: TelemetryCode,
    pub operation: ExecutionOperation,
    pub category: ExecutionCategory,
    pub action: ExecutionAction,
    pub provider: Option<ProviderToken>,
    pub terminal_status: Option<TerminalStatus>,
}

impl DiagnosticFields {
    #[must_use]
    pub const fn new(
        code: TelemetryCode,
        operation: ExecutionOperation,
        category: ExecutionCategory,
        action: ExecutionAction,
    ) -> Self {
        Self {
            code,
            operation,
            category,
            action,
            provider: None,
            terminal_status: None,
        }
    }

    #[must_use]
    pub const fn with_provider(mut self, provider: ProviderToken) -> Self {
        self.provider = Some(provider);
        self
    }

    #[must_use]
    pub const fn with_terminal_status(mut self, status: TerminalStatus) -> Self {
        self.terminal_status = Some(status);
        self
    }
}

/// Closed semantic event vocabulary. There is deliberately no arbitrary
/// event-name or field-map constructor.
#[derive(Clone, Eq, PartialEq)]
pub enum SemanticEvent {
    RuntimeStarted {
        mode: RuntimeMode,
    },
    RuntimeShutdownStarted,
    RuntimeShutdownCompleted {
        outcome: EventOutcome,
    },
    RuntimeWorkerTerminated {
        code: TelemetryCode,
        fatal: bool,
    },
    WorkerPollIdle,
    WorkerJobAcquired {
        context: CorrelationContext,
        attempt_number: u32,
        lease_generation: u64,
    },
    WorkerTurnCompleted {
        context: CorrelationContext,
        outcome: WorkerTurnOutcome,
        failure_code: Option<TelemetryCode>,
        primary: Option<DiagnosticFields>,
        secondary_count: u8,
    },
    WorkerTurnRuntimeFailure {
        code: TelemetryCode,
        disposition: WorkerRuntimeDisposition,
    },
    WorkerFatal {
        code: TelemetryCode,
    },
    JobLeaseLost {
        context: CorrelationContext,
    },
    JobRetryScheduled {
        context: CorrelationContext,
        failure_code: TelemetryCode,
    },
    JobCancelled {
        context: CorrelationContext,
    },
    JobRecoveryPrepared {
        context: CorrelationContext,
        action: RecoveryAction,
        generation: u64,
        recovered_count: u64,
        outcome: EventOutcome,
    },
    JobActionEnqueued {
        context: CorrelationContext,
        action: JobActionToken,
    },
    NetworkAdmissionDecided {
        context: CorrelationContext,
        outcome: AdmissionOutcome,
        code: Option<TelemetryCode>,
    },
    RobotsEvaluated {
        context: CorrelationContext,
        decision: RobotsDecisionToken,
        evidence: RobotsEvidence,
        code: Option<TelemetryCode>,
    },
    PacingAcquireCompleted {
        context: CorrelationContext,
        outcome: EventOutcome,
        duration_ms: u64,
    },
    ProviderExecuteCompleted {
        context: CorrelationContext,
        provider: ProviderToken,
        outcome: EventOutcome,
        duration_ms: u64,
        code: Option<TelemetryCode>,
    },
    ExecutionSecondaryFailure {
        context: CorrelationContext,
        diagnostic: DiagnosticFields,
    },
    ArtifactPersisted {
        context: CorrelationContext,
        kind: ArtifactKind,
        count: u64,
        bytes: u64,
        outcome: EventOutcome,
    },
    CheckpointPersisted {
        context: CorrelationContext,
        version: u16,
        phase: CheckpointPhase,
        bytes: u64,
        work_generation: u64,
        outcome: EventOutcome,
    },
    CheckpointRecovered {
        context: CorrelationContext,
        version: u16,
        phase: CheckpointPhase,
        bytes: u64,
        work_generation: u64,
        outcome: EventOutcome,
    },
    RecoveryReconstructed {
        context: CorrelationContext,
        action: RecoveryAction,
        generation: u64,
        recovered_count: u64,
        outcome: EventOutcome,
    },
    CrawlRunTerminalCommitted {
        context: CorrelationContext,
        status: TerminalStatus,
    },
    JobTerminalReconciled {
        context: CorrelationContext,
        job_state: JobStateToken,
        run_status: TerminalStatus,
    },
    ProgressTerminalPublished {
        context: CorrelationContext,
        status: TerminalStatus,
    },
    ProgressTerminalDurableOnly {
        context: CorrelationContext,
        status: TerminalStatus,
    },
    ProgressTerminalRepairPending {
        context: CorrelationContext,
        status: TerminalStatus,
    },
    ProgressTerminalRepaired {
        context: CorrelationContext,
        status: TerminalStatus,
    },
    ProgressTerminalContradiction {
        context: CorrelationContext,
        code: TelemetryCode,
    },
    QuickScrapeAccepted {
        context: CorrelationContext,
        item_count: u64,
    },
    QuickScrapeRejected {
        context: CorrelationContext,
        code: TelemetryCode,
        item_count: u64,
        validation_rejected_count: u64,
        system_error_count: u64,
        not_processed_count: u64,
        halted: bool,
        operational: bool,
    },
    QuickScrapeBatchAccepted {
        context: CorrelationContext,
        item_count: u64,
        accepted_count: u64,
        validation_rejected_count: u64,
        system_error_count: u64,
        not_processed_count: u64,
        halted: bool,
    },
    QuickScrapeBatchRejected {
        context: CorrelationContext,
        item_count: u64,
        accepted_count: u64,
        validation_rejected_count: u64,
        system_error_count: u64,
        not_processed_count: u64,
        halted: bool,
        code: TelemetryCode,
    },
    ProductionAccepted {
        context: CorrelationContext,
    },
    ProductionRejected {
        context: CorrelationContext,
        code: TelemetryCode,
        operational: bool,
    },
    JobActionAccepted {
        context: CorrelationContext,
        action: JobActionToken,
    },
    JobActionRejected {
        context: CorrelationContext,
        action: JobActionToken,
        code: TelemetryCode,
        operational: bool,
    },
}

macro_rules! emit_event {
    ($level:ident, $name:literal, $context:expr, $($fields:tt)*) => {{
        tracing::$level!(
            target: crate::fields::ERABI_TELEMETRY_TARGET,
            event_name = $name,
            job_id = ?$context.job_id(),
            attempt_id = ?$context.attempt_id(),
            crawl_run_id = ?$context.crawl_run_id(),
            crawl_execution_id = ?$context.crawl_execution_id(),
            crawler_id = ?$context.crawler_id(),
            crawler_version_id = ?$context.crawler_version_id(),
            source_job_id = ?$context.source_job_id(),
            action_job_id = ?$context.action_job_id(),
            $($fields)*
        );
    }};
}

/// Emits a typed semantic event at its fixed policy level.
#[allow(clippy::too_many_lines)]
pub fn emit(event: SemanticEvent) {
    match event {
        SemanticEvent::RuntimeStarted { mode } => {
            emit_event!(
                info,
                "runtime.started",
                CorrelationContext::new(),
                mode = mode.as_str(),
            );
        }
        SemanticEvent::RuntimeShutdownStarted => {
            emit_event!(info, "runtime.shutdown.started", CorrelationContext::new(),);
        }
        SemanticEvent::RuntimeShutdownCompleted { outcome } => {
            emit_event!(
                info,
                "runtime.shutdown.completed",
                CorrelationContext::new(),
                outcome = outcome.as_str(),
            );
        }
        SemanticEvent::RuntimeWorkerTerminated { code, fatal } => {
            if fatal {
                emit_event!(
                    error,
                    "runtime.worker.terminated",
                    CorrelationContext::new(),
                    code = code.as_str(),
                );
            } else {
                emit_event!(
                    info,
                    "runtime.worker.terminated",
                    CorrelationContext::new(),
                    code = code.as_str(),
                );
            }
        }
        SemanticEvent::WorkerPollIdle => {
            emit_event!(trace, "worker.poll.idle", CorrelationContext::new(),);
        }
        SemanticEvent::WorkerJobAcquired {
            context,
            attempt_number,
            lease_generation,
        } => {
            emit_event!(
                info,
                "worker.job.acquired",
                &context,
                attempt_number = attempt_number,
                lease_generation = lease_generation,
            );
        }
        SemanticEvent::WorkerTurnCompleted {
            context,
            outcome,
            failure_code,
            primary,
            secondary_count,
        } => {
            let primary_code = primary.as_ref().map(|value| value.code.as_str());
            let primary_operation = primary.as_ref().map(|value| value.operation.as_str());
            let primary_category = primary.as_ref().map(|value| value.category.as_str());
            let primary_action = primary.as_ref().map(|value| value.action.as_str());
            if outcome == WorkerTurnOutcome::Idle {
                emit_event!(trace, "worker.turn.completed", &context,
                    outcome = outcome.as_str(),
                    failure_code = ?failure_code.as_ref().map(TelemetryCode::as_str),
                    primary_error_code = ?primary_code,
                    operation = ?primary_operation,
                    category = ?primary_category,
                    action = ?primary_action,
                    secondary_count = u64::from(secondary_count),
                );
            } else {
                emit_event!(info, "worker.turn.completed", &context,
                    outcome = outcome.as_str(),
                    failure_code = ?failure_code.as_ref().map(TelemetryCode::as_str),
                    primary_error_code = ?primary_code,
                    operation = ?primary_operation,
                    category = ?primary_category,
                    action = ?primary_action,
                    secondary_count = u64::from(secondary_count),
                );
            }
        }
        SemanticEvent::WorkerTurnRuntimeFailure { code, disposition } => {
            emit_event!(
                warn,
                "worker.turn.runtime_failure",
                CorrelationContext::new(),
                code = code.as_str(),
                disposition = disposition.as_str(),
            );
        }
        SemanticEvent::WorkerFatal { code } => {
            emit_event!(
                error,
                "worker.fatal",
                CorrelationContext::new(),
                code = code.as_str(),
            );
        }
        SemanticEvent::JobLeaseLost { context } => {
            emit_event!(debug, "job.lease.lost", &context,);
        }
        SemanticEvent::JobRetryScheduled {
            context,
            failure_code,
        } => {
            emit_event!(
                info,
                "job.retry.scheduled",
                &context,
                failure_code = failure_code.as_str(),
            );
        }
        SemanticEvent::JobCancelled { context } => {
            emit_event!(info, "job.cancelled", &context,);
        }
        SemanticEvent::JobRecoveryPrepared {
            context,
            action,
            generation,
            recovered_count,
            outcome,
        } => {
            emit_event!(
                info,
                "job.recovery.prepared",
                &context,
                action = action.as_str(),
                generation = generation,
                recovered_count = recovered_count,
                outcome = outcome.as_str(),
            );
        }
        SemanticEvent::JobActionEnqueued { context, action } => {
            emit_event!(
                info,
                "job.action.enqueued",
                &context,
                action = action.as_str(),
            );
        }
        SemanticEvent::NetworkAdmissionDecided {
            context,
            outcome,
            code,
        } => {
            emit_event!(debug, "network.admission.decided", &context, outcome = outcome.as_str(), code = ?code.as_ref().map(TelemetryCode::as_str),);
        }
        SemanticEvent::RobotsEvaluated {
            context,
            decision,
            evidence,
            code,
        } => {
            emit_event!(debug, "robots.evaluated", &context, decision = decision.as_str(), evidence = evidence.as_str(), code = ?code.as_ref().map(TelemetryCode::as_str),);
        }
        SemanticEvent::PacingAcquireCompleted {
            context,
            outcome,
            duration_ms,
        } => {
            emit_event!(
                debug,
                "pacing.acquire.completed",
                &context,
                outcome = outcome.as_str(),
                duration_ms = duration_ms,
            );
        }
        SemanticEvent::ProviderExecuteCompleted {
            context,
            provider,
            outcome,
            duration_ms,
            code,
        } => {
            let level = outcome;
            if matches!(level, EventOutcome::Failure | EventOutcome::Rejected) {
                emit_event!(warn, "provider.execute.completed", &context, provider = provider.as_str(), outcome = outcome.as_str(), duration_ms = duration_ms, code = ?code.as_ref().map(TelemetryCode::as_str),);
            } else {
                emit_event!(debug, "provider.execute.completed", &context, provider = provider.as_str(), outcome = outcome.as_str(), duration_ms = duration_ms, code = ?code.as_ref().map(TelemetryCode::as_str),);
            }
        }
        SemanticEvent::ExecutionSecondaryFailure {
            context,
            diagnostic,
        } => {
            emit_event!(warn, "execution.secondary_failure", &context,
                diagnostic_role = "SECONDARY",
                error_code = diagnostic.code.as_str(),
                operation = diagnostic.operation.as_str(),
                category = diagnostic.category.as_str(),
                action = diagnostic.action.as_str(),
                provider = ?diagnostic.provider.map(ProviderToken::as_str),
                terminal_status = ?diagnostic.terminal_status.map(TerminalStatus::as_str),
            );
        }
        SemanticEvent::ArtifactPersisted {
            context,
            kind,
            count,
            bytes,
            outcome,
        } => {
            emit_event!(
                debug,
                "artifact.persisted",
                &context,
                artifact_kind = kind.as_str(),
                artifact_count = count,
                artifact_bytes = bytes,
                outcome = outcome.as_str(),
            );
        }
        SemanticEvent::CheckpointPersisted {
            context,
            version,
            phase,
            bytes,
            work_generation,
            outcome,
        } => {
            emit_event!(
                debug,
                "checkpoint.persisted",
                &context,
                checkpoint_version = u64::from(version),
                checkpoint_phase = phase.as_str(),
                checkpoint_bytes = bytes,
                work_generation = work_generation,
                outcome = outcome.as_str(),
            );
        }
        SemanticEvent::CheckpointRecovered {
            context,
            version,
            phase,
            bytes,
            work_generation,
            outcome,
        } => {
            emit_event!(
                info,
                "checkpoint.recovered",
                &context,
                checkpoint_version = u64::from(version),
                checkpoint_phase = phase.as_str(),
                checkpoint_bytes = bytes,
                work_generation = work_generation,
                outcome = outcome.as_str(),
            );
        }
        SemanticEvent::RecoveryReconstructed {
            context,
            action,
            generation,
            recovered_count,
            outcome,
        } => {
            emit_event!(
                info,
                "recovery.reconstructed",
                &context,
                action = action.as_str(),
                generation = generation,
                recovered_count = recovered_count,
                outcome = outcome.as_str(),
            );
        }
        SemanticEvent::CrawlRunTerminalCommitted { context, status } => {
            emit_event!(
                info,
                "crawl_run.terminal_committed",
                &context,
                terminal_status = status.as_str(),
            );
        }
        SemanticEvent::JobTerminalReconciled {
            context,
            job_state,
            run_status,
        } => {
            emit_event!(
                info,
                "job.terminal_reconciled",
                &context,
                terminal_job_state = job_state.as_str(),
                terminal_run_status = run_status.as_str(),
            );
        }
        SemanticEvent::ProgressTerminalPublished { context, status } => {
            emit_event!(
                info,
                "progress.terminal_published",
                &context,
                terminal_status = status.as_str(),
            );
        }
        SemanticEvent::ProgressTerminalDurableOnly { context, status } => {
            emit_event!(
                warn,
                "progress.terminal_durable_only",
                &context,
                terminal_status = status.as_str(),
            );
        }
        SemanticEvent::ProgressTerminalRepairPending { context, status } => {
            emit_event!(
                warn,
                "progress.terminal_repair_pending",
                &context,
                terminal_status = status.as_str(),
            );
        }
        SemanticEvent::ProgressTerminalRepaired { context, status } => {
            emit_event!(
                info,
                "progress.terminal_repaired",
                &context,
                terminal_status = status.as_str(),
            );
        }
        SemanticEvent::ProgressTerminalContradiction { context, code } => {
            emit_event!(
                error,
                "progress.terminal_contradiction",
                &context,
                code = code.as_str(),
            );
        }
        SemanticEvent::QuickScrapeAccepted {
            context,
            item_count,
        } => {
            emit_event!(
                info,
                "submission.quick_scrape.accepted",
                &context,
                item_count = item_count,
            );
        }
        SemanticEvent::QuickScrapeRejected {
            context,
            code,
            item_count,
            validation_rejected_count,
            system_error_count,
            not_processed_count,
            halted,
            operational,
        } => {
            if operational {
                emit_event!(
                    warn,
                    "submission.quick_scrape.rejected",
                    &context,
                    code = code.as_str(),
                    item_count = item_count,
                    validation_rejected_count = validation_rejected_count,
                    system_error_count = system_error_count,
                    not_processed_count = not_processed_count,
                    halted = halted,
                );
            } else {
                emit_event!(
                    debug,
                    "submission.quick_scrape.rejected",
                    &context,
                    code = code.as_str(),
                    item_count = item_count,
                    validation_rejected_count = validation_rejected_count,
                    system_error_count = system_error_count,
                    not_processed_count = not_processed_count,
                    halted = halted,
                );
            }
        }
        SemanticEvent::QuickScrapeBatchAccepted {
            context,
            item_count,
            accepted_count,
            validation_rejected_count,
            system_error_count,
            not_processed_count,
            halted,
        } => {
            emit_event!(
                info,
                "submission.quick_scrape.accepted",
                &context,
                item_count = item_count,
                accepted_count = accepted_count,
                validation_rejected_count = validation_rejected_count,
                system_error_count = system_error_count,
                not_processed_count = not_processed_count,
                halted = halted,
            );
        }
        SemanticEvent::QuickScrapeBatchRejected {
            context,
            item_count,
            accepted_count,
            validation_rejected_count,
            system_error_count,
            not_processed_count,
            halted,
            code,
        } => {
            if system_error_count > 0 {
                emit_event!(
                    warn,
                    "submission.quick_scrape.rejected",
                    &context,
                    code = code.as_str(),
                    item_count = item_count,
                    accepted_count = accepted_count,
                    validation_rejected_count = validation_rejected_count,
                    system_error_count = system_error_count,
                    not_processed_count = not_processed_count,
                    halted = halted,
                );
            } else {
                emit_event!(
                    debug,
                    "submission.quick_scrape.rejected",
                    &context,
                    code = code.as_str(),
                    item_count = item_count,
                    accepted_count = accepted_count,
                    validation_rejected_count = validation_rejected_count,
                    system_error_count = system_error_count,
                    not_processed_count = not_processed_count,
                    halted = halted,
                );
            }
        }
        SemanticEvent::ProductionAccepted { context } => {
            emit_event!(info, "submission.production.accepted", &context,);
        }
        SemanticEvent::ProductionRejected {
            context,
            code,
            operational,
        } => {
            if operational {
                emit_event!(
                    warn,
                    "submission.production.rejected",
                    &context,
                    code = code.as_str(),
                );
            } else {
                emit_event!(
                    debug,
                    "submission.production.rejected",
                    &context,
                    code = code.as_str(),
                );
            }
        }
        SemanticEvent::JobActionAccepted { context, action } => {
            emit_event!(
                info,
                "job_action.accepted",
                &context,
                action = action.as_str(),
            );
        }
        SemanticEvent::JobActionRejected {
            context,
            action,
            code,
            operational,
        } => {
            if operational {
                emit_event!(
                    warn,
                    "job_action.rejected",
                    &context,
                    action = action.as_str(),
                    code = code.as_str(),
                );
            } else {
                emit_event!(
                    debug,
                    "job_action.rejected",
                    &context,
                    action = action.as_str(),
                    code = code.as_str(),
                );
            }
        }
    }
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use super::*;
    use crate::TelemetryId;
    use crate::test_support::{Capture, CapturedRecord};

    fn event_records(capture: &Capture, name: &str) -> Vec<crate::test_support::CapturedEvent> {
        capture
            .records()
            .into_iter()
            .filter_map(|record| match record {
                CapturedRecord::Event(event)
                    if event
                        .fields
                        .iter()
                        .any(|field| field.name == "event_name" && field.value == name) =>
                {
                    Some(event)
                }
                CapturedRecord::Event(_) | CapturedRecord::Span(_) => None,
            })
            .collect()
    }

    #[test]
    fn invalid_codes_are_rejected_without_echoing_content() {
        let sentinel = "DO_NOT_LOG_PROVIDER_BODY_42019!";
        assert!(TelemetryCode::new(sentinel).is_none());
        assert_eq!(
            TelemetryCode::from_static("lower-case").as_str(),
            "UNSAFE_CODE"
        );
    }

    #[test]
    fn acquired_and_attempt_events_use_only_fixed_correlation_slots() {
        let capture = Capture::new();
        let job_id = TelemetryId::new_v7();
        let attempt_id = TelemetryId::new_v7();
        let context = CorrelationContext::new()
            .with_job_id(job_id)
            .with_attempt_id(attempt_id);
        block_on(capture.run(async {
            emit(SemanticEvent::WorkerJobAcquired {
                context,
                attempt_number: 1,
                lease_generation: 2,
            });
        }));

        let records = event_records(&capture, "worker.job.acquired");
        assert_eq!(records.len(), 1);
        let fields = &records[0].fields;
        assert!(fields.iter().any(|field| field.name == "job_id"));
        assert!(fields.iter().any(|field| field.name == "attempt_id"));
        assert!(!fields.iter().any(|field| field.name == "worker_id"));
        assert!(!fields.iter().any(|field| field.name == "lease_id"));
        assert!(
            !fields
                .iter()
                .any(|field| field.name == "DO_NOT_LOG_TARGET_URL_42017")
        );
    }

    #[test]
    fn diagnostic_events_preserve_bounded_primary_and_secondary_fields() {
        let capture = Capture::new();
        let context = CorrelationContext::new().with_job_id(TelemetryId::new_v7());
        let primary = DiagnosticFields::new(
            TelemetryCode::from_static("PROVIDER_FAILED"),
            ExecutionOperation::ProviderExecution,
            ExecutionCategory::Provider,
            ExecutionAction::Retry,
        )
        .with_provider(ProviderToken::Crawl4Ai);
        let secondary = primary.clone();
        block_on(capture.run(async {
            emit(SemanticEvent::WorkerTurnCompleted {
                context,
                outcome: WorkerTurnOutcome::RetryScheduled,
                failure_code: Some(TelemetryCode::from_static("HANDLER_FAILED")),
                primary: Some(primary),
                secondary_count: 1,
            });
            emit(SemanticEvent::ExecutionSecondaryFailure {
                context,
                diagnostic: secondary,
            });
        }));

        let completion = event_records(&capture, "worker.turn.completed");
        assert_eq!(completion.len(), 1);
        assert_eq!(completion[0].level, crate::TelemetryLevel::Info);
        for field_name in [
            "primary_error_code",
            "operation",
            "category",
            "action",
            "secondary_count",
        ] {
            assert!(
                completion[0]
                    .fields
                    .iter()
                    .any(|field| field.name == field_name)
            );
        }
        let secondary_events = event_records(&capture, "execution.secondary_failure");
        assert_eq!(secondary_events.len(), 1);
        assert!(
            secondary_events[0]
                .fields
                .iter()
                .all(|field| !field.value.contains("DO_NOT_LOG_PROVIDER_BODY_42019"))
        );
    }

    #[test]
    fn durable_only_is_a_distinct_terminal_event() {
        let capture = Capture::new();
        block_on(capture.run(async {
            emit(SemanticEvent::ProgressTerminalDurableOnly {
                context: CorrelationContext::new(),
                status: TerminalStatus::Succeeded,
            });
        }));
        assert_eq!(
            event_records(&capture, "progress.terminal_durable_only").len(),
            1
        );
        assert!(event_records(&capture, "progress.terminal_repair_pending").is_empty());
    }

    fn block_on<F>(future: F) -> F::Output
    where
        F: std::future::Future,
    {
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        let mut future = std::pin::pin!(future);
        loop {
            match future.as_mut().poll(&mut context) {
                std::task::Poll::Ready(output) => return output,
                std::task::Poll::Pending => std::thread::yield_now(),
            }
        }
    }
}
