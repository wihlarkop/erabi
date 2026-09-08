//! Dependency-light, safety-oriented telemetry primitives for Erabi.

mod context;
mod events;
mod fields;
mod identity;
mod span;
mod subscriber;

#[cfg(feature = "test-support")]
pub mod test_support;

pub use context::CorrelationContext;
pub use events::{
    AdmissionOutcome, ArtifactKind, CheckpointPhase, DiagnosticFields, EventOutcome,
    ExecutionAction, ExecutionCategory, ExecutionOperation, JobActionToken, JobStateToken,
    ProviderToken, RecoveryAction, RobotsDecisionToken, RobotsEvidence, RuntimeMode, SemanticEvent,
    TelemetryCode, TerminalStatus, WorkerRuntimeDisposition, WorkerTurnOutcome, emit,
};
pub use fields::{HttpMethod, RouteTemplate};
pub use identity::{RequestTraceId, TelemetryId, TelemetryIdError};
pub use span::{CrawlExecutionSpan, HttpRequestSpan, JobAttemptSpan, WorkerLifecycleSpan};
pub use subscriber::{
    ConfigurationWarning, InstallOutcome, TelemetryConfig, TelemetryFormat, TelemetryLevel,
    emit_startup_configuration_warning, install,
};
