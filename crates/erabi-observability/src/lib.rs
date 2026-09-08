//! Dependency-light, safety-oriented telemetry primitives for Erabi.

mod fields;
mod identity;
mod span;
mod subscriber;

#[cfg(feature = "test-support")]
pub mod test_support;

pub use fields::{HttpMethod, RouteTemplate};
pub use identity::{RequestTraceId, TelemetryId, TelemetryIdError};
pub use span::HttpRequestSpan;
pub use subscriber::{
    ConfigurationWarning, InstallOutcome, TelemetryConfig, TelemetryFormat, TelemetryLevel,
    emit_startup_configuration_warning, install,
};
