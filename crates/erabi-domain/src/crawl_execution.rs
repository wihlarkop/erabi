//! Provider-neutral durable page-execution semantics.

/// The outcome of one Erabi page execution, independent of any acquisition
/// provider's status vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrawlExecutionOutcome {
    Completed,
    Partial,
    Failed,
    Cancelled,
}

/// A bounded, provider-neutral classification for an execution failure or
/// partial/cancelled result.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrawlExecutionErrorCode {
    AccessDenied,
    NotFound,
    Timeout,
    ProviderUnavailable,
    InvalidResponse,
    RateLimited,
    RemoteFailure,
    UnsupportedCapability,
    PartialResult,
    Cancelled,
    RobotsExcluded,
    PageTypeAmbiguous,
    StoragePressure,
}
