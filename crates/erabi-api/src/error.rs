//! Stable, safe API error responses.

use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;

/// An optional recovery hint for an expected failure.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Recoverability {
    /// Whether retrying can be meaningful after the listed action.
    pub recoverable: bool,
    /// Safe user-facing actions; never a generic bypass of a domain invariant.
    pub actions: Vec<String>,
}

/// Version-stable JSON envelope for every API error emitted by this crate.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ApiErrorEnvelope {
    /// Stable machine-readable code for client logic.
    pub code: String,
    /// Safe message suitable for a user-facing error state.
    pub message: String,
    /// Optional structured diagnostic details that contain no secrets or raw content.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    /// Optional bounded recovery guidance for expected errors.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recoverability: Option<Recoverability>,
    /// A safe request identifier, returned also as `X-Erabi-Trace-Id`.
    pub trace_id: String,
}

impl ApiErrorEnvelope {
    /// Creates a safe error envelope for the supplied trace.
    #[must_use]
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        trace_id: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
            recoverability: None,
            trace_id: trace_id.into(),
        }
    }

    /// Adds structured details that have already been classified as safe.
    #[must_use]
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    /// Adds safe recovery guidance.
    #[must_use]
    pub fn with_recoverability(mut self, recoverability: Recoverability) -> Self {
        self.recoverability = Some(recoverability);
        self
    }
}

/// Converts a safe envelope into an HTTP JSON response.
pub(crate) fn error_response(
    status: StatusCode,
    envelope: ApiErrorEnvelope,
) -> axum::response::Response {
    (status, Json(envelope)).into_response()
}
