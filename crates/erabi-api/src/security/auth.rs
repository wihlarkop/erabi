//! Bearer authentication that keeps credentials out of responses and spans.

use axum::{
    extract::State,
    http::{HeaderValue, Request, StatusCode},
    middleware::Next,
    response::Response,
};
use secrecy::ExposeSecret;
use subtle::ConstantTimeEq;

use crate::{
    error::{ApiErrorEnvelope, error_response},
    security::SecurityConfig,
};

use super::super::app::trace_id_for;

/// Enforces remote bearer authentication for every protected router branch.
pub(crate) async fn require_bearer(
    State(config): State<SecurityConfig>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if !config.requires_bearer_authentication() || request.method() == axum::http::Method::OPTIONS {
        return next.run(request).await;
    }

    let trace_id = trace_id_for(&request);
    let Some(header) = request.headers().get(axum::http::header::AUTHORIZATION) else {
        return error_response(
            StatusCode::UNAUTHORIZED,
            ApiErrorEnvelope::new(
                "AUTHENTICATION_REQUIRED",
                "Bearer authentication is required for this route.",
                trace_id,
            ),
        );
    };

    let Some(provided) = parse_bearer(header) else {
        return error_response(
            StatusCode::UNAUTHORIZED,
            ApiErrorEnvelope::new(
                "INVALID_BEARER_TOKEN",
                "The Authorization header must use a bearer token.",
                trace_id,
            ),
        );
    };
    let Some(expected) = config.access_token() else {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiErrorEnvelope::new(
                "SECURITY_CONFIGURATION_INVALID",
                "The server security configuration is unavailable.",
                trace_id,
            ),
        );
    };

    if !bool::from(
        provided
            .as_bytes()
            .ct_eq(expected.expose_secret().as_bytes()),
    ) {
        return error_response(
            StatusCode::FORBIDDEN,
            ApiErrorEnvelope::new(
                "AUTHENTICATION_FAILED",
                "The bearer token was not accepted.",
                trace_id,
            ),
        );
    }

    next.run(request).await
}

fn parse_bearer(value: &HeaderValue) -> Option<&str> {
    let value = value.to_str().ok()?;
    let token = value.strip_prefix("Bearer ")?;
    if token.is_empty() || token.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return None;
    }
    Some(token)
}
