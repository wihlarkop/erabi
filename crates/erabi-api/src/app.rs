//! Hardened route composition for the stable `/api/v1` boundary.

use axum::{
    Json, Router,
    extract::{Extension, State},
    http::{HeaderName, HeaderValue, Request, StatusCode},
    middleware,
    response::{Html, IntoResponse, Response},
    routing::{any, get},
};
use serde::Serialize;
use tracing::Instrument;
use uuid::Uuid;

use crate::{
    AppState, SecurityConfig,
    error::{ApiErrorEnvelope, error_response},
    security::{apply_security_headers, enforce_browser_request_policy, require_bearer},
};

const TRACE_HEADER: HeaderName = HeaderName::from_static("x-erabi-trace-id");

/// Safe request trace identity generated or propagated by the outer shell layer.
#[derive(Clone, Debug)]
pub(crate) struct TraceId(String);

impl TraceId {
    fn from_request(request: &Request<axum::body::Body>) -> Self {
        let trace_id = request
            .headers()
            .get(&TRACE_HEADER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| is_safe_trace_id(value))
            .map_or_else(|| Uuid::now_v7().to_string(), ToOwned::to_owned);
        Self(trace_id)
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

/// Builds the versioned API, protected future-surface groups, and SPA boundary.
///
/// The protected router deliberately owns API, SSE, static assets, export,
/// backup, raw-artifact, diagnostics, and SPA surfaces in one place. Later
/// route modules attach below this boundary instead of creating parallel,
/// accidentally unauthenticated routers.
pub fn build_router(app_state: AppState, security: SecurityConfig) -> Router {
    let liveness = Router::new().route("/api/v1/health", get(liveness));

    let protected = Router::new()
        .route("/api/v1/readiness", get(readiness))
        .route("/api/v1/diagnostics/{*path}", any(unavailable))
        .route("/api/v1/events/{*path}", any(unavailable))
        .route("/api/v1/assets/{*path}", any(unavailable))
        .route("/api/v1/exports/{*path}", any(unavailable))
        .route("/api/v1/backups/{*path}", any(unavailable))
        .route("/api/v1/artifacts/{*path}", any(unavailable))
        .route("/assets/{*path}", any(unavailable))
        .route("/", get(spa_boundary))
        .route("/{*path}", get(spa_boundary))
        .fallback(unavailable)
        .with_state(app_state)
        .layer(middleware::from_fn_with_state(
            security.clone(),
            enforce_browser_request_policy,
        ))
        .layer(middleware::from_fn_with_state(security, require_bearer));

    liveness
        .merge(protected)
        .layer(middleware::from_fn(apply_security_headers))
        .layer(middleware::from_fn(trace_request))
}

async fn liveness() -> Json<LivenessResponse> {
    Json(LivenessResponse { status: "live" })
}

async fn readiness(
    State(app_state): State<AppState>,
    Extension(trace_id): Extension<TraceId>,
) -> Response {
    if app_state.is_ready() {
        return Json(ReadinessResponse { status: "ready" }).into_response();
    }
    error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        ApiErrorEnvelope::new(
            "NOT_READY",
            "The service has not completed startup.",
            trace_id.as_str(),
        ),
    )
}

async fn unavailable(Extension(trace_id): Extension<TraceId>) -> Response {
    error_response(
        StatusCode::NOT_IMPLEMENTED,
        ApiErrorEnvelope::new(
            "ROUTE_NOT_AVAILABLE",
            "This API surface is reserved for a later Erabi plan.",
            trace_id.as_str(),
        ),
    )
}

async fn spa_boundary() -> Html<&'static str> {
    Html("<!doctype html><title>Erabi</title><main id=\"erabi-root\"></main>")
}

async fn trace_request(mut request: Request<axum::body::Body>, next: middleware::Next) -> Response {
    let trace_id = TraceId::from_request(&request);
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    request.extensions_mut().insert(trace_id.clone());

    let span = tracing::info_span!(
        "erabi.http.request",
        trace_id = %trace_id.as_str(),
        method = %method,
        path = %path,
    );
    let mut response = next.run(request).instrument(span).await;
    response.headers_mut().insert(
        TRACE_HEADER,
        HeaderValue::from_str(trace_id.as_str())
            .unwrap_or_else(|_| HeaderValue::from_static("invalid")),
    );
    response
}

/// Reads the safe trace ID already attached by the outer middleware.
#[must_use]
pub(crate) fn trace_id_for(request: &Request<axum::body::Body>) -> String {
    request.extensions().get::<TraceId>().map_or_else(
        || "trace-unavailable".to_owned(),
        |trace_id| trace_id.0.clone(),
    )
}

fn is_safe_trace_id(value: &str) -> bool {
    (8..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

#[derive(Serialize)]
struct LivenessResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct ReadinessResponse {
    status: &'static str,
}
