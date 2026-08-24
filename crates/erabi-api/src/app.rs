//! Hardened route composition for the stable `/api/v1` boundary.

use axum::{
    Json, Router,
    extract::{Extension, State},
    http::{HeaderName, HeaderValue, Request, StatusCode, header},
    middleware,
    response::{Html, IntoResponse, Response},
    routing::{any, delete, get, post},
};
use serde::Serialize;
use std::collections::BTreeMap;
use tracing::Instrument;
use uuid::Uuid;

use crate::{
    AppState, Crawl4AiAvailability, MutationAdmission, RuntimeMode, SecurityConfig,
    error::{ApiErrorEnvelope, error_response},
    job_actions::{
        cancel as cancel_job, remove as remove_job, reprioritize as reprioritize_job,
        rerun_full_crawl, restart as restart_job, resume as resume_job, retry as retry_job,
        retry_failed_parts,
    },
    progress::job_progress_sse,
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

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// Builds the versioned API, protected future-surface groups, and SPA boundary.
///
/// Browser shell routes deliberately stay separate from protected API/data
/// groups so a remote browser can load the token-free SPA before JavaScript
/// reads its session-stored bearer token. Later API modules attach only below
/// the protected boundary.
#[allow(clippy::needless_pass_by_value)] // Public contract intentionally owns the shared router state.
pub fn build_router(app_state: AppState, security: SecurityConfig) -> Router {
    let liveness = Router::new().route("/api/v1/health", get(liveness));
    let documentation = if security.openapi_enabled() {
        Router::new().route("/api/v1/openapi.json", get(openapi_document))
    } else {
        Router::new().route("/api/v1/openapi.json", get(openapi_disabled))
    };

    let protected = Router::new()
        .merge(documentation)
        .route("/api/v1/readiness", get(readiness))
        .route("/api/v1/diagnostics/status", get(runtime_diagnostics))
        .route("/api/v1/diagnostics/{*path}", any(unavailable))
        .route(
            "/api/v1/events/jobs/{job_id}/progress",
            get(job_progress_sse),
        )
        .route(
            "/api/v1/jobs/{job_id}/retry-failed-parts",
            post(retry_failed_parts),
        )
        .route(
            "/api/v1/jobs/{job_id}/rerun-full-crawl",
            post(rerun_full_crawl),
        )
        .route("/api/v1/jobs/{job_id}/resume", post(resume_job))
        .route("/api/v1/jobs/{job_id}/restart", post(restart_job))
        .route("/api/v1/jobs/{job_id}/retry", post(retry_job))
        .route("/api/v1/jobs/{job_id}/cancel", post(cancel_job))
        .route("/api/v1/jobs/{job_id}/priority", post(reprioritize_job))
        .route("/api/v1/jobs/{job_id}", delete(remove_job))
        .route("/api/v1/events/{*path}", any(unavailable))
        .route("/api/v1/assets/{*path}", any(unavailable))
        .route("/api/v1/exports/{*path}", any(unavailable))
        .route("/api/v1/backups/{*path}", any(unavailable))
        .route("/api/v1/artifacts/{*path}", any(unavailable))
        .route("/api/v1/{*path}", any(unavailable))
        .with_state(app_state.clone())
        .layer(middleware::from_fn_with_state(
            security.clone(),
            enforce_browser_request_policy,
        ))
        .layer(middleware::from_fn_with_state(
            app_state.clone(),
            mutation_admission_guard,
        ))
        .layer(middleware::from_fn_with_state(security, require_bearer));

    let browser_bootstrap = Router::new()
        .route("/assets/{*path}", get(static_asset_boundary))
        .route("/", get(spa_boundary))
        .route("/{*path}", get(spa_boundary));

    liveness
        .merge(protected)
        .merge(browser_bootstrap)
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
        let status = match app_state.crawl4ai_availability() {
            Crawl4AiAvailability::Available => "ready",
            Crawl4AiAvailability::Degraded { .. } => "degraded",
        };
        return Json(ReadinessResponse {
            status,
            crawl4ai: app_state.crawl4ai_availability(),
        })
        .into_response();
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

async fn runtime_diagnostics(
    State(app_state): State<AppState>,
) -> Json<RuntimeDiagnosticsResponse> {
    Json(RuntimeDiagnosticsResponse {
        mode: app_state.runtime_mode(),
        crawl4ai: app_state.crawl4ai_availability(),
    })
}

async fn openapi_document() -> Json<OpenApiDocument> {
    Json(OpenApiDocument::generated())
}

async fn openapi_disabled(Extension(trace_id): Extension<TraceId>) -> Response {
    error_response(
        StatusCode::NOT_FOUND,
        ApiErrorEnvelope::new(
            "OPENAPI_DISABLED",
            "OpenAPI documentation is disabled for this bind mode.",
            trace_id.as_str(),
        ),
    )
}

async fn mutation_admission_guard(
    State(app_state): State<AppState>,
    request: Request<axum::body::Body>,
    next: middleware::Next,
) -> Response {
    if matches!(
        *request.method(),
        axum::http::Method::POST
            | axum::http::Method::PUT
            | axum::http::Method::PATCH
            | axum::http::Method::DELETE
    ) {
        let (code, message) = match app_state.mutation_admission() {
            MutationAdmission::Allowed => return next.run(request).await,
            MutationAdmission::Recovery => (
                "RECOVERY_MODE_MUTATION_BLOCKED",
                "Normal mutations are disabled while the service is in Recovery Mode.",
            ),
            MutationAdmission::ShuttingDown => (
                "SERVICE_SHUTTING_DOWN",
                "Normal mutations are unavailable while Erabi is shutting down.",
            ),
        };
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            ApiErrorEnvelope::new(code, message, trace_id_for(&request)),
        );
    }
    next.run(request).await
}

async fn unavailable(
    method: axum::http::Method,
    Extension(trace_id): Extension<TraceId>,
) -> Response {
    let (status, code, message) = if method == axum::http::Method::GET {
        (
            StatusCode::NOT_IMPLEMENTED,
            "ROUTE_NOT_AVAILABLE",
            "This API surface is reserved for a later Erabi plan.",
        )
    } else {
        (
            StatusCode::METHOD_NOT_ALLOWED,
            "METHOD_NOT_ALLOWED",
            "This API surface does not support that HTTP method.",
        )
    };
    error_response(
        status,
        ApiErrorEnvelope::new(code, message, trace_id.as_str()),
    )
}

async fn spa_boundary() -> Html<&'static str> {
    Html("<!doctype html><title>Erabi</title><main id=\"erabi-root\"></main>")
}

/// A token-free compiled-asset boundary. Plan 03 does not manufacture a UI
/// bundle; later UI integration mounts its generated assets here. Returning an
/// empty JavaScript module keeps the browser bootstrap contract usable without
/// treating API/download assets as public.
async fn static_asset_boundary() -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        "",
    )
        .into_response()
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
    crawl4ai: Crawl4AiAvailability,
}

#[derive(Serialize)]
struct RuntimeDiagnosticsResponse {
    mode: RuntimeMode,
    crawl4ai: Crawl4AiAvailability,
}

/// `OpenAPI` document generated from the currently available stable route contracts.
#[derive(Serialize)]
struct OpenApiDocument {
    openapi: &'static str,
    info: OpenApiInfo,
    paths: BTreeMap<&'static str, OpenApiPath>,
}

impl OpenApiDocument {
    fn generated() -> Self {
        let mut paths = BTreeMap::new();
        paths.insert("/api/v1/health", OpenApiPath::get("Liveness"));
        paths.insert("/api/v1/readiness", OpenApiPath::get("Readiness"));
        paths.insert(
            "/api/v1/diagnostics/status",
            OpenApiPath::get("Safe runtime diagnostics"),
        );
        paths.insert(
            "/api/v1/events/jobs/{job_id}/progress",
            OpenApiPath::get("Replayable job progress stream"),
        );
        for (path, summary) in [
            (
                "/api/v1/jobs/{job_id}/retry-failed-parts",
                "Retry failed parts",
            ),
            ("/api/v1/jobs/{job_id}/rerun-full-crawl", "Rerun full crawl"),
            (
                "/api/v1/jobs/{job_id}/resume",
                "Resume compatible checkpoint",
            ),
            ("/api/v1/jobs/{job_id}/restart", "Restart from beginning"),
            ("/api/v1/jobs/{job_id}/retry", "Retry bounded job attempt"),
            ("/api/v1/jobs/{job_id}/cancel", "Cancel job cooperatively"),
            ("/api/v1/jobs/{job_id}/priority", "Move queued job"),
        ] {
            paths.insert(path, OpenApiPath::post(summary));
        }
        paths.insert(
            "/api/v1/jobs/{job_id}",
            OpenApiPath::delete("Remove safe never-started job"),
        );
        paths.insert("/api/v1/openapi.json", OpenApiPath::get("OpenAPI document"));
        Self {
            openapi: "3.1.0",
            info: OpenApiInfo {
                title: "Erabi API",
                version: env!("CARGO_PKG_VERSION"),
            },
            paths,
        }
    }
}

#[derive(Serialize)]
struct OpenApiInfo {
    title: &'static str,
    version: &'static str,
}

#[derive(Serialize)]
struct OpenApiPath {
    #[serde(skip_serializing_if = "Option::is_none")]
    get: Option<OpenApiOperation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    post: Option<OpenApiOperation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delete: Option<OpenApiOperation>,
}

impl OpenApiPath {
    const fn get(summary: &'static str) -> Self {
        Self {
            get: Some(OpenApiOperation { summary }),
            post: None,
            delete: None,
        }
    }

    const fn post(summary: &'static str) -> Self {
        Self {
            get: None,
            post: Some(OpenApiOperation { summary }),
            delete: None,
        }
    }

    const fn delete(summary: &'static str) -> Self {
        Self {
            get: None,
            post: None,
            delete: Some(OpenApiOperation { summary }),
        }
    }
}

#[derive(Serialize)]
struct OpenApiOperation {
    summary: &'static str,
}
