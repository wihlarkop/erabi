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
use serde_json::Value;
use std::collections::BTreeMap;
use tracing::Instrument;
use uuid::Uuid;

use crate::{
    AppState, Crawl4AiAvailability, MutationAdmission, RuntimeMode, SecurityConfig,
    crawler_authoring::{
        create_crawler, create_draft, list_crawlers, list_versions, publish_version,
        reactivate_version, read_crawler, read_version,
    },
    error::{ApiErrorEnvelope, error_response},
    job_actions::{
        cancel as cancel_job, remove as remove_job, reprioritize as reprioritize_job,
        rerun_full_crawl, restart as restart_job, resume as resume_job, retry as retry_job,
        retry_failed_parts,
    },
    page_type_authoring::{
        create_matcher, create_page_type, delete_matcher, delete_page_type, list_matchers,
        list_page_types, match_page_type, read_matcher, read_page_type, update_matcher,
        update_page_type,
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
        .route("/api/v1/crawlers", get(list_crawlers).post(create_crawler))
        .route("/api/v1/crawlers/{crawler_id}", get(read_crawler))
        .route("/api/v1/crawlers/{crawler_id}/versions", get(list_versions))
        .route(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}",
            get(read_version),
        )
        .route("/api/v1/crawlers/{crawler_id}/drafts", post(create_draft))
        .route(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/publish",
            post(publish_version),
        )
        .route(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/reactivate",
            post(reactivate_version),
        )
        .route(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types",
            get(list_page_types).post(create_page_type),
        )
        .route(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}",
            get(read_page_type).put(update_page_type).delete(delete_page_type),
        )
        .route(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers",
            get(list_matchers).post(create_matcher),
        )
        .route(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers/{matcher_id}",
            get(read_matcher).put(update_matcher).delete(delete_matcher),
        )
        .route(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/match-page-type",
            post(match_page_type),
        )
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
        storage_pressure: app_state.storage_pressure(),
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
    storage_pressure: erabi_jobs::StoragePressureState,
}

/// `OpenAPI` document generated from the currently available stable route contracts.
#[derive(Serialize)]
struct OpenApiDocument {
    openapi: &'static str,
    info: OpenApiInfo,
    paths: BTreeMap<&'static str, OpenApiPath>,
    components: OpenApiComponents,
}

#[derive(Serialize)]
struct OpenApiComponents {
    schemas: BTreeMap<&'static str, Value>,
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
            "/api/v1/crawlers",
            OpenApiPath::get_post("List or create Crawlers"),
        );
        paths.insert(
            "/api/v1/crawlers/{crawler_id}",
            OpenApiPath::get("Read a Crawler"),
        );
        paths.insert(
            "/api/v1/crawlers/{crawler_id}/versions",
            OpenApiPath::get("List CrawlerVersions"),
        );
        paths.insert(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}",
            OpenApiPath::get("Read a CrawlerVersion"),
        );
        paths.insert(
            "/api/v1/crawlers/{crawler_id}/drafts",
            OpenApiPath::post("Create an active Draft"),
        );
        paths.insert(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/publish",
            OpenApiPath::post("Publish the active Draft"),
        );
        paths.insert(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/reactivate",
            OpenApiPath::post("Reactivate a historical Published version"),
        );
        paths.insert(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types",
            OpenApiPath::get_post("List or create PageTypes"),
        );
        paths.insert(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}",
            OpenApiPath::get_put_delete("Read, update, or delete a PageType"),
        );
        paths.insert(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers",
            OpenApiPath::get_post("List or create typed URLMatchers"),
        );
        paths.insert(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers/{matcher_id}",
            OpenApiPath::get_put_delete("Read, update, or delete a URLMatcher"),
        );
        paths.insert(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/match-page-type",
            OpenApiPath::post("Explain deterministic PageType matching"),
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
            components: OpenApiComponents {
                schemas: task2_openapi_schemas(),
            },
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
    put: Option<OpenApiOperation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delete: Option<OpenApiOperation>,
}

impl OpenApiPath {
    const fn get(summary: &'static str) -> Self {
        Self {
            get: Some(OpenApiOperation { summary }),
            post: None,
            put: None,
            delete: None,
        }
    }

    const fn post(summary: &'static str) -> Self {
        Self {
            get: None,
            post: Some(OpenApiOperation { summary }),
            put: None,
            delete: None,
        }
    }

    const fn get_post(summary: &'static str) -> Self {
        Self {
            get: Some(OpenApiOperation { summary }),
            post: Some(OpenApiOperation { summary }),
            put: None,
            delete: None,
        }
    }

    const fn get_put_delete(summary: &'static str) -> Self {
        Self {
            get: Some(OpenApiOperation { summary }),
            post: None,
            put: Some(OpenApiOperation { summary }),
            delete: Some(OpenApiOperation { summary }),
        }
    }

    const fn delete(summary: &'static str) -> Self {
        Self {
            get: None,
            post: None,
            put: None,
            delete: Some(OpenApiOperation { summary }),
        }
    }
}

#[derive(Serialize)]
struct OpenApiOperation {
    summary: &'static str,
}

#[allow(clippy::too_many_lines)]
fn task2_openapi_schemas() -> BTreeMap<&'static str, Value> {
    let mut schemas = BTreeMap::new();
    let matcher_variants = vec![
        serde_json::json!({
            "type": "object",
            "required": ["kind", "url"],
            "properties": {
                "kind": {"const": "EXACT_URL"},
                "url": {"type": "string", "format": "uri"}
            }
        }),
        serde_json::json!({
            "type": "object",
            "required": ["kind", "host", "path_template", "query"],
            "properties": {
                "kind": {"const": "EXACT_HOST_PATH_TEMPLATE"},
                "host": {"type": "string"},
                "path_template": {"type": "string"},
                "query": {
                    "type": "object",
                    "additionalProperties": {"type": "string"}
                }
            }
        }),
        serde_json::json!({
            "type": "object",
            "required": ["kind", "prefix"],
            "properties": {
                "kind": {"const": "PATH_PREFIX"},
                "host": {"type": ["string", "null"]},
                "prefix": {"type": "string"}
            }
        }),
        serde_json::json!({
            "type": "object",
            "required": ["kind", "pattern"],
            "properties": {
                "kind": {"const": "PATH_GLOB"},
                "host": {"type": ["string", "null"]},
                "pattern": {"type": "string"}
            }
        }),
        serde_json::json!({
            "type": "object",
            "required": ["kind", "pattern"],
            "properties": {
                "kind": {"const": "REGEX"},
                "pattern": {"type": "string"}
            }
        }),
    ];
    schemas.insert(
        "PageTypeRequest",
        serde_json::json!({
            "type": "object",
            "required": ["name", "priority"],
            "properties": {"name": {"type": "string", "maxLength": 256}, "priority": {"type": "integer"}}
        }),
    );
    schemas.insert(
        "UrlMatcherRequest",
        serde_json::json!({"oneOf": matcher_variants.clone()}),
    );
    let matcher_response_variants = matcher_variants
        .iter()
        .map(|variant| {
            serde_json::json!({
                "allOf": [
                    variant,
                    {"type": "object", "required": ["id", "ordinal"], "properties": {"id": {"type": "string", "format": "uuid"}, "ordinal": {"type": "integer", "minimum": 0}}}
                ]
            })
        })
        .collect::<Vec<_>>();
    schemas.insert(
        "UrlMatcherResponse",
        serde_json::json!({
            "description": "The typed matcher variants above plus application id and presentation ordinal.",
            "oneOf": matcher_response_variants
        }),
    );
    schemas.insert(
        "PageTypeResponse",
        serde_json::json!({
            "type": "object",
            "required": ["id", "crawler_version_id", "name", "priority", "matchers"],
            "properties": {"id": {"type": "string", "format": "uuid"}, "crawler_version_id": {"type": "string", "format": "uuid"}, "name": {"type": "string"}, "priority": {"type": "integer"}, "matchers": {"type": "array", "items": {"$ref": "#/components/schemas/UrlMatcherResponse"}}}
        }),
    );
    schemas.insert(
        "MatchCandidate",
        serde_json::json!({
            "type": "object",
            "required": ["page_type_id", "page_type_name", "explicit_priority", "best_matcher_kind", "matcher_kind_rank", "best_matched_patterns", "literal_path_segments", "explicit_query_constraints", "literal_characters", "wildcard_capture_count"],
            "properties": {"page_type_id": {"type": "string", "format": "uuid"}, "page_type_name": {"type": "string"}, "explicit_priority": {"type": "integer"}, "best_matcher_kind": {"type": "string"}, "matcher_kind_rank": {"type": "integer"}, "best_matched_patterns": {"type": "array", "items": {"type": "string"}}, "literal_path_segments": {"type": "integer"}, "explicit_query_constraints": {"type": "integer"}, "literal_characters": {"type": "integer"}, "wildcard_capture_count": {"type": "integer"}}
        }),
    );
    schemas.insert(
        "MatchDecision",
        serde_json::json!({
            "oneOf": [
                {"type": "object", "required": ["decision", "candidate", "candidates"], "properties": {"decision": {"const": "MATCHED"}, "candidate": {"$ref": "#/components/schemas/MatchCandidate"}, "candidates": {"type": "array", "maxItems": 0}}},
                {"type": "object", "required": ["decision", "candidate", "candidates"], "properties": {"decision": {"const": "AMBIGUOUS_PAGE_TYPE"}, "candidate": {"type": "null"}, "candidates": {"type": "array", "minItems": 2, "items": {"$ref": "#/components/schemas/MatchCandidate"}}}},
                {"type": "object", "required": ["decision", "candidate", "candidates"], "properties": {"decision": {"const": "UNMATCHED"}, "candidate": {"type": "null"}, "candidates": {"type": "array", "maxItems": 0}}}
            ]
        }),
    );
    schemas
}
