//! Hardened route composition for the stable `/api/v1` boundary.

use axum::{
    Json, Router,
    extract::{Extension, MatchedPath, Path, State},
    http::{HeaderName, HeaderValue, Request, StatusCode, header},
    middleware,
    response::{Html, IntoResponse, Response},
    routing::{any, get},
};
use erabi_observability::{HttpMethod, HttpRequestSpan, RequestTraceId, RouteTemplate};
use serde::Serialize;
use std::time::Instant;
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::{
    AppState, Crawl4AiAvailability, MutationAdmission, RuntimeMode, SecurityConfig,
    error::{ApiErrorEnvelope, error_response},
    security::{apply_security_headers, enforce_browser_request_policy, require_bearer},
};

const TRACE_HEADER: HeaderName = HeaderName::from_static("x-erabi-trace-id");

/// Safe request trace identity generated or propagated by the outer shell layer.
#[derive(Clone)]
pub(crate) struct TraceId(RequestTraceId);

impl TraceId {
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self(RequestTraceId::from_incoming(Some("test-trace-id")))
    }

    fn from_request(request: &Request<axum::body::Body>) -> Self {
        Self(RequestTraceId::from_incoming(
            request
                .headers()
                .get(&TRACE_HEADER)
                .and_then(|value| value.to_str().ok()),
        ))
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Builds the versioned API, protected future-surface groups, and SPA boundary.
///
/// Browser shell routes deliberately stay separate from protected API/data
/// groups so a remote browser can load the token-free SPA before JavaScript
/// reads its session-stored bearer token. Later API modules attach only below
/// the protected boundary.
#[allow(clippy::needless_pass_by_value)] // Public contract intentionally owns the shared router state.
#[allow(clippy::too_many_lines)]
pub fn build_router(app_state: AppState, security: SecurityConfig) -> Router {
    let liveness: Router = liveness_router().with_state(app_state.clone()).into();
    let documentation = if security.openapi_enabled() {
        let machine_readable_documentation: Router = openapi_document_router()
            .with_state(app_state.clone())
            .into();
        machine_readable_documentation.route("/api/docs", get(crate::openapi::scalar_docs))
    } else {
        Router::new()
            .route("/api/v1/openapi.json", get(openapi_disabled))
            .route("/api/docs", get(openapi_disabled))
    };

    let protected_api: Router = crate::openapi::runtime_router()
        .with_state(app_state.clone())
        .into();

    let protected = protected_api
        .merge(documentation)
        .route("/api/v1/diagnostics/{*path}", any(unavailable))
        .route("/api/v1/events/{*path}", any(unavailable))
        .route("/api/v1/assets/{*path}", any(unavailable))
        .route("/api/v1/exports/{*path}", any(unavailable))
        .route("/api/v1/backups/{*path}", any(unavailable))
        .route("/api/v1/artifacts/{*path}", any(unavailable))
        .route("/api/v1/{*path}", any(unavailable))
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

#[utoipa::path(
    get,
    path = "/api/v1/health",
    responses((status = 200, description = "Liveness response", body = LivenessResponse))
)]
pub(crate) async fn liveness() -> Json<LivenessResponse> {
    Json(LivenessResponse { status: "live" })
}

#[utoipa::path(
    get,
    path = "/api/v1/readiness",
    responses(
        (status = 200, description = "Readiness response", body = ReadinessResponse),
        (status = 503, description = "Service is not ready", body = ApiErrorEnvelope)
    )
)]
pub(crate) async fn readiness(
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
            crawl4ai: app_state.crawl4ai_availability().into(),
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

#[utoipa::path(
    get,
    path = "/api/v1/diagnostics/status",
    responses((status = 200, description = "Safe runtime diagnostics", body = RuntimeDiagnosticsResponse))
)]
pub(crate) async fn runtime_diagnostics(
    State(app_state): State<AppState>,
) -> Json<RuntimeDiagnosticsResponse> {
    Json(RuntimeDiagnosticsResponse {
        mode: app_state.runtime_mode().into(),
        crawl4ai: app_state.crawl4ai_availability().into(),
        storage_pressure: app_state.storage_pressure().into(),
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/openapi.json",
    responses((status = 200, description = "Generated OpenAPI document", content_type = "application/json"))
)]
pub(crate) async fn openapi_document() -> impl IntoResponse {
    Json(crate::openapi::generated_document())
}

pub(crate) fn liveness_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::<AppState>::new().routes(routes!(liveness))
}

pub(crate) fn openapi_document_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::<AppState>::new().routes(routes!(openapi_document))
}

pub(crate) fn openapi_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::<AppState>::new()
        .routes(routes!(readiness))
        .routes(routes!(runtime_diagnostics))
}

pub(crate) async fn openapi_disabled(Extension(trace_id): Extension<TraceId>) -> Response {
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
async fn static_asset_boundary(Path(path): Path<String>) -> Response {
    if path == "scalar.js"
        && let Some((mime_type, content)) = crate::openapi::scalar_asset()
    {
        let Ok(content_type) = HeaderValue::from_str(&mime_type) else {
            return (
                [(
                    header::CONTENT_TYPE,
                    "application/javascript; charset=utf-8",
                )],
                "",
            )
                .into_response();
        };
        let mut response = Response::new(axum::body::Body::from(content));
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
        return response;
    }

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
    let method = observed_http_method(request.method());
    let route_template = route_template_for(
        request
            .extensions()
            .get::<MatchedPath>()
            .map(MatchedPath::as_str),
    );
    let span = HttpRequestSpan::new(&trace_id.0, method, &route_template);
    request.extensions_mut().insert(trace_id.clone());

    let started_at = Instant::now();
    let mut response = span.run(next.run(request)).await;
    span.record_outcome(response.status().as_u16(), started_at.elapsed());
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
        |trace_id| trace_id.as_str().to_owned(),
    )
}

fn observed_http_method(method: &axum::http::Method) -> HttpMethod {
    if method == axum::http::Method::GET {
        HttpMethod::Get
    } else if method == axum::http::Method::POST {
        HttpMethod::Post
    } else if method == axum::http::Method::PUT {
        HttpMethod::Put
    } else if method == axum::http::Method::PATCH {
        HttpMethod::Patch
    } else if method == axum::http::Method::DELETE {
        HttpMethod::Delete
    } else if method == axum::http::Method::HEAD {
        HttpMethod::Head
    } else if method == axum::http::Method::OPTIONS {
        HttpMethod::Options
    } else if method == axum::http::Method::CONNECT {
        HttpMethod::Connect
    } else if method == axum::http::Method::TRACE {
        HttpMethod::Trace
    } else {
        HttpMethod::Other
    }
}

#[allow(clippy::too_many_lines)]
fn route_template_for(matched_path: Option<&str>) -> RouteTemplate {
    match matched_path {
        Some("/api/v1/health") => RouteTemplate::ApiV1Health,
        Some("/api/v1/openapi.json") => RouteTemplate::ApiV1OpenapiJson,
        Some("/api/docs") => RouteTemplate::ApiDocs,
        Some("/api/v1/readiness") => RouteTemplate::ApiV1Readiness,
        Some("/api/v1/diagnostics/status") => RouteTemplate::ApiV1DiagnosticsStatus,
        Some("/api/v1/quick-scrapes") => RouteTemplate::ApiV1QuickScrapes,
        Some("/api/v1/quick-scrapes/batch") => RouteTemplate::ApiV1QuickScrapesBatch,
        Some("/api/v1/crawlers") => RouteTemplate::ApiV1Crawlers,
        Some("/api/v1/crawlers/{crawler_id}") => RouteTemplate::ApiV1CrawlersCrawlerId,
        Some("/api/v1/crawlers/{crawler_id}/versions") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersions
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionId
        }
        Some("/api/v1/crawlers/{crawler_id}/drafts") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdDrafts
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/publish") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdPublish
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/publish-validation") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdPublishValidation
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/reactivate") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdReactivate
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdPageTypes
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdPageTypesPageTypeId
        }
        Some(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers",
        ) => RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdPageTypesPageTypeIdMatchers,
        Some(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers/{matcher_id}",
        ) => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdPageTypesPageTypeIdMatchersMatcherId
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/match-page-type") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdMatchPageType
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/canonicalization") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdCanonicalization
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/canonicalize-url") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdCanonicalizeUrl
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/domain-scope") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdDomainScope
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/classify-domain-scope") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdClassifyDomainScope
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/guardrails") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdGuardrails
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdTransitions
        }
        Some(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions/{transition_id}",
        ) => RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdTransitionsTransitionId,
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/test-lab/tests") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdTestLabTests
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/discovery-preview") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdDiscoveryPreview
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/production-runs") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdProductionRuns
        }
        Some("/api/v1/crawlers/{crawler_id}/versions/{version_id}/test-evidence") => {
            RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdTestEvidence
        }
        Some(
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/test-evidence/{evidence_id}",
        ) => RouteTemplate::ApiV1CrawlersCrawlerIdVersionsVersionIdTestEvidenceEvidenceId,
        Some("/api/v1/diagnostics/{*path}") => RouteTemplate::ApiV1DiagnosticsWildcard,
        Some("/api/v1/events/jobs/{job_id}/progress") => {
            RouteTemplate::ApiV1EventsJobsJobIdProgress
        }
        Some("/api/v1/jobs/{job_id}/retry-failed-parts") => {
            RouteTemplate::ApiV1JobsJobIdRetryFailedParts
        }
        Some("/api/v1/jobs/{job_id}/rerun-full-crawl") => {
            RouteTemplate::ApiV1JobsJobIdRerunFullCrawl
        }
        Some("/api/v1/jobs/{job_id}/resume") => RouteTemplate::ApiV1JobsJobIdResume,
        Some("/api/v1/jobs/{job_id}/restart") => RouteTemplate::ApiV1JobsJobIdRestart,
        Some("/api/v1/jobs/{job_id}/retry") => RouteTemplate::ApiV1JobsJobIdRetry,
        Some("/api/v1/jobs/{job_id}/cancel") => RouteTemplate::ApiV1JobsJobIdCancel,
        Some("/api/v1/jobs/{job_id}/priority") => RouteTemplate::ApiV1JobsJobIdPriority,
        Some("/api/v1/jobs/{job_id}") => RouteTemplate::ApiV1JobsJobId,
        Some("/api/v1/events/{*path}") => RouteTemplate::ApiV1EventsWildcard,
        Some("/api/v1/assets/{*path}") => RouteTemplate::ApiV1AssetsWildcard,
        Some("/api/v1/exports/{*path}") => RouteTemplate::ApiV1ExportsWildcard,
        Some("/api/v1/backups/{*path}") => RouteTemplate::ApiV1BackupsWildcard,
        Some("/api/v1/artifacts/{*path}") => RouteTemplate::ApiV1ArtifactsWildcard,
        Some("/api/v1/{*path}") => RouteTemplate::ApiV1Wildcard,
        Some("/assets/{*path}") => RouteTemplate::AssetsWildcard,
        Some("/") => RouteTemplate::Root,
        Some("/{*path}") => RouteTemplate::RootWildcard,
        None | Some(_) => RouteTemplate::UNKNOWN,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    #[test]
    fn unrecognized_matched_route_becomes_unknown_route() {
        let route = route_template_for(Some("/api/v1/artifacts/DO_NOT_LOG_SECRET_PATH_74291"));
        assert!(route == RouteTemplate::UNKNOWN);
    }

    #[test]
    fn recognized_matched_route_uses_static_contract() {
        let route = route_template_for(Some("/api/v1/artifacts/{*path}"));
        assert!(route == RouteTemplate::ApiV1ArtifactsWildcard);
    }

    #[test]
    fn scalar_docs_route_uses_the_closed_observability_template() {
        let route = route_template_for(Some("/api/docs"));
        assert!(route == RouteTemplate::ApiDocs);
    }

    #[tokio::test]
    async fn system_route_fragments_couple_runtime_and_openapi_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let (health_runtime, health_openapi) = liveness_router().split_for_parts();
        let health_response = health_runtime
            .with_state(AppState::ready())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(health_response.status(), StatusCode::OK);
        let health_document = serde_json::to_value(health_openapi)?;
        assert!(health_document["paths"]["/api/v1/health"]["get"].is_object());

        let (document_runtime, document_openapi) = openapi_document_router().split_for_parts();
        let document_response = document_runtime
            .with_state(AppState::ready())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/openapi.json")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(document_response.status(), StatusCode::OK);
        let document_metadata = serde_json::to_value(document_openapi)?;
        assert!(document_metadata["paths"]["/api/v1/openapi.json"]["get"].is_object());

        Ok(())
    }
}

#[derive(Serialize, ToSchema)]
pub(crate) struct LivenessResponse {
    status: &'static str,
}

#[derive(Serialize, ToSchema)]
struct ReadinessResponse {
    status: &'static str,
    crawl4ai: Crawl4AiAvailabilityResponse,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct RuntimeDiagnosticsResponse {
    mode: RuntimeModeResponse,
    crawl4ai: Crawl4AiAvailabilityResponse,
    storage_pressure: StoragePressureResponse,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum Crawl4AiAvailabilityResponse {
    Available,
    Degraded { message: String },
}

impl From<Crawl4AiAvailability> for Crawl4AiAvailabilityResponse {
    fn from(value: Crawl4AiAvailability) -> Self {
        match value {
            Crawl4AiAvailability::Available => Self::Available,
            Crawl4AiAvailability::Degraded { message } => Self::Degraded { message },
        }
    }
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "mode")]
enum RuntimeModeResponse {
    Normal,
    Recovery { code: String, message: String },
    ShuttingDown,
}

impl From<RuntimeMode> for RuntimeModeResponse {
    fn from(value: RuntimeMode) -> Self {
        match value {
            RuntimeMode::Normal => Self::Normal,
            RuntimeMode::Recovery { code, message } => Self::Recovery { code, message },
            RuntimeMode::ShuttingDown => Self::ShuttingDown,
        }
    }
}

#[derive(Serialize, ToSchema)]
struct StoragePressureResponse {
    level: StoragePressureLevelResponse,
    free_bytes: Option<u64>,
    warning_threshold: u64,
    critical_threshold: u64,
}

impl From<erabi_jobs::StoragePressureState> for StoragePressureResponse {
    fn from(value: erabi_jobs::StoragePressureState) -> Self {
        Self {
            level: value.level.into(),
            free_bytes: value.free_bytes,
            warning_threshold: value.warning_threshold,
            critical_threshold: value.critical_threshold,
        }
    }
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum StoragePressureLevelResponse {
    Healthy,
    Warning,
    Critical,
    Unavailable,
}

impl From<erabi_jobs::StoragePressureLevel> for StoragePressureLevelResponse {
    fn from(value: erabi_jobs::StoragePressureLevel) -> Self {
        match value {
            erabi_jobs::StoragePressureLevel::Healthy => Self::Healthy,
            erabi_jobs::StoragePressureLevel::Warning => Self::Warning,
            erabi_jobs::StoragePressureLevel::Critical => Self::Critical,
            erabi_jobs::StoragePressureLevel::Unavailable => Self::Unavailable,
        }
    }
}
