use axum::{
    Json,
    extract::{Extension, Path, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use erabi_crawler::{TestLabError, TestLabRequest};
use erabi_domain::{
    CrawlerId, CrawlerVersionId, DiscoveryTransitionId, PageTypeId, TestEvidence, TestEvidenceId,
    TestKind,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};
use uuid::Uuid;

use crate::{
    AppState,
    app::TraceId,
    error::{ApiErrorEnvelope, error_response},
    openapi::wire::TestEvidenceSchema,
};

#[derive(Debug, Deserialize, ToSchema)]
#[schema(as = TestLabRequest)]
#[serde(
    tag = "test_type",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
pub(crate) enum TestLabRequestDto {
    UrlCanonicalization {
        input_urls: Vec<String>,
        #[serde(default)]
        compare_with_active_published: bool,
        #[serde(default)]
        reuse_artifact_ids: Vec<String>,
    },
    PageTypeMatching {
        input_urls: Vec<String>,
        #[serde(default)]
        page_type_id: Option<String>,
        #[serde(default)]
        compare_with_active_published: bool,
        #[serde(default)]
        reuse_artifact_ids: Vec<String>,
    },
    Extraction {
        input_urls: Vec<String>,
        page_type_id: String,
        #[serde(default)]
        compare_with_active_published: bool,
        #[serde(default)]
        reuse_artifact_ids: Vec<String>,
    },
    SelectorCoverage {
        input_urls: Vec<String>,
        page_type_id: String,
        #[serde(default)]
        compare_with_active_published: bool,
        #[serde(default)]
        reuse_artifact_ids: Vec<String>,
    },
    Pagination {
        input_urls: Vec<String>,
        #[serde(default)]
        compare_with_active_published: bool,
        #[serde(default)]
        reuse_artifact_ids: Vec<String>,
    },
    DiscoveryTransition {
        input_urls: Vec<String>,
        transition_id: String,
        #[serde(default)]
        compare_with_active_published: bool,
        #[serde(default)]
        reuse_artifact_ids: Vec<String>,
    },
    DiscoveredUrlPreview {
        input_urls: Vec<String>,
        #[serde(default)]
        compare_with_active_published: bool,
        #[serde(default)]
        reuse_artifact_ids: Vec<String>,
    },
    CombinedUrlEvaluation {
        input_urls: Vec<String>,
        #[serde(default)]
        page_type_id: Option<String>,
        #[serde(default)]
        compare_with_active_published: bool,
        #[serde(default)]
        reuse_artifact_ids: Vec<String>,
    },
}

impl TestLabRequestDto {
    #[allow(clippy::too_many_lines)]
    fn into_request(self) -> Result<TestLabRequest, &'static str> {
        let (test_kind, input_urls, page_type_id, transition_id, compare, artifacts) = match self {
            Self::UrlCanonicalization {
                input_urls,
                compare_with_active_published,
                reuse_artifact_ids,
            } => (
                TestKind::UrlCanonicalization,
                input_urls,
                None,
                None,
                compare_with_active_published,
                reuse_artifact_ids,
            ),
            Self::PageTypeMatching {
                input_urls,
                page_type_id,
                compare_with_active_published,
                reuse_artifact_ids,
            } => (
                TestKind::PageTypeMatching,
                input_urls,
                page_type_id,
                None,
                compare_with_active_published,
                reuse_artifact_ids,
            ),
            Self::Extraction {
                input_urls,
                page_type_id,
                compare_with_active_published,
                reuse_artifact_ids,
            } => (
                TestKind::Extraction,
                input_urls,
                Some(page_type_id),
                None,
                compare_with_active_published,
                reuse_artifact_ids,
            ),
            Self::SelectorCoverage {
                input_urls,
                page_type_id,
                compare_with_active_published,
                reuse_artifact_ids,
            } => (
                TestKind::SelectorCoverage,
                input_urls,
                Some(page_type_id),
                None,
                compare_with_active_published,
                reuse_artifact_ids,
            ),
            Self::Pagination {
                input_urls,
                compare_with_active_published,
                reuse_artifact_ids,
            } => (
                TestKind::Pagination,
                input_urls,
                None,
                None,
                compare_with_active_published,
                reuse_artifact_ids,
            ),
            Self::DiscoveryTransition {
                input_urls,
                transition_id,
                compare_with_active_published,
                reuse_artifact_ids,
            } => (
                TestKind::DiscoveryTransition,
                input_urls,
                None,
                Some(transition_id),
                compare_with_active_published,
                reuse_artifact_ids,
            ),
            Self::DiscoveredUrlPreview {
                input_urls,
                compare_with_active_published,
                reuse_artifact_ids,
            } => (
                TestKind::DiscoveredUrlPreview,
                input_urls,
                None,
                None,
                compare_with_active_published,
                reuse_artifact_ids,
            ),
            Self::CombinedUrlEvaluation {
                input_urls,
                page_type_id,
                compare_with_active_published,
                reuse_artifact_ids,
            } => (
                TestKind::CombinedUrlEvaluation,
                input_urls,
                page_type_id,
                None,
                compare_with_active_published,
                reuse_artifact_ids,
            ),
        };
        Ok(TestLabRequest {
            test_kind,
            input_urls,
            page_type_id: page_type_id
                .as_deref()
                .map(parse_page_type_id)
                .transpose()?,
            transition_id: transition_id
                .as_deref()
                .map(parse_transition_id)
                .transpose()?,
            compare_with_active_published: compare,
            reuse_artifact_ids: artifacts
                .iter()
                .map(|value| parse_artifact_id(value))
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub(crate) struct TestEvidenceResponse {
    #[serde(flatten)]
    #[schema(value_type = TestEvidenceSchema)]
    evidence: TestEvidence,
    matches_current_configuration: bool,
}

impl From<erabi_db::repositories::TestEvidenceRecord> for TestEvidenceResponse {
    fn from(record: erabi_db::repositories::TestEvidenceRecord) -> Self {
        Self {
            evidence: record.evidence,
            matches_current_configuration: record.matches_current_configuration,
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/crawlers/{crawler_id}/versions/{version_id}/test-lab/tests",
    request_body = TestLabRequestDto,
    responses(
        (status = 201, description = "Test Lab evidence created", body = TestEvidenceResponse),
        (status = 400, description = "Invalid Test Lab request", body = ApiErrorEnvelope),
        (status = 404, description = "CrawlerVersion not found", body = ApiErrorEnvelope),
        (status = 409, description = "Test Lab conflict", body = ApiErrorEnvelope),
        (status = 503, description = "Test Lab unavailable", body = ApiErrorEnvelope)
    )
)]
pub(crate) async fn run_test_lab(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
    input: Result<Json<TestLabRequestDto>, JsonRejection>,
) -> Response {
    let (crawler_id, version_id) = match parse_version_path(&raw_crawler_id, &raw_version_id) {
        Ok(ids) => ids,
        Err(message) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_TEST_LAB_REQUEST",
                message,
                &trace,
            );
        }
    };
    let Ok(Json(input)) = input else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_TEST_LAB_REQUEST",
            "The Test Lab request body is invalid.",
            &trace,
        );
    };
    let Ok(request) = input.into_request() else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_TEST_LAB_REQUEST",
            "The Test Lab request contains an invalid identifier.",
            &trace,
        );
    };
    let Some(service) = state.test_lab_runtime() else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "TEST_LAB_PROVIDER_UNAVAILABLE",
            "Test Lab is not configured in this runtime.",
            &trace,
        );
    };
    match service.execute(crawler_id, version_id, request).await {
        Ok(record) => (
            StatusCode::CREATED,
            Json(TestEvidenceResponse::from(record)),
        )
            .into_response(),
        Err(error) => test_lab_error(error, &trace),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/crawlers/{crawler_id}/versions/{version_id}/test-evidence",
    responses(
        (status = 200, description = "Test evidence", body = [TestEvidenceResponse]),
        (status = 400, description = "Invalid Test Lab request", body = ApiErrorEnvelope),
        (status = 404, description = "CrawlerVersion not found", body = ApiErrorEnvelope),
        (status = 503, description = "Test Lab unavailable", body = ApiErrorEnvelope)
    )
)]
pub(crate) async fn list_test_evidence(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
) -> Response {
    let (crawler_id, version_id) = match parse_version_path(&raw_crawler_id, &raw_version_id) {
        Ok(ids) => ids,
        Err(message) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_TEST_LAB_REQUEST",
                message,
                &trace,
            );
        }
    };
    let Some(service) = state.test_lab_runtime() else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "TEST_LAB_PROVIDER_UNAVAILABLE",
            "Test Lab is not configured in this runtime.",
            &trace,
        );
    };
    match service.list_evidence(crawler_id, version_id).await {
        Ok(records) => Json(
            records
                .into_iter()
                .map(TestEvidenceResponse::from)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(error) => test_lab_error(error, &trace),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/crawlers/{crawler_id}/versions/{version_id}/test-evidence/{evidence_id}",
    responses(
        (status = 200, description = "Test evidence", body = TestEvidenceResponse),
        (status = 400, description = "Invalid Test Lab request", body = ApiErrorEnvelope),
        (status = 404, description = "TestEvidence not found", body = ApiErrorEnvelope),
        (status = 503, description = "Test Lab unavailable", body = ApiErrorEnvelope)
    )
)]
pub(crate) async fn read_test_evidence(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id, raw_evidence_id)): Path<(String, String, String)>,
) -> Response {
    let (crawler_id, version_id) = match parse_version_path(&raw_crawler_id, &raw_version_id) {
        Ok(ids) => ids,
        Err(message) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_TEST_LAB_REQUEST",
                message,
                &trace,
            );
        }
    };
    let Ok(evidence_id) = parse_evidence_id(&raw_evidence_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_TEST_LAB_REQUEST",
            "The TestEvidence identifier is invalid.",
            &trace,
        );
    };
    let Some(service) = state.test_lab_runtime() else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "TEST_LAB_PROVIDER_UNAVAILABLE",
            "Test Lab is not configured in this runtime.",
            &trace,
        );
    };
    match service
        .read_evidence(crawler_id, version_id, evidence_id)
        .await
    {
        Ok(record) => Json(TestEvidenceResponse::from(record)).into_response(),
        Err(error) => test_lab_error(error, &trace),
    }
}

pub(crate) fn openapi_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::<AppState>::new()
        .routes(routes!(run_test_lab))
        .routes(routes!(list_test_evidence))
        .routes(routes!(read_test_evidence))
}

fn parse_version_path(
    crawler: &str,
    version: &str,
) -> Result<(CrawlerId, CrawlerVersionId), &'static str> {
    let crawler = Uuid::parse_str(crawler)
        .ok()
        .and_then(CrawlerId::from_uuid)
        .ok_or("The Crawler or CrawlerVersion identifier is invalid.")?;
    let version = Uuid::parse_str(version)
        .ok()
        .and_then(CrawlerVersionId::from_uuid)
        .ok_or("The Crawler or CrawlerVersion identifier is invalid.")?;
    Ok((crawler, version))
}

fn parse_page_type_id(value: &str) -> Result<PageTypeId, &'static str> {
    Uuid::parse_str(value)
        .ok()
        .and_then(PageTypeId::from_uuid)
        .ok_or("The PageType identifier is invalid.")
}

fn parse_transition_id(value: &str) -> Result<DiscoveryTransitionId, &'static str> {
    Uuid::parse_str(value)
        .ok()
        .and_then(DiscoveryTransitionId::from_uuid)
        .ok_or("The DiscoveryTransition identifier is invalid.")
}

fn parse_artifact_id(value: &str) -> Result<erabi_domain::ArtifactId, &'static str> {
    Uuid::parse_str(value)
        .ok()
        .and_then(erabi_domain::ArtifactId::from_uuid)
        .ok_or("The Artifact identifier is invalid.")
}

fn parse_evidence_id(value: &str) -> Result<TestEvidenceId, ()> {
    Uuid::parse_str(value)
        .ok()
        .and_then(TestEvidenceId::from_uuid)
        .ok_or(())
}

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::too_many_lines)]
fn test_lab_error(error: TestLabError, trace: &TraceId) -> Response {
    let (status, code, message) = match error {
        TestLabError::CrawlerNotFound => (
            StatusCode::NOT_FOUND,
            "CRAWLER_NOT_FOUND",
            "The Crawler was not found.",
        ),
        TestLabError::CrawlerVersionNotFound => (
            StatusCode::NOT_FOUND,
            "CRAWLER_VERSION_NOT_FOUND",
            "The CrawlerVersion was not found.",
        ),
        TestLabError::VersionNotOwnedByCrawler => (
            StatusCode::CONFLICT,
            "VERSION_NOT_OWNED_BY_CRAWLER",
            "The CrawlerVersion does not belong to this Crawler.",
        ),
        TestLabError::VersionNotDraft => (
            StatusCode::CONFLICT,
            "VERSION_NOT_DRAFT",
            "Test Lab execution requires a Draft CrawlerVersion.",
        ),
        TestLabError::VersionNotActiveDraft => (
            StatusCode::CONFLICT,
            "VERSION_NOT_ACTIVE_DRAFT",
            "Test Lab execution requires the active Draft.",
        ),
        TestLabError::PageTypeNotFound => (
            StatusCode::NOT_FOUND,
            "PAGE_TYPE_NOT_FOUND",
            "The PageType was not found.",
        ),
        TestLabError::PageTypeNotOwnedByVersion => (
            StatusCode::CONFLICT,
            "PAGE_TYPE_NOT_OWNED_BY_VERSION",
            "The PageType does not belong to this CrawlerVersion.",
        ),
        TestLabError::DiscoveryTransitionNotFound => (
            StatusCode::NOT_FOUND,
            "DISCOVERY_TRANSITION_NOT_FOUND",
            "The DiscoveryTransition was not found.",
        ),
        TestLabError::TransitionNotOwnedByVersion => (
            StatusCode::CONFLICT,
            "TRANSITION_NOT_OWNED_BY_VERSION",
            "The DiscoveryTransition does not belong to this CrawlerVersion.",
        ),
        TestLabError::InvalidRequest => (
            StatusCode::BAD_REQUEST,
            "INVALID_TEST_LAB_REQUEST",
            "The Test Lab request is invalid.",
        ),
        TestLabError::TooManyUrls => (
            StatusCode::BAD_REQUEST,
            "TOO_MANY_TEST_URLS",
            "The Test Lab URL batch exceeds the bounded limit.",
        ),
        TestLabError::ProviderUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "TEST_LAB_PROVIDER_UNAVAILABLE",
            "The requested Test Lab observation is unavailable.",
        ),
        TestLabError::ProviderObservationRequestMismatch => (
            StatusCode::BAD_GATEWAY,
            "TEST_LAB_PROVIDER_OBSERVATION_MISMATCH",
            "The Test Lab provider returned an observation for a different requested URL.",
        ),
        TestLabError::ProviderObservationInvalid => (
            StatusCode::BAD_GATEWAY,
            "TEST_LAB_PROVIDER_OBSERVATION_INVALID",
            "The Test Lab provider returned an invalid observed final URL.",
        ),
        TestLabError::ArtifactNotFound => (
            StatusCode::NOT_FOUND,
            "ARTIFACT_NOT_FOUND",
            "A referenced artifact was not found.",
        ),
        TestLabError::ArtifactNotReusable => (
            StatusCode::CONFLICT,
            "ARTIFACT_NOT_REUSABLE",
            "The referenced artifact cannot safely supply this observation.",
        ),
        TestLabError::TestEvidenceNotFound => (
            StatusCode::NOT_FOUND,
            "TEST_EVIDENCE_NOT_FOUND",
            "The TestEvidence record was not found.",
        ),
        TestLabError::TestEvidenceNotOwnedByVersion => (
            StatusCode::CONFLICT,
            "TEST_EVIDENCE_NOT_OWNED_BY_VERSION",
            "The TestEvidence record does not belong to this CrawlerVersion.",
        ),
        TestLabError::ConfigurationChanged => (
            StatusCode::CONFLICT,
            "DRAFT_CONFIGURATION_CHANGED",
            "The Draft changed while Test Lab was executing; no evidence was recorded.",
        ),
        TestLabError::PersistedStateInvalid => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "PERSISTED_STATE_INVALID",
            "Stored crawler state is invalid; no evidence was recorded.",
        ),
        TestLabError::PersistenceFailed => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "TEST_EVIDENCE_PERSISTENCE_FAILED",
            "TestEvidence could not be durably recorded.",
        ),
    };
    api_error(status, code, message, trace)
}

fn api_error(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    trace: &TraceId,
) -> Response {
    error_response(status, ApiErrorEnvelope::new(code, message, trace.as_str()))
}
