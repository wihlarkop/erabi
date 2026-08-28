use std::collections::BTreeMap;

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
use serde_json::Value;
use uuid::Uuid;

use crate::{
    AppState,
    app::TraceId,
    error::{ApiErrorEnvelope, error_response},
};

#[derive(Debug, Deserialize)]
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

#[derive(Clone, Debug, Serialize)]
pub(crate) struct TestEvidenceResponse {
    #[serde(flatten)]
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

#[allow(clippy::too_many_lines)]
pub(crate) fn test_lab_openapi_schemas() -> BTreeMap<&'static str, Value> {
    let mut schemas = BTreeMap::new();
    schemas.insert("TestKind", serde_json::json!({"type":"string","enum":["URL_CANONICALIZATION","PAGE_TYPE_MATCHING","EXTRACTION","SELECTOR_COVERAGE","PAGINATION","DISCOVERY_TRANSITION","DISCOVERED_URL_PREVIEW","COMBINED_URL_EVALUATION"]}));
    schemas.insert("TestLabRequest", serde_json::json!({"oneOf":[{"$ref":"#/components/schemas/UrlCanonicalizationTest"},{"$ref":"#/components/schemas/PageTypeMatchingTest"},{"$ref":"#/components/schemas/ExtractionTest"},{"$ref":"#/components/schemas/SelectorCoverageTest"},{"$ref":"#/components/schemas/PaginationTest"},{"$ref":"#/components/schemas/DiscoveryTransitionTest"},{"$ref":"#/components/schemas/DiscoveredUrlPreviewTest"},{"$ref":"#/components/schemas/CombinedUrlEvaluationTest"}]}));
    for (name, discriminator, page_type_required, page_type_optional, transition_required) in [
        (
            "UrlCanonicalizationTest",
            "URL_CANONICALIZATION",
            false,
            false,
            false,
        ),
        (
            "PageTypeMatchingTest",
            "PAGE_TYPE_MATCHING",
            false,
            true,
            false,
        ),
        ("ExtractionTest", "EXTRACTION", true, false, false),
        (
            "SelectorCoverageTest",
            "SELECTOR_COVERAGE",
            true,
            false,
            false,
        ),
        ("PaginationTest", "PAGINATION", false, false, false),
        (
            "DiscoveryTransitionTest",
            "DISCOVERY_TRANSITION",
            false,
            false,
            true,
        ),
        (
            "DiscoveredUrlPreviewTest",
            "DISCOVERED_URL_PREVIEW",
            false,
            false,
            false,
        ),
        (
            "CombinedUrlEvaluationTest",
            "COMBINED_URL_EVALUATION",
            false,
            true,
            false,
        ),
    ] {
        schemas.insert(
            name,
            test_lab_request_schema(
                discriminator,
                page_type_required,
                page_type_optional,
                transition_required,
            ),
        );
    }
    schemas.insert("CanonicalizationDecisionEvidence", serde_json::json!({"type":"object","required":["code"],"properties":{"code":{"type":"string"},"parameter":{"type":["string","null"]}}}));
    schemas.insert("CanonicalizationEvidence", serde_json::json!({"type":"object","required":["original_url","canonical_url","outcome","decisions"],"properties":{"original_url":{"type":"string","format":"uri"},"canonical_url":{"type":["string","null"],"format":"uri"},"outcome":{"type":"string"},"decisions":{"type":"array","items":{"$ref":"#/components/schemas/CanonicalizationDecisionEvidence"}}}}));
    schemas.insert("PageTypeCandidateEvidence", serde_json::json!({"type":"object","required":["page_type_id","page_type_name","priority","matcher_kind","specificity","matched_patterns"],"properties":{"page_type_id":{"type":"string","format":"uuid"},"page_type_name":{"type":"string"},"priority":{"type":"integer"},"matcher_kind":{"type":"string"},"specificity":{"$ref":"#/components/schemas/MatcherSpecificityEvidence"},"matched_patterns":{"type":"array","items":{"type":"string"}}}}));
    schemas.insert("MatcherSpecificityEvidence", serde_json::json!({"type":"object","properties":{"matcher_kind_rank":{"type":"integer"},"literal_path_segments":{"type":"integer"},"explicit_query_constraints":{"type":"integer"},"literal_characters":{"type":"integer"},"wildcard_capture_count":{"type":"integer"}}}));
    schemas.insert("PageTypeMatchEvidence", serde_json::json!({"type":"object","required":["decision","winner","candidates"],"properties":{"decision":{"type":"string"},"winner":{"anyOf":[{"$ref":"#/components/schemas/PageTypeCandidateEvidence"},{"type":"null"}]},"candidates":{"type":"array","items":{"$ref":"#/components/schemas/PageTypeCandidateEvidence"}}}}));
    schemas.insert("DomainScopeEvidence", serde_json::json!({"type":"object","required":["classification","host","rationale"],"properties":{"classification":{"type":"string","enum":["IN_SCOPE","EXTERNAL","BLOCKED"]},"host":{"type":"string"},"rationale":{"type":"string"}}}));
    schemas.insert("TransitionBudgetEvidence", serde_json::json!({"type":"object","required":["allowed","exclusion"],"properties":{"allowed":{"type":"boolean"},"exclusion":{"type":["string","null"]}}}));
    schemas.insert("DiscoveredUrlEvidence", serde_json::json!({"type":"object","required":["raw_href","resolved_original_url","canonical_url","canonicalization","scope","duplicate","duplicate_of_canonical_url","page_type_match","transition_eligible","budget"],"properties":{"raw_href":{"type":"string"},"resolved_original_url":{"type":["string","null"],"format":"uri"},"canonical_url":{"type":["string","null"],"format":"uri"},"canonicalization":{"anyOf":[{"$ref":"#/components/schemas/CanonicalizationEvidence"},{"type":"null"}]},"scope":{"anyOf":[{"$ref":"#/components/schemas/DomainScopeEvidence"},{"type":"null"}]},"duplicate":{"type":"boolean"},"duplicate_of_canonical_url":{"type":["string","null"],"format":"uri"},"page_type_match":{"anyOf":[{"$ref":"#/components/schemas/PageTypeMatchEvidence"},{"type":"null"}]},"transition_eligible":{"type":"boolean"},"budget":{"anyOf":[{"$ref":"#/components/schemas/TransitionBudgetEvidence"},{"type":"null"}]}}}));
    schemas.insert("ExtractionObservation", serde_json::json!({"oneOf":[{"type":"object","required":["status","fields"],"properties":{"status":{"const":"AVAILABLE"},"fields":{"type":"array"}}},{"type":"object","required":["status","reason"],"properties":{"status":{"const":"UNAVAILABLE"},"reason":{"type":"string"}}},{"type":"object","required":["status","diagnostic"],"properties":{"status":{"const":"ERROR"},"diagnostic":{"$ref":"#/components/schemas/TestDiagnostic"}}}],"description":"Available, unavailable, or error extraction observation; no Plan 07 production records."}));
    schemas.insert("SelectorCoverageEvidence", serde_json::json!({"type":"object","required":["selector","matches_found","status"],"properties":{"selector":{"type":"string"},"matches_found":{"type":"integer","minimum":0},"status":{"type":"string"}}}));
    schemas.insert("PaginationEvidence", serde_json::json!({"type":"object","required":["kind"],"properties":{"kind":{"type":"string"},"selector":{"type":["string","null"]},"target_url":{"type":["string","null"]}}}));
    schemas.insert("DiscoveryTransitionEvidence", serde_json::json!({"type":"object","required":["transition_id","transition_name","source_page_type_id","target_page_type_id","source_match","selector","discovered_urls","eligible_link_count","per_page_limit","per_page_limit_reached"],"properties":{"transition_id":{"type":["string","null"],"format":"uuid"},"transition_name":{"type":["string","null"]},"source_page_type_id":{"type":["string","null"],"format":"uuid"},"target_page_type_id":{"type":["string","null"],"format":"uuid"},"source_match":{"anyOf":[{"$ref":"#/components/schemas/PageTypeMatchEvidence"},{"type":"null"}]},"selector":{"$ref":"#/components/schemas/SelectorCoverageEvidence"},"discovered_urls":{"type":"array","maxItems":64},"eligible_link_count":{"type":"integer","minimum":0},"per_page_limit":{"type":"integer","minimum":0},"per_page_limit_reached":{"type":"boolean"}}}));
    schemas.insert("TestDiagnostic", serde_json::json!({"type":"object","required":["code","message"],"properties":{"code":{"type":"string"},"message":{"type":"string"}}}));
    schemas.insert("TestLabComparison", serde_json::json!({"type":"object","required":["status","draft_version_id","draft_config_hash","published_version_id","published_config_hash","canonicalization_difference","draft_canonicalization","published_canonicalization","page_type_match_difference","draft_page_type_match","published_page_type_match","discovery_difference","extraction_difference","warnings"],"properties":{"status":{"type":"string","enum":["COMPARED","NO_ACTIVE_PUBLISHED_VERSION"]},"draft_version_id":{"type":"string","format":"uuid"},"draft_config_hash":{"type":"string","pattern":"^[0-9a-fA-F]{64}$"},"published_version_id":{"type":["string","null"],"format":"uuid"},"published_config_hash":{"type":["string","null"],"pattern":"^[0-9a-fA-F]{64}$"},"canonicalization_difference":{"type":"boolean"},"draft_canonicalization":{"type":"array","items":{"$ref":"#/components/schemas/CanonicalizationEvidence"}},"published_canonicalization":{"type":"array","items":{"$ref":"#/components/schemas/CanonicalizationEvidence"}},"page_type_match_difference":{"type":"boolean"},"draft_page_type_match":{"type":"array","items":{"$ref":"#/components/schemas/PageTypeMatchEvidence"}},"published_page_type_match":{"type":"array","items":{"$ref":"#/components/schemas/PageTypeMatchEvidence"}},"discovery_difference":{"type":["boolean","null"]},"extraction_difference":{"type":["boolean","null"]},"warnings":{"type":"array","items":{"$ref":"#/components/schemas/TestDiagnostic"}}}}));
    schemas.insert("TestEvidence", serde_json::json!({"type":"object","required":["schema_version","id","crawler_version_id","test_kind","input_urls","evaluated_page_type_id","tested_transition_id","canonicalization","page_type_match","extraction","selector_coverage","pagination","discovery","warnings","errors","artifact_ids","config_hash","executed_at","published_comparison"],"properties":{"schema_version":{"type":"integer","const":1},"id":{"type":"string","format":"uuid"},"crawler_version_id":{"type":"string","format":"uuid"},"test_kind":{"$ref":"#/components/schemas/TestKind"},"input_urls":{"type":"array","maxItems":8,"items":{"type":"string"}},"evaluated_page_type_id":{"type":["string","null"],"format":"uuid"},"tested_transition_id":{"type":["string","null"],"format":"uuid"},"canonicalization":{"type":"array","items":{"$ref":"#/components/schemas/CanonicalizationEvidence"}},"page_type_match":{"type":"array","items":{"$ref":"#/components/schemas/PageTypeMatchEvidence"}},"extraction":{"anyOf":[{"$ref":"#/components/schemas/ExtractionObservation"},{"type":"null"}]},"selector_coverage":{"type":"array","items":{"$ref":"#/components/schemas/SelectorCoverageEvidence"}},"pagination":{"anyOf":[{"$ref":"#/components/schemas/PaginationEvidence"},{"type":"null"}]},"discovery":{"anyOf":[{"$ref":"#/components/schemas/DiscoveryTransitionEvidence"},{"type":"null"}]},"warnings":{"type":"array","items":{"$ref":"#/components/schemas/TestDiagnostic"}},"errors":{"type":"array","items":{"$ref":"#/components/schemas/TestDiagnostic"}},"artifact_ids":{"type":"array","maxItems":16,"items":{"type":"string","format":"uuid"}},"config_hash":{"type":"string","pattern":"^[0-9a-fA-F]{64}$"},"executed_at":{"type":"string"},"published_comparison":{"anyOf":[{"$ref":"#/components/schemas/TestLabComparison"},{"type":"null"}]}}}));
    schemas.insert("TestEvidenceResponse", serde_json::json!({"allOf":[{"$ref":"#/components/schemas/TestEvidence"},{"type":"object","required":["matches_current_configuration"],"properties":{"matches_current_configuration":{"type":"boolean"}}}]}));
    schemas
}

fn test_lab_request_schema(
    discriminator: &str,
    page_type_required: bool,
    page_type_optional: bool,
    transition_required: bool,
) -> Value {
    let mut required = vec![
        serde_json::json!("test_type"),
        serde_json::json!("input_urls"),
    ];
    let mut properties = serde_json::Map::from_iter([
        (
            "test_type".to_owned(),
            serde_json::json!({"const": discriminator}),
        ),
        (
            "input_urls".to_owned(),
            serde_json::json!({"type":"array","minItems":1,"maxItems":8,"items":{"type":"string"}}),
        ),
        (
            "compare_with_active_published".to_owned(),
            serde_json::json!({"type":"boolean","default":false}),
        ),
        (
            "reuse_artifact_ids".to_owned(),
            serde_json::json!({"type":"array","maxItems":16,"items":{"type":"string","format":"uuid"}}),
        ),
    ]);
    if page_type_required || page_type_optional {
        properties.insert(
            "page_type_id".to_owned(),
            serde_json::json!({"type":"string","format":"uuid"}),
        );
    }
    if transition_required {
        required.push(serde_json::json!("transition_id"));
        properties.insert(
            "transition_id".to_owned(),
            serde_json::json!({"type":"string","format":"uuid"}),
        );
    }
    if page_type_required {
        required.push(serde_json::json!("page_type_id"));
    }
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Typed Test Lab request; evidence identity, timestamp, hash, and result payload are server-owned.",
        "required": required,
        "properties": properties,
    })
}
