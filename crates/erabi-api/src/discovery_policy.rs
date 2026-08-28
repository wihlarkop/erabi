use axum::{
    Json,
    extract::{Extension, Path, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use erabi_db::repositories::{
    CrawlerRepository, CrawlerRepositoryError, DiscoveryTransitionRecord,
};
use erabi_domain::{
    CanonicalizationDecision, CanonicalizationPolicy, CanonicalizationResult, CrawlerId,
    CrawlerVersionGuardrails, CrawlerVersionId, DiscoveryTransition, DiscoveryTransitionId,
    DomainScopeClassification, DomainScopePolicy, PageTypeId, ProductError, TransitionBudget,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AppState,
    app::TraceId,
    error::{ApiErrorEnvelope, error_response},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CanonicalizeUrlRequest {
    url: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClassifyDomainScopeRequest {
    url: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DiscoveryTransitionRequest {
    source_page_type_id: String,
    target_page_type_id: String,
    name: String,
    enabled: bool,
    link_selector: String,
    url_constraints: Option<String>,
    priority: i32,
    max_links_per_source_page: u32,
    total_transition_budget: Option<u64>,
    depth_contribution: u32,
    deduplicate: bool,
}

impl DiscoveryTransitionRequest {
    fn into_domain(self, id: DiscoveryTransitionId) -> Result<DiscoveryTransition, &'static str> {
        let source_page_type_id = parse_page_type_id(&self.source_page_type_id)
            .map_err(|()| "The source PageType identifier is invalid.")?;
        let target_page_type_id = parse_page_type_id(&self.target_page_type_id)
            .map_err(|()| "The target PageType identifier is invalid.")?;
        Ok(DiscoveryTransition {
            id,
            source_page_type_id,
            target_page_type_id,
            name: self.name,
            enabled: self.enabled,
            link_selector: self.link_selector,
            url_constraints: self.url_constraints,
            priority: self.priority,
            budget: TransitionBudget {
                max_links_per_source_page: self.max_links_per_source_page,
                total_budget: self.total_transition_budget,
                depth_contribution: self.depth_contribution,
            },
            deduplicate: self.deduplicate,
            latest_test_evidence_id: None,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CanonicalizationExplanation {
    original_url: String,
    canonical_url: String,
    decisions: Vec<CanonicalizationDecision>,
}

impl From<CanonicalizationResult> for CanonicalizationExplanation {
    fn from(result: CanonicalizationResult) -> Self {
        Self {
            original_url: result.original_url,
            canonical_url: result.canonical_url.to_string(),
            decisions: result.decisions,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CanonicalizedDomainScopeResult {
    canonicalization: CanonicalizationExplanation,
    classification: DomainScopeClassification,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DiscoveryTransitionDto {
    id: String,
    source_page_type_id: String,
    target_page_type_id: String,
    name: String,
    enabled: bool,
    link_selector: String,
    url_constraints: Option<String>,
    priority: i32,
    max_links_per_source_page: u32,
    total_transition_budget: Option<u64>,
    depth_contribution: u32,
    deduplicate: bool,
    latest_test_evidence_id: Option<String>,
}

impl From<&DiscoveryTransitionRecord> for DiscoveryTransitionDto {
    fn from(record: &DiscoveryTransitionRecord) -> Self {
        let transition = &record.transition;
        Self {
            id: transition.id.to_string(),
            source_page_type_id: transition.source_page_type_id.to_string(),
            target_page_type_id: transition.target_page_type_id.to_string(),
            name: transition.name.clone(),
            enabled: transition.enabled,
            link_selector: transition.link_selector.clone(),
            url_constraints: transition.url_constraints.clone(),
            priority: transition.priority,
            max_links_per_source_page: transition.budget.max_links_per_source_page,
            total_transition_budget: transition.budget.total_budget,
            depth_contribution: transition.budget.depth_contribution,
            deduplicate: transition.deduplicate,
            latest_test_evidence_id: transition.latest_test_evidence_id.map(|id| id.to_string()),
        }
    }
}

pub(crate) async fn read_canonicalization(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
) -> Response {
    let (crawler_id, version_id) =
        match parse_version_path(&raw_crawler_id, &raw_version_id, &trace) {
            Ok(ids) => ids,
            Err(response) => return response,
        };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return crawler_authoring_unavailable(&trace);
    };
    match CrawlerRepository::new(runtime.database())
        .canonicalization_policy(crawler_id, version_id)
        .await
    {
        Ok(policy) => Json(policy).into_response(),
        Err(error) => crawler_discovery_repository_error(error, &trace),
    }
}

pub(crate) async fn update_canonicalization(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
    input: Result<Json<CanonicalizationPolicy>, JsonRejection>,
) -> Response {
    let (crawler_id, version_id) =
        match parse_version_path(&raw_crawler_id, &raw_version_id, &trace) {
            Ok(ids) => ids,
            Err(response) => return response,
        };
    let Ok(Json(policy)) = input else {
        return invalid_policy_request(&trace);
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return crawler_authoring_unavailable(&trace);
    };
    match CrawlerRepository::new(runtime.database())
        .update_canonicalization_policy(
            crawler_id,
            version_id,
            &policy,
            "api",
            &current_timestamp(),
        )
        .await
    {
        Ok(policy) => Json(policy).into_response(),
        Err(error) => crawler_discovery_repository_error(error, &trace),
    }
}

pub(crate) async fn canonicalize_url(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
    input: Result<Json<CanonicalizeUrlRequest>, JsonRejection>,
) -> Response {
    let (crawler_id, version_id) =
        match parse_version_path(&raw_crawler_id, &raw_version_id, &trace) {
            Ok(ids) => ids,
            Err(response) => return response,
        };
    let Ok(Json(input)) = input else {
        return invalid_policy_request(&trace);
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return crawler_authoring_unavailable(&trace);
    };
    let repository = CrawlerRepository::new(runtime.database());
    let policy = match repository
        .canonicalization_policy(crawler_id, version_id)
        .await
    {
        Ok(policy) => policy,
        Err(error) => return crawler_discovery_repository_error(error, &trace),
    };
    match policy.canonicalize(&input.url) {
        Ok(result) => Json(CanonicalizationExplanation::from(result)).into_response(),
        Err(error) => policy_validation_error(error, &trace),
    }
}

pub(crate) async fn read_domain_scope(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
) -> Response {
    let (crawler_id, version_id) =
        match parse_version_path(&raw_crawler_id, &raw_version_id, &trace) {
            Ok(ids) => ids,
            Err(response) => return response,
        };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return crawler_authoring_unavailable(&trace);
    };
    match CrawlerRepository::new(runtime.database())
        .domain_scope_policy(crawler_id, version_id)
        .await
    {
        Ok(policy) => Json(policy).into_response(),
        Err(error) => crawler_discovery_repository_error(error, &trace),
    }
}

pub(crate) async fn update_domain_scope(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
    input: Result<Json<DomainScopePolicy>, JsonRejection>,
) -> Response {
    let (crawler_id, version_id) =
        match parse_version_path(&raw_crawler_id, &raw_version_id, &trace) {
            Ok(ids) => ids,
            Err(response) => return response,
        };
    let Ok(Json(policy)) = input else {
        return invalid_policy_request(&trace);
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return crawler_authoring_unavailable(&trace);
    };
    match CrawlerRepository::new(runtime.database())
        .update_domain_scope_policy(crawler_id, version_id, &policy, "api", &current_timestamp())
        .await
    {
        Ok(policy) => Json(policy).into_response(),
        Err(error) => crawler_discovery_repository_error(error, &trace),
    }
}

pub(crate) async fn classify_domain_scope(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
    input: Result<Json<ClassifyDomainScopeRequest>, JsonRejection>,
) -> Response {
    let (crawler_id, version_id) =
        match parse_version_path(&raw_crawler_id, &raw_version_id, &trace) {
            Ok(ids) => ids,
            Err(response) => return response,
        };
    let Ok(Json(input)) = input else {
        return invalid_policy_request(&trace);
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return crawler_authoring_unavailable(&trace);
    };
    let repository = CrawlerRepository::new(runtime.database());
    let version = match repository.version(crawler_id, version_id).await {
        Ok(record) => record.version,
        Err(error) => return crawler_discovery_repository_error(error, &trace),
    };
    let result = match version.canonicalization_policy().canonicalize(&input.url) {
        Ok(result) => result,
        Err(error) => return policy_validation_error(error, &trace),
    };
    let classification = match version
        .domain_scope()
        .classify(&result.canonical_url, version.seeds())
    {
        Ok(classification) => classification,
        Err(error) => return policy_validation_error(error, &trace),
    };
    Json(CanonicalizedDomainScopeResult {
        canonicalization: result.into(),
        classification,
    })
    .into_response()
}

pub(crate) async fn read_crawler_version_guardrails(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
) -> Response {
    let (crawler_id, version_id) =
        match parse_version_path(&raw_crawler_id, &raw_version_id, &trace) {
            Ok(ids) => ids,
            Err(response) => return response,
        };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return crawler_authoring_unavailable(&trace);
    };
    match CrawlerRepository::new(runtime.database())
        .crawler_version_guardrails(crawler_id, version_id)
        .await
    {
        Ok(guardrails) => Json(guardrails).into_response(),
        Err(error) => crawler_discovery_repository_error(error, &trace),
    }
}

pub(crate) async fn update_crawler_version_guardrails(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
    input: Result<Json<CrawlerVersionGuardrails>, JsonRejection>,
) -> Response {
    let (crawler_id, version_id) =
        match parse_version_path(&raw_crawler_id, &raw_version_id, &trace) {
            Ok(ids) => ids,
            Err(response) => return response,
        };
    let Ok(Json(guardrails)) = input else {
        return invalid_policy_request(&trace);
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return crawler_authoring_unavailable(&trace);
    };
    match CrawlerRepository::new(runtime.database())
        .update_crawler_version_guardrails(
            crawler_id,
            version_id,
            &guardrails,
            "api",
            &current_timestamp(),
        )
        .await
    {
        Ok(guardrails) => Json(guardrails).into_response(),
        Err(error) => crawler_discovery_repository_error(error, &trace),
    }
}

pub(crate) async fn list_discovery_transitions(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
) -> Response {
    let (crawler_id, version_id) =
        match parse_version_path(&raw_crawler_id, &raw_version_id, &trace) {
            Ok(ids) => ids,
            Err(response) => return response,
        };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return crawler_authoring_unavailable(&trace);
    };
    match CrawlerRepository::new(runtime.database())
        .list_discovery_transitions(crawler_id, version_id)
        .await
    {
        Ok(transitions) => Json(
            transitions
                .iter()
                .map(DiscoveryTransitionDto::from)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(error) => crawler_discovery_repository_error(error, &trace),
    }
}

pub(crate) async fn create_discovery_transition(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
    input: Result<Json<DiscoveryTransitionRequest>, JsonRejection>,
) -> Response {
    let (crawler_id, version_id) =
        match parse_version_path(&raw_crawler_id, &raw_version_id, &trace) {
            Ok(ids) => ids,
            Err(response) => return response,
        };
    let Ok(Json(input)) = input else {
        return invalid_policy_request(&trace);
    };
    let transition = match input.into_domain(DiscoveryTransitionId::new()) {
        Ok(transition) => transition,
        Err(message) => {
            return policy_api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_DISCOVERY_TRANSITION",
                message,
                &trace,
            );
        }
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return crawler_authoring_unavailable(&trace);
    };
    match CrawlerRepository::new(runtime.database())
        .create_discovery_transition(
            crawler_id,
            version_id,
            &transition,
            "api",
            &current_timestamp(),
        )
        .await
    {
        Ok(record) => (
            StatusCode::CREATED,
            Json(DiscoveryTransitionDto::from(&record)),
        )
            .into_response(),
        Err(error) => crawler_discovery_repository_error(error, &trace),
    }
}

pub(crate) async fn read_discovery_transition(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id, raw_transition_id)): Path<(String, String, String)>,
) -> Response {
    let (crawler_id, version_id) =
        match parse_version_path(&raw_crawler_id, &raw_version_id, &trace) {
            Ok(ids) => ids,
            Err(response) => return response,
        };
    let Ok(transition_id) = parse_transition_id(&raw_transition_id) else {
        return policy_api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_TRANSITION_ID",
            "The DiscoveryTransition identifier is invalid.",
            &trace,
        );
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return crawler_authoring_unavailable(&trace);
    };
    match CrawlerRepository::new(runtime.database())
        .discovery_transition(crawler_id, version_id, transition_id)
        .await
    {
        Ok(record) => Json(DiscoveryTransitionDto::from(&record)).into_response(),
        Err(error) => crawler_discovery_repository_error(error, &trace),
    }
}

pub(crate) async fn update_discovery_transition(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id, raw_transition_id)): Path<(String, String, String)>,
    input: Result<Json<DiscoveryTransitionRequest>, JsonRejection>,
) -> Response {
    let (crawler_id, version_id) =
        match parse_version_path(&raw_crawler_id, &raw_version_id, &trace) {
            Ok(ids) => ids,
            Err(response) => return response,
        };
    let Ok(transition_id) = parse_transition_id(&raw_transition_id) else {
        return policy_api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_TRANSITION_ID",
            "The DiscoveryTransition identifier is invalid.",
            &trace,
        );
    };
    let Ok(Json(input)) = input else {
        return invalid_policy_request(&trace);
    };
    let transition = match input.into_domain(transition_id) {
        Ok(transition) => transition,
        Err(message) => {
            return policy_api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_DISCOVERY_TRANSITION",
                message,
                &trace,
            );
        }
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return crawler_authoring_unavailable(&trace);
    };
    match CrawlerRepository::new(runtime.database())
        .update_discovery_transition(
            crawler_id,
            version_id,
            transition_id,
            &transition,
            "api",
            &current_timestamp(),
        )
        .await
    {
        Ok(record) => Json(DiscoveryTransitionDto::from(&record)).into_response(),
        Err(error) => crawler_discovery_repository_error(error, &trace),
    }
}

pub(crate) async fn delete_discovery_transition(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id, raw_transition_id)): Path<(String, String, String)>,
) -> Response {
    let (crawler_id, version_id) =
        match parse_version_path(&raw_crawler_id, &raw_version_id, &trace) {
            Ok(ids) => ids,
            Err(response) => return response,
        };
    let Ok(transition_id) = parse_transition_id(&raw_transition_id) else {
        return policy_api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_TRANSITION_ID",
            "The DiscoveryTransition identifier is invalid.",
            &trace,
        );
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return crawler_authoring_unavailable(&trace);
    };
    match CrawlerRepository::new(runtime.database())
        .delete_discovery_transition(
            crawler_id,
            version_id,
            transition_id,
            "api",
            &current_timestamp(),
        )
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => crawler_discovery_repository_error(error, &trace),
    }
}

#[allow(clippy::result_large_err)]
fn parse_version_path(
    raw_crawler_id: &str,
    raw_version_id: &str,
    trace: &TraceId,
) -> Result<(CrawlerId, CrawlerVersionId), Response> {
    let Ok(crawler_id) = parse_crawler_id(raw_crawler_id) else {
        return Err(policy_api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_CRAWLER_ID",
            "The Crawler identifier is invalid.",
            trace,
        ));
    };
    let Ok(version_id) = parse_version_id(raw_version_id) else {
        return Err(policy_api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_VERSION_ID",
            "The CrawlerVersion identifier is invalid.",
            trace,
        ));
    };
    Ok((crawler_id, version_id))
}

fn parse_crawler_id(value: &str) -> Result<CrawlerId, ()> {
    Uuid::parse_str(value)
        .ok()
        .and_then(CrawlerId::from_uuid)
        .ok_or(())
}

fn parse_version_id(value: &str) -> Result<CrawlerVersionId, ()> {
    Uuid::parse_str(value)
        .ok()
        .and_then(CrawlerVersionId::from_uuid)
        .ok_or(())
}

fn parse_page_type_id(value: &str) -> Result<PageTypeId, ()> {
    Uuid::parse_str(value)
        .ok()
        .and_then(PageTypeId::from_uuid)
        .ok_or(())
}

fn parse_transition_id(value: &str) -> Result<DiscoveryTransitionId, ()> {
    Uuid::parse_str(value)
        .ok()
        .and_then(DiscoveryTransitionId::from_uuid)
        .ok_or(())
}

fn current_timestamp() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
        .to_string()
}

#[allow(clippy::needless_pass_by_value)]
fn policy_validation_error(error: ProductError, trace: &TraceId) -> Response {
    let (status, code, message) = match error.code {
        erabi_domain::ErrorCode::InvalidCanonicalizationPolicy => (
            StatusCode::BAD_REQUEST,
            "INVALID_CANONICALIZATION_POLICY",
            "The canonicalization policy is invalid.",
        ),
        erabi_domain::ErrorCode::InvalidUrl => (
            StatusCode::BAD_REQUEST,
            "INVALID_URL",
            "The URL is invalid.",
        ),
        erabi_domain::ErrorCode::UnsupportedUrlScheme => (
            StatusCode::BAD_REQUEST,
            "UNSUPPORTED_URL_SCHEME",
            "Only HTTP and HTTPS URLs are supported.",
        ),
        erabi_domain::ErrorCode::InvalidDomainScope => (
            StatusCode::BAD_REQUEST,
            "INVALID_DOMAIN_SCOPE",
            "The Domain Scope policy is invalid.",
        ),
        erabi_domain::ErrorCode::InvalidDomainScopeRule => (
            StatusCode::BAD_REQUEST,
            "INVALID_DOMAIN_SCOPE_RULE",
            "The Domain Scope rule is invalid.",
        ),
        erabi_domain::ErrorCode::RegistrableDomainUnavailable => (
            StatusCode::BAD_REQUEST,
            "REGISTRABLE_DOMAIN_UNAVAILABLE",
            "Registrable-domain classification is unavailable for this host.",
        ),
        erabi_domain::ErrorCode::InvalidCrawlGuardrails => (
            StatusCode::BAD_REQUEST,
            "INVALID_CRAWL_GUARDRAILS",
            "The crawler guardrails are invalid.",
        ),
        erabi_domain::ErrorCode::InvalidPageTypeBudget => (
            StatusCode::BAD_REQUEST,
            "INVALID_PAGE_TYPE_BUDGET",
            "The PageType budget is invalid.",
        ),
        erabi_domain::ErrorCode::InvalidTransitionBudget => (
            StatusCode::BAD_REQUEST,
            "INVALID_TRANSITION_BUDGET",
            "The transition budget is invalid.",
        ),
        erabi_domain::ErrorCode::InvalidDiscoveryTransition => (
            StatusCode::BAD_REQUEST,
            "INVALID_DISCOVERY_TRANSITION",
            "The DiscoveryTransition is invalid.",
        ),
        _ => (
            StatusCode::BAD_REQUEST,
            "INVALID_POLICY_REQUEST",
            "The policy request is invalid.",
        ),
    };
    policy_api_error(status, code, message, trace)
}

#[allow(
    clippy::match_same_arms,
    clippy::needless_pass_by_value,
    clippy::too_many_lines
)]
fn crawler_discovery_repository_error(error: CrawlerRepositoryError, trace: &TraceId) -> Response {
    let (status, code, message) = match error {
        CrawlerRepositoryError::CrawlerNotFound => (
            StatusCode::NOT_FOUND,
            "CRAWLER_NOT_FOUND",
            "The requested Crawler does not exist.",
        ),
        CrawlerRepositoryError::CrawlerVersionNotFound => (
            StatusCode::NOT_FOUND,
            "CRAWLER_VERSION_NOT_FOUND",
            "The requested CrawlerVersion does not exist.",
        ),
        CrawlerRepositoryError::VersionNotOwnedByCrawler => (
            StatusCode::CONFLICT,
            "VERSION_NOT_OWNED_BY_CRAWLER",
            "The CrawlerVersion does not belong to this Crawler.",
        ),
        CrawlerRepositoryError::VersionNotDraft => (
            StatusCode::CONFLICT,
            "VERSION_NOT_DRAFT",
            "Only a Draft can be changed.",
        ),
        CrawlerRepositoryError::VersionNotActiveDraft => (
            StatusCode::CONFLICT,
            "VERSION_NOT_ACTIVE_DRAFT",
            "Only the active Draft can be changed.",
        ),
        CrawlerRepositoryError::PublishedVersionImmutable => (
            StatusCode::CONFLICT,
            "PUBLISHED_VERSION_IMMUTABLE",
            "Published CrawlerVersions are immutable.",
        ),
        CrawlerRepositoryError::PageTypeNotFound => (
            StatusCode::NOT_FOUND,
            "PAGE_TYPE_NOT_FOUND",
            "The requested PageType does not exist.",
        ),
        CrawlerRepositoryError::DiscoveryTransitionNotFound => (
            StatusCode::NOT_FOUND,
            "DISCOVERY_TRANSITION_NOT_FOUND",
            "The requested DiscoveryTransition does not exist.",
        ),
        CrawlerRepositoryError::TransitionNotOwnedByVersion => (
            StatusCode::CONFLICT,
            "TRANSITION_NOT_OWNED_BY_VERSION",
            "The DiscoveryTransition does not belong to this CrawlerVersion.",
        ),
        CrawlerRepositoryError::TransitionSourcePageTypeNotFound => (
            StatusCode::NOT_FOUND,
            "TRANSITION_SOURCE_PAGE_TYPE_NOT_FOUND",
            "The transition source PageType does not exist.",
        ),
        CrawlerRepositoryError::TransitionTargetPageTypeNotFound => (
            StatusCode::NOT_FOUND,
            "TRANSITION_TARGET_PAGE_TYPE_NOT_FOUND",
            "The transition target PageType does not exist.",
        ),
        CrawlerRepositoryError::InvalidDiscoveryTransition => (
            StatusCode::BAD_REQUEST,
            "INVALID_DISCOVERY_TRANSITION",
            "The DiscoveryTransition is invalid.",
        ),
        CrawlerRepositoryError::InvalidCanonicalizationPolicy => (
            StatusCode::BAD_REQUEST,
            "INVALID_CANONICALIZATION_POLICY",
            "The canonicalization policy is invalid.",
        ),
        CrawlerRepositoryError::InvalidDomainScope => (
            StatusCode::BAD_REQUEST,
            "INVALID_DOMAIN_SCOPE",
            "The Domain Scope policy is invalid.",
        ),
        CrawlerRepositoryError::InvalidCrawlGuardrails => (
            StatusCode::BAD_REQUEST,
            "INVALID_CRAWL_GUARDRAILS",
            "The crawler guardrails are invalid.",
        ),
        CrawlerRepositoryError::InvalidPageTypeBudget => (
            StatusCode::BAD_REQUEST,
            "INVALID_PAGE_TYPE_BUDGET",
            "The PageType budget is invalid.",
        ),
        CrawlerRepositoryError::InvalidTransitionBudget => (
            StatusCode::BAD_REQUEST,
            "INVALID_TRANSITION_BUDGET",
            "The transition budget is invalid.",
        ),
        CrawlerRepositoryError::CorruptState => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "PERSISTED_STATE_INVALID",
            "The durable Crawler state failed validation.",
        ),
        CrawlerRepositoryError::PublicationValidationFailed(_)
        | CrawlerRepositoryError::PublicationValidationInfrastructure => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "CRAWLER_PUBLICATION_VALIDATION_FAILED",
            "Crawler publication validation could not complete safely.",
        ),
        CrawlerRepositoryError::Database(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "CRAWLER_PERSISTENCE_FAILED",
            "The Crawler operation could not be completed safely.",
        ),
        CrawlerRepositoryError::ActiveDraftExists => (
            StatusCode::CONFLICT,
            "ACTIVE_DRAFT_EXISTS",
            "The Crawler already has an active Draft.",
        ),
        CrawlerRepositoryError::VersionNotPublished => (
            StatusCode::CONFLICT,
            "VERSION_NOT_PUBLISHED",
            "The CrawlerVersion is not Published.",
        ),
        CrawlerRepositoryError::PageTypeNotOwnedByVersion => (
            StatusCode::CONFLICT,
            "PAGE_TYPE_NOT_OWNED_BY_VERSION",
            "The PageType does not belong to this CrawlerVersion.",
        ),
        CrawlerRepositoryError::PageTypeInUse => (
            StatusCode::CONFLICT,
            "PAGE_TYPE_IN_USE",
            "The PageType is still referenced by the Draft configuration.",
        ),
        CrawlerRepositoryError::UrlMatcherNotFound
        | CrawlerRepositoryError::UrlMatcherNotOwnedByPageType
        | CrawlerRepositoryError::InvalidUrlMatcherDefinition => (
            StatusCode::CONFLICT,
            "CRAWLER_STATE_CONFLICT",
            "The Crawler configuration could not be changed safely.",
        ),
        CrawlerRepositoryError::InvalidLifecycleTransition
        | CrawlerRepositoryError::ConcurrentVersionTransition => (
            StatusCode::CONFLICT,
            "CRAWLER_STATE_CONFLICT",
            "The Crawler configuration could not be changed safely.",
        ),
    };
    policy_api_error(status, code, message, trace)
}

fn crawler_authoring_unavailable(trace: &TraceId) -> Response {
    policy_api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "CRAWLER_AUTHORING_UNAVAILABLE",
        "Crawler authoring is not attached to this runtime.",
        trace,
    )
}

fn invalid_policy_request(trace: &TraceId) -> Response {
    policy_api_error(
        StatusCode::BAD_REQUEST,
        "INVALID_REQUEST",
        "The request body is invalid.",
        trace,
    )
}

fn policy_api_error(status: StatusCode, code: &str, message: &str, trace: &TraceId) -> Response {
    error_response(status, ApiErrorEnvelope::new(code, message, trace.as_str()))
}
