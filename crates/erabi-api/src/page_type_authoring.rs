use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Extension, Path, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use erabi_db::{
    ErabiDatabase,
    repositories::{CrawlerRepository, CrawlerRepositoryError, PageTypeRecord, UrlMatcherRecord},
};
use erabi_domain::{
    CrawlerId, CrawlerVersionId, PageTypeId, PageTypeMatchDecision, SpecificityKey, UrlMatcher,
    UrlMatcherDefinition, UrlMatcherKind, resolve_page_type,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AppState,
    app::TraceId,
    error::{ApiErrorEnvelope, error_response},
};

#[derive(Clone, Debug)]
pub(crate) struct PageTypeAuthoringService {
    database: ErabiDatabase,
}

impl PageTypeAuthoringService {
    #[must_use]
    pub(crate) const fn new(database: ErabiDatabase) -> Self {
        Self { database }
    }

    async fn list(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
    ) -> Result<Vec<PageTypeRecord>, CrawlerRepositoryError> {
        CrawlerRepository::new(&self.database)
            .list_page_types(crawler_id, version_id)
            .await
    }

    async fn read(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
    ) -> Result<PageTypeRecord, CrawlerRepositoryError> {
        CrawlerRepository::new(&self.database)
            .page_type(crawler_id, version_id, page_type_id)
            .await
    }

    async fn create(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        name: &str,
        priority: i32,
    ) -> Result<PageTypeRecord, CrawlerRepositoryError> {
        CrawlerRepository::new(&self.database)
            .create_page_type(crawler_id, version_id, name, priority, "api", &now())
            .await
    }

    async fn update(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
        name: &str,
        priority: i32,
    ) -> Result<PageTypeRecord, CrawlerRepositoryError> {
        CrawlerRepository::new(&self.database)
            .update_page_type(
                crawler_id,
                version_id,
                page_type_id,
                name,
                priority,
                "api",
                &now(),
            )
            .await
    }

    async fn delete(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
    ) -> Result<(), CrawlerRepositoryError> {
        CrawlerRepository::new(&self.database)
            .delete_page_type(crawler_id, version_id, page_type_id, "api", &now())
            .await
    }

    async fn list_matchers(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
    ) -> Result<Vec<UrlMatcherRecord>, CrawlerRepositoryError> {
        CrawlerRepository::new(&self.database)
            .list_url_matchers(crawler_id, version_id, page_type_id)
            .await
    }

    async fn read_matcher(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
        matcher_id: &str,
    ) -> Result<UrlMatcherRecord, CrawlerRepositoryError> {
        CrawlerRepository::new(&self.database)
            .url_matcher(crawler_id, version_id, page_type_id, matcher_id)
            .await
    }

    async fn create_matcher(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
        matcher: &UrlMatcher,
    ) -> Result<UrlMatcherRecord, CrawlerRepositoryError> {
        CrawlerRepository::new(&self.database)
            .create_url_matcher(crawler_id, version_id, page_type_id, matcher, "api", &now())
            .await
    }

    async fn update_matcher(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
        matcher_id: &str,
        matcher: &UrlMatcher,
    ) -> Result<UrlMatcherRecord, CrawlerRepositoryError> {
        CrawlerRepository::new(&self.database)
            .update_url_matcher(
                crawler_id,
                version_id,
                page_type_id,
                matcher_id,
                matcher,
                "api",
                &now(),
            )
            .await
    }

    async fn delete_matcher(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        page_type_id: PageTypeId,
        matcher_id: &str,
    ) -> Result<(), CrawlerRepositoryError> {
        CrawlerRepository::new(&self.database)
            .delete_url_matcher(
                crawler_id,
                version_id,
                page_type_id,
                matcher_id,
                "api",
                &now(),
            )
            .await
    }

    async fn resolve(
        &self,
        crawler_id: CrawlerId,
        version_id: CrawlerVersionId,
        input_url: &str,
    ) -> Result<PageTypeMatchDecision, MatchServiceError> {
        let url = url::Url::parse(input_url).map_err(|_| MatchServiceError::InvalidMatchUrl)?;
        let records = self
            .list(crawler_id, version_id)
            .await
            .map_err(MatchServiceError::Repository)?;
        let page_types = records
            .iter()
            .map(PageTypeRecord::domain_page_type)
            .collect::<Vec<_>>();
        Ok(resolve_page_type(&url, &page_types))
    }
}

#[derive(Debug)]
enum MatchServiceError {
    InvalidMatchUrl,
    Repository(CrawlerRepositoryError),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PageTypeRequest {
    name: String,
    priority: i32,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub(crate) enum MatcherDefinitionRequest {
    ExactUrl {
        url: String,
    },
    ExactHostPathTemplate {
        host: String,
        path_template: String,
        query: BTreeMap<String, String>,
    },
    PathPrefix {
        host: Option<String>,
        prefix: String,
    },
    PathGlob {
        host: Option<String>,
        pattern: String,
    },
    Regex {
        pattern: String,
    },
}

impl MatcherDefinitionRequest {
    fn into_domain(self) -> Result<UrlMatcher, &'static str> {
        match self {
            Self::ExactUrl { url } => url::Url::parse(&url)
                .map(UrlMatcher::exact_url)
                .map_err(|_| "The exact URL is malformed."),
            Self::ExactHostPathTemplate {
                host,
                path_template,
                query,
            } => UrlMatcher::try_exact_host_path_template(host, path_template, query)
                .map_err(|_| "The exact host/path template is invalid."),
            Self::PathPrefix { host, prefix } => {
                UrlMatcher::try_path_prefix(host, prefix).map_err(|_| "The path prefix is invalid.")
            }
            Self::PathGlob { host, pattern } => {
                UrlMatcher::path_glob(host, pattern).map_err(|_| "The path glob is invalid.")
            }
            Self::Regex { pattern } => {
                UrlMatcher::regex(pattern).map_err(|_| "The regular expression is invalid.")
            }
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
enum MatcherResponse {
    ExactUrl {
        id: String,
        ordinal: i64,
        url: String,
    },
    ExactHostPathTemplate {
        id: String,
        ordinal: i64,
        host: String,
        path_template: String,
        query: BTreeMap<String, String>,
    },
    PathPrefix {
        id: String,
        ordinal: i64,
        host: Option<String>,
        prefix: String,
    },
    PathGlob {
        id: String,
        ordinal: i64,
        host: Option<String>,
        pattern: String,
    },
    Regex {
        id: String,
        ordinal: i64,
        pattern: String,
    },
}

impl MatcherResponse {
    fn from_record(record: &UrlMatcherRecord) -> Self {
        match record.matcher.definition() {
            UrlMatcherDefinition::ExactUrl { url } => Self::ExactUrl {
                id: record.id.clone(),
                ordinal: record.ordinal,
                url: url.to_string(),
            },
            UrlMatcherDefinition::ExactHostPathTemplate {
                host,
                path_template,
                query,
            } => Self::ExactHostPathTemplate {
                id: record.id.clone(),
                ordinal: record.ordinal,
                host,
                path_template,
                query,
            },
            UrlMatcherDefinition::PathPrefix { host, prefix } => Self::PathPrefix {
                id: record.id.clone(),
                ordinal: record.ordinal,
                host,
                prefix,
            },
            UrlMatcherDefinition::PathGlob { host, pattern } => Self::PathGlob {
                id: record.id.clone(),
                ordinal: record.ordinal,
                host,
                pattern,
            },
            UrlMatcherDefinition::Regex { pattern } => Self::Regex {
                id: record.id.clone(),
                ordinal: record.ordinal,
                pattern,
            },
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct PageTypeResponse {
    id: String,
    crawler_version_id: String,
    name: String,
    priority: i32,
    matchers: Vec<MatcherResponse>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum MatchDecisionKind {
    Matched,
    AmbiguousPageType,
    Unmatched,
}

#[derive(Clone, Debug, Serialize)]
struct CandidateResponse {
    page_type_id: String,
    page_type_name: String,
    explicit_priority: i32,
    best_matcher_kind: String,
    matcher_kind_rank: u8,
    best_matched_patterns: Vec<String>,
    literal_path_segments: u32,
    explicit_query_constraints: u32,
    literal_characters: u32,
    wildcard_capture_count: u32,
}

#[derive(Clone, Debug, Serialize)]
struct MatchDecisionResponse {
    decision: MatchDecisionKind,
    candidate: Option<CandidateResponse>,
    candidates: Vec<CandidateResponse>,
}

pub(crate) async fn list_page_types(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
) -> Response {
    let Ok((crawler_id, version_id)) = parse_context(&raw_crawler_id, &raw_version_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_VERSION_ID",
            "The CrawlerVersion identifier is invalid.",
            &trace,
        );
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    match PageTypeAuthoringService::new(runtime.database().clone())
        .list(crawler_id, version_id)
        .await
    {
        Ok(page_types) => Json(
            page_types
                .iter()
                .map(page_type_response)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(error) => page_type_error(error, &trace),
    }
}

pub(crate) async fn read_page_type(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id, raw_page_type_id)): Path<(String, String, String)>,
) -> Response {
    let Ok((crawler_id, version_id)) = parse_context(&raw_crawler_id, &raw_version_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_VERSION_ID",
            "The CrawlerVersion identifier is invalid.",
            &trace,
        );
    };
    let Ok(page_type_id) = parse_page_type_id(&raw_page_type_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_PAGE_TYPE_ID",
            "The PageType identifier is invalid.",
            &trace,
        );
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    match PageTypeAuthoringService::new(runtime.database().clone())
        .read(crawler_id, version_id, page_type_id)
        .await
    {
        Ok(page_type) => Json(page_type_response(&page_type)).into_response(),
        Err(error) => page_type_error(error, &trace),
    }
}

pub(crate) async fn create_page_type(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
    input: Result<Json<PageTypeRequest>, JsonRejection>,
) -> Response {
    let Ok((crawler_id, version_id)) = parse_context(&raw_crawler_id, &raw_version_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_VERSION_ID",
            "The CrawlerVersion identifier is invalid.",
            &trace,
        );
    };
    let Ok(Json(input)) = input else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_PAGE_TYPE",
            "The PageType request is invalid.",
            &trace,
        );
    };
    if !valid_page_type_name(&input.name) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_PAGE_TYPE",
            "The PageType name must be non-empty and at most 256 characters.",
            &trace,
        );
    }
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    match PageTypeAuthoringService::new(runtime.database().clone())
        .create(crawler_id, version_id, &input.name, input.priority)
        .await
    {
        Ok(page_type) => {
            (StatusCode::CREATED, Json(page_type_response(&page_type))).into_response()
        }
        Err(error) => page_type_error(error, &trace),
    }
}

pub(crate) async fn update_page_type(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id, raw_page_type_id)): Path<(String, String, String)>,
    input: Result<Json<PageTypeRequest>, JsonRejection>,
) -> Response {
    let Ok((crawler_id, version_id)) = parse_context(&raw_crawler_id, &raw_version_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_VERSION_ID",
            "The CrawlerVersion identifier is invalid.",
            &trace,
        );
    };
    let Ok(page_type_id) = parse_page_type_id(&raw_page_type_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_PAGE_TYPE_ID",
            "The PageType identifier is invalid.",
            &trace,
        );
    };
    let Ok(Json(input)) = input else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_PAGE_TYPE",
            "The PageType request is invalid.",
            &trace,
        );
    };
    if !valid_page_type_name(&input.name) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_PAGE_TYPE",
            "The PageType name must be non-empty and at most 256 characters.",
            &trace,
        );
    }
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    match PageTypeAuthoringService::new(runtime.database().clone())
        .update(
            crawler_id,
            version_id,
            page_type_id,
            &input.name,
            input.priority,
        )
        .await
    {
        Ok(page_type) => Json(page_type_response(&page_type)).into_response(),
        Err(error) => page_type_error(error, &trace),
    }
}

pub(crate) async fn delete_page_type(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id, raw_page_type_id)): Path<(String, String, String)>,
) -> Response {
    let Ok((crawler_id, version_id)) = parse_context(&raw_crawler_id, &raw_version_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_VERSION_ID",
            "The CrawlerVersion identifier is invalid.",
            &trace,
        );
    };
    let Ok(page_type_id) = parse_page_type_id(&raw_page_type_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_PAGE_TYPE_ID",
            "The PageType identifier is invalid.",
            &trace,
        );
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    match PageTypeAuthoringService::new(runtime.database().clone())
        .delete(crawler_id, version_id, page_type_id)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => page_type_error(error, &trace),
    }
}

pub(crate) async fn list_matchers(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id, raw_page_type_id)): Path<(String, String, String)>,
) -> Response {
    let Ok((crawler_id, version_id)) = parse_context(&raw_crawler_id, &raw_version_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_VERSION_ID",
            "The CrawlerVersion identifier is invalid.",
            &trace,
        );
    };
    let Ok(page_type_id) = parse_page_type_id(&raw_page_type_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_PAGE_TYPE_ID",
            "The PageType identifier is invalid.",
            &trace,
        );
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    match PageTypeAuthoringService::new(runtime.database().clone())
        .list_matchers(crawler_id, version_id, page_type_id)
        .await
    {
        Ok(matchers) => Json(
            matchers
                .iter()
                .map(MatcherResponse::from_record)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(error) => page_type_error(error, &trace),
    }
}

pub(crate) async fn read_matcher(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id, raw_page_type_id, raw_matcher_id)): Path<(
        String,
        String,
        String,
        String,
    )>,
) -> Response {
    let Ok((crawler_id, version_id)) = parse_context(&raw_crawler_id, &raw_version_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_VERSION_ID",
            "The CrawlerVersion identifier is invalid.",
            &trace,
        );
    };
    let Ok(page_type_id) = parse_page_type_id(&raw_page_type_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_PAGE_TYPE_ID",
            "The PageType identifier is invalid.",
            &trace,
        );
    };
    if !valid_uuid_v7(&raw_matcher_id) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_URL_MATCHER_ID",
            "The URLMatcher identifier is invalid.",
            &trace,
        );
    }
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    match PageTypeAuthoringService::new(runtime.database().clone())
        .read_matcher(crawler_id, version_id, page_type_id, &raw_matcher_id)
        .await
    {
        Ok(matcher) => Json(MatcherResponse::from_record(&matcher)).into_response(),
        Err(error) => page_type_error(error, &trace),
    }
}

pub(crate) async fn create_matcher(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id, raw_page_type_id)): Path<(String, String, String)>,
    input: Result<Json<MatcherDefinitionRequest>, JsonRejection>,
) -> Response {
    let Ok((crawler_id, version_id, page_type_id)) =
        parse_matcher_context(&raw_crawler_id, &raw_version_id, &raw_page_type_id, &trace)
    else {
        return invalid_matcher_context(
            &raw_crawler_id,
            &raw_version_id,
            &raw_page_type_id,
            &trace,
        );
    };
    let Ok(Json(input)) = input else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_URL_MATCHER",
            "The URLMatcher request is invalid.",
            &trace,
        );
    };
    let matcher = match input.into_domain() {
        Ok(matcher) => matcher,
        Err(message) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_URL_MATCHER",
                message,
                &trace,
            );
        }
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    match PageTypeAuthoringService::new(runtime.database().clone())
        .create_matcher(crawler_id, version_id, page_type_id, &matcher)
        .await
    {
        Ok(matcher) => (
            StatusCode::CREATED,
            Json(MatcherResponse::from_record(&matcher)),
        )
            .into_response(),
        Err(error) => page_type_error(error, &trace),
    }
}

pub(crate) async fn update_matcher(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id, raw_page_type_id, raw_matcher_id)): Path<(
        String,
        String,
        String,
        String,
    )>,
    input: Result<Json<MatcherDefinitionRequest>, JsonRejection>,
) -> Response {
    let Ok((crawler_id, version_id, page_type_id)) =
        parse_matcher_context(&raw_crawler_id, &raw_version_id, &raw_page_type_id, &trace)
    else {
        return invalid_matcher_context(
            &raw_crawler_id,
            &raw_version_id,
            &raw_page_type_id,
            &trace,
        );
    };
    if !valid_uuid_v7(&raw_matcher_id) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_URL_MATCHER_ID",
            "The URLMatcher identifier is invalid.",
            &trace,
        );
    }
    let Ok(Json(input)) = input else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_URL_MATCHER",
            "The URLMatcher request is invalid.",
            &trace,
        );
    };
    let matcher = match input.into_domain() {
        Ok(matcher) => matcher,
        Err(message) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_URL_MATCHER",
                message,
                &trace,
            );
        }
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    match PageTypeAuthoringService::new(runtime.database().clone())
        .update_matcher(
            crawler_id,
            version_id,
            page_type_id,
            &raw_matcher_id,
            &matcher,
        )
        .await
    {
        Ok(matcher) => Json(MatcherResponse::from_record(&matcher)).into_response(),
        Err(error) => page_type_error(error, &trace),
    }
}

pub(crate) async fn delete_matcher(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id, raw_page_type_id, raw_matcher_id)): Path<(
        String,
        String,
        String,
        String,
    )>,
) -> Response {
    let Ok((crawler_id, version_id, page_type_id)) =
        parse_matcher_context(&raw_crawler_id, &raw_version_id, &raw_page_type_id, &trace)
    else {
        return invalid_matcher_context(
            &raw_crawler_id,
            &raw_version_id,
            &raw_page_type_id,
            &trace,
        );
    };
    if !valid_uuid_v7(&raw_matcher_id) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_URL_MATCHER_ID",
            "The URLMatcher identifier is invalid.",
            &trace,
        );
    }
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    match PageTypeAuthoringService::new(runtime.database().clone())
        .delete_matcher(crawler_id, version_id, page_type_id, &raw_matcher_id)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => page_type_error(error, &trace),
    }
}

pub(crate) async fn match_page_type(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
    input: Result<Json<MatchRequest>, JsonRejection>,
) -> Response {
    let Ok((crawler_id, version_id)) = parse_context(&raw_crawler_id, &raw_version_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_VERSION_ID",
            "The CrawlerVersion identifier is invalid.",
            &trace,
        );
    };
    let Ok(Json(input)) = input else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_MATCH_URL",
            "The match URL request is invalid.",
            &trace,
        );
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    let decision = match PageTypeAuthoringService::new(runtime.database().clone())
        .resolve(crawler_id, version_id, &input.url)
        .await
    {
        Ok(decision) => decision,
        Err(MatchServiceError::InvalidMatchUrl) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_MATCH_URL",
                "The URL to match is malformed.",
                &trace,
            );
        }
        Err(MatchServiceError::Repository(error)) => return page_type_error(error, &trace),
    };
    Json(match_decision_response(decision)).into_response()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MatchRequest {
    url: String,
}

fn page_type_response(page_type: &PageTypeRecord) -> PageTypeResponse {
    PageTypeResponse {
        id: page_type.id.to_string(),
        crawler_version_id: page_type.crawler_version_id.to_string(),
        name: page_type.name.clone(),
        priority: page_type.priority,
        matchers: page_type
            .matchers
            .iter()
            .map(MatcherResponse::from_record)
            .collect(),
    }
}

fn match_decision_response(decision: PageTypeMatchDecision) -> MatchDecisionResponse {
    match decision {
        PageTypeMatchDecision::Matched(candidate) => {
            let candidate = candidate_response(candidate);
            MatchDecisionResponse {
                decision: MatchDecisionKind::Matched,
                candidate: Some(candidate),
                candidates: Vec::new(),
            }
        }
        PageTypeMatchDecision::Ambiguous { candidates } => MatchDecisionResponse {
            decision: MatchDecisionKind::AmbiguousPageType,
            candidate: None,
            candidates: candidates.into_iter().map(candidate_response).collect(),
        },
        PageTypeMatchDecision::Unmatched => MatchDecisionResponse {
            decision: MatchDecisionKind::Unmatched,
            candidate: None,
            candidates: Vec::new(),
        },
    }
}

fn candidate_response(candidate: erabi_domain::PageTypeCandidate) -> CandidateResponse {
    let wildcard_capture_count = candidate.specificity.wildcard_capture_count();
    let SpecificityKey {
        matcher_kind_rank,
        literal_path_segments,
        explicit_query_constraints,
        literal_characters,
        inverse_wildcards: _,
    } = candidate.specificity;
    CandidateResponse {
        page_type_id: candidate.page_type_id.to_string(),
        page_type_name: candidate.page_type_name,
        explicit_priority: candidate.priority,
        best_matcher_kind: matcher_kind_label(candidate.matcher_kind).to_owned(),
        matcher_kind_rank,
        best_matched_patterns: candidate.matched_patterns,
        literal_path_segments,
        explicit_query_constraints,
        literal_characters,
        wildcard_capture_count,
    }
}

fn matcher_kind_label(kind: UrlMatcherKind) -> &'static str {
    match kind {
        UrlMatcherKind::ExactUrl => "EXACT_URL",
        UrlMatcherKind::ExactHostPathTemplate => "EXACT_HOST_PATH_TEMPLATE",
        UrlMatcherKind::PathPrefixOrGlob => "PATH_PREFIX_OR_GLOB",
        UrlMatcherKind::Regex => "REGEX",
    }
}

fn parse_context(
    raw_crawler_id: &str,
    raw_version_id: &str,
) -> Result<(CrawlerId, CrawlerVersionId), ()> {
    Ok((
        parse_crawler_id(raw_crawler_id)?,
        parse_version_id(raw_version_id)?,
    ))
}

fn parse_matcher_context(
    raw_crawler_id: &str,
    raw_version_id: &str,
    raw_page_type_id: &str,
    _trace: &TraceId,
) -> Result<(CrawlerId, CrawlerVersionId, PageTypeId), ()> {
    Ok((
        parse_crawler_id(raw_crawler_id)?,
        parse_version_id(raw_version_id)?,
        parse_page_type_id(raw_page_type_id)?,
    ))
}

fn invalid_matcher_context(
    raw_crawler_id: &str,
    raw_version_id: &str,
    raw_page_type_id: &str,
    trace: &TraceId,
) -> Response {
    if parse_crawler_id(raw_crawler_id).is_err() || parse_version_id(raw_version_id).is_err() {
        api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_VERSION_ID",
            "The CrawlerVersion identifier is invalid.",
            trace,
        )
    } else if parse_page_type_id(raw_page_type_id).is_err() {
        api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_PAGE_TYPE_ID",
            "The PageType identifier is invalid.",
            trace,
        )
    } else {
        api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_URL_MATCHER",
            "The URLMatcher request is invalid.",
            trace,
        )
    }
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

fn valid_uuid_v7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| uuid.get_version_num() == 7)
}

fn valid_page_type_name(name: &str) -> bool {
    !name.trim().is_empty() && name.chars().count() <= 256
}

#[allow(clippy::needless_pass_by_value)]
fn page_type_error(error: CrawlerRepositoryError, trace: &TraceId) -> Response {
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
            "The requested CrawlerVersion is not a Draft.",
        ),
        CrawlerRepositoryError::VersionNotActiveDraft => (
            StatusCode::CONFLICT,
            "VERSION_NOT_ACTIVE_DRAFT",
            "Only the active Draft may be mutated.",
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
        CrawlerRepositoryError::UrlMatcherNotFound => (
            StatusCode::NOT_FOUND,
            "URL_MATCHER_NOT_FOUND",
            "The requested URLMatcher does not exist.",
        ),
        CrawlerRepositoryError::UrlMatcherNotOwnedByPageType => (
            StatusCode::CONFLICT,
            "URL_MATCHER_NOT_OWNED_BY_PAGE_TYPE",
            "The URLMatcher does not belong to this PageType.",
        ),
        CrawlerRepositoryError::InvalidUrlMatcherDefinition => (
            StatusCode::BAD_REQUEST,
            "INVALID_URL_MATCHER",
            "The URLMatcher definition is invalid.",
        ),
        CrawlerRepositoryError::CorruptState => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "PERSISTED_STATE_INVALID",
            "The durable crawler semantic state failed validation.",
        ),
        CrawlerRepositoryError::Database(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "CRAWLER_PERSISTENCE_FAILED",
            "The crawler semantic operation could not be completed safely.",
        ),
        _ => (
            StatusCode::CONFLICT,
            "CRAWLER_SEMANTIC_CONFLICT",
            "The crawler semantic operation could not be applied.",
        ),
    };
    api_error(status, code, message, trace)
}

fn unavailable(trace: &TraceId) -> Response {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "CRAWLER_AUTHORING_UNAVAILABLE",
        "Crawler authoring is not attached to this runtime.",
        trace,
    )
}

fn api_error(status: StatusCode, code: &str, message: &str, trace: &TraceId) -> Response {
    error_response(status, ApiErrorEnvelope::new(code, message, trace.as_str()))
}

fn now() -> String {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    format!("unix:{seconds}")
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;

    use super::*;

    #[tokio::test]
    async fn persisted_matcher_corruption_maps_to_the_stable_api_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = page_type_error(CrawlerRepositoryError::CorruptState, &TraceId::for_test());
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let body: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(body["code"], "PERSISTED_STATE_INVALID");
        Ok(())
    }
}
