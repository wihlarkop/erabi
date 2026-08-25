use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Json,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use erabi_db::{
    ErabiDatabase,
    repositories::{CrawlerRepository, CrawlerRepositoryError, CrawlerVersionRecord},
};
use erabi_domain::{Crawler, CrawlerId, CrawlerVersionId, CrawlerVersionState};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AppState,
    app::TraceId,
    error::{ApiErrorEnvelope, error_response},
};

#[derive(Clone, Debug)]
pub(crate) struct CrawlerAuthoringService {
    database: ErabiDatabase,
}

impl CrawlerAuthoringService {
    #[must_use]
    pub(crate) const fn new(database: ErabiDatabase) -> Self {
        Self { database }
    }

    pub(crate) const fn database(&self) -> &ErabiDatabase {
        &self.database
    }

    async fn create(&self, name: String) -> Result<Crawler, CrawlerRepositoryError> {
        let crawler = Crawler::new(name);
        CrawlerRepository::new(&self.database)
            .create(&crawler)
            .await
            .map_err(CrawlerRepositoryError::Database)?;
        Ok(crawler)
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateCrawlerRequest {
    name: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateDraftRequest {
    base_version_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ActorRequest {
    actor: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct CrawlerDto {
    id: String,
    name: String,
    collection_id: Option<String>,
    active_draft_version_id: Option<String>,
    active_published_version_id: Option<String>,
    operational_defaults: erabi_domain::OperationalOverrides,
}

#[derive(Clone, Debug, Serialize)]
struct CrawlerVersionDto {
    id: String,
    crawler_id: String,
    state: CrawlerVersionState,
    active_draft: bool,
    active_published: bool,
    seed_count: usize,
    page_type_count: usize,
    transition_count: usize,
    config_hash: Option<String>,
    base_version_id: Option<String>,
    actor: Option<String>,
    occurred_at: Option<String>,
    warning_summary: Vec<String>,
}

pub(crate) async fn create_crawler(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Json(input): Json<CreateCrawlerRequest>,
) -> Response {
    if input.name.trim().is_empty() || input.name.chars().count() > 256 {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_CRAWLER_NAME",
            "The Crawler name must be non-empty and at most 256 characters.",
            &trace,
        );
    }
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    match runtime.create(input.name).await {
        Ok(crawler) => (StatusCode::CREATED, Json(crawler_dto(&crawler))).into_response(),
        Err(error) => crawler_error(error, &trace),
    }
}

pub(crate) async fn list_crawlers(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
) -> Response {
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    match CrawlerRepository::new(runtime.database()).list().await {
        Ok(crawlers) => Json(crawlers.iter().map(crawler_dto).collect::<Vec<_>>()).into_response(),
        Err(error) => crawler_error(error, &trace),
    }
}

pub(crate) async fn read_crawler(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path(raw_id): Path<String>,
) -> Response {
    let Ok(crawler_id) = parse_crawler_id(&raw_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_CRAWLER_ID",
            "The Crawler identifier is invalid.",
            &trace,
        );
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    match CrawlerRepository::new(runtime.database())
        .get(crawler_id)
        .await
    {
        Ok(crawler) => Json(crawler_dto(&crawler)).into_response(),
        Err(error) => crawler_error(error, &trace),
    }
}

pub(crate) async fn list_versions(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path(raw_id): Path<String>,
) -> Response {
    let Ok(crawler_id) = parse_crawler_id(&raw_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_CRAWLER_ID",
            "The Crawler identifier is invalid.",
            &trace,
        );
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    let repository = CrawlerRepository::new(runtime.database());
    let crawler = match repository.get(crawler_id).await {
        Ok(crawler) => crawler,
        Err(error) => return crawler_error(error, &trace),
    };
    match repository.list_versions(crawler_id).await {
        Ok(versions) => Json(
            versions
                .iter()
                .map(|version| version_dto(&crawler, version))
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(error) => crawler_error(error, &trace),
    }
}

pub(crate) async fn read_version(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
) -> Response {
    let (Ok(crawler_id), Ok(version_id)) = (
        parse_crawler_id(&raw_crawler_id),
        parse_version_id(&raw_version_id),
    ) else {
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
    let repository = CrawlerRepository::new(runtime.database());
    let crawler = match repository.get(crawler_id).await {
        Ok(crawler) => crawler,
        Err(error) => return crawler_error(error, &trace),
    };
    match repository.version(crawler_id, version_id).await {
        Ok(version) => Json(version_dto(&crawler, &version)).into_response(),
        Err(error) => crawler_error(error, &trace),
    }
}

pub(crate) async fn create_draft(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path(raw_crawler_id): Path<String>,
    Json(input): Json<CreateDraftRequest>,
) -> Response {
    let Ok(crawler_id) = parse_crawler_id(&raw_crawler_id) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_CRAWLER_ID",
            "The Crawler identifier is invalid.",
            &trace,
        );
    };
    let base_version_id = match input.base_version_id.as_deref() {
        Some(raw_id) => match parse_version_id(raw_id) {
            Ok(id) => Some(id),
            Err(()) => {
                return api_error(
                    StatusCode::BAD_REQUEST,
                    "INVALID_VERSION_ID",
                    "The base CrawlerVersion identifier is invalid.",
                    &trace,
                );
            }
        },
        None => None,
    };
    let Some(runtime) = state.crawler_authoring_runtime() else {
        return unavailable(&trace);
    };
    let repository = CrawlerRepository::new(runtime.database());
    let result = match base_version_id {
        Some(source) => {
            repository
                .create_draft_from_published(crawler_id, source, "api", &now())
                .await
        }
        None => repository.create_draft(crawler_id, "api", &now()).await,
    };
    match result {
        Ok(version) => match (
            repository.get(crawler_id).await,
            repository.version(crawler_id, version.id()).await,
        ) {
            (Ok(crawler), Ok(record)) => {
                (StatusCode::CREATED, Json(version_dto(&crawler, &record))).into_response()
            }
            (Err(error), _) | (_, Err(error)) => crawler_error(error, &trace),
        },
        Err(error) => crawler_error(error, &trace),
    }
}

pub(crate) async fn publish_version(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
    Json(input): Json<ActorRequest>,
) -> Response {
    lifecycle_version(
        state,
        trace,
        raw_crawler_id,
        raw_version_id,
        input.actor,
        true,
    )
    .await
}

pub(crate) async fn reactivate_version(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
    Json(input): Json<ActorRequest>,
) -> Response {
    lifecycle_version(
        state,
        trace,
        raw_crawler_id,
        raw_version_id,
        input.actor,
        false,
    )
    .await
}

async fn lifecycle_version(
    state: AppState,
    trace: TraceId,
    raw_crawler_id: String,
    raw_version_id: String,
    actor: Option<String>,
    publish: bool,
) -> Response {
    let (Ok(crawler_id), Ok(version_id)) = (
        parse_crawler_id(&raw_crawler_id),
        parse_version_id(&raw_version_id),
    ) else {
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
    let actor = actor
        .filter(|actor| !actor.trim().is_empty())
        .unwrap_or_else(|| "api".into());
    let repository = CrawlerRepository::new(&runtime.database);
    let result = if publish {
        repository
            .publish(crawler_id, version_id, &actor, &now())
            .await
    } else {
        match repository
            .reactivate_published_typed(crawler_id, version_id, &actor, &now())
            .await
        {
            Ok(()) => repository.version(crawler_id, version_id).await,
            Err(error) => Err(error),
        }
    };
    match result {
        Ok(record) => match repository.get(crawler_id).await {
            Ok(crawler) => Json(version_dto(&crawler, &record)).into_response(),
            Err(error) => crawler_error(error, &trace),
        },
        Err(error) => crawler_error(error, &trace),
    }
}

fn crawler_dto(crawler: &Crawler) -> CrawlerDto {
    CrawlerDto {
        id: crawler.id().to_string(),
        name: crawler.name.clone(),
        collection_id: crawler.collection_id().map(|id| id.to_string()),
        active_draft_version_id: crawler.active_draft_version_id().map(|id| id.to_string()),
        active_published_version_id: crawler
            .active_published_version_id()
            .map(|id| id.to_string()),
        operational_defaults: crawler.operational_defaults().clone(),
    }
}

fn version_dto(crawler: &Crawler, record: &CrawlerVersionRecord) -> CrawlerVersionDto {
    let version = &record.version;
    CrawlerVersionDto {
        id: version.id().to_string(),
        crawler_id: version.crawler_id().to_string(),
        state: version.state(),
        active_draft: crawler.active_draft_version_id() == Some(version.id()),
        active_published: crawler.active_published_version_id() == Some(version.id()),
        seed_count: version.seeds().len(),
        page_type_count: version.page_type_ids().len(),
        transition_count: version.transition_ids().len(),
        config_hash: record.audit.config_hash.clone(),
        base_version_id: record.audit.base_version_id.map(|id| id.to_string()),
        actor: record.audit.actor.clone(),
        occurred_at: record.audit.occurred_at.clone(),
        warning_summary: record.audit.warning_summary.clone(),
    }
}

#[allow(clippy::needless_pass_by_value)]
fn crawler_error(error: CrawlerRepositoryError, trace: &TraceId) -> Response {
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
        CrawlerRepositoryError::ActiveDraftExists => (
            StatusCode::CONFLICT,
            "ACTIVE_DRAFT_EXISTS",
            "The Crawler already has an active Draft.",
        ),
        CrawlerRepositoryError::VersionNotDraft => (
            StatusCode::CONFLICT,
            "VERSION_NOT_DRAFT",
            "Only a Draft can be published.",
        ),
        CrawlerRepositoryError::VersionNotActiveDraft => (
            StatusCode::CONFLICT,
            "VERSION_NOT_ACTIVE_DRAFT",
            "Only the active Draft can be mutated.",
        ),
        CrawlerRepositoryError::VersionNotPublished => (
            StatusCode::CONFLICT,
            "VERSION_NOT_PUBLISHED",
            "Only a Published version can be reactivated or cloned.",
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
        CrawlerRepositoryError::InvalidLifecycleTransition => (
            StatusCode::CONFLICT,
            "INVALID_LIFECYCLE_TRANSITION",
            "The requested CrawlerVersion lifecycle transition is not valid.",
        ),
        CrawlerRepositoryError::ConcurrentVersionTransition => (
            StatusCode::CONFLICT,
            "CONCURRENT_VERSION_TRANSITION",
            "Another CrawlerVersion lifecycle request won the transition.",
        ),
        CrawlerRepositoryError::CorruptState => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "PERSISTED_STATE_INVALID",
            "The durable Crawler state failed validation.",
        ),
        CrawlerRepositoryError::Database(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "CRAWLER_PERSISTENCE_FAILED",
            "The Crawler operation could not be completed safely.",
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

fn now() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    format!("unix:{seconds}")
}
