//! Durable HTTP acceptance for one exact Published `CrawlerVersion`.

use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Json,
    extract::{Extension, Path, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use erabi_crawler::{ProductionRunSubmissionError, ProductionRunSubmissionRequest};
use erabi_db::repositories::{CrawlerRepository, CrawlerRepositoryError};
use erabi_domain::{
    CrawlerId, CrawlerVersionId, LayerValue, ResolvedValue, SeedId, SettingLayers,
    SnapshotOperationalSettings,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AppState,
    app::TraceId,
    error::{ApiErrorEnvelope, error_response},
    run_safety::{RobotsDecisionContext, RobotsOverrideInput, new_run_robots_decision},
};

const API_ACTOR: &str = "api";

/// Production accepts only an optional explicit Seed set and a fresh robots
/// override. Provider controls are never HTTP DTO fields.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProductionRunRequest {
    #[serde(default)]
    selected_seed_ids: Option<Vec<String>>,
    #[serde(default)]
    robots_override: Option<RobotsOverrideRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RobotsOverrideRequest {
    reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ProductionRunAcceptedResponse {
    run_id: String,
    job_id: String,
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn start_production_run(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((crawler_id, version_id)): Path<(String, String)>,
    input: Result<Json<ProductionRunRequest>, JsonRejection>,
) -> Response {
    let Ok(Json(input)) = input else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_PRODUCTION_RUN_REQUEST",
            "The Production Run request body is invalid.",
            &trace,
        );
    };
    let Some(service) = state.production_run_runtime() else {
        return unavailable(&trace);
    };
    let Some(database) = state.production_run_database() else {
        return unavailable(&trace);
    };
    let (Some(crawler_id), Some(version_id)) =
        (parse_crawler_id(&crawler_id), parse_version_id(&version_id))
    else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_PRODUCTION_RUN_REQUEST",
            "The Crawler or CrawlerVersion identity is invalid.",
            &trace,
        );
    };
    let Ok(selected_seed_ids) = parse_seed_ids(input.selected_seed_ids.as_deref()) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_PRODUCTION_RUN_REQUEST",
            "A selected Seed identity is invalid.",
            &trace,
        );
    };
    let repository = CrawlerRepository::new(&database);
    let crawler = match repository.get(crawler_id).await {
        Ok(crawler) => crawler,
        Err(error) => return crawler_error(&error, &trace),
    };
    let version = match repository.version(crawler_id, version_id).await {
        Ok(version) => version.version,
        Err(error) => return crawler_error(&error, &trace),
    };
    if version.state() != erabi_domain::CrawlerVersionState::Published {
        return api_error(
            StatusCode::CONFLICT,
            "CRAWLER_VERSION_NOT_PUBLISHED",
            "A Production Run requires the explicitly selected Published CrawlerVersion.",
            &trace,
        );
    }
    let settings = production_settings(&crawler, &version);
    let created_at = timestamp();
    let robots_input = input
        .robots_override
        .map_or(RobotsOverrideInput::Respect, |value| {
            RobotsOverrideInput::Override {
                reason: value.reason,
            }
        });
    let Ok(robots) = new_run_robots_decision(
        robots_input,
        RobotsDecisionContext {
            actor: API_ACTOR.to_owned(),
            decided_at: created_at.clone(),
            affected_scope: format!("crawler:{crawler_id}/version:{version_id}"),
            user_agent: settings.user_agent.value.clone(),
            crawler_version_id: Some(version_id),
        },
    ) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_ROBOTS_OVERRIDE",
            "A Production Run robots override requires a non-empty bounded reason.",
            &trace,
        );
    };
    let request = ProductionRunSubmissionRequest {
        crawler_id,
        crawler_version_id: version_id,
        selected_seed_ids,
        settings,
        robots,
        actor: API_ACTOR.to_owned(),
        created_at,
        priority: 0,
    };
    match service.submit(request, epoch_seconds()).await {
        Ok(accepted) => (
            StatusCode::ACCEPTED,
            Json(ProductionRunAcceptedResponse {
                run_id: accepted.run_id.to_string(),
                job_id: accepted.job_id,
            }),
        )
            .into_response(),
        Err(error) => production_error(&error, &trace),
    }
}

fn production_settings(
    crawler: &erabi_domain::Crawler,
    version: &erabi_domain::CrawlerVersion,
) -> SnapshotOperationalSettings {
    fn resolve<T: Clone>(value: &LayerValue<T>, built_in: T) -> ResolvedValue<T> {
        SettingLayers {
            per_run: LayerValue::Inherit,
            run_profile: None,
            crawler: Some(value.clone()),
            collection: None,
            global: LayerValue::Inherit,
        }
        .resolve(built_in)
    }
    let defaults = crawler.operational_defaults();
    let guardrails = version.guardrails();
    SnapshotOperationalSettings {
        max_pages: resolve(&defaults.max_pages, guardrails.max_pages),
        max_depth: resolve(&defaults.max_depth, guardrails.max_depth),
        max_duration_seconds: resolve(
            &defaults.max_duration_seconds,
            guardrails.max_duration_seconds,
        ),
        concurrency: resolve(
            &defaults.concurrency,
            guardrails.max_concurrent_requests_per_domain,
        ),
        request_delay_ms: resolve(&defaults.request_delay_ms, guardrails.min_request_delay_ms),
        timeout_ms: resolve(&defaults.timeout_ms, 30_000),
        screenshot: resolve(&defaults.screenshot, false),
        asset_download_limit_bytes: resolve(&defaults.asset_download_limit_bytes, 1_000_000),
        retain_artifacts: ResolvedValue {
            value: true,
            source: erabi_domain::SettingSource::BuiltInDefault,
        },
        user_agent: ResolvedValue {
            value: "Erabi/0.1".to_owned(),
            source: erabi_domain::SettingSource::BuiltInDefault,
        },
    }
}

fn parse_seed_ids(values: Option<&[String]>) -> Result<Option<Vec<SeedId>>, ()> {
    values
        .map(|values| {
            values
                .iter()
                .map(|value| {
                    Uuid::parse_str(value)
                        .ok()
                        .and_then(SeedId::from_uuid)
                        .ok_or(())
                })
                .collect()
        })
        .transpose()
}

fn parse_crawler_id(value: &str) -> Option<CrawlerId> {
    Uuid::parse_str(value).ok().and_then(CrawlerId::from_uuid)
}

fn parse_version_id(value: &str) -> Option<CrawlerVersionId> {
    Uuid::parse_str(value)
        .ok()
        .and_then(CrawlerVersionId::from_uuid)
}

fn crawler_error(error: &CrawlerRepositoryError, trace: &TraceId) -> Response {
    let (status, code, message) = match error {
        CrawlerRepositoryError::CrawlerNotFound => (
            StatusCode::NOT_FOUND,
            "CRAWLER_NOT_FOUND",
            "The Crawler was not found.",
        ),
        CrawlerRepositoryError::CrawlerVersionNotFound => (
            StatusCode::NOT_FOUND,
            "CRAWLER_VERSION_NOT_FOUND",
            "The CrawlerVersion was not found.",
        ),
        CrawlerRepositoryError::VersionNotOwnedByCrawler => (
            StatusCode::CONFLICT,
            "CRAWLER_VERSION_NOT_OWNED",
            "The CrawlerVersion does not belong to this Crawler.",
        ),
        CrawlerRepositoryError::VersionNotPublished => (
            StatusCode::CONFLICT,
            "CRAWLER_VERSION_NOT_PUBLISHED",
            "A Production Run requires the explicitly selected Published CrawlerVersion.",
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "PRODUCTION_RUN_SUBMISSION_FAILED",
            "Production Run submission could not be completed.",
        ),
    };
    api_error(status, code, message, trace)
}

fn production_error(error: &ProductionRunSubmissionError, trace: &TraceId) -> Response {
    match error {
        ProductionRunSubmissionError::Crawler(error) => crawler_error(error, trace),
        ProductionRunSubmissionError::NoEnabledSeeds => api_error(
            StatusCode::BAD_REQUEST,
            "NO_ENABLED_SEEDS",
            "Production Run requires at least one enabled selected Seed.",
            trace,
        ),
        ProductionRunSubmissionError::DuplicateSeedSelection
        | ProductionRunSubmissionError::SeedNotOwnedByVersion
        | ProductionRunSubmissionError::SeedDisabled
        | ProductionRunSubmissionError::Guardrails
        | ProductionRunSubmissionError::Snapshot(_) => api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_PRODUCTION_RUN_REQUEST",
            "The Production Run request is invalid for this immutable CrawlerVersion.",
            trace,
        ),
        ProductionRunSubmissionError::Job(_) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "PRODUCTION_RUN_SUBMISSION_FAILED",
            "Production Run submission could not be durably accepted.",
            trace,
        ),
    }
}

fn unavailable(trace: &TraceId) -> Response {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "PRODUCTION_RUN_UNAVAILABLE",
        "Production Run submission is not configured in this runtime.",
        trace,
    )
}

fn api_error(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    trace: &TraceId,
) -> Response {
    error_response(status, ApiErrorEnvelope::new(code, message, trace.as_str()))
}

fn timestamp() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    format!("unix:{seconds}")
}

fn epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
        })
}
