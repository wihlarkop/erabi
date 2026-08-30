//! Stable HTTP acceptance boundary for one independent Quick Scrape URL.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Json,
    extract::{Extension, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use erabi_crawler::{OriginKey, QuickScrapeSubmissionError, QuickScrapeSubmissionRequest};
use erabi_domain::{ResolvedValue, SettingSource, SnapshotOperationalSettings};
use serde::{Deserialize, Serialize};

use crate::{
    AppState,
    app::TraceId,
    error::{ApiErrorEnvelope, error_response},
    run_safety::{RobotsDecisionContext, RobotsOverrideInput, new_run_robots_decision},
};

const API_ACTOR: &str = "api";
const QUICK_SCRAPE_MAX_ATTEMPTS: u32 = 3;

/// The deliberately small Task 6 API: exactly one target and, optionally, a
/// fresh reasoned robots override. There are no provider fields or batch URLs.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QuickScrapeRequest {
    target_url: String,
    #[serde(default)]
    robots_override: Option<RobotsOverrideRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RobotsOverrideRequest {
    reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[allow(clippy::struct_field_names)] // Wire contract intentionally uses durable identity names.
pub(crate) struct QuickScrapeAcceptedResponse {
    run_id: String,
    job_id: String,
    source_id: String,
}

pub(crate) async fn start_quick_scrape(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    input: Result<Json<QuickScrapeRequest>, JsonRejection>,
) -> Response {
    let Ok(Json(input)) = input else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_QUICK_SCRAPE_REQUEST",
            "The Quick Scrape request body is invalid.",
            &trace,
        );
    };
    let Some(service) = state.quick_scrape_runtime() else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "QUICK_SCRAPE_UNAVAILABLE",
            "Quick Scrape is not configured in this runtime.",
            &trace,
        );
    };
    let Ok(url) = input.target_url.parse() else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_QUICK_SCRAPE_REQUEST",
            "The target URL is invalid.",
            &trace,
        );
    };
    let Ok(origin) = OriginKey::from_url(&url) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_QUICK_SCRAPE_REQUEST",
            "The target URL must use a supported HTTP(S) origin.",
            &trace,
        );
    };
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
            affected_scope: origin.to_string(),
            user_agent: quick_scrape_settings().user_agent.value.clone(),
            crawler_version_id: None,
        },
    ) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_ROBOTS_OVERRIDE",
            "A Quick Scrape robots override requires a non-empty bounded reason.",
            &trace,
        );
    };

    let request = QuickScrapeSubmissionRequest {
        target_url: input.target_url,
        collection_id: None,
        source_name: None,
        settings: quick_scrape_settings(),
        robots,
        actor: API_ACTOR.to_owned(),
        created_at,
        priority: 0,
        max_attempts: QUICK_SCRAPE_MAX_ATTEMPTS,
    };
    match service.submit(request, epoch_seconds()).await {
        Ok(accepted) => (
            StatusCode::ACCEPTED,
            Json(QuickScrapeAcceptedResponse {
                run_id: accepted.run_id.to_string(),
                job_id: accepted.job_id,
                source_id: accepted.source_id.to_string(),
            }),
        )
            .into_response(),
        Err(error) => quick_scrape_error(&error, &trace),
    }
}

fn quick_scrape_settings() -> SnapshotOperationalSettings {
    fn resolved<T>(value: T) -> ResolvedValue<T> {
        ResolvedValue {
            value,
            source: SettingSource::BuiltInDefault,
        }
    }
    SnapshotOperationalSettings {
        max_pages: resolved(1),
        max_depth: resolved(0),
        max_duration_seconds: resolved(60),
        concurrency: resolved(1),
        request_delay_ms: resolved(250),
        timeout_ms: resolved(30_000),
        screenshot: resolved(false),
        asset_download_limit_bytes: resolved(1_000_000),
        retain_artifacts: resolved(true),
        user_agent: resolved("Erabi/0.1".to_owned()),
    }
}

fn quick_scrape_error(error: &QuickScrapeSubmissionError, trace: &TraceId) -> Response {
    let (status, code, message) = match error {
        QuickScrapeSubmissionError::SourceIntake(_) => (
            StatusCode::BAD_REQUEST,
            "QUICK_SCRAPE_TARGET_REJECTED",
            "The target could not be accepted safely.",
        ),
        QuickScrapeSubmissionError::Snapshot(_) => (
            StatusCode::BAD_REQUEST,
            "INVALID_QUICK_SCRAPE_REQUEST",
            "The Quick Scrape request is invalid.",
        ),
        QuickScrapeSubmissionError::Job(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "QUICK_SCRAPE_SUBMISSION_FAILED",
            "Quick Scrape could not be durably accepted.",
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

fn epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
        })
}

fn timestamp() -> String {
    format!("unix:{}", epoch_seconds())
}

#[allow(dead_code)]
fn _assert_runtime_send_sync(_: Arc<erabi_crawler::QuickScrapeSubmissionService>) {}
