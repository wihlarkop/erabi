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
use erabi_crawler::{
    OriginKey, QuickScrapeSubmissionError, QuickScrapeSubmissionRequest, SourceIntakeError,
};
use erabi_db::repositories::SourceRepositoryError;
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
/// Matches the established bounded multi-URL Test Lab surface. Sequential
/// submission keeps the associated DNS/probe work conservatively bounded.
pub(crate) const QUICK_SCRAPE_BATCH_MAX_ITEMS: usize = 8;
/// Fits eight independently bounded 4 KiB target URLs and 1 KiB override
/// reasons with JSON overhead, while remaining below the 64 KiB global
/// mutation-body ceiling.
pub(crate) const QUICK_SCRAPE_BATCH_BODY_LIMIT_BYTES: usize = 48 * 1024;

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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QuickScrapeBatchRequest {
    items: Vec<QuickScrapeRequest>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[allow(clippy::struct_field_names)] // Wire contract intentionally uses durable identity names.
pub(crate) struct QuickScrapeAcceptedResponse {
    run_id: String,
    job_id: String,
    source_id: String,
}

#[derive(Debug, Serialize)]
struct QuickScrapeBatchResponse {
    halted: bool,
    items: Vec<QuickScrapeBatchOutcome>,
}

/// Ordered wire outcomes for the convenience envelope. `CONFLICT` is omitted
/// deliberately: Task 6's single-item primitive has no legitimate conflict
/// error, so the batch route must not manufacture one.
#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "SCREAMING_SNAKE_CASE")]
enum QuickScrapeBatchOutcome {
    Accepted {
        run_id: String,
        job_id: String,
        source_id: String,
    },
    ValidationError {
        code: &'static str,
    },
    SystemError {
        code: &'static str,
        trace_id: String,
    },
    NotProcessed {
        code: &'static str,
    },
}

#[derive(Clone, Copy)]
enum QuickScrapeRequestError {
    InvalidTargetUrl,
    UnsupportedOrigin,
    InvalidRobotsOverride,
}

enum QuickScrapeBatchItemError {
    Validation { code: &'static str },
    System,
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
    let created_at = timestamp();
    let request = match input.into_submission_request(&created_at) {
        Ok(request) => request,
        Err(error) => return quick_scrape_request_error(error, &trace),
    };
    match service.submit(request, epoch_seconds()).await {
        Ok(accepted) => (
            StatusCode::ACCEPTED,
            Json(QuickScrapeAcceptedResponse::from(accepted)),
        )
            .into_response(),
        Err(error) => quick_scrape_error(&error, &trace),
    }
}

/// Accepts a bounded pasted-URL convenience envelope. Every accepted item
/// delegates to the Task 6 primitive; this route owns neither a batch run nor
/// a batch transaction.
pub(crate) async fn start_quick_scrape_batch(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    input: Result<Json<QuickScrapeBatchRequest>, JsonRejection>,
) -> Response {
    let input = match input {
        Ok(Json(input)) => input,
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            return api_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "BODY_TOO_LARGE",
                "The Quick Scrape batch body exceeds this endpoint's limit.",
                &trace,
            );
        }
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_QUICK_SCRAPE_BATCH_REQUEST",
                "The Quick Scrape batch request body is invalid.",
                &trace,
            );
        }
    };
    if input.items.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "EMPTY_QUICK_SCRAPE_BATCH",
            "A Quick Scrape batch must contain at least one item.",
            &trace,
        );
    }
    if input.items.len() > QUICK_SCRAPE_BATCH_MAX_ITEMS {
        return api_error(
            StatusCode::BAD_REQUEST,
            "TOO_MANY_QUICK_SCRAPE_ITEMS",
            "The Quick Scrape batch exceeds the fixed item limit.",
            &trace,
        );
    }
    let Some(service) = state.quick_scrape_runtime() else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "QUICK_SCRAPE_UNAVAILABLE",
            "Quick Scrape is not configured in this runtime.",
            &trace,
        );
    };

    // All independently accepted runs share the request acceptance instant,
    // but each call below retains Task 6's own Source and run/job transaction.
    let (created_at, now) = submission_time();
    let mut outcomes = Vec::with_capacity(input.items.len());
    let mut items = input.items.into_iter();
    let mut halted = false;
    while let Some(item) = items.next() {
        let request = match item.into_submission_request(&created_at) {
            Ok(request) => request,
            Err(error) => {
                outcomes.push(QuickScrapeBatchOutcome::ValidationError {
                    code: quick_scrape_request_error_code(error),
                });
                continue;
            }
        };
        match service.submit(request, now).await {
            Ok(accepted) => outcomes.push(QuickScrapeBatchOutcome::Accepted {
                run_id: accepted.run_id.to_string(),
                job_id: accepted.job_id,
                source_id: accepted.source_id.to_string(),
            }),
            Err(error) => match quick_scrape_batch_item_error(&error) {
                QuickScrapeBatchItemError::Validation { code } => {
                    outcomes.push(QuickScrapeBatchOutcome::ValidationError { code });
                }
                QuickScrapeBatchItemError::System => {
                    outcomes.push(QuickScrapeBatchOutcome::SystemError {
                        code: "QUICK_SCRAPE_SUBMISSION_FAILED",
                        trace_id: trace.as_str().to_owned(),
                    });
                    // A database or invariant failure is not a validation
                    // result. Keep prior durable acceptances visible, mark
                    // each remaining input as unattempted, and stop admission.
                    let has_unprocessed_items = items.len() > 0;
                    outcomes.extend(items.by_ref().map(|_| {
                        QuickScrapeBatchOutcome::NotProcessed {
                            code: "BATCH_HALTED",
                        }
                    }));
                    halted = has_unprocessed_items;
                    break;
                }
            },
        }
    }

    (
        StatusCode::ACCEPTED,
        Json(QuickScrapeBatchResponse {
            halted,
            items: outcomes,
        }),
    )
        .into_response()
}

impl QuickScrapeRequest {
    fn into_submission_request(
        self,
        created_at: &str,
    ) -> Result<QuickScrapeSubmissionRequest, QuickScrapeRequestError> {
        let url = self
            .target_url
            .parse()
            .map_err(|_| QuickScrapeRequestError::InvalidTargetUrl)?;
        let origin =
            OriginKey::from_url(&url).map_err(|_| QuickScrapeRequestError::UnsupportedOrigin)?;
        let settings = quick_scrape_settings();
        let robots_input = self
            .robots_override
            .map_or(RobotsOverrideInput::Respect, |value| {
                RobotsOverrideInput::Override {
                    reason: value.reason,
                }
            });
        let robots = new_run_robots_decision(
            robots_input,
            RobotsDecisionContext {
                actor: API_ACTOR.to_owned(),
                decided_at: created_at.to_owned(),
                affected_scope: origin.to_string(),
                user_agent: settings.user_agent.value.clone(),
                crawler_version_id: None,
            },
        )
        .map_err(|_| QuickScrapeRequestError::InvalidRobotsOverride)?;

        Ok(QuickScrapeSubmissionRequest {
            target_url: self.target_url,
            collection_id: None,
            source_name: None,
            settings,
            robots,
            actor: API_ACTOR.to_owned(),
            created_at: created_at.to_owned(),
            priority: 0,
            max_attempts: QUICK_SCRAPE_MAX_ATTEMPTS,
        })
    }
}

impl From<erabi_crawler::QuickScrapeSubmission> for QuickScrapeAcceptedResponse {
    fn from(accepted: erabi_crawler::QuickScrapeSubmission) -> Self {
        Self {
            run_id: accepted.run_id.to_string(),
            job_id: accepted.job_id,
            source_id: accepted.source_id.to_string(),
        }
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

fn quick_scrape_batch_item_error(error: &QuickScrapeSubmissionError) -> QuickScrapeBatchItemError {
    match error {
        QuickScrapeSubmissionError::SourceIntake(
            SourceIntakeError::Canonicalization(_)
            | SourceIntakeError::NetworkTarget(_)
            | SourceIntakeError::Repository(SourceRepositoryError::InvalidInput(_)),
        ) => QuickScrapeBatchItemError::Validation {
            code: "QUICK_SCRAPE_TARGET_REJECTED",
        },
        QuickScrapeSubmissionError::Snapshot(_)
        | QuickScrapeSubmissionError::SourceIntake(SourceIntakeError::Repository(
            SourceRepositoryError::CollectionNotFound
            | SourceRepositoryError::NotFound
            | SourceRepositoryError::CorruptState
            | SourceRepositoryError::Database(_),
        ))
        | QuickScrapeSubmissionError::Job(_) => QuickScrapeBatchItemError::System,
    }
}

fn quick_scrape_request_error(error: QuickScrapeRequestError, trace: &TraceId) -> Response {
    let (code, message) = match error {
        QuickScrapeRequestError::InvalidTargetUrl => {
            ("INVALID_QUICK_SCRAPE_REQUEST", "The target URL is invalid.")
        }
        QuickScrapeRequestError::UnsupportedOrigin => (
            "INVALID_QUICK_SCRAPE_REQUEST",
            "The target URL must use a supported HTTP(S) origin.",
        ),
        QuickScrapeRequestError::InvalidRobotsOverride => (
            "INVALID_ROBOTS_OVERRIDE",
            "A Quick Scrape robots override requires a non-empty bounded reason.",
        ),
    };
    api_error(StatusCode::BAD_REQUEST, code, message, trace)
}

const fn quick_scrape_request_error_code(error: QuickScrapeRequestError) -> &'static str {
    match error {
        QuickScrapeRequestError::InvalidTargetUrl | QuickScrapeRequestError::UnsupportedOrigin => {
            "INVALID_QUICK_SCRAPE_REQUEST"
        }
        QuickScrapeRequestError::InvalidRobotsOverride => "INVALID_ROBOTS_OVERRIDE",
    }
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

fn submission_time() -> (String, i64) {
    let now = epoch_seconds();
    (format!("unix:{now}"), now)
}

fn timestamp() -> String {
    format!("unix:{}", epoch_seconds())
}

#[allow(dead_code)]
fn _assert_runtime_send_sync(_: Arc<erabi_crawler::QuickScrapeSubmissionService>) {}
