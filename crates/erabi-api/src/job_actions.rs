//! Protected HTTP presentation for explicit Task 4 job actions.

use std::{
    future::Future,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use erabi_db::repositories::JobId;
use erabi_jobs::{JobAction, JobActionError, JobActionResult, RerunFullCrawlInput};
use serde::{Deserialize, Serialize};

use crate::{
    AppState,
    app::TraceId,
    error::{ApiErrorEnvelope, error_response},
};

#[derive(Deserialize)]
pub(crate) struct QueueActionInput {
    pub priority: i32,
    pub scheduled_at: Option<i64>,
}

#[derive(Deserialize)]
pub(crate) struct RerunFullCrawlRequest {
    pub robots_override_reason: Option<String>,
}

pub(crate) async fn retry_failed_parts(
    State(state): State<AppState>,
    Path(raw_job_id): Path<String>,
    axum::extract::Extension(trace): axum::extract::Extension<TraceId>,
) -> Response {
    action_response(state, raw_job_id, trace, |service, job_id| async move {
        service.retry_failed_parts(&job_id, now()).await
    })
    .await
}

pub(crate) async fn rerun_full_crawl(
    State(state): State<AppState>,
    Path(raw_job_id): Path<String>,
    axum::extract::Extension(trace): axum::extract::Extension<TraceId>,
    Json(input): Json<RerunFullCrawlRequest>,
) -> Response {
    action_response(state, raw_job_id, trace, |service, job_id| async move {
        service
            .rerun_full_crawl(
                &job_id,
                now(),
                RerunFullCrawlInput {
                    robots_override_reason: input.robots_override_reason,
                },
            )
            .await
    })
    .await
}

pub(crate) async fn resume(
    State(state): State<AppState>,
    Path(raw_job_id): Path<String>,
    axum::extract::Extension(trace): axum::extract::Extension<TraceId>,
) -> Response {
    action_response(state, raw_job_id, trace, |service, job_id| async move {
        service.resume(&job_id, now()).await
    })
    .await
}

pub(crate) async fn restart(
    State(state): State<AppState>,
    Path(raw_job_id): Path<String>,
    axum::extract::Extension(trace): axum::extract::Extension<TraceId>,
) -> Response {
    action_response(state, raw_job_id, trace, |service, job_id| async move {
        service.restart_from_beginning(&job_id, now()).await
    })
    .await
}

pub(crate) async fn retry(
    State(state): State<AppState>,
    Path(raw_job_id): Path<String>,
    axum::extract::Extension(trace): axum::extract::Extension<TraceId>,
) -> Response {
    action_response(state, raw_job_id, trace, |service, job_id| async move {
        service.retry(&job_id, now()).await
    })
    .await
}

pub(crate) async fn cancel(
    State(state): State<AppState>,
    Path(raw_job_id): Path<String>,
    axum::extract::Extension(trace): axum::extract::Extension<TraceId>,
) -> Response {
    action_response(state, raw_job_id, trace, |service, job_id| async move {
        service.cancel(&job_id, now()).await
    })
    .await
}

pub(crate) async fn reprioritize(
    State(state): State<AppState>,
    Path(raw_job_id): Path<String>,
    axum::extract::Extension(trace): axum::extract::Extension<TraceId>,
    Json(input): Json<QueueActionInput>,
) -> Response {
    let Some(runtime) = state.job_actions_runtime() else {
        return unavailable(&trace);
    };
    let Ok(job_id) = raw_job_id.parse::<JobId>() else {
        return invalid_job_id(&trace);
    };
    match runtime
        .service()
        .reprioritize(&job_id, input.priority, input.scheduled_at, now())
        .await
    {
        Ok(result) => success(result),
        Err(error) => action_error(&error, &trace),
    }
}

pub(crate) async fn remove(
    State(state): State<AppState>,
    Path(raw_job_id): Path<String>,
    axum::extract::Extension(trace): axum::extract::Extension<TraceId>,
) -> Response {
    let Some(runtime) = state.job_actions_runtime() else {
        return unavailable(&trace);
    };
    let Ok(job_id) = raw_job_id.parse::<JobId>() else {
        return invalid_job_id(&trace);
    };
    match runtime.service().remove(&job_id).await {
        Ok(result) => success(result),
        Err(error) => action_error(&error, &trace),
    }
}

async fn action_response<F, Fut>(
    state: AppState,
    raw_job_id: String,
    trace: TraceId,
    action: F,
) -> Response
where
    F: FnOnce(erabi_jobs::JobActionService, JobId) -> Fut,
    Fut: Future<Output = Result<JobActionResult, JobActionError>>,
{
    let Some(runtime) = state.job_actions_runtime() else {
        return unavailable(&trace);
    };
    let Ok(job_id) = raw_job_id.parse::<JobId>() else {
        return invalid_job_id(&trace);
    };
    match action(runtime.service().clone(), job_id).await {
        Ok(result) => success(result),
        Err(error) => action_error(&error, &trace),
    }
}

fn success(result: JobActionResult) -> Response {
    Json(JobActionResponse {
        action: action_name(result.action),
        job_id: result.job_id.to_string(),
        parent_job_id: result.parent_job_id.map(|id| id.to_string()),
        crawl_run_id: result.crawl_run_id,
        state: if result.action == JobAction::Remove {
            "REMOVED"
        } else {
            state_name(result.state)
        },
        failed_part_count: result.failed_part_count,
        removed: result.action == JobAction::Remove,
    })
    .into_response()
}

fn action_error(error: &JobActionError, trace: &TraceId) -> Response {
    let (status, code, message) = match error {
        JobActionError::NotFound => (
            StatusCode::NOT_FOUND,
            "JOB_NOT_FOUND",
            "The requested job or run does not exist.",
        ),
        JobActionError::IllegalLifecycleState => (
            StatusCode::CONFLICT,
            "ILLEGAL_LIFECYCLE_STATE",
            "The requested action is not legal for the job's current state.",
        ),
        JobActionError::AttemptsExhausted => (
            StatusCode::CONFLICT,
            "ATTEMPTS_EXHAUSTED",
            "The job has reached its bounded attempt limit.",
        ),
        JobActionError::RetryAlreadyContinued => (
            StatusCode::CONFLICT,
            "RETRY_ALREADY_CONTINUED",
            "Retry the latest continuation instead of creating a sibling retry.",
        ),
        JobActionError::CrawlRunRequired => (
            StatusCode::CONFLICT,
            "CRAWL_RUN_REQUIRED",
            "Rerun Full Crawl requires durable Crawl Run evidence.",
        ),
        JobActionError::RobotsOverrideReasonRequired => (
            StatusCode::BAD_REQUEST,
            "ROBOTS_OVERRIDE_REASON_REQUIRED",
            "A new robots override reason is required for this independent rerun.",
        ),
        JobActionError::RobotsOverrideReasonInvalid => (
            StatusCode::BAD_REQUEST,
            "ROBOTS_OVERRIDE_REASON_INVALID",
            "The submitted robots override reason is invalid.",
        ),
        JobActionError::CheckpointMissing => (
            StatusCode::CONFLICT,
            "CHECKPOINT_MISSING",
            "No durable checkpoint is available for resume.",
        ),
        JobActionError::CheckpointUnsafe => (
            StatusCode::CONFLICT,
            "CHECKPOINT_UNSAFE",
            "The durable checkpoint cannot be used safely.",
        ),
        JobActionError::CheckpointIncompatible => (
            StatusCode::CONFLICT,
            "CHECKPOINT_INCOMPATIBLE",
            "The checkpoint does not match the immutable run snapshot.",
        ),
        JobActionError::NotRemovable => (
            StatusCode::CONFLICT,
            "NOT_REMOVABLE",
            "Only safe, never-started queue work may be removed.",
        ),
        JobActionError::NotReprioritizable => (
            StatusCode::CONFLICT,
            "NOT_REPRIORITIZABLE",
            "Only queued work may be moved in the queue.",
        ),
        JobActionError::ConcurrentTransition => (
            StatusCode::CONFLICT,
            "CONCURRENT_TRANSITION_CONFLICT",
            "Another action or lease owns the current transition.",
        ),
        JobActionError::Repository(_) | JobActionError::Cancellation(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "JOB_ACTION_FAILED",
            "The job action could not be completed safely.",
        ),
    };
    error_response(status, ApiErrorEnvelope::new(code, message, trace.as_str()))
}

fn unavailable(trace: &TraceId) -> Response {
    error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        ApiErrorEnvelope::new(
            "JOB_ACTIONS_UNAVAILABLE",
            "Durable job actions are not attached to this runtime.",
            trace.as_str(),
        ),
    )
}

fn invalid_job_id(trace: &TraceId) -> Response {
    error_response(
        StatusCode::BAD_REQUEST,
        ApiErrorEnvelope::new(
            "INVALID_JOB_ID",
            "The job identifier is invalid.",
            trace.as_str(),
        ),
    )
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
        })
}

fn action_name(action: JobAction) -> &'static str {
    match action {
        JobAction::RetryFailedParts => "RETRY_FAILED_PARTS",
        JobAction::RerunFullCrawl => "RERUN_FULL_CRAWL",
        JobAction::ResumeCheckpoint => "RESUME_CHECKPOINT",
        JobAction::RestartFromBeginning => "RESTART_FROM_BEGINNING",
        JobAction::Retry => "RETRY",
        JobAction::Cancel => "CANCEL",
        JobAction::Reprioritize => "REPRIORITIZE",
        JobAction::Remove => "REMOVE",
    }
}

fn state_name(state: erabi_db::repositories::JobState) -> &'static str {
    match state {
        erabi_db::repositories::JobState::Queued => "QUEUED",
        erabi_db::repositories::JobState::Running => "RUNNING",
        erabi_db::repositories::JobState::Succeeded => "SUCCEEDED",
        erabi_db::repositories::JobState::Failed => "FAILED",
        erabi_db::repositories::JobState::Cancelled => "CANCELLED",
    }
}

#[derive(Serialize)]
struct JobActionResponse {
    action: &'static str,
    job_id: String,
    parent_job_id: Option<String>,
    crawl_run_id: Option<String>,
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    failed_part_count: Option<usize>,
    removed: bool,
}
