//! Authenticated replayable SSE transport for durable job progress.

use std::{
    collections::{BTreeMap, VecDeque},
    convert::Infallible,
    time::Duration,
};

use axum::{
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use erabi_db::repositories::{
    JobId, ProgressEvent, ProgressMetadataValue, ProgressReplayRequest, ProgressRepositoryError,
    ProgressSequence, ProgressTerminalState,
};
use erabi_jobs::{ProgressService, ProgressServiceError};
use futures_util::{Stream, stream};
use serde::Serialize;
use tokio::sync::broadcast::error::RecvError;

use crate::{
    AppState,
    app::TraceId,
    error::{ApiErrorEnvelope, error_response},
    state::ProgressRuntimeState,
};

const REPLAY_PAGE_SIZE: usize = 256;
const KEEP_ALIVE_SECONDS: u64 = 15;

pub(crate) async fn job_progress_sse(
    State(app_state): State<AppState>,
    Path(raw_job_id): Path<String>,
    headers: HeaderMap,
    Extension(trace_id): Extension<TraceId>,
) -> Response {
    let Some(runtime) = app_state.progress_runtime() else {
        return error_response(
            StatusCode::NOT_IMPLEMENTED,
            ApiErrorEnvelope::new(
                "ROUTE_NOT_AVAILABLE",
                "Durable progress streaming is not attached to this runtime.",
                trace_id.as_str(),
            ),
        );
    };

    let Ok(job_id) = raw_job_id.parse::<JobId>() else {
        return error_response(
            StatusCode::BAD_REQUEST,
            ApiErrorEnvelope::new(
                "INVALID_JOB_ID",
                "The progress stream job identifier is invalid.",
                trace_id.as_str(),
            ),
        );
    };
    let Ok(cursor) = parse_last_event_id(&headers) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            ApiErrorEnvelope::new(
                "INVALID_LAST_EVENT_ID",
                "Last-Event-ID must be a positive durable progress sequence.",
                trace_id.as_str(),
            ),
        );
    };

    // Subscribe first. An event committed while the durable replay query runs is
    // therefore either returned by replay, queued live, or both; sequence-based
    // de-duplication makes all three cases safe.
    let receiver = runtime.live_hub().subscribe();
    let Ok(request) = replay_request(cursor) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            ApiErrorEnvelope::new(
                "INVALID_LAST_EVENT_ID",
                "Last-Event-ID is outside the supported progress sequence range.",
                trace_id.as_str(),
            ),
        );
    };
    let initial = match ProgressService::new(runtime.database())
        .replay(&job_id, request)
        .await
    {
        Ok(page) => page,
        Err(error) => return initial_replay_error(&error, &trace_id),
    };
    let replay_more = initial.next_after.is_some();
    let state = ProgressStreamState {
        runtime,
        job_id,
        receiver,
        cursor,
        pending: initial.events.into_iter().collect(),
        replay_more,
        done: false,
    };

    Sse::new(progress_stream(state))
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(KEEP_ALIVE_SECONDS))
                .text("keep-alive"),
        )
        .into_response()
}

struct ProgressStreamState {
    runtime: std::sync::Arc<ProgressRuntimeState>,
    job_id: JobId,
    receiver: tokio::sync::broadcast::Receiver<ProgressEvent>,
    cursor: u64,
    pending: VecDeque<ProgressEvent>,
    replay_more: bool,
    done: bool,
}

impl ProgressStreamState {
    async fn refill_from_durable(&mut self) -> Result<(), ProgressServiceError> {
        let request = replay_request(self.cursor)?;
        let page = ProgressService::new(self.runtime.database())
            .replay(&self.job_id, request)
            .await?;
        self.replay_more = page.next_after.is_some();
        self.pending.extend(
            page.events
                .into_iter()
                .filter(|event| event.sequence.get() > self.cursor),
        );
        Ok(())
    }
}

fn progress_stream(
    state: ProgressStreamState,
) -> impl Stream<Item = Result<Event, Infallible>> + Send {
    stream::unfold(state, next_progress_item)
}

async fn next_progress_item(
    mut state: ProgressStreamState,
) -> Option<(Result<Event, Infallible>, ProgressStreamState)> {
    loop {
        if state.done {
            return None;
        }

        if let Some(event) = state.pending.pop_front() {
            if event.sequence.get() <= state.cursor {
                continue;
            }
            let terminal = event.terminal.is_some();
            state.cursor = event.sequence.get();
            let Ok(encoded) = encode_progress_event(&event) else {
                return None;
            };
            state.done = terminal;
            return Some((Ok(encoded), state));
        }

        if state.replay_more {
            if state.refill_from_durable().await.is_err() {
                return None;
            }
            continue;
        }

        match state.receiver.recv().await {
            Ok(event) => {
                if event.job_id != state.job_id || event.sequence.get() <= state.cursor {
                    continue;
                }
                let expected = state.cursor.checked_add(1);
                if expected == Some(event.sequence.get()) {
                    state.pending.push_back(event);
                    continue;
                }

                // A sequence jump means the bounded live buffer did not carry
                // every event for this job. Catch up from durable storage rather
                // than trusting the live candidate as complete history.
                let live_candidate = event;
                if state.refill_from_durable().await.is_err() {
                    return None;
                }
                if state.pending.is_empty() && live_candidate.sequence.get() > state.cursor {
                    state.pending.push_back(live_candidate);
                }
            }
            Err(RecvError::Lagged(_)) => {
                if state.refill_from_durable().await.is_err() {
                    return None;
                }
            }
            Err(RecvError::Closed) => return None,
        }
    }
}

fn parse_last_event_id(headers: &HeaderMap) -> Result<u64, ()> {
    let Some(value) = headers.get("last-event-id") else {
        return Ok(0);
    };
    let value = value.to_str().map_err(|_| ())?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(());
    }
    let cursor = value.parse::<u64>().map_err(|_| ())?;
    if cursor == 0 {
        return Err(());
    }
    Ok(cursor)
}

fn replay_request(cursor: u64) -> Result<ProgressReplayRequest, ProgressRepositoryError> {
    let after = if cursor == 0 {
        None
    } else {
        Some(ProgressSequence::new(cursor)?)
    };
    ProgressReplayRequest::new(after, REPLAY_PAGE_SIZE)
}

fn initial_replay_error(error: &ProgressServiceError, trace_id: &TraceId) -> Response {
    match error {
        ProgressServiceError::Repository(ProgressRepositoryError::JobNotFound) => error_response(
            StatusCode::NOT_FOUND,
            ApiErrorEnvelope::new(
                "JOB_NOT_FOUND",
                "The requested progress stream job does not exist.",
                trace_id.as_str(),
            ),
        ),
        ProgressServiceError::Repository(
            ProgressRepositoryError::InvalidReplayRequest
            | ProgressRepositoryError::InvalidProgressSequence,
        ) => error_response(
            StatusCode::BAD_REQUEST,
            ApiErrorEnvelope::new(
                "INVALID_LAST_EVENT_ID",
                "Last-Event-ID is outside the supported progress sequence range.",
                trace_id.as_str(),
            ),
        ),
        ProgressServiceError::Repository(_) => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            ApiErrorEnvelope::new(
                "PROGRESS_STREAM_UNAVAILABLE",
                "Durable progress could not be replayed safely.",
                trace_id.as_str(),
            ),
        ),
    }
}

fn encode_progress_event(event: &ProgressEvent) -> Result<Event, serde_json::Error> {
    let metadata = event
        .metadata
        .entries()
        .iter()
        .map(|(key, value)| {
            let value = match value {
                ProgressMetadataValue::Code(code) => {
                    ProgressMetadataPayload::Code(code.as_str().to_owned())
                }
                ProgressMetadataValue::Count(value) => ProgressMetadataPayload::Count(*value),
                ProgressMetadataValue::Flag(value) => ProgressMetadataPayload::Flag(*value),
            };
            (key.as_str().to_owned(), value)
        })
        .collect();
    let payload = ProgressSsePayload {
        event_id: event.id.as_str().to_owned(),
        job_id: event.job_id.as_str().to_owned(),
        attempt_id: event
            .attempt_id
            .as_ref()
            .map(|attempt| attempt.as_str().to_owned()),
        sequence: event.sequence.get(),
        key: event.key.as_str().to_owned(),
        metadata,
        terminal: event.terminal.map(terminal_label),
        created_at: event.created_at,
    };
    let data = serde_json::to_string(&payload)?;
    Ok(Event::default()
        .id(event.sequence.get().to_string())
        .event("progress")
        .data(data))
}

const fn terminal_label(terminal: ProgressTerminalState) -> &'static str {
    match terminal {
        ProgressTerminalState::Succeeded => "SUCCEEDED",
        ProgressTerminalState::Failed => "FAILED",
        ProgressTerminalState::Cancelled => "CANCELLED",
    }
}

#[derive(Serialize)]
struct ProgressSsePayload {
    event_id: String,
    job_id: String,
    attempt_id: Option<String>,
    sequence: u64,
    key: String,
    metadata: BTreeMap<String, ProgressMetadataPayload>,
    terminal: Option<&'static str>,
    created_at: i64,
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "SCREAMING_SNAKE_CASE")]
enum ProgressMetadataPayload {
    Code(String),
    Count(u32),
    Flag(bool),
}
