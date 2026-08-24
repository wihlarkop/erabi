use std::net::SocketAddr;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use erabi_api::{AppState, SecurityConfig, build_router};
use erabi_db::{
    ErabiDatabase, MigrationRunner,
    repositories::{JobId, JobKind, JobRepository, NewJob},
};
use erabi_jobs::{
    NewProgressEvent, ProgressKey, ProgressLiveHub, ProgressMetadata, ProgressService,
    ProgressTerminalState,
};
use secrecy::SecretString;
use tower::ServiceExt;

const LOOPBACK: &str = "127.0.0.1:7878";
const REMOTE: &str = "192.0.2.10:7878";
const TOKEN: &str = "progress-test-bearer-token";
const BODY_LIMIT: usize = 1024 * 1024;

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

async fn fixture(
    hub: ProgressLiveHub,
) -> Result<(ErabiDatabase, JobId, Router, ProgressLiveHub), Box<dyn std::error::Error>> {
    let database = database().await?;
    let jobs = JobRepository::new(&database);
    let job = NewJob::new(JobKind::new("TEST_WORK")?, 0, 0, 1)?;
    let job_id = job.id.clone();
    jobs.enqueue(&job, 0).await?;
    let address: SocketAddr = LOOPBACK.parse()?;
    let state = AppState::ready().with_progress_runtime(database.clone(), hub.clone());
    let router = build_router(state, SecurityConfig::loopback(address)?);
    Ok((database, job_id, router, hub))
}

fn progress(job_id: JobId, key: &str) -> Result<NewProgressEvent, Box<dyn std::error::Error>> {
    Ok(NewProgressEvent::new(
        job_id,
        ProgressKey::new(key)?,
        ProgressMetadata::default(),
    ))
}

async fn append_terminal(
    database: &ErabiDatabase,
    hub: &ProgressLiveHub,
    job_id: JobId,
    created_at: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = NewProgressEvent::terminal(
        job_id,
        ProgressTerminalState::Succeeded,
        ProgressMetadata::default(),
    )?;
    ProgressService::new(database)
        .append_and_publish_at(hub, &event, created_at)
        .await?;
    Ok(())
}

fn request(path: &str) -> axum::http::request::Builder {
    Request::builder().method("GET").uri(path)
}

async fn error_code(
    response: axum::response::Response,
) -> Result<String, Box<dyn std::error::Error>> {
    let body = to_bytes(response.into_body(), BODY_LIMIT).await?;
    let value: serde_json::Value = serde_json::from_slice(&body)?;
    Ok(value["code"].as_str().unwrap_or_default().to_owned())
}

fn event_ids(body: &[u8]) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let text = std::str::from_utf8(body)?;
    text.lines()
        .filter_map(|line| line.strip_prefix("id:"))
        .map(|value| Ok(value.trim().parse::<u64>()?))
        .collect()
}

#[tokio::test]
async fn reconnect_replays_strictly_after_last_event_id_and_closes_on_terminal()
-> Result<(), Box<dyn std::error::Error>> {
    let hub = ProgressLiveHub::new();
    let (database, job_id, router, hub) = fixture(hub).await?;
    let service = ProgressService::new(&database);
    service
        .append_and_publish_at(&hub, &progress(job_id.clone(), "LOADING")?, 1)
        .await?;
    service
        .append_and_publish_at(&hub, &progress(job_id.clone(), "SAVING")?, 2)
        .await?;
    append_terminal(&database, &hub, job_id.clone(), 3).await?;

    let response = router
        .oneshot(
            request(&format!("/api/v1/events/jobs/{job_id}/progress"))
                .header("last-event-id", "1")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE),
        Some(&axum::http::HeaderValue::from_static("text/event-stream"))
    );
    let body = to_bytes(response.into_body(), BODY_LIMIT).await?;
    assert_eq!(event_ids(&body)?, vec![2, 3]);
    Ok(())
}

#[tokio::test]
async fn lagged_live_receiver_catches_up_durably_without_gaps_or_duplicates()
-> Result<(), Box<dyn std::error::Error>> {
    let hub = ProgressLiveHub::with_capacity(1)?;
    let (database, job_id, router, hub) = fixture(hub).await?;
    let service = ProgressService::new(&database);
    service
        .append_and_publish_at(&hub, &progress(job_id.clone(), "LOADING")?, 1)
        .await?;

    let response = router
        .oneshot(
            request(&format!("/api/v1/events/jobs/{job_id}/progress"))
                .header("last-event-id", "1")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(hub.subscriber_count(), 1);

    service
        .append_and_publish_at(&hub, &progress(job_id.clone(), "SAVING")?, 2)
        .await?;
    append_terminal(&database, &hub, job_id, 3).await?;

    let body = to_bytes(response.into_body(), BODY_LIMIT).await?;
    assert_eq!(event_ids(&body)?, vec![2, 3]);
    Ok(())
}

#[tokio::test]
async fn invalid_cursor_and_unknown_job_return_stable_errors()
-> Result<(), Box<dyn std::error::Error>> {
    let hub = ProgressLiveHub::new();
    let (_database, job_id, router, _hub) = fixture(hub).await?;
    let invalid = router
        .clone()
        .oneshot(
            request(&format!("/api/v1/events/jobs/{job_id}/progress"))
                .header("last-event-id", "not-a-sequence")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error_code(invalid).await?, "INVALID_LAST_EVENT_ID");

    let unknown = JobId::new();
    let missing = router
        .oneshot(request(&format!("/api/v1/events/jobs/{unknown}/progress")).body(Body::empty())?)
        .await?;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(error_code(missing).await?, "JOB_NOT_FOUND");
    Ok(())
}

#[tokio::test]
async fn malformed_job_id_is_rejected_before_streaming() -> Result<(), Box<dyn std::error::Error>> {
    let hub = ProgressLiveHub::new();
    let (_database, _job_id, router, _hub) = fixture(hub).await?;
    let response = router
        .oneshot(request("/api/v1/events/jobs/not-a-job-id/progress").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error_code(response).await?, "INVALID_JOB_ID");
    Ok(())
}

#[tokio::test]
async fn remote_progress_stream_inherits_bearer_authentication()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let jobs = JobRepository::new(&database);
    let job = NewJob::new(JobKind::new("TEST_WORK")?, 0, 0, 1)?;
    let job_id = job.id.clone();
    jobs.enqueue(&job, 0).await?;
    let hub = ProgressLiveHub::new();
    let state = AppState::ready().with_progress_runtime(database, hub);
    let address: SocketAddr = REMOTE.parse()?;
    let security = SecurityConfig::remote(address, SecretString::from(TOKEN), Vec::new())?;
    let router = build_router(state, security);
    let path = format!("/api/v1/events/jobs/{job_id}/progress");

    let denied = router
        .clone()
        .oneshot(request(&path).body(Body::empty())?)
        .await?;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(error_code(denied).await?, "AUTHENTICATION_REQUIRED");

    let allowed = router
        .oneshot(
            request(&path)
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::HOST, REMOTE)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(allowed.status(), StatusCode::OK);
    assert_eq!(
        allowed.headers().get(header::CONTENT_TYPE),
        Some(&axum::http::HeaderValue::from_static("text/event-stream"))
    );
    Ok(())
}
