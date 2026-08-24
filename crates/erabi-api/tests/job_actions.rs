use std::net::SocketAddr;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use erabi_api::{AppState, SecurityConfig, build_router};
use erabi_db::repositories::{JobKind, JobRepository, NewJob};
use erabi_db::{ErabiDatabase, MigrationRunner};
use erabi_jobs::CancellationController;
use secrecy::SecretString;
use tower::ServiceExt;

const TOKEN: &str = "task-4-test-token";

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

async fn job(database: &ErabiDatabase) -> Result<NewJob, Box<dyn std::error::Error>> {
    let job = NewJob::new(JobKind::new("TEST_WORK")?, 0, 0, 2)?;
    JobRepository::new(database).enqueue(&job, 0).await?;
    Ok(job)
}

fn loopback(database: &ErabiDatabase) -> Result<Router, Box<dyn std::error::Error>> {
    let address: SocketAddr = "127.0.0.1:7878".parse()?;
    Ok(build_router(
        AppState::ready()
            .with_job_actions_runtime(database.clone(), CancellationController::default()),
        SecurityConfig::loopback(address)?,
    ))
}

fn remote(database: &ErabiDatabase) -> Result<Router, Box<dyn std::error::Error>> {
    let address: SocketAddr = "192.0.2.10:7878".parse()?;
    Ok(build_router(
        AppState::ready()
            .with_job_actions_runtime(database.clone(), CancellationController::default()),
        SecurityConfig::remote(address, SecretString::from(TOKEN), Vec::new())?,
    ))
}

fn request(path: &str) -> axum::http::request::Builder {
    Request::builder()
        .method("POST")
        .uri(path)
        .header(header::HOST, "127.0.0.1:7878")
        .header(header::CONTENT_TYPE, "application/json")
}

async fn body_json(
    response: axum::response::Response,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX).await?,
    )?)
}

#[tokio::test]
async fn queued_cancel_action_is_exposed_through_the_protected_api()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let job = job(&database).await?;
    let response = loopback(&database)?
        .oneshot(request(&format!("/api/v1/jobs/{}/cancel", job.id)).body(Body::from("{}"))?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let value = body_json(response).await?;
    assert_eq!(value["action"], "CANCEL");
    assert_eq!(value["state"], "CANCELLED");
    assert_eq!(
        JobRepository::new(&database).job(&job.id).await?.state,
        erabi_db::repositories::JobState::Cancelled
    );
    Ok(())
}

#[tokio::test]
async fn action_api_returns_stable_lifecycle_errors() -> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let job = job(&database).await?;
    let response = loopback(&database)?
        .oneshot(request(&format!("/api/v1/jobs/{}/retry", job.id)).body(Body::from("{}"))?)
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let value = body_json(response).await?;
    assert_eq!(value["code"], "ILLEGAL_LIFECYCLE_STATE");
    assert!(value.get("checkpoint").is_none());
    Ok(())
}

#[tokio::test]
async fn remote_task_4_actions_inherit_bearer_authentication()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let job = job(&database).await?;
    let response = remote(&database)?
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/jobs/{}/cancel", job.id))
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let value = body_json(response).await?;
    assert_eq!(value["code"], "AUTHENTICATION_REQUIRED");
    Ok(())
}

#[tokio::test]
async fn openapi_lists_task_4_action_routes() -> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let address: SocketAddr = "127.0.0.1:7878".parse()?;
    let router = build_router(
        AppState::ready().with_job_actions_runtime(database, CancellationController::default()),
        SecurityConfig::loopback(address)?,
    );
    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v1/openapi.json")
                .body(Body::empty())?,
        )
        .await?;
    let value = body_json(response).await?;
    assert_eq!(
        value["paths"]["/api/v1/jobs/{job_id}/resume"]["post"]["summary"],
        "Resume compatible checkpoint"
    );
    assert_eq!(
        value["paths"]["/api/v1/jobs/{job_id}"]["delete"]["summary"],
        "Remove safe never-started job"
    );
    Ok(())
}
