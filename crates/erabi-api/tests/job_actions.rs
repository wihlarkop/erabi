use std::{collections::BTreeMap, net::SocketAddr};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use erabi_api::{AppState, SecurityConfig, build_router};
use erabi_db::repositories::{CrawlRunRepository, JobKind, JobRepository, NewJob};
use erabi_db::{ErabiDatabase, MigrationRunner};
use erabi_domain::{
    CrawlRunId, CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunStatus, CrawlRunType,
    ResolvedValue, RobotsAudit, RunConfiguration, SettingSource, SnapshotOperationalSettings,
};
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

fn resolved<T>(value: T) -> ResolvedValue<T> {
    ResolvedValue {
        value,
        source: SettingSource::BuiltInDefault,
    }
}

fn quick_scrape_snapshot() -> Result<CrawlRunSnapshot, Box<dyn std::error::Error>> {
    Ok(CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
        run_type: CrawlRunType::QuickScrape,
        configuration: RunConfiguration::QuickScrape {
            target_url: "https://example.test/item".parse()?,
            ad_hoc_configuration: BTreeMap::new(),
        },
        selected_seed_ids: Vec::new(),
        run_profile_id: None,
        settings: SnapshotOperationalSettings {
            max_pages: resolved(1),
            max_depth: resolved(0),
            max_duration_seconds: resolved(30),
            concurrency: resolved(1),
            request_delay_ms: resolved(0),
            timeout_ms: resolved(1_000),
            screenshot: resolved(false),
            asset_download_limit_bytes: resolved(0),
            retain_artifacts: resolved(false),
            user_agent: resolved("Erabi/0.1".to_owned()),
        },
        robots: RobotsAudit::respect(
            "api-test",
            "2026-08-23T00:00:00Z",
            "https://example.test",
            "Erabi/0.1",
            None,
        ),
        actor: "api-test".to_owned(),
        created_at: "2026-08-23T00:00:00Z".to_owned(),
    })?)
}

async fn cancelled_crawl_job(
    database: &ErabiDatabase,
) -> Result<NewJob, Box<dyn std::error::Error>> {
    let run_id = CrawlRunId::new();
    CrawlRunRepository::new(database)
        .create(run_id, CrawlRunStatus::Queued, &quick_scrape_snapshot()?)
        .await?;
    let mut job = NewJob::new(JobKind::new("QUICK_SCRAPE")?, 0, 0, 2)?;
    job.crawl_run_id = Some(run_id.to_string());
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
async fn resume_without_checkpoint_returns_a_bounded_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let job = cancelled_crawl_job(&database).await?;
    let router = loopback(&database)?;
    let cancelled = router
        .clone()
        .oneshot(request(&format!("/api/v1/jobs/{}/cancel", job.id)).body(Body::from("{}"))?)
        .await?;
    assert_eq!(cancelled.status(), StatusCode::OK);

    let response = router
        .oneshot(request(&format!("/api/v1/jobs/{}/resume", job.id)).body(Body::from("{}"))?)
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let value = body_json(response).await?;
    assert_eq!(value["code"], "CHECKPOINT_MISSING");
    assert_eq!(
        value["message"],
        "The requested recovery action cannot proceed because no durable checkpoint is available."
    );
    assert!(value.get("details").is_none());
    assert!(value.get("recoverability").is_none());
    assert!(value.get("checkpoint").is_none());
    assert!(value.get("payload").is_none());
    assert!(!value.to_string().contains("checkpoint_json"));
    Ok(())
}

#[tokio::test]
async fn rerun_action_requires_a_durable_crawl_run() -> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let job = job(&database).await?;
    let router = loopback(&database)?;
    let cancelled = router
        .clone()
        .oneshot(request(&format!("/api/v1/jobs/{}/cancel", job.id)).body(Body::from("{}"))?)
        .await?;
    assert_eq!(cancelled.status(), StatusCode::OK);
    let response = router
        .oneshot(
            request(&format!("/api/v1/jobs/{}/rerun-full-crawl", job.id)).body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let value = body_json(response).await?;
    assert_eq!(value["code"], "CRAWL_RUN_REQUIRED");
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
