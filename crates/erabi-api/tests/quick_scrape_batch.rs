use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
    response::Response,
};
use erabi_api::{AppState, SecurityConfig, build_router};
use erabi_crawler::{
    ContentProbeDecision, ContentProbeExecutor, NetworkTargetPolicy, QuickScrapeSubmissionService,
    StaticNetworkResolver, ValidatedNetworkTarget,
};
use erabi_db::{
    ErabiDatabase, MigrationRunner,
    repositories::{
        CrawlExecutionRepository, CrawlRunRepository, JobRepository, JobState, SourceRepository,
    },
};
use erabi_domain::{CrawlRunId, CrawlRunType, RobotsDecision, SourceId};
use serde_json::Value;
use tempfile::TempDir;
use tower::ServiceExt;
use turso::Connection;
use uuid::Uuid;

const QUICK_SCRAPE_BATCH_MAX_ITEMS: usize = 8;
const QUICK_SCRAPE_BATCH_BODY_LIMIT_BYTES: usize = 48 * 1024;
const BATCH_PATH: &str = "/api/v1/quick-scrapes/batch";

#[derive(Clone)]
struct CountingProbe {
    calls: Arc<AtomicUsize>,
}

impl ContentProbeExecutor for CountingProbe {
    fn probe<'probe>(
        &'probe self,
        _target: &'probe ValidatedNetworkTarget,
    ) -> erabi_crawler::ContentProbeFuture<'probe> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { ContentProbeDecision::NormalWebCrawl })
    }
}

struct TestRuntime {
    router: Router,
    database: ErabiDatabase,
    probe_calls: Arc<AtomicUsize>,
}

async fn runtime() -> Result<TestRuntime, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    runtime_with_database(database)
}

async fn file_backed_database() -> Result<(TempDir, ErabiDatabase), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = ErabiDatabase::open_local(directory.path().join("erabi.db")).await?;
    MigrationRunner::default().apply(&database).await?;
    Ok((directory, database))
}

async fn file_backed_runtime() -> Result<(TempDir, TestRuntime), Box<dyn std::error::Error>> {
    let (directory, database) = file_backed_database().await?;
    let runtime = runtime_with_database(database)?;
    Ok((directory, runtime))
}

fn runtime_with_database(
    database: ErabiDatabase,
) -> Result<TestRuntime, Box<dyn std::error::Error>> {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)), 443);
    let policy = NetworkTargetPolicy::new(Arc::new(StaticNetworkResolver::single(
        "example.test",
        address,
    )));
    let probe_calls = Arc::new(AtomicUsize::new(0));
    let service = QuickScrapeSubmissionService::new(database.clone(), policy).with_probe_executor(
        Arc::new(CountingProbe {
            calls: Arc::clone(&probe_calls),
        }),
    );
    let state = AppState::ready().with_quick_scrape_runtime(service);
    Ok(TestRuntime {
        router: build_router(state, SecurityConfig::loopback("127.0.0.1:7878".parse()?)?),
        database,
        probe_calls,
    })
}

async fn raw_connection(directory: &TempDir) -> Result<Connection, Box<dyn std::error::Error>> {
    let path = directory.path().join("erabi.db");
    let database = turso::Builder::new_local(path.to_string_lossy().as_ref())
        .build()
        .await?;
    let connection = database.connect()?;
    connection.pragma_update("foreign_keys", "ON").await?;
    Ok(connection)
}

async fn durable_counts(
    directory: &TempDir,
) -> Result<(i64, i64, i64), Box<dyn std::error::Error>> {
    let connection = raw_connection(directory).await?;
    let runs = connection
        .prepare("SELECT COUNT(*) FROM crawl_runs")
        .await?
        .query_row(())
        .await?;
    let jobs = connection
        .prepare("SELECT COUNT(*) FROM jobs")
        .await?
        .query_row(())
        .await?;
    let sources = connection
        .prepare("SELECT COUNT(*) FROM sources")
        .await?
        .query_row(())
        .await?;
    Ok((runs.get(0)?, jobs.get(0)?, sources.get(0)?))
}

async fn insert_corrupt_source_for(
    directory: &TempDir,
    canonical_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let connection = raw_connection(directory).await?;
    connection
        .execute(
            "INSERT INTO sources (id, collection_id, name, original_url, canonical_url, target_type, status) VALUES (?1, NULL, ?2, ?3, ?4, 'WEB_PAGE', 'ACTIVE')",
            (
                SourceId::new().to_string(),
                "Corrupt source",
                "https://example.test/different",
                canonical_url,
            ),
        )
        .await?;
    Ok(())
}

fn request(path: &str, body: impl Into<Body>) -> Result<Request<Body>, axum::http::Error> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header(header::HOST, "127.0.0.1:7878")
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.into())
}

async fn json_body(response: Response) -> Result<Value, Box<dyn std::error::Error>> {
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    Ok(serde_json::from_slice(&body)?)
}

fn run_id(item: &Value) -> Result<CrawlRunId, Box<dyn std::error::Error>> {
    let value = item["run_id"]
        .as_str()
        .ok_or("accepted outcome did not include a run_id")?;
    CrawlRunId::from_uuid(Uuid::parse_str(value)?)
        .ok_or_else(|| "accepted outcome had a non-UUIDv7 run_id".into())
}

fn source_id(item: &Value) -> Result<SourceId, Box<dyn std::error::Error>> {
    let value = item["source_id"]
        .as_str()
        .ok_or("accepted outcome did not include a source_id")?;
    SourceId::from_uuid(Uuid::parse_str(value)?)
        .ok_or_else(|| "accepted outcome had a non-UUIDv7 source_id".into())
}

#[tokio::test]
async fn ordered_mixed_outcomes_create_independent_queued_quick_scrapes_without_provider_execution()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = runtime().await?;
    let response = runtime
        .router
        .clone()
        .oneshot(request(
            BATCH_PATH,
            Body::from(
                r#"{"items":[{"target_url":"https://example.test/first"},{"target_url":"not a URL"},{"target_url":"https://example.test/third"}]}"#,
            ),
        )?)
        .await?;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = json_body(response).await?;
    assert_eq!(body["halted"], false);
    let items = body["items"].as_array().ok_or("missing batch items")?;
    assert_eq!(items.len(), 3);
    assert_eq!(items[0]["status"], "ACCEPTED");
    assert_eq!(items[1]["status"], "VALIDATION_ERROR");
    assert_eq!(items[1]["code"], "INVALID_QUICK_SCRAPE_REQUEST");
    assert_eq!(items[2]["status"], "ACCEPTED");
    assert!(items[1].get("run_id").is_none());
    assert!(items[1].get("job_id").is_none());
    assert!(items[1].get("source_id").is_none());

    let first_run_id = run_id(&items[0])?;
    let third_run_id = run_id(&items[2])?;
    assert_ne!(first_run_id, third_run_id);
    assert_ne!(items[0]["job_id"], items[2]["job_id"]);
    let runs = CrawlRunRepository::new(&runtime.database);
    let jobs = JobRepository::new(&runtime.database);
    let executions = CrawlExecutionRepository::new(&runtime.database);
    for (run_id, item) in [(first_run_id, &items[0]), (third_run_id, &items[2])] {
        let snapshot = runs.snapshot(run_id).await?;
        let job_id = item["job_id"].as_str().ok_or("missing job_id")?.parse()?;
        let job = jobs.job(&job_id).await?;

        assert_eq!(snapshot.run_type(), CrawlRunType::QuickScrape);
        assert_eq!(job.kind.as_str(), "QUICK_SCRAPE");
        assert_eq!(job.state, JobState::Queued);
        assert_eq!(
            job.crawl_run_id.as_deref(),
            Some(run_id.to_string().as_str())
        );
        assert!(executions.list_for_run(run_id).await?.is_empty());
    }
    assert_eq!(runtime.probe_calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn duplicate_equivalent_urls_preserve_positions_and_reuse_only_the_source_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let (directory, runtime) = file_backed_runtime().await?;
    let response = runtime
        .router
        .clone()
        .oneshot(request(
            BATCH_PATH,
            Body::from(
                r#"{"items":[{"target_url":"https://example.test/duplicate?utm_source=first"},{"target_url":"https://example.test/duplicate"}]}"#,
            ),
        )?)
        .await?;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = json_body(response).await?;
    let items = body["items"].as_array().ok_or("missing batch items")?;
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["status"], "ACCEPTED");
    assert_eq!(items[1]["status"], "ACCEPTED");
    assert_eq!(items[0]["source_id"], items[1]["source_id"]);
    assert_ne!(items[0]["run_id"], items[1]["run_id"]);
    assert_ne!(items[0]["job_id"], items[1]["job_id"]);
    let first_run_id = run_id(&items[0])?;
    let second_run_id = run_id(&items[1])?;
    let first_source_id = source_id(&items[0])?;
    let second_source_id = source_id(&items[1])?;
    assert_ne!(first_run_id, second_run_id);
    assert_eq!(first_source_id, second_source_id);

    let runs = CrawlRunRepository::new(&runtime.database);
    let jobs = JobRepository::new(&runtime.database);
    for (run_id, item) in [(first_run_id, &items[0]), (second_run_id, &items[1])] {
        let snapshot = runs.snapshot(run_id).await?;
        let job_id = item["job_id"].as_str().ok_or("missing job_id")?.parse()?;
        let job = jobs.job(&job_id).await?;
        assert_eq!(snapshot.run_type(), CrawlRunType::QuickScrape);
        assert_eq!(job.kind.as_str(), "QUICK_SCRAPE");
        assert_eq!(
            job.crawl_run_id.as_deref(),
            Some(run_id.to_string().as_str())
        );
    }
    let source = SourceRepository::new(&runtime.database)
        .read(first_source_id)
        .await?;
    assert_eq!(source.id, second_source_id);
    assert_eq!(durable_counts(&directory).await?, (2, 2, 1));
    assert_eq!(runtime.probe_calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn batch_item_bound_is_validated_before_any_submission_and_empty_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let accepted_runtime = runtime().await?;
    let accepted_items = (0..QUICK_SCRAPE_BATCH_MAX_ITEMS)
        .map(|index| format!(r#"{{"target_url":"https://example.test/{index}"}}"#))
        .collect::<Vec<_>>()
        .join(",");
    let response = accepted_runtime
        .router
        .clone()
        .oneshot(request(
            BATCH_PATH,
            Body::from(format!(r#"{{"items":[{accepted_items}]}}"#)),
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = json_body(response).await?;
    assert_eq!(body["halted"], false);
    let items = body["items"].as_array().ok_or("missing batch items")?;
    assert_eq!(items.len(), QUICK_SCRAPE_BATCH_MAX_ITEMS);
    assert!(items.iter().all(|item| item["status"] == "ACCEPTED"));
    assert_eq!(
        accepted_runtime.probe_calls.load(Ordering::SeqCst),
        QUICK_SCRAPE_BATCH_MAX_ITEMS
    );

    let (too_many_directory, too_many_runtime) = file_backed_runtime().await?;
    let too_many_items = (0..=QUICK_SCRAPE_BATCH_MAX_ITEMS)
        .map(|index| format!(r#"{{"target_url":"https://example.test/{index}"}}"#))
        .collect::<Vec<_>>()
        .join(",");
    let too_many = too_many_runtime
        .router
        .clone()
        .oneshot(request(
            BATCH_PATH,
            Body::from(format!(r#"{{"items":[{too_many_items}]}}"#)),
        )?)
        .await?;
    assert_eq!(too_many.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(too_many).await?["code"],
        "TOO_MANY_QUICK_SCRAPE_ITEMS"
    );
    assert_eq!(too_many_runtime.probe_calls.load(Ordering::SeqCst), 0);
    assert_eq!(durable_counts(&too_many_directory).await?, (0, 0, 0));

    let (empty_directory, empty_runtime) = file_backed_runtime().await?;
    let empty = empty_runtime
        .router
        .clone()
        .oneshot(request(BATCH_PATH, Body::from(r#"{"items":[]}"#))?)
        .await?;
    assert_eq!(empty.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_body(empty).await?["code"], "EMPTY_QUICK_SCRAPE_BATCH");
    assert_eq!(empty_runtime.probe_calls.load(Ordering::SeqCst), 0);
    assert_eq!(durable_counts(&empty_directory).await?, (0, 0, 0));
    Ok(())
}

#[tokio::test]
async fn robots_override_reasons_remain_item_local_and_invalid_reason_does_not_rollback_siblings()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = runtime().await?;
    let response = runtime
        .router
        .clone()
        .oneshot(request(
            BATCH_PATH,
            Body::from(
                r#"{"items":[{"target_url":"https://example.test/override","robots_override":{"reason":"operator approved item A"}},{"target_url":"https://example.test/respect"},{"target_url":"https://example.test/invalid-override","robots_override":{"reason":""}}]}"#,
            ),
        )?)
        .await?;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = json_body(response).await?;
    assert_eq!(body["halted"], false);
    let items = body["items"].as_array().ok_or("missing batch items")?;
    assert_eq!(items[0]["status"], "ACCEPTED");
    assert_eq!(items[1]["status"], "ACCEPTED");
    assert_eq!(items[2]["status"], "VALIDATION_ERROR");
    assert_eq!(items[2]["code"], "INVALID_ROBOTS_OVERRIDE");

    let runs = CrawlRunRepository::new(&runtime.database);
    let override_snapshot = runs.snapshot(run_id(&items[0])?).await?;
    let respect_snapshot = runs.snapshot(run_id(&items[1])?).await?;
    assert!(matches!(
        override_snapshot.robots().decision(),
        RobotsDecision::Override { reason } if reason == "operator approved item A"
    ));
    assert!(matches!(
        respect_snapshot.robots().decision(),
        RobotsDecision::Respect
    ));
    assert_eq!(runtime.probe_calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn systemic_submission_failure_halts_without_admitting_later_items()
-> Result<(), Box<dyn std::error::Error>> {
    let (directory, database) = file_backed_database().await?;
    insert_corrupt_source_for(&directory, "https://example.test/system-failure").await?;
    let runtime = runtime_with_database(database)?;
    let response = runtime
        .router
        .clone()
        .oneshot(request(
            BATCH_PATH,
            Body::from(
                r#"{"items":[{"target_url":"https://example.test/accepted"},{"target_url":"https://example.test/system-failure"},{"target_url":"https://example.test/not-processed-c"},{"target_url":"https://example.test/not-processed-d"}]}"#,
            ),
        )?)
        .await?;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = json_body(response).await?;
    assert_eq!(body["halted"], true);
    let items = body["items"].as_array().ok_or("missing batch items")?;
    assert_eq!(items.len(), 4);
    assert_eq!(items[0]["status"], "ACCEPTED");
    assert_eq!(items[1]["status"], "SYSTEM_ERROR");
    assert_eq!(items[1]["code"], "QUICK_SCRAPE_SUBMISSION_FAILED");
    assert!(items[1]["trace_id"].as_str().is_some());
    for item in &items[2..] {
        assert_eq!(item["status"], "NOT_PROCESSED");
        assert_eq!(item["code"], "BATCH_HALTED");
        assert!(item.get("run_id").is_none());
        assert!(item.get("job_id").is_none());
        assert!(item.get("source_id").is_none());
    }

    let accepted_run_id = run_id(&items[0])?;
    let accepted_job_id = items[0]["job_id"]
        .as_str()
        .ok_or("accepted outcome did not include a job_id")?
        .parse()?;
    let runs = CrawlRunRepository::new(&runtime.database);
    let jobs = JobRepository::new(&runtime.database);
    let snapshot = runs.snapshot(accepted_run_id).await?;
    let job = jobs.job(&accepted_job_id).await?;
    assert_eq!(snapshot.run_type(), CrawlRunType::QuickScrape);
    assert_eq!(job.kind.as_str(), "QUICK_SCRAPE");
    assert_eq!(
        job.crawl_run_id.as_deref(),
        Some(accepted_run_id.to_string().as_str())
    );
    assert!(items[1].get("run_id").is_none());
    assert!(items[1].get("job_id").is_none());
    assert!(items[1].get("source_id").is_none());
    assert_eq!(durable_counts(&directory).await?, (1, 1, 2));
    assert_eq!(runtime.probe_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn batch_route_rejects_unknown_fields_oversized_bodies_and_invalid_mutation_requests()
-> Result<(), Box<dyn std::error::Error>> {
    let (directory, runtime) = file_backed_runtime().await?;
    for body in [
        r#"{"items":[{"target_url":"https://example.test/page"}],"unknown":true}"#,
        r#"{"items":[{"target_url":"https://example.test/page","unknown":true}]}"#,
    ] {
        let response = runtime
            .router
            .clone()
            .oneshot(request(BATCH_PATH, Body::from(body))?)
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            json_body(response).await?["code"],
            "INVALID_QUICK_SCRAPE_BATCH_REQUEST"
        );
    }

    let oversized = format!(
        r#"{{"items":[{{"target_url":"https://example.test/large","padding":"{}"}}]}}"#,
        "x".repeat(QUICK_SCRAPE_BATCH_BODY_LIMIT_BYTES)
    );
    let oversized_response = runtime
        .router
        .clone()
        .oneshot(request(BATCH_PATH, Body::from(oversized))?)
        .await?;
    assert_eq!(oversized_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        json_body(oversized_response).await?["code"],
        "BODY_TOO_LARGE"
    );

    let host_rejected = runtime
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(BATCH_PATH)
                .header(header::HOST, "attacker.example.test")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"items":[{"target_url":"https://example.test/page"}]}"#,
                ))?,
        )
        .await?;
    assert_eq!(host_rejected.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_body(host_rejected).await?["code"], "HOST_NOT_ALLOWED");

    let origin_rejected = runtime
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(BATCH_PATH)
                .header(header::HOST, "127.0.0.1:7878")
                .header(header::ORIGIN, "https://attacker.example.test")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"items":[{"target_url":"https://example.test/page"}]}"#,
                ))?,
        )
        .await?;
    assert_eq!(origin_rejected.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        json_body(origin_rejected).await?["code"],
        "ORIGIN_NOT_ALLOWED"
    );

    let content_type_rejected = runtime
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(BATCH_PATH)
                .header(header::HOST, "127.0.0.1:7878")
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from(
                    r#"{"items":[{"target_url":"https://example.test/page"}]}"#,
                ))?,
        )
        .await?;
    assert_eq!(
        content_type_rejected.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    assert_eq!(
        json_body(content_type_rejected).await?["code"],
        "CONTENT_TYPE_NOT_ALLOWED"
    );
    assert_eq!(runtime.probe_calls.load(Ordering::SeqCst), 0);
    assert_eq!(durable_counts(&directory).await?, (0, 0, 0));
    Ok(())
}
