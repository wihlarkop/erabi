use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use erabi_api::{AppState, SecurityConfig, build_router};
use erabi_crawler::{
    ContentProbeDecision, ContentProbeExecutor, NetworkTargetPolicy, QuickScrapeSubmissionService,
    StaticNetworkResolver, ValidatedNetworkTarget,
};
use erabi_db::{ErabiDatabase, MigrationRunner};
use tower::ServiceExt;

#[derive(Clone)]
struct FixedProbe;

impl ContentProbeExecutor for FixedProbe {
    fn probe<'probe>(
        &'probe self,
        _target: &'probe ValidatedNetworkTarget,
    ) -> erabi_crawler::ContentProbeFuture<'probe> {
        Box::pin(async { ContentProbeDecision::NormalWebCrawl })
    }
}

async fn router() -> Result<Router, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)), 443);
    let policy = NetworkTargetPolicy::new(Arc::new(StaticNetworkResolver::single(
        "example.test",
        address,
    )));
    let service = QuickScrapeSubmissionService::new(database, policy)
        .with_probe_executor(Arc::new(FixedProbe));
    let state = AppState::ready().with_quick_scrape_runtime(service);
    Ok(build_router(
        state,
        SecurityConfig::loopback("127.0.0.1:7878".parse()?)?,
    ))
}

fn start_request(body: &'static str) -> Result<Request<Body>, axum::http::Error> {
    Request::builder()
        .method("POST")
        .uri("/api/v1/quick-scrapes")
        .header(header::HOST, "127.0.0.1:7878")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
}

#[tokio::test]
async fn accepted_single_url_returns_only_erabi_durable_ids()
-> Result<(), Box<dyn std::error::Error>> {
    let response = router()
        .await?
        .oneshot(start_request(
            r#"{"target_url":"https://example.test/page"}"#,
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    let value: serde_json::Value = serde_json::from_slice(&body)?;
    assert!(value["run_id"].is_string());
    assert!(value["job_id"].is_string());
    assert!(value["source_id"].is_string());
    assert_eq!(value.as_object().map_or(0, serde_json::Map::len), 3);
    Ok(())
}

#[tokio::test]
async fn bare_robots_boolean_and_batch_urls_are_rejected_by_strict_contract()
-> Result<(), Box<dyn std::error::Error>> {
    for body in [
        r#"{"target_url":"https://example.test/page","robots_override":true}"#,
        r#"{"urls":["https://example.test/page"]}"#,
    ] {
        let response = router().await?.oneshot(start_request(body)?).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value["code"], "INVALID_QUICK_SCRAPE_REQUEST");
    }
    Ok(())
}
