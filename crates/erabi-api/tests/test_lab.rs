use std::net::SocketAddr;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use erabi_api::{AppState, SecurityConfig, build_router};
use erabi_db::{ErabiDatabase, MigrationRunner, repositories::CrawlerRepository};
use erabi_domain::{Crawler, Seed};
use secrecy::SecretString;
use tower::ServiceExt;

const TOKEN: &str = "test-lab-token";

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

fn request(method: &str, path: &str, body: &str) -> Result<Request<Body>, axum::http::Error> {
    Request::builder()
        .method(method)
        .uri(path)
        .header(header::HOST, "127.0.0.1:7878")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_owned()))
}

async fn json(
    response: axum::response::Response,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX).await?,
    )?)
}

fn loopback(database: &ErabiDatabase) -> Result<Router, Box<dyn std::error::Error>> {
    Ok(build_router(
        AppState::ready().with_test_lab_runtime(database.clone(), None, None),
        SecurityConfig::loopback("127.0.0.1:7878".parse::<SocketAddr>()?)?,
    ))
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_lab_executes_persists_and_reads_server_owned_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let crawler = Crawler::new("API Test Lab");
    let repository = CrawlerRepository::new(&database);
    repository.create(&crawler).await?;
    let version = repository
        .create_draft(crawler.id(), "operator", "unix:1")
        .await?;
    let mut version_to_publish = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    version_to_publish.add_seed(Seed::new(
        "https://example.test/".parse()?,
        "https://example.test/".parse()?,
    ))?;
    repository
        .save_draft(&version_to_publish, "operator", "unix:1b")
        .await?;
    let router = loopback(&database)?;
    let path = format!(
        "/api/v1/crawlers/{}/versions/{}/test-lab/tests",
        crawler.id(),
        version.id()
    );
    let response = router
        .clone()
        .oneshot(request("POST", &path, r#"{"test_type":"URL_CANONICALIZATION","input_urls":["HTTPS://EXAMPLE.TEST:443/items#top"]}"#)?)
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let evidence = json(response).await?;
    assert_eq!(evidence["test_kind"], "URL_CANONICALIZATION");
    assert_eq!(evidence["crawler_version_id"], version.id().to_string());
    assert_eq!(evidence["matches_current_configuration"], true);
    for field in [
        "extraction",
        "pagination",
        "discovery",
        "published_comparison",
    ] {
        assert!(evidence[field].is_null(), "{field} must serialize as null");
    }
    assert!(evidence["id"].as_str().is_some());
    assert!(evidence["executed_at"].as_str().is_some());
    assert!(evidence["config_hash"].as_str().is_some());
    let evidence_id = evidence["id"].as_str().ok_or("missing evidence id")?;

    let list_path = format!(
        "/api/v1/crawlers/{}/versions/{}/test-evidence",
        crawler.id(),
        version.id()
    );
    let listed = router
        .clone()
        .oneshot(request("GET", &list_path, "")?)
        .await?;
    assert_eq!(listed.status(), StatusCode::OK);
    assert_eq!(json(listed).await?.as_array().map(Vec::len), Some(1));
    let read_path = format!("{list_path}/{evidence_id}");
    let read = router
        .clone()
        .oneshot(request("GET", &read_path, "")?)
        .await?;
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(json(read).await?["id"], evidence_id);

    let openapi = router
        .oneshot(request("GET", "/api/v1/openapi.json", "")?)
        .await?;
    let openapi = json(openapi).await?;
    assert!(
        openapi["paths"]["/api/v1/crawlers/{crawler_id}/versions/{version_id}/test-lab/tests"]
            .is_object()
    );
    assert!(
        openapi["paths"]["/api/v1/crawlers/{crawler_id}/versions/{version_id}/test-evidence"]
            .is_object()
    );
    for schema in [
        "TestLabRequest",
        "TestEvidence",
        "TestKind",
        "CanonicalizationEvidence",
        "PageTypeMatchEvidence",
        "ExtractionObservation",
        "SelectorCoverageEvidence",
        "PaginationEvidence",
        "DiscoveryTransitionEvidence",
        "TestDiagnostic",
        "TestLabComparison",
    ] {
        assert!(
            openapi["components"]["schemas"][schema].is_object(),
            "missing {schema}"
        );
    }
    for (schema, field, reference) in [
        ("TestEvidence", "extraction", "ExtractionObservation"),
        ("TestEvidence", "pagination", "PaginationEvidence"),
        ("TestEvidence", "discovery", "DiscoveryTransitionEvidence"),
        ("TestEvidence", "published_comparison", "TestLabComparison"),
        (
            "DiscoveryTransitionEvidence",
            "source_match",
            "PageTypeMatchEvidence",
        ),
    ] {
        let property = &openapi["components"]["schemas"][schema]["properties"][field];
        assert_eq!(
            property["anyOf"][0]["$ref"],
            format!("#/components/schemas/{reference}"),
            "{schema}.{field} must retain its typed reference"
        );
        assert_eq!(
            property["anyOf"][1]["type"], "null",
            "{schema}.{field} must accept serde null"
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_lab_inherits_authentication_and_recovery_mutation_gating()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let crawler = Crawler::new("API Test Lab security");
    let repository = CrawlerRepository::new(&database);
    repository.create(&crawler).await?;
    let version = repository
        .create_draft(crawler.id(), "operator", "unix:1")
        .await?;
    let path = format!(
        "/api/v1/crawlers/{}/versions/{}/test-lab/tests",
        crawler.id(),
        version.id()
    );
    let remote = build_router(
        AppState::ready().with_test_lab_runtime(database.clone(), None, None),
        SecurityConfig::remote(
            "192.0.2.10:7878".parse()?,
            SecretString::from(TOKEN),
            Vec::new(),
        )?,
    );
    let unauthenticated = remote
        .clone()
        .oneshot(request(
            "POST",
            &path,
            r#"{"test_type":"URL_CANONICALIZATION","input_urls":["https://example.test"]}"#,
        )?)
        .await?;
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        json(unauthenticated).await?["code"],
        "AUTHENTICATION_REQUIRED"
    );
    let state = AppState::ready().with_test_lab_runtime(database, None, None);
    state.enter_recovery("RECOVERY_TEST", "recovery");
    let recovery = build_router(state, SecurityConfig::loopback("127.0.0.1:7878".parse()?)?)
        .oneshot(request(
            "POST",
            &path,
            r#"{"test_type":"URL_CANONICALIZATION","input_urls":["https://example.test"]}"#,
        )?)
        .await?;
    assert_eq!(recovery.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json(recovery).await?["code"],
        "RECOVERY_MODE_MUTATION_BLOCKED"
    );
    Ok(())
}

#[tokio::test]
async fn published_test_lab_targets_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let crawler = Crawler::new("API Test Lab lifecycle");
    let repository = CrawlerRepository::new(&database);
    repository.create(&crawler).await?;
    let version = repository
        .create_draft(crawler.id(), "operator", "unix:1")
        .await?;
    let mut version_to_publish = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    version_to_publish.add_seed(Seed::new(
        "https://example.test/".parse()?,
        "https://example.test/".parse()?,
    ))?;
    repository
        .save_draft(&version_to_publish, "operator", "unix:1b")
        .await?;
    repository
        .publish(crawler.id(), version.id(), "operator", "unix:2")
        .await?;
    let router = loopback(&database)?;
    let path = format!(
        "/api/v1/crawlers/{}/versions/{}/test-lab/tests",
        crawler.id(),
        version.id()
    );
    let response = router
        .oneshot(request(
            "POST",
            &path,
            r#"{"test_type":"URL_CANONICALIZATION","input_urls":["https://example.test"]}"#,
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(json(response).await?["code"], "VERSION_NOT_DRAFT");
    Ok(())
}

#[tokio::test]
async fn provider_unavailability_is_a_stable_api_error() -> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let crawler = Crawler::new("API Test Lab provider");
    let repository = CrawlerRepository::new(&database);
    repository.create(&crawler).await?;
    let version = repository
        .create_draft(crawler.id(), "operator", "unix:1")
        .await?;
    let page_type = repository
        .create_page_type(crawler.id(), version.id(), "Item", 1, "operator", "unix:2")
        .await?;
    let router = loopback(&database)?;
    let path = format!(
        "/api/v1/crawlers/{}/versions/{}/test-lab/tests",
        crawler.id(),
        version.id()
    );
    let response = router
        .oneshot(request(
            "POST",
            &path,
            &format!(
                "{{\"test_type\":\"SELECTOR_COVERAGE\",\"input_urls\":[\"https://example.test\"],\"page_type_id\":\"{}\"}}",
                page_type.id
            ),
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json(response).await?["code"],
        "TEST_LAB_PROVIDER_UNAVAILABLE"
    );
    Ok(())
}
