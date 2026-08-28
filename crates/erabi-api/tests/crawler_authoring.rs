use std::net::SocketAddr;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use erabi_api::{AppState, SecurityConfig, build_router};
use erabi_db::{ErabiDatabase, MigrationRunner, repositories::CrawlerRepository};
use erabi_domain::{CrawlerId, CrawlerVersionId, Seed};
use secrecy::SecretString;
use tower::ServiceExt;

const TOKEN: &str = "crawler-authoring-token";

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

fn loopback(database: &ErabiDatabase) -> Result<Router, Box<dyn std::error::Error>> {
    Ok(build_router(
        AppState::ready().with_crawler_authoring_runtime(database.clone()),
        SecurityConfig::loopback("127.0.0.1:7878".parse::<SocketAddr>()?)?,
    ))
}

fn remote(database: &ErabiDatabase) -> Result<Router, Box<dyn std::error::Error>> {
    Ok(build_router(
        AppState::ready().with_crawler_authoring_runtime(database.clone()),
        SecurityConfig::remote(
            "192.0.2.10:7878".parse::<SocketAddr>()?,
            SecretString::from(TOKEN),
            Vec::new(),
        )?,
    ))
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

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn crawler_authoring_routes_return_typed_lifecycle_dtos_and_errors()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let router = loopback(&database)?;
    let created = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/v1/crawlers",
            r#"{"name":"Catalog"}"#,
        )?)
        .await?;
    assert_eq!(created.status(), StatusCode::CREATED);
    let crawler = json(created).await?;
    let crawler_id = crawler["id"].as_str().ok_or("missing crawler id")?;

    let second = router
        .clone()
        .oneshot(request("POST", "/api/v1/crawlers", r#"{"name":"Alpha"}"#)?)
        .await?;
    assert_eq!(second.status(), StatusCode::CREATED);

    let listed = router
        .clone()
        .oneshot(request("GET", "/api/v1/crawlers", "")?)
        .await?;
    assert_eq!(listed.status(), StatusCode::OK);
    let listed = json(listed).await?;
    assert_eq!(listed[0]["name"], "Alpha");
    assert_eq!(listed[1]["id"], crawler_id);

    let read = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/api/v1/crawlers/{crawler_id}"),
            "",
        )?)
        .await?;
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(json(read).await?["id"], crawler_id);

    let draft_response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/drafts"),
            r#"{"base_version_id":null}"#,
        )?)
        .await?;
    assert_eq!(draft_response.status(), StatusCode::CREATED);
    let draft = json(draft_response).await?;
    let version_id = draft["id"].as_str().ok_or("missing version id")?;
    assert_eq!(draft["state"], "DRAFT");
    assert!(draft["active_draft"].as_bool().unwrap_or(false));
    let version_id =
        CrawlerVersionId::from_uuid(version_id.parse()?).ok_or("invalid version id")?;
    let typed_crawler_id = CrawlerId::from_uuid(crawler_id.parse()?).ok_or("invalid crawler id")?;
    let repository = CrawlerRepository::new(&database);
    let mut version_to_publish = repository
        .version(typed_crawler_id, version_id)
        .await?
        .version;
    version_to_publish.add_seed(Seed::new(
        "https://example.test/".parse()?,
        "https://example.test/".parse()?,
    ))?;
    repository
        .save_draft(&version_to_publish, "operator", "unix:1b")
        .await?;
    let version_id = version_id.to_string();

    let preflight = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/publish-validation"),
            "",
        )?)
        .await?;
    assert_eq!(preflight.status(), StatusCode::OK);
    let preflight = json(preflight).await?;
    assert_eq!(preflight["version_id"], version_id);
    assert_eq!(preflight["publishable"], true);
    assert_eq!(preflight["blockers"], serde_json::json!([]));

    let duplicate = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/drafts"),
            r#"{"base_version_id":null}"#,
        )?)
        .await?;
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);
    assert_eq!(json(duplicate).await?["code"], "ACTIVE_DRAFT_EXISTS");

    let published = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/publish"),
            r#"{"actor":"operator"}"#,
        )?)
        .await?;
    assert_eq!(published.status(), StatusCode::OK);
    let published = json(published).await?;
    assert_eq!(published["state"], "PUBLISHED");
    assert_eq!(published["active_published"], true);
    assert_eq!(published["active_draft"], false);
    assert_eq!(published["warning_summary"], serde_json::json!([]));
    assert_eq!(published["config_hash"].as_str().map(str::len), Some(64));

    let versions = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/api/v1/crawlers/{crawler_id}/versions"),
            "",
        )?)
        .await?;
    assert_eq!(versions.status(), StatusCode::OK);
    assert_eq!(json(versions).await?[0]["id"], version_id);

    let read_version = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}"),
            "",
        )?)
        .await?;
    assert_eq!(read_version.status(), StatusCode::OK);
    assert_eq!(json(read_version).await?["id"], version_id);

    let invalid_draft = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/drafts"),
            r#"{"base_version_id":null}"#,
        )?)
        .await?;
    assert_eq!(invalid_draft.status(), StatusCode::CREATED);
    let invalid_draft = json(invalid_draft).await?;
    let invalid_draft_id = invalid_draft["id"]
        .as_str()
        .ok_or("missing invalid draft id")?;
    let blocked = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/versions/{invalid_draft_id}/publish"),
            r#"{"actor":"operator"}"#,
        )?)
        .await?;
    assert_eq!(blocked.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let blocked = json(blocked).await?;
    assert_eq!(blocked["code"], "PUBLISH_VALIDATION_FAILED");
    assert_eq!(blocked["details"]["version_id"], invalid_draft_id);
    assert_eq!(blocked["details"]["publishable"], false);
    assert_eq!(blocked["details"]["blockers"][0]["code"], "NO_ENABLED_SEED");

    let openapi = router
        .oneshot(request("GET", "/api/v1/openapi.json", "")?)
        .await?;
    let openapi = json(openapi).await?;
    assert!(
        openapi["paths"]
            .get("/api/v1/crawlers/{crawler_id}/drafts")
            .is_some()
    );
    assert!(
        openapi["paths"]
            .get("/api/v1/crawlers/{crawler_id}/versions/{version_id}/reactivate")
            .is_some()
    );
    assert!(
        openapi["paths"]
            .get("/api/v1/crawlers/{crawler_id}/versions/{version_id}/publish-validation")
            .is_some()
    );
    assert!(openapi["components"]["schemas"]["VersionValidationReport"].is_object());
    Ok(())
}

#[tokio::test]
async fn remote_crawler_mutation_inherits_bearer_authentication()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let response = remote(&database)?
        .oneshot(request(
            "POST",
            "/api/v1/crawlers",
            r#"{"name":"Catalog"}"#,
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(json(response).await?["code"], "AUTHENTICATION_REQUIRED");
    Ok(())
}
