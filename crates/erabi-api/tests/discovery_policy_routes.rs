use std::net::SocketAddr;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use erabi_api::{AppState, SecurityConfig, build_router};
use erabi_db::{ErabiDatabase, MigrationRunner, repositories::CrawlerRepository};
use erabi_domain::{Crawler, CrawlerVersionGuardrails, Seed};
use secrecy::SecretString;
use tower::ServiceExt;

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

async fn persistent_database()
-> Result<(tempfile::TempDir, ErabiDatabase), Box<dyn std::error::Error>> {
    let data_dir = tempfile::tempdir()?;
    let database = ErabiDatabase::open_local(data_dir.path().join("erabi.db")).await?;
    MigrationRunner::default().apply(&database).await?;
    Ok((data_dir, database))
}

fn router(database: &ErabiDatabase) -> Result<Router, Box<dyn std::error::Error>> {
    Ok(build_router(
        AppState::ready().with_crawler_authoring_runtime(database.clone()),
        SecurityConfig::loopback("127.0.0.1:7878".parse::<SocketAddr>()?)?,
    ))
}

fn remote_router(database: &ErabiDatabase) -> Result<Router, Box<dyn std::error::Error>> {
    Ok(build_router(
        AppState::ready().with_crawler_authoring_runtime(database.clone()),
        SecurityConfig::remote(
            "192.0.2.10:7878".parse::<SocketAddr>()?,
            SecretString::from("discovery-policy-token"),
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
async fn crawler_discovery_policy_routes_are_typed_and_protected_by_lifecycle()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let router = router(&database)?;
    let crawler_response = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/v1/crawlers",
            r#"{"name":"Catalog"}"#,
        )?)
        .await?;
    let crawler = json(crawler_response).await?;
    let crawler_id = crawler["id"]
        .as_str()
        .ok_or("missing crawler id")?
        .to_owned();
    let draft_response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/drafts"),
            r#"{"base_version_id":null}"#,
        )?)
        .await?;
    let version = json(draft_response).await?;
    let version_id = version["id"]
        .as_str()
        .ok_or("missing version id")?
        .to_owned();
    let page_path = format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types");
    let source = router
        .clone()
        .oneshot(request(
            "POST",
            &page_path,
            r#"{"name":"Listing","priority":1}"#,
        )?)
        .await?;
    let source_id = json(source).await?["id"]
        .as_str()
        .ok_or("missing source PageType id")?
        .to_owned();
    let target = router
        .clone()
        .oneshot(request(
            "POST",
            &page_path,
            r#"{"name":"Product","priority":1}"#,
        )?)
        .await?;
    let target_id = json(target).await?["id"]
        .as_str()
        .ok_or("missing target PageType id")?
        .to_owned();

    let canonicalization_path =
        format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/canonicalization");
    let canonicalization = router
        .clone()
        .oneshot(request("GET", &canonicalization_path, "")?)
        .await?;
    assert_eq!(canonicalization.status(), StatusCode::OK);
    let canonicalization = json(canonicalization).await?;
    assert_eq!(canonicalization["version"], 1);
    let updated_canonicalization = router
        .clone()
        .oneshot(request(
            "PUT",
            &canonicalization_path,
            r#"{"version":1,"explicit_keep_parameters":["utm_source"],"explicit_drop_parameters":["session_mode"]}"#,
        )?)
        .await?;
    assert_eq!(updated_canonicalization.status(), StatusCode::OK);
    let explained = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/canonicalize-url"),
            r#"{"url":"https://EXAMPLE.test:443/product?utm_source=x&id=42#part"}"#,
        )?)
        .await?;
    assert_eq!(explained.status(), StatusCode::OK);
    let explained = json(explained).await?;
    assert_eq!(
        explained["canonical_url"],
        "https://example.test/product?id=42&utm_source=x"
    );
    assert_eq!(
        explained["original_url"],
        "https://EXAMPLE.test:443/product?utm_source=x&id=42#part"
    );

    let repository = CrawlerRepository::new(&database);
    let mut version_domain = repository
        .version(
            crawler_id
                .parse()
                .ok()
                .and_then(erabi_domain::CrawlerId::from_uuid)
                .ok_or("invalid crawler id")?,
            version_id
                .parse()
                .ok()
                .and_then(erabi_domain::CrawlerVersionId::from_uuid)
                .ok_or("invalid version id")?,
        )
        .await?
        .version;
    version_domain.add_seed(Seed::new(
        "https://example.test/".parse()?,
        "https://example.test/".parse()?,
    ))?;
    repository
        .save_draft(&version_domain, "operator", "now")
        .await?;

    let scope_path = format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/domain-scope");
    let scope_update = router
        .clone()
        .oneshot(request(
            "PUT",
            &scope_path,
            r#"{"version":1,"policy":{"kind":"EXPLICIT_ALLOWLIST","hosts":["example.test"]}}"#,
        )?)
        .await?;
    assert_eq!(scope_update.status(), StatusCode::OK);
    let classified = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/classify-domain-scope"),
            r#"{"url":"https://external.test/item"}"#,
        )?)
        .await?;
    assert_eq!(classified.status(), StatusCode::OK);
    let classified = json(classified).await?;
    assert_eq!(classified["classification"]["classification"], "EXTERNAL");
    assert_eq!(
        classified["canonicalization"]["canonical_url"],
        "https://external.test/item"
    );

    let guardrails_path = format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/guardrails");
    let guardrails = router
        .clone()
        .oneshot(request("GET", &guardrails_path, "")?)
        .await?;
    assert_eq!(guardrails.status(), StatusCode::OK);
    let mut guardrails = json(guardrails).await?;
    guardrails["max_pages"] = serde_json::json!(20);
    let guardrails_update = router
        .clone()
        .oneshot(request("PUT", &guardrails_path, &guardrails.to_string())?)
        .await?;
    assert_eq!(guardrails_update.status(), StatusCode::OK);

    let transition_path =
        format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions");
    let transition_body = format!(
        r#"{{"source_page_type_id":"{source_id}","target_page_type_id":"{target_id}","name":"listing links","enabled":true,"link_selector":"a[href]","url_constraints":null,"priority":1,"max_links_per_source_page":5,"total_transition_budget":20,"depth_contribution":1,"deduplicate":true}}"#
    );
    let created_transition = router
        .clone()
        .oneshot(request("POST", &transition_path, &transition_body)?)
        .await?;
    assert_eq!(created_transition.status(), StatusCode::CREATED);
    let created_transition = json(created_transition).await?;
    let transition_id = created_transition["id"]
        .as_str()
        .ok_or("missing transition id")?
        .to_owned();
    let listed = router
        .clone()
        .oneshot(request("GET", &transition_path, "")?)
        .await?;
    assert_eq!(listed.status(), StatusCode::OK);
    assert_eq!(json(listed).await?.as_array().map(Vec::len), Some(1));
    let transition_item_path = format!("{transition_path}/{transition_id}");
    let updated_body = transition_body.replace("listing links", "updated links");
    let updated_transition = router
        .clone()
        .oneshot(request("PUT", &transition_item_path, &updated_body)?)
        .await?;
    assert_eq!(updated_transition.status(), StatusCode::OK);
    assert_eq!(json(updated_transition).await?["name"], "updated links");
    let deleted_transition = router
        .clone()
        .oneshot(request("DELETE", &transition_item_path, "{}")?)
        .await?;
    if deleted_transition.status() != StatusCode::NO_CONTENT {
        return Err(format!(
            "delete transition failed: {}",
            json(deleted_transition).await?
        )
        .into());
    }

    let malformed = router
        .clone()
        .oneshot(request(
            "PUT",
            &canonicalization_path,
            r#"{"version":1,"unexpected":true}"#,
        )?)
        .await?;
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json(malformed).await?["code"], "INVALID_REQUEST");

    let published = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/publish"),
            r#"{"actor":"operator"}"#,
        )?)
        .await?;
    assert_eq!(published.status(), StatusCode::OK);
    let immutable = router
        .clone()
        .oneshot(request(
            "PUT",
            &canonicalization_path,
            r#"{"version":1,"explicit_keep_parameters":[],"explicit_drop_parameters":["gclid"]}"#,
        )?)
        .await?;
    assert_eq!(immutable.status(), StatusCode::CONFLICT);
    assert_eq!(
        json(immutable).await?["code"],
        "PUBLISHED_VERSION_IMMUTABLE"
    );

    let openapi = router
        .oneshot(request("GET", "/api/v1/openapi.json", "")?)
        .await?;
    let openapi = json(openapi).await?;
    assert!(
        openapi["paths"]
            .get("/api/v1/crawlers/{crawler_id}/versions/{version_id}/canonicalization")
            .is_some()
    );
    assert!(
        openapi["paths"]
            .get("/api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions/{transition_id}")
            .is_some()
    );
    assert!(openapi["components"]["schemas"]["CrawlerVersionGuardrails"].is_object());
    assert!(openapi["components"]["schemas"]["CanonicalizedDomainScopeResult"].is_object());
    Ok(())
}

#[tokio::test]
async fn remote_discovery_policy_mutations_require_bearer_authentication()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let response = remote_router(&database)?
        .oneshot(request(
            "PUT",
            "/api/v1/crawlers/00000000-0000-7000-8000-000000000000/versions/00000000-0000-7000-8000-000000000001/canonicalization",
            r#"{"version":1,"explicit_keep_parameters":[],"explicit_drop_parameters":[]}"#,
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(json(response).await?["code"], "AUTHENTICATION_REQUIRED");
    Ok(())
}

#[tokio::test]
async fn domain_scope_classification_fails_closed_for_corrupt_seed_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let (data_dir, database) = persistent_database().await?;
    let router = router(&database)?;
    let repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("Corrupt seed projection");
    repository.create(&crawler).await?;
    let mut version = repository
        .create_draft(crawler.id(), "operator", "now")
        .await?;
    let seed = Seed::new(
        "https://example.test/original".parse()?,
        "https://example.test/canonical".parse()?,
    );
    version.add_seed(seed.clone())?;
    repository.save_draft(&version, "operator", "now").await?;
    let database_path = data_dir.path().join("erabi.db");
    let raw_database = turso::Builder::new_local(database_path.to_string_lossy().as_ref())
        .build()
        .await?;
    raw_database
        .connect()?
        .execute(
            "UPDATE seeds SET enabled = 0 WHERE id = ?1",
            [seed.id.to_string()],
        )
        .await?;

    let response = router
        .oneshot(request(
            "POST",
            &format!(
                "/api/v1/crawlers/{}/versions/{}/classify-domain-scope",
                crawler.id(),
                version.id()
            ),
            r#"{"url":"https://example.test/next"}"#,
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(json(response).await?["code"], "PERSISTED_STATE_INVALID");
    Ok(())
}

#[tokio::test]
async fn page_type_guardrail_reference_blocks_draft_delete_with_in_use_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let router = router(&database)?;
    let repository = CrawlerRepository::new(&database);
    let crawler = Crawler::new("Guardrail PageType reference");
    repository.create(&crawler).await?;
    let version = repository
        .create_draft(crawler.id(), "operator", "now")
        .await?;
    let page_type = repository
        .create_page_type(crawler.id(), version.id(), "Listing", 1, "operator", "now")
        .await?;
    let mut guardrails = CrawlerVersionGuardrails::default();
    guardrails
        .page_types
        .push(erabi_domain::PageTypeDiscoveryGuardrails {
            page_type_id: page_type.id,
            page_budget: Some(10),
            health_threshold: None,
        });
    repository
        .update_crawler_version_guardrails(
            crawler.id(),
            version.id(),
            &guardrails,
            "operator",
            "now",
        )
        .await?;

    let response = router
        .oneshot(request(
            "DELETE",
            &format!(
                "/api/v1/crawlers/{}/versions/{}/page-types/{}",
                crawler.id(),
                version.id(),
                page_type.id
            ),
            "{}",
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(json(response).await?["code"], "PAGE_TYPE_IN_USE");
    Ok(())
}
