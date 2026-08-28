use std::{net::SocketAddr, sync::Arc};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use erabi_api::{AppState, SecurityConfig, build_router};
use erabi_crawler::{FixtureDiscoveryPreviewProvider, ObservedLink, PageObservation};
use erabi_db::{ErabiDatabase, MigrationRunner, repositories::CrawlerRepository};
use erabi_domain::{Crawler, Seed, UrlMatcher};
use tower::ServiceExt;

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

async fn fixture_router(
    database: &ErabiDatabase,
) -> Result<
    (
        Router,
        Crawler,
        erabi_domain::CrawlerVersion,
        erabi_domain::Seed,
    ),
    Box<dyn std::error::Error>,
> {
    let repository = CrawlerRepository::new(database);
    let crawler = Crawler::new("Discovery Preview API");
    repository.create(&crawler).await?;
    let version = repository
        .create_draft(crawler.id(), "operator", "unix:1")
        .await?;
    let page_type = repository
        .create_page_type(
            crawler.id(),
            version.id(),
            "Listing",
            1,
            "operator",
            "unix:2",
        )
        .await?;
    repository
        .create_url_matcher(
            crawler.id(),
            version.id(),
            page_type.id,
            &UrlMatcher::path_prefix(Some("example.test".to_owned()), "/listing"),
            "operator",
            "unix:3",
        )
        .await?;
    let seed = Seed::new(
        "https://example.test/listing".parse()?,
        "https://example.test/listing".parse()?,
    );
    let mut current = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    current.add_seed(seed.clone())?;
    repository
        .save_draft(&current, "operator", "unix:4")
        .await?;
    let version = repository
        .version(crawler.id(), version.id())
        .await?
        .version;
    let provider = FixtureDiscoveryPreviewProvider::observed(
        [PageObservation {
            requested_url: seed.original_url.to_string(),
            final_url: None,
            artifact_ids: Vec::new(),
            discovered_links: vec![ObservedLink {
                raw_href: "/listing/next".to_owned(),
                selector: None,
            }],
            selector_observations: Vec::new(),
            pagination_observations: Vec::new(),
        }],
        1,
    );
    let router = build_router(
        AppState::ready()
            .with_discovery_preview_runtime(database.clone(), Some(Arc::new(provider))),
        SecurityConfig::loopback("127.0.0.1:7878".parse::<SocketAddr>()?)?,
    );
    Ok((router, crawler, version, seed))
}

fn assert_typed_preview_openapi(openapi: &serde_json::Value) {
    assert!(
        openapi["paths"]["/api/v1/crawlers/{crawler_id}/versions/{version_id}/discovery-preview"]
            .is_object()
    );
    for schema in [
        "DiscoveryPreviewRequest",
        "PreviewLimits",
        "TransitionPreviewTotalLimit",
        "DiscoveryPreviewResult",
        "DiscoveryPreviewSummary",
        "DiscoveryPreviewPage",
        "DiscoveryPath",
        "PreviewGrowthIndicators",
        "PreviewQueryVariantGroup",
        "PreviewGrowthWarning",
        "EffectiveTransitionPreviewTotalLimit",
        "PreviewPageTypeDistribution",
        "PreviewTransitionCount",
        "PreviewBudgetHit",
    ] {
        assert!(
            openapi["components"]["schemas"][schema].is_object(),
            "missing {schema}"
        );
    }
    assert_eq!(
        openapi["components"]["schemas"]["PreviewGrowthIndicators"]["properties"]["query_variant_groups"]
            ["items"]["$ref"],
        "#/components/schemas/PreviewQueryVariantGroup"
    );
    assert!(
        openapi["components"]["schemas"]["PreviewQueryVariantGroup"]["required"]
            .as_array()
            .is_some_and(|required| required.contains(&serde_json::json!("total_identities")))
    );
    assert_eq!(
        openapi["components"]["schemas"]["DiscoveryPath"]["properties"]["canonicalization"]["anyOf"]
            [0]["$ref"],
        "#/components/schemas/CanonicalizationEvidence"
    );
    assert_eq!(
        openapi["components"]["schemas"]["DiscoveryPath"]["properties"]["scope"]["anyOf"][0]["$ref"],
        "#/components/schemas/DomainScopeEvidence"
    );
    assert_eq!(
        openapi["components"]["schemas"]["DiscoveryPath"]["properties"]["target_page_type_match"]["anyOf"]
            [0]["$ref"],
        "#/components/schemas/PageTypeMatchEvidence"
    );
    assert_eq!(
        openapi["components"]["schemas"]["DiscoveryPreviewPage"]["properties"]["diagnostic"]["anyOf"]
            [0]["$ref"],
        "#/components/schemas/TestDiagnostic"
    );
    assert_eq!(
        openapi["components"]["schemas"]["DiscoveryPreviewPage"]["properties"]["diagnostic"]["anyOf"]
            [1]["type"],
        "null"
    );
    assert_eq!(
        openapi["components"]["schemas"]["DiscoveryPreviewSummary"]["properties"]["page_type_distribution"]
            ["items"]["$ref"],
        "#/components/schemas/PreviewPageTypeDistribution"
    );
    assert_eq!(
        openapi["components"]["schemas"]["DiscoveryPreviewSummary"]["properties"]["transition_counts"]
            ["items"]["$ref"],
        "#/components/schemas/PreviewTransitionCount"
    );
    assert_eq!(
        openapi["components"]["schemas"]["DiscoveryPreviewSeed"]["properties"]["scope"]["anyOf"][1]
            ["type"],
        "null"
    );
    assert_eq!(
        openapi["components"]["schemas"]["DiscoveryPreviewPage"]["properties"]["page_type_match"]["anyOf"]
            [1]["type"],
        "null"
    );
}

#[tokio::test]
async fn discovery_preview_route_returns_ephemeral_preview_and_typed_openapi()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (router, crawler, version, seed) = fixture_router(&database).await?;
    let path = format!(
        "/api/v1/crawlers/{}/versions/{}/discovery-preview",
        crawler.id(),
        version.id()
    );
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            &path,
            &serde_json::json!({
                "seed_ids": [seed.id.to_string()],
                "limits": {
                    "max_pages": 3,
                    "max_depth": 2,
                    "max_duration_ms": 1000,
                    "default_transition_total_limit": 4,
                    "transition_total_limits": []
                }
            })
            .to_string(),
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let result = json(response).await?;
    assert_eq!(result["result_semantics"], "PREVIEW_ONLY");
    assert_eq!(result["crawler_version_id"], version.id().to_string());
    assert_eq!(result["selected_seed_ids"][0], seed.id.to_string());
    assert_eq!(result["summary"]["pages_sampled"], 1);
    assert!(result.get("complete_snapshot_eligible").is_none());
    assert!(result["pages"][0]["scope"].is_object());
    assert!(result["pages"][0]["page_type_match"].is_object());
    assert!(result["pages"][0]["diagnostic"].is_null());
    assert!(result["discovery_paths"][0]["canonicalization"].is_object());
    assert!(result["discovery_paths"][0]["scope"].is_object());
    assert!(result["discovery_paths"][0]["target_page_type_match"].is_object());

    let openapi = router
        .oneshot(request("GET", "/api/v1/openapi.json", "")?)
        .await?;
    let openapi = json(openapi).await?;
    assert_typed_preview_openapi(&openapi);
    Ok(())
}

#[tokio::test]
async fn discovery_preview_route_maps_invalid_request_and_inherits_bearer_auth()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let (router, crawler, version, _seed) = fixture_router(&database).await?;
    let path = format!(
        "/api/v1/crawlers/{}/versions/{}/discovery-preview",
        crawler.id(),
        version.id()
    );
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            &path,
            r#"{"seed_ids":[],"limits":{"max_pages":1,"max_depth":0,"max_duration_ms":1,"default_transition_total_limit":1,"transition_total_limits":[]}}"#,
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json(response).await?["code"], "NO_SELECTED_SEEDS");

    let protected = build_router(
        AppState::ready().with_discovery_preview_runtime(database, None),
        SecurityConfig::remote(
            "192.0.2.10:7878".parse::<SocketAddr>()?,
            secrecy::SecretString::from("preview-token"),
            Vec::new(),
        )?,
    );
    let unauthenticated = protected
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&path)
                .header(header::HOST, "192.0.2.10:7878")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}
