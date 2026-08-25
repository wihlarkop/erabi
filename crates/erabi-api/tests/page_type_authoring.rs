use std::collections::BTreeSet;
use std::net::SocketAddr;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use erabi_api::{AppState, SecurityConfig, build_router};
use erabi_db::{ErabiDatabase, MigrationRunner};
use tower::ServiceExt;

async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
    let database = ErabiDatabase::in_memory().await?;
    MigrationRunner::default().apply(&database).await?;
    Ok(database)
}

fn router(database: &ErabiDatabase) -> Result<Router, Box<dyn std::error::Error>> {
    Ok(build_router(
        AppState::ready().with_crawler_authoring_runtime(database.clone()),
        SecurityConfig::loopback("127.0.0.1:7878".parse::<SocketAddr>()?)?,
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

async fn draft_ids(router: &Router) -> Result<(String, String), Box<dyn std::error::Error>> {
    let crawler = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/v1/crawlers",
            r#"{"name":"Catalog"}"#,
        )?)
        .await?;
    let crawler = json(crawler).await?;
    let crawler_id = crawler["id"]
        .as_str()
        .ok_or("missing crawler id")?
        .to_owned();
    let draft = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/drafts"),
            r#"{"base_version_id":null}"#,
        )?)
        .await?;
    let draft = json(draft).await?;
    let version_id = draft["id"].as_str().ok_or("missing version id")?.to_owned();
    Ok((crawler_id, version_id))
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn page_type_and_matcher_crud_keeps_matching_explainable()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let router = router(&database)?;
    let (crawler_id, version_id) = draft_ids(&router).await?;
    let page_path = format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types");

    let created = router
        .clone()
        .oneshot(request(
            "POST",
            &page_path,
            r#"{"name":"Products","priority":4}"#,
        )?)
        .await?;
    assert_eq!(created.status(), StatusCode::CREATED);
    let created = json(created).await?;
    let page_type_id = created["id"].as_str().ok_or("missing PageType id")?;
    assert_eq!(created["matchers"], serde_json::json!([]));

    let matcher_path = format!("{page_path}/{page_type_id}/matchers");
    let prefix = router
        .clone()
        .oneshot(request(
            "POST",
            &matcher_path,
            r#"{"kind":"PATH_PREFIX","host":"example.test","prefix":"/products"}"#,
        )?)
        .await?;
    assert_eq!(prefix.status(), StatusCode::CREATED);
    let prefix = json(prefix).await?;
    assert_eq!(prefix["kind"], "PATH_PREFIX");
    assert_eq!(prefix["ordinal"], 0);
    let prefix_id = prefix["id"].as_str().ok_or("missing matcher id")?;

    let exact = router
        .clone()
        .oneshot(request(
            "POST",
            &matcher_path,
            r#"{"kind":"EXACT_URL","url":"https://example.test/products/42"}"#,
        )?)
        .await?;
    assert_eq!(exact.status(), StatusCode::CREATED);
    let exact = json(exact).await?;
    assert_eq!(exact["ordinal"], 1);

    let matched = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/match-page-type"),
            r#"{"url":"https://example.test/products/42"}"#,
        )?)
        .await?;
    assert_eq!(matched.status(), StatusCode::OK);
    let matched = json(matched).await?;
    assert_eq!(matched["decision"], "MATCHED");
    assert_eq!(matched["candidate"]["page_type_id"], page_type_id);
    assert_eq!(matched["candidate"]["best_matcher_kind"], "EXACT_URL");
    assert_eq!(matched["candidate"]["matcher_kind_rank"], 4);
    assert_eq!(matched["candidate"]["literal_path_segments"], 2);
    assert_eq!(matched["candidate"]["wildcard_capture_count"], 0);

    let read = router
        .clone()
        .oneshot(request("GET", &format!("{page_path}/{page_type_id}"), "")?)
        .await?;
    let read = json(read).await?;
    assert_eq!(read["matchers"].as_array().map(Vec::len), Some(2));

    let updated = router
        .clone()
        .oneshot(request(
            "PUT",
            &format!("{page_path}/{page_type_id}"),
            r#"{"name":"Products renamed","priority":7}"#,
        )?)
        .await?;
    assert_eq!(updated.status(), StatusCode::OK);
    assert_eq!(json(updated).await?["name"], "Products renamed");

    let deleted_matcher = router
        .clone()
        .oneshot(request(
            "DELETE",
            &format!("{matcher_path}/{prefix_id}"),
            "{}",
        )?)
        .await?;
    if deleted_matcher.status() != StatusCode::NO_CONTENT {
        let body = json(deleted_matcher).await?;
        return Err(format!("delete matcher failed: {body}").into());
    }
    let deleted_page = router
        .clone()
        .oneshot(request(
            "DELETE",
            &format!("{page_path}/{page_type_id}"),
            "{}",
        )?)
        .await?;
    assert_eq!(deleted_page.status(), StatusCode::NO_CONTENT);

    let version = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}"),
            "",
        )?)
        .await?;
    assert_eq!(json(version).await?["page_type_count"], 0);
    let openapi = router
        .oneshot(request("GET", "/api/v1/openapi.json", "")?)
        .await?;
    let openapi = json(openapi).await?;
    assert!(
        openapi["components"]["schemas"]["UrlMatcherRequest"]["oneOf"]
            .as_array()
            .is_some_and(|variants| variants.len() == 5)
    );
    assert!(openapi["components"]["schemas"]["MatchDecision"].is_object());
    Ok(())
}

#[tokio::test]
async fn invalid_matchers_are_rejected_and_unmatched_is_explicit()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let router = router(&database)?;
    let (crawler_id, version_id) = draft_ids(&router).await?;
    let page_path = format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types");
    let page = router
        .clone()
        .oneshot(request(
            "POST",
            &page_path,
            r#"{"name":"Products","priority":0}"#,
        )?)
        .await?;
    let page_id = json(page).await?["id"]
        .as_str()
        .ok_or("missing page id")?
        .to_owned();
    let matcher_path = format!("{page_path}/{page_id}/matchers");

    for body in [
        r#"{"kind":"REGEX","pattern":"["}"#,
        r#"{"kind":"PATH_GLOB","pattern":""}"#,
        r#"{"kind":"EXACT_URL","url":"not a url"}"#,
        r#"{"kind":"UNKNOWN","pattern":"x"}"#,
    ] {
        let response = router
            .clone()
            .oneshot(request("POST", &matcher_path, body)?)
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json(response).await?["code"], "INVALID_URL_MATCHER");
    }
    let match_response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/match-page-type"),
            r#"{"url":"https://example.test/other"}"#,
        )?)
        .await?;
    assert_eq!(json(match_response).await?["decision"], "UNMATCHED");
    let match_invalid = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/match-page-type"),
            r#"{"url":"not a url"}"#,
        )?)
        .await?;
    assert_eq!(match_invalid.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json(match_invalid).await?["code"], "INVALID_MATCH_URL");
    let matchers = router.oneshot(request("GET", &matcher_path, "")?).await?;
    assert_eq!(json(matchers).await?, serde_json::json!([]));
    Ok(())
}

#[tokio::test]
async fn equal_candidates_remain_ambiguous_and_published_mutations_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let router = router(&database)?;
    let (crawler_id, version_id) = draft_ids(&router).await?;
    let page_path = format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types");
    let mut ids = Vec::new();
    let mut matcher_ids = Vec::new();
    for _ in 0..2 {
        let page = router
            .clone()
            .oneshot(request(
                "POST",
                &page_path,
                r#"{"name":"Same","priority":3}"#,
            )?)
            .await?;
        let page = json(page).await?;
        ids.push(page["id"].as_str().ok_or("missing page id")?.to_owned());
        let matcher_page_id = ids.last().ok_or("missing page id")?;
        let matcher_path = format!("{page_path}/{matcher_page_id}/matchers");
        let matcher = router
            .clone()
            .oneshot(request(
                "POST",
                &matcher_path,
                r#"{"kind":"PATH_PREFIX","host":"example.test","prefix":"/same"}"#,
            )?)
            .await?;
        assert_eq!(matcher.status(), StatusCode::CREATED);
        matcher_ids.push(
            json(matcher).await?["id"]
                .as_str()
                .ok_or("missing matcher id")?
                .to_owned(),
        );
    }
    let decision = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/match-page-type"),
            r#"{"url":"https://example.test/same/page"}"#,
        )?)
        .await?;
    let decision = json(decision).await?;
    assert_eq!(decision["decision"], "AMBIGUOUS_PAGE_TYPE");
    let candidates = decision["candidates"]
        .as_array()
        .ok_or("missing candidates")?;
    assert_eq!(candidates.len(), 2);
    let actual = candidates
        .iter()
        .filter_map(|candidate| candidate["page_type_id"].as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual,
        ids.iter().map(String::as_str).collect::<BTreeSet<_>>()
    );

    let published = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_id}/versions/{version_id}/publish"),
            r#"{"actor":"test"}"#,
        )?)
        .await?;
    assert_eq!(published.status(), StatusCode::OK);
    let mutation = router
        .clone()
        .oneshot(request(
            "POST",
            &page_path,
            r#"{"name":"Rejected","priority":0}"#,
        )?)
        .await?;
    assert_eq!(mutation.status(), StatusCode::CONFLICT);
    assert_eq!(json(mutation).await?["code"], "PUBLISHED_VERSION_IMMUTABLE");
    let matcher_mutation = router
        .clone()
        .oneshot(request(
            "PUT",
            &format!(
                "{page_path}/{}/matchers/{}",
                ids.first().ok_or("missing page id")?,
                matcher_ids.first().ok_or("missing matcher id")?
            ),
            r#"{"kind":"REGEX","pattern":"products"}"#,
        )?)
        .await?;
    assert_eq!(matcher_mutation.status(), StatusCode::CONFLICT);
    assert_eq!(
        json(matcher_mutation).await?["code"],
        "PUBLISHED_VERSION_IMMUTABLE"
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn ownership_is_validated_at_crawler_version_page_type_and_matcher_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let database = database().await?;
    let router = router(&database)?;
    let (crawler_a, version_a) = draft_ids(&router).await?;
    let crawler_b_response = router
        .clone()
        .oneshot(request("POST", "/api/v1/crawlers", r#"{"name":"Other"}"#)?)
        .await?;
    let crawler_b = json(crawler_b_response).await?["id"]
        .as_str()
        .ok_or("missing second crawler id")?
        .to_owned();
    let version_b_response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_b}/drafts"),
            r#"{"base_version_id":null}"#,
        )?)
        .await?;
    let version_b = json(version_b_response).await?["id"]
        .as_str()
        .ok_or("missing second version id")?
        .to_owned();
    let page_b_response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_b}/versions/{version_b}/page-types"),
            r#"{"name":"Other page","priority":0}"#,
        )?)
        .await?;
    let page_b = json(page_b_response).await?["id"]
        .as_str()
        .ok_or("missing second page id")?
        .to_owned();
    let own_page_response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/v1/crawlers/{crawler_a}/versions/{version_a}/page-types"),
            r#"{"name":"Own page","priority":0}"#,
        )?)
        .await?;
    let page_a = json(own_page_response).await?["id"]
        .as_str()
        .ok_or("missing own page id")?
        .to_owned();
    let matcher_a_response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!(
                "/api/v1/crawlers/{crawler_a}/versions/{version_a}/page-types/{page_a}/matchers"
            ),
            r#"{"kind":"PATH_PREFIX","prefix":"/own"}"#,
        )?)
        .await?;
    let matcher_a = json(matcher_a_response).await?["id"]
        .as_str()
        .ok_or("missing own matcher id")?
        .to_owned();

    let wrong_version = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/api/v1/crawlers/{crawler_a}/versions/{version_b}/page-types"),
            "",
        )?)
        .await?;
    assert_eq!(wrong_version.status(), StatusCode::CONFLICT);
    assert_eq!(
        json(wrong_version).await?["code"],
        "VERSION_NOT_OWNED_BY_CRAWLER"
    );

    let wrong_page = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/api/v1/crawlers/{crawler_a}/versions/{version_a}/page-types/{page_b}"),
            "",
        )?)
        .await?;
    assert_eq!(wrong_page.status(), StatusCode::CONFLICT);
    assert_eq!(
        json(wrong_page).await?["code"],
        "PAGE_TYPE_NOT_OWNED_BY_VERSION"
    );

    let wrong_matcher = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/api/v1/crawlers/{crawler_b}/versions/{version_b}/page-types/{page_b}/matchers/{matcher_a}"),
            "",
        )?)
        .await?;
    assert_eq!(wrong_matcher.status(), StatusCode::CONFLICT);
    assert_eq!(
        json(wrong_matcher).await?["code"],
        "URL_MATCHER_NOT_OWNED_BY_PAGE_TYPE"
    );
    Ok(())
}
