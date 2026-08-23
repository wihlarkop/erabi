use std::net::SocketAddr;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use erabi_api::{AppState, SecurityConfig, SecurityConfigError, build_router};
use secrecy::SecretString;
use tower::ServiceExt;

const REMOTE_HOST: &str = "192.0.2.10:7878";
const TOKEN: &str = "test-shared-bearer-token";

fn loopback_router() -> Result<Router, Box<dyn std::error::Error>> {
    let address: SocketAddr = "127.0.0.1:7878".parse()?;
    Ok(build_router(
        AppState::ready(),
        SecurityConfig::loopback(address)?,
    ))
}

fn remote_router() -> Result<Router, Box<dyn std::error::Error>> {
    remote_router_for(REMOTE_HOST, Vec::new())
}

fn remote_router_for(
    bind: &str,
    allowed_origins: Vec<String>,
) -> Result<Router, Box<dyn std::error::Error>> {
    let address: SocketAddr = bind.parse()?;
    let security = SecurityConfig::remote(address, SecretString::from(TOKEN), allowed_origins)?;
    Ok(build_router(AppState::ready(), security))
}

fn remote_router_with_openapi(enabled: bool) -> Result<Router, Box<dyn std::error::Error>> {
    let address: SocketAddr = REMOTE_HOST.parse()?;
    let security = SecurityConfig::remote(address, SecretString::from(TOKEN), Vec::new())?
        .with_openapi_enabled(enabled);
    Ok(build_router(AppState::ready(), security))
}

#[test]
fn loopback_policy_rejects_a_non_loopback_listener() -> Result<(), Box<dyn std::error::Error>> {
    let address: SocketAddr = REMOTE_HOST.parse()?;
    assert!(matches!(
        SecurityConfig::loopback(address),
        Err(SecurityConfigError::LoopbackAddressRequired)
    ));
    Ok(())
}

async fn error_code(
    response: axum::response::Response,
) -> Result<String, Box<dyn std::error::Error>> {
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    let value: serde_json::Value = serde_json::from_slice(&body)?;
    Ok(value["code"].as_str().unwrap_or_default().to_owned())
}

fn request(method: &str, path: &str) -> axum::http::request::Builder {
    Request::builder().method(method).uri(path)
}

#[tokio::test]
async fn loopback_routes_are_available_without_a_login() -> Result<(), Box<dyn std::error::Error>> {
    let response = loopback_router()?
        .oneshot(request("GET", "/api/v1/readiness").body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn remote_route_groups_require_a_bearer_token() -> Result<(), Box<dyn std::error::Error>> {
    for path in [
        "/api/v1/readiness",
        "/api/v1/events/stream",
        "/api/v1/assets/1",
        "/api/v1/exports/1",
        "/api/v1/backups/1",
        "/api/v1/artifacts/raw",
        "/api/v1/diagnostics/runtime",
    ] {
        let response = remote_router()?
            .oneshot(request("GET", path).body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
        assert_eq!(error_code(response).await?, "AUTHENTICATION_REQUIRED");
    }
    Ok(())
}

#[tokio::test]
async fn remote_browser_shell_and_compiled_asset_boundary_bootstrap_without_a_bearer()
-> Result<(), Box<dyn std::error::Error>> {
    for path in [
        "/",
        "/crawlers",
        "/runs",
        "/settings",
        "/future/client-route",
    ] {
        let shell = remote_router()?
            .oneshot(request("GET", path).body(Body::empty())?)
            .await?;
        assert_eq!(shell.status(), StatusCode::OK, "{path}");
    }

    let asset = remote_router()?
        .oneshot(request("GET", "/assets/app.js").body(Body::empty())?)
        .await?;
    assert_eq!(asset.status(), StatusCode::OK);
    assert_eq!(
        asset.headers().get(header::CONTENT_TYPE),
        Some(&axum::http::HeaderValue::from_static(
            "application/javascript; charset=utf-8"
        ))
    );
    Ok(())
}

#[tokio::test]
async fn remote_bearer_success_and_failures_are_stable() -> Result<(), Box<dyn std::error::Error>> {
    let success = remote_router()?
        .oneshot(
            request("GET", "/api/v1/readiness")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(success.status(), StatusCode::OK);

    let malformed = remote_router()?
        .oneshot(
            request("GET", "/api/v1/readiness")
                .header(header::AUTHORIZATION, "Basic abc")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(malformed.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(error_code(malformed).await?, "INVALID_BEARER_TOKEN");

    let wrong = remote_router()?
        .oneshot(
            request("GET", "/api/v1/readiness")
                .header(header::AUTHORIZATION, "Bearer wrong")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(wrong.status(), StatusCode::FORBIDDEN);
    assert_eq!(error_code(wrong).await?, "AUTHENTICATION_FAILED");
    Ok(())
}

#[tokio::test]
async fn mutation_host_origin_content_type_and_body_limits_are_enforced()
-> Result<(), Box<dyn std::error::Error>> {
    let unauthenticated = remote_router()?
        .oneshot(
            request("POST", "/api/v1/runs")
                .header(header::HOST, "attacker.example.test")
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from("not-json"))?,
        )
        .await?;
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        error_code(unauthenticated).await?,
        "AUTHENTICATION_REQUIRED"
    );

    let host_rejected = remote_router()?
        .oneshot(
            request("POST", "/api/v1/runs")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::HOST, "attacker.example.test")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(host_rejected.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error_code(host_rejected).await?, "HOST_NOT_ALLOWED");

    let origin_rejected = remote_router()?
        .oneshot(
            request("POST", "/api/v1/runs")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::HOST, REMOTE_HOST)
                .header(header::ORIGIN, "https://attacker.example.test")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(origin_rejected.status(), StatusCode::FORBIDDEN);
    assert_eq!(error_code(origin_rejected).await?, "ORIGIN_NOT_ALLOWED");

    let content_type_rejected = remote_router()?
        .oneshot(
            request("POST", "/api/v1/runs")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::HOST, REMOTE_HOST)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(
        content_type_rejected.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    assert_eq!(
        error_code(content_type_rejected).await?,
        "CONTENT_TYPE_NOT_ALLOWED"
    );

    let body_rejected = remote_router()?
        .oneshot(
            request("POST", "/api/v1/runs")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::HOST, REMOTE_HOST)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("x".repeat(64 * 1024 + 1)))?,
        )
        .await?;
    assert_eq!(body_rejected.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(error_code(body_rejected).await?, "BODY_TOO_LARGE");
    Ok(())
}

#[tokio::test]
async fn remote_host_policy_handles_explicit_and_wildcard_binds_without_trusting_cors_origins()
-> Result<(), Box<dyn std::error::Error>> {
    let explicit = remote_router_for("192.0.2.10:7878", Vec::new())?
        .oneshot(
            request("POST", "/api/v1/runs")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::HOST, "192.0.2.10:7878")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(explicit.status(), StatusCode::METHOD_NOT_ALLOWED);

    let wildcard = remote_router_for("0.0.0.0:7878", Vec::new())?
        .oneshot(
            request("POST", "/api/v1/runs")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::HOST, "192.0.2.34:7878")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(wildcard.status(), StatusCode::METHOD_NOT_ALLOWED);

    let attacker = remote_router_for("0.0.0.0:7878", Vec::new())?
        .oneshot(
            request("POST", "/api/v1/runs")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::HOST, "attacker.example.test:7878")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(attacker.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error_code(attacker).await?, "HOST_NOT_ALLOWED");

    let cors_is_not_host_trust = remote_router_for(
        "0.0.0.0:7878",
        vec!["https://frontend.example.test".to_owned()],
    )?
    .oneshot(
        request("POST", "/api/v1/runs")
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .header(header::HOST, "frontend.example.test:7878")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{}"))?,
    )
    .await?;
    assert_eq!(cors_is_not_host_trust.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        error_code(cors_is_not_host_trust).await?,
        "HOST_NOT_ALLOWED"
    );
    Ok(())
}

#[tokio::test]
async fn wildcard_ipv6_bind_accepts_only_concrete_ipv6_literal_hosts()
-> Result<(), Box<dyn std::error::Error>> {
    let accepted = remote_router_for("[::]:7878", Vec::new())?
        .oneshot(
            request("POST", "/api/v1/runs")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::HOST, "[2001:db8::34]:7878")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(accepted.status(), StatusCode::METHOD_NOT_ALLOWED);

    let unspecified = remote_router_for("[::]:7878", Vec::new())?
        .oneshot(
            request("POST", "/api/v1/runs")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::HOST, "[::]:7878")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(unspecified.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn cors_is_closed_by_default_and_security_headers_are_present()
-> Result<(), Box<dyn std::error::Error>> {
    let response = loopback_router()?
        .oneshot(
            request("GET", "/api/v1/health")
                .header(header::ORIGIN, "https://other.example.test")
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none()
    );
    assert!(
        response
            .headers()
            .contains_key(header::CONTENT_SECURITY_POLICY)
    );
    assert_eq!(
        response.headers().get(header::X_CONTENT_TYPE_OPTIONS),
        Some(&axum::http::HeaderValue::from_static("nosniff"))
    );
    assert_eq!(
        response.headers().get(header::REFERRER_POLICY),
        Some(&axum::http::HeaderValue::from_static("no-referrer"))
    );
    assert_eq!(
        response.headers().get(header::X_FRAME_OPTIONS),
        Some(&axum::http::HeaderValue::from_static("DENY"))
    );
    Ok(())
}

#[tokio::test]
async fn safe_trace_ids_propagate_into_headers_and_error_envelopes()
-> Result<(), Box<dyn std::error::Error>> {
    let trace_id = "trace-id-0001";
    let response = remote_router()?
        .oneshot(
            request("GET", "/api/v1/readiness")
                .header("x-erabi-trace-id", trace_id)
                .body(Body::empty())?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get("x-erabi-trace-id"),
        Some(&axum::http::HeaderValue::from_static("trace-id-0001"))
    );
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    let value: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(value["code"], "AUTHENTICATION_REQUIRED");
    assert_eq!(value["trace_id"], trace_id);
    assert!(value.get("details").is_none());
    Ok(())
}

#[tokio::test]
async fn recovery_mode_blocks_mutations_but_keeps_safe_diagnostics_available()
-> Result<(), Box<dyn std::error::Error>> {
    let address: SocketAddr = "127.0.0.1:7878".parse()?;
    let state = AppState::ready();
    state.enter_recovery("INTEGRITY_FAILURE", "Integrity check failed safely.");
    let router = build_router(state, SecurityConfig::loopback(address)?);

    let mutation = router
        .clone()
        .oneshot(
            request("POST", "/api/v1/runs")
                .header(header::HOST, "127.0.0.1:7878")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(mutation.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        error_code(mutation).await?,
        "RECOVERY_MODE_MUTATION_BLOCKED"
    );

    let diagnostics = router
        .oneshot(request("GET", "/api/v1/diagnostics/status").body(Body::empty())?)
        .await?;
    assert_eq!(diagnostics.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn graceful_shutdown_uses_a_distinct_mutation_error_and_runtime_state()
-> Result<(), Box<dyn std::error::Error>> {
    let address: SocketAddr = "127.0.0.1:7878".parse()?;
    let state = AppState::ready();
    state.stop_accepting_mutations();
    state.mark_shutting_down();
    let router = build_router(state.clone(), SecurityConfig::loopback(address)?);

    let mutation = router
        .clone()
        .oneshot(
            request("POST", "/api/v1/runs")
                .header(header::HOST, "127.0.0.1:7878")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(mutation.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error_code(mutation).await?, "SERVICE_SHUTTING_DOWN");
    assert_eq!(state.runtime_mode(), erabi_api::RuntimeMode::ShuttingDown);

    let diagnostics = router
        .oneshot(request("GET", "/api/v1/diagnostics/status").body(Body::empty())?)
        .await?;
    assert_eq!(diagnostics.status(), StatusCode::OK);
    let body = to_bytes(diagnostics.into_body(), usize::MAX).await?;
    assert!(String::from_utf8(body.to_vec())?.contains("SHUTTING_DOWN"));
    Ok(())
}

#[tokio::test]
async fn crawl4ai_outage_is_degraded_without_entering_recovery_mode()
-> Result<(), Box<dyn std::error::Error>> {
    let address: SocketAddr = "127.0.0.1:7878".parse()?;
    let state = AppState::ready();
    state.set_crawl4ai_degraded("adapter unavailable");
    let router = build_router(state, SecurityConfig::loopback(address)?);
    let readiness = router
        .oneshot(request("GET", "/api/v1/readiness").body(Body::empty())?)
        .await?;
    assert_eq!(readiness.status(), StatusCode::OK);
    let body = to_bytes(readiness.into_body(), usize::MAX).await?;
    let value: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(value["status"], "degraded");
    assert_eq!(
        value["crawl4ai"]["DEGRADED"]["message"],
        "adapter unavailable"
    );
    Ok(())
}

#[tokio::test]
async fn loopback_exposes_a_secret_free_openapi_document_by_default()
-> Result<(), Box<dyn std::error::Error>> {
    let response = loopback_router()?
        .oneshot(request("GET", "/api/v1/openapi.json").body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    let document: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(document["openapi"], "3.1.0");
    assert!(document["paths"].get("/api/v1/readiness").is_some());
    assert!(!String::from_utf8(body.to_vec())?.contains(TOKEN));
    Ok(())
}

#[tokio::test]
async fn remote_openapi_is_disabled_without_explicit_opt_in()
-> Result<(), Box<dyn std::error::Error>> {
    let unauthenticated = remote_router()?
        .oneshot(request("GET", "/api/v1/openapi.json").body(Body::empty())?)
        .await?;
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let disabled = remote_router()?
        .oneshot(
            request("GET", "/api/v1/openapi.json")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(disabled.status(), StatusCode::NOT_FOUND);
    assert_eq!(error_code(disabled).await?, "OPENAPI_DISABLED");
    Ok(())
}

#[tokio::test]
async fn explicitly_enabled_remote_openapi_remains_bearer_protected()
-> Result<(), Box<dyn std::error::Error>> {
    let unauthenticated = remote_router_with_openapi(true)?
        .oneshot(request("GET", "/api/v1/openapi.json").body(Body::empty())?)
        .await?;
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        error_code(unauthenticated).await?,
        "AUTHENTICATION_REQUIRED"
    );

    let authenticated = remote_router_with_openapi(true)?
        .oneshot(
            request("GET", "/api/v1/openapi.json")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(authenticated.status(), StatusCode::OK);
    Ok(())
}
