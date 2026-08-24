use std::net::SocketAddr;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use erabi_api::{AppState, SecurityConfig, build_router};
use erabi_jobs::{StoragePressureController, StoragePressurePolicy};
use tower::ServiceExt;

#[tokio::test]
async fn diagnostics_expose_typed_storage_pressure_without_raw_probe_errors()
-> Result<(), Box<dyn std::error::Error>> {
    let policy = StoragePressurePolicy::new(100, 50)?;
    let controller = StoragePressureController::new(policy);
    controller.update(policy.classify(50));
    let state = AppState::ready().with_storage_pressure_controller(controller);
    let router = build_router(
        state,
        SecurityConfig::loopback("127.0.0.1:7878".parse::<SocketAddr>()?)?,
    );

    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/diagnostics/status")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    let body_text = String::from_utf8(body.to_vec())?;
    let value: serde_json::Value = serde_json::from_str(&body_text)?;
    assert_eq!(value["storage_pressure"]["level"], "CRITICAL");
    assert_eq!(value["storage_pressure"]["free_bytes"], 50);
    assert_eq!(value["storage_pressure"]["warning_threshold"], 100);
    assert_eq!(value["storage_pressure"]["critical_threshold"], 50);
    assert!(!body_text.contains("storage free-space probe is unavailable"));
    Ok(())
}
