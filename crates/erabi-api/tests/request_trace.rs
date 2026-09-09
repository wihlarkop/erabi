use std::net::SocketAddr;

use axum::{Router, body::Body, http::Request};
use erabi_api::{AppState, SecurityConfig, build_router};
use erabi_observability::test_support::Capture;
use erabi_observability::test_support::CapturedRecord;
use tower::ServiceExt;

const SECRET_PATH_SEGMENT: &str = "DO_NOT_LOG_SECRET_PATH_74291";

fn loopback_router() -> Result<Router, Box<dyn std::error::Error>> {
    let address: SocketAddr = "127.0.0.1:7878".parse()?;
    Ok(build_router(
        AppState::ready(),
        SecurityConfig::loopback(address)?,
    ))
}

#[tokio::test]
async fn request_trace_captures_only_the_matched_route_template()
-> Result<(), Box<dyn std::error::Error>> {
    let capture = Capture::new();
    let router = loopback_router()?;
    let request = Request::builder()
        .method("GET")
        .uri(format!("/api/v1/artifacts/{SECRET_PATH_SEGMENT}"))
        .body(Body::empty())?;

    let response = capture.run(router.oneshot(request)).await?;
    assert_eq!(response.status(), axum::http::StatusCode::NOT_IMPLEMENTED);

    let records = capture.records();
    let Some(request_span) = records.iter().find_map(|record| match record {
        CapturedRecord::Span(span) if span.name == "http.request" => Some(span),
        _ => None,
    }) else {
        return Err("http.request span was not captured".into());
    };

    let route_template = request_span
        .fields
        .iter()
        .find(|field| field.name == "route_template")
        .map(|field| field.value.as_str());
    assert_eq!(route_template, Some("/api/v1/artifacts/{*path}"));

    let status_code = request_span
        .fields
        .iter()
        .find(|field| field.name == "status_code")
        .map(|field| field.value.as_str());
    assert_eq!(status_code, Some("501"));

    let duration_ms = request_span
        .fields
        .iter()
        .find(|field| field.name == "duration_ms")
        .map(|field| field.value.as_str());
    assert!(duration_ms.is_some_and(|value| value.parse::<u64>().is_ok()));

    assert!(!records.iter().any(|record| {
        match record {
            CapturedRecord::Span(span) => span
                .fields
                .iter()
                .any(|field| field.value.contains(SECRET_PATH_SEGMENT)),
            CapturedRecord::Event(event) => event
                .fields
                .iter()
                .any(|field| field.value.contains(SECRET_PATH_SEGMENT)),
        }
    }));
    Ok(())
}

#[tokio::test]
async fn request_trace_uses_the_closed_scalar_docs_template()
-> Result<(), Box<dyn std::error::Error>> {
    let capture = Capture::new();
    let response = capture
        .run(
            loopback_router()?.oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/docs")
                    .body(Body::empty())?,
            ),
        )
        .await?;
    assert_eq!(response.status(), axum::http::StatusCode::OK);

    let records = capture.records();
    let Some(request_span) = records.iter().find_map(|record| match record {
        CapturedRecord::Span(span) if span.name == "http.request" => Some(span),
        _ => None,
    }) else {
        return Err("http.request span was not captured".into());
    };

    let route_template = request_span
        .fields
        .iter()
        .find(|field| field.name == "route_template")
        .map(|field| field.value.as_str());
    assert_eq!(route_template, Some("/api/docs"));
    Ok(())
}
