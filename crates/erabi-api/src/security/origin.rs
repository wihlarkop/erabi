//! Host, origin, CORS, content-type, and request-body policy for mutations.

use axum::{
    body::{Body, to_bytes},
    extract::State,
    http::{HeaderValue, Method, Request, StatusCode, header},
    middleware::Next,
    response::Response,
};

use crate::{
    error::{ApiErrorEnvelope, error_response},
    security::{SecurityConfig, canonical_origin},
};

use super::super::app::trace_id_for;

/// Applies closed-by-default CORS and strict mutation request validation.
pub(crate) async fn enforce_browser_request_policy(
    State(config): State<SecurityConfig>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let trace_id = trace_id_for(&request);
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .and_then(canonical_origin);

    if request.method() == Method::OPTIONS && request.headers().contains_key(header::ORIGIN) {
        return preflight_response(&config, origin.as_deref(), &trace_id);
    }

    if is_mutation(request.method()) {
        let Some(host) = request
            .headers()
            .get(header::HOST)
            .and_then(|value| value.to_str().ok())
            .map(str::to_ascii_lowercase)
        else {
            return host_rejection(trace_id);
        };
        if !config.host_is_expected(&host) {
            return host_rejection(trace_id);
        }

        if let Some(raw_origin) = request.headers().get(header::ORIGIN) {
            let Some(origin) = raw_origin.to_str().ok().and_then(canonical_origin) else {
                return origin_rejection(trace_id);
            };
            if !same_origin(&origin, &host) && !config.is_allowed_cross_origin(&origin) {
                return origin_rejection(trace_id);
            }
        }

        if !is_json_content_type(request.headers().get(header::CONTENT_TYPE)) {
            return error_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                ApiErrorEnvelope::new(
                    "CONTENT_TYPE_NOT_ALLOWED",
                    "State-changing API requests must use application/json.",
                    trace_id,
                ),
            );
        }

        let (parts, body) = request.into_parts();
        let Ok(bytes) = to_bytes(body, config.mutation_body_limit_bytes()).await else {
            return error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                ApiErrorEnvelope::new(
                    "BODY_TOO_LARGE",
                    "The request body exceeds this endpoint's configured limit.",
                    trace_id,
                ),
            );
        };
        if serde_json::from_slice::<serde_json::Value>(&bytes).is_err() {
            return error_response(
                StatusCode::BAD_REQUEST,
                ApiErrorEnvelope::new(
                    "MALFORMED_JSON",
                    "The request body must contain valid JSON.",
                    trace_id,
                ),
            );
        }
        let request = Request::from_parts(parts, Body::from(bytes));
        return apply_cors_headers(next.run(request).await, &config, origin.as_deref());
    }

    apply_cors_headers(next.run(request).await, &config, origin.as_deref())
}

fn preflight_response(config: &SecurityConfig, origin: Option<&str>, trace_id: &str) -> Response {
    let Some(origin) = origin.filter(|origin| config.is_allowed_cross_origin(origin)) else {
        return origin_rejection(trace_id.to_owned());
    };
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;
    add_cors_headers(response.headers_mut(), origin);
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, PUT, PATCH, DELETE, OPTIONS"),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("authorization, content-type, x-erabi-trace-id"),
    );
    response
}

fn apply_cors_headers(
    mut response: Response,
    config: &SecurityConfig,
    origin: Option<&str>,
) -> Response {
    if let Some(origin) = origin.filter(|origin| config.is_allowed_cross_origin(origin)) {
        add_cors_headers(response.headers_mut(), origin);
    }
    response
}

fn add_cors_headers(headers: &mut axum::http::HeaderMap, origin: &str) {
    if let Ok(origin) = HeaderValue::from_str(origin) {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }
    headers.insert(header::VARY, HeaderValue::from_static("Origin"));
}

fn is_mutation(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

fn is_json_content_type(content_type: Option<&HeaderValue>) -> bool {
    content_type
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

fn same_origin(origin: &str, host: &str) -> bool {
    canonical_origin(origin).is_some_and(|origin| {
        url::Url::parse(&origin).is_ok_and(|parsed| super::host_for_url(&parsed) == host)
    })
}

fn host_rejection(trace_id: impl Into<String>) -> Response {
    error_response(
        StatusCode::BAD_REQUEST,
        ApiErrorEnvelope::new(
            "HOST_NOT_ALLOWED",
            "The request Host header is not allowed for this server.",
            trace_id,
        ),
    )
}

fn origin_rejection(trace_id: impl Into<String>) -> Response {
    error_response(
        StatusCode::FORBIDDEN,
        ApiErrorEnvelope::new(
            "ORIGIN_NOT_ALLOWED",
            "The request Origin is not allowed for this operation.",
            trace_id,
        ),
    )
}
