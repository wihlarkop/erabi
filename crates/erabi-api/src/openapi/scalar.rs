//! Self-hosted Scalar presentation for the generated `OpenAPI` document.

use axum::{
    body::Body,
    http::{HeaderValue, StatusCode, header},
    response::Response,
};
use serde_json::json;
use uuid::Uuid;

const SCALAR_ASSET: &str = "scalar.js";
const SCALAR_ASSET_PATH: &str = "/assets/scalar.js";

pub(crate) async fn docs() -> Response {
    let document = crate::openapi::generated_document();
    let configuration = json!({
        "spec": {
            "content": document,
        },
        "agent": {
            "disabled": true,
        },
    });
    let Ok(configuration_json) = serde_json::to_string(&configuration) else {
        return scalar_error_response();
    };
    let configuration_json = escape_script_data(&configuration_json);
    let nonce = Uuid::now_v7().simple().to_string();
    let html = scalar_api_reference::render_scalar(&configuration_json, Some(SCALAR_ASSET_PATH))
        .replacen(
            "<script>\n      Scalar.createApiReference",
            &format!("<script nonce=\"{nonce}\">\n      Scalar.createApiReference"),
            1,
        );

    let Ok(content_security_policy) = HeaderValue::from_str(&docs_content_security_policy(&nonce))
    else {
        return scalar_error_response();
    };
    let mut response = Response::new(Body::from(html));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(header::CONTENT_SECURITY_POLICY, content_security_policy);
    response
}

pub(crate) fn asset() -> Option<(String, Vec<u8>)> {
    scalar_api_reference::get_asset_with_mime(SCALAR_ASSET)
}

fn escape_script_data(value: &str) -> String {
    value
        .replace('&', "\\u0026")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}

fn docs_content_security_policy(nonce: &str) -> String {
    format!(
        "default-src 'self'; base-uri 'self'; object-src 'none'; frame-ancestors 'none'; form-action 'self'; script-src 'self' 'nonce-{nonce}'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'"
    )
}

fn scalar_error_response() -> Response {
    let mut response = Response::new(Body::from("Scalar documentation is unavailable."));
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_asset_is_embedded_and_non_empty() {
        let Some((mime, content)) = asset() else {
            panic!("Scalar asset is not embedded");
        };
        assert_eq!(mime, "application/javascript");
        assert!(!content.is_empty());
    }

    #[test]
    fn docs_policy_allows_only_nonce_scoped_inline_script() {
        let policy = docs_content_security_policy("nonce-value");
        assert!(policy.contains("script-src 'self' 'nonce-nonce-value'"));
        assert!(!policy.contains("script-src 'unsafe-inline'"));
        assert!(policy.contains("style-src 'self' 'unsafe-inline'"));
    }
}
