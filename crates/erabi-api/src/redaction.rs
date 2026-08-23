//! Default-safe redaction for diagnostics, audit-adjacent presentation, and logs.

use axum::http::HeaderMap;
use url::Url;

/// The stable replacement for content that must not leave a security boundary.
pub const REDACTED: &str = "[REDACTED]";

/// Redacts sensitive HTTP header values while preserving harmless headers.
#[must_use]
pub fn redact_header(name: &str, value: &str) -> String {
    if is_sensitive_key(name) {
        REDACTED.to_owned()
    } else {
        value.to_owned()
    }
}

/// Returns only headers that are safe for logs or a diagnostic response.
#[must_use]
pub fn redact_headers(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(name, value)| {
            let value = value.to_str().unwrap_or(REDACTED);
            (
                name.as_str().to_owned(),
                redact_header(name.as_str(), value),
            )
        })
        .collect()
}

/// Removes user info and query values from a URL before presentation or logging.
#[must_use]
pub fn redact_url(value: &str) -> String {
    let Ok(mut url) = Url::parse(value) else {
        return REDACTED.to_owned();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

/// Recursively redacts secret-bearing and scraped-content JSON fields.
#[must_use]
pub fn redact_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => object
            .iter()
            .map(|(key, value)| {
                let redacted = if is_sensitive_key(key) || is_scraped_content_key(key) {
                    serde_json::Value::String(REDACTED.to_owned())
                } else {
                    redact_json(value)
                };
                (key.clone(), redacted)
            })
            .collect(),
        serde_json::Value::Array(values) => values.iter().map(redact_json).collect(),
        value => value.clone(),
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "authorization",
        "cookie",
        "token",
        "secret",
        "password",
        "credential",
        "api_key",
        "access_key",
        "connection_string",
        "database_url",
        "turso",
        "crawl4ai",
    ]
    .iter()
    .any(|marker| key.contains(marker))
}

fn is_scraped_content_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "request_body",
        "response_body",
        "raw_page",
        "raw_html",
        "page_content",
        "extracted",
        "record_values",
    ]
    .iter()
    .any(|marker| key.contains(marker))
}
