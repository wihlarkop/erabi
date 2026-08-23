use axum::http::{HeaderMap, HeaderValue, header};
use erabi_api::{REDACTED, redact_header, redact_headers, redact_json, redact_url};

#[test]
fn default_redaction_covers_headers_urls_and_nested_payloads() {
    assert_eq!(
        redact_header("Authorization", "Bearer token-value"),
        REDACTED
    );
    assert_eq!(redact_header("Cookie", "session=value"), REDACTED);
    assert_eq!(
        redact_header("Accept", "application/json"),
        "application/json"
    );

    let url =
        redact_url("https://user:password@example.test/path?access_token=token-value#fragment");
    assert_eq!(url, "https://example.test/path");
    assert!(!url.contains("token-value"));
    assert_eq!(redact_url("not a URL"), REDACTED);

    let value = serde_json::json!({
        "authorization": "Bearer token-value",
        "request_body": { "private": "value" },
        "raw_html": "<secret-page />",
        "extracted_values": { "email": "person@example.test" },
        "database_url": "libsql://db.example.test?authToken=token-value",
        "safe": { "status": "ok" }
    });
    let redacted = redact_json(&value);
    assert_eq!(redacted["authorization"], REDACTED);
    assert_eq!(redacted["request_body"], REDACTED);
    assert_eq!(redacted["raw_html"], REDACTED);
    assert_eq!(redacted["extracted_values"], REDACTED);
    assert_eq!(redacted["database_url"], REDACTED);
    assert_eq!(redacted["safe"]["status"], "ok");
    assert!(!redacted.to_string().contains("token-value"));
    assert!(!redacted.to_string().contains("person@example.test"));
}

#[test]
fn default_header_redaction_never_exposes_authorization_or_cookies() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer token-value"),
    );
    headers.insert(header::COOKIE, HeaderValue::from_static("session=value"));
    headers.insert(header::ACCEPT, HeaderValue::from_static("application/json"));

    let redacted = redact_headers(&headers);
    assert!(
        redacted
            .iter()
            .any(|(name, value)| name == "authorization" && value == REDACTED)
    );
    assert!(
        redacted
            .iter()
            .any(|(name, value)| name == "cookie" && value == REDACTED)
    );
    assert!(
        redacted
            .iter()
            .any(|(name, value)| name == "accept" && value == "application/json")
    );
}
