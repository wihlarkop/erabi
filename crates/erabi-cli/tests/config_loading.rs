use std::collections::BTreeMap;

use erabi::{BindMode, BootstrapConfig, BootstrapConfigError};
use secrecy::ExposeSecret;

fn config(values: &[(&str, &str)]) -> Result<BootstrapConfig, BootstrapConfigError> {
    BootstrapConfig::from_values(
        &values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<BTreeMap<_, _>>(),
    )
}

#[test]
fn loopback_defaults_are_secure_and_local() -> Result<(), Box<dyn std::error::Error>> {
    let loaded = config(&[])?;

    assert_eq!(loaded.bind_address().to_string(), "127.0.0.1:7878");
    assert_eq!(loaded.bind_mode(), BindMode::Loopback);
    assert!(loaded.access_token().is_none());
    assert!(loaded.openapi_enabled());
    assert!(!loaded.telemetry_enabled());
    assert!(loaded.cors_allowed_origins().is_empty());
    Ok(())
}

#[test]
fn explicit_loopback_does_not_require_a_login() -> Result<(), Box<dyn std::error::Error>> {
    let loaded = config(&[("ERABI_HOST", "::1"), ("ERABI_PORT", "9090")])?;

    assert_eq!(loaded.bind_address().to_string(), "[::1]:9090");
    assert_eq!(loaded.bind_mode(), BindMode::Loopback);
    assert!(loaded.access_token().is_none());
    Ok(())
}

#[test]
fn remote_bind_with_a_token_is_allowed() -> Result<(), Box<dyn std::error::Error>> {
    let loaded = config(&[
        ("ERABI_HOST", "0.0.0.0"),
        ("ERABI_ACCESS_TOKEN", "not-an-example-secret"),
    ])?;

    assert_eq!(loaded.bind_mode(), BindMode::Remote);
    assert_eq!(
        loaded.access_token().map(ExposeSecret::expose_secret),
        Some("not-an-example-secret")
    );
    assert!(!loaded.openapi_enabled());
    Ok(())
}

#[test]
fn remote_bind_without_a_token_is_rejected() {
    assert!(matches!(
        config(&[("ERABI_HOST", "192.0.2.10")]),
        Err(BootstrapConfigError::RemoteAccessTokenRequired)
    ));
}

#[test]
fn remote_bind_with_an_empty_token_is_rejected() {
    assert!(matches!(
        config(&[("ERABI_HOST", "192.0.2.10"), ("ERABI_ACCESS_TOKEN", "   "),]),
        Err(BootstrapConfigError::RemoteAccessTokenRequired)
    ));
}

#[test]
fn invalid_bootstrap_values_fail_without_echoing_input() {
    assert!(matches!(
        config(&[("ERABI_HOST", "localhost")]),
        Err(BootstrapConfigError::InvalidHost)
    ));
    assert!(matches!(
        config(&[("ERABI_PORT", "70000")]),
        Err(BootstrapConfigError::InvalidPort)
    ));
    assert!(matches!(
        config(&[("ERABI_OPENAPI_ENABLED", "yes")]),
        Err(BootstrapConfigError::InvalidBoolean {
            variable: "ERABI_OPENAPI_ENABLED"
        })
    ));
    assert!(matches!(
        config(&[("ERABI_CORS_ALLOWED_ORIGINS", "*")]),
        Err(BootstrapConfigError::InvalidCorsOrigins)
    ));
}

#[test]
fn configured_values_override_dotenv_style_fallback_values()
-> Result<(), Box<dyn std::error::Error>> {
    let os_environment = BTreeMap::from([("ERABI_PORT".to_owned(), "8080".to_owned())]);
    let dotenv_fallback = BTreeMap::from([
        ("ERABI_HOST".to_owned(), "127.0.0.1".to_owned()),
        ("ERABI_PORT".to_owned(), "7878".to_owned()),
    ]);

    let loaded = BootstrapConfig::from_layered_values(&os_environment, &dotenv_fallback)?;
    assert_eq!(loaded.bind_address().port(), 8080);
    Ok(())
}

#[test]
fn debug_output_and_safe_urls_redact_all_bootstrap_secrets()
-> Result<(), Box<dyn std::error::Error>> {
    let loaded = config(&[
        ("ERABI_ACCESS_TOKEN", "access-token-value"),
        ("CRAWL4AI_API_TOKEN", "crawl4ai-token-value"),
        ("TURSO_AUTH_TOKEN", "turso-token-value"),
        (
            "CRAWL4AI_BASE_URL",
            "https://operator:connection-password@crawl.example.test:8443/path?token=query-token",
        ),
        (
            "TURSO_DATABASE_URL",
            "https://user:database-password@db.example.test/?authToken=query-token",
        ),
    ])?;

    let debug = format!("{loaded:?}");
    for secret in [
        "access-token-value",
        "crawl4ai-token-value",
        "turso-token-value",
        "connection-password",
        "database-password",
        "query-token",
    ] {
        assert!(!debug.contains(secret), "debug leaked {secret}");
    }
    assert!(debug.contains("https://crawl.example.test:8443"));
    assert!(debug.contains("https://db.example.test"));
    Ok(())
}
