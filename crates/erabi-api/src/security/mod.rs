//! Reusable remote-exposure and browser-request security boundaries.

mod auth;
mod headers;
mod origin;

use std::{collections::BTreeSet, fmt, net::SocketAddr};

use secrecy::{ExposeSecret, SecretString};
use url::Url;

pub(crate) use auth::require_bearer;
pub(crate) use headers::apply_security_headers;
pub(crate) use origin::enforce_browser_request_policy;

/// Whether protected routes require the shared remote bearer token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Exposure {
    /// Local loopback operation with the MVP local-operator trust model.
    Loopback,
    /// Non-loopback operation that requires shared bearer authentication.
    Remote,
}

/// Security policy applied consistently around all protected route groups.
#[derive(Clone)]
pub struct SecurityConfig {
    exposure: Exposure,
    access_token: Option<SecretString>,
    expected_hosts: BTreeSet<String>,
    allowed_origins: BTreeSet<String>,
    mutation_body_limit_bytes: usize,
    openapi_enabled: bool,
}

impl SecurityConfig {
    /// Builds the local-only policy for a verified loopback bind address.
    ///
    /// # Errors
    /// Returns a typed failure when the supplied listener is not loopback.
    pub fn loopback(bind_address: SocketAddr) -> Result<Self, SecurityConfigError> {
        if !bind_address.ip().is_loopback() {
            return Err(SecurityConfigError::LoopbackAddressRequired);
        }
        Ok(Self {
            exposure: Exposure::Loopback,
            access_token: None,
            expected_hosts: BTreeSet::from([host_for_socket_addr(bind_address)]),
            allowed_origins: BTreeSet::new(),
            mutation_body_limit_bytes: 64 * 1024,
            openapi_enabled: true,
        })
    }

    /// Builds the strict remote policy for an explicit bind and CORS allowlist.
    ///
    /// # Errors
    /// Returns a typed error when the supplied bearer token is empty or an
    /// allowlist entry is not an origin. The values themselves are never
    /// included in the error.
    pub fn remote(
        bind_address: SocketAddr,
        access_token: SecretString,
        allowed_origins: impl IntoIterator<Item = String>,
    ) -> Result<Self, SecurityConfigError> {
        if access_token.expose_secret().trim().is_empty() {
            return Err(SecurityConfigError::EmptyAccessToken);
        }

        let mut expected_hosts = BTreeSet::from([host_for_socket_addr(bind_address)]);
        let mut canonical_origins = BTreeSet::new();
        for origin in allowed_origins {
            let parsed = parse_origin(&origin)?;
            expected_hosts.insert(host_for_url(&parsed));
            canonical_origins.insert(parsed.origin().ascii_serialization());
        }

        Ok(Self {
            exposure: Exposure::Remote,
            access_token: Some(access_token),
            expected_hosts,
            allowed_origins: canonical_origins,
            mutation_body_limit_bytes: 64 * 1024,
            openapi_enabled: false,
        })
    }

    /// Returns the exposure classification without exposing authentication data.
    #[must_use]
    pub const fn exposure(&self) -> Exposure {
        self.exposure
    }

    /// Returns a copy with a bounded mutation-body limit for a route test or deployment policy.
    #[must_use]
    pub const fn with_mutation_body_limit(mut self, bytes: usize) -> Self {
        self.mutation_body_limit_bytes = bytes;
        self
    }

    /// Explicitly configures whether the generated `OpenAPI` document is exposed.
    ///
    /// Remote callers must opt in; loopback starts enabled by default.
    #[must_use]
    pub const fn with_openapi_enabled(mut self, enabled: bool) -> Self {
        self.openapi_enabled = enabled;
        self
    }

    /// Returns whether `OpenAPI` is exposed behind the protected router boundary.
    #[must_use]
    pub const fn openapi_enabled(&self) -> bool {
        self.openapi_enabled
    }

    pub(crate) fn requires_bearer_authentication(&self) -> bool {
        self.exposure == Exposure::Remote
    }

    pub(crate) fn access_token(&self) -> Option<&SecretString> {
        self.access_token.as_ref()
    }

    pub(crate) fn host_is_expected(&self, host: &str) -> bool {
        self.expected_hosts.contains(&host.to_ascii_lowercase())
    }

    pub(crate) fn is_allowed_cross_origin(&self, origin: &str) -> bool {
        self.allowed_origins.contains(origin)
    }

    pub(crate) const fn mutation_body_limit_bytes(&self) -> usize {
        self.mutation_body_limit_bytes
    }
}

impl fmt::Debug for SecurityConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecurityConfig")
            .field("exposure", &self.exposure)
            .field("access_token_configured", &self.access_token.is_some())
            .field("expected_hosts", &self.expected_hosts)
            .field("allowed_origins", &self.allowed_origins)
            .field("mutation_body_limit_bytes", &self.mutation_body_limit_bytes)
            .field("openapi_enabled", &self.openapi_enabled)
            .finish()
    }
}

/// Construction failures for the runtime security boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SecurityConfigError {
    /// Local unauthenticated policy is valid only for a loopback listener.
    #[error("loopback security configuration requires a loopback bind address")]
    LoopbackAddressRequired,
    /// Remote operation cannot use a missing or whitespace-only token.
    #[error("remote security configuration requires a non-empty access token")]
    EmptyAccessToken,
    /// A CORS allowlist entry was not a concrete HTTP(S) origin.
    #[error("CORS allowlist entries must be concrete HTTP(S) origins")]
    InvalidAllowedOrigin,
}

pub(crate) fn canonical_origin(origin: &str) -> Option<String> {
    parse_origin(origin)
        .ok()
        .map(|parsed| parsed.origin().ascii_serialization())
}

pub(crate) fn host_for_url(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    match url.port_or_known_default() {
        Some(port) => format!("{host}:{port}"),
        None => host,
    }
}

fn host_for_socket_addr(address: SocketAddr) -> String {
    match address.ip() {
        std::net::IpAddr::V4(ip) => format!("{ip}:{}", address.port()),
        std::net::IpAddr::V6(ip) => format!("[{ip}]:{}", address.port()),
    }
}

fn parse_origin(value: &str) -> Result<Url, SecurityConfigError> {
    if value == "*" {
        return Err(SecurityConfigError::InvalidAllowedOrigin);
    }
    let parsed = Url::parse(value).map_err(|_| SecurityConfigError::InvalidAllowedOrigin)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(SecurityConfigError::InvalidAllowedOrigin);
    }
    Ok(parsed)
}
