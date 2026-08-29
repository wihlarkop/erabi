use std::fmt;

use secrecy::SecretString;
use thiserror::Error;
use url::Url;

/// Validated connection settings for a self-hosted `Crawl4AI` HTTP server.
#[derive(Clone)]
pub struct Crawl4AiConfig {
    base_endpoint: Url,
    api_token: Option<SecretString>,
}

impl Crawl4AiConfig {
    /// Creates settings after validating the endpoint and optional token.
    ///
    /// # Errors
    /// Returns an error when the endpoint or token is outside the bounded
    /// connection configuration contract.
    pub fn new(
        base_endpoint: impl AsRef<str>,
        api_token: Option<String>,
    ) -> Result<Self, Crawl4AiConfigError> {
        let raw_endpoint = base_endpoint.as_ref();
        if raw_endpoint.is_empty() {
            return Err(Crawl4AiConfigError::EndpointEmpty);
        }
        if raw_endpoint.chars().any(char::is_control) {
            return Err(Crawl4AiConfigError::EndpointControlCharacter);
        }

        let mut endpoint =
            Url::parse(raw_endpoint).map_err(|_| Crawl4AiConfigError::EndpointInvalid)?;
        match endpoint.scheme() {
            "http" | "https" => {}
            _ => return Err(Crawl4AiConfigError::EndpointSchemeUnsupported),
        }
        if endpoint.host_str().is_none() {
            return Err(Crawl4AiConfigError::EndpointHostRequired);
        }
        if !endpoint.username().is_empty() || endpoint.password().is_some() {
            return Err(Crawl4AiConfigError::EndpointCredentialsNotAllowed);
        }
        if endpoint.query().is_some() {
            return Err(Crawl4AiConfigError::EndpointQueryNotAllowed);
        }
        if endpoint.fragment().is_some() {
            return Err(Crawl4AiConfigError::EndpointFragmentNotAllowed);
        }

        let endpoint_path = endpoint.path().to_owned();
        if !endpoint_path.ends_with('/') {
            endpoint.set_path(&format!("{endpoint_path}/"));
        }

        let api_token = api_token.map(|token| {
            if token.trim().is_empty() {
                Err(Crawl4AiConfigError::ApiTokenEmpty)
            } else if token.chars().any(char::is_control) {
                Err(Crawl4AiConfigError::ApiTokenInvalid)
            } else {
                Ok(SecretString::from(token))
            }
        });
        let api_token = api_token.transpose()?;

        Ok(Self {
            base_endpoint: endpoint,
            api_token,
        })
    }

    pub(crate) fn endpoint(&self, path: &str) -> Result<Url, Crawl4AiConfigError> {
        self.base_endpoint
            .join(path)
            .map_err(|_| Crawl4AiConfigError::EndpointInvalid)
    }

    pub(crate) fn api_token(&self) -> Option<&SecretString> {
        self.api_token.as_ref()
    }

    /// Returns the validated base endpoint without any secret material.
    #[must_use]
    pub fn base_endpoint(&self) -> &Url {
        &self.base_endpoint
    }

    /// Returns whether protected provider requests will use a bearer token.
    #[must_use]
    pub const fn has_api_token(&self) -> bool {
        self.api_token.is_some()
    }
}

impl fmt::Debug for Crawl4AiConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Crawl4AiConfig")
            .field("base_endpoint", &self.base_endpoint)
            .field("api_token", &self.api_token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Configuration failures that are safe to expose to callers.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum Crawl4AiConfigError {
    #[error("Crawl4AI endpoint is empty")]
    EndpointEmpty,
    #[error("Crawl4AI endpoint is invalid")]
    EndpointInvalid,
    #[error("Crawl4AI endpoint must use HTTP or HTTPS")]
    EndpointSchemeUnsupported,
    #[error("Crawl4AI endpoint must include a host")]
    EndpointHostRequired,
    #[error("Crawl4AI endpoint credentials are not allowed")]
    EndpointCredentialsNotAllowed,
    #[error("Crawl4AI endpoint query parameters are not allowed")]
    EndpointQueryNotAllowed,
    #[error("Crawl4AI endpoint fragments are not allowed")]
    EndpointFragmentNotAllowed,
    #[error("Crawl4AI endpoint contains a control character")]
    EndpointControlCharacter,
    #[error("Crawl4AI API token is empty")]
    ApiTokenEmpty,
    #[error("Crawl4AI API token is invalid")]
    ApiTokenInvalid,
}
