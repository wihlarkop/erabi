//! Environment-only bootstrap configuration with secret-safe presentation.

use std::{
    collections::BTreeMap,
    fmt,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

use secrecy::SecretString;
use url::Url;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 7878;

/// Whether the process is safe to expose without authentication in the MVP.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindMode {
    /// A local operator is connecting over a loopback interface.
    Loopback,
    /// The process can receive connections from outside the host and must authenticate.
    Remote,
}

/// An externally configured URL that is deliberately never formatted verbatim.
///
/// URLs may carry credentials or query-string credentials. This wrapper keeps
/// the parsed value usable by adapters while its `Debug` and `Display` forms
/// expose only an origin-shaped identifier.
#[derive(Clone, Eq, PartialEq)]
pub struct SafeUrl(Url);

impl SafeUrl {
    fn parse(variable: &'static str, value: &str) -> Result<Self, BootstrapConfigError> {
        let url = Url::parse(value).map_err(|_| BootstrapConfigError::InvalidUrl { variable })?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(BootstrapConfigError::InvalidUrl { variable });
        }
        Ok(Self(url))
    }

    /// Returns the configured endpoint for an adapter that needs to connect.
    #[must_use]
    pub const fn as_url(&self) -> &Url {
        &self.0
    }

    fn safe_identifier(&self) -> String {
        let host = self.0.host_str().unwrap_or("invalid-host");
        match self.0.port() {
            Some(port) => format!("{}://{host}:{port}", self.0.scheme()),
            None => format!("{}://{host}", self.0.scheme()),
        }
    }
}

impl fmt::Debug for SafeUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SafeUrl")
            .field(&self.safe_identifier())
            .finish()
    }
}

impl fmt::Display for SafeUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_identifier())
    }
}

/// `Crawl4AI` configuration obtained only from bootstrap environment values.
#[derive(Clone, Default)]
pub struct Crawl4AiBootstrapConfig {
    base_url: Option<SafeUrl>,
    api_token: Option<SecretString>,
}

impl Crawl4AiBootstrapConfig {
    /// Returns the configured `Crawl4AI` endpoint, when one was supplied.
    #[must_use]
    pub const fn base_url(&self) -> Option<&SafeUrl> {
        self.base_url.as_ref()
    }

    /// Returns the API token without exposing it through diagnostics.
    #[must_use]
    pub const fn api_token(&self) -> Option<&SecretString> {
        self.api_token.as_ref()
    }
}

impl fmt::Debug for Crawl4AiBootstrapConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Crawl4AiBootstrapConfig")
            .field("base_url", &self.base_url)
            .field("api_token_configured", &self.api_token.is_some())
            .finish()
    }
}

/// Turso bootstrap configuration. It is separate from ordinary persisted settings.
#[derive(Clone, Default)]
pub struct TursoBootstrapConfig {
    database_url: Option<SafeUrl>,
    auth_token: Option<SecretString>,
}

impl TursoBootstrapConfig {
    /// Returns the optional Turso endpoint.
    #[must_use]
    pub const fn database_url(&self) -> Option<&SafeUrl> {
        self.database_url.as_ref()
    }

    /// Returns the optional Turso token without exposing it through diagnostics.
    #[must_use]
    pub const fn auth_token(&self) -> Option<&SecretString> {
        self.auth_token.as_ref()
    }
}

impl fmt::Debug for TursoBootstrapConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TursoBootstrapConfig")
            .field("database_url", &self.database_url)
            .field("auth_token_configured", &self.auth_token.is_some())
            .finish()
    }
}

/// Fully parsed, environment-only configuration required before startup.
#[derive(Clone)]
pub struct BootstrapConfig {
    host: IpAddr,
    port: u16,
    data_dir: PathBuf,
    cors_allowed_origins: Vec<SafeUrl>,
    openapi_enabled: bool,
    telemetry_enabled: bool,
    crawl4ai: Crawl4AiBootstrapConfig,
    turso: TursoBootstrapConfig,
    access_token: Option<SecretString>,
}

impl BootstrapConfig {
    /// Loads `.env` as a fallback, while preserving existing OS environment values.
    ///
    /// # Errors
    /// Returns a typed error when fallback loading or validation fails. Values
    /// and secrets are intentionally omitted from every error variant.
    pub fn from_process_environment() -> Result<Self, BootstrapConfigError> {
        if let Err(error) = dotenvy::dotenv() {
            let missing_file = matches!(
                error,
                dotenvy::Error::Io(ref io_error)
                    if io_error.kind() == std::io::ErrorKind::NotFound
            );
            if !missing_file {
                return Err(BootstrapConfigError::DotenvUnavailable);
            }
        }

        let values = std::env::vars().collect::<BTreeMap<_, _>>();
        Self::from_values(&values)
    }

    /// Applies OS values over `.env` fallback values and parses the result.
    ///
    /// This captures the production precedence rule without exposing a secret
    /// in an error or requiring tests to mutate global process environment.
    ///
    /// # Errors
    /// Returns the same typed validation failures as [`Self::from_values`].
    pub fn from_layered_values(
        os_environment: &BTreeMap<String, String>,
        dotenv_fallback: &BTreeMap<String, String>,
    ) -> Result<Self, BootstrapConfigError> {
        let mut values = dotenv_fallback.clone();
        values.extend(os_environment.clone());
        Self::from_values(&values)
    }

    /// Parses bootstrap configuration from an explicit key/value source.
    ///
    /// This deterministic constructor keeps tests from mutating process-global
    /// environment state and makes precedence behavior explicit.
    ///
    /// # Errors
    /// Returns a typed error without incorporating an invalid supplied value.
    pub fn from_values(values: &BTreeMap<String, String>) -> Result<Self, BootstrapConfigError> {
        let host = value_or_default(values, "ERABI_HOST", DEFAULT_HOST)
            .parse::<IpAddr>()
            .map_err(|_| BootstrapConfigError::InvalidHost)?;
        let port = match values.get("ERABI_PORT") {
            Some(value) => value
                .parse::<u16>()
                .map_err(|_| BootstrapConfigError::InvalidPort)?,
            None => DEFAULT_PORT,
        };
        let data_dir = values
            .get("ERABI_DATA_DIR")
            .map_or_else(|| PathBuf::from("./data"), PathBuf::from);
        if data_dir.as_os_str().is_empty() {
            return Err(BootstrapConfigError::InvalidDataDirectory);
        }

        let bind_mode = bind_mode(host);
        let access_token = optional_secret(values, "ERABI_ACCESS_TOKEN");
        if bind_mode == BindMode::Remote
            && access_token.as_ref().is_none_or(|token| {
                secrecy::ExposeSecret::expose_secret(token)
                    .trim()
                    .is_empty()
            })
        {
            return Err(BootstrapConfigError::RemoteAccessTokenRequired);
        }

        let cors_allowed_origins = parse_origins(values.get("ERABI_CORS_ALLOWED_ORIGINS"))?;
        let openapi_enabled = match values.get("ERABI_OPENAPI_ENABLED") {
            Some(value) => parse_bool("ERABI_OPENAPI_ENABLED", value)?,
            None => bind_mode == BindMode::Loopback,
        };
        let telemetry_enabled = match values.get("ERABI_TELEMETRY_ENABLED") {
            Some(value) => parse_bool("ERABI_TELEMETRY_ENABLED", value)?,
            None => false,
        };

        Ok(Self {
            host,
            port,
            data_dir,
            cors_allowed_origins,
            openapi_enabled,
            telemetry_enabled,
            crawl4ai: Crawl4AiBootstrapConfig {
                base_url: optional_url(values, "CRAWL4AI_BASE_URL")?,
                api_token: optional_secret(values, "CRAWL4AI_API_TOKEN"),
            },
            turso: TursoBootstrapConfig {
                database_url: optional_url(values, "TURSO_DATABASE_URL")?,
                auth_token: optional_secret(values, "TURSO_AUTH_TOKEN"),
            },
            access_token,
        })
    }

    /// Returns the bind address validated before a server may start.
    #[must_use]
    pub const fn bind_address(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }

    /// Returns the secure bind classification.
    #[must_use]
    pub const fn bind_mode(&self) -> BindMode {
        bind_mode(self.host)
    }

    /// Returns the local data directory before startup canonicalizes it.
    #[must_use]
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Returns explicit external browser origins. An empty list means closed CORS.
    #[must_use]
    pub fn cors_allowed_origins(&self) -> &[SafeUrl] {
        &self.cors_allowed_origins
    }

    /// Returns whether `OpenAPI` is exposed under the bind-specific default policy.
    #[must_use]
    pub const fn openapi_enabled(&self) -> bool {
        self.openapi_enabled
    }

    /// Returns whether opt-in telemetry is enabled. The default is always false.
    #[must_use]
    pub const fn telemetry_enabled(&self) -> bool {
        self.telemetry_enabled
    }

    /// Returns `Crawl4AI` bootstrap values.
    #[must_use]
    pub const fn crawl4ai(&self) -> &Crawl4AiBootstrapConfig {
        &self.crawl4ai
    }

    /// Returns Turso bootstrap values.
    #[must_use]
    pub const fn turso(&self) -> &TursoBootstrapConfig {
        &self.turso
    }

    /// Returns the remote access secret for security middleware.
    #[must_use]
    pub const fn access_token(&self) -> Option<&SecretString> {
        self.access_token.as_ref()
    }
}

impl fmt::Debug for BootstrapConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BootstrapConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("bind_address", &self.bind_address())
            .field("bind_mode", &self.bind_mode())
            .field("data_dir", &self.data_dir)
            .field("cors_allowed_origins", &self.cors_allowed_origins)
            .field("openapi_enabled", &self.openapi_enabled)
            .field("telemetry_enabled", &self.telemetry_enabled)
            .field("crawl4ai", &self.crawl4ai)
            .field("turso", &self.turso)
            .field("access_token_configured", &self.access_token.is_some())
            .finish()
    }
}

/// Typed configuration failures that do not include untrusted configuration values.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BootstrapConfigError {
    /// The optional `.env` file could not be read safely.
    #[error("could not load .env fallback")]
    DotenvUnavailable,
    /// The host was not a literal IPv4 or IPv6 address.
    #[error("ERABI_HOST must be a literal IPv4 or IPv6 address")]
    InvalidHost,
    /// The configured port was outside the valid u16 range.
    #[error("ERABI_PORT must be a valid TCP port")]
    InvalidPort,
    /// The data directory was empty.
    #[error("ERABI_DATA_DIR must not be empty")]
    InvalidDataDirectory,
    /// A boolean setting did not use `true` or `false`.
    #[error("{variable} must be true or false")]
    InvalidBoolean { variable: &'static str },
    /// A configured external endpoint was not an absolute HTTP(S) URL.
    #[error("{variable} must be an absolute HTTP(S) URL")]
    InvalidUrl { variable: &'static str },
    /// CORS remains closed unless every allowlisted origin is explicit and safe.
    #[error(
        "ERABI_CORS_ALLOWED_ORIGINS must contain explicit HTTP(S) origins and cannot use wildcard"
    )]
    InvalidCorsOrigins,
    /// Remote exposure may not start without a meaningful shared bearer token.
    #[error("non-loopback bind requires a non-empty ERABI_ACCESS_TOKEN")]
    RemoteAccessTokenRequired,
}

fn value_or_default<'a>(
    values: &'a BTreeMap<String, String>,
    key: &str,
    default: &'a str,
) -> &'a str {
    values.get(key).map_or(default, String::as_str)
}

const fn bind_mode(host: IpAddr) -> BindMode {
    if host.is_loopback() {
        BindMode::Loopback
    } else {
        BindMode::Remote
    }
}

fn optional_secret(values: &BTreeMap<String, String>, key: &str) -> Option<SecretString> {
    values.get(key).cloned().map(SecretString::from)
}

fn optional_url(
    values: &BTreeMap<String, String>,
    variable: &'static str,
) -> Result<Option<SafeUrl>, BootstrapConfigError> {
    values
        .get(variable)
        .filter(|value| !value.trim().is_empty())
        .map(|value| SafeUrl::parse(variable, value))
        .transpose()
}

fn parse_bool(variable: &'static str, value: &str) -> Result<bool, BootstrapConfigError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(BootstrapConfigError::InvalidBoolean { variable }),
    }
}

fn parse_origins(value: Option<&String>) -> Result<Vec<SafeUrl>, BootstrapConfigError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }

    value
        .split(',')
        .map(str::trim)
        .map(|origin| {
            if origin == "*" {
                return Err(BootstrapConfigError::InvalidCorsOrigins);
            }
            let parsed = SafeUrl::parse("ERABI_CORS_ALLOWED_ORIGINS", origin)
                .map_err(|_| BootstrapConfigError::InvalidCorsOrigins)?;
            let url = parsed.as_url();
            if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
                return Err(BootstrapConfigError::InvalidCorsOrigins);
            }
            Ok(parsed)
        })
        .collect()
}
