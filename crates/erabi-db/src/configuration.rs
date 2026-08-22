//! Persistence-safe bootstrap and ordinary configuration records.

use erabi_domain::LayerValue;

use crate::DbError;

/// Errors that protect the boundary between secret bootstrap data and persisted configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigurationError {
    #[error("invalid persisted configuration: {0}")]
    Invalid(String),
    #[error("configuration serialization error: {0}")]
    Serialization(String),
    #[error(transparent)]
    Database(#[from] DbError),
}

impl From<turso::Error> for ConfigurationError {
    fn from(error: turso::Error) -> Self {
        Self::Database(DbError::from(error))
    }
}

/// The name of an environment variable that contains a secret; never the secret value.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(transparent)]
pub struct SecretEnvironmentVariableName(String);

impl SecretEnvironmentVariableName {
    /// Validates and records an environment-variable name without reading its value.
    ///
    /// # Errors
    /// Returns an error for an empty or non-portable environment-variable name.
    pub fn new(value: impl Into<String>) -> Result<Self, ConfigurationError> {
        let value = value.into();
        let mut characters = value.chars();
        let Some(first) = characters.next() else {
            return Err(ConfigurationError::Invalid(
                "secret environment-variable name must not be empty".into(),
            ));
        };
        if !(first.is_ascii_uppercase() || first == '_')
            || !characters.all(|character| {
                character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
            })
        {
            return Err(ConfigurationError::Invalid(
                "secret environment-variable name must use uppercase letters, digits, and underscores"
                    .into(),
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> serde::Deserialize<'de> for SecretEnvironmentVariableName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <String as serde::Deserialize>::deserialize(deserializer)
            .map_err(serde::de::Error::custom)
            .and_then(|value| Self::new(value).map_err(serde::de::Error::custom))
    }
}

/// Bootstrap secret references and privacy-safe defaults supplied outside Turso.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BootstrapConfiguration {
    pub turso_auth_token_environment: Option<SecretEnvironmentVariableName>,
    pub crawl4ai_api_token_environment: Option<SecretEnvironmentVariableName>,
    pub access_token_environment: Option<SecretEnvironmentVariableName>,
    pub telemetry_enabled: bool,
}

/// The supported scope of an ordinary persisted setting.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "scope")]
pub enum SettingScope {
    Global,
    Collection { collection_id: String },
    Crawler { crawler_id: String },
    RunProfile { run_profile_id: String },
}

/// An ordinary persisted setting with an explicit tri-state layer value.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct PersistedSetting {
    scope: SettingScope,
    key: String,
    value: LayerValue<serde_json::Value>,
    updated_at: String,
}

impl PersistedSetting {
    /// Builds a persisted ordinary setting while rejecting secret-shaped keys.
    ///
    /// # Errors
    /// Returns an error when the key/time is invalid or the key could hold a secret.
    pub fn new(
        scope: SettingScope,
        key: impl Into<String>,
        value: LayerValue<serde_json::Value>,
        updated_at: impl Into<String>,
    ) -> Result<Self, ConfigurationError> {
        let setting = Self {
            scope,
            key: key.into(),
            value,
            updated_at: updated_at.into(),
        };
        setting.validate()?;
        Ok(setting)
    }

    #[must_use]
    pub const fn scope(&self) -> &SettingScope {
        &self.scope
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub const fn value(&self) -> &LayerValue<serde_json::Value> {
        &self.value
    }

    #[must_use]
    pub fn updated_at(&self) -> &str {
        &self.updated_at
    }

    pub(crate) fn id(&self) -> String {
        format!("{}:{}", self.scope.identifier(), self.key)
    }

    pub(crate) fn validate(&self) -> Result<(), ConfigurationError> {
        self.scope.validate()?;
        validate_ordinary_key(&self.key)?;
        validate_non_secret_layer_value(&self.value)?;
        require_non_empty("setting update time", &self.updated_at)
    }
}

#[derive(serde::Deserialize)]
struct PersistedSettingWire {
    scope: SettingScope,
    key: String,
    value: LayerValue<serde_json::Value>,
    updated_at: String,
}

impl<'de> serde::Deserialize<'de> for PersistedSetting {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = <PersistedSettingWire as serde::Deserialize>::deserialize(deserializer)?;
        Self::new(wire.scope, wire.key, wire.value, wire.updated_at)
            .map_err(serde::de::Error::custom)
    }
}

/// A persisted destination that may reference a secret by environment-variable name.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct PersistedDestination {
    id: String,
    name: String,
    destination_kind: String,
    configuration: serde_json::Value,
    secret_environment_variable_name: Option<SecretEnvironmentVariableName>,
    created_at: String,
    updated_at: String,
}

impl PersistedDestination {
    /// Builds a destination while rejecting secret values from persisted configuration.
    ///
    /// # Errors
    /// Returns an error when required fields are empty or configuration contains
    /// sensitive keys/values instead of an environment-variable name.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        destination_kind: impl Into<String>,
        configuration: serde_json::Value,
        secret_environment_variable_name: Option<SecretEnvironmentVariableName>,
        created_at: impl Into<String>,
        updated_at: impl Into<String>,
    ) -> Result<Self, ConfigurationError> {
        let destination = Self {
            id: id.into(),
            name: name.into(),
            destination_kind: destination_kind.into(),
            configuration,
            secret_environment_variable_name,
            created_at: created_at.into(),
            updated_at: updated_at.into(),
        };
        destination.validate()?;
        Ok(destination)
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn destination_kind(&self) -> &str {
        &self.destination_kind
    }

    #[must_use]
    pub const fn configuration(&self) -> &serde_json::Value {
        &self.configuration
    }

    #[must_use]
    pub const fn secret_environment_variable_name(&self) -> Option<&SecretEnvironmentVariableName> {
        self.secret_environment_variable_name.as_ref()
    }

    #[must_use]
    pub fn created_at(&self) -> &str {
        &self.created_at
    }

    #[must_use]
    pub fn updated_at(&self) -> &str {
        &self.updated_at
    }

    pub(crate) fn validate(&self) -> Result<(), ConfigurationError> {
        require_non_empty("destination id", &self.id)?;
        require_non_empty("destination name", &self.name)?;
        require_non_empty("destination kind", &self.destination_kind)?;
        require_non_empty("destination creation time", &self.created_at)?;
        require_non_empty("destination update time", &self.updated_at)?;
        validate_non_secret_json(&self.configuration)
    }
}

#[derive(serde::Deserialize)]
struct PersistedDestinationWire {
    id: String,
    name: String,
    destination_kind: String,
    configuration: serde_json::Value,
    secret_environment_variable_name: Option<SecretEnvironmentVariableName>,
    created_at: String,
    updated_at: String,
}

impl<'de> serde::Deserialize<'de> for PersistedDestination {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = <PersistedDestinationWire as serde::Deserialize>::deserialize(deserializer)?;
        Self::new(
            wire.id,
            wire.name,
            wire.destination_kind,
            wire.configuration,
            wire.secret_environment_variable_name,
            wire.created_at,
            wire.updated_at,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Metadata used by Plan 03 to diagnose ownership of one local data directory.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LocalDataOwnership {
    pub canonical_data_directory: String,
    pub process_id: i64,
    pub started_at: String,
    pub erabi_version: String,
    pub bind_address: String,
    pub updated_at: String,
}

impl LocalDataOwnership {
    /// Validates a local-data ownership metadata record without acquiring a lock.
    ///
    /// # Errors
    /// Returns an error when required diagnostic metadata is missing.
    pub fn new(
        canonical_data_directory: impl Into<String>,
        process_id: i64,
        started_at: impl Into<String>,
        erabi_version: impl Into<String>,
        bind_address: impl Into<String>,
        updated_at: impl Into<String>,
    ) -> Result<Self, ConfigurationError> {
        let ownership = Self {
            canonical_data_directory: canonical_data_directory.into(),
            process_id,
            started_at: started_at.into(),
            erabi_version: erabi_version.into(),
            bind_address: bind_address.into(),
            updated_at: updated_at.into(),
        };
        require_non_empty(
            "canonical data directory",
            &ownership.canonical_data_directory,
        )?;
        require_non_empty("owner start time", &ownership.started_at)?;
        require_non_empty("Erabi version", &ownership.erabi_version)?;
        require_non_empty("owner bind address", &ownership.bind_address)?;
        require_non_empty("owner update time", &ownership.updated_at)?;
        Ok(ownership)
    }
}

impl SettingScope {
    fn validate(&self) -> Result<(), ConfigurationError> {
        match self {
            Self::Global => Ok(()),
            Self::Collection { collection_id } => {
                require_non_empty("collection setting scope", collection_id)
            }
            Self::Crawler { crawler_id } => require_non_empty("crawler setting scope", crawler_id),
            Self::RunProfile { run_profile_id } => {
                require_non_empty("run profile setting scope", run_profile_id)
            }
        }
    }

    pub(crate) fn database_parts(&self) -> (&'static str, Option<&str>) {
        match self {
            Self::Global => ("GLOBAL", None),
            Self::Collection { collection_id } => ("COLLECTION", Some(collection_id)),
            Self::Crawler { crawler_id } => ("CRAWLER", Some(crawler_id)),
            Self::RunProfile { run_profile_id } => ("RUN_PROFILE", Some(run_profile_id)),
        }
    }

    pub(crate) fn from_database_parts(
        scope_type: &str,
        scope_id: Option<&str>,
    ) -> Result<Self, ConfigurationError> {
        let required_id = |label: &str| {
            scope_id.map(str::to_owned).ok_or_else(|| {
                ConfigurationError::Invalid(format!("{label} setting scope requires an identifier"))
            })
        };
        match scope_type {
            "GLOBAL" => Ok(Self::Global),
            "COLLECTION" => Ok(Self::Collection {
                collection_id: required_id("collection")?,
            }),
            "CRAWLER" => Ok(Self::Crawler {
                crawler_id: required_id("crawler")?,
            }),
            "RUN_PROFILE" => Ok(Self::RunProfile {
                run_profile_id: required_id("run profile")?,
            }),
            _ => Err(ConfigurationError::Invalid(format!(
                "unsupported persisted setting scope {scope_type}"
            ))),
        }
    }

    fn identifier(&self) -> String {
        match self {
            Self::Global => "GLOBAL".into(),
            Self::Collection { collection_id } => format!("COLLECTION:{collection_id}"),
            Self::Crawler { crawler_id } => format!("CRAWLER:{crawler_id}"),
            Self::RunProfile { run_profile_id } => format!("RUN_PROFILE:{run_profile_id}"),
        }
    }
}

pub(crate) fn layer_value_parts(
    value: &LayerValue<serde_json::Value>,
) -> Result<(&'static str, Option<String>), ConfigurationError> {
    match value {
        LayerValue::Inherit => Ok(("INHERIT", None)),
        LayerValue::Custom(value) => Ok((
            "CUSTOM",
            Some(
                serde_json::to_string(value)
                    .map_err(|error| ConfigurationError::Serialization(error.to_string()))?,
            ),
        )),
        LayerValue::ResetToBuiltIn => Ok(("RESET_TO_BUILT_IN", None)),
    }
}

pub(crate) fn layer_value_from_parts(
    state: &str,
    value_json: Option<String>,
) -> Result<LayerValue<serde_json::Value>, ConfigurationError> {
    match state {
        "INHERIT" if value_json.is_none() => Ok(LayerValue::Inherit),
        "RESET_TO_BUILT_IN" if value_json.is_none() => Ok(LayerValue::ResetToBuiltIn),
        "CUSTOM" => value_json
            .ok_or_else(|| ConfigurationError::Invalid("CUSTOM setting has no value".into()))
            .and_then(|value| {
                serde_json::from_str(&value)
                    .map_err(|error| ConfigurationError::Serialization(error.to_string()))
                    .map(LayerValue::Custom)
            }),
        _ => Err(ConfigurationError::Invalid(
            "stored setting state/value combination is invalid".into(),
        )),
    }
}

fn validate_ordinary_key(key: &str) -> Result<(), ConfigurationError> {
    require_non_empty("setting key", key)?;
    let normalized = key.to_ascii_lowercase();
    if [
        "secret",
        "token",
        "password",
        "credential",
        "api_key",
        "authorization",
    ]
    .iter()
    .any(|sensitive| normalized.contains(sensitive))
    {
        return Err(ConfigurationError::Invalid(
            "ordinary persisted settings cannot hold secret-shaped keys".into(),
        ));
    }
    Ok(())
}

fn validate_non_secret_json(value: &serde_json::Value) -> Result<(), ConfigurationError> {
    match value {
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                validate_ordinary_key(key)?;
                validate_non_secret_json(value)?;
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                validate_non_secret_json(value)?;
            }
        }
        serde_json::Value::String(value) if value.starts_with("Bearer ") => {
            return Err(ConfigurationError::Invalid(
                "persisted configuration cannot contain bearer secret values".into(),
            ));
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
    Ok(())
}

fn validate_non_secret_layer_value(
    value: &LayerValue<serde_json::Value>,
) -> Result<(), ConfigurationError> {
    if let LayerValue::Custom(value) = value {
        validate_non_secret_json(value)?;
    }
    Ok(())
}

fn require_non_empty(label: &str, value: &str) -> Result<(), ConfigurationError> {
    if value.trim().is_empty() {
        return Err(ConfigurationError::Invalid(format!(
            "{label} must not be empty"
        )));
    }
    Ok(())
}
