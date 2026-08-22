use crate::ErabiDatabase;
use crate::configuration::{
    ConfigurationError, LocalDataOwnership, PersistedDestination, PersistedSetting,
    SecretEnvironmentVariableName, SettingScope, layer_value_from_parts, layer_value_parts,
};

/// Persistence operations for ordinary settings, destination references, and lock diagnostics.
#[derive(Clone, Copy, Debug)]
pub struct ConfigurationRepository<'database> {
    database: &'database ErabiDatabase,
}

impl<'database> ConfigurationRepository<'database> {
    #[must_use]
    pub const fn new(database: &'database ErabiDatabase) -> Self {
        Self { database }
    }

    /// Upserts one non-secret tri-state setting.
    ///
    /// # Errors
    /// Returns an error when serialization or persistence fails.
    pub async fn save_setting(&self, setting: &PersistedSetting) -> Result<(), ConfigurationError> {
        let (scope_type, scope_id) = setting.scope.database_parts();
        let (state, value_json) = layer_value_parts(&setting.value)?;
        let connection = self.database.connection()?;
        connection
            .execute(
                "INSERT INTO settings (id, scope_type, scope_id, setting_key, state, value_json, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(id) DO UPDATE SET state = excluded.state, value_json = excluded.value_json, updated_at = excluded.updated_at",
                (
                    setting.id(),
                    scope_type,
                    scope_id.map_or(turso::Value::Null, |value| turso::Value::Text(value.to_owned())),
                    setting.key.as_str(),
                    state,
                    value_json.map_or(turso::Value::Null, turso::Value::Text),
                    setting.updated_at.as_str(),
                ),
            )
            .await?;
        Ok(())
    }

    /// Loads one ordinary setting by scope/key, preserving its tri-state value.
    ///
    /// # Errors
    /// Returns an error when persistence data is malformed or cannot be read.
    pub async fn setting(
        &self,
        scope: &SettingScope,
        key: &str,
    ) -> Result<Option<PersistedSetting>, ConfigurationError> {
        let id = PersistedSetting::new(
            scope.clone(),
            key,
            erabi_domain::LayerValue::Inherit,
            "query",
        )?
        .id();
        let connection = self.database.connection()?;
        let mut rows = connection
            .query(
                "SELECT scope_type, scope_id, setting_key, state, value_json, updated_at FROM settings WHERE id = ?1",
                [id],
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        let scope_type: String = row.get(0)?;
        let scope_id: Option<String> = row.get(1)?;
        let key: String = row.get(2)?;
        let state: String = row.get(3)?;
        let value_json: Option<String> = row.get(4)?;
        let updated_at: String = row.get(5)?;
        Ok(Some(PersistedSetting {
            scope: SettingScope::from_database_parts(&scope_type, scope_id.as_deref())?,
            key,
            value: layer_value_from_parts(&state, value_json)?,
            updated_at,
        }))
    }

    /// Saves a destination configuration that references secrets only by environment-variable name.
    ///
    /// # Errors
    /// Returns an error when a secret-shaped configuration value is detected or
    /// persistence fails.
    pub async fn save_destination(
        &self,
        destination: &PersistedDestination,
    ) -> Result<(), ConfigurationError> {
        let configuration = serde_json::to_string(&destination.configuration)
            .map_err(|error| ConfigurationError::Serialization(error.to_string()))?;
        let connection = self.database.connection()?;
        connection
            .execute(
                "INSERT INTO persisted_destinations (id, name, destination_kind, configuration_json, secret_environment_variable_name, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(id) DO UPDATE SET name = excluded.name, destination_kind = excluded.destination_kind, configuration_json = excluded.configuration_json, secret_environment_variable_name = excluded.secret_environment_variable_name, updated_at = excluded.updated_at",
                (
                    destination.id.as_str(),
                    destination.name.as_str(),
                    destination.destination_kind.as_str(),
                    configuration,
                    destination.secret_environment_variable_name.as_ref().map_or(turso::Value::Null, |value| turso::Value::Text(value.as_str().to_owned())),
                    destination.created_at.as_str(),
                    destination.updated_at.as_str(),
                ),
            )
            .await?;
        Ok(())
    }

    /// Loads a destination without resolving any secret environment value.
    ///
    /// # Errors
    /// Returns an error when stored destination data is malformed or cannot be read.
    pub async fn destination(
        &self,
        id: &str,
    ) -> Result<Option<PersistedDestination>, ConfigurationError> {
        let connection = self.database.connection()?;
        let mut rows = connection
            .query(
                "SELECT name, destination_kind, configuration_json, secret_environment_variable_name, created_at, updated_at FROM persisted_destinations WHERE id = ?1",
                [id],
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        let name: String = row.get(0)?;
        let destination_kind: String = row.get(1)?;
        let configuration_json: String = row.get(2)?;
        let secret_name: Option<String> = row.get(3)?;
        let created_at: String = row.get(4)?;
        let updated_at: String = row.get(5)?;
        let configuration = serde_json::from_str(&configuration_json)
            .map_err(|error| ConfigurationError::Serialization(error.to_string()))?;
        let secret_environment_variable_name = secret_name
            .map(SecretEnvironmentVariableName::new)
            .transpose()?;
        Ok(Some(PersistedDestination::new(
            id,
            name,
            destination_kind,
            configuration,
            secret_environment_variable_name,
            created_at,
            updated_at,
        )?))
    }

    /// Upserts ownership metadata without acquiring or reclaiming a process lock.
    ///
    /// # Errors
    /// Returns an error when metadata cannot be persisted.
    pub async fn save_local_data_ownership(
        &self,
        ownership: &LocalDataOwnership,
    ) -> Result<(), ConfigurationError> {
        let connection = self.database.connection()?;
        connection
            .execute(
                "INSERT INTO local_data_owners (canonical_data_directory, process_id, started_at, erabi_version, bind_address, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(canonical_data_directory) DO UPDATE SET process_id = excluded.process_id, started_at = excluded.started_at, erabi_version = excluded.erabi_version, bind_address = excluded.bind_address, updated_at = excluded.updated_at",
                (
                    ownership.canonical_data_directory.as_str(),
                    ownership.process_id,
                    ownership.started_at.as_str(),
                    ownership.erabi_version.as_str(),
                    ownership.bind_address.as_str(),
                    ownership.updated_at.as_str(),
                ),
            )
            .await?;
        Ok(())
    }

    /// Reads persisted local-data ownership diagnostics.
    ///
    /// # Errors
    /// Returns an error when metadata cannot be read or is malformed.
    pub async fn local_data_ownership(
        &self,
        canonical_data_directory: &str,
    ) -> Result<Option<LocalDataOwnership>, ConfigurationError> {
        let connection = self.database.connection()?;
        let mut rows = connection
            .query(
                "SELECT process_id, started_at, erabi_version, bind_address, updated_at FROM local_data_owners WHERE canonical_data_directory = ?1",
                [canonical_data_directory],
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        Ok(Some(LocalDataOwnership::new(
            canonical_data_directory,
            row.get::<i64>(0)?,
            row.get::<String>(1)?,
            row.get::<String>(2)?,
            row.get::<String>(3)?,
            row.get::<String>(4)?,
        )?))
    }
}
