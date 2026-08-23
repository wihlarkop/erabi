//! Bounded Turso persistence adapters for Erabi domain contracts.

mod artifact_store;
mod configuration;
mod integrity;
mod migrate;
pub mod repositories;

use std::path::Path;

pub use artifact_store::{ArtifactStore, ArtifactStoreError, StoredArtifact};
pub use configuration::{
    BootstrapConfiguration, ConfigurationError, LocalDataOwnership, PersistedDestination,
    PersistedSetting, SecretEnvironmentVariableName, SettingScope,
};
pub use integrity::{LightweightIntegrityChecker, LightweightIntegrityError};
pub use migrate::{Migration, MigrationReport, MigrationRunner, SchemaVersion};

/// A structured migration failure suitable for a later Recovery Mode surface.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationFailure {
    pub version: Option<String>,
    pub state: MigrationFailureState,
    pub message: String,
}

/// The durable recovery-relevant class of a migration failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationFailureState {
    Apply,
    ChecksumMismatch,
    UnsupportedSchema,
    InvalidPlan,
}

/// Errors exposed by the persistence adapter boundary.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("Turso database error: {0}")]
    Turso(#[from] turso::Error),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("migration failure ({failure:?})")]
    MigrationFailure { failure: MigrationFailure },
    #[error("repository invariant violation: {0}")]
    Invariant(String),
}

/// The only database-handle type exposed by `erabi-db`.
#[derive(Clone, Debug)]
pub struct ErabiDatabase {
    database: turso::Database,
}

impl ErabiDatabase {
    /// Opens a local Turso database at a controlled path.
    ///
    /// # Errors
    /// Returns a Turso error when the local database cannot be opened.
    pub async fn open_local(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path = path.as_ref().to_string_lossy().into_owned();
        let database = turso::Builder::new_local(&path).build().await?;
        Ok(Self { database })
    }

    /// Opens an isolated in-memory database for tests and bounded probes.
    ///
    /// # Errors
    /// Returns a Turso error when the database cannot be opened.
    pub async fn in_memory() -> Result<Self, DbError> {
        let database = turso::Builder::new_local(":memory:").build().await?;
        Ok(Self { database })
    }

    /// Opens a connection with Erabi's required per-connection invariants.
    ///
    /// Foreign-key enforcement is connection-local in SQLite/Turso, so every
    /// repository and migration connection must enable it before issuing normal
    /// SQL. Keeping this factory crate-private prevents callers from obtaining
    /// an uninitialized raw connection through the persistence boundary.
    pub(crate) async fn connection(&self) -> Result<turso::Connection, DbError> {
        let connection = self.database.connect()?;
        connection.pragma_update("foreign_keys", "ON").await?;
        Ok(connection)
    }
}
