//! Bounded Turso persistence adapters for Erabi domain contracts.

mod artifact_store;
mod configuration;
mod integrity;
mod migrate;
pub mod repositories;

use std::{path::Path, sync::Arc};

use erabi_domain::VersionValidationRegistry;

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

impl DbError {
    /// Returns whether the worker must fail closed for this database failure.
    ///
    /// This exhaustive mapping keeps durable, representation, and programming
    /// failures from silently inheriting the retryable disposition when Turso
    /// adds another error variant.
    #[must_use]
    pub const fn is_durable_invariant(&self) -> bool {
        match self {
            Self::Serialization(_) | Self::MigrationFailure { .. } | Self::Invariant(_) => true,
            Self::Turso(error) => match error {
                turso::Error::ToSqlConversionFailure(_)
                | turso::Error::ConversionFailure(_)
                | turso::Error::Error(_)
                | turso::Error::Misuse(_)
                | turso::Error::NotAdb(_)
                | turso::Error::Corrupt(_)
                // A raw constraint has no operation context. Expected
                // duplicate or ownership semantics must be represented by
                // the repository boundary before reaching this classifier.
                | turso::Error::Constraint(_) => true,
                // These are explicit operational or contextual outcomes. A
                // later poll may succeed after contention, cancellation,
                // availability, storage, or ownership conditions change.
                turso::Error::QueryReturnedNoRows
                | turso::Error::Busy(_)
                | turso::Error::BusySnapshot(_)
                | turso::Error::Interrupt(_)
                | turso::Error::Readonly(_)
                // This explicit Turso capacity status remains operational;
                // external storage cleanup may allow a later poll to succeed.
                | turso::Error::DatabaseFull(_) => false,
                // ErrorKind is non-exhaustive; only these explicitly
                // retryable I/O conditions may continue polling.
                // Unlike DatabaseFull, raw I/O has no repository context; the
                // negated allowlist makes storage and future kinds fatal.
                turso::Error::IoError(kind, _) => !matches!(
                    kind,
                    std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::TimedOut
                ),
            },
        }
    }
}

/// The only database-handle type exposed by `erabi-db`.
#[derive(Clone, Debug)]
pub struct ErabiDatabase {
    database: turso::Database,
    validation_registry: Arc<VersionValidationRegistry>,
}

impl ErabiDatabase {
    /// Opens a local Turso database at a controlled path.
    ///
    /// # Errors
    /// Returns a Turso error when the local database cannot be opened.
    pub async fn open_local(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path = path.as_ref().to_string_lossy().into_owned();
        let database = turso::Builder::new_local(&path).build().await?;
        Ok(Self {
            database,
            validation_registry: Arc::new(VersionValidationRegistry::new()),
        })
    }

    /// Opens an isolated in-memory database for tests and bounded probes.
    ///
    /// # Errors
    /// Returns a Turso error when the database cannot be opened.
    pub async fn in_memory() -> Result<Self, DbError> {
        let database = turso::Builder::new_local(":memory:").build().await?;
        Ok(Self {
            database,
            validation_registry: Arc::new(VersionValidationRegistry::new()),
        })
    }

    /// Replaces the runtime's complete publication-validation registry before
    /// the database is attached to application services.
    #[must_use]
    pub fn with_version_validation_registry(mut self, registry: VersionValidationRegistry) -> Self {
        self.validation_registry = Arc::new(registry);
        self
    }

    pub(crate) fn version_validation_registry(&self) -> &VersionValidationRegistry {
        &self.validation_registry
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

#[cfg(test)]
mod tests {
    use super::DbError;

    #[test]
    fn typed_turso_invalid_and_programming_failures_are_fatal() {
        assert!(
            DbError::Turso(turso::Error::NotAdb("not a database".to_owned()))
                .is_durable_invariant()
        );
        assert!(
            DbError::Turso(turso::Error::Misuse("invalid API use".to_owned()))
                .is_durable_invariant()
        );
        assert!(
            DbError::Turso(turso::Error::ConversionFailure("invalid value".to_owned()))
                .is_durable_invariant()
        );
        assert!(
            DbError::Turso(turso::Error::Error("no such table".to_owned())).is_durable_invariant()
        );
        assert!(DbError::Turso(turso::Error::Corrupt("corrupt".to_owned())).is_durable_invariant());
        assert!(
            DbError::Turso(turso::Error::Constraint("foreign key".to_owned()))
                .is_durable_invariant()
        );

        let to_sql_conversion = match turso::Value::try_from(u64::MAX) {
            Err(error @ turso::Error::ToSqlConversionFailure(_)) => error,
            Err(error) => panic!("unexpected conversion error variant: {error:?}"),
            Ok(_) => panic!("u64::MAX unexpectedly converted to a SQL value"),
        };
        assert!(DbError::Turso(to_sql_conversion).is_durable_invariant());
    }

    #[test]
    fn typed_turso_operational_and_contextual_failures_are_retryable() {
        assert!(!DbError::Turso(turso::Error::Busy("busy".to_owned())).is_durable_invariant());
        assert!(
            !DbError::Turso(turso::Error::BusySnapshot("stale snapshot".to_owned()))
                .is_durable_invariant()
        );
        assert!(
            !DbError::Turso(turso::Error::Interrupt("interrupted".to_owned()))
                .is_durable_invariant()
        );
        assert!(!DbError::Turso(turso::Error::QueryReturnedNoRows).is_durable_invariant());
        assert!(
            !DbError::Turso(turso::Error::Readonly("readonly".to_owned())).is_durable_invariant()
        );
        assert!(
            !DbError::Turso(turso::Error::DatabaseFull("full".to_owned())).is_durable_invariant()
        );
    }

    #[test]
    fn explicitly_transient_turso_io_errors_are_retryable() {
        for kind in [
            std::io::ErrorKind::WouldBlock,
            std::io::ErrorKind::Interrupted,
            std::io::ErrorKind::TimedOut,
        ] {
            assert!(
                !DbError::Turso(turso::Error::IoError(kind, "read")).is_durable_invariant(),
                "{kind:?} should remain retryable"
            );
        }
    }

    #[test]
    fn unclassified_turso_io_errors_fail_closed() {
        for kind in [
            std::io::ErrorKind::InvalidData,
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::NotFound,
            std::io::ErrorKind::Other,
        ] {
            assert!(
                DbError::Turso(turso::Error::IoError(kind, "read")).is_durable_invariant(),
                "{kind:?} should fail closed"
            );
        }
    }
}
