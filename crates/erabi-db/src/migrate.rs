use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use turso::transaction::TransactionBehavior;

use crate::{DbError, ErabiDatabase, MigrationFailure, MigrationFailureState};

const BOOTSTRAP_SQL: &str = r"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS migration_lock (
    lock_key TEXT PRIMARY KEY NOT NULL,
    owner TEXT NOT NULL,
    acquired_at TEXT NOT NULL
);
";

const MIGRATIONS: &[(&str, &str, &str)] = &[
    (
        "0001",
        "system",
        include_str!("../../../migrations/0001_system.sql"),
    ),
    (
        "0002",
        "crawler_core",
        include_str!("../../../migrations/0002_crawler_core.sql"),
    ),
    (
        "0003",
        "runs",
        include_str!("../../../migrations/0003_runs.sql"),
    ),
    (
        "0004",
        "jobs",
        include_str!("../../../migrations/0004_jobs.sql"),
    ),
];

/// One ordered SQL migration owned by Erabi.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Migration {
    version: String,
    name: String,
    sql: String,
}

impl Migration {
    #[must_use]
    pub fn new(
        version: impl Into<String>,
        name: impl Into<String>,
        sql: impl Into<String>,
    ) -> Self {
        Self {
            version: version.into(),
            name: name.into(),
            sql: sql.into(),
        }
    }

    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// An applied schema version recorded in the database.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaVersion {
    pub version: String,
    pub name: String,
    pub checksum: String,
    pub applied_at: String,
}

/// Result of a migration operation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MigrationReport {
    pub applied: Vec<String>,
}

/// SQL migration orchestration with ordered tracking, checksums, and a write lock.
#[derive(Clone, Debug)]
pub struct MigrationRunner {
    migrations: Vec<Migration>,
}

impl Default for MigrationRunner {
    fn default() -> Self {
        let migrations = MIGRATIONS
            .iter()
            .map(|(version, name, sql)| Migration::new(*version, *name, *sql))
            .collect();
        Self { migrations }
    }
}

impl MigrationRunner {
    /// Creates a runner after validating strict version ordering.
    ///
    /// # Errors
    /// Returns a typed invalid-plan failure when migration versions are empty,
    /// duplicated, or out of order.
    pub fn new(migrations: Vec<Migration>) -> Result<Self, DbError> {
        let runner = Self { migrations };
        runner.validate_plan()?;
        Ok(runner)
    }

    #[must_use]
    pub fn migrations(&self) -> &[Migration] {
        &self.migrations
    }

    /// Applies every pending migration under an immediate transaction lock.
    ///
    /// # Errors
    /// Returns a typed migration failure if an applied version is incompatible,
    /// a checksum differs, or a pending SQL migration cannot be applied.
    pub async fn apply(&self, database: &ErabiDatabase) -> Result<MigrationReport, DbError> {
        self.apply_until(database, None).await
    }

    /// Applies pending migrations up to and including `last_version`.
    ///
    /// This is primarily useful for validating a supported prior baseline.
    ///
    /// # Errors
    /// Returns an invalid-plan failure when `last_version` is unknown.
    pub async fn apply_through(
        &self,
        database: &ErabiDatabase,
        last_version: &str,
    ) -> Result<MigrationReport, DbError> {
        if !self
            .migrations
            .iter()
            .any(|migration| migration.version == last_version)
        {
            return Err(migration_failure(
                Some(last_version),
                MigrationFailureState::InvalidPlan,
                "requested migration baseline does not exist",
            ));
        }
        self.apply_until(database, Some(last_version)).await
    }

    /// Lists applied schema versions without mutating the database.
    ///
    /// # Errors
    /// Returns a Turso error when schema tracking cannot be read.
    pub async fn status(&self, database: &ErabiDatabase) -> Result<Vec<SchemaVersion>, DbError> {
        let connection = database.connection().await?;
        connection.execute_batch(BOOTSTRAP_SQL).await?;
        read_schema_versions(&connection).await
    }

    /// Verifies that recorded migrations exactly match this bundled chain
    /// without creating schema metadata or modifying product state.
    ///
    /// # Errors
    /// Returns a typed migration failure when schema history is incomplete,
    /// unknown, reordered, renamed, or checksum-incompatible.
    pub async fn verify(&self, database: &ErabiDatabase) -> Result<(), DbError> {
        self.validate_plan()?;
        let connection = database.connection().await?;
        let applied = read_schema_versions(&connection).await?;
        self.validate_applied_versions(&applied)?;
        if applied.len() != self.migrations.len() {
            return Err(migration_failure(
                None,
                MigrationFailureState::UnsupportedSchema,
                "recorded migrations do not cover the complete bundled schema chain",
            ));
        }
        for (migration, recorded) in self.migrations.iter().zip(&applied) {
            if recorded.version != migration.version || recorded.name != migration.name {
                return Err(migration_failure(
                    Some(&recorded.version),
                    MigrationFailureState::UnsupportedSchema,
                    "recorded migration identity differs from the bundled schema chain",
                ));
            }
            if recorded.checksum != migration_checksum(migration)? {
                return Err(migration_failure(
                    Some(&recorded.version),
                    MigrationFailureState::ChecksumMismatch,
                    "recorded migration checksum differs from the bundled schema chain",
                ));
            }
        }
        Ok(())
    }

    async fn apply_until(
        &self,
        database: &ErabiDatabase,
        last_version: Option<&str>,
    ) -> Result<MigrationReport, DbError> {
        self.validate_plan()?;
        let mut connection = database.connection().await?;
        connection.execute_batch(BOOTSTRAP_SQL).await?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;

        let result = self.apply_in_transaction(&transaction, last_version).await;
        match result {
            Ok(report) => transaction
                .commit()
                .await
                .map(|()| report)
                .map_err(DbError::from),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn apply_in_transaction(
        &self,
        connection: &turso::Connection,
        last_version: Option<&str>,
    ) -> Result<MigrationReport, DbError> {
        let lock_time = timestamp();
        connection
            .execute(
                "INSERT INTO migration_lock (lock_key, owner, acquired_at) VALUES (?1, ?2, ?3)",
                ("schema", "erabi-migrator", lock_time.as_str()),
            )
            .await
            .map_err(|error| {
                migration_failure(
                    None,
                    MigrationFailureState::Apply,
                    format!("could not acquire migration lock: {error}"),
                )
            })?;

        let applied = read_schema_versions(connection).await?;
        self.validate_applied_versions(&applied)?;
        let applied = applied
            .into_iter()
            .map(|version| (version.version.clone(), version))
            .collect::<BTreeMap<_, _>>();
        let mut report = MigrationReport::default();

        for migration in &self.migrations {
            if let Some(last_version) = last_version
                && migration.version.as_str() > last_version
            {
                break;
            }

            let checksum = migration_checksum(migration)?;
            if let Some(applied) = applied.get(&migration.version) {
                if applied.checksum != checksum {
                    return Err(migration_failure(
                        Some(&migration.version),
                        MigrationFailureState::ChecksumMismatch,
                        "an applied migration's checksum differs from the bundled SQL",
                    ));
                }
                continue;
            }

            if let Err(error) = execute_script(connection, &migration.sql).await {
                return Err(migration_failure(
                    Some(&migration.version),
                    MigrationFailureState::Apply,
                    format!("{}: {error}", migration.name),
                ));
            }
            let applied_at = timestamp();
            connection
                .execute(
                    "INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?1, ?2, ?3, ?4)",
                    (
                        migration.version.as_str(),
                        migration.name.as_str(),
                        checksum.as_str(),
                        applied_at.as_str(),
                    ),
                )
                .await?;
            report.applied.push(migration.version.clone());
        }

        connection
            .execute("DELETE FROM migration_lock WHERE lock_key = ?1", ["schema"])
            .await?;
        Ok(report)
    }

    fn validate_plan(&self) -> Result<(), DbError> {
        if self.migrations.is_empty() {
            return Err(migration_failure(
                None,
                MigrationFailureState::InvalidPlan,
                "migration plan must not be empty",
            ));
        }
        let mut previous: Option<&str> = None;
        for migration in &self.migrations {
            if migration.version.is_empty() {
                return Err(migration_failure(
                    None,
                    MigrationFailureState::InvalidPlan,
                    "migration version must not be empty",
                ));
            }
            if previous.is_some_and(|version| version >= migration.version.as_str()) {
                return Err(migration_failure(
                    Some(&migration.version),
                    MigrationFailureState::InvalidPlan,
                    "migration versions must be strictly increasing",
                ));
            }
            previous = Some(&migration.version);
        }
        Ok(())
    }

    fn validate_applied_versions(&self, applied: &[SchemaVersion]) -> Result<(), DbError> {
        for version in applied {
            if !self
                .migrations
                .iter()
                .any(|migration| migration.version == version.version)
            {
                return Err(migration_failure(
                    Some(&version.version),
                    MigrationFailureState::UnsupportedSchema,
                    "database contains a migration outside the supported Plan 02 schema chain",
                ));
            }
        }
        Ok(())
    }
}

async fn read_schema_versions(
    connection: &turso::Connection,
) -> Result<Vec<SchemaVersion>, DbError> {
    let mut rows = connection
        .query(
            "SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version",
            (),
        )
        .await?;
    let mut versions = Vec::new();
    while let Some(row) = rows.next().await? {
        versions.push(SchemaVersion {
            version: row.get(0)?,
            name: row.get(1)?,
            checksum: row.get(2)?,
            applied_at: row.get(3)?,
        });
    }
    Ok(versions)
}

async fn execute_script(connection: &turso::Connection, script: &str) -> Result<(), turso::Error> {
    connection.execute_batch(script).await
}

fn migration_checksum(migration: &Migration) -> Result<String, DbError> {
    erabi_domain::canonical_sha256(&migration.sql)
        .map_err(|error| DbError::Serialization(error.to_string()))
}

fn migration_failure(
    version: Option<&str>,
    state: MigrationFailureState,
    message: impl Into<String>,
) -> DbError {
    DbError::MigrationFailure {
        failure: MigrationFailure {
            version: version.map(str::to_owned),
            state,
            message: message.into(),
        },
    }
}

fn timestamp() -> String {
    let seconds = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    };
    format!("unix:{seconds}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn jobs_migration_owns_append_only_checkpoint_and_progress_substrates()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = ErabiDatabase::in_memory().await?;
        MigrationRunner::default().apply(&database).await?;
        let connection = database.connection().await?;

        for (object_type, object_name) in [
            ("table", "job_checkpoints"),
            ("table", "job_progress_events"),
            ("index", "job_checkpoints_by_job"),
            ("index", "job_checkpoints_by_attempt"),
            ("index", "job_progress_events_by_job_sequence"),
            ("index", "job_progress_events_by_attempt"),
            ("trigger", "job_checkpoints_no_update"),
            ("trigger", "job_checkpoints_no_delete"),
            ("trigger", "job_progress_events_no_update"),
            ("trigger", "job_progress_events_no_delete"),
        ] {
            let mut rows = connection
                .query(
                    "SELECT 1 FROM sqlite_schema WHERE type = ?1 AND name = ?2",
                    (object_type, object_name),
                )
                .await?;
            assert!(
                rows.next().await?.is_some(),
                "missing {object_type} {object_name}"
            );
        }

        connection
            .execute_batch(
                "
                INSERT INTO jobs (id, kind, priority, state, parent_job_id, crawl_run_id, scheduled_at, current_attempt, max_attempts, lease_id, lease_owner, lease_generation, lease_acquired_at, lease_expires_at, heartbeat_at, failure_code, created_at, updated_at)
                VALUES ('job-1', 'TEST', 0, 'QUEUED', NULL, NULL, 0, 0, 1, NULL, NULL, 0, NULL, NULL, NULL, NULL, 0, 0);
                INSERT INTO job_checkpoints (id, job_id, attempt_id, checkpoint_json, created_at)
                VALUES ('checkpoint-1', 'job-1', NULL, '{}', 0);
                INSERT INTO job_progress_events (id, job_id, attempt_id, sequence, event_type, payload_json, created_at)
                VALUES ('event-1', 'job-1', NULL, 1, 'STATUS', '{}', 0);
                ",
            )
            .await?;
        assert!(
            connection
                .execute_batch(
                    "UPDATE job_checkpoints SET checkpoint_json = '{}' WHERE id = 'checkpoint-1'"
                )
                .await
                .is_err()
        );
        assert!(
            connection
                .execute_batch("DELETE FROM job_progress_events WHERE id = 'event-1'")
                .await
                .is_err()
        );
        Ok(())
    }
}
