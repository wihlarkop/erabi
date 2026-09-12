//! Bounded SQLite persistence adapters for Erabi domain contracts.

mod artifact_store;
mod configuration;
mod integrity;
mod migrate;
pub mod repositories;
mod worker;

use std::{path::Path, sync::Arc};

use erabi_domain::VersionValidationRegistry;

const DB_WORKER_CAPACITY: usize = 4;
const DB_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

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

/// The semantic class of a database operation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseFailure {
    Busy,
    BusySnapshot,
    Full,
    ReadOnly,
    ConstraintViolation,
    NotADatabase,
    Corrupt,
    Interrupted,
    Conversion,
    Other,
}

/// Failure caused by the database worker infrastructure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerFailureKind {
    ShuttingDown,
    Panicked,
}

/// Errors exposed by the persistence adapter boundary.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("database operation failed: {0:?}")]
    Database(DatabaseFailure),
    #[error("database worker failed: {0:?}")]
    Worker(WorkerFailureKind),
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
    #[must_use]
    pub const fn is_durable_invariant(&self) -> bool {
        match self {
            Self::Worker(WorkerFailureKind::ShuttingDown)
            | Self::Database(
                DatabaseFailure::Busy
                | DatabaseFailure::BusySnapshot
                | DatabaseFailure::Full
                | DatabaseFailure::ReadOnly
                | DatabaseFailure::Interrupted,
            ) => false,
            Self::Database(
                DatabaseFailure::ConstraintViolation
                | DatabaseFailure::NotADatabase
                | DatabaseFailure::Corrupt
                | DatabaseFailure::Conversion
                | DatabaseFailure::Other,
            )
            | Self::Serialization(_)
            | Self::MigrationFailure { .. }
            | Self::Invariant(_)
            | Self::Worker(WorkerFailureKind::Panicked) => true,
        }
    }
}

impl From<rusqlite::Error> for DbError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(classify_rusqlite_error(&error))
    }
}

impl From<worker::WorkerFailure> for DbError {
    fn from(failure: worker::WorkerFailure) -> Self {
        Self::Worker(match failure {
            worker::WorkerFailure::ShuttingDown => WorkerFailureKind::ShuttingDown,
            worker::WorkerFailure::Panicked => WorkerFailureKind::Panicked,
        })
    }
}

fn classify_rusqlite_error(error: &rusqlite::Error) -> DatabaseFailure {
    match error {
        rusqlite::Error::SqliteFailure(error, _) => match error.extended_code {
            517 => DatabaseFailure::BusySnapshot,
            code if code & 0xff == rusqlite::ffi::SQLITE_BUSY => DatabaseFailure::Busy,
            code if code & 0xff == rusqlite::ffi::SQLITE_FULL => DatabaseFailure::Full,
            code if code & 0xff == rusqlite::ffi::SQLITE_READONLY => DatabaseFailure::ReadOnly,
            code if code & 0xff == rusqlite::ffi::SQLITE_CONSTRAINT => {
                DatabaseFailure::ConstraintViolation
            }
            code if code & 0xff == rusqlite::ffi::SQLITE_NOTADB => DatabaseFailure::NotADatabase,
            code if code & 0xff == rusqlite::ffi::SQLITE_CORRUPT => DatabaseFailure::Corrupt,
            code if code & 0xff == rusqlite::ffi::SQLITE_INTERRUPT => DatabaseFailure::Interrupted,
            _ => DatabaseFailure::Other,
        },
        rusqlite::Error::FromSqlConversionFailure(..)
        | rusqlite::Error::IntegralValueOutOfRange(..)
        | rusqlite::Error::ToSqlConversionFailure(..)
        | rusqlite::Error::Utf8Error(..)
        | rusqlite::Error::InvalidPath(..) => DatabaseFailure::Conversion,
        _ => DatabaseFailure::Other,
    }
}

/// The only database-handle type exposed by `erabi-db`.
#[derive(Clone)]
pub struct ErabiDatabase {
    inner: Arc<DatabaseInner>,
    validation_registry: Arc<VersionValidationRegistry>,
}

struct DatabaseInner {
    handle: worker::DbWorkerHandle,
    worker: std::sync::Mutex<Option<worker::DbWorker>>,
}

impl Drop for DatabaseInner {
    fn drop(&mut self) {
        // The final inner owner defensively drops the unique worker owner.
        // DbWorker::Drop closes admission without joining, so an idle worker
        // cannot remain parked after the last ErabiDatabase clone is gone.
        match self.worker.lock() {
            Ok(mut owner) => drop(owner.take()),
            Err(poisoned) => drop(poisoned.into_inner().take()),
        }
    }
}

impl std::fmt::Debug for ErabiDatabase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ErabiDatabase")
            .field("validation_registry", &self.validation_registry)
            .finish_non_exhaustive()
    }
}

impl ErabiDatabase {
    /// Opens a local SQLite database at a controlled path.
    ///
    /// # Errors
    /// Returns a database error when the local database cannot be opened.
    pub async fn open_local(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path = path.as_ref().to_string_lossy().into_owned();
        let worker = tokio::task::spawn_blocking(move || {
            worker::DbWorker::start_local(DB_WORKER_CAPACITY, path, initialize_connection)
        })
        .await
        .map_err(|_| DbError::Invariant("database worker startup task panicked".into()))?
        .map_err(|_| DbError::Invariant("database worker startup failed".into()))?;
        Ok(Self::from_worker(worker))
    }

    /// Opens an isolated in-memory database for tests and bounded probes.
    ///
    /// # Errors
    /// Returns a database error when the database cannot be opened.
    pub async fn in_memory() -> Result<Self, DbError> {
        let worker = tokio::task::spawn_blocking(move || {
            worker::DbWorker::start(DB_WORKER_CAPACITY, initialize_connection)
        })
        .await
        .map_err(|_| DbError::Invariant("database worker startup task panicked".into()))?
        .map_err(|_| DbError::Invariant("database worker startup failed".into()))?;
        Ok(Self::from_worker(worker))
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

    fn from_worker(worker: worker::DbWorker) -> Self {
        let handle = worker.handle();
        Self {
            inner: Arc::new(DatabaseInner {
                handle,
                worker: std::sync::Mutex::new(Some(worker)),
            }),
            validation_registry: Arc::new(VersionValidationRegistry::new()),
        }
    }

    pub(crate) async fn call<T, E, F>(&self, operation: F) -> Result<T, E>
    where
        T: Send + 'static,
        E: Send + 'static + From<DbError>,
        F: FnOnce(&mut rusqlite::Connection) -> Result<T, E> + Send + 'static,
    {
        match self.inner.handle.call(operation).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(error),
            Err(failure) => Err(E::from(failure.into())),
        }
    }

    #[cfg(test)]
    fn install_timing_observer(&self, observer: Arc<worker::TestTimingObserver>) {
        let owner = match self.inner.worker.lock() {
            Ok(owner) => owner,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(worker) = owner.as_ref() {
            worker.install_timing_observer(observer);
        }
    }
}

#[cfg(test)]
pub(crate) async fn test_call<T, F>(database: &ErabiDatabase, operation: F) -> Result<T, DbError>
where
    T: Send + 'static,
    F: FnOnce(&mut rusqlite::Connection) -> Result<T, rusqlite::Error> + Send + 'static,
{
    database
        .call(move |connection| operation(connection).map_err(DbError::from))
        .await
}

fn initialize_connection(connection: &mut rusqlite::Connection) -> Result<(), rusqlite::Error> {
    connection.busy_timeout(DB_BUSY_TIMEOUT)?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    let foreign_keys: i64 =
        connection.pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
    if foreign_keys != 1 {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(())
}

/// Synchronous, worker-confined SQLite view used by repository code. Query
/// results are materialized before the worker operation returns so no SQLite
/// borrow can escape the worker boundary.
pub(crate) struct SqliteConnection<'connection> {
    connection: &'connection mut rusqlite::Connection,
}

pub(crate) use rusqlite::types::Value as SqliteValue;

impl<'connection> SqliteConnection<'connection> {
    pub(crate) fn new(connection: &'connection mut rusqlite::Connection) -> Self {
        Self { connection }
    }

    pub(crate) fn execute<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<usize, rusqlite::Error> {
        self.connection.execute(sql, params)
    }

    pub(crate) fn execute_batch(&self, sql: &str) -> Result<(), rusqlite::Error> {
        self.connection.execute_batch(sql)
    }

    pub(crate) fn prepare<'statement>(
        &'statement self,
        sql: &str,
    ) -> Result<SqliteStatement<'statement>, rusqlite::Error> {
        Ok(SqliteStatement {
            statement: self.connection.prepare(sql)?,
        })
    }

    pub(crate) fn query<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<SqliteRows, rusqlite::Error> {
        let mut statement = self.connection.prepare(sql)?;
        materialize_rows(&mut statement, params)
    }

    pub(crate) fn transaction_with_behavior(
        &mut self,
        behavior: rusqlite::TransactionBehavior,
    ) -> Result<SqliteTransaction<'_>, rusqlite::Error> {
        Ok(SqliteTransaction {
            transaction: self.connection.transaction_with_behavior(behavior)?,
        })
    }
}

pub(crate) struct SqliteTransaction<'connection> {
    transaction: rusqlite::Transaction<'connection>,
}

impl SqliteTransaction<'_> {
    pub(crate) fn execute<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<usize, rusqlite::Error> {
        self.transaction.execute(sql, params)
    }

    pub(crate) fn execute_batch(&self, sql: &str) -> Result<(), rusqlite::Error> {
        self.transaction.execute_batch(sql)
    }

    pub(crate) fn prepare<'statement>(
        &'statement self,
        sql: &str,
    ) -> Result<SqliteStatement<'statement>, rusqlite::Error> {
        Ok(SqliteStatement {
            statement: self.transaction.prepare(sql)?,
        })
    }

    pub(crate) fn query<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<SqliteRows, rusqlite::Error> {
        let mut statement = self.transaction.prepare(sql)?;
        materialize_rows(&mut statement, params)
    }

    pub(crate) fn commit(self) -> Result<(), rusqlite::Error> {
        self.transaction.commit()
    }

    pub(crate) fn rollback(self) -> Result<(), rusqlite::Error> {
        self.transaction.rollback()
    }
}

/// The private query surface shared by a live connection and a transaction.
/// This is a SQLite implementation detail, not a public backend abstraction.
pub(crate) trait SqliteExecutor {
    fn execute<P: rusqlite::Params>(&self, sql: &str, params: P) -> Result<usize, rusqlite::Error>;

    fn prepare<'statement>(
        &'statement self,
        sql: &str,
    ) -> Result<SqliteStatement<'statement>, rusqlite::Error>;

    fn query<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<SqliteRows, rusqlite::Error>;
}

impl SqliteExecutor for SqliteConnection<'_> {
    fn execute<P: rusqlite::Params>(&self, sql: &str, params: P) -> Result<usize, rusqlite::Error> {
        SqliteConnection::execute(self, sql, params)
    }

    fn prepare<'statement>(
        &'statement self,
        sql: &str,
    ) -> Result<SqliteStatement<'statement>, rusqlite::Error> {
        SqliteConnection::prepare(self, sql)
    }

    fn query<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<SqliteRows, rusqlite::Error> {
        SqliteConnection::query(self, sql, params)
    }
}

impl SqliteExecutor for SqliteTransaction<'_> {
    fn execute<P: rusqlite::Params>(&self, sql: &str, params: P) -> Result<usize, rusqlite::Error> {
        SqliteTransaction::execute(self, sql, params)
    }

    fn prepare<'statement>(
        &'statement self,
        sql: &str,
    ) -> Result<SqliteStatement<'statement>, rusqlite::Error> {
        SqliteTransaction::prepare(self, sql)
    }

    fn query<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<SqliteRows, rusqlite::Error> {
        SqliteTransaction::query(self, sql, params)
    }
}

pub(crate) struct SqliteStatement<'statement> {
    statement: rusqlite::Statement<'statement>,
}

impl SqliteStatement<'_> {
    pub(crate) fn query_row<P: rusqlite::Params>(
        &mut self,
        params: P,
    ) -> Result<SqliteRow, rusqlite::Error> {
        let mut rows = self.statement.query(params)?;
        let Some(row) = rows.next()? else {
            return Err(rusqlite::Error::QueryReturnedNoRows);
        };
        materialize_row(row)
    }
}

pub(crate) struct SqliteRows {
    rows: Vec<SqliteRow>,
    next: usize,
}

impl SqliteRows {
    // The adapter deliberately retains rusqlite's fallible row-iteration shape
    // while rows are already materialized, keeping repository conversion code
    // uniform across connection and transaction queries.
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn next(&mut self) -> Result<Option<&SqliteRow>, rusqlite::Error> {
        let row = self.rows.get(self.next);
        if row.is_some() {
            self.next += 1;
        }
        Ok(row)
    }
}

#[derive(Clone)]
pub(crate) struct SqliteRow {
    values: Vec<rusqlite::types::Value>,
}

impl SqliteRow {
    pub(crate) fn get<T: rusqlite::types::FromSql>(
        &self,
        index: usize,
    ) -> Result<T, rusqlite::Error> {
        let value = self
            .values
            .get(index)
            .ok_or(rusqlite::Error::InvalidColumnIndex(index))?;
        T::column_result(rusqlite::types::ValueRef::from(value)).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(index, value.data_type(), Box::new(error))
        })
    }
}

fn materialize_rows<P: rusqlite::Params>(
    statement: &mut rusqlite::Statement<'_>,
    params: P,
) -> Result<SqliteRows, rusqlite::Error> {
    let mut rows = statement.query(params)?;
    let mut materialized = Vec::new();
    while let Some(row) = rows.next()? {
        materialized.push(materialize_row(row)?);
    }
    Ok(SqliteRows {
        rows: materialized,
        next: 0,
    })
}

fn materialize_row(row: &rusqlite::Row<'_>) -> Result<SqliteRow, rusqlite::Error> {
    let mut values = Vec::with_capacity(row.as_ref().column_count());
    for index in 0..row.as_ref().column_count() {
        let value = row.get_ref(index)?;
        values.push(rusqlite::types::Value::try_from(value).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(index, value.data_type(), Box::new(error))
        })?);
    }
    Ok(SqliteRow { values })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        DatabaseFailure, DbError, ErabiDatabase, MigrationRunner, VersionValidationRegistry,
        WorkerFailureKind, classify_rusqlite_error, test_call, worker,
    };

    #[test]
    fn backend_failures_preserve_durable_classification() {
        for failure in [
            DatabaseFailure::ConstraintViolation,
            DatabaseFailure::NotADatabase,
            DatabaseFailure::Corrupt,
            DatabaseFailure::Conversion,
            DatabaseFailure::Other,
        ] {
            assert!(DbError::Database(failure).is_durable_invariant());
        }
    }

    #[test]
    fn operational_failures_remain_non_durable() {
        for failure in [
            DatabaseFailure::Busy,
            DatabaseFailure::BusySnapshot,
            DatabaseFailure::Full,
            DatabaseFailure::ReadOnly,
            DatabaseFailure::Interrupted,
        ] {
            assert!(!DbError::Database(failure).is_durable_invariant());
        }
    }

    #[test]
    fn busy_snapshot_extended_code_precedes_primary_busy() {
        let error = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ffi::ErrorCode::DatabaseBusy,
                extended_code: 517,
            },
            None,
        );
        assert_eq!(
            classify_rusqlite_error(&error),
            DatabaseFailure::BusySnapshot
        );
    }

    #[test]
    fn sqlite_primary_codes_map_to_backend_neutral_failures() {
        let cases = [
            (rusqlite::ffi::SQLITE_BUSY, DatabaseFailure::Busy),
            (rusqlite::ffi::SQLITE_FULL, DatabaseFailure::Full),
            (rusqlite::ffi::SQLITE_READONLY, DatabaseFailure::ReadOnly),
            (
                rusqlite::ffi::SQLITE_CONSTRAINT,
                DatabaseFailure::ConstraintViolation,
            ),
            (rusqlite::ffi::SQLITE_NOTADB, DatabaseFailure::NotADatabase),
            (rusqlite::ffi::SQLITE_CORRUPT, DatabaseFailure::Corrupt),
            (
                rusqlite::ffi::SQLITE_INTERRUPT,
                DatabaseFailure::Interrupted,
            ),
            (rusqlite::ffi::SQLITE_LOCKED, DatabaseFailure::Other),
            (0x7fff, DatabaseFailure::Other),
        ];
        for (code, expected) in cases {
            let error = rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::DatabaseBusy,
                    extended_code: code,
                },
                None,
            );
            assert_eq!(classify_rusqlite_error(&error), expected);
        }
    }

    #[test]
    fn conversion_errors_map_to_conversion_failure() {
        let error = rusqlite::Error::IntegralValueOutOfRange(0, i64::MAX);
        assert_eq!(classify_rusqlite_error(&error), DatabaseFailure::Conversion);
    }

    #[tokio::test]
    async fn startup_configures_explicit_busy_timeout_and_foreign_keys()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = ErabiDatabase::in_memory().await?;
        let settings = test_call(&database, |connection| {
            let busy_timeout: i64 =
                connection.query_row("PRAGMA busy_timeout", [], |row| row.get(0))?;
            let foreign_keys: i64 =
                connection.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
            Ok((busy_timeout, foreign_keys))
        })
        .await?;
        assert_eq!(settings, (100, 1));
        Ok(())
    }

    #[tokio::test]
    async fn cloned_in_memory_databases_share_one_persistence_worker()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = ErabiDatabase::in_memory().await?;
        let second = first.clone();
        test_call(&first, |connection| {
            connection.execute_batch("CREATE TABLE shared_state (value INTEGER NOT NULL)")
        })
        .await?;
        test_call(&second, |connection| {
            connection.execute("INSERT INTO shared_state (value) VALUES (7)", [])
        })
        .await?;
        let value: i64 = test_call(&first, |connection| {
            connection.query_row("SELECT value FROM shared_state", [], |row| row.get(0))
        })
        .await?;
        assert_eq!(value, 7);
        Ok(())
    }

    #[tokio::test]
    async fn dropping_sibling_clone_keeps_surviving_database_usable()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = ErabiDatabase::in_memory().await?;
        let sibling = first.clone();
        test_call(&first, |connection| {
            connection.execute_batch("CREATE TABLE surviving_state (value INTEGER NOT NULL)")
        })
        .await?;
        test_call(&sibling, |connection| {
            connection.execute("INSERT INTO surviving_state (value) VALUES (7)", [])
        })
        .await?;

        drop(sibling);

        test_call(&first, |connection| {
            connection.execute("INSERT INTO surviving_state (value) VALUES (11)", [])
        })
        .await?;
        let values: Vec<i64> = test_call(&first, |connection| {
            let mut statement =
                connection.prepare("SELECT value FROM surviving_state ORDER BY value")?;
            statement
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()
        })
        .await?;
        assert_eq!(values, vec![7, 11]);
        Ok(())
    }

    #[tokio::test]
    async fn local_open_preserves_existing_wal_database_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("erabi.db");
        let source = rusqlite::Connection::open(&path)?;
        source.pragma_update(None, "journal_mode", "WAL")?;
        source.execute_batch(
            "CREATE TABLE wal_probe (value INTEGER NOT NULL); INSERT INTO wal_probe VALUES (41);",
        )?;
        let source_mode: String =
            source.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
        assert_eq!(source_mode.to_ascii_lowercase(), "wal");

        let database = ErabiDatabase::open_local(&path).await?;
        let (mode, value): (String, i64) = test_call(&database, |connection| {
            let mode: String =
                connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
            let value =
                connection.query_row("SELECT value FROM wal_probe", [], |row| row.get(0))?;
            Ok((mode, value))
        })
        .await?;
        assert_eq!(mode.to_ascii_lowercase(), "wal");
        assert_eq!(value, 41);
        Ok(())
    }

    #[tokio::test]
    async fn validation_registry_isolated_per_database_handle()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = ErabiDatabase::in_memory().await?;
        let sibling = first.clone();
        let first_registry = std::sync::Arc::clone(&first.validation_registry);
        let second = sibling.with_version_validation_registry(VersionValidationRegistry::new());
        assert!(std::sync::Arc::ptr_eq(
            &first.validation_registry,
            &first_registry
        ));
        assert!(!std::sync::Arc::ptr_eq(
            &first.validation_registry,
            &second.validation_registry
        ));

        test_call(&first, |connection| {
            connection.execute_batch("CREATE TABLE registry_state (value INTEGER NOT NULL)")
        })
        .await?;
        test_call(&second, |connection| {
            connection.execute("INSERT INTO registry_state (value) VALUES (11)", [])
        })
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::too_many_lines)]
    async fn production_cutover_latency_acceptance_measurement()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::collections::BTreeMap;

        use erabi_domain::{
            CrawlRunId, CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunStatus, CrawlRunType,
            ResolvedValue, RobotsAudit, RunConfiguration, SettingSource,
            SnapshotOperationalSettings,
        };

        use crate::{
            repositories::{
                CheckpointEnvelope, CheckpointIdentity, CheckpointPayloadKind,
                CheckpointRepository, CrawlRunRepository, CrawlTraversalRepository, JobKind,
                JobRepository, NewJob, NewProgressEvent, ProgressAttemptId, ProgressKey,
                ProgressMetadata, ProgressRepository,
            },
            worker::TestTimingObserver,
        };

        fn setting<T>(value: T) -> ResolvedValue<T> {
            ResolvedValue {
                value,
                source: SettingSource::BuiltInDefault,
            }
        }

        let database = ErabiDatabase::in_memory().await?;
        MigrationRunner::default().apply(&database).await?;

        let run_id = CrawlRunId::new();
        let snapshot = CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
            run_type: CrawlRunType::QuickScrape,
            configuration: RunConfiguration::QuickScrape {
                target_url: "https://example.test/".parse()?,
                ad_hoc_configuration: BTreeMap::new(),
            },
            selected_seed_ids: Vec::new(),
            run_profile_id: None,
            settings: SnapshotOperationalSettings {
                max_pages: setting(10),
                max_depth: setting(1),
                max_duration_seconds: setting(30),
                concurrency: setting(1),
                request_delay_ms: setting(0),
                timeout_ms: setting(1_000),
                screenshot: setting(false),
                asset_download_limit_bytes: setting(1_000),
                retain_artifacts: setting(false),
                user_agent: setting("Erabi/latency".to_owned()),
            },
            robots: RobotsAudit::respect(
                "latency-test",
                "2026-09-12T00:00:00Z",
                "https://example.test",
                "Erabi/latency",
                None,
            ),
            actor: "latency-test".to_owned(),
            created_at: "2026-09-12T00:00:00Z".to_owned(),
        })?;
        CrawlRunRepository::new(&database)
            .create(run_id, CrawlRunStatus::Queued, &snapshot)
            .await
            .map_err(|error| std::io::Error::other(format!("run setup failed: {error:?}")))?;
        test_call(&database, move |connection| {
            connection.execute(
                "INSERT INTO crawl_traversal_control (crawl_run_id,consumed_bytes,raw_link_count,duplicate_count,robots_excluded_count,peak_expansion_count,elapsed_millis,time_budget_hit,duration_work_not_expanded,pagination_truncation_count,next_admission_sequence,external_url_count,blocked_url_count,provider_error_count) VALUES (?1,0,0,0,0,0,0,0,0,0,1,0,0,0)",
                [run_id.to_string()],
            )
        })
        .await?;

        let jobs = JobRepository::new(&database);
        let job = NewJob::new(JobKind::new("LATENCY_PROBE")?, 0, 0, 1)?;
        jobs.enqueue(&job, 0).await?;

        let observer = Arc::new(TestTimingObserver::new());
        database.install_timing_observer(Arc::clone(&observer));

        let acquired = jobs
            .acquire_next("latency-worker", 1, 30)
            .await?
            .ok_or("latency probe job was not acquired")?;
        let lease = acquired
            .job
            .lease
            .clone()
            .ok_or("latency probe lease was not created")?;
        let lease = jobs.heartbeat(&job.id, &lease, 2, 30).await?;

        let event = NewProgressEvent::new(
            job.id.clone(),
            ProgressKey::new("PROGRESS")?,
            ProgressMetadata::default(),
        )
        .with_attempt(ProgressAttemptId::new(acquired.attempt.id.clone())?);
        ProgressRepository::new(&database)
            .append_at(&event, 2)
            .await?;

        let checkpoint = CheckpointEnvelope::new(
            CheckpointIdentity::new("latency", "a".repeat(64), "b".repeat(64))?,
            CheckpointPayloadKind::new("LATENCY")?,
            serde_json::json!({"cursor": 1}),
        )?;
        CheckpointRepository::new(&database)
            .append(&job.id, &acquired.attempt.id, &lease, &checkpoint, 2)
            .await?;

        CrawlTraversalRepository::new(&database)
            .read_traversal_control(run_id)
            .await
            .map_err(|error| std::io::Error::other(format!("traversal read failed: {error:?}")))?;
        jobs.succeed(&job.id, &lease, 3).await?;

        let observer_for_wait = Arc::clone(&observer);
        let samples =
            tokio::task::spawn_blocking(move || observer_for_wait.wait_for_samples(6)).await?;
        assert_eq!(samples.len(), 6);
        print_latency_samples(&samples);
        Ok(())
    }

    fn print_latency_samples(samples: &[worker::TimingSample]) {
        fn micros(sample: worker::TimingSample) -> (u128, u128, u128) {
            (
                sample.queue_wait.as_micros(),
                sample.service_time.as_micros(),
                sample.end_to_end.as_micros(),
            )
        }

        let values: Vec<_> = samples.iter().copied().map(micros).collect();
        println!("latency_samples={values:?}");
    }

    #[test]
    fn worker_panics_are_durable_but_shutdown_is_not() {
        assert!(!DbError::Worker(WorkerFailureKind::ShuttingDown).is_durable_invariant());
        assert!(DbError::Worker(WorkerFailureKind::Panicked).is_durable_invariant());
    }
}
