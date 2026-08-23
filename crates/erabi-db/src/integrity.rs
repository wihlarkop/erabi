//! Read-only startup integrity checks for the implemented Plan 01-04 surface.

use std::{fs, path::Path};

use crate::{ErabiDatabase, MigrationRunner, repositories::ConfigurationRepository};

const CRITICAL_TABLES: &[&str] = &[
    "schema_migrations",
    "migration_lock",
    "system_metadata",
    "settings",
    "audit_events",
    "local_data_owners",
    "persisted_destinations",
    "collections",
    "sources",
    "crawlers",
    "crawler_versions",
    "seeds",
    "page_types",
    "url_matchers",
    "discovery_transitions",
    "run_profiles",
    "test_evidence",
    "crawl_runs",
    "discovered_urls",
    "artifacts",
    "jobs",
    "job_attempts",
    "job_checkpoints",
    "job_progress_events",
];

const CRITICAL_INDEXES: &[&str] = &[
    "crawler_versions_by_crawler",
    "sources_by_collection",
    "crawl_runs_by_created_at",
    "discovered_urls_by_run",
    "artifacts_by_hash",
    "jobs_ready_by_schedule",
    "jobs_running_by_lease_expiry",
    "jobs_by_parent",
    "jobs_by_crawl_run",
    "job_attempts_by_job",
    "job_attempts_running_by_job",
    "job_checkpoints_by_job",
    "job_checkpoints_by_attempt",
    "job_progress_events_by_job_sequence",
    "job_progress_events_by_attempt",
];

const CRITICAL_TRIGGERS: &[&str] = &[
    "crawler_versions_published_no_update",
    "crawler_versions_published_no_delete",
    "seeds_published_version_no_insert",
    "seeds_published_version_no_update",
    "seeds_published_version_no_delete",
    "page_types_published_version_no_insert",
    "page_types_published_version_no_update",
    "page_types_published_version_no_delete",
    "url_matchers_published_version_no_insert",
    "url_matchers_published_version_no_update",
    "url_matchers_published_version_no_delete",
    "discovery_transitions_published_version_no_insert",
    "discovery_transitions_published_version_no_update",
    "discovery_transitions_published_version_no_delete",
    "crawl_runs_snapshot_immutable",
    "jobs_must_start_queued",
    "jobs_legal_state_transition",
    "jobs_lease_state_consistency",
    "job_attempts_terminal_history_immutable",
    "job_attempts_no_delete",
    "job_checkpoints_no_update",
    "job_checkpoints_no_delete",
    "job_progress_events_no_update",
    "job_progress_events_no_delete",
];

/// Stable, secret-free failures surfaced through Recovery Mode diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LightweightIntegrityError {
    /// The database could not perform a simple read-only query.
    #[error("the internal database is not readable")]
    DatabaseUnreadable,
    /// Recorded migrations do not match the bundled Plan 02 chain.
    #[error("recorded migrations are incompatible with the bundled schema chain")]
    MigrationStateIncompatible,
    /// A table, index, or trigger required for the implemented invariants is absent.
    #[error("a critical schema object required by Erabi is missing")]
    CriticalSchemaObjectMissing,
    /// A Crawler active-version pointer does not refer to a matching version/state.
    #[error("a Crawler active-version pointer is inconsistent")]
    ActiveVersionPointerInconsistent,
    /// Persisted settings or destination records cannot pass their validated boundary.
    #[error("a persisted configuration record is invalid")]
    PersistedConfigurationInvalid,
    /// The existing artifact root is not a controlled accessible directory.
    #[error("the controlled artifact root is inaccessible or unsafe")]
    ArtifactRootUnsafe,
    /// Durable lease or attempt history could permit unsafe job scheduling.
    #[error("durable job ownership or attempt history is inconsistent")]
    QueueInvariantInconsistent,
}

impl LightweightIntegrityError {
    /// Stable Recovery Mode diagnostic code without object names or stored values.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::DatabaseUnreadable => "DATABASE_UNREADABLE",
            Self::MigrationStateIncompatible => "MIGRATION_STATE_INCOMPATIBLE",
            Self::CriticalSchemaObjectMissing => "CRITICAL_SCHEMA_OBJECT_MISSING",
            Self::ActiveVersionPointerInconsistent => "ACTIVE_VERSION_POINTER_INCONSISTENT",
            Self::PersistedConfigurationInvalid => "PERSISTED_CONFIGURATION_INVALID",
            Self::ArtifactRootUnsafe => "ARTIFACT_ROOT_UNSAFE",
            Self::QueueInvariantInconsistent => "QUEUE_INVARIANT_VIOLATION",
        }
    }

    /// Safe message for the limited Recovery Mode surface.
    #[must_use]
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::DatabaseUnreadable => {
                "The internal database could not complete a required read-only check."
            }
            Self::MigrationStateIncompatible => {
                "Recorded migrations are incompatible with this Erabi version."
            }
            Self::CriticalSchemaObjectMissing => {
                "A critical database schema object required for safety is missing."
            }
            Self::ActiveVersionPointerInconsistent => {
                "A Crawler active-version pointer is inconsistent and requires recovery."
            }
            Self::PersistedConfigurationInvalid => {
                "A persisted configuration record is invalid and requires recovery."
            }
            Self::ArtifactRootUnsafe => "The controlled artifact root is inaccessible or unsafe.",
            Self::QueueInvariantInconsistent => {
                "Durable job ownership or attempt history is inconsistent and requires recovery."
            }
        }
    }
}

/// Bounded startup checker for Plan 01-03 persistence and artifact safety.
///
/// The checker performs only reads of the database schema/data and filesystem
/// metadata. It never repairs an object or changes product state.
#[derive(Clone, Copy, Debug)]
pub struct LightweightIntegrityChecker<'database, 'path> {
    database: &'database ErabiDatabase,
    migrations: &'database MigrationRunner,
    canonical_data_dir: &'path Path,
}

impl<'database, 'path> LightweightIntegrityChecker<'database, 'path> {
    /// Creates a checker rooted in an already canonical Erabi data directory.
    #[must_use]
    pub const fn new(
        database: &'database ErabiDatabase,
        migrations: &'database MigrationRunner,
        canonical_data_dir: &'path Path,
    ) -> Self {
        Self {
            database,
            migrations,
            canonical_data_dir,
        }
    }

    /// Performs the full lightweight startup integrity pass.
    ///
    /// # Errors
    /// Returns a stable Recovery Mode condition. The error deliberately omits
    /// raw SQL, stored values, paths, and secrets.
    pub async fn check(&self) -> Result<(), LightweightIntegrityError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(|_| LightweightIntegrityError::DatabaseUnreadable)?;
        let mut readable = connection
            .query("SELECT 1", ())
            .await
            .map_err(|_| LightweightIntegrityError::DatabaseUnreadable)?;
        readable
            .next()
            .await
            .map_err(|_| LightweightIntegrityError::DatabaseUnreadable)?;

        self.migrations
            .verify(self.database)
            .await
            .map_err(|_| LightweightIntegrityError::MigrationStateIncompatible)?;

        ensure_schema_objects(&connection, "table", CRITICAL_TABLES).await?;
        ensure_schema_objects(&connection, "index", CRITICAL_INDEXES).await?;
        ensure_schema_objects(&connection, "trigger", CRITICAL_TRIGGERS).await?;
        ensure_active_version_pointers(&connection).await?;
        ensure_job_queue_invariants(&connection).await?;
        ConfigurationRepository::new(self.database)
            .validate_all()
            .await
            .map_err(|_| LightweightIntegrityError::PersistedConfigurationInvalid)?;
        ensure_artifact_root(self.canonical_data_dir)?;
        Ok(())
    }
}

async fn ensure_job_queue_invariants(
    connection: &turso::Connection,
) -> Result<(), LightweightIntegrityError> {
    const INCONSISTENCIES: [&str; 5] = [
        "SELECT 1 FROM jobs AS job WHERE (job.state = 'RUNNING' AND (job.current_attempt = 0 OR job.lease_id IS NULL OR job.lease_owner IS NULL OR job.lease_generation = 0 OR job.lease_acquired_at IS NULL OR job.lease_expires_at IS NULL OR job.heartbeat_at IS NULL)) OR (job.state <> 'RUNNING' AND (job.lease_id IS NOT NULL OR job.lease_owner IS NOT NULL OR job.lease_acquired_at IS NOT NULL OR job.lease_expires_at IS NOT NULL OR job.heartbeat_at IS NOT NULL)) LIMIT 1",
        "SELECT 1 FROM jobs AS job LEFT JOIN job_attempts AS attempt ON attempt.job_id = job.id AND attempt.attempt_number = job.current_attempt AND attempt.outcome = 'RUNNING' WHERE job.state = 'RUNNING' AND (attempt.id IS NULL OR attempt.lease_id <> job.lease_id OR attempt.lease_generation <> job.lease_generation OR attempt.worker_id <> job.lease_owner) LIMIT 1",
        "SELECT 1 FROM jobs AS job JOIN job_attempts AS attempt ON attempt.job_id = job.id WHERE job.state <> 'RUNNING' AND attempt.outcome = 'RUNNING' LIMIT 1",
        "SELECT 1 FROM jobs AS job WHERE job.current_attempt <> COALESCE((SELECT MAX(attempt.attempt_number) FROM job_attempts AS attempt WHERE attempt.job_id = job.id), 0) LIMIT 1",
        "SELECT 1 FROM job_attempts AS attempt LEFT JOIN jobs AS job ON job.id = attempt.job_id WHERE job.id IS NULL OR attempt.attempt_number > job.max_attempts LIMIT 1",
    ];
    for query in INCONSISTENCIES {
        let mut rows = connection
            .query(query, ())
            .await
            .map_err(|_| LightweightIntegrityError::DatabaseUnreadable)?;
        if rows
            .next()
            .await
            .map_err(|_| LightweightIntegrityError::DatabaseUnreadable)?
            .is_some()
        {
            return Err(LightweightIntegrityError::QueueInvariantInconsistent);
        }
    }
    Ok(())
}

async fn ensure_schema_objects(
    connection: &turso::Connection,
    object_type: &str,
    expected_names: &[&str],
) -> Result<(), LightweightIntegrityError> {
    for name in expected_names {
        let mut rows = connection
            .query(
                "SELECT 1 FROM sqlite_schema WHERE type = ?1 AND name = ?2 LIMIT 1",
                (object_type, *name),
            )
            .await
            .map_err(|_| LightweightIntegrityError::DatabaseUnreadable)?;
        if rows
            .next()
            .await
            .map_err(|_| LightweightIntegrityError::DatabaseUnreadable)?
            .is_none()
        {
            return Err(LightweightIntegrityError::CriticalSchemaObjectMissing);
        }
    }
    Ok(())
}

async fn ensure_active_version_pointers(
    connection: &turso::Connection,
) -> Result<(), LightweightIntegrityError> {
    for query in [
        "SELECT 1 FROM crawlers AS crawler LEFT JOIN crawler_versions AS version ON version.id = crawler.active_draft_version_id WHERE crawler.active_draft_version_id IS NOT NULL AND (version.id IS NULL OR version.crawler_id <> crawler.id OR version.state <> 'DRAFT') LIMIT 1",
        "SELECT 1 FROM crawlers AS crawler LEFT JOIN crawler_versions AS version ON version.id = crawler.active_published_version_id WHERE crawler.active_published_version_id IS NOT NULL AND (version.id IS NULL OR version.crawler_id <> crawler.id OR version.state <> 'PUBLISHED') LIMIT 1",
    ] {
        let mut rows = connection
            .query(query, ())
            .await
            .map_err(|_| LightweightIntegrityError::DatabaseUnreadable)?;
        if rows
            .next()
            .await
            .map_err(|_| LightweightIntegrityError::DatabaseUnreadable)?
            .is_some()
        {
            return Err(LightweightIntegrityError::ActiveVersionPointerInconsistent);
        }
    }
    Ok(())
}

fn ensure_artifact_root(canonical_data_dir: &Path) -> Result<(), LightweightIntegrityError> {
    let root = canonical_data_dir.join("artifacts");
    let metadata = match fs::symlink_metadata(&root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(LightweightIntegrityError::ArtifactRootUnsafe),
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(LightweightIntegrityError::ArtifactRootUnsafe);
    }
    let canonical_root = root
        .canonicalize()
        .map_err(|_| LightweightIntegrityError::ArtifactRootUnsafe)?;
    if !canonical_root.starts_with(canonical_data_dir) {
        return Err(LightweightIntegrityError::ArtifactRootUnsafe);
    }
    fs::read_dir(canonical_root)
        .map(|mut entries| entries.next())
        .map_err(|_| LightweightIntegrityError::ArtifactRootUnsafe)?;
    Ok(())
}
