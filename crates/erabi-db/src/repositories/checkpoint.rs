//! Bounded, append-only checkpoint evidence for cooperative job recovery.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use turso::{Connection, transaction::TransactionBehavior};
use uuid::Uuid;

use super::job::{JobId, JobLease};
use crate::{DbError, ErabiDatabase};

/// The only checkpoint schema currently understood by the generic worker.
pub const CURRENT_CHECKPOINT_SCHEMA_VERSION: u16 = 1;
/// Maximum encoded checkpoint size persisted in one append-only row.
pub const MAX_CHECKPOINT_BYTES: usize = 64 * 1024;
/// Maximum number of unit identities across one checkpoint.
pub const MAX_CHECKPOINT_UNITS: usize = 1_024;
/// Maximum number of artifact references in one checkpoint.
pub const MAX_CHECKPOINT_ARTIFACTS: usize = 1_024;
const MAX_SNAPSHOT_ID_BYTES: usize = 128;
const MAX_UNIT_ID_BYTES: usize = 512;
const MAX_POSITION_KIND_BYTES: usize = 64;
const MAX_POSITION_VALUE_BYTES: usize = 8 * 1024;
const MAX_ARTIFACT_ID_BYTES: usize = 128;
const MAX_ATTEMPT_ID_BYTES: usize = 128;

/// Immutable run/configuration identity captured by a checkpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointIdentity {
    pub snapshot_id: String,
    pub snapshot_hash: String,
    pub compatibility_hash: String,
}

impl CheckpointIdentity {
    /// Creates a bounded identity containing the immutable snapshot hashes.
    ///
    /// # Errors
    /// Returns an error when an identity field is empty, oversized, or is not a
    /// SHA-256 hex hash where a hash is required.
    pub fn new(
        snapshot_id: impl Into<String>,
        snapshot_hash: impl Into<String>,
        compatibility_hash: impl Into<String>,
    ) -> Result<Self, CheckpointRepositoryError> {
        let identity = Self {
            snapshot_id: snapshot_id.into(),
            snapshot_hash: snapshot_hash.into(),
            compatibility_hash: compatibility_hash.into(),
        };
        identity.validate()?;
        Ok(identity)
    }

    fn validate(&self) -> Result<(), CheckpointRepositoryError> {
        bounded_non_empty(&self.snapshot_id, MAX_SNAPSHOT_ID_BYTES)?;
        valid_hash(&self.snapshot_hash)?;
        valid_hash(&self.compatibility_hash)
    }
}

/// Stable identity of one unit of future crawl/discovery work.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct CheckpointUnitId(String);

impl CheckpointUnitId {
    /// Creates a bounded opaque unit identity; it is not scraped content.
    ///
    /// # Errors
    /// Returns an error when the identity is empty or oversized.
    pub fn new(value: impl Into<String>) -> Result<Self, CheckpointRepositoryError> {
        let value = value.into();
        bounded_non_empty(&value, MAX_UNIT_ID_BYTES)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque but typed position for bounded discovery or pagination resume.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointPosition {
    pub kind: String,
    pub value: String,
}

impl CheckpointPosition {
    /// Creates a bounded typed position without accepting an arbitrary JSON
    /// object or request/response body.
    ///
    /// # Errors
    /// Returns an error when either field is empty or oversized.
    pub fn new(
        kind: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, CheckpointRepositoryError> {
        let position = Self {
            kind: kind.into(),
            value: value.into(),
        };
        position.validate()?;
        Ok(position)
    }

    fn validate(&self) -> Result<(), CheckpointRepositoryError> {
        bounded_non_empty(&self.kind, MAX_POSITION_KIND_BYTES)?;
        bounded_non_empty(&self.value, MAX_POSITION_VALUE_BYTES)
    }
}

/// Resume phase for extraction state that is still safe to continue.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExtractionResumePhase {
    NotStarted,
    InProgress,
    AwaitingValidation,
}

/// Typed extraction progress needed to avoid treating partial work as done.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtractionResumeState {
    pub phase: ExtractionResumePhase,
    pub active_unit: Option<CheckpointUnitId>,
    pub cursor: Option<CheckpointPosition>,
}

impl ExtractionResumeState {
    /// Creates an empty, explicitly not-started extraction state.
    #[must_use]
    pub const fn not_started() -> Self {
        Self {
            phase: ExtractionResumePhase::NotStarted,
            active_unit: None,
            cursor: None,
        }
    }

    fn validate(&self) -> Result<(), CheckpointRepositoryError> {
        if let Some(active_unit) = &self.active_unit {
            bounded_non_empty(active_unit.as_str(), MAX_UNIT_ID_BYTES)?;
        }
        if let Some(cursor) = &self.cursor {
            cursor.validate()?;
        }
        if self.phase == ExtractionResumePhase::NotStarted
            && (self.active_unit.is_some() || self.cursor.is_some())
        {
            return Err(CheckpointRepositoryError::InvalidEnvelope);
        }
        Ok(())
    }
}

/// Reference to an artifact already committed durably elsewhere.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointArtifactReference {
    pub artifact_id: String,
    pub content_hash: String,
}

impl CheckpointArtifactReference {
    /// Creates a bounded artifact reference; content itself is never stored.
    ///
    /// # Errors
    /// Returns an error when the artifact identity or hash is invalid.
    pub fn new(
        artifact_id: impl Into<String>,
        content_hash: impl Into<String>,
    ) -> Result<Self, CheckpointRepositoryError> {
        let reference = Self {
            artifact_id: artifact_id.into(),
            content_hash: content_hash.into(),
        };
        reference.validate()?;
        Ok(reference)
    }

    fn validate(&self) -> Result<(), CheckpointRepositoryError> {
        bounded_non_empty(&self.artifact_id, MAX_ARTIFACT_ID_BYTES)?;
        valid_hash(&self.content_hash)
    }
}

/// Generic bounded checkpoint envelope for future plan-specific typed payloads.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointEnvelope {
    pub schema_version: u16,
    pub identity: CheckpointIdentity,
    pub completed_units: Vec<CheckpointUnitId>,
    pub pending_units: Vec<CheckpointUnitId>,
    pub failed_units: Vec<CheckpointUnitId>,
    pub discovery_position: Option<CheckpointPosition>,
    pub artifact_references: Vec<CheckpointArtifactReference>,
    pub extraction: ExtractionResumeState,
}

impl CheckpointEnvelope {
    /// Creates an empty checkpoint for a specific immutable run snapshot.
    #[must_use]
    pub fn new(identity: CheckpointIdentity) -> Self {
        Self {
            schema_version: CURRENT_CHECKPOINT_SCHEMA_VERSION,
            identity,
            completed_units: Vec::new(),
            pending_units: Vec::new(),
            failed_units: Vec::new(),
            discovery_position: None,
            artifact_references: Vec::new(),
            extraction: ExtractionResumeState::not_started(),
        }
    }

    /// Validates typed bounds and serializes the envelope for durable storage.
    ///
    /// # Errors
    /// Returns a typed error when the envelope is malformed, contains duplicate
    /// unit identities, or exceeds the bounded storage limit.
    pub fn encode(&self) -> Result<String, CheckpointRepositoryError> {
        self.validate()?;
        let encoded =
            serde_json::to_string(self).map_err(|_| CheckpointRepositoryError::Serialization)?;
        if encoded.len() > MAX_CHECKPOINT_BYTES {
            return Err(CheckpointRepositoryError::PayloadTooLarge);
        }
        Ok(encoded)
    }

    /// Tests whether this checkpoint matches the current immutable identity.
    #[must_use]
    pub fn compatibility_with(&self, current: &CheckpointIdentity) -> CheckpointCompatibility {
        if self.identity == *current {
            CheckpointCompatibility::Compatible
        } else {
            CheckpointCompatibility::Incompatible
        }
    }

    fn validate(&self) -> Result<(), CheckpointRepositoryError> {
        if self.schema_version != CURRENT_CHECKPOINT_SCHEMA_VERSION {
            return Err(CheckpointRepositoryError::InvalidEnvelope);
        }
        self.identity.validate()?;
        let total_units = self
            .completed_units
            .len()
            .checked_add(self.pending_units.len())
            .and_then(|count| count.checked_add(self.failed_units.len()))
            .ok_or(CheckpointRepositoryError::InvalidEnvelope)?;
        if total_units > MAX_CHECKPOINT_UNITS {
            return Err(CheckpointRepositoryError::PayloadTooLarge);
        }
        let mut identities = BTreeSet::new();
        for unit in self
            .completed_units
            .iter()
            .chain(&self.pending_units)
            .chain(&self.failed_units)
        {
            bounded_non_empty(unit.as_str(), MAX_UNIT_ID_BYTES)?;
            if !identities.insert(unit.as_str()) {
                return Err(CheckpointRepositoryError::InvalidEnvelope);
            }
        }
        if let Some(position) = &self.discovery_position {
            position.validate()?;
        }
        if self.artifact_references.len() > MAX_CHECKPOINT_ARTIFACTS {
            return Err(CheckpointRepositoryError::PayloadTooLarge);
        }
        for artifact in &self.artifact_references {
            artifact.validate()?;
        }
        self.extraction.validate()
    }

    fn decode(value: &str, encoded_length: usize) -> Result<Self, CheckpointRepositoryError> {
        if encoded_length > MAX_CHECKPOINT_BYTES {
            return Err(CheckpointRepositoryError::PayloadTooLarge);
        }
        let checkpoint: Self =
            serde_json::from_str(value).map_err(|_| CheckpointRepositoryError::Malformed)?;
        checkpoint
            .validate()
            .map_err(|_| CheckpointRepositoryError::Inconsistent)?;
        Ok(checkpoint)
    }
}

/// Result of the deterministic immutable snapshot compatibility check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointCompatibility {
    Compatible,
    Incompatible,
}

/// Startup classification for one stale active job's latest checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointRecoveryDisposition {
    /// A valid checkpoint matches the current immutable run identity.
    Recoverable,
    /// The job may restart from the beginning, but no safe resume evidence is
    /// available or the immutable identity does not match.
    RestartRequired,
    /// Durable evidence is malformed or internally inconsistent and needs
    /// typed operator/recovery handling.
    Unsafe,
}

/// Typed startup assessment without exposing checkpoint payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointRecoveryAssessment {
    pub job_id: JobId,
    pub disposition: CheckpointRecoveryDisposition,
}

/// One append-only durable checkpoint record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointRecord {
    pub id: String,
    pub job_id: JobId,
    pub attempt_id: Option<String>,
    pub checkpoint: CheckpointEnvelope,
    pub created_at: i64,
}

/// Typed checkpoint failures that never include the raw payload.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointRepositoryError {
    #[error("the durable checkpoint database operation failed")]
    Database(#[source] DbError),
    #[error("the checkpoint envelope is invalid")]
    InvalidEnvelope,
    #[error("the checkpoint envelope exceeds the bounded storage limit")]
    PayloadTooLarge,
    #[error("the checkpoint evidence is malformed")]
    Malformed,
    #[error("the checkpoint evidence is internally inconsistent")]
    Inconsistent,
    #[error("checkpoint serialization failed")]
    Serialization,
    #[error("the requested job does not exist")]
    NotFound,
    #[error("the current worker no longer owns this checkpoint lease")]
    LeaseLost,
}

impl CheckpointRepositoryError {
    fn database(error: turso::Error) -> Self {
        Self::Database(DbError::from(error))
    }

    fn from_db(error: DbError) -> Self {
        Self::Database(error)
    }
}

/// Repository for append-only checkpoint evidence in the existing jobs schema.
#[derive(Clone, Copy, Debug)]
pub struct CheckpointRepository<'database> {
    database: &'database ErabiDatabase,
}

impl<'database> CheckpointRepository<'database> {
    #[must_use]
    pub const fn new(database: &'database ErabiDatabase) -> Self {
        Self { database }
    }

    /// Appends one checkpoint only while the supplied worker still owns the
    /// active attempt. The insert commits before the caller may mark work
    /// cancelled or resumable.
    ///
    /// # Errors
    /// Returns a typed validation, ownership, or durable persistence failure.
    pub async fn append(
        &self,
        job_id: &JobId,
        attempt_id: &str,
        lease: &JobLease,
        checkpoint: &CheckpointEnvelope,
        created_at: i64,
    ) -> Result<CheckpointRecord, CheckpointRepositoryError> {
        if attempt_id.is_empty() || attempt_id.len() > MAX_ATTEMPT_ID_BYTES {
            return Err(CheckpointRepositoryError::InvalidEnvelope);
        }
        let encoded = checkpoint.encode()?;
        let mut connection = self
            .database
            .connection()
            .await
            .map_err(CheckpointRepositoryError::from_db)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(CheckpointRepositoryError::database)?;
        let result = async {
            ensure_owned_attempt(&transaction, job_id, attempt_id, lease, created_at).await?;
            let id = Uuid::now_v7().to_string();
            transaction
                .execute(
                    "INSERT INTO job_checkpoints (id, job_id, attempt_id, checkpoint_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                    (id.as_str(), job_id.as_str(), attempt_id, encoded.as_str(), created_at),
                )
                .await
                .map_err(CheckpointRepositoryError::database)?;
            Ok(CheckpointRecord {
                id,
                job_id: job_id.clone(),
                attempt_id: Some(attempt_id.to_owned()),
                checkpoint: checkpoint.clone(),
                created_at,
            })
        }
        .await;
        match result {
            Ok(record) => transaction
                .commit()
                .await
                .map(|()| record)
                .map_err(CheckpointRepositoryError::database),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Returns all append-only checkpoint evidence in trusted-time order.
    ///
    /// # Errors
    /// Returns a typed malformed/inconsistent result instead of treating bad
    /// evidence as resumable.
    pub async fn records(
        &self,
        job_id: &JobId,
    ) -> Result<Vec<CheckpointRecord>, CheckpointRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(CheckpointRepositoryError::from_db)?;
        records_from_connection(&connection, job_id).await
    }

    /// Returns the latest checkpoint, if any, without hiding malformed or
    /// structurally inconsistent earlier evidence.
    ///
    /// # Errors
    /// Returns a typed error for malformed or inconsistent durable evidence.
    pub async fn latest(
        &self,
        job_id: &JobId,
    ) -> Result<Option<CheckpointRecord>, CheckpointRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(CheckpointRepositoryError::from_db)?;
        Ok(records_from_connection(&connection, job_id).await?.pop())
    }

    /// Classifies every expired active job using only durable checkpoint and
    /// immutable run identity evidence. This does not mutate queue state.
    ///
    /// # Errors
    /// Returns an error only when the database cannot be inspected. Malformed
    /// checkpoint rows are returned as [`CheckpointRecoveryDisposition::Unsafe`].
    pub async fn assess_stale_jobs(
        &self,
        now: i64,
    ) -> Result<Vec<CheckpointRecoveryAssessment>, CheckpointRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(CheckpointRepositoryError::from_db)?;
        let mut rows = connection
            .query(
                "SELECT id, crawl_run_id FROM jobs WHERE state = 'RUNNING' AND lease_expires_at <= ?1 ORDER BY id",
                [now],
            )
            .await
            .map_err(CheckpointRepositoryError::database)?;
        let mut jobs = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(CheckpointRepositoryError::database)?
        {
            let job_id =
                JobId::from_stored(row.get(0).map_err(CheckpointRepositoryError::database)?);
            let run_id: Option<String> = row.get(1).map_err(CheckpointRepositoryError::database)?;
            jobs.push((job_id, run_id));
        }
        drop(rows);

        let mut assessments = Vec::with_capacity(jobs.len());
        for (job_id, run_id) in jobs {
            let disposition = assess_one_stale_job(&connection, &job_id, run_id.as_deref()).await?;
            assessments.push(CheckpointRecoveryAssessment {
                job_id,
                disposition,
            });
        }
        Ok(assessments)
    }
}

async fn ensure_owned_attempt(
    connection: &Connection,
    job_id: &JobId,
    attempt_id: &str,
    lease: &JobLease,
    now: i64,
) -> Result<(), CheckpointRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT state, lease_id, lease_owner, lease_generation, lease_expires_at FROM jobs WHERE id = ?1",
            [job_id.as_str()],
        )
        .await
        .map_err(CheckpointRepositoryError::database)?;
    let row = rows
        .next()
        .await
        .map_err(CheckpointRepositoryError::database)?
        .ok_or(CheckpointRepositoryError::NotFound)?;
    let state: String = row.get(0).map_err(CheckpointRepositoryError::database)?;
    let lease_id: Option<String> = row.get(1).map_err(CheckpointRepositoryError::database)?;
    let lease_owner: Option<String> = row.get(2).map_err(CheckpointRepositoryError::database)?;
    let lease_generation: i64 = row.get(3).map_err(CheckpointRepositoryError::database)?;
    let lease_expires_at: Option<i64> = row.get(4).map_err(CheckpointRepositoryError::database)?;
    if state != "RUNNING"
        || lease_id.as_deref() != Some(lease.id.as_str())
        || lease_owner.as_deref() != Some(lease.owner.as_str())
        || lease_generation != i64::try_from(lease.generation).unwrap_or(i64::MIN)
        || lease_expires_at.is_none_or(|expires_at| expires_at <= now)
    {
        return Err(CheckpointRepositoryError::LeaseLost);
    }
    let mut attempts = connection
        .query(
            "SELECT 1 FROM job_attempts WHERE id = ?1 AND job_id = ?2 AND lease_id = ?3 AND lease_generation = ?4 AND worker_id = ?5 AND outcome = 'RUNNING'",
            (
                attempt_id,
                job_id.as_str(),
                lease.id.as_str(),
                i64::try_from(lease.generation).unwrap_or(i64::MIN),
                lease.owner.as_str(),
            ),
        )
        .await
        .map_err(CheckpointRepositoryError::database)?;
    if attempts
        .next()
        .await
        .map_err(CheckpointRepositoryError::database)?
        .is_none()
    {
        return Err(CheckpointRepositoryError::LeaseLost);
    }
    Ok(())
}

async fn assess_one_stale_job(
    connection: &Connection,
    job_id: &JobId,
    run_id: Option<&str>,
) -> Result<CheckpointRecoveryDisposition, CheckpointRepositoryError> {
    let records = match records_from_connection(connection, job_id).await {
        Ok(records) => records,
        Err(
            CheckpointRepositoryError::Malformed
            | CheckpointRepositoryError::Inconsistent
            | CheckpointRepositoryError::PayloadTooLarge,
        ) => return Ok(CheckpointRecoveryDisposition::Unsafe),
        Err(error) => return Err(error),
    };
    let Some(latest) = records.last() else {
        return Ok(CheckpointRecoveryDisposition::RestartRequired);
    };
    let Some(attempt_id) = latest.attempt_id.as_deref() else {
        return Ok(CheckpointRecoveryDisposition::Unsafe);
    };
    if !active_checkpoint_lineage_is_valid(connection, job_id, attempt_id).await? {
        return Ok(CheckpointRecoveryDisposition::Unsafe);
    }
    let Some(run_id) = run_id else {
        return Ok(CheckpointRecoveryDisposition::RestartRequired);
    };
    let mut rows = connection
        .query(
            "SELECT snapshot_hash, checkpoint_compatibility_hash FROM crawl_runs WHERE id = ?1",
            [run_id],
        )
        .await
        .map_err(CheckpointRepositoryError::database)?;
    let Some(row) = rows
        .next()
        .await
        .map_err(CheckpointRepositoryError::database)?
    else {
        return Ok(CheckpointRecoveryDisposition::Unsafe);
    };
    let snapshot_hash: String = row.get(0).map_err(CheckpointRepositoryError::database)?;
    let compatibility_hash: String = row.get(1).map_err(CheckpointRepositoryError::database)?;
    let Ok(current) = CheckpointIdentity::new(run_id, snapshot_hash, compatibility_hash) else {
        return Ok(CheckpointRecoveryDisposition::Unsafe);
    };
    Ok(match latest.checkpoint.compatibility_with(&current) {
        CheckpointCompatibility::Compatible => CheckpointRecoveryDisposition::Recoverable,
        CheckpointCompatibility::Incompatible => CheckpointRecoveryDisposition::RestartRequired,
    })
}

async fn active_checkpoint_lineage_is_valid(
    connection: &Connection,
    job_id: &JobId,
    attempt_id: &str,
) -> Result<bool, CheckpointRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT 1 FROM jobs AS job JOIN job_attempts AS attempt ON attempt.id = ?1 WHERE job.id = ?2 AND job.state = 'RUNNING' AND attempt.job_id = job.id AND attempt.attempt_number = job.current_attempt AND attempt.outcome = 'RUNNING' AND attempt.lease_id = job.lease_id AND attempt.lease_generation = job.lease_generation AND attempt.worker_id = job.lease_owner LIMIT 1",
            (attempt_id, job_id.as_str()),
        )
        .await
        .map_err(CheckpointRepositoryError::database)?;
    Ok(rows
        .next()
        .await
        .map_err(CheckpointRepositoryError::database)?
        .is_some())
}

async fn records_from_connection(
    connection: &Connection,
    job_id: &JobId,
) -> Result<Vec<CheckpointRecord>, CheckpointRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT checkpoint.id, checkpoint.job_id, checkpoint.attempt_id, checkpoint.checkpoint_json, checkpoint.created_at, length(checkpoint.checkpoint_json), attempt.job_id FROM job_checkpoints AS checkpoint LEFT JOIN job_attempts AS attempt ON attempt.id = checkpoint.attempt_id WHERE checkpoint.job_id = ?1 ORDER BY checkpoint.created_at, checkpoint.id",
            [job_id.as_str()],
        )
        .await
        .map_err(CheckpointRepositoryError::database)?;
    let mut records = Vec::new();
    while let Some(row) = rows
        .next()
        .await
        .map_err(CheckpointRepositoryError::database)?
    {
        records.push(record_from_row(&row)?);
    }
    Ok(records)
}

fn record_from_row(row: &turso::Row) -> Result<CheckpointRecord, CheckpointRepositoryError> {
    let job_id: String = row.get(1).map_err(CheckpointRepositoryError::database)?;
    let attempt_id: Option<String> = row.get(2).map_err(CheckpointRepositoryError::database)?;
    let attempt_job_id: Option<String> = row.get(6).map_err(CheckpointRepositoryError::database)?;
    if attempt_id.is_none() || attempt_job_id.as_deref() != Some(job_id.as_str()) {
        return Err(CheckpointRepositoryError::Inconsistent);
    }
    let encoded_length: i64 = row.get(5).map_err(CheckpointRepositoryError::database)?;
    let encoded_length =
        usize::try_from(encoded_length).map_err(|_| CheckpointRepositoryError::Inconsistent)?;
    let encoded: String = row.get(3).map_err(CheckpointRepositoryError::database)?;
    let checkpoint = CheckpointEnvelope::decode(&encoded, encoded_length)?;
    Ok(CheckpointRecord {
        id: row.get(0).map_err(CheckpointRepositoryError::database)?,
        job_id: JobId::from_stored(job_id),
        attempt_id,
        checkpoint,
        created_at: row.get(4).map_err(CheckpointRepositoryError::database)?,
    })
}

fn bounded_non_empty(value: &str, max_bytes: usize) -> Result<(), CheckpointRepositoryError> {
    if value.trim().is_empty() || value.len() > max_bytes {
        return Err(CheckpointRepositoryError::InvalidEnvelope);
    }
    Ok(())
}

fn valid_hash(value: &str) -> Result<(), CheckpointRepositoryError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CheckpointRepositoryError::InvalidEnvelope);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MigrationRunner, repositories::JobFailureCode, repositories::JobKind,
        repositories::JobRepository, repositories::NewJob,
    };

    async fn database() -> Result<ErabiDatabase, Box<dyn std::error::Error>> {
        let database = ErabiDatabase::in_memory().await?;
        MigrationRunner::default().apply(&database).await?;
        Ok(database)
    }

    fn job(max_attempts: u32) -> Result<NewJob, Box<dyn std::error::Error>> {
        Ok(NewJob::new(
            JobKind::new("CHECKPOINT_TEST")?,
            1,
            0,
            max_attempts,
        )?)
    }

    fn checkpoint(unit: &str) -> Result<CheckpointEnvelope, Box<dyn std::error::Error>> {
        checkpoint_for("generic-job", unit)
    }

    fn checkpoint_for(
        snapshot_id: &str,
        unit: &str,
    ) -> Result<CheckpointEnvelope, Box<dyn std::error::Error>> {
        let identity = CheckpointIdentity::new(snapshot_id, "a".repeat(64), "b".repeat(64))?;
        let mut checkpoint = CheckpointEnvelope::new(identity);
        checkpoint
            .completed_units
            .push(CheckpointUnitId::new(unit)?);
        Ok(checkpoint)
    }

    #[test]
    fn compatibility_rejects_snapshot_or_semantic_hash_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let identity = CheckpointIdentity::new("run-1", "a".repeat(64), "b".repeat(64))?;
        let checkpoint = CheckpointEnvelope::new(identity.clone());
        let mismatch = CheckpointIdentity::new("run-1", "a".repeat(64), "c".repeat(64))?;
        assert_eq!(
            checkpoint.compatibility_with(&identity),
            CheckpointCompatibility::Compatible
        );
        assert_eq!(
            checkpoint.compatibility_with(&mismatch),
            CheckpointCompatibility::Incompatible
        );
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_records_are_append_only_and_preserve_earlier_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let jobs = JobRepository::new(&database);
        let job = job(2)?;
        jobs.enqueue(&job, 0).await?;
        let acquired = jobs
            .acquire_next("checkpoint-worker", 0, 10)
            .await?
            .ok_or("job was not acquired")?;
        let lease = acquired.job.lease.ok_or("lease missing")?;
        jobs.append_checkpoint(
            &job.id,
            &acquired.attempt.id,
            &lease,
            &checkpoint("first")?,
            1,
        )
        .await?;
        jobs.append_checkpoint(
            &job.id,
            &acquired.attempt.id,
            &lease,
            &checkpoint("second")?,
            2,
        )
        .await?;

        let records = jobs.checkpoints(&job.id).await?;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].checkpoint.completed_units[0].as_str(), "first");
        assert_eq!(records[1].checkpoint.completed_units[0].as_str(), "second");
        Ok(())
    }

    #[tokio::test]
    async fn valid_checkpoint_is_classified_recoverable_after_simulated_restart()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let jobs = JobRepository::new(&database);
        let connection = database.connection().await?;
        connection
            .execute(
                "INSERT INTO crawl_runs (id, run_type, status, crawler_id, crawler_version_id, snapshot_json, snapshot_hash, checkpoint_compatibility_hash, actor, created_at) VALUES (?1, 'QUICK_SCRAPE', 'RUNNING', NULL, NULL, '{}', ?2, ?3, 'operator', '2026-08-25T00:00:00Z')",
                ("run-1", "a".repeat(64), "b".repeat(64)),
            )
            .await?;
        let mut job = job(2)?;
        job.crawl_run_id = Some("run-1".to_owned());
        jobs.enqueue(&job, 0).await?;
        let acquired = jobs
            .acquire_next("checkpoint-worker", 0, 5)
            .await?
            .ok_or("job was not acquired")?;
        let lease = acquired.job.lease.ok_or("lease missing")?;
        jobs.append_checkpoint(
            &job.id,
            &acquired.attempt.id,
            &lease,
            &checkpoint_for("run-1", "resume")?,
            1,
        )
        .await?;

        let assessments = CheckpointRepository::new(&database)
            .assess_stale_jobs(5)
            .await?;
        assert_eq!(assessments.len(), 1);
        assert_eq!(
            assessments[0].disposition,
            CheckpointRecoveryDisposition::Recoverable
        );
        let recovery = jobs.recover_stale_jobs(5).await?;
        assert_eq!(recovery.recoverable, 1);
        assert_eq!(recovery.restart_required, 0);
        assert_eq!(
            jobs.job(&job.id).await?.state,
            super::super::job::JobState::Queued
        );
        Ok(())
    }

    #[tokio::test]
    async fn incompatible_current_identity_requires_restart()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let jobs = JobRepository::new(&database);
        let connection = database.connection().await?;
        connection
            .execute(
                "INSERT INTO crawl_runs (id, run_type, status, crawler_id, crawler_version_id, snapshot_json, snapshot_hash, checkpoint_compatibility_hash, actor, created_at) VALUES (?1, 'QUICK_SCRAPE', 'RUNNING', NULL, NULL, '{}', ?2, ?3, 'operator', '2026-08-25T00:00:00Z')",
                ("run-mismatch", "a".repeat(64), "c".repeat(64)),
            )
            .await?;
        let mut job = job(2)?;
        job.crawl_run_id = Some("run-mismatch".to_owned());
        jobs.enqueue(&job, 0).await?;
        let acquired = jobs
            .acquire_next("checkpoint-worker", 0, 5)
            .await?
            .ok_or("job was not acquired")?;
        let lease = acquired.job.lease.ok_or("lease missing")?;
        jobs.append_checkpoint(
            &job.id,
            &acquired.attempt.id,
            &lease,
            &checkpoint_for("run-mismatch", "resume")?,
            1,
        )
        .await?;
        let assessments = CheckpointRepository::new(&database)
            .assess_stale_jobs(5)
            .await?;
        assert_eq!(assessments.len(), 1);
        assert_eq!(
            assessments[0].disposition,
            CheckpointRecoveryDisposition::RestartRequired
        );
        Ok(())
    }

    #[tokio::test]
    async fn missing_or_unverifiable_identity_requires_restart()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let jobs = JobRepository::new(&database);
        let with_checkpoint = job(2)?;
        let without_checkpoint = job(2)?;
        jobs.enqueue(&with_checkpoint, 0).await?;
        jobs.enqueue(&without_checkpoint, 0).await?;
        let acquired = jobs
            .acquire_next("checkpoint-worker", 0, 5)
            .await?
            .ok_or("checkpoint job was not acquired")?;
        let lease = acquired.job.lease.ok_or("lease missing")?;
        jobs.append_checkpoint(
            &with_checkpoint.id,
            &acquired.attempt.id,
            &lease,
            &checkpoint("generic-resume")?,
            1,
        )
        .await?;
        let second = jobs
            .acquire_next("checkpoint-worker", 0, 5)
            .await?
            .ok_or("missing-checkpoint job was not acquired")?;
        assert_eq!(second.job.id, without_checkpoint.id);

        let assessments = CheckpointRepository::new(&database)
            .assess_stale_jobs(5)
            .await?;
        assert_eq!(assessments.len(), 2);
        assert!(assessments.iter().all(|assessment| {
            assessment.disposition == CheckpointRecoveryDisposition::RestartRequired
        }));
        Ok(())
    }

    #[tokio::test]
    async fn invalid_checkpoint_attempt_lineage_is_unsafe_and_never_resumable()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let jobs = JobRepository::new(&database);
        let prior_attempt = job(2)?;
        let cross_job = job(2)?;
        let null_attempt = job(2)?;
        jobs.enqueue(&prior_attempt, 0).await?;
        let prior_first = jobs
            .acquire_next("checkpoint-worker", 0, 5)
            .await?
            .ok_or("prior attempt job was not acquired")?;
        jobs.fail(
            &prior_attempt.id,
            &prior_first.job.lease.ok_or("lease missing")?,
            1,
            JobFailureCode::HandlerFailed,
            1,
        )
        .await?;
        let prior_second = jobs
            .acquire_next("checkpoint-worker", 1, 4)
            .await?
            .ok_or("current attempt job was not acquired")?;
        assert_eq!(prior_second.job.id, prior_attempt.id);
        jobs.enqueue(&cross_job, 0).await?;
        jobs.enqueue(&null_attempt, 0).await?;
        let first = jobs
            .acquire_next("checkpoint-worker", 1, 4)
            .await?
            .ok_or("cross-job checkpoint job was not acquired")?;
        let second = jobs
            .acquire_next("checkpoint-worker", 1, 4)
            .await?
            .ok_or("null-attempt checkpoint job was not acquired")?;
        let connection = database.connection().await?;
        let encoded = checkpoint("corrupt-lineage")?.encode()?;
        connection
            .execute(
                "INSERT INTO job_checkpoints (id, job_id, attempt_id, checkpoint_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    Uuid::now_v7().to_string(),
                    first.job.id.as_str(),
                    second.attempt.id.as_str(),
                    encoded.as_str(),
                    1,
                ),
            )
            .await?;
        connection
            .execute(
                "INSERT INTO job_checkpoints (id, job_id, attempt_id, checkpoint_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    Uuid::now_v7().to_string(),
                    prior_attempt.id.as_str(),
                    prior_first.attempt.id.as_str(),
                    encoded.as_str(),
                    1,
                ),
            )
            .await?;
        connection
            .execute(
                "INSERT INTO job_checkpoints (id, job_id, attempt_id, checkpoint_json, created_at) VALUES (?1, ?2, NULL, ?3, ?4)",
                (
                    Uuid::now_v7().to_string(),
                    second.job.id.as_str(),
                    encoded.as_str(),
                    1,
                ),
            )
            .await?;

        assert!(matches!(
            CheckpointRepository::new(&database)
                .latest(&first.job.id)
                .await,
            Err(CheckpointRepositoryError::Inconsistent)
        ));
        assert!(matches!(
            CheckpointRepository::new(&database)
                .records(&second.job.id)
                .await,
            Err(CheckpointRepositoryError::Inconsistent)
        ));
        let assessments = CheckpointRepository::new(&database)
            .assess_stale_jobs(5)
            .await?;
        assert_eq!(assessments.len(), 3);
        assert!(
            assessments.iter().all(|assessment| {
                assessment.disposition == CheckpointRecoveryDisposition::Unsafe
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn malformed_checkpoint_is_typed_unsafe_and_never_resumable()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let jobs = JobRepository::new(&database);
        let job = job(2)?;
        jobs.enqueue(&job, 0).await?;
        let acquired = jobs
            .acquire_next("checkpoint-worker", 0, 5)
            .await?
            .ok_or("job was not acquired")?;
        let connection = database.connection().await?;
        connection
            .execute(
                "INSERT INTO job_checkpoints (id, job_id, attempt_id, checkpoint_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    Uuid::now_v7().to_string(),
                    job.id.as_str(),
                    acquired.attempt.id.as_str(),
                    "{malformed",
                    1,
                ),
            )
            .await?;

        assert!(matches!(
            CheckpointRepository::new(&database).latest(&job.id).await,
            Err(CheckpointRepositoryError::Malformed)
        ));
        let recovery = jobs.recover_stale_jobs(5).await?;
        assert_eq!(recovery.recoverable, 0);
        assert_eq!(recovery.unsafe_checkpoints, 1);
        Ok(())
    }
}
