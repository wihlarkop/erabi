//! Bounded, append-only checkpoint evidence for cooperative job recovery.

use crate::{SqliteConnection as Connection, SqliteRow as Row};
use rusqlite::TransactionBehavior;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::job::{JobId, JobLease};
use crate::{DbError, ErabiDatabase};

/// The only checkpoint envelope format currently understood by the generic
/// repository.
pub const CHECKPOINT_ENVELOPE_FORMAT_VERSION: u16 = 1;
/// Maximum encoded checkpoint size persisted in one append-only row.
pub const MAX_CHECKPOINT_BYTES: usize = 64 * 1024;
/// Maximum encoded payload-kind identifier size.
pub const MAX_CHECKPOINT_PAYLOAD_KIND_BYTES: usize = 64;
const MAX_SNAPSHOT_ID_BYTES: usize = 128;
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

/// Bounded opaque identifier for the subsystem-owned checkpoint payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CheckpointPayloadKind(String);

impl CheckpointPayloadKind {
    /// Creates a stable upper-case payload-kind identifier.
    ///
    /// # Errors
    /// Returns an error when the identifier is empty, oversized, or contains
    /// anything other than upper-case ASCII letters, digits, or underscores.
    pub fn new(value: impl Into<String>) -> Result<Self, CheckpointRepositoryError> {
        let kind = Self(value.into());
        kind.validate()?;
        Ok(kind)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<(), CheckpointRepositoryError> {
        if self.0.is_empty()
            || self.0.len() > MAX_CHECKPOINT_PAYLOAD_KIND_BYTES
            || !self
                .0
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(CheckpointRepositoryError::InvalidEnvelope);
        }
        Ok(())
    }
}

/// Generic bounded checkpoint envelope for future plan-specific typed payloads.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointEnvelope {
    pub format_version: u16,
    /// Monotonic append position assigned by this repository for one job.
    /// This is persisted in the existing JSON column so recovery does not
    /// infer checkpoint order from row order or UUID lexical order.
    pub sequence: u64,
    pub identity: CheckpointIdentity,
    pub payload_kind: CheckpointPayloadKind,
    pub payload: serde_json::Value,
}

impl CheckpointEnvelope {
    /// Creates a current-format envelope for a specific immutable run
    /// snapshot and subsystem-owned structured payload.
    ///
    /// # Errors
    /// Returns [`CheckpointRepositoryError::InvalidEnvelope`] when the
    /// identity, payload kind, or payload shape violates the current
    /// transport contract.
    pub fn new(
        identity: CheckpointIdentity,
        payload_kind: CheckpointPayloadKind,
        payload: serde_json::Value,
    ) -> Result<Self, CheckpointRepositoryError> {
        let envelope = Self {
            format_version: CHECKPOINT_ENVELOPE_FORMAT_VERSION,
            sequence: 0,
            identity,
            payload_kind,
            payload,
        };
        envelope.validate()?;
        Ok(envelope)
    }

    /// Validates typed bounds and serializes the envelope for durable storage.
    ///
    /// # Errors
    /// Returns a typed error when the envelope is invalid or exceeds the
    /// bounded storage limit.
    pub fn encode(&self) -> Result<String, CheckpointRepositoryError> {
        self.validate()?;
        let encoded =
            serde_json::to_string(self).map_err(|_| CheckpointRepositoryError::Serialization)?;
        if encoded.len() > MAX_CHECKPOINT_BYTES {
            return Err(CheckpointRepositoryError::PayloadTooLarge);
        }
        Ok(encoded)
    }

    fn validate(&self) -> Result<(), CheckpointRepositoryError> {
        if self.format_version != CHECKPOINT_ENVELOPE_FORMAT_VERSION {
            return Err(CheckpointRepositoryError::UnsupportedFormatVersion);
        }
        self.identity.validate()?;
        self.payload_kind.validate()?;
        if !self.payload.is_object() {
            return Err(CheckpointRepositoryError::InvalidEnvelope);
        }
        Ok(())
    }

    fn decode(value: &str, encoded_length: usize) -> Result<Self, CheckpointRepositoryError> {
        if encoded_length > MAX_CHECKPOINT_BYTES {
            return Err(CheckpointRepositoryError::PayloadTooLarge);
        }
        let parsed: serde_json::Value =
            serde_json::from_str(value).map_err(|_| CheckpointRepositoryError::Malformed)?;
        let object = parsed
            .as_object()
            .ok_or(CheckpointRepositoryError::Malformed)?;
        if object.contains_key("schema_version") {
            return Err(CheckpointRepositoryError::UnsupportedFormatVersion);
        }
        if let Some(format_version) = object.get("format_version")
            && format_version.is_number()
            && format_version.as_u64() != Some(u64::from(CHECKPOINT_ENVELOPE_FORMAT_VERSION))
        {
            return Err(CheckpointRepositoryError::UnsupportedFormatVersion);
        }
        let checkpoint: Self =
            serde_json::from_value(parsed).map_err(|_| CheckpointRepositoryError::Malformed)?;
        checkpoint.validate()?;
        if checkpoint.sequence == 0 {
            return Err(CheckpointRepositoryError::InvalidEnvelope);
        }
        Ok(checkpoint)
    }
}

/// Generic stale-job assessment. A resume candidate has only passed the
/// transport and immutable-identity checks; it has not passed crawler-specific
/// recovery validation or durable traversal reconstruction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CheckpointEnvelopeDisposition {
    ResumeCandidate,
    RestartRequired,
    Invalid,
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
    #[error("the checkpoint envelope format is unsupported")]
    UnsupportedFormatVersion,
    #[error("the checkpoint envelope is invalid")]
    InvalidEnvelope,
    #[error("the checkpoint envelope exceeds the bounded storage limit")]
    PayloadTooLarge,
    #[error("the checkpoint evidence is malformed")]
    Malformed,
    #[error("checkpoint serialization failed")]
    Serialization,
    #[error("the requested job does not exist")]
    NotFound,
    #[error("the current worker no longer owns this checkpoint lease")]
    LeaseLost,
}

impl CheckpointRepositoryError {
    fn database(error: rusqlite::Error) -> Self {
        Self::Database(DbError::from(error))
    }
}

impl From<DbError> for CheckpointRepositoryError {
    fn from(error: DbError) -> Self {
        Self::Database(error)
    }
}

impl From<rusqlite::Error> for CheckpointRepositoryError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(DbError::from(error))
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
        let job_id = job_id.clone();
        let attempt_id = attempt_id.to_owned();
        let lease = lease.clone();
        let checkpoint = checkpoint.clone();
        self.database
            .call(move |raw| {
                let mut connection = Connection::new(raw);
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(CheckpointRepositoryError::database)?;
                let result = append_in_transaction(
                    &transaction,
                    &job_id,
                    &attempt_id,
                    &lease,
                    &checkpoint,
                    created_at,
                );
                match result {
                    Ok(record) => transaction
                        .commit()
                        .map(|()| record)
                        .map_err(CheckpointRepositoryError::database),
                    Err(error) => {
                        let _ = transaction.rollback();
                        Err(error)
                    }
                }
            })
            .await
    }

    /// Returns all append-only checkpoint evidence in trusted-time order.
    ///
    /// # Errors
    /// Returns a typed malformed/invalid result instead of treating bad
    /// evidence as resumable.
    pub async fn records(
        &self,
        job_id: &JobId,
    ) -> Result<Vec<CheckpointRecord>, CheckpointRepositoryError> {
        let job_id = job_id.clone();
        self.database
            .call(move |raw| {
                let connection = Connection::new(raw);
                records_from_connection(&connection, &job_id)
            })
            .await
    }

    /// Returns the latest checkpoint, if any, without hiding malformed or
    /// structurally inconsistent earlier evidence.
    ///
    /// # Errors
    /// Returns a typed error for malformed or invalid durable evidence.
    pub async fn latest(
        &self,
        job_id: &JobId,
    ) -> Result<Option<CheckpointRecord>, CheckpointRepositoryError> {
        let job_id = job_id.clone();
        self.database
            .call(move |raw| {
                let connection = Connection::new(raw);
                Ok(records_from_connection(&connection, &job_id)?.pop())
            })
            .await
    }
}

/// Appends generic checkpoint evidence inside a caller-owned immediate
/// transaction. Callers may use this boundary to couple the checkpoint with
/// other durable work; ownership is still verified against the active attempt
/// and lease before inserting anything.
pub(crate) fn append_in_transaction(
    transaction: &impl crate::SqliteExecutor,
    job_id: &JobId,
    attempt_id: &str,
    lease: &JobLease,
    checkpoint: &CheckpointEnvelope,
    created_at: i64,
) -> Result<CheckpointRecord, CheckpointRepositoryError> {
    if attempt_id.is_empty() || attempt_id.len() > MAX_ATTEMPT_ID_BYTES {
        return Err(CheckpointRepositoryError::InvalidEnvelope);
    }
    ensure_owned_attempt(transaction, job_id, attempt_id, lease, created_at)?;
    let mut stored_checkpoint = checkpoint.clone();
    stored_checkpoint.sequence = next_checkpoint_sequence(transaction, job_id)?;
    let encoded = stored_checkpoint.encode()?;
    let id = Uuid::now_v7().to_string();
    transaction
        .execute(
            "INSERT INTO job_checkpoints (id, job_id, attempt_id, checkpoint_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            (id.as_str(), job_id.as_str(), attempt_id, encoded.as_str(), created_at),
        )
        .map_err(CheckpointRepositoryError::database)?;
    Ok(CheckpointRecord {
        id,
        job_id: job_id.clone(),
        attempt_id: Some(attempt_id.to_owned()),
        checkpoint: stored_checkpoint,
        created_at,
    })
}

fn ensure_owned_attempt(
    connection: &impl crate::SqliteExecutor,
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
        .map_err(CheckpointRepositoryError::database)?;
    let row = rows
        .next()
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
        .map_err(CheckpointRepositoryError::database)?;
    if attempts
        .next()
        .map_err(CheckpointRepositoryError::database)?
        .is_none()
    {
        return Err(CheckpointRepositoryError::LeaseLost);
    }
    Ok(())
}

pub(super) fn assess_one_stale_job(
    connection: &impl crate::SqliteExecutor,
    job_id: &JobId,
    run_id: Option<&str>,
) -> Result<CheckpointEnvelopeDisposition, CheckpointRepositoryError> {
    let records = match records_from_connection(connection, job_id) {
        Ok(records) => records,
        Err(
            CheckpointRepositoryError::UnsupportedFormatVersion
            | CheckpointRepositoryError::Malformed
            | CheckpointRepositoryError::InvalidEnvelope
            | CheckpointRepositoryError::PayloadTooLarge,
        ) => return Ok(CheckpointEnvelopeDisposition::Invalid),
        Err(error) => return Err(error),
    };
    let Some(latest) = records.last() else {
        return Ok(CheckpointEnvelopeDisposition::RestartRequired);
    };
    let Some(attempt_id) = latest.attempt_id.as_deref() else {
        return Ok(CheckpointEnvelopeDisposition::Invalid);
    };
    if !active_checkpoint_lineage_is_valid(connection, job_id, attempt_id)? {
        return Ok(CheckpointEnvelopeDisposition::Invalid);
    }
    let Some(run_id) = run_id else {
        return Ok(CheckpointEnvelopeDisposition::RestartRequired);
    };
    let mut rows = connection
        .query(
            "SELECT snapshot_hash, checkpoint_compatibility_hash FROM crawl_runs WHERE id = ?1",
            [run_id],
        )
        .map_err(CheckpointRepositoryError::database)?;
    let Some(row) = rows.next().map_err(CheckpointRepositoryError::database)? else {
        return Ok(CheckpointEnvelopeDisposition::Invalid);
    };
    let snapshot_hash: String = row.get(0).map_err(CheckpointRepositoryError::database)?;
    let compatibility_hash: String = row.get(1).map_err(CheckpointRepositoryError::database)?;
    let Ok(current) = CheckpointIdentity::new(run_id, snapshot_hash, compatibility_hash) else {
        return Ok(CheckpointEnvelopeDisposition::Invalid);
    };
    Ok(if latest.checkpoint.identity == current {
        CheckpointEnvelopeDisposition::ResumeCandidate
    } else {
        CheckpointEnvelopeDisposition::RestartRequired
    })
}

fn active_checkpoint_lineage_is_valid(
    connection: &impl crate::SqliteExecutor,
    job_id: &JobId,
    attempt_id: &str,
) -> Result<bool, CheckpointRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT 1 FROM jobs AS job JOIN job_attempts AS attempt ON attempt.id = ?1 WHERE job.id = ?2 AND job.state = 'RUNNING' AND attempt.job_id = job.id AND attempt.attempt_number = job.current_attempt AND attempt.outcome = 'RUNNING' AND attempt.lease_id = job.lease_id AND attempt.lease_generation = job.lease_generation AND attempt.worker_id = job.lease_owner LIMIT 1",
            (attempt_id, job_id.as_str()),
        )
        .map_err(CheckpointRepositoryError::database)?;
    Ok(rows
        .next()
        .map_err(CheckpointRepositoryError::database)?
        .is_some())
}

fn records_from_connection(
    connection: &impl crate::SqliteExecutor,
    job_id: &JobId,
) -> Result<Vec<CheckpointRecord>, CheckpointRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT checkpoint.id, checkpoint.job_id, checkpoint.attempt_id, checkpoint.checkpoint_json, checkpoint.created_at, length(checkpoint.checkpoint_json), attempt.job_id FROM job_checkpoints AS checkpoint LEFT JOIN job_attempts AS attempt ON attempt.id = checkpoint.attempt_id WHERE checkpoint.job_id = ?1",
            [job_id.as_str()],
        )
        .map_err(CheckpointRepositoryError::database)?;
    let mut records = Vec::new();
    while let Some(row) = rows.next().map_err(CheckpointRepositoryError::database)? {
        records.push(record_from_row(row)?);
    }
    records.sort_by(|left, right| {
        left.checkpoint
            .sequence
            .cmp(&right.checkpoint.sequence)
            .then(left.created_at.cmp(&right.created_at))
            .then_with(|| {
                let left_payload = left.checkpoint.encode().unwrap_or_default();
                let right_payload = right.checkpoint.encode().unwrap_or_default();
                left_payload.cmp(&right_payload)
            })
    });
    Ok(records)
}

fn next_checkpoint_sequence(
    connection: &impl crate::SqliteExecutor,
    job_id: &JobId,
) -> Result<u64, CheckpointRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT checkpoint_json FROM job_checkpoints WHERE job_id = ?1",
            [job_id.as_str()],
        )
        .map_err(CheckpointRepositoryError::database)?;
    let mut maximum = None;
    while let Some(row) = rows.next().map_err(CheckpointRepositoryError::database)? {
        let encoded: String = row.get(0).map_err(CheckpointRepositoryError::database)?;
        let checkpoint = CheckpointEnvelope::decode(&encoded, encoded.len())?;
        maximum = Some(maximum.map_or(checkpoint.sequence, |value: u64| {
            value.max(checkpoint.sequence)
        }));
    }
    maximum.map_or(Ok(1), |value| {
        value
            .checked_add(1)
            .ok_or(CheckpointRepositoryError::InvalidEnvelope)
    })
}

fn record_from_row(row: &Row) -> Result<CheckpointRecord, CheckpointRepositoryError> {
    let job_id: String = row.get(1).map_err(CheckpointRepositoryError::database)?;
    let attempt_id: Option<String> = row.get(2).map_err(CheckpointRepositoryError::database)?;
    let attempt_job_id: Option<String> = row.get(6).map_err(CheckpointRepositoryError::database)?;
    if attempt_id.is_none() || attempt_job_id.as_deref() != Some(job_id.as_str()) {
        return Err(CheckpointRepositoryError::InvalidEnvelope);
    }
    let encoded_length: i64 = row.get(5).map_err(CheckpointRepositoryError::database)?;
    let encoded_length =
        usize::try_from(encoded_length).map_err(|_| CheckpointRepositoryError::InvalidEnvelope)?;
    let encoded: String = row.get(3).map_err(CheckpointRepositoryError::database)?;
    let checkpoint = CheckpointEnvelope::decode(&encoded, encoded_length.max(encoded.len()))?;
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

    async fn assess(
        database: &ErabiDatabase,
        job_id: &JobId,
        run_id: Option<&str>,
    ) -> Result<CheckpointEnvelopeDisposition, CheckpointRepositoryError> {
        let job_id = job_id.clone();
        let run_id = run_id.map(str::to_owned);
        database
            .call(move |raw| {
                let connection = crate::SqliteConnection::new(raw);
                assess_one_stale_job(&connection, &job_id, run_id.as_deref())
            })
            .await
    }

    async fn execute_sql<F>(
        database: &ErabiDatabase,
        operation: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnOnce(&mut rusqlite::Connection) -> Result<(), rusqlite::Error> + Send + 'static,
    {
        crate::test_call(database, operation).await?;
        Ok(())
    }

    fn job(max_attempts: u32) -> Result<NewJob, Box<dyn std::error::Error>> {
        Ok(NewJob::new(
            JobKind::new("CHECKPOINT_TEST")?,
            1,
            0,
            max_attempts,
        )?)
    }

    fn checkpoint(marker: &str) -> Result<CheckpointEnvelope, Box<dyn std::error::Error>> {
        checkpoint_for("generic-job", marker)
    }

    fn checkpoint_for(
        snapshot_id: &str,
        marker: &str,
    ) -> Result<CheckpointEnvelope, Box<dyn std::error::Error>> {
        let identity = CheckpointIdentity::new(snapshot_id, "a".repeat(64), "b".repeat(64))?;
        Ok(CheckpointEnvelope::new(
            identity,
            CheckpointPayloadKind::new("TEST_PAYLOAD")?,
            serde_json::json!({"marker": marker}),
        )?)
    }

    #[test]
    fn payload_kind_is_transparent_and_bounded() -> Result<(), Box<dyn std::error::Error>> {
        let kind = CheckpointPayloadKind::new("CRAWL_RECOVERY")?;
        assert_eq!(kind.as_str(), "CRAWL_RECOVERY");
        assert_eq!(
            serde_json::to_value(&kind)?,
            serde_json::json!("CRAWL_RECOVERY")
        );
        assert!(matches!(
            CheckpointPayloadKind::new(""),
            Err(CheckpointRepositoryError::InvalidEnvelope)
        ));
        assert!(matches!(
            CheckpointPayloadKind::new("A".repeat(MAX_CHECKPOINT_PAYLOAD_KIND_BYTES + 1)),
            Err(CheckpointRepositoryError::InvalidEnvelope)
        ));
        for invalid in ["crawl_recovery", "Foo", "FOO-BAR", "FOO BAR"] {
            assert!(matches!(
                CheckpointPayloadKind::new(invalid),
                Err(CheckpointRepositoryError::InvalidEnvelope)
            ));
        }
        Ok(())
    }

    #[test]
    fn non_object_payloads_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let identity = CheckpointIdentity::new("run-1", "a".repeat(64), "b".repeat(64))?;
        for payload in [
            serde_json::Value::Null,
            serde_json::json!([]),
            serde_json::json!("payload"),
            serde_json::json!(42),
            serde_json::json!(true),
        ] {
            assert!(matches!(
                CheckpointEnvelope::new(
                    identity.clone(),
                    CheckpointPayloadKind::new("TEST_PAYLOAD")?,
                    payload,
                ),
                Err(CheckpointRepositoryError::InvalidEnvelope)
            ));
        }
        Ok(())
    }

    #[test]
    fn unsupported_format_is_distinct_from_malformed_json() {
        assert!(matches!(
            CheckpointEnvelope::decode(r#"{"schema_version":1}"#, 20),
            Err(CheckpointRepositoryError::UnsupportedFormatVersion)
        ));
        assert!(matches!(
            CheckpointEnvelope::decode(r#"{"format_version":2}"#, 20),
            Err(CheckpointRepositoryError::UnsupportedFormatVersion)
        ));
        assert!(matches!(
            CheckpointEnvelope::decode("{malformed", 10),
            Err(CheckpointRepositoryError::Malformed)
        ));
        assert!(matches!(
            CheckpointEnvelope::decode("[]", 2),
            Err(CheckpointRepositoryError::Malformed)
        ));
    }

    #[test]
    fn current_envelope_validates_identity_and_format_on_encode()
    -> Result<(), Box<dyn std::error::Error>> {
        let invalid_identity = CheckpointEnvelope {
            format_version: CHECKPOINT_ENVELOPE_FORMAT_VERSION,
            sequence: 0,
            identity: CheckpointIdentity {
                snapshot_id: "run-1".to_owned(),
                snapshot_hash: "not-a-hash".to_owned(),
                compatibility_hash: "b".repeat(64),
            },
            payload_kind: CheckpointPayloadKind::new("TEST_PAYLOAD")?,
            payload: serde_json::json!({}),
        };
        assert!(matches!(
            invalid_identity.encode(),
            Err(CheckpointRepositoryError::InvalidEnvelope)
        ));

        let unsupported = CheckpointEnvelope {
            format_version: CHECKPOINT_ENVELOPE_FORMAT_VERSION + 1,
            sequence: 0,
            identity: CheckpointIdentity::new("run-1", "a".repeat(64), "b".repeat(64))?,
            payload_kind: CheckpointPayloadKind::new("TEST_PAYLOAD")?,
            payload: serde_json::json!({}),
        };
        assert!(matches!(
            unsupported.encode(),
            Err(CheckpointRepositoryError::UnsupportedFormatVersion)
        ));
        Ok(())
    }

    #[test]
    fn encoded_checkpoint_size_is_bounded() -> Result<(), Box<dyn std::error::Error>> {
        let identity = CheckpointIdentity::new("run-1", "a".repeat(64), "b".repeat(64))?;
        let checkpoint = CheckpointEnvelope::new(
            identity,
            CheckpointPayloadKind::new("TEST_PAYLOAD")?,
            serde_json::json!({"data": "x".repeat(MAX_CHECKPOINT_BYTES)}),
        )?;
        assert!(matches!(
            checkpoint.encode(),
            Err(CheckpointRepositoryError::PayloadTooLarge)
        ));
        Ok(())
    }

    #[test]
    fn current_envelope_round_trips_a_structured_payload() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut checkpoint = checkpoint("round-trip")?;
        checkpoint.sequence = 1;
        let encoded = checkpoint.encode()?;
        let decoded = CheckpointEnvelope::decode(&encoded, encoded.len())?;
        assert_eq!(decoded, checkpoint);
        assert!(decoded.payload.is_object());
        assert_eq!(decoded.payload["marker"], "round-trip");
        Ok(())
    }

    #[tokio::test]
    async fn append_requires_the_current_attempt_and_lease()
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
        assert!(matches!(
            jobs.append_checkpoint(
                &job.id,
                "not-the-current-attempt",
                &lease,
                &checkpoint("wrong-attempt")?,
                1,
            )
            .await,
            Err(crate::repositories::JobRepositoryError::Checkpoint(
                CheckpointRepositoryError::LeaseLost
            ))
        ));
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
        let first_checkpoint = checkpoint("first")?;
        let first_record = jobs
            .append_checkpoint(&job.id, &acquired.attempt.id, &lease, &first_checkpoint, 1)
            .await?;
        let mut second_checkpoint = checkpoint("second")?;
        second_checkpoint.sequence = 999;
        let second_record = jobs
            .append_checkpoint(&job.id, &acquired.attempt.id, &lease, &second_checkpoint, 2)
            .await?;

        let records = jobs.checkpoints(&job.id).await?;
        assert_eq!(records.len(), 2);
        assert_eq!(first_checkpoint.sequence, 0);
        assert_eq!(second_checkpoint.sequence, 999);
        assert_eq!(first_record.checkpoint.sequence, 1);
        assert_eq!(second_record.checkpoint.sequence, 2);
        assert_eq!(records[0].checkpoint.sequence, 1);
        assert_eq!(records[1].checkpoint.sequence, 2);
        assert_eq!(records[0].checkpoint.payload["marker"], "first");
        assert_eq!(records[1].checkpoint.payload["marker"], "second");
        Ok(())
    }

    #[tokio::test]
    async fn identity_compatible_checkpoint_is_only_a_resume_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let jobs = JobRepository::new(&database);
        execute_sql(&database, move |connection| {
            connection.execute(
                "INSERT INTO crawl_runs (id, run_type, status, crawler_id, crawler_version_id, snapshot_json, snapshot_hash, checkpoint_compatibility_hash, actor, created_at) VALUES (?1, 'QUICK_SCRAPE', 'RUNNING', NULL, NULL, '{}', ?2, ?3, 'operator', '2026-08-25T00:00:00Z')",
                ("run-1", "a".repeat(64), "b".repeat(64)),
            )
            .map(|_| ())
        }).await?;
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

        let disposition = assess(&database, &job.id, Some("run-1")).await?;
        assert_eq!(disposition, CheckpointEnvelopeDisposition::ResumeCandidate);
        let recovery = jobs.recover_stale_jobs(5).await?;
        assert_eq!(recovery.resume_candidates, 1);
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
        execute_sql(&database, move |connection| {
            connection.execute(
                "INSERT INTO crawl_runs (id, run_type, status, crawler_id, crawler_version_id, snapshot_json, snapshot_hash, checkpoint_compatibility_hash, actor, created_at) VALUES (?1, 'QUICK_SCRAPE', 'RUNNING', NULL, NULL, '{}', ?2, ?3, 'operator', '2026-08-25T00:00:00Z')",
                ("run-mismatch", "a".repeat(64), "c".repeat(64)),
            )
            .map(|_| ())
        }).await?;
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
        let disposition = assess(&database, &job.id, Some("run-mismatch")).await?;
        assert_eq!(disposition, CheckpointEnvelopeDisposition::RestartRequired);
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

        assert_eq!(
            assess(&database, &with_checkpoint.id, None).await?,
            CheckpointEnvelopeDisposition::RestartRequired
        );
        assert_eq!(
            assess(&database, &without_checkpoint.id, None).await?,
            CheckpointEnvelopeDisposition::RestartRequired
        );
        Ok(())
    }

    #[tokio::test]
    async fn invalid_checkpoint_attempt_lineage_is_invalid_and_never_resumable()
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
        let encoded = checkpoint("corrupt-lineage")?.encode()?;
        let first_job_id = first.job.id.to_string();
        let second_attempt_id = second.attempt.id.clone();
        let prior_job_id = prior_attempt.id.to_string();
        let prior_attempt_id = prior_first.attempt.id.clone();
        let second_job_id = second.job.id.to_string();
        execute_sql(&database, move |connection| {
            connection.execute(
                "INSERT INTO job_checkpoints (id, job_id, attempt_id, checkpoint_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    Uuid::now_v7().to_string(),
                    first_job_id.as_str(),
                    second_attempt_id.as_str(),
                    encoded.as_str(),
                    1,
                ),
            ).map(|_| ())?;
            connection.execute(
                "INSERT INTO job_checkpoints (id, job_id, attempt_id, checkpoint_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    Uuid::now_v7().to_string(),
                    prior_job_id.as_str(),
                    prior_attempt_id.as_str(),
                    encoded.as_str(),
                    1,
                ),
            ).map(|_| ())?;
            connection.execute(
                "INSERT INTO job_checkpoints (id, job_id, attempt_id, checkpoint_json, created_at) VALUES (?1, ?2, NULL, ?3, ?4)",
                (
                    Uuid::now_v7().to_string(),
                    second_job_id.as_str(),
                    encoded.as_str(),
                    1,
                ),
            ).map(|_| ())
        }).await?;

        assert!(matches!(
            CheckpointRepository::new(&database)
                .latest(&first.job.id)
                .await,
            Err(CheckpointRepositoryError::InvalidEnvelope)
        ));
        assert!(matches!(
            CheckpointRepository::new(&database)
                .records(&second.job.id)
                .await,
            Err(CheckpointRepositoryError::InvalidEnvelope)
        ));
        let recovery = jobs.recover_stale_jobs(5).await?;
        assert_eq!(recovery.resume_candidates, 0);
        assert_eq!(recovery.invalid_checkpoints, 3);
        Ok(())
    }

    #[tokio::test]
    async fn historical_checkpoint_is_invalid_and_never_a_resume_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let jobs = JobRepository::new(&database);
        let job = job(2)?;
        jobs.enqueue(&job, 0).await?;
        let acquired = jobs
            .acquire_next("checkpoint-worker", 0, 5)
            .await?
            .ok_or("job was not acquired")?;
        let historical = r#"{
            "schema_version": 1,
            "sequence": 1,
            "identity": {
                "snapshot_id": "generic-job",
                "snapshot_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "compatibility_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            },
            "completed_units": [],
            "pending_units": [],
            "failed_units": [],
            "artifact_references": [],
            "extraction": {"phase": "NOT_STARTED"}
        }"#;
        let job_id = job.id.to_string();
        let attempt_id = acquired.attempt.id.clone();
        execute_sql(&database, move |connection| {
            connection.execute(
                "INSERT INTO job_checkpoints (id, job_id, attempt_id, checkpoint_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    Uuid::now_v7().to_string(),
                    job_id.as_str(),
                    attempt_id.as_str(),
                    historical,
                    1,
                ),
            ).map(|_| ())
        }).await?;

        assert!(matches!(
            CheckpointRepository::new(&database).latest(&job.id).await,
            Err(CheckpointRepositoryError::UnsupportedFormatVersion)
        ));
        assert_eq!(
            assess(&database, &job.id, None).await?,
            CheckpointEnvelopeDisposition::Invalid
        );
        Ok(())
    }

    #[tokio::test]
    async fn malformed_checkpoint_is_invalid_and_never_resumable()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = database().await?;
        let jobs = JobRepository::new(&database);
        let job = job(2)?;
        jobs.enqueue(&job, 0).await?;
        let acquired = jobs
            .acquire_next("checkpoint-worker", 0, 5)
            .await?
            .ok_or("job was not acquired")?;
        let job_id = job.id.to_string();
        let attempt_id = acquired.attempt.id.clone();
        execute_sql(&database, move |connection| {
            connection.execute(
                "INSERT INTO job_checkpoints (id, job_id, attempt_id, checkpoint_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    Uuid::now_v7().to_string(),
                    job_id.as_str(),
                    attempt_id.as_str(),
                    "{malformed",
                    1,
                ),
            ).map(|_| ())
        }).await?;

        assert!(matches!(
            CheckpointRepository::new(&database).latest(&job.id).await,
            Err(CheckpointRepositoryError::Malformed)
        ));
        let recovery = jobs.recover_stale_jobs(5).await?;
        assert_eq!(recovery.resume_candidates, 0);
        assert_eq!(recovery.invalid_checkpoints, 1);
        Ok(())
    }
}
