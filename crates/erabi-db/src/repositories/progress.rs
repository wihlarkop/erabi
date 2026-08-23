//! Durable, user-facing job progress persistence.

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};
use turso::{Connection, transaction::TransactionBehavior};
use uuid::Uuid;

use crate::{DbError, ErabiDatabase};

use super::JobId;

const MAX_PROGRESS_KEY_BYTES: usize = 64;
const MAX_METADATA_ENTRIES: usize = 16;
const MAX_METADATA_KEY_BYTES: usize = 32;
const MAX_METADATA_CODE_BYTES: usize = 64;
const MAX_REPLAY_LIMIT: usize = 256;

/// A committed, per-job position in the durable progress stream.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ProgressSequence(u64);

impl ProgressSequence {
    /// Creates a non-zero durable sequence position.
    ///
    /// # Errors
    /// Returns an error when the sequence is zero, which is not a valid
    /// committed event position.
    pub const fn new(value: u64) -> Result<Self, ProgressRepositoryError> {
        if value == 0 {
            Err(ProgressRepositoryError::InvalidProgressSequence)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the durable sequence value used for replay cursors.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Stable identity of one durable progress event.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProgressEventId(String);

impl ProgressEventId {
    fn new() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    /// Returns the event identifier for future live/replay consumers.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProgressEventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A validated optional link to a durable job execution attempt.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProgressAttemptId(String);

impl ProgressAttemptId {
    /// Validates a time-sortable durable attempt identity.
    ///
    /// # Errors
    /// Returns an error unless `value` is a `UUIDv7` attempt identity.
    pub fn new(value: impl Into<String>) -> Result<Self, ProgressRepositoryError> {
        let value = value.into();
        let parsed =
            Uuid::parse_str(&value).map_err(|_| ProgressRepositoryError::InvalidAttemptId)?;
        if parsed.get_version_num() != 7 {
            return Err(ProgressRepositoryError::InvalidAttemptId);
        }
        Ok(Self(value))
    }

    /// Returns the durable attempt identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A stable user-facing workflow key, never a tracing/logging message.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProgressKey(String);

impl ProgressKey {
    /// Creates a stable upper-case workflow key such as `LOADING`.
    ///
    /// # Errors
    /// Returns an error unless the key is upper-case ASCII, digits, or
    /// underscores and is within the durable schema bound.
    pub fn new(value: impl Into<String>) -> Result<Self, ProgressRepositoryError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_PROGRESS_KEY_BYTES
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(ProgressRepositoryError::InvalidProgressKey);
        }
        Ok(Self(value))
    }

    /// Returns the stable progress key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One explicit durable terminal condition for a progress stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgressTerminalState {
    Succeeded,
    Failed,
    Cancelled,
}

impl ProgressTerminalState {
    fn as_storage(self) -> &'static str {
        match self {
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }

    fn parse(value: &str) -> Result<Self, ProgressRepositoryError> {
        match value {
            "SUCCEEDED" => Ok(Self::Succeeded),
            "FAILED" => Ok(Self::Failed),
            "CANCELLED" => Ok(Self::Cancelled),
            _ => Err(ProgressRepositoryError::ProgressInvariant),
        }
    }
}

/// A stable metadata field name for bounded, user-facing progress context.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ProgressMetadataKey(String);

impl ProgressMetadataKey {
    /// Creates a lower-snake-case metadata field name.
    ///
    /// # Errors
    /// Returns an error for malformed, oversized, or secret-shaped names.
    pub fn new(value: impl Into<String>) -> Result<Self, ProgressRepositoryError> {
        let value = value.into();
        let valid_shape = !value.is_empty()
            && value.len() <= MAX_METADATA_KEY_BYTES
            && value.bytes().enumerate().all(|(index, byte)| {
                (index == 0 && byte.is_ascii_lowercase())
                    || (index > 0
                        && (byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'))
            });
        if !valid_shape || has_sensitive_name(&value) {
            return Err(ProgressRepositoryError::InvalidProgressMetadata);
        }
        Ok(Self(value))
    }

    /// Returns the stable metadata field name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A stable non-sensitive symbolic metadata value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgressMetadataCode(String);

impl ProgressMetadataCode {
    /// Creates a stable upper-case symbolic value.
    ///
    /// # Errors
    /// Returns an error unless the code is upper-case ASCII, digits, or
    /// underscores and fits the bounded progress metadata contract.
    pub fn new(value: impl Into<String>) -> Result<Self, ProgressRepositoryError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_METADATA_CODE_BYTES
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            || has_sensitive_name(&value.to_ascii_lowercase())
        {
            return Err(ProgressRepositoryError::InvalidProgressMetadata);
        }
        Ok(Self(value))
    }

    /// Returns the symbolic metadata value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The deliberately small value set allowed in progress metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgressMetadataValue {
    Code(ProgressMetadataCode),
    Count(u32),
    Flag(bool),
}

/// Bounded, typed user-facing metadata; it intentionally cannot contain raw
/// request bodies, pages, extracted records, credentials, or arbitrary JSON.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProgressMetadata {
    entries: BTreeMap<ProgressMetadataKey, ProgressMetadataValue>,
}

impl ProgressMetadata {
    /// Creates a bounded metadata collection.
    ///
    /// # Errors
    /// Returns an error when more than sixteen safe metadata entries are
    /// supplied.
    pub fn new(
        entries: BTreeMap<ProgressMetadataKey, ProgressMetadataValue>,
    ) -> Result<Self, ProgressRepositoryError> {
        if entries.len() > MAX_METADATA_ENTRIES {
            return Err(ProgressRepositoryError::InvalidProgressMetadata);
        }
        Ok(Self { entries })
    }

    /// Returns the typed safe metadata entries.
    #[must_use]
    pub fn entries(&self) -> &BTreeMap<ProgressMetadataKey, ProgressMetadataValue> {
        &self.entries
    }
}

/// Validated input for one durable progress append.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewProgressEvent {
    job_id: JobId,
    attempt_id: Option<ProgressAttemptId>,
    key: ProgressKey,
    metadata: ProgressMetadata,
    terminal: Option<ProgressTerminalState>,
}

impl NewProgressEvent {
    /// Creates a non-terminal progress event.
    #[must_use]
    pub const fn new(job_id: JobId, key: ProgressKey, metadata: ProgressMetadata) -> Self {
        Self {
            job_id,
            attempt_id: None,
            key,
            metadata,
            terminal: None,
        }
    }

    /// Creates the sole terminal event for a stream using the stable
    /// `COMPLETED` user-facing workflow key.
    ///
    /// # Errors
    /// Returns an error only if the built-in terminal progress key cannot be
    /// constructed, which would indicate a programming invariant failure.
    pub fn terminal(
        job_id: JobId,
        terminal: ProgressTerminalState,
        metadata: ProgressMetadata,
    ) -> Result<Self, ProgressRepositoryError> {
        Ok(Self {
            job_id,
            attempt_id: None,
            key: ProgressKey::new("COMPLETED")?,
            metadata,
            terminal: Some(terminal),
        })
    }

    /// Links this event to one durable execution attempt.
    #[must_use]
    pub fn with_attempt(mut self, attempt_id: ProgressAttemptId) -> Self {
        self.attempt_id = Some(attempt_id);
        self
    }

    /// Returns the linked job identity.
    #[must_use]
    pub const fn job_id(&self) -> &JobId {
        &self.job_id
    }
}

/// A committed durable progress event in replay order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgressEvent {
    pub id: ProgressEventId,
    pub job_id: JobId,
    pub attempt_id: Option<ProgressAttemptId>,
    pub sequence: ProgressSequence,
    pub key: ProgressKey,
    pub metadata: ProgressMetadata,
    pub terminal: Option<ProgressTerminalState>,
    pub created_at: i64,
}

/// Validated bounded replay input. `after` is exclusive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgressReplayRequest {
    after: Option<ProgressSequence>,
    limit: usize,
}

impl ProgressReplayRequest {
    /// Creates one bounded replay request.
    ///
    /// # Errors
    /// Returns an error when the requested limit is zero or exceeds 256 events.
    pub const fn new(
        after: Option<ProgressSequence>,
        limit: usize,
    ) -> Result<Self, ProgressRepositoryError> {
        if limit == 0 || limit > MAX_REPLAY_LIMIT {
            return Err(ProgressRepositoryError::InvalidReplayRequest);
        }
        Ok(Self { after, limit })
    }
}

/// One bounded, sequence-ordered durable replay page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgressReplayPage {
    pub events: Vec<ProgressEvent>,
    /// Exclusive cursor for a following page when more durable events exist.
    pub next_after: Option<ProgressSequence>,
}

/// Typed progress persistence failures without SQL, metadata, or event payloads.
#[derive(Debug, thiserror::Error)]
pub enum ProgressRepositoryError {
    #[error("the durable progress database operation failed")]
    Database(#[source] DbError),
    #[error("progress keys must be stable upper-case identifiers")]
    InvalidProgressKey,
    #[error("progress metadata does not meet the safe bounded contract")]
    InvalidProgressMetadata,
    #[error("the progress attempt identifier is invalid")]
    InvalidAttemptId,
    #[error("progress sequence must be positive")]
    InvalidProgressSequence,
    #[error("the replay cursor or limit is invalid")]
    InvalidReplayRequest,
    #[error("the job does not exist")]
    JobNotFound,
    #[error("the linked attempt does not exist")]
    AttemptNotFound,
    #[error("the linked attempt belongs to another job")]
    AttemptJobMismatch,
    #[error("the progress stream is already terminal")]
    TerminalStreamClosed,
    #[error("durable progress history is inconsistent")]
    ProgressInvariant,
}

/// The sole normal persistence boundary for append-only user progress.
#[derive(Clone, Copy, Debug)]
pub struct ProgressRepository<'database> {
    database: &'database ErabiDatabase,
}

impl<'database> ProgressRepository<'database> {
    /// Creates the repository over Erabi's controlled database handle.
    #[must_use]
    pub const fn new(database: &'database ErabiDatabase) -> Self {
        Self { database }
    }

    /// Appends one event under an immediate transaction, allocating its next
    /// visible sequence only if the complete insert commits.
    ///
    /// # Errors
    /// Returns typed input, linkage, terminal-stream, or durable database
    /// failures without exposing stored payloads.
    pub async fn append_at(
        &self,
        event: &NewProgressEvent,
        created_at: i64,
    ) -> Result<ProgressEvent, ProgressRepositoryError> {
        let mut connection = self
            .database
            .connection()
            .await
            .map_err(ProgressRepositoryError::from_db)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(ProgressRepositoryError::database)?;
        let result = async {
            ensure_job_exists(&transaction, event.job_id()).await?;
            ensure_attempt_belongs_to_job(&transaction, event).await?;
            ensure_stream_open(&transaction, event.job_id()).await?;
            let sequence = allocate_sequence(&transaction, event.job_id()).await?;
            let payload = StoredProgressPayload::from_event(event)?;
            let payload_json = serde_json::to_string(&payload)
                .map_err(|_| ProgressRepositoryError::ProgressInvariant)?;
            let id = ProgressEventId::new();
            transaction
                .execute(
                    "INSERT INTO job_progress_events (id, job_id, attempt_id, sequence, event_type, payload_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    (
                        id.as_str(),
                        event.job_id.as_str(),
                        event.attempt_id.as_ref().map(ProgressAttemptId::as_str),
                        i64::try_from(sequence.get())
                            .map_err(|_| ProgressRepositoryError::ProgressInvariant)?,
                        event.key.as_str(),
                        payload_json,
                        created_at,
                    ),
                )
                .await
                .map_err(ProgressRepositoryError::database)?;
            Ok(ProgressEvent {
                id,
                job_id: event.job_id.clone(),
                attempt_id: event.attempt_id.clone(),
                sequence,
                key: event.key.clone(),
                metadata: event.metadata.clone(),
                terminal: event.terminal,
                created_at,
            })
        }
        .await;
        match result {
            Ok(progress) => transaction
                .commit()
                .await
                .map(|()| progress)
                .map_err(ProgressRepositoryError::database),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Returns events with a sequence strictly after `request.after`, in
    /// ascending durable sequence order and within the requested bound.
    ///
    /// # Errors
    /// Returns a typed error for an invalid request, unknown job, malformed
    /// durable history, or database failure.
    pub async fn replay(
        &self,
        job_id: &JobId,
        request: ProgressReplayRequest,
    ) -> Result<ProgressReplayPage, ProgressRepositoryError> {
        let connection = self
            .database
            .connection()
            .await
            .map_err(ProgressRepositoryError::from_db)?;
        ensure_job_exists(&connection, job_id).await?;
        let after = request.after.map_or(Ok(0_i64), |value| {
            i64::try_from(value.get()).map_err(|_| ProgressRepositoryError::InvalidReplayRequest)
        })?;
        let query_limit = i64::try_from(request.limit.saturating_add(1))
            .map_err(|_| ProgressRepositoryError::InvalidReplayRequest)?;
        let mut rows = connection
            .query(
                "SELECT id, job_id, attempt_id, sequence, event_type, payload_json, created_at FROM job_progress_events WHERE job_id = ?1 AND sequence > ?2 ORDER BY sequence ASC LIMIT ?3",
                (job_id.as_str(), after, query_limit),
            )
            .await
            .map_err(ProgressRepositoryError::database)?;
        let mut events = Vec::with_capacity(request.limit);
        let mut has_more = false;
        while let Some(row) = rows
            .next()
            .await
            .map_err(ProgressRepositoryError::database)?
        {
            if events.len() == request.limit {
                has_more = true;
                break;
            }
            events.push(progress_event_from_row(&row)?);
        }
        let next_after = has_more
            .then(|| events.last().map(|event| event.sequence))
            .flatten();
        Ok(ProgressReplayPage { events, next_after })
    }
}

impl ProgressRepositoryError {
    fn from_db(error: DbError) -> Self {
        Self::Database(error)
    }

    fn database(error: turso::Error) -> Self {
        Self::Database(DbError::from(error))
    }
}

#[derive(Deserialize, Serialize)]
struct StoredProgressPayload {
    metadata: BTreeMap<String, StoredProgressMetadataValue>,
    terminal: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "SCREAMING_SNAKE_CASE")]
enum StoredProgressMetadataValue {
    Code(String),
    Count(u32),
    Flag(bool),
}

impl StoredProgressPayload {
    fn from_event(event: &NewProgressEvent) -> Result<Self, ProgressRepositoryError> {
        let metadata = event
            .metadata
            .entries
            .iter()
            .map(|(key, value)| {
                let value = match value {
                    ProgressMetadataValue::Code(code) => {
                        StoredProgressMetadataValue::Code(code.as_str().to_owned())
                    }
                    ProgressMetadataValue::Count(value) => {
                        StoredProgressMetadataValue::Count(*value)
                    }
                    ProgressMetadataValue::Flag(value) => StoredProgressMetadataValue::Flag(*value),
                };
                Ok((key.as_str().to_owned(), value))
            })
            .collect::<Result<BTreeMap<_, _>, ProgressRepositoryError>>()?;
        Ok(Self {
            metadata,
            terminal: event
                .terminal
                .map(ProgressTerminalState::as_storage)
                .map(str::to_owned),
        })
    }

    fn into_metadata(
        self,
    ) -> Result<(ProgressMetadata, Option<ProgressTerminalState>), ProgressRepositoryError> {
        let entries = self
            .metadata
            .into_iter()
            .map(|(key, value)| {
                let key = ProgressMetadataKey::new(key)
                    .map_err(|_| ProgressRepositoryError::ProgressInvariant)?;
                let value = match value {
                    StoredProgressMetadataValue::Code(value) => ProgressMetadataValue::Code(
                        ProgressMetadataCode::new(value)
                            .map_err(|_| ProgressRepositoryError::ProgressInvariant)?,
                    ),
                    StoredProgressMetadataValue::Count(value) => {
                        ProgressMetadataValue::Count(value)
                    }
                    StoredProgressMetadataValue::Flag(value) => ProgressMetadataValue::Flag(value),
                };
                Ok((key, value))
            })
            .collect::<Result<BTreeMap<_, _>, ProgressRepositoryError>>()?;
        let metadata = ProgressMetadata::new(entries)
            .map_err(|_| ProgressRepositoryError::ProgressInvariant)?;
        let terminal = self
            .terminal
            .as_deref()
            .map(ProgressTerminalState::parse)
            .transpose()?;
        Ok((metadata, terminal))
    }
}

async fn ensure_job_exists(
    connection: &Connection,
    job_id: &JobId,
) -> Result<(), ProgressRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT 1 FROM jobs WHERE id = ?1 LIMIT 1",
            [job_id.as_str()],
        )
        .await
        .map_err(ProgressRepositoryError::database)?;
    if rows
        .next()
        .await
        .map_err(ProgressRepositoryError::database)?
        .is_some()
    {
        Ok(())
    } else {
        Err(ProgressRepositoryError::JobNotFound)
    }
}

async fn ensure_attempt_belongs_to_job(
    connection: &Connection,
    event: &NewProgressEvent,
) -> Result<(), ProgressRepositoryError> {
    let Some(attempt_id) = &event.attempt_id else {
        return Ok(());
    };
    let mut rows = connection
        .query(
            "SELECT job_id FROM job_attempts WHERE id = ?1 LIMIT 1",
            [attempt_id.as_str()],
        )
        .await
        .map_err(ProgressRepositoryError::database)?;
    let row = rows
        .next()
        .await
        .map_err(ProgressRepositoryError::database)?
        .ok_or(ProgressRepositoryError::AttemptNotFound)?;
    let attempt_job_id: String = row.get(0).map_err(ProgressRepositoryError::database)?;
    if attempt_job_id == event.job_id.as_str() {
        Ok(())
    } else {
        Err(ProgressRepositoryError::AttemptJobMismatch)
    }
}

async fn ensure_stream_open(
    connection: &Connection,
    job_id: &JobId,
) -> Result<(), ProgressRepositoryError> {
    let mut rows = connection
        .query(
            "SELECT payload_json FROM job_progress_events WHERE job_id = ?1 ORDER BY sequence DESC LIMIT 1",
            [job_id.as_str()],
        )
        .await
        .map_err(ProgressRepositoryError::database)?;
    let Some(row) = rows
        .next()
        .await
        .map_err(ProgressRepositoryError::database)?
    else {
        return Ok(());
    };
    let payload_json: String = row.get(0).map_err(ProgressRepositoryError::database)?;
    let payload: StoredProgressPayload = serde_json::from_str(&payload_json)
        .map_err(|_| ProgressRepositoryError::ProgressInvariant)?;
    let (_, terminal) = payload.into_metadata()?;
    if terminal.is_some() {
        Err(ProgressRepositoryError::TerminalStreamClosed)
    } else {
        Ok(())
    }
}

async fn allocate_sequence(
    connection: &Connection,
    job_id: &JobId,
) -> Result<ProgressSequence, ProgressRepositoryError> {
    let row = connection
        .prepare("SELECT COALESCE(MAX(sequence), 0) FROM job_progress_events WHERE job_id = ?1")
        .await
        .map_err(ProgressRepositoryError::database)?
        .query_row([job_id.as_str()])
        .await
        .map_err(ProgressRepositoryError::database)?;
    let previous: i64 = row.get(0).map_err(ProgressRepositoryError::database)?;
    let next = previous
        .checked_add(1)
        .ok_or(ProgressRepositoryError::ProgressInvariant)?;
    let next = u64::try_from(next).map_err(|_| ProgressRepositoryError::ProgressInvariant)?;
    ProgressSequence::new(next).map_err(|_| ProgressRepositoryError::ProgressInvariant)
}

fn progress_event_from_row(row: &turso::Row) -> Result<ProgressEvent, ProgressRepositoryError> {
    let id: String = row.get(0).map_err(ProgressRepositoryError::database)?;
    Uuid::parse_str(&id).map_err(|_| ProgressRepositoryError::ProgressInvariant)?;
    let job_id = JobId::from_stored(row.get(1).map_err(ProgressRepositoryError::database)?);
    let attempt_id: Option<String> = row.get(2).map_err(ProgressRepositoryError::database)?;
    let attempt_id = attempt_id
        .map(ProgressAttemptId::new)
        .transpose()
        .map_err(|_| ProgressRepositoryError::ProgressInvariant)?;
    let sequence = row
        .get::<i64>(3)
        .map_err(ProgressRepositoryError::database)?;
    let sequence =
        u64::try_from(sequence).map_err(|_| ProgressRepositoryError::ProgressInvariant)?;
    let sequence =
        ProgressSequence::new(sequence).map_err(|_| ProgressRepositoryError::ProgressInvariant)?;
    let key = ProgressKey::new(
        row.get::<String>(4)
            .map_err(ProgressRepositoryError::database)?,
    )
    .map_err(|_| ProgressRepositoryError::ProgressInvariant)?;
    let payload_json: String = row.get(5).map_err(ProgressRepositoryError::database)?;
    let payload: StoredProgressPayload = serde_json::from_str(&payload_json)
        .map_err(|_| ProgressRepositoryError::ProgressInvariant)?;
    let (metadata, terminal) = payload.into_metadata()?;
    Ok(ProgressEvent {
        id: ProgressEventId(id),
        job_id,
        attempt_id,
        sequence,
        key,
        metadata,
        terminal,
        created_at: row.get(6).map_err(ProgressRepositoryError::database)?,
    })
}

fn has_sensitive_name(value: &str) -> bool {
    [
        "token",
        "secret",
        "password",
        "credential",
        "authorization",
        "cookie",
        "connection",
        "request",
        "response",
        "body",
        "record",
    ]
    .iter()
    .any(|sensitive| value.contains(sensitive))
}
