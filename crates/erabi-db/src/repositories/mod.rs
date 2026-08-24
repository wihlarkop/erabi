//! Bounded repositories for Plan 02-owned persistence contracts.

mod artifact;
mod checkpoint;
mod configuration;
mod crawler;
mod identity;
mod job;
mod progress;
mod run;

pub use artifact::ArtifactRepository;
pub use checkpoint::{
    CURRENT_CHECKPOINT_SCHEMA_VERSION, CheckpointArtifactReference, CheckpointCompatibility,
    CheckpointEnvelope, CheckpointIdentity, CheckpointPosition, CheckpointRecord,
    CheckpointRecoveryAssessment, CheckpointRecoveryDisposition, CheckpointRepository,
    CheckpointRepositoryError, CheckpointUnitId, ExtractionResumePhase, ExtractionResumeState,
    MAX_CHECKPOINT_ARTIFACTS, MAX_CHECKPOINT_BYTES, MAX_CHECKPOINT_UNITS,
};
pub use configuration::ConfigurationRepository;
pub use crawler::{CrawlerPointers, CrawlerRepository};
pub use identity::JobIdParseError;
pub use job::{
    AcquiredJob, ActionRunAssociation, AttemptOutcome, ConcurrencyState, JobAttempt,
    JobFailureCode, JobId, JobKind, JobLease, JobRecord, JobRepository, JobRepositoryError,
    JobState, NewJob, StaleJobRecovery,
};
pub use progress::{
    NewProgressEvent, ProgressAttemptId, ProgressEvent, ProgressEventId, ProgressKey,
    ProgressMetadata, ProgressMetadataCode, ProgressMetadataKey, ProgressMetadataValue,
    ProgressReplayPage, ProgressReplayRequest, ProgressRepository, ProgressRepositoryError,
    ProgressSequence, ProgressTerminalState,
};
pub use run::{CrawlRunRepository, CrawlRunRepositoryError};
