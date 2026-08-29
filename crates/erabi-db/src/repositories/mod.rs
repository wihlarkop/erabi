//! Bounded repositories for Plan 02-owned persistence contracts.

mod artifact;
mod checkpoint;
mod configuration;
mod crawl_execution;
mod crawler;
mod identity;
mod job;
mod progress;
mod run;
mod test_evidence;

pub use artifact::ArtifactRepository;
pub use checkpoint::{
    CURRENT_CHECKPOINT_SCHEMA_VERSION, CheckpointArtifactReference, CheckpointCompatibility,
    CheckpointEnvelope, CheckpointIdentity, CheckpointPosition, CheckpointRecord,
    CheckpointRecoveryAssessment, CheckpointRecoveryDisposition, CheckpointRepository,
    CheckpointRepositoryError, CheckpointUnitId, ExtractionResumePhase, ExtractionResumeState,
    MAX_CHECKPOINT_ARTIFACTS, MAX_CHECKPOINT_BYTES, MAX_CHECKPOINT_UNITS,
};
pub use configuration::ConfigurationRepository;
pub use crawl_execution::{
    CrawlExecutionArtifact, CrawlExecutionArtifactKind, CrawlExecutionRecord,
    CrawlExecutionRepository, CrawlExecutionRepositoryError, CrawlExecutionSummary,
};
pub use crawler::{
    CrawlerAuditMetadata, CrawlerEvaluationSnapshot, CrawlerPointers, CrawlerRepository,
    CrawlerRepositoryError, CrawlerSemanticSnapshot, CrawlerVersionRecord,
    DiscoveryTransitionRecord, PageTypeRecord, UrlMatcherRecord,
};
pub use identity::JobIdParseError;
pub use job::{
    AcquiredJob, ActionRunAssociation, AttemptOutcome, ConcurrencyState, JobAttempt,
    JobFailureCode, JobId, JobKind, JobLease, JobRecord, JobRepository, JobRepositoryError,
    JobState, JobStorageClass, NewJob, StaleJobRecovery,
};
pub use progress::{
    NewProgressEvent, ProgressAttemptId, ProgressEvent, ProgressEventId, ProgressKey,
    ProgressMetadata, ProgressMetadataCode, ProgressMetadataKey, ProgressMetadataValue,
    ProgressReplayPage, ProgressReplayRequest, ProgressRepository, ProgressRepositoryError,
    ProgressSequence, ProgressTerminalState,
};
pub use run::{CrawlRunRepository, CrawlRunRepositoryError};
pub use test_evidence::{TestEvidenceRecord, TestEvidenceRepository, TestEvidenceRepositoryError};
