//! Bounded repositories for Plan 02-owned persistence contracts.

mod artifact;
mod configuration;
mod crawler;
mod identity;
mod job;
mod progress;
mod run;

pub use artifact::ArtifactRepository;
pub use configuration::ConfigurationRepository;
pub use crawler::{CrawlerPointers, CrawlerRepository};
pub use identity::JobIdParseError;
pub use job::{
    AcquiredJob, AttemptOutcome, ConcurrencyState, JobAttempt, JobFailureCode, JobId, JobKind,
    JobLease, JobRecord, JobRepository, JobRepositoryError, JobState, NewJob, StaleJobRecovery,
};
pub use progress::{
    NewProgressEvent, ProgressAttemptId, ProgressEvent, ProgressEventId, ProgressKey,
    ProgressMetadata, ProgressMetadataCode, ProgressMetadataKey, ProgressMetadataValue,
    ProgressReplayPage, ProgressReplayRequest, ProgressRepository, ProgressRepositoryError,
    ProgressSequence, ProgressTerminalState,
};
pub use run::CrawlRunRepository;
