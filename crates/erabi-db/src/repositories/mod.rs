//! Bounded repositories for Plan 02-owned persistence contracts.

mod artifact;
mod configuration;
mod crawler;
mod job;
mod run;

pub use artifact::ArtifactRepository;
pub use configuration::ConfigurationRepository;
pub use crawler::{CrawlerPointers, CrawlerRepository};
pub use job::{
    AcquiredJob, AttemptOutcome, ConcurrencyState, JobAttempt, JobFailureCode, JobId, JobKind,
    JobLease, JobRecord, JobRepository, JobRepositoryError, JobState, NewJob, StaleJobRecovery,
};
pub use run::CrawlRunRepository;
