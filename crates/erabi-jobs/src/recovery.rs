//! Shared crawl recovery validation for explicit and automatic execution.

use erabi_crawler::{
    CrawlRecoveryCheckpoint, CrawlRecoveryCheckpointError, CrawlStructuralFacts,
    CrawlStructuralFactsError, reconstruct_crawl_structural_facts,
};
use erabi_db::repositories::{
    CheckpointRecord, CheckpointRepositoryError, CrawlExecutionRecord, DiscoveredUrlRecord,
    JobRepositoryError, ReconstructedTraversalState,
};
use erabi_domain::{CrawlRunId, CrawlRunSnapshot, CrawlRunStatus};

/// The one jobs-layer interpretation of a same-run crawl recovery checkpoint.
/// The checkpoint supplies recovery identity/control; `facts` proves the
/// corresponding durable crawl state is coherent enough for execution.
#[derive(Clone, Debug)]
pub(crate) struct ValidatedCrawlRecovery {
    pub(crate) facts: CrawlStructuralFacts,
}

/// Safe semantic categories shared by action and runtime diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CrawlRecoveryValidationError {
    Missing,
    FormatUnsupported,
    Malformed,
    IdentityMismatch,
    StateInvalid,
}

impl CrawlRecoveryValidationError {
    pub(crate) const fn diagnostic_code(self) -> &'static str {
        match self {
            Self::Missing => "CHECKPOINT_MISSING",
            Self::FormatUnsupported => "CHECKPOINT_FORMAT_UNSUPPORTED",
            Self::Malformed => "CHECKPOINT_MALFORMED",
            Self::IdentityMismatch => "CHECKPOINT_IDENTITY_MISMATCH",
            Self::StateInvalid => "RECOVERY_STATE_INVALID",
        }
    }
}

/// Validates current crawl recovery control and reconstructs durable facts.
///
/// This is intentionally concrete to crawl recovery. It does not decode
/// historical payloads and does not treat a generic stale-job candidate as
/// authorization to perform provider work.
pub(crate) fn validate_crawl_recovery(
    checkpoint: Option<&CheckpointRecord>,
    snapshot: &CrawlRunSnapshot,
    run_id: CrawlRunId,
    current_status: CrawlRunStatus,
    executions: &[CrawlExecutionRecord],
    discovered_urls: &[DiscoveredUrlRecord],
    durable: Option<&ReconstructedTraversalState>,
) -> Result<ValidatedCrawlRecovery, CrawlRecoveryValidationError> {
    let checkpoint = checkpoint.ok_or(CrawlRecoveryValidationError::Missing)?;
    let _checkpoint =
        CrawlRecoveryCheckpoint::from_envelope(&checkpoint.checkpoint, snapshot, run_id)
            .map_err(|error| map_crawl_checkpoint_error(&error))?;
    let durable = durable.ok_or(CrawlRecoveryValidationError::StateInvalid)?;
    let facts = reconstruct_crawl_structural_facts(
        snapshot,
        current_status,
        executions,
        discovered_urls,
        Some(&durable.control),
        Some(&durable.work),
    )
    .map_err(map_structural_facts_error)?;
    Ok(ValidatedCrawlRecovery { facts })
}

pub(crate) fn checkpoint_error_code(error: &JobRepositoryError) -> Option<&'static str> {
    match error {
        JobRepositoryError::Checkpoint(error) => map_checkpoint_repository_error(error)
            .map(CrawlRecoveryValidationError::diagnostic_code),
        _ => None,
    }
}

pub(crate) const fn map_checkpoint_repository_error(
    error: &CheckpointRepositoryError,
) -> Option<CrawlRecoveryValidationError> {
    match error {
        CheckpointRepositoryError::UnsupportedFormatVersion => {
            Some(CrawlRecoveryValidationError::FormatUnsupported)
        }
        CheckpointRepositoryError::Malformed
        | CheckpointRepositoryError::InvalidEnvelope
        | CheckpointRepositoryError::PayloadTooLarge
        | CheckpointRepositoryError::Serialization => Some(CrawlRecoveryValidationError::Malformed),
        CheckpointRepositoryError::Database(_)
        | CheckpointRepositoryError::NotFound
        | CheckpointRepositoryError::LeaseLost => None,
    }
}

fn map_crawl_checkpoint_error(
    error: &CrawlRecoveryCheckpointError,
) -> CrawlRecoveryValidationError {
    match error {
        CrawlRecoveryCheckpointError::UnsupportedFormatVersion
        | CrawlRecoveryCheckpointError::UnexpectedPayloadKind
        | CrawlRecoveryCheckpointError::UnsupportedRunType => {
            CrawlRecoveryValidationError::FormatUnsupported
        }
        CrawlRecoveryCheckpointError::MalformedPayload
        | CrawlRecoveryCheckpointError::Serialization => CrawlRecoveryValidationError::Malformed,
        CrawlRecoveryCheckpointError::IncompatibleRunIdentity => {
            CrawlRecoveryValidationError::IdentityMismatch
        }
    }
}

fn map_structural_facts_error(_error: CrawlStructuralFactsError) -> CrawlRecoveryValidationError {
    CrawlRecoveryValidationError::StateInvalid
}
