use serde::{Deserialize, Serialize};

use crate::{CrawlRunStatus, CrawlRunType};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExtractionHealth {
    NotRequired,
    NotEvaluated,
    Healthy,
    CriticalFailure,
    ProductionBreakingSchemaDrift,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompleteSnapshotReason {
    NonProductionRun,
    RunNotSuccessful,
    InScopeWorkIncomplete,
    PaginationTruncated,
    UnresolvedPartialWork,
    PageTypeAmbiguity,
    ExtractionHealthNotEvaluated,
    CriticalExtractionFailure,
    ProductionBreakingSchemaDrift,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompleteSnapshotStructuralInput {
    pub run_type: CrawlRunType,
    pub status: CrawlRunStatus,
    pub in_scope_pages_planned: u64,
    pub in_scope_pages_completed: u64,
    pub pagination_truncation_count: u64,
    pub unresolved_partial_work_count: u64,
    pub page_type_ambiguity_count: u64,
    pub extraction_health: ExtractionHealth,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CompleteSnapshotStructuralInputError {
    #[error("completed in-scope pages cannot exceed planned pages")]
    CompletedExceedsPlanned,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "decision")]
pub enum CompleteSnapshotStructuralDecision {
    Complete,
    Incomplete {
        reasons: Vec<CompleteSnapshotReason>,
    },
}

impl CompleteSnapshotStructuralInput {
    /// Evaluates whether the structural facts permit a complete snapshot.
    ///
    /// # Errors
    /// Returns an input error when the completed-page count exceeds the
    /// planned-page count, because that input cannot describe a real run.
    pub fn decide(
        &self,
    ) -> Result<CompleteSnapshotStructuralDecision, CompleteSnapshotStructuralInputError> {
        if self.in_scope_pages_completed > self.in_scope_pages_planned {
            return Err(CompleteSnapshotStructuralInputError::CompletedExceedsPlanned);
        }
        let mut reasons = Vec::new();
        if self.run_type != CrawlRunType::ProductionRun {
            reasons.push(CompleteSnapshotReason::NonProductionRun);
        }
        if self.status != CrawlRunStatus::Succeeded {
            reasons.push(CompleteSnapshotReason::RunNotSuccessful);
        }
        if self.in_scope_pages_completed < self.in_scope_pages_planned {
            reasons.push(CompleteSnapshotReason::InScopeWorkIncomplete);
        }
        if self.pagination_truncation_count > 0 {
            reasons.push(CompleteSnapshotReason::PaginationTruncated);
        }
        if self.unresolved_partial_work_count > 0 {
            reasons.push(CompleteSnapshotReason::UnresolvedPartialWork);
        }
        if self.page_type_ambiguity_count > 0 {
            reasons.push(CompleteSnapshotReason::PageTypeAmbiguity);
        }
        match self.extraction_health {
            ExtractionHealth::NotRequired | ExtractionHealth::Healthy => {}
            ExtractionHealth::NotEvaluated => {
                reasons.push(CompleteSnapshotReason::ExtractionHealthNotEvaluated);
            }
            ExtractionHealth::CriticalFailure => {
                reasons.push(CompleteSnapshotReason::CriticalExtractionFailure);
            }
            ExtractionHealth::ProductionBreakingSchemaDrift => {
                reasons.push(CompleteSnapshotReason::ProductionBreakingSchemaDrift);
            }
        }
        if reasons.is_empty() {
            Ok(CompleteSnapshotStructuralDecision::Complete)
        } else {
            Ok(CompleteSnapshotStructuralDecision::Incomplete { reasons })
        }
    }
}
