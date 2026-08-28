use erabi_domain::{
    CompleteSnapshotReason, CompleteSnapshotStructuralDecision, CompleteSnapshotStructuralInput,
    CompleteSnapshotStructuralInputError, CrawlRunStatus, CrawlRunType, ExtractionHealth,
};

fn healthy() -> CompleteSnapshotStructuralInput {
    CompleteSnapshotStructuralInput {
        run_type: CrawlRunType::ProductionRun,
        status: CrawlRunStatus::Succeeded,
        in_scope_pages_planned: 4,
        in_scope_pages_completed: 4,
        pagination_truncation_count: 0,
        unresolved_partial_work_count: 0,
        page_type_ambiguity_count: 0,
        extraction_health: ExtractionHealth::Healthy,
    }
}

#[test]
fn healthy_production_snapshot_is_complete() {
    assert_eq!(
        healthy().decide(),
        Ok(CompleteSnapshotStructuralDecision::Complete)
    );
}

#[test]
fn only_healthy_production_runs_can_be_complete() {
    for run_type in [
        CrawlRunType::QuickScrape,
        CrawlRunType::TestRun,
        CrawlRunType::DiscoveryPreview,
    ] {
        let input = CompleteSnapshotStructuralInput {
            run_type,
            ..healthy()
        };
        assert_eq!(
            input.decide(),
            Ok(CompleteSnapshotStructuralDecision::Incomplete {
                reasons: vec![CompleteSnapshotReason::NonProductionRun]
            })
        );
    }
    for status in [
        CrawlRunStatus::Queued,
        CrawlRunStatus::Running,
        CrawlRunStatus::PartialResult,
        CrawlRunStatus::Failed,
        CrawlRunStatus::Cancelled,
    ] {
        let input = CompleteSnapshotStructuralInput {
            status,
            ..healthy()
        };
        assert_eq!(
            input.decide(),
            Ok(CompleteSnapshotStructuralDecision::Incomplete {
                reasons: vec![CompleteSnapshotReason::RunNotSuccessful]
            })
        );
    }
}

#[test]
fn structural_counts_and_health_return_all_deterministic_reasons() {
    let input = CompleteSnapshotStructuralInput {
        in_scope_pages_completed: 3,
        pagination_truncation_count: 2,
        unresolved_partial_work_count: 1,
        page_type_ambiguity_count: 1,
        extraction_health: ExtractionHealth::CriticalFailure,
        ..healthy()
    };
    assert_eq!(
        input.decide(),
        Ok(CompleteSnapshotStructuralDecision::Incomplete {
            reasons: vec![
                CompleteSnapshotReason::InScopeWorkIncomplete,
                CompleteSnapshotReason::PaginationTruncated,
                CompleteSnapshotReason::UnresolvedPartialWork,
                CompleteSnapshotReason::PageTypeAmbiguity,
                CompleteSnapshotReason::CriticalExtractionFailure,
            ]
        })
    );
}

#[test]
fn extraction_health_fails_closed_without_a_generic_bypass() {
    for health in [
        ExtractionHealth::NotEvaluated,
        ExtractionHealth::CriticalFailure,
        ExtractionHealth::ProductionBreakingSchemaDrift,
    ] {
        let input = CompleteSnapshotStructuralInput {
            extraction_health: health,
            ..healthy()
        };
        assert!(matches!(
            input.decide(),
            Ok(CompleteSnapshotStructuralDecision::Incomplete { .. })
        ));
    }
    let input = CompleteSnapshotStructuralInput {
        extraction_health: ExtractionHealth::NotRequired,
        ..healthy()
    };
    assert_eq!(
        input.decide(),
        Ok(CompleteSnapshotStructuralDecision::Complete)
    );
}

#[test]
fn impossible_structural_counts_are_rejected() {
    let input = CompleteSnapshotStructuralInput {
        in_scope_pages_completed: 5,
        ..healthy()
    };
    assert_eq!(
        input.decide(),
        Err(CompleteSnapshotStructuralInputError::CompletedExceedsPlanned)
    );
}
