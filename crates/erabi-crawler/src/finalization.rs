//! Durable crawl structural-facts reconstruction.
//!
//! This module is a deep read-side boundary. It reconstructs crawl facts from
//! durable execution, discovery, traversal-control, and logical-work records.
//! Recovery checkpoint bytes are control evidence and are intentionally not a
//! source of structural truth here.

use std::collections::{BTreeMap, BTreeSet};

use erabi_db::repositories::{
    CrawlAdmissionState, CrawlExecutionRecord, CrawlTraversalControl, CrawlUrlStateRecord,
    CrawlWorkState, DiscoveredUrlRecord,
};
use erabi_domain::{
    CrawlExecutionOutcome, CrawlRunId, CrawlRunSnapshot, CrawlRunStatus, CrawlRunType,
};

/// Crawl-only structural facts reconstructed from authoritative durable
/// execution, discovery, traversal-control, and logical-work evidence.
///
/// This type contains no recovery checkpoint or extraction-health state.
/// Production callers compose it with extraction health at their workflow
/// boundary before asking the domain to decide trusted snapshot semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlStructuralFacts {
    pub status: CrawlRunStatus,
    pub in_scope_pages_planned: u64,
    pub in_scope_pages_completed: u64,
    pub pagination_truncation_count: u64,
    pub unresolved_partial_work_count: u64,
    pub page_type_ambiguity_count: u64,
}

/// Typed failure when durable crawl evidence cannot describe one coherent run.
#[derive(Debug, thiserror::Error)]
pub enum CrawlStructuralFactsError {
    #[error("durable crawl structural state is inconsistent")]
    InconsistentDurableState,
}

/// Reconstructs crawl-only structural facts from durable execution, discovery,
/// traversal-control, and logical-work evidence.
///
/// Logical work supersedes historical execution outcomes when present.
/// Canonical URL identity is the physical-page counting key; historical
/// attempts do not inflate counts. Recovery checkpoint bytes are not accepted
/// as a fallback authority.
///
/// # Errors
/// Returns [`CrawlStructuralFactsError::InconsistentDurableState`] when
/// durable evidence disagrees about run identity, URL validity, or structural
/// counts.
#[allow(clippy::too_many_lines)]
pub fn reconstruct_crawl_structural_facts(
    snapshot: &CrawlRunSnapshot,
    current_status: CrawlRunStatus,
    executions: &[CrawlExecutionRecord],
    discovered_urls: &[DiscoveredUrlRecord],
    control: Option<&CrawlTraversalControl>,
    work: Option<&[CrawlUrlStateRecord]>,
) -> Result<CrawlStructuralFacts, CrawlStructuralFactsError> {
    let expected_run_id = expected_run_id(executions, discovered_urls, control, work);
    for execution in executions {
        validate_execution(execution)?;
        ensure_run_id(expected_run_id, execution.crawl_run_id)?;
    }
    for discovered in discovered_urls {
        ensure_run_id(expected_run_id, discovered.crawl_run_id)?;
        validate_fragment_free(&discovered.original_url)?;
        validate_fragment_free(&discovered.canonical_url)?;
    }
    if let Some(control) = control {
        ensure_run_id(expected_run_id, control.crawl_run_id)?;
    }
    if let Some(work) = work {
        for state in work {
            ensure_run_id(expected_run_id, state.crawl_run_id)?;
        }
    }

    let mut planned = BTreeSet::new();
    for execution in executions {
        planned.insert(execution.canonical_url.clone());
    }
    if let Some(work) = work {
        for state in work {
            if state.admission_state == CrawlAdmissionState::Admitted {
                planned.insert(state.canonical_url.clone());
            }
        }
    } else {
        for discovered in discovered_urls {
            if discovered.status == "ADMITTED" {
                planned.insert(discovered.canonical_url.clone());
            }
        }
    }

    let mut completed = BTreeSet::new();
    let mut unresolved = BTreeSet::new();
    if let Some(work) = work {
        for state in work {
            if state.admission_state != CrawlAdmissionState::Admitted {
                continue;
            }
            match state.current_work_state {
                Some(CrawlWorkState::Completed) => {
                    completed.insert(state.canonical_url.clone());
                }
                Some(CrawlWorkState::Partial) => {
                    completed.insert(state.canonical_url.clone());
                    unresolved.insert(state.canonical_url.clone());
                }
                Some(
                    CrawlWorkState::Pending
                    | CrawlWorkState::Running
                    | CrawlWorkState::Failed
                    | CrawlWorkState::Cancelled,
                )
                | None => {
                    unresolved.insert(state.canonical_url.clone());
                }
            }
        }
    } else {
        let mut current_outcomes = BTreeMap::<String, DurablePageOutcome>::new();
        for execution in executions {
            let outcome = match execution.outcome {
                CrawlExecutionOutcome::Completed => DurablePageOutcome::Completed,
                CrawlExecutionOutcome::Partial => DurablePageOutcome::Partial,
                CrawlExecutionOutcome::Failed | CrawlExecutionOutcome::Cancelled => {
                    DurablePageOutcome::Failed
                }
            };
            current_outcomes
                .entry(execution.canonical_url.clone())
                .and_modify(|current| {
                    if outcome.rank() > current.rank() {
                        *current = outcome;
                    }
                })
                .or_insert(outcome);
        }
        for (canonical_url, outcome) in current_outcomes {
            match outcome {
                DurablePageOutcome::Completed => {
                    completed.insert(canonical_url);
                }
                DurablePageOutcome::Partial => {
                    completed.insert(canonical_url.clone());
                    unresolved.insert(canonical_url);
                }
                DurablePageOutcome::Failed => {
                    unresolved.insert(canonical_url);
                }
            }
        }
    }

    let mut ambiguous = BTreeSet::new();
    if let Some(work) = work {
        for state in work {
            if state.preserve_reason.as_deref() == Some("AMBIGUOUS_PAGE_TYPE") {
                ambiguous.insert(state.canonical_url.clone());
            }
        }
    }
    // Ambiguity is append-only Page Type evaluation evidence. It remains
    // durable even when no executable logical unit is admitted for it.
    for discovered in discovered_urls {
        if discovered.status == "AMBIGUOUS_PAGE_TYPE" {
            ambiguous.insert(discovered.canonical_url.clone());
        }
    }

    let pagination_truncation_count = control.map_or(0, |value| value.pagination_truncation_count);
    let planned_count = count(planned.len())?;
    let completed_count = count(completed.len())?;
    ensure_counts_are_coherent(planned_count, completed_count)?;
    let unresolved_count = count(unresolved.len())?
        .checked_add(u64::from(
            control.is_some_and(|value| value.duration_work_not_expanded),
        ))
        .ok_or(CrawlStructuralFactsError::InconsistentDurableState)?;
    let ambiguity_count = count(ambiguous.len())?;

    let structurally_incomplete = completed_count < planned_count
        || unresolved_count > 0
        || pagination_truncation_count > 0
        || ambiguity_count > 0;
    let status = match current_status {
        CrawlRunStatus::Cancelled => CrawlRunStatus::Cancelled,
        CrawlRunStatus::Failed => CrawlRunStatus::Failed,
        CrawlRunStatus::PartialResult => CrawlRunStatus::PartialResult,
        CrawlRunStatus::Queued | CrawlRunStatus::Running | CrawlRunStatus::Succeeded => {
            if snapshot.run_type() == CrawlRunType::QuickScrape {
                quick_scrape_current_terminal_status(work).unwrap_or(if structurally_incomplete {
                    CrawlRunStatus::PartialResult
                } else {
                    CrawlRunStatus::Succeeded
                })
            } else if structurally_incomplete {
                CrawlRunStatus::PartialResult
            } else {
                CrawlRunStatus::Succeeded
            }
        }
    };
    Ok(CrawlStructuralFacts {
        status,
        in_scope_pages_planned: planned_count,
        in_scope_pages_completed: completed_count,
        pagination_truncation_count,
        unresolved_partial_work_count: unresolved_count,
        page_type_ambiguity_count: ambiguity_count,
    })
}

fn expected_run_id(
    executions: &[CrawlExecutionRecord],
    discovered_urls: &[DiscoveredUrlRecord],
    control: Option<&CrawlTraversalControl>,
    work: Option<&[CrawlUrlStateRecord]>,
) -> Option<CrawlRunId> {
    work.and_then(|values| values.first().map(|value| value.crawl_run_id))
        .or_else(|| executions.first().map(|value| value.crawl_run_id))
        .or_else(|| discovered_urls.first().map(|value| value.crawl_run_id))
        .or_else(|| control.map(|value| value.crawl_run_id))
}

fn ensure_run_id(
    expected_run_id: Option<CrawlRunId>,
    actual_run_id: CrawlRunId,
) -> Result<(), CrawlStructuralFactsError> {
    if expected_run_id.is_some_and(|expected| expected != actual_run_id) {
        return Err(CrawlStructuralFactsError::InconsistentDurableState);
    }
    Ok(())
}

fn count(value: usize) -> Result<u64, CrawlStructuralFactsError> {
    u64::try_from(value).map_err(|_| CrawlStructuralFactsError::InconsistentDurableState)
}

fn ensure_counts_are_coherent(
    planned: u64,
    completed: u64,
) -> Result<(), CrawlStructuralFactsError> {
    if completed > planned {
        return Err(CrawlStructuralFactsError::InconsistentDurableState);
    }
    Ok(())
}

fn quick_scrape_current_terminal_status(
    work: Option<&[CrawlUrlStateRecord]>,
) -> Option<CrawlRunStatus> {
    let work = work?;
    if work.iter().any(|state| {
        state.admission_state == CrawlAdmissionState::Admitted
            && state.current_work_state == Some(CrawlWorkState::Failed)
    }) {
        return Some(CrawlRunStatus::Failed);
    }
    if work.iter().any(|state| {
        state.admission_state == CrawlAdmissionState::Admitted
            && state.current_work_state == Some(CrawlWorkState::Cancelled)
    }) {
        return Some(CrawlRunStatus::Cancelled);
    }
    None
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum DurablePageOutcome {
    Failed,
    Partial,
    Completed,
}

impl DurablePageOutcome {
    const fn rank(self) -> u8 {
        match self {
            Self::Failed => 0,
            Self::Partial => 1,
            Self::Completed => 2,
        }
    }
}

fn validate_execution(record: &CrawlExecutionRecord) -> Result<(), CrawlStructuralFactsError> {
    validate_fragment_free(&record.requested_url)?;
    validate_fragment_free(&record.canonical_url)?;
    if record
        .observed_final_url
        .as_deref()
        .is_some_and(|value| value.contains('#'))
    {
        return Err(CrawlStructuralFactsError::InconsistentDurableState);
    }
    Ok(())
}

fn validate_fragment_free(value: &str) -> Result<(), CrawlStructuralFactsError> {
    if value.contains('#') || url::Url::parse(value).is_err() {
        return Err(CrawlStructuralFactsError::InconsistentDurableState);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use erabi_db::repositories::{
        CrawlAdmissionState, CrawlExecutionRecord, CrawlTraversalControl, CrawlUrlStateRecord,
        CrawlWorkState, DiscoveredUrlRecord,
    };
    use erabi_domain::{
        CrawlExecutionId, CrawlRunId, CrawlRunSnapshotDraft, ResolvedValue, RobotsAudit,
        RunConfiguration, SettingSource, SnapshotOperationalSettings,
    };

    use super::*;

    fn snapshot() -> Result<CrawlRunSnapshot, Box<dyn std::error::Error>> {
        fn resolved<T>(value: T) -> ResolvedValue<T> {
            ResolvedValue {
                value,
                source: SettingSource::BuiltInDefault,
            }
        }
        Ok(CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
            run_type: CrawlRunType::QuickScrape,
            configuration: RunConfiguration::QuickScrape {
                target_url: "https://example.test/page".parse()?,
                ad_hoc_configuration: BTreeMap::new(),
            },
            selected_seed_ids: Vec::new(),
            run_profile_id: None,
            settings: SnapshotOperationalSettings {
                max_pages: resolved(1),
                max_depth: resolved(0),
                max_duration_seconds: resolved(60),
                concurrency: resolved(1),
                request_delay_ms: resolved(0),
                timeout_ms: resolved(1_000),
                screenshot: resolved(false),
                asset_download_limit_bytes: resolved(1_000),
                retain_artifacts: resolved(false),
                user_agent: resolved("Erabi/0.1".to_owned()),
            },
            robots: RobotsAudit::respect(
                "operator",
                "unix:1",
                "https://example.test",
                "Erabi/0.1",
                None,
            ),
            actor: "operator".to_owned(),
            created_at: "unix:1".to_owned(),
        })?)
    }

    fn execution(run_id: CrawlRunId, outcome: CrawlExecutionOutcome) -> CrawlExecutionRecord {
        CrawlExecutionRecord {
            id: CrawlExecutionId::new(),
            crawl_run_id: run_id,
            requested_url: "https://example.test/page".to_owned(),
            canonical_url: "https://example.test/page".to_owned(),
            observed_final_url: None,
            source_id: None,
            page_type_id: None,
            transition_id: None,
            discovered_url_id: None,
            outcome,
            error_code: None,
            http_status: Some(200),
            media_type: Some("text/html".to_owned()),
            content_length_bytes: Some(1),
            provider_elapsed_ms: Some(1),
            artifacts: Vec::new(),
        }
    }

    fn quick_work(
        run_id: CrawlRunId,
        state: CrawlWorkState,
        generation: u64,
    ) -> CrawlUrlStateRecord {
        CrawlUrlStateRecord {
            id: format!("quick:{run_id}"),
            crawl_run_id: run_id,
            canonical_url: "https://example.test/page".to_owned(),
            first_discovered_url_id: None,
            requested_url: "https://example.test/page".to_owned(),
            parent_url_state_id: None,
            parent_discovered_url_id: None,
            admission_state: CrawlAdmissionState::Admitted,
            preserve_reason: None,
            resolved_to_url_state_id: None,
            admission_sequence: Some(0),
            depth: Some(0),
            target_page_type_id: None,
            transition_id: None,
            pagination: false,
            final_canonical_url: None,
            current_work_state: Some(state),
            work_generation: generation,
            current_execution_id: Some("current-execution".to_owned()),
            seed_provenance: Vec::new(),
            seen: true,
            sampled: state != CrawlWorkState::Pending,
            expanded: false,
            in_scope: false,
            page_type_match_state: None,
        }
    }

    fn control(run_id: CrawlRunId) -> CrawlTraversalControl {
        CrawlTraversalControl {
            crawl_run_id: run_id,
            consumed_bytes: 0,
            raw_link_count: 0,
            duplicate_count: 0,
            robots_excluded_count: 0,
            provider_error_count: 0,
            external_url_count: 0,
            blocked_url_count: 0,
            peak_expansion_count: 0,
            elapsed_millis: 0,
            time_budget_hit: false,
            duration_work_not_expanded: false,
            pagination_truncation_count: 0,
            next_admission_sequence: 1,
        }
    }

    fn discovered_for(run_id: CrawlRunId, status: &str) -> DiscoveredUrlRecord {
        DiscoveredUrlRecord {
            id: "discovered-1".to_owned(),
            crawl_run_id: run_id,
            source_id: None,
            raw_href: None,
            original_url: "https://example.test/page".to_owned(),
            canonical_url: "https://example.test/page".to_owned(),
            status: status.to_owned(),
            discovered_at: "unix:1".to_owned(),
            detail: serde_json::json!({}),
        }
    }

    #[test]
    fn complete_coherent_durable_work_reconstructs_facts_without_checkpoint_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let run_id = CrawlRunId::new();
        let facts = reconstruct_crawl_structural_facts(
            &snapshot()?,
            CrawlRunStatus::Running,
            &[execution(run_id, CrawlExecutionOutcome::Completed)],
            &[],
            None,
            None,
        )?;
        assert_eq!(facts.status, CrawlRunStatus::Succeeded);
        assert_eq!(facts.in_scope_pages_planned, 1);
        assert_eq!(facts.in_scope_pages_completed, 1);
        assert_eq!(facts.unresolved_partial_work_count, 0);
        Ok(())
    }

    #[test]
    fn historical_failure_followed_by_current_success_is_completed()
    -> Result<(), Box<dyn std::error::Error>> {
        let run_id = CrawlRunId::new();
        let records = vec![
            execution(run_id, CrawlExecutionOutcome::Failed),
            execution(run_id, CrawlExecutionOutcome::Completed),
        ];
        let facts = reconstruct_crawl_structural_facts(
            &snapshot()?,
            CrawlRunStatus::Running,
            &records,
            &[],
            None,
            None,
        )?;
        assert_eq!(facts.in_scope_pages_completed, 1);
        assert_eq!(facts.unresolved_partial_work_count, 0);
        assert_eq!(facts.status, CrawlRunStatus::Succeeded);
        Ok(())
    }

    #[test]
    fn current_logical_work_supersedes_historical_attempt_outcomes()
    -> Result<(), Box<dyn std::error::Error>> {
        let run_id = CrawlRunId::new();
        let current = quick_work(run_id, CrawlWorkState::Completed, 2);
        let facts = reconstruct_crawl_structural_facts(
            &snapshot()?,
            CrawlRunStatus::Running,
            &[execution(run_id, CrawlExecutionOutcome::Failed)],
            &[],
            Some(&control(run_id)),
            Some(&[current]),
        )?;
        assert_eq!(facts.in_scope_pages_completed, 1);
        assert_eq!(facts.unresolved_partial_work_count, 0);
        assert_eq!(facts.status, CrawlRunStatus::Succeeded);
        Ok(())
    }

    #[test]
    fn partial_logical_work_counts_as_completed_and_unresolved()
    -> Result<(), Box<dyn std::error::Error>> {
        let run_id = CrawlRunId::new();
        let partial = quick_work(run_id, CrawlWorkState::Partial, 1);
        let facts = reconstruct_crawl_structural_facts(
            &snapshot()?,
            CrawlRunStatus::Running,
            &[],
            &[],
            Some(&control(run_id)),
            Some(&[partial]),
        )?;
        assert_eq!(facts.in_scope_pages_planned, 1);
        assert_eq!(facts.in_scope_pages_completed, 1);
        assert_eq!(facts.unresolved_partial_work_count, 1);
        assert_eq!(facts.status, CrawlRunStatus::PartialResult);
        Ok(())
    }

    #[test]
    fn pagination_truncation_comes_from_traversal_control() -> Result<(), Box<dyn std::error::Error>>
    {
        let run_id = CrawlRunId::new();
        let mut traversal = control(run_id);
        traversal.pagination_truncation_count = 2;
        let facts = reconstruct_crawl_structural_facts(
            &snapshot()?,
            CrawlRunStatus::Running,
            &[],
            &[],
            Some(&traversal),
            None,
        )?;
        assert_eq!(facts.pagination_truncation_count, 2);
        assert_eq!(facts.unresolved_partial_work_count, 0);
        assert_eq!(facts.status, CrawlRunStatus::PartialResult);
        Ok(())
    }

    #[test]
    fn ambiguity_comes_from_durable_discovery_evidence() -> Result<(), Box<dyn std::error::Error>> {
        let run_id = CrawlRunId::new();
        let facts = reconstruct_crawl_structural_facts(
            &snapshot()?,
            CrawlRunStatus::Running,
            &[],
            &[discovered_for(run_id, "AMBIGUOUS_PAGE_TYPE")],
            Some(&control(run_id)),
            None,
        )?;
        assert_eq!(facts.page_type_ambiguity_count, 1);
        assert_eq!(facts.status, CrawlRunStatus::PartialResult);
        Ok(())
    }

    #[test]
    fn ambiguity_comes_from_durable_logical_work_evidence() -> Result<(), Box<dyn std::error::Error>>
    {
        let run_id = CrawlRunId::new();
        let mut work = quick_work(run_id, CrawlWorkState::Completed, 1);
        work.preserve_reason = Some("AMBIGUOUS_PAGE_TYPE".to_owned());
        let facts = reconstruct_crawl_structural_facts(
            &snapshot()?,
            CrawlRunStatus::Running,
            &[],
            &[],
            Some(&control(run_id)),
            Some(&[work]),
        )?;
        assert_eq!(facts.page_type_ambiguity_count, 1);
        Ok(())
    }

    #[test]
    fn cross_run_durable_evidence_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let first_run = CrawlRunId::new();
        let second_run = CrawlRunId::new();
        let result = reconstruct_crawl_structural_facts(
            &snapshot()?,
            CrawlRunStatus::Running,
            &[execution(first_run, CrawlExecutionOutcome::Completed)],
            &[discovered_for(second_run, "ADMITTED")],
            None,
            None,
        );
        assert!(matches!(
            result,
            Err(CrawlStructuralFactsError::InconsistentDurableState)
        ));
        Ok(())
    }

    #[test]
    fn completed_greater_than_planned_fails_closed() {
        assert!(matches!(
            ensure_counts_are_coherent(0, 1),
            Err(CrawlStructuralFactsError::InconsistentDurableState)
        ));
    }

    #[test]
    fn terminal_failed_and_cancelled_statuses_remain_terminal()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = snapshot()?;
        for status in [CrawlRunStatus::Failed, CrawlRunStatus::Cancelled] {
            let facts =
                reconstruct_crawl_structural_facts(&snapshot, status, &[], &[], None, None)?;
            assert_eq!(facts.status, status);
        }
        Ok(())
    }

    #[test]
    fn quick_scrape_current_failure_and_cancellation_remain_terminal()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = snapshot()?;
        for (work_state, expected_status) in [
            (CrawlWorkState::Failed, CrawlRunStatus::Failed),
            (CrawlWorkState::Cancelled, CrawlRunStatus::Cancelled),
        ] {
            let run_id = CrawlRunId::new();
            let work = quick_work(run_id, work_state, 1);
            let facts = reconstruct_crawl_structural_facts(
                &snapshot,
                CrawlRunStatus::Running,
                &[],
                &[],
                Some(&control(run_id)),
                Some(&[work]),
            )?;
            assert_eq!(facts.status, expected_status);
        }
        Ok(())
    }

    #[test]
    fn control_duration_work_is_counted_without_pagination_double_counting()
    -> Result<(), Box<dyn std::error::Error>> {
        let run_id = CrawlRunId::new();
        let mut traversal = control(run_id);
        traversal.pagination_truncation_count = 1;
        traversal.duration_work_not_expanded = false;
        let facts = reconstruct_crawl_structural_facts(
            &snapshot()?,
            CrawlRunStatus::Running,
            &[],
            &[],
            Some(&traversal),
            None,
        )?;
        assert_eq!(facts.pagination_truncation_count, 1);
        assert_eq!(facts.unresolved_partial_work_count, 0);

        traversal.duration_work_not_expanded = true;
        let facts = reconstruct_crawl_structural_facts(
            &snapshot()?,
            CrawlRunStatus::Running,
            &[],
            &[],
            Some(&traversal),
            None,
        )?;
        assert_eq!(facts.pagination_truncation_count, 1);
        assert_eq!(facts.unresolved_partial_work_count, 1);
        Ok(())
    }

    #[test]
    fn control_run_identity_must_agree_with_other_durable_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let first_run = CrawlRunId::new();
        let second_run = CrawlRunId::new();
        let result = reconstruct_crawl_structural_facts(
            &snapshot()?,
            CrawlRunStatus::Running,
            &[execution(first_run, CrawlExecutionOutcome::Completed)],
            &[],
            Some(&control(second_run)),
            None,
        );
        assert!(matches!(
            result,
            Err(CrawlStructuralFactsError::InconsistentDurableState)
        ));
        Ok(())
    }
}
