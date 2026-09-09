//! Durable Production/Quick Scrape finalization.
//!
//! This module is a deep read-side module: callers provide the immutable run
//! snapshot and validated durable evidence. The canonical reconstruction path
//! returns crawl-only structural facts; the legacy public finalizers retain
//! the historical complete-snapshot compatibility surface.

use std::collections::{BTreeMap, BTreeSet};

use erabi_db::repositories::{
    CrawlAdmissionState, CrawlExecutionRecord, CrawlTraversalControl, CrawlUrlStateRecord,
    CrawlWorkState, DiscoveredUrlRecord,
};
use erabi_domain::{
    CompleteSnapshotStructuralDecision, CompleteSnapshotStructuralInput,
    CompleteSnapshotStructuralInputError, CrawlExecutionOutcome, CrawlRunSnapshot, CrawlRunStatus,
    CrawlRunType, ExtractionHealth,
};

use crate::CrawlCheckpoint;

/// Crawl-only structural facts reconstructed from authoritative durable
/// execution, discovery, traversal-control, checkpoint, and logical-work
/// evidence.
///
/// This type deliberately contains no extraction health or application-layer
/// composition. Production callers combine it with the current extraction
/// health at their workflow boundary before asking the domain to decide trust.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlStructuralFacts {
    pub status: CrawlRunStatus,
    pub in_scope_pages_planned: u64,
    pub in_scope_pages_completed: u64,
    pub pagination_truncation_count: u64,
    pub unresolved_partial_work_count: u64,
    pub page_type_ambiguity_count: u64,
}

/// Durable structural facts and the existing Plan 05 complete-snapshot result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlFinalization {
    pub structural_input: CompleteSnapshotStructuralInput,
    pub decision: CompleteSnapshotStructuralDecision,
    pub status: CrawlRunStatus,
}

/// Typed failures when durable execution evidence cannot describe a coherent
/// run. The caller must terminalize such a run as FAILED; it must not clamp or
/// silently repair the counters.
#[derive(Debug, thiserror::Error)]
pub enum CrawlFinalizationError {
    #[error("durable crawl structural state is inconsistent")]
    Invariant,
    #[error("the complete-snapshot structural input is invalid")]
    StructuralInput(#[from] CompleteSnapshotStructuralInputError),
}

/// Reconstructs crawl-only structural facts from durable execution, discovery,
/// and optional typed checkpoint evidence. Canonical URL identity is the only
/// physical-page counting key; historical attempt rows never inflate counts.
///
/// # Errors
/// Returns a typed invariant error when durable rows disagree or the existing
/// durable evidence cannot describe one coherent crawl result.
#[allow(clippy::too_many_lines)]
pub fn reconstruct_crawl_structural_facts(
    snapshot: &CrawlRunSnapshot,
    current_status: CrawlRunStatus,
    executions: &[CrawlExecutionRecord],
    discovered_urls: &[DiscoveredUrlRecord],
    checkpoint: Option<&CrawlCheckpoint>,
    control: Option<&CrawlTraversalControl>,
    work: Option<&[CrawlUrlStateRecord]>,
) -> Result<CrawlStructuralFacts, CrawlFinalizationError> {
    let mut planned = BTreeSet::new();
    let expected_run_id = checkpoint
        .map(|value| value.crawl_run_id)
        .or_else(|| work.and_then(|values| values.first().map(|value| value.crawl_run_id)))
        .or_else(|| executions.first().map(|value| value.crawl_run_id))
        .or_else(|| discovered_urls.first().map(|value| value.crawl_run_id));
    for execution in executions {
        validate_execution(execution)?;
        if expected_run_id.is_some_and(|run_id| run_id != execution.crawl_run_id) {
            return Err(CrawlFinalizationError::Invariant);
        }
        planned.insert(execution.canonical_url.clone());
    }
    for discovered in discovered_urls {
        if expected_run_id.is_some_and(|run_id| run_id != discovered.crawl_run_id) {
            return Err(CrawlFinalizationError::Invariant);
        }
        validate_fragment_free(&discovered.original_url)?;
        validate_fragment_free(&discovered.canonical_url)?;
    }
    if let Some(work) = work {
        for state in work {
            if expected_run_id.is_some_and(|run_id| run_id != state.crawl_run_id) {
                return Err(CrawlFinalizationError::Invariant);
            }
            if state.admission_state == CrawlAdmissionState::Admitted {
                planned.insert(state.canonical_url.clone());
            }
        }
    } else if let Some(checkpoint) = checkpoint {
        for unit in checkpoint
            .completed_units
            .iter()
            .chain(&checkpoint.pending_units)
            .chain(&checkpoint.failed_units)
            .chain(&checkpoint.partial_units)
        {
            planned.insert(unit.canonical_url.clone());
        }
    } else {
        for discovered in discovered_urls {
            if discovered.status == "ADMITTED" {
                validate_fragment_free(&discovered.canonical_url)?;
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
    if work.is_none()
        && let Some(checkpoint) = checkpoint
    {
        for unit in &checkpoint.pending_units {
            if completed.contains(&unit.canonical_url) {
                return Err(CrawlFinalizationError::Invariant);
            }
            unresolved.insert(unit.canonical_url.clone());
        }
        for unit in &checkpoint.failed_units {
            if completed.contains(&unit.canonical_url) {
                return Err(CrawlFinalizationError::Invariant);
            }
            unresolved.insert(unit.canonical_url.clone());
        }
        for unit in &checkpoint.partial_units {
            unresolved.insert(unit.canonical_url.clone());
        }
        for unit in &checkpoint.completed_units {
            if !completed.contains(&unit.canonical_url) {
                return Err(CrawlFinalizationError::Invariant);
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
    // Ambiguity is append-only PageType-evaluation evidence. It remains
    // durable even when no executable logical unit is admitted for it.
    for discovered in discovered_urls {
        if discovered.status == "AMBIGUOUS_PAGE_TYPE" {
            validate_fragment_free(&discovered.canonical_url)?;
            ambiguous.insert(discovered.canonical_url.clone());
        }
    }
    let pagination_truncation_count = control.map_or_else(
        || checkpoint.map_or(0, |value| value.traversal.pagination_truncation_count),
        |value| value.pagination_truncation_count,
    );
    let planned_count =
        u64::try_from(planned.len()).map_err(|_| CrawlFinalizationError::Invariant)?;
    let completed_count =
        u64::try_from(completed.len()).map_err(|_| CrawlFinalizationError::Invariant)?;
    // Pagination truncation is structural evidence in its own right, not an
    // additional unresolved work item.  In particular, a targetful
    // pagination observation at the duration boundary must not count once as
    // pagination and again as generic duration work.  The duration flag is
    // reserved by SemanticTraversal for regular unrepresented work.
    let unresolved_count = u64::try_from(unresolved.len())
        .map_err(|_| CrawlFinalizationError::Invariant)?
        .checked_add(u64::from(control.map_or_else(
            || checkpoint.is_some_and(|item| item.traversal.duration_work_not_expanded),
            |value| value.duration_work_not_expanded,
        )))
        .ok_or(CrawlFinalizationError::Invariant)?;
    let ambiguity_count =
        u64::try_from(ambiguous.len()).map_err(|_| CrawlFinalizationError::Invariant)?;
    if completed_count > planned_count {
        return Err(CrawlFinalizationError::Invariant);
    }

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

/// Finalizes durable state through the historical complete-snapshot
/// compatibility surface. The canonical crawl reconstruction is performed
/// first, then the established run-type extraction-health default is attached
/// only for this legacy wrapper.
///
/// # Errors
/// Returns an invariant or structural-input error when durable evidence cannot
/// describe one coherent crawl result.
#[allow(clippy::too_many_lines)]
pub fn finalize_durable_state(
    snapshot: &CrawlRunSnapshot,
    current_status: CrawlRunStatus,
    executions: &[CrawlExecutionRecord],
    discovered_urls: &[DiscoveredUrlRecord],
    checkpoint: Option<&CrawlCheckpoint>,
) -> Result<CrawlFinalization, CrawlFinalizationError> {
    finalize_durable_state_with_control(
        snapshot,
        current_status,
        executions,
        discovered_urls,
        checkpoint,
        None,
    )
}

/// Same durable finalization with Task 9 scalar traversal control. The
/// control row supersedes the old growing checkpoint for pagination/duration
/// facts.
///
/// # Errors
/// Returns an invariant or structural-input error when durable evidence cannot
/// describe one coherent crawl result.
#[allow(clippy::too_many_lines)]
pub fn finalize_durable_state_with_control(
    snapshot: &CrawlRunSnapshot,
    current_status: CrawlRunStatus,
    executions: &[CrawlExecutionRecord],
    discovered_urls: &[DiscoveredUrlRecord],
    checkpoint: Option<&CrawlCheckpoint>,
    control: Option<&CrawlTraversalControl>,
) -> Result<CrawlFinalization, CrawlFinalizationError> {
    finalize_durable_state_with_traversal(
        snapshot,
        current_status,
        executions,
        discovered_urls,
        checkpoint,
        control,
        None,
    )
}

/// Finalizes a Task 9 production run from the authoritative logical-work
/// projection. `work` takes precedence over append-only execution history:
/// historical failures cannot make a currently completed generation
/// unresolved again.
///
/// # Errors
/// Returns an invariant or structural-input error when durable logical work,
/// immutable snapshot, and append-only evidence disagree.
#[allow(clippy::too_many_lines)]
pub fn finalize_durable_state_with_traversal(
    snapshot: &CrawlRunSnapshot,
    current_status: CrawlRunStatus,
    executions: &[CrawlExecutionRecord],
    discovered_urls: &[DiscoveredUrlRecord],
    checkpoint: Option<&CrawlCheckpoint>,
    control: Option<&CrawlTraversalControl>,
    work: Option<&[CrawlUrlStateRecord]>,
) -> Result<CrawlFinalization, CrawlFinalizationError> {
    let facts = reconstruct_crawl_structural_facts(
        snapshot,
        current_status,
        executions,
        discovered_urls,
        checkpoint,
        control,
        work,
    )?;
    let extraction_health = match snapshot.run_type() {
        CrawlRunType::ProductionRun => ExtractionHealth::NotEvaluated,
        CrawlRunType::QuickScrape | CrawlRunType::TestRun | CrawlRunType::DiscoveryPreview => {
            ExtractionHealth::NotRequired
        }
    };
    let structural_input = CompleteSnapshotStructuralInput {
        run_type: snapshot.run_type(),
        status: facts.status,
        in_scope_pages_planned: facts.in_scope_pages_planned,
        in_scope_pages_completed: facts.in_scope_pages_completed,
        pagination_truncation_count: facts.pagination_truncation_count,
        unresolved_partial_work_count: facts.unresolved_partial_work_count,
        page_type_ambiguity_count: facts.page_type_ambiguity_count,
        extraction_health,
    };
    let decision = structural_input.decide()?;
    Ok(CrawlFinalization {
        structural_input,
        decision,
        status: facts.status,
    })
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

fn validate_execution(record: &CrawlExecutionRecord) -> Result<(), CrawlFinalizationError> {
    validate_fragment_free(&record.requested_url)?;
    validate_fragment_free(&record.canonical_url)?;
    if record
        .observed_final_url
        .as_deref()
        .is_some_and(|value| value.contains('#'))
    {
        return Err(CrawlFinalizationError::Invariant);
    }
    Ok(())
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

fn validate_fragment_free(value: &str) -> Result<(), CrawlFinalizationError> {
    if value.contains('#') || url::Url::parse(value).is_err() {
        return Err(CrawlFinalizationError::Invariant);
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
        CrawlExecutionId, CrawlRunId, CrawlRunSnapshotDraft, CrawlerId, CrawlerVersionId,
        ResolvedValue, RobotsAudit, RunConfiguration, SettingSource, SnapshotOperationalSettings,
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

    fn execution(
        run_id: erabi_domain::CrawlRunId,
        outcome: CrawlExecutionOutcome,
    ) -> CrawlExecutionRecord {
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

    fn production_snapshot() -> Result<CrawlRunSnapshot, Box<dyn std::error::Error>> {
        let crawler_version_id = CrawlerVersionId::new();
        Ok(CrawlRunSnapshot::new(CrawlRunSnapshotDraft {
            run_type: CrawlRunType::ProductionRun,
            configuration: RunConfiguration::CrawlerVersion {
                crawler_id: CrawlerId::new(),
                crawler_version_id,
                semantic_config_hash: "c".repeat(64),
            },
            selected_seed_ids: Vec::new(),
            run_profile_id: None,
            settings: snapshot()?.settings().clone(),
            robots: RobotsAudit::respect(
                "operator",
                "unix:1",
                "https://example.test",
                "Erabi/0.1",
                Some(crawler_version_id),
            ),
            actor: "operator".to_owned(),
            created_at: "unix:1".to_owned(),
        })?)
    }

    fn discovered(status: &str) -> DiscoveredUrlRecord {
        DiscoveredUrlRecord {
            id: "discovered-1".to_owned(),
            crawl_run_id: CrawlRunId::new(),
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
    fn historical_failure_followed_by_success_is_not_unresolved()
    -> Result<(), Box<dyn std::error::Error>> {
        let run_id = erabi_domain::CrawlRunId::new();
        let snapshot = snapshot()?;
        let records = vec![
            execution(run_id, CrawlExecutionOutcome::Failed),
            execution(run_id, CrawlExecutionOutcome::Completed),
        ];
        let facts = reconstruct_crawl_structural_facts(
            &snapshot,
            CrawlRunStatus::Running,
            &records,
            &[],
            None,
            None,
            None,
        )?;
        assert_eq!(facts.status, CrawlRunStatus::Succeeded);
        assert_eq!(facts.in_scope_pages_planned, 1);
        assert_eq!(facts.in_scope_pages_completed, 1);
        assert_eq!(facts.pagination_truncation_count, 0);
        assert_eq!(facts.unresolved_partial_work_count, 0);
        assert_eq!(facts.page_type_ambiguity_count, 0);
        let finalized =
            finalize_durable_state(&snapshot, CrawlRunStatus::Running, &records, &[], None)?;
        assert_eq!(finalized.status, CrawlRunStatus::Succeeded);
        assert_eq!(finalized.structural_input.in_scope_pages_planned, 1);
        assert_eq!(finalized.structural_input.in_scope_pages_completed, 1);
        assert_eq!(finalized.structural_input.unresolved_partial_work_count, 0);
        assert!(matches!(
            finalized.decision,
            CompleteSnapshotStructuralDecision::Incomplete { .. }
        ));
        Ok(())
    }

    #[test]
    fn coherent_partial_work_is_reconstructed_as_crawl_facts()
    -> Result<(), Box<dyn std::error::Error>> {
        let run_id = CrawlRunId::new();
        let snapshot = snapshot()?;
        let partial = quick_work(run_id, CrawlWorkState::Partial, 1);
        let facts = reconstruct_crawl_structural_facts(
            &snapshot,
            CrawlRunStatus::Running,
            &[],
            &[],
            None,
            Some(&control(run_id)),
            Some(&[partial]),
        )?;
        assert_eq!(facts.status, CrawlRunStatus::PartialResult);
        assert_eq!(facts.in_scope_pages_planned, 1);
        assert_eq!(facts.in_scope_pages_completed, 1);
        assert_eq!(facts.unresolved_partial_work_count, 1);
        assert_eq!(facts.pagination_truncation_count, 0);
        assert_eq!(facts.page_type_ambiguity_count, 0);
        Ok(())
    }

    #[test]
    fn quick_finalization_uses_current_generation_over_historical_outcomes()
    -> Result<(), Box<dyn std::error::Error>> {
        let run_id = CrawlRunId::new();
        let snapshot = snapshot()?;
        let historical = vec![
            execution(run_id, CrawlExecutionOutcome::Completed),
            execution(run_id, CrawlExecutionOutcome::Failed),
        ];
        let failed = quick_work(run_id, CrawlWorkState::Failed, 1);
        let finalized = finalize_durable_state_with_traversal(
            &snapshot,
            CrawlRunStatus::Running,
            &historical,
            &[],
            None,
            Some(&control(run_id)),
            Some(&[failed]),
        )?;
        assert_eq!(finalized.status, CrawlRunStatus::Failed);
        assert_eq!(finalized.structural_input.in_scope_pages_planned, 1);
        assert_eq!(finalized.structural_input.in_scope_pages_completed, 0);
        assert_eq!(finalized.structural_input.unresolved_partial_work_count, 1);

        let completed = quick_work(run_id, CrawlWorkState::Completed, 2);
        let finalized = finalize_durable_state_with_traversal(
            &snapshot,
            CrawlRunStatus::Running,
            &historical,
            &[],
            None,
            Some(&control(run_id)),
            Some(&[completed]),
        )?;
        assert_eq!(finalized.status, CrawlRunStatus::Succeeded);
        assert_eq!(finalized.structural_input.in_scope_pages_completed, 1);
        assert_eq!(finalized.structural_input.unresolved_partial_work_count, 0);
        Ok(())
    }

    #[test]
    fn checkpoint_pending_work_is_currently_incomplete() -> Result<(), Box<dyn std::error::Error>> {
        let run_id = erabi_domain::CrawlRunId::new();
        let snapshot = snapshot()?;
        let pending = crate::CrawlCheckpointUnit {
            state: crate::CrawlCheckpointUnitState::Pending,
            requested_url: "https://example.test/page".to_owned(),
            canonical_url: "https://example.test/page".to_owned(),
            discovered_url_id: None,
            depth: 0,
            page_type_id: None,
            transition_id: None,
            parent_canonical_url: None,
            final_canonical_url: None,
            pagination: false,
            seed_ids: Vec::new(),
            execution_ids: Vec::new(),
        };
        let checkpoint = crate::CrawlCheckpoint::new(
            run_id,
            &snapshot,
            crate::SemanticTraversalCheckpoint::empty(Vec::new()),
            Vec::new(),
            vec![pending],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )?;
        let finalized = finalize_durable_state(
            &snapshot,
            CrawlRunStatus::Running,
            &[],
            &[],
            Some(&checkpoint),
        )?;
        assert_eq!(finalized.status, CrawlRunStatus::PartialResult);
        assert_eq!(finalized.structural_input.unresolved_partial_work_count, 1);
        Ok(())
    }

    #[test]
    fn ambiguity_and_pagination_are_reconstructed_from_durable_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let run_id = CrawlRunId::new();
        let snapshot = snapshot()?;
        let mut traversal = crate::SemanticTraversalCheckpoint::empty(Vec::new());
        traversal.pagination_truncation_count = 2;
        let checkpoint = crate::CrawlCheckpoint::new(
            run_id,
            &snapshot,
            traversal,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )?;
        let mut ambiguity = discovered("AMBIGUOUS_PAGE_TYPE");
        ambiguity.crawl_run_id = run_id;
        let facts = reconstruct_crawl_structural_facts(
            &snapshot,
            CrawlRunStatus::Running,
            &[],
            &[ambiguity],
            Some(&checkpoint),
            None,
            None,
        )?;
        assert_eq!(facts.pagination_truncation_count, 2);
        assert_eq!(facts.page_type_ambiguity_count, 1);
        assert_eq!(facts.status, CrawlRunStatus::PartialResult);
        let finalized = finalize_durable_state(
            &snapshot,
            CrawlRunStatus::Running,
            &[],
            &[],
            Some(&checkpoint),
        )?;
        assert!(matches!(
            finalized.decision,
            CompleteSnapshotStructuralDecision::Incomplete { .. }
        ));
        Ok(())
    }

    #[test]
    fn pagination_only_duration_evidence_is_not_double_counted()
    -> Result<(), Box<dyn std::error::Error>> {
        let run_id = CrawlRunId::new();
        let snapshot = snapshot()?;
        let mut traversal = crate::SemanticTraversalCheckpoint::empty(Vec::new());
        traversal.pagination_truncation_count = 1;
        // SemanticTraversal deliberately leaves the generic duration flag
        // clear for a targetful pagination-only boundary.
        let checkpoint = crate::CrawlCheckpoint::new(
            run_id,
            &snapshot,
            traversal,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )?;
        let facts = reconstruct_crawl_structural_facts(
            &snapshot,
            CrawlRunStatus::Running,
            &[],
            &[],
            Some(&checkpoint),
            None,
            None,
        )?;
        assert_eq!(facts.pagination_truncation_count, 1);
        assert_eq!(facts.unresolved_partial_work_count, 0);
        Ok(())
    }

    #[test]
    fn contradictory_checkpoint_and_execution_evidence_is_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let run_id = CrawlRunId::new();
        let snapshot = snapshot()?;
        let pending = crate::CrawlCheckpointUnit {
            state: crate::CrawlCheckpointUnitState::Pending,
            requested_url: "https://example.test/page".to_owned(),
            canonical_url: "https://example.test/page".to_owned(),
            discovered_url_id: None,
            depth: 0,
            page_type_id: None,
            transition_id: None,
            parent_canonical_url: None,
            final_canonical_url: None,
            pagination: false,
            seed_ids: Vec::new(),
            execution_ids: Vec::new(),
        };
        let checkpoint = crate::CrawlCheckpoint::new(
            run_id,
            &snapshot,
            crate::SemanticTraversalCheckpoint::empty(Vec::new()),
            Vec::new(),
            vec![pending],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )?;
        let result = reconstruct_crawl_structural_facts(
            &snapshot,
            CrawlRunStatus::Running,
            &[execution(run_id, CrawlExecutionOutcome::Completed)],
            &[],
            Some(&checkpoint),
            None,
            None,
        );
        assert!(matches!(result, Err(CrawlFinalizationError::Invariant)));
        Ok(())
    }

    #[test]
    fn failed_and_cancelled_statuses_remain_terminal_and_incomplete()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = snapshot()?;
        for status in [CrawlRunStatus::Failed, CrawlRunStatus::Cancelled] {
            let finalized = finalize_durable_state(&snapshot, status, &[], &[], None)?;
            assert_eq!(finalized.status, status);
            assert!(matches!(
                finalized.decision,
                CompleteSnapshotStructuralDecision::Incomplete { .. }
            ));
        }
        Ok(())
    }

    #[test]
    fn production_extraction_remains_not_evaluated() -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = production_snapshot()?;
        let finalized = finalize_durable_state(&snapshot, CrawlRunStatus::Running, &[], &[], None)?;
        assert_eq!(
            finalized.structural_input.extraction_health,
            ExtractionHealth::NotEvaluated
        );
        assert!(matches!(
            finalized.decision,
            CompleteSnapshotStructuralDecision::Incomplete { .. }
        ));
        Ok(())
    }
}
