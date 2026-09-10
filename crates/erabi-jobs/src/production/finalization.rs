//! Production post-crawl finalization composition.
//!
//! This module is the application boundary that combines canonical crawl
//! facts with the current Production extraction-health compatibility value,
//! asks the domain for its trust decision, and durably terminalizes the run.

#[allow(clippy::wildcard_imports)]
use super::*;
use crate::production::crawl_stage::ReadyForPostCrawl;
use erabi_domain::{CompleteSnapshotStructuralInput, ExtractionHealth};

fn complete_snapshot_input(
    snapshot: &CrawlRunSnapshot,
    facts: &erabi_crawler::CrawlStructuralFacts,
    extraction_health: ExtractionHealth,
) -> CompleteSnapshotStructuralInput {
    CompleteSnapshotStructuralInput {
        run_type: snapshot.run_type(),
        status: facts.status,
        in_scope_pages_planned: facts.in_scope_pages_planned,
        in_scope_pages_completed: facts.in_scope_pages_completed,
        pagination_truncation_count: facts.pagination_truncation_count,
        unresolved_partial_work_count: facts.unresolved_partial_work_count,
        page_type_ambiguity_count: facts.page_type_ambiguity_count,
        extraction_health,
    }
}

#[allow(clippy::result_large_err, clippy::too_many_lines)]
impl ProductionCrawlJobHandler {
    pub(super) async fn finalize_ready_for_post_crawl(
        &self,
        context: &JobExecutionContext,
        ready: &ReadyForPostCrawl,
    ) -> ProductionResult<CrawlRunStatus> {
        self.finalize_durable_evidence(
            context,
            &ready.snapshot,
            ready.run_id,
            ready.current_status,
            ExtractionHealth::NotEvaluated,
        )
        .await
    }

    pub(super) async fn finalize_cancelled(
        &self,
        context: &JobExecutionContext,
        snapshot: &CrawlRunSnapshot,
        run_id: CrawlRunId,
    ) -> ProductionResult<CrawlRunStatus> {
        self.finalize_durable_evidence(
            context,
            snapshot,
            run_id,
            CrawlRunStatus::Cancelled,
            ExtractionHealth::NotEvaluated,
        )
        .await
    }

    async fn finalize_durable_evidence(
        &self,
        context: &JobExecutionContext,
        snapshot: &CrawlRunSnapshot,
        run_id: CrawlRunId,
        current_status: CrawlRunStatus,
        extraction_health: ExtractionHealth,
    ) -> ProductionResult<CrawlRunStatus> {
        let executions = CrawlExecutionRepository::new(&self.database)
            .list_for_run(run_id)
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::PersistExecution,
                    "EXECUTIONS_LOAD_FAILED",
                )
            })?;
        let discovered = CrawlRunRepository::new(&self.database)
            .discovered_urls(run_id)
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "DISCOVERY_LOAD_FAILED",
                )
            })?;
        let latest = JobRepository::new(&self.database)
            .latest_checkpoint_for_lineage(context.job_id())
            .await
            .map_err(|error| {
                production_checkpoint_load_error(ExecutionOperation::LoadCheckpoint, &error)
            })?;
        let durable = CrawlTraversalRepository::new(&self.database)
            .reconstruct_recovery_state(run_id)
            .await
            .map_err(|error| {
                production_traversal_error(ExecutionOperation::LoadCheckpoint, error)
            })?;
        if let Some(record) = latest.as_ref() {
            validate_crawl_recovery(
                Some(record),
                snapshot,
                run_id,
                current_status,
                &executions,
                &discovered,
                Some(&durable),
            )
            .map_err(|error| {
                production_recovery_error(ExecutionOperation::LoadCheckpoint, error)
            })?;
        }
        let facts = erabi_crawler::reconstruct_crawl_structural_facts(
            snapshot,
            current_status,
            &executions,
            &discovered,
            Some(&durable.control),
            Some(&durable.work),
        )
        .map_err(|_| {
            production_recovery_error(
                ExecutionOperation::FinalizeRun,
                CrawlRecoveryValidationError::StateInvalid,
            )
        })?;
        let structural_input = complete_snapshot_input(snapshot, &facts, extraction_health);
        let _decision = structural_input.decide().map_err(|_| {
            ProductionError::new(
                ExecutionDiagnostic::new(
                    OrchestrationErrorCategory::Finalization,
                    ExecutionOperation::FinalizeRun,
                    ExecutionAction::Retry,
                    "CRAWL_RUN_FINALIZATION_FAILED",
                )
                .with_run(run_id),
            )
        })?;
        let summary = CrawlExecutionSummary {
            crawl_run_id: run_id,
            in_scope_pages_planned: structural_input.in_scope_pages_planned,
            in_scope_pages_completed: structural_input.in_scope_pages_completed,
            pagination_truncation_count: structural_input.pagination_truncation_count,
            unresolved_partial_work_count: structural_input.unresolved_partial_work_count,
            page_type_ambiguity_count: structural_input.page_type_ambiguity_count,
        };
        CrawlExecutionRepository::new(&self.database)
            .finalize(&summary, facts.status)
            .await
            .map_err(|_| {
                ProductionError::new(
                    ExecutionDiagnostic::new(
                        OrchestrationErrorCategory::Finalization,
                        ExecutionOperation::FinalizeRun,
                        ExecutionAction::Retry,
                        "CRAWL_RUN_FINALIZATION_FAILED",
                    )
                    .with_run(run_id),
                )
            })?;
        context.mark_terminal_crawl_run(run_id, facts.status);
        Ok(facts.status)
    }
}

#[cfg(test)]
mod tests {
    use erabi_domain::{
        CrawlRunSnapshot, CrawlRunSnapshotDraft, CrawlRunType, CrawlerId, CrawlerVersionId,
        ResolvedValue, RobotsAudit, RunConfiguration, SettingSource, SnapshotOperationalSettings,
    };

    use super::*;

    fn production_snapshot() -> Result<CrawlRunSnapshot, Box<dyn std::error::Error>> {
        fn resolved<T>(value: T) -> ResolvedValue<T> {
            ResolvedValue {
                value,
                source: SettingSource::BuiltInDefault,
            }
        }

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
                Some(crawler_version_id),
            ),
            actor: "operator".to_owned(),
            created_at: "unix:1".to_owned(),
        })?)
    }

    #[test]
    fn structural_incompleteness_cannot_be_repaired_by_healthy_extraction()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = production_snapshot()?;
        let partial = erabi_crawler::CrawlStructuralFacts {
            status: CrawlRunStatus::PartialResult,
            in_scope_pages_planned: 2,
            in_scope_pages_completed: 1,
            pagination_truncation_count: 0,
            unresolved_partial_work_count: 1,
            page_type_ambiguity_count: 0,
        };
        let input = complete_snapshot_input(&snapshot, &partial, ExtractionHealth::Healthy);
        let decision = input.decide()?;
        assert!(matches!(
            decision,
            erabi_domain::CompleteSnapshotStructuralDecision::Incomplete { .. }
        ));

        let complete_crawl = erabi_crawler::CrawlStructuralFacts {
            status: CrawlRunStatus::Succeeded,
            in_scope_pages_planned: 1,
            in_scope_pages_completed: 1,
            pagination_truncation_count: 0,
            unresolved_partial_work_count: 0,
            page_type_ambiguity_count: 0,
        };
        let not_evaluated =
            complete_snapshot_input(&snapshot, &complete_crawl, ExtractionHealth::NotEvaluated);
        let decision = not_evaluated.decide()?;
        assert!(matches!(
            decision,
            erabi_domain::CompleteSnapshotStructuralDecision::Incomplete { .. }
        ));
        Ok(())
    }
}
