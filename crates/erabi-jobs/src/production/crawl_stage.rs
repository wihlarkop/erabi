//! Production crawl-stage orchestration.
//!
//! This module owns frozen-run loading/recovery, `SemanticTraversal` driving,
//! durable crawl evidence/checkpoint progression, and safe worker boundaries.

#[allow(clippy::wildcard_imports)]
use super::*;
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet};

#[allow(clippy::large_enum_variant)]
pub(super) enum CrawlStageOutcome {
    ReadyForPostCrawl(ReadyForPostCrawl),
    DeferredNoPostCrawl,
}

pub(super) struct ReadyForPostCrawl {
    pub(super) run_id: CrawlRunId,
    pub(super) snapshot: CrawlRunSnapshot,
    pub(super) current_status: CrawlRunStatus,
}

type ExecutionProvenanceKey = (String, String);
type ExecutionProvenanceIds = BTreeMap<ExecutionProvenanceKey, String>;

fn transition_source_counts(
    run_id: CrawlRunId,
    traversal: &SemanticTraversal,
) -> Vec<CrawlTransitionSourceCount> {
    traversal
        .checkpoint_state()
        .transition_page_counts
        .into_iter()
        .map(
            |(transition_id, source_canonical_url, eligible_edge_count)| {
                CrawlTransitionSourceCount {
                    transition_id: transition_id.to_string(),
                    source_url_state_id: crawl_url_state_id(run_id, &source_canonical_url),
                    eligible_edge_count: u64::from(eligible_edge_count),
                }
            },
        )
        .collect()
}

fn semantic_projection(state: &SemanticTraversalCheckpoint) -> CrawlTraversalSemanticProjection {
    let mut urls = BTreeSet::new();
    for values in [
        &state.admitted_canonical_urls,
        &state.seen_canonical_urls,
        &state.sampled_canonical_urls,
        &state.expanded_canonical_urls,
        &state.matching_canonical_urls,
        &state.unmatched_canonical_urls,
        &state.ambiguous_canonical_urls,
        &state.in_scope_canonical_urls,
    ] {
        urls.extend(values.iter().cloned());
    }
    let url_states = urls
        .into_iter()
        .map(|canonical_url| CrawlTraversalUrlSemanticState {
            sampled: state.sampled_canonical_urls.contains(&canonical_url),
            expanded: state.expanded_canonical_urls.contains(&canonical_url),
            in_scope: state.in_scope_canonical_urls.contains(&canonical_url),
            page_type_match_state: if state.ambiguous_canonical_urls.contains(&canonical_url) {
                Some(CrawlPageTypeMatchState::Ambiguous)
            } else if state.unmatched_canonical_urls.contains(&canonical_url) {
                Some(CrawlPageTypeMatchState::Unmatched)
            } else if state.matching_canonical_urls.contains(&canonical_url) {
                Some(CrawlPageTypeMatchState::Matched)
            } else {
                None
            },
            canonical_url,
        })
        .collect();
    let mut page_type_counts = BTreeMap::<String, (u64, u64)>::new();
    for (page_type_id, count) in &state.page_type_sampled {
        page_type_counts
            .entry(page_type_id.to_string())
            .or_default()
            .0 = *count;
    }
    for (page_type_id, count) in &state.page_type_discovered {
        page_type_counts
            .entry(page_type_id.to_string())
            .or_default()
            .1 = *count;
    }
    let page_type_counts = page_type_counts
        .into_iter()
        .map(
            |(page_type_id, (sampled_count, discovered_count))| CrawlTraversalPageTypeCounts {
                page_type_id,
                sampled_count,
                discovered_count,
            },
        )
        .collect();
    CrawlTraversalSemanticProjection {
        url_states,
        page_type_counts,
    }
}

#[allow(clippy::result_large_err)]
impl ProductionCrawlJobHandler {
    #[allow(clippy::too_many_lines)]
    pub(super) async fn run_crawl_stage(
        &self,
        context: JobExecutionContext,
    ) -> ProductionResult<CrawlStageOutcome> {
        if !is_production_job_kind(context.kind().as_str()) {
            return Err(ProductionError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Invariant,
                ExecutionOperation::QueueLifecycle,
                ExecutionAction::Fail,
                "JOB_KIND_UNSUPPORTED",
            )));
        }
        let job = JobRepository::new(&self.database)
            .job(context.job_id())
            .await
            .map_err(|_| {
                ProductionError::repository(ExecutionOperation::LoadJob, "JOB_LOAD_FAILED")
            })?;
        let run_id = job
            .crawl_run_id
            .as_deref()
            .and_then(parse_run_id)
            .ok_or_else(|| {
                ProductionError::new(ExecutionDiagnostic::new(
                    OrchestrationErrorCategory::Invariant,
                    ExecutionOperation::LoadJob,
                    ExecutionAction::Fail,
                    "RUN_ID_INVALID",
                ))
            })?;
        let snapshot = CrawlRunRepository::new(&self.database)
            .snapshot(run_id)
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "RUN_SNAPSHOT_LOAD_FAILED",
                )
            })?;
        let semantic = load_frozen_production_semantics(&self.database, &snapshot)
            .await
            .map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::LoadRunSnapshot,
                    "FROZEN_SEMANTICS_LOAD_FAILED",
                )
            })?;
        let limits = production_limits(&snapshot, &semantic.version).map_err(|()| {
            ProductionError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Invariant,
                ExecutionOperation::LoadRunSnapshot,
                ExecutionAction::Fail,
                "PRODUCTION_LIMITS_INVALID",
            ))
        })?;
        let deadline = ProductionDeadline::new(
            self.clock.clone(),
            self.clock.now_millis(),
            limits.max_duration_ms,
        );
        let executions = CrawlExecutionRepository::new(&self.database)
            .list_for_run(run_id)
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "EXECUTION_LIST_FAILED",
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
        let checkpoint = if context.kind().as_str() == "RERUN_FULL_CRAWL" {
            None
        } else {
            JobRepository::new(&self.database)
                .latest_checkpoint_for_lineage(context.job_id())
                .await
                .map_err(|_| {
                    ProductionError::checkpoint(
                        ExecutionOperation::LoadCheckpoint,
                        "CHECKPOINT_LOAD_FAILED",
                    )
                })?
        };

        if checkpoint.is_none() && (!executions.is_empty() || !discovered.is_empty()) {
            if !durable_production_completion_without_checkpoint(&executions, &discovered) {
                // Durable discovery without a compatible recovery frontier is
                // not permission to rediscover Seeds.  This is an interrupted
                // run and must fail closed.
                return Err(ProductionError::checkpoint(
                    ExecutionOperation::LoadCheckpoint,
                    "RECOVERY_FRONTIER_MISSING",
                ));
            }
            let current_status = CrawlRunRepository::new(&self.database)
                .status(run_id)
                .await
                .map_err(|_| {
                    ProductionError::repository(
                        ExecutionOperation::LoadRunSnapshot,
                        "RUN_STATUS_LOAD_FAILED",
                    )
                })?;
            return Ok(CrawlStageOutcome::ReadyForPostCrawl(ReadyForPostCrawl {
                run_id,
                snapshot,
                current_status,
            }));
        }

        let run_repository = CrawlRunRepository::new(&self.database);
        if matches!(
            context.kind().as_str(),
            "RETRY" | "RETRY_FAILED_PARTS" | "RESUME_CHECKPOINT"
        ) {
            run_repository
                .transition_recovery_status(run_id)
                .await
                .map_err(|_| {
                    ProductionError::repository(
                        ExecutionOperation::TransitionRun,
                        "RUN_RECOVERY_TRANSITION_FAILED",
                    )
                })?;
        } else {
            run_repository
                .transition_execution_status(run_id, CrawlRunStatus::Running)
                .await
                .map_err(|_| {
                    ProductionError::repository(
                        ExecutionOperation::TransitionRun,
                        "RUN_EXECUTION_TRANSITION_FAILED",
                    )
                })?;
        }
        self.progress(
            &context,
            if checkpoint.is_some() {
                "RECOVERY_ACCEPTED"
            } else {
                "PRODUCTION_STARTED"
            },
            None,
        )
        .await?;

        let provider = Arc::new(ProductionTraversalProvider::new(
            self.clone(),
            context.clone(),
            snapshot.clone(),
            deadline,
        ));
        let mut provenance = self.load_provenance_ids(run_id).await?;
        let mut traversal = if let Some(record) = checkpoint.as_ref() {
            CrawlCheckpointV2::from_envelope(&record.checkpoint, &snapshot, run_id).map_err(
                |_| {
                    ProductionError::checkpoint(
                        ExecutionOperation::LoadCheckpoint,
                        "CHECKPOINT_INVALID",
                    )
                },
            )?;
            // A provider result can be durably committed immediately before a
            // process crash and before the following checkpoint append.  The
            // immutable execution rows win over that stale work partition.
            let recovery_selection = match context.kind().as_str() {
                "RETRY" | "RETRY_FAILED_PARTS" => {
                    // Make the action child recoverable before its durable
                    // generation selection. If the process dies after the
                    // selection transaction commits but before the next
                    // checkpoint, queue recovery can replay this same action
                    // instead of exhausting the child as checkpoint-less.
                    context.checkpoint(&record.checkpoint).await.map_err(|_| {
                        ProductionError::checkpoint(
                            ExecutionOperation::LoadCheckpoint,
                            "CHECKPOINT_PERSIST_FAILED",
                        )
                    })?;
                    let action_kind = if context.kind().as_str() == "RETRY" {
                        erabi_db::repositories::CrawlRecoveryActionKind::Retry
                    } else {
                        erabi_db::repositories::CrawlRecoveryActionKind::RetryFailedParts
                    };
                    Some(
                        CrawlTraversalRepository::new(&self.database)
                            .prepare_recovery_action(
                                context.job_id(),
                                context.attempt_id(),
                                run_id,
                                action_kind,
                                context.ownership_now(),
                            )
                            .await
                            .map_err(|_| {
                                ProductionError::repository(
                                    ExecutionOperation::QueueLifecycle,
                                    "RECOVERY_ACTION_PERSIST_FAILED",
                                )
                            })?,
                    )
                }
                _ => None,
            };
            let selected_state_ids = recovery_selection
                .as_ref()
                .map(|selection| selection.state_ids.iter().cloned().collect::<BTreeSet<_>>());
            let (durable, recovery_entries) = self
                .reconstruct_traversal_checkpoint(
                    run_id,
                    &snapshot,
                    &semantic,
                    selected_state_ids.as_ref(),
                )
                .await?;
            if recovery_selection.is_some() {
                SemanticTraversal::restore_for_recovery(
                    semantic,
                    snapshot.selected_seed_ids().to_vec(),
                    limits,
                    provider.clone(),
                    self.clock.clone(),
                    durable,
                    recovery_entries,
                )
                .map_err(|_| {
                    ProductionError::checkpoint(
                        ExecutionOperation::Serialization,
                        "TRAVERSAL_RESTORE_FAILED",
                    )
                })?
            } else {
                SemanticTraversal::restore_from_checkpoint(
                    semantic,
                    snapshot.selected_seed_ids().to_vec(),
                    limits,
                    provider.clone(),
                    self.clock.clone(),
                    durable,
                )
                .map_err(|_| {
                    ProductionError::checkpoint(
                        ExecutionOperation::Serialization,
                        "TRAVERSAL_RESTORE_FAILED",
                    )
                })?
            }
        } else {
            if !executions.is_empty()
                || !discovered.is_empty()
                || !matches!(
                    context.kind().as_str(),
                    PRODUCTION_CRAWL_JOB_KIND | "RERUN_FULL_CRAWL"
                )
            {
                return Err(ProductionError::checkpoint(
                    ExecutionOperation::LoadCheckpoint,
                    "RECOVERY_STATE_INCOMPATIBLE",
                ));
            }
            SemanticTraversal::for_frozen_snapshot(
                semantic,
                snapshot.selected_seed_ids().to_vec(),
                limits,
                provider.clone(),
                self.clock.clone(),
            )
            .map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::LoadRunSnapshot,
                    "TRAVERSAL_INITIALIZATION_FAILED",
                )
            })?
        };

        // Initialization atomically commits the exact root evidence, logical
        // work, traversal control, and compact checkpoint before dispatch.
        let mut seeds_persisted = checkpoint.is_some();
        if checkpoint.is_some() {
            self.synchronize_traversal_state(run_id, &traversal)
                .await
                .map_err(|_| {
                    ProductionError::repository(
                        ExecutionOperation::PersistExecution,
                        "TRAVERSAL_STATE_PERSIST_FAILED",
                    )
                })?;
            self.save_checkpoint(&context, &snapshot, run_id, &traversal, &provenance)
                .await?;
        } else {
            self.initialize_traversal_state(
                &context,
                &snapshot,
                run_id,
                &traversal,
                &mut provenance,
            )
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::PersistExecution,
                    "TRAVERSAL_STATE_INITIALIZATION_FAILED",
                )
            })?;
            seeds_persisted = true;
        }

        loop {
            let pending_canonical_url = traversal
                .next_pending_canonical_url()
                .map(ToOwned::to_owned);
            let expected_work_generation = if let Some(canonical_url) =
                pending_canonical_url.as_deref()
            {
                Some(
                    CrawlTraversalRepository::new(&self.database)
                        .read_work_generation(run_id, &crawl_url_state_id(run_id, canonical_url))
                        .await
                        .map_err(|_| {
                            ProductionError::repository(
                                ExecutionOperation::PersistExecution,
                                "WORK_GENERATION_LOAD_FAILED",
                            )
                        })?,
                )
            } else {
                None
            };
            match traversal.step().await.map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::Serialization,
                    "TRAVERSAL_STEP_FAILED",
                )
            })? {
                SemanticTraversalStep::Processed {
                    pages,
                    discovery_paths,
                } => {
                    let attempts = provider.take_attempts().await;
                    let seeds = if seeds_persisted {
                        Vec::new()
                    } else {
                        traversal.seed_evidence()
                    };
                    let evidence = self.collect_discovery_delta(
                        run_id,
                        &seeds,
                        &discovery_paths,
                        &pages,
                        &attempts,
                        expected_work_generation.unwrap_or(0),
                        &mut provenance,
                    )?;
                    self.synchronize_traversal_state_with_evidence(
                        run_id,
                        &traversal,
                        &evidence,
                        &pages,
                        expected_work_generation,
                    )
                    .await?;
                    seeds_persisted = true;
                    self.persist_page_delta(
                        run_id,
                        &snapshot,
                        &pages,
                        &discovery_paths,
                        &provenance,
                        attempts,
                        expected_work_generation,
                        &context,
                    )
                    .await?;
                    self.save_checkpoint(&context, &snapshot, run_id, &traversal, &provenance)
                        .await?;
                    if context.cancellation().is_cancelled() {
                        return self
                            .cancellation_boundary(&context, &snapshot, run_id)
                            .await
                            .map(|()| CrawlStageOutcome::DeferredNoPostCrawl);
                    }
                    if context.storage_pressure().is_signalled() {
                        return Ok(CrawlStageOutcome::DeferredNoPostCrawl);
                    }
                }
                SemanticTraversalStep::Interrupted(reason) => {
                    if !seeds_persisted {
                        let seeds = traversal.seed_evidence();
                        let evidence = self.collect_discovery_delta(
                            run_id,
                            &seeds,
                            &[],
                            &[],
                            &BTreeMap::new(),
                            0,
                            &mut provenance,
                        )?;
                        self.synchronize_traversal_state_with_evidence(
                            run_id,
                            &traversal,
                            &evidence,
                            &[],
                            None,
                        )
                        .await?;
                    }
                    // The interruption itself can advance durable traversal
                    // control (notably duration/pagination structural
                    // evidence) even when it produces no page delta. Persist
                    // that semantic result before the compact checkpoint.
                    self.synchronize_traversal_state(run_id, &traversal)
                        .await
                        .map_err(|_| {
                            ProductionError::repository(
                                ExecutionOperation::PersistExecution,
                                "TRAVERSAL_STATE_PERSIST_FAILED",
                            )
                        })?;
                    self.save_checkpoint(&context, &snapshot, run_id, &traversal, &provenance)
                        .await?;
                    match reason {
                        erabi_crawler::DiscoveryPreviewInterruption::Cancelled => {
                            return self
                                .cancellation_boundary(&context, &snapshot, run_id)
                                .await
                                .map(|()| CrawlStageOutcome::DeferredNoPostCrawl);
                        }
                        erabi_crawler::DiscoveryPreviewInterruption::StoragePressure => {
                            return Ok(CrawlStageOutcome::DeferredNoPostCrawl);
                        }
                    }
                }
                SemanticTraversalStep::Complete => {
                    if !seeds_persisted {
                        let seeds = traversal.seed_evidence();
                        let evidence = self.collect_discovery_delta(
                            run_id,
                            &seeds,
                            &[],
                            &[],
                            &BTreeMap::new(),
                            0,
                            &mut provenance,
                        )?;
                        self.synchronize_traversal_state_with_evidence(
                            run_id,
                            &traversal,
                            &evidence,
                            &[],
                            None,
                        )
                        .await?;
                    }
                    // Completion may follow a budget decision with no further
                    // page delta. The scalar control row, not the compact
                    // checkpoint, owns those final traversal facts.
                    self.synchronize_traversal_state(run_id, &traversal)
                        .await
                        .map_err(|_| {
                            ProductionError::repository(
                                ExecutionOperation::PersistExecution,
                                "TRAVERSAL_STATE_PERSIST_FAILED",
                            )
                        })?;
                    self.save_checkpoint(&context, &snapshot, run_id, &traversal, &provenance)
                        .await?;
                    break;
                }
            }
        }

        let current_status = CrawlRunRepository::new(&self.database)
            .status(run_id)
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "RUN_STATUS_LOAD_FAILED",
                )
            })?;
        Ok(CrawlStageOutcome::ReadyForPostCrawl(ReadyForPostCrawl {
            run_id,
            snapshot,
            current_status,
        }))
    }
}

#[allow(clippy::result_large_err)]
impl ProductionCrawlJobHandler {
    async fn cancellation_boundary(
        &self,
        context: &JobExecutionContext,
        snapshot: &CrawlRunSnapshot,
        run_id: CrawlRunId,
    ) -> ProductionResult<()> {
        self.finalize_cancelled(context, snapshot, run_id).await?;
        if let Err(error) = self
            .progress(context, "CANCELLATION_SAFE_BOUNDARY", None)
            .await
        {
            context.record_secondary_diagnostics(error.diagnostics);
        }
        if let Err(error) = self
            .progress(
                context,
                "PRODUCTION_CANCELLED",
                Some(ProgressTerminalState::Cancelled),
            )
            .await
        {
            context.record_secondary_diagnostics(error.diagnostics);
        }
        Err(ProductionError::new(ExecutionDiagnostic::new(
            OrchestrationErrorCategory::Finalization,
            ExecutionOperation::FinalizeRun,
            ExecutionAction::Fail,
            "CRAWL_RUN_CANCELLED",
        )))
    }

    async fn load_provenance_ids(
        &self,
        run_id: CrawlRunId,
    ) -> ProductionResult<ExecutionProvenanceIds> {
        let mut latest = BTreeMap::<ExecutionProvenanceKey, (u64, String)>::new();
        let records = CrawlRunRepository::new(&self.database)
            .discovered_urls(run_id)
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::LoadRunSnapshot,
                    "PROVENANCE_LOAD_FAILED",
                )
            })?;
        for record in records {
            if matches!(record.status.as_str(), "ADMITTED" | "EXECUTION_RECONCILED")
                || record
                    .detail
                    .get("origin")
                    .and_then(serde_json::Value::as_str)
                    == Some("SEED")
            {
                let generation = record
                    .detail
                    .get("work_generation")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let key = (record.original_url, record.canonical_url);
                if latest
                    .get(&key)
                    .is_none_or(|(known_generation, _)| generation >= *known_generation)
                {
                    latest.insert(key, (generation, record.id));
                }
            }
        }
        Ok(latest.into_iter().map(|(key, (_, id))| (key, id)).collect())
    }

    /// Persists only facts already decided by the one `SemanticTraversal`. The
    /// DB sees queue ordering/provenance; it never re-evaluates selectors,
    /// canonicalization, scope, or transition eligibility.
    async fn synchronize_traversal_state(
        &self,
        run_id: CrawlRunId,
        traversal: &SemanticTraversal,
    ) -> ProductionResult<()> {
        let (work, control) = Self::durable_traversal_snapshot(run_id, traversal);
        let semantic_state = semantic_projection(&traversal.checkpoint_state());
        let transition_source_counts = transition_source_counts(run_id, traversal);
        let repository = CrawlTraversalRepository::new(&self.database);
        match repository.read_traversal_control(run_id).await {
            Ok(_) => repository
                .apply_discovery_delta_with_projection(
                    run_id,
                    &[],
                    &work,
                    &control,
                    &transition_source_counts,
                    &semantic_state,
                    &[],
                    &[],
                )
                .await
                .map_err(|_| {
                    ProductionError::repository(
                        ExecutionOperation::PersistExecution,
                        "TRAVERSAL_STATE_PERSIST_FAILED",
                    )
                }),
            // A checkpoint without its coupled traversal-control row is an
            // interrupted or pre-Task-9 state, not proof that initialization
            // completed. Do not recreate roots here: that would allow
            // recovery to proceed without the atomic Seed evidence phase.
            Err(_) => Err(ProductionError::checkpoint(
                ExecutionOperation::LoadCheckpoint,
                "TRAVERSAL_CONTROL_MISSING",
            )),
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn synchronize_traversal_state_with_evidence(
        &self,
        run_id: CrawlRunId,
        traversal: &SemanticTraversal,
        evidence: &[DiscoveredUrlRecord],
        pages: &[DiscoveryPreviewPage],
        expected_work_generation: Option<u64>,
    ) -> ProductionResult<()> {
        let (mut work, control) = Self::durable_traversal_snapshot(run_id, traversal);
        for state in &mut work {
            for record in evidence.iter().filter(|record| {
                record.status == "ADMITTED" && record.canonical_url == state.canonical_url
            }) {
                if state.first_discovered_url_id.is_none() {
                    state.first_discovered_url_id = Some(record.id.clone());
                }
                let source_state_id = record
                    .detail
                    .get("source_canonical_url")
                    .and_then(serde_json::Value::as_str)
                    .map(|source| crawl_url_state_id(run_id, source));
                if source_state_id.as_deref() == state.parent_url_state_id.as_deref() {
                    state.parent_discovered_url_id = Some(record.id.clone());
                }
            }
        }
        for record in evidence {
            let Some(reason) = preserve_reason_for_discovery_status(&record.status) else {
                continue;
            };
            if work
                .iter()
                .any(|state| state.canonical_url == record.canonical_url)
            {
                continue;
            }
            let seed_provenance = record
                .detail
                .get("seed_ids")
                .and_then(serde_json::Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect()
                })
                .unwrap_or_default();
            work.push(CrawlUrlStateRecord {
                id: crawl_url_state_id(run_id, &record.canonical_url),
                crawl_run_id: run_id,
                canonical_url: record.canonical_url.clone(),
                first_discovered_url_id: Some(record.id.clone()),
                requested_url: record.original_url.clone(),
                parent_url_state_id: None,
                parent_discovered_url_id: None,
                admission_state: CrawlAdmissionState::PreserveOnly,
                preserve_reason: Some(reason.to_owned()),
                resolved_to_url_state_id: None,
                admission_sequence: None,
                depth: None,
                target_page_type_id: None,
                transition_id: None,
                pagination: false,
                final_canonical_url: None,
                current_work_state: None,
                work_generation: 0,
                current_execution_id: None,
                seed_provenance,
                seen: true,
                sampled: false,
                expanded: false,
                in_scope: false,
                page_type_match_state: None,
            });
        }
        let repository = CrawlTraversalRepository::new(&self.database);
        let transition_source_counts = transition_source_counts(run_id, traversal);
        let redirects = pages
            .iter()
            .filter_map(|page| {
                let final_canonical_url = page.canonical_url.as_ref()?;
                (page.requested_canonical_url != *final_canonical_url).then(|| {
                    CrawlRedirectReconciliation {
                        alias_url_state_id: crawl_url_state_id(
                            run_id,
                            &page.requested_canonical_url,
                        ),
                        final_url_state_id: crawl_url_state_id(run_id, final_canonical_url),
                        alias_canonical_url: page.requested_canonical_url.clone(),
                        final_canonical_url: final_canonical_url.clone(),
                    }
                })
            })
            .collect::<Vec<_>>();
        // SemanticTraversal has already committed the observation to its
        // in-memory sets by this point. Keep that source logical unit
        // non-pending in the same discovery transaction until the guarded
        // execution write can attach its result. A sampled PENDING row would
        // be an impossible checkpoint-equivalent state: strict restore would
        // reject it, while recovery could otherwise select it again.
        // Provider failures and robots exclusions are deliberately left
        // pending; they have no sampled semantic observation and remain
        // eligible for the existing retry/recovery actions.
        let in_flight_work = pages
            .iter()
            .filter(|page| {
                !matches!(
                    page.state,
                    PreviewUrlState::ProviderError | PreviewUrlState::RobotsExcluded
                )
            })
            .map(|page| {
                let canonical_url = page
                    .canonical_url
                    .as_deref()
                    .unwrap_or(&page.requested_canonical_url);
                CrawlInFlightWork {
                    state_id: crawl_url_state_id(run_id, canonical_url),
                    expected_work_generation: (page.requested_canonical_url == canonical_url)
                        .then_some(expected_work_generation)
                        .flatten(),
                }
            })
            .collect::<Vec<_>>();
        let semantic_state = semantic_projection(&traversal.checkpoint_state());
        repository
            .apply_discovery_delta_with_projection(
                run_id,
                evidence,
                &work,
                &control,
                &transition_source_counts,
                &semantic_state,
                &redirects,
                &in_flight_work,
            )
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::PersistExecution,
                    "TRAVERSAL_STATE_PERSIST_FAILED",
                )
            })
    }

    fn durable_traversal_snapshot(
        run_id: CrawlRunId,
        traversal: &SemanticTraversal,
    ) -> (Vec<CrawlUrlStateRecord>, CrawlTraversalControl) {
        let state = traversal.checkpoint_state();
        let projection = semantic_projection(&state);
        let work = state
            .pending
            .iter()
            .map(|entry| CrawlUrlStateRecord {
                id: crawl_url_state_id(run_id, &entry.canonical_url),
                crawl_run_id: run_id,
                canonical_url: entry.canonical_url.clone(),
                first_discovered_url_id: entry.discovered_url_id.clone(),
                requested_url: entry.requested_url.clone(),
                parent_url_state_id: entry
                    .parent_canonical_url
                    .as_deref()
                    .map(|url| crawl_url_state_id(run_id, url)),
                parent_discovered_url_id: None,
                admission_state: CrawlAdmissionState::Admitted,
                preserve_reason: None,
                resolved_to_url_state_id: None,
                admission_sequence: Some(entry.order),
                depth: Some(entry.depth),
                target_page_type_id: entry.target_page_type_id.map(|id| id.to_string()),
                transition_id: entry.transition_id.map(|id| id.to_string()),
                pagination: entry.pagination,
                final_canonical_url: None,
                current_work_state: Some(CrawlWorkState::Pending),
                work_generation: 0,
                current_execution_id: None,
                seed_provenance: entry.seed_ids.iter().map(ToString::to_string).collect(),
                seen: true,
                sampled: projection
                    .url_states
                    .iter()
                    .find(|value| value.canonical_url == entry.canonical_url)
                    .is_some_and(|value| value.sampled),
                expanded: projection
                    .url_states
                    .iter()
                    .find(|value| value.canonical_url == entry.canonical_url)
                    .is_some_and(|value| value.expanded),
                in_scope: projection
                    .url_states
                    .iter()
                    .find(|value| value.canonical_url == entry.canonical_url)
                    .is_some_and(|value| value.in_scope),
                page_type_match_state: projection
                    .url_states
                    .iter()
                    .find(|value| value.canonical_url == entry.canonical_url)
                    .and_then(|value| value.page_type_match_state),
            })
            .collect::<Vec<_>>();
        let control = CrawlTraversalControl {
            crawl_run_id: run_id,
            consumed_bytes: state.consumed_bytes,
            raw_link_count: state.urls_discovered,
            duplicate_count: state.duplicates_prevented,
            robots_excluded_count: state.robots_excluded,
            provider_error_count: state.provider_errors,
            external_url_count: state.external_urls,
            blocked_url_count: state.blocked_urls,
            peak_expansion_count: state.peak_new_from_page,
            elapsed_millis: state.elapsed_millis,
            time_budget_hit: state.time_budget_hit,
            duration_work_not_expanded: state.duration_work_not_expanded,
            pagination_truncation_count: state.pagination_truncation_count,
            next_admission_sequence: state
                .newly_enqueued_urls
                .saturating_add(u64::try_from(state.selected_seed_ids.len()).unwrap_or(u64::MAX)),
        };
        (work, control)
    }

    #[allow(clippy::too_many_lines)]
    async fn initialize_traversal_state(
        &self,
        context: &JobExecutionContext,
        snapshot: &CrawlRunSnapshot,
        run_id: CrawlRunId,
        traversal: &SemanticTraversal,
        provenance: &mut ExecutionProvenanceIds,
    ) -> ProductionResult<()> {
        let state = traversal.checkpoint_state();
        let seed_evidence = self.collect_discovery_delta(
            run_id,
            &traversal.seed_evidence(),
            &[],
            &[],
            &BTreeMap::new(),
            0,
            provenance,
        )?;
        let (mut work, control) = Self::durable_traversal_snapshot(run_id, traversal);
        for record in &seed_evidence {
            let seed_ids = record
                .detail
                .get("seed_ids")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>();
            if record.status == "ADMITTED" {
                if let Some(state) = work
                    .iter_mut()
                    .find(|state| state.canonical_url == record.canonical_url)
                {
                    if state.first_discovered_url_id.is_none() {
                        state.first_discovered_url_id = Some(record.id.clone());
                    }
                    for seed_id in seed_ids {
                        if !state.seed_provenance.contains(&seed_id) {
                            state.seed_provenance.push(seed_id);
                        }
                    }
                }
            } else if let Some(reason) = preserve_reason_for_discovery_status(&record.status) {
                if let Some(state) = work
                    .iter_mut()
                    .find(|state| state.canonical_url == record.canonical_url)
                {
                    for seed_id in seed_ids {
                        if !state.seed_provenance.contains(&seed_id) {
                            state.seed_provenance.push(seed_id);
                        }
                    }
                } else {
                    work.push(CrawlUrlStateRecord {
                        id: crawl_url_state_id(run_id, &record.canonical_url),
                        crawl_run_id: run_id,
                        canonical_url: record.canonical_url.clone(),
                        first_discovered_url_id: Some(record.id.clone()),
                        requested_url: record.original_url.clone(),
                        parent_url_state_id: None,
                        parent_discovered_url_id: None,
                        admission_state: CrawlAdmissionState::PreserveOnly,
                        preserve_reason: Some(reason.to_owned()),
                        resolved_to_url_state_id: None,
                        admission_sequence: None,
                        depth: None,
                        target_page_type_id: None,
                        transition_id: None,
                        pagination: false,
                        final_canonical_url: None,
                        current_work_state: None,
                        work_generation: 0,
                        current_execution_id: None,
                        seed_provenance: seed_ids,
                        seen: true,
                        sampled: false,
                        expanded: false,
                        in_scope: false,
                        page_type_match_state: None,
                    });
                }
            }
        }
        let checkpoint = CrawlCheckpointV2::new(run_id, snapshot, CrawlRecoveryPhase::Traversing)
            .map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::Serialization,
                    "CHECKPOINT_BUILD_FAILED",
                )
            })?
            .to_envelope()
            .map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::Serialization,
                    "CHECKPOINT_ENVELOPE_FAILED",
                )
            })?;
        let (job_id, attempt_id, lease, created_at) =
            context.checkpoint_lineage().await.map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::LoadCheckpoint,
                    "CHECKPOINT_LINEAGE_LOAD_FAILED",
                )
            })?;
        CrawlTraversalRepository::new(&self.database)
            .initialize_run_state_with_checkpoint_and_evidence(
                run_id,
                &work,
                &seed_evidence,
                &control,
                &semantic_projection(&state),
                &job_id,
                &attempt_id,
                &lease,
                &checkpoint,
                created_at,
            )
            .await
            .map_err(|_| {
                ProductionError::repository(
                    ExecutionOperation::PersistExecution,
                    "TRAVERSAL_STATE_INITIALIZATION_FAILED",
                )
            })?;
        context.mark_checkpoint_persisted();
        self.progress(context, "CHECKPOINT_SAVED", None).await
    }

    #[allow(clippy::too_many_lines)]
    async fn reconstruct_traversal_checkpoint(
        &self,
        run_id: CrawlRunId,
        snapshot: &CrawlRunSnapshot,
        semantic: &erabi_db::repositories::CrawlerSemanticSnapshot,
        selected_state_ids: Option<&BTreeSet<String>>,
    ) -> ProductionResult<(
        SemanticTraversalCheckpoint,
        Vec<SemanticTraversalQueueEntry>,
    )> {
        let durable = CrawlTraversalRepository::new(&self.database)
            .reconstruct_recovery_state(run_id)
            .await
            .map_err(|_| {
                ProductionError::checkpoint(
                    ExecutionOperation::LoadCheckpoint,
                    "TRAVERSAL_STATE_RECONSTRUCTION_FAILED",
                )
            })?;
        emit(SemanticEvent::RecoveryReconstructed {
            context: crate::telemetry_id(&run_id.to_string())
                .map_or(erabi_observability::CorrelationContext::new(), |id| {
                    erabi_observability::CorrelationContext::new().with_crawl_run_id(id)
                }),
            action: erabi_observability::RecoveryAction::Reconstructed,
            generation: 0,
            recovered_count: u64::try_from(durable.work.len()).unwrap_or(u64::MAX),
            outcome: EventOutcome::Reconstructed,
        });
        let checkpoint_error =
            |code| ProductionError::checkpoint(ExecutionOperation::LoadCheckpoint, code);
        let queue_entry = |work: &CrawlUrlStateRecord| -> Result<_, ProductionError> {
            Ok(SemanticTraversalQueueEntry {
                requested_url: work.requested_url.clone(),
                canonical_url: work.canonical_url.clone(),
                depth: work.depth.ok_or_else(|| {
                    ProductionError::checkpoint(
                        ExecutionOperation::LoadCheckpoint,
                        "TRAVERSAL_DEPTH_MISSING",
                    )
                })?,
                seed_ids: work
                    .seed_provenance
                    .iter()
                    .map(|id| {
                        decode_id(id).map_err(|()| {
                            ProductionError::checkpoint(
                                ExecutionOperation::LoadCheckpoint,
                                "TRAVERSAL_SEED_ID_INVALID",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                target_page_type_id: work
                    .target_page_type_id
                    .as_deref()
                    .map(|id| {
                        decode_id(id).map_err(|()| {
                            ProductionError::checkpoint(
                                ExecutionOperation::LoadCheckpoint,
                                "TRAVERSAL_PAGE_TYPE_ID_INVALID",
                            )
                        })
                    })
                    .transpose()?,
                transition_id: work
                    .transition_id
                    .as_deref()
                    .map(|id| {
                        decode_id(id)
                            .map_err(|()| checkpoint_error("TRAVERSAL_TRANSITION_ID_INVALID"))
                    })
                    .transpose()?,
                parent_canonical_url: work
                    .parent_url_state_id
                    .as_deref()
                    .and_then(|parent| durable.work.iter().find(|candidate| candidate.id == parent))
                    .map(|parent| parent.canonical_url.clone()),
                pagination: work.pagination,
                discovered_url_id: work.first_discovered_url_id.clone(),
                order: work.admission_sequence.ok_or_else(|| {
                    ProductionError::checkpoint(
                        ExecutionOperation::LoadCheckpoint,
                        "TRAVERSAL_ADMISSION_SEQUENCE_MISSING",
                    )
                })?,
            })
        };
        let pending = durable
            .work
            .iter()
            .filter(|work| {
                work.current_work_state == Some(CrawlWorkState::Pending)
                    && selected_state_ids.is_none_or(|ids| ids.contains(&work.id))
            })
            .map(queue_entry)
            .collect::<Result<Vec<_>, ProductionError>>()?;
        let recovery_entries = selected_state_ids
            .map(|ids| {
                durable
                    .work
                    .iter()
                    .filter(|work| {
                        ids.contains(&work.id)
                            && work.current_work_state != Some(CrawlWorkState::Completed)
                    })
                    .map(queue_entry)
                    .collect::<Result<Vec<_>, ProductionError>>()
            })
            .transpose()?
            .unwrap_or_default();
        let seen = durable
            .work
            .iter()
            .filter(|work| work.seen)
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let admitted = durable
            .work
            .iter()
            .filter(|work| work.admission_state == CrawlAdmissionState::Admitted)
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let sampled = durable
            .work
            .iter()
            .filter(|work| work.sampled)
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let expanded = durable
            .work
            .iter()
            .filter(|work| work.expanded)
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let matching = durable
            .work
            .iter()
            .filter(|work| work.page_type_match_state == Some(CrawlPageTypeMatchState::Matched))
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let unmatched = durable
            .work
            .iter()
            .filter(|work| work.page_type_match_state == Some(CrawlPageTypeMatchState::Unmatched))
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let ambiguous = durable
            .work
            .iter()
            .filter(|work| work.page_type_match_state == Some(CrawlPageTypeMatchState::Ambiguous))
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let in_scope = durable
            .work
            .iter()
            .filter(|work| work.in_scope)
            .map(|work| work.canonical_url.clone())
            .collect::<Vec<_>>();
        let mut transition_page_counts = Vec::new();
        let mut transition_counts = semantic
            .transitions
            .iter()
            .map(|transition| {
                (
                    transition.transition.id.to_string(),
                    (transition.transition.id, 0_u64, BTreeSet::new()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for count in &durable.transition_source_counts {
            let source = durable
                .work
                .iter()
                .find(|work| work.id == count.source_url_state_id)
                .ok_or_else(|| checkpoint_error("TRAVERSAL_SOURCE_STATE_MISSING"))?;
            let transition_id = decode_id(&count.transition_id)
                .map_err(|()| checkpoint_error("TRAVERSAL_TRANSITION_ID_INVALID"))?;
            transition_page_counts.push((
                transition_id,
                source.canonical_url.clone(),
                u32::try_from(count.eligible_edge_count)
                    .map_err(|_| checkpoint_error("TRAVERSAL_EDGE_COUNT_INVALID"))?,
            ));
            let entry = transition_counts
                .entry(count.transition_id.clone())
                .or_insert((transition_id, 0, BTreeSet::new()));
            entry.1 = entry.1.saturating_add(count.eligible_edge_count);
            entry.2.insert(source.canonical_url.clone());
        }
        let transition_counts = semantic
            .transitions
            .iter()
            .map(|transition| {
                let (_, eligible_edges, source_pages) = transition_counts
                    .remove(&transition.transition.id.to_string())
                    .ok_or_else(|| checkpoint_error("TRAVERSAL_TRANSITION_COUNT_MISSING"))?;
                Ok(SemanticTraversalTransitionState {
                    transition_id: transition.transition.id,
                    name: transition.transition.name.clone(),
                    eligible_edges,
                    source_pages: source_pages.into_iter().collect(),
                })
            })
            .collect::<Result<Vec<_>, ProductionError>>()?;
        let mut page_type_scheduled = BTreeMap::<String, u64>::new();
        for state in &durable.work {
            if state.admission_state == CrawlAdmissionState::Admitted
                && let Some(page_type_id) = state.target_page_type_id.as_deref()
            {
                *page_type_scheduled
                    .entry(page_type_id.to_owned())
                    .or_default() += 1;
            }
        }
        let page_type_sampled = durable
            .page_type_counts
            .iter()
            .map(|count| {
                Ok((
                    decode_id(count.page_type_id.as_str())
                        .map_err(|()| checkpoint_error("TRAVERSAL_PAGE_TYPE_ID_INVALID"))?,
                    count.sampled_count,
                ))
            })
            .collect::<Result<Vec<_>, ProductionError>>()?;
        let page_type_discovered = durable
            .page_type_counts
            .iter()
            .map(|count| {
                Ok((
                    decode_id(count.page_type_id.as_str())
                        .map_err(|()| checkpoint_error("TRAVERSAL_PAGE_TYPE_ID_INVALID"))?,
                    count.discovered_count,
                ))
            })
            .collect::<Result<Vec<_>, ProductionError>>()?;
        let page_type_scheduled = page_type_scheduled
            .into_iter()
            .map(|(id, count)| {
                Ok((
                    decode_id(id.as_str())
                        .map_err(|()| checkpoint_error("TRAVERSAL_PAGE_TYPE_ID_INVALID"))?,
                    count,
                ))
            })
            .collect::<Result<Vec<_>, ProductionError>>()?;
        Ok((
            SemanticTraversalCheckpoint {
                selected_seed_ids: snapshot.selected_seed_ids().to_vec(),
                pending,
                admitted_canonical_urls: admitted.clone(),
                seen_canonical_urls: seen,
                sampled_canonical_urls: sampled.clone(),
                expanded_canonical_urls: expanded,
                matching_canonical_urls: matching,
                unmatched_canonical_urls: unmatched,
                ambiguous_canonical_urls: ambiguous,
                in_scope_canonical_urls: in_scope,
                consumed_bytes: durable.control.consumed_bytes,
                pages_sampled: u64::try_from(sampled.len())
                    .map_err(|_| checkpoint_error("TRAVERSAL_SAMPLE_COUNT_INVALID"))?,
                urls_discovered: durable.control.raw_link_count,
                duplicates_prevented: durable.control.duplicate_count,
                robots_excluded: durable.control.robots_excluded_count,
                provider_errors: durable.control.provider_error_count,
                external_urls: durable.control.external_url_count,
                blocked_urls: durable.control.blocked_url_count,
                newly_enqueued_urls: u64::try_from(admitted.len())
                    .map_err(|_| checkpoint_error("TRAVERSAL_ADMISSION_COUNT_INVALID"))?,
                peak_new_from_page: durable.control.peak_expansion_count,
                time_budget_hit: durable.control.time_budget_hit,
                pagination_truncation_count: durable.control.pagination_truncation_count,
                duration_work_not_expanded: durable.control.duration_work_not_expanded,
                page_type_sampled,
                page_type_discovered,
                page_type_scheduled,
                transition_counts,
                transition_page_counts,
                elapsed_millis: durable.control.elapsed_millis,
            },
            recovery_entries,
        ))
    }

    async fn save_checkpoint(
        &self,
        context: &JobExecutionContext,
        snapshot: &CrawlRunSnapshot,
        run_id: CrawlRunId,
        _traversal: &SemanticTraversal,
        _provenance: &ExecutionProvenanceIds,
    ) -> ProductionResult<()> {
        let checkpoint = CrawlCheckpointV2::new(run_id, snapshot, CrawlRecoveryPhase::Traversing)
            .map_err(|_| {
            ProductionError::checkpoint(
                ExecutionOperation::Serialization,
                "CHECKPOINT_BUILD_FAILED",
            )
        })?;
        let envelope = checkpoint.to_envelope().map_err(|_| {
            ProductionError::checkpoint(
                ExecutionOperation::Serialization,
                "CHECKPOINT_ENVELOPE_FAILED",
            )
        })?;
        context.checkpoint(&envelope).await.map_err(|_| {
            ProductionError::checkpoint(
                ExecutionOperation::LoadCheckpoint,
                "CHECKPOINT_PERSIST_FAILED",
            )
        })?;
        emit(SemanticEvent::CheckpointPersisted {
            context: crate::telemetry_crawl_context(context, Some(&run_id.to_string()), None),
            version: checkpoint.payload_version,
            phase: crate::telemetry_checkpoint_phase(CrawlRecoveryPhase::Traversing),
            bytes: envelope
                .payload
                .as_ref()
                .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
            work_generation: 0,
            outcome: EventOutcome::Durable,
        });
        self.progress(context, "CHECKPOINT_SAVED", None).await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn collect_discovery_delta(
        &self,
        run_id: CrawlRunId,
        seeds: &[DiscoveryPreviewSeed],
        paths: &[DiscoveryPath],
        pages: &[DiscoveryPreviewPage],
        attempts: &BTreeMap<String, ProductionPageAttempt>,
        work_generation: u64,
        ids: &mut ExecutionProvenanceIds,
    ) -> ProductionResult<Vec<DiscoveredUrlRecord>> {
        let mut evidence = Vec::new();
        for seed in seeds {
            let original_url = fragment_free_fetch_url(&seed.requested_url).map_err(|()| {
                ProductionError::projection(
                    ExecutionOperation::Serialization,
                    "DISCOVERY_URL_SERIALIZATION_FAILED",
                )
            })?;
            let status = if seed.duplicate_of_canonical_url.is_some() {
                "CANONICAL_DUPLICATE"
            } else if seed.state == PreviewUrlState::InScopeMatched {
                "ADMITTED"
            } else {
                discovery_seed_status(seed.state)
            };
            let detail = serde_json::json!({
                "origin": "SEED",
                "work_generation": work_generation,
                "seed_ids": [seed.seed_id.to_string()],
                "entry_page_type_hint": seed.entry_page_type_hint.map(|id| id.to_string()),
                "duplicate_of_canonical_url": seed.duplicate_of_canonical_url,
                "scope": seed.scope,
                "page_type_match": seed.page_type_match,
                "budget_hits": seed.budget_hits,
            });
            let id = semantic_discovered_id(
                run_id,
                "SEED",
                work_generation,
                &(
                    seed.seed_id.to_string(),
                    seed.canonical_url.clone(),
                    status,
                    &detail,
                ),
            )
            .map_err(|()| {
                ProductionError::projection(
                    ExecutionOperation::Serialization,
                    "DISCOVERY_ID_BUILD_FAILED",
                )
            })?;
            evidence.push(DiscoveredUrlRecord {
                id: id.clone(),
                crawl_run_id: run_id,
                source_id: None,
                raw_href: None,
                original_url: original_url.clone(),
                canonical_url: seed.canonical_url.clone(),
                status: status.to_owned(),
                discovered_at: discovered_at(
                    attempts,
                    &seed.requested_url,
                    self.clock.now_millis(),
                ),
                detail,
            });
            if seed.duplicate_of_canonical_url.is_none() {
                insert_execution_provenance(ids, original_url, seed.canonical_url.clone(), id);
            }
        }
        for path in paths {
            let canonical_url = path
                .canonical_url
                .clone()
                .unwrap_or_else(|| path.source_canonical_url.clone());
            let resolved_original_url = path
                .resolved_original_url
                .clone()
                .unwrap_or_else(|| path.source_canonical_url.clone());
            let original_url = fragment_free_fetch_url(&resolved_original_url).map_err(|()| {
                ProductionError::projection(
                    ExecutionOperation::Serialization,
                    "DISCOVERY_URL_SERIALIZATION_FAILED",
                )
            })?;
            let status = discovery_status(path);
            let detail = serde_json::json!({
                "seed_ids": path.seed_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "work_generation": work_generation,
                "source_requested_url": path.source_requested_url,
                "source_final_url": path.source_final_url,
                "source_canonical_url": path.source_canonical_url,
                "source_depth": path.source_depth,
                "selector": path.selector,
                "resolved_observation_url": path.resolved_original_url,
                "duplicate_of_canonical_url": path.duplicate_of_canonical_url,
                "transition_evaluations": path.transition_evaluations,
                "budget_hits": path.budget_hits,
            });
            let id = semantic_discovered_id(
                run_id,
                "DISCOVERY_PATH",
                work_generation,
                &(
                    path.source_requested_url.clone(),
                    path.source_canonical_url.clone(),
                    path.raw_href.clone(),
                    path.selector.clone(),
                    original_url.clone(),
                    canonical_url.clone(),
                    status,
                    &detail,
                ),
            )
            .map_err(|()| {
                ProductionError::projection(
                    ExecutionOperation::Serialization,
                    "DISCOVERY_ID_BUILD_FAILED",
                )
            })?;
            evidence.push(DiscoveredUrlRecord {
                id: id.clone(),
                crawl_run_id: run_id,
                source_id: None,
                raw_href: Some(path.raw_href.clone()),
                original_url: original_url.clone(),
                canonical_url: canonical_url.clone(),
                status: status.to_owned(),
                discovered_at: discovered_at(
                    attempts,
                    &path.source_requested_url,
                    self.clock.now_millis(),
                ),
                detail,
            });
            if status == "ADMITTED" {
                insert_execution_provenance(ids, original_url, canonical_url.clone(), id);
            }
        }
        for page in pages {
            let canonical_url = page
                .canonical_url
                .clone()
                .unwrap_or_else(|| page.requested_url.clone());
            let original_url = fragment_free_fetch_url(&page.requested_url).map_err(|()| {
                ProductionError::projection(
                    ExecutionOperation::Serialization,
                    "DISCOVERY_URL_SERIALIZATION_FAILED",
                )
            })?;
            let key = (original_url.clone(), canonical_url.clone());
            if !ids.contains_key(&key) {
                let detail = serde_json::json!({
                    "origin": "EXECUTION_RECONCILIATION",
                    "work_generation": work_generation,
                    "requested_url": page.requested_url,
                    "observed_final_url": page.final_url,
                    "authoritative_canonical_url": canonical_url.clone(),
                    "seed_ids": page.seed_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                });
                let id = semantic_discovered_id(
                    run_id,
                    "EXECUTION_RECONCILIATION",
                    work_generation,
                    &(original_url.clone(), canonical_url.clone(), &detail),
                )
                .map_err(|()| {
                    ProductionError::projection(
                        ExecutionOperation::Serialization,
                        "DISCOVERY_ID_BUILD_FAILED",
                    )
                })?;
                evidence.push(DiscoveredUrlRecord {
                    id: id.clone(),
                    crawl_run_id: run_id,
                    source_id: None,
                    raw_href: None,
                    original_url: original_url.clone(),
                    canonical_url: canonical_url.clone(),
                    status: "EXECUTION_RECONCILED".to_owned(),
                    discovered_at: discovered_at(
                        attempts,
                        &page.requested_url,
                        self.clock.now_millis(),
                    ),
                    detail,
                });
                insert_execution_provenance(ids, original_url, canonical_url.clone(), id);
            }
            if page.state == PreviewUrlState::AmbiguousPageType {
                let detail = serde_json::json!({
                    "origin": "PAGE_TYPE_EVALUATION",
                    "work_generation": work_generation,
                    "requested_url": page.requested_url,
                });
                let ambiguity_id = semantic_discovered_id(
                    run_id,
                    "PAGE_TYPE_EVALUATION",
                    work_generation,
                    &(page.requested_url.clone(), canonical_url.clone(), &detail),
                )
                .map_err(|()| {
                    ProductionError::projection(
                        ExecutionOperation::Serialization,
                        "DISCOVERY_ID_BUILD_FAILED",
                    )
                })?;
                evidence.push(DiscoveredUrlRecord {
                    id: ambiguity_id,
                    crawl_run_id: run_id,
                    source_id: None,
                    raw_href: None,
                    original_url: page.requested_url.clone(),
                    canonical_url: page
                        .canonical_url
                        .clone()
                        .unwrap_or_else(|| page.requested_url.clone()),
                    status: "AMBIGUOUS_PAGE_TYPE".to_owned(),
                    discovered_at: discovered_at(
                        attempts,
                        &page.requested_url,
                        self.clock.now_millis(),
                    ),
                    detail,
                });
            }
        }
        Ok(evidence)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn persist_page_delta(
        &self,
        run_id: CrawlRunId,
        snapshot: &CrawlRunSnapshot,
        pages: &[DiscoveryPreviewPage],
        paths: &[DiscoveryPath],
        discovered_ids: &ExecutionProvenanceIds,
        mut attempts: BTreeMap<String, ProductionPageAttempt>,
        expected_work_generation: Option<u64>,
        context: &JobExecutionContext,
    ) -> ProductionResult<()> {
        for page in pages {
            let attempt = attempts.remove(&page.requested_url).ok_or_else(|| {
                ProductionError::new(ExecutionDiagnostic::new(
                    OrchestrationErrorCategory::Invariant,
                    ExecutionOperation::PersistExecution,
                    ExecutionAction::Fail,
                    "PAGE_ATTEMPT_MISSING",
                ))
            })?;
            match attempt {
                ProductionPageAttempt::Observed { page: result, .. } => {
                    let canonical_url = page
                        .canonical_url
                        .clone()
                        .unwrap_or_else(|| result.observation.requested_url.clone());
                    let page_type_id = page
                        .page_type_match
                        .as_ref()
                        .and_then(|evidence| evidence.winner.as_ref())
                        .map(|winner| winner.page_type_id);
                    let page_is_partial = matches!(
                        result.completeness,
                        CrawlerResultCompleteness::Partial { .. }
                    );
                    let artifacts = self
                        .persist_artifacts(
                            context,
                            run_id,
                            snapshot.created_at(),
                            result.artifacts.clone(),
                            snapshot.settings().retain_artifacts.value,
                        )
                        .await?;
                    let expected_generation = self
                        .expected_generation_for_page(
                            run_id,
                            page,
                            &canonical_url,
                            expected_work_generation,
                        )
                        .await?;
                    self.persist_execution(
                        CrawlExecutionRecord {
                            id: CrawlExecutionId::new(),
                            crawl_run_id: run_id,
                            requested_url: page.requested_url.clone(),
                            canonical_url: canonical_url.clone(),
                            observed_final_url: result.observation.final_url.clone(),
                            source_id: None,
                            page_type_id,
                            transition_id: page_type_id
                                .and_then(|id| transition_for_paths(&canonical_url, id, paths)),
                            discovered_url_id: discovered_ids
                                .get(&(page.requested_url.clone(), canonical_url.clone()))
                                .cloned(),
                            outcome: if page_is_partial {
                                CrawlExecutionOutcome::Partial
                            } else {
                                CrawlExecutionOutcome::Completed
                            },
                            error_code: page_is_partial
                                .then_some(CrawlExecutionErrorCode::PartialResult),
                            http_status: result.status,
                            media_type: result.media_type.clone(),
                            content_length_bytes: result.content_length,
                            provider_elapsed_ms: result.elapsed_ms,
                            artifacts,
                        },
                        context,
                        Some(expected_generation),
                        page.requested_canonical_url != canonical_url,
                    )
                    .await?;
                    self.progress(
                        context,
                        if page_is_partial {
                            "PAGE_PARTIAL"
                        } else {
                            "PAGE_COMPLETED"
                        },
                        None,
                    )
                    .await?;
                }
                ProductionPageAttempt::Failed { failure, .. } => {
                    let canonical_url = page
                        .canonical_url
                        .clone()
                        .unwrap_or_else(|| page.requested_url.clone());
                    let expected_generation = self
                        .expected_generation_for_page(
                            run_id,
                            page,
                            &canonical_url,
                            expected_work_generation,
                        )
                        .await?;
                    self.persist_execution(
                        CrawlExecutionRecord {
                            id: CrawlExecutionId::new(),
                            crawl_run_id: run_id,
                            requested_url: page.requested_url.clone(),
                            canonical_url: canonical_url.clone(),
                            observed_final_url: None,
                            source_id: None,
                            page_type_id: None,
                            transition_id: None,
                            discovered_url_id: discovered_ids
                                .get(&(page.requested_url.clone(), canonical_url.clone()))
                                .cloned(),
                            outcome: if failure.code == CrawlExecutionErrorCode::Cancelled {
                                CrawlExecutionOutcome::Cancelled
                            } else {
                                CrawlExecutionOutcome::Failed
                            },
                            error_code: Some(failure.code),
                            http_status: failure.status,
                            media_type: None,
                            content_length_bytes: None,
                            provider_elapsed_ms: None,
                            artifacts: Vec::new(),
                        },
                        context,
                        Some(expected_generation),
                        page.requested_canonical_url != canonical_url,
                    )
                    .await?;
                    self.progress(context, "PAGE_FAILED", None).await?;
                }
            }
        }
        if attempts.is_empty() {
            Ok(())
        } else {
            Err(ProductionError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Invariant,
                ExecutionOperation::PersistExecution,
                ExecutionAction::Fail,
                "PAGE_ATTEMPTS_UNCONSUMED",
            )))
        }
    }

    #[allow(dead_code, clippy::too_many_arguments, clippy::too_many_lines)]
    async fn persist_page_attempts(
        &self,
        run_id: CrawlRunId,
        snapshot: &CrawlRunSnapshot,
        traversal: &DiscoveryPreviewResult,
        discovered_ids: &ExecutionProvenanceIds,
        attempts: &mut BTreeMap<String, ProductionPageAttempt>,
        context: &JobExecutionContext,
    ) -> ProductionResult<PageAttemptCounts> {
        let mut counts = PageAttemptCounts::default();
        for page in &traversal.pages {
            let attempt = attempts.remove(&page.requested_url).ok_or_else(|| {
                ProductionError::new(ExecutionDiagnostic::new(
                    OrchestrationErrorCategory::Invariant,
                    ExecutionOperation::PersistExecution,
                    ExecutionAction::Fail,
                    "PAGE_ATTEMPT_MISSING",
                ))
            })?;
            counts.attempted = counts.attempted.saturating_add(1);
            match attempt {
                ProductionPageAttempt::Observed { page: result, .. } => {
                    let canonical_url = page
                        .canonical_url
                        .clone()
                        .unwrap_or_else(|| result.observation.requested_url.clone());
                    let page_type_id = page
                        .page_type_match
                        .as_ref()
                        .and_then(|evidence| evidence.winner.as_ref())
                        .map(|winner| winner.page_type_id);
                    let transition_id =
                        page_type_id.and_then(|id| transition_for(&canonical_url, id, traversal));
                    let artifacts = self
                        .persist_artifacts(
                            context,
                            run_id,
                            snapshot.created_at(),
                            result.artifacts.clone(),
                            snapshot.settings().retain_artifacts.value,
                        )
                        .await?;
                    let page_is_partial = matches!(
                        result.completeness,
                        CrawlerResultCompleteness::Partial { .. }
                    );
                    self.persist_execution(
                        CrawlExecutionRecord {
                            id: CrawlExecutionId::new(),
                            crawl_run_id: run_id,
                            requested_url: page.requested_url.clone(),
                            canonical_url: canonical_url.clone(),
                            observed_final_url: result.observation.final_url.clone(),
                            source_id: None,
                            page_type_id,
                            transition_id,
                            discovered_url_id: discovered_ids
                                .get(&(page.requested_url.clone(), canonical_url.clone()))
                                .cloned(),
                            outcome: if page_is_partial {
                                CrawlExecutionOutcome::Partial
                            } else {
                                CrawlExecutionOutcome::Completed
                            },
                            error_code: page_is_partial
                                .then_some(CrawlExecutionErrorCode::PartialResult),
                            http_status: result.status,
                            media_type: result.media_type.clone(),
                            content_length_bytes: result.content_length,
                            provider_elapsed_ms: result.elapsed_ms,
                            artifacts,
                        },
                        context,
                        None,
                        false,
                    )
                    .await?;
                    counts.completed = counts.completed.saturating_add(1);
                    if page_is_partial {
                        counts.unresolved_partial_work =
                            counts.unresolved_partial_work.saturating_add(1);
                    }
                    self.progress(
                        context,
                        if page_is_partial {
                            "PAGE_PARTIAL"
                        } else {
                            "PAGE_COMPLETED"
                        },
                        None,
                    )
                    .await?;
                }
                ProductionPageAttempt::Failed { failure, .. } => {
                    let canonical_url = page
                        .canonical_url
                        .clone()
                        .unwrap_or_else(|| page.requested_url.clone());
                    self.persist_execution(
                        CrawlExecutionRecord {
                            id: CrawlExecutionId::new(),
                            crawl_run_id: run_id,
                            requested_url: page.requested_url.clone(),
                            canonical_url: canonical_url.clone(),
                            observed_final_url: None,
                            source_id: None,
                            page_type_id: None,
                            transition_id: None,
                            discovered_url_id: discovered_ids
                                .get(&(page.requested_url.clone(), canonical_url.clone()))
                                .cloned(),
                            outcome: CrawlExecutionOutcome::Failed,
                            error_code: Some(failure.code),
                            http_status: failure.status,
                            media_type: None,
                            content_length_bytes: None,
                            provider_elapsed_ms: None,
                            artifacts: Vec::new(),
                        },
                        context,
                        None,
                        false,
                    )
                    .await?;
                    counts.unresolved_partial_work =
                        counts.unresolved_partial_work.saturating_add(1);
                    self.progress(context, "PAGE_FAILED", None).await?;
                }
            }
        }
        attempts.is_empty().then_some(counts).ok_or_else(|| {
            ProductionError::new(ExecutionDiagnostic::new(
                OrchestrationErrorCategory::Invariant,
                ExecutionOperation::PersistExecution,
                ExecutionAction::Fail,
                "PAGE_ATTEMPTS_UNCONSUMED",
            ))
        })
    }

    async fn expected_generation_for_page(
        &self,
        run_id: CrawlRunId,
        page: &DiscoveryPreviewPage,
        canonical_url: &str,
        expected_work_generation: Option<u64>,
    ) -> ProductionResult<u64> {
        if page.requested_canonical_url == canonical_url {
            expected_work_generation.ok_or_else(|| {
                ProductionError::new(ExecutionDiagnostic::new(
                    OrchestrationErrorCategory::Invariant,
                    ExecutionOperation::PersistExecution,
                    ExecutionAction::Fail,
                    "WORK_GENERATION_MISSING",
                ))
            })
        } else {
            CrawlTraversalRepository::new(&self.database)
                .read_work_generation(run_id, &crawl_url_state_id(run_id, canonical_url))
                .await
                .map_err(|_| {
                    ProductionError::repository(
                        ExecutionOperation::PersistExecution,
                        "WORK_GENERATION_LOAD_FAILED",
                    )
                })
        }
    }

    #[allow(dead_code)]
    async fn persist_discovery_paths(
        &self,
        run_id: CrawlRunId,
        traversal: &DiscoveryPreviewResult,
        attempts: &BTreeMap<String, ProductionPageAttempt>,
    ) -> Result<ExecutionProvenanceIds, ()> {
        let repository = CrawlRunRepository::new(&self.database);
        let mut execution_ids = BTreeMap::new();
        for seed in &traversal.seeds {
            let id = discovered_id();
            let original_url = fragment_free_fetch_url(&seed.requested_url)?;
            let status = if seed.duplicate_of_canonical_url.is_some() {
                "CANONICAL_DUPLICATE"
            } else if seed.state == PreviewUrlState::InScopeMatched {
                "ADMITTED"
            } else {
                discovery_seed_status(seed.state)
            };
            repository
                .record_discovered_url(&DiscoveredUrlRecord {
                    id: id.clone(),
                    crawl_run_id: run_id,
                    source_id: None,
                    raw_href: None,
                    original_url: original_url.clone(),
                    canonical_url: seed.canonical_url.clone(),
                    status: status.to_owned(),
                    discovered_at: discovered_at(
                        attempts,
                        &seed.requested_url,
                        self.clock.now_millis(),
                    ),
                    detail: serde_json::json!({
                        "origin": "SEED",
                        "seed_ids": [seed.seed_id.to_string()],
                        "entry_page_type_hint": seed.entry_page_type_hint.map(|id| id.to_string()),
                        "duplicate_of_canonical_url": seed.duplicate_of_canonical_url,
                        "scope": seed.scope,
                        "page_type_match": seed.page_type_match,
                        "budget_hits": seed.budget_hits,
                    }),
                })
                .await
                .map_err(|_| ())?;
            if seed.duplicate_of_canonical_url.is_none() {
                insert_execution_provenance(
                    &mut execution_ids,
                    original_url,
                    seed.canonical_url.clone(),
                    id,
                );
            }
        }
        for path in &traversal.discovery_paths {
            let id = discovered_id();
            let canonical_url = path
                .canonical_url
                .clone()
                .unwrap_or_else(|| path.source_canonical_url.clone());
            let resolved_original_url = path
                .resolved_original_url
                .clone()
                .unwrap_or_else(|| path.source_canonical_url.clone());
            let original_url = fragment_free_fetch_url(&resolved_original_url)?;
            let status = discovery_status(path);
            repository
                .record_discovered_url(&DiscoveredUrlRecord {
                    id: id.clone(),
                    crawl_run_id: run_id,
                    source_id: None,
                    raw_href: Some(path.raw_href.clone()),
                    original_url: original_url.clone(),
                    canonical_url: canonical_url.clone(),
                    status: status.to_owned(),
                    // This is captured with the observed source page rather
                    // than copying frozen run submission metadata.
                    discovered_at: discovered_at(
                        attempts,
                        &path.source_requested_url,
                        self.clock.now_millis(),
                    ),
                    detail: serde_json::json!({
                        "seed_ids": path.seed_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                        "source_requested_url": path.source_requested_url,
                        "source_final_url": path.source_final_url,
                        "source_canonical_url": path.source_canonical_url,
                        "source_depth": path.source_depth,
                        "selector": path.selector,
                        "resolved_observation_url": path.resolved_original_url,
                        "duplicate_of_canonical_url": path.duplicate_of_canonical_url,
                        "transition_evaluations": path.transition_evaluations,
                        "budget_hits": path.budget_hits,
                    }),
                })
                .await
                .map_err(|_| ())?;
            if status == "ADMITTED" {
                insert_execution_provenance(&mut execution_ids, original_url, canonical_url, id);
            }
        }
        self.persist_execution_reconciliations(run_id, traversal, attempts, &mut execution_ids)
            .await?;
        Ok(execution_ids)
    }

    #[allow(dead_code)]
    async fn persist_execution_reconciliations(
        &self,
        run_id: CrawlRunId,
        traversal: &DiscoveryPreviewResult,
        attempts: &BTreeMap<String, ProductionPageAttempt>,
        execution_ids: &mut ExecutionProvenanceIds,
    ) -> Result<(), ()> {
        let repository = CrawlRunRepository::new(&self.database);
        for page in &traversal.pages {
            let canonical_url = page
                .canonical_url
                .clone()
                .unwrap_or_else(|| page.requested_url.clone());
            let original_url = fragment_free_fetch_url(&page.requested_url)?;
            let key = (original_url.clone(), canonical_url.clone());
            if execution_ids.contains_key(&key) {
                continue;
            }
            let id = discovered_id();
            repository
                .record_discovered_url(&DiscoveredUrlRecord {
                    id: id.clone(),
                    crawl_run_id: run_id,
                    source_id: None,
                    raw_href: None,
                    original_url: original_url.clone(),
                    canonical_url: canonical_url.clone(),
                    status: "EXECUTION_RECONCILED".to_owned(),
                    discovered_at: discovered_at(
                        attempts,
                        &page.requested_url,
                        self.clock.now_millis(),
                    ),
                    detail: serde_json::json!({
                        "origin": "EXECUTION_RECONCILIATION",
                        "requested_url": page.requested_url,
                        "observed_final_url": page.final_url,
                        "authoritative_canonical_url": canonical_url.clone(),
                        "seed_ids": page.seed_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
                    }),
                })
                .await
                .map_err(|_| ())?;
            insert_execution_provenance(execution_ids, original_url, canonical_url, id);
        }
        Ok(())
    }
}

fn production_limits(
    snapshot: &CrawlRunSnapshot,
    version: &erabi_domain::CrawlerVersion,
) -> Result<EffectiveDiscoveryPreviewLimits, ()> {
    let max_duration_ms = snapshot
        .settings()
        .max_duration_seconds
        .value
        .checked_mul(1_000)
        .ok_or(())?;
    let mut transition_total_limits = version
        .transition_ids()
        .iter()
        .copied()
        .map(|transition_id| EffectiveTransitionPreviewTotalLimit {
            transition_id,
            // The shared traversal independently evaluates configured total
            // budgets. Production adds no Preview-only artificial cap.
            effective_total_limit: u64::MAX,
        })
        .collect::<Vec<_>>();
    transition_total_limits.sort_by(|left, right| {
        left.transition_id
            .to_string()
            .cmp(&right.transition_id.to_string())
    });
    Ok(EffectiveDiscoveryPreviewLimits {
        max_pages: snapshot.settings().max_pages.value,
        max_depth: snapshot.settings().max_depth.value,
        max_duration_ms,
        max_downloaded_bytes: version.guardrails().max_downloaded_bytes,
        transition_total_limits,
    })
}

/// A checkpoint is required to recover interrupted frontier state, not to
/// repeat a run whose every admitted unit is already represented by durable
/// discovery and execution evidence.  The provenance tuple prevents a
/// redirect/canonical collision from treating a different admission as done.
fn durable_production_completion_without_checkpoint(
    executions: &[CrawlExecutionRecord],
    discovered: &[DiscoveredUrlRecord],
) -> bool {
    let admitted = discovered
        .iter()
        .filter(|record| matches!(record.status.as_str(), "ADMITTED" | "EXECUTION_RECONCILED"))
        .map(|record| {
            (
                record.id.as_str(),
                record.original_url.as_str(),
                record.canonical_url.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    !admitted.is_empty()
        && admitted.iter().all(|(id, requested, canonical)| {
            executions.iter().any(|execution| {
                execution.discovered_url_id.as_deref() == Some(*id)
                    && execution.requested_url == *requested
                    && execution.canonical_url == *canonical
            })
        })
}

#[allow(dead_code)]
fn transition_for(
    canonical_url: &str,
    page_type_id: PageTypeId,
    traversal: &DiscoveryPreviewResult,
) -> Option<DiscoveryTransitionId> {
    traversal
        .discovery_paths
        .iter()
        .find(|path| path.canonical_url.as_deref() == Some(canonical_url))
        .and_then(|path| {
            path.transition_evaluations.iter().find_map(|evaluation| {
                (evaluation.eligible && evaluation.target_page_type_id == page_type_id)
                    .then_some(evaluation.transition_id)
            })
        })
}

fn transition_for_paths(
    canonical_url: &str,
    page_type_id: PageTypeId,
    paths: &[DiscoveryPath],
) -> Option<DiscoveryTransitionId> {
    paths
        .iter()
        .find(|path| path.canonical_url.as_deref() == Some(canonical_url))
        .and_then(|path| {
            path.transition_evaluations.iter().find_map(|evaluation| {
                (evaluation.eligible && evaluation.target_page_type_id == page_type_id)
                    .then_some(evaluation.transition_id)
            })
        })
}

#[allow(dead_code)]
fn count_ambiguities(traversal: &DiscoveryPreviewResult) -> u64 {
    let pages = traversal
        .pages
        .iter()
        .filter(|page| page.state == PreviewUrlState::AmbiguousPageType)
        .count();
    let paths = traversal
        .discovery_paths
        .iter()
        .filter(|path| path.state == PreviewUrlState::AmbiguousPageType)
        .count();
    u64::try_from(pages.saturating_add(paths)).unwrap_or(u64::MAX)
}

#[allow(dead_code)]
fn duration_left_known_incomplete(traversal: &DiscoveryPreviewResult) -> bool {
    let duration_hit = traversal
        .summary
        .budget_hit_counts
        .get(&PreviewBudgetKind::MaxDuration)
        .is_some_and(|count| *count > 0);
    duration_hit
        && (traversal.summary.frontier_remaining > 0
            || traversal.summary.duration_work_not_expanded)
}

fn fragment_free_fetch_url(value: &str) -> Result<String, ()> {
    let mut parsed = url::Url::parse(value).map_err(|_| ())?;
    parsed.set_fragment(None);
    Ok(parsed.to_string())
}

fn decode_id<T: DeserializeOwned>(value: &str) -> Result<T, ()> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).map_err(|_| ())
}

fn insert_execution_provenance(
    ids: &mut ExecutionProvenanceIds,
    original_url: String,
    canonical_url: String,
    id: String,
) {
    ids.insert((original_url, canonical_url), id);
}

fn discovered_at(
    attempts: &BTreeMap<String, ProductionPageAttempt>,
    source_requested_url: &str,
    fallback_millis: u64,
) -> String {
    let millis = attempts
        .get(source_requested_url)
        .map_or(fallback_millis, ProductionPageAttempt::observed_at_millis);
    format!("unix-ms:{millis}")
}

fn discovery_seed_status(state: PreviewUrlState) -> &'static str {
    match state {
        PreviewUrlState::InScopeMatched => "ADMITTED",
        PreviewUrlState::AmbiguousPageType => "AMBIGUOUS_PAGE_TYPE",
        PreviewUrlState::Unmatched => "UNMATCHED",
        PreviewUrlState::External => "EXTERNAL",
        PreviewUrlState::Blocked => "BLOCKED",
        PreviewUrlState::CanonicalDuplicate => "CANONICAL_DUPLICATE",
        PreviewUrlState::BudgetExcluded => "BUDGET_EXCLUDED",
        PreviewUrlState::InvalidUrl => "INVALID",
        PreviewUrlState::RobotsExcluded => "ROBOTS_EXCLUDED",
        PreviewUrlState::ProviderError => "PROVIDER_ERROR",
        PreviewUrlState::Sampled => "SAMPLED",
    }
}

fn discovery_status(path: &erabi_domain::DiscoveryPath) -> &'static str {
    match path.state {
        PreviewUrlState::InScopeMatched => {
            if path
                .transition_evaluations
                .iter()
                .any(|evaluation| evaluation.eligible)
            {
                "ADMITTED"
            } else {
                "TRANSITION_INELIGIBLE"
            }
        }
        PreviewUrlState::AmbiguousPageType => "AMBIGUOUS_PAGE_TYPE",
        PreviewUrlState::Unmatched => "UNMATCHED",
        PreviewUrlState::External => "EXTERNAL",
        PreviewUrlState::Blocked => "BLOCKED",
        PreviewUrlState::CanonicalDuplicate => "CANONICAL_DUPLICATE",
        PreviewUrlState::BudgetExcluded => "BUDGET_EXCLUDED",
        PreviewUrlState::InvalidUrl => "INVALID",
        PreviewUrlState::RobotsExcluded => "ROBOTS_EXCLUDED",
        PreviewUrlState::ProviderError => "PROVIDER_ERROR",
        PreviewUrlState::Sampled => "SAMPLED",
    }
}

fn preserve_reason_for_discovery_status(status: &str) -> Option<&'static str> {
    match status {
        "CANONICAL_DUPLICATE" => Some("CANONICAL_DUPLICATE"),
        "AMBIGUOUS_PAGE_TYPE" => Some("AMBIGUOUS_PAGE_TYPE"),
        "UNMATCHED" => Some("UNMATCHED"),
        "EXTERNAL" => Some("EXTERNAL"),
        "BLOCKED" => Some("BLOCKED"),
        "BUDGET_EXCLUDED" => Some("BUDGET_EXCLUDED"),
        "INVALID" => Some("INVALID"),
        "ROBOTS_EXCLUDED" => Some("ROBOTS_EXCLUDED"),
        "PROVIDER_ERROR" => Some("PROVIDER_ERROR"),
        "TRANSITION_INELIGIBLE" => Some("TRANSITION_INELIGIBLE"),
        _ => None,
    }
}

fn parse_run_id(value: &str) -> Option<CrawlRunId> {
    Uuid::parse_str(value).ok().and_then(CrawlRunId::from_uuid)
}

pub(super) fn discovered_id() -> String {
    Uuid::now_v7().to_string()
}

fn semantic_discovered_id<T: serde::Serialize>(
    run_id: CrawlRunId,
    kind: &str,
    work_generation: u64,
    identity: &T,
) -> Result<String, ()> {
    let digest =
        erabi_domain::canonical_sha256(&(run_id.to_string(), kind, work_generation, identity))
            .map_err(|_| ())?;
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&digest[index * 2..index * 2 + 2], 16).map_err(|_| ())?;
    }
    // Keep the existing UUID-shaped DiscoveredUrlId contract while making the
    // semantic observation identity stable across an uncertain transaction
    // replay. The work generation remains part of the identity, so a retry is
    // retained as a distinct physical discovery observation.
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(bytes).to_string())
}
