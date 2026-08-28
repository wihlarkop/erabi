//! Synchronous, bounded Discovery Preview orchestration.
//!
//! This module deliberately owns only an ephemeral sampling traversal. It
//! does not create run records, persist evidence, or know about a production
//! crawler worker. The database is consulted once for the coherent semantic
//! snapshot and all subsequent decisions use that frozen value.

#![allow(
    clippy::assigning_clones,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::collapsible_if,
    clippy::if_not_else,
    clippy::needless_pass_by_value,
    clippy::too_many_lines,
    clippy::unused_self
)]

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

use erabi_db::{
    ErabiDatabase,
    repositories::{
        CrawlerEvaluationSnapshot, CrawlerRepository, CrawlerRepositoryError,
        CrawlerSemanticSnapshot,
    },
};
use erabi_domain::{
    CanonicalizationDecision, CanonicalizationDecisionCode, CanonicalizationEvidence,
    CanonicalizationOutcome, DiscoveryBudgetCandidate, DiscoveryBudgetDecision,
    DiscoveryBudgetEvaluator, DiscoveryBudgetExclusion, DiscoveryPath, DiscoveryPreviewLimits,
    DiscoveryPreviewRequest, DiscoveryPreviewResult, DiscoveryPreviewResultSemantics,
    DiscoveryPreviewSeed, DiscoveryPreviewSummary, DiscoveryTransition, DiscoveryTransitionId,
    DomainScopeEvidence, MAX_PREVIEW_DIAGNOSTICS, MAX_PREVIEW_LINKS_PER_OBSERVATION,
    MAX_PREVIEW_PROVENANCE_EDGES, MAX_PREVIEW_SELECTED_SEEDS, MAX_PREVIEW_URL_CHARS, PageType,
    PageTypeMatchEvidence, PageTypeMatchStatus, PreviewBudgetHit, PreviewBudgetKind,
    PreviewDiagnostic, PreviewGrowthIndicators, PreviewGrowthWarning, PreviewGrowthWarningCode,
    PreviewPageTypeDistribution, PreviewQueryVariantGroup, PreviewTransitionCount,
    PreviewTransitionEvaluation, PreviewUrlState, Seed, SeedId, TestDiagnostic, TransitionGraph,
    resolve_page_type,
};

use super::{
    DiscoveryPreviewObservationRequest, DiscoveryPreviewProvider, DiscoveryPreviewProviderError,
    DiscoveryPreviewProviderOutcome, MonotonicPreviewClock, PreviewClock,
};
use crate::observation::{ObservedLink, PageObservation};

const MAX_PROVIDER_DIAGNOSTIC_CHARS: usize = 1_024;
const MAX_ROBOTS_REASON_CHARS: usize = 512;

/// Errors crossing the application-service boundary. Ordinary page outcomes
/// are retained in a successful result; these variants are request, snapshot,
/// or provider-contract failures.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryPreviewError {
    #[error("Crawler was not found")]
    CrawlerNotFound,
    #[error("CrawlerVersion was not found")]
    CrawlerVersionNotFound,
    #[error("CrawlerVersion does not belong to the requested Crawler")]
    VersionNotOwnedByCrawler,
    #[error("CrawlerVersion is not a Draft")]
    VersionNotDraft,
    #[error("CrawlerVersion is not the active Draft")]
    VersionNotActiveDraft,
    #[error("invalid Discovery Preview request")]
    InvalidRequest,
    #[error("no selected Seeds were provided")]
    NoSelectedSeeds,
    #[error("selected Seed IDs contain duplicates")]
    DuplicateSeedSelection,
    #[error("selected Seed does not belong to the CrawlerVersion")]
    SeedNotOwnedByVersion,
    #[error("selected Seed is disabled")]
    SeedDisabled,
    #[error("Preview limits are invalid")]
    InvalidPreviewLimits,
    #[error("Preview transition limit is invalid")]
    InvalidTransitionPreviewLimit,
    #[error("Preview transition does not belong to the CrawlerVersion")]
    TransitionNotOwnedByVersion,
    #[error("Discovery Preview provider is unavailable")]
    ProviderUnavailable,
    #[error("Discovery Preview provider returned an observation for a different request URL")]
    ProviderObservationRequestMismatch,
    #[error("Discovery Preview provider returned an invalid observation")]
    ProviderObservationInvalid,
    #[error("durable Crawler state is invalid")]
    PersistedStateInvalid,
    #[error("Discovery Preview budget arithmetic overflowed")]
    BudgetOverflow,
}

/// Application service for ephemeral bounded preview traversal.
pub struct DiscoveryPreviewService {
    database: ErabiDatabase,
    provider: Option<Arc<dyn DiscoveryPreviewProvider>>,
    clock: Arc<dyn PreviewClock>,
}

impl std::fmt::Debug for DiscoveryPreviewService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DiscoveryPreviewService")
            .field("database", &self.database)
            .field("provider_configured", &self.provider.is_some())
            .finish_non_exhaustive()
    }
}

impl DiscoveryPreviewService {
    #[must_use]
    pub fn new(
        database: ErabiDatabase,
        provider: Option<Arc<dyn DiscoveryPreviewProvider>>,
    ) -> Self {
        Self::with_clock(database, provider, Arc::new(MonotonicPreviewClock::new()))
    }

    #[must_use]
    pub fn with_clock(
        database: ErabiDatabase,
        provider: Option<Arc<dyn DiscoveryPreviewProvider>>,
        clock: Arc<dyn PreviewClock>,
    ) -> Self {
        Self {
            database,
            provider,
            clock,
        }
    }

    /// Captures one active Draft snapshot, then executes a deterministic BFS.
    /// No run, job, `TestEvidence`, or production record is created.
    ///
    /// # Errors
    /// Returns a stable request, persisted-state, or provider-contract error.
    pub async fn execute(
        &self,
        crawler_id: erabi_domain::CrawlerId,
        version_id: erabi_domain::CrawlerVersionId,
        request: DiscoveryPreviewRequest,
    ) -> Result<DiscoveryPreviewResult, DiscoveryPreviewError> {
        request
            .limits
            .validate()
            .map_err(|_| DiscoveryPreviewError::InvalidPreviewLimits)?;
        if request.seed_ids.is_empty() {
            return Err(DiscoveryPreviewError::NoSelectedSeeds);
        }
        if request.seed_ids.len() > MAX_PREVIEW_SELECTED_SEEDS {
            return Err(DiscoveryPreviewError::InvalidRequest);
        }
        let mut requested_seed_ids = BTreeSet::new();
        if request
            .seed_ids
            .iter()
            .any(|seed_id| !requested_seed_ids.insert(seed_id.to_string()))
        {
            return Err(DiscoveryPreviewError::DuplicateSeedSelection);
        }
        let provider = self
            .provider
            .as_ref()
            .ok_or(DiscoveryPreviewError::ProviderUnavailable)?
            .clone();

        // This is the only semantic read. All traversal decisions below use
        // the owned snapshot, so a concurrent Draft edit cannot be mixed in.
        let snapshot = CrawlerRepository::new(&self.database)
            .evaluation_snapshot(crawler_id, version_id, false)
            .await
            .map_err(map_crawler_error)?;
        let context = PreviewContext::new(&snapshot, request.limits.clone())?;
        let selected_seeds = select_seeds(&context, &request.seed_ids)?;
        let mut run = PreviewRun::new(
            context,
            request.seed_ids,
            provider,
            self.clock.clone(),
            self.clock.now_millis(),
        );
        run.admit_roots(&selected_seeds)?;
        run.traverse().await?;
        Ok(run.finish())
    }
}

struct PreviewContext {
    snapshot: CrawlerSemanticSnapshot,
    page_types: Vec<PageType>,
    transitions: Vec<DiscoveryTransition>,
    graph: TransitionGraph,
    limits: erabi_domain::EffectiveDiscoveryPreviewLimits,
}

impl PreviewContext {
    fn new(
        evaluation: &CrawlerEvaluationSnapshot,
        limits: DiscoveryPreviewLimits,
    ) -> Result<Self, DiscoveryPreviewError> {
        let snapshot = evaluation.draft.clone();
        snapshot
            .version
            .validate_semantic_contract()
            .map_err(|_| DiscoveryPreviewError::PersistedStateInvalid)?;
        let page_types = snapshot
            .page_types
            .iter()
            .map(erabi_db::repositories::PageTypeRecord::domain_page_type)
            .collect::<Vec<_>>();
        let persisted_page_type_ids = page_types
            .iter()
            .map(|page_type| page_type.id.to_string())
            .collect::<BTreeSet<_>>();
        let declared_page_type_ids = snapshot
            .version
            .page_type_ids()
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        if persisted_page_type_ids != declared_page_type_ids {
            return Err(DiscoveryPreviewError::PersistedStateInvalid);
        }
        let transitions = snapshot
            .transitions
            .iter()
            .map(|record| record.transition.clone())
            .collect::<Vec<_>>();
        let persisted_transition_ids = transitions
            .iter()
            .map(|transition| transition.id.to_string())
            .collect::<BTreeSet<_>>();
        let declared_transition_ids = snapshot
            .version
            .transition_ids()
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        if persisted_transition_ids != declared_transition_ids {
            return Err(DiscoveryPreviewError::PersistedStateInvalid);
        }
        let graph = TransitionGraph::new(snapshot.version.page_type_ids(), transitions.clone())
            .map_err(|_| DiscoveryPreviewError::PersistedStateInvalid)?;
        let semantic_duration_ms = snapshot
            .version
            .guardrails()
            .max_duration_seconds
            .checked_mul(1_000)
            .ok_or(DiscoveryPreviewError::BudgetOverflow)?;
        let transition_total_limits = limits
            .transition_total_limits
            .iter()
            .map(|override_limit| {
                if !transitions
                    .iter()
                    .any(|transition| transition.id == override_limit.transition_id)
                {
                    return Err(DiscoveryPreviewError::TransitionNotOwnedByVersion);
                }
                Ok(erabi_domain::TransitionPreviewTotalLimit {
                    transition_id: override_limit.transition_id,
                    max_total_links: override_limit
                        .max_total_links
                        .min(limits.default_transition_total_limit),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let effective = erabi_domain::EffectiveDiscoveryPreviewLimits {
            max_pages: limits
                .max_pages
                .min(snapshot.version.guardrails().max_pages),
            max_depth: limits
                .max_depth
                .min(snapshot.version.guardrails().max_depth),
            max_duration_ms: limits.max_duration_ms.min(semantic_duration_ms),
            max_downloaded_bytes: snapshot.version.guardrails().max_downloaded_bytes,
            default_transition_total_limit: limits.default_transition_total_limit,
            transition_total_limits,
        };
        Ok(Self {
            snapshot,
            page_types,
            transitions,
            graph,
            limits: effective,
        })
    }

    fn transition_total_limit(&self, id: DiscoveryTransitionId) -> u64 {
        self.limits
            .transition_total_limits
            .iter()
            .find(|limit| limit.transition_id == id)
            .map_or(self.limits.default_transition_total_limit, |limit| {
                limit
                    .max_total_links
                    .min(self.limits.default_transition_total_limit)
            })
    }

    fn seed(&self, id: SeedId) -> Option<&Seed> {
        self.snapshot
            .version
            .seeds()
            .iter()
            .find(|seed| seed.id == id)
    }
}

fn select_seeds(
    context: &PreviewContext,
    ids: &[SeedId],
) -> Result<Vec<Seed>, DiscoveryPreviewError> {
    ids.iter()
        .map(|id| {
            let seed = context
                .seed(*id)
                .ok_or(DiscoveryPreviewError::SeedNotOwnedByVersion)?;
            if !seed.enabled {
                return Err(DiscoveryPreviewError::SeedDisabled);
            }
            let canonical = context
                .snapshot
                .version
                .canonicalization_policy()
                .canonicalize(seed.original_url.as_str())
                .map_err(|_| DiscoveryPreviewError::PersistedStateInvalid)?;
            if canonical.canonical_url != seed.canonical_url {
                return Err(DiscoveryPreviewError::PersistedStateInvalid);
            }
            if seed
                .entry_page_type_hint
                .is_some_and(|id| !context.snapshot.version.page_type_ids().contains(&id))
            {
                return Err(DiscoveryPreviewError::PersistedStateInvalid);
            }
            Ok(seed.clone())
        })
        .collect()
}

struct QueueEntry {
    requested_url: String,
    canonical_url: String,
    depth: u32,
    seed_ids: Vec<SeedId>,
    /// Present only for a discovery-admitted target. Roots are already at
    /// depth zero and therefore never need a duplicate depth reduction.
    target_page_type_id: Option<erabi_domain::PageTypeId>,
    order: usize,
}

struct PreviewRun {
    context: PreviewContext,
    selected_seed_ids: Vec<SeedId>,
    provider: Arc<dyn DiscoveryPreviewProvider>,
    clock: Arc<dyn PreviewClock>,
    start_millis: u64,
    queue: VecDeque<QueueEntry>,
    queued: BTreeMap<String, QueueEntry>,
    admitted: BTreeSet<String>,
    seen: BTreeSet<String>,
    sampled: BTreeSet<String>,
    expanded: BTreeSet<String>,
    pages: Vec<erabi_domain::DiscoveryPreviewPage>,
    seeds: Vec<DiscoveryPreviewSeed>,
    paths: Vec<DiscoveryPath>,
    diagnostics: Vec<PreviewDiagnostic>,
    budget_hits: BTreeMap<PreviewBudgetKind, u64>,
    transition_counts: BTreeMap<String, TransitionRuntimeCount>,
    transition_page_counts: BTreeMap<(String, String), u32>,
    page_type_sampled: BTreeMap<String, u64>,
    page_type_discovered: BTreeMap<String, u64>,
    page_type_scheduled: BTreeMap<String, u64>,
    matching_urls: BTreeSet<String>,
    unmatched_urls: BTreeSet<String>,
    ambiguous_urls: BTreeSet<String>,
    in_scope_urls: BTreeSet<String>,
    consumed_bytes: u64,
    pages_sampled: u64,
    urls_discovered: u64,
    duplicates_prevented: u64,
    robots_excluded: u64,
    provider_errors: u64,
    external_urls: u64,
    blocked_urls: u64,
    newly_enqueued_urls: u64,
    peak_new_from_page: u64,
    time_budget_hit: bool,
}

#[derive(Default)]
struct TransitionRuntimeCount {
    transition_id: DiscoveryTransitionId,
    name: String,
    eligible_edges: u64,
    source_pages: BTreeSet<String>,
}

impl PreviewRun {
    fn new(
        context: PreviewContext,
        selected_seed_ids: Vec<SeedId>,
        provider: Arc<dyn DiscoveryPreviewProvider>,
        clock: Arc<dyn PreviewClock>,
        start_millis: u64,
    ) -> Self {
        let transition_counts = context
            .transitions
            .iter()
            .map(|transition| {
                (
                    transition.id.to_string(),
                    TransitionRuntimeCount {
                        transition_id: transition.id,
                        name: transition.name.clone(),
                        ..TransitionRuntimeCount::default()
                    },
                )
            })
            .collect();
        Self {
            context,
            selected_seed_ids,
            provider,
            clock,
            start_millis,
            queue: VecDeque::new(),
            queued: BTreeMap::new(),
            admitted: BTreeSet::new(),
            seen: BTreeSet::new(),
            sampled: BTreeSet::new(),
            expanded: BTreeSet::new(),
            pages: Vec::new(),
            seeds: Vec::new(),
            paths: Vec::new(),
            diagnostics: Vec::new(),
            budget_hits: BTreeMap::new(),
            transition_counts,
            transition_page_counts: BTreeMap::new(),
            page_type_sampled: BTreeMap::new(),
            page_type_discovered: BTreeMap::new(),
            page_type_scheduled: BTreeMap::new(),
            matching_urls: BTreeSet::new(),
            unmatched_urls: BTreeSet::new(),
            ambiguous_urls: BTreeSet::new(),
            in_scope_urls: BTreeSet::new(),
            consumed_bytes: 0,
            pages_sampled: 0,
            urls_discovered: 0,
            duplicates_prevented: 0,
            robots_excluded: 0,
            provider_errors: 0,
            external_urls: 0,
            blocked_urls: 0,
            newly_enqueued_urls: 0,
            peak_new_from_page: 0,
            time_budget_hit: false,
        }
    }

    fn admit_roots(&mut self, seeds: &[Seed]) -> Result<(), DiscoveryPreviewError> {
        for (order, seed) in seeds.iter().enumerate() {
            let canonical_url = seed.canonical_url.to_string();
            let scope = self.scope(&seed.canonical_url)?;
            let mut state = PreviewUrlState::InScopeMatched;
            let duplicate_of = if !self.seen.insert(canonical_url.clone()) {
                self.duplicates_prevented = self.duplicates_prevented.saturating_add(1);
                state = PreviewUrlState::CanonicalDuplicate;
                let merged_root_provenance =
                    if let Some(existing) = self.queued.get_mut(&canonical_url) {
                        // Selected roots are admitted before traversal. Merge
                        // equivalent roots into that one queue identity so the
                        // sampled page and every retained path preserve all
                        // explicit Seed provenance in request order.
                        if !existing.seed_ids.contains(&seed.id) {
                            existing.seed_ids.push(seed.id);
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                if merged_root_provenance {
                    self.rebuild_queue();
                }
                Some(canonical_url.clone())
            } else {
                None
            };
            if duplicate_of.is_none()
                && matches!(
                    scope.as_ref().map(|item| item.classification),
                    Some(erabi_domain::DomainScopeStatus::InScope)
                )
            {
                if self.admitted.len() as u64 >= self.context.limits.max_pages {
                    state = PreviewUrlState::BudgetExcluded;
                    self.record_budget(PreviewBudgetHit {
                        kind: PreviewBudgetKind::MaxPages,
                        transition_id: None,
                        page_type_id: None,
                        observed: self.admitted.len() as u64,
                        limit: self.context.limits.max_pages,
                    });
                } else {
                    self.admitted.insert(canonical_url.clone());
                    self.queued.insert(
                        canonical_url.clone(),
                        QueueEntry {
                            requested_url: seed.original_url.to_string(),
                            canonical_url: canonical_url.clone(),
                            depth: 0,
                            seed_ids: vec![seed.id],
                            target_page_type_id: None,
                            order,
                        },
                    );
                    self.rebuild_queue();
                    self.newly_enqueued_urls = self.newly_enqueued_urls.saturating_add(1);
                }
            } else if duplicate_of.is_none() {
                state = scope_state(scope.as_ref());
                self.count_scope(state);
            }
            self.seeds.push(DiscoveryPreviewSeed {
                seed_id: seed.id,
                requested_url: seed.original_url.to_string(),
                canonical_url,
                entry_page_type_hint: seed.entry_page_type_hint,
                state,
                duplicate_of_canonical_url: duplicate_of,
                scope,
                page_type_match: None,
                budget_hits: Vec::new(),
            });
        }
        Ok(())
    }

    fn rebuild_queue(&mut self) {
        let mut values = self.queued.values().collect::<Vec<_>>();
        values.sort_by(|left, right| {
            left.depth
                .cmp(&right.depth)
                .then(left.order.cmp(&right.order))
                .then(left.canonical_url.cmp(&right.canonical_url))
                .then(left.requested_url.cmp(&right.requested_url))
        });
        self.queue = values
            .into_iter()
            .map(|entry| QueueEntry {
                requested_url: entry.requested_url.clone(),
                canonical_url: entry.canonical_url.clone(),
                depth: entry.depth,
                seed_ids: entry.seed_ids.clone(),
                target_page_type_id: entry.target_page_type_id,
                order: entry.order,
            })
            .collect();
    }

    async fn traverse(&mut self) -> Result<(), DiscoveryPreviewError> {
        while !self.queue.is_empty() {
            if self.elapsed_millis() >= self.context.limits.max_duration_ms {
                self.hit_time_budget();
                break;
            }
            if self.consumed_bytes >= self.context.limits.max_downloaded_bytes {
                self.record_budget(PreviewBudgetHit {
                    kind: PreviewBudgetKind::MaxDownloadedBytes,
                    transition_id: None,
                    page_type_id: None,
                    observed: self.consumed_bytes,
                    limit: self.context.limits.max_downloaded_bytes,
                });
                break;
            }
            let Some(entry) = self.queue.pop_front() else {
                break;
            };
            self.queued.remove(&entry.canonical_url);
            let remaining = self
                .context
                .limits
                .max_downloaded_bytes
                .checked_sub(self.consumed_bytes)
                .ok_or(DiscoveryPreviewError::BudgetOverflow)?;
            let outcome = self
                .provider
                .observe(DiscoveryPreviewObservationRequest {
                    requested_url: entry.requested_url.clone(),
                    remaining_download_bytes: remaining,
                })
                .await
                .map_err(map_provider_error)?;
            if self.elapsed_millis() >= self.context.limits.max_duration_ms {
                self.time_budget_hit = true;
                self.record_budget(PreviewBudgetHit {
                    kind: PreviewBudgetKind::MaxDuration,
                    transition_id: None,
                    page_type_id: None,
                    observed: self.elapsed_millis(),
                    limit: self.context.limits.max_duration_ms,
                });
            }
            self.process_outcome(entry, outcome)?;
        }
        Ok(())
    }

    fn process_outcome(
        &mut self,
        entry: QueueEntry,
        outcome: DiscoveryPreviewProviderOutcome,
    ) -> Result<(), DiscoveryPreviewError> {
        match outcome {
            DiscoveryPreviewProviderOutcome::RobotsExcluded { reason } => {
                validate_safe_text(&reason, MAX_ROBOTS_REASON_CHARS)?;
                self.robots_excluded = self.robots_excluded.saturating_add(1);
                let scope = url::Url::parse(&entry.canonical_url)
                    .ok()
                    .map(|url| self.scope(&url))
                    .transpose()?
                    .flatten();
                self.update_seed_outcome(
                    &entry.seed_ids,
                    PreviewUrlState::RobotsExcluded,
                    scope.clone(),
                    &[],
                );
                self.pages.push(erabi_domain::DiscoveryPreviewPage {
                    requested_url: entry.requested_url,
                    final_url: None,
                    canonical_url: Some(entry.canonical_url),
                    depth: entry.depth,
                    state: PreviewUrlState::RobotsExcluded,
                    seed_ids: entry.seed_ids,
                    scope,
                    page_type_match: None,
                    downloaded_bytes: None,
                    robots_reason: Some(reason),
                    diagnostic: None,
                    budget_hits: Vec::new(),
                });
            }
            DiscoveryPreviewProviderOutcome::PageFailed { diagnostic } => {
                validate_diagnostic(&diagnostic)?;
                self.provider_errors = self.provider_errors.saturating_add(1);
                let scope = url::Url::parse(&entry.canonical_url)
                    .ok()
                    .map(|url| self.scope(&url))
                    .transpose()?
                    .flatten();
                self.update_seed_outcome(
                    &entry.seed_ids,
                    PreviewUrlState::ProviderError,
                    scope.clone(),
                    &[],
                );
                self.pages.push(erabi_domain::DiscoveryPreviewPage {
                    requested_url: entry.requested_url,
                    final_url: None,
                    canonical_url: Some(entry.canonical_url),
                    depth: entry.depth,
                    state: PreviewUrlState::ProviderError,
                    seed_ids: entry.seed_ids,
                    scope,
                    page_type_match: None,
                    downloaded_bytes: None,
                    robots_reason: None,
                    diagnostic: Some(diagnostic),
                    budget_hits: Vec::new(),
                });
            }
            DiscoveryPreviewProviderOutcome::Observed {
                observation,
                downloaded_bytes,
            } => {
                self.process_observation(entry, observation, downloaded_bytes)?;
            }
        }
        Ok(())
    }

    fn process_observation(
        &mut self,
        entry: QueueEntry,
        observation: PageObservation,
        downloaded_bytes: u64,
    ) -> Result<(), DiscoveryPreviewError> {
        if observation.requested_url != entry.requested_url {
            return Err(DiscoveryPreviewError::ProviderObservationRequestMismatch);
        }
        let remaining = self
            .context
            .limits
            .max_downloaded_bytes
            .checked_sub(self.consumed_bytes)
            .ok_or(DiscoveryPreviewError::BudgetOverflow)?;
        if downloaded_bytes > remaining
            || observation.discovered_links.len() > MAX_PREVIEW_LINKS_PER_OBSERVATION
        {
            return Err(DiscoveryPreviewError::ProviderObservationInvalid);
        }
        validate_observation(&observation)?;
        self.consumed_bytes = self
            .consumed_bytes
            .checked_add(downloaded_bytes)
            .ok_or(DiscoveryPreviewError::BudgetOverflow)?;
        let final_original = observation
            .final_url
            .as_deref()
            .unwrap_or(&observation.requested_url);
        let final_canonical = self
            .context
            .snapshot
            .version
            .canonicalization_policy()
            .canonicalize(final_original)
            .map_err(|_| DiscoveryPreviewError::ProviderObservationInvalid)?;
        let final_url_string = observation.final_url.clone();
        let canonical_url = final_canonical.canonical_url.to_string();
        let final_was_seen = !self.seen.insert(canonical_url.clone());
        if final_was_seen && canonical_url != entry.canonical_url {
            self.duplicates_prevented = self.duplicates_prevented.saturating_add(1);
        }
        if canonical_url != entry.canonical_url {
            self.queued.remove(&canonical_url);
            self.rebuild_queue();
        }
        let scope = self.scope(&final_canonical.canonical_url)?;
        let page_match = if is_in_scope(scope.as_ref()) {
            let decision =
                resolve_page_type(&final_canonical.canonical_url, &self.context.page_types);
            let evidence = PageTypeMatchEvidence::from_decision(&decision);
            self.register_match(&canonical_url, &evidence);
            Some(evidence)
        } else {
            None
        };
        let mut page_budget_hits = Vec::new();
        if let Some(page_match) = &page_match
            && page_match.decision == PageTypeMatchStatus::Matched
        {
            if let Some(page_type_id) = page_match.winner.as_ref().map(|winner| winner.page_type_id)
            {
                let count = self
                    .page_type_sampled
                    .get(&page_type_id.to_string())
                    .copied()
                    .unwrap_or(0);
                if self
                    .context
                    .snapshot
                    .version
                    .guardrails()
                    .page_type(page_type_id)
                    .and_then(|budget| budget.page_budget)
                    .is_some_and(|limit| count >= limit)
                {
                    let hit = PreviewBudgetHit {
                        kind: PreviewBudgetKind::PageTypePageBudget,
                        transition_id: None,
                        page_type_id: Some(page_type_id),
                        observed: count,
                        limit: self
                            .context
                            .snapshot
                            .version
                            .guardrails()
                            .page_type(page_type_id)
                            .and_then(|budget| budget.page_budget)
                            .unwrap_or(count),
                    };
                    self.record_budget(hit.clone());
                    page_budget_hits.push(hit);
                }
                *self
                    .page_type_sampled
                    .entry(page_type_id.to_string())
                    .or_default() += 1;
                *self
                    .page_type_scheduled
                    .entry(page_type_id.to_string())
                    .or_default() += 1;
            }
        }
        self.pages_sampled = self.pages_sampled.saturating_add(1);
        self.sampled.insert(canonical_url.clone());
        let page_state = match scope.as_ref().map(|item| item.classification) {
            Some(erabi_domain::DomainScopeStatus::External) => PreviewUrlState::External,
            Some(erabi_domain::DomainScopeStatus::Blocked) => PreviewUrlState::Blocked,
            _ => match page_match.as_ref().map(|item| item.decision) {
                Some(PageTypeMatchStatus::Ambiguous) => PreviewUrlState::AmbiguousPageType,
                Some(PageTypeMatchStatus::Unmatched) | None => PreviewUrlState::Unmatched,
                Some(PageTypeMatchStatus::Matched) => PreviewUrlState::Sampled,
            },
        };
        self.count_scope(page_state);
        for seed in &mut self.seeds {
            if entry.seed_ids.contains(&seed.seed_id) {
                seed.page_type_match.clone_from(&page_match);
                seed.state = page_state;
                seed.budget_hits.clone_from(&page_budget_hits);
            }
        }
        self.pages.push(erabi_domain::DiscoveryPreviewPage {
            requested_url: entry.requested_url.clone(),
            final_url: final_url_string,
            canonical_url: Some(canonical_url.clone()),
            depth: entry.depth,
            state: page_state,
            seed_ids: entry.seed_ids.clone(),
            scope: scope.clone(),
            page_type_match: page_match.clone(),
            downloaded_bytes: Some(downloaded_bytes),
            robots_reason: None,
            diagnostic: None,
            budget_hits: page_budget_hits.clone(),
        });
        if self.elapsed_millis() >= self.context.limits.max_duration_ms {
            self.hit_time_budget();
            self.urls_discovered = self
                .urls_discovered
                .saturating_add(observation.discovered_links.len() as u64);
            self.push_diagnostic(PreviewDiagnostic {
                code: "PREVIEW_TIME_BUDGET_LINKS_NOT_EXPANDED".to_owned(),
                message: "Links from the completed observation were counted but not expanded after the time cap.".to_owned(),
                observed: Some(observation.discovered_links.len() as u64),
                threshold: Some(0),
            });
        } else if page_budget_hits.is_empty()
            && is_in_scope(scope.as_ref())
            && page_match
                .as_ref()
                .is_some_and(|item| item.decision == PageTypeMatchStatus::Matched)
            && !self.expanded.contains(&canonical_url)
        {
            if let Some(source_match) = page_match.as_ref() {
                self.expand_page(&entry, &observation, &canonical_url, source_match)?;
            }
        }
        Ok(())
    }

    fn expand_page(
        &mut self,
        entry: &QueueEntry,
        observation: &PageObservation,
        source_canonical_url: &str,
        source_match: &PageTypeMatchEvidence,
    ) -> Result<(), DiscoveryPreviewError> {
        if !self.expanded.insert(source_canonical_url.to_owned()) {
            return Ok(());
        }
        let Some(source_page_type_id) = source_match
            .winner
            .as_ref()
            .map(|winner| winner.page_type_id)
        else {
            return Ok(());
        };
        let base = observation
            .final_url
            .as_deref()
            .unwrap_or(observation.requested_url.as_str());
        let base_url =
            url::Url::parse(base).map_err(|_| DiscoveryPreviewError::ProviderObservationInvalid)?;
        let mut links = observation.discovered_links.clone();
        links.sort_by(|left, right| {
            left.raw_href
                .cmp(&right.raw_href)
                .then(left.selector.cmp(&right.selector))
        });
        let mut new_from_page = 0_u64;
        let mut provenance_truncated = false;
        for link in links {
            self.urls_discovered = self.urls_discovered.saturating_add(1);
            if self.paths.len() >= MAX_PREVIEW_PROVENANCE_EDGES {
                if !provenance_truncated {
                    self.record_budget(PreviewBudgetHit {
                        kind: PreviewBudgetKind::ProvenanceRetention,
                        transition_id: None,
                        page_type_id: None,
                        observed: self.paths.len() as u64,
                        limit: MAX_PREVIEW_PROVENANCE_EDGES as u64,
                    });
                    self.push_diagnostic(PreviewDiagnostic {
                        code: "PREVIEW_PROVENANCE_TRUNCATED".to_owned(),
                        message: "The bounded provenance retention cap was reached; raw href counts continue to include omitted edges.".to_owned(),
                        observed: Some(self.paths.len() as u64),
                        threshold: Some(MAX_PREVIEW_PROVENANCE_EDGES as u64),
                    });
                    provenance_truncated = true;
                }
                continue;
            }
            let resolved = base_url.join(&link.raw_href).ok();
            let Some(resolved_url) = resolved else {
                self.paths.push(self.invalid_path(
                    entry,
                    source_match,
                    &link,
                    observation.final_url.as_deref(),
                ));
                continue;
            };
            if resolved_url.to_string().chars().count() > MAX_PREVIEW_URL_CHARS {
                self.paths.push(self.invalid_path(
                    entry,
                    source_match,
                    &link,
                    observation.final_url.as_deref(),
                ));
                continue;
            }
            let canonicalization = self
                .context
                .snapshot
                .version
                .canonicalization_policy()
                .canonicalize(resolved_url.as_str())
                .ok();
            let Some(canonicalization_result) = canonicalization else {
                self.paths.push(self.invalid_path(
                    entry,
                    source_match,
                    &link,
                    observation.final_url.as_deref(),
                ));
                continue;
            };
            let canonical_url = canonicalization_result.canonical_url.to_string();
            let scope = self.scope(&canonicalization_result.canonical_url)?;
            let unique = self.seen.insert(canonical_url.clone());
            if !is_in_scope(scope.as_ref()) {
                if !unique {
                    self.duplicates_prevented = self.duplicates_prevented.saturating_add(1);
                }
                let state = if unique {
                    scope_state(scope.as_ref())
                } else {
                    PreviewUrlState::CanonicalDuplicate
                };
                if unique {
                    self.count_scope(state);
                }
                let mut path = self.path_base(
                    entry,
                    source_match,
                    &link,
                    Some(resolved_url.to_string()),
                    Some(canonical_url),
                    Some(canonicalization_result),
                    scope,
                    state,
                    None,
                    None,
                    Vec::new(),
                    Vec::new(),
                );
                path.source_final_url = observation.final_url.clone();
                self.paths.push(path);
                continue;
            }
            if !unique {
                self.duplicates_prevented = self.duplicates_prevented.saturating_add(1);
                let prospective_depth = if self.sampled.contains(&canonical_url) {
                    None
                } else {
                    let depth = self.queued_duplicate_prospective_depth(
                        entry,
                        source_match,
                        &link,
                        &canonical_url,
                    )?;
                    self.merge_queued_duplicate_provenance(&canonical_url, &entry.seed_ids, depth);
                    depth
                };
                let mut path = self.path_base(
                    entry,
                    source_match,
                    &link,
                    Some(resolved_url.to_string()),
                    Some(canonical_url.clone()),
                    Some(canonicalization_result),
                    scope,
                    PreviewUrlState::CanonicalDuplicate,
                    None,
                    Some(canonical_url),
                    Vec::new(),
                    Vec::new(),
                );
                path.source_final_url = observation.final_url.clone();
                path.prospective_depth = prospective_depth;
                self.paths.push(path);
                continue;
            }
            let target_match = PageTypeMatchEvidence::from_decision(&resolve_page_type(
                &canonicalization_result.canonical_url,
                &self.context.page_types,
            ));
            self.register_match(&canonical_url, &target_match);
            let mut evaluations = Vec::new();
            let mut eligible = Vec::new();
            for transition in self.sorted_transitions_for(source_page_type_id) {
                let selector_eligible =
                    link.selector.as_deref() == Some(transition.link_selector.as_str());
                let target_page_type_eligible = target_match.decision
                    == PageTypeMatchStatus::Matched
                    && target_match.winner.as_ref().is_some_and(|winner| {
                        winner.page_type_id == transition.target_page_type_id
                    });
                let constraints_eligible = transition.url_constraints.is_none();
                let mut budget_hits = Vec::new();
                let mut diagnostic = None;
                if !constraints_eligible {
                    diagnostic = Some(PreviewDiagnostic {
                        code: "TRANSITION_URL_CONSTRAINT_UNEVALUATED".to_owned(),
                        message:
                            "The transition URL constraint has no executable DSL in this Preview."
                                .to_owned(),
                        observed: None,
                        threshold: None,
                    });
                }
                let semantic_eligible =
                    selector_eligible && target_page_type_eligible && constraints_eligible;
                let mut is_eligible = semantic_eligible;
                if semantic_eligible {
                    if let Some(hit) = self.check_transition_budget(
                        entry,
                        source_canonical_url,
                        &transition,
                        &target_match,
                    )? {
                        is_eligible = false;
                        budget_hits.push(hit);
                    }
                }
                if is_eligible {
                    let prospective_depth = entry
                        .depth
                        .checked_add(transition.budget.depth_contribution)
                        .ok_or(DiscoveryPreviewError::BudgetOverflow)?;
                    if prospective_depth > self.context.limits.max_depth {
                        let hit = PreviewBudgetHit {
                            kind: PreviewBudgetKind::MaxDepth,
                            transition_id: Some(transition.id),
                            page_type_id: Some(transition.target_page_type_id),
                            observed: prospective_depth as u64,
                            limit: self.context.limits.max_depth as u64,
                        };
                        self.record_budget(hit.clone());
                        budget_hits.push(hit);
                        is_eligible = false;
                    }
                }
                if is_eligible {
                    self.consume_transition(&transition, source_canonical_url);
                    eligible.push((
                        transition.clone(),
                        entry
                            .depth
                            .checked_add(transition.budget.depth_contribution)
                            .ok_or(DiscoveryPreviewError::BudgetOverflow)?,
                    ));
                }
                evaluations.push(PreviewTransitionEvaluation {
                    transition_id: transition.id,
                    transition_name: transition.name.clone(),
                    source_page_type_id: transition.source_page_type_id,
                    target_page_type_id: transition.target_page_type_id,
                    priority: transition.priority,
                    selector_eligible,
                    target_page_type_eligible,
                    constraints_eligible,
                    eligible: is_eligible,
                    budget_hits,
                    diagnostic,
                });
            }
            let state = match target_match.decision {
                PageTypeMatchStatus::Ambiguous => PreviewUrlState::AmbiguousPageType,
                PageTypeMatchStatus::Unmatched => PreviewUrlState::Unmatched,
                PageTypeMatchStatus::Matched
                    if eligible.is_empty()
                        && evaluations
                            .iter()
                            .any(|evaluation| !evaluation.budget_hits.is_empty()) =>
                {
                    PreviewUrlState::BudgetExcluded
                }
                PageTypeMatchStatus::Matched => PreviewUrlState::InScopeMatched,
            };
            let prospective_depth = eligible.iter().map(|(_, depth)| *depth).min();
            let mut path = self.path_base(
                entry,
                source_match,
                &link,
                Some(resolved_url.to_string()),
                Some(canonical_url.clone()),
                Some(canonicalization_result),
                scope,
                state,
                Some(target_match.clone()),
                None,
                evaluations,
                Vec::new(),
            );
            path.source_final_url = observation.final_url.clone();
            path.prospective_depth = prospective_depth;
            self.paths.push(path);
            if let Some((_, depth)) = eligible.iter().min_by(|left, right| left.1.cmp(&right.1)) {
                let target_page_type_id = target_match
                    .winner
                    .as_ref()
                    .map(|winner| winner.page_type_id);
                if let Some(winner) = target_match.winner.as_ref() {
                    let page_count = self
                        .page_type_scheduled
                        .get(&winner.page_type_id.to_string())
                        .copied()
                        .unwrap_or(0);
                    let configured_page_limit = self
                        .context
                        .snapshot
                        .version
                        .guardrails()
                        .page_type(winner.page_type_id)
                        .and_then(|budget| budget.page_budget);
                    let allowed_by_page_budget =
                        configured_page_limit.is_none_or(|limit| page_count < limit);
                    if !allowed_by_page_budget {
                        self.record_budget(PreviewBudgetHit {
                            kind: PreviewBudgetKind::PageTypePageBudget,
                            transition_id: None,
                            page_type_id: Some(winner.page_type_id),
                            observed: page_count,
                            limit: configured_page_limit.unwrap_or(page_count),
                        });
                        continue;
                    }
                }
                if self.admitted.len() as u64 >= self.context.limits.max_pages {
                    self.record_budget(PreviewBudgetHit {
                        kind: PreviewBudgetKind::MaxPages,
                        transition_id: None,
                        page_type_id: None,
                        observed: self.admitted.len() as u64,
                        limit: self.context.limits.max_pages,
                    });
                    continue;
                }
                self.admitted.insert(canonical_url.clone());
                if let Some(winner) = target_match.winner.as_ref() {
                    *self
                        .page_type_scheduled
                        .entry(winner.page_type_id.to_string())
                        .or_default() += 1;
                }
                let order = self.newly_enqueued_urls as usize + self.selected_seed_ids.len();
                self.queued.insert(
                    canonical_url.clone(),
                    QueueEntry {
                        requested_url: canonical_url.clone(),
                        canonical_url,
                        depth: *depth,
                        seed_ids: entry.seed_ids.clone(),
                        target_page_type_id,
                        order,
                    },
                );
                self.newly_enqueued_urls = self.newly_enqueued_urls.saturating_add(1);
                new_from_page = new_from_page.saturating_add(1);
            }
        }
        self.peak_new_from_page = self.peak_new_from_page.max(new_from_page);
        self.rebuild_queue();
        Ok(())
    }

    fn sorted_transitions_for(&self, source: erabi_domain::PageTypeId) -> Vec<DiscoveryTransition> {
        let mut transitions = self
            .context
            .transitions
            .iter()
            .filter(|transition| transition.enabled && transition.source_page_type_id == source)
            .cloned()
            .collect::<Vec<_>>();
        transitions.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then(left.name.cmp(&right.name))
                .then(left.id.to_string().cmp(&right.id.to_string()))
        });
        transitions
    }

    /// Computes only the queue-placement depth for an already admitted
    /// canonical identity. This deliberately does not resolve the target
    /// again, evaluate a transition budget, or increment transition counters:
    /// those actions belong exclusively to the first unique discovery event.
    fn queued_duplicate_prospective_depth(
        &self,
        entry: &QueueEntry,
        source_match: &PageTypeMatchEvidence,
        link: &ObservedLink,
        canonical_url: &str,
    ) -> Result<Option<u32>, DiscoveryPreviewError> {
        let Some(target_page_type_id) = self
            .queued
            .get(canonical_url)
            .and_then(|queued| queued.target_page_type_id)
        else {
            return Ok(None);
        };
        let Some(source_page_type_id) = source_match
            .winner
            .as_ref()
            .map(|winner| winner.page_type_id)
        else {
            return Ok(None);
        };
        let mut minimum = None;
        for transition in self.context.transitions.iter().filter(|transition| {
            transition.enabled
                && transition.source_page_type_id == source_page_type_id
                && transition.target_page_type_id == target_page_type_id
                && transition.url_constraints.is_none()
                && link.selector.as_deref() == Some(transition.link_selector.as_str())
        }) {
            let prospective = entry
                .depth
                .checked_add(transition.budget.depth_contribution)
                .ok_or(DiscoveryPreviewError::BudgetOverflow)?;
            minimum = Some(minimum.map_or(prospective, |current: u32| current.min(prospective)));
        }
        Ok(minimum)
    }

    /// Keeps every selected-root provenance link on the one queued identity
    /// and applies the minimum known runtime depth without reserving another
    /// provider-work slot.
    fn merge_queued_duplicate_provenance(
        &mut self,
        canonical_url: &str,
        seed_ids: &[SeedId],
        prospective_depth: Option<u32>,
    ) {
        let mut changed = false;
        if let Some(queued) = self.queued.get_mut(canonical_url) {
            for seed_id in seed_ids {
                if !queued.seed_ids.contains(seed_id) {
                    queued.seed_ids.push(*seed_id);
                    changed = true;
                }
            }
            if let Some(depth) = prospective_depth
                && depth < queued.depth
            {
                queued.depth = depth;
                changed = true;
            }
        }
        if changed {
            self.rebuild_queue();
        }
    }

    fn check_transition_budget(
        &mut self,
        entry: &QueueEntry,
        source_canonical_url: &str,
        transition: &DiscoveryTransition,
        target_match: &PageTypeMatchEvidence,
    ) -> Result<Option<PreviewBudgetHit>, DiscoveryPreviewError> {
        let key = transition.id.to_string();
        let source_key = (key.clone(), source_canonical_url.to_owned());
        let per_page = self
            .transition_page_counts
            .get(&source_key)
            .copied()
            .unwrap_or(0);
        let total = self
            .transition_counts
            .get(&key)
            .map_or(0, |count| count.eligible_edges);
        let page_count = target_match
            .winner
            .as_ref()
            .and_then(|winner| {
                self.page_type_scheduled
                    .get(&winner.page_type_id.to_string())
                    .copied()
            })
            .unwrap_or(0);
        if self.admitted.len() as u64 >= self.context.limits.max_pages {
            return Ok(Some(self.budget_hit(
                PreviewBudgetKind::MaxPages,
                Some(transition.id),
                Some(transition.target_page_type_id),
                self.admitted.len() as u64,
                self.context.limits.max_pages,
            )));
        }
        let candidate = DiscoveryBudgetCandidate {
            pages_already_scheduled: self.admitted.len() as u64,
            current_depth: entry.depth,
            elapsed_duration_seconds: self.elapsed_millis() / 1_000,
            downloaded_bytes: self.consumed_bytes,
            page_type_pages: page_count,
            transition_links_on_source_page: per_page,
            transition_total_links: total,
            prospective_download_bytes: 0,
        };
        let semantic = DiscoveryBudgetEvaluator::new(
            self.context.snapshot.version.guardrails(),
            self.context
                .snapshot
                .version
                .guardrails()
                .page_type(transition.target_page_type_id),
            Some(&transition.budget),
        )
        .evaluate(candidate)
        .map_err(|error| match error {
            erabi_domain::DiscoveryBudgetError::Overflow => DiscoveryPreviewError::BudgetOverflow,
            _ => DiscoveryPreviewError::PersistedStateInvalid,
        })?;
        if let DiscoveryBudgetDecision::Excluded(exclusion) = semantic {
            return Ok(Some(
                self.budget_hit_from_exclusion(exclusion, transition, candidate),
            ));
        }
        if total >= self.context.transition_total_limit(transition.id) {
            return Ok(Some(self.budget_hit(
                PreviewBudgetKind::TransitionTotal,
                Some(transition.id),
                None,
                total,
                self.context.transition_total_limit(transition.id),
            )));
        }
        Ok(None)
    }

    fn consume_transition(&mut self, transition: &DiscoveryTransition, source: &str) {
        let key = transition.id.to_string();
        let source_key = (key.clone(), source.to_owned());
        *self.transition_page_counts.entry(source_key).or_default() += 1;
        if let Some(count) = self.transition_counts.get_mut(&key) {
            count.eligible_edges = count.eligible_edges.saturating_add(1);
            count.source_pages.insert(source.to_owned());
        }
    }

    fn register_match(&mut self, url: &str, evidence: &PageTypeMatchEvidence) {
        if self.in_scope_urls.insert(url.to_owned()) {
            match evidence.decision {
                PageTypeMatchStatus::Matched => {
                    self.matching_urls.insert(url.to_owned());
                    if let Some(winner) = evidence.winner.as_ref() {
                        *self
                            .page_type_discovered
                            .entry(winner.page_type_id.to_string())
                            .or_default() += 1;
                    }
                }
                PageTypeMatchStatus::Ambiguous => {
                    self.matching_urls.insert(url.to_owned());
                    self.ambiguous_urls.insert(url.to_owned());
                }
                PageTypeMatchStatus::Unmatched => {
                    self.matching_urls.insert(url.to_owned());
                    self.unmatched_urls.insert(url.to_owned());
                }
            }
        }
    }

    fn update_seed_outcome(
        &mut self,
        seed_ids: &[SeedId],
        state: PreviewUrlState,
        scope: Option<DomainScopeEvidence>,
        budget_hits: &[PreviewBudgetHit],
    ) {
        for seed in &mut self.seeds {
            if seed_ids.contains(&seed.seed_id) {
                seed.state = state;
                seed.scope.clone_from(&scope);
                seed.budget_hits = budget_hits.to_vec();
            }
        }
    }

    fn scope(&self, url: &url::Url) -> Result<Option<DomainScopeEvidence>, DiscoveryPreviewError> {
        self.context
            .snapshot
            .version
            .domain_scope()
            .classify(url, self.context.snapshot.version.seeds())
            .map(|classification| Some(DomainScopeEvidence::from_classification(&classification)))
            .map_err(|_| DiscoveryPreviewError::PersistedStateInvalid)
    }

    fn invalid_path(
        &self,
        entry: &QueueEntry,
        source_match: &PageTypeMatchEvidence,
        link: &ObservedLink,
        source_final_url: Option<&str>,
    ) -> DiscoveryPath {
        let mut path = self.path_base(
            entry,
            source_match,
            link,
            None,
            None,
            None,
            None,
            PreviewUrlState::InvalidUrl,
            None,
            None,
            Vec::new(),
            Vec::new(),
        );
        path.source_final_url = source_final_url.map(ToOwned::to_owned);
        path
    }

    #[allow(clippy::too_many_arguments)]
    fn path_base(
        &self,
        entry: &QueueEntry,
        source_match: &PageTypeMatchEvidence,
        link: &ObservedLink,
        resolved: Option<String>,
        canonical: Option<String>,
        canonicalization: Option<erabi_domain::CanonicalizationResult>,
        scope: Option<DomainScopeEvidence>,
        state: PreviewUrlState,
        target_match: Option<PageTypeMatchEvidence>,
        duplicate: Option<String>,
        evaluations: Vec<PreviewTransitionEvaluation>,
        budget_hits: Vec<PreviewBudgetHit>,
    ) -> DiscoveryPath {
        DiscoveryPath {
            seed_id: entry.seed_ids.first().copied().unwrap_or_else(SeedId::new),
            seed_ids: entry.seed_ids.clone(),
            source_requested_url: entry.requested_url.clone(),
            source_final_url: None,
            source_canonical_url: entry.canonical_url.clone(),
            source_page_type_match: source_match.clone(),
            selector: link.selector.clone(),
            raw_href: link.raw_href.clone(),
            resolved_original_url: resolved,
            canonical_url: canonical,
            canonicalization: canonicalization.map(canonicalization_evidence),
            scope,
            state,
            duplicate_of_canonical_url: duplicate,
            target_page_type_match: target_match,
            source_depth: entry.depth,
            prospective_depth: None,
            transition_evaluations: evaluations,
            budget_hits,
        }
    }

    fn budget_hit(
        &mut self,
        kind: PreviewBudgetKind,
        transition_id: Option<DiscoveryTransitionId>,
        page_type_id: Option<erabi_domain::PageTypeId>,
        observed: u64,
        limit: u64,
    ) -> PreviewBudgetHit {
        let hit = PreviewBudgetHit {
            kind,
            transition_id,
            page_type_id,
            observed,
            limit,
        };
        self.record_budget(hit.clone());
        hit
    }

    fn budget_hit_from_exclusion(
        &mut self,
        exclusion: DiscoveryBudgetExclusion,
        transition: &DiscoveryTransition,
        candidate: DiscoveryBudgetCandidate,
    ) -> PreviewBudgetHit {
        let (kind, observed, limit) = match exclusion {
            DiscoveryBudgetExclusion::MaxPages => (
                PreviewBudgetKind::MaxPages,
                candidate.pages_already_scheduled,
                self.context.limits.max_pages,
            ),
            DiscoveryBudgetExclusion::MaxDuration => (
                PreviewBudgetKind::MaxDuration,
                candidate.elapsed_duration_seconds,
                self.context
                    .snapshot
                    .version
                    .guardrails()
                    .max_duration_seconds,
            ),
            DiscoveryBudgetExclusion::MaxDepth => (
                PreviewBudgetKind::MaxDepth,
                candidate
                    .current_depth
                    .saturating_add(transition.budget.depth_contribution) as u64,
                self.context.limits.max_depth as u64,
            ),
            DiscoveryBudgetExclusion::MaxDownloadedBytes => (
                PreviewBudgetKind::MaxDownloadedBytes,
                candidate.downloaded_bytes,
                self.context.limits.max_downloaded_bytes,
            ),
            DiscoveryBudgetExclusion::PageTypePageBudget => (
                PreviewBudgetKind::PageTypePageBudget,
                candidate.page_type_pages,
                self.context
                    .snapshot
                    .version
                    .guardrails()
                    .page_type(transition.target_page_type_id)
                    .and_then(|budget| budget.page_budget)
                    .unwrap_or(candidate.page_type_pages),
            ),
            DiscoveryBudgetExclusion::TransitionPerPageLinkLimit => (
                PreviewBudgetKind::TransitionPerSourcePage,
                candidate.transition_links_on_source_page as u64,
                transition.budget.max_links_per_source_page as u64,
            ),
            DiscoveryBudgetExclusion::TransitionTotalBudget => (
                PreviewBudgetKind::TransitionTotal,
                candidate.transition_total_links,
                transition
                    .budget
                    .total_budget
                    .unwrap_or(candidate.transition_total_links),
            ),
        };
        self.budget_hit(
            kind,
            Some(transition.id),
            Some(transition.target_page_type_id),
            observed,
            limit,
        )
    }

    fn record_budget(&mut self, hit: PreviewBudgetHit) {
        *self.budget_hits.entry(hit.kind).or_default() += 1;
    }

    fn push_diagnostic(&mut self, diagnostic: PreviewDiagnostic) {
        if self.diagnostics.len() < MAX_PREVIEW_DIAGNOSTICS {
            self.diagnostics.push(diagnostic);
        } else {
            *self
                .budget_hits
                .entry(PreviewBudgetKind::DiagnosticRetention)
                .or_default() += 1;
        }
    }

    fn hit_time_budget(&mut self) {
        self.time_budget_hit = true;
        self.record_budget(PreviewBudgetHit {
            kind: PreviewBudgetKind::MaxDuration,
            transition_id: None,
            page_type_id: None,
            observed: self.elapsed_millis(),
            limit: self.context.limits.max_duration_ms,
        });
    }

    fn elapsed_millis(&self) -> u64 {
        self.clock.now_millis().saturating_sub(self.start_millis)
    }

    fn finish(mut self) -> DiscoveryPreviewResult {
        if self.time_budget_hit {
            self.push_diagnostic(PreviewDiagnostic {
                code: "PREVIEW_TIME_BUDGET_HIT".to_owned(),
                message: "Preview stopped at a safe clock boundary.".to_owned(),
                observed: Some(self.elapsed_millis()),
                threshold: Some(self.context.limits.max_duration_ms),
            });
        }
        let frontier_remaining = self.queue.len() as u64;
        let transition_counts = self
            .transition_counts
            .values()
            .map(|count| PreviewTransitionCount {
                transition_id: count.transition_id,
                transition_name: count.name.clone(),
                eligible_edges: count.eligible_edges,
                source_pages_with_eligible_edges: count.source_pages.len() as u64,
            })
            .collect::<Vec<_>>();
        let distributions = self
            .context
            .page_types
            .iter()
            .map(|page_type| PreviewPageTypeDistribution {
                page_type_id: page_type.id,
                page_type_name: page_type.name.clone(),
                discovered_unique_urls: self
                    .page_type_discovered
                    .get(&page_type.id.to_string())
                    .copied()
                    .unwrap_or(0),
                sampled_pages: self
                    .page_type_sampled
                    .get(&page_type.id.to_string())
                    .copied()
                    .unwrap_or(0),
            })
            .collect::<Vec<_>>();
        let total_edges = transition_counts
            .iter()
            .map(|count| count.eligible_edges)
            .sum::<u64>();
        let dominant = unique_dominant_transition(&transition_counts);
        let dominant_share = if total_edges > 0 {
            dominant.map(|count| count.eligible_edges.saturating_mul(100) / total_edges)
        } else {
            None
        };
        let query_variant_groups = query_variant_groups(&self.in_scope_urls);
        let indicators = PreviewGrowthIndicators {
            peak_new_canonical_urls_from_one_page: self.peak_new_from_page,
            total_newly_enqueued_urls: self.newly_enqueued_urls,
            frontier_remaining,
            dominant_transition_id: dominant.map(|count| count.transition_id),
            dominant_transition_eligible_edges: dominant.map_or(0, |count| count.eligible_edges),
            total_eligible_transition_edges: total_edges,
            dominant_transition_share_percent: dominant_share,
            query_variant_groups: query_variant_groups.clone(),
            unmatched_denominator: self.matching_urls.len() as u64,
            ambiguity_denominator: self.matching_urls.len() as u64,
        };
        let growth_warnings = growth_warnings(
            &self,
            &transition_counts,
            total_edges,
            dominant,
            dominant_share,
            &query_variant_groups,
            frontier_remaining,
        );
        let mut budget_hit_counts = self.budget_hits;
        if self.time_budget_hit {
            budget_hit_counts
                .entry(PreviewBudgetKind::MaxDuration)
                .or_insert(1);
        }
        DiscoveryPreviewResult {
            result_semantics: DiscoveryPreviewResultSemantics::PreviewOnly,
            crawler_version_id: self.context.snapshot.version.id(),
            config_hash: self.context.snapshot.config_hash,
            selected_seed_ids: self.selected_seed_ids,
            effective_limits: self.context.limits,
            seeds: self.seeds,
            pages: self.pages,
            discovery_paths: self.paths,
            summary: DiscoveryPreviewSummary {
                pages_sampled: self.pages_sampled,
                urls_discovered: self.urls_discovered,
                canonical_unique_urls: self.seen.len() as u64,
                duplicates_prevented: self.duplicates_prevented,
                page_type_distribution: distributions,
                ambiguous_urls: self.ambiguous_urls.len() as u64,
                unmatched_urls: self.unmatched_urls.len() as u64,
                external_urls: self.external_urls,
                blocked_urls: self.blocked_urls,
                robots_excluded: self.robots_excluded,
                provider_errors: self.provider_errors,
                transition_counts,
                budget_hit_counts,
                frontier_remaining,
                newly_enqueued_urls: self.newly_enqueued_urls,
            },
            growth_indicators: indicators,
            growth_warnings,
            warnings: self.diagnostics,
        }
    }
}

fn canonicalization_evidence(
    result: erabi_domain::CanonicalizationResult,
) -> CanonicalizationEvidence {
    CanonicalizationEvidence {
        original_url: result.original_url,
        canonical_url: Some(result.canonical_url.to_string()),
        outcome: CanonicalizationOutcome::Canonicalized,
        decisions: result
            .decisions
            .into_iter()
            .map(|decision| {
                let (code, parameter) = match decision {
                    CanonicalizationDecision::SchemeNormalized => {
                        (CanonicalizationDecisionCode::SchemeNormalized, None)
                    }
                    CanonicalizationDecision::HostNormalized => {
                        (CanonicalizationDecisionCode::HostNormalized, None)
                    }
                    CanonicalizationDecision::DefaultPortRemoved => {
                        (CanonicalizationDecisionCode::DefaultPortRemoved, None)
                    }
                    CanonicalizationDecision::FragmentRemoved => {
                        (CanonicalizationDecisionCode::FragmentRemoved, None)
                    }
                    CanonicalizationDecision::PathNormalized => {
                        (CanonicalizationDecisionCode::PathNormalized, None)
                    }
                    CanonicalizationDecision::QuerySorted => {
                        (CanonicalizationDecisionCode::QuerySorted, None)
                    }
                    CanonicalizationDecision::TrackingParameterRemoved { parameter } => (
                        CanonicalizationDecisionCode::TrackingParameterRemoved,
                        Some(parameter),
                    ),
                    CanonicalizationDecision::CustomParameterDropped { parameter } => (
                        CanonicalizationDecisionCode::CustomParameterDropped,
                        Some(parameter),
                    ),
                    CanonicalizationDecision::ExplicitParameterKept { parameter } => (
                        CanonicalizationDecisionCode::ExplicitParameterKept,
                        Some(parameter),
                    ),
                };
                erabi_domain::CanonicalizationDecisionEvidence { code, parameter }
            })
            .collect(),
    }
}

fn validate_observation(observation: &PageObservation) -> Result<(), DiscoveryPreviewError> {
    validate_safe_text(&observation.requested_url, MAX_PREVIEW_URL_CHARS)?;
    if let Some(final_url) = &observation.final_url {
        validate_safe_text(final_url, MAX_PREVIEW_URL_CHARS)?;
    }
    for link in &observation.discovered_links {
        validate_safe_text(&link.raw_href, MAX_PREVIEW_URL_CHARS)?;
        if let Some(selector) = &link.selector {
            validate_safe_text(selector, 1_024)?;
        }
    }
    Ok(())
}

fn validate_diagnostic(diagnostic: &TestDiagnostic) -> Result<(), DiscoveryPreviewError> {
    validate_safe_text(&diagnostic.code, 128)?;
    validate_safe_text(&diagnostic.message, MAX_PROVIDER_DIAGNOSTIC_CHARS)
}

fn validate_safe_text(value: &str, max: usize) -> Result<(), DiscoveryPreviewError> {
    if value.is_empty() || value.chars().count() > max || value.chars().any(char::is_control) {
        return Err(DiscoveryPreviewError::ProviderObservationInvalid);
    }
    Ok(())
}

fn is_in_scope(scope: Option<&DomainScopeEvidence>) -> bool {
    scope.is_some_and(|scope| scope.classification == erabi_domain::DomainScopeStatus::InScope)
}

fn scope_state(scope: Option<&DomainScopeEvidence>) -> PreviewUrlState {
    match scope.map(|scope| scope.classification) {
        Some(erabi_domain::DomainScopeStatus::External) => PreviewUrlState::External,
        Some(erabi_domain::DomainScopeStatus::Blocked) => PreviewUrlState::Blocked,
        _ => PreviewUrlState::InScopeMatched,
    }
}

fn map_provider_error(error: DiscoveryPreviewProviderError) -> DiscoveryPreviewError {
    match error {
        DiscoveryPreviewProviderError::Unavailable => DiscoveryPreviewError::ProviderUnavailable,
    }
}

fn map_crawler_error(error: CrawlerRepositoryError) -> DiscoveryPreviewError {
    match error {
        CrawlerRepositoryError::CrawlerNotFound => DiscoveryPreviewError::CrawlerNotFound,
        CrawlerRepositoryError::CrawlerVersionNotFound => {
            DiscoveryPreviewError::CrawlerVersionNotFound
        }
        CrawlerRepositoryError::VersionNotOwnedByCrawler => {
            DiscoveryPreviewError::VersionNotOwnedByCrawler
        }
        CrawlerRepositoryError::VersionNotDraft
        | CrawlerRepositoryError::PublishedVersionImmutable => {
            DiscoveryPreviewError::VersionNotDraft
        }
        CrawlerRepositoryError::VersionNotActiveDraft => {
            DiscoveryPreviewError::VersionNotActiveDraft
        }
        CrawlerRepositoryError::CorruptState
        | CrawlerRepositoryError::InvalidCanonicalizationPolicy
        | CrawlerRepositoryError::InvalidDomainScope
        | CrawlerRepositoryError::InvalidCrawlGuardrails
        | CrawlerRepositoryError::InvalidPageTypeBudget
        | CrawlerRepositoryError::InvalidTransitionBudget
        | CrawlerRepositoryError::InvalidDiscoveryTransition
        | CrawlerRepositoryError::TransitionSourcePageTypeNotFound
        | CrawlerRepositoryError::TransitionTargetPageTypeNotFound => {
            DiscoveryPreviewError::PersistedStateInvalid
        }
        _ => DiscoveryPreviewError::PersistedStateInvalid,
    }
}

fn is_cycle(
    graph: &TransitionGraph,
    source: erabi_domain::PageTypeId,
    target: erabi_domain::PageTypeId,
    visited: &mut BTreeSet<String>,
) -> bool {
    if source == target {
        return true;
    }
    if !visited.insert(source.to_string()) {
        return false;
    }
    graph
        .transitions()
        .iter()
        .filter(|transition| transition.enabled && transition.source_page_type_id == source)
        .any(|transition| is_cycle(graph, transition.target_page_type_id, target, visited))
}

/// A transition is dominant only when the observed edge counts identify one
/// non-zero maximum. Ties intentionally remain advisory-ambiguous; no stable
/// identifier or presentation order may fabricate a winner.
fn unique_dominant_transition(
    counts: &[PreviewTransitionCount],
) -> Option<&PreviewTransitionCount> {
    let maximum = counts.iter().map(|count| count.eligible_edges).max()?;
    if maximum == 0 {
        return None;
    }
    let mut tied = counts
        .iter()
        .filter(|count| count.eligible_edges == maximum);
    let dominant = tied.next()?;
    tied.next().is_none().then_some(dominant)
}

fn query_variant_groups(urls: &BTreeSet<String>) -> Vec<PreviewQueryVariantGroup> {
    #[derive(Default)]
    struct QueryGroupAccounting {
        total_identities: u64,
        query_bearing_identities: u64,
        variants: BTreeSet<String>,
    }

    let mut groups: BTreeMap<(String, String), QueryGroupAccounting> = BTreeMap::new();
    for value in urls {
        if let Ok(url) = url::Url::parse(value) {
            let group = groups
                .entry((
                    url.host_str().unwrap_or_default().to_ascii_lowercase(),
                    url.path().to_owned(),
                ))
                .or_default();
            group.total_identities = group.total_identities.saturating_add(1);
            if let Some(query) = url.query() {
                group.query_bearing_identities = group.query_bearing_identities.saturating_add(1);
                group.variants.insert(query.to_owned());
            }
        }
    }
    groups
        .into_iter()
        .filter_map(|((host, path), accounting)| {
            (accounting.variants.len() >= erabi_domain::QUERY_EXPLOSION_MIN_VARIANTS as usize)
                .then_some(PreviewQueryVariantGroup {
                    host,
                    path,
                    total_identities: accounting.total_identities,
                    query_bearing_identities: accounting.query_bearing_identities,
                    canonical_query_variants: accounting.variants.len() as u64,
                })
        })
        .collect()
}

fn growth_warnings(
    run: &PreviewRun,
    counts: &[PreviewTransitionCount],
    total: u64,
    dominant: Option<&PreviewTransitionCount>,
    share: Option<u64>,
    groups: &[PreviewQueryVariantGroup],
    frontier: u64,
) -> Vec<PreviewGrowthWarning> {
    let mut warnings = Vec::new();
    if let (Some(dominant), Some(share)) = (dominant, share) {
        let transition = run
            .context
            .transitions
            .iter()
            .find(|transition| transition.id == dominant.transition_id);
        if transition.is_some_and(|transition| {
            is_cycle(
                &run.context.graph,
                transition.source_page_type_id,
                transition.target_page_type_id,
                &mut BTreeSet::new(),
            )
        }) && dominant.eligible_edges >= erabi_domain::DOMINANT_TRANSITION_MIN_EDGES
            && share >= erabi_domain::DOMINANT_TRANSITION_SHARE_PERCENT
        {
            warnings.push(PreviewGrowthWarning {
                code: PreviewGrowthWarningCode::CyclicTransitionDominance,
                message: "A cyclic transition dominates eligible preview edges.".to_owned(),
                observed: share,
                threshold: erabi_domain::DOMINANT_TRANSITION_SHARE_PERCENT,
            });
        }
    }
    let exploding_query_groups = groups
        .iter()
        .filter(|group| {
            group.canonical_query_variants >= erabi_domain::QUERY_EXPLOSION_MIN_VARIANTS
                && group.query_bearing_identities.saturating_mul(100)
                    >= group
                        .total_identities
                        .saturating_mul(erabi_domain::QUERY_EXPLOSION_QUERY_BEARING_PERCENT)
        })
        .collect::<Vec<_>>();
    if !exploding_query_groups.is_empty() {
        warnings.push(PreviewGrowthWarning {
            code: PreviewGrowthWarningCode::QueryParameterExplosion,
            message: "A host/path has many canonical query variants.".to_owned(),
            observed: exploding_query_groups
                .iter()
                .map(|group| group.canonical_query_variants)
                .max()
                .unwrap_or(0),
            threshold: erabi_domain::QUERY_EXPLOSION_MIN_VARIANTS,
        });
    }
    let denominator = run.matching_urls.len() as u64;
    let unmatched = run.unmatched_urls.len() as u64;
    let ambiguous = run.ambiguous_urls.len() as u64;
    if denominator >= erabi_domain::HIGH_UNMATCHED_MIN_DENOMINATOR
        && unmatched * 100 >= denominator * erabi_domain::HIGH_UNMATCHED_SHARE_PERCENT
    {
        warnings.push(PreviewGrowthWarning {
            code: PreviewGrowthWarningCode::HighUnmatchedRate,
            message: "At least half of matched in-scope URLs were unmatched.".to_owned(),
            observed: unmatched,
            threshold: erabi_domain::HIGH_UNMATCHED_SHARE_PERCENT,
        });
    }
    if denominator >= erabi_domain::WIDESPREAD_AMBIGUITY_MIN_DENOMINATOR
        && ambiguous * 100 >= denominator * erabi_domain::WIDESPREAD_AMBIGUITY_SHARE_PERCENT
    {
        warnings.push(PreviewGrowthWarning {
            code: PreviewGrowthWarningCode::WidespreadPageTypeAmbiguity,
            message: "Many matched in-scope URLs have ambiguous PageTypes.".to_owned(),
            observed: ambiguous,
            threshold: erabi_domain::WIDESPREAD_AMBIGUITY_SHARE_PERCENT,
        });
    }
    let pressure = frontier > 0
        && (has_relevant_budget_hit(&run.budget_hits)
            || has_budget_pressure_utilization(run, counts, total));
    if pressure {
        warnings.push(PreviewGrowthWarning {
            code: PreviewGrowthWarningCode::BudgetPressure,
            message: "The remaining frontier is pressing against a bounded preview budget."
                .to_owned(),
            observed: frontier,
            threshold: erabi_domain::BUDGET_PRESSURE_PERCENT,
        });
    }
    warnings
}

fn has_relevant_budget_hit(hits: &BTreeMap<PreviewBudgetKind, u64>) -> bool {
    hits.keys().any(|kind| {
        matches!(
            kind,
            PreviewBudgetKind::MaxPages
                | PreviewBudgetKind::MaxDepth
                | PreviewBudgetKind::MaxDuration
                | PreviewBudgetKind::MaxDownloadedBytes
                | PreviewBudgetKind::PageTypePageBudget
                | PreviewBudgetKind::TransitionPerSourcePage
                | PreviewBudgetKind::TransitionTotal
        )
    })
}

fn reaches_budget_pressure(observed: u64, limit: u64) -> bool {
    limit > 0
        && u128::from(observed) * 100
            >= u128::from(limit) * u128::from(erabi_domain::BUDGET_PRESSURE_PERCENT)
}

fn has_budget_pressure_utilization(
    run: &PreviewRun,
    counts: &[PreviewTransitionCount],
    total: u64,
) -> bool {
    if reaches_budget_pressure(run.admitted.len() as u64, run.context.limits.max_pages)
        || reaches_budget_pressure(run.consumed_bytes, run.context.limits.max_downloaded_bytes)
        || reaches_budget_pressure(run.elapsed_millis(), run.context.limits.max_duration_ms)
    {
        return true;
    }
    if run.context.page_types.iter().any(|page_type| {
        run.context
            .snapshot
            .version
            .guardrails()
            .page_type(page_type.id)
            .and_then(|budget| budget.page_budget)
            .is_some_and(|limit| {
                reaches_budget_pressure(
                    run.page_type_scheduled
                        .get(&page_type.id.to_string())
                        .copied()
                        .unwrap_or(0),
                    limit,
                )
            })
    }) {
        return true;
    }
    if run.transition_page_counts.iter().any(|((id, _), count)| {
        run.context
            .transitions
            .iter()
            .find(|transition| transition.id.to_string() == *id)
            .is_some_and(|transition| {
                reaches_budget_pressure(
                    u64::from(*count),
                    u64::from(transition.budget.max_links_per_source_page),
                )
            })
    }) {
        return true;
    }
    total > 0
        && counts.iter().any(|count| {
            reaches_budget_pressure(
                count.eligible_edges,
                run.context.transition_total_limit(count.transition_id),
            )
        })
}

impl PreviewRun {
    fn count_scope(&mut self, state: PreviewUrlState) {
        match state {
            PreviewUrlState::External => {
                self.external_urls = self.external_urls.saturating_add(1);
            }
            PreviewUrlState::Blocked => {
                self.blocked_urls = self.blocked_urls.saturating_add(1);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(id: DiscoveryTransitionId, eligible_edges: u64) -> PreviewTransitionCount {
        PreviewTransitionCount {
            transition_id: id,
            transition_name: "transition".to_owned(),
            eligible_edges,
            source_pages_with_eligible_edges: eligible_edges,
        }
    }

    #[test]
    fn dominant_transition_has_no_identifier_or_input_order_tiebreak() {
        let first = DiscoveryTransitionId::new();
        let second = DiscoveryTransitionId::new();
        let tied = [count(first, 8), count(second, 8)];
        assert!(unique_dominant_transition(&tied).is_none());
        let reversed = [count(second, 8), count(first, 8)];
        assert!(unique_dominant_transition(&reversed).is_none());

        let unique = [count(first, 9), count(second, 8)];
        assert_eq!(
            unique_dominant_transition(&unique).map(|item| item.transition_id),
            Some(first)
        );
        let no_observations = [count(first, 0)];
        assert!(unique_dominant_transition(&no_observations).is_none());
    }

    #[test]
    fn budget_pressure_recognizes_every_retained_work_budget_hit() {
        for kind in [
            PreviewBudgetKind::MaxPages,
            PreviewBudgetKind::MaxDownloadedBytes,
            PreviewBudgetKind::PageTypePageBudget,
            PreviewBudgetKind::TransitionPerSourcePage,
            PreviewBudgetKind::TransitionTotal,
        ] {
            let hits = BTreeMap::from([(kind, 1)]);
            assert!(has_relevant_budget_hit(&hits), "{kind:?}");
        }
        let retention_only = BTreeMap::from([(PreviewBudgetKind::ProvenanceRetention, 1)]);
        assert!(!has_relevant_budget_hit(&retention_only));
    }
}
