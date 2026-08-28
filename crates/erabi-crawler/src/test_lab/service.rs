//! Test Lab application service orchestration.

use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use erabi_db::{
    ErabiDatabase,
    repositories::{
        CrawlerEvaluationSnapshot, CrawlerRepository, CrawlerRepositoryError,
        CrawlerSemanticSnapshot, TestEvidenceRecord, TestEvidenceRepository,
        TestEvidenceRepositoryError,
    },
};
use erabi_domain::{
    CanonicalizationDecision, CanonicalizationDecisionCode, CanonicalizationEvidence,
    CanonicalizationOutcome, DiscoveredUrlEvidence, DiscoveryBudgetCandidate,
    DiscoveryBudgetDecision, DiscoveryBudgetExclusion, DiscoveryTransition,
    DiscoveryTransitionEvidence, DomainScopeEvidence, ExtractionObservation, PageType,
    PageTypeMatchEvidence, PaginationEvidence, PaginationKind, PublishedComparisonStatus,
    SelectorCoverageEvidence, SelectorCoverageStatus, TestDiagnostic, TestEvidence, TestEvidenceId,
    TestKind, TestLabComparison, TransitionBudgetEvidence, TransitionBudgetExclusionEvidence,
    resolve_page_type,
};

use super::provider::{
    ExtractionTestHook, ExtractionTestRequest, PageObservation, PaginationObservation,
    SelectorObservation, TestLabObservationRequest, TestLabProvider, TestLabProviderError,
};

const MAX_OBSERVED_LINKS: usize = 64;

#[derive(Clone, Debug)]
pub struct TestLabRequest {
    pub test_kind: TestKind,
    pub input_urls: Vec<String>,
    pub page_type_id: Option<erabi_domain::PageTypeId>,
    pub transition_id: Option<erabi_domain::DiscoveryTransitionId>,
    pub compare_with_active_published: bool,
    pub reuse_artifact_ids: Vec<erabi_domain::ArtifactId>,
}

#[derive(Debug, thiserror::Error)]
pub enum TestLabError {
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
    #[error("PageType was not found")]
    PageTypeNotFound,
    #[error("PageType does not belong to the requested CrawlerVersion")]
    PageTypeNotOwnedByVersion,
    #[error("DiscoveryTransition was not found")]
    DiscoveryTransitionNotFound,
    #[error("DiscoveryTransition does not belong to the requested CrawlerVersion")]
    TransitionNotOwnedByVersion,
    #[error("invalid Test Lab request")]
    InvalidRequest,
    #[error("Test Lab request has too many URLs")]
    TooManyUrls,
    #[error("Test Lab provider is unavailable")]
    ProviderUnavailable,
    #[error("Test Lab provider returned an observation for a different request URL")]
    ProviderObservationRequestMismatch,
    #[error("Artifact was not found")]
    ArtifactNotFound,
    #[error("Artifact cannot safely be reused for this observation")]
    ArtifactNotReusable,
    #[error("TestEvidence was not found")]
    TestEvidenceNotFound,
    #[error("TestEvidence does not belong to the requested CrawlerVersion")]
    TestEvidenceNotOwnedByVersion,
    #[error("Draft configuration changed during Test Lab execution")]
    ConfigurationChanged,
    #[error("durable state is invalid")]
    PersistedStateInvalid,
    #[error("TestEvidence persistence failed")]
    PersistenceFailed,
}

pub struct TestLabService {
    database: ErabiDatabase,
    provider: Option<Arc<dyn TestLabProvider>>,
    extraction_hook: Option<Arc<dyn ExtractionTestHook>>,
}

impl std::fmt::Debug for TestLabService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TestLabService")
            .field("database", &self.database)
            .field("provider_configured", &self.provider.is_some())
            .field(
                "extraction_hook_configured",
                &self.extraction_hook.is_some(),
            )
            .finish()
    }
}

impl TestLabService {
    #[must_use]
    pub fn new(
        database: ErabiDatabase,
        provider: Option<Arc<dyn TestLabProvider>>,
        extraction_hook: Option<Arc<dyn ExtractionTestHook>>,
    ) -> Self {
        Self {
            database,
            provider,
            extraction_hook,
        }
    }

    #[allow(clippy::too_many_lines)]
    ///
    /// # Errors
    /// Returns a stable validation, provider, lifecycle, conflict, or
    /// persistence error. A provider failure never creates partial evidence.
    pub async fn execute(
        &self,
        crawler_id: erabi_domain::CrawlerId,
        version_id: erabi_domain::CrawlerVersionId,
        request: TestLabRequest,
    ) -> Result<TestEvidenceRecord, TestLabError> {
        validate_request(&request)?;
        let needs_provider = requires_provider(request.test_kind);
        let snapshot = CrawlerRepository::new(&self.database)
            .evaluation_snapshot(
                crawler_id,
                version_id,
                request.compare_with_active_published,
            )
            .await
            .map_err(|error| map_crawler_error(&error))?;
        validate_page_type_request(&snapshot, request.page_type_id)?;
        let transition = selected_transition(&snapshot, request.transition_id)?;
        if needs_provider && self.provider.is_none() {
            return Err(TestLabError::ProviderUnavailable);
        }
        self.validate_artifacts(&request.reuse_artifact_ids).await?;
        if !request.reuse_artifact_ids.is_empty() {
            let provider = self
                .provider
                .as_ref()
                .ok_or(TestLabError::ProviderUnavailable)?;
            for artifact_id in &request.reuse_artifact_ids {
                provider
                    .validate_reuse(*artifact_id)
                    .map_err(map_provider_error)?;
            }
        }

        let mut warnings = Vec::new();
        let mut errors = Vec::new();
        let mut canonicalization = Vec::new();
        let mut page_type_match = Vec::new();
        let mut observations = Vec::new();
        for input_url in &request.input_urls {
            let (canonical, page_match, canonical_url) =
                evaluate_url(input_url, &snapshot.draft, &mut warnings, &mut errors);
            canonicalization.push(canonical);
            if let Some(page_match) = page_match {
                page_type_match.push(page_match);
            }
            if needs_provider {
                let provider = self
                    .provider
                    .as_ref()
                    .ok_or(TestLabError::ProviderUnavailable)?;
                let observation = provider
                    .observe(TestLabObservationRequest {
                        requested_url: input_url.clone(),
                        reuse_artifact_ids: request.reuse_artifact_ids.clone(),
                    })
                    .await
                    .map_err(map_provider_error)?;
                if observation.requested_url != *input_url {
                    return Err(TestLabError::ProviderObservationRequestMismatch);
                }
                if canonical_url.is_none() {
                    return Err(TestLabError::InvalidRequest);
                }
                observations.push(observation);
            }
        }

        let mut artifact_ids = request.reuse_artifact_ids.clone();
        for observation in &observations {
            artifact_ids.extend(observation.artifact_ids.iter().copied());
        }
        sort_unique_ids(&mut artifact_ids);
        if artifact_ids.len() > erabi_domain::MAX_TEST_EVIDENCE_ARTIFACTS {
            return Err(TestLabError::InvalidRequest);
        }

        let mut extraction = None;
        let mut selector_coverage = Vec::new();
        let mut pagination = None;
        let mut discovery = None;
        match request.test_kind {
            TestKind::Extraction => {
                extraction = Some(
                    self.extraction_observation(
                        request.page_type_id.ok_or(TestLabError::InvalidRequest)?,
                        &request.input_urls[0],
                        observations
                            .first()
                            .ok_or(TestLabError::ProviderUnavailable)?,
                    )
                    .await,
                );
            }
            TestKind::SelectorCoverage => {
                selector_coverage = selector_evidence(
                    &observations
                        .first()
                        .ok_or(TestLabError::ProviderUnavailable)?
                        .selector_observations,
                );
                if selector_coverage
                    .iter()
                    .any(|item| item.status == SelectorCoverageStatus::NoMatches)
                {
                    push_diagnostic(
                        &mut warnings,
                        diagnostic(
                            "SELECTOR_NO_MATCHES",
                            "A tested selector matched no observed elements.",
                        ),
                    );
                }
            }
            TestKind::Pagination => {
                pagination = pagination_evidence(
                    &observations
                        .first()
                        .ok_or(TestLabError::ProviderUnavailable)?
                        .pagination_observations,
                );
            }
            TestKind::DiscoveredUrlPreview => {
                let observation = observations
                    .first()
                    .ok_or(TestLabError::ProviderUnavailable)?;
                discovery = Some(Self::discovery_evidence(
                    &snapshot.draft,
                    None,
                    &request.input_urls[0],
                    observation,
                    &mut warnings,
                )?);
            }
            TestKind::DiscoveryTransition => {
                let transition = transition.as_ref().ok_or(TestLabError::InvalidRequest)?;
                let observation = observations
                    .first()
                    .ok_or(TestLabError::ProviderUnavailable)?;
                discovery = Some(Self::discovery_evidence(
                    &snapshot.draft,
                    Some(transition),
                    &request.input_urls[0],
                    observation,
                    &mut warnings,
                )?);
            }
            TestKind::UrlCanonicalization
            | TestKind::PageTypeMatching
            | TestKind::CombinedUrlEvaluation => {}
        }

        let published_comparison = if request.compare_with_active_published {
            Some(
                self.comparison(
                    &snapshot,
                    request.test_kind,
                    &request.input_urls,
                    &canonicalization,
                    &page_type_match,
                    extraction.as_ref(),
                    discovery.as_ref(),
                    observations.first(),
                    transition.as_ref(),
                    request.page_type_id,
                    &mut warnings,
                )
                .await,
            )
        } else {
            None
        };

        let evidence = TestEvidence {
            schema_version: erabi_domain::TEST_EVIDENCE_SCHEMA_VERSION,
            id: TestEvidenceId::new(),
            crawler_version_id: version_id,
            test_kind: request.test_kind,
            input_urls: request.input_urls,
            evaluated_page_type_id: request.page_type_id,
            tested_transition_id: request.transition_id,
            canonicalization,
            page_type_match,
            extraction,
            selector_coverage,
            pagination,
            discovery,
            warnings,
            errors,
            artifact_ids,
            config_hash: snapshot.draft.config_hash,
            executed_at: execution_timestamp(),
            published_comparison,
        };
        evidence
            .validate()
            .map_err(|_| TestLabError::InvalidRequest)?;
        TestEvidenceRepository::new(&self.database)
            .persist_if_configuration_matches(crawler_id, &evidence)
            .await
            .map_err(map_evidence_error)?;
        TestEvidenceRepository::new(&self.database)
            .read(crawler_id, version_id, evidence.id)
            .await
            .map_err(map_evidence_error)
    }

    ///
    /// # Errors
    /// Returns a typed persistence or corruption error.
    pub async fn list_evidence(
        &self,
        crawler_id: erabi_domain::CrawlerId,
        version_id: erabi_domain::CrawlerVersionId,
    ) -> Result<Vec<TestEvidenceRecord>, TestLabError> {
        TestEvidenceRepository::new(&self.database)
            .list(crawler_id, version_id)
            .await
            .map_err(map_evidence_error)
    }

    ///
    /// # Errors
    /// Returns a typed persistence or corruption error.
    pub async fn read_evidence(
        &self,
        crawler_id: erabi_domain::CrawlerId,
        version_id: erabi_domain::CrawlerVersionId,
        evidence_id: TestEvidenceId,
    ) -> Result<TestEvidenceRecord, TestLabError> {
        TestEvidenceRepository::new(&self.database)
            .read(crawler_id, version_id, evidence_id)
            .await
            .map_err(map_evidence_error)
    }

    async fn validate_artifacts(
        &self,
        artifact_ids: &[erabi_domain::ArtifactId],
    ) -> Result<(), TestLabError> {
        let repository = erabi_db::repositories::ArtifactRepository::new(&self.database);
        for artifact_id in artifact_ids {
            repository
                .safe_relative_path(*artifact_id)
                .await
                .map_err(|_| TestLabError::ArtifactNotFound)?;
        }
        Ok(())
    }

    async fn extraction_observation(
        &self,
        page_type_id: erabi_domain::PageTypeId,
        input_url: &str,
        observation: &PageObservation,
    ) -> ExtractionObservation {
        let Some(hook) = &self.extraction_hook else {
            return ExtractionObservation::Unavailable {
                reason: "No extraction Test Lab hook is configured.".to_owned(),
            };
        };
        hook.evaluate(ExtractionTestRequest {
            page_type_id,
            input_url: input_url.to_owned(),
            observation: observation.clone(),
        })
        .await
    }

    #[allow(clippy::too_many_lines)]
    fn discovery_evidence(
        snapshot: &CrawlerSemanticSnapshot,
        transition: Option<&DiscoveryTransition>,
        input_url: &str,
        observation: &PageObservation,
        warnings: &mut Vec<TestDiagnostic>,
    ) -> Result<DiscoveryTransitionEvidence, TestLabError> {
        let base_url = observation.final_url.as_deref().unwrap_or(input_url);
        let base_url = snapshot
            .version
            .canonicalization_policy()
            .canonicalize(base_url)
            .map_err(|_| TestLabError::InvalidRequest)?
            .canonical_url;
        let source_match = transition.and_then(|_| {
            let (_, source_match, _) = evaluate_url_without_diagnostics(input_url, snapshot);
            source_match
        });
        let source_is_applicable = transition.is_none_or(|selected| {
            let applicable = source_match.as_ref().is_some_and(|source| {
                source.decision == erabi_domain::PageTypeMatchStatus::Matched
                    && source
                        .winner
                        .as_ref()
                        .is_some_and(|winner| winner.page_type_id == selected.source_page_type_id)
            });
            if !applicable {
                let (code, message) = match source_match.as_ref().map(|source| source.decision) {
                    Some(erabi_domain::PageTypeMatchStatus::Ambiguous) => (
                        "AMBIGUOUS_TRANSITION_SOURCE_PAGE_TYPE",
                        "The source URL has tied PageType candidates, so the selected transition is ineligible.",
                    ),
                    Some(erabi_domain::PageTypeMatchStatus::Unmatched) | None => (
                        "UNMATCHED_TRANSITION_SOURCE_PAGE_TYPE",
                        "The source URL did not resolve to the selected transition source PageType.",
                    ),
                    Some(erabi_domain::PageTypeMatchStatus::Matched) => (
                        "TRANSITION_SOURCE_PAGE_TYPE_MISMATCH",
                        "The source URL matched a different PageType than the selected transition source.",
                    ),
                };
                push_diagnostic(warnings, diagnostic(code, message));
            }
            applicable
        });
        if transition.is_some_and(|selected| selected.budget.total_budget.is_some()) {
            push_diagnostic(
                warnings,
                diagnostic(
                    "FOCUSED_TRANSITION_TOTAL_BASELINE",
                    "Focused Test Lab evaluates the selected transition total budget from zero for this observed page.",
                ),
            );
        }
        let transition_for_budget = transition.map(|item| &item.budget);
        let target_guardrail = transition.and_then(|item| {
            snapshot
                .version
                .guardrails()
                .page_types
                .iter()
                .find(|guardrail| guardrail.page_type_id == item.target_page_type_id)
        });
        let page_types = snapshot
            .page_types
            .iter()
            .map(erabi_db::repositories::PageTypeRecord::domain_page_type)
            .collect::<Vec<_>>();
        let mut links = observation.discovered_links.clone();
        links.sort_by(|left, right| {
            left.raw_href
                .cmp(&right.raw_href)
                .then(left.selector.cmp(&right.selector))
        });
        links.truncate(MAX_OBSERVED_LINKS);
        let mut canonical_urls = BTreeSet::new();
        let mut discovered_urls = Vec::with_capacity(links.len());
        let mut eligible_link_count = 0_u32;
        // Test Lab has no production-run history. These counters are the
        // selected transition's already-consumed focused-page baseline only.
        let mut transition_links_on_source_page = 0_u32;
        let mut transition_total_links = 0_u64;
        let per_page_limit = transition.map_or(0, |item| item.budget.max_links_per_source_page);
        let mut reported_missing_selector_provenance = false;
        for link in &links {
            let mut item = DiscoveredUrlEvidence {
                raw_href: link.raw_href.clone(),
                resolved_original_url: None,
                canonical_url: None,
                canonicalization: None,
                scope: None,
                duplicate: false,
                duplicate_of_canonical_url: None,
                page_type_match: None,
                transition_eligible: false,
                budget: None,
            };
            let Ok(resolved) = base_url.join(&link.raw_href) else {
                push_diagnostic(
                    warnings,
                    diagnostic(
                        "INVALID_DISCOVERED_URL",
                        "A discovered href could not be resolved against the page URL.",
                    ),
                );
                discovered_urls.push(item);
                continue;
            };
            item.resolved_original_url = Some(resolved.to_string());
            let Ok(canonical) = snapshot
                .version
                .canonicalization_policy()
                .canonicalize(resolved.as_str())
            else {
                item.canonicalization = Some(CanonicalizationEvidence {
                    original_url: resolved.to_string(),
                    canonical_url: None,
                    outcome: CanonicalizationOutcome::InvalidUrl,
                    decisions: Vec::new(),
                });
                push_diagnostic(
                    warnings,
                    diagnostic(
                        "INVALID_DISCOVERED_URL",
                        "A discovered href is not a valid crawl URL.",
                    ),
                );
                discovered_urls.push(item);
                continue;
            };
            let canonical_url = canonical.canonical_url.clone();
            item.canonical_url = Some(canonical_url.to_string());
            item.canonicalization = Some(canonicalization_from_result(canonical));
            let scope = snapshot
                .version
                .domain_scope()
                .classify(&canonical_url, snapshot.version.seeds())
                .map_err(|_| TestLabError::PersistedStateInvalid)?;
            let in_scope = matches!(
                scope,
                erabi_domain::DomainScopeClassification::InScope { .. }
            );
            item.scope = Some(DomainScopeEvidence::from_classification(&scope));
            if !canonical_urls.insert(canonical_url.to_string()) {
                item.duplicate = true;
                item.duplicate_of_canonical_url = Some(canonical_url.to_string());
                discovered_urls.push(item);
                continue;
            }
            if !in_scope {
                push_diagnostic(
                    warnings,
                    diagnostic(
                        "OUT_OF_SCOPE_DISCOVERED_URL",
                        "The discovered URL was preserved but is not eligible for this transition.",
                    ),
                );
                discovered_urls.push(item);
                continue;
            }
            let match_decision = resolve_page_type(&canonical_url, &page_types);
            let matched_target = transition.is_some_and(|selected| {
                matches!(&match_decision, erabi_domain::PageTypeMatchDecision::Matched(candidate) if candidate.page_type_id == selected.target_page_type_id)
            });
            item.page_type_match = Some(PageTypeMatchEvidence::from_decision(&match_decision));
            if matches!(
                match_decision,
                erabi_domain::PageTypeMatchDecision::Ambiguous { .. }
            ) {
                push_diagnostic(
                    warnings,
                    diagnostic(
                        "AMBIGUOUS_PAGE_TYPE",
                        "The discovered URL has tied PageType candidates; no winner was selected.",
                    ),
                );
            } else if matches!(
                match_decision,
                erabi_domain::PageTypeMatchDecision::Unmatched
            ) {
                push_diagnostic(
                    warnings,
                    diagnostic(
                        "UNMATCHED_PAGE_TYPE",
                        "The discovered URL matched no PageType.",
                    ),
                );
            }
            let Some(selected) = transition else {
                discovered_urls.push(item);
                continue;
            };
            if !selected.enabled || !source_is_applicable {
                discovered_urls.push(item);
                continue;
            }
            let Some(selector) = link.selector.as_deref() else {
                if !reported_missing_selector_provenance {
                    push_diagnostic(
                        warnings,
                        diagnostic(
                            "TRANSITION_SELECTOR_PROVENANCE_UNAVAILABLE",
                            "A discovered link has no selector provenance and is not eligible for the selected transition.",
                        ),
                    );
                    reported_missing_selector_provenance = true;
                }
                discovered_urls.push(item);
                continue;
            };
            if selector != selected.link_selector || !matched_target {
                discovered_urls.push(item);
                continue;
            }
            let evaluator = erabi_domain::DiscoveryBudgetEvaluator::new(
                snapshot.version.guardrails(),
                target_guardrail,
                transition_for_budget,
            );
            let decision = evaluator
                .evaluate(DiscoveryBudgetCandidate {
                    transition_links_on_source_page,
                    transition_total_links,
                    ..DiscoveryBudgetCandidate::default()
                })
                .map_err(|_| TestLabError::PersistedStateInvalid)?;
            let budget = match decision {
                DiscoveryBudgetDecision::Allowed => TransitionBudgetEvidence {
                    allowed: true,
                    exclusion: None,
                },
                DiscoveryBudgetDecision::Excluded(exclusion) => TransitionBudgetEvidence {
                    allowed: false,
                    exclusion: Some(budget_exclusion(exclusion)),
                },
            };
            item.transition_eligible = budget.allowed;
            item.budget = Some(budget);
            if item.transition_eligible {
                eligible_link_count = eligible_link_count.saturating_add(1);
                transition_links_on_source_page = transition_links_on_source_page.saturating_add(1);
                transition_total_links = transition_total_links.saturating_add(1);
            }
            discovered_urls.push(item);
        }
        let per_page_limit_reached = transition.is_some_and(|selected| {
            transition_links_on_source_page >= selected.budget.max_links_per_source_page
                || discovered_urls.iter().any(|item| {
                    item.budget.as_ref().and_then(|budget| budget.exclusion)
                        == Some(TransitionBudgetExclusionEvidence::TransitionPerPageLinkLimit)
                })
        });
        let (transition_id, transition_name, source_page_type_id, target_page_type_id) = transition
            .map_or_else(
                || (None, None, None, None),
                |selected| {
                    (
                        Some(selected.id),
                        Some(selected.name.clone()),
                        Some(selected.source_page_type_id),
                        Some(selected.target_page_type_id),
                    )
                },
            );
        Ok(DiscoveryTransitionEvidence {
            transition_id,
            transition_name,
            source_page_type_id,
            target_page_type_id,
            source_match,
            selector: transition.map_or_else(
                || SelectorCoverageEvidence {
                    selector: "discovered_links".to_owned(),
                    matches_found: u32::try_from(observation.discovered_links.len())
                        .unwrap_or(u32::MAX),
                    status: if observation.discovered_links.is_empty() {
                        SelectorCoverageStatus::NoMatches
                    } else {
                        SelectorCoverageStatus::Observed
                    },
                },
                |selected| selector_for_transition(observation, selected),
            ),
            discovered_urls,
            eligible_link_count,
            per_page_limit,
            per_page_limit_reached,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn comparison(
        &self,
        snapshot: &CrawlerEvaluationSnapshot,
        test_kind: TestKind,
        input_urls: &[String],
        draft_canonicalization: &[CanonicalizationEvidence],
        draft_page_match: &[PageTypeMatchEvidence],
        draft_extraction: Option<&ExtractionObservation>,
        draft_discovery: Option<&DiscoveryTransitionEvidence>,
        observation: Option<&PageObservation>,
        draft_transition: Option<&DiscoveryTransition>,
        draft_page_type_id: Option<erabi_domain::PageTypeId>,
        warnings: &mut Vec<TestDiagnostic>,
    ) -> TestLabComparison {
        let Some(published) = &snapshot.published else {
            let warning = diagnostic(
                "NO_ACTIVE_PUBLISHED_VERSION",
                "No active Published version was available for comparison.",
            );
            push_diagnostic(warnings, warning.clone());
            return TestLabComparison {
                status: PublishedComparisonStatus::NoActivePublishedVersion,
                draft_version_id: snapshot.draft.version.id(),
                draft_config_hash: snapshot.draft.config_hash.clone(),
                published_version_id: None,
                published_config_hash: None,
                canonicalization_difference: false,
                draft_canonicalization: draft_canonicalization.to_vec(),
                published_canonicalization: Vec::new(),
                page_type_match_difference: false,
                draft_page_type_match: draft_page_match.to_vec(),
                published_page_type_match: Vec::new(),
                discovery_difference: None,
                extraction_difference: None,
                warnings: vec![warning],
            };
        };
        let mut published_canonicalization = Vec::new();
        let mut published_page_match = Vec::new();
        for input_url in input_urls {
            let (canonical, page_match, _) = evaluate_url_without_diagnostics(input_url, published);
            published_canonicalization.push(canonical);
            if let Some(page_match) = page_match {
                published_page_match.push(page_match);
            }
        }
        let (discovery_difference, discovery_warnings) = comparison_discovery_difference(
            snapshot,
            published,
            &input_urls[0],
            draft_discovery,
            observation,
            draft_transition,
        );
        let (extraction_difference, extraction_warnings) = if test_kind == TestKind::Extraction {
            self.comparison_extraction_difference(
                snapshot,
                published,
                &input_urls[0],
                draft_extraction,
                observation,
                draft_page_type_id,
            )
            .await
        } else {
            (None, Vec::new())
        };
        let mut comparison_warnings = discovery_warnings;
        comparison_warnings.extend(extraction_warnings);
        for warning in &comparison_warnings {
            push_diagnostic(warnings, warning.clone());
        }
        TestLabComparison {
            status: PublishedComparisonStatus::Compared,
            draft_version_id: snapshot.draft.version.id(),
            draft_config_hash: snapshot.draft.config_hash.clone(),
            published_version_id: Some(published.version.id()),
            published_config_hash: Some(published.config_hash.clone()),
            canonicalization_difference: draft_canonicalization != published_canonicalization,
            draft_canonicalization: draft_canonicalization.to_vec(),
            published_canonicalization,
            page_type_match_difference:
                !erabi_domain::page_type_match_sequences_behaviorally_equivalent(
                    draft_page_match,
                    &published_page_match,
                ),
            draft_page_type_match: draft_page_match.to_vec(),
            published_page_type_match: published_page_match,
            discovery_difference,
            extraction_difference,
            warnings: comparison_warnings,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn comparison_extraction_difference(
        &self,
        snapshot: &CrawlerEvaluationSnapshot,
        published: &CrawlerSemanticSnapshot,
        input_url: &str,
        draft_extraction: Option<&ExtractionObservation>,
        observation: Option<&PageObservation>,
        draft_page_type_id: Option<erabi_domain::PageTypeId>,
    ) -> (Option<bool>, Vec<TestDiagnostic>) {
        let Some(draft_extraction) = draft_extraction else {
            return (None, Vec::new());
        };
        let Some(observation) = observation else {
            return (None, Vec::new());
        };
        let Some(draft_page_type_id) = draft_page_type_id else {
            return (None, Vec::new());
        };
        if self.extraction_hook.is_none() {
            return (None, Vec::new());
        }
        let Some(published_page_type) =
            unique_corresponding_page_type(&snapshot.draft, published, draft_page_type_id)
        else {
            return (
                None,
                vec![diagnostic(
                    "PUBLISHED_PAGE_TYPE_CORRESPONDENCE_UNAVAILABLE",
                    "No unique semantic Published PageType corresponds to the selected Draft PageType.",
                )],
            );
        };
        let published_extraction = self
            .extraction_observation(published_page_type.id, input_url, observation)
            .await;
        (
            Some(!extraction_observations_equal(
                draft_extraction,
                &published_extraction,
            )),
            Vec::new(),
        )
    }
}

fn comparison_discovery_difference(
    snapshot: &CrawlerEvaluationSnapshot,
    published: &CrawlerSemanticSnapshot,
    input_url: &str,
    draft_discovery: Option<&DiscoveryTransitionEvidence>,
    observation: Option<&PageObservation>,
    draft_transition: Option<&DiscoveryTransition>,
) -> (Option<bool>, Vec<TestDiagnostic>) {
    let Some(draft_discovery) = draft_discovery else {
        return (None, Vec::new());
    };
    let Some(observation) = observation else {
        return (None, Vec::new());
    };
    let published_transition = if let Some(draft_transition) = draft_transition {
        let Some(transition) =
            unique_corresponding_transition(&snapshot.draft, published, draft_transition)
        else {
            return (
                None,
                vec![diagnostic(
                    "PUBLISHED_TRANSITION_CORRESPONDENCE_UNAVAILABLE",
                    "No unique semantic Published transition corresponds to the selected Draft transition.",
                )],
            );
        };
        Some(transition)
    } else {
        None
    };
    let mut warnings = Vec::new();
    if let Ok(published_discovery) = TestLabService::discovery_evidence(
        published,
        published_transition,
        input_url,
        observation,
        &mut warnings,
    ) {
        (
            Some(!discovery_outcomes_equal(
                draft_discovery,
                &published_discovery,
            )),
            warnings,
        )
    } else {
        (
            None,
            vec![diagnostic(
                "PUBLISHED_DISCOVERY_COMPARISON_UNAVAILABLE",
                "Published discovery behavior could not be evaluated from the captured observation.",
            )],
        )
    }
}

fn validate_request(request: &TestLabRequest) -> Result<(), TestLabError> {
    if request.input_urls.is_empty() {
        return Err(TestLabError::InvalidRequest);
    }
    if request.input_urls.len() > erabi_domain::MAX_TEST_EVIDENCE_INPUT_URLS {
        return Err(TestLabError::TooManyUrls);
    }
    if request.input_urls.iter().any(|url| {
        url.is_empty() || url.chars().count() > erabi_domain::MAX_TEST_EVIDENCE_URL_CHARS
    }) {
        return Err(TestLabError::InvalidRequest);
    }
    if request.reuse_artifact_ids.len() > erabi_domain::MAX_TEST_EVIDENCE_ARTIFACTS {
        return Err(TestLabError::InvalidRequest);
    }
    match request.test_kind {
        TestKind::Extraction
        | TestKind::SelectorCoverage
        | TestKind::Pagination
        | TestKind::DiscoveredUrlPreview
        | TestKind::DiscoveryTransition
            if request.input_urls.len() != 1 =>
        {
            Err(TestLabError::InvalidRequest)
        }
        TestKind::DiscoveryTransition if request.transition_id.is_none() => {
            Err(TestLabError::InvalidRequest)
        }
        TestKind::Extraction | TestKind::SelectorCoverage if request.page_type_id.is_none() => {
            Err(TestLabError::InvalidRequest)
        }
        _ => Ok(()),
    }
}

fn requires_provider(kind: TestKind) -> bool {
    matches!(
        kind,
        TestKind::Extraction
            | TestKind::SelectorCoverage
            | TestKind::Pagination
            | TestKind::DiscoveredUrlPreview
            | TestKind::DiscoveryTransition
    )
}

fn validate_page_type_request(
    snapshot: &CrawlerEvaluationSnapshot,
    page_type_id: Option<erabi_domain::PageTypeId>,
) -> Result<(), TestLabError> {
    if let Some(page_type_id) = page_type_id
        && !snapshot
            .draft
            .page_types
            .iter()
            .any(|page_type| page_type.id == page_type_id)
    {
        return Err(TestLabError::PageTypeNotOwnedByVersion);
    }
    Ok(())
}

fn selected_transition(
    snapshot: &CrawlerEvaluationSnapshot,
    transition_id: Option<erabi_domain::DiscoveryTransitionId>,
) -> Result<Option<DiscoveryTransition>, TestLabError> {
    let Some(transition_id) = transition_id else {
        return Ok(None);
    };
    snapshot
        .draft
        .transitions
        .iter()
        .find(|transition| transition.transition.id == transition_id)
        .map(|record| Some(record.transition.clone()))
        .ok_or(TestLabError::TransitionNotOwnedByVersion)
}

fn unique_corresponding_page_type<'a>(
    draft: &CrawlerSemanticSnapshot,
    published: &'a CrawlerSemanticSnapshot,
    draft_page_type_id: erabi_domain::PageTypeId,
) -> Option<&'a erabi_db::repositories::PageTypeRecord> {
    let draft_page_type = draft
        .page_types
        .iter()
        .find(|page_type| page_type.id == draft_page_type_id)?;
    let matches = published
        .page_types
        .iter()
        .filter(|published_page_type| {
            page_type_records_semantically_equivalent(draft_page_type, published_page_type)
        })
        .collect::<Vec<_>>();
    (matches.len() == 1).then_some(matches[0])
}

fn unique_corresponding_transition<'a>(
    draft: &CrawlerSemanticSnapshot,
    published: &'a CrawlerSemanticSnapshot,
    draft_transition: &DiscoveryTransition,
) -> Option<&'a DiscoveryTransition> {
    let matches = published
        .transitions
        .iter()
        .map(|record| &record.transition)
        .filter(|published_transition| {
            transitions_semantically_equivalent(
                draft,
                published,
                draft_transition,
                published_transition,
            )
        })
        .collect::<Vec<_>>();
    (matches.len() == 1).then_some(matches[0])
}

fn transitions_semantically_equivalent(
    draft: &CrawlerSemanticSnapshot,
    published: &CrawlerSemanticSnapshot,
    draft_transition: &DiscoveryTransition,
    published_transition: &DiscoveryTransition,
) -> bool {
    let Some(source) =
        unique_corresponding_page_type(draft, published, draft_transition.source_page_type_id)
    else {
        return false;
    };
    let Some(target) =
        unique_corresponding_page_type(draft, published, draft_transition.target_page_type_id)
    else {
        return false;
    };
    published_transition.source_page_type_id == source.id
        && published_transition.target_page_type_id == target.id
        && draft_transition.name == published_transition.name
        && draft_transition.enabled == published_transition.enabled
        && draft_transition.link_selector == published_transition.link_selector
        && draft_transition.url_constraints == published_transition.url_constraints
        && draft_transition.priority == published_transition.priority
        && draft_transition.budget == published_transition.budget
        && draft_transition.deduplicate == published_transition.deduplicate
}

fn page_type_records_semantically_equivalent(
    left: &erabi_db::repositories::PageTypeRecord,
    right: &erabi_db::repositories::PageTypeRecord,
) -> bool {
    left.name == right.name
        && left.priority == right.priority
        && matcher_multisets_equal(&left.matchers, &right.matchers)
}

fn matcher_multisets_equal(
    left: &[erabi_db::repositories::UrlMatcherRecord],
    right: &[erabi_db::repositories::UrlMatcherRecord],
) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut used = vec![false; right.len()];
    left.iter().all(|left_matcher| {
        let matching_index = right.iter().enumerate().find_map(|(index, right_matcher)| {
            (!used[index]
                && left_matcher.matcher.definition() == right_matcher.matcher.definition())
            .then_some(index)
        });
        matching_index.is_some_and(|index| {
            used[index] = true;
            true
        })
    })
}

fn discovery_outcomes_equal(
    draft: &DiscoveryTransitionEvidence,
    published: &DiscoveryTransitionEvidence,
) -> bool {
    draft.selector == published.selector
        && optional_page_type_match_evidence_equal(
            draft.source_match.as_ref(),
            published.source_match.as_ref(),
        )
        && draft.eligible_link_count == published.eligible_link_count
        && draft.per_page_limit == published.per_page_limit
        && draft.per_page_limit_reached == published.per_page_limit_reached
        && draft.discovered_urls.len() == published.discovered_urls.len()
        && draft
            .discovered_urls
            .iter()
            .zip(&published.discovered_urls)
            .all(|(draft, published)| discovered_url_outcomes_equal(draft, published))
}

fn optional_page_type_match_evidence_equal(
    left: Option<&PageTypeMatchEvidence>,
    right: Option<&PageTypeMatchEvidence>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.behaviorally_equivalent(right),
        (None, None) => true,
        _ => false,
    }
}

fn discovered_url_outcomes_equal(
    draft: &DiscoveredUrlEvidence,
    published: &DiscoveredUrlEvidence,
) -> bool {
    draft.raw_href == published.raw_href
        && draft.resolved_original_url == published.resolved_original_url
        && draft.canonical_url == published.canonical_url
        && draft.canonicalization == published.canonicalization
        && draft.scope == published.scope
        && draft.duplicate == published.duplicate
        && draft.duplicate_of_canonical_url == published.duplicate_of_canonical_url
        && optional_page_type_match_evidence_equal(
            draft.page_type_match.as_ref(),
            published.page_type_match.as_ref(),
        )
        && draft.transition_eligible == published.transition_eligible
        && draft.budget == published.budget
}

fn extraction_observations_equal(
    left: &ExtractionObservation,
    right: &ExtractionObservation,
) -> bool {
    match (left, right) {
        (
            ExtractionObservation::Available { fields: left },
            ExtractionObservation::Available { fields: right },
        ) => {
            let mut left = left.clone();
            let mut right = right.clone();
            left.sort_by(|left, right| {
                left.name
                    .cmp(&right.name)
                    .then(left.observed.cmp(&right.observed))
            });
            right.sort_by(|left, right| {
                left.name
                    .cmp(&right.name)
                    .then(left.observed.cmp(&right.observed))
            });
            left == right
        }
        _ => left == right,
    }
}

fn evaluate_url(
    input_url: &str,
    snapshot: &CrawlerSemanticSnapshot,
    warnings: &mut Vec<TestDiagnostic>,
    errors: &mut Vec<TestDiagnostic>,
) -> (
    CanonicalizationEvidence,
    Option<PageTypeMatchEvidence>,
    Option<url::Url>,
) {
    let (canonical, page_match, url) = evaluate_url_without_diagnostics(input_url, snapshot);
    if canonical.outcome == CanonicalizationOutcome::InvalidUrl {
        push_diagnostic(
            errors,
            diagnostic("INVALID_URL", "The input URL could not be canonicalized."),
        );
    }
    if let Some(page_match) = &page_match {
        match page_match.decision {
            erabi_domain::PageTypeMatchStatus::Ambiguous => push_diagnostic(
                warnings,
                diagnostic(
                    "AMBIGUOUS_PAGE_TYPE",
                    "Tied PageType candidates were retained without choosing a winner.",
                ),
            ),
            erabi_domain::PageTypeMatchStatus::Unmatched => push_diagnostic(
                warnings,
                diagnostic("UNMATCHED_PAGE_TYPE", "The URL matched no PageType."),
            ),
            erabi_domain::PageTypeMatchStatus::Matched => {}
        }
    }
    (canonical, page_match, url)
}

fn evaluate_url_without_diagnostics(
    input_url: &str,
    snapshot: &CrawlerSemanticSnapshot,
) -> (
    CanonicalizationEvidence,
    Option<PageTypeMatchEvidence>,
    Option<url::Url>,
) {
    match snapshot
        .version
        .canonicalization_policy()
        .canonicalize(input_url)
    {
        Ok(result) => {
            let url = result.canonical_url.clone();
            let page_types = snapshot
                .page_types
                .iter()
                .map(erabi_db::repositories::PageTypeRecord::domain_page_type)
                .collect::<Vec<PageType>>();
            let decision = resolve_page_type(&url, &page_types);
            (
                canonicalization_from_result(result),
                Some(PageTypeMatchEvidence::from_decision(&decision)),
                Some(url),
            )
        }
        Err(_) => (
            CanonicalizationEvidence {
                original_url: input_url.to_owned(),
                canonical_url: None,
                outcome: CanonicalizationOutcome::InvalidUrl,
                decisions: Vec::new(),
            },
            None,
            None,
        ),
    }
}

fn canonicalization_from_result(
    result: erabi_domain::CanonicalizationResult,
) -> CanonicalizationEvidence {
    CanonicalizationEvidence {
        original_url: result.original_url,
        canonical_url: Some(result.canonical_url.to_string()),
        outcome: CanonicalizationOutcome::Canonicalized,
        decisions: result
            .decisions
            .into_iter()
            .map(decision_evidence)
            .collect(),
    }
}

fn decision_evidence(
    decision: CanonicalizationDecision,
) -> erabi_domain::CanonicalizationDecisionEvidence {
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
        CanonicalizationDecision::QuerySorted => (CanonicalizationDecisionCode::QuerySorted, None),
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
}

fn selector_evidence(observations: &[SelectorObservation]) -> Vec<SelectorCoverageEvidence> {
    let mut observations = observations.to_vec();
    observations.sort_by(|left, right| left.selector.cmp(&right.selector));
    observations
        .into_iter()
        .take(erabi_domain::MAX_TEST_EVIDENCE_INPUT_URLS)
        .map(|observation| SelectorCoverageEvidence {
            selector: observation.selector,
            matches_found: observation.matches_found,
            status: if observation.matches_found == 0 {
                SelectorCoverageStatus::NoMatches
            } else {
                SelectorCoverageStatus::Observed
            },
        })
        .collect()
}

fn pagination_evidence(observations: &[PaginationObservation]) -> Option<PaginationEvidence> {
    let mut observations = observations.to_vec();
    observations.sort_by(|left, right| {
        pagination_rank(left.kind)
            .cmp(&pagination_rank(right.kind))
            .then(left.selector.cmp(&right.selector))
            .then(left.target_url.cmp(&right.target_url))
    });
    observations
        .into_iter()
        .next()
        .map(|observation| PaginationEvidence {
            kind: observation.kind,
            selector: observation.selector,
            target_url: observation.target_url,
        })
}

fn pagination_rank(kind: PaginationKind) -> u8 {
    match kind {
        PaginationKind::RelNext => 0,
        PaginationKind::NextOlderMoreLink => 1,
        PaginationKind::NumberedPagination => 2,
        PaginationKind::UrlPageNumber => 3,
    }
}

fn selector_for_transition(
    observation: &PageObservation,
    transition: &DiscoveryTransition,
) -> SelectorCoverageEvidence {
    let matching_links = observation
        .discovered_links
        .iter()
        .filter(|link| link.selector.as_deref() == Some(transition.link_selector.as_str()))
        .count();
    let has_attributed_link = observation
        .discovered_links
        .iter()
        .any(|link| link.selector.is_some());
    SelectorCoverageEvidence {
        selector: transition.link_selector.clone(),
        matches_found: u32::try_from(matching_links).unwrap_or(u32::MAX),
        status: if matching_links > 0 {
            SelectorCoverageStatus::Observed
        } else if has_attributed_link {
            SelectorCoverageStatus::NoMatches
        } else {
            SelectorCoverageStatus::Unavailable
        },
    }
}

fn budget_exclusion(exclusion: DiscoveryBudgetExclusion) -> TransitionBudgetExclusionEvidence {
    match exclusion {
        DiscoveryBudgetExclusion::MaxPages => TransitionBudgetExclusionEvidence::MaxPages,
        DiscoveryBudgetExclusion::MaxDuration => TransitionBudgetExclusionEvidence::MaxDuration,
        DiscoveryBudgetExclusion::MaxDepth => TransitionBudgetExclusionEvidence::MaxDepth,
        DiscoveryBudgetExclusion::MaxDownloadedBytes => {
            TransitionBudgetExclusionEvidence::MaxDownloadedBytes
        }
        DiscoveryBudgetExclusion::PageTypePageBudget => {
            TransitionBudgetExclusionEvidence::PageTypePageBudget
        }
        DiscoveryBudgetExclusion::TransitionPerPageLinkLimit => {
            TransitionBudgetExclusionEvidence::TransitionPerPageLinkLimit
        }
        DiscoveryBudgetExclusion::TransitionTotalBudget => {
            TransitionBudgetExclusionEvidence::TransitionTotalBudget
        }
    }
}

fn sort_unique_ids(ids: &mut Vec<erabi_domain::ArtifactId>) {
    ids.sort_by_key(ToString::to_string);
    ids.dedup();
}

fn diagnostic(code: impl Into<String>, message: impl Into<String>) -> TestDiagnostic {
    TestDiagnostic {
        code: code.into(),
        message: message.into(),
    }
}

fn push_diagnostic(diagnostics: &mut Vec<TestDiagnostic>, diagnostic: TestDiagnostic) {
    if diagnostics.len() < erabi_domain::MAX_TEST_EVIDENCE_DIAGNOSTICS {
        diagnostics.push(diagnostic);
    }
}

fn execution_timestamp() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    format!("unix:{seconds}")
}

fn map_provider_error(error: TestLabProviderError) -> TestLabError {
    match error {
        TestLabProviderError::Unavailable | TestLabProviderError::Failed => {
            TestLabError::ProviderUnavailable
        }
        TestLabProviderError::ArtifactNotReusable => TestLabError::ArtifactNotReusable,
    }
}

fn map_crawler_error(error: &CrawlerRepositoryError) -> TestLabError {
    match error {
        CrawlerRepositoryError::CrawlerNotFound => TestLabError::CrawlerNotFound,
        CrawlerRepositoryError::CrawlerVersionNotFound => TestLabError::CrawlerVersionNotFound,
        CrawlerRepositoryError::VersionNotOwnedByCrawler => TestLabError::VersionNotOwnedByCrawler,
        CrawlerRepositoryError::VersionNotDraft
        | CrawlerRepositoryError::PublishedVersionImmutable => TestLabError::VersionNotDraft,
        CrawlerRepositoryError::VersionNotActiveDraft => TestLabError::VersionNotActiveDraft,
        CrawlerRepositoryError::PageTypeNotFound => TestLabError::PageTypeNotFound,
        CrawlerRepositoryError::PageTypeNotOwnedByVersion => {
            TestLabError::PageTypeNotOwnedByVersion
        }
        CrawlerRepositoryError::DiscoveryTransitionNotFound => {
            TestLabError::DiscoveryTransitionNotFound
        }
        CrawlerRepositoryError::TransitionNotOwnedByVersion => {
            TestLabError::TransitionNotOwnedByVersion
        }
        CrawlerRepositoryError::CorruptState
        | CrawlerRepositoryError::InvalidCanonicalizationPolicy
        | CrawlerRepositoryError::InvalidDomainScope
        | CrawlerRepositoryError::InvalidCrawlGuardrails
        | CrawlerRepositoryError::InvalidPageTypeBudget
        | CrawlerRepositoryError::InvalidTransitionBudget => TestLabError::PersistedStateInvalid,
        _ => TestLabError::PersistenceFailed,
    }
}

fn map_evidence_error(error: TestEvidenceRepositoryError) -> TestLabError {
    match error {
        TestEvidenceRepositoryError::ConfigurationChanged => TestLabError::ConfigurationChanged,
        TestEvidenceRepositoryError::ArtifactNotFound => TestLabError::ArtifactNotFound,
        TestEvidenceRepositoryError::CorruptState => TestLabError::PersistedStateInvalid,
        TestEvidenceRepositoryError::TestEvidenceNotFound => TestLabError::TestEvidenceNotFound,
        TestEvidenceRepositoryError::TestEvidenceNotOwnedByVersion => {
            TestLabError::TestEvidenceNotOwnedByVersion
        }
        TestEvidenceRepositoryError::Database(_) => TestLabError::PersistenceFailed,
        TestEvidenceRepositoryError::Crawler(error) => map_crawler_error(&error),
    }
}
