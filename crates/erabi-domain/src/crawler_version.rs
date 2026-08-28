use crate::{
    CanonicalizationPolicy, CanonicalizationPolicyId, CrawlerId, CrawlerVersionGuardrails,
    CrawlerVersionId, DiscoveryTransitionId, DomainScopeId, DomainScopePolicy, PageTypeId,
    ProductError, Seed,
};
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrawlerVersionState {
    Draft,
    Published,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CrawlerVersion {
    id: CrawlerVersionId,
    crawler_id: CrawlerId,
    state: CrawlerVersionState,
    seeds: Vec<Seed>,
    page_type_ids: Vec<PageTypeId>,
    transition_ids: Vec<DiscoveryTransitionId>,
    canonicalization_policy_id: Option<CanonicalizationPolicyId>,
    domain_scope_id: Option<DomainScopeId>,
    #[serde(default)]
    canonicalization_policy: CanonicalizationPolicy,
    #[serde(default)]
    domain_scope: DomainScopePolicy,
    #[serde(default)]
    guardrails: CrawlerVersionGuardrails,
}
impl CrawlerVersion {
    #[must_use]
    pub fn draft(crawler_id: CrawlerId) -> Self {
        Self {
            id: CrawlerVersionId::new(),
            crawler_id,
            state: CrawlerVersionState::Draft,
            seeds: Vec::new(),
            page_type_ids: Vec::new(),
            transition_ids: Vec::new(),
            canonicalization_policy_id: None,
            domain_scope_id: None,
            canonicalization_policy: CanonicalizationPolicy::default(),
            domain_scope: DomainScopePolicy::default(),
            guardrails: CrawlerVersionGuardrails::default(),
        }
    }
    #[must_use]
    pub const fn id(&self) -> CrawlerVersionId {
        self.id
    }
    #[must_use]
    pub const fn crawler_id(&self) -> CrawlerId {
        self.crawler_id
    }
    #[must_use]
    pub const fn state(&self) -> CrawlerVersionState {
        self.state
    }
    #[must_use]
    pub fn seeds(&self) -> &[Seed] {
        &self.seeds
    }
    #[must_use]
    pub fn page_type_ids(&self) -> &[PageTypeId] {
        &self.page_type_ids
    }
    #[must_use]
    pub fn transition_ids(&self) -> &[DiscoveryTransitionId] {
        &self.transition_ids
    }
    #[must_use]
    pub const fn canonicalization_policy_id(&self) -> Option<CanonicalizationPolicyId> {
        self.canonicalization_policy_id
    }
    #[must_use]
    pub const fn domain_scope_id(&self) -> Option<DomainScopeId> {
        self.domain_scope_id
    }
    #[must_use]
    pub const fn canonicalization_policy(&self) -> &CanonicalizationPolicy {
        &self.canonicalization_policy
    }
    #[must_use]
    pub const fn domain_scope(&self) -> &DomainScopePolicy {
        &self.domain_scope
    }
    #[must_use]
    pub const fn guardrails(&self) -> &CrawlerVersionGuardrails {
        &self.guardrails
    }
    fn ensure_draft(&self) -> Result<(), ProductError> {
        if self.state == CrawlerVersionState::Published {
            Err(ProductError::conflict(
                "published crawler versions are immutable",
            ))
        } else {
            Ok(())
        }
    }
    /// Adds a Seed to a Draft configuration.
    ///
    /// # Errors
    /// Returns a conflict when the version is Published.
    pub fn add_seed(&mut self, seed: Seed) -> Result<(), ProductError> {
        self.ensure_draft()?;
        self.seeds.push(seed);
        Ok(())
    }
    /// Replaces Draft Page Type references.
    ///
    /// # Errors
    /// Returns a conflict when the version is Published.
    pub fn set_page_type_ids(&mut self, ids: Vec<PageTypeId>) -> Result<(), ProductError> {
        self.ensure_draft()?;
        if self
            .guardrails
            .page_types
            .iter()
            .any(|budget| !ids.contains(&budget.page_type_id))
        {
            return Err(ProductError::with_code(
                crate::ErrorCode::InvalidPageTypeBudget,
                "PageType guardrail references a PageType outside the version",
            ));
        }
        self.page_type_ids = ids;
        Ok(())
    }
    /// Replaces Draft transition references.
    ///
    /// # Errors
    /// Returns a conflict when the version is Published.
    pub fn set_transition_ids(
        &mut self,
        ids: Vec<DiscoveryTransitionId>,
    ) -> Result<(), ProductError> {
        self.ensure_draft()?;
        self.transition_ids = ids;
        Ok(())
    }
    /// Sets the Draft canonicalization policy reference.
    ///
    /// # Errors
    /// Returns a conflict when the version is Published.
    pub fn set_canonicalization_policy_id(
        &mut self,
        id: Option<CanonicalizationPolicyId>,
    ) -> Result<(), ProductError> {
        self.ensure_draft()?;
        self.canonicalization_policy_id = id;
        Ok(())
    }
    /// Sets the Draft domain-scope reference.
    ///
    /// # Errors
    /// Returns a conflict when the version is Published.
    pub fn set_domain_scope_id(&mut self, id: Option<DomainScopeId>) -> Result<(), ProductError> {
        self.ensure_draft()?;
        self.domain_scope_id = id;
        Ok(())
    }
    /// Replaces the Draft's versioned canonicalization semantics.
    ///
    /// # Errors
    /// Returns a conflict for a Published version or an invalid policy error.
    pub fn set_canonicalization_policy(
        &mut self,
        policy: CanonicalizationPolicy,
    ) -> Result<(), ProductError> {
        self.ensure_draft()?;
        policy.validate()?;
        self.canonicalization_policy = policy;
        Ok(())
    }
    /// Replaces the Draft's versioned Domain Scope semantics.
    ///
    /// # Errors
    /// Returns a conflict for a Published version or an invalid policy error.
    pub fn set_domain_scope(&mut self, policy: DomainScopePolicy) -> Result<(), ProductError> {
        self.ensure_draft()?;
        policy.validate()?;
        self.domain_scope = policy;
        Ok(())
    }
    /// Replaces the Draft's mandatory semantic guardrail baseline.
    ///
    /// # Errors
    /// Returns a conflict for a Published version or an invalid guardrail
    /// contract.
    pub fn set_guardrails(
        &mut self,
        guardrails: CrawlerVersionGuardrails,
    ) -> Result<(), ProductError> {
        self.ensure_draft()?;
        guardrails.validate()?;
        if guardrails
            .page_types
            .iter()
            .any(|budget| !self.page_type_ids.contains(&budget.page_type_id))
        {
            return Err(ProductError::with_code(
                crate::ErrorCode::InvalidPageTypeBudget,
                "PageType guardrail references a PageType outside the version",
            ));
        }
        self.guardrails = guardrails;
        Ok(())
    }
    /// Validates the complete version-level discovery semantic contract.
    ///
    /// # Errors
    /// Returns a typed validation error. Callers loading durable state should
    /// map this failure to their sanitized corruption boundary.
    pub fn validate_semantic_contract(&self) -> Result<(), ProductError> {
        self.canonicalization_policy.validate()?;
        self.domain_scope.validate()?;
        self.guardrails.validate()?;
        if self
            .guardrails
            .page_types
            .iter()
            .any(|budget| !self.page_type_ids.contains(&budget.page_type_id))
        {
            return Err(ProductError::with_code(
                crate::ErrorCode::InvalidPageTypeBudget,
                "PageType guardrail references a PageType outside the version",
            ));
        }
        Ok(())
    }
    /// Publishes a Draft and freezes its configuration.
    ///
    /// # Errors
    /// Returns a conflict when already Published.
    pub fn publish(&mut self) -> Result<(), ProductError> {
        self.ensure_draft()?;
        self.state = CrawlerVersionState::Published;
        Ok(())
    }
    /// Clones an immutable Published version to a new editable Draft identity.
    ///
    /// # Errors
    /// Returns a conflict when this version is not Published.
    pub fn draft_from_published(&self) -> Result<Self, ProductError> {
        self.validate_semantic_contract()?;
        if self.state != CrawlerVersionState::Published {
            return Err(ProductError::conflict(
                "only published crawler versions can create drafts",
            ));
        }
        let source_page_type_ids = self.page_type_ids.clone();
        if self.seeds.iter().any(|seed| {
            seed.entry_page_type_hint
                .is_some_and(|hint| !source_page_type_ids.contains(&hint))
        }) {
            return Err(ProductError::conflict(
                "published seed references a missing Page Type",
            ));
        }
        let mut clone = Self {
            id: CrawlerVersionId::new(),
            crawler_id: self.crawler_id,
            state: CrawlerVersionState::Draft,
            seeds: self.seeds.clone(),
            page_type_ids: self.page_type_ids.clone(),
            transition_ids: self.transition_ids.clone(),
            canonicalization_policy_id: self.canonicalization_policy_id,
            domain_scope_id: self.domain_scope_id,
            canonicalization_policy: self.canonicalization_policy.clone(),
            domain_scope: self.domain_scope.clone(),
            guardrails: self.guardrails.clone(),
        };
        for seed in &mut clone.seeds {
            seed.id = crate::SeedId::new();
        }
        let seed_hint_indexes = clone
            .seeds
            .iter()
            .map(|seed| {
                seed.entry_page_type_hint.and_then(|hint| {
                    source_page_type_ids
                        .iter()
                        .position(|source| *source == hint)
                })
            })
            .collect::<Vec<_>>();
        for page_type_id in &mut clone.page_type_ids {
            *page_type_id = crate::PageTypeId::new();
        }
        for page_budget in &mut clone.guardrails.page_types {
            let Some(index) = source_page_type_ids
                .iter()
                .position(|source| *source == page_budget.page_type_id)
            else {
                return Err(ProductError::with_code(
                    crate::ErrorCode::InvalidPageTypeBudget,
                    "published PageType guardrail references a missing PageType",
                ));
            };
            page_budget.page_type_id = clone.page_type_ids[index];
        }
        for (seed, hint_index) in clone.seeds.iter_mut().zip(seed_hint_indexes) {
            seed.entry_page_type_hint = hint_index.map(|index| clone.page_type_ids[index]);
        }
        for transition_id in &mut clone.transition_ids {
            *transition_id = crate::DiscoveryTransitionId::new();
        }
        Ok(clone)
    }
}
