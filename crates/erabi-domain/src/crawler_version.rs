use crate::{
    CanonicalizationPolicyId, CrawlerId, CrawlerVersionId, DiscoveryTransitionId, DomainScopeId,
    OperationalOverrides, PageTypeId, ProductError, Seed,
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
    operational_defaults: OperationalOverrides,
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
            operational_defaults: OperationalOverrides::default(),
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
    pub const fn operational_defaults(&self) -> &OperationalOverrides {
        &self.operational_defaults
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
    /// Sets Draft crawler-level operational defaults.
    ///
    /// # Errors
    /// Returns a conflict when the version is Published.
    pub fn set_operational_defaults(
        &mut self,
        defaults: OperationalOverrides,
    ) -> Result<(), ProductError> {
        self.ensure_draft()?;
        self.operational_defaults = defaults;
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
        if self.state != CrawlerVersionState::Published {
            return Err(ProductError::conflict(
                "only published crawler versions can create drafts",
            ));
        }
        let mut clone = self.clone();
        clone.id = CrawlerVersionId::new();
        clone.state = CrawlerVersionState::Draft;
        Ok(clone)
    }
}
