use crate::{EntityId, OperationalOverrides, ProductError, Seed};
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrawlerVersionState {
    Draft,
    Published,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CrawlerVersion {
    pub id: EntityId,
    pub crawler_id: EntityId,
    pub state: CrawlerVersionState,
    pub seeds: Vec<Seed>,
    pub page_type_ids: Vec<EntityId>,
    pub transition_ids: Vec<EntityId>,
    pub canonicalization_policy_id: Option<EntityId>,
    pub domain_scope_id: Option<EntityId>,
    pub operational_defaults: OperationalOverrides,
}
impl CrawlerVersion {
    #[must_use]
    pub fn draft(crawler_id: EntityId) -> Self {
        Self {
            id: EntityId::new(),
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
    pub fn fixture_published() -> Self {
        let mut version = Self::draft(EntityId::new());
        version.state = CrawlerVersionState::Published;
        version
    }
    /// Adds explicit versioned Seed configuration to a Draft.
    ///
    /// # Errors
    ///
    /// Returns a conflict when this version is Published.
    pub fn add_seed(&mut self, seed: Seed) -> Result<(), ProductError> {
        if self.state == CrawlerVersionState::Published {
            return Err(ProductError::conflict(
                "published crawler versions are immutable",
            ));
        }
        self.seeds.push(seed);
        Ok(())
    }

    /// Transitions a Draft version to its immutable Published state.
    ///
    /// # Errors
    ///
    /// Returns a conflict when this version is already Published.
    pub fn publish(&mut self) -> Result<(), ProductError> {
        if self.state == CrawlerVersionState::Published {
            return Err(ProductError::conflict(
                "published crawler versions are immutable",
            ));
        }
        self.state = CrawlerVersionState::Published;
        Ok(())
    }
    #[must_use]
    pub fn draft_from_published(&self) -> Self {
        let mut clone = self.clone();
        clone.id = EntityId::new();
        clone.state = CrawlerVersionState::Draft;
        clone
    }
}
