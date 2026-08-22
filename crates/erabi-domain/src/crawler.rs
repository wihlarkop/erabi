use crate::{
    CollectionId, CrawlerId, CrawlerVersion, CrawlerVersionId, CrawlerVersionState, ProductError,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Crawler {
    id: CrawlerId,
    pub name: String,
    collection_id: Option<CollectionId>,
    active_published_version_id: Option<CrawlerVersionId>,
    active_draft_version_id: Option<CrawlerVersionId>,
}
impl Crawler {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            id: CrawlerId::new(),
            name: name.into(),
            collection_id: None,
            active_published_version_id: None,
            active_draft_version_id: None,
        }
    }
    #[must_use]
    pub const fn id(&self) -> CrawlerId {
        self.id
    }
    #[must_use]
    pub const fn collection_id(&self) -> Option<CollectionId> {
        self.collection_id
    }
    #[must_use]
    pub const fn active_draft_version_id(&self) -> Option<CrawlerVersionId> {
        self.active_draft_version_id
    }
    #[must_use]
    pub const fn active_published_version_id(&self) -> Option<CrawlerVersionId> {
        self.active_published_version_id
    }
    /// Activates the sole editable version for this crawler.
    ///
    /// # Errors
    /// Returns a conflict when the Draft does not belong to this crawler or another Draft is active.
    pub fn activate_draft(&mut self, version: &CrawlerVersion) -> Result<(), ProductError> {
        if version.crawler_id() != self.id || version.state() != CrawlerVersionState::Draft {
            return Err(ProductError::conflict(
                "draft version does not belong to this crawler",
            ));
        }
        if self
            .active_draft_version_id
            .is_some_and(|id| id != version.id())
        {
            return Err(ProductError::conflict(
                "crawler already has an active draft",
            ));
        }
        self.active_draft_version_id = Some(version.id());
        Ok(())
    }
    /// Reactivates an immutable Published version.
    ///
    /// # Errors
    /// Returns a conflict when the Published version does not belong to this crawler.
    pub fn reactivate_published(&mut self, version: &CrawlerVersion) -> Result<(), ProductError> {
        if version.crawler_id() != self.id || version.state() != CrawlerVersionState::Published {
            return Err(ProductError::conflict(
                "published version does not belong to this crawler",
            ));
        }
        if self.active_draft_version_id == Some(version.id()) {
            self.active_draft_version_id = None;
        }
        self.active_published_version_id = Some(version.id());
        Ok(())
    }
}
