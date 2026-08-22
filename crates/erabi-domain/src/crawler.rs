use crate::{CrawlerVersion, CrawlerVersionState, EntityId, ProductError};
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Crawler {
    pub id: EntityId,
    pub name: String,
    pub collection_id: Option<EntityId>,
    pub active_published_version_id: Option<EntityId>,
    pub active_draft_version_id: Option<EntityId>,
}
impl Crawler {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            id: EntityId::new(),
            name: name.into(),
            collection_id: None,
            active_published_version_id: None,
            active_draft_version_id: None,
        }
    }

    /// Activates the sole editable version for this crawler.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the version is not this crawler's Draft or another Draft is active.
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

    /// Makes an immutable Published version the active production pointer.
    ///
    /// # Errors
    ///
    /// Returns a conflict when the version is not this crawler's Published version.
    pub fn reactivate_published(&mut self, version: &CrawlerVersion) -> Result<(), ProductError> {
        if version.crawler_id() != self.id || version.state() != CrawlerVersionState::Published {
            return Err(ProductError::conflict(
                "published version does not belong to this crawler",
            ));
        }
        self.active_published_version_id = Some(version.id());
        Ok(())
    }
}
