use crate::{EntityId, SourceStatus, SourceTargetType};
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Source {
    pub id: EntityId,
    pub collection_id: Option<EntityId>,
    pub name: String,
    pub original_url: url::Url,
    pub canonical_url: url::Url,
    pub target_type: SourceTargetType,
    pub status: SourceStatus,
    pub run_ids: Vec<EntityId>,
    pub artifact_ids: Vec<EntityId>,
}
impl Source {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        original_url: url::Url,
        canonical_url: url::Url,
        target_type: SourceTargetType,
    ) -> Self {
        Self {
            id: EntityId::new(),
            collection_id: None,
            name: name.into(),
            original_url,
            canonical_url,
            target_type,
            status: SourceStatus::Active,
            run_ids: Vec::new(),
            artifact_ids: Vec::new(),
        }
    }
    #[must_use]
    pub fn canonical_url(&self) -> &url::Url {
        &self.canonical_url
    }
}
