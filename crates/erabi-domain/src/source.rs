use crate::{ArtifactId, CollectionId, CrawlRunId, SourceId, SourceStatus, SourceTargetType};
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Source {
    pub id: SourceId,
    pub collection_id: Option<CollectionId>,
    pub name: String,
    pub original_url: url::Url,
    pub canonical_url: url::Url,
    pub target_type: SourceTargetType,
    pub status: SourceStatus,
    pub run_ids: Vec<CrawlRunId>,
    pub artifact_ids: Vec<ArtifactId>,
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
            id: SourceId::new(),
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
