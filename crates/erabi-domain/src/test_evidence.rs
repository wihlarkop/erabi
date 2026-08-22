use crate::{ArtifactId, CrawlerVersionId, PageTypeId, TestEvidenceId};
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TestEvidence {
    pub id: TestEvidenceId,
    pub crawler_version_id: CrawlerVersionId,
    pub test_type: String,
    pub input_urls: Vec<url::Url>,
    pub evaluated_page_type_id: Option<PageTypeId>,
    pub match_summary: String,
    pub extraction_summary: String,
    pub discovery_summary: String,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    pub artifact_ids: Vec<ArtifactId>,
    pub config_hash: String,
    pub executed_at: String,
}
