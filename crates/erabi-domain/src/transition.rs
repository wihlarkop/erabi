use crate::{DiscoveryTransitionId, PageTypeId, TestEvidenceId};
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TransitionBudget {
    pub max_links_per_source_page: u32,
    pub total_budget: Option<u64>,
    pub depth_contribution: u32,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DiscoveryTransition {
    pub id: DiscoveryTransitionId,
    pub source_page_type_id: PageTypeId,
    pub target_page_type_id: PageTypeId,
    pub name: String,
    pub enabled: bool,
    pub link_selector: String,
    pub url_constraints: Option<String>,
    pub priority: i32,
    pub budget: TransitionBudget,
    pub deduplicate: bool,
    pub latest_test_evidence_id: Option<TestEvidenceId>,
}
impl DiscoveryTransition {
    /// Validates transition-local budget guardrails.
    ///
    /// # Errors
    ///
    /// Returns a conflict when no per-page link budget is configured.
    pub fn validate(&self) -> Result<(), crate::ProductError> {
        if self.budget.max_links_per_source_page == 0 {
            return Err(crate::ProductError::conflict(
                "transition link budget must be positive",
            ));
        }
        Ok(())
    }
}
