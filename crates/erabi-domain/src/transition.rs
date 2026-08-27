use crate::{DiscoveryTransitionId, PageTypeId, TestEvidenceId};
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionBudget {
    pub max_links_per_source_page: u32,
    pub total_budget: Option<u64>,
    pub depth_contribution: u32,
}
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Validates transition-local authoring fields and budgets.
    ///
    /// # Errors
    /// Returns a typed invalid-transition or invalid-budget error. Cycles and
    /// self-transitions are intentionally not checked here.
    pub fn validate(&self) -> Result<(), crate::ProductError> {
        if self.name.trim().is_empty() || self.name.chars().count() > 256 {
            return Err(crate::ProductError::with_code(
                crate::ErrorCode::InvalidDiscoveryTransition,
                "transition name must be non-empty and at most 256 characters",
            ));
        }
        if self.link_selector.trim().is_empty() || self.link_selector.chars().count() > 1_024 {
            return Err(crate::ProductError::with_code(
                crate::ErrorCode::InvalidDiscoveryTransition,
                "transition link selector must be non-empty and bounded",
            ));
        }
        if let Some(constraints) = &self.url_constraints
            && (constraints.trim().is_empty()
                || constraints.chars().count() > 2_048
                || constraints.chars().any(char::is_control))
        {
            return Err(crate::ProductError::with_code(
                crate::ErrorCode::InvalidDiscoveryTransition,
                "transition URL constraints are invalid",
            ));
        }
        if self.budget.max_links_per_source_page == 0 {
            return Err(crate::ProductError::with_code(
                crate::ErrorCode::InvalidTransitionBudget,
                "transition link budget must be positive",
            ));
        }
        if self.budget.total_budget == Some(0) {
            return Err(crate::ProductError::with_code(
                crate::ErrorCode::InvalidTransitionBudget,
                "transition total budget must be positive when configured",
            ));
        }
        Ok(())
    }
}

/// A validated directed `PageType` graph. The constructor checks references but
/// deliberately does not require the graph to be acyclic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionGraph {
    transitions: Vec<DiscoveryTransition>,
}

impl TransitionGraph {
    /// Builds a graph from one version's `PageTypes` and transitions.
    ///
    /// # Errors
    /// Returns a typed invalid-transition error for duplicate IDs or missing
    /// source/target `PageTypes`. A -> B -> A and self-edges are valid.
    pub fn new(
        page_type_ids: &[PageTypeId],
        transitions: Vec<DiscoveryTransition>,
    ) -> Result<Self, crate::ProductError> {
        let page_type_ids = page_type_ids
            .iter()
            .map(ToString::to_string)
            .collect::<std::collections::BTreeSet<_>>();
        let mut transition_ids = std::collections::BTreeSet::new();
        for transition in &transitions {
            transition.validate()?;
            if !transition_ids.insert(transition.id.to_string()) {
                return Err(crate::ProductError::with_code(
                    crate::ErrorCode::InvalidDiscoveryTransition,
                    "transition IDs must be unique",
                ));
            }
            if !page_type_ids.contains(&transition.source_page_type_id.to_string()) {
                return Err(crate::ProductError::with_code(
                    crate::ErrorCode::InvalidDiscoveryTransition,
                    "transition source PageType is not owned by the version",
                ));
            }
            if !page_type_ids.contains(&transition.target_page_type_id.to_string()) {
                return Err(crate::ProductError::with_code(
                    crate::ErrorCode::InvalidDiscoveryTransition,
                    "transition target PageType is not owned by the version",
                ));
            }
        }
        Ok(Self { transitions })
    }

    #[must_use]
    pub fn transitions(&self) -> &[DiscoveryTransition] {
        &self.transitions
    }
}
