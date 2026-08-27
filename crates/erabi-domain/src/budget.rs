use crate::{
    CrawlerVersionGuardrails, ErrorCode, PageTypeDiscoveryGuardrails, ProductError,
    TransitionBudget,
};

/// The pure input for one prospective page/link scheduling decision.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DiscoveryBudgetCandidate {
    pub pages_already_scheduled: u64,
    pub current_depth: u32,
    pub elapsed_duration_seconds: u64,
    pub downloaded_bytes: u64,
    pub page_type_pages: u64,
    pub transition_links_on_source_page: u32,
    pub transition_total_links: u64,
    pub prospective_download_bytes: u64,
    pub depth_contribution: u32,
}

/// Typed reasons why a prospective decision is preserve-only/excluded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiscoveryBudgetExclusion {
    MaxPages,
    MaxDuration,
    MaxDepth,
    MaxDownloadedBytes,
    PageTypePageBudget,
    TransitionPerPageLinkLimit,
    TransitionTotalBudget,
}

/// A bounded pure policy result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiscoveryBudgetDecision {
    Allowed,
    Excluded(DiscoveryBudgetExclusion),
}

/// Arithmetic/configuration failures are distinct from ordinary budget hits.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DiscoveryBudgetError {
    #[error("invalid crawler guardrails")]
    InvalidGuardrails,
    #[error("invalid transition budget")]
    InvalidTransitionBudget,
    #[error("budget arithmetic overflow")]
    Overflow,
}

/// Evaluates one candidate against crawler, `PageType`, and transition caps.
pub struct DiscoveryBudgetEvaluator<'a> {
    guardrails: &'a CrawlerVersionGuardrails,
    page_type: Option<&'a PageTypeDiscoveryGuardrails>,
    transition: Option<&'a TransitionBudget>,
}

impl<'a> DiscoveryBudgetEvaluator<'a> {
    #[must_use]
    pub const fn new(
        guardrails: &'a CrawlerVersionGuardrails,
        page_type: Option<&'a PageTypeDiscoveryGuardrails>,
        transition: Option<&'a TransitionBudget>,
    ) -> Self {
        Self {
            guardrails,
            page_type,
            transition,
        }
    }

    /// Returns ALLOWED or the first deterministic typed exclusion.
    ///
    /// # Errors
    /// Returns overflow or malformed mandatory/transition configuration. It
    /// never treats malformed state as unlimited.
    pub fn evaluate(
        &self,
        candidate: DiscoveryBudgetCandidate,
    ) -> Result<DiscoveryBudgetDecision, DiscoveryBudgetError> {
        self.guardrails
            .validate()
            .map_err(|_| DiscoveryBudgetError::InvalidGuardrails)?;
        if let Some(page_type) = self.page_type
            && page_type.page_budget == Some(0)
        {
            return Err(DiscoveryBudgetError::InvalidGuardrails);
        }
        if let Some(transition) = self.transition
            && (transition.max_links_per_source_page == 0 || transition.total_budget == Some(0))
        {
            return Err(DiscoveryBudgetError::InvalidTransitionBudget);
        }

        if candidate.pages_already_scheduled >= self.guardrails.max_pages {
            return Ok(DiscoveryBudgetDecision::Excluded(
                DiscoveryBudgetExclusion::MaxPages,
            ));
        }
        if candidate.elapsed_duration_seconds >= self.guardrails.max_duration_seconds {
            return Ok(DiscoveryBudgetDecision::Excluded(
                DiscoveryBudgetExclusion::MaxDuration,
            ));
        }
        let prospective_depth = candidate
            .current_depth
            .checked_add(candidate.depth_contribution)
            .ok_or(DiscoveryBudgetError::Overflow)?;
        if prospective_depth > self.guardrails.max_depth {
            return Ok(DiscoveryBudgetDecision::Excluded(
                DiscoveryBudgetExclusion::MaxDepth,
            ));
        }
        let prospective_bytes = candidate
            .downloaded_bytes
            .checked_add(candidate.prospective_download_bytes)
            .ok_or(DiscoveryBudgetError::Overflow)?;
        if prospective_bytes > self.guardrails.max_downloaded_bytes {
            return Ok(DiscoveryBudgetDecision::Excluded(
                DiscoveryBudgetExclusion::MaxDownloadedBytes,
            ));
        }
        if let Some(page_budget) = self.page_type.and_then(|page_type| page_type.page_budget)
            && candidate.page_type_pages >= page_budget
        {
            return Ok(DiscoveryBudgetDecision::Excluded(
                DiscoveryBudgetExclusion::PageTypePageBudget,
            ));
        }
        if let Some(transition) = self.transition {
            if candidate.transition_links_on_source_page >= transition.max_links_per_source_page {
                return Ok(DiscoveryBudgetDecision::Excluded(
                    DiscoveryBudgetExclusion::TransitionPerPageLinkLimit,
                ));
            }
            if let Some(total_budget) = transition.total_budget
                && candidate.transition_total_links >= total_budget
            {
                return Ok(DiscoveryBudgetDecision::Excluded(
                    DiscoveryBudgetExclusion::TransitionTotalBudget,
                ));
            }
            candidate
                .transition_links_on_source_page
                .checked_add(1)
                .ok_or(DiscoveryBudgetError::Overflow)?;
            candidate
                .transition_total_links
                .checked_add(1)
                .ok_or(DiscoveryBudgetError::Overflow)?;
        }
        candidate
            .pages_already_scheduled
            .checked_add(1)
            .ok_or(DiscoveryBudgetError::Overflow)?;
        Ok(DiscoveryBudgetDecision::Allowed)
    }
}

impl From<DiscoveryBudgetError> for ProductError {
    fn from(error: DiscoveryBudgetError) -> Self {
        match error {
            DiscoveryBudgetError::Overflow => ProductError::with_code(
                ErrorCode::BudgetOverflow,
                "budget arithmetic exceeded its safe integer range",
            ),
            DiscoveryBudgetError::InvalidGuardrails => ProductError::with_code(
                ErrorCode::InvalidCrawlGuardrails,
                "crawler guardrails are invalid",
            ),
            DiscoveryBudgetError::InvalidTransitionBudget => ProductError::with_code(
                ErrorCode::InvalidTransitionBudget,
                "transition budget is invalid",
            ),
        }
    }
}
