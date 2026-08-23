//! API-to-domain construction for immutable robots decisions.

use erabi_domain::{CrawlRunSnapshot, CrawlerVersionId, RobotsAudit, SnapshotError};

/// API input for a new, independent run's robots policy.
///
/// There is intentionally no field for an earlier run or a prior reason. New
/// independent runs cannot adopt historical override context implicitly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RobotsOverrideInput {
    /// Keep the default robots-respecting policy.
    Respect,
    /// Request an override with the exact operator-supplied reason.
    Override { reason: String },
}

/// The complete audit context that must be frozen into a run snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RobotsDecisionContext {
    /// Actor making the decision.
    pub actor: String,
    /// Decision timestamp supplied by the application clock.
    pub decided_at: String,
    /// Affected origin or domain scope.
    pub affected_scope: String,
    /// Active effective User-Agent.
    pub user_agent: String,
    /// Optional crawler-version identity for crawler-backed runs.
    pub crawler_version_id: Option<CrawlerVersionId>,
}

/// Builds the robots audit object for a brand-new run.
///
/// The domain snapshot constructor remains authoritative for validating all
/// frozen context. This adapter only ensures a new override has the required
/// fresh reason and never accepts an earlier run as input.
///
/// # Errors
/// Returns the domain validation error for a blank or overlong override reason.
pub fn new_run_robots_decision(
    input: RobotsOverrideInput,
    context: RobotsDecisionContext,
) -> Result<RobotsAudit, SnapshotError> {
    match input {
        RobotsOverrideInput::Respect => Ok(RobotsAudit::respect(
            context.actor,
            context.decided_at,
            context.affected_scope,
            context.user_agent,
            context.crawler_version_id,
        )),
        RobotsOverrideInput::Override { reason } => RobotsAudit::override_with_reason(
            reason,
            context.actor,
            context.decided_at,
            context.affected_scope,
            context.user_agent,
            context.crawler_version_id,
        ),
    }
}

/// Reuses robots context only for retry/resume of this same immutable snapshot.
#[must_use]
pub fn reuse_frozen_robots_decision(snapshot: &CrawlRunSnapshot) -> &RobotsAudit {
    snapshot.robots()
}
