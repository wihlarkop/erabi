use crate::{ErrorCode, PageTypeId, ProductError};

/// The currently supported guardrail representation.
pub const GUARDRAIL_POLICY_VERSION: u16 = 1;

/// A future extraction-health contract placeholder. It carries no extraction
/// metric; extraction and validation semantics are intentionally deferred.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum DeferredPageTypeHealth {
    DeferredExtractionHealth { version: u16 },
}

/// Versioned `PageType` guardrails owned by the crawler semantic contract.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageTypeDiscoveryGuardrails {
    pub page_type_id: PageTypeId,
    pub page_budget: Option<u64>,
    pub health_threshold: Option<DeferredPageTypeHealth>,
}

/// Mandatory version-level safety baseline. Run Profiles and per-run values
/// remain operational inputs, but their effective values must not exceed this
/// semantic baseline when execution is added later.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrawlerVersionGuardrails {
    pub version: u16,
    pub max_pages: u64,
    pub max_depth: u32,
    pub max_duration_seconds: u64,
    pub max_downloaded_bytes: u64,
    pub max_concurrent_requests_per_domain: u32,
    pub min_request_delay_ms: u64,
    #[serde(default)]
    pub page_types: Vec<PageTypeDiscoveryGuardrails>,
}

impl Default for CrawlerVersionGuardrails {
    fn default() -> Self {
        Self {
            version: GUARDRAIL_POLICY_VERSION,
            max_pages: 1_000,
            max_depth: 16,
            max_duration_seconds: 3_600,
            max_downloaded_bytes: 100 * 1024 * 1024,
            max_concurrent_requests_per_domain: 1,
            min_request_delay_ms: 100,
            page_types: Vec::new(),
        }
    }
}

impl CrawlerVersionGuardrails {
    /// Validates the mandatory baseline and optional `PageType` budgets.
    ///
    /// # Errors
    /// Returns a typed error; zero mandatory caps never mean unlimited.
    pub fn validate(&self) -> Result<(), ProductError> {
        if self.version != GUARDRAIL_POLICY_VERSION
            || self.max_pages == 0
            || self.max_depth == 0
            || self.max_duration_seconds == 0
            || self.max_downloaded_bytes == 0
            || self.max_concurrent_requests_per_domain == 0
        {
            return Err(ProductError::with_code(
                ErrorCode::InvalidCrawlGuardrails,
                "mandatory crawler guardrails must be valid and positive",
            ));
        }
        let mut page_type_ids = std::collections::BTreeSet::new();
        for page_type in &self.page_types {
            if !page_type_ids.insert(page_type.page_type_id.to_string())
                || page_type.page_budget == Some(0)
            {
                return Err(ProductError::with_code(
                    ErrorCode::InvalidPageTypeBudget,
                    "PageType budgets must be positive and unique",
                ));
            }
            if let Some(DeferredPageTypeHealth::DeferredExtractionHealth { version }) =
                &page_type.health_threshold
                && *version != 1
            {
                return Err(ProductError::with_code(
                    ErrorCode::InvalidPageTypeBudget,
                    "the PageType health contract version is unsupported",
                ));
            }
        }
        Ok(())
    }

    /// Validates an already-resolved operational run limit against this
    /// version's semantic safety baseline. The existing operational-layer
    /// precedence still resolves the value first; this method only prevents
    /// an operational override from widening the safe contract.
    ///
    /// # Errors
    /// Returns an invalid-guardrails error when resolved operational values
    /// widen a mandatory semantic cap or reduce the semantic delay.
    pub fn validate_effective_operational_limits(
        &self,
        limits: &ResolvedOperationalLimits,
    ) -> Result<(), ProductError> {
        self.validate()?;
        if limits.max_pages > self.max_pages
            || limits.max_depth > self.max_depth
            || limits.max_duration_seconds > self.max_duration_seconds
            || limits.max_downloaded_bytes > self.max_downloaded_bytes
            || limits.concurrency > self.max_concurrent_requests_per_domain
            || limits.request_delay_ms < self.min_request_delay_ms
        {
            return Err(ProductError::with_code(
                ErrorCode::InvalidCrawlGuardrails,
                "resolved operational limits exceed the crawler safety baseline",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn page_type(&self, page_type_id: PageTypeId) -> Option<&PageTypeDiscoveryGuardrails> {
        self.page_types
            .iter()
            .find(|budget| budget.page_type_id == page_type_id)
    }
}

/// Values after the per-run → profile → crawler → collection → global
/// → built-in resolution. These are operational and may be lower than the
/// semantic baseline (or use a safer delay), but may never widen it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedOperationalLimits {
    pub max_pages: u64,
    pub max_depth: u32,
    pub max_duration_seconds: u64,
    pub max_downloaded_bytes: u64,
    pub concurrency: u32,
    pub request_delay_ms: u64,
}
