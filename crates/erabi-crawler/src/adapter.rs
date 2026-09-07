//! Provider-neutral crawling contract.
//!
//! This module deliberately contains no `Crawl4AI` paths, DTOs, headers, or
//! provider configuration. It is the seam between Erabi orchestration and a
//! bounded provider work unit.

use std::{collections::BTreeSet, fmt, future::Future, pin::Pin, sync::Arc, time::Duration};

use url::Url;

use crate::observation::{ObservationValidationError, PageObservation, safe_url_identity};

pub const MAX_CRAWLER_URL_CHARS: usize = 4_096;
pub const MAX_CRAWLER_USER_AGENT_CHARS: usize = 512;
pub const MAX_CRAWLER_SELECTOR_CHARS: usize = 1_024;
pub const MAX_CRAWLER_TIMEOUT_MS: u64 = 900_000;
pub const MAX_CRAWLER_AUTO_SCROLL_STEPS: u16 = 64;
pub const MAX_CRAWLER_DISCOVERED_LINKS: usize = 256;
pub const MAX_CRAWLER_SELECTOR_OBSERVATIONS: usize = 256;
pub const MAX_CRAWLER_PAGINATION_OBSERVATIONS: usize = 64;
pub const MAX_CRAWLER_MEDIA_TYPE_CHARS: usize = 256;
pub const MAX_CRAWLER_PROVIDER_VERSION_CHARS: usize = 128;
pub const MAX_CRAWLER_ARTIFACTS: usize = 5;
pub const MAX_CRAWLER_TEXT_ARTIFACT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CRAWLER_SCREENSHOT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_CRAWLER_TOTAL_ARTIFACT_BYTES: usize = 32 * 1024 * 1024;

/// An object-safe, Tokio-independent future used by adapter implementations.
///
/// The future owns one bounded provider operation. Implementations must not
/// spawn detached provider work; dropping this future must release or cancel
/// owned request work as safely as the implementation permits.
pub type CrawlerFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, CrawlerAdapterError>> + Send + 'a>>;

/// Provider-neutral acquisition seam used by later crawl orchestration.
pub trait CrawlerAdapter: Send + Sync {
    fn health(&self) -> CrawlerFuture<'_, CrawlerHealth>;

    fn execute(&self, request: CrawlerExecuteRequest) -> CrawlerFuture<'_, CrawlerExecuteResult>;
}

/// Stable provider and capability observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlerHealth {
    status: CrawlerHealthStatus,
    provider_version: Option<CrawlerProviderVersion>,
    capabilities: CrawlerCapabilities,
}

impl CrawlerHealth {
    #[must_use]
    pub fn new(
        status: CrawlerHealthStatus,
        provider_version: Option<CrawlerProviderVersion>,
        capabilities: CrawlerCapabilities,
    ) -> Self {
        Self {
            status,
            provider_version,
            capabilities,
        }
    }

    #[must_use]
    pub const fn status(&self) -> CrawlerHealthStatus {
        self.status
    }

    #[must_use]
    pub const fn provider_version(&self) -> Option<&CrawlerProviderVersion> {
        self.provider_version.as_ref()
    }

    #[must_use]
    pub const fn capabilities(&self) -> &CrawlerCapabilities {
        &self.capabilities
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CrawlerHealthStatus {
    Healthy,
    Degraded,
}

/// Fixed acquisition capabilities. This is intentionally not a provider map.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CrawlerCapabilities {
    pub rendered_html: bool,
    pub cleaned_html: bool,
    pub markdown: bool,
    pub screenshot: bool,
    pub wait_for_selector: bool,
    pub bounded_auto_scroll: bool,
    pub discovered_links: bool,
}

/// Bounded provider-version text returned by health observation.
#[derive(Clone, Eq, PartialEq)]
pub struct CrawlerProviderVersion(String);

impl CrawlerProviderVersion {
    /// Creates a bounded provider version without interpreting its contents.
    ///
    /// # Errors
    /// Returns an error for an empty, oversized, or control-containing value.
    pub fn new(value: impl Into<String>) -> Result<Self, CrawlerValueError> {
        let value = value.into();
        validate_bounded_text(
            &value,
            MAX_CRAWLER_PROVIDER_VERSION_CHARS,
            CrawlerValueError::ProviderVersionEmpty,
            CrawlerValueError::ProviderVersionTooLong,
        )?;
        Ok(Self(value.trim().to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CrawlerProviderVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CrawlerProviderVersion")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for CrawlerProviderVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Open, bounded media-type text. It is deliberately not a closed MIME enum.
#[derive(Clone, Eq, PartialEq)]
pub struct CrawlerMediaType(String);

impl CrawlerMediaType {
    /// Creates a normalized, bounded media type.
    ///
    /// # Errors
    /// Returns an error for an empty, oversized, or control-containing value.
    pub fn new(value: impl Into<String>) -> Result<Self, CrawlerValueError> {
        let value = value.into();
        validate_bounded_text(
            &value,
            MAX_CRAWLER_MEDIA_TYPE_CHARS,
            CrawlerValueError::MediaTypeEmpty,
            CrawlerValueError::MediaTypeTooLong,
        )?;
        Ok(Self(value.trim().to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CrawlerMediaType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CrawlerMediaType")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for CrawlerMediaType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CrawlerValueError {
    #[error("provider version is empty")]
    ProviderVersionEmpty,
    #[error("provider version is too long")]
    ProviderVersionTooLong,
    #[error("media type is empty")]
    MediaTypeEmpty,
    #[error("media type is too long")]
    MediaTypeTooLong,
    #[error("value contains a control character")]
    ControlCharacter,
}

/// Rendering capability requested for one bounded provider operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenderingRequirement {
    RawHtml,
    RenderedHtml,
}

/// Explicitly bounded auto-scroll request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AutoScrollPolicy {
    max_steps: u16,
}

impl AutoScrollPolicy {
    /// Creates an auto-scroll policy with at most 64 steps.
    ///
    /// # Errors
    /// Returns an error when the step count is zero or exceeds the fixed cap.
    pub fn new(max_steps: u16) -> Result<Self, CrawlerRequestError> {
        if max_steps == 0 || max_steps > MAX_CRAWLER_AUTO_SCROLL_STEPS {
            return Err(CrawlerRequestError::AutoScrollStepsOutOfRange);
        }
        Ok(Self { max_steps })
    }

    #[must_use]
    pub const fn max_steps(self) -> u16 {
        self.max_steps
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ScreenshotPolicy {
    #[default]
    None,
    Viewport,
    FullPage,
}

/// Fixed acquisition evidence requirements. It contains no provider options.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CrawlerEvidencePolicy {
    pub raw_html: bool,
    pub cleaned_html: bool,
    pub rendered_html: bool,
    pub markdown: bool,
    pub screenshot: ScreenshotPolicy,
    pub discovered_links: bool,
    pub selector_observations: bool,
    pub pagination_observations: bool,
}

/// One validated bounded request. It contains no durable job or provider ID.
#[derive(Clone, Eq, PartialEq)]
pub struct CrawlerExecuteRequest {
    target_url: Url,
    timeout: Duration,
    user_agent: String,
    rendering: RenderingRequirement,
    wait_for_selector: Option<String>,
    auto_scroll: Option<AutoScrollPolicy>,
    evidence: CrawlerEvidencePolicy,
}

impl CrawlerExecuteRequest {
    /// Creates one bounded provider work request.
    ///
    /// # Errors
    /// Returns a request validation error when an acquisition input is outside
    /// the provider-neutral contract.
    pub fn try_new(
        target_url: Url,
        timeout: Duration,
        user_agent: impl Into<String>,
        rendering: RenderingRequirement,
        wait_for_selector: Option<String>,
        auto_scroll: Option<AutoScrollPolicy>,
        evidence: CrawlerEvidencePolicy,
    ) -> Result<Self, CrawlerRequestError> {
        let url_text = target_url.as_str();
        if url_text.chars().count() > MAX_CRAWLER_URL_CHARS
            || url_text.chars().any(char::is_control)
            || target_url.host_str().is_none()
            || !matches!(target_url.scheme(), "http" | "https")
            || !target_url.username().is_empty()
            || target_url.password().is_some()
            || target_url.fragment().is_some()
        {
            return Err(CrawlerRequestError::InvalidHttpUrl);
        }
        if timeout.is_zero() {
            return Err(CrawlerRequestError::TimeoutMustBePositive);
        }
        if timeout > Duration::from_millis(MAX_CRAWLER_TIMEOUT_MS) {
            return Err(CrawlerRequestError::TimeoutTooLong);
        }

        let user_agent = user_agent.into();
        if user_agent.trim().is_empty() {
            return Err(CrawlerRequestError::UserAgentEmpty);
        }
        if user_agent.chars().count() > MAX_CRAWLER_USER_AGENT_CHARS
            || user_agent.chars().any(char::is_control)
        {
            return Err(CrawlerRequestError::UserAgentInvalid);
        }

        if let Some(selector) = &wait_for_selector
            && (selector.trim().is_empty()
                || selector.chars().count() > MAX_CRAWLER_SELECTOR_CHARS
                || selector.chars().any(char::is_control))
        {
            return Err(CrawlerRequestError::WaitForSelectorInvalid);
        }

        Ok(Self {
            target_url,
            timeout,
            user_agent,
            rendering,
            wait_for_selector,
            auto_scroll,
            evidence,
        })
    }

    #[must_use]
    pub const fn target_url(&self) -> &Url {
        &self.target_url
    }

    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    #[must_use]
    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    #[must_use]
    pub const fn rendering(&self) -> RenderingRequirement {
        self.rendering
    }

    #[must_use]
    pub fn wait_for_selector(&self) -> Option<&str> {
        self.wait_for_selector.as_deref()
    }

    #[must_use]
    pub const fn auto_scroll(&self) -> Option<AutoScrollPolicy> {
        self.auto_scroll
    }

    #[must_use]
    pub const fn evidence(&self) -> CrawlerEvidencePolicy {
        self.evidence
    }
}

impl fmt::Debug for CrawlerExecuteRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CrawlerExecuteRequest")
            .field("target_url", &safe_url_identity(self.target_url.as_str()))
            .field("timeout", &self.timeout)
            .field("user_agent", &"<configured>")
            .field("rendering", &self.rendering)
            .field(
                "wait_for_selector_configured",
                &self.wait_for_selector.is_some(),
            )
            .field("auto_scroll", &self.auto_scroll)
            .field("evidence", &self.evidence)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CrawlerRequestError {
    #[error("target URL must be a bounded HTTP(S) URL")]
    InvalidHttpUrl,
    #[error("crawler timeout must be positive")]
    TimeoutMustBePositive,
    #[error("crawler timeout exceeds the bounded maximum")]
    TimeoutTooLong,
    #[error("User-Agent must not be empty")]
    UserAgentEmpty,
    #[error("User-Agent is outside the bounded contract")]
    UserAgentInvalid,
    #[error("wait-for-selector is outside the bounded contract")]
    WaitForSelectorInvalid,
    #[error("auto-scroll steps are outside the bounded contract")]
    AutoScrollStepsOutOfRange,
}

/// Safe fixed response metadata. Headers and bodies never appear here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrawlerResponseMetadata {
    status_code: Option<u16>,
    media_type: Option<CrawlerMediaType>,
    content_length_bytes: Option<u64>,
    provider_elapsed_ms: Option<u64>,
}

impl CrawlerResponseMetadata {
    /// Creates normalized response metadata.
    ///
    /// # Errors
    /// Returns `InvalidProviderResponse` for an impossible HTTP status.
    pub fn try_new(
        status_code: Option<u16>,
        media_type: Option<CrawlerMediaType>,
        content_length_bytes: Option<u64>,
        provider_elapsed_ms: Option<u64>,
    ) -> Result<Self, CrawlerAdapterError> {
        if status_code.is_some_and(|status| !(100..=599).contains(&status)) {
            return Err(CrawlerAdapterError::InvalidProviderResponse);
        }
        Ok(Self {
            status_code,
            media_type,
            content_length_bytes,
            provider_elapsed_ms,
        })
    }

    #[must_use]
    pub const fn status_code(&self) -> Option<u16> {
        self.status_code
    }

    #[must_use]
    pub const fn media_type(&self) -> Option<&CrawlerMediaType> {
        self.media_type.as_ref()
    }

    #[must_use]
    pub const fn content_length_bytes(&self) -> Option<u64> {
        self.content_length_bytes
    }

    #[must_use]
    pub const fn provider_elapsed_ms(&self) -> Option<u64> {
        self.provider_elapsed_ms
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CrawlerArtifactKind {
    RawHtml,
    CleanedHtml,
    RenderedHtml,
    Markdown,
    Screenshot,
}

/// New acquisition evidence owned by the normalized result.
#[derive(Clone, Eq, PartialEq)]
pub enum CrawlerArtifactEvidence {
    RawHtml(Arc<str>),
    CleanedHtml(Arc<str>),
    RenderedHtml(Arc<str>),
    Markdown(Arc<str>),
    Screenshot {
        media_type: CrawlerMediaType,
        bytes: Arc<[u8]>,
    },
}

impl CrawlerArtifactEvidence {
    /// Creates raw HTML evidence.
    ///
    /// # Errors
    /// Returns `InvalidProviderResponse` when the payload exceeds the bounded
    /// text-artifact limit.
    pub fn raw_html(body: impl Into<String>) -> Result<Self, CrawlerAdapterError> {
        Self::text(CrawlerArtifactKind::RawHtml, body.into())
    }

    /// Creates cleaned HTML evidence.
    ///
    /// # Errors
    /// Returns `InvalidProviderResponse` when the payload exceeds the bounded
    /// text-artifact limit.
    pub fn cleaned_html(body: impl Into<String>) -> Result<Self, CrawlerAdapterError> {
        Self::text(CrawlerArtifactKind::CleanedHtml, body.into())
    }

    /// Creates rendered HTML evidence.
    ///
    /// # Errors
    /// Returns `InvalidProviderResponse` when the payload exceeds the bounded
    /// text-artifact limit.
    pub fn rendered_html(body: impl Into<String>) -> Result<Self, CrawlerAdapterError> {
        Self::text(CrawlerArtifactKind::RenderedHtml, body.into())
    }

    /// Creates Markdown evidence.
    ///
    /// # Errors
    /// Returns `InvalidProviderResponse` when the payload exceeds the bounded
    /// text-artifact limit.
    pub fn markdown(body: impl Into<String>) -> Result<Self, CrawlerAdapterError> {
        Self::text(CrawlerArtifactKind::Markdown, body.into())
    }

    /// Creates screenshot evidence.
    ///
    /// # Errors
    /// Returns `InvalidProviderResponse` when the bytes exceed the bounded
    /// screenshot-artifact limit.
    pub fn screenshot(
        media_type: CrawlerMediaType,
        bytes: Vec<u8>,
    ) -> Result<Self, CrawlerAdapterError> {
        if bytes.len() > MAX_CRAWLER_SCREENSHOT_BYTES {
            return Err(CrawlerAdapterError::InvalidProviderResponse);
        }
        Ok(Self::Screenshot {
            media_type,
            bytes: Arc::from(bytes),
        })
    }

    #[must_use]
    pub const fn kind(&self) -> CrawlerArtifactKind {
        match self {
            Self::RawHtml(_) => CrawlerArtifactKind::RawHtml,
            Self::CleanedHtml(_) => CrawlerArtifactKind::CleanedHtml,
            Self::RenderedHtml(_) => CrawlerArtifactKind::RenderedHtml,
            Self::Markdown(_) => CrawlerArtifactKind::Markdown,
            Self::Screenshot { .. } => CrawlerArtifactKind::Screenshot,
        }
    }

    #[must_use]
    pub fn byte_len(&self) -> usize {
        match self {
            Self::RawHtml(body)
            | Self::CleanedHtml(body)
            | Self::RenderedHtml(body)
            | Self::Markdown(body) => body.len(),
            Self::Screenshot { bytes, .. } => bytes.len(),
        }
    }

    fn text(kind: CrawlerArtifactKind, body: String) -> Result<Self, CrawlerAdapterError> {
        if body.len() > MAX_CRAWLER_TEXT_ARTIFACT_BYTES {
            return Err(CrawlerAdapterError::InvalidProviderResponse);
        }
        let body: Arc<str> = Arc::from(body);
        Ok(match kind {
            CrawlerArtifactKind::RawHtml => Self::RawHtml(body),
            CrawlerArtifactKind::CleanedHtml => Self::CleanedHtml(body),
            CrawlerArtifactKind::RenderedHtml => Self::RenderedHtml(body),
            CrawlerArtifactKind::Markdown => Self::Markdown(body),
            CrawlerArtifactKind::Screenshot => {
                return Err(CrawlerAdapterError::InvalidProviderResponse);
            }
        })
    }
}

impl fmt::Debug for CrawlerArtifactEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CrawlerArtifactEvidence")
            .field("kind", &self.kind())
            .field("byte_len", &self.byte_len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CrawlerPartialReason {
    ProviderReportedPartial,
    RequestedEvidenceUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CrawlerResultCompleteness {
    Complete,
    Partial { reason: CrawlerPartialReason },
}

impl CrawlerResultCompleteness {
    #[must_use]
    pub const fn from_flags(
        provider_reported_partial: bool,
        requested_evidence_unavailable: bool,
    ) -> Self {
        if provider_reported_partial {
            Self::Partial {
                reason: CrawlerPartialReason::ProviderReportedPartial,
            }
        } else if requested_evidence_unavailable {
            Self::Partial {
                reason: CrawlerPartialReason::RequestedEvidenceUnavailable,
            }
        } else {
            Self::Complete
        }
    }
}

/// Normalized evidence from one bounded provider work unit.
#[derive(Clone, Eq, PartialEq)]
pub struct CrawlerExecuteResult {
    observation: PageObservation,
    response: CrawlerResponseMetadata,
    artifacts: Vec<CrawlerArtifactEvidence>,
    completeness: CrawlerResultCompleteness,
}

impl CrawlerExecuteResult {
    /// Builds and validates a result. Missing requested artifact evidence is
    /// represented as a deterministic partial outcome, never as truncation.
    ///
    /// # Errors
    /// Returns `InvalidProviderResponse` for request mismatches, invalid
    /// observations, duplicate/oversized evidence, or impossible metadata.
    pub fn try_new(
        request: &CrawlerExecuteRequest,
        observation: PageObservation,
        response: CrawlerResponseMetadata,
        artifacts: Vec<CrawlerArtifactEvidence>,
        provider_reported_partial: bool,
    ) -> Result<Self, CrawlerAdapterError> {
        if observation.requested_url != request.target_url.as_str()
            || !observation.artifact_ids.is_empty()
        {
            return Err(CrawlerAdapterError::InvalidProviderResponse);
        }
        observation
            .validate_for_adapter()
            .map_err(|_: ObservationValidationError| {
                CrawlerAdapterError::InvalidProviderResponse
            })?;
        if let Some(final_url) = observation.final_url.as_deref() {
            validate_absolute_http_url(final_url)?;
        }
        if artifacts.len() > MAX_CRAWLER_ARTIFACTS {
            return Err(CrawlerAdapterError::InvalidProviderResponse);
        }

        let mut kinds = BTreeSet::new();
        let mut total_bytes = 0_usize;
        for artifact in &artifacts {
            if !kinds.insert(artifact.kind()) {
                return Err(CrawlerAdapterError::InvalidProviderResponse);
            }
            let limit = match artifact.kind() {
                CrawlerArtifactKind::Screenshot => MAX_CRAWLER_SCREENSHOT_BYTES,
                CrawlerArtifactKind::RawHtml
                | CrawlerArtifactKind::CleanedHtml
                | CrawlerArtifactKind::RenderedHtml
                | CrawlerArtifactKind::Markdown => MAX_CRAWLER_TEXT_ARTIFACT_BYTES,
            };
            if artifact.byte_len() > limit {
                return Err(CrawlerAdapterError::InvalidProviderResponse);
            }
            total_bytes = total_bytes
                .checked_add(artifact.byte_len())
                .ok_or(CrawlerAdapterError::InvalidProviderResponse)?;
        }
        if total_bytes > MAX_CRAWLER_TOTAL_ARTIFACT_BYTES {
            return Err(CrawlerAdapterError::InvalidProviderResponse);
        }

        let requested_evidence_unavailable =
            requested_evidence_unavailable(request.evidence, &kinds);
        let completeness = CrawlerResultCompleteness::from_flags(
            provider_reported_partial,
            requested_evidence_unavailable,
        );
        Ok(Self {
            observation,
            response,
            artifacts,
            completeness,
        })
    }

    /// Revalidates a fixture or provider-owned result against its request.
    ///
    /// # Errors
    /// Returns `InvalidProviderResponse` when the result violates the seam.
    pub fn validate_for(&self, request: &CrawlerExecuteRequest) -> Result<(), CrawlerAdapterError> {
        let rebuilt = Self::try_new(
            request,
            self.observation.clone(),
            self.response.clone(),
            self.artifacts.clone(),
            matches!(
                self.completeness,
                CrawlerResultCompleteness::Partial {
                    reason: CrawlerPartialReason::ProviderReportedPartial
                }
            ),
        )?;
        (rebuilt.completeness == self.completeness)
            .then_some(())
            .ok_or(CrawlerAdapterError::InvalidProviderResponse)
    }

    #[must_use]
    pub const fn observation(&self) -> &PageObservation {
        &self.observation
    }

    #[must_use]
    pub const fn response(&self) -> &CrawlerResponseMetadata {
        &self.response
    }

    #[must_use]
    pub fn artifacts(&self) -> &[CrawlerArtifactEvidence] {
        &self.artifacts
    }

    #[must_use]
    pub const fn completeness(&self) -> CrawlerResultCompleteness {
        self.completeness
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        PageObservation,
        CrawlerResponseMetadata,
        Vec<CrawlerArtifactEvidence>,
        CrawlerResultCompleteness,
    ) {
        (
            self.observation,
            self.response,
            self.artifacts,
            self.completeness,
        )
    }
}

impl fmt::Debug for CrawlerExecuteResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CrawlerExecuteResult")
            .field(
                "requested_url",
                &safe_url_identity(&self.observation.requested_url),
            )
            .field(
                "final_url",
                &self.observation.final_url.as_deref().map(safe_url_identity),
            )
            .field("response", &self.response)
            .field("artifact_count", &self.artifacts.len())
            .field(
                "artifact_kinds",
                &self
                    .artifacts
                    .iter()
                    .map(CrawlerArtifactEvidence::kind)
                    .collect::<Vec<_>>(),
            )
            .field(
                "artifact_sizes_bytes",
                &self
                    .artifacts
                    .iter()
                    .map(CrawlerArtifactEvidence::byte_len)
                    .collect::<Vec<_>>(),
            )
            .field(
                "discovered_link_count",
                &self.observation.discovered_links.len(),
            )
            .field(
                "selector_observation_count",
                &self.observation.selector_observations.len(),
            )
            .field(
                "pagination_observation_count",
                &self.observation.pagination_observations.len(),
            )
            .field("completeness", &self.completeness)
            .finish()
    }
}

/// Stable categories for provider and target failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CrawlerAdapterError {
    #[error("crawler provider is unavailable")]
    Unavailable,
    #[error("crawler request timed out")]
    Timeout,
    #[error("crawler target access was denied")]
    AccessDenied,
    #[error("crawler target was not found")]
    NotFound,
    #[error("crawler target was rate limited")]
    RateLimited { retry_after_ms: Option<u64> },
    #[error("crawler target returned a remote failure")]
    RemoteFailure { status_code: Option<u16> },
    #[error("crawler provider does not support the requested capability")]
    UnsupportedCapability,
    #[error("crawler provider returned an invalid response")]
    InvalidProviderResponse,
    #[error("crawler operation was cancelled")]
    Cancelled,
}

fn validate_bounded_text(
    value: &str,
    max_chars: usize,
    empty_error: CrawlerValueError,
    too_long_error: CrawlerValueError,
) -> Result<(), CrawlerValueError> {
    if value.trim().is_empty() {
        return Err(empty_error);
    }
    if value.chars().count() > max_chars {
        return Err(too_long_error);
    }
    if value.chars().any(char::is_control) {
        return Err(CrawlerValueError::ControlCharacter);
    }
    Ok(())
}

fn validate_absolute_http_url(value: &str) -> Result<(), CrawlerAdapterError> {
    if value.chars().count() > MAX_CRAWLER_URL_CHARS || value.chars().any(char::is_control) {
        return Err(CrawlerAdapterError::InvalidProviderResponse);
    }
    let parsed = Url::parse(value).map_err(|_| CrawlerAdapterError::InvalidProviderResponse)?;
    if parsed.host_str().is_none()
        || !matches!(parsed.scheme(), "http" | "https")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err(CrawlerAdapterError::InvalidProviderResponse);
    }
    Ok(())
}

fn requested_evidence_unavailable(
    policy: CrawlerEvidencePolicy,
    kinds: &BTreeSet<CrawlerArtifactKind>,
) -> bool {
    (policy.raw_html && !kinds.contains(&CrawlerArtifactKind::RawHtml))
        || (policy.cleaned_html && !kinds.contains(&CrawlerArtifactKind::CleanedHtml))
        || (policy.rendered_html && !kinds.contains(&CrawlerArtifactKind::RenderedHtml))
        || (policy.markdown && !kinds.contains(&CrawlerArtifactKind::Markdown))
        || (policy.screenshot != ScreenshotPolicy::None
            && !kinds.contains(&CrawlerArtifactKind::Screenshot))
}
