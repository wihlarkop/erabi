use std::{fmt, future::Future, pin::Pin, sync::Arc};

use erabi_db::repositories::{NewSource, SourceRepository, SourceRepositoryError};
use erabi_domain::{
    CanonicalizationPolicy, CollectionId, ProductError, Source, derive_source_name,
};

use crate::observation::safe_url_identity;
use crate::{
    ContentProbe, ContentProbeDecision, NetworkTargetError, NetworkTargetPolicy,
    ValidatedNetworkTarget,
};

const MAX_GENERATED_SOURCE_NAME_CHARS: usize = 512;

pub type ContentProbeFuture<'probe> =
    Pin<Box<dyn Future<Output = ContentProbeDecision> + Send + 'probe>>;

/// The probe execution seam keeps Source intake deterministic without coupling
/// the durable Source repository to HTTP or provider DTOs.
pub trait ContentProbeExecutor: Send + Sync {
    fn probe<'probe>(
        &'probe self,
        target: &'probe ValidatedNetworkTarget,
    ) -> ContentProbeFuture<'probe>;
}

impl ContentProbeExecutor for ContentProbe {
    fn probe<'probe>(
        &'probe self,
        target: &'probe ValidatedNetworkTarget,
    ) -> ContentProbeFuture<'probe> {
        Box::pin(ContentProbe::probe(self, target))
    }
}

/// A validated Source-intake request. The original URL remains provenance;
/// canonicalization supplies only durable identity and the probe target.
#[derive(Clone)]
pub struct SourceIntakeRequest {
    pub original_url: String,
    pub collection_id: Option<CollectionId>,
    pub name: Option<String>,
}

impl fmt::Debug for SourceIntakeRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceIntakeRequest")
            .field("original_url", &safe_url_identity(&self.original_url))
            .field("collection_id", &self.collection_id)
            .field("name", &self.name)
            .finish()
    }
}

impl SourceIntakeRequest {
    #[must_use]
    pub fn new(original_url: impl Into<String>, collection_id: Option<CollectionId>) -> Self {
        Self {
            original_url: original_url.into(),
            collection_id,
            name: None,
        }
    }
}

/// The durable Source and transient pre-crawl route returned by intake.
#[derive(Clone)]
pub struct SourceIntakeResult {
    pub source: Source,
    pub original_url: String,
    pub canonical_url: url::Url,
    pub decision: ContentProbeDecision,
}

impl fmt::Debug for SourceIntakeResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceIntakeResult")
            .field("source_id", &self.source.id)
            .field("collection_id", &self.source.collection_id)
            .field("source_target_type", &self.source.target_type)
            .field("source_status", &self.source.status)
            .field("original_url", &safe_url_identity(&self.original_url))
            .field(
                "canonical_url",
                &safe_url_identity(self.canonical_url.as_str()),
            )
            .field("decision", &self.decision)
            .finish()
    }
}

/// Typed Source-intake failures. Network policy rejection is deliberately
/// separate from ordinary probe uncertainty, which returns a web-crawl route.
#[derive(Debug, thiserror::Error)]
pub enum SourceIntakeError {
    #[error("Source URL canonicalization failed: {0}")]
    Canonicalization(#[source] ProductError),
    #[error("Source outbound target rejected by network policy: {0}")]
    NetworkTarget(#[source] NetworkTargetError),
    #[error("Source persistence failed: {0}")]
    Repository(#[source] SourceRepositoryError),
}

/// Reusable internal Source intake service for Quick Scrape and later run
/// orchestration consumers.
pub struct SourceIntakeService<'database> {
    source_repository: SourceRepository<'database>,
    network_policy: NetworkTargetPolicy,
    probe: Arc<dyn ContentProbeExecutor>,
    canonicalization_policy: CanonicalizationPolicy,
}

impl std::fmt::Debug for SourceIntakeService<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SourceIntakeService")
            .field("network_policy", &self.network_policy)
            .field("canonicalization_policy", &self.canonicalization_policy)
            .finish_non_exhaustive()
    }
}

impl<'database> SourceIntakeService<'database> {
    #[must_use]
    pub fn new(
        database: &'database erabi_db::ErabiDatabase,
        network_policy: NetworkTargetPolicy,
        probe: ContentProbe,
    ) -> Self {
        Self {
            source_repository: SourceRepository::new(database),
            network_policy,
            probe: Arc::new(probe),
            canonicalization_policy: CanonicalizationPolicy::default(),
        }
    }

    #[must_use]
    pub fn with_probe_executor(
        database: &'database erabi_db::ErabiDatabase,
        network_policy: NetworkTargetPolicy,
        probe: Arc<dyn ContentProbeExecutor>,
    ) -> Self {
        Self {
            source_repository: SourceRepository::new(database),
            network_policy,
            probe,
            canonicalization_policy: CanonicalizationPolicy::default(),
        }
    }

    /// Canonicalizes, validates, probes, and durably creates or reuses a
    /// Source. A confident direct-file result upgrades only Source target
    /// classification; it never changes Crawler Seeds or configuration.
    ///
    /// # Errors
    /// Returns a canonicalization, security-policy, or persistence error. An
    /// unavailable or ambiguous content probe is represented by
    /// `NormalWebCrawl` in the successful result.
    pub async fn intake(
        &self,
        request: &SourceIntakeRequest,
    ) -> Result<SourceIntakeResult, SourceIntakeError> {
        let canonicalized = self
            .canonicalization_policy
            .canonicalize(&request.original_url)
            .map_err(SourceIntakeError::Canonicalization)?;
        let original_url = canonicalized
            .original_url
            .parse::<url::Url>()
            .map_err(|_| SourceIntakeError::Canonicalization(invalid_url_error()))?;
        let original_url_text = canonicalized.original_url.clone();
        self.network_policy
            .validate_url(&original_url)
            .map_err(SourceIntakeError::NetworkTarget)?;
        let canonical_url = canonicalized.canonical_url;
        let target = self
            .network_policy
            .validate_and_resolve(&canonical_url)
            .await
            .map_err(SourceIntakeError::NetworkTarget)?;

        let name = request
            .name
            .clone()
            .unwrap_or_else(|| bounded_generated_source_name(&canonical_url));
        let source = self
            .source_repository
            .create_or_reuse(&NewSource {
                collection_id: request.collection_id,
                name,
                original_url: original_url_text,
                canonical_url: canonical_url.clone(),
                target_type: erabi_domain::SourceTargetType::WebPage,
            })
            .await
            .map_err(SourceIntakeError::Repository)?;

        let decision = self.probe.probe(&target).await;
        let source = if decision.is_file_asset() {
            self.source_repository
                .mark_file_asset(source.id)
                .await
                .map_err(SourceIntakeError::Repository)?
        } else {
            source
        };

        Ok(SourceIntakeResult {
            source,
            original_url: request.original_url.clone(),
            canonical_url,
            decision,
        })
    }
}

fn bounded_generated_source_name(url: &url::Url) -> String {
    derive_source_name(None, None, url)
        .chars()
        .take(MAX_GENERATED_SOURCE_NAME_CHARS)
        .collect()
}

fn invalid_url_error() -> ProductError {
    ProductError::with_code(
        erabi_domain::ErrorCode::InvalidUrl,
        "the Source URL could not be parsed after canonicalization",
    )
}
