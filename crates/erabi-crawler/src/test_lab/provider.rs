use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
};

use erabi_domain::ArtifactId;

pub use crate::observation::{
    ExtractionTestHook, ExtractionTestRequest, ObservedLink, PageObservation,
    PaginationObservation, SelectorObservation,
};

/// A bounded provider request. The returned observation must retain this exact
/// request URL so the service can reject misattributed provider output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestLabObservationRequest {
    pub requested_url: String,
    pub reuse_artifact_ids: Vec<ArtifactId>,
}

/// Future-compatible observation provider. It can be implemented by a later
/// asynchronous acquisition adapter without adding a dependency here.
pub trait TestLabProvider: Send + Sync {
    fn observe(
        &self,
        request: TestLabObservationRequest,
    ) -> Pin<Box<dyn Future<Output = Result<PageObservation, TestLabProviderError>> + Send + '_>>;

    /// Validates that an existing artifact can safely supply the requested
    /// observation.
    ///
    /// # Errors
    /// Returns `ArtifactNotReusable` when the provider cannot consume it.
    fn validate_reuse(&self, artifact_id: ArtifactId) -> Result<(), TestLabProviderError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TestLabProviderError {
    Unavailable,
    ArtifactNotReusable,
    Failed,
}

/// Deterministic in-memory fixture source for tests and bounded local probes.
#[derive(Clone, Debug, Default)]
pub struct FixtureTestLabProvider {
    pages: BTreeMap<String, PageObservation>,
    reusable_artifacts: BTreeSet<String>,
}

impl FixtureTestLabProvider {
    #[must_use]
    pub fn new(pages: impl IntoIterator<Item = PageObservation>) -> Self {
        Self {
            pages: pages
                .into_iter()
                .map(|page| (page.requested_url.clone(), page))
                .collect(),
            reusable_artifacts: BTreeSet::new(),
        }
    }

    #[must_use]
    pub fn with_reusable_artifact(mut self, artifact_id: ArtifactId) -> Self {
        self.reusable_artifacts.insert(artifact_id.to_string());
        self
    }
}

impl TestLabProvider for FixtureTestLabProvider {
    fn observe(
        &self,
        request: TestLabObservationRequest,
    ) -> Pin<Box<dyn Future<Output = Result<PageObservation, TestLabProviderError>> + Send + '_>>
    {
        let page = self.pages.get(&request.requested_url).cloned();
        Box::pin(async move { page.ok_or(TestLabProviderError::Unavailable) })
    }

    fn validate_reuse(&self, artifact_id: ArtifactId) -> Result<(), TestLabProviderError> {
        if self.reusable_artifacts.contains(&artifact_id.to_string()) {
            Ok(())
        } else {
            Err(TestLabProviderError::ArtifactNotReusable)
        }
    }
}
