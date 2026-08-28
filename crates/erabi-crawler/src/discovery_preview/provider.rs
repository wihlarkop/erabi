use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};

use crate::observation::PageObservation;
use erabi_domain::TestDiagnostic;

/// Provider input carries the remaining semantic byte allowance. A provider
/// must not return a successful observation larger than this allowance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryPreviewObservationRequest {
    pub requested_url: String,
    pub remaining_download_bytes: u64,
}

/// One bounded provider outcome. `PageObservation.final_url` is the sole
/// observed final-URL field; this envelope only adds the outcome kind and byte
/// accounting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiscoveryPreviewProviderOutcome {
    Observed {
        observation: PageObservation,
        downloaded_bytes: u64,
    },
    RobotsExcluded {
        reason: String,
    },
    PageFailed {
        diagnostic: TestDiagnostic,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DiscoveryPreviewProviderError {
    #[error("Discovery Preview provider is unavailable")]
    Unavailable,
}

pub trait DiscoveryPreviewProvider: Send + Sync {
    fn observe(
        &self,
        request: DiscoveryPreviewObservationRequest,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<DiscoveryPreviewProviderOutcome, DiscoveryPreviewProviderError>,
                > + Send
                + '_,
        >,
    >;
}

/// Deterministic in-memory provider for tests only. Production runtime does
/// not install this provider implicitly.
#[derive(Clone, Debug, Default)]
pub struct FixtureDiscoveryPreviewProvider {
    outcomes: BTreeMap<String, DiscoveryPreviewProviderOutcome>,
}

impl FixtureDiscoveryPreviewProvider {
    #[must_use]
    pub fn new(
        outcomes: impl IntoIterator<Item = (String, DiscoveryPreviewProviderOutcome)>,
    ) -> Self {
        Self {
            outcomes: outcomes.into_iter().collect(),
        }
    }

    #[must_use]
    pub fn observed(
        pages: impl IntoIterator<Item = PageObservation>,
        downloaded_bytes: u64,
    ) -> Self {
        Self::new(pages.into_iter().map(|observation| {
            (
                observation.requested_url.clone(),
                DiscoveryPreviewProviderOutcome::Observed {
                    observation,
                    downloaded_bytes,
                },
            )
        }))
    }

    #[must_use]
    pub fn with_robots_excluded(
        mut self,
        requested_url: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        self.outcomes.insert(
            requested_url.into(),
            DiscoveryPreviewProviderOutcome::RobotsExcluded {
                reason: reason.into(),
            },
        );
        self
    }

    #[must_use]
    pub fn with_page_failure(
        mut self,
        requested_url: impl Into<String>,
        diagnostic: TestDiagnostic,
    ) -> Self {
        self.outcomes.insert(
            requested_url.into(),
            DiscoveryPreviewProviderOutcome::PageFailed { diagnostic },
        );
        self
    }
}

impl DiscoveryPreviewProvider for FixtureDiscoveryPreviewProvider {
    fn observe(
        &self,
        request: DiscoveryPreviewObservationRequest,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<DiscoveryPreviewProviderOutcome, DiscoveryPreviewProviderError>,
                > + Send
                + '_,
        >,
    > {
        let outcome = self.outcomes.get(&request.requested_url).cloned();
        Box::pin(async move {
            Ok(
                outcome.unwrap_or(DiscoveryPreviewProviderOutcome::PageFailed {
                    diagnostic: TestDiagnostic {
                        code: "FIXTURE_PAGE_MISSING".to_owned(),
                        message: "The deterministic fixture has no outcome for this URL."
                            .to_owned(),
                    },
                }),
            )
        })
    }
}

impl<T> DiscoveryPreviewProvider for Arc<T>
where
    T: DiscoveryPreviewProvider + ?Sized,
{
    fn observe(
        &self,
        request: DiscoveryPreviewObservationRequest,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<DiscoveryPreviewProviderOutcome, DiscoveryPreviewProviderError>,
                > + Send
                + '_,
        >,
    > {
        (**self).observe(request)
    }
}
