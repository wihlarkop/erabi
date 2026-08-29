//! Deterministic, network-free adapter fixtures.

use std::{collections::BTreeMap, fmt};

use url::Url;

use crate::adapter::{
    CrawlerAdapter, CrawlerAdapterError, CrawlerExecuteRequest, CrawlerExecuteResult,
    CrawlerFuture, CrawlerHealth,
};

/// Health behavior for the deterministic mock.
#[derive(Clone, Debug)]
pub enum DeterministicMockHealth {
    Healthy(CrawlerHealth),
    Unavailable,
}

/// One explicit URL-keyed mock behavior.
#[derive(Clone, Debug)]
pub enum MockCrawlerFixture {
    Success(CrawlerExecuteResult),
    Partial(CrawlerExecuteResult),
    Timeout,
    AccessDenied,
    NotFound,
    Unavailable,
    RateLimited { retry_after_ms: Option<u64> },
    RemoteFailure { status_code: Option<u16> },
    UnsupportedCapability,
    InvalidProviderResponse,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MockAdapterConfigError {
    #[error("the deterministic mock fixture URL is already configured")]
    DuplicateFixtureUrl,
}

/// A `Send + Sync` deterministic fixture adapter. Fixture lookup is by exact
/// URL text and never by insertion order or UUID order.
#[derive(Clone)]
pub struct DeterministicMockAdapter {
    health: DeterministicMockHealth,
    fixtures: BTreeMap<String, MockCrawlerFixture>,
}

impl fmt::Debug for DeterministicMockAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeterministicMockAdapter")
            .field("health", &self.health)
            .field("fixture_count", &self.fixtures.len())
            .finish()
    }
}

impl DeterministicMockAdapter {
    #[must_use]
    pub fn new(health: DeterministicMockHealth) -> Self {
        Self {
            health,
            fixtures: BTreeMap::new(),
        }
    }

    /// Adds one explicit HTTP(S) URL fixture.
    ///
    /// # Errors
    /// Returns an error when the URL has already been configured.
    pub fn insert_fixture(
        &mut self,
        target_url: &Url,
        fixture: MockCrawlerFixture,
    ) -> Result<(), MockAdapterConfigError> {
        if self
            .fixtures
            .insert(target_url.as_str().to_owned(), fixture)
            .is_some()
        {
            return Err(MockAdapterConfigError::DuplicateFixtureUrl);
        }
        Ok(())
    }

    #[must_use]
    pub fn fixture_count(&self) -> usize {
        self.fixtures.len()
    }
}

impl CrawlerAdapter for DeterministicMockAdapter {
    fn health(&self) -> CrawlerFuture<'_, CrawlerHealth> {
        let health = self.health.clone();
        Box::pin(async move {
            match health {
                DeterministicMockHealth::Healthy(health) => Ok(health),
                DeterministicMockHealth::Unavailable => Err(CrawlerAdapterError::Unavailable),
            }
        })
    }

    fn execute(&self, request: CrawlerExecuteRequest) -> CrawlerFuture<'_, CrawlerExecuteResult> {
        let fixture = self.fixtures.get(request.target_url().as_str()).cloned();
        Box::pin(async move {
            let Some(fixture) = fixture else {
                return Err(CrawlerAdapterError::InvalidProviderResponse);
            };
            match fixture {
                MockCrawlerFixture::Success(result) => {
                    if result.completeness() != crate::CrawlerResultCompleteness::Complete {
                        return Err(CrawlerAdapterError::InvalidProviderResponse);
                    }
                    result.validate_for(&request)?;
                    Ok(result)
                }
                MockCrawlerFixture::Partial(result) => {
                    if !matches!(
                        result.completeness(),
                        crate::CrawlerResultCompleteness::Partial { .. }
                    ) {
                        return Err(CrawlerAdapterError::InvalidProviderResponse);
                    }
                    result.validate_for(&request)?;
                    Ok(result)
                }
                MockCrawlerFixture::Timeout => Err(CrawlerAdapterError::Timeout),
                MockCrawlerFixture::AccessDenied => Err(CrawlerAdapterError::AccessDenied),
                MockCrawlerFixture::NotFound => Err(CrawlerAdapterError::NotFound),
                MockCrawlerFixture::Unavailable => Err(CrawlerAdapterError::Unavailable),
                MockCrawlerFixture::RateLimited { retry_after_ms } => {
                    Err(CrawlerAdapterError::RateLimited { retry_after_ms })
                }
                MockCrawlerFixture::RemoteFailure { status_code } => {
                    Err(CrawlerAdapterError::RemoteFailure { status_code })
                }
                MockCrawlerFixture::UnsupportedCapability => {
                    Err(CrawlerAdapterError::UnsupportedCapability)
                }
                MockCrawlerFixture::InvalidProviderResponse => {
                    Err(CrawlerAdapterError::InvalidProviderResponse)
                }
                MockCrawlerFixture::Cancelled => Err(CrawlerAdapterError::Cancelled),
            }
        })
    }
}

impl<T> CrawlerAdapter for std::sync::Arc<T>
where
    T: CrawlerAdapter + ?Sized,
{
    fn health(&self) -> CrawlerFuture<'_, CrawlerHealth> {
        (**self).health()
    }

    fn execute(&self, request: CrawlerExecuteRequest) -> CrawlerFuture<'_, CrawlerExecuteResult> {
        (**self).execute(request)
    }
}
