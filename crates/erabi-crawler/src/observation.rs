use std::fmt;

use erabi_domain::{ExtractionObservation, PageTypeId, PaginationKind};

use crate::adapter::{
    MAX_CRAWLER_DISCOVERED_LINKS, MAX_CRAWLER_PAGINATION_OBSERVATIONS, MAX_CRAWLER_SELECTOR_CHARS,
    MAX_CRAWLER_SELECTOR_OBSERVATIONS, MAX_CRAWLER_URL_CHARS,
};

/// A bounded provider observation shared by Test Lab and Discovery Preview.
#[derive(Clone, Eq, PartialEq)]
pub struct PageObservation {
    pub requested_url: String,
    pub final_url: Option<String>,
    pub artifact_ids: Vec<erabi_domain::ArtifactId>,
    pub discovered_links: Vec<ObservedLink>,
    pub selector_observations: Vec<SelectorObservation>,
    pub pagination_observations: Vec<PaginationObservation>,
}

impl PageObservation {
    pub(crate) fn validate_for_adapter(&self) -> Result<(), ObservationValidationError> {
        if self.requested_url.is_empty()
            || self.requested_url.chars().count() > MAX_CRAWLER_URL_CHARS
            || self.requested_url.chars().any(char::is_control)
            || self.discovered_links.len() > MAX_CRAWLER_DISCOVERED_LINKS
            || self.selector_observations.len() > MAX_CRAWLER_SELECTOR_OBSERVATIONS
            || self.pagination_observations.len() > MAX_CRAWLER_PAGINATION_OBSERVATIONS
        {
            return Err(ObservationValidationError::Invalid);
        }
        if let Some(final_url) = &self.final_url
            && (final_url.is_empty()
                || final_url.chars().count() > MAX_CRAWLER_URL_CHARS
                || final_url.chars().any(char::is_control))
        {
            return Err(ObservationValidationError::Invalid);
        }
        for link in &self.discovered_links {
            if link.raw_href.is_empty()
                || link.raw_href.chars().count() > MAX_CRAWLER_URL_CHARS
                || link.raw_href.chars().any(char::is_control)
                || link.selector.as_deref().is_some_and(|selector| {
                    selector.is_empty()
                        || selector.chars().count() > MAX_CRAWLER_SELECTOR_CHARS
                        || selector.chars().any(char::is_control)
                })
            {
                return Err(ObservationValidationError::Invalid);
            }
        }
        for selector in &self.selector_observations {
            if selector.selector.is_empty()
                || selector.selector.chars().count() > MAX_CRAWLER_SELECTOR_CHARS
                || selector.selector.chars().any(char::is_control)
            {
                return Err(ObservationValidationError::Invalid);
            }
        }
        for pagination in &self.pagination_observations {
            if pagination.selector.as_deref().is_some_and(|selector| {
                selector.is_empty()
                    || selector.chars().count() > MAX_CRAWLER_SELECTOR_CHARS
                    || selector.chars().any(char::is_control)
            }) || pagination.target_url.as_deref().is_some_and(|target_url| {
                target_url.is_empty()
                    || target_url.chars().count() > MAX_CRAWLER_URL_CHARS
                    || target_url.chars().any(char::is_control)
            }) {
                return Err(ObservationValidationError::Invalid);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObservationValidationError {
    Invalid,
}

impl fmt::Debug for PageObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PageObservation")
            .field("requested_url", &safe_url_identity(&self.requested_url))
            .field(
                "final_url",
                &self.final_url.as_deref().map(safe_url_identity),
            )
            .field("artifact_count", &self.artifact_ids.len())
            .field("discovered_link_count", &self.discovered_links.len())
            .field(
                "selector_observation_count",
                &self.selector_observations.len(),
            )
            .field(
                "pagination_observation_count",
                &self.pagination_observations.len(),
            )
            .finish()
    }
}

pub(crate) fn safe_url_identity(value: &str) -> String {
    let Ok(mut url) = url::Url::parse(value) else {
        return "<invalid-url>".to_owned();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedLink {
    /// The href exactly as emitted by the provider.
    pub raw_href: String,
    /// Positive provider provenance for the selector that produced this link.
    pub selector: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectorObservation {
    pub selector: String,
    pub matches_found: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaginationObservation {
    pub kind: PaginationKind,
    pub selector: Option<String>,
    pub target_url: Option<String>,
}

/// The smallest Plan 07-compatible seam for optional extraction observations.
pub struct ExtractionTestRequest {
    pub page_type_id: PageTypeId,
    pub input_url: String,
    pub observation: PageObservation,
}

pub trait ExtractionTestHook: Send + Sync {
    fn evaluate(
        &self,
        request: ExtractionTestRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ExtractionObservation> + Send + '_>>;
}
