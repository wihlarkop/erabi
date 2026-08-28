use erabi_domain::{ExtractionObservation, PageTypeId, PaginationKind};

/// A bounded provider observation shared by Test Lab and Discovery Preview.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageObservation {
    pub requested_url: String,
    pub final_url: Option<String>,
    pub artifact_ids: Vec<erabi_domain::ArtifactId>,
    pub discovered_links: Vec<ObservedLink>,
    pub selector_observations: Vec<SelectorObservation>,
    pub pagination_observations: Vec<PaginationObservation>,
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
