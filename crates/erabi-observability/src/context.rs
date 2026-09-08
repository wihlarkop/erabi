use crate::TelemetryId;

/// Fixed, optional correlation slots approved for Erabi semantic telemetry.
///
/// This type intentionally has no generic field insertion API. Worker IDs,
/// lease IDs, source IDs, seed IDs, URLs, and arbitrary strings cannot be
/// represented by the context.
#[derive(Clone, Copy, Default, Eq, PartialEq)]
#[allow(clippy::struct_field_names)]
pub struct CorrelationContext {
    job_id: Option<TelemetryId>,
    attempt_id: Option<TelemetryId>,
    crawl_run_id: Option<TelemetryId>,
    crawl_execution_id: Option<TelemetryId>,
    crawler_id: Option<TelemetryId>,
    crawler_version_id: Option<TelemetryId>,
    source_job_id: Option<TelemetryId>,
    action_job_id: Option<TelemetryId>,
}

impl CorrelationContext {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            job_id: None,
            attempt_id: None,
            crawl_run_id: None,
            crawl_execution_id: None,
            crawler_id: None,
            crawler_version_id: None,
            source_job_id: None,
            action_job_id: None,
        }
    }

    #[must_use]
    pub const fn with_job_id(mut self, value: TelemetryId) -> Self {
        self.job_id = Some(value);
        self
    }

    #[must_use]
    pub const fn with_attempt_id(mut self, value: TelemetryId) -> Self {
        self.attempt_id = Some(value);
        self
    }

    #[must_use]
    pub const fn with_crawl_run_id(mut self, value: TelemetryId) -> Self {
        self.crawl_run_id = Some(value);
        self
    }

    #[must_use]
    pub const fn with_crawl_execution_id(mut self, value: TelemetryId) -> Self {
        self.crawl_execution_id = Some(value);
        self
    }

    #[must_use]
    pub const fn with_crawler_id(mut self, value: TelemetryId) -> Self {
        self.crawler_id = Some(value);
        self
    }

    #[must_use]
    pub const fn with_crawler_version_id(mut self, value: TelemetryId) -> Self {
        self.crawler_version_id = Some(value);
        self
    }

    #[must_use]
    pub const fn with_source_job_id(mut self, value: TelemetryId) -> Self {
        self.source_job_id = Some(value);
        self
    }

    #[must_use]
    pub const fn with_action_job_id(mut self, value: TelemetryId) -> Self {
        self.action_job_id = Some(value);
        self
    }

    pub(crate) fn job_id(&self) -> Option<String> {
        self.job_id.as_ref().map(TelemetryId::as_string)
    }

    pub(crate) fn attempt_id(&self) -> Option<String> {
        self.attempt_id.as_ref().map(TelemetryId::as_string)
    }

    pub(crate) fn crawl_run_id(&self) -> Option<String> {
        self.crawl_run_id.as_ref().map(TelemetryId::as_string)
    }

    pub(crate) fn crawl_execution_id(&self) -> Option<String> {
        self.crawl_execution_id.as_ref().map(TelemetryId::as_string)
    }

    pub(crate) fn crawler_id(&self) -> Option<String> {
        self.crawler_id.as_ref().map(TelemetryId::as_string)
    }

    pub(crate) fn crawler_version_id(&self) -> Option<String> {
        self.crawler_version_id.as_ref().map(TelemetryId::as_string)
    }

    pub(crate) fn source_job_id(&self) -> Option<String> {
        self.source_job_id.as_ref().map(TelemetryId::as_string)
    }

    pub(crate) fn action_job_id(&self) -> Option<String> {
        self.action_job_id.as_ref().map(TelemetryId::as_string)
    }
}
