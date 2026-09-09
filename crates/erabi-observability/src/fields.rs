pub(crate) const FIELD_TRACE_ID: &str = "trace_id";
pub(crate) const FIELD_HTTP_METHOD: &str = "http_method";
pub(crate) const FIELD_ROUTE_TEMPLATE: &str = "route_template";
pub(crate) const FIELD_STATUS_CODE: &str = "status_code";
pub(crate) const FIELD_DURATION_MS: &str = "duration_ms";
pub(crate) const FIELD_EVENT_NAME: &str = "event_name";
pub(crate) const FIELD_CODE: &str = "code";

pub(crate) const ERABI_TELEMETRY_TARGET: &str = "erabi.telemetry";
pub(crate) const HTTP_REQUEST_SPAN_NAME: &str = "http.request";
pub(crate) const TELEMETRY_CONFIGURATION_EVENT_NAME: &str = "telemetry.configuration";

/// A fixed set of HTTP method tokens safe for telemetry rendering.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
    Head,
    Options,
    Connect,
    Trace,
    Other,
}

impl HttpMethod {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
            Self::Head => "HEAD",
            Self::Options => "OPTIONS",
            Self::Connect => "CONNECT",
            Self::Trace => "TRACE",
            Self::Other => "OTHER",
        }
    }
}

/// A closed set of route-template capabilities approved by the API boundary.
///
/// There is intentionally no public string constructor, conversion, or raw
/// representation. The API maps Axum's runtime matched route to one of these
/// variants before a request span is created.
///
/// ```compile_fail
/// use erabi_observability::RouteTemplate;
///
/// let runtime = String::from("/api/v1/artifacts/concrete-value");
/// let _route = RouteTemplate::from_static_contract(runtime.as_str());
/// let leaked = Box::leak(String::from("/api/v1/artifacts/leaked").into_boxed_str());
/// let _route = RouteTemplate::from_static_contract(leaked);
/// ```
#[non_exhaustive]
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum RouteTemplate {
    Unknown,
    ApiV1Health,
    ApiV1OpenapiJson,
    ApiDocs,
    ApiV1Readiness,
    ApiV1DiagnosticsStatus,
    ApiV1QuickScrapes,
    ApiV1QuickScrapesBatch,
    ApiV1Crawlers,
    ApiV1CrawlersCrawlerId,
    ApiV1CrawlersCrawlerIdVersions,
    ApiV1CrawlersCrawlerIdVersionsVersionId,
    ApiV1CrawlersCrawlerIdDrafts,
    ApiV1CrawlersCrawlerIdVersionsVersionIdPublish,
    ApiV1CrawlersCrawlerIdVersionsVersionIdPublishValidation,
    ApiV1CrawlersCrawlerIdVersionsVersionIdReactivate,
    ApiV1CrawlersCrawlerIdVersionsVersionIdPageTypes,
    ApiV1CrawlersCrawlerIdVersionsVersionIdPageTypesPageTypeId,
    ApiV1CrawlersCrawlerIdVersionsVersionIdPageTypesPageTypeIdMatchers,
    ApiV1CrawlersCrawlerIdVersionsVersionIdPageTypesPageTypeIdMatchersMatcherId,
    ApiV1CrawlersCrawlerIdVersionsVersionIdMatchPageType,
    ApiV1CrawlersCrawlerIdVersionsVersionIdCanonicalization,
    ApiV1CrawlersCrawlerIdVersionsVersionIdCanonicalizeUrl,
    ApiV1CrawlersCrawlerIdVersionsVersionIdDomainScope,
    ApiV1CrawlersCrawlerIdVersionsVersionIdClassifyDomainScope,
    ApiV1CrawlersCrawlerIdVersionsVersionIdGuardrails,
    ApiV1CrawlersCrawlerIdVersionsVersionIdTransitions,
    ApiV1CrawlersCrawlerIdVersionsVersionIdTransitionsTransitionId,
    ApiV1CrawlersCrawlerIdVersionsVersionIdTestLabTests,
    ApiV1CrawlersCrawlerIdVersionsVersionIdDiscoveryPreview,
    ApiV1CrawlersCrawlerIdVersionsVersionIdProductionRuns,
    ApiV1CrawlersCrawlerIdVersionsVersionIdTestEvidence,
    ApiV1CrawlersCrawlerIdVersionsVersionIdTestEvidenceEvidenceId,
    ApiV1DiagnosticsWildcard,
    ApiV1EventsJobsJobIdProgress,
    ApiV1JobsJobIdRetryFailedParts,
    ApiV1JobsJobIdRerunFullCrawl,
    ApiV1JobsJobIdResume,
    ApiV1JobsJobIdRestart,
    ApiV1JobsJobIdRetry,
    ApiV1JobsJobIdCancel,
    ApiV1JobsJobIdPriority,
    ApiV1JobsJobId,
    ApiV1EventsWildcard,
    ApiV1AssetsWildcard,
    ApiV1ExportsWildcard,
    ApiV1BackupsWildcard,
    ApiV1ArtifactsWildcard,
    ApiV1Wildcard,
    AssetsWildcard,
    Root,
    RootWildcard,
}

impl RouteTemplate {
    /// The static fallback used when Axum has no recognized matched route.
    pub const UNKNOWN: Self = Self::Unknown;

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN_ROUTE",
            Self::ApiV1Health => "/api/v1/health",
            Self::ApiV1OpenapiJson => "/api/v1/openapi.json",
            Self::ApiDocs => "/api/docs",
            Self::ApiV1Readiness => "/api/v1/readiness",
            Self::ApiV1DiagnosticsStatus => "/api/v1/diagnostics/status",
            Self::ApiV1QuickScrapes => "/api/v1/quick-scrapes",
            Self::ApiV1QuickScrapesBatch => "/api/v1/quick-scrapes/batch",
            Self::ApiV1Crawlers => "/api/v1/crawlers",
            Self::ApiV1CrawlersCrawlerId => "/api/v1/crawlers/{crawler_id}",
            Self::ApiV1CrawlersCrawlerIdVersions => "/api/v1/crawlers/{crawler_id}/versions",
            Self::ApiV1CrawlersCrawlerIdVersionsVersionId => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}"
            }
            Self::ApiV1CrawlersCrawlerIdDrafts => "/api/v1/crawlers/{crawler_id}/drafts",
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdPublish => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/publish"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdPublishValidation => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/publish-validation"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdReactivate => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/reactivate"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdPageTypes => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdPageTypesPageTypeId => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdPageTypesPageTypeIdMatchers => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdPageTypesPageTypeIdMatchersMatcherId => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers/{matcher_id}"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdMatchPageType => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/match-page-type"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdCanonicalization => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/canonicalization"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdCanonicalizeUrl => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/canonicalize-url"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdDomainScope => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/domain-scope"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdClassifyDomainScope => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/classify-domain-scope"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdGuardrails => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/guardrails"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdTransitions => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdTransitionsTransitionId => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions/{transition_id}"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdTestLabTests => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/test-lab/tests"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdDiscoveryPreview => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/discovery-preview"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdProductionRuns => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/production-runs"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdTestEvidence => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/test-evidence"
            }
            Self::ApiV1CrawlersCrawlerIdVersionsVersionIdTestEvidenceEvidenceId => {
                "/api/v1/crawlers/{crawler_id}/versions/{version_id}/test-evidence/{evidence_id}"
            }
            Self::ApiV1DiagnosticsWildcard => "/api/v1/diagnostics/{*path}",
            Self::ApiV1EventsJobsJobIdProgress => "/api/v1/events/jobs/{job_id}/progress",
            Self::ApiV1JobsJobIdRetryFailedParts => "/api/v1/jobs/{job_id}/retry-failed-parts",
            Self::ApiV1JobsJobIdRerunFullCrawl => "/api/v1/jobs/{job_id}/rerun-full-crawl",
            Self::ApiV1JobsJobIdResume => "/api/v1/jobs/{job_id}/resume",
            Self::ApiV1JobsJobIdRestart => "/api/v1/jobs/{job_id}/restart",
            Self::ApiV1JobsJobIdRetry => "/api/v1/jobs/{job_id}/retry",
            Self::ApiV1JobsJobIdCancel => "/api/v1/jobs/{job_id}/cancel",
            Self::ApiV1JobsJobIdPriority => "/api/v1/jobs/{job_id}/priority",
            Self::ApiV1JobsJobId => "/api/v1/jobs/{job_id}",
            Self::ApiV1EventsWildcard => "/api/v1/events/{*path}",
            Self::ApiV1AssetsWildcard => "/api/v1/assets/{*path}",
            Self::ApiV1ExportsWildcard => "/api/v1/exports/{*path}",
            Self::ApiV1BackupsWildcard => "/api/v1/backups/{*path}",
            Self::ApiV1ArtifactsWildcard => "/api/v1/artifacts/{*path}",
            Self::ApiV1Wildcard => "/api/v1/{*path}",
            Self::AssetsWildcard => "/assets/{*path}",
            Self::Root => "/",
            Self::RootWildcard => "/{*path}",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_mapping_is_bounded() {
        assert_eq!(HttpMethod::Get.as_str(), "GET");
        assert_eq!(HttpMethod::Other.as_str(), "OTHER");
    }

    #[test]
    fn route_template_exposes_only_approved_capabilities() {
        assert_eq!(
            RouteTemplate::ApiV1CrawlersCrawlerId.as_str(),
            "/api/v1/crawlers/{crawler_id}"
        );
        assert_eq!(RouteTemplate::UNKNOWN.as_str(), "UNKNOWN_ROUTE");
    }
}
