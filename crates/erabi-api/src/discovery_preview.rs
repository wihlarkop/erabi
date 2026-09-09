//! HTTP boundary for the ephemeral Discovery Preview service.

use axum::{
    Json,
    extract::{Extension, Path, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use erabi_crawler::DiscoveryPreviewError;
use erabi_domain::{
    DiscoveryPreviewLimits, DiscoveryPreviewRequest, DiscoveryTransitionId, SeedId,
    TransitionPreviewTotalLimit,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};
use uuid::Uuid;

use crate::{
    AppState,
    app::TraceId,
    error::{ApiErrorEnvelope, error_response},
    openapi::wire::{
        CanonicalizationEvidenceSchema, DomainScopeEvidenceSchema, PageTypeMatchEvidenceSchema,
        TestDiagnosticSchema,
    },
};

#[derive(Debug, Deserialize, ToSchema)]
#[schema(as = DiscoveryPreviewRequest)]
#[serde(deny_unknown_fields)]
pub(crate) struct DiscoveryPreviewRequestDto {
    pub seed_ids: Vec<String>,
    pub limits: PreviewLimitsDto,
}

#[derive(Debug, Deserialize, ToSchema)]
#[schema(as = PreviewLimits)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreviewLimitsDto {
    pub max_pages: u64,
    pub max_depth: u32,
    pub max_duration_ms: u64,
    pub default_transition_total_limit: u64,
    #[serde(default)]
    pub transition_total_limits: Vec<TransitionPreviewTotalLimitDto>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[schema(as = TransitionPreviewTotalLimit)]
#[serde(deny_unknown_fields)]
pub(crate) struct TransitionPreviewTotalLimitDto {
    pub transition_id: String,
    pub max_total_links: u64,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = DiscoveryPreviewResult)]
struct DiscoveryPreviewResultSchema {
    result_semantics: DiscoveryPreviewResultSemanticsSchema,
    crawler_version_id: String,
    config_hash: String,
    selected_seed_ids: Vec<String>,
    effective_limits: EffectiveDiscoveryPreviewLimitsSchema,
    seeds: Vec<DiscoveryPreviewSeedSchema>,
    pages: Vec<DiscoveryPreviewPageSchema>,
    discovery_paths: Vec<DiscoveryPathSchema>,
    summary: DiscoveryPreviewSummarySchema,
    growth_indicators: PreviewGrowthIndicatorsSchema,
    growth_warnings: Vec<PreviewGrowthWarningSchema>,
    warnings: Vec<PreviewDiagnosticSchema>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = DiscoveryPreviewResultSemantics)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum DiscoveryPreviewResultSemanticsSchema {
    PreviewOnly,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = EffectiveDiscoveryPreviewLimits)]
struct EffectiveDiscoveryPreviewLimitsSchema {
    max_pages: u64,
    max_depth: u32,
    max_duration_ms: u64,
    max_downloaded_bytes: u64,
    transition_total_limits: Vec<EffectiveTransitionPreviewTotalLimitSchema>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = EffectiveTransitionPreviewTotalLimit)]
struct EffectiveTransitionPreviewTotalLimitSchema {
    transition_id: String,
    effective_total_limit: u64,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = DiscoveryPreviewSeed)]
struct DiscoveryPreviewSeedSchema {
    seed_id: String,
    requested_url: String,
    canonical_url: String,
    #[schema(required = true)]
    entry_page_type_hint: Option<String>,
    state: PreviewUrlStateSchema,
    #[schema(required = true)]
    duplicate_of_canonical_url: Option<String>,
    #[schema(required = true)]
    scope: Option<DomainScopeEvidenceSchema>,
    #[schema(required = true)]
    page_type_match: Option<PageTypeMatchEvidenceSchema>,
    budget_hits: Vec<PreviewBudgetHitSchema>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = DiscoveryPreviewPage)]
struct DiscoveryPreviewPageSchema {
    requested_url: String,
    requested_canonical_url: String,
    #[schema(required = true)]
    final_url: Option<String>,
    #[schema(required = true)]
    canonical_url: Option<String>,
    depth: u32,
    state: PreviewUrlStateSchema,
    seed_ids: Vec<String>,
    #[schema(required = true)]
    scope: Option<DomainScopeEvidenceSchema>,
    #[schema(required = true)]
    page_type_match: Option<PageTypeMatchEvidenceSchema>,
    #[schema(required = true)]
    downloaded_bytes: Option<u64>,
    #[schema(required = true)]
    robots_reason: Option<String>,
    #[schema(required = true)]
    diagnostic: Option<TestDiagnosticSchema>,
    budget_hits: Vec<PreviewBudgetHitSchema>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = DiscoveryPath)]
struct DiscoveryPathSchema {
    seed_id: String,
    seed_ids: Vec<String>,
    source_requested_url: String,
    #[schema(required = true)]
    source_final_url: Option<String>,
    source_canonical_url: String,
    source_page_type_match: PageTypeMatchEvidenceSchema,
    #[schema(required = true)]
    selector: Option<String>,
    raw_href: String,
    #[schema(required = true)]
    resolved_original_url: Option<String>,
    #[schema(required = true)]
    canonical_url: Option<String>,
    #[schema(required = true)]
    canonicalization: Option<CanonicalizationEvidenceSchema>,
    #[schema(required = true)]
    scope: Option<DomainScopeEvidenceSchema>,
    state: PreviewUrlStateSchema,
    #[schema(required = true)]
    duplicate_of_canonical_url: Option<String>,
    #[schema(required = true)]
    target_page_type_match: Option<PageTypeMatchEvidenceSchema>,
    source_depth: u32,
    #[schema(required = true)]
    prospective_depth: Option<u32>,
    transition_evaluations: Vec<PreviewTransitionEvaluationSchema>,
    budget_hits: Vec<PreviewBudgetHitSchema>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PreviewUrlState)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum PreviewUrlStateSchema {
    Sampled,
    InScopeMatched,
    AmbiguousPageType,
    Unmatched,
    External,
    Blocked,
    CanonicalDuplicate,
    RobotsExcluded,
    BudgetExcluded,
    ProviderError,
    InvalidUrl,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PreviewBudgetKind)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum PreviewBudgetKindSchema {
    MaxPages,
    MaxDepth,
    MaxDuration,
    MaxDownloadedBytes,
    PageTypePageBudget,
    TransitionPerSourcePage,
    TransitionTotal,
    ProvenanceRetention,
    DiagnosticRetention,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PreviewBudgetHit)]
struct PreviewBudgetHitSchema {
    kind: PreviewBudgetKindSchema,
    #[schema(required = true)]
    transition_id: Option<String>,
    #[schema(required = true)]
    page_type_id: Option<String>,
    observed: u64,
    limit: u64,
}

#[allow(dead_code)]
#[allow(clippy::struct_excessive_bools)]
#[derive(ToSchema)]
#[schema(as = PreviewTransitionEvaluation)]
struct PreviewTransitionEvaluationSchema {
    transition_id: String,
    transition_name: String,
    source_page_type_id: String,
    target_page_type_id: String,
    priority: i32,
    selector_eligible: bool,
    target_page_type_eligible: bool,
    constraints_eligible: bool,
    eligible: bool,
    budget_hits: Vec<PreviewBudgetHitSchema>,
    #[schema(required = true)]
    diagnostic: Option<PreviewDiagnosticSchema>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PreviewDiagnostic)]
struct PreviewDiagnosticSchema {
    code: String,
    message: String,
    #[schema(required = true)]
    observed: Option<u64>,
    #[schema(required = true)]
    threshold: Option<u64>,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = DiscoveryPreviewSummary)]
struct DiscoveryPreviewSummarySchema {
    pages_sampled: u64,
    urls_discovered: u64,
    canonical_unique_urls: u64,
    duplicates_prevented: u64,
    page_type_distribution: Vec<PreviewPageTypeDistributionSchema>,
    ambiguous_urls: u64,
    unmatched_urls: u64,
    external_urls: u64,
    blocked_urls: u64,
    robots_excluded: u64,
    provider_errors: u64,
    transition_counts: Vec<PreviewTransitionCountSchema>,
    budget_hit_counts: BTreeMap<String, u64>,
    frontier_remaining: u64,
    newly_enqueued_urls: u64,
    pagination_truncation_count: u64,
    duration_work_not_expanded: bool,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PreviewPageTypeDistribution)]
struct PreviewPageTypeDistributionSchema {
    page_type_id: String,
    page_type_name: String,
    discovered_unique_urls: u64,
    sampled_pages: u64,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PreviewTransitionCount)]
struct PreviewTransitionCountSchema {
    transition_id: String,
    transition_name: String,
    eligible_edges: u64,
    source_pages_with_eligible_edges: u64,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PreviewGrowthIndicators)]
struct PreviewGrowthIndicatorsSchema {
    peak_new_canonical_urls_from_one_page: u64,
    total_newly_enqueued_urls: u64,
    frontier_remaining: u64,
    #[schema(required = true)]
    dominant_transition_id: Option<String>,
    dominant_transition_eligible_edges: u64,
    total_eligible_transition_edges: u64,
    #[schema(required = true)]
    dominant_transition_share_percent: Option<u64>,
    query_variant_groups: Vec<PreviewQueryVariantGroupSchema>,
    unmatched_denominator: u64,
    ambiguity_denominator: u64,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PreviewQueryVariantGroup)]
struct PreviewQueryVariantGroupSchema {
    host: String,
    path: String,
    total_identities: u64,
    query_bearing_identities: u64,
    canonical_query_variants: u64,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PreviewGrowthWarning)]
struct PreviewGrowthWarningSchema {
    code: PreviewGrowthWarningCodeSchema,
    message: String,
    observed: u64,
    threshold: u64,
}

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = PreviewGrowthWarningCode)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum PreviewGrowthWarningCodeSchema {
    CyclicTransitionDominance,
    QueryParameterExplosion,
    HighUnmatchedRate,
    WidespreadPageTypeAmbiguity,
    BudgetPressure,
}

impl DiscoveryPreviewRequestDto {
    fn into_request(self) -> Result<DiscoveryPreviewRequest, ()> {
        let seed_ids = self
            .seed_ids
            .iter()
            .map(|value| parse_id::<SeedId>(value))
            .collect::<Result<Vec<_>, _>>()?;
        let transition_total_limits = self
            .limits
            .transition_total_limits
            .into_iter()
            .map(|value| {
                Ok(TransitionPreviewTotalLimit {
                    transition_id: parse_id::<DiscoveryTransitionId>(&value.transition_id)?,
                    max_total_links: value.max_total_links,
                })
            })
            .collect::<Result<Vec<_>, ()>>()?;
        Ok(DiscoveryPreviewRequest {
            seed_ids,
            limits: DiscoveryPreviewLimits {
                max_pages: self.limits.max_pages,
                max_depth: self.limits.max_depth,
                max_duration_ms: self.limits.max_duration_ms,
                default_transition_total_limit: self.limits.default_transition_total_limit,
                transition_total_limits,
            },
        })
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/crawlers/{crawler_id}/versions/{version_id}/discovery-preview",
    request_body = DiscoveryPreviewRequestDto,
    responses(
        (status = 200, description = "Discovery Preview result", body = DiscoveryPreviewResultSchema),
        (status = 400, description = "Invalid Discovery Preview request", body = ApiErrorEnvelope),
        (status = 404, description = "CrawlerVersion not found", body = ApiErrorEnvelope),
        (status = 409, description = "Discovery Preview conflict", body = ApiErrorEnvelope),
        (status = 502, description = "Discovery Preview provider error", body = ApiErrorEnvelope),
        (status = 503, description = "Discovery Preview unavailable", body = ApiErrorEnvelope)
    )
)]
pub(crate) async fn run_discovery_preview(
    State(state): State<AppState>,
    Extension(trace): Extension<TraceId>,
    Path((raw_crawler_id, raw_version_id)): Path<(String, String)>,
    input: Result<Json<DiscoveryPreviewRequestDto>, JsonRejection>,
) -> Response {
    let Some((crawler_id, version_id)) = parse_version_path(&raw_crawler_id, &raw_version_id)
    else {
        return preview_error(
            StatusCode::BAD_REQUEST,
            "INVALID_DISCOVERY_PREVIEW_REQUEST",
            "The Crawler or CrawlerVersion identifier is invalid.",
            &trace,
        );
    };
    let Ok(Json(input)) = input else {
        return preview_error(
            StatusCode::BAD_REQUEST,
            "INVALID_DISCOVERY_PREVIEW_REQUEST",
            "The Discovery Preview request body is invalid.",
            &trace,
        );
    };
    let Ok(request) = input.into_request() else {
        return preview_error(
            StatusCode::BAD_REQUEST,
            "INVALID_DISCOVERY_PREVIEW_REQUEST",
            "The Discovery Preview request contains an invalid identifier.",
            &trace,
        );
    };
    let Some(service) = state.discovery_preview_runtime() else {
        return preview_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "DISCOVERY_PREVIEW_PROVIDER_UNAVAILABLE",
            "Discovery Preview is not configured in this runtime.",
            &trace,
        );
    };
    match service.execute(crawler_id, version_id, request).await {
        Ok(result) => Json(result).into_response(),
        Err(error) => map_preview_error(error, &trace),
    }
}

pub(crate) fn openapi_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::<AppState>::new().routes(routes!(run_discovery_preview))
}

fn parse_id<T>(value: &str) -> Result<T, ()>
where
    T: FromUuid,
{
    Uuid::parse_str(value).ok().and_then(T::from_uuid).ok_or(())
}

trait FromUuid: Sized {
    fn from_uuid(value: Uuid) -> Option<Self>;
}

impl FromUuid for SeedId {
    fn from_uuid(value: Uuid) -> Option<Self> {
        Self::from_uuid(value)
    }
}

impl FromUuid for DiscoveryTransitionId {
    fn from_uuid(value: Uuid) -> Option<Self> {
        Self::from_uuid(value)
    }
}

fn parse_version_path(
    crawler: &str,
    version: &str,
) -> Option<(erabi_domain::CrawlerId, erabi_domain::CrawlerVersionId)> {
    Some((
        Uuid::parse_str(crawler)
            .ok()
            .and_then(erabi_domain::CrawlerId::from_uuid)?,
        Uuid::parse_str(version)
            .ok()
            .and_then(erabi_domain::CrawlerVersionId::from_uuid)?,
    ))
}

#[allow(clippy::needless_pass_by_value)]
fn map_preview_error(error: DiscoveryPreviewError, trace: &TraceId) -> Response {
    let (status, code, message) = match error {
        DiscoveryPreviewError::CrawlerNotFound => (
            StatusCode::NOT_FOUND,
            "CRAWLER_NOT_FOUND",
            "The Crawler was not found.",
        ),
        DiscoveryPreviewError::CrawlerVersionNotFound => (
            StatusCode::NOT_FOUND,
            "CRAWLER_VERSION_NOT_FOUND",
            "The CrawlerVersion was not found.",
        ),
        DiscoveryPreviewError::VersionNotOwnedByCrawler => (
            StatusCode::CONFLICT,
            "VERSION_NOT_OWNED_BY_CRAWLER",
            "The CrawlerVersion does not belong to this Crawler.",
        ),
        DiscoveryPreviewError::VersionNotDraft => (
            StatusCode::CONFLICT,
            "VERSION_NOT_DRAFT",
            "Discovery Preview requires a Draft CrawlerVersion.",
        ),
        DiscoveryPreviewError::VersionNotActiveDraft => (
            StatusCode::CONFLICT,
            "VERSION_NOT_ACTIVE_DRAFT",
            "Discovery Preview requires the active Draft.",
        ),
        DiscoveryPreviewError::InvalidRequest | DiscoveryPreviewError::InvalidPreviewLimits => (
            StatusCode::BAD_REQUEST,
            "INVALID_DISCOVERY_PREVIEW_REQUEST",
            "The Discovery Preview request is invalid.",
        ),
        DiscoveryPreviewError::NoSelectedSeeds => (
            StatusCode::BAD_REQUEST,
            "NO_SELECTED_SEEDS",
            "Discovery Preview requires at least one selected Seed.",
        ),
        DiscoveryPreviewError::DuplicateSeedSelection => (
            StatusCode::BAD_REQUEST,
            "DUPLICATE_SEED_SELECTION",
            "Discovery Preview does not accept duplicate Seed IDs.",
        ),
        DiscoveryPreviewError::SeedNotOwnedByVersion => (
            StatusCode::CONFLICT,
            "SEED_NOT_OWNED_BY_VERSION",
            "A selected Seed does not belong to this CrawlerVersion.",
        ),
        DiscoveryPreviewError::SeedDisabled => (
            StatusCode::CONFLICT,
            "SEED_DISABLED",
            "A selected Seed is disabled.",
        ),
        DiscoveryPreviewError::InvalidTransitionPreviewLimit => (
            StatusCode::BAD_REQUEST,
            "INVALID_TRANSITION_PREVIEW_LIMIT",
            "A Preview transition limit is invalid.",
        ),
        DiscoveryPreviewError::TransitionNotOwnedByVersion => (
            StatusCode::CONFLICT,
            "TRANSITION_NOT_OWNED_BY_VERSION",
            "A Preview transition limit does not belong to this CrawlerVersion.",
        ),
        DiscoveryPreviewError::ProviderUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "DISCOVERY_PREVIEW_PROVIDER_UNAVAILABLE",
            "The Discovery Preview provider is unavailable.",
        ),
        DiscoveryPreviewError::ProviderObservationRequestMismatch => (
            StatusCode::BAD_GATEWAY,
            "DISCOVERY_PREVIEW_PROVIDER_OBSERVATION_MISMATCH",
            "The Discovery Preview provider returned an observation for a different URL.",
        ),
        DiscoveryPreviewError::ProviderObservationInvalid => (
            StatusCode::BAD_GATEWAY,
            "DISCOVERY_PREVIEW_PROVIDER_OBSERVATION_INVALID",
            "The Discovery Preview provider returned an invalid bounded observation.",
        ),
        DiscoveryPreviewError::BudgetOverflow => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "DISCOVERY_PREVIEW_BUDGET_OVERFLOW",
            "Discovery Preview exceeded a safe internal budget boundary.",
        ),
        DiscoveryPreviewError::QueueEntryMissingSeedProvenance
        | DiscoveryPreviewError::PersistedStateInvalid => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "PERSISTED_STATE_INVALID",
            "Stored crawler state is invalid; no Preview was produced.",
        ),
    };
    preview_error(status, code, message, trace)
}

fn preview_error(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    trace: &TraceId,
) -> Response {
    error_response(status, ApiErrorEnvelope::new(code, message, trace.as_str()))
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use serde_json::Value;

    use super::*;

    #[tokio::test]
    async fn budget_overflow_is_not_reported_as_provider_corruption()
    -> Result<(), Box<dyn std::error::Error>> {
        let response =
            map_preview_error(DiscoveryPreviewError::BudgetOverflow, &TraceId::for_test());
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let error: Value = serde_json::from_slice(&body)?;
        assert_eq!(error["code"], "DISCOVERY_PREVIEW_BUDGET_OVERFLOW");
        Ok(())
    }
}
