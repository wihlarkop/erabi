//! HTTP boundary for the ephemeral Discovery Preview service.

use std::collections::BTreeMap;

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
use serde_json::Value;
use uuid::Uuid;

use crate::{
    AppState,
    app::TraceId,
    error::{ApiErrorEnvelope, error_response},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DiscoveryPreviewRequestDto {
    pub seed_ids: Vec<String>,
    pub limits: PreviewLimitsDto,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreviewLimitsDto {
    pub max_pages: u64,
    pub max_depth: u32,
    pub max_duration_ms: u64,
    pub default_transition_total_limit: u64,
    #[serde(default)]
    pub transition_total_limits: Vec<TransitionPreviewTotalLimitDto>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TransitionPreviewTotalLimitDto {
    pub transition_id: String,
    pub max_total_links: u64,
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

pub(crate) fn discovery_preview_openapi_schemas() -> BTreeMap<&'static str, Value> {
    let mut schemas = BTreeMap::new();
    schemas.insert("DiscoveryPreviewRequest", serde_json::json!({"type":"object","required":["seed_ids","limits"],"properties":{"seed_ids":{"type":"array","minItems":1,"maxItems":32,"items":{"type":"string","format":"uuid"}},"limits":{"$ref":"#/components/schemas/PreviewLimits"}}}));
    schemas.insert("PreviewLimits", serde_json::json!({"type":"object","required":["max_pages","max_depth","max_duration_ms","default_transition_total_limit","transition_total_limits"],"properties":{"max_pages":{"type":"integer","minimum":1},"max_depth":{"type":"integer","minimum":0},"max_duration_ms":{"type":"integer","minimum":1},"default_transition_total_limit":{"type":"integer","minimum":1},"transition_total_limits":{"type":"array","maxItems":128,"items":{"$ref":"#/components/schemas/TransitionPreviewTotalLimit"}}}}));
    schemas.insert("TransitionPreviewTotalLimit", serde_json::json!({"type":"object","required":["transition_id","max_total_links"],"properties":{"transition_id":{"type":"string","format":"uuid"},"max_total_links":{"type":"integer","minimum":1}}}));
    schemas.insert("EffectiveTransitionPreviewTotalLimit", serde_json::json!({"type":"object","required":["transition_id","effective_total_limit"],"properties":{"transition_id":{"type":"string","format":"uuid"},"effective_total_limit":{"type":"integer","minimum":1}}}));
    schemas.insert("EffectiveDiscoveryPreviewLimits", serde_json::json!({"type":"object","required":["max_pages","max_depth","max_duration_ms","max_downloaded_bytes","transition_total_limits"],"properties":{"max_pages":{"type":"integer","minimum":1},"max_depth":{"type":"integer","minimum":0},"max_duration_ms":{"type":"integer","minimum":1},"max_downloaded_bytes":{"type":"integer","minimum":1},"transition_total_limits":{"type":"array","items":{"$ref":"#/components/schemas/EffectiveTransitionPreviewTotalLimit"}}}}));
    schemas.insert("DiscoveryPreviewResult", serde_json::json!({"type":"object","required":["result_semantics","crawler_version_id","config_hash","selected_seed_ids","effective_limits","seeds","pages","discovery_paths","summary","growth_indicators","growth_warnings","warnings"],"properties":{"result_semantics":{"$ref":"#/components/schemas/DiscoveryPreviewResultSemantics"},"crawler_version_id":{"type":"string","format":"uuid"},"config_hash":{"type":"string"},"selected_seed_ids":{"type":"array","items":{"type":"string","format":"uuid"}},"effective_limits":{"$ref":"#/components/schemas/EffectiveDiscoveryPreviewLimits"},"seeds":{"type":"array","items":{"$ref":"#/components/schemas/DiscoveryPreviewSeed"}},"pages":{"type":"array","items":{"$ref":"#/components/schemas/DiscoveryPreviewPage"}},"discovery_paths":{"type":"array","items":{"$ref":"#/components/schemas/DiscoveryPath"}},"summary":{"$ref":"#/components/schemas/DiscoveryPreviewSummary"},"growth_indicators":{"$ref":"#/components/schemas/PreviewGrowthIndicators"},"growth_warnings":{"type":"array","items":{"$ref":"#/components/schemas/PreviewGrowthWarning"}},"warnings":{"type":"array","items":{"$ref":"#/components/schemas/PreviewDiagnostic"}}}}));
    schemas.insert("DiscoveryPreviewSeed", serde_json::json!({"type":"object","required":["seed_id","requested_url","canonical_url","entry_page_type_hint","state","duplicate_of_canonical_url","scope","page_type_match","budget_hits"],"properties":{"seed_id":{"type":"string","format":"uuid"},"requested_url":{"type":"string","format":"uri"},"canonical_url":{"type":"string","format":"uri"},"entry_page_type_hint":{"anyOf":[{"type":"string","format":"uuid"},{"type":"null"}]},"state":{"$ref":"#/components/schemas/PreviewUrlState"},"duplicate_of_canonical_url":{"type":["string","null"],"format":"uri"},"scope":{"anyOf":[{"$ref":"#/components/schemas/DomainScopeEvidence"},{"type":"null"}]},"page_type_match":{"anyOf":[{"$ref":"#/components/schemas/PageTypeMatchEvidence"},{"type":"null"}]},"budget_hits":{"type":"array","items":{"$ref":"#/components/schemas/PreviewBudgetHit"}}}}));
    schemas.insert("DiscoveryPreviewPage", serde_json::json!({"type":"object","required":["requested_url","final_url","canonical_url","depth","state","seed_ids","scope","page_type_match","downloaded_bytes","robots_reason","diagnostic","budget_hits"],"properties":{"requested_url":{"type":"string","format":"uri"},"final_url":{"type":["string","null"],"format":"uri"},"canonical_url":{"type":["string","null"],"format":"uri"},"depth":{"type":"integer","minimum":0},"state":{"$ref":"#/components/schemas/PreviewUrlState"},"seed_ids":{"type":"array","items":{"type":"string","format":"uuid"}},"scope":{"anyOf":[{"$ref":"#/components/schemas/DomainScopeEvidence"},{"type":"null"}]},"page_type_match":{"anyOf":[{"$ref":"#/components/schemas/PageTypeMatchEvidence"},{"type":"null"}]},"downloaded_bytes":{"type":["integer","null"],"minimum":0},"robots_reason":{"type":["string","null"]},"diagnostic":{"anyOf":[{"$ref":"#/components/schemas/TestDiagnostic"},{"type":"null"}]},"budget_hits":{"type":"array","items":{"$ref":"#/components/schemas/PreviewBudgetHit"}}}}));
    schemas.insert("DiscoveryPath", serde_json::json!({"type":"object","description":"A retained raw-href discovery edge with observed-source, canonicalization, scope, PageType, transition, and budget provenance.","required":["seed_id","seed_ids","source_requested_url","source_final_url","source_canonical_url","source_page_type_match","selector","raw_href","resolved_original_url","canonical_url","canonicalization","scope","state","duplicate_of_canonical_url","target_page_type_match","source_depth","prospective_depth","transition_evaluations","budget_hits"],"properties":{"seed_id":{"type":"string","format":"uuid"},"seed_ids":{"type":"array","items":{"type":"string","format":"uuid"}},"source_requested_url":{"type":"string","format":"uri"},"source_final_url":{"type":["string","null"],"format":"uri"},"source_canonical_url":{"type":"string","format":"uri"},"source_page_type_match":{"$ref":"#/components/schemas/PageTypeMatchEvidence"},"selector":{"type":["string","null"]},"raw_href":{"type":"string"},"resolved_original_url":{"type":["string","null"],"format":"uri"},"canonical_url":{"type":["string","null"],"format":"uri"},"canonicalization":{"anyOf":[{"$ref":"#/components/schemas/CanonicalizationEvidence"},{"type":"null"}]},"scope":{"anyOf":[{"$ref":"#/components/schemas/DomainScopeEvidence"},{"type":"null"}]},"state":{"$ref":"#/components/schemas/PreviewUrlState"},"duplicate_of_canonical_url":{"type":["string","null"],"format":"uri"},"target_page_type_match":{"anyOf":[{"$ref":"#/components/schemas/PageTypeMatchEvidence"},{"type":"null"}]},"source_depth":{"type":"integer","minimum":0},"prospective_depth":{"type":["integer","null"],"minimum":0},"transition_evaluations":{"type":"array","items":{"$ref":"#/components/schemas/PreviewTransitionEvaluation"}},"budget_hits":{"type":"array","items":{"$ref":"#/components/schemas/PreviewBudgetHit"}}}}));
    schemas.insert("DiscoveryPreviewSummary", serde_json::json!({"type":"object","description":"Bounded Preview counts; sampled pages are successful observations, discovered PageType URLs arise only from first-unique hrefs, and transition counts are first-unique eligible edges.","required":["pages_sampled","urls_discovered","canonical_unique_urls","duplicates_prevented","page_type_distribution","ambiguous_urls","unmatched_urls","external_urls","blocked_urls","robots_excluded","provider_errors","transition_counts","budget_hit_counts","frontier_remaining","newly_enqueued_urls"],"properties":{"pages_sampled":{"type":"integer","minimum":0},"urls_discovered":{"type":"integer","minimum":0},"canonical_unique_urls":{"type":"integer","minimum":0},"duplicates_prevented":{"type":"integer","minimum":0},"page_type_distribution":{"type":"array","items":{"$ref":"#/components/schemas/PreviewPageTypeDistribution"}},"ambiguous_urls":{"type":"integer","minimum":0},"unmatched_urls":{"type":"integer","minimum":0},"external_urls":{"type":"integer","minimum":0},"blocked_urls":{"type":"integer","minimum":0},"robots_excluded":{"type":"integer","minimum":0},"provider_errors":{"type":"integer","minimum":0},"transition_counts":{"type":"array","items":{"$ref":"#/components/schemas/PreviewTransitionCount"}},"budget_hit_counts":{"type":"object","additionalProperties":{"type":"integer","minimum":0}},"frontier_remaining":{"type":"integer","minimum":0},"newly_enqueued_urls":{"type":"integer","minimum":0}}}));
    schemas.insert("PreviewGrowthIndicators", serde_json::json!({"type":"object","description":"Advisory measured growth evidence, never an exact site-size estimate.","required":["peak_new_canonical_urls_from_one_page","total_newly_enqueued_urls","frontier_remaining","dominant_transition_id","dominant_transition_eligible_edges","total_eligible_transition_edges","dominant_transition_share_percent","query_variant_groups","unmatched_denominator","ambiguity_denominator"],"properties":{"peak_new_canonical_urls_from_one_page":{"type":"integer","minimum":0},"total_newly_enqueued_urls":{"type":"integer","minimum":0},"frontier_remaining":{"type":"integer","minimum":0},"dominant_transition_id":{"type":["string","null"],"format":"uuid"},"dominant_transition_eligible_edges":{"type":"integer","minimum":0},"total_eligible_transition_edges":{"type":"integer","minimum":0},"dominant_transition_share_percent":{"type":["integer","null"],"minimum":0},"query_variant_groups":{"type":"array","items":{"$ref":"#/components/schemas/PreviewQueryVariantGroup"}},"unmatched_denominator":{"type":"integer","minimum":0},"ambiguity_denominator":{"type":"integer","minimum":0}}}));
    schemas.insert("PreviewQueryVariantGroup", serde_json::json!({"type":"object","required":["host","path","total_identities","query_bearing_identities","canonical_query_variants"],"properties":{"host":{"type":"string"},"path":{"type":"string"},"total_identities":{"type":"integer","minimum":0},"query_bearing_identities":{"type":"integer","minimum":0},"canonical_query_variants":{"type":"integer","minimum":0}}}));
    schemas.insert("PreviewGrowthWarningCode", serde_json::json!({"type":"string","enum":["CYCLIC_TRANSITION_DOMINANCE","QUERY_PARAMETER_EXPLOSION","HIGH_UNMATCHED_RATE","WIDESPREAD_PAGE_TYPE_AMBIGUITY","BUDGET_PRESSURE"]}));
    schemas.insert("PreviewGrowthWarning", serde_json::json!({"type":"object","required":["code","message","observed","threshold"],"properties":{"code":{"$ref":"#/components/schemas/PreviewGrowthWarningCode"},"message":{"type":"string"},"observed":{"type":"integer","minimum":0},"threshold":{"type":"integer","minimum":0}}}));
    schemas.insert("PreviewDiagnostic", serde_json::json!({"type":"object","required":["code","message","observed","threshold"],"properties":{"code":{"type":"string"},"message":{"type":"string"},"observed":{"type":["integer","null"],"minimum":0},"threshold":{"type":["integer","null"],"minimum":0}}}));
    schemas.insert(
        "DiscoveryPreviewResultSemantics",
        serde_json::json!({"type":"string","enum":["PREVIEW_ONLY"]}),
    );
    schemas.insert("PreviewUrlState", serde_json::json!({"type":"string","enum":["SAMPLED","IN_SCOPE_MATCHED","AMBIGUOUS_PAGE_TYPE","UNMATCHED","EXTERNAL","BLOCKED","CANONICAL_DUPLICATE","ROBOTS_EXCLUDED","BUDGET_EXCLUDED","PROVIDER_ERROR","INVALID_URL"]}));
    schemas.insert("PreviewBudgetKind", serde_json::json!({"type":"string","enum":["MAX_PAGES","MAX_DEPTH","MAX_DURATION","MAX_DOWNLOADED_BYTES","PAGE_TYPE_PAGE_BUDGET","TRANSITION_PER_SOURCE_PAGE","TRANSITION_TOTAL","PROVENANCE_RETENTION","DIAGNOSTIC_RETENTION"]}));
    schemas.insert("PreviewBudgetHit", serde_json::json!({"type":"object","required":["kind","transition_id","page_type_id","observed","limit"],"properties":{"kind":{"$ref":"#/components/schemas/PreviewBudgetKind"},"transition_id":{"type":["string","null"],"format":"uuid"},"page_type_id":{"type":["string","null"],"format":"uuid"},"observed":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":0}}}));
    schemas.insert("PreviewTransitionEvaluation", serde_json::json!({"type":"object","required":["transition_id","transition_name","source_page_type_id","target_page_type_id","priority","selector_eligible","target_page_type_eligible","constraints_eligible","eligible","budget_hits","diagnostic"],"properties":{"transition_id":{"type":"string","format":"uuid"},"transition_name":{"type":"string"},"source_page_type_id":{"type":"string","format":"uuid"},"target_page_type_id":{"type":"string","format":"uuid"},"priority":{"type":"integer"},"selector_eligible":{"type":"boolean"},"target_page_type_eligible":{"type":"boolean"},"constraints_eligible":{"type":"boolean"},"eligible":{"type":"boolean"},"budget_hits":{"type":"array","items":{"$ref":"#/components/schemas/PreviewBudgetHit"}},"diagnostic":{"anyOf":[{"$ref":"#/components/schemas/PreviewDiagnostic"},{"type":"null"}]}}}));
    schemas.insert("PreviewTransitionCount", serde_json::json!({"type":"object","required":["transition_id","transition_name","eligible_edges","source_pages_with_eligible_edges"],"properties":{"transition_id":{"type":"string","format":"uuid"},"transition_name":{"type":"string"},"eligible_edges":{"type":"integer","minimum":0},"source_pages_with_eligible_edges":{"type":"integer","minimum":0}}}));
    schemas.insert("PreviewPageTypeDistribution", serde_json::json!({"type":"object","required":["page_type_id","page_type_name","discovered_unique_urls","sampled_pages"],"properties":{"page_type_id":{"type":"string","format":"uuid"},"page_type_name":{"type":"string"},"discovered_unique_urls":{"type":"integer","minimum":0},"sampled_pages":{"type":"integer","minimum":0}}}));
    schemas
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;

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
