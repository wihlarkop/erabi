use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use erabi_api::{AppState, SecurityConfig, build_router};
use erabi_domain::{
    CrawlerVersionId, DiscoveryPreviewPage, DiscoveryPreviewResult,
    DiscoveryPreviewResultSemantics, DiscoveryPreviewSummary, EffectiveDiscoveryPreviewLimits,
    ExtractionFieldEvidence, ExtractionObservation, PreviewBudgetKind, PreviewGrowthIndicators,
    PreviewUrlState, TestDiagnostic, TestKind,
};
use serde_json::Value;
use std::net::SocketAddr;
use tower::ServiceExt;

fn loopback_router() -> Result<Router, Box<dyn std::error::Error>> {
    let address: SocketAddr = "127.0.0.1:7878".parse()?;
    Ok(build_router(
        AppState::ready(),
        SecurityConfig::loopback(address)?,
    ))
}

fn path_methods(document: &Value) -> BTreeSet<(String, String)> {
    document
        .get("paths")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|paths| {
            paths.iter().flat_map(|(path, item)| {
                item.as_object().into_iter().flat_map(move |operations| {
                    operations
                        .keys()
                        .filter(|key| {
                            matches!(
                                key.as_str(),
                                "get"
                                    | "post"
                                    | "put"
                                    | "patch"
                                    | "delete"
                                    | "head"
                                    | "options"
                                    | "trace"
                            )
                        })
                        .map(|method| (path.clone(), method.clone()))
                })
            })
        })
        .collect()
}

#[allow(clippy::too_many_lines)]
fn expected_operations() -> BTreeSet<(String, String)> {
    [
        ("/api/v1/health", "get"),
        ("/api/v1/readiness", "get"),
        ("/api/v1/diagnostics/status", "get"),
        ("/api/v1/quick-scrapes", "post"),
        ("/api/v1/quick-scrapes/batch", "post"),
        ("/api/v1/crawlers", "get"),
        ("/api/v1/crawlers", "post"),
        ("/api/v1/crawlers/{crawler_id}", "get"),
        ("/api/v1/crawlers/{crawler_id}/versions", "get"),
        ("/api/v1/crawlers/{crawler_id}/versions/{version_id}", "get"),
        ("/api/v1/crawlers/{crawler_id}/drafts", "post"),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/publish",
            "post",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/publish-validation",
            "get",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/reactivate",
            "post",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types",
            "get",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types",
            "post",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}",
            "get",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}",
            "put",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}",
            "delete",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers",
            "get",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers",
            "post",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers/{matcher_id}",
            "get",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers/{matcher_id}",
            "put",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers/{matcher_id}",
            "delete",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/match-page-type",
            "post",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/canonicalization",
            "get",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/canonicalization",
            "put",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/canonicalize-url",
            "post",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/domain-scope",
            "get",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/domain-scope",
            "put",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/classify-domain-scope",
            "post",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/guardrails",
            "get",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/guardrails",
            "put",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions",
            "get",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions",
            "post",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions/{transition_id}",
            "get",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions/{transition_id}",
            "put",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions/{transition_id}",
            "delete",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/test-lab/tests",
            "post",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/discovery-preview",
            "post",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/production-runs",
            "post",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/test-evidence",
            "get",
        ),
        (
            "/api/v1/crawlers/{crawler_id}/versions/{version_id}/test-evidence/{evidence_id}",
            "get",
        ),
        ("/api/v1/events/jobs/{job_id}/progress", "get"),
        ("/api/v1/jobs/{job_id}/retry-failed-parts", "post"),
        ("/api/v1/jobs/{job_id}/rerun-full-crawl", "post"),
        ("/api/v1/jobs/{job_id}/resume", "post"),
        ("/api/v1/jobs/{job_id}/restart", "post"),
        ("/api/v1/jobs/{job_id}/retry", "post"),
        ("/api/v1/jobs/{job_id}/cancel", "post"),
        ("/api/v1/jobs/{job_id}/priority", "post"),
        ("/api/v1/jobs/{job_id}", "delete"),
        ("/api/v1/openapi.json", "get"),
    ]
    .into_iter()
    .map(|(path, method)| (path.to_owned(), method.to_owned()))
    .collect()
}

fn response_has_status(operation: &Value, status: &str) -> bool {
    operation
        .get("responses")
        .and_then(Value::as_object)
        .is_some_and(|responses| responses.contains_key(status))
}

#[tokio::test]
async fn generated_contract_matches_the_implemented_api_surface()
-> Result<(), Box<dyn std::error::Error>> {
    let response = loopback_router()?
        .oneshot(
            Request::builder()
                .uri("/api/v1/openapi.json")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let document: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;

    assert_eq!(document["openapi"], "3.1.0");
    assert_eq!(document["info"]["title"], "Erabi API");
    assert_eq!(document["info"]["version"], env!("CARGO_PKG_VERSION"));

    let Some(security_schemes) = document["components"]["securitySchemes"].as_object() else {
        return Err("generated bearer security scheme is missing".into());
    };
    assert_eq!(security_schemes.len(), 1);
    assert_eq!(security_schemes["erabiBearer"]["type"], "http");
    assert_eq!(security_schemes["erabiBearer"]["scheme"], "bearer");

    let methods = path_methods(&document);
    assert_eq!(methods, expected_operations());

    let Some(schemas) = document["components"]["schemas"].as_object() else {
        return Err("generated schemas are missing".into());
    };
    for required in [
        "ApiErrorEnvelope",
        "Recoverability",
        "LivenessResponse",
        "ReadinessResponse",
        "RuntimeDiagnosticsResponse",
        "QuickScrapeRequest",
        "QuickScrapeBatchRequest",
        "CrawlerDto",
        "CrawlerVersionDto",
        "PageTypeResponse",
        "MatchDecision",
        "TestLabRequest",
        "TestEvidenceResponse",
        "DiscoveryPreviewRequest",
        "ProductionRunRequest",
        "JobActionResponse",
        "CanonicalizationPolicy",
        "CanonicalizationDecision",
        "DomainScopePolicy",
        "DomainScopeKind",
        "DomainScopeClassification",
        "CrawlerVersionGuardrails",
        "PageTypeDiscoveryGuardrails",
    ] {
        assert!(schemas.contains_key(required), "missing schema {required}");
    }

    assert!(response_has_status(
        &document["paths"]["/api/v1/quick-scrapes"]["post"],
        "202"
    ));
    assert!(response_has_status(
        &document["paths"]["/api/v1/quick-scrapes/batch"]["post"],
        "413"
    ));
    assert_eq!(
        document["paths"]["/api/v1/events/jobs/{job_id}/progress"]["get"]["responses"]["200"]["content"]
            ["text/event-stream"],
        Value::Object(serde_json::Map::new())
    );

    for reserved in [
        "/api/v1/assets/{*path}",
        "/api/v1/exports/{*path}",
        "/api/v1/backups/{*path}",
        "/api/v1/artifacts/{*path}",
        "/api/v1/events/{*path}",
        "/api/v1/diagnostics/{*path}",
        "/api/v1/{*path}",
        "/api/docs",
        "/assets/{*path}",
        "/",
        "/{*path}",
    ] {
        let Some(paths) = document["paths"].as_object() else {
            return Err("generated paths are missing".into());
        };
        assert!(
            !paths.contains_key(reserved),
            "reserved path leaked: {reserved}"
        );
    }

    let serialized = document.to_string().to_ascii_lowercase();
    for sentinel in [
        "dx-s06-test-bearer-secret",
        "default-bearer-token",
        "authorization: bearer",
        "proxyurl",
        "registry",
    ] {
        assert!(
            !serialized.contains(sentinel),
            "secret sentinel leaked: {sentinel}"
        );
    }
    assert!(!serialized.contains("token"));

    Ok(())
}

fn schema_enum_values(
    document: &Value,
    schema_name: &str,
) -> Result<BTreeSet<String>, Box<dyn std::error::Error>> {
    let Some(values) = document["components"]["schemas"][schema_name]["enum"].as_array() else {
        return Err(format!("{schema_name} is not a string enum schema").into());
    };
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("{schema_name} contains a non-string enum value").into())
        })
        .collect()
}

fn object_keys(value: &Value) -> Result<BTreeSet<String>, Box<dyn std::error::Error>> {
    let Some(object) = value.as_object() else {
        return Err("runtime value should be an object".into());
    };
    Ok(object.keys().cloned().collect())
}

fn schema_property_names(
    document: &Value,
    schema_name: &str,
) -> Result<BTreeSet<String>, Box<dyn std::error::Error>> {
    let Some(object) = document["components"]["schemas"][schema_name]["properties"].as_object()
    else {
        return Err("schema should declare object properties".into());
    };
    Ok(object.keys().cloned().collect())
}

fn schema_required_names(
    document: &Value,
    schema_name: &str,
) -> Result<BTreeSet<String>, Box<dyn std::error::Error>> {
    let Some(values) = document["components"]["schemas"][schema_name]["required"].as_array() else {
        return Err("schema should declare required properties".into());
    };
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| "required property name should be a string".into())
        })
        .collect()
}

fn representative_discovery_preview_result() -> DiscoveryPreviewResult {
    DiscoveryPreviewResult {
        result_semantics: DiscoveryPreviewResultSemantics::PreviewOnly,
        crawler_version_id: CrawlerVersionId::new(),
        config_hash: "config-hash".to_owned(),
        selected_seed_ids: Vec::new(),
        effective_limits: EffectiveDiscoveryPreviewLimits {
            max_pages: 1,
            max_depth: 0,
            max_duration_ms: 1,
            max_downloaded_bytes: 1,
            transition_total_limits: Vec::new(),
        },
        seeds: Vec::new(),
        pages: vec![DiscoveryPreviewPage {
            requested_url: "https://example.test/requested".to_owned(),
            requested_canonical_url: "https://example.test/requested".to_owned(),
            final_url: None,
            canonical_url: None,
            depth: 0,
            state: PreviewUrlState::Sampled,
            seed_ids: Vec::new(),
            scope: None,
            page_type_match: None,
            downloaded_bytes: None,
            robots_reason: None,
            diagnostic: None,
            budget_hits: Vec::new(),
        }],
        discovery_paths: Vec::new(),
        summary: DiscoveryPreviewSummary {
            pages_sampled: 1,
            urls_discovered: 1,
            canonical_unique_urls: 1,
            duplicates_prevented: 0,
            page_type_distribution: Vec::new(),
            ambiguous_urls: 0,
            unmatched_urls: 0,
            external_urls: 0,
            blocked_urls: 0,
            robots_excluded: 0,
            provider_errors: 0,
            transition_counts: Vec::new(),
            budget_hit_counts: BTreeMap::from([(PreviewBudgetKind::MaxPages, 1)]),
            frontier_remaining: 0,
            newly_enqueued_urls: 0,
            pagination_truncation_count: 2,
            duration_work_not_expanded: true,
        },
        growth_indicators: PreviewGrowthIndicators {
            peak_new_canonical_urls_from_one_page: 0,
            total_newly_enqueued_urls: 0,
            frontier_remaining: 0,
            dominant_transition_id: None,
            dominant_transition_eligible_edges: 0,
            total_eligible_transition_edges: 0,
            dominant_transition_share_percent: None,
            query_variant_groups: Vec::new(),
            unmatched_denominator: 0,
            ambiguity_denominator: 0,
        },
        growth_warnings: Vec::new(),
        warnings: Vec::new(),
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn generated_enum_schemas_match_authoritative_serde_values()
-> Result<(), Box<dyn std::error::Error>> {
    let response = loopback_router()?
        .oneshot(
            Request::builder()
                .uri("/api/v1/openapi.json")
                .body(Body::empty())?,
        )
        .await?;
    let document: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;

    let expected = [
        (
            "TestKind",
            vec![
                TestKind::UrlCanonicalization,
                TestKind::PageTypeMatching,
                TestKind::Extraction,
                TestKind::SelectorCoverage,
                TestKind::Pagination,
                TestKind::DiscoveryTransition,
                TestKind::DiscoveredUrlPreview,
                TestKind::CombinedUrlEvaluation,
            ]
            .into_iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?,
        ),
        (
            "DiscoveryPreviewResultSemantics",
            vec![DiscoveryPreviewResultSemantics::PreviewOnly]
                .into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        (
            "PreviewUrlState",
            vec![
                PreviewUrlState::Sampled,
                PreviewUrlState::InScopeMatched,
                PreviewUrlState::AmbiguousPageType,
                PreviewUrlState::Unmatched,
                PreviewUrlState::External,
                PreviewUrlState::Blocked,
                PreviewUrlState::CanonicalDuplicate,
                PreviewUrlState::RobotsExcluded,
                PreviewUrlState::BudgetExcluded,
                PreviewUrlState::ProviderError,
                PreviewUrlState::InvalidUrl,
            ]
            .into_iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?,
        ),
        (
            "PreviewBudgetKind",
            vec![
                PreviewBudgetKind::MaxPages,
                PreviewBudgetKind::MaxDepth,
                PreviewBudgetKind::MaxDuration,
                PreviewBudgetKind::MaxDownloadedBytes,
                PreviewBudgetKind::PageTypePageBudget,
                PreviewBudgetKind::TransitionPerSourcePage,
                PreviewBudgetKind::TransitionTotal,
                PreviewBudgetKind::ProvenanceRetention,
                PreviewBudgetKind::DiagnosticRetention,
            ]
            .into_iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?,
        ),
    ];

    for (schema_name, runtime_values) in expected {
        let runtime_values = runtime_values
            .into_iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| format!("{schema_name} runtime value is not a string").into())
            })
            .collect::<Result<BTreeSet<_>, Box<dyn std::error::Error>>>()?;
        assert_eq!(schema_enum_values(&document, schema_name)?, runtime_values);
    }

    let runtime_observations = [
        (
            ExtractionObservation::Available {
                fields: vec![ExtractionFieldEvidence {
                    name: "title".to_owned(),
                    observed: true,
                }],
            },
            "AVAILABLE",
            "fields",
        ),
        (
            ExtractionObservation::Unavailable {
                reason: "provider did not expose extraction hooks".to_owned(),
            },
            "UNAVAILABLE",
            "reason",
        ),
        (
            ExtractionObservation::Error {
                diagnostic: TestDiagnostic {
                    code: "EXTRACTION_FAILED".to_owned(),
                    message: "bounded test diagnostic".to_owned(),
                },
            },
            "ERROR",
            "diagnostic",
        ),
    ];
    let extraction_schema = &document["components"]["schemas"]["ExtractionObservation"];
    let variants = extraction_schema
        .get("oneOf")
        .or_else(|| extraction_schema.get("anyOf"))
        .and_then(Value::as_array)
        .ok_or("ExtractionObservation must be a tagged union")?;
    for (runtime_observation, tag, field) in runtime_observations {
        let serialized = serde_json::to_value(runtime_observation)?;
        assert_eq!(serialized["status"], tag);
        let Some(variant) = variants.iter().find(|variant| {
            variant["properties"]["status"]["enum"]
                .as_array()
                .is_some_and(|values| values.iter().any(|value| value == tag))
        }) else {
            return Err(format!(
                "ExtractionObservation is missing tag {tag}: {}",
                serde_json::to_string(variants)?
            )
            .into());
        };
        assert!(variant["properties"].get(field).is_some());
    }
    assert!(variants.iter().all(|variant| {
        variant["properties"]["status"]["enum"].is_array()
            && variant["properties"].get("Available").is_none()
            && variant["properties"].get("Unavailable").is_none()
            && variant["properties"].get("Error").is_none()
    }));

    Ok(())
}

#[tokio::test]
async fn discovery_preview_response_properties_match_authoritative_serde()
-> Result<(), Box<dyn std::error::Error>> {
    let response = loopback_router()?
        .oneshot(
            Request::builder()
                .uri("/api/v1/openapi.json")
                .body(Body::empty())?,
        )
        .await?;
    let document: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
    let runtime = serde_json::to_value(representative_discovery_preview_result())?;

    assert_eq!(
        object_keys(&runtime)?,
        schema_property_names(&document, "DiscoveryPreviewResult")?
    );
    assert_eq!(
        object_keys(&runtime)?,
        schema_required_names(&document, "DiscoveryPreviewResult")?
    );
    assert_eq!(
        object_keys(&runtime["pages"][0])?,
        schema_property_names(&document, "DiscoveryPreviewPage")?
    );
    assert_eq!(
        object_keys(&runtime["pages"][0])?,
        schema_required_names(&document, "DiscoveryPreviewPage")?
    );
    assert!(runtime["pages"][0].get("requested_canonical_url").is_some());
    assert_eq!(
        object_keys(&runtime["summary"])?,
        schema_property_names(&document, "DiscoveryPreviewSummary")?
    );
    assert_eq!(
        object_keys(&runtime["summary"])?,
        schema_required_names(&document, "DiscoveryPreviewSummary")?
    );
    for field in ["pagination_truncation_count", "duration_work_not_expanded"] {
        assert!(runtime["summary"].get(field).is_some());
    }
    assert_eq!(
        document["components"]["schemas"]["DiscoveryPreviewSummary"]["properties"]["budget_hit_counts"]
            ["additionalProperties"]["type"],
        "integer"
    );

    Ok(())
}
