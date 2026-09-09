//! Generated `OpenAPI` composition for the API wire contract.

mod scalar;
pub(crate) mod wire;

use utoipa::openapi::{Components, Info, OpenApi, Paths};
use utoipa_axum::router::OpenApiRouter;

use crate::{
    AppState, app, crawler_authoring, discovery_policy, discovery_preview, job_actions,
    page_type_authoring, production_run, progress, quick_scrape, test_lab,
};

pub(crate) fn generated_document() -> utoipa::openapi::OpenApi {
    documented_router().into_openapi()
}

pub(crate) use scalar::{asset as scalar_asset, docs as scalar_docs};

pub(crate) fn documented_router() -> OpenApiRouter<AppState> {
    runtime_router()
        .merge(app::liveness_router())
        .merge(app::openapi_document_router())
}

pub(crate) fn runtime_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::<AppState>::with_openapi(base_document())
        .merge(app::openapi_router())
        .merge(crawler_authoring::openapi_router())
        .merge(page_type_authoring::openapi_router())
        .merge(discovery_policy::openapi_router())
        .merge(test_lab::openapi_router())
        .merge(discovery_preview::openapi_router())
        .merge(production_run::openapi_router())
        .merge(job_actions::openapi_router())
        .merge(progress::openapi_router())
        .merge(quick_scrape::openapi_router())
}

fn base_document() -> OpenApi {
    let mut document = OpenApi::new(
        Info::new("Erabi API", env!("CARGO_PKG_VERSION")),
        Paths::new(),
    );
    let mut components = Components::new();
    components.add_security_scheme(
        "erabiBearer",
        utoipa::openapi::security::SecurityScheme::Http(utoipa::openapi::security::Http::new(
            utoipa::openapi::security::HttpAuthScheme::Bearer,
        )),
    );
    document.components = Some(components);
    document
}

/// Returns the generated document as a JSON value for semantic contract tests.
#[cfg(test)]
pub(crate) fn generated_document_json() -> serde_json::Value {
    match serde_json::to_value(generated_document()) {
        Ok(document) => document,
        Err(error) => panic!("generated OpenAPI is not serializable: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn path_methods(document: &serde_json::Value) -> BTreeSet<(String, String)> {
        document["paths"]
            .as_object()
            .into_iter()
            .flat_map(|paths| {
                paths.iter().flat_map(|(path, item)| {
                    item.as_object().into_iter().flat_map(move |operations| {
                        operations
                            .keys()
                            .filter(|method| {
                                matches!(
                                    method.as_str(),
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

    #[test]
    fn document_shell_is_deterministic_and_has_bearer_scheme() {
        let first = generated_document_json();
        let second = generated_document_json();
        assert_eq!(first, second);
        assert_eq!(first["openapi"], "3.1.0");
        assert_eq!(first["info"]["title"], "Erabi API");
        assert_eq!(first["info"]["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(
            first["components"]["securitySchemes"]["erabiBearer"]["type"],
            "http"
        );
        assert_eq!(
            first["components"]["securitySchemes"]["erabiBearer"]["scheme"],
            "bearer"
        );
        assert!(
            first
                .to_string()
                .to_ascii_lowercase()
                .find("token")
                .is_none()
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn generated_contract_covers_every_implemented_stable_operation() {
        let document = generated_document_json();
        let expected = [
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
        .collect::<BTreeSet<_>>();
        assert_eq!(path_methods(&document), expected);
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
            assert!(
                document["paths"].get(reserved).is_none(),
                "reserved path leaked: {reserved}"
            );
        }
    }
}
