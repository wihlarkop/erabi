# DX-S06 Implementation Plan Self-Review

Reviewed against:

- `docs/superpowers/specs/2026-09-09-dx-s06-generated-openapi-scalar-design.md`
- `docs/superpowers/specs/2026-09-09-dx-s06-scalar-csp-amendment.md`
- `docs/superpowers/plans/2026-09-09-dx-s06-generated-openapi-scalar-implementation-plan.md`

## Spec coverage

- Generated OpenAPI from API wire types and handler-local metadata: Task 1–4.
- `erabi-api`-only Utoipa/Scalar dependency ownership: Global Constraints, Task 1, Task 3, Task 7.
- `/api/v1/openapi.json` retained: Task 5.
- Scalar `/api/docs`: Task 6.
- Same generated document for OpenAPI and Scalar: Task 6.
- `openapi_enabled` fail-closed behavior: Task 5–6.
- Remote bearer protection: Task 5–6.
- Self-hosted Scalar assets: Task 6.
- Scalar Agent/proxy/registry/CDN/token-preload prohibitions: Global Constraints, Task 6–7.
- Route-scoped CSP only, no global weakening: CSP amendment, Task 6.
- Legacy/manual OpenAPI retained until parity and then removed: Task 1–5.
- Reserved/future fallbacks excluded: Global Constraints, Task 4–5.
- SSE represented as `text/event-stream`: Task 4.
- Stable `ApiErrorEnvelope` documented without behavior rewrite: Task 2, Task 5.
- Drift tests: Task 1–5, Task 7.
- No DB/migration/Crawl4AI/CI/frontend/unrelated architecture work: Global Constraints, Task 7.
- Existing runtime/security behavior preservation: every task gate plus Task 7.

No uncovered design requirement found.

## Placeholder scan

Checked the plan for `TODO`, `TBD`, `implement later`, `similar to`, and unspecified generic instructions. None are used as implementation placeholders.

The one intentionally conditional integration choice is Scalar CSP compatibility. It is not unspecified: the approved amendment defines an ordered implementation policy and an explicit safe fallback.

## Type/interface consistency

The plan consistently uses:

```text
generated_document() -> utoipa::openapi::OpenApi
generated_document_json() -> serde_json::Value
feature-local openapi_router() -> OpenApiRouter<AppState>
```

Exact helper visibility or naming may be adjusted by the implementer only if the actual Utoipa/Axum generic signature requires it; the ownership and data-flow boundary must remain identical.

## Project-convention overrides

The generic writing-plans skill suggests TDD, subagent execution, and frequent commits. Erabi's explicit project rules override those defaults:

- implementation-first verification;
- no subagents;
- no implementation commits until independent review accepts the complete package.

The plan reflects the Erabi rules.

## Result

No design gap, placeholder, or material type-boundary inconsistency found.

DX-S06 IMPLEMENTATION PLAN SELF-REVIEW CLEAN
