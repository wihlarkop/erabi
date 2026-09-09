# DX-S06 — Generated OpenAPI & Scalar API Reference Implementation Plan

> **For agentic workers:** Execute this plan inline, one task at a time, with a review gate after each task. Do not dispatch subagents. Do not commit or push implementation work until DX-S06 independent review is clean. Erabi uses implementation-first verification; do not introduce TDD as a new project convention.

**Goal:** Replace Erabi's manually assembled OpenAPI contract with generated Rust-owned API metadata, keep `/api/v1/openapi.json` canonical, add a self-hosted Scalar reference at `/api/docs`, and preserve existing security/error/runtime behavior.

**Architecture:** `erabi-api` becomes the sole owner of OpenAPI generation dependencies. API wire DTOs derive `ToSchema`; handlers carry `#[utoipa::path]` metadata; feature-local `OpenApiRouter<AppState>` fragments co-register runtime routes and documentation. A small `openapi` module composes the generated document, security scheme, Scalar surface, and migration-only parity helpers. Existing manual OpenAPI code stays available only until semantic parity is proven, then is removed. Scalar receives a route-scoped CSP exception only for the CSS behavior it requires; the global Erabi CSP is not weakened.

**Tech Stack:** Rust 2024, Axum 0.8.9, utoipa 5.x, utoipa-axum 0.2.x, scalar-api-reference Rust integration, serde/serde_json, existing Erabi security middleware and test infrastructure.

**Spec:** `docs/superpowers/specs/2026-09-09-dx-s06-generated-openapi-scalar-design.md`

## Global Constraints

- Preserve all existing API behavior, status codes, auth, CORS, host/origin policy, trace headers, mutation admission, body limits, SSE behavior, and error envelopes.
- Keep `/api/v1/openapi.json` as the canonical machine-readable contract.
- Add `/api/docs` as the only interactive API-reference UI; do not add Swagger UI.
- `openapi_enabled` controls both `/api/v1/openapi.json` and `/api/docs`; remote exposure remains bearer-protected and disabled by default.
- Keep Utoipa/Scalar dependencies inside `erabi-api`; do not add documentation dependencies to `erabi-domain`, `erabi-db`, `erabi-crawler`, or `erabi-jobs`.
- Do not alter `erabi-db`, migrations, `erabi-crawl4ai`, CI, frontend, or unrelated architecture.
- Do not expose secrets, bearer tokens, raw errors, internal paths, checkpoint/artifact payloads, request bodies, robots data, or diagnostic-only internals through OpenAPI or Scalar.
- Reserved/future fallback routes such as `/api/v1/assets/{*path}`, `/api/v1/exports/{*path}`, `/api/v1/backups/{*path}`, `/api/v1/artifacts/{*path}`, and generic `/api/v1/{*path}` must remain runtime fallbacks but must not appear as implemented OpenAPI operations.
- Keep SSE `/api/v1/events/jobs/{job_id}/progress` represented as `text/event-stream`, not ordinary JSON.
- Do not weaken the global CSP. Scalar-specific CSS allowance is route-scoped to `/api/docs` only. Prefer `style-src-attr 'unsafe-inline'` plus nonce-compatible style handling if Scalar works correctly; otherwise use the minimum route-scoped `style-src 'self' 'unsafe-inline'` fallback. Never add global `'unsafe-inline'`, `unsafe-eval`, CDN script/style sources, proxy, registry, Agent, or token prefill.
- Self-host Scalar JavaScript from the binary via the existing public asset boundary; `/assets/scalar.js` is the preferred URL.
- Keep the existing global security headers on all routes. `/api/docs` may override only its own CSP header after the global header layer has run.
- Legacy manual OpenAPI assembly may be removed only after semantic parity tests pass.
- No provider calls, durable state transitions, recovery flow, queue behavior, or runtime lifecycle may be affected by documentation work.
- Work remains uncommitted/unpushed during implementation and independent review. Commit only after `BLOCKER: 0` and `IMPORTANT: 0`.
- Avoid `cargo clean` unless disk exhaustion makes it genuinely necessary.

---

## File Structure Locked by This Plan

### New files

- `crates/erabi-api/src/openapi/mod.rs`
  - generated OpenAPI document composition;
  - OpenAPI info/security metadata;
  - documented route inventory helpers used by tests;
  - migration-only legacy/generated semantic parity helpers until Task 5 removes legacy assembly.
- `crates/erabi-api/src/openapi/scalar.rs`
  - `/api/docs` response;
  - Scalar configuration built from the same generated document;
  - `/assets/scalar.js` embedded asset response;
  - route-scoped Scalar CSP policy only.
- `crates/erabi-api/tests/openapi_contract.rs`
  - permanent generated-contract inventory/schema/security/SSE tests;
  - temporary semantic parity tests while the legacy builder still exists.

### Existing files intentionally modified

- `crates/erabi-api/Cargo.toml`
- `Cargo.lock`
- `crates/erabi-api/src/lib.rs`
- `crates/erabi-api/src/app.rs`
- `crates/erabi-api/src/error.rs`
- `crates/erabi-api/src/quick_scrape.rs`
- `crates/erabi-api/src/crawler_authoring.rs`
- `crates/erabi-api/src/page_type_authoring.rs`
- `crates/erabi-api/src/discovery_policy.rs`
- `crates/erabi-api/src/discovery_preview.rs`
- `crates/erabi-api/src/test_lab.rs`
- `crates/erabi-api/src/production_run.rs`
- `crates/erabi-api/src/job_actions.rs`
- `crates/erabi-api/src/progress.rs`
- `crates/erabi-api/src/security/headers.rs` only if a narrow helper is needed to apply the route-scoped docs CSP without duplicating the entire global policy.
- `crates/erabi-api/tests/security_shell.rs`
- `crates/erabi-api/tests/request_trace.rs`
- `crates/erabi-observability/src/fields.rs` only to add a closed `RouteTemplate::ApiDocs` capability for `/api/docs`; do not add arbitrary route construction.

### Files explicitly out of scope

- `crates/erabi-domain/**`
- `crates/erabi-db/**`
- `crates/erabi-crawler/**`
- `crates/erabi-jobs/**`
- `crates/erabi-crawl4ai/**`
- `migrations/**`
- `.github/**`
- frontend/UI application source

---

## Task 1: Add the Generated-Contract Foundation Without Replacing Runtime Output

**Files:**
- Modify: `crates/erabi-api/Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `crates/erabi-api/src/lib.rs`
- Create: `crates/erabi-api/src/openapi/mod.rs`
- Create: `crates/erabi-api/tests/openapi_contract.rs`
- Read-only reference during this task: `crates/erabi-api/src/app.rs`

**Interfaces:**
- Produces: `pub(crate) fn generated_document() -> utoipa::openapi::OpenApi`
- Produces: `pub(crate) fn generated_document_json() -> serde_json::Value`
- Produces: a fixed bearer security scheme named `erabiBearer` with HTTP bearer semantics and no token/default value.
- Produces: a test-only semantic inventory helper that extracts `(path, method)` pairs, response statuses, schema names, and security scheme names from an OpenAPI document.
- Does **not** change `/api/v1/openapi.json` yet.

- [ ] **Step 1: Add API-local documentation dependencies.**

Add maintained versions compatible with Axum 0.8:

```toml
utoipa = { version = "5", features = ["axum_extras"] }
utoipa-axum = "0.2"
scalar_api_reference = "0.1"
```

If the currently resolved `scalar_api_reference` release exposes an `axum` feature that is required for its helpers, enable only that feature. Do not add these crates to workspace-wide dependencies or to any business crate.

- [ ] **Step 2: Introduce the private `openapi` module.**

In `lib.rs` add only:

```rust
mod openapi;
```

Do not re-export `utoipa` types publicly from `erabi-api`.

- [ ] **Step 3: Implement the empty generated document shell.**

`generated_document()` must initially produce OpenAPI 3.1 metadata with:

```text
info.title = "Erabi API"
info.version = env!("CARGO_PKG_VERSION")
components.securitySchemes.erabiBearer = HTTP bearer
```

The function must be deterministic and contain no runtime secret/config value. Use Utoipa builders/derive output, not ad-hoc JSON assembly.

- [ ] **Step 4: Add semantic inventory helpers in `openapi_contract.rs`.**

Use `serde_json::to_value` only in tests to derive stable sets such as:

```rust
BTreeSet<(String, String)> // (path, lowercase HTTP method)
BTreeSet<String>           // component schema names
BTreeSet<String>           // security scheme names
```

Do not snapshot the entire JSON document and do not compare serialized ordering.

- [ ] **Step 5: Capture the legacy semantic inventory before migration.**

Call the existing HTTP `/api/v1/openapi.json` route using the loopback router and record the semantic inventory expected from the current manual document. This is a migration guard, not the permanent final assertion set.

At minimum the inventory must include:

```text
GET    /api/v1/health
GET    /api/v1/readiness
GET    /api/v1/diagnostics/status
POST   /api/v1/quick-scrapes
POST   /api/v1/quick-scrapes/batch
GET    /api/v1/crawlers
POST   /api/v1/crawlers
GET    /api/v1/crawlers/{crawler_id}
GET    /api/v1/crawlers/{crawler_id}/versions
GET    /api/v1/crawlers/{crawler_id}/versions/{version_id}
POST   /api/v1/crawlers/{crawler_id}/drafts
POST   /api/v1/crawlers/{crawler_id}/versions/{version_id}/publish
GET    /api/v1/crawlers/{crawler_id}/versions/{version_id}/publish-validation
POST   /api/v1/crawlers/{crawler_id}/versions/{version_id}/reactivate
GET    /api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types
POST   /api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types
GET    /api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}
PUT    /api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}
DELETE /api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}
GET    /api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers
POST   /api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers
GET    /api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers/{matcher_id}
PUT    /api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers/{matcher_id}
DELETE /api/v1/crawlers/{crawler_id}/versions/{version_id}/page-types/{page_type_id}/matchers/{matcher_id}
POST   /api/v1/crawlers/{crawler_id}/versions/{version_id}/match-page-type
GET    /api/v1/crawlers/{crawler_id}/versions/{version_id}/canonicalization
PUT    /api/v1/crawlers/{crawler_id}/versions/{version_id}/canonicalization
POST   /api/v1/crawlers/{crawler_id}/versions/{version_id}/canonicalize-url
GET    /api/v1/crawlers/{crawler_id}/versions/{version_id}/domain-scope
PUT    /api/v1/crawlers/{crawler_id}/versions/{version_id}/domain-scope
POST   /api/v1/crawlers/{crawler_id}/versions/{version_id}/classify-domain-scope
GET    /api/v1/crawlers/{crawler_id}/versions/{version_id}/guardrails
PUT    /api/v1/crawlers/{crawler_id}/versions/{version_id}/guardrails
GET    /api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions
POST   /api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions
GET    /api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions/{transition_id}
PUT    /api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions/{transition_id}
DELETE /api/v1/crawlers/{crawler_id}/versions/{version_id}/transitions/{transition_id}
POST   /api/v1/crawlers/{crawler_id}/versions/{version_id}/test-lab/tests
POST   /api/v1/crawlers/{crawler_id}/versions/{version_id}/discovery-preview
POST   /api/v1/crawlers/{crawler_id}/versions/{version_id}/production-runs
GET    /api/v1/crawlers/{crawler_id}/versions/{version_id}/test-evidence
GET    /api/v1/crawlers/{crawler_id}/versions/{version_id}/test-evidence/{evidence_id}
GET    /api/v1/events/jobs/{job_id}/progress
POST   /api/v1/jobs/{job_id}/retry-failed-parts
POST   /api/v1/jobs/{job_id}/rerun-full-crawl
POST   /api/v1/jobs/{job_id}/resume
POST   /api/v1/jobs/{job_id}/restart
POST   /api/v1/jobs/{job_id}/retry
POST   /api/v1/jobs/{job_id}/cancel
POST   /api/v1/jobs/{job_id}/priority
DELETE /api/v1/jobs/{job_id}
GET    /api/v1/openapi.json
```

The existing manual document may not yet describe every request/response status in detail. Record its current path/method coverage exactly; do not invent missing legacy facts.

- [ ] **Step 6: Run focused foundation verification.**

```powershell
cargo test -p erabi-api --test openapi_contract
cargo check -p erabi-api --all-targets
cargo fmt --all --check
```

Expected: current runtime OpenAPI remains unchanged; the new generated document exists only as an internal foundation.

**Task 1 review gate:** reject the task if documentation dependencies leak outside `erabi-api`, if runtime output changes, or if the semantic comparison depends on JSON ordering.

---

## Task 2: Convert Shared Error/System/Quick-Scrape Contracts and Establish the Route Fragment Pattern

**Files:**
- Modify: `crates/erabi-api/src/error.rs`
- Modify: `crates/erabi-api/src/app.rs`
- Modify: `crates/erabi-api/src/quick_scrape.rs`
- Modify: `crates/erabi-api/src/openapi/mod.rs`
- Modify: `crates/erabi-api/tests/openapi_contract.rs`
- Modify: existing quick-scrape/security tests only when needed to prove unchanged behavior.

**Interfaces:**
- Each feature module that owns documented routes produces `pub(crate) fn openapi_router() -> OpenApiRouter<AppState>` or an equivalently narrow feature-local router helper.
- API wire structs/enums derive `utoipa::ToSchema` only in `erabi-api`.
- `ApiErrorEnvelope` and `Recoverability` become reusable OpenAPI schemas.
- System handlers become annotated operations: health, readiness, diagnostics, OpenAPI document.

- [ ] **Step 1: Add schemas for the stable error envelope.**

Add `ToSchema` to:

```rust
Recoverability
ApiErrorEnvelope
```

Represent `details: Option<serde_json::Value>` as arbitrary JSON, not a fabricated domain schema. Preserve every serde attribute and runtime field exactly.

- [ ] **Step 2: Add API-local schema wrappers when foreign types would otherwise force Utoipa into business crates.**

Do not derive `ToSchema` in domain/jobs/crawler. For a foreign type already serialized directly in a response, choose one of these in `erabi-api`:

```rust
#[schema(value_type = String)]
```

for simple wire values, or an API-owned response DTO mirroring the actual wire shape for complex response bodies.

The wrapper must preserve serialized field names/enum representation exactly; add a focused equality/shape regression where there is any doubt.

- [ ] **Step 3: Annotate system operations.**

Add `#[utoipa::path]` metadata for:

```text
GET /api/v1/health
GET /api/v1/readiness
GET /api/v1/diagnostics/status
GET /api/v1/openapi.json
```

Document only statuses already supported by runtime behavior. `GET /api/v1/openapi.json` may include `200` and `404 ApiErrorEnvelope`; auth middleware failures remain global security behavior and must not change runtime execution.

- [ ] **Step 4: Annotate Quick Scrape single and batch request/response DTOs and handlers.**

Use the existing request/response structs as the wire source of truth. Preserve strict serde behavior (`deny_unknown_fields`, tagged variants, body limits) and existing accepted/error status codes.

Do not document target URLs, source intake internals, robots override reasons, or hidden operational fields beyond what is already present in public request/response DTOs.

- [ ] **Step 5: Establish the `OpenApiRouter` registration pattern.**

For routes that share a path, register handlers together through `utoipa_axum::routes!`, for example conceptually:

```rust
OpenApiRouter::new().routes(routes!(list_crawlers, create_crawler))
```

For `/api/v1/quick-scrapes/batch`, preserve the existing `DefaultBodyLimit::max(QUICK_SCRAPE_BATCH_BODY_LIMIT_BYTES)` only on that route fragment before merging it into the protected API router.

Do not yet remove the legacy manual OpenAPI builder.

- [ ] **Step 6: Extend semantic parity assertions for the migrated operations.**

For each migrated operation assert:

```text
path + method present
request-body presence matches the handler contract
successful response status present
ApiErrorEnvelope referenced for documented error statuses
no reserved wildcard route leaked into generated paths
```

- [ ] **Step 7: Run focused verification.**

```powershell
cargo test -p erabi-api --test openapi_contract
cargo test -p erabi-api --test quick_scrape
cargo test -p erabi-api --test quick_scrape_batch
cargo test -p erabi-api --test security_shell
cargo check -p erabi-api --all-targets
cargo fmt --all --check
```

**Task 2 review gate:** reject if any serde/runtime response changes, if `utoipa` leaks into business crates, or if generated docs claim statuses/fields that runtime does not support.

---

## Task 3: Migrate Crawler Authoring, Page Types, Discovery Policy, Test Lab, Preview, and Production Run

**Files:**
- Modify: `crates/erabi-api/src/crawler_authoring.rs`
- Modify: `crates/erabi-api/src/page_type_authoring.rs`
- Modify: `crates/erabi-api/src/discovery_policy.rs`
- Modify: `crates/erabi-api/src/test_lab.rs`
- Modify: `crates/erabi-api/src/discovery_preview.rs`
- Modify: `crates/erabi-api/src/production_run.rs`
- Modify: `crates/erabi-api/src/openapi/mod.rs`
- Modify: `crates/erabi-api/tests/openapi_contract.rs`
- Reuse existing feature tests under `crates/erabi-api/tests/`.

**Interfaces:**
- Each module owns its DTO schemas, `#[utoipa::path]` handler metadata, and documented `OpenApiRouter<AppState>` fragment.
- Complex business-crate return values are projected through API-owned schema representations; business crates remain documentation-free.

- [ ] **Step 1: Convert crawler authoring DTOs and operations.**

Cover:

```text
GET/POST /api/v1/crawlers
GET      /api/v1/crawlers/{crawler_id}
GET      /api/v1/crawlers/{crawler_id}/versions
GET      /api/v1/crawlers/{crawler_id}/versions/{version_id}
POST     /api/v1/crawlers/{crawler_id}/drafts
POST     /api/v1/crawlers/{crawler_id}/versions/{version_id}/publish
GET      /api/v1/crawlers/{crawler_id}/versions/{version_id}/publish-validation
POST     /api/v1/crawlers/{crawler_id}/versions/{version_id}/reactivate
```

Use `CrawlerDto`, `CrawlerVersionDto`, request DTOs, and an API-local publication-validation schema projection where the current response comes from domain types.

- [ ] **Step 2: Convert PageType and matcher DTOs/operations.**

Cover list/create/read/update/delete PageTypes, list/create/read/update/delete matchers, and `match-page-type`.

Preserve the existing tagged matcher wire contract:

```text
kind = EXACT_URL
kind = EXACT_HOST_PATH_TEMPLATE
kind = PATH_PREFIX
kind = PATH_GLOB
kind = REGEX
```

The generated schema must preserve the discriminator/tag semantics rather than flattening them into an arbitrary object.

- [ ] **Step 3: Convert discovery-policy DTOs/operations.**

Cover canonicalization, canonicalize-url, domain-scope, classify-domain-scope, guardrails, transitions collection, and transition item operations.

Do not put Utoipa on `CanonicalizationPolicy`, `DomainScopePolicy`, `CrawlerVersionGuardrails`, or other business types in `erabi-domain`. When those types are direct wire contracts, express their schema through API-local wrappers or Utoipa `value_type`/schema aliases that match existing serde output.

- [ ] **Step 4: Convert Test Lab and TestEvidence operations.**

Preserve the externally tagged `test_type` request variants and existing `TestEvidenceResponse` wire shape. Do not expose internal artifact payloads or provider details.

- [ ] **Step 5: Convert Discovery Preview schemas/operation.**

Replace `discovery_preview_openapi_schemas()` manual JSON with generated/API-local schema definitions. Preserve the current public result shape, including nullable fields and bounded request DTOs. This is documentation migration only; do not alter preview execution.

- [ ] **Step 6: Convert Production Run operation.**

Document only public request/accepted/error wire fields. Keep seed/crawler validation and durable submission behavior unchanged.

- [ ] **Step 7: Add semantic schema assertions for high-risk shapes.**

At minimum assert generated schema structure for:

```text
MatcherDefinitionRequest discriminator/tag variants
TestLabRequestDto discriminator/tag variants
ApiErrorEnvelope.details optional arbitrary JSON
DiscoveryPreviewRequest required fields
Production Run accepted response IDs
Crawler/CrawlerVersion response ID fields as strings/UUID-formatted where already true
```

Do not attempt byte-for-byte parity with manual JSON schema; compare business-relevant schema semantics.

- [ ] **Step 8: Run focused feature verification.**

```powershell
cargo test -p erabi-api --test crawler_authoring
cargo test -p erabi-api --test page_type_authoring
cargo test -p erabi-api --test discovery_policy_routes
cargo test -p erabi-api --test test_lab
cargo test -p erabi-api --test discovery_preview
cargo test -p erabi-api --test openapi_contract
cargo check -p erabi-api --all-targets
cargo fmt --all --check
```

If a production-run-specific integration test exists, run it here; otherwise rely on existing `erabi-api` package tests in Task 7.

**Task 3 review gate:** reject if documentation changes require a business-crate dependency, if any wire shape drifts, or if manual schema JSON remains necessary for migrated feature contracts without a concrete justified exception.

---

## Task 4: Migrate Job Actions and SSE, Then Reach Full Generated Path/Method Parity

**Files:**
- Modify: `crates/erabi-api/src/job_actions.rs`
- Modify: `crates/erabi-api/src/progress.rs`
- Modify: `crates/erabi-api/src/openapi/mod.rs`
- Modify: `crates/erabi-api/tests/openapi_contract.rs`
- Reuse: `crates/erabi-api/tests/job_actions.rs`
- Reuse: `crates/erabi-api/tests/progress_sse.rs`

**Interfaces:**
- All currently implemented stable API operations are now generated.
- SSE operation explicitly advertises `text/event-stream`.
- Job action request/response schemas are API-owned.

- [ ] **Step 1: Annotate every supported job action.**

Cover:

```text
POST   /api/v1/jobs/{job_id}/retry-failed-parts
POST   /api/v1/jobs/{job_id}/rerun-full-crawl
POST   /api/v1/jobs/{job_id}/resume
POST   /api/v1/jobs/{job_id}/restart
POST   /api/v1/jobs/{job_id}/retry
POST   /api/v1/jobs/{job_id}/cancel
POST   /api/v1/jobs/{job_id}/priority
DELETE /api/v1/jobs/{job_id}
```

Do not expose lease IDs, worker IDs, raw repository errors, or queue internals.

- [ ] **Step 2: Annotate the progress SSE operation.**

Represent successful response content as:

```text
200 text/event-stream
```

Document `Last-Event-ID` as the existing optional request header and keep its validation semantics unchanged. Error responses may use `ApiErrorEnvelope` for the existing 400/404/501/5xx cases that are actually reachable before stream establishment.

Do not model the successful stream as `application/json` and do not alter the actual SSE event encoding.

- [ ] **Step 3: Make full generated path/method parity pass.**

At this point the generated path/method inventory must equal the legacy implemented-operation inventory, with the deliberate exception that reserved/future wildcard fallback routes are absent from both the expected public contract and generated document.

- [ ] **Step 4: Add permanent negative inventory assertions.**

Assert these are absent from generated `paths`:

```text
/api/v1/assets/{*path}
/api/v1/exports/{*path}
/api/v1/backups/{*path}
/api/v1/artifacts/{*path}
/api/v1/events/{*path}
/api/v1/diagnostics/{*path}
/api/v1/{*path}
```

Also assert no browser route (`/`, `/{*path}`, `/assets/{*path}`) appears as an API operation.

- [ ] **Step 5: Run focused verification.**

```powershell
cargo test -p erabi-api --test job_actions
cargo test -p erabi-api --test progress_sse
cargo test -p erabi-api --test openapi_contract
cargo check -p erabi-api --all-targets
cargo fmt --all --check
```

**Task 4 review gate:** full generated path/method parity is mandatory before Task 5 may delete legacy OpenAPI assembly.

---

## Task 5: Switch `/api/v1/openapi.json` to Generated Output and Remove Superseded Manual Assembly

**Files:**
- Modify: `crates/erabi-api/src/app.rs`
- Modify: `crates/erabi-api/src/openapi/mod.rs`
- Modify: `crates/erabi-api/src/crawler_authoring.rs`
- Modify: `crates/erabi-api/src/discovery_preview.rs`
- Modify: `crates/erabi-api/src/test_lab.rs`
- Modify: any migrated module still containing a `*_openapi_schemas()` helper
- Modify: `crates/erabi-api/tests/openapi_contract.rs`
- Modify: `crates/erabi-api/tests/security_shell.rs`

**Interfaces:**
- `GET /api/v1/openapi.json` serializes `generated_document()`.
- Legacy `OpenApiDocument`, `OpenApiPath`, `OpenApiOperation`, manual `BTreeMap<String, Value>` schema builders, and migrated feature-local `*_openapi_schemas()` helpers are deleted.
- Runtime documented routes are composed through `OpenApiRouter`; reserved fallback routes remain ordinary Axum routes layered after documented routes.

- [ ] **Step 1: Replace the OpenAPI HTTP handler body.**

Return the generated Utoipa document through Axum JSON without introducing runtime config into the document:

```rust
Json(openapi::generated_document())
```

or the equivalent owned value required by the actual type.

- [ ] **Step 2: Remove manual OpenAPI document structs and path builders from `app.rs`.**

Delete the entire superseded manual documentation section, including:

```text
OpenApiDocument
OpenApiComponents
OpenApiInfo
OpenApiPath
OpenApiOperation
task2_openapi_schemas
crawler_discovery_openapi_schemas
```

and feature-local manual schema helpers once generated schemas replace them.

Do not remove runtime DTOs/handlers just because they previously lived near manual docs.

- [ ] **Step 3: Preserve router middleware/body-limit ordering.**

After converting documented route groups to `OpenApiRouter`, split to Axum `Router<AppState>` only at the composition boundary. Preserve the current protected route middleware order:

```text
enforce_browser_request_policy
mutation_admission_guard
require_bearer
```

with the same effective behavior as before. Preserve the public liveness route and browser bootstrap outside the protected group.

- [ ] **Step 4: Convert temporary legacy parity test into permanent generated-contract tests.**

Remove dependency on the old manual builder. Keep explicit expected route/method inventory plus permanent tests for:

```text
OpenAPI 3.1.x
info.title == "Erabi API"
info.version == crate version
required component schemas exist
erabiBearer exists and contains no credential/default
SSE uses text/event-stream
reserved/future routes absent
no secret sentinel/token appears anywhere in serialized contract
```

- [ ] **Step 5: Re-run security behavior around OpenAPI.**

Keep the existing semantics exactly:

```text
loopback default: GET /api/v1/openapi.json -> 200
remote default unauthenticated -> 401
remote default authenticated -> 404 OPENAPI_DISABLED
remote explicit enable unauthenticated -> 401
remote explicit enable authenticated -> 200
```

- [ ] **Step 6: Run the migration gate.**

```powershell
cargo test -p erabi-api --test openapi_contract
cargo test -p erabi-api --test security_shell
cargo test -p erabi-api
cargo check -p erabi-api --all-targets
cargo fmt --all --check
```

**Task 5 review gate:** no legacy manual OpenAPI assembly may remain unless the reviewer can identify a specific contract that cannot be represented safely with the generated approach. Runtime route behavior must remain unchanged.

---

## Task 6: Add Scalar `/api/docs`, Self-Hosted Asset, Route-Scoped CSP, and Telemetry Route Classification

**Files:**
- Create: `crates/erabi-api/src/openapi/scalar.rs`
- Modify: `crates/erabi-api/src/openapi/mod.rs`
- Modify: `crates/erabi-api/src/app.rs`
- Modify: `crates/erabi-api/src/security/headers.rs` only if needed for a reusable route-scoped CSP helper
- Modify: `crates/erabi-api/tests/security_shell.rs`
- Modify: `crates/erabi-api/tests/request_trace.rs`
- Modify: `crates/erabi-observability/src/fields.rs`
- Modify: `Cargo.lock` if Scalar feature resolution changes

**Interfaces:**
- Produces: protected `GET /api/docs`.
- Produces: public static asset `GET /assets/scalar.js` from `scalar_api_reference::get_asset_with_mime("scalar.js")`.
- Produces: Scalar config with inline OpenAPI `content`, Agent disabled, no CDN/proxy/registry, no credential prefill.
- Produces: `RouteTemplate::ApiDocs` mapping to the fixed string `/api/docs`.

- [ ] **Step 1: Add a closed observability route capability for docs.**

In `erabi-observability/src/fields.rs`, add only:

```rust
ApiDocs
```

mapping internally to:

```text
/api/docs
```

Do not add a public raw string constructor or change existing telemetry safety rules.

- [ ] **Step 2: Serve `scalar.js` from the existing public asset boundary.**

Update `static_asset_boundary` so that exactly the path `scalar.js` returns the embedded Scalar JavaScript with its MIME type. Existing arbitrary `/assets/{*path}` behavior for the placeholder SPA asset boundary must remain compatible; do not turn the public asset route into a filesystem server.

The implementation must use the embedded asset API, conceptually:

```rust
scalar_api_reference::get_asset_with_mime("scalar.js")
```

No runtime network fetch is allowed.

- [ ] **Step 3: Build Scalar configuration from the same generated document.**

Use inline `content`, not a second browser fetch to `/api/v1/openapi.json`:

```json
{
  "content": { "openapi": "3.1.x", "...": "same generated document" },
  "agent": { "disabled": true }
}
```

Do not set `proxyUrl`, registry URL, CDN URL, authentication token, or default bearer value.

- [ ] **Step 4: Generate a CSP-compliant Scalar shell without weakening global CSP.**

The official Scalar Rust `scalar_html` template contains an inline initialization script, so do **not** use it unchanged under Erabi's current `script-src 'self'` policy.

Use the embedded `scalar.js` bundle and a docs-only HTML shell that initializes Scalar without global inline-script permission. Preferred implementation order:

1. external self-hosted initialization asset if the Scalar bundle can initialize from a DOM-embedded JSON/config element without inline JavaScript;
2. otherwise a narrowly generated nonce on the single initialization `<script>` and a docs-only CSP containing that nonce;
3. never add `'unsafe-inline'` to `script-src`.

For Scalar CSS behavior, first try the narrower docs-only policy:

```text
style-src 'self'; style-src-attr 'unsafe-inline'
```

plus nonce handling for any generated `<style>` elements if required.

If verified Scalar rendering requires broader inline style permission, use only on `/api/docs`:

```text
style-src 'self' 'unsafe-inline'
```

This fallback is permitted by the approved amendment. It must never be applied globally.

- [ ] **Step 5: Ensure the final response carries one CSP header, not conflicting duplicates.**

Because `apply_security_headers` is global, implement docs CSP as a replacement of the response's `Content-Security-Policy` value for `/api/docs`, not an additional contradictory header. All other global security headers (`nosniff`, referrer policy, frame denial, permissions policy, CORP) remain present.

- [ ] **Step 6: Gate `/api/docs` with the same documentation exposure policy.**

The documentation router must behave as:

```text
Loopback + default openapi_enabled=true:
  /api/docs -> 200

Remote + default openapi_enabled=false:
  unauthenticated /api/docs -> 401
  authenticated /api/docs -> 404 OPENAPI_DISABLED

Remote + explicit openapi_enabled=true:
  unauthenticated /api/docs -> 401
  authenticated /api/docs -> 200
```

When disabled, use the existing `OPENAPI_DISABLED` `ApiErrorEnvelope`; do not create a second docs-specific error model.

- [ ] **Step 7: Keep `/api/docs` out of the documented API contract unless explicitly desired as documentation metadata.**

The OpenAPI document describes business/API operations, not the interactive HTML viewer. Do not add `/api/docs` as an OpenAPI operation. `/api/v1/openapi.json` may remain documented because it is the machine-readable API contract endpoint already present in the existing public contract.

- [ ] **Step 8: Add security/CSP/asset tests.**

Tests must assert:

```text
/api/docs HTML contains no external http:// or https:// script/style/proxy source
/api/docs does not contain TOKEN sentinel
/api/docs has CSP compatible with Scalar but only on that route
/api/v1/health retains the original strict global CSP
/assets/scalar.js -> 200 + JavaScript content type
/assets/scalar.js is non-empty
remote docs auth/openapi_enabled matrix matches /openapi.json
Scalar Agent is disabled
serialized docs/config contains no bearer credential/default
```

Also assert request tracing records `/api/docs` as the fixed `RouteTemplate::ApiDocs`, never a raw path.

- [ ] **Step 9: Run focused Scalar/security verification.**

```powershell
cargo test -p erabi-observability
cargo test -p erabi-api --test security_shell
cargo test -p erabi-api --test request_trace
cargo test -p erabi-api --test openapi_contract
cargo check -p erabi-observability --all-targets
cargo check -p erabi-api --all-targets
cargo fmt --all --check
```

**Task 6 review gate:** reject if global CSP is weakened, external assets/network dependencies appear, auth/docs exposure changes, Scalar Agent is enabled, token prefill is possible, or `/api/docs` leaks into browser SPA fallback rather than the protected docs boundary.

---

## Task 7: Final Drift Gates, Full Regression Verification, and Review Handoff

**Files:**
- Modify only tests/docs needed to close a concrete verification gap found in this task.
- No unrelated refactor.

**Interfaces:**
- Permanent generated-contract tests become the future drift guard for Plan 07 and later API expansion.
- Implementation remains uncommitted and unpushed for independent review.

- [ ] **Step 1: Run a source/dependency audit.**

Confirm with repository search/diff:

```text
No `utoipa` dependency outside erabi-api
No `scalar_api_reference` dependency outside erabi-api
No manual `OpenApiDocument`/`OpenApiPath`/`*_openapi_schemas` leftovers for migrated contracts
No CDN/proxy/registry/Agent/token prefill strings
No changes under erabi-db, erabi-crawl4ai, migrations, CI, frontend
No direct business behavior changes mixed into docs work
```

- [ ] **Step 2: Run the permanent contract tests.**

```powershell
cargo test -p erabi-api --test openapi_contract
cargo test -p erabi-api --test security_shell
cargo test -p erabi-api --test request_trace
cargo test -p erabi-api --test progress_sse
cargo test -p erabi-api --test job_actions
cargo test -p erabi-api --test quick_scrape
cargo test -p erabi-api --test quick_scrape_batch
cargo test -p erabi-api --test crawler_authoring
cargo test -p erabi-api --test page_type_authoring
cargo test -p erabi-api --test discovery_policy_routes
cargo test -p erabi-api --test test_lab
cargo test -p erabi-api --test discovery_preview
```

- [ ] **Step 3: Run package/runtime regression tests.**

```powershell
cargo test -p erabi-observability
cargo test -p erabi-api
cargo test -p erabi --lib
cargo test -p erabi --test runtime_server -j 1
```

- [ ] **Step 4: Run compile/lint/format gates.**

```powershell
cargo check -p erabi-observability --all-targets
cargo check -p erabi-api --all-targets
cargo check -p erabi --all-targets
cargo fmt --all --check
cargo clippy -p erabi-observability -p erabi-api -p erabi --all-targets -- -D warnings
git diff --check
```

- [ ] **Step 5: Inspect the final diff for scope and security.**

Review specifically:

```text
OpenApiRouter vs Axum route parity
middleware ordering
batch body limit placement
ApiErrorEnvelope schema only, no error behavior rewrite
SSE content type
openapi_enabled matrix for both docs surfaces
route-scoped CSP only
no secret/token/default bearer in OpenAPI or Scalar HTML/config
no reserved wildcard paths in generated contract
no Utoipa/Scalar business-crate dependency
no manual docs duplicate source of truth
```

- [ ] **Step 6: Prepare the implementation report for independent review.**

The implementation agent must report:

```text
BASELINE
BRANCH
IMPLEMENTATION SUMMARY
DEPENDENCY OWNERSHIP
GENERATED CONTRACT COVERAGE
WIRE-SCHEMA MIGRATION
LEGACY MANUAL OPENAPI REMOVAL
SCALAR SELF-HOSTING
SCALAR CSP STRATEGY
SECURITY / OPENAPI_ENABLED MATRIX
SSE CONTRACT
ERROR CONTRACT
DRIFT TESTS
BEHAVIOR PRESERVATION
FILES CHANGED
FRESH VERIFICATION
SCOPE CHECK
KNOWN LIMITATIONS
CONCLUSION
```

Finish with exactly one of:

```text
DX-S06 IMPLEMENTATION COMPLETE — READY FOR INDEPENDENT REVIEW
```

or

```text
DX-S06 IMPLEMENTATION BLOCKED — REVIEW REQUIRED
```

Then STOP. Do not commit or push.

---

## Final Acceptance Criteria

DX-S06 is ready for independent review only when all of the following are true:

1. Every currently implemented stable backend API operation is represented by generated OpenAPI metadata.
2. Request/response schemas are generated from API-owned wire contracts or safe API-local projections; no documentation dependency leaks into business crates.
3. `/api/v1/openapi.json` serves the generated document and remains behaviorally compatible with the existing security boundary.
4. Scalar renders the same generated document at `/api/docs` with Agent disabled and no external CDN/proxy/registry dependency.
5. `/assets/scalar.js` is self-hosted from the embedded Scalar package.
6. `openapi_enabled=false` fails closed for both machine-readable and interactive docs surfaces.
7. Remote docs remain bearer-protected.
8. The global Erabi CSP is unchanged; any inline-style allowance exists only on `/api/docs`, and inline script remains forbidden except for a nonce-scoped initialization script if absolutely required.
9. Reserved/future fallback routes are absent from generated OpenAPI paths.
10. SSE is documented as `text/event-stream`.
11. `ApiErrorEnvelope` is the documented stable error shape; runtime error codes/status behavior is not redesigned.
12. Manual OpenAPI/schema builders that are superseded by generated contracts are removed after parity.
13. Permanent route/schema/security/SSE drift tests pass.
14. Existing API/security/runtime behavior tests pass without changed expectations except new docs-specific assertions.
15. No DB, migration, Crawl4AI, CI, frontend, or unrelated architecture work enters the diff.

## Independent Review Focus

The reviewer should treat these as high-risk acceptance seams:

- accidental docs dependency in domain/db/crawler/jobs;
- generated route inventory missing an actual implemented Axum route;
- OpenAPI claiming a request/response/status that runtime does not support;
- foreign business types represented inaccurately in API-local schemas;
- middleware or body-limit ordering changed during `OpenApiRouter` migration;
- `ApiErrorEnvelope` runtime shape changed while adding `ToSchema`;
- reserved future routes accidentally presented as implemented;
- SSE documented as JSON;
- remote `/api/docs` bypassing bearer auth or `openapi_enabled`;
- Scalar HTML/config containing a token/default credential, proxy, Registry, Agent, CDN, or external asset;
- global CSP relaxed for Scalar instead of route-scoped handling;
- multiple conflicting CSP headers on `/api/docs`;
- raw route strings added to observability instead of a closed `RouteTemplate::ApiDocs` variant;
- legacy manual OpenAPI remaining as a second authoritative contract after generated parity is established.
