# DX-S06 — Generated OpenAPI & Scalar API Reference Design

## Status

Approved design for implementation planning.

## Context

Erabi already exposes a protected `/api/v1/openapi.json` endpoint, but the OpenAPI document is assembled manually in `crates/erabi-api/src/app.rs`. The current implementation duplicates API knowledge across:

- Axum route registration;
- manual OpenAPI path registration;
- manual JSON-schema construction;
- feature-local helper schema builders.

This duplication increases drift risk and makes `app.rs` carry documentation responsibilities in addition to top-level router composition and middleware.

Plan 07 will add substantial Extraction, Dataset, Review, Record, and Provenance API surface. DX-S06 moves Erabi to generated API contracts before that expansion.

## Goals

DX-S06 will:

1. generate the machine-readable OpenAPI contract from Rust API wire types and handler-local operation metadata;
2. keep `/api/v1/openapi.json` as the canonical machine-readable endpoint;
3. add `/api/docs` using Scalar as the single interactive API reference;
4. preserve the existing `openapi_enabled` fail-closed security boundary;
5. remove superseded manual OpenAPI assembly only after generated-contract parity is proven;
6. keep documentation dependencies inside `erabi-api` rather than leaking them into business or persistence crates;
7. establish drift tests so future API additions cannot silently become undocumented.

## Non-goals

DX-S06 does not:

- change API business behavior;
- redesign authentication, authorization, CORS, or exposure rules;
- redesign API errors;
- convert reserved/future route fallbacks into implemented API capabilities;
- add Swagger UI alongside Scalar;
- introduce OpenTelemetry, telemetry exporters, or observability changes beyond adding closed route templates for the new documentation route where required;
- refactor `app.rs` or feature modules beyond changes directly required by API-documentation ownership;
- add `utoipa` dependencies to `erabi-domain`, `erabi-db`, `erabi-crawler`, `erabi-jobs`, or `erabi-observability`.

## Selected approach

Use:

- `utoipa` for OpenAPI 3.1 schema and operation generation;
- `utoipa-axum` for documented Axum route composition;
- the official Scalar Rust integration for the interactive API reference;
- local/self-hosted Scalar assets rather than runtime CDN, registry, proxy, or agent dependencies.

`utoipa-axum` is preferred over maintaining a separate Axum router and manual `ApiDoc` path registry because documented routes can be registered once while contributing their OpenAPI metadata to the generated document.

The migration is hybrid rather than a whole-router rewrite: implemented documented routes move through `OpenApiRouter`, while reserved wildcard/fallback routes and browser-shell routes remain ordinary Axum composition.

## Dependency boundary

Documentation dependencies belong only to `erabi-api`:

```text
erabi-domain          no utoipa
erabi-db              no utoipa
erabi-crawler         no utoipa
erabi-jobs            no utoipa
erabi-observability   no utoipa

erabi-api
  ├── utoipa
  ├── utoipa-axum
  └── scalar_api_reference
```

API-facing request/response DTOs may derive `ToSchema` in `erabi-api`.

When an HTTP wire DTO includes a foreign type that should not gain a documentation dependency, the API layer should express the wire representation explicitly, for example with an API-owned wrapper or a schema `value_type` override. Business crates must not derive `ToSchema` solely to support documentation.

## Contract ownership

The desired local ownership pattern is:

```text
feature module
  ├── request DTO + ToSchema
  ├── response DTO + ToSchema
  ├── handler
  ├── utoipa operation metadata
  └── documented route registration
```

The generated document becomes composition of these feature-local contracts.

Internal types remain undocumented unless they are part of the public HTTP wire contract. Examples that must remain internal include:

- DB rows/models;
- repository parameter structs;
- checkpoint state;
- provider payloads;
- traversal work state;
- internal execution diagnostics.

## Router architecture

Target shape:

```text
handler + API DTOs
        │
        ├── #[utoipa::path(...)]
        └── #[derive(ToSchema)]
        │
        ▼
utoipa_axum::OpenApiRouter
        │
        ├───────────────┐
        ▼               ▼
    Axum routes     OpenAPI document
        │               │
        │               ├── GET /api/v1/openapi.json
        │               └── Scalar /api/docs
        │
        └── existing Erabi security/middleware
```

Top-level `app.rs` remains responsible for router composition and middleware, not a large hand-built OpenAPI document.

A small `openapi` module should own document-level metadata and Scalar rendering:

```text
crates/erabi-api/src/openapi/
  ├── mod.rs      document composition, title/version/security metadata
  └── scalar.rs   Scalar HTML/configuration and embedded asset serving
```

This is a targeted responsibility extraction, not a generic module-size refactor.

## Implemented versus reserved routes

The generated API reference documents currently implemented API operations.

Reserved/future wildcard surfaces such as the existing assets/exports/backups/artifacts/fallback groups are not documented as implemented API capabilities merely because Axum has a fallback route for them.

The API reference must describe what Erabi implements, not every route that can produce a rejection response.

## Existing error semantics

DX-S06 preserves current API error behavior.

`ApiErrorEnvelope` remains the stable error wire shape:

```text
code
message
details?
recoverability?
trace_id
```

It should derive `ToSchema` in `erabi-api`. `details: Option<serde_json::Value>` remains arbitrary safe JSON rather than being documented as a fabricated narrower business schema.

Handlers should document the status codes they can actually return using the existing `ApiErrorEnvelope` for error bodies.

Important current behavior that must not be changed by DX-S06:

- resource-not-found paths already map to HTTP 404 where handler semantics define a missing resource, for example `JOB_NOT_FOUND`;
- disabled documentation returns HTTP 404 with `OPENAPI_DISABLED`;
- the generic/reserved `/api/v1/*` fallback is not a generic 404 contract: existing GET behavior returns `501 ROUTE_NOT_AVAILABLE`, while unsupported methods return `405 METHOD_NOT_ALLOWED`;
- DX-S06 documents existing behavior rather than normalizing these statuses.

If later product work wants a different unknown-route policy, that is a separate behavior-changing package.

## Security model

`SecurityConfig` remains authoritative.

Current behavior is preserved:

- loopback mode starts with OpenAPI enabled;
- remote exposure starts with OpenAPI disabled;
- remote exposure may explicitly opt in;
- protected remote documentation remains subject to existing bearer authentication and browser-request policy.

Both documentation surfaces use the same gate:

```text
openapi_enabled = true
  ├── /api/v1/openapi.json available
  └── /api/docs available

openapi_enabled = false
  ├── /api/v1/openapi.json -> 404 OPENAPI_DISABLED
  └── /api/docs            -> 404 OPENAPI_DISABLED
```

No token, secret, configured origin, bind address, or runtime credential is inserted into OpenAPI or Scalar configuration.

The generated contract includes a stable Bearer security scheme for remote/API-client understanding, with explicit documentation that loopback operation does not require bearer authentication while remote exposure does.

Health remains documented according to its actual public security behavior.

## Scalar design

Scalar is the only interactive API reference UI.

The generated `OpenApi` value is the single contract source for both outputs:

```text
generated OpenApi
  ├── serialize -> /api/v1/openapi.json
  └── provide directly to Scalar -> /api/docs
```

Scalar should not need to perform a second authenticated fetch of `/api/v1/openapi.json` merely to render the same in-process document.

Use local/self-hosted assets supplied by the official Rust integration. Do not depend on:

- external CDN at runtime;
- Scalar Registry;
- Scalar proxy;
- Scalar Agent;
- prefilled bearer tokens.

Generic embedded JS assets may be served through an existing safe static-asset boundary if that can be done without weakening API-document exposure. The OpenAPI document itself remains protected by the documentation gate.

## Observability route safety

DX-D02 established a closed route-template vocabulary. Adding `/api/docs` or any dedicated Scalar asset route must not create a raw-path logging escape hatch.

If a new route is visible to request tracing, add only the required closed `RouteTemplate` variant(s) in `erabi-observability`. Do not redesign the observability API.

Prefer reusing the existing generic `/assets/{*path}` route for Scalar assets when practical so additional closed documentation-asset route variants are unnecessary.

## SSE documentation

`GET /api/v1/events/jobs/{job_id}/progress` remains documented as an SSE operation.

The OpenAPI response must represent `text/event-stream`, not pretend the HTTP response is an ordinary JSON object.

DX-S06 should document the stable stream semantics that actually exist, but should not invent a richer event schema merely for documentation if the current SSE contract is not represented by a stable reusable wire DTO.

## Migration strategy

The migration must not be a blind replacement of the existing manual document.

### Phase A — generated contract alongside legacy contract

Build the generated OpenAPI document internally while retaining the existing manual document.

Add semantic parity checks between the two representations.

### Phase B — feature route/schema migration

Migrate implemented API groups incrementally, for example:

- health/readiness/diagnostics;
- Quick Scrape;
- crawler authoring;
- PageType/matcher authoring;
- discovery policy and preview;
- Test Lab/evidence;
- Production Run;
- progress SSE;
- job actions.

Exact implementation checkpoint grouping belongs in the implementation plan rather than this design.

### Phase C — generated contract becomes canonical

Only after parity is demonstrated:

- switch `/api/v1/openapi.json` to the generated document;
- remove the superseded manual `OpenApiDocument`, `OpenApiPath`, `OpenApiOperation`, and manual schema builders that are no longer authoritative.

### Phase D — Scalar

Add `/api/docs` using the same generated contract, preserve the security gate, and serve local assets.

## Parity strategy

Do not compare serialized JSON byte-for-byte. Library ordering and equivalent OpenAPI representations may differ.

Temporary migration parity tests should compare contract semantics, including:

- implemented paths;
- HTTP methods;
- request-body presence where applicable;
- response status sets;
- important schema presence;
- OpenAPI version;
- security scheme presence.

A generated contract missing a currently documented implemented operation is a migration failure.

The temporary legacy-versus-generated parity harness may be removed after the manual document is removed.

## Permanent drift tests

After migration, permanent tests must verify at least:

- generated OpenAPI version is 3.1;
- expected implemented route inventory is present;
- expected methods are present per route;
- required wire schemas are present;
- `ApiErrorEnvelope` is present;
- bearer security scheme is present;
- SSE uses `text/event-stream`;
- reserved/future fallback routes are absent from implemented API documentation;
- `/api/v1/openapi.json` serves the generated contract when enabled;
- `/api/docs` renders from the same generated contract when enabled;
- both documentation surfaces fail closed when `openapi_enabled` is false.

Feature-local tests should continue to verify API behavior separately from documentation metadata.

## Behavior-preservation constraints

DX-S06 must not change:

- API route paths or methods;
- status codes;
- request/response JSON wire shapes;
- `ApiErrorEnvelope` behavior;
- trace header behavior;
- authentication requirements;
- CORS/browser-origin policy;
- host policy;
- mutation admission behavior;
- Quick Scrape or Production semantics;
- job action semantics;
- SSE replay semantics;
- reserved-route rejection semantics.

Documentation metadata must observe and describe existing behavior, not become a second business-rules implementation.

## Expected file ownership after migration

`app.rs` should retain router/middleware/browser composition while large manual OpenAPI builders disappear.

Feature modules own their request/response schema and operation annotations.

The new `openapi` module owns only cross-cutting API-reference composition, security scheme metadata, document info, and Scalar rendering.

DX-S03 remains responsible only for any small residual OpenAPI composition cleanup or deriving remaining schema/version facts from authoritative domain constants after DX-S06. It must not duplicate this migration.

## Verification gate

Implementation verification should include, at minimum:

- focused generated-contract/parity/drift tests;
- documentation security-gate tests;
- Scalar route and asset tests;
- `cargo test -p erabi-api`;
- relevant CLI/runtime-server integration tests where router behavior is exercised;
- `cargo check -p erabi-api --all-targets`;
- any affected workspace checks required by dependency changes;
- `cargo fmt --all --check`;
- strict Clippy for affected crates with `-D warnings`;
- `git diff --check`.

Heavy gates should be run once at implementation acceptance rather than repeatedly when production code has not changed.

## Acceptance criteria

DX-S06 is complete only when all of the following hold:

1. all currently implemented backend operations intended for public API reference are represented by generated OpenAPI with reviewed parity;
2. request/response schemas are derived from API wire types where appropriate;
3. documentation dependencies do not leak into business/persistence crates;
4. `/api/v1/openapi.json` is generated rather than manually assembled;
5. `/api/docs` renders the same generated contract through Scalar;
6. Scalar assets are self-hosted/local and do not require runtime CDN, proxy, registry, or agent services;
7. `openapi_enabled` controls both documentation surfaces fail-closed;
8. existing resource 404s, reserved-route 501/405 behavior, error envelopes, auth, status codes, and trace headers are unchanged;
9. reserved/future fallback routes are not presented as implemented API capabilities;
10. SSE is documented with `text/event-stream` semantics;
11. permanent route/schema/security drift tests exist;
12. superseded manual OpenAPI path/schema assembly has been safely removed or reduced to a genuinely small composition layer;
13. no unrelated DX or architecture-wave refactor is included.

## Implementation gate

Implementation must not begin until this design spec is reviewed and approved. After approval, create the detailed implementation plan using the Erabi implementation-first workflow. No TDD mandate, subagent workflow, or commit-per-checkpoint requirement is introduced by this design.
