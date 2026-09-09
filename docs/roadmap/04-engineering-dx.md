# Erabi Engineering / DX Improvement Track

This document tracks engineering-experience, maintainability, observability, documentation, and architecture hardening that improves how Erabi is developed without changing the product milestone order.

The DX track is subordinate to the canonical product specifications. A DX package must not silently introduce roadmap-only product behavior or change accepted domain semantics.

## Working rules

- Keep each DX package independently reviewable.
- Preserve business behavior unless the package explicitly owns a previously approved semantic change.
- Prefer typed, bounded, fail-closed internal APIs over convention-only safety.
- Do not mix unrelated cleanup into product implementation plans.
- A DX package may be scheduled between MVP plans when doing it first materially reduces rework or risk in the next product plan.
- Implementation plans remain separate from this roadmap and are created only after the package design is accepted.

## Status legend

- `CURRENT` — active package.
- `NEXT` — recommended next package.
- `OPEN` — accepted backlog, not yet started.
- `MERGED` — completed and merged to `main`.

## Current DX sequence

| Package | Status | Purpose |
|---|---|---|
| DX-D01 | MERGED | Harden execution failure, secondary-error, terminal-progress, and worker-disposition semantics. |
| DX-08-04 | MERGED | Stabilize migration checksums across Windows line-ending behavior. |
| DX-D02a | MERGED | Safe structured tracing foundation, bounded telemetry APIs, exact target filtering, and sanitized HTTP request tracing. |
| DX-D02b | MERGED | Semantic runtime observability across jobs, crawler, API, CLI, provider execution, diagnostics, and terminal repair without changing business truth. Merged via PR #12 (`2681c8880f7f23ad72b80418e36532983d517bbe`). |
| **DX-S06** | **MERGED** | **Generated OpenAPI contract and Scalar API reference for the backend.** |
| DX-D03 | OPEN | Decide checkpoint compatibility/version naming without breaking live compatibility. |
| **DX-D04** | **MERGED** | **Reconcile migration-number allocation drift before the next persistence-owning MVP plan.** |
| DX-D05 | MERGED | Clarify Production orchestration ownership/extraction boundaries. |
| DX-D06 | OPEN | Separate semantic traversal responsibilities from service orchestration where warranted. |
| DX-D07 | OPEN | Improve crawler repository private module ownership without changing persistence semantics. |
| DX-S01 | OPEN | Replace crawl-root string routing/classification with private typed routing. |
| DX-S02 | OPEN | Pilot clearer test layout and crate-local test support while preserving true private unit tests inline where necessary. |
| DX-S03 | OPEN | Clean up OpenAPI internals that remain after DX-S06 and derive schema/version facts from authoritative domain contracts where possible. |
| DX-S04 | OPEN | Centralize shared jobs artifact projection mapping. |
| DX-S05 | OPEN | Replace production-source Task/Plan provenance comments with semantic wording where the historical label is not operationally useful. |

The recommended order is not immutable. A package may move earlier when it removes material risk or duplicate work for the next MVP plan.

---

## DX-S06 — Generated OpenAPI & Scalar API Reference

### Why before Plan 07

Erabi already exposes a machine-readable OpenAPI endpoint, while parts of the document/schema assembly are still maintained manually. Plan 07 will add substantial Extraction, Dataset, Review, and Provenance API surface. Moving to generated API contracts before that expansion avoids writing and later replacing a large amount of manual API documentation.

### Goal

Make backend API reference documentation derive as much as practical from the actual Rust wire contracts and route declarations, while preserving Erabi's existing security boundary.

Target shape:

```text
Rust handler + request/response DTOs
              │
              ▼
       generated OpenAPI
              │
      ┌───────┴────────┐
      ▼                ▼
/api/v1/openapi.json  /api/docs
machine contract       Scalar UI
```

### Intended scope

- Audit the current manual OpenAPI document/schema builder and identify the authoritative route/DTO sources.
- Introduce a maintained Rust OpenAPI generation approach compatible with the current Axum stack; `utoipa` / `utoipa-axum` is the preferred candidate unless implementation-time evidence favors another mature option.
- Derive schemas from request/response DTOs where doing so keeps the wire contract authoritative and readable.
- Generate endpoint path/method/request/response/error metadata from code-local declarations rather than duplicating a second large manual document.
- Keep `/api/v1/openapi.json` as the canonical machine-readable contract.
- Add `/api/docs` using Scalar as the single interactive API reference UI.
- Preserve the existing `openapi_enabled` security decision: when OpenAPI is disabled, both the JSON contract and interactive docs must fail closed.
- Prefer local/self-hosted documentation assets and avoid making API documentation availability depend on an external proxy or runtime CDN where practical.
- Add drift/parity tests so documented paths, methods, important schemas, and security behavior cannot silently diverge from the actual API.
- Remove superseded manual OpenAPI assembly only after generated-contract parity is demonstrated.

### Explicit non-goals

DX-S06 does **not** replace:

- product specifications;
- architecture documentation;
- ADRs;
- migration/recovery documentation;
- business invariants and lifecycle semantics;
- operator documentation.

OpenAPI + Scalar own API reference material: routes, methods, parameters, request/response schemas, status codes, authentication requirements, and safe examples.

### Security / DX constraints

- No documentation route may weaken loopback/remote access rules.
- Do not expose secrets, tokens, raw error payloads, internal filesystem paths, or undocumented diagnostic data through generated examples/schemas.
- Do not make Scalar or OpenAPI a second source of business truth.
- Do not add both Swagger UI and Scalar; Scalar is the preferred interactive reference unless a later review finds a concrete incompatibility.
- Keep the generated OpenAPI document testable without requiring a browser.

### Exit evidence

DX-S06 is complete when:

1. current backend endpoints are represented by the generated OpenAPI document with reviewed parity;
2. `/api/docs` renders the same local OpenAPI contract through Scalar;
3. OpenAPI-disabled mode disables both documentation surfaces;
4. route/schema drift tests fail when an API contract changes without corresponding documentation metadata;
5. existing API behavior, status codes, authentication, trace headers, and error envelopes remain unchanged;
6. manual OpenAPI code that is no longer authoritative has been safely removed or reduced to a small composition layer.

### Relationship to DX-S03

DX-S06 owns the **generated API contract and interactive reference migration**. DX-S03 must not duplicate that migration. After DX-S06, DX-S03 is limited to any remaining internal OpenAPI composition cleanup and deriving schema/version metadata from authoritative domain constants where that work is still necessary.

---

## DX-D04 - Migration Allocation Reconciliation

Approved design: [DX-D04 — Migration Allocation and Reservation Design](../superpowers/specs/2026-09-09-dx-d04-migration-allocation-design.md).

### Why before Plan 07

The runtime migration chain already consumes `0006` for crawl traversal state,
while the active Plan 07 and Plan 08 documents previously claimed `0006` and
`0007`. Reconcile the allocation before Plan 07 creates persistence so neither
plan can reuse an implemented migration number.

### Goal

Keep implemented migration history immutable and make future persistence
ownership deterministic through a reviewed, append-only planning ledger.

### Intended scope

- Maintain [`migrations/README.md`](../../migrations/README.md) as the canonical
  planning allocation ledger.
- Record `0001`-`0006` as implemented history, reserve `0007` for Plan 07,
  reserve `0008` for Plan 08, and identify `0009` as next unallocated.
- Reconcile the MVP plan index and active Plan 07/08 migration references with
  those reservations.
- Require fail-closed reconciliation when a plan and the ledger disagree.

### Explicit non-goals

DX-D04 does not change executable SQL, `MigrationRunner`, migration checksums,
database migration state or locking, runtime migration behavior, product
milestone order, or Plan 07/08 persistence and product semantics. Reservations
are planning metadata only; they do not create placeholder SQL or runtime
state.

### Exit evidence

DX-D04 exits with unchanged SQL and runtime migration code, no `0007`/`0008`
placeholder migrations, consistent active planning references, a clean
documentation diff, and an uncommitted candidate ready for independent review.

## DX-D05 - Production Orchestration Ownership

Approved design: [DX-D05 - Production Orchestration Ownership and Extraction Boundary](../superpowers/specs/2026-09-09-dx-d05-production-orchestration-design.md).

DX-D05 is a merged architecture package. It establishes the ownership seam
before Plan 07 adds extraction, separating Production workflow, crawl-stage
orchestration, bounded page execution, and finalization composition while
keeping canonical crawl facts in `erabi-crawler`.

The scope is behavior-preserving: provider, durable evidence, checkpoint,
recovery, progress, cancellation, storage-pressure, retry, and final-status
semantics remain unchanged. Non-goals are extraction implementation,
selector/normalization/validation work, database or API changes, and DX-D06 or
DX-D07 cleanup. Exit evidence was focused and full verification plus
independent review; the package is `MERGED` after accepted independent review
and integration to `main`.

---

## Architecture-wave backlog

These are broader packages and should not be folded into the small DX items above without a dedicated design review:

- dependency direction and business/support crate boundaries;
- functional-core / imperative-shell opportunities;
- crawler ↔ database coupling reduction;
- public module ownership and test-support boundaries;
- repository transaction-boundary review;
- workspace dependency policy;
- compile/test performance work;
- project `xtask` tooling where repeated workflows justify it;
- ADR coverage for stable architectural decisions;
- final architecture audit after the high-value DX packages land.

This backlog is intentionally separate from MVP product capability delivery. The team should pull architecture work forward only when it clearly reduces correctness risk, maintenance cost, or rework for the next product milestone.
