# Plan 06 Execution Design — Crawl4AI, Quick Scrape, and Production Crawl

**Status:** Approved supplemental design for Plan 06 execution  
**Date:** 2026-08-29  
**Canonical plan:** [`docs/superpowers/plans/2026-08-22-06-crawl4ai-and-quick-scrape.md`](../plans/2026-08-22-06-crawl4ai-and-quick-scrape.md)  
**Canonical specs:** [`01-product-and-experience.md`](../../specs/01-product-and-experience.md), [`03-discovery-graph-and-runs.md`](../../specs/03-discovery-graph-and-runs.md), [`06-security-reliability-and-operations.md`](../../specs/06-security-reliability-and-operations.md)

## Authority and purpose

This document does not replace or extend the canonical product specification. It freezes the architecture and delivery boundaries used to execute Plan 06 safely after Plan 05. If this document conflicts with `docs/specs/`, the canonical specs win and implementation stops until the conflict is reconciled.

The canonical Plan 06 defines five capability groups. This supplemental design preserves all of them but decomposes delivery into nine independently reviewable tasks so adapter integration, persistence, intake, Quick Scrape, production traversal, and recovery do not land as one oversized change.

## Goals

Plan 06 must:

- integrate unmodified Crawl4AI behind a stable Erabi-owned adapter;
- keep Erabi as the authoritative durable job, progress, retry, cancellation, and checkpoint owner;
- create/reuse durable `Source` identity for Quick Scrape and retained crawl history without mutating Crawler Seeds;
- classify confident direct non-HTML targets before HTML extraction;
- enforce robots, User-Agent, per-domain pacing, and conservative `429 Retry-After` handling;
- make one-URL Quick Scrape a first-class execution path without a CrawlerVersion;
- implement bounded pasted batch as an ordered submission envelope over independent Quick Scrape runs;
- add the Plan 06 execution persistence migration without changing historical migrations;
- execute bounded production crawls only from Published CrawlerVersions while reusing Plan 05 discovery semantics;
- integrate Plan 04 recovery/progress infrastructure and Plan 05 complete-snapshot structural health;
- leave Plan 07 extraction/schema health and Plan 08 physical asset-download ownership open through explicit seams.

## Non-goals

Plan 06 must not implement:

- a fifth `BATCH` run type;
- a second durable queue owned by Crawl4AI;
- arbitrary browser click/fill/login workflows;
- sitemap, RSS, CSV, or JSONL bulk submission;
- Dataset, record curation, unique-key validation, schema-drift detection, or `MISSING_CANDIDATE` creation;
- full physical Asset lifecycle, archive extraction, export, retention, or backup behavior from Plan 08;
- SvelteKit product UI;
- hosted CI or release automation;
- anti-bot/CAPTCHA bypass behavior.

## Starting contracts inherited from Plans 01–05

Plan 06 builds on existing contracts rather than redesigning them:

- exactly four run types already exist: `QUICK_SCRAPE`, `TEST_RUN`, `DISCOVERY_PREVIEW`, `PRODUCTION_RUN`;
- `CrawlRunSnapshot` already freezes Quick Scrape ad-hoc configuration or CrawlerVersion semantic identity, resolved operational settings, robots decision/reason, User-Agent, actor/time, snapshot hash, and checkpoint compatibility hash;
- `SourceTargetType` already distinguishes `WEB_PAGE` and `FILE_ASSET`;
- `crawl_runs`, `discovered_urls`, and generic `artifacts` metadata already exist in migration `0003_runs.sql`;
- Plan 04 already owns durable jobs, leases, progress, cancellation, checkpoints, retry/resume actions, and storage-pressure signals;
- Plan 05 already owns canonicalization, domain scope, PageType matching, transition validation, discovery budgets, publication validation, Discovery Preview semantics, and the pure complete-snapshot structural gate.

Plan 06 should extend these boundaries, not create parallel versions.

---

## 1. Durable execution ownership

### Decision

Erabi is the only authoritative durable orchestration layer.

The execution topology is:

```text
Erabi CrawlRun
    -> Erabi durable root/page Job
    -> Erabi crawl orchestration service
    -> CrawlerAdapter
    -> erabi-crawl4ai HTTP adapter
    -> Crawl4AI crawl engine
```

Crawl4AI may internally perform browser work, but its own asynchronous job API must not become the durable source of truth for Erabi run lifecycle, retry lineage, checkpoints, cancellation state, or progress replay.

### Rationale

Plan 04 already solved durable leasing, retry, checkpoints, recovery, SSE progress, and shutdown. Delegating durable ownership to an upstream queue would create two independent retry/cancellation/checkpoint models and make recovery ambiguous.

### Upstream compatibility boundary

Current upstream Docker deployments expose health/schema and crawl-oriented HTTP endpoints, but their exact paths, request DTOs, authentication defaults, and deployment behavior can change by Crawl4AI release. `erabi-crawl4ai` therefore owns all upstream path/DTO mapping. No domain/service/API type may encode an upstream route name or raw Crawl4AI DTO.

The initial Erabi adapter should prefer one bounded non-streaming crawl operation per Erabi work unit. Upstream asynchronous job APIs or streaming endpoints are not authoritative Erabi lifecycle mechanisms and are not required for Plan 06. If later used as an optimization, they remain hidden behind `CrawlerAdapter` and cannot change durable semantics.

Release validation in Plan 10 must smoke-test the exact supported Crawl4AI release/image rather than assuming `latest` behavior.

---

## 2. Stable `CrawlerAdapter` boundary

### Ownership

- `erabi-crawler`: provider-neutral adapter contract and deterministic mock.
- `erabi-crawl4ai`: Reqwest/Rustls implementation and upstream HTTP/JSON mapping.
- `erabi-api`/`erabi-jobs`: consume provider-neutral application services only.

### Required capabilities

The stable contract must cover:

- provider health/version/capability observation;
- one bounded crawl execution request;
- normalized final URL, HTTP/content metadata, rendered/raw/cleaned/Markdown evidence when available, links, screenshot/reference metadata, timing, and partial-result state;
- best-effort cancellation signal where the provider supports it without making it the durable cancellation authority;
- normalized typed errors.

The contract must not expose:

- raw Crawl4AI request/response structs;
- upstream URL paths;
- upstream task IDs as Erabi durable identities;
- provider tokens in `Debug`, tracing, API errors, persisted failure messages, or progress events.

### Deterministic mock

The mock is a first-class test provider. It never touches the network and supports deterministic fixtures for at least:

- success;
- timeout;
- access denied;
- not found;
- provider unavailable;
- partial result;
- redirect/final URL;
- non-HTML final content classification;
- controlled links/artifact evidence.

Provider fixture identity is semantic and deterministic; tests must not rely on random ordering or wall-clock behavior.

---

## 3. Execution persistence and migration `0005`

### Decision

Create `migrations/0005_crawl_execution.sql` before Quick Scrape and production execution services need it.

Do not edit migrations `0001`–`0004`.

### Ownership boundary

Migration `0005` stores crawl execution results/summaries that are missing from `0003_runs.sql`. It must not absorb Plan 07 curated records or Plan 08 physical asset/export/backup state.

The migration should provide typed durable projections for:

- per-page crawl execution result/outcome tied to one `crawl_run` and canonical URL identity;
- requested URL and authoritative observed final URL;
- provider-neutral HTTP/content metadata;
- PageType/transition/provenance references where applicable;
- execution status/error classification;
- persisted artifact references, not duplicate artifact payload storage;
- bounded execution counters/summaries needed to finalize run status and complete-snapshot structural input.

### Integrity expectations

- row IDs and JSON payload identities must agree where both exist;
- run ownership must be version-local/run-local and fail closed on corruption;
- completed counts may never exceed planned counts;
- finalization summaries are deterministic and auditable;
- generic `artifacts` remain the metadata index for stored evidence; execution rows reference artifacts rather than embedding large raw bodies.

---

## 4. Source intake and direct-file classification

### Source identity

Quick Scrape and retained crawl history create or reuse `Source` by stable URL identity according to existing canonicalization rules. Source is supporting target/history identity only.

Creating/reusing a Source must never:

- add a Seed;
- rewrite a Seed;
- change CrawlerVersion semantic configuration;
- silently move a Source/Crawler between Collections.

### Intake decision

Before normal HTML crawl, Erabi may perform a bounded safe content-type probe. The decision is:

```text
input URL
-> parse/validate
-> canonicalize
-> create/reuse Source identity
-> bounded safe content-type probe when appropriate
   -> confident non-HTML => FILE_ASSET intake metadata
   -> HTML => normal crawl path
   -> unavailable/ambiguous => normal crawl path, classify authoritative final response later
```

Confident direct PDF/CSV/JSON/archive/image/office-like content becomes `SourceTargetType::FileAsset` and must not enter HTML extraction/preview.

### Plan 08 boundary

Plan 06 owns classification, provenance, content metadata, and an explicit safe-download capability boundary. It does not implement the complete Plan 08 physical Asset subsystem.

Plan 06 must not automatically:

- execute/open downloaded files;
- extract archives;
- parse arbitrary direct files into Dataset records;
- invent full retention/export semantics.

If a minimal bounded probe body is required for content classification, it is not a user Asset download and must obey strict response-size and timeout limits.

---

## 5. Robots, User-Agent, and per-domain pacing

### Snapshot ownership

The immutable `CrawlRunSnapshot` remains authoritative for:

- robots respect/override decision;
- exact validated non-empty override reason;
- actor/time/scope;
- active User-Agent;
- resolved request delay/concurrency/timeout settings.

A new independent run never copies a prior run's override reason implicitly. Retry/resume of the same immutable run may use the reason already frozen in that run.

### Robots policy

Erabi respects robots by default. The Plan 06 policy/cache evaluates the User-Agent-relevant Allow/Disallow semantics and Crawl-delay needed by MVP. Robots fetch/parse failure must follow a conservative documented policy and produce typed evidence; it may not silently behave as an override.

### Per-domain limiter

Quick Scrape and Production share the same origin/domain pacing service. No route or batch mode gets a hidden bypass.

The limiter must combine:

- resolved snapshot concurrency/request-delay settings;
- robots Crawl-delay where applicable;
- conservative bounded backoff;
- `429 Retry-After` when present and valid.

A malformed/excessive `Retry-After` is clamped to a bounded safe policy rather than trusted unboundedly.

---

## 6. Quick Scrape single URL as the primitive

### Decision

Single-URL Quick Scrape is the only Quick Scrape execution primitive. Batch submission delegates to it item by item.

A successful submission creates, atomically where ownership requires:

- a create/reused Source association;
- immutable `CrawlRunSnapshot` using `RunConfiguration::QuickScrape`;
- one independent `CrawlRun` of exactly `QUICK_SCRAPE`;
- one durable root job with explicit run association;
- durable progress/retry/cancellation identity.

The run does not require a Crawler or CrawlerVersion.

### Execution

The worker uses the shared Source intake, robots/pacing, adapter, execution persistence, artifact metadata, and Plan 04 progress/cancellation services. A provider outage fails the crawl action with stable provider-unavailable semantics without making existing Erabi data unavailable.

---

## 7. Quick Scrape bounded batch

### Decision

Batch is an ordered request/response envelope, not an entity or run type.

For each input item in original order:

- validate independently;
- create/reuse Source independently;
- submit an independent single-URL Quick Scrape when accepted;
- return one per-item outcome containing accepted run identity or stable validation/conflict error.

One invalid/conflicting item cannot roll back unrelated accepted items.

The batch layer owns only:

- input count/size bounds;
- input order preservation;
- per-item outcome aggregation.

It does not own shared execution lifecycle, cancellation, retry, or a batch-level CrawlRun.

---

## 8. Production crawl orchestration

### Published-only requirement

A normal `PRODUCTION_RUN` must reference a Published CrawlerVersion. Draft Test/Preview behavior remains owned by existing Plan 05 flows and is not broadened here.

### Reuse Plan 05 semantic pipeline

Production scheduling must reuse the authoritative canonical discovery semantics already implemented for Plan 05. It must not fork resolver/canonicalizer/matcher/budget logic.

The scheduling sequence remains:

```text
raw discovered href
-> resolve against observed source/final base URL
-> validate URL
-> canonicalize
-> domain-scope classification
-> deduplicate
-> PageType matching
-> transition validation
-> budget checks
-> enqueue or preserve-only decision
-> provider crawl/render for admitted work
-> persist page/artifact/provenance evidence
```

For each discovered URL, preserve enough evidence to explain matched, ambiguous, unmatched, external, blocked, duplicate, budget-excluded, or completed state.

### Bounded traversal

Production traversal is bounded by the immutable run snapshot and CrawlerVersion guardrails. Cycles, pagination, query growth, redirects, and duplicate canonical identities may never create unbounded scheduling.

### Artifact evidence

Persist configured raw/cleaned/rendered/Markdown/screenshot/link evidence through existing artifact metadata boundaries and safe controlled storage behavior available in the repository. Plan 06 does not introduce Plan 08 export/retention semantics.

---

## 9. Recovery, finalization, and complete-snapshot health

### Plan 04 integration

Crawl workers use Plan 04 durable primitives for:

- leasing/heartbeat;
- cooperative cancellation;
- progress publication;
- checkpoint persistence;
- retry failed parts;
- full rerun lineage;
- resume compatibility;
- shutdown/storage-pressure safe boundaries.

Checkpoint state for crawling must preserve enough deterministic information to resume without recomputing a different semantic frontier: completed/pending canonical URLs, provenance/scheduling state, pagination/partial state, failed units, artifact references, and immutable run compatibility evidence.

Resume is rejected when checkpoint compatibility no longer matches the immutable run snapshot.

### Run finalization

Final status and structural counters are derived from durable execution state, not only in-memory worker completion.

At minimum finalization derives:

- planned in-scope page count;
- completed in-scope page count;
- pagination truncation count;
- unresolved partial-work count;
- PageType ambiguity count;
- final run lifecycle status.

Impossible count relationships are corruption/invariant failures, not plausible user results.

### Complete snapshot

Plan 06 feeds those actual structural facts into Plan 05 `CompleteSnapshotStructuralInput`.

Until Plan 07 provides required extraction health, the caller must use the correct explicit `ExtractionHealth` state:

- `NotRequired` only when extraction genuinely is not required by the production contract;
- `NotEvaluated` when required extraction health has not yet been evaluated;
- `Healthy`, `CriticalFailure`, or `ProductionBreakingSchemaDrift` only from the owning semantic layer.

Plan 06 must not invent a schema-drift engine or `USE_ANYWAY` path.

A `PARTIAL_RESULT`, `FAILED`, `CANCELLED`, ambiguity-bearing, truncated, or structurally incomplete run cannot be marked complete.

Plan 06 does not create `MISSING_CANDIDATE`; Plan 07 consumes complete healthy production semantics later.

---

## Provider-neutral error model

Adapter and orchestration code use stable Erabi errors. Upstream strings are diagnostic input only and must not become public API contracts.

Required semantic classes include:

- provider unavailable;
- timeout;
- access denied;
- not found;
- robots excluded;
- rate limited/retry scheduled;
- invalid provider response/contract violation;
- partial provider result;
- cancelled;
- persisted execution state invalid.

Ordinary page/provider failures can contribute to `PARTIAL_RESULT` when the bounded run remains structurally meaningful. Provider contract violations, impossible durable state, and corrupted identities fail closed.

Sensitive upstream headers, tokens, raw bodies, and query parameters are redacted according to the security spec.

## API boundary

Plan 06 API additions should remain focused application contracts, not Crawl4AI facades.

Expected surfaces include:

- Quick Scrape single submission;
- bounded Quick Scrape batch submission;
- production run submission for an eligible Published CrawlerVersion;
- existing run/job progress and action endpoints reused where possible;
- connection/health status exposed through a normalized Erabi representation when needed.

Mutation routes preserve existing Host/Origin/Content-Type/body-limit/auth/trace conventions. They return stable typed Erabi errors and never expose provider tokens or raw provider DTOs.

## Security constraints

Implementation must preserve:

- loopback/default security and existing non-loopback bearer requirements;
- SSRF-aware URL validation and domain-scope enforcement before provider execution where applicable;
- no secret token in logs/errors/Debug;
- bounded request/response/probe sizes and timeouts;
- no automatic execution/opening/extraction of untrusted files;
- no hidden concurrency bypass;
- cooperative cancellation rather than arbitrary unsafe task abortion;
- three-second application shutdown contract through Plan 04 recovery semantics.

## Testing strategy

Each task ends with focused package/integration verification before coordinator review. Tests use deterministic local/mock HTTP fixtures; routine test suites must not depend on a live Crawl4AI service or the public internet.

Required coverage across Plan 06 includes:

- adapter normalization and token redaction;
- provider outage/timeout/access-denied/not-found/partial fixtures;
- Source reuse and Seed independence;
- direct PDF/CSV/JSON/archive/image/office-like classification and ambiguous fallback;
- robots allow/disallow/Crawl-delay and override semantics;
- shared per-domain pacing and bounded `Retry-After` behavior;
- Quick Scrape immutable snapshot/run/job/progress identity;
- ordered mixed-validity batch and independent failure/cancel/retry identities;
- migration/projection corruption tests;
- Published-only production execution;
- bounded cyclic discovery and canonical dedupe;
- cancellation/checkpoint/resume/retry;
- partial failure and structural complete-snapshot decisions.

The final Plan 06 gate remains exactly:

```text
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

No Plan 07 work begins until that gate passes from a clean checkout.

## Delivery decomposition

The approved review sequence is:

1. CrawlerAdapter contract + deterministic mock.
2. Crawl4AI HTTP adapter.
3. Crawl execution persistence / migration `0005`.
4. Source intake + direct-file classification.
5. Robots + per-domain pacing.
6. Quick Scrape single URL.
7. Quick Scrape bounded batch.
8. Production crawl orchestration.
9. Recovery + complete-snapshot finalization.

Each task is a separate implementation/review boundary. Remediation after a review stays within the owning task before the next task begins.
