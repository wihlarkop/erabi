# Plan 06 Supplemental Execution Plan — Crawl4AI and Quick Scrape

> **For agentic workers:** This is the approved task decomposition for canonical Plan 06. Implement one task end-to-end, compile/check it, add or update meaningful verification, run the task gate, commit, push, and stop for review. Erabi uses implementation-first verification-after sequencing; do not intentionally create failing tests first.

**Goal:** Deliver canonical Plan 06 through nine independently reviewable tasks while preserving Plans 01–05 contracts and leaving Plan 07/08 ownership intact.

**Architecture:** Erabi remains the authoritative durable run/job/recovery owner. `erabi-crawler` defines provider-neutral crawling and orchestration contracts, `erabi-crawl4ai` contains all upstream HTTP mapping, Plan 06 migration `0005` stores execution-specific results/summaries, Quick Scrape single URL is the primitive behind ordered batch submission, and Production reuses Plan 05 discovery semantics.

**Tech Stack:** stable Rust, Tokio, Axum, Reqwest/Rustls, Turso, Serde, deterministic local HTTP fixtures, existing Plan 04 durable jobs/progress/checkpoints.

**Canonical plan:** [`2026-08-22-06-crawl4ai-and-quick-scrape.md`](2026-08-22-06-crawl4ai-and-quick-scrape.md)  
**Supplemental design:** [`../specs/2026-08-29-plan-06-execution-design.md`](../specs/2026-08-29-plan-06-execution-design.md)  
**Canonical specs:** `docs/specs/01-product-and-experience.md`, `docs/specs/03-discovery-graph-and-runs.md`, `docs/specs/06-security-reliability-and-operations.md`

## Global constraints

Every task inherits these requirements:

- canonical `docs/specs/` wins over this document on conflict;
- exactly four run types remain `QUICK_SCRAPE`, `TEST_RUN`, `DISCOVERY_PREVIEW`, `PRODUCTION_RUN`;
- no `BATCH` CrawlRunType or durable batch lifecycle;
- Published CrawlerVersions remain immutable;
- Source is supporting durable target/history identity and never silently mutates Crawler Seeds;
- direct confident non-HTML URLs follow FileAsset intake and never HTML extraction;
- robots override requires the exact non-empty reason frozen in the immutable run snapshot/audit;
- Quick Scrape and Production share mandatory per-domain pacing;
- Erabi owns durable jobs, retries, checkpoints, cancellation, progress, and lifecycle;
- no provider token or sensitive upstream body/header leaks through `Debug`, tracing, errors, progress, or persistence;
- migration `0005_crawl_execution.sql` belongs to Plan 06; migrations `0001`–`0004` are historical and must not be edited;
- Plan 07 Dataset/extraction/unique-key/schema-drift semantics are not implemented here;
- Plan 08 complete physical Asset/download/export/retention/backup semantics are not implemented here;
- no UI and no GitHub Actions/hosted CI in Plan 06;
- tests use deterministic mock/local fixtures by default and do not require public internet access;
- no task begins until the previous task's review/remediation is accepted.

## Branch and commit convention

Create one Plan 06 feature branch from the current clean `main` that contains this document:

```text
feat/plan-06-crawl4ai-quick-scrape
```

Keep all nine tasks on that branch. Each accepted task gets a normal commit and push. Do not open a PR until the final Plan 06 workspace gate passes. Do not amend an already reviewed task commit; remediation gets a new commit.

---

## Task 1 — `CrawlerAdapter` contract and deterministic mock

### Deliverable

Introduce the provider-neutral crawling boundary used by Quick Scrape, production crawling, and future focused tests. No real Crawl4AI network client is implemented in this task.

### Files

Create or split as needed:

- `crates/erabi-crawler/src/adapter.rs`
- `crates/erabi-crawler/src/mock_adapter.rs`
- `crates/erabi-crawler/tests/adapter_contract.rs`

Modify:

- `crates/erabi-crawler/src/lib.rs`
- `crates/erabi-crawler/src/observation.rs` only when a neutral observation type must be reused/extended without changing accepted Plan 05 semantics;
- `crates/erabi-crawler/Cargo.toml` only for dependencies genuinely required by the provider-neutral contract/tests.

### Interfaces to produce

The exact Rust names may be refined during implementation if the repository's established naming requires it, but the semantic contract must provide equivalents of:

```text
CrawlerAdapter
CrawlerHealth
CrawlerExecuteRequest
CrawlerExecuteResult
CrawlerResponseMetadata
CrawlerArtifactEvidence
CrawlerAdapterError
```

`CrawlerAdapter` is asynchronous, `Send + Sync`, and supports:

- health/version observation;
- one bounded crawl/execute request;
- best-effort cancel when a provider-specific in-flight handle exists, without making that handle a durable Erabi identity.

`CrawlerExecuteRequest` contains only normalized Erabi inputs needed by the provider, including target URL, timeout/render/wait/scroll/screenshot settings that Plan 06 actually supports, and a safe request identity. It must not expose a raw provider DTO.

`CrawlerExecuteResult` contains normalized provider evidence, including:

- requested URL;
- authoritative final URL when observed;
- HTTP/status/content type metadata where available;
- raw/cleaned/rendered/Markdown evidence or bounded references according to the existing artifact boundary;
- discovered links;
- screenshot evidence/reference when requested and available;
- partial-result marker/reason;
- provider timing/diagnostic metadata only when bounded and safe.

### Deterministic mock requirements

Provide fixture configuration for:

- success;
- timeout;
- access denied;
- not found;
- unavailable;
- partial result;
- redirect/final URL;
- final non-HTML response;
- deterministic discovered-link sets.

The mock never uses network, random order, wall-clock sleep, or hidden UUID ordering to determine semantics.

### Error requirements

Normalize expected failures. Provider error text is not a public contract. Stable semantic classes include provider unavailable, timeout, access denied, not found, invalid provider response/contract violation, partial result, and cancellation.

Do not persist or expose secret material through error payloads.

### Verification

Run at minimum:

```text
cargo test -p erabi-crawler
cargo fmt --all --check
cargo clippy -p erabi-crawler --all-targets -- -D warnings
```

Add regressions proving deterministic fixture behavior, final URL authority, provider-neutral errors, and no provider DTO/path leakage into public `erabi-crawler` types.

### Commit boundary

Suggested commit:

```text
feat(crawler): add provider-neutral crawl adapter
```

Push the branch and stop for review.

---

## Task 2 — Crawl4AI HTTP adapter

### Deliverable

Implement the real `CrawlerAdapter` in `erabi-crawl4ai` using Reqwest/Rustls and deterministic local HTTP fixtures. All upstream paths, authentication, JSON fields, version compatibility, and response normalization remain inside this crate.

### Files

Create:

- `crates/erabi-crawl4ai/src/client.rs`
- `crates/erabi-crawl4ai/src/dto.rs`
- `crates/erabi-crawl4ai/src/config.rs` if endpoint/token validation merits a separate focused module;
- `crates/erabi-crawl4ai/tests/http_adapter.rs`

Modify:

- `crates/erabi-crawl4ai/src/lib.rs`
- `crates/erabi-crawl4ai/Cargo.toml`
- workspace lockfile as required by compatible dependencies.

### Contract

Implement Task 1's adapter without changing its provider-neutral semantics to mirror Crawl4AI.

The adapter owns:

- base endpoint normalization;
- health/version/schema compatibility observation needed by Erabi;
- authentication header/token handling;
- request DTO conversion;
- one bounded non-streaming crawl execution path per Erabi work unit;
- upstream status/JSON normalization;
- bounded HTTP/body/timeouts;
- safe correlation/diagnostic metadata;
- best-effort provider cancellation only if supported by the chosen request mode.

Upstream asynchronous job IDs or stream IDs are not Erabi durable job IDs.

### Security

- endpoint configuration is validated;
- secrets use a non-plain-Debug representation such as `SecretString` where appropriate;
- Authorization/Cookie/token values never appear in Debug/log/errors;
- raw upstream bodies are not copied into user-facing errors;
- redirects/final URLs are normalized into Task 1's result contract;
- invalid/malformed upstream JSON is a provider contract violation, not a fabricated partial success.

### Upstream compatibility

Do not make routine tests depend on a public/live Crawl4AI instance. Use local fixture HTTP servers to exercise status codes and exact expected request/response mapping.

Keep current upstream endpoint names isolated to this crate. Plan 10 owns the real release-image smoke test against the exact supported Crawl4AI release/image.

### Verification

Run at minimum:

```text
cargo test -p erabi-crawl4ai
cargo test -p erabi-crawler
cargo fmt --all --check
cargo clippy -p erabi-crawl4ai -p erabi-crawler --all-targets -- -D warnings
```

Cover health success/outage, authenticated requests, token redaction, success, timeout, access denied, not found, malformed response, partial response, redirect/final URL, and bounded body/timeout handling.

### Commit boundary

Suggested commit:

```text
feat(crawl4ai): implement HTTP crawl adapter
```

Push and stop for review.

---

## Task 3 — Crawl execution persistence and migration `0005`

### Deliverable

Add the Plan 06 execution-specific durable model before Quick Scrape/Production services rely on it.

### Files

Create:

- `migrations/0005_crawl_execution.sql`
- `crates/erabi-db/src/repositories/crawl_execution.rs`
- `crates/erabi-db/tests/crawl_execution.rs`

Modify:

- `crates/erabi-db/src/repositories/mod.rs`
- migration registry/runner source only where the existing runner explicitly enumerates migrations;
- `crates/erabi-domain/src/` only for small provider-neutral execution status/value objects that belong in the domain rather than repository-local DTOs.

Do not modify `migrations/0001_system.sql` through `0004_jobs.sql`.

### Persistence model

`0005` stores only execution results/summaries missing from existing `crawl_runs`, `discovered_urls`, and `artifacts` tables.

The design must represent at least:

- one durable page execution result identity;
- owning CrawlRun;
- requested URL;
- authoritative observed final URL when available;
- canonical URL identity used by Erabi scheduling;
- source/PageType/transition/provenance references where applicable;
- provider-neutral outcome/status/error code;
- bounded HTTP/content metadata;
- artifact IDs/references rather than large artifact bodies;
- completed/partial/cancelled evidence required by finalization;
- run execution summary/counters needed by Task 9.

Do not add curated Dataset/Record tables or Plan 08 physical Asset/export tables.

### Integrity rules

- impossible ownership/reference combinations fail closed;
- row/payload identities agree;
- foreign run/source/PageType/transition references are rejected where the model requires current ownership;
- counters use checked conversions and coherent relationships;
- completed in-scope work cannot exceed planned in-scope work;
- durable summary reads are deterministic and not dependent on row insertion order;
- existing run snapshot immutability remains untouched.

### Verification

Run at minimum:

```text
cargo test -p erabi-db
cargo test -p erabi-domain
cargo fmt --all --check
cargo clippy -p erabi-db -p erabi-domain --all-targets -- -D warnings
```

Include migration-upgrade tests from an existing 0001–0004 database, execution round-trip tests, reference-corruption tests, impossible-counter tests, and confirmation that historical migrations are unchanged.

### Commit boundary

Suggested commit:

```text
feat(db): add crawl execution persistence
```

Push and stop for review.

---

## Task 4 — Source intake and direct-file classification

### Deliverable

Implement create/reuse Source identity and the bounded decision that keeps confident direct non-HTML URLs out of HTML extraction.

### Files

Create as needed:

- `crates/erabi-db/src/repositories/source.rs`
- `crates/erabi-crawler/src/source_intake.rs`
- `crates/erabi-crawler/src/content_probe.rs`
- `crates/erabi-crawler/tests/source_intake.rs`
- `crates/erabi-api/src/source_intake.rs` only if an API surface is needed in this task rather than first used by Task 6;
- corresponding API integration tests when a route is exposed.

Modify:

- `crates/erabi-db/src/repositories/mod.rs`
- `crates/erabi-crawler/src/lib.rs`
- `crates/erabi-api/src/lib.rs`/router wiring only for real exposed routes.

### Source semantics

Create/reuse Source from validated original/canonical URL without mutating Crawler Seeds or CrawlerVersion semantic configuration.

Reuse must be deterministic. If existing persisted Source state is contradictory or corrupt, fail closed rather than selecting an arbitrary duplicate by UUID/database order.

### Probe contract

Use a bounded safe probe only when appropriate. It may use HEAD and/or a tightly bounded GET fallback according to actual HTTP behavior, but must:

- obey timeout and response-size limits;
- apply SSRF/network safety policy appropriate to crawl targets;
- inspect response content type and limited signature evidence where practical;
- never buffer an arbitrary large file;
- never auto-open/execute/extract a file.

Classify confident PDF/CSV/JSON/archive/image/office-like targets as `FileAsset`.

If the probe is unavailable, contradictory, or ambiguous, return a normal-web-crawl decision. The authoritative final provider response may then classify the target as non-HTML after crawl.

### Plan 08 boundary

Do not build the full physical Asset downloader. Persist Source/probe/final classification metadata and expose a future-safe explicit download boundary only when needed by existing API/domain contracts.

### Verification

Run at minimum:

```text
cargo test -p erabi-crawler
cargo test -p erabi-db
cargo test -p erabi-api
cargo fmt --all --check
cargo clippy -p erabi-crawler -p erabi-db -p erabi-api --all-targets -- -D warnings
```

Cover Source reuse, Source/Seed independence, PDF, CSV, JSON, archive, image, office-like fixtures, misleading extension/MIME, ambiguous probe fallback, redirect/final classification, bounded probe size/timeout, and path/file safety boundaries that Plan 06 owns.

### Commit boundary

Suggested commit:

```text
feat(crawler): add source intake classification
```

Push and stop for review.

---

## Task 5 — Robots policy and per-domain pacing

### Deliverable

Create one shared robots/pacing layer used by both Quick Scrape and Production. No caller can bypass it through higher concurrency or batch execution.

### Files

Create:

- `crates/erabi-crawler/src/robots.rs`
- `crates/erabi-crawler/src/pacing.rs`
- `crates/erabi-crawler/tests/robots_pacing.rs`

Modify only as required:

- `crates/erabi-crawler/src/lib.rs`
- `crates/erabi-api/src/run_safety.rs` to reuse, not duplicate, already accepted override validation;
- settings integration modules that resolve existing concurrency/request-delay/User-Agent values.

### Robots requirements

Support the User-Agent-relevant MVP semantics for:

- Allow;
- Disallow;
- Crawl-delay where represented by the parsed policy;
- cache identity/expiry suitable for bounded local crawling;
- typed unavailable/invalid policy evidence.

Respect is the default. An override is legal only when the immutable run snapshot already contains the validated non-empty reason. This service does not invent/recover/copy a reason.

### Pacing requirements

Key limiter state by an explicit normalized origin/domain key, not raw arbitrary URL strings.

Admission combines:

- immutable resolved snapshot concurrency;
- request delay;
- robots Crawl-delay where applicable;
- conservative bounded backoff;
- valid `Retry-After` response timing.

Malformed, negative, overflowed, or excessively large retry values are rejected/clamped according to one deterministic bounded policy.

Concurrency state must not depend on hash-map iteration order for semantic decisions.

### Verification

Run at minimum:

```text
cargo test -p erabi-crawler
cargo test -p erabi-api
cargo fmt --all --check
cargo clippy -p erabi-crawler -p erabi-api --all-targets -- -D warnings
```

Cover allow/disallow precedence, User-Agent selection, Crawl-delay, cache behavior, respect default, override/no-reason API rejection via existing run safety, per-domain isolation, same-domain serialization/limits, valid `Retry-After`, malformed/overflowed `Retry-After`, and no batch/Quick Scrape bypass.

### Commit boundary

Suggested commit:

```text
feat(crawler): enforce robots and crawl pacing
```

Push and stop for review.

---

## Task 6 — Quick Scrape single URL

### Deliverable

Implement the default Start backend flow for one URL as an independent durable `QUICK_SCRAPE` run with Source, snapshot, root job, progress, provider execution, and stored result/artifact evidence.

### Files

Create as needed:

- `crates/erabi-crawler/src/quick_scrape.rs`
- `crates/erabi-api/src/quick_scrape.rs`
- `crates/erabi-api/tests/quick_scrape.rs`
- root crawl job handler/service module in `crates/erabi-jobs/src/` if Plan 04's generic runtime requires a Plan 06 handler registration boundary.

Modify:

- `crates/erabi-crawler/src/lib.rs`
- `crates/erabi-api/src/lib.rs`
- `crates/erabi-api/src/app.rs`/router wiring following existing route organization;
- `crates/erabi-api/src/state.rs` for injected adapter/runtime state where appropriate;
- `crates/erabi-jobs/src/lib.rs` only for the focused handler export/registration;
- existing run/job repositories only when a narrow transactional helper is required to create a run and its root job coherently.

### Submission contract

One accepted request must produce exactly one independent `QUICK_SCRAPE` run.

The immutable snapshot uses:

```text
RunConfiguration::QuickScrape
```

and freezes resolved ad-hoc operational settings, robots decision/reason, User-Agent, actor/time, and target URL. It has no CrawlerVersion requirement.

Create/reuse Source through Task 4. If it is confidently a FileAsset, do not schedule HTML crawling.

### Atomicity

Submission must not create a run with no viable root job or an orphan job with no run association. Use one coherent transaction or an existing repository primitive that guarantees equivalent atomic ownership.

A provider outage after accepted durable submission is an execution failure/retry outcome, not rollback of the run's existence/history.

### Worker execution

The root/page handler uses:

- Task 4 Source intake/final classification;
- Task 5 robots/pacing;
- Task 1/2 `CrawlerAdapter`;
- Task 3 execution persistence;
- existing artifact metadata/storage boundary;
- Plan 04 progress, cancellation, retry, checkpoint, storage-pressure signals.

### API

Use a stable Erabi endpoint under `/api/v1`; do not expose Crawl4AI DTOs. Preserve existing Host/Origin/auth/Content-Type/body-limit/trace/error-envelope conventions.

Return accepted run/job/source identity required by later UI, not raw provider handles.

### Verification

Run at minimum:

```text
cargo test -p erabi-crawler
cargo test -p erabi-db
cargo test -p erabi-jobs
cargo test -p erabi-api
cargo test -p erabi --test runtime_server
cargo fmt --all --check
cargo clippy -p erabi-crawler -p erabi-db -p erabi-jobs -p erabi-api -p erabi --all-targets -- -D warnings
```

Cover one-URL success, no-CrawlerVersion invariant, Source association, immutable snapshot/audit, exactly `QUICK_SCRAPE`, root job association, progress, provider unavailable/timeout/access denied/not found/partial, cancellation identity, direct FileAsset bypass, and retry preserving run history.

### Commit boundary

Suggested commit:

```text
feat(crawler): add single-url quick scrape
```

Push and stop for review.

---

## Task 7 — Quick Scrape bounded pasted batch

### Deliverable

Add the convenience batch submission envelope by delegating each accepted item to the accepted Task 6 single-URL submission primitive.

### Files

Create or extend:

- `crates/erabi-api/src/quick_scrape.rs`
- `crates/erabi-crawler/src/quick_scrape.rs` only if a reusable ordered submission coordinator belongs in the service layer;
- `crates/erabi-api/tests/quick_scrape_batch.rs`

Avoid creating a batch domain entity/table/job unless the canonical spec is changed; none is required for MVP.

### Batch contract

Define one fixed bounded maximum item count and request body size consistent with existing API hardening conventions. The exact bound must be explicit in code/API schema and tested; do not leave it environment-dependent without a canonical setting.

For each input item in original order return a typed outcome such as:

```text
ACCEPTED(run_id, job_id, source_id)
VALIDATION_ERROR(code)
CONFLICT(code)
```

Each accepted item creates an independent run/root job through Task 6.

### Atomicity

No all-items transaction. One invalid or conflicting item must not roll back already accepted unrelated items.

Within one accepted item, Task 6's single-run atomicity still applies.

There is no batch-level cancellation/retry lifecycle; callers act on the independent returned run/job IDs.

### Verification

Run at minimum:

```text
cargo test -p erabi-api
cargo test -p erabi-crawler
cargo test -p erabi-db
cargo test -p erabi-jobs
cargo fmt --all --check
cargo clippy -p erabi-api -p erabi-crawler -p erabi-db -p erabi-jobs --all-targets -- -D warnings
```

Cover ordered mixed valid/invalid inputs, bound rejection, duplicates according to Source/run semantics, independent run IDs, independent root job IDs, one execution failure not affecting siblings, per-item cancellation/retry identity, and a compile/domain assertion that no fifth run type was introduced.

### Commit boundary

Suggested commit:

```text
feat(api): add bounded quick scrape batch
```

Push and stop for review.

---

## Task 8 — Production crawl orchestration

### Deliverable

Implement bounded production execution from a Published CrawlerVersion using the same semantic discovery decisions as Plan 05, then actual provider crawl/render and durable page/artifact/provenance persistence.

### Files

Create focused modules rather than growing `lib.rs`:

- `crates/erabi-crawler/src/production/` with separate scheduling/orchestration/final observation modules as responsibility requires;
- `crates/erabi-crawler/tests/production_crawl.rs`;
- `crates/erabi-api/src/production_run.rs`;
- `crates/erabi-api/tests/production_run.rs`;
- Plan 06 crawl job handler modules in `crates/erabi-jobs/src/` as required.

Modify:

- `crates/erabi-crawler/src/lib.rs`
- API router/state wiring;
- `crates/erabi-db/src/repositories/crawl_execution.rs` only for narrowly missing operations discovered by actual orchestration;
- existing crawler repository only for read-only Published snapshot loading or a focused publication-snapshot helper, not to duplicate validation semantics.

### Submission eligibility

Normal production submission requires:

- existing Crawler;
- referenced CrawlerVersion belongs to it;
- CrawlerVersion state is Published;
- immutable run snapshot resolves exact semantic config hash and operational settings;
- robots/User-Agent audit is valid;
- root job creation is coherent with run creation.

Draft must fail with a stable client error; do not auto-publish or silently use active Draft.

### Scheduling semantics

Reuse Plan 05's canonical resolver/canonicalization/domain-scope/PageType/transition/budget primitives. Refactor neutral shared code only when necessary to eliminate duplication without changing accepted Plan 05 behavior.

Canonical discovered-link order remains:

```text
raw href
-> resolve against actual observed source/final URL
-> validate
-> canonicalize
-> scope
-> dedupe
-> PageType match
-> transition eligibility
-> budget
-> enqueue or preserve-only
```

Only admitted work reaches provider execution. Preserve non-admitted decision evidence.

### Traversal invariants

- canonical identity is the scheduling dedupe key;
- duplicate queued/completed work is not re-evaluated as fresh semantic discovery unless the canonical contract explicitly requires an observation count;
- cycles/self-transitions remain bounded by depth/page/transition/total guardrails;
- unexpected pagination truncation is recorded for final health;
- ambiguous PageType URLs are preserved but not silently assigned through UUID/database order;
- external/blocked/unmatched/budget-excluded URLs retain explanation/provenance;
- authoritative provider final URL is used consistently after redirects.

### Persistence

For every attempted/admitted unit record durable Task 3 page execution outcome and artifact references. Progress is durable through Plan 04 and user-facing categories stay separate from technical logs.

Partial page failures do not erase successful evidence. Run status becomes `PARTIAL_RESULT` when the execution remains usable but incomplete according to canonical semantics.

### Verification

Run at minimum:

```text
cargo test -p erabi-domain
cargo test -p erabi-db
cargo test -p erabi-crawler
cargo test -p erabi-jobs
cargo test -p erabi-api
cargo test -p erabi --test runtime_server
cargo fmt --all --check
cargo clippy -p erabi-domain -p erabi-db -p erabi-crawler -p erabi-jobs -p erabi-api -p erabi --all-targets -- -D warnings
```

Cover Published-only submission, wrong-owner version rejection, bounded seed traversal, redirect final URL, canonical dedupe, PageType ambiguity, unmatched/external/blocked/budget evidence, cyclic transition bounds, pagination truncation, provider partial/page failures, artifact references, durable progress, and no change to accepted Discovery Preview semantics.

### Commit boundary

Suggested commit:

```text
feat(crawler): add bounded production crawl orchestration
```

Push and stop for review.

---

## Task 9 — Recovery, run finalization, and complete-snapshot integration

### Deliverable

Close Plan 06 by wiring production/Quick Scrape execution into Plan 04 cancellation/checkpoint/resume/retry and deriving durable final run status/structural facts for Plan 05 complete-snapshot evaluation.

### Files

Create as needed:

- `crates/erabi-crawler/src/finalization.rs`
- `crates/erabi-crawler/src/checkpoint.rs` if crawl-specific checkpoint payloads do not fit an existing focused module;
- `crates/erabi-crawler/tests/recovery_finalization.rs`
- `crates/erabi-jobs/tests/crawl_recovery.rs`
- API integration tests for existing resume/retry/cancel actions against Plan 06 jobs.

Modify narrowly:

- `crates/erabi-jobs/src/actions.rs` and/or existing action registration only to support Plan 06 run/job kinds using accepted Plan 04 lineage semantics;
- `crates/erabi-db/src/repositories/crawl_execution.rs` for atomic durable summary/finalization;
- `crates/erabi-db/src/repositories/run.rs` for typed status transitions if existing primitives are insufficient;
- `crates/erabi-crawler/src/lib.rs`.

### Checkpoint contract

A crawl checkpoint contains enough deterministic state to resume the same immutable run without discovering a different frontier from in-memory accidents:

- immutable run/snapshot compatibility identity;
- completed canonical URLs;
- pending admitted canonical URLs with required provenance/depth/transition state;
- pagination state;
- failed/partial units;
- persisted artifact references;
- counters required to continue bounded budgets safely.

Checkpoint payloads remain bounded and validated. Resume requires compatibility with the same immutable run snapshot; semantic configuration mutation means a new run, not a forced resume.

### Cancellation and retry

Cooperative cancellation:

1. stops new scheduling;
2. signals active work;
3. reaches safe unit boundaries;
4. persists checkpoint/current durable state;
5. finalizes run `CANCELLED` when appropriate.

Retry Failed Parts and Resume reuse accepted Plan 04 lineage; prior failed attempts remain history. Full Rerun creates the appropriate independent run lineage according to existing action semantics.

Robots override reason is reused only for resume/retry of the same immutable run snapshot, never copied into an independent new run silently.

### Finalization

Derive from durable execution state, not in-memory counters:

```text
run status
in_scope_pages_planned
in_scope_pages_completed
pagination_truncation_count
unresolved_partial_work_count
page_type_ambiguity_count
```

Impossible relationships are typed persisted-state/invariant failures.

Feed Production facts into existing `CompleteSnapshotStructuralInput`.

Quick Scrape is never a complete production snapshot because the existing structural gate rejects non-production run types.

Use extraction health only according to the Plan 05 seam. Do not fabricate `Healthy` for required extraction that Plan 07 has not evaluated.

No `MISSING_CANDIDATE` creation occurs in Plan 06.

### Verification

Run focused verification at minimum:

```text
cargo test -p erabi-domain
cargo test -p erabi-db
cargo test -p erabi-crawler
cargo test -p erabi-jobs
cargo test -p erabi-api
cargo test -p erabi --test runtime_server
cargo fmt --all --check
cargo clippy -p erabi-domain -p erabi-db -p erabi-crawler -p erabi-jobs -p erabi-api -p erabi --all-targets -- -D warnings
```

Cover cancellation checkpoint, valid resume, incompatible resume rejection, retry lineage, storage-pressure checkpoint behavior, durable-counter reconstruction, partial/failure/cancel finalization, ambiguity/truncation finalization, `NotEvaluated` extraction fail-closed behavior, `NotRequired` behavior where genuinely applicable, and confirmation that no missing/deletion candidate subsystem exists.

### Commit boundary

Suggested commit:

```text
feat(crawler): finalize crawl recovery and snapshot health
```

Push and stop for independent Task 9 review.

---

# Final Plan 06 workspace gate

Only after Tasks 1–9 and all remediation are independently accepted, run from a clean checkout of the exact reviewed Plan 06 head:

```text
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

The final gate also confirms:

- first-run one-URL Quick Scrape works without a CrawlerVersion;
- ordered pasted batch produces independent Quick Scrape runs and independent job/action identities;
- no fifth run type exists;
- direct confident non-HTML targets bypass HTML extraction;
- Source reuse never mutates Seeds;
- robots override/reason/User-Agent are frozen and audited;
- mandatory per-domain pacing and bounded `Retry-After` behavior are shared across Quick Scrape and Production;
- adapter/mock/HTTP mapping contracts pass without public network dependency;
- migration `0005` is present and migrations `0001`–`0004` are unchanged;
- normal Production requires a Published CrawlerVersion;
- Production reuses Plan 05 canonical discovery semantics;
- partial/cancelled/failed/ambiguous/truncated production execution cannot become a complete snapshot;
- Plan 07/08/UI/CI scope has not leaked into Plan 06.

If the gate passes, open one Plan 06 PR, review the fresh PR head, and squash merge to `main`. Do not begin Plan 07 before that merge and gate are complete.
