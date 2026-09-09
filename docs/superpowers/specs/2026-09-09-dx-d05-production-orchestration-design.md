# DX-D05 — Production Orchestration Ownership and Extraction Boundary Design

**Status:** Design approved in discussion; written specification pending explicit user review.

**Package:** DX-D05

**Purpose:** Clarify and enforce Production Run orchestration ownership before Plan 07 adds extraction, validation, schema-drift, Dataset, review, and provenance behavior.

## 1. Problem statement

The current Production Run path is functionally correct but concentrates multiple application responsibilities inside `erabi-jobs/src/production.rs`: provider execution, network admission, robots/pacing coordination, traversal driving, durable discovery/work persistence, checkpoint/recovery, artifact persistence, progress, and finalization.

At the same time, Erabi already has an intentionally separate `erabi-extraction` crate boundary, the domain complete-snapshot contract already models `ExtractionHealth`, and the generic durable checkpoint envelope already models extraction resume phases. Plan 07 will need to plug extraction into Production without making the Production orchestration monolith larger or making `erabi-crawler` depend on extraction concerns.

DX-D05 therefore establishes the Production workflow seam that Plan 07 will later consume. It is an architecture refactor and MUST preserve current product/runtime behavior.

## 2. Approved architectural decision

Production extraction is an explicit **post-crawl stage in the same durable Production Run/root workflow**.

Target lifecycle:

```text
Production Run
    ↓
crawl / traversal
    ↓
durable crawl evidence complete enough for post-crawl handling
    ↓
post-crawl extraction stage
    ↓
validation / schema-drift evaluation
    ↓
ExtractionHealth
    ↓
trusted finalization decision
    ↓
terminalization
```

DX-D05 does **not** implement extraction. It creates the durable and ownership seam so Plan 07 can add that stage without redesigning Production orchestration again.

A separate durable child extraction job is not introduced for MVP. Extraction remains part of the same Production root workflow unless a future package has concrete scaling or isolation requirements that justify a separate job lifecycle.

## 3. Crate ownership

### 3.1 `erabi-jobs` — Production workflow owner

`erabi-jobs` owns the durable Production workflow and stage order. It may know that the workflow is conceptually:

```text
load/recover
→ crawl
→ post-crawl extraction
→ validation/drift
→ finalization
→ terminal progress
```

It owns orchestration concerns already associated with the durable worker boundary, including job lease ownership, cancellation, checkpoint lineage, progress publication, recovery coordination, and terminalization.

It does **not** own extraction semantics. It must not implement selector evaluation, value normalization, field validation, unique-key extraction, Dataset compatibility, or schema-drift rules.

Rule:

> `erabi-jobs` decides **when** extraction runs, not **how** extraction works.

### 3.2 `erabi-crawler` — crawl truth owner

`erabi-crawler` owns crawl/network/traversal semantics and the reconstruction of durable structural crawl facts, including facts such as:

- in-scope work planned/completed;
- pagination truncation;
- unresolved partial crawl work;
- Page Type ambiguity;
- coherent crawl status derived from durable evidence.

It does not own extraction health and must not depend on `erabi-extraction`.

### 3.3 `erabi-extraction` — extraction truth owner

Plan 07 will make `erabi-extraction` the owner of extraction and schema/drift semantics, including:

- safe preview and deterministic extraction evidence;
- selector/node mapping;
- extraction definitions and value sources;
- raw versus normalized values;
- typed normalization;
- validation;
- Dataset compatibility and unique-key semantics;
- structural fingerprints;
- schema-drift detection and classification;
- extraction health results.

DX-D05 does not add runtime APIs, traits, or dependencies to `erabi-extraction` yet.

### 3.4 `erabi-domain` — shared semantic vocabulary

`erabi-domain` owns stable business vocabulary and decisions shared across application boundaries, including `ExtractionHealth`, complete-snapshot reasons/decision, and future Plan 07 domain invariants.

It performs no DB, filesystem, Crawl4AI, or job orchestration work.

### 3.5 `erabi-db` — persistence mechanism

`erabi-db` persists and reconstructs durable state. Repositories do not decide when extraction runs, whether selectors imply schema drift, or whether a Production snapshot is trusted.

## 4. Dependency direction

The intended direction is:

```text
erabi-domain
    ↑
    ├── erabi-crawler
    ├── erabi-extraction
    └── erabi-db

crawler ─┐
db ──────┼──→ jobs
domain ──┤
          └── extraction (Plan 07, not DX-D05)
```

Required constraints:

```text
jobs → crawler       allowed
jobs → db            allowed
jobs → domain        allowed
jobs → extraction    future Plan 07, NOT added by DX-D05

crawler → extraction forbidden
extraction → jobs    forbidden
domain → extraction  forbidden
db → extraction      forbidden
```

## 5. Durable crawl-to-extraction handoff

The crawl stage hands off through **durable evidence**, not transient provider DTOs or in-memory HTML/result objects.

Conceptually:

```text
provider execution
    ↓
artifact + execution/discovery/work evidence committed durably
    ↓
crawl stage reaches safe handoff
    ↓
post-crawl stage reads durable evidence
```

A process crash after crawl completion must not require recrawling solely to feed extraction. Recovery must resume from the latest durably completed stage.

The post-crawl seam may carry identities, structural facts, and bounded durable references. Raw provider response bodies or credentials must not become workflow handoff payloads.

## 6. Recovery and checkpoint ownership

The generic job checkpoint boundary already models extraction progress through `ExtractionResumeState` and the phases:

```text
NOT_STARTED
IN_PROGRESS
AWAITING_VALIDATION
```

Those phases are the future Plan 07 extraction recovery seam.

The crawler-specific compact checkpoint remains crawl-only. DX-D05 must **not** add an `Extracting` value to `CrawlRecoveryPhase` and must not change checkpoint schema/version semantics.

Conceptual future recovery behavior:

```text
if crawl incomplete/recoverable:
    recover crawl stage

else if extraction == NOT_STARTED:
    start extraction from durable crawl evidence

else if extraction == IN_PROGRESS:
    resume/reconcile extraction from durable extraction state

else if extraction == AWAITING_VALIDATION:
    validate/reconcile durable extraction output

else:
    finalization/reconciliation
```

Extraction failure is not permission to recrawl. Validation failure is not permission to blindly re-extract. Recovery resumes the latest stage that is not durably complete.

DX-D05 itself does not activate these extraction phases or change the generic checkpoint payload.

## 7. Crawl-only structural facts

`erabi-crawler` must expose a canonical crawl-only structural result, conceptually:

```rust
pub struct CrawlStructuralFacts {
    pub status: CrawlRunStatus,
    pub in_scope_pages_planned: u64,
    pub in_scope_pages_completed: u64,
    pub pagination_truncation_count: u64,
    pub unresolved_partial_work_count: u64,
    pub page_type_ambiguity_count: u64,
}
```

The exact public/internal name may differ if implementation evidence supports a clearer name, but the type must remain pure crawl truth:

- no `ExtractionHealth`;
- no jobs types;
- no DB handle;
- no provider DTO;
- no extraction-specific state.

The canonical crawler path becomes conceptually:

```text
durable execution/discovery/traversal evidence
        ↓
erabi-crawler
        ↓
CrawlStructuralFacts
```

## 8. Complete-snapshot composition ownership

`ExtractionHealth` must no longer be conceptually owned by crawler finalization.

The application composition belongs in `erabi-jobs`:

```text
CrawlStructuralFacts
        +
ExtractionHealth
        ↓
CompleteSnapshotStructuralInput
        ↓
erabi-domain decision
```

The domain remains the authority that decides Complete versus Incomplete.

For DX-D05 compatibility, Production continues to compose:

```text
ExtractionHealth::NotEvaluated
```

so current behavior is preserved until Plan 07 supplies real extraction health.

Existing non-Production compatibility paths may continue to use `ExtractionHealth::NotRequired` where that is already the established behavior.

## 9. Compatibility wrappers in `erabi-crawler`

DX-D05 should avoid a big-bang API break.

Existing finalization APIs such as:

```text
finalize_durable_state
finalize_durable_state_with_control
finalize_durable_state_with_traversal
```

may remain as compatibility wrappers if current callers/tests need them.

A new canonical crawl-only core should reconstruct structural facts. Legacy wrappers may adapt those facts into the existing `CrawlFinalization`/complete-snapshot result by attaching the historical extraction-health default.

Production jobs should use the crawl-only structural facts explicitly, so Plan 07 does not need to reopen the crawler ownership boundary.

## 10. Explicit post-crawl disposition

The Production crawl stage needs a private, closed disposition equivalent to:

```text
READY_FOR_POST_CRAWL
TERMINAL_NO_POST_CRAWL
```

Exact Rust names are implementation details, but arbitrary strings are not acceptable.

`READY_FOR_POST_CRAWL` includes coherent durable outcomes where downstream processing may safely inspect available evidence, including:

- structurally complete bounded crawl;
- coherent bounded partial result;
- pagination truncation;
- coherent partial page evidence.

`TERMINAL_NO_POST_CRAWL` includes states where new downstream work must not begin, including:

- cancellation before post-crawl handoff;
- fatal invariant failure;
- unsafe durable-state contradiction;
- unrecoverable orchestration failure.

## 11. Partial-result semantics

Structural incompleteness does **not** automatically prohibit extraction.

A coherent partial Production crawl may later allow Plan 07 to extract whatever durable page/artifact evidence was successfully obtained, preserving partial records, diagnostics, validation information, and provenance.

However:

```text
structurally incomplete + extraction healthy
≠ complete snapshot
```

Extraction health can preserve or reduce trust. It can never repair structural crawl incompleteness.

A coherent partial crawl therefore may be ready for the post-crawl stage while remaining ineligible for trusted complete-snapshot semantics and `MISSING_CANDIDATE` generation.

## 12. Cancellation and fatal-state behavior

If cancellation is active before post-crawl extraction starts, the workflow must not schedule new extraction work. It persists the safe durable boundary/checkpoint and terminalizes according to the existing cancellation contract.

If cancellation occurs after extraction has started in Plan 07, that future stage must stop at a safe extraction unit, persist extraction recovery state, and terminalize without recrawling.

Fatal/invariant crawl failure must not be hidden by downstream extraction. If durable crawl truth is unsafe or contradictory, extraction does not start merely because some artifacts happen to exist.

## 13. Run lifecycle remains unchanged

DX-D05 does not add persisted primary Crawl Run states such as `CRAWLING`, `EXTRACTING`, or `VALIDATING`.

The established primary lifecycle remains:

```text
QUEUED
RUNNING
SUCCEEDED
PARTIAL_RESULT
FAILED
CANCELLED
```

Crawl/extraction/validation are workflow, checkpoint, and progress stages, not replacements for the primary product lifecycle.

## 14. Production module decomposition

The current `erabi-jobs/src/production.rs` should be decomposed by semantic responsibility.

Preferred target shape:

```text
crates/erabi-jobs/src/production/
├── mod.rs
├── crawl_stage.rs
├── page_execution.rs
└── finalization.rs
```

The exact file count is not normative. Responsibility boundaries are more important than line-count targets.

### 14.1 `production/mod.rs`

Owns the public Production handler and stage sequencing:

```text
load/recover
→ crawl stage
→ inspect post-crawl disposition
→ explicit post-crawl seam
→ finalization
```

`ProductionCrawlJobHandler` remains the stable public entry point unless implementation evidence shows a behavior-preserving internal alias is needed.

### 14.2 `crawl_stage.rs`

Owns Production crawl application orchestration:

- restore/create semantic traversal;
- drive the traversal loop;
- persist durable discovery/work deltas;
- progress crawl checkpoints;
- coordinate recovery of crawl work;
- observe safe cancellation/storage-pressure boundaries;
- return a typed coherent-partial versus terminal/fatal disposition.

It does not own extraction, validation, schema drift, or Dataset/record semantics.

Semantic traversal algorithms remain owned by `erabi-crawler`. DX-D05 must not redesign them.

### 14.3 `page_execution.rs`

Owns one bounded physical page-attempt application flow:

```text
network admission
→ robots
→ pacing
→ provider execution
→ provider-result validation/normalization
→ artifact/evidence persistence outcome
```

Crawler policies/types remain in their current ownership. This module coordinates runtime calls; it does not move network/crawler semantics into jobs.

### 14.4 `finalization.rs`

Owns Production application composition:

```text
load/reconstruct durable crawl structural facts
→ obtain current post-crawl health
→ construct complete-snapshot domain input
→ call domain decision
→ persist/reconcile final durable run state
```

DX-D05's post-crawl health remains `ExtractionHealth::NotEvaluated` for Production.

## 15. No speculative extraction API

DX-D05 must not introduce speculative interfaces such as:

```text
ProductionExtractionStage
ExtractionStage trait
NoopExtractor
FakeExtractionService
```

unless an implementation blocker proves an abstraction is required to preserve behavior—which would require design review before continuing.

`erabi-extraction` source and Cargo dependencies should remain unchanged in DX-D05.

Plan 07 will define the real `erabi-extraction` input/output boundary from concrete Task 1–3 requirements.

## 16. Hard non-goals

DX-D05 does **not** include:

- Plan 07 extraction implementation;
- HTML parsing, selectors, normalization, or validation;
- Dataset or review persistence;
- schema-drift implementation;
- new migrations;
- child extraction jobs;
- new persisted `CrawlRunStatus` values;
- semantic traversal redesign (DX-D06);
- crawler repository decomposition (DX-D07);
- checkpoint compatibility/version redesign (DX-D03);
- API or OpenAPI behavior changes;
- provider behavior changes;
- network/robots/pacing semantic changes;
- retry/recovery semantic changes;
- unrelated test-layout architecture work.

## 17. Behavior-preservation requirement

DX-D05 is an architecture refactor.

For every existing externally observable Production behavior:

```text
before DX-D05 = after DX-D05
```

This includes, where applicable:

- provider call count/order;
- robots/network/pacing behavior;
- artifact persistence;
- discovery/traversal semantics;
- checkpoint/recovery behavior;
- cancellation;
- storage-pressure handling;
- retry behavior;
- progress semantics/order;
- durable run status;
- error classification/diagnostics;
- current `ExtractionHealth::NotEvaluated` Production trust result.

DX-D05 must not opportunistically "fix" the fact that extraction is currently not evaluated. Plan 07 owns that semantic activation.

## 18. Test strategy

Use the existing repository testing conventions. Do not introduce a new testing architecture solely for DX-D05.

### 18.1 Crawler focused coverage

Add or adapt focused tests proving crawl-only structural reconstruction for at least:

- complete crawl;
- pagination truncation;
- coherent partial work;
- Page Type ambiguity;
- inconsistent durable state rejection.

The canonical structural-facts result must not require `ExtractionHealth`.

### 18.2 Jobs focused coverage

Add/adapt focused tests proving:

- `CrawlStructuralFacts + ExtractionHealth::NotEvaluated` preserves the previous Production decision;
- coherent bounded partial crawl is eligible for the post-crawl seam but remains an incomplete trusted snapshot;
- cancellation before handoff produces terminal/no-post-crawl behavior;
- fatal/unsafe durable contradiction does not enter post-crawl processing;
- existing Production behavior remains unchanged across provider calls, durable evidence, progress, checkpoint, recovery, and final status.

## 19. Verification gate

Use focused verification after each implementation subtask. Final DX-D05 verification should include:

```powershell
cargo test -p erabi-crawler -j 1
cargo test -p erabi-jobs --test production -j 1
cargo test -p erabi-jobs -j 1

cargo test -p erabi --lib -j 1
cargo test -p erabi --test runtime_server -j 1

cargo check -p erabi-crawler --all-targets -j 1
cargo check -p erabi-jobs --all-targets -j 1
cargo check -p erabi --all-targets -j 1

cargo fmt --all --check

cargo clippy -p erabi-crawler -p erabi-jobs -p erabi --all-targets -j 1 -- -D warnings

git diff --check
```

Using `-j 1` is permitted/preferred on Windows when needed to avoid pagefile pressure.

Do not use `cargo clean`.

## 20. Source-level architecture review gate

Independent review must inspect architecture, not only test results.

The reviewer must confirm that the decomposition creates real semantic ownership boundaries rather than simply moving the old monolithic file into `production/mod.rs`.

Required distinction:

```text
workflow sequencing
≠ crawl-stage orchestration
≠ bounded page execution
≠ finalization composition
```

There is no arbitrary line-count acceptance threshold. A larger cohesive module is preferable to many small mixed-responsibility files.

## 21. Dependency gate

After DX-D05:

```text
jobs → crawler       unchanged/allowed
jobs → db            unchanged/allowed
jobs → domain        unchanged/allowed
jobs → extraction    absent
crawler → extraction absent
extraction → jobs    absent
```

`erabi-extraction/Cargo.toml` and runtime source should remain unchanged.

## 22. Exit criteria

DX-D05 is complete only when all of the following are true:

1. Production orchestration is decomposed by semantic responsibility.
2. `erabi-crawler` exposes canonical crawl-only structural facts.
3. Extraction-health composition is owned by `erabi-jobs`, not crawler structural reconstruction.
4. An explicit post-crawl seam exists in the Production workflow.
5. Coherent partial versus cancellation/fatal no-post-crawl behavior is represented by a typed closed disposition.
6. Durable evidence is the crawl-to-post-crawl handoff authority.
7. Current runtime/Product behavior remains unchanged.
8. `erabi-extraction` is not implemented or depended on by DX-D05.
9. Checkpoint schema/version compatibility is unchanged.
10. DX-D06/DX-D07 work is not mixed into the package.
11. Focused and runtime regression verification passes.
12. Independent review reports `BLOCKER: 0` and `IMPORTANT: 0`.

## 23. Intended post-DX-D05 Plan 07 seam

After DX-D05, Plan 07 should be able to add the real extraction stage without reopening crawl ownership:

```text
Production workflow (`erabi-jobs`)
        │
        ├── CrawlStage
        │      ↓
        │   durable crawl evidence
        │      ↓
        ├── post-crawl seam
        │      ↓
        │   `erabi-extraction` (Plan 07)
        │      ↓
        │   ExtractionHealth
        │      ↓
        └── FinalizationStage
               ↓
          domain complete-snapshot decision
```

The architectural objective is not merely a smaller source file. The objective is a stable ownership seam that lets Plan 07 extend Production without coupling crawler semantics to extraction or forcing another Production orchestration redesign.