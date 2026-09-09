# DX-D05 Production Orchestration Ownership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use `superpowers:executing-plans` to implement this plan task-by-task. Erabi explicitly forbids subagents for this package. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor Production Run orchestration into explicit stage-oriented ownership, expose crawl-only structural facts from `erabi-crawler`, move `ExtractionHealth` composition into `erabi-jobs`, and create the behavior-preserving post-crawl seam that Plan 07 will later fill.

**Architecture:** `erabi-jobs` remains the durable Production workflow owner; `erabi-crawler` owns crawl/traversal structural truth; `erabi-domain` remains the complete-snapshot business-decision authority; `erabi-extraction` remains untouched until Plan 07. Production orchestration is decomposed into workflow sequencing, crawl-stage orchestration, bounded page execution, and finalization composition without changing product/runtime semantics.

**Tech Stack:** stable Rust, Tokio, existing Erabi crawler/jobs/domain/db/observability crates, current Turso-backed repositories, existing integration fixtures/tests. No new runtime dependency is expected.

**Spec:** `docs/superpowers/specs/2026-09-09-dx-d05-production-orchestration-design.md`

## Global Constraints

- Implementation must begin from the current merged `main`; the implementation agent must report the actual starting SHA.
- The approved architecture is behavior-preserving. Existing Production externally observable behavior before and after DX-D05 must remain equivalent.
- `ProductionCrawlJobHandler` remains the stable public jobs entry point.
- Existing exported crawler finalization APIs and `CrawlFinalization` remain available as compatibility surfaces during DX-D05.
- `erabi-crawler` must expose canonical crawl-only structural facts with no `ExtractionHealth`, jobs type, DB handle, provider DTO, or extraction-specific state.
- Complete-snapshot composition of crawl facts + `ExtractionHealth` moves to `erabi-jobs`; the domain still decides Complete versus Incomplete.
- DX-D05 Production compatibility health remains `ExtractionHealth::NotEvaluated`.
- Existing non-Production compatibility wrappers may retain `ExtractionHealth::NotRequired` where already established.
- Coherent bounded partial crawl may be ready for post-crawl processing but can never become a trusted complete snapshot merely because extraction health is healthy.
- Cancellation before the post-crawl handoff and fatal/unsafe durable-state contradiction must not schedule new downstream work.
- Use a private closed typed post-crawl disposition; do not use arbitrary string classification.
- Durable execution/discovery/work/artifact evidence is the handoff authority. Do not hand transient provider response bodies or credentials across the post-crawl seam.
- Do not add `CrawlRecoveryPhase::Extracting` or modify checkpoint schema/version semantics.
- Do not activate or alter generic `ExtractionResumeState` semantics in DX-D05.
- Do not modify `erabi-extraction/Cargo.toml` or `erabi-extraction/src/**`.
- Do not add `erabi-extraction` as a dependency of `erabi-jobs`, `erabi-crawler`, `erabi-domain`, or `erabi-db`.
- Do not create speculative `ExtractionStage`, `ProductionExtractionStage`, `NoopExtractor`, or equivalent future API.
- Do not implement Plan 07 extraction, validation, schema drift, Dataset, review, candidate, or provenance behavior.
- Do not create migrations or change DB schema/repository semantics.
- Do not redesign `SemanticTraversal` (DX-D06).
- Do not decompose crawler repositories or alter their transaction semantics (DX-D07).
- Do not redesign checkpoint compatibility/version naming (DX-D03).
- Do not change API/OpenAPI contracts, CLI process composition, persisted primary CrawlRunStatus values, provider semantics, robots/network/pacing behavior, retry semantics, or progress meaning/order.
- Follow Erabi implementation-first convention. Do not add failing-test-first/TDD ceremony.
- Do not introduce a new testing architecture. Use existing crawler tests, jobs production integration tests, and true private unit tests where appropriate.
- Use `-j 1` for heavy Cargo verification on Windows when useful to avoid pagefile pressure.
- Do not use `cargo clean`.
- Do not commit, push, or create a PR during implementation. Keep one uncommitted candidate until independent Terra review returns `BLOCKER: 0` and `IMPORTANT: 0`.

---

## Expected File Map

The implementation should center on these files:

```text
crates/erabi-crawler/src/finalization.rs
    Canonical crawl-only structural reconstruction plus legacy finalization wrappers.

crates/erabi-crawler/src/lib.rs
    Existing glob export should keep the old finalization surface and expose crawl facts naturally.

crates/erabi-jobs/src/production.rs
    Removed/replaced by the `production/` module directory during the refactor.

crates/erabi-jobs/src/production/mod.rs
    Stable ProductionCrawlJobHandler, shared private types/helpers, JobHandler integration, stage sequencing.

crates/erabi-jobs/src/production/crawl_stage.rs
    Frozen-run loading/recovery and SemanticTraversal-driven crawl-stage orchestration, durable traversal/work persistence, checkpoint progression, and typed post-crawl disposition.

crates/erabi-jobs/src/production/page_execution.rs
    One bounded physical page-attempt flow: network admission, robots, pacing, provider execution, normalization, artifact/execution evidence helpers that belong to the page-attempt responsibility.

crates/erabi-jobs/src/production/finalization.rs
    Durable crawl-fact reconstruction, jobs-owned ExtractionHealth composition, domain complete-snapshot decision, durable run finalization/reconciliation.

crates/erabi-jobs/src/lib.rs
    Module declaration only if required by the file-to-directory conversion; existing public jobs exports/behavior must remain stable.

crates/erabi-jobs/tests/production.rs
    Primary end-to-end regression proof for Production behavior and stage outcomes.

docs/roadmap/04-engineering-dx.md
    Mark DX-D05 CURRENT during implementation and link the approved design without changing product order.
```

The exact number of `production/` files may differ only if source inspection proves a clearly more cohesive responsibility map. Do not split by symbol type or line count. If the proposed boundary materially differs from the approved semantic responsibilities, STOP for design review.

Files expected to remain unchanged include:

```text
crates/erabi-extraction/Cargo.toml
crates/erabi-extraction/src/**
crates/erabi-domain/src/complete_snapshot.rs
crates/erabi-db/**
migrations/**
crates/erabi-api/**
crates/erabi-cli/**
```

A tiny import/export adjustment outside the primary file map is acceptable only when mechanically required by the refactor and must be reported explicitly.

---

### Task 1: Introduce Canonical Crawl-Only Structural Facts in `erabi-crawler`

**Files:**
- Modify: `crates/erabi-crawler/src/finalization.rs`
- Modify only if mechanically required: `crates/erabi-crawler/src/lib.rs`
- Tests: existing private tests in `crates/erabi-crawler/src/finalization.rs`

**Interfaces:**
- Consumes: existing durable execution, discovery, checkpoint, traversal-control, logical-work evidence.
- Produces:

```rust
pub struct CrawlStructuralFacts {
    pub status: CrawlRunStatus,
    pub in_scope_pages_planned: u64,
    pub in_scope_pages_completed: u64,
    pub pagination_truncation_count: u64,
    pub unresolved_partial_work_count: u64,
    pub page_type_ambiguity_count: u64,
}

pub fn reconstruct_crawl_structural_facts(
    snapshot: &CrawlRunSnapshot,
    current_status: CrawlRunStatus,
    executions: &[CrawlExecutionRecord],
    discovered_urls: &[DiscoveredUrlRecord],
    checkpoint: Option<&CrawlCheckpoint>,
    control: Option<&CrawlTraversalControl>,
    work: Option<&[CrawlUrlStateRecord]>,
) -> Result<CrawlStructuralFacts, CrawlFinalizationError>;
```

If implementation evidence strongly favors a clearer equivalent function name, retain the exact `CrawlStructuralFacts` semantics and record the naming change. Do not alter the input evidence authority or behavior.

- [ ] **Step 1: Capture baseline crawler finalization behavior before edits.**

Run:

```powershell
cargo test -p erabi-crawler -j 1
cargo check -p erabi-crawler --all-targets -j 1
```

Expected: PASS on the clean implementation baseline. If baseline fails independently of DX-D05, STOP and report.

- [ ] **Step 2: Extract structural reconstruction from complete-snapshot composition.**

In `finalization.rs`, keep all current validation/counting/status logic authoritative:

```text
validate execution/discovery/run identities
→ reconstruct planned set
→ reconstruct completed/unresolved work
→ reconstruct ambiguity
→ reconstruct pagination/duration incompleteness
→ derive current CrawlRunStatus
→ return CrawlStructuralFacts
```

The canonical function must stop after producing `CrawlStructuralFacts`. It must not construct `ExtractionHealth`, `CompleteSnapshotStructuralInput`, or `CompleteSnapshotStructuralDecision`.

Do not change existing counting semantics such as:

- logical work taking precedence over historical attempts;
- partial work counting as completed plus unresolved;
- pagination truncation not being double-counted as generic duration work;
- Quick Scrape current-generation terminal-state behavior;
- cancelled/failed current statuses remaining terminal;
- ambiguity retained from durable evidence.

- [ ] **Step 3: Keep legacy crawler finalization APIs as compatibility wrappers.**

Preserve these public functions with existing signatures and behavior:

```rust
finalize_durable_state(...)
finalize_durable_state_with_control(...)
finalize_durable_state_with_traversal(...)
```

and preserve:

```rust
pub struct CrawlFinalization {
    pub structural_input: CompleteSnapshotStructuralInput,
    pub decision: CompleteSnapshotStructuralDecision,
    pub status: CrawlRunStatus,
}
```

The deepest compatibility wrapper should call `reconstruct_crawl_structural_facts(...)`, then adapt facts to the historical extraction-health default:

```rust
let extraction_health = match snapshot.run_type() {
    CrawlRunType::ProductionRun => ExtractionHealth::NotEvaluated,
    CrawlRunType::QuickScrape | CrawlRunType::TestRun | CrawlRunType::DiscoveryPreview => {
        ExtractionHealth::NotRequired
    }
};
```

Build the same `CompleteSnapshotStructuralInput`, call `decide()`, and return the same `CrawlFinalization` result as before.

Do not expose a new generic extraction-health composition helper from `erabi-crawler`; that would preserve the wrong ownership.

- [ ] **Step 4: Add focused structural-facts tests.**

Add/adapt tests so the canonical facts function independently proves at least:

```text
complete coherent crawl
→ planned == completed, no truncation/unresolved/ambiguity

pagination truncation
→ pagination_truncation_count > 0 and status PARTIAL_RESULT

coherent partial work
→ completed may be > 0, unresolved > 0, status PARTIAL_RESULT

Page Type ambiguity
→ page_type_ambiguity_count > 0, status PARTIAL_RESULT

inconsistent durable evidence
→ CrawlFinalizationError::Invariant
```

Add an explicit source-level/unit assertion through construction/use that `CrawlStructuralFacts` contains no extraction health and its result is usable without deciding complete-snapshot trust.

Retain the existing compatibility tests proving Production remains `ExtractionHealth::NotEvaluated` and legacy finalizers retain previous outcomes.

- [ ] **Step 5: Run Task 1 verification.**

Run:

```powershell
cargo test -p erabi-crawler -j 1
cargo check -p erabi-crawler --all-targets -j 1
cargo fmt --all --check
```

Inspect:

```powershell
git diff -- crates/erabi-crawler/src/finalization.rs crates/erabi-crawler/src/lib.rs
```

**Task 1 review gate:** reject if crawler facts contain extraction health, if old public finalization APIs disappear/change signatures, if counting/status behavior changes, or if another crate is modified unnecessarily.

---

### Task 2: Convert Production to a Focused Module and Extract Bounded Page Execution

**Files:**
- Replace: `crates/erabi-jobs/src/production.rs`
- Create: `crates/erabi-jobs/src/production/mod.rs`
- Create: `crates/erabi-jobs/src/production/page_execution.rs`
- Modify only if mechanically required: `crates/erabi-jobs/src/lib.rs`
- Tests: existing private production unit tests and `crates/erabi-jobs/tests/production.rs`

**Interfaces:**
- Consumes: current `ProductionCrawlJobHandler`, provider policies, progress/error helpers, existing jobs/public API.
- Produces: stable handler/public behavior plus a private page-execution module used by crawl-stage orchestration.

- [ ] **Step 1: Convert the Rust module path without changing public behavior.**

Move the current `production.rs` contents into `production/mod.rs` as the starting point, preserving:

```rust
pub struct ProductionCrawlJobHandler
impl ProductionCrawlJobHandler::new
with_progress_live_hub
with_clock
impl JobHandler for ProductionCrawlJobHandler
```

`crates/erabi-jobs/src/lib.rs` should continue to declare:

```rust
mod production;
```

Rust resolves the directory module automatically; no public export redesign is required.

Immediately run:

```powershell
cargo check -p erabi-jobs --all-targets -j 1
cargo test -p erabi-jobs --test production -j 1
```

before extracting responsibility. This proves the file-to-directory conversion itself is behavior-neutral.

- [ ] **Step 2: Move the physical page-attempt responsibility into `page_execution.rs`.**

Move together the cohesive runtime page-attempt surface, including the equivalents of:

```text
ProductionTraversalProvider
ProductionDeadline
SystemProductionClock
ProductionPageAttempt
PageResult
PageFailure
execute_page
provider adapter-error mapping
HTML response filtering used by PageResult
```

Also move helper code whose only reason to exist is page execution/admission/provider outcome handling. Keep persistence helpers in the module that owns durable crawl-stage evidence if they are not purely page-attempt concerns.

Use `pub(super)` only where `crawl_stage.rs`/`mod.rs` needs the symbol. Do not make these types public outside the `production` module.

Preserve this exact page-attempt ordering:

```text
parse/validate target
→ network target policy
→ pacing registration
→ robots evaluation
→ pacing permit
→ remaining frozen-run timeout
→ PAGE_LOADING progress
→ CrawlerExecuteRequest construction
→ provider call
→ pacing outcome recording
→ final URL validation
→ normalized PageResult/PageFailure
```

Do not move policy ownership from `erabi-crawler` into jobs; the module merely orchestrates existing policy/service calls.

- [ ] **Step 3: Preserve telemetry and primary/secondary diagnostics.**

Ensure the moved code retains the same:

```text
NETWORK_TARGET_REJECTED
ORIGIN_INVALID
PACING_REGISTRATION_FAILED
ROBOTS_POLICY_FAILED
ROBOTS_PACING_OUTCOME_RECORD_FAILED
ROBOTS_EXCLUDED
PACING_PERMIT_ACQUISITION_FAILED
provider diagnostic/error-code mapping
PACING_OUTCOME_RECORD_FAILED
```

and the existing ProviderExecuteCompleted semantic event behavior.

No error category/action/code should change merely because code moved modules.

- [ ] **Step 4: Preserve deterministic clock/deadline tests.**

Keep the existing no-sleep duration/deadline test and production recovery-kind routing test. Relocate private tests to the module that owns their private implementation if needed; do not move them into a new test architecture just for style.

- [ ] **Step 5: Run Task 2 verification.**

Run:

```powershell
cargo test -p erabi-jobs --test production -j 1
cargo test -p erabi-jobs -j 1
cargo check -p erabi-jobs --all-targets -j 1
cargo fmt --all --check
```

Inspect the module tree and diff. `production/mod.rs` may still be large at this checkpoint; that is acceptable because Tasks 3–4 complete the semantic split.

**Task 2 review gate:** reject if provider call behavior, robots/pacing/network ordering, public handler API, diagnostics, progress, or tests change semantically.

---

### Task 3: Extract Crawl-Stage Orchestration and Introduce the Typed Post-Crawl Disposition

**Files:**
- Create: `crates/erabi-jobs/src/production/crawl_stage.rs`
- Modify: `crates/erabi-jobs/src/production/mod.rs`
- Modify as needed: `crates/erabi-jobs/src/production/page_execution.rs`
- Test: `crates/erabi-jobs/tests/production.rs`

**Interfaces:**
- Consumes: stable handler dependencies and `page_execution` provider surface from Task 2.
- Produces a private stage result equivalent to:

```rust
pub(super) enum PostCrawlDisposition {
    ReadyForPostCrawl {
        run_id: CrawlRunId,
        current_status: CrawlRunStatus,
    },
    TerminalNoPostCrawl,
}
```

The exact internal payload may additionally carry immutable snapshot identity or bounded durable references needed to avoid reloading obvious identities. It must not carry provider bodies, raw HTML, credentials, extraction state, or an `ExtractionHealth` result.

- [ ] **Step 1: Move frozen-run/recovery/traversal orchestration into `crawl_stage.rs`.**

Move the cohesive parts of current `execute_inner` that own:

```text
job/run-id loading needed for crawl stage
frozen snapshot + semantic config loading
Production limits/deadline construction
existing execution/discovery/checkpoint inspection
recovery action selection
durable traversal reconstruction
SemanticTraversal create/restore
initial root/traversal state persistence
traversal loop
per-step discovery/work/execution persistence
crawl checkpoint save
storage-pressure/cancellation safe boundaries
```

Keep `SemanticTraversal` algorithms themselves untouched in `erabi-crawler`.

The stage may call existing jobs-private persistence/progress helpers. Group helpers in `crawl_stage.rs` when they only support crawl-stage orchestration; keep `mod.rs` focused on workflow sequencing.

- [ ] **Step 2: Make the successful crawl handoff explicit.**

When traversal reaches a coherent durable completion boundary, return:

```text
PostCrawlDisposition::ReadyForPostCrawl
```

for both:

- structurally complete bounded crawl;
- coherent bounded partial crawl where durable evidence can safely be inspected downstream.

Do not use structural completeness itself as the eligibility switch. Final trusted-snapshot eligibility is decided later from structural facts + extraction health.

The handoff must occur only after the relevant traversal/work/execution/checkpoint evidence has been durably persisted according to the existing ordering.

- [ ] **Step 3: Keep cancellation/fatal/unsafe states out of the post-crawl seam.**

Cancellation before handoff must preserve existing cancellation finalization/progress/error behavior and result in terminal/no-post-crawl behavior rather than returning ReadyForPostCrawl.

Unsafe recovery contradictions and invariant/repository/checkpoint errors must remain errors; they must not be converted to a partial ReadyForPostCrawl outcome merely because artifacts exist.

Storage-pressure behavior must remain exactly as current runtime semantics require. Do not reinterpret a currently resumable storage-pressure return as a terminal fatal outcome.

- [ ] **Step 4: Keep durable checkpoint ownership unchanged.**

The crawl stage continues to use existing `CrawlCheckpointV2` and current `CrawlRecoveryPhase::{Initialized, Traversing, Finalizing}` behavior. Do not add an extracting phase, do not modify payload versions, and do not write generic extraction resume fields.

Source audit after Task 3 must show no changes to:

```text
crates/erabi-crawler/src/checkpoint.rs
crates/erabi-db/src/repositories/checkpoint.rs
```

unless an import-only mechanical move is proven necessary; semantic changes there are a design violation and require STOP.

- [ ] **Step 5: Add/adjust integration tests for post-crawl eligibility.**

Use existing Production fixtures to prove:

```text
coherent complete run
→ reaches normal post-crawl/finalization path

coherent provider-partial/bounded partial run
→ is not rejected as fatal before post-crawl composition
→ final status remains PARTIAL_RESULT

cancellation before handoff
→ terminal cancellation path
→ no synthetic post-crawl success

unsafe/incompatible recovery evidence
→ existing typed failure/recovery behavior
→ never treated as post-crawl-ready
```

Do not add a fake extraction service to observe the seam. Test via durable status/progress/evidence and private closed-disposition unit coverage where needed.

- [ ] **Step 6: Run Task 3 verification.**

Run:

```powershell
cargo test -p erabi-jobs --test production -j 1
cargo test -p erabi-jobs -j 1
cargo check -p erabi-jobs --all-targets -j 1
cargo fmt --all --check
```

**Task 3 review gate:** reject if coherent partial work is collapsed into fatal/no-post-crawl solely due to incompleteness, if cancellation begins new work, if recovery/checkpoint semantics change, or if `SemanticTraversal` is redesigned.

---

### Task 4: Move Production Complete-Snapshot Composition into Jobs Finalization

**Files:**
- Create: `crates/erabi-jobs/src/production/finalization.rs`
- Modify: `crates/erabi-jobs/src/production/mod.rs`
- Modify: `crates/erabi-jobs/src/production/crawl_stage.rs`
- Consume: `erabi_crawler::CrawlStructuralFacts` and `reconstruct_crawl_structural_facts` from Task 1
- Test: `crates/erabi-jobs/tests/production.rs`

**Interfaces:**
- Consumes: `ReadyForPostCrawl`, durable repositories, frozen `CrawlRunSnapshot`, and Task 1 crawl-only facts.
- Produces jobs-owned complete-snapshot composition and the existing final durable CrawlRun status.

Define a small private composition helper equivalent to:

```rust
fn complete_snapshot_input(
    snapshot: &CrawlRunSnapshot,
    facts: &CrawlStructuralFacts,
    extraction_health: ExtractionHealth,
) -> CompleteSnapshotStructuralInput {
    CompleteSnapshotStructuralInput {
        run_type: snapshot.run_type(),
        status: facts.status,
        in_scope_pages_planned: facts.in_scope_pages_planned,
        in_scope_pages_completed: facts.in_scope_pages_completed,
        pagination_truncation_count: facts.pagination_truncation_count,
        unresolved_partial_work_count: facts.unresolved_partial_work_count,
        page_type_ambiguity_count: facts.page_type_ambiguity_count,
        extraction_health,
    }
}
```

The helper stays in jobs. Do not move it back into crawler or domain.

- [ ] **Step 1: Move durable finalization/reconciliation into `finalization.rs`.**

Move the current finalization application flow:

```text
load execution rows
load discovered rows
validate latest compatible crawl checkpoint lineage
reconstruct durable traversal work/control
reconstruct CrawlStructuralFacts from crawler
compose ExtractionHealth
construct CompleteSnapshotStructuralInput
call domain `decide()`
build CrawlExecutionSummary
persist repository finalization
mark terminal crawl run in JobExecutionContext
```

Preserve current error category/action/code behavior, especially `CRAWL_RUN_FINALIZATION_FAILED`, checkpoint-load/invalid errors, and durable finalization retry semantics.

- [ ] **Step 2: Make the DX-D05 post-crawl compatibility stage explicit.**

In workflow sequencing, after `ReadyForPostCrawl`, obtain current post-crawl health through a direct, non-speculative compatibility step:

```rust
let extraction_health = ExtractionHealth::NotEvaluated;
```

Do not introduce a trait/service/no-op extractor around this value.

Pass the value to jobs finalization. This is the explicit seam that Plan 07 will later replace with real `erabi-extraction` work.

- [ ] **Step 3: Prove extraction health cannot repair structural incompleteness.**

Add a focused jobs-private test for composition using synthetic `CrawlStructuralFacts`:

```text
facts.status = PARTIAL_RESULT
facts contain unresolved/pagination/ambiguity structural incompleteness
ExtractionHealth::Healthy
→ domain decision remains Incomplete
```

This test is architecture evidence only; do not change Production compatibility health to Healthy in runtime.

Also prove:

```text
structurally complete Production facts
+ ExtractionHealth::NotEvaluated
→ domain decision remains Incomplete
```

matching pre-DX-D05 behavior.

- [ ] **Step 4: Preserve terminal progress semantics.**

Keep existing successful terminal sequence/keys and secondary-error handling:

```text
FINALIZATION_COMPLETED
PRODUCTION_PARTIAL_RESULT or PRODUCTION_BOUNDED_COMPLETE
terminal ProgressTerminalState::Succeeded
```

Cancellation keeps its existing cancellation-specific terminal progress.

Do not add `EXTRACTION_STARTED`/`EXTRACTION_COMPLETED` events in DX-D05 because no extraction executes yet.

- [ ] **Step 5: Run Task 4 verification.**

Run:

```powershell
cargo test -p erabi-crawler -j 1
cargo test -p erabi-jobs --test production -j 1
cargo test -p erabi-jobs -j 1
cargo check -p erabi-crawler --all-targets -j 1
cargo check -p erabi-jobs --all-targets -j 1
cargo fmt --all --check
```

Inspect source and ensure Production jobs now call `reconstruct_crawl_structural_facts` rather than the legacy crawler finalization wrapper for the canonical Production path.

**Task 4 review gate:** reject if Production still delegates extraction-health composition to crawler, if runtime health changes away from NotEvaluated, if progress/terminal state changes, or if a speculative extraction abstraction appears.

---

### Task 5: Complete Responsibility Decomposition and DX Lifecycle Documentation

**Files:**
- Modify: `crates/erabi-jobs/src/production/mod.rs`
- Modify as needed: `crawl_stage.rs`, `page_execution.rs`, `finalization.rs`
- Modify: `docs/roadmap/04-engineering-dx.md`
- Read only: `docs/ROADMAP.md`

**Interfaces:**
- Consumes: Tasks 1–4 working implementation.
- Produces: reviewable stage-oriented source ownership and accurate DX lifecycle status.

- [ ] **Step 1: Audit `production/mod.rs` for composition-root responsibility.**

By the end of this task, `mod.rs` should primarily contain:

```text
shared imports/constants/types genuinely used across stages
ProductionError / ProductionResult if shared broadly
ProductionCrawlJobHandler construction/configuration
execute_inner high-level workflow sequencing
JobHandler implementation
small shared progress/error/identity helpers only when no child module clearly owns them
```

`execute_inner` should read conceptually as:

```text
validate/load durable Production identity
→ run/recover crawl stage
→ inspect PostCrawlDisposition
→ current post-crawl compatibility health (NotEvaluated)
→ finalization stage
→ terminal progress
```

Do not keep the old entire traversal loop or page provider implementation in `mod.rs` merely to avoid moving private helpers.

- [ ] **Step 2: Audit child modules by semantic ownership, not size.**

Confirm:

```text
crawl_stage.rs
    traversal/recovery/durable crawl-stage orchestration

page_execution.rs
    bounded physical page attempt/provider admission and normalization

finalization.rs
    crawl-fact + extraction-health + domain-decision composition and durable terminalization
```

A helper used only by one responsibility should live with that responsibility. A helper shared by two tightly related production stages may remain in the nearest common parent.

Do not split further simply because a module is large. Do not perform DX-D06/D07 cleanup.

- [ ] **Step 3: Update the DX roadmap lifecycle.**

In `docs/roadmap/04-engineering-dx.md`:

- mark `DX-D05` as `CURRENT`;
- leave `DX-D04` as `MERGED`;
- do not mark DX-D05 MERGED yet;
- add a focused DX-D05 section if none exists, linking:

```text
docs/superpowers/specs/2026-09-09-dx-d05-production-orchestration-design.md
```

The section should summarize why D05 precedes Plan 07, the ownership goal, behavior-preserving scope, explicit non-goals, and exit evidence. Do not duplicate the entire spec.

Run:

```powershell
git diff -- docs/ROADMAP.md
```

Expected: empty. Product milestone order remains unchanged.

- [ ] **Step 4: Run source dependency/non-goal audit.**

Run repository searches equivalent to:

```powershell
rg -n "erabi[_-]extraction|ExtractionStage|ProductionExtractionStage|NoopExtractor|FakeExtractionService" crates/erabi-jobs crates/erabi-crawler crates/erabi-domain crates/erabi-db Cargo.toml
rg -n "Extracting|CrawlRecoveryPhase::" crates/erabi-crawler/src/checkpoint.rs crates/erabi-jobs/src/production
```

Interpret existing unrelated references carefully, but required DX-D05 truth is:

```text
no new erabi-extraction dependency/use
no speculative extraction abstraction
no new crawler Extracting phase
```

Verify explicitly:

```powershell
git diff -- crates/erabi-extraction/Cargo.toml crates/erabi-extraction/src
```

Expected: empty.

- [ ] **Step 5: Run Task 5 focused checks.**

Run:

```powershell
cargo check -p erabi-crawler --all-targets -j 1
cargo check -p erabi-jobs --all-targets -j 1
cargo fmt --all --check
git diff --check
```

**Task 5 review gate:** reject if `mod.rs` remains the old monolith under a new path, if responsibilities are mixed, if roadmap product order changes, or if extraction/D06/D07/checkpoint work enters the diff.

---

### Task 6: Run the Full DX-D05 Regression and Architecture Gate

**Files:**
- Modify only when a concrete DX-D05 defect is found.
- Do not broaden scope for cleanup during final verification.

**Interfaces:**
- Consumes: complete Tasks 1–5 candidate.
- Produces: one verified uncommitted candidate ready for independent Terra HIGH review.

- [ ] **Step 1: Run the complete approved test gate.**

Run:

```powershell
cargo test -p erabi-crawler -j 1
cargo test -p erabi-jobs --test production -j 1
cargo test -p erabi-jobs -j 1

cargo test -p erabi --lib -j 1
cargo test -p erabi --test runtime_server -j 1
```

Expected: PASS.

- [ ] **Step 2: Run all-target checks.**

```powershell
cargo check -p erabi-crawler --all-targets -j 1
cargo check -p erabi-jobs --all-targets -j 1
cargo check -p erabi --all-targets -j 1
```

Expected: PASS.

- [ ] **Step 3: Run format/lint/diff gates.**

```powershell
cargo fmt --all --check
cargo clippy -p erabi-crawler -p erabi-jobs -p erabi --all-targets -j 1 -- -D warnings
git diff --check
```

Expected: PASS.

Do not use `cargo clean` if the Windows pagefile is stressed; keep `-j 1`.

- [ ] **Step 4: Prove dependency boundaries.**

Inspect:

```powershell
git diff -- crates/erabi-jobs/Cargo.toml crates/erabi-crawler/Cargo.toml crates/erabi-extraction/Cargo.toml Cargo.toml Cargo.lock
```

Expected: no dependency changes for DX-D05.

If a manifest/lockfile changed, explain why; adding extraction or another new dependency is outside the approved design and requires STOP.

- [ ] **Step 5: Prove checkpoint and persistence semantics are untouched.**

Run:

```powershell
git diff -- crates/erabi-crawler/src/checkpoint.rs crates/erabi-db migrations
```

Expected: empty unless a truly mechanical import formatting change was already reviewed. Any semantic checkpoint/repository/migration diff is a scope violation.

- [ ] **Step 6: Prove API/CLI/product surfaces are untouched.**

Run:

```powershell
git diff -- crates/erabi-api crates/erabi-cli docs/ROADMAP.md
```

Expected: empty.

- [ ] **Step 7: Perform architecture source audit.**

Review the final production source and answer explicitly:

```text
Does mod.rs primarily sequence stages rather than implement all stages?
Does crawl_stage.rs own crawl/recovery/durable traversal orchestration only?
Does page_execution.rs own bounded provider/network/robots/pacing attempt flow?
Does finalization.rs own crawl-facts + ExtractionHealth composition and terminalization?
Does erabi-crawler facts reconstruction contain no ExtractionHealth?
Do legacy crawler finalizers still behave and remain callable?
Is the post-crawl disposition typed and closed?
Can coherent partial work reach the post-crawl seam without becoming trusted complete?
Can cancellation/fatal/unsafe state avoid new post-crawl work?
Is erabi-extraction untouched?
Are DX-D06/D07/D03 behaviors untouched?
```

All answers must satisfy the approved spec before handoff.

- [ ] **Step 8: Review the final changed-file scope.**

Run:

```powershell
git status --short
git diff --name-only
git diff --stat
git diff --cached
```

Expected cached diff: empty.

Expected changes should be limited to the D05 crawler finalization surface, jobs production module decomposition/tests, and DX roadmap lifecycle documentation. Report any additional file explicitly and justify it as mechanically necessary; otherwise revert/stop rather than hiding scope expansion.

- [ ] **Step 9: Prepare the independent review report.**

Return exactly these sections:

```text
BASELINE
BRANCH
IMPLEMENTATION SUMMARY
CRAWLER STRUCTURAL FACTS
LEGACY CRAWLER FINALIZATION COMPATIBILITY
PRODUCTION MODULE DECOMPOSITION
PAGE EXECUTION OWNERSHIP
CRAWL STAGE OWNERSHIP
POST-CRAWL DISPOSITION
FINALIZATION OWNERSHIP
EXTRACTION HEALTH COMPOSITION
PARTIAL / CANCELLATION / FATAL SEMANTICS
DURABLE HANDOFF / CHECKPOINT PRESERVATION
DEPENDENCY OWNERSHIP
ERABI-EXTRACTION NON-CHANGE
DX-D06 / DX-D07 NON-SCOPE PROOF
BEHAVIOR PRESERVATION
DX ROADMAP STATUS
FILES CHANGED
FRESH VERIFICATION
SCOPE CHECK
KNOWN LIMITATIONS
CONCLUSION
```

Finish exactly with one of:

```text
DX-D05 IMPLEMENTATION COMPLETE — READY FOR INDEPENDENT REVIEW
```

or:

```text
DX-D05 IMPLEMENTATION BLOCKED — REVIEW REQUIRED
```

Then STOP.

Do not commit.
Do not push.
Do not create a PR.

---

## Final Acceptance Criteria

DX-D05 is ready for independent review only when all are true:

1. `ProductionCrawlJobHandler` remains the stable public Production jobs entry point.
2. `erabi-jobs/src/production.rs` has been replaced by a stage-oriented `production/` module with real semantic responsibility separation.
3. `erabi-crawler` exposes canonical crawl-only `CrawlStructuralFacts` reconstructed from the same durable evidence semantics as before.
4. Crawl structural reconstruction contains no `ExtractionHealth` composition.
5. Existing exported crawler finalization functions and `CrawlFinalization` remain available and behavior-compatible.
6. Production jobs use crawl-only structural facts for their canonical finalization path.
7. Jobs own construction of `CompleteSnapshotStructuralInput` from crawl facts + extraction health.
8. DX-D05 runtime Production health remains `ExtractionHealth::NotEvaluated`.
9. The domain remains the Complete/Incomplete decision authority through `CompleteSnapshotStructuralInput::decide()`.
10. An explicit private typed post-crawl disposition separates ReadyForPostCrawl from terminal/no-post-crawl conditions.
11. Coherent bounded partial crawl can reach the post-crawl seam but cannot become trusted complete because of extraction health or any other downstream stage.
12. Cancellation before handoff does not schedule new post-crawl work.
13. Fatal/invariant/unsafe durable contradiction cannot be converted into a post-crawl-ready partial outcome.
14. Crawl-to-post-crawl handoff depends on durable evidence, not transient provider response bodies.
15. Provider/network/robots/pacing behavior and ordering remain unchanged.
16. Durable execution/artifact/discovery/work/checkpoint behavior remains unchanged.
17. Progress keys/order/terminal meaning remain unchanged; no fake extraction progress is added.
18. No new persisted CrawlRunStatus values are added.
19. `erabi-extraction` source/manifests remain unchanged and jobs do not depend on it yet.
20. Checkpoint schema/version and crawler recovery phase semantics remain unchanged.
21. SemanticTraversal is not redesigned and crawler repositories are not refactored.
22. No DB/migration/API/OpenAPI/CLI/product behavior changes enter the package.
23. DX-D05 is marked CURRENT in the engineering DX roadmap while implementation is unmerged; product roadmap order is unchanged.
24. Full approved focused/runtime verification passes with strict Clippy/format/diff checks.
25. The candidate remains uncommitted and unstaged for Terra HIGH review.
26. Independent review acceptance requires `BLOCKER: 0` and `IMPORTANT: 0`.

## Independent Review Focus

Terra should scrutinize these high-risk seams:

- whether `CrawlStructuralFacts` is truly crawl-only or still hides extraction/trust composition;
- whether Production jobs really use the new crawl-only core instead of continuing through the legacy wrapper;
- whether compatibility wrapper refactoring subtly changes Quick Scrape/Production counts/statuses;
- whether `production/mod.rs` remains effectively the old monolith;
- whether page execution preserves network → robots → pacing → provider ordering and diagnostics;
- whether crawl-stage extraction changes recovery/checkpoint durability ordering;
- whether the typed post-crawl disposition correctly distinguishes coherent partial from cancellation/fatal/unsafe state;
- whether partial + hypothetical healthy extraction could accidentally become trusted complete;
- whether NotEvaluated Production behavior remains unchanged until Plan 07;
- whether progress, cancellation, storage pressure, retries, artifacts, and recovery remain behavior-equivalent;
- whether speculative extraction API/dependency work was introduced;
- whether DX-D06/D07/D03 or unrelated cleanup is mixed into the diff.

Acceptance threshold:

```text
BLOCKER: 0
IMPORTANT: 0
```
