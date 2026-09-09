# DX-D05 Implementation Plan Self-Review

**Date:** 2026-09-09  
**Plan:** `docs/superpowers/plans/2026-09-09-dx-d05-production-orchestration-implementation-plan.md`  
**Amendment:** `docs/superpowers/plans/2026-09-09-dx-d05-production-orchestration-implementation-plan-amendment.md`  
**Spec:** `docs/superpowers/specs/2026-09-09-dx-d05-production-orchestration-design.md`

## Result

```text
DX-D05 IMPLEMENTATION PLAN SELF-REVIEW CLEAN
```

The implementation plan plus its amendment covers the approved design without requiring Plan 07 extraction work, DX-D06 traversal redesign, DX-D07 repository decomposition, or DX-D03 checkpoint compatibility changes.

## Spec coverage

Reviewed every substantive design section against the implementation plan.

Covered:

- Production extraction remains a future explicit post-crawl stage in the same durable Production root workflow;
- `erabi-jobs` owns Production workflow/stage order, recovery coordination, progress, and terminalization;
- `erabi-crawler` owns crawl/traversal structural truth;
- `erabi-extraction` remains untouched until Plan 07;
- `erabi-domain` remains the complete-snapshot business-decision authority;
- `erabi-db` remains persistence mechanism, not extraction/trust policy owner;
- durable evidence, not provider DTO/body state, is the crawl-to-post-crawl handoff authority;
- generic extraction resume phases remain future Plan 07 recovery state and are not activated by DX-D05;
- crawler checkpoint schema/version and `CrawlRecoveryPhase` remain unchanged;
- canonical crawl-only `CrawlStructuralFacts` is introduced without `ExtractionHealth`;
- jobs owns `CrawlStructuralFacts + ExtractionHealth → CompleteSnapshotStructuralInput` composition;
- Production compatibility health remains `ExtractionHealth::NotEvaluated`;
- legacy crawler finalization functions and `CrawlFinalization` remain available and behavior-compatible;
- Production source is decomposed by semantic responsibility rather than line-count or symbol type;
- a typed post-crawl seam distinguishes coherent ready work from paths that must not begin downstream work;
- coherent bounded partial crawl can reach post-crawl processing but cannot become trusted complete;
- cancellation/fatal/unsafe state cannot enter post-crawl processing;
- run lifecycle values remain unchanged;
- no speculative extraction trait/service is introduced;
- provider/network/robots/pacing, progress, durable evidence, retry, cancellation, storage-pressure, checkpoint, and final status behavior are regression-gated;
- `erabi-extraction` dependency direction remains unchanged;
- DX roadmap marks DX-D05 CURRENT during implementation without changing product order;
- full crawler/jobs/runtime verification plus strict format/Clippy/diff gates are required;
- independent Terra review requires `BLOCKER: 0`, `IMPORTANT: 0`.

No approved design requirement remains without an implementation task or acceptance gate.

## Source-fit review

Current source supports the planned decomposition:

- `erabi-jobs/src/lib.rs` publicly re-exports `ProductionCrawlJobHandler`, so the plan explicitly preserves that export and handler API.
- `erabi-jobs/src/production.rs` currently contains both high-level workflow sequencing and the bounded physical page-attempt implementation, validating the `mod.rs` / `crawl_stage.rs` / `page_execution.rs` / `finalization.rs` ownership split.
- `erabi-crawler/src/finalization.rs` currently reconstructs crawl facts and then attaches `ExtractionHealth`, validating Task 1's separation into crawl-only facts plus compatibility wrappers.
- `erabi-extraction` remains an empty intended boundary with no runtime dependencies, validating the non-goal of creating a speculative API in DX-D05.

No new dependency or module boundary is required beyond the approved refactor.

## Self-review correction

One material implementation-plan ambiguity was found and corrected through the amendment.

### Storage pressure

Current Production behavior can return from an unfinished crawl on storage pressure without terminalizing the run. That state is resumable/deferred; it is neither:

```text
ReadyForPostCrawl
```

nor a newly terminal outcome.

The original Task 3 wording could have encouraged an implementation to force every crawl-stage exit into a two-variant post-crawl enum and accidentally convert storage pressure into terminal/no-post-crawl behavior.

The amendment now requires:

```text
storage-pressure deferral
→ preserve existing resumable behavior
→ no finalization
→ no post-crawl work
```

and allows that deferral to return outside the post-crawl disposition.

Cancellation and fatal/unsafe errors may retain their existing typed error paths when wrapping them would alter diagnostics, progress, terminalization, or retry semantics. The architectural invariant is that they cannot reach `ReadyForPostCrawl`.

### Handoff payload

The amendment also closes the Task 3 → Task 4 context gap. A ready handoff should carry the immutable run identity/snapshot already loaded by the crawl stage (or an equivalent bounded immutable context), while Task 4 reconstructs structural truth from durable repositories.

The handoff must not carry provider bodies, raw/rendered HTML, credentials, extraction results, or `ExtractionHealth`.

This amendment clarifies implementation mechanics without changing the user-approved architecture.

## Compatibility review

The plan explicitly preserves:

```text
erabi_jobs::ProductionCrawlJobHandler

erabi_crawler::CrawlFinalization
erabi_crawler::finalize_durable_state
erabi_crawler::finalize_durable_state_with_control
erabi_crawler::finalize_durable_state_with_traversal
```

The canonical Production jobs path moves to crawl-only facts, while compatibility callers retain the historical finalization behavior.

No public breaking cleanup is authorized by DX-D05.

## Behavior-preservation review

The plan contains explicit regression gates for:

- provider call count/order;
- network target admission;
- robots evaluation;
- pacing registration/acquisition/outcome recording;
- frozen-run duration timeout behavior;
- provider result normalization;
- artifact persistence;
- durable discovery/work/execution evidence;
- checkpoint/recovery lineage;
- retry/recovery action behavior;
- storage-pressure deferral;
- cancellation safe boundary;
- progress keys/order/terminal meaning;
- final CrawlRun status;
- primary/secondary execution diagnostics;
- Production `ExtractionHealth::NotEvaluated` trust behavior.

No intentional product/runtime semantic change is present in the plan.

## Non-goal review

The plan explicitly prevents:

```text
Plan 07 extraction implementation
HTML parsing/selectors/normalization/validation
Dataset/review/provenance persistence
schema-drift implementation
new migrations
child extraction jobs
new CrawlRunStatus values
new checkpoint phases/versions
SemanticTraversal redesign (DX-D06)
crawler repository decomposition (DX-D07)
checkpoint compatibility redesign (DX-D03)
API/OpenAPI changes
CLI process changes
new dependencies
speculative extraction trait/service
unrelated test-layout cleanup
```

## Verification review

Focused verification exists after each task and the final gate includes:

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

Final source audits additionally prove:

- no extraction dependency/source change;
- no checkpoint/repository/migration semantic change;
- no API/CLI/product-roadmap change;
- real module responsibility separation;
- typed post-crawl eligibility;
- storage-pressure/cancellation/fatal behavior preservation;
- legacy crawler finalization compatibility.

## Placeholder / ambiguity scan

No unresolved `TBD`, `TODO`, unowned interface, or unspecified acceptance criterion remains after the amendment.

The plan intentionally permits implementation-level naming/layout variation only when semantic responsibility remains identical. A material architecture change requires STOP and design review.

## Final conclusion

The plan is executable as written together with its amendment and is ready for explicit user plan approval before implementation begins.

```text
DX-D05 IMPLEMENTATION PLAN SELF-REVIEW CLEAN
```
