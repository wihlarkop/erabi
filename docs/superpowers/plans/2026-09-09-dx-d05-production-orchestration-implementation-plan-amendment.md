# DX-D05 Implementation Plan Amendment — Storage-Pressure Deferral and Handoff Payload

**Date:** 2026-09-09  
**Plan:** `docs/superpowers/plans/2026-09-09-dx-d05-production-orchestration-implementation-plan.md`  
**Spec:** `docs/superpowers/specs/2026-09-09-dx-d05-production-orchestration-design.md`

This amendment resolves two implementation-plan ambiguities found during self-review. It does not change the user-approved architecture.

## 1. Storage-pressure deferral is not a post-crawl disposition

The current Production behavior treats storage pressure during an unfinished crawl as a resumable/deferred worker boundary:

```text
crawl work still incomplete
→ storage pressure signalled
→ persist the existing safe durable state/checkpoint
→ return without Production terminalization
→ do not enter post-crawl processing
```

Therefore DX-D05 MUST NOT force storage-pressure deferral into either:

```text
ReadyForPostCrawl
```

or a terminal classification merely to satisfy the typed post-crawl design.

The approved `ReadyForPostCrawl` versus `TerminalNoPostCrawl` distinction applies only once the workflow has reached a real post-crawl eligibility/terminal decision. A resumable deferral can return outside that post-crawl disposition.

A valid implementation may use an outer stage result conceptually equivalent to:

```rust
enum CrawlStageOutcome {
    ReadyForPostCrawl(ReadyForPostCrawl),
    DeferredNoPostCrawl,
}
```

while preserving cancellation/fatal paths as the existing typed `Err(ProductionError)` where wrapping them in another enum would alter error, diagnostics, terminalization, progress, or retry semantics.

The architecture requirement is:

```text
coherent durable completion/partial boundary
→ may become ReadyForPostCrawl

storage-pressure resumable deferral
→ no post-crawl work
→ no new terminalization

cancellation before handoff
→ existing cancellation terminalization/progress/error
→ never ReadyForPostCrawl

fatal/invariant/unsafe contradiction
→ existing typed error/failure semantics
→ never ReadyForPostCrawl
```

A private closed `TerminalNoPostCrawl` variant MAY be used when it preserves existing semantics exactly, but the implementation MUST NOT manufacture a new terminal result merely so every exit fits one enum. Source/tests must prove cancellation/fatal/unsafe exits cannot reach `ReadyForPostCrawl`.

This clarification is behavior-preserving and takes precedence over any implementation-plan wording that appears to classify storage-pressure deferral as terminal.

## 2. Ready handoff carries immutable workflow identity, not transient provider state

Task 3's post-crawl handoff must provide Task 4 enough immutable context without reopening the crawl stage or depending on transient provider results.

Preferred private handoff shape is conceptually:

```rust
struct ReadyForPostCrawl {
    run_id: CrawlRunId,
    snapshot: CrawlRunSnapshot,
    current_status: CrawlRunStatus,
}
```

An equivalent shape is acceptable if it carries the same immutable workflow identity and Task 4 can reconstruct authoritative crawl facts from durable repositories.

The handoff MUST NOT contain:

```text
CrawlerExecuteResult
provider DTO/body
raw or rendered HTML body
credential/token
in-memory extraction result
ExtractionHealth
```

Task 4 reconstructs `CrawlStructuralFacts` from durable execution/discovery/work/control evidence. Carrying the already-loaded immutable `CrawlRunSnapshot` is permitted and preferred over needlessly reloading it solely because modules were split.

## 3. Task 3 / Task 4 interpretation

Interpret Task 3 as producing these semantic outcomes:

```text
ReadyForPostCrawl
    safe durable handoff reached;
    structurally complete OR coherent bounded partial;
    downstream stage may inspect durable evidence.

DeferredNoPostCrawl
    existing resumable storage-pressure boundary;
    no downstream work;
    no terminalization introduced.

Cancellation / fatal / unsafe error
    existing behavior preserved;
    cannot enter post-crawl path.
```

Interpret Task 4 as accepting only `ReadyForPostCrawl` and then performing:

```text
reconstruct authoritative CrawlStructuralFacts
→ use DX-D05 compatibility ExtractionHealth::NotEvaluated
→ construct CompleteSnapshotStructuralInput in jobs
→ domain decide()
→ durable finalization
```

## 4. Verification amendment

Focused tests/source review MUST explicitly prove:

1. storage pressure before crawl completion preserves the existing resumable/non-terminal behavior;
2. storage pressure does not emit finalization/post-crawl success merely because the module now has a post-crawl seam;
3. cancellation before handoff preserves the existing cancellation terminalization/progress/error path and never reaches ReadyForPostCrawl;
4. fatal/invariant/unsafe recovery failures never reach ReadyForPostCrawl;
5. ReadyForPostCrawl carries only immutable/bounded identity context and relies on durable repositories for structural truth;
6. no provider body/HTML/credential is carried across the seam.

## 5. Scope

This amendment does NOT authorize:

- a new persisted CrawlRunStatus;
- a new checkpoint phase/version;
- a new extraction abstraction;
- an `erabi-extraction` dependency;
- changed storage-pressure semantics;
- changed cancellation/error/retry semantics;
- DX-D06/D07 work.

All other implementation-plan tasks and acceptance criteria remain unchanged.
