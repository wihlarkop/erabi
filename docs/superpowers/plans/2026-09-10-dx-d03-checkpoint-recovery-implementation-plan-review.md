# DX-D03 Implementation Plan Self-Review

**Plan:** `docs/superpowers/plans/2026-09-10-dx-d03-checkpoint-recovery-implementation-plan.md`  
**Spec:** `docs/superpowers/specs/2026-09-10-dx-d03-checkpoint-recovery-contract-design.md`  
**Date:** 2026-09-10

## Result

**DX-D03 IMPLEMENTATION PLAN SELF-REVIEW CLEAN**

The plan is sufficiently specific for implementation after user approval. No implementation code has been started.

## Checks Performed

### Placeholder / incompleteness scan

- No `TBD` or `TODO` requirements.
- No unspecified migration number.
- No unspecified checkpoint payload kind.
- No unspecified envelope or crawl-recovery format version.
- No unspecified recovery API status code.
- No requirement to choose between legacy/current decoders at implementation time.

### Architecture consistency

The plan preserves the approved ownership model:

```text
erabi-db
→ generic append-only checkpoint transport only

erabi-crawler
→ CrawlRecoveryCheckpoint + crawl structural truth

erabi-jobs
→ recovery/action workflow semantics

erabi-api
→ safe HTTP presentation

Plan 07
→ future extraction recovery design
```

The plan does not reintroduce `ExtractionResumeState`, growing crawl-unit checkpoint payloads, or checkpoint-owned crawl progress truth.

### Pre-release compatibility policy

The plan consistently applies the approved rule that development-only compatibility is not an acceptance requirement before Erabi's first release:

- no `CrawlCheckpointV2` alias;
- no deprecated `CrawlCheckpoint` compatibility type;
- no dual decoder;
- no V1/V2 converter;
- no JSON rewrite migration;
- no SQL migration for checkpoint JSON;
- historical rows fail closed.

### Persistence / migration consistency

The plan preserves the reviewed migration allocation:

```text
0001–0006 implemented / immutable
0007 Plan 07
0008 Plan 08
0009 next unallocated
```

DX-D03 changes only the JSON stored in existing `job_checkpoints.checkpoint_json`; it must not consume `0009` or modify `0004_jobs.sql`.

### Recovery semantics consistency

The plan keeps the approved distinction:

```text
generic envelope + immutable identity
→ resume candidate only

CrawlRecoveryCheckpoint validation
+ durable crawl-state reconstruction
→ actual same-run recovery permission
```

This prevents `erabi-db` from overclaiming crawler-specific safety while ensuring explicit Resume and recovered runtime execution use the same crawler recovery authority before provider work.

The action distinctions remain explicit:

- Resume = same-run continuation from safe current state;
- Retry = bounded continuation that cannot bypass required recovery validation;
- Retry Failed Parts = durable failed/partial logical-work selection;
- Quick Scrape Restart = explicit start-over action where legal;
- Production Restart = illegal;
- Production Rerun Full Crawl = independent new run;
- storage pressure = resumable deferral.

### Error taxonomy consistency

The plan implements the approved public recovery conflict codes, all HTTP 409:

```text
CHECKPOINT_MISSING
CHECKPOINT_FORMAT_UNSUPPORTED
CHECKPOINT_MALFORMED
CHECKPOINT_IDENTITY_MISMATCH
RECOVERY_STATE_INVALID
```

Generic envelope errors, crawl-payload errors, immutable-identity mismatch, and durable recovery-state contradiction remain distinct. Raw checkpoint/provider/credential content must not leak.

### Task ordering

The five tasks form one dependency chain:

```text
1. generic DB envelope
        ↓
2. canonical crawler recovery
        ↓
3. checkpoint-independent structural facts
        ↓
4. jobs/runtime/action integration
        ↓
5. API contract + full verification
```

Task 1 intentionally breaks downstream source compatibility until Task 2/4 migrate callers. Therefore Task-level gates are ownership-local; only Task 5 may establish package-wide `VERIFIED` state.

### Scope check

The package remains focused. It does not implement:

- Plan 07 extraction/recovery;
- DX-D06 SemanticTraversal decomposition;
- DX-D07 repository decomposition;
- provider/network/robots/pacing changes;
- migration work;
- progress/SSE redesign;
- new run/job statuses;
- unrelated public API cleanup.

### Workflow consistency

The plan correctly overrides generic Superpowers defaults with Erabi repository rules:

- implementation-first, verification-after;
- no failing-test-first ceremony;
- no subagents;
- no `cargo clean`;
- no implementation commits/push/PR before Terra acceptance;
- fresh complete verification before `VERIFIED`;
- Terra HIGH `BLOCKER: 0`, `IMPORTANT: 0` before `ACCEPTED`.

## Binding Clarifications from Self-Review

Two plan sentences contain defensive call-site wording. They must **not** be interpreted as discretion to preserve legacy compatibility:

1. Task 1 legacy public checkpoint assessment surface (`CheckpointCompatibility`, `CheckpointRecoveryDisposition`, `CheckpointRecoveryAssessment`) is expected to be removed from the supported public API. The audited current responsibility is generic repository-internal stale-envelope classification. If implementation discovers a genuine non-legacy production consumer that makes removal architecturally incorrect, **STOP and report a design conflict** instead of retaining the old API silently.

2. Task 3 historical checkpoint-driven finalization wrappers (`CrawlFinalization`, `finalize_durable_state*`) are expected to be removed. Current Production uses `reconstruct_crawl_structural_facts` directly with durable traversal state. If implementation discovers a genuine current non-legacy production consumer, **STOP and report a design conflict** instead of preserving compatibility wrappers.

These clarifications do not change the approved architecture; they make the pre-release break policy explicit.

## Implementation Entry Condition

Implementation must not start until the user approves this plan. After approval, merge the planning-doc branch first, then begin implementation from the resulting fresh `main` in a distinct implementation session/branch.
