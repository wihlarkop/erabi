# DX-D03 Canonical Checkpoint and Recovery Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use `superpowers:executing-plans` to implement this plan task-by-task. Erabi explicitly forbids subagents for this package. Follow Erabi's implementation-first workflow; do not introduce RED/GREEN or failing-test-first ceremony. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Erabi's historical checkpoint/version surface with one clean pre-release recovery contract: a minimal generic `CheckpointEnvelope`, a semantic `CrawlRecoveryCheckpoint`, checkpoint-independent durable crawl reconstruction, one crawl recovery validation path across runtime/actions, and actionable API recovery errors.

**Architecture:** `erabi-db` owns only generic append-only checkpoint transport and immutable identity; `erabi-crawler` owns crawl-specific recovery control; `erabi-jobs` owns recovery workflow/action semantics and composes checkpoint validity with durable crawl-state coherence; `erabi-api` owns safe HTTP presentation. Durable crawl progress remains authoritative in execution/discovery/traversal-control/logical-work persistence introduced by migration `0006`. Plan 07 owns any future extraction recovery design.

**Tech Stack:** stable Rust, Serde/serde_json, Tokio, existing Turso repositories, existing Erabi crawler/jobs/API/runtime tests. No new runtime dependency is expected.

**Spec:** `docs/superpowers/specs/2026-09-10-dx-d03-checkpoint-recovery-contract-design.md`

## Global Constraints

- Implementation begins only after the DX-D03 planning docs are merged to `main`. Start a fresh implementation branch from that merged `main` and report the actual baseline SHA before editing code.
- Recommended implementation branch: `refactor/dx-d03-checkpoint-recovery-contract`.
- Erabi is pre-release. Historical development Rust APIs, checkpoint JSON shapes, aliases, and recovery rows are **not** compatibility requirements. Prefer the cleanest correct architecture and idiomatic naming.
- Old checkpoint formats must fail closed. Do not add compatibility aliases, deprecated wrappers, V1/V2 converters, dual decoders, automatic JSON rewrites, or migrations for development-only checkpoint state.
- Do not preserve a public symbol merely because tests or old source use it. Replace tests/callers with the canonical current concept.
- Do not change correct business semantics for Production/Quick Scrape merely because source and persistence formats may break.
- `CheckpointEnvelope` becomes generic transport only; it must not own crawl units, discovery cursors, artifact references, or extraction state.
- `CrawlRecoveryCheckpoint` is recovery control only; it must not become the crawl progress data plane.
- Durable crawl truth remains execution history + discovered URL evidence + `CrawlTraversalControl` + `CrawlUrlStateRecord` logical work.
- Explicit Resume and automatic recovered execution must use the same canonical crawl checkpoint decoder/identity validation before crawl work proceeds.
- A generic stale-job envelope check may classify a job only as a **resume candidate**. It must not claim full crawl recoverability before crawler-specific checkpoint validation and durable-state reconstruction.
- `Retry Failed Parts` selects failed/partial work from durable logical work, never from checkpoint unit arrays.
- Invalid Resume never silently falls back to Restart or Rerun.
- Quick Scrape may explicitly Restart from Beginning where current lifecycle rules allow it.
- Production same-run restart from Seeds remains illegal; fresh full Production execution uses `Rerun Full Crawl` and a new independent `CrawlRun`.
- Storage pressure remains resumable deferral, not failure or terminalization.
- Do not add `CrawlRecoveryPhase::Extracting`, `Validating`, or any speculative extraction phase/state.
- Do not modify `erabi-extraction/**` or implement any Plan 07 selector, normalization, validation, schema-drift, Dataset, Record, review, candidate, or provenance behavior.
- Do not redesign `SemanticTraversal` (DX-D06) or crawler repository ownership/transactions (DX-D07).
- Do not change provider/network/robots/pacing behavior, primary CrawlRun/Job statuses, progress meaning/order, or unrelated API contracts.
- Do not create or modify executable migrations. `0007` remains Plan 07, `0008` remains Plan 08, and `0009` remains unallocated.
- Do not modify `migrations/0004_jobs.sql`; `job_checkpoints.checkpoint_json` already permits the pre-release JSON contract reset.
- Do not add a new dependency unless an implementation-time compiler requirement proves it unavoidable; `serde_json` already exists in all affected crates.
- Do not introduce a new testing architecture. Reuse current crate-private tests and existing integration-test files.
- Use `-j 1` for heavy Cargo commands on Windows when useful to avoid pagefile pressure.
- Do not use `cargo clean`.
- During implementation, do **not** commit, push, create a PR, or merge. Keep one implementation candidate uncommitted until independent Terra HIGH review returns `BLOCKER: 0` and `IMPORTANT: 0`.
- If fresh Terra review runs the complete final gate on the exact candidate and no production code changes afterward, do not rerun the heavy gate merely for ceremony.

---

## Expected File Map

Primary implementation files:

```text
crates/erabi-db/src/repositories/checkpoint.rs
    Minimal generic envelope, payload-kind newtype, precise envelope format errors,
    append/read semantics, and private stale-envelope assessment.

crates/erabi-db/src/repositories/job.rs
    Stale-job recovery wording/counters where the old generic "recoverable"
    classification overclaims plan-specific recovery safety.

crates/erabi-db/src/repositories/mod.rs
    Remove obsolete checkpoint exports; export only current generic transport API.

crates/erabi-crawler/src/checkpoint.rs
    Canonical CrawlRecoveryCheckpoint, CrawlRecoveryPhase, format/payload-kind
    validation, compact-size bound, legacy-model removal.

crates/erabi-crawler/src/finalization.rs
    CrawlStructuralFacts reconstruction solely from durable data-plane evidence;
    remove legacy checkpoint-driven finalization compatibility surface.

crates/erabi-crawler/src/lib.rs
    Replace checkpoint glob exposure with explicit current recovery exports where useful.

crates/erabi-jobs/src/actions.rs
    One same-run recovery validation path for Resume/Retry/Retry Failed Parts and
    precise JobActionError taxonomy.

crates/erabi-jobs/src/quick_scrape.rs
    Canonical checkpoint construction/validation while preserving Quick Scrape lifecycle.

crates/erabi-jobs/src/production/mod.rs
crates/erabi-jobs/src/production/crawl_stage.rs
crates/erabi-jobs/src/production/finalization.rs
    Canonical Production checkpoint handling and durable-state recovery/finalization.

crates/erabi-jobs/src/lib.rs
    Remove obsolete checkpoint re-exports; keep only legitimately current public surface.

crates/erabi-api/src/job_actions.rs
    Five approved recovery error codes, all conflict-safe and payload-redacted.

crates/erabi-jobs/tests/actions.rs
crates/erabi-jobs/tests/durable_queue.rs
crates/erabi-jobs/tests/production.rs
crates/erabi-jobs/tests/quick_scrape.rs
crates/erabi-jobs/tests/storage_pressure.rs
crates/erabi-api/tests/job_actions.rs
crates/erabi-api/tests/openapi_contract.rs
    Existing regression surfaces updated only where D03 semantics require it.

docs/roadmap/04-engineering-dx.md
    Keep DX-D03 CURRENT during implementation and link approved design/plan.
```

Files expected to remain unchanged unless a mechanical import cleanup is proven necessary:

```text
crates/erabi-extraction/**
crates/erabi-domain/**
crates/erabi-crawl4ai/**
migrations/*.sql
migrations/README.md
Cargo.lock
```

If implementation discovers a material need to change one of those expected-unchanged areas, stop and reconcile the design rather than silently expanding scope.

---

## Task 1: Replace the Generic DB Checkpoint Envelope

**Files:**
- Modify: `crates/erabi-db/src/repositories/checkpoint.rs`
- Modify: `crates/erabi-db/src/repositories/job.rs`
- Modify: `crates/erabi-db/src/repositories/mod.rs`
- Tests: existing private tests in `checkpoint.rs`, existing DB/job repository tests as needed

**Target current generic API:**

```rust
pub const CHECKPOINT_ENVELOPE_FORMAT_VERSION: u16 = 1;
pub const MAX_CHECKPOINT_BYTES: usize = 64 * 1024;
pub const MAX_CHECKPOINT_PAYLOAD_KIND_BYTES: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct CheckpointPayloadKind(String);

pub struct CheckpointEnvelope {
    pub format_version: u16,
    pub sequence: u64,
    pub identity: CheckpointIdentity,
    pub payload_kind: CheckpointPayloadKind,
    pub payload: serde_json::Value,
}
```

Preferred constructor shape:

```rust
pub fn new(
    identity: CheckpointIdentity,
    payload_kind: CheckpointPayloadKind,
    payload: serde_json::Value,
) -> Result<Self, CheckpointRepositoryError>;
```

The constructor must initialize `sequence = 0`, require a JSON object payload, validate the kind and identity, and create only the current envelope format.

- [ ] **Step 1: Implement `CheckpointPayloadKind` and the minimal envelope.**

`CheckpointPayloadKind::new(...)` must reject empty/oversized/unstable identifiers. Use the repository's established stable-identifier convention: upper-case ASCII letters, ASCII digits, and underscore only, with a maximum of 64 bytes. `as_str()` returns the validated inner value.

Remove from `CheckpointEnvelope`:

```text
completed_units
pending_units
failed_units
discovery_position
artifact_references
extraction
payload: Option<String>
```

Replace the payload with a required `serde_json::Value` object. Null, string, number, boolean, or array payloads are invalid generic envelopes.

Remove obsolete generic checkpoint types/constants that exist only for those deleted fields:

```text
CheckpointUnitId
CheckpointPosition
CheckpointArtifactReference
ExtractionResumePhase
ExtractionResumeState
MAX_CHECKPOINT_UNITS
MAX_CHECKPOINT_ARTIFACTS
associated unit/position/artifact-only bounds
```

Do not rename these into "legacy" production types.

- [ ] **Step 2: Make envelope decoding distinguish old/unsupported format from malformed current data.**

Refactor decoding conceptually as:

```text
encoded byte bound
→ parse JSON value
→ require object
→ inspect format_version
→ unsupported/missing historical schema marker classification
→ deserialize current CheckpointEnvelope
→ validate current invariants
```

Use this error vocabulary:

```rust
pub enum CheckpointRepositoryError {
    Database(DbError),
    UnsupportedFormatVersion,
    InvalidEnvelope,
    PayloadTooLarge,
    Malformed,
    Serialization,
    NotFound,
    LeaseLost,
}
```

Rules:

- invalid JSON / non-object top-level / undecodable current structure → `Malformed`;
- old recognized `schema_version` envelope or numeric `format_version != 1` → `UnsupportedFormatVersion`;
- current-format structure that decodes but violates identity/kind/object/sequence invariants → `InvalidEnvelope`;
- remove broad `Inconsistent` when the current-format failure can be stated precisely;
- never include raw JSON in Display/debug-facing error messages.

`encode()` must reject a non-current `format_version` with `UnsupportedFormatVersion`, not silently rewrite it.

- [ ] **Step 3: Preserve repository-assigned monotonic sequence and append-only ownership.**

Keep the existing transaction ordering and lease/attempt checks. The caller supplies a logical envelope with sequence zero; the repository-owned append path assigns the next trusted sequence to the persisted copy exactly as today.

Do not weaken:

```text
active lease ownership
attempt lineage
append commit before resumability
monotonic per-job sequence
64 KiB encoded ceiling
records/latest chronological validation
```

No SQL changes are permitted.

- [ ] **Step 4: Stop generic DB code from overclaiming full recovery safety.**

The current `CheckpointRecoveryDisposition::Recoverable` is based only on generic envelope + immutable identity and therefore overstates what `erabi-db` knows.

Remove the public `CheckpointCompatibility`, `CheckpointRecoveryDisposition`, and `CheckpointRecoveryAssessment` surface if no legitimate external production caller remains after D03. Keep stale-job classification private to the repository boundary with semantics equivalent to:

```rust
enum CheckpointEnvelopeDisposition {
    ResumeCandidate,
    RestartRequired,
    Invalid,
}
```

`ResumeCandidate` means only: current generic envelope and immutable identity are compatible enough to requeue as a candidate. It is **not** authorization to execute crawl recovery.

Rename stale-recovery counters that expose the old overclaiming vocabulary:

```text
recoverable       → resume_candidates
unsafe_checkpoints → invalid_checkpoints
```

Preserve `requeued`, `failed`, and `restart_required` meaning. Actual crawl work still performs crawler-specific Stage A and durable Stage B validation in Task 4.

- [ ] **Step 5: Clean `erabi-db` exports.**

`crates/erabi-db/src/repositories/mod.rs` should export only the current generic checkpoint transport concepts needed cross-crate, including:

```text
CHECKPOINT_ENVELOPE_FORMAT_VERSION
MAX_CHECKPOINT_BYTES
CheckpointPayloadKind
CheckpointEnvelope
CheckpointIdentity
CheckpointRecord
CheckpointRepository
CheckpointRepositoryError
```

Do not re-export deleted legacy unit/extraction types or private stale-envelope assessment types.

- [ ] **Step 6: Add/update DB tests after implementation.**

Cover at least:

```text
current envelope structured-object round trip
transparent payload-kind JSON string
empty/oversized/invalid payload kind rejection
null/non-object payload rejection
unsupported format distinct from malformed JSON
recognized old schema_version shape → UnsupportedFormatVersion
invalid current identity → InvalidEnvelope
encoded >64 KiB → PayloadTooLarge
repository sequence remains monotonic and caller input is not authority
append requires current lease/attempt
old/malformed persisted evidence is never a ResumeCandidate
identity-compatible envelope is only a ResumeCandidate
```

Tests for raw historical JSON should live where private decode access is available; do not preserve a public legacy decoder for testing.

- [ ] **Step 7: Run Task 1 focused verification.**

```powershell
cargo test -p erabi-db -j 1
cargo check -p erabi-db --all-targets -j 1
cargo fmt --all --check
```

Inspect:

```powershell
git diff -- crates/erabi-db/src/repositories/checkpoint.rs `
            crates/erabi-db/src/repositories/job.rs `
            crates/erabi-db/src/repositories/mod.rs
```

**Task 1 review gate:** reject if `erabi-db` still knows crawl/extraction semantics, if old unit/extraction fields survive only for compatibility, if malformed vs unsupported is collapsed, if sequence becomes caller-owned, or if append/lease semantics change.

---

## Task 2: Replace Historical Crawler Checkpoints with `CrawlRecoveryCheckpoint`

**Files:**
- Rewrite/modify: `crates/erabi-crawler/src/checkpoint.rs`
- Modify: `crates/erabi-crawler/src/lib.rs`
- Tests: focused private checkpoint tests in `checkpoint.rs`

**Target API:**

```rust
pub const CRAWL_RECOVERY_FORMAT_VERSION: u16 = 1;
pub const CRAWL_RECOVERY_PAYLOAD_KIND: &str = "CRAWL_RECOVERY";
pub const MAX_CRAWL_RECOVERY_PAYLOAD_BYTES: usize = 1_024;

pub struct CrawlRecoveryCheckpoint {
    pub format_version: u16,
    pub crawl_run_id: CrawlRunId,
    pub run_type: CrawlRunType,
    pub snapshot_hash: String,
    pub checkpoint_compatibility_hash: String,
    pub crawler_version_id: Option<CrawlerVersionId>,
    pub semantic_config_hash: Option<String>,
    pub recovery_phase: CrawlRecoveryPhase,
}

pub enum CrawlRecoveryPhase {
    Initialized,
    Traversing,
    Finalizing,
}

pub enum CrawlRecoveryCheckpointError {
    UnsupportedFormatVersion,
    UnexpectedPayloadKind,
    MalformedPayload,
    IncompatibleRunIdentity,
    UnsupportedRunType,
    Serialization,
}
```

- [ ] **Step 1: Implement the canonical compact recovery type from the current semantics.**

Preserve only the legitimate compact identity/phase fields from current `CrawlCheckpointV2`. Reset serialized field `payload_version` to semantic `format_version = 1`.

`new(...)` must derive crawler-version/semantic-config identity from the immutable snapshot and support only:

```text
PRODUCTION_RUN
QUICK_SCRAPE
```

Test/Discovery run types are not resumable through this crawl recovery contract.

- [ ] **Step 2: Implement structured `to_envelope` / `from_envelope`.**

`to_envelope()`:

```text
validate against immutable snapshot
→ serde_json::to_value(self)
→ require serialized payload <= 1 KiB
→ construct CheckpointIdentity
→ construct CheckpointPayloadKind("CRAWL_RECOVERY")
→ construct current CheckpointEnvelope
```

`from_envelope()`:

```text
payload_kind must equal CRAWL_RECOVERY
→ payload must be object
→ inspect recovery format_version
→ deserialize CrawlRecoveryCheckpoint
→ require payload <= 1 KiB
→ verify crawl_run_id + run_type + snapshot hash + compatibility hash
→ verify CrawlerVersion/semantic_config identity or Quick Scrape None/None
```

Classification rules:

- wrong kind → `UnexpectedPayloadKind`;
- numeric recovery format other than 1 → `UnsupportedFormatVersion`;
- missing/wrong-shaped fields → `MalformedPayload`;
- run/snapshot/config mismatch → `IncompatibleRunIdentity`;
- wrong run type → `UnsupportedRunType`.

Do not inspect generic legacy unit fields because they no longer exist.

- [ ] **Step 3: Delete the legacy growing checkpoint implementation.**

Remove production code for:

```text
CrawlCheckpoint
CrawlCheckpointV2
CrawlCheckpointUnit
CrawlCheckpointUnitState
CheckpointUnitSets
CRAWL_CHECKPOINT_PAYLOAD_VERSION
CRAWL_CHECKPOINT_V2_PAYLOAD_VERSION
MAX_CRAWL_CHECKPOINT_V2_PAYLOAD_BYTES
legacy unit reconstruction/merge/carry-forward helpers
legacy opaque-unit projection helpers
legacy traversal-sized payload validation
```

Also remove now-unused imports such as `SemanticTraversalCheckpoint`, checkpoint unit/artifact types, and collections used only by the old model.

No aliases, deprecated wrappers, or `Legacy*` production models.

- [ ] **Step 4: Replace checkpoint glob export with explicit current exports.**

In `erabi-crawler/src/lib.rs`, do not expose historical checkpoint internals via `pub use checkpoint::*`. Export the current recovery contract explicitly, for example:

```rust
pub use checkpoint::{
    CRAWL_RECOVERY_FORMAT_VERSION,
    CRAWL_RECOVERY_PAYLOAD_KIND,
    MAX_CRAWL_RECOVERY_PAYLOAD_BYTES,
    CrawlRecoveryCheckpoint,
    CrawlRecoveryCheckpointError,
    CrawlRecoveryPhase,
};
```

Keep other module exports outside D03 unchanged.

- [ ] **Step 5: Add canonical crawler recovery tests after implementation.**

Cover:

```text
Production round trip
Quick Scrape round trip
payload kind is exactly CRAWL_RECOVERY
wrong payload kind
unsupported recovery format
malformed structured payload
run ID mismatch
snapshot hash mismatch
compatibility hash mismatch
CrawlerVersion mismatch
semantic config mismatch
Quick Scrape requires None/None crawler identity
unsupported Test/Discovery type
payload remains <=1 KiB independent of crawl frontier cardinality
historical payload_version/schema shape is not interpreted as current
```

- [ ] **Step 6: Run Task 2 focused verification.**

```powershell
cargo test -p erabi-crawler -j 1
cargo check -p erabi-crawler --all-targets -j 1
cargo fmt --all --check
```

Also inspect dead historical symbols in crawler production source:

```powershell
rg -n "CrawlCheckpointV2|CrawlCheckpointUnit|CRAWL_CHECKPOINT_PAYLOAD_VERSION|CRAWL_CHECKPOINT_V2_PAYLOAD_VERSION|MAX_CRAWL_CHECKPOINT_V2_PAYLOAD_BYTES" crates/erabi-crawler/src
```

Expected: no old production symbols. Raw historical field names may appear only inside explicit rejection test fixtures.

**Task 2 review gate:** reject if the new type retains `V2`/`payload_version` vocabulary, if checkpoint content grows with frontier size, if crawler owns durable progress partitions, or if a compatibility decoder remains.

---

## Task 3: Make Crawl Structural Facts Independent of Serialized Checkpoints

**Files:**
- Modify: `crates/erabi-crawler/src/finalization.rs`
- Modify only as mechanically required: `crates/erabi-crawler/src/lib.rs`
- Tests: existing private finalization tests

**Canonical interface:**

```rust
pub fn reconstruct_crawl_structural_facts(
    snapshot: &CrawlRunSnapshot,
    current_status: CrawlRunStatus,
    executions: &[CrawlExecutionRecord],
    discovered_urls: &[DiscoveredUrlRecord],
    control: Option<&CrawlTraversalControl>,
    work: Option<&[CrawlUrlStateRecord]>,
) -> Result<CrawlStructuralFacts, CrawlStructuralFactsError>;

pub enum CrawlStructuralFactsError {
    InconsistentDurableState,
}
```

`control` / `work` may remain optional only to support legitimate current non-traversal/fallback durable evidence paths. A serialized checkpoint is no longer a fallback authority.

- [ ] **Step 1: Remove `CrawlCheckpoint` from structural reconstruction.**

Delete the checkpoint parameter and every fallback branch that reads:

```text
checkpoint crawl_run_id
checkpoint completed/pending/failed/partial units
checkpoint traversal pagination_truncation_count
checkpoint traversal duration_work_not_expanded
```

Reconstruct current facts only from durable sources:

```text
executions
discovered_urls
CrawlTraversalControl when present
CrawlUrlStateRecord logical work when present
```

Preserve these accepted semantics:

- logical work supersedes historical attempt outcomes when present;
- Partial work contributes completed + unresolved;
- pagination truncation is not double-counted as generic duration work;
- Page Type ambiguity remains durable evidence;
- terminal Cancelled/Failed status remains terminal;
- Quick Scrape current logical-work failure/cancel status remains respected;
- completed count cannot exceed planned count;
- cross-run durable evidence fails closed.

- [ ] **Step 2: Rename the error to match the surviving responsibility.**

After legacy complete-snapshot wrappers are removed, `CrawlFinalizationError` is misleading. Use `CrawlStructuralFactsError::InconsistentDurableState` for incoherent crawl evidence.

Do not keep complete-snapshot/domain-decision errors in this crawler read-side function.

- [ ] **Step 3: Remove historical checkpoint-driven finalization wrappers.**

Delete `CrawlFinalization` and public functions whose reason to exist is the old growing checkpoint compatibility surface, including the historical:

```text
finalize_durable_state
finalize_durable_state_with_control
finalize_durable_state_with_traversal
```

provided call-site inspection confirms there is no current non-legacy production consumer. D05 preserved these only because D05 explicitly did not own checkpoint compatibility; D03 now owns their retirement.

Do not replace them with deprecated wrappers.

- [ ] **Step 4: Update tests after implementation.**

Prove:

```text
complete coherent durable work
historical failure followed by durable success stays completed
partial logical work remains unresolved
pagination truncation comes from CrawlTraversalControl
ambiguity comes from durable work/discovery evidence
cross-run/incoherent evidence fails InconsistentDurableState
facts can be reconstructed with no checkpoint representation
```

Remove tests that exist only to prove legacy checkpoint wrapper behavior.

- [ ] **Step 5: Run Task 3 focused verification.**

```powershell
cargo test -p erabi-crawler -j 1
cargo check -p erabi-crawler --all-targets -j 1
cargo fmt --all --check
```

Inspect:

```powershell
rg -n "CrawlFinalization|finalize_durable_state|CrawlCheckpoint" crates/erabi-crawler/src
```

Expected: no legacy finalization/checkpoint compatibility surface remains; current `CrawlStructuralFacts` remains public.

**Task 3 review gate:** reject if structural truth depends on checkpoint bytes, if counting/status semantics drift, or if complete-snapshot/extraction-health ownership moves back into crawler.

---

## Task 4: Unify Jobs Runtime and Action Recovery on the Canonical Contract

**Files:**
- Modify: `crates/erabi-jobs/src/actions.rs`
- Modify: `crates/erabi-jobs/src/quick_scrape.rs`
- Modify: `crates/erabi-jobs/src/production/mod.rs`
- Modify: `crates/erabi-jobs/src/production/crawl_stage.rs`
- Modify: `crates/erabi-jobs/src/production/finalization.rs`
- Modify: `crates/erabi-jobs/src/lib.rs`
- Optional focused private module if it reduces duplicate mapping without creating a framework: `crates/erabi-jobs/src/recovery.rs`
- Tests: `crates/erabi-jobs/tests/actions.rs`, `durable_queue.rs`, `production.rs`, `quick_scrape.rs`, `storage_pressure.rs`

**Canonical jobs action errors:**

```rust
pub enum JobActionError {
    // existing unrelated variants ...
    CheckpointMissing,
    CheckpointFormatUnsupported,
    CheckpointMalformed,
    CheckpointIdentityMismatch,
    RecoveryStateInvalid,
    // existing repository/cancellation variants ...
}
```

Remove `CheckpointUnsafe` and `CheckpointIncompatible`.

- [ ] **Step 1: Clean jobs checkpoint re-exports and generic queue fixtures.**

`erabi-jobs/src/lib.rs` must stop re-exporting deleted generic checkpoint unit/extraction APIs. Re-export only current generic transport concepts if jobs consumers actually need them.

Update `tests/durable_queue.rs` generic checkpoint fixture to construct the new envelope with a stable test payload kind and JSON object. Tests that previously made a checkpoint invalid by duplicating completed/pending units should now corrupt current envelope invariants intentionally, e.g. unsupported format, invalid kind, or non-object/raw persisted fixture through an appropriate repository-private test surface.

Do not recreate fake generic work-unit fields in the new envelope just to keep old tests recognizable.

- [ ] **Step 2: Replace `compatible_checkpoint()` with one current crawl recovery path.**

Remove all manual JSON branching such as:

```text
read payload_version
if 2 → CrawlCheckpointV2
else → CrawlCheckpoint
```

The jobs action path should always:

```text
load source CrawlRun snapshot
→ load latest checkpoint for lineage
→ validate current generic envelope result/error
→ CrawlRecoveryCheckpoint::from_envelope(...)
→ reconstruct durable crawl recovery state
→ return validated same-run recovery context
```

A small private helper/module is encouraged if it prevents `actions.rs`, Quick Scrape, and Production from each inventing their own mapping. Keep it concrete to crawl recovery; do not create a generic multi-stage recovery framework for future Plan 07.

Map failures exactly:

```text
no checkpoint → CheckpointMissing
unsupported envelope/recovery version or wrong payload kind → CheckpointFormatUnsupported
malformed/invalid current envelope or malformed crawl payload → CheckpointMalformed
run/snapshot/config identity mismatch → CheckpointIdentityMismatch
durable traversal/logical-work contradiction → RecoveryStateInvalid
repository I/O/lease failures → existing Repository(...) path
```

- [ ] **Step 3: Make failed-part selection durable-data-only.**

`Retry Failed Parts` must count/select:

```rust
Some(CrawlWorkState::Failed | CrawlWorkState::Partial)
```

from reconstructed current logical work. Completed current-generation work is never reintroduced because an old execution failed.

Remove all test helpers that manufacture failed parts through `CrawlCheckpointUnit` arrays. Persist/reconstruct the same logical-work evidence production code actually uses.

- [ ] **Step 4: Apply the canonical checkpoint to Quick Scrape runtime.**

Replace all `CrawlCheckpointV2` construction/validation with `CrawlRecoveryCheckpoint`.

Preserve:

- same frozen Quick Scrape snapshot/Source validation;
- retry generation mechanics;
- durable completed work winning after a crash;
- explicit `RESTART_FROM_BEGINNING` skipping old checkpoint evidence;
- storage-pressure deferral;
- cancellation and terminal reconciliation;
- provider/network/robots/pacing ordering.

For same-run Resume/Retry/Retry Failed Parts, a checkpoint accepted for execution must be current-format and identity-compatible before provider work. If the operator intentionally wants to start a failed Quick Scrape over without safe recovery state, `Restart from Beginning` is the explicit action.

- [ ] **Step 5: Apply the canonical checkpoint to Production crawl stage/finalization.**

Replace `CrawlCheckpointV2` with `CrawlRecoveryCheckpoint` in:

```text
initial recovery validation
action-marker checkpoint persistence
ordinary crawl-stage checkpoint saves
finalization checkpoint validation
```

Preserve D05 semantics exactly:

```text
no compatible recovery frontier + incomplete durable evidence → fail closed
execution rows committed before a crash win over stale checkpoint timing
Retry/Retry Failed Parts prepare durable generation selection safely
storage pressure → DeferredNoPostCrawl
cancellation before handoff → no post-crawl work
post-crawl handoff remains bounded and body/credential-free
```

Update the finalization call to the Task 3 signature with no checkpoint argument.

Do not change `ReadyForPostCrawl`, page execution, provider policy, or extraction-health composition.

- [ ] **Step 6: Ensure automatic stale-job recovery is only a candidate until handler validation.**

The generic DB recovery from Task 1 may requeue a run-backed stale job because its current envelope/identity is a `ResumeCandidate`. Before the resumed handler performs crawl work, it must pass the same `CrawlRecoveryCheckpoint::from_envelope(...)` validation used by explicit Resume, followed by durable-state reconstruction.

Do not introduce a second looser automatic decoder. A stale job with an unsupported/malformed crawl payload may be generically requeued as a candidate only if the generic envelope itself is valid; the handler must reject it before provider work and record the precise checkpoint diagnostic.

- [ ] **Step 7: Make runtime diagnostics match the new taxonomy.**

Where the runtime currently collapses failures into `CHECKPOINT_INVALID`, use safe bounded diagnostic codes matching the semantic reason where available:

```text
CHECKPOINT_FORMAT_UNSUPPORTED
CHECKPOINT_MALFORMED
CHECKPOINT_IDENTITY_MISMATCH
RECOVERY_STATE_INVALID
```

Do not include raw payload/URL/provider bodies in diagnostics or telemetry.

- [ ] **Step 8: Update jobs tests after implementation.**

Cover at minimum:

```text
Resume requires current canonical checkpoint
old/unsupported checkpoint does not Resume
malformed checkpoint does not Resume
identity mismatch does not Resume
valid checkpoint + contradictory durable recovery state is rejected
Retry cannot bypass required crawl recovery validation
Retry Failed Parts derives count from durable Failed/Partial work
completed current work wins over historical failure
invalid Resume never silently enqueues Restart/Rerun
Quick Scrape explicit Restart remains legal
Production Restart from Beginning remains illegal
Production Rerun Full Crawl creates a new run identity
storage pressure remains resumable deferral
stale generic resume candidate still passes crawler validation before provider work
```

- [ ] **Step 9: Run Task 4 focused verification.**

```powershell
cargo test -p erabi-jobs --test actions -j 1
cargo test -p erabi-jobs --test durable_queue -j 1
cargo test -p erabi-jobs --test production -j 1
cargo test -p erabi-jobs --test quick_scrape -j 1
cargo test -p erabi-jobs --test storage_pressure -j 1
cargo test -p erabi-jobs -j 1
cargo check -p erabi-jobs --all-targets -j 1
cargo fmt --all --check
```

If a named integration test target differs from the current Cargo auto-discovery, use the repository's actual test target and record the substitution; do not invent a new test architecture.

**Task 4 review gate:** reject if any action/manual runtime path still chooses a decoder by historical version, if failed parts come from checkpoint arrays, if invalid resume restarts implicitly, or if Production/Quick lifecycle semantics drift.

---

## Task 5: Publish the Precise API Contract and Run the Full D03 Gate

**Files:**
- Modify: `crates/erabi-api/src/job_actions.rs`
- Modify: `crates/erabi-api/tests/job_actions.rs`
- Modify only if generated-contract regression requires it: `crates/erabi-api/tests/openapi_contract.rs`
- Modify: `docs/roadmap/04-engineering-dx.md`
- Audit all D03-touched crate tests/source for dead legacy vocabulary

- [ ] **Step 1: Replace ambiguous API recovery errors with the approved five-code contract.**

Map `JobActionError` to HTTP `409 Conflict` exactly:

```text
CheckpointMissing
→ CHECKPOINT_MISSING

CheckpointFormatUnsupported
→ CHECKPOINT_FORMAT_UNSUPPORTED

CheckpointMalformed
→ CHECKPOINT_MALFORMED

CheckpointIdentityMismatch
→ CHECKPOINT_IDENTITY_MISMATCH

RecoveryStateInvalid
→ RECOVERY_STATE_INVALID
```

Keep unrelated status mappings unchanged (`400`, `404`, `503`, etc.).

Messages must be actionable but bounded and content-free. They may explain that Resume cannot proceed and that a fresh explicit action may be required, but must not expose checkpoint JSON, raw URLs, hashes, provider data, filesystem paths, SQL, or credentials.

- [ ] **Step 2: Add API mapping and leakage tests after implementation.**

Use private unit tests in `job_actions.rs` where direct typed-error mapping is the clearest proof, plus integration tests for representative real action states.

Prove all five codes use 409 and response bodies contain no checkpoint/raw payload field. Preserve bearer-auth behavior and existing action routes.

Because route/status declarations do not change, generated OpenAPI should remain structurally equivalent. Keep the existing OpenAPI parity test green; do not add five bespoke response schemas when all use the established `ApiErrorEnvelope`.

- [ ] **Step 3: Reconcile roadmap documentation without changing product order.**

Keep DX-D03 `CURRENT` during implementation and add links to the approved design and implementation plan. Do not mark `MERGED` until the implementation PR actually merges.

Do not alter Plan 07 product semantics in this task.

- [ ] **Step 4: Audit removal of historical production vocabulary.**

Run source-only searches such as:

```powershell
rg -n "CURRENT_CHECKPOINT_SCHEMA_VERSION|CrawlCheckpointV2|CrawlCheckpointUnit|CRAWL_CHECKPOINT_PAYLOAD_VERSION|CRAWL_CHECKPOINT_V2_PAYLOAD_VERSION|MAX_CRAWL_CHECKPOINT_V2_PAYLOAD_BYTES|ExtractionResumeState|ExtractionResumePhase|CheckpointArtifactReference|CheckpointPosition|CheckpointUnitId" crates
```

Expected: no obsolete production references. A literal old field/type name is allowed only in an explicit historical-format rejection fixture/comment that cannot be mistaken for a supported API.

Also verify the new vocabulary is authoritative:

```powershell
rg -n "CHECKPOINT_ENVELOPE_FORMAT_VERSION|CheckpointPayloadKind|CrawlRecoveryCheckpoint|CRAWL_RECOVERY_FORMAT_VERSION|CRAWL_RECOVERY" crates
```

- [ ] **Step 5: Prove there was no migration/dependency scope drift.**

```powershell
git diff -- migrations

git diff -- Cargo.lock
```

Expected: both empty for D03 implementation.

Confirm `migrations/README.md` still says:

```text
0007 → Plan 07
0008 → Plan 08
0009 → next unallocated
```

- [ ] **Step 6: Run the complete fresh D03 verification gate.**

Focused crate tests:

```powershell
cargo test -p erabi-db -j 1
cargo test -p erabi-crawler -j 1
cargo test -p erabi-jobs -j 1
cargo test -p erabi-api -j 1
```

Runtime integration:

```powershell
cargo test -p erabi --lib -j 1
cargo test -p erabi --test runtime_server -j 1
```

Compilation:

```powershell
cargo check -p erabi-db --all-targets -j 1
cargo check -p erabi-crawler --all-targets -j 1
cargo check -p erabi-jobs --all-targets -j 1
cargo check -p erabi-api --all-targets -j 1
cargo check -p erabi --all-targets -j 1
```

Quality:

```powershell
cargo fmt --all --check

cargo clippy `
  -p erabi-db `
  -p erabi-crawler `
  -p erabi-jobs `
  -p erabi-api `
  -p erabi `
  --all-targets -j 1 -- -D warnings

git diff --check
```

All commands must be fresh on the final candidate. Record exact pass/fail output summary; do not claim `VERIFIED` from partial earlier task gates.

- [ ] **Step 7: Produce the implementation report for independent review.**

Report:

```text
baseline main SHA
current implementation branch
changed files by responsibility
removed legacy public types/constants
new envelope/crawler format contracts
recovery/action behavior preserved or intentionally changed by approved D03 design
migration/Cargo.lock status
focused + full verification results
known minor observations, if any
```

Do not commit yet.

---

## Independent Review and Closure Gate

After Tasks 1–5 are implemented and the final fresh gate passes:

1. Open/use the dedicated Terra HIGH reviewer chat for DX-D03.
2. Review the exact uncommitted candidate against:
   - approved design spec;
   - this implementation plan;
   - actual `main` baseline;
   - canonical product specs/AGENTS rules;
   - scope and migration invariants.
3. Terra must independently inspect behavior, not merely trust the implementation report.
4. Required acceptance result:

```text
BLOCKER: 0
IMPORTANT: 0
```

5. If Terra finds issues, return to the same implementation chat for remediation; then re-review in the same Terra chat.
6. Do not commit/push/open PR until the review gate is clean.
7. After clean review, run only any verification required by actual post-review code changes. If no code changed after Terra's fresh full gate, do not rerun the heavy gate unnecessarily.
8. Then create the implementation commit/PR using the accepted candidate.
9. After merge, use a tiny docs-only lifecycle closure if needed to mark DX-D03 `MERGED`, following the D04/D05 precedent.

State transitions remain explicit:

```text
IMPLEMENTED
    ↓ fresh complete gate
VERIFIED
    ↓ Terra BLOCKER 0 / IMPORTANT 0
ACCEPTED
    ↓ implementation PR merged
MERGED
```

Do not begin Plan 07 until DX-D03 reaches the agreed closure state and the Plan 07 readiness refresh has been performed.

---

## Final Acceptance Checklist

DX-D03 is acceptable only if all are true:

- [ ] `CheckpointEnvelope` is minimal generic transport with `format_version`, repository-owned sequence, identity, payload kind, and structured object payload.
- [ ] Generic envelope no longer contains crawl units, discovery position, artifact references, or extraction state.
- [ ] Old generic unit/extraction checkpoint types/constants are gone from supported public API.
- [ ] Unsupported envelope format is distinct from malformed current JSON.
- [ ] Generic stale recovery uses resume-candidate language and does not claim crawler-specific safety.
- [ ] `CrawlRecoveryCheckpoint` is the sole current crawl checkpoint type.
- [ ] Crawl recovery uses `format_version = 1`, payload kind `CRAWL_RECOVERY`, and a 1 KiB payload ceiling.
- [ ] No `CrawlCheckpoint`, `CrawlCheckpointV2`, unit-array legacy model, aliases, or dual decoders remain.
- [ ] `CrawlStructuralFacts` reconstruction has no serialized checkpoint dependency.
- [ ] Structural facts come from durable executions/discovery/control/work only.
- [ ] Resume, Retry, Retry Failed Parts, and recovered runtime all use the current crawl checkpoint validation authority before crawl work.
- [ ] Failed-part selection comes from durable logical work.
- [ ] Invalid recovery never silently restarts.
- [ ] Quick Scrape explicit Restart semantics remain valid.
- [ ] Production same-run restart remains illegal; Rerun Full Crawl remains independent.
- [ ] Storage pressure remains resumable deferral.
- [ ] API exposes exactly the five approved recovery-conflict codes with HTTP 409.
- [ ] Raw checkpoint/provider/credential content never leaks through errors or telemetry.
- [ ] No SQL migration or migration-number allocation change exists.
- [ ] No Plan 07/D06/D07 implementation leaked into D03.
- [ ] Final Cargo test/check/fmt/clippy/diff gate passes fresh.
- [ ] Terra independent review returns `BLOCKER: 0`, `IMPORTANT: 0` before implementation commit/PR.
