# DX-D03 — Canonical Checkpoint and Recovery Contract Design

**Status:** Approved design  
**Date:** 2026-09-10  
**Package:** DX-D03

## 1. Purpose

DX-D03 establishes the canonical pre-release checkpoint/recovery contract before Plan 07 introduces extraction recovery requirements.

The current code has three overlapping historical version concepts:

- generic `CheckpointEnvelope` with `CURRENT_CHECKPOINT_SCHEMA_VERSION = 1`;
- legacy growing `CrawlCheckpoint` with `CRAWL_CHECKPOINT_PAYLOAD_VERSION = 1`;
- current compact `CrawlCheckpointV2` with `CRAWL_CHECKPOINT_V2_PAYLOAD_VERSION = 2`.

Production and Quick Scrape now use the compact path, while durable crawl progress/frontier truth lives in the migration `0006` data plane. The generic envelope nevertheless still carries old crawl-unit fields, artifact/discovery state, and speculative extraction state. DX-D03 removes that historical coupling before it becomes permanent architecture.

## 2. Pre-release compatibility policy

Erabi has not shipped a public release. Internal development Rust APIs, checkpoint JSON shapes, and old recovery rows are therefore **not compatibility contracts**.

> While Erabi remains pre-release, prefer the cleanest correct architecture, idiomatic Rust naming, and explicit ownership over compatibility with development-only state.

Breaking changes are allowed when they improve the canonical design. Old checkpoint formats may be removed outright. No compatibility alias, legacy decoder, converter, or persisted-state rewrite is required for development-only recovery state.

This policy does not make durable business data generally disposable. It is specific to pre-release operational recovery state and accidental internal API surface.

All incompatible or malformed old state must still fail closed; it must never be silently reinterpreted using new semantics.

## 3. Goals and non-goals

DX-D03 must:

1. make checkpoint terminology unambiguous and idiomatic;
2. separate generic persistence from crawl-specific recovery semantics;
3. remove the legacy growing crawl checkpoint model from the canonical runtime;
4. keep crawl recovery control fixed-cardinality and bounded;
5. make durable crawl data the sole authority for crawl progress truth;
6. distinguish format, corruption, immutable-identity, and durable-state failures;
7. make explicit Resume and automatic crash recovery use one interpretation;
8. remove speculative extraction recovery ownership from `erabi-db`;
9. leave Plan 07 free to design extraction recovery from actual requirements;
10. change checkpoint JSON without consuming a SQL migration number.

DX-D03 does **not** implement or redesign:

- Plan 07 extraction/curation/validation/schema-drift/Dataset/Record/provenance behavior;
- extraction recovery semantics;
- migrations `0007`, `0008`, or `0009`;
- DX-D06 `SemanticTraversal` decomposition;
- DX-D07 crawler repository decomposition;
- provider/network/robots/pacing behavior;
- job/run status models;
- storage-pressure policy;
- progress/SSE architecture;
- unrelated API or repository cleanup.

Breaking-change freedom is not permission for unrelated refactoring.

## 4. Ownership

### `erabi-db`

Owns generic checkpoint transport and durability only:

- append-only persistence;
- repository-assigned sequence;
- immutable checkpoint identity;
- generic envelope format validation;
- payload-kind and encoded-size bounds;
- lease/attempt ownership for append;
- latest-checkpoint lineage loading.

It does not know crawl phases, crawl work partitions, extraction phases, or plan-specific payload fields.

### `erabi-crawler`

Owns:

- `CrawlRecoveryCheckpoint`;
- `CrawlRecoveryPhase`;
- crawl recovery format version;
- immutable crawl identity validation;
- crawl-specific compact-size bound;
- conversion to/from the generic envelope.

The crawl recovery checkpoint answers only: **which immutable crawl is this recovery control for, and what crawl recovery phase was durably reached?**

It does not answer which URLs are completed, failed, partial, or pending.

### `erabi-jobs`

Owns recovery workflow semantics:

- when recovery is attempted;
- Resume / Retry / Retry Failed Parts / Restart / Rerun semantics;
- combining checkpoint validity with durable recovery coherence;
- operator-facing recovery failure classification;
- Production vs Quick Scrape recovery policy.

### `erabi-api`

Owns safe HTTP presentation of actionable recovery errors without exposing checkpoint payloads.

### Plan 07

Plan 07 owns future extraction recovery design. DX-D03 must not invent `Extracting`, `Validating`, or replacement extraction resume fields.

## 5. Canonical generic envelope

The generic envelope becomes a minimal bounded persistence container:

```rust
pub const CHECKPOINT_ENVELOPE_FORMAT_VERSION: u16 = 1;

pub struct CheckpointEnvelope {
    pub format_version: u16,
    pub sequence: u64,
    pub identity: CheckpointIdentity,
    pub payload_kind: CheckpointPayloadKind,
    pub payload: serde_json::Value,
}
```

### `CheckpointPayloadKind`

Use a bounded opaque newtype, serialized transparently as a JSON string:

```rust
#[serde(transparent)]
pub struct CheckpointPayloadKind(String);
```

It exposes a validated constructor and `as_str()`. The owning subsystem supplies the semantic value. Crawl recovery uses exactly:

```text
CRAWL_RECOVERY
```

Do not use a generic closed enum that forces `erabi-db` to know all present/future subsystem payload kinds.

### Removed generic envelope concepts

Remove from `CheckpointEnvelope`:

- `completed_units`;
- `pending_units`;
- `failed_units`;
- `discovery_position`;
- `artifact_references`;
- `extraction`.

Remove their supporting generic types/constants when they have no remaining legitimate current responsibility, including the existing `CheckpointUnitId`, `CheckpointPosition`, `CheckpointArtifactReference`, `ExtractionResumePhase`, and `ExtractionResumeState`. Do not retain them only for compatibility. If implementation audit proves a concept still serves a current non-legacy responsibility, re-home/rename it according to that responsibility instead.

### Structured payload

Replace `payload: Option<String>` with a required structured JSON payload. The generic envelope accepts a non-null JSON object and enforces the overall encoded-size ceiling; it does not inspect plan-specific object fields.

Persisted shape:

```json
{
  "format_version": 1,
  "sequence": 12,
  "identity": {
    "snapshot_id": "...",
    "snapshot_hash": "...",
    "compatibility_hash": "..."
  },
  "payload_kind": "CRAWL_RECOVERY",
  "payload": {
    "format_version": 1,
    "crawl_run_id": "...",
    "run_type": "PRODUCTION_RUN",
    "snapshot_hash": "...",
    "checkpoint_compatibility_hash": "...",
    "crawler_version_id": "...",
    "semantic_config_hash": "...",
    "recovery_phase": "TRAVERSING"
  }
}
```

No JSON-string-inside-JSON representation remains.

## 6. Canonical crawl recovery format

Replace historical `CrawlCheckpoint` / `CrawlCheckpointV2` naming with one semantic current type:

```rust
pub const CRAWL_RECOVERY_FORMAT_VERSION: u16 = 1;
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
```

`CrawlRecoveryPhase` remains crawl-only. Do not add extraction or validation phases.

The envelope and crawl payload versions are independent axes:

```text
CheckpointEnvelope format v1
    └── CrawlRecoveryCheckpoint format v1
```

There is no single global “checkpoint version”.

Idiomatic semantic names replace historical names:

- `CHECKPOINT_ENVELOPE_FORMAT_VERSION`, not `CURRENT_CHECKPOINT_SCHEMA_VERSION`;
- `CRAWL_RECOVERY_FORMAT_VERSION`, not `CRAWL_CHECKPOINT_V2_PAYLOAD_VERSION`;
- `MAX_CRAWL_RECOVERY_PAYLOAD_BYTES`, not a `V2`-named size constant;
- `CrawlRecoveryCheckpoint`, not `CrawlCheckpointV2`.

`new`, `to_envelope`, and `from_envelope` are the preferred constructor/conversion vocabulary if those methods remain the cleanest implementation.

## 7. Legacy removal

Remove the old development checkpoint model from production API/code:

- `CrawlCheckpoint`;
- `CrawlCheckpointV2`;
- `CrawlCheckpointUnit`;
- `CrawlCheckpointUnitState`;
- `CRAWL_CHECKPOINT_PAYLOAD_VERSION`;
- `CRAWL_CHECKPOINT_V2_PAYLOAD_VERSION`;
- compatibility code whose only purpose is decoding or projecting those formats.

Do not add:

```rust
type CrawlCheckpointV2 = CrawlRecoveryCheckpoint;
```

Do not retain deprecated aliases, dual decoders, old-format migration helpers, or automatic rewrites. Raw JSON fixtures are sufficient to test rejection of historical shapes.

Audit broad re-exports such as `pub use checkpoint::*`; only legitimately cross-crate current concepts should remain public.

## 8. Checkpoint control vs durable crawl truth

Checkpoint state is recovery **control**, not crawl progress truth.

Authoritative crawl progress is reconstructed from:

```text
crawl execution history
+ discovered URL evidence
+ CrawlTraversalControl
+ CrawlUrlStateRecord logical work
```

This durable data plane must win over stale checkpoint timing. If an execution commits immediately before a crash and before the next checkpoint append, recovery reconstructs the newer durable truth rather than replaying stale checkpoint partitions.

Canonical structural reconstruction therefore no longer accepts a serialized checkpoint representation:

```rust
reconstruct_crawl_structural_facts(
    snapshot,
    current_status,
    executions,
    discovered_urls,
    control,
    work,
)
```

There is no canonical `Option<&CrawlCheckpoint>` fallback.

This deliberately separates:

1. **Can this run safely resume?** — checkpoint control + durable recovery coherence.
2. **What did this crawl durably accomplish?** — durable data plane → `CrawlStructuralFacts`.

Historical finalization wrappers that exist only to consume the old growing checkpoint must be removed or rewritten around the canonical durable-data path.

## 9. Recovery validation

Recovery has two independent gates.

### Stage A — recovery-control validity

Validate:

1. envelope format;
2. payload kind (`CRAWL_RECOVERY`);
3. crawl recovery format;
4. immutable run identity;
5. run-type compatibility;
6. crawl-specific compact-size bound.

### Stage B — durable recovery coherence

Reconstruct and validate:

- traversal control;
- logical work state;
- execution evidence;
- discovery evidence;
- recovery generation state where applicable.

A valid checkpoint does not imply a safe resume. Same-run recovery is allowed only after both stages succeed.

Explicit Resume, automatic crash recovery, Retry, and Retry Failed Parts must use the same Stage-A crawl recovery authority. No caller may inspect historical JSON fields such as `payload_version` to choose a decoder.

## 10. Error and API taxonomy

### Generic envelope errors

The generic checkpoint repository must distinguish unsupported format from malformed current-format data. Its exact error vocabulary should include the semantic categories:

- `UnsupportedFormatVersion` — recognized checkpoint JSON/header, but envelope format is not current;
- `Malformed` — JSON cannot be decoded as the current envelope shape;
- `InvalidEnvelope` — current-format envelope decodes but violates bounds/invariants;
- `PayloadTooLarge`;
- `Serialization`;
- existing persistence/ownership errors such as `Database`, `NotFound`, and `LeaseLost`.

Do not keep a broad `Inconsistent` category when a more specific current-format error is known.

### Crawl recovery errors

Use the canonical type name:

```rust
pub enum CrawlRecoveryCheckpointError {
    UnsupportedFormatVersion,
    UnexpectedPayloadKind,
    MalformedPayload,
    IncompatibleRunIdentity,
    UnsupportedRunType,
    Serialization,
}
```

Generic envelope decode failures remain generic repository errors; crawl payload failures use `CrawlRecoveryCheckpointError`. Durable data-plane contradiction is a jobs-layer `RecoveryStateInvalid` condition, not a malformed payload.

### API errors

Rejected recovery actions expose these safe codes:

- `CHECKPOINT_MISSING`;
- `CHECKPOINT_FORMAT_UNSUPPORTED`;
- `CHECKPOINT_MALFORMED`;
- `CHECKPOINT_IDENTITY_MISMATCH`;
- `RECOVERY_STATE_INVALID`.

All five recovery-conflict conditions use HTTP **409 Conflict**. Existing invalid-request/not-found/infrastructure semantics outside this taxonomy remain unchanged.

Mapping rules:

- unsupported envelope or crawl recovery format, or unexpected payload kind → `CHECKPOINT_FORMAT_UNSUPPORTED`;
- malformed/invalid current envelope or malformed crawl payload → `CHECKPOINT_MALFORMED`;
- immutable run/snapshot/config identity mismatch → `CHECKPOINT_IDENTITY_MISMATCH`;
- coherent serialization but contradictory durable recovery data → `RECOVERY_STATE_INVALID`.

Raw checkpoint JSON, URLs contained only in raw payloads, provider data, credentials, and internal persistence details must not leak through API responses, display text, logs, or telemetry.

## 11. Job-action semantics

### Resume

Continue interrupted work only when current-format recovery control matches the immutable run and durable recovery state is coherent.

- same `CrawlRun`;
- same frozen snapshot;
- no Seed replay;
- no silent failed-work reset;
- no independent new run.

### Retry Failed Parts

Create a new recovery generation for currently failed/partial logical work in the same immutable run. Selection comes from durable logical work, not checkpoint unit arrays. Completed work remains authoritative; historical failed/partial execution rows remain immutable evidence.

### Retry

Remain a bounded job-level continuation. For crawl jobs requiring recovery, Retry cannot bypass checkpoint compatibility or durable recovery validation.

### Restart from Beginning

Never act as an implicit fallback from failed Resume.

- Quick Scrape may explicitly restart the same run from its frozen configuration where current lifecycle rules permit it.
- Production may not replay an existing durable run from Seeds.

### Rerun Full Crawl

For Production, a fresh full execution is an independent `CrawlRun` with a new immutable run identity/evidence lineage. It is not recovery.

### Storage pressure

Storage pressure remains resumable deferral, not failure or terminalization. D03 changes checkpoint representation, not this behavior.

## 12. Persistence and migration policy

`job_checkpoints` already stores `checkpoint_json TEXT`; the SQL table does not encode the envelope fields. Therefore DX-D03 performs a JSON contract reset without a SQL migration.

- no migration `0009`;
- do not rewrite `0004_jobs.sql`;
- `0007` remains reserved for Plan 07;
- `0008` remains reserved for Plan 08;
- `0009` remains next unallocated.

Old development checkpoint rows are intentionally non-resumable:

```text
old checkpoint JSON
    ↓
current decoder
    ↓
unsupported / malformed
    ↓
Resume rejected
```

No rewrite or converter is introduced. Developers may reset local pre-release databases. Product actions remain explicit: Quick Scrape may Restart where legal; Production uses Rerun Full Crawl when same-run recovery is unsafe.

Existing durability rules remain unchanged:

- checkpoint rows are append-only;
- no checkpoint UPDATE or DELETE;
- append remains lease/attempt owned;
- a checkpoint becomes resumable only after append commits;
- sequence remains repository-owned monotonic ordering.

Keep the existing generic encoded checkpoint safety ceiling (currently 64 KiB) and the crawl-specific compact payload ceiling at 1 KiB. Secrets, provider bodies, raw HTML, credentials, and unbounded scraped content never belong in checkpoint payloads.

## 13. Required test coverage

### Generic envelope

- current-format round-trip;
- unsupported format is distinct from malformed JSON;
- invalid/empty/oversized payload kind rejection;
- non-object/null payload rejection;
- structured payload round-trip;
- immutable identity validation;
- encoded-size ceiling;
- repository-owned sequence behavior;
- append-only/lease semantics remain intact.

### Crawl recovery

- `CrawlRecoveryCheckpoint` round-trip;
- wrong payload kind;
- unsupported recovery format;
- malformed payload;
- run ID mismatch;
- snapshot hash mismatch;
- compatibility hash mismatch;
- CrawlerVersion/semantic-config identity mismatch;
- valid Quick Scrape identity;
- unsupported run type;
- 1 KiB compact bound.

### Historical format rejection

- old envelope/payload shapes cannot resume;
- no historical shape is silently interpreted as current;
- no legacy decoder fallback exists.

Raw JSON fixtures are sufficient.

### Durable recovery

- checkpoint validity alone is insufficient;
- contradictory control/work state rejects recovery;
- durable completed work wins over stale checkpoint timing;
- `CrawlStructuralFacts` reconstruction has no checkpoint-format dependency.

### Job actions/API

- Resume requires current compatible recovery;
- Retry cannot bypass validation;
- Retry Failed Parts derives failed/partial selection from durable work;
- invalid Resume never silently restarts;
- Quick Scrape explicit restart remains legal where already permitted;
- Production same-run restart remains illegal;
- Production Rerun Full Crawl remains independent-run semantics;
- each recovery API code maps correctly and leaks no raw checkpoint payload.

## 14. Verification and acceptance

Fresh verification before acceptance must include at least:

```powershell
cargo test -p erabi-db -j 1
cargo test -p erabi-crawler -j 1
cargo test -p erabi-jobs -j 1
cargo test -p erabi-api -j 1
cargo test -p erabi --lib -j 1
cargo test -p erabi --test runtime_server -j 1

cargo check -p erabi-db --all-targets -j 1
cargo check -p erabi-crawler --all-targets -j 1
cargo check -p erabi-jobs --all-targets -j 1
cargo check -p erabi-api --all-targets -j 1
cargo check -p erabi --all-targets -j 1

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

DX-D03 follows the existing package state gates:

```text
IMPLEMENTED != VERIFIED != ACCEPTED != MERGED
```

- **IMPLEMENTED** — scoped implementation/documentation complete.
- **VERIFIED** — fresh relevant gate passes.
- **ACCEPTED** — independent Terra HIGH review on the exact candidate reports `BLOCKER: 0` and `IMPORTANT: 0`.
- **MERGED** — accepted candidate is integrated to `main` and lifecycle documentation is reconciled.

If Terra already runs the full fresh gate on the exact unchanged candidate, do not rerun the heavy gate merely for closure.

## 15. Resulting architecture

```text
                       erabi-db
                CheckpointEnvelope v1
                ├── sequence
                ├── immutable identity
                ├── payload kind
                └── structured payload
                         │
                         ▼
                    erabi-crawler
              CrawlRecoveryCheckpoint v1
                ├── immutable crawl identity
                └── CrawlRecoveryPhase
                         │
              validates recovery control
                         │
                         ▼
                durable crawl data plane
                ├── executions
                ├── discovery
                ├── traversal control
                └── logical work
                         │
                         ▼
                 CrawlStructuralFacts
```

Recovery control and crawl progress truth are separate by design. Plan 07 starts from this seam instead of from speculative extraction state embedded in the generic database envelope.

## 16. Locked decisions / implementation handoff

The following decisions are approved and must not be silently reopened by the implementation plan:

1. Pre-release checkpoint compatibility is not an acceptance requirement.
2. Breaking source/API/serialized checkpoint changes are allowed to reach the clean canonical design.
3. Historical checkpoint types/names are removed without aliases or dual decoders.
4. Generic `CheckpointEnvelope` is minimal and uses structured JSON payload plus bounded opaque payload kind.
5. Current crawl recovery is `CrawlRecoveryCheckpoint` format v1 with `Initialized`, `Traversing`, and `Finalizing` phases only.
6. Durable crawl data, not checkpoint partitions, is progress truth.
7. Canonical structural finalization does not depend on serialized checkpoint representation.
8. Recovery failures distinguish unsupported format, malformed data, immutable identity mismatch, and invalid durable state.
9. Invalid Resume never silently restarts.
10. Quick Scrape may explicitly Restart where legal; Production requires independent Rerun Full Crawl when same-run recovery is unsafe.
11. Storage pressure remains resumable deferral.
12. No extraction recovery semantics are designed in D03.
13. No SQL migration is created; `0009` remains unallocated.
14. Old development checkpoint rows are intentionally non-resumable.
15. Append-only durability and repository-owned sequence remain unchanged.
16. D03 does not absorb D06, D07, Plan 07, or unrelated cleanup.

The implementation plan must audit every current call site that:

- uses `CrawlCheckpoint` or `CrawlCheckpointV2`;
- inspects `payload_version` manually;
- relies on old generic envelope fields;
- passes legacy checkpoint state into structural finalization;
- collapses distinct checkpoint/recovery failures into broad `unsafe`/`incompatible` errors.

The implementation plan must preserve Erabi's implementation-first, verification-after workflow and must not introduce TDD sequencing unless explicitly requested later.
