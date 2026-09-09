# DX-D03 — Canonical Checkpoint and Recovery Contract Design

**Status:** Approved design

**Date:** 2026-09-10

**Package:** DX-D03

**Repository:** `wihlarkop/erabi`

**Purpose:** Establish the canonical pre-release checkpoint/recovery contract, naming, ownership, persistence format, and failure semantics before Plan 07 introduces extraction recovery requirements.

---

## 1. Context

Erabi currently has multiple checkpoint concepts whose names and ownership reflect implementation history rather than the architecture that now exists after Plan 06 Task 9 and DX-D05.

The current code contains three overlapping version concepts:

- the generic `CheckpointEnvelope` with `CURRENT_CHECKPOINT_SCHEMA_VERSION = 1`;
- a historical large `CrawlCheckpoint` payload with `CRAWL_CHECKPOINT_PAYLOAD_VERSION = 1`;
- a compact current `CrawlCheckpointV2` payload with `CRAWL_CHECKPOINT_V2_PAYLOAD_VERSION = 2`.

The current Production and Quick Scrape runtime uses the compact `CrawlCheckpointV2` path. Durable crawl progress, frontier state, traversal counters, and current logical work have moved out of the growing checkpoint payload and into the durable crawl data plane introduced by migration `0006`.

At the same time, the generic database checkpoint envelope still contains crawl/extraction-specific fields such as completed/pending/failed units, discovery position, artifact references, and `ExtractionResumeState`. Those fields are no longer the current crawl authority and the extraction fields are speculative ahead of Plan 07.

DX-D03 cleans this up before those historical shapes become permanent architecture.

---

## 2. Pre-release compatibility policy

Erabi has not shipped a public release. Therefore DX-D03 does **not** treat internal development checkpoint formats, Rust APIs, or serialized checkpoint JSON as compatibility contracts.

The governing principle is:

> While Erabi remains pre-release, prefer the cleanest correct architecture, idiomatic Rust naming, and explicit ownership over compatibility with development-only state.

Breaking changes are acceptable when they materially improve the canonical contract. Historical checkpoint formats may be removed outright. Development databases may require a reset or explicit fresh execution after the change.

This does **not** mean durable business data is generally disposable. The relaxed compatibility policy in this package applies specifically to pre-release operational recovery state and accidental internal APIs. Future released user/business contracts require deliberate migration and compatibility policy.

DX-D03 must still fail closed. Old or malformed state must never be silently interpreted using new semantics.

---

## 3. Design goals

DX-D03 must:

1. make checkpoint terminology unambiguous and idiomatic;
2. separate generic checkpoint persistence from plan-specific recovery semantics;
3. remove legacy/growing crawl checkpoint representations from the canonical runtime surface;
4. keep the current crawl recovery checkpoint small and bounded;
5. make durable crawl data the sole authority for crawl progress truth;
6. distinguish serialized checkpoint failures from durable recovery-state failures;
7. ensure explicit Resume and automatic crash recovery use one interpretation;
8. preserve correct job/run lifecycle semantics while allowing pre-release API/format breakage;
9. remove speculative extraction recovery ownership from `erabi-db`;
10. leave Plan 07 free to design extraction recovery from actual requirements;
11. avoid consuming a migration number merely to replace JSON stored in `job_checkpoints.checkpoint_json`.

---

## 4. Non-goals

DX-D03 does **not** implement or redesign:

- Plan 07 extraction, curation, validation, schema drift, Dataset, Record, review, or provenance behavior;
- extraction selectors, node maps, normalization, or typed value extraction;
- extraction recovery semantics;
- migration `0007`, `0008`, or `0009`;
- `SemanticTraversal` decomposition owned by DX-D06;
- crawler repository decomposition owned by DX-D07;
- provider execution, network admission, robots, or pacing semantics;
- run-type or job-status models;
- storage-pressure policy;
- progress/SSE architecture;
- retry algorithms outside checkpoint/recovery validation integration;
- unrelated API cleanup;
- unrelated repository architecture work.

Breaking changes are allowed, but they are not an excuse for unrelated refactoring.

---

## 5. Ownership model

### 5.1 `erabi-db`: generic durable checkpoint transport

`erabi-db` owns:

- append-only checkpoint persistence;
- checkpoint row ordering/sequence;
- immutable checkpoint identity storage;
- generic envelope format validation;
- generic payload-kind bounds;
- overall encoded-size bounds;
- lease/attempt ownership when appending checkpoint rows;
- loading the latest checkpoint in a lineage.

`erabi-db` does **not** own:

- crawl recovery phases;
- crawl URL/work partitions;
- extraction phases;
- crawl or extraction business interpretation;
- plan-specific payload decoding.

### 5.2 `erabi-crawler`: crawl recovery control

`erabi-crawler` owns:

- `CrawlRecoveryCheckpoint`;
- `CrawlRecoveryPhase`;
- crawl recovery format version;
- crawl-specific identity validation against the immutable `CrawlRunSnapshot`;
- crawl-specific recovery payload size bound;
- conversion between `CrawlRecoveryCheckpoint` and the generic envelope.

The crawl checkpoint answers:

> Is this recovery control record for the same immutable crawl, and what crawl recovery phase had been durably reached?

It does **not** answer:

> Which URLs are complete, failed, partial, or pending?

That truth belongs to the durable data plane.

### 5.3 `erabi-jobs`: workflow and action semantics

`erabi-jobs` owns:

- when recovery is attempted;
- action semantics for Resume, Retry, Retry Failed Parts, Restart from Beginning, and Rerun Full Crawl;
- composition of checkpoint validity with durable recovery-state coherence;
- operator-facing recovery error classification;
- Production vs Quick Scrape lifecycle policy.

### 5.4 `erabi-api`: safe presentation

`erabi-api` owns stable, actionable HTTP error presentation without exposing raw checkpoint payloads.

### 5.5 Plan 07

Plan 07 will decide extraction recovery ownership and representation from actual extraction workflow requirements. DX-D03 must not invent `Extracting`, `Validating`, or generic extraction checkpoint state in advance.

---

## 6. Canonical generic envelope

The generic envelope becomes a minimal bounded persistence container.

Target Rust shape:

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

### 6.1 `CheckpointPayloadKind`

`CheckpointPayloadKind` is a bounded opaque newtype, not a closed generic enum:

```rust
pub struct CheckpointPayloadKind(String);
```

It exposes a validated constructor and read accessor, for example:

```rust
impl CheckpointPayloadKind {
    pub fn new(value: impl Into<String>) -> Result<Self, CheckpointRepositoryError>;
    pub fn as_str(&self) -> &str;
}
```

The database crate must not know every current or future recovery kind. The owning subsystem supplies the semantic kind value, such as `CRAWL_RECOVERY`.

### 6.2 Removed generic fields

The following concepts are removed from the canonical generic envelope:

- `completed_units`;
- `pending_units`;
- `failed_units`;
- `discovery_position`;
- `artifact_references`;
- `extraction`.

Supporting generic types that become unused after the replacement must also be removed rather than retained as accidental public APIs, including, where no remaining legitimate responsibility exists:

- `CheckpointUnitId`;
- `CheckpointPosition`;
- `CheckpointArtifactReference`;
- `ExtractionResumePhase`;
- `ExtractionResumeState`;
- associated bounds/constants whose only purpose was those fields.

If implementation-time audit finds one of these concepts still has a legitimate current responsibility outside the superseded envelope, it must be renamed/re-homed according to that responsibility rather than retained only for compatibility.

### 6.3 Structured payload, not JSON inside JSON

The current `payload: Option<String>` double-encodes typed JSON inside envelope JSON. DX-D03 replaces this with structured `serde_json::Value`.

The persisted shape becomes conceptually:

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

`erabi-db` treats `payload` as opaque structured transport. It does not inspect crawl-specific fields.

---

## 7. Canonical crawl recovery checkpoint

Historical names are removed from the canonical API.

The current concept becomes:

```rust
pub const CRAWL_RECOVERY_FORMAT_VERSION: u16 = 1;

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
```

The recovery phase remains deliberately crawl-only:

```rust
pub enum CrawlRecoveryPhase {
    Initialized,
    Traversing,
    Finalizing,
}
```

No extraction/post-processing phases are added.

### 7.1 Idiomatic naming

The API names concepts by semantics rather than historical revision number:

- `CrawlRecoveryCheckpoint`, not `CrawlCheckpointV2`;
- `CHECKPOINT_ENVELOPE_FORMAT_VERSION`, not `CURRENT_CHECKPOINT_SCHEMA_VERSION`;
- `CRAWL_RECOVERY_FORMAT_VERSION`, not `CRAWL_CHECKPOINT_V2_PAYLOAD_VERSION`;
- a semantic crawl recovery size constant, not `MAX_CRAWL_CHECKPOINT_V2_PAYLOAD_BYTES`.

`new`, `to_envelope`, and `from_envelope` remain appropriate constructor/conversion names if the implementation keeps those methods.

### 7.2 Independent version axes

The generic envelope and crawl recovery payload have independent format versions:

```text
CheckpointEnvelope format v1
    └── CrawlRecoveryCheckpoint format v1
```

A future envelope v2 and crawl recovery v1, or envelope v1 and crawl recovery v2, can be independently reasoned about. There is no single global "checkpoint version".

---

## 8. Legacy checkpoint removal

DX-D03 removes historical development models rather than preserving compatibility aliases or permanent legacy decoders.

The canonical codebase must no longer expose historical concepts such as:

- `CrawlCheckpoint` as the old growing payload;
- `CrawlCheckpointV2` as the current payload;
- `CrawlCheckpointUnit`;
- `CrawlCheckpointUnitState`;
- `CRAWL_CHECKPOINT_PAYLOAD_VERSION`;
- `CRAWL_CHECKPOINT_V2_PAYLOAD_VERSION`.

Do not introduce:

```rust
type CrawlCheckpointV2 = CrawlRecoveryCheckpoint;
```

Do not retain deprecated public types solely for compatibility.

Do not build:

- a V1-to-V2/V3 converter;
- an old-format migration framework;
- a dual runtime decoder;
- a compatibility shim that silently rewrites old checkpoint JSON.

Tests may use small raw JSON fixtures to prove that historical shapes are rejected safely. They do not require maintaining historical production Rust models.

---

## 9. Checkpoint vs durable crawl truth

The serialized checkpoint is recovery **control**, not crawl progress truth.

The authoritative crawl data plane is:

```text
crawl execution history
+ discovered URL evidence
+ CrawlTraversalControl
+ CrawlUrlStateRecord logical work
```

That data reconstructs the current crawl truth after a crash, including when an execution row committed immediately before the process died and before the next checkpoint append.

Therefore stale checkpoint timing must not override newer durable work state.

### 9.1 Structural finalization

The canonical crawl structural reconstruction API must no longer depend on a serialized checkpoint representation.

Target conceptual shape:

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

There is no canonical `Option<&CrawlCheckpoint>` fallback parameter.

This separates two independent questions:

1. **Can execution safely resume?** — checkpoint + recovery coherence.
2. **What did the crawl durably accomplish?** — durable data plane → `CrawlStructuralFacts`.

Historical compatibility finalizers that only exist to feed the old growing checkpoint representation should be removed or rewritten so the canonical path does not require that representation.

---

## 10. Recovery validation model

Recovery validation has two explicit stages.

### Stage A — checkpoint control validity

Validate:

1. generic envelope format;
2. payload kind;
3. crawl recovery format;
4. immutable run identity;
5. run-type compatibility;
6. crawl-specific size bound.

### Stage B — durable recovery coherence

Reconstruct and validate the durable crawl data plane:

- traversal control;
- logical work state;
- execution evidence;
- discovery evidence;
- recovery generation state where applicable.

A valid serialized checkpoint does **not** by itself imply recovery is safe.

Only Stage A + Stage B success permits same-run recovery.

---

## 11. Recovery error taxonomy

DX-D03 must stop collapsing materially different recovery failures into a single ambiguous "unsafe" category.

Crawler-level recovery decoding should distinguish at least the following semantic categories, with exact enum naming finalized in the implementation plan:

- invalid generic envelope;
- unsupported checkpoint/recovery format;
- malformed crawl recovery payload;
- immutable run identity mismatch;
- unsupported run type;
- encoding/serialization failure.

Durable data-plane contradictions are a separate jobs-layer recovery-state error. They must not be mislabeled as malformed serialized payload.

Conceptually:

```text
serialized checkpoint problem
    !=
durable recovery data-plane problem
```

### 11.1 API presentation

The API should present actionable safe codes for the distinct operator states. The intended semantic set is:

- `CHECKPOINT_MISSING`;
- `CHECKPOINT_FORMAT_UNSUPPORTED`;
- `CHECKPOINT_MALFORMED`;
- `CHECKPOINT_IDENTITY_MISMATCH`;
- `RECOVERY_STATE_INVALID`.

Exact HTTP status remains `409 Conflict` for rejected recovery actions unless implementation-time API-contract review finds an existing canonical convention that requires a different status. Raw checkpoint contents must never appear in API responses, telemetry, logs, or error display text.

---

## 12. Canonical job-action semantics

DX-D03 preserves lifecycle semantics that are already correct while making checkpoint validation consistent.

### 12.1 Resume

`Resume` means continue interrupted compatible work exactly where durable state permits.

Requirements:

- same `CrawlRun`;
- same frozen immutable snapshot;
- current-format compatible checkpoint;
- coherent durable recovery state;
- no Seed replay;
- no silent reset of failed work;
- no new independent CrawlRun.

### 12.2 Retry Failed Parts

`Retry Failed Parts` creates a new recovery generation for currently failed/partial logical units in the same immutable run.

Completed work remains current truth. Historical failed/partial executions remain immutable evidence.

Failed/partial selection comes from the durable data plane, not from legacy checkpoint unit arrays.

### 12.3 Retry

Generic `Retry` remains a bounded job-level retry/continuation. For crawl jobs that require durable recovery, it must not bypass checkpoint compatibility or durable recovery-state validation.

### 12.4 Restart from Beginning

Restart is explicit and never an implicit fallback from failed Resume.

For Quick Scrape, an explicit same-run restart from its frozen configuration may remain legal according to current lifecycle rules.

For Production, replaying an existing durable run from Seeds is unsafe and remains illegal. A fresh full Production execution uses `Rerun Full Crawl`.

### 12.5 Rerun Full Crawl

`Rerun Full Crawl` is not recovery. It creates a new independent Production run and evidence lineage from intentionally reused frozen semantic configuration where allowed.

### 12.6 Storage pressure

Storage pressure remains resumable deferral, not failure or terminalization. A safe durable checkpoint/data-plane boundary is persisted and the worker turn defers without inventing a failed/partial result.

---

## 13. Automatic crash recovery and explicit Resume share one authority

Explicit Resume, worker startup/crash recovery, Retry, and Retry Failed Parts must not implement different interpretations of the same checkpoint.

All crawl recovery entry points use the same canonical crawl recovery decode/validation authority, conceptually:

```rust
CrawlRecoveryCheckpoint::from_envelope(...)
```

or one equivalent semantic helper owned by `erabi-crawler`.

Durable-state reconstruction remains a separate second-stage authority.

No component may manually inspect arbitrary checkpoint JSON fields such as historical `payload_version` to decide which decoder to invoke.

---

## 14. Persistence and migration policy

### 14.1 No SQL migration

`job_checkpoints` persists the envelope in `checkpoint_json TEXT`. DX-D03 changes the JSON representation, not the table shape.

Therefore:

- no `0009` migration is created;
- existing `0004_jobs.sql` is not rewritten;
- `0007` remains reserved for Plan 07;
- `0008` remains reserved for Plan 08;
- `0009` remains the next unallocated migration.

### 14.2 Old development checkpoint rows

Historical checkpoint JSON becomes intentionally non-resumable.

Expected behavior:

```text
old development checkpoint
    ↓
new decoder
    ↓
unsupported / malformed pre-release format
    ↓
Resume rejected
```

There is no checkpoint rewrite or automatic conversion.

A developer may reset a local pre-release database. At product-action level:

- Quick Scrape may use explicit Restart from Beginning where legal;
- Production uses an explicit independent Rerun Full Crawl when recovery cannot be trusted.

### 14.3 Append-only guarantees remain unchanged

D03 must preserve existing durability rules:

- checkpoint rows are append-only;
- no checkpoint UPDATE;
- no checkpoint DELETE;
- checkpoint append remains lease/attempt owned;
- a checkpoint is not considered durable/resumable until its append transaction commits;
- sequence remains repository-owned monotonic ordering rather than payload-owned state.

---

## 15. Size and safety bounds

The generic repository retains a bounded absolute encoded checkpoint ceiling, currently 64 KiB unless implementation evidence justifies renaming/restructuring the constant without changing the safety property.

Crawler recovery retains a much smaller payload-specific bound, conceptually 1 KiB, because its representation is fixed-cardinality control identity rather than a crawl frontier or artifact carrier.

The hierarchy is:

```text
generic checkpoint envelope <= repository safety ceiling
crawl recovery payload       <= crawl-specific compact ceiling
```

Plan 07 defines any extraction-specific recovery bound from actual extraction needs.

Secrets, provider response bodies, raw HTML, credentials, and unbounded scraped content must never enter checkpoint payloads.

---

## 16. Public/private API discipline

D03 is allowed to break pre-release Rust APIs to remove accidental surface.

The target is:

```text
one semantic concept
→ one canonical Rust name
→ one canonical serialized format
```

Do not preserve aliases, deprecated wrappers, or dual decoders solely to keep development callers compiling.

However, breaking change freedom does not justify unnecessary public exposure. Types/functions should remain private or crate-visible unless another crate legitimately depends on them.

Existing broad `pub use checkpoint::*` exposure must be audited so removed legacy concepts do not remain accidentally public through re-exports.

---

## 17. Documentation reconciliation

The DX roadmap currently describes DX-D03 as preserving live checkpoint compatibility. That wording is no longer correct for the approved pre-release design.

DX-D03 documentation must describe the package as establishing the canonical pre-release checkpoint/recovery contract, naming, ownership, and failure semantics.

The design/implementation plan must also state explicitly that pre-release checkpoint compatibility is not an acceptance requirement.

---

## 18. Required test coverage

### 18.1 Generic envelope

Tests must cover:

- current envelope round-trip;
- unsupported envelope format rejection;
- invalid/empty/oversized payload kind rejection;
- structured payload round-trip;
- immutable identity validation;
- encoded-size ceiling;
- repository-owned sequence behavior;
- append-only persistence behavior remains intact.

### 18.2 Crawl recovery

Tests must cover:

- `CrawlRecoveryCheckpoint` round-trip;
- unsupported recovery format rejection as a distinct category;
- malformed payload rejection;
- wrong payload kind rejection;
- run ID mismatch;
- snapshot hash mismatch;
- compatibility hash mismatch;
- CrawlerVersion/semantic-config identity mismatch;
- valid Quick Scrape identity;
- unsupported run type;
- compact size bound.

### 18.3 Historical format rejection

Tests must prove that:

- old schema/payload shapes do not resume;
- historical development payloads are not silently interpreted as the new format;
- no legacy decoder fallback exists.

Raw JSON fixtures are sufficient; historical production models are not required.

### 18.4 Durable recovery coherence

Tests must prove that:

- checkpoint validity alone is insufficient for resume;
- contradictory durable control/work state fails recovery;
- durable completed work wins over stale checkpoint timing;
- structural crawl facts no longer require a serialized checkpoint representation.

### 18.5 Job actions

Tests must cover:

- Resume requires current compatible recovery state;
- Retry cannot bypass recovery validation;
- Retry Failed Parts derives selection from durable failed/partial work;
- invalid Resume never silently restarts;
- Quick Scrape explicit restart remains legal where current lifecycle allows it;
- Production same-run restart remains illegal;
- Production Rerun Full Crawl retains independent-run semantics.

### 18.6 API

Tests must cover safe error mapping for:

- `CHECKPOINT_MISSING`;
- `CHECKPOINT_FORMAT_UNSUPPORTED`;
- `CHECKPOINT_MALFORMED`;
- `CHECKPOINT_IDENTITY_MISMATCH`;
- `RECOVERY_STATE_INVALID`.

Raw checkpoint JSON must not leak through response bodies or diagnostics.

---

## 19. Verification gate

DX-D03 is a cross-crate architecture package across database, crawler, jobs, API, and runtime integration.

The implementation plan must include at least the following fresh verification before acceptance.

Focused/package tests:

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

Heavy verification should not be repeated after a fresh accepted review run if no production code changes afterward.

---

## 20. Acceptance state model

DX-D03 follows Erabi's existing package gates:

```text
IMPLEMENTED != VERIFIED != ACCEPTED != MERGED
```

- **IMPLEMENTED** — scoped code/documentation changes are complete.
- **VERIFIED** — the relevant fresh verification gate passes.
- **ACCEPTED** — independent Terra HIGH review reports `BLOCKER: 0` and `IMPORTANT: 0` on the exact candidate.
- **MERGED** — accepted implementation is integrated into `main` and roadmap lifecycle state is reconciled.

No package state may be skipped.

---

## 21. Resulting architecture

After DX-D03, checkpoint architecture is:

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

Recovery control and crawl progress truth are separate by design.

Plan 07 starts from this clean seam rather than from a generic database envelope containing speculative extraction state.

---

## 22. Locked design decisions

The following are explicitly approved and must not be silently changed during implementation planning or implementation:

1. Pre-release checkpoint compatibility is not an acceptance requirement.
2. Breaking Rust/source/serialized checkpoint changes are allowed when they improve the canonical design.
3. Historical `CrawlCheckpoint` / `CrawlCheckpointV2` naming is removed, not preserved through aliases.
4. The canonical crawl type is `CrawlRecoveryCheckpoint` with semantic naming and an independently versioned format.
5. The generic envelope is minimized and no longer carries crawl-unit, artifact, discovery-position, or extraction-specific fields.
6. Generic checkpoint payload becomes structured JSON instead of a JSON string nested inside JSON.
7. Durable crawl data, not checkpoint partitions, is the source of crawl progress truth.
8. Canonical structural finalization does not depend on a serialized checkpoint representation.
9. Resume validation distinguishes unsupported format, malformed payload, identity mismatch, and invalid durable recovery state.
10. Invalid Resume never silently falls back to Restart or Rerun.
11. Quick Scrape may explicitly Restart from Beginning where legal; Production requires an independent Rerun Full Crawl when same-run recovery is unsafe.
12. Storage pressure remains resumable deferral.
13. No extraction recovery semantics are designed in D03.
14. No SQL migration is created; `0009` remains unallocated.
15. Old development checkpoint rows are intentionally non-resumable and are not migrated.
16. Append-only checkpoint durability and sequence ownership remain intact.
17. D03 stays focused on checkpoint/recovery ownership and does not absorb D06, D07, Plan 07, or unrelated cleanup.

---

## 23. Implementation-planning handoff

After this design is reviewed and accepted in written form, the implementation plan must decompose the change into auditable tasks that preserve a compilable progression across `erabi-db`, `erabi-crawler`, `erabi-jobs`, `erabi-api`, tests, and documentation.

The implementation plan must not reintroduce legacy compatibility work that this design explicitly removes. It must identify exact production call sites that currently:

- use `CrawlCheckpoint` or `CrawlCheckpointV2`;
- inspect historical `payload_version` manually;
- rely on old generic checkpoint fields;
- pass legacy checkpoint state into structural finalization;
- map all malformed/incompatible states into overly broad action errors.

The plan must preserve Erabi's implementation-first, verification-after workflow and must not introduce TDD sequencing unless explicitly requested later.
