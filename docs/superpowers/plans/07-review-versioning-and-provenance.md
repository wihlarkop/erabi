# Erabi Review, Versioning, and Provenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist field-level provenance, immutable Dataset and Record versions, unique-key comparison, semantic change candidates, draft autosave, approval/rejection, diffs, and review closure.

**Architecture:** Every extracted field retains raw and normalized values plus its exact source evidence. Recrawls compare normalized values by unique key, never overwrite approved versions, and only create missing candidates from complete snapshots; review mutations run in explicit transactions and enforce non-overridable errors.

**Tech Stack:** Rust domain services, Turso repositories and transactions, Axum review APIs, UUIDv7.

## Global Constraints

- Use only the latest compatible stable dependency release available at implementation time.
- Never add alpha, beta, RC, preview, nightly-only, or Git-commit dependencies.
- Add Rust dependencies with `cargo add`; do not hand-invent crate version pins.
- Add frontend dependencies with `bun add`; Bun is the only JavaScript package manager and task runner.
- Commit `Cargo.lock` and `bun.lock`; CI installs from frozen lockfiles.
- Use the official `turso` Rust crate for the Erabi application database.
- Generate UUIDv7 application-side for every primary domain entity.
- Keep Crawl4AI unmodified and isolated behind `CrawlerAdapter`.
- Use one default process, `erabi serve`; distributed workers are roadmap-only.
- Bind to `127.0.0.1` by default; non-loopback binding requires `ERABI_ACCESS_TOKEN`.
- Read secrets and bootstrap-only settings from environment variables or `.env`; never persist secret values in Turso.
- Store normal user-configurable settings in Turso using built-in → global → Collection → per-run resolution.
- Freeze each Crawl Run configuration when it is created, including while `QUEUED`, retried, or resumed.
- Store large raw artifacts, logs, assets, exports, and backups on the filesystem, not as database blobs.
- Never mutate approved Schema, Dataset, or Record versions; edits always create a new version.
- Only a successful complete snapshot may create `MISSING_CANDIDATE` records.
- Validation errors block approval and cannot be overridden; warnings do not block approval.
- Do not emit telemetry or crash reports by default.
- Graceful shutdown is mandatory and has a fixed three-second deadline in the MVP.
- Automatic backup, deep integrity scheduling, retention cleanup, browser notifications, and Trash cleanup are all off by default.
- Target WCAG 2.2 AA, keyboard operation, visible focus, reduced motion, no color-only states, and 200% zoom usability.
- Use English UI copy through translation keys from the first commit.
- Implement roadmap items only when a later specification admits them; do not opportunistically add them to this plan.

---

## Scope, Dependencies, and Phase Gate

- **Depends on:** [06 Extraction and Schema System](./06-extraction-and-schemas.md).
- **Produces:** Provenance records, immutable version graph, semantic change detection, approval/rejection services, draft autosave, and Close/Reopen Review.
- **Gate:** Curation gate: tests prove approved immutability, complete-snapshot deletion safeguards, partial bulk approval, audit events, provenance retention, and semantic no-change behavior.
- **Execution order:** Complete every task in this file in numerical order and commit after each task. Do not begin the next plan until this gate passes.

## Focused File Map

```text
crates/erabi-domain/src/provenance/
crates/erabi-domain/src/content/
crates/erabi-db/src/repositories/datasets/
crates/erabi-db/src/repositories/records/
crates/erabi-db/src/repositories/provenance/
crates/erabi-api/src/routes/datasets.rs
crates/erabi-api/src/routes/records.rs
tests/integration/review/
```

---

### Task 30: Persist Field-Level Provenance for Every Extracted Value

**Files:**
- Create: `crates/erabi-domain/src/provenance.rs`
- Create: `crates/erabi-db/src/provenance.rs`
- Create: `crates/erabi-api/src/routes/provenance.rs`
- Modify: `crates/erabi-extraction/src/extract.rs`
- Test: `crates/erabi-db/tests/provenance_repository.rs`
- Test: `crates/erabi-api/tests/provenance_drawer.rs`

**Interfaces:**
- Produces: `FieldProvenance` linked to record version and field name.
- Produces: `GET /api/v1/record-versions/{id}/provenance` and per-field lookup.
- Preserves: source URL, Crawl Run, raw artifact, node, selector, raw/normalized value, transformations, Schema Version, timestamp, artifact hash.

- [ ] **Step 1: Write provenance completeness tests**

For an extracted `price` field, assert all mandatory fields are present and the artifact hash matches the stored artifact. Approving, superseding, or rejecting the record must not delete provenance.

- [ ] **Step 2: Define provenance model**

```rust
pub struct FieldProvenance {
    pub id: EntityId,
    pub record_version_id: EntityId,
    pub field_name: String,
    pub source_url: url::Url,
    pub crawl_run_id: EntityId,
    pub artifact_id: EntityId,
    pub artifact_hash: String,
    pub node_id: String,
    pub selector: String,
    pub raw_value: serde_json::Value,
    pub normalized_value: serde_json::Value,
    pub transformations: Vec<String>,
    pub schema_version_id: Option<EntityId>,
    pub extracted_at: Timestamp,
}
```

- [ ] **Step 3: Persist records and provenance atomically**

The extraction job inserts Dataset Version, Record Versions, validation results, and all field provenance in one transaction after artifacts exist. Failure rolls back all database rows and leaves the Crawl Run recoverable.

- [ ] **Step 4: Implement provenance API actions**

Return data needed to highlight the source node, open the original URL, open raw/DOM artifacts, copy selector, inspect normalization, and navigate to Crawl Run/Schema Version. Do not render raw HTML through this endpoint.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p erabi-db --test provenance_repository
cargo test -p erabi-api --test provenance_drawer
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/erabi-domain crates/erabi-db crates/erabi-api crates/erabi-extraction
git commit -m "feat(provenance): trace every extracted field"
```
### Task 31: Implement Dataset and Record Versions, Unique Keys, and Semantic Change Detection

**Files:**
- Create: `crates/erabi-domain/src/dataset.rs`
- Create: `crates/erabi-domain/src/record.rs`
- Create: `crates/erabi-domain/src/change.rs`
- Create: `crates/erabi-db/src/datasets.rs`
- Create: `crates/erabi-db/src/records.rs`
- Create: `crates/erabi-jobs/src/handlers/compare_snapshot.rs`
- Test: `crates/erabi-domain/tests/change_detection.rs`
- Test: `crates/erabi-jobs/tests/snapshot_comparison.rs`

**Interfaces:**
- Produces: single/composite unique keys with normalization options and content-hash fallback.
- Produces: `NoChange`, `NewCandidate`, `UpdatedCandidate`, `MissingCandidate`, `RestoredCandidate` classification.
- Produces: exact/possible duplicate signals and explicit Keep Both, Keep A, Keep B, or Merge Manually decisions; never automatic merge.
- Enforces: missing only from complete snapshot.

- [ ] **Step 1: Write semantic comparison tests**

Cover:

- whitespace-only raw change → no meaningful change;
- URL tracking parameter change → no meaningful change;
- ignored `updated_at` field → no meaningful change;
- normalized title change → `UpdatedCandidate`;
- new unique key → `NewCandidate`;
- absent approved key in complete snapshot → `MissingCandidate`;
- absent key in partial/failed/cancelled snapshot → no missing candidate;
- deleted key reappears → `RestoredCandidate`;
- same canonical URL/content hash/unique key → exact duplicate signal;
- fuzzy title/content similarity → possible duplicate only, never automatic merge.

- [ ] **Step 2: Implement unique-key construction**

Support one or ordered composite fields. Options include trim, case sensitivity, URL normalization, and empty behavior. Reject duplicate/empty configured keys as validation errors. When no key configured, use a deterministic normalized content hash and mark it as fallback in metadata.

- [ ] **Step 3: Implement canonical semantic hashes**

Serialize normalized compared fields with sorted keys, excluding fields configured `ignore_in_change_detection`. Hash the canonical bytes with SHA-256. Raw artifact hashes remain separate and may change without review.

- [ ] **Step 4: Implement snapshot comparison**

Compare the new extracted Draft against current approved versions by unique key. Reuse approved versions for exact semantic matches. Create new Draft Record Versions only for new/updated/restored candidates. Create missing candidates only when `complete_snapshot=true` and all unique-key health checks pass.

- [ ] **Step 5: Implement no-change outcome**

When new, updated, and missing counts are all zero, set Crawl Run `SUCCEEDED` with result `NO_CHANGES`, store summary/artifacts/audit, and do not create a Review or empty Dataset Version.

Implement duplicate suggestions separately from semantic version matching. Exact signals are canonical URL, normalized content hash, and configured unique key. Possible duplicates use bounded fuzzy title/content similarity. Persist the evidence and user decision. Actions are Keep Both, Keep A, Keep B, and Merge Manually; Merge Manually opens normal Draft editing and never merges automatically. Bulk actions may keep first/latest/all or send selected items to review.

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p erabi-domain --test change_detection
cargo test -p erabi-jobs --test snapshot_comparison
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/erabi-domain crates/erabi-db crates/erabi-jobs
git commit -m "feat(versioning): detect meaningful record changes"
```
### Task 32: Implement Review, Draft Autosave, Approval, Rejection, Diff, and Close/Reopen

**Files:**
- Create: `crates/erabi-domain/src/review.rs`
- Create: `crates/erabi-db/src/reviews.rs`
- Create: `crates/erabi-api/src/routes/reviews.rs`
- Create: `crates/erabi-api/src/routes/records.rs`
- Create: `crates/erabi-api/src/routes/approvals.rs`
- Test: `crates/erabi-api/tests/review_workflow.rs`
- Test: `crates/erabi-db/tests/approval_atomicity.rs`

**Interfaces:**
- Produces: Review listing, Grid/Card data, Draft cell update, bulk approve valid, reject, field diff decisions, missing/deleted/restore decisions, close/reopen.
- Enforces: approved versions immutable and validation errors non-overridable.

- [ ] **Step 1: Write complete workflow tests**

Test:

- scrape Draft auto-opens an `OPEN` Review;
- Draft update with correct expected revision autosaves;
- stale revision returns 409;
- approval locks Record Version;
- editing approved returns conflict and requires new version;
- `Approve All Valid` approves valid, skips invalid, leaves Dataset `PARTIALLY_APPROVED`;
- warning records approve without confirmation;
- single rejection reason optional;
- bulk rejection reason required;
- close with unresolved items requires confirmation and creates `CLOSED_WITH_UNRESOLVED_ITEMS`;
- reopen records audit event;
- recrawl change exposes per-field diff.

- [ ] **Step 2: Implement Draft autosave**

`PATCH /api/v1/record-versions/{id}/draft` accepts field changes and `expected_revision`. Validate immediately, store raw manual override separately from crawler provenance, increment Draft revision, and return `SAVING/SAVED` compatible state. Do not add Undo/Redo history.

- [ ] **Step 3: Implement atomic approval transaction**

Inside one transaction:

1. verify no validation errors;
2. verify expected revision/current pointer;
3. mark prior approved version `SUPERSEDED` when present;
4. mark Draft version `APPROVED` and immutable;
5. update Record current pointer/status;
6. update Dataset Version status;
7. insert approval row;
8. append audit event;
9. commit.

- [ ] **Step 4: Implement partial bulk approval**

Process selected records in bounded batches. Valid records commit; invalid records stay Draft. Return exact approved/skipped/warning counts. The operation must never silently approve an invalid record.

- [ ] **Step 5: Implement rejection and candidate decisions**

Preserve raw data/provenance. Support preset/free-text reason. Missing candidate actions: mark deleted, keep active, ignore this run, recrawl, open source. Restored candidate requires explicit confirmation. Record deletion is a versioned event, not physical removal.

- [ ] **Step 6: Implement Close/Reopen Review**

Close normal when no unresolved items. With unresolved items, require `confirm_unresolved=true`, set special status, and include summary. Close does not alter Record states. Reopen may occur at any time and is audited.

- [ ] **Step 7: Run tests**

Run:

```bash
cargo test -p erabi-api --test review_workflow
cargo test -p erabi-db --test approval_atomicity
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/erabi-domain crates/erabi-db crates/erabi-api
git commit -m "feat(review): curate immutable approved data"
```
