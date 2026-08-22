# Erabi Extraction, Curation, and Provenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement safe visual extraction owned by Page Types, typed normalization/validation, production schema-drift diagnostics, Dataset/Record review and immutable approvals, semantic recrawl candidates, relationships, and durable field-level provenance.

**Architecture:** Raw crawl artifacts remain immutable evidence. Page Type extraction definitions are semantic Crawler Version configuration; they do not have an independent global approval lifecycle. Curated Dataset/Record versions and candidate values are persisted separately from raw artifacts and never silently overwrite approved state.

**Tech Stack:** stable Rust, HTML parser/sanitizer crates selected via stable `cargo add`, CSS selector evaluation, Serde, Axum, Turso repositories.

**Spec:** `docs/specs/04-extraction-curation-and-provenance.md`, `docs/specs/03-discovery-graph-and-runs.md`, `docs/specs/08-ux-accessibility-and-verification.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Global Constraints

- Extraction configuration belongs to Page Types inside a Crawler Version.
- Raw website HTML is untrusted and never rendered directly in the primary app DOM.
- Raw artifacts remain immutable even when sanitized preview artifacts are generated.
- MVP selectors are CSS selectors; field selectors are relative to the selected container where applicable.
- Automatic selector repair is not MVP.
- Validation `ERROR` blocks approval and cannot be overridden; `WARNING` does not block approval.
- Shared Dataset mappings require compatible identity, field types, required semantics, normalization, and unique-key contracts.
- Approved record versions are immutable.
- Field-level candidate merge never silently overwrites approved values.
- `MISSING_CANDIDATE` is allowed only after a healthy complete `PRODUCTION_RUN`.
- Production-breaking `SCHEMA_DRIFT` blocks trusted complete/missing semantics and requires a new Crawler Draft/test/publish fix; no production `USE_ANYWAY` escape.
- Provenance must survive ordinary artifact retention at a minimum metadata level.

## Focused File Map

```text
migrations/0006_curated_data.sql
crates/erabi-domain/src/extraction.rs
crates/erabi-domain/src/dataset.rs
crates/erabi-domain/src/review.rs
crates/erabi-domain/src/provenance.rs
crates/erabi-extraction/src/preview.rs
crates/erabi-extraction/src/mode.rs
crates/erabi-extraction/src/extract.rs
crates/erabi-extraction/src/normalize.rs
crates/erabi-extraction/src/validate.rs
crates/erabi-extraction/src/drift.rs
crates/erabi-api/src/routes/extraction.rs
crates/erabi-api/src/routes/reviews.rs
crates/erabi-db/src/repositories/datasets.rs
crates/erabi-db/src/repositories/reviews.rs
```

---

### Task 1: Build sanitized preview artifacts and deterministic node mapping

**Files:**
- Create: `crates/erabi-extraction/src/preview.rs`
- Create: `crates/erabi-extraction/src/mode.rs`
- Modify: `crates/erabi-extraction/src/lib.rs`
- Create: `crates/erabi-api/src/routes/extraction.rs`
- Test: `crates/erabi-extraction/tests/preview_security.rs`
- Test: `crates/erabi-extraction/tests/mode_detection.rs`
- Create: `tests/fixtures/extraction/hostile.html`
- Create: `tests/fixtures/extraction/article.html`
- Create: `tests/fixtures/extraction/listing.html`

**Interfaces:**
- Produces `PreviewDocument { html, nodes, base_url, artifact_hash }`.
- Produces deterministic `PreviewNode` IDs and bounding/selector metadata for UI selection.
- Produces `ModeSuggestion::{Document, Records}` with confidence/evidence and manual switch support.

- [ ] **Step 1: Add stable parsing/sanitizing dependencies**

Use `cargo add` at implementation time for the smallest compatible stable set, for example an HTML parser/selector crate plus `ammonia` or equivalent mature sanitizer. Add `url`, `serde`, `sha2`, `hex` as needed. Do not execute source-site JavaScript in Erabi preview generation.

- [ ] **Step 2: Write failing hostile-preview tests**

`hostile.html` must contain script tags, inline event handlers, `javascript:` URLs, forms, iframes/object/embed, meta refresh, unsafe base tags, SVG script, and top-navigation attempts. Assert sanitized preview contains none of those active capabilities while safe text/images remain.

```rust
#[test]
fn sanitized_preview_neutralizes_active_content() {
    let raw = include_str!("../../../tests/fixtures/extraction/hostile.html");
    let preview = erabi_extraction::PreviewBuilder::build(raw, "https://fixture.test/").unwrap();
    assert!(!preview.html.contains("<script"));
    assert!(!preview.html.contains("javascript:"));
    assert!(!preview.html.contains("onsubmit="));
    assert!(!preview.html.contains("<iframe"));
}
```

- [ ] **Step 3: Write failing deterministic-node and mode tests**

Build the same artifact twice and assert identical node IDs/order. Node IDs derive from artifact hash + document ordinal, not untrusted source IDs. Article fixture yields high-confidence Document; listing yields Records; ambiguous fixture returns lower confidence with evidence and allows manual switch without recrawl.

- [ ] **Step 4: Run RED**

```bash
cargo test -p erabi-extraction --test preview_security --test mode_detection
```

- [ ] **Step 5: Implement preview/mode services and endpoint**

Sanitize via explicit allowlist. Resolve relative safe `http`/`https` image/link references against final page URL, then make navigation inert for preview. Block `javascript:`, active forms, top navigation, external embeds, executable content. Preview response uses isolated route policy/CSP and `Cache-Control: private, no-store`.

`PreviewNode` stores deterministic node ID, tag, safe stable attributes/classes, text sample, parent/children IDs, candidate CSS selector, and enough geometry metadata for an overlay selection layer. Do not trust source-generated element IDs automatically when generating selectors.

- [ ] **Step 6: Run GREEN and commit**

```bash
cargo test -p erabi-extraction --test preview_security --test mode_detection
git add Cargo.lock crates/erabi-extraction crates/erabi-api tests/fixtures/extraction
 git commit -m "feat(extraction): build safe deterministic previews"
```

---

### Task 2: Define Page Type extraction contracts and implement typed extraction/normalization/validation

**Files:**
- Create: `crates/erabi-domain/src/extraction.rs`
- Create: `crates/erabi-domain/src/dataset.rs`
- Modify: `crates/erabi-domain/src/page_type.rs`
- Modify: `crates/erabi-domain/src/lib.rs`
- Create: `crates/erabi-extraction/src/extract.rs`
- Create: `crates/erabi-extraction/src/normalize.rs`
- Create: `crates/erabi-extraction/src/validate.rs`
- Test: `crates/erabi-extraction/tests/extraction_contract.rs`
- Test: `crates/erabi-domain/tests/shared_dataset_compatibility.rs`

**Interfaces:**
- Produces `ExtractionDefinition`, `FieldDefinition`, `FieldType`, `FieldValueSource`, `NormalizationRule`, `ValidationRule`, `UniqueKeyDefinition`, `DatasetMapping`.
- Produces `ExtractionEngine::extract(preview, definition)`.
- Produces raw/normalized candidate values and validation issues.

- [ ] **Step 1: Write failing type/value-source tests**

Exact MVP field types:

```rust
pub enum FieldType { Text, RichText, Number, Boolean, DateTime, Url, ImageUrl, RawHtml }

pub enum FieldValueSource {
    TextContent,
    InnerHtml,
    OuterHtml,
    Attribute { name: String },
    AbsoluteUrlAttribute { name: String },
    BooleanPresence,
}
```

Write fixture tests for title text, link absolute URL, image URL, boolean presence, date, number, RichText sanitation, and RawHtml explicit opt-in.

- [ ] **Step 2: Write failing selector/normalization/validation tests**

Selector preference tests assert stable semantic ID → semantic class → stable `data-*`/`aria-*` → semantic structure → positional fallback with fragility warning. Records Mode requires one root container and relative field selectors; Document Mode produces one logical record from document root.

Normalization tests store both raw and normalized values. Do not infer locale-specific currency/number behavior silently; schema config must specify needed parsing rules. Errors: missing required, invalid configured type, empty/duplicate unique key, required rule violation. Warnings: low coverage, optional image missing, fragile selector, outlier heuristic.

- [ ] **Step 3: Write failing shared-Dataset compatibility tests**

Two Page Types may map to one Dataset only when unique-key identity meaning/order, field types, required/optional semantics, and normalization contracts are compatible. Assert conflicting type or key blocks publish validation hook.

- [ ] **Step 4: Run RED**

```bash
cargo test -p erabi-extraction --test extraction_contract
cargo test -p erabi-domain --test shared_dataset_compatibility
```

- [ ] **Step 5: Implement extraction definition as Page Type-owned semantic config**

```rust
pub struct ExtractionDefinition {
    pub mode: ExtractionMode,
    pub container_selector: Option<String>,
    pub fallback_container_selectors: Vec<String>,
    pub structural_fingerprint: Option<String>,
    pub fields: Vec<FieldDefinition>,
    pub unique_key: Option<UniqueKeyDefinition>,
    pub dataset_mapping: Option<DatasetMapping>,
    pub comparison_policy: ComparisonPolicy,
}
```

Page Type draft mutations edit this definition inside Crawler Version semantic config. Publishing freezes it with the version. There is no separate global `SchemaVersion` entity/API.

Extraction engine returns candidate rows with per-field `raw_value`, `normalized_value`, selector/node evidence, validation issues, and coverage counts. Unsafe URL schemes are rejected when resolving URL fields.

- [ ] **Step 6: Run GREEN and commit**

```bash
cargo test -p erabi-extraction --test extraction_contract
cargo test -p erabi-domain --test shared_dataset_compatibility
git add Cargo.lock crates/erabi-domain crates/erabi-extraction
 git commit -m "feat(extraction): extract typed Page Type records"
```

---

### Task 3: Implement schema-drift diagnostics without production bypass

**Files:**
- Create: `crates/erabi-extraction/src/drift.rs`
- Modify: `crates/erabi-extraction/src/lib.rs`
- Modify: `crates/erabi-crawler/src/snapshot_health.rs`
- Create: `crates/erabi-api/src/routes/drift.rs`
- Test: `crates/erabi-extraction/tests/schema_drift.rs`
- Test: `crates/erabi-api/tests/schema_drift_actions.rs`

**Interfaces:**
- Produces `DriftReport`, `DriftSignal`, `DriftSeverity`.
- Adds extraction-health input to `SnapshotHealth`.
- Produces diagnostic action `CREATE_DRAFT_FIX`/review actions but no trust-restoring `USE_ANYWAY`.

- [ ] **Step 1: Write failing drift signal tests**

Fixtures cover missing container, missing required selector, required-field coverage drop, unexpected configured type, record-count anomaly relative to recent complete snapshots, unique-key extraction failure, structural fingerprint divergence.

```rust
#[test]
fn required_unique_key_failure_is_production_breaking_drift() {
    let report = erabi_extraction::test_support::drift_missing_unique_key();
    assert!(report.production_breaking());
    assert_eq!(report.code(), erabi_domain::ErrorCode::SchemaDrift);
}
```

- [ ] **Step 2: Write failing production action/health tests**

A Production Run with production-breaking drift must have incomplete snapshot health, preserve artifacts/diagnostics, and expose action leading to new Crawler Draft/Test Lab. Assert API response does not contain action code `USE_ANYWAY`. Test Run/Discovery/Quick Scrape may inspect report without mutating Published config or auto-approving data.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-extraction --test schema_drift
cargo test -p erabi-api --test schema_drift_actions
```

- [ ] **Step 4: Implement drift report and snapshot-health integration**

Drift detector compares current extraction evidence to the Published Page Type extraction contract and recent healthy complete snapshot statistics only where a comparison is meaningful. Record-count anomaly is diagnostic and must be combined with configured thresholds/evidence; do not treat any count change as automatic drift.

Production-breaking drift adds `HealthReason::SchemaDrift` to snapshot health. It cannot be cleared by a run-level override; only a later corrected Draft that passes validation/tests and becomes Published can restore normal production trust semantics.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-extraction --test schema_drift
cargo test -p erabi-api --test schema_drift_actions
git add crates/erabi-extraction crates/erabi-crawler crates/erabi-api
 git commit -m "feat(extraction): diagnose production schema drift"
```

---

### Task 4: Persist Datasets, reviews, candidates, validation, and immutable approved Record versions

**Files:**
- Create: `migrations/0006_curated_data.sql`
- Create: `crates/erabi-domain/src/review.rs`
- Create: `crates/erabi-domain/src/provenance.rs`
- Modify: `crates/erabi-domain/src/lib.rs`
- Create: `crates/erabi-db/src/repositories/datasets.rs`
- Create: `crates/erabi-db/src/repositories/reviews.rs`
- Create: `crates/erabi-api/src/routes/reviews.rs`
- Test: `crates/erabi-db/tests/review_persistence.rs`
- Test: `crates/erabi-api/tests/review_actions.rs`

**Interfaces:**
- Produces `Dataset`, `RecordVersion`, `RecordCandidate`, `Review`, `ReviewStatus`, `RecordStatus`, `ValidationIssue`.
- Produces optimistic-concurrency Draft edit, approve/reject/bulk actions, Close/Reopen.

- [ ] **Step 1: Define migration ownership and write failing immutable-version tests**

`0006_curated_data.sql` owns datasets, dataset fields/mappings metadata as needed, record identities, record_versions, candidate_values, validation_issues, reviews/review_items, provenance rows, and dataset_relationship definitions/references. It does not duplicate crawler or crawl-execution tables.

Test direct repository update of an Approved RecordVersion fails; manual edit creates a new Draft version linked to the Approved parent.

- [ ] **Step 2: Write failing review behavior tests**

Exact review states: `OPEN`, `CLOSED`, `CLOSED_WITH_UNRESOLVED_ITEMS`, `REOPENED`. Approve All Valid approves rows without ERROR, includes WARNING rows, skips ERROR rows, and reports approved/skipped/warning counts. Single reject reason optional; bulk reject requires non-empty reason. Closing unresolved review requires explicit confirmation payload and does not mutate record statuses.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-db --test review_persistence
cargo test -p erabi-api --test review_actions
```

- [ ] **Step 4: Implement transactional review/repository rules**

Draft cell/record edits use expected revision; on mismatch return 409 conflict and never silently overwrite. Approval transaction validates current revision/issues, inserts immutable Approved version, supersedes prior current pointer where applicable without mutating history, stores approval audit event, and preserves candidate/provenance evidence.

Review close/reopen is independent from record approval lifecycle. Rejected records remain persisted with evidence/provenance.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-db --test review_persistence
cargo test -p erabi-api --test review_actions
git add migrations/0006_curated_data.sql crates/erabi-domain crates/erabi-db crates/erabi-api
 git commit -m "feat(review): persist immutable curated record versions"
```

---

### Task 5: Implement semantic recrawl change detection and candidate generation

**Files:**
- Create: `crates/erabi-extraction/src/change_detection.rs`
- Create: `crates/erabi-db/src/repositories/candidates.rs`
- Test: `crates/erabi-extraction/tests/change_detection.rs`
- Test: `crates/erabi-db/tests/missing_candidate_guard.rs`

**Interfaces:**
- Produces `ChangeDecision::{Unchanged, NewCandidate, UpdatedCandidate, MissingCandidate, RestoredCandidate}`.
- Produces field-level candidate groups without auto-merge.

- [ ] **Step 1: Write failing normalized comparison tests**

Test normal comparison, whitespace-normalized comparison, canonical-URL comparison, and ignore-in-change-detection. Raw HTML-only differences with identical normalized fields do not create review work.

- [ ] **Step 2: Write failing field-level shared Dataset merge tests**

For same identity: absent approved field + candidate → enrichment candidate; same normalized value → unchanged; different value → preserve conflict candidate; multiple Page Types with different values preserve all candidates. Source-preference configuration ranks candidates but never auto-approves/discards.

- [ ] **Step 3: Write failing complete/partial missing guard tests**

```rust
#[tokio::test]
async fn partial_run_cannot_create_missing_candidate() {
    let fixture = erabi_db::test_support::approved_product_then_partial_recrawl().await;
    fixture.generate_candidates().await.unwrap();
    assert_eq!(fixture.count_status("MISSING_CANDIDATE").await, 0);
}
```

Repeat for Failed, Cancelled, Test Run, Discovery Preview, and production-breaking schema drift. Healthy complete Production snapshot with absent approved key creates MissingCandidate. Reappearing confirmed Deleted identity creates RestoredCandidate, not silent reactivation.

- [ ] **Step 4: Run RED**

```bash
cargo test -p erabi-extraction --test change_detection
cargo test -p erabi-db --test missing_candidate_guard
```

- [ ] **Step 5: Implement candidate generator gated by final SnapshotHealth**

Candidate generation takes immutable run type + final snapshot health + extracted normalized records + approved current versions. It immediately refuses missing detection unless `(run_type == ProductionRun && health == Complete)`. Duplicate identity collisions create validation/candidate conflicts and are never auto-merged.

- [ ] **Step 6: Run GREEN and commit**

```bash
cargo test -p erabi-extraction --test change_detection
cargo test -p erabi-db --test missing_candidate_guard
git add crates/erabi-extraction crates/erabi-db
 git commit -m "feat(review): generate safe recrawl candidates"
```

---

### Task 6: Implement durable field provenance and Dataset relationships

**Files:**
- Modify: `crates/erabi-domain/src/provenance.rs`
- Create: `crates/erabi-db/src/repositories/provenance.rs`
- Create: `crates/erabi-db/src/repositories/relationships.rs`
- Create: `crates/erabi-api/src/routes/provenance.rs`
- Test: `crates/erabi-api/tests/provenance_trace.rs`
- Test: `crates/erabi-db/tests/relationships.rs`

**Interfaces:**
- Produces `FieldProvenance` trace API.
- Produces field/key Dataset relationships and `UNRESOLVED_REFERENCE` diagnostics.

- [ ] **Step 1: Write failing end-to-end provenance trace test**

For one Approved field, load provenance and assert it identifies original URL, canonical URL, Source ID, Crawler/CrawlerVersion where applicable, Crawl Run, Page Type, transition/discovery path when relevant, artifact ID/hash, selector/node evidence, raw value, normalized value, transformations, extraction time.

- [ ] **Step 2: Write failing relationship diagnostics test**

Define `Reviews.product_id → Products.product_id`; a missing target surfaces `UNRESOLVED_REFERENCE` warning/reference state without deleting either record or blocking unrelated data by default. Do not add generic cascade/ORM/admin semantics.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-api --test provenance_trace
cargo test -p erabi-db --test relationships
```

- [ ] **Step 4: Implement provenance persistence/API and retention minimum**

Store field provenance when candidate extraction is persisted and link Approved RecordVersion fields to the exact candidate lineage chosen. API returns safe structured lineage and artifact reference; raw artifact download remains protected by Plan 03 routes. Minimum durable provenance metadata remains when ordinary retention later removes large artifact payloads: source/run/version/selector/value lineage, artifact hash/reference summary, audit history.

- [ ] **Step 5: Run full Plan 07 gate and commit**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p erabi-domain
cargo test -p erabi-extraction
cargo test -p erabi-db --test review_persistence --test missing_candidate_guard --test relationships
cargo test -p erabi-api --test review_actions --test schema_drift_actions --test provenance_trace
```

Expected: shared Listing+Detail Dataset never silently overwrites; drift requires Draft fix; duplicate identity/candidates do not auto-merge; complete-vs-partial missing semantics pass; Approved field trace is complete.

```bash
git add crates/erabi-domain crates/erabi-extraction crates/erabi-db crates/erabi-api
 git commit -m "feat(provenance): preserve field lineage and relationships"
```

## Plan 07 Gate

Do not start Plan 08 until Task 6 Step 5 passes from a clean checkout and `git status --short` is empty.
