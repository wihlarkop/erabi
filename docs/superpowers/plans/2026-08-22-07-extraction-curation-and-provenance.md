# Erabi Extraction, Curation, and Provenance Implementation Plan

> **For agentic workers:** Implement each extraction/curation capability end-to-end, then compile/check, add or update meaningful tests, run verification, and commit. Do not use failing-test-first or RED/GREEN sequencing by default.

**Goal:** Implement safe visual extraction owned by Page Types, typed normalization/validation, production schema-drift diagnostics, Dataset/Record review and immutable approvals, semantic recrawl candidates, relationships, and durable field-level provenance.

**Architecture:** Raw crawl artifacts remain immutable evidence. Page Type extraction definitions are semantic Crawler Version configuration; they do not have an independent global approval lifecycle. Curated Dataset/Record versions and candidate values are persisted separately from raw artifacts and never silently overwrite approved state.

**Tech Stack:** stable Rust, HTML parser/sanitizer crates selected via stable `cargo add`, CSS selector evaluation, Serde, Axum, Turso repositories.

**Spec:** `docs/specs/04-extraction-curation-and-provenance.md`, `docs/specs/03-discovery-graph-and-runs.md`, `docs/specs/08-ux-accessibility-and-verification.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

**Migration ownership:** `migrations/0006_curated_data.sql` for Datasets, record versions/candidates, validation, reviews, provenance, and relationships.

---

### Task 1: Safe preview artifacts and deterministic node mapping

**Files:** preview/sanitizer/mode modules, extraction preview API, hostile/article/listing fixtures/tests.

**Requirements:**

- Raw website HTML is untrusted and never inserted directly into the primary app DOM.
- Sanitization removes active scripts/inline handlers/forms/navigation escapes/unsafe schemes/active embeds/meta refresh and isolates preview rendering.
- Raw artifacts remain immutable evidence.
- Generate deterministic preview node IDs from controlled evidence, not untrusted element IDs or random runtime order.
- Produce enough safe node/selector/geometry metadata for visual highlighting plus keyboard/manual-selector workflow.
- Deterministic/local `ModeSuggestion::{Document, Records}` is advisory and switchable without recrawl.

**Verification:** hostile HTML security fixtures, deterministic node-map repeatability, mode suggestion/manual switch, safe URL/resource handling.

---

### Task 2: Page Type-owned extraction contracts and typed extraction

**Files:** `erabi-domain` extraction/Dataset contracts, PageType integration, extraction/normalize/validate modules, tests.

**MVP field types:** Text, RichText, Number, Boolean, DateTime, URL, ImageURL, RawHTML.

**Value sources:** text content, inner HTML, explicit outer HTML, attribute, resolved absolute URL attribute, Boolean presence.

**Requirements:**

- `ExtractionDefinition` belongs inside PageType semantic config and is frozen by Published CrawlerVersion.
- Support container selector/fallbacks, relative field selectors, normalization, validation, unique-key definition, Dataset mapping, comparison policy, structural fingerprint/evidence.
- Preserve raw and normalized values separately where representation changes.
- No silent locale/currency inference; configuration must make parsing behavior explicit.
- Shared Dataset compatibility checks enforce same identity meaning, compatible key order/semantics, field types, required/optional semantics, and normalization.
- Implement `ExtractionValidationContributor` and register it with Plan 05 `VersionValidationContributor` so incompatible extraction/unique-key/Dataset contracts block publish.
- Do not create a global `SchemaVersion`/Schema approval subsystem.

**Verification:** field-type/value-source fixtures, relative selector behavior, normalization/validation, unsafe URL rejection, shared Dataset compatibility, publication contributor integration.

---

### Task 3: Schema drift diagnostics and production trust semantics

**Files:** drift detector/report API, crawl snapshot-health integration, tests.

**Detect/report:** missing container, missing required selector, required coverage drop, type mismatch, record-count anomaly with meaningful baseline, unique-key extraction failure, structural divergence.

**Requirements:**

- Production-breaking drift marks run/snapshot non-complete and preserves diagnostics/artifacts/partial results.
- It cannot create trusted missing-record semantics.
- There is no generic production `USE_ANYWAY` action that restores trust.
- Corrective path is new Crawler Draft → validate/Test Lab → publish new version → later production run.
- Test/Discovery/Quick Scrape may inspect drift diagnostically without mutating Published config or auto-approving.

**Verification:** each drift signal, production incomplete health, no `USE_ANYWAY`, Draft-fix action, diagnostic-only non-production behavior.

---

### Task 4: Dataset/review persistence and immutable approved Record versions

**Files:** `migrations/0006_curated_data.sql`, Dataset/review/provenance domain types, repositories/routes/tests.

**Persist:** datasets, identity/record versions, candidate values, validation issues, reviews/items, provenance rows, relationships/references as defined by MVP.

**Requirements:**

- Validation `ERROR` blocks approval and cannot be overridden; `WARNING` remains approvable.
- Review lifecycle: `OPEN`, `CLOSED`, `CLOSED_WITH_UNRESOLVED_ITEMS`, `REOPENED`.
- Approved RecordVersion is immutable; edits create a new Draft version linked to prior history.
- Draft editing uses optimistic concurrency; conflict never silently overwrites another edit.
- Approve All Valid approves valid+warning rows, skips ERROR rows, and reports counts.
- Single rejection reason optional; bulk rejection requires non-empty reason.
- Closing unresolved review requires explicit confirmation and does not mutate record states.

**Verification:** repository immutability, optimistic conflict, approval severity, bulk actions, close/reopen, evidence/history preservation.

---

### Task 5: Semantic recrawl change detection and candidates

**Files:** change detection/candidate repository modules and tests.

**Healthy complete Production snapshot semantics:**

- existing key + same normalized values => unchanged;
- existing key + changed normalized values => `UPDATED_CANDIDATE`;
- new key => `NEW_CANDIDATE`;
- previously approved key absent => `MISSING_CANDIDATE`;
- previously confirmed deleted identity reappears => `RESTORED_CANDIDATE`.

**Requirements:**

- Compare normalized values using explicit field comparison policies.
- Field-level candidates preserve all conflicting values/source evidence; never auto-merge/silent overwrite.
- Partial/failed/cancelled/Test/Discovery/schema-drift-invalid runs never create `MISSING_CANDIDATE`.
- Source preference per field only ranks/recommends; it never auto-approves or discards alternatives.

**Verification:** unchanged/update/new/missing/restored, partial-run missing guard, duplicate/conflict non-auto-merge, preferred-source non-approval.

---

### Task 6: Field-level provenance and Dataset relationships

**Files:** provenance/relationship services/repositories/APIs and tests.

**Provenance must answer:** Source original/canonical URL, Crawler/Version, CrawlRun, PageType, discovery transition/path, artifact/hash/reference, selector/node evidence, raw value, normalized value, transformations, extraction time.

Retention cleanup may remove large artifacts but approved curated values retain minimum durable lineage metadata/audit evidence.

Dataset relationships use domain-specific key/field references; unresolved targets surface `UNRESOLVED_REFERENCE` without generic ORM/cascade behavior.

**Verification:** trace approved field back to source/artifact/version, provenance survives ordinary artifact cleanup metadata-wise, unresolved relationship warnings do not delete/block unrelated records.

---

## Plan 07 Gate

```bash
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Confirm safe preview isolation, PageType-owned extraction, Plan 05 validation contributor integration, shared Dataset compatibility, production drift trust rules, immutable approvals, candidate non-auto-merge, complete-vs-partial missing semantics, and field provenance all pass. Do not begin Plan 08 until the gate passes.
