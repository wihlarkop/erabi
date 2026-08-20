# Erabi Extraction and Schema System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the sanitized preview model, detect Document versus Records mode, version extraction schemas, detect drift, and extract typed normalized records from user-selected containers and fields.

**Architecture:** Raw crawled HTML is transformed into an isolated sanitized preview with stable node mapping. Extraction schemas remain drafts until approved, then become immutable versions containing relative CSS selectors, field types, normalization, validation, unique-key rules, and URL matching.

**Tech Stack:** Rust HTML parsing/sanitization, CSS selector evaluation, Serde, Turso, Axum preview APIs.

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

- **Depends on:** [05 Crawl4AI Integration and Crawl Orchestration](./05-crawl4ai-integration.md).
- **Produces:** Preview artifacts, node map, mode recommendation, schema lifecycle, drift detection, field suggestions, extraction, normalization, and validation results.
- **Gate:** Extraction gate: malicious-preview fixtures are neutralized and representative document/listing fixtures produce deterministic typed record previews with drift and validation coverage.
- **Execution order:** Complete every task in this file in numerical order and commit after each task. Do not begin the next plan until this gate passes.

## Focused File Map

```text
crates/erabi-extraction/
crates/erabi-domain/src/schemas/
crates/erabi-db/src/repositories/schemas/
crates/erabi-api/src/routes/extraction.rs
crates/erabi-api/src/routes/schemas.rs
tests/fixtures/websites/
tests/integration/extraction/
```

---

### Task 26: Build the Sanitized Preview Document and DOM Node Map

**Files:**
- Create: `crates/erabi-extraction/src/preview.rs`
- Create: `crates/erabi-extraction/src/dom.rs`
- Create: `crates/erabi-extraction/src/sanitize.rs`
- Create: `crates/erabi-api/src/routes/previews.rs`
- Modify: `crates/erabi-extraction/src/lib.rs`
- Test: `crates/erabi-extraction/tests/preview_security.rs`
- Test: `crates/erabi-extraction/tests/node_mapping.rs`

**Interfaces:**
- Produces: `PreviewDocument { html, nodes, base_url }`.
- Produces: stable internal node IDs mapping sanitized elements to original selectors/signatures.
- Enforces: no script, event handler, form submission, active embed, unsafe URL, or top navigation.

- [ ] **Step 1: Add stable HTML dependencies**

Run:

```bash
cargo add -p erabi-extraction scraper
cargo add -p erabi-extraction ammonia
cargo add -p erabi-extraction lol_html
cargo add -p erabi-extraction url
cargo add -p erabi-extraction serde --features derive
cargo add -p erabi-extraction serde_json
cargo add -p erabi-extraction thiserror
cargo add -p erabi-extraction sha2
cargo add -p erabi-extraction hex
cargo add -p erabi-extraction --path crates/erabi-domain erabi-domain
```

- [ ] **Step 2: Write hostile HTML security tests**

The fixture must include script tags, inline event handlers, `javascript:` URLs, forms, iframes, object/embed, meta refresh, SVG script, external styles, and base tags. Assert the sanitized result contains none of them, links cannot navigate the top frame, and safe text/images remain.

- [ ] **Step 3: Implement sanitization and URL resolution**

Sanitize using an allowlist. Resolve relative `href` and `src` against the final page URL, then allow only `http`, `https`, `data:image` under size policy, and `blob` only when generated by Erabi. Replace links with inert elements carrying the original target in an Erabi data attribute.

- [ ] **Step 4: Inject deterministic node IDs**

Walk elements in document order and assign IDs derived from artifact hash plus element ordinal, such as `n-000012`. Record tag name, stable classes/attributes, text sample, parent ID, child IDs, and candidate CSS selector. Never expose raw generated IDs from the source page as trusted identifiers.

- [ ] **Step 5: Implement preview endpoint**

`GET /api/v1/artifacts/{id}/preview` returns a sandboxable HTML document. It must use a separate route CSP, no auth token in query strings, and `Cache-Control: private, no-store`.

- [ ] **Step 6: Run tests**

Run: `cargo test -p erabi-extraction`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.lock crates/erabi-extraction crates/erabi-api
git commit -m "feat(extraction): create safe mapped page previews"
```
### Task 27: Detect Document Mode or Records Mode with a Manual Switch

**Files:**
- Create: `crates/erabi-extraction/src/mode_detection.rs`
- Create: `crates/erabi-api/src/routes/mode_detection.rs`
- Test: `crates/erabi-extraction/tests/mode_detection.rs`

**Interfaces:**
- Produces: `ModeSuggestion { recommended, confidence, evidence, candidate_containers }`.
- Produces: API action to switch extraction mode without recrawling.

- [ ] **Step 1: Add representative HTML fixtures**

Create fixture cases for article, documentation page, profile, product grid, forum comments, table/directory, and ambiguous mixed layout.

- [ ] **Step 2: Write expected detection tests**

Assert high-confidence article → Document, repeated product/comment/table rows → Records, and ambiguous content returns lower confidence with both options. Detection must be deterministic and local-only.

- [ ] **Step 3: Implement heuristics**

Score Document Mode from semantic `article/main`, one dominant text block, heading hierarchy, metadata, and low repeated-container evidence. Score Records Mode from repeated sibling structures, consistent child signatures, table rows, cards/list items, and repeated link/image/text patterns. Return evidence strings as stable codes, not generated prose.

- [ ] **Step 4: Implement manual mode switch**

Switching mode creates or updates an extraction Draft referencing the same raw artifact. It queues a new extraction preview only; it never starts a Crawl Run.

- [ ] **Step 5: Run tests**

Run: `cargo test -p erabi-extraction --test mode_detection`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/erabi-extraction crates/erabi-api tests/fixtures
git commit -m "feat(extraction): detect document and records modes"
```
### Task 28: Implement Extraction Schema Drafts, Versions, URL Matching, and Drift Detection

**Files:**
- Create: `crates/erabi-domain/src/schema.rs`
- Create: `crates/erabi-db/src/schemas.rs`
- Create: `crates/erabi-api/src/routes/schemas.rs`
- Create: `crates/erabi-extraction/src/drift.rs`
- Test: `crates/erabi-domain/tests/schema_versioning.rs`
- Test: `crates/erabi-extraction/tests/schema_drift.rs`

**Interfaces:**
- Produces: `ExtractionSchema`, `SchemaVersion`, `SchemaDefinition`, `FieldDefinition`, `UniqueKeyDefinition`.
- Produces: Draft autosave, approve immutable version, match URL pattern, preview before apply.
- Produces: `DriftReport` without automatic repair.

- [ ] **Step 1: Write immutable schema version tests**

Test that:

- editing a Draft updates the Draft revision;
- approving produces immutable version 1;
- editing approved data returns `Conflict` and creates version 2 Draft instead;
- URL match suggests but never silently applies a schema;
- unique-key settings are included in definition hash.

- [ ] **Step 2: Define schema structures**

A `SchemaDefinition` must include mode, container selector and fallback selectors, structural fingerprint, fields/types/value sources, required flags, normalization, validation, unique key, pagination settings, include/exclude selectors, URL pattern, and comparison-ignore flags.

- [ ] **Step 3: Implement Draft autosave with optimistic concurrency**

`PATCH /api/v1/schemas/{id}/draft` accepts `expected_revision`. On match, update definition JSON/hash and increment revision. On mismatch, return 409. Approval validates the schema against selected sample artifacts and inserts an immutable version row.

- [ ] **Step 4: Implement drift signals**

Detect:

- missing required selector;
- container not found;
- required field coverage drop;
- unexpected field type;
- record count anomaly relative to recent complete snapshots;
- unique-key extraction failures;
- structural fingerprint divergence.

Return `SCHEMA_DRIFT` and actions `REVIEW_SELECTORS`, `USE_ANYWAY`, `CANCEL`. Never mutate or repair the version automatically.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p erabi-domain --test schema_versioning
cargo test -p erabi-extraction --test schema_drift
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/erabi-domain crates/erabi-db crates/erabi-api crates/erabi-extraction
git commit -m "feat(schemas): version extraction rules and detect drift"
```
### Task 29: Implement Container Selection, Field Suggestions, Extraction, Normalization, and Validation

**Files:**
- Create: `crates/erabi-extraction/src/selector.rs`
- Create: `crates/erabi-extraction/src/suggestion.rs`
- Create: `crates/erabi-extraction/src/extract.rs`
- Create: `crates/erabi-extraction/src/normalize.rs`
- Create: `crates/erabi-extraction/src/validate.rs`
- Create: `crates/erabi-api/src/routes/extraction_preview.rs`
- Test: `crates/erabi-extraction/tests/extraction_contract.rs`
- Test: `crates/erabi-extraction/tests/normalization.rs`

**Interfaces:**
- Produces: relative CSS selector extraction from one selected container.
- Produces: MVP field types Text, RichText, Number, Boolean, DateTime, URL, ImageUrl, RawHtml.
- Produces: live paginated extraction preview with cancellation/debounce support at API level.

- [ ] **Step 1: Write selector quality and suggestion tests**

Assert selector preference order:

1. stable non-generated ID;
2. semantic class;
3. stable `data-*`/`aria-*` attribute;
4. semantic structure;
5. positional selector last with fragility warning.

Field suggestion fixtures must identify title, link, image, date, price, and description from local heuristics and report coverage such as 24/24.

- [ ] **Step 2: Implement one-container extraction**

Records Mode requires exactly one root container selector. Every field selector is relative to that container. Document Mode uses one logical document record and permits selectors from the document root. Do not add arbitrary cross-container selection in the MVP.

- [ ] **Step 3: Implement exact value sources**

```rust
pub enum ValueSource {
    TextContent,
    InnerHtml,
    OuterHtml,
    Attribute { name: String },
    AbsoluteUrlAttribute { name: String },
    BooleanPresence,
}
```

Resolve relative URLs against final page URL and reject unsafe schemes.

- [ ] **Step 4: Implement raw/normalized value pairs**

Store `RawValue` and `NormalizedValue` separately. Implement trim/collapse whitespace, locale-neutral number parsing with explicit schema configuration, Boolean presence, RFC3339/declared date formats, URL canonicalization, and safe RichText sanitation. Never infer locale-dependent currency silently.

- [ ] **Step 5: Implement validation severity**

Errors: missing required field, empty/duplicate unique key, invalid configured type, required rule violation. Warnings: low coverage, short description, missing optional image, outlier heuristic, fragile selector. Errors block approval; warnings do not require confirmation.

- [ ] **Step 6: Implement preview endpoint**

`POST /api/v1/extraction/preview` receives artifact ID plus temporary schema definition and returns sample records, total count, coverage, validation, and node mappings. Limit returned rows and paginate larger previews. Use a request generation ID so the frontend can discard stale responses.

- [ ] **Step 7: Run tests**

Run: `cargo test -p erabi-extraction`

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/erabi-extraction crates/erabi-api
git commit -m "feat(extraction): extract and validate structured records"
```
