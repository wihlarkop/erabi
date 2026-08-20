# Erabi Assets and Exports Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Discover and safely download selected assets, export approved data to files, build provenance bundles, test saved destinations, and publish SQLite or Turso tables atomically.

**Architecture:** Assets are untrusted, streamed to isolated storage, and never opened automatically. Export adapters consume immutable approved dataset versions; standard exports stay clean, provenance exports use sidecars and manifests, and database destinations publish through validated staging tables.

**Tech Stack:** Rust streaming I/O, ZIP and SHA-256 tooling, CSV/JSON/JSONL/Markdown writers, SQLite/Turso destination adapters.

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

- **Depends on:** [07 Review, Versioning, and Provenance](./07-review-versioning-and-provenance.md).
- **Produces:** Asset service, file export service, provenance ZIP bundles, saved destination capability tests, and atomic database exports.
- **Gate:** Portability A: asset safety fixtures, approved-only exports, checksums, provenance correlation, destination capability failures, and atomic replacement rollback tests pass.
- **Execution order:** Complete every task in this file in numerical order and commit after each task. Do not begin the next plan until this gate passes.

## Focused File Map

```text
crates/erabi-artifacts/src/assets/
crates/erabi-export/
crates/erabi-api/src/routes/assets.rs
crates/erabi-api/src/routes/exports.rs
crates/erabi-api/src/routes/destinations.rs
crates/erabi-db/src/repositories/exports/
crates/erabi-db/src/repositories/destinations/
tests/integration/assets/
tests/integration/exports/
```

## Shared Contract Produced by This Plan

```rust
#[async_trait::async_trait]
pub trait DestinationAdapter: Send + Sync {
    async fn test(
        &self,
        destination: &SavedDestination,
    ) -> Result<DestinationCapabilities, ExportError>;

    async fn export(&self, request: ExportRequest) -> Result<ExportReceipt, ExportError>;
}
```

---

### Task 33: Discover and Safely Download Selected Assets

**Files:**
- Create: `crates/erabi-domain/src/asset.rs`
- Create: `crates/erabi-db/src/assets.rs`
- Create: `crates/erabi-jobs/src/handlers/download_asset.rs`
- Create: `crates/erabi-api/src/routes/assets.rs`
- Test: `crates/erabi-jobs/tests/asset_download.rs`
- Test: `crates/erabi-api/tests/assets_api.rs`

**Interfaces:**
- Produces: Assets tab data and `Download Selected` jobs.
- Enforces: URL+metadata only by default; explicit download; untrusted file handling.
- Asset approval follows source/document approval in MVP.

- [ ] **Step 1: Write unsafe download tests**

Cover path traversal filename, absolute path, Windows reserved name, Unicode control characters, MIME/extension mismatch, oversized response, redirect to unsafe scheme, partial download cancellation, ZIP not auto-extracted, and executable warning/explicit confirmation.

- [ ] **Step 2: Implement asset discovery**

From extracted fields and page links, persist original URL, resolved URL, proposed filename, declared MIME, known size, source node/provenance, and status `URL_ONLY`. Do not download automatically.

- [ ] **Step 3: Implement streaming selected downloads**

Use Reqwest/Rustls streaming with:

- configured per-file and per-run byte limits;
- safe redirect policy;
- MIME sniffing from initial bytes plus response header;
- same atomic partial-file behavior as artifacts;
- SHA-256 while streaming;
- cooperative cancellation;
- cleanup of partial files;
- statuses `DOWNLOADING`, `DOWNLOADED`, `FAILED`, `BLOCKED`.

- [ ] **Step 4: Implement browser download endpoint**

Serve downloaded assets only by Asset ID with `Content-Disposition: attachment`, safe filename, `nosniff`, and auth. Safe image preview uses a controlled endpoint; other types are never embedded automatically.

- [ ] **Step 5: Implement local removal without URL deletion**

Removing a downloaded file returns status to `URL_ONLY`, preserves URL/provenance/history/hash metadata, and writes an audit event.

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p erabi-jobs --test asset_download
cargo test -p erabi-api --test assets_api
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/erabi-domain crates/erabi-db crates/erabi-jobs crates/erabi-api
git commit -m "feat(assets): safely download selected page assets"
```
### Task 34: Implement Approved-Only File Exports

**Files:**
- Create: `crates/erabi-export/src/model.rs`
- Create: `crates/erabi-export/src/selection.rs`
- Create: `crates/erabi-export/src/file/json.rs`
- Create: `crates/erabi-export/src/file/jsonl.rs`
- Create: `crates/erabi-export/src/file/csv.rs`
- Create: `crates/erabi-export/src/file/markdown.rs`
- Create: `crates/erabi-export/src/file/sqlite.rs`
- Create: `crates/erabi-export/src/file/mod.rs`
- Modify: `crates/erabi-export/src/lib.rs`
- Create: `crates/erabi-jobs/src/handlers/export_dataset.rs`
- Create: `crates/erabi-api/src/routes/exports.rs`
- Test: `crates/erabi-export/tests/file_exports.rs`

**Interfaces:**
- Produces: JSON, JSONL, CSV, Markdown, and SQLite exports.
- Enforces: normal exports include approved Record Versions only.
- Produces: debug selection only through explicit mode.

- [ ] **Step 1: Add stable export dependencies**

Run:

```bash
cargo add -p erabi-export async-trait
cargo add -p erabi-export serde --features derive
cargo add -p erabi-export serde_json
cargo add -p erabi-export csv
cargo add -p erabi-export rusqlite --features bundled
cargo add -p erabi-export tokio --features fs,io-util
cargo add -p erabi-export thiserror
cargo add -p erabi-export sha2
cargo add -p erabi-export hex
cargo add -p erabi-export --path crates/erabi-domain erabi-domain
cargo add -p erabi-export --path crates/erabi-artifacts erabi-artifacts
```

- [ ] **Step 2: Write format contract tests from one deterministic dataset**

Create a fixture with approved, Draft, and rejected records. Assert:

- Standard JSON/JSONL/CSV/Markdown/SQLite contain only approved records;
- field order follows Schema Version order;
- UTF-8 and newlines round-trip;
- JSONL is one valid JSON object per line;
- CSV escaping is valid;
- Markdown has stable headings/table layout;
- SQLite has real typed columns and optional debug raw JSON only in Debug mode;
- output is streamed and does not require loading every row into memory.

- [ ] **Step 3: Define export request and selection modes**

```rust
pub enum ExportSelection { StandardApproved, WithProvenance, Debug }
pub enum FileExportFormat { Json, JsonLines, Csv, Markdown, Sqlite }
pub struct ExportRequest {
    pub id: EntityId,
    pub dataset_version_id: EntityId,
    pub selection: ExportSelection,
    pub format: FileExportFormat,
    pub include_downloaded_assets: bool,
}
```

- [ ] **Step 4: Implement deterministic automatic filenames**

Use:

```text
{dataset-slug}-{yyyy-mm-dd}-{first-6-hex-of-export-id}.{extension}
```

Sanitize for Windows/macOS/Linux. Users cannot customize names in MVP. Never overwrite an existing file; collision adds the full Export ID.

- [ ] **Step 5: Implement streaming exporters**

Each exporter consumes an async/paginated approved-record reader. Convert field types deterministically. CSV nested/raw complex values use compact JSON strings. SQLite export creates a new file through `rusqlite`, defines typed columns from Schema Version, inserts in transactions, validates row count, closes, hashes, then atomically publishes.

- [ ] **Step 6: Implement export job and API**

`POST /api/v1/exports` validates the Dataset Version and enqueues a job. `GET /api/v1/exports/{id}` returns status/progress/history. Download endpoint uses Artifact ID and attachment headers. Export failure retains error summary and removes partial output.

- [ ] **Step 7: Run tests**

Run: `cargo test -p erabi-export --test file_exports`

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add Cargo.lock crates/erabi-export crates/erabi-jobs crates/erabi-api
git commit -m "feat(exports): generate approved file exports"
```
### Task 35: Build Provenance ZIP Bundles, Manifests, Checksums, and Optional Assets

**Files:**
- Create: `crates/erabi-export/src/bundle.rs`
- Create: `crates/erabi-export/src/manifest.rs`
- Create: `crates/erabi-export/src/provenance.rs`
- Test: `crates/erabi-export/tests/provenance_bundle.rs`

**Interfaces:**
- Produces: ZIP bundle for With Provenance and Debug modes.
- Produces: JSONL field sidecar, `manifest.json`, `checksums.sha256`, optional downloaded asset files.
- Keeps: Standard export as clean data-only file.

- [ ] **Step 1: Add stable archive dependencies**

Run:

```bash
cargo add -p erabi-export zip
cargo add -p erabi-export tempfile
cargo add -p erabi-export walkdir
```

- [ ] **Step 2: Write bundle structure tests**

For With Provenance without assets, assert exact entries:

```text
data/products.csv
provenance/products.provenance.jsonl
manifest.json
checksums.sha256
```

With `include_downloaded_assets=true`, assert downloaded files appear beneath `assets/`, URL-only assets remain references only, missing files are reported in manifest, and every included file has a checksum entry.

- [ ] **Step 3: Implement streaming provenance JSONL**

One line per field provenance record, including record ID, field, source URL, selector, raw/normalized value, transformations, Schema Version, Crawl Run, artifact ID/hash, and extraction time. Keep field values out of the top-level manifest.

- [ ] **Step 4: Implement versioned manifest**

Include export manifest version, Erabi version, export ID/time, Collection/Dataset identities and names, Dataset/Schema Versions, Crawl Runs, selection/format, approved record count, warning counts, included asset counts, omitted/missing asset reports, and ordered file entries with bytes/checksums.

- [ ] **Step 5: Implement ZIP publication**

Build in a temporary file, finalize all entries, close archive, compute final checksum, then atomically move it to exports. Clean partial ZIP on cancellation/failure. Never store secret references or tokens.

- [ ] **Step 6: Run tests**

Run: `cargo test -p erabi-export --test provenance_bundle`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.lock crates/erabi-export
git commit -m "feat(exports): bundle provenance and checksums"
```
### Task 36: Implement Saved Destinations and Capability Testing

**Files:**
- Create: `crates/erabi-domain/src/destination.rs`
- Create: `crates/erabi-db/src/destinations.rs`
- Create: `crates/erabi-export/src/destination/mod.rs`
- Create: `crates/erabi-api/src/routes/destinations.rs`
- Test: `crates/erabi-api/tests/destination_capabilities.rs`

**Interfaces:**
- Produces: Saved Destination CRUD without secret values.
- Produces: `Test Connection` for SQLite and Turso destinations.
- Persists: token environment variable name, not token contents.

- [ ] **Step 1: Write secret reference tests**

Create a Turso destination with `token_env_var: "TURSO_EXPORT_TOKEN_SCANDAL"`. Assert database rows and API responses contain only the variable name/status, never the resolved token. Diagnostic and Debug formatting must also omit it.

- [ ] **Step 2: Define destination models**

Support:

```rust
pub enum DestinationType { Sqlite, Turso }
pub enum DatabaseLayout { DedicatedPerCollection, SharedWithPrefix }
pub struct SavedDestination {
    pub id: EntityId,
    pub name: String,
    pub destination_type: DestinationType,
    pub config: serde_json::Value,
    pub token_env_var: Option<String>,
    pub last_test: Option<DestinationTestSummary>,
}
```

Dedicated database per Collection is the default layout. Shared database layout uses table name prefix `{collection_slug}__{dataset_slug}` and must be sanitized/length-limited; Erabi metadata tables map each Collection/Dataset/export to its generated table.

- [ ] **Step 3: Implement capability checks**

Test endpoint reachability, token presence/authentication, create/write/rename/drop permissions using uniquely named temporary staging objects, transaction behavior, detected engine/version, and basic latency. Always clean temporary objects. Cache only a timestamped summary and revalidate immediately before export.

- [ ] **Step 4: Define `DestinationAdapter`**

Keep exact fixed trait contract. The adapter receives resolved secret values in-memory from a dedicated secret resolver, but `SavedDestination` itself contains only references. Ensure errors redact URLs with sensitive queries.

- [ ] **Step 5: Run tests**

Run: `cargo test -p erabi-api --test destination_capabilities`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/erabi-domain crates/erabi-db crates/erabi-export crates/erabi-api
git commit -m "feat(destinations): save and test export targets"
```
### Task 37: Implement Atomic SQLite and Turso Database Exports

**Files:**
- Create: `crates/erabi-export/src/destination/sqlite.rs`
- Create: `crates/erabi-export/src/destination/turso.rs`
- Create: `crates/erabi-export/src/destination/naming.rs`
- Test: `crates/erabi-export/tests/database_export.rs`

**Interfaces:**
- Produces: `CreateNew` default and `ReplaceAtomically` modes.
- Does not implement Append or Upsert.
- Validates row count, schema, constraints, and destination capabilities before publish.

- [ ] **Step 1: Enable official Turso sync support for destination export**

Run: `cargo add -p erabi-export turso --features sync`.

This does not change the application database contract. Turso destination export uses a temporary local synced database and explicit push as supported by the official crate.

- [ ] **Step 2: Write atomic failure tests**

For SQLite and a fake/mock Turso sync boundary, prove:

- Create New generates a unique versioned table and leaves old tables untouched;
- Replace writes to staging, validates, then swaps;
- injected failure halfway leaves original table unchanged;
- staging is cleaned after failure/startup recovery;
- successful row count equals approved selection count;
- no table is marked completed until destination verification passes.

- [ ] **Step 3: Implement real table schemas**

Map field types to database types. Create an Erabi metadata table mapping Collection, Dataset, Dataset Version, Schema Version, table name, Export Run, record count, and exported time. Do not store the entire dataset only as opaque JSON.

- [ ] **Step 4: Implement Create New**

Generate `{base}__v{dataset_version}` plus short Export ID when needed. Create, stream insert, validate, write metadata, commit/push, and return receipt.

- [ ] **Step 5: Implement Replace Atomically**

Create `{target}__staging__{short-id}`. After full validation, swap inside one transaction where supported. For sync-based Turso, perform local transaction, push, and verify remote result before marking complete. If exact rename atomicity is unavailable, capability test must disable Replace rather than emulate unsafe partial replacement.

- [ ] **Step 6: Run tests**

Run: `cargo test -p erabi-export --test database_export`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.lock crates/erabi-export
git commit -m "feat(exports): publish database exports atomically"
```
