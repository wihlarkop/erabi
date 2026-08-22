# Erabi Assets, Exports, and Backups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement safe asset handling, approved-only file exports and provenance bundles, atomic SQLite/Turso destination publication, conservative retention/storage protection, and verified backup/restore/integrity workflows.

**Architecture:** Filesystem payloads remain separate from internal Turso metadata. Export destinations are adapters with credentials/configuration independent from Erabi's internal application DB. Backups are versioned portable containers verified before mutation; restore runs as a controlled maintenance operation and falls back to Recovery Mode on invariant failure.

**Tech Stack:** stable Rust, Tokio streaming I/O, Serde/Serde JSON, CSV, ZIP/archive library, SHA-256, SQLite destination adapter, official `turso` crate for Turso destination access, mature high-level authenticated backup-encryption library selected at implementation time.

**Spec:** `docs/specs/07-exports-assets-retention-and-backups.md`, `docs/specs/05-system-architecture-and-persistence.md`, `docs/specs/06-security-reliability-and-operations.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Global Constraints

- Normal exports contain Approved records only.
- Debug exports/bundles are explicit and visually/auditably distinct from trusted standard exports.
- Export files are never silently overwritten.
- Internal Erabi DB and user destination DBs use separate adapters/configuration/tables.
- SQLite/Turso destination publication is atomic from the user's perspective; failed replacement preserves the prior valid target.
- Append/Upsert and PostgreSQL destinations are roadmap, not MVP.
- Default asset behavior stores URL + metadata; downloading is explicit.
- Untrusted downloaded files are never auto-executed/opened and archives are never auto-extracted.
- Automatic destructive retention cleanup is OFF by default.
- Automatic backup is OFF by default.
- Wrong backup password/corruption/incompatibility must not mutate active data.
- Never implement custom cryptography; use a mature stable authenticated encryption/container library behind an Erabi adapter.

## Focused File Map

```text
migrations/0007_assets_exports_backups.sql
crates/erabi-domain/src/export.rs
crates/erabi-domain/src/backup.rs
crates/erabi-export/src/assets.rs
crates/erabi-export/src/files.rs
crates/erabi-export/src/provenance_bundle.rs
crates/erabi-export/src/destination.rs
crates/erabi-export/src/sqlite.rs
crates/erabi-export/src/turso.rs
crates/erabi-export/src/retention.rs
crates/erabi-export/src/backup.rs
crates/erabi-export/src/integrity.rs
crates/erabi-db/src/repositories/exports.rs
crates/erabi-db/src/repositories/assets.rs
crates/erabi-db/src/repositories/backups.rs
crates/erabi-api/src/routes/assets.rs
crates/erabi-api/src/routes/exports.rs
crates/erabi-api/src/routes/backups.rs
crates/erabi-api/src/routes/integrity.rs
```

---

### Task 1: Persist asset/export/backup metadata and implement safe asset download

**Files:**
- Create: `migrations/0007_assets_exports_backups.sql`
- Create: `crates/erabi-export/src/assets.rs`
- Create: `crates/erabi-db/src/repositories/assets.rs`
- Create: `crates/erabi-api/src/routes/assets.rs`
- Modify: `crates/erabi-export/src/lib.rs`
- Modify: `crates/erabi-db/src/repositories/mod.rs`
- Modify: `crates/erabi-api/src/app.rs`
- Test: `crates/erabi-export/tests/asset_safety.rs`
- Test: `crates/erabi-api/tests/assets.rs`

**Interfaces:**
- Produces `Asset`, `AssetStatus::{UrlOnly, Downloading, Downloaded, Failed, Blocked}`.
- Produces `AssetDownloader::download(asset_id)` using the controlled filesystem layout from Plan 02.
- Produces asset metadata/download/remove-local-file routes.

- [ ] **Step 1: Write migration ownership and failing asset-safety tests**

`0007_assets_exports_backups.sql` owns only asset, export-run, saved-destination, backup, retention-policy, and integrity-run metadata required by this plan. It must not duplicate Dataset/review or crawl-execution tables.

Create tests:

```rust
#[tokio::test]
async fn downloaded_asset_cannot_escape_controlled_root() {
    let fixture = erabi_export::test_support::asset_fixture("../../evil.exe").await;
    let result = fixture.download().await;
    assert!(result.is_err() || fixture.local_path_is_inside_asset_root().await);
}

#[tokio::test]
async fn failed_download_leaves_no_partial_file() {
    let fixture = erabi_export::test_support::failing_stream_fixture().await;
    assert!(fixture.download().await.is_err());
    assert_eq!(fixture.partial_file_count().await, 0);
}
```

Also assert filename sanitation handles control characters, Windows reserved names, collisions, and misleading extensions; MIME/signature metadata is retained where practical.

- [ ] **Step 2: Run RED**

```bash
cargo test -p erabi-export --test asset_safety
cargo test -p erabi-api --test assets
```

Expected: compile failure for missing asset contracts.

- [ ] **Step 3: Implement explicit asset model and streaming downloader**

Add domain/API shape:

```rust
pub struct Asset {
    pub id: EntityId,
    pub source_id: EntityId,
    pub crawl_run_id: EntityId,
    pub original_url: url::Url,
    pub mime_type: Option<String>,
    pub size_bytes: Option<u64>,
    pub sha256: Option<String>,
    pub status: AssetStatus,
    pub local_artifact_id: Option<EntityId>,
}
```

Downloader accepts only an Asset ID, loads its stored URL, then streams into an application-owned safe path. Enforce request timeout, configured maximum bytes, safe redirects/schemes, and cancellation. Never accept a user-supplied local filesystem destination. Use Plan 02 atomic artifact writer and clean partial data on failure/cancel.

Removing a local file returns Asset status to `URL_ONLY` while preserving URL/source/run/hash history as appropriate. Serving downloaded files uses Plan 03 auth + attachment semantics + `nosniff`.

- [ ] **Step 4: Implement explicit direct-file download action**

The FileAsset Source path from Plan 06 creates/links an Asset URL-only record. `POST /api/v1/assets/{id}/download` is the only download action; merely pasting a direct-file URL must not auto-download an arbitrary file unless the product flow explicitly requested it.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-export --test asset_safety
cargo test -p erabi-api --test assets
git add migrations/0007_assets_exports_backups.sql crates/erabi-export crates/erabi-db crates/erabi-api
 git commit -m "feat(assets): download untrusted files safely"
```

---

### Task 2: Implement Approved-only file exports and provenance bundles

**Files:**
- Create: `crates/erabi-domain/src/export.rs`
- Create: `crates/erabi-export/src/files.rs`
- Create: `crates/erabi-export/src/provenance_bundle.rs`
- Create: `crates/erabi-db/src/repositories/exports.rs`
- Create: `crates/erabi-api/src/routes/exports.rs`
- Test: `crates/erabi-export/tests/file_exports.rs`
- Test: `crates/erabi-export/tests/provenance_bundle.rs`
- Test: `crates/erabi-api/tests/export_actions.rs`

**Interfaces:**
- Produces `ExportFormat::{Json, Jsonl, Csv, Markdown}` for file exports.
- Produces `ExportMode::{Standard, WithProvenance, DebugBundle}`.
- Produces `ExportManifest`, `ExportRun`, `FileExportService`.

- [ ] **Step 1: Add stable export dependencies**

Use `cargo add` for current stable `csv`, `sha2`, `hex`, and one stable ZIP-writing crate that supports streaming entries. Add only dependencies actually used by these formats.

- [ ] **Step 2: Write failing Approved-only export test**

```rust
#[tokio::test]
async fn standard_export_excludes_draft_and_rejected_versions() {
    let fixture = erabi_export::test_support::mixed_dataset_fixture().await;
    let jsonl = fixture.export_standard_jsonl().await.unwrap();
    let rows = jsonl.lines().collect::<Vec<_>>();
    assert_eq!(rows.len(), fixture.approved_record_count());
    assert!(!jsonl.contains("rejected-secret-fixture"));
}
```

Repeat semantically for JSON, CSV, and Markdown output. Export rows are drawn from current explicitly selected Approved Dataset/Record versions, not all DB rows.

- [ ] **Step 3: Write failing provenance-bundle verification test**

Bundle layout must contain:

```text
data/<generated-file>
provenance/fields.provenance.jsonl
manifest.json
checksums.sha256
```

Manifest includes export ID/time, Dataset/version identity, record count, Crawler/Page Type references where applicable, file list/checksums, application/export-format versions. Verify every checksum and provenance row references an exported Approved record/field lineage.

- [ ] **Step 4: Run RED**

```bash
cargo test -p erabi-export --test file_exports --test provenance_bundle
cargo test -p erabi-api --test export_actions
```

- [ ] **Step 5: Implement deterministic safe filenames and streaming exporters**

Generate names:

```text
{dataset-slug}-{yyyy-mm-dd}-{short-export-id}.{ext}
```

Never overwrite an existing file; Export Run UUID is part of collision safety. JSONL/CSV/provenance sidecars stream records rather than buffering complete large Datasets. Markdown uses a deterministic schema-aware representation documented by tests.

`DebugBundle` must require explicit mode selection and record an audit event because it may include selected logs/raw artifacts/diagnostics. Do not silently include raw evidence in Standard/WithProvenance modes.

- [ ] **Step 6: Run GREEN and commit**

```bash
cargo test -p erabi-export --test file_exports --test provenance_bundle
cargo test -p erabi-api --test export_actions
git add Cargo.lock crates/erabi-domain crates/erabi-export crates/erabi-db crates/erabi-api
 git commit -m "feat(export): publish approved data and provenance bundles"
```

---

### Task 3: Implement atomic SQLite and Turso destination adapters

**Files:**
- Create: `crates/erabi-export/src/destination.rs`
- Create: `crates/erabi-export/src/sqlite.rs`
- Create: `crates/erabi-export/src/turso.rs`
- Modify: `crates/erabi-export/src/lib.rs`
- Extend: `crates/erabi-db/src/repositories/exports.rs`
- Test: `crates/erabi-export/tests/destination_contract.rs`
- Test: `crates/erabi-export/tests/sqlite_atomic.rs`
- Test: `crates/erabi-export/tests/turso_contract.rs`

**Interfaces:**
- Produces `DestinationAdapter`, `SavedDestination`, `DestinationCapabilities`, `DestinationMode::{CreateNew, ReplaceAtomically}`, `DestinationReceipt`.

- [ ] **Step 1: Define adapter contract and write failing capability tests**

```rust
#[async_trait::async_trait]
pub trait DestinationAdapter: Send + Sync {
    async fn test(&self, destination: &SavedDestination) -> Result<DestinationCapabilities, ExportError>;
    async fn export(&self, request: DestinationExportRequest) -> Result<DestinationReceipt, ExportError>;
}
```

`SavedDestination` stores non-secret configuration plus environment-variable **name** for secret/token fields, never the secret value itself.

- [ ] **Step 2: Write failing SQLite atomic-replacement regression test**

Create a valid target table with version A, inject a failure while staging version B, and assert the original target table/data remain unchanged. Successful replacement validates staging schema/row count then performs an atomic transaction/swap appropriate to SQLite.

- [ ] **Step 3: Write failing Turso adapter contract tests against a deterministic test endpoint/local Turso mode**

Test Connection returns reachability/auth/create-write/staging/rename/drop/transaction capabilities and version info. Export start revalidates capabilities. Failed replacement never reports partial staging as published success.

- [ ] **Step 4: Run RED**

```bash
cargo test -p erabi-export --test destination_contract --test sqlite_atomic --test turso_contract
```

- [ ] **Step 5: Implement adapters with separate internal/destination boundaries**

Use a stable SQLite library selected with `cargo add` for SQLite destination files and the official `turso` crate for Turso destinations. Do not pass the internal Erabi `Database` repository connection into destination adapters.

Create New is default: create unique/versioned physical table → stream typed columns → validate → return published receipt. Replace Atomically: create staging table → stream/validate → transactional swap/rename using detected capabilities → cleanup old/staging safely. Preserve prior valid target on failure.

For shared SQLite/Turso DB organization, derive deterministic safe table namespace/prefix such as `{collection_slug}__{dataset_name}` and persist physical mapping in Export metadata. Use real typed columns; an optional raw/debug JSON column cannot replace useful typed columns.

- [ ] **Step 6: Run GREEN and commit**

```bash
cargo test -p erabi-export --test destination_contract --test sqlite_atomic --test turso_contract
git add Cargo.lock crates/erabi-export crates/erabi-db
 git commit -m "feat(export): add atomic database destinations"
```

---

### Task 4: Implement retention preview and disk-pressure measurement without automatic deletion

**Files:**
- Create: `crates/erabi-export/src/retention.rs`
- Create: `crates/erabi-export/src/storage.rs`
- Create: `crates/erabi-api/src/routes/retention.rs`
- Modify: `crates/erabi-jobs/src/admission.rs`
- Test: `crates/erabi-export/tests/retention.rs`
- Test: `crates/erabi-export/tests/storage_pressure.rs`
- Test: `crates/erabi-api/tests/retention_actions.rs`

**Interfaces:**
- Produces `RetentionPolicy`, `RetentionPreview`, `StorageState::{Healthy, Warning, Critical}`.
- Supplies measured storage state to Plan 04 admission policy.

- [ ] **Step 1: Write failing retention-preview test**

Given mixed raw artifacts, downloaded assets, Approved records/provenance, and audit rows, preview must report removable file count/bytes/categories and retained evidence. Assert ordinary artifact cleanup never schedules deletion of Approved curated data, minimum provenance metadata, audit events, or lifecycle/version metadata.

- [ ] **Step 2: Write failing OFF-by-default test**

A fresh settings DB has no automatic destructive cleanup schedule. Merely entering Critical storage state must not delete any user file; it only changes admission state and surfaces actions.

- [ ] **Step 3: Write failing threshold/admission integration test**

Inject filesystem free-space metrics around Warning/Critical thresholds and assert Plan 04 blocks artifact-heavy Crawl/Asset/Export/Backup work only at Critical while diagnostics/integrity/review remain available.

- [ ] **Step 4: Run RED**

```bash
cargo test -p erabi-export --test retention --test storage_pressure
cargo test -p erabi-api --test retention_actions
```

- [ ] **Step 5: Implement preview-first cleanup service**

Retention selection is explicit (indefinite, N days, latest N runs, approved/reference-required variants as admitted by spec/settings). Manual cleanup requires a preview token/hash tied to current candidate set; execution rejects stale preview when underlying eligible set changed materially. Cleanup deletes only selected removable payloads and updates metadata/history transactionally where practical.

- [ ] **Step 6: Run GREEN and commit**

```bash
cargo test -p erabi-export --test retention --test storage_pressure
cargo test -p erabi-api --test retention_actions
cargo test -p erabi-jobs --test storage_admission
git add crates/erabi-export crates/erabi-api crates/erabi-jobs
 git commit -m "feat(storage): preview retention and enforce disk pressure"
```

---

### Task 5: Implement versioned `.erabi-backup`, verification, restore, and integrity operations

**Files:**
- Create: `crates/erabi-domain/src/backup.rs`
- Create: `crates/erabi-export/src/backup.rs`
- Create: `crates/erabi-export/src/integrity.rs`
- Create: `crates/erabi-db/src/repositories/backups.rs`
- Create: `crates/erabi-api/src/routes/backups.rs`
- Create: `crates/erabi-api/src/routes/integrity.rs`
- Test: `crates/erabi-export/tests/backup_roundtrip.rs`
- Test: `crates/erabi-export/tests/backup_encryption.rs`
- Test: `crates/erabi-api/tests/restore_recovery.rs`

**Interfaces:**
- Produces `BackupType::{DatabaseOnly, Full}`, `BackupManifest`, `BackupReceipt`, `BackupVerifier`, `RestoreService`, `IntegrityReport`.
- Produces versioned outer extension `*.erabi-backup`.

- [ ] **Step 1: Write failing backup-format roundtrip tests**

Database Only contains DB snapshot + migration/schema metadata + settings + crawler/domain config + version/approval/audit state + manifest/checksums. Full adds selected/all artifact index/payloads, logs, screenshots, assets according to explicit selection.

Test create → verify → inspect manifest without restoring. Cancelled/failed backup must not leave a file that `BackupVerifier` recognizes as valid.

- [ ] **Step 2: Write failing encryption/wrong-password tests**

Before choosing a crate, verify a mature stable high-level authenticated encryption/passphrase container library is compatible with the current Rust toolchain. Add it with `cargo add` and wrap it behind:

```rust
pub trait BackupCipher {
    fn encrypt(&self, plaintext: &[u8], password: &secrecy::SecretString) -> Result<Vec<u8>, BackupError>;
    fn decrypt(&self, ciphertext: &[u8], password: &secrecy::SecretString) -> Result<Vec<u8>, BackupError>;
}
```

Do not design primitives/KDF/nonces yourself. Test wrong password and one-byte corruption fail authentication before any restore mutation. Password never appears in DB/log/metadata.

- [ ] **Step 3: Write failing restore safety test**

```rust
#[tokio::test]
async fn invalid_backup_never_mutates_active_database() {
    let fixture = erabi_export::test_support::active_system_with_invalid_backup().await;
    let before = fixture.active_state_hash().await;
    assert!(fixture.restore().await.is_err());
    assert_eq!(fixture.active_state_hash().await, before);
}
```

Also test a restore failure after replacement becomes unavoidable enters Recovery Mode rather than Ready; successful restore runs migrations only when compatibility policy explicitly supports it and then deep/lightweight integrity checks before returning Ready.

- [ ] **Step 4: Run RED**

```bash
cargo test -p erabi-export --test backup_roundtrip --test backup_encryption
cargo test -p erabi-api --test restore_recovery
```

- [ ] **Step 5: Implement maintenance-mode restore sequence**

Exact restore sequence:

```text
stop accepting mutations/new jobs
→ settle/cancel/checkpoint active work
→ verify format/version/checksums/password/compatibility
→ optionally create safety snapshot of current state
→ restore into staging locations
→ apply explicitly supported migrations
→ integrity-check staged result
→ atomically activate restored DB/artifact roots where platform permits
→ rebuild queue/runtime state
→ Ready only if healthy; otherwise Recovery Mode
```

Deep integrity includes DB engine diagnostics where available, immutable-version/current-pointer invariants, run/snapshot references, selected artifact hash existence, audit consistency, and backup readability. Automatic deep-check scheduling and automatic backup remain OFF by default; user-triggered operations are MVP.

- [ ] **Step 6: Run full Plan 08 gate and commit**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p erabi-export
cargo test -p erabi-api --test assets --test export_actions --test retention_actions --test restore_recovery
cargo test -p erabi-jobs --test storage_admission
```

Expected: Approved-only exports, provenance bundle checksum verification, atomic destination failure preservation, explicit safe asset download, retention OFF by default, critical storage without auto-delete, and backup → verify → restore all pass.

```bash
git add Cargo.lock crates/erabi-domain crates/erabi-export crates/erabi-db crates/erabi-api crates/erabi-jobs
 git commit -m "feat(backup): verify backup restore and integrity workflows"
```

## Plan 08 Gate

Do not start Plan 09 until Task 5 Step 6 passes from a clean checkout and `git status --short` is empty.
