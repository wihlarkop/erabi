# Erabi Retention, Backup, Recovery, and Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Archive/Trash/retention semantics, versioned encrypted `.erabi-backup` files, safe restore, diagnostics, integrity checks, disk-pressure protection, settings APIs, and metadata search.

**Architecture:** Destructive actions are conservative and audited; automatic cleanup remains off by default. Backup and restore use a versioned portable format with verification before mutation, while diagnostics and integrity services can place Erabi into read-only Recovery Mode instead of risking corrupted writes.

**Tech Stack:** Rust, Turso snapshots, filesystem archives, stable password-based encryption crates, SHA-256, Axum operations APIs.

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

- **Depends on:** [08 Assets and Exports](./08-assets-and-exports.md).
- **Produces:** Retention planner, Trash lifecycle, `.erabi-backup` tooling, restore coordinator, integrity/doctor services, disk safety stop, settings API, and metadata search.
- **Gate:** Portability B: no implicit deletion, encrypted and unencrypted backup verification, failed-restore non-mutation, integrity failure Recovery Mode, disk critical blocking, and search correctness tests pass.
- **Execution order:** Complete every task in this file in numerical order and commit after each task. Do not begin the next plan until this gate passes.

## Focused File Map

```text
crates/erabi-domain/src/retention/
crates/erabi-cli/src/backup/
crates/erabi-cli/src/doctor/
crates/erabi-cli/src/integrity/
crates/erabi-api/src/routes/backups.rs
crates/erabi-api/src/routes/diagnostics.rs
crates/erabi-api/src/routes/settings.rs
crates/erabi-api/src/routes/search.rs
tests/integration/backup/
tests/integration/operations/
```

---

### Task 38: Implement Retention, Archive, Trash, Permanent Deletion, and Export File Lifecycle

**Files:**
- Create: `crates/erabi-domain/src/retention.rs`
- Create: `crates/erabi-db/src/retention.rs`
- Create: `crates/erabi-jobs/src/handlers/retention_cleanup.rs`
- Create: `crates/erabi-api/src/routes/trash.rs`
- Create: `crates/erabi-api/src/routes/retention.rs`
- Modify: `crates/erabi-api/src/routes/exports.rs`
- Test: `crates/erabi-jobs/tests/retention_cleanup.rs`
- Test: `crates/erabi-api/tests/trash_workflow.rs`

**Interfaces:**
- Produces: Archive/reactivate, Move to Trash/restore, explicit permanent deletion, and retention preview/execute.
- Enforces: automatic cleanup off by default and no low-storage automatic deletion.
- Preserves: minimum permanent provenance/audit/summary metadata.

- [ ] **Step 1: Write lifecycle tests**

Assert:

- Archive hides Source from active views but retains all data;
- Trash disables related queued work and allows restore;
- default Trash retention is 30 days but auto-cleanup is off;
- permanent delete requires exact Source-name confirmation and impact token;
- referenced approved/provenance data produces warning or blocks according to impact policy;
- export file deletion changes status to `FILE_REMOVED` but keeps Export Run/summary/checksum/audit;
- deleted export cannot regenerate in MVP.

- [ ] **Step 2: Implement retention policy types**

Support indefinite, N days, latest N runs, and approved-versions-only for detailed artifacts/logs. Export and Trash have separate policies. Resolution follows built-in/global/Collection. A cleanup plan lists exact file count/bytes and permanent metadata retained.

- [ ] **Step 3: Implement two-phase cleanup**

`POST /retention/preview` returns a signed/hashed plan based on current state. `POST /retention/execute` requires that plan hash and rechecks references. Delete files first only when database can record cleanup outcome safely; use resumable cleanup records for partial failures.

- [ ] **Step 4: Implement permanent deletion impact calculation**

Enumerate Sources, runs, artifacts, schemas owned only by Source, Dataset/Record versions, assets, export references, and provenance. Preserve append-only tombstone audit with entity ID/name/hash/time but no deleted content. Never delete a shared Schema Version or active destination because one Source is deleted.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p erabi-jobs --test retention_cleanup
cargo test -p erabi-api --test trash_workflow
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/erabi-domain crates/erabi-db crates/erabi-jobs crates/erabi-api
git commit -m "feat(retention): archive trash and clean data safely"
```
### Task 39: Implement Versioned `.erabi-backup` Creation, Verification, Encryption, and Restore

**Files:**
- Create: `crates/erabi-export/src/backup/model.rs`
- Create: `crates/erabi-export/src/backup/archive.rs`
- Create: `crates/erabi-export/src/backup/encryption.rs`
- Create: `crates/erabi-export/src/backup/verify.rs`
- Create: `crates/erabi-export/src/backup/restore.rs`
- Create: `crates/erabi-export/src/backup/mod.rs`
- Create: `crates/erabi-jobs/src/handlers/backup.rs`
- Create: `crates/erabi-api/src/routes/backups.rs`
- Create: `crates/erabi-cli/src/commands/backup.rs`
- Test: `crates/erabi-export/tests/backup_roundtrip.rs`
- Test: `crates/erabi-api/tests/restore_safety.rs`

**Interfaces:**
- Produces: Database Only and Full Backup in one `.erabi-backup` file format.
- Produces: optional password encryption, verify, download, restore, and delete.
- Enforces: automatic backup off by default; wrong password/corruption never changes active data.

- [ ] **Step 1: Add stable archive and encryption libraries**

Run:

```bash
cargo add -p erabi-export tar
cargo add -p erabi-export zstd
cargo add -p erabi-export age
cargo add -p erabi-export zeroize --features derive
cargo add -p erabi-export secrecy
cargo add -p erabi-export tempfile
```

Use `age` passphrase encryption rather than designing cryptography.

- [ ] **Step 2: Write backup round-trip tests**

Test Database Only and Full backups, encrypted and unencrypted. Assert exact format version, manifest/checksums, database state restoration, artifact inclusion only in Full, wrong password rejection, corrupt checksum rejection, partial file cleanup, and format-version incompatibility error.

- [ ] **Step 3: Define the container format**

A small unencrypted fixed header contains only magic `ERABIBKP`, format version, encryption flag, and payload length. The payload is a compressed tar; when password encryption is enabled, encrypt the entire compressed payload including manifest/checksums/content. Do not leak internal filenames in the encrypted header.

- [ ] **Step 4: Create consistent database snapshots**

Quiesce mutation briefly or use the Turso-supported checkpoint/snapshot mechanism available in the selected stable crate. Include migration version and schema verification. Database Only contains DB snapshot, manifest, checksums. Full also contains indexed artifacts/logs/assets/exports according to selected scope.

- [ ] **Step 5: Implement safe restore**

Restore sequence:

1. stop accepting new jobs/mutations;
2. checkpoint/cancel active jobs safely;
3. upload/read backup to temporary location;
4. verify header, password, checksums, format, and database migrations;
5. optionally create current-state safety backup;
6. extract into a new temporary data directory with path traversal/symlink protection;
7. run deep integrity check against restored data;
8. atomically swap active DB/artifact directories;
9. restart runtime state;
10. preserve old directory until success, then cleanup.

- [ ] **Step 6: Implement migration warning behavior**

When automatic backup is off and a migration is required, interactive UI offers Create Backup & Continue, Continue Without Backup, or Cancel. Docker non-interactive behavior uses explicit bootstrap policy and never waits forever for a dialog.

- [ ] **Step 7: Run tests**

Run:

```bash
cargo test -p erabi-export --test backup_roundtrip
cargo test -p erabi-api --test restore_safety
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add Cargo.lock crates/erabi-export crates/erabi-jobs crates/erabi-api crates/erabi-cli
git commit -m "feat(backup): create encrypted verified backups"
```
### Task 40: Implement Diagnostics, Integrity Checks, Disk Pressure, Settings API, and Metadata Search

**Files:**
- Create: `crates/erabi-observability/src/diagnostics.rs`
- Create: `crates/erabi-observability/src/bundle.rs`
- Create: `crates/erabi-db/src/integrity.rs`
- Create: `crates/erabi-jobs/src/storage_monitor.rs`
- Create: `crates/erabi-db/src/search.rs`
- Create: `crates/erabi-api/src/routes/diagnostics.rs`
- Create: `crates/erabi-api/src/routes/integrity.rs`
- Create: `crates/erabi-api/src/routes/settings.rs`
- Create: `crates/erabi-api/src/routes/search.rs`
- Create: `crates/erabi-cli/src/commands/doctor.rs`
- Test: `crates/erabi-db/tests/integrity_checks.rs`
- Test: `crates/erabi-api/tests/system_operations.rs`

**Interfaces:**
- Produces: `erabi doctor`, System Diagnostics, redacted diagnostic bundle.
- Produces: always-on lightweight integrity check and manual/schedulable deep check, default schedule off.
- Produces: warning/critical disk pressure with safety stop, never automatic deletion.
- Produces: Settings API with inheritance sources and global metadata search/command data.

- [ ] **Step 1: Write integrity corruption tests**

Create test databases with missing required index, broken current-version pointer, approved version mutated, provenance pointing to missing artifact, invalid migration checksum, and orphan job lease. Assert lightweight detects startup-critical structural issues; deep check detects every relational/artifact/audit issue and returns stable codes.

- [ ] **Step 2: Implement lightweight startup checks**

Check migration consistency, required tables/indexes, database readability, artifact directory read/write permission, unfinished transactions/jobs, disk availability, and current-version pointer basics. Any critical failure enters Recovery Mode and stops mutations/workers.

- [ ] **Step 3: Implement deep checks**

Verify database engine integrity, foreign references, immutable approval invariants, pointer consistency, artifact existence/hashes, audit event structural consistency, backup readability, and optional full file checksum. Persist report and progress; scheduling exists but is off by default.

- [ ] **Step 4: Implement `erabi doctor` and redacted bundle**

Report application/Rust/Turso/Crawl4AI/Bun build versions, migration version, database status, directories/permissions, disk space, bind/token/OpenAPI/CORS state, queue/stale jobs, backups, and crawler connectivity. Default bundle excludes URLs, record values, raw content, and assets. Apply redaction again at bundle creation.

Add an explicitly enabled Diagnostic Mode for temporarily richer technical context. It displays a sensitive-data warning, records enable/disable audit events, expires automatically after a fixed short period, and still never reveals passwords, tokens, cookies, authorization headers, or connection secrets.

- [ ] **Step 5: Implement disk pressure monitor**

Use stable filesystem free-space API selected with `cargo add`. Defaults are configurable absolute/percentage warning and critical thresholds. Warning emits event. Critical blocks new artifact-heavy jobs and asks active jobs to checkpoint at safe boundaries; review/approval/settings/diagnostics remain available. Use `BLOCKED_LOW_STORAGE`, not crawl failure. Never delete automatically.

- [ ] **Step 6: Implement Settings API**

Expose built-in, global, Collection, and active resolved values with source labels. Mutations audit old/new/scope. Changes apply only to Crawl Runs created afterward. Secrets are never accepted. Theme/locale/notification preferences are normal settings.

- [ ] **Step 7: Implement metadata search and command data**

`GET /api/v1/search?q=` searches Sources (name/URL/domain/status), Collections, Schemas, Datasets, Crawl Runs, and Exports using indexed metadata queries and bounded result counts. Do not search record content. `GET /api/v1/commands` returns safe quick actions; destructive actions are excluded.

- [ ] **Step 8: Run tests**

Run:

```bash
cargo test -p erabi-db --test integrity_checks
cargo test -p erabi-api --test system_operations
cargo test -p erabi-observability
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add Cargo.lock crates/erabi-observability crates/erabi-db crates/erabi-jobs crates/erabi-api crates/erabi-cli
git commit -m "feat(operations): add diagnostics integrity and search"
```
