# Erabi Assets, Exports, and Backups Implementation Plan

> **For agentic workers:** Implement each asset/export/backup capability end-to-end, then compile/check, add or update meaningful tests, run verification, and commit. Do not use failing-test-first or RED/GREEN sequencing by default.

**Goal:** Implement safe asset handling, approved-only exports, provenance bundles, atomic SQLite/Turso destinations, retention, backups, restore, integrity, and storage protection.

**Architecture:** Assets/artifacts/filesystem payloads remain separate from internal Turso metadata. Export destination DBs never reuse internal application tables. Backup/restore is maintenance-mode and integrity-first.

**Tech Stack:** stable Rust streaming I/O, SQLite/Turso destination adapters, ZIP/checksums, mature stable crypto/KDF libraries, Turso metadata.

**Spec:** `docs/specs/07-exports-assets-retention-and-backups.md`, `docs/specs/05-system-architecture-and-persistence.md`, `docs/specs/06-security-reliability-and-operations.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

**Migration ownership:** `migrations/0007_assets_exports_backups.sql`.

---

### Task 1: Asset discovery/download safety

**Files:** asset domain/repository/download service/API plus migration tables/tests.

**Requirements:**

- Default stores discovered asset URL + metadata, not every physical file.
- Explicit Download Selected/direct-file action performs safe streaming download.
- Sanitize filename/path, handle collisions/Windows reserved/control chars, reject traversal/absolute/symlink escapes.
- Inspect MIME/signature where practical rather than trusting extension only.
- Stream large files, hash/size, clean failed/cancelled partials, serve as attachment.
- Never auto-execute/open/extract archives.
- Removing local file retains URL/provenance/history.

**Verification:** safe filename/path fixtures, streaming/hash, cancel cleanup, collision, MIME mismatch, archive no-auto-extract, URL provenance after local removal.

---

### Task 2: Approved-only file exports and provenance bundles

**Files:** export service/formatters/manifest/bundle writers, APIs/tests.

**MVP formats:** JSON, JSONL, CSV, Markdown plus optional provenance ZIP bundle.

**Requirements:**

- Standard export includes Approved records only.
- With Provenance produces data + JSONL provenance sidecar + manifest + SHA-256 checksums.
- Debug Bundle is visibly opt-in/sensitive and may include explicitly selected diagnostics/raw evidence only.
- Export filename is safe/deterministic enough for MVP and never silently overwrites an existing file.
- Export history remains when physical file is later removed.
- Physical downloaded assets are included only with explicit `Include downloaded assets`; export must not silently trigger missing asset downloads.

**Verification:** approved-only filter, JSON/JSONL/CSV/Markdown content, provenance manifest/checksum verification, no silent overwrite, sensitive debug opt-in, optional assets manifest states.

---

### Task 3: SQLite/Turso destination adapters and atomic publication

**Files:** destination contracts/adapters, destination metadata persistence, connection test/export tests.

**Requirements:**

- Internal Erabi DB and destination DB use separate APIs/credentials/table namespaces.
- Saved destination stores non-secret config + env-var secret reference only.
- Test Connection validates reachability/auth/write/staging/rename/drop/transaction capabilities required by selected mode.
- Create New default writes typed columns to unique target table, validates schema/row count/constraints, then reports success.
- Replace Atomically writes staging, validates, then swaps/renames/publishes according to destination capability; failure preserves prior valid target.
- Shared DB uses deterministic Collection/Dataset namespace/prefix mapping persisted in export metadata.
- Append/Upsert/PostgreSQL remain roadmap.

**Verification:** destination separation, secret reference, failed staging preserving old data, typed columns, capability revalidation, deterministic mapping.

---

### Task 4: Retention and disk-pressure behavior

**Files:** retention policy/service/API and storage-pressure integration/tests.

**Requirements:**

- Automatic destructive cleanup OFF by default.
- Manual cleanup preview shows policy, counts, estimated bytes, categories removed, evidence retained.
- Approved curated data/minimum provenance/audit/lifecycle metadata survive ordinary artifact cleanup.
- Critical storage blocks new artifact-heavy work through Plan 04 pressure controls; never auto-delete solely because disk is low.
- Export cleanup/history behavior matches spec.

**Verification:** preview calculations, protected metadata, manual cleanup, critical-pressure block/no-auto-delete.

---

### Task 5: Backup, verify, restore, and integrity

**Files:** backup format/manifest/checksum/encryption/restore services, APIs/CLI hooks/tests.

**Backup types:** Database Only default; Full Backup adds selected/all artifacts/logs/screenshots/downloaded assets/index.

**Requirements:**

- One versioned `*.erabi-backup` container with format/app/DB compatibility metadata, manifest, checksums, optional encryption metadata.
- Optional password encryption is OFF by default and uses mature stable crypto/password-KDF libraries; never custom cryptography.
- Password never stored in DB/.env/logs/recoverable metadata.
- Wrong password/corruption never mutates active data.
- Restore enters maintenance flow: stop mutations/jobs → settle/cancel safely → verify format/checksum/password/compatibility → optional safety snapshot → restore → supported migration → integrity check → rebuild runtime → normal only when healthy.
- Failure preserves old state when possible; otherwise Recovery Mode.
- Automatic backup scheduling OFF by default.

**Verification:** database-only/full create/verify, corruption/wrong-password non-mutation, cancelled backup invalidation/cleanup, restore success, restore failure Recovery Mode, optional encryption roundtrip.

---

## Plan 08 Gate

```bash
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Confirm approved-only export + provenance bundle, safe asset handling, internal/destination DB separation, atomic destination failure preservation, retention/no-auto-delete, and backup → verify → restore all pass with fresh evidence. Do not begin Plan 09 until the gate passes.
