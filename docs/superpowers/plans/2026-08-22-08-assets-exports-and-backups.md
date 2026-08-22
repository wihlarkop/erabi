# Erabi Assets, Exports, and Backups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement safe asset handling, approved-only exports, provenance bundles, atomic SQLite/Turso destinations, retention, backups, restore, integrity, and storage protection.

**Architecture:** Assets/artifacts/filesystem payloads remain separate from internal Turso metadata. Export destination DBs never reuse internal application tables. Backup/restore is maintenance-mode and integrity-first.

**Tech Stack:** Rust streaming I/O, SQLite/Turso destination adapters, ZIP/checksums, stable crypto/KDF libraries, Turso metadata.

**Spec:** `docs/specs/07-exports-assets-retention-and-backups.md`, `05-system-architecture-and-persistence.md`, `06-security-reliability-and-operations.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

### Task 1: Asset discovery/download safety

- [ ] Store URL+metadata by default; download only explicit selected assets/direct-file action.
- [ ] Sanitize names/paths, detect MIME where practical, stream large files, hash, clean partials, never auto-extract archives.
- [ ] Preserve URL provenance after local file removal.

### Task 2: File exports and provenance bundles

- [ ] Standard exports contain approved records only.
- [ ] Implement JSON/JSONL/CSV/Markdown and optional provenance ZIP with manifest/checksums.
- [ ] Debug bundles visibly opt-in to sensitive diagnostics; never silently overwrite files.

### Task 3: Destination database adapters

- [ ] Internal Erabi DB and destination DB are separate APIs/credentials/tables.
- [ ] SQLite/Turso create-new default writes typed columns into unique target table, validates, then publishes success.
- [ ] Replace Atomically uses staging + validation + atomic swap/rename where capability supports it; failed export preserves prior valid target.
- [ ] Dedicated DB or deterministic shared namespace/prefix mapping persisted in export metadata.

### Task 4: Retention and disk pressure

- [ ] Automatic destructive cleanup OFF by default; manual cleanup previews count/bytes/categories/evidence retained.
- [ ] Approved curated data/minimum provenance/audit/lifecycle metadata survive ordinary artifact cleanup.
- [ ] Critical disk pressure blocks artifact-heavy jobs without automatic deletion.

### Task 5: Backup, verify, restore

- [ ] Database-only and Full `.erabi-backup` formats with version, manifest, checksums, compatibility metadata.
- [ ] Optional password encryption uses mature stable libraries; wrong password/corruption never mutates active data.
- [ ] Restore stops mutations/jobs, verifies first, optionally snapshots current state, restores, migrates if supported, integrity-checks, rebuilds runtime; failure enters Recovery Mode.
- [ ] Automatic backup remains OFF by default.

**Gate:** approved-only export+provenance bundle verifies; atomic destination failure preserves prior data; backup → verify → restore passes; low storage blocks without deletion.
