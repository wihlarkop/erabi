# Erabi Persistence and Settings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist Crawler Studio state safely in local Turso, implement explicit tri-state settings resolution, immutable run snapshots, migrations, repositories, and atomic filesystem artifacts.

**Architecture:** Structured state lives behind repository/transaction interfaces using the official `turso` crate. Large artifacts live in an atomic filesystem store. Settings resolution is pure and produces a frozen snapshot before queued work exists.

**Tech Stack:** Rust, official `turso`, SQL migrations, Tokio, Serde, SHA-256, filesystem atomic writes.

**Spec:** `docs/specs/05-system-architecture-and-persistence.md`, `06-security-reliability-and-operations.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

### Task 1: Implement tri-state settings resolver

**Files:** `crates/erabi-domain/src/settings.rs`; test `settings_resolution.rs`.

- [ ] Test `INHERIT`, `CUSTOM(value)`, and `RESET_TO_BUILT_IN` as three distinct states.
- [ ] Test precedence: per-run → RunProfile → Crawler default → Collection → Global → built-in.
- [ ] Test Quick Scrape skips Crawler/RunProfile when absent.
- [ ] Persist `ResolvedValue { value, source }`; `RESET_TO_BUILT_IN` must bypass lower stored customizations.

### Task 2: Implement immutable Crawl Run snapshots

**Files:** `crawl_snapshot.rs`, migration tables for snapshots/runs.

- [ ] Test queued runs do not adopt later settings/config changes.
- [ ] Snapshot run type, semantic config hash/reference, resolved operational values+sources, selected seeds, robots decision/reason, User-Agent, retention/screenshot config, actor/time.
- [ ] Hash canonical serialized data deterministically.
- [ ] Retry/resume reuse the same snapshot identity.

### Task 3: Create Turso schema and repositories

**Files:** `migrations/0001_*.sql`, `crates/erabi-db/src/repositories/*`.

- [ ] Migration tests run from empty DB and supported prior versions.
- [ ] Persist Collections, Sources, Crawlers, CrawlerVersions, Seeds, PageTypes, URLMatchers, Transitions, RunProfiles, TestEvidence, CrawlRuns, Jobs, Datasets/records, provenance, exports/backups, settings, audit events.
- [ ] Enforce immutable published-version and approved-record updates through repository APIs/transactions.
- [ ] Add migration lock and Recovery Mode failure signal.

### Task 4: Implement atomic ArtifactStore

**Files:** `crates/erabi-artifacts/src/*`; integration tests.

- [ ] Write temp → fsync/close → atomic rename under canonical controlled root.
- [ ] Reject traversal, unsafe symlink escapes, and absolute user paths.
- [ ] Verify size/hash metadata and clean failed partial writes.
- [ ] Store only IDs/hashes/safe paths in DB, not giant blobs.

### Task 5: Implement configuration/bootstrap persistence boundaries

- [ ] Secrets remain environment/`.env` only; saved destinations store env-var names, never secret values.
- [ ] Ordinary settings live in Turso with explicit layer state.
- [ ] Validate local data directory single-owner metadata and migration state.

**Gate:** migration/repository tests pass; settings matrix covers every layer/state; snapshot hashes are deterministic; artifact atomicity/path-safety tests pass.
