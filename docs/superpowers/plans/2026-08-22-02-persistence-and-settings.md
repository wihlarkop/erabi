# Erabi Persistence and Settings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist Crawler Studio core state safely in local Turso, implement explicit tri-state settings resolution, freeze immutable run snapshots, establish migration/repository boundaries, and provide atomic local artifact storage.

**Architecture:** Structured state is accessed through `erabi-db` repositories using the official `turso` crate. Ordinary settings resolve in pure domain code before a run is queued. Large artifacts live on the filesystem behind a small persistence service while Turso stores IDs, hashes, metadata, and safe relative paths.

**Tech Stack:** stable Rust, official `turso` crate, Tokio, Serde/Serde JSON, SHA-256, SQL migrations, filesystem atomic rename.

**Spec:** `docs/specs/05-system-architecture-and-persistence.md`, `docs/specs/06-security-reliability-and-operations.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Global Constraints

- Use the official stable `turso` Rust crate; do not substitute another application DB SDK without a spec change.
- Secrets/bootstrap values remain environment/`.env` only and are never persisted as secret values in Turso.
- Inheritable ordinary settings use exactly `INHERIT`, `CUSTOM(value)`, `RESET_TO_BUILT_IN`.
- Operational precedence is per-run → Run Profile → Crawler operational default → Collection → Global → built-in, using only applicable layers.
- `RESET_TO_BUILT_IN` stops resolution and bypasses lower stored customizations.
- Semantic crawler configuration remains Crawler-Version-owned and cannot be overridden by settings layers.
- Run snapshots are immutable from creation, including while queued, retried, or resumed.
- Large artifacts are filesystem payloads, not giant DB blobs.
- Internal application DB and future export destination DBs are separate concepts.

## Focused File Map

```text
crates/erabi-domain/src/settings.rs
crates/erabi-domain/src/run_snapshot.rs
crates/erabi-db/src/database.rs
crates/erabi-db/src/migrations.rs
crates/erabi-db/src/repositories/
crates/erabi-db/src/artifact_store.rs
migrations/0001_system.sql
migrations/0002_crawler_core.sql
migrations/0003_runs.sql
tests/integration/persistence/
```

---

### Task 1: Implement tri-state settings resolution

**Files:**
- Create: `crates/erabi-domain/src/settings.rs`
- Modify: `crates/erabi-domain/src/lib.rs`
- Test: `crates/erabi-domain/tests/settings_resolution.rs`

**Interfaces:**
- Consumes `OperationalOverrides` from Plan 01.
- Produces `SettingOverride<T>`, `SettingLayer`, `ValueSource`, `ResolvedValue<T>`, `ResolvedOperationalSettings`, `SettingsResolver`.

- [ ] **Step 1: Write the failing precedence/state tests**

```rust
use erabi_domain::{
    BuiltInOperationalSettings, SettingLayer, SettingOverride, SettingsResolver, ValueSource,
};

#[test]
fn collection_reset_to_builtin_bypasses_global_custom_value() {
    let built_in = BuiltInOperationalSettings::test_defaults();
    let global = SettingLayer::test().with_request_delay(SettingOverride::Custom(900));
    let collection = SettingLayer::test().with_request_delay(SettingOverride::ResetToBuiltIn);

    let resolved = SettingsResolver::resolve(
        &built_in,
        Some(&global),
        Some(&collection),
        None,
        None,
        None,
    );

    assert_eq!(resolved.request_delay_ms.value, built_in.request_delay_ms);
    assert_eq!(resolved.request_delay_ms.source, ValueSource::BuiltInReset);
}

#[test]
fn per_run_custom_wins_over_all_applicable_layers() {
    let fixture = erabi_domain::test_support::all_settings_layers();
    let resolved = SettingsResolver::resolve(
        &fixture.built_in,
        Some(&fixture.global),
        Some(&fixture.collection),
        Some(&fixture.crawler),
        Some(&fixture.run_profile),
        Some(&fixture.per_run),
    );
    assert_eq!(resolved.max_pages.source, ValueSource::PerRun);
}

#[test]
fn quick_scrape_without_crawler_skips_crawler_and_run_profile_layers() {
    let fixture = erabi_domain::test_support::quick_scrape_settings_layers();
    let resolved = SettingsResolver::resolve(
        &fixture.built_in,
        Some(&fixture.global),
        Some(&fixture.collection),
        None,
        None,
        Some(&fixture.per_run),
    );
    assert_ne!(resolved.max_pages.source, ValueSource::Crawler);
    assert_ne!(resolved.max_pages.source, ValueSource::RunProfile);
}
```

- [ ] **Step 2: Run RED**

```bash
cargo test -p erabi-domain --test settings_resolution
```

Expected: compile failure because settings types do not exist.

- [ ] **Step 3: Implement explicit tri-state types and resolver**

```rust
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", content = "value", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SettingOverride<T> {
    #[default]
    Inherit,
    Custom(T),
    ResetToBuiltIn,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ValueSource {
    BuiltIn,
    BuiltInReset,
    Global,
    Collection,
    Crawler,
    RunProfile,
    PerRun,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ResolvedValue<T> {
    pub value: T,
    pub source: ValueSource,
}
```

Implement resolver as a pure high-to-low scan. For each field:

```text
per-run CUSTOM -> return
per-run RESET -> built-in + BuiltInReset
per-run INHERIT -> continue
Run Profile -> same
Crawler -> same
Collection -> same
Global -> same
otherwise built-in + BuiltIn
```

Define the MVP operational fields at minimum: max pages, max depth, max duration, concurrency, request delay, timeout, screenshot, wait selector, network-idle, bounded auto-scroll, asset/download limit, User-Agent, robots policy/override marker, retention options, storage thresholds, notification preference, theme, locale, backup/deep-integrity scheduling flags. Secret values are not fields in these structures.

- [ ] **Step 4: Run GREEN and matrix coverage**

```bash
cargo test -p erabi-domain --test settings_resolution
cargo test -p erabi-domain
```

Add a table-driven test covering every `ValueSource` and all three `SettingOverride` variants for at least two representative fields so nullable-value ambiguity cannot regress.

- [ ] **Step 5: Commit**

```bash
git add crates/erabi-domain
 git commit -m "feat(domain): resolve tri-state operational settings"
```

---

### Task 2: Implement immutable Crawl Run configuration snapshots

**Files:**
- Create: `crates/erabi-domain/src/run_snapshot.rs`
- Modify: `crates/erabi-domain/src/lib.rs`
- Test: `crates/erabi-domain/tests/run_snapshot.rs`

**Interfaces:**
- Consumes `CrawlRunType`, `ResolvedOperationalSettings`, `EntityId`.
- Produces `RunConfigSnapshot`, `SemanticConfigRef`, `RobotsDecision`, `SnapshotHash`.

- [ ] **Step 1: Add deterministic hashing dependencies**

```bash
cargo add -p erabi-domain sha2
cargo add -p erabi-domain hex
cargo add -p erabi-domain serde_json
```

- [ ] **Step 2: Write failing snapshot tests**

```rust
use erabi_domain::{RobotsDecision, RunConfigSnapshot};

#[test]
fn equivalent_snapshot_content_has_the_same_hash() {
    let a = erabi_domain::test_support::production_snapshot_fixture();
    let b = erabi_domain::test_support::production_snapshot_fixture();
    assert_eq!(a.config_hash(), b.config_hash());
}

#[test]
fn robots_override_reason_changes_the_snapshot_hash() {
    let a = erabi_domain::test_support::snapshot_with_robots_override("research run");
    let b = erabi_domain::test_support::snapshot_with_robots_override("compliance exception");
    assert_ne!(a.config_hash(), b.config_hash());
    assert!(matches!(a.robots, RobotsDecision::Override { .. }));
}
```

Also test a Quick Scrape snapshot has no Crawler Version reference and a Production Run snapshot requires one.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-domain --test run_snapshot
```

Expected: compile failure for missing snapshot contracts.

- [ ] **Step 4: Implement canonical serialized snapshots**

Use explicit structures, not arbitrary maps:

```rust
pub struct SemanticConfigRef {
    pub crawler_id: Option<EntityId>,
    pub crawler_version_id: Option<EntityId>,
    pub crawler_config_hash: Option<String>,
    pub selected_seed_ids: Vec<EntityId>,
}

pub enum RobotsDecision {
    Respect,
    Override {
        reason: String,
        actor: String,
        decided_at_unix_ms: i64,
        affected_origin: String,
        user_agent: String,
    },
}

pub struct RunConfigSnapshot {
    pub run_type: CrawlRunType,
    pub semantic: SemanticConfigRef,
    pub operational: ResolvedOperationalSettings,
    pub robots: RobotsDecision,
    pub crawl4ai_connection_name: String,
    pub created_by: String,
    pub created_at_unix_ms: i64,
    pub config_hash: String,
}
```

Construct a temporary representation without `config_hash`, serialize deterministically with fixed structs/sorted collections, hash with SHA-256, then store the hex digest. Validate: Production requires a Crawler Version; Test/Discovery require a Draft-capable Crawler Version; Quick Scrape may omit it.

- [ ] **Step 5: Run GREEN**

```bash
cargo test -p erabi-domain --test run_snapshot
```

Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add Cargo.lock crates/erabi-domain
 git commit -m "feat(domain): freeze immutable run configuration snapshots"
```

---

### Task 3: Establish Turso database, migration runner, and core schema

**Files:**
- Create: `crates/erabi-db/src/database.rs`
- Create: `crates/erabi-db/src/migrations.rs`
- Create: `crates/erabi-db/src/error.rs`
- Modify: `crates/erabi-db/src/lib.rs`
- Create: `migrations/0001_system.sql`
- Create: `migrations/0002_crawler_core.sql`
- Create: `migrations/0003_runs.sql`
- Test: `crates/erabi-db/tests/migrations.rs`

**Interfaces:**
- Produces `Database::open_local(path)`.
- Produces `MigrationRunner::apply_all()` and `MigrationRunner::verify()`.
- Produces schema version/checksum tracking table `erabi_schema_migrations`.

- [ ] **Step 1: Add stable persistence dependencies**

Run only current-compatible stable commands:

```bash
cargo add -p erabi-db turso
cargo add -p erabi-db tokio --features fs,sync,rt-multi-thread,macros
cargo add -p erabi-db sha2
cargo add -p erabi-db hex
cargo add -p erabi-db thiserror
cargo add -p erabi-db tracing
cargo add -p erabi-db --path crates/erabi-domain erabi-domain
cargo add -p erabi-db --dev tempfile
```

- [ ] **Step 2: Write failing migration idempotency/checksum tests**

```rust
use erabi_db::{Database, MigrationRunner};
use tempfile::tempdir;

#[tokio::test]
async fn migrations_apply_once_and_verify_checksums() {
    let dir = tempdir().unwrap();
    let db = Database::open_local(dir.path().join("erabi.db")).await.unwrap();
    let runner = MigrationRunner::new(db.clone());
    assert_eq!(runner.apply_all().await.unwrap().applied, 3);
    assert_eq!(runner.apply_all().await.unwrap().applied, 0);
    runner.verify().await.unwrap();
}
```

Add a test fixture with a deliberately modified recorded checksum and assert `verify()` returns a typed migration-integrity error.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-db --test migrations
```

Expected: compile failure for missing DB/migration runner.

- [ ] **Step 4: Implement migration runner and exact core migration ownership**

`0001_system.sql` owns only system primitives: migration tracking, ordinary settings rows/layers, audit events, process/system metadata.

`0002_crawler_core.sql` owns: Collections, Sources, Crawlers, Crawler Versions, Seeds, Page Types, URL Matchers, Discovery Transitions, Run Profiles, Test Evidence. Add foreign keys and indexes for active published/draft pointers. Enforce at-most-one active Draft through repository transaction logic plus a DB constraint/index where supported safely.

`0003_runs.sql` owns Crawl Runs, immutable run snapshot JSON/hash/reference columns, discovered URL metadata/status, and artifact metadata/index rows. Job queue tables are deliberately deferred to Plan 04; Dataset/review tables to Plan 07; asset/export/backup tables to Plan 08.

The runner records migration number, name, SHA-256 checksum, and applied timestamp. It acquires a migration lock through the DB/runtime boundary before applying migrations. On failure, return an error that Plan 03 maps to Recovery Mode; never continue normal mutable startup against a half-applied schema.

Because the official `turso` crate API may evolve, wrap it only inside `database.rs`; tests target Erabi's `Database` API rather than upstream method names.

- [ ] **Step 5: Run GREEN**

```bash
cargo test -p erabi-db --test migrations
cargo test -p erabi-db
```

Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add Cargo.lock crates/erabi-db migrations
 git commit -m "feat(db): add Turso migration foundation"
```

---

### Task 4: Implement core repositories and transactional immutability boundaries

**Files:**
- Create: `crates/erabi-db/src/repositories/mod.rs`
- Create: `crates/erabi-db/src/repositories/crawlers.rs`
- Create: `crates/erabi-db/src/repositories/sources.rs`
- Create: `crates/erabi-db/src/repositories/settings.rs`
- Create: `crates/erabi-db/src/repositories/runs.rs`
- Create: `crates/erabi-db/src/repositories/audit.rs`
- Test: `crates/erabi-db/tests/repository_invariants.rs`

**Interfaces:**
- Produces `CrawlerRepository`, `SourceRepository`, `SettingsRepository`, `RunRepository`, `AuditRepository`.
- Produces transactional operation `publish_draft(...)` used by Plan 05.
- Produces transactional `create_run_with_snapshot(...)` used by Plans 04–06.

- [ ] **Step 1: Write failing invariant tests**

```rust
#[tokio::test]
async fn published_crawler_version_cannot_be_updated_in_place() {
    let fixture = erabi_db::test_support::db_with_published_crawler().await;
    let result = fixture.crawlers.update_published_name(fixture.version_id, "mutated").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn queued_run_keeps_original_snapshot_after_setting_change() {
    let fixture = erabi_db::test_support::queued_run_fixture().await;
    fixture.settings.set_global_max_pages(999).await.unwrap();
    let loaded = fixture.runs.get_snapshot(fixture.run_id).await.unwrap();
    assert_eq!(loaded.config_hash(), fixture.original_hash);
}
```

Also test creating a second active Draft through repository APIs fails atomically.

- [ ] **Step 2: Run RED**

```bash
cargo test -p erabi-db --test repository_invariants
```

Expected: compile failure for missing repositories.

- [ ] **Step 3: Implement narrow repository APIs**

Do not expose generic `update(table, json)` helpers. Repository methods accept typed domain IDs/models and parameterized SQL. `publish_draft` must execute in one transaction:

```text
load Draft + parent/base
→ validate state is Draft
→ insert immutable Published row/content snapshot
→ set crawler active_published_version_id
→ clear active_draft_version_id if it points to the published Draft
→ append publish audit event
→ commit
```

`create_run_with_snapshot` inserts the immutable snapshot/hash and Crawl Run row in one transaction; later plan code may extend the same transaction to root job creation.

- [ ] **Step 4: Run GREEN**

```bash
cargo test -p erabi-db --test repository_invariants
cargo test -p erabi-db
```

Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add crates/erabi-db
 git commit -m "feat(db): enforce core repository invariants"
```

---

### Task 5: Implement atomic local artifact storage and bootstrap persistence boundaries

**Files:**
- Create: `crates/erabi-db/src/artifact_store.rs`
- Create: `crates/erabi-db/src/bootstrap.rs`
- Modify: `crates/erabi-db/src/lib.rs`
- Test: `crates/erabi-db/tests/artifact_store.rs`
- Test: `crates/erabi-db/tests/bootstrap_boundaries.rs`

**Interfaces:**
- Produces `LocalArtifactStore::new(root)`.
- Produces `ArtifactWrite`, `ArtifactRef`, `ArtifactKind`.
- Produces `BootstrapDataLayout` canonical paths for `database/`, `artifacts/`, `assets/`, `exports/`, `backups/`.

- [ ] **Step 1: Write failing atomicity/path-safety tests**

```rust
#[tokio::test]
async fn artifact_write_never_escapes_root() {
    let store = erabi_db::test_support::artifact_store().await;
    let result = store.write_named("../escape.html", b"x").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn successful_write_returns_verified_hash_and_size() {
    let store = erabi_db::test_support::artifact_store().await;
    let reference = store.write_bytes(erabi_db::ArtifactKind::RawHtml, b"<html/>".to_vec()).await.unwrap();
    assert_eq!(reference.size_bytes, 7);
    store.verify(&reference).await.unwrap();
}
```

Add a simulated write-failure test ensuring temporary files are absent after error.

- [ ] **Step 2: Run RED**

```bash
cargo test -p erabi-db --test artifact_store --test bootstrap_boundaries
```

Expected: compile failure for missing store/layout types.

- [ ] **Step 3: Implement controlled filesystem layout and atomic writes**

Canonicalize the configured data root. Create only controlled child directories. Artifact writes use:

```text
generate application-owned safe relative name
→ create temp file inside target directory
→ stream/write bytes
→ flush + sync file where supported
→ close
→ atomic rename within same filesystem
→ compute/store SHA-256 + size metadata
```

Never accept an arbitrary user path as the final path. Reject absolute paths, `..`, path separators in generated filenames, and symlink escapes. Cleanup temporary files on failure/cancellation.

`bootstrap.rs` handles non-secret paths/config only. Secret environment loading belongs to Plan 03 runtime configuration; do not write tokens into DB settings.

- [ ] **Step 4: Run the full Plan 02 gate**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p erabi-domain
cargo test -p erabi-db
```

Expected: exit 0. Confirm migration tests, settings state/precedence matrix, snapshot determinism, Published immutability, second-Draft rejection, and artifact traversal/atomicity all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/erabi-db
 git commit -m "feat(storage): add atomic local artifact persistence"
```

## Plan 02 Gate

Do not start Plan 03 until Task 5 Step 4 passes from a clean checkout and `git status --short` is empty.
