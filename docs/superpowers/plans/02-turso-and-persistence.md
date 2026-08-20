# Erabi Turso and Persistence Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement settings resolution, immutable crawl snapshots, environment bootstrap, structured tracing, Local Turso persistence, migrations, repository transactions, and atomic filesystem artifacts.

**Architecture:** Normal settings are resolved through built-in, global, Collection, and per-run layers, while secrets remain environment-only. Persistence uses the official Turso Rust crate behind repository boundaries, and large artifacts use an atomic filesystem store rather than database blobs.

**Tech Stack:** Rust, official `turso` crate, SQL migrations, Tokio, Serde, `tracing`, filesystem atomic writes.

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

- **Depends on:** [01 Workspace and Domain Foundation](./01-workspace-and-domain-foundation.md).
- **Produces:** Application database, migrations, transaction boundary, settings snapshots, bootstrap configuration, privacy-safe logs, and `ArtifactStore`.
- **Gate:** Foundation B: migration up/down tests, repository transaction tests, settings snapshot tests, redaction tests, and artifact atomicity tests pass.
- **Execution order:** Complete every task in this file in numerical order and commit after each task. Do not begin the next plan until this gate passes.

## Focused File Map

```text
crates/erabi-domain/src/settings/
crates/erabi-cli/src/config/
crates/erabi-observability/
crates/erabi-db/
crates/erabi-artifacts/
migrations/
tests/integration/database/
tests/integration/artifacts/
```

## Shared Contract Produced by This Plan

```rust
#[async_trait::async_trait]
pub trait ArtifactStore: Send + Sync {
    async fn write_atomic(&self, request: ArtifactWrite) -> Result<ArtifactRef, ArtifactError>;
    async fn verify(&self, reference: &ArtifactRef) -> Result<ArtifactVerification, ArtifactError>;
    async fn remove(&self, reference: &ArtifactRef) -> Result<(), ArtifactError>;
}
```

---

### Task 7: Implement Settings Inheritance and Immutable Crawl Snapshots

**Files:**
- Create: `crates/erabi-domain/src/settings.rs`
- Create: `crates/erabi-domain/src/crawl_config.rs`
- Modify: `crates/erabi-domain/src/lib.rs`
- Test: `crates/erabi-domain/tests/settings_resolution.rs`

**Interfaces:**
- Produces: `SettingOverride<T>`, `SettingsLayer`, `ResolvedSettings`, `ResolvedValue<T>`.
- Produces: `SettingsResolver::resolve(built_in, global, collection, per_run)` and `CrawlConfigSnapshot::from_resolved(resolved)` with stable SHA-256 `config_hash`.

- [ ] **Step 1: Add hashing and serialization dependencies**

Run:

```bash
cargo add -p erabi-domain sha2
cargo add -p erabi-domain hex
cargo add -p erabi-domain serde_json
```

- [ ] **Step 2: Write failing precedence tests**

Create `crates/erabi-domain/tests/settings_resolution.rs`:

```rust
use erabi_domain::{BuiltInSettings, CollectionSettings, GlobalSettings, PerRunSettings, SettingsResolver, ValueSource};

#[test]
fn per_run_overrides_collection_and_global() {
    let resolved = SettingsResolver::resolve(
        BuiltInSettings::default(),
        GlobalSettings { concurrent_pages: Some(2), ..Default::default() },
        CollectionSettings { concurrent_pages: Some(3), ..Default::default() },
        PerRunSettings { concurrent_pages: Some(4), ..Default::default() },
    );
    assert_eq!(resolved.concurrent_pages.value, 4);
    assert_eq!(resolved.concurrent_pages.source, ValueSource::PerRun);
}

#[test]
fn resetting_collection_uses_builtin_not_global() {
    // Use SettingOverride::ResetToBuiltIn for the Collection layer.
    let resolved = erabi_domain::test_support::resolve_reset_example();
    assert_eq!(resolved.request_delay_ms.source, ValueSource::BuiltIn);
}
```

- [ ] **Step 3: Implement tri-state overrides**

Create `settings.rs` with:

```rust
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub enum SettingOverride<T> {
    #[default]
    Inherit,
    Custom(T),
    ResetToBuiltIn,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ValueSource { BuiltIn, Global, Collection, PerRun }

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ResolvedValue<T> { pub value: T, pub source: ValueSource }
```

Define the complete MVP setting set: active jobs, pages per job, request delay, per-domain limit, timeout, maximum pages, wait selector, network-idle, auto-scroll and limits, screenshots, retention, robots policy, User-Agent, storage thresholds, notification preferences, theme, locale, and backup/integrity scheduling flags.

- [ ] **Step 4: Implement immutable snapshot hashing**

Create `crawl_config.rs`:

```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CrawlConfigSnapshot {
    pub resolved: ResolvedSettings,
    pub config_hash: String,
}

impl CrawlConfigSnapshot {
    pub fn from_resolved(resolved: ResolvedSettings) -> Result<Self, serde_json::Error> {
        use sha2::{Digest, Sha256};
        let canonical = serde_json::to_vec(&resolved)?;
        let config_hash = hex::encode(Sha256::digest(canonical));
        Ok(Self { resolved, config_hash })
    }
}
```

Keep map-like settings in sorted containers so hashes remain deterministic.

- [ ] **Step 5: Run tests**

Run: `cargo test -p erabi-domain --test settings_resolution`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add Cargo.lock crates/erabi-domain
git commit -m "feat(domain): resolve settings into crawl snapshots"
```
### Task 8: Load Bootstrap Configuration from Environment and `.env`

**Files:**
- Create: `crates/erabi-cli/src/config.rs`
- Modify: `crates/erabi-cli/src/main.rs`
- Test: `crates/erabi-cli/tests/config_loading.rs`

**Interfaces:**
- Produces: `BootstrapConfig::load()`.
- Enforces: non-loopback host requires a non-empty access token.
- Enforces: OS environment overrides `.env`.

- [ ] **Step 1: Add stable configuration dependencies**

Run:

```bash
cargo add -p erabi dotenvy
cargo add -p erabi serde --features derive
cargo add -p erabi thiserror
cargo add -p erabi url
```

- [ ] **Step 2: Write failing configuration tests**

Create `crates/erabi-cli/tests/config_loading.rs`:

```rust
use erabi::config::{BootstrapConfig, ConfigError};

#[test]
fn localhost_does_not_require_access_token() {
    let config = BootstrapConfig::from_pairs([
        ("ERABI_HOST", "127.0.0.1"),
        ("ERABI_PORT", "7878"),
    ]).unwrap();
    assert!(config.access_token.is_none());
}

#[test]
fn non_loopback_requires_access_token() {
    let error = BootstrapConfig::from_pairs([
        ("ERABI_HOST", "0.0.0.0"),
        ("ERABI_PORT", "7878"),
    ]).unwrap_err();
    assert!(matches!(error, ConfigError::MissingAccessToken));
}
```

- [ ] **Step 3: Implement typed bootstrap configuration**

Create `crates/erabi-cli/src/config.rs` with fields for host, port, data directory, log format/level, Crawl4AI URL/token reference, access token, CORS allowlist, and OpenAPI flag. Parse the host into `std::net::IpAddr`; reject invalid ports and empty token strings. Expose `is_loopback()`.

Use this validation logic:

```rust
if !host.is_loopback() && access_token.as_deref().is_none_or(str::is_empty) {
    return Err(ConfigError::MissingAccessToken);
}
```

`BootstrapConfig::load()` must call `dotenvy::dotenv().ok()` before reading `std::env`, which preserves environment-over-file priority.

- [ ] **Step 4: Export the config module from a library target**

Create `crates/erabi-cli/src/lib.rs`:

```rust
pub mod config;
```

Update `main.rs` to call `BootstrapConfig::load()` and print a redacted startup summary.

- [ ] **Step 5: Run tests**

Run: `cargo test -p erabi --test config_loading`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add Cargo.lock crates/erabi-cli
git commit -m "feat(config): load secure bootstrap settings"
```
### Task 9: Establish Structured Tracing and Privacy Redaction

**Files:**
- Create: `crates/erabi-observability/src/config.rs`
- Create: `crates/erabi-observability/src/redaction.rs`
- Create: `crates/erabi-observability/src/lib.rs`
- Test: `crates/erabi-observability/tests/redaction.rs`

**Interfaces:**
- Produces: `init_tracing(TracingConfig)`.
- Produces: `RedactedUrl`, `Sensitive<T>`, and `redact_json_value()`.
- Enforces: no query values, tokens, cookies, request/response bodies, or extracted values in default logs.

- [ ] **Step 1: Add stable tracing dependencies**

Run:

```bash
cargo add -p erabi-observability tracing
cargo add -p erabi-observability tracing-subscriber --features env-filter,json
cargo add -p erabi-observability serde --features derive
cargo add -p erabi-observability serde_json
cargo add -p erabi-observability url
cargo add -p erabi-observability secrecy
```

- [ ] **Step 2: Write failing redaction tests**

Create `crates/erabi-observability/tests/redaction.rs`:

```rust
use erabi_observability::{redact_json_value, RedactedUrl};
use serde_json::json;

#[test]
fn url_queries_are_removed() {
    let url = RedactedUrl::parse("https://example.com/path?token=secret&x=1").unwrap();
    assert_eq!(url.to_string(), "https://example.com/path?[REDACTED]");
}

#[test]
fn sensitive_json_keys_are_redacted_recursively() {
    let mut value = json!({"authorization":"Bearer secret","nested":{"cookie":"abc"}});
    redact_json_value(&mut value);
    assert_eq!(value["authorization"], "[REDACTED]");
    assert_eq!(value["nested"]["cookie"], "[REDACTED]");
}
```

- [ ] **Step 3: Implement redaction primitives**

Implement case-insensitive sensitive-key matching for `authorization`, `cookie`, `set-cookie`, `token`, `password`, `secret`, `api_key`, and `auth_token`. `RedactedUrl` must preserve scheme, host, port, and path while replacing any query with a single `[REDACTED]` marker and removing fragments.

- [ ] **Step 4: Implement pretty and JSON tracing formats**

`init_tracing` must:

```rust
pub enum LogFormat { Pretty, Json }
pub struct TracingConfig { pub format: LogFormat, pub filter: String }
```

- pretty output for local development;
- JSON one-event-per-line output for Docker;
- default filter `info`;
- include event names and span fields such as `trace_id`, `job_id`, `crawl_run_id`, and `source_id` when present;
- avoid ANSI in JSON mode.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p erabi-observability
cargo clippy -p erabi-observability --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add Cargo.lock crates/erabi-observability
git commit -m "feat(observability): add structured redacted tracing"
```
### Task 10: Connect Local Turso and Implement SQL Migrations

**Files:**
- Create: `crates/erabi-db/src/database.rs`
- Create: `crates/erabi-db/src/migration.rs`
- Create: `crates/erabi-db/src/error.rs`
- Modify: `crates/erabi-db/src/lib.rs`
- Create: `migrations/0001_system.sql`
- Test: `crates/erabi-db/tests/migration_runner.rs`

**Interfaces:**
- Produces: `Database::open_local(path)` and `Database::connect()`.
- Produces: `MigrationRunner::apply_all()` and `MigrationRunner::verify()`.
- Persists: `erabi_schema_migrations` with version, name, checksum, and applied timestamp.

- [ ] **Step 1: Add the official stable Turso crate and supporting dependencies**

Run:

```bash
cargo add -p erabi-db turso
cargo add -p erabi-db tokio --features fs,sync,rt-multi-thread,macros
cargo add -p erabi-db sha2
cargo add -p erabi-db hex
cargo add -p erabi-db thiserror
cargo add -p erabi-db tracing
cargo add -p erabi-db serde --features derive
```

Do not substitute `libsql` for the application database.

- [ ] **Step 2: Write failing migration tests**

Create `crates/erabi-db/tests/migration_runner.rs`:

```rust
use erabi_db::{Database, MigrationRunner};
use tempfile::tempdir;

#[tokio::test]
async fn migrations_are_applied_once_and_verified() {
    let dir = tempdir().unwrap();
    let db = Database::open_local(dir.path().join("erabi.db")).await.unwrap();
    let runner = MigrationRunner::new(db.clone());

    let first = runner.apply_all().await.unwrap();
    let second = runner.apply_all().await.unwrap();

    assert_eq!(first.applied, 1);
    assert_eq!(second.applied, 0);
    runner.verify().await.unwrap();
}
```

Run: `cargo add -p erabi-db --dev tempfile`.

- [ ] **Step 3: Implement the database wrapper**

Create `database.rs`:

```rust
#[derive(Clone)]
pub struct Database {
    inner: std::sync::Arc<turso::Database>,
}

impl Database {
    pub async fn open_local(path: impl AsRef<std::path::Path>) -> Result<Self, DatabaseError> {
        let db = turso::Builder::new_local(path.as_ref()).build().await?;
        Ok(Self { inner: std::sync::Arc::new(db) })
    }

    pub fn connect(&self) -> Result<turso::Connection, DatabaseError> {
        self.inner.connect().map_err(DatabaseError::from)
    }
}
```

Wrap Turso errors in a typed `DatabaseError`; never expose raw database messages through the public API.

- [ ] **Step 4: Create the first migration**

Create `migrations/0001_system.sql`:

```sql
CREATE TABLE IF NOT EXISTS erabi_schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS system_settings (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS system_events (
    id BLOB PRIMARY KEY,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    summary TEXT NOT NULL,
    trace_id BLOB,
    created_at TEXT NOT NULL
);
```

- [ ] **Step 5: Implement a deterministic internal migration runner**

Embed migrations as a sorted static list. For each migration:

1. compute SHA-256 of the SQL bytes;
2. query the version row;
3. reject a matching version with a different checksum;
4. execute the SQL inside a transaction;
5. insert the migration row in the same transaction;
6. commit;
7. return the count applied.

`verify()` must confirm every embedded migration exists with the expected checksum and required system tables are queryable.

- [ ] **Step 6: Run migration tests**

Run:

```bash
cargo test -p erabi-db --test migration_runner
cargo clippy -p erabi-db --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.lock crates/erabi-db migrations
git commit -m "feat(db): connect Turso and run verified migrations"
```
### Task 11: Create the Complete MVP Database Schema and Repository Transaction Boundary

**Files:**
- Create: `migrations/0002_domain.sql`
- Create: `migrations/0003_jobs_events.sql`
- Create: `migrations/0004_exports_backups.sql`
- Create: `crates/erabi-db/src/transaction.rs`
- Create: `crates/erabi-db/src/repository.rs`
- Create: `crates/erabi-db/src/audit.rs`
- Create: `crates/erabi-db/src/row.rs`
- Modify: `crates/erabi-db/src/lib.rs`
- Test: `crates/erabi-db/tests/schema_contract.rs`
- Test: `crates/erabi-db/tests/transaction_contract.rs`

**Interfaces:**
- Produces: `DbTransaction`, `Database::begin()`, `Database::with_transaction()`.
- Produces: focused repository traits implemented inside `erabi-db`.
- Persists every entity required by the approved data model.

- [ ] **Step 1: Write failing schema presence tests**

Create `crates/erabi-db/tests/schema_contract.rs`:

```rust
use erabi_db::{Database, MigrationRunner};
use tempfile::tempdir;

const REQUIRED_TABLES: &[&str] = &[
    "collections", "sources", "crawl_runs", "crawl_tasks", "raw_artifacts",
    "extraction_schemas", "schema_versions", "datasets", "dataset_versions",
    "records", "record_versions", "field_provenance", "validation_results",
    "reviews", "approvals", "audit_events", "jobs", "job_checkpoints",
    "progress_events", "assets", "saved_destinations", "export_runs",
    "backup_runs", "trash_entries",
];

#[tokio::test]
async fn every_mvp_table_exists_after_migration() {
    let dir = tempdir().unwrap();
    let db = Database::open_local(dir.path().join("erabi.db")).await.unwrap();
    MigrationRunner::new(db.clone()).apply_all().await.unwrap();
    let conn = db.connect().unwrap();

    for table in REQUIRED_TABLES {
        let mut rows = conn.query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?1",
            [*table],
        ).await.unwrap();
        assert!(rows.next().await.unwrap().is_some(), "missing table {table}");
    }
}
```

- [ ] **Step 2: Write the transaction rollback test**

Create `crates/erabi-db/tests/transaction_contract.rs`:

```rust
use erabi_db::{Database, MigrationRunner};
use tempfile::tempdir;

#[tokio::test]
async fn failed_transaction_rolls_back_every_write() {
    let dir = tempdir().unwrap();
    let db = Database::open_local(dir.path().join("erabi.db")).await.unwrap();
    MigrationRunner::new(db.clone()).apply_all().await.unwrap();

    let tx = db.begin().await.unwrap();
    tx.execute("INSERT INTO collections(id,name,slug,created_at) VALUES(?1,?2,?3,?4)",
        (vec![1_u8;16], "A", "a", "2026-07-22T00:00:00Z")).await.unwrap();
    tx.rollback().await.unwrap();

    let conn = db.connect().unwrap();
    let mut rows = conn.query("SELECT COUNT(*) FROM collections", ()).await.unwrap();
    assert_eq!(rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(), 0);
}
```

- [ ] **Step 3: Create `0002_domain.sql` with immutable-version-friendly tables**

The migration must define exact foreign keys and indexes for:

```sql
CREATE TABLE collections (
    id BLOB PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL,
    settings_json TEXT,
    created_at TEXT NOT NULL,
    archived_at TEXT
);
CREATE UNIQUE INDEX collections_slug_uq ON collections(slug);

CREATE TABLE sources (
    id BLOB PRIMARY KEY,
    collection_id BLOB REFERENCES collections(id),
    name TEXT NOT NULL,
    original_url TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    source_type TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    archived_at TEXT,
    trashed_at TEXT
);
CREATE INDEX sources_collection_idx ON sources(collection_id);
CREATE INDEX sources_canonical_url_idx ON sources(canonical_url);

CREATE TABLE crawl_runs (
    id BLOB PRIMARY KEY,
    source_id BLOB NOT NULL REFERENCES sources(id),
    parent_run_id BLOB REFERENCES crawl_runs(id),
    status TEXT NOT NULL,
    result_kind TEXT,
    config_snapshot_json TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    schema_version_id BLOB,
    planned_pages INTEGER NOT NULL DEFAULT 1,
    completed_pages INTEGER NOT NULL DEFAULT 0,
    failed_pages INTEGER NOT NULL DEFAULT 0,
    complete_snapshot INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT
);

CREATE TABLE raw_artifacts (
    id BLOB PRIMARY KEY,
    crawl_run_id BLOB NOT NULL REFERENCES crawl_runs(id),
    artifact_type TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    byte_size INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE extraction_schemas (
    id BLOB PRIMARY KEY,
    collection_id BLOB REFERENCES collections(id),
    name TEXT NOT NULL,
    url_pattern TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE schema_versions (
    id BLOB PRIMARY KEY,
    schema_id BLOB NOT NULL REFERENCES extraction_schemas(id),
    version_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    definition_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    approved_at TEXT,
    UNIQUE(schema_id, version_number)
);

CREATE TABLE datasets (
    id BLOB PRIMARY KEY,
    source_id BLOB NOT NULL REFERENCES sources(id),
    name TEXT NOT NULL,
    review_mode TEXT NOT NULL,
    current_version_id BLOB,
    created_at TEXT NOT NULL
);

CREATE TABLE dataset_versions (
    id BLOB PRIMARY KEY,
    dataset_id BLOB NOT NULL REFERENCES datasets(id),
    crawl_run_id BLOB NOT NULL REFERENCES crawl_runs(id),
    version_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    approved_at TEXT,
    superseded_at TEXT,
    UNIQUE(dataset_id, version_number)
);

CREATE TABLE records (
    id BLOB PRIMARY KEY,
    dataset_id BLOB NOT NULL REFERENCES datasets(id),
    unique_key TEXT NOT NULL,
    current_version_id BLOB,
    lifecycle_status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(dataset_id, unique_key)
);

CREATE TABLE record_versions (
    id BLOB PRIMARY KEY,
    record_id BLOB NOT NULL REFERENCES records(id),
    dataset_version_id BLOB NOT NULL REFERENCES dataset_versions(id),
    version_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    values_json TEXT NOT NULL,
    normalized_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    approved_at TEXT,
    superseded_at TEXT,
    UNIQUE(record_id, version_number)
);
```

Continue the same migration with `field_provenance`, `validation_results`, `reviews`, `approvals`, `audit_events`, and `trash_entries`. Store raw and normalized values separately in provenance JSON columns. Add indexes for every foreign key and common status/date query.

- [ ] **Step 4: Create job, event, export, asset, destination, and backup migrations**

`0003_jobs_events.sql` must create durable jobs, attempts, leases, checkpoints, crawl tasks, and sequence-indexed progress events. `0004_exports_backups.sql` must create assets, saved destinations, export runs, backup runs, and retention cleanup records. Use `BLOB` IDs, RFC3339 text timestamps, and JSON text for versioned payloads.

- [ ] **Step 5: Implement the concrete transaction wrapper**

Create `transaction.rs`:

```rust
pub struct DbTransaction {
    inner: turso::Transaction,
    completed: bool,
}

impl DbTransaction {
    pub async fn execute<P: turso::IntoParams>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<u64, DatabaseError> {
        self.inner.execute(sql, params).await.map_err(Into::into)
    }

    pub async fn commit(mut self) -> Result<(), DatabaseError> {
        self.inner.commit().await?;
        self.completed = true;
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<(), DatabaseError> {
        self.inner.rollback().await?;
        self.completed = true;
        Ok(())
    }
}
```

Add `Database::begin()` using `Connection::transaction()`. Do not attempt implicit commit on drop.

- [ ] **Step 6: Define repository boundaries**

Create `repository.rs` with focused traits such as `CollectionRepository`, `SourceRepository`, `CrawlRunRepository`, `SchemaRepository`, `DatasetRepository`, `RecordRepository`, `AuditRepository`, and `SettingsRepository`. Methods accepting atomic work must receive `&DbTransaction`; read-only methods may receive `&Database`.

Implement `audit.rs` as append-only storage. `AuditRepository::append(&DbTransaction, NewAuditEvent)` inserts UUIDv7 ID, stable event type, actor (`local-user` in MVP), entity type/ID, timestamp, redacted before/after summaries, reason, trace ID, and metadata. Expose no update/delete method for audit rows; permanent deletion writes a tombstone event rather than deleting history.

- [ ] **Step 7: Run all database tests**

Run:

```bash
cargo test -p erabi-db
cargo clippy -p erabi-db --all-targets -- -D warnings
```

Expected: all schema and transaction tests PASS.

- [ ] **Step 8: Commit**

```bash
git add migrations crates/erabi-db Cargo.lock
git commit -m "feat(db): add complete MVP schema and transactions"
```
### Task 12: Implement Atomic Filesystem Artifact Storage

**Files:**
- Create: `crates/erabi-artifacts/src/model.rs`
- Create: `crates/erabi-artifacts/src/path.rs`
- Create: `crates/erabi-artifacts/src/store.rs`
- Create: `crates/erabi-artifacts/src/error.rs`
- Modify: `crates/erabi-artifacts/src/lib.rs`
- Test: `crates/erabi-artifacts/tests/store_contract.rs`
- Test: `crates/erabi-artifacts/tests/path_safety.rs`

**Interfaces:**
- Produces: the fixed `ArtifactStore` trait and `LocalArtifactStore` implementation.
- Enforces: atomic write-then-rename, SHA-256, size tracking, safe relative paths, no symlink traversal.

- [ ] **Step 1: Add dependencies**

Run:

```bash
cargo add -p erabi-artifacts async-trait
cargo add -p erabi-artifacts tokio --features fs,io-util
cargo add -p erabi-artifacts sha2
cargo add -p erabi-artifacts hex
cargo add -p erabi-artifacts thiserror
cargo add -p erabi-artifacts serde --features derive
cargo add -p erabi-artifacts uuid --features v7,serde
cargo add -p erabi-artifacts mime
cargo add -p erabi-artifacts sanitize-filename
cargo add -p erabi-artifacts --dev tempfile
```

- [ ] **Step 2: Write atomicity and path traversal tests**

Create tests proving:

```rust
#[tokio::test]
async fn write_atomic_persists_bytes_and_hash() { /* write b"hello"; assert content, size=5, known SHA-256 */ }

#[tokio::test]
async fn failed_write_leaves_no_partial_file() { /* inject invalid parent; assert no .partial remains */ }

#[test]
fn unsafe_names_never_escape_the_data_directory() {
    assert_eq!(safe_component("../../CON.txt"), "CON_.txt");
}
```

Use the exact known SHA-256 for `hello`: `2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824`.

- [ ] **Step 3: Implement artifact models**

Define `ArtifactType::{RawHtml,CleanedHtml,RenderedDom,Markdown,StructuredJson,Screenshot,TechnicalLog,FailedResponse,DownloadedAsset,Export,Backup}` and:

```rust
pub struct ArtifactWrite {
    pub owner_id: EntityId,
    pub artifact_type: ArtifactType,
    pub suggested_name: String,
    pub bytes: bytes::Bytes,
}

pub struct ArtifactRef {
    pub id: EntityId,
    pub relative_path: std::path::PathBuf,
    pub sha256: String,
    pub byte_size: u64,
}
```

Add `bytes` through `cargo add -p erabi-artifacts bytes`.

- [ ] **Step 4: Implement safe path construction**

Construct paths only from trusted IDs and sanitized file components:

```text
artifacts/{owner-uuid}/{artifact-type}/{artifact-uuid}-{safe-name}
```

Canonicalize the data root once. Reject absolute paths, `..`, Windows device names, control characters, empty names, and any existing symlink component.

- [ ] **Step 5: Implement atomic writes and verification**

Write to a same-directory `.{uuid}.partial` file, flush, optionally sync, rename atomically, then return metadata. `verify()` rehashes the file and reports `Valid`, `Missing`, or `HashMismatch`. `remove()` must remove only the referenced regular file beneath the canonical root.

- [ ] **Step 6: Run tests**

Run: `cargo test -p erabi-artifacts`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.lock crates/erabi-artifacts
git commit -m "feat(artifacts): add safe atomic artifact storage"
```
