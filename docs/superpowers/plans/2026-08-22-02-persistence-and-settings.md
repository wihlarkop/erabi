# Erabi Persistence and Settings Implementation Plan

> **For agentic workers:** Implement each task end-to-end, then compile/check it, add or update meaningful tests, run verification, and commit. Do not use failing-test-first or RED/GREEN sequencing by default.

**Goal:** Persist Crawler Studio state safely in local Turso, implement explicit tri-state settings resolution, immutable run snapshots, migrations, repositories, and atomic filesystem artifacts.

**Architecture:** Structured state lives behind repository/transaction interfaces using the official `turso` crate. Large artifacts live in an atomic filesystem store. Settings resolution is pure and produces a frozen snapshot before queued work exists.

**Tech Stack:** Stable Rust, official `turso`, SQL migrations, Tokio, Serde, SHA-256, filesystem atomic writes.

**Spec:** `docs/specs/05-system-architecture-and-persistence.md`, `docs/specs/06-security-reliability-and-operations.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Migration ownership

Plan 02 owns:

- `migrations/0001_system.sql`
- `migrations/0002_crawler_core.sql`
- `migrations/0003_runs.sql`

Do not create later-plan tables prematurely.

---

### Task 1: Implement tri-state settings resolution

**Files:**
- `crates/erabi-domain/src/settings.rs`
- exports/tests under `crates/erabi-domain/`

**Interfaces:**

```rust
pub enum LayerValue<T> {
    Inherit,
    Custom(T),
    ResetToBuiltIn,
}

pub struct ResolvedValue<T> {
    pub value: T,
    pub source: SettingSource,
}
```

**Implementation requirements:**

Resolve applicable layers in this exact precedence:

```text
per-run override
→ Run Profile
→ Crawler operational default
→ Collection override
→ Global setting
→ built-in default
```

`RESET_TO_BUILT_IN` stops inheritance and returns the built-in value; it is not nullable `INHERIT`. Quick Scrape without Crawler/RunProfile skips those layers unless equivalent ad-hoc configuration exists.

**Verification:**

Add tests covering every layer/state combination, reset semantics, Quick Scrape applicability, and effective-source reporting.

```bash
cargo test -p erabi-domain settings
cargo clippy -p erabi-domain --all-targets -- -D warnings
```

---

### Task 2: Implement immutable Crawl Run snapshots

**Files:**
- `crates/erabi-domain/src/crawl_snapshot.rs`
- related run domain files
- `migrations/0003_runs.sql`
- repository support in `erabi-db`

**Snapshot must freeze:**

- run type;
- semantic CrawlerVersion/config identity/hash or ad-hoc Quick Scrape config;
- resolved operational values and each effective source;
- selected seed IDs where applicable;
- robots decision and reason;
- User-Agent;
- retention/screenshot/storage settings;
- actor/timestamp;
- any fields required to determine checkpoint compatibility.

Retry/resume of the same immutable run reuses this snapshot. Changing settings later affects only newly created runs.

**Verification:**

Test deterministic canonical hashing, no adoption of later setting/config changes, and same-run retry/resume snapshot identity.

```bash
cargo test -p erabi-domain crawl_snapshot
```

---

### Task 3: Implement Turso migrations and repository boundaries

**Files:**
- `migrations/0001_system.sql`
- `migrations/0002_crawler_core.sql`
- `migrations/0003_runs.sql`
- `crates/erabi-db/src/migrate.rs`
- bounded repositories under `crates/erabi-db/src/repositories/`

**Ownership:**

- `0001_system.sql`: migration/schema tracking, settings, audit/system metadata foundation.
- `0002_crawler_core.sql`: Collections, Sources, Crawlers/Versions, Seeds, PageTypes, URLMatchers, Transitions, RunProfiles, TestEvidence.
- `0003_runs.sql`: CrawlRuns, immutable snapshots, discovered URL/artifact metadata foundation needed by later plans.

**Implementation requirements:**

- Use official `turso` crate through `erabi-db`; domain modules do not scatter direct SDK calls.
- Ordered schema version tracking and migration lock.
- Migration failure yields a typed state consumable by Recovery Mode in Plan 03.
- Transactional operations for multi-record version/pointer/audit changes.
- Repository APIs prevent mutation of immutable Published versions through normal write paths.
- Do not add job, curated-data, export, backup, or later-plan tables here.

**Verification:**

Run migrations from an empty DB and the supported prior baseline. Test rollback/failure behavior and repository immutability constraints.

```bash
cargo test -p erabi-db
```

---

### Task 4: Implement atomic ArtifactStore

**Files:**
- `crates/erabi-artifacts/` only if Plan 01 workspace includes it; otherwise place this bounded implementation in the artifact module/crate location defined by the current workspace without inventing duplicate ownership.
- integration tests for path/atomicity behavior

**Interface:** a controlled-root artifact store that writes bytes/streams and returns safe ID/hash/size/path metadata.

**Implementation requirements:**

```text
controlled root
→ safe relative path
→ temp file
→ write/flush/fsync as appropriate
→ close
→ atomic rename/publish
```

Reject traversal, absolute user paths, unsafe symlink escapes, and uncontrolled filesystem destinations. Clean failed partial writes. Store IDs/hashes/metadata/safe paths in DB, not giant blobs.

**Verification:**

Test successful atomic publication, interrupted write cleanup, traversal rejection, symlink escape rejection, hash/size correctness, and collision-safe paths.

---

### Task 5: Implement bootstrap/configuration persistence boundaries

**Files:** relevant config/settings repository modules and `.env.example` updates.

**Requirements:**

- Secrets remain environment/`.env` only.
- Saved destinations store the environment-variable **name** containing a secret, never the secret value.
- Ordinary non-secret settings live in Turso with explicit layer state.
- Local data directory ownership/single-instance metadata is persisted or represented consistently for Plan 03 runtime lock handling.
- No telemetry configuration silently enables outbound telemetry.

**Verification:**

Add tests asserting secrets are absent from persisted ordinary settings and setting layer states round-trip exactly.

---

## Plan 02 Gate

From a clean checkout:

```bash
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Confirm:

- settings matrix covers `INHERIT`, `CUSTOM`, `RESET_TO_BUILT_IN` across all applicable layers;
- snapshot hashes/configuration are deterministic and immutable;
- migration ownership remains `0001`–`0003` only;
- migrations work from required baselines and fail safely;
- Published/version immutability is preserved by repository APIs;
- artifacts are atomic/path-safe;
- secrets are not persisted as ordinary settings.

Do not begin Plan 03 until this gate passes.
