# Erabi MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the complete Erabi MVP: a local-first, no-code web data ingestion and curation platform with durable crawling, visual extraction, immutable review/versioning, field-level provenance, exports, backups, recovery, and hardened Docker deployment.

**Architecture:** Erabi is a Rust modular monolith. A single `erabi serve` process hosts the Axum API, SvelteKit static UI, Tokio job runtime, Turso persistence, filesystem artifacts, SSE progress, and all product services; unmodified Crawl4AI remains a separate HTTP service. Domain rules are isolated from infrastructure behind explicit repository, crawler, artifact, and destination interfaces.

**Tech Stack:** Stable Rust, Axum, Tokio, Tower, official `turso` crate, Serde, Reqwest/Rustls, `tracing`, SvelteKit, Svelte, TypeScript, Bun, Playwright, Docker Compose, UUIDv7.

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

## How to Use This Complete Reference

This is one downloadable plan file, but it is intentionally divided into independently reviewable phases. Execute tasks in numerical order. Every task ends with a focused test cycle and a commit. Do not begin a phase until the previous phase gate passes.

### Phase gates

1. **Foundation gate:** Tasks 1–10; workspace, domain primitives, configuration, observability, database, and migrations compile and test.
2. **Runtime gate:** Tasks 11–20; repositories, artifacts, security, API shell, startup/recovery, jobs, SSE, and crawler contract work without the product UI.
3. **Crawl and curation gate:** Tasks 21–33; source creation, crawling, extraction, provenance, versioning, review, and assets work through API integration tests.
4. **Data portability gate:** Tasks 34–40; exports, destinations, retention, backup/restore, diagnostics, and metadata search work.
5. **Product UI gate:** Tasks 41–47; Start, progress, extraction editor, review, operations pages, accessibility, and browser notifications work.
6. **Release gate:** Tasks 48–52; Docker, E2E, real Crawl4AI smoke tests, CI/security, and acceptance documentation pass.

## Complete File Map

The plan creates the following product structure. Do not consolidate focused files into large catch-all modules.

```text
erabi/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── package.json
├── bun.lock
├── bunfig.toml
├── clippy.toml
├── .env.example
├── .gitignore
├── LICENSE
├── README.md
├── apps/
│   └── web/
│       ├── package.json
│       ├── svelte.config.js
│       ├── vite.config.ts
│       ├── tsconfig.json
│       ├── static/
│       └── src/
│           ├── app.html
│           ├── app.css
│           ├── lib/
│           │   ├── api/
│           │   ├── components/
│           │   ├── features/
│           │   ├── i18n/
│           │   ├── stores/
│           │   └── types/
│           └── routes/
├── crates/
│   ├── erabi-domain/
│   ├── erabi-db/
│   ├── erabi-api/
│   ├── erabi-jobs/
│   ├── erabi-crawler/
│   ├── erabi-crawl4ai/
│   ├── erabi-extraction/
│   ├── erabi-export/
│   ├── erabi-artifacts/
│   ├── erabi-security/
│   ├── erabi-observability/
│   └── erabi-cli/
├── migrations/
├── docker/
│   ├── Dockerfile
│   └── compose.yaml
├── tests/
│   ├── fixtures/
│   │   ├── websites/
│   │   └── crawl4ai/
│   ├── integration/
│   ├── smoke/
│   └── e2e/
└── docs/
    ├── superpowers/specs/
    ├── superpowers/plans/
    ├── operations/
    └── api/
```

## Fixed Cross-Crate Contracts

These contracts are introduced incrementally by the tasks below. Later tasks may add fields, but must not rename established methods without updating every consumer and test in the same commit.

```rust
#[async_trait::async_trait]
pub trait CrawlerAdapter: Send + Sync {
    async fn health_check(&self) -> Result<CrawlerHealth, CrawlerError>;
    async fn crawl(&self, request: CrawlRequest) -> Result<CrawlOutput, CrawlerError>;
    async fn cancel(&self, external_job_id: &str) -> Result<(), CrawlerError>;
}

#[async_trait::async_trait]
pub trait ArtifactStore: Send + Sync {
    async fn write_atomic(&self, request: ArtifactWrite) -> Result<ArtifactRef, ArtifactError>;
    async fn verify(&self, reference: &ArtifactRef) -> Result<ArtifactVerification, ArtifactError>;
    async fn remove(&self, reference: &ArtifactRef) -> Result<(), ArtifactError>;
}

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

## Phase 1: Foundation

### Task 1: Create the Cargo and Bun Workspaces

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `package.json`
- Create: `bunfig.toml`
- Create: `clippy.toml`
- Create: `.gitignore`
- Create: `.env.example`
- Create: `scripts/verify-workspace.ts`
- Modify: `README.md`

**Interfaces:**
- Produces: Cargo workspace membership under `crates/*`.
- Produces: Bun workspace membership under `apps/*` and `packages/*`.
- Produces: root scripts `check`, `test`, `build`, and `dev`.

- [ ] **Step 1: Write the workspace verification script before creating the workspace files**

Create `scripts/verify-workspace.ts`:

```ts
import { existsSync, readFileSync } from "node:fs";

const required = [
  "Cargo.toml",
  "rust-toolchain.toml",
  "package.json",
  "bunfig.toml",
  ".env.example",
];

for (const path of required) {
  if (!existsSync(path)) throw new Error(`missing ${path}`);
}

const pkg = JSON.parse(readFileSync("package.json", "utf8"));
if (pkg.packageManager?.startsWith("bun@") !== true) {
  throw new Error("packageManager must pin stable Bun");
}
if (!Array.isArray(pkg.workspaces) || !pkg.workspaces.includes("apps/*")) {
  throw new Error("apps/* Bun workspace is required");
}

console.log("workspace files are valid");
```

- [ ] **Step 2: Run the verification script and confirm it fails**

Run: `bun scripts/verify-workspace.ts`

Expected: FAIL with `missing Cargo.toml`.

- [ ] **Step 3: Create the root Cargo workspace**

Create `Cargo.toml`:

```toml
[workspace]
members = ["crates/*"]
resolver = "3"

[workspace.package]
version = "0.1.0"
edition = "2024"
license = "Apache-2.0"
rust-version = "1.85"

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
all = "warn"
pedantic = "warn"
nursery = "warn"
unwrap_used = "deny"
expect_used = "deny"
```

Create `rust-toolchain.toml`:

```toml
[toolchain]
channel = "stable"
components = ["clippy", "rustfmt"]
profile = "minimal"
```

The `rust-version` value is a minimum compatibility floor, not a toolchain pin. At execution time, raise it only when a stable selected dependency requires a newer compiler.

- [ ] **Step 4: Create the Bun workspace and scripts**

Create `package.json`:

```json
{
  "name": "erabi",
  "private": true,
  "version": "0.1.0",
  "workspaces": ["apps/*", "packages/*"],
  "scripts": {
    "dev": "bun --cwd apps/web run dev",
    "check": "bun scripts/verify-workspace.ts && bun --cwd apps/web run check",
    "test": "bun --cwd apps/web run test && cargo test --workspace",
    "build": "bun --cwd apps/web run build && cargo build --workspace",
    "e2e": "bunx playwright test"
  }
}
```

Create `bunfig.toml`:

```toml
[install]
frozenLockfile = false
```

Create `clippy.toml` so production code keeps strict unwrap rules while tests may use direct assertions:

```toml
allow-unwrap-in-tests = true
allow-expect-in-tests = true
```

Record the stable Bun version actually executing the bootstrap without hard-coding an older release:

```bash
bun -e 'const p = await Bun.file("package.json").json(); p.packageManager = `bun@${Bun.version}`; await Bun.write("package.json", JSON.stringify(p, null, 2) + "\n");'
```

During CI, always invoke `bun install --frozen-lockfile` regardless of this local-development default.

- [ ] **Step 5: Add bootstrap environment documentation**

Create `.env.example`:

```dotenv
ERABI_HOST=127.0.0.1
ERABI_PORT=7878
ERABI_DATA_DIR=./data
ERABI_LOG_FORMAT=pretty
ERABI_LOG_LEVEL=info
CRAWL4AI_BASE_URL=http://crawl4ai:11235
CRAWL4AI_API_TOKEN=
ERABI_ACCESS_TOKEN=
ERABI_CORS_ALLOWED_ORIGINS=
ERABI_OPENAPI_ENABLED=true
TURSO_DATABASE_URL=
TURSO_AUTH_TOKEN=
```

Create `.gitignore`:

```gitignore
.env
/data/
/target/
/node_modules/
/apps/web/.svelte-kit/
/apps/web/build/
/playwright-report/
/test-results/
*.erabi-backup
```

- [ ] **Step 6: Update the README entry points**

Add this section to `README.md`:

```markdown
## Project documents

- [Approved design specifications](docs/superpowers/specs/2026-07-22-erabi-design-index.md)
- [MVP implementation plan](docs/superpowers/plans/2026-07-22-erabi-mvp-plan-index.md)

Implementation uses Cargo and Bun workspaces only. No external monorepo orchestrator is used.
```

- [ ] **Step 7: Run workspace verification**

Run: `bun scripts/verify-workspace.ts`

Expected: `workspace files are valid`.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml rust-toolchain.toml package.json bunfig.toml clippy.toml .gitignore .env.example scripts/verify-workspace.ts README.md
git commit -m "chore: initialize Erabi workspaces"
```

### Task 2: Scaffold Focused Rust Crates

**Files:**
- Create: `crates/erabi-domain/src/lib.rs`
- Create: `crates/erabi-db/src/lib.rs`
- Create: `crates/erabi-api/src/lib.rs`
- Create: `crates/erabi-jobs/src/lib.rs`
- Create: `crates/erabi-crawler/src/lib.rs`
- Create: `crates/erabi-crawl4ai/src/lib.rs`
- Create: `crates/erabi-extraction/src/lib.rs`
- Create: `crates/erabi-export/src/lib.rs`
- Create: `crates/erabi-artifacts/src/lib.rs`
- Create: `crates/erabi-security/src/lib.rs`
- Create: `crates/erabi-observability/src/lib.rs`
- Create: `crates/erabi-cli/src/main.rs`
- Test: `crates/erabi-domain/tests/workspace_contract.rs`

**Interfaces:**
- Produces: one crate per approved architectural boundary.
- Produces: executable package `erabi` from `erabi-cli`.

- [ ] **Step 1: Write the failing workspace contract test**

Create `crates/erabi-domain/tests/workspace_contract.rs` after scaffolding only the directory:

```rust
#[test]
fn product_version_is_semver() {
    let version = env!("CARGO_PKG_VERSION");
    assert_eq!(version, "0.1.0");
}
```

- [ ] **Step 2: Create each crate using Cargo**

Run:

```bash
cargo new --lib crates/erabi-domain
cargo new --lib crates/erabi-db
cargo new --lib crates/erabi-api
cargo new --lib crates/erabi-jobs
cargo new --lib crates/erabi-crawler
cargo new --lib crates/erabi-crawl4ai
cargo new --lib crates/erabi-extraction
cargo new --lib crates/erabi-export
cargo new --lib crates/erabi-artifacts
cargo new --lib crates/erabi-security
cargo new --lib crates/erabi-observability
cargo new --bin crates/erabi-cli --name erabi
```

- [ ] **Step 3: Apply workspace package metadata to every crate**

Each crate `Cargo.toml` must use:

```toml
[package]
name = "erabi-domain"
version.workspace = true
edition.workspace = true
license.workspace = true
rust-version.workspace = true

[lints]
workspace = true
```

Use the corresponding package name for every crate. Keep `erabi-cli` package name as `erabi`.

- [ ] **Step 4: Replace generated placeholder code with explicit crate documentation**

For example, create `crates/erabi-domain/src/lib.rs`:

```rust
#![doc = "Pure Erabi domain types, invariants, and state transitions."]
```

Use equivalent one-line crate documentation matching each crate responsibility. Create `crates/erabi-cli/src/main.rs`:

```rust
fn main() {
    println!("erabi 0.1.0");
}
```

- [ ] **Step 5: Run the workspace tests**

Run: `cargo test --workspace`

Expected: all crates compile and `product_version_is_semver` passes.

- [ ] **Step 6: Run formatting and linting**

Run:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock crates
git commit -m "chore: scaffold Erabi Rust boundaries"
```

### Task 3: Scaffold the SvelteKit SPA with Bun

**Files:**
- Create: `apps/web/package.json`
- Create: `apps/web/svelte.config.js`
- Create: `apps/web/vite.config.ts`
- Create: `apps/web/tsconfig.json`
- Create: `apps/web/src/app.html`
- Create: `apps/web/src/app.css`
- Create: `apps/web/src/routes/+layout.ts`
- Create: `apps/web/src/routes/+layout.svelte`
- Create: `apps/web/src/routes/+page.svelte`
- Create: `apps/web/src/routes/start/+page.svelte`
- Test: `apps/web/src/routes/start/start-page.test.ts`

**Interfaces:**
- Produces: static SPA build under `apps/web/build`.
- Produces: `/` redirect to `/start`.
- Produces: English translation-key-ready shell, without feature UI yet.

- [ ] **Step 1: Add stable frontend dependencies using Bun**

Run from `apps/web` after creating an empty package:

```bash
mkdir -p apps/web
cd apps/web
bun init -y
bun add -d @sveltejs/kit @sveltejs/adapter-static @sveltejs/vite-plugin-svelte svelte vite typescript svelte-check vitest jsdom @testing-library/svelte @testing-library/jest-dom
cd ../..
```

Remove any generated npm lockfile. Keep the root `bun.lock` only.

- [ ] **Step 2: Write the failing Start page test**

Create `apps/web/src/routes/start/start-page.test.ts`:

```ts
import { render, screen } from "@testing-library/svelte";
import { describe, expect, it } from "vitest";
import StartPage from "./+page.svelte";

describe("Start page", () => {
  it("focuses the user on a URL input", () => {
    render(StartPage);
    expect(screen.getByRole("textbox", { name: "Website URL" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Scrape" })).toBeVisible();
  });
});
```

- [ ] **Step 3: Configure SvelteKit as a static SPA**

Create `apps/web/svelte.config.js`:

```js
import adapter from "@sveltejs/adapter-static";
import { vitePreprocess } from "@sveltejs/vite-plugin-svelte";

export default {
  preprocess: vitePreprocess(),
  kit: {
    adapter: adapter({ fallback: "index.html" })
  }
};
```

Create `apps/web/vite.config.ts`:

```ts
import { sveltekit } from "@sveltejs/kit/vite";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [sveltekit()],
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test-setup.ts"],
    include: ["src/**/*.test.ts"]
  }
});
```

Create `apps/web/src/test-setup.ts`:

```ts
import "@testing-library/jest-dom/vitest";
```

- [ ] **Step 4: Create the package scripts**

Replace `apps/web/package.json` with:

```json
{
  "name": "@erabi/web",
  "private": true,
  "type": "module",
  "scripts": {
    "dev": "vite dev",
    "build": "vite build",
    "preview": "vite preview",
    "check": "svelte-kit sync && svelte-check --tsconfig ./tsconfig.json",
    "test": "vitest run"
  },
  "devDependencies": {}
}
```

After replacement, rerun this exact command so Bun writes the resolved stable dependencies back into `devDependencies`:

```bash
cd apps/web
bun add -d @sveltejs/kit @sveltejs/adapter-static @sveltejs/vite-plugin-svelte svelte vite typescript svelte-check vitest jsdom @testing-library/svelte @testing-library/jest-dom
cd ../..
```

- [ ] **Step 5: Implement the minimal route shell**

Create `apps/web/src/routes/+layout.ts`:

```ts
export const ssr = false;
export const prerender = true;
```

Create `apps/web/src/routes/+page.svelte`:

```svelte
<script lang="ts">
  import { goto } from "$app/navigation";
  import { onMount } from "svelte";
  onMount(() => void goto("/start", { replaceState: true }));
</script>
```

Create `apps/web/src/routes/start/+page.svelte`:

```svelte
<svelte:head><title>Start | Erabi</title></svelte:head>

<main>
  <h1>Turn messy websites into trusted data</h1>
  <form>
    <label for="url">Website URL</label>
    <input id="url" name="url" type="url" required />
    <button type="submit">Scrape</button>
  </form>
</main>
```

- [ ] **Step 6: Run frontend tests and checks**

Run:

```bash
bun install
bun --cwd apps/web run test
bun --cwd apps/web run check
bun --cwd apps/web run build
```

Expected: all commands PASS and `apps/web/build/index.html` exists.

- [ ] **Step 7: Commit**

```bash
git add apps/web package.json bun.lock
git commit -m "chore: scaffold Erabi SvelteKit SPA"
```

### Task 4: Add UUIDv7 Identifiers and Time Primitives

**Files:**
- Create: `crates/erabi-domain/src/id.rs`
- Create: `crates/erabi-domain/src/time.rs`
- Modify: `crates/erabi-domain/src/lib.rs`
- Test: `crates/erabi-domain/tests/id_contract.rs`

**Interfaces:**
- Produces: `EntityId::new()`, `EntityId::parse(&str)`, `EntityId::as_uuid()`.
- Produces: `Timestamp` as UTC RFC3339-serializable time.

- [ ] **Step 1: Add stable dependencies**

Run:

```bash
cargo add -p erabi-domain uuid --features v7,serde
cargo add -p erabi-domain time --features serde,formatting,parsing,macros
cargo add -p erabi-domain serde --features derive
cargo add -p erabi-domain thiserror
```

- [ ] **Step 2: Write failing identifier tests**

Create `crates/erabi-domain/tests/id_contract.rs`:

```rust
use erabi_domain::EntityId;

#[test]
fn generated_ids_are_uuid_v7_and_time_ordered() {
    let first = EntityId::new();
    let second = EntityId::new();
    assert_eq!(first.as_uuid().get_version_num(), 7);
    assert_eq!(second.as_uuid().get_version_num(), 7);
    assert!(first <= second);
}

#[test]
fn ids_round_trip_through_canonical_strings() {
    let id = EntityId::new();
    assert_eq!(EntityId::parse(&id.to_string()).unwrap(), id);
}
```

- [ ] **Step 3: Implement the identifier value object**

Create `crates/erabi-domain/src/id.rs`:

```rust
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EntityId(Uuid);

#[derive(Debug, Error)]
#[error("invalid entity id: {0}")]
pub struct ParseEntityIdError(uuid::Error);

impl EntityId {
    #[must_use]
    pub fn new() -> Self { Self(Uuid::now_v7()) }
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid { &self.0 }
    pub fn parse(value: &str) -> Result<Self, ParseEntityIdError> {
        Uuid::parse_str(value).map(Self).map_err(ParseEntityIdError)
    }
    #[must_use]
    pub fn into_bytes(self) -> [u8; 16] { *self.0.as_bytes() }
    #[must_use]
    pub fn from_bytes(bytes: [u8; 16]) -> Self { Self(Uuid::from_bytes(bytes)) }
}

impl Default for EntityId { fn default() -> Self { Self::new() } }
impl fmt::Display for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { self.0.fmt(f) }
}
impl FromStr for EntityId {
    type Err = ParseEntityIdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> { Self::parse(s) }
}
```

Create `crates/erabi-domain/src/time.rs`:

```rust
pub type Timestamp = time::OffsetDateTime;

#[must_use]
pub fn now_utc() -> Timestamp {
    time::OffsetDateTime::now_utc()
}
```

Export both modules from `lib.rs`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p erabi-domain --test id_contract`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock crates/erabi-domain
git commit -m "feat(domain): add UUIDv7 identifiers"
```

### Task 5: Define Stable Product Errors and Lifecycle Enums

**Files:**
- Create: `crates/erabi-domain/src/error.rs`
- Create: `crates/erabi-domain/src/status.rs`
- Modify: `crates/erabi-domain/src/lib.rs`
- Test: `crates/erabi-domain/tests/status_contract.rs`

**Interfaces:**
- Produces: `ErrorCode`, `ProductError`, `SuggestedAction`.
- Produces: `SourceStatus`, `CrawlRunStatus`, `CrawlResultKind`, `DatasetStatus`, `ReviewStatus`, `RecordStatus`, `JobStatus`.

- [ ] **Step 1: Write failing status serialization tests**

Create `crates/erabi-domain/tests/status_contract.rs`:

```rust
use erabi_domain::{CrawlRunStatus, RecordStatus, ReviewStatus};

#[test]
fn statuses_use_stable_screaming_snake_case_names() {
    assert_eq!(serde_json::to_string(&CrawlRunStatus::PartialResult).unwrap(), "\"PARTIAL_RESULT\"");
    assert_eq!(serde_json::to_string(&RecordStatus::MissingCandidate).unwrap(), "\"MISSING_CANDIDATE\"");
    assert_eq!(serde_json::to_string(&ReviewStatus::ClosedWithUnresolvedItems).unwrap(), "\"CLOSED_WITH_UNRESOLVED_ITEMS\"");
}
```

- [ ] **Step 2: Add the test dependency**

Run: `cargo add -p erabi-domain --dev serde_json`.

- [ ] **Step 3: Implement exact lifecycle enums**

Create `crates/erabi-domain/src/status.rs` with `Serialize`, `Deserialize`, `Clone`, `Copy`, `Debug`, `Eq`, and `PartialEq` derives and `#[serde(rename_all = "SCREAMING_SNAKE_CASE")]` on each enum:

```rust
pub enum SourceStatus { Active, CrawlFailed, AccessDenied, NotFound, SchemaDrift, ContentChanged, PartialResult, Archived, Trashed }
pub enum CrawlRunStatus { Queued, Running, Succeeded, PartialResult, Failed, Cancelled }
pub enum CrawlResultKind { Changed, NoChanges }
pub enum DatasetStatus { Draft, PartiallyApproved, Approved, Superseded }
pub enum ReviewStatus { Open, Closed, ClosedWithUnresolvedItems, Reopened }
pub enum RecordStatus { Draft, Approved, Rejected, NewCandidate, UpdatedCandidate, MissingCandidate, Deleted, RestoredCandidate, Superseded }
pub enum JobStatus { Queued, Running, Recoverable, Succeeded, Failed, Cancelled, BlockedLowStorage }
```

- [ ] **Step 4: Implement stable product errors**

Create `crates/erabi-domain/src/error.rs`:

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidInput,
    Conflict,
    NotFound,
    SchemaDrift,
    ValidationFailed,
    CrawlerUnavailable,
    CrawlerTimeout,
    PartialResult,
    LowStorage,
    MigrationFailed,
    IntegrityFailed,
    Unauthorized,
    ForbiddenOrigin,
    UnsupportedMediaType,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SuggestedAction {
    ReviewSelectors,
    UseAnyway,
    Retry,
    Resume,
    Restart,
    OpenSettings,
    RunDiagnostics,
    RestoreBackup,
    Cancel,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProductError {
    pub code: ErrorCode,
    pub message: String,
    pub details: Value,
    pub recoverable: bool,
    pub suggested_actions: Vec<SuggestedAction>,
}
```

Export the modules and types from `lib.rs`.

- [ ] **Step 5: Run tests and clippy**

Run:

```bash
cargo test -p erabi-domain
cargo clippy -p erabi-domain --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add Cargo.lock crates/erabi-domain
git commit -m "feat(domain): define lifecycle states and errors"
```

### Task 6: Model Collections, Sources, and Auto-Naming

**Files:**
- Create: `crates/erabi-domain/src/collection.rs`
- Create: `crates/erabi-domain/src/source.rs`
- Create: `crates/erabi-domain/src/naming.rs`
- Modify: `crates/erabi-domain/src/lib.rs`
- Test: `crates/erabi-domain/tests/source_contract.rs`

**Interfaces:**
- Produces: `Collection`, `Source`, `SourceUrl`, `SourceType`.
- Produces: `derive_source_name()` and `derive_dataset_name()`.
- Enforces: Inbox through `collection_id: None`.

- [ ] **Step 1: Add URL parsing support**

Run: `cargo add -p erabi-domain url --features serde`.

- [ ] **Step 2: Write failing auto-naming tests**

Create `crates/erabi-domain/tests/source_contract.rs`:

```rust
use erabi_domain::{derive_dataset_name, derive_source_name, ReviewMode};

#[test]
fn source_name_prefers_page_title() {
    let name = derive_source_name(
        Some(" SCANDAL Announces New Album "),
        Some("Ignored OG title"),
        "https://example.com/news/album",
    );
    assert_eq!(name, "SCANDAL Announces New Album");
}

#[test]
fn source_name_falls_back_to_domain_and_path() {
    let name = derive_source_name(None, None, "https://shop.example.com/category/guitars");
    assert_eq!(name, "shop.example.com — category/guitars");
}

#[test]
fn dataset_name_includes_detected_mode() {
    assert_eq!(derive_dataset_name("Example Shop", ReviewMode::Records), "Example Shop — Records");
}
```

- [ ] **Step 3: Implement entities and naming rules**

Create `collection.rs` and `source.rs` using `EntityId`, `Timestamp`, and the fixed statuses. Include:

```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Collection {
    pub id: EntityId,
    pub name: String,
    pub slug: String,
    pub created_at: Timestamp,
    pub archived_at: Option<Timestamp>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Source {
    pub id: EntityId,
    pub collection_id: Option<EntityId>,
    pub name: String,
    pub original_url: url::Url,
    pub canonical_url: url::Url,
    pub source_type: SourceType,
    pub status: SourceStatus,
    pub created_at: Timestamp,
    pub archived_at: Option<Timestamp>,
    pub trashed_at: Option<Timestamp>,
}
```

Define `SourceType::{WebPage, FileAsset}` and `ReviewMode::{Document, Records}`.

Create `naming.rs` with deterministic trimming, host/path fallback, and `Untitled Source` fallback. Never fetch network data inside naming functions.

- [ ] **Step 4: Run tests**

Run: `cargo test -p erabi-domain --test source_contract`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock crates/erabi-domain
git commit -m "feat(domain): model collections and sources"
```

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

## Phase 2: Runtime and Infrastructure

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

### Task 13: Implement Access Token, Origin, Host, Content-Type, and Request Limits

**Files:**
- Create: `crates/erabi-security/src/auth.rs`
- Create: `crates/erabi-security/src/origin.rs`
- Create: `crates/erabi-security/src/headers.rs`
- Create: `crates/erabi-security/src/limits.rs`
- Create: `crates/erabi-security/src/lib.rs`
- Test: `crates/erabi-security/tests/security_contract.rs`

**Interfaces:**
- Produces: Axum/Tower middleware layers for bearer auth, Host/Origin validation, media-type enforcement, and body limits.
- Produces: strict security headers including CSP.

- [ ] **Step 1: Add stable dependencies**

Run:

```bash
cargo add -p erabi-security axum
cargo add -p erabi-security tower
cargo add -p erabi-security tower-http --features cors,limit,set-header,trace
cargo add -p erabi-security http
cargo add -p erabi-security subtle
cargo add -p erabi-security secrecy
cargo add -p erabi-security dashmap
cargo add -p erabi-security thiserror
cargo add -p erabi-security tracing
```

- [ ] **Step 2: Write failing middleware tests**

Create integration tests with an Axum test router proving:

```rust
#[tokio::test]
async fn local_mode_allows_request_without_token() { /* expect 200 */ }
#[tokio::test]
async fn network_mode_rejects_missing_or_wrong_bearer_token() { /* expect 401 */ }
#[tokio::test]
async fn mutation_rejects_untrusted_origin() { /* expect 403 FORBIDDEN_ORIGIN */ }
#[tokio::test]
async fn json_endpoint_rejects_text_plain() { /* expect 415 */ }
#[tokio::test]
async fn responses_include_csp_and_nosniff() { /* assert headers */ }
```

- [ ] **Step 3: Implement constant-time bearer comparison**

Parse only `Authorization: Bearer <token>`. Compare the expected and received byte slices using `subtle::ConstantTimeEq`. Never include either token in logs or error details. Add a small per-IP failed-auth limiter that returns 429 after the configured threshold and naturally expires entries.

- [ ] **Step 4: Implement same-origin and CORS policy**

Default CORS layer is absent. When an allowlist is configured, parse exact origins and allow only required methods/headers. Reject wildcard origins when an access token is configured. Validate `Host` against the bound host plus explicit trusted hosts.

- [ ] **Step 5: Implement content-type and body-size policy**

- JSON mutations accept only `application/json` and structured `+json` types.
- Backup upload accepts only `multipart/form-data`.
- Default JSON body limit: 1 MiB.
- URL batch body limit: 10 MiB.
- Backup upload limit: configurable, default 10 GiB.
- Asset upload is not exposed in the MVP.

- [ ] **Step 6: Implement security headers**

Set at least:

```text
Content-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; connect-src 'self'; frame-src 'self'; object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'
X-Content-Type-Options: nosniff
Referrer-Policy: no-referrer
Permissions-Policy: camera=(), microphone=(), geolocation=()
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Resource-Policy: same-origin
```

The sandbox preview route receives a separate, even stricter policy and never weakens the main UI policy.

- [ ] **Step 7: Run tests**

Run: `cargo test -p erabi-security`

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add Cargo.lock crates/erabi-security
git commit -m "feat(security): harden network and HTTP requests"
```

### Task 14: Build the Axum API Shell, Error Envelope, Static UI, and Local OpenAPI

**Files:**
- Create: `crates/erabi-api/src/state.rs`
- Create: `crates/erabi-api/src/error.rs`
- Create: `crates/erabi-api/src/router.rs`
- Create: `crates/erabi-api/src/routes/system.rs`
- Create: `crates/erabi-api/src/routes/audit.rs`
- Create: `crates/erabi-api/src/openapi.rs`
- Create: `crates/erabi-api/src/static_ui.rs`
- Modify: `crates/erabi-api/src/lib.rs`
- Test: `crates/erabi-api/tests/api_shell.rs`

**Interfaces:**
- Produces: `build_router(AppState, ApiConfig) -> Router`.
- Produces: `/api/v1/system/health`, paginated `/api/v1/audit-events`, `/api/v1/openapi.json`, Swagger UI, and SPA fallback.
- Produces: consistent `ApiErrorResponse` with trace ID.

- [ ] **Step 1: Add stable API dependencies**

Run:

```bash
cargo add -p erabi-api axum --features json,macros
cargo add -p erabi-api tokio --features sync
cargo add -p erabi-api tower
cargo add -p erabi-api tower-http --features fs,request-id,trace
cargo add -p erabi-api serde --features derive
cargo add -p erabi-api serde_json
cargo add -p erabi-api utoipa --features axum_extras,uuid,time
cargo add -p erabi-api utoipa-swagger-ui --features axum
cargo add -p erabi-api tracing
cargo add -p erabi-api uuid --features v7,serde
cargo add -p erabi-api http-body-util
cargo add -p erabi-api --path crates/erabi-domain erabi-domain
cargo add -p erabi-api --path crates/erabi-security erabi-security
```

- [ ] **Step 2: Write failing shell tests**

Test:

```rust
#[tokio::test]
async fn health_uses_the_versioned_api_path() { /* GET /api/v1/system/health -> 200 */ }
#[tokio::test]
async fn unknown_api_route_returns_json_error_with_trace_id() { /* 404 envelope */ }
#[tokio::test]
async fn local_openapi_is_available_when_enabled() { /* 200 */ }
#[tokio::test]
async fn spa_route_falls_back_to_index_html() { /* GET /start -> HTML */ }
```

- [ ] **Step 3: Implement the stable error envelope**

Use:

```rust
#[derive(serde::Serialize)]
pub struct ApiErrorBody {
    pub error: ApiError,
}

#[derive(serde::Serialize)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    pub details: serde_json::Value,
    pub recoverable: bool,
    pub suggested_actions: Vec<SuggestedAction>,
    pub trace_id: String,
}
```

Map domain error codes to deterministic HTTP status codes. Never serialize internal backtraces or raw SQL errors.

- [ ] **Step 4: Implement router construction**

Nest all product routes under `/api/v1`. Apply request ID, tracing, security, and body limit layers centrally. Serve static files from a configured web build directory and use `index.html` only for non-API GET routes. Add a read-only paginated Audit Events endpoint with filters for event type, entity, actor, date, and trace ID; it never exposes secret values or raw scraped content.

- [ ] **Step 5: Implement OpenAPI exposure rules**

- localhost and `openapi_enabled=true`: expose JSON and Swagger UI;
- non-loopback: disabled unless explicitly enabled;
- when enabled on network: auth middleware still applies;
- never document internal recovery mutation endpoints as public examples with secrets.

- [ ] **Step 6: Run API shell tests**

Run: `cargo test -p erabi-api --test api_shell`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.lock crates/erabi-api
git commit -m "feat(api): add hardened Axum application shell"
```

### Task 15: Implement Startup, Process Lock, Recovery Mode, and Three-Second Shutdown

**Files:**
- Create: `crates/erabi-cli/src/lock.rs`
- Create: `crates/erabi-cli/src/startup.rs`
- Create: `crates/erabi-cli/src/shutdown.rs`
- Create: `crates/erabi-cli/src/runtime.rs`
- Modify: `crates/erabi-cli/src/main.rs`
- Test: `crates/erabi-cli/tests/process_lock.rs`
- Test: `crates/erabi-cli/tests/recovery_startup.rs`

**Interfaces:**
- Produces: `RuntimeMode::{Normal,Recovery}` and `StartupReport`.
- Enforces: one process per canonical local data directory.
- Enforces: three-second graceful shutdown deadline.

- [ ] **Step 1: Add dependencies**

Run:

```bash
cargo add -p erabi fs2
cargo add -p erabi tokio --features full
cargo add -p erabi tokio-util --features rt
cargo add -p erabi serde --features derive
cargo add -p erabi serde_json
cargo add -p erabi tracing
cargo add -p erabi thiserror
cargo add -p erabi --path crates/erabi-db erabi-db
cargo add -p erabi --path crates/erabi-api erabi-api
cargo add -p erabi --path crates/erabi-observability erabi-observability
cargo add -p erabi --dev tempfile
```

- [ ] **Step 2: Write process lock tests**

Prove that the first lock succeeds, a second lock on the same canonical directory returns `AlreadyRunning` with PID/start/address metadata, and a lock on another directory succeeds.

- [ ] **Step 3: Implement lock metadata and stale recovery**

Create `.erabi.lock` in the data directory, acquire an exclusive OS lock with `fs2`, then write JSON:

```json
{"pid":18420,"started_at":"2026-07-22T16:00:00Z","version":"0.1.0","address":"http://127.0.0.1:7878"}
```

Never delete an actively locked file. A stale unlocked file may be overwritten after its metadata is recorded in a startup event.

- [ ] **Step 4: Implement the startup sequence**

Execute in order:

1. validate configuration;
2. create/canonicalize data directories;
3. acquire process lock;
4. initialize tracing;
5. open Turso;
6. acquire migration lock and run migrations;
7. run lightweight integrity checks;
8. verify artifact directory;
9. recover stale jobs;
10. health-check Crawl4AI without failing the UI;
11. build Axum and start workers;
12. emit `system.ready`.

A migration or integrity failure sets `RuntimeMode::Recovery`, starts read-only API/diagnostics, and does not start job workers.

- [ ] **Step 5: Implement the fixed shutdown deadline**

Use a root `CancellationToken`. On Ctrl+C/SIGTERM:

```rust
let deadline = std::time::Duration::from_secs(3);
root_cancel.cancel();
let _ = tokio::time::timeout(deadline, runtime.shutdown()).await;
```

`runtime.shutdown()` stops accepting mutations/jobs, checkpoints active work, flushes audit/error summaries, closes listeners, and releases the process lock. Any still-running job becomes `RECOVERABLE` at next startup.

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p erabi --test process_lock
cargo test -p erabi --test recovery_startup
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.lock crates/erabi-cli
git commit -m "feat(runtime): add startup recovery and graceful shutdown"
```

### Task 16: Define Durable Jobs, Attempts, Checkpoints, and Queue Repository

**Files:**
- Create: `crates/erabi-jobs/src/model.rs`
- Create: `crates/erabi-jobs/src/repository.rs`
- Create: `crates/erabi-jobs/src/checkpoint.rs`
- Create: `crates/erabi-jobs/src/error.rs`
- Modify: `crates/erabi-jobs/src/lib.rs`
- Create: `crates/erabi-db/src/jobs.rs`
- Test: `crates/erabi-jobs/tests/model_contract.rs`
- Test: `crates/erabi-db/tests/job_repository.rs`

**Interfaces:**
- Produces: `Job`, `JobKind`, `JobAttempt`, `JobLease`, `JobCheckpoint`.
- Produces: `JobRepository::{enqueue,claim_next,heartbeat,checkpoint,complete,fail,cancel,recover_stale}`.

- [ ] **Step 1: Add dependencies and crate links**

Run:

```bash
cargo add -p erabi-jobs async-trait
cargo add -p erabi-jobs serde --features derive
cargo add -p erabi-jobs serde_json
cargo add -p erabi-jobs thiserror
cargo add -p erabi-jobs tokio-util --features rt
cargo add -p erabi-jobs futures-util
cargo add -p erabi-jobs tracing
cargo add -p erabi-jobs --path crates/erabi-domain erabi-domain
cargo add -p erabi-db --path crates/erabi-jobs erabi-jobs
```

- [ ] **Step 2: Write state transition tests**

Test legal transitions:

```rust
assert!(JobStatus::Queued.can_transition_to(JobStatus::Running));
assert!(JobStatus::Running.can_transition_to(JobStatus::Recoverable));
assert!(!JobStatus::Succeeded.can_transition_to(JobStatus::Running));
```

Test that checkpoint JSON always includes `config_hash`, completed/pending units, failed units, and saved artifact IDs.

- [ ] **Step 3: Implement exact MVP job kinds**

```rust
pub enum JobKind {
    CrawlPage,
    DiscoverPagination,
    ExtractDataset,
    ValidateDataset,
    DownloadAsset,
    ExportDataset,
    CreateBackup,
    VerifyBackup,
    RestoreBackup,
    IntegrityCheck,
    RetentionCleanup,
}
```

A `Job` includes UUIDv7 ID, kind, status, priority, JSON payload, attempts, max attempts, schedule/start/heartbeat/finish timestamps, parent job, checkpoint, collection/domain keys, and immutable configuration hash.

- [ ] **Step 4: Implement the Turso queue repository**

`claim_next()` must execute atomically:

1. select the highest priority eligible `QUEUED` job ordered by `priority DESC, scheduled_at ASC, id ASC`;
2. ensure global/Collection/domain limits are not exceeded;
3. update it to `RUNNING`, assign lease owner and expiry, increment attempt;
4. return the claimed row;
5. commit.

Use a short transaction and optimistic `WHERE status='QUEUED'` update so two claimers cannot both succeed.

- [ ] **Step 5: Implement stale job recovery**

A `RUNNING` job with expired lease becomes:

- `RECOVERABLE` when a valid checkpoint exists and its config hash matches;
- `FAILED` otherwise, with `JOB_LEASE_EXPIRED` summary.

Never auto-resume a stale job before the UI/runtime policy explicitly chooses resume.

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p erabi-jobs
cargo test -p erabi-db --test job_repository
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.lock crates/erabi-jobs crates/erabi-db
git commit -m "feat(jobs): add durable queue and checkpoints"
```

### Task 17: Implement Tokio Workers, Concurrency Limits, Cancellation, and Panic Isolation

**Files:**
- Create: `crates/erabi-jobs/src/handler.rs`
- Create: `crates/erabi-jobs/src/worker.rs`
- Create: `crates/erabi-jobs/src/limits.rs`
- Create: `crates/erabi-jobs/src/recovery.rs`
- Modify: `crates/erabi-jobs/src/lib.rs`
- Test: `crates/erabi-jobs/tests/worker_runtime.rs`

**Interfaces:**
- Produces: `JobHandler`, `WorkerRuntime`, `ConcurrencyController`.
- Enforces: default one active job, two pages per job, Collection/domain semaphores, cooperative cancellation.
- Enforces: task panic isolation.

- [ ] **Step 1: Write worker behavior tests**

Use fake handlers to prove:

- priority ordering;
- global limit one by default;
- domain limit prevents simultaneous same-domain work;
- cancellation produces a checkpoint and `CANCELLED`;
- a handler panic fails only that job and the worker loop continues;
- shutdown marks unfinished work recoverable within the runtime deadline.

- [ ] **Step 2: Define the handler contract**

```rust
#[async_trait::async_trait]
pub trait JobHandler: Send + Sync {
    fn kind(&self) -> JobKind;
    async fn handle(&self, context: JobContext) -> Result<JobOutcome, JobError>;
}

pub struct JobContext {
    pub job: Job,
    pub cancellation: tokio_util::sync::CancellationToken,
    pub checkpoint: CheckpointWriter,
}
```

- [ ] **Step 3: Implement hierarchical concurrency controls**

Use `tokio::sync::Semaphore` for:

- global active jobs;
- per-Collection active jobs;
- per-domain active units;
- active browser pages.

Acquire permits in a fixed order: global → Collection → domain → browser. Drop in reverse order automatically through owned permits to prevent deadlocks.

- [ ] **Step 4: Isolate handler panics**

Wrap each handler future using `std::panic::AssertUnwindSafe` and `futures_util::FutureExt::catch_unwind`. On panic:

- record a redacted backtrace/error summary;
- checkpoint if possible;
- mark the job `RECOVERABLE` when checkpoint valid, otherwise `FAILED`;
- continue the worker loop.

A database invariant panic is promoted to the runtime critical-failure channel and triggers Recovery Mode.

- [ ] **Step 5: Run tests**

Run: `cargo test -p erabi-jobs --test worker_runtime`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/erabi-jobs Cargo.lock
git commit -m "feat(jobs): run isolated cancellable Tokio workers"
```

### Task 18: Persist Progress Events and Support SSE Replay

**Files:**
- Create: `crates/erabi-jobs/src/events.rs`
- Create: `crates/erabi-db/src/progress_events.rs`
- Create: `crates/erabi-api/src/routes/crawl_events.rs`
- Modify: `crates/erabi-api/src/router.rs`
- Test: `crates/erabi-api/tests/sse_replay.rs`

**Interfaces:**
- Produces: `ProgressEvent`, `ProgressEventStore`, `ProgressPublisher`.
- Produces: `GET /api/v1/crawl-runs/{id}/events` with `Last-Event-ID` replay.

- [ ] **Step 1: Write failing replay tests**

Persist three events with sequence 1–3, connect with `Last-Event-ID: 1`, and assert the stream first returns sequences 2 and 3 before live sequence 4. Test sequence monotonicity under concurrent publishers.

- [ ] **Step 2: Define stable event shape**

```rust
pub struct ProgressEvent {
    pub id: EntityId,
    pub crawl_run_id: EntityId,
    pub event_type: String,
    pub sequence: i64,
    pub timestamp: Timestamp,
    pub progress: Option<ProgressValue>,
    pub message_key: String,
    pub message_args: serde_json::Value,
    pub technical: serde_json::Value,
}
```

Use translation keys rather than persisted rendered English messages for user-facing progress.

- [ ] **Step 3: Implement event persistence and broadcast**

Inside one transaction, allocate the next sequence for the Crawl Run and insert the event. After commit, send it through a Tokio broadcast channel. If no subscriber exists, persistence still succeeds.

- [ ] **Step 4: Implement SSE replay**

The endpoint:

1. authenticates like every API endpoint;
2. parses `Last-Event-ID` as the last sequence, not the UUID event ID;
3. queries persisted events with a greater sequence;
4. streams them in order;
5. subscribes to live events;
6. sends keepalive comments;
7. serializes each event as JSON and sets SSE `id` to the sequence.

- [ ] **Step 5: Run tests**

Run: `cargo test -p erabi-api --test sse_replay`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add Cargo.lock crates/erabi-jobs crates/erabi-db crates/erabi-api
git commit -m "feat(progress): persist and replay crawl SSE events"
```

### Task 19: Define Crawler-Neutral Contracts and a Deterministic Mock Adapter

**Files:**
- Create: `crates/erabi-crawler/src/model.rs`
- Create: `crates/erabi-crawler/src/adapter.rs`
- Create: `crates/erabi-crawler/src/error.rs`
- Create: `crates/erabi-crawler/src/mock.rs`
- Modify: `crates/erabi-crawler/src/lib.rs`
- Test: `crates/erabi-crawler/tests/contract.rs`
- Create: `tests/fixtures/crawl4ai/single-page-success.json`
- Create: `tests/fixtures/crawl4ai/partial-pagination.json`

**Interfaces:**
- Produces: the fixed `CrawlerAdapter` trait.
- Produces: `CrawlRequest`, `CrawlOutput`, `CrawledPage`, `CrawlerHealth`, typed errors.
- Produces: `MockCrawlerAdapter` for PR CI and E2E tests.

- [ ] **Step 1: Add dependencies**

Run:

```bash
cargo add -p erabi-crawler async-trait
cargo add -p erabi-crawler serde --features derive
cargo add -p erabi-crawler serde_json
cargo add -p erabi-crawler thiserror
cargo add -p erabi-crawler url --features serde
cargo add -p erabi-crawler bytes --features serde
cargo add -p erabi-crawler --path crates/erabi-domain erabi-domain
```

- [ ] **Step 2: Write adapter contract tests**

Test that a mock success returns HTML, cleaned HTML, rendered DOM, Markdown, metadata, screenshot bytes when requested, links, and an external job ID. Test deterministic timeout, access denied, not found, and partial page fixtures.

- [ ] **Step 3: Define request settings without Crawl4AI-specific names**

`CrawlRequest` includes:

```rust
pub struct CrawlRequest {
    pub url: url::Url,
    pub user_agent: String,
    pub timeout_ms: u64,
    pub wait_for_selector: Option<String>,
    pub wait_for_network_idle: bool,
    pub auto_scroll: Option<AutoScrollConfig>,
    pub screenshot: bool,
    pub capture_links: bool,
}
```

- [ ] **Step 4: Define normalized output**

`CrawledPage` includes final URL, status code, content type, raw HTML, cleaned HTML, rendered DOM, Markdown, screenshot, discovered links, canonical URL, title, Open Graph title, response timing, and adapter metadata isolated in an opaque JSON field.

- [ ] **Step 5: Implement the mock adapter**

The mock is configured with a queue of responses. `crawl()` pops exactly one response, records the request, and returns a clear error when no response remains. `cancel()` records external IDs. This adapter must never access the network.

- [ ] **Step 6: Run tests**

Run: `cargo test -p erabi-crawler`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.lock crates/erabi-crawler tests/fixtures/crawl4ai
git commit -m "feat(crawler): define adapter contracts and mock"
```

### Task 20: Implement the Crawl4AI HTTP Adapter Without Modifying Crawl4AI

**Files:**
- Create: `crates/erabi-crawl4ai/src/client.rs`
- Create: `crates/erabi-crawl4ai/src/dto.rs`
- Create: `crates/erabi-crawl4ai/src/mapper.rs`
- Create: `crates/erabi-crawl4ai/src/lib.rs`
- Test: `crates/erabi-crawl4ai/tests/http_contract.rs`

**Interfaces:**
- Consumes: `CrawlerAdapter`, `CrawlRequest`, `CrawlOutput`.
- Produces: `Crawl4AiAdapter::new(Crawl4AiConfig)`.
- Uses: official Crawl4AI Docker HTTP API through configurable base URL and optional API token.

- [ ] **Step 1: Add stable HTTP dependencies**

Run:

```bash
cargo add -p erabi-crawl4ai reqwest --features json,rustls-tls,stream
cargo add -p erabi-crawl4ai serde --features derive
cargo add -p erabi-crawl4ai serde_json
cargo add -p erabi-crawl4ai async-trait
cargo add -p erabi-crawl4ai thiserror
cargo add -p erabi-crawl4ai tracing
cargo add -p erabi-crawl4ai url
cargo add -p erabi-crawl4ai --path crates/erabi-crawler erabi-crawler
cargo add -p erabi-crawl4ai --dev wiremock
cargo add -p erabi-crawl4ai --dev tokio --features macros,rt-multi-thread
```

- [ ] **Step 2: Write HTTP mapping tests against `wiremock`**

Cover:

- `GET /health` healthy and unavailable responses;
- `POST /crawl` request body mapping for timeout, wait selector, scroll, screenshot, and User-Agent;
- success DTO mapping to crawler-neutral output;
- 401/403 → `AccessDenied`;
- 404 target result → `NotFound`;
- HTTP timeout → recoverable `CrawlerTimeout`;
- malformed response → non-recoverable `InvalidResponse`;
- token header is sent but never included in Debug output.

- [ ] **Step 3: Implement API DTOs isolated from domain types**

Mirror only the stable response fields Erabi consumes. Put all optional upstream fields behind `Option` and `#[serde(default)]`. Do not expose DTOs from the crate public API.

- [ ] **Step 4: Implement request and response mapping**

Map `CrawlRequest` into the installed Crawl4AI server's `/crawl` schema. Keep endpoint path configurable inside one module so upstream changes affect only `client.rs`/`dto.rs`. Map output into `CrawledPage`, preserving unknown metadata only in the opaque adapter JSON.

- [ ] **Step 5: Implement cancellation best-effort semantics**

When the configured Crawl4AI server exposes a cancellation endpoint, invoke it. When it does not, cancel the local Reqwest request and return success with a technical warning event. Never claim remote cancellation succeeded when only local cancellation occurred.

- [ ] **Step 6: Run contract tests**

Run: `cargo test -p erabi-crawl4ai --test http_contract`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.lock crates/erabi-crawl4ai
git commit -m "feat(crawl4ai): add isolated HTTP adapter"
```

## Phase 3: Crawling, Extraction, and Curation

### Task 21: Implement Collections, Inbox, Sources, Duplicate URL Handling, and Start Scrape API

**Files:**
- Create: `crates/erabi-db/src/collections.rs`
- Create: `crates/erabi-db/src/sources.rs`
- Create: `crates/erabi-api/src/routes/collections.rs`
- Create: `crates/erabi-api/src/routes/sources.rs`
- Create: `crates/erabi-api/src/dto/sources.rs`
- Modify: `crates/erabi-api/src/router.rs`
- Test: `crates/erabi-api/tests/source_flow.rs`

**Interfaces:**
- Produces: `POST /api/v1/collections`, `GET /api/v1/collections`.
- Produces: `POST /api/v1/sources`, `GET /api/v1/sources`, `GET /api/v1/sources/{id}`.
- Produces: `POST /api/v1/sources/{id}/crawl` returning a durable queued Crawl Run.
- Enforces: `collection_id = null` means Inbox.

- [ ] **Step 1: Write API integration tests before repository code**

Test these exact behaviors:

```rust
#[tokio::test]
async fn creating_a_source_without_collection_puts_it_in_inbox() { /* POST source; collection_id null */ }

#[tokio::test]
async fn duplicate_url_returns_options_instead_of_silently_copying() {
    /* second POST -> 409 CONFLICT with OPEN_EXISTING, RECRAWL_EXISTING, CREATE_NEW_ANYWAY, CANCEL */
}

#[tokio::test]
async fn crawl_mutation_returns_queued_run_and_events_url() {
    /* POST /sources/{id}/crawl -> 202 with immutable snapshot hash */
}

#[tokio::test]
async fn direct_file_url_is_detected_and_offered_as_asset() {
    /* HEAD/content-type PDF -> SourceType::FileAsset and DOWNLOAD_ASSET action, no HTML extraction */
}
```

- [ ] **Step 2: Implement URL canonicalization**

Canonicalization must:

- lowercase scheme and host;
- remove default ports;
- remove fragments;
- remove known tracking query keys such as `utm_*`, `fbclid`, and `gclid`;
- preserve meaningful query parameters in sorted order;
- normalize an empty path to `/`;
- never follow the network.

Store both original and canonical URL. Before creating an HTML crawl, perform a bounded content-type probe when safe. A direct PDF, CSV, JSON, ZIP, image, or office-document URL becomes `SourceType::FileAsset` and the API offers `Download as Asset`; parsing the file into records is roadmap-only. If the probe is unavailable or ambiguous, continue with the normal Crawl4AI request and classify from its final content type.

- [ ] **Step 3: Implement repositories with parameterized SQL**

`SourceRepository::find_by_canonical_url()` returns every active, archived, and trashed match with status and Collection. `create()` accepts `allow_duplicate: bool`; when false and a match exists, return `SourceConflict` without inserting.

- [ ] **Step 4: Implement auto-naming at creation and post-crawl rename**

Before a crawl, use host/path fallback. After the first successful crawl, update an automatically generated name from page title/OG title only when the user has not manually renamed it. Persist `name_origin = AUTO | USER` in a migration amendment.

- [ ] **Step 5: Implement the start crawl mutation atomically**

Inside one transaction:

1. read Source and resolved settings;
2. construct and hash `CrawlConfigSnapshot`;
3. create `CrawlRun` in `QUEUED`;
4. create root `Job` with `CRAWL_PAGE` kind;
5. append `CRAWL_QUEUED` audit event;
6. commit;
7. publish `crawl.queued` progress.

Support `Idempotency-Key`; repeated identical requests return the original run.

- [ ] **Step 6: Run source flow tests**

Run: `cargo test -p erabi-api --test source_flow`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add migrations crates/erabi-db crates/erabi-api
git commit -m "feat(sources): create inbox sources and queued crawls"
```

### Task 22: Implement Safe Crawling Policies, Robots Rules, Rate Limits, and Large-Crawl Confirmation

**Files:**
- Create: `crates/erabi-crawler/src/robots.rs`
- Create: `crates/erabi-crawler/src/rate_limit.rs`
- Create: `crates/erabi-crawler/src/safety.rs`
- Create: `crates/erabi-api/src/routes/crawl_estimates.rs`
- Test: `crates/erabi-crawler/tests/robots_policy.rs`
- Test: `crates/erabi-crawler/tests/rate_limit.rs`
- Test: `crates/erabi-api/tests/large_crawl_confirmation.rs`

**Interfaces:**
- Produces: `RobotsPolicy`, `DomainRateLimiter`, `CrawlEstimate`, `SafetyDecision`.
- Enforces: robots and per-domain delay on by default; explicit audited override only.
- Enforces: 429 honors `Retry-After` and backs off without aggressive retry.

- [ ] **Step 1: Write robots parser tests**

Use fixtures covering exact `User-agent`, `Allow`, `Disallow`, wildcard user agent, longest-path precedence, comments, blank lines, and missing robots file. Tests must show:

- missing/404 robots allows crawling;
- a matching disallow blocks by default;
- explicit override permits the request and returns `override_required_for_audit = true`;
- Erabi's configured User-Agent is used for matching.

- [ ] **Step 2: Implement the minimal standards-focused robots parser**

Parse only the rules Erabi needs: groups, `User-agent`, `Allow`, `Disallow`, and optional `Crawl-delay`. Use longest matching path, with Allow winning equal specificity. Cache robots results per origin for a configurable duration. Do not attempt sitemap ingestion in the MVP.

- [ ] **Step 3: Write and implement rate limit tests**

Use Tokio paused time to prove:

- requests to one domain are separated by the resolved delay;
- different domains may proceed concurrently within global limits;
- `Retry-After: 5` delays the next request by at least five seconds;
- exponential retry is bounded and jittered;
- cancellation wakes a waiter.

Implement a per-domain next-allowed timestamp guarded by async synchronization. The resolved setting snapshot, not live settings, controls each run.

- [ ] **Step 4: Implement transparent customizable User-Agent behavior**

Default format:

```text
Erabi/{application-version} (+project-url)
```

Allow global, Collection, and per-run override. Reject empty/control-character values. Warn and require explicit confirmation for common impersonation strings such as `Googlebot`. Persist active User-Agent and override reason in the Crawl Run snapshot and audit trail.

- [ ] **Step 5: Implement large crawl estimate and confirmation**

Create `POST /api/v1/crawl-estimates` returning planned pages, expected requests, estimated storage, resolved delay, screenshot policy, and whether explicit confirmation is required. Require a confirmation token bound to the config hash when estimates exceed configured page, request, or storage thresholds.

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p erabi-crawler --test robots_policy
cargo test -p erabi-crawler --test rate_limit
cargo test -p erabi-api --test large_crawl_confirmation
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/erabi-crawler crates/erabi-api
git commit -m "feat(crawling): enforce safe crawl defaults"
```

### Task 23: Implement Single-Page and Batch Crawl Orchestration with Live Steps

**Files:**
- Create: `crates/erabi-jobs/src/handlers/crawl_page.rs`
- Create: `crates/erabi-jobs/src/handlers/crawl_batch.rs`
- Create: `crates/erabi-jobs/src/handlers/mod.rs`
- Create: `crates/erabi-api/src/routes/batch.rs`
- Modify: `crates/erabi-cli/src/runtime.rs`
- Test: `crates/erabi-jobs/tests/crawl_handler.rs`
- Test: `crates/erabi-api/tests/batch_urls.rs`

**Interfaces:**
- Produces: `CrawlPageHandler` and simple URL batch endpoint.
- Enforces: every URL becomes a separate Source/Draft.
- Publishes: user-friendly progress plus expandable technical events.

- [ ] **Step 1: Write handler sequence tests with `MockCrawlerAdapter`**

Assert the exact user event sequence for a normal page:

```text
crawl.started
crawl.robots_checked
page.loading
page.rendering
page.completed
artifact.saving
extraction.queued
crawl.completed
```

Test crawler timeout → `PARTIAL_RESULT` or `FAILED` according to whether a valid page artifact exists. Test cancelled handler writes checkpoint before final status.

- [ ] **Step 2: Implement the crawl handler pipeline**

The handler must:

1. load the immutable Crawl Run snapshot;
2. check robots and acquire rate-limit permit;
3. call `CrawlerAdapter::crawl`;
4. map access denied/not found/timeout into Source and Crawl Run states;
5. persist the page result and links through the artifact service;
6. update page counts;
7. enqueue extraction;
8. emit audit and progress events;
9. never directly approve data.

- [ ] **Step 3: Implement simple batch URL input**

`POST /api/v1/batches/urls` accepts a JSON array of URL strings and optional Collection ID. Validate each URL independently and return per-item outcome. Every accepted URL creates a separate Source, Crawl Run, and root job. Duplicate decisions may be supplied per URL or as one bulk policy.

- [ ] **Step 4: Limit MVP batch scope**

Do not add CSV, JSONL file upload, sitemap, or RSS ingestion. Enforce a configurable maximum count and request body size. Preserve input order in the response.

- [ ] **Step 5: Register handlers in the single process runtime**

Construct handlers with shared `Arc` dependencies and register one handler per `JobKind`. Confirm no handler creates its own database or HTTP client.

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p erabi-jobs --test crawl_handler
cargo test -p erabi-api --test batch_urls
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/erabi-jobs crates/erabi-api crates/erabi-cli
git commit -m "feat(crawling): orchestrate page and batch crawls"
```

### Task 24: Implement Pagination Detection, Confirmation, Partial Results, Retry, and Resume

**Files:**
- Create: `crates/erabi-crawler/src/pagination.rs`
- Create: `crates/erabi-jobs/src/handlers/discover_pagination.rs`
- Create: `crates/erabi-jobs/src/handlers/retry.rs`
- Create: `crates/erabi-api/src/routes/pagination.rs`
- Create: `crates/erabi-api/src/routes/recovery_actions.rs`
- Test: `crates/erabi-crawler/tests/pagination_detection.rs`
- Test: `crates/erabi-jobs/tests/retry_resume.rs`

**Interfaces:**
- Produces: `PaginationSuggestion`, `PaginationScope`, and confidence/evidence.
- Produces: confirm all, first N, custom range, or maximum pages.
- Produces: retry failed parts, rerun full crawl, cancel, and resume endpoints.

- [ ] **Step 1: Create deterministic pagination fixtures and tests**

Cover:

- `<link rel="next">`;
- anchors labelled Next, Older, More, or arrows;
- numbered pagination;
- URL `page=2` and `/page/2` patterns;
- false-positive navigation links;
- preview of the next URL;
- no automatic follow before confirmation.

- [ ] **Step 2: Implement heuristic detection with evidence**

Return candidates with confidence and evidence list. Deduplicate canonical URLs and reject cross-origin candidates by default. `PaginationSuggestion` remains a suggestion until the user confirms a scope.

- [ ] **Step 3: Implement confirmation endpoint**

`POST /api/v1/crawl-runs/{id}/pagination/confirm` accepts one of:

```json
{"mode":"ALL","maximum_pages":100}
{"mode":"FIRST_N","count":10}
{"mode":"RANGE","start":2,"end":20}
```

Resolve concrete page units and enqueue them. Persist the decision in the immutable run plan extension and audit event.

- [ ] **Step 4: Implement complete-snapshot calculation**

Set `complete_snapshot=true` only when every planned page succeeded, pagination scope is complete, extraction succeeded, schema healthy, unique keys valid, and run was not cancelled. A configured intentional maximum page stop yields `PARTIAL_RESULT`, not complete snapshot.

- [ ] **Step 5: Implement retry and resume**

- Retry Failed Parts creates child job attempts for exact failed page/task units.
- Rerun Full Crawl creates a new Crawl Run with a new snapshot from current settings.
- Resume uses the original snapshot/config hash and remaining checkpoint units.
- Resume is rejected when schema, unique-key config, or crawl-rule hash differs.
- Successful retry may combine with prior valid units and promote the run to complete only after all units succeed.

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p erabi-crawler --test pagination_detection
cargo test -p erabi-jobs --test retry_resume
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/erabi-crawler crates/erabi-jobs crates/erabi-api
git commit -m "feat(crawling): add confirmed pagination and recovery"
```

### Task 25: Persist Raw Crawl Artifacts, Screenshots, Logs, and Crawl Summaries

**Files:**
- Create: `crates/erabi-jobs/src/crawl_artifacts.rs`
- Create: `crates/erabi-db/src/artifacts.rs`
- Create: `crates/erabi-api/src/routes/artifacts.rs`
- Test: `crates/erabi-jobs/tests/crawl_artifacts.rs`
- Test: `crates/erabi-api/tests/artifact_access.rs`

**Interfaces:**
- Produces: immutable artifact snapshots linked to Crawl Run and page task.
- Produces: authenticated raw/cleaned/DOM/Markdown/screenshot/log download endpoints.
- Enforces: screenshot on for single page, off for batch by default.

- [ ] **Step 1: Write artifact set tests**

A successful single page with screenshot enabled must persist:

- raw HTML;
- cleaned HTML when supplied;
- rendered DOM;
- Markdown;
- structured metadata JSON;
- screenshot;
- technical log stream or file segment.

A batch page with inherited defaults must omit screenshot. Missing optional upstream content must not fail the crawl.

- [ ] **Step 2: Implement artifact persistence after crawler output**

Write every artifact atomically, then insert metadata rows in one database transaction. If database insertion fails, remove newly written unreferenced files. If file write fails, do not insert metadata or mark the page complete.

- [ ] **Step 3: Store an immutable Crawl Summary**

Persist page counts, records extracted, errors by stable code, completeness, final URLs, timing, active User-Agent, robots decision, screenshot setting, schema version, and config hash. Keep this summary permanently even when detailed logs/artifacts are removed by retention.

- [ ] **Step 4: Implement safe artifact access**

Only serve artifacts through ID lookup, never user-provided paths. Raw HTML is always downloaded as attachment. Sanitized preview uses its own endpoint and CSP. Screenshot/image responses set `nosniff` and known MIME type. Missing cleaned artifacts return 404 without affecting the Crawl Run.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p erabi-jobs --test crawl_artifacts
cargo test -p erabi-api --test artifact_access
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/erabi-jobs crates/erabi-db crates/erabi-api
git commit -m "feat(artifacts): persist immutable crawl evidence"
```

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

### Task 30: Persist Field-Level Provenance for Every Extracted Value

**Files:**
- Create: `crates/erabi-domain/src/provenance.rs`
- Create: `crates/erabi-db/src/provenance.rs`
- Create: `crates/erabi-api/src/routes/provenance.rs`
- Modify: `crates/erabi-extraction/src/extract.rs`
- Test: `crates/erabi-db/tests/provenance_repository.rs`
- Test: `crates/erabi-api/tests/provenance_drawer.rs`

**Interfaces:**
- Produces: `FieldProvenance` linked to record version and field name.
- Produces: `GET /api/v1/record-versions/{id}/provenance` and per-field lookup.
- Preserves: source URL, Crawl Run, raw artifact, node, selector, raw/normalized value, transformations, Schema Version, timestamp, artifact hash.

- [ ] **Step 1: Write provenance completeness tests**

For an extracted `price` field, assert all mandatory fields are present and the artifact hash matches the stored artifact. Approving, superseding, or rejecting the record must not delete provenance.

- [ ] **Step 2: Define provenance model**

```rust
pub struct FieldProvenance {
    pub id: EntityId,
    pub record_version_id: EntityId,
    pub field_name: String,
    pub source_url: url::Url,
    pub crawl_run_id: EntityId,
    pub artifact_id: EntityId,
    pub artifact_hash: String,
    pub node_id: String,
    pub selector: String,
    pub raw_value: serde_json::Value,
    pub normalized_value: serde_json::Value,
    pub transformations: Vec<String>,
    pub schema_version_id: Option<EntityId>,
    pub extracted_at: Timestamp,
}
```

- [ ] **Step 3: Persist records and provenance atomically**

The extraction job inserts Dataset Version, Record Versions, validation results, and all field provenance in one transaction after artifacts exist. Failure rolls back all database rows and leaves the Crawl Run recoverable.

- [ ] **Step 4: Implement provenance API actions**

Return data needed to highlight the source node, open the original URL, open raw/DOM artifacts, copy selector, inspect normalization, and navigate to Crawl Run/Schema Version. Do not render raw HTML through this endpoint.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p erabi-db --test provenance_repository
cargo test -p erabi-api --test provenance_drawer
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/erabi-domain crates/erabi-db crates/erabi-api crates/erabi-extraction
git commit -m "feat(provenance): trace every extracted field"
```

### Task 31: Implement Dataset and Record Versions, Unique Keys, and Semantic Change Detection

**Files:**
- Create: `crates/erabi-domain/src/dataset.rs`
- Create: `crates/erabi-domain/src/record.rs`
- Create: `crates/erabi-domain/src/change.rs`
- Create: `crates/erabi-db/src/datasets.rs`
- Create: `crates/erabi-db/src/records.rs`
- Create: `crates/erabi-jobs/src/handlers/compare_snapshot.rs`
- Test: `crates/erabi-domain/tests/change_detection.rs`
- Test: `crates/erabi-jobs/tests/snapshot_comparison.rs`

**Interfaces:**
- Produces: single/composite unique keys with normalization options and content-hash fallback.
- Produces: `NoChange`, `NewCandidate`, `UpdatedCandidate`, `MissingCandidate`, `RestoredCandidate` classification.
- Produces: exact/possible duplicate signals and explicit Keep Both, Keep A, Keep B, or Merge Manually decisions; never automatic merge.
- Enforces: missing only from complete snapshot.

- [ ] **Step 1: Write semantic comparison tests**

Cover:

- whitespace-only raw change → no meaningful change;
- URL tracking parameter change → no meaningful change;
- ignored `updated_at` field → no meaningful change;
- normalized title change → `UpdatedCandidate`;
- new unique key → `NewCandidate`;
- absent approved key in complete snapshot → `MissingCandidate`;
- absent key in partial/failed/cancelled snapshot → no missing candidate;
- deleted key reappears → `RestoredCandidate`;
- same canonical URL/content hash/unique key → exact duplicate signal;
- fuzzy title/content similarity → possible duplicate only, never automatic merge.

- [ ] **Step 2: Implement unique-key construction**

Support one or ordered composite fields. Options include trim, case sensitivity, URL normalization, and empty behavior. Reject duplicate/empty configured keys as validation errors. When no key configured, use a deterministic normalized content hash and mark it as fallback in metadata.

- [ ] **Step 3: Implement canonical semantic hashes**

Serialize normalized compared fields with sorted keys, excluding fields configured `ignore_in_change_detection`. Hash the canonical bytes with SHA-256. Raw artifact hashes remain separate and may change without review.

- [ ] **Step 4: Implement snapshot comparison**

Compare the new extracted Draft against current approved versions by unique key. Reuse approved versions for exact semantic matches. Create new Draft Record Versions only for new/updated/restored candidates. Create missing candidates only when `complete_snapshot=true` and all unique-key health checks pass.

- [ ] **Step 5: Implement no-change outcome**

When new, updated, and missing counts are all zero, set Crawl Run `SUCCEEDED` with result `NO_CHANGES`, store summary/artifacts/audit, and do not create a Review or empty Dataset Version.

Implement duplicate suggestions separately from semantic version matching. Exact signals are canonical URL, normalized content hash, and configured unique key. Possible duplicates use bounded fuzzy title/content similarity. Persist the evidence and user decision. Actions are Keep Both, Keep A, Keep B, and Merge Manually; Merge Manually opens normal Draft editing and never merges automatically. Bulk actions may keep first/latest/all or send selected items to review.

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p erabi-domain --test change_detection
cargo test -p erabi-jobs --test snapshot_comparison
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/erabi-domain crates/erabi-db crates/erabi-jobs
git commit -m "feat(versioning): detect meaningful record changes"
```

### Task 32: Implement Review, Draft Autosave, Approval, Rejection, Diff, and Close/Reopen

**Files:**
- Create: `crates/erabi-domain/src/review.rs`
- Create: `crates/erabi-db/src/reviews.rs`
- Create: `crates/erabi-api/src/routes/reviews.rs`
- Create: `crates/erabi-api/src/routes/records.rs`
- Create: `crates/erabi-api/src/routes/approvals.rs`
- Test: `crates/erabi-api/tests/review_workflow.rs`
- Test: `crates/erabi-db/tests/approval_atomicity.rs`

**Interfaces:**
- Produces: Review listing, Grid/Card data, Draft cell update, bulk approve valid, reject, field diff decisions, missing/deleted/restore decisions, close/reopen.
- Enforces: approved versions immutable and validation errors non-overridable.

- [ ] **Step 1: Write complete workflow tests**

Test:

- scrape Draft auto-opens an `OPEN` Review;
- Draft update with correct expected revision autosaves;
- stale revision returns 409;
- approval locks Record Version;
- editing approved returns conflict and requires new version;
- `Approve All Valid` approves valid, skips invalid, leaves Dataset `PARTIALLY_APPROVED`;
- warning records approve without confirmation;
- single rejection reason optional;
- bulk rejection reason required;
- close with unresolved items requires confirmation and creates `CLOSED_WITH_UNRESOLVED_ITEMS`;
- reopen records audit event;
- recrawl change exposes per-field diff.

- [ ] **Step 2: Implement Draft autosave**

`PATCH /api/v1/record-versions/{id}/draft` accepts field changes and `expected_revision`. Validate immediately, store raw manual override separately from crawler provenance, increment Draft revision, and return `SAVING/SAVED` compatible state. Do not add Undo/Redo history.

- [ ] **Step 3: Implement atomic approval transaction**

Inside one transaction:

1. verify no validation errors;
2. verify expected revision/current pointer;
3. mark prior approved version `SUPERSEDED` when present;
4. mark Draft version `APPROVED` and immutable;
5. update Record current pointer/status;
6. update Dataset Version status;
7. insert approval row;
8. append audit event;
9. commit.

- [ ] **Step 4: Implement partial bulk approval**

Process selected records in bounded batches. Valid records commit; invalid records stay Draft. Return exact approved/skipped/warning counts. The operation must never silently approve an invalid record.

- [ ] **Step 5: Implement rejection and candidate decisions**

Preserve raw data/provenance. Support preset/free-text reason. Missing candidate actions: mark deleted, keep active, ignore this run, recrawl, open source. Restored candidate requires explicit confirmation. Record deletion is a versioned event, not physical removal.

- [ ] **Step 6: Implement Close/Reopen Review**

Close normal when no unresolved items. With unresolved items, require `confirm_unresolved=true`, set special status, and include summary. Close does not alter Record states. Reopen may occur at any time and is audited.

- [ ] **Step 7: Run tests**

Run:

```bash
cargo test -p erabi-api --test review_workflow
cargo test -p erabi-db --test approval_atomicity
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/erabi-domain crates/erabi-db crates/erabi-api
git commit -m "feat(review): curate immutable approved data"
```

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

## Phase 4: Exports, Retention, Backup, and Operations

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

## Phase 5: Product UI

### Task 41: Build the Typed API Client, Application Shell, Start Page, and Recent Activity

**Files:**
- Create: `apps/web/src/lib/api/client.ts`
- Create: `apps/web/src/lib/api/errors.ts`
- Create: `apps/web/src/lib/types/api.ts`
- Create: `apps/web/src/lib/components/layout/AppShell.svelte`
- Create: `apps/web/src/lib/components/layout/Sidebar.svelte`
- Create: `apps/web/src/lib/features/start/UrlScrapeForm.svelte`
- Create: `apps/web/src/lib/features/start/GettingStarted.svelte`
- Create: `apps/web/src/lib/features/start/RecentActivity.svelte`
- Create: `apps/web/src/lib/features/auth/AccessTokenPrompt.svelte`
- Modify: `apps/web/src/routes/+layout.svelte`
- Modify: `apps/web/src/routes/start/+page.svelte`
- Test: `apps/web/src/lib/features/start/start-flow.test.ts`

**Interfaces:**
- Produces: same-origin typed API client with bearer token from session/local storage.
- Produces: Start as default page and approved sidebar navigation.
- Produces: simple URL → Scrape interaction, advanced options collapsed, recent activity, non-blocking first-run checklist.

- [ ] **Step 1: Write failing Start flow tests**

Create tests asserting:

```ts
it("submits a single URL and navigates to live progress", async () => { /* mock POST; expect /crawl-runs/{id} */ });
it("keeps advanced options collapsed by default", () => { /* no rate-limit fields visible */ });
it("shows recent drafts and failed runs before ordinary activity", () => { /* ordered cards */ });
it("shows first-run readiness without a blocking wizard", () => { /* checklist and usable URL field */ });
```

- [ ] **Step 2: Implement a typed API client**

`apiRequest<T>()` must:

- use relative `/api/v1` paths;
- send JSON `Content-Type` for JSON mutations;
- add `Authorization: Bearer` only when a stored token exists;
- store the token in `sessionStorage` by default; use `localStorage` only after explicit `Remember on this device`; provide `Forget token` and never mirror the token into application settings or URLs;
- add an `Idempotency-Key` UUID for crawl/export/backup mutations;
- parse the stable API error envelope into `ErabiApiError`;
- never put tokens in URLs;
- handle 401 by emitting an auth-required event rather than logging token data.

Use browser `crypto.randomUUID()` for request idempotency only; domain IDs still come from backend UUIDv7. Implement `AccessTokenPrompt` on 401/network mode, with password-style input, explicit Remember checkbox, and no token value rendered after submit.

- [ ] **Step 3: Implement the shell and sidebar**

Sidebar order is fixed:

```text
Start
Inbox
Collections
Crawl Runs
Schemas
Datasets
Assets
Exports
Settings
```

Show compact crawler/queue/storage status at the bottom. Use real links, semantic navigation, visible active state, and mobile drawer behavior.

- [ ] **Step 4: Implement the Start form**

Primary view contains headline, labelled URL input, Scrape button, and collapsed Advanced options. Advanced options include Collection, existing Schema suggestion, screenshot, wait selector, auto-scroll, User-Agent, rate-limit override, and crawler connection summary. Start submits single page by default.

- [ ] **Step 5: Implement Recent Activity and Getting Started**

Fetch readiness, recent Sources, Drafts awaiting review, failed/partial runs, and recent exports. Priority order: action-required items then latest normal activity. Getting Started checklist shows database, artifacts, Crawl4AI, and first scrape; it never blocks the form.

- [ ] **Step 6: Run tests and checks**

Run:

```bash
bun --cwd apps/web run test -- start-flow
bun --cwd apps/web run check
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add apps/web bun.lock
git commit -m "feat(web): add Start page and application shell"
```

### Task 42: Implement Crawl Progress, SSE Reconnect, Technical Logs, Cancel, Retry, and Resume UI

**Files:**
- Create: `apps/web/src/lib/api/sse.ts`
- Create: `apps/web/src/lib/features/crawls/CrawlProgress.svelte`
- Create: `apps/web/src/lib/features/crawls/ProgressSteps.svelte`
- Create: `apps/web/src/lib/features/crawls/TechnicalLogs.svelte`
- Create: `apps/web/src/lib/features/crawls/CrawlActions.svelte`
- Create: `apps/web/src/routes/crawl-runs/[id]/+page.svelte`
- Test: `apps/web/src/lib/features/crawls/crawl-progress.test.ts`

**Interfaces:**
- Produces: replay-capable SSE client using `Last-Event-ID` semantics.
- Produces: live friendly steps separated from technical logs.
- Produces: Cancel, Resume from Checkpoint, Retry Failed Parts, Rerun Full Crawl.

- [ ] **Step 1: Write failing SSE and rendering tests**

Test:

- persisted events render in sequence;
- disconnect reconnects from last sequence and does not duplicate events;
- progress uses message translation keys/args;
- technical log panel stays collapsed by default;
- screen-reader live region announces stage changes but not every technical log;
- Cancel changes UI to checkpointing/cancelled;
- `PARTIAL_RESULT` exposes review/debug/retry but never claims complete snapshot.

- [ ] **Step 2: Implement authenticated SSE fetch streaming**

Native `EventSource` cannot set Authorization headers. Implement `fetch()` with streaming `ReadableStream`, `Accept: text/event-stream`, bearer header, parser for `id`, `event`, `data`, and reconnect with last sequence. Use bounded exponential reconnect and stop on explicit cancellation/terminal status.

- [ ] **Step 3: Implement friendly progress steps**

Display Preparing crawler, Checking robots, Loading, Rendering, Waiting/Scrolling, Pagination, Extracting, Validating, Saving Draft, Assets, Complete. Show completed/planned page and record counts. Preserve last known state on reconnect.

- [ ] **Step 4: Implement structured technical logs**

Filters: level, module, event, job/page, text search. Main rows show time, level, concise event, duration/status. Expand reveals trace/job/run/source, code, recoverable, retry unit, redacted context, stack trace, Copy, View Context, Retry. Never interpolate untrusted HTML.

- [ ] **Step 5: Implement terminal navigation**

On successful extraction Draft creation, automatically navigate to Review. Keep a secondary notification/action for Crawl More Pages or Select Links. `NO_CHANGES` stays on run summary and states no review required.

- [ ] **Step 6: Run tests**

Run: `bun --cwd apps/web run test -- crawl-progress`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add apps/web
git commit -m "feat(web): stream crawl progress and recovery actions"
```

### Task 43: Build the Three-Panel Visual Extraction Editor

**Files:**
- Create: `apps/web/src/lib/features/extraction/ExtractionEditor.svelte`
- Create: `apps/web/src/lib/features/extraction/PagePreview.svelte`
- Create: `apps/web/src/lib/features/extraction/FieldEditor.svelte`
- Create: `apps/web/src/lib/features/extraction/RecordPreview.svelte`
- Create: `apps/web/src/lib/features/extraction/DomTree.svelte`
- Create: `apps/web/src/lib/features/extraction/editor-state.svelte.ts`
- Create: `apps/web/src/routes/reviews/[id]/extract/+page.svelte`
- Test: `apps/web/src/lib/features/extraction/extraction-editor.test.ts`

**Interfaces:**
- Produces: Preview | Field Configuration | Record Preview three-panel desktop layout and tabbed small-screen layout.
- Produces: bidirectional Preview ↔ Field ↔ Record highlighting.
- Produces: one-container visual selection, manual selector entry, live preview, Schema Draft autosave.

- [ ] **Step 1: Write editor interaction tests**

Assert:

- clicking a mapped preview node selects container/field;
- hovering a field highlights all matching nodes;
- choosing Record 8 highlights the eighth container;
- keyboard DOM tree can select the same node without pointer;
- manual selector update refreshes preview;
- fragile selector warning is visible text, not color only;
- switching Document/Records mode calls extraction preview, not recrawl;
- stale preview responses are discarded;
- autosave state shows Editing, Saving, Saved, Failed/Retry.

- [ ] **Step 2: Implement isolated sandbox preview messaging**

Render the sanitized preview endpoint inside `<iframe sandbox="allow-same-origin">` without script permission. Because scripts cannot run in the frame, overlay a same-origin selection layer using node bounding boxes returned by the backend. Do not depend on executing source-site or injected scripts. Pointer coordinates select backend node boxes; keyboard uses DOM tree.

- [ ] **Step 3: Implement editor state with Svelte runes**

Store mode, selected container/node, fields, temporary Schema definition, highlighted nodes, selected record, preview request generation, autosave revision, and validation. Debounce extraction preview/autosave; abort prior fetch when a new request begins.

- [ ] **Step 4: Implement container and field workflows**

Container stage shows detected similar count and highlights matches. Field stage shows name, type, relative selector, value source/attribute, coverage, samples, required/optional, normalization, validation, unique-key participation, and delete/add/manual point actions.

- [ ] **Step 5: Implement Record Preview**

Default Grid with paginated records and Card toggle. Updates immediately from backend preview. Selecting a record updates source highlight. Surface missing/error/warning counts and coverage. Do not allow approval from temporary unsaved extraction state.

- [ ] **Step 6: Run tests and accessibility checks**

Run:

```bash
bun --cwd apps/web run test -- extraction-editor
bun --cwd apps/web run check
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add apps/web
git commit -m "feat(web): add visual extraction editor"
```

### Task 44: Build Review Grid/Card Views, Provenance Drawer, Diff, Approval, Rejection, and Close/Reopen

**Files:**
- Create: `apps/web/src/lib/features/review/ReviewPage.svelte`
- Create: `apps/web/src/lib/features/review/RecordGrid.svelte`
- Create: `apps/web/src/lib/features/review/RecordCards.svelte`
- Create: `apps/web/src/lib/features/review/ProvenanceDrawer.svelte`
- Create: `apps/web/src/lib/features/review/DiffReview.svelte`
- Create: `apps/web/src/lib/features/review/ValidationSummary.svelte`
- Create: `apps/web/src/lib/features/review/ReviewActions.svelte`
- Create: `apps/web/src/routes/reviews/[id]/+page.svelte`
- Test: `apps/web/src/lib/features/review/review-workflow.test.ts`

**Interfaces:**
- Produces: Grid default, Card optional, inline Draft editing/autosave, provenance, validation filters, approval/rejection, change decisions, Close/Reopen.

- [ ] **Step 1: Write end-user behavior tests**

Test:

- errors visibly disable approval and cannot be overridden;
- warnings remain approvable without confirmation;
- Approve All Valid reports approved/skipped/warning counts;
- inline Draft edit shows save state and conflict handling;
- approved cells are locked and offer Create New Version;
- per-field provenance opens source, raw/normalized, selector, transformations, Schema/Crawl links;
- diff accepts all/selected/keep old/reject new;
- bulk rejection requires reason, single rejection does not;
- closing unresolved Review displays counts and explicit confirmation;
- `CLOSED_WITH_UNRESOLVED_ITEMS` is not labelled complete.

- [ ] **Step 2: Implement an accessible paginated data grid**

Use semantic table markup, sticky headers, keyboard cell navigation, row selection checkboxes with labels, sort/filter controls, validation icons plus text, and provenance buttons per cell. Avoid an external grid dependency in the MVP. Card View provides a simpler alternative.

- [ ] **Step 3: Implement Draft edits and conflicts**

Debounce per-cell PATCH with expected revision. Show Saving/Saved/Failed. On 409, stop autosave and show Reload latest / Compare choices; never overwrite silently. Before navigation, warn only when a local request is still unsent/in-flight.

- [ ] **Step 4: Implement provenance and highlight integration**

Drawer links to the extraction preview with selected node. Original URL opens in a new tab with `noopener,noreferrer`. Raw HTML downloads rather than embeds. Copy selector/value actions use plain text.

- [ ] **Step 5: Implement candidate workflows**

New/Updated/Missing/Restored badges include text. Diff review supports field decisions. Missing actions match the API. Deleted/old approved versions remain accessible in history.

- [ ] **Step 6: Run tests**

Run: `bun --cwd apps/web run test -- review-workflow`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add apps/web
git commit -m "feat(web): add provenance-driven review workflow"
```

### Task 45: Build Inbox, Collections, Runs, Schemas, Datasets, Assets, Exports, Backup, Trash, Settings, Diagnostics, and Search Pages

**Files:**
- Create: `apps/web/src/routes/inbox/+page.svelte`
- Create: `apps/web/src/routes/collections/+page.svelte`
- Create: `apps/web/src/routes/crawl-runs/+page.svelte`
- Create: `apps/web/src/routes/schemas/+page.svelte`
- Create: `apps/web/src/routes/datasets/+page.svelte`
- Create: `apps/web/src/routes/assets/+page.svelte`
- Create: `apps/web/src/routes/exports/+page.svelte`
- Create: `apps/web/src/routes/settings/+page.svelte`
- Create: `apps/web/src/routes/settings/backup/+page.svelte`
- Create: `apps/web/src/routes/settings/diagnostics/+page.svelte`
- Create: `apps/web/src/routes/trash/+page.svelte`
- Create: `apps/web/src/lib/features/search/CommandPalette.svelte`
- Create: `apps/web/src/lib/features/settings/InheritedSetting.svelte`
- Test: `apps/web/src/lib/features/operations/operations-pages.test.ts`

**Interfaces:**
- Produces: all primary navigation destinations and Ctrl/Cmd+K metadata search/quick actions.
- Produces: Assets selected download, Exports history/creation/download/delete file, backup/restore/verify, diagnostics/integrity, settings inheritance, archive/trash.

- [ ] **Step 1: Write operations page tests**

Cover:

- Inbox lists uncollected Sources and action-required Drafts;
- Collections show override indicators;
- Runs filter status and open details;
- Assets default URL-only and explicit selection/download;
- Exports show `FILE_REMOVED` without download and no Regenerate button;
- backup automatic setting defaults off and type defaults Database Only;
- encryption password is never retained after submit;
- Restore requires verification and confirmation;
- settings show source Built-in/Global/Collection/Per-run;
- Trash supports Restore and permanent-delete impact confirmation;
- command palette searches metadata and excludes destructive commands.

- [ ] **Step 2: Implement consistent list patterns**

Use reusable pagination, filtering, empty/loading/error states, status text, and action menus. Avoid one-off fetch code by using typed API modules. URLs in UI are safely rendered as text and shortened visually without losing accessible full value.

- [ ] **Step 3: Implement Assets and Exports workflows**

Assets show preview where safe, MIME, known size, original URL, status, and batch Download Selected. Exports create Standard/With Provenance/Debug with format and optional Include Downloaded Assets. Display job progress and persistent history.

- [ ] **Step 4: Implement Backup and Recovery UI**

Create/verify/download/restore/delete backup. Show Database Only vs Full size estimates, encryption option, password no-recovery warning, and progress/cancel. In Recovery Mode, replace normal mutation nav with diagnostics/backup restore/migration retry actions.

- [ ] **Step 5: Implement inherited settings controls**

Each Collection setting offers Inherit Global, Custom, Reset to Built-in and displays active value/source. Global settings edit only ordinary settings. `.env` secrets/bootstrap fields show status and restart-required guidance but no editable secret value.

- [ ] **Step 6: Implement command palette**

Open with Ctrl/Cmd+K, search debounced metadata, keyboard navigate results, and include safe actions: Scrape URL, Create Collection, Open Inbox, View failed runs, Resume cancelled crawl, Create backup, Run integrity check, Open Settings. Destructive actions never appear.

- [ ] **Step 7: Run tests**

Run: `bun --cwd apps/web run test -- operations-pages`

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add apps/web
git commit -m "feat(web): add Erabi management and operations pages"
```

### Task 46: Add English-First i18n, Theme Modes, Browser Notifications, and Accessibility Baseline

**Files:**
- Create: `apps/web/src/lib/i18n/en.ts`
- Create: `apps/web/src/lib/i18n/index.ts`
- Create: `apps/web/src/lib/stores/preferences.svelte.ts`
- Create: `apps/web/src/lib/features/notifications/browser.ts`
- Create: `apps/web/src/lib/components/a11y/LiveRegion.svelte`
- Modify: `apps/web/src/app.css`
- Modify: all UI components created in Tasks 41–45
- Test: `apps/web/src/lib/i18n/i18n.test.ts`
- Test: `apps/web/src/lib/features/notifications/browser.test.ts`

**Interfaces:**
- Produces: translation keys for every UI string.
- Produces: Follow System default, Light, Dark.
- Produces: optional browser notifications off by default with explicit permission.
- Enforces: WCAG 2.2 AA baseline.

- [ ] **Step 1: Write translation completeness test**

Walk the English dictionary, ensure keys are unique/non-empty, and ensure status/error/progress codes have translations. Add a lint/test helper that rejects hard-coded user-visible strings in designated feature directories except test fixtures and data values.

- [ ] **Step 2: Implement a small typed i18n layer**

English is the only MVP dictionary, but components call `t("start.scrape")`. Support interpolation, plural count, and locale-aware date/number/byte formatting through `Intl`. Do not translate user data.

- [ ] **Step 3: Implement appearance preferences**

Follow system uses `prefers-color-scheme`. Light/Dark override immediately and persist through Settings API/database. Define CSS custom properties for surfaces, text, border, focus, success/warning/error, and highlighted extraction nodes. Status always includes icon/text, not color alone.

- [ ] **Step 4: Implement browser notification opt-in**

Default off. Request browser permission only after toggle. Notify crawl/export/backup/integrity completion/failure/partial for sufficiently long background jobs. Notification title/body never include URL, content, token, or values. Click focuses/opens related Erabi route. No in-app Notification Center.

- [ ] **Step 5: Apply accessibility requirements**

- keyboard reachability and logical focus order;
- visible high-contrast focus rings;
- skip link and semantic landmarks;
- accessible names/descriptions/errors;
- reduced-motion media query;
- restrained polite live regions;
- extraction DOM tree/manual selector alternative;
- usable at 200% zoom and narrow widths;
- dialogs trap/restore focus;
- tables/cards offer equivalent actions.

- [ ] **Step 6: Run tests and manual checklist**

Run:

```bash
bun --cwd apps/web run test -- i18n
bun --cwd apps/web run test -- browser
bun --cwd apps/web run check
```

Manually verify keyboard-only Start → crawl progress → Review and 200% zoom before commit.

- [ ] **Step 7: Commit**

```bash
git add apps/web
git commit -m "feat(web): add localization theme and accessibility"
```

### Task 47: Add Frontend Component Coverage and Automated Accessibility Checks

**Files:**
- Create: `apps/web/src/test-utils/render.ts`
- Create: `apps/web/src/test-utils/api.ts`
- Create: `apps/web/src/test-utils/a11y.ts`
- Create: `apps/web/src/lib/components/**/*.test.ts`
- Modify: `apps/web/package.json`
- Modify: `apps/web/vite.config.ts`

**Interfaces:**
- Produces: deterministic API/SSE mocks for component tests.
- Produces: automated axe checks on critical component states.
- Establishes: coverage thresholds for critical frontend logic.

- [ ] **Step 1: Add accessibility and coverage dependencies**

Run from `apps/web`:

```bash
bun add -d vitest-axe @vitest/coverage-v8
```

- [ ] **Step 2: Create typed test helpers**

Implement a mock API router that matches method/path and records requests, an SSE event builder with sequence, and `expectNoA11yViolations(container)` using `vitest-axe`. Ensure helpers never hide unexpected calls.

- [ ] **Step 3: Add critical-state component tests**

At minimum test accessibility and actions for:

- Start normal/Crawl4AI unavailable;
- progress running/partial/failed/cancelled;
- extraction editor pointer and keyboard states;
- Review valid/errors/diff/missing;
- Assets download states;
- Export create/completed/file removed;
- Backup create/encrypted/restore validation;
- Recovery Mode;
- access-token prompt;
- command palette.

- [ ] **Step 4: Configure practical coverage thresholds**

Set 80% statements/lines/functions and 70% branches for `src/lib/api`, `src/lib/features`, and `src/lib/stores`. Exclude generated SvelteKit files and simple route wrappers. Do not game coverage with meaningless assertions.

- [ ] **Step 5: Run complete frontend suite**

Run:

```bash
bun --cwd apps/web run test --coverage
bun --cwd apps/web run check
bun --cwd apps/web run build
```

Expected: PASS and thresholds met.

- [ ] **Step 6: Commit**

```bash
git add apps/web bun.lock
git commit -m "test(web): cover critical Erabi UI states"
```

## Phase 6: Packaging, End-to-End Verification, and Release

### Task 48: Package Erabi and Official Crawl4AI with Docker Compose

**Files:**
- Create: `docker/Dockerfile`
- Create: `docker/compose.yaml`
- Create: `docker/entrypoint.sh`
- Create: `scripts/resolve-crawl4ai-image.ts`
- Create: `docker/crawl4ai-image.env`
- Modify: `.env.example`
- Modify: `README.md`
- Test: `tests/smoke/docker-compose.sh`

**Interfaces:**
- Produces: primary MVP installation with `docker compose --env-file .env -f docker/compose.yaml up -d`.
- Produces: two services, `erabi` and unmodified official `crawl4ai`.
- Persists: database, artifacts, assets, exports, and backups under one mounted data root.

- [ ] **Step 1: Create a deterministic stable Crawl4AI image resolver**

Create `scripts/resolve-crawl4ai-image.ts` that:

1. fetches the latest non-draft, non-prerelease GitHub release for `unclecode/crawl4ai`;
2. removes a leading `v` from the tag;
3. rejects tags containing `alpha`, `beta`, `rc`, `pre`, or other hyphenated prerelease suffixes;
4. runs `docker buildx imagetools inspect unclecode/crawl4ai:<version>`;
5. extracts the manifest-list digest;
6. writes exactly `CRAWL4AI_IMAGE=unclecode/crawl4ai:<version>@sha256:<digest>` to `docker/crawl4ai-image.env`;
7. exits non-zero if no stable digest can be resolved.

Use native `fetch` and `Bun.spawn`; do not require npm, jq, or a custom Docker image.

- [ ] **Step 2: Run the resolver and review the pinned result**

Run:

```bash
bun scripts/resolve-crawl4ai-image.ts
cat docker/crawl4ai-image.env
```

Expected: one exact version-and-digest line, never `latest` and never a prerelease tag.

- [ ] **Step 3: Create the multi-stage Erabi Dockerfile**

Stages:

1. Bun stable image installs from `bun.lock` and builds `apps/web`;
2. Rust stable image builds `erabi` release binary from `Cargo.lock`;
3. minimal Debian runtime installs only CA certificates and runtime essentials;
4. copies `erabi`, web `build/`, migrations if not embedded, and entrypoint;
5. creates non-root `erabi` user;
6. exposes 7878 internally;
7. healthcheck calls `/api/v1/system/health` without exposing sensitive detail.

Use BuildKit cache mounts but ensure a clean build succeeds without cache.

- [ ] **Step 4: Create Compose configuration**

`docker/compose.yaml` must include:

```yaml
services:
  erabi:
    build:
      context: ..
      dockerfile: docker/Dockerfile
    env_file:
      - ../.env
    ports:
      - "127.0.0.1:${ERABI_PORT:-7878}:7878"
    volumes:
      - ../data:/data
    depends_on:
      crawl4ai:
        condition: service_healthy
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "/usr/local/bin/erabi", "doctor", "--healthcheck"]
      interval: 10s
      timeout: 3s
      retries: 12

  crawl4ai:
    image: ${CRAWL4AI_IMAGE}
    shm_size: 1gb
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-fsS", "http://127.0.0.1:11235/health"]
      interval: 10s
      timeout: 5s
      retries: 18
```

Merge `docker/crawl4ai-image.env` when invoking Compose. Do not mount modified Crawl4AI source or patch its container.

- [ ] **Step 5: Implement graceful dependency behavior**

Although Compose waits for initial health, Erabi must still start and remain usable when Crawl4AI later becomes unavailable. The UI shows crawler unavailable and disables Scrape; old data/review/export/backup remain usable.

- [ ] **Step 6: Write and run Docker smoke test**

`tests/smoke/docker-compose.sh` must build, start, wait for health, assert Start page and health endpoint, stop Crawl4AI and assert Erabi remains up with crawler unavailable, then cleanly `down` without deleting data volume.

Run:

```bash
bash tests/smoke/docker-compose.sh
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add docker scripts/resolve-crawl4ai-image.ts .env.example README.md tests/smoke/docker-compose.sh
git commit -m "feat(deploy): package Erabi with pinned Crawl4AI"
```

### Task 49: Add Playwright End-to-End Tests with a Deterministic Mock Crawl4AI Server

**Files:**
- Create: `playwright.config.ts`
- Create: `tests/fixtures/websites/server.ts`
- Create: `tests/fixtures/crawl4ai/server.ts`
- Create: `tests/e2e/helpers.ts`
- Create: `tests/e2e/start-review-export.spec.ts`
- Create: `tests/e2e/cancel-resume.spec.ts`
- Create: `tests/e2e/schema-drift.spec.ts`
- Create: `tests/e2e/backup-recovery.spec.ts`
- Create: `tests/e2e/accessibility.spec.ts`
- Modify: `package.json`
- Modify: `bun.lock`

**Interfaces:**
- Produces: PR-safe end-to-end suite independent of public websites and real Crawl4AI.
- Exercises: complete main user journeys through browser, API, Turso, jobs, and filesystem.

- [ ] **Step 1: Add Playwright using Bun**

Run:

```bash
bun add -d @playwright/test axe-core
bunx playwright install chromium
```

Use Chromium for PR E2E to control runtime. Browser matrix expansion is optional after MVP.

- [ ] **Step 2: Configure isolated test data per worker**

`playwright.config.ts` starts:

- local deterministic website fixture server;
- mock Crawl4AI HTTP server;
- Erabi process with temporary `ERABI_DATA_DIR`, localhost bind, no auth, and mock crawler URL;
- web UI served by Axum.

Use one E2E worker initially because the MVP uses one local data-directory lock. Each test resets database/artifacts through a test-only process restart, never through a production reset endpoint.

- [ ] **Step 3: Implement the golden journey test**

`start-review-export.spec.ts` must:

1. open Start;
2. paste fixture URL;
3. observe live progress;
4. auto-enter Records Review;
5. verify visual source highlight and provenance;
6. edit one Draft field and wait for Saved;
7. approve all valid while one invalid remains Draft;
8. export approved CSV with provenance;
9. download ZIP;
10. inspect ZIP names through Node/Bun helper;
11. verify manifest counts and sidecar.

- [ ] **Step 4: Implement recovery and safety journeys**

- Cancel/resume from checkpoint without duplicate records.
- Partial pagination cannot produce missing candidates.
- Complete recrawl creates new/updated/missing candidates and no-change run creates no Review.
- Schema drift blocks normal application and offers review/use/cancel.
- Backup encrypted roundtrip and wrong password leaves current data unchanged.
- Recovery Mode exposes diagnostics/restore but blocks mutation.

- [ ] **Step 5: Implement accessibility E2E**

Run axe on Start, progress, extraction editor, Review, Assets, Exports, Settings, and Recovery Mode. Add keyboard-only journey through URL submit, log expand, DOM tree field selection, Grid cell edit, approval, command palette, and close Review.

- [ ] **Step 6: Run E2E**

Run: `bunx playwright test`

Expected: all tests PASS without external network access.

- [ ] **Step 7: Commit**

```bash
git add playwright.config.ts tests/e2e tests/fixtures package.json bun.lock
git commit -m "test(e2e): verify complete Erabi workflows"
```

### Task 50: Add Scheduled Smoke Tests Against the Real Official Crawl4AI Container

**Files:**
- Create: `tests/fixtures/websites/static/article.html`
- Create: `tests/fixtures/websites/static/products.html`
- Create: `tests/fixtures/websites/static/pagination-1.html`
- Create: `tests/fixtures/websites/static/pagination-2.html`
- Create: `tests/fixtures/websites/static/lazy.html`
- Create: `tests/smoke/real-crawl4ai.spec.ts`
- Create: `docker/compose.smoke.yaml`

**Interfaces:**
- Produces: scheduled compatibility checks for the pinned Crawl4AI image.
- Covers: static HTML, JavaScript rendering, pagination, lazy loading, screenshot, adapter mapping.

- [ ] **Step 1: Build local-only fixture website scenarios**

The server must expose deterministic pages with known content and no internet dependency. `lazy.html` uses local JavaScript to append content on scroll. Pagination uses rel=next and numbered links. Include an image asset for screenshot/asset detection.

- [ ] **Step 2: Create real-service smoke Compose overlay**

Start Erabi, the pinned official Crawl4AI image, and the fixture server on an isolated Docker network. The fixture server is reachable by Crawl4AI; it is not public.

- [ ] **Step 3: Write the smoke specification**

Assert:

- health and version captured;
- static article returns raw/rendered/Markdown;
- JS/lazy content appears after configured wait/scroll;
- screenshot artifact is valid PNG;
- pagination candidate is detected and confirmed;
- Records extraction returns expected count/values;
- access token/JWT needed by the selected Crawl4AI version is correctly configured by environment, never hard-coded.

- [ ] **Step 4: Run the real smoke test locally once**

Run:

```bash
docker compose --env-file docker/crawl4ai-image.env -f docker/compose.yaml -f docker/compose.smoke.yaml up -d --build
bunx playwright test tests/smoke/real-crawl4ai.spec.ts
docker compose --env-file docker/crawl4ai-image.env -f docker/compose.yaml -f docker/compose.smoke.yaml down
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/websites/static tests/smoke/real-crawl4ai.spec.ts docker/compose.smoke.yaml
git commit -m "test(smoke): verify real Crawl4AI compatibility"
```

### Task 51: Add CI, Dependency/Security Checks, Licensing, and SemVer Release Metadata

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/crawl4ai-smoke.yml`
- Create: `.github/dependabot.yml`
- Create: `deny.toml`
- Create: `LICENSE`
- Create: `SECURITY.md`
- Create: `CONTRIBUTING.md`
- Create: `docs/operations/release.md`
- Create: `crates/erabi-cli/src/version.rs`
- Modify: `README.md`

**Interfaces:**
- Produces: frozen-lockfile CI, real smoke schedule, audit/license checks, version diagnostics.
- Establishes: Apache-2.0 and Semantic Versioning.
- Does not establish DCO/CLA in MVP.

- [ ] **Step 1: Add Rust security/license tools to CI installation**

CI installs stable `cargo-audit` and `cargo-deny`. Do not make them runtime dependencies. Configure `deny.toml` to allow Apache-2.0, MIT, BSD-2/3-Clause, ISC, Unicode, Zlib, and other specifically reviewed permissive licenses; deny unlicensed, copyleft-incompatible, yanked, and duplicate-risk crates according to documented exceptions.

- [ ] **Step 2: Implement pull-request CI**

Run jobs for:

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo build --workspace --release
cargo audit
cargo deny check
bun install --frozen-lockfile
bun --cwd apps/web run check
bun --cwd apps/web run test --coverage
bun --cwd apps/web run build
bunx playwright install --with-deps chromium
bunx playwright test
Docker image build
```

Cache registries/build outputs without bypassing lockfile verification.

- [ ] **Step 3: Implement scheduled real Crawl4AI smoke workflow**

Run nightly or weekly, manually triggerable, using the committed pinned image. Upload logs/traces on failure, but run diagnostic redaction before artifact upload. Do not expose `.env` contents or scraped values.

- [ ] **Step 4: Add Apache-2.0 and simple contributor/security documents**

Use the canonical Apache License 2.0 text. `CONTRIBUTING.md` explains Cargo/Bun commands, stable-dependency rule, TDD, specs/plans, and no DCO/CLA yet. `SECURITY.md` gives private reporting instructions without promising unsupported response times.

- [ ] **Step 5: Implement version reporting**

Expose application SemVer, API version `v1`, database schema version, backup format version, export manifest version, Rust version at build, Turso crate version when obtainable, Crawl4AI health/version, and OS. Build metadata must not change API compatibility behavior.

- [ ] **Step 6: Document release rules**

- `0.1.0` MVP;
- minor releases may contain documented breaking changes during `0.x` with migration/compatibility notes;
- patch releases are compatible fixes;
- after 1.0, deprecate before removal and remove only in a major release;
- no automatic app updates;
- Docker users update with pull/up;
- images are tagged versions, never deployment `latest`.

- [ ] **Step 7: Run the CI command set locally**

Run every command from Step 2. Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add .github deny.toml LICENSE SECURITY.md CONTRIBUTING.md docs/operations/release.md crates/erabi-cli README.md
git commit -m "ci: secure and version Erabi releases"
```

### Task 52: Run the Full MVP Acceptance Gate and Finalize Operator/User Documentation

**Files:**
- Create: `docs/operations/install.md`
- Create: `docs/operations/configuration.md`
- Create: `docs/operations/backup-restore.md`
- Create: `docs/operations/recovery-mode.md`
- Create: `docs/operations/security.md`
- Create: `docs/api/overview.md`
- Create: `docs/mvp-acceptance-report.md`
- Modify: `README.md`
- Modify: `.env.example`

**Interfaces:**
- Produces: a verified release candidate and reproducible acceptance report.
- Confirms: every frozen MVP requirement has implementation and evidence.

- [ ] **Step 1: Create the acceptance matrix before the final run**

`docs/mvp-acceptance-report.md` must list every requirement from all approved specs with:

- requirement ID;
- implementation task/file;
- automated test name;
- manual verification where necessary;
- result and evidence command.

No requirement may be marked complete without a specific test or inspection.

- [ ] **Step 2: Verify fresh installation**

On a clean data directory:

```bash
cp .env.example .env
docker compose --env-file .env --env-file docker/crawl4ai-image.env -f docker/compose.yaml up -d --build
```

Verify Local Turso/database creation, migrations, artifact directories, Crawl4AI health, Start page, no wizard, and local-only port binding.

- [ ] **Step 3: Execute the complete product acceptance journey**

Verify a fresh user can:

1. paste a public/local fixture URL;
2. see robots/rate-limit/progress behavior;
3. cancel/resume safely;
4. detect Document/Records mode and switch without recrawl;
5. select container/fields visually and by keyboard;
6. inspect field provenance;
7. edit Draft/autosave;
8. approve valid while invalid remains Draft;
9. reject single/bulk with reason rules;
10. close unresolved Review with special status;
11. export each format and provenance ZIP;
12. download selected assets safely;
13. recrawl and see only meaningful changes;
14. get no Review for no-change;
15. create/verify/restore encrypted backup;
16. survive Crawl4AI outage;
17. enter and use Recovery Mode after a controlled integrity failure;
18. receive disk-pressure safety stop without deletion;
19. use metadata search/command palette;
20. operate keyboard-only at 200% zoom.

- [ ] **Step 4: Verify destructive and security boundaries**

Test non-loopback startup without token fails, wrong token rate limits, CORS/Origin/Host/media-type rejection, CSP, preview script isolation, path traversal downloads, permanent deletion impact confirmation, Trash restore, no secret in DB/log/diagnostic/backup manifest, and OpenAPI disabled by default on network.

- [ ] **Step 5: Verify shutdown and restart recovery**

Start active crawl/export/backup jobs, send termination, measure process exit at no more than three seconds, restart, and verify jobs are recoverable/consistent with no approved data corruption or duplicate records.

- [ ] **Step 6: Write complete operator documentation**

Document exact installation, `.env` fields, local/network exposure, Crawl4AI connection/token, data directories, update procedure, settings inheritance, backup types/encryption/password warning, restore, Recovery Mode, diagnostics, retention, OpenAPI, and security limitations. Clearly separate MVP from roadmap.

- [ ] **Step 7: Run final verification commands**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo build --workspace --release
cargo audit
cargo deny check
bun install --frozen-lockfile
bun --cwd apps/web run check
bun --cwd apps/web run test --coverage
bun --cwd apps/web run build
bunx playwright test
bash tests/smoke/docker-compose.sh
```

Then run the real Crawl4AI smoke command from Task 50.

Expected: every command PASS; acceptance report has no failed or unverified frozen-MVP requirement.

- [ ] **Step 8: Commit the release-ready documentation and evidence**

```bash
git add README.md .env.example docs/operations docs/api docs/mvp-acceptance-report.md
git commit -m "docs: finalize Erabi MVP operations and acceptance"
```

---

## Specification Coverage Matrix

| Approved specification | Primary implementation tasks |
|---|---|
| Product scope, Start, navigation, recent activity, naming | 3, 6, 21, 41, 45 |
| English-first, theme, accessibility, notifications | 41–47, 49, 52 |
| Rust modular monolith and dependency policy | 1–3, 14–20, 48, 51 |
| Official Turso application persistence and migrations | 10–11, 15, 39–40 |
| API v1, error envelope, idempotency, optimistic concurrency | 14, 18, 21, 28, 32 |
| Durable queue, leases, panic isolation, cancellation, SSE | 16–18, 23–24, 42 |
| Crawl4AI boundary and real compatibility | 19–20, 48–50 |
| Safe crawling, pagination, partial/retry/resume | 22–24, 49–50 |
| Raw artifacts, retention, storage pressure | 12, 25, 38, 40 |
| Visual extraction, mode detection, Schema Versions/drift | 26–29, 43 |
| Validation, immutable approval, diff, Review lifecycle | 31–32, 44 |
| Field-level provenance | 30, 35, 44 |
| Assets and downloaded-file safety | 12, 33, 35, 45 |
| File/SQLite/Turso exports and atomic destination publish | 34–37, 45 |
| Archive, Trash, permanent deletion, export history | 38, 45 |
| Backup, encryption, restore, Recovery Mode | 15, 39–40, 45, 49 |
| Security, CORS, CSP, OpenAPI, redacted tracing | 8–9, 13–15, 40, 48, 51–52 |
| Docker Compose, manual updates, SemVer | 48, 51–52 |
| Roadmap exclusions | Enforced throughout; no task implements deferred features |

## Deliberately Deferred Items

Do not add these while executing this plan:

- Source movement between Collections;
- Schema JSON import/export;
- custom export filename;
- Regenerate Export;
- Undo/Redo and persistent Draft history;
- in-app Notification Center;
- CSV/JSON/JSONL file ingestion, sitemap, RSS/Atom;
- full-text record search;
- schedules and automatic crawl;
- authenticated browser/session/action workflows;
- file parsing/OCR;
- Append/Upsert database exports;
- PostgreSQL/MySQL/S3/R2/vector/RAG connectors;
- optional AI assistance;
- generated frontends or assistants;
- Tauri desktop installer;
- accounts, teams, hosted SaaS;
- multi-instance/distributed workers;
- DCO/CLA governance;
- automatic software updates.

## Final Implementation Discipline

- Use a clean worktree before Task 1 when implementation begins.
- Run the targeted test before and after each implementation change.
- Do not combine multiple task commits unless a reviewer explicitly requests it.
- Keep every route handler thin and every SQL statement in `erabi-db`.
- Keep Crawl4AI DTOs private to `erabi-crawl4ai`.
- Treat every crawled byte and downloaded file as untrusted.
- Never weaken an invariant merely to make an E2E test pass.
- When the exact current stable upstream API differs from a code snippet in this plan, preserve the contract and behavior, use `cargo add`/`bun add` for the latest stable release, consult official documentation, and update the focused adapter implementation plus tests in the same commit.
