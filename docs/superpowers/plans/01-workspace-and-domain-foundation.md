# Erabi Workspace and Domain Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the minimal Cargo/Bun monorepo, scaffold focused Rust and SvelteKit packages, and establish stable identifiers, lifecycle types, Collections, Sources, and automatic naming.

**Architecture:** This plan creates the repository skeleton and the dependency-free domain base that every later subsystem consumes. Domain types remain infrastructure-agnostic, while the frontend is only scaffolded enough to prove Bun and SvelteKit workspace integration.

**Tech Stack:** Stable Rust, Cargo workspace, UUIDv7, Serde, SvelteKit, Svelte, TypeScript, Bun.

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

- **Depends on:** Approved Erabi design specifications only.
- **Produces:** Compiling Cargo and Bun workspaces; domain IDs, errors, lifecycle enums, Collection/Source models, and auto-naming contracts.
- **Gate:** Foundation A: root verification scripts, Rust workspace tests, frontend checks, and domain unit tests all pass.
- **Execution order:** Complete every task in this file in numerical order and commit after each task. Do not begin the next plan until this gate passes.

## Focused File Map

```text
Cargo.toml
rust-toolchain.toml
package.json
bunfig.toml
clippy.toml
.env.example
scripts/verify-workspace.ts
crates/erabi-domain/
crates/*/src/lib.rs
apps/web/
README.md
```

---

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
