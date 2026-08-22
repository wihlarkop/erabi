# Erabi Domain and Workspace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bootstrap the Cargo/Bun monorepo and implement the dependency-light Crawler Studio domain contracts that every later subsystem consumes.

**Architecture:** Pure domain types live in `erabi-domain`; infrastructure crates are scaffolded but do not leak Turso, Axum, or Crawl4AI details into domain code. `Crawler`/`CrawlerVersion`/`PageType` are the reusable design center; `Source` is supporting durable target/history identity.

**Tech Stack:** stable Rust, Cargo workspace, Rust 2024 edition, Serde, `uuid` UUIDv7, `url`, Bun, SvelteKit, TypeScript.

**Spec:** `docs/specs/01-product-and-experience.md`, `docs/specs/02-crawler-studio-domain.md`, `docs/specs/03-discovery-graph-and-runs.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Global Constraints

- Use current compatible stable dependency releases at implementation time; no alpha/beta/RC/Git dependencies without a later explicit spec change.
- Add Rust dependencies with `cargo add`; add frontend dependencies with `bun add` / `bun add -d`.
- Commit `Cargo.lock` and `bun.lock`; do not add npm/pnpm/yarn lockfiles.
- Use application-generated UUIDv7 for every major entity.
- Published Crawler Versions are immutable.
- MVP has exactly four run types: `QUICK_SCRAPE`, `TEST_RUN`, `DISCOVERY_PREVIEW`, `PRODUCTION_RUN`.
- Extraction configuration belongs to Page Types inside Crawler Versions; do not create an independent global Schema lifecycle.
- Source never mutates or replaces Crawler Seeds implicitly.
- Page Type ties remain `AMBIGUOUS_PAGE_TYPE`; never use creation/row/UUID order as a hidden tie-breaker.

## Focused File Map

```text
Cargo.toml
Cargo.lock
rust-toolchain.toml
package.json
bun.lock
bunfig.toml
.env.example
scripts/verify-workspace.ts
crates/erabi-domain/
crates/erabi-db/
crates/erabi-api/
crates/erabi-jobs/
crates/erabi-crawler/
crates/erabi-crawl4ai/
crates/erabi-extraction/
crates/erabi-export/
crates/erabi-cli/
apps/web/
```

---

### Task 1: Bootstrap Cargo and Bun workspaces

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `package.json`
- Create: `bunfig.toml`
- Create: `.env.example`
- Create: `scripts/verify-workspace.ts`
- Create: `crates/erabi-domain/Cargo.toml`
- Create: `crates/erabi-domain/src/lib.rs`
- Create: the eight remaining crate manifests/source entry points listed in the file map
- Create: `apps/web/package.json`
- Create: `apps/web/svelte.config.js`
- Create: `apps/web/vite.config.ts`
- Create: `apps/web/tsconfig.json`
- Create: `apps/web/src/routes/+page.svelte`

**Interfaces:**
- Produces Cargo workspace members `crates/*`.
- Produces Bun workspace member `apps/*`.
- Produces root scripts `check`, `test`, `build`, `dev`.

- [ ] **Step 1: Write the workspace verification script before the workspace files exist**

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
if (!Array.isArray(pkg.workspaces) || !pkg.workspaces.includes("apps/*")) {
  throw new Error("apps/* Bun workspace is required");
}
if (typeof pkg.packageManager !== "string" || !pkg.packageManager.startsWith("bun@")) {
  throw new Error("packageManager must record the executing Bun version");
}
console.log("workspace contract ok");
```

- [ ] **Step 2: Run the verifier and confirm RED**

Run:

```bash
bun scripts/verify-workspace.ts
```

Expected: non-zero exit with `missing Cargo.toml`.

- [ ] **Step 3: Create the minimal Cargo workspace and nine architectural crates**

Create root `Cargo.toml`:

```toml
[workspace]
members = ["crates/*"]
resolver = "3"

[workspace.package]
version = "0.1.0"
edition = "2024"
license = "Apache-2.0"

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
all = "warn"
pedantic = "warn"
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

Create exactly these packages with Cargo commands so manifests are valid for the current toolchain:

```bash
cargo new --lib crates/erabi-domain
cargo new --lib crates/erabi-db
cargo new --lib crates/erabi-api
cargo new --lib crates/erabi-jobs
cargo new --lib crates/erabi-crawler
cargo new --lib crates/erabi-crawl4ai
cargo new --lib crates/erabi-extraction
cargo new --lib crates/erabi-export
cargo new --bin crates/erabi-cli --name erabi
```

For each crate, replace generated placeholder logic with one crate-level responsibility doc and add:

```toml
[lints]
workspace = true
```

- [ ] **Step 4: Create Bun/SvelteKit workspace using only Bun**

Create root `package.json` initially as:

```json
{
  "name": "erabi",
  "private": true,
  "version": "0.1.0",
  "workspaces": ["apps/*"],
  "scripts": {
    "dev": "bun --cwd apps/web run dev",
    "check": "bun scripts/verify-workspace.ts && bun --cwd apps/web run check",
    "test": "cargo test --workspace && bun --cwd apps/web run test",
    "build": "cargo build --workspace && bun --cwd apps/web run build"
  }
}
```

Record the actual stable Bun version executing the bootstrap:

```bash
bun -e 'const p=await Bun.file("package.json").json(); p.packageManager=`bun@${Bun.version}`; await Bun.write("package.json", JSON.stringify(p,null,2)+"\n")'
```

Create `bunfig.toml`:

```toml
[install]
frozenLockfile = false
```

Bootstrap `apps/web` with current stable SvelteKit packages using `bun add` / `bun add -d`. Configure a static-compatible SPA and create a minimal `/` page containing only an `Erabi` heading. Do not add product feature UI in this task.

- [ ] **Step 5: Add bootstrap environment documentation**

Create `.env.example` with non-secret names only:

```dotenv
ERABI_HOST=127.0.0.1
ERABI_PORT=7878
ERABI_DATA_DIR=./data
ERABI_ACCESS_TOKEN=
ERABI_CORS_ALLOWED_ORIGINS=
ERABI_OPENAPI_ENABLED=true
CRAWL4AI_BASE_URL=http://crawl4ai:11235
CRAWL4AI_API_TOKEN=
TURSO_DATABASE_URL=
TURSO_AUTH_TOKEN=
```

- [ ] **Step 6: Run GREEN workspace verification**

Run:

```bash
bun scripts/verify-workspace.ts
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
bun --cwd apps/web run check
bun --cwd apps/web run test
```

Expected: all commands exit 0 and the verifier prints `workspace contract ok`.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock rust-toolchain.toml package.json bun.lock bunfig.toml .env.example scripts crates apps
 git commit -m "chore: initialize Erabi workspace"
```

---

### Task 2: Implement identifiers, run types, lifecycle states, and stable domain errors

**Files:**
- Create: `crates/erabi-domain/src/id.rs`
- Create: `crates/erabi-domain/src/status.rs`
- Create: `crates/erabi-domain/src/error.rs`
- Modify: `crates/erabi-domain/src/lib.rs`
- Test: `crates/erabi-domain/tests/domain_primitives.rs`

**Interfaces:**
- Produces `EntityId`.
- Produces `CrawlRunType`, `CrawlRunStatus`, `SourceTargetType`, `SourceStatus`.
- Produces `ErrorCode`, `ProductError`, `SuggestedAction`.

- [ ] **Step 1: Add stable domain dependencies**

Run:

```bash
cargo add -p erabi-domain serde --features derive
cargo add -p erabi-domain uuid --features v7,serde
cargo add -p erabi-domain thiserror
```

- [ ] **Step 2: Write failing primitive contract tests**

Create `crates/erabi-domain/tests/domain_primitives.rs`:

```rust
use erabi_domain::{CrawlRunStatus, CrawlRunType, EntityId, ErrorCode};

#[test]
fn entity_ids_are_uuid_v7() {
    let id = EntityId::new();
    assert_eq!(id.as_uuid().get_version_num(), 7);
    assert_eq!(id.to_string().parse::<uuid::Uuid>().unwrap(), *id.as_uuid());
}

#[test]
fn run_types_are_exactly_the_four_mvp_types() {
    let json = serde_json::to_string(&[
        CrawlRunType::QuickScrape,
        CrawlRunType::TestRun,
        CrawlRunType::DiscoveryPreview,
        CrawlRunType::ProductionRun,
    ]).unwrap();
    assert_eq!(json, r#"["QUICK_SCRAPE","TEST_RUN","DISCOVERY_PREVIEW","PRODUCTION_RUN"]"#);
}

#[test]
fn lifecycle_and_error_codes_are_stable() {
    assert_eq!(serde_json::to_string(&CrawlRunStatus::PartialResult).unwrap(), r#""PARTIAL_RESULT""#);
    assert_eq!(serde_json::to_string(&ErrorCode::AmbiguousPageType).unwrap(), r#""AMBIGUOUS_PAGE_TYPE""#);
}
```

Add `serde_json` as a dev dependency if needed with `cargo add -p erabi-domain --dev serde_json`.

- [ ] **Step 3: Run the test and confirm RED**

```bash
cargo test -p erabi-domain --test domain_primitives
```

Expected: compile failure because the exported types do not exist.

- [ ] **Step 4: Implement the primitives**

Create `id.rs`:

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct EntityId(uuid::Uuid);

impl EntityId {
    #[must_use]
    pub fn new() -> Self { Self(uuid::Uuid::now_v7()) }
    #[must_use]
    pub const fn as_uuid(&self) -> &uuid::Uuid { &self.0 }
}

impl Default for EntityId { fn default() -> Self { Self::new() } }
impl std::fmt::Display for EntityId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.fmt(f) }
}
```

Create `status.rs` with Serde `SCREAMING_SNAKE_CASE` enums:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrawlRunType { QuickScrape, TestRun, DiscoveryPreview, ProductionRun }

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrawlRunStatus { Queued, Running, Succeeded, PartialResult, Failed, Cancelled }

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceTargetType { WebPage, FileAsset }

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceStatus { Active, Archived, Trashed }
```

Create `error.rs` with at minimum these serialized codes: `SCHEMA_DRIFT`, `AMBIGUOUS_PAGE_TYPE`, `UNRESOLVED_REFERENCE`, `STORAGE_CRITICAL`, `CRAWLER_UNAVAILABLE`, `VALIDATION_ERROR`, `CONFLICT`, `NOT_FOUND`, `ACCESS_DENIED`, `CRAWLER_TIMEOUT`. `ProductError` carries code, safe message, details JSON, recoverable flag, suggested actions, and trace ID string.

- [ ] **Step 5: Run GREEN primitive tests**

```bash
cargo test -p erabi-domain --test domain_primitives
cargo clippy -p erabi-domain --all-targets -- -D warnings
```

Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add Cargo.lock crates/erabi-domain
 git commit -m "feat(domain): add stable identifiers and lifecycle primitives"
```

---

### Task 3: Implement Crawler, Crawler Version, Seed, Run Profile, and Test Evidence contracts

**Files:**
- Create: `crates/erabi-domain/src/crawler.rs`
- Create: `crates/erabi-domain/src/crawler_version.rs`
- Create: `crates/erabi-domain/src/seed.rs`
- Create: `crates/erabi-domain/src/run_profile.rs`
- Create: `crates/erabi-domain/src/test_evidence.rs`
- Modify: `crates/erabi-domain/src/lib.rs`
- Test: `crates/erabi-domain/tests/crawler_version_contract.rs`

**Interfaces:**
- Produces `Crawler`, `CrawlerVersion`, `CrawlerVersionState`, `Seed`, `RunProfile`, `TestEvidence`.
- Produces `OperationalOverrides`; semantic configuration is not represented in this type.

- [ ] **Step 1: Write failing lifecycle tests**

Create tests proving Published immutability and operational-only Run Profiles:

```rust
use erabi_domain::{CrawlerVersion, CrawlerVersionState, OperationalOverrides, RunProfile};

#[test]
fn published_version_cannot_be_mutated() {
    let published = CrawlerVersion::fixture_published();
    assert!(published.rename_page_type("renamed").is_err());
}

#[test]
fn run_profile_contains_only_operational_overrides() {
    let profile = RunProfile::new("Quick Test", OperationalOverrides {
        max_pages: Some(10),
        max_depth: Some(1),
        ..OperationalOverrides::default()
    });
    assert_eq!(profile.name(), "Quick Test");
    assert_eq!(profile.overrides().max_pages, Some(10));
}

#[test]
fn published_state_serializes_explicitly() {
    assert_eq!(serde_json::to_string(&CrawlerVersionState::Published).unwrap(), r#""PUBLISHED""#);
}
```

Use a `test_support` module behind `#[cfg(any(test, feature = "test-support"))]` only if fixtures cannot be built through public constructors; do not put production invariants behind test-only behavior.

- [ ] **Step 2: Run RED**

```bash
cargo test -p erabi-domain --test crawler_version_contract
```

Expected: compile failure for missing contracts.

- [ ] **Step 3: Implement exact domain boundaries**

Use these shapes as the public boundary:

```rust
pub struct Crawler {
    pub id: EntityId,
    pub name: String,
    pub collection_id: Option<EntityId>,
    pub active_published_version_id: Option<EntityId>,
    pub active_draft_version_id: Option<EntityId>,
}

pub enum CrawlerVersionState { Draft, Published }

pub struct Seed {
    pub id: EntityId,
    pub original_url: url::Url,
    pub canonical_url: url::Url,
    pub enabled: bool,
    pub label: Option<String>,
    pub entry_page_type_hint: Option<EntityId>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct OperationalOverrides {
    pub max_pages: Option<u64>,
    pub max_depth: Option<u32>,
    pub max_duration_seconds: Option<u64>,
    pub concurrency: Option<u32>,
    pub request_delay_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub screenshot: Option<bool>,
    pub asset_download_limit_bytes: Option<u64>,
}
```

`CrawlerVersion` owns semantic configuration IDs/collections for Seeds, Page Types, transitions, canonicalization policy, domain scope, and Crawler operational defaults. Its mutating methods return a typed conflict when state is Published. A crawler may point to at most one ordinary active Draft; repository enforcement is implemented in Plan 02.

`TestEvidence` stores version ID, test type, input URLs, evaluated Page Type, match/extraction/discovery summaries, warnings/errors, artifact IDs, config hash, execution timestamp.

Add `url` with Serde support using `cargo add -p erabi-domain url --features serde`.

- [ ] **Step 4: Run GREEN**

```bash
cargo test -p erabi-domain --test crawler_version_contract
cargo test -p erabi-domain
```

Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock crates/erabi-domain
 git commit -m "feat(domain): model crawlers and immutable versions"
```

---

### Task 4: Implement Page Types and deterministic URL matching

**Files:**
- Create: `crates/erabi-domain/src/page_type.rs`
- Create: `crates/erabi-domain/src/url_matcher.rs`
- Create: `crates/erabi-domain/src/matching.rs`
- Modify: `crates/erabi-domain/src/lib.rs`
- Test: `crates/erabi-domain/tests/page_type_matching.rs`

**Interfaces:**
- Produces `PageType`, `UrlMatcher`, `UrlMatcherKind`, `SpecificityKey`, `PageTypeMatchDecision`, `PageTypeCandidate`.
- Produces pure `resolve_page_type(url, page_types)` with no database/order dependency.

- [ ] **Step 1: Write the ordering-independence and ambiguity tests**

```rust
use erabi_domain::{resolve_page_type, PageTypeMatchDecision};

#[test]
fn reversing_page_type_order_does_not_change_the_winner() {
    let (url, a, b) = erabi_domain::test_support::specificity_fixture();
    let forward = resolve_page_type(&url, &[a.clone(), b.clone()]);
    let reverse = resolve_page_type(&url, &[b, a]);
    assert_eq!(forward, reverse);
}

#[test]
fn complete_resolution_key_tie_is_ambiguous() {
    let (url, a, b) = erabi_domain::test_support::exact_tie_fixture();
    let decision = resolve_page_type(&url, &[a, b]);
    assert!(matches!(decision, PageTypeMatchDecision::Ambiguous { .. }));
}
```

Also add matcher-kind tests for exact URL > exact host/path/template > path prefix/glob > regex.

- [ ] **Step 2: Run RED**

```bash
cargo test -p erabi-domain --test page_type_matching
```

Expected: compile failure for missing matching service.

- [ ] **Step 3: Implement validated matcher definitions and explicit specificity**

Use an explicit sortable key that does not depend on IDs:

```rust
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SpecificityKey {
    pub matcher_kind_rank: u8,
    pub literal_path_segments: u32,
    pub explicit_query_constraints: u32,
    pub literal_characters: u32,
    pub inverse_wildcards: std::cmp::Reverse<u32>,
}
```

If `Reverse<u32>` makes serialized explanations awkward, store `wildcard_count: u32` and compare it in reverse explicitly; the externally shown rationale must retain the real wildcard count. Matcher definitions validate syntax on construction. Compute the key reproducibly from matcher definition only.

Resolution algorithm:

```text
collect matching Page Types
→ choose highest Page Type priority
→ within tied priority choose greatest best-matcher SpecificityKey
→ if one Page Type remains: Matched
→ if multiple share the complete key: Ambiguous
→ if none: Unmatched
```

`PageTypeCandidate` records Page Type ID/name, explicit priority, matcher kind, specificity components, and matched pattern for Test Lab/Discovery Preview explanations.

- [ ] **Step 4: Run GREEN and property-style reorder coverage**

```bash
cargo test -p erabi-domain --test page_type_matching
cargo test -p erabi-domain
```

Add at least one loop/permutation test over several input orderings; do not rely on one forward/reverse case only.

- [ ] **Step 5: Commit**

```bash
git add crates/erabi-domain
 git commit -m "feat(domain): resolve page types deterministically"
```

---

### Task 5: Implement transitions, Source, Collection, and deterministic naming

**Files:**
- Create: `crates/erabi-domain/src/transition.rs`
- Create: `crates/erabi-domain/src/source.rs`
- Create: `crates/erabi-domain/src/collection.rs`
- Create: `crates/erabi-domain/src/naming.rs`
- Modify: `crates/erabi-domain/src/lib.rs`
- Test: `crates/erabi-domain/tests/source_transition_contract.rs`

**Interfaces:**
- Produces `DiscoveryTransition`, `TransitionBudget`, `Collection`, `Source`.
- Produces `derive_source_name()` and `derive_dataset_name()` pure functions.

- [ ] **Step 1: Write failing transition and Source-boundary tests**

```rust
use erabi_domain::{DiscoveryTransition, Source};

#[test]
fn self_transition_is_valid_for_pagination_when_budgeted() {
    let transition = DiscoveryTransition::test_self_cycle(25, Some(100));
    assert!(transition.validate().is_ok());
}

#[test]
fn source_metadata_has_no_seed_mutation_api() {
    let source = Source::test_web("https://example.test/product/1");
    assert_eq!(source.canonical_url().as_str(), "https://example.test/product/1");
}
```

Add compile-time/API review in this task: `Source` must not expose `seed_id`, `crawler_version_id`, or methods that mutate Seeds. Run associations may be stored as references by persistence later without making Source crawler configuration.

- [ ] **Step 2: Run RED**

```bash
cargo test -p erabi-domain --test source_transition_contract
```

Expected: compile failure for missing types.

- [ ] **Step 3: Implement the contracts**

`DiscoveryTransition` contains ID, source/target Page Type IDs, name, enabled, link selector/rule, optional URL constraints, priority, maximum links per source page, optional total budget, depth contribution, deduplication behavior, and optional latest Test Evidence summary/reference.

`Source` contains:

```rust
pub struct Source {
    pub id: EntityId,
    pub collection_id: Option<EntityId>,
    pub name: String,
    pub original_url: url::Url,
    pub canonical_url: url::Url,
    pub target_type: SourceTargetType,
    pub status: SourceStatus,
}
```

`Collection` is lightweight organizational metadata plus references to shared ordinary defaults; semantic Crawler Version data does not live in Collection.

Naming functions are deterministic/local only. Source fallback order: page title → OG title → domain + meaningful path → domain → `Untitled Source`. Do not fetch network data from naming functions.

- [ ] **Step 4: Run complete Plan 01 gate**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
bun scripts/verify-workspace.ts
bun --cwd apps/web run check
bun --cwd apps/web run test
```

Expected: all exit 0. Confirm test inventory includes four run types, Published immutability, operational-only RunProfile, deterministic matcher permutations/ambiguity, valid cycles, and Source boundary.

- [ ] **Step 5: Commit**

```bash
git add crates/erabi-domain
 git commit -m "feat(domain): add source and discovery transition contracts"
```

## Plan 01 Gate

Do not start Plan 02 until all commands in Task 5 Step 4 pass from a clean checkout and `git status --short` is empty.
