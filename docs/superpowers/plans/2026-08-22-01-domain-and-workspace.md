# Erabi Domain and Workspace Implementation Plan

> **For agentic workers:** Implement each task as a complete scoped feature, then build/check it, add or update meaningful tests, run verification, and commit. Do not use RED/GREEN or test-first ceremony unless explicitly requested by the user.

**Goal:** Bootstrap Cargo/Bun workspaces and implement the dependency-light Crawler Studio domain contracts that every later subsystem consumes.

**Architecture:** Pure domain types live in `erabi-domain`; infrastructure crates are scaffolded but do not leak DB/Crawl4AI concerns into domain code. Crawler/CrawlerVersion/PageType are first-class; Source is supporting target/history identity.

**Tech Stack:** Stable Rust, Cargo workspace, Serde, UUIDv7, URL parsing, Bun, SvelteKit scaffold.

**Spec:** `docs/specs/01-product-and-experience.md`, `docs/specs/02-crawler-studio-domain.md`, `docs/specs/03-discovery-graph-and-runs.md`, `docs/specs/05-system-architecture-and-persistence.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Global Constraints

- Use current compatible stable dependencies only.
- IDs are UUIDv7 generated application-side and serialized as canonical UUID strings at API boundaries.
- Published Crawler Versions are immutable.
- Exactly four Crawl Run types exist: `QUICK_SCRAPE`, `TEST_RUN`, `DISCOVERY_PREVIEW`, `PRODUCTION_RUN`.
- There is no `BATCH` run type.
- Source does not replace Crawler, Seed, Page Type, Dataset, or Crawl Run.
- Extraction configuration belongs to Page Types inside Crawler Versions; do not create a global Schema lifecycle.
- Page Type matching is deterministic and never uses insertion/database/map/UUID order as a hidden tie-break.
- Do not implement roadmap-only functionality.

## Target repository shape

```text
Cargo.toml
Cargo.lock
rust-toolchain.toml
package.json
bun.lock
bunfig.toml
.env.example
apps/web/
crates/
├── erabi-domain/
├── erabi-db/
├── erabi-api/
├── erabi-jobs/
├── erabi-crawler/
├── erabi-crawl4ai/
├── erabi-extraction/
├── erabi-export/
└── erabi-cli/
migrations/
docker/
```

Do not add Turborepo, Nx, npm/pnpm/yarn lockfiles, Redis, external queues, or speculative framework layers.

---

### Task 1: Bootstrap Cargo and Bun workspaces

**Files:**
- Create/modify root workspace manifests and toolchain files.
- Create focused Rust crates listed above.
- Create `apps/web/` SvelteKit SPA scaffold using Bun only.
- Create `.env.example` with names/placeholders only; never real secrets.

**Implementation requirements:**

- Configure one Cargo workspace containing the Rust crates above.
- Keep `erabi-domain` dependency-light; infrastructure crates may depend on domain, not the reverse.
- Configure Bun workspace/package scripts for the SvelteKit app.
- Use `cargo add` and `bun add` for dependencies rather than hand-pinning speculative versions.
- Commit `Cargo.lock` and `bun.lock`.
- Keep `apps/desktop/` post-MVP; do not scaffold Tauri now.

**Verification:**

```bash
cargo metadata --no-deps
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
bun install --frozen-lockfile
bun --cwd apps/web run check
```

Add/update workspace contract tests or scripts as useful to assert the required members/package manager and absence of unsupported JS lockfiles. Tests are verification, not a prerequisite to writing the workspace.

**Commit:** one focused workspace/bootstrap commit after all checks pass.

---

### Task 2: Implement identifiers, error codes, and lifecycle enums

**Files:**
- `crates/erabi-domain/src/id.rs`
- `crates/erabi-domain/src/error.rs`
- `crates/erabi-domain/src/status.rs`
- `crates/erabi-domain/src/lib.rs`
- domain tests under `crates/erabi-domain/tests/`

**Interfaces to produce:**

- strongly typed UUIDv7 IDs for major domain entities;
- stable `ErrorCode` enum including at least `SCHEMA_DRIFT`, `AMBIGUOUS_PAGE_TYPE`, `UNRESOLVED_REFERENCE`, `STORAGE_CRITICAL`, `CRAWLER_UNAVAILABLE`;
- `CrawlRunType::{QuickScrape, TestRun, DiscoveryPreview, ProductionRun}`;
- lifecycle states needed by the canonical run/domain model.

**Implementation requirements:**

- UUID generation is application-side UUIDv7.
- IDs serialize/deserialize predictably and do not expose DB implementation details.
- Stable error codes are separate from user-facing message text.
- Do not add `Batch` to the run-type enum.
- Keep expected failures as typed Results; do not model normal errors as panics.

**Verification:**

Add tests for UUIDv7 generation/roundtrip, stable error serialization, lifecycle serialization, and exact run-type membership. Then run:

```bash
cargo test -p erabi-domain
cargo clippy -p erabi-domain --all-targets -- -D warnings
```

**Commit:** identifiers/errors/lifecycle as one independently reviewable domain commit.

---

### Task 3: Implement Crawler and Crawler Version contracts

**Files:**
- `crates/erabi-domain/src/crawler.rs`
- `crates/erabi-domain/src/crawler_version.rs`
- `crates/erabi-domain/src/seed.rs`
- `crates/erabi-domain/src/run_profile.rs`
- `crates/erabi-domain/src/test_evidence.rs`
- exports in `lib.rs`
- focused domain tests

**Interfaces to produce:**

- `Crawler` with identity/metadata/optional Collection association and version pointers;
- `CrawlerVersion` with Draft/Published lifecycle;
- `Seed`, `RunProfile`, and `TestEvidence` contracts;
- operations that create Drafts from prior Published versions and reactivate Published pointers without mutation.

**Implementation requirements:**

- At most one ordinary active Draft per Crawler in MVP.
- Published versions are immutable values; editing means a new Draft identity.
- Historical run/version references must remain stable.
- Seeds are explicit versioned semantic configuration.
- RunProfile may override only operational settings, never PageType matchers/transitions/extraction/identity/canonicalization/domain scope.
- TestEvidence is durable confidence metadata, never production approval.

**Verification:**

Test at-most-one Draft behavior, immutable Published semantics, Draft-from-Published cloning, active-version pointer switching, and RunProfile semantic-boundary rejection.

```bash
cargo test -p erabi-domain crawler
cargo test -p erabi-domain crawler_version
cargo clippy -p erabi-domain --all-targets -- -D warnings
```

**Commit:** Crawler/version domain contract after verification.

---

### Task 4: Implement Page Types and deterministic URL matching

**Files:**
- `crates/erabi-domain/src/page_type.rs`
- `crates/erabi-domain/src/url_matcher.rs`
- `crates/erabi-domain/src/matching.rs`
- test `crates/erabi-domain/tests/page_type_matching.rs`

**Interfaces to produce:**

- `PageType` with explicit integer priority and matcher collection;
- validated URL matcher variants for exact canonical URL, exact host+path/template, path glob/prefix, and regex;
- explainable match candidate/result structures;
- `AMBIGUOUS_PAGE_TYPE` result for complete ties.

**Canonical resolution order:**

1. higher explicit Page Type priority;
2. matcher-kind rank: exact canonical URL > exact host+path/template > path glob/prefix > regex;
3. more literal path segments;
4. more explicit query-key/value constraints;
5. more literal characters;
6. fewer wildcard/capture tokens;
7. complete tie => ambiguity, never an implicit winner.

Represent the deterministic matcher specificity key explicitly so it can be persisted or reproducibly recomputed from the validated matcher definition.

**Verification:**

Tests must reverse creation order, matcher insertion order, and database-row fixture order and obtain the same winner/ambiguity. Include exact complete-tie ambiguity and unmatched cases.

```bash
cargo test -p erabi-domain --test page_type_matching
cargo clippy -p erabi-domain --all-targets -- -D warnings
```

**Commit:** deterministic Page Type matching as a standalone domain commit.

---

### Task 5: Implement DiscoveryTransition, Source, Collection, and deterministic naming

**Files:**
- `crates/erabi-domain/src/transition.rs`
- `crates/erabi-domain/src/source.rs`
- `crates/erabi-domain/src/collection.rs`
- `crates/erabi-domain/src/naming.rs`
- exports/tests

**Interfaces to produce:**

- `DiscoveryTransition` with source/target Page Types, selector/rule, priority, per-page link limit, optional total budget, depth contribution, enabled state, and provenance/test-evidence hooks;
- `Source` with original/canonical URL, target type (`WebPage`/`FileAsset`), lifecycle, optional Collection, run/artifact associations;
- `Collection` identity/metadata foundation;
- deterministic display naming helpers.

**Implementation requirements:**

- Cycles are valid domain data; guardrails/budgets make them safe later.
- Source is supporting target/history identity and may exist without a Crawler for Quick Scrape.
- Source metadata changes must never silently rewrite Crawler Seeds.
- Preserve original and canonical URL separately.
- Naming helpers must be deterministic and safe for display/slug use without silently changing identity semantics.

**Verification:**

Test valid cyclic transition data, explicit transition budgets, Source target types, Source/Seed independence, Collection association, and deterministic naming.

```bash
cargo test -p erabi-domain
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

**Commit:** transition/source/collection/naming domain foundation.

---

## Plan 01 Gate

From a clean checkout of the Plan 01 result, run:

```bash
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
bun install --frozen-lockfile
bun --cwd apps/web run check
```

Then confirm all of the following by tests/code review:

- exactly four run types and no Batch run type;
- Published Crawler Versions cannot be mutated;
- Source cannot replace or silently mutate Crawler Seed semantics;
- Page Type resolution is independent of insertion/database/map/UUID order;
- complete specificity ties remain `AMBIGUOUS_PAGE_TYPE`;
- cycles are valid transition data with explicit guardrail fields;
- no global Schema lifecycle or roadmap-only subsystem was introduced;
- workspace/package-manager constraints match the canonical spec.

Do not begin Plan 02 until this gate passes with fresh evidence.
