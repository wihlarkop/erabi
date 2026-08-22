# Erabi Domain and Workspace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bootstrap Cargo/Bun workspaces and implement the dependency-light Crawler Studio domain contracts that every later subsystem consumes.

**Architecture:** Pure domain types live in `erabi-domain`; infrastructure crates are scaffolded but do not leak DB/Crawl4AI concerns into domain code. Crawler/CrawlerVersion/PageType are first-class; Source is supporting target/history identity.

**Tech Stack:** Stable Rust, Cargo workspace, Serde, UUIDv7, URL parsing, Bun, SvelteKit scaffold.

**Spec:** `docs/specs/01-product-and-experience.md`, `02-crawler-studio-domain.md`, `03-discovery-graph-and-runs.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Global Constraints

- Use stable dependencies only.
- IDs are UUIDv7 generated application-side.
- Published Crawler Versions are immutable.
- Exactly four official Crawl Run types.
- No global independent Schema lifecycle.

### Task 1: Bootstrap Cargo and Bun workspaces

**Files:** `Cargo.toml`, `rust-toolchain.toml`, `package.json`, `bunfig.toml`, `.env.example`, `crates/*`, `apps/web/*`.

- [ ] Write a workspace contract test asserting required Cargo/Bun members and package manager.
- [ ] Run it before scaffolding and confirm failure.
- [ ] Create focused crates: `erabi-domain`, `erabi-db`, `erabi-api`, `erabi-jobs`, `erabi-crawler`, `erabi-crawl4ai`, `erabi-extraction`, `erabi-export`, `erabi-artifacts`, `erabi-security`, `erabi-observability`, `erabi-cli`.
- [ ] Scaffold SvelteKit static SPA using Bun only.
- [ ] Run `cargo test --workspace`, `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and frontend `check`.

### Task 2: Implement identifiers, errors, and lifecycle enums

**Files:** `crates/erabi-domain/src/id.rs`, `error.rs`, `status.rs`; tests under `crates/erabi-domain/tests/`.

- [ ] Test UUIDv7 generation/serialization and stable error-code envelopes.
- [ ] Implement `CrawlRunType::{QuickScrape, TestRun, DiscoveryPreview, ProductionRun}` and lifecycle states `Queued/Running/Succeeded/PartialResult/Failed/Cancelled`.
- [ ] Implement `ErrorCode` including `SCHEMA_DRIFT`, `AMBIGUOUS_PAGE_TYPE`, `UNRESOLVED_REFERENCE`, `STORAGE_CRITICAL`, `CRAWLER_UNAVAILABLE`.
- [ ] Verify no `Batch` run type exists.

### Task 3: Implement Crawler and Crawler Version contracts

**Files:** `crawler.rs`, `crawler_version.rs`, `seed.rs`, `run_profile.rs`, `test_evidence.rs`.

- [ ] Test at-most-one active Draft, immutable Published versions, activation pointer changes, and historical run-version retention.
- [ ] Implement `Crawler`, `CrawlerVersion`, `Seed`, `RunProfile`, `TestEvidence`.
- [ ] Ensure semantic behavior belongs to CrawlerVersion while operational RunProfile fields cannot override semantic structure.

### Task 4: Implement Page Types and deterministic URL matching

**Files:** `page_type.rs`, `url_matcher.rs`, `matching.rs`; test `page_type_matching.rs`.

- [ ] Write tests that reverse creation/insertion/database ordering and still obtain the same winner.
- [ ] Represent matcher specificity key as `(kind_rank, literal_segments, explicit_query_constraints, literal_chars, inverse_wildcards)` after explicit Page Type priority.
- [ ] Implement matcher kind order: exact canonical URL > exact host+path/template > path glob/prefix > regex.
- [ ] Return `AMBIGUOUS_PAGE_TYPE` for complete ties; never use UUID/order as implicit tie-break.

### Task 5: Implement transitions, Source, Collection, and naming

**Files:** `transition.rs`, `source.rs`, `collection.rs`, `naming.rs`.

- [ ] Test cycles are valid domain data and transition budgets are explicit.
- [ ] Implement Source with original/canonical URL, target type (`WebPage`/`FileAsset`), lifecycle, optional Collection, run/artifact associations.
- [ ] Prove Source cannot mutate Crawler Seeds implicitly.
- [ ] Implement deterministic source/dataset display naming.

**Gate:** domain tests cover immutable versions, all four run types, deterministic match ties, Source boundary, and transition cycles; workspace checks pass.
