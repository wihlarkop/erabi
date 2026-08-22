# Erabi Crawler Studio and Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Crawler/CrawlerVersion authoring, Page Type configuration, deterministic matching explanations, canonicalization/domain scope, directed discovery with budgets, Test Lab, Discovery Preview, validated publication, and complete-snapshot structural gates.

**Architecture:** All semantic crawling configuration is versioned inside `CrawlerVersion`. Drafts are editable/testable; Published versions are immutable production inputs. Discovery services depend on a small `DiscoveryPageProvider` port so deterministic fixtures can validate graph behavior before Plan 06 wires Crawl4AI. Publish validation is contributor-based: this plan supplies crawler/discovery structural validation, while Plan 07 adds extraction/Dataset/unique-key validation after those contracts exist.

**Tech Stack:** stable Rust, Axum, Turso repositories from Plan 02, `url`, stable regex/glob/public-suffix support selected with `cargo add`, deterministic local fixtures.

**Spec:** `docs/specs/02-crawler-studio-domain.md`, `docs/specs/03-discovery-graph-and-runs.md`, `docs/specs/08-ux-accessibility-and-verification.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Global Constraints

- Crawler is the primary reusable design object; Source is not a substitute.
- Published Crawler Versions are immutable; normal Production Runs use Published versions.
- Drafts may be exercised by Test Run and Discovery Preview.
- Domain scope, canonicalization, Page Types, transitions, extraction definitions, unique keys, and Dataset mappings are semantic versioned configuration.
- Run Profiles/per-run overrides cannot change semantic configuration.
- URL matching is deterministic and explainable; complete tie remains `AMBIGUOUS_PAGE_TYPE`.
- Default domain scope is seed domains only; external URLs are preserved as `EXTERNAL` but not crawled.
- Cycles are valid only with mandatory crawler/Page-Type/transition guardrails and deduplication.
- Discovery Preview is bounded sampling and never a complete production snapshot.
- Warnings do not block publish; structural errors do.
- A validation concern cannot be silently skipped merely because its implementation arrives in a later plan: use the explicit contributor interface and register it when the owning contracts are added.

## Focused File Map

```text
crates/erabi-crawler/src/canonicalize.rs
crates/erabi-crawler/src/scope.rs
crates/erabi-crawler/src/discovery.rs
crates/erabi-crawler/src/provider.rs
crates/erabi-crawler/src/test_lab.rs
crates/erabi-crawler/src/discovery_preview.rs
crates/erabi-crawler/src/publish_validation.rs
crates/erabi-crawler/src/snapshot_health.rs
crates/erabi-api/src/routes/crawlers.rs
crates/erabi-api/src/routes/page_types.rs
crates/erabi-api/src/routes/test_lab.rs
crates/erabi-api/src/routes/discovery_preview.rs
```

---

### Task 1: Implement Crawler Draft/Published authoring APIs

**Files:**
- Create: `crates/erabi-api/src/routes/crawlers.rs`
- Create: `crates/erabi-api/src/dto/crawlers.rs`
- Modify: `crates/erabi-api/src/app.rs`
- Extend: `crates/erabi-db/src/repositories/crawlers.rs`
- Test: `crates/erabi-api/tests/crawler_version_flow.rs`

**Interfaces:**
- Consumes Plan 01 Crawler/CrawlerVersion types and Plan 02 repository invariants.
- Produces explicit create/read Draft/Published actions; no generic table mutation API.

- [ ] **Step 1: Write failing lifecycle tests**

```rust
#[tokio::test]
async fn editing_published_version_returns_conflict() {
    let fixture = erabi_api::test_support::crawler_with_published_version().await;
    let response = fixture.patch_version(fixture.published_id, serde_json::json!({"name":"mutated"})).await;
    assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);
}

#[tokio::test]
async fn draft_from_old_version_does_not_mutate_parent() {
    let fixture = erabi_api::test_support::crawler_with_two_published_versions().await;
    let hash = fixture.version_hash(fixture.v1).await;
    let draft = fixture.create_draft_from(fixture.v1).await;
    assert_ne!(draft.id, fixture.v1);
    assert_eq!(fixture.version_hash(fixture.v1).await, hash);
}
```

Also test at-most-one ordinary active Draft and activating an older Published version changes only the active pointer.

- [ ] **Step 2: Run RED**

```bash
cargo test -p erabi-api --test crawler_version_flow
```

- [ ] **Step 3: Implement typed routes**

```text
POST   /api/v1/crawlers
GET    /api/v1/crawlers
GET    /api/v1/crawlers/{crawler_id}
POST   /api/v1/crawlers/{crawler_id}/drafts
GET    /api/v1/crawlers/{crawler_id}/versions/{version_id}
PATCH  /api/v1/crawlers/{crawler_id}/versions/{version_id}/draft
POST   /api/v1/crawlers/{crawler_id}/versions/{version_id}/publish
POST   /api/v1/crawlers/{crawler_id}/versions/{version_id}/activate
```

Draft PATCH uses expected revision/hash. Publish route invokes `PublishValidator` introduced in Task 6 before calling Plan 02 transactional publication. Until Task 6 exists, keep publish route test-double wired and do not claim final publish gate behavior.

- [ ] **Step 4: Run GREEN and commit**

```bash
cargo test -p erabi-api --test crawler_version_flow
git add crates/erabi-api crates/erabi-db
git commit -m "feat(studio): add crawler draft and version APIs"
```

---

### Task 2: Implement Draft Page Type/matcher authoring and deterministic explanation service

**Files:**
- Create: `crates/erabi-api/src/routes/page_types.rs`
- Create: `crates/erabi-api/src/dto/page_types.rs`
- Create: `crates/erabi-crawler/src/matching.rs`
- Modify: `crates/erabi-crawler/src/lib.rs`
- Test: `crates/erabi-crawler/tests/matching_service.rs`
- Test: `crates/erabi-api/tests/page_type_drafts.rs`

**Interfaces:**
- Consumes Plan 01 pure `resolve_page_type`.
- Produces `MatchExplanation { canonical_url, candidates, decision }`.

- [ ] **Step 1: Write failing explanation/order tests**

```rust
#[test]
fn explanation_contains_every_specificity_component() {
    let explanation = erabi_crawler::test_support::matching_service_fixture()
        .explain("https://shop.test/products/42?lang=en").unwrap();
    let candidate = explanation.candidates.first().unwrap();
    assert!(candidate.literal_path_segments > 0);
    assert!(candidate.literal_characters > 0);
    assert!(!candidate.matcher_kind.is_empty());
}

#[test]
fn row_order_cannot_resolve_a_complete_tie() {
    let a = erabi_crawler::test_support::tie_fixture(false);
    let b = erabi_crawler::test_support::tie_fixture(true);
    assert_eq!(a.decision, b.decision);
    assert!(matches!(a.decision, erabi_domain::PageTypeMatchDecision::Ambiguous { .. }));
}
```

- [ ] **Step 2: Run RED**

```bash
cargo test -p erabi-crawler --test matching_service
cargo test -p erabi-api --test page_type_drafts
```

- [ ] **Step 3: Implement Draft-only mutation and explanation**

Matcher DTOs are explicitly tagged by kind and validated before persistence. Service invokes Plan 01 resolver and returns Page Type ID/name, explicit priority, matcher kind/pattern, literal path segments, explicit query constraints, literal character count, wildcard/capture count, and rationale. Never sort complete ties by ID/time/row order.

Persist validated matcher definition plus reproducible specificity components or recompute and assert equality on load.

- [ ] **Step 4: Run GREEN and commit**

```bash
cargo test -p erabi-crawler --test matching_service
cargo test -p erabi-api --test page_type_drafts
git add crates/erabi-crawler crates/erabi-api crates/erabi-db
git commit -m "feat(studio): explain deterministic page type matching"
```

---

### Task 3: Implement versioned canonicalization and Domain Scope

**Files:**
- Create: `crates/erabi-crawler/src/canonicalize.rs`
- Create: `crates/erabi-crawler/src/scope.rs`
- Test: `crates/erabi-crawler/tests/canonicalization.rs`
- Test: `crates/erabi-crawler/tests/domain_scope.rs`

**Interfaces:**
- Produces `CanonicalizationPolicy`, `CanonicalizationDecision`, `DomainScopePolicy`, `ScopeDecision`.

- [ ] **Step 1: Add only required stable URL/pattern/public-suffix dependencies with `cargo add`**

Do not hand-write registrable-domain/public-suffix logic. Keep dependency use isolated to this crate.

- [ ] **Step 2: Write failing canonicalization tests**

Cover scheme/host case, default ports, fragment removal, consistent path handling, query sorting, default `utm_*`/`fbclid`/`gclid` removal, unknown meaningful query preservation, explicit crawler keep/drop rules, and original URL retention.

```rust
#[test]
fn tracking_drops_but_unknown_semantics_remain() {
    let result = erabi_crawler::CanonicalizationPolicy::default()
        .apply("HTTPS://Example.COM:443/p?id=7&utm_source=x&variant=blue#frag").unwrap();
    assert_eq!(result.canonical.as_str(), "https://example.com/p?id=7&variant=blue");
}
```

- [ ] **Step 3: Write failing scope tests**

Default seed-domains-only permits seed host and classifies another registrable domain `EXTERNAL`. Explicit subdomain/allowlist/custom policies preserve out-of-scope URL/provenance rather than dropping it.

- [ ] **Step 4: Run RED, implement pure policies, then GREEN**

```bash
cargo test -p erabi-crawler --test canonicalization --test domain_scope
```

Canonicalization returns original + canonical + transformation explanation. Scope classification runs after canonicalization and belongs to CrawlerVersion semantic config, never RunProfile/per-run DTOs.

```bash
cargo test -p erabi-crawler --test canonicalization --test domain_scope
git add Cargo.lock crates/erabi-crawler
git commit -m "feat(crawler): canonicalize URLs and enforce domain scope"
```

---

### Task 4: Implement directed discovery, provenance, deduplication, cycles, and budgets

**Files:**
- Create: `crates/erabi-crawler/src/provider.rs`
- Create: `crates/erabi-crawler/src/discovery.rs`
- Create: `tests/fixtures/discovery/cycle.json`
- Create: `tests/fixtures/discovery/external-links.json`
- Test: `crates/erabi-crawler/tests/discovery_graph.rs`

**Interfaces:**
- Produces `DiscoveryPageProvider`, `DiscoveryEngine`, `DiscoveredUrl`, `DiscoveryDisposition`, `DiscoveryBudgetState`.

- [ ] **Step 1: Write failing bounded-cycle/external tests**

```rust
#[tokio::test]
async fn self_cycle_stops_at_budget_without_duplicate_enqueues() {
    let engine = erabi_crawler::test_support::bounded_engine(
        erabi_crawler::test_support::cycle_provider(), 10, 3
    );
    let result = engine.preview().await.unwrap();
    assert!(result.pages_sampled <= 10);
    assert!(result.duplicates_prevented > 0);
}
```

External URL test asserts provider is never asked to fetch the external target.

- [ ] **Step 2: Run RED**

```bash
cargo test -p erabi-crawler --test discovery_graph
```

- [ ] **Step 3: Define the pre-Crawl4AI provider port and fixed pipeline**

```rust
#[async_trait::async_trait]
pub trait DiscoveryPageProvider: Send + Sync {
    async fn fetch_discovery_page(&self, url: &url::Url) -> Result<DiscoveryPage, DiscoveryProviderError>;
}
```

Pipeline is fixed:

```text
raw href → resolve → validate → canonicalize → scope → dedupe
→ Page Type match → transition validation → budgets → enqueue/preserve
```

Every discovered URL stores source Page Type, transition, selector/rule, raw href, resolved original URL, canonical URL, timestamp, and decision rationale. Preserve `UNMATCHED`, `AMBIGUOUS_PAGE_TYPE`, `EXTERNAL`, blocked, duplicate, budget-excluded, completed states.

- [ ] **Step 4: Implement mandatory crawler/Page-Type/transition guardrails**

Require max pages/depth/duration/storage; enforce transition links-per-page + optional total budget and optional Page Type budget. Reject runnable config lacking mandatory global guardrails.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-crawler --test discovery_graph
git add Cargo.lock crates/erabi-crawler tests/fixtures/discovery
git commit -m "feat(discovery): traverse bounded crawler graphs"
```

---

### Task 5: Implement Test Lab and Discovery Preview with durable Test Evidence

**Files:**
- Create: `crates/erabi-crawler/src/test_lab.rs`
- Create: `crates/erabi-crawler/src/discovery_preview.rs`
- Create: `crates/erabi-api/src/routes/test_lab.rs`
- Create: `crates/erabi-api/src/routes/discovery_preview.rs`
- Modify: `crates/erabi-api/src/app.rs`
- Test: `crates/erabi-api/tests/test_lab.rs`
- Test: `crates/erabi-api/tests/discovery_preview.rs`

**Interfaces:**
- Produces Draft-version focused Test Lab operations and bounded Discovery Preview summary.
- Persists Plan 01 `TestEvidence` through Plan 02 repository.

- [ ] **Step 1: Write failing Test Lab evidence tests**

Match test response includes exact Draft/version config hash, input URL, canonicalization explanation, all Page Type candidates/specificity rationale, decision, warnings/errors, Test Evidence ID. Complete tie exposes `AMBIGUOUS_PAGE_TYPE` and all tied candidates.

Extraction/pagination/transition tests use explicit ports/results; Plan 06/07 adapters register their implementations without changing Test Lab response contracts.

- [ ] **Step 2: Write failing Discovery Preview tests**

Summary includes sampled pages, discovered/canonical uniques, duplicates, Page Type distribution, ambiguous/unmatched/external/blocked lists, transition counts, robots-exclusion hook, budget hits, growth warnings. Assert `is_complete_production_snapshot == false` always.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-api --test test_lab --test discovery_preview
```

- [ ] **Step 4: Implement bounded services/routes**

Input explicitly sets selected seed IDs plus low page/depth/transition/time caps. Growth warning codes cover query-space expansion, cyclic concentration, unmatched/ambiguity ratios, and budget forecast. Never present sampled estimate as guaranteed site size; never mutate Published configuration/trusted production approval.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-api --test test_lab --test discovery_preview
git add crates/erabi-crawler crates/erabi-api crates/erabi-db
git commit -m "feat(studio): add Test Lab and Discovery Preview"
```

---

### Task 6: Implement extensible publish validation and structural snapshot health

**Files:**
- Create: `crates/erabi-crawler/src/publish_validation.rs`
- Create: `crates/erabi-crawler/src/snapshot_health.rs`
- Modify: `crates/erabi-api/src/routes/crawlers.rs`
- Test: `crates/erabi-crawler/tests/publish_validation.rs`
- Test: `crates/erabi-crawler/tests/snapshot_health.rs`

**Interfaces:**
- Produces `VersionValidationContributor`, `PublishValidator`, `PublishValidation { errors, warnings }`.
- Produces `SnapshotHealth::{Complete, Incomplete(Vec<HealthReason>)}` consumed/extended by Plans 06–07.

- [ ] **Step 1: Write failing base-validation tests**

This plan's built-in contributor blocks: no enabled seed, invalid matcher syntax, unresolved known Page Type ambiguity, transition missing endpoints, missing mandatory guardrails, invalid domain scope, invalid canonicalization, invalid crawl budgets. Warnings: untested Page Type, transition without evidence, broad matcher, rapid-growth preview. Warnings alone permit publish.

Do **not** pretend extraction/shared-Dataset/unique-key validation is implemented here; those types are introduced in Plan 07.

- [ ] **Step 2: Write failing contributor-extension test**

```rust
pub trait VersionValidationContributor: Send + Sync {
    fn validate(&self, context: &VersionValidationContext) -> Vec<VersionValidationIssue>;
}
```

Test a fake contributor returning a blocking error and prove `PublishValidator` merges it with base validation and prevents publication. The contributor interface is the mandatory integration point Plan 07 uses for extraction definition, shared Dataset compatibility, and unique-key validation.

- [ ] **Step 3: Write failing structural snapshot-health tests**

Unresolved ambiguity, unexpected pagination truncation, required page failure, cancellation produce `Incomplete`. Include explicit `HealthSignal::SchemaDrift` variant now, but detection is registered/produced by Plan 07—not fabricated here.

- [ ] **Step 4: Run RED**

```bash
cargo test -p erabi-crawler --test publish_validation --test snapshot_health
```

- [ ] **Step 5: Implement base validator, contributor composition, and publication wiring**

Publish route invokes all registered contributors then base validation. Errors → 422/no publication. Warnings are returned visibly; explicit publish request confirms the exact Draft ID/revision/config hash being published, not a generic “override validation” switch. Plan 02 repository atomically publishes + audits only after validation passes.

`SnapshotHealth` only classifies trustworthiness; it does not itself generate missing candidates. Plan 07 combines structural + execution + extraction health before candidate generation.

- [ ] **Step 6: Run Plan 05 gate and commit**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p erabi-crawler
cargo test -p erabi-api --test crawler_version_flow --test page_type_drafts --test test_lab --test discovery_preview
```

Expected: multi-seed/multi-Page-Type discovery-only Draft can Test Lab/Preview/publish when base validation passes; specificity tie stays ambiguous across ordering; cyclic preview terminates; external URL remains preserved/outside scope; fake validation contributor can block publish.

```bash
git add crates/erabi-crawler crates/erabi-api crates/erabi-db
git commit -m "feat(studio): validate publication and snapshot structure"
```

## Plan 05 Gate

Do not start Plan 06 until Task 6 Step 6 passes from a clean checkout and `git status --short` is empty.
