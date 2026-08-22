# Erabi Crawler Studio and Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Crawler/CrawlerVersion authoring, Page Type configuration, deterministic matching explanations, canonicalization/domain scope, directed discovery with budgets, Test Lab, Discovery Preview, validated publication, and complete-snapshot structural gates.

**Architecture:** All semantic crawling/extraction configuration is versioned inside `CrawlerVersion`. Drafts are editable/testable; Published versions are immutable production inputs. Discovery services depend on a small `DiscoveryPageProvider` port so deterministic fixtures can validate graph behavior before Plan 06 wires Crawl4AI.

**Tech Stack:** stable Rust, Axum, Turso repositories from Plan 02, `url`, regex/glob parsing selected via stable crates, deterministic local fixtures.

**Spec:** `docs/specs/02-crawler-studio-domain.md`, `docs/specs/03-discovery-graph-and-runs.md`, `docs/specs/08-ux-accessibility-and-verification.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Global Constraints

- Crawler is the primary reusable design object; Source is not a substitute.
- Published Crawler Versions are immutable; normal Production Runs use Published versions.
- Drafts may be tested through Test Run and Discovery Preview.
- Domain scope, canonicalization, Page Types, transitions, extraction definitions, unique keys, and Dataset mappings are semantic versioned configuration.
- Run Profiles/per-run overrides cannot change semantic configuration.
- URL matching is deterministic and explainable; complete tie remains `AMBIGUOUS_PAGE_TYPE`.
- Default domain scope is seed domains only; external URLs are preserved as `EXTERNAL` but not crawled.
- Cycles are valid only with mandatory global/Page-Type/transition guardrails and deduplication.
- Discovery Preview is bounded sampling and never a complete production snapshot.
- Warnings do not block publish; structural errors do.

## Focused File Map

```text
crates/erabi-crawler/src/canonicalize.rs
crates/erabi-crawler/src/scope.rs
crates/erabi-crawler/src/discovery.rs
crates/erabi-crawler/src/test_lab.rs
crates/erabi-crawler/src/publish_validation.rs
crates/erabi-api/src/routes/crawlers.rs
crates/erabi-api/src/routes/page_types.rs
crates/erabi-api/src/routes/test_lab.rs
crates/erabi-api/src/routes/discovery_preview.rs
crates/erabi-api/tests/crawler_studio.rs
tests/fixtures/discovery/
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
- Consumes `CrawlerRepository` and Plan 01 domain types.
- Produces CRUD/read endpoints for Crawlers and Draft configuration.
- Produces explicit actions `create_draft_from_version`, `publish_draft`, `activate_published_version`.

- [ ] **Step 1: Write failing API lifecycle tests**

```rust
#[tokio::test]
async fn editing_published_version_returns_conflict() {
    let fixture = erabi_api::test_support::crawler_with_published_version().await;
    let response = fixture.patch_version(fixture.published_id, serde_json::json!({"name": "mutated"})).await;
    assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);
}

#[tokio::test]
async fn draft_can_be_created_from_older_published_version_without_mutating_it() {
    let fixture = erabi_api::test_support::crawler_with_two_published_versions().await;
    let original_hash = fixture.version_hash(fixture.v1).await;
    let draft = fixture.create_draft_from(fixture.v1).await;
    assert_ne!(draft.id, fixture.v1);
    assert_eq!(fixture.version_hash(fixture.v1).await, original_hash);
}
```

Also test at-most-one ordinary active Draft and activating an older Published version changes only the crawler pointer.

- [ ] **Step 2: Run RED**

```bash
cargo test -p erabi-api --test crawler_version_flow
```

- [ ] **Step 3: Implement typed routes and repository actions**

Use routes under:

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

PATCH validates state is Draft and uses expected revision/hash for optimistic concurrency. Publish endpoint initially calls a validation service stub returning typed validation output; Task 6 supplies the complete implementation. Never expose generic table mutation endpoints.

- [ ] **Step 4: Run GREEN and commit**

```bash
cargo test -p erabi-api --test crawler_version_flow
git add crates/erabi-api crates/erabi-db
 git commit -m "feat(studio): add crawler draft and version APIs"
```

---

### Task 2: Implement Page Type authoring and deterministic matching explanation service

**Files:**
- Create: `crates/erabi-api/src/routes/page_types.rs`
- Create: `crates/erabi-api/src/dto/page_types.rs`
- Create: `crates/erabi-crawler/src/matching.rs`
- Modify: `crates/erabi-crawler/src/lib.rs`
- Test: `crates/erabi-crawler/tests/matching_service.rs`
- Test: `crates/erabi-api/tests/page_type_drafts.rs`

**Interfaces:**
- Consumes pure domain matcher resolver from Plan 01.
- Produces Draft-only Page Type/matcher CRUD.
- Produces `MatchExplanation { canonical_url, candidates, decision }`.

- [ ] **Step 1: Write failing explanation/order tests**

```rust
#[test]
fn explanation_contains_every_specificity_component() {
    let service = erabi_crawler::test_support::matching_service_fixture();
    let explanation = service.explain("https://shop.test/products/42?lang=en").unwrap();
    let candidate = explanation.candidates.first().unwrap();
    assert!(candidate.literal_path_segments > 0);
    assert!(candidate.literal_characters > 0);
    assert!(!candidate.matcher_kind.is_empty());
}

#[test]
fn db_row_order_cannot_change_complete_tie_into_a_winner() {
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

- [ ] **Step 3: Implement service and Draft-only mutation rules**

Add Page Type/matcher routes only under a Draft version. Matcher DTOs must be tagged by explicit kind and validate syntax before persistence. Matching service loads Page Types, invokes Plan 01 pure resolver, and returns explicit priority/kind/specificity rationale. It never sorts by entity ID, creation timestamp, or row order after a complete resolution-key tie.

Persist either validated specificity components or enough normalized matcher definition to reproducibly recompute them; tests compare persisted/recomputed values.

- [ ] **Step 4: Run GREEN and commit**

```bash
cargo test -p erabi-crawler --test matching_service
cargo test -p erabi-api --test page_type_drafts
git add crates/erabi-crawler crates/erabi-api crates/erabi-db
 git commit -m "feat(studio): explain deterministic page type matching"
```

---

### Task 3: Implement versioned canonicalization and Domain Scope policy

**Files:**
- Create: `crates/erabi-crawler/src/canonicalize.rs`
- Create: `crates/erabi-crawler/src/scope.rs`
- Test: `crates/erabi-crawler/tests/canonicalization.rs`
- Test: `crates/erabi-crawler/tests/domain_scope.rs`

**Interfaces:**
- Produces `CanonicalizationPolicy`, `CanonicalizationDecision`, `DomainScopePolicy`, `ScopeDecision`.

- [ ] **Step 1: Add stable URL-pattern support dependencies only as required**

Use `cargo add` after choosing the minimum stable crates needed for registrable-domain/public-suffix and glob/regex validation. Keep all dependency choices isolated in this crate; do not hand-write a public-suffix parser.

- [ ] **Step 2: Write failing canonicalization tests**

Test exactly:

```text
HTTP scheme/host case normalization
remove default :80/:443
remove fragment
consistent empty/trailing path handling
sort query parameters
remove utm_*, fbclid, gclid by default
preserve unknown meaningful query parameters
crawler-specific explicit keep/drop rules
retain original URL separately
```

Example:

```rust
#[test]
fn tracking_params_drop_but_unknown_semantics_remain() {
    let policy = erabi_crawler::CanonicalizationPolicy::default();
    let result = policy.apply("HTTPS://Example.COM:443/p?id=7&utm_source=x&variant=blue#frag").unwrap();
    assert_eq!(result.canonical.as_str(), "https://example.com/p?id=7&variant=blue");
}
```

- [ ] **Step 3: Write failing scope tests**

Default seed-domains-only allows seed host and blocks a different registrable domain as `EXTERNAL`. Explicit allowlist/subdomain policy tests must preserve out-of-scope URL/provenance rather than dropping it.

- [ ] **Step 4: Implement pure policies**

Canonicalization returns both original parsed URL and canonical URL plus an explanation of transformations. Scope classification runs after canonicalization. Domain Scope is serialized inside CrawlerVersion semantic configuration and is not accepted in RunProfile/per-run override DTOs.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-crawler --test canonicalization --test domain_scope
git add Cargo.lock crates/erabi-crawler
 git commit -m "feat(crawler): canonicalize URLs and enforce domain scope"
```

---

### Task 4: Implement directed discovery transitions, deduplication, cycles, and budgets

**Files:**
- Create: `crates/erabi-crawler/src/discovery.rs`
- Create: `crates/erabi-crawler/src/provider.rs`
- Create: `tests/fixtures/discovery/cycle.json`
- Create: `tests/fixtures/discovery/external-links.json`
- Test: `crates/erabi-crawler/tests/discovery_graph.rs`

**Interfaces:**
- Produces `DiscoveryPageProvider` trait.
- Produces `DiscoveryEngine`, `DiscoveredUrl`, `DiscoveryDisposition`, `DiscoveryBudgetState`.

- [ ] **Step 1: Write failing cyclic bounded-discovery test**

```rust
#[tokio::test]
async fn self_transition_cycle_stops_at_budget_without_duplicate_enqueues() {
    let provider = erabi_crawler::test_support::cycle_provider();
    let engine = erabi_crawler::test_support::bounded_engine(provider, 10, 3);
    let result = engine.preview().await.unwrap();
    assert!(result.pages_sampled <= 10);
    assert!(result.duplicates_prevented > 0);
    assert!(result.budget_hits.iter().any(|x| x.kind == "MAX_PAGES"));
}
```

Add external-link test asserting disposition is `ExternalPreserved` and provider is never asked to load it.

- [ ] **Step 2: Run RED**

```bash
cargo test -p erabi-crawler --test discovery_graph
```

- [ ] **Step 3: Define provider and canonical pipeline**

```rust
#[async_trait::async_trait]
pub trait DiscoveryPageProvider: Send + Sync {
    async fn fetch_discovery_page(&self, url: &url::Url) -> Result<DiscoveryPage, DiscoveryProviderError>;
}
```

Engine order is fixed:

```text
raw href
→ resolve against source URL
→ validate
→ canonicalize
→ scope classify
→ deduplicate canonical identity
→ Page Type match
→ transition validation
→ page/type/transition/depth/time/storage budget checks
→ enqueue or preserve-only disposition
```

Each `DiscoveredUrl` stores source Page Type, transition ID, selector/rule, raw href, resolved original URL, canonical URL, timestamp, and decision rationale. `UNMATCHED`, `AMBIGUOUS_PAGE_TYPE`, `EXTERNAL`, blocked, duplicate, budget-excluded, completed states remain inspectable.

- [ ] **Step 4: Implement mandatory guardrails**

Engine requires Crawler-wide max pages/depth/duration/storage budget. Transition enforces per-page max links and optional total transition budget. Optional Page Type page budgets are enforced. A config lacking mandatory global guardrails is rejected before engine start.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-crawler --test discovery_graph
git add Cargo.lock crates/erabi-crawler tests/fixtures/discovery
 git commit -m "feat(discovery): traverse bounded crawler graphs"
```

---

### Task 5: Implement Test Lab and Discovery Preview services with durable Test Evidence

**Files:**
- Create: `crates/erabi-crawler/src/test_lab.rs`
- Create: `crates/erabi-crawler/src/discovery_preview.rs`
- Create: `crates/erabi-api/src/routes/test_lab.rs`
- Create: `crates/erabi-api/src/routes/discovery_preview.rs`
- Modify: `crates/erabi-api/src/app.rs`
- Test: `crates/erabi-api/tests/test_lab.rs`
- Test: `crates/erabi-api/tests/discovery_preview.rs`

**Interfaces:**
- Produces focused Test Lab operations over a Draft version.
- Produces bounded Discovery Preview summary and durable `TestEvidence`.

- [ ] **Step 1: Write failing Test Lab response tests**

Test Page Type match response contains exact Draft version ID/config hash, input URL, canonicalization explanation, all matcher candidates/specificity rationale, decision, warnings/errors, and persisted Test Evidence ID. A complete tie must expose `AMBIGUOUS_PAGE_TYPE` and both candidates.

Test extraction/pagination/transition hooks are represented as ports/results; Plan 06/07 wires crawling/extraction implementations without changing Test Lab response shape.

- [ ] **Step 2: Write failing Discovery Preview summary tests**

Fixture preview must return pages sampled, URLs discovered, canonical unique count, duplicate count, Page Type distribution, ambiguous/unmatched/external/blocked lists, transition counts, robots exclusions placeholder/hook, budget hits, and growth warnings. Assert `is_complete_production_snapshot == false` unconditionally.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-api --test test_lab --test discovery_preview
```

- [ ] **Step 4: Implement routes/services**

Use explicit bounded input DTOs with selected seed IDs, low page/depth/transition/time caps. Persist Test Evidence through Plan 02 repository. Growth warnings are stable codes such as query-space expansion, cyclic concentration, unmatched ratio, ambiguity ratio, and budget forecast; never present sampled count as guaranteed site total.

No Test Lab/Discovery Preview action mutates a Published Crawler Version or writes trusted production approval state.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-api --test test_lab --test discovery_preview
git add crates/erabi-crawler crates/erabi-api crates/erabi-db
 git commit -m "feat(studio): add Test Lab and Discovery Preview"
```

---

### Task 6: Implement publish validation and complete-snapshot structural health classification

**Files:**
- Create: `crates/erabi-crawler/src/publish_validation.rs`
- Create: `crates/erabi-crawler/src/snapshot_health.rs`
- Modify: `crates/erabi-api/src/routes/crawlers.rs`
- Test: `crates/erabi-crawler/tests/publish_validation.rs`
- Test: `crates/erabi-crawler/tests/snapshot_health.rs`

**Interfaces:**
- Produces `PublishValidation { errors, warnings }`.
- Produces `SnapshotHealth::{Complete, Incomplete(Vec<HealthReason>)}` structural component consumed by Plan 06/07.

- [ ] **Step 1: Write failing publish validation matrix**

Blocking errors include: no enabled seed, invalid matcher, unresolved design-time Page Type ambiguity, missing transition endpoint, invalid extraction definition hook, incompatible shared Dataset mapping hook, invalid unique-key hook, absent mandatory guardrails, invalid domain scope, invalid canonicalization, invalid budgets.

Warnings include: untested Page Type, transition with no Test Evidence, low selector coverage evidence, broad matcher, rapid-growth preview. Assert warnings alone still permit publish.

- [ ] **Step 2: Write failing structural snapshot-health tests**

```rust
#[test]
fn unresolved_page_type_ambiguity_prevents_complete_snapshot() {
    let health = erabi_crawler::SnapshotHealth::from_signals(
        erabi_crawler::test_support::signals_with_ambiguity()
    );
    assert!(matches!(health, erabi_crawler::SnapshotHealth::Incomplete(_)));
}
```

Also cover unexpected pagination truncation, required page failures, cancelled run, and later `SchemaDrift` signal hook.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-crawler --test publish_validation --test snapshot_health
```

- [ ] **Step 4: Implement validation/classification and wire publication**

Publish endpoint runs validation first. If errors non-empty, return 422 stable validation envelope and do not call repository publication. If only warnings, return warnings to caller and require explicit publish action payload acknowledging the presented version/hash—not a generic warning override mechanism. Repository then atomically publishes and audits.

`SnapshotHealth` does not itself create missing candidates; it only supplies a trustworthy complete/incomplete decision to Plan 07 change detection after Plan 06 execution health and Plan 07 extraction health are combined.

- [ ] **Step 5: Run full Plan 05 gate and commit**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p erabi-crawler
cargo test -p erabi-api --test crawler_version_flow --test page_type_drafts --test test_lab --test discovery_preview
```

Expected: multi-seed/multi-Page-Type Draft → Test Lab → Publish passes; complete specificity tie remains ambiguous across input orderings; bounded cyclic preview terminates; external URL remains preserved/outside scope.

```bash
git add crates/erabi-crawler crates/erabi-api crates/erabi-db
 git commit -m "feat(studio): validate publication and snapshot structure"
```

## Plan 05 Gate

Do not start Plan 06 until Task 6 Step 5 passes from a clean checkout and `git status --short` is empty.
