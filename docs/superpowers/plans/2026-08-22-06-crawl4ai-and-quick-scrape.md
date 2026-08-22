# Erabi Crawl4AI and Quick Scrape Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate unmodified Crawl4AI behind a stable Erabi adapter and implement safe Source intake, direct-file routing, single/batch Quick Scrape, robots/rate policy, and production crawl orchestration with durable evidence.

**Architecture:** `erabi-crawler` owns crawler-neutral request/output contracts and orchestration; `erabi-crawl4ai` owns all upstream HTTP DTO/path mapping. Source intake classifies confident non-HTML direct files before HTML extraction. Every accepted batch URL creates an independent `QUICK_SCRAPE` run and root durable job.

**Tech Stack:** stable Rust, Reqwest/Rustls, Tokio, Axum, Crawl4AI official HTTP service, wiremock/local fixtures, Turso repositories, filesystem ArtifactStore from Plan 02.

**Spec:** `docs/specs/01-product-and-experience.md`, `docs/specs/03-discovery-graph-and-runs.md`, `docs/specs/05-system-architecture-and-persistence.md`, `docs/specs/06-security-reliability-and-operations.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Global Constraints

- Erabi does not fork, modify, or import Crawl4AI internals.
- Domain/orchestration code depends only on `CrawlerAdapter`.
- Quick Scrape works without a Crawler/Crawler Version.
- Batch submission is an envelope over independent Quick Scrapes; no fifth `BATCH` run type exists.
- Direct non-HTML files use Source/Asset intake and never enter HTML preview/extraction.
- Robots is respected by default; override requires the validated non-empty reason from Plan 03 and freezes it in the Plan 02 snapshot.
- Per-domain rate limiting is mandatory; 429 honors `Retry-After` and uses conservative backoff.
- Crawl Run configuration is frozen before a root job is queued.
- Raw/cleaned/rendered/Markdown/screenshot/link evidence is persisted atomically.
- Only a healthy complete Production Run can later create missing-record candidates.

## Focused File Map

```text
migrations/0005_crawl_execution.sql
crates/erabi-crawler/src/adapter.rs
crates/erabi-crawler/src/model.rs
crates/erabi-crawler/src/mock.rs
crates/erabi-crawler/src/source_intake.rs
crates/erabi-crawler/src/robots.rs
crates/erabi-crawler/src/rate_limit.rs
crates/erabi-crawler/src/orchestrator.rs
crates/erabi-crawl4ai/src/client.rs
crates/erabi-crawl4ai/src/dto.rs
crates/erabi-crawl4ai/src/mapper.rs
crates/erabi-api/src/routes/quick_scrape.rs
crates/erabi-jobs/src/handlers/crawl.rs
tests/fixtures/crawl4ai/
```

---

### Task 1: Define crawler-neutral adapter contracts, deterministic mock, and Crawl4AI HTTP adapter

**Files:**
- Create: `crates/erabi-crawler/src/model.rs`
- Create: `crates/erabi-crawler/src/adapter.rs`
- Create: `crates/erabi-crawler/src/mock.rs`
- Modify: `crates/erabi-crawler/src/lib.rs`
- Create: `crates/erabi-crawl4ai/src/client.rs`
- Create: `crates/erabi-crawl4ai/src/dto.rs`
- Create: `crates/erabi-crawl4ai/src/mapper.rs`
- Modify: `crates/erabi-crawl4ai/src/lib.rs`
- Create: `tests/fixtures/crawl4ai/single-page-success.json`
- Create: `tests/fixtures/crawl4ai/partial-result.json`
- Test: `crates/erabi-crawler/tests/adapter_contract.rs`
- Test: `crates/erabi-crawl4ai/tests/http_contract.rs`

**Interfaces:**
- Produces `CrawlerAdapter`, `CrawlRequest`, `CrawlOutput`, `CrawledPage`, `CrawlerHealth`, `CrawlerError`.
- Produces `MockCrawlerAdapter`.
- Produces `Crawl4AiAdapter` isolated behind `CrawlerAdapter`.

- [ ] **Step 1: Add stable adapter dependencies**

```bash
cargo add -p erabi-crawler async-trait
cargo add -p erabi-crawler serde --features derive
cargo add -p erabi-crawler serde_json
cargo add -p erabi-crawler bytes
cargo add -p erabi-crawler thiserror
cargo add -p erabi-crawler url --features serde
cargo add -p erabi-crawler --path crates/erabi-domain erabi-domain
cargo add -p erabi-crawl4ai reqwest --features json,rustls-tls,stream
cargo add -p erabi-crawl4ai serde --features derive
cargo add -p erabi-crawl4ai serde_json
cargo add -p erabi-crawl4ai async-trait
cargo add -p erabi-crawl4ai thiserror
cargo add -p erabi-crawl4ai tracing
cargo add -p erabi-crawl4ai --path crates/erabi-crawler erabi-crawler
cargo add -p erabi-crawl4ai --dev wiremock
cargo add -p erabi-crawl4ai --dev tokio --features macros,rt-multi-thread
```

- [ ] **Step 2: Write failing adapter contract tests**

```rust
#[tokio::test]
async fn mock_success_returns_normalized_page_evidence() {
    let adapter = erabi_crawler::MockCrawlerAdapter::from_fixture("single-page-success");
    let output = adapter.crawl(erabi_crawler::test_support::request("https://example.test/")).await.unwrap();
    let page = &output.pages[0];
    assert_eq!(page.final_url.as_str(), "https://example.test/");
    assert!(page.raw_html.is_some());
    assert!(page.rendered_dom.is_some());
    assert!(!page.discovered_links.is_empty());
}

#[tokio::test]
async fn mock_timeout_is_deterministic_and_network_free() {
    let adapter = erabi_crawler::MockCrawlerAdapter::timeout();
    assert!(matches!(adapter.crawl(erabi_crawler::test_support::request("https://example.test/")).await, Err(erabi_crawler::CrawlerError::Timeout)));
}
```

Contract fixtures cover success, timeout, access denied, target not found, malformed/invalid upstream response, and partial result.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-crawler --test adapter_contract
```

- [ ] **Step 4: Implement crawler-neutral types and mock**

```rust
#[async_trait::async_trait]
pub trait CrawlerAdapter: Send + Sync {
    async fn health_check(&self) -> Result<CrawlerHealth, CrawlerError>;
    async fn crawl(&self, request: CrawlRequest) -> Result<CrawlOutput, CrawlerError>;
    async fn cancel(&self, external_job_id: &str) -> Result<(), CrawlerError>;
}

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

`CrawledPage` contains final URL, status code, content type, raw HTML, cleaned HTML, rendered DOM, Markdown, optional screenshot bytes, discovered links, canonical hint, title, OG title, timing, and opaque adapter metadata JSON. Upstream DTO types are never exported from `erabi-crawl4ai`.

Mock holds a queue of predetermined outputs/errors, records received requests, and never performs network I/O.

- [ ] **Step 5: Write failing HTTP mapping tests against wiremock**

Before implementing defaults, inspect the official Crawl4AI API exposed by the target stable Docker image/documentation and record the verified health/crawl/cancel endpoint paths in one `Crawl4AiEndpointPaths` struct. Tests mock those paths and cover request mapping for timeout/wait/scroll/screenshot/User-Agent, success mapping, auth header, 401/403 → AccessDenied, target 404 → NotFound, network timeout → Timeout, malformed body → InvalidResponse. Secret tokens must not appear in `Debug` output.

- [ ] **Step 6: Implement Crawl4AI client/DTO/mapper in isolation**

`Crawl4AiConfig` contains base URL, optional secret token, and endpoint paths. DTO modules mirror only upstream fields Erabi consumes and use `Option`/`#[serde(default)]` for optional upstream fields. Unknown adapter metadata may be retained only in the opaque JSON field.

Cancellation semantics: call verified upstream cancellation endpoint when available; otherwise cancel local request/best-effort work and emit a technical warning without claiming remote cancellation succeeded.

- [ ] **Step 7: Run GREEN and commit**

```bash
cargo test -p erabi-crawler --test adapter_contract
cargo test -p erabi-crawl4ai --test http_contract
cargo clippy -p erabi-crawler -p erabi-crawl4ai --all-targets -- -D warnings
git add Cargo.lock crates/erabi-crawler crates/erabi-crawl4ai tests/fixtures/crawl4ai
 git commit -m "feat(crawler): add isolated Crawl4AI adapter"
```

---

### Task 2: Implement Source intake, duplicate identity, and direct-file routing

**Files:**
- Create: `crates/erabi-crawler/src/source_intake.rs`
- Extend: `crates/erabi-db/src/repositories/sources.rs`
- Create: `crates/erabi-api/src/dto/source_intake.rs`
- Test: `crates/erabi-crawler/tests/source_intake.rs`
- Test: `crates/erabi-api/tests/source_intake.rs`

**Interfaces:**
- Produces `ContentTypeProbe`, `ProbeResult`, `SourceIntakeDecision`.
- Produces canonical duplicate lookup and explicit duplicate outcomes.

- [ ] **Step 1: Write failing direct-file routing tests**

```rust
#[tokio::test]
async fn confident_pdf_probe_routes_to_file_asset_without_crawl() {
    let probe = erabi_crawler::test_support::probe("application/pdf");
    let decision = erabi_crawler::SourceIntakeService::new(probe)
        .classify("https://example.test/report.pdf").await.unwrap();
    assert!(matches!(decision, erabi_crawler::SourceIntakeDecision::FileAsset { .. }));
}

#[tokio::test]
async fn ambiguous_probe_falls_back_to_html_crawl_path() {
    let probe = erabi_crawler::test_support::ambiguous_probe();
    let decision = erabi_crawler::SourceIntakeService::new(probe)
        .classify("https://example.test/download?id=7").await.unwrap();
    assert!(matches!(decision, erabi_crawler::SourceIntakeDecision::NeedsCrawlerClassification { .. }));
}
```

Test direct PDF, CSV, JSON, ZIP/archive, image, and common office MIME categories. Do not parse these files into records in MVP.

- [ ] **Step 2: Write duplicate URL repository tests**

Canonical duplicate lookup returns active/archived/trashed matches with Collection/status rather than silently creating a copy. Creating anyway must be an explicit caller decision. Source state changes never mutate Crawler Seeds.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-crawler --test source_intake
cargo test -p erabi-api --test source_intake
```

- [ ] **Step 4: Implement bounded safe probe and Source intake**

`ContentTypeProbe` performs a bounded request strategy suitable for the selected HTTP stack: prefer HEAD when reliable, optionally use a small/range-limited GET when HEAD is unsupported/ambiguous, enforce timeout/body-byte cap, and never download an unbounded body merely to classify content. Treat MIME/signature as stronger than filename extension when available.

`FileAsset` intake creates/reuses Source with `SourceTargetType::FileAsset`, preserves original/canonical URL and detected content type, and returns a user action to download through Plan 08 asset service. It does not enqueue HTML extraction. Ambiguous probe continues to crawler classification and final response content type decides.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-crawler --test source_intake
cargo test -p erabi-api --test source_intake
git add crates/erabi-crawler crates/erabi-api crates/erabi-db
 git commit -m "feat(sources): route direct files safely"
```

---

### Task 3: Implement single Quick Scrape and bounded pasted URL batch

**Files:**
- Create: `crates/erabi-api/src/routes/quick_scrape.rs`
- Create: `crates/erabi-api/src/dto/quick_scrape.rs`
- Modify: `crates/erabi-api/src/app.rs`
- Extend: `crates/erabi-db/src/repositories/runs.rs`
- Extend: `crates/erabi-db/src/repositories/jobs.rs`
- Test: `crates/erabi-api/tests/quick_scrape.rs`
- Test: `crates/erabi-api/tests/quick_scrape_batch.rs`

**Interfaces:**
- Produces `POST /api/v1/quick-scrapes`.
- Produces `POST /api/v1/quick-scrape-batches`.
- Produces per-item `QuickScrapeSubmissionOutcome` preserving input order.

- [ ] **Step 1: Write failing single-run atomic creation test**

Assert request creates Source association, `QUICK_SCRAPE` Crawl Run, immutable Plan 02 snapshot, root `CRAWL_PAGE` job, audit/progress queued event, and returns run ID/events URL. Simulate transaction failure before commit and assert no partial run/job is visible.

- [ ] **Step 2: Write failing batch independence/order tests**

```rust
#[tokio::test]
async fn batch_preserves_input_order_and_creates_no_batch_run_type() {
    let fixture = erabi_api::test_support::app().await;
    let result = fixture.submit_batch(vec!["https://a.test/", "bad://", "https://b.test/"]).await;
    assert_eq!(result.items.len(), 3);
    assert!(result.items[0].run_id.is_some());
    assert!(result.items[1].validation_error.is_some());
    assert!(result.items[2].run_id.is_some());
    assert_ne!(result.items[0].run_id, result.items[2].run_id);
}
```

Assert failure/validation of one item does not roll back unrelated accepted items. Enforce configurable maximum URL count and request body size. CSV/JSONL upload, sitemap, RSS remain absent.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-api --test quick_scrape --test quick_scrape_batch
```

- [ ] **Step 4: Implement run creation service**

Single Quick Scrape flow:

```text
parse/canonicalize URL
→ Source intake/duplicate decision
→ resolve ordinary settings with no Crawler/RunProfile layers unless ad-hoc config explicitly supplies value
→ validate robots override decision if requested
→ build immutable QUICK_SCRAPE snapshot
→ transaction: create/reuse Source + CrawlRun QUEUED + root Job + audit/progress event
→ commit
→ publish live queued event
```

Batch endpoint validates each element independently and invokes the same single-run service per accepted item. Return an envelope only; never persist `BATCH` as a Crawl Run type or root semantic run.

Support `Idempotency-Key` on mutations so retried HTTP submissions return the same created run(s) for the same key/request hash.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-api --test quick_scrape --test quick_scrape_batch
git add crates/erabi-api crates/erabi-db
 git commit -m "feat(scrape): create independent Quick Scrape runs"
```

---

### Task 4: Implement robots policy, transparent User-Agent, and per-domain rate limiting

**Files:**
- Create: `crates/erabi-crawler/src/robots.rs`
- Create: `crates/erabi-crawler/src/rate_limit.rs`
- Create: `crates/erabi-crawler/src/safety.rs`
- Test: `crates/erabi-crawler/tests/robots_policy.rs`
- Test: `crates/erabi-crawler/tests/rate_limit.rs`

**Interfaces:**
- Produces `RobotsPolicy`, `RobotsDecisionResult`, `DomainRateLimiter`, `RetryAfter` parser.

- [ ] **Step 1: Write robots parser/policy tests**

Fixtures cover exact and wildcard User-agent groups, Allow/Disallow, longest matching path precedence, equal-specificity Allow preference, comments/blank lines, optional Crawl-delay, missing/404 robots allowed by default, disallow blocked by default, explicit frozen override permitted. Use the configured transparent Erabi User-Agent for matching.

- [ ] **Step 2: Write rate/429 tests using paused Tokio time**

Assert two requests to same domain obey configured delay; different domains do not share one global delay; per-domain concurrency cap is enforced. Parse both delta-seconds and HTTP-date `Retry-After` forms. A 429 with Retry-After schedules non-aggressive retry no earlier than requested time; without header use bounded exponential backoff with jitter source injectable/deterministic in tests.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-crawler --test robots_policy --test rate_limit
```

- [ ] **Step 4: Implement safety services**

Implement only robots semantics required by spec; do not add sitemap ingestion. Cache robots results per origin for a bounded configurable duration. Override service consumes already-validated `RobotsDecision::Override` from snapshot; it cannot invent/recover a reason itself.

Default User-Agent is transparent and versioned (for example `Erabi/<version> (+project-url)` once project URL is established); user override is recorded in snapshot/audit and UI later warns against misleading impersonation.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-crawler --test robots_policy --test rate_limit
git add crates/erabi-crawler
 git commit -m "feat(crawler): enforce robots and domain rate limits"
```

---

### Task 5: Implement crawl execution persistence and durable Quick/Production handlers

**Files:**
- Create: `migrations/0005_crawl_execution.sql`
- Create: `crates/erabi-crawler/src/orchestrator.rs`
- Create: `crates/erabi-jobs/src/handlers/crawl.rs`
- Create: `crates/erabi-jobs/src/handlers/mod.rs`
- Extend: `crates/erabi-db/src/repositories/runs.rs`
- Modify: `crates/erabi-cli/src/runtime.rs`
- Test: `crates/erabi-jobs/tests/crawl_handler.rs`
- Test: `crates/erabi-db/tests/crawl_persistence.rs`

**Interfaces:**
- Produces persisted `CrawlPageResult`, `CrawlSummary`, execution-health signals.
- Registers `CRAWL_PAGE` and discovery/classification handlers in Plan 04 runtime.

- [ ] **Step 1: Add crawl execution migration and failing persistence test**

`0005_crawl_execution.sql` owns `crawl_pages` and `crawl_summaries` plus any required source naming-origin field/index. Do not duplicate jobs/progress tables from Plan 04.

Test successful page persistence links Run, Source, final URL, status/content type, artifact IDs, discovered-link count, timing, and result health. A failed atomic artifact write must not mark page complete.

- [ ] **Step 2: Write failing handler event-sequence tests using MockCrawlerAdapter**

Expected user event order for a normal page includes:

```text
crawl.started
crawl.robots_checked
page.loading
page.rendering
page.completed
artifact.saving
extraction.queued
```

A direct-file Source never enters this HTML handler. Access denied/not found/timeout map to stable Run/Source outcome without panicking. Cancellation persists checkpoint through Plan 04.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-db --test crawl_persistence
cargo test -p erabi-jobs --test crawl_handler
```

- [ ] **Step 4: Implement orchestration pipeline**

Quick/Production page handler:

```text
load immutable Run snapshot
→ verify run type/version rules
→ robots policy decision
→ acquire per-domain rate permit
→ call CrawlerAdapter
→ classify final content type (route non-HTML evidence away from extraction)
→ atomically persist page artifacts
→ persist page result/links
→ update discovery queue through Plan 05 engine when applicable
→ enqueue extraction hook for HTML page
→ publish durable progress
```

Production Run creation requires active Published Crawler Version. Test/Discovery execution uses separate Draft-aware start services from Plan 05. Production discovery follows Plan 05 canonical pipeline exactly.

Combine execution signals into `CrawlSummary`: page counts, errors by stable code, planned/completed counts, final URLs, User-Agent, robots decision, config hash, duration/storage summary, and provisional structural completeness. Plan 07 adds extraction/schema health before final trusted complete-snapshot classification.

- [ ] **Step 5: Run full Plan 06 gate and commit**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p erabi-crawler
cargo test -p erabi-crawl4ai
cargo test -p erabi-jobs --test crawl_handler
cargo test -p erabi-api --test quick_scrape --test quick_scrape_batch --test source_intake
```

Expected: first Quick Scrape queues/runs through mock; ordered batch creates independent runs; direct file bypasses HTML extraction; robots reason remains required/frozen; 429 behavior passes; Crawl4AI mapping tests are deterministic.

```bash
git add Cargo.lock migrations/0005_crawl_execution.sql crates/erabi-crawler crates/erabi-crawl4ai crates/erabi-jobs crates/erabi-db crates/erabi-cli
 git commit -m "feat(crawling): orchestrate durable crawl execution"
```

## Plan 06 Gate

Do not start Plan 07 until Task 5 Step 5 passes from a clean checkout and `git status --short` is empty.
