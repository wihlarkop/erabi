# Erabi Crawl4AI Integration and Crawl Orchestration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Define crawler-neutral contracts, integrate the unmodified official Crawl4AI service, create Sources and Crawl Runs, enforce safe crawling defaults, orchestrate page/batch/pagination work, and persist raw crawl evidence.

**Architecture:** Erabi never imports or modifies Crawl4AI internals; a thin HTTP adapter maps stable Erabi crawl requests and outputs. Durable jobs coordinate safe policies, robots decisions, rate limits, pagination confirmation, partial results, retries, resumable checkpoints, and artifact persistence.

**Tech Stack:** Rust, Reqwest with Rustls, Axum, Tokio, Crawl4AI HTTP API, Turso jobs, filesystem artifacts.

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

- **Depends on:** [04 Durable Jobs and SSE Progress](./04-durable-jobs-and-sse.md).
- **Produces:** `CrawlerAdapter`, mock and Crawl4AI adapters, Source/Crawl APIs, safe crawl policy engine, pagination/retry/resume orchestration, and raw crawl snapshots.
- **Gate:** Crawling gate: deterministic adapter contract tests and API integration tests cover successful, denied, partial, cancelled, retried, resumed, and paginated crawls.
- **Execution order:** Complete every task in this file in numerical order and commit after each task. Do not begin the next plan until this gate passes.

## Focused File Map

```text
crates/erabi-crawler/
crates/erabi-crawl4ai/
crates/erabi-jobs/src/crawl/
crates/erabi-api/src/routes/sources.rs
crates/erabi-api/src/routes/crawl_runs.rs
crates/erabi-db/src/repositories/sources/
crates/erabi-db/src/repositories/crawl_runs/
tests/fixtures/crawl4ai/
tests/integration/crawling/
```

## Shared Contract Produced by This Plan

```rust
#[async_trait::async_trait]
pub trait CrawlerAdapter: Send + Sync {
    async fn health_check(&self) -> Result<CrawlerHealth, CrawlerError>;
    async fn crawl(&self, request: CrawlRequest) -> Result<CrawlOutput, CrawlerError>;
    async fn cancel(&self, external_job_id: &str) -> Result<(), CrawlerError>;
}
```

---

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
