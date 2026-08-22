# Erabi Crawl4AI and Quick Scrape Implementation Plan

> **For agentic workers:** Implement each Crawl4AI/Quick Scrape capability end-to-end, then compile/check, add or update meaningful tests, run verification, and commit. Do not use failing-test-first or RED/GREEN sequencing by default.

**Goal:** Integrate unmodified Crawl4AI behind a stable adapter and implement safe Quick Scrape, bounded batch submission, direct-file intake, robots/rate policy, and production crawl orchestration.

**Architecture:** Domain code depends on `CrawlerAdapter`, not Crawl4AI DTOs. Source intake decides non-HTML direct-file handling before HTML extraction when safe. Each accepted pasted-batch URL becomes an independent Quick Scrape run.

**Tech Stack:** Reqwest/Rustls, Crawl4AI HTTP API, Rust, Tokio, Axum, deterministic local fixtures.

**Spec:** `docs/specs/01-product-and-experience.md`, `docs/specs/03-discovery-graph-and-runs.md`, `docs/specs/06-security-reliability-and-operations.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

**Migration ownership:** `migrations/0005_crawl_execution.sql` for crawl page results/summaries only.

---

### Task 1: Define CrawlerAdapter and deterministic mock

**Files:** adapter contracts in `erabi-crawler`, Crawl4AI implementation in `erabi-crawl4ai`, deterministic mock/tests.

**Interface capabilities:** health/version, crawl/execute request, normalized result metadata/artifacts/links, best-effort cancel, normalized Erabi errors.

**Requirements:**

- Domain/services never depend directly on upstream Crawl4AI DTO/path names.
- Mock never accesses network and provides deterministic success/timeout/access-denied/not-found/partial fixtures.
- `erabi-crawl4ai` owns upstream HTTP mapping and bundled/external endpoint/token details.
- Tokens are redacted from Debug/logs/errors.
- Crawl4AI outage maps to stable `CRAWLER_UNAVAILABLE`-style behavior and does not make existing Erabi data unavailable.

**Verification:** adapter contract tests against mock plus HTTP mapping fixture tests.

---

### Task 2: Source intake and direct-file handling

**Files:** Source intake service, content-type probe/download metadata modules, API tests.

**Requirements:**

- Create/reuse Source using original+canonical URL without mutating Crawler Seeds.
- Perform bounded/safe content-type probe where appropriate.
- Confident non-HTML PDF/CSV/JSON/archive/image/office-like responses become `FileAsset` intake.
- Direct non-HTML path records metadata and offers explicit safe download handling; never enters HTML preview/extraction.
- Never auto-execute/open/extract archives.
- Ambiguous probe falls through to normal crawl and classifies final response content type afterward.
- Apply filename/path/MIME/streaming safety from security spec.

**Verification:** direct PDF/CSV/etc fixtures, ambiguous content-type fallback, Source reuse, Seed independence, path/file safety tests.

---

### Task 3: Quick Scrape single URL and bounded pasted batch

**Files:** Quick Scrape service/routes/DTOs, batch envelope DTO, persistence integration, tests.

**Requirements:**

- `QUICK_SCRAPE` works without CrawlerVersion and freezes ad-hoc settings in immutable run snapshot.
- One URL remains default Start flow.
- Bounded pasted batch preserves input order and returns per-item accepted/validation/conflict outcome.
- Every accepted URL creates/reuses Source association and creates an independent `QUICK_SCRAPE` run, root job, snapshot, artifacts/progress/review identity.
- One item failure does not roll back unrelated accepted items.
- There is no `BATCH` CrawlRunType.
- CSV/JSONL upload, sitemap, RSS bulk inputs remain outside MVP.

**Verification:** one-URL flow, mixed valid/invalid ordered batch, independent failures, independent cancel/retry IDs, exact run-type invariant.

---

### Task 4: Robots, User-Agent, and rate limiting

**Files:** robots policy/cache, per-domain limiter/backoff modules, orchestration integration/tests.

**Requirements:**

- Respect robots by default and support relevant User-agent/Allow/Disallow/Crawl-delay semantics needed by MVP.
- Override is accepted only through already validated Plan 03 decision with non-empty reason; snapshot/audit preserve it.
- Per-domain limiter is mandatory.
- Honor `Retry-After` on 429 with conservative bounded backoff.
- User-Agent is transparent/configurable and frozen in run snapshot.
- No hidden high-concurrency bypass for Quick Scrape/batch.

**Verification:** robots allow/disallow/delay, override/no-reason rejection via API integration, 429 Retry-After, per-domain concurrency/rate behavior.

---

### Task 5: Production crawl orchestration and snapshot health

**Files:** crawl execution pipeline, `migrations/0005_crawl_execution.sql`, page-result repositories, snapshot-health integration, tests.

**Requirements:**

- Normal Production Run requires Published CrawlerVersion; Test/Discovery Draft rules stay separate.
- Pipeline order is explicit:

```text
resolve discovered URL
→ validate/parse
→ canonicalize
→ domain scope
→ dedupe
→ PageType match
→ transition eligibility
→ budgets/guardrails
→ enqueue/preserve
→ crawl/render
→ persist evidence
```

- Persist raw/cleaned/rendered/Markdown/screenshot/link evidence atomically according to configured policy.
- Preserve unmatched/external/blocked/disallowed evidence/status where required.
- Complete snapshot flag only after structural and available extraction-health conditions pass; Plan 07 later plugs production schema-drift health into the same gate.
- Cancellation/checkpoint/progress use Plan 04 services.

**Verification:** Published-only production, bounded discovery, artifact persistence, cancel/resume integration, partial failure, structural complete-snapshot tests.

---

## Plan 06 Gate

```bash
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Confirm first-run Quick Scrape works, ordered pasted batch creates independent runs, direct files bypass HTML extraction, Source never mutates Seeds, robots reason/rate policy are enforced, Crawl4AI mock/HTTP contracts pass, and production crawl health never marks partial/ambiguous execution as complete. Do not begin Plan 07 until the gate passes.
