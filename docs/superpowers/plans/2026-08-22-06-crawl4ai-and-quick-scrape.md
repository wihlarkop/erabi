# Erabi Crawl4AI and Quick Scrape Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate unmodified Crawl4AI behind a stable adapter and implement safe Quick Scrape, bounded batch submission, direct-file intake, robots/rate policy, and production crawl orchestration.

**Architecture:** Domain code depends on `CrawlerAdapter`, not Crawl4AI DTOs. Source intake decides non-HTML direct-file handling before HTML extraction when safe. Each accepted batch URL becomes an independent Quick Scrape run.

**Tech Stack:** Reqwest/Rustls, Crawl4AI HTTP API, Rust, Tokio, Axum, deterministic fixtures.

**Spec:** `docs/specs/01-product-and-experience.md`, `03-discovery-graph-and-runs.md`, `06-security-reliability-and-operations.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

### Task 1: CrawlerAdapter and deterministic mock

- [ ] Define health, crawl, cancel contracts with crawler-neutral request/output types.
- [ ] Mock never accesses network and returns deterministic success/timeout/access-denied/not-found/partial fixtures.
- [ ] Crawl4AI crate owns upstream DTO/path mapping and optional token; no token in Debug/logs.

### Task 2: Source intake and direct-file handling

- [ ] Create/reuse Source from original+canonical URL without mutating Crawler Seeds.
- [ ] Bounded content-type probe when safe; direct PDF/CSV/JSON/archive/image/office content becomes `FileAsset` intake.
- [ ] Non-HTML path shows metadata/download action and never enters HTML preview/extraction.
- [ ] Ambiguous probe falls through to crawl then classifies final content type.

### Task 3: Quick Scrape one URL and pasted batch

- [ ] `QUICK_SCRAPE` works without CrawlerVersion and freezes ad-hoc settings at creation.
- [ ] Batch endpoint preserves input order and returns per-item accepted/validation/conflict outcome.
- [ ] Every accepted URL creates separate Source association, `QUICK_SCRAPE` run, root job, snapshot, events URL.
- [ ] One failure never rolls back unrelated accepted items; no `BATCH` run type.
- [ ] Keep CSV/JSONL upload, sitemap, RSS outside MVP.

### Task 4: Robots, User-Agent, and rate limiting

- [ ] Respect robots by default; parse relevant Allow/Disallow/User-agent/Crawl-delay semantics.
- [ ] Override requires non-empty reason already validated by API and stores decision in snapshot/audit.
- [ ] Per-domain limiter mandatory; honor `Retry-After` on 429 with conservative backoff.
- [ ] Transparent configurable User-Agent recorded in run snapshot.

### Task 5: Production crawl orchestration

- [ ] Production requires Published CrawlerVersion; Test/Discovery Draft rules remain separate.
- [ ] Discovery pipeline order: resolve → validate → canonicalize → scope → dedupe → PageType match → transition → budgets → enqueue/preserve.
- [ ] Persist raw/cleaned/rendered/Markdown/screenshot/link evidence atomically.
- [ ] Complete snapshot flag only after all required structural/extraction health conditions pass.

**Gate:** first-run Quick Scrape works; ordered pasted batch creates independent runs; direct file bypasses HTML extraction; robots reason is required; 429 behavior and real/mock Crawl4AI contracts pass.
