# Erabi CI, End-to-End, and Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Package Erabi with unmodified Crawl4AI, enforce deterministic CI/release gates, automate every canonical MVP journey, and publish operator/recovery documentation without claiming roadmap-only features.

**Architecture:** PR CI uses local deterministic website fixtures + mocked/stubbed Crawl4AI contract. Explicit/scheduled smoke tests use official Crawl4AI container against local fixture sites. Docker Compose is primary distribution.

**Tech Stack:** GitHub Actions, Docker/Compose, Playwright, Cargo/Bun test suites, official Crawl4AI image.

**Spec:** `docs/specs/08-ux-accessibility-and-verification.md`, `06-security-reliability-and-operations.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

### Task 1: Docker and deterministic fixtures

- [ ] Build `erabi` image/server+static UI and Compose with official unmodified Crawl4AI image.
- [ ] Loopback default; remote access-token configuration explicit.
- [ ] Fixture sites cover article, listing/detail, pagination cycle, ambiguity, external links, schema drift, direct file, 429, robots, malicious preview.

### Task 2: PR CI gates

- [ ] Frozen Cargo/Bun lock installs; fmt/clippy/Rust tests/frontend check/tests/Playwright.
- [ ] Migration up from supported baseline, backup/restore, adapter contract, documentation links/placeholders.
- [ ] Tests never depend on arbitrary public websites.

### Task 3: Required Playwright journeys

Automate every journey from public spec, including:

- [ ] first-run Start → Quick Scrape → Review;
- [ ] pasted batch → independent ordered Quick Scrapes;
- [ ] direct file → Source/Asset without HTML extraction;
- [ ] Quick Scrape → Save as Crawler Draft;
- [ ] multi-Seed/multi-Page-Type Draft → Test Lab → Publish;
- [ ] ambiguity blocks publish and complete specificity ties stay ambiguous regardless ordering;
- [ ] bounded cyclic Discovery Preview;
- [ ] external URL stays outside scope;
- [ ] canonicalization prevents tracking duplicates;
- [ ] Production Run → SSE → Cancel → recover/resume;
- [ ] robots override requires reason; retry/resume preserves same-run reason; new run requires explicit reason;
- [ ] Listing+Detail shared Dataset never silently overwrites;
- [ ] schema drift diagnostic blocks trusted complete/missing semantics and requires Draft fix;
- [ ] duplicate candidates never auto-merge;
- [ ] complete snapshot creates missing candidates; partial does not;
- [ ] approved provenance traces source/artifact/version;
- [ ] approved-only export + provenance bundle;
- [ ] backup → verify → restore;
- [ ] tri-state settings precedence/resolution;
- [ ] remote bind rejected without token;
- [ ] low-storage blocks artifact-heavy work without auto-delete.

### Task 4: Real Crawl4AI smoke and release candidate gate

- [ ] Run official container against deterministic fixtures for rendering, links, waits/scroll, screenshots, error mapping.
- [ ] Record image/version used for release candidate.
- [ ] No release called MVP-complete until real smoke passes.

### Task 5: Operator and release documentation

- [ ] Document installation, data directory, backup/restore, integrity/recovery, remote bind/token, Crawl4AI troubleshooting, storage pressure.
- [ ] Check all public doc links and archived-plan warnings.
- [ ] Release notes explicitly distinguish implemented MVP from roadmap-only capabilities.

**Gate:** all Rust/frontend/Playwright/migration/backup/fixture/real-Crawl4AI/documentation gates pass from clean checkout; no stale July plan is referenced as current.
