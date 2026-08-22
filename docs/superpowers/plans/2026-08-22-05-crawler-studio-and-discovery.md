# Erabi Crawler Studio and Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Crawler/CrawlerVersion authoring, Page Types, deterministic matching, transitions, canonicalization/domain scope, Test Lab, Discovery Preview, publication, and complete-snapshot structural gates.

**Architecture:** All crawling/extraction semantics are versioned inside CrawlerVersion. Drafts are editable/testable; Published versions are immutable production inputs.

**Tech Stack:** Rust domain/services, Axum APIs, Turso repositories, deterministic fixture tests.

**Spec:** `docs/specs/02-crawler-studio-domain.md`, `03-discovery-graph-and-runs.md`, `08-ux-accessibility-and-verification.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

### Task 1: Crawler and Draft/Published APIs

- [ ] Create/list/read Crawlers and Drafts; creating from prior Published copies semantic config into a new Draft identity.
- [ ] Publish validates then writes immutable Published version + audit event + active-version pointer.
- [ ] Editing Published directly returns conflict; reactivating older version changes pointer only.

### Task 2: Page Types and deterministic match service

- [ ] CRUD PageTypes/URLMatchers inside Draft only.
- [ ] Test priority and full specificity tuple exactly as public spec defines.
- [ ] Reverse insertion/DB row order in fixtures and prove identical result.
- [ ] Complete tie returns `AMBIGUOUS_PAGE_TYPE` with candidate rationale.

### Task 3: Canonicalization, scope, transitions, and budgets

- [ ] Canonicalize scheme/host/default port/fragment/path/query/tracking parameters without broadly dropping unknown query semantics.
- [ ] Domain scope defaults seed domains only; external URLs preserved but not crawled.
- [ ] Implement directed transitions, cycles, per-page links, total transition budgets, crawler page/depth/duration/storage guardrails.

### Task 4: Test Lab and durable TestEvidence

- [ ] Test canonicalization, Page Type matching, extraction hook, selector coverage, pagination detection, one transition, discovered URL preview.
- [ ] Persist config hash, inputs, candidates/rationale, extraction/discovery summaries, warnings/errors, artifacts, time.
- [ ] Compare Draft behavior against active Published version without mutating either.

### Task 5: Discovery Preview

- [ ] Execute bounded draft discovery with selected seeds, page/depth/time/transition budgets.
- [ ] Report canonical uniques, duplicates, PageType distribution, ambiguity, unmatched, external/blocked, transition counts, robots exclusions, budget hits, growth warnings.
- [ ] Never classify preview as complete production snapshot.

### Task 6: Publish and complete-snapshot structural gates

- [ ] Block publish on invalid seeds/matchers/ambiguity known at design time/transitions/extraction/unique-key/domain-scope/canonicalization/budgets.
- [ ] Warnings remain non-blocking and visible.
- [ ] Complete production snapshot requires no unresolved Page Type ambiguity or production-breaking schema drift.

**Gate:** multi-seed/multi-page-type Draft → Test Lab → Publish passes; deterministic tie stays ambiguous; cyclic Discovery Preview remains bounded; external URL remains outside scope.
