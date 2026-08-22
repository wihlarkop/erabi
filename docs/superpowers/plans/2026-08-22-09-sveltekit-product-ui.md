# Erabi SvelteKit Product UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the complete accessible SvelteKit Crawler Studio UI matching the canonical navigation, Start/Quick Scrape flow, Crawler Studio, Test Lab, Discovery Preview, review/provenance, operations, and settings contracts.

**Architecture:** Static SvelteKit SPA talks only to `/api/v1` and authenticated fetch-based SSE. Feature modules share typed API/error/state primitives while keeping crawler authoring, runs, review, and operations isolated.

**Tech Stack:** SvelteKit, Svelte, TypeScript, Bun, REST, fetch streaming SSE, Playwright/component testing.

**Spec:** `docs/specs/01-product-and-experience.md`, `08-ux-accessibility-and-verification.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

### Task 1: App shell and Start

- [ ] Navigation exactly: Start, Crawlers, Collections, Runs, Datasets, Assets, Exports, Settings; no global Schemas/Inbox primary item.
- [ ] Start foregrounds one URL; optional bounded pasted batch preserves per-item ordered outcomes.
- [ ] Direct-file result routes to Asset handling, not extraction review.
- [ ] Recent activity remains secondary; first-run checklist non-blocking.

### Task 2: Run progress and recovery

- [ ] Fetch-stream SSE supports bearer header, replay from last ID, dedupe, terminal stop.
- [ ] Friendly steps distinct from expandable technical logs.
- [ ] Cancel, Retry Failed Parts, Resume, Rerun Full Crawl and partial-result messaging.
- [ ] Batch envelope links each accepted item to its own Quick Scrape run.

### Task 3: Crawler Overview and Studio

- [ ] Overview makes Published vs Draft unambiguous and surfaces health/test evidence/latest production run.
- [ ] Studio tabs: Graph, Seeds, Page Types, Discovery, Extraction, Test Lab; accessible list/table equivalent for graph.
- [ ] Page Type matcher editor exposes priority and specificity rationale; ambiguity tie is visible.
- [ ] Publish gate shows blocking errors versus warnings.

### Task 4: Test Lab and Discovery Preview

- [ ] Test Lab shows exact Draft/config hash, URL, canonicalization, matched candidates/specificity, extraction, coverage, transition result, warnings/errors, TestEvidence.
- [ ] Discovery Preview shows paths/table, PageType distribution, duplicates, unmatched/ambiguous/external/blocked, budgets, growth warnings.
- [ ] No preview/test surface labels itself production complete.

### Task 5: Extraction and Review UX

- [ ] Three-panel Preview ↔ Field ↔ Record workflow with keyboard DOM/manual selector alternative.
- [ ] Grid default/Card optional, inline Draft edit, validation, conflicts, provenance drawer, bulk approve/reject, Close/Reopen.
- [ ] `SCHEMA_DRIFT` production error routes to new Draft fix and never offers trust-restoring `USE_ANYWAY`.

### Task 6: Settings, security, accessibility

- [ ] Inheritable setting control explicitly offers Inherit / Custom / Reset to built-in and displays effective source.
- [ ] Robots override cannot submit without reason; active override context prominent.
- [ ] Token stored session by default, local only after explicit Remember; never URL/settings/log.
- [ ] English translation keys, system/light/dark, WCAG 2.2 AA, visible focus, reduced motion, no color-only state, 200% zoom.

**Gate:** component/accessibility tests cover all primary routes, settings tri-state, robots reason, ambiguity, SSE reconnect, extraction keyboard workflow, review validation, and Recovery Mode restrictions.
