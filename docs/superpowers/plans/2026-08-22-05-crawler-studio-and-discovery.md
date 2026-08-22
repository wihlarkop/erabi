# Erabi Crawler Studio and Discovery Implementation Plan

> **For agentic workers:** Implement each Crawler Studio/discovery capability as a complete scoped feature, then build/check it, add or update meaningful tests, run verification, and commit. Do not use failing-test-first or RED/GREEN sequencing by default.

**Goal:** Implement Crawler/CrawlerVersion authoring, Page Types, deterministic matching, transitions, canonicalization/domain scope, Test Lab, Discovery Preview, publication, and complete-snapshot structural gates.

**Architecture:** All crawling/extraction semantics are versioned inside CrawlerVersion. Drafts are editable/testable; Published versions are immutable production inputs. Publish validation owns core Crawler semantics and exposes contributor hooks for extraction/Dataset validation added by Plan 07.

**Tech Stack:** Rust domain/services, Axum APIs, Turso repositories, deterministic local fixtures.

**Spec:** `docs/specs/02-crawler-studio-domain.md`, `docs/specs/03-discovery-graph-and-runs.md`, `docs/specs/08-ux-accessibility-and-verification.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

---

### Task 1: Crawler and Draft/Published authoring APIs

**Files:** crawler service/API DTO/routes, repository integration, tests.

**Requirements:**

- Create/list/read Crawlers and their versions.
- Create a Draft from any prior Published version by copying semantic configuration into a new version identity.
- At most one ordinary active Draft.
- Publish creates a new immutable Published version, audit event, config hash, warning summary, parent/base reference, and active-version pointer.
- Direct edits to Published versions return conflict.
- Reactivating an older Published version changes the active pointer only; historical object remains immutable.

**Verification:** lifecycle/API/repository tests covering Draft creation, immutable Published behavior, reactivation, audit/config-hash persistence, and historical references.

---

### Task 2: Page Type authoring and deterministic match service

**Files:** Draft PageType/URLMatcher services/routes plus match explanation tests.

**Requirements:**

- CRUD PageTypes/URLMatchers only inside Draft semantic configuration.
- Reuse Plan 01 canonical resolution algorithm exactly.
- Return candidate rationale with explicit PageType priority, matcher kind, literal segments, query constraints, literal chars, wildcard/capture count.
- Complete tie returns `AMBIGUOUS_PAGE_TYPE` with all tied candidates.
- Never use row/insertion/map/UUID order as a hidden winner.

**Verification:** reverse insertion and fixture DB row order and prove identical winner/ambiguity. Test unmatched and invalid matcher syntax.

---

### Task 3: Canonicalization, Domain Scope, transitions, and budgets

**Files:** canonicalization/scope/discovery policy services and tests.

**Canonicalization:** parse/validate → lowercase scheme/host → normalize default port → remove fragment → consistent path/trailing slash → sort query params → remove known tracking params → apply explicit keep/drop rules. Preserve both original and canonical URLs. Do not broadly drop unknown query semantics.

**Domain scope:** default seed domains only; support same registrable domain + explicit subdomains, allowlist, and custom allow/block policy as specified. Out-of-scope URLs are preserved as `EXTERNAL` but not crawled.

**Transitions/guardrails:** directed PageType→PageType transitions, valid cycles, per-page link limits, total transition budget, crawler max pages/depth/duration/storage, PageType budgets/health thresholds.

**Verification:** canonicalization identity/dedupe, external URL preservation, tracking-param duplicate prevention, bounded cycle, transition budget, and scope policy tests.

---

### Task 4: Test Lab and durable TestEvidence

**Files:** Test Lab service/API, TestEvidence repository, local fixtures/tests.

**Requirements:**

For focused Draft tests report/persist:

- exact Draft Version/config hash;
- input URL;
- canonicalization decisions;
- PageType candidates/specificity/rationale;
- extraction hook result/coverage where available;
- pagination/transition/discovered URL preview;
- warnings/errors;
- artifact references;
- execution timestamp.

Use deterministic provider/fixture ports so Plan 05 does not require the real Crawl4AI HTTP adapter yet. Compare relevant Draft behavior against active Published version without mutating either.

**Verification:** deterministic evidence persistence/compare behavior and ambiguity rationale tests.

---

### Task 5: Discovery Preview

**Files:** discovery preview service/API and fixture tests.

**Requirements:**

- Operate on selected Draft seeds with explicit page/depth/time/transition budgets.
- Report canonical uniques, duplicates, PageType distribution, ambiguity, unmatched, external/blocked URLs, transition counts, robots exclusions, budget hits, and suspicious-growth warnings.
- Preserve discovery paths/provenance.
- Preview is never a complete production snapshot and never creates missing-record semantics.

**Verification:** multi-seed bounded preview, cyclic graph, external scope, canonical duplicate, unmatched/ambiguity, and budget exhaustion tests.

---

### Task 6: Publish validator and complete-snapshot structural gate

**Files:** publication validator/service plus tests.

**Interface:** expose a stable `VersionValidationContributor` contract so later semantic modules can add blockers/warnings without circular ownership. Plan 07 will register `ExtractionValidationContributor` for extraction, unique-key, and Dataset compatibility rules.

**Core publish blockers owned here:**

- no enabled seed;
- invalid matcher syntax/design-time unresolved ambiguity;
- transition references missing Page Types;
- invalid canonicalization/domain scope;
- missing/invalid mandatory guardrails/budgets.

Warnings remain visible/non-blocking. Later contributors add extraction/identity/Dataset blockers before final publish.

Complete production snapshot structural health requires no unresolved PageType ambiguity and accepts extraction-health input from Plan 07; production-breaking schema drift must make the snapshot non-complete.

**Verification:** publish blocker/warning tests, contributor aggregation tests, and structural complete-snapshot tests.

---

## Plan 05 Gate

```bash
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Confirm multi-seed/multi-PageType Draft → Test Lab → Publish works for available core contracts; deterministic ties remain ambiguous independent of ordering; canonicalization/scope are stable; cyclic Discovery Preview is bounded; external URLs stay outside crawl scope; publication validator exposes the contributor handoff required by Plan 07. Do not begin Plan 06 until the gate passes.
