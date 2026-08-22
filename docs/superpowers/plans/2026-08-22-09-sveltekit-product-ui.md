# Erabi SvelteKit Product UI Implementation Plan

> **For agentic workers:** Implement each UI capability as a complete usable feature, then run type/build checks, add or update meaningful component/accessibility tests, verify behavior, and commit. Do not use failing-test-first or RED/GREEN sequencing by default.

**Goal:** Build the complete accessible SvelteKit Crawler Studio UI matching the canonical navigation, Start/Quick Scrape flow, Crawler Studio, Test Lab, Discovery Preview, review/provenance, operations, and settings contracts.

**Architecture:** Static-compatible SvelteKit SPA talks only to `/api/v1` and authenticated fetch-based SSE. Feature modules share typed API/error/state primitives while keeping crawler authoring, runs, review, and operations isolated. Domain truth remains server-side; the client does not recreate business semantics independently.

**Tech Stack:** SvelteKit, Svelte, TypeScript, Bun, REST, fetch-streaming SSE, component/accessibility testing, Playwright integration from Plan 10.

**Spec:** `docs/specs/01-product-and-experience.md`, `docs/specs/08-ux-accessibility-and-verification.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

---

### Task 1: Application shell, navigation, and Start/Quick Scrape

**Files:** app layout/navigation, Start route/components, typed API client, activity/checklist components, tests.

**Primary navigation exactly:** Start, Crawlers, Collections, Runs, Datasets, Assets, Exports, Settings. Do not add global Inbox or Schemas navigation.

**Requirements:**

- Opening Erabi foregrounds one URL input and Quick Scrape action.
- Optional bounded pasted batch preserves ordered per-item outcome/status and links accepted items to their independent Quick Scrape runs.
- Do not present batch envelope as a fifth run type.
- Direct non-HTML result routes to Source/Asset handling instead of extraction review.
- Recent activity is secondary and first-run checklist is non-blocking.
- Shared API client handles stable error envelopes/trace IDs and remote bearer token rules.

**Verification:** type/component tests for navigation, single/batch Start states, direct-file route choice, per-item failures/order, no Batch run-type presentation.

---

### Task 2: Run progress, SSE reconnect, cancellation, and recovery actions

**Files:** run route/components, SSE client/store, action controls/tests.

**Requirements:**

- Use fetch-streaming SSE so remote bearer header can be sent.
- Track last event ID, replay/reconnect, dedupe events, and stop at terminal state.
- Show friendly stable progress steps separately from expandable technical events/logs.
- Expose Cancel, Retry Failed Parts, Resume, Restart/Rerun Full Crawl only when server state allows.
- Clearly communicate partial/cancelled/recoverable states and checkpoint compatibility errors.
- Batch view links each accepted item to its own run.

**Verification:** component/store tests for replay dedupe, reconnect, terminal close, allowed/disabled actions, partial messaging, auth header behavior.

---

### Task 3: Crawler Overview and Studio

**Files:** crawler overview/studio routes and Graph/Seeds/PageTypes/Discovery/Extraction/Test Lab tabs plus accessible list/table equivalents/tests.

**Requirements:**

- Published vs Draft state is visually and semantically unambiguous.
- Overview surfaces validation health, TestEvidence, latest production run, active version, and primary actions.
- Studio tabs: Graph, Seeds, Page Types, Discovery, Extraction, Test Lab.
- Graph is inspectable with equivalent keyboard-accessible list/table; full drag/drop graph editing remains post-MVP.
- Matcher editor exposes explicit PageType priority and full specificity rationale.
- Complete matcher tie is visibly `AMBIGUOUS_PAGE_TYPE`, never hidden by ordering.
- Publish gate distinguishes blocking errors from warnings and consumes server validation rather than duplicating it client-side.

**Verification:** UI state/accessibility tests for Published/Draft, ambiguity, matcher rationale, graph alternative, publish blockers/warnings.

---

### Task 4: Test Lab and Discovery Preview UX

**Files:** Test Lab/Discovery Preview panels, result tables/graphs, tests.

**Test Lab must show:** exact Draft/config hash, input URL, canonicalization explanation, PageType candidates/specificity/rationale, extraction/coverage evidence, transition/discovered URLs, warnings/errors, saved TestEvidence, Published comparison where available.

**Discovery Preview must show:** paths/tree/graph + table, PageType distribution, duplicates/canonicalization stats, unmatched/ambiguous/external/blocked, transitions/cycles/budget counts, robots exclusions, growth/scope warnings.

No Test/Preview surface may label itself production-complete or imply missing-record trust semantics.

**Verification:** component/accessibility tests for all result states and graph/table equivalence.

---

### Task 5: Extraction Studio and Dataset Review UX

**Files:** extraction three-panel workflow, manual selector inspector, Dataset review/grid/card/provenance components, tests.

**Requirements:**

- Desktop Preview ↔ Field Configuration ↔ Record Preview; narrow layout becomes accessible tabs.
- Bidirectional highlight/focus where practical.
- Keyboard/manual selector/value inspector is a full alternative to pointer-only selection.
- Review defaults to Grid/Table; Card optional.
- Support sort/filter, optimistic inline Draft edits, validation visibility, multi-select, field-conflict resolution, provenance drawer, Approve All Valid, reject, Close/Reopen.
- Approved values visually/semantically differ from Draft candidates.
- Production `SCHEMA_DRIFT` routes to new Draft/Test Lab correction; never render trust-restoring `USE_ANYWAY`.

**Verification:** keyboard extraction workflow, sanitised preview containment, validation states, optimistic conflict, bulk approval/rejection, provenance navigation, schema-drift action tests.

---

### Task 6: Settings, security token UX, theme, localization readiness, accessibility

**Files:** Settings/security/theme/i18n/command palette components and tests.

**Requirements:**

- Inheritable controls explicitly distinguish Inherit / Custom / Reset to built-in and display effective value/source.
- Robots override cannot submit without non-empty reason; active override context is prominent.
- Remote token uses `sessionStorage` by default; `localStorage` only after explicit Remember; Forget action available; token never enters URL/settings/log.
- Themes: Follow system, Light, Dark.
- English first with translation keys/localization-ready copy/layout.
- `Ctrl/Cmd+K` accessible command/search palette; destructive actions route to dedicated confirmation.
- WCAG 2.2 AA target: keyboard, visible focus, semantic HTML, labels/errors, contrast, no color-only status, reduced motion, accessible dialogs/tables, usable 200% zoom.

**Verification:** component/accessibility tests for tri-state settings, robots reason, token storage, themes, command palette, focus/navigation/dialog behavior, reduced motion, zoom-responsive layouts.

---

## Plan 09 Gate

```bash
bun install --frozen-lockfile
bun --cwd apps/web run check
bun --cwd apps/web run test
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Confirm all primary routes work with the canonical navigation; SSE reconnect/actions, Crawler Studio/Test Lab/Discovery Preview, extraction/review, tri-state settings, robots reason, token behavior, Recovery Mode restrictions, and accessibility-owned UI pass focused verification. Do not begin Plan 10 until the gate passes.
