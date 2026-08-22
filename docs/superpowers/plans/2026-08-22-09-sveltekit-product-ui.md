# Erabi SvelteKit Product UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the complete accessible SvelteKit Crawler Studio UI for Start/Quick Scrape, run progress/recovery, Crawler Studio, Test Lab, Discovery Preview, visual extraction, review/provenance, exports/assets/backup operations, and settings/security.

**Architecture:** A static-compatible SvelteKit SPA talks only to `/api/v1` plus authenticated fetch-stream SSE. Feature modules consume a generated/hand-maintained typed API boundary derived from backend DTO contracts; they do not duplicate domain decisions in frontend state. The product has one canonical navigation and every pointer-heavy visual workflow has a keyboard-operable equivalent.

**Tech Stack:** current stable SvelteKit/Svelte/TypeScript/Vite, Bun, Vitest, Testing Library, fetch streaming, Playwright accessibility helpers in Plan 10.

**Spec:** `docs/specs/01-product-and-experience.md`, `docs/specs/02-crawler-studio-domain.md`, `docs/specs/03-discovery-graph-and-runs.md`, `docs/specs/04-extraction-curation-and-provenance.md`, `docs/specs/08-ux-accessibility-and-verification.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Global Constraints

- Global navigation is exactly Start, Crawlers, Collections, Runs, Datasets, Assets, Exports, Settings.
- There is no global primary `Schemas` or `Inbox` navigation item.
- Start foregrounds a URL, not a wizard/configuration screen.
- Quick Scrape batch is an ordered envelope over independent Quick Scrape runs; never present it as a fifth run type.
- Direct-file results route to Asset handling and never to HTML extraction review.
- Published vs Draft Crawler state must be visually and programmatically unambiguous.
- Test Lab/Discovery Preview never imply production completeness.
- Validation ERROR blocks approval; WARNING does not.
- Production `SCHEMA_DRIFT` routes to Draft/Test Lab correction; no trust-restoring `USE_ANYWAY` action.
- Inheritable settings visibly distinguish Inherit / Custom / Reset to built-in and display effective source/value.
- Robots override cannot submit without a non-empty reason and active override state is prominent.
- Browser bearer token uses `sessionStorage` by default and `localStorage` only after explicit Remember-on-device choice; never URL/settings/log.
- UI copy uses translation keys from first implementation.
- MVP targets WCAG 2.2 AA: keyboard, visible focus, semantic HTML, no color-only state, reduced motion, 200% zoom, accessible dialogs/tables/non-pointer extraction.

## Focused File Map

```text
apps/web/src/lib/api/client.ts
apps/web/src/lib/api/auth.ts
apps/web/src/lib/api/sse.ts
apps/web/src/lib/types/api.ts
apps/web/src/lib/i18n/
apps/web/src/lib/components/layout/
apps/web/src/lib/features/start/
apps/web/src/lib/features/runs/
apps/web/src/lib/features/crawlers/
apps/web/src/lib/features/extraction/
apps/web/src/lib/features/review/
apps/web/src/lib/features/operations/
apps/web/src/lib/features/settings/
apps/web/src/routes/
```

---

### Task 1: Establish typed API/auth primitives, app shell, canonical navigation, and Start/Quick Scrape UI

**Files:**
- Create: `apps/web/src/lib/api/client.ts`
- Create: `apps/web/src/lib/api/auth.ts`
- Create: `apps/web/src/lib/types/api.ts`
- Create: `apps/web/src/lib/i18n/en.ts`
- Create: `apps/web/src/lib/i18n/index.ts`
- Create: `apps/web/src/lib/components/layout/AppShell.svelte`
- Create: `apps/web/src/lib/components/layout/Sidebar.svelte`
- Create: `apps/web/src/lib/features/start/QuickScrapeForm.svelte`
- Create: `apps/web/src/lib/features/start/BatchQuickScrapeForm.svelte`
- Create: `apps/web/src/lib/features/start/RecentActivity.svelte`
- Create: `apps/web/src/lib/features/start/GettingStarted.svelte`
- Create: `apps/web/src/lib/features/start/AccessTokenPrompt.svelte`
- Modify: `apps/web/src/routes/+layout.svelte`
- Modify: `apps/web/src/routes/+page.svelte`
- Test: `apps/web/src/lib/features/start/start-flow.test.ts`
- Test: `apps/web/src/lib/api/auth.test.ts`

**Interfaces:**
- Produces `apiRequest<T>()`, `ApiError`, token storage helpers, `QuickScrapeRequest/Response`, `QuickScrapeBatchResponse` types.
- `/` is the Start experience; no separate onboarding gate.

- [ ] **Step 1: Add current stable frontend testing dependencies with Bun**

From `apps/web`:

```bash
bun add -d vitest jsdom @testing-library/svelte @testing-library/jest-dom
```

Configure Vitest in the existing Vite/SvelteKit config with `jsdom` and a test setup importing `@testing-library/jest-dom/vitest`. Do not add another package manager lockfile.

- [ ] **Step 2: Write failing canonical-navigation and Start tests**

```ts
import { render, screen } from '@testing-library/svelte';
import AppShell from '../../components/layout/AppShell.svelte';
import QuickScrapeForm from './QuickScrapeForm.svelte';

it('renders only the canonical primary navigation labels', () => {
  render(AppShell, { children: (() => null) as never });
  for (const label of ['Start','Crawlers','Collections','Runs','Datasets','Assets','Exports','Settings']) {
    expect(screen.getByRole('link', { name: label })).toBeInTheDocument();
  }
  expect(screen.queryByRole('link', { name: 'Schemas' })).not.toBeInTheDocument();
  expect(screen.queryByRole('link', { name: 'Inbox' })).not.toBeInTheDocument();
});

it('starts with one labelled URL input and one primary Scrape action', () => {
  render(QuickScrapeForm);
  expect(screen.getByRole('textbox', { name: /website url/i })).toBeVisible();
  expect(screen.getByRole('button', { name: /^scrape$/i })).toBeVisible();
});
```

Also test advanced settings collapsed by default, first-run checklist never disables URL input, and Recent Activity appears after the primary form.

- [ ] **Step 3: Write failing auth storage tests**

Test: token defaults to `sessionStorage`; Remember-on-device moves/sets it in `localStorage`; Forget removes both; no helper returns a URL containing the token; API client adds `Authorization` header only when token exists; 401 emits auth-required state without logging token.

- [ ] **Step 4: Run RED**

```bash
bun --cwd apps/web run test -- start-flow auth
```

Expected: failing tests/missing modules.

- [ ] **Step 5: Implement typed client/auth and translation-key shell**

`apiRequest<T>` uses relative `/api/v1` paths, JSON Content-Type only for JSON mutations, `Authorization: Bearer` when a stored token exists, and `Idempotency-Key` via `crypto.randomUUID()` for run/export/backup mutations. Parse backend `ApiErrorEnvelope` exactly; never interpolate raw stack traces as user copy.

Translation helper may be intentionally minimal but every product-owned string in new components must come from keys in `en.ts`; user/scraped text is not translated.

- [ ] **Step 6: Implement single/batch Start flows**

Single submit calls `POST /api/v1/quick-scrapes`, then navigates to `/runs/{runId}` for accepted HTML crawl. A direct-file intake response navigates to `/assets/{assetId}` with detected MIME/download action.

Batch form is a secondary/expandable convenience accepting bounded pasted URL lines. It calls `POST /api/v1/quick-scrape-batches` and renders one row per input in original order with validation/conflict/accepted state and link to each independent run. Never display one synthetic batch Run ID/status.

- [ ] **Step 7: Run GREEN and commit**

```bash
bun --cwd apps/web run test -- start-flow auth
bun --cwd apps/web run check
git add apps/web bun.lock
 git commit -m "feat(web): add canonical shell and Quick Scrape start"
```

---

### Task 2: Implement authenticated replayable run progress, technical logs, cancel/retry/resume, and batch item tracking

**Files:**
- Create: `apps/web/src/lib/api/sse.ts`
- Create: `apps/web/src/lib/features/runs/RunProgress.svelte`
- Create: `apps/web/src/lib/features/runs/ProgressSteps.svelte`
- Create: `apps/web/src/lib/features/runs/TechnicalLogs.svelte`
- Create: `apps/web/src/lib/features/runs/RunActions.svelte`
- Create: `apps/web/src/routes/runs/[id]/+page.svelte`
- Test: `apps/web/src/lib/features/runs/run-progress.test.ts`
- Test: `apps/web/src/lib/api/sse.test.ts`

**Interfaces:**
- Produces `streamEvents(url, token, lastEventId, signal)` async iterator/callback API.
- Produces visible run actions based on backend status/recoverability.

- [ ] **Step 1: Write failing SSE parser/reconnect tests**

Feed a deterministic byte stream containing `id`, `event`, multiline `data`, comments/heartbeats, and fragmented chunks. Assert parser reconstructs events exactly. Simulate disconnect after event 5 and reconnect from event 5; events 1–5 are not duplicated and 6+ continue.

Native `EventSource` must not be used because remote auth requires bearer headers.

- [ ] **Step 2: Write failing progress/action component tests**

Persisted event keys render translated friendly stages while technical detail remains collapsed. Screen-reader live region announces stage changes, not every log line. `PARTIAL_RESULT` exposes review/debug/retry choices but never labels the run a complete snapshot. Cancel transitions to cancelling/checkpoint state; Resume appears only when backend returns a valid checkpoint action.

- [ ] **Step 3: Run RED**

```bash
bun --cwd apps/web run test -- sse run-progress
```

- [ ] **Step 4: Implement fetch-stream SSE with bounded reconnect**

Use `fetch()` with `Accept: text/event-stream`, Authorization header from auth store, and `Last-Event-ID` header. Parse incrementally with `TextDecoder`. Keep highest accepted durable sequence/event ID, drop replay duplicates, use bounded exponential reconnect for recoverable network failures, and stop immediately on AbortSignal or terminal run status.

- [ ] **Step 5: Implement progress/log/actions UI**

Friendly stages cover preparation, robots, loading/rendering, waiting/scrolling, discovery/pagination, extraction, validation, saving, assets, complete. Show completed/planned page and record counts where supplied. Technical logs use structured rows/filters and escape all content.

Actions call exact backend endpoints from Plans 04/06 and surface failures through `ApiError`; no optimistic claim that remote cancellation succeeded until backend state confirms it.

- [ ] **Step 6: Run GREEN and commit**

```bash
bun --cwd apps/web run test -- sse run-progress
bun --cwd apps/web run check
git add apps/web
 git commit -m "feat(web): stream durable run progress and recovery actions"
```

---

### Task 3: Build Crawler Overview, Draft/Published Studio, Graph/list equivalence, Seeds/Page Types/Discovery editors, and publish gate

**Files:**
- Create: `apps/web/src/lib/features/crawlers/CrawlerOverview.svelte`
- Create: `apps/web/src/lib/features/crawlers/VersionBadge.svelte`
- Create: `apps/web/src/lib/features/crawlers/StudioNav.svelte`
- Create: `apps/web/src/lib/features/crawlers/CrawlerGraph.svelte`
- Create: `apps/web/src/lib/features/crawlers/CrawlerGraphTable.svelte`
- Create: `apps/web/src/lib/features/crawlers/SeedEditor.svelte`
- Create: `apps/web/src/lib/features/crawlers/PageTypeEditor.svelte`
- Create: `apps/web/src/lib/features/crawlers/MatcherEditor.svelte`
- Create: `apps/web/src/lib/features/crawlers/TransitionEditor.svelte`
- Create: `apps/web/src/lib/features/crawlers/PublishPanel.svelte`
- Create: `apps/web/src/routes/crawlers/+page.svelte`
- Create: `apps/web/src/routes/crawlers/[id]/+page.svelte`
- Test: `apps/web/src/lib/features/crawlers/crawler-studio.test.ts`

**Interfaces:**
- Produces Overview actions Run Crawler, Test Draft, Discovery Preview, Edit/Create Draft.
- Studio sections are Graph, Seeds, Page Types, Discovery, Extraction, Test Lab within a crawler—not global nav.

- [ ] **Step 1: Write failing Published-vs-Draft and Overview tests**

Assert accessible text exposes active Published version and optional Draft separately; health includes Page Type/transition/test evidence warnings, Crawl4AI status, last Production Run summary. Editing controls target Draft only; Published view offers Create Draft rather than editable fields.

- [ ] **Step 2: Write failing graph/table-equivalence tests**

For a fixture graph, every Page Type node/Transition edge shown visually must be represented in the accessible table/list with link/button to the same configuration panel and warning state. Graph operation cannot be pointer-only.

- [ ] **Step 3: Write failing matcher ambiguity/publish tests**

Matcher Test result shows explicit priority, matcher kind, literal path segments, query constraints, literal characters, wildcard count, candidates, and rationale. Complete tie visibly says `AMBIGUOUS_PAGE_TYPE`. Publish errors disable/withhold publish action; warnings remain non-blocking but visible.

- [ ] **Step 4: Run RED**

```bash
bun --cwd apps/web run test -- crawler-studio
```

- [ ] **Step 5: Implement Crawler Studio state from backend contracts**

Do not recompute Page Type winner in TypeScript; render backend match explanation. Draft edits use expected revision/hash and show Editing/Saving/Saved/Conflict/Failed states. On 409, stop silent autosave and offer Reload latest / Compare.

Graph is inspectable; configuration mutation uses deterministic forms/panels. Do not build post-MVP arbitrary drag/drop graph programming.

- [ ] **Step 6: Run GREEN and commit**

```bash
bun --cwd apps/web run test -- crawler-studio
bun --cwd apps/web run check
git add apps/web
 git commit -m "feat(web): build Crawler Studio authoring UI"
```

---

### Task 4: Build Test Lab and bounded Discovery Preview UX

**Files:**
- Create: `apps/web/src/lib/features/crawlers/TestLab.svelte`
- Create: `apps/web/src/lib/features/crawlers/MatchExplanation.svelte`
- Create: `apps/web/src/lib/features/crawlers/DiscoveryPreview.svelte`
- Create: `apps/web/src/lib/features/crawlers/DiscoveryTable.svelte`
- Create: `apps/web/src/lib/features/crawlers/GrowthWarnings.svelte`
- Create: `apps/web/src/routes/crawlers/[id]/test/+page.svelte`
- Create: `apps/web/src/routes/crawlers/[id]/discovery-preview/+page.svelte`
- Test: `apps/web/src/lib/features/crawlers/test-lab.test.ts`
- Test: `apps/web/src/lib/features/crawlers/discovery-preview.test.ts`

**Interfaces:**
- Renders exact Draft Version/config hash and durable Test Evidence.
- Discovery Preview always displays sample bounds and non-production status.

- [ ] **Step 1: Write failing Test Lab evidence tests**

Response fixture renders input URL, canonicalization explanation, all Page Type candidates/specificity rationale, extraction preview/coverage hook, transition result, warnings/errors, exact Draft/config hash, and Test Evidence ID. An ambiguity fixture must not hide competing candidates.

- [ ] **Step 2: Write failing bounded-preview tests**

Preview shows selected seed/page/depth/time/transition limits, sampled pages, canonical unique/duplicates, Page Type distribution, ambiguous/unmatched/external/blocked lists, transition counts, robots exclusions, budget hits, growth warnings. Accessible table/list covers every path shown in optional graph/tree.

Assert visible copy says preview/sample and never “complete production snapshot”.

- [ ] **Step 3: Run RED**

```bash
bun --cwd apps/web run test -- test-lab discovery-preview
```

- [ ] **Step 4: Implement state/actions using backend evidence only**

Test Lab can request canonicalization, Page Type match, extraction, selector coverage, pagination detection, transition test, and discovered URL preview endpoints admitted by backend. Switching tests or retrying uses Draft version/config hash in request; stale response with older request generation is discarded.

Discovery Preview exposes cycle/budget warnings and lets user navigate to relevant Page Type/Transition Draft editor; it does not mutate Published configuration itself.

- [ ] **Step 5: Run GREEN and commit**

```bash
bun --cwd apps/web run test -- test-lab discovery-preview
bun --cwd apps/web run check
git add apps/web
 git commit -m "feat(web): add Test Lab and Discovery Preview UX"
```

---

### Task 5: Build visual extraction editor and Dataset review/provenance workflows

**Files:**
- Create: `apps/web/src/lib/features/extraction/ExtractionEditor.svelte`
- Create: `apps/web/src/lib/features/extraction/PagePreview.svelte`
- Create: `apps/web/src/lib/features/extraction/DomTree.svelte`
- Create: `apps/web/src/lib/features/extraction/FieldEditor.svelte`
- Create: `apps/web/src/lib/features/extraction/RecordPreview.svelte`
- Create: `apps/web/src/lib/features/review/ReviewGrid.svelte`
- Create: `apps/web/src/lib/features/review/ReviewCards.svelte`
- Create: `apps/web/src/lib/features/review/ProvenanceDrawer.svelte`
- Create: `apps/web/src/lib/features/review/ReviewActions.svelte`
- Create: `apps/web/src/routes/crawlers/[id]/extract/+page.svelte`
- Create: `apps/web/src/routes/datasets/[id]/+page.svelte`
- Test: `apps/web/src/lib/features/extraction/extraction-editor.test.ts`
- Test: `apps/web/src/lib/features/review/review-workflow.test.ts`

**Interfaces:**
- Produces Preview ↔ Field ↔ Record linked selection with keyboard DOM/manual selector alternative.
- Produces Grid default/Card optional Review and field provenance drawer.

- [ ] **Step 1: Write failing extraction interaction/accessibility tests**

Assert pointer selection and keyboard DOM-tree selection resolve the same backend node ID; hovering/focusing a field highlights all matching node IDs; selecting a record highlights its container; manual CSS selector entry refreshes preview; fragile selector warning includes text; mode switch calls extraction preview only and never starts a Crawl Run.

- [ ] **Step 2: Write failing review/validation/immutability tests**

ERROR visibly blocks approval and cannot be overridden; WARNING remains approvable. Approve All Valid reports approved/skipped/warning counts. Draft inline edits show Saving/Saved/Conflict; Approved cells are read-only and offer Create New Draft Version. Bulk rejection requires reason; single rejection does not. Close with unresolved items shows counts/explicit confirmation and never labels unresolved work complete.

- [ ] **Step 3: Write failing provenance/drift tests**

Opening one field displays Source/original+canonical URL, Crawler/Version, Run, Page Type, transition path when relevant, artifact hash/ref, selector, raw/normalized values, transformations, timestamp. Production `SCHEMA_DRIFT` surface offers Create/Edit Draft and Test Lab; assert no button/action named `USE_ANYWAY`.

- [ ] **Step 4: Run RED**

```bash
bun --cwd apps/web run test -- extraction-editor review-workflow
```

- [ ] **Step 5: Implement isolated preview selection and review state**

Render sanitized preview in a sandboxed iframe without script permission. Because injected/source scripts cannot run, use backend node/bounding metadata to render/select through a parent overlay; keyboard users operate `DomTree`. Never depend on page-provided JS for selection.

Debounce/abort extraction preview and Draft autosave; attach request generation/revision and ignore stale responses. Render backend raw/normalized/validation/provenance data as text—not `{@html}` except the separately isolated sanitized preview endpoint.

- [ ] **Step 6: Run GREEN and commit**

```bash
bun --cwd apps/web run test -- extraction-editor review-workflow
bun --cwd apps/web run check
git add apps/web
 git commit -m "feat(web): add extraction review and provenance workflows"
```

---

### Task 6: Build operations pages, tri-state Settings, robots reason UX, command palette, themes/localization readiness, and accessibility baseline

**Files:**
- Create: `apps/web/src/lib/features/operations/AssetsPage.svelte`
- Create: `apps/web/src/lib/features/operations/ExportsPage.svelte`
- Create: `apps/web/src/lib/features/operations/BackupsPage.svelte`
- Create: `apps/web/src/lib/features/operations/DiagnosticsPage.svelte`
- Create: `apps/web/src/lib/features/settings/TriStateSetting.svelte`
- Create: `apps/web/src/lib/features/settings/RobotsOverrideField.svelte`
- Create: `apps/web/src/lib/features/settings/SettingsPage.svelte`
- Create: `apps/web/src/lib/components/CommandPalette.svelte`
- Create: `apps/web/src/routes/assets/+page.svelte`
- Create: `apps/web/src/routes/exports/+page.svelte`
- Create: `apps/web/src/routes/settings/+page.svelte`
- Test: `apps/web/src/lib/features/settings/settings.test.ts`
- Test: `apps/web/src/lib/components/command-palette.test.ts`
- Test: `apps/web/src/lib/features/operations/operations.test.ts`

**Interfaces:**
- Tri-state component round-trips `INHERIT`, `CUSTOM(value)`, `RESET_TO_BUILT_IN` and shows effective source/value.
- Command palette performs safe navigation/actions only; destructive operations route to dedicated confirmation screens.

- [ ] **Step 1: Write failing tri-state and robots-reason tests**

```ts
it('distinguishes inherit custom and reset-to-built-in', async () => {
  // render fixture and assert three explicit options plus effective source label
});

it('cannot submit robots override without a reason', async () => {
  // enable override, leave reason blank, expect submit disabled and accessible validation message
});
```

Also test reason is not silently prefilled from a different previous run; same-run Retry/Resume displays frozen backend reason context read-only/explicitly associated with that run.

- [ ] **Step 2: Write failing operations safety tests**

Asset page requires explicit Download action. Standard export labels Approved-only and Debug Bundle separately. Retention cleanup displays preview count/bytes/categories before confirmation. Backup restore shows verify step and maintenance warning. Critical storage displays blocked-work explanation without auto-delete action being triggered.

- [ ] **Step 3: Write failing command palette and appearance tests**

`Ctrl/Cmd+K` is keyboard operable; safe actions include Scrape URL/Create Crawler/Create Collection/Open failed runs/Resume recoverable run/Create backup/Run integrity/Open Settings. Permanent delete/restore/empty Trash route to dedicated screen/dialog rather than execute immediately.

Theme options exactly Follow system, Light, Dark. Reduced motion preference is honored in CSS. No status component relies on color alone.

- [ ] **Step 4: Run RED**

```bash
bun --cwd apps/web run test -- settings command-palette operations
```

- [ ] **Step 5: Implement Settings/operations and accessibility primitives**

Use semantic form controls, fieldsets/legends, `aria-describedby` for validation/help, visible focus, modal focus trap/restore using platform/component primitives selected carefully, and semantic tables for data grids/accessible graph equivalent. Use CSS that remains usable at 200% zoom; avoid fixed-height content clipping.

Browser notifications are OFF by default and request permission only after explicit user enable. Notification copy excludes extracted values/secrets/full sensitive URLs.

- [ ] **Step 6: Run full Plan 09 gate and commit**

```bash
bun --cwd apps/web run test
bun --cwd apps/web run check
bun --cwd apps/web run build
```

Expected: all primary route/component tests pass, including canonical nav, single/batch/direct-file Start, SSE replay UI, Published/Draft state, ambiguity rationale, bounded preview labels, keyboard extraction, review validation/provenance, tri-state settings, robots reason, token storage, Recovery Mode/operations restrictions, theme/localization readiness.

```bash
git add apps/web bun.lock
 git commit -m "feat(web): complete accessible Erabi MVP interface"
```

## Plan 09 Gate

Do not start Plan 10 until Task 6 Step 6 passes from a clean checkout and `git status --short` is empty.
