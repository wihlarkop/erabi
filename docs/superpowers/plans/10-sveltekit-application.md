# Erabi SvelteKit Product Application Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the typed SvelteKit application shell, URL-first Start page, live crawl progress, visual extraction editor, review experience, and all operational resource pages.

**Architecture:** The static SvelteKit SPA talks only to `/api/v1` and SSE through a typed client. Route-level feature modules keep Start, crawling, extraction, review, assets, exports, backup, diagnostics, and search independent while sharing a consistent shell and state model.

**Tech Stack:** SvelteKit, Svelte, TypeScript, Bun, REST, SSE, CSS, component test tooling.

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

- **Depends on:** [09 Retention, Backup, Recovery, and Diagnostics](./09-backup-recovery-and-diagnostics.md).
- **Produces:** Complete MVP user interface for starting, monitoring, reviewing, curating, exporting, backing up, diagnosing, and searching Erabi data.
- **Gate:** Product UI A: frontend checks and component tests cover every primary route, SSE reconnect, extraction interactions, review actions, error states, and Recovery Mode restrictions.
- **Execution order:** Complete every task in this file in numerical order and commit after each task. Do not begin the next plan until this gate passes.

## Focused File Map

```text
apps/web/src/lib/api/
apps/web/src/lib/components/
apps/web/src/lib/features/start/
apps/web/src/lib/features/crawls/
apps/web/src/lib/features/extraction/
apps/web/src/lib/features/review/
apps/web/src/lib/features/operations/
apps/web/src/routes/
apps/web/src/app.css
```

---

### Task 41: Build the Typed API Client, Application Shell, Start Page, and Recent Activity

**Files:**
- Create: `apps/web/src/lib/api/client.ts`
- Create: `apps/web/src/lib/api/errors.ts`
- Create: `apps/web/src/lib/types/api.ts`
- Create: `apps/web/src/lib/components/layout/AppShell.svelte`
- Create: `apps/web/src/lib/components/layout/Sidebar.svelte`
- Create: `apps/web/src/lib/features/start/UrlScrapeForm.svelte`
- Create: `apps/web/src/lib/features/start/GettingStarted.svelte`
- Create: `apps/web/src/lib/features/start/RecentActivity.svelte`
- Create: `apps/web/src/lib/features/auth/AccessTokenPrompt.svelte`
- Modify: `apps/web/src/routes/+layout.svelte`
- Modify: `apps/web/src/routes/start/+page.svelte`
- Test: `apps/web/src/lib/features/start/start-flow.test.ts`

**Interfaces:**
- Produces: same-origin typed API client with bearer token from session/local storage.
- Produces: Start as default page and approved sidebar navigation.
- Produces: simple URL → Scrape interaction, advanced options collapsed, recent activity, non-blocking first-run checklist.

- [ ] **Step 1: Write failing Start flow tests**

Create tests asserting:

```ts
it("submits a single URL and navigates to live progress", async () => { /* mock POST; expect /crawl-runs/{id} */ });
it("keeps advanced options collapsed by default", () => { /* no rate-limit fields visible */ });
it("shows recent drafts and failed runs before ordinary activity", () => { /* ordered cards */ });
it("shows first-run readiness without a blocking wizard", () => { /* checklist and usable URL field */ });
```

- [ ] **Step 2: Implement a typed API client**

`apiRequest<T>()` must:

- use relative `/api/v1` paths;
- send JSON `Content-Type` for JSON mutations;
- add `Authorization: Bearer` only when a stored token exists;
- store the token in `sessionStorage` by default; use `localStorage` only after explicit `Remember on this device`; provide `Forget token` and never mirror the token into application settings or URLs;
- add an `Idempotency-Key` UUID for crawl/export/backup mutations;
- parse the stable API error envelope into `ErabiApiError`;
- never put tokens in URLs;
- handle 401 by emitting an auth-required event rather than logging token data.

Use browser `crypto.randomUUID()` for request idempotency only; domain IDs still come from backend UUIDv7. Implement `AccessTokenPrompt` on 401/network mode, with password-style input, explicit Remember checkbox, and no token value rendered after submit.

- [ ] **Step 3: Implement the shell and sidebar**

Sidebar order is fixed:

```text
Start
Inbox
Collections
Crawl Runs
Schemas
Datasets
Assets
Exports
Settings
```

Show compact crawler/queue/storage status at the bottom. Use real links, semantic navigation, visible active state, and mobile drawer behavior.

- [ ] **Step 4: Implement the Start form**

Primary view contains headline, labelled URL input, Scrape button, and collapsed Advanced options. Advanced options include Collection, existing Schema suggestion, screenshot, wait selector, auto-scroll, User-Agent, rate-limit override, and crawler connection summary. Start submits single page by default.

- [ ] **Step 5: Implement Recent Activity and Getting Started**

Fetch readiness, recent Sources, Drafts awaiting review, failed/partial runs, and recent exports. Priority order: action-required items then latest normal activity. Getting Started checklist shows database, artifacts, Crawl4AI, and first scrape; it never blocks the form.

- [ ] **Step 6: Run tests and checks**

Run:

```bash
bun --cwd apps/web run test -- start-flow
bun --cwd apps/web run check
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add apps/web bun.lock
git commit -m "feat(web): add Start page and application shell"
```
### Task 42: Implement Crawl Progress, SSE Reconnect, Technical Logs, Cancel, Retry, and Resume UI

**Files:**
- Create: `apps/web/src/lib/api/sse.ts`
- Create: `apps/web/src/lib/features/crawls/CrawlProgress.svelte`
- Create: `apps/web/src/lib/features/crawls/ProgressSteps.svelte`
- Create: `apps/web/src/lib/features/crawls/TechnicalLogs.svelte`
- Create: `apps/web/src/lib/features/crawls/CrawlActions.svelte`
- Create: `apps/web/src/routes/crawl-runs/[id]/+page.svelte`
- Test: `apps/web/src/lib/features/crawls/crawl-progress.test.ts`

**Interfaces:**
- Produces: replay-capable SSE client using `Last-Event-ID` semantics.
- Produces: live friendly steps separated from technical logs.
- Produces: Cancel, Resume from Checkpoint, Retry Failed Parts, Rerun Full Crawl.

- [ ] **Step 1: Write failing SSE and rendering tests**

Test:

- persisted events render in sequence;
- disconnect reconnects from last sequence and does not duplicate events;
- progress uses message translation keys/args;
- technical log panel stays collapsed by default;
- screen-reader live region announces stage changes but not every technical log;
- Cancel changes UI to checkpointing/cancelled;
- `PARTIAL_RESULT` exposes review/debug/retry but never claims complete snapshot.

- [ ] **Step 2: Implement authenticated SSE fetch streaming**

Native `EventSource` cannot set Authorization headers. Implement `fetch()` with streaming `ReadableStream`, `Accept: text/event-stream`, bearer header, parser for `id`, `event`, `data`, and reconnect with last sequence. Use bounded exponential reconnect and stop on explicit cancellation/terminal status.

- [ ] **Step 3: Implement friendly progress steps**

Display Preparing crawler, Checking robots, Loading, Rendering, Waiting/Scrolling, Pagination, Extracting, Validating, Saving Draft, Assets, Complete. Show completed/planned page and record counts. Preserve last known state on reconnect.

- [ ] **Step 4: Implement structured technical logs**

Filters: level, module, event, job/page, text search. Main rows show time, level, concise event, duration/status. Expand reveals trace/job/run/source, code, recoverable, retry unit, redacted context, stack trace, Copy, View Context, Retry. Never interpolate untrusted HTML.

- [ ] **Step 5: Implement terminal navigation**

On successful extraction Draft creation, automatically navigate to Review. Keep a secondary notification/action for Crawl More Pages or Select Links. `NO_CHANGES` stays on run summary and states no review required.

- [ ] **Step 6: Run tests**

Run: `bun --cwd apps/web run test -- crawl-progress`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add apps/web
git commit -m "feat(web): stream crawl progress and recovery actions"
```
### Task 43: Build the Three-Panel Visual Extraction Editor

**Files:**
- Create: `apps/web/src/lib/features/extraction/ExtractionEditor.svelte`
- Create: `apps/web/src/lib/features/extraction/PagePreview.svelte`
- Create: `apps/web/src/lib/features/extraction/FieldEditor.svelte`
- Create: `apps/web/src/lib/features/extraction/RecordPreview.svelte`
- Create: `apps/web/src/lib/features/extraction/DomTree.svelte`
- Create: `apps/web/src/lib/features/extraction/editor-state.svelte.ts`
- Create: `apps/web/src/routes/reviews/[id]/extract/+page.svelte`
- Test: `apps/web/src/lib/features/extraction/extraction-editor.test.ts`

**Interfaces:**
- Produces: Preview | Field Configuration | Record Preview three-panel desktop layout and tabbed small-screen layout.
- Produces: bidirectional Preview ↔ Field ↔ Record highlighting.
- Produces: one-container visual selection, manual selector entry, live preview, Schema Draft autosave.

- [ ] **Step 1: Write editor interaction tests**

Assert:

- clicking a mapped preview node selects container/field;
- hovering a field highlights all matching nodes;
- choosing Record 8 highlights the eighth container;
- keyboard DOM tree can select the same node without pointer;
- manual selector update refreshes preview;
- fragile selector warning is visible text, not color only;
- switching Document/Records mode calls extraction preview, not recrawl;
- stale preview responses are discarded;
- autosave state shows Editing, Saving, Saved, Failed/Retry.

- [ ] **Step 2: Implement isolated sandbox preview messaging**

Render the sanitized preview endpoint inside `<iframe sandbox="allow-same-origin">` without script permission. Because scripts cannot run in the frame, overlay a same-origin selection layer using node bounding boxes returned by the backend. Do not depend on executing source-site or injected scripts. Pointer coordinates select backend node boxes; keyboard uses DOM tree.

- [ ] **Step 3: Implement editor state with Svelte runes**

Store mode, selected container/node, fields, temporary Schema definition, highlighted nodes, selected record, preview request generation, autosave revision, and validation. Debounce extraction preview/autosave; abort prior fetch when a new request begins.

- [ ] **Step 4: Implement container and field workflows**

Container stage shows detected similar count and highlights matches. Field stage shows name, type, relative selector, value source/attribute, coverage, samples, required/optional, normalization, validation, unique-key participation, and delete/add/manual point actions.

- [ ] **Step 5: Implement Record Preview**

Default Grid with paginated records and Card toggle. Updates immediately from backend preview. Selecting a record updates source highlight. Surface missing/error/warning counts and coverage. Do not allow approval from temporary unsaved extraction state.

- [ ] **Step 6: Run tests and accessibility checks**

Run:

```bash
bun --cwd apps/web run test -- extraction-editor
bun --cwd apps/web run check
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add apps/web
git commit -m "feat(web): add visual extraction editor"
```
### Task 44: Build Review Grid/Card Views, Provenance Drawer, Diff, Approval, Rejection, and Close/Reopen

**Files:**
- Create: `apps/web/src/lib/features/review/ReviewPage.svelte`
- Create: `apps/web/src/lib/features/review/RecordGrid.svelte`
- Create: `apps/web/src/lib/features/review/RecordCards.svelte`
- Create: `apps/web/src/lib/features/review/ProvenanceDrawer.svelte`
- Create: `apps/web/src/lib/features/review/DiffReview.svelte`
- Create: `apps/web/src/lib/features/review/ValidationSummary.svelte`
- Create: `apps/web/src/lib/features/review/ReviewActions.svelte`
- Create: `apps/web/src/routes/reviews/[id]/+page.svelte`
- Test: `apps/web/src/lib/features/review/review-workflow.test.ts`

**Interfaces:**
- Produces: Grid default, Card optional, inline Draft editing/autosave, provenance, validation filters, approval/rejection, change decisions, Close/Reopen.

- [ ] **Step 1: Write end-user behavior tests**

Test:

- errors visibly disable approval and cannot be overridden;
- warnings remain approvable without confirmation;
- Approve All Valid reports approved/skipped/warning counts;
- inline Draft edit shows save state and conflict handling;
- approved cells are locked and offer Create New Version;
- per-field provenance opens source, raw/normalized, selector, transformations, Schema/Crawl links;
- diff accepts all/selected/keep old/reject new;
- bulk rejection requires reason, single rejection does not;
- closing unresolved Review displays counts and explicit confirmation;
- `CLOSED_WITH_UNRESOLVED_ITEMS` is not labelled complete.

- [ ] **Step 2: Implement an accessible paginated data grid**

Use semantic table markup, sticky headers, keyboard cell navigation, row selection checkboxes with labels, sort/filter controls, validation icons plus text, and provenance buttons per cell. Avoid an external grid dependency in the MVP. Card View provides a simpler alternative.

- [ ] **Step 3: Implement Draft edits and conflicts**

Debounce per-cell PATCH with expected revision. Show Saving/Saved/Failed. On 409, stop autosave and show Reload latest / Compare choices; never overwrite silently. Before navigation, warn only when a local request is still unsent/in-flight.

- [ ] **Step 4: Implement provenance and highlight integration**

Drawer links to the extraction preview with selected node. Original URL opens in a new tab with `noopener,noreferrer`. Raw HTML downloads rather than embeds. Copy selector/value actions use plain text.

- [ ] **Step 5: Implement candidate workflows**

New/Updated/Missing/Restored badges include text. Diff review supports field decisions. Missing actions match the API. Deleted/old approved versions remain accessible in history.

- [ ] **Step 6: Run tests**

Run: `bun --cwd apps/web run test -- review-workflow`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add apps/web
git commit -m "feat(web): add provenance-driven review workflow"
```
### Task 45: Build Inbox, Collections, Runs, Schemas, Datasets, Assets, Exports, Backup, Trash, Settings, Diagnostics, and Search Pages

**Files:**
- Create: `apps/web/src/routes/inbox/+page.svelte`
- Create: `apps/web/src/routes/collections/+page.svelte`
- Create: `apps/web/src/routes/crawl-runs/+page.svelte`
- Create: `apps/web/src/routes/schemas/+page.svelte`
- Create: `apps/web/src/routes/datasets/+page.svelte`
- Create: `apps/web/src/routes/assets/+page.svelte`
- Create: `apps/web/src/routes/exports/+page.svelte`
- Create: `apps/web/src/routes/settings/+page.svelte`
- Create: `apps/web/src/routes/settings/backup/+page.svelte`
- Create: `apps/web/src/routes/settings/diagnostics/+page.svelte`
- Create: `apps/web/src/routes/trash/+page.svelte`
- Create: `apps/web/src/lib/features/search/CommandPalette.svelte`
- Create: `apps/web/src/lib/features/settings/InheritedSetting.svelte`
- Test: `apps/web/src/lib/features/operations/operations-pages.test.ts`

**Interfaces:**
- Produces: all primary navigation destinations and Ctrl/Cmd+K metadata search/quick actions.
- Produces: Assets selected download, Exports history/creation/download/delete file, backup/restore/verify, diagnostics/integrity, settings inheritance, archive/trash.

- [ ] **Step 1: Write operations page tests**

Cover:

- Inbox lists uncollected Sources and action-required Drafts;
- Collections show override indicators;
- Runs filter status and open details;
- Assets default URL-only and explicit selection/download;
- Exports show `FILE_REMOVED` without download and no Regenerate button;
- backup automatic setting defaults off and type defaults Database Only;
- encryption password is never retained after submit;
- Restore requires verification and confirmation;
- settings show source Built-in/Global/Collection/Per-run;
- Trash supports Restore and permanent-delete impact confirmation;
- command palette searches metadata and excludes destructive commands.

- [ ] **Step 2: Implement consistent list patterns**

Use reusable pagination, filtering, empty/loading/error states, status text, and action menus. Avoid one-off fetch code by using typed API modules. URLs in UI are safely rendered as text and shortened visually without losing accessible full value.

- [ ] **Step 3: Implement Assets and Exports workflows**

Assets show preview where safe, MIME, known size, original URL, status, and batch Download Selected. Exports create Standard/With Provenance/Debug with format and optional Include Downloaded Assets. Display job progress and persistent history.

- [ ] **Step 4: Implement Backup and Recovery UI**

Create/verify/download/restore/delete backup. Show Database Only vs Full size estimates, encryption option, password no-recovery warning, and progress/cancel. In Recovery Mode, replace normal mutation nav with diagnostics/backup restore/migration retry actions.

- [ ] **Step 5: Implement inherited settings controls**

Each Collection setting offers Inherit Global, Custom, Reset to Built-in and displays active value/source. Global settings edit only ordinary settings. `.env` secrets/bootstrap fields show status and restart-required guidance but no editable secret value.

- [ ] **Step 6: Implement command palette**

Open with Ctrl/Cmd+K, search debounced metadata, keyboard navigate results, and include safe actions: Scrape URL, Create Collection, Open Inbox, View failed runs, Resume cancelled crawl, Create backup, Run integrity check, Open Settings. Destructive actions never appear.

- [ ] **Step 7: Run tests**

Run: `bun --cwd apps/web run test -- operations-pages`

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add apps/web
git commit -m "feat(web): add Erabi management and operations pages"
```
