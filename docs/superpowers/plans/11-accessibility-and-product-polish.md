# Erabi Accessibility and Product Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add English-first localization infrastructure, light/dark/system themes, opt-in browser notifications, WCAG 2.2 AA behavior, component coverage, and automated accessibility checks.

**Architecture:** UI copy uses translation keys from the start and preferences persist through Erabi settings. Complex controls provide keyboard and screen-reader alternatives, status never relies on color alone, and automated checks supplement explicit manual keyboard/zoom verification.

**Tech Stack:** SvelteKit, TypeScript, Bun, browser Notification API, accessibility test tooling, Playwright/axe-compatible checks.

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

- **Depends on:** [10 SvelteKit Product Application](./10-sveltekit-application.md).
- **Produces:** Localization-ready copy, theme system, optional notifications, accessible data/extraction interactions, and automated frontend accessibility coverage.
- **Gate:** Product UI B: component suite passes, automated accessibility checks report no serious/critical findings, and documented manual keyboard/200% zoom checks pass.
- **Execution order:** Complete every task in this file in numerical order and commit after each task. Do not begin the next plan until this gate passes.

## Focused File Map

```text
apps/web/src/lib/i18n/
apps/web/src/lib/stores/theme.ts
apps/web/src/lib/features/notifications/
apps/web/src/lib/components/accessibility/
apps/web/src/**/*.test.ts
apps/web/tests/accessibility/
```

---

### Task 46: Add English-First i18n, Theme Modes, Browser Notifications, and Accessibility Baseline

**Files:**
- Create: `apps/web/src/lib/i18n/en.ts`
- Create: `apps/web/src/lib/i18n/index.ts`
- Create: `apps/web/src/lib/stores/preferences.svelte.ts`
- Create: `apps/web/src/lib/features/notifications/browser.ts`
- Create: `apps/web/src/lib/components/a11y/LiveRegion.svelte`
- Modify: `apps/web/src/app.css`
- Modify: all UI components created in Tasks 41–45
- Test: `apps/web/src/lib/i18n/i18n.test.ts`
- Test: `apps/web/src/lib/features/notifications/browser.test.ts`

**Interfaces:**
- Produces: translation keys for every UI string.
- Produces: Follow System default, Light, Dark.
- Produces: optional browser notifications off by default with explicit permission.
- Enforces: WCAG 2.2 AA baseline.

- [ ] **Step 1: Write translation completeness test**

Walk the English dictionary, ensure keys are unique/non-empty, and ensure status/error/progress codes have translations. Add a lint/test helper that rejects hard-coded user-visible strings in designated feature directories except test fixtures and data values.

- [ ] **Step 2: Implement a small typed i18n layer**

English is the only MVP dictionary, but components call `t("start.scrape")`. Support interpolation, plural count, and locale-aware date/number/byte formatting through `Intl`. Do not translate user data.

- [ ] **Step 3: Implement appearance preferences**

Follow system uses `prefers-color-scheme`. Light/Dark override immediately and persist through Settings API/database. Define CSS custom properties for surfaces, text, border, focus, success/warning/error, and highlighted extraction nodes. Status always includes icon/text, not color alone.

- [ ] **Step 4: Implement browser notification opt-in**

Default off. Request browser permission only after toggle. Notify crawl/export/backup/integrity completion/failure/partial for sufficiently long background jobs. Notification title/body never include URL, content, token, or values. Click focuses/opens related Erabi route. No in-app Notification Center.

- [ ] **Step 5: Apply accessibility requirements**

- keyboard reachability and logical focus order;
- visible high-contrast focus rings;
- skip link and semantic landmarks;
- accessible names/descriptions/errors;
- reduced-motion media query;
- restrained polite live regions;
- extraction DOM tree/manual selector alternative;
- usable at 200% zoom and narrow widths;
- dialogs trap/restore focus;
- tables/cards offer equivalent actions.

- [ ] **Step 6: Run tests and manual checklist**

Run:

```bash
bun --cwd apps/web run test -- i18n
bun --cwd apps/web run test -- browser
bun --cwd apps/web run check
```

Manually verify keyboard-only Start → crawl progress → Review and 200% zoom before commit.

- [ ] **Step 7: Commit**

```bash
git add apps/web
git commit -m "feat(web): add localization theme and accessibility"
```
### Task 47: Add Frontend Component Coverage and Automated Accessibility Checks

**Files:**
- Create: `apps/web/src/test-utils/render.ts`
- Create: `apps/web/src/test-utils/api.ts`
- Create: `apps/web/src/test-utils/a11y.ts`
- Create: `apps/web/src/lib/components/**/*.test.ts`
- Modify: `apps/web/package.json`
- Modify: `apps/web/vite.config.ts`

**Interfaces:**
- Produces: deterministic API/SSE mocks for component tests.
- Produces: automated axe checks on critical component states.
- Establishes: coverage thresholds for critical frontend logic.

- [ ] **Step 1: Add accessibility and coverage dependencies**

Run from `apps/web`:

```bash
bun add -d vitest-axe @vitest/coverage-v8
```

- [ ] **Step 2: Create typed test helpers**

Implement a mock API router that matches method/path and records requests, an SSE event builder with sequence, and `expectNoA11yViolations(container)` using `vitest-axe`. Ensure helpers never hide unexpected calls.

- [ ] **Step 3: Add critical-state component tests**

At minimum test accessibility and actions for:

- Start normal/Crawl4AI unavailable;
- progress running/partial/failed/cancelled;
- extraction editor pointer and keyboard states;
- Review valid/errors/diff/missing;
- Assets download states;
- Export create/completed/file removed;
- Backup create/encrypted/restore validation;
- Recovery Mode;
- access-token prompt;
- command palette.

- [ ] **Step 4: Configure practical coverage thresholds**

Set 80% statements/lines/functions and 70% branches for `src/lib/api`, `src/lib/features`, and `src/lib/stores`. Exclude generated SvelteKit files and simple route wrappers. Do not game coverage with meaningless assertions.

- [ ] **Step 5: Run complete frontend suite**

Run:

```bash
bun --cwd apps/web run test --coverage
bun --cwd apps/web run check
bun --cwd apps/web run build
```

Expected: PASS and thresholds met.

- [ ] **Step 6: Commit**

```bash
git add apps/web bun.lock
git commit -m "test(web): cover critical Erabi UI states"
```
