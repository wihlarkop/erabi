# Erabi CI, End-to-End, and Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Package Erabi with unmodified Crawl4AI, enforce deterministic documentation/code/test gates, automate every canonical MVP journey, run explicit real-Crawl4AI smoke tests, and produce operator/recovery documentation for an MVP release candidate.

**Architecture:** PR CI uses deterministic local fixture websites and mock/wire-contract Crawl4AI paths for reproducibility. Real Crawl4AI is tested separately against those same local fixtures using an exact official image tag/digest recorded for the release candidate. Documentation/plan structure is machine-checked so coding agents cannot accidentally regain obsolete execution paths.

**Tech Stack:** GitHub Actions, Docker/Compose, Bun, Playwright, current stable accessibility tooling, Cargo/Rust toolchain, official Crawl4AI container image.

**Spec:** `docs/specs/08-ux-accessibility-and-verification.md`, `docs/specs/06-security-reliability-and-operations.md`, `docs/specs/07-exports-assets-retention-and-backups.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Global Constraints

- Tests MUST NOT depend on arbitrary public websites for correctness.
- PR CI must use deterministic local fixtures and mock/stub/contract Crawl4AI coverage.
- A release candidate must additionally pass an explicit real official Crawl4AI smoke run against local fixture sites.
- Docker Compose is the primary MVP distribution.
- Crawl4AI image is official and unmodified; record the exact release tag and resolved digest used for the release candidate.
- CI installs Cargo/Bun from committed lockfiles using frozen/locked behavior.
- All 22 required E2E journeys in the canonical spec are automated before MVP-complete status.
- Documentation links/placeholders/current-plan topology are checked in CI.
- No source/history/document may cause deleted July/reconciliation files to become an active current plan path.
- Release notes distinguish implemented MVP behavior from roadmap-only capabilities.

## Focused File Map

```text
docker/Dockerfile
docker/compose.yaml
tests/fixtures/web/
tests/fixtures/files/
tests/fixture-server.ts
tests/smoke/crawl4ai-smoke.ts
tests/e2e/*.spec.ts
playwright.config.ts
scripts/check-docs.ts
scripts/check-e2e-manifest.ts
tests/e2e/mvp-journeys.json
.github/workflows/ci.yml
.github/workflows/crawl4ai-smoke.yml
docs/operations/INSTALL.md
docs/operations/BACKUP-RESTORE.md
docs/operations/RECOVERY.md
docs/operations/REMOTE-ACCESS.md
docs/operations/CRAWL4AI.md
docs/operations/STORAGE.md
```

---

### Task 1: Build deterministic fixture server and production Docker/Compose packaging

**Files:**
- Create: `tests/fixture-server.ts`
- Create: `tests/fixtures/web/article.html`
- Create: `tests/fixtures/web/listing.html`
- Create: `tests/fixtures/web/detail-1.html`
- Create: `tests/fixtures/web/detail-2.html`
- Create: `tests/fixtures/web/pagination-1.html`
- Create: `tests/fixtures/web/pagination-2.html`
- Create: `tests/fixtures/web/ambiguous.html`
- Create: `tests/fixtures/web/schema-drift-v1.html`
- Create: `tests/fixtures/web/schema-drift-v2.html`
- Create: `tests/fixtures/web/malicious-preview.html`
- Create: `tests/fixtures/files/report.pdf`
- Create: `tests/fixtures/files/data.csv`
- Create: `docker/Dockerfile`
- Create: `docker/compose.yaml`
- Test: `tests/smoke/fixture-server.test.ts`

**Interfaces:**
- Fixture server provides deterministic routes for article/list/detail, cycles, ambiguity, external links, tracking-query duplicates, robots, 429/Retry-After, delayed/rendered content hooks, malicious preview, direct file MIME responses, and version-switchable schema drift.
- Compose provides `erabi`, `crawl4ai`, and fixture service/network for explicit test profiles while normal distribution exposes only required application services.

- [ ] **Step 1: Write failing fixture-server contract tests**

```ts
import { describe, expect, test } from 'bun:test';

const base = process.env.FIXTURE_BASE_URL!;

test('robots fixture is deterministic', async () => {
  const text = await fetch(`${base}/robots.txt`).then(r => r.text());
  expect(text).toContain('User-agent:');
  expect(text).toContain('Disallow: /blocked');
});

test('429 fixture includes Retry-After', async () => {
  const response = await fetch(`${base}/rate-limited`);
  expect(response.status).toBe(429);
  expect(response.headers.get('retry-after')).toBeTruthy();
});
```

Also test direct PDF/CSV responses have deterministic Content-Type; external-link fixture points to a second fixture hostname/domain alias; tracking variants resolve same underlying page; drift route can deterministically select v1/v2 via fixture control state rather than random time.

- [ ] **Step 2: Run RED**

Start fixture test command as defined in root scripts, e.g.:

```bash
bun test tests/smoke/fixture-server.test.ts
```

Expected: fail until fixture server/routes exist.

- [ ] **Step 3: Implement fixture server with Bun and no external network dependency**

Use `Bun.serve` with explicit route table and fixture files. Bind fixture server only to the test network/loopback. Keep fixture state reset endpoint available only in test mode. Every test must be able to reset state before execution.

Do not encode production logic into fixture server; it only supplies deterministic website behavior.

- [ ] **Step 4: Create multi-stage production Dockerfile**

Build frontend using frozen `bun.lock`; build Rust with `Cargo.lock`; final image contains `erabi` binary + static frontend and runs as a non-root user where platform supports it. Persist `/data` via mounted volume. Do not bake secrets into image layers.

Container healthcheck uses a safe health/readiness endpoint. Default command is `erabi serve`.

- [ ] **Step 5: Create Compose topology with official Crawl4AI image**

Before selecting the image, verify the currently supported stable official Crawl4AI Docker image/tag from upstream documentation. Record image **tag and resolved digest** in `docker/compose.yaml` comments or release metadata; do not invent a tag from memory.

Normal Compose behavior:

```text
erabi      → app/API/static UI, persistent data volume
crawl4ai   → official unmodified image, private Compose network
```

Fixture service is enabled only under a test/smoke profile. Do not expose Crawl4AI publicly by default unless required for operator debugging and explicitly configured.

- [ ] **Step 6: Run GREEN packaging/fixture checks and commit**

```bash
bun test tests/smoke/fixture-server.test.ts
docker compose -f docker/compose.yaml config
docker build -f docker/Dockerfile -t erabi:test .
```

Expected: fixture tests pass, Compose config validates, image builds.

```bash
git add docker tests/fixture-server.ts tests/fixtures
 git commit -m "build: package Erabi and deterministic website fixtures"
```

---

### Task 2: Add machine-enforced documentation, plan-topology, placeholder, and E2E-manifest checks

**Files:**
- Create: `scripts/check-docs.ts`
- Create: `scripts/check-e2e-manifest.ts`
- Create: `tests/e2e/mvp-journeys.json`
- Modify: `package.json`
- Test: `tests/smoke/docs-check.test.ts`

**Interfaces:**
- Produces root command `bun run check:docs`.
- Produces root command `bun run check:e2e-manifest`.
- `mvp-journeys.json` contains stable IDs `MVP-01` through `MVP-22` matching spec order.

- [ ] **Step 1: Write failing docs-topology test**

```ts
import { describe, expect, test } from 'bun:test';
import { readdirSync, existsSync } from 'node:fs';

test('repository exposes exactly one active MVP plan set', () => {
  const plans = readdirSync('docs/superpowers/plans').filter(x => x.endsWith('.md'));
  const numbered = plans.filter(x => /^2026-08-22-\d\d-/.test(x));
  expect(numbered).toHaveLength(10);
  expect(existsSync('docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md')).toBe(true);
  expect(existsSync('AGENTS.md')).toBe(true);
});
```

Add assertions current tree contains no file path matching `2026-07-22`, `public-spec-reconciliation`, or `docs/superpowers/specs/`; spec directory contains README + exactly `01-` through `08-` canonical specs; all active plans contain exact spec revision `679b499e617fcef14e4e40b9a7fc826b379b8a30`.

- [ ] **Step 2: Write failing Markdown-link and placeholder checker tests**

`check-docs.ts` walks tracked `.md` files (use `git ls-files '*.md'`) and:

- resolves relative Markdown links to repository files/anchors where practical;
- ignores external HTTP links for offline correctness unless a separate explicit external-link job is desired;
- rejects broken local file links;
- rejects `TBD`, `TODO`, `FIXME`, `implement later`, `fill in details`, and the old deleted-plan path patterns inside active spec/plan/AGENTS docs;
- rejects active plan references to global Schema lifecycle, fifth Batch run type, or old Inbox/Schemas primary navigation when they appear as implementation instructions rather than explicit anti-goals.

Keep false-positive exceptions explicit and small; do not make a checker that silently ignores all code blocks or all historical words.

- [ ] **Step 3: Create canonical 22-journey manifest and checker**

`tests/e2e/mvp-journeys.json`:

```json
[
  {"id":"MVP-01","title":"First-run Quick Scrape to Review"},
  {"id":"MVP-02","title":"Ordered pasted URL batch"},
  {"id":"MVP-03","title":"Direct file to Source/Asset"},
  {"id":"MVP-04","title":"Quick Scrape to Crawler Draft"},
  {"id":"MVP-05","title":"Multi-seed Page Types Test Lab Publish"},
  {"id":"MVP-06","title":"Page Type ambiguity blocks publish"},
  {"id":"MVP-07","title":"Equal specificity tie remains ambiguous"},
  {"id":"MVP-08","title":"Bounded cyclic Discovery Preview"},
  {"id":"MVP-09","title":"External URL remains outside scope"},
  {"id":"MVP-10","title":"Canonicalization prevents tracking duplicates"},
  {"id":"MVP-11","title":"Production SSE cancel recover resume"},
  {"id":"MVP-12","title":"Robots override reason lifecycle"},
  {"id":"MVP-13","title":"Listing detail shared Dataset without overwrite"},
  {"id":"MVP-14","title":"Schema drift requires Draft fix"},
  {"id":"MVP-15","title":"Duplicate candidates never auto-merge"},
  {"id":"MVP-16","title":"Complete snapshot missing candidate guard"},
  {"id":"MVP-17","title":"Approved field provenance trace"},
  {"id":"MVP-18","title":"Approved-only export provenance bundle"},
  {"id":"MVP-19","title":"Backup verify restore"},
  {"id":"MVP-20","title":"Tri-state setting inheritance"},
  {"id":"MVP-21","title":"Remote bind requires token"},
  {"id":"MVP-22","title":"Low-storage safety without auto-delete"}
]
```

`check-e2e-manifest.ts` verifies exactly 22 unique sequential IDs and that each ID appears in at least one `tests/e2e/*.spec.ts` test title/tag once Task 4 is complete.

- [ ] **Step 4: Run RED then implement checkers**

```bash
bun test tests/smoke/docs-check.test.ts
bun run check:docs
bun run check:e2e-manifest
```

Before E2E files exist, `check:e2e-manifest` is expected to fail with explicit missing IDs; document that this command becomes required CI only after Task 4. `check:docs` must pass once checker implementation is complete.

- [ ] **Step 5: Commit**

```bash
git add package.json bun.lock scripts tests/e2e/mvp-journeys.json tests/smoke/docs-check.test.ts
 git commit -m "test(docs): enforce canonical spec and plan topology"
```

---

### Task 3: Add deterministic PR CI gates

**Files:**
- Create: `.github/workflows/ci.yml`
- Modify: `package.json`
- Modify: `playwright.config.ts` when created by Task 4 bootstrap

**Interfaces:**
- PR/push CI validates Rust, frontend, migrations, unit/integration, docs topology, deterministic adapter/fixture tests, and E2E after Task 4.

- [ ] **Step 1: Define required CI jobs without network-dependent website tests**

Workflow jobs:

```text
docs
rust
frontend
persistence-and-backup
crawler-contract
playwright
```

Use official GitHub Actions releases pinned to stable major/full SHA according to repository security policy chosen at implementation. Install stable Rust and Bun; caches are optimization only, never correctness dependencies.

- [ ] **Step 2: Implement frozen/locked install and Rust gates**

Commands must include:

```bash
bun install --frozen-lockfile
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
bun run check:docs
bun --cwd apps/web run check
bun --cwd apps/web run test
bun --cwd apps/web run build
```

Migrations must be exercised from empty DB and each explicitly supported prior baseline represented by test fixtures/migration tests. Backup create/verify/restore tests run deterministically with temp directories.

- [ ] **Step 3: Run crawler contract/fixture gates**

Run mock `CrawlerAdapter`, Crawl4AI HTTP DTO/mapping wiremock tests, robots/rate/canonicalization/discovery fixtures, malicious preview fixture, direct-file fixture, and fixture server tests. Do not start real Crawl4AI in ordinary PR CI unless upstream image reproducibility/cost is explicitly accepted later; real smoke remains Task 5.

- [ ] **Step 4: Validate workflow syntax and commit**

Use an available local workflow linter when practical or at minimum YAML parser plus GitHub Actions schema-aware review. Then:

```bash
git add .github/workflows/ci.yml package.json bun.lock
 git commit -m "ci: enforce deterministic Erabi quality gates"
```

---

### Task 4: Automate all 22 canonical Playwright journeys

**Files:**
- Create: `playwright.config.ts`
- Create: `tests/e2e/fixtures.ts`
- Create: `tests/e2e/start.spec.ts`
- Create: `tests/e2e/crawler-studio.spec.ts`
- Create: `tests/e2e/discovery-runs.spec.ts`
- Create: `tests/e2e/review-provenance.spec.ts`
- Create: `tests/e2e/operations.spec.ts`
- Create: `tests/e2e/security-settings.spec.ts`
- Modify: `package.json`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Every canonical journey test title contains `[MVP-NN]` matching manifest.
- Test fixture starts/reset deterministic local website/Crawl4AI mock/application state per test or isolated worker.

- [ ] **Step 1: Add stable Playwright test dependency with Bun**

```bash
bun add -d @playwright/test
bunx playwright install --with-deps chromium
```

Use Chromium as the required CI browser for MVP unless spec later requires a wider matrix. Component/browser accessibility is supplemented with a current stable accessibility checker if selected; add it with Bun rather than copying scripts from a CDN.

- [ ] **Step 2: Create deterministic fixture harness**

`tests/e2e/fixtures.ts` supplies:

- Erabi base URL;
- fixture website base URL(s);
- reset DB/data helper or fresh temp data directory per worker;
- deterministic mock Crawl4AI mode for normal E2E;
- helper to wait on durable run status rather than arbitrary sleeps;
- helpers for direct-file, robots, schema-drift, low-storage test controls.

Do not use `waitForTimeout` as the primary synchronization mechanism. Wait on UI/API states/events.

- [ ] **Step 3: Implement MVP-01 through MVP-04 Start/intake journeys**

`start.spec.ts`:

```text
[MVP-01] fresh Start → one URL Quick Scrape → live progress → Review
[MVP-02] pasted URL batch → ordered independent Quick Scrape outcomes/run links
[MVP-03] direct file URL → Source/Asset handling, no extraction review
[MVP-04] successful Quick Scrape → Save as Crawler Draft
```

Assert one failed batch item does not roll back successful siblings and no Batch run type appears.

- [ ] **Step 4: Implement MVP-05 through MVP-10 Crawler Studio/discovery journeys**

`crawler-studio.spec.ts` + `discovery-runs.spec.ts`:

```text
[MVP-05] multi-Seed/multi-Page-Type Draft → Test Lab → Publish
[MVP-06] ambiguity blocks publish
[MVP-07] equal priority + complete specificity tie remains ambiguous after reversing creation/order
[MVP-08] cyclic Discovery Preview terminates at budget
[MVP-09] external URL preserved/outside Domain Scope and never fetched
[MVP-10] tracking-parameter variants canonicalize/dedupe
```

For MVP-07 create the competing Page Types in opposite order in two isolated crawlers and assert same Ambiguous decision/rationale set.

- [ ] **Step 5: Implement MVP-11 through MVP-17 run/review/provenance journeys**

```text
[MVP-11] Production Run → SSE progress → Cancel → checkpoint → recover/resume
[MVP-12] robots override missing reason blocked; valid reason frozen; same-run resume preserves; new run needs explicit reason
[MVP-13] Listing + Detail enrich shared Dataset without silent field overwrite
[MVP-14] production schema drift → diagnostic/non-complete → Draft fix/Test Lab/publish; no USE_ANYWAY
[MVP-15] duplicate/conflicting record candidates never auto-merge
[MVP-16] healthy complete snapshot creates MissingCandidate; partial snapshot does not
[MVP-17] Approved field provenance drawer traces Source/artifact/CrawlerVersion/Run/PageType/selector/raw/normalized
```

- [ ] **Step 6: Implement MVP-18 through MVP-22 operations/security/settings journeys**

```text
[MVP-18] Approved-only export + provenance ZIP verification
[MVP-19] backup → verify → restore → healthy state/data restored
[MVP-20] tri-state setting precedence Inherit/Custom/Reset across applicable layers with effective source
[MVP-21] non-loopback startup without access token is rejected; configured remote auth protects API/SSE
[MVP-22] simulated Critical storage blocks artifact-heavy work and does not auto-delete data
```

MVP-21 may combine CLI/process integration test with browser remote-auth behavior if binding a non-loopback interface is unreliable in CI; the Playwright manifest test must still reference verified integration evidence rather than fake success.

- [ ] **Step 7: Run manifest checker and complete E2E suite**

```bash
bun run check:e2e-manifest
bunx playwright test
```

Expected: checker reports all `MVP-01`..`MVP-22` covered exactly as required and Playwright exits 0.

- [ ] **Step 8: Add Playwright job to PR CI and commit**

CI archives Playwright report/test artifacts on failure without including secrets or arbitrary scraped public content. Then:

```bash
git add playwright.config.ts tests/e2e package.json bun.lock .github/workflows/ci.yml
 git commit -m "test(e2e): automate all Erabi MVP journeys"
```

---

### Task 5: Add explicit real Crawl4AI smoke workflow and release-candidate evidence

**Files:**
- Create: `tests/smoke/crawl4ai-smoke.ts`
- Create: `.github/workflows/crawl4ai-smoke.yml`
- Create: `docs/operations/CRAWL4AI.md`
- Test: local/CI smoke command defined in `package.json`

**Interfaces:**
- Produces explicit/manual and optionally scheduled workflow against official image digest.
- Produces smoke evidence for health, rendering, links, wait/scroll, screenshot, content type, representative errors.

- [ ] **Step 1: Write smoke assertions before real adapter run**

`crawl4ai-smoke.ts` points Crawl4AI only at local fixture server URLs and asserts normalized Erabi adapter output for:

```text
health/version reachable
basic rendered page
link discovery
wait-selector page
bounded auto-scroll/lazy fixture when supported by chosen official image
screenshot request
404/access/error mapping as fixture/network permits
non-HTML final Content-Type classification
```

The script records Crawl4AI image tag/digest and Erabi commit SHA in output metadata.

- [ ] **Step 2: Verify official image/tag/digest at execution time**

Fetch upstream official documentation/release source, choose a stable supported image, pull it, resolve immutable digest, and update Compose/workflow metadata. Do not use `latest` as the release evidence identity even if upstream documentation demonstrates it interactively.

- [ ] **Step 3: Run local real-container smoke**

```bash
docker compose -f docker/compose.yaml --profile smoke up -d --build
bun run smoke:crawl4ai
docker compose -f docker/compose.yaml --profile smoke down -v
```

Expected: all smoke assertions pass. Always tear down test volumes/services in CI `if: always()` cleanup.

- [ ] **Step 4: Implement manual/scheduled workflow**

`crawl4ai-smoke.yml` runs on `workflow_dispatch` and optionally a low-frequency schedule; it is required explicitly for a release candidate even if not a required PR check. Upload sanitized smoke evidence including exact image digest and failing fixture/result metadata.

- [ ] **Step 5: Commit**

```bash
git add docker/compose.yaml tests/smoke/crawl4ai-smoke.ts .github/workflows/crawl4ai-smoke.yml docs/operations/CRAWL4AI.md package.json bun.lock
 git commit -m "test(smoke): verify Erabi against official Crawl4AI"
```

---

### Task 6: Write operator/recovery documentation and enforce final MVP release gate

**Files:**
- Create: `docs/operations/INSTALL.md`
- Create: `docs/operations/BACKUP-RESTORE.md`
- Create: `docs/operations/RECOVERY.md`
- Create: `docs/operations/REMOTE-ACCESS.md`
- Create: `docs/operations/STORAGE.md`
- Modify: `README.md`
- Modify: `scripts/check-docs.ts`
- Test: documentation checker + full release commands

**Interfaces:**
- Produces operator documentation for installation, data directory, backups, integrity/Recovery Mode, remote token/CORS, Crawl4AI troubleshooting, and storage pressure.
- Defines one final release verification command/documented checklist.

- [ ] **Step 1: Write operator docs from implemented behavior only**

`INSTALL.md`: Docker Compose install, volume/data directory, localhost default, upgrade prerequisites.  
`BACKUP-RESTORE.md`: Database Only/Full, encryption behavior, verify-before-restore, password-loss warning, maintenance flow.  
`RECOVERY.md`: migration/integrity Recovery Mode, diagnostics, safe retry/restore, no destructive auto-repair.  
`REMOTE-ACCESS.md`: non-loopback requires `ERABI_ACCESS_TOKEN`, token storage behavior, CORS allowlist, OpenAPI remote opt-in.  
`STORAGE.md`: warning/critical behavior, retention preview, no automatic deletion.

Do not document scheduler, auth crawling, AI copilot, distributed workers, desktop app, or other roadmap features as implemented.

- [ ] **Step 2: Extend docs checker to require operator links and prohibit false implementation claims**

README links all required operator docs once they exist. `check-docs.ts` validates links and rejects roadmap-only feature claims in an explicit “implemented features” section unless a release manifest says they are implemented. Keep the rule narrow enough not to reject roadmap documents themselves.

- [ ] **Step 3: Run fresh final verification from clean checkout**

Required release gate:

```bash
bun install --frozen-lockfile
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
bun run check:docs
bun run check:e2e-manifest
bun --cwd apps/web run check
bun --cwd apps/web run test
bun --cwd apps/web run build
bunx playwright test
docker compose -f docker/compose.yaml config
```

Additionally require the target release candidate's explicit real-Crawl4AI smoke workflow to have passed for the exact candidate commit/image digest, and verify migration/backup/restore tests are included in the green Cargo suite/CI evidence.

- [ ] **Step 4: Record release-candidate evidence**

Create a release checklist/evidence record (for example `docs/operations/release-evidence/<version>.md`) containing candidate Git SHA, Erabi version, DB migration version, backup/export format versions, Crawl4AI image digest, commands/workflow run references, and pass/fail. Do not call MVP complete with an unchecked item.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/operations scripts/check-docs.ts
 git commit -m "docs: add Erabi operator and release verification guide"
```

## Plan 10 Gate — MVP Definition of Done

Erabi MVP may be called complete only when, from a clean candidate checkout:

1. all Rust tests, fmt, and clippy gates pass;
2. frontend check/unit/build gates pass;
3. documentation topology/link/placeholder checks pass;
4. all 22 `MVP-NN` Playwright journeys pass;
5. migration tests from every supported baseline pass;
6. backup create/verify/restore and integrity/recovery tests pass;
7. deterministic fixture/Crawl4AI HTTP contract tests pass;
8. explicit real official Crawl4AI smoke passes for the exact candidate commit and image digest;
9. Docker Compose validates and the packaged app starts healthy on localhost;
10. no current document exposes a second/obsolete implementation plan path or claims roadmap-only capabilities as implemented.
