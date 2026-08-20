# Erabi Docker, End-to-End Verification, and Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Package Erabi with the official Crawl4AI image, verify complete workflows with Playwright and real-container smoke tests, enforce CI/security policy, and finish release and operator documentation.

**Architecture:** Docker Compose runs one Erabi container and one unmodified official Crawl4AI container with persistent volumes and localhost-only default binding. Deterministic PR tests use a mock Crawl4AI service, scheduled smoke tests use the official image, and the release gate verifies the complete MVP contract.

**Tech Stack:** Docker Compose, Rust release build, Bun production build, Playwright, GitHub Actions-compatible CI, cargo audit/deny tooling, SemVer.

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

- **Depends on:** [11 Accessibility and Product Polish](./11-accessibility-and-product-polish.md).
- **Produces:** Deployable Docker release, deterministic E2E suite, scheduled Crawl4AI compatibility smoke tests, CI policy, release metadata, and final user/operator documentation.
- **Gate:** Release gate: clean checkout build, frozen dependency install, full Rust/frontend/E2E suite, Docker health, smoke tests, security checks, and documented MVP acceptance checklist pass.
- **Execution order:** Complete every task in this file in numerical order and commit after each task. Do not begin the next plan until this gate passes.

## Focused File Map

```text
docker/Dockerfile
docker/compose.yaml
.env.example
tests/e2e/
tests/smoke/
tests/fixtures/websites/
.github/workflows/
docs/operations/
docs/api/
README.md
LICENSE
SECURITY.md
CONTRIBUTING.md
```

---

### Task 48: Package Erabi and Official Crawl4AI with Docker Compose

**Files:**
- Create: `docker/Dockerfile`
- Create: `docker/compose.yaml`
- Create: `docker/entrypoint.sh`
- Create: `scripts/resolve-crawl4ai-image.ts`
- Create: `docker/crawl4ai-image.env`
- Modify: `.env.example`
- Modify: `README.md`
- Test: `tests/smoke/docker-compose.sh`

**Interfaces:**
- Produces: primary MVP installation with `docker compose --env-file .env -f docker/compose.yaml up -d`.
- Produces: two services, `erabi` and unmodified official `crawl4ai`.
- Persists: database, artifacts, assets, exports, and backups under one mounted data root.

- [ ] **Step 1: Create a deterministic stable Crawl4AI image resolver**

Create `scripts/resolve-crawl4ai-image.ts` that:

1. fetches the latest non-draft, non-prerelease GitHub release for `unclecode/crawl4ai`;
2. removes a leading `v` from the tag;
3. rejects tags containing `alpha`, `beta`, `rc`, `pre`, or other hyphenated prerelease suffixes;
4. runs `docker buildx imagetools inspect unclecode/crawl4ai:<version>`;
5. extracts the manifest-list digest;
6. writes exactly `CRAWL4AI_IMAGE=unclecode/crawl4ai:<version>@sha256:<digest>` to `docker/crawl4ai-image.env`;
7. exits non-zero if no stable digest can be resolved.

Use native `fetch` and `Bun.spawn`; do not require npm, jq, or a custom Docker image.

- [ ] **Step 2: Run the resolver and review the pinned result**

Run:

```bash
bun scripts/resolve-crawl4ai-image.ts
cat docker/crawl4ai-image.env
```

Expected: one exact version-and-digest line, never `latest` and never a prerelease tag.

- [ ] **Step 3: Create the multi-stage Erabi Dockerfile**

Stages:

1. Bun stable image installs from `bun.lock` and builds `apps/web`;
2. Rust stable image builds `erabi` release binary from `Cargo.lock`;
3. minimal Debian runtime installs only CA certificates and runtime essentials;
4. copies `erabi`, web `build/`, migrations if not embedded, and entrypoint;
5. creates non-root `erabi` user;
6. exposes 7878 internally;
7. healthcheck calls `/api/v1/system/health` without exposing sensitive detail.

Use BuildKit cache mounts but ensure a clean build succeeds without cache.

- [ ] **Step 4: Create Compose configuration**

`docker/compose.yaml` must include:

```yaml
services:
  erabi:
    build:
      context: ..
      dockerfile: docker/Dockerfile
    env_file:
      - ../.env
    ports:
      - "127.0.0.1:${ERABI_PORT:-7878}:7878"
    volumes:
      - ../data:/data
    depends_on:
      crawl4ai:
        condition: service_healthy
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "/usr/local/bin/erabi", "doctor", "--healthcheck"]
      interval: 10s
      timeout: 3s
      retries: 12

  crawl4ai:
    image: ${CRAWL4AI_IMAGE}
    shm_size: 1gb
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-fsS", "http://127.0.0.1:11235/health"]
      interval: 10s
      timeout: 5s
      retries: 18
```

Merge `docker/crawl4ai-image.env` when invoking Compose. Do not mount modified Crawl4AI source or patch its container.

- [ ] **Step 5: Implement graceful dependency behavior**

Although Compose waits for initial health, Erabi must still start and remain usable when Crawl4AI later becomes unavailable. The UI shows crawler unavailable and disables Scrape; old data/review/export/backup remain usable.

- [ ] **Step 6: Write and run Docker smoke test**

`tests/smoke/docker-compose.sh` must build, start, wait for health, assert Start page and health endpoint, stop Crawl4AI and assert Erabi remains up with crawler unavailable, then cleanly `down` without deleting data volume.

Run:

```bash
bash tests/smoke/docker-compose.sh
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add docker scripts/resolve-crawl4ai-image.ts .env.example README.md tests/smoke/docker-compose.sh
git commit -m "feat(deploy): package Erabi with pinned Crawl4AI"
```
### Task 49: Add Playwright End-to-End Tests with a Deterministic Mock Crawl4AI Server

**Files:**
- Create: `playwright.config.ts`
- Create: `tests/fixtures/websites/server.ts`
- Create: `tests/fixtures/crawl4ai/server.ts`
- Create: `tests/e2e/helpers.ts`
- Create: `tests/e2e/start-review-export.spec.ts`
- Create: `tests/e2e/cancel-resume.spec.ts`
- Create: `tests/e2e/schema-drift.spec.ts`
- Create: `tests/e2e/backup-recovery.spec.ts`
- Create: `tests/e2e/accessibility.spec.ts`
- Modify: `package.json`
- Modify: `bun.lock`

**Interfaces:**
- Produces: PR-safe end-to-end suite independent of public websites and real Crawl4AI.
- Exercises: complete main user journeys through browser, API, Turso, jobs, and filesystem.

- [ ] **Step 1: Add Playwright using Bun**

Run:

```bash
bun add -d @playwright/test axe-core
bunx playwright install chromium
```

Use Chromium for PR E2E to control runtime. Browser matrix expansion is optional after MVP.

- [ ] **Step 2: Configure isolated test data per worker**

`playwright.config.ts` starts:

- local deterministic website fixture server;
- mock Crawl4AI HTTP server;
- Erabi process with temporary `ERABI_DATA_DIR`, localhost bind, no auth, and mock crawler URL;
- web UI served by Axum.

Use one E2E worker initially because the MVP uses one local data-directory lock. Each test resets database/artifacts through a test-only process restart, never through a production reset endpoint.

- [ ] **Step 3: Implement the golden journey test**

`start-review-export.spec.ts` must:

1. open Start;
2. paste fixture URL;
3. observe live progress;
4. auto-enter Records Review;
5. verify visual source highlight and provenance;
6. edit one Draft field and wait for Saved;
7. approve all valid while one invalid remains Draft;
8. export approved CSV with provenance;
9. download ZIP;
10. inspect ZIP names through Node/Bun helper;
11. verify manifest counts and sidecar.

- [ ] **Step 4: Implement recovery and safety journeys**

- Cancel/resume from checkpoint without duplicate records.
- Partial pagination cannot produce missing candidates.
- Complete recrawl creates new/updated/missing candidates and no-change run creates no Review.
- Schema drift blocks normal application and offers review/use/cancel.
- Backup encrypted roundtrip and wrong password leaves current data unchanged.
- Recovery Mode exposes diagnostics/restore but blocks mutation.

- [ ] **Step 5: Implement accessibility E2E**

Run axe on Start, progress, extraction editor, Review, Assets, Exports, Settings, and Recovery Mode. Add keyboard-only journey through URL submit, log expand, DOM tree field selection, Grid cell edit, approval, command palette, and close Review.

- [ ] **Step 6: Run E2E**

Run: `bunx playwright test`

Expected: all tests PASS without external network access.

- [ ] **Step 7: Commit**

```bash
git add playwright.config.ts tests/e2e tests/fixtures package.json bun.lock
git commit -m "test(e2e): verify complete Erabi workflows"
```
### Task 50: Add Scheduled Smoke Tests Against the Real Official Crawl4AI Container

**Files:**
- Create: `tests/fixtures/websites/static/article.html`
- Create: `tests/fixtures/websites/static/products.html`
- Create: `tests/fixtures/websites/static/pagination-1.html`
- Create: `tests/fixtures/websites/static/pagination-2.html`
- Create: `tests/fixtures/websites/static/lazy.html`
- Create: `tests/smoke/real-crawl4ai.spec.ts`
- Create: `docker/compose.smoke.yaml`

**Interfaces:**
- Produces: scheduled compatibility checks for the pinned Crawl4AI image.
- Covers: static HTML, JavaScript rendering, pagination, lazy loading, screenshot, adapter mapping.

- [ ] **Step 1: Build local-only fixture website scenarios**

The server must expose deterministic pages with known content and no internet dependency. `lazy.html` uses local JavaScript to append content on scroll. Pagination uses rel=next and numbered links. Include an image asset for screenshot/asset detection.

- [ ] **Step 2: Create real-service smoke Compose overlay**

Start Erabi, the pinned official Crawl4AI image, and the fixture server on an isolated Docker network. The fixture server is reachable by Crawl4AI; it is not public.

- [ ] **Step 3: Write the smoke specification**

Assert:

- health and version captured;
- static article returns raw/rendered/Markdown;
- JS/lazy content appears after configured wait/scroll;
- screenshot artifact is valid PNG;
- pagination candidate is detected and confirmed;
- Records extraction returns expected count/values;
- access token/JWT needed by the selected Crawl4AI version is correctly configured by environment, never hard-coded.

- [ ] **Step 4: Run the real smoke test locally once**

Run:

```bash
docker compose --env-file docker/crawl4ai-image.env -f docker/compose.yaml -f docker/compose.smoke.yaml up -d --build
bunx playwright test tests/smoke/real-crawl4ai.spec.ts
docker compose --env-file docker/crawl4ai-image.env -f docker/compose.yaml -f docker/compose.smoke.yaml down
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/websites/static tests/smoke/real-crawl4ai.spec.ts docker/compose.smoke.yaml
git commit -m "test(smoke): verify real Crawl4AI compatibility"
```
### Task 51: Add CI, Dependency/Security Checks, Licensing, and SemVer Release Metadata

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/crawl4ai-smoke.yml`
- Create: `.github/dependabot.yml`
- Create: `deny.toml`
- Create: `LICENSE`
- Create: `SECURITY.md`
- Create: `CONTRIBUTING.md`
- Create: `docs/operations/release.md`
- Create: `crates/erabi-cli/src/version.rs`
- Modify: `README.md`

**Interfaces:**
- Produces: frozen-lockfile CI, real smoke schedule, audit/license checks, version diagnostics.
- Establishes: Apache-2.0 and Semantic Versioning.
- Does not establish DCO/CLA in MVP.

- [ ] **Step 1: Add Rust security/license tools to CI installation**

CI installs stable `cargo-audit` and `cargo-deny`. Do not make them runtime dependencies. Configure `deny.toml` to allow Apache-2.0, MIT, BSD-2/3-Clause, ISC, Unicode, Zlib, and other specifically reviewed permissive licenses; deny unlicensed, copyleft-incompatible, yanked, and duplicate-risk crates according to documented exceptions.

- [ ] **Step 2: Implement pull-request CI**

Run jobs for:

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo build --workspace --release
cargo audit
cargo deny check
bun install --frozen-lockfile
bun --cwd apps/web run check
bun --cwd apps/web run test --coverage
bun --cwd apps/web run build
bunx playwright install --with-deps chromium
bunx playwright test
Docker image build
```

Cache registries/build outputs without bypassing lockfile verification.

- [ ] **Step 3: Implement scheduled real Crawl4AI smoke workflow**

Run nightly or weekly, manually triggerable, using the committed pinned image. Upload logs/traces on failure, but run diagnostic redaction before artifact upload. Do not expose `.env` contents or scraped values.

- [ ] **Step 4: Add Apache-2.0 and simple contributor/security documents**

Use the canonical Apache License 2.0 text. `CONTRIBUTING.md` explains Cargo/Bun commands, stable-dependency rule, TDD, specs/plans, and no DCO/CLA yet. `SECURITY.md` gives private reporting instructions without promising unsupported response times.

- [ ] **Step 5: Implement version reporting**

Expose application SemVer, API version `v1`, database schema version, backup format version, export manifest version, Rust version at build, Turso crate version when obtainable, Crawl4AI health/version, and OS. Build metadata must not change API compatibility behavior.

- [ ] **Step 6: Document release rules**

- `0.1.0` MVP;
- minor releases may contain documented breaking changes during `0.x` with migration/compatibility notes;
- patch releases are compatible fixes;
- after 1.0, deprecate before removal and remove only in a major release;
- no automatic app updates;
- Docker users update with pull/up;
- images are tagged versions, never deployment `latest`.

- [ ] **Step 7: Run the CI command set locally**

Run every command from Step 2. Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add .github deny.toml LICENSE SECURITY.md CONTRIBUTING.md docs/operations/release.md crates/erabi-cli README.md
git commit -m "ci: secure and version Erabi releases"
```
### Task 52: Run the Full MVP Acceptance Gate and Finalize Operator/User Documentation

**Files:**
- Create: `docs/operations/install.md`
- Create: `docs/operations/configuration.md`
- Create: `docs/operations/backup-restore.md`
- Create: `docs/operations/recovery-mode.md`
- Create: `docs/operations/security.md`
- Create: `docs/api/overview.md`
- Create: `docs/mvp-acceptance-report.md`
- Modify: `README.md`
- Modify: `.env.example`

**Interfaces:**
- Produces: a verified release candidate and reproducible acceptance report.
- Confirms: every frozen MVP requirement has implementation and evidence.

- [ ] **Step 1: Create the acceptance matrix before the final run**

`docs/mvp-acceptance-report.md` must list every requirement from all approved specs with:

- requirement ID;
- implementation task/file;
- automated test name;
- manual verification where necessary;
- result and evidence command.

No requirement may be marked complete without a specific test or inspection.

- [ ] **Step 2: Verify fresh installation**

On a clean data directory:

```bash
cp .env.example .env
docker compose --env-file .env --env-file docker/crawl4ai-image.env -f docker/compose.yaml up -d --build
```

Verify Local Turso/database creation, migrations, artifact directories, Crawl4AI health, Start page, no wizard, and local-only port binding.

- [ ] **Step 3: Execute the complete product acceptance journey**

Verify a fresh user can:

1. paste a public/local fixture URL;
2. see robots/rate-limit/progress behavior;
3. cancel/resume safely;
4. detect Document/Records mode and switch without recrawl;
5. select container/fields visually and by keyboard;
6. inspect field provenance;
7. edit Draft/autosave;
8. approve valid while invalid remains Draft;
9. reject single/bulk with reason rules;
10. close unresolved Review with special status;
11. export each format and provenance ZIP;
12. download selected assets safely;
13. recrawl and see only meaningful changes;
14. get no Review for no-change;
15. create/verify/restore encrypted backup;
16. survive Crawl4AI outage;
17. enter and use Recovery Mode after a controlled integrity failure;
18. receive disk-pressure safety stop without deletion;
19. use metadata search/command palette;
20. operate keyboard-only at 200% zoom.

- [ ] **Step 4: Verify destructive and security boundaries**

Test non-loopback startup without token fails, wrong token rate limits, CORS/Origin/Host/media-type rejection, CSP, preview script isolation, path traversal downloads, permanent deletion impact confirmation, Trash restore, no secret in DB/log/diagnostic/backup manifest, and OpenAPI disabled by default on network.

- [ ] **Step 5: Verify shutdown and restart recovery**

Start active crawl/export/backup jobs, send termination, measure process exit at no more than three seconds, restart, and verify jobs are recoverable/consistent with no approved data corruption or duplicate records.

- [ ] **Step 6: Write complete operator documentation**

Document exact installation, `.env` fields, local/network exposure, Crawl4AI connection/token, data directories, update procedure, settings inheritance, backup types/encryption/password warning, restore, Recovery Mode, diagnostics, retention, OpenAPI, and security limitations. Clearly separate MVP from roadmap.

- [ ] **Step 7: Run final verification commands**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo build --workspace --release
cargo audit
cargo deny check
bun install --frozen-lockfile
bun --cwd apps/web run check
bun --cwd apps/web run test --coverage
bun --cwd apps/web run build
bunx playwright test
bash tests/smoke/docker-compose.sh
```

Then run the real Crawl4AI smoke command from Task 50.

Expected: every command PASS; acceptance report has no failed or unverified frozen-MVP requirement.

- [ ] **Step 8: Commit the release-ready documentation and evidence**

```bash
git add README.md .env.example docs/operations docs/api docs/mvp-acceptance-report.md
git commit -m "docs: finalize Erabi MVP operations and acceptance"
```

---

## Specification Coverage Matrix

| Approved specification | Primary implementation tasks |
|---|---|
| Product scope, Start, navigation, recent activity, naming | 3, 6, 21, 41, 45 |
| English-first, theme, accessibility, notifications | 41–47, 49, 52 |
| Rust modular monolith and dependency policy | 1–3, 14–20, 48, 51 |
| Official Turso application persistence and migrations | 10–11, 15, 39–40 |
| API v1, error envelope, idempotency, optimistic concurrency | 14, 18, 21, 28, 32 |
| Durable queue, leases, panic isolation, cancellation, SSE | 16–18, 23–24, 42 |
| Crawl4AI boundary and real compatibility | 19–20, 48–50 |
| Safe crawling, pagination, partial/retry/resume | 22–24, 49–50 |
| Raw artifacts, retention, storage pressure | 12, 25, 38, 40 |
| Visual extraction, mode detection, Schema Versions/drift | 26–29, 43 |
| Validation, immutable approval, diff, Review lifecycle | 31–32, 44 |
| Field-level provenance | 30, 35, 44 |
| Assets and downloaded-file safety | 12, 33, 35, 45 |
| File/SQLite/Turso exports and atomic destination publish | 34–37, 45 |
| Archive, Trash, permanent deletion, export history | 38, 45 |
| Backup, encryption, restore, Recovery Mode | 15, 39–40, 45, 49 |
| Security, CORS, CSP, OpenAPI, redacted tracing | 8–9, 13–15, 40, 48, 51–52 |
| Docker Compose, manual updates, SemVer | 48, 51–52 |
| Roadmap exclusions | Enforced throughout; no task implements deferred features |

## Deliberately Deferred Items

Do not add these while executing this plan:

- Source movement between Collections;
- Schema JSON import/export;
- custom export filename;
- Regenerate Export;
- Undo/Redo and persistent Draft history;
- in-app Notification Center;
- CSV/JSON/JSONL file ingestion, sitemap, RSS/Atom;
- full-text record search;
- schedules and automatic crawl;
- authenticated browser/session/action workflows;
- file parsing/OCR;
- Append/Upsert database exports;
- PostgreSQL/MySQL/S3/R2/vector/RAG connectors;
- optional AI assistance;
- generated frontends or assistants;
- Tauri desktop installer;
- accounts, teams, hosted SaaS;
- multi-instance/distributed workers;
- DCO/CLA governance;
- automatic software updates.

## Final Implementation Discipline

- Use a clean worktree before Task 1 when implementation begins.
- Run the targeted test before and after each implementation change.
- Do not combine multiple task commits unless a reviewer explicitly requests it.
- Keep every route handler thin and every SQL statement in `erabi-db`.
- Keep Crawl4AI DTOs private to `erabi-crawl4ai`.
- Treat every crawled byte and downloaded file as untrusted.
- Never weaken an invariant merely to make an E2E test pass.
- When the exact current stable upstream API differs from a code snippet in this plan, preserve the contract and behavior, use `cargo add`/`bun add` for the latest stable release, consult official documentation, and update the focused adapter implementation plus tests in the same commit.
