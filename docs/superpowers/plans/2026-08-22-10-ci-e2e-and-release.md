# Erabi CI, End-to-End, and Release Implementation Plan

> **For agentic workers:** Build the CI/E2E/release enforcement as working infrastructure first, then run it, add or update meaningful regression checks where gaps are found, fix failures, and commit. Do not use failing-test-first or RED/GREEN sequencing by default.

**Goal:** Package Erabi with unmodified Crawl4AI, enforce deterministic CI/release gates, automate every canonical MVP journey, and publish operator/recovery documentation without claiming roadmap-only features.

**Architecture:** PR CI uses local deterministic website fixtures + mocked/stubbed Crawl4AI contract. Explicit/scheduled release smoke tests use the official Crawl4AI container against local fixture sites. Docker Compose is the primary distribution.

**Tech Stack:** GitHub Actions, Docker/Compose, Playwright, Cargo/Bun test suites, official Crawl4AI image.

**Spec:** `docs/specs/08-ux-accessibility-and-verification.md`, `docs/specs/06-security-reliability-and-operations.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

---

### Task 1: Docker packaging and deterministic fixture sites

**Files:** Dockerfiles/Compose, local fixture server/sites, fixture data, smoke scripts.

**Requirements:**

- Build Erabi server + static SPA for primary Docker Compose distribution.
- Run the official unmodified Crawl4AI image; do not fork/repackage its internals.
- Preserve loopback-first defaults and explicit remote access-token configuration.
- Fixture sites cover at least article, listing/detail shared identity, pagination cycle, PageType ambiguity, external links, tracking canonicalization, schema drift, direct files, 429/Retry-After, robots behavior, and malicious preview HTML.
- Fixtures are deterministic and local; correctness tests never depend on arbitrary public websites.

**Verification:** build images, start Compose, health/readiness, fixture reachability, mock/real adapter endpoint wiring.

---

### Task 2: PR CI gates

**Files:** `.github/workflows/` CI definitions and scripts.

**Required gates:**

```text
frozen Cargo/Bun install
cargo fmt --check
cargo clippy -D warnings
cargo test --workspace
frontend check/tests
Playwright MVP suite
migration-from-supported-baseline checks
backup/restore verification
Crawl4AI adapter contract fixtures
documentation topology/link/placeholder checks
```

**Documentation enforcement:**

- current tree must expose exactly one active MVP plan index and ten numbered active subsystem plans;
- no `2026-07-22` active plan/design paths or temporary reconciliation docs may reappear;
- active plan/spec links must resolve;
- active plans must reference canonical spec revision `679b499e617fcef14e4e40b9a7fc826b379b8a30`;
- implementation plans must not require TDD/RED-GREEN sequencing by default; `AGENTS.md` implementation-first rule is authoritative;
- scan active docs for placeholders such as unresolved `TBD`/`TODO` where they would make implementation ambiguous.

**Verification:** run workflow-equivalent commands locally where possible and validate GitHub Actions syntax/configuration.

---

### Task 3: Automate all 22 canonical Playwright MVP journeys

Create stable fixtures/helpers and automate every journey from `docs/specs/08-ux-accessibility-and-verification.md`:

1. first-run Start → Quick Scrape → Review;
2. pasted URL batch → independent ordered Quick Scrape outcomes;
3. direct file URL → Source/Asset handling without HTML extraction;
4. Quick Scrape → Save as Crawler Draft;
5. multi-Seed / multi-PageType Draft → Test Lab → Publish;
6. PageType ambiguity blocks publish;
7. equal-priority/equal-specificity tie remains `AMBIGUOUS_PAGE_TYPE` regardless of ordering;
8. bounded cyclic Discovery Preview;
9. external URL stays outside Domain Scope;
10. tracking canonicalization prevents duplicate crawling;
11. Production Run → live SSE → Cancel → recover/resume;
12. robots override requires reason; same-run retry/resume preserves it; new run requires explicit reason;
13. Listing + Detail enrich shared Dataset without silent overwrite;
14. schema drift produces diagnostics, blocks trusted complete semantics, and requires Draft fix;
15. duplicate candidates never auto-merge;
16. healthy complete snapshot creates missing candidates, partial snapshot does not;
17. provenance traces approved value to source/artifact/version;
18. approved-only export + provenance bundle verification;
19. backup → verify → restore;
20. setting inheritance distinguishes Inherit/Custom/Reset across Global → Collection → Crawler → RunProfile → per-run;
21. remote bind is rejected without access token;
22. low-storage safety blocks artifact-heavy work without auto-deletion.

**Requirements:** use stable selectors/accessibility roles/data contracts, deterministic local fixtures, and reusable setup helpers without hiding product assertions inside generic abstractions.

**Verification:** the complete Playwright suite passes repeatedly against a clean deterministic local environment.

---

### Task 4: Real official Crawl4AI release smoke

**Requirements:**

- Start the exact official Crawl4AI image selected for the release candidate.
- Test rendering, links, waits/scroll behavior required by supported MVP paths, screenshots where configured, content types/direct-file behavior, and upstream error normalization against local fixtures.
- Record image version/digest and Erabi release-candidate identity.
- Do not call a release MVP-complete if only the mock adapter passes.

**Verification:** explicit release smoke command produces a durable pass/fail summary and fails non-zero on contract violations.

---

### Task 5: Operator, recovery, and release documentation

**Files:** current user/operator docs, recovery/runbook/release notes as appropriate.

**Document:** installation, Docker Compose, data directory, environment/secrets, backup/verify/restore, integrity/Recovery Mode, remote bind/token, Crawl4AI configuration/troubleshooting, storage pressure/retention, export destinations, update/migration expectations.

Release notes must distinguish implemented MVP from roadmap-only capabilities and must not promise features that fail the release gate.

**Verification:** documentation links/topology/placeholders plus command examples against the release candidate.

---

## Plan 10 / MVP Release Gate

From a clean checkout/release-candidate state, all applicable commands must pass:

```bash
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
bun install --frozen-lockfile
bun --cwd apps/web run check
bun --cwd apps/web run test
# project-defined Playwright command
# migration baseline verification
# backup/restore verification
# fixture Crawl4AI contract suite
# explicit real official Crawl4AI smoke
# documentation topology/link/placeholder checks
```

Do not call Erabi MVP-complete until all 22 Playwright journeys pass, real Crawl4AI smoke passes for the exact release candidate, Docker Compose is healthy, migration/backup/recovery gates pass, documentation is consistent, and no roadmap-only capability is misrepresented as implemented.
