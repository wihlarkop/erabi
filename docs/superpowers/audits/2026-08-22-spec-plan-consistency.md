# Erabi Specification and Plan Consistency Audit

**Date:** 2026-08-22  
**Canonical public-spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`  
**Active plan index:** `docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md`  
**Audit branch:** `audit/spec-corrections-2026-08-22`

## Result

**PASS — no unresolved current-document contradiction remains in the audited correction scope, and the active implementation plan set has been hardened for deterministic Codex/Superpowers execution.**

The current working tree intentionally exposes one canonical public specification and one active ten-plan implementation sequence. Superseded July design/plan files and temporary reconciliation documents are absent from the working tree.

## 1. Reconciliation acceptance criteria

| # | Requirement | Result | Evidence / current contract |
|---|---|---|---|
| 1 | Quick Scrape is not single-page-only while E2E requires pasted batches | FIXED | One URL remains default; bounded pasted batch is an envelope creating independent `QUICK_SCRAPE` runs. |
| 2 | Source has one Crawler-compatible definition | FIXED | Source is durable input/history identity; Crawler remains reusable design center; Seeds remain explicit versioned config. |
| 3 | Page Type tie resolution is deterministic | FIXED | Priority then explicit specificity key; complete tie is `AMBIGUOUS_PAGE_TYPE`; no insertion/DB/UUID tie-break. |
| 4 | Robots override cannot exist without stored reason | FIXED | Non-empty reason required before creation/resume; snapshot+audit retain reason/actor/time/scope/User-Agent/version context. |
| 5 | Setting inheritance is explicit tri-state with full precedence | FIXED | `INHERIT`, `CUSTOM(value)`, `RESET_TO_BUILT_IN`; per-run → Run Profile → Crawler → Collection → Global → built-in. |
| 6 | Direct-file URLs have a defined non-HTML path | FIXED | Confident non-HTML direct files use Source/Asset intake and do not enter HTML extraction. |
| 7 | `SCHEMA_DRIFT` cannot bypass production trust semantics | FIXED | Production-breaking drift prevents trusted complete/missing semantics and requires new Draft/test/publish correction. |
| 8 | Destination DB separation remains intact | PASS | Internal Erabi DB remains separate from SQLite/Turso export destinations; atomic typed publication retained. |
| 9 | Current tree exposes one implementation path to agents | PASS | July/reconciliation docs are absent; `AGENTS.md` points to one spec index and one active plan index. |
| 10 | Active plans reference exact corrected spec SHA | PASS | Plan index and all ten subsystem plans use `679b499e617fcef14e4e40b9a7fc826b379b8a30`. |
| 11 | Required MVP E2E journeys map to plan tests | PASS | Active Plan 10 Task 4 implements a machine-checked `MVP-01` through `MVP-22` Playwright manifest. |
| 12 | Terminology audit has no unresolved role conflict | PASS | Canonical role matrix below is consistent across current spec and active plans. |
| 13 | Plan tasks are executable rather than outline-only | FIXED | All ten plans now state focused files/interfaces, RED test step, RED command, implementation contract, GREEN command, commit boundary, and plan gate. |
| 14 | Persistence migration ownership is non-overlapping | PASS | Active plan index reserves `0001`–`0007` by bounded subsystem; later changes are additive migrations. |
| 15 | CI will prevent obsolete plan topology from returning | PASS | Plan 10 Task 2 requires a docs/topology checker for active plan count, spec revision, deleted-path patterns, broken links, and placeholders. |

## 2. Canonical terminology matrix

| Concept | Canonical role | Must not become |
|---|---|---|
| Crawler | Primary reusable crawling/extraction design object | A single execution or a Source alias |
| Crawler Version | Immutable Published config or editable Draft | Mutable Published state |
| Source | Durable target/history identity for web/direct-file inputs | Replacement for Crawler/Seed/Page Type/Dataset/Run |
| Seed | Versioned Crawler entry URL/config | Automatically rewritten from Source metadata |
| Page Type | Structural/semantic page class with matcher, extraction, validation, identity and Dataset mapping | Generic global schema/resource type |
| Extraction configuration | Page-Type-owned behavior inside Crawler Version | Independently approved global Schema subsystem |
| Discovery Transition | Directed Page Type → Page Type discovery behavior with budgets/provenance | Unbounded implicit traversal |
| Run Profile | Reusable operational overrides only | Semantic Crawler Version override |
| Crawl Run | Immutable execution snapshot/history | Mutable pointer that adopts later settings |
| Test Evidence | Durable confidence/diagnostic evidence | Production approval |
| Dataset | Curated structured output and record-version context | Internal application DB table namespace |

## 3. Run semantics and safety audit

**PASS.** Current documents/plans use exactly:

```text
QUICK_SCRAPE
TEST_RUN
DISCOVERY_PREVIEW
PRODUCTION_RUN
```

A pasted URL batch is not a fifth run type. Each accepted item has its own Quick Scrape run, immutable snapshot, status, artifacts, provenance, cancellation/retry state, and review outcome.

Normal Production Runs require Published Crawler Versions. Test Run and Discovery Preview may operate on Drafts. Only a healthy complete Production Run can create `MISSING_CANDIDATE` records.

Safety/settings contracts are consistently carried through domain, API, execution, UI, and E2E plans:

- deterministic Page Type specificity with complete-tie ambiguity;
- robots respected by default plus explicit non-empty override reason;
- no silent reuse of a prior independent run's reason;
- `INHERIT` / `CUSTOM(value)` / `RESET_TO_BUILT_IN` settings semantics;
- full operational precedence including Run Profile and Crawler defaults;
- direct-file non-HTML routing;
- production schema drift blocking trusted complete/missing semantics;
- loopback default and access token for non-loopback bind;
- no telemetry by default;
- internal DB/export destination separation.

## 4. Plan execution-hardening audit

Each active subsystem plan now follows the same agent execution pattern:

```text
read canonical spec
→ write explicit failing test
→ run RED command
→ implement bounded contract
→ run GREEN command
→ commit
→ complete plan-wide gate from clean checkout
```

Cross-plan boundaries are explicit:

- Plan 01 owns domain names/types/workspace shape.
- Plan 02 owns settings, run snapshots, core Turso repositories, migrations `0001`–`0003`, and atomic artifact persistence.
- Plan 03 owns bootstrap/network security, errors/audit/redaction, Recovery Mode, and three-second shutdown.
- Plan 04 owns durable jobs/SSE/checkpoints and migration `0004`.
- Plan 05 owns Crawler Studio semantic services, canonicalization/scope/discovery/Test Lab/Preview/publish health and remains Crawl4AI-neutral through a provider port.
- Plan 06 owns Crawl4AI adapter, Quick Scrape/direct-file/robots/rate/execution and migration `0005`.
- Plan 07 owns Page-Type extraction, drift/review/candidates/provenance and migration `0006`.
- Plan 08 owns assets/exports/destinations/retention/backup/integrity and migration `0007`.
- Plan 09 owns the complete accessible SvelteKit product UI and does not reimplement backend domain decisions client-side.
- Plan 10 owns deterministic fixtures, docs topology enforcement, CI, all 22 E2E journeys, real Crawl4AI smoke, operator docs, and release evidence.

No active plan relies on an independently approved global Schema entity, an Inbox-first product model, or a Batch Crawl Run type.

## 5. Required MVP E2E mapping

All 22 journeys in `docs/specs/08-ux-accessibility-and-verification.md` map to Plan 10 Task 4, with earlier implementation/test coverage as follows:

| Journey | Primary implementation/test plan |
|---|---|
| First-run Start → Quick Scrape → Review | 06 Task 3, 09 Task 1, 10 Task 4 |
| Pasted batch → independent ordered Quick Scrapes | 06 Task 3, 09 Tasks 1–2, 10 Task 4 |
| Direct file → Source/Asset, no HTML extraction | 06 Task 2, 08 Task 1, 09 Task 1, 10 Task 4 |
| Quick Scrape → Save as Crawler Draft | 05 Task 1, 06 Task 3, 09 Task 3, 10 Task 4 |
| Multi-Seed/multi-Page-Type Draft → Test Lab → Publish | 05 Tasks 1–2,5–6; 09 Tasks 3–4; 10 Task 4 |
| Page Type ambiguity blocks publish | 01 Task 4, 05 Tasks 2/6, 09 Tasks 3–4, 10 Task 4 |
| Equal specificity tie independent of ordering | 01 Task 4, 05 Task 2, 10 Task 4 |
| Bounded cyclic Discovery Preview | 05 Tasks 4–5, 09 Task 4, 10 Task 4 |
| External URL stays outside Domain Scope | 05 Tasks 3–4, 10 Task 4 |
| Canonicalization prevents tracking duplicates | 05 Tasks 3–4, 10 Task 4 |
| Production Run → SSE → Cancel → recover/resume | 04 Tasks 2–4, 06 Task 5, 09 Task 2, 10 Task 4 |
| Robots override reason lifecycle | 03 Task 3, 06 Tasks 3–4, 09 Task 6, 10 Task 4 |
| Listing + Detail shared Dataset no silent overwrite | 07 Tasks 2/5, 09 Task 5, 10 Task 4 |
| Schema drift diagnostics → Draft fix | 07 Task 3, 09 Task 5, 10 Task 4 |
| Duplicate candidates never auto-merge | 07 Task 5, 09 Task 5, 10 Task 4 |
| Complete snapshot creates missing; partial does not | 05 Task 6, 06 Task 5, 07 Task 5, 10 Task 4 |
| Provenance traces Approved value to source/artifact/version | 07 Task 6, 09 Task 5, 10 Task 4 |
| Approved-only export + provenance bundle | 08 Task 2, 09 Task 6, 10 Task 4 |
| Backup → verify → restore | 08 Task 5, 09 Task 6, 10 Task 4 |
| Tri-state settings precedence | 02 Task 1, 09 Task 6, 10 Task 4 |
| Remote bind rejected without token | 03 Task 1, 09 Task 1 auth UI, 10 Task 4 |
| Low-storage blocks without auto-delete | 04 Task 5, 08 Task 4, 09 Task 6, 10 Task 4 |

Plan 10 Task 2 additionally creates `tests/e2e/mvp-journeys.json` with sequential IDs `MVP-01` through `MVP-22`, and CI verifies every ID appears in executable E2E coverage.

## 6. Agent-facing tree audit

The implementation input surface is deliberately small:

- `AGENTS.md` — repository-wide execution rules;
- `docs/specs/README.md` — canonical product-spec entry point;
- `docs/specs/01-*` through `08-*` — canonical product contracts;
- `docs/ROADMAP.md` and `docs/roadmap/` — MVP/deferred boundaries;
- `docs/superpowers/plans/README.md` — plan-directory pointer;
- `docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md` — only MVP implementation-plan entry point;
- `docs/superpowers/plans/2026-08-22-01-*` through `10-*` — active ordered subsystem plans;
- this audit document.

Superseded July files are not present in the working tree. Temporary reconciliation design/plan files are also not present. `docs/superpowers/specs/` no longer exists. Historical Git revisions remain human archaeology only and are explicitly prohibited by `AGENTS.md` as alternate implementation requirements.

## 7. Current implementation entry point

Use only:

`docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md`

For Codex, the intended first instruction is to read `AGENTS.md`, then the active plan index, then execute Plan 01 through its gate before opening Plan 02.

## 8. Open findings

**None in the correction/hardening scope.**

Future spec changes that alter persisted data contracts, Crawler lifecycle, run semantics, approval semantics, or security invariants must update the canonical public spec first, record its exact revision, then explicitly reconcile the active plan before implementation continues.
