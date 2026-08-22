# Erabi Specification and Plan Consistency Audit

**Date:** 2026-08-22  
**Canonical public-spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`  
**Replacement plan commit:** `a522b54e6ef448620d9942e9fe3c6ab6491b215a`  
**Audit branch:** `audit/spec-corrections-2026-08-22`

## Result

**PASS — no unresolved current-document contradiction remains in the audited correction scope.**

July design and implementation documents are `HISTORICAL_ONLY`; they are explicitly superseded/stale and are not current implementation instructions.

## 1. Reconciliation acceptance criteria

| # | Requirement | Result | Evidence / current contract |
|---|---|---|---|
| 1 | Quick Scrape is not single-page-only while E2E requires pasted batches | FIXED | One URL remains default; bounded pasted batch is an envelope creating independent `QUICK_SCRAPE` runs. |
| 2 | Source has one Crawler-compatible definition | FIXED | Source is durable input/history identity; Crawler remains reusable design center; Seeds remain versioned config. |
| 3 | Page Type tie resolution is deterministic | FIXED | Priority then explicit specificity key; complete tie is `AMBIGUOUS_PAGE_TYPE`; no insertion/DB/UUID tie-break. |
| 4 | Robots override cannot exist without stored reason | FIXED | Non-empty reason required before create/resume; snapshot+audit capture reason/actor/time/scope/User-Agent/version context. |
| 5 | Setting inheritance is explicit tri-state with full precedence | FIXED | `INHERIT`, `CUSTOM(value)`, `RESET_TO_BUILT_IN`; per-run → Run Profile → Crawler → Collection → Global → built-in. |
| 6 | Direct-file URLs have a defined non-HTML path | FIXED | Confident non-HTML direct files use Source/Asset intake and do not enter HTML extraction. |
| 7 | `SCHEMA_DRIFT` cannot bypass production trust semantics | FIXED | Production-breaking drift prevents trusted complete/missing semantics and requires new Draft/test/publish correction. |
| 8 | Destination DB separation remains intact | PASS | Internal Erabi DB remains separate from SQLite/Turso export destinations; atomic typed publication retained. |
| 9 | Current planning does not instruct agents to build July product | FIXED | All July plans contain `STALE — DO NOT EXECUTE`; all July designs are superseded. |
| 10 | Replacement plans reference exact corrected spec SHA | PASS | New plan index and all subsystem plans use `679b499e617fcef14e4e40b9a7fc826b379b8a30`. |
| 11 | Required MVP E2E journeys map to plan tasks/tests | PASS | Replacement plan 10 Task 3 enumerates every journey; related subsystem plans establish lower-level tests. |
| 12 | Terminology audit has no unresolved role conflict | PASS | Canonical role matrix below is consistent across current August docs/plans. |

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

## 3. Run semantics audit

**PASS.** Current documents and replacement plans use exactly:

```text
QUICK_SCRAPE
TEST_RUN
DISCOVERY_PREVIEW
PRODUCTION_RUN
```

A pasted URL batch is not a fifth run type. Each accepted item has an independent Quick Scrape run, snapshot, status, artifacts, provenance, cancellation/retry state, and review outcome.

Normal Production Runs require Published Crawler Versions. Test Run and Discovery Preview may operate on Drafts. Only a healthy complete Production Run can create `MISSING_CANDIDATE` records.

## 4. Safety and settings audit

**PASS.** Current contracts require:

- deterministic Page Type specificity with complete-tie ambiguity;
- robots respected by default;
- explicit non-empty robots override reason in snapshot and audit;
- no silent reuse of a prior independent run's reason;
- `INHERIT` / `CUSTOM(value)` / `RESET_TO_BUILT_IN` settings semantics;
- full operational precedence including Run Profile and Crawler default;
- direct-file non-HTML routing;
- production schema drift blocking trusted complete/missing semantics;
- loopback default and access token for non-loopback bind;
- no telemetry by default;
- internal DB/export destination separation.

## 5. Required MVP E2E mapping

All journeys in `docs/specs/08-ux-accessibility-and-verification.md` map to replacement Plan 10 (`2026-08-22-10-ci-e2e-and-release.md`, Task 3), with subsystem coverage as follows:

| Journey | Primary implementation/test plan |
|---|---|
| First-run Start → Quick Scrape → Review | 06 Task 3, 09 Task 1, 10 Task 3 |
| Pasted batch → independent ordered Quick Scrapes | 06 Task 3, 09 Task 1/2, 10 Task 3 |
| Direct file → Source/Asset, no HTML extraction | 06 Task 2, 08 Task 1, 10 Task 3 |
| Quick Scrape → Save as Crawler Draft | 05 Task 1, 06 Task 3, 09 Task 3, 10 Task 3 |
| Multi-Seed/multi-Page-Type Draft → Test Lab → Publish | 05 Tasks 1/4/6, 09 Tasks 3/4, 10 Task 3 |
| Page Type ambiguity blocks publish | 01 Task 4, 05 Tasks 2/6, 10 Task 3 |
| Equal specificity tie independent of ordering | 01 Task 4, 05 Task 2, 10 Task 3 |
| Bounded cyclic Discovery Preview | 05 Tasks 3/5, 10 Task 3 |
| External URL stays outside Domain Scope | 05 Task 3, 10 Task 3 |
| Canonicalization prevents tracking duplicates | 05 Task 3, 10 Task 3 |
| Production Run → SSE → Cancel → recover/resume | 04 Tasks 2–4, 06 Task 5, 09 Task 2, 10 Task 3 |
| Robots override reason lifecycle | 03 Task 3, 06 Task 4, 09 Task 6, 10 Task 3 |
| Listing + Detail shared Dataset no silent overwrite | 07 Tasks 2/4, 10 Task 3 |
| Schema drift diagnostics → Draft fix | 07 Task 3, 09 Task 5, 10 Task 3 |
| Duplicate candidates never auto-merge | 07 Tasks 4/5, 10 Task 3 |
| Complete snapshot creates missing; partial does not | 05 Task 6, 06 Task 5, 07 Task 5, 10 Task 3 |
| Provenance traces approved value to source/artifact/version | 07 Task 6, 10 Task 3 |
| Approved-only export + provenance bundle | 08 Task 2, 10 Task 3 |
| Backup → verify → restore | 08 Task 5, 10 Task 3 |
| Tri-state settings precedence | 02 Task 1, 09 Task 6, 10 Task 3 |
| Remote bind rejected without token | 03 Task 1, 10 Task 3 |
| Low-storage blocks without auto-delete | 04 Task 5, 08 Task 4, 10 Task 3 |

## 6. Historical-document audit

**HISTORICAL_ONLY.** The following are not current contracts:

- `docs/superpowers/specs/2026-07-22-*`
- `docs/superpowers/plans/01-*` through `12-*`
- `docs/superpowers/plans/2026-07-22-erabi-mvp-plan-index.md`
- `docs/superpowers/plans/2026-07-22-erabi-mvp-implementation-plan-complete.md`

Every July plan file contains the literal warning `STALE — DO NOT EXECUTE`. Historical content remains retrievable from Git revision `54800bb33754a07afccbd1f369f15f43a2cb3629`.

## 7. Current implementation entry point

Use only:

`docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md`

The replacement plan set is intentionally Crawler-centered, contains no independent global Schema approval subsystem, does not use Inbox as primary navigation, and does not invent a Batch run type.

## 8. Open findings

**None in the correction scope.**

Future specification changes that alter persisted data contracts, Crawler lifecycle, run semantics, approval semantics, or security invariants must repeat the reconciliation rule: update the canonical public spec first, record its exact revision, then reconcile active plans explicitly.
