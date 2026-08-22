# Erabi Specification and Plan Consistency Audit

**Date:** 2026-08-22  
**Canonical public-spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`  
**Active plan index:** `docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md`

## Result

**PASS — no unresolved current-document contradiction remains in the audited correction scope.**

The current tree exposes one canonical public specification and one active ten-plan implementation sequence. Superseded July design/plan files and temporary reconciliation documents are absent. The active execution workflow is now explicitly **implementation-first, verification-after** rather than TDD-first.

## 1. Acceptance matrix

| # | Requirement | Result | Current contract |
|---|---|---|---|
| 1 | Quick Scrape batch semantics | PASS | One URL remains default; bounded pasted batch is an envelope over independent `QUICK_SCRAPE` runs, not a fifth run type. |
| 2 | Source role | PASS | Source is durable target/history identity; Crawler remains reusable design center; Seeds remain explicit versioned config. |
| 3 | Deterministic Page Type matching | PASS | Explicit priority + specificity; complete tie is `AMBIGUOUS_PAGE_TYPE`; no insertion/DB/map/UUID tie-break. |
| 4 | Robots override reason | PASS | Explicit non-empty reason before create/resume; immutable snapshot/audit preserve reason/context. |
| 5 | Tri-state settings | PASS | `INHERIT`, `CUSTOM(value)`, `RESET_TO_BUILT_IN`; per-run → RunProfile → Crawler → Collection → Global → built-in. |
| 6 | Direct-file path | PASS | Confident non-HTML direct files use Source/Asset intake and bypass HTML extraction. |
| 7 | `SCHEMA_DRIFT` trust semantics | PASS | Production-breaking drift blocks trusted complete/missing semantics and requires Draft/test/publish correction. |
| 8 | Destination DB separation | PASS | Internal Erabi DB remains separate from SQLite/Turso export destinations. |
| 9 | Single agent execution path | PASS | `AGENTS.md` → one spec index → one plan index → ten ordered plans. |
| 10 | Exact canonical spec revision | PASS | Active index/plans reference `679b499e617fcef14e4e40b9a7fc826b379b8a30`. |
| 11 | All required MVP E2E journeys covered | PASS | Plan 10 Task 3 enumerates all 22 canonical journeys. |
| 12 | Terminology consistency | PASS | Crawler/Version/Source/Seed/PageType/RunProfile/Run/Dataset roles are non-conflicting. |
| 13 | Implementation workflow is unambiguous | PASS | `AGENTS.md`, plan index, and all ten plans require feature implementation first, then build/tests/verification; deliberately failing test-first cycles are not the default. |
| 14 | Migration ownership | PASS | Plan index reserves `0001`–`0007` by bounded subsystem; later persisted concepts use additive migrations. |
| 15 | Obsolete-plan topology cannot silently return | PASS | Plan 10 requires CI checks for active-plan count/path/spec revision/links/placeholders/workflow contract. |

## 2. Canonical terminology matrix

| Concept | Canonical role | Must not become |
|---|---|---|
| Crawler | Primary reusable crawling/extraction design object | A single execution or Source alias |
| Crawler Version | Editable Draft or immutable Published config | Mutable Published state |
| Source | Durable target/history identity for web/direct-file inputs | Replacement for Crawler/Seed/PageType/Dataset/Run |
| Seed | Versioned Crawler entry URL/config | Automatically rewritten from Source metadata |
| Page Type | Structural/semantic page class with matcher/extraction/validation/identity/Dataset mapping | Generic global schema resource |
| Extraction config | PageType-owned behavior inside Crawler Version | Independently approved global Schema subsystem |
| Discovery Transition | Directed PageType → PageType discovery behavior with budgets/provenance | Unbounded implicit traversal |
| Run Profile | Reusable operational overrides only | Semantic CrawlerVersion override |
| Crawl Run | Immutable execution snapshot/history | Mutable pointer adopting later settings |
| Test Evidence | Durable confidence/diagnostic evidence | Production approval |
| Dataset | Curated structured output/record-version context | Internal application DB namespace |

## 3. Execution workflow audit

Every active plan follows the same pattern:

```text
read canonical spec and complete task boundary
→ implement the scoped feature end-to-end
→ build / compile / type-check
→ add or update meaningful tests for behavior/invariants/regressions
→ run task verification and fix failures
→ run formatting/linting and the plan gate
→ commit completed working feature
```

The repository explicitly does **not** require TDD, deliberately failing tests first, or RED/GREEN ceremony by default. Tests remain mandatory where the spec/plan requires them; they verify implemented behavior rather than dictating implementation order.

Cross-plan boundaries remain explicit:

- Plan 01: workspace and core domain contracts.
- Plan 02: settings, immutable run snapshots, Turso core persistence/migrations `0001`–`0003`, artifact persistence boundary.
- Plan 03: runtime/API/network security, errors/audit/redaction, Recovery Mode, 3-second shutdown.
- Plan 04: durable jobs/SSE/checkpoints, migration `0004`.
- Plan 05: Crawler Studio semantic services, canonicalization/scope/discovery/Test Lab/Preview/publish health and validation contributor port.
- Plan 06: Crawl4AI adapter, Quick Scrape/direct-file/robots/rate/crawl execution, migration `0005`.
- Plan 07: PageType-owned extraction, Dataset/review/drift/candidates/provenance, `ExtractionValidationContributor`, migration `0006`.
- Plan 08: assets/exports/destinations/retention/backup/integrity, migration `0007`.
- Plan 09: accessible SvelteKit product UI consuming server domain truth.
- Plan 10: deterministic fixtures, CI/docs topology, all 22 E2E journeys, real Crawl4AI smoke, operator/release docs.

## 4. Run/safety/settings audit

**PASS.** Active documents/plans consistently require:

```text
QUICK_SCRAPE
TEST_RUN
DISCOVERY_PREVIEW
PRODUCTION_RUN
```

A pasted batch is not a fifth run type. Each accepted item has its own run, immutable snapshot, status, artifacts, provenance, cancellation/retry state, and review outcome.

Safety/settings invariants are carried across plans:

- robots respected by default and override requires audited reason;
- no silent reason reuse for a new independent run;
- tri-state setting semantics and full precedence;
- direct-file non-HTML routing;
- deterministic matcher ambiguity;
- production drift blocks complete/missing trust;
- loopback default + token for non-loopback;
- telemetry off by default;
- internal/export DB separation;
- low-storage blocks heavy work without automatic deletion.

## 5. Required MVP E2E mapping

Plan 10 Task 3 automates all 22 journeys from `docs/specs/08-ux-accessibility-and-verification.md`. Earlier subsystem coverage maps as follows:

| Journey | Primary implementation plans |
|---|---|
| Start → Quick Scrape → Review | 06, 09, 10 |
| Pasted batch independent ordered runs | 06, 09, 10 |
| Direct file → Source/Asset | 06, 08, 09, 10 |
| Quick Scrape → Crawler Draft | 05, 06, 09, 10 |
| Multi-Seed/PageType Draft → Test Lab → Publish | 05, 09, 10 |
| PageType ambiguity / ordering independence | 01, 05, 09, 10 |
| Bounded cyclic Discovery Preview | 05, 09, 10 |
| External scope + canonicalization dedupe | 05, 10 |
| Production Run → SSE → Cancel → resume | 04, 06, 09, 10 |
| Robots reason lifecycle | 03, 06, 09, 10 |
| Shared Dataset without silent overwrite | 07, 09, 10 |
| Schema drift → Draft fix | 07, 09, 10 |
| Duplicate candidates never auto-merge | 07, 09, 10 |
| Complete vs partial missing semantics | 05, 06, 07, 10 |
| Field provenance trace | 07, 09, 10 |
| Approved-only export + provenance | 08, 10 |
| Backup → verify → restore | 08, 10 |
| Tri-state setting precedence | 02, 09, 10 |
| Remote bind rejected without token | 03, 10 |
| Low-storage no-auto-delete | 04, 08, 10 |

## 6. Agent-facing tree audit

Current implementation inputs are deliberately limited to:

- `AGENTS.md` — repository execution rules;
- `docs/specs/README.md` and `docs/specs/01-*` through `08-*` — canonical product contract;
- `docs/ROADMAP.md` and `docs/roadmap/` — MVP/deferred boundary;
- `docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md` — only implementation-plan entry point;
- `docs/superpowers/plans/2026-08-22-01-*` through `10-*` — active ordered plans;
- this audit document.

Superseded July/reconciliation docs are absent. Git history is human archaeology only and is not an alternate implementation source.

## 7. Current implementation entry point

Use only:

`docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md`

For Codex: read `AGENTS.md`, read the active plan index and referenced specs, execute Plan 01 through its gate, then stop before Plan 02 unless explicitly instructed to continue.

## 8. Open findings

**None in the current correction/execution-workflow scope.**

Future changes to persisted contracts, Crawler lifecycle, run semantics, approval semantics, security invariants, or execution workflow must update the canonical/current documents explicitly rather than relying on stale history.
