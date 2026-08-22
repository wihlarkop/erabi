# Erabi Agent Instructions

These instructions apply to the entire repository.

## Canonical product specification

The product source of truth is:

- `docs/specs/README.md`
- `docs/specs/01-product-and-experience.md` through `docs/specs/08-ux-accessibility-and-verification.md`
- `docs/ROADMAP.md` and `docs/roadmap/` for MVP/deferred boundaries

The implementation plan set was derived from canonical public-spec revision:

`679b499e617fcef14e4e40b9a7fc826b379b8a30`

Do not infer current requirements from Git history, deleted documents, old branches, old commits, or closed discussions when the current specification is explicit.

## Implementation entry point

Start only from:

`docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md`

Execute the ten linked subsystem plans in numerical order. Do not begin a later plan until the previous plan's gate passes from a clean checkout.

## Implementation-first workflow

Erabi uses an **implementation-first, verification-after** workflow.

Do not use test-driven development, RED/GREEN ceremony, or intentionally failing tests as the default implementation method. The `superpowers:test-driven-development` skill is explicitly not required for Erabi unless the user asks for TDD in a future task.

Within each plan/task:

1. read the referenced canonical spec and understand the complete scoped feature;
2. implement the feature/task end-to-end within its defined boundary;
3. build/compile/type-check the implementation;
4. add or update meaningful tests for important behavior, invariants, regressions, and acceptance criteria after the implementation exists;
5. run the task's verification commands and fix every real failure;
6. run formatting, linting, tests, and the plan gate before declaring completion;
7. commit completed working features at sensible task boundaries.

Tests remain mandatory where the plan/spec requires them. What changes is sequencing: tests verify completed behavior rather than driving implementation through deliberately failing test-first cycles.

Do not weaken tests, skip verification, or claim completion because implementation looks plausible.

## Conflict rule

If a plan conflicts with `docs/specs/`, the canonical specification wins. Do not guess or silently reinterpret the requirement. Reconcile the plan with the specification before implementing the conflicting behavior.

Roadmap-only capabilities must not be implemented opportunistically.

## Frozen MVP invariants

- `Crawler` is the primary reusable design object; `Source` is supporting durable target/history identity.
- Published `CrawlerVersion` values are immutable.
- MVP has exactly four run types: `QUICK_SCRAPE`, `TEST_RUN`, `DISCOVERY_PREVIEW`, and `PRODUCTION_RUN`.
- A pasted URL batch creates independent `QUICK_SCRAPE` runs; it is not a fifth run type.
- Extraction configuration belongs to Page Types inside Crawler Versions; there is no independent global Schema approval subsystem in MVP.
- Page Type matching is deterministic; a complete specificity tie is `AMBIGUOUS_PAGE_TYPE`.
- Direct non-HTML URLs use Source/Asset intake rather than HTML extraction.
- Robots override requires a non-empty reason stored in the immutable run snapshot and audit history.
- Inheritable settings use `INHERIT`, `CUSTOM(value)`, or `RESET_TO_BUILT_IN` with per-run → Run Profile → Crawler → Collection → Global → built-in precedence where layers apply.
- Production-breaking `SCHEMA_DRIFT` cannot be bypassed into trusted complete-snapshot or missing-record semantics.
- Only healthy complete production snapshots may create `MISSING_CANDIDATE` records.
- The internal Erabi application database remains separate from user export destination databases.
- Non-loopback bind requires an access token; telemetry is off by default; graceful shutdown deadline is three seconds.

## Completion standard

Do not claim a task, plan, or MVP gate is complete without fresh verification evidence. The final MVP gate is defined by the active plan index and `docs/specs/08-ux-accessibility-and-verification.md`.
