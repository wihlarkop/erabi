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

Within a plan:

1. read the canonical spec sections referenced by the task;
2. follow the task order and TDD steps;
3. use the Superpowers skill named by the plan when available in Codex;
4. run the exact verification commands required by the task;
5. commit only after verification passes;
6. preserve task boundaries so each commit is independently reviewable.

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
