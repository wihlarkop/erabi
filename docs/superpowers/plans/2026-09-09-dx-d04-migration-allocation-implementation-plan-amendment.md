# DX-D04 Implementation Plan Amendment — Active Stale-Reference Scope

**Status:** AUTHORITATIVE PLAN AMENDMENT  
**Date:** 2026-09-09  
**Applies to:** `docs/superpowers/plans/2026-09-09-dx-d04-migration-allocation-implementation-plan.md`

## Reason

Self-review found that Task 2 Step 4's broad search over `docs/superpowers/plans` can match the DX-D04 implementation plan itself because that plan intentionally quotes the old conflicting names while explaining what must be reconciled.

Those explanatory references are not active migration ownership claims and must not fail the stale-allocation gate.

## Authoritative correction

For stale active-allocation verification, use only these active allocation authorities:

```text
migrations/README.md
docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md
docs/superpowers/plans/2026-08-22-07-extraction-curation-and-provenance.md
docs/superpowers/plans/2026-08-22-08-assets-exports-and-backups.md
```

The stale-reference command is therefore:

```powershell
rg -n "0006_curated_data|0007_assets_exports_backups" migrations/README.md docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md docs/superpowers/plans/2026-08-22-07-extraction-curation-and-provenance.md docs/superpowers/plans/2026-08-22-08-assets-exports-and-backups.md
```

Expected: no matches.

Positive allocation verification remains:

```powershell
rg -n "0006_crawl_traversal_state|0007_curated_data|0008_assets_exports_backups|0009" migrations/README.md docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md docs/superpowers/plans/2026-08-22-07-extraction-curation-and-provenance.md docs/superpowers/plans/2026-08-22-08-assets-exports-and-backups.md
```

Historical/spec/implementation documents may quote the former collision to explain why DX-D04 exists. Those quotes are acceptable so long as they are clearly explanatory and not active ownership instructions.

Task 4 Step 4 in the main implementation plan already uses the correct targeted active-authority scope and remains authoritative.

No other implementation-plan requirement changes.
