# DX-D04 — Migration Allocation and Reservation Design

**Status:** APPROVED SPEC  
**Date:** 2026-09-09  
**Package:** DX-D04  
**Baseline:** `244ad0f2b4f0180b999a80b19ad9e3b25501945f`

## 1. Problem

Erabi's runtime migration chain has already advanced through:

```text
0001_system.sql
0002_crawler_core.sql
0003_runs.sql
0004_jobs.sql
0005_crawl_execution.sql
0006_crawl_traversal_state.sql
```

`0006_crawl_traversal_state.sql` is an implemented migration and is part of the supported migration history.

However, the active MVP planning documents still allocate:

```text
0006_curated_data.sql            → Plan 07
0007_assets_exports_backups.sql  → Plan 08
```

This creates migration-number allocation drift. If Plan 07 followed the current plan literally, it would attempt to reuse version `0006`, which is already owned by crawl traversal state. Renumbering Plan 07 to `0007` alone would merely move the collision to Plan 08.

The runtime migration system treats migration identity as durable history. The bundled chain records a version, logical name, and SQL checksum, and database verification rejects unsupported, renamed, reordered, or checksum-incompatible history. Therefore an implemented migration number cannot be reassigned safely.

## 2. Goal

DX-D04 establishes an append-only migration allocation contract that:

1. preserves all implemented migration history unchanged;
2. reserves the next persistence-owning MVP migrations before their implementation;
3. prevents active plans from inventing or reusing migration numbers;
4. makes migration ownership deterministic even when implementation order changes;
5. provides a canonical planning ledger without changing runtime migration behavior.

## 3. Non-goals

DX-D04 does **not**:

- rename, delete, or edit any existing SQL migration;
- change checksums for migrations `0001` through `0006`;
- add placeholder SQL for future reservations;
- add reserved migrations to `MigrationRunner` before their SQL exists;
- change `schema_migrations` or migration-lock persistence;
- change migration execution, validation, or recovery semantics;
- introduce timestamp-based or semantic migration identifiers;
- introduce migration auto-discovery;
- add an `xtask`, linter, database table, or service solely for reservation management;
- implement Plan 07 or Plan 08 persistence;
- reorder product plans.

## 4. Existing runtime truth remains authoritative

Runtime migration truth continues to be represented by the actual SQL files plus the bundled migration list in `erabi-db`.

At the start of DX-D04 the supported runtime chain is:

| Version | Name | File | Repository state |
|---|---|---|---|
| `0001` | `system` | `migrations/0001_system.sql` | IMPLEMENTED / IMMUTABLE |
| `0002` | `crawler_core` | `migrations/0002_crawler_core.sql` | IMPLEMENTED / IMMUTABLE |
| `0003` | `runs` | `migrations/0003_runs.sql` | IMPLEMENTED / IMMUTABLE |
| `0004` | `jobs` | `migrations/0004_jobs.sql` | IMPLEMENTED / IMMUTABLE |
| `0005` | `crawl_execution` | `migrations/0005_crawl_execution.sql` | IMPLEMENTED / IMMUTABLE |
| `0006` | `crawl_traversal_state` | `migrations/0006_crawl_traversal_state.sql` | IMPLEMENTED / IMMUTABLE |

`IMPLEMENTED / IMMUTABLE` describes repository-supported migration history. It does not claim that every database instance has already applied every migration; a database may still have pending migrations and apply them through the normal runner.

DX-D04 must not modify this chain.

## 5. Separate runtime history from planning allocation

Erabi will maintain two related but intentionally separate concepts.

### 5.1 Runtime migration history

Owned by:

```text
migrations/*.sql
+
MigrationRunner bundled migration list
```

It contains only migrations that actually exist and can be applied.

### 5.2 Planning allocation ledger

Owned by:

```text
migrations/README.md
```

It records:

- implemented immutable history for orientation;
- reserved future migration slots for active persistence-owning plans;
- allocation rules.

The planning ledger does not cause migration execution and is not read by `MigrationRunner` at runtime.

This separation prevents reservation metadata from pretending to be an executable schema migration.

## 6. Migration allocation states

A migration number has exactly one repository/planning allocation state:

```text
UNALLOCATED
    |
    | explicit reservation
    v
RESERVED
    |
    | SQL implemented and merged into supported runtime chain
    v
IMPLEMENTED / IMMUTABLE
```

These allocation states are distinct from per-database execution status. A migration that is `IMPLEMENTED / IMMUTABLE` in the repository may still be pending on a particular database until `MigrationRunner` applies it.

### UNALLOCATED

The version has no owner and may be reserved by the next reviewed persistence-owning package.

### RESERVED

The version is owned by a specific package and logical migration name, but there is no executable migration yet.

A reserved version:

- does not have a placeholder `.sql` file;
- does not appear in the runtime bundled chain;
- does not appear in `schema_migrations` merely because it is reserved;
- cannot be claimed by another package.

### IMPLEMENTED / IMMUTABLE

The migration exists in the supported runtime chain. Its version is permanently consumed and is never reused, even if later product behavior stops using the tables it created.

## 7. Approved allocation after DX-D04

DX-D04 reconciles the active MVP chain to:

### Implemented / immutable

| Version | Logical name | Owner |
|---|---|---|
| `0001` | `system` | Plan 02 |
| `0002` | `crawler_core` | Plan 02 |
| `0003` | `runs` | Plan 02 |
| `0004` | `jobs` | Plan 04 |
| `0005` | `crawl_execution` | Plan 06 |
| `0006` | `crawl_traversal_state` | Plan 06 |

### Reserved

| Version | Planned logical name | Owner | Expected future file |
|---|---|---|---|
| `0007` | `curated_data` | Plan 07 | `migrations/0007_curated_data.sql` |
| `0008` | `assets_exports_backups` | Plan 08 | `migrations/0008_assets_exports_backups.sql` |

The next unallocated migration version is therefore `0009`.

## 8. Allocation rules

### Rule 1 — Implemented history is append-only

Once a migration is part of the supported runtime chain, its version is permanently consumed.

Do not reuse an implemented version for another feature or plan.

### Rule 2 — Reservations are unique

A reserved version has exactly one package owner and one planned logical name.

No two active packages may reserve the same version.

### Rule 3 — Allocate after the complete implemented + reserved chain

A new reservation takes the next sequential unallocated version after both:

- implemented migrations; and
- existing reservations.

For the approved post-DX-D04 state, a newly discovered persistence package cannot take `0007` or `0008`; its earliest available version is `0009`.

### Rule 4 — No speculative numeric IDs

An active plan may name an exact future migration number only when that number is present in the canonical reservation ledger.

If a future persistence need has not yet received a reviewed reservation, planning documents must refer to the logical migration need without inventing a number.

### Rule 5 — Reservation is not runtime state

Do not create placeholder SQL files, runner entries, or database migration records for a reservation.

### Rule 6 — Implementation consumes the reservation

When a reserved migration is implemented, it must use the reserved version and logical name unless a separate planning reconciliation is approved first.

Plan 07 therefore implements `0007_curated_data.sql`; Plan 08 implements `0008_assets_exports_backups.sql` under the currently approved plan.

### Rule 7 — Reservation changes require reconciliation

If a reserved plan is canceled, split, reordered, or materially changes persistence ownership, do not silently move or reuse its number during implementation.

First reconcile:

- `migrations/README.md`;
- affected active implementation plans;
- affected plan index/roadmap references.

Only then may implementation proceed.

### Rule 8 — Implemented gaps are not collapsed

If an implemented feature becomes obsolete, the historical number remains consumed. Later migrations remain additive.

## 9. Conflict behavior

Migration allocation conflicts are planning failures and must fail closed before SQL implementation.

Examples:

### Planned number differs from reservation

```text
Plan says: 0007_new_feature.sql
Ledger says: 0007_curated_data → Plan 07
```

Required action:

```text
STOP
→ report planning conflict
→ reconcile authoritative planning documents
→ resume only after review
```

Do not auto-renumber the new feature.

### Implementation arrives before an earlier reserved plan

If a new persistence package is implemented before Plan 07 or Plan 08, reservations still win.

With `0007` and `0008` reserved, the new package must reserve `0009` rather than taking the lowest number whose SQL file does not yet exist.

Implementation order must not silently redefine migration identity.

## 10. Planning reconciliation owned by DX-D04

DX-D04 must reconcile every active MVP planning document that currently conflicts with the approved allocation.

Required updates:

### `migrations/README.md`

Create the canonical migration allocation ledger and rules defined by this design.

### `docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md`

Change migration ownership from:

```text
0006_curated_data.sql            → Plan 07
0007_assets_exports_backups.sql  → Plan 08
```

to:

```text
0006_crawl_traversal_state.sql   → Plan 06
0007_curated_data.sql            → Plan 07
0008_assets_exports_backups.sql  → Plan 08
```

Strengthen the plan-index allocation rules so exact future numbers must match the canonical reservation ledger.

### `docs/superpowers/plans/2026-08-22-07-extraction-curation-and-provenance.md`

Replace every active ownership/file reference to:

```text
migrations/0006_curated_data.sql
```

with:

```text
migrations/0007_curated_data.sql
```

No Plan 07 product semantics change.

### `docs/superpowers/plans/2026-08-22-08-assets-exports-and-backups.md`

Replace every active ownership/file reference to:

```text
migrations/0007_assets_exports_backups.sql
```

with:

```text
migrations/0008_assets_exports_backups.sql
```

No Plan 08 product semantics change.

### `docs/roadmap/04-engineering-dx.md`

Update lifecycle status to reflect:

- DX-S06 is merged;
- DX-D04 is the active package while this work is in progress;
- after accepted implementation, DX-D04 becomes merged.

This status update must not change product milestone order.

## 11. Verification contract

DX-D04 is a planning-contract package. It intentionally changes no runtime Rust code and no SQL migration content.

The verification gate therefore prioritizes source-of-truth and drift checks rather than rerunning the full Rust workspace suite without production changes.

Required verification:

1. `migrations/` still contains executable SQL only for `0001` through `0006`.
2. No existing SQL migration content or filename changed.
3. `crates/erabi-db/src/migrate.rs` is unchanged.
4. The canonical allocation ledger records:
   - `0001`–`0006` as implemented/immutable;
   - `0007_curated_data` reserved for Plan 07;
   - `0008_assets_exports_backups` reserved for Plan 08.
5. Active MVP plan index matches the ledger.
6. Plan 07 uses `0007_curated_data.sql` consistently.
7. Plan 08 uses `0008_assets_exports_backups.sql` consistently.
8. Active planning contains no stale conflicting references to:
   - `0006_curated_data.sql`;
   - `0007_assets_exports_backups.sql`.
9. Implemented and reserved versions are unique and strictly sequential through `0008`.
10. No placeholder `0007` or `0008` SQL file exists.
11. `git diff --check` passes.

Because no production code or executable SQL changes, a full workspace Cargo test is not required by DX-D04 unless implementation unexpectedly touches production/runtime files. If implementation scope expands into runtime code or executable migrations, the package must stop and revisit its design/verification plan before proceeding.

## 12. Independent review

DX-D04 still requires independent Terra HIGH review before acceptance because an allocation error can create an irreversible migration-history collision in Plan 07 or Plan 08.

The reviewer should verify:

- existing `0001`–`0006` history is untouched;
- `0006_crawl_traversal_state` remains the runtime truth;
- `0007` and `0008` reservations are unique and consistent across active planning;
- reservation rules prevent future speculative reuse;
- no runtime migration behavior was changed;
- no product scope from Plan 07 or Plan 08 was implemented opportunistically.

Acceptance threshold:

```text
BLOCKER: 0
IMPORTANT: 0
```

## 13. Exit criteria

DX-D04 is complete when all of the following are true:

1. existing migration history `0001`–`0006` remains unchanged;
2. `0007_curated_data` is canonically reserved for Plan 07;
3. `0008_assets_exports_backups` is canonically reserved for Plan 08;
4. `0009` is clearly the next unallocated migration version;
5. Plan 07 and Plan 08 active documents match their reservations;
6. the MVP migration ownership index matches the ledger;
7. no active stale allocation conflict remains;
8. runtime migration code and SQL remain unchanged;
9. verification passes;
10. independent review returns `BLOCKER: 0` and `IMPORTANT: 0`.

Only after DX-D04 is accepted may Plan 07 create `migrations/0007_curated_data.sql` without a migration-number planning conflict.
