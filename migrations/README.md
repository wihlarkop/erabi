# Migration Allocation Ledger

This README is the canonical planning allocation ledger for Erabi migration
versions. It records implemented migration history and reviewed reservations;
it is not executable runtime input.

Runtime migration truth remains the SQL files under `migrations/*.sql` plus the
bundled `MigrationRunner` chain in `crates/erabi-db/src/migrate.rs`. A ledger
reservation does not create runtime state, and the `MigrationRunner` does not
read this README.

## Implemented / Immutable

| Version | Logical name | Owner | File |
| --- | --- | --- | --- |
| `0001` | `system` | Plan 02 | `0001_system.sql` |
| `0002` | `crawler_core` | Plan 02 | `0002_crawler_core.sql` |
| `0003` | `runs` | Plan 02 | `0003_runs.sql` |
| `0004` | `jobs` | Plan 04 | `0004_jobs.sql` |
| `0005` | `crawl_execution` | Plan 06 | `0005_crawl_execution.sql` |
| `0006` | `crawl_traversal_state` | Plan 06 | `0006_crawl_traversal_state.sql` |

`IMPLEMENTED / IMMUTABLE` describes repository-supported migration history. It
does not claim that every database instance has already applied every
migration. A database may still have pending migrations and apply them through
the normal runtime migration runner.

## Reserved

| Version | Planned logical name | Owner | Expected future file |
| --- | --- | --- | --- |
| `0007` | `curated_data` | Plan 07 | `0007_curated_data.sql` |
| `0008` | `assets_exports_backups` | Plan 08 | `0008_assets_exports_backups.sql` |

These reservations are planning ownership only. They do not create placeholder
SQL, `MigrationRunner` entries, `schema_migrations` records, migration-lock
behavior, or any other runtime state.

**Next unallocated migration: `0009`**

## Allocation lifecycle

Migration allocation states are repository/planning states and are separate
from a particular database's applied or pending execution status:

```text
UNALLOCATED
    |
    | explicit reviewed reservation
    v
RESERVED
    |
    | SQL implemented and merged into supported runtime chain
    v
IMPLEMENTED / IMMUTABLE
```

## Allocation rules

1. Implemented history is append-only. An implemented migration number is
   permanently consumed and is never reused or renumbered.
2. Every reservation uniquely owns one version, one package owner, and one
   logical migration name. Two packages must not reserve the same version.
3. New reservations allocate the next sequential version after the complete
   implemented plus reserved chain.
4. Active plans must not invent an exact future migration number. An exact
   number may be named only after that number exists as a reservation in this
   canonical ledger.
5. A reservation is not runtime state. It must not create placeholder SQL,
   bundled runner entries, `schema_migrations` records, migration-lock
   behavior, or migration execution behavior.
6. Implementing a reservation must consume its reserved version and logical
   name unless planning reconciliation is explicitly approved first.
7. Canceling, splitting, reordering, or materially changing persistence
   ownership requires reconciliation of this ledger and the affected active
   plans before implementation.
8. Implemented migration numbers are never collapsed or recycled, even when a
   feature becomes obsolete.

## Fail-closed conflicts

Migration allocation conflicts are planning failures. For example:

```text
Plan:
0007_new_feature.sql

Ledger:
0007_curated_data -> Plan 07

Result:
STOP
-> report planning conflict
-> reconcile authoritative planning documents
-> resume only after review
```

Do not auto-renumber a conflicting plan. Implementation order does not
override reservation order. If another persistence package is implemented
before Plan 07 or Plan 08, it cannot take `0007` or `0008`; with both
reservations active, its earliest currently available reservation is `0009`.
