# DX-D04 Migration Allocation Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use `superpowers:executing-plans` to implement this plan task-by-task. Erabi explicitly forbids subagents for this package. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reconcile Erabi's migration allocation planning so implemented migration history remains immutable, Plan 07 canonically owns reserved migration `0007_curated_data.sql`, Plan 08 canonically owns reserved migration `0008_assets_exports_backups.sql`, and `0009` becomes the next unallocated version without changing runtime migration behavior or executable SQL.

**Architecture:** Keep runtime migration truth and planning allocation truth intentionally separate. Runtime truth remains the existing `migrations/*.sql` files plus `MigrationRunner`; planning allocation becomes a canonical `migrations/README.md` ledger that records immutable implemented history and non-executable future reservations. Active MVP plans must consume only reservations present in that ledger.

**Tech Stack:** Markdown planning documents, Git source-of-truth checks, PowerShell/`rg` verification. No Rust, SQL, database, or dependency changes are expected.

**Spec:** `docs/superpowers/specs/2026-09-09-dx-d04-migration-allocation-design.md`

## Global Constraints

- Baseline is `244ad0f2b4f0180b999a80b19ad9e3b25501945f` unless `main` has advanced only through already-approved documentation integration before implementation begins; the implementation agent must fetch/pull and report the actual starting `main` SHA.
- Existing executable migrations `0001` through `0006` are repository-supported immutable history.
- `migrations/0006_crawl_traversal_state.sql` must not be renamed, edited, deleted, or reassigned.
- Do not modify any existing SQL migration content or filename.
- Do not create placeholder `0007` or `0008` SQL files.
- Do not modify `crates/erabi-db/src/migrate.rs` or any runtime migration behavior.
- Do not change `schema_migrations`, migration locking, checksum behavior, migration validation, or recovery semantics.
- Do not introduce auto-discovery, timestamp IDs, an `xtask`, linter, reservation DB table, or new dependency.
- Do not implement any Plan 07 or Plan 08 product/persistence behavior.
- Product milestone order remains unchanged.
- `migrations/README.md` owns planning allocation only; it is not executable runtime input.
- `0007` is reserved for Plan 07 logical migration `curated_data`.
- `0008` is reserved for Plan 08 logical migration `assets_exports_backups`.
- `0009` is the next unallocated migration version after DX-D04.
- Exact future migration numbers may appear in active planning only when present in the canonical reservation ledger.
- A planning/allocation conflict is fail-closed: STOP and report instead of auto-renumbering.
- Implementation-first, verification-after. Do not add artificial failing tests or TDD ceremony for a documentation-only reconciliation.
- Keep the complete implementation uncommitted and unpushed until independent Terra review returns `BLOCKER: 0` and `IMPORTANT: 0`.
- Do not use `cargo clean`.

---

## Expected File Map

DX-D04 should touch only these implementation files:

```text
migrations/README.md
    Canonical migration allocation ledger and allocation/conflict rules.

docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md
    Active MVP migration ownership table and execution rule alignment.

docs/superpowers/plans/2026-08-22-07-extraction-curation-and-provenance.md
    Plan 07 reserved migration reference: 0007_curated_data.sql.

docs/superpowers/plans/2026-08-22-08-assets-exports-and-backups.md
    Plan 08 reserved migration reference: 0008_assets_exports_backups.sql.

docs/roadmap/04-engineering-dx.md
    DX lifecycle status: DX-S06 merged and DX-D04 current during implementation.
```

No production/runtime source file or executable SQL file should change.

---

### Task 1: Establish the Canonical Migration Allocation Ledger

**Files:**
- Create: `migrations/README.md`
- Read only: `migrations/0001_system.sql`
- Read only: `migrations/0002_crawler_core.sql`
- Read only: `migrations/0003_runs.sql`
- Read only: `migrations/0004_jobs.sql`
- Read only: `migrations/0005_crawl_execution.sql`
- Read only: `migrations/0006_crawl_traversal_state.sql`
- Read only: `crates/erabi-db/src/migrate.rs`

**Interfaces:**
- Consumes: the existing supported runtime migration chain `0001`–`0006`.
- Produces: the canonical planning allocation ledger used by Tasks 2–4.

- [ ] **Step 1: Verify implementation branch and clean baseline before editing.**

Run:

```powershell
git switch main
git pull --ff-only
git rev-parse HEAD
git status --short
```

Then create a fresh package branch:

```powershell
git switch -c docs/dx-d04-migration-allocation-reconciliation
```

If that branch name already exists locally or remotely, STOP and report instead of reusing an unknown branch.

Expected before edits:

```text
working tree clean
branch created from current main
```

Do not continue if unrelated user changes exist.

- [ ] **Step 2: Verify the executable migration chain is exactly the expected implemented history.**

Run:

```powershell
Get-ChildItem migrations -File | Sort-Object Name | Select-Object -ExpandProperty Name
rg -n '"000[1-6]"|000[1-6]_.*\.sql' crates/erabi-db/src/migrate.rs
```

Confirm executable SQL files are exactly:

```text
0001_system.sql
0002_crawler_core.sql
0003_runs.sql
0004_jobs.sql
0005_crawl_execution.sql
0006_crawl_traversal_state.sql
```

`README.md` may exist only after this task creates it.

If another executable migration already exists on current `main`, STOP and report the new allocation reality; do not blindly reserve `0007`/`0008` from an outdated assumption.

- [ ] **Step 3: Record pre-change protection evidence for runtime migration files.**

Run:

```powershell
git diff -- migrations/*.sql crates/erabi-db/src/migrate.rs
```

Expected: empty.

Do not edit these files at any later step.

- [ ] **Step 4: Create `migrations/README.md` as the canonical planning allocation ledger.**

The document must state explicitly that runtime migration truth remains:

```text
migrations/*.sql
+
crates/erabi-db/src/migrate.rs bundled MigrationRunner chain
```

and that the README is planning/allocation metadata only.

Include an **Implemented / Immutable** table with exactly:

| Version | Logical name | Owner | File |
|---|---|---|---|
| `0001` | `system` | Plan 02 | `0001_system.sql` |
| `0002` | `crawler_core` | Plan 02 | `0002_crawler_core.sql` |
| `0003` | `runs` | Plan 02 | `0003_runs.sql` |
| `0004` | `jobs` | Plan 04 | `0004_jobs.sql` |
| `0005` | `crawl_execution` | Plan 06 | `0005_crawl_execution.sql` |
| `0006` | `crawl_traversal_state` | Plan 06 | `0006_crawl_traversal_state.sql` |

State that `IMPLEMENTED / IMMUTABLE` describes repository-supported history and does not imply every database instance has already applied every migration.

Include a **Reserved** table with exactly:

| Version | Planned logical name | Owner | Expected future file |
|---|---|---|---|
| `0007` | `curated_data` | Plan 07 | `0007_curated_data.sql` |
| `0008` | `assets_exports_backups` | Plan 08 | `0008_assets_exports_backups.sql` |

State explicitly:

```text
Next unallocated migration: 0009
```

- [ ] **Step 5: Encode the allocation lifecycle and rules in the ledger.**

The README must document this repository/planning lifecycle:

```text
UNALLOCATED
    ↓ explicit reviewed reservation
RESERVED
    ↓ SQL implemented and merged into supported runtime chain
IMPLEMENTED / IMMUTABLE
```

Include these rules with equivalent normative force:

1. implemented history is append-only and never reused;
2. each reservation has one version, owner, and logical name;
3. new reservation uses the next sequential number after the complete implemented + reserved chain;
4. active plans must not invent an exact future number without a ledger reservation;
5. reservation is not runtime state and must not create placeholder SQL/runner/database entries;
6. implementation consumes its reserved version/logical name unless planning is reconciled first;
7. cancel/split/reorder/ownership changes require ledger + active-plan reconciliation before implementation;
8. implemented historical gaps are never collapsed or recycled.

- [ ] **Step 6: Encode fail-closed conflict behavior.**

Include a concrete example equivalent to:

```text
Plan says: 0007_new_feature.sql
Ledger says: 0007_curated_data → Plan 07

STOP
→ report planning conflict
→ reconcile authoritative planning documents
→ resume only after review
```

Also state that a package implemented before a reserved package still cannot steal the reserved version. With `0007` and `0008` reserved, another reviewed persistence package must allocate `0009` or later.

- [ ] **Step 7: Verify Task 1 scope.**

Run:

```powershell
git status --short
git diff -- migrations/README.md
git diff -- migrations/*.sql crates/erabi-db/src/migrate.rs
git diff --check
```

Expected:

```text
migrations/README.md is new
all existing .sql files unchanged
migrate.rs unchanged
git diff --check clean
```

**Task 1 review gate:** reject if README is executable/runtime-coupled, if any placeholder SQL appears, if the ledger omits `0006_crawl_traversal_state`, if `0007`/`0008` reservations are ambiguous, or if another file changed unexpectedly.

---

### Task 2: Reconcile the Active MVP Migration Ownership Plans

**Files:**
- Modify: `docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md`
- Modify: `docs/superpowers/plans/2026-08-22-07-extraction-curation-and-provenance.md`
- Modify: `docs/superpowers/plans/2026-08-22-08-assets-exports-and-backups.md`
- Read: `migrations/README.md`

**Interfaces:**
- Consumes: canonical reservations from Task 1.
- Produces: active Plan 07/08 documents that consume those exact reservations and no stale collision.

- [ ] **Step 1: Reconcile the MVP plan-index migration ownership table.**

In `docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md`, replace the stale ownership section so it contains the complete chain:

```text
0001_system.sql                 → Plan 02
0002_crawler_core.sql           → Plan 02
0003_runs.sql                   → Plan 02
0004_jobs.sql                   → Plan 04
0005_crawl_execution.sql        → Plan 06
0006_crawl_traversal_state.sql  → Plan 06
0007_curated_data.sql           → Plan 07 (reserved)
0008_assets_exports_backups.sql → Plan 08 (reserved)
```

Keep the existing detailed scope descriptions for `0001`–`0005` unless a wording update is required only to identify reservation status consistently.

Add or strengthen wording that:

- implemented migration identities are never reused or renumbered;
- exact future migration numbers must exist in `migrations/README.md` before an active plan may claim them;
- reserved entries are planning ownership, not executable migration state;
- a later persistence need allocates the next unallocated version after implemented + reserved entries;
- implementation must STOP on a ledger/plan mismatch rather than silently auto-renumber.

Link to `migrations/README.md` as the canonical allocation ledger.

- [ ] **Step 2: Reconcile Plan 07 migration ownership.**

In `docs/superpowers/plans/2026-08-22-07-extraction-curation-and-provenance.md`, replace every active reference:

```text
migrations/0006_curated_data.sql
```

with:

```text
migrations/0007_curated_data.sql
```

At the top-level migration ownership statement, state that `0007_curated_data.sql` is reserved by the canonical migration allocation ledger.

In Task 4 file ownership, the migration file must be exactly:

```text
migrations/0007_curated_data.sql
```

Do not change Dataset, review, provenance, candidate, schema-drift, extraction, validation, or product semantics.

- [ ] **Step 3: Reconcile Plan 08 migration ownership.**

In `docs/superpowers/plans/2026-08-22-08-assets-exports-and-backups.md`, replace every active reference:

```text
migrations/0007_assets_exports_backups.sql
```

with:

```text
migrations/0008_assets_exports_backups.sql
```

At the top-level migration ownership statement, state that `0008_assets_exports_backups.sql` is reserved by the canonical migration allocation ledger.

Do not change asset handling, exports, destination databases, retention, backup/restore, or product semantics.

- [ ] **Step 4: Verify no stale Plan 07/08 collision remains in active MVP planning.**

Run:

```powershell
rg -n "0006_curated_data|0007_assets_exports_backups" docs/superpowers/plans migrations
rg -n "0007_curated_data|0008_assets_exports_backups|0006_crawl_traversal_state" docs/superpowers/plans migrations/README.md
```

Expected:

```text
first command: no matches
second command: consistent matches in ledger/index/owning plans
```

Historical design/spec documents that intentionally describe the former conflict are not active implementation inputs and do not need destructive rewriting. If the first command finds such a historical explanatory document under `docs/superpowers/specs/`, classify it as historical explanation instead of treating it as an active stale plan. The critical stale-reference gate applies to active MVP plans and the canonical ledger.

- [ ] **Step 5: Verify Task 2 semantic scope.**

Run:

```powershell
git diff -- docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md
git diff -- docs/superpowers/plans/2026-08-22-07-extraction-curation-and-provenance.md
git diff -- docs/superpowers/plans/2026-08-22-08-assets-exports-and-backups.md
git diff --check
```

Review manually that differences are restricted to migration allocation/ownership language and links. No product requirement may be added, removed, or reinterpreted.

**Task 2 review gate:** reject if Plan 07 or Plan 08 product semantics changed, if `0006` is still assigned to curated data, if Plan 08 still owns `0007`, or if active plans can claim numbers without ledger reservations.

---

### Task 3: Reconcile DX Lifecycle Status Without Changing Product Order

**Files:**
- Modify: `docs/roadmap/04-engineering-dx.md`
- Read: `docs/ROADMAP.md`
- Read: `migrations/README.md`

**Interfaces:**
- Consumes: completed DX-S06 merge state and active DX-D04 package state.
- Produces: roadmap status that accurately reflects the active DX package while preserving product milestone ordering.

- [ ] **Step 1: Update the DX sequence status table.**

In `docs/roadmap/04-engineering-dx.md`:

- mark `DX-S06` as `MERGED`;
- mark `DX-D04` as `CURRENT`;
- preserve all other package statuses unless a factual merged status already present on current `main` requires no change;
- do not reorder product plans or the top-level product roadmap.

DX-S06's purpose text may remain unchanged. If the roadmap convention records merge metadata for completed packages, add only the already-known merged PR/commit facts for DX-S06; do not invent metadata.

- [ ] **Step 2: Add/refresh the DX-D04 package section if needed for discoverability.**

If `docs/roadmap/04-engineering-dx.md` currently has only the table entry for DX-D04, add a focused section describing:

```text
Why before Plan 07
Goal
Intended scope
Explicit non-goals
Exit evidence
```

The section must summarize the approved design without becoming a second detailed specification. Link to:

```text
docs/superpowers/specs/2026-09-09-dx-d04-migration-allocation-design.md
migrations/README.md
```

If an equivalent DX-D04 section already exists on current `main`, update rather than duplicate it.

- [ ] **Step 3: Confirm product roadmap ordering is untouched.**

Run:

```powershell
git diff -- docs/ROADMAP.md
git diff -- docs/roadmap/04-engineering-dx.md
```

Expected:

```text
docs/ROADMAP.md unchanged
DX roadmap changes only package status/DX-D04 explanation
```

**Task 3 review gate:** reject if product milestone order changes, Plan 07/08 features move, unrelated DX packages are redesigned, or roadmap-only product behavior is introduced.

---

### Task 4: Run the DX-D04 Drift Gate and Prepare Independent Review Handoff

**Files:**
- Modify only a planning document if a concrete verification inconsistency is discovered.
- Do not modify runtime source or executable SQL.

**Interfaces:**
- Consumes: Tasks 1–3 completed candidate.
- Produces: verified uncommitted DX-D04 candidate ready for independent Terra HIGH review.

- [ ] **Step 1: Audit the actual migration directory.**

Run:

```powershell
Get-ChildItem migrations -File | Sort-Object Name | Select-Object -ExpandProperty Name
```

Expected executable SQL files:

```text
0001_system.sql
0002_crawler_core.sql
0003_runs.sql
0004_jobs.sql
0005_crawl_execution.sql
0006_crawl_traversal_state.sql
```

Plus:

```text
README.md
```

There must be no `0007*.sql` or `0008*.sql` file.

- [ ] **Step 2: Prove runtime migration source and SQL history are untouched.**

Run:

```powershell
git diff main -- crates/erabi-db/src/migrate.rs
git diff main -- migrations/*.sql
```

Expected: empty for both commands.

If either diff is non-empty, STOP. DX-D04 scope has been violated.

- [ ] **Step 3: Prove active planning allocation consistency.**

Run:

```powershell
rg -n "0006_crawl_traversal_state|0007_curated_data|0008_assets_exports_backups|next unallocated|0009" migrations/README.md docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md docs/superpowers/plans/2026-08-22-07-extraction-curation-and-provenance.md docs/superpowers/plans/2026-08-22-08-assets-exports-and-backups.md
```

Manually verify:

```text
0006 = implemented crawl_traversal_state / Plan 06
0007 = reserved curated_data / Plan 07
0008 = reserved assets_exports_backups / Plan 08
0009 = next unallocated
```

- [ ] **Step 4: Prove stale conflicting allocation is absent from active implementation planning.**

Run:

```powershell
rg -n "0006_curated_data|0007_assets_exports_backups" docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md docs/superpowers/plans/2026-08-22-07-extraction-curation-and-provenance.md docs/superpowers/plans/2026-08-22-08-assets-exports-and-backups.md migrations/README.md
```

Expected: no matches.

Do not treat the DX-D04 design spec's explanation of the old conflict as a stale active planning reference.

- [ ] **Step 5: Check uniqueness and sequential allocation manually.**

Review the ledger table and confirm there is exactly one owner for each version:

```text
0001
0002
0003
0004
0005
0006
0007
0008
```

with no duplicate or missing allocation, and that `0009` is not reserved.

Do not add tooling or code generation merely to automate this eight-row check.

- [ ] **Step 6: Run repository diff/scope checks.**

Run:

```powershell
git status --short
git diff --stat
git diff --name-only
git diff --check
```

Expected implementation-file set:

```text
migrations/README.md
docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md
docs/superpowers/plans/2026-08-22-07-extraction-curation-and-provenance.md
docs/superpowers/plans/2026-08-22-08-assets-exports-and-backups.md
docs/roadmap/04-engineering-dx.md
```

The design/implementation-plan documents may already exist on `main` if the planning branch was merged before implementation; they are not implementation drift.

There must be no source, SQL, lockfile, dependency, CI, frontend, or migration-runner change.

- [ ] **Step 7: Decide whether Cargo verification is required.**

If the diff contains only the expected Markdown files:

```text
Do NOT run cargo test/check/clippy merely for ceremony.
```

Run only:

```powershell
git diff --check
```

If any runtime Rust file, Cargo manifest/lockfile, or executable SQL file changed unexpectedly:

```text
STOP — DESIGN/SCOPE REVIEW REQUIRED
```

Do not expand the gate automatically; the approved design must be revisited first.

- [ ] **Step 8: Prepare the implementation report for Terra.**

Report exactly these sections:

```text
BASELINE
BRANCH
IMPLEMENTATION SUMMARY
CANONICAL LEDGER
IMPLEMENTED HISTORY PROTECTION
RESERVATIONS
PLAN INDEX RECONCILIATION
PLAN 07 RECONCILIATION
PLAN 08 RECONCILIATION
DX ROADMAP STATUS
STALE REFERENCE AUDIT
RUNTIME / SQL NON-CHANGE PROOF
FILES CHANGED
FRESH VERIFICATION
SCOPE CHECK
KNOWN LIMITATIONS
CONCLUSION
```

Finish with exactly one of:

```text
DX-D04 IMPLEMENTATION COMPLETE — READY FOR INDEPENDENT REVIEW
```

or

```text
DX-D04 IMPLEMENTATION BLOCKED — REVIEW REQUIRED
```

Then STOP.

Do not commit.
Do not push.
Do not create a PR.

---

## Final Acceptance Criteria

DX-D04 is ready for independent review only when all of the following are true:

1. `migrations/README.md` is the canonical planning allocation ledger and explicitly does not replace runtime migration truth.
2. Implemented repository history is documented as `0001`–`0006`, including `0006_crawl_traversal_state` owned by Plan 06.
3. No existing SQL migration file/content/name changed.
4. `crates/erabi-db/src/migrate.rs` is unchanged.
5. No placeholder `0007` or `0008` executable migration exists.
6. `0007_curated_data` is uniquely reserved for Plan 07.
7. `0008_assets_exports_backups` is uniquely reserved for Plan 08.
8. `0009` is explicitly identified as the next unallocated migration version.
9. The MVP plan index matches the ledger.
10. Plan 07 consistently uses `migrations/0007_curated_data.sql` and retains existing product semantics.
11. Plan 08 consistently uses `migrations/0008_assets_exports_backups.sql` and retains existing product semantics.
12. Active planning contains no stale `0006_curated_data.sql` or `0007_assets_exports_backups.sql` allocation.
13. Allocation rules prohibit speculative numeric IDs and require fail-closed reconciliation on conflict.
14. DX-S06 is marked merged and DX-D04 current in the DX roadmap during implementation.
15. Product milestone order is unchanged.
16. No runtime source, SQL, dependency, CI, frontend, or unrelated architecture work enters the implementation diff.
17. `git diff --check` passes.
18. The candidate remains uncommitted and unpushed for independent Terra HIGH review.

## Independent Review Focus

Terra should treat these as the high-risk acceptance seams:

- accidental edit/rename/reuse of implemented `0001`–`0006` history;
- treating a reservation as executable runtime state;
- omitting `0006_crawl_traversal_state` from the authoritative active plan index;
- duplicate ownership of `0007` or `0008`;
- moving the collision from Plan 07 to Plan 08 instead of resolving both;
- active stale references to `0006_curated_data` or `0007_assets_exports_backups`;
- a Plan 07/08 product-semantic edit hidden inside allocation reconciliation;
- migration rules that still allow an agent to invent a number from implementation order;
- `MigrationRunner` or SQL history changes entering a planning-only package;
- roadmap status changes that accidentally reorder product milestones.

Acceptance threshold:

```text
BLOCKER: 0
IMPORTANT: 0
```
