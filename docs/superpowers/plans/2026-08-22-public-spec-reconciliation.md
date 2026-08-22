# Erabi Public Specification Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct the canonical August public specification, retire conflicting July design/planning contracts, regenerate an implementation plan aligned to the Crawler Studio model, and verify repository-wide terminology and acceptance criteria.

**Architecture:** `docs/specs/` and `docs/roadmap/` remain authoritative. Corrections are made there first, historical July design/planning documents are then marked superseded/stale, and only after the corrected spec commit is known is a replacement plan set generated with that exact commit SHA as its base revision.

**Tech Stack:** Markdown specifications and implementation plans, Git/GitHub repository history, repository-wide document audits.

**Spec:** `docs/superpowers/specs/2026-08-22-public-spec-reconciliation-design.md`

## Global Constraints

- Do not add post-MVP capabilities while reconciling the specification.
- `docs/specs/` is canonical for product/domain behavior.
- `docs/roadmap/` is canonical for MVP versus deferred capability boundaries.
- `Crawler` remains the primary reusable design object.
- `Source` remains a supporting durable input/history identity and does not replace Crawler, Seed, Page Type, Dataset, or Crawl Run.
- Official run types remain exactly `QUICK_SCRAPE`, `TEST_RUN`, `DISCOVERY_PREVIEW`, and `PRODUCTION_RUN`.
- Published Crawler Versions remain immutable.
- Validation errors block approval and cannot be overridden; warnings do not block approval.
- Only healthy complete production snapshots may create `MISSING_CANDIDATE` records.
- Robots override requires a non-empty reason stored in the immutable run snapshot and audit history.
- Inheritable ordinary settings use `INHERIT`, `CUSTOM(value)`, or `RESET_TO_BUILT_IN`.
- The internal Erabi database remains separate from user export destinations.

---

### Task 1: Correct the Canonical Public Specification

**Files:**
- Modify: `docs/specs/README.md`
- Modify: `docs/specs/01-product-and-experience.md`
- Modify: `docs/specs/02-crawler-studio-domain.md`
- Modify: `docs/specs/03-discovery-graph-and-runs.md`
- Modify: `docs/specs/05-system-architecture-and-persistence.md`
- Modify: `docs/specs/06-security-reliability-and-operations.md`
- Modify: `docs/specs/08-ux-accessibility-and-verification.md`

**Interfaces:**
- Produces: one canonical definition for Source, Quick Scrape batch semantics, direct-file handling, Page Type specificity, robots override reason, tri-state settings, and production `SCHEMA_DRIFT` behavior.
- Preserves: Crawler-centered architecture, official run types, immutable published versions, destination DB separation.

- [ ] **Step 1: Add cross-spec invariants to the specification index**

Add explicit invariants covering Source role, the four run types, bounded Quick Scrape batch behavior, robots override reason, and tri-state settings.

- [ ] **Step 2: Clarify Source, Quick Scrape batch, and direct-file behavior in product experience**

State that Start accepts one URL by default and a bounded pasted URL batch as a convenience; each accepted URL is an independent Quick Scrape run. Define direct-file URLs as Source/Asset intake rather than HTML extraction.

- [ ] **Step 3: Make Page Type matching fully deterministic**

Specify the lexicographic match key: explicit priority, matcher kind, literal path segments, explicit query constraints, literal characters, then inverse wildcard/capture count. Equal keys remain `AMBIGUOUS_PAGE_TYPE` with no implicit insertion/row-order tie-break.

- [ ] **Step 4: Tighten run semantics and schema drift**

Remove single-page-only wording for Quick Scrape, define batch envelope semantics, require robots override reason at run creation/resume, and state that production-breaking `SCHEMA_DRIFT` cannot be bypassed into trusted change/missing semantics.

- [ ] **Step 5: Define tri-state settings in persistence specification**

Use exactly `INHERIT`, `CUSTOM(value)`, and `RESET_TO_BUILT_IN`; define precedence as per-run → Run Profile → Crawler operational default → Collection → Global → built-in.

- [ ] **Step 6: Align security and E2E verification**

Require robots override reason in the security spec and add E2E coverage for deterministic Page Type ties, URL batches, direct files, robots reason, tri-state settings, and schema drift.

- [ ] **Step 7: Run the public-spec audit**

Verify all acceptance statements below are true by rereading the changed files:

```text
Quick Scrape is not described as single-page-only.
Source has one Crawler-compatible role.
Page Type tie resolution is deterministic.
Robots override cannot exist without a reason.
Tri-state inheritance and full precedence are explicit.
Direct-file URLs have a non-HTML path.
SCHEMA_DRIFT cannot bypass production trust semantics.
Destination DB separation is unchanged.
```

- [ ] **Step 8: Commit the corrected public specification**

```bash
git add docs/specs
git commit -m "docs(spec): reconcile crawler studio contracts"
```

### Task 2: Retire the July Design and Plan Set Safely

**Files:**
- Modify: `docs/superpowers/specs/2026-07-22-erabi-design-index.md`
- Modify: every `docs/superpowers/specs/2026-07-22-erabi-*-design.md`
- Modify: `docs/superpowers/plans/2026-07-22-erabi-mvp-plan-index.md`
- Modify: `docs/superpowers/plans/2026-07-22-erabi-mvp-implementation-plan-complete.md`
- Modify: `docs/superpowers/plans/01-workspace-and-domain-foundation.md` through `12-docker-ci-and-release.md`

**Interfaces:**
- Produces: unmistakable historical/superseded markers on every executable July plan and design document.
- Prevents: agents from following Source/Schema-centric July contracts as current implementation instructions.

- [ ] **Step 1: Add the superseded banner to July design documents**

Use this exact banner immediately below each title:

```markdown
> **Superseded:** This July design predates the canonical Crawler Studio specification in `docs/specs/`. It is retained only as historical design context and MUST NOT override current public specifications.
```

- [ ] **Step 2: Add the stale banner to July implementation plans**

Use this exact banner immediately below each title:

```markdown
> **STALE — DO NOT EXECUTE:** This plan was derived from the superseded July Source/Schema-centric design. Use the replacement plan set generated from the corrected `docs/specs/` revision instead.
```

- [ ] **Step 3: Rewrite July index status text**

The July design index must say historical/superseded. The July plan index must say archived/stale and link to the replacement August plan index.

- [ ] **Step 4: Verify no July plan can be mistaken for current instructions**

Every executable July plan file must contain the literal marker `STALE — DO NOT EXECUTE`.

- [ ] **Step 5: Commit the retirement markers**

```bash
git add docs/superpowers/specs docs/superpowers/plans
git commit -m "docs: retire superseded July Erabi plans"
```

### Task 3: Generate the Replacement Crawler-Centered MVP Plan Set

**Files:**
- Create: `docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md`
- Create: `docs/superpowers/plans/2026-08-22-01-domain-and-workspace.md`
- Create: `docs/superpowers/plans/2026-08-22-02-persistence-and-settings.md`
- Create: `docs/superpowers/plans/2026-08-22-03-api-security-runtime.md`
- Create: `docs/superpowers/plans/2026-08-22-04-jobs-progress-and-recovery.md`
- Create: `docs/superpowers/plans/2026-08-22-05-crawler-studio-and-discovery.md`
- Create: `docs/superpowers/plans/2026-08-22-06-crawl4ai-and-quick-scrape.md`
- Create: `docs/superpowers/plans/2026-08-22-07-extraction-curation-and-provenance.md`
- Create: `docs/superpowers/plans/2026-08-22-08-assets-exports-and-backups.md`
- Create: `docs/superpowers/plans/2026-08-22-09-sveltekit-product-ui.md`
- Create: `docs/superpowers/plans/2026-08-22-10-ci-e2e-and-release.md`

**Interfaces:**
- Consumes: the exact commit SHA produced by Task 1.
- Produces: one canonical ordered implementation plan set aligned directly to the public Crawler Studio specification.

- [ ] **Step 1: Capture the corrected public-spec commit SHA**

Record the full SHA in every replacement plan header as `Spec revision:`.

- [ ] **Step 2: Generate the plan index**

The index must establish execution order, dependency gates, source-of-truth links, and the Crawler-centered fixed contracts.

- [ ] **Step 3: Generate domain, persistence, API/runtime, and durable-jobs plans**

The first four plans must establish Crawler/CrawlerVersion/Seed/PageType/DiscoveryTransition/RunProfile/TestEvidence/Source contracts, tri-state settings, immutable run snapshots, secure local runtime, jobs, SSE, checkpoints, cancellation, retry, and recovery.

- [ ] **Step 4: Generate crawler/discovery and Quick Scrape/Crawl4AI plans**

These plans must cover deterministic Page Type matching, Test Lab, Discovery Preview, four run types, bounded batch Quick Scrape, direct-file handling, robots reason, rate limits, complete snapshot semantics, and Crawl4AI adapter contracts.

- [ ] **Step 5: Generate extraction/curation and export/backup plans**

Extraction configuration belongs to Page Types/Crawler Versions. Cover schema drift, review, validation, immutable approvals, provenance, missing candidates, assets, destination DB separation, atomic exports, retention, backup, and restore.

- [ ] **Step 6: Generate frontend and release plans**

Navigation must match the public spec and include Crawler Studio, Published versus Draft UX, Test Lab, Discovery Preview, review/provenance, settings tri-state controls, and all required E2E journeys.

- [ ] **Step 7: Self-review the replacement plan set**

Check each public-spec requirement has a concrete task/test. Scan for `TBD`, `TODO`, `implement later`, independent global Schema approval, `Inbox` as primary navigation, missing `Crawlers`, or a fifth `BATCH` run type; none may remain.

- [ ] **Step 8: Commit the replacement plan set**

```bash
git add docs/superpowers/plans/2026-08-22-*
git commit -m "docs(plan): regenerate Erabi MVP from public spec"
```

### Task 4: Repository-Wide Consistency Audit

**Files:**
- Create: `docs/superpowers/audits/2026-08-22-spec-plan-consistency.md`
- Modify: any current August document found inconsistent during the audit.

**Interfaces:**
- Produces: an auditable matrix mapping canonical concepts and required E2E journeys to specification sections and replacement plan tasks.
- Gate: no unresolved current-document contradiction remains before opening the review PR.

- [ ] **Step 1: Audit canonical terminology**

Check `Crawler`, `Source`, `Seed`, `Page Type`, `Extraction configuration`, `Dataset`, `Crawl Run`, and `Run Profile` roles across current August documents.

- [ ] **Step 2: Audit run semantics**

Verify exactly four run types; Quick Scrape batch is an envelope over independent runs; production uses published Crawler Versions; Test/Discovery may use Drafts; complete-snapshot rules remain production-only.

- [ ] **Step 3: Audit safety and settings**

Verify deterministic matching, robots reason, tri-state settings, direct-file behavior, schema drift, network/access-token rules, and destination DB separation.

- [ ] **Step 4: Audit all required E2E journeys**

Map every journey in `docs/specs/08-ux-accessibility-and-verification.md` to at least one concrete replacement-plan test task.

- [ ] **Step 5: Write the audit report**

Record each requirement as `PASS`, `FIXED`, or `HISTORICAL_ONLY`. No current requirement may remain `OPEN`.

- [ ] **Step 6: Commit audit fixes and report**

```bash
git add docs
git commit -m "docs(audit): verify Erabi spec and plan consistency"
```

### Task 5: Final Verification and Review PR

**Files:**
- Verify: all files changed on `audit/spec-corrections-2026-08-22`

**Interfaces:**
- Produces: reviewable branch/PR containing only specification, planning, retirement markers, and audit documentation.

- [ ] **Step 1: Compare branch against `main`**

Confirm no product source code or unrelated roadmap scope has changed.

- [ ] **Step 2: Verify acceptance criteria**

Re-check all twelve acceptance criteria from `docs/superpowers/specs/2026-08-22-public-spec-reconciliation-design.md` against the branch head.

- [ ] **Step 3: Verify old/new plan status**

Old July plans must be unmistakably stale; the August replacement index must be the only current execution entry point.

- [ ] **Step 4: Open a review PR**

Use title:

```text
docs: reconcile Erabi public spec and implementation plans
```

PR body must summarize corrected contracts, retired July planning, generated replacement plans, and audit result.
