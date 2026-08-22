# Erabi MVP Implementation Plan Index — Crawler Studio

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement these plans in order. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Erabi MVP from the canonical Crawler Studio public specification using one ordered, reviewable implementation path.

**Architecture:** Rust/Axum modular monolith + Tokio durable jobs + local Turso + filesystem artifacts + SvelteKit/Bun SPA + unmodified Crawl4AI HTTP adapter. `Crawler`/`CrawlerVersion` are the reusable design center; `Source` is supporting durable input/history identity.

**Tech Stack:** Stable Rust, Axum, Tokio, Tower, official `turso` crate, Serde, Reqwest/Rustls, `tracing`, SvelteKit, TypeScript, Bun, Playwright, Docker Compose, UUIDv7.

**Spec:** [`docs/specs/README.md`](../../specs/README.md)  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Agent execution contract

Repository-level agent instructions live in [`AGENTS.md`](../../../AGENTS.md). Read them before starting implementation.

This file is the only MVP implementation-plan entry point in the current tree. Execute the ten plans below in numerical order. Every plan is written as reviewable TDD tasks with exact file ownership, interfaces, RED verification, implementation steps, GREEN verification, commit boundaries, and a plan gate.

Execution rules:

1. read the plan's referenced canonical spec sections before its first task;
2. execute tasks and checkbox steps in order;
3. write the specified failing test before implementing behavior;
4. run the RED command and confirm the expected failure is caused by missing behavior—not an unrelated environment failure;
5. implement only the scoped behavior for that task;
6. run every GREEN command specified by the task;
7. commit only after GREEN verification passes;
8. run the plan gate from a clean checkout before beginning the next plan;
9. if a plan conflicts with `docs/specs/`, stop and reconcile the plan to the canonical spec before implementing the conflict;
10. do not use Git history, deleted documents, old branches, or roadmap-only ideas as alternate current requirements.

## Global Constraints

- Exactly four run types: `QUICK_SCRAPE`, `TEST_RUN`, `DISCOVERY_PREVIEW`, `PRODUCTION_RUN`.
- Published Crawler Versions and approved record versions are immutable.
- Source does not replace Crawler, Seed, Page Type, Dataset, or Crawl Run.
- Extraction configuration belongs to Page Types inside Crawler Versions; no independent global Schema approval subsystem in MVP.
- Page Type matching uses deterministic priority/specificity and never hidden insertion/database order.
- Quick Scrape batches create independent ordered Quick Scrape runs; no fifth Batch run type.
- Direct non-HTML file URLs follow Source/Asset intake, not HTML extraction.
- Robots override requires a non-empty reason frozen in the run snapshot and audit history.
- Inheritable settings use `INHERIT`, `CUSTOM(value)`, `RESET_TO_BUILT_IN` with per-run → Run Profile → Crawler → Collection → Global → built-in precedence.
- Production-breaking `SCHEMA_DRIFT` cannot restore trusted complete-snapshot/missing semantics through `USE_ANYWAY`.
- Only healthy complete production snapshots may create `MISSING_CANDIDATE` records.
- Internal Erabi DB and export destination DBs remain separate.
- Non-loopback bind requires `ERABI_ACCESS_TOKEN`; no telemetry by default; graceful shutdown deadline is 3 seconds.
- Use current compatible stable dependencies at execution time; Bun is the JS package manager; commit Cargo/Bun lockfiles.
- Roadmap-only capabilities are not implemented opportunistically.

## Execution Order

1. [Domain and Workspace](2026-08-22-01-domain-and-workspace.md)
2. [Persistence and Settings](2026-08-22-02-persistence-and-settings.md)
3. [API Security and Runtime](2026-08-22-03-api-security-runtime.md)
4. [Jobs, Progress, and Recovery](2026-08-22-04-jobs-progress-and-recovery.md)
5. [Crawler Studio and Discovery](2026-08-22-05-crawler-studio-and-discovery.md)
6. [Crawl4AI and Quick Scrape](2026-08-22-06-crawl4ai-and-quick-scrape.md)
7. [Extraction, Curation, and Provenance](2026-08-22-07-extraction-curation-and-provenance.md)
8. [Assets, Exports, and Backups](2026-08-22-08-assets-exports-and-backups.md)
9. [SvelteKit Product UI](2026-08-22-09-sveltekit-product-ui.md)
10. [CI, E2E, and Release](2026-08-22-10-ci-e2e-and-release.md)

## Required Cross-Plan Interface Handoffs

These interfaces are intentionally introduced before all implementations that consume them. Later plans extend or register implementations; they do not invent parallel contracts.

| Producer | Consumer | Fixed handoff |
|---|---|---|
| Plan 02 | Plans 03–08 | typed repositories, immutable `RunConfigSnapshot`, resolved settings, atomic artifact store |
| Plan 03 | Plan 04 | `Runtime` startup hooks and one `ShutdownCoordinator` checkpoint-flush/cancellation integration point |
| Plan 04 | Plan 06 | `JobRuntime`/`JobHandler`, durable progress publisher, checkpoint/cancel/retry/resume model |
| Plan 05 | Plan 06 | `DiscoveryPageProvider`/Discovery Engine and canonical discovery pipeline; Crawl4AI output adapts into this port rather than replacing it |
| Plan 05 | Plan 07 | `VersionValidationContributor`; Plan 07 MUST register an extraction/Dataset/unique-key contributor after those types are introduced |
| Plan 05 | Plans 06–07 | `SnapshotHealth`; execution and extraction/drift signals extend the final Complete/Incomplete decision |
| Plan 06 | Plan 07 | persisted crawl page/artifact evidence and extraction-queue handoff |
| Plan 07 | Plans 08–09 | immutable Approved Dataset/Record versions, provenance, review/candidate APIs |
| Plans 03–08 | Plan 09 | backend API DTO/error/SSE contracts; frontend renders decisions rather than reimplementing domain resolution |
| Plans 01–09 | Plan 10 | executable tests/routes/commands consumed by deterministic CI/E2E/release gates |

When implementing Plan 07 Task 2, create an `ExtractionValidationContributor` (or equally explicit name) implementing Plan 05 `VersionValidationContributor`; it validates Page Type extraction definitions, shared Dataset compatibility, and unique-key contracts and is registered in the same `PublishValidator` used by the publish API. Plan 07's gate is not complete until a publish test proves this contributor can block invalid extraction/Dataset configuration.

## Migration Ownership

Migration numbering is reserved by the plan that owns the bounded persistence model. Do not reuse or renumber an already-committed migration after implementation begins.

| Migration | Owner | Scope |
|---|---|---|
| `0001_system.sql` | Plan 02 | migration tracking, settings, audit/system metadata |
| `0002_crawler_core.sql` | Plan 02 | Collections, Sources, Crawlers/Versions, Seeds, Page Types, matchers, transitions, Run Profiles, Test Evidence |
| `0003_runs.sql` | Plan 02 | Crawl Runs, immutable snapshots, discovered URL/artifact metadata foundation |
| `0004_jobs.sql` | Plan 04 | durable jobs, attempts, checkpoints, progress events |
| `0005_crawl_execution.sql` | Plan 06 | crawl page results and summaries |
| `0006_curated_data.sql` | Plan 07 | Datasets, Record versions/candidates, validation, reviews, provenance, relationships |
| `0007_assets_exports_backups.sql` | Plan 08 | Assets, Export Runs/destinations, backups, retention/integrity metadata |

A later task that needs a new persisted concept after its owning migration is committed creates the next additive migration rather than editing historical migration semantics silently.

## Fixed Domain Contracts

```text
Crawler
└── CrawlerVersion (Draft | Published immutable)
    ├── Seed[]
    ├── PageType[]
    │   ├── URLMatcher[]
    │   ├── ExtractionDefinition
    │   ├── ValidationRules
    │   ├── UniqueKey
    │   └── DatasetMapping
    ├── DiscoveryTransition[]
    ├── CanonicalizationPolicy
    └── DomainScope

Crawler
├── RunProfile[]
└── CrawlRun[]

Source = supporting target/history identity
TestEvidence = durable confidence evidence
```

## Final Definition of Done

All ten plan gates pass from a clean checkout; every required journey in `docs/specs/08-ux-accessibility-and-verification.md` is automated; Docker Compose is healthy; explicit real official Crawl4AI smoke tests pass against deterministic fixtures for the exact release candidate/image digest; current documentation contains no unresolved role conflict among Crawler, Source, Seed, Page Type, Dataset, and Crawl Run; documentation topology checks expose only this active plan path; and no deleted/superseded planning material or roadmap-only capability is used as implementation input.
