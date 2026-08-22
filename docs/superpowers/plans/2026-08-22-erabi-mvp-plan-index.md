# Erabi MVP Implementation Plan Index — Crawler Studio

> **For agentic workers:** Implement these plans in numerical order using an implementation-first, verification-after workflow. Use `superpowers:executing-plans` or equivalent execution support when useful, but do not default to test-driven-development/RED-GREEN sequencing.

**Goal:** Build Erabi MVP from the canonical Crawler Studio public specification using one ordered, reviewable implementation path.

**Architecture:** Rust/Axum modular monolith + Tokio durable jobs + local Turso + filesystem artifacts + SvelteKit/Bun SPA + unmodified Crawl4AI HTTP adapter. `Crawler`/`CrawlerVersion` are the reusable design center; `Source` is supporting durable input/history identity.

**Tech Stack:** Stable Rust, Axum, Tokio, Tower, official `turso` crate, Serde, Reqwest/Rustls, `tracing`, SvelteKit, TypeScript, Bun, Playwright, Docker Compose, UUIDv7.

**Spec:** [`docs/specs/README.md`](../../specs/README.md)  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Agent execution contract

Repository-level agent instructions live in [`AGENTS.md`](../../../AGENTS.md). Read them before starting implementation.

This file is the only MVP implementation-plan entry point in the current tree. Execute the ten plans below in numerical order. Every plan defines exact scope, file ownership, important interfaces, feature implementation requirements, verification commands, commit boundaries, and a plan gate.

Execution rules:

1. read the plan's referenced canonical spec sections before implementation;
2. understand the complete task/feature boundary before changing code;
3. implement the scoped feature end-to-end first;
4. build/compile/type-check the implementation;
5. add or update meaningful tests after implementation for important behavior, invariants, regressions, and acceptance criteria;
6. run all verification commands required by the task and fix failures;
7. run formatting/linting and the plan gate before declaring completion;
8. commit completed working features at sensible task boundaries;
9. do not begin the next plan until the current plan gate passes from a clean checkout;
10. if a plan conflicts with `docs/specs/`, stop and reconcile the plan to the canonical spec before implementing the conflict;
11. do not use Git history, deleted documents, old branches, or roadmap-only ideas as alternate current requirements.

Do **not** intentionally create failing tests first, perform RED/GREEN ceremony, or split a feature into artificial micro-steps merely to satisfy a TDD process. Tests are still required where specified; they verify the implemented behavior.

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

## Cross-plan interface handoffs

- Plan 01 defines core domain identities, Crawler/CrawlerVersion, Source, Page Type matcher primitives, lifecycle enums, and error codes.
- Plan 02 persists those contracts and defines immutable run snapshots/settings resolution.
- Plan 03 defines runtime/API/security/recovery boundaries consumed by later route modules.
- Plan 04 provides durable job/progress/checkpoint services used by crawling/export/backup work.
- Plan 05 owns Crawler Studio semantic validation and exposes `VersionValidationContributor` so later semantic modules can add publish-blocking validation without circular ownership.
- Plan 06 installs the Crawl4AI adapter, Source intake, Quick Scrape, and production crawl execution.
- Plan 07 defines Page Type-owned extraction/Dataset contracts and must register an `ExtractionValidationContributor` implementation with the Plan 05 publication validator for extraction, unique-key, and Dataset compatibility checks.
- Plan 08 adds asset/export/backup persistence and workers without mixing destination database tables with Erabi internal tables.
- Plan 09 consumes the stable `/api/v1` contracts; it does not recreate domain semantics client-side.
- Plan 10 enforces the complete integration/release contract and all 22 canonical MVP E2E journeys.

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
