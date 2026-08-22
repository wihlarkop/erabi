# Erabi MVP Implementation Plan Index — Crawler Studio

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement these plans in order. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Erabi MVP from the canonical Crawler Studio public specification using one ordered, reviewable implementation path.

**Architecture:** Rust/Axum modular monolith + Tokio durable jobs + local Turso + filesystem artifacts + SvelteKit/Bun SPA + unmodified Crawl4AI HTTP adapter. `Crawler`/`CrawlerVersion` are the reusable design center; `Source` is supporting durable input/history identity.

**Tech Stack:** Stable Rust, Axum, Tokio, Tower, official `turso` crate, Serde, Reqwest/Rustls, `tracing`, SvelteKit, TypeScript, Bun, Playwright, Docker Compose, UUIDv7.

**Spec:** [`docs/specs/README.md`](../../specs/README.md)  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Agent execution contract

Repository-level agent instructions live in [`AGENTS.md`](../../../AGENTS.md). Read them before starting implementation.

This file is the only MVP implementation-plan entry point in the current tree. Execute the ten plans below in numerical order. Each plan has a gate; do not begin the next plan until its predecessor passes from a clean checkout.

If a plan conflicts with `docs/specs/`, the canonical specification wins. Reconcile the plan before implementing the conflicting behavior. Do not use Git history as an alternative source of current product requirements.

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

All ten plan gates pass from a clean checkout; every required journey in `docs/specs/08-ux-accessibility-and-verification.md` is automated; Docker Compose is healthy; real Crawl4AI smoke tests pass against deterministic fixtures; current documentation contains no unresolved role conflict among Crawler, Source, Seed, Page Type, Dataset, and Crawl Run; and no deleted/superseded planning material is used as implementation input.
