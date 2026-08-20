# Erabi Design Specification Index

**Status:** Approved MVP specification set
**Date:** 2026-07-22  
**License decision:** Apache-2.0  
**Primary distribution:** Docker Compose  
**Primary platform:** Self-hosted/local-first web application; Windows desktop through Tauri 2 follows after the web MVP.

Erabi is an open-source, no-code, human-in-the-loop web data ingestion and curation platform. It turns webpages into structured, reviewed, versioned, auditable datasets without requiring users to write scraping code.

This design is intentionally split into focused specifications. Each document owns one subsystem and avoids duplicating implementation details from neighboring documents.

## Specification Map

1. [Product Scope and Experience](./2026-07-22-erabi-product-scope-design.md)  
   Defines positioning, terminology, MVP boundaries, navigation, first-run experience, and success criteria.

2. [System Architecture and Engineering Standards](./2026-07-22-erabi-system-architecture-design.md)  
   Defines the Rust modular monolith, SvelteKit/Bun frontend, Turso-first persistence, repository layout, dependency policy, testing strategy, API versioning, and deployment topology.

3. [Data Model, Versioning, and Provenance](./2026-07-22-erabi-data-lifecycle-design.md)  
   Defines entities, UUIDv7 identifiers, source and record lifecycle, immutable approved versions, semantic change detection, provenance, audit, settings inheritance, and deletion semantics.

4. [Crawling, Jobs, and Progress](./2026-07-22-erabi-crawling-jobs-design.md)  
   Defines Crawl4AI integration, safe crawling defaults, job orchestration, pagination, dynamic pages, cancellation, checkpointing, retry, SSE progress, and storage-pressure behavior.

5. [Extraction and Review Experience](./2026-07-22-erabi-extraction-review-design.md)  
   Defines Document and Records modes, visual extraction, schema lifecycle, review grid/card views, validation, approval, rejection, autosave, accessibility, and review closure.

6. [Exports, Assets, Retention, and Backups](./2026-07-22-erabi-export-assets-backup-design.md)  
   Defines clean exports, provenance sidecars, ZIP bundles, database destinations, atomic publication, downloaded assets, retention, backup formats, encryption, and restore behavior.

7. [Security, Reliability, and Operations](./2026-07-22-erabi-security-operations-design.md)  
   Defines network exposure, shared-token authentication, CORS, request hardening, CSP, untrusted file handling, diagnostics, migrations, recovery mode, integrity checks, process locking, shutdown, logging, and update policy.

8. [Roadmap and Deferred Capabilities](./2026-07-22-erabi-roadmap-design.md)  
   Records deliberately deferred capabilities so they remain visible without expanding the MVP.

## Implementation Planning

The approved design is implemented through the [Erabi MVP implementation plan index](../plans/2026-07-22-erabi-mvp-plan-index.md), split into twelve ordered subsystem plans. A [complete monolithic reference](../plans/2026-07-22-erabi-mvp-implementation-plan-complete.md) is retained for full-text lookup.

## Architectural Summary

```text
SvelteKit + TypeScript SPA (Bun)
             │
             │ REST /api/v1 + SSE
             ▼
Rust modular monolith
├── Axum HTTP API
├── Tokio job runtime
├── extraction and review domain
├── versioning, provenance, and audit
├── destination and artifact adapters
└── operational safeguards
             │
             ├── Local Turso database by default
             ├── Local filesystem for artifacts/assets/exports/backups
             └── HTTP adapter to unmodified Crawl4AI
```

The default runtime is one process:

```bash
erabi serve
```

The default Docker deployment is two containers:

```text
erabi      Rust server + embedded static SvelteKit UI
crawl4ai   Official Crawl4AI image, unmodified
```

## Frozen Product Principles

- Start with a URL, not a configuration wizard.
- Single-page scrape is the default; broader crawling is offered after the first result.
- Every scrape creates a durable, inspectable run.
- Raw artifacts and curated data are separate.
- Approved versions are immutable.
- Every approved field remains traceable to its source.
- Safe crawling, privacy, and conservative deletion are defaults.
- Errors block approval; warnings do not.
- Partial or failed crawls never imply record deletion.
- Crawl4AI is treated as an external engine and is not forked or rewritten.
- MVP remains local-first, single-user, and operationally simple.

## MVP Scope Freeze Rule

After approval of these specifications, a new capability enters MVP only when it is required to fix:

1. a correctness defect;
2. a security weakness;
3. a data-integrity risk;
4. a deployment blocker; or
5. a missing requirement already stated in these specifications.

All other ideas move to the roadmap.

## Visual Direction

The Start page concept is captured in [`docs/assets/erabi-start-concept.png`](../../assets/erabi-start-concept.png). It is directional rather than a pixel-perfect contract. The core interaction is fixed: a prominent URL input is the first thing a user sees.
