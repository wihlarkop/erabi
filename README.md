# Erabi

> **The open-source studio for web crawling and extraction.**
>
> Design, test, run, inspect, and curate crawlers without writing code.

Erabi is a local-first, self-hosted crawling studio for turning websites into structured, reviewable, provenance-rich datasets. It is designed for people who need more than a one-shot scraper but do not want to build and debug crawler orchestration, extraction, review, versioning, and export tooling from scratch.

Erabi is currently in the **public specification / implementation-planning / pre-implementation** stage. The canonical specifications in `docs/specs/` are the product source of truth. The active implementation plan set is derived from corrected public-spec revision `679b499e617fcef14e4e40b9a7fc826b379b8a30`.

## What Erabi is

Erabi combines three workflows in one product:

1. **Design** — define seeds, page types, URL matching, discovery transitions, canonicalization, extraction, validation, datasets, and run profiles.
2. **Operate** — use Quick Scrape, test crawler drafts, preview discovery, run published crawlers, monitor progress, inspect logs, retry/resume partial work, and compare runs.
3. **Curate** — review extracted records, resolve field conflicts, preserve field-level provenance, approve immutable versions, and export trusted data.

The central reusable object is a **Crawler**. A crawler contains versioned crawling and extraction behavior. A **Crawl Run** is one immutable execution of a crawler version or an ad-hoc Quick Scrape configuration. A **Source** is supporting durable input/history identity for a web or direct-file target; it is not a replacement for a Crawler, Seed, Page Type, Dataset, or Crawl Run.

```text
Crawler
├── Seeds
├── Page Types
│   ├── URL matching rules
│   ├── extraction schema
│   ├── validation
│   ├── unique-key contract
│   └── dataset mapping
├── Discovery Graph
│   └── Page Type → Transition → Page Type
├── URL Canonicalization
├── Domain Scope
├── Run Profiles
└── Published / Draft Versions
```

## Product principles

- **Input first.** Opening Erabi should immediately let a user paste a URL and scrape it.
- **Simple first, deep when needed.** Quick Scrape stays easy; Crawler Studio provides advanced control.
- **Human in the loop.** Erabi may suggest, detect, and rank; it does not silently approve or overwrite trusted data.
- **Provenance is first-class.** Every curated field should remain traceable to its source page, crawl run, artifact, selector, raw value, normalization, and crawler version.
- **Published configuration is immutable.** Production runs are reproducible.
- **Safe crawling by default.** Robots policy, rate limits, domain scope, URL canonicalization, crawl budgets, and storage guardrails are built in.
- **Local-first.** The default deployment works on one machine without requiring a hosted Erabi service.
- **Erabi is a product, not a generic application framework.** Generic CRUD/admin/dashboard building is intentionally outside the product boundary.

## Planned MVP stack

| Area | Choice |
|---|---|
| Frontend | SvelteKit + TypeScript |
| JS package manager | Bun |
| Backend | Rust |
| HTTP API | Axum |
| Async runtime | Tokio |
| Database | official `turso` Rust crate, local Turso by default |
| Crawler engine | Crawl4AI as an external/bundled service; Erabi does not fork or modify it |
| Desktop direction | Tauri 2, Windows first, post-MVP |
| IDs | UUIDv7 |
| Live progress | Server-Sent Events (SSE) with replay |
| Primary distribution | Docker Compose |

Dependency policy: use the latest compatible **stable package release** available when implementation happens, add Rust dependencies with `cargo add`, frontend dependencies with `bun add`, and avoid alpha/beta/RC package releases or Git dependencies by default. The official `turso` crate is an explicit product decision and is paired with mandatory backup, integrity-check, and recovery safeguards.

## Documentation and implementation

For Codex or another coding agent, read [`AGENTS.md`](AGENTS.md) first.

Product specification entry point:

- [Specification Index](docs/specs/README.md)

Implementation entry point:

- [2026-08-22 MVP Plan Index](docs/superpowers/plans/2026-08-22-erabi-mvp-plan-index.md)

Supporting documents:

- [Specification/plan consistency audit](docs/superpowers/audits/2026-08-22-spec-plan-consistency.md)
- [Roadmap](docs/ROADMAP.md)

The current working tree intentionally contains no superseded July design or implementation-plan set. Git history remains available for archaeology, but it is not an implementation source of truth.

## Status

No production implementation is claimed yet. The corrected specification and active implementation plans are ready to execute in order; roadmap-only capabilities remain explicitly deferred.

## License

Erabi is licensed under the Apache License 2.0. See [LICENSE](LICENSE).
