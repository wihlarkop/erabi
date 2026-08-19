# Erabi Public Specification Index

**Status:** Approved product direction; implementation plan intentionally deferred until this specification stabilizes.

**Specification date:** 2026-08-20

This directory is the public source of truth for the Erabi Crawler Studio design. Requirements use the terms **MUST**, **SHOULD**, and **MAY** in their usual normative sense.

## Reading order

1. [Product and Experience](01-product-and-experience.md)
2. [Crawler Studio Domain](02-crawler-studio-domain.md)
3. [Discovery Graph, Test Lab, and Runs](03-discovery-graph-and-runs.md)
4. [Extraction, Curation, and Provenance](04-extraction-curation-and-provenance.md)
5. [System Architecture and Persistence](05-system-architecture-and-persistence.md)
6. [Security, Reliability, and Operations](06-security-reliability-and-operations.md)
7. [Exports, Assets, Retention, and Backups](07-exports-assets-retention-and-backups.md)
8. [UX, Accessibility, and Verification](08-ux-accessibility-and-verification.md)
9. [Roadmap](../ROADMAP.md)

## Canonical product statement

> **Erabi is an open-source visual studio for designing, testing, running, inspecting, and curating web crawlers.**

Primary tagline:

> **The open-source studio for web crawling and extraction.**

Supporting line:

> **Design, test, run, inspect, and curate crawlers without writing code.**

## Core mental model

```text
DESIGN
Crawler
├── Seeds
├── Page Types
├── Discovery Graph
├── Canonicalization
├── Domain Scope
├── Extraction
├── Datasets
└── Run Profiles

OPERATE
├── Test Lab
├── Discovery Preview
├── Production Runs
├── Live Progress
├── Checkpoints
├── Retry / Resume
├── Logs
└── Run Comparison

CURATE
├── Records
├── Field-level provenance
├── Validation
├── Conflicts
├── Approval
├── Versioning
├── Dataset Relationships
└── Export
```

## MVP boundary

The MVP is deliberately broad enough to prove the complete Studio workflow, but it is not a generic browser automation platform, a hosted crawler network, or a generic admin framework.

The following are specifically **not required for MVP** and are tracked in the roadmap: scheduler/cron crawling, authenticated browser workflows, arbitrary click/fill automation, full drag-and-drop graph programming, schema sharing/import/export, full-text search across record bodies, AI copilot features, desktop packaging, distributed workers, team accounts, plugin marketplace, crawler templates/marketplace, and generated RAG/frontends.

## Stable cross-spec invariants

These rules apply across every specification:

- Every major entity uses UUIDv7 generated application-side.
- Published crawler versions are immutable.
- Normal production runs use a published crawler version; draft versions are exercised through Test Run and Discovery Preview.
- A Crawl Run stores an immutable resolved configuration snapshot at creation time, including queued runs.
- Changes to global, Collection, Crawler, Run Profile, or per-run settings never mutate an existing run.
- Crawl4AI is treated as a crawler engine behind an adapter. Erabi does not fork it or depend on its internal Python implementation.
- Approved curated record versions are immutable.
- Raw artifacts and curated data are separate; Erabi never overwrites raw evidence with curated values.
- Missing/deleted candidates are created only from complete, healthy snapshots.
- Validation errors block approval and cannot be overridden. Warnings do not block approval.
- No silent data merge, no silent field overwrite, and no silent Page Type conflict resolution.
- Secrets come from environment variables / `.env`, not the internal database.
- Default network bind is loopback. Non-loopback binding requires an access token.
- Detailed logs redact sensitive content by default.
- Graceful shutdown is mandatory with a three-second deadline.
- Automatic destructive cleanup is off by default.
- Automatic backup and scheduled deep integrity checks are off by default.
- Accessibility target is WCAG 2.2 AA for the primary application experience.

## Change management

Until implementation starts, specification changes are expected. When implementation planning begins, the plan must reference the exact specification revision or commit SHA it was derived from.

Once implementation is in progress, a spec change that alters persisted data contracts, crawler lifecycle, run semantics, approval semantics, or security invariants must be explicitly reconciled with the active implementation plan rather than silently edited underneath it.
