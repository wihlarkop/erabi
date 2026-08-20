# Erabi MVP Implementation Plan Index

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement these plans in order. Each subsystem plan uses checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the complete Erabi MVP through twelve independently reviewable subsystem plans while preserving one canonical execution order across Tasks 1–52.

**Architecture:** Erabi is a Rust modular monolith. A single `erabi serve` process hosts the Axum API, SvelteKit static UI, Tokio job runtime, Turso persistence, filesystem artifacts, SSE progress, and product services; unmodified Crawl4AI remains a separate HTTP service.

**Tech Stack:** Stable Rust, Axum, Tokio, Tower, official `turso` crate, Serde, Reqwest/Rustls, `tracing`, SvelteKit, Svelte, TypeScript, Bun, Playwright, Docker Compose, UUIDv7.

## Global Constraints

- Use only the latest compatible stable dependency release available at implementation time.
- Never add alpha, beta, RC, preview, nightly-only, or Git-commit dependencies.
- Add Rust dependencies with `cargo add`; do not hand-invent crate version pins.
- Add frontend dependencies with `bun add`; Bun is the only JavaScript package manager and task runner.
- Commit `Cargo.lock` and `bun.lock`; CI installs from frozen lockfiles.
- Use the official `turso` Rust crate for the Erabi application database.
- Generate UUIDv7 application-side for every primary domain entity.
- Keep Crawl4AI unmodified and isolated behind `CrawlerAdapter`.
- Use one default process, `erabi serve`; distributed workers are roadmap-only.
- Bind to `127.0.0.1` by default; non-loopback binding requires `ERABI_ACCESS_TOKEN`.
- Read secrets and bootstrap-only settings from environment variables or `.env`; never persist secret values in Turso.
- Store normal user-configurable settings in Turso using built-in → global → Collection → per-run resolution.
- Freeze each Crawl Run configuration when it is created, including while `QUEUED`, retried, or resumed.
- Store large raw artifacts, logs, assets, exports, and backups on the filesystem, not as database blobs.
- Never mutate approved Schema, Dataset, or Record versions; edits always create a new version.
- Only a successful complete snapshot may create `MISSING_CANDIDATE` records.
- Validation errors block approval and cannot be overridden; warnings do not block approval.
- Do not emit telemetry or crash reports by default.
- Graceful shutdown is mandatory and has a fixed three-second deadline in the MVP.
- Automatic backup, deep integrity scheduling, retention cleanup, browser notifications, and Trash cleanup are all off by default.
- Target WCAG 2.2 AA, keyboard operation, visible focus, reduced motion, no color-only states, and 200% zoom usability.
- Use English UI copy through translation keys from the first commit.
- Implement roadmap items only when a later specification admits them; do not opportunistically add them to this plan.

---

## How to Execute the Plan Set

1. Read the approved [design specification index](../specs/2026-07-22-erabi-design-index.md).
2. Execute the plans below strictly in order.
3. Inside each plan, execute tasks numerically and commit after every task.
4. Run the plan-specific phase gate before moving to the next file.
5. Use the complete monolithic plan only as a searchable reference, not as the primary execution document.

## Plan Status and Execution Order

- [ ] **01. [Erabi Workspace and Domain Foundation](./01-workspace-and-domain-foundation.md)** — Tasks 1–6. Create the minimal Cargo/Bun monorepo, scaffold focused Rust and SvelteKit packages, and establish stable identifiers, lifecycle types, Collections, Sources, and automatic naming.
- [ ] **02. [Erabi Turso and Persistence Foundation](./02-turso-and-persistence.md)** — Tasks 7–12. Implement settings resolution, immutable crawl snapshots, environment bootstrap, structured tracing, Local Turso persistence, migrations, repository transactions, and atomic filesystem artifacts.
- [ ] **03. [Erabi API Security and Runtime](./03-api-security-and-runtime.md)** — Tasks 13–15. Build the hardened Axum application shell, same-origin static UI serving, local OpenAPI behavior, startup checks, single-instance locking, Recovery Mode, and mandatory three-second graceful shutdown.
- [ ] **04. [Erabi Durable Jobs and SSE Progress](./04-durable-jobs-and-sse.md)** — Tasks 16–18. Implement durable queued jobs, attempts, leases, heartbeats, checkpoints, Tokio workers, concurrency limits, cooperative cancellation, panic isolation, and replayable SSE progress.
- [ ] **05. [Erabi Crawl4AI Integration and Crawl Orchestration](./05-crawl4ai-integration.md)** — Tasks 19–25. Define crawler-neutral contracts, integrate the unmodified official Crawl4AI service, create Sources and Crawl Runs, enforce safe crawling defaults, orchestrate page/batch/pagination work, and persist raw crawl evidence.
- [ ] **06. [Erabi Extraction and Schema System](./06-extraction-and-schemas.md)** — Tasks 26–29. Create the sanitized preview model, detect Document versus Records mode, version extraction schemas, detect drift, and extract typed normalized records from user-selected containers and fields.
- [ ] **07. [Erabi Review, Versioning, and Provenance](./07-review-versioning-and-provenance.md)** — Tasks 30–32. Persist field-level provenance, immutable Dataset and Record versions, unique-key comparison, semantic change candidates, draft autosave, approval/rejection, diffs, and review closure.
- [ ] **08. [Erabi Assets and Exports](./08-assets-and-exports.md)** — Tasks 33–37. Discover and safely download selected assets, export approved data to files, build provenance bundles, test saved destinations, and publish SQLite or Turso tables atomically.
- [ ] **09. [Erabi Retention, Backup, Recovery, and Diagnostics](./09-backup-recovery-and-diagnostics.md)** — Tasks 38–40. Implement Archive/Trash/retention semantics, versioned encrypted `.erabi-backup` files, safe restore, diagnostics, integrity checks, disk-pressure protection, settings APIs, and metadata search.
- [ ] **10. [Erabi SvelteKit Product Application](./10-sveltekit-application.md)** — Tasks 41–45. Build the typed SvelteKit application shell, URL-first Start page, live crawl progress, visual extraction editor, review experience, and all operational resource pages.
- [ ] **11. [Erabi Accessibility and Product Polish](./11-accessibility-and-product-polish.md)** — Tasks 46–47. Add English-first localization infrastructure, light/dark/system themes, opt-in browser notifications, WCAG 2.2 AA behavior, component coverage, and automated accessibility checks.
- [ ] **12. [Erabi Docker, End-to-End Verification, and Release](./12-docker-ci-and-release.md)** — Tasks 48–52. Package Erabi with the official Crawl4AI image, verify complete workflows with Playwright and real-container smoke tests, enforce CI/security policy, and finish release and operator documentation.

## Dependency Chain

```text
01 Workspace and Domain Foundation
  → 02 Turso and Persistence
  → 03 API Security and Runtime
  → 04 Durable Jobs and SSE
  → 05 Crawl4AI Integration
  → 06 Extraction and Schemas
  → 07 Review, Versioning, and Provenance
  → 08 Assets and Exports
  → 09 Backup, Recovery, and Diagnostics
  → 10 SvelteKit Application
  → 11 Accessibility and Product Polish
  → 12 Docker, CI, and Release
```

## Shared Architecture and Complete File Map

The plan creates the following product structure. Do not consolidate focused files into large catch-all modules.

```text
erabi/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── package.json
├── bun.lock
├── bunfig.toml
├── clippy.toml
├── .env.example
├── .gitignore
├── LICENSE
├── README.md
├── apps/
│   └── web/
│       ├── package.json
│       ├── svelte.config.js
│       ├── vite.config.ts
│       ├── tsconfig.json
│       ├── static/
│       └── src/
│           ├── app.html
│           ├── app.css
│           ├── lib/
│           │   ├── api/
│           │   ├── components/
│           │   ├── features/
│           │   ├── i18n/
│           │   ├── stores/
│           │   └── types/
│           └── routes/
├── crates/
│   ├── erabi-domain/
│   ├── erabi-db/
│   ├── erabi-api/
│   ├── erabi-jobs/
│   ├── erabi-crawler/
│   ├── erabi-crawl4ai/
│   ├── erabi-extraction/
│   ├── erabi-export/
│   ├── erabi-artifacts/
│   ├── erabi-security/
│   ├── erabi-observability/
│   └── erabi-cli/
├── migrations/
├── docker/
│   ├── Dockerfile
│   └── compose.yaml
├── tests/
│   ├── fixtures/
│   │   ├── websites/
│   │   └── crawl4ai/
│   ├── integration/
│   ├── smoke/
│   └── e2e/
└── docs/
    ├── superpowers/specs/
    ├── superpowers/plans/
    ├── operations/
    └── api/
```

## Fixed Cross-Crate Contracts

These contracts are introduced incrementally by the tasks below. Later tasks may add fields, but must not rename established methods without updating every consumer and test in the same commit.

```rust
#[async_trait::async_trait]
pub trait CrawlerAdapter: Send + Sync {
    async fn health_check(&self) -> Result<CrawlerHealth, CrawlerError>;
    async fn crawl(&self, request: CrawlRequest) -> Result<CrawlOutput, CrawlerError>;
    async fn cancel(&self, external_job_id: &str) -> Result<(), CrawlerError>;
}

#[async_trait::async_trait]
pub trait ArtifactStore: Send + Sync {
    async fn write_atomic(&self, request: ArtifactWrite) -> Result<ArtifactRef, ArtifactError>;
    async fn verify(&self, reference: &ArtifactRef) -> Result<ArtifactVerification, ArtifactError>;
    async fn remove(&self, reference: &ArtifactRef) -> Result<(), ArtifactError>;
}

#[async_trait::async_trait]
pub trait DestinationAdapter: Send + Sync {
    async fn test(
        &self,
        destination: &SavedDestination,
    ) -> Result<DestinationCapabilities, ExportError>;

    async fn export(&self, request: ExportRequest) -> Result<ExportReceipt, ExportError>;
}
```

## Reference Documents

- [Approved design specifications](../specs/2026-07-22-erabi-design-index.md)
- [Complete monolithic implementation-plan reference](./2026-07-22-erabi-mvp-implementation-plan-complete.md)

## Final Definition of Done

Erabi MVP is complete only after all twelve plan checkboxes and every task checkbox are complete, all plan gates pass from a clean checkout, Docker Compose starts healthy on localhost, real Crawl4AI smoke tests pass, no validation or integrity invariant is bypassed, and the final acceptance documentation is committed.
