# Erabi System Architecture and Engineering Standards Design

**Status:** Approved specification
**Date:** 2026-07-22

## 1. Architectural Choice

Erabi is a Rust modular monolith with explicit internal boundaries and adapter interfaces. It is not a microservice system and is not a public plugin platform in the MVP.

The default runtime is one process:

```bash
erabi serve
```

That process hosts:

- Axum HTTP API;
- static SvelteKit application assets;
- Tokio job scheduler and workers;
- extraction, validation, review, versioning, audit, export, backup, and operational services;
- Turso database access;
- SSE event delivery.

Crawl4AI remains a separate external service accessed through HTTP.

## 2. Technology Stack

### Frontend

- SvelteKit;
- TypeScript;
- SPA/static build suitable for serving from Axum and reuse inside Tauri 2;
- Bun as the only JavaScript package manager and task runner;
- Playwright for end-to-end tests.

### Backend

- Rust stable toolchain;
- Axum for HTTP routing;
- Tokio for asynchronous runtime and background tasks;
- Tower and `tower-http` for middleware;
- Serde for typed serialization;
- Reqwest with Rustls for Crawl4AI and destination HTTP calls;
- `tracing` ecosystem for structured observability.

### Persistence

- Turso Database through the official `turso` Rust crate;
- local Turso database as the default application database;
- optional Turso Cloud synchronization or remote database mode;
- SQL migration files through a stable Rust migration library or a focused internal migration runner selected during implementation.

### Desktop

- Tauri 2;
- Windows first;
- macOS and Linux portability preserved in filesystem, process, and UI design.

### Crawler

- Official Crawl4AI image or an explicitly configured external Crawl4AI endpoint;
- no fork, embedded patch, or rewrite of Crawl4AI.

### Identifiers

- UUIDv7 for all primary domain entities;
- generated application-side;
- stored as 16-byte binary values when supported cleanly;
- represented as canonical UUID strings in APIs and files.

## 3. Repository Layout

```text
erabi/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── package.json
├── bun.lock
├── apps/
│   ├── web/                       # SvelteKit frontend
│   └── desktop/                   # Tauri 2 application
├── crates/
│   ├── erabi-domain/              # entities, value objects, state transitions
│   ├── erabi-db/                  # Turso connection, migrations, repositories
│   ├── erabi-api/                 # Axum routes, middleware, DTOs, SSE
│   ├── erabi-jobs/                # durable queue, leases, checkpoints, workers
│   ├── erabi-crawler/             # crawler-neutral interfaces and contracts
│   ├── erabi-crawl4ai/            # Crawl4AI HTTP adapter
│   ├── erabi-extraction/          # DOM extraction, normalization, validation
│   ├── erabi-export/              # file and database destination adapters
│   ├── erabi-artifacts/           # filesystem storage and retention
│   ├── erabi-security/            # auth token, redaction, request hardening
│   ├── erabi-observability/       # tracing, diagnostics, diagnostic bundle
│   └── erabi-cli/                 # serve, migrate, doctor, backup commands
├── migrations/
├── docker/
├── tests/
│   ├── fixtures/                  # deterministic websites and Crawl4AI payloads
│   └── e2e/                       # Playwright journeys
└── docs/
```

Crates are boundaries inside one product. They are not independently deployed services.

## 4. Boundary Rules

### Domain

`erabi-domain` contains no Axum, Turso, filesystem, HTTP client, or UI dependencies. It owns:

- identifiers and value objects;
- entity state machines;
- validation policies;
- immutable-version invariants;
- semantic change classification;
- typed errors meaningful to the product.

### Database

`erabi-db` implements repositories and transaction boundaries. Other crates do not embed SQL directly except migration code and purpose-built query modules owned by `erabi-db`.

### Crawler

`erabi-crawler` defines stable internal request/result contracts. `erabi-crawl4ai` translates those contracts to the installed Crawl4AI API. Crawl4AI-specific response details must not leak into domain entities.

### Artifacts

`erabi-artifacts` owns path construction, safe filenames, hashing, atomic file writes, retention, and artifact metadata. Other modules request artifact operations through its interface rather than constructing paths.

### API

`erabi-api` maps HTTP DTOs to application services. Route handlers do not contain domain rules, queue algorithms, SQL, or filesystem operations.

### Jobs

`erabi-jobs` owns durable job state, leases, heartbeat, cancellation, checkpoint contracts, recovery, and concurrency limits. A job handler delegates business work to focused services.

## 5. Core Interfaces

The exact signatures may be refined during implementation, but their responsibilities are fixed.

### Crawler adapter

```rust
#[async_trait]
pub trait CrawlerAdapter: Send + Sync {
    async fn health_check(&self) -> Result<CrawlerHealth, CrawlerError>;
    async fn crawl(&self, request: CrawlRequest) -> Result<CrawlOutput, CrawlerError>;
    async fn cancel(&self, external_job_id: &str) -> Result<(), CrawlerError>;
}
```

### Application repository transaction

```rust
#[async_trait]
pub trait TransactionManager: Send + Sync {
    async fn begin(&self) -> Result<Box<dyn Transaction>, DatabaseError>;
}
```

Repositories receive a transaction context for operations that must commit atomically.

### Artifact storage

```rust
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    async fn write_atomic(&self, request: ArtifactWrite) -> Result<ArtifactRef, ArtifactError>;
    async fn verify(&self, reference: &ArtifactRef) -> Result<ArtifactVerification, ArtifactError>;
    async fn remove(&self, reference: &ArtifactRef) -> Result<(), ArtifactError>;
}
```

### Destination adapter

```rust
#[async_trait]
pub trait DestinationAdapter: Send + Sync {
    async fn test(&self, destination: &SavedDestination) -> Result<DestinationCapabilities, ExportError>;
    async fn export(&self, request: ExportRequest) -> Result<ExportReceipt, ExportError>;
}
```

## 6. API Architecture

The base path is:

```text
/api/v1/
```

Primary resources:

```text
collections
sources
crawl-runs
schemas
datasets
records
assets
destinations
exports
audit-events
settings
system
```

Mutations that start long-running work return immediately with a durable run identifier and event URL.

Example:

```json
{
  "crawl_run_id": "019d...",
  "status": "QUEUED",
  "events_url": "/api/v1/crawl-runs/019d.../events"
}
```

Mutation endpoints that can be retried by browsers or clients support an idempotency key. Approval and draft updates use optimistic concurrency through an expected version value.

All errors follow one shape:

```json
{
  "error": {
    "code": "SCHEMA_DRIFT",
    "message": "The required title field was not found.",
    "details": {
      "field": "title",
      "selector": "article h1"
    },
    "recoverable": true,
    "suggested_actions": ["REVIEW_SELECTORS", "USE_ANYWAY", "CANCEL"],
    "trace_id": "019d..."
  }
}
```

Error codes are stable product contracts. Human-readable messages are localization keys rendered with safe details.

## 7. Runtime Model

### MVP

One `erabi serve` process runs the API and internal workers. It persists queue and checkpoint state in Turso and maintains active concurrency state in memory.

No Redis, Kafka, NATS, or external queue is introduced.

### Future deployment mode

The same binary may later expose:

```bash
erabi serve
erabi worker
erabi migrate
erabi doctor
```

Distributed workers are deferred and must not complicate the local MVP design.

## 8. Application Database

The application database is logically separate from user export destinations.

### Application database contains

- Collections and Sources;
- Crawl Runs and tasks;
- Schemas and versions;
- Datasets, Records, and versions;
- validation results and approvals;
- provenance metadata;
- jobs, leases, and checkpoints;
- settings and resolved configuration snapshots;
- destination metadata and secret references;
- export and backup history;
- audit and operational summaries.

### Filesystem contains

- raw and cleaned HTML;
- rendered DOM;
- Markdown;
- screenshots;
- detailed technical logs;
- downloaded assets;
- export files;
- backup files.

Large artifacts are not stored as database blobs.

### Application database modes

```text
local   default; no cloud account required
sync    local reads/writes with explicit Turso Cloud push/pull
remote  direct remote Turso connection for advanced deployment
```

The MVP UX may expose local and remote configuration first while keeping sync contracts stable for later refinement.

## 9. Migration Strategy

- SQL migration files are versioned independently of the application version.
- `erabi serve` acquires a migration lock and runs pending migrations automatically.
- `erabi migrate`, `erabi migrate status`, and `erabi migrate verify` remain available.
- Migration tests cover an empty database and every supported previous schema version.
- Failed migration or integrity verification enters Recovery Mode.
- Migration code does not depend on PostgreSQL-only features.
- Backup-before-migration is user-configurable; automatic backup remains off by default.

## 10. Dependency Policy

At implementation time, every dependency is resolved to the latest compatible stable release.

Allowed:

- stable crates.io releases;
- stable npm ecosystem packages installed through Bun;
- stable Rust toolchain;
- stable Bun release.

Disallowed by default:

- alpha, beta, release-candidate, preview, or pre-release package versions;
- Rust nightly-only dependencies;
- Git branch or commit dependencies;
- manually invented version pins when `cargo add` or `bun add` can resolve the package.

Approved exception:

- Erabi uses the latest non-pre-release crates.io release of the official `turso` crate even while the upstream Turso Database engine is publicly described as beta. This exception is intentional because Turso's current Rust documentation recommends `turso` for new local/embedded and local-first sync projects. The database boundary, backup/restore, startup integrity checks, migration verification, and Recovery Mode are mandatory safeguards. Erabi does not silently substitute `libsql`; any compatibility blocker must be documented and resolved explicitly.

Rust dependencies are added with `cargo add`. Frontend dependencies are added with `bun add`. `Cargo.lock` and `bun.lock` are committed. Updates are controlled and must pass the complete verification suite.

No Turborepo, Nx, pnpm, Yarn, or npm lockfile is introduced.

## 11. Build and Task Commands

Root scripts provide a small interface over Cargo and Bun:

```text
bun run dev
bun run build
bun run check
bun run test
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features
cargo fmt --check
```

CI installs from lockfiles and does not update dependencies implicitly.

## 12. Testing Strategy

### Rust tests

- pure domain unit tests;
- Turso local integration tests;
- transaction and migration tests;
- Axum API integration tests;
- durable queue, lease, checkpoint, and startup-recovery tests;
- Crawl4AI adapter contract tests against a mock server;
- artifact path and atomic-write tests;
- export and backup integrity tests;
- security middleware tests.

### Frontend tests

- TypeScript and Svelte checks;
- component tests for complex UI states;
- accessibility assertions;
- Playwright end-to-end journeys.

### Pull-request CI

Crawl4AI is mocked with deterministic fixture responses. Fixture websites cover:

- static document;
- repeated records;
- pagination;
- JavaScript-rendered content;
- lazy loading;
- schema drift;
- timeout and partial result;
- duplicate keys;
- unsafe HTML and file names.

### Scheduled smoke tests

A scheduled pipeline uses the official real Crawl4AI container against local fixture sites. It verifies adapter compatibility, JavaScript rendering, pagination, screenshot generation, and end-to-end extraction.

Public websites are not used as CI dependencies.

## 13. Semantic Versioning and Compatibility

Erabi uses Semantic Versioning:

```text
0.1.0 initial MVP
0.2.0 new features or breaking changes during early development
0.2.1 compatible bug fixes
1.0.0 stable core API and persistent formats
```

The following have independent version markers:

- application version;
- API version;
- database schema version;
- `.erabi-backup` format version;
- export manifest format version;
- schema serialization format version, reserved for future import/export.

During `0.x`, breaking changes are permitted only in minor releases and require migrations, release notes, and compatibility notes. After `1.0`, removal follows deprecation and a major release.

## 14. Docker Deployment

The primary MVP distribution is Docker Compose:

```text
erabi
├── Rust server
├── static SvelteKit UI
├── internal Tokio workers
└── Local Turso connection

crawl4ai
└── official image, unmodified
```

Persistent host directories:

```text
./data/
├── database/
├── artifacts/
├── assets/
├── exports/
└── backups/
```

Default port binding:

```text
127.0.0.1:7878:7878
```

Compose uses `.env`, health checks based on readiness, pinned release image tags, restart policy, and persistent volumes. Erabi remains available if Crawl4AI is unhealthy.

## 15. Desktop Direction

Tauri 2 reuses the SvelteKit static frontend and Rust core crates. The first desktop target is Windows. Desktop packaging is deferred until the web MVP is stable, but the architecture prohibits assumptions that require Docker-only paths, Unix-only signals, or browser-only storage.

Crawl4AI remains external. The desktop design may later manage an installed or sidecar Crawl4AI lifecycle, but it does not embed or rewrite the crawler in the MVP.
