# System Architecture and Persistence Specification

## 1. Architectural style

Erabi MVP is a **modular monolith with adapter boundaries**.

It is intentionally not decomposed into microservices. Module boundaries exist to keep domain responsibilities clear and permit later replacement of external adapters without imposing distributed-system complexity on local-first users.

```text
SvelteKit SPA
     │ REST + SSE
     ▼
Rust / Axum modular monolith
├── domain modules
├── API
├── Tokio jobs
├── extraction/curation
├── export/backup/ops
└── adapter interfaces
     │
     ├── Turso persistence
     ├── local artifact filesystem
     └── Crawl4AI HTTP adapter
```

## 2. Technology choices

### Frontend

- SvelteKit;
- TypeScript;
- SPA/static-compatible build so the same frontend can later be reused by Tauri;
- Bun for install, scripts, testing, and workspace management.

### Backend

- Rust stable toolchain;
- Axum HTTP API;
- Tokio async runtime;
- Tower/tower-http style middleware where appropriate;
- Serde for serialization;
- Reqwest or equivalent stable HTTP client for Crawl4AI/external operations;
- `tracing` ecosystem for structured observability.

### Database

- official `turso` Rust crate;
- local Turso database by default;
- optional local-first Turso Cloud push/pull sync through the official `turso` sync capability;
- direct remote-only application persistence is not required for 0.1 and remains adapter-compatible for a later official Turso remote SDK path;
- SQL migrations owned by Erabi.

### Crawler engine

- Crawl4AI runs as a bundled official container by default;
- user may configure an external Crawl4AI endpoint/token;
- Erabi does not fork, modify, or reimplement Crawl4AI.

## 3. Dependency policy

Implementation always resolves current compatibility against the ecosystem at implementation time.

Rules:

- use latest compatible stable package releases;
- use `cargo add` for Rust dependencies;
- use `bun add` / `bun add -d` for frontend dependencies;
- commit `Cargo.lock` and `bun.lock`;
- do not mix npm/pnpm/yarn lockfiles;
- avoid alpha/beta/RC package versions and Git dependencies by default;
- stable Rust, no nightly dependency unless a future spec explicitly permits it;
- the official `turso` crate is an explicit product decision; backup, integrity-check, and recovery safeguards remain mandatory around persistence.

## 4. Repository shape

Target monorepo shape:

```text
erabi/
├── apps/
│   ├── web/
│   └── desktop/              # post-MVP Tauri path
├── crates/
│   ├── erabi-domain/
│   ├── erabi-db/
│   ├── erabi-api/
│   ├── erabi-jobs/
│   ├── erabi-crawler/
│   ├── erabi-crawl4ai/
│   ├── erabi-extraction/
│   ├── erabi-export/
│   └── erabi-cli/
├── migrations/
├── docker/
└── docs/
```

Cargo workspace + Bun workspace are sufficient. Turborepo, Nx, or another monorepo framework is not part of MVP.

## 5. Default process model

Default runtime command conceptually:

```text
erabi serve
├── Axum API
├── internal durable job scheduler/worker runtime
├── SSE broadcaster/replay
├── crawl orchestration
├── extraction/validation
├── export/backup workers
└── system health/recovery services
```

MVP does not require Redis, RabbitMQ, Kafka, or another external message broker.

Future deployment may split `serve` and `worker` modes while preserving the same domain contracts.

## 6. Persistence boundaries

The internal application database stores structured application/domain state:

- Collections;
- Crawlers and Crawler Versions;
- Seeds;
- Page Types;
- Discovery Transitions;
- Run Profiles;
- Crawl Runs and durable jobs;
- discovered URL metadata/status;
- datasets/records/versions;
- validation/approval/rejection state;
- provenance metadata;
- export/backup metadata;
- settings;
- audit/system events.

Large binary/text artifacts are stored on the local filesystem, not as giant database blobs:

- raw HTML;
- cleaned HTML;
- rendered DOM;
- Markdown;
- screenshots;
- downloaded assets;
- detailed technical log files where configured;
- export files;
- backup files.

The database stores artifact IDs, hashes, metadata, retention state, and safe paths.

## 7. Application database vs export destinations

The internal Erabi database and user dataset destinations are separate concepts.

```text
Internal application DB
└── Erabi operational/domain metadata

Destinations
├── JSON / JSONL / CSV / Markdown
├── SQLite
└── Turso
```

An export to Turso must never mean “write user dataset tables into Erabi internal metadata tables.”

## 8. Database access

Domain modules should not scatter direct SDK calls.

Persistence is organized behind repository/transaction interfaces appropriate to each bounded module. External implementation detail remains in `erabi-db`.

Important multi-record operations such as approval, version switching, durable job acquisition, and audit recording are transactional.

## 9. Migrations

Database migration requirements:

- ordered SQL migrations with explicit schema version tracking;
- automatically run during `erabi serve` startup after acquiring a migration lock;
- manual commands such as `erabi migrate`, `erabi migrate status`, and `erabi migrate verify` available for advanced operation;
- migrations tested from an empty database and supported prior schema versions;
- migration failure enters Recovery Mode rather than serving mutations against a half-migrated schema.

Automatic backup before migration is configurable but OFF by default. Interactive UI should recommend a backup when appropriate; non-interactive startup must never hang waiting for an unanswered prompt.

## 10. Configuration sources

### Secrets/bootstrap

Secrets are read from OS environment variables and `.env` fallback. They are not stored in Turso.

Examples:

- Turso remote/sync token;
- external Crawl4AI API token;
- non-loopback Erabi access token;
- future provider secrets.

Saved destinations store the **name of the environment variable** containing a secret, not the secret itself.

### Ordinary settings

Non-secret settings live in the internal database and are editable in UI.

Resolution hierarchy:

```text
per-run override
→ Run Profile / Crawler operational default
→ Collection override
→ Global setting
→ built-in default
```

Semantic crawler configuration belongs to Crawler Version and cannot be altered by operational settings layers.

Collection/global settings support explicit inheritance semantics and the UI shows the source of the active value.

## 11. Configuration snapshots

At run creation, all required semantic and operational configuration is resolved and snapshotted.

The snapshot is immutable for:

- queued runs;
- running runs;
- retry attempts;
- checkpoint resume.

Changing settings later affects only newly created runs.

Configuration hashes are used to decide whether a checkpoint remains resumable.

## 12. Crawl4AI adapter

Domain code talks to a crawler abstraction, not Crawl4AI endpoints directly.

Conceptual adapter responsibilities:

- health/version check;
- submit/execute crawl request;
- receive/render result metadata;
- cancel best-effort external work when supported;
- normalize Crawl4AI errors into Erabi error codes;
- hide bundled-vs-external connection details.

A Crawl4AI outage leaves Erabi UI/data/diagnostics available.

## 13. Local data directory

Default persistent data layout conceptually includes:

```text
data/
├── database/
├── artifacts/
├── assets/
├── exports/
└── backups/
```

Paths must be canonicalized and protected from traversal/symlink surprises according to the security specification.

## 14. Single-instance local lock

Only one active `erabi serve` process may own a given local data directory in MVP.

The process lock stores enough metadata to diagnose contention, such as PID, start time, Erabi version, and bind address.

A stale lock may be reclaimed only after verifying the owning process is no longer active. Multi-instance shared local operation is not MVP.

## 15. API versioning

HTTP API base path:

```text
/api/v1/...
```

During `0.x`, breaking API changes occur only on minor releases and require release/compatibility notes. After 1.0, removal of public API behavior follows staged deprecation and major-version semantics.

Application version, API version, DB migration version, `.erabi-backup` format version, and export manifest format version are tracked separately.

## 16. Semantic Versioning

Erabi application releases follow Semantic Versioning.

Example development interpretation:

- `0.1.0` initial MVP;
- `0.2.0` significant backward-incompatible pre-1.0 feature/domain evolution;
- `0.2.1` compatible bug fix;
- `1.0.0` stable public core contracts.

## 17. OpenAPI

OpenAPI schema/documentation is available by default on localhost development/self-hosted loopback operation.

When Erabi binds to LAN/public interfaces, API documentation is disabled by default and requires explicit environment opt-in. If enabled remotely, it remains access-token protected.

The schema is generated from the real API contracts; examples must not contain real user secrets or scraped content.
