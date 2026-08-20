# Erabi Durable Jobs and SSE Progress Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement durable queued jobs, attempts, leases, heartbeats, checkpoints, Tokio workers, concurrency limits, cooperative cancellation, panic isolation, and replayable SSE progress.

**Architecture:** Turso stores authoritative job and event state, while Tokio owns only active execution. Workers lease jobs, heartbeat them, checkpoint safe progress, isolate task panics, and publish ordered durable events that SSE clients can replay using `Last-Event-ID`.

**Tech Stack:** Rust, Tokio, Turso, Axum SSE, cancellation tokens, structured tracing.

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

## Scope, Dependencies, and Phase Gate

- **Depends on:** [03 API Security and Runtime](./03-api-security-and-runtime.md).
- **Produces:** Durable queue repositories, worker runtime, checkpoint model, event log, and SSE replay endpoint.
- **Gate:** Runtime B: lease exclusivity, stale-job recovery, cancellation, panic isolation, concurrency, event ordering, and SSE reconnect tests pass.
- **Execution order:** Complete every task in this file in numerical order and commit after each task. Do not begin the next plan until this gate passes.

## Focused File Map

```text
crates/erabi-jobs/
crates/erabi-api/src/routes/events.rs
crates/erabi-db/src/repositories/jobs/
crates/erabi-db/src/repositories/events/
tests/integration/jobs/
tests/integration/sse/
```

---

### Task 16: Define Durable Jobs, Attempts, Checkpoints, and Queue Repository

**Files:**
- Create: `crates/erabi-jobs/src/model.rs`
- Create: `crates/erabi-jobs/src/repository.rs`
- Create: `crates/erabi-jobs/src/checkpoint.rs`
- Create: `crates/erabi-jobs/src/error.rs`
- Modify: `crates/erabi-jobs/src/lib.rs`
- Create: `crates/erabi-db/src/jobs.rs`
- Test: `crates/erabi-jobs/tests/model_contract.rs`
- Test: `crates/erabi-db/tests/job_repository.rs`

**Interfaces:**
- Produces: `Job`, `JobKind`, `JobAttempt`, `JobLease`, `JobCheckpoint`.
- Produces: `JobRepository::{enqueue,claim_next,heartbeat,checkpoint,complete,fail,cancel,recover_stale}`.

- [ ] **Step 1: Add dependencies and crate links**

Run:

```bash
cargo add -p erabi-jobs async-trait
cargo add -p erabi-jobs serde --features derive
cargo add -p erabi-jobs serde_json
cargo add -p erabi-jobs thiserror
cargo add -p erabi-jobs tokio-util --features rt
cargo add -p erabi-jobs futures-util
cargo add -p erabi-jobs tracing
cargo add -p erabi-jobs --path crates/erabi-domain erabi-domain
cargo add -p erabi-db --path crates/erabi-jobs erabi-jobs
```

- [ ] **Step 2: Write state transition tests**

Test legal transitions:

```rust
assert!(JobStatus::Queued.can_transition_to(JobStatus::Running));
assert!(JobStatus::Running.can_transition_to(JobStatus::Recoverable));
assert!(!JobStatus::Succeeded.can_transition_to(JobStatus::Running));
```

Test that checkpoint JSON always includes `config_hash`, completed/pending units, failed units, and saved artifact IDs.

- [ ] **Step 3: Implement exact MVP job kinds**

```rust
pub enum JobKind {
    CrawlPage,
    DiscoverPagination,
    ExtractDataset,
    ValidateDataset,
    DownloadAsset,
    ExportDataset,
    CreateBackup,
    VerifyBackup,
    RestoreBackup,
    IntegrityCheck,
    RetentionCleanup,
}
```

A `Job` includes UUIDv7 ID, kind, status, priority, JSON payload, attempts, max attempts, schedule/start/heartbeat/finish timestamps, parent job, checkpoint, collection/domain keys, and immutable configuration hash.

- [ ] **Step 4: Implement the Turso queue repository**

`claim_next()` must execute atomically:

1. select the highest priority eligible `QUEUED` job ordered by `priority DESC, scheduled_at ASC, id ASC`;
2. ensure global/Collection/domain limits are not exceeded;
3. update it to `RUNNING`, assign lease owner and expiry, increment attempt;
4. return the claimed row;
5. commit.

Use a short transaction and optimistic `WHERE status='QUEUED'` update so two claimers cannot both succeed.

- [ ] **Step 5: Implement stale job recovery**

A `RUNNING` job with expired lease becomes:

- `RECOVERABLE` when a valid checkpoint exists and its config hash matches;
- `FAILED` otherwise, with `JOB_LEASE_EXPIRED` summary.

Never auto-resume a stale job before the UI/runtime policy explicitly chooses resume.

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p erabi-jobs
cargo test -p erabi-db --test job_repository
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.lock crates/erabi-jobs crates/erabi-db
git commit -m "feat(jobs): add durable queue and checkpoints"
```
### Task 17: Implement Tokio Workers, Concurrency Limits, Cancellation, and Panic Isolation

**Files:**
- Create: `crates/erabi-jobs/src/handler.rs`
- Create: `crates/erabi-jobs/src/worker.rs`
- Create: `crates/erabi-jobs/src/limits.rs`
- Create: `crates/erabi-jobs/src/recovery.rs`
- Modify: `crates/erabi-jobs/src/lib.rs`
- Test: `crates/erabi-jobs/tests/worker_runtime.rs`

**Interfaces:**
- Produces: `JobHandler`, `WorkerRuntime`, `ConcurrencyController`.
- Enforces: default one active job, two pages per job, Collection/domain semaphores, cooperative cancellation.
- Enforces: task panic isolation.

- [ ] **Step 1: Write worker behavior tests**

Use fake handlers to prove:

- priority ordering;
- global limit one by default;
- domain limit prevents simultaneous same-domain work;
- cancellation produces a checkpoint and `CANCELLED`;
- a handler panic fails only that job and the worker loop continues;
- shutdown marks unfinished work recoverable within the runtime deadline.

- [ ] **Step 2: Define the handler contract**

```rust
#[async_trait::async_trait]
pub trait JobHandler: Send + Sync {
    fn kind(&self) -> JobKind;
    async fn handle(&self, context: JobContext) -> Result<JobOutcome, JobError>;
}

pub struct JobContext {
    pub job: Job,
    pub cancellation: tokio_util::sync::CancellationToken,
    pub checkpoint: CheckpointWriter,
}
```

- [ ] **Step 3: Implement hierarchical concurrency controls**

Use `tokio::sync::Semaphore` for:

- global active jobs;
- per-Collection active jobs;
- per-domain active units;
- active browser pages.

Acquire permits in a fixed order: global → Collection → domain → browser. Drop in reverse order automatically through owned permits to prevent deadlocks.

- [ ] **Step 4: Isolate handler panics**

Wrap each handler future using `std::panic::AssertUnwindSafe` and `futures_util::FutureExt::catch_unwind`. On panic:

- record a redacted backtrace/error summary;
- checkpoint if possible;
- mark the job `RECOVERABLE` when checkpoint valid, otherwise `FAILED`;
- continue the worker loop.

A database invariant panic is promoted to the runtime critical-failure channel and triggers Recovery Mode.

- [ ] **Step 5: Run tests**

Run: `cargo test -p erabi-jobs --test worker_runtime`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/erabi-jobs Cargo.lock
git commit -m "feat(jobs): run isolated cancellable Tokio workers"
```
### Task 18: Persist Progress Events and Support SSE Replay

**Files:**
- Create: `crates/erabi-jobs/src/events.rs`
- Create: `crates/erabi-db/src/progress_events.rs`
- Create: `crates/erabi-api/src/routes/crawl_events.rs`
- Modify: `crates/erabi-api/src/router.rs`
- Test: `crates/erabi-api/tests/sse_replay.rs`

**Interfaces:**
- Produces: `ProgressEvent`, `ProgressEventStore`, `ProgressPublisher`.
- Produces: `GET /api/v1/crawl-runs/{id}/events` with `Last-Event-ID` replay.

- [ ] **Step 1: Write failing replay tests**

Persist three events with sequence 1–3, connect with `Last-Event-ID: 1`, and assert the stream first returns sequences 2 and 3 before live sequence 4. Test sequence monotonicity under concurrent publishers.

- [ ] **Step 2: Define stable event shape**

```rust
pub struct ProgressEvent {
    pub id: EntityId,
    pub crawl_run_id: EntityId,
    pub event_type: String,
    pub sequence: i64,
    pub timestamp: Timestamp,
    pub progress: Option<ProgressValue>,
    pub message_key: String,
    pub message_args: serde_json::Value,
    pub technical: serde_json::Value,
}
```

Use translation keys rather than persisted rendered English messages for user-facing progress.

- [ ] **Step 3: Implement event persistence and broadcast**

Inside one transaction, allocate the next sequence for the Crawl Run and insert the event. After commit, send it through a Tokio broadcast channel. If no subscriber exists, persistence still succeeds.

- [ ] **Step 4: Implement SSE replay**

The endpoint:

1. authenticates like every API endpoint;
2. parses `Last-Event-ID` as the last sequence, not the UUID event ID;
3. queries persisted events with a greater sequence;
4. streams them in order;
5. subscribes to live events;
6. sends keepalive comments;
7. serializes each event as JSON and sets SSE `id` to the sequence.

- [ ] **Step 5: Run tests**

Run: `cargo test -p erabi-api --test sse_replay`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add Cargo.lock crates/erabi-jobs crates/erabi-db crates/erabi-api
git commit -m "feat(progress): persist and replay crawl SSE events"
```
