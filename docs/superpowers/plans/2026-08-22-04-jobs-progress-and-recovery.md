# Erabi Jobs, Progress, and Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement durable queued work, transactional leases/heartbeats, replayable SSE progress, panic isolation, cancellation/checkpoints, retry/resume, queue controls, and storage-pressure admission gates.

**Architecture:** Jobs and progress events are persisted in Turso before they are exposed live. Tokio workers run inside the single `erabi serve` process and lease durable jobs transactionally. User-facing progress events are stable product events; technical tracing remains a separate log surface.

**Tech Stack:** stable Rust, Tokio, official Turso repositories, Axum SSE/fetch streaming, Serde, `tracing`.

**Spec:** `docs/specs/03-discovery-graph-and-runs.md`, `docs/specs/06-security-reliability-and-operations.md`, `docs/specs/08-ux-accessibility-and-verification.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Global Constraints

- Jobs are durable and cannot rely on in-memory identity for correctness.
- MVP runs one active Erabi process per data directory; job data must still survive process restart.
- Progress event IDs/sequences are durable and monotonic per run/job stream.
- SSE reconnect using `Last-Event-ID` replays missed durable events without duplication.
- Cancellation is cooperative and stops new scheduling before checkpointing.
- Resume is valid only when the checkpoint semantic/config hash matches the immutable run snapshot.
- Retry/resume never erase prior attempts or failure evidence.
- Worker panics must not terminate unrelated API/jobs.
- Critical storage pressure blocks new artifact-heavy work; it never auto-deletes user data.

## Focused File Map

```text
migrations/0004_jobs.sql
crates/erabi-jobs/src/model.rs
crates/erabi-jobs/src/runtime.rs
crates/erabi-jobs/src/worker.rs
crates/erabi-jobs/src/checkpoint.rs
crates/erabi-jobs/src/cancel.rs
crates/erabi-db/src/repositories/jobs.rs
crates/erabi-db/src/repositories/progress.rs
crates/erabi-api/src/routes/events.rs
crates/erabi-api/src/routes/jobs.rs
```

---

### Task 1: Persist jobs, attempts, checkpoints, and progress events

**Files:**
- Create: `migrations/0004_jobs.sql`
- Create: `crates/erabi-jobs/src/model.rs`
- Modify: `crates/erabi-jobs/src/lib.rs`
- Create: `crates/erabi-db/src/repositories/jobs.rs`
- Create: `crates/erabi-db/src/repositories/progress.rs`
- Modify: `crates/erabi-db/src/repositories/mod.rs`
- Test: `crates/erabi-db/tests/job_repository.rs`

**Interfaces:**
- Produces `Job`, `JobId`, `JobKind`, `JobStatus`, `JobLease`, `JobAttempt`, `CheckpointRef`, `ProgressEvent`.
- Produces `JobRepository::enqueue`, `lease_next`, `heartbeat`, `complete`, `fail`, `release_stale`.
- Produces `ProgressRepository::append` and `events_after`.

- [ ] **Step 1: Define exact durable job kinds/statuses and write failing serialization tests**

Create `crates/erabi-jobs/src/model.rs` tests or integration test:

```rust
#[test]
fn job_status_serializes_stably() {
    assert_eq!(serde_json::to_string(&erabi_jobs::JobStatus::Running).unwrap(), r#""RUNNING""#);
}

#[test]
fn required_initial_job_kinds_are_present() {
    use erabi_jobs::JobKind;
    let kinds = [
        JobKind::CrawlPage,
        JobKind::DiscoverClassify,
        JobKind::ExtractRecords,
        JobKind::ValidateRecords,
        JobKind::DownloadAsset,
        JobKind::ExportDataset,
        JobKind::RetentionCleanup,
        JobKind::Backup,
        JobKind::IntegrityCheck,
    ];
    assert_eq!(kinds.len(), 9);
}
```

Use statuses `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`, `RECOVERABLE` for job units; Crawl Run status remains the domain enum from Plan 01.

- [ ] **Step 2: Write failing transactional lease test**

```rust
#[tokio::test]
async fn concurrent_workers_cannot_lease_the_same_job() {
    let fixture = erabi_db::test_support::job_queue_fixture().await;
    let job_id = fixture.enqueue_one().await;
    let (a, b) = tokio::join!(
        fixture.jobs.lease_next("worker-a", fixture.now_ms(), 30_000),
        fixture.jobs.lease_next("worker-b", fixture.now_ms(), 30_000),
    );
    let leased = [a.unwrap(), b.unwrap()].into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].job_id, job_id);
}
```

Also test stale lease recovery requires expired lease/heartbeat and increments attempt lineage rather than rewriting the prior attempt.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-db --test job_repository
```

- [ ] **Step 4: Implement `0004_jobs.sql` and repositories**

Migration owns:

```text
jobs
job_attempts
job_checkpoints
progress_events
```

`jobs` stores job ID, run ID when applicable, kind, status, priority, scheduled-at, parent job ID, current checkpoint ID, lease owner, lease expiry, heartbeat timestamp, created/updated timestamps. `job_attempts` is append-only attempt history. `progress_events` has `(stream_id, sequence)` unique constraint and durable event ID.

Lease acquisition must be one DB transaction using a compare/update condition on `QUEUED`/expired state. Never implement “SELECT then later UPDATE” without transactional protection.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-db --test job_repository
cargo test -p erabi-db
git add migrations/0004_jobs.sql crates/erabi-jobs crates/erabi-db
 git commit -m "feat(jobs): persist durable queue and leases"
```

---

### Task 2: Implement Tokio worker runtime, heartbeats, stale recovery, and panic isolation

**Files:**
- Create: `crates/erabi-jobs/src/runtime.rs`
- Create: `crates/erabi-jobs/src/worker.rs`
- Modify: `crates/erabi-jobs/src/lib.rs`
- Modify: `crates/erabi-cli/src/runtime.rs`
- Test: `crates/erabi-jobs/tests/worker_runtime.rs`

**Interfaces:**
- Produces `JobHandler` trait and `JobRuntime`.
- Produces registration `JobRuntime::register(kind, handler)`.
- Integrates with startup stale-job recovery hook from Plan 03.

- [ ] **Step 1: Add runtime dependencies**

```bash
cargo add -p erabi-jobs tokio --features sync,time,rt-multi-thread,macros
cargo add -p erabi-jobs async-trait
cargo add -p erabi-jobs futures-util
cargo add -p erabi-jobs tracing
cargo add -p erabi-jobs thiserror
cargo add -p erabi-jobs --path crates/erabi-domain erabi-domain
cargo add -p erabi-jobs --path crates/erabi-db erabi-db
```

- [ ] **Step 2: Write failing worker/heartbeat/panic tests**

```rust
#[tokio::test(start_paused = true)]
async fn running_job_renews_heartbeat_before_lease_expiry() {
    let fixture = erabi_jobs::test_support::runtime_with_blocking_handler().await;
    fixture.start_one().await;
    tokio::time::advance(std::time::Duration::from_secs(10)).await;
    assert!(fixture.latest_heartbeat_ms().await > fixture.initial_heartbeat_ms());
}

#[tokio::test]
async fn panicking_handler_fails_only_its_job() {
    let fixture = erabi_jobs::test_support::runtime_with_panicking_and_success_handlers().await;
    let result = fixture.run_both().await;
    assert_eq!(result.panicked_job_status, erabi_jobs::JobStatus::Failed);
    assert_eq!(result.other_job_status, erabi_jobs::JobStatus::Succeeded);
}
```

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-jobs --test worker_runtime
```

- [ ] **Step 4: Implement runtime boundary**

```rust
#[async_trait::async_trait]
pub trait JobHandler: Send + Sync {
    async fn handle(&self, context: JobContext) -> Result<JobOutcome, JobHandlerError>;
}
```

`JobRuntime` leases one job, creates an attempt, starts a heartbeat task, invokes the handler behind panic isolation (`FutureExt::catch_unwind` with `AssertUnwindSafe` only at the outer worker boundary), then atomically records completion/failure/recoverability. Concurrency is bounded by a Tokio semaphore configured from resolved system settings. Do not hold DB transactions across network/long handler work.

Startup hook calls `release_stale`/recovery classification before new workers start.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-jobs --test worker_runtime
cargo clippy -p erabi-jobs --all-targets -- -D warnings
git add Cargo.lock crates/erabi-jobs crates/erabi-cli
 git commit -m "feat(jobs): run leased jobs with panic isolation"
```

---

### Task 3: Persist user progress and expose replayable authenticated SSE

**Files:**
- Create: `crates/erabi-jobs/src/progress.rs`
- Create: `crates/erabi-api/src/routes/events.rs`
- Modify: `crates/erabi-api/src/app.rs`
- Test: `crates/erabi-api/tests/sse_replay.rs`
- Test: `crates/erabi-jobs/tests/progress_sequence.rs`

**Interfaces:**
- Produces `ProgressPublisher::publish(stream_id, key, args, level)`.
- Produces `GET /api/v1/runs/{id}/events` SSE endpoint.

- [ ] **Step 1: Write failing monotonic sequence test**

```rust
#[tokio::test]
async fn progress_is_persisted_before_live_delivery() {
    let fixture = erabi_jobs::test_support::progress_fixture().await;
    let first = fixture.publisher.publish("run-1", "crawl.started", serde_json::json!({})).await.unwrap();
    let second = fixture.publisher.publish("run-1", "page.loading", serde_json::json!({"page": 1})).await.unwrap();
    assert_eq!(first.sequence + 1, second.sequence);
    assert_eq!(fixture.persisted().await.len(), 2);
}
```

- [ ] **Step 2: Write failing SSE replay/no-duplicate test**

Seed durable events 1–5, connect with `Last-Event-ID` for event 2, and assert response begins with events 3,4,5 exactly once before later live event 6. On remote security fixture, missing bearer token must return 401 before streaming.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-jobs --test progress_sequence
cargo test -p erabi-api --test sse_replay
```

- [ ] **Step 4: Implement persisted-then-broadcast flow**

Progress payload is stable product data:

```rust
pub struct ProgressEvent {
    pub event_id: String,
    pub stream_id: String,
    pub sequence: i64,
    pub key: String,
    pub args: serde_json::Value,
    pub level: ProgressLevel,
    pub occurred_at_unix_ms: i64,
}
```

Do not embed localized prose in durable events; UI translates `key` + args. SSE route reads `Last-Event-ID`, resolves the corresponding sequence, replays DB events after it, subscribes to the live broadcaster, drops duplicates by sequence, and emits heartbeats/comments as needed. Technical tracing events are not automatically mirrored as user progress.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-jobs --test progress_sequence
cargo test -p erabi-api --test sse_replay
git add crates/erabi-jobs crates/erabi-api crates/erabi-db
 git commit -m "feat(progress): replay durable run events over SSE"
```

---

### Task 4: Implement cooperative cancellation, checkpoints, retry, and resume validation

**Files:**
- Create: `crates/erabi-jobs/src/cancel.rs`
- Create: `crates/erabi-jobs/src/checkpoint.rs`
- Create: `crates/erabi-jobs/src/retry.rs`
- Create: `crates/erabi-api/src/routes/jobs.rs`
- Modify: `crates/erabi-cli/src/shutdown.rs`
- Test: `crates/erabi-jobs/tests/checkpoint_resume.rs`
- Test: `crates/erabi-api/tests/job_actions.rs`

**Interfaces:**
- Produces `CancellationRegistry`, `Checkpoint`, `ResumeDecision`, `RetryMode`.
- Produces job/run actions: cancel, retry failed parts, rerun full, resume checkpoint, restart beginning.

- [ ] **Step 1: Write failing checkpoint compatibility tests**

```rust
#[test]
fn semantic_hash_mismatch_forces_new_run() {
    let checkpoint = erabi_jobs::test_support::checkpoint("config-a");
    assert_eq!(checkpoint.resume_decision("config-b"), erabi_jobs::ResumeDecision::RequiresNewRun);
}

#[test]
fn matching_snapshot_hash_is_resumable() {
    let checkpoint = erabi_jobs::test_support::checkpoint("config-a");
    assert_eq!(checkpoint.resume_decision("config-a"), erabi_jobs::ResumeDecision::ResumeSameRun);
}
```

Checkpoint fields include completed/pending canonical URLs, pagination/discovery state, failed units, artifact refs, run config hash, Crawler Version ID when present, extraction state, and persisted timestamp.

- [ ] **Step 2: Write failing cancel/action tests**

Assert cancel stops new child scheduling before status becomes `CANCELLED`; safe checkpoint is persisted; retry creates a new attempt lineage; same-run resume loads the original immutable snapshot including original robots override reason.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-jobs --test checkpoint_resume
cargo test -p erabi-api --test job_actions
```

- [ ] **Step 4: Implement cancellation/action workflow**

Cancellation sequence:

```text
mark cancellation requested
→ stop scheduling children
→ signal active handler cancellation token
→ handler reaches safe unit boundary
→ persist checkpoint
→ persist terminal job/run cancellation state
→ publish cancellation progress
```

Retry Failed Parts creates attempts/child work only for failed units and links to original run. Rerun Full Crawl creates a new Crawl Run snapshot through later run-start service; it must not mutate the prior run. Restart beginning of same recoverable run is permitted only where the immutable snapshot remains the same and product semantics allow it.

Register checkpoint flush with Plan 03 `ShutdownCoordinator` so shutdown does not create a second cancellation model.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-jobs --test checkpoint_resume
cargo test -p erabi-api --test job_actions
git add crates/erabi-jobs crates/erabi-api crates/erabi-cli
 git commit -m "feat(jobs): checkpoint cancel retry and resume"
```

---

### Task 5: Implement queue controls and storage-pressure admission

**Files:**
- Create: `crates/erabi-jobs/src/admission.rs`
- Modify: `crates/erabi-api/src/routes/jobs.rs`
- Create: `crates/erabi-api/src/routes/queue.rs`
- Test: `crates/erabi-api/tests/queue_controls.rs`
- Test: `crates/erabi-jobs/tests/storage_admission.rs`

**Interfaces:**
- Produces `AdmissionDecision::{Allowed, BlockedStorageCritical}`.
- Produces safe queue actions prioritize, move-down, cancel, resume, retry, remove queued-not-started.

- [ ] **Step 1: Write failing queue safety tests**

Assert removing a `RUNNING` job is rejected; removing an untouched `QUEUED` job succeeds with audit event; reprioritization preserves job identity/history.

- [ ] **Step 2: Write failing storage-critical admission test**

```rust
#[test]
fn artifact_heavy_job_is_blocked_at_critical_storage() {
    let policy = erabi_jobs::AdmissionPolicy::new(erabi_jobs::StorageState::Critical);
    assert_eq!(policy.check(erabi_jobs::JobKind::CrawlPage), erabi_jobs::AdmissionDecision::BlockedStorageCritical);
    assert_eq!(policy.check(erabi_jobs::JobKind::IntegrityCheck), erabi_jobs::AdmissionDecision::Allowed);
}
```

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-api --test queue_controls
cargo test -p erabi-jobs --test storage_admission
```

- [ ] **Step 4: Implement safe admission/actions**

Storage state is provided by a small runtime service; Plan 08 implements filesystem threshold measurement/cleanup UI. At Critical, block new artifact-heavy crawl/download/export/backup jobs and request safe checkpoints for active heavy work when possible. Review, DB reads, diagnostics, integrity operations, and safe cleanup preview remain available. Never call destructive cleanup automatically.

- [ ] **Step 5: Run the full Plan 04 gate and commit**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p erabi-jobs
cargo test -p erabi-api
cargo test -p erabi-db --test job_repository
```

Expected: all exit 0 with deterministic coverage for lease exclusivity, heartbeat/stale recovery, panic isolation, persisted SSE replay/no duplicates, cancel/checkpoint/config mismatch, retry lineage, queue actions, and storage admission.

```bash
git add crates/erabi-jobs crates/erabi-api
 git commit -m "feat(jobs): add queue controls and storage admission"
```

## Plan 04 Gate

Do not start Plan 05 until Task 5 Step 5 passes from a clean checkout and `git status --short` is empty.
