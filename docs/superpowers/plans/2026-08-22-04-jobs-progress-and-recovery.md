# Erabi Jobs, Progress, and Recovery Implementation Plan

> **For agentic workers:** Implement each durable-jobs feature end-to-end, then compile/check, add or update meaningful tests, run verification, and commit. Do not use test-first RED/GREEN sequencing by default.

**Goal:** Implement durable queued work, leases/heartbeats, replayable SSE progress, cancellation, checkpoints, retry/resume, panic isolation, and queue controls.

**Architecture:** Jobs are Turso-backed and leased by Tokio workers inside the single process. User-facing progress is durable/replayable and distinct from technical tracing logs.

**Tech Stack:** Tokio, Turso repositories, Axum SSE, tracing.

**Spec:** `docs/specs/03-discovery-graph-and-runs.md`, `docs/specs/06-security-reliability-and-operations.md`, `docs/specs/08-ux-accessibility-and-verification.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

**Migration ownership:** `migrations/0004_jobs.sql` only for durable jobs/attempts/checkpoints/progress events.

---

### Task 1: Durable queue, attempts, leases, and worker isolation

**Files:** job domain/repository/worker modules, `migrations/0004_jobs.sql`, focused tests.

**Requirements:**

- Persist job kind, priority, state, attempts, schedule time, lease owner/expiry, heartbeat, parent/run linkage, checkpoint linkage, created/updated timestamps.
- Lease acquisition is transactional and prevents double ownership.
- Startup recovers stale `RUNNING` jobs according to lease/checkpoint state.
- Retries are bounded and attempt history is preserved.
- Panic inside one worker/task marks only related work failed/recoverable and does not kill unrelated API/jobs.
- Critical queue invariants escalate to Recovery Mode rather than continuing corrupt scheduling.

**Verification:** deterministic lease-race, stale-recovery, bounded-attempt, panic-isolation, and queue-invariant tests.

---

### Task 2: Durable progress events and replayable SSE

**Files:** progress repository/service, SSE route, event DTOs/tests.

**Requirements:**

- Persist progress event with monotonic per-stream sequence/event ID **before** live broadcast.
- Reconnect with `Last-Event-ID` replays missed durable events then switches to live without gaps/duplicates.
- Stable user progress keys/steps are separate from raw tracing logs.
- Remote SSE inherits Plan 03 bearer/security rules.
- Terminal states close/complete streams predictably.

**Verification:** replay/no-duplicate/reconnect/terminal/auth tests, including reconnect during concurrent event publication.

---

### Task 3: Cooperative cancellation and checkpoints

**Files:** cancellation/checkpoint modules, repositories, tests.

**Requirements:**

- Cancellation stops scheduling new units, signals active work, persists a safe checkpoint when possible, then marks run/job `CANCELLED`.
- Checkpoint records completed/pending URLs, pagination/discovery state, failed units, artifacts, config/snapshot identity, and extraction state required for a safe resume.
- Resume requires compatible immutable snapshot/config hash.
- Semantic mismatch invalidates resume and requires a new run/restart.
- Checkpoints are durable before reporting resumability.

**Verification:** cancel mid-work, safe checkpoint persistence, incompatible-resume rejection, and crash/restart checkpoint recovery tests.

---

### Task 4: Retry/resume and queue actions

**Files:** job/run action service and API routes/tests.

**Actions:**

- Retry Failed Parts;
- Rerun Full Crawl;
- Resume valid checkpoint;
- Restart from beginning;
- prioritize/move queued work where supported;
- cancel;
- retry;
- remove only safe non-started work.

**Requirements:** preserve lineage/attempt history and immutable original run snapshot semantics. Do not mutate prior failure evidence to make a retry look like the first attempt.

**Verification:** API/service tests for every action, illegal-state rejection, lineage preservation, and same-run snapshot reuse.

---

### Task 5: Disk-pressure integration

**Files:** storage-pressure policy/service plus worker scheduling integration/tests.

**Requirements:**

- Warning/critical free-storage thresholds are observable.
- Critical pressure blocks new artifact-heavy work.
- Active work moves to safe checkpoints where possible.
- DB/UI/review/diagnostics remain available where safe.
- Never automatically delete user artifacts solely because disk is low.

**Verification:** threshold transition, blocked scheduling, checkpoint behavior, and no-auto-delete tests.

---

## Plan 04 Gate

```bash
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Confirm durable leases, stale recovery, SSE replay/no duplicate, cancel/checkpoint/resume mismatch, panic isolation, queue actions, attempt lineage, and storage-pressure blocking all pass with fresh evidence. Do not begin Plan 05 until the gate passes.
