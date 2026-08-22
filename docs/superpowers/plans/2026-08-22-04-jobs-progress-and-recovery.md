# Erabi Jobs, Progress, and Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement durable queued work, leases/heartbeats, replayable SSE progress, cancellation, checkpoints, retry/resume, panic isolation, and queue controls.

**Architecture:** Jobs are Turso-backed and leased by Tokio workers inside the single process. User-facing progress is durable/replayable and distinct from technical tracing logs.

**Tech Stack:** Tokio, Turso repositories, Axum SSE, tracing.

**Spec:** `docs/specs/03-discovery-graph-and-runs.md`, `06-security-reliability-and-operations.md`, `08-ux-accessibility-and-verification.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

### Task 1: Durable job queue and leases

- [ ] Model job kind, priority, attempts, schedule time, lease owner/expiry, heartbeat, parent linkage, checkpoint linkage.
- [ ] Transactionally acquire leases and recover stale `RUNNING` jobs after startup.
- [ ] Bound retries and isolate worker panics without terminating unrelated API/jobs.

### Task 2: Durable progress event stream

- [ ] Persist monotonic event sequence + event ID before broadcast.
- [ ] SSE reconnect with `Last-Event-ID` replays missed durable events then switches live without duplication.
- [ ] Separate stable user progress keys from technical logs.

### Task 3: Cancellation and checkpoints

- [ ] Cooperative cancel stops new scheduling, signals active tasks, persists safe checkpoint, marks `CANCELLED`.
- [ ] Checkpoint includes completed/pending URLs, pagination state, failed units, artifacts, config hash/version, extraction state.
- [ ] Semantic config mismatch makes resume invalid and requires new run.

### Task 4: Retry/resume and queue actions

- [ ] Implement Retry Failed Parts, Rerun Full Crawl, Resume valid checkpoint, Restart from beginning.
- [ ] Preserve prior failures/attempt lineage.
- [ ] Implement queue prioritize/move/cancel/resume/retry/remove-safe-nonstarted controls.

### Task 5: Disk-pressure integration

- [ ] Critical storage blocks new artifact-heavy work and checkpoints active work where safe.
- [ ] Never auto-delete user artifacts solely because disk is low.

**Gate:** deterministic tests cover leases, stale recovery, SSE replay/no duplicate, cancel/checkpoint/resume mismatch, panic isolation, queue actions, and storage blocking.
