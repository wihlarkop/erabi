# Erabi API Security and Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the hardened Axum runtime, secure local/network exposure, API contracts, audit primitives, startup/recovery behavior, and three-second graceful shutdown.

**Architecture:** One `erabi serve` process serves REST/SSE/static UI and internal workers. Loopback is open for the local operator; non-loopback requires bearer token. Security-relevant run choices are validated before snapshot creation.

**Tech Stack:** Axum, Tower/tower-http, Tokio, Serde, tracing.

**Spec:** `docs/specs/05-system-architecture-and-persistence.md`, `06-security-reliability-and-operations.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

### Task 1: Bootstrap secure Axum server

- [ ] Implement `/api/v1`, health/readiness, static SPA serving, same-origin defaults.
- [ ] Test `127.0.0.1` requires no login and non-loopback without `ERABI_ACCESS_TOKEN` refuses startup.
- [ ] Enforce Host/Origin/Content-Type/body-size parsing policies and no wildcard CORS with bearer auth.
- [ ] Protect SSE/assets/exports/backups/raw artifacts/diagnostics consistently.

### Task 2: Implement stable API error/audit envelope

- [ ] Return stable error code, user message, trace ID, structured details, recoverability/safe actions.
- [ ] Implement append-only audit events for version publication, security settings, robots overrides, deletion, restore, diagnostic-mode changes.
- [ ] Redact secret headers, bodies, extracted values, raw content, and URL query values from default logs.

### Task 3: Enforce robots override reason contract

- [ ] API tests reject override enabled with empty/missing reason before run creation.
- [ ] Validate bounded non-empty reason and include it in immutable snapshot/audit event with actor/time/origin/User-Agent/CrawlerVersion when applicable.
- [ ] Retry/resume same run preserves frozen reason; new independent run must provide reason again.

### Task 4: Startup, process lock, integrity, and Recovery Mode

- [ ] Implement ordered startup: data dir → lock → bootstrap → DB/migrations → lightweight integrity → artifacts → stale jobs → Crawl4AI health → routes/workers.
- [ ] Test Crawl4AI outage leaves existing UI/data/diagnostics available.
- [ ] Migration/invariant failures expose limited Recovery Mode and disable mutations/jobs.

### Task 5: Graceful shutdown and OpenAPI policy

- [ ] Test fixed 3-second shutdown deadline with cooperative cancellation/checkpoint persistence.
- [ ] OpenAPI enabled by default on loopback; remote docs disabled unless explicitly enabled and token-protected.

**Gate:** security integration tests cover loopback/remote auth, CORS/origin, robots reason, redaction, Recovery Mode, and 3-second shutdown.
