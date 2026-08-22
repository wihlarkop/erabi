# Erabi API Security and Runtime Implementation Plan

> **For agentic workers:** Implement each runtime/security feature completely, then compile/check, add or update meaningful tests, run verification, and commit. Do not use RED/GREEN or intentionally failing tests as the default workflow.

**Goal:** Build the hardened Axum runtime, secure loopback/remote exposure, stable API/error/audit contracts, startup and Recovery Mode behavior, robots-override validation, and the fixed three-second graceful shutdown.

**Architecture:** One `erabi serve` process hosts REST, SSE, static UI, and later internal workers. Bootstrap secrets remain environment-only. Loopback is unauthenticated for the local operator; any non-loopback bind requires a shared bearer token and stricter remote defaults.

**Tech Stack:** stable Rust, Axum, Tokio, Tower/tower-http, Serde, `tracing`, `secrecy`, `dotenvy`.

**Spec:** `docs/specs/05-system-architecture-and-persistence.md`, `docs/specs/06-security-reliability-and-operations.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Global Constraints

- Default bind is `127.0.0.1`; loopback requires no login in MVP.
- Non-loopback bind MUST refuse startup without non-empty `ERABI_ACCESS_TOKEN`.
- Bearer auth protects API, SSE, assets, exports, backups, raw artifacts, and sensitive diagnostics remotely.
- Same-origin/CORS closed by default; wildcard CORS is forbidden with bearer auth.
- Secrets never enter Turso ordinary settings, URLs, logs, API examples, or OpenAPI examples.
- State-changing requests enforce Host/Origin/Content-Type/body limits and typed parsing.
- Recovery Mode disables normal mutations/new jobs and preserves safe inspection/recovery surfaces.
- Graceful shutdown deadline is exactly 3 seconds.
- No telemetry is sent by default.

---

### Task 1: Bootstrap configuration and secure bind rules

**Files:** `crates/erabi-cli/src/config.rs`, `lib.rs`, `main.rs`, config tests.

**Interfaces:** `BootstrapConfig`, `BindMode::{Loopback, Remote}`, typed config errors, secret-wrapped token accessors.

**Implementation requirements:**

- `.env` fallback plus OS environment precedence.
- Parse host/port/data-dir/CORS/OpenAPI/Crawl4AI/Turso bootstrap values.
- Keep access/Crawl4AI/Turso tokens secret-wrapped and redacted.
- Remote bind without token or with empty token fails before serving.
- Loopback may run without login.

**Verification:** tests for loopback/remote/invalid host/empty token and secret redaction, then:

```bash
cargo test -p erabi --test config_loading
cargo clippy -p erabi --all-targets -- -D warnings
```

---

### Task 2: Build hardened Axum shell and stable API errors

**Files:** `crates/erabi-api/src/app.rs`, `error.rs`, `security/{auth,origin,headers}.rs`, health routes, integration tests.

**Interface:** `build_router(AppState, SecurityConfig) -> axum::Router` and stable `ApiErrorEnvelope` with code, message, structured details, recoverability/actions, and trace ID.

**Requirements:**

- `/api/v1` base path, health/readiness, SPA/static serving.
- Bearer middleware applies consistently to protected remote route groups.
- Reject malformed mutation Content-Type/body/Origin/Host policies with stable error codes.
- Apply CSP, nosniff, restrictive referrer/permissions/frame policies.
- Generate request trace IDs and structured spans.
- Never log bearer tokens/request bodies/extracted values by default.

**Verification:** router-level tests via `tower::ServiceExt`, covering remote auth, loopback, CORS, body limits, content type, origin, and security headers.

```bash
cargo test -p erabi-api --test security_shell
cargo test -p erabi-api
```

---

### Task 3: Audit/redaction and robots override contract

**Files:** redaction module, run-safety DTO/route, audit repository integration, tests.

**Requirements:**

- Robots override enabled => explicit non-empty bounded reason before run create/resume.
- Validated decision includes actor, timestamp, affected origin/scope, User-Agent, optional Crawler/Version.
- New independent run never silently inherits an older reason; same immutable run retry/resume may reuse frozen reason.
- Audit security-sensitive events append-only.
- Default logging redacts Authorization/Cookie/secret connection strings/query values/request bodies/extracted values/raw page content.

**Verification:** tests for reason validation, independent-run reason behavior, audit payload safety, and redaction.

```bash
cargo test -p erabi-api --test robots_override --test redaction
```

---

### Task 4: Ordered startup, single-instance lock, integrity, Recovery Mode

**Files:** `crates/erabi-cli/src/process_lock.rs`, `startup.rs`, `runtime.rs`; API recovery/diagnostics modules and tests.

**Startup order:**

```text
resolve/canonicalize data dir
→ exclusive process lock
→ bootstrap validation
→ DB open
→ migration lock/apply
→ lightweight integrity
→ artifact dirs/permissions
→ stale-job recovery hook
→ concurrency-state rebuild hook
→ Crawl4AI health
→ routes/workers
→ readiness
```

Crawl4AI outage yields degraded-but-usable state, not Recovery Mode. Migration/invariant corruption risk enters Recovery Mode. Process lock includes PID/start/version/bind metadata and stale reclaim requires owner-liveness verification.

**Verification:** injected startup-order tests, lock contention/stale tests, migration/integrity Recovery Mode tests, Crawl4AI-degraded tests.

---

### Task 5: Three-second graceful shutdown and OpenAPI exposure

**Files:** shutdown/runtime/OpenAPI modules and tests.

**Requirements:**

- Stop new mutations/jobs first.
- Signal cooperative cancellation/checkpoint hooks.
- Safely settle/rollback DB transactions.
- Flush critical audit/error state and release lock/resources.
- Exit by fixed 3-second deadline; do not wait for long crawls/downloads to finish naturally.
- OpenAPI enabled by default on loopback.
- Remote OpenAPI disabled unless explicit opt-in and then bearer-protected.

**Verification:** deterministic/tokio-time shutdown tests and loopback/remote OpenAPI policy tests.

---

## Plan 03 Gate

```bash
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Confirm remote-bind refusal without token, closed/default CORS, consistent protected routes, robots reason lifecycle, privacy redaction, ordered startup, Crawl4AI degraded availability, Recovery Mode restrictions, 3-second shutdown, and OpenAPI policy. Do not begin Plan 04 until the gate passes.
