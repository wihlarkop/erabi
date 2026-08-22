# Erabi API Security and Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the hardened Axum runtime, secure loopback/remote exposure, stable API/error/audit contracts, startup and Recovery Mode behavior, robots-override validation, and the fixed three-second graceful shutdown.

**Architecture:** One `erabi serve` process hosts REST, SSE, static UI, and later internal workers. Bootstrap secrets remain environment-only. Loopback is unauthenticated for the local operator; any non-loopback bind requires a shared bearer token and stricter remote defaults.

**Tech Stack:** stable Rust, Axum, Tokio, Tower/tower-http, Serde, `tracing`, `secrecy`, `dotenvy`.

**Spec:** `docs/specs/05-system-architecture-and-persistence.md`, `docs/specs/06-security-reliability-and-operations.md`  
**Spec revision:** `679b499e617fcef14e4e40b9a7fc826b379b8a30`

## Global Constraints

- Default bind is `127.0.0.1`; loopback requires no login in MVP.
- Non-loopback bind MUST fail startup without a non-empty `ERABI_ACCESS_TOKEN`.
- Bearer auth protects API, SSE, assets, exports, backups, raw-artifact access, and sensitive diagnostics on remote binds.
- Default deployment is same-origin; CORS is closed unless an explicit allowlist is configured.
- Wildcard CORS is forbidden while bearer auth is enabled.
- Secrets never enter Turso ordinary settings, URLs, logs, API examples, or OpenAPI examples.
- State-changing requests enforce Host/Origin/Content-Type/body limits and typed parsing.
- Recovery Mode disables mutations/new jobs and keeps only safe inspection/recovery surfaces.
- Graceful shutdown deadline is exactly three seconds for MVP.
- No telemetry is sent by default.

## Focused File Map

```text
crates/erabi-cli/src/config.rs
crates/erabi-cli/src/runtime.rs
crates/erabi-cli/src/main.rs
crates/erabi-api/src/app.rs
crates/erabi-api/src/error.rs
crates/erabi-api/src/security/
crates/erabi-api/src/routes/
crates/erabi-api/src/openapi.rs
crates/erabi-api/tests/
```

---

### Task 1: Load bootstrap configuration and enforce loopback/remote authentication invariants

**Files:**
- Create: `crates/erabi-cli/src/config.rs`
- Create: `crates/erabi-cli/src/lib.rs`
- Modify: `crates/erabi-cli/src/main.rs`
- Test: `crates/erabi-cli/tests/config_loading.rs`

**Interfaces:**
- Produces `BootstrapConfig::load()` and `BootstrapConfig::from_pairs()`.
- Produces `BindMode::{Loopback, Remote}` and secret-wrapped access/Crawl4AI/Turso tokens.

- [ ] **Step 1: Add stable configuration dependencies**

```bash
cargo add -p erabi dotenvy
cargo add -p erabi serde --features derive
cargo add -p erabi secrecy
cargo add -p erabi thiserror
cargo add -p erabi url
```

- [ ] **Step 2: Write failing configuration tests**

```rust
use erabi::{BootstrapConfig, ConfigError};

#[test]
fn loopback_bind_does_not_require_access_token() {
    let config = BootstrapConfig::from_pairs([
        ("ERABI_HOST", "127.0.0.1"),
        ("ERABI_PORT", "7878"),
    ]).unwrap();
    assert!(config.access_token().is_none());
}

#[test]
fn remote_bind_without_token_is_rejected() {
    let error = BootstrapConfig::from_pairs([
        ("ERABI_HOST", "0.0.0.0"),
        ("ERABI_PORT", "7878"),
    ]).unwrap_err();
    assert!(matches!(error, ConfigError::MissingAccessToken));
}

#[test]
fn empty_remote_token_is_rejected() {
    let error = BootstrapConfig::from_pairs([
        ("ERABI_HOST", "0.0.0.0"),
        ("ERABI_ACCESS_TOKEN", ""),
    ]).unwrap_err();
    assert!(matches!(error, ConfigError::MissingAccessToken));
}
```

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi --test config_loading
```

Expected: compile failure for missing configuration types.

- [ ] **Step 4: Implement typed bootstrap configuration**

`BootstrapConfig` fields:

```rust
pub struct BootstrapConfig {
    pub host: std::net::IpAddr,
    pub port: u16,
    pub data_dir: std::path::PathBuf,
    pub cors_allowed_origins: Vec<url::Url>,
    pub openapi_enabled: bool,
    pub crawl4ai_base_url: url::Url,
    access_token: Option<secrecy::SecretString>,
    crawl4ai_api_token: Option<secrecy::SecretString>,
    turso_database_url: Option<String>,
    turso_auth_token: Option<secrecy::SecretString>,
}
```

`load()` calls `dotenvy::dotenv().ok()` then reads OS environment so OS values override `.env`. Parse host as `IpAddr`; reject invalid/zero port and empty token values. `bind_mode()` returns Loopback only when `host.is_loopback()`.

Do not derive `Debug` for secret-bearing structures unless secret fields are explicitly redacted.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi --test config_loading
cargo clippy -p erabi --all-targets -- -D warnings
git add Cargo.lock crates/erabi-cli
 git commit -m "feat(runtime): validate secure bootstrap configuration"
```

---

### Task 2: Build Axum application shell, auth middleware, request hardening, and stable API errors

**Files:**
- Create: `crates/erabi-api/src/app.rs`
- Create: `crates/erabi-api/src/error.rs`
- Create: `crates/erabi-api/src/security/auth.rs`
- Create: `crates/erabi-api/src/security/origin.rs`
- Create: `crates/erabi-api/src/security/headers.rs`
- Create: `crates/erabi-api/src/security/mod.rs`
- Create: `crates/erabi-api/src/routes/health.rs`
- Modify: `crates/erabi-api/src/lib.rs`
- Test: `crates/erabi-api/tests/security_shell.rs`

**Interfaces:**
- Consumes `BootstrapConfig`, `ProductError`, database/application state.
- Produces `build_router(AppState, SecurityConfig) -> axum::Router`.
- Produces stable JSON `ApiErrorEnvelope`.

- [ ] **Step 1: Add stable HTTP dependencies**

```bash
cargo add -p erabi-api axum
cargo add -p erabi-api tokio --features macros,rt-multi-thread,signal,time
cargo add -p erabi-api tower
cargo add -p erabi-api tower-http --features cors,limit,trace,set-header,fs
cargo add -p erabi-api serde --features derive
cargo add -p erabi-api serde_json
cargo add -p erabi-api tracing
cargo add -p erabi-api uuid --features v7
cargo add -p erabi-api --path crates/erabi-domain erabi-domain
cargo add -p erabi-api --path crates/erabi-db erabi-db
```

- [ ] **Step 2: Write failing HTTP security tests**

Use `tower::ServiceExt::oneshot` against the router; do not open real sockets for these tests.

```rust
#[tokio::test]
async fn remote_router_rejects_missing_bearer_token() {
    let app = erabi_api::test_support::remote_router("secret-token").await;
    let response = app.oneshot(
        axum::http::Request::builder()
            .uri("/api/v1/health")
            .body(axum::body::Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn loopback_router_allows_health_without_auth() {
    let app = erabi_api::test_support::loopback_router().await;
    let response = app.oneshot(erabi_api::test_support::get("/api/v1/health")).await.unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
}
```

Also test: wildcard CORS + bearer config is rejected during router construction; malformed mutation Content-Type returns 415; oversized JSON returns 413; disallowed Origin returns 403; security headers are present.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-api --test security_shell
```

Expected: compile failure for missing router/security contracts.

- [ ] **Step 4: Implement exact security behavior**

`ApiErrorEnvelope`:

```rust
#[derive(serde::Serialize)]
pub struct ApiErrorEnvelope {
    pub code: erabi_domain::ErrorCode,
    pub message: String,
    pub details: serde_json::Value,
    pub recoverable: bool,
    pub suggested_actions: Vec<erabi_domain::SuggestedAction>,
    pub trace_id: String,
}
```

Generate a request trace UUID for every request and include it in errors/structured spans. Remote auth compares `Authorization: Bearer <token>` without logging either provided or expected token. Apply auth consistently to protected route groups; later SSE/file routes inherit the same security layer.

Default CSP/security headers must reject arbitrary inline execution and include `nosniff`, restrictive referrer policy, permissions policy, and frame restrictions. Preview CSP is implemented separately in Plan 07.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-api --test security_shell
cargo clippy -p erabi-api --all-targets -- -D warnings
git add Cargo.lock crates/erabi-api
 git commit -m "feat(api): add hardened Axum security shell"
```

---

### Task 3: Implement privacy-safe structured logs, durable audit events, and robots override validation

**Files:**
- Create: `crates/erabi-api/src/security/redaction.rs`
- Create: `crates/erabi-api/src/routes/run_safety.rs`
- Create: `crates/erabi-api/src/dto/run_safety.rs`
- Modify: `crates/erabi-api/src/app.rs`
- Modify: `crates/erabi-db/src/repositories/audit.rs`
- Test: `crates/erabi-api/tests/robots_override.rs`
- Test: `crates/erabi-api/tests/redaction.rs`

**Interfaces:**
- Produces `RobotsOverrideInput { enabled, reason }` validation.
- Produces `ValidatedRobotsDecision` convertible to domain `RobotsDecision`.
- Produces redaction helpers for URLs/headers/JSON metadata.

- [ ] **Step 1: Write failing robots-reason tests**

```rust
#[tokio::test]
async fn robots_override_requires_non_empty_reason() {
    let app = erabi_api::test_support::loopback_router().await;
    let response = erabi_api::test_support::post_json(
        app,
        "/api/v1/run-safety/validate",
        serde_json::json!({"override_robots": true, "reason": "   "}),
    ).await;
    assert_eq!(response.status(), axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn valid_override_returns_frozen_decision_fields() {
    let response = erabi_api::test_support::validate_robots_override("Public research exception").await;
    assert_eq!(response.reason, "Public research exception");
    assert!(!response.affected_origin.is_empty());
}
```

Add tests that a new independent run request does not default its reason from prior history; retry/resume behavior is covered in Plan 04 against the frozen snapshot.

- [ ] **Step 2: Write failing redaction tests**

Assert default formatting removes Authorization/Cookie values, query parameter values, secret-looking JSON fields, extracted values, and raw page content. Preserve scheme/host/path when logging URLs, replacing the query with a safe marker or omitting it.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-api --test robots_override --test redaction
```

- [ ] **Step 4: Implement validation/redaction/audit append**

Validate reason after trimming for emptiness; store the submitted reason text subject only to a documented max UTF-8 byte/character limit. The validated decision includes actor, timestamp, affected origin/scope, User-Agent, and optional Crawler/Crawler Version IDs before snapshot creation.

Append audit events through `AuditRepository`; event payloads contain IDs/safe metadata, not bearer tokens or extracted content. Required audit categories include Crawler publication, robots override, destructive deletion/restore, security-setting changes, and diagnostic-mode enable/disable.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-api --test robots_override --test redaction
cargo test -p erabi-api
git add crates/erabi-api crates/erabi-db
 git commit -m "feat(security): audit crawl overrides and redact sensitive data"
```

---

### Task 4: Implement ordered startup, single-instance lock, integrity checks, and Recovery Mode

**Files:**
- Create: `crates/erabi-cli/src/process_lock.rs`
- Create: `crates/erabi-cli/src/startup.rs`
- Create: `crates/erabi-api/src/recovery.rs`
- Create: `crates/erabi-api/src/routes/diagnostics.rs`
- Modify: `crates/erabi-cli/src/runtime.rs`
- Test: `crates/erabi-cli/tests/startup_sequence.rs`
- Test: `crates/erabi-api/tests/recovery_mode.rs`

**Interfaces:**
- Produces `ProcessLockGuard`.
- Produces `StartupState::{Ready, DegradedCrawlerUnavailable, RecoveryMode}`.
- Produces read-only Recovery Mode router surface.

- [ ] **Step 1: Write failing startup-order and Recovery Mode tests**

Use an injected `StartupProbe`/fake services to record step order without depending on a real Crawl4AI server:

```rust
#[tokio::test]
async fn startup_runs_integrity_before_workers_and_routes() {
    let events = erabi::test_support::run_startup_probe().await;
    assert!(events.position("integrity.checked") < events.position("runtime.started"));
}
```

Add tests: second process lock acquisition fails while first guard is alive; stale lock is reclaimed only after owner liveness check; migration/integrity failure enters Recovery Mode; Crawl4AI health failure results in degraded-but-usable state rather than Recovery Mode.

- [ ] **Step 2: Run RED**

```bash
cargo test -p erabi --test startup_sequence
cargo test -p erabi-api --test recovery_mode
```

- [ ] **Step 3: Implement the fixed startup sequence**

Implement exactly:

```text
resolve/canonicalize data directory
→ acquire exclusive process lock
→ validate bootstrap configuration
→ open DB
→ acquire migration lock + apply migrations
→ lightweight integrity check
→ verify artifact directories/permissions
→ recover stale jobs hook (Plan 04 plugs implementation here)
→ rebuild concurrency hook
→ health-check Crawl4AI
→ start routes/workers
→ readiness
```

Process lock metadata includes PID, start time, Erabi version, and bind address. Recovery Mode exposes diagnostics, migration retry hooks, backup verify/restore hooks, and safe read-only state where possible; it rejects normal mutation/job routes and retention cleanup.

- [ ] **Step 4: Run GREEN and commit**

```bash
cargo test -p erabi --test startup_sequence
cargo test -p erabi-api --test recovery_mode
git add crates/erabi-cli crates/erabi-api
 git commit -m "feat(runtime): add startup integrity and recovery mode"
```

---

### Task 5: Implement fixed three-second graceful shutdown and OpenAPI exposure policy

**Files:**
- Create: `crates/erabi-cli/src/shutdown.rs`
- Create: `crates/erabi-api/src/openapi.rs`
- Modify: `crates/erabi-cli/src/runtime.rs`
- Modify: `crates/erabi-api/src/app.rs`
- Test: `crates/erabi-cli/tests/graceful_shutdown.rs`
- Test: `crates/erabi-api/tests/openapi_policy.rs`

**Interfaces:**
- Produces `ShutdownCoordinator` with a fixed `Duration::from_secs(3)` deadline.
- Produces OpenAPI route policy based on bind mode + explicit remote opt-in.

- [ ] **Step 1: Write failing shutdown tests with paused Tokio time**

```rust
#[tokio::test(start_paused = true)]
async fn shutdown_deadline_is_three_seconds() {
    let coordinator = erabi::ShutdownCoordinator::test_fixture();
    let handle = tokio::spawn(async move { coordinator.shutdown().await });
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    assert!(handle.await.unwrap().deadline_reached_or_completed());
}
```

Also assert shutdown order: stop mutations/new work → signal cancellation → settle/rollback transactions → persist checkpoint hook → flush critical logs → release process lock. Long crawl work must not extend the deadline.

- [ ] **Step 2: Write OpenAPI policy tests**

Loopback + default enabled exposes docs/schema. Remote bind + no explicit opt-in does not expose docs. Remote opt-in still requires bearer auth. Example payloads must be synthetic and contain no user secrets/content.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi --test graceful_shutdown
cargo test -p erabi-api --test openapi_policy
```

- [ ] **Step 4: Implement shutdown coordinator and OpenAPI policy**

Use one cancellation token/broadcast mechanism owned by runtime. Keep Plan 04 worker checkpointing behind a hook registered with `ShutdownCoordinator`; do not invent a second worker shutdown path later.

Generate OpenAPI from real route contracts using a current stable compatible library selected at execution time with `cargo add`; do not hand-maintain a divergent JSON schema file.

- [ ] **Step 5: Run the full Plan 03 gate and commit**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p erabi-api
cargo test -p erabi
```

Expected: all exit 0 and cover loopback/remote auth, CORS/origin/body policy, redaction, robots reason, startup/recovery, OpenAPI remote protection, and 3-second shutdown.

```bash
git add Cargo.lock crates/erabi-api crates/erabi-cli
 git commit -m "feat(runtime): enforce graceful shutdown and API exposure policy"
```

## Plan 03 Gate

Do not start Plan 04 until Task 5 Step 5 passes from a clean checkout and `git status --short` is empty.
