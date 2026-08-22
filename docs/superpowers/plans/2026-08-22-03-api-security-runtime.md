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
crates/erabi-cli/src/process_lock.rs
crates/erabi-cli/src/startup.rs
crates/erabi-cli/src/shutdown.rs
crates/erabi-api/src/app.rs
crates/erabi-api/src/error.rs
crates/erabi-api/src/security/
crates/erabi-api/src/routes/
crates/erabi-api/src/openapi.rs
```

---

### Task 1: Load bootstrap configuration and enforce loopback/remote authentication invariants

**Files:**
- Create: `crates/erabi-cli/src/config.rs`
- Create: `crates/erabi-cli/src/lib.rs`
- Modify: `crates/erabi-cli/src/main.rs`
- Test: `crates/erabi-cli/tests/config_loading.rs`

**Interfaces:**
- Produces `BootstrapConfig::load()` / `BootstrapConfig::from_pairs()`.
- Produces `BindMode::{Loopback, Remote}` and secret-wrapped access/Crawl4AI/Turso tokens.

- [ ] **Step 1: Add stable dependencies**

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
fn loopback_does_not_require_access_token() {
    let config = BootstrapConfig::from_pairs([
        ("ERABI_HOST", "127.0.0.1"),
        ("ERABI_PORT", "7878"),
    ]).unwrap();
    assert!(config.access_token().is_none());
}

#[test]
fn remote_without_token_is_rejected() {
    let error = BootstrapConfig::from_pairs([
        ("ERABI_HOST", "0.0.0.0"),
        ("ERABI_PORT", "7878"),
    ]).unwrap_err();
    assert!(matches!(error, ConfigError::MissingAccessToken));
}
```

Add an empty-token case and invalid host/port cases.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi --test config_loading
```

Expected: compile failure for missing configuration types.

- [ ] **Step 4: Implement typed bootstrap configuration**

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

`load()` calls `dotenvy::dotenv().ok()` before OS env reads so OS env wins. Parse host as `IpAddr`; reject invalid/zero port and empty remote token. `bind_mode()` returns Loopback only for `host.is_loopback()`. Do not derive secret-revealing Debug output.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi --test config_loading
cargo clippy -p erabi --all-targets -- -D warnings
git add Cargo.lock crates/erabi-cli
git commit -m "feat(runtime): validate secure bootstrap configuration"
```

---

### Task 2: Build the Axum shell, auth/request-hardening middleware, security headers, and stable API errors

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

- [ ] **Step 2: Write failing router-security tests using `tower::ServiceExt::oneshot`**

```rust
#[tokio::test]
async fn remote_router_rejects_missing_bearer_token() {
    let app = erabi_api::test_support::remote_router("secret-token").await;
    let response = app.oneshot(erabi_api::test_support::get("/api/v1/health")).await.unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn loopback_router_allows_health_without_auth() {
    let app = erabi_api::test_support::loopback_router().await;
    let response = app.oneshot(erabi_api::test_support::get("/api/v1/health")).await.unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
}
```

Also test wildcard CORS + bearer config rejected during construction, malformed mutation Content-Type → 415, oversized JSON → 413, disallowed Origin → 403, and security headers present.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-api --test security_shell
```

- [ ] **Step 4: Implement exact error/security behavior**

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

Generate a request trace UUID for each request. Compare Bearer tokens without logging either token. Protect later SSE/file routes by placing them in the same security layer. Default headers include strict CSP appropriate to app shell, `nosniff`, restrictive referrer/permissions/frame policy. Sanitized preview gets its separate stricter policy in Plan 07.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-api --test security_shell
cargo clippy -p erabi-api --all-targets -- -D warnings
git add Cargo.lock crates/erabi-api
git commit -m "feat(api): add hardened Axum security shell"
```

---

### Task 3: Implement redaction/audit primitives and robots override reason validation

**Files:**
- Create: `crates/erabi-api/src/security/redaction.rs`
- Create: `crates/erabi-api/src/routes/run_safety.rs`
- Create: `crates/erabi-api/src/dto/run_safety.rs`
- Modify: `crates/erabi-api/src/app.rs`
- Modify: `crates/erabi-db/src/repositories/audit.rs`
- Test: `crates/erabi-api/tests/robots_override.rs`
- Test: `crates/erabi-api/tests/redaction.rs`

**Interfaces:**
- Produces `RobotsOverrideInput` validation and `ValidatedRobotsDecision` convertible to Plan 02 `RobotsDecision`.
- Produces URL/header/JSON redaction helpers.

- [ ] **Step 1: Write failing robots-reason tests**

```rust
#[tokio::test]
async fn override_requires_non_empty_reason() {
    let response = erabi_api::test_support::validate_robots_override("   ").await;
    assert_eq!(response.status(), axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}
```

Valid result must carry submitted reason, actor, timestamp, affected origin/scope, User-Agent, optional Crawler/Version IDs. A new independent run request must not inherit a previous reason.

- [ ] **Step 2: Write failing redaction tests**

Assert default logs omit/redact Authorization/Cookie values, token/password/secret-like fields, body/extracted values/raw page content, and URL query values while preserving safe scheme/host/path context.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi-api --test robots_override --test redaction
```

- [ ] **Step 4: Implement validation/redaction/audit append**

Reject blank-after-trim reason; bound UTF-8 length but preserve accepted submitted text. Append audit events through `AuditRepository`; security event payloads use safe IDs/metadata only. Required categories include Crawler publication, robots override, destructive deletion/restore, security changes, diagnostic-mode enable/disable.

- [ ] **Step 5: Run GREEN and commit**

```bash
cargo test -p erabi-api --test robots_override --test redaction
cargo test -p erabi-api
git add crates/erabi-api crates/erabi-db
git commit -m "feat(security): audit overrides and redact sensitive data"
```

---

### Task 4: Create runtime orchestration, ordered startup, process lock, integrity checks, and Recovery Mode

**Files:**
- Create: `crates/erabi-cli/src/runtime.rs`
- Create: `crates/erabi-cli/src/process_lock.rs`
- Create: `crates/erabi-cli/src/startup.rs`
- Create: `crates/erabi-api/src/recovery.rs`
- Create: `crates/erabi-api/src/routes/diagnostics.rs`
- Test: `crates/erabi-cli/tests/startup_sequence.rs`
- Test: `crates/erabi-api/tests/recovery_mode.rs`

**Interfaces:**
- Produces `Runtime`, `ProcessLockGuard`, `StartupState::{Ready, DegradedCrawlerUnavailable, RecoveryMode}`.
- Produces read-only Recovery Mode route surface.
- Creates the `runtime.rs` file consumed by Plans 04 and 06.

- [ ] **Step 1: Write failing startup-order and lock tests**

Use injected fake services/`StartupProbe`:

```rust
#[tokio::test]
async fn integrity_precedes_workers_and_routes() {
    let events = erabi::test_support::run_startup_probe().await;
    assert!(events.position("integrity.checked") < events.position("runtime.started"));
}
```

Also test second live lock acquisition fails, stale lock reclaim requires owner-liveness check, migration/integrity failure → RecoveryMode, and Crawl4AI outage → DegradedCrawlerUnavailable rather than RecoveryMode.

- [ ] **Step 2: Run RED**

```bash
cargo test -p erabi --test startup_sequence
cargo test -p erabi-api --test recovery_mode
```

- [ ] **Step 3: Implement the fixed startup sequence**

```text
canonicalize data directory
→ acquire exclusive process lock
→ validate bootstrap config
→ open DB
→ migration lock + migrations
→ lightweight integrity
→ verify artifact directories
→ stale-job recovery hook (Plan 04 supplies implementation)
→ concurrency rebuild hook
→ Crawl4AI health check
→ routes/workers
→ readiness
```

Lock metadata includes PID, start time, Erabi version, bind address. Recovery Mode exposes only diagnostics/migration retry/backup verify-restore/safe reads where possible and rejects normal mutations/jobs/retention cleanup.

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
- Modify: `crates/erabi-cli/src/runtime.rs`
- Create: `crates/erabi-api/src/openapi.rs`
- Modify: `crates/erabi-api/src/app.rs`
- Test: `crates/erabi-cli/tests/graceful_shutdown.rs`
- Test: `crates/erabi-api/tests/openapi_policy.rs`

**Interfaces:**
- Produces `ShutdownCoordinator` with fixed `Duration::from_secs(3)` deadline and checkpoint-flush registration hook for Plan 04.
- Produces OpenAPI route policy based on bind mode + explicit remote opt-in.

- [ ] **Step 1: Write failing shutdown tests with paused Tokio time**

Assert shutdown order: stop mutations/new work → signal cancellation → settle/rollback active DB transactions → invoke checkpoint hook → flush critical logs → release process lock. A long handler cannot extend the 3-second deadline.

- [ ] **Step 2: Write failing OpenAPI exposure tests**

Loopback + default exposes schema/docs. Remote without explicit opt-in returns 404/not mounted. Remote with opt-in still requires bearer auth. Generated examples contain synthetic data only.

- [ ] **Step 3: Run RED**

```bash
cargo test -p erabi --test graceful_shutdown
cargo test -p erabi-api --test openapi_policy
```

- [ ] **Step 4: Implement shutdown/OpenAPI policy**

Use one runtime cancellation mechanism; Plan 04 registers its worker checkpoint flush with this coordinator rather than adding a second shutdown model. Generate OpenAPI from real route contracts with a current stable compatible crate selected via `cargo add` at execution time.

- [ ] **Step 5: Run Plan 03 gate and commit**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p erabi-api
cargo test -p erabi
```

Expected: loopback/remote auth, CORS/origin/body policy, redaction, robots reason, startup/recovery, remote OpenAPI protection, and 3-second shutdown all pass.

```bash
git add Cargo.lock crates/erabi-api crates/erabi-cli
git commit -m "feat(runtime): enforce shutdown and API exposure policy"
```

## Plan 03 Gate

Do not start Plan 04 until Task 5 Step 5 passes from a clean checkout and `git status --short` is empty.
