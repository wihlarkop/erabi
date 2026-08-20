# Erabi API Security and Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the hardened Axum application shell, same-origin static UI serving, local OpenAPI behavior, startup checks, single-instance locking, Recovery Mode, and mandatory three-second graceful shutdown.

**Architecture:** Axum serves both `/api/v1` and the built SvelteKit SPA from one origin. Security middleware rejects unsafe hosts, origins, content types, oversized bodies, and missing bearer tokens for non-loopback exposure; startup gates mutation behind migration and integrity health.

**Tech Stack:** Rust, Axum, Tower, tower-http, Tokio cancellation, process locking, CSP/security headers.

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

- **Depends on:** [02 Turso and Persistence Foundation](./02-turso-and-persistence.md).
- **Produces:** Runnable API shell, stable error envelope, security middleware, startup/recovery state machine, and shutdown coordinator.
- **Gate:** Runtime A: localhost and non-loopback authentication tests, request-hardening tests, startup/recovery tests, and three-second shutdown tests pass.
- **Execution order:** Complete every task in this file in numerical order and commit after each task. Do not begin the next plan until this gate passes.

## Focused File Map

```text
crates/erabi-security/
crates/erabi-api/
crates/erabi-cli/src/serve/
crates/erabi-cli/src/runtime/
apps/web/build/ (served output contract)
tests/integration/api/
tests/integration/runtime/
```

---

### Task 13: Implement Access Token, Origin, Host, Content-Type, and Request Limits

**Files:**
- Create: `crates/erabi-security/src/auth.rs`
- Create: `crates/erabi-security/src/origin.rs`
- Create: `crates/erabi-security/src/headers.rs`
- Create: `crates/erabi-security/src/limits.rs`
- Create: `crates/erabi-security/src/lib.rs`
- Test: `crates/erabi-security/tests/security_contract.rs`

**Interfaces:**
- Produces: Axum/Tower middleware layers for bearer auth, Host/Origin validation, media-type enforcement, and body limits.
- Produces: strict security headers including CSP.

- [ ] **Step 1: Add stable dependencies**

Run:

```bash
cargo add -p erabi-security axum
cargo add -p erabi-security tower
cargo add -p erabi-security tower-http --features cors,limit,set-header,trace
cargo add -p erabi-security http
cargo add -p erabi-security subtle
cargo add -p erabi-security secrecy
cargo add -p erabi-security dashmap
cargo add -p erabi-security thiserror
cargo add -p erabi-security tracing
```

- [ ] **Step 2: Write failing middleware tests**

Create integration tests with an Axum test router proving:

```rust
#[tokio::test]
async fn local_mode_allows_request_without_token() { /* expect 200 */ }
#[tokio::test]
async fn network_mode_rejects_missing_or_wrong_bearer_token() { /* expect 401 */ }
#[tokio::test]
async fn mutation_rejects_untrusted_origin() { /* expect 403 FORBIDDEN_ORIGIN */ }
#[tokio::test]
async fn json_endpoint_rejects_text_plain() { /* expect 415 */ }
#[tokio::test]
async fn responses_include_csp_and_nosniff() { /* assert headers */ }
```

- [ ] **Step 3: Implement constant-time bearer comparison**

Parse only `Authorization: Bearer <token>`. Compare the expected and received byte slices using `subtle::ConstantTimeEq`. Never include either token in logs or error details. Add a small per-IP failed-auth limiter that returns 429 after the configured threshold and naturally expires entries.

- [ ] **Step 4: Implement same-origin and CORS policy**

Default CORS layer is absent. When an allowlist is configured, parse exact origins and allow only required methods/headers. Reject wildcard origins when an access token is configured. Validate `Host` against the bound host plus explicit trusted hosts.

- [ ] **Step 5: Implement content-type and body-size policy**

- JSON mutations accept only `application/json` and structured `+json` types.
- Backup upload accepts only `multipart/form-data`.
- Default JSON body limit: 1 MiB.
- URL batch body limit: 10 MiB.
- Backup upload limit: configurable, default 10 GiB.
- Asset upload is not exposed in the MVP.

- [ ] **Step 6: Implement security headers**

Set at least:

```text
Content-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; connect-src 'self'; frame-src 'self'; object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'
X-Content-Type-Options: nosniff
Referrer-Policy: no-referrer
Permissions-Policy: camera=(), microphone=(), geolocation=()
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Resource-Policy: same-origin
```

The sandbox preview route receives a separate, even stricter policy and never weakens the main UI policy.

- [ ] **Step 7: Run tests**

Run: `cargo test -p erabi-security`

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add Cargo.lock crates/erabi-security
git commit -m "feat(security): harden network and HTTP requests"
```
### Task 14: Build the Axum API Shell, Error Envelope, Static UI, and Local OpenAPI

**Files:**
- Create: `crates/erabi-api/src/state.rs`
- Create: `crates/erabi-api/src/error.rs`
- Create: `crates/erabi-api/src/router.rs`
- Create: `crates/erabi-api/src/routes/system.rs`
- Create: `crates/erabi-api/src/routes/audit.rs`
- Create: `crates/erabi-api/src/openapi.rs`
- Create: `crates/erabi-api/src/static_ui.rs`
- Modify: `crates/erabi-api/src/lib.rs`
- Test: `crates/erabi-api/tests/api_shell.rs`

**Interfaces:**
- Produces: `build_router(AppState, ApiConfig) -> Router`.
- Produces: `/api/v1/system/health`, paginated `/api/v1/audit-events`, `/api/v1/openapi.json`, Swagger UI, and SPA fallback.
- Produces: consistent `ApiErrorResponse` with trace ID.

- [ ] **Step 1: Add stable API dependencies**

Run:

```bash
cargo add -p erabi-api axum --features json,macros
cargo add -p erabi-api tokio --features sync
cargo add -p erabi-api tower
cargo add -p erabi-api tower-http --features fs,request-id,trace
cargo add -p erabi-api serde --features derive
cargo add -p erabi-api serde_json
cargo add -p erabi-api utoipa --features axum_extras,uuid,time
cargo add -p erabi-api utoipa-swagger-ui --features axum
cargo add -p erabi-api tracing
cargo add -p erabi-api uuid --features v7,serde
cargo add -p erabi-api http-body-util
cargo add -p erabi-api --path crates/erabi-domain erabi-domain
cargo add -p erabi-api --path crates/erabi-security erabi-security
```

- [ ] **Step 2: Write failing shell tests**

Test:

```rust
#[tokio::test]
async fn health_uses_the_versioned_api_path() { /* GET /api/v1/system/health -> 200 */ }
#[tokio::test]
async fn unknown_api_route_returns_json_error_with_trace_id() { /* 404 envelope */ }
#[tokio::test]
async fn local_openapi_is_available_when_enabled() { /* 200 */ }
#[tokio::test]
async fn spa_route_falls_back_to_index_html() { /* GET /start -> HTML */ }
```

- [ ] **Step 3: Implement the stable error envelope**

Use:

```rust
#[derive(serde::Serialize)]
pub struct ApiErrorBody {
    pub error: ApiError,
}

#[derive(serde::Serialize)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    pub details: serde_json::Value,
    pub recoverable: bool,
    pub suggested_actions: Vec<SuggestedAction>,
    pub trace_id: String,
}
```

Map domain error codes to deterministic HTTP status codes. Never serialize internal backtraces or raw SQL errors.

- [ ] **Step 4: Implement router construction**

Nest all product routes under `/api/v1`. Apply request ID, tracing, security, and body limit layers centrally. Serve static files from a configured web build directory and use `index.html` only for non-API GET routes. Add a read-only paginated Audit Events endpoint with filters for event type, entity, actor, date, and trace ID; it never exposes secret values or raw scraped content.

- [ ] **Step 5: Implement OpenAPI exposure rules**

- localhost and `openapi_enabled=true`: expose JSON and Swagger UI;
- non-loopback: disabled unless explicitly enabled;
- when enabled on network: auth middleware still applies;
- never document internal recovery mutation endpoints as public examples with secrets.

- [ ] **Step 6: Run API shell tests**

Run: `cargo test -p erabi-api --test api_shell`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.lock crates/erabi-api
git commit -m "feat(api): add hardened Axum application shell"
```
### Task 15: Implement Startup, Process Lock, Recovery Mode, and Three-Second Shutdown

**Files:**
- Create: `crates/erabi-cli/src/lock.rs`
- Create: `crates/erabi-cli/src/startup.rs`
- Create: `crates/erabi-cli/src/shutdown.rs`
- Create: `crates/erabi-cli/src/runtime.rs`
- Modify: `crates/erabi-cli/src/main.rs`
- Test: `crates/erabi-cli/tests/process_lock.rs`
- Test: `crates/erabi-cli/tests/recovery_startup.rs`

**Interfaces:**
- Produces: `RuntimeMode::{Normal,Recovery}` and `StartupReport`.
- Enforces: one process per canonical local data directory.
- Enforces: three-second graceful shutdown deadline.

- [ ] **Step 1: Add dependencies**

Run:

```bash
cargo add -p erabi fs2
cargo add -p erabi tokio --features full
cargo add -p erabi tokio-util --features rt
cargo add -p erabi serde --features derive
cargo add -p erabi serde_json
cargo add -p erabi tracing
cargo add -p erabi thiserror
cargo add -p erabi --path crates/erabi-db erabi-db
cargo add -p erabi --path crates/erabi-api erabi-api
cargo add -p erabi --path crates/erabi-observability erabi-observability
cargo add -p erabi --dev tempfile
```

- [ ] **Step 2: Write process lock tests**

Prove that the first lock succeeds, a second lock on the same canonical directory returns `AlreadyRunning` with PID/start/address metadata, and a lock on another directory succeeds.

- [ ] **Step 3: Implement lock metadata and stale recovery**

Create `.erabi.lock` in the data directory, acquire an exclusive OS lock with `fs2`, then write JSON:

```json
{"pid":18420,"started_at":"2026-07-22T16:00:00Z","version":"0.1.0","address":"http://127.0.0.1:7878"}
```

Never delete an actively locked file. A stale unlocked file may be overwritten after its metadata is recorded in a startup event.

- [ ] **Step 4: Implement the startup sequence**

Execute in order:

1. validate configuration;
2. create/canonicalize data directories;
3. acquire process lock;
4. initialize tracing;
5. open Turso;
6. acquire migration lock and run migrations;
7. run lightweight integrity checks;
8. verify artifact directory;
9. recover stale jobs;
10. health-check Crawl4AI without failing the UI;
11. build Axum and start workers;
12. emit `system.ready`.

A migration or integrity failure sets `RuntimeMode::Recovery`, starts read-only API/diagnostics, and does not start job workers.

- [ ] **Step 5: Implement the fixed shutdown deadline**

Use a root `CancellationToken`. On Ctrl+C/SIGTERM:

```rust
let deadline = std::time::Duration::from_secs(3);
root_cancel.cancel();
let _ = tokio::time::timeout(deadline, runtime.shutdown()).await;
```

`runtime.shutdown()` stops accepting mutations/jobs, checkpoints active work, flushes audit/error summaries, closes listeners, and releases the process lock. Any still-running job becomes `RECOVERABLE` at next startup.

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p erabi --test process_lock
cargo test -p erabi --test recovery_startup
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.lock crates/erabi-cli
git commit -m "feat(runtime): add startup recovery and graceful shutdown"
```
