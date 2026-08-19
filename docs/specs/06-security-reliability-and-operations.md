# Security, Reliability, and Operations Specification

## 1. Network defaults

Erabi binds to loopback by default:

```text
127.0.0.1
```

Local loopback operation requires no login in MVP.

Binding to a non-loopback address requires a shared access token supplied from environment / `.env`. Erabi MUST refuse startup when non-loopback binding is configured without the required token.

The bearer token protects API, SSE, asset downloads, exports, backups, raw-artifact access, and diagnostics that expose sensitive operational detail.

Browser token storage:

- `sessionStorage` by default;
- `localStorage` only after explicit “Remember on this device” choice;
- “Forget token” action available;
- token never placed in URLs or query strings.

## 2. Same-origin and CORS

The default deployment serves frontend and API from the same origin. CORS is effectively closed/disabled by default.

External frontend origins require an explicit environment allowlist. Wildcard origin is not allowed while bearer authentication is enabled.

SSE follows the same origin/authentication policy as the rest of the API.

## 3. Request hardening

State-changing API requests enforce:

- Host validation;
- Origin validation where browser-origin semantics apply;
- expected Content-Type;
- endpoint-specific body size limits;
- strict parsing and typed validation;
- rejection of malformed/ambiguous requests;
- stable error codes and trace IDs.

JSON mutation endpoints accept `application/json`; upload endpoints explicitly accept `multipart/form-data`. Generic browser form/text mutation fallbacks are not silently accepted.

Sensitive/destructive actions such as restore backup, permanent delete, security-setting changes, and empty Trash require dedicated confirmation flows.

## 4. Security headers

Production responses use a strict security baseline including:

- Content-Security-Policy;
- `X-Content-Type-Options: nosniff`;
- restrictive Referrer Policy;
- Permissions Policy;
- frame restrictions;
- appropriate cross-origin isolation/resource policies where compatible.

Production CSP avoids `unsafe-eval`, broad wildcards, and arbitrary inline script execution.

The sanitized crawl preview is isolated from the primary application DOM/origin boundary and follows its own restricted sandbox policy.

## 5. Untrusted downloaded files

All files acquired from crawled websites are untrusted.

Requirements:

- never auto-execute or auto-open downloads;
- sanitize filenames and paths;
- reject path traversal and absolute-path escapes;
- handle Windows reserved names/control characters safely;
- do not follow untrusted symbolic links;
- store assets under controlled asset roots;
- generate unique safe names on collision;
- inspect MIME/signature where practical instead of trusting extension only;
- stream large files rather than buffering entire content;
- record size/hash/download status;
- clean partial files after failed/cancelled downloads;
- serve downloads using attachment semantics;
- never automatically extract archives.

Executable-like content requires explicit user action and warning if download is allowed at all.

## 6. Log privacy and structure

Erabi uses structured tracing with clean, stable event names.

Development logs are human-readable. Docker/production logs are structured JSON by default.

Typical fields include:

- timestamp;
- level;
- module/target;
- stable event name;
- trace ID;
- job ID;
- Crawl Run ID;
- Crawler ID/version where relevant;
- Source/URL identity references where safe;
- duration/attempt counters;
- stable error code.

Default log level is INFO.

### 6.1 Redaction

Default logs redact or omit:

- bearer tokens/passwords;
- Authorization/Cookie headers;
- secret connection strings;
- request/response bodies;
- extracted record values;
- URL query parameters by default;
- sensitive headers;
- raw page content.

Diagnostic mode may temporarily increase detail after explicit user enablement and warning, but secrets remain redacted. Diagnostic mode has an automatic timeout and enable/disable events are audited.

## 7. Progress vs technical logs

User-facing progress is not raw log output.

Progress communicates stable steps such as loading, rendering, discovering, extracting, validating, saving, and completing.

Technical logs live in an expandable/filterable viewer with level/module/event/job search. Stack traces and verbose details are collapsed by default.

## 8. Error model

Expected failures use typed `Result` paths and stable error codes, not panics.

Examples include:

- crawler timeout;
- robots exclusion;
- access denied;
- schema drift;
- low storage;
- destination authentication failure;
- export validation failure;
- Page Type ambiguity;
- migration/integrity failure.

API errors include a user-readable message, structured details, recoverability/suggested safe actions where useful, and trace ID.

## 9. Panic isolation

A panic inside one worker/task is isolated at worker boundaries:

- API and unrelated jobs continue;
- related job becomes FAILED or RECOVERABLE depending on checkpoint/invariant state;
- sanitized panic/backtrace metadata is recorded;
- retries are bounded.

Critical invariant failures escalate to Recovery Mode or controlled shutdown, including corruption risk, invalid migration state, broken queue consistency, or violation of approved-version immutability.

## 10. Startup sequence

Normal startup conceptually:

1. resolve/canonicalize data directory;
2. acquire exclusive process lock;
3. validate bootstrap configuration;
4. open database;
5. acquire migration lock and apply pending migrations;
6. run lightweight integrity check;
7. verify artifact directories/permissions;
8. recover stale jobs;
9. rebuild in-memory concurrency state;
10. health-check Crawl4AI;
11. start Axum routes and internal workers;
12. report readiness.

Crawl4AI being unavailable does not trigger Recovery Mode by itself.

## 11. Lightweight integrity check

Runs on every startup and is mandatory.

Checks include:

- database readability;
- migration state consistency;
- expected critical tables/indexes;
- artifact root accessibility;
- process/job ownership state;
- obvious current-version pointer consistency;
- critical configuration validity.

## 12. Deep integrity check

A user-triggered deep check is MVP. Scheduling the deep check is supported in Settings but automatic scheduling defaults OFF.

Deep checks may include:

- database-engine integrity diagnostics;
- foreign-reference/domain consistency;
- immutable approved-version invariants;
- current-version pointer validity;
- Crawl Run/config snapshot references;
- artifact existence/hash verification according to selected depth;
- audit/event-chain consistency;
- backup readability verification.

## 13. Recovery Mode

Migration or critical integrity failures enter **Recovery Mode**, not normal mutable service.

Recovery Mode:

- keeps a limited UI/diagnostics surface available;
- stops new jobs and mutating APIs;
- does not run retention cleanup;
- surfaces the failure and trace/evidence;
- allows safe backup verification/restore flows where possible;
- allows migration retry after corrective action;
- avoids automatic destructive “repair” that could destroy evidence.

## 14. Graceful shutdown

Graceful shutdown is mandatory with a fixed MVP deadline of **3 seconds**.

Priority order:

1. stop accepting new mutations/jobs;
2. mark system shutting down;
3. signal cooperative cancellation;
4. complete/rollback active atomic DB transactions safely;
5. persist checkpoints/job recoverability state;
6. flush critical audit/error logs;
7. release process lock/resources;
8. exit by the deadline.

Erabi does not wait for a long crawl/download to finish naturally. Remaining work becomes recoverable where safe.

## 15. Disk pressure protection

Erabi monitors free storage and exposes warning/critical thresholds.

At critical pressure:

- block new artifact-heavy work;
- move active work to safe checkpoints where possible;
- keep database/UI/review/diagnostics available where safe;
- surface cleanup/storage actions.

Erabi MUST NOT automatically delete user artifacts solely because disk space is low.

## 16. Retention safety

Automatic destructive retention cleanup is OFF by default.

Before manual cleanup, Erabi shows:

- selected retention policy;
- artifact/file counts;
- estimated bytes to free;
- categories to be removed;
- data/evidence that will remain.

Approved curated data, minimum provenance, audit events, and required lifecycle metadata are not discarded by ordinary artifact cleanup.

## 17. Telemetry and privacy

No telemetry is sent by default.

Anonymous analytics/crash reporting, if added, is opt-in only. Erabi remains fully usable without an Erabi-hosted telemetry service.

Scraped URLs/content, tokens, crawler configuration, and raw crash logs are not uploaded without explicit user action/consent.

## 18. Browser notifications

Browser notifications are optional and OFF by default. Permission is requested only after the user explicitly enables them.

MVP notification events may include completion/failure of long-running crawl, export, backup, and integrity-check jobs. Notification content avoids scraped values, full sensitive URLs, and secrets.

An in-app Notification Center is roadmap; the durable event model should remain suitable for adding it later.
