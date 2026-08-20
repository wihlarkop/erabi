# Erabi Crawling, Jobs, and Progress Design

**Status:** Approved specification
**Date:** 2026-07-22

## 1. Crawl4AI Boundary

Crawl4AI is a finished external crawler engine. Erabi does not modify, fork, embed patches into, or reimplement it.

Crawl4AI owns:

- browser lifecycle;
- Chromium rendering;
- JavaScript execution;
- page loading;
- browser-level waiting and scrolling;
- screenshot generation;
- crawler-native raw output.

Erabi owns:

- source identity and crawl intent;
- safe-crawling policy;
- job queue and configuration snapshots;
- calls to Crawl4AI;
- storage of outputs;
- pagination confirmation and planning;
- extraction, review, versioning, audit, and export;
- retry, checkpoint, resume, and progress presentation.

## 2. Crawler Connections

The MVP supports:

1. bundled official Crawl4AI container, default;
2. external Crawl4AI base URL with optional token.

A saved crawler connection stores only non-secret configuration and an environment-variable reference for its token.

Connection state:

```text
HEALTHY
DEGRADED
UNAVAILABLE
MISCONFIGURED
```

Crawl4AI unavailability does not prevent Erabi from opening or working with existing data.

## 3. Default Crawl Flow

### Single-page first

The Start page creates one page Crawl Run by default.

After completion, Erabi may offer:

- Review this page;
- Select Links;
- Crawl this section/site;
- Continue detected pagination.

Erabi does not silently expand a single-page request into a site crawl.

### Batch URL input

The MVP supports simple pasted URL batches. Each URL becomes a separate Source or Source decision and a separate Draft result. Rich CSV, JSON, JSONL, sitemap, RSS, or API ingestion is deferred.

## 4. Public Pages and Dynamic Rendering

The MVP crawls public pages only.

Supported rendering controls:

- normal browser render;
- wait for CSS selector;
- timeout;
- network-idle where appropriate;
- simple auto-scroll with maximum scroll count and delay;
- rendered DOM capture;
- screenshot override.

Authenticated browser sessions, cookie import, saved browser profiles, automated login workflows, CAPTCHA bypass, and access-control circumvention are not supported.

## 5. File URLs

HTML is the MVP’s structured extraction target.

When a URL resolves to PDF, CSV, JSON, ZIP, image, office document, or another file type, Erabi identifies it as a file and offers `Download as Asset` rather than pretending it is an HTML Dataset.

Parsing those files into Records is deferred to future source adapters.

## 6. Pagination

Erabi detects likely pagination using:

- `rel=next`;
- Next, Older, More, or arrow controls;
- numbered page controls;
- URL page-number patterns;
- preview of the likely next page.

The user confirms scope before crawling:

- all detected pages;
- first N pages;
- a custom range;
- maximum page limit.

Infinite scroll, Load More workflows, API cursor pagination, and recorded browser actions are deferred.

A pagination plan becomes part of the immutable Crawl Run snapshot.

## 7. Safe Crawling Defaults

### robots.txt

Respecting `robots.txt` is on by default.

An advanced user may explicitly override the decision. Erabi requires a reason or note and writes an audit event. It must communicate a disallow decision clearly instead of returning an unexplained generic error.

### Rate limiting

Per-domain rate limiting is on by default. Configuration exists at global, Collection, and per-run scopes.

The default implementation must:

- restrict concurrent requests per domain;
- support a request delay;
- honor `Retry-After` when receiving HTTP 429;
- slow down rather than retry aggressively;
- expose active rate limits in the crawl configuration summary.

### Large crawl warning

Before a large crawl, Erabi shows an estimate of:

- pages;
- requests;
- storage;
- request delay;
- expected screenshots or assets.

The user reviews limits before confirmation.

### User-Agent

Erabi uses a transparent default User-Agent and allows overrides at global, Collection, and per-run scopes.

The active User-Agent appears in the run summary and audit. Erabi warns when the value appears to impersonate a known crawler or bot. Contact URL or email may be included.

## 8. Queue and Concurrency

Recommended built-in defaults:

```text
active jobs: 1
concurrent pages per job: 2
```

Configurable limits:

- active jobs globally;
- active jobs per Collection;
- concurrent pages per job;
- concurrent requests per domain;
- request delay;
- browser process limit;
- memory and storage warnings.

The Queue UI shows Running, Queued, Completed, Failed, Cancelled, Recoverable, and Blocked jobs.

Safe queue actions:

- prioritize;
- move down;
- cancel;
- resume;
- retry failed units;
- remove a queued job.

## 9. Durable Job Model

Suggested fields:

```text
id: UUIDv7
kind
status
priority
payload
attempts
max_attempts
scheduled_at
started_at
heartbeat_at
finished_at
parent_job_id
crawl_run_id
checkpoint_id
lease_owner
lease_expires_at
```

Initial job kinds:

```text
CRAWL_PAGE
DISCOVER_PAGINATION
EXTRACT_DATASET
VALIDATE_DATASET
DOWNLOAD_ASSET
EXPORT_DATASET
CREATE_BACKUP
VERIFY_BACKUP
RUN_INTEGRITY_CHECK
RETENTION_CLEANUP
```

## 10. Leasing and Recovery

A worker atomically acquires a lease before running a job. While running, it updates heartbeat and checkpoint state.

On startup, stale `RUNNING` jobs are inspected:

```text
valid checkpoint    → RECOVERABLE
invalid/no checkpoint → FAILED with stable reason
```

A job is never executed by two local workers at the same time.

## 11. Crawl Run Completeness

A Crawl Run is a complete snapshot only when:

- every planned page completed;
- pagination completed;
- no task failed or was cancelled;
- extraction completed;
- Schema remained healthy;
- unique keys were valid;
- browser and storage operations completed;
- the run did not stop because of a page or storage limit unless that limit was the complete confirmed scope.

The run stores:

- planned, completed, and failed page counts;
- expected and extracted record counts;
- complete-snapshot boolean;
- failure or partial reasons.

Only complete snapshots can create `MISSING_CANDIDATE` records.

## 12. Partial Result

`PARTIAL_RESULT` covers:

- incomplete pagination;
- timeout after some pages succeeded;
- some batch URLs or pages failed;
- partial extraction;
- browser crash after saved progress;
- user-confirmed maximum page stop before the discovered sequence finished;
- storage or operational interruption after valid units were stored.

Partial data can be reviewed, edited, and exported as debug data when explicitly requested, but it is never considered a complete snapshot and never triggers missing/deletion logic.

## 13. Retry and Rerun

The MVP supports:

### Retry Failed Parts

Retry individual failed units such as:

- pages;
- URL ranges;
- asset downloads;
- extraction tasks;
- destination publication steps when safe.

Attempts are linked to the parent Crawl Run. A combined snapshot becomes complete only when every planned unit eventually succeeds under the same immutable configuration.

### Rerun Full Crawl

Creates a new Crawl Run from the Source with current settings and selected Schema Version. It does not mutate or replace the old run.

## 14. Cancel and Resume

The MVP supports cooperative cancel with resumable checkpoints, not an arbitrary pause command.

On cancel:

1. trigger a Tokio cancellation token;
2. stop scheduling new units;
3. finish or stop the current unit at a safe boundary;
4. persist completed and pending units;
5. persist valid artifacts and extracted data;
6. mark the Crawl Run `CANCELLED`;
7. expose Resume from Checkpoint and Restart Full Crawl.

Checkpoint contents:

- completed URLs;
- pending URLs;
- pagination cursor or range;
- failed units;
- artifact references;
- Schema Version;
- unique-key configuration;
- configuration hash.

Resume is allowed only when the configuration hash remains compatible.

## 15. Live Progress

Every scrape, including a single page, exposes user-facing progress.

User-facing stages:

```text
Preparing crawler
Checking robots.txt
Loading page
Rendering JavaScript
Waiting / scrolling
Detecting pagination
Extracting data
Validating records
Saving artifacts
Saving Draft
Completed
```

Technical logs remain expandable.

## 16. SSE Event Stream

Endpoint:

```text
GET /api/v1/crawl-runs/{id}/events
Accept: text/event-stream
```

Event types include:

```text
crawl.queued
crawl.started
page.loading
page.rendering
page.completed
pagination.detected
extraction.started
record.extracted
validation.warning
artifact.saved
crawl.checkpointed
crawl.completed
crawl.failed
```

Every event includes:

- event ID;
- Crawl Run ID;
- event type;
- monotonically increasing sequence;
- timestamp;
- user-facing message;
- progress counts when available;
- trace ID.

SSE supports `Last-Event-ID`. Reconnecting clients receive missed durable events and then continue live streaming.

Progress events and technical logs are separate records even when derived from the same operation.

## 17. Screenshot Defaults

- Single-page scrape: screenshot on by default.
- Batch or multi-page crawl: screenshot off by default.
- Global, Collection, and per-run overrides are supported.

## 18. Storage Pressure

Erabi monitors free disk space.

### Warning threshold

- show persistent warning;
- estimate impact before new crawl, export, backup, screenshot, or asset download;
- direct the user to Storage Settings and cleanup preview.

### Critical threshold

- stop accepting artifact-heavy jobs;
- checkpoint active jobs at a safe boundary;
- use `BLOCKED_LOW_STORAGE` rather than reporting a crawler failure;
- keep database, Review, Settings, diagnostics, and deletion tools available.

Erabi never deletes artifacts automatically merely because storage is low. Automatic cleanup runs only when the user explicitly configured a schedule.

## 19. Startup and Shutdown Interaction

Startup recovers stale jobs only after database migration and integrity checks pass.

Graceful shutdown has a fixed three-second deadline in the MVP. The job runtime prioritizes:

1. database consistency;
2. checkpoint persistence;
3. audit/error summary flush;
4. resource cleanup;
5. process exit.

Incomplete jobs become Recoverable instead of silently Failed.
