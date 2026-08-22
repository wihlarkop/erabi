# Discovery Graph, Test Lab, and Runs Specification

## 1. Official run types

MVP has four first-class run types:

```text
QUICK_SCRAPE
TEST_RUN
DISCOVERY_PREVIEW
PRODUCTION_RUN
```

All four share the same durable job, logging, progress, artifact, cancellation, checkpoint, and provenance infrastructure. Their permissions and result semantics differ.

### QUICK_SCRAPE

- ad-hoc configuration, no Crawler Version required;
- optimized for immediate one-URL exploration;
- a bounded pasted URL batch is a submission envelope that creates one independent `QUICK_SCRAPE` run per accepted URL, not a fifth run type;
- may become a saved Crawler draft later;
- produces reviewable data and provenance.

A Quick Scrape batch preserves input order and per-item validation/outcome. Each accepted URL has its own immutable run snapshot, lifecycle, artifacts, cancellation/retry state, and review result. Failure of one item does not silently roll back unrelated accepted items.

### TEST_RUN

- operates against a Draft Crawler Version;
- bounded and clearly marked non-production;
- stores artifacts and test evidence;
- never silently writes trusted/approved production data.

### DISCOVERY_PREVIEW

- bounded sampling focused on URL discovery and graph health;
- may perform enough rendering to discover links;
- emphasizes classification, transitions, scope, canonicalization, duplicates, and growth estimates;
- does not represent a complete production snapshot.

### PRODUCTION_RUN

- must reference a published Crawler Version;
- may use a Run Profile and temporary operational overrides;
- can produce production review candidates;
- only a healthy complete snapshot may trigger missing-record detection.

## 2. Crawl Run lifecycle

Primary lifecycle states:

```text
QUEUED
RUNNING
SUCCEEDED
PARTIAL_RESULT
FAILED
CANCELLED
```

Result classifications such as `NO_CHANGES` are outcomes/summary fields, not replacements for the primary lifecycle status.

A run stores at creation time:

- run type;
- crawler/crawler-version reference when applicable;
- selected seeds;
- Run Profile reference when applicable;
- all resolved operational values and their effective sources;
- full crawler semantic configuration hash/snapshot reference;
- robots policy decision;
- robots override reason when an override is active;
- active User-Agent;
- Crawl4AI connection reference;
- retention/screenshot settings;
- actor/time metadata.

Queued runs do not adopt later setting changes.

## 3. Durable job orchestration

The backend runs as a modular monolith with a durable Turso-backed job queue and Tokio workers.

Initial job kinds include:

- crawl page;
- discover/classify URLs;
- extract dataset records;
- validate records;
- download selected assets;
- export dataset;
- retention cleanup;
- backup/integrity operations where appropriate.

Jobs store priority, attempts, schedule time, heartbeat/lease metadata, parent linkage, and checkpoint linkage.

### 3.1 Leasing and stale recovery

Workers acquire a lease before running a durable job and renew a heartbeat while active.

On startup, stale `RUNNING` jobs are inspected:

- valid checkpoint/config snapshot → recoverable;
- unsafe/inconsistent state → failed or Recovery Mode depending on invariant severity.

MVP uses one active Erabi process per local data directory, but the job model must not depend on purely in-memory identity.

## 4. Discovery pipeline

For a discovered link, the canonical order is:

```text
raw discovered href
→ resolve against base URL
→ validate URL
→ canonicalize
→ domain-scope classification
→ deduplicate
→ Page Type matching
→ transition validation
→ budget checks
→ enqueue or preserve-only status
```

Possible URL states include:

- in-scope matched;
- `AMBIGUOUS_PAGE_TYPE`;
- `UNMATCHED`;
- `EXTERNAL`;
- explicitly blocked;
- duplicate/canonical duplicate;
- budget excluded;
- already completed.

Every decision stores enough metadata to explain why a URL was or was not scheduled.

## 5. Discovery graph provenance

Each discovered URL retains its path origin:

```text
Crawler Version
Run
Source URL
Source Page Type
Discovery Transition
Link selector/rule
Raw href
Resolved original URL
Canonical URL
Discovery timestamp
```

This provenance enables graph inspection and root-cause analysis when a crawler unexpectedly expands.

## 6. Discovery Preview

Discovery Preview is an explicit Studio operation on a Draft Crawler Version.

The preview applies tight configurable sample limits such as:

- selected seed(s);
- low page cap;
- low depth cap;
- transition budgets;
- optional time cap.

The result MUST show:

- pages sampled;
- URLs discovered;
- canonical unique URLs;
- duplicates prevented;
- Page Type distribution;
- ambiguous URLs;
- unmatched URLs;
- external/blocked URLs;
- transition counts;
- robots-policy exclusions;
- budget hits;
- estimated growth indicators.

### 6.1 Growth warnings

The preview SHOULD detect suspicious patterns such as:

- one cyclic transition producing most discovered URLs;
- query-parameter explosion;
- calendar/session/search-space traps;
- many unmatched URLs;
- widespread ambiguity;
- expected crawl exceeding max page/storage budgets.

Growth estimation is advisory. Erabi must not present a sampled estimate as guaranteed total site size.

## 7. Test Lab

Test Lab supports small, focused tests against a Draft Crawler Version.

MVP capabilities:

- Test URL canonicalization;
- Test Page Type matching, including deterministic specificity rationale and ambiguity ties;
- Test extraction;
- Test selector coverage;
- Test pagination detection/config;
- Test one discovery transition;
- Preview discovered URLs;
- Compare relevant result/config behavior against active published version;
- Save test evidence.

The Test Lab reuses stored artifacts when safe/appropriate so a user can iterate on extraction without repeatedly hitting a website.

## 8. Pagination

MVP pagination support focuses on detection and explicit confirmation/configuration:

- `rel=next`;
- Next/Older/More-style links;
- numbered pagination;
- URL page-number patterns.

Pagination is represented as discovery behavior, often a self-transition such as `Listing → Listing`.

Erabi MUST NOT blindly traverse an unbounded pagination pattern. The user confirms scope/budget through the crawler configuration or run limits.

Roadmap pagination types include load-more actions, infinite-scroll orchestration, API/cursor pagination, and reusable browser action workflows.

## 9. Dynamic pages

MVP delegates rendering to Crawl4AI and supports configuration for:

- normal rendered crawl;
- wait for CSS selector;
- timeout;
- network idle when appropriate;
- bounded auto-scroll for lazy content;
- rendered DOM artifact;
- screenshot policy.

Arbitrary click/fill/login workflows are post-MVP.

## 10. Robots, User-Agent, and rate limiting

Safe crawling is the default.

### robots.txt

Erabi respects robots policy by default. An advanced explicit override may be supported when legally/operationally appropriate, but it requires an explicit non-empty user-provided reason before a run can be created or resumed with the override.

The immutable run snapshot and audit trail store at minimum:

- override decision;
- reason;
- actor;
- timestamp;
- affected origin/scope;
- active User-Agent;
- Crawler/Crawler Version when applicable.

The UI must make the override state prominent. A prior override reason MUST NOT be silently reused for a later independent run.

### User-Agent

Erabi uses a transparent default User-Agent and allows customization. User-Agent configuration is visible in run snapshots and audit history. The UI warns against misleading impersonation of unrelated crawlers.

### Rate limits

Per-domain rate limiting is mandatory. `429 Too Many Requests` handling respects `Retry-After` when present and uses non-aggressive backoff.

Rate/concurrency values may be inherited from global, Collection, Crawler, Run Profile, and per-run layers according to allowed operational override rules.

## 11. Progress events and SSE

Every run exposes user-friendly live progress. SSE is the default transport.

Event categories include:

- run queued/started/completed/failed;
- page loading/rendering/completed;
- URL discovered/classified;
- pagination detected;
- extraction started/completed;
- validation warning/error;
- artifact saved;
- checkpoint persisted;
- retry scheduled;
- storage/security warnings.

Events carry a monotonic sequence and durable event ID. Reconnecting clients can send `Last-Event-ID`; Erabi replays missed durable events before switching back to the live stream.

User progress and technical logs are separate presentation layers even when generated from related tracing spans.

## 12. Cancel, resume, and retry

Cancellation is cooperative.

On cancel:

1. stop scheduling new work;
2. signal active Tokio tasks;
3. finish/abort only at safe unit boundaries;
4. persist checkpoint and current durable state;
5. mark the run `CANCELLED`.

Checkpoint data includes completed/pending URLs, pagination state, failed units, artifact references, config hash, crawler version, and relevant extraction state.

Resume is allowed only when the semantic configuration required by the checkpoint still matches. Changing Page Types, transitions, canonicalization, unique keys, or extraction contracts requires a new run.

A resumed run that uses a robots override still requires the override reason already captured in that run's immutable snapshot. A new independent run cannot inherit the old run's reason silently.

MVP supports:

- Retry Failed Parts;
- Rerun Full Crawl;
- Resume from valid checkpoint;
- Restart from beginning.

Retry attempts are linked to the parent run/job rather than erasing prior failure history.

## 13. Complete snapshot semantics

A production run can be considered a complete snapshot only when all required conditions hold, including:

- planned/discovered in-scope pages were completed within the configured bounded crawl contract;
- pagination was not truncated unexpectedly;
- critical extraction did not fail;
- required schema/unique-key health is acceptable;
- no production-breaking `SCHEMA_DRIFT` remains unresolved;
- no unresolved partial task invalidates completeness;
- run was not cancelled;
- Page Type ambiguity affecting expected records is not unresolved.

Only a complete snapshot may create `MISSING_CANDIDATE` records.

`PARTIAL_RESULT`, `FAILED`, `CANCELLED`, `TEST_RUN`, and `DISCOVERY_PREVIEW` MUST NOT trigger missing/deletion candidates.

When `SCHEMA_DRIFT` breaks required extraction or identity health in a `PRODUCTION_RUN`, Erabi may retain artifacts and diagnostic/partial results, but the run MUST NOT be treated as a trusted complete snapshot and MUST NOT use a generic `USE_ANYWAY` path to create trusted change/missing semantics. Correction requires a new Crawler Draft, applicable validation/Test Lab evidence, and a later published Crawler Version for normal production.

`TEST_RUN`, `DISCOVERY_PREVIEW`, and ad-hoc Quick Scrape may inspect drift output for diagnosis. Diagnosis does not mutate or repair a published Crawler Version and does not auto-approve data.

## 14. No-change runs

A successful recrawl with no meaningful normalized-data changes stores the Crawl Run and evidence but does not create an empty review.

Example summary:

```text
Status: SUCCEEDED
Outcome: NO_CHANGES
New records: 0
Updated records: 0
Missing records: 0
Review created: no
```

Raw artifact changes may still be retained according to retention policy.

## 15. Run comparison

Focused Run Comparison is MVP.

Two runs of the same crawler may be compared across:

- crawler version/config differences;
- Run Profile/per-run override differences;
- page counts;
- discovered/canonical URL counts;
- Page Type distributions;
- transition distributions;
- error/partial counts;
- extraction record counts;
- new/updated/missing candidates;
- duration/storage summaries.

This is operational comparison, not a generic analytics system.

## 16. Queue and concurrency UI

A global Runs/Queue view shows running, queued, completed, failed, cancelled, recoverable, and storage-blocked work.

MVP safe queue actions may include:

- prioritize;
- move down;
- cancel;
- resume;
- retry;
- remove a queued non-started job where safe.

Concurrency defaults should be conservative for local machines. The implementation plan will choose exact initial defaults after validating Crawl4AI behavior and local resource use.
