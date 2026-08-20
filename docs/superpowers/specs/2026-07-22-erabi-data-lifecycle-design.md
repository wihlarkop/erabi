# Erabi Data Model, Versioning, and Provenance Design

**Status:** Approved specification
**Date:** 2026-07-22

## 1. Data Integrity Principles

1. Raw crawler output and curated data are separate.
2. A Crawl Run never overwrites an approved version.
3. Approved Schema, Dataset, and Record versions are immutable.
4. Every state transition is explicit and auditable.
5. A partial, failed, or cancelled crawl never implies that an existing Record disappeared.
6. Meaningful change is determined from normalized field values, not raw HTML differences alone.
7. Every approved field retains provenance.
8. Deletion is conservative: Archive and Trash precede permanent deletion.

## 2. Entity Model

```text
Collection
├── Sources
├── Extraction Schemas
├── Saved Destinations
└── Setting overrides

Source
├── Crawl Runs
├── Raw Artifacts
├── Datasets
├── Assets
└── source-level state

Extraction Schema
└── immutable Schema Versions

Dataset
├── Review lifecycle
├── Dataset Versions
└── Records
    └── immutable Record Versions

Export Run
Backup Run
Audit Event
Job / Checkpoint
```

Every primary entity uses UUIDv7.

## 3. Collection and Inbox

A Source has a nullable `collection_id`.

```text
collection_id = null → Inbox
collection_id set    → Collection
```

For MVP, moving a Source between Collections is not supported. This avoids ambiguous historical ownership and configuration inheritance. The data model must not make future movement impossible.

A Collection may span multiple domains and may be used only as a simple organizational folder. It may additionally own shared schemas, destinations, and setting overrides.

## 4. Source

A Source represents a durable crawl target.

Suggested fields:

```text
id: UUIDv7
collection_id: UUIDv7 | null
name
original_url
canonical_url
source_type
current_status
created_at
updated_at
archived_at
trashed_at
```

### Source states

```text
ACTIVE
CRAWL_FAILED
ACCESS_DENIED
NOT_FOUND
SCHEMA_DRIFT
CONTENT_CHANGED
PARTIAL_RESULT
ARCHIVED
TRASHED
```

Source state describes crawlability or organization. It never directly changes Record lifecycle.

### Duplicate URL handling

Before creating a Source, Erabi checks normalized/canonical URL matches and offers:

- Open Existing;
- Recrawl Existing;
- Create New Anyway;
- Cancel.

When the user creates another Source anyway, Erabi records a possible-duplicate relationship rather than merging them.

## 5. Crawl Run

Every Scrape, Crawl, Recrawl, Retry, and Resume produces or is linked to a durable Crawl Run.

### Execution status

```text
QUEUED
RUNNING
SUCCEEDED
PARTIAL_RESULT
FAILED
CANCELLED
```

`NO_CHANGES` is an outcome of a successful Crawl Run, not an execution status.

### Immutable configuration snapshot

Configuration is resolved when the Crawl Run is created, including while still queued. It never reads updated settings at execution time.

The snapshot includes:

- URL and canonical URL;
- Collection and Source identity at creation;
- Crawl4AI connection;
- User-Agent;
- robots.txt decision and override reason;
- rate limits and concurrency;
- render, wait, scroll, screenshot, and timeout settings;
- pagination plan;
- Schema Version and unique-key configuration;
- retention configuration;
- setting provenance: built-in, global, Collection, or per-run.

Retry and Resume use the original snapshot. A changed critical configuration requires a new Crawl Run.

## 6. Raw Artifacts

Raw artifacts are immutable and addressed by metadata plus cryptographic hash.

Possible artifacts:

- raw HTML;
- cleaned HTML;
- rendered DOM;
- extracted Markdown;
- Crawl4AI structured output;
- screenshot;
- failed response metadata;
- detailed technical log;
- asset discovery manifest.

The database stores metadata and paths, not large blobs.

Extraction may be rerun against a stored artifact without recrawling when the artifact contains the required DOM or content.

Retention may remove artifact files, but permanent metadata needed for approved provenance remains.

## 7. Extraction Schema and Versioning

An Extraction Schema is a reusable identity with one or more immutable versions.

A Schema Version contains:

- domain and URL pattern matching rules;
- mode: Document or Records;
- container selector for Records mode;
- field definitions;
- field types;
- relative selectors and value sources;
- required/optional status;
- normalization and validation rules;
- unique key, including composite field order;
- ignored fields for semantic change detection;
- pagination configuration;
- include/exclude selectors;
- structural fingerprints and sample signatures;
- creation reason and audit metadata.

A matching schema is offered, not silently applied. Erabi runs a preview before confirmation.

### Schema approval

```text
temporary editor state
→ Schema Draft
→ test against stored artifacts
→ approve
→ immutable Schema Version
```

Editing an approved version creates a new version.

### Schema drift

Drift signals include:

- required selector missing;
- container missing;
- field coverage drop;
- unexpected type;
- abnormal record count;
- unique-key extraction failure;
- significant DOM fingerprint change.

Drift produces `SCHEMA_DRIFT`, preserves the old version, and offers Review Selectors, Use Anyway, or Cancel. Erabi does not repair automatically.

## 8. Dataset and Review Lifecycle

A Crawl Run may create a Draft Dataset when extracted content requires review.

### Dataset content status

```text
DRAFT
PARTIALLY_APPROVED
APPROVED
SUPERSEDED
```

### Review workflow status

```text
OPEN
CLOSED
CLOSED_WITH_UNRESOLVED_ITEMS
REOPENED
```

Content status and review workflow status are independent.

A Review can be closed while Drafts or validation errors remain. Erabi shows counts, requires confirmation, records an optional note, and uses `CLOSED_WITH_UNRESOLVED_ITEMS`.

A successful recrawl with no meaningful changes stores its Crawl Run and artifacts but creates no empty Dataset or Review.

## 9. Record Lifecycle

### Record states

```text
DRAFT
APPROVED
REJECTED
NEW_CANDIDATE
UPDATED_CANDIDATE
MISSING_CANDIDATE
DELETED
RESTORED_CANDIDATE
SUPERSEDED
```

A stable Record identity is determined by a configured unique key or, when no unique key exists, a content-derived fallback hash.

### Unique key

A Schema Version may configure:

- one field;
- a composite ordered set of fields;
- trimming;
- case sensitivity;
- URL canonicalization;
- empty-key behavior.

Unique-key configuration is versioned. Preview exposes duplicates and missing keys.

### New and changed Records

```text
new unique key                         → NEW_CANDIDATE
existing key + changed normalized data → UPDATED_CANDIDATE
existing key + same normalized data    → preserve current approved version
```

New and changed candidates require review and are never auto-approved.

### Missing Records

A previously approved key missing from a new complete snapshot becomes `MISSING_CANDIDATE`.

Missing detection is prohibited unless all of the following are true:

- Crawl Run status is `SUCCEEDED`;
- all planned pages completed;
- pagination is complete;
- no page, extraction, or browser task failed;
- Schema is healthy;
- unique-key extraction is healthy;
- the run is marked a complete snapshot.

Actions:

- Mark Deleted;
- Keep Active;
- Ignore This Run;
- Recrawl Again;
- Open Source.

A confirmed deletion creates a deletion event and a new immutable state; it does not erase history. If the key returns later, Erabi creates `RESTORED_CANDIDATE` for confirmation.

## 10. Immutable Approval

Approval is atomic.

A Record approval transaction must:

1. verify the Draft version and optimistic concurrency value;
2. verify no validation error remains;
3. mark the previous approved version `SUPERSEDED`, if present;
4. mark the new version `APPROVED`;
5. update the current-version pointer;
6. persist approval metadata;
7. append an audit event;
8. commit all changes together.

Approved values cannot be edited. The user chooses `Create New Version` to modify approved data through manual edit, recrawl, or cloning a previous version.

## 11. Validation and Bulk Approval

Validation levels:

```text
ERROR   blocks approval and cannot be overridden
WARNING visible but does not block or require extra confirmation
```

Typical errors:

- required field missing;
- unique key empty or duplicate;
- invalid field type;
- mandatory rule violation.

Typical warnings:

- unusually short content;
- missing optional image;
- low selector coverage;
- unusual value distribution.

`Approve All Valid` approves valid records, skips invalid records, and leaves the Dataset `PARTIALLY_APPROVED` when unresolved records remain.

## 12. Rejection

Single-record rejection reason is optional. Bulk rejection reason is required.

Preset reasons:

- Irrelevant;
- Duplicate;
- Incomplete;
- Incorrect extraction;
- Out of scope;
- Other.

Rejected data is retained with raw artifact, provenance, and audit history.

## 13. Semantic Change Detection

A raw artifact hash change does not automatically create a review.

Change detection compares normalized field values according to the active Schema Version.

Ignored by default or through normalization:

- whitespace and formatting differences;
- HTML attribute order;
- tracking query parameters such as `utm_*`;
- script, style, or decorative content not represented as fields;
- technical timestamps not extracted as meaningful fields;
- fields marked `Ignore in change detection`.

Outcomes:

```text
important normalized field changed → change candidate and Review
raw artifact only changed          → artifact-change audit summary
no meaningful difference           → SUCCEEDED + NO_CHANGES
```

## 14. Field-Level Provenance

Every extracted field stores provenance sufficient to explain the value:

- Record and Record Version;
- field name;
- source URL;
- Crawl Run;
- raw artifact reference and hash;
- source element signature;
- selector;
- raw value;
- normalized value;
- transformation list;
- Schema Version;
- extraction timestamp.

The Review UI exposes a provenance drawer with actions to:

- highlight the source element;
- open the original URL;
- inspect raw HTML or rendered DOM;
- open the Crawl Run and logs;
- copy the selector;
- compare against a previous version.

When retention removes detailed artifacts, Erabi permanently preserves the metadata, hashes, URL, selector, Schema Version, Crawl Run summary, and transformation record required to maintain auditability.

## 15. Audit Trail

Audit events are append-only. Important event types include:

```text
SOURCE_CREATED
SOURCE_RENAMED
SOURCE_ARCHIVED
SOURCE_TRASHED
CRAWL_CREATED
CRAWL_STARTED
ROBOTS_OVERRIDDEN
CRAWL_CANCELLED
CRAWL_RESUMED
SCHEMA_APPLIED
SCHEMA_DRIFT_DETECTED
RECORD_EDITED
RECORD_APPROVED
RECORD_REJECTED
RECORD_MARKED_DELETED
RECORD_RESTORED
REVIEW_CLOSED
REVIEW_REOPENED
EXPORT_COMPLETED
BACKUP_CREATED
SETTINGS_CHANGED
RETENTION_CLEANUP
```

The MVP actor is `local-user`, while the schema reserves an actor identifier and type for future multi-user operation.

Detailed technical logs may expire; audit events and stable error summaries do not.

## 16. Settings Inheritance

Non-secret settings are stored in the application database.

Resolution order:

```text
per-run override
→ Collection override
→ global setting
→ built-in default
```

A Collection setting supports:

- Inherit global;
- Use custom value;
- Reset to built-in default.

The UI shows both the active value and its source. Setting changes affect only Crawl Runs created after the change. Queued, running, retry, and resumed runs retain their immutable snapshots.

Secrets and bootstrap connection values remain in environment variables.

## 17. Archive, Trash, and Permanent Deletion

### Archive

Archive hides a Source from active work while retaining all data. It can be reactivated.

### Trash

Move to Trash:

- hides the Source from Inbox and Collection views;
- disables related future jobs;
- retains records, artifacts, assets, provenance, and history;
- allows Restore.

Default Trash retention is 30 days. Automatic Trash cleanup is off by default.

### Permanent deletion

Permanent deletion is explicit and requires confirmation with the Source name. Erabi displays:

- affected Datasets, Records, artifacts, assets, and exports;
- estimated reclaimed storage;
- active provenance or dataset references;
- data that remains as audit tombstones.

Audit evidence that a deletion occurred remains even when content is removed.

## 18. Export History Lifecycle

Deleting an export file removes only the physical file. Erabi retains:

- Export Run;
- compact manifest;
- checksum;
- format;
- record count;
- timestamps;
- configuration summary;
- audit event.

The Export Run state becomes `FILE_REMOVED`. Regeneration is deferred.
