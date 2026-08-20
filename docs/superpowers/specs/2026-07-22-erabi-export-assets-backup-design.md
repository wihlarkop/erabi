# Erabi Exports, Assets, Retention, and Backups Design

**Status:** Approved specification
**Date:** 2026-07-22

## 1. Export Principles

1. Standard exports contain approved data only.
2. Draft and rejected data require explicit debug export options.
3. Provenance remains separate from clean data.
4. Export files are immutable and never overwritten.
5. Database export publication is atomic.
6. Secrets are resolved from environment variables and are never persisted.
7. Export history remains after a physical file is removed.

## 2. MVP Export Formats

File formats:

- JSON;
- JSONL;
- CSV;
- Markdown;
- SQLite database file.

Database destinations:

- Turso/local libSQL-compatible database destination;
- Turso remote destination.

PostgreSQL, MySQL, object storage, vector databases, webhooks, and AI-platform connectors are deferred.

## 3. Export Record Selection

### Standard

Approved Records only, clean fields only.

### With Provenance

Approved Records plus provenance sidecar and manifest.

### Debug Bundle

Explicitly selected Draft or Rejected Records, validation errors, selectors, raw/normalized values, logs, and chosen raw artifacts.

A partial Crawl Run may be exported only through an explicit debug path and is labelled incomplete in its manifest.

## 4. Provenance Bundle

A provenance export is a ZIP bundle:

```text
dataset-export.zip
├── data/
│   └── dataset.csv
├── provenance/
│   └── fields.provenance.jsonl
├── manifest.json
└── checksums.sha256
```

JSONL is used for field provenance to support streaming large datasets.

A provenance line includes:

- Record ID and Record Version ID;
- field;
- source URL;
- selector and source-element signature;
- raw and normalized values;
- transformations;
- Schema Version ID;
- Crawl Run ID;
- artifact hash;
- extraction timestamp.

The manifest includes:

- export and Dataset identities;
- export timestamp;
- Dataset Version;
- Record count and status scope;
- Schema Versions and Crawl Runs;
- included files and checksums;
- Erabi application version;
- manifest format version;
- completeness indicator.

## 5. Asset Inclusion

Default export stores asset URL and metadata only.

A user may enable `Include downloaded assets`, which adds:

```text
assets/
├── images/
├── documents/
└── other/
```

The manifest records downloaded, missing, blocked, and failed assets. Included files receive checksums.

Asset download and Dataset export remain separate actions.

## 6. Export Filenames and Storage

MVP filenames are generated automatically:

```text
{dataset-slug}-{date}-{short-export-id}.{extension}
```

The filename cannot be customized before export in the MVP.

Each Export Run uses a new UUIDv7 and never overwrites an older file.

Files remain in Erabi until explicit deletion or export-specific retention cleanup. Automatic cleanup is off by default.

When a file is deleted:

- the physical file is removed;
- Export Run remains;
- compact manifest and checksum remain;
- state becomes `FILE_REMOVED`;
- regeneration is not offered in the MVP.

## 7. Saved Destinations

A saved destination stores:

- name;
- destination type;
- non-secret database URL or path;
- token environment-variable name;
- table naming mode;
- last tested capabilities and timestamp;
- optional Collection association.

Example:

```text
token_env_var = TURSO_EXPORT_TOKEN_SCANDAL
```

The token value exists only in `.env` or the process environment.

A Collection may own multiple saved destinations.

## 8. Destination Database Layout

Two modes:

### Dedicated database per Collection, default

Provides isolation and portability.

### Shared database

Because SQLite/Turso does not expose PostgreSQL-style schemas, Erabi uses table names:

```text
{collection_slug}__{dataset_name}
```

Metadata tables map Collection, Dataset, physical table, Schema Version, and Export Run.

Records are written to real typed columns. An optional raw JSON debug column may be included, but the dataset is not stored only as opaque JSON.

## 9. Destination Capability Test

`Test Connection` checks:

- endpoint or file accessibility;
- authentication;
- database version;
- create and write permission;
- staging-table creation;
- rename or swap capability;
- drop-staging permission;
- transaction support;
- basic latency.

The result is cached with a timestamp for display. Every Export Run revalidates required capabilities immediately before writing.

## 10. Atomic Database Export

MVP modes:

### Create New, default

Creates a unique/versioned table and does not touch an existing table.

### Replace Atomically

1. create staging table;
2. stream approved Records;
3. validate schema, constraints, and row count;
4. publish using the strongest atomic mechanism supported by the destination;
5. retain the previous table if publication fails;
6. clean failed staging state;
7. mark Export Run completed only after verification.

Append and Upsert are deferred.

If a destination cannot support a safe replace operation, the capability check disables `Replace Atomically` rather than pretending it is atomic.

## 11. Export Job Behavior

Exports stream records and files to avoid loading a complete large Dataset into memory.

Progress includes:

- preparing schema;
- records written/total;
- validation;
- publication;
- checksum generation;
- bundle finalization.

Cancellation removes partial files or marks staging state for safe cleanup. A failed export never presents a partial result as complete.

## 12. Asset Discovery and Download

The Assets tab displays discovered files with:

- preview when safe;
- original URL;
- filename;
- MIME type;
- size when known;
- source and Crawl Run;
- status: URL Only, Downloading, Downloaded, Failed, Blocked;
- local path when downloaded;
- checksum.

MVP behavior:

- store URL and metadata by default;
- user selects assets to download;
- batch `Download Selected`;
- remove local copy without deleting URL metadata;
- assets follow the Source/Document approval context and do not have separate approval states.

## 13. Artifact and Asset Retention

Raw artifacts are retained indefinitely by default.

Configurable policies at global and Collection scopes:

- indefinitely;
- N days;
- latest N runs;
- approved versions only.

Retention cleanup previews:

- files affected;
- estimated reclaimed storage;
- provenance impact;
- metadata retained.

Cleanup never removes:

- approved curated values;
- audit events;
- source and Crawl Run summaries;
- artifact hashes referenced by approved provenance;
- Schema Version references;
- deletion and approval history.

Automatic cleanup is off by default unless the user explicitly enables a schedule.

## 14. Backup Types

### Database Only, default

Contains:

- Turso application database snapshot;
- database schema version;
- settings;
- Schemas and versions;
- Dataset and Record versions;
- approvals;
- provenance metadata;
- audit trail;
- manifest and checksums.

### Full Backup

Contains Database Only plus:

- raw HTML and rendered DOM;
- Markdown and structured artifacts;
- screenshots;
- detailed logs;
- downloaded assets;
- export files when selected by the defined backup policy;
- artifact index and checksums.

Full Backup estimates size before starting and reports files that could not be included.

## 15. Backup File Format

All backups use one portable extension:

```text
*.erabi-backup
```

The format includes:

- magic bytes and format version;
- encrypted/not-encrypted flag;
- backup type;
- creation metadata;
- compressed archive payload;
- manifest;
- checksums;
- optional encryption metadata.

The format version is independent of the application version.

## 16. Backup Encryption

Encryption is optional and off by default.

When enabled:

- the complete payload, including manifest and checksums, is encrypted;
- password is never stored in database, `.env`, logs, or backup history;
- a stable, audited Rust cryptography library is used;
- keys are derived with a strong password-based KDF;
- encryption uses authenticated encryption;
- wrong password or corrupt data changes nothing in the active installation;
- there is no password recovery;
- partial output is removed after failure or cancellation.

Cryptographic algorithms are selected during implementation from the latest stable, maintained libraries and are documented in the backup format specification. Erabi does not invent custom cryptography.

## 17. Automatic Backup

Backup and Restore are MVP features. Automatic backup is off by default.

Configurable schedules:

- before migration;
- daily;
- weekly;
- retain latest N;
- destination directory;
- verify after creation.

When migration requires a backup and automatic backup is off, interactive use offers:

- Create Backup and Continue;
- Continue Without Backup;
- Cancel.

Non-interactive Docker startup follows a deterministic bootstrap setting and never waits forever for a UI response.

## 18. Restore

Restore procedure:

1. stop accepting jobs and mutations;
2. checkpoint active jobs;
3. verify format, password, checksums, and compatibility;
4. optionally create a safety backup of the current database;
5. restore database to a temporary location;
6. run migration and integrity checks against the temporary state;
7. restore artifacts to the selected directory for Full Backup;
8. atomically activate the restored state;
9. recover queue state;
10. retain an audit/system record of the operation.

A failed verification or restore does not modify the current active database.

## 19. Backup Verification

`Verify Backup` works without restoring. It checks:

- file format;
- password/authentication tag where encrypted;
- archive readability;
- checksums;
- manifest consistency;
- database readability;
- database schema version compatibility;
- artifact index consistency for Full Backup.
