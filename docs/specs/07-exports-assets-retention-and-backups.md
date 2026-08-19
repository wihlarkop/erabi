# Exports, Assets, Retention, and Backups Specification

## 1. Export principles

Normal exports use **approved records only**.

Debug/development export flows may explicitly include drafts/rejections and diagnostic metadata, but this is opt-in and visibly distinct from a trusted standard export.

MVP formats:

- JSON;
- JSONL;
- CSV;
- Markdown;
- SQLite;
- Turso.

PostgreSQL and other destinations are roadmap.

## 2. Provenance sidecars

Provenance is not mixed into standard clean dataset fields by default.

When the user selects **With Provenance**, Erabi creates a bundle such as:

```text
dataset-export.zip
├── data/
│   └── dataset.csv
├── provenance/
│   └── fields.provenance.jsonl
├── manifest.json
└── checksums.sha256
```

JSONL is preferred for large provenance sidecars because it can be streamed.

The provenance sidecar can include record ID, field, source URL/reference, selector, raw/normalized values, transformations, Page Type, Crawler Version, Crawl Run, and artifact hash/reference.

The manifest records dataset/version identity, export ID/time, record count, schema/crawler references, file list, checksums, and Erabi format/application versions.

## 3. Export modes

Conceptual modes:

- **Standard** — clean approved data;
- **With Provenance** — data + provenance sidecar + manifest + checksums in ZIP;
- **Debug Bundle** — explicitly selected logs/raw artifacts/diagnostics in addition to provenance.

Debug Bundle is clearly marked as potentially sensitive.

## 4. Export naming and lifecycle

MVP generates safe filenames automatically:

```text
{dataset-slug}-{date}-{short-export-id}
```

Custom filename editing is roadmap.

Exports are never silently overwritten. Each Export Run has UUIDv7 identity and its own file.

Export files remain available until explicitly deleted or covered by an enabled export retention policy. Automatic export cleanup is OFF by default.

When a file is deleted, Export Run history remains with status such as `FILE_REMOVED`, plus format, timestamp, record count, checksum/manifest summary, and audit evidence.

Regenerate Export from history is roadmap; users can create a new export manually from available dataset versions in MVP.

## 5. Saved destinations and Test Connection

A Collection/Crawler may save reusable destination configuration. Destination records store non-secret configuration and secret environment-variable references.

Test Connection validates:

- reachability;
- authentication;
- required create/write operations;
- staging/rename/drop operations needed by selected export mode;
- transaction capabilities;
- detected DB version/capabilities;
- basic latency summary.

Capabilities are revalidated when an export starts.

## 6. Atomic database export

Turso/SQLite database export is atomic from the user's perspective.

MVP modes:

### Create New — default

- create a new versioned/unique target table;
- stream records;
- validate row count/schema/constraints;
- publish success only after validation.

### Replace Atomically

- create staging table;
- stream/validate all records;
- atomically swap/rename/publish according to destination capability;
- preserve prior valid target when the new export fails.

Append and Upsert are roadmap.

Failed exports clean their staging state where safe and never report partial staging content as success.

## 7. Destination database organization

For SQLite/Turso-style destinations without PostgreSQL schema namespaces, Erabi supports:

- dedicated DB per Collection/Dataset use case;
- shared DB with deterministic table namespace/prefix such as `{collection_slug}__{dataset_name}`.

Erabi maintains metadata sufficient to map Collection/Dataset/export version to physical destination tables.

Records should use real typed table columns rather than only opaque JSON blobs. Optional raw/debug JSON storage may supplement, not replace, the useful schema.

## 8. Assets model

Erabi discovers asset references during crawling/extraction. Default behavior stores URL + metadata only rather than downloading every asset.

Assets UI supports:

- image preview where safe;
- MIME type;
- size if known;
- original URL;
- source/run provenance;
- local status: URL_ONLY, DOWNLOADING, DOWNLOADED, FAILED, BLOCKED;
- select and Download Selected;
- remove local file while retaining the URL reference.

Assets follow source/record review status in MVP; there is no independent asset-approval workflow.

## 9. Exported assets

Standard export includes asset references/metadata, not physical files.

With explicit `Include downloaded assets`, the bundle adds an `assets/` tree and records checksums/missing/failed states in the manifest.

Download and export are separate actions; exporting does not trigger arbitrary missing asset downloads unless the UI explicitly says so.

## 10. Artifact retention

Raw artifacts include categories such as:

- raw HTML;
- cleaned HTML;
- rendered DOM;
- extracted Markdown;
- structured extraction snapshots;
- screenshots;
- logs;
- failed response diagnostics.

Retention options may include:

- indefinitely;
- N days;
- latest N runs;
- approved/reference-required variants.

Default is conservative/no destructive cleanup unless the user configures otherwise.

Single-page screenshot default may be ON while large batch/crawl screenshot default is OFF. Settings may be global/Collection/Crawler/run operational overrides where allowed.

## 11. Backup types

MVP supports two backup types.

### Database Only — default

Contains:

- internal application database snapshot;
- migration/schema metadata;
- settings;
- crawler/domain configuration;
- version/approval/audit state;
- manifest and checksums.

### Full Backup

Contains Database Only plus selected/all local artifacts, logs, screenshots, downloaded assets, and artifact index.

Full Backup reports estimated size and progress and handles cancellation without leaving a misleading valid file.

## 12. `.erabi-backup` format

Both encrypted and unencrypted backups use one versioned portable file extension:

```text
*.erabi-backup
```

The container format records at least:

- format version;
- backup type;
- application/database compatibility metadata;
- payload manifest;
- checksums/integrity metadata;
- encryption metadata when enabled.

Internal representation can evolve as long as the outer format is versioned and restore can reject incompatible/invalid data safely.

## 13. Backup encryption

Password encryption is optional and OFF by default.

Requirements:

- use mature stable Rust crypto/password-KDF libraries, never custom cryptography;
- password is never stored in Turso, `.env`, logs, or backup metadata in recoverable plaintext;
- wrong password/corrupt file never mutates active data;
- restore verifies integrity before replacement;
- no password recovery promise;
- partial cancelled/failed backup files are cleaned or clearly invalidated.

## 14. Automatic backup

Automatic backup is OFF by default and configurable in Settings.

Possible policies:

- before database migration;
- daily;
- weekly;
- keep latest N;
- verify after creation.

Manual Create / Verify / Download / Restore / Delete Backup actions are MVP.

## 15. Restore flow

Restore is a controlled maintenance operation:

1. stop accepting new jobs/mutations;
2. settle/cancel active work at safe boundaries;
3. verify backup format/checksum/password/compatibility;
4. optionally create a safety snapshot of current state;
5. restore DB/artifact payload according to selected backup type;
6. run migration/compatibility handling if explicitly supported;
7. run integrity check;
8. rebuild queue/runtime state;
9. return to normal service only when healthy.

Failure must leave the prior active data untouched whenever possible and otherwise force Recovery Mode rather than pretending normal health.
