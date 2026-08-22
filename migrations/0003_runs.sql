CREATE TABLE crawl_runs (
    id TEXT PRIMARY KEY NOT NULL,
    run_type TEXT NOT NULL CHECK (run_type IN ('QUICK_SCRAPE', 'TEST_RUN', 'DISCOVERY_PREVIEW', 'PRODUCTION_RUN')),
    status TEXT NOT NULL CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'PARTIAL_RESULT', 'FAILED', 'CANCELLED')),
    crawler_id TEXT REFERENCES crawlers(id),
    crawler_version_id TEXT REFERENCES crawler_versions(id),
    snapshot_json TEXT NOT NULL,
    snapshot_hash TEXT NOT NULL,
    checkpoint_compatibility_hash TEXT NOT NULL,
    actor TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TRIGGER crawl_runs_snapshot_immutable
BEFORE UPDATE OF snapshot_json, snapshot_hash, checkpoint_compatibility_hash ON crawl_runs
BEGIN
    SELECT RAISE(ABORT, 'crawl run snapshots are immutable');
END;

CREATE TABLE discovered_urls (
    id TEXT PRIMARY KEY NOT NULL,
    crawl_run_id TEXT NOT NULL REFERENCES crawl_runs(id),
    source_id TEXT REFERENCES sources(id),
    raw_href TEXT,
    original_url TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    status TEXT NOT NULL,
    discovered_at TEXT NOT NULL,
    detail_json TEXT NOT NULL
);

CREATE TABLE artifacts (
    id TEXT PRIMARY KEY NOT NULL,
    crawl_run_id TEXT REFERENCES crawl_runs(id),
    source_id TEXT REFERENCES sources(id),
    content_hash TEXT NOT NULL,
    byte_size INTEGER NOT NULL CHECK (byte_size >= 0),
    media_type TEXT,
    safe_relative_path TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE INDEX crawl_runs_by_created_at ON crawl_runs (created_at);
CREATE INDEX discovered_urls_by_run ON discovered_urls (crawl_run_id, canonical_url);
CREATE INDEX artifacts_by_hash ON artifacts (content_hash);
