CREATE TABLE collections (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    tags_json TEXT NOT NULL
);

CREATE TABLE sources (
    id TEXT PRIMARY KEY NOT NULL,
    collection_id TEXT REFERENCES collections(id),
    name TEXT NOT NULL,
    original_url TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    target_type TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE crawlers (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    collection_id TEXT REFERENCES collections(id),
    operational_defaults_json TEXT NOT NULL,
    active_published_version_id TEXT,
    active_draft_version_id TEXT
);

CREATE TABLE crawler_versions (
    id TEXT PRIMARY KEY NOT NULL,
    crawler_id TEXT NOT NULL REFERENCES crawlers(id),
    state TEXT NOT NULL CHECK (state IN ('DRAFT', 'PUBLISHED')),
    semantic_configuration_json TEXT NOT NULL
);

CREATE TRIGGER crawler_versions_published_no_update
BEFORE UPDATE ON crawler_versions
WHEN OLD.state = 'PUBLISHED'
BEGIN
    SELECT RAISE(ABORT, 'published crawler versions are immutable');
END;

CREATE TRIGGER crawler_versions_published_no_delete
BEFORE DELETE ON crawler_versions
WHEN OLD.state = 'PUBLISHED'
BEGIN
    SELECT RAISE(ABORT, 'published crawler versions are immutable');
END;

CREATE TABLE seeds (
    id TEXT PRIMARY KEY NOT NULL,
    crawler_version_id TEXT NOT NULL REFERENCES crawler_versions(id),
    original_url TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    enabled INTEGER NOT NULL,
    label TEXT,
    entry_page_type_hint_id TEXT
);

CREATE TABLE page_types (
    id TEXT PRIMARY KEY NOT NULL,
    crawler_version_id TEXT NOT NULL REFERENCES crawler_versions(id),
    name TEXT NOT NULL,
    priority INTEGER NOT NULL,
    configuration_json TEXT NOT NULL
);

CREATE TABLE url_matchers (
    id TEXT PRIMARY KEY NOT NULL,
    page_type_id TEXT NOT NULL REFERENCES page_types(id),
    ordinal INTEGER NOT NULL,
    matcher_json TEXT NOT NULL,
    UNIQUE (page_type_id, ordinal)
);

CREATE TABLE discovery_transitions (
    id TEXT PRIMARY KEY NOT NULL,
    crawler_version_id TEXT NOT NULL REFERENCES crawler_versions(id),
    configuration_json TEXT NOT NULL
);

CREATE TABLE run_profiles (
    id TEXT PRIMARY KEY NOT NULL,
    crawler_id TEXT NOT NULL REFERENCES crawlers(id),
    name TEXT NOT NULL,
    operational_overrides_json TEXT NOT NULL
);

CREATE TABLE test_evidence (
    id TEXT PRIMARY KEY NOT NULL,
    crawler_version_id TEXT NOT NULL REFERENCES crawler_versions(id),
    evidence_json TEXT NOT NULL,
    executed_at TEXT NOT NULL
);

CREATE INDEX crawler_versions_by_crawler ON crawler_versions (crawler_id, state);
CREATE INDEX sources_by_collection ON sources (collection_id);
