CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS migration_lock (
    lock_key TEXT PRIMARY KEY NOT NULL,
    owner TEXT NOT NULL,
    acquired_at TEXT NOT NULL
);

CREATE TABLE system_metadata (
    key TEXT PRIMARY KEY NOT NULL,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE settings (
    id TEXT PRIMARY KEY NOT NULL,
    scope_type TEXT NOT NULL CHECK (scope_type IN ('GLOBAL', 'COLLECTION', 'CRAWLER', 'RUN_PROFILE')),
    scope_id TEXT,
    setting_key TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('INHERIT', 'CUSTOM', 'RESET_TO_BUILT_IN')),
    value_json TEXT,
    updated_at TEXT NOT NULL,
    UNIQUE (scope_type, scope_id, setting_key),
    CHECK (
        (state = 'CUSTOM' AND value_json IS NOT NULL)
        OR (state IN ('INHERIT', 'RESET_TO_BUILT_IN') AND value_json IS NULL)
    )
);

CREATE TABLE audit_events (
    id TEXT PRIMARY KEY NOT NULL,
    event_type TEXT NOT NULL,
    actor TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE local_data_owners (
    canonical_data_directory TEXT PRIMARY KEY NOT NULL,
    process_id INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    erabi_version TEXT NOT NULL,
    bind_address TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE persisted_destinations (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    destination_kind TEXT NOT NULL,
    configuration_json TEXT NOT NULL,
    secret_environment_variable_name TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
