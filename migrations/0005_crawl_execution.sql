CREATE TABLE crawl_execution_results (
    id TEXT PRIMARY KEY NOT NULL,
    crawl_run_id TEXT NOT NULL REFERENCES crawl_runs(id),
    requested_url TEXT NOT NULL CHECK (length(requested_url) BETWEEN 1 AND 4096),
    canonical_url TEXT NOT NULL CHECK (length(canonical_url) BETWEEN 1 AND 4096),
    observed_final_url TEXT CHECK (observed_final_url IS NULL OR length(observed_final_url) BETWEEN 1 AND 4096),
    source_id TEXT REFERENCES sources(id),
    page_type_id TEXT REFERENCES page_types(id),
    transition_id TEXT REFERENCES discovery_transitions(id),
    discovered_url_id TEXT REFERENCES discovered_urls(id),
    outcome TEXT NOT NULL CHECK (outcome IN ('COMPLETED', 'PARTIAL', 'FAILED', 'CANCELLED')),
    error_code TEXT CHECK (
        error_code IS NULL OR error_code IN (
            'ACCESS_DENIED',
            'NOT_FOUND',
            'TIMEOUT',
            'PROVIDER_UNAVAILABLE',
            'INVALID_RESPONSE',
            'RATE_LIMITED',
            'REMOTE_FAILURE',
            'UNSUPPORTED_CAPABILITY',
            'PARTIAL_RESULT',
            'CANCELLED',
            'ROBOTS_EXCLUDED',
            'PAGE_TYPE_AMBIGUOUS',
            'STORAGE_PRESSURE'
        )
    ),
    http_status INTEGER CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    media_type TEXT CHECK (media_type IS NULL OR length(media_type) BETWEEN 1 AND 256),
    content_length_bytes INTEGER CHECK (content_length_bytes IS NULL OR content_length_bytes >= 0),
    provider_elapsed_ms INTEGER CHECK (provider_elapsed_ms IS NULL OR provider_elapsed_ms >= 0),
    CHECK (
        (outcome = 'COMPLETED' AND error_code IS NULL)
        OR (outcome = 'PARTIAL' AND error_code = 'PARTIAL_RESULT')
        OR (
            outcome = 'FAILED'
            AND error_code IS NOT NULL
            AND error_code NOT IN ('PARTIAL_RESULT', 'CANCELLED')
        )
        OR (outcome = 'CANCELLED' AND error_code = 'CANCELLED')
    )
);

CREATE TABLE crawl_execution_artifacts (
    crawl_execution_id TEXT NOT NULL REFERENCES crawl_execution_results(id),
    artifact_id TEXT NOT NULL REFERENCES artifacts(id),
    artifact_kind TEXT NOT NULL CHECK (artifact_kind IN ('RAW_HTML', 'CLEANED_HTML', 'RENDERED_HTML', 'MARKDOWN', 'SCREENSHOT')),
    PRIMARY KEY (crawl_execution_id, artifact_kind),
    UNIQUE (crawl_execution_id, artifact_id)
);

CREATE TABLE crawl_execution_summaries (
    crawl_run_id TEXT PRIMARY KEY NOT NULL REFERENCES crawl_runs(id),
    in_scope_pages_planned INTEGER NOT NULL CHECK (in_scope_pages_planned >= 0),
    in_scope_pages_completed INTEGER NOT NULL CHECK (in_scope_pages_completed >= 0),
    pagination_truncation_count INTEGER NOT NULL CHECK (pagination_truncation_count >= 0),
    unresolved_partial_work_count INTEGER NOT NULL CHECK (unresolved_partial_work_count >= 0),
    page_type_ambiguity_count INTEGER NOT NULL CHECK (page_type_ambiguity_count >= 0),
    CHECK (in_scope_pages_completed <= in_scope_pages_planned)
);

CREATE INDEX crawl_execution_results_by_run_identity
    ON crawl_execution_results (crawl_run_id, canonical_url COLLATE BINARY, id COLLATE BINARY);
CREATE INDEX crawl_execution_results_by_run_outcome
    ON crawl_execution_results (crawl_run_id, outcome, canonical_url COLLATE BINARY, id COLLATE BINARY);
CREATE INDEX crawl_execution_artifacts_by_artifact
    ON crawl_execution_artifacts (artifact_id, crawl_execution_id COLLATE BINARY);
