-- Task 9: authoritative, bounded crawl recovery state. Historical evidence
-- remains append-only; these tables are the mutable logical-work projection.
CREATE UNIQUE INDEX discovered_urls_run_id_id ON discovered_urls (crawl_run_id, id);
CREATE TABLE crawl_url_state (
    id TEXT PRIMARY KEY NOT NULL,
    crawl_run_id TEXT NOT NULL REFERENCES crawl_runs(id),
    canonical_url TEXT NOT NULL CHECK (length(canonical_url) > 0 AND length(canonical_url) <= 4096),
    first_discovered_url_id TEXT,
    requested_url TEXT NOT NULL CHECK (length(requested_url) > 0 AND length(requested_url) <= 4096),
    parent_url_state_id TEXT,
    parent_discovered_url_id TEXT,
    admission_state TEXT NOT NULL CHECK (admission_state IN ('ADMITTED', 'PRESERVE_ONLY', 'RESOLVED')),
    preserve_reason TEXT CHECK (preserve_reason IS NULL OR preserve_reason IN ('CANONICAL_DUPLICATE', 'AMBIGUOUS_PAGE_TYPE', 'UNMATCHED', 'EXTERNAL', 'BLOCKED', 'BUDGET_EXCLUDED', 'INVALID', 'ROBOTS_EXCLUDED', 'PROVIDER_ERROR', 'TRANSITION_INELIGIBLE', 'CANONICAL_REDIRECT')),
    resolved_to_url_state_id TEXT,
    admission_sequence INTEGER,
    depth INTEGER,
    target_page_type_id TEXT REFERENCES page_types(id),
    transition_id TEXT REFERENCES discovery_transitions(id),
    pagination INTEGER NOT NULL DEFAULT 0 CHECK (pagination IN (0, 1)),
    final_canonical_url TEXT CHECK (final_canonical_url IS NULL OR (length(final_canonical_url) > 0 AND length(final_canonical_url) <= 4096)),
    current_work_state TEXT CHECK (current_work_state IS NULL OR current_work_state IN ('PENDING', 'RUNNING', 'COMPLETED', 'PARTIAL', 'FAILED', 'CANCELLED')),
    work_generation INTEGER NOT NULL DEFAULT 0 CHECK (work_generation >= 0),
    current_execution_id TEXT,
    seen INTEGER NOT NULL DEFAULT 1 CHECK (seen IN (0, 1)),
    sampled INTEGER NOT NULL DEFAULT 0 CHECK (sampled IN (0, 1)),
    expanded INTEGER NOT NULL DEFAULT 0 CHECK (expanded IN (0, 1)),
    in_scope INTEGER NOT NULL DEFAULT 0 CHECK (in_scope IN (0, 1)),
    page_type_match_state TEXT CHECK (page_type_match_state IS NULL OR page_type_match_state IN ('MATCHED', 'UNMATCHED', 'AMBIGUOUS')),
    CHECK (expanded = 0 OR sampled = 1),
    CHECK (in_scope = (page_type_match_state IS NOT NULL)),
    CHECK (
        admission_state <> 'ADMITTED'
        OR (
            preserve_reason IS NULL
            AND resolved_to_url_state_id IS NULL
            AND admission_sequence IS NOT NULL
            AND depth IS NOT NULL
            AND current_work_state IS NOT NULL
        )
    ),
    CHECK (
        admission_state <> 'PRESERVE_ONLY'
        OR preserve_reason IS NOT NULL
    ),
    CHECK (admission_state <> 'PRESERVE_ONLY' OR resolved_to_url_state_id IS NULL),
    CHECK (admission_state <> 'PRESERVE_ONLY' OR admission_sequence IS NULL),
    CHECK (admission_state <> 'PRESERVE_ONLY' OR depth IS NULL),
    CHECK (admission_state <> 'PRESERVE_ONLY' OR target_page_type_id IS NULL),
    CHECK (admission_state <> 'PRESERVE_ONLY' OR transition_id IS NULL),
    CHECK (admission_state <> 'PRESERVE_ONLY' OR current_work_state IS NULL),
    CHECK (admission_state <> 'PRESERVE_ONLY' OR current_execution_id IS NULL),
    CHECK (admission_state <> 'PRESERVE_ONLY' OR sampled = 0),
    CHECK (admission_state <> 'PRESERVE_ONLY' OR expanded = 0),
    CHECK (admission_state <> 'RESOLVED' OR preserve_reason = 'CANONICAL_REDIRECT'),
    CHECK (admission_state <> 'RESOLVED' OR resolved_to_url_state_id IS NOT NULL),
    CHECK (admission_state <> 'RESOLVED' OR admission_sequence IS NULL),
    CHECK (admission_state <> 'RESOLVED' OR depth IS NULL),
    CHECK (admission_state <> 'RESOLVED' OR target_page_type_id IS NULL),
    CHECK (admission_state <> 'RESOLVED' OR transition_id IS NULL),
    CHECK (admission_state <> 'RESOLVED' OR current_work_state IS NULL),
    CHECK (admission_state <> 'RESOLVED' OR current_execution_id IS NULL),
    UNIQUE (crawl_run_id, id),
    UNIQUE (crawl_run_id, canonical_url),
    FOREIGN KEY (crawl_run_id, first_discovered_url_id) REFERENCES discovered_urls(crawl_run_id, id),
    FOREIGN KEY (crawl_run_id, parent_discovered_url_id) REFERENCES discovered_urls(crawl_run_id, id),
    FOREIGN KEY (crawl_run_id, parent_url_state_id) REFERENCES crawl_url_state(crawl_run_id, id),
    FOREIGN KEY (crawl_run_id, resolved_to_url_state_id) REFERENCES crawl_url_state(crawl_run_id, id),
    FOREIGN KEY (crawl_run_id, current_execution_id) REFERENCES crawl_execution_results(crawl_run_id, id)
);
CREATE UNIQUE INDEX crawl_url_state_admission_sequence_unique ON crawl_url_state (crawl_run_id, admission_sequence) WHERE admission_sequence IS NOT NULL;
CREATE INDEX crawl_url_state_recovery_order ON crawl_url_state (crawl_run_id, current_work_state, depth, admission_sequence, canonical_url COLLATE BINARY, requested_url COLLATE BINARY);
CREATE INDEX crawl_url_state_parent_provenance ON crawl_url_state (crawl_run_id, parent_url_state_id, parent_discovered_url_id);
CREATE INDEX crawl_url_state_semantic_sets ON crawl_url_state (crawl_run_id, seen, sampled, expanded, in_scope, page_type_match_state);

-- A recovery action is a logical Plan 04 continuation.  Its selected work is
-- immutable once prepared, so a replay of the same action job cannot advance
-- a second generation or broaden the executable set.
CREATE TABLE crawl_recovery_actions (
    action_job_id TEXT PRIMARY KEY NOT NULL REFERENCES jobs(id),
    source_job_id TEXT NOT NULL REFERENCES jobs(id),
    crawl_run_id TEXT NOT NULL REFERENCES crawl_runs(id),
    action_kind TEXT NOT NULL CHECK (action_kind IN ('RETRY', 'RETRY_FAILED_PARTS', 'RESTART_FROM_BEGINNING')),
    prepared_at INTEGER NOT NULL,
    UNIQUE (action_job_id, crawl_run_id)
);

CREATE TABLE crawl_recovery_action_items (
    action_job_id TEXT NOT NULL,
    crawl_run_id TEXT NOT NULL,
    crawl_url_state_id TEXT NOT NULL,
    previous_work_generation INTEGER NOT NULL CHECK (previous_work_generation >= 0),
    prepared_work_generation INTEGER NOT NULL CHECK (prepared_work_generation >= 0),
    PRIMARY KEY (action_job_id, crawl_url_state_id),
    FOREIGN KEY (action_job_id, crawl_run_id) REFERENCES crawl_recovery_actions(action_job_id, crawl_run_id),
    FOREIGN KEY (crawl_run_id, crawl_url_state_id) REFERENCES crawl_url_state(crawl_run_id, id),
    CHECK (prepared_work_generation = previous_work_generation + 1)
);
CREATE INDEX crawl_recovery_action_items_by_run ON crawl_recovery_action_items (crawl_run_id, action_job_id, crawl_url_state_id);

CREATE TRIGGER crawl_recovery_action_same_run
BEFORE INSERT ON crawl_recovery_actions
WHEN NOT EXISTS (
    SELECT 1
    FROM jobs AS action_job
    JOIN jobs AS source_job ON source_job.id = NEW.source_job_id
    WHERE action_job.id = NEW.action_job_id
      AND action_job.parent_job_id = source_job.id
      AND action_job.crawl_run_id = NEW.crawl_run_id
      AND source_job.crawl_run_id = NEW.crawl_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'recovery action lineage does not belong to one crawl run');
END;

CREATE TRIGGER crawl_recovery_action_immutable
BEFORE UPDATE ON crawl_recovery_actions
BEGIN
    SELECT RAISE(ABORT, 'recovery action identity is immutable');
END;

CREATE TRIGGER crawl_recovery_action_no_delete
BEFORE DELETE ON crawl_recovery_actions
BEGIN
    SELECT RAISE(ABORT, 'recovery action identity cannot be deleted');
END;

CREATE TRIGGER crawl_recovery_action_item_immutable
BEFORE UPDATE ON crawl_recovery_action_items
BEGIN
    SELECT RAISE(ABORT, 'recovery action selection is immutable');
END;

CREATE TRIGGER crawl_recovery_action_item_no_delete
BEFORE DELETE ON crawl_recovery_action_items
BEGIN
    SELECT RAISE(ABORT, 'recovery action selection cannot be deleted');
END;

CREATE TABLE crawl_url_seed_provenance (
    crawl_url_state_id TEXT NOT NULL REFERENCES crawl_url_state(id),
    seed_id TEXT NOT NULL REFERENCES seeds(id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY (crawl_url_state_id, seed_id),
    UNIQUE (crawl_url_state_id, ordinal)
);

CREATE TABLE crawl_traversal_page_type_counts (
    crawl_run_id TEXT NOT NULL REFERENCES crawl_runs(id),
    page_type_id TEXT NOT NULL REFERENCES page_types(id),
    sampled_count INTEGER NOT NULL DEFAULT 0 CHECK (sampled_count >= 0),
    discovered_count INTEGER NOT NULL DEFAULT 0 CHECK (discovered_count >= 0),
    PRIMARY KEY (crawl_run_id, page_type_id)
);
CREATE INDEX crawl_traversal_page_type_counts_run ON crawl_traversal_page_type_counts (crawl_run_id, page_type_id COLLATE BINARY);

CREATE TABLE crawl_transition_source_counts (
    crawl_run_id TEXT NOT NULL REFERENCES crawl_runs(id),
    transition_id TEXT NOT NULL REFERENCES discovery_transitions(id),
    source_url_state_id TEXT NOT NULL,
    eligible_edge_count INTEGER NOT NULL DEFAULT 0 CHECK (eligible_edge_count >= 0),
    PRIMARY KEY (crawl_run_id, transition_id, source_url_state_id),
    FOREIGN KEY (crawl_run_id, source_url_state_id) REFERENCES crawl_url_state(crawl_run_id, id)
);
CREATE INDEX crawl_transition_source_counts_totals ON crawl_transition_source_counts (crawl_run_id, transition_id, source_url_state_id);

CREATE TABLE crawl_traversal_control (
    crawl_run_id TEXT PRIMARY KEY NOT NULL REFERENCES crawl_runs(id),
    consumed_bytes INTEGER NOT NULL DEFAULT 0 CHECK (consumed_bytes >= 0),
    raw_link_count INTEGER NOT NULL DEFAULT 0 CHECK (raw_link_count >= 0),
    duplicate_count INTEGER NOT NULL DEFAULT 0 CHECK (duplicate_count >= 0),
    robots_excluded_count INTEGER NOT NULL DEFAULT 0 CHECK (robots_excluded_count >= 0),
    provider_error_count INTEGER NOT NULL DEFAULT 0 CHECK (provider_error_count >= 0),
    external_url_count INTEGER NOT NULL DEFAULT 0 CHECK (external_url_count >= 0),
    blocked_url_count INTEGER NOT NULL DEFAULT 0 CHECK (blocked_url_count >= 0),
    peak_expansion_count INTEGER NOT NULL DEFAULT 0 CHECK (peak_expansion_count >= 0),
    elapsed_millis INTEGER NOT NULL DEFAULT 0 CHECK (elapsed_millis >= 0),
    time_budget_hit INTEGER NOT NULL DEFAULT 0 CHECK (time_budget_hit IN (0, 1)),
    duration_work_not_expanded INTEGER NOT NULL DEFAULT 0 CHECK (duration_work_not_expanded IN (0, 1)),
    pagination_truncation_count INTEGER NOT NULL DEFAULT 0 CHECK (pagination_truncation_count >= 0),
    next_admission_sequence INTEGER NOT NULL CHECK (next_admission_sequence >= 0)
);

ALTER TABLE crawl_execution_results ADD COLUMN crawl_url_state_id TEXT REFERENCES crawl_url_state(id);
ALTER TABLE crawl_execution_results ADD COLUMN job_attempt_id TEXT REFERENCES job_attempts(id);
ALTER TABLE crawl_execution_results ADD COLUMN work_generation INTEGER NOT NULL DEFAULT 0 CHECK (work_generation >= 0);
CREATE INDEX crawl_execution_results_recovery_lineage ON crawl_execution_results (crawl_run_id, crawl_url_state_id, work_generation, id COLLATE BINARY);
CREATE UNIQUE INDEX crawl_execution_results_run_id_id ON crawl_execution_results (crawl_run_id, id);

CREATE TRIGGER crawl_url_state_work_generation_monotonic BEFORE UPDATE OF work_generation ON crawl_url_state WHEN NEW.work_generation < OLD.work_generation BEGIN SELECT RAISE(ABORT, 'crawl work generation cannot decrease'); END;
CREATE TRIGGER crawl_url_state_current_execution_matches_work BEFORE UPDATE OF current_execution_id, work_generation, crawl_run_id ON crawl_url_state WHEN NEW.current_execution_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM crawl_execution_results WHERE id = NEW.current_execution_id AND crawl_run_id = NEW.crawl_run_id AND crawl_url_state_id = NEW.id AND work_generation = NEW.work_generation) BEGIN SELECT RAISE(ABORT, 'current execution does not match crawl work state'); END;
CREATE TRIGGER crawl_execution_result_matches_url_state BEFORE INSERT ON crawl_execution_results WHEN NEW.crawl_url_state_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM crawl_url_state WHERE id = NEW.crawl_url_state_id AND crawl_run_id = NEW.crawl_run_id AND work_generation = NEW.work_generation) BEGIN SELECT RAISE(ABORT, 'execution result does not match crawl work state'); END;
CREATE TRIGGER crawl_execution_result_update_matches_url_state BEFORE UPDATE OF crawl_url_state_id, crawl_run_id, work_generation ON crawl_execution_results WHEN NEW.crawl_url_state_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM crawl_url_state WHERE id = NEW.crawl_url_state_id AND crawl_run_id = NEW.crawl_run_id AND work_generation = NEW.work_generation) BEGIN SELECT RAISE(ABORT, 'execution result does not match crawl work state'); END;
CREATE TRIGGER crawl_execution_result_attempt_matches_run BEFORE INSERT ON crawl_execution_results WHEN NEW.job_attempt_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM job_attempts AS attempt JOIN jobs AS job ON job.id = attempt.job_id WHERE attempt.id = NEW.job_attempt_id AND job.crawl_run_id = NEW.crawl_run_id) BEGIN SELECT RAISE(ABORT, 'execution attempt does not match crawl run'); END;
CREATE TRIGGER crawl_execution_result_update_attempt_matches_run BEFORE UPDATE OF job_attempt_id, crawl_run_id ON crawl_execution_results WHEN NEW.job_attempt_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM job_attempts AS attempt JOIN jobs AS job ON job.id = attempt.job_id WHERE attempt.id = NEW.job_attempt_id AND job.crawl_run_id = NEW.crawl_run_id) BEGIN SELECT RAISE(ABORT, 'execution attempt does not match crawl run'); END;
CREATE TRIGGER crawl_execution_result_discovery_matches_run BEFORE INSERT ON crawl_execution_results WHEN NEW.discovered_url_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM discovered_urls WHERE id = NEW.discovered_url_id AND crawl_run_id = NEW.crawl_run_id) BEGIN SELECT RAISE(ABORT, 'execution discovery evidence does not match crawl run'); END;
CREATE TRIGGER crawl_execution_result_update_discovery_matches_run BEFORE UPDATE OF discovered_url_id, crawl_run_id ON crawl_execution_results WHEN NEW.discovered_url_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM discovered_urls WHERE id = NEW.discovered_url_id AND crawl_run_id = NEW.crawl_run_id) BEGIN SELECT RAISE(ABORT, 'execution discovery evidence does not match crawl run'); END;
CREATE TRIGGER crawl_execution_result_lineage_is_complete BEFORE INSERT ON crawl_execution_results WHEN (NEW.crawl_url_state_id IS NULL) != (NEW.job_attempt_id IS NULL) BEGIN SELECT RAISE(ABORT, 'execution lineage is incomplete'); END;
CREATE TRIGGER crawl_execution_result_update_lineage_is_complete BEFORE UPDATE OF crawl_url_state_id, job_attempt_id ON crawl_execution_results WHEN (NEW.crawl_url_state_id IS NULL) != (NEW.job_attempt_id IS NULL) BEGIN SELECT RAISE(ABORT, 'execution lineage is incomplete'); END;
