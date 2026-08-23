CREATE TABLE jobs (
    id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL CHECK (length(kind) BETWEEN 1 AND 64),
    priority INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    parent_job_id TEXT REFERENCES jobs(id),
    crawl_run_id TEXT REFERENCES crawl_runs(id),
    scheduled_at INTEGER NOT NULL,
    current_attempt INTEGER NOT NULL DEFAULT 0 CHECK (current_attempt >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts >= 1),
    lease_id TEXT,
    lease_owner TEXT,
    lease_generation INTEGER NOT NULL DEFAULT 0 CHECK (lease_generation >= 0),
    lease_acquired_at INTEGER,
    lease_expires_at INTEGER,
    heartbeat_at INTEGER,
    failure_code TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK (current_attempt <= max_attempts),
    CHECK (
        (state = 'RUNNING'
            AND lease_id IS NOT NULL
            AND lease_owner IS NOT NULL
            AND lease_acquired_at IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND heartbeat_at IS NOT NULL)
        OR
        (state <> 'RUNNING'
            AND lease_id IS NULL
            AND lease_owner IS NULL
            AND lease_acquired_at IS NULL
            AND lease_expires_at IS NULL
            AND heartbeat_at IS NULL)
    ),
    CHECK (lease_expires_at IS NULL OR lease_expires_at >= heartbeat_at),
    CHECK (heartbeat_at IS NULL OR heartbeat_at >= lease_acquired_at)
);

CREATE TABLE job_attempts (
    id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
    lease_id TEXT NOT NULL,
    lease_generation INTEGER NOT NULL CHECK (lease_generation >= 1),
    worker_id TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    outcome TEXT NOT NULL CHECK (outcome IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'LEASE_EXPIRED')),
    failure_code TEXT,
    UNIQUE (job_id, attempt_number),
    UNIQUE (lease_id),
    CHECK (
        (outcome = 'RUNNING' AND finished_at IS NULL AND failure_code IS NULL)
        OR
        (outcome = 'SUCCEEDED' AND finished_at IS NOT NULL AND failure_code IS NULL)
        OR
        (outcome IN ('FAILED', 'LEASE_EXPIRED') AND finished_at IS NOT NULL AND failure_code IS NOT NULL)
    )
);

CREATE TABLE job_checkpoints (
    id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id),
    attempt_id TEXT REFERENCES job_attempts(id),
    checkpoint_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE job_progress_events (
    id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id),
    attempt_id TEXT REFERENCES job_attempts(id),
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    event_type TEXT NOT NULL CHECK (length(event_type) BETWEEN 1 AND 64),
    payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE (job_id, sequence)
);

CREATE INDEX jobs_ready_by_schedule ON jobs (state, scheduled_at, priority DESC, created_at, id);
CREATE INDEX jobs_running_by_lease_expiry ON jobs (state, lease_expires_at);
CREATE INDEX jobs_by_parent ON jobs (parent_job_id);
CREATE INDEX jobs_by_crawl_run ON jobs (crawl_run_id);
CREATE INDEX job_attempts_by_job ON job_attempts (job_id, attempt_number);
CREATE INDEX job_attempts_running_by_job ON job_attempts (job_id, outcome);
CREATE INDEX job_checkpoints_by_job ON job_checkpoints (job_id, created_at, id);
CREATE INDEX job_checkpoints_by_attempt ON job_checkpoints (attempt_id, created_at, id);
CREATE INDEX job_progress_events_by_job_sequence ON job_progress_events (job_id, sequence);
CREATE INDEX job_progress_events_by_attempt ON job_progress_events (attempt_id, sequence);

CREATE TRIGGER jobs_must_start_queued
BEFORE INSERT ON jobs
WHEN NEW.state <> 'QUEUED'
BEGIN
    SELECT RAISE(ABORT, 'jobs must be created queued');
END;

CREATE TRIGGER jobs_legal_state_transition
BEFORE UPDATE OF state ON jobs
WHEN NOT (
    (OLD.state = 'QUEUED' AND NEW.state IN ('RUNNING', 'CANCELLED'))
    OR (OLD.state = 'RUNNING' AND NEW.state IN ('QUEUED', 'SUCCEEDED', 'FAILED', 'CANCELLED'))
)
BEGIN
    SELECT RAISE(ABORT, 'illegal job lifecycle transition');
END;

CREATE TRIGGER jobs_lease_state_consistency
BEFORE UPDATE ON jobs
WHEN (
    (NEW.state = 'RUNNING' AND (
        NEW.lease_id IS NULL
        OR NEW.lease_owner IS NULL
        OR NEW.lease_acquired_at IS NULL
        OR NEW.lease_expires_at IS NULL
        OR NEW.heartbeat_at IS NULL
    ))
    OR
    (NEW.state <> 'RUNNING' AND (
        NEW.lease_id IS NOT NULL
        OR NEW.lease_owner IS NOT NULL
        OR NEW.lease_acquired_at IS NOT NULL
        OR NEW.lease_expires_at IS NOT NULL
        OR NEW.heartbeat_at IS NOT NULL
    ))
)
BEGIN
    SELECT RAISE(ABORT, 'job lease fields do not match lifecycle state');
END;

CREATE TRIGGER job_attempts_terminal_history_immutable
BEFORE UPDATE ON job_attempts
WHEN NOT (
    OLD.outcome = 'RUNNING'
    AND NEW.id = OLD.id
    AND NEW.job_id = OLD.job_id
    AND NEW.attempt_number = OLD.attempt_number
    AND NEW.lease_id = OLD.lease_id
    AND NEW.lease_generation = OLD.lease_generation
    AND NEW.worker_id = OLD.worker_id
    AND NEW.started_at = OLD.started_at
    AND NEW.outcome IN ('SUCCEEDED', 'FAILED', 'LEASE_EXPIRED')
    AND NEW.finished_at IS NOT NULL
)
BEGIN
    SELECT RAISE(ABORT, 'completed job attempt history is immutable');
END;

CREATE TRIGGER job_attempts_no_delete
BEFORE DELETE ON job_attempts
BEGIN
    SELECT RAISE(ABORT, 'job attempt history cannot be deleted');
END;

CREATE TRIGGER job_checkpoints_no_update
BEFORE UPDATE ON job_checkpoints
BEGIN
    SELECT RAISE(ABORT, 'job checkpoints are append-only');
END;

CREATE TRIGGER job_checkpoints_no_delete
BEFORE DELETE ON job_checkpoints
BEGIN
    SELECT RAISE(ABORT, 'job checkpoints cannot be deleted');
END;

CREATE TRIGGER job_progress_events_no_update
BEFORE UPDATE ON job_progress_events
BEGIN
    SELECT RAISE(ABORT, 'job progress events are append-only');
END;

CREATE TRIGGER job_progress_events_no_delete
BEFORE DELETE ON job_progress_events
BEGIN
    SELECT RAISE(ABORT, 'job progress events cannot be deleted');
END;
