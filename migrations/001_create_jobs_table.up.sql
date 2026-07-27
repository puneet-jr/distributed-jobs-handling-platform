CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    payload JSONB NOT NULL,
    idempotency_key TEXT UNIQUE,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 5,
    error_message TEXT,
    worker_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    next_run_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    CONSTRAINT jobs_status_check CHECK (
        status IN ('pending', 'running', 'completed', 'failed', 'retrying', 'cancelled')
    ),
    CONSTRAINT jobs_retry_count_check CHECK (retry_count >= 0),
    CONSTRAINT jobs_max_retries_check CHECK (max_retries >= 0)
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_next_run_at
    ON jobs (status, next_run_at);

CREATE INDEX IF NOT EXISTS idx_jobs_running_started_at
    ON jobs (status, started_at)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_jobs_created_at
    ON jobs (created_at DESC);
