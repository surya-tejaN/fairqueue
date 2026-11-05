CREATE TABLE IF NOT EXISTS jobs (
    id            TEXT PRIMARY KEY,
    payload       TEXT NOT NULL,
    status        TEXT NOT NULL,
    attempts      INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL DEFAULT 3,
    visible_at    TIMESTAMP NOT NULL,
    created_at    TIMESTAMP NOT NULL,
    result        TEXT
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key        TEXT PRIMARY KEY,
    job_id     TEXT NOT NULL REFERENCES jobs(id),
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS jobs_claim_idx
    ON jobs (status, visible_at, created_at);
