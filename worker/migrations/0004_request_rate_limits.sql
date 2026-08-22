CREATE TABLE IF NOT EXISTS request_rate_limit_buckets (
    scope TEXT NOT NULL,
    subject_hash TEXT NOT NULL,
    window_start INTEGER NOT NULL,
    count INTEGER NOT NULL DEFAULT 0 CHECK (count >= 0),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (scope, subject_hash, window_start)
);

CREATE INDEX IF NOT EXISTS idx_request_rate_limit_window
ON request_rate_limit_buckets(window_start);
