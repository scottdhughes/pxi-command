-- Run-level pipeline lock guard to prevent overlapping executions

CREATE TABLE IF NOT EXISTS pipeline_locks (
  lock_key TEXT PRIMARY KEY,
  lock_token TEXT NOT NULL,
  acquired_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pipeline_locks_acquired_at
ON pipeline_locks(acquired_at_utc);
