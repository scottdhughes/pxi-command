-- Durable Cloudflare Cron execution ledger and missed daily-close incidents.
-- One row is reused across at-least-once delivery attempts for each slot.
CREATE TABLE IF NOT EXISTS refresh_scheduler_runs (
    slot_key TEXT PRIMARY KEY,
    schedule_id TEXT NOT NULL CHECK(schedule_id IN (
        'overnight', 'premarket', 'midday', 'daily_close'
    )),
    scheduled_at TEXT NOT NULL,
    decision_date TEXT NOT NULL CHECK(
        length(decision_date) = 10
        AND decision_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
    ),
    status TEXT NOT NULL CHECK(status IN ('running', 'success', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 1 CHECK(attempt_count >= 1),
    claimed_at TEXT NOT NULL,
    completed_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(schedule_id, scheduled_at),
    CHECK(
        (status = 'running' AND completed_at IS NULL)
        OR (status IN ('success', 'failed') AND completed_at IS NOT NULL)
    ),
    CHECK(status <> 'success' OR last_error IS NULL)
);

CREATE INDEX IF NOT EXISTS idx_refresh_scheduler_runs_decision
ON refresh_scheduler_runs(schedule_id, decision_date DESC, scheduled_at DESC);

CREATE INDEX IF NOT EXISTS idx_refresh_scheduler_runs_status
ON refresh_scheduler_runs(status, updated_at DESC);

-- Incidents retain history after recovery. The unique date/type constraint
-- makes repeated watchdog delivery update one incident instead of multiplying it.
CREATE TABLE IF NOT EXISTS refresh_scheduler_incidents (
    incident_id TEXT PRIMARY KEY,
    incident_type TEXT NOT NULL CHECK(incident_type = 'missed_daily_close'),
    decision_date TEXT NOT NULL CHECK(
        length(decision_date) = 10
        AND decision_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
    ),
    status TEXT NOT NULL CHECK(status IN ('open', 'resolved')),
    expected_slot_key TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    last_checked_at TEXT NOT NULL,
    resolved_at TEXT,
    resolution_slot_key TEXT,
    details_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(incident_type, decision_date),
    CHECK(
        (status = 'open' AND resolved_at IS NULL AND resolution_slot_key IS NULL)
        OR (status = 'resolved' AND resolved_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_refresh_scheduler_incidents_status
ON refresh_scheduler_incidents(status, decision_date DESC);

-- One global lease coordinates every production path that mutates indicator
-- values or scores. Expired rows are atomically reclaimed in place.
CREATE TABLE IF NOT EXISTS refresh_mutation_locks (
    lock_name TEXT PRIMARY KEY CHECK(lock_name = 'indicator_score_mutation'),
    holder_id TEXT NOT NULL CHECK(length(trim(holder_id)) BETWEEN 1 AND 200),
    holder_type TEXT NOT NULL CHECK(holder_type IN (
        'cloudflare_cron',
        'deploy_smoke',
        'github_daily_refresh',
        'history_reconstruction',
        'deploy'
    )),
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    lease_version INTEGER NOT NULL DEFAULT 1 CHECK(lease_version >= 1),
    updated_at TEXT NOT NULL,
    CHECK(expires_at > acquired_at)
);
