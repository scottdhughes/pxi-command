ALTER TABLE market_refresh_runs RENAME TO market_refresh_runs_legacy_status;

CREATE TABLE market_refresh_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL CHECK(status IN ('running', 'success', 'failed', 'blocked')),
    "trigger" TEXT NOT NULL DEFAULT 'unknown',
    brief_generated INTEGER DEFAULT 0,
    opportunities_generated INTEGER DEFAULT 0,
    calibrations_generated INTEGER DEFAULT 0,
    alerts_generated INTEGER DEFAULT 0,
    stale_count INTEGER,
    critical_stale_count INTEGER,
    as_of TEXT,
    error TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

INSERT INTO market_refresh_runs (
    id,
    started_at,
    completed_at,
    status,
    "trigger",
    brief_generated,
    opportunities_generated,
    calibrations_generated,
    alerts_generated,
    stale_count,
    critical_stale_count,
    as_of,
    error,
    created_at
)
SELECT
    id,
    started_at,
    completed_at,
    status,
    "trigger",
    brief_generated,
    opportunities_generated,
    calibrations_generated,
    alerts_generated,
    stale_count,
    critical_stale_count,
    as_of,
    error,
    created_at
FROM market_refresh_runs_legacy_status;

DROP TABLE market_refresh_runs_legacy_status;

CREATE INDEX IF NOT EXISTS idx_market_refresh_runs_completed ON market_refresh_runs(status, completed_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_refresh_runs_created ON market_refresh_runs(created_at DESC);
