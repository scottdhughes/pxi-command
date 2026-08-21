CREATE TABLE IF NOT EXISTS research_feature_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id TEXT NOT NULL UNIQUE,
    decision_date TEXT NOT NULL,
    available_at TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    storage_contract TEXT NOT NULL,
    capture_source TEXT NOT NULL,
    benchmark_close REAL NOT NULL,
    benchmark_observation_date TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_research_feature_snapshots_decision
ON research_feature_snapshots(decision_date, available_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_feature_snapshots_available
ON research_feature_snapshots(available_at DESC);

CREATE TRIGGER IF NOT EXISTS research_feature_snapshots_no_update
BEFORE UPDATE ON research_feature_snapshots
BEGIN
    SELECT RAISE(ABORT, 'research feature snapshots are immutable');
END;

CREATE TRIGGER IF NOT EXISTS research_feature_snapshots_no_delete
BEFORE DELETE ON research_feature_snapshots
BEGIN
    SELECT RAISE(ABORT, 'research feature snapshots are immutable');
END;
