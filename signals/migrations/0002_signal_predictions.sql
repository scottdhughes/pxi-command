-- Signal predictions table for tracking prediction accuracy
-- Each row represents a prediction made for a theme at signal generation time

CREATE TABLE IF NOT EXISTS signal_predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    signal_date TEXT NOT NULL,
    target_date TEXT NOT NULL,
    theme_id TEXT NOT NULL,
    theme_name TEXT NOT NULL,
    rank INTEGER NOT NULL,
    score REAL NOT NULL,
    signal_type TEXT NOT NULL,
    confidence TEXT NOT NULL,
    timing TEXT NOT NULL,
    stars INTEGER NOT NULL,
    proxy_etf TEXT,
    entry_price REAL,
    exit_price REAL,
    return_pct REAL,
    evaluated_at TEXT,
    hit INTEGER,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(run_id, theme_id)
);

-- Index for querying predictions by target date (for evaluation)
CREATE INDEX IF NOT EXISTS idx_signal_predictions_target ON signal_predictions(target_date);

-- Partial index for pending predictions (not yet evaluated)
CREATE INDEX IF NOT EXISTS idx_signal_predictions_pending ON signal_predictions(evaluated_at) WHERE evaluated_at IS NULL;

-- Index for accuracy calculations by timing
CREATE INDEX IF NOT EXISTS idx_signal_predictions_timing ON signal_predictions(timing) WHERE evaluated_at IS NOT NULL;

-- Index for accuracy calculations by confidence  
CREATE INDEX IF NOT EXISTS idx_signal_predictions_confidence ON signal_predictions(confidence) WHERE evaluated_at IS NOT NULL;
