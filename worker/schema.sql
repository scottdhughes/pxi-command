-- PXI Database Schema for D1 (SQLite)

-- Raw indicator values from data sources
CREATE TABLE IF NOT EXISTS indicator_values (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator_id TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL NOT NULL,
    source TEXT NOT NULL,
    fetched_at TEXT DEFAULT (datetime('now')),
    UNIQUE(indicator_id, date)
);

CREATE INDEX IF NOT EXISTS idx_indicator_values_date ON indicator_values(date DESC);
CREATE INDEX IF NOT EXISTS idx_indicator_values_indicator ON indicator_values(indicator_id);
CREATE INDEX IF NOT EXISTS idx_indicator_values_lookup ON indicator_values(indicator_id, date DESC);

-- Normalized indicator scores (0-100)
CREATE TABLE IF NOT EXISTS indicator_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator_id TEXT NOT NULL,
    date TEXT NOT NULL,
    raw_value REAL NOT NULL,
    normalized_value REAL NOT NULL,
    percentile_rank REAL,
    lookback_days INTEGER DEFAULT 504,
    calculated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(indicator_id, date)
);

CREATE INDEX IF NOT EXISTS idx_indicator_scores_date ON indicator_scores(date DESC);
CREATE INDEX IF NOT EXISTS idx_indicator_scores_lookup ON indicator_scores(indicator_id, date DESC);

-- Category scores
CREATE TABLE IF NOT EXISTS category_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT NOT NULL,
    date TEXT NOT NULL,
    score REAL NOT NULL,
    weight REAL NOT NULL,
    weighted_score REAL NOT NULL,
    calculated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(category, date)
);

CREATE INDEX IF NOT EXISTS idx_category_scores_date ON category_scores(date DESC);

-- Final PXI composite scores
CREATE TABLE IF NOT EXISTS pxi_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL UNIQUE,
    score REAL NOT NULL,
    label TEXT NOT NULL,
    status TEXT NOT NULL,
    delta_1d REAL,
    delta_7d REAL,
    delta_30d REAL,
    calculated_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_pxi_scores_date ON pxi_scores(date DESC);

-- Fetch logs for monitoring
CREATE TABLE IF NOT EXISTS fetch_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    indicator_id TEXT,
    status TEXT NOT NULL,
    records_fetched INTEGER DEFAULT 0,
    error_message TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_fetch_logs_source ON fetch_logs(source, started_at DESC);

-- Market regime embeddings for AI/ML
CREATE TABLE IF NOT EXISTS market_embeddings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL UNIQUE,
    embedding_id TEXT NOT NULL,
    pxi_score REAL NOT NULL,
    forward_return_7d REAL,
    forward_return_30d REAL,
    regime_label TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_market_embeddings_date ON market_embeddings(date DESC);
