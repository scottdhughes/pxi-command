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
    lookback_days INTEGER DEFAULT 1260,
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

-- ============================================
-- PXI v1.1 Schema Extensions
-- ============================================

-- PXI Signal layer (trading/risk allocation)
CREATE TABLE IF NOT EXISTS pxi_signal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL UNIQUE,
    pxi_level REAL NOT NULL,
    delta_pxi_7d REAL,
    delta_pxi_30d REAL,
    category_dispersion REAL,
    regime TEXT NOT NULL,
    volatility_percentile REAL,
    risk_allocation REAL NOT NULL,
    signal_type TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_pxi_signal_date ON pxi_signal(date DESC);
CREATE INDEX IF NOT EXISTS idx_pxi_signal_regime ON pxi_signal(regime);
CREATE INDEX IF NOT EXISTS idx_pxi_signal_type ON pxi_signal(signal_type);

-- Alert history with performance metrics
CREATE TABLE IF NOT EXISTS alert_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_date TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    pxi_at_alert REAL,
    historical_frequency REAL,
    median_return_7d REAL,
    median_return_30d REAL,
    false_positive_rate REAL,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(alert_date, alert_type)
);

CREATE INDEX IF NOT EXISTS idx_alert_history_date ON alert_history(alert_date DESC);
CREATE INDEX IF NOT EXISTS idx_alert_history_type ON alert_history(alert_type);

-- Backtest results for validation
CREATE TABLE IF NOT EXISTS backtest_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT NOT NULL,
    strategy TEXT NOT NULL,
    lookback_start TEXT NOT NULL,
    lookback_end TEXT NOT NULL,
    cagr REAL,
    volatility REAL,
    sharpe REAL,
    max_drawdown REAL,
    total_trades INTEGER,
    win_rate REAL,
    baseline_comparison TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(run_date, strategy)
);

CREATE INDEX IF NOT EXISTS idx_backtest_results_date ON backtest_results(run_date DESC);
CREATE INDEX IF NOT EXISTS idx_backtest_results_strategy ON backtest_results(strategy);

-- ============================================
-- ML/Prediction Tracking Tables
-- ============================================

-- Prediction log for tracking predictions vs actual outcomes
CREATE TABLE IF NOT EXISTS prediction_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prediction_date TEXT NOT NULL UNIQUE,
    target_date_7d TEXT,
    target_date_30d TEXT,
    current_score REAL NOT NULL,
    predicted_change_7d REAL,
    predicted_change_30d REAL,
    actual_change_7d REAL,
    actual_change_30d REAL,
    confidence_7d REAL,
    confidence_30d REAL,
    similar_periods TEXT,  -- JSON array of period dates used
    evaluated_at TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_prediction_log_date ON prediction_log(prediction_date DESC);
CREATE INDEX IF NOT EXISTS idx_prediction_log_evaluated ON prediction_log(evaluated_at);

-- Model parameters for tuning
CREATE TABLE IF NOT EXISTS model_params (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    param_key TEXT NOT NULL UNIQUE,
    param_value REAL NOT NULL,
    notes TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
);

-- Period accuracy tracking (which historical periods are good predictors)
CREATE TABLE IF NOT EXISTS period_accuracy (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    period_date TEXT NOT NULL UNIQUE,
    times_used INTEGER DEFAULT 0,
    correct_predictions INTEGER DEFAULT 0,
    total_predictions INTEGER DEFAULT 0,
    mean_absolute_error REAL,
    accuracy_score REAL,  -- 0-1 score based on prediction quality
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_period_accuracy_date ON period_accuracy(period_date DESC);
CREATE INDEX IF NOT EXISTS idx_period_accuracy_score ON period_accuracy(accuracy_score DESC);

-- ML Ensemble prediction tracking (XGBoost + LSTM)
CREATE TABLE IF NOT EXISTS ensemble_predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prediction_date TEXT NOT NULL UNIQUE,
    target_date_7d TEXT NOT NULL,
    target_date_30d TEXT NOT NULL,
    current_score REAL NOT NULL,
    -- XGBoost predictions
    xgboost_7d REAL,
    xgboost_30d REAL,
    -- LSTM predictions
    lstm_7d REAL,
    lstm_30d REAL,
    -- Ensemble (weighted average)
    ensemble_7d REAL,
    ensemble_30d REAL,
    -- Confidence based on model agreement
    confidence_7d TEXT,  -- HIGH, MEDIUM, LOW
    confidence_30d TEXT,
    -- Actual outcomes (filled in when target date arrives)
    actual_change_7d REAL,
    actual_change_30d REAL,
    -- Evaluation metadata
    direction_correct_7d INTEGER,  -- 1 = correct, 0 = wrong, NULL = not evaluated
    direction_correct_30d INTEGER,
    evaluated_at TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_ensemble_predictions_date ON ensemble_predictions(prediction_date DESC);
CREATE INDEX IF NOT EXISTS idx_ensemble_predictions_target7d ON ensemble_predictions(target_date_7d);
CREATE INDEX IF NOT EXISTS idx_ensemble_predictions_target30d ON ensemble_predictions(target_date_30d);

-- ============================================
-- PXI Product Layer Tables (Brief/Opportunities/Alerts)
-- ============================================

CREATE TABLE IF NOT EXISTS email_subscribers (
    id TEXT PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'active', 'unsubscribed', 'bounced')),
    cadence TEXT NOT NULL DEFAULT 'daily_8am_et',
    types_json TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'America/New_York',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_email_subscribers_status ON email_subscribers(status);
CREATE INDEX IF NOT EXISTS idx_email_subscribers_updated ON email_subscribers(updated_at DESC);

CREATE TABLE IF NOT EXISTS email_verification_tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    used_at TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_email_verification_email_expires ON email_verification_tokens(email, expires_at DESC);
CREATE INDEX IF NOT EXISTS idx_email_verification_hash ON email_verification_tokens(token_hash);

CREATE TABLE IF NOT EXISTS email_unsubscribe_tokens (
    subscriber_id TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_email_unsubscribe_hash ON email_unsubscribe_tokens(token_hash);

CREATE TABLE IF NOT EXISTS market_brief_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_market_brief_as_of ON market_brief_snapshots(as_of DESC);

CREATE TABLE IF NOT EXISTS opportunity_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of TEXT NOT NULL,
    horizon TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(as_of, horizon)
);

CREATE INDEX IF NOT EXISTS idx_opportunity_snapshots_lookup ON opportunity_snapshots(as_of DESC, horizon);

CREATE TABLE IF NOT EXISTS market_calibration_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of TEXT NOT NULL,
    metric TEXT NOT NULL,
    horizon TEXT,
    payload_json TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(as_of, metric, horizon)
);

CREATE INDEX IF NOT EXISTS idx_market_calibration_lookup ON market_calibration_snapshots(metric, horizon, as_of DESC);

CREATE TABLE IF NOT EXISTS market_alert_events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL CHECK(severity IN ('info', 'warning', 'critical')),
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    entity_type TEXT NOT NULL CHECK(entity_type IN ('market', 'theme', 'indicator')),
    entity_id TEXT,
    dedupe_key TEXT NOT NULL UNIQUE,
    payload_json TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_market_alert_events_created ON market_alert_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_alert_events_type ON market_alert_events(event_type, created_at DESC);

CREATE TABLE IF NOT EXISTS market_alert_deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    channel TEXT NOT NULL CHECK(channel IN ('in_app', 'email')),
    subscriber_id TEXT,
    status TEXT NOT NULL CHECK(status IN ('queued', 'sent', 'failed')),
    provider_id TEXT,
    error TEXT,
    attempted_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_market_alert_deliveries_event ON market_alert_deliveries(event_id, attempted_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_alert_deliveries_subscriber ON market_alert_deliveries(subscriber_id, attempted_at DESC);
