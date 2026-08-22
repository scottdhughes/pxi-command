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

-- Immutable feature vectors captured at the production decision boundary.
CREATE TABLE IF NOT EXISTS research_feature_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id TEXT NOT NULL UNIQUE,
    decision_date TEXT NOT NULL,
    available_at TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    storage_contract TEXT NOT NULL,
    capture_source TEXT NOT NULL,
    canonical_slot TEXT CHECK (canonical_slot IS NULL OR canonical_slot = 'daily_close_22z'),
    benchmark_close REAL NOT NULL,
    benchmark_observation_date TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (canonical_slot IS NULL OR benchmark_observation_date = decision_date)
);

CREATE INDEX IF NOT EXISTS idx_research_feature_snapshots_decision
ON research_feature_snapshots(decision_date, available_at DESC);
CREATE INDEX IF NOT EXISTS idx_research_feature_snapshots_available
ON research_feature_snapshots(available_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_research_feature_snapshots_canonical_slot
ON research_feature_snapshots(
    decision_date,
    feature_version,
    storage_contract,
    canonical_slot
)
WHERE canonical_slot IS NOT NULL;
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

-- Immutable daily market predictions. Only outcome fields may be filled later.
CREATE TABLE IF NOT EXISTS market_prediction_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id TEXT NOT NULL UNIQUE,
    prediction_date TEXT NOT NULL,
    prediction_available_at TEXT NOT NULL,
    canonical_slot TEXT NOT NULL CHECK (canonical_slot = 'daily_close_22z'),
    feature_snapshot_id TEXT NOT NULL REFERENCES research_feature_snapshots(snapshot_id),
    model_family TEXT NOT NULL,
    model_version TEXT NOT NULL,
    target_metric TEXT NOT NULL CHECK (target_metric = 'spy_return_pct'),
    current_pxi_score REAL NOT NULL CHECK (current_pxi_score >= 0 AND current_pxi_score <= 100),
    pxi_bucket TEXT NOT NULL,
    bucket_lower REAL NOT NULL,
    bucket_upper REAL NOT NULL,
    benchmark_close REAL NOT NULL CHECK (benchmark_close > 0),
    benchmark_observation_date TEXT NOT NULL,
    target_date_7d TEXT NOT NULL,
    target_date_30d TEXT NOT NULL,
    predicted_return_7d REAL,
    predicted_return_30d REAL,
    median_return_7d REAL,
    median_return_30d REAL,
    win_rate_7d REAL CHECK (win_rate_7d IS NULL OR (win_rate_7d >= 0 AND win_rate_7d <= 1)),
    win_rate_30d REAL CHECK (win_rate_30d IS NULL OR (win_rate_30d >= 0 AND win_rate_30d <= 1)),
    ci95_low_7d REAL,
    ci95_high_7d REAL,
    ci95_low_30d REAL,
    ci95_high_30d REAL,
    sample_size_7d INTEGER NOT NULL CHECK (sample_size_7d >= 0),
    sample_size_30d INTEGER NOT NULL CHECK (sample_size_30d >= 0),
    training_cutoff_date TEXT,
    methodology_json TEXT NOT NULL,
    outcome_status_7d TEXT NOT NULL DEFAULT 'pending' CHECK (outcome_status_7d IN ('pending', 'observed', 'unavailable')),
    outcome_status_30d TEXT NOT NULL DEFAULT 'pending' CHECK (outcome_status_30d IN ('pending', 'observed', 'unavailable')),
    outcome_unavailable_reason_7d TEXT,
    outcome_unavailable_reason_30d TEXT,
    actual_return_7d REAL,
    actual_return_30d REAL,
    actual_observation_date_7d TEXT,
    actual_observation_date_30d TEXT,
    evaluated_at_7d TEXT,
    evaluated_at_30d TEXT,
    evaluated_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (prediction_date, canonical_slot, model_version),
    CHECK (benchmark_observation_date = prediction_date),
    CHECK (target_date_7d = date(prediction_date, '+7 days')),
    CHECK (target_date_30d = date(prediction_date, '+30 days')),
    CHECK (
      (outcome_status_7d = 'pending' AND actual_return_7d IS NULL AND actual_observation_date_7d IS NULL AND evaluated_at_7d IS NULL AND outcome_unavailable_reason_7d IS NULL) OR
      (outcome_status_7d = 'observed' AND actual_return_7d IS NOT NULL AND actual_observation_date_7d IS NOT NULL AND evaluated_at_7d IS NOT NULL AND outcome_unavailable_reason_7d IS NULL) OR
      (outcome_status_7d = 'unavailable' AND actual_return_7d IS NULL AND actual_observation_date_7d IS NULL AND evaluated_at_7d IS NOT NULL AND outcome_unavailable_reason_7d IS NOT NULL)
    ),
    CHECK (
      (outcome_status_30d = 'pending' AND actual_return_30d IS NULL AND actual_observation_date_30d IS NULL AND evaluated_at_30d IS NULL AND outcome_unavailable_reason_30d IS NULL) OR
      (outcome_status_30d = 'observed' AND actual_return_30d IS NOT NULL AND actual_observation_date_30d IS NOT NULL AND evaluated_at_30d IS NOT NULL AND outcome_unavailable_reason_30d IS NULL) OR
      (outcome_status_30d = 'unavailable' AND actual_return_30d IS NULL AND actual_observation_date_30d IS NULL AND evaluated_at_30d IS NOT NULL AND outcome_unavailable_reason_30d IS NOT NULL)
    ),
    CHECK (evaluated_at IS NULL OR (outcome_status_7d != 'pending' AND outcome_status_30d != 'pending'))
);

CREATE INDEX IF NOT EXISTS idx_market_prediction_evidence_date
ON market_prediction_evidence(prediction_date DESC);
CREATE INDEX IF NOT EXISTS idx_market_prediction_evidence_model_date
ON market_prediction_evidence(canonical_slot, model_version, prediction_date DESC);
CREATE INDEX IF NOT EXISTS idx_market_prediction_evidence_evaluation
ON market_prediction_evidence(evaluated_at, prediction_date);

CREATE TRIGGER IF NOT EXISTS market_prediction_evidence_prediction_immutable
BEFORE UPDATE ON market_prediction_evidence
WHEN
    NEW.id IS NOT OLD.id OR
    NEW.evidence_id IS NOT OLD.evidence_id OR
    NEW.prediction_date IS NOT OLD.prediction_date OR
    NEW.prediction_available_at IS NOT OLD.prediction_available_at OR
    NEW.canonical_slot IS NOT OLD.canonical_slot OR
    NEW.feature_snapshot_id IS NOT OLD.feature_snapshot_id OR
    NEW.model_family IS NOT OLD.model_family OR
    NEW.model_version IS NOT OLD.model_version OR
    NEW.target_metric IS NOT OLD.target_metric OR
    NEW.current_pxi_score IS NOT OLD.current_pxi_score OR
    NEW.pxi_bucket IS NOT OLD.pxi_bucket OR
    NEW.bucket_lower IS NOT OLD.bucket_lower OR
    NEW.bucket_upper IS NOT OLD.bucket_upper OR
    NEW.benchmark_close IS NOT OLD.benchmark_close OR
    NEW.benchmark_observation_date IS NOT OLD.benchmark_observation_date OR
    NEW.target_date_7d IS NOT OLD.target_date_7d OR
    NEW.target_date_30d IS NOT OLD.target_date_30d OR
    NEW.predicted_return_7d IS NOT OLD.predicted_return_7d OR
    NEW.predicted_return_30d IS NOT OLD.predicted_return_30d OR
    NEW.median_return_7d IS NOT OLD.median_return_7d OR
    NEW.median_return_30d IS NOT OLD.median_return_30d OR
    NEW.win_rate_7d IS NOT OLD.win_rate_7d OR
    NEW.win_rate_30d IS NOT OLD.win_rate_30d OR
    NEW.ci95_low_7d IS NOT OLD.ci95_low_7d OR
    NEW.ci95_high_7d IS NOT OLD.ci95_high_7d OR
    NEW.ci95_low_30d IS NOT OLD.ci95_low_30d OR
    NEW.ci95_high_30d IS NOT OLD.ci95_high_30d OR
    NEW.sample_size_7d IS NOT OLD.sample_size_7d OR
    NEW.sample_size_30d IS NOT OLD.sample_size_30d OR
    NEW.training_cutoff_date IS NOT OLD.training_cutoff_date OR
    NEW.methodology_json IS NOT OLD.methodology_json OR
    NEW.created_at IS NOT OLD.created_at
BEGIN
    SELECT RAISE(ABORT, 'market prediction evidence forecast is immutable');
END;

CREATE TRIGGER IF NOT EXISTS market_prediction_evidence_outcomes_write_once
BEFORE UPDATE ON market_prediction_evidence
WHEN
    (OLD.actual_return_7d IS NOT NULL AND NEW.actual_return_7d IS NOT OLD.actual_return_7d) OR
    (OLD.actual_return_30d IS NOT NULL AND NEW.actual_return_30d IS NOT OLD.actual_return_30d) OR
    (OLD.actual_observation_date_7d IS NOT NULL AND NEW.actual_observation_date_7d IS NOT OLD.actual_observation_date_7d) OR
    (OLD.actual_observation_date_30d IS NOT NULL AND NEW.actual_observation_date_30d IS NOT OLD.actual_observation_date_30d) OR
    (OLD.evaluated_at_7d IS NOT NULL AND NEW.evaluated_at_7d IS NOT OLD.evaluated_at_7d) OR
    (OLD.evaluated_at_30d IS NOT NULL AND NEW.evaluated_at_30d IS NOT OLD.evaluated_at_30d) OR
    (OLD.outcome_status_7d != 'pending' AND NEW.outcome_status_7d IS NOT OLD.outcome_status_7d) OR
    (OLD.outcome_status_30d != 'pending' AND NEW.outcome_status_30d IS NOT OLD.outcome_status_30d) OR
    (OLD.outcome_unavailable_reason_7d IS NOT NULL AND NEW.outcome_unavailable_reason_7d IS NOT OLD.outcome_unavailable_reason_7d) OR
    (OLD.outcome_unavailable_reason_30d IS NOT NULL AND NEW.outcome_unavailable_reason_30d IS NOT OLD.outcome_unavailable_reason_30d) OR
    (OLD.evaluated_at IS NOT NULL AND NEW.evaluated_at IS NOT OLD.evaluated_at)
BEGIN
    SELECT RAISE(ABORT, 'market prediction evidence outcomes are write-once');
END;

CREATE TRIGGER IF NOT EXISTS market_prediction_evidence_no_delete
BEFORE DELETE ON market_prediction_evidence
BEGIN
    SELECT RAISE(ABORT, 'market prediction evidence is append-only');
END;

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
    history_origin TEXT NOT NULL DEFAULT 'legacy_unclassified'
        CHECK(history_origin IN ('legacy_unclassified', 'live_recorded')),
    calculated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(category, date)
);

CREATE INDEX IF NOT EXISTS idx_category_scores_date ON category_scores(date DESC);
CREATE INDEX IF NOT EXISTS idx_category_scores_origin_date
ON category_scores(history_origin, date DESC);

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
    history_origin TEXT NOT NULL DEFAULT 'legacy_unclassified'
        CHECK(history_origin IN ('legacy_unclassified', 'live_recorded')),
    calculated_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_pxi_scores_date ON pxi_scores(date DESC);
CREATE INDEX IF NOT EXISTS idx_pxi_scores_origin_date
ON pxi_scores(history_origin, date DESC);

-- Retrospective score reconstructions are physically isolated from the live
-- score tables. Production, research, prediction, and market-product queries
-- therefore fail closed unless they explicitly opt into these audit tables.
CREATE TABLE IF NOT EXISTS pxi_score_reconstructions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL UNIQUE,
    score REAL NOT NULL,
    label TEXT NOT NULL,
    status TEXT NOT NULL,
    delta_1d REAL,
    delta_7d REAL,
    delta_30d REAL,
    history_origin TEXT NOT NULL DEFAULT 'retrospective_reconstruction'
        CHECK(history_origin = 'retrospective_reconstruction'),
    reconstructed_at TEXT NOT NULL CHECK(length(trim(reconstructed_at)) > 0),
    reconstruction_method TEXT NOT NULL CHECK(length(trim(reconstruction_method)) > 0),
    reconstruction_build_sha TEXT NOT NULL CHECK(length(trim(reconstruction_build_sha)) > 0),
    source_data_as_of TEXT NOT NULL CHECK(length(trim(source_data_as_of)) > 0),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_pxi_score_reconstructions_date
ON pxi_score_reconstructions(date DESC);

CREATE TABLE IF NOT EXISTS category_score_reconstructions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT NOT NULL CHECK(category IN (
        'breadth', 'credit', 'crypto', 'global', 'macro', 'positioning', 'volatility'
    )),
    date TEXT NOT NULL,
    score REAL NOT NULL,
    weight REAL NOT NULL,
    weighted_score REAL NOT NULL,
    history_origin TEXT NOT NULL DEFAULT 'retrospective_reconstruction'
        CHECK(history_origin = 'retrospective_reconstruction'),
    reconstructed_at TEXT NOT NULL CHECK(length(trim(reconstructed_at)) > 0),
    reconstruction_method TEXT NOT NULL CHECK(length(trim(reconstruction_method)) > 0),
    reconstruction_build_sha TEXT NOT NULL CHECK(length(trim(reconstruction_build_sha)) > 0),
    source_data_as_of TEXT NOT NULL CHECK(length(trim(source_data_as_of)) > 0),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(category, date)
);

CREATE INDEX IF NOT EXISTS idx_category_score_reconstructions_date
ON category_score_reconstructions(date DESC);

CREATE TRIGGER IF NOT EXISTS pxi_scores_no_reconstruction_insert
BEFORE INSERT ON pxi_scores
WHEN EXISTS (SELECT 1 FROM pxi_score_reconstructions reconstruction WHERE reconstruction.date = NEW.date)
  OR EXISTS (SELECT 1 FROM category_score_reconstructions reconstruction WHERE reconstruction.date = NEW.date)
BEGIN
    SELECT RAISE(ABORT, 'live PXI history cannot supersede a retrospective reconstruction');
END;

CREATE TRIGGER IF NOT EXISTS pxi_scores_no_reconstruction_update
BEFORE UPDATE ON pxi_scores
WHEN EXISTS (SELECT 1 FROM pxi_score_reconstructions reconstruction WHERE reconstruction.date = NEW.date)
  OR EXISTS (SELECT 1 FROM category_score_reconstructions reconstruction WHERE reconstruction.date = NEW.date)
BEGIN
    SELECT RAISE(ABORT, 'live PXI history cannot supersede a retrospective reconstruction');
END;

CREATE TRIGGER IF NOT EXISTS category_scores_no_reconstruction_insert
BEFORE INSERT ON category_scores
WHEN EXISTS (SELECT 1 FROM pxi_score_reconstructions reconstruction WHERE reconstruction.date = NEW.date)
  OR EXISTS (SELECT 1 FROM category_score_reconstructions reconstruction WHERE reconstruction.date = NEW.date)
BEGIN
    SELECT RAISE(ABORT, 'live category history cannot supersede a retrospective reconstruction');
END;

CREATE TRIGGER IF NOT EXISTS category_scores_no_reconstruction_update
BEFORE UPDATE ON category_scores
WHEN EXISTS (SELECT 1 FROM pxi_score_reconstructions reconstruction WHERE reconstruction.date = NEW.date)
  OR EXISTS (SELECT 1 FROM category_score_reconstructions reconstruction WHERE reconstruction.date = NEW.date)
BEGIN
    SELECT RAISE(ABORT, 'live category history cannot supersede a retrospective reconstruction');
END;

CREATE TRIGGER IF NOT EXISTS pxi_score_reconstructions_missing_only
BEFORE INSERT ON pxi_score_reconstructions
WHEN EXISTS (SELECT 1 FROM pxi_score_reconstructions existing WHERE existing.date = NEW.date)
  OR EXISTS (SELECT 1 FROM pxi_scores live WHERE live.date = NEW.date)
  OR EXISTS (SELECT 1 FROM category_scores live WHERE live.date = NEW.date)
  OR (
    SELECT COUNT(*)
    FROM category_score_reconstructions category
    WHERE category.date = NEW.date
      AND category.history_origin = NEW.history_origin
      AND category.reconstructed_at = NEW.reconstructed_at
      AND category.reconstruction_method = NEW.reconstruction_method
      AND category.reconstruction_build_sha = NEW.reconstruction_build_sha
      AND category.source_data_as_of = NEW.source_data_as_of
  ) <> 7
BEGIN
    SELECT RAISE(ABORT, 'retrospective PXI reconstruction requires a new, complete canonical category aggregate');
END;

CREATE TRIGGER IF NOT EXISTS category_score_reconstructions_missing_only
BEFORE INSERT ON category_score_reconstructions
WHEN EXISTS (
    SELECT 1 FROM category_score_reconstructions existing
    WHERE existing.category = NEW.category AND existing.date = NEW.date
  )
  OR EXISTS (SELECT 1 FROM pxi_score_reconstructions existing WHERE existing.date = NEW.date)
  OR EXISTS (
    SELECT 1
    FROM category_score_reconstructions existing
    WHERE existing.date = NEW.date
      AND (
        existing.history_origin <> NEW.history_origin
        OR existing.reconstructed_at <> NEW.reconstructed_at
        OR existing.reconstruction_method <> NEW.reconstruction_method
        OR existing.reconstruction_build_sha <> NEW.reconstruction_build_sha
        OR existing.source_data_as_of <> NEW.source_data_as_of
      )
  )
  OR EXISTS (SELECT 1 FROM pxi_scores live WHERE live.date = NEW.date)
  OR EXISTS (SELECT 1 FROM category_scores live WHERE live.date = NEW.date)
BEGIN
    SELECT RAISE(ABORT, 'retrospective category reconstruction must be a new missing-only key');
END;

CREATE TRIGGER IF NOT EXISTS pxi_score_reconstructions_no_update
BEFORE UPDATE ON pxi_score_reconstructions
BEGIN
    SELECT RAISE(ABORT, 'retrospective PXI reconstructions are immutable');
END;

CREATE TRIGGER IF NOT EXISTS pxi_score_reconstructions_no_delete
BEFORE DELETE ON pxi_score_reconstructions
BEGIN
    SELECT RAISE(ABORT, 'retrospective PXI reconstructions are immutable');
END;

CREATE TRIGGER IF NOT EXISTS category_score_reconstructions_no_update
BEFORE UPDATE ON category_score_reconstructions
BEGIN
    SELECT RAISE(ABORT, 'retrospective category reconstructions are immutable');
END;

CREATE TRIGGER IF NOT EXISTS category_score_reconstructions_no_delete
BEFORE DELETE ON category_score_reconstructions
BEGIN
    SELECT RAISE(ABORT, 'retrospective category reconstructions are immutable');
END;

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
    contract_version TEXT NOT NULL DEFAULT '2026-02-17-v2',
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

CREATE TABLE IF NOT EXISTS market_opportunity_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    refresh_run_id INTEGER,
    as_of TEXT NOT NULL,
    horizon TEXT NOT NULL CHECK(horizon IN ('7d', '30d')),
    candidate_count INTEGER NOT NULL DEFAULT 0,
    published_count INTEGER NOT NULL DEFAULT 0,
    suppressed_count INTEGER NOT NULL DEFAULT 0,
    quality_filtered_count INTEGER NOT NULL DEFAULT 0,
    coherence_suppressed_count INTEGER NOT NULL DEFAULT 0,
    data_quality_suppressed_count INTEGER NOT NULL DEFAULT 0,
    degraded_reason TEXT,
    top_direction_candidate TEXT CHECK(top_direction_candidate IN ('bullish', 'bearish', 'neutral')),
    top_direction_published TEXT CHECK(top_direction_published IN ('bullish', 'bearish', 'neutral')),
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_created ON market_opportunity_ledger(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_as_of ON market_opportunity_ledger(as_of DESC, horizon);
CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_run ON market_opportunity_ledger(refresh_run_id, horizon);

CREATE TABLE IF NOT EXISTS market_opportunity_item_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    refresh_run_id INTEGER,
    as_of TEXT NOT NULL,
    horizon TEXT NOT NULL CHECK(horizon IN ('7d', '30d')),
    opportunity_id TEXT NOT NULL,
    theme_id TEXT NOT NULL,
    theme_name TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('bullish', 'bearish', 'neutral')),
    conviction_score INTEGER NOT NULL,
    published INTEGER NOT NULL,
    suppression_reason TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(as_of, horizon, opportunity_id)
);

CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_asof_horizon ON market_opportunity_item_ledger(as_of DESC, horizon);
CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_theme_horizon_asof ON market_opportunity_item_ledger(theme_id, horizon, as_of DESC);
CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_published_created ON market_opportunity_item_ledger(published, created_at DESC);

CREATE TABLE IF NOT EXISTS market_decision_impact_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of TEXT NOT NULL,
    horizon TEXT NOT NULL CHECK(horizon IN ('7d', '30d')),
    scope TEXT NOT NULL CHECK(scope IN ('market', 'theme')),
    window_days INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(as_of, horizon, scope, window_days)
);

CREATE INDEX IF NOT EXISTS idx_market_decision_impact_lookup ON market_decision_impact_snapshots(scope, horizon, window_days, as_of DESC);

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

CREATE TABLE IF NOT EXISTS market_consistency_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of TEXT NOT NULL UNIQUE,
    score REAL NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('PASS', 'WARN', 'FAIL')),
    violations_json TEXT NOT NULL,
    components_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_market_consistency_created ON market_consistency_checks(created_at DESC);

CREATE TABLE IF NOT EXISTS market_refresh_runs (
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

CREATE INDEX IF NOT EXISTS idx_market_refresh_runs_completed ON market_refresh_runs(status, completed_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_refresh_runs_created ON market_refresh_runs(created_at DESC);

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
        'market_backfill',
        'deploy'
    )),
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    lease_version INTEGER NOT NULL DEFAULT 1 CHECK(lease_version >= 1),
    updated_at TEXT NOT NULL,
    CHECK(expires_at > acquired_at)
);

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
CREATE UNIQUE INDEX IF NOT EXISTS idx_market_alert_deliveries_email_unique
ON market_alert_deliveries(event_id, channel, subscriber_id)
WHERE subscriber_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS market_utility_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    route TEXT,
    actionability_state TEXT,
    payload_json TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_market_utility_events_created ON market_utility_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_utility_events_type ON market_utility_events(event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_utility_events_session ON market_utility_events(session_id, created_at DESC);

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
