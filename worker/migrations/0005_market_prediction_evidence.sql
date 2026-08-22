DROP TRIGGER IF EXISTS research_feature_snapshots_no_update;
DROP TRIGGER IF EXISTS research_feature_snapshots_no_delete;

-- Historical backfills predate the prospective publication contract. Preserve
-- their rows for audit, but remove them from every decision-impact numerator and
-- clear derived snapshots that may have incorporated them.
UPDATE market_opportunity_item_ledger
SET published = 0,
    suppression_reason = 'historical_backfill_nonprospective'
WHERE refresh_run_id IS NULL
  AND published = 1;

UPDATE market_opportunity_ledger
SET suppressed_count = suppressed_count + published_count,
    published_count = 0,
    degraded_reason = 'historical_backfill_nonprospective',
    top_direction_published = NULL
WHERE refresh_run_id IS NULL
  AND published_count > 0;

DELETE FROM market_decision_impact_snapshots;

CREATE TABLE research_feature_snapshots_v2 (
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

INSERT INTO research_feature_snapshots_v2 (
    id,
    snapshot_id,
    decision_date,
    available_at,
    feature_version,
    storage_contract,
    capture_source,
    canonical_slot,
    benchmark_close,
    benchmark_observation_date,
    payload_json,
    created_at
)
SELECT
    id,
    snapshot_id,
    decision_date,
    available_at,
    feature_version,
    storage_contract,
    capture_source,
    NULL,
    benchmark_close,
    benchmark_observation_date,
    payload_json,
    created_at
FROM research_feature_snapshots;

DROP TABLE research_feature_snapshots;
ALTER TABLE research_feature_snapshots_v2 RENAME TO research_feature_snapshots;

CREATE INDEX idx_research_feature_snapshots_decision
ON research_feature_snapshots(decision_date, available_at DESC);

CREATE INDEX idx_research_feature_snapshots_available
ON research_feature_snapshots(available_at DESC);

CREATE UNIQUE INDEX idx_research_feature_snapshots_canonical_slot
ON research_feature_snapshots(
    decision_date,
    feature_version,
    storage_contract,
    canonical_slot
)
WHERE canonical_slot IS NOT NULL;

CREATE TRIGGER research_feature_snapshots_no_update
BEFORE UPDATE ON research_feature_snapshots
BEGIN
  SELECT RAISE(ABORT, 'research feature snapshots are immutable');
END;

CREATE TRIGGER research_feature_snapshots_no_delete
BEFORE DELETE ON research_feature_snapshots
BEGIN
  SELECT RAISE(ABORT, 'research feature snapshots are immutable');
END;

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
