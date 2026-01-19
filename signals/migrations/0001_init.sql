CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  created_at_utc TEXT NOT NULL,
  lookback_days INTEGER NOT NULL,
  baseline_days INTEGER NOT NULL,
  status TEXT NOT NULL,
  summary_json TEXT,
  report_html_key TEXT NOT NULL,
  results_json_key TEXT NOT NULL,
  raw_json_key TEXT,
  error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at_utc DESC);
