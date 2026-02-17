-- Anchor prediction evaluation exits to target-date historical closes.
-- Adds auditability fields for resolved exit price date and any unresolved-data notes.

ALTER TABLE signal_predictions
ADD COLUMN exit_price_date TEXT;

ALTER TABLE signal_predictions
ADD COLUMN evaluation_note TEXT;

CREATE INDEX IF NOT EXISTS idx_signal_predictions_exit_price_date
ON signal_predictions(exit_price_date)
WHERE evaluated_at IS NOT NULL;
