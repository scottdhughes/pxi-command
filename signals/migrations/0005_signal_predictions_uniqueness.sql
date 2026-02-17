-- Canonicalize historical same-day duplicate predictions and enforce logical uniqueness.
-- Logical key: (signal_date, theme_id)

-- 1) Safety backup before cleanup.
CREATE TABLE IF NOT EXISTS signal_predictions_backup_0005 AS
SELECT * FROM signal_predictions;

-- 2) Remove duplicate logical rows, keeping the earliest (created_at, id).
WITH ranked AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY signal_date, theme_id
      ORDER BY created_at ASC, id ASC
    ) AS rn
  FROM signal_predictions
)
DELETE FROM signal_predictions
WHERE id IN (
  SELECT id
  FROM ranked
  WHERE rn > 1
);

-- 3) Enforce DB-level uniqueness for future inserts/reruns.
CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_predictions_signal_theme_unique
ON signal_predictions(signal_date, theme_id);
