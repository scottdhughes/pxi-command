-- Separate live/prospective observations from retrospective reconstructions.
-- Existing rows are deliberately unclassified unless a successful production
-- cron run proves that the score was recorded live on the same as-of date.

ALTER TABLE pxi_scores
ADD COLUMN history_origin TEXT NOT NULL DEFAULT 'legacy_unclassified'
CHECK(history_origin IN ('legacy_unclassified', 'live_recorded'));

ALTER TABLE category_scores
ADD COLUMN history_origin TEXT NOT NULL DEFAULT 'legacy_unclassified'
CHECK(history_origin IN ('legacy_unclassified', 'live_recorded'));

UPDATE pxi_scores
SET history_origin = 'live_recorded'
WHERE EXISTS (
    SELECT 1
    FROM market_refresh_runs refresh
    WHERE refresh.status = 'success'
      AND refresh."trigger" = 'cron_fast_pipeline'
      AND substr(COALESCE(refresh.as_of, ''), 1, 10) = pxi_scores.date
);

UPDATE category_scores
SET history_origin = 'live_recorded'
WHERE EXISTS (
    SELECT 1
    FROM market_refresh_runs refresh
    WHERE refresh.status = 'success'
      AND refresh."trigger" = 'cron_fast_pipeline'
      AND substr(COALESCE(refresh.as_of, ''), 1, 10) = category_scores.date
);

CREATE INDEX IF NOT EXISTS idx_pxi_scores_origin_date
ON pxi_scores(history_origin, date DESC);

CREATE INDEX IF NOT EXISTS idx_category_scores_origin_date
ON category_scores(history_origin, date DESC);

-- Retrospective rows live in dedicated, immutable audit tables. No existing
-- prospective, research, prediction, evidence, or product query can consume
-- them accidentally; /api/history is the sole read path that opts into them.
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
