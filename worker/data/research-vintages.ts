export const RESEARCH_FEATURE_VERSION = 'pxi-feature-snapshot/v1';
export const RESEARCH_STORAGE_CONTRACT = 'append-only-d1-research-snapshots/v1';

export interface ResearchSnapshotPxiInput {
  date: string;
  score: number;
  delta_1d: number | null;
  delta_7d: number | null;
  delta_30d: number | null;
}

interface IndicatorVintageRow {
  indicator_id: string;
  observation_date: string;
  value: number;
  source: string;
}

interface CategoryVintageRow {
  category: string;
  score: number;
}

export interface StoredResearchSnapshot {
  snapshot_id: string;
  decision_date: string;
  available_at: string;
  feature_version: string;
  storage_contract: string;
  capture_source: string;
  benchmark_close: number;
  benchmark_observation_date: string;
  features: Record<string, number>;
  feature_observation_dates: Record<string, string>;
  feature_sources: Record<string, string>;
}

function safeFeatureName(prefix: string, rawName: string): string {
  const normalized = rawName.trim().toLowerCase().replace(/[^a-z0-9_]+/g, '_').replace(/^_+|_+$/g, '');
  return `${prefix}_${normalized || 'unknown'}`;
}

function addFeature(
  payload: Pick<StoredResearchSnapshot, 'features' | 'feature_observation_dates' | 'feature_sources'>,
  name: string,
  value: number | null,
  observationDate: string,
  source: string,
): void {
  if (value === null || !Number.isFinite(value)) return;
  payload.features[name] = value;
  payload.feature_observation_dates[name] = observationDate;
  payload.feature_sources[name] = source;
}

export async function ensureResearchSnapshotSchema(db: D1Database): Promise<void> {
  await db.prepare(`
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
    )
  `).run();
  await db.prepare(`
    CREATE INDEX IF NOT EXISTS idx_research_feature_snapshots_decision
    ON research_feature_snapshots(decision_date, available_at DESC)
  `).run();
  await db.prepare(`
    CREATE INDEX IF NOT EXISTS idx_research_feature_snapshots_available
    ON research_feature_snapshots(available_at DESC)
  `).run();
  await db.prepare(`
    CREATE TRIGGER IF NOT EXISTS research_feature_snapshots_no_update
    BEFORE UPDATE ON research_feature_snapshots
    BEGIN
      SELECT RAISE(ABORT, 'research feature snapshots are immutable');
    END
  `).run();
  await db.prepare(`
    CREATE TRIGGER IF NOT EXISTS research_feature_snapshots_no_delete
    BEFORE DELETE ON research_feature_snapshots
    BEGIN
      SELECT RAISE(ABORT, 'research feature snapshots are immutable');
    END
  `).run();
}

export async function captureResearchFeatureSnapshot(
  db: D1Database,
  pxi: ResearchSnapshotPxiInput,
  captureSource: string,
  availableAt = new Date().toISOString(),
): Promise<StoredResearchSnapshot | null> {
  await ensureResearchSnapshotSchema(db);

  const [indicatorResult, categoryResult] = await Promise.all([
    db.prepare(`
      SELECT
        iv.indicator_id,
        iv.date AS observation_date,
        iv.value,
        iv.source
      FROM indicator_values iv
      INNER JOIN (
        SELECT indicator_id, MAX(date) AS max_date
        FROM indicator_values
        WHERE date <= ?
        GROUP BY indicator_id
      ) latest
        ON latest.indicator_id = iv.indicator_id
       AND latest.max_date = iv.date
      ORDER BY iv.indicator_id
    `).bind(pxi.date).all<IndicatorVintageRow>(),
    db.prepare(`
      SELECT category, score
      FROM category_scores
      WHERE date = ?
      ORDER BY category
    `).bind(pxi.date).all<CategoryVintageRow>(),
  ]);

  const indicators = indicatorResult.results || [];
  const benchmark = indicators.find((row) => row.indicator_id === 'spy_close');
  if (!benchmark || !Number.isFinite(benchmark.value) || benchmark.value <= 0) {
    return null;
  }

  const snapshotId = crypto.randomUUID();
  const payload: StoredResearchSnapshot = {
    snapshot_id: snapshotId,
    decision_date: pxi.date,
    available_at: availableAt,
    feature_version: RESEARCH_FEATURE_VERSION,
    storage_contract: RESEARCH_STORAGE_CONTRACT,
    capture_source: captureSource,
    benchmark_close: benchmark.value,
    benchmark_observation_date: benchmark.observation_date,
    features: {},
    feature_observation_dates: {},
    feature_sources: {},
  };

  addFeature(payload, 'pxi_score', pxi.score, pxi.date, 'pxi-calculation');
  addFeature(payload, 'pxi_delta_1d', pxi.delta_1d, pxi.date, 'pxi-calculation');
  addFeature(payload, 'pxi_delta_7d', pxi.delta_7d, pxi.date, 'pxi-calculation');
  addFeature(payload, 'pxi_delta_30d', pxi.delta_30d, pxi.date, 'pxi-calculation');

  for (const category of categoryResult.results || []) {
    addFeature(
      payload,
      safeFeatureName('category', category.category),
      category.score,
      pxi.date,
      'pxi-category-calculation',
    );
  }

  for (const indicator of indicators) {
    if (indicator.indicator_id === 'spy_close') continue;
    addFeature(
      payload,
      safeFeatureName('indicator', indicator.indicator_id),
      indicator.value,
      indicator.observation_date,
      indicator.source,
    );
  }

  await db.prepare(`
    INSERT INTO research_feature_snapshots (
      snapshot_id,
      decision_date,
      available_at,
      feature_version,
      storage_contract,
      capture_source,
      benchmark_close,
      benchmark_observation_date,
      payload_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).bind(
    snapshotId,
    pxi.date,
    availableAt,
    RESEARCH_FEATURE_VERSION,
    RESEARCH_STORAGE_CONTRACT,
    captureSource,
    benchmark.value,
    benchmark.observation_date,
    JSON.stringify(payload),
  ).run();

  return payload;
}
