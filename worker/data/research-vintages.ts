export const RESEARCH_FEATURE_VERSION = 'pxi-feature-snapshot/v1';
export const RESEARCH_STORAGE_CONTRACT = 'append-only-d1-research-snapshots/v1';
export const DAILY_CLOSE_CANONICAL_SLOT = 'daily_close_22z';

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
  canonical_slot: string | null;
  benchmark_close: number;
  benchmark_observation_date: string;
  features: Record<string, number>;
  feature_observation_dates: Record<string, string>;
  feature_sources: Record<string, string>;
}

export interface ResearchSnapshotCaptureOptions {
  canonicalSlot?: typeof DAILY_CLOSE_CANONICAL_SLOT | null;
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

function newYorkDateKey(value: string): string | null {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date);
  const year = parts.find((part) => part.type === 'year')?.value;
  const month = parts.find((part) => part.type === 'month')?.value;
  const day = parts.find((part) => part.type === 'day')?.value;
  return year && month && day ? `${year}-${month}-${day}` : null;
}

export function isDailyCloseCanonicalWindow(value: string): boolean {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return false;
  const utcHour = date.getUTCHours();
  return utcHour >= 22 || utcHour <= 4;
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
      canonical_slot TEXT CHECK (canonical_slot IS NULL OR canonical_slot = 'daily_close_22z'),
      benchmark_close REAL NOT NULL,
      benchmark_observation_date TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      CHECK (canonical_slot IS NULL OR benchmark_observation_date = decision_date)
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
    CREATE UNIQUE INDEX IF NOT EXISTS idx_research_feature_snapshots_canonical_slot
    ON research_feature_snapshots(
      decision_date,
      feature_version,
      storage_contract,
      canonical_slot
    )
    WHERE canonical_slot IS NOT NULL
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
  options: ResearchSnapshotCaptureOptions = {},
): Promise<StoredResearchSnapshot | null> {
  const canonicalSlot = options.canonicalSlot ?? null;
  if (canonicalSlot !== null && canonicalSlot !== DAILY_CLOSE_CANONICAL_SLOT) return null;
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
  if (canonicalSlot === DAILY_CLOSE_CANONICAL_SLOT && benchmark.observation_date !== pxi.date) {
    return null;
  }
  if (canonicalSlot === DAILY_CLOSE_CANONICAL_SLOT && newYorkDateKey(availableAt) !== pxi.date) {
    return null;
  }
  if (canonicalSlot === DAILY_CLOSE_CANONICAL_SLOT && !isDailyCloseCanonicalWindow(availableAt)) {
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
    canonical_slot: canonicalSlot,
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

  const insertSql = `
    INSERT INTO research_feature_snapshots (
      snapshot_id,
      decision_date,
      available_at,
      feature_version,
      storage_contract,
      capture_source,
      canonical_slot,
      benchmark_close,
      benchmark_observation_date,
      payload_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ${canonicalSlot === null ? '' : `
      ON CONFLICT(decision_date, feature_version, storage_contract, canonical_slot)
      WHERE canonical_slot IS NOT NULL
      DO NOTHING
    `}
  `;
  const insertResult = await db.prepare(insertSql).bind(
    snapshotId,
    pxi.date,
    availableAt,
    RESEARCH_FEATURE_VERSION,
    RESEARCH_STORAGE_CONTRACT,
    captureSource,
    canonicalSlot,
    benchmark.value,
    benchmark.observation_date,
    JSON.stringify(payload),
  ).run();

  if (canonicalSlot !== null && insertResult.meta?.changes === 0) {
    const existing = await db.prepare(`
      SELECT payload_json
      FROM research_feature_snapshots
      WHERE decision_date = ?
        AND feature_version = ?
        AND storage_contract = ?
        AND canonical_slot = ?
      LIMIT 1
    `).bind(
      pxi.date,
      RESEARCH_FEATURE_VERSION,
      RESEARCH_STORAGE_CONTRACT,
      canonicalSlot,
    ).first<{ payload_json: string }>();
    if (!existing?.payload_json) return null;
    try {
      return JSON.parse(existing.payload_json) as StoredResearchSnapshot;
    } catch {
      return null;
    }
  }

  return payload;
}
