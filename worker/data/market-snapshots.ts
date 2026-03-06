import {
  calibrationQualityForSampleSize,
  clamp,
  parseIsoDate,
  toNumber,
} from '../lib/market-primitives';
import type {
  BriefSnapshot,
  CalibrationBinSnapshot,
  ConsistencySnapshot,
  ConsistencyState,
  DecisionImpactResponsePayload,
  MarketCalibrationSnapshotPayload,
  OpportunitySnapshot,
} from '../types';

const BRIEF_CONTRACT_VERSION = '2026-02-17-v2';

function parseCalibrationSnapshotPayload(raw: string | null | undefined): MarketCalibrationSnapshotPayload | null {
  if (!raw) return null;

  try {
    const parsed = JSON.parse(raw) as Partial<MarketCalibrationSnapshotPayload>;
    if (!parsed || typeof parsed !== 'object' || !Array.isArray(parsed.bins)) {
      return null;
    }

    const bins = parsed.bins
      .map((bin): CalibrationBinSnapshot | null => {
        if (!bin || typeof bin !== 'object') return null;
        const candidate = bin as Partial<CalibrationBinSnapshot>;
        if (typeof candidate.bin !== 'string') return null;

        const sampleSize = toNumber(candidate.sample_size, 0);
        const inferredCorrectCount = Math.round(
          sampleSize > 0 && candidate.probability_correct !== null && candidate.probability_correct !== undefined
            ? clamp(0, 1, toNumber(candidate.probability_correct, 0)) * sampleSize
            : 0,
        );
        const correctCount = Math.max(
          0,
          Math.min(
            Math.floor(sampleSize),
            Math.floor(toNumber(candidate.correct_count, inferredCorrectCount)),
          ),
        );
        const probability = candidate.probability_correct === null || candidate.probability_correct === undefined
          ? null
          : clamp(0, 1, toNumber(candidate.probability_correct, 0));
        const ci95Low = candidate.ci95_low === null || candidate.ci95_low === undefined
          ? null
          : clamp(0, 1, toNumber(candidate.ci95_low, 0));
        const ci95High = candidate.ci95_high === null || candidate.ci95_high === undefined
          ? null
          : clamp(0, 1, toNumber(candidate.ci95_high, 0));

        return {
          bin: candidate.bin,
          correct_count: correctCount,
          probability_correct: probability,
          ci95_low: ci95Low,
          ci95_high: ci95High,
          sample_size: Math.max(0, Math.floor(sampleSize)),
          quality: calibrationQualityForSampleSize(sampleSize),
        };
      })
      .filter((value): value is CalibrationBinSnapshot => Boolean(value));

    return {
      as_of: String(parsed.as_of || ''),
      metric: parsed.metric === 'conviction' ? 'conviction' : 'edge_quality',
      horizon: parsed.horizon === '30d' ? '30d' : parsed.horizon === '7d' ? '7d' : null,
      basis: parsed.basis === 'conviction_decile' ? 'conviction_decile' : 'edge_quality_decile',
      bins,
      total_samples: Math.max(0, Math.floor(toNumber(parsed.total_samples, 0))),
    };
  } catch {
    return null;
  }
}

export async function fetchLatestCalibrationSnapshot(
  db: D1Database,
  metric: 'edge_quality' | 'conviction',
  horizon: '7d' | '30d' | null,
): Promise<MarketCalibrationSnapshotPayload | null> {
  try {
    let row: { payload_json: string } | null = null;
    if (horizon === null) {
      row = await db.prepare(`
        SELECT payload_json
        FROM market_calibration_snapshots
        WHERE metric = ?
          AND horizon IS NULL
        ORDER BY as_of DESC
        LIMIT 1
      `).bind(metric).first<{ payload_json: string }>();
    } else {
      row = await db.prepare(`
        SELECT payload_json
        FROM market_calibration_snapshots
        WHERE metric = ?
          AND horizon = ?
        ORDER BY as_of DESC
        LIMIT 1
      `).bind(metric, horizon).first<{ payload_json: string }>();
    }

    return parseCalibrationSnapshotPayload(row?.payload_json) || null;
  } catch (err) {
    console.warn('Calibration snapshot lookup failed:', err);
    return null;
  }
}

export async function fetchCalibrationSnapshotAtOrBefore(
  db: D1Database,
  metric: 'edge_quality' | 'conviction',
  horizon: '7d' | '30d' | null,
  asOf?: string | null,
): Promise<MarketCalibrationSnapshotPayload | null> {
  try {
    const asOfFilter = asOf && parseIsoDate(asOf) ? `${asOf.slice(0, 10)}T23:59:59.999Z` : null;
    let row: { payload_json: string } | null = null;

    if (horizon === null) {
      if (asOfFilter) {
        row = await db.prepare(`
          SELECT payload_json
          FROM market_calibration_snapshots
          WHERE metric = ?
            AND horizon IS NULL
            AND as_of <= ?
          ORDER BY as_of DESC
          LIMIT 1
        `).bind(metric, asOfFilter).first<{ payload_json: string }>();
      } else {
        row = await db.prepare(`
          SELECT payload_json
          FROM market_calibration_snapshots
          WHERE metric = ?
            AND horizon IS NULL
          ORDER BY as_of DESC
          LIMIT 1
        `).bind(metric).first<{ payload_json: string }>();
      }
    } else if (asOfFilter) {
      row = await db.prepare(`
        SELECT payload_json
        FROM market_calibration_snapshots
        WHERE metric = ?
          AND horizon = ?
          AND as_of <= ?
        ORDER BY as_of DESC
        LIMIT 1
      `).bind(metric, horizon, asOfFilter).first<{ payload_json: string }>();
    } else {
      row = await db.prepare(`
        SELECT payload_json
        FROM market_calibration_snapshots
        WHERE metric = ?
          AND horizon = ?
        ORDER BY as_of DESC
        LIMIT 1
      `).bind(metric, horizon).first<{ payload_json: string }>();
    }

    return parseCalibrationSnapshotPayload(row?.payload_json) || null;
  } catch (err) {
    console.warn('Calibration snapshot lookup failed:', err);
    return null;
  }
}

export async function storeCalibrationSnapshot(
  db: D1Database,
  snapshot: MarketCalibrationSnapshotPayload,
): Promise<void> {
  if (snapshot.horizon === null) {
    await db.prepare(`
      DELETE FROM market_calibration_snapshots
      WHERE as_of = ?
        AND metric = ?
        AND horizon IS NULL
    `).bind(snapshot.as_of, snapshot.metric).run();
  }

  await db.prepare(`
    INSERT OR REPLACE INTO market_calibration_snapshots
      (as_of, metric, horizon, payload_json, created_at)
    VALUES (?, ?, ?, ?, datetime('now'))
  `).bind(
    snapshot.as_of,
    snapshot.metric,
    snapshot.horizon,
    JSON.stringify(snapshot),
  ).run();
}

export async function storeBriefSnapshot(db: D1Database, brief: BriefSnapshot): Promise<void> {
  await db.prepare(`
    INSERT OR REPLACE INTO market_brief_snapshots (as_of, contract_version, payload_json, created_at)
    VALUES (?, ?, ?, datetime('now'))
  `).bind(brief.as_of, brief.contract_version || BRIEF_CONTRACT_VERSION, JSON.stringify(brief)).run();
}

export async function storeConsistencyCheck(
  db: D1Database,
  asOf: string,
  consistency: ConsistencySnapshot,
): Promise<void> {
  await db.prepare(`
    INSERT OR REPLACE INTO market_consistency_checks (as_of, score, state, violations_json, components_json, created_at)
    VALUES (?, ?, ?, ?, ?, datetime('now'))
  `).bind(
    asOf,
    consistency.score,
    consistency.state,
    JSON.stringify(consistency.violations),
    JSON.stringify(consistency.components),
  ).run();
}

export async function fetchLatestConsistencyCheck(db: D1Database): Promise<{
  as_of: string;
  score: number;
  state: ConsistencyState;
  violations: string[];
  components: ConsistencySnapshot['components'];
  created_at: string;
} | null> {
  const row = await db.prepare(`
    SELECT as_of, score, state, violations_json, components_json, created_at
    FROM market_consistency_checks
    ORDER BY as_of DESC
    LIMIT 1
  `).first<{
    as_of: string;
    score: number;
    state: string;
    violations_json: string;
    components_json?: string | null;
    created_at: string;
  }>();

  if (!row) return null;

  let violations: string[] = [];
  let components: ConsistencySnapshot['components'] = {
    base_score: 100,
    structural_penalty: 0,
    reliability_penalty: 0,
  };

  try {
    const parsed = JSON.parse(row.violations_json) as unknown;
    if (Array.isArray(parsed)) {
      violations = parsed.map((value) => String(value));
    }
  } catch {
    violations = [];
  }

  try {
    if (row.components_json) {
      const parsed = JSON.parse(row.components_json) as Partial<ConsistencySnapshot['components']>;
      if (parsed && typeof parsed === 'object') {
        components = {
          base_score: toNumber(parsed.base_score, 100),
          structural_penalty: toNumber(parsed.structural_penalty, 0),
          reliability_penalty: toNumber(parsed.reliability_penalty, 0),
        };
      }
    }
  } catch {
    components = {
      base_score: 100,
      structural_penalty: 0,
      reliability_penalty: 0,
    };
  }

  const state: ConsistencyState = row.state === 'FAIL' ? 'FAIL' : row.state === 'WARN' ? 'WARN' : 'PASS';
  return {
    as_of: row.as_of,
    score: toNumber(row.score, 0),
    state,
    violations,
    components,
    created_at: row.created_at,
  };
}

export async function fetchDecisionImpactSnapshotAtOrBefore(
  db: D1Database,
  horizon: '7d' | '30d',
  scope: 'market' | 'theme',
  windowDays: 30 | 90,
  asOf?: string | null,
): Promise<DecisionImpactResponsePayload | null> {
  const asOfFilter = asOf && parseIsoDate(asOf) ? `${asOf.slice(0, 10)}T23:59:59.999Z` : null;
  let row: { payload_json: string } | null = null;

  if (asOfFilter) {
    row = await db.prepare(`
      SELECT payload_json
      FROM market_decision_impact_snapshots
      WHERE horizon = ?
        AND scope = ?
        AND window_days = ?
        AND as_of <= ?
      ORDER BY as_of DESC
      LIMIT 1
    `).bind(horizon, scope, windowDays, asOfFilter).first<{ payload_json: string }>();
  } else {
    row = await db.prepare(`
      SELECT payload_json
      FROM market_decision_impact_snapshots
      WHERE horizon = ?
        AND scope = ?
        AND window_days = ?
      ORDER BY as_of DESC
      LIMIT 1
    `).bind(horizon, scope, windowDays).first<{ payload_json: string }>();
  }

  if (!row?.payload_json) return null;

  try {
    const parsed = JSON.parse(row.payload_json) as DecisionImpactResponsePayload;
    if (!parsed || typeof parsed !== 'object') return null;
    if (parsed.horizon !== horizon || parsed.scope !== scope || parsed.window_days !== windowDays) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

export async function storeDecisionImpactSnapshot(
  db: D1Database,
  payload: DecisionImpactResponsePayload,
): Promise<void> {
  await db.prepare(`
    INSERT OR REPLACE INTO market_decision_impact_snapshots (
      as_of,
      horizon,
      scope,
      window_days,
      payload_json,
      created_at
    ) VALUES (?, ?, ?, ?, ?, datetime('now'))
  `).bind(
    payload.as_of,
    payload.horizon,
    payload.scope,
    payload.window_days,
    JSON.stringify(payload),
  ).run();
}

export async function fetchLatestMarketProductSnapshotWrite(db: D1Database): Promise<string | null> {
  try {
    const row = await db.prepare(`
      SELECT MAX(created_at) as latest_created_at
      FROM (
        SELECT created_at FROM market_brief_snapshots
        UNION ALL
        SELECT created_at FROM opportunity_snapshots
        UNION ALL
        SELECT created_at FROM market_calibration_snapshots
      )
    `).first<{ latest_created_at: string | null }>();
    return row?.latest_created_at || null;
  } catch {
    return null;
  }
}

export async function storeOpportunitySnapshot(db: D1Database, snapshot: OpportunitySnapshot): Promise<void> {
  await db.prepare(`
    INSERT OR REPLACE INTO opportunity_snapshots (as_of, horizon, payload_json, created_at)
    VALUES (?, ?, ?, datetime('now'))
  `).bind(snapshot.as_of, snapshot.horizon, JSON.stringify(snapshot)).run();
}
