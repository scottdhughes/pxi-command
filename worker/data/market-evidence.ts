import {
  DAILY_CLOSE_CANONICAL_SLOT,
  RESEARCH_FEATURE_VERSION,
  RESEARCH_STORAGE_CONTRACT,
  isDailyCloseCanonicalWindow,
  type StoredResearchSnapshot,
} from './research-vintages';

export const MARKET_EVIDENCE_MODEL_FAMILY = 'empirical_bucket_spy_return';
export const MARKET_EVIDENCE_MODEL_VERSION = 'empirical-bucket-spy-return/v1';
export const MARKET_EVIDENCE_TARGET_METRIC = 'spy_return_pct';

const DEFAULT_HISTORY_LIMIT = 2500;
const MAX_HISTORY_LIMIT = 5000;
const OUTCOME_LOOKAHEAD_DAYS = 4;
const EVALUATION_BATCH_LIMIT = 100;
const BUCKET_BOUNDARIES = [20, 40, 60, 80] as const;

interface ProspectiveTrainingRow {
  prediction_date: string;
  prediction_available_at: string;
  current_pxi_score: number;
  outcome_status_7d: 'pending' | 'observed' | 'unavailable';
  outcome_status_30d: 'pending' | 'observed' | 'unavailable';
  actual_return_7d: number | null;
  actual_return_30d: number | null;
  actual_observation_date_7d: string | null;
  actual_observation_date_30d: string | null;
  evaluated_at_7d: string | null;
  evaluated_at_30d: string | null;
}

interface CanonicalPriceObservation {
  date: string;
  value: number;
  snapshot_id: string;
}

interface SummaryStats {
  mean: number | null;
  median: number | null;
  win_rate: number | null;
  ci95_low: number | null;
  ci95_high: number | null;
  sample_size: number;
}

export interface MarketPredictionEvidence {
  id?: number;
  evidence_id: string;
  prediction_date: string;
  prediction_available_at: string;
  canonical_slot: typeof DAILY_CLOSE_CANONICAL_SLOT;
  feature_snapshot_id: string;
  model_family: string;
  model_version: string;
  target_metric: typeof MARKET_EVIDENCE_TARGET_METRIC;
  current_pxi_score: number;
  pxi_bucket: string;
  bucket_lower: number;
  bucket_upper: number;
  benchmark_close: number;
  benchmark_observation_date: string;
  target_date_7d: string;
  target_date_30d: string;
  predicted_return_7d: number | null;
  predicted_return_30d: number | null;
  median_return_7d: number | null;
  median_return_30d: number | null;
  win_rate_7d: number | null;
  win_rate_30d: number | null;
  ci95_low_7d: number | null;
  ci95_high_7d: number | null;
  ci95_low_30d: number | null;
  ci95_high_30d: number | null;
  sample_size_7d: number;
  sample_size_30d: number;
  training_cutoff_date: string | null;
  methodology_json: string;
  outcome_status_7d: 'pending' | 'observed' | 'unavailable';
  outcome_status_30d: 'pending' | 'observed' | 'unavailable';
  outcome_unavailable_reason_7d: string | null;
  outcome_unavailable_reason_30d: string | null;
  actual_return_7d: number | null;
  actual_return_30d: number | null;
  actual_observation_date_7d: string | null;
  actual_observation_date_30d: string | null;
  evaluated_at_7d: string | null;
  evaluated_at_30d: string | null;
  evaluated_at: string | null;
  created_at?: string;
}

export interface MarketPredictionEvidenceCounts {
  total: number;
  evaluated: number;
  pending: number;
  with_7d_outcome: number;
  with_30d_outcome: number;
}

export interface EvidenceEvaluationSummary {
  pending: number;
  evaluated_7d: number;
  evaluated_30d: number;
  unavailable_7d: number;
  unavailable_30d: number;
  completed: number;
  skipped_reason: 'outside_daily_close_window' | null;
}

function isDateKey(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00.000Z`);
  return !Number.isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}

function addCalendarDays(dateKey: string, days: number): string {
  const date = new Date(`${dateKey}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function minimumDate(left: string, right: string): string {
  return left <= right ? left : right;
}

function resolveBucket(score: number): { label: string; lower: number; upper: number } {
  if (score < BUCKET_BOUNDARIES[0]) return { label: '0-20', lower: 0, upper: 20 };
  if (score < BUCKET_BOUNDARIES[1]) return { label: '20-40', lower: 20, upper: 40 };
  if (score < BUCKET_BOUNDARIES[2]) return { label: '40-60', lower: 40, upper: 60 };
  if (score < BUCKET_BOUNDARIES[3]) return { label: '60-80', lower: 60, upper: 80 };
  return { label: '80-100', lower: 80, upper: 100 };
}

function scoreIsInBucket(score: number, bucket: { lower: number; upper: number }): boolean {
  if (bucket.upper === 100) return score >= bucket.lower && score <= bucket.upper;
  return score >= bucket.lower && score < bucket.upper;
}

function summarize(values: number[]): SummaryStats {
  if (values.length === 0) {
    return {
      mean: null,
      median: null,
      win_rate: null,
      ci95_low: null,
      ci95_high: null,
      sample_size: 0,
    };
  }

  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  const median = sorted.length % 2 === 1
    ? sorted[middle]
    : (sorted[middle - 1] + sorted[middle]) / 2;
  const winRate = values.filter((value) => value > 0).length / values.length;

  let ci95Low: number | null = null;
  let ci95High: number | null = null;
  if (values.length >= 2) {
    const variance = values.reduce((sum, value) => sum + Math.pow(value - mean, 2), 0) / (values.length - 1);
    const margin = 1.96 * Math.sqrt(variance / values.length);
    ci95Low = mean - margin;
    ci95High = mean + margin;
  }

  return {
    mean,
    median,
    win_rate: winRate,
    ci95_low: ci95Low,
    ci95_high: ci95High,
    sample_size: values.length,
  };
}

function asFiniteNumber(value: unknown): number | null {
  const numeric = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function normalizeHistoryLimit(value: number | undefined): number {
  if (!Number.isFinite(value)) return DEFAULT_HISTORY_LIMIT;
  return Math.max(1, Math.min(MAX_HISTORY_LIMIT, Math.floor(value!)));
}

function normalizeAsOf(asOf: Date | string): { date: string; evaluatedAt: string } {
  if (asOf instanceof Date) {
    if (Number.isNaN(asOf.getTime())) throw new Error('Invalid evaluation date');
    const evaluatedAt = asOf.toISOString();
    const date = newYorkDateKey(evaluatedAt);
    if (!date) throw new Error('Invalid evaluation date');
    return { date, evaluatedAt };
  }
  if (isDateKey(asOf)) {
    return { date: asOf, evaluatedAt: `${asOf}T23:59:59.999Z` };
  }
  const parsed = new Date(asOf);
  if (Number.isNaN(parsed.getTime())) throw new Error('Invalid evaluation date');
  const evaluatedAt = parsed.toISOString();
  const date = newYorkDateKey(evaluatedAt);
  if (!date) throw new Error('Invalid evaluation date');
  return { date, evaluatedAt };
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

function snapshotIsCausallyUsable(snapshot: StoredResearchSnapshot): boolean {
  if (snapshot.canonical_slot !== DAILY_CLOSE_CANONICAL_SLOT) return false;
  if (snapshot.feature_version !== RESEARCH_FEATURE_VERSION) return false;
  if (snapshot.storage_contract !== RESEARCH_STORAGE_CONTRACT) return false;
  if (!isDateKey(snapshot.decision_date)) return false;
  if (newYorkDateKey(snapshot.available_at) !== snapshot.decision_date) return false;
  if (!isDailyCloseCanonicalWindow(snapshot.available_at)) return false;
  if (snapshot.benchmark_observation_date !== snapshot.decision_date) return false;
  if (!Number.isFinite(snapshot.benchmark_close) || snapshot.benchmark_close <= 0) return false;
  const score = snapshot.features?.pxi_score;
  if (!Number.isFinite(score) || score < 0 || score > 100) return false;
  const featureNames = Object.keys(snapshot.features || {});
  if (featureNames.length === 0) return false;
  return featureNames.every((featureName) => {
    const value = snapshot.features[featureName];
    const observationDate = snapshot.feature_observation_dates?.[featureName];
    const source = snapshot.feature_sources?.[featureName];
    return Number.isFinite(value)
      && typeof observationDate === 'string'
      && isDateKey(observationDate)
      && observationDate <= snapshot.decision_date
      && typeof source === 'string'
      && source.trim().length > 0;
  });
}

export async function computeEmpiricalBucketMarketPredictionEvidence(
  db: D1Database,
  snapshot: StoredResearchSnapshot,
  options: { historyLimit?: number } = {},
): Promise<MarketPredictionEvidence | null> {
  if (!snapshotIsCausallyUsable(snapshot)) return null;

  const historyLimit = normalizeHistoryLimit(options.historyLimit);
  const decisionDate = snapshot.decision_date;
  const currentPxiScore = snapshot.features.pxi_score;
  const bucket = resolveBucket(currentPxiScore);

  const historyResult = await db.prepare(`
    SELECT *
    FROM (
      SELECT
        prediction_date,
        prediction_available_at,
        current_pxi_score,
        outcome_status_7d,
        outcome_status_30d,
        actual_return_7d,
        actual_return_30d,
        actual_observation_date_7d,
        actual_observation_date_30d,
        evaluated_at_7d,
        evaluated_at_30d
      FROM market_prediction_evidence
      WHERE canonical_slot = ?
        AND model_version = ?
        AND prediction_date < ?
      ORDER BY prediction_date DESC
      LIMIT ?
    )
    ORDER BY prediction_date ASC
  `).bind(
    DAILY_CLOSE_CANONICAL_SLOT,
    MARKET_EVIDENCE_MODEL_VERSION,
    decisionDate,
    historyLimit,
  ).all<ProspectiveTrainingRow>();

  const returns7d: number[] = [];
  const returns30d: number[] = [];
  const outcomeObservationDates: string[] = [];

  for (const row of historyResult.results || []) {
    if (
      !isDateKey(row.prediction_date)
      || row.prediction_date >= decisionDate
      || !Number.isFinite(row.current_pxi_score)
      || !scoreIsInBucket(row.current_pxi_score, bucket)
    ) continue;

    if (
      row.outcome_status_7d === 'observed'
      && Number.isFinite(row.actual_return_7d)
      && typeof row.actual_observation_date_7d === 'string'
      && isDateKey(row.actual_observation_date_7d)
      && row.actual_observation_date_7d <= decisionDate
      && typeof row.evaluated_at_7d === 'string'
      && row.evaluated_at_7d <= snapshot.available_at
    ) {
      returns7d.push(Number(row.actual_return_7d));
      outcomeObservationDates.push(row.actual_observation_date_7d);
    }

    if (
      row.outcome_status_30d === 'observed'
      && Number.isFinite(row.actual_return_30d)
      && typeof row.actual_observation_date_30d === 'string'
      && isDateKey(row.actual_observation_date_30d)
      && row.actual_observation_date_30d <= decisionDate
      && typeof row.evaluated_at_30d === 'string'
      && row.evaluated_at_30d <= snapshot.available_at
    ) {
      returns30d.push(Number(row.actual_return_30d));
      outcomeObservationDates.push(row.actual_observation_date_30d);
    }
  }

  const stats7d = summarize(returns7d);
  const stats30d = summarize(returns30d);
  const trainingCutoffDate = outcomeObservationDates.length > 0
    ? outcomeObservationDates.sort().at(-1) || null
    : null;

  const methodology = {
    model_family: MARKET_EVIDENCE_MODEL_FAMILY,
    model_version: MARKET_EVIDENCE_MODEL_VERSION,
    target_metric: MARKET_EVIDENCE_TARGET_METRIC,
    bucket_boundaries: BUCKET_BOUNDARIES,
    bucket_membership: 'lower_inclusive_upper_exclusive_except_80_100',
    point_forecast: 'arithmetic_mean_of_same_bucket_prior_prospective_outcomes',
    interval: 'normal_approximation_sample_mean_95pct',
    outcome_price: 'first_spy_observation_on_or_after_target',
    outcome_lookahead_calendar_days: OUTCOME_LOOKAHEAD_DAYS,
    realized_outcome_source: 'immutable_daily_close_research_snapshot',
    training_source: 'prior_rows_in_immutable_market_prediction_evidence',
    historical_score_filter: 'prediction_date_strictly_before_current_prediction_date',
    causal_outcome_filter: 'observed_outcome_evaluated_at_lte_current_prediction_available_at',
    history_limit: historyLimit,
  };

  return {
    evidence_id: `market-evidence:${decisionDate}:${DAILY_CLOSE_CANONICAL_SLOT}:${MARKET_EVIDENCE_MODEL_VERSION}`,
    prediction_date: decisionDate,
    prediction_available_at: snapshot.available_at,
    canonical_slot: DAILY_CLOSE_CANONICAL_SLOT,
    feature_snapshot_id: snapshot.snapshot_id,
    model_family: MARKET_EVIDENCE_MODEL_FAMILY,
    model_version: MARKET_EVIDENCE_MODEL_VERSION,
    target_metric: MARKET_EVIDENCE_TARGET_METRIC,
    current_pxi_score: currentPxiScore,
    pxi_bucket: bucket.label,
    bucket_lower: bucket.lower,
    bucket_upper: bucket.upper,
    benchmark_close: snapshot.benchmark_close,
    benchmark_observation_date: snapshot.benchmark_observation_date,
    target_date_7d: addCalendarDays(decisionDate, 7),
    target_date_30d: addCalendarDays(decisionDate, 30),
    predicted_return_7d: stats7d.mean,
    predicted_return_30d: stats30d.mean,
    median_return_7d: stats7d.median,
    median_return_30d: stats30d.median,
    win_rate_7d: stats7d.win_rate,
    win_rate_30d: stats30d.win_rate,
    ci95_low_7d: stats7d.ci95_low,
    ci95_high_7d: stats7d.ci95_high,
    ci95_low_30d: stats30d.ci95_low,
    ci95_high_30d: stats30d.ci95_high,
    sample_size_7d: stats7d.sample_size,
    sample_size_30d: stats30d.sample_size,
    training_cutoff_date: trainingCutoffDate,
    methodology_json: JSON.stringify(methodology),
    outcome_status_7d: 'pending',
    outcome_status_30d: 'pending',
    outcome_unavailable_reason_7d: null,
    outcome_unavailable_reason_30d: null,
    actual_return_7d: null,
    actual_return_30d: null,
    actual_observation_date_7d: null,
    actual_observation_date_30d: null,
    evaluated_at_7d: null,
    evaluated_at_30d: null,
    evaluated_at: null,
  };
}

export async function insertMarketPredictionEvidence(
  db: D1Database,
  evidence: MarketPredictionEvidence,
): Promise<{ status: 'inserted' | 'existing'; evidence: MarketPredictionEvidence }> {
  const result = await db.prepare(`
    INSERT INTO market_prediction_evidence (
      evidence_id,
      prediction_date,
      prediction_available_at,
      canonical_slot,
      feature_snapshot_id,
      model_family,
      model_version,
      target_metric,
      current_pxi_score,
      pxi_bucket,
      bucket_lower,
      bucket_upper,
      benchmark_close,
      benchmark_observation_date,
      target_date_7d,
      target_date_30d,
      predicted_return_7d,
      predicted_return_30d,
      median_return_7d,
      median_return_30d,
      win_rate_7d,
      win_rate_30d,
      ci95_low_7d,
      ci95_high_7d,
      ci95_low_30d,
      ci95_high_30d,
      sample_size_7d,
      sample_size_30d,
      training_cutoff_date,
      methodology_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(prediction_date, canonical_slot, model_version) DO NOTHING
  `).bind(
    evidence.evidence_id,
    evidence.prediction_date,
    evidence.prediction_available_at,
    evidence.canonical_slot,
    evidence.feature_snapshot_id,
    evidence.model_family,
    evidence.model_version,
    evidence.target_metric,
    evidence.current_pxi_score,
    evidence.pxi_bucket,
    evidence.bucket_lower,
    evidence.bucket_upper,
    evidence.benchmark_close,
    evidence.benchmark_observation_date,
    evidence.target_date_7d,
    evidence.target_date_30d,
    evidence.predicted_return_7d,
    evidence.predicted_return_30d,
    evidence.median_return_7d,
    evidence.median_return_30d,
    evidence.win_rate_7d,
    evidence.win_rate_30d,
    evidence.ci95_low_7d,
    evidence.ci95_high_7d,
    evidence.ci95_low_30d,
    evidence.ci95_high_30d,
    evidence.sample_size_7d,
    evidence.sample_size_30d,
    evidence.training_cutoff_date,
    evidence.methodology_json,
  ).run();

  if (result.meta?.changes !== 0) return { status: 'inserted', evidence };
  const existing = await fetchLatestMarketPredictionEvidence(
    db,
    evidence.canonical_slot,
    evidence.model_version,
    evidence.prediction_date,
  );
  if (!existing) throw new Error('Canonical prediction evidence conflict could not be resolved');
  return { status: 'existing', evidence: existing };
}

export async function captureCanonicalMarketPredictionEvidence(
  db: D1Database,
  snapshot: StoredResearchSnapshot,
  options: { historyLimit?: number } = {},
): Promise<{ status: 'inserted' | 'existing'; evidence: MarketPredictionEvidence } | null> {
  const evidence = await computeEmpiricalBucketMarketPredictionEvidence(db, snapshot, options);
  if (!evidence) return null;
  return insertMarketPredictionEvidence(db, evidence);
}

async function fetchFirstOutcomeObservation(
  db: D1Database,
  targetDate: string,
  asOfDate: string,
): Promise<CanonicalPriceObservation | null> {
  const lastAllowedDate = minimumDate(addCalendarDays(targetDate, OUTCOME_LOOKAHEAD_DAYS), asOfDate);
  return db.prepare(`
    SELECT
      snapshot_id,
      decision_date AS date,
      benchmark_close AS value
    FROM research_feature_snapshots
    WHERE canonical_slot = ?
      AND feature_version = ?
      AND storage_contract = ?
      AND decision_date >= ?
      AND decision_date <= ?
    ORDER BY decision_date ASC
    LIMIT 1
  `).bind(
    DAILY_CLOSE_CANONICAL_SLOT,
    RESEARCH_FEATURE_VERSION,
    RESEARCH_STORAGE_CONTRACT,
    targetDate,
    lastAllowedDate,
  ).first<CanonicalPriceObservation>();
}

export async function evaluatePendingMarketPredictionEvidence(
  db: D1Database,
  asOf: Date | string = new Date(),
): Promise<EvidenceEvaluationSummary> {
  const normalized = normalizeAsOf(asOf);
  if (!isDailyCloseCanonicalWindow(normalized.evaluatedAt)) {
    return {
      pending: 0,
      evaluated_7d: 0,
      evaluated_30d: 0,
      unavailable_7d: 0,
      unavailable_30d: 0,
      completed: 0,
      skipped_reason: 'outside_daily_close_window',
    };
  }
  const pending = await db.prepare(`
    SELECT
      evidence_id,
      benchmark_close,
      target_date_7d,
      target_date_30d,
      outcome_status_7d,
      outcome_status_30d,
      actual_return_7d,
      actual_return_30d,
      actual_observation_date_7d,
      actual_observation_date_30d,
      evaluated_at_7d,
      evaluated_at_30d,
      evaluated_at
    FROM market_prediction_evidence
    WHERE evaluated_at IS NULL
      AND (
        (outcome_status_7d = 'pending' AND target_date_7d <= ?)
        OR (outcome_status_30d = 'pending' AND target_date_30d <= ?)
      )
    ORDER BY prediction_date ASC
    LIMIT ${EVALUATION_BATCH_LIMIT}
  `).bind(normalized.date, normalized.date).all<Pick<
    MarketPredictionEvidence,
    | 'evidence_id'
    | 'benchmark_close'
    | 'target_date_7d'
    | 'target_date_30d'
    | 'outcome_status_7d'
    | 'outcome_status_30d'
    | 'actual_return_7d'
    | 'actual_return_30d'
    | 'actual_observation_date_7d'
    | 'actual_observation_date_30d'
    | 'evaluated_at_7d'
    | 'evaluated_at_30d'
    | 'evaluated_at'
  >>();

  let evaluated7d = 0;
  let evaluated30d = 0;
  let unavailable7d = 0;
  let unavailable30d = 0;
  let completed = 0;

  for (const row of pending.results || []) {
    if (row.outcome_status_7d === 'pending' && row.target_date_7d <= normalized.date) {
      const observation = await fetchFirstOutcomeObservation(db, row.target_date_7d, normalized.date);
      if (observation && Number.isFinite(observation.value) && observation.value > 0) {
        const result = await db.prepare(`
          UPDATE market_prediction_evidence
          SET outcome_status_7d = 'observed',
              actual_return_7d = ?,
              actual_observation_date_7d = ?,
              evaluated_at_7d = ?
          WHERE evidence_id = ?
            AND outcome_status_7d = 'pending'
        `).bind(
          ((observation.value - row.benchmark_close) / row.benchmark_close) * 100,
          observation.date,
          normalized.evaluatedAt,
          row.evidence_id,
        ).run();
        if (Number(result.meta?.changes ?? 0) > 0) evaluated7d += 1;
      } else if (normalized.date > addCalendarDays(row.target_date_7d, OUTCOME_LOOKAHEAD_DAYS)) {
        const result = await db.prepare(`
          UPDATE market_prediction_evidence
          SET outcome_status_7d = 'unavailable',
              outcome_unavailable_reason_7d = 'canonical_close_missing_within_tolerance',
              evaluated_at_7d = ?
          WHERE evidence_id = ?
            AND outcome_status_7d = 'pending'
        `).bind(normalized.evaluatedAt, row.evidence_id).run();
        if (Number(result.meta?.changes ?? 0) > 0) unavailable7d += 1;
      }
    }

    if (row.outcome_status_30d === 'pending' && row.target_date_30d <= normalized.date) {
      const observation = await fetchFirstOutcomeObservation(db, row.target_date_30d, normalized.date);
      if (observation && Number.isFinite(observation.value) && observation.value > 0) {
        const result = await db.prepare(`
          UPDATE market_prediction_evidence
          SET outcome_status_30d = 'observed',
              actual_return_30d = ?,
              actual_observation_date_30d = ?,
              evaluated_at_30d = ?
          WHERE evidence_id = ?
            AND outcome_status_30d = 'pending'
        `).bind(
          ((observation.value - row.benchmark_close) / row.benchmark_close) * 100,
          observation.date,
          normalized.evaluatedAt,
          row.evidence_id,
        ).run();
        if (Number(result.meta?.changes ?? 0) > 0) evaluated30d += 1;
      } else if (normalized.date > addCalendarDays(row.target_date_30d, OUTCOME_LOOKAHEAD_DAYS)) {
        const result = await db.prepare(`
          UPDATE market_prediction_evidence
          SET outcome_status_30d = 'unavailable',
              outcome_unavailable_reason_30d = 'canonical_close_missing_within_tolerance',
              evaluated_at_30d = ?
          WHERE evidence_id = ?
            AND outcome_status_30d = 'pending'
        `).bind(normalized.evaluatedAt, row.evidence_id).run();
        if (Number(result.meta?.changes ?? 0) > 0) unavailable30d += 1;
      }
    }

    const completion = await db.prepare(`
      UPDATE market_prediction_evidence
      SET evaluated_at = ?
      WHERE evidence_id = ?
        AND evaluated_at IS NULL
        AND outcome_status_7d != 'pending'
        AND outcome_status_30d != 'pending'
    `).bind(
      normalized.evaluatedAt,
      row.evidence_id,
    ).run();
    if (Number(completion.meta?.changes ?? 0) > 0) completed += 1;
  }

  return {
    pending: pending.results?.length || 0,
    evaluated_7d: evaluated7d,
    evaluated_30d: evaluated30d,
    unavailable_7d: unavailable7d,
    unavailable_30d: unavailable30d,
    completed,
    skipped_reason: null,
  };
}

export async function fetchMarketPredictionEvidenceRows(
  db: D1Database,
  options: {
    canonicalSlot?: typeof DAILY_CLOSE_CANONICAL_SLOT;
    modelVersion?: string;
    throughDate?: string;
    evaluatedOnly?: boolean;
    limit?: number;
  } = {},
): Promise<MarketPredictionEvidence[]> {
  const canonicalSlot = options.canonicalSlot ?? DAILY_CLOSE_CANONICAL_SLOT;
  const modelVersion = options.modelVersion ?? MARKET_EVIDENCE_MODEL_VERSION;
  const throughDate = options.throughDate ?? '9999-12-31';
  if (!isDateKey(throughDate)) throw new Error('Invalid evidence through date');
  const limit = Math.max(1, Math.min(5000, Math.floor(options.limit ?? 100)));
  const rows = await db.prepare(`
    SELECT *
    FROM (
      SELECT *
      FROM market_prediction_evidence
      WHERE canonical_slot = ?
        AND model_version = ?
        AND prediction_date <= ?
        ${options.evaluatedOnly ? 'AND (actual_return_7d IS NOT NULL OR actual_return_30d IS NOT NULL)' : ''}
      ORDER BY prediction_date DESC
      LIMIT ?
    )
    ORDER BY prediction_date ASC
  `).bind(canonicalSlot, modelVersion, throughDate, limit).all<MarketPredictionEvidence>();
  return rows.results || [];
}

export async function fetchLatestMarketPredictionEvidence(
  db: D1Database,
  canonicalSlot: typeof DAILY_CLOSE_CANONICAL_SLOT = DAILY_CLOSE_CANONICAL_SLOT,
  modelVersion = MARKET_EVIDENCE_MODEL_VERSION,
  throughDate = '9999-12-31',
): Promise<MarketPredictionEvidence | null> {
  if (!isDateKey(throughDate)) throw new Error('Invalid evidence through date');
  return db.prepare(`
    SELECT *
    FROM market_prediction_evidence
    WHERE canonical_slot = ?
      AND model_version = ?
      AND prediction_date <= ?
    ORDER BY prediction_date DESC
    LIMIT 1
  `).bind(canonicalSlot, modelVersion, throughDate).first<MarketPredictionEvidence>();
}

export async function fetchMarketPredictionEvidenceCounts(
  db: D1Database,
  canonicalSlot: typeof DAILY_CLOSE_CANONICAL_SLOT = DAILY_CLOSE_CANONICAL_SLOT,
  modelVersion = MARKET_EVIDENCE_MODEL_VERSION,
): Promise<MarketPredictionEvidenceCounts> {
  const row = await db.prepare(`
    SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN evaluated_at IS NOT NULL THEN 1 ELSE 0 END) AS evaluated,
      SUM(CASE WHEN evaluated_at IS NULL THEN 1 ELSE 0 END) AS pending,
      SUM(CASE WHEN actual_return_7d IS NOT NULL THEN 1 ELSE 0 END) AS with_7d_outcome,
      SUM(CASE WHEN actual_return_30d IS NOT NULL THEN 1 ELSE 0 END) AS with_30d_outcome
    FROM market_prediction_evidence
    WHERE canonical_slot = ?
      AND model_version = ?
  `).bind(canonicalSlot, modelVersion).first<Partial<MarketPredictionEvidenceCounts>>();

  return {
    total: Math.max(0, Math.floor(asFiniteNumber(row?.total) ?? 0)),
    evaluated: Math.max(0, Math.floor(asFiniteNumber(row?.evaluated) ?? 0)),
    pending: Math.max(0, Math.floor(asFiniteNumber(row?.pending) ?? 0)),
    with_7d_outcome: Math.max(0, Math.floor(asFiniteNumber(row?.with_7d_outcome) ?? 0)),
    with_30d_outcome: Math.max(0, Math.floor(asFiniteNumber(row?.with_30d_outcome) ?? 0)),
  };
}
