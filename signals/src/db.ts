import type { Env } from "./config"

export interface RunRow {
  id: string
  created_at_utc: string
  lookback_days: number
  baseline_days: number
  status: string
  summary_json: string | null
  report_html_key: string
  results_json_key: string
  raw_json_key: string | null
  error_message: string | null
}

export interface LatestRunSummary {
  id: string
  created_at_utc: string
}

export interface PipelineFreshness {
  latest_success_at: string | null
  hours_since_success: number | null
  threshold_days: number
  is_stale: boolean
  status: "ok" | "stale" | "no_history"
}

export interface PipelineLockAcquireResult {
  acquired: boolean
  reason?: "already_locked"
}

export async function insertRun(env: Env, row: RunRow) {
  await env.SIGNALS_DB.prepare(
    `INSERT INTO runs (id, created_at_utc, lookback_days, baseline_days, status, summary_json, report_html_key, results_json_key, raw_json_key, error_message)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      row.id,
      row.created_at_utc,
      row.lookback_days,
      row.baseline_days,
      row.status,
      row.summary_json,
      row.report_html_key,
      row.results_json_key,
      row.raw_json_key,
      row.error_message
    )
    .run()
}

export async function listRuns(
  env: Env,
  limit = 20,
  status?: string
): Promise<RunRow[]> {
  if (status) {
    const res = await env.SIGNALS_DB.prepare(
      `SELECT * FROM runs WHERE status = ? ORDER BY created_at_utc DESC LIMIT ?`
    )
      .bind(status, limit)
      .all<RunRow>()
    return res.results || []
  }

  const res = await env.SIGNALS_DB.prepare(
    `SELECT * FROM runs ORDER BY created_at_utc DESC LIMIT ?`
  )
    .bind(limit)
    .all<RunRow>()
  return res.results || []
}

export async function getRun(env: Env, id: string): Promise<RunRow | null> {
  const res = await env.SIGNALS_DB.prepare(`SELECT * FROM runs WHERE id = ?`).bind(id).first<RunRow>()
  return res || null
}

export async function getLatestSuccessfulRun(env: Env): Promise<LatestRunSummary | null> {
  const res = await env.SIGNALS_DB.prepare(
    `SELECT id, created_at_utc
     FROM runs
     WHERE status = 'ok'
     ORDER BY created_at_utc DESC
     LIMIT 1`
  ).first<LatestRunSummary>()
  return res || null
}

export async function getPipelineFreshness(
  env: Env,
  opts: { now?: Date; thresholdDays?: number } = {}
): Promise<PipelineFreshness> {
  const thresholdDays = opts.thresholdDays ?? 8
  const now = opts.now ?? new Date()
  const latestSuccess = await getLatestSuccessfulRun(env)

  if (!latestSuccess) {
    return {
      latest_success_at: null,
      hours_since_success: null,
      threshold_days: thresholdDays,
      is_stale: true,
      status: "no_history",
    }
  }

  const latestMs = Date.parse(latestSuccess.created_at_utc)
  if (!Number.isFinite(latestMs)) {
    return {
      latest_success_at: latestSuccess.created_at_utc,
      hours_since_success: null,
      threshold_days: thresholdDays,
      is_stale: true,
      status: "stale",
    }
  }

  const elapsedHours = Math.max(0, (now.getTime() - latestMs) / (1000 * 60 * 60))
  const roundedHours = Math.round(elapsedHours * 100) / 100
  const isStale = elapsedHours > thresholdDays * 24

  return {
    latest_success_at: latestSuccess.created_at_utc,
    hours_since_success: roundedHours,
    threshold_days: thresholdDays,
    is_stale: isStale,
    status: isStale ? "stale" : "ok",
  }
}

function isPipelineLockUniqueConstraintError(error: unknown): boolean {
  if (!(error instanceof Error)) return false
  const message = error.message || ""
  return message.includes("UNIQUE constraint failed") && message.includes("pipeline_locks.lock_key")
}

export async function acquirePipelineLock(
  env: Env,
  lockKey: string,
  lockToken: string,
  nowIso: string,
  ttlSeconds: number
): Promise<PipelineLockAcquireResult> {
  const nowMs = Date.parse(nowIso)
  const fallbackNowMs = Number.isFinite(nowMs) ? nowMs : Date.now()
  const staleThresholdIso = new Date(fallbackNowMs - ttlSeconds * 1000).toISOString()

  await env.SIGNALS_DB.prepare(
    `DELETE FROM pipeline_locks
     WHERE lock_key = ?
       AND acquired_at_utc < ?`
  )
    .bind(lockKey, staleThresholdIso)
    .run()

  try {
    await env.SIGNALS_DB.prepare(
      `INSERT INTO pipeline_locks (lock_key, lock_token, acquired_at_utc)
       VALUES (?, ?, ?)`
    )
      .bind(lockKey, lockToken, nowIso)
      .run()

    return { acquired: true }
  } catch (err) {
    if (isPipelineLockUniqueConstraintError(err)) {
      return { acquired: false, reason: "already_locked" }
    }
    throw err
  }
}

export async function releasePipelineLock(env: Env, lockKey: string, lockToken: string): Promise<void> {
  await env.SIGNALS_DB.prepare(
    `DELETE FROM pipeline_locks
     WHERE lock_key = ? AND lock_token = ?`
  )
    .bind(lockKey, lockToken)
    .run()
}

// ─────────────────────────────────────────────────────────────────────────────
// Signal Predictions
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Canonical prediction row policy:
 * - Partition by logical signal key: (signal_date, theme_id)
 * - Keep earliest row by (created_at, id)
 *
 * This de-biases read paths when historical duplicate rows exist from same-day reruns.
 */
const CANONICAL_PREDICTIONS_CTE = `
  WITH ranked_predictions AS (
    SELECT
      sp.*,
      ROW_NUMBER() OVER (
        PARTITION BY sp.signal_date, sp.theme_id
        ORDER BY sp.created_at ASC, sp.id ASC
      ) AS canonical_row_num
    FROM signal_predictions sp
  ),
  canonical_predictions AS (
    SELECT
      id,
      run_id,
      signal_date,
      target_date,
      theme_id,
      theme_name,
      rank,
      score,
      signal_type,
      confidence,
      timing,
      stars,
      proxy_etf,
      entry_price,
      exit_price,
      exit_price_date,
      return_pct,
      evaluated_at,
      hit,
      evaluation_note,
      created_at
    FROM ranked_predictions
    WHERE canonical_row_num = 1
  )
`

export interface SignalPredictionRow {
  id: number
  run_id: string
  signal_date: string
  target_date: string
  theme_id: string
  theme_name: string
  rank: number
  score: number
  signal_type: string
  confidence: string
  timing: string
  stars: number
  proxy_etf: string | null
  entry_price: number | null
  exit_price: number | null
  exit_price_date: string | null
  return_pct: number | null
  evaluated_at: string | null
  hit: number | null
  evaluation_note: string | null
  created_at: string
}

export interface SignalPredictionInput {
  run_id: string
  signal_date: string
  target_date: string
  theme_id: string
  theme_name: string
  rank: number
  score: number
  signal_type: string
  confidence: string
  timing: string
  stars: number
  proxy_etf: string | null
  entry_price: number | null
}

/**
 * Insert a new signal prediction into the database.
 */
export async function insertSignalPrediction(env: Env, prediction: SignalPredictionInput): Promise<void> {
  await env.SIGNALS_DB.prepare(
    `INSERT INTO signal_predictions 
     (run_id, signal_date, target_date, theme_id, theme_name, rank, score, signal_type, confidence, timing, stars, proxy_etf, entry_price)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(signal_date, theme_id) DO NOTHING`
  )
    .bind(
      prediction.run_id,
      prediction.signal_date,
      prediction.target_date,
      prediction.theme_id,
      prediction.theme_name,
      prediction.rank,
      prediction.score,
      prediction.signal_type,
      prediction.confidence,
      prediction.timing,
      prediction.stars,
      prediction.proxy_etf,
      prediction.entry_price
    )
    .run()
}

/**
 * Insert multiple signal predictions in a batch.
 */
export async function insertSignalPredictions(env: Env, predictions: SignalPredictionInput[]): Promise<void> {
  const stmt = env.SIGNALS_DB.prepare(
    `INSERT INTO signal_predictions 
     (run_id, signal_date, target_date, theme_id, theme_name, rank, score, signal_type, confidence, timing, stars, proxy_etf, entry_price)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(signal_date, theme_id) DO NOTHING`
  )

  const batch = predictions.map((p) =>
    stmt.bind(
      p.run_id,
      p.signal_date,
      p.target_date,
      p.theme_id,
      p.theme_name,
      p.rank,
      p.score,
      p.signal_type,
      p.confidence,
      p.timing,
      p.stars,
      p.proxy_etf,
      p.entry_price
    )
  )

  await env.SIGNALS_DB.batch(batch)
}

/**
 * Get pending predictions that are ready for evaluation.
 * Returns predictions where target_date <= today and not yet evaluated.
 */
export async function getPendingPredictions(env: Env): Promise<SignalPredictionRow[]> {
  const today = new Date().toISOString().slice(0, 10)
  const res = await env.SIGNALS_DB.prepare(
    `${CANONICAL_PREDICTIONS_CTE}
     SELECT *
     FROM canonical_predictions
     WHERE evaluated_at IS NULL AND target_date <= ?
     ORDER BY target_date ASC, rank ASC`
  )
    .bind(today)
    .all<SignalPredictionRow>()
  return res.results || []
}

/**
 * Update payload for prediction evaluation outcomes.
 */
export interface PredictionOutcomeUpdate {
  exitPrice: number | null
  exitPriceDate: string | null
  returnPct: number | null
  hit: number | null
  evaluationNote: string | null
  evaluatedAt?: string
}

/**
 * Update a prediction with evaluation outcome.
 */
export async function updatePredictionOutcome(
  env: Env,
  id: number,
  outcome: PredictionOutcomeUpdate
): Promise<void> {
  const evaluatedAt = outcome.evaluatedAt ?? new Date().toISOString()
  await env.SIGNALS_DB.prepare(
    `UPDATE signal_predictions
     SET exit_price = ?, exit_price_date = ?, return_pct = ?, hit = ?, evaluation_note = ?, evaluated_at = ?
     WHERE id = ?`
  )
    .bind(
      outcome.exitPrice,
      outcome.exitPriceDate,
      outcome.returnPct,
      outcome.hit,
      outcome.evaluationNote,
      evaluatedAt,
      id
    )
    .run()
}

/**
 * Get accuracy statistics aggregated by timing and confidence.
 */
export interface AccuracyBucketStats {
  total: number
  hits: number
  hit_rate: number
  hit_rate_ci_low: number
  hit_rate_ci_high: number
  avg_return: number
}

export interface AccuracyStats {
  overall: AccuracyBucketStats
  by_timing: Record<string, AccuracyBucketStats>
  by_confidence: Record<string, AccuracyBucketStats>
  evaluated_total: number
  resolved_total: number
  unresolved_total: number
  unresolved_rate: number
}

const WILSON_Z_95 = 1.959963984540054

/**
 * Wilson score interval for a binomial proportion.
 *
 * Returns bounds in [0, 1].
 */
export function computeWilsonInterval(
  hits: number,
  total: number,
  z = WILSON_Z_95
): { low: number; high: number } {
  if (total <= 0) {
    return { low: 0, high: 0 }
  }

  const boundedHits = Math.min(total, Math.max(0, hits))
  const p = boundedHits / total
  const z2 = z * z
  const denominator = 1 + z2 / total
  const center = (p + z2 / (2 * total)) / denominator
  const margin =
    (z * Math.sqrt((p * (1 - p) + z2 / (4 * total)) / total)) / denominator

  return {
    low: Math.max(0, center - margin),
    high: Math.min(1, center + margin),
  }
}

function buildAccuracyBucket(total: number, hits: number, avgReturn: number): AccuracyBucketStats {
  const hitRate = total > 0 ? (hits / total) * 100 : 0
  const ci = computeWilsonInterval(hits, total)

  return {
    total,
    hits,
    hit_rate: hitRate,
    hit_rate_ci_low: ci.low * 100,
    hit_rate_ci_high: ci.high * 100,
    avg_return: avgReturn,
  }
}

export async function getAccuracyStats(env: Env): Promise<AccuracyStats> {
  // Overall stats
  const overallRes = await env.SIGNALS_DB.prepare(
    `${CANONICAL_PREDICTIONS_CTE}
     SELECT 
       COUNT(*) as total,
       COALESCE(SUM(hit), 0) as hits,
       COALESCE(AVG(return_pct), 0) as avg_return
     FROM canonical_predictions
     WHERE evaluated_at IS NOT NULL AND proxy_etf IS NOT NULL AND hit IS NOT NULL`
  ).first<{ total: number; hits: number; avg_return: number }>()

  const overallTotal = Number(overallRes?.total ?? 0)
  const overallHits = Number(overallRes?.hits ?? 0)
  const overallAvgReturn = Number(overallRes?.avg_return ?? 0)

  const overall = buildAccuracyBucket(overallTotal, overallHits, overallAvgReturn)

  // By timing
  const timingRes = await env.SIGNALS_DB.prepare(
    `${CANONICAL_PREDICTIONS_CTE}
     SELECT 
       timing,
       COUNT(*) as total,
       COALESCE(SUM(hit), 0) as hits,
       COALESCE(AVG(return_pct), 0) as avg_return
     FROM canonical_predictions
     WHERE evaluated_at IS NOT NULL AND proxy_etf IS NOT NULL AND hit IS NOT NULL
     GROUP BY timing`
  ).all<{ timing: string; total: number; hits: number; avg_return: number }>()

  const by_timing: AccuracyStats["by_timing"] = {}
  for (const row of timingRes.results || []) {
    by_timing[row.timing] = buildAccuracyBucket(
      Number(row.total),
      Number(row.hits),
      Number(row.avg_return)
    )
  }

  // By confidence
  const confidenceRes = await env.SIGNALS_DB.prepare(
    `${CANONICAL_PREDICTIONS_CTE}
     SELECT 
       confidence,
       COUNT(*) as total,
       COALESCE(SUM(hit), 0) as hits,
       COALESCE(AVG(return_pct), 0) as avg_return
     FROM canonical_predictions
     WHERE evaluated_at IS NOT NULL AND proxy_etf IS NOT NULL AND hit IS NOT NULL
     GROUP BY confidence`
  ).all<{ confidence: string; total: number; hits: number; avg_return: number }>()

  const by_confidence: AccuracyStats["by_confidence"] = {}
  for (const row of confidenceRes.results || []) {
    by_confidence[row.confidence] = buildAccuracyBucket(
      Number(row.total),
      Number(row.hits),
      Number(row.avg_return)
    )
  }

  const completenessRes = await env.SIGNALS_DB.prepare(
    `${CANONICAL_PREDICTIONS_CTE}
     SELECT
       COUNT(*) as evaluated_total,
       COALESCE(SUM(CASE WHEN hit IS NULL THEN 1 ELSE 0 END), 0) as unresolved_total
     FROM canonical_predictions
     WHERE evaluated_at IS NOT NULL AND proxy_etf IS NOT NULL`
  ).first<{ evaluated_total: number; unresolved_total: number }>()

  const evaluatedTotal = Number(completenessRes?.evaluated_total ?? 0)
  const unresolvedTotal = Number(completenessRes?.unresolved_total ?? 0)
  const resolvedTotal = overallTotal
  const unresolvedRate = evaluatedTotal > 0 ? (unresolvedTotal / evaluatedTotal) * 100 : 0

  return {
    overall,
    by_timing,
    by_confidence,
    evaluated_total: evaluatedTotal,
    resolved_total: resolvedTotal,
    unresolved_total: unresolvedTotal,
    unresolved_rate: unresolvedRate,
  }
}

/**
 * List predictions with optional filters.
 */
export async function listPredictions(
  env: Env,
  opts: { limit?: number; evaluated?: boolean } = {}
): Promise<SignalPredictionRow[]> {
  const limit = opts.limit || 50
  let query = `${CANONICAL_PREDICTIONS_CTE} SELECT * FROM canonical_predictions`

  if (opts.evaluated === true) {
    query += ` WHERE evaluated_at IS NOT NULL`
  } else if (opts.evaluated === false) {
    query += ` WHERE evaluated_at IS NULL`
  }

  query += ` ORDER BY signal_date DESC, rank ASC LIMIT ?`

  const res = await env.SIGNALS_DB.prepare(query).bind(limit).all<SignalPredictionRow>()
  return res.results || []
}

/**
 * Returns the set of theme IDs that already have predictions for a signal date.
 *
 * Used to keep prediction storage idempotent across same-day reruns, which
 * prevents duplicate theme/date rows from inflating evaluation statistics.
 */
export async function getExistingPredictionThemesForDate(env: Env, signalDate: string): Promise<Set<string>> {
  const res = await env.SIGNALS_DB.prepare(
    `${CANONICAL_PREDICTIONS_CTE}
     SELECT theme_id
     FROM canonical_predictions
     WHERE signal_date = ?`
  )
    .bind(signalDate)
    .all<{ theme_id: string }>()

  const rows = res.results || []
  return new Set(rows.map((row) => row.theme_id))
}
