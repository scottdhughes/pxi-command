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

export async function listRuns(env: Env, limit = 20): Promise<RunRow[]> {
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


// ─────────────────────────────────────────────────────────────────────────────
// Signal Predictions
// ─────────────────────────────────────────────────────────────────────────────

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
  return_pct: number | null
  evaluated_at: string | null
  hit: number | null
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
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
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
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
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
    `SELECT * FROM signal_predictions 
     WHERE evaluated_at IS NULL AND target_date <= ?
     ORDER BY target_date ASC, rank ASC`
  )
    .bind(today)
    .all<SignalPredictionRow>()
  return res.results || []
}

/**
 * Update a prediction with evaluation outcome.
 */
export async function updatePredictionOutcome(
  env: Env,
  id: number,
  exitPrice: number | null,
  returnPct: number | null,
  hit: number | null
): Promise<void> {
  const evaluatedAt = new Date().toISOString()
  await env.SIGNALS_DB.prepare(
    `UPDATE signal_predictions 
     SET exit_price = ?, return_pct = ?, hit = ?, evaluated_at = ?
     WHERE id = ?`
  )
    .bind(exitPrice, returnPct, hit, evaluatedAt, id)
    .run()
}

/**
 * Get accuracy statistics aggregated by timing and confidence.
 */
export interface AccuracyStats {
  overall: {
    total: number
    hits: number
    hit_rate: number
    avg_return: number
  }
  by_timing: Record<string, { total: number; hits: number; hit_rate: number; avg_return: number }>
  by_confidence: Record<string, { total: number; hits: number; hit_rate: number; avg_return: number }>
}

export async function getAccuracyStats(env: Env): Promise<AccuracyStats> {
  // Overall stats
  const overallRes = await env.SIGNALS_DB.prepare(
    `SELECT 
       COUNT(*) as total,
       COALESCE(SUM(hit), 0) as hits,
       COALESCE(AVG(return_pct), 0) as avg_return
     FROM signal_predictions 
     WHERE evaluated_at IS NOT NULL AND proxy_etf IS NOT NULL`
  ).first<{ total: number; hits: number; avg_return: number }>()

  const overall = {
    total: overallRes?.total || 0,
    hits: overallRes?.hits || 0,
    hit_rate: overallRes?.total ? (overallRes.hits / overallRes.total) * 100 : 0,
    avg_return: overallRes?.avg_return || 0,
  }

  // By timing
  const timingRes = await env.SIGNALS_DB.prepare(
    `SELECT 
       timing,
       COUNT(*) as total,
       COALESCE(SUM(hit), 0) as hits,
       COALESCE(AVG(return_pct), 0) as avg_return
     FROM signal_predictions 
     WHERE evaluated_at IS NOT NULL AND proxy_etf IS NOT NULL
     GROUP BY timing`
  ).all<{ timing: string; total: number; hits: number; avg_return: number }>()

  const by_timing: AccuracyStats["by_timing"] = {}
  for (const row of timingRes.results || []) {
    by_timing[row.timing] = {
      total: row.total,
      hits: row.hits,
      hit_rate: row.total ? (row.hits / row.total) * 100 : 0,
      avg_return: row.avg_return,
    }
  }

  // By confidence
  const confidenceRes = await env.SIGNALS_DB.prepare(
    `SELECT 
       confidence,
       COUNT(*) as total,
       COALESCE(SUM(hit), 0) as hits,
       COALESCE(AVG(return_pct), 0) as avg_return
     FROM signal_predictions 
     WHERE evaluated_at IS NOT NULL AND proxy_etf IS NOT NULL
     GROUP BY confidence`
  ).all<{ confidence: string; total: number; hits: number; avg_return: number }>()

  const by_confidence: AccuracyStats["by_confidence"] = {}
  for (const row of confidenceRes.results || []) {
    by_confidence[row.confidence] = {
      total: row.total,
      hits: row.hits,
      hit_rate: row.total ? (row.hits / row.total) * 100 : 0,
      avg_return: row.avg_return,
    }
  }

  return { overall, by_timing, by_confidence }
}

/**
 * List predictions with optional filters.
 */
export async function listPredictions(
  env: Env,
  opts: { limit?: number; evaluated?: boolean } = {}
): Promise<SignalPredictionRow[]> {
  const limit = opts.limit || 50
  let query = `SELECT * FROM signal_predictions`

  if (opts.evaluated === true) {
    query += ` WHERE evaluated_at IS NOT NULL`
  } else if (opts.evaluated === false) {
    query += ` WHERE evaluated_at IS NULL`
  }

  query += ` ORDER BY signal_date DESC, rank ASC LIMIT ?`

  const res = await env.SIGNALS_DB.prepare(query).bind(limit).all<SignalPredictionRow>()
  return res.results || []
}
