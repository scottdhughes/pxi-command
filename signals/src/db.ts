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
