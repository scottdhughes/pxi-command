import type { Env } from "./config"
import { getConfig } from "./config"
import { fetchRedditDataset } from "./reddit/reddit_client"
import type { RedditDataset } from "./reddit/types"
import { THEMES } from "./analysis/themes"
import { computeMetrics } from "./analysis/metrics"
import { scoreThemes } from "./analysis/scoring"
import { classifyTheme } from "./analysis/classify"
import { buildTakeaways } from "./analysis/takeaways"
import { renderJson, type ThemeReportItem } from "./report/render_json"
import { renderHtml } from "./report/render_html"
import { insertRun } from "./db"
import { putObject, setLatestRun } from "./storage"
import { nowUtcIso } from "./utils/time"
import { ulid } from "ulidx"

const DEFAULT_SUBREDDITS = [
  "stocks",
  "investing",
  "wallstreetbets",
  "energy",
  "space",
]

export interface PipelineResult {
  runId: string
  reportHtml: string
  resultsJson: string
  rawJson?: string
}

export async function runPipeline(env: Env, opts: { dataset?: RedditDataset } = {}): Promise<PipelineResult> {
  const cfg = getConfig(env)
  const runId = `${new Date().toISOString().slice(0, 10).replace(/-/g, "")}-${ulid()}`
  const generatedAt = nowUtcIso()

  let dataset: RedditDataset
  if (opts.dataset) {
    dataset = opts.dataset
  } else {
    dataset = await fetchRedditDataset(env, DEFAULT_SUBREDDITS)
  }

  const metricResult = computeMetrics(dataset, THEMES, cfg.lookbackDays, cfg.baselineDays, cfg.enableComments)
  const scores = scoreThemes(metricResult.metrics)

  const eligible = scores.filter((s) => {
    const m = metricResult.metrics.find((mm) => mm.theme_id === s.theme_id)
    return m ? m.evidence_links.length >= 3 : false
  })

  if (eligible.length < cfg.topN) {
    throw new Error("insufficient_evidence")
  }

  const ranked: ThemeReportItem[] = eligible.slice(0, cfg.topN).map((s, idx) => {
      const m = metricResult.metrics.find((mm) => mm.theme_id === s.theme_id)
      if (!m) throw new Error("Missing metrics for theme")
      const classification = classifyTheme(m, s, idx + 1, cfg.topN)
      return {
        rank: idx + 1,
        theme_id: s.theme_id,
        theme_name: s.theme_name,
        score: s.score,
        classification,
        metrics: m,
        scoring: s,
        evidence_links: m.evidence_links,
        key_tickers: m.key_tickers,
      }
    })

  const takeaways = buildTakeaways(ranked.map((r) => ({ metrics: r.metrics, score: r.scoring, classification: r.classification })))
  const reportJson = renderJson(
    runId,
    generatedAt,
    {
      lookback_days: cfg.lookbackDays,
      baseline_days: cfg.baselineDays,
      top_n: cfg.topN,
      price_provider: cfg.priceProvider,
      enable_comments: cfg.enableComments,
      enable_rss: cfg.enableRss,
    },
    metricResult.docs.length,
    ranked
  )
  const reportHtml = renderHtml(reportJson, takeaways)

  const reportKey = `reports/${runId}/report.html`
  const resultsKey = `reports/${runId}/results.json`
  const rawKey = `reports/${runId}/raw.json`

  const resultsJson = JSON.stringify(reportJson, null, 2)
  const rawJson = JSON.stringify(dataset, null, 2)

  await putObject(env, reportKey, reportHtml, "text/html; charset=utf-8")
  await putObject(env, resultsKey, resultsJson, "application/json")
  await putObject(env, rawKey, rawJson, "application/json")

  await insertRun(env, {
    id: runId,
    created_at_utc: generatedAt,
    lookback_days: cfg.lookbackDays,
    baseline_days: cfg.baselineDays,
    status: "ok",
    summary_json: JSON.stringify({
      top_themes: ranked.map((r) => ({ rank: r.rank, theme: r.theme_name, score: r.score })),
      total_docs: metricResult.docs.length,
    }),
    report_html_key: reportKey,
    results_json_key: resultsKey,
    raw_json_key: rawKey,
    error_message: null,
  })

  await setLatestRun(env, runId, generatedAt)

  return { runId, reportHtml, resultsJson, rawJson }
}
