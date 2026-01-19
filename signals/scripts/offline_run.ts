import { mkdir, writeFile } from "fs/promises"
import path from "path"
import sampleData from "../data/sample_reddit.json" assert { type: "json" }
import type { RedditDataset } from "../src/reddit/types"
import { THEMES } from "../src/analysis/themes"
import { computeMetrics } from "../src/analysis/metrics"
import { scoreThemes } from "../src/analysis/scoring"
import { classifyTheme } from "../src/analysis/classify"
import { buildTakeaways } from "../src/analysis/takeaways"
import { renderJson } from "../src/report/render_json"
import { renderHtml } from "../src/report/render_html"
import { nowUtcIso } from "../src/utils/time"

const OUT_DIR = path.join(process.cwd(), "out", "offline")

async function run() {
  const dataset = sampleData as RedditDataset
  const lookbackDays = 7
  const baselineDays = 30
  const topN = 10

  const metricResult = computeMetrics(dataset, THEMES, lookbackDays, baselineDays, false)
  const scores = scoreThemes(metricResult.metrics)

  const eligible = scores.filter((s) => {
    const m = metricResult.metrics.find((mm) => mm.theme_id === s.theme_id)
    return m ? m.evidence_links.length >= 3 : false
  })
  if (eligible.length < topN) {
    throw new Error("insufficient_evidence")
  }
  const ranked = eligible.slice(0, topN).map((s, idx) => {
    const m = metricResult.metrics.find((mm) => mm.theme_id === s.theme_id)!
    const classification = classifyTheme(m, s, idx + 1, topN)
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
    "offline-demo",
    nowUtcIso(),
    {
      lookback_days: lookbackDays,
      baseline_days: baselineDays,
      top_n: topN,
      price_provider: "none",
      enable_comments: false,
      enable_rss: false,
    },
    metricResult.docs.length,
    ranked
  )

  const html = renderHtml(reportJson, takeaways)

  await mkdir(OUT_DIR, { recursive: true })
  await writeFile(path.join(OUT_DIR, "report.html"), html)
  await writeFile(path.join(OUT_DIR, "results.json"), JSON.stringify(reportJson, null, 2))

  console.log("Offline report generated in out/offline/")
}

run().catch((err) => {
  console.error(err)
  process.exit(1)
})
