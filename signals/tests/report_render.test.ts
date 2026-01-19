import { describe, it, expect } from "vitest"
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

describe("report rendering", () => {
  it("includes required sections", () => {
    const dataset = sampleData as RedditDataset
    const result = computeMetrics(dataset, THEMES, 7, 30, false)
    const scores = scoreThemes(result.metrics)
    const ranked = scores.slice(0, 10).map((s, idx) => {
      const m = result.metrics.find((mm) => mm.theme_id === s.theme_id)!
      const classification = classifyTheme(m, s, idx + 1, 10)
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
        lookback_days: 7,
        baseline_days: 30,
        top_n: 10,
        price_provider: "none",
        enable_comments: false,
        enable_rss: false,
      },
      result.docs.length,
      ranked
    )
    const html = renderHtml(reportJson, takeaways)
    expect(html).toContain("Signal Distribution")
    expect(html).toContain("Key Takeaways")
    expect(html).toContain("Not investment advice.")
    expect(html).toContain("Top Signal")
  })
})
