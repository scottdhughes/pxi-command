import { describe, it, expect } from "vitest"
import sampleData from "../data/sample_reddit.json" assert { type: "json" }
import type { RedditDataset } from "../src/reddit/types"
import { THEMES } from "../src/analysis/themes"
import { computeMetrics } from "../src/analysis/metrics"
import { scoreThemes } from "../src/analysis/scoring"
import { classifyTheme } from "../src/analysis/classify"

describe("offline pipeline", () => {
  it("produces top 10 themes with evidence", () => {
    const dataset = sampleData as RedditDataset
    const result = computeMetrics(dataset, THEMES, 7, 30, false)
    const scores = scoreThemes(result.metrics)
    const ranked = scores.slice(0, 10).map((s, idx) => {
      const m = result.metrics.find((mm) => mm.theme_id === s.theme_id)!
      const classification = classifyTheme(m, s, idx + 1, 10)
      return { m, classification }
    })
    expect(ranked.length).toBe(10)
    for (const r of ranked) {
      expect(r.m.evidence_links.length).toBeGreaterThanOrEqual(3)
      expect(r.classification.signal_type.length).toBeGreaterThan(0)
    }
  })
})
