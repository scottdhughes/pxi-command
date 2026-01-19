import { describe, it, expect } from "vitest"
import sampleData from "../data/sample_reddit.json" assert { type: "json" }
import type { RedditDataset } from "../src/reddit/types"
import { THEMES } from "../src/analysis/themes"
import { computeMetrics } from "../src/analysis/metrics"
import { scoreThemes } from "../src/analysis/scoring"

describe("scoring", () => {
  it("produces stable ranking length and non-NaN scores", () => {
    const dataset = sampleData as RedditDataset
    const result = computeMetrics(dataset, THEMES, 7, 30, false)
    const scores = scoreThemes(result.metrics)
    expect(scores.length).toBe(THEMES.length)
    for (const s of scores) {
      expect(Number.isNaN(s.score)).toBe(false)
    }
    for (let i = 1; i < scores.length; i++) {
      expect(scores[i - 1].score).toBeGreaterThanOrEqual(scores[i].score)
    }
  })
})
