import { describe, it, expect } from "vitest"
import { computeMetrics } from "../../../src/analysis/metrics"
import { GROWTH_RATIO_CAP } from "../../../src/analysis/constants"
import type { RedditDataset } from "../../../src/reddit/types"
import type { ThemeDefinition } from "../../../src/analysis/themes"

describe("computeMetrics", () => {
  it("caps growth_ratio when baseline rate is ~0", () => {
    const now = 1_700_000_000 // deterministic timestamp

    const dataset: RedditDataset = {
      generated_at_utc: new Date(now * 1000).toISOString(),
      subreddits: ["wallstreetbets"],
      posts: [
        {
          id: "p1",
          subreddit: "wallstreetbets",
          created_utc: now,
          title: "Defense names are moving",
          selftext: "defense contractors discussion",
          permalink: "/r/wallstreetbets/comments/p1",
          score: 1,
          num_comments: 0,
        },
      ],
    }

    const themes: ThemeDefinition[] = [
      {
        theme_id: "defense",
        display_name: "Defense",
        keywords: ["defense"],
        seed_tickers: [],
      },
    ]

    const result = computeMetrics(dataset, themes, 7, 30, false)
    expect(result.metrics).toHaveLength(1)

    const m = result.metrics[0]
    expect(m.mentions_L).toBe(1)
    expect(m.mentions_B).toBe(0)
    expect(m.baseline_rate).toBe(0)
    expect(m.growth_ratio).toBe(GROWTH_RATIO_CAP)
  })
})
