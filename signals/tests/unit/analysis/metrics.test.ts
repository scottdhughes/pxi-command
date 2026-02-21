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
    expect(m.growth_ratio_capped).toBe(true)
  })

  it("marks growth_ratio_capped=false when ratio is within cap", () => {
    const now = 1_700_000_000

    const dataset: RedditDataset = {
      generated_at_utc: new Date(now * 1000).toISOString(),
      subreddits: ["stocks"],
      posts: [
        {
          id: "p1",
          subreddit: "stocks",
          created_utc: now,
          title: "Solar demand improving",
          selftext: "energy and grid modernization",
          permalink: "/r/stocks/comments/p1",
          score: 5,
          num_comments: 0,
        },
        {
          id: "p2",
          subreddit: "stocks",
          created_utc: now - 10 * 24 * 3600,
          title: "Solar names still discussed",
          selftext: "energy policy",
          permalink: "/r/stocks/comments/p2",
          score: 3,
          num_comments: 0,
        },
      ],
    }

    const themes: ThemeDefinition[] = [
      {
        theme_id: "solar",
        display_name: "Solar",
        keywords: ["solar"],
        seed_tickers: [],
      },
    ]

    const result = computeMetrics(dataset, themes, 7, 30, false)
    expect(result.metrics).toHaveLength(1)
    expect(result.metrics[0].growth_ratio_capped).toBe(false)
  })

  it("uses word-boundary matching for short keywords", () => {
    const now = 1_700_000_000
    const themes: ThemeDefinition[] = [
      {
        theme_id: "midstream",
        display_name: "Midstream",
        keywords: ["lng"],
        seed_tickers: [],
      },
    ]

    const falsePositiveDataset: RedditDataset = {
      generated_at_utc: new Date(now * 1000).toISOString(),
      subreddits: ["stocks"],
      posts: [
        {
          id: "p1",
          subreddit: "stocks",
          created_utc: now,
          title: "Sling TV update",
          selftext: "No energy discussion here",
          permalink: "/r/stocks/comments/p1",
          score: 5,
          num_comments: 0,
        },
      ],
    }

    const truePositiveDataset: RedditDataset = {
      generated_at_utc: new Date(now * 1000).toISOString(),
      subreddits: ["stocks"],
      posts: [
        {
          id: "p2",
          subreddit: "stocks",
          created_utc: now,
          title: "LNG exports keep growing",
          selftext: "Terminal buildout discussion",
          permalink: "/r/stocks/comments/p2",
          score: 5,
          num_comments: 0,
        },
      ],
    }

    const falsePositiveResult = computeMetrics(falsePositiveDataset, themes, 7, 30, false)
    const truePositiveResult = computeMetrics(truePositiveDataset, themes, 7, 30, false)

    expect(falsePositiveResult.metrics[0].mentions_L).toBe(0)
    expect(truePositiveResult.metrics[0].mentions_L).toBe(1)
  })

  it("sanitizes key_tickers and removes stopword-like jargon tokens", () => {
    const now = 1_700_000_000
    const dataset: RedditDataset = {
      generated_at_utc: new Date(now * 1000).toISOString(),
      subreddits: ["stocks"],
      posts: [
        {
          id: "p1",
          subreddit: "stocks",
          created_utc: now,
          title: "GPT and LLM chatter around NVDA",
          selftext: "GPU demand remains strong for NVDA",
          permalink: "/r/stocks/comments/p1",
          score: 5,
          num_comments: 0,
        },
      ],
    }

    const themes: ThemeDefinition[] = [
      {
        theme_id: "ai_compute",
        display_name: "AI Compute",
        keywords: ["ai"],
        seed_tickers: ["GPT", "NVDA"],
      },
    ]

    const result = computeMetrics(dataset, themes, 7, 30, false)
    expect(result.metrics).toHaveLength(1)
    const keyTickers = result.metrics[0].key_tickers
    expect(keyTickers).toContain("NVDA")
    expect(keyTickers).not.toContain("GPT")
    expect(keyTickers).not.toContain("LLM")
    expect(keyTickers).not.toContain("GPU")
  })
})
