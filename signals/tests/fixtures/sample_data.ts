/**
 * Sample data fixtures for testing analysis modules.
 */

import type { Doc } from "../../src/analysis/metrics"
import type { ThemeMetrics } from "../../src/analysis/metrics"
import type { ThemeScore } from "../../src/analysis/scoring"
import type { ThemeClassification } from "../../src/analysis/classify"

/**
 * Creates a sample document for testing.
 */
export function createDoc(overrides: Partial<Doc> = {}): Doc {
  const now = Math.floor(Date.now() / 1000)
  return {
    id: "test-doc-1",
    subreddit: "stocks",
    created_utc: now - 86400, // 1 day ago
    text: "Sample text about $TSLA and the stock market",
    permalink: "https://reddit.com/r/stocks/test",
    score: 10,
    num_comments: 5,
    post_id: "test-post-1",
    is_comment: false,
    ...overrides,
  }
}

/**
 * Creates multiple documents with varying timestamps.
 */
export function createDocsWithTimestamps(count: number, baseTsSeconds: number, intervalSeconds = 3600): Doc[] {
  return Array.from({ length: count }, (_, i) =>
    createDoc({
      id: `doc-${i}`,
      created_utc: baseTsSeconds - (i * intervalSeconds),
      text: `Post ${i} about $TSLA and investing`,
      post_id: `post-${i}`,
    })
  )
}

/**
 * Creates sample ThemeMetrics for testing classify and takeaways.
 */
export function createThemeMetrics(overrides: Partial<ThemeMetrics> = {}): ThemeMetrics {
  return {
    theme_id: "test-theme",
    theme_name: "Test Theme",
    mentions_L: 15,
    mentions_B: 8,
    current_rate: 2.14,
    baseline_rate: 1.14,
    growth_ratio: 1.875,
    growth_ratio_capped: false,
    daily_counts_L: [2, 2, 1, 3, 2, 3, 2],
    slope: 0.3,
    current_sent: 0.65,
    baseline_sent: 0.55,
    sentiment_shift: 0.1,
    unique_subreddits: 4,
    concentration: 0.35,
    confirmation_score: 0.8,
    key_tickers: ["TSLA", "NVDA", "AMD"],
    evidence_links: [
      "https://reddit.com/r/stocks/1",
      "https://reddit.com/r/investing/2",
      "https://reddit.com/r/wallstreetbets/3",
    ],
    momentum_score: null,
    divergence_score: null,
    price_available: true,
    ...overrides,
  }
}

/**
 * Creates sample ThemeScore for testing.
 */
export function createThemeScore(overrides: Partial<ThemeScore> = {}): ThemeScore {
  return {
    theme_id: "test-theme",
    theme_name: "Test Theme",
    score: 0.75,
    components: {
      velocity: 1.2,
      sentiment_shift: 0.8,
      confirmation: 1.0,
      price: 0,
    },
    raw: {
      velocity: 1.5,
      sentiment_shift: 0.1,
      confirmation: 0.8,
      price: 0,
    },
    ...overrides,
  }
}

/**
 * Creates sample ThemeClassification for testing.
 */
export function createThemeClassification(overrides: Partial<ThemeClassification> = {}): ThemeClassification {
  return {
    signal_type: "Rotation",
    confidence: "High",
    timing: "Now",
    stars: 4,
    ...overrides,
  }
}

/**
 * High-confidence metrics (all thresholds met).
 */
export const highConfidenceMetrics = createThemeMetrics({
  mentions_L: 20,
  unique_subreddits: 5,
  concentration: 0.3,
  price_available: true,
  growth_ratio: 2.5,
  slope: 0.4,
})

/**
 * Low-confidence metrics (few thresholds met).
 */
export const lowConfidenceMetrics = createThemeMetrics({
  mentions_L: 3,
  unique_subreddits: 1,
  concentration: 0.8,
  price_available: false,
  growth_ratio: 0.8,
  slope: 0.05,
})

/**
 * Metrics indicating "Building" timing.
 */
export const buildingTimingMetrics = createThemeMetrics({
  growth_ratio: 1.5,
  slope: 0.15,
  concentration: 0.4,
})

/**
 * Metrics indicating mean reversion signal type.
 */
export const meanReversionMetrics = createThemeMetrics({
  momentum_score: null,
  divergence_score: null,
  sentiment_shift: -0.6,
  growth_ratio: 0.7,
})
