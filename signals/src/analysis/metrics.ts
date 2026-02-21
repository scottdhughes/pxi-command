import type { RedditDataset } from "../reddit/types"
import type { ThemeDefinition } from "./themes"
import { scoreSentiment } from "./sentiment"
import { extractTickers, STOPLIST } from "./tickers"
import {
  SECONDS_PER_DAY,
  RATE_EPSILON,
  GROWTH_RATIO_CAP,
  LIMITS,
  CONFIRMATION_SUBREDDIT_DIVISOR,
} from "./constants"

export interface Doc {
  id: string
  subreddit: string
  created_utc: number
  text: string
  permalink: string
  score: number
  num_comments: number
  post_id: string
  is_comment: boolean
}

export interface ThemeMetrics {
  theme_id: string
  theme_name: string
  mentions_L: number
  mentions_B: number
  current_rate: number
  baseline_rate: number
  growth_ratio: number
  growth_ratio_capped: boolean
  daily_counts_L: number[]
  slope: number
  current_sent: number
  baseline_sent: number
  sentiment_shift: number
  unique_subreddits: number
  concentration: number
  confirmation_score: number
  key_tickers: string[]
  evidence_links: string[]
  momentum_score: number | null
  divergence_score: number | null
  price_available: boolean
}

export interface MetricsResult {
  docs: Doc[]
  metrics: ThemeMetrics[]
  lookbackDays: number
  baselineDays: number
  windowEndUtc: number
}

/**
 * Calculates the slope of a linear regression line through the given values.
 * Used to detect acceleration/deceleration in mention velocity over time.
 *
 * @param values - Array of daily mention counts (index = day number)
 * @returns Slope coefficient (positive = accelerating, negative = decelerating)
 */
function linearRegressionSlope(values: number[]): number {
  const n = values.length
  if (n === 0) return 0
  let sumX = 0
  let sumY = 0
  let sumXY = 0
  let sumXX = 0
  for (let i = 0; i < n; i++) {
    const x = i
    const y = values[i]
    sumX += x
    sumY += y
    sumXY += x * y
    sumXX += x * x
  }
  const denom = n * sumXX - sumX * sumX
  if (denom === 0) return 0
  return (n * sumXY - sumX * sumY) / denom
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
}

function buildKeywordRegex(keyword: string): RegExp {
  const escaped = escapeRegex(keyword.trim().toLowerCase())
  return new RegExp(`(^|[^a-z0-9])${escaped}([^a-z0-9]|$)`, "i")
}

function isValidTickerToken(value: string): boolean {
  return /^[A-Z]{2,5}$/.test(value) && !STOPLIST.has(value)
}

function buildDocs(dataset: RedditDataset, includeComments: boolean): Doc[] {
  const docs: Doc[] = []
  for (const post of dataset.posts) {
    const postDoc: Doc = {
      id: post.id,
      subreddit: post.subreddit,
      created_utc: post.created_utc,
      text: `${post.title}\n${post.selftext}`.trim(),
      permalink: post.permalink,
      score: post.score,
      num_comments: post.num_comments,
      post_id: post.id,
      is_comment: false,
    }
    docs.push(postDoc)
    if (includeComments && post.comments) {
      for (const c of post.comments) {
        docs.push({
          id: c.id,
          subreddit: post.subreddit,
          created_utc: c.created_utc,
          text: c.body,
          permalink: c.permalink,
          score: 0,
          num_comments: 0,
          post_id: post.id,
          is_comment: true,
        })
      }
    }
  }
  return docs
}

function withinWindow(ts: number, start: number, end: number) {
  return ts >= start && ts <= end
}

function dayIndex(ts: number, start: number): number {
  return Math.floor((ts - start) / SECONDS_PER_DAY)
}

function pickEvidenceLinks(docs: Doc[], maxLinks: number): string[] {
  const posts = docs.filter((d) => !d.is_comment).sort((a, b) => (b.score - a.score) || (b.created_utc - a.created_utc))
  const comments = docs.filter((d) => d.is_comment).sort((a, b) => b.created_utc - a.created_utc)
  const ordered = [...posts, ...comments]
  const links: string[] = []
  for (const d of ordered) {
    if (!links.includes(d.permalink)) {
      links.push(d.permalink)
    }
    if (links.length >= maxLinks) break
  }
  return links
}

/**
 * Computes comprehensive metrics for each investment theme.
 *
 * This is the core analysis function that processes Reddit data and calculates:
 * - Mention velocity (growth ratio + linear regression slope)
 * - Sentiment shift (VADER compound difference between periods)
 * - Confirmation score (subreddit diversity minus concentration penalty)
 * - Key tickers and evidence links for each theme
 *
 * @param dataset - Reddit posts/comments to analyze
 * @param themes - Investment themes with keywords and seed tickers
 * @param lookbackDays - Recent period window (default: 7 days)
 * @param baselineDays - Historical comparison window (default: 30 days)
 * @param includeComments - Whether to include comment text in analysis
 * @returns MetricsResult containing processed docs and per-theme metrics
 * @throws Error with message "no_docs" if dataset produces no documents
 *
 * @example
 * ```typescript
 * const result = computeMetrics(dataset, THEMES, 7, 30, false);
 * console.log(result.metrics[0].growth_ratio); // e.g., 2.5
 * ```
 */
export function computeMetrics(dataset: RedditDataset, themes: ThemeDefinition[], lookbackDays: number, baselineDays: number, includeComments: boolean): MetricsResult {
  const docs = buildDocs(dataset, includeComments)
  if (docs.length === 0) {
    throw new Error("no_docs")
  }
  const windowEndUtc = Math.max(...docs.map((d) => d.created_utc))
  const lookbackStart = windowEndUtc - lookbackDays * SECONDS_PER_DAY
  const baselineStart = lookbackStart - baselineDays * SECONDS_PER_DAY

  const { perDoc } = extractTickers(docs)

  const metrics: ThemeMetrics[] = []

  for (const theme of themes) {
    const keywordRegexes = theme.keywords.map((k) => buildKeywordRegex(k))
    const seedTickers = new Set(theme.seed_tickers.map((t) => t.toUpperCase()))

    const mentionsL: Doc[] = []
    const mentionsB: Doc[] = []

    for (const doc of docs) {
      const textLower = doc.text.toLowerCase()
      const tickers = perDoc.get(doc.id) || []
      const hasKeyword = keywordRegexes.some((re) => re.test(textLower))
      const hasSeedTicker = tickers.some((t) => seedTickers.has(t))
      const hasCooccurringTicker = hasKeyword && tickers.length > 0

      const isMention = hasKeyword || hasSeedTicker || hasCooccurringTicker
      if (!isMention) continue

      if (withinWindow(doc.created_utc, lookbackStart, windowEndUtc)) {
        mentionsL.push(doc)
      } else if (withinWindow(doc.created_utc, baselineStart, lookbackStart)) {
        mentionsB.push(doc)
      }
    }

    const dailyCounts = Array.from({ length: lookbackDays }, () => 0)
    for (const doc of mentionsL) {
      const idx = dayIndex(doc.created_utc, lookbackStart)
      if (idx >= 0 && idx < dailyCounts.length) {
        dailyCounts[idx] += 1
      }
    }

    const currentRate = mentionsL.length / Math.max(lookbackDays, 1)
    const baselineRate = mentionsB.length / Math.max(baselineDays, 1)
    const growthRatioUncapped = (currentRate + RATE_EPSILON) / (baselineRate + RATE_EPSILON)
    const growthRatio = Math.min(growthRatioUncapped, GROWTH_RATIO_CAP)
    const growthRatioCapped = growthRatioUncapped > GROWTH_RATIO_CAP
    const slope = linearRegressionSlope(dailyCounts)

    const currentSent = mentionsL.length
      ? mentionsL.reduce((a, d) => a + scoreSentiment(d.text), 0) / mentionsL.length
      : 0
    const baselineSent = mentionsB.length
      ? mentionsB.reduce((a, d) => a + scoreSentiment(d.text), 0) / mentionsB.length
      : 0
    const sentimentShift = currentSent - baselineSent

    const uniqueSubreddits = new Set(mentionsL.map((d) => d.subreddit)).size

    const mentionsByPost = new Map<string, number>()
    for (const doc of mentionsL) {
      const key = doc.post_id
      mentionsByPost.set(key, (mentionsByPost.get(key) || 0) + 1)
    }
    const mentionCounts = Array.from(mentionsByPost.values()).sort((a, b) => b - a)
    const topN = mentionCounts.slice(0, LIMITS.topPostsForConcentration).reduce((a, b) => a + b, 0)
    const concentration = mentionsL.length ? topN / mentionsL.length : 0

    const confirmationScore = Math.max(0, Math.min(1, (uniqueSubreddits / CONFIRMATION_SUBREDDIT_DIVISOR) - concentration))

    const tickersForTheme = new Set<string>()
    for (const doc of mentionsL) {
      const tks = perDoc.get(doc.id) || []
      for (const t of tks) tickersForTheme.add(t)
    }
    for (const t of theme.seed_tickers) tickersForTheme.add(String(t || "").trim().toUpperCase())

    const keyTickers = Array.from(tickersForTheme)
      .map((ticker) => ticker.trim().toUpperCase())
      .filter((ticker) => isValidTickerToken(ticker))
      .slice(0, LIMITS.maxTickers)

    const evidenceLinks = pickEvidenceLinks([...mentionsL, ...mentionsB], LIMITS.maxEvidenceLinks)

    metrics.push({
      theme_id: theme.theme_id,
      theme_name: theme.display_name,
      mentions_L: mentionsL.length,
      mentions_B: mentionsB.length,
      current_rate: currentRate,
      baseline_rate: baselineRate,
      growth_ratio: growthRatio,
      growth_ratio_capped: growthRatioCapped,
      daily_counts_L: dailyCounts,
      slope,
      current_sent: currentSent,
      baseline_sent: baselineSent,
      sentiment_shift: sentimentShift,
      unique_subreddits: uniqueSubreddits,
      concentration,
      confirmation_score: confirmationScore,
      key_tickers: keyTickers,
      evidence_links: evidenceLinks.slice(0, LIMITS.maxEvidenceLinks),
      momentum_score: null,
      divergence_score: null,
      price_available: false,
    })
  }

  return { docs, metrics, lookbackDays, baselineDays, windowEndUtc }
}
