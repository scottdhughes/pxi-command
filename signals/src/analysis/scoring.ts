import { zScore } from "./normalize"
import type { ThemeMetrics } from "./metrics"
import { WEIGHTS } from "./constants"

export interface ThemeScore {
  theme_id: string
  theme_name: string
  score: number
  components: {
    velocity: number
    sentiment_shift: number
    confirmation: number
    price: number
  }
  raw: {
    velocity: number
    sentiment_shift: number
    confirmation: number
    price: number
  }
}

/**
 * Scores and ranks themes using z-score normalization across components.
 *
 * The scoring algorithm:
 * 1. Computes raw values for velocity, sentiment, confirmation, and price
 * 2. Normalizes each component using z-scores (mean=0, std=1)
 * 3. Applies weighted combination (velocity 40%, sentiment 20%, confirmation 30%, price 10%)
 * 4. If no price data is available, redistributes the 10% proportionally
 * 5. Sorts themes by final composite score (descending)
 *
 * @param metrics - Array of ThemeMetrics from computeMetrics()
 * @returns Array of ThemeScore objects sorted by composite score (highest first)
 *
 * @example
 * ```typescript
 * const scores = scoreThemes(metrics);
 * const topTheme = scores[0];
 * console.log(topTheme.theme_name, topTheme.score);
 * ```
 */
const MIN_GROWTH_RATIO_FOR_LOG = 1e-6

function safeLogGrowthRatio(growthRatio: number): number {
  if (!Number.isFinite(growthRatio)) return Math.log(MIN_GROWTH_RATIO_FOR_LOG)
  return Math.log(Math.max(growthRatio, MIN_GROWTH_RATIO_FOR_LOG))
}

export function scoreThemes(metrics: ThemeMetrics[]) {
  const velocityRaw = metrics.map((m) => safeLogGrowthRatio(m.growth_ratio) + m.slope)
  const sentimentRaw = metrics.map((m) => m.sentiment_shift)
  const confirmationRaw = metrics.map((m) => m.confirmation_score)
  const priceRaw = metrics.map((m) => (m.momentum_score || 0) + (m.divergence_score || 0))

  const velocityZ = zScore(velocityRaw)
  const sentimentZ = zScore(sentimentRaw)
  const confirmationZ = zScore(confirmationRaw)
  const priceZ = zScore(priceRaw)

  const anyPrice = metrics.some((m) => m.price_available)
  const weightPrice = anyPrice ? WEIGHTS.price : 0
  const weightTotal = WEIGHTS.velocity + WEIGHTS.sentiment + WEIGHTS.confirmation + weightPrice

  const scores: ThemeScore[] = metrics.map((m, i) => {
    const score =
      (WEIGHTS.velocity * velocityZ[i] +
        WEIGHTS.sentiment * sentimentZ[i] +
        WEIGHTS.confirmation * confirmationZ[i] +
        weightPrice * priceZ[i]) /
      weightTotal

    return {
      theme_id: m.theme_id,
      theme_name: m.theme_name,
      score,
      components: {
        velocity: velocityZ[i],
        sentiment_shift: sentimentZ[i],
        confirmation: confirmationZ[i],
        price: priceZ[i],
      },
      raw: {
        velocity: velocityRaw[i],
        sentiment_shift: sentimentRaw[i],
        confirmation: confirmationRaw[i],
        price: priceRaw[i],
      },
    }
  })

  scores.sort((a, b) => b.score - a.score)
  return scores
}
