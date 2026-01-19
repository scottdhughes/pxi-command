import type { ThemeMetrics } from "./metrics"
import type { ThemeScore } from "./scoring"
import {
  CONFIDENCE_THRESHOLDS,
  TIMING_THRESHOLDS,
  SIGNAL_TYPE_THRESHOLDS,
} from "./constants"

export interface ThemeClassification {
  signal_type: string
  confidence: string
  timing: string
  stars: number
}

/**
 * Classifies a theme's signal characteristics for display in reports.
 *
 * Classification dimensions:
 * - **Signal Type**: Rotation (default), Momentum, Divergence, or Mean Reversion
 * - **Confidence**: Very High (4 pts), High (3), Medium-High (2), Medium (1), Medium-Low (0)
 * - **Timing**: Now, Now (volatile), Building, Ongoing, or Early
 * - **Stars**: 1-5 rating based on relative rank position
 *
 * Confidence points are earned for:
 * - 8+ mentions in lookback period
 * - 3+ unique subreddits
 * - Concentration ≤ 50%
 * - Price data available
 *
 * @param m - Theme metrics from computeMetrics()
 * @param s - Theme score from scoreThemes()
 * @param rank - 1-based rank position (1 = highest scoring)
 * @param total - Total number of ranked themes
 * @returns ThemeClassification with signal_type, confidence, timing, and stars
 */
export function classifyTheme(m: ThemeMetrics, s: ThemeScore, rank: number, total: number): ThemeClassification {
  let signalType = "Rotation"
  if (m.momentum_score !== null && m.divergence_score !== null) {
    signalType = m.momentum_score >= m.divergence_score ? "Momentum" : "Divergence"
  } else if (m.sentiment_shift < SIGNAL_TYPE_THRESHOLDS.meanReversionSentiment && m.growth_ratio < 1) {
    signalType = "Mean Reversion"
  }

  let confidenceScore = 0
  if (m.mentions_L >= CONFIDENCE_THRESHOLDS.minMentions) confidenceScore += 1
  if (m.unique_subreddits >= CONFIDENCE_THRESHOLDS.minSubreddits) confidenceScore += 1
  if (m.concentration <= CONFIDENCE_THRESHOLDS.maxConcentration) confidenceScore += 1
  if (m.price_available) confidenceScore += 1

  let confidence = "Medium-Low"
  if (confidenceScore >= 4) confidence = "Very High"
  else if (confidenceScore === 3) confidence = "High"
  else if (confidenceScore === 2) confidence = "Medium-High"
  else if (confidenceScore === 1) confidence = "Medium"

  let timing = "Early"
  if (m.growth_ratio >= TIMING_THRESHOLDS.growthNow && m.slope > TIMING_THRESHOLDS.slopeNow) timing = "Now"
  else if (m.growth_ratio >= TIMING_THRESHOLDS.growthNow && m.concentration > TIMING_THRESHOLDS.concentrationVolatile) timing = "Now (volatile)"
  else if (m.growth_ratio >= TIMING_THRESHOLDS.growthBuilding) timing = "Building"
  else if (m.growth_ratio >= TIMING_THRESHOLDS.growthOngoing) timing = "Ongoing"

  const stars = Math.max(1, Math.min(5, Math.round(((total - rank + 1) / total) * 5)))

  return { signal_type: signalType, confidence, timing, stars }
}
