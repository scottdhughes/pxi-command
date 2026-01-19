import type { ThemeMetrics } from "./metrics"
import type { ThemeScore } from "./scoring"
import type { ThemeClassification } from "./classify"
import { LIMITS, RISK_THRESHOLDS } from "./constants"

export interface Takeaways {
  data_shows: string[]
  actionable_signals: string[]
  risk_factors: string[]
}

/**
 * Generates narrative takeaways from analyzed themes for the report summary.
 *
 * Produces three sections:
 * 1. **Data Shows**: Top 3 themes with velocity stats and subreddit coverage
 * 2. **Actionable Signals**: Themes grouped by timing (Immediate, Building, Watch)
 * 3. **Risk Factors**: Themes with high concentration or low sample size
 *
 * @param items - Ranked theme data with metrics, scores, and classification
 * @returns Takeaways object with data_shows, actionable_signals, and risk_factors arrays
 *
 * @example
 * ```typescript
 * const takeaways = buildTakeaways(rankedThemes);
 * console.log(takeaways.actionable_signals);
 * // ["Immediate: Nuclear/Uranium, Defense.", "Building: Copper."]
 * ```
 */
export function buildTakeaways(items: Array<{ metrics: ThemeMetrics; score: ThemeScore; classification: ThemeClassification }>): Takeaways {
  const top = items.slice(0, LIMITS.topTakeaways)
  const dataShows = top.map((t) => {
    const growth = t.metrics.growth_ratio.toFixed(2)
    return `${t.metrics.theme_name} velocity is up ${growth}x vs baseline with ${t.metrics.unique_subreddits} subreddits contributing.`
  })

  const immediate = items.filter((t) => t.classification.timing.startsWith("Now"))
  const building = items.filter((t) => t.classification.timing === "Building")
  const watch = items.filter((t) => t.classification.timing === "Early" || t.classification.timing === "Ongoing")

  const actionable = [] as string[]
  if (immediate.length) {
    actionable.push(`Immediate: ${immediate.map((t) => t.metrics.theme_name).slice(0, LIMITS.topTakeaways).join(", ")}.`)
  }
  if (building.length) {
    actionable.push(`Building: ${building.map((t) => t.metrics.theme_name).slice(0, LIMITS.topTakeaways).join(", ")}.`)
  }
  if (watch.length) {
    actionable.push(`Watch: ${watch.map((t) => t.metrics.theme_name).slice(0, LIMITS.topTakeaways).join(", ")}.`)
  }

  const riskFactors = items
    .filter((t) => t.metrics.concentration > RISK_THRESHOLDS.highConcentration || t.metrics.mentions_L < RISK_THRESHOLDS.lowMentions)
    .slice(0, LIMITS.topTakeaways)
    .map((t) => {
      const reasons = []
      if (t.metrics.concentration > RISK_THRESHOLDS.highConcentration) reasons.push("high concentration")
      if (t.metrics.mentions_L < RISK_THRESHOLDS.lowMentions) reasons.push("low sample size")
      return `${t.metrics.theme_name}: ${reasons.join(" and ")}.`
    })

  return {
    data_shows: dataShows,
    actionable_signals: actionable,
    risk_factors: riskFactors.length ? riskFactors : ["No outsized risk factors detected from concentration or sample size."],
  }
}
