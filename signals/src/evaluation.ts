/**
 * Signal evaluation module
 *
 * Handles evaluation of pending predictions and storage of new predictions.
 * Runs as part of the scheduled pipeline to track signal accuracy.
 */

import type { Env } from "./config"
import type { ThemeReportItem } from "./report/render_json"
import {
  getPendingPredictions,
  updatePredictionOutcome,
  insertSignalPredictions,
  type SignalPredictionInput,
} from "./db"
import {
  fetchMultipleETFPrices,
  calculateReturn,
  isHit,
} from "./utils/price"
import { THEMES } from "./analysis/themes"
import { logInfo, logWarn } from "./utils/logger"

/** Number of days to wait before evaluating a prediction */
const EVALUATION_WINDOW_DAYS = 7

/**
 * Evaluates all pending predictions that have reached their target date.
 * Fetches exit prices and calculates returns for each prediction.
 *
 * @param env - Worker environment
 * @returns Summary of evaluation results
 */
export async function evaluatePendingPredictions(env: Env): Promise<{
  evaluated: number
  hits: number
  errors: number
}> {
  const pending = await getPendingPredictions(env)

  if (pending.length === 0) {
    logInfo("No pending predictions to evaluate")
    return { evaluated: 0, hits: 0, errors: 0 }
  }

  logInfo(`Evaluating ${pending.length} pending predictions`)

  // Collect unique proxy ETFs that need price fetching
  const etfsToFetch = [...new Set(pending.map((p) => p.proxy_etf).filter(Boolean))] as string[]
  const priceMap = await fetchMultipleETFPrices(etfsToFetch)

  let evaluated = 0
  let hits = 0
  let errors = 0

  for (const prediction of pending) {
    try {
      // Skip if no proxy ETF
      if (!prediction.proxy_etf) {
        await updatePredictionOutcome(env, prediction.id, null, null, null)
        evaluated++
        continue
      }

      // Skip if no entry price
      if (prediction.entry_price === null) {
        await updatePredictionOutcome(env, prediction.id, null, null, null)
        evaluated++
        continue
      }

      const priceResult = priceMap.get(prediction.proxy_etf)

      if (!priceResult || priceResult.error || priceResult.price === null) {
        logWarn(`Price fetch failed for ${prediction.proxy_etf}: ${priceResult?.error || "No data"}`)
        // Still mark as evaluated but with no outcome
        await updatePredictionOutcome(env, prediction.id, null, null, null)
        errors++
        evaluated++
        continue
      }

      const exitPrice = priceResult.price
      const returnPct = calculateReturn(prediction.entry_price, exitPrice)
      const hitValue = isHit(returnPct)

      await updatePredictionOutcome(env, prediction.id, exitPrice, returnPct, hitValue)

      evaluated++
      if (hitValue === 1) hits++

      logInfo(
        `Evaluated ${prediction.theme_name}: entry=${prediction.entry_price}, exit=${exitPrice}, return=${returnPct}%, hit=${hitValue}`
      )
    } catch (err) {
      logWarn(`Error evaluating prediction ${prediction.id}: ${err}`)
      errors++
    }
  }

  logInfo(`Evaluation complete: ${evaluated} evaluated, ${hits} hits, ${errors} errors`)
  return { evaluated, hits, errors }
}

/**
 * Stores new predictions from a pipeline run.
 * Fetches entry prices for proxy ETFs and creates prediction records.
 *
 * @param env - Worker environment
 * @param runId - ID of the current run
 * @param ranked - Ranked theme results from the pipeline
 * @returns Number of predictions stored
 */
export async function storePredictions(
  env: Env,
  runId: string,
  ranked: ThemeReportItem[]
): Promise<number> {
  const signalDate = new Date().toISOString().slice(0, 10)
  const targetDate = calculateTargetDate(signalDate, EVALUATION_WINDOW_DAYS)

  // Get proxy ETFs for each theme
  const themeEtfMap = new Map<string, string>()
  for (const theme of THEMES) {
    if (theme.proxy_etf) {
      themeEtfMap.set(theme.theme_id, theme.proxy_etf)
    }
  }

  // Collect ETFs that need prices
  const etfsToFetch: string[] = []
  for (const item of ranked) {
    const etf = themeEtfMap.get(item.theme_id)
    if (etf) etfsToFetch.push(etf)
  }

  const priceMap = await fetchMultipleETFPrices(etfsToFetch)

  const predictions: SignalPredictionInput[] = ranked.map((item) => {
    const proxyEtf = themeEtfMap.get(item.theme_id) || null
    const priceResult = proxyEtf ? priceMap.get(proxyEtf) : null
    const entryPrice = priceResult?.price ?? null

    return {
      run_id: runId,
      signal_date: signalDate,
      target_date: targetDate,
      theme_id: item.theme_id,
      theme_name: item.theme_name,
      rank: item.rank,
      score: item.score,
      signal_type: item.classification.signal_type,
      confidence: item.classification.confidence,
      timing: item.classification.timing,
      stars: item.classification.stars,
      proxy_etf: proxyEtf,
      entry_price: entryPrice,
    }
  })

  await insertSignalPredictions(env, predictions)

  logInfo(`Stored ${predictions.length} predictions for run ${runId}, target date ${targetDate}`)
  return predictions.length
}

/**
 * Calculates target date by adding business days (skipping weekends).
 * For simplicity, we add calendar days but this could be enhanced.
 *
 * @param startDate - Signal date in YYYY-MM-DD format
 * @param days - Number of days to add
 * @returns Target date in YYYY-MM-DD format
 */
function calculateTargetDate(startDate: string, days: number): string {
  const date = new Date(startDate)
  date.setDate(date.getDate() + days)
  return date.toISOString().slice(0, 10)
}

/**
 * Calculates Spearman rank correlation between predicted ranks and actual returns.
 * Useful for measuring if higher-ranked signals produce better returns.
 *
 * @param ranks - Array of prediction ranks (1 = best)
 * @param returns - Array of corresponding percentage returns
 * @returns Correlation coefficient (-1 to 1, negative means good)
 */
export function calculateSpearmanCorrelation(ranks: number[], returns: number[]): number {
  if (ranks.length !== returns.length || ranks.length < 2) {
    return 0
  }

  const n = ranks.length

  // Rank the returns (higher return = better rank = lower rank number)
  const returnRanks = rankArray(returns, true)

  // Calculate sum of squared rank differences
  let sumD2 = 0
  for (let i = 0; i < n; i++) {
    const d = ranks[i] - returnRanks[i]
    sumD2 += d * d
  }

  // Spearman's rho formula
  const rho = 1 - (6 * sumD2) / (n * (n * n - 1))
  return Math.round(rho * 1000) / 1000
}

/**
 * Converts array values to ranks (1-based).
 *
 * @param arr - Array of numbers
 * @param descending - If true, highest value gets rank 1
 * @returns Array of ranks
 */
function rankArray(arr: number[], descending: boolean): number[] {
  const indexed = arr.map((val, idx) => ({ val, idx }))
  indexed.sort((a, b) => (descending ? b.val - a.val : a.val - b.val))

  const ranks = new Array(arr.length)
  for (let i = 0; i < indexed.length; i++) {
    ranks[indexed[i].idx] = i + 1
  }
  return ranks
}
