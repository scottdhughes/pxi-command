import { computeWilsonInterval } from "./db"

export interface WalkForwardConfig {
  minTrainSize: number
  testSize: number
  stepSize?: number
  expandingWindow?: boolean
  maxSlices?: number
}

export interface WalkForwardSlice {
  slice_id: number
  train_start: string
  train_end: string
  test_start: string
  test_end: string
  train_dates: string[]
  test_dates: string[]
}

export interface EvaluatedPredictionObservation {
  signal_date: string
  rank: number
  return_pct: number | null
}

export interface RankICPoint {
  signal_date: string
  sample_size: number
  spearman_rho: number
  rank_ic: number
}

export interface HitRateIntervalSummary {
  sample_size: number
  hits: number
  hit_rate: number
  hit_rate_ci_low: number
  hit_rate_ci_high: number
  minimum_recommended_sample_size: number
  sample_size_warning: boolean
}

export interface HypothesisPValue {
  hypothesis_id: string
  p_value: number
}

export interface AdjustedHypothesisResult {
  hypothesis_id: string
  p_value: number
  p_value_adjusted: number
  familywise_alpha: number
  reject: boolean
}

function ensurePositiveInt(name: string, value: number): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`)
  }
}

/**
 * Build deterministic walk-forward splits over ordered signal dates.
 *
 * Defaults:
 * - expanding window training set
 * - step size equal to test size
 */
export function computeWalkForwardSlices(
  signalDates: string[],
  config: WalkForwardConfig
): WalkForwardSlice[] {
  ensurePositiveInt("minTrainSize", config.minTrainSize)
  ensurePositiveInt("testSize", config.testSize)

  const stepSize = config.stepSize ?? config.testSize
  ensurePositiveInt("stepSize", stepSize)

  const uniqueSortedDates = [...new Set(signalDates)].sort()
  if (uniqueSortedDates.length < config.minTrainSize + config.testSize) {
    return []
  }

  const expandingWindow = config.expandingWindow ?? true
  const maxSlices = config.maxSlices

  if (maxSlices !== undefined && (!Number.isFinite(maxSlices) || maxSlices <= 0)) {
    throw new Error("maxSlices must be a positive finite number when provided")
  }

  const slices: WalkForwardSlice[] = []

  for (
    let testStartIdx = config.minTrainSize;
    testStartIdx + config.testSize <= uniqueSortedDates.length;
    testStartIdx += stepSize
  ) {
    const trainStartIdx = expandingWindow ? 0 : testStartIdx - config.minTrainSize
    const trainEndIdx = testStartIdx - 1
    const testEndIdx = testStartIdx + config.testSize - 1

    const trainDates = uniqueSortedDates.slice(trainStartIdx, trainEndIdx + 1)
    const testDates = uniqueSortedDates.slice(testStartIdx, testEndIdx + 1)

    slices.push({
      slice_id: slices.length + 1,
      train_start: trainDates[0],
      train_end: trainDates[trainDates.length - 1],
      test_start: testDates[0],
      test_end: testDates[testDates.length - 1],
      train_dates: trainDates,
      test_dates: testDates,
    })

    if (maxSlices !== undefined && slices.length >= maxSlices) {
      break
    }
  }

  return slices
}

function computeAverageRanks(values: number[], descending = false): number[] {
  const ordered = values
    .map((value, idx) => ({ value, idx }))
    .sort((a, b) => {
      if (a.value === b.value) return a.idx - b.idx
      return descending ? b.value - a.value : a.value - b.value
    })

  const ranks = new Array<number>(values.length)

  let i = 0
  while (i < ordered.length) {
    let j = i
    while (j + 1 < ordered.length && ordered[j + 1].value === ordered[i].value) {
      j++
    }

    const averageRank = (i + 1 + (j + 1)) / 2
    for (let k = i; k <= j; k++) {
      ranks[ordered[k].idx] = averageRank
    }

    i = j + 1
  }

  return ranks
}

function pearsonCorrelation(a: number[], b: number[]): number {
  if (a.length !== b.length || a.length < 2) return 0

  const n = a.length
  const meanA = a.reduce((sum, x) => sum + x, 0) / n
  const meanB = b.reduce((sum, x) => sum + x, 0) / n

  let covariance = 0
  let varianceA = 0
  let varianceB = 0

  for (let i = 0; i < n; i++) {
    const da = a[i] - meanA
    const db = b[i] - meanB
    covariance += da * db
    varianceA += da * da
    varianceB += db * db
  }

  if (varianceA === 0 || varianceB === 0) {
    return 0
  }

  return covariance / Math.sqrt(varianceA * varianceB)
}

/**
 * Spearman rank correlation with tie-aware average ranks.
 */
export function computeSpearmanCorrelation(ranks: number[], returns: number[]): number {
  if (ranks.length !== returns.length || ranks.length < 2) {
    return 0
  }

  const rankRanks = computeAverageRanks(ranks, false)
  const returnRanks = computeAverageRanks(returns, true)

  return pearsonCorrelation(rankRanks, returnRanks)
}

/**
 * Rank IC series by signal_date.
 *
 * With rank=1 as strongest prediction and return ranks computed descending,
 * positive `spearman_rho` and positive `rank_ic` indicate better alignment.
 */
export function computeRankICSeries(rows: EvaluatedPredictionObservation[]): RankICPoint[] {
  const grouped = new Map<string, EvaluatedPredictionObservation[]>()

  for (const row of rows) {
    const valid = Number.isFinite(row.rank) && Number.isFinite(row.return_pct)
    if (!valid) continue

    const group = grouped.get(row.signal_date)
    if (group) {
      group.push(row)
    } else {
      grouped.set(row.signal_date, [row])
    }
  }

  const points: RankICPoint[] = []

  for (const signalDate of [...grouped.keys()].sort()) {
    const group = grouped.get(signalDate) || []
    if (group.length < 2) continue

    const ranks = group.map((row) => row.rank)
    const returns = group.map((row) => Number(row.return_pct))
    const spearman = computeSpearmanCorrelation(ranks, returns)

    points.push({
      signal_date: signalDate,
      sample_size: group.length,
      spearman_rho: spearman,
      rank_ic: spearman,
    })
  }

  return points
}

export function computeHitRateIntervals(
  hits: number,
  total: number,
  minimumRecommendedSampleSize = 30
): HitRateIntervalSummary {
  ensurePositiveInt("minimumRecommendedSampleSize", minimumRecommendedSampleSize)

  const boundedTotal = Number.isFinite(total) ? Math.max(0, Math.trunc(total)) : 0
  const boundedHits = Number.isFinite(hits)
    ? Math.min(boundedTotal, Math.max(0, Math.trunc(hits)))
    : 0

  const ci = computeWilsonInterval(boundedHits, boundedTotal)
  const hitRate = boundedTotal > 0 ? (boundedHits / boundedTotal) * 100 : 0

  return {
    sample_size: boundedTotal,
    hits: boundedHits,
    hit_rate: hitRate,
    hit_rate_ci_low: ci.low * 100,
    hit_rate_ci_high: ci.high * 100,
    minimum_recommended_sample_size: minimumRecommendedSampleSize,
    sample_size_warning: boundedTotal < minimumRecommendedSampleSize,
  }
}

/**
 * Holm step-down adjusted p-values.
 *
 * This is a conservative family-wise error-rate control baseline and can be
 * replaced later by bootstrap Reality Check / SPA implementations without
 * changing call sites.
 */
export function computeMultipleTestingAdjustedPvalues(
  hypotheses: HypothesisPValue[],
  familywiseAlpha = 0.05
): AdjustedHypothesisResult[] {
  if (!Number.isFinite(familywiseAlpha) || familywiseAlpha <= 0 || familywiseAlpha >= 1) {
    throw new Error("familywiseAlpha must be in (0, 1)")
  }

  if (hypotheses.length === 0) {
    return []
  }

  const seen = new Set<string>()
  const validated = hypotheses.map((item, index) => {
    const id = item.hypothesis_id?.trim()
    if (!id) {
      throw new Error(`hypothesis_id is required at index ${index}`)
    }
    if (seen.has(id)) {
      throw new Error(`duplicate hypothesis_id: ${id}`)
    }
    seen.add(id)

    if (!Number.isFinite(item.p_value) || item.p_value < 0 || item.p_value > 1) {
      throw new Error(`invalid p_value for hypothesis_id=${id}`)
    }

    return {
      idx: index,
      hypothesis_id: id,
      p_value: item.p_value,
    }
  })

  const sorted = [...validated].sort((a, b) => {
    if (a.p_value === b.p_value) return a.idx - b.idx
    return a.p_value - b.p_value
  })

  const m = sorted.length
  const adjustedSorted = new Array<number>(m)
  let runningMax = 0

  for (let i = 0; i < m; i++) {
    const scaled = (m - i) * sorted[i].p_value
    runningMax = Math.max(runningMax, scaled)
    adjustedSorted[i] = Math.min(1, runningMax)
  }

  const adjustedByIndex = new Map<number, number>()
  for (let i = 0; i < m; i++) {
    adjustedByIndex.set(sorted[i].idx, adjustedSorted[i])
  }

  return validated.map((item) => {
    const adjusted = adjustedByIndex.get(item.idx) ?? 1
    return {
      hypothesis_id: item.hypothesis_id,
      p_value: item.p_value,
      p_value_adjusted: adjusted,
      familywise_alpha: familywiseAlpha,
      reject: adjusted <= familywiseAlpha,
    }
  })
}
