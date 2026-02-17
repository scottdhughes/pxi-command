import {
  computeHitRateIntervals,
  computeMultipleTestingAdjustedPvalues,
  computeRankICSeries,
  computeWalkForwardSlices,
  type EvaluatedPredictionObservation,
} from "../evaluation_validation"

export interface EvaluationReportCliArgs {
  inputPath: string
  outDir: string
  minTrainSize: number
  testSize: number
  stepSize: number
  expandingWindow: boolean
  maxSlices?: number
  familywiseAlpha: number
  minimumRecommendedSampleSize: number
  minResolvedObservations: number
  maxUnresolvedRatePct: number
  minSliceCount: number
}

export interface EvaluationReportInputRow {
  signal_date: string
  rank: number
  return_pct: number | null
  hit: number | null
  confidence: string | null
  timing: string | null
}

export interface RankICSummary {
  count: number
  mean: number
  median: number
  q1: number
  q3: number
  iqr: number
}

export interface EvaluationReportSlice {
  slice_id: number
  train_start: string
  train_end: string
  test_start: string
  test_end: string
  train_dates_count: number
  test_dates_count: number
  resolved_count: number
  hits: number
  hit_rate: number
  hit_rate_ci_low: number
  hit_rate_ci_high: number
  sample_size_warning: boolean
  avg_return: number
  rank_ic_summary: RankICSummary
  p_value: number
  p_value_adjusted: number | null
  reject: boolean | null
}

export interface EvaluationReportOutput {
  generated_at: string
  source: {
    observations: number
    unique_signal_dates: number
    resolved_observations: number
  }
  config: {
    min_train_size: number
    test_size: number
    step_size: number
    expanding_window: boolean
    max_slices: number | null
    familywise_alpha: number
    minimum_recommended_sample_size: number
    governance_thresholds: {
      min_resolved_observations: number
      max_unresolved_rate_pct: number
      min_slice_count: number
    }
  }
  full_sample: {
    resolved_count: number
    hits: number
    hit_rate: number
    hit_rate_ci_low: number
    hit_rate_ci_high: number
    sample_size_warning: boolean
    avg_return: number
    rank_ic_summary: RankICSummary
  }
  walk_forward: {
    slice_count: number
    slices: EvaluationReportSlice[]
  }
  multiple_testing: {
    familywise_alpha: number
    hypotheses: Array<{
      hypothesis_id: string
      p_value: number
      p_value_adjusted: number
      reject: boolean
    }>
  }
  governance_status: {
    status: "pass" | "fail"
    reasons: string[]
    observed: {
      resolved_observations: number
      unresolved_rate_pct: number
      slice_count: number
    }
  }
  references: Array<{
    title: string
    url: string
  }>
}

const DEFAULT_INPUT_PATH = "data/evaluation_sample_predictions.json"
const DEFAULT_OUT_DIR = "out/evaluation"
const DEFAULT_MIN_TRAIN_SIZE = 20
const DEFAULT_TEST_SIZE = 5
const DEFAULT_STEP_SIZE = 5
const DEFAULT_FAMILYWISE_ALPHA = 0.05
const DEFAULT_MINIMUM_RECOMMENDED_SAMPLE_SIZE = 30
const DEFAULT_MIN_RESOLVED_OBSERVATIONS = 30
const DEFAULT_MAX_UNRESOLVED_RATE_PCT = 20
const DEFAULT_MIN_SLICE_COUNT = 3

const PRIMARY_SOURCE_REFERENCES = [
  {
    title: "White (2000), A Reality Check for Data Snooping",
    url: "https://doi.org/10.1111/1468-0262.00152",
  },
  {
    title: "Romano & Wolf (2005), Stepwise Multiple Testing",
    url: "https://doi.org/10.1111/j.1468-0262.2005.00615.x",
  },
  {
    title: "Bailey et al. (2016), The Probability of Backtest Overfitting",
    url: "https://doi.org/10.21314/jcf.2016.322",
  },
  {
    title: "Arian et al. (2024), Backtest Overfitting in the ML Era",
    url: "https://doi.org/10.1016/j.knosys.2024.112477",
  },
  {
    title: "Bailey & López de Prado (2014), Deflated Sharpe Ratio",
    url: "https://doi.org/10.3905/jpm.2014.40.5.094",
  },
]

function parsePositiveInt(flag: string, value: string): number {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${flag} must be a positive integer`)
  }
  return parsed
}

function parseUnitInterval(flag: string, value: string): number {
  const parsed = Number.parseFloat(value)
  if (!Number.isFinite(parsed) || parsed <= 0 || parsed >= 1) {
    throw new Error(`${flag} must be a number in (0, 1)`)
  }
  return parsed
}

function parsePercent(flag: string, value: string): number {
  const parsed = Number.parseFloat(value)
  if (!Number.isFinite(parsed) || parsed < 0 || parsed > 100) {
    throw new Error(`${flag} must be a number in [0, 100]`)
  }
  return parsed
}

function requireFlagValue(flag: string, value: string | undefined): string {
  if (!value || value.trim().length === 0) {
    throw new Error(`${flag} requires a value`)
  }
  return value.trim()
}

export function parseEvaluationReportArgs(argv: string[]): EvaluationReportCliArgs {
  let inputPath = DEFAULT_INPUT_PATH
  let outDir = DEFAULT_OUT_DIR
  let minTrainSize = DEFAULT_MIN_TRAIN_SIZE
  let testSize = DEFAULT_TEST_SIZE
  let stepSize = DEFAULT_STEP_SIZE
  let expandingWindow = true
  let maxSlices: number | undefined
  let familywiseAlpha = DEFAULT_FAMILYWISE_ALPHA
  let minimumRecommendedSampleSize = DEFAULT_MINIMUM_RECOMMENDED_SAMPLE_SIZE
  let minResolvedObservations = DEFAULT_MIN_RESOLVED_OBSERVATIONS
  let maxUnresolvedRatePct = DEFAULT_MAX_UNRESOLVED_RATE_PCT
  let minSliceCount = DEFAULT_MIN_SLICE_COUNT

  const args = argv.slice(2)
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]

    switch (arg) {
      case "--input": {
        inputPath = requireFlagValue("--input", args[++i])
        break
      }
      case "--out": {
        outDir = requireFlagValue("--out", args[++i])
        break
      }
      case "--min-train": {
        minTrainSize = parsePositiveInt("--min-train", requireFlagValue("--min-train", args[++i]))
        break
      }
      case "--test-size": {
        testSize = parsePositiveInt("--test-size", requireFlagValue("--test-size", args[++i]))
        break
      }
      case "--step-size": {
        stepSize = parsePositiveInt("--step-size", requireFlagValue("--step-size", args[++i]))
        break
      }
      case "--max-slices": {
        maxSlices = parsePositiveInt("--max-slices", requireFlagValue("--max-slices", args[++i]))
        break
      }
      case "--rolling": {
        expandingWindow = false
        break
      }
      case "--alpha": {
        familywiseAlpha = parseUnitInterval("--alpha", requireFlagValue("--alpha", args[++i]))
        break
      }
      case "--minimum-sample": {
        minimumRecommendedSampleSize = parsePositiveInt(
          "--minimum-sample",
          requireFlagValue("--minimum-sample", args[++i])
        )
        break
      }
      case "--min-resolved": {
        minResolvedObservations = parsePositiveInt("--min-resolved", requireFlagValue("--min-resolved", args[++i]))
        break
      }
      case "--max-unresolved-rate": {
        maxUnresolvedRatePct = parsePercent(
          "--max-unresolved-rate",
          requireFlagValue("--max-unresolved-rate", args[++i])
        )
        break
      }
      case "--min-slices": {
        minSliceCount = parsePositiveInt("--min-slices", requireFlagValue("--min-slices", args[++i]))
        break
      }
      default: {
        throw new Error(`Unknown flag: ${arg}`)
      }
    }
  }

  return {
    inputPath,
    outDir,
    minTrainSize,
    testSize,
    stepSize,
    expandingWindow,
    maxSlices,
    familywiseAlpha,
    minimumRecommendedSampleSize,
    minResolvedObservations,
    maxUnresolvedRatePct,
    minSliceCount,
  }
}

interface JsonObject {
  [key: string]: unknown
}

function asRecord(value: unknown): JsonObject | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null
  }
  return value as JsonObject
}

function normalizeString(value: unknown): string | null {
  if (typeof value !== "string") return null
  const trimmed = value.trim()
  return trimmed.length > 0 ? trimmed : null
}

function normalizeFiniteNumber(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null
  }
  return value
}

function normalizeHit(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null
  }

  if (typeof value === "boolean") {
    return value ? 1 : 0
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    if (value === 1) return 1
    if (value === 0) return 0
  }

  return null
}

/**
 * Accepts either:
 * - an array of prediction-like rows
 * - an object with `predictions: []`
 */
export function normalizeEvaluationRows(payload: unknown): EvaluationReportInputRow[] {
  let rowsPayload: unknown[] = []

  if (Array.isArray(payload)) {
    rowsPayload = payload
  } else {
    const record = asRecord(payload)
    if (record && Array.isArray(record.predictions)) {
      rowsPayload = record.predictions
    }
  }

  const rows: EvaluationReportInputRow[] = []

  for (const raw of rowsPayload) {
    const row = asRecord(raw)
    if (!row) continue

    const signalDate = normalizeString(row.signal_date)
    const rank = normalizeFiniteNumber(row.rank)

    if (!signalDate || rank === null) {
      continue
    }

    rows.push({
      signal_date: signalDate,
      rank,
      return_pct: normalizeFiniteNumber(row.return_pct),
      hit: normalizeHit(row.hit),
      confidence: normalizeString(row.confidence),
      timing: normalizeString(row.timing),
    })
  }

  return rows.sort((a, b) => {
    if (a.signal_date === b.signal_date) {
      return a.rank - b.rank
    }
    return a.signal_date.localeCompare(b.signal_date)
  })
}

function mean(values: number[]): number {
  if (values.length === 0) return 0
  return values.reduce((sum, v) => sum + v, 0) / values.length
}

function quantile(sortedValues: number[], q: number): number {
  if (sortedValues.length === 0) return 0
  if (sortedValues.length === 1) return sortedValues[0]

  const position = (sortedValues.length - 1) * q
  const lower = Math.floor(position)
  const upper = Math.ceil(position)
  if (lower === upper) return sortedValues[lower]

  const weight = position - lower
  return sortedValues[lower] * (1 - weight) + sortedValues[upper] * weight
}

function summarizeRankIC(points: Array<{ rank_ic: number }>): RankICSummary {
  const values = points.map((point) => point.rank_ic).sort((a, b) => a - b)
  const q1 = quantile(values, 0.25)
  const median = quantile(values, 0.5)
  const q3 = quantile(values, 0.75)

  return {
    count: values.length,
    mean: mean(values),
    median,
    q1,
    q3,
    iqr: q3 - q1,
  }
}

const LOG_FACTORIAL_CACHE = [0]

function logFactorial(n: number): number {
  while (LOG_FACTORIAL_CACHE.length <= n) {
    const next = LOG_FACTORIAL_CACHE.length
    LOG_FACTORIAL_CACHE.push(LOG_FACTORIAL_CACHE[next - 1] + Math.log(next))
  }
  return LOG_FACTORIAL_CACHE[n]
}

function binomialProbability(n: number, k: number, p: number): number {
  if (k < 0 || k > n) return 0
  if (p <= 0) return k === 0 ? 1 : 0
  if (p >= 1) return k === n ? 1 : 0

  const logComb = logFactorial(n) - logFactorial(k) - logFactorial(n - k)
  const logProb = logComb + k * Math.log(p) + (n - k) * Math.log(1 - p)
  return Math.exp(logProb)
}

/**
 * Two-sided exact binomial p-value using probability ordering.
 */
export function computeTwoSidedBinomialPValue(hits: number, total: number, p0 = 0.5): number {
  if (!Number.isInteger(total) || total < 0) {
    throw new Error("total must be a non-negative integer")
  }

  if (!Number.isInteger(hits) || hits < 0 || hits > total) {
    throw new Error("hits must be an integer in [0, total]")
  }

  if (!Number.isFinite(p0) || p0 <= 0 || p0 >= 1) {
    throw new Error("p0 must be in (0, 1)")
  }

  if (total === 0) {
    return 1
  }

  const observedProb = binomialProbability(total, hits, p0)
  let cumulative = 0

  for (let k = 0; k <= total; k++) {
    const prob = binomialProbability(total, k, p0)
    if (prob <= observedProb + 1e-15) {
      cumulative += prob
    }
  }

  return Math.min(1, cumulative)
}

function toRankIcRows(rows: EvaluationReportInputRow[]): EvaluatedPredictionObservation[] {
  return rows.map((row) => ({
    signal_date: row.signal_date,
    rank: row.rank,
    return_pct: row.return_pct,
  }))
}

function computeHitStats(rows: EvaluationReportInputRow[], minimumRecommendedSampleSize: number) {
  const resolved = rows.filter((row) => row.hit !== null)
  const hits = resolved.reduce((sum, row) => sum + (row.hit === 1 ? 1 : 0), 0)
  const interval = computeHitRateIntervals(hits, resolved.length, minimumRecommendedSampleSize)

  const returns = resolved
    .map((row) => row.return_pct)
    .filter((value): value is number => typeof value === "number" && Number.isFinite(value))

  return {
    resolved,
    hits,
    interval,
    avgReturn: mean(returns),
  }
}

function hypothesisForGroup(
  hypothesisId: string,
  rows: EvaluationReportInputRow[]
): { hypothesis_id: string; p_value: number } | null {
  const resolvedRows = rows.filter((row) => row.hit !== null)
  if (resolvedRows.length === 0) {
    return null
  }

  const hits = resolvedRows.reduce((sum, row) => sum + (row.hit === 1 ? 1 : 0), 0)
  return {
    hypothesis_id: hypothesisId,
    p_value: computeTwoSidedBinomialPValue(hits, resolvedRows.length, 0.5),
  }
}

export function buildEvaluationReport(
  rows: EvaluationReportInputRow[],
  args: Pick<
    EvaluationReportCliArgs,
    | "minTrainSize"
    | "testSize"
    | "stepSize"
    | "expandingWindow"
    | "maxSlices"
    | "familywiseAlpha"
    | "minimumRecommendedSampleSize"
    | "minResolvedObservations"
    | "maxUnresolvedRatePct"
    | "minSliceCount"
  >
): EvaluationReportOutput {
  const uniqueSignalDates = [...new Set(rows.map((row) => row.signal_date))].sort()

  const walkForwardSlices = computeWalkForwardSlices(uniqueSignalDates, {
    minTrainSize: args.minTrainSize,
    testSize: args.testSize,
    stepSize: args.stepSize,
    expandingWindow: args.expandingWindow,
    maxSlices: args.maxSlices,
  })

  const fullSampleHitStats = computeHitStats(rows, args.minimumRecommendedSampleSize)
  const fullSampleRankIC = summarizeRankIC(computeRankICSeries(toRankIcRows(rows)))

  const hypotheses: Array<{ hypothesis_id: string; p_value: number }> = []
  const reportSlices: EvaluationReportSlice[] = []

  for (const slice of walkForwardSlices) {
    const testDateSet = new Set(slice.test_dates)
    const testRows = rows.filter((row) => testDateSet.has(row.signal_date))

    const testHitStats = computeHitStats(testRows, args.minimumRecommendedSampleSize)
    const testRankICSummary = summarizeRankIC(computeRankICSeries(toRankIcRows(testRows)))

    const hypothesis = hypothesisForGroup(`slice:${slice.slice_id}`, testRows)
    if (hypothesis) {
      hypotheses.push(hypothesis)
    }

    reportSlices.push({
      slice_id: slice.slice_id,
      train_start: slice.train_start,
      train_end: slice.train_end,
      test_start: slice.test_start,
      test_end: slice.test_end,
      train_dates_count: slice.train_dates.length,
      test_dates_count: slice.test_dates.length,
      resolved_count: testHitStats.resolved.length,
      hits: testHitStats.hits,
      hit_rate: testHitStats.interval.hit_rate,
      hit_rate_ci_low: testHitStats.interval.hit_rate_ci_low,
      hit_rate_ci_high: testHitStats.interval.hit_rate_ci_high,
      sample_size_warning: testHitStats.interval.sample_size_warning,
      avg_return: testHitStats.avgReturn,
      rank_ic_summary: testRankICSummary,
      p_value: hypothesis?.p_value ?? 1,
      p_value_adjusted: null,
      reject: null,
    })
  }

  const groupedHypotheses = [
    {
      prefix: "confidence",
      values: [...new Set(rows.map((row) => row.confidence).filter((value): value is string => Boolean(value)))].sort(),
      getGroupRows: (value: string) => rows.filter((row) => row.confidence === value),
    },
    {
      prefix: "timing",
      values: [...new Set(rows.map((row) => row.timing).filter((value): value is string => Boolean(value)))].sort(),
      getGroupRows: (value: string) => rows.filter((row) => row.timing === value),
    },
  ]

  for (const group of groupedHypotheses) {
    for (const value of group.values) {
      const hypothesis = hypothesisForGroup(`${group.prefix}:${value}`, group.getGroupRows(value))
      if (hypothesis) {
        hypotheses.push(hypothesis)
      }
    }
  }

  const adjusted = computeMultipleTestingAdjustedPvalues(hypotheses, args.familywiseAlpha)
  const adjustedById = new Map(adjusted.map((item) => [item.hypothesis_id, item]))

  for (const slice of reportSlices) {
    const adjustedItem = adjustedById.get(`slice:${slice.slice_id}`)
    if (!adjustedItem) continue

    slice.p_value_adjusted = adjustedItem.p_value_adjusted
    slice.reject = adjustedItem.reject
  }

  const unresolvedCount = rows.length - fullSampleHitStats.resolved.length
  const unresolvedRatePct = rows.length > 0 ? (unresolvedCount / rows.length) * 100 : 0
  const governanceReasons: string[] = []

  if (fullSampleHitStats.resolved.length < args.minResolvedObservations) {
    governanceReasons.push(
      `resolved observations ${fullSampleHitStats.resolved.length} below minimum ${args.minResolvedObservations}`
    )
  }

  if (unresolvedRatePct > args.maxUnresolvedRatePct) {
    governanceReasons.push(
      `unresolved rate ${unresolvedRatePct.toFixed(1)}% exceeds maximum ${args.maxUnresolvedRatePct.toFixed(1)}%`
    )
  }

  if (reportSlices.length < args.minSliceCount) {
    governanceReasons.push(`walk-forward slices ${reportSlices.length} below minimum ${args.minSliceCount}`)
  }

  const governanceStatus: EvaluationReportOutput["governance_status"] = {
    status: governanceReasons.length === 0 ? "pass" : "fail",
    reasons: governanceReasons,
    observed: {
      resolved_observations: fullSampleHitStats.resolved.length,
      unresolved_rate_pct: unresolvedRatePct,
      slice_count: reportSlices.length,
    },
  }

  return {
    generated_at: new Date().toISOString(),
    source: {
      observations: rows.length,
      unique_signal_dates: uniqueSignalDates.length,
      resolved_observations: fullSampleHitStats.resolved.length,
    },
    config: {
      min_train_size: args.minTrainSize,
      test_size: args.testSize,
      step_size: args.stepSize,
      expanding_window: args.expandingWindow,
      max_slices: args.maxSlices ?? null,
      familywise_alpha: args.familywiseAlpha,
      minimum_recommended_sample_size: args.minimumRecommendedSampleSize,
      governance_thresholds: {
        min_resolved_observations: args.minResolvedObservations,
        max_unresolved_rate_pct: args.maxUnresolvedRatePct,
        min_slice_count: args.minSliceCount,
      },
    },
    full_sample: {
      resolved_count: fullSampleHitStats.resolved.length,
      hits: fullSampleHitStats.hits,
      hit_rate: fullSampleHitStats.interval.hit_rate,
      hit_rate_ci_low: fullSampleHitStats.interval.hit_rate_ci_low,
      hit_rate_ci_high: fullSampleHitStats.interval.hit_rate_ci_high,
      sample_size_warning: fullSampleHitStats.interval.sample_size_warning,
      avg_return: fullSampleHitStats.avgReturn,
      rank_ic_summary: fullSampleRankIC,
    },
    walk_forward: {
      slice_count: reportSlices.length,
      slices: reportSlices,
    },
    multiple_testing: {
      familywise_alpha: args.familywiseAlpha,
      hypotheses: adjusted.map((item) => ({
        hypothesis_id: item.hypothesis_id,
        p_value: item.p_value,
        p_value_adjusted: item.p_value_adjusted,
        reject: item.reject,
      })),
    },
    governance_status: governanceStatus,
    references: PRIMARY_SOURCE_REFERENCES,
  }
}

function formatPercent(value: number): string {
  return `${value.toFixed(1)}%`
}

function formatSignedPercent(value: number): string {
  const prefix = value > 0 ? "+" : ""
  return `${prefix}${value.toFixed(2)}%`
}

export function renderEvaluationReportMarkdown(report: EvaluationReportOutput): string {
  const lines: string[] = []

  lines.push("# Evaluation Robustness Report")
  lines.push("")
  lines.push(`Generated: ${report.generated_at}`)
  lines.push("")

  lines.push("## Full Sample")
  lines.push(`- Observations: ${report.source.observations}`)
  lines.push(`- Unique signal dates: ${report.source.unique_signal_dates}`)
  lines.push(`- Resolved outcomes: ${report.full_sample.resolved_count}`)
  lines.push(
    `- Hit rate (95% Wilson CI): ${formatPercent(report.full_sample.hit_rate)} ` +
      `(${formatPercent(report.full_sample.hit_rate_ci_low)} to ${formatPercent(report.full_sample.hit_rate_ci_high)})`
  )
  lines.push(`- Average return: ${formatSignedPercent(report.full_sample.avg_return)}`)
  lines.push(
    `- Rank-IC median [IQR]: ${report.full_sample.rank_ic_summary.median.toFixed(3)} ` +
      `[${report.full_sample.rank_ic_summary.q1.toFixed(3)}, ${report.full_sample.rank_ic_summary.q3.toFixed(3)}]`
  )
  lines.push("")

  lines.push("## Walk-Forward Slices")
  if (report.walk_forward.slices.length === 0) {
    lines.push("- No valid slices under current configuration.")
  } else {
    for (const slice of report.walk_forward.slices) {
      const adjustedText =
        slice.p_value_adjusted === null ? "n/a" : slice.p_value_adjusted.toFixed(4)
      const rejectText = slice.reject === null ? "n/a" : String(slice.reject)
      lines.push(
        `- Slice ${slice.slice_id} test ${slice.test_start}→${slice.test_end}: ` +
          `n=${slice.resolved_count}, hit=${formatPercent(slice.hit_rate)}, ` +
          `CI=[${formatPercent(slice.hit_rate_ci_low)}, ${formatPercent(slice.hit_rate_ci_high)}], ` +
          `rankIC_median=${slice.rank_ic_summary.median.toFixed(3)}, ` +
          `p=${slice.p_value.toFixed(4)}, p_adj=${adjustedText}, reject=${rejectText}`
      )
    }
  }
  lines.push("")

  lines.push("## Multiple-Testing (Holm)")
  lines.push(`- Family-wise alpha: ${report.multiple_testing.familywise_alpha}`)
  lines.push(`- Hypotheses tested: ${report.multiple_testing.hypotheses.length}`)
  const rejected = report.multiple_testing.hypotheses.filter((item) => item.reject)
  lines.push(`- Rejections: ${rejected.length}`)
  lines.push("")

  lines.push("## Governance Status")
  lines.push(`- Status: ${report.governance_status.status.toUpperCase()}`)
  lines.push(`- Resolved observations: ${report.governance_status.observed.resolved_observations}`)
  lines.push(`- Unresolved rate: ${report.governance_status.observed.unresolved_rate_pct.toFixed(1)}%`)
  lines.push(`- Walk-forward slices: ${report.governance_status.observed.slice_count}`)
  if (report.governance_status.reasons.length === 0) {
    lines.push("- Reasons: none")
  } else {
    for (const reason of report.governance_status.reasons) {
      lines.push(`- ${reason}`)
    }
  }
  lines.push("")

  lines.push("## Primary Sources")
  for (const ref of report.references) {
    lines.push(`- ${ref.title}: ${ref.url}`)
  }

  return lines.join("\n")
}
