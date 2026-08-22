const DAY_MS = 24 * 60 * 60 * 1000;
const OUTCOME_OBSERVATION_TOLERANCE_DAYS = 4;

// Promotion is checked repeatedly as the prospective ledger grows. A 5-sigma
// boundary is a conservative Bonferroni control for four promotion comparisons
// at each of at most 5,000 unique sample looks (union-bound error < 0.05).
export const EDGE_EVIDENCE_MAX_UNIQUE_LOOKS = 5000;
export const EDGE_EVIDENCE_COMPARISONS_PER_LOOK = 4;
export const EDGE_EVIDENCE_FAMILYWISE_CRITICAL_VALUE = 5;

export const EDGE_EVIDENCE_METHOD = 'paired_calendar_bartlett_newey_west' as const;

export interface EdgeEvidenceGate {
  pass: boolean;
  reasons: string[];
}

export interface EdgeEvidenceRow {
  horizon: string;
  prediction_date: string;
  target_date: string | null;
  predicted_change: number | null;
  actual_change: number | null;
  actual_observation_date: string | null;
  realized_return_pct: number | null;
  model_version: string | null;
  evaluated_at?: string | null;
  integrity_pass?: boolean;
  integrity_reasons?: readonly string[];
}

export interface EdgeEvidenceHorizonConfig {
  horizon: string;
  horizon_days: number;
}

export interface EdgeEvidenceThresholds {
  minimum_paired_observations: number;
  paired_observations_per_horizon_day: number;
  minimum_discordant_pairs: number;
  minimum_span_horizon_multiples: number;
  minimum_weekday_coverage_ratio: number;
  maximum_prediction_age_days: number;
  maximum_actual_observation_age_days: number;
}

export const DEFAULT_EDGE_EVIDENCE_THRESHOLDS: Readonly<EdgeEvidenceThresholds> = Object.freeze({
  minimum_paired_observations: 60,
  paired_observations_per_horizon_day: 4,
  minimum_discordant_pairs: 30,
  minimum_span_horizon_multiples: 12,
  minimum_weekday_coverage_ratio: 0.8,
  maximum_prediction_age_days: 4,
  maximum_actual_observation_age_days: 4,
});

export interface EdgeEvidenceOptions {
  horizons: readonly EdgeEvidenceHorizonConfig[];
  current_model_version: string | null;
  round_trip_cost_pct: number;
  upstream_integrity: EdgeEvidenceGate;
  thresholds?: Partial<EdgeEvidenceThresholds>;
}

export interface CalendarHacSample {
  date: string;
  value: number;
}

export interface CalendarHacEstimate {
  method: typeof EDGE_EVIDENCE_METHOD;
  bandwidth_days: number;
  confidence_level: 0.95;
  sample_size: number;
  mean: number | null;
  standard_error: number | null;
  ci95_low: number | null;
  ci95_high: number | null;
  lower_bound_positive: boolean;
  unavailable_reasons: string[];
}

export interface PairedEdgeObservation {
  prediction_date: string;
  target_date: string;
  baseline_prediction_date: string;
  baseline_target_date: string;
  baseline_actual_observation_date: string;
  model_direction: -1 | 0 | 1;
  baseline_direction: -1 | 0 | 1;
  actual_direction: -1 | 0 | 1;
  model_correct: 0 | 1;
  baseline_correct: 0 | 1;
  direction_difference: -1 | 0 | 1;
  model_signed_return_after_cost_pct: number | null;
  baseline_signed_return_after_cost_pct: number | null;
  signed_return_difference_after_cost_pct: number | null;
}

export interface CausalPairBuildResult {
  observations: PairedEdgeObservation[];
  integrity_reasons: string[];
  current_model_row_count: number;
  completed_current_model_row_count: number;
  causal_baseline_unavailable_count: number;
  matured_outcome_missing_count: number;
  latest_prediction_date: string | null;
  latest_evaluated_target_date: string | null;
  latest_actual_observation_date: string | null;
}

export interface EdgeMetricEvidence extends CalendarHacEstimate {
  model_mean: number | null;
  baseline_mean: number | null;
  uplift: number | null;
}

export interface EdgeEvidenceWindow {
  horizon: string;
  horizon_days: number;
  as_of: string | null;
  current_model_version: string | null;
  paired_sample_size: number;
  signed_return_sample_size: number;
  discordant_pairs: number;
  causal_baseline_unavailable_count: number;
  matured_outcome_missing_count: number;
  calendar_span_days: number;
  expected_weekdays: number;
  observed_weekdays: number;
  weekday_coverage_ratio: number;
  latest_prediction_date: string | null;
  latest_prediction_age_days: number | null;
  latest_evaluated_target_date: string | null;
  latest_actual_observation_date: string | null;
  latest_actual_observation_age_days: number | null;
  minimum_required_paired_sample: number;
  minimum_required_span_days: number;
  direction: EdgeMetricEvidence;
  signed_return_after_cost_pct: EdgeMetricEvidence;
  integrity_gate: EdgeEvidenceGate;
  eligibility_gate: EdgeEvidenceGate;
  performance_gate: EdgeEvidenceGate;
  evidence_gate: EdgeEvidenceGate;
}

export interface EdgeEvidenceReport {
  as_of: string | null;
  current_model_version: string | null;
  method: typeof EDGE_EVIDENCE_METHOD;
  windows: EdgeEvidenceWindow[];
  integrity_gate: EdgeEvidenceGate;
  performance_gate: EdgeEvidenceGate;
  evidence_gate: EdgeEvidenceGate;
  promotion_gate: EdgeEvidenceGate;
}

interface ParsedRow {
  input_index: number;
  prediction_date: string;
  prediction_day: number;
  target_date: string;
  target_day: number;
  predicted_change: number | null;
  actual_change: number | null;
  actual_observation_date: string | null;
  actual_observation_day: number | null;
  realized_return_pct: number | null;
  model_version: string | null;
}

function uniqueReasons(reasons: readonly string[]): string[] {
  return [...new Set(reasons.filter((reason) => reason.length > 0))];
}

function gate(reasons: readonly string[]): EdgeEvidenceGate {
  const unique = uniqueReasons(reasons);
  return { pass: unique.length === 0, reasons: unique };
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

function parseIsoDateDay(value: unknown): number | null {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
  const [year, month, day] = value.split('-').map(Number);
  const timestamp = Date.UTC(year, month - 1, day);
  if (!Number.isFinite(timestamp)) return null;
  const parsed = new Date(timestamp);
  if (
    parsed.getUTCFullYear() !== year
    || parsed.getUTCMonth() !== month - 1
    || parsed.getUTCDate() !== day
  ) {
    return null;
  }
  return Math.floor(timestamp / DAY_MS);
}

function isoDateFromDay(day: number): string {
  return new Date(day * DAY_MS).toISOString().slice(0, 10);
}

function parseNow(value: Date | string): { instant: Date | null; day: number | null } {
  if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value)) {
    const day = parseIsoDateDay(value);
    return day === null
      ? { instant: null, day: null }
      : { instant: new Date(`${value}T00:00:00.000Z`), day };
  }
  const instant = value instanceof Date ? new Date(value.getTime()) : new Date(value);
  if (!Number.isFinite(instant.getTime())) return { instant: null, day: null };
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(instant);
  const part = (type: Intl.DateTimeFormatPartTypes) =>
    parts.find((entry) => entry.type === type)?.value;
  const dateKey = `${part('year') || ''}-${part('month') || ''}-${part('day') || ''}`;
  return {
    instant,
    day: parseIsoDateDay(dateKey),
  };
}

function directionSign(value: number): -1 | 0 | 1 {
  if (value > 0) return 1;
  if (value < 0) return -1;
  return 0;
}

function countWeekdaysInclusive(firstDay: number, lastDay: number): number {
  if (lastDay < firstDay) return 0;
  const totalDays = lastDay - firstDay + 1;
  const completeWeeks = Math.floor(totalDays / 7);
  let weekdays = completeWeeks * 5;
  const remainder = totalDays % 7;
  const firstWeekday = new Date(firstDay * DAY_MS).getUTCDay();
  for (let offset = 0; offset < remainder; offset += 1) {
    const weekday = (firstWeekday + offset) % 7;
    if (weekday !== 0 && weekday !== 6) weekdays += 1;
  }
  return weekdays;
}

function isWeekday(day: number): boolean {
  const weekday = new Date(day * DAY_MS).getUTCDay();
  return weekday !== 0 && weekday !== 6;
}

function mean(values: readonly number[]): number | null {
  if (values.length === 0) return null;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

/**
 * A paired score at t depends on both the current H-day outcome and a prior
 * H-day outcome reused as the last-observable baseline. Allow four calendar
 * days for a tradable close and one strict-availability day. Because the
 * Bartlett kernel includes gaps strictly below its bandwidth, add one more
 * day so the full conservative dependence window is represented.
 */
export function edgeEvidenceHacBandwidthDays(horizonDays: number): number {
  return (2 * horizonDays) + OUTCOME_OBSERVATION_TOLERANCE_DAYS + 2;
}

export function computeCalendarTimeHac95(
  samples: readonly CalendarHacSample[],
  bandwidthDays: number,
  bounds?: readonly [number, number],
): CalendarHacEstimate {
  const unavailableReasons: string[] = [];
  if (!Number.isInteger(bandwidthDays) || bandwidthDays <= 0) {
    unavailableReasons.push('invalid_hac_bandwidth');
  }
  if (bounds && (
    !isFiniteNumber(bounds[0])
    || !isFiniteNumber(bounds[1])
    || bounds[0] > bounds[1]
  )) {
    unavailableReasons.push('invalid_hac_bounds');
  }

  const parsed = samples.map((sample, index) => ({
    index,
    date: sample.date,
    day: parseIsoDateDay(sample.date),
    value: sample.value,
  }));
  for (const sample of parsed) {
    if (sample.day === null) unavailableReasons.push(`invalid_sample_date_${sample.index}`);
    if (!isFiniteNumber(sample.value)) unavailableReasons.push(`invalid_sample_value_${sample.index}`);
  }

  const seenDays = new Set<number>();
  for (const sample of parsed) {
    if (sample.day === null) continue;
    if (seenDays.has(sample.day)) unavailableReasons.push(`duplicate_sample_date_${sample.date}`);
    seenDays.add(sample.day);
  }
  if (samples.length < 2) unavailableReasons.push('insufficient_hac_sample');

  const normalizedReasons = uniqueReasons(unavailableReasons);
  if (normalizedReasons.length > 0) {
    return {
      method: EDGE_EVIDENCE_METHOD,
      bandwidth_days: bandwidthDays,
      confidence_level: 0.95,
      sample_size: samples.length,
      mean: null,
      standard_error: null,
      ci95_low: null,
      ci95_high: null,
      lower_bound_positive: false,
      unavailable_reasons: normalizedReasons,
    };
  }

  const ordered = parsed
    .map((sample) => ({ day: sample.day as number, value: sample.value }))
    .sort((left, right) => left.day - right.day);
  const sampleMean = ordered.reduce((sum, sample) => sum + sample.value, 0) / ordered.length;
  const centered = ordered.map((sample) => sample.value - sampleMean);
  let kernelSum = centered.reduce((sum, value) => sum + (value * value), 0);

  for (let right = 0; right < ordered.length; right += 1) {
    for (let left = right - 1; left >= 0; left -= 1) {
      const gapDays = ordered[right].day - ordered[left].day;
      if (gapDays >= bandwidthDays) break;
      const weight = 1 - (gapDays / bandwidthDays);
      kernelSum += 2 * weight * centered[right] * centered[left];
    }
  }

  const sampleSize = ordered.length;
  const varianceOfMean = Math.max(
    0,
    (sampleSize / (sampleSize - 1)) * kernelSum / (sampleSize * sampleSize),
  );
  const standardError = Math.sqrt(varianceOfMean);
  let ciLow = sampleMean - (EDGE_EVIDENCE_FAMILYWISE_CRITICAL_VALUE * standardError);
  let ciHigh = sampleMean + (EDGE_EVIDENCE_FAMILYWISE_CRITICAL_VALUE * standardError);
  if (bounds) {
    ciLow = Math.max(bounds[0], ciLow);
    ciHigh = Math.min(bounds[1], ciHigh);
  }

  return {
    method: EDGE_EVIDENCE_METHOD,
    bandwidth_days: bandwidthDays,
    confidence_level: 0.95,
    sample_size: sampleSize,
    mean: sampleMean,
    standard_error: standardError,
    ci95_low: ciLow,
    ci95_high: ciHigh,
    lower_bound_positive: ciLow > 0,
    unavailable_reasons: [],
  };
}

function validateAndParseRows(
  rows: readonly EdgeEvidenceRow[],
  horizon: EdgeEvidenceHorizonConfig,
  currentModelVersion: string,
  todayDay: number | null,
): { rows: ParsedRow[]; integrityReasons: string[]; currentModelRowCount: number } {
  const parsedRows: ParsedRow[] = [];
  const integrityReasons: string[] = [];
  let currentModelRowCount = 0;

  rows.forEach((row, inputIndex) => {
    if (row.horizon !== horizon.horizon) return;
    const isCurrentModel = row.model_version === currentModelVersion;
    if (isCurrentModel) currentModelRowCount += 1;
    const reasonPrefix = `row_${inputIndex}`;
    const addCurrentModelReason = (reason: string) => {
      if (isCurrentModel) integrityReasons.push(reason);
    };

    if (row.integrity_pass === false) {
      const suppliedReasons = row.integrity_reasons || [];
      if (suppliedReasons.length === 0) addCurrentModelReason(`${reasonPrefix}:integrity_failed`);
      for (const reason of suppliedReasons) addCurrentModelReason(`${reasonPrefix}:${reason}`);
    }

    const predictionDay = parseIsoDateDay(row.prediction_date);
    const targetDay = parseIsoDateDay(row.target_date);
    if (predictionDay === null) addCurrentModelReason(`${reasonPrefix}:invalid_prediction_date`);
    if (targetDay === null) addCurrentModelReason(`${reasonPrefix}:invalid_target_date`);
    if (predictionDay !== null && todayDay !== null && predictionDay > todayDay) {
      addCurrentModelReason(`${reasonPrefix}:future_prediction_date`);
    }
    if (
      predictionDay !== null
      && targetDay !== null
      && targetDay !== predictionDay + horizon.horizon_days
    ) {
      addCurrentModelReason(`${reasonPrefix}:target_horizon_mismatch`);
    }

    const predictedChange = row.predicted_change;
    const actualChange = row.actual_change;
    const actualObservationDay = parseIsoDateDay(row.actual_observation_date);
    const realizedReturn = row.realized_return_pct;
    if (predictedChange !== null && !isFiniteNumber(predictedChange)) {
      addCurrentModelReason(`${reasonPrefix}:invalid_predicted_change`);
    }
    if (actualChange !== null && !isFiniteNumber(actualChange)) {
      addCurrentModelReason(`${reasonPrefix}:invalid_actual_change`);
    }
    if (actualChange !== null && actualObservationDay === null) {
      addCurrentModelReason(`${reasonPrefix}:actual_observation_date_missing_or_invalid`);
    }
    if (actualChange === null && row.actual_observation_date !== null) {
      addCurrentModelReason(`${reasonPrefix}:observation_date_without_actual_outcome`);
    }
    if (realizedReturn !== null && !isFiniteNumber(realizedReturn)) {
      addCurrentModelReason(`${reasonPrefix}:invalid_realized_return`);
    }
    if (realizedReturn !== null && actualChange === null) {
      addCurrentModelReason(`${reasonPrefix}:return_without_actual_outcome`);
    }
    if (isCurrentModel && predictedChange === null) {
      addCurrentModelReason(`${reasonPrefix}:current_model_prediction_missing`);
    }
    if (actualChange !== null && targetDay !== null && todayDay !== null && targetDay > todayDay) {
      addCurrentModelReason(`${reasonPrefix}:future_target_evaluated`);
    }
    if (
      actualChange !== null
      && actualObservationDay !== null
      && targetDay !== null
      && actualObservationDay < targetDay
    ) {
      addCurrentModelReason(`${reasonPrefix}:actual_observed_before_target`);
    }
    if (
      actualChange !== null
      && actualObservationDay !== null
      && todayDay !== null
      && actualObservationDay > todayDay
    ) {
      addCurrentModelReason(`${reasonPrefix}:future_actual_observation`);
    }

    if (actualChange !== null && row.evaluated_at) {
      const evaluatedAt = new Date(row.evaluated_at);
      if (!Number.isFinite(evaluatedAt.getTime())) {
        addCurrentModelReason(`${reasonPrefix}:invalid_evaluated_at`);
      } else if (targetDay !== null && evaluatedAt.getTime() < targetDay * DAY_MS) {
        addCurrentModelReason(`${reasonPrefix}:evaluated_before_target`);
      }
    }

    const structurallyValid = predictionDay !== null
      && targetDay !== null
      && targetDay === predictionDay + horizon.horizon_days
      && (predictedChange === null || isFiniteNumber(predictedChange))
      && (actualChange === null || isFiniteNumber(actualChange))
      && (actualChange === null || actualObservationDay !== null)
      && (realizedReturn === null || isFiniteNumber(realizedReturn));
    if (!structurallyValid) return;

    parsedRows.push({
      input_index: inputIndex,
      prediction_date: row.prediction_date,
      prediction_day: predictionDay,
      target_date: row.target_date as string,
      target_day: targetDay,
      predicted_change: predictedChange,
      actual_change: actualChange,
      actual_observation_date: row.actual_observation_date,
      actual_observation_day: actualObservationDay,
      realized_return_pct: realizedReturn,
      model_version: row.model_version,
    });
  });

  const rowsByPredictionDate = new Map<string, ParsedRow[]>();
  for (const row of parsedRows) {
    const duplicateKey = `${row.model_version || 'unversioned'}\u0000${row.prediction_date}`;
    const bucket = rowsByPredictionDate.get(duplicateKey) || [];
    bucket.push(row);
    rowsByPredictionDate.set(duplicateKey, bucket);
  }
  const duplicateKeys = new Set<string>();
  for (const [duplicateKey, duplicates] of rowsByPredictionDate.entries()) {
    if (duplicates.length > 1) {
      duplicateKeys.add(duplicateKey);
      if (duplicates[0].model_version === currentModelVersion) {
        integrityReasons.push(`duplicate_model_prediction_date_${duplicates[0].model_version}_${duplicates[0].prediction_date}`);
      }
    }
  }

  return {
    rows: parsedRows.filter((row) => (
      !duplicateKeys.has(`${row.model_version || 'unversioned'}\u0000${row.prediction_date}`)
    )),
    integrityReasons: uniqueReasons(integrityReasons),
    currentModelRowCount,
  };
}

export function buildCausalPairedObservations(
  rows: readonly EdgeEvidenceRow[],
  now: Date | string,
  args: {
    horizon: EdgeEvidenceHorizonConfig;
    current_model_version: string;
    round_trip_cost_pct: number;
  },
): CausalPairBuildResult {
  const parsedNow = parseNow(now);
  const parsed = validateAndParseRows(
    rows,
    args.horizon,
    args.current_model_version,
    parsedNow.day,
  );
  const integrityReasons = [...parsed.integrityReasons];
  const currentRows = parsed.rows
    .filter((row) => row.model_version === args.current_model_version)
    .sort((left, right) => left.prediction_day - right.prediction_day);
  const baselineCandidates = parsed.rows
    .filter((row) => (
      row.model_version === args.current_model_version
      && row.actual_change !== null
      && row.actual_observation_day !== null
    ))
    .sort((left, right) => (
      Math.max(left.target_day, left.actual_observation_day as number)
      - Math.max(right.target_day, right.actual_observation_day as number)
      || left.target_day - right.target_day
      || left.prediction_day - right.prediction_day
      || left.input_index - right.input_index
    ));

  const latestAnyModelRow = [...parsed.rows]
    .filter((row) => row.predicted_change !== null)
    .sort((left, right) => left.prediction_day - right.prediction_day)
    .at(-1);
  if (latestAnyModelRow && latestAnyModelRow.model_version !== args.current_model_version) {
    integrityReasons.push('current_model_is_not_latest_prediction_version');
  }

  const latestPredictionRow = currentRows
    .filter((row) => row.predicted_change !== null)
    .at(-1);
  const completedCurrentRows = currentRows.filter((row) => (
    row.predicted_change !== null
    && row.actual_change !== null
    && row.actual_observation_day !== null
    && (parsedNow.day === null || row.target_day <= parsedNow.day)
    && (parsedNow.day === null || (row.actual_observation_day as number) <= parsedNow.day)
  ));
  const latestEvaluatedTarget = [...completedCurrentRows]
    .sort((left, right) => left.target_day - right.target_day)
    .at(-1);
  const latestActualObservation = [...completedCurrentRows]
    .sort((left, right) => (
      (left.actual_observation_day as number) - (right.actual_observation_day as number)
    ))
    .at(-1);
  const maturedOutcomeMissingCount = currentRows.filter((row) => (
    row.predicted_change !== null
    && row.actual_change === null
    && parsedNow.day !== null
    && row.target_day + OUTCOME_OBSERVATION_TOLERANCE_DAYS < parsedNow.day
  )).length;

  const observations: PairedEdgeObservation[] = [];
  let baselineCursor = 0;
  let latestBaseline: ParsedRow | null = null;
  let causalBaselineUnavailableCount = 0;

  for (const current of completedCurrentRows) {
    while (
      baselineCursor < baselineCandidates.length
      && Math.max(
        baselineCandidates[baselineCursor].target_day,
        baselineCandidates[baselineCursor].actual_observation_day as number,
      ) < current.prediction_day
    ) {
      const candidate = baselineCandidates[baselineCursor];
      if (
        latestBaseline === null
        || candidate.target_day > latestBaseline.target_day
        || (
          candidate.target_day === latestBaseline.target_day
          && candidate.prediction_day > latestBaseline.prediction_day
        )
      ) {
        latestBaseline = candidate;
      }
      baselineCursor += 1;
    }
    if (!latestBaseline || latestBaseline.actual_change === null) {
      causalBaselineUnavailableCount += 1;
      continue;
    }
    if (
      latestBaseline.target_day >= current.prediction_day
      || latestBaseline.actual_observation_day === null
      || latestBaseline.actual_observation_day >= current.prediction_day
    ) {
      integrityReasons.push(`noncausal_baseline_${current.prediction_date}`);
      continue;
    }

    const modelDirection = directionSign(current.predicted_change as number);
    const baselineDirection = directionSign(latestBaseline.actual_change);
    const actualDirection = directionSign(current.actual_change as number);
    const modelCorrect: 0 | 1 = modelDirection === actualDirection ? 1 : 0;
    const baselineCorrect: 0 | 1 = baselineDirection === actualDirection ? 1 : 0;
    const directionDifference = (modelCorrect - baselineCorrect) as -1 | 0 | 1;

    let modelReturn: number | null = null;
    let baselineReturn: number | null = null;
    let returnDifference: number | null = null;
    if (current.realized_return_pct !== null && isFiniteNumber(args.round_trip_cost_pct)) {
      modelReturn = (modelDirection * current.realized_return_pct)
        - (modelDirection === 0 ? 0 : args.round_trip_cost_pct);
      baselineReturn = (baselineDirection * current.realized_return_pct)
        - (baselineDirection === 0 ? 0 : args.round_trip_cost_pct);
      returnDifference = modelReturn - baselineReturn;
    }

    observations.push({
      prediction_date: current.prediction_date,
      target_date: current.target_date,
      baseline_prediction_date: latestBaseline.prediction_date,
      baseline_target_date: latestBaseline.target_date,
      baseline_actual_observation_date: latestBaseline.actual_observation_date as string,
      model_direction: modelDirection,
      baseline_direction: baselineDirection,
      actual_direction: actualDirection,
      model_correct: modelCorrect,
      baseline_correct: baselineCorrect,
      direction_difference: directionDifference,
      model_signed_return_after_cost_pct: modelReturn,
      baseline_signed_return_after_cost_pct: baselineReturn,
      signed_return_difference_after_cost_pct: returnDifference,
    });
  }

  return {
    observations,
    integrity_reasons: uniqueReasons(integrityReasons),
    current_model_row_count: parsed.currentModelRowCount,
    completed_current_model_row_count: completedCurrentRows.length,
    causal_baseline_unavailable_count: causalBaselineUnavailableCount,
    matured_outcome_missing_count: maturedOutcomeMissingCount,
    latest_prediction_date: latestPredictionRow?.prediction_date || null,
    latest_evaluated_target_date: latestEvaluatedTarget?.target_date || null,
    latest_actual_observation_date: latestActualObservation?.actual_observation_date || null,
  };
}

function buildMetricEvidence(
  observations: readonly PairedEdgeObservation[],
  horizonDays: number,
  kind: 'direction' | 'return',
): EdgeMetricEvidence {
  const metricRows = observations.flatMap((observation) => {
    if (kind === 'direction') {
      return [{
        date: observation.prediction_date,
        model: observation.model_correct,
        baseline: observation.baseline_correct,
        difference: observation.direction_difference,
      }];
    }
    if (
      observation.model_signed_return_after_cost_pct === null
      || observation.baseline_signed_return_after_cost_pct === null
      || observation.signed_return_difference_after_cost_pct === null
    ) {
      return [];
    }
    return [{
      date: observation.prediction_date,
      model: observation.model_signed_return_after_cost_pct,
      baseline: observation.baseline_signed_return_after_cost_pct,
      difference: observation.signed_return_difference_after_cost_pct,
    }];
  });
  const hac = computeCalendarTimeHac95(
    metricRows.map((row) => ({ date: row.date, value: row.difference })),
    edgeEvidenceHacBandwidthDays(horizonDays),
    kind === 'direction' ? [-1, 1] : undefined,
  );
  return {
    ...hac,
    model_mean: mean(metricRows.map((row) => row.model)),
    baseline_mean: mean(metricRows.map((row) => row.baseline)),
    uplift: hac.mean,
  };
}

function resolveThresholds(overrides?: Partial<EdgeEvidenceThresholds>): EdgeEvidenceThresholds {
  return {
    ...DEFAULT_EDGE_EVIDENCE_THRESHOLDS,
    ...(overrides || {}),
  };
}

function validateThresholds(thresholds: EdgeEvidenceThresholds): string[] {
  const reasons: string[] = [];
  if (!Number.isInteger(thresholds.minimum_paired_observations) || thresholds.minimum_paired_observations < 2) {
    reasons.push('invalid_minimum_paired_observations');
  }
  if (!isFiniteNumber(thresholds.paired_observations_per_horizon_day) || thresholds.paired_observations_per_horizon_day <= 0) {
    reasons.push('invalid_paired_observations_per_horizon_day');
  }
  if (!Number.isInteger(thresholds.minimum_discordant_pairs) || thresholds.minimum_discordant_pairs < 1) {
    reasons.push('invalid_minimum_discordant_pairs');
  }
  if (!isFiniteNumber(thresholds.minimum_span_horizon_multiples) || thresholds.minimum_span_horizon_multiples <= 0) {
    reasons.push('invalid_minimum_span_horizon_multiples');
  }
  if (
    !isFiniteNumber(thresholds.minimum_weekday_coverage_ratio)
    || thresholds.minimum_weekday_coverage_ratio < 0
    || thresholds.minimum_weekday_coverage_ratio > 1
  ) {
    reasons.push('invalid_minimum_weekday_coverage_ratio');
  }
  if (!Number.isInteger(thresholds.maximum_prediction_age_days) || thresholds.maximum_prediction_age_days < 0) {
    reasons.push('invalid_maximum_prediction_age_days');
  }
  if (!Number.isInteger(thresholds.maximum_actual_observation_age_days) || thresholds.maximum_actual_observation_age_days < 0) {
    reasons.push('invalid_maximum_actual_observation_age_days');
  }
  return reasons;
}

function buildWindow(
  rows: readonly EdgeEvidenceRow[],
  now: Date | string,
  nowDay: number | null,
  horizon: EdgeEvidenceHorizonConfig,
  options: EdgeEvidenceOptions,
  thresholds: EdgeEvidenceThresholds,
  sharedIntegrityReasons: readonly string[],
): EdgeEvidenceWindow {
  const currentModelVersion = String(options.current_model_version || '').trim();
  const pairBuild = buildCausalPairedObservations(rows, now, {
    horizon,
    current_model_version: currentModelVersion,
    round_trip_cost_pct: options.round_trip_cost_pct,
  });
  const integrityReasons = [
    ...sharedIntegrityReasons,
    ...pairBuild.integrity_reasons,
  ];
  if (pairBuild.current_model_row_count === 0) integrityReasons.push('current_model_rows_unavailable');
  const integrityGate = gate(integrityReasons);

  const observations = pairBuild.observations;
  const returnObservations = observations.filter((observation) => (
    observation.signed_return_difference_after_cost_pct !== null
  ));
  const pairedDays = observations
    .map((observation) => parseIsoDateDay(observation.prediction_date))
    .filter((day): day is number => day !== null)
    .sort((left, right) => left - right);
  const firstPairedDay = pairedDays[0] ?? null;
  const lastPairedDay = pairedDays.at(-1) ?? null;
  const calendarSpanDays = firstPairedDay === null || lastPairedDay === null
    ? 0
    : lastPairedDay - firstPairedDay + 1;
  const expectedWeekdays = firstPairedDay === null || lastPairedDay === null
    ? 0
    : countWeekdaysInclusive(firstPairedDay, lastPairedDay);
  const observedWeekdays = new Set(pairedDays.filter(isWeekday)).size;
  const weekdayCoverageRatio = expectedWeekdays > 0 ? observedWeekdays / expectedWeekdays : 0;
  const discordantPairs = observations.filter((observation) => (
    observation.model_correct !== observation.baseline_correct
  )).length;
  const latestPredictionDay = parseIsoDateDay(pairBuild.latest_prediction_date);
  const latestActualObservationDay = parseIsoDateDay(pairBuild.latest_actual_observation_date);
  const latestPredictionAge = nowDay === null || latestPredictionDay === null
    ? null
    : nowDay - latestPredictionDay;
  const latestActualObservationAge = nowDay === null || latestActualObservationDay === null
    ? null
    : nowDay - latestActualObservationDay;
  const minimumRequiredPairedSample = Math.max(
    thresholds.minimum_paired_observations,
    Math.ceil(thresholds.paired_observations_per_horizon_day * horizon.horizon_days),
  );
  const minimumRequiredSpanDays = Math.ceil(
    thresholds.minimum_span_horizon_multiples * horizon.horizon_days,
  );

  const eligibilityReasons: string[] = [];
  if (observations.length < minimumRequiredPairedSample) {
    eligibilityReasons.push('insufficient_paired_sample');
  }
  if (discordantPairs < thresholds.minimum_discordant_pairs) {
    eligibilityReasons.push('insufficient_discordant_pairs');
  }
  if (calendarSpanDays < minimumRequiredSpanDays) {
    eligibilityReasons.push('insufficient_calendar_span');
  }
  if (weekdayCoverageRatio < thresholds.minimum_weekday_coverage_ratio) {
    eligibilityReasons.push('insufficient_weekday_coverage');
  }
  if (latestPredictionAge === null) {
    eligibilityReasons.push('prediction_freshness_unavailable');
  } else if (latestPredictionAge < 0 || latestPredictionAge > thresholds.maximum_prediction_age_days) {
    eligibilityReasons.push('stale_prediction_stream');
  }
  if (latestActualObservationAge === null) {
    eligibilityReasons.push('actual_observation_freshness_unavailable');
  } else if (
    latestActualObservationAge < 0
    || latestActualObservationAge > thresholds.maximum_actual_observation_age_days
  ) {
    eligibilityReasons.push('stale_actual_observation');
  }
  if (pairBuild.matured_outcome_missing_count > 0) {
    eligibilityReasons.push('matured_outcomes_missing');
  }
  if (returnObservations.length !== observations.length) {
    eligibilityReasons.push('signed_return_outcomes_incomplete');
  }
  if (pairBuild.causal_baseline_unavailable_count > 0 && observations.length === 0) {
    eligibilityReasons.push('causal_baseline_unavailable');
  }
  const eligibilityGate = gate(eligibilityReasons);

  const direction = buildMetricEvidence(observations, horizon.horizon_days, 'direction');
  const signedReturn = buildMetricEvidence(observations, horizon.horizon_days, 'return');
  const performanceReasons: string[] = [];
  if (direction.uplift === null || direction.ci95_low === null) {
    performanceReasons.push('direction_evidence_unavailable');
  } else {
    if (direction.uplift <= 0) performanceReasons.push('direction_uplift_not_positive');
    if (!direction.lower_bound_positive) performanceReasons.push('direction_hac_lower_bound_not_positive');
  }
  if (signedReturn.uplift === null || signedReturn.ci95_low === null) {
    performanceReasons.push('signed_return_evidence_unavailable');
  } else {
    if (signedReturn.model_mean === null || signedReturn.model_mean <= 0) {
      performanceReasons.push('model_signed_return_after_cost_not_positive');
    }
    if (signedReturn.uplift <= 0) performanceReasons.push('signed_return_uplift_not_positive');
    if (!signedReturn.lower_bound_positive) performanceReasons.push('signed_return_hac_lower_bound_not_positive');
  }
  const performanceGate = gate(performanceReasons);
  const evidenceReasons = [
    ...integrityGate.reasons.map((reason) => `integrity:${reason}`),
    ...eligibilityGate.reasons.map((reason) => `eligibility:${reason}`),
    ...performanceGate.reasons.map((reason) => `performance:${reason}`),
  ];

  return {
    horizon: horizon.horizon,
    horizon_days: horizon.horizon_days,
    as_of: pairBuild.latest_evaluated_target_date,
    current_model_version: options.current_model_version,
    paired_sample_size: observations.length,
    signed_return_sample_size: returnObservations.length,
    discordant_pairs: discordantPairs,
    causal_baseline_unavailable_count: pairBuild.causal_baseline_unavailable_count,
    matured_outcome_missing_count: pairBuild.matured_outcome_missing_count,
    calendar_span_days: calendarSpanDays,
    expected_weekdays: expectedWeekdays,
    observed_weekdays: observedWeekdays,
    weekday_coverage_ratio: weekdayCoverageRatio,
    latest_prediction_date: pairBuild.latest_prediction_date,
    latest_prediction_age_days: latestPredictionAge,
    latest_evaluated_target_date: pairBuild.latest_evaluated_target_date,
    latest_actual_observation_date: pairBuild.latest_actual_observation_date,
    latest_actual_observation_age_days: latestActualObservationAge,
    minimum_required_paired_sample: minimumRequiredPairedSample,
    minimum_required_span_days: minimumRequiredSpanDays,
    direction,
    signed_return_after_cost_pct: signedReturn,
    integrity_gate: integrityGate,
    eligibility_gate: eligibilityGate,
    performance_gate: performanceGate,
    evidence_gate: gate(evidenceReasons),
  };
}

export function buildEdgeEvidenceReport(
  rows: readonly EdgeEvidenceRow[],
  now: Date | string,
  options: EdgeEvidenceOptions,
): EdgeEvidenceReport {
  const parsedNow = parseNow(now);
  const thresholds = resolveThresholds(options.thresholds);
  const sharedIntegrityReasons: string[] = [];
  if (parsedNow.instant === null || parsedNow.day === null) sharedIntegrityReasons.push('invalid_report_time');
  if (!String(options.current_model_version || '').trim()) sharedIntegrityReasons.push('current_model_version_missing');
  if (!isFiniteNumber(options.round_trip_cost_pct) || options.round_trip_cost_pct < 0) {
    sharedIntegrityReasons.push('invalid_round_trip_cost');
  }
  if (!options.upstream_integrity?.pass) {
    const upstreamReasons = options.upstream_integrity?.reasons || [];
    if (upstreamReasons.length === 0) sharedIntegrityReasons.push('upstream_integrity_failed');
    for (const reason of upstreamReasons) sharedIntegrityReasons.push(`upstream:${reason}`);
  }
  sharedIntegrityReasons.push(...validateThresholds(thresholds));

  const configuredHorizons = new Set<string>();
  for (const horizon of options.horizons) {
    if (!horizon.horizon.trim()) sharedIntegrityReasons.push('horizon_name_missing');
    if (!Number.isInteger(horizon.horizon_days) || horizon.horizon_days <= 0) {
      sharedIntegrityReasons.push(`invalid_horizon_days_${horizon.horizon || 'unknown'}`);
    }
    if (configuredHorizons.has(horizon.horizon)) {
      sharedIntegrityReasons.push(`duplicate_horizon_config_${horizon.horizon}`);
    }
    configuredHorizons.add(horizon.horizon);
  }
  if (options.horizons.length === 0) sharedIntegrityReasons.push('horizons_unavailable');

  const windows = options.horizons.map((horizon) => buildWindow(
    rows,
    now,
    parsedNow.day,
    horizon,
    options,
    thresholds,
    uniqueReasons(sharedIntegrityReasons),
  ));
  const integrityReasons = [
    ...sharedIntegrityReasons,
    ...windows.flatMap((window) => (
      window.integrity_gate.pass
        ? []
        : window.integrity_gate.reasons.map((reason) => `${window.horizon}:${reason}`)
    )),
  ];
  const evidenceReasons = [
    ...gate(integrityReasons).reasons.map((reason) => `integrity:${reason}`),
    ...windows.flatMap((window) => (
      window.evidence_gate.pass
        ? []
        : window.evidence_gate.reasons.map((reason) => `${window.horizon}:${reason}`)
    )),
  ];
  const performanceReasons = [
    ...(windows.length === 0 ? ['horizons_unavailable'] : []),
    ...windows.flatMap((window) => (
      window.performance_gate.pass
        ? []
        : window.performance_gate.reasons.map((reason) => `${window.horizon}:${reason}`)
    )),
  ];
  const integrityGate = gate(integrityReasons);
  const performanceGate = gate(performanceReasons);
  const evidenceGate = gate(evidenceReasons);
  const promotionGate = gate([
    ...integrityGate.reasons.map((reason) => `integrity:${reason}`),
    ...performanceGate.reasons.map((reason) => `performance:${reason}`),
    ...evidenceGate.reasons.map((reason) => `evidence:${reason}`),
  ]);

  return {
    as_of: parsedNow.instant?.toISOString() || null,
    current_model_version: options.current_model_version,
    method: EDGE_EVIDENCE_METHOD,
    windows,
    integrity_gate: integrityGate,
    performance_gate: performanceGate,
    evidence_gate: evidenceGate,
    promotion_gate: promotionGate,
  };
}
