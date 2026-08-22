import {
  MARKET_EVIDENCE_MODEL_FAMILY,
  MARKET_EVIDENCE_MODEL_VERSION,
  MARKET_EVIDENCE_TARGET_METRIC,
  fetchMarketPredictionEvidenceCounts,
  fetchMarketPredictionEvidenceRows,
  type MarketPredictionEvidence,
} from '../data/market-evidence';
import {
  EDGE_EVIDENCE_COMPARISONS_PER_LOOK,
  EDGE_EVIDENCE_FAMILYWISE_CRITICAL_VALUE,
  EDGE_EVIDENCE_MAX_UNIQUE_LOOKS,
  EDGE_EVIDENCE_METHOD,
  buildEdgeEvidenceReport,
  type EdgeEvidenceRow,
} from './edge-evidence';
import type {
  CalibrationQuality,
  EdgeDiagnosticsHorizon,
  EdgeDiagnosticsReport,
  EdgeDiagnosticsWindow,
} from '../types';

const ROUND_TRIP_COST_PCT = 0.10;

function isExplicitWarmupRow(
  row: MarketPredictionEvidence,
  horizon: EdgeDiagnosticsHorizon,
): boolean {
  const suffix = horizon === '7d' ? '7d' : '30d';
  if (row[`predicted_return_${suffix}`] !== null) return false;
  // The schema records one shared cutoff for both horizons. A matured 7d
  // training row can therefore set it while 30d is still a valid zero-sample
  // warm-up; recognize warm-up state from the horizon-local fields.
  if (row[`sample_size_${suffix}`] !== 0) return false;
  try {
    const methodology = JSON.parse(row.methodology_json) as Record<string, unknown>;
    return methodology.model_version === row.model_version
      && methodology.target_metric === row.target_metric
      && methodology.training_source === 'prior_rows_in_immutable_market_prediction_evidence';
  } catch {
    return false;
  }
}

function addCalendarDays(dateKey: string, days: number): string | null {
  const date = new Date(`${dateKey}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime())) return null;
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function newYorkDateKey(value: string): string | null {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date);
  const part = (type: Intl.DateTimeFormatPartTypes) =>
    parts.find((entry) => entry.type === type)?.value;
  const year = part('year');
  const month = part('month');
  const day = part('day');
  return year && month && day ? `${year}-${month}-${day}` : null;
}

function validateStoredEvidence(
  row: MarketPredictionEvidence,
  horizon: EdgeDiagnosticsHorizon,
): string[] {
  const suffix = horizon === '7d' ? '7d' : '30d';
  const horizonDays = horizon === '7d' ? 7 : 30;
  const targetDate = row[`target_date_${suffix}`];
  const predictedReturn = row[`predicted_return_${suffix}`];
  const actualReturn = row[`actual_return_${suffix}`];
  const actualObservationDate = row[`actual_observation_date_${suffix}`];
  const evaluatedAt = row[`evaluated_at_${suffix}`];
  const outcomeStatus = row[`outcome_status_${suffix}`];
  const unavailableReason = row[`outcome_unavailable_reason_${suffix}`];
  const sampleSize = row[`sample_size_${suffix}`];
  const reasons: string[] = [];

  if (row.model_family !== MARKET_EVIDENCE_MODEL_FAMILY) reasons.push('model_family_mismatch');
  if (row.model_version !== MARKET_EVIDENCE_MODEL_VERSION) reasons.push('model_version_mismatch');
  if (row.target_metric !== MARKET_EVIDENCE_TARGET_METRIC) reasons.push('target_metric_mismatch');
  if (!row.feature_snapshot_id) reasons.push('feature_snapshot_missing');
  if (row.benchmark_observation_date !== row.prediction_date) reasons.push('benchmark_not_current_at_prediction');
  if (newYorkDateKey(row.prediction_available_at) !== row.prediction_date) {
    reasons.push('prediction_availability_not_on_decision_date');
  }
  if (addCalendarDays(row.prediction_date, horizonDays) !== targetDate) {
    reasons.push('target_horizon_mismatch');
  }
  if (!Number.isInteger(sampleSize) || sampleSize < 0) reasons.push('invalid_training_sample_size');
  if (predictedReturn === null && !isExplicitWarmupRow(row, horizon)) reasons.push('prediction_missing');
  if (row.training_cutoff_date && row.training_cutoff_date > row.prediction_date) {
    reasons.push('training_cutoff_after_prediction');
  }
  if (sampleSize > 0 && !row.training_cutoff_date) reasons.push('training_cutoff_missing');
  try {
    const methodology = JSON.parse(row.methodology_json) as Record<string, unknown>;
    if (methodology.model_version !== row.model_version) reasons.push('methodology_version_mismatch');
    if (methodology.target_metric !== row.target_metric) reasons.push('methodology_target_mismatch');
  } catch {
    reasons.push('methodology_invalid_json');
  }

  if (!['pending', 'observed', 'unavailable'].includes(outcomeStatus)) {
    reasons.push('outcome_status_invalid');
  } else if (outcomeStatus === 'pending') {
    if (actualReturn !== null || actualObservationDate !== null || evaluatedAt !== null || unavailableReason !== null) {
      reasons.push('pending_outcome_has_terminal_fields');
    }
  } else if (outcomeStatus === 'observed') {
    if (actualReturn === null || actualObservationDate === null || evaluatedAt === null || unavailableReason !== null) {
      reasons.push('observed_outcome_fields_incomplete');
    }
  } else if (
    actualReturn !== null
    || actualObservationDate !== null
    || evaluatedAt === null
    || typeof unavailableReason !== 'string'
    || unavailableReason.length === 0
  ) {
    reasons.push('unavailable_outcome_fields_invalid');
  }
  if (actualObservationDate !== null) {
    const lastAllowed = addCalendarDays(targetDate, 4);
    if (actualObservationDate < targetDate) reasons.push('actual_observed_before_target');
    if (lastAllowed && actualObservationDate > lastAllowed) reasons.push('actual_observation_outside_tolerance');
  }
  if (evaluatedAt !== null) {
    const evaluated = new Date(evaluatedAt);
    const observationStart = actualObservationDate
      ? new Date(`${actualObservationDate}T00:00:00.000Z`)
      : null;
    if (Number.isNaN(evaluated.getTime())) reasons.push('evaluated_at_invalid');
    if (observationStart && evaluated.getTime() < observationStart.getTime()) {
      reasons.push('evaluated_before_observation');
    }
  }

  return [...new Set(reasons)];
}

export function unfoldStoredMarketEvidence(
  rows: readonly MarketPredictionEvidence[],
  horizons: readonly EdgeDiagnosticsHorizon[],
): EdgeEvidenceRow[] {
  return rows.flatMap((row) => horizons.flatMap((horizon): EdgeEvidenceRow[] => {
    const suffix = horizon === '7d' ? '7d' : '30d';
    const reasons = validateStoredEvidence(row, horizon);
    if (isExplicitWarmupRow(row, horizon) && reasons.length === 0) return [];
    const actualReturn = row[`actual_return_${suffix}`];
    return [{
      horizon,
      prediction_date: row.prediction_date,
      target_date: row[`target_date_${suffix}`],
      predicted_change: row[`predicted_return_${suffix}`],
      actual_change: actualReturn,
      actual_observation_date: row[`actual_observation_date_${suffix}`],
      realized_return_pct: actualReturn,
      model_version: row.model_version,
      evaluated_at: row[`evaluated_at_${suffix}`],
      integrity_pass: reasons.length === 0,
      integrity_reasons: reasons,
    }];
  }));
}

function qualityForWindow(sampleSize: number, minimumSample: number, eligibilityPass: boolean): CalibrationQuality {
  if (eligibilityPass) return 'ROBUST';
  if (sampleSize >= Math.max(20, Math.floor(minimumSample / 2))) return 'LIMITED';
  return 'INSUFFICIENT';
}

export async function buildMarketEdgeDiagnosticsReport(
  db: D1Database,
  requestedHorizons: EdgeDiagnosticsHorizon[],
  now: Date = new Date(),
): Promise<EdgeDiagnosticsReport> {
  const horizons = [...new Set(requestedHorizons)].filter(
    (horizon): horizon is EdgeDiagnosticsHorizon => horizon === '7d' || horizon === '30d',
  );
  const [storedRows, evidenceCounts] = await Promise.all([
    fetchMarketPredictionEvidenceRows(db, {
      modelVersion: MARKET_EVIDENCE_MODEL_VERSION,
      limit: EDGE_EVIDENCE_MAX_UNIQUE_LOOKS,
    }),
    fetchMarketPredictionEvidenceCounts(db, undefined, MARKET_EVIDENCE_MODEL_VERSION),
  ]);
  const sequentialIntegrityReasons = evidenceCounts.total > EDGE_EVIDENCE_MAX_UNIQUE_LOOKS
    ? ['sequential_analysis_look_budget_exhausted']
    : [];
  const pure = buildEdgeEvidenceReport(
    unfoldStoredMarketEvidence(storedRows, horizons),
    now,
    {
      horizons: horizons.map((horizon) => ({
        horizon,
        horizon_days: horizon === '7d' ? 7 : 30,
      })),
      current_model_version: MARKET_EVIDENCE_MODEL_VERSION,
      round_trip_cost_pct: ROUND_TRIP_COST_PCT,
      upstream_integrity: {
        pass: sequentialIntegrityReasons.length === 0,
        reasons: sequentialIntegrityReasons,
      },
    },
  );

  const windows: EdgeDiagnosticsWindow[] = pure.windows.map((window) => {
    const qualityBand = qualityForWindow(
      window.paired_sample_size,
      window.minimum_required_paired_sample,
      window.eligibility_gate.pass,
    );
    return {
      horizon: window.horizon as EdgeDiagnosticsHorizon,
      as_of: window.latest_prediction_date ? `${window.latest_prediction_date}T00:00:00.000Z` : null,
      sample_size: window.paired_sample_size,
      model_direction_accuracy: window.direction.model_mean,
      baseline_direction_accuracy: window.direction.baseline_mean,
      uplift_vs_baseline: window.direction.uplift,
      uplift_ci95_low: window.direction.ci95_low,
      uplift_ci95_high: window.direction.ci95_high,
      lower_bound_positive: window.direction.lower_bound_positive,
      minimum_reliable_sample: window.minimum_required_paired_sample,
      quality_band: qualityBand,
      baseline_strategy: 'last_observable_actual_direction',
      model_version: MARKET_EVIDENCE_MODEL_VERSION,
      horizon_days: window.horizon_days,
      hac_bandwidth_days: window.direction.bandwidth_days,
      discordant_pairs: window.discordant_pairs,
      calendar_span_days: window.calendar_span_days,
      weekday_coverage_ratio: window.weekday_coverage_ratio,
      latest_prediction_date: window.latest_prediction_date,
      latest_prediction_age_days: window.latest_prediction_age_days,
      latest_evaluated_target_date: window.latest_evaluated_target_date,
      latest_actual_observation_date: window.latest_actual_observation_date,
      latest_actual_observation_age_days: window.latest_actual_observation_age_days,
      signed_return_after_cost_pct: window.signed_return_after_cost_pct,
      integrity_gate: window.integrity_gate,
      eligibility_gate: window.eligibility_gate,
      performance_gate: window.performance_gate,
      evidence_gate: window.evidence_gate,
      leakage_sentinel: {
        pass: window.integrity_gate.pass,
        violation_count: window.integrity_gate.reasons.length,
        reasons: window.integrity_gate.reasons,
      },
      calibration_diagnostics: {
        brier_score: null,
        ece: null,
        log_loss: null,
        quality_band: qualityBand,
        minimum_reliable_sample: window.minimum_required_paired_sample,
        insufficient_reasons: window.eligibility_gate.reasons,
      },
    };
  });
  const policyAlignmentGate = {
    pass: false,
    reasons: ['validated_spy_forecast_not_bound_to_plan_sizing_or_theme_policy'],
  };
  return {
    as_of: pure.as_of || now.toISOString(),
    basis: 'immutable_spy_return_evidence_vs_last_observable_actual_direction',
    model_version: MARKET_EVIDENCE_MODEL_VERSION,
    method: EDGE_EVIDENCE_METHOD,
    inference_control: {
      strategy: 'finite_horizon_bonferroni',
      familywise_confidence_level: 0.95,
      maximum_unique_looks: EDGE_EVIDENCE_MAX_UNIQUE_LOOKS,
      simultaneous_comparisons_per_look: EDGE_EVIDENCE_COMPARISONS_PER_LOOK,
      critical_value: EDGE_EVIDENCE_FAMILYWISE_CRITICAL_VALUE,
      coverage_basis: 'asymptotic_hac_normal_approximation',
      finite_sample_guarantee: false,
    },
    policy_alignment_gate: policyAlignmentGate,
    windows,
    integrity_gate: pure.integrity_gate,
    performance_gate: pure.performance_gate,
    evidence_gate: pure.evidence_gate,
    promotion_gate: {
      pass: false,
      reasons: [
        ...pure.promotion_gate.reasons,
        ...policyAlignmentGate.reasons,
      ],
    },
  };
}

export async function fetchCurrentMarketEvidenceEvaluationSampleSize(db: D1Database): Promise<number> {
  const counts = await fetchMarketPredictionEvidenceCounts(
    db,
    undefined,
    MARKET_EVIDENCE_MODEL_VERSION,
  );
  return Math.min(counts.with_7d_outcome, counts.with_30d_outcome);
}
