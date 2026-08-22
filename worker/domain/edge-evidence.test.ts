import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildCausalPairedObservations,
  buildEdgeEvidenceReport,
  computeCalendarTimeHac95,
  edgeEvidenceHacBandwidthDays,
  type EdgeEvidenceOptions,
  type EdgeEvidenceRow,
} from './edge-evidence.js';

const DAY_MS = 24 * 60 * 60 * 1000;

function addDays(date: string, days: number): string {
  const timestamp = Date.parse(`${date}T00:00:00.000Z`);
  return new Date(timestamp + (days * DAY_MS)).toISOString().slice(0, 10);
}

function makeRow(args: {
  predictionDate: string;
  horizonDays?: number;
  predictedChange?: number | null;
  actualChange?: number | null;
  observationDate?: string | null;
  realizedReturnPct?: number | null;
  modelVersion?: string | null;
  targetDate?: string | null;
  integrityPass?: boolean;
}): EdgeEvidenceRow {
  const horizonDays = args.horizonDays ?? 2;
  const actualChange = args.actualChange === undefined ? null : args.actualChange;
  const targetDate = args.targetDate === undefined
    ? addDays(args.predictionDate, horizonDays)
    : args.targetDate;
  return {
    horizon: `${horizonDays}d`,
    prediction_date: args.predictionDate,
    target_date: targetDate,
    predicted_change: args.predictedChange === undefined ? 1 : args.predictedChange,
    actual_change: actualChange,
    actual_observation_date: args.observationDate === undefined
      ? (actualChange === null ? null : targetDate)
      : args.observationDate,
    realized_return_pct: args.realizedReturnPct === undefined ? null : args.realizedReturnPct,
    model_version: args.modelVersion === undefined ? 'v1' : args.modelVersion,
    integrity_pass: args.integrityPass,
  };
}

function permissiveOptions(overrides: Partial<EdgeEvidenceOptions> = {}): EdgeEvidenceOptions {
  return {
    horizons: [{ horizon: '2d', horizon_days: 2 }],
    current_model_version: 'v1',
    round_trip_cost_pct: 0.1,
    upstream_integrity: { pass: true, reasons: [] },
    thresholds: {
      minimum_paired_observations: 4,
      paired_observations_per_horizon_day: 1,
      minimum_discordant_pairs: 3,
      minimum_span_horizon_multiples: 2,
      minimum_weekday_coverage_ratio: 0.8,
      maximum_prediction_age_days: 4,
      maximum_actual_observation_age_days: 4,
    },
    ...overrides,
  };
}

function buildStrongRows(startDate = '2026-01-01', totalDays = 25): EdgeEvidenceRow[] {
  const nowDate = addDays(startDate, totalDays - 1);
  return Array.from({ length: totalDays }, (_, index) => {
    const predictionDate = addDays(startDate, index);
    const targetDate = addDays(predictionDate, 2);
    const matured = targetDate <= nowDate;
    const direction = index % 2 === 0 ? 1 : -1;
    return makeRow({
      predictionDate,
      predictedChange: direction,
      actualChange: matured ? direction : null,
      observationDate: matured ? targetDate : null,
      realizedReturnPct: matured ? direction : null,
    });
  });
}

test('calendar-time HAC is deterministic, paired, and widens for clustered overlap', () => {
  const samples = [
    { date: '2026-01-01', value: 1 },
    { date: '2026-01-02', value: 1 },
    { date: '2026-01-03', value: 1 },
    { date: '2026-01-04', value: -1 },
    { date: '2026-01-05', value: -1 },
    { date: '2026-01-06', value: -1 },
  ];
  const estimate = computeCalendarTimeHac95(samples, 3, [-1, 1]);
  const reordered = computeCalendarTimeHac95([...samples].reverse(), 3, [-1, 1]);
  const iidStandardError = Math.sqrt(6 / 5 / 6);

  assert.deepEqual(reordered, estimate);
  assert.equal(estimate.mean, 0);
  assert.ok(estimate.standard_error !== null);
  assert.ok(estimate.standard_error > iidStandardError);
  assert.ok((estimate.ci95_low as number) < 0);
  assert.ok((estimate.ci95_high as number) > 0);
});

test('calendar-time HAC fails closed for duplicate dates or invalid values', () => {
  const estimate = computeCalendarTimeHac95([
    { date: '2026-01-01', value: 1 },
    { date: '2026-01-01', value: Number.NaN },
  ], 7, [-1, 1]);

  assert.equal(estimate.mean, null);
  assert.equal(estimate.lower_bound_positive, false);
  assert.ok(estimate.unavailable_reasons.includes('duplicate_sample_date_2026-01-01'));
  assert.ok(estimate.unavailable_reasons.includes('invalid_sample_value_1'));
});

test('causal baseline requires both target and observation strictly before prediction', () => {
  const rows = [
    makeRow({
      predictionDate: '2026-01-01',
      predictedChange: -1,
      actualChange: -1,
      observationDate: '2026-01-03',
      realizedReturnPct: -1,
    }),
    makeRow({
      predictionDate: '2026-01-03',
      predictedChange: 1,
      actualChange: 1,
      observationDate: '2026-01-05',
      realizedReturnPct: 1,
    }),
    makeRow({
      predictionDate: '2026-01-05',
      predictedChange: 1,
      actualChange: 1,
      observationDate: '2026-01-07',
      realizedReturnPct: 1,
    }),
    makeRow({ predictionDate: '2026-01-07', actualChange: null }),
  ];

  const paired = buildCausalPairedObservations(rows, '2026-01-07T12:00:00.000Z', {
    horizon: { horizon: '2d', horizon_days: 2 },
    current_model_version: 'v1',
    round_trip_cost_pct: 0.1,
  });

  assert.equal(paired.observations.length, 1);
  const observation = paired.observations[0];
  assert.equal(observation.prediction_date, '2026-01-05');
  assert.equal(observation.baseline_target_date, '2026-01-03');
  assert.equal(observation.baseline_actual_observation_date, '2026-01-03');
  assert.equal(observation.baseline_direction, -1);
  assert.equal(observation.direction_difference, 1);
  assert.equal(observation.model_signed_return_after_cost_pct, 0.9);
  assert.equal(observation.baseline_signed_return_after_cost_pct, -1.1);
  assert.equal(observation.signed_return_difference_after_cost_pct, 2);
});

test('baseline and evidence are frozen to the current model version', () => {
  const rows = [
    makeRow({
      predictionDate: '2026-01-01',
      actualChange: -1,
      observationDate: '2026-01-03',
      modelVersion: 'v1',
    }),
    makeRow({
      predictionDate: '2026-01-02',
      actualChange: 1,
      observationDate: '2026-01-04',
      modelVersion: 'v0',
    }),
    makeRow({
      predictionDate: '2026-01-05',
      actualChange: 1,
      observationDate: '2026-01-07',
      realizedReturnPct: 1,
      modelVersion: 'v1',
    }),
    makeRow({ predictionDate: '2026-01-07', modelVersion: 'v1' }),
  ];

  const paired = buildCausalPairedObservations(rows, '2026-01-07T12:00:00.000Z', {
    horizon: { horizon: '2d', horizon_days: 2 },
    current_model_version: 'v1',
    round_trip_cost_pct: 0.1,
  });

  assert.equal(paired.observations.length, 1);
  assert.equal(paired.observations[0].baseline_prediction_date, '2026-01-01');
  assert.equal(paired.observations[0].baseline_direction, -1);
});

test('complete strong current-model evidence passes every gate', () => {
  const rows = buildStrongRows();
  const report = buildEdgeEvidenceReport(
    rows,
    '2026-01-25T12:00:00.000Z',
    permissiveOptions(),
  );
  const window = report.windows[0];

  assert.equal(window.integrity_gate.pass, true, window.integrity_gate.reasons.join(','));
  assert.equal(window.eligibility_gate.pass, true, window.eligibility_gate.reasons.join(','));
  assert.equal(window.performance_gate.pass, true, window.performance_gate.reasons.join(','));
  assert.equal(window.evidence_gate.pass, true, window.evidence_gate.reasons.join(','));
  assert.equal(report.integrity_gate.pass, true);
  assert.equal(report.performance_gate.pass, true);
  assert.equal(report.evidence_gate.pass, true);
  assert.equal(report.promotion_gate.pass, true);
  assert.equal(window.direction.uplift, 1);
  assert.equal(window.direction.ci95_low, 1);
  assert.equal(window.direction.bandwidth_days, edgeEvidenceHacBandwidthDays(2));
  assert.equal(window.direction.bandwidth_days, 10);
  assert.equal(window.signed_return_after_cost_pct.bandwidth_days, 10);
  assert.equal(window.signed_return_after_cost_pct.model_mean, 0.9);
  assert.equal(window.signed_return_after_cost_pct.uplift, 2);
  assert.equal(window.latest_actual_observation_date, '2026-01-25');
  assert.equal(window.latest_actual_observation_age_days, 0);
  assert.equal(window.weekday_coverage_ratio, 1);
});

test('freshness ages use the New York decision date after UTC midnight', () => {
  const report = buildEdgeEvidenceReport(
    buildStrongRows(),
    '2026-01-26T01:30:00.000Z',
    permissiveOptions(),
  );

  assert.equal(report.windows[0].latest_prediction_date, '2026-01-25');
  assert.equal(report.windows[0].latest_prediction_age_days, 0);
  assert.equal(report.windows[0].latest_actual_observation_age_days, 0);
});

test('missing return, stale streams, and weak sample fail closed with explicit reasons', () => {
  const rows = buildStrongRows().map((row, index) => (
    index === 10 ? { ...row, realized_return_pct: null } : row
  ));
  const report = buildEdgeEvidenceReport(
    rows,
    '2026-02-10T12:00:00.000Z',
    permissiveOptions({
      thresholds: {
        ...permissiveOptions().thresholds,
        minimum_paired_observations: 50,
        minimum_span_horizon_multiples: 20,
      },
    }),
  );
  const window = report.windows[0];

  assert.equal(window.evidence_gate.pass, false);
  assert.ok(window.eligibility_gate.reasons.includes('insufficient_paired_sample'));
  assert.ok(window.eligibility_gate.reasons.includes('insufficient_calendar_span'));
  assert.ok(window.eligibility_gate.reasons.includes('signed_return_outcomes_incomplete'));
  assert.ok(window.eligibility_gate.reasons.includes('stale_prediction_stream'));
  assert.ok(window.eligibility_gate.reasons.includes('stale_actual_observation'));
  assert.equal(report.promotion_gate.pass, false);
});

test('weekday coverage, discordance, current version, and upstream integrity are enforced', () => {
  const sparseRows = Array.from({ length: 8 }, (_, index) => {
    const predictionDate = addDays('2026-01-05', index * 7);
    return makeRow({
      predictionDate,
      actualChange: index % 2 === 0 ? 1 : -1,
      observationDate: addDays(predictionDate, 2),
      realizedReturnPct: index % 2 === 0 ? 1 : -1,
      modelVersion: 'v1',
    });
  });
  sparseRows.push(makeRow({ predictionDate: '2026-03-01', modelVersion: 'v2' }));
  const report = buildEdgeEvidenceReport(
    sparseRows,
    '2026-03-01T12:00:00.000Z',
    permissiveOptions({
      upstream_integrity: { pass: false, reasons: ['leakage_sentinel_failed'] },
      thresholds: {
        ...permissiveOptions().thresholds,
        minimum_paired_observations: 2,
        minimum_discordant_pairs: 10,
        minimum_span_horizon_multiples: 1,
      },
    }),
  );
  const window = report.windows[0];

  assert.equal(window.integrity_gate.pass, false);
  assert.ok(window.integrity_gate.reasons.includes('upstream:leakage_sentinel_failed'));
  assert.ok(window.integrity_gate.reasons.includes('current_model_is_not_latest_prediction_version'));
  assert.ok(window.eligibility_gate.reasons.includes('insufficient_weekday_coverage'));
  assert.ok(window.eligibility_gate.reasons.includes('insufficient_discordant_pairs'));
  assert.equal(window.evidence_gate.pass, false);
});

test('non-positive model signed return blocks performance even with positive uplift', () => {
  const rows = buildStrongRows().map((row) => (
    row.actual_change === null
      ? row
      : { ...row, realized_return_pct: (row.realized_return_pct as number) * 0.05 }
  ));
  const report = buildEdgeEvidenceReport(
    rows,
    '2026-01-25T12:00:00.000Z',
    permissiveOptions({ round_trip_cost_pct: 0.1 }),
  );
  const window = report.windows[0];

  assert.ok((window.signed_return_after_cost_pct.uplift as number) > 0);
  assert.ok((window.signed_return_after_cost_pct.model_mean as number) < 0);
  assert.equal(window.performance_gate.pass, false);
  assert.ok(window.performance_gate.reasons.includes('model_signed_return_after_cost_not_positive'));
  assert.equal(window.evidence_gate.pass, false);
});

test('malformed observations and duplicate current-version dates fail integrity', () => {
  const rows = buildStrongRows();
  rows.push({ ...rows[5] });
  rows[8] = {
    ...rows[8],
    actual_observation_date: addDays(rows[8].target_date as string, -1),
  };
  const report = buildEdgeEvidenceReport(
    rows,
    '2026-01-25T12:00:00.000Z',
    permissiveOptions(),
  );
  const reasons = report.windows[0].integrity_gate.reasons;

  assert.equal(report.integrity_gate.pass, false);
  assert.ok(reasons.some((reason) => reason.startsWith('duplicate_model_prediction_date_v1_')));
  assert.ok(reasons.some((reason) => reason.endsWith(':actual_observed_before_target')));
  assert.equal(report.promotion_gate.pass, false);
});

test('invalid empty configuration fails every top-level gate closed', () => {
  const report = buildEdgeEvidenceReport([], 'not-a-date', {
    horizons: [],
    current_model_version: null,
    round_trip_cost_pct: Number.NaN,
    upstream_integrity: { pass: false, reasons: [] },
  });

  assert.equal(report.integrity_gate.pass, false);
  assert.equal(report.performance_gate.pass, false);
  assert.equal(report.evidence_gate.pass, false);
  assert.equal(report.promotion_gate.pass, false);
  assert.ok(report.performance_gate.reasons.includes('horizons_unavailable'));
});
