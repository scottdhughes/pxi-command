import assert from 'node:assert/strict';
import test from 'node:test';

import {
  MARKET_EVIDENCE_MODEL_FAMILY,
  MARKET_EVIDENCE_MODEL_VERSION,
  MARKET_EVIDENCE_TARGET_METRIC,
  type MarketPredictionEvidence,
} from '../data/market-evidence.js';
import {
  buildMarketEdgeDiagnosticsReport,
  unfoldStoredMarketEvidence,
} from './edge-diagnostics.js';

function storedEvidence(overrides: Partial<MarketPredictionEvidence> = {}): MarketPredictionEvidence {
  return {
    evidence_id: 'market-evidence:2026-08-21:v1',
    prediction_date: '2026-08-21',
    prediction_available_at: '2026-08-21T22:05:00.000Z',
    canonical_slot: 'daily_close_22z',
    feature_snapshot_id: 'snapshot-1',
    model_family: MARKET_EVIDENCE_MODEL_FAMILY,
    model_version: MARKET_EVIDENCE_MODEL_VERSION,
    target_metric: MARKET_EVIDENCE_TARGET_METRIC,
    current_pxi_score: 61,
    pxi_bucket: '60-80',
    bucket_lower: 60,
    bucket_upper: 80,
    benchmark_close: 650,
    benchmark_observation_date: '2026-08-21',
    target_date_7d: '2026-08-28',
    target_date_30d: '2026-09-20',
    predicted_return_7d: 0.8,
    predicted_return_30d: 2.1,
    median_return_7d: 0.7,
    median_return_30d: 1.9,
    win_rate_7d: 0.55,
    win_rate_30d: 0.6,
    ci95_low_7d: 0.1,
    ci95_high_7d: 1.5,
    ci95_low_30d: 0.4,
    ci95_high_30d: 3.8,
    sample_size_7d: 200,
    sample_size_30d: 180,
    training_cutoff_date: '2026-08-21',
    methodology_json: JSON.stringify({
      model_version: MARKET_EVIDENCE_MODEL_VERSION,
      target_metric: MARKET_EVIDENCE_TARGET_METRIC,
    }),
    outcome_status_7d: 'pending',
    outcome_status_30d: 'pending',
    outcome_unavailable_reason_7d: null,
    outcome_unavailable_reason_30d: null,
    actual_return_7d: null,
    actual_return_30d: null,
    actual_observation_date_7d: null,
    actual_observation_date_30d: null,
    evaluated_at_7d: null,
    evaluated_at_30d: null,
    evaluated_at: null,
    ...overrides,
  };
}

test('stored evidence adapter preserves horizon outcomes and fails malformed provenance', () => {
  const rows = unfoldStoredMarketEvidence([storedEvidence({
    outcome_status_7d: 'observed',
    actual_return_7d: 1.2,
    actual_observation_date_7d: '2026-08-28',
    evaluated_at_7d: '2026-08-28T22:00:00.000Z',
  })], ['7d', '30d']);

  assert.equal(rows.length, 2);
  assert.equal(rows[0].actual_change, 1.2);
  assert.equal(rows[0].actual_observation_date, '2026-08-28');
  assert.equal(rows[0].integrity_pass, true);
  assert.equal(rows[1].actual_change, null);

  const malformed = unfoldStoredMarketEvidence([
    storedEvidence({ benchmark_observation_date: '2026-08-20' }),
  ], ['7d']);
  assert.equal(malformed[0].integrity_pass, false);
  assert.ok(malformed[0].integrity_reasons?.includes('benchmark_not_current_at_prediction'));
});

test('explicit zero-sample warm-up forecasts are excluded without poisoning integrity', () => {
  const warmup = storedEvidence({
    predicted_return_7d: null,
    median_return_7d: null,
    win_rate_7d: null,
    ci95_low_7d: null,
    ci95_high_7d: null,
    sample_size_7d: 0,
    training_cutoff_date: null,
    methodology_json: JSON.stringify({
      model_version: MARKET_EVIDENCE_MODEL_VERSION,
      target_metric: MARKET_EVIDENCE_TARGET_METRIC,
      training_source: 'prior_rows_in_immutable_market_prediction_evidence',
    }),
  });
  assert.deepEqual(unfoldStoredMarketEvidence([warmup], ['7d']), []);

  const malformed = unfoldStoredMarketEvidence([{
    ...warmup,
    methodology_json: '{}',
  }], ['7d']);
  assert.equal(malformed.length, 1);
  assert.equal(malformed[0].integrity_pass, false);
  assert.ok(malformed[0].integrity_reasons?.includes('prediction_missing'));
});

test('a 30d warm-up remains valid when 7d training sets the shared cutoff', () => {
  const mixedHorizon = storedEvidence({
    predicted_return_7d: 0.6,
    sample_size_7d: 1,
    predicted_return_30d: null,
    median_return_30d: null,
    win_rate_30d: null,
    ci95_low_30d: null,
    ci95_high_30d: null,
    sample_size_30d: 0,
    training_cutoff_date: '2026-08-20',
    methodology_json: JSON.stringify({
      model_version: MARKET_EVIDENCE_MODEL_VERSION,
      target_metric: MARKET_EVIDENCE_TARGET_METRIC,
      training_source: 'prior_rows_in_immutable_market_prediction_evidence',
    }),
  });

  assert.deepEqual(unfoldStoredMarketEvidence([mixedHorizon], ['30d']), []);
  const sevenDay = unfoldStoredMarketEvidence([mixedHorizon], ['7d']);
  assert.equal(sevenDay.length, 1);
  assert.equal(sevenDay[0].integrity_pass, true);
});

test('D1 diagnostics adapter returns an explicit NO-GO for an empty forward evidence stream', async () => {
  const db = {
    prepare() {
      const statement = {
        bind: () => statement,
        all: async () => ({ results: [] }),
        first: async () => ({ total: 0, evaluated: 0, pending: 0, with_7d_outcome: 0, with_30d_outcome: 0 }),
      };
      return statement;
    },
  } as unknown as D1Database;

  const report = await buildMarketEdgeDiagnosticsReport(
    db,
    ['7d', '30d'],
    new Date('2026-08-21T22:30:00.000Z'),
  );

  assert.equal(report.promotion_gate.pass, false);
  assert.equal(report.policy_alignment_gate.pass, false);
  assert.equal(report.inference_control.maximum_unique_looks, 5000);
  assert.equal(report.inference_control.coverage_basis, 'asymptotic_hac_normal_approximation');
  assert.equal(report.inference_control.finite_sample_guarantee, false);
  assert.equal(report.integrity_gate.pass, false);
  assert.equal(report.windows.length, 2);
  assert.equal(report.windows[0].baseline_strategy, 'last_observable_actual_direction');
  assert.ok(report.windows[0].evidence_gate.reasons.some((reason) =>
    reason.includes('current_model_rows_unavailable')));
});
