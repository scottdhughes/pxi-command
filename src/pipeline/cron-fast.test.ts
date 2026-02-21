import test from 'node:test';
import assert from 'node:assert/strict';

import {
  evaluateEdgePromotionGate,
  isRetryableYahooError,
  parseYahooChartResponse,
  retryWithBackoff,
} from './cron-fast.js';

test('parseYahooChartResponse parses adjusted close data', () => {
  const payload = {
    chart: {
      result: [
        {
          timestamp: [1735689600, 1735776000],
          indicators: {
            adjclose: [{ adjclose: [100.5, 101.25] }],
            quote: [{ close: [100, 101] }],
          },
        },
      ],
      error: null,
    },
  };

  const rows = parseYahooChartResponse(payload, 'copper_gold_ratio', 'yahoo_direct');
  assert.equal(rows.length, 2);
  assert.equal(rows[0].indicator_id, 'copper_gold_ratio');
  assert.equal(rows[0].source, 'yahoo_direct');
  assert.equal(rows[0].value, 100.5);
});

test('parseYahooChartResponse falls back to quote close values', () => {
  const payload = {
    chart: {
      result: [
        {
          timestamp: [1735689600, 1735776000],
          indicators: {
            quote: [{ close: [99.2, null] }],
          },
        },
      ],
      error: null,
    },
  };

  const rows = parseYahooChartResponse(payload, 'vix');
  assert.equal(rows.length, 1);
  assert.equal(rows[0].value, 99.2);
  assert.equal(rows[0].source, 'yahoo_direct');
});

test('retryWithBackoff retries 429 errors then succeeds', async () => {
  let attempts = 0;

  const result = await retryWithBackoff(
    async () => {
      attempts += 1;
      if (attempts < 3) {
        const err = new Error('Too Many Requests');
        throw err;
      }
      return 'ok';
    },
    {
      maxAttempts: 4,
      baseDelayMs: 1,
      maxDelayMs: 2,
      shouldRetry: isRetryableYahooError,
    }
  );

  assert.equal(result, 'ok');
  assert.equal(attempts, 3);
});

test('isRetryableYahooError identifies non-retryable 4xx errors', () => {
  assert.equal(isRetryableYahooError(new Error('Request failed with status code 400')), false);
  assert.equal(isRetryableYahooError(new Error('Too Many Requests')), true);
});

test('evaluateEdgePromotionGate passes when no leakage sentinel violations exist', () => {
  const evaluation = evaluateEdgePromotionGate({
    as_of: '2026-02-21T00:00:00.000Z',
    basis: 'prediction_log_forward_chain_vs_lagged_actual_baseline',
    windows: [
      {
        horizon: '7d',
        sample_size: 120,
        uplift_vs_baseline: 0.04,
        uplift_ci95_low: 0.01,
        uplift_ci95_high: 0.07,
        leakage_sentinel: {
          pass: true,
          violation_count: 0,
          reasons: [],
        },
      },
      {
        horizon: '30d',
        sample_size: 95,
        uplift_vs_baseline: 0.02,
        uplift_ci95_low: -0.01,
        uplift_ci95_high: 0.05,
        leakage_sentinel: {
          pass: true,
          violation_count: 0,
          reasons: [],
        },
      },
    ],
    promotion_gate: {
      pass: true,
      reasons: [],
    },
  });

  assert.equal(evaluation.pass, true);
  assert.deepEqual(evaluation.reasons, []);
});

test('evaluateEdgePromotionGate fails on leakage sentinel violations', () => {
  const evaluation = evaluateEdgePromotionGate({
    as_of: '2026-02-21T00:00:00.000Z',
    basis: 'prediction_log_forward_chain_vs_lagged_actual_baseline',
    windows: [
      {
        horizon: '7d',
        sample_size: 70,
        uplift_vs_baseline: 0.01,
        uplift_ci95_low: -0.02,
        uplift_ci95_high: 0.04,
        leakage_sentinel: {
          pass: false,
          violation_count: 2,
          reasons: ['evaluated_before_target_2'],
        },
      },
    ],
    promotion_gate: {
      pass: false,
      reasons: ['7d:evaluated_before_target_2'],
    },
  });

  assert.equal(evaluation.pass, false);
  assert.equal(evaluation.reasons.includes('7d:evaluated_before_target_2'), true);
  assert.equal(evaluation.reasons.includes('gate:7d:evaluated_before_target_2'), true);
});
