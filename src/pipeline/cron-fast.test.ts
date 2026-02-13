import test from 'node:test';
import assert from 'node:assert/strict';

import {
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
