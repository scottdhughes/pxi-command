import assert from 'node:assert/strict';
import test from 'node:test';

import { currentNewYorkDate, HISTORY_RECONSTRUCTION_CONTRACT } from './lib/history-provenance.js';

test('currentNewYorkDate observes the New York day across UTC boundaries', () => {
  assert.equal(currentNewYorkDate(new Date('2026-01-15T04:59:59.000Z')), '2026-01-14');
  assert.equal(currentNewYorkDate(new Date('2026-01-15T05:00:00.000Z')), '2026-01-15');
  assert.equal(currentNewYorkDate(new Date('2026-07-15T03:59:59.000Z')), '2026-07-14');
  assert.equal(currentNewYorkDate(new Date('2026-07-15T04:00:00.000Z')), '2026-07-15');
});

test('history reconstruction capability identifier is explicit and versioned', () => {
  assert.equal(HISTORY_RECONSTRUCTION_CONTRACT, 'isolated-missing-only-v1');
});
