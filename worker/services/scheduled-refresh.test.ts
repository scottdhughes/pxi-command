import assert from 'node:assert/strict';
import test from 'node:test';

import {
  evaluateCandidateSla,
  selectLatestIndicatorCandidates,
} from './candidate-sla.js';

const freshDate = '2026-08-21';

function criticalCandidates() {
  return [
    'aaii_sentiment',
    'copper_gold_ratio',
    'vix',
    'spy_close',
    'dxy',
    'hyg',
    'lqd',
    'fear_greed',
  ].map((indicator_id) => ({
    indicator_id,
    date: freshDate,
    value: 1,
    source: 'test',
  }));
}

test('candidate SLA gate passes all current critical series', () => {
  const summary = evaluateCandidateSla(
    criticalCandidates(),
    new Date('2026-08-22T12:00:00Z'),
  );
  assert.deepEqual(summary.critical_failures, []);
  assert.ok(summary.checked >= 8);
});

test('candidate SLA gate fails closed when a critical series is missing', () => {
  const candidates = criticalCandidates().filter((row) => row.indicator_id !== 'spy_close');
  const summary = evaluateCandidateSla(candidates, new Date('2026-08-22T12:00:00Z'));
  assert.deepEqual(
    summary.critical_failures.find((failure) => failure.indicator_id === 'spy_close'),
    { indicator_id: 'spy_close', latest_date: null, status: 'missing' },
  );
});

test('market-day SLA aging does not count the weekend against Friday closes', () => {
  const summary = evaluateCandidateSla(
    criticalCandidates(),
    new Date('2026-08-24T12:00:00Z'),
  );
  const marketFailures = summary.critical_failures.filter((failure) => (
    ['vix', 'spy_close', 'hyg', 'lqd'].includes(failure.indicator_id)
  ));
  assert.deepEqual(marketFailures, []);
});

test('native refresh writes only the newest valid row per indicator', () => {
  assert.deepEqual(selectLatestIndicatorCandidates([
    { indicator_id: 'vix', date: '2026-08-20', value: 18, source: 'older' },
    { indicator_id: 'spy_close', date: '2026-08-21', value: 650, source: 'yahoo' },
    { indicator_id: 'vix', date: '2026-08-21', value: 17, source: 'newer' },
    { indicator_id: 'vix', date: 'invalid', value: 19, source: 'bad' },
    { indicator_id: 'vix', date: '2026-02-30', value: 19, source: 'impossible' },
    { indicator_id: 'vix', date: '9999-12-31', value: 99, source: 'future' },
    { indicator_id: 'broken', date: '2026-08-21', value: Number.NaN, source: 'bad' },
  ], '2026-08-22'), [
    { indicator_id: 'spy_close', date: '2026-08-21', value: 650, source: 'yahoo' },
    { indicator_id: 'vix', date: '2026-08-21', value: 17, source: 'newer' },
  ]);
});

test('candidate SLA ignores impossible and future observations', () => {
  const candidates = criticalCandidates().filter((row) => row.indicator_id !== 'spy_close');
  candidates.push(
    { indicator_id: 'spy_close', date: '2026-02-30', value: 650, source: 'impossible' },
    { indicator_id: 'spy_close', date: '9999-12-31', value: 651, source: 'future' },
  );
  const summary = evaluateCandidateSla(candidates, new Date('2026-08-22T12:00:00Z'));
  assert.deepEqual(
    summary.critical_failures.find((failure) => failure.indicator_id === 'spy_close'),
    { indicator_id: 'spy_close', latest_date: null, status: 'missing' },
  );
});
