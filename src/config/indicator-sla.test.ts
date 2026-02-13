import test from 'node:test';
import assert from 'node:assert/strict';

import {
  CRITICAL_INDICATORS,
  evaluateSla,
  resolveIndicatorSla,
} from './indicator-sla.js';

test('resolveIndicatorSla uses frequency defaults', () => {
  assert.equal(resolveIndicatorSla('foo_daily', 'daily').max_age_days, 4);
  assert.equal(resolveIndicatorSla('foo_realtime', 'realtime').max_age_days, 4);
  assert.equal(resolveIndicatorSla('foo_weekly', 'weekly').max_age_days, 10);
  assert.equal(resolveIndicatorSla('foo_monthly', 'monthly').max_age_days, 45);
});

test('resolveIndicatorSla infers frequency from indicator config when omitted', () => {
  assert.equal(resolveIndicatorSla('jobless_claims').max_age_days, 10);
  assert.equal(resolveIndicatorSla('cfnai').max_age_days, 120);
  assert.equal(resolveIndicatorSla('net_liquidity').class, 'weekly');
});

test('resolveIndicatorSla honors source-lagged overrides', () => {
  const wti = resolveIndicatorSla('wti_crude', 'daily');
  const dxy = resolveIndicatorSla('dollar_index', 'daily');

  assert.equal(wti.class, 'source_lagged');
  assert.equal(wti.max_age_days, 7);
  assert.equal(dxy.class, 'source_lagged');
  assert.equal(dxy.max_age_days, 10);
});

test('resolveIndicatorSla honors explicit threshold overrides', () => {
  assert.equal(resolveIndicatorSla('cfnai', 'monthly').max_age_days, 120);
  assert.equal(resolveIndicatorSla('m2_yoy', 'monthly').max_age_days, 120);
  assert.equal(resolveIndicatorSla('fed_balance_sheet', 'weekly').max_age_days, 14);
});

test('critical indicator membership is wired to policy', () => {
  const policy = resolveIndicatorSla('vix', 'daily');
  assert.equal(CRITICAL_INDICATORS.has('vix'), true);
  assert.equal(policy.critical, true);
  assert.equal(resolveIndicatorSla('net_liquidity', 'weekly').critical, false);
});

test('evaluateSla detects stale and missing states', () => {
  const now = new Date('2026-02-13T00:00:00Z');

  const freshPolicy = resolveIndicatorSla('vix', 'daily');
  const freshEval = evaluateSla('2026-02-12', now, freshPolicy);
  assert.equal(freshEval.missing, false);
  assert.equal(freshEval.stale, false);

  const stalePolicy = resolveIndicatorSla('aaii_sentiment', 'weekly');
  const staleEval = evaluateSla('2026-01-01', now, stalePolicy);
  assert.equal(staleEval.missing, false);
  assert.equal(staleEval.stale, true);

  const missingEval = evaluateSla(null, now, stalePolicy);
  assert.equal(missingEval.missing, true);
  assert.equal(missingEval.stale, true);
});
