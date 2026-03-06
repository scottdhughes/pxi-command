import assert from 'node:assert/strict';
import test from 'node:test';

import {
  backfillFixture,
  backfillProductsFixture,
  refreshProductsFixture,
  utilityFunnelFixture,
  workerOperationalFixtures,
} from './worker-operational-fixtures';

test('worker operational fixtures cover the admin and publication payload surface', () => {
  assert.deepEqual(Object.keys(workerOperationalFixtures).sort(), [
    'backfill',
    'backfillProducts',
    'ignoredUtilityEvent',
    'migration',
    'recalculate',
    'recalculateAllSignals',
    'refreshIngestion',
    'refreshProducts',
    'refreshProductsSkipped',
    'sendDigest',
    'skippedDigest',
    'utilityEvent',
    'utilityFunnel',
    'write',
  ]);
});

test('worker operational fixtures preserve core runtime invariants', () => {
  assert.equal(backfillFixture.results.some((result) => result.error), true);
  assert.equal(refreshProductsFixture.opportunity_item_ledger_rows >= refreshProductsFixture.opportunity_ledger_rows, true);
  assert.equal(refreshProductsFixture.decision_impact?.market_7d_sample_size !== null, true);
  assert.equal(backfillProductsFixture.calibration_samples.edge_total_samples !== null, true);
  assert.equal(utilityFunnelFixture.funnel.total_events >= utilityFunnelFixture.funnel.decision_events_total, true);
});
