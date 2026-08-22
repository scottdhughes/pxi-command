import assert from 'node:assert/strict';
import test from 'node:test';

import {
  isHistoricalBackfillOpportunitySnapshot,
  parseProspectiveOpportunitySnapshot,
} from './lib/opportunity-snapshot-history.js';

const seedByFactor = {
  items: [{
    direction: 'bullish',
    conviction_score: 80,
    supporting_factors: ['historical_backfill_seed'],
    expectancy: { unavailable_reason: null },
  }],
};

const seedByExpectancy = {
  items: [{
    direction: 'bearish',
    conviction_score: 70,
    supporting_factors: [],
    expectancy: { unavailable_reason: 'historical_backfill_seed' },
  }],
};

test('historical opportunity seed detection checks both explicit marker locations', () => {
  assert.equal(isHistoricalBackfillOpportunitySnapshot(seedByFactor), true);
  assert.equal(isHistoricalBackfillOpportunitySnapshot(seedByExpectancy), true);
  assert.equal(isHistoricalBackfillOpportunitySnapshot({
    items: [{
      rationale: 'historical_backfill_seed',
      supporting_factors: ['prospective_observation'],
      expectancy: { unavailable_reason: null },
    }],
  }), false);
});

test('prospective opportunity parsing excludes historical seeds and malformed payloads', () => {
  const prospective = {
    as_of: '2026-01-01T00:00:00.000Z',
    horizon: '7d',
    items: [{
      direction: 'bullish',
      conviction_score: 75,
      supporting_factors: ['prospective_observation'],
      expectancy: { unavailable_reason: null },
    }],
  };

  assert.equal(parseProspectiveOpportunitySnapshot(JSON.stringify(seedByFactor)), null);
  assert.equal(parseProspectiveOpportunitySnapshot(JSON.stringify(seedByExpectancy)), null);
  assert.deepEqual(parseProspectiveOpportunitySnapshot(JSON.stringify(prospective)), prospective);
  assert.equal(parseProspectiveOpportunitySnapshot('{not-json'), null);
});
