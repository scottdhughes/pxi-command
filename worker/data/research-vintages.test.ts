import assert from 'node:assert/strict';
import test from 'node:test';

import {
  RESEARCH_FEATURE_VERSION,
  RESEARCH_STORAGE_CONTRACT,
  captureResearchFeatureSnapshot,
} from './research-vintages.js';

test('captureResearchFeatureSnapshot writes an immutable provenance-rich payload', async () => {
  let insertedArgs: unknown[] | null = null;
  const db = {
    prepare(sql: string) {
      const statement = (args: unknown[] = []) => ({
        bind: (...nextArgs: unknown[]) => statement(nextArgs),
        run: async () => {
          if (sql.includes('INSERT INTO research_feature_snapshots')) insertedArgs = args;
          return { success: true };
        },
        all: async () => {
          if (sql.includes('FROM indicator_values')) {
            return {
              results: [
                { indicator_id: 'ism_manufacturing', observation_date: '2026-08-03', value: 48.2, source: 'fred' },
                { indicator_id: 'spy_close', observation_date: '2026-08-21', value: 651.4, source: 'yahoo' },
              ],
            };
          }
          if (sql.includes('FROM category_scores')) {
            return { results: [{ category: 'Macro Growth', score: 54.5 }] };
          }
          return { results: [] };
        },
      });
      return statement();
    },
  };

  const snapshot = await captureResearchFeatureSnapshot(db as any, {
    date: '2026-08-21',
    score: 61.2,
    delta_1d: 0.5,
    delta_7d: null,
    delta_30d: 2.4,
  }, 'test', '2026-08-21T20:00:00.000Z');

  assert.ok(snapshot);
  assert.equal(snapshot.feature_version, RESEARCH_FEATURE_VERSION);
  assert.equal(snapshot.storage_contract, RESEARCH_STORAGE_CONTRACT);
  assert.equal(snapshot.benchmark_close, 651.4);
  assert.equal(snapshot.features.category_macro_growth, 54.5);
  assert.equal(snapshot.features.indicator_ism_manufacturing, 48.2);
  assert.equal(snapshot.feature_observation_dates.indicator_ism_manufacturing, '2026-08-03');
  assert.equal(snapshot.feature_sources.indicator_ism_manufacturing, 'fred');
  assert.equal('pxi_delta_7d' in snapshot.features, false);
  assert.ok(insertedArgs);
  assert.equal(insertedArgs![1], '2026-08-21');
  assert.match(String(insertedArgs![8]), /append-only-d1-research-snapshots/);
});

test('captureResearchFeatureSnapshot refuses rows without a valid benchmark', async () => {
  let inserted = false;
  const db = {
    prepare(sql: string) {
      const statement = () => ({
        bind: () => statement(),
        run: async () => {
          if (sql.includes('INSERT INTO research_feature_snapshots')) inserted = true;
          return { success: true };
        },
        all: async () => ({
          results: sql.includes('FROM indicator_values')
            ? [{ indicator_id: 'vix', observation_date: '2026-08-21', value: 18, source: 'fred' }]
            : [],
        }),
      });
      return statement();
    },
  };

  const snapshot = await captureResearchFeatureSnapshot(db as any, {
    date: '2026-08-21',
    score: 50,
    delta_1d: null,
    delta_7d: null,
    delta_30d: null,
  }, 'test');

  assert.equal(snapshot, null);
  assert.equal(inserted, false);
});
