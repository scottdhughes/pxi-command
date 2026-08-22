import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DAILY_CLOSE_CANONICAL_SLOT,
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
  assert.equal(snapshot.canonical_slot, null);
  assert.equal(snapshot.benchmark_close, 651.4);
  assert.equal(snapshot.features.category_macro_growth, 54.5);
  assert.equal(snapshot.features.indicator_ism_manufacturing, 48.2);
  assert.equal(snapshot.feature_observation_dates.indicator_ism_manufacturing, '2026-08-03');
  assert.equal(snapshot.feature_sources.indicator_ism_manufacturing, 'fred');
  assert.equal('pxi_delta_7d' in snapshot.features, false);
  assert.ok(insertedArgs);
  assert.equal(insertedArgs![1], '2026-08-21');
  assert.equal(insertedArgs![6], null);
  assert.match(String(insertedArgs![9]), /append-only-d1-research-snapshots/);
});

test('captureResearchFeatureSnapshot reserves one daily-close canonical slot', async () => {
  let insertSql = '';
  let insertedArgs: unknown[] = [];
  const db = {
    prepare(sql: string) {
      const statement = (args: unknown[] = []) => ({
        bind: (...nextArgs: unknown[]) => statement(nextArgs),
        run: async () => {
          if (sql.includes('INSERT INTO research_feature_snapshots')) {
            insertSql = sql;
            insertedArgs = args;
          }
          return { success: true, meta: { changes: 1 } };
        },
        all: async () => {
          if (sql.includes('FROM indicator_values')) {
            return {
              results: [
                { indicator_id: 'spy_close', observation_date: '2026-08-21', value: 651.4, source: 'yahoo' },
              ],
            };
          }
          if (sql.includes('FROM category_scores')) return { results: [] };
          return { results: [] };
        },
      });
      return statement();
    },
  };

  const snapshot = await captureResearchFeatureSnapshot(db as any, {
    date: '2026-08-21',
    score: 61.2,
    delta_1d: null,
    delta_7d: null,
    delta_30d: null,
  }, 'scheduled_recalculate', '2026-08-22T01:05:00.000Z', {
    canonicalSlot: DAILY_CLOSE_CANONICAL_SLOT,
  });

  assert.ok(snapshot);
  assert.equal(snapshot.canonical_slot, DAILY_CLOSE_CANONICAL_SLOT);
  assert.match(insertSql, /ON CONFLICT\(decision_date, feature_version, storage_contract, canonical_slot\)/);
  assert.equal(insertedArgs[6], DAILY_CLOSE_CANONICAL_SLOT);
  assert.match(String(insertedArgs[9]), /daily_close_22z/);
});

test('captureResearchFeatureSnapshot returns the persisted canonical row after a conflict', async () => {
  const persisted = {
    snapshot_id: 'persisted-snapshot',
    decision_date: '2026-08-21',
    available_at: '2026-08-21T22:01:00.000Z',
    feature_version: RESEARCH_FEATURE_VERSION,
    storage_contract: RESEARCH_STORAGE_CONTRACT,
    capture_source: 'scheduled_recalculate',
    canonical_slot: DAILY_CLOSE_CANONICAL_SLOT,
    benchmark_close: 650,
    benchmark_observation_date: '2026-08-21',
    features: { pxi_score: 60 },
    feature_observation_dates: { pxi_score: '2026-08-21' },
    feature_sources: { pxi_score: 'pxi-calculation' },
  };
  const db = {
    prepare(sql: string) {
      const statement = () => ({
        bind: () => statement(),
        run: async () => ({ success: true, meta: { changes: sql.includes('INSERT INTO research_feature_snapshots') ? 0 : 1 } }),
        all: async () => ({
          results: sql.includes('FROM indicator_values')
            ? [{ indicator_id: 'spy_close', observation_date: '2026-08-21', value: 651.4, source: 'yahoo' }]
            : [],
        }),
        first: async () => sql.includes('SELECT payload_json')
          ? { payload_json: JSON.stringify(persisted) }
          : null,
      });
      return statement();
    },
  };

  const snapshot = await captureResearchFeatureSnapshot(db as any, {
    date: '2026-08-21',
    score: 62,
    delta_1d: null,
    delta_7d: null,
    delta_30d: null,
  }, 'scheduled_recalculate', '2026-08-21T22:10:00.000Z', {
    canonicalSlot: DAILY_CLOSE_CANONICAL_SLOT,
  });

  assert.equal(snapshot?.snapshot_id, 'persisted-snapshot');
  assert.equal(snapshot?.benchmark_close, 650);
});

test('daily-close canonical capture requires a same-date SPY benchmark', async () => {
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
            ? [{ indicator_id: 'spy_close', observation_date: '2026-08-20', value: 650, source: 'yahoo' }]
            : [],
        }),
      });
      return statement();
    },
  };

  const snapshot = await captureResearchFeatureSnapshot(db as any, {
    date: '2026-08-21',
    score: 60,
    delta_1d: null,
    delta_7d: null,
    delta_30d: null,
  }, 'scheduled_recalculate', '2026-08-21T22:05:00.000Z', {
    canonicalSlot: DAILY_CLOSE_CANONICAL_SLOT,
  });

  assert.equal(snapshot, null);
  assert.equal(inserted, false);

  const rawSnapshot = await captureResearchFeatureSnapshot(db as any, {
    date: '2026-08-21',
    score: 60,
    delta_1d: null,
    delta_7d: null,
    delta_30d: null,
  }, 'worker_recalculate', '2026-08-21T18:00:00.000Z');
  assert.ok(rawSnapshot);
  assert.equal(rawSnapshot.canonical_slot, null);
  assert.equal(rawSnapshot.benchmark_observation_date, '2026-08-20');
  assert.equal(inserted, true);
});

test('daily-close canonical capture rejects after-the-fact New York decision dates', async () => {
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
            ? [{ indicator_id: 'spy_close', observation_date: '2026-08-21', value: 650, source: 'yahoo' }]
            : [],
        }),
      });
      return statement();
    },
  };

  const afterTheFact = await captureResearchFeatureSnapshot(db as any, {
    date: '2026-08-21',
    score: 60,
    delta_1d: null,
    delta_7d: null,
    delta_30d: null,
  }, 'scheduled_recalculate', '2026-08-22T05:00:00.000Z', {
    canonicalSlot: DAILY_CLOSE_CANONICAL_SLOT,
  });

  assert.equal(afterTheFact, null);
  assert.equal(inserted, false);
});

test('daily-close canonical capture rejects intraday reservations', async () => {
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
            ? [{ indicator_id: 'spy_close', observation_date: '2026-08-21', value: 650, source: 'yahoo' }]
            : [],
        }),
      });
      return statement();
    },
  };

  const intraday = await captureResearchFeatureSnapshot(db as any, {
    date: '2026-08-21',
    score: 60,
    delta_1d: null,
    delta_7d: null,
    delta_30d: null,
  }, 'manual_recalculate', '2026-08-21T18:00:00.000Z', {
    canonicalSlot: DAILY_CLOSE_CANONICAL_SLOT,
  });

  assert.equal(intraday, null);
  assert.equal(inserted, false);
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
