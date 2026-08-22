import assert from 'node:assert/strict';
import test from 'node:test';

import {
  captureCanonicalMarketPredictionEvidence,
  computeEmpiricalBucketMarketPredictionEvidence,
  evaluatePendingMarketPredictionEvidence,
  fetchMarketPredictionEvidenceCounts,
  fetchMarketPredictionEvidenceRows,
  insertMarketPredictionEvidence,
  MARKET_EVIDENCE_MODEL_VERSION,
  type MarketPredictionEvidence,
} from './market-evidence.js';
import {
  DAILY_CLOSE_CANONICAL_SLOT,
  RESEARCH_FEATURE_VERSION,
  RESEARCH_STORAGE_CONTRACT,
  type StoredResearchSnapshot,
} from './research-vintages.js';

function canonicalSnapshot(overrides: Partial<StoredResearchSnapshot> = {}): StoredResearchSnapshot {
  return {
    snapshot_id: 'snapshot-2026-03-10',
    decision_date: '2026-03-10',
    available_at: '2026-03-10T22:05:00.000Z',
    feature_version: RESEARCH_FEATURE_VERSION,
    storage_contract: RESEARCH_STORAGE_CONTRACT,
    capture_source: 'scheduled_recalculate',
    canonical_slot: DAILY_CLOSE_CANONICAL_SLOT,
    benchmark_close: 310,
    benchmark_observation_date: '2026-03-10',
    features: { pxi_score: 72 },
    feature_observation_dates: { pxi_score: '2026-03-10' },
    feature_sources: { pxi_score: 'pxi-calculation' },
    ...overrides,
  };
}

function evidenceFixture(overrides: Partial<MarketPredictionEvidence> = {}): MarketPredictionEvidence {
  return {
    evidence_id: `market-evidence:2026-01-01:${DAILY_CLOSE_CANONICAL_SLOT}:${MARKET_EVIDENCE_MODEL_VERSION}`,
    prediction_date: '2026-01-01',
    prediction_available_at: '2026-01-01T22:05:00.000Z',
    canonical_slot: DAILY_CLOSE_CANONICAL_SLOT,
    feature_snapshot_id: 'snapshot-1',
    model_family: 'empirical_bucket_spy_return',
    model_version: MARKET_EVIDENCE_MODEL_VERSION,
    target_metric: 'spy_return_pct',
    current_pxi_score: 72,
    pxi_bucket: '60-80',
    bucket_lower: 60,
    bucket_upper: 80,
    benchmark_close: 100,
    benchmark_observation_date: '2026-01-01',
    target_date_7d: '2026-01-08',
    target_date_30d: '2026-01-31',
    predicted_return_7d: 1,
    predicted_return_30d: 2,
    median_return_7d: 1,
    median_return_30d: 2,
    win_rate_7d: 0.6,
    win_rate_30d: 0.7,
    ci95_low_7d: -1,
    ci95_high_7d: 3,
    ci95_low_30d: 0,
    ci95_high_30d: 4,
    sample_size_7d: 10,
    sample_size_30d: 8,
    training_cutoff_date: '2025-12-31',
    methodology_json: '{}',
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

test('empirical bucket evidence trains only on prior prospective outcomes observable before capture', async () => {
  const queries: Array<{ sql: string; args: unknown[] }> = [];
  const db = {
    prepare(sql: string) {
      const statement = (args: unknown[] = []) => ({
        bind: (...nextArgs: unknown[]) => statement(nextArgs),
        all: async () => {
          queries.push({ sql, args });
          if (sql.includes('FROM market_prediction_evidence')) {
            return {
              results: [
                {
                  prediction_date: '2026-01-01',
                  prediction_available_at: '2026-01-01T22:05:00.000Z',
                  current_pxi_score: 72,
                  outcome_status_7d: 'observed',
                  outcome_status_30d: 'observed',
                  actual_return_7d: 10,
                  actual_return_30d: 20,
                  actual_observation_date_7d: '2026-01-08',
                  actual_observation_date_30d: '2026-01-31',
                  evaluated_at_7d: '2026-01-08T22:05:00.000Z',
                  evaluated_at_30d: '2026-01-31T22:05:00.000Z',
                },
                {
                  prediction_date: '2026-01-05',
                  prediction_available_at: '2026-01-05T22:05:00.000Z',
                  current_pxi_score: 75,
                  outcome_status_7d: 'observed',
                  outcome_status_30d: 'observed',
                  actual_return_7d: -10,
                  actual_return_30d: 10,
                  actual_observation_date_7d: '2026-01-12',
                  actual_observation_date_30d: '2026-02-04',
                  evaluated_at_7d: '2026-01-12T22:05:00.000Z',
                  evaluated_at_30d: '2026-02-04T22:05:00.000Z',
                },
                {
                  prediction_date: '2026-02-01',
                  prediction_available_at: '2026-02-01T22:05:00.000Z',
                  current_pxi_score: 50,
                  outcome_status_7d: 'observed',
                  outcome_status_30d: 'observed',
                  actual_return_7d: 99,
                  actual_return_30d: 99,
                  actual_observation_date_7d: '2026-02-08',
                  actual_observation_date_30d: '2026-03-03',
                  evaluated_at_7d: '2026-02-08T22:05:00.000Z',
                  evaluated_at_30d: '2026-03-03T22:05:00.000Z',
                },
                {
                  prediction_date: '2026-03-01',
                  prediction_available_at: '2026-03-01T22:05:00.000Z',
                  current_pxi_score: 70,
                  outcome_status_7d: 'observed',
                  outcome_status_30d: 'pending',
                  actual_return_7d: 50,
                  actual_return_30d: null,
                  actual_observation_date_7d: '2026-03-08',
                  actual_observation_date_30d: null,
                  evaluated_at_7d: '2026-03-11T22:05:00.000Z',
                  evaluated_at_30d: null,
                },
              ],
            };
          }
          return { results: [] };
        },
      });
      return statement();
    },
  };

  const evidence = await computeEmpiricalBucketMarketPredictionEvidence(
    db as any,
    canonicalSnapshot(),
    { historyLimit: 5000 },
  );

  assert.ok(evidence);
  assert.equal(evidence.pxi_bucket, '60-80');
  assert.equal(evidence.sample_size_7d, 2);
  assert.equal(evidence.sample_size_30d, 2);
  assert.ok(Math.abs((evidence.predicted_return_7d ?? NaN) - 0) < 1e-9);
  assert.ok(Math.abs((evidence.predicted_return_30d ?? NaN) - 15) < 1e-9);
  assert.equal(evidence.win_rate_7d, 0.5);
  assert.equal(evidence.win_rate_30d, 1);
  assert.equal(evidence.training_cutoff_date, '2026-02-04');
  assert.match(evidence.evidence_id, /empirical-bucket-spy-return\/v1$/);

  assert.equal(queries.length, 1);
  const trainingQuery = queries[0];
  assert.match(trainingQuery.sql, /FROM market_prediction_evidence/);
  assert.doesNotMatch(trainingQuery.sql, /FROM pxi_scores/);
  assert.doesNotMatch(trainingQuery.sql, /FROM indicator_values/);
  assert.match(trainingQuery.sql, /prediction_date < \?/);
  assert.match(trainingQuery.sql, /ORDER BY prediction_date DESC\s+LIMIT \?[\s\S]*ORDER BY prediction_date ASC/);
  assert.deepEqual(trainingQuery.args, [
    DAILY_CLOSE_CANONICAL_SLOT,
    MARKET_EVIDENCE_MODEL_VERSION,
    '2026-03-10',
    5000,
  ]);
  assert.equal(JSON.parse(evidence.methodology_json).training_source, 'prior_rows_in_immutable_market_prediction_evidence');
});

test('canonical evidence rejects stale, noncanonical, or after-the-fact snapshots before querying', async () => {
  let prepared = 0;
  const db = { prepare: () => { prepared += 1; throw new Error('should not query'); } };

  assert.equal(await computeEmpiricalBucketMarketPredictionEvidence(db as any, canonicalSnapshot({
    benchmark_observation_date: '2026-03-09',
  })), null);
  assert.equal(await computeEmpiricalBucketMarketPredictionEvidence(db as any, canonicalSnapshot({
    canonical_slot: null,
  })), null);
  assert.equal(await computeEmpiricalBucketMarketPredictionEvidence(db as any, canonicalSnapshot({
    available_at: '2026-03-12T14:00:00.000Z',
  })), null);
  assert.equal(await computeEmpiricalBucketMarketPredictionEvidence(db as any, canonicalSnapshot({
    available_at: '2026-03-10T18:00:00.000Z',
  })), null);
  assert.equal(await computeEmpiricalBucketMarketPredictionEvidence(db as any, canonicalSnapshot({
    feature_version: 'pxi-feature-snapshot/obsolete',
  })), null);
  assert.equal(await computeEmpiricalBucketMarketPredictionEvidence(db as any, canonicalSnapshot({
    storage_contract: 'append-only-d1-research-snapshots/obsolete',
  })), null);
  assert.equal(await computeEmpiricalBucketMarketPredictionEvidence(db as any, canonicalSnapshot({
    feature_observation_dates: {},
  })), null);
  assert.equal(await computeEmpiricalBucketMarketPredictionEvidence(db as any, canonicalSnapshot({
    feature_sources: {},
  })), null);
  assert.equal(await computeEmpiricalBucketMarketPredictionEvidence(db as any, canonicalSnapshot({
    decision_date: '2026-02-31',
    benchmark_observation_date: '2026-02-31',
  })), null);
  assert.equal(prepared, 0);
});

test('canonical capture keeps the frozen model version idempotent', async () => {
  const evidence = evidenceFixture();
  let insertSql = '';
  const db = {
    prepare(sql: string) {
      const statement = () => ({
        bind: () => statement(),
        run: async () => {
          insertSql = sql;
          return { success: true, meta: { changes: 0 } };
        },
        first: async () => sql.includes('SELECT *') ? evidence : null,
      });
      return statement();
    },
  };

  const stored = await insertMarketPredictionEvidence(db as any, evidence);
  assert.equal(stored.status, 'existing');
  assert.equal(stored.evidence.evidence_id, evidence.evidence_id);
  assert.match(insertSql, /ON CONFLICT\(prediction_date, canonical_slot, model_version\) DO NOTHING/);
});

test('pending evidence evaluates each horizon from the first SPY observation within four days', async () => {
  const row = evidenceFixture();
  const outcomeBounds: unknown[][] = [];
  const updates: Array<{ sql: string; args: unknown[] }> = [];
  const evaluatedAt = '2026-02-04T22:00:00.000Z';
  const db = {
    prepare(sql: string) {
      const statement = (args: unknown[] = []) => ({
        bind: (...nextArgs: unknown[]) => statement(nextArgs),
        all: async () => ({ results: sql.includes('FROM market_prediction_evidence') ? [row] : [] }),
        first: async () => {
          if (!sql.includes('FROM research_feature_snapshots')) return null;
          outcomeBounds.push(args);
          if (args[3] === '2026-01-08') return { snapshot_id: 'outcome-7d', date: '2026-01-10', value: 110 };
          if (args[3] === '2026-01-31') return { snapshot_id: 'outcome-30d', date: '2026-02-02', value: 120 };
          return null;
        },
        run: async () => {
          if (sql.includes('UPDATE market_prediction_evidence')) updates.push({ sql, args });
          return { success: true, meta: { changes: 1 } };
        },
      });
      return statement();
    },
  };

  const result = await evaluatePendingMarketPredictionEvidence(db as any, new Date(evaluatedAt));

  assert.deepEqual(result, {
    pending: 1,
    evaluated_7d: 1,
    evaluated_30d: 1,
    unavailable_7d: 0,
    unavailable_30d: 0,
    completed: 1,
    skipped_reason: null,
  });
  assert.deepEqual(outcomeBounds, [
    [DAILY_CLOSE_CANONICAL_SLOT, RESEARCH_FEATURE_VERSION, RESEARCH_STORAGE_CONTRACT, '2026-01-08', '2026-01-12'],
    [DAILY_CLOSE_CANONICAL_SLOT, RESEARCH_FEATURE_VERSION, RESEARCH_STORAGE_CONTRACT, '2026-01-31', '2026-02-04'],
  ]);
  assert.equal(updates.length, 3);
  assert.ok(Math.abs(Number(updates[0].args[0]) - 10) < 1e-9);
  assert.equal(updates[0].args[1], '2026-01-10');
  assert.equal(updates[0].args[2], evaluatedAt);
  assert.equal(updates[0].args[3], row.evidence_id);
  assert.ok(Math.abs(Number(updates[1].args[0]) - 20) < 1e-9);
  assert.equal(updates[1].args[1], '2026-02-02');
  assert.equal(updates[1].args[2], evaluatedAt);
  assert.equal(updates[1].args[3], row.evidence_id);
  assert.deepEqual(updates[2].args, [evaluatedAt, row.evidence_id]);
});

test('pending market evidence query has a fixed 25-row budget', async () => {
  let pendingSql = '';
  const db = {
    prepare(sql: string) {
      const statement = () => ({
        bind: () => statement(),
        all: async () => {
          if (sql.includes('FROM market_prediction_evidence')) pendingSql = sql;
          return { results: [] };
        },
      });
      return statement();
    },
  };

  await evaluatePendingMarketPredictionEvidence(db as any, '2026-02-04T22:00:00.000Z');
  assert.match(pendingSql, /LIMIT 25/);
});

test('outcome evaluation uses per-horizon compare-and-set writes for concurrent runners', async () => {
  const row = evidenceFixture({ target_date_30d: '2026-12-01' });
  const updateSql: string[] = [];
  const db = {
    prepare(sql: string) {
      const statement = (args: unknown[] = []) => ({
        bind: (...nextArgs: unknown[]) => statement(nextArgs),
        all: async () => ({ results: sql.includes('FROM market_prediction_evidence') ? [row] : [] }),
        first: async () => sql.includes('FROM research_feature_snapshots')
          ? { snapshot_id: 'outcome-7d', date: '2026-01-08', value: 105 }
          : null,
        run: async () => {
          updateSql.push(sql);
          return { success: true, meta: { changes: 0 } };
        },
      });
      return statement();
    },
  };

  const result = await evaluatePendingMarketPredictionEvidence(db as any, '2026-01-10');

  assert.deepEqual(result, {
    pending: 1,
    evaluated_7d: 0,
    evaluated_30d: 0,
    unavailable_7d: 0,
    unavailable_30d: 0,
    completed: 0,
    skipped_reason: null,
  });
  assert.match(updateSql[0], /outcome_status_7d = 'pending'/);
  assert.match(updateSql[1], /evaluated_at IS NULL[\s\S]*outcome_status_7d != 'pending'/);
});

test('evidence row and count helpers pin model version and preserve chronological diagnostics order', async () => {
  const statements: Array<{ sql: string; args: unknown[] }> = [];
  const db = {
    prepare(sql: string) {
      const statement = (args: unknown[] = []) => ({
        bind: (...nextArgs: unknown[]) => statement(nextArgs),
        all: async () => {
          statements.push({ sql, args });
          return { results: [evidenceFixture()] };
        },
        first: async () => {
          statements.push({ sql, args });
          return { total: 5, evaluated: 2, pending: 3, with_7d_outcome: 4, with_30d_outcome: 2 };
        },
      });
      return statement();
    },
  };

  const rows = await fetchMarketPredictionEvidenceRows(db as any, { evaluatedOnly: true, limit: 5000 });
  const counts = await fetchMarketPredictionEvidenceCounts(db as any);

  assert.equal(rows.length, 1);
  assert.deepEqual(counts, { total: 5, evaluated: 2, pending: 3, with_7d_outcome: 4, with_30d_outcome: 2 });
  assert.match(statements[0].sql, /ORDER BY prediction_date DESC\s+LIMIT \?[\s\S]*ORDER BY prediction_date ASC/);
  assert.match(statements[0].sql, /actual_return_7d IS NOT NULL OR actual_return_30d IS NOT NULL/);
  assert.deepEqual(statements[0].args, [DAILY_CLOSE_CANONICAL_SLOT, MARKET_EVIDENCE_MODEL_VERSION, '9999-12-31', 5000]);
  assert.deepEqual(statements[1].args, [DAILY_CLOSE_CANONICAL_SLOT, MARKET_EVIDENCE_MODEL_VERSION]);
});

test('outcome evaluation refuses intraday observations before the canonical close window', async () => {
  let prepared = 0;
  const db = { prepare: () => { prepared += 1; throw new Error('should not query'); } };

  const result = await evaluatePendingMarketPredictionEvidence(
    db as any,
    '2026-02-04T18:00:00.000Z',
  );

  assert.deepEqual(result, {
    pending: 0,
    evaluated_7d: 0,
    evaluated_30d: 0,
    unavailable_7d: 0,
    unavailable_30d: 0,
    completed: 0,
    skipped_reason: 'outside_daily_close_window',
  });
  assert.equal(prepared, 0);
});

test('outcome evaluation terminally records canonical-close gaps in bounded batches', async () => {
  const row = evidenceFixture({
    target_date_7d: '2026-01-08',
    target_date_30d: '2026-01-31',
  });
  const updates: string[] = [];
  const db = {
    prepare(sql: string) {
      const statement = () => ({
        bind: () => statement(),
        all: async () => ({ results: sql.includes('FROM market_prediction_evidence') ? [row] : [] }),
        first: async () => null,
        run: async () => {
          updates.push(sql);
          return { success: true, meta: { changes: 1 } };
        },
      });
      return statement();
    },
  };

  const result = await evaluatePendingMarketPredictionEvidence(db as any, '2026-02-05');

  assert.deepEqual(result, {
    pending: 1,
    evaluated_7d: 0,
    evaluated_30d: 0,
    unavailable_7d: 1,
    unavailable_30d: 1,
    completed: 1,
    skipped_reason: null,
  });
  assert.match(updates[0], /outcome_status_7d = 'unavailable'/);
  assert.match(updates[1], /outcome_status_30d = 'unavailable'/);
  assert.match(updates[2], /outcome_status_7d != 'pending'/);
});

test('outcome evaluation uses the New York decision date after UTC midnight', async () => {
  const queryArgs: unknown[][] = [];
  const db = {
    prepare() {
      const statement = (args: unknown[] = []) => ({
        bind: (...nextArgs: unknown[]) => statement(nextArgs),
        all: async () => {
          queryArgs.push(args);
          return { results: [] };
        },
      });
      return statement();
    },
  };

  await evaluatePendingMarketPredictionEvidence(db as any, '2026-02-05T01:00:00.000Z');
  assert.deepEqual(queryArgs[0], ['2026-02-04', '2026-02-04']);
});

test('capture helper returns null rather than reserving evidence for an invalid snapshot', async () => {
  const result = await captureCanonicalMarketPredictionEvidence({} as D1Database, canonicalSnapshot({
    benchmark_observation_date: '2026-03-09',
  }));
  assert.equal(result, null);
});
