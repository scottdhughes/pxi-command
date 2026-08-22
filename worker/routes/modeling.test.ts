import assert from 'node:assert/strict';
import test from 'node:test';

import { tryHandleModelingRoute } from './modeling.js';

type QueryResult =
  | null
  | undefined
  | Record<string, unknown>
  | { results: Record<string, unknown>[] }
  | Record<string, unknown>[];

function createFakeDb(handler: (sql: string, args: unknown[]) => QueryResult) {
  const buildStatement = (sql: string, args: unknown[] = []) => ({
    bind: (...boundArgs: unknown[]) => buildStatement(sql, boundArgs),
    all: async <T>() => {
      const result = handler(sql, args);
      if (Array.isArray(result)) {
        return { results: result as T[] };
      }
      if (result && typeof result === 'object' && 'results' in result) {
        return result as { results: T[] };
      }
      return { results: result ? [result as T] : [] };
    },
    first: async <T>() => {
      const result = handler(sql, args);
      if (Array.isArray(result)) {
        return (result[0] ?? null) as T | null;
      }
      if (result && typeof result === 'object' && 'results' in result) {
        return ((result as { results?: T[] }).results?.[0] ?? null) as T | null;
      }
      return (result ?? null) as T | null;
    },
    run: async () => {
      handler(sql, args);
      return { success: true };
    },
  });

  return {
    prepare(sql: string) {
      return buildStatement(sql);
    },
    async batch(statements: unknown[]) {
      return { success: true, statements: statements.length };
    },
  };
}

function createExecutionContext() {
  const waits: Promise<unknown>[] = [];
  return {
    waits,
    context: {
      waitUntil(promise: Promise<unknown>) {
        waits.push(Promise.resolve(promise));
      },
      passThroughOnException() {},
    },
  };
}

function createRouteContext(url: string, init?: RequestInit, envOverrides: Record<string, unknown> = {}) {
  const request = new Request(url, init);
  const execution = createExecutionContext();
  return {
    request,
    env: {
      DB: createFakeDb(() => null),
      AI: { run: async () => ({ response: 'ok' }) },
      VECTORIZE: {
        query: async () => ({ matches: [] }),
        upsert: async () => undefined,
      },
      ML_MODELS: {},
      ...envOverrides,
    },
    url: new URL(request.url),
    method: request.method,
    corsHeaders: {},
    clientIP: '127.0.0.1',
    executionContext: execution.context,
    waits: execution.waits,
  };
}

test('tryHandleModelingRoute serves /api/predict with empirical stats', async () => {
  const route = createRouteContext('https://pxi.test/api/predict', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('SELECT date, score, label FROM pxi_scores ORDER BY date DESC LIMIT 1')) {
        return { date: '2026-03-05', score: 72, label: 'risk-on' };
      }
      if (sql.includes('SELECT date, score FROM (') && sql.includes('FROM pxi_scores ORDER BY date DESC LIMIT ?')) {
        return [
          { date: '2026-02-20', score: 72 },
          { date: '2026-02-25', score: 74 },
        ];
      }
      if (sql.includes("WHERE indicator_id = 'spy_close'")) {
        return [
          { date: '2026-02-20', value: 100 },
          { date: '2026-02-27', value: 104 },
          { date: '2026-03-22', value: 110 },
          { date: '2026-02-25', value: 101 },
          { date: '2026-03-04', value: 105 },
          { date: '2026-03-27', value: 109 },
        ];
      }
      if (sql.includes("param_key LIKE 'bucket_threshold_%'")) {
        return [];
      }
      if (sql.includes('FROM prediction_log')) {
        return null;
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandleModelingRoute(route as any, {});
  assert.ok(response);
  assert.equal(response?.status, 200);
  const payload = await response!.json() as any;
  assert.equal(payload.current.bucket, '60-80');
  assert.equal(payload.prediction.d7.sample_size, 2);
});

test('tryHandleModelingRoute enforces auth on /api/evaluate', async () => {
  const route = createRouteContext('https://pxi.test/api/evaluate', { method: 'POST' });
  const response = await tryHandleModelingRoute(route as any, {
    enforceAdminAuth: async () => Response.json({ error: 'Unauthorized' }, { status: 401 }),
  });
  assert.equal(response?.status, 401);
});

test('tryHandleModelingRoute bounds due prediction evaluation work', async () => {
  const queries: Array<{ sql: string; args: unknown[] }> = [];
  const route = createRouteContext('https://pxi.test/api/evaluate', { method: 'POST' }, {
    DB: createFakeDb((sql, args) => {
      queries.push({ sql, args });
      return [];
    }),
  });
  const response = await tryHandleModelingRoute(route as any, {
    enforceAdminAuth: async () => null,
  });

  assert.equal(response?.status, 200);
  const predictionQuery = queries.find((query) => query.sql.includes('FROM prediction_log'));
  assert.ok(predictionQuery);
  assert.match(predictionQuery.sql, /target_date_7d <= \?/);
  assert.match(predictionQuery.sql, /target_date_30d <= \?/);
  assert.match(predictionQuery.sql, /LIMIT 25/);
  assert.equal(predictionQuery.args.length, 2);
  const payload = await response!.json() as Record<string, unknown>;
  assert.equal(payload.batch_limit, 25);
});

test('tryHandleModelingRoute returns 503 for missing /api/ml/predict model', async () => {
  const route = createRouteContext('https://pxi.test/api/ml/predict');
  const response = await tryHandleModelingRoute(route as any, {
    loadMLModel: async () => null,
  });
  assert.equal(response?.status, 503);
});

test('tryHandleModelingRoute keeps public /api/ml/ensemble reads side-effect free', async () => {
  const route = createRouteContext('https://pxi.test/api/ml/ensemble', undefined, {
    DB: createFakeDb((sql, args) => {
      if (sql.includes('SELECT date, score, delta_1d, delta_7d, delta_30d FROM pxi_scores ORDER BY date DESC LIMIT 1')) {
        return { date: '2026-03-05', score: 70, delta_1d: 1, delta_7d: 3, delta_30d: 5 };
      }
      if (sql.includes('SELECT category, score FROM category_scores WHERE date = ?')) {
        return [{ category: 'macro', score: 75 }];
      }
      if (sql.includes('SELECT indicator_id, value FROM indicator_values WHERE date = ?')) {
        return [{ indicator_id: 'vix', value: 18 }];
      }
      if (sql.includes('SELECT score FROM pxi_scores ORDER BY date DESC LIMIT 20')) {
        return [{ score: 70 }, { score: 68 }];
      }
      if (sql.includes('SELECT date, score, delta_7d FROM pxi_scores ORDER BY date DESC LIMIT ?')) {
        return [
          { date: '2026-03-05', score: 70, delta_7d: 3 },
          { date: '2026-03-04', score: 68, delta_7d: 2 },
        ];
      }
      if (sql.includes('SELECT date, category, score FROM category_scores')) {
        return [
          { date: '2026-03-05', category: 'macro', score: 75 },
          { date: '2026-03-04', category: 'macro', score: 73 },
        ];
      }
      if (sql.includes("WHERE indicator_id = 'vix' AND date IN")) {
        return [
          { date: '2026-03-05', value: 18 },
          { date: '2026-03-04', value: 19 },
        ];
      }
      throw new Error(`Unhandled query: ${sql} :: ${JSON.stringify(args)}`);
    }),
  });

  const response = await tryHandleModelingRoute(route as any, {
    loadMLModel: async () => ({
      m: { '7d': {}, '30d': {} },
      f: [],
    }),
    loadLSTMModel: async () => ({
      v: 'lstm-v1',
      c: { s: 2, h: 1, f: ['pxi_score'] },
      n: {},
      m: {
        '7d': { lstm: {}, fc: {} },
        '30d': { lstm: {}, fc: {} },
      },
    }),
    extractMLFeatures: () => ({ pxi_score: 70 }),
    xgbPredict: (_model: unknown, _features: unknown, names?: string[]) => names === undefined ? 0 : names.length === 0 ? 6 : 6,
    extractLSTMFeatures: () => [0.1],
    lstmForward: (_sequence: unknown, _lstm: unknown, fc: unknown) => fc ? 2 : 2,
  });

  assert.equal(response?.status, 200);
  const payload = await response!.json() as any;
  assert.equal(payload.ensemble.predictions.pxi_change_7d.confidence, 'MEDIUM');
  await Promise.all((route as any).waits);
  assert.equal((route as any).waits.length, 0);
});

test('tryHandleModelingRoute returns empty coverage payload for /api/accuracy with no predictions', async () => {
  const route = createRouteContext('https://pxi.test/api/accuracy');
  const response = await tryHandleModelingRoute(route as any, {
    MINIMUM_RELIABLE_SAMPLE: 30,
    asIsoDateTime: () => '2026-03-05T00:00:00.000Z',
  });
  assert.equal(response?.status, 200);
  const payload = await response!.json() as any;
  assert.equal(payload.coverage_quality, 'INSUFFICIENT');
  assert.equal(payload.metrics, null);
});

test('tryHandleModelingRoute returns empty ensemble coverage payload for /api/ml/accuracy with no predictions', async () => {
  const route = createRouteContext('https://pxi.test/api/ml/accuracy');
  const response = await tryHandleModelingRoute(route as any, {
    MINIMUM_RELIABLE_SAMPLE: 30,
    asIsoDateTime: () => '2026-03-05T00:00:00.000Z',
  });
  assert.equal(response?.status, 200);
  const payload = await response!.json() as any;
  assert.equal(payload.coverage_quality, 'INSUFFICIENT');
  assert.equal(payload.metrics, null);
});

test('tryHandleModelingRoute returns 503 for /api/ml/backtest without models', async () => {
  const route = createRouteContext('https://pxi.test/api/ml/backtest');
  const response = await tryHandleModelingRoute(route as any, {
    loadMLModel: async () => null,
    loadLSTMModel: async () => null,
  });
  assert.equal(response?.status, 503);
});

test('tryHandleModelingRoute clamps attacker-controlled ML backtest bounds', async () => {
  let queryBounds: unknown[] = [];
  const route = createRouteContext('https://pxi.test/api/ml/backtest?limit=999999999&offset=-50', undefined, {
    DB: createFakeDb((sql, args) => {
      if (sql.includes('FROM pxi_scores p')) {
        queryBounds = args;
        return [];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });
  const response = await tryHandleModelingRoute(route as any, {
    loadMLModel: async () => ({ m: {}, f: [] }),
    loadLSTMModel: async () => null,
  });

  assert.equal(response?.status, 400);
  assert.deepEqual(queryBounds, [500, 0]);
});

test('tryHandleModelingRoute returns no-op summary for /api/retrain without evaluated predictions', async () => {
  const route = createRouteContext('https://pxi.test/api/retrain', { method: 'POST' });
  const response = await tryHandleModelingRoute(route as any, {
    enforceAdminAuth: async () => null,
  });
  assert.equal(response?.status, 200);
  const payload = await response!.json() as any;
  assert.equal(payload.periods_updated, 0);
});

test('tryHandleModelingRoute serves /api/model', async () => {
  const route = createRouteContext('https://pxi.test/api/model', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('SELECT param_key, param_value, updated_at, notes FROM model_params')) {
        return [{ param_key: 'accuracy_weight', param_value: 0.3, updated_at: '2026-03-05', notes: 'n' }];
      }
      if (sql.includes('SELECT period_date, accuracy_score, times_used, avg_error_7d')) {
        return [{ period_date: '2026-02-01', accuracy_score: 0.7, times_used: 2, avg_error_7d: 1.1 }];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandleModelingRoute(route as any, {});
  const payload = await response!.json() as any;
  assert.equal(payload.params.length, 1);
  assert.equal(payload.top_accurate_periods.length, 1);
});

test('tryHandleModelingRoute returns 400 for /api/backtest without SPY data', async () => {
  const route = createRouteContext('https://pxi.test/api/backtest', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('SELECT date, score FROM (') && sql.includes('FROM pxi_scores ORDER BY date DESC LIMIT ?')) {
        return [{ date: '2026-03-05', score: 70 }];
      }
      if (sql.includes("WHERE indicator_id = 'spy_close'")) {
        return [];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandleModelingRoute(route as any, {});
  assert.equal(response?.status, 400);
});

test('tryHandleModelingRoute caps public raw backtest output', async () => {
  const rows = Array.from({ length: 600 }, (_, index) => {
    const date = new Date(Date.UTC(2024, 0, 1 + index)).toISOString().slice(0, 10);
    return { date, score: 50 + (index % 20), value: 100 + index };
  });
  const route = createRouteContext('https://pxi.test/api/backtest?raw=true', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('FROM pxi_scores ORDER BY date DESC LIMIT ?')) {
        return rows.map(({ date, score }) => ({ date, score }));
      }
      if (sql.includes("WHERE indicator_id = 'spy_close'")) {
        return rows.map(({ date, value }) => ({ date, value }));
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandleModelingRoute(route as any, {});
  const payload = await response!.json() as any;
  assert.equal(response?.status, 200);
  assert.equal(payload.raw_data.length, 500);
  assert.equal(payload.raw_data_truncated, true);
});

test('tryHandleModelingRoute returns 400 for /api/backtest/signal with insufficient signal history', async () => {
  const route = createRouteContext('https://pxi.test/api/backtest/signal', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('FROM pxi_signal ORDER BY date DESC LIMIT ?')) {
        return [{ date: '2026-03-05', pxi_level: 70, risk_allocation: 0.8, signal_type: 'FULL_RISK', regime: 'RISK_ON' }];
      }
      if (sql.includes("WHERE indicator_id = 'spy_close'")) {
        return [{ date: '2026-03-05', value: 100 }];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandleModelingRoute(route as any, {
    formatDate: () => '2026-03-05',
  });
  assert.equal(response?.status, 400);
});

test('public signal backtest computes results without writing to D1', async () => {
  const spyRows = Array.from({ length: 230 }, (_, index) => {
    const date = new Date(Date.UTC(2025, 0, 1 + index)).toISOString().slice(0, 10);
    return { date, value: 100 + index };
  });
  const signalRows = spyRows.slice(-30).map(({ date }, index) => ({
    date,
    pxi_level: 60,
    risk_allocation: 0.75,
    signal_type: index % 2 === 0 ? 'FULL_RISK' : 'REDUCED_RISK',
    regime: 'RISK_ON',
  }));
  let writeAttempted = false;
  const route = createRouteContext('https://pxi.test/api/backtest/signal', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('INSERT OR REPLACE INTO backtest_results')) {
        writeAttempted = true;
        throw new Error('public GET attempted a write');
      }
      if (sql.includes('FROM pxi_signal ORDER BY date DESC LIMIT ?')) return signalRows;
      if (sql.includes("WHERE indicator_id = 'spy_close'")) return spyRows;
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandleModelingRoute(route as any, {});
  assert.equal(response?.status, 200);
  assert.equal(writeAttempted, false);
});

test('tryHandleModelingRoute serves /api/backtest/history', async () => {
  const route = createRouteContext('https://pxi.test/api/backtest/history', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('SELECT * FROM backtest_results ORDER BY run_date DESC LIMIT 10')) {
        return [{ run_date: '2026-03-05', strategy: 'PXI-Signal' }];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandleModelingRoute(route as any, {});
  const payload = await response!.json() as any;
  assert.equal(payload.history.length, 1);
});

test('tryHandleModelingRoute exports /api/export/history as CSV', async () => {
  const route = createRouteContext('https://pxi.test/api/export/history?format=csv&days=7', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('FROM pxi_scores p')) {
        return [{ date: '2026-03-05', score: 70, label: 'risk-on', status: 'bullish', delta_1d: 1, delta_7d: 2, delta_30d: 4 }];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandleModelingRoute(route as any, {});
  assert.equal(response?.headers.get('Content-Type'), 'text/csv');
  const csv = await response!.text();
  assert.match(csv, /date,score,label,status/);
});

test('tryHandleModelingRoute enforces auth on /api/export/training-data', async () => {
  const route = createRouteContext('https://pxi.test/api/export/training-data');
  const response = await tryHandleModelingRoute(route as any, {
    enforceAdminAuth: async () => Response.json({ error: 'Unauthorized' }, { status: 401 }),
  });
  assert.equal(response?.status, 401);
});

test('tryHandleModelingRoute enforces auth on /api/export/research-snapshot', async () => {
  const route = createRouteContext('https://pxi.test/api/export/research-snapshot');
  const response = await tryHandleModelingRoute(route as any, {
    enforceAdminAuth: async () => Response.json({ error: 'Unauthorized' }, { status: 401 }),
  });
  assert.equal(response?.status, 401);
});

test('tryHandleModelingRoute exports immutable point-in-time research snapshots', async () => {
  const stored = {
    snapshot_id: 'snapshot-1',
    decision_date: '2026-08-21',
    available_at: '2026-08-21T20:00:00.000Z',
    feature_version: 'pxi-feature-snapshot/v1',
    storage_contract: 'append-only-d1-research-snapshots/v1',
    capture_source: 'test',
    canonical_slot: 'daily_close_22z',
    benchmark_close: 651.4,
    benchmark_observation_date: '2026-08-21',
    features: { pxi_score: 61.2, indicator_ism_manufacturing: 48.2 },
    feature_observation_dates: { pxi_score: '2026-08-21', indicator_ism_manufacturing: '2026-08-03' },
    feature_sources: { pxi_score: 'pxi-calculation', indicator_ism_manufacturing: 'fred' },
  };
  const route = createRouteContext('https://pxi.test/api/export/research-snapshot', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('FROM research_feature_snapshots')) {
        return [{ payload_json: JSON.stringify(stored) }];
      }
      return null;
    }),
  });
  const response = await tryHandleModelingRoute(route as any, {
    enforceAdminAuth: async () => null,
  });
  assert.equal(response?.status, 200);
  const payload = await response!.json() as any;
  assert.equal(payload.point_in_time_guarantee, true);
  assert.equal(payload.rows.length, 1);
  assert.equal(payload.rows[0].immutable_snapshot, true);
  assert.equal(payload.rows[0].canonical_slot, 'daily_close_22z');
  assert.equal(payload.rows[0].feature_sources.indicator_ism_manufacturing, 'fred');
});
