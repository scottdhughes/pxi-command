import assert from 'node:assert/strict';
import test from 'node:test';

import { tryHandleAdminIngestionRoute } from './admin-ingestion.js';
import { selectLatestPxiWithCategories } from '../domain/market-core.js';
import { tryHandlePublicReadRoute } from './public-read.js';
import { tryHandleSimilarityRoute } from './similarity.js';
import { tryHandleSystemRoute } from './system.js';

type QueryResult =
  | null
  | undefined
  | Record<string, unknown>
  | { results: Record<string, unknown>[] }
  | Record<string, unknown>[];

function createFakeDb(handler: (sql: string, args: unknown[]) => QueryResult) {
  const buildStatement = (sql: string, args: unknown[] = []) => ({
    sql,
    args,
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
    run: async () => ({ success: true }),
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

function createRouteContext(url: string, init?: RequestInit, envOverrides: Record<string, unknown> = {}) {
  const request = new Request(url, init);
  return {
    request,
    env: {
      DB: createFakeDb(() => null),
      AI: { run: async () => ({ response: 'ok' }) },
      VECTORIZE: {
        query: async () => ({ matches: [] }),
        upsert: async () => undefined,
      },
      ...envOverrides,
    },
    url: new URL(request.url),
    method: request.method,
    corsHeaders: {},
    clientIP: '127.0.0.1',
  };
}

const CANONICAL_PXI_CATEGORIES = [
  'breadth',
  'credit',
  'crypto',
  'global',
  'macro',
  'positioning',
  'volatility',
];

test('tryHandleSystemRoute serves /health', async () => {
  const route = createRouteContext('https://pxi.test/health', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('SELECT 1 as ok')) return { ok: 1 };
      if (sql.includes('FROM sqlite_master')) return { ready: 1 };
      throw new Error(`Unhandled query: ${sql}`);
    }),
    DEPLOY_ENV: 'production',
    BUILD_SHA: 'a1b2c3d4e5f6',
    BUILD_TIMESTAMP: '2026-03-06T14:22:00.000Z',
    WORKER_VERSION: 'pxi-a1b2c3d4e5f6-2026-03-06T14:22:00.000Z',
  });

  const response = await tryHandleSystemRoute(route as any);
  assert.ok(response);
  assert.equal(response?.status, 200);
  const body = await response!.json() as Record<string, unknown>;
  assert.equal(body.status, 'healthy');
  assert.equal(body.db, true);
  assert.equal(body.environment, 'production');
  assert.equal(body.build_sha, 'a1b2c3d4e5f6');
  assert.equal(body.build_timestamp, '2026-03-06T14:22:00.000Z');
  assert.equal(body.worker_version, 'pxi-a1b2c3d4e5f6-2026-03-06T14:22:00.000Z');
  assert.equal(body.history_reconstruction_contract, 'isolated-missing-only-v1');
});

test('tryHandleSystemRoute serves safe defaults on /health when deploy metadata is absent', async () => {
  const route = createRouteContext('https://pxi.test/health', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('SELECT 1 as ok')) return { ok: 1 };
      if (sql.includes('FROM sqlite_master')) return { ready: 0 };
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandleSystemRoute(route as any);
  assert.ok(response);
  const body = await response!.json() as Record<string, unknown>;
  assert.equal(body.environment, 'development');
  assert.equal(body.build_sha, 'local-dev');
  assert.equal(body.build_timestamp, '1970-01-01T00:00:00.000Z');
  assert.equal(body.worker_version, 'pxi-dev');
  assert.equal(body.history_reconstruction_contract, 'unavailable');
});

test('tryHandleSystemRoute serves /og-image.svg', async () => {
  const route = createRouteContext('https://pxi.test/og-image.svg', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('SELECT score, label, status, delta_7d FROM pxi_scores')) {
        return { score: 72, label: 'risk-on', status: 'pamping', delta_7d: 3.5 };
      }
      if (sql.includes('SELECT category, score FROM category_scores')) {
        return [{ category: 'macro', score: 74 }];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandleSystemRoute(route as any);
  assert.ok(response);
  assert.equal(response?.headers.get('Content-Type'), 'image/svg+xml');
  const svg = await response!.text();
  assert.match(svg, /PXI/);
});

test('tryHandlePublicReadRoute serves /api/history in chronological order', async () => {
  const route = createRouteContext('https://pxi.test/api/history?days=2', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('FROM pxi_scores p')) {
        return [
          {
            date: '2026-03-05',
            score: 70,
            label: 'risk-on',
            status: 'bullish',
            history_origin: 'live_recorded',
            reconstructed_at: null,
            reconstruction_method: null,
            reconstruction_build_sha: null,
            source_data_as_of: null,
          },
          {
            date: '2026-03-02',
            score: 55,
            label: 'neutral',
            status: 'neutral',
            history_origin: 'retrospective_reconstruction',
            reconstructed_at: '2026-08-22T18:00:00.000Z',
            reconstruction_method: 'current_indicator_store_percentile_v1',
            reconstruction_build_sha: 'a1b2c3d4e5f6',
            source_data_as_of: '2026-08-22T17:55:00.000Z',
          },
        ];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandlePublicReadRoute(route as any, {});
  assert.ok(response);
  const payload = await response!.json() as any;
  assert.equal(payload.data[0].date, '2026-03-02');
  assert.equal(payload.data[1].regime, 'RISK_ON');
  assert.deepEqual(payload.provenance_counts, {
    legacy_unclassified: 0,
    live_recorded: 1,
    retrospective_reconstruction: 1,
  });
  assert.deepEqual(payload.continuity, {
    is_contiguous: false,
    start_date: '2026-03-02',
    end_date: '2026-03-05',
    observed_days: 2,
    expected_days: 4,
    missing_days: 2,
    gap_count: 1,
    gaps: [{ start_date: '2026-03-03', end_date: '2026-03-04', missing_days: 2 }],
  });
});

test('tryHandlePublicReadRoute serves /api/alerts summaries', async () => {
  const route = createRouteContext('https://pxi.test/api/alerts?limit=10', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('FROM alerts WHERE 1=1')) {
        return [{
          id: 1,
          date: '2026-03-05',
          alert_type: 'bullish_breakout',
          message: 'Breakout',
          severity: 'info',
          acknowledged: 0,
          pxi_score: 72,
          forward_return_7d: 1.2,
          forward_return_30d: 2.8,
          created_at: '2026-03-05T12:00:00.000Z',
        }];
      }
      if (sql.includes('GROUP BY alert_type ORDER BY count DESC')) {
        return [{ alert_type: 'bullish_breakout', count: 1 }];
      }
      if (sql.includes('AVG(forward_return_7d) as avg_return_7d')) {
        return [{ alert_type: 'bullish_breakout', total: 1, correct_7d: 1, avg_return_7d: 1.2 }];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandlePublicReadRoute(route as any, {});
  const payload = await response!.json() as any;
  assert.equal(payload.alerts.length, 1);
  assert.equal(payload.alerts[0].acknowledged, false);
  assert.equal(payload.filters.types[0].type, 'bullish_breakout');
});

test('tryHandlePublicReadRoute rejects invalid category paths', async () => {
  const route = createRouteContext('https://pxi.test/api/category/invalid');
  const response = await tryHandlePublicReadRoute(route as any, {});
  assert.equal(response?.status, 400);
});

test('tryHandlePublicReadRoute preserves legacy indicator IDs and exposes category provenance', async () => {
  const route = createRouteContext('https://pxi.test/api/category/macro', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('WITH latest AS')) {
        return [{
          indicator_id: 'ism_manufacturing',
          observation_date: '2026-07-01',
          raw_value: 12611,
          source: 'fred',
          fetched_at: '2026-08-23 14:00:31',
          normalized_value: 11.875,
        }];
      }
      if (sql.includes('SELECT date, score FROM category_scores')) {
        return [
          { date: '2026-08-22', score: 63.4 },
          { date: '2026-08-23', score: 63.47 },
        ];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandlePublicReadRoute(route as any, {
    selectLatestPxiWithCategories: async () => ({
      pxi: { date: '2026-08-23' },
      categories: [{ category: 'macro', score: 63.47, weight: 0.1 }],
    }),
  });
  assert.ok(response);
  assert.equal(response!.status, 200);

  const payload = await response!.json() as any;
  assert.deepEqual(payload.indicators, [{
    id: 'ism_manufacturing',
    canonical_id: 'manufacturing_payrolls',
    legacy_id: 'ism_manufacturing',
    identity_status: 'legacy_storage_id',
    definition_version: 'indicator-contract/v1',
    name: 'Manufacturing Payrolls',
    raw_value: 12611,
    normalized_value: 11.875,
    source: 'fred',
    observed_source: 'fred',
    configured_source: 'fred',
    series: 'MANEMP',
    source_series: 'MANEMP',
    frequency: 'monthly',
    observation_date: '2026-07-01',
    fetched_at: '2026-08-23 14:00:31',
    description: 'Manufacturing employees, thousands (FRED MANEMP); legacy internal ID retained for history compatibility',
    units: 'Thousands of persons, seasonally adjusted',
    source_url: 'https://fred.stlouisfed.org/series/MANEMP',
    publisher: 'U.S. Bureau of Labor Statistics via FRED',
    release_name: 'Current Employment Statistics',
    freshness: {
      status: 'fresh',
      basis: 'observation_date_sla',
      age_days: 53,
      max_age_days: 65,
      sla_class: 'monthly',
    },
  }]);
});

test('tryHandlePublicReadRoute anchors category detail to the latest exact PXI state', async () => {
  let historyArgs: unknown[] = [];
  const route = createRouteContext('https://pxi.test/api/category/macro', undefined, {
    DB: createFakeDb((sql, args) => {
      if (sql.includes('FROM pxi_scores ORDER BY date DESC LIMIT 10')) {
        return [
          {
            date: '2026-08-23',
            score: 71,
            label: 'Constructive',
            status: 'GREEN',
            delta_1d: 1,
            delta_7d: 2,
            delta_30d: 3,
          },
          {
            date: '2026-08-22',
            score: 69,
            label: 'Constructive',
            status: 'GREEN',
            delta_1d: 0,
            delta_7d: 1,
            delta_30d: 2,
          },
        ];
      }
      if (sql.includes('SELECT category, score, weight FROM category_scores')) {
        const date = String(args[0]);
        const categories = date === '2026-08-23'
          ? CANONICAL_PXI_CATEGORIES.filter((candidate) => candidate !== 'macro')
          : CANONICAL_PXI_CATEGORIES;
        return categories.map((candidate) => ({
          category: candidate,
          score: candidate === 'macro' ? 63.47 : 55,
          weight: 0.1,
        }));
      }
      if (sql.includes('WITH latest AS')) return [];
      if (sql.includes('SELECT date, score FROM category_scores')) {
        historyArgs = args;
        return [{ date: '2026-08-22', score: 63.47 }];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandlePublicReadRoute(route as any, {
    selectLatestPxiWithCategories,
  });
  assert.ok(response);
  assert.equal(response!.status, 200);

  const payload = await response!.json() as any;
  assert.equal(payload.date, '2026-08-22');
  assert.equal(payload.score, 63.47);
  assert.equal(payload.weight, 0.1);
  assert.deepEqual(historyArgs, ['macro', '2026-08-22']);
});

test('tryHandlePublicReadRoute serves /api/analyze', async () => {
  const route = createRouteContext('https://pxi.test/api/analyze', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('SELECT date, score, label, status FROM pxi_scores')) {
        return { date: '2026-03-05', score: 72, label: 'risk-on', status: 'bullish' };
      }
      if (sql.includes('SELECT category, score FROM category_scores')) {
        return [{ category: 'macro', score: 74 }];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
    AI: {
      run: async () => ({ response: 'Risk appetite is firm.' }),
    },
  });

  const response = await tryHandlePublicReadRoute(route as any, {});
  const payload = await response!.json() as Record<string, unknown>;
  assert.equal(payload.analysis, 'Risk appetite is firm.');
});

test('tryHandleSimilarityRoute surfaces Workers AI failures on /api/similar', async () => {
  const route = createRouteContext('https://pxi.test/api/similar', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('SELECT date, score, delta_7d, delta_30d FROM pxi_scores')) {
        return { date: '2026-03-05', score: 72, delta_7d: 2.1, delta_30d: 4.8 };
      }
      if (sql.includes('SELECT indicator_id, value FROM indicator_values')) {
        return [{ indicator_id: 'vix', value: 18.2 }];
      }
      if (sql.includes('SELECT category, score FROM category_scores')) {
        return [{ category: 'macro', score: 74 }];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
    AI: {
      run: async () => {
        throw new Error('ai_down');
      },
    },
  });

  const response = await tryHandleSimilarityRoute(route as any, {
    generateEmbeddingText: () => 'embedding text',
    getEmbeddingVector: () => [0.1, 0.2],
  });
  assert.equal(response?.status, 503);
  const payload = await response!.json() as Record<string, unknown>;
  assert.equal(payload.error, 'AI embedding failed');
  assert.equal(Object.hasOwn(payload, 'details'), false);
});

test('tryHandleSimilarityRoute embeds historical dates on /api/embed', async () => {
  let upserted = 0;
  const route = createRouteContext('https://pxi.test/api/embed', { method: 'POST' }, {
    DB: createFakeDb((sql, args) => {
      if (sql.includes('SELECT DISTINCT date FROM indicator_values')) {
        return [{ date: '2026-03-05' }];
      }
      if (sql.includes('SELECT indicator_id, value FROM indicator_values')) {
        return Array.from({ length: 10 }, (_, index) => ({ indicator_id: `id_${index}`, value: index + 1 }));
      }
      throw new Error(`Unhandled query: ${sql} ${JSON.stringify(args)}`);
    }),
    AI: {
      run: async () => ({ data: [[0.1, 0.2, 0.3]] }),
    },
    VECTORIZE: {
      query: async () => ({ matches: [] }),
      upsert: async () => {
        upserted += 1;
      },
    },
  });

  const response = await tryHandleSimilarityRoute(route as any, {
    enforceAdminAuth: async () => null,
    getEmbeddingVector: (embedding: any) => embedding.data[0],
  });
  const payload = await response!.json() as Record<string, unknown>;
  assert.equal(payload.embedded_dates, 1);
  assert.equal(upserted, 1);
});

test('tryHandleSimilarityRoute rejects unauthenticated embedding rebuilds', async () => {
  const route = createRouteContext('https://pxi.test/api/embed', { method: 'POST' });
  const response = await tryHandleSimilarityRoute(route as any, {
    enforceAdminAuth: async () => Response.json({ error: 'Unauthorized' }, { status: 401 }),
  });
  assert.equal(response?.status, 401);
});

test('tryHandleAdminIngestionRoute enforces auth on /api/migrate', async () => {
  const route = createRouteContext('https://pxi.test/api/migrate', { method: 'POST' });
  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => Response.json({ error: 'Unauthorized' }, { status: 401 }),
  });
  assert.equal(response?.status, 401);
});

test('tryHandleAdminIngestionRoute keeps /api/write successful when embedding fails', async () => {
  const route = createRouteContext('https://pxi.test/api/write', {
    method: 'POST',
    body: JSON.stringify({
      pxi: {
        date: '2026-03-05',
        score: 72,
        label: 'risk-on',
        status: 'bullish',
        delta_1d: 1,
        delta_7d: 2,
        delta_30d: 4,
      },
    }),
    headers: {
      'Content-Type': 'application/json',
    },
  }, {
    DB: createFakeDb((sql) => {
      if (sql.includes('SELECT indicator_id, value FROM indicator_values')) {
        return Array.from({ length: 10 }, (_, index) => ({ indicator_id: `id_${index}`, value: index + 1 }));
      }
      return null;
    }),
    AI: {
      run: async () => {
        throw new Error('embedding_failed');
      },
    },
  });

  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => null,
    currentNewYorkDate: () => '2026-03-05',
    getEmbeddingVector: () => [0.1, 0.2],
  });
  const payload = await response!.json() as Record<string, unknown>;
  assert.equal(payload.success, true);
  assert.equal(payload.written, 1);
});

test('tryHandleAdminIngestionRoute rejects historical score/category writes before touching D1', async () => {
  let databaseTouched = false;
  const route = createRouteContext('https://pxi.test/api/write', {
    method: 'POST',
    body: JSON.stringify({
      categories: [{
        category: 'macro',
        date: '2026-03-04',
        score: 70,
        weight: 0.3,
        weighted_score: 21,
      }],
      pxi: {
        date: '2026-03-04',
        score: 72,
        label: 'risk-on',
        status: 'bullish',
        delta_1d: 1,
        delta_7d: 2,
        delta_30d: 4,
      },
    }),
    headers: { 'Content-Type': 'application/json' },
  }, {
    DB: {
      prepare() {
        databaseTouched = true;
        throw new Error('D1 must not be touched for rejected historical score writes');
      },
      async batch() {
        databaseTouched = true;
        throw new Error('D1 must not be touched for rejected historical score writes');
      },
    },
  });

  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => null,
    currentNewYorkDate: () => '2026-03-05',
  });
  assert.equal(response?.status, 400);
  const payload = await response!.json() as any;
  assert.equal(payload.current_date, '2026-03-05');
  assert.deepEqual(payload.invalid_fields, [
    { field: 'categories[0].date', date: '2026-03-04' },
    { field: 'pxi.date', date: '2026-03-04' },
  ]);
  assert.equal(databaseTouched, false);
});

test('tryHandleAdminIngestionRoute still accepts historical indicator-only writes', async () => {
  const batches: unknown[][] = [];
  const route = createRouteContext('https://pxi.test/api/write', {
    method: 'POST',
    body: JSON.stringify({
      type: 'indicator',
      data: {
        indicator_id: 'vix',
        date: '2026-02-20',
        value: 18.5,
        source: 'test',
      },
    }),
    headers: { 'Content-Type': 'application/json' },
  }, {
    DB: {
      ...createFakeDb(() => null),
      async batch(statements: unknown[]) {
        batches.push(statements);
        return { success: true };
      },
    },
  });

  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => null,
    currentNewYorkDate: () => '2026-03-05',
  });
  assert.equal(response?.status, 200);
  const payload = await response!.json() as any;
  assert.equal(payload.written, 1);
  assert.equal(batches.length, 1);
});

test('tryHandleAdminIngestionRoute rejects historical /api/recalculate before calculation', async () => {
  let calculateCalls = 0;
  const route = createRouteContext('https://pxi.test/api/recalculate', {
    method: 'POST',
    body: JSON.stringify({ date: '2026-03-04' }),
    headers: { 'Content-Type': 'application/json' },
  });

  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => null,
    currentNewYorkDate: () => '2026-03-05',
    calculatePXI: async () => {
      calculateCalls += 1;
      return null;
    },
  });
  assert.equal(response?.status, 400);
  const payload = await response!.json() as any;
  assert.equal(payload.current_date, '2026-03-05');
  assert.equal(payload.requested_date, '2026-03-04');
  assert.equal(calculateCalls, 0);
});

test('tryHandleAdminIngestionRoute permanently disables the unversioned /api/backfill route', async () => {
  let authCalls = 0;
  const route = createRouteContext('https://pxi.test/api/backfill', {
    method: 'POST',
    body: JSON.stringify({
      start: '2026-03-01',
      end: '2026-03-02',
      limit: 2,
      expected_build_sha: 'a1b2c3d4e5f6',
    }),
    headers: { 'Content-Type': 'application/json' },
  });

  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => {
      authCalls += 1;
      return null;
    },
  });
  assert.equal(response?.status, 410);
  assert.equal(authCalls, 0);
});

test('tryHandleAdminIngestionRoute validates versioned reconstruction date ranges', async () => {
  const route = createRouteContext('https://pxi.test/api/history/reconstruct-missing-v1', {
    method: 'POST',
    body: JSON.stringify({ start: 'bad', end: 'bad' }),
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => null,
    parseJsonBody: async () => ({ start: 'bad', end: 'bad' }),
    parseBackfillDateRange: () => null,
  });
  assert.equal(response?.status, 400);
});

test('tryHandleAdminIngestionRoute reconstructs only missing scores with explicit nonclaims', async () => {
  const batches: Array<Array<{ sql: string; args: unknown[] }>> = [];
  const calculateCalls: Array<{ date: string; options: unknown }> = [];
  const route = createRouteContext('https://pxi.test/api/history/reconstruct-missing-v1', {
    method: 'POST',
    body: JSON.stringify({
      start: '2026-03-01',
      end: '2026-03-03',
      limit: 3,
      expected_build_sha: 'a1b2c3d4e5f6',
      missing_only: true,
      overwrite: false,
      record_evidence: false,
      refresh_products: false,
      include_decision_impact: false,
      include_decision_grade: false,
      rebuild_ledgers: false,
      generate_embeddings: false,
      recalibrate: false,
    }),
    headers: {
      'Content-Type': 'application/json',
    },
  }, {
    BUILD_SHA: 'a1b2c3d4e5f6',
    DB: {
      ...createFakeDb((sql) => {
        if (sql.includes('MAX(fetched_at) AS source_data_as_of')) {
          return { source_data_as_of: '2026-08-22T17:55:00.000Z' };
        }
        if (sql.includes("SELECT date, 'live_pxi' AS storage_kind")) {
          return [
            {
              date: '2026-03-01',
              storage_kind: 'live_pxi',
              history_origin: 'live_recorded',
              reconstructed_at: null,
              reconstruction_method: null,
              reconstruction_build_sha: null,
              source_data_as_of: null,
              category: null,
            },
            ...CANONICAL_PXI_CATEGORIES.map((category) => ({
              date: '2026-03-01',
              storage_kind: 'live_category',
              history_origin: 'live_recorded',
              reconstructed_at: null,
              reconstruction_method: null,
              reconstruction_build_sha: null,
              source_data_as_of: null,
              category,
            })),
            {
              date: '2026-03-02',
              storage_kind: 'reconstruction_pxi',
              history_origin: 'retrospective_reconstruction',
              reconstructed_at: '2026-08-21T18:00:00.000Z',
              reconstruction_method: 'current_indicator_store_percentile_v1',
              reconstruction_build_sha: '112233445566',
              source_data_as_of: '2026-08-21T17:55:00.000Z',
              category: null,
            },
            ...CANONICAL_PXI_CATEGORIES.map((category) => ({
              date: '2026-03-02',
              storage_kind: 'reconstruction_category',
              history_origin: 'retrospective_reconstruction',
              reconstructed_at: '2026-08-21T18:00:00.000Z',
              reconstruction_method: 'current_indicator_store_percentile_v1',
              reconstruction_build_sha: '112233445566',
              source_data_as_of: '2026-08-21T17:55:00.000Z',
              category,
            })),
          ];
        }
        throw new Error(`Unexpected database query: ${sql}`);
      }),
      async batch(statements: Array<{ sql: string; args: unknown[] }>) {
        batches.push(statements);
        return { success: true };
      },
    },
    AI: {
      run: async () => {
        throw new Error('backfill must not invoke Workers AI');
      },
    },
    VECTORIZE: {
      upsert: async () => {
        throw new Error('backfill must not write Vectorize');
      },
    },
  });

  const requestBody = {
    start: '2026-03-01',
    end: '2026-03-03',
    limit: 3,
    expected_build_sha: 'a1b2c3d4e5f6',
    missing_only: true,
    overwrite: false,
    record_evidence: false,
    refresh_products: false,
    include_decision_impact: false,
    include_decision_grade: false,
    rebuild_ledgers: false,
    generate_embeddings: false,
    recalibrate: false,
  };
  const forbiddenDependency = async () => {
    throw new Error('backfill called a forbidden evidence/product/refresh dependency');
  };
  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => null,
    parseJsonBody: async () => requestBody,
    parseBackfillDateRange: () => ({ start: '2026-03-01', end: '2026-03-03' }),
    currentNewYorkDate: () => '2026-03-05',
    formatDate: (value: Date) => value.toISOString().slice(0, 10),
    PXI_SCORE_CATEGORIES: CANONICAL_PXI_CATEGORIES,
    calculatePXI: async (_db: unknown, date: string, options: unknown) => {
      calculateCalls.push({ date, options });
      return {
        pxi: {
          date,
          score: 70,
          label: 'risk-on',
          status: 'bullish',
          delta_1d: 1,
          delta_7d: 2,
          delta_30d: 4,
        },
        categories: CANONICAL_PXI_CATEGORIES.map((category) => ({
          category,
          date,
          score: 72,
          weight: 1 / CANONICAL_PXI_CATEGORIES.length,
          weighted_score: 72 / CANONICAL_PXI_CATEGORIES.length,
        })),
      };
    },
    recordMarketRefreshRunStart: forbiddenDependency,
    recordMarketRefreshRunFinish: forbiddenDependency,
    ensureMarketProductSchema: forbiddenDependency,
    generateEmbeddingText: forbiddenDependency,
    getEmbeddingVector: forbiddenDependency,
  });

  assert.equal(response?.status, 200);
  const payload = await response!.json() as any;
  assert.equal(payload.success, true);
  assert.equal(payload.history_origin, 'retrospective_reconstruction');
  assert.equal(payload.missing_only, true);
  assert.equal(payload.point_in_time_guarantee, false);
  assert.equal(payload.research_evidence_captured, false);
  assert.equal(payload.market_products_refreshed, false);
  assert.equal(payload.decision_impact_refreshed, false);
  assert.equal(payload.embeddings_generated, 0);
  assert.equal(payload.replaced, 0);
  assert.equal(payload.succeeded, 1);
  assert.equal(payload.skipped_existing, 2);
  assert.equal(payload.failed, 0);
  assert.equal(payload.stopped_early, false);
  assert.equal(payload.unprocessed, 0);
  assert.deepEqual(payload.results, [
    { date: '2026-03-01', status: 'skipped_existing' },
    { date: '2026-03-02', status: 'skipped_existing' },
    { date: '2026-03-03', status: 'inserted' },
  ]);
  assert.deepEqual(calculateCalls, [{
    date: '2026-03-03',
    options: { includeRetrospectiveHistory: true },
  }]);
  assert.equal(batches.length, 1);
  assert.equal(batches[0].length, 8);
  assert.ok(batches[0].slice(0, 7).every((statement) =>
    /INSERT INTO category_score_reconstructions/.test(statement.sql)));
  assert.ok(batches[0].slice(0, 7).every((statement) => !/OR REPLACE/.test(statement.sql)));
  assert.match(batches[0][7].sql, /INSERT INTO pxi_score_reconstructions/);
  assert.doesNotMatch(batches[0][7].sql, /OR REPLACE/);
  assert.ok(batches[0].every((statement) => statement.args.includes('retrospective_reconstruction')));
  assert.ok(batches[0].every((statement) => statement.args.includes('a1b2c3d4e5f6')));
});

test('tryHandleAdminIngestionRoute fail-stops on partial or mixed existing history', async (t) => {
  const provenance = {
    reconstructed_at: null,
    reconstruction_method: null,
    reconstruction_build_sha: null,
    source_data_as_of: null,
  };
  const scenarios = [
    {
      name: 'category-only live state',
      rows: [{
        date: '2026-03-01',
        storage_kind: 'live_category',
        history_origin: 'live_recorded',
        category: 'macro',
        ...provenance,
      }],
      expectedCategories: ['macro'],
    },
    {
      name: 'mixed live/reconstruction state',
      rows: [
        {
          date: '2026-03-01',
          storage_kind: 'live_pxi',
          history_origin: 'live_recorded',
          category: null,
          ...provenance,
        },
        {
          date: '2026-03-01',
          storage_kind: 'reconstruction_category',
          history_origin: 'retrospective_reconstruction',
          reconstructed_at: '2026-08-22T18:00:00.000Z',
          reconstruction_method: 'current_indicator_store_percentile_v1',
          reconstruction_build_sha: 'a1b2c3d4e5f6',
          source_data_as_of: '2026-08-22T17:55:00.000Z',
          category: 'macro',
        },
      ],
      expectedCategories: ['macro'],
    },
    {
      name: 'live category subset',
      rows: [
        {
          date: '2026-03-01',
          storage_kind: 'live_pxi',
          history_origin: 'live_recorded',
          category: null,
          ...provenance,
        },
        {
          date: '2026-03-01',
          storage_kind: 'live_category',
          history_origin: 'live_recorded',
          category: 'macro',
          ...provenance,
        },
      ],
      expectedCategories: ['macro', 'credit'],
    },
    {
      name: 'reconstructed category subset',
      rows: [
        {
          date: '2026-03-01',
          storage_kind: 'reconstruction_pxi',
          history_origin: 'retrospective_reconstruction',
          reconstructed_at: '2026-08-22T18:00:00.000Z',
          reconstruction_method: 'current_indicator_store_percentile_v1',
          reconstruction_build_sha: 'a1b2c3d4e5f6',
          source_data_as_of: '2026-08-22T17:55:00.000Z',
          category: null,
        },
        {
          date: '2026-03-01',
          storage_kind: 'reconstruction_category',
          history_origin: 'retrospective_reconstruction',
          reconstructed_at: '2026-08-22T18:00:00.000Z',
          reconstruction_method: 'current_indicator_store_percentile_v1',
          reconstruction_build_sha: 'a1b2c3d4e5f6',
          source_data_as_of: '2026-08-22T17:55:00.000Z',
          category: 'macro',
        },
      ],
      expectedCategories: ['macro', 'credit'],
    },
  ];

  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      let calculateCalls = 0;
      let batchCalls = 0;
      const body = {
        start: '2026-03-01',
        end: '2026-03-02',
        limit: 2,
        expected_build_sha: 'a1b2c3d4e5f6',
      };
      const route = createRouteContext('https://pxi.test/api/history/reconstruct-missing-v1', {
        method: 'POST',
        body: JSON.stringify(body),
        headers: { 'Content-Type': 'application/json' },
      }, {
        BUILD_SHA: 'a1b2c3d4e5f6',
        DB: {
          ...createFakeDb((sql) => {
            if (sql.includes('MAX(fetched_at) AS source_data_as_of')) {
              return { source_data_as_of: '2026-08-22T17:55:00.000Z' };
            }
            if (sql.includes("SELECT date, 'live_pxi' AS storage_kind")) {
              return scenario.rows;
            }
            throw new Error(`Unexpected database query: ${sql}`);
          }),
          async batch() {
            batchCalls += 1;
            return { success: true };
          },
        },
      });

      const response = await tryHandleAdminIngestionRoute(route as any, {
        enforceAdminAuth: async () => null,
        parseJsonBody: async () => body,
        parseBackfillDateRange: () => ({ start: body.start, end: body.end }),
        currentNewYorkDate: () => '2026-03-05',
        formatDate: (value: Date) => value.toISOString().slice(0, 10),
        PXI_SCORE_CATEGORIES: scenario.expectedCategories,
        calculatePXI: async () => {
          calculateCalls += 1;
          return null;
        },
      });

      assert.equal(response?.status, 200);
      const payload = await response!.json() as any;
      assert.equal(payload.success, false);
      assert.equal(payload.failed, 1);
      assert.equal(payload.stopped_early, true);
      assert.equal(payload.unprocessed, 1);
      assert.equal(payload.results.length, 1);
      assert.equal(payload.results[0].status, 'conflict');
      assert.match(payload.results[0].error, /partial or mixed/);
      assert.equal(calculateCalls, 0);
      assert.equal(batchCalls, 0);
    });
  }
});

test('tryHandleAdminIngestionRoute stops before later dates after calculate or batch failure', async (t) => {
  const runScenario = async (failure: 'calculate' | 'batch' | 'calculated_subset') => {
    const calculateDates: string[] = [];
    let batchCalls = 0;
    const body = {
      start: '2026-03-01',
      end: '2026-03-03',
      limit: 3,
      expected_build_sha: 'a1b2c3d4e5f6',
    };
    const route = createRouteContext('https://pxi.test/api/history/reconstruct-missing-v1', {
      method: 'POST',
      body: JSON.stringify(body),
      headers: { 'Content-Type': 'application/json' },
    }, {
      BUILD_SHA: 'a1b2c3d4e5f6',
      DB: {
        ...createFakeDb((sql) => {
          if (sql.includes('MAX(fetched_at) AS source_data_as_of')) {
            return { source_data_as_of: '2026-08-22T17:55:00.000Z' };
          }
          if (sql.includes("SELECT date, 'live_pxi' AS storage_kind")) return [];
          throw new Error(`Unexpected database query: ${sql}`);
        }),
        async batch() {
          batchCalls += 1;
          if (failure === 'batch') throw new Error('simulated batch failure');
          return { success: true };
        },
      },
    });

    const response = await tryHandleAdminIngestionRoute(route as any, {
      enforceAdminAuth: async () => null,
      parseJsonBody: async () => body,
      parseBackfillDateRange: () => ({ start: body.start, end: body.end }),
      currentNewYorkDate: () => '2026-03-05',
      formatDate: (value: Date) => value.toISOString().slice(0, 10),
      PXI_SCORE_CATEGORIES: failure === 'calculated_subset' ? ['macro', 'credit'] : ['macro'],
      calculatePXI: async (_db: unknown, date: string) => {
        calculateDates.push(date);
        if (failure === 'calculate' && date === '2026-03-02') return null;
        return {
          pxi: {
            date,
            score: 70,
            label: 'risk-on',
            status: 'bullish',
            delta_1d: 1,
            delta_7d: 2,
            delta_30d: 4,
          },
          categories: [{
            category: 'macro',
            date,
            score: 72,
            weight: 0.3,
            weighted_score: 21.6,
          }],
        };
      },
    });
    const payload = await response!.json() as any;
    return { payload, calculateDates, batchCalls };
  };

  await t.test('calculation failure', async () => {
    const { payload, calculateDates, batchCalls } = await runScenario('calculate');
    assert.deepEqual(calculateDates, ['2026-03-01', '2026-03-02']);
    assert.equal(batchCalls, 1);
    assert.deepEqual(payload.results.map((item: any) => item.status), ['inserted', 'failed']);
    assert.equal(payload.stopped_early, true);
    assert.equal(payload.unprocessed, 1);
  });

  await t.test('batch failure', async () => {
    const { payload, calculateDates, batchCalls } = await runScenario('batch');
    assert.deepEqual(calculateDates, ['2026-03-01']);
    assert.equal(batchCalls, 1);
    assert.deepEqual(payload.results.map((item: any) => item.status), ['failed']);
    assert.match(payload.results[0].error, /simulated batch failure/);
    assert.equal(payload.stopped_early, true);
    assert.equal(payload.unprocessed, 2);
  });

  await t.test('calculated category subset', async () => {
    const { payload, calculateDates, batchCalls } = await runScenario('calculated_subset');
    assert.deepEqual(calculateDates, ['2026-03-01']);
    assert.equal(batchCalls, 0);
    assert.deepEqual(payload.results.map((item: any) => item.status), ['failed']);
    assert.match(payload.results[0].error, /incomplete or mismatched/);
    assert.equal(payload.stopped_early, true);
    assert.equal(payload.unprocessed, 2);
  });
});

test('tryHandleAdminIngestionRoute uses the New York date for reconstruction cutoff', async () => {
  let databaseTouched = false;
  const body = { start: '2026-03-04', end: '2026-03-05', limit: 2 };
  const route = createRouteContext('https://pxi.test/api/history/reconstruct-missing-v1', {
    method: 'POST',
    body: JSON.stringify(body),
    headers: { 'Content-Type': 'application/json' },
  }, {
    BUILD_SHA: 'a1b2c3d4e5f6',
    DB: createFakeDb(() => {
      databaseTouched = true;
      return null;
    }),
  });

  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => null,
    parseJsonBody: async () => body,
    parseBackfillDateRange: () => ({ start: body.start, end: body.end }),
    currentNewYorkDate: () => '2026-03-05',
  });
  assert.equal(response?.status, 400);
  assert.equal(databaseTouched, false);
});

test('tryHandleAdminIngestionRoute rejects unsafe reconstruction options before writes', async () => {
  let databaseTouched = false;
  const route = createRouteContext('https://pxi.test/api/history/reconstruct-missing-v1', {
    method: 'POST',
    body: JSON.stringify({
      start: '2026-03-01',
      end: '2026-03-02',
      limit: 2,
      overwrite: true,
    }),
    headers: { 'Content-Type': 'application/json' },
  }, {
    BUILD_SHA: 'a1b2c3d4e5f6',
    DB: createFakeDb(() => {
      databaseTouched = true;
      return null;
    }),
  });

  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => null,
    parseJsonBody: async () => ({
      start: '2026-03-01',
      end: '2026-03-02',
      limit: 2,
      overwrite: true,
    }),
  });
  assert.equal(response?.status, 400);
  assert.equal(databaseTouched, false);
});

test('tryHandleAdminIngestionRoute rejects reconstruction without a deployed build SHA', async () => {
  const route = createRouteContext('https://pxi.test/api/history/reconstruct-missing-v1', {
    method: 'POST',
    body: JSON.stringify({ start: '2026-03-01', end: '2026-03-02', limit: 2 }),
    headers: { 'Content-Type': 'application/json' },
  });

  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => null,
    parseJsonBody: async () => ({
      start: '2026-03-01',
      end: '2026-03-02',
      limit: 2,
      expected_build_sha: 'a1b2c3d4e5f6',
    }),
    parseBackfillDateRange: () => ({ start: '2026-03-01', end: '2026-03-02' }),
    currentNewYorkDate: () => '2026-03-05',
    formatDate: (value: Date) => value.toISOString().slice(0, 10),
  });
  assert.equal(response?.status, 503);
});

test('tryHandleAdminIngestionRoute rejects a build SHA mismatch before D1 access', async () => {
  let databaseTouched = false;
  const body = {
    start: '2026-03-01',
    end: '2026-03-02',
    limit: 2,
    expected_build_sha: 'ffeeddccbbaa',
  };
  const route = createRouteContext('https://pxi.test/api/history/reconstruct-missing-v1', {
    method: 'POST',
    body: JSON.stringify(body),
    headers: { 'Content-Type': 'application/json' },
  }, {
    BUILD_SHA: 'a1b2c3d4e5f6',
    DB: createFakeDb(() => {
      databaseTouched = true;
      return null;
    }),
  });

  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => null,
    parseJsonBody: async () => body,
    parseBackfillDateRange: () => ({ start: body.start, end: body.end }),
    currentNewYorkDate: () => '2026-03-05',
  });
  assert.equal(response?.status, 409);
  assert.equal(databaseTouched, false);
});

test('tryHandleAdminIngestionRoute caps each reconstruction request at three days', async () => {
  let databaseTouched = false;
  const body = {
    start: '2026-03-01',
    end: '2026-03-04',
    limit: 4,
    expected_build_sha: 'a1b2c3d4e5f6',
  };
  const route = createRouteContext('https://pxi.test/api/history/reconstruct-missing-v1', {
    method: 'POST',
    body: JSON.stringify(body),
    headers: { 'Content-Type': 'application/json' },
  }, {
    BUILD_SHA: 'a1b2c3d4e5f6',
    DB: createFakeDb(() => {
      databaseTouched = true;
      return null;
    }),
  });

  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => null,
    parseJsonBody: async () => body,
    parseBackfillDateRange: () => ({ start: body.start, end: body.end }),
  });
  assert.equal(response?.status, 400);
  assert.equal(databaseTouched, false);
});

test('tryHandleAdminIngestionRoute serves /api/recalculate-all-signals summaries', async () => {
  const batched: unknown[][] = [];
  const route = createRouteContext('https://pxi.test/api/recalculate-all-signals', { method: 'POST' }, {
    DB: {
      ...createFakeDb((sql) => {
        if (sql.includes('SELECT date, score, delta_7d, delta_30d FROM pxi_scores')) {
          return [{ date: '2026-03-05', score: 72, delta_7d: 2, delta_30d: 4 }];
        }
        if (sql.includes('SELECT date, category, score FROM category_scores')) {
          return [{ date: '2026-03-05', category: 'macro', score: 70 }];
        }
        if (sql.includes("WHERE indicator_id = 'vix'")) {
          return [{ date: '2026-03-05', value: 18 }];
        }
        throw new Error(`Unhandled query: ${sql}`);
      }),
      async batch(statements: unknown[]) {
        batched.push(statements);
        return { success: true };
      },
    },
  });

  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => null,
  });
  const payload = await response!.json() as Record<string, unknown>;
  assert.equal(payload.success, true);
  assert.equal(payload.processed, 1);
  assert.equal(batched.length, 1);
});
