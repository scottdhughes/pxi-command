import assert from 'node:assert/strict';
import test from 'node:test';

import { tryHandleAdminIngestionRoute } from './admin-ingestion.js';
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

test('tryHandleSystemRoute serves /health', async () => {
  const route = createRouteContext('https://pxi.test/health', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('SELECT 1 as ok')) return { ok: 1 };
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
});

test('tryHandleSystemRoute serves safe defaults on /health when deploy metadata is absent', async () => {
  const route = createRouteContext('https://pxi.test/health', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('SELECT 1 as ok')) return { ok: 1 };
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
          { date: '2026-03-05', score: 70, label: 'risk-on', status: 'bullish' },
          { date: '2026-03-04', score: 55, label: 'neutral', status: 'neutral' },
        ];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandlePublicReadRoute(route as any, {});
  assert.ok(response);
  const payload = await response!.json() as { data: Array<{ date: string; regime: string }> };
  assert.equal(payload.data[0].date, '2026-03-04');
  assert.equal(payload.data[1].regime, 'RISK_ON');
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
    getEmbeddingVector: (embedding: any) => embedding.data[0],
  });
  const payload = await response!.json() as Record<string, unknown>;
  assert.equal(payload.embedded_dates, 1);
  assert.equal(upserted, 1);
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
    getEmbeddingVector: () => [0.1, 0.2],
  });
  const payload = await response!.json() as Record<string, unknown>;
  assert.equal(payload.success, true);
  assert.equal(payload.written, 1);
});

test('tryHandleAdminIngestionRoute validates /api/backfill date ranges', async () => {
  const route = createRouteContext('https://pxi.test/api/backfill', {
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

test('tryHandleAdminIngestionRoute records /api/backfill runs with the current helper contract', async () => {
  const finishCalls: Array<{ runId: unknown; payload: unknown }> = [];
  const route = createRouteContext('https://pxi.test/api/backfill', {
    method: 'POST',
    body: JSON.stringify({ start: '2026-03-01', end: '2026-03-02', limit: 5 }),
    headers: {
      'Content-Type': 'application/json',
    },
  }, {
    DB: createFakeDb((sql) => {
      if (sql.includes('SELECT indicator_id, value FROM indicator_values')) {
        return [];
      }
      return null;
    }),
  });

  const response = await tryHandleAdminIngestionRoute(route as any, {
    enforceAdminAuth: async () => null,
    parseJsonBody: async () => ({ start: '2026-03-01', end: '2026-03-02', limit: 5 }),
    parseBackfillDateRange: () => ({ start: '2026-03-01', end: '2026-03-02' }),
    parseBackfillLimit: () => 5,
    recordMarketRefreshRunStart: async () => 77,
    recordMarketRefreshRunFinish: async (_db: unknown, runId: unknown, payload: unknown) => {
      finishCalls.push({ runId, payload });
    },
    formatDate: (value: Date) => value.toISOString().slice(0, 10),
    calculatePXI: async (db: unknown, date: string) => ({
      pxi: {
        date,
        score: 70,
        label: 'risk-on',
        status: 'bullish',
        delta_1d: 1,
        delta_7d: 2,
        delta_30d: 4,
      },
      categories: [
        { category: 'macro', date, score: 72, weight: 0.3, weighted_score: 21.6 },
      ],
    }),
    generateEmbeddingText: () => 'embedding text',
    getEmbeddingVector: () => [0.1, 0.2],
    ensureMarketProductSchema: async () => undefined,
  });

  assert.equal(response?.status, 200);
  const payload = await response!.json() as any;
  assert.equal(payload.success, true);
  assert.equal(payload.run_id, 77);
  assert.equal(finishCalls.length, 1);
  assert.equal(finishCalls[0]?.runId, 77);
  assert.equal((finishCalls[0]?.payload as any).status, 'success');
  assert.equal((finishCalls[0]?.payload as any).as_of, '2026-03-02');
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
