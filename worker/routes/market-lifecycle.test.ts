import assert from 'node:assert/strict';
import test from 'node:test';

import { tryHandleMarketLifecycleRoute } from './market-lifecycle.js';

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

test('tryHandleMarketLifecycleRoute ignores CTA utility events when tracking is disabled', async () => {
  const route = createRouteContext('https://pxi.test/api/metrics/utility-event', {
    method: 'POST',
    body: JSON.stringify({
      session_id: 'session-1',
      event_type: 'cta_action_click',
      route: '/',
      actionability_state: 'ACTIONABLE',
    }),
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const response = await tryHandleMarketLifecycleRoute(route as any, {
    ensureMarketProductSchema: async () => undefined,
    parseJsonBody: async () => ({
      session_id: 'session-1',
      event_type: 'cta_action_click',
      route: '/',
      actionability_state: 'ACTIONABLE',
    }),
    normalizeUtilitySessionId: () => 'session-1',
    normalizeUtilityEventType: () => 'cta_action_click',
    normalizeUtilityRoute: () => '/',
    normalizeUtilityActionabilityState: () => 'ACTIONABLE',
    sanitizeUtilityPayload: () => null,
    asIsoDateTime: () => '2026-03-05T14:00:00.000Z',
    isFeatureEnabled: () => false,
  });

  assert.equal(response?.status, 200);
  const payload = await response!.json() as Record<string, unknown>;
  assert.equal(payload.ok, true);
  assert.equal(payload.stored, false);
  assert.equal(payload.ignored_reason, 'cta_intent_tracking_disabled');
});

test('tryHandleMarketLifecycleRoute serves /api/ops/utility-funnel', async () => {
  const route = createRouteContext('https://pxi.test/api/ops/utility-funnel?window=30');

  const response = await tryHandleMarketLifecycleRoute(route as any, {
    ensureMarketProductSchema: async () => undefined,
    computeUtilityFunnelSummary: async () => ({
      window_days: 30,
      days_observed: 12,
      total_events: 80,
      unique_sessions: 21,
      plan_views: 18,
      opportunities_views: 16,
      decision_actionable_views: 14,
      decision_watch_views: 9,
      decision_no_action_views: 7,
      no_action_unlock_views: 4,
      cta_action_clicks: 5,
      actionable_view_sessions: 11,
      actionable_sessions: 12,
      cta_action_rate_pct: 41.67,
      decision_events_total: 30,
      decision_events_per_session: 1.4286,
      no_action_unlock_coverage_pct: 57.14,
      last_event_at: '2026-03-05T13:58:00.000Z',
    }),
    asIsoDateTime: () => '2026-03-05T14:00:00.000Z',
  });

  assert.equal(response?.status, 200);
  const payload = await response!.json() as any;
  assert.equal(payload.funnel.window_days, 30);
  assert.equal(payload.funnel.cta_action_clicks, 5);
});

test('tryHandleMarketLifecycleRoute enforces auth on /api/market/refresh-products', async () => {
  const route = createRouteContext('https://pxi.test/api/market/refresh-products', { method: 'POST' });

  const response = await tryHandleMarketLifecycleRoute(route as any, {
    enforceAdminAuth: async () => Response.json({ error: 'Unauthorized' }, { status: 401 }),
  });

  assert.equal(response?.status, 401);
});

test('tryHandleMarketLifecycleRoute returns a skip payload when refresh-products is already in progress', async () => {
  const route = createRouteContext('https://pxi.test/api/market/refresh-products', { method: 'POST' });

  const response = await tryHandleMarketLifecycleRoute(route as any, {
    enforceAdminAuth: async () => null,
    ensureMarketProductSchema: async () => undefined,
    isFeatureEnabled: () => true,
    resolveDecisionImpactGovernance: () => ({
      enforce_enabled: false,
      min_sample_size: 30,
      min_actionable_sessions: 10,
    }),
    claimMarketRefreshRun: async () => ({
      status: 'skipped',
      run_id: 42,
      refresh_trigger: 'github_actions',
      reason: 'refresh_in_progress',
    }),
  });

  assert.equal(response?.status, 200);
  const payload = await response!.json() as Record<string, unknown>;
  assert.equal(payload.ok, true);
  assert.equal(payload.skipped, true);
  assert.equal(payload.reason, 'refresh_in_progress');
  assert.equal(payload.refresh_run_id, 42);
});

test('tryHandleMarketLifecycleRoute serves /api/market/backfill-products in dry-run mode', async () => {
  const route = createRouteContext('https://pxi.test/api/market/backfill-products', {
    method: 'POST',
    body: JSON.stringify({
      start: '2026-03-01',
      end: '2026-03-02',
      dry_run: true,
    }),
    headers: {
      'Content-Type': 'application/json',
    },
  }, {
    DB: createFakeDb((sql) => {
      if (sql.includes('FROM pxi_scores p')) {
        return [
          { date: '2026-03-02', category_count: 4 },
          { date: '2026-03-01', category_count: 4 },
        ];
      }
      if (sql.includes('FROM opportunity_snapshots')) {
        return [];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const snapshot = { as_of: '2026-03-01T00:00:00.000Z', horizon: '7d', items: [] };
  const response = await tryHandleMarketLifecycleRoute(route as any, {
    enforceAdminAuth: async () => null,
    ensureMarketProductSchema: async () => undefined,
    isFeatureEnabled: () => true,
    parseBackfillDateRange: () => ({ start: '2026-03-01', end: '2026-03-02' }),
    parseBackfillLimit: () => 10,
    asIsoDate: () => '2026-03-05',
    addCalendarDays: () => '2024-09-12',
    buildHistoricalOpportunitySnapshot: async (_db: unknown, date: string, horizon: '7d' | '30d') => ({
      ...snapshot,
      as_of: `${date}T00:00:00.000Z`,
      horizon,
    }),
  });

  assert.equal(response?.status, 200);
  const payload = await response!.json() as any;
  assert.equal(payload.dry_run, true);
  assert.equal(payload.scanned_dates, 2);
  assert.equal(payload.seeded_snapshots, 4);
});

test('tryHandleMarketLifecycleRoute skips /api/market/send-digest when email alerts are disabled', async () => {
  const route = createRouteContext('https://pxi.test/api/market/send-digest', { method: 'POST' });

  const response = await tryHandleMarketLifecycleRoute(route as any, {
    enforceAdminAuth: async () => null,
    ensureMarketProductSchema: async () => undefined,
    isFeatureEnabled: () => false,
  });

  assert.equal(response?.status, 200);
  const payload = await response!.json() as any;
  assert.equal(payload.ok, true);
  assert.equal(payload.skipped, true);
});

test('tryHandleMarketLifecycleRoute does not send duplicate digests when sent or queued rows already exist', async () => {
  let sendCalls = 0;
  const route = createRouteContext('https://pxi.test/api/market/send-digest', { method: 'POST' }, {
    DB: createFakeDb((sql) => {
      if (sql.includes('FROM market_brief_snapshots')) {
        return [{ payload_json: JSON.stringify({ as_of: '2026-03-05T00:00:00.000Z', summary: 'Brief' }) }];
      }
      if (sql.includes("FROM opportunity_snapshots WHERE horizon = '7d'")) {
        return [{ payload_json: JSON.stringify({ as_of: '2026-03-05T00:00:00.000Z', horizon: '7d', items: [] }) }];
      }
      if (sql.includes('FROM market_alert_events')) {
        return [];
      }
      if (sql.includes('FROM email_subscribers')) {
        return [
          { id: 'sub-sent', email: 'sent@example.com', types_json: '[]', cadence: 'daily_8am_et', status: 'active' },
          { id: 'sub-queued', email: 'queued@example.com', types_json: '[]', cadence: 'daily_8am_et', status: 'active' },
        ];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandleMarketLifecycleRoute(route as any, {
    enforceAdminAuth: async () => null,
    ensureMarketProductSchema: async () => undefined,
    isFeatureEnabled: () => true,
    asIsoDate: () => '2026-03-05',
    canSendEmail: () => true,
    getAlertsSigningSecret: () => 'secret',
    normalizeAlertTypes: () => ['regime_change'],
    reserveDigestDeliveryForSubscriber: async (_db: unknown, _eventId: string, subscriberId: string) => (
      subscriberId === 'sub-sent'
        ? { action: 'skip', status: 'sent' }
        : { action: 'skip', status: 'queued' }
    ),
    sendDigestToSubscriber: async () => {
      sendCalls += 1;
      return { ok: true, providerId: 'provider' };
    },
  });

  assert.equal(response?.status, 200);
  const payload = await response!.json() as any;
  assert.equal(payload.sent_count, 0);
  assert.equal(payload.fail_count, 0);
  assert.equal(payload.active_subscribers, 2);
  assert.equal(sendCalls, 0);
});

test('tryHandleMarketLifecycleRoute retries failed digest deliveries and only counts a recovered subscriber once', async () => {
  let attempts = 0;
  const finalized: Array<{ kind: 'sent' | 'failed'; providerId?: string | null; error?: string }> = [];
  const route = createRouteContext('https://pxi.test/api/market/send-digest', { method: 'POST' }, {
    DB: createFakeDb((sql) => {
      if (sql.includes('FROM market_brief_snapshots')) {
        return [{ payload_json: JSON.stringify({ as_of: '2026-03-05T00:00:00.000Z', summary: 'Brief' }) }];
      }
      if (sql.includes("FROM opportunity_snapshots WHERE horizon = '7d'")) {
        return [{ payload_json: JSON.stringify({ as_of: '2026-03-05T00:00:00.000Z', horizon: '7d', items: [] }) }];
      }
      if (sql.includes('FROM market_alert_events')) {
        return [];
      }
      if (sql.includes('FROM email_subscribers')) {
        return [
          { id: 'sub-retry', email: 'retry@example.com', types_json: '[]', cadence: 'daily_8am_et', status: 'active' },
        ];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandleMarketLifecycleRoute(route as any, {
    enforceAdminAuth: async () => null,
    ensureMarketProductSchema: async () => undefined,
    isFeatureEnabled: () => true,
    asIsoDate: () => '2026-03-05',
    canSendEmail: () => true,
    getAlertsSigningSecret: () => 'secret',
    normalizeAlertTypes: () => ['regime_change'],
    reserveDigestDeliveryForSubscriber: async () => ({ action: 'send' }),
    generateToken: () => 'token',
    hashToken: async () => 'hashed-token',
    sendDigestToSubscriber: async () => {
      attempts += 1;
      if (attempts < 3) {
        return { ok: false, error: 'smtp timeout' };
      }
      return { ok: true, providerId: 'provider-1' };
    },
    finalizeDigestDeliverySent: async (_db: unknown, _eventId: string, _subscriberId: string, providerId: string | null) => {
      finalized.push({ kind: 'sent', providerId });
    },
    finalizeDigestDeliveryFailure: async (_db: unknown, _eventId: string, _subscriberId: string, error: string) => {
      finalized.push({ kind: 'failed', error });
    },
  });

  assert.equal(response?.status, 200);
  const payload = await response!.json() as any;
  assert.equal(payload.sent_count, 1);
  assert.equal(payload.fail_count, 0);
  assert.equal(payload.active_subscribers, 1);
  assert.equal(attempts, 3);
  assert.deepEqual(finalized, [{ kind: 'sent', providerId: 'provider-1' }]);
});
