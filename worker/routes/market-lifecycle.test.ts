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

function addCalendarDays(date: string, days: number): string {
  const value = new Date(`${date}T00:00:00.000Z`);
  value.setUTCDate(value.getUTCDate() + days);
  return value.toISOString().slice(0, 10);
}

function createOpportunityItem(id: string) {
  return {
    id,
    symbol: 'SPY',
    theme_id: 'theme-1',
    theme_name: 'Theme 1',
    direction: 'bullish',
    conviction_score: 72,
    rationale: 'Test rationale',
    supporting_factors: ['factor-1'],
    historical_hit_rate: 0.41,
    sample_size: 12,
    calibration: {
      probability_correct_direction: 0.62,
      ci95_low: 0.51,
      ci95_high: 0.73,
      sample_size: 12,
      quality: 'ROBUST',
      basis: 'conviction_decile',
      window: '7d',
      unavailable_reason: null,
    },
    expectancy: {
      expected_move_pct: 3.2,
      max_adverse_move_pct: -1.4,
      sample_size: 12,
      basis: 'theme_direction',
      quality: 'ROBUST',
      unavailable_reason: null,
    },
    eligibility: {
      passed: true,
      failed_checks: [],
    },
    decision_contract: {
      coherent: true,
      confidence_band: 'medium',
      rationale_codes: ['opportunity_test'],
    },
    updated_at: '2026-03-05T00:00:00.000Z',
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
  assert.equal(payload.publication_status, 'skipped');
  assert.equal(payload.reason, 'refresh_in_progress');
  assert.equal(payload.refresh_run_id, 42);
});

test('tryHandleMarketLifecycleRoute returns a blocked payload when governance blocks live publication', async () => {
  const storeBriefCalls: unknown[] = [];
  const storeOpportunityCalls: unknown[] = [];
  const insertEventCalls: unknown[] = [];
  const finishCalls: unknown[] = [];
  const storeDecisionImpactCalls: unknown[] = [];
  const itemLedgerInsertCalls: unknown[] = [];

  const route = createRouteContext('https://pxi.test/api/market/refresh-products', { method: 'POST' }, {
    DB: createFakeDb((sql, args) => {
      if (sql.includes('FROM market_opportunity_item_ledger')) {
        const [horizon] = args as [string];
        if (horizon === '7d') {
          return [{ as_of: '2026-02-20T00:00:00.000Z', theme_id: 'theme-1', theme_name: 'Theme 1', direction: 'bullish' }];
        }
        if (horizon === '30d') {
          return [{ as_of: '2026-01-10T00:00:00.000Z', theme_id: 'theme-1', theme_name: 'Theme 1', direction: 'bullish' }];
        }
        return [];
      }
      if (sql.includes('FROM indicator_values')) {
        return [
          { indicator_id: 'spy_close', date: '2026-01-10', value: 100 },
          { indicator_id: 'spy_close', date: '2026-02-09', value: 90 },
          { indicator_id: 'spy_close', date: '2026-02-20', value: 100 },
          { indicator_id: 'spy_close', date: '2026-02-27', value: 90 },
          { indicator_id: 'spy_close', date: '2026-03-05', value: 95 },
        ];
      }
      return [];
    }),
  });

  const response = await tryHandleMarketLifecycleRoute(route as any, {
    enforceAdminAuth: async () => null,
    ensureMarketProductSchema: async () => undefined,
    claimMarketRefreshRun: async () => ({
      status: 'claimed',
      run_id: 77,
      refresh_trigger: 'github_actions',
    }),
    resolveDecisionImpactGovernance: () => ({
      enforce_enabled: true,
      min_sample_size: 1,
      min_actionable_sessions: 1,
    }),
    isFeatureEnabled: (_env: unknown, flagName: string) => ![
      'FEATURE_ENABLE_CALIBRATION_DIAGNOSTICS',
      'FEATURE_ENABLE_EDGE_DIAGNOSTICS',
      'FEATURE_ENABLE_IN_APP_ALERTS',
    ].includes(flagName),
    buildEdgeQualityCalibrationSnapshot: async () => ({
      as_of: '2026-03-05T00:00:00.000Z',
      metric: 'edge_quality',
      horizon: null,
      basis: 'edge_quality_decile',
      bins: [],
      total_samples: 5,
    }),
    buildConvictionCalibrationSnapshot: async (_db: unknown, horizon: '7d' | '30d') => ({
      as_of: '2026-03-05T00:00:00.000Z',
      metric: 'conviction',
      horizon,
      basis: 'conviction_decile',
      bins: [],
      total_samples: 5,
    }),
    storeCalibrationSnapshot: async () => undefined,
    buildBriefSnapshot: async () => ({
      as_of: '2026-03-05T00:00:00.000Z',
      source_plan_as_of: '2026-03-05T00:00:00.000Z',
      consistency: {
        score: 92,
        state: 'PASS',
        violations: [],
        components: {},
      },
      freshness_status: {
        has_stale_data: false,
        stale_count: 0,
        critical_stale_count: 0,
      },
    }),
    storeConsistencyCheck: async () => undefined,
    buildOpportunitySnapshot: async (_db: unknown, horizon: '7d' | '30d') => ({
      as_of: '2026-03-05T00:00:00.000Z',
      horizon,
      items: [createOpportunityItem(`opportunity-${horizon}`)],
    }),
    computeFreshnessStatus: async () => ({
      has_stale_data: false,
      stale_count: 0,
      critical_stale_count: 0,
    }),
    fetchLatestConsistencyCheck: async () => null,
    buildOpportunityLedgerProjection: (args: Record<string, unknown>) => ({
      ledger_row: {
        refresh_run_id: args.refresh_run_id,
        as_of: '2026-03-05T00:00:00.000Z',
        horizon: (args.snapshot as { horizon: '7d' | '30d' }).horizon,
        candidate_count: 1,
        published_count: 0,
        suppressed_count: 1,
        quality_filtered_count: 0,
        coherence_suppressed_count: 0,
        data_quality_suppressed_count: 0,
        degraded_reason: 'governance_blocked',
        top_direction_candidate: 'bullish',
        top_direction_published: null,
      },
      item_rows: [{
        refresh_run_id: args.refresh_run_id,
        as_of: '2026-03-05T00:00:00.000Z',
        horizon: (args.snapshot as { horizon: '7d' | '30d' }).horizon,
        opportunity_id: `item-${(args.snapshot as { horizon: '7d' | '30d' }).horizon}`,
        theme_id: 'theme-1',
        theme_name: 'Theme 1',
        direction: 'bullish',
        conviction_score: 72,
        published: 0,
        suppression_reason: 'governance_blocked',
      }],
      projected: {
        total_candidates: 1,
        items: [],
        suppressed_count: 1,
        quality_filtered_count: 0,
        coherence_suppressed_count: 0,
        suppressed_data_quality: false,
        degraded_reason: 'governance_blocked',
        suppression_by_reason: {
          coherence_failed: 0,
          quality_filtered: 0,
          data_quality_suppressed: 0,
        },
      },
      normalized_items: [createOpportunityItem('normalized')],
    }),
    insertOpportunityLedgerRow: async () => undefined,
    insertOpportunityItemLedgerRow: async (_db: unknown, payload: unknown) => {
      itemLedgerInsertCalls.push(payload);
    },
    storeDecisionImpactSnapshot: async (_db: unknown, snapshot: unknown) => {
      storeDecisionImpactCalls.push(snapshot);
    },
    storeBriefSnapshot: async (_db: unknown, brief: unknown) => {
      storeBriefCalls.push(brief);
    },
    storeOpportunitySnapshot: async (_db: unknown, snapshot: unknown) => {
      storeOpportunityCalls.push(snapshot);
    },
    insertMarketEvents: async (_db: unknown, events: unknown) => {
      insertEventCalls.push(events);
      return 1;
    },
    computeUtilityFunnelSummary: async () => ({
      window_days: 30,
      days_observed: 30,
      total_events: 1,
      unique_sessions: 1,
      plan_views: 1,
      opportunities_views: 1,
      decision_actionable_views: 1,
      decision_watch_views: 0,
      decision_no_action_views: 0,
      no_action_unlock_views: 0,
      cta_action_clicks: 0,
      actionable_view_sessions: 1,
      actionable_sessions: 1,
      cta_action_rate_pct: 0,
      decision_events_total: 1,
      decision_events_per_session: 1,
      no_action_unlock_coverage_pct: 0,
      last_event_at: '2026-03-05T00:00:00.000Z',
    }),
    asIsoDateTime: () => '2026-03-05T14:00:00.000Z',
    asIsoDate: () => '2026-03-05',
    parseIsoDate: (value: string) => /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : null,
    addCalendarDays,
    latestDateInSeries: (series: Array<{ date: string }>) => series[series.length - 1]?.date ?? null,
    resolveThemeProxyRules: () => [],
    priceOnOrAfterSeries: (series: Map<string, number> | undefined, date: string) => series?.get(date) ?? null,
    buildDecisionImpactMarketStats: (observations: Array<{ signed_return_pct: number; forward_return_pct: number }>) => {
      const sampleSize = observations.length;
      const hitRate = sampleSize > 0
        ? observations.filter((item) => item.signed_return_pct > 0).length / sampleSize
        : 0;
      const avgSignedReturnPct = sampleSize > 0
        ? observations.reduce((sum, item) => sum + item.signed_return_pct, 0) / sampleSize
        : 0;
      const avgForwardReturnPct = sampleSize > 0
        ? observations.reduce((sum, item) => sum + item.forward_return_pct, 0) / sampleSize
        : 0;
      return {
        sample_size: sampleSize,
        hit_rate: Number(hitRate.toFixed(4)),
        avg_signed_return_pct: Number(avgSignedReturnPct.toFixed(4)),
        avg_forward_return_pct: Number(avgForwardReturnPct.toFixed(4)),
        quality_band: 'ROBUST',
      };
    },
    buildDecisionImpactThemeStats: () => [],
    roundMetric: (value: number) => Number(value.toFixed(4)),
    recordMarketRefreshRunFinish: async (_db: unknown, _runId: number, payload: unknown) => {
      finishCalls.push(payload);
    },
  });

  assert.equal(response?.status, 200);
  const payload = await response!.json() as Record<string, unknown>;
  assert.equal(payload.ok, true);
  assert.equal(payload.blocked, true);
  assert.equal(payload.publication_status, 'blocked');
  assert.equal(payload.reason, 'decision_impact_enforcement_failed');
  assert.deepEqual(payload.governance_breaches, [
    'market_7d_hit_rate_breach',
    'market_7d_avg_signed_return_breach',
    'market_30d_hit_rate_breach',
    'market_30d_avg_signed_return_breach',
    'cta_action_rate_breach',
  ]);
  assert.equal(payload.brief_generated, 0);
  assert.equal(payload.opportunities_generated, 0);
  assert.equal(payload.alerts_generated, 0);
  assert.equal(storeBriefCalls.length, 0);
  assert.equal(storeOpportunityCalls.length, 0);
  assert.equal(insertEventCalls.length, 0);
  assert.equal(storeDecisionImpactCalls.length, 8);
  assert.equal(itemLedgerInsertCalls.length, 2);
  assert.equal((itemLedgerInsertCalls[0] as { suppression_reason?: string }).suppression_reason, 'governance_blocked');
  assert.equal(finishCalls.length, 1);
  assert.equal((finishCalls[0] as { status?: string }).status, 'blocked');
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
