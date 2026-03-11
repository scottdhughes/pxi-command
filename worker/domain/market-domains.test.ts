import assert from 'node:assert/strict';
import test from 'node:test';

import { buildCanonicalMarketDecision, tryHandleMarketCoreRoute } from './market-core.js';
import { generateMarketEvents, tryHandleMarketProductsRoute } from './market-products.js';
import { computeDecisionGradeScorecard } from './market-ops.js';
import type { WorkerRouteContext } from '../types.js';

const DAY_MS = 24 * 60 * 60 * 1000;

type QueryResult =
  | null
  | undefined
  | Record<string, unknown>
  | { results: Record<string, unknown>[] }
  | Record<string, unknown>[];

function addDays(date: Date, days: number): Date {
  return new Date(date.getTime() + (days * DAY_MS));
}

function toIsoDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function toIsoDateTime(date: Date): string {
  return `${toIsoDate(date)}T00:00:00.000Z`;
}

function createFakeDb(handler: (sql: string, args: unknown[]) => QueryResult): D1Database {
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
  });

  return {
    prepare(sql: string) {
      return buildStatement(sql);
    },
  } as D1Database;
}

function createRouteContext(
  url: string,
  init?: RequestInit,
  envOverrides: Record<string, unknown> = {},
): WorkerRouteContext {
  const request = new Request(url, init);
  return {
    request,
    env: {
      DB: createFakeDb(() => null),
      ...envOverrides,
    } as WorkerRouteContext['env'],
    url: new URL(request.url),
    method: request.method,
    corsHeaders: {},
    clientIP: '127.0.0.1',
  };
}

function createDecisionImpactHelpers(overrides: Record<string, unknown> = {}) {
  return {
    clamp(min: number, max: number, value: number) {
      return Math.min(max, Math.max(min, value));
    },
    toNumber(value: unknown, fallback = 0) {
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : fallback;
    },
    asIsoDate(date: Date) {
      return toIsoDate(date);
    },
    asIsoDateTime(date: Date) {
      return date.toISOString();
    },
    parseIsoDate(value: string) {
      return /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : null;
    },
    addCalendarDays(date: string, days: number) {
      return toIsoDate(addDays(new Date(`${date}T00:00:00.000Z`), days));
    },
    latestDateInSeries(series: Array<{ date: string }>) {
      return series.length > 0 ? series[series.length - 1].date : null;
    },
    priceOnOrAfterSeries(series: Map<string, number> | undefined, startDate: string, maxLag: number) {
      if (!series) return null;
      for (let offset = 0; offset <= maxLag; offset += 1) {
        const candidate = toIsoDate(addDays(new Date(`${startDate}T00:00:00.000Z`), offset));
        const price = series.get(candidate);
        if (typeof price === 'number') return price;
      }
      return null;
    },
    resolveThemeProxyRules(themeId: string) {
      if (themeId !== 'ai') return [];
      return [{ indicator_id: 'qqq_close', weight: 1, invert: false }];
    },
    buildDecisionImpactMarketStats(observations: Array<{ signed_return_pct: number }>) {
      const sampleSize = observations.length;
      const hitCount = observations.filter((observation) => observation.signed_return_pct > 0).length;
      const avgSignedReturnPct = sampleSize > 0
        ? observations.reduce((sum, observation) => sum + observation.signed_return_pct, 0) / sampleSize
        : 0;
      return {
        sample_size: sampleSize,
        hit_rate: sampleSize > 0 ? Number((hitCount / sampleSize).toFixed(4)) : 0,
        avg_signed_return_pct: Number(avgSignedReturnPct.toFixed(4)),
      };
    },
    buildDecisionImpactThemeStats(
      observations: Array<{ theme_id: string; theme_name: string; signed_return_pct: number }>,
      limit: number,
    ) {
      const grouped = new Map<string, { theme_id: string; theme_name: string; sample_size: number; total: number }>();
      for (const observation of observations) {
        const current = grouped.get(observation.theme_id) || {
          theme_id: observation.theme_id,
          theme_name: observation.theme_name,
          sample_size: 0,
          total: 0,
        };
        current.sample_size += 1;
        current.total += observation.signed_return_pct;
        grouped.set(observation.theme_id, current);
      }
      return [...grouped.values()]
        .map((entry) => ({
          theme_id: entry.theme_id,
          theme_name: entry.theme_name,
          sample_size: entry.sample_size,
          avg_signed_return_pct: Number((entry.total / entry.sample_size).toFixed(4)),
          quality_band: entry.sample_size >= 5 ? 'ROBUST' : 'LIMITED',
        }))
        .slice(0, limit);
    },
    roundMetric(value: number) {
      return Number(value.toFixed(4));
    },
    ...overrides,
  };
}

function buildOpsFixture({
  positiveReturns,
  publishedCount,
  suppressedCount,
  topDirection30d,
}: {
  positiveReturns: boolean;
  publishedCount: number;
  suppressedCount: number;
  topDirection30d: 'bullish' | 'bearish';
}) {
  const baseAsOf = addDays(new Date(), -35);
  const asOf = toIsoDate(baseAsOf);
  const maturity7d = toIsoDate(addDays(baseAsOf, 7));
  const maturity30d = toIsoDate(addDays(baseAsOf, 30));

  const spy7dForward = positiveReturns ? 104 : 96;
  const spy30dForward = positiveReturns ? 110 : 90;
  const qqq7dForward = positiveReturns ? 210 : 190;

  return {
    opportunityLedgerRows: [
      {
        as_of: toIsoDateTime(baseAsOf),
        horizon: '7d',
        candidate_count: 10,
        published_count: publishedCount,
        suppressed_count: suppressedCount,
        quality_filtered_count: positiveReturns ? 1 : 4,
        coherence_suppressed_count: positiveReturns ? 0 : 3,
        data_quality_suppressed_count: 0,
        degraded_reason: positiveReturns ? null : 'refresh_ttl_overdue',
        top_direction_candidate: 'bullish',
        top_direction_published: 'bullish',
        created_at: `${asOf}T06:00:00.000Z`,
      },
      {
        as_of: toIsoDateTime(baseAsOf),
        horizon: '30d',
        candidate_count: 10,
        published_count: publishedCount,
        suppressed_count: suppressedCount,
        quality_filtered_count: positiveReturns ? 1 : 4,
        coherence_suppressed_count: positiveReturns ? 0 : 3,
        data_quality_suppressed_count: 0,
        degraded_reason: positiveReturns ? null : 'refresh_ttl_overdue',
        top_direction_candidate: 'bullish',
        top_direction_published: topDirection30d,
        created_at: `${asOf}T06:00:00.000Z`,
      },
    ],
    itemLedgerRowsByHorizon: {
      '7d': [{ as_of: toIsoDateTime(baseAsOf), theme_id: 'ai', theme_name: 'AI', direction: 'bullish' }],
      '30d': [{ as_of: toIsoDateTime(baseAsOf), theme_id: 'ai', theme_name: 'AI', direction: 'bullish' }],
    },
    indicatorRows: [
      { indicator_id: 'spy_close', date: asOf, value: 100 },
      { indicator_id: 'spy_close', date: maturity7d, value: spy7dForward },
      { indicator_id: 'spy_close', date: maturity30d, value: spy30dForward },
      { indicator_id: 'qqq_close', date: asOf, value: 200 },
      { indicator_id: 'qqq_close', date: maturity7d, value: qqq7dForward },
      { indicator_id: 'qqq_close', date: maturity30d, value: positiveReturns ? 220 : 180 },
    ],
  };
}

test('buildCanonicalMarketDecision assembles the extracted decision payload', async () => {
  const decision = await buildCanonicalMarketDecision({} as D1Database, {
    detectRegime: async () => ({ regime: 'RISK_ON', confidence: 78, description: 'Risk appetite is improving.' }),
    computeFreshnessStatus: async () => ({ stale_count: 1, critical_stale_count: 0 }),
    fetchPredictionEvaluationSampleSize: async () => 42,
    buildCurrentBucketRiskBands: async () => ({
      d7: { sample_size: 15 },
      d30: { sample_size: 24 },
    }),
    fetchLatestCalibrationSnapshot: async () => ({ total_samples: 18 }),
    calculatePXISignal: async () => ({ signal_type: 'FULL_RISK', risk_allocation: 0.8 }),
    detectDivergence: async () => ({ alerts: ['rates_vs_credit'] }),
    resolveConflictState: () => 'ALIGNED',
    toNumber: (value: unknown, fallback = 0) => {
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : fallback;
    },
    freshnessPenaltyCount: () => 1,
    computeEdgeQualitySnapshot: () => ({ score: 58, label: 'LOW' }),
    buildEdgeQualityCalibrationFromSnapshot: () => ({ quality: 'LIMITED' }),
    buildPolicyStateSnapshot: () => ({ stance: 'RISK_ON', risk_posture: 'FULL_RISK', conflict_state: 'ALIGNED' }),
    buildUncertaintySnapshot: (reasons: string[]) => ({ reasons }),
    computeRiskSizingSnapshot: () => ({ target_pct: 60 }),
    buildTraderPlaybookSnapshot: async () => ({ recommended_size_pct: { target: 59.5 } }),
    buildConsistencySnapshot: () => ({ score: 82, state: 'WARN', components: {} }),
  }, {
    pxi: {
      date: '2026-03-05',
      score: 71,
      label: 'risk-on',
      status: 'bullish',
      delta_1d: 1.1,
      delta_7d: 4.2,
      delta_30d: 7.3,
    } as any,
    categories: [
      { category: 'credit', score: 68, weight: 0.25 },
      { category: 'macro', score: 72, weight: 0.25 },
      { category: 'liquidity', score: 74, weight: 0.25 },
    ] as any,
  });

  assert.equal(decision.as_of, '2026-03-05T00:00:00.000Z');
  assert.equal(decision.policy_state.stance, 'RISK_ON');
  assert.equal(decision.consistency.state, 'WARN');
  assert.deepEqual(decision.degraded_reasons, [
    'limited_scenario_sample',
    'stale_inputs',
    'low_edge_quality',
    'limited_calibration_sample',
  ]);
});

test('tryHandleMarketProductsRoute rebuilds brief snapshots when freshness state drifts', async () => {
  let storedSnapshot: Record<string, unknown> | null = null;

  const route = createRouteContext('https://pxi.test/api/brief?scope=market', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('FROM market_brief_snapshots')) {
        return {
          payload_json: JSON.stringify({
            as_of: '2026-03-11T00:00:00.000Z',
            summary: 'stale snapshot',
            regime_delta: 'UNCHANGED',
            top_changes: [],
            risk_posture: 'neutral',
            policy_state: {
              stance: 'MIXED',
              risk_posture: 'neutral',
              conflict_state: 'MIXED',
              base_signal: 'DEFENSIVE',
              regime_context: 'TRANSITION',
              rationale: 'mixed:stale_inputs',
              rationale_codes: ['stale_inputs'],
            },
            source_plan_as_of: '2026-03-11T00:00:00.000Z',
            contract_version: '2026-02-17-v2',
            consistency: {
              score: 97,
              state: 'PASS',
              violations: ['stale_inputs_penalty'],
              components: {
                base_score: 100,
                structural_penalty: 0,
                reliability_penalty: 3,
              },
            },
            freshness_status: {
              has_stale_data: true,
              stale_count: 2,
              critical_stale_count: 0,
            },
          }),
        };
      }
      if (sql.includes('SELECT date') && sql.includes('FROM pxi_scores')) {
        return { date: '2026-03-11' };
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const rebuiltSnapshot = {
    as_of: '2026-03-11T00:00:00.000Z',
    summary: 'fresh snapshot',
    regime_delta: 'UNCHANGED',
    top_changes: [],
    risk_posture: 'risk_off',
    policy_state: {
      stance: 'RISK_OFF',
      risk_posture: 'risk_off',
      conflict_state: 'MIXED',
      base_signal: 'DEFENSIVE',
      regime_context: 'TRANSITION',
      rationale: 'aligned:defensive',
      rationale_codes: ['aligned'],
    },
    source_plan_as_of: '2026-03-11T00:00:00.000Z',
    contract_version: '2026-02-17-v2',
    consistency: {
      score: 100,
      state: 'PASS',
      violations: [],
      components: {
        base_score: 100,
        structural_penalty: 0,
        reliability_penalty: 0,
      },
    },
    freshness_status: {
      has_stale_data: false,
      stale_count: 0,
      critical_stale_count: 0,
    },
  };

  const response = await tryHandleMarketProductsRoute(route as any, {
    isFeatureEnabled: () => true,
    ensureMarketProductSchema: async () => undefined,
    computeFreshnessStatus: async () => ({
      has_stale_data: false,
      stale_count: 0,
      critical_stale_count: 0,
    }),
    toNumber: (value: unknown, fallback = 0) => {
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : fallback;
    },
    buildBriefSnapshot: async () => rebuiltSnapshot,
    storeBriefSnapshot: async (_db: D1Database, snapshot: Record<string, unknown>) => {
      storedSnapshot = snapshot;
    },
    buildBriefFallbackSnapshot: (reason: string) => ({ degraded_reason: reason }),
  });

  assert.ok(response);
  assert.deepEqual(await response!.json(), rebuiltSnapshot);
  assert.deepEqual(storedSnapshot, rebuiltSnapshot);
});

test('tryHandleMarketCoreRoute reports a generic plan build failure for unexpected decision errors', async () => {
  const route = createRouteContext('https://pxi.test/api/plan', undefined, {
    DB: createFakeDb((sql) => {
      if (sql.includes('FROM pxi_scores ORDER BY date DESC LIMIT 10')) {
        return [{
          date: '2026-03-05',
          score: 71,
          label: 'risk-on',
          status: 'bullish',
          delta_1d: 1.1,
          delta_7d: 4.2,
          delta_30d: 7.3,
        }];
      }
      if (sql.includes('FROM category_scores WHERE date = ?')) {
        return [
          { category: 'credit', score: 68, weight: 0.25 },
          { category: 'macro', score: 72, weight: 0.25 },
          { category: 'liquidity', score: 74, weight: 0.25 },
        ];
      }
      throw new Error(`Unhandled query: ${sql}`);
    }),
  });

  const response = await tryHandleMarketCoreRoute(route, {
    isFeatureEnabled: () => true,
    detectRegime: async () => ({ regime: 'RISK_ON', confidence: 78, description: 'Risk appetite is improving.' }),
    computeFreshnessStatus: async () => {
      throw new Error('missing_build_helper');
    },
    fetchPredictionEvaluationSampleSize: async () => 42,
    buildCurrentBucketRiskBands: async () => ({
      d7: { sample_size: 15 },
      d30: { sample_size: 24 },
    }),
    fetchLatestCalibrationSnapshot: async () => ({ total_samples: 18 }),
    buildPlanFallbackPayload: (reason: string) => ({ degraded_reason: reason }),
  });

  assert.equal(response?.status, 200);
  const payload = await response!.json() as Record<string, unknown>;
  assert.equal(payload.degraded_reason, 'decision_build_failed');
});

test('generateMarketEvents emits regime, threshold, opportunity, and freshness events', async () => {
  const db = createFakeDb((sql) => {
    if (sql.includes('FROM pxi_scores')) {
      return [
        { date: '2026-03-05', score: 70 },
        { date: '2026-03-04', score: 60 },
      ];
    }
    if (sql.includes('FROM opportunity_snapshots')) {
      return {
        payload_json: JSON.stringify({
          items: [{ conviction_score: 48 }],
        }),
      };
    }
    throw new Error(`Unhandled query: ${sql}`);
  });

  const events = await generateMarketEvents(
    db,
    {
      stableHash: () => 'hash123',
      asIsoDateTime: () => '2026-03-05T12:00:00.000Z',
      toNumber: (value: unknown, fallback = 0) => {
        const numeric = Number(value);
        return Number.isFinite(numeric) ? numeric : fallback;
      },
    },
    {
      as_of: '2026-03-05T00:00:00.000Z',
      risk_posture: 'RISK_ON',
      regime_delta: 'SHIFTED',
      consistency: {
        state: 'PASS',
        score: 91,
        violations: [],
      },
      freshness_status: {
        stale_count: 2,
        critical_stale_count: 1,
        has_stale_data: true,
      },
    } as any,
    {
      horizon: '7d',
      items: [
        {
          theme_id: 'ai',
          theme_name: 'AI',
          conviction_score: 64,
        },
      ],
    } as any,
  );

  assert.deepEqual(
    events.map((event) => event.event_type).sort(),
    ['freshness_warning', 'opportunity_spike', 'regime_change', 'threshold_cross'],
  );
});

test('tryHandleMarketProductsRoute preserves opportunity coherence and CTA fields', async () => {
  const request = new Request('https://pxi.test/api/opportunities?horizon=7d&limit=1');
  const route = {
    request,
    env: {
      DB: createFakeDb((sql) => {
        if (sql.includes('FROM opportunity_snapshots')) {
          return null;
        }
        throw new Error(`Unhandled query: ${sql}`);
      }),
    },
    url: new URL(request.url),
    method: 'GET',
    corsHeaders: {},
  };

  const response = await tryHandleMarketProductsRoute(route as any, {
    isFeatureEnabled: () => true,
    ensureMarketProductSchema: async () => {},
    buildOpportunityFallbackSnapshot: () => {
      throw new Error('fallback should not be used');
    },
    buildOpportunitySnapshot: async () => ({
      as_of: '2026-03-05T00:00:00.000Z',
      horizon: '7d',
      items: [
        { id: 'op-1', theme_id: 'ai', theme_name: 'AI', conviction_score: 61 },
        { id: 'op-2', theme_id: 'rates', theme_name: 'Rates', conviction_score: 52 },
      ],
    }),
    storeOpportunitySnapshot: async () => {},
    fetchLatestCalibrationSnapshot: async () => ({ quality_band: 'LIMITED' }),
    computeFreshnessStatus: async () => ({ stale_count: 0 }),
    fetchLatestConsistencyCheck: async () => ({ state: 'WARN' }),
    resolveLatestRefreshTimestamp: async () => ({ last_refresh_at_utc: null }),
    normalizeOpportunityItemsForPublishing: (items: unknown[]) => items,
    projectOpportunityFeed: () => ({
      items: [{ id: 'op-1', theme_id: 'ai', theme_name: 'AI', conviction_score: 61 }],
      suppressed_count: 2,
      quality_filtered_count: 1,
      coherence_suppressed_count: 1,
      suppression_by_reason: { coherence: 1 },
      quality_filter_rate: 0.5,
      coherence_fail_rate: 0.33,
      degraded_reason: null,
    }),
    computeCalibrationDiagnostics: () => ({ quality_band: 'LIMITED' }),
    evaluateOpportunityCtaState: () => ({
      actionability_state: 'WATCH',
      cta_enabled: false,
      cta_disabled_reasons: ['freshness_guard'],
    }),
  });

  assert.ok(response);
  assert.equal(response?.headers.get('Cache-Control'), 'no-store');

  const payload = await response!.json() as Record<string, unknown>;
  assert.equal(payload.coherence_suppressed_count, 1);
  assert.equal(payload.coherence_fail_rate, 0.33);
  assert.equal(payload.degraded_reason, 'refresh_ttl_unknown');
  assert.equal(payload.cta_enabled, false);
  assert.deepEqual(
    (payload.actionability_reason_codes as string[]).sort(),
    ['cta_freshness_guard', 'opportunity_refresh_ttl_unknown', 'watch_state'].sort(),
  );
});

test('computeDecisionGradeScorecard composes go-live blockers from extracted diagnostics', async () => {
  const fixture = buildOpsFixture({
    positiveReturns: false,
    publishedCount: 0,
    suppressedCount: 10,
    topDirection30d: 'bearish',
  });

  const db = createFakeDb((sql, args) => {
    if (sql.includes('FROM market_consistency_checks')) {
      return [{ state: 'FAIL', count: 2 }];
    }
    if (sql.includes('FROM market_opportunity_ledger')) {
      return fixture.opportunityLedgerRows;
    }
    if (sql.includes('FROM market_opportunity_item_ledger')) {
      return fixture.itemLedgerRowsByHorizon[args[0] as '7d' | '30d'] || [];
    }
    if (sql.includes('FROM indicator_values')) {
      return fixture.indicatorRows;
    }
    throw new Error(`Unhandled query: ${sql}`);
  });

  const scorecard = await computeDecisionGradeScorecard(
    db,
    createDecisionImpactHelpers({
      computeFreshnessSloWindow: async () => ({
        slo_attainment_pct: 72,
        days_with_critical_stale: 3,
        days_observed: 30,
      }),
      computeUtilityFunnelSummary: async () => ({
        window_days: 30,
        days_observed: 30,
        actionable_sessions: 1,
        cta_action_rate_pct: 0.5,
        decision_actionable_views: 3,
        decision_events_total: 1,
        no_action_unlock_views: 2,
        no_action_unlock_coverage_pct: 10,
        cta_action_clicks: 0,
        unique_sessions: 1,
      }),
      fetchLatestCalibrationSnapshot: async (_db: D1Database, metric: string, horizon: string | null) => {
        if (metric === 'conviction' && horizon === '7d') return { quality_band: 'INSUFFICIENT' };
        if (metric === 'conviction' && horizon === '30d') return { quality_band: 'LIMITED' };
        return { quality_band: 'LIMITED' };
      },
      computeCalibrationDiagnostics: (snapshot: { quality_band?: string }) => ({
        quality_band: snapshot.quality_band || 'LIMITED',
      }),
      buildEdgeDiagnosticsReport: async () => ({
        windows: [
          { lower_bound_positive: false, leakage_sentinel: { pass: false } },
          { lower_bound_positive: false, leakage_sentinel: { pass: true } },
        ],
        promotion_gate: {
          pass: false,
          reasons: ['leakage_sentinel_fail'],
        },
      }),
    }),
    30,
    {
      enforce_enabled: true,
      min_sample_size: 5,
      min_actionable_sessions: 5,
    },
  );

  assert.equal(scorecard.go_live_ready, false);
  assert.equal(scorecard.grade, 'RED');
  assert.ok(scorecard.go_live_blockers.includes('score_below_threshold'));
  assert.ok(scorecard.go_live_blockers.includes('freshness_not_pass'));
  assert.ok(scorecard.go_live_blockers.includes('consistency_fail'));
  assert.ok(scorecard.go_live_blockers.includes('calibration_fail'));
  assert.ok(scorecard.go_live_blockers.includes('edge_promotion_gate_fail'));
  assert.ok(scorecard.go_live_blockers.includes('opportunity_hygiene_fail'));
  assert.ok(scorecard.go_live_blockers.includes('decision_impact_not_enforce_ready'));
  assert.ok(scorecard.go_live_blockers.includes('decision_impact_market_7d_below_enforce_min_sample'));
  assert.ok(scorecard.go_live_blockers.includes('decision_impact_cta_action_below_enforce_min_sessions'));
});
