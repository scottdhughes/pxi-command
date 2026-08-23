import assert from 'node:assert/strict';
import test from 'node:test';

type ResolveHook = (
  specifier: string,
  context: unknown,
  nextResolve: (specifier: string, context: unknown) => unknown,
) => unknown;

const { registerHooks } = await import('node:module') as unknown as {
  registerHooks: (hooks: { resolve: ResolveHook }) => void;
};
const cloudflareEmailStub = `data:text/javascript,${encodeURIComponent('export class EmailMessage {}')}`;
registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === 'cloudflare:email') {
      return { url: cloudflareEmailStub, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});

const {
  buildDecisionStack,
  buildOpportunitySnapshot,
  buildPlanFallbackPayload,
  resolvePlanActionability,
  selectLatestPxiWithCategories,
} = await import('../runtime/legacy.js');

const CANONICAL_CATEGORIES = [
  'positioning',
  'credit',
  'volatility',
  'breadth',
  'macro',
  'global',
  'crypto',
] as const;

function actionabilityInput(opportunityRef: Record<string, unknown>) {
  return {
    opportunity_ref: opportunityRef,
    edge_quality: { label: 'HIGH' },
    freshness: { critical_stale_count: 0 },
    consistency: { state: 'PASS' },
    edge_evidence_gate: { pass: true },
  } as any;
}

test('legacy plan fallback exposes one fail-closed decision contract', () => {
  const payload = buildPlanFallbackPayload('test_failure') as any;

  assert.match(payload.setup_summary, /^NO ACTION/);
  assert.equal(payload.decision_contract.headline, 'No actionable signal');
  assert.equal(payload.decision_contract.evidence.status, 'BLOCKED');
  assert.equal(payload.decision_contract.action_authorized, false);
  assert.equal(payload.action_now.action_authorized, false);
  assert.equal(payload.action_now.risk_allocation_target, null);
  assert.equal(payload.edge_quality.diagnostic_scope, 'INPUT_AND_MODEL_STRUCTURE');
  assert.equal(payload.edge_quality.validated_edge, false);
  assert.equal(payload.trader_playbook.authorization, 'WITHHELD');
  assert.deepEqual(
    payload.trader_playbook.recommended_size_pct,
    { min: null, target: null, max: null },
  );
  assert.match(payload.decision_stack.why_now, /^Prospective evidence blocked/);
  assert.match(payload.decision_stack.confidence, /^evidence=BLOCKED \| action=WITHHELD/);
  assert.equal(payload.decision_stack.cta_state, 'NO_ACTION');
});

test('legacy plan actionability requires an explicitly enabled opportunity CTA', () => {
  const disabled = resolvePlanActionability(actionabilityInput({
    eligible_count: 2,
    degraded_reason: 'refresh_ttl_overdue',
    cta_enabled: false,
    cta_disabled_reasons: ['refresh_ttl_overdue', 'calibration_quality_not_robust'],
  }));

  assert.equal(disabled.state, 'NO_ACTION');
  assert.deepEqual(disabled.reason_codes, [
    'opportunity_cta_disabled',
    'opportunity_cta_refresh_ttl_overdue',
    'opportunity_cta_calibration_quality_not_robust',
  ]);

  const missing = resolvePlanActionability(actionabilityInput({
    eligible_count: 2,
    degraded_reason: null,
  }));
  assert.equal(missing.state, 'NO_ACTION');
  assert.deepEqual(missing.reason_codes, ['opportunity_cta_disabled']);

  const enabled = resolvePlanActionability(actionabilityInput({
    eligible_count: 2,
    degraded_reason: null,
    cta_enabled: true,
    cta_disabled_reasons: [],
  }));
  assert.equal(enabled.state, 'ACTIONABLE');
  assert.deepEqual(enabled.reason_codes, ['high_edge_with_eligible_opportunities']);
});

test('legacy decision stack never presents an unauthorized actionable CTA', () => {
  const stack = buildDecisionStack({
    actionability_state: 'ACTIONABLE',
    action_authorized: true,
    actionability_reason_codes: ['opportunity_cta_disabled'],
    edge_evidence_gate: { pass: true, reasons: [] },
    setup_summary: 'Research posture only.',
    edge_quality: { label: 'HIGH' },
    consistency: { state: 'PASS' },
  } as any);

  assert.equal(stack.cta_state, 'NO_ACTION');
  assert.match(stack.what_to_do, /^No action\./);
  assert.match(stack.why_now, /^Action authorization withheld/);
  assert.match(stack.confidence, /^evidence=PASSED \| action=WITHHELD/);

  const authorized = buildDecisionStack({
    actionability_state: 'ACTIONABLE',
    action_authorized: true,
    actionability_reason_codes: ['high_edge_with_eligible_opportunities'],
    edge_evidence_gate: { pass: true, reasons: [] },
    setup_summary: 'Authorized plan.',
    edge_quality: { label: 'HIGH' },
    consistency: { state: 'PASS' },
    opportunity_ref: { cta_enabled: true },
  } as any);
  assert.equal(authorized.cta_state, 'ACTIONABLE');
  assert.match(authorized.why_now, /^Action authorized/);
});

function createSelectionDb(categoriesByDate: Record<string, string[]>): D1Database {
  const scores = Object.keys(categoriesByDate)
    .sort()
    .reverse()
    .map((date) => ({
      date,
      score: 70,
      label: 'Constructive',
      status: 'GREEN',
      delta_1d: 1,
      delta_7d: 2,
      delta_30d: 3,
    }));

  return {
    prepare(sql: string) {
      let boundDate = '';
      return {
        bind(date: string) {
          boundDate = date;
          return this;
        },
        async all() {
          if (sql.includes('FROM pxi_scores')) return { results: scores };
          if (sql.includes('FROM category_scores')) {
            return {
              results: (categoriesByDate[boundDate] || []).map((category) => ({
                category,
                score: 70,
                weight: 1 / CANONICAL_CATEGORIES.length,
              })),
            };
          }
          throw new Error(`Unhandled query: ${sql}`);
        },
      };
    },
  } as unknown as D1Database;
}

test('legacy PXI selector skips partial refreshes and filters to the exact canonical model state', async () => {
  const selected = await selectLatestPxiWithCategories(createSelectionDb({
    '2026-08-23': ['macro', 'credit', 'breadth'],
    '2026-08-22': [...CANONICAL_CATEGORIES, 'legacy_extra'],
  }));

  assert.equal(selected.pxi?.date, '2026-08-22');
  assert.deepEqual(
    selected.categories.map((category) => category.category).sort(),
    [...CANONICAL_CATEGORIES].sort(),
  );
});

test('legacy PXI selector fails closed when no exact canonical model state exists', async () => {
  const selected = await selectLatestPxiWithCategories(createSelectionDb({
    '2026-08-23': ['macro', 'credit', 'breadth'],
  }));

  assert.equal(selected.pxi, null);
  assert.deepEqual(selected.categories, []);
});

test('legacy opportunity snapshot is anchored to the latest complete PXI date and date-bounds its inputs', async () => {
  const canonicalDate = '2026-03-10';
  const queryBounds: Record<string, unknown[]> = {};

  const statement = (sql: string, bound: unknown[] = []) => ({
    bind: (...values: unknown[]) => statement(sql, values),
    async all() {
      if (sql.includes('FROM pxi_scores') && sql.includes('LIMIT 10')) {
        return {
          results: [
            {
              date: '2026-03-11',
              score: 82,
              label: 'MAX PAMP',
              status: 'partial',
              delta_1d: 3,
              delta_7d: 6,
              delta_30d: 9,
            },
            {
              date: canonicalDate,
              score: 68,
              label: 'PAMPING',
              status: 'complete',
              delta_1d: 1,
              delta_7d: 2,
              delta_30d: 4,
            },
          ],
        };
      }
      if (sql.includes('SELECT category, score, weight FROM category_scores')) {
        const date = String(bound[0]);
        return {
          results: (date === canonicalDate ? CANONICAL_CATEGORIES : ['macro', 'credit']).map((category) => ({
            category,
            score: 68,
            weight: 1 / CANONICAL_CATEGORIES.length,
          })),
        };
      }
      if (sql.includes('FROM prediction_log')) return { results: [] };
      if (sql.includes('FROM opportunity_snapshots')) return { results: [] };
      if (sql.includes("indicator_id = 'spy_close'")) return { results: [] };
      if (sql.includes('SELECT indicator_id, MAX(date) as last_date')) return { results: [] };
      if (sql.includes('SELECT category as theme_id')) {
        return {
          results: CANONICAL_CATEGORIES.map((category) => ({
            theme_id: category,
            theme_name: category,
            score: 68,
          })),
        };
      }
      throw new Error(`Unhandled all query: ${sql}`);
    },
    async first() {
      if (sql.includes('FROM pxi_signal')) {
        queryBounds.signal = bound;
        return {
          date: canonicalDate,
          risk_allocation: 0.65,
          signal_type: 'REDUCED_RISK',
          regime: 'TRANSITION',
        };
      }
      if (sql.includes('FROM ensemble_predictions')) {
        queryBounds.ensemble = bound;
        return {
          prediction_date: canonicalDate,
          ensemble_7d: 0.2,
          ensemble_30d: 0.4,
          confidence_7d: 'medium',
          confidence_30d: 'medium',
        };
      }
      if (sql.includes('FROM market_calibration_snapshots')) return null;
      throw new Error(`Unhandled first query: ${sql}`);
    },
  });

  const requestedSignalUrls: string[] = [];
  const signalsService = {
    async fetch(request: Request) {
      requestedSignalUrls.push(request.url);
      if (request.url.includes('/api/runs?')) {
        return Response.json({
          runs: [
            { id: 'future-run', created_at_utc: '2026-03-11T06:00:00.000Z' },
            { id: 'canonical-run', created_at_utc: '2026-03-10T06:00:00.000Z' },
          ],
        });
      }
      if (request.url.endsWith('/api/runs/canonical-run')) {
        return Response.json({
          themes: [{ theme_id: 'canonical-theme', theme_name: 'Canonical theme', score: 70, key_tickers: [] }],
        });
      }
      return Response.json({ error: 'unexpected run' }, { status: 404 });
    },
  };

  const snapshot = await buildOpportunitySnapshot(
    { prepare: (sql: string) => statement(sql) } as unknown as D1Database,
    '7d',
    null,
    { signals_service: signalsService },
  );

  assert.equal(snapshot?.as_of, `${canonicalDate}T00:00:00.000Z`);
  assert.equal(snapshot?.items[0]?.theme_id, 'canonical-theme');
  assert.deepEqual(queryBounds.signal, [canonicalDate]);
  assert.deepEqual(queryBounds.ensemble, [canonicalDate]);
  assert.ok(requestedSignalUrls.some((url) => url.endsWith('/api/runs/canonical-run')));
  assert.ok(!requestedSignalUrls.some((url) => url.endsWith('/api/runs/future-run')));
});
