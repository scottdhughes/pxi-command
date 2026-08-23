import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildDecisionContractSnapshot,
  CANONICAL_PXI_CATEGORIES,
  evaluateOpportunityAuthorityProjection,
  projectTraderPlaybookForDecision,
  selectLatestPxiWithCategories,
} from './market-core.js';

function createSelectionDb(args: {
  dates: string[];
  categoriesByDate: Record<string, string[]>;
}): D1Database {
  const statement = (sql: string, bound: unknown[] = []) => ({
    bind: (...values: unknown[]) => statement(sql, values),
    all: async <T>() => {
      if (sql.includes('FROM pxi_scores')) {
        return {
          results: args.dates.map((date, index) => ({
            date,
            score: 60 - index,
            label: 'NEUTRAL',
            status: 'neutral',
            delta_1d: 0,
            delta_7d: 0,
            delta_30d: 0,
          })) as T[],
        };
      }
      if (sql.includes('FROM category_scores')) {
        const date = String(bound[0]);
        return {
          results: (args.categoriesByDate[date] || []).map((category) => ({
            category,
            score: 50,
            weight: 1 / CANONICAL_PXI_CATEGORIES.length,
          })) as T[],
        };
      }
      throw new Error(`Unexpected query: ${sql}`);
    },
  });

  return {
    prepare(sql: string) {
      return statement(sql);
    },
  } as D1Database;
}

test('selectLatestPxiWithCategories skips a partial refresh and selects the latest exact model state', async () => {
  const selected = await selectLatestPxiWithCategories(createSelectionDb({
    dates: ['2026-08-23', '2026-08-22'],
    categoriesByDate: {
      '2026-08-23': ['positioning', 'credit', 'volatility'],
      '2026-08-22': [...CANONICAL_PXI_CATEGORIES],
    },
  }));

  assert.equal(selected.pxi?.date, '2026-08-22');
  assert.deepEqual(
    selected.categories.map((row) => row.category).sort(),
    [...CANONICAL_PXI_CATEGORIES].sort(),
  );
});

test('selectLatestPxiWithCategories fails closed when no exact model state exists', async () => {
  const selected = await selectLatestPxiWithCategories(createSelectionDb({
    dates: ['2026-08-23'],
    categoriesByDate: {
      '2026-08-23': ['positioning', 'credit', 'volatility', 'breadth', 'macro', 'global'],
    },
  }));

  assert.equal(selected.pxi, null);
  assert.deepEqual(selected.categories, []);
});

function decisionArgs() {
  return {
    actionability_state: 'ACTIONABLE' as const,
    action_authorized: true,
    actionability_reason_codes: ['candidate_actionable'],
    pxi_label: 'NEUTRAL',
    regime: 'RISK_ON' as const,
    research_posture: 'REDUCED_RISK' as const,
    evidence_gate: { pass: true, reasons: [] },
    consistency_state: 'PASS' as const,
    opportunity_cta_enabled: true,
    allocation_target: 0.62,
    structural_quality: { score: 91, label: 'HIGH' as const },
  };
}

test('decision authority is granted only when every canonical gate passes', () => {
  const authorized = buildDecisionContractSnapshot(decisionArgs());

  assert.equal(authorized.headline, 'Actionable plan');
  assert.equal(authorized.actionability_state, 'ACTIONABLE');
  assert.equal(authorized.action_authorized, true);
  assert.equal(authorized.structural_quality.interpretation, 'INPUT_AND_MODEL_DIAGNOSTIC_NOT_VALIDATED_EDGE');
});

test('high structural quality cannot override failed evidence, CTA, consistency, or target gates', () => {
  const cases = [
    {
      patch: { evidence_gate: { pass: false, reasons: ['prospective_sample_insufficient'] } },
      reason: 'edge_evidence_gate_block',
    },
    {
      patch: { opportunity_cta_enabled: false },
      reason: 'opportunity_cta_disabled',
    },
    {
      patch: { consistency_state: 'FAIL' as const },
      reason: 'consistency_fail_block',
    },
    {
      patch: { allocation_target: null },
      reason: 'allocation_target_missing',
    },
  ];

  for (const { patch, reason } of cases) {
    const decision = buildDecisionContractSnapshot({ ...decisionArgs(), ...patch });
    assert.equal(decision.headline, 'No actionable signal');
    assert.equal(decision.actionability_state, 'NO_ACTION');
    assert.equal(decision.action_authorized, false);
    assert.ok(decision.actionability_reason_codes.includes(reason));
  }
});

test('withheld trader playbook cannot publish allocation sizing', () => {
  const projected = projectTraderPlaybookForDecision({
    recommended_size_pct: { min: 45, target: 60, max: 75 },
    scenarios: [{ condition: 'If confirmed', action: 'Increase', invalidation: 'If reversed' }],
    benchmark_follow_through_7d: {
      hit_rate: 0.55,
      sample_size: 40,
      unavailable_reason: null,
    },
  }, false);

  assert.equal(projected.authorization, 'WITHHELD');
  assert.deepEqual(projected.recommended_size_pct, { min: null, target: null, max: null });
  assert.match(projected.scenarios[0].action, /Do not use/i);
});

test('opportunity authority projection carries the shared calibration/TTL CTA veto', () => {
  const authority = evaluateOpportunityAuthorityProjection({
    items: [{ id: 'theme-1' }] as any,
    calibration: { total_samples: 5 },
    coherence_gate_enabled: true,
    freshness: { critical_stale_count: 0 },
    consistency_state: 'PASS',
    edge_evidence_gate: { pass: true, reasons: [] },
    ttl: {
      data_age_seconds: 60,
      ttl_state: 'fresh',
      next_expected_refresh_at: '2026-08-24T06:00:00.000Z',
      overdue_seconds: 0,
    },
  }, {
    normalizeOpportunityItemsForPublishing: (items: unknown[]) => items,
    projectOpportunityFeed: (items: unknown[]) => ({
      items,
      suppressed_count: 0,
      degraded_reason: null,
      suppression_by_reason: {},
    }),
    computeCalibrationDiagnostics: () => ({ quality_band: 'INSUFFICIENT', ece: null }),
    evaluateOpportunityCtaState: () => ({
      cta_enabled: false,
      cta_disabled_reasons: ['calibration_quality_not_robust', 'calibration_ece_unavailable'],
      actionability_state: 'ACTIONABLE',
    }),
  });

  assert.equal(authority.projected_feed.items.length, 1);
  assert.equal(authority.cta_state.actionability_state, 'ACTIONABLE');
  assert.equal(authority.cta_state.cta_enabled, false);
  assert.deepEqual(authority.cta_state.cta_disabled_reasons, [
    'calibration_quality_not_robust',
    'calibration_ece_unavailable',
  ]);
});

test('opportunity authority projection cannot enable a CTA for a watch-only projection', () => {
  const authority = evaluateOpportunityAuthorityProjection({
    items: [{ id: 'theme-1' }] as any,
    calibration: { total_samples: 80 },
    coherence_gate_enabled: true,
    freshness: { critical_stale_count: 0 },
    consistency_state: 'WARN',
    edge_evidence_gate: { pass: true, reasons: [] },
    ttl: {
      data_age_seconds: 60,
      ttl_state: 'fresh',
      next_expected_refresh_at: '2026-08-24T06:00:00.000Z',
      overdue_seconds: 0,
    },
  }, {
    normalizeOpportunityItemsForPublishing: (items: unknown[]) => items,
    projectOpportunityFeed: (items: unknown[]) => ({
      items,
      suppressed_count: 1,
      degraded_reason: 'quality_filtered',
      suppression_by_reason: { quality_filtered: 1 },
    }),
    computeCalibrationDiagnostics: () => ({ quality_band: 'ROBUST', ece: 0.03 }),
    evaluateOpportunityCtaState: () => ({
      cta_enabled: true,
      cta_disabled_reasons: [],
      actionability_state: 'WATCH',
    }),
  });

  assert.equal(authority.cta_state.actionability_state, 'WATCH');
  assert.equal(authority.cta_state.cta_enabled, false);
  assert.deepEqual(authority.cta_state.cta_disabled_reasons, ['actionability_state_not_actionable']);
});
