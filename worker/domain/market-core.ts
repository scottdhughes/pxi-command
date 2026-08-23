import type {
  CanonicalMarketDecision,
  CategoryRow,
  DecisionContractSnapshot,
  OpportunitySnapshot,
  OpportunityTtlMetadata,
  PlanPayload,
  PXIResponsePayload,
  PXIRow,
  SignalResponsePayload,
  SparklineRow,
  WorkerRouteContext,
} from '../types';
import {
  HISTORICAL_BACKFILL_SEED_MARKER,
  parseProspectiveOpportunitySnapshot,
} from '../lib/opportunity-snapshot-history';

type MarketCoreDeps = Record<string, any>;

export interface EdgeEvidenceGateResolution {
  pass: boolean;
  reasons: string[];
}

export const CANONICAL_PXI_CATEGORIES = Object.freeze([
  'positioning',
  'credit',
  'volatility',
  'breadth',
  'macro',
  'global',
  'crypto',
] as const);

function selectCanonicalCategories(categories: CategoryRow[]): CategoryRow[] | null {
  const canonical = new Set<string>(CANONICAL_PXI_CATEGORIES);
  const selected = categories.filter((row) => canonical.has(row.category));
  const present = new Set(selected.map((row) => row.category));
  return selected.length === CANONICAL_PXI_CATEGORIES.length &&
    present.size === CANONICAL_PXI_CATEGORIES.length &&
    CANONICAL_PXI_CATEGORIES.every((category) => present.has(category))
    ? selected
    : null;
}

export function buildDecisionContractSnapshot(args: {
  actionability_state: DecisionContractSnapshot['actionability_state'];
  action_authorized: boolean;
  actionability_reason_codes: string[];
  pxi_label: string;
  regime: DecisionContractSnapshot['descriptive_context']['regime'];
  research_posture: DecisionContractSnapshot['descriptive_context']['research_posture'];
  evidence_gate: EdgeEvidenceGateResolution;
  consistency_state: 'PASS' | 'WARN' | 'FAIL';
  opportunity_cta_enabled: boolean;
  allocation_target: number | null;
  structural_quality: {
    score: number;
    label: DecisionContractSnapshot['structural_quality']['label'];
  };
}): DecisionContractSnapshot {
  const hasNumericAllocationTarget = typeof args.allocation_target === 'number' &&
    Number.isFinite(args.allocation_target);
  const actionAuthorized = args.action_authorized === true &&
    args.actionability_state === 'ACTIONABLE' &&
    args.evidence_gate.pass === true &&
    args.consistency_state !== 'FAIL' &&
    args.opportunity_cta_enabled === true &&
    hasNumericAllocationTarget;
  const actionabilityState = actionAuthorized
    ? 'ACTIONABLE'
    : args.actionability_state === 'ACTIONABLE'
      ? 'NO_ACTION'
      : args.actionability_state;
  const evidenceReasons = Array.from(new Set(args.evidence_gate.reasons));
  const reasonCodes = Array.from(new Set([
    ...args.actionability_reason_codes,
    ...(args.evidence_gate.pass ? [] : ['edge_evidence_gate_block']),
    ...(args.action_authorized && args.consistency_state === 'FAIL' ? ['consistency_fail_block'] : []),
    ...(args.action_authorized && args.opportunity_cta_enabled !== true ? ['opportunity_cta_disabled'] : []),
    ...(args.action_authorized && !hasNumericAllocationTarget ? ['allocation_target_missing'] : []),
    ...(args.action_authorized && !actionAuthorized ? ['authorization_invariant_block'] : []),
  ]));

  return {
    contract_version: '2026-08-23-v1',
    headline: actionAuthorized
      ? 'Actionable plan'
      : actionabilityState === 'WATCH'
        ? 'Watch only'
        : 'No actionable signal',
    actionability_state: actionabilityState,
    action_authorized: actionAuthorized,
    actionability_reason_codes: reasonCodes,
    descriptive_context: {
      pxi_label: args.pxi_label,
      regime: args.regime,
      research_posture: args.research_posture,
    },
    evidence: {
      status: args.evidence_gate.pass ? 'PASSED' : 'BLOCKED',
      pass: args.evidence_gate.pass,
      reason_codes: evidenceReasons,
    },
    structural_quality: {
      score: args.structural_quality.score,
      label: args.structural_quality.label,
      interpretation: 'INPUT_AND_MODEL_DIAGNOSTIC_NOT_VALIDATED_EDGE',
    },
    consistency_scope: 'INTERNAL_COHERENCE',
  };
}

export function projectTraderPlaybookForDecision(
  playbook: CanonicalMarketDecision['trader_playbook'],
  actionAuthorized: boolean,
): PlanPayload['trader_playbook'] {
  if (actionAuthorized) {
    return {
      authorization: 'AUTHORIZED',
      recommended_size_pct: playbook.recommended_size_pct,
      scenarios: playbook.scenarios,
      benchmark_follow_through_7d: playbook.benchmark_follow_through_7d,
    };
  }

  return {
    authorization: 'WITHHELD',
    recommended_size_pct: { min: null, target: null, max: null },
    scenarios: [{
      condition: 'The prospective evidence or plan actionability gate is blocked.',
      action: 'Do not use the research posture to change allocation.',
      invalidation: 'Re-evaluate only after the evidence gate passes and the plan becomes actionable.',
    }],
    benchmark_follow_through_7d: playbook.benchmark_follow_through_7d,
  };
}

function asObject(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === 'object'
    ? value as Record<string, unknown>
    : null;
}

function gateReasons(
  gate: Record<string, unknown> | null,
  prefix: string,
): string[] {
  if (!gate || !Array.isArray(gate.reasons)) return [];
  return gate.reasons
    .filter((reason): reason is string => typeof reason === 'string' && reason.length > 0)
    .map((reason) => `${prefix}:${reason}`);
}

/**
 * Resolve one horizon's actionable evidence gate. The consumer deliberately
 * requires every new gate component so a stale or partially deployed payload
 * cannot silently restore actionability.
 */
export function resolveEdgeEvidenceGate(
  report: unknown,
  horizon: '7d' | '30d',
): EdgeEvidenceGateResolution {
  const reportObject = asObject(report);
  if (!reportObject || !Array.isArray(reportObject.windows)) {
    return { pass: false, reasons: ['edge_diagnostics_unavailable'] };
  }

  const window = reportObject.windows
    .map(asObject)
    .find((candidate) => candidate?.horizon === horizon) || null;
  const integrityGate = asObject(reportObject.integrity_gate);
  const policyAlignmentGate = asObject(reportObject.policy_alignment_gate);
  const performanceGate = asObject(window?.performance_gate);
  const evidenceGate = asObject(window?.evidence_gate);
  const reasons: string[] = [];

  if (typeof integrityGate?.pass !== 'boolean') reasons.push('integrity_gate_missing');
  if (typeof policyAlignmentGate?.pass !== 'boolean') reasons.push('policy_alignment_gate_missing');
  if (!window) reasons.push(`${horizon}:diagnostic_window_missing`);
  if (typeof performanceGate?.pass !== 'boolean') reasons.push(`${horizon}:performance_gate_missing`);
  if (typeof evidenceGate?.pass !== 'boolean') reasons.push(`${horizon}:evidence_gate_missing`);

  reasons.push(...gateReasons(integrityGate, 'integrity'));
  reasons.push(...gateReasons(policyAlignmentGate, 'policy_alignment'));
  reasons.push(...gateReasons(performanceGate, `${horizon}:performance`));
  reasons.push(...gateReasons(evidenceGate, `${horizon}:evidence`));

  if (integrityGate?.pass === false && gateReasons(integrityGate, 'integrity').length === 0) {
    reasons.push('integrity_gate_failed');
  }
  if (performanceGate?.pass === false && gateReasons(performanceGate, `${horizon}:performance`).length === 0) {
    reasons.push(`${horizon}:performance_gate_failed`);
  }
  if (evidenceGate?.pass === false && gateReasons(evidenceGate, `${horizon}:evidence`).length === 0) {
    reasons.push(`${horizon}:evidence_gate_failed`);
  }

  return {
    pass:
      integrityGate?.pass === true &&
      policyAlignmentGate?.pass === true &&
      performanceGate?.pass === true &&
      evidenceGate?.pass === true,
    reasons: Array.from(new Set(reasons)),
  };
}

export function suppressProjectionForEdgeEvidence<T extends Record<string, any>>(
  projection: T,
  gate: EdgeEvidenceGateResolution,
): T {
  if (gate.pass) return projection;

  const publishedItems = Array.isArray(projection.items) ? projection.items : [];
  const priorSuppressed = Number.isFinite(Number(projection.suppressed_count))
    ? Math.max(0, Number(projection.suppressed_count))
    : 0;
  const suppressionByReason = asObject(projection.suppression_by_reason) || {};
  const priorEdgeSuppressed = Number.isFinite(Number(suppressionByReason.edge_evidence_suppressed))
    ? Math.max(0, Number(suppressionByReason.edge_evidence_suppressed))
    : 0;

  return {
    ...projection,
    items: [],
    suppressed_count: priorSuppressed + publishedItems.length,
    degraded_reason: 'edge_evidence_gate_failed',
    suppression_by_reason: {
      ...suppressionByReason,
      edge_evidence_suppressed: priorEdgeSuppressed + publishedItems.length,
    },
  };
}

export function evaluateOpportunityAuthorityProjection(args: {
  items: OpportunitySnapshot['items'];
  calibration: unknown;
  coherence_gate_enabled: boolean;
  freshness: unknown;
  consistency_state: 'PASS' | 'WARN' | 'FAIL';
  edge_evidence_gate: EdgeEvidenceGateResolution;
  ttl: OpportunityTtlMetadata;
}, deps: MarketCoreDeps): {
  projected_feed: Record<string, any>;
  cta_state: {
    cta_enabled: boolean;
    cta_disabled_reasons: string[];
    actionability_state: 'ACTIONABLE' | 'WATCH' | 'NO_ACTION';
  };
} {
  const normalizedItems = deps.normalizeOpportunityItemsForPublishing(
    args.items,
    args.calibration,
  );
  const candidateProjection = deps.projectOpportunityFeed(normalizedItems, {
    coherence_gate_enabled: args.coherence_gate_enabled,
    freshness: args.freshness,
    consistency_state: args.consistency_state,
  });
  const projected = suppressProjectionForEdgeEvidence(
    candidateProjection,
    args.edge_evidence_gate,
  );

  let effectiveDegradedReason = projected.degraded_reason;
  if (!effectiveDegradedReason && args.ttl.ttl_state === 'overdue') {
    effectiveDegradedReason = 'refresh_ttl_overdue';
  } else if (!effectiveDegradedReason && args.ttl.ttl_state === 'unknown') {
    effectiveDegradedReason = 'refresh_ttl_unknown';
  }

  const projectedFeed = {
    ...projected,
    degraded_reason: effectiveDegradedReason,
  };
  if (typeof deps.computeCalibrationDiagnostics !== 'function' ||
    typeof deps.evaluateOpportunityCtaState !== 'function') {
    return {
      projected_feed: projectedFeed,
      cta_state: {
        cta_enabled: false,
        cta_disabled_reasons: ['authority_contract_unavailable'],
        actionability_state: 'NO_ACTION',
      },
    };
  }

  const ctaState = deps.evaluateOpportunityCtaState(
    projectedFeed,
    deps.computeCalibrationDiagnostics(args.calibration),
    args.ttl,
    effectiveDegradedReason,
  );
  const evaluatedActionabilityState = ctaState.actionability_state === 'ACTIONABLE' ||
    ctaState.actionability_state === 'WATCH'
    ? ctaState.actionability_state
    : 'NO_ACTION';
  const actionabilityState = args.edge_evidence_gate.pass
    ? evaluatedActionabilityState
    : 'NO_ACTION';
  const ctaEnabled = args.edge_evidence_gate.pass === true &&
    ctaState.cta_enabled === true &&
    actionabilityState === 'ACTIONABLE';
  const disabledReasons = Array.isArray(ctaState.cta_disabled_reasons)
    ? ctaState.cta_disabled_reasons
    : ['authority_contract_unavailable'];
  return {
    projected_feed: projectedFeed,
    cta_state: {
      cta_enabled: ctaEnabled,
      cta_disabled_reasons: Array.from(new Set([
        ...disabledReasons,
        ...(!args.edge_evidence_gate.pass ? ['edge_evidence_gate_failed'] : []),
        ...(!ctaEnabled && ctaState.cta_enabled === true
          ? ['actionability_state_not_actionable']
          : []),
      ])),
      actionability_state: actionabilityState,
    },
  };
}

function resolvePlanFallbackReason(error: unknown): string {
  if (error instanceof Error && error.message === 'no_pxi_data') {
    return 'no_pxi_data';
  }
  if (error instanceof Error && error.message === 'incomplete_pxi_categories') {
    return 'incomplete_pxi_state';
  }
  return 'decision_build_failed';
}

export async function selectLatestPxiWithCategories(db: D1Database): Promise<{
  pxi: PXIRow | null;
  categories: CategoryRow[];
}> {
  const recentScores = await db.prepare(
    'SELECT date, score, label, status, delta_1d, delta_7d, delta_30d FROM pxi_scores ORDER BY date DESC LIMIT 10'
  ).all<PXIRow>();

  let selected: PXIRow | null = null;
  let selectedCategories: CategoryRow[] = [];

  for (const candidate of recentScores.results || []) {
    const cats = await db.prepare(
      'SELECT category, score, weight FROM category_scores WHERE date = ?'
    ).bind(candidate.date).all<CategoryRow>();
    const canonicalCategories = selectCanonicalCategories(cats.results || []);
    if (canonicalCategories) {
      selected = candidate;
      selectedCategories = canonicalCategories;
      break;
    }
  }

  return {
    pxi: selected,
    categories: selectedCategories,
  };
}

export async function buildCanonicalMarketDecision(
  db: D1Database,
  deps: MarketCoreDeps,
  options?: {
    pxi?: PXIRow;
    categories?: CategoryRow[];
  }
): Promise<CanonicalMarketDecision> {
  let pxi = options?.pxi || null;
  let categories = options?.categories || [];

  if (!pxi) {
    const selected = await selectLatestPxiWithCategories(db);
    pxi = selected.pxi;
    categories = selected.categories;
  }

  if (!pxi) {
    throw new Error('no_pxi_data');
  }

  const canonicalCategories = selectCanonicalCategories(categories);
  if (!canonicalCategories) {
    throw new Error('incomplete_pxi_categories');
  }
  categories = canonicalCategories;

  const categoryScores = categories.map((row) => ({ score: row.score }));
  const [regime, freshness, mlSampleSize, riskBand, edgeCalibrationSnapshot] = await Promise.all([
    deps.detectRegime(db, pxi.date),
    deps.computeFreshnessStatus(db),
    deps.fetchPredictionEvaluationSampleSize(db),
    deps.buildCurrentBucketRiskBands(db, pxi.score),
    deps.fetchLatestCalibrationSnapshot(db, 'edge_quality', null),
  ]);

  const signal = await deps.calculatePXISignal(
    db,
    { score: pxi.score, delta_7d: pxi.delta_7d, delta_30d: pxi.delta_30d },
    regime,
    categoryScores,
  );

  const divergence = await deps.detectDivergence(db, pxi.score, regime);
  const conflictState = deps.resolveConflictState(regime, signal);
  const staleCountRaw = Math.max(0, Math.floor(deps.toNumber(freshness.stale_count, 0)));
  const stalePenaltyUnits = deps.freshnessPenaltyCount(freshness);
  const edgeQuality = deps.computeEdgeQualitySnapshot({
    staleCount: staleCountRaw,
    mlSampleSize,
    regime,
    conflictState,
    divergenceCount: divergence.alerts.length,
  });
  const edgeQualityWithCalibration = {
    ...edgeQuality,
    calibration: deps.buildEdgeQualityCalibrationFromSnapshot(edgeCalibrationSnapshot, edgeQuality.score),
  };

  const policyState = deps.buildPolicyStateSnapshot({
    signal,
    regime,
    edgeQuality: edgeQualityWithCalibration,
    freshness,
    calibrationQuality: edgeQualityWithCalibration.calibration.quality,
  });

  const degradedReasons: string[] = [];
  if (riskBand.d7.sample_size < 20 || riskBand.d30.sample_size < 20) degradedReasons.push('limited_scenario_sample');
  if (stalePenaltyUnits > 0) degradedReasons.push('stale_inputs');
  if (edgeQualityWithCalibration.label === 'LOW') degradedReasons.push('low_edge_quality');
  if (
    edgeQualityWithCalibration.calibration.quality === 'INSUFFICIENT' ||
    (edgeQualityWithCalibration.calibration.quality === 'LIMITED' && stalePenaltyUnits > 0)
  ) {
    degradedReasons.push('limited_calibration_sample');
  }

  const uncertainty = deps.buildUncertaintySnapshot(degradedReasons);
  const riskSizing = deps.computeRiskSizingSnapshot({
    signal,
    policyState,
    edgeQuality: edgeQualityWithCalibration,
    freshness,
  });
  const traderPlaybook = await deps.buildTraderPlaybookSnapshot(db, {
    signal,
    policyState,
    edgeQuality: edgeQualityWithCalibration,
    freshness,
    sizing: riskSizing,
  });
  const limitedScenarioSample = riskBand.d7.sample_size < 20 || riskBand.d30.sample_size < 20;
  const allocationTargetMismatch = Math.abs(traderPlaybook.recommended_size_pct.target - riskSizing.target_pct) > 0.5;
  const consistency = deps.buildConsistencySnapshot(policyState, {
    stale_count: stalePenaltyUnits,
    calibration_quality: edgeQualityWithCalibration.calibration.quality,
    limited_scenario_sample: limitedScenarioSample,
    conflict_state: policyState.conflict_state,
    allocation_target_mismatch: allocationTargetMismatch,
  });

  return {
    as_of: `${pxi.date}T00:00:00.000Z`,
    pxi,
    categories,
    signal,
    risk_sizing: riskSizing,
    regime,
    freshness,
    risk_band: riskBand,
    edge_quality: edgeQualityWithCalibration,
    policy_state: policyState,
    degraded_reasons: degradedReasons,
    uncertainty,
    consistency,
    trader_playbook: traderPlaybook,
  };
}

export async function tryHandleMarketCoreRoute(
  route: WorkerRouteContext,
  deps: MarketCoreDeps,
): Promise<Response | null> {
  const { env, url, method, corsHeaders } = route;

  if (url.pathname === '/api/pxi') {
    const selected = await selectLatestPxiWithCategories(env.DB);
    const pxi = selected.pxi;
    const categories = selected.categories;

    if (!pxi) {
      return Response.json({ error: 'No data' }, { status: 404, headers: corsHeaders });
    }

    const sparkResult = await env.DB.prepare(
      'SELECT date, score FROM pxi_scores WHERE date <= ? ORDER BY date DESC LIMIT 30'
    ).bind(pxi.date).all<SparklineRow>();

    const regime = await deps.detectRegime(env.DB, pxi.date);
    const divergence = await deps.detectDivergence(env.DB, pxi.score, regime);
    const freshnessDiagnostics = await deps.computeFreshnessDiagnostics(env.DB);
    const staleIndicators = freshnessDiagnostics.stale_indicators;
    const hasStaleData = freshnessDiagnostics.status.has_stale_data;
    const topOffenders = staleIndicators
      .map((stale: any) => {
        const stalePolicy = deps.resolveStalePolicy(
          stale.indicator_id,
          deps.INDICATOR_FREQUENCY_HINTS.get(stale.indicator_id) ?? null
        );
        return {
          id: stale.indicator_id,
          status: stale.status,
          critical: stale.critical,
          lastUpdate: stale.latest_date,
          daysOld: stale.days_old === null || !Number.isFinite(stale.days_old) ? null : Math.round(stale.days_old),
          maxAgeDays: stale.max_age_days,
          chronic: deps.isChronicStaleness(stale.days_old, stale.max_age_days),
          owner: stalePolicy.owner,
          escalation: stalePolicy.escalation,
          priorityScore: (stale.critical ? 1000 : 0) +
            (stale.status === 'missing' ? 500 : 0) +
            (stale.days_old === null || !Number.isFinite(stale.days_old) ? 250 : stale.days_old - stale.max_age_days),
        };
      })
      .sort((a: any, b: any) => b.priorityScore - a.priorityScore)
      .slice(0, 3)
      .map(({ priorityScore: _priorityScore, ...offender }: any) => offender);

    const latestRefreshResolver =
      deps.resolveLatestObservedRefreshTimestamp || deps.resolveLatestRefreshTimestamp;
    const latestRefresh = await latestRefreshResolver(env.DB);
    const nextRefresh = deps.computeNextExpectedRefresh(new Date());

    const payload: PXIResponsePayload = {
      date: pxi.date,
      score: pxi.score,
      label: pxi.label,
      status: pxi.status,
      delta: {
        d1: pxi.delta_1d,
        d7: pxi.delta_7d,
        d30: pxi.delta_30d,
      },
      categories: categories.map((category) => ({
        name: category.category,
        score: category.score,
        weight: category.weight,
      })),
      sparkline: (sparkResult.results || []).reverse().map((row) => ({
        date: row.date,
        score: row.score,
      })),
      regime: regime ? {
        type: regime.regime,
        confidence: regime.confidence,
        description: regime.description,
      } : null,
      divergence: divergence.has_divergence ? {
        alerts: divergence.alerts.map((alert: any) => ({
          ...alert,
          actionable: false,
          metrics: undefined,
        })),
      } : null,
      dataFreshness: {
        hasStaleData,
        staleCount: freshnessDiagnostics.status.stale_count,
        criticalStaleCount: freshnessDiagnostics.status.critical_stale_count,
        staleIndicators: hasStaleData ? staleIndicators.slice(0, 5).map((stale: any) => ({
          id: stale.indicator_id,
          status: stale.status,
          critical: stale.critical,
          lastUpdate: stale.latest_date,
          daysOld: stale.days_old === null || !Number.isFinite(stale.days_old) ? null : Math.round(stale.days_old),
          maxAgeDays: stale.max_age_days,
        })) : [],
        topOffenders,
        lastRefreshAtUtc: latestRefresh.last_refresh_at_utc,
        lastRefreshSource: latestRefresh.source,
        nextExpectedRefreshAtUtc: nextRefresh.at,
        nextExpectedRefreshInMinutes: nextRefresh.in_minutes,
      },
    };

    return Response.json(payload, {
      headers: {
        ...corsHeaders,
        'Cache-Control': 'public, max-age=60',
      },
    });
  }

  if (url.pathname === '/api/signal') {
    const selected = await selectLatestPxiWithCategories(env.DB);
    const pxi = selected.pxi;

    if (!pxi) {
      return Response.json({ error: 'No data' }, { status: 404, headers: corsHeaders });
    }

    const categories = selected.categories;

    const regime = await deps.detectRegime(env.DB, pxi.date);
    const signal = await deps.calculatePXISignal(
      env.DB,
      { score: pxi.score, delta_7d: pxi.delta_7d, delta_30d: pxi.delta_30d },
      regime,
      categories
    );

    const [divergence, freshness, mlSampleSize, edgeCalibrationSnapshot] = await Promise.all([
      deps.detectDivergence(env.DB, pxi.score, regime),
      deps.computeFreshnessStatus(env.DB),
      deps.fetchPredictionEvaluationSampleSize(env.DB),
      deps.fetchLatestCalibrationSnapshot(env.DB, 'edge_quality', null),
    ]);
    const conflictState = deps.resolveConflictState(regime, signal);
    const stalePenaltyUnits = deps.freshnessPenaltyCount(freshness);
    const edgeQuality = deps.computeEdgeQualitySnapshot({
      staleCount: stalePenaltyUnits,
      mlSampleSize,
      regime,
      conflictState,
      divergenceCount: divergence.alerts.length,
    });
    const edgeQualityWithCalibration = {
      ...edgeQuality,
      calibration: deps.buildEdgeQualityCalibrationFromSnapshot(edgeCalibrationSnapshot, edgeQuality.score),
    };
    let signalEdgeDiagnostics: unknown = null;
    if (deps.isFeatureEnabled(
      env,
      'FEATURE_ENABLE_EDGE_DIAGNOSTICS',
      'ENABLE_EDGE_DIAGNOSTICS',
      true,
    )) {
      try {
        signalEdgeDiagnostics = await deps.buildEdgeDiagnosticsReport(env.DB, ['7d']);
      } catch (err) {
        console.warn('Signal edge diagnostics unavailable; allocation is withheld:', err);
      }
    }
    const signalEdgeGate = resolveEdgeEvidenceGate(signalEdgeDiagnostics, '7d');
    // `/api/plan` is the sole action-authority surface. The standalone signal
    // remains descriptive even after its evidence window matures, avoiding a
    // second, weaker authorization policy.
    const signalActionAuthorized = false;
    const signalDecisionContract = buildDecisionContractSnapshot({
      actionability_state: 'NO_ACTION',
      action_authorized: signalActionAuthorized,
      actionability_reason_codes: signalEdgeGate.pass
        ? ['plan_authority_required']
        : ['edge_evidence_gate_block'],
      pxi_label: pxi.label,
      regime: regime?.regime || null,
      research_posture: signal.signal_type,
      evidence_gate: signalEdgeGate,
      consistency_state: 'FAIL',
      opportunity_cta_enabled: false,
      allocation_target: null,
      structural_quality: edgeQualityWithCalibration,
    });

    const payload: SignalResponsePayload = {
      date: pxi.date,
      decision_contract: signalDecisionContract,
      state: {
        score: pxi.score,
        label: pxi.label,
        status: pxi.status,
        delta: {
          d1: pxi.delta_1d,
          d7: pxi.delta_7d,
          d30: pxi.delta_30d,
        },
        categories: categories.map((category) => ({
          name: category.category,
          score: category.score,
          weight: category.weight,
        })),
      },
      signal: {
        type: signal.signal_type,
        action_authorized: signalActionAuthorized,
        risk_allocation: signalActionAuthorized ? signal.risk_allocation : null,
        raw_risk_allocation: signal.risk_allocation,
        evidence_gate: signalEdgeGate,
        volatility_percentile: signal.volatility_percentile,
        category_dispersion: signal.category_dispersion,
        adjustments: signal.adjustments,
        conflict_state: conflictState,
      },
      regime: regime ? {
        type: regime.regime,
        confidence: regime.confidence,
        description: regime.description,
      } : null,
      divergence: divergence.has_divergence ? {
        alerts: divergence.alerts.map((alert: any) => ({
          ...alert,
          actionable: false,
          metrics: undefined,
        })),
      } : null,
      edge_quality: {
        ...edgeQualityWithCalibration,
        diagnostic_scope: 'INPUT_AND_MODEL_STRUCTURE',
        validated_edge: signalEdgeGate.pass,
        calibration_authority: 'NON_AUTHORITATIVE_LEGACY_PREDICTION_LOG',
      },
      freshness_status: freshness,
    };

    return Response.json(payload, {
      headers: {
        ...corsHeaders,
        'Cache-Control': signalActionAuthorized ? 'public, max-age=60' : 'no-store',
      },
    });
  }

  if (url.pathname === '/api/plan' && method === 'GET') {
    if (!deps.isFeatureEnabled(env, 'FEATURE_ENABLE_PLAN', 'ENABLE_PLAN', true)) {
      return Response.json(deps.buildPlanFallbackPayload('feature_disabled'), {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'no-store',
        },
      });
    }

    let canonical: CanonicalMarketDecision | null = null;
    try {
      canonical = await buildCanonicalMarketDecision(env.DB, deps);
    } catch (err) {
      console.error('Failed to build canonical market decision:', err);
      return Response.json(deps.buildPlanFallbackPayload(resolvePlanFallbackReason(err)), {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'no-store',
        },
      });
    }

    const { pxi, signal, regime, freshness, risk_band, edge_quality, policy_state, degraded_reasons, risk_sizing } = canonical;
    const stalePenaltyUnits = deps.freshnessPenaltyCount(freshness);
    const setupContext = `PXI ${pxi.score.toFixed(1)} (${pxi.label}); ${policy_state.stance.replace('_', ' ')} stance with ${signal.signal_type.replace('_', ' ')} research posture.${
      stalePenaltyUnits > 0
        ? ` stale-input pressure ${stalePenaltyUnits} (critical stale: ${freshness.critical_stale_count}).`
        : ''
    }`;

    let briefRef: PlanPayload['brief_ref'] | undefined;
    let opportunityRef: PlanPayload['opportunity_ref'] | undefined;
    let alertsRef: PlanPayload['alerts_ref'] | undefined;
    let crossHorizonRef: PlanPayload['cross_horizon'] | undefined;
    const edgeEvidenceByHorizon: Record<'7d' | '30d', EdgeEvidenceGateResolution> = {
      '7d': resolveEdgeEvidenceGate(null, '7d'),
      '30d': resolveEdgeEvidenceGate(null, '30d'),
    };
    try {
      await deps.ensureMarketProductSchema(env.DB);

      const [briefRow, alertsCounts] = await Promise.all([
        env.DB.prepare(`
          SELECT payload_json
          FROM market_brief_snapshots
          ORDER BY as_of DESC
          LIMIT 1
        `).first<{ payload_json: string }>(),
        env.DB.prepare(`
          SELECT
            MAX(created_at) as latest_as_of,
            SUM(CASE WHEN severity = 'warning' THEN 1 ELSE 0 END) as warning_count,
            SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical_count
          FROM market_alert_events
          WHERE datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', '-24 hours')
        `).first<{
          latest_as_of: string | null;
          warning_count: number | null;
          critical_count: number | null;
        }>(),
      ]);

      if (briefRow?.payload_json) {
        try {
          const briefSnapshot = JSON.parse(briefRow.payload_json);
          if (deps.isBriefSnapshotCompatible(briefSnapshot)) {
            briefRef = {
              as_of: briefSnapshot.as_of,
              regime_delta: briefSnapshot.regime_delta,
              risk_posture: briefSnapshot.risk_posture,
            };
          }
        } catch {
          briefRef = undefined;
        }
      }

      const opportunityRows = await env.DB.prepare(`
        SELECT horizon, as_of, payload_json, created_at
        FROM opportunity_snapshots
        WHERE horizon IN ('7d', '30d')
          AND instr(payload_json, ?) = 0
          AND as_of = (
            SELECT MAX(s2.as_of)
            FROM opportunity_snapshots s2
            WHERE s2.horizon = opportunity_snapshots.horizon
              AND instr(s2.payload_json, ?) = 0
          )
      `).bind(
        HISTORICAL_BACKFILL_SEED_MARKER,
        HISTORICAL_BACKFILL_SEED_MARKER,
      ).all<{
        horizon: '7d' | '30d';
        as_of: string;
        payload_json: string;
        created_at: string | null;
      }>();

      const coherenceGateEnabled = deps.isFeatureEnabled(
        env,
        'FEATURE_ENABLE_OPPORTUNITY_COHERENCE_GATE',
        'ENABLE_OPPORTUNITY_COHERENCE_GATE',
        true,
      );
      let edgeDiagnosticsReport: unknown = null;
      if (deps.isFeatureEnabled(
        env,
        'FEATURE_ENABLE_EDGE_DIAGNOSTICS',
        'ENABLE_EDGE_DIAGNOSTICS',
        true,
      )) {
        try {
          edgeDiagnosticsReport = await deps.buildEdgeDiagnosticsReport(env.DB, ['7d', '30d']);
        } catch (err) {
          console.warn('Plan edge diagnostics unavailable; actionability is disabled:', err);
        }
      }
      edgeEvidenceByHorizon['7d'] = resolveEdgeEvidenceGate(edgeDiagnosticsReport, '7d');
      edgeEvidenceByHorizon['30d'] = resolveEdgeEvidenceGate(edgeDiagnosticsReport, '30d');
      const projectedByHorizon: Record<string, any> = {};
      const asOfByHorizon: Record<string, string> = {};
      const ctaByHorizon: Record<string, {
        cta_enabled: boolean;
        cta_disabled_reasons: string[];
        ttl_state: 'fresh' | 'stale' | 'overdue' | 'unknown';
      }> = {};

      for (const row of opportunityRows.results || []) {
        try {
          const opportunitySnapshot = parseProspectiveOpportunitySnapshot<OpportunitySnapshot>(row.payload_json);
          if (
            !opportunitySnapshot ||
            (opportunitySnapshot.horizon !== '7d' && opportunitySnapshot.horizon !== '30d') ||
            !Array.isArray(opportunitySnapshot.items)
          ) {
            continue;
          }

          const snapshotAsOf = typeof opportunitySnapshot.as_of === 'string'
            ? opportunitySnapshot.as_of
            : '';
          const snapshotDate = /^\d{4}-\d{2}-\d{2}(?:T|$)/.test(snapshotAsOf)
            ? snapshotAsOf.slice(0, 10)
            : null;
          const snapshotMatchesCanonicalPxi = snapshotDate === pxi.date;
          const ttlMetadata = typeof deps.computeOpportunityTtlMetadata === 'function'
            ? deps.computeOpportunityTtlMetadata(row.created_at || null, new Date())
            : {
                data_age_seconds: null,
                ttl_state: 'unknown' as const,
                next_expected_refresh_at: null,
                overdue_seconds: null,
              };

          const calibration = await deps.fetchLatestCalibrationSnapshot(
            env.DB,
            'conviction',
            opportunitySnapshot.horizon
          );
          const horizonEdgeGate = opportunitySnapshot.horizon === '7d'
            ? edgeEvidenceByHorizon['7d']
            : opportunitySnapshot.horizon === '30d'
              ? edgeEvidenceByHorizon['30d']
              : { pass: false, reasons: ['opportunity_horizon_invalid'] };
          const edgeGate = snapshotMatchesCanonicalPxi
            ? horizonEdgeGate
            : {
                pass: false,
                reasons: Array.from(new Set([
                  ...horizonEdgeGate.reasons,
                  'opportunity_snapshot_date_mismatch',
                ])),
              };
          edgeEvidenceByHorizon[opportunitySnapshot.horizon] = edgeGate;
          const authorityProjection = evaluateOpportunityAuthorityProjection({
            items: opportunitySnapshot.items,
            calibration,
            coherence_gate_enabled: coherenceGateEnabled,
            freshness,
            consistency_state: canonical.consistency.state,
            edge_evidence_gate: edgeGate,
            ttl: ttlMetadata,
          }, deps);

          projectedByHorizon[opportunitySnapshot.horizon] = authorityProjection.projected_feed;
          asOfByHorizon[opportunitySnapshot.horizon] = snapshotAsOf || canonical.as_of;
          ctaByHorizon[opportunitySnapshot.horizon] = {
            cta_enabled: authorityProjection.cta_state.cta_enabled,
            cta_disabled_reasons: authorityProjection.cta_state.cta_disabled_reasons,
            ttl_state: ttlMetadata.ttl_state,
          };
        } catch {
          // Skip malformed snapshot payloads.
        }
      }

      if (projectedByHorizon['7d']) {
        opportunityRef = {
          as_of: asOfByHorizon['7d'] || canonical.as_of,
          horizon: '7d',
          eligible_count: projectedByHorizon['7d'].items.length,
          suppressed_count: projectedByHorizon['7d'].suppressed_count,
          degraded_reason: projectedByHorizon['7d'].degraded_reason,
          cta_enabled: ctaByHorizon['7d']?.cta_enabled === true,
          cta_disabled_reasons: ctaByHorizon['7d']?.cta_disabled_reasons || ['authority_contract_unavailable'],
          ttl_state: ctaByHorizon['7d']?.ttl_state || 'unknown',
        };
      } else if (projectedByHorizon['30d']) {
        opportunityRef = {
          as_of: asOfByHorizon['30d'] || canonical.as_of,
          horizon: '30d',
          eligible_count: projectedByHorizon['30d'].items.length,
          suppressed_count: projectedByHorizon['30d'].suppressed_count,
          degraded_reason: projectedByHorizon['30d'].degraded_reason,
          cta_enabled: ctaByHorizon['30d']?.cta_enabled === true,
          cta_disabled_reasons: ctaByHorizon['30d']?.cta_disabled_reasons || ['authority_contract_unavailable'],
          ttl_state: ctaByHorizon['30d']?.ttl_state || 'unknown',
        };
      }

      crossHorizonRef = deps.summarizeCrossHorizonCoherence({
        projected_7d: projectedByHorizon['7d'] || null,
        projected_30d: projectedByHorizon['30d'] || null,
        as_of_7d: asOfByHorizon['7d'] || null,
        as_of_30d: asOfByHorizon['30d'] || null,
      }) || undefined;

      alertsRef = {
        as_of: alertsCounts?.latest_as_of || canonical.as_of,
        warning_count_24h: Math.max(0, Math.floor(deps.toNumber(alertsCounts?.warning_count, 0))),
        critical_count_24h: Math.max(0, Math.floor(deps.toNumber(alertsCounts?.critical_count, 0))),
      };
    } catch (err) {
      console.warn('Failed to attach plan reference blocks:', err);
    }

    const selectedEdgeEvidenceGate = opportunityRef
      ? edgeEvidenceByHorizon[opportunityRef.horizon]
      : { pass: false, reasons: ['opportunity_reference_unavailable'] };
    const actionability = deps.resolvePlanActionability({
      opportunity_ref: opportunityRef,
      edge_quality,
      freshness,
      consistency: canonical.consistency,
      edge_evidence_gate: selectedEdgeEvidenceGate,
    });
    const actionabilityWithCrossHorizon = deps.applyCrossHorizonActionabilityOverride(actionability, crossHorizonRef || null);
    const actionAuthorized = selectedEdgeEvidenceGate.pass &&
      opportunityRef?.cta_enabled === true &&
      canonical.consistency.state !== 'FAIL' &&
      actionabilityWithCrossHorizon.state === 'ACTIONABLE' &&
      Number.isFinite(risk_sizing.target_pct);
    const decisionContract = buildDecisionContractSnapshot({
      actionability_state: actionabilityWithCrossHorizon.state,
      action_authorized: actionAuthorized,
      actionability_reason_codes: actionabilityWithCrossHorizon.reason_codes,
      pxi_label: pxi.label,
      regime: regime?.regime || null,
      research_posture: signal.signal_type,
      evidence_gate: selectedEdgeEvidenceGate,
      consistency_state: canonical.consistency.state,
      opportunity_cta_enabled: opportunityRef?.cta_enabled === true,
      allocation_target: Number.isFinite(risk_sizing.target_pct)
        ? risk_sizing.target_pct / 100
        : null,
      structural_quality: edge_quality,
    });
    const setupSummary = actionAuthorized
      ? `${setupContext} Authorized target ${risk_sizing.target_pct}% under the prospective evidence and actionability gates.`
      : `NO ACTION — ${setupContext} Allocation withheld until the prospective evidence and actionability gates pass.`;
    const decisionStack = deps.buildDecisionStack({
      actionability_state: actionabilityWithCrossHorizon.state,
      action_authorized: actionAuthorized,
      actionability_reason_codes: actionabilityWithCrossHorizon.reason_codes,
      edge_evidence_gate: selectedEdgeEvidenceGate,
      setup_summary: setupSummary,
      edge_quality,
      consistency: canonical.consistency,
      opportunity_ref: opportunityRef,
      brief_ref: briefRef,
      alerts_ref: alertsRef,
      cross_horizon: crossHorizonRef,
    });
    const invalidationRules = deps.buildInvalidationRules({
      pxi,
      freshness,
      regime,
      edgeQuality: edge_quality,
    });
    if (crossHorizonRef?.invalidation_note) {
      invalidationRules.unshift(crossHorizonRef.invalidation_note);
    }
    const finalInvalidationRules = Array.from(new Set(invalidationRules)) as string[];

    const payload: PlanPayload = {
      as_of: canonical.as_of,
      setup_summary: setupSummary,
      decision_contract: decisionContract,
      policy_state,
      edge_evidence_gate: selectedEdgeEvidenceGate,
      actionability_state: decisionContract.actionability_state,
      actionability_reason_codes: decisionContract.actionability_reason_codes,
      action_now: {
        action_authorized: decisionContract.action_authorized,
        risk_allocation_target: decisionContract.action_authorized ? risk_sizing.target_pct / 100 : null,
        raw_signal_allocation_target: risk_sizing.raw_signal_allocation_target,
        risk_allocation_basis: decisionContract.action_authorized
          ? 'penalized_playbook_target'
          : selectedEdgeEvidenceGate.pass
            ? 'withheld_actionability'
            : 'withheld_edge_evidence',
        horizon_bias: deps.resolveHorizonBias(signal, regime, edge_quality.score),
        primary_signal: signal.signal_type,
      },
      edge_quality: {
        ...edge_quality,
        diagnostic_scope: 'INPUT_AND_MODEL_STRUCTURE',
        validated_edge: selectedEdgeEvidenceGate.pass,
        calibration_authority: 'NON_AUTHORITATIVE_LEGACY_PREDICTION_LOG',
      },
      risk_band,
      uncertainty: canonical.uncertainty,
      consistency: canonical.consistency,
      trader_playbook: projectTraderPlaybookForDecision(canonical.trader_playbook, decisionContract.action_authorized),
      invalidation_rules: finalInvalidationRules,
      ...(briefRef ? { brief_ref: briefRef } : {}),
      ...(opportunityRef ? { opportunity_ref: opportunityRef } : {}),
      ...(alertsRef ? { alerts_ref: alertsRef } : {}),
      ...(crossHorizonRef ? { cross_horizon: crossHorizonRef } : {}),
      decision_stack: decisionStack,
      degraded_reason: degraded_reasons.length > 0 ? degraded_reasons.join(',') : null,
    };

    return Response.json(payload, {
      headers: {
        ...corsHeaders,
        'Cache-Control': 'no-store',
      },
    });
  }

  if (url.pathname === '/api/market/consistency' && method === 'GET') {
    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Consistency schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    try {
      const canonical = await buildCanonicalMarketDecision(env.DB, deps);
      return Response.json({
        as_of: canonical.as_of,
        score: canonical.consistency.score,
        state: canonical.consistency.state,
        violations: canonical.consistency.violations,
        components: canonical.consistency.components,
        scope: 'INTERNAL_COHERENCE',
        note: 'A PASS means the descriptive fields agree internally; it does not authorize action or validate prospective edge.',
        source: 'CURRENT_CANONICAL_STATE',
        created_at: deps.asIsoDateTime(new Date()),
      }, { headers: corsHeaders });
    } catch (err) {
      console.warn('Current consistency computation unavailable; attempting stored fallback:', err);
    }

    const latest = await deps.fetchLatestConsistencyCheck(env.DB);
    if (latest) {
      return Response.json({
        as_of: latest.as_of,
        score: latest.score,
        state: latest.state,
        violations: latest.violations,
        components: latest.components,
        scope: 'INTERNAL_COHERENCE',
        note: 'Stored fallback only. A PASS means the descriptive fields agreed internally at the stated time; it does not authorize action or validate prospective edge.',
        source: 'STORED_FALLBACK',
        created_at: latest.created_at,
      }, { headers: corsHeaders });
    }

    return Response.json({ error: 'Consistency unavailable' }, { status: 503, headers: corsHeaders });
  }

  if (url.pathname === '/api/ops/freshness-slo' && method === 'GET') {
    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Freshness SLO schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    try {
      const [window7d, window30d] = await Promise.all([
        deps.computeFreshnessSloWindow(env.DB, 7),
        deps.computeFreshnessSloWindow(env.DB, 30),
      ]);

      return Response.json({
        as_of: deps.asIsoDateTime(new Date()),
        windows: {
          '7d': window7d,
          '30d': window30d,
        },
      }, {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'public, max-age=300',
        },
      });
    } catch (err) {
      console.error('Freshness SLO computation failed:', err);
      return Response.json({ error: 'Freshness SLO unavailable' }, { status: 503, headers: corsHeaders });
    }
  }

  return null;
}
