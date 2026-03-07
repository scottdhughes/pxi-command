import { buildCanonicalMarketDecision } from '../domain/market-core';
import { generateMarketEvents } from '../domain/market-products';
import {
  buildDecisionImpactOpsResponse,
  computeDecisionGradeScorecard,
  computeDecisionImpact,
} from '../domain/market-ops';
import type {
  BackfillProductsResponsePayload,
  BriefSnapshot,
  MarketAlertEvent,
  MarketAlertType,
  OpportunitySnapshot,
  RefreshProductsCompletedResponsePayload,
  RefreshProductsResponsePayload,
  SendDigestResponsePayload,
  UtilityEventResponsePayload,
  UtilityFunnelResponsePayload,
  WorkerRouteContext,
} from '../types';

type MarketLifecycleDeps = Record<string, any>;

const MAX_BACKFILL_LIMIT = 365;
const UTILITY_WINDOW_DAY_OPTIONS = new Set<number>([7, 30]);
const VALID_ALERT_TYPES: MarketAlertType[] = ['regime_change', 'threshold_cross', 'opportunity_spike', 'freshness_warning'];

export async function tryHandleMarketLifecycleRoute(
  route: WorkerRouteContext,
  deps: MarketLifecycleDeps,
): Promise<Response | null> {
  const { request, env, url, method, corsHeaders, clientIP } = route;

  if (url.pathname === '/api/metrics/utility-event' && method === 'POST') {
    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Utility-event schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    const body = await deps.parseJsonBody(request) as {
      session_id?: unknown;
      event_type?: unknown;
      route?: unknown;
      actionability_state?: unknown;
      metadata?: unknown;
    } | null;

    const sessionId = deps.normalizeUtilitySessionId(body?.session_id);
    if (!sessionId) {
      return Response.json({ error: 'Invalid session_id' }, { status: 400, headers: corsHeaders });
    }

    const eventType = deps.normalizeUtilityEventType(body?.event_type);
    if (!eventType) {
      return Response.json({ error: 'Invalid event_type' }, { status: 400, headers: corsHeaders });
    }

    const routeName = deps.normalizeUtilityRoute(body?.route);
    const actionabilityState = deps.normalizeUtilityActionabilityState(body?.actionability_state);
    const payloadJson = deps.sanitizeUtilityPayload(body?.metadata);
    const createdAt = deps.asIsoDateTime(new Date());
    const ctaIntentEnabled = deps.isFeatureEnabled(
      env,
      'FEATURE_ENABLE_CTA_INTENT_TRACKING',
      'ENABLE_CTA_INTENT_TRACKING',
      true,
    );

    if (eventType === 'cta_action_click' && !ctaIntentEnabled) {
      const payload: UtilityEventResponsePayload = {
        ok: true,
        stored: false,
        ignored_reason: 'cta_intent_tracking_disabled',
        accepted: {
          event_type: eventType,
          route: routeName,
          actionability_state: actionabilityState,
        },
      };

      return Response.json(payload, {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'no-store',
        },
      });
    }

    try {
      await deps.insertUtilityEvent(env.DB, {
        session_id: sessionId,
        event_type: eventType,
        route: routeName,
        actionability_state: actionabilityState,
        payload_json: payloadJson,
        created_at: createdAt,
      });
    } catch (err) {
      console.error('Utility-event insert failed:', err);
      return Response.json({ error: 'Failed to record utility event' }, { status: 503, headers: corsHeaders });
    }

    const payload: UtilityEventResponsePayload = {
      ok: true,
      accepted: {
        event_type: eventType,
        route: routeName,
        actionability_state: actionabilityState,
      },
    };

    return Response.json(payload, {
      headers: {
        ...corsHeaders,
        'Cache-Control': 'no-store',
      },
    });
  }

  if (url.pathname === '/api/ops/utility-funnel' && method === 'GET') {
    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Utility-funnel schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    const windowRaw = Number.parseInt((url.searchParams.get('window') || '7').trim(), 10);
    const windowDays = UTILITY_WINDOW_DAY_OPTIONS.has(windowRaw) ? windowRaw : 7;

    try {
      const funnel = await deps.computeUtilityFunnelSummary(env.DB, windowDays);
      const payload: UtilityFunnelResponsePayload = {
        as_of: deps.asIsoDateTime(new Date()),
        funnel,
      };
      return Response.json(payload, {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'public, max-age=120',
        },
      });
    } catch (err) {
      console.error('Utility-funnel computation failed:', err);
      return Response.json({ error: 'Utility funnel unavailable' }, { status: 503, headers: corsHeaders });
    }
  }

  if (url.pathname === '/api/market/refresh-products' && method === 'POST') {
    const adminAuthFailure = await deps.enforceAdminAuth(request, env, corsHeaders, clientIP);
    if (adminAuthFailure) {
      return adminAuthFailure;
    }

    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Refresh-products schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    const briefEnabled = deps.isFeatureEnabled(env, 'FEATURE_ENABLE_BRIEF', 'ENABLE_BRIEF', true);
    const opportunitiesEnabled = deps.isFeatureEnabled(env, 'FEATURE_ENABLE_OPPORTUNITIES', 'ENABLE_OPPORTUNITIES', true);
    const inAppAlertsEnabled = deps.isFeatureEnabled(env, 'FEATURE_ENABLE_ALERTS_IN_APP', 'ENABLE_ALERTS_IN_APP', true);
    const coherenceGateEnabled = deps.isFeatureEnabled(
      env,
      'FEATURE_ENABLE_OPPORTUNITY_COHERENCE_GATE',
      'ENABLE_OPPORTUNITY_COHERENCE_GATE',
      true,
    );
    const signalsSanitizerEnabled = deps.isFeatureEnabled(
      env,
      'FEATURE_ENABLE_SIGNALS_SANITIZER',
      'ENABLE_SIGNALS_SANITIZER',
      true,
    );
    const calibrationDiagnosticsEnabled = deps.isFeatureEnabled(
      env,
      'FEATURE_ENABLE_CALIBRATION_DIAGNOSTICS',
      'ENABLE_CALIBRATION_DIAGNOSTICS',
      true,
    );
    const edgeDiagnosticsEnabled = deps.isFeatureEnabled(
      env,
      'FEATURE_ENABLE_EDGE_DIAGNOSTICS',
      'ENABLE_EDGE_DIAGNOSTICS',
      true,
    );
    const decisionImpactEnabled = deps.isFeatureEnabled(
      env,
      'FEATURE_ENABLE_DECISION_IMPACT',
      'ENABLE_DECISION_IMPACT',
      true,
    );
    const decisionImpactGovernance = deps.resolveDecisionImpactGovernance(env);
    const triggerHeader = request.headers.get('X-Refresh-Trigger');
    const refreshTrigger = triggerHeader && triggerHeader.trim().length > 0
      ? triggerHeader.trim().slice(0, 128)
      : 'unknown';
    const refreshClaim = await deps.claimMarketRefreshRun(env.DB, refreshTrigger);
    if (refreshClaim.status === 'skipped') {
      const payload: RefreshProductsResponsePayload = {
        ok: true,
        skipped: true,
        publication_status: 'skipped',
        reason: 'refresh_in_progress',
        refresh_run_id: refreshClaim.run_id,
        refresh_trigger: refreshClaim.refresh_trigger,
      };
      return Response.json(payload, { headers: corsHeaders });
    }
    const refreshRunId = refreshClaim.run_id;

    try {
      let edgeDiagnosticsReport: any = null;
      if (edgeDiagnosticsEnabled) {
        edgeDiagnosticsReport = await deps.buildEdgeDiagnosticsReport(env.DB, ['7d', '30d']);
        if (!edgeDiagnosticsReport.promotion_gate.pass) {
          throw new Error(`promotion_gate_failed:${edgeDiagnosticsReport.promotion_gate.reasons.join(',')}`);
        }
      }

      let edgeCalibrationSnapshot: any = null;
      let convictionCalibration7d: any = null;
      let convictionCalibration30d: any = null;
      let calibrationsGenerated = 0;

      try {
        edgeCalibrationSnapshot = await deps.buildEdgeQualityCalibrationSnapshot(env.DB);
        await deps.storeCalibrationSnapshot(env.DB, edgeCalibrationSnapshot);
        calibrationsGenerated += 1;
      } catch (err) {
        console.error('Edge calibration refresh failed:', err);
        edgeCalibrationSnapshot = null;
      }

      if (opportunitiesEnabled) {
        try {
          convictionCalibration7d = await deps.buildConvictionCalibrationSnapshot(env.DB, '7d');
          await deps.storeCalibrationSnapshot(env.DB, convictionCalibration7d);
          calibrationsGenerated += 1;
        } catch (err) {
          console.error('7d conviction calibration refresh failed:', err);
          convictionCalibration7d = null;
        }

        try {
          convictionCalibration30d = await deps.buildConvictionCalibrationSnapshot(env.DB, '30d');
          await deps.storeCalibrationSnapshot(env.DB, convictionCalibration30d);
          calibrationsGenerated += 1;
        } catch (err) {
          console.error('30d conviction calibration refresh failed:', err);
          convictionCalibration30d = null;
        }
      }

      let brief: BriefSnapshot | null = null;
      let consistencyStored = false;
      if (briefEnabled) {
        brief = await deps.buildBriefSnapshot(env.DB);
        if (brief) {
          await deps.storeConsistencyCheck(env.DB, brief.source_plan_as_of, brief.consistency);
          consistencyStored = true;
        }
      }

      if (!consistencyStored) {
        try {
          const canonical = await buildCanonicalMarketDecision(env.DB, deps);
          await deps.storeConsistencyCheck(env.DB, canonical.as_of, canonical.consistency);
          consistencyStored = true;
        } catch (err) {
          console.error('Consistency snapshot fallback store failed:', err);
        }
      }

      let opportunities7d: OpportunitySnapshot | null = null;
      let opportunities30d: OpportunitySnapshot | null = null;
      if (opportunitiesEnabled) {
        opportunities7d = await deps.buildOpportunitySnapshot(env.DB, '7d', convictionCalibration7d, {
          sanitize_signals_tickers: signalsSanitizerEnabled,
        });
        opportunities30d = await deps.buildOpportunitySnapshot(env.DB, '30d', convictionCalibration30d, {
          sanitize_signals_tickers: signalsSanitizerEnabled,
        });
      }

      const freshnessForRun = brief?.freshness_status || await deps.computeFreshnessStatus(env.DB);
      let consistencyStateForRun: RefreshProductsCompletedResponsePayload['consistency_state'] = brief?.consistency.state || 'PASS';
      if (!brief) {
        const latestConsistency = await deps.fetchLatestConsistencyCheck(env.DB);
        if (latestConsistency) {
          consistencyStateForRun = latestConsistency.state;
        } else {
          try {
            consistencyStateForRun = (await buildCanonicalMarketDecision(env.DB, deps)).consistency.state;
          } catch {
            consistencyStateForRun = 'PASS';
          }
        }
      }

      let decisionImpactSnapshotsGenerated = 0;
      let decisionImpactSummary: RefreshProductsCompletedResponsePayload['decision_impact'] = null;
      let decisionImpactGenerationError: string | null = null;
      let decisionImpactEnforcementError: string | null = null;
      if (decisionImpactEnabled) {
        try {
          const asOfFallback = brief?.as_of || opportunities7d?.as_of || opportunities30d?.as_of || deps.asIsoDateTime(new Date());
          const asOfByHorizon: Record<'7d' | '30d', string> = {
            '7d': opportunities7d?.as_of || asOfFallback,
            '30d': opportunities30d?.as_of || opportunities7d?.as_of || asOfFallback,
          };
          for (const horizon of ['7d', '30d'] as const) {
            for (const scope of ['market', 'theme'] as const) {
              for (const windowDays of [30, 90] as const) {
                const snapshot = await computeDecisionImpact(env.DB, deps, {
                  horizon,
                  scope,
                  window_days: windowDays,
                  limit: scope === 'theme' ? 50 : 10,
                  as_of: asOfByHorizon[horizon],
                });
                await deps.storeDecisionImpactSnapshot(env.DB, snapshot);
                decisionImpactSnapshotsGenerated += 1;
              }
            }
          }

          const opsDecisionImpact = await buildDecisionImpactOpsResponse(env.DB, deps, 30, decisionImpactGovernance);
          decisionImpactSummary = {
            market_7d_hit_rate: opsDecisionImpact.market_7d.hit_rate,
            market_7d_sample_size: opsDecisionImpact.market_7d.sample_size,
            market_30d_hit_rate: opsDecisionImpact.market_30d.hit_rate,
            market_30d_sample_size: opsDecisionImpact.market_30d.sample_size,
            actionable_sessions: opsDecisionImpact.utility_attribution.actionable_sessions,
            cta_action_rate_pct: opsDecisionImpact.utility_attribution.cta_action_rate_pct,
            governance_mode: opsDecisionImpact.observe_mode.mode,
            enforce_ready: opsDecisionImpact.observe_mode.enforce_ready,
            enforce_breach_count: opsDecisionImpact.observe_mode.enforce_breach_count,
            enforce_breaches: opsDecisionImpact.observe_mode.enforce_breaches,
            minimum_samples_required: opsDecisionImpact.observe_mode.minimum_samples_required,
            minimum_actionable_sessions_required: opsDecisionImpact.observe_mode.minimum_actionable_sessions_required,
            observe_breach_count: opsDecisionImpact.observe_mode.breach_count,
            observe_breaches: opsDecisionImpact.observe_mode.breaches,
          };
          if (
            decisionImpactGovernance.enforce_enabled &&
            opsDecisionImpact.observe_mode.enforce_ready &&
            opsDecisionImpact.observe_mode.enforce_breach_count > 0
          ) {
            decisionImpactEnforcementError = `decision_impact_enforcement_failed:${opsDecisionImpact.observe_mode.enforce_breaches.join(',')}`;
          }
        } catch (err) {
          decisionImpactGenerationError = err instanceof Error ? err.message : String(err);
          console.error('Decision impact snapshot generation failed:', err);
        }
      }

      const governanceBreaches = decisionImpactSummary?.enforce_breaches || [];
      const publicationBlocked = Boolean(decisionImpactEnforcementError);
      const publicationBlockedReason = publicationBlocked ? 'governance_blocked' : null;

      let qualityFilteredCount = 0;
      let coherenceSuppressedCount = 0;
      let suppressedDataQualityCount = 0;
      const ledgerRows: any[] = [];
      const itemLedgerRows: any[] = [];
      const projectionTargets = [
        { snapshot: opportunities7d, calibration: convictionCalibration7d },
        { snapshot: opportunities30d, calibration: convictionCalibration30d },
      ];
      for (const target of projectionTargets) {
        if (!target.snapshot) continue;
        const ledgerBuild = deps.buildOpportunityLedgerProjection({
          refresh_run_id: refreshRunId,
          snapshot: target.snapshot,
          calibration: target.calibration,
          coherence_gate_enabled: coherenceGateEnabled,
          freshness: freshnessForRun,
          consistency_state: consistencyStateForRun,
          publication_allowed: !publicationBlocked,
          publication_blocked_reason: publicationBlockedReason,
        });
        qualityFilteredCount += ledgerBuild.projected.quality_filtered_count;
        coherenceSuppressedCount += ledgerBuild.projected.coherence_suppressed_count;
        suppressedDataQualityCount += ledgerBuild.projected.suppression_by_reason.data_quality_suppressed;
        ledgerRows.push(ledgerBuild.ledger_row);
        itemLedgerRows.push(...ledgerBuild.item_rows);
      }
      for (const row of ledgerRows) {
        try {
          await deps.insertOpportunityLedgerRow(env.DB, row);
        } catch (err) {
          console.error('Opportunity ledger insert failed:', err);
        }
      }
      for (const row of itemLedgerRows) {
        try {
          await deps.insertOpportunityItemLedgerRow(env.DB, row);
        } catch (err) {
          console.error('Opportunity item ledger insert failed:', err);
        }
      }
      const overSuppressedCount = ledgerRows.filter((row) =>
        row.candidate_count > 0 &&
        row.published_count === 0 &&
        row.data_quality_suppressed_count === 0,
      ).length;
      const horizon7 = ledgerRows.find((row) => row.horizon === '7d') || null;
      const horizon30 = ledgerRows.find((row) => row.horizon === '30d') || null;
      const crossHorizonState: RefreshProductsCompletedResponsePayload['cross_horizon_state'] = (
        horizon7 &&
        horizon30 &&
        horizon7.published_count > 0 &&
        horizon30.published_count > 0 &&
        horizon7.top_direction_published &&
        horizon30.top_direction_published
      )
        ? (horizon7.top_direction_published === horizon30.top_direction_published ? 'ALIGNED' : 'CONFLICT')
        : (
          horizon7 &&
          horizon30 &&
          horizon7.published_count === 0 &&
          horizon30.published_count === 0
        )
          ? 'INSUFFICIENT'
          : (
            horizon7 || horizon30
          )
            ? 'MIXED'
            : 'INSUFFICIENT';

      let decisionGradeSnapshot: RefreshProductsCompletedResponsePayload['decision_grade_snapshot'] = null;
      try {
        const scorecard = await computeDecisionGradeScorecard(
          env.DB,
          deps,
          30,
          decisionImpactGovernance,
        );
        decisionGradeSnapshot = {
          score: scorecard.score,
          grade: scorecard.grade,
          go_live_ready: scorecard.go_live_ready,
          go_live_blockers: scorecard.go_live_blockers,
          readiness: scorecard.readiness,
          opportunity_hygiene: {
            over_suppression_rate_pct: scorecard.components.opportunity_hygiene.over_suppression_rate_pct,
            cross_horizon_conflict_rate_pct: scorecard.components.opportunity_hygiene.cross_horizon_conflict_rate_pct,
            conflict_persistence_days: scorecard.components.opportunity_hygiene.conflict_persistence_days,
          },
        };
      } catch (err) {
        console.warn('Decision-grade snapshot unavailable during refresh-products:', err);
      }

      let diagnosticsSummary: RefreshProductsCompletedResponsePayload['calibration_diagnostics'] = null;
      if (calibrationDiagnosticsEnabled) {
        diagnosticsSummary = {
          edge_quality: deps.computeCalibrationDiagnostics(edgeCalibrationSnapshot).quality_band,
          conviction_7d: deps.computeCalibrationDiagnostics(convictionCalibration7d).quality_band,
          conviction_30d: deps.computeCalibrationDiagnostics(convictionCalibration30d).quality_band,
        };
      }

      const edgeDiagnosticsSummary = edgeDiagnosticsReport
        ? {
            as_of: edgeDiagnosticsReport.as_of,
            promotion_gate: edgeDiagnosticsReport.promotion_gate,
            windows: edgeDiagnosticsReport.windows.map((window: any) => ({
              horizon: window.horizon,
              sample_size: window.sample_size,
              model_direction_accuracy: window.model_direction_accuracy,
              baseline_direction_accuracy: window.baseline_direction_accuracy,
              uplift_vs_baseline: window.uplift_vs_baseline,
              uplift_ci95_low: window.uplift_ci95_low,
              uplift_ci95_high: window.uplift_ci95_high,
              lower_bound_positive: window.lower_bound_positive,
              leakage_sentinel: window.leakage_sentinel,
              quality_band: window.quality_band,
            })),
          }
        : null;
      const asOfForRun = brief?.as_of || opportunities7d?.as_of || opportunities30d?.as_of || null;
      const basePayload = {
        ok: true as const,
        consistency_stored: consistencyStored ? 1 : 0,
        consistency_state: consistencyStateForRun,
        consistency_score: brief?.consistency.score ?? null,
        as_of: asOfForRun,
        stale_count: freshnessForRun.stale_count,
        critical_stale_count: freshnessForRun.critical_stale_count,
        quality_filtered_count: qualityFilteredCount,
        coherence_suppressed_count: coherenceSuppressedCount,
        suppressed_data_quality_count: suppressedDataQualityCount,
        over_suppressed_count: overSuppressedCount,
        cross_horizon_state: crossHorizonState,
        opportunity_ledger_rows: ledgerRows.length,
        opportunity_item_ledger_rows: itemLedgerRows.length,
        decision_impact_snapshots_generated: decisionImpactSnapshotsGenerated,
        decision_impact: decisionImpactSummary,
        decision_impact_error: decisionImpactGenerationError,
        calibration_diagnostics: diagnosticsSummary,
        decision_grade_snapshot: decisionGradeSnapshot,
        edge_diagnostics: edgeDiagnosticsSummary,
        refresh_trigger: refreshTrigger,
        refresh_run_id: refreshRunId,
      };

      if (publicationBlocked) {
        await deps.recordMarketRefreshRunFinish(env.DB, refreshRunId, {
          status: 'blocked',
          brief_generated: 0,
          opportunities_generated: 0,
          calibrations_generated: calibrationsGenerated,
          alerts_generated: 0,
          stale_count: freshnessForRun.stale_count,
          critical_stale_count: freshnessForRun.critical_stale_count,
          as_of: asOfForRun,
          error: decisionImpactEnforcementError?.slice(0, 1000) || 'decision_impact_enforcement_failed',
        });

        const payload: RefreshProductsResponsePayload = {
          ...basePayload,
          blocked: true,
          reason: 'decision_impact_enforcement_failed',
          publication_status: 'blocked',
          governance_breaches: governanceBreaches,
          brief_generated: 0,
          opportunities_generated: 0,
          calibrations_generated: calibrationsGenerated,
          alerts_generated: 0,
        };
        return Response.json(payload, { headers: corsHeaders });
      }

      if (brief) {
        await deps.storeBriefSnapshot(env.DB, brief);
      }
      if (opportunities7d) {
        await deps.storeOpportunitySnapshot(env.DB, opportunities7d);
      }
      if (opportunities30d) {
        await deps.storeOpportunitySnapshot(env.DB, opportunities30d);
      }

      let alertsGenerated = 0;
      if (brief && opportunities7d) {
        const projectedForAlerts = deps.projectOpportunityFeed(
          deps.normalizeOpportunityItemsForPublishing(opportunities7d.items, convictionCalibration7d),
          {
            coherence_gate_enabled: coherenceGateEnabled,
            freshness: brief.freshness_status,
            consistency_state: brief.consistency.state,
          },
        );
        const generated = await generateMarketEvents(env.DB, deps, brief, {
          ...opportunities7d,
          items: projectedForAlerts.items,
        });
        alertsGenerated = await deps.insertMarketEvents(env.DB, generated, inAppAlertsEnabled);
      }

      await deps.recordMarketRefreshRunFinish(env.DB, refreshRunId, {
        status: 'success',
        brief_generated: brief ? 1 : 0,
        opportunities_generated: (opportunities7d ? 1 : 0) + (opportunities30d ? 1 : 0),
        calibrations_generated: calibrationsGenerated,
        alerts_generated: alertsGenerated,
        stale_count: freshnessForRun.stale_count,
        critical_stale_count: freshnessForRun.critical_stale_count,
        as_of: asOfForRun,
        error: null,
      });

      const payload: RefreshProductsCompletedResponsePayload = {
        ...basePayload,
        publication_status: 'published',
        brief_generated: brief ? 1 : 0,
        opportunities_generated: (opportunities7d ? 1 : 0) + (opportunities30d ? 1 : 0),
        calibrations_generated: calibrationsGenerated,
        alerts_generated: alertsGenerated,
      };

      return Response.json(payload, { headers: corsHeaders });
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : String(err);
      await deps.recordMarketRefreshRunFinish(env.DB, refreshRunId, {
        status: 'failed',
        error: errorMessage.slice(0, 1000),
      });
      console.error('Refresh-products failed:', err);
      return Response.json({ error: 'Refresh products failed', detail: errorMessage }, { status: 500, headers: corsHeaders });
    }
  }

  if (url.pathname === '/api/market/backfill-products' && method === 'POST') {
    const adminAuthFailure = await deps.enforceAdminAuth(request, env, corsHeaders, clientIP);
    if (adminAuthFailure) {
      return adminAuthFailure;
    }

    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Backfill-products schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    const signalsSanitizerEnabled = deps.isFeatureEnabled(
      env,
      'FEATURE_ENABLE_SIGNALS_SANITIZER',
      'ENABLE_SIGNALS_SANITIZER',
      true,
    );

    let body: {
      start?: string;
      end?: string;
      limit?: number;
      overwrite?: boolean;
      dry_run?: boolean;
      recalibrate?: boolean;
      rebuild_ledgers?: boolean;
    } = {};
    try {
      body = await request.json() as typeof body;
    } catch {
      body = {};
    }

    let dateFilter: { start: string | null; end: string | null };
    try {
      dateFilter = deps.parseBackfillDateRange(body.start, body.end);
    } catch (err) {
      return Response.json({
        error: err instanceof Error ? err.message : 'Invalid date range',
      }, { status: 400, headers: corsHeaders });
    }

    const limit = deps.parseBackfillLimit(body.limit);
    const today = deps.asIsoDate(new Date());
    const startDate = dateFilter.start || deps.addCalendarDays(today, -540);
    const endDate = dateFilter.end || today;
    const overwrite = body.overwrite === true;
    const dryRun = body.dry_run === true;
    const recalibrate = body.recalibrate !== false;
    const rebuildLedgers = body.rebuild_ledgers !== false;

    const dateRows = await env.DB.prepare(`
      SELECT p.date as date, COUNT(c.category) as category_count
      FROM pxi_scores p
      JOIN category_scores c ON c.date = p.date
      WHERE p.date >= ?
        AND p.date <= ?
      GROUP BY p.date
      HAVING COUNT(c.category) >= 3
      ORDER BY p.date DESC
      LIMIT ?
    `).bind(startDate, endDate, limit).all<{ date: string; category_count: number }>();

    const candidateDates = (dateRows.results || []).map((row) => row.date);
    const existingByDate = new Map<string, Set<'7d' | '30d'>>();
    if (!overwrite && candidateDates.length > 0) {
      const asOfDates = candidateDates.map((date) => `${date}T00:00:00.000Z`);
      const placeholders = asOfDates.map(() => '?').join(',');
      const existingRows = await env.DB.prepare(`
        SELECT as_of, horizon
        FROM opportunity_snapshots
        WHERE as_of IN (${placeholders})
          AND horizon IN ('7d', '30d')
      `).bind(...asOfDates).all<{ as_of: string; horizon: '7d' | '30d' }>();

      for (const row of existingRows.results || []) {
        const date = row.as_of.slice(0, 10);
        const set = existingByDate.get(date) || new Set<'7d' | '30d'>();
        set.add(row.horizon);
        existingByDate.set(date, set);
      }
    }

    const processDates = [...candidateDates].sort((a, b) => a.localeCompare(b));
    let seededSnapshots = 0;
    let processedDates = 0;
    let skippedDates = 0;
    const skippedExisting: string[] = [];
    const failedDates: Array<{ date: string; error: string }> = [];

    for (const date of processDates) {
      const existing = existingByDate.get(date);
      if (!overwrite && existing?.has('7d') && existing?.has('30d')) {
        skippedDates += 1;
        if (skippedExisting.length < 20) {
          skippedExisting.push(date);
        }
        continue;
      }

      processedDates += 1;
      try {
        const [snapshot7d, snapshot30d] = await Promise.all([
          deps.buildHistoricalOpportunitySnapshot(env.DB, date, '7d'),
          deps.buildHistoricalOpportunitySnapshot(env.DB, date, '30d'),
        ]);

        if (!dryRun) {
          if (snapshot7d) {
            await deps.storeOpportunitySnapshot(env.DB, snapshot7d);
            seededSnapshots += 1;
          }
          if (snapshot30d) {
            await deps.storeOpportunitySnapshot(env.DB, snapshot30d);
            seededSnapshots += 1;
          }
        } else {
          if (snapshot7d) seededSnapshots += 1;
          if (snapshot30d) seededSnapshots += 1;
        }
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : String(err);
        failedDates.push({ date, error: errorMessage.slice(0, 300) });
      }
    }

    let calibrationsGenerated = 0;
    let convictionCalibration7d: any = null;
    let convictionCalibration30d: any = null;
    let edgeCalibration: any = null;
    let ledgerRowsGenerated = 0;
    let itemLedgerRowsGenerated = 0;
    let decisionImpactSnapshotsGenerated = 0;
    let decisionImpactSummary: BackfillProductsResponsePayload['decision_impact'] = null;
    if (!dryRun && recalibrate) {
      try {
        convictionCalibration7d = await deps.buildConvictionCalibrationSnapshot(env.DB, '7d');
        await deps.storeCalibrationSnapshot(env.DB, convictionCalibration7d);
        calibrationsGenerated += 1;
      } catch (err) {
        console.error('Backfill 7d conviction calibration failed:', err);
      }

      try {
        convictionCalibration30d = await deps.buildConvictionCalibrationSnapshot(env.DB, '30d');
        await deps.storeCalibrationSnapshot(env.DB, convictionCalibration30d);
        calibrationsGenerated += 1;
      } catch (err) {
        console.error('Backfill 30d conviction calibration failed:', err);
      }

      try {
        edgeCalibration = await deps.buildEdgeQualityCalibrationSnapshot(env.DB);
        await deps.storeCalibrationSnapshot(env.DB, edgeCalibration);
        calibrationsGenerated += 1;
      } catch (err) {
        console.error('Backfill edge calibration failed:', err);
      }

      try {
        const latest7d = await deps.buildOpportunitySnapshot(env.DB, '7d', convictionCalibration7d, {
          sanitize_signals_tickers: signalsSanitizerEnabled,
        });
        const latest30d = await deps.buildOpportunitySnapshot(env.DB, '30d', convictionCalibration30d, {
          sanitize_signals_tickers: signalsSanitizerEnabled,
        });
        if (latest7d) await deps.storeOpportunitySnapshot(env.DB, latest7d);
        if (latest30d) await deps.storeOpportunitySnapshot(env.DB, latest30d);
      } catch (err) {
        console.error('Backfill latest opportunity refresh failed:', err);
      }
    }

    if (!dryRun && rebuildLedgers) {
      const snapshotLimit = Math.max(
        1,
        Math.min(MAX_BACKFILL_LIMIT * 2, candidateDates.length * 2 + 10),
      );
      const snapshotRows = await env.DB.prepare(`
        SELECT as_of, horizon, payload_json
        FROM opportunity_snapshots
        WHERE substr(as_of, 1, 10) >= ?
          AND substr(as_of, 1, 10) <= ?
          AND horizon IN ('7d', '30d')
        ORDER BY as_of ASC, horizon ASC
        LIMIT ?
      `).bind(startDate, endDate, snapshotLimit).all<{
        as_of: string;
        horizon: '7d' | '30d';
        payload_json: string;
      }>();

      for (const row of snapshotRows.results || []) {
        let snapshot: OpportunitySnapshot | null = null;
        try {
          snapshot = JSON.parse(row.payload_json) as OpportunitySnapshot;
        } catch {
          snapshot = null;
        }
        if (!snapshot || !Array.isArray(snapshot.items)) {
          continue;
        }

        try {
          const calibration = row.horizon === '7d' ? convictionCalibration7d : convictionCalibration30d;
          const ledgerBuild = deps.buildOpportunityLedgerProjection({
            refresh_run_id: null,
            snapshot,
            calibration,
            coherence_gate_enabled: true,
            freshness: {
              has_stale_data: false,
              stale_count: 0,
              critical_stale_count: 0,
            },
            consistency_state: 'PASS',
          });

          await deps.insertOpportunityLedgerRow(env.DB, ledgerBuild.ledger_row);
          ledgerRowsGenerated += 1;

          for (const itemRow of ledgerBuild.item_rows) {
            await deps.insertOpportunityItemLedgerRow(env.DB, itemRow);
            itemLedgerRowsGenerated += 1;
          }
        } catch (err) {
          const errorMessage = err instanceof Error ? err.message : String(err);
          failedDates.push({
            date: row.as_of.slice(0, 10),
            error: `ledger:${errorMessage.slice(0, 260)}`,
          });
        }
      }

      const asOfForImpact = `${endDate}T00:00:00.000Z`;
      try {
        for (const horizon of ['7d', '30d'] as const) {
          for (const scope of ['market', 'theme'] as const) {
            for (const windowDays of [30, 90] as const) {
              const snapshot = await computeDecisionImpact(env.DB, deps, {
                horizon,
                scope,
                window_days: windowDays,
                limit: scope === 'theme' ? 50 : 10,
                as_of: asOfForImpact,
              });
              await deps.storeDecisionImpactSnapshot(env.DB, snapshot);
              decisionImpactSnapshotsGenerated += 1;
            }
          }
        }

        const governance = deps.resolveDecisionImpactGovernance(env);
        const opsDecisionImpact = await buildDecisionImpactOpsResponse(env.DB, deps, 30, governance);
        decisionImpactSummary = {
          market_7d_hit_rate: opsDecisionImpact.market_7d.hit_rate,
          market_7d_sample_size: opsDecisionImpact.market_7d.sample_size,
          market_30d_hit_rate: opsDecisionImpact.market_30d.hit_rate,
          market_30d_sample_size: opsDecisionImpact.market_30d.sample_size,
          actionable_sessions: opsDecisionImpact.utility_attribution.actionable_sessions,
          cta_action_rate_pct: opsDecisionImpact.utility_attribution.cta_action_rate_pct,
          governance_mode: opsDecisionImpact.observe_mode.mode,
          enforce_ready: opsDecisionImpact.observe_mode.enforce_ready,
          enforce_breach_count: opsDecisionImpact.observe_mode.enforce_breach_count,
          enforce_breaches: opsDecisionImpact.observe_mode.enforce_breaches,
          observe_breach_count: opsDecisionImpact.observe_mode.breach_count,
          observe_breaches: opsDecisionImpact.observe_mode.breaches,
        };
      } catch (err) {
        console.error('Backfill decision-impact snapshot regeneration failed:', err);
      }
    }

    const payload: BackfillProductsResponsePayload = {
      ok: true,
      dry_run: dryRun,
      requested: {
        start: startDate,
        end: endDate,
        limit,
        overwrite,
        recalibrate,
        rebuild_ledgers: rebuildLedgers,
      },
      scanned_dates: candidateDates.length,
      processed_dates: processedDates,
      skipped_dates: skippedDates,
      seeded_snapshots: seededSnapshots,
      calibrations_generated: calibrationsGenerated,
      opportunity_ledger_rows_generated: ledgerRowsGenerated,
      opportunity_item_ledger_rows_generated: itemLedgerRowsGenerated,
      decision_impact_snapshots_generated: decisionImpactSnapshotsGenerated,
      decision_impact: decisionImpactSummary,
      calibration_samples: {
        edge_total_samples: edgeCalibration?.total_samples ?? null,
        conviction_7d_total_samples: convictionCalibration7d?.total_samples ?? null,
        conviction_30d_total_samples: convictionCalibration30d?.total_samples ?? null,
      },
      skipped_existing_dates: skippedExisting,
      failed_dates: failedDates.slice(0, 20),
    };

    return Response.json(payload, { headers: corsHeaders });
  }

  if (url.pathname === '/api/market/send-digest' && method === 'POST') {
    const adminAuthFailure = await deps.enforceAdminAuth(request, env, corsHeaders, clientIP);
    if (adminAuthFailure) {
      return adminAuthFailure;
    }

    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Send-digest schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    if (!deps.isFeatureEnabled(env, 'FEATURE_ENABLE_ALERTS_EMAIL', 'ENABLE_ALERTS_EMAIL', true)) {
      const payload: SendDigestResponsePayload = {
        ok: true,
        skipped: true,
        reason: 'Email alerts disabled',
      };
      return Response.json(payload, { headers: corsHeaders });
    }

    if (!deps.canSendEmail(env)) {
      return Response.json({ error: 'Email service unavailable' }, { status: 503, headers: corsHeaders });
    }

    const secret = deps.getAlertsSigningSecret(env);
    if (!secret) {
      return Response.json({ error: 'Signing secret unavailable' }, { status: 503, headers: corsHeaders });
    }

    const [briefRow, opportunityRow, alertRows, subscribers] = await Promise.all([
      env.DB.prepare(`SELECT payload_json FROM market_brief_snapshots ORDER BY as_of DESC LIMIT 1`).first<{ payload_json: string }>(),
      env.DB.prepare(`SELECT payload_json FROM opportunity_snapshots WHERE horizon = '7d' ORDER BY as_of DESC LIMIT 1`).first<{ payload_json: string }>(),
      env.DB.prepare(`
        SELECT id, event_type, severity, title, body, entity_type, entity_id, dedupe_key, payload_json, created_at
        FROM market_alert_events
        WHERE datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', '-24 hours')
        ORDER BY created_at DESC
        LIMIT 200
      `).all<MarketAlertEvent>(),
      env.DB.prepare(`
        SELECT id, email, types_json, cadence, status
        FROM email_subscribers
        WHERE status = 'active'
          AND cadence = 'daily_8am_et'
      `).all<{ id: string; email: string; types_json: string; cadence: string; status: string }>(),
    ]);

    let brief: BriefSnapshot | null = null;
    let opportunities: OpportunitySnapshot | null = null;
    try {
      brief = briefRow?.payload_json ? (JSON.parse(briefRow.payload_json) as BriefSnapshot) : null;
    } catch {
      brief = null;
    }
    try {
      opportunities = opportunityRow?.payload_json ? (JSON.parse(opportunityRow.payload_json) as OpportunitySnapshot) : null;
    } catch {
      opportunities = null;
    }
    const events = alertRows.results || [];

    let sentCount = 0;
    let failCount = 0;
    let bounceCount = 0;
    const digestEventId = `digest-${deps.asIsoDate(new Date())}`;

    for (const subscriber of subscribers.results || []) {
      let types: MarketAlertType[] = VALID_ALERT_TYPES;
      try {
        types = deps.normalizeAlertTypes(JSON.parse(subscriber.types_json));
      } catch {
        types = VALID_ALERT_TYPES;
      }

      const filteredEvents = events.filter((event: MarketAlertEvent) => types.includes(event.event_type));
      const reservation = await deps.reserveDigestDeliveryForSubscriber(env.DB, digestEventId, subscriber.id);
      if (reservation.action === 'skip') {
        continue;
      }

      const unsubscribeToken = `${subscriber.id}.${deps.generateToken(8)}`;
      const unsubscribeHash = await deps.hashToken(secret, unsubscribeToken);
      await env.DB.prepare(`
        INSERT OR REPLACE INTO email_unsubscribe_tokens (subscriber_id, token_hash, created_at)
        VALUES (?, ?, datetime('now'))
      `).bind(subscriber.id, unsubscribeHash).run();

      let result: { ok: boolean; providerId?: string; error?: string } = { ok: false, error: 'Unknown error' };
      for (let attempt = 1; attempt <= 3; attempt += 1) {
        result = await deps.sendDigestToSubscriber(env, subscriber.email, brief, opportunities, filteredEvents, unsubscribeToken);
        if (result.ok) break;
        const delayMs = attempt * attempt * 300;
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }

      if (result.ok) {
        sentCount += 1;
        await deps.finalizeDigestDeliverySent(env.DB, digestEventId, subscriber.id, result.providerId || null);
      } else {
        failCount += 1;
        const errorText = result.error || 'Delivery failed';
        if (errorText.toLowerCase().includes('bounce')) {
          bounceCount += 1;
          await env.DB.prepare(`
            UPDATE email_subscribers SET status = 'bounced', updated_at = datetime('now')
            WHERE id = ?
          `).bind(subscriber.id).run();
        }
        await deps.finalizeDigestDeliveryFailure(env.DB, digestEventId, subscriber.id, errorText);
      }
    }

    const payload: SendDigestResponsePayload = {
      ok: true,
      sent_count: sentCount,
      fail_count: failCount,
      bounce_count: bounceCount,
      active_subscribers: subscribers.results?.length || 0,
    };

    return Response.json(payload, { headers: corsHeaders });
  }

  return null;
}
