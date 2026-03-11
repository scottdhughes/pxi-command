import { buildCanonicalMarketDecision } from './market-core';
import type {
  AlertsFeedResponsePayload,
  BriefSnapshot,
  MarketAlertEvent,
  MarketAlertType,
  OpportunityFeedResponsePayload,
  OpportunitySnapshot,
  OpportunityTtlMetadata,
  WorkerRouteContext,
} from '../types';

type MarketProductsDeps = Record<string, any>;

const BRIEF_CONTRACT_VERSION = '2026-02-17-v2';
const REFRESH_HOURS_UTC = [6, 14, 18, 22];
const OPPORTUNITY_REFRESH_TTL_GRACE_SECONDS = 90 * 60;
const VALID_ALERT_TYPES: MarketAlertType[] = ['regime_change', 'threshold_cross', 'opportunity_spike', 'freshness_warning'];

export function isBriefSnapshotCompatible(snapshot: BriefSnapshot | null): boolean {
  if (!snapshot) return false;
  if (snapshot.contract_version !== BRIEF_CONTRACT_VERSION) return false;
  if (!snapshot.policy_state || !snapshot.source_plan_as_of || !snapshot.consistency) return false;
  if (!snapshot.policy_state.stance || !snapshot.policy_state.risk_posture) return false;
  if (typeof snapshot.consistency.score !== 'number' || typeof snapshot.consistency.state !== 'string') return false;
  if (!snapshot.consistency.components || typeof snapshot.consistency.components !== 'object') return false;
  return true;
}

export function computeNextExpectedRefresh(now = new Date()): { at: string; in_minutes: number } {
  const nowMs = now.getTime();
  const candidateDates: Date[] = [];

  for (let dayOffset = 0; dayOffset <= 2; dayOffset += 1) {
    for (const hour of REFRESH_HOURS_UTC) {
      const candidate = new Date(Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate() + dayOffset,
        hour,
        0,
        0,
        0,
      ));
      if (candidate.getTime() >= nowMs) {
        candidateDates.push(candidate);
      }
    }
  }

  const next = candidateDates.length > 0
    ? candidateDates.sort((a, b) => a.getTime() - b.getTime())[0]
    : new Date(nowMs + (6 * 60 * 60 * 1000));

  return {
    at: next.toISOString(),
    in_minutes: Math.max(0, Math.round((next.getTime() - nowMs) / 60000)),
  };
}

function parseIsoTimestamp(value: string | null | undefined): Date | null {
  if (!value) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function snapshotAsOfDate(snapshot: OpportunitySnapshot | null): string | null {
  if (!snapshot?.as_of) return null;
  return snapshot.as_of.slice(0, 10);
}

export function computeOpportunityTtlMetadata(
  lastRefreshAtUtc: string | null,
  now = new Date(),
): OpportunityTtlMetadata {
  const lastRefreshDate = parseIsoTimestamp(lastRefreshAtUtc);
  if (!lastRefreshDate) {
    return {
      data_age_seconds: null,
      ttl_state: 'unknown',
      next_expected_refresh_at: null,
      overdue_seconds: null,
    };
  }

  const ageSeconds = Math.max(0, Math.floor((now.getTime() - lastRefreshDate.getTime()) / 1000));
  const nextExpected = computeNextExpectedRefresh(lastRefreshDate);
  const nextExpectedDate = parseIsoTimestamp(nextExpected.at);
  if (!nextExpectedDate) {
    return {
      data_age_seconds: ageSeconds,
      ttl_state: 'unknown',
      next_expected_refresh_at: null,
      overdue_seconds: null,
    };
  }

  const overdueSecondsRaw = Math.floor((now.getTime() - nextExpectedDate.getTime()) / 1000);
  const overdueSeconds = Math.max(0, overdueSecondsRaw);
  let ttlState: OpportunityTtlMetadata['ttl_state'] = 'fresh';
  if (overdueSecondsRaw > 0 && overdueSeconds <= OPPORTUNITY_REFRESH_TTL_GRACE_SECONDS) {
    ttlState = 'stale';
  } else if (overdueSeconds > OPPORTUNITY_REFRESH_TTL_GRACE_SECONDS) {
    ttlState = 'overdue';
  }

  return {
    data_age_seconds: ageSeconds,
    ttl_state: ttlState,
    next_expected_refresh_at: nextExpectedDate.toISOString(),
    overdue_seconds: overdueSeconds,
  };
}

function buildMarketEvent(
  deps: MarketProductsDeps,
  type: MarketAlertType,
  runDate: string,
  severity: 'info' | 'warning' | 'critical',
  title: string,
  body: string,
  entityType: 'market' | 'theme' | 'indicator',
  entityId: string | null,
  payload: Record<string, unknown>
): MarketAlertEvent {
  const dedupeKey = `${type}:${runDate}:${entityId || 'market'}`;
  return {
    id: `${runDate}-${type}-${deps.stableHash(dedupeKey)}`,
    event_type: type,
    severity,
    title,
    body,
    entity_type: entityType,
    entity_id: entityId,
    dedupe_key: dedupeKey,
    payload_json: JSON.stringify(payload),
    created_at: deps.asIsoDateTime(new Date()),
  };
}

export async function generateMarketEvents(
  db: D1Database,
  deps: MarketProductsDeps,
  brief: BriefSnapshot,
  opportunities: OpportunitySnapshot
): Promise<MarketAlertEvent[]> {
  const events: MarketAlertEvent[] = [];
  const runDate = brief.as_of.slice(0, 10);

  if (brief.regime_delta === 'SHIFTED') {
    events.push(buildMarketEvent(
      deps,
      'regime_change',
      runDate,
      'warning',
      'Market regime shifted',
      `PXI regime changed as of ${runDate}. Current posture is ${brief.risk_posture.replace('_', '-')}.`,
      'market',
      'pxi',
      { regime_delta: brief.regime_delta, risk_posture: brief.risk_posture }
    ));
  }

  const latestPxiRows = await db.prepare(`
    SELECT date, score FROM pxi_scores ORDER BY date DESC LIMIT 2
  `).all<{ date: string; score: number }>();
  const current = latestPxiRows.results?.[0];
  const previous = latestPxiRows.results?.[1];
  if (current && previous) {
    for (const threshold of [30, 45, 65, 80]) {
      const crossedUp = previous.score < threshold && current.score >= threshold;
      const crossedDown = previous.score > threshold && current.score <= threshold;
      if (crossedUp || crossedDown) {
        events.push(buildMarketEvent(
          deps,
          'threshold_cross',
          runDate,
          threshold >= 65 ? 'warning' : 'info',
          `PXI crossed ${threshold}`,
          `PXI moved ${crossedUp ? 'above' : 'below'} ${threshold} (${previous.score.toFixed(1)} → ${current.score.toFixed(1)}).`,
          'indicator',
          `pxi_${threshold}`,
          { threshold, from: previous.score, to: current.score, direction: crossedUp ? 'up' : 'down' }
        ));
      }
    }
  }

  const topOpportunity = opportunities.items[0];
  if (topOpportunity) {
    const previousSnapshot = await db.prepare(`
      SELECT payload_json
      FROM opportunity_snapshots
      WHERE horizon = ?
      ORDER BY as_of DESC
      LIMIT 1 OFFSET 1
    `).bind(opportunities.horizon).first<{ payload_json: string }>();

    let previousTopConviction: number | null = null;
    if (previousSnapshot?.payload_json) {
      try {
        const previousPayload = JSON.parse(previousSnapshot.payload_json) as OpportunitySnapshot;
        previousTopConviction = previousPayload.items?.[0]?.conviction_score ?? null;
      } catch {
        previousTopConviction = null;
      }
    }

    if (previousTopConviction !== null && (topOpportunity.conviction_score - previousTopConviction) >= 12) {
      events.push(buildMarketEvent(
        deps,
        'opportunity_spike',
        runDate,
        'info',
        'Opportunity conviction spike',
        `${topOpportunity.theme_name} conviction jumped from ${previousTopConviction} to ${topOpportunity.conviction_score}.`,
        'theme',
        topOpportunity.theme_id,
        {
          theme_id: topOpportunity.theme_id,
          previous_conviction: previousTopConviction,
          current_conviction: topOpportunity.conviction_score,
        }
      ));
    }
  }

  const staleCount = Math.max(0, Math.floor(deps.toNumber(brief.freshness_status.stale_count, 0)));
  const criticalStaleCount = Math.max(0, Math.floor(deps.toNumber(brief.freshness_status.critical_stale_count, 0)));
  if (brief.freshness_status.has_stale_data || staleCount > 0) {
    const severity = criticalStaleCount > 0 ? 'critical' : 'warning';
    events.push(buildMarketEvent(
      deps,
      'freshness_warning',
      runDate,
      severity,
      'Data freshness warning',
      criticalStaleCount > 0
        ? `${staleCount} indicator(s) are stale (${criticalStaleCount} critical) and may impact confidence.`
        : `${staleCount} non-critical indicator(s) are stale and may impact confidence.`,
      'market',
      'data_freshness',
      { stale_count: staleCount, critical_stale_count: criticalStaleCount }
    ));
  }

  if (brief.consistency.state === 'WARN' || brief.consistency.state === 'FAIL') {
    events.push(buildMarketEvent(
      deps,
      'threshold_cross',
      runDate,
      brief.consistency.state === 'FAIL' ? 'critical' : 'warning',
      'Consistency warning',
      `Public decision consistency is ${brief.consistency.state} (score ${brief.consistency.score}).`,
      'market',
      'consistency',
      {
        consistency_state: brief.consistency.state,
        consistency_score: brief.consistency.score,
        violations: brief.consistency.violations,
      }
    ));
  }

  return events;
}

function buildOpportunityRouteFallbackResponse(
  fallback: any,
  ttl: OpportunityTtlMetadata,
): OpportunityFeedResponsePayload {
  return {
    as_of: fallback.as_of,
    horizon: fallback.horizon,
    items: fallback.items,
    suppressed_count: 0,
    quality_filtered_count: 0,
    coherence_suppressed_count: 0,
    suppression_by_reason: {
      coherence_failed: 0,
      quality_filtered: 0,
      data_quality_suppressed: 0,
    },
    quality_filter_rate: 0,
    coherence_fail_rate: 0,
    degraded_reason: fallback.degraded_reason,
    actionability_state: 'NO_ACTION',
    actionability_reason_codes: ['no_eligible_opportunities', `opportunity_${fallback.degraded_reason}`],
    cta_enabled: false,
    cta_disabled_reasons: ['no_eligible_opportunities'],
    data_age_seconds: ttl.data_age_seconds,
    ttl_state: ttl.ttl_state,
    next_expected_refresh_at: ttl.next_expected_refresh_at,
    overdue_seconds: ttl.overdue_seconds,
  };
}

export async function tryHandleMarketProductsRoute(
  route: WorkerRouteContext,
  deps: MarketProductsDeps,
): Promise<Response | null> {
  const { request, env, url, method, corsHeaders, clientIP } = route;

  if (url.pathname === '/api/brief' && method === 'GET') {
    if (!deps.isFeatureEnabled(env, 'FEATURE_ENABLE_BRIEF', 'ENABLE_BRIEF', true)) {
      return Response.json(deps.buildBriefFallbackSnapshot('feature_disabled'), {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'no-store',
        },
      });
    }

    const scope = (url.searchParams.get('scope') || 'market').trim().toLowerCase();
    if (scope !== 'market') {
      return Response.json({ error: 'Only scope=market is supported in phase 1' }, { status: 400, headers: corsHeaders });
    }

    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Brief schema guard failed:', err);
      return Response.json(deps.buildBriefFallbackSnapshot('migration_guard_failed'), {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'no-store',
        },
      });
    }

    let snapshot: BriefSnapshot | null = null;
    let stored: { payload_json: string } | null = null;
    try {
      stored = await env.DB.prepare(`
        SELECT payload_json
        FROM market_brief_snapshots
        ORDER BY as_of DESC
        LIMIT 1
      `).first<{ payload_json: string }>();
    } catch (err) {
      console.error('Brief snapshot lookup failed:', err);
      stored = null;
    }

    if (stored?.payload_json) {
      try {
        snapshot = JSON.parse(stored.payload_json) as BriefSnapshot;
      } catch {
        snapshot = null;
      }
    }

    if (snapshot && !isBriefSnapshotCompatible(snapshot)) {
      snapshot = null;
    }

    if (snapshot) {
      const latestDateRow = await env.DB.prepare(`
        SELECT date
        FROM pxi_scores
        ORDER BY date DESC
        LIMIT 1
      `).first<{ date: string }>();
      const expectedPlanAsOf = latestDateRow?.date ? `${latestDateRow.date}T00:00:00.000Z` : null;
      if (expectedPlanAsOf && snapshot.source_plan_as_of !== expectedPlanAsOf) {
        snapshot = null;
      }
    }

    if (snapshot) {
      try {
        const currentFreshness = await deps.computeFreshnessStatus(env.DB);
        const snapshotFreshness = snapshot.freshness_status || {
          has_stale_data: false,
          stale_count: 0,
          critical_stale_count: 0,
        };

        if (
          Boolean(snapshotFreshness.has_stale_data) !== Boolean(currentFreshness.has_stale_data) ||
          deps.toNumber(snapshotFreshness.stale_count, 0) !== deps.toNumber(currentFreshness.stale_count, 0) ||
          deps.toNumber(snapshotFreshness.critical_stale_count, 0) !== deps.toNumber(currentFreshness.critical_stale_count, 0)
        ) {
          snapshot = null;
        }
      } catch (err) {
        console.error('Brief freshness parity check failed:', err);
        snapshot = null;
      }
    }

    if (!snapshot) {
      try {
        snapshot = await deps.buildBriefSnapshot(env.DB);
      } catch (err) {
        console.error('Failed to build brief snapshot:', err);
        snapshot = null;
      }
      if (!snapshot) {
        return Response.json(deps.buildBriefFallbackSnapshot('snapshot_unavailable'), {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'no-store',
          },
        });
      }
      try {
        await deps.storeBriefSnapshot(env.DB, snapshot);
      } catch (err) {
        console.error('Brief snapshot store failed:', err);
      }
    }

    return Response.json(snapshot, {
      headers: {
        ...corsHeaders,
        'Cache-Control': 'public, max-age=60',
      },
    });
  }

  if (url.pathname === '/api/opportunities' && method === 'GET') {
    const horizon = (url.searchParams.get('horizon') || '7d').trim() === '30d' ? '30d' : '7d';
    const limit = Math.min(50, Math.max(1, parseInt(url.searchParams.get('limit') || '20', 10)));
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
    const unknownTtlMetadata: OpportunityTtlMetadata = {
      data_age_seconds: null,
      ttl_state: 'unknown',
      next_expected_refresh_at: null,
      overdue_seconds: null,
    };

    if (!deps.isFeatureEnabled(env, 'FEATURE_ENABLE_OPPORTUNITIES', 'ENABLE_OPPORTUNITIES', true)) {
      const fallback = deps.buildOpportunityFallbackSnapshot(horizon, 'feature_disabled');
      return Response.json(buildOpportunityRouteFallbackResponse(fallback, unknownTtlMetadata), {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'no-store',
        },
      });
    }

    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Opportunities schema guard failed:', err);
      const fallback = deps.buildOpportunityFallbackSnapshot(horizon, 'migration_guard_failed');
      return Response.json(buildOpportunityRouteFallbackResponse(fallback, unknownTtlMetadata), {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'no-store',
        },
      });
    }

    let snapshot: OpportunitySnapshot | null = null;
    let stored: { payload_json: string } | null = null;
    try {
      stored = await env.DB.prepare(`
        SELECT payload_json
        FROM opportunity_snapshots
        WHERE horizon = ?
        ORDER BY as_of DESC
        LIMIT 1
      `).bind(horizon).first<{ payload_json: string }>();
    } catch (err) {
      console.error('Opportunity snapshot lookup failed:', err);
      stored = null;
    }

    if (stored?.payload_json) {
      try {
        snapshot = JSON.parse(stored.payload_json) as OpportunitySnapshot;
      } catch {
        snapshot = null;
      }
    }

    let latestPxiDate: string | null = null;
    try {
      const latestPxiRow = await env.DB.prepare(`
        SELECT date
        FROM pxi_scores
        ORDER BY date DESC
        LIMIT 1
      `).first<{ date: string | null }>();
      latestPxiDate = latestPxiRow?.date || null;
    } catch (err) {
      console.error('Latest PXI date lookup failed for opportunities:', err);
    }

    const hadStoredSnapshot = Boolean(snapshot);
    const shouldRebuildSnapshot = !snapshot || (
      Boolean(latestPxiDate) &&
      Boolean(snapshotAsOfDate(snapshot)) &&
      String(snapshotAsOfDate(snapshot)) < String(latestPxiDate)
    );

	    if (shouldRebuildSnapshot) {
	      try {
	        snapshot = await deps.buildOpportunitySnapshot(env.DB, horizon, undefined, {
          sanitize_signals_tickers: signalsSanitizerEnabled,
        });
      } catch (err) {
        console.error('Failed to build opportunity snapshot:', err);
        snapshot = null;
      }
      if (!snapshot) {
        const fallback = deps.buildOpportunityFallbackSnapshot(horizon, 'snapshot_unavailable');
        return Response.json(buildOpportunityRouteFallbackResponse(fallback, unknownTtlMetadata), {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'no-store',
          },
        });
      }
	      if (!hadStoredSnapshot) {
	        try {
	          await deps.storeOpportunitySnapshot(env.DB, snapshot);
	        } catch (err) {
	          console.error('Opportunity snapshot store failed:', err);
	        }
	      }
	    }
	    if (!snapshot) {
	      const fallback = deps.buildOpportunityFallbackSnapshot(horizon, 'snapshot_unavailable');
	      return Response.json(buildOpportunityRouteFallbackResponse(fallback, unknownTtlMetadata), {
	        headers: {
	          ...corsHeaders,
	          'Cache-Control': 'no-store',
	        },
	      });
	    }
	    const effectiveSnapshot = snapshot;

	    const resolveRefreshTimestamp = deps.resolveLatestObservedRefreshTimestamp || deps.resolveLatestRefreshTimestamp;
    const [convictionCalibration, freshness, latestConsistencyCheck, latestRefresh] = await Promise.all([
      deps.fetchLatestCalibrationSnapshot(env.DB, 'conviction', horizon),
      deps.computeFreshnessStatus(env.DB),
      deps.fetchLatestConsistencyCheck(env.DB),
      resolveRefreshTimestamp(env.DB),
    ]);

    let consistencyState = latestConsistencyCheck?.state ?? 'PASS';
    if (!latestConsistencyCheck) {
      try {
        consistencyState = (await buildCanonicalMarketDecision(env.DB, deps)).consistency.state;
      } catch {
        consistencyState = 'PASS';
      }
    }

	    const normalizedItems = deps.normalizeOpportunityItemsForPublishing(effectiveSnapshot.items, convictionCalibration);
    const projectedFeed = deps.projectOpportunityFeed(normalizedItems, {
      coherence_gate_enabled: coherenceGateEnabled,
      freshness,
      consistency_state: consistencyState,
    });
    const ttlMetadata = computeOpportunityTtlMetadata(latestRefresh.last_refresh_at_utc, new Date());
    let effectiveDegradedReason = projectedFeed.degraded_reason;
    if (!effectiveDegradedReason && ttlMetadata.ttl_state === 'overdue') {
      effectiveDegradedReason = 'refresh_ttl_overdue';
    } else if (!effectiveDegradedReason && ttlMetadata.ttl_state === 'unknown') {
      effectiveDegradedReason = 'refresh_ttl_unknown';
    }
    const diagnostics = deps.computeCalibrationDiagnostics(convictionCalibration);
    const ctaState = deps.evaluateOpportunityCtaState(projectedFeed, diagnostics, ttlMetadata, effectiveDegradedReason);
    const actionabilityReasonCodes = Array.from(new Set([
      ...(projectedFeed.items.length === 0 ? ['no_eligible_opportunities'] : []),
      ...(effectiveDegradedReason ? [`opportunity_${effectiveDegradedReason}`] : []),
      ...(ctaState.actionability_state === 'WATCH' ? ['watch_state'] : []),
      ...(ctaState.actionability_state === 'ACTIONABLE' ? ['eligible_opportunities_available'] : []),
      ...ctaState.cta_disabled_reasons.map((reason: string) => `cta_${reason}`),
    ]));
    const responseItems = projectedFeed.items.slice(0, limit);
    const cacheControl = ttlMetadata.ttl_state === 'overdue' || ttlMetadata.ttl_state === 'unknown'
      ? 'no-store'
      : 'public, max-age=60';

	    const responsePayload: OpportunityFeedResponsePayload = {
	      as_of: effectiveSnapshot.as_of,
	      horizon: effectiveSnapshot.horizon,
      items: responseItems,
      suppressed_count: projectedFeed.suppressed_count,
      quality_filtered_count: projectedFeed.quality_filtered_count,
      coherence_suppressed_count: projectedFeed.coherence_suppressed_count,
      suppression_by_reason: projectedFeed.suppression_by_reason,
      quality_filter_rate: projectedFeed.quality_filter_rate,
      coherence_fail_rate: projectedFeed.coherence_fail_rate,
      degraded_reason: effectiveDegradedReason,
      actionability_state: ctaState.actionability_state,
      actionability_reason_codes: actionabilityReasonCodes,
      cta_enabled: ctaState.cta_enabled,
      cta_disabled_reasons: ctaState.cta_disabled_reasons,
      data_age_seconds: ttlMetadata.data_age_seconds,
      ttl_state: ttlMetadata.ttl_state,
      next_expected_refresh_at: ttlMetadata.next_expected_refresh_at,
      overdue_seconds: ttlMetadata.overdue_seconds,
    };

    return Response.json(responsePayload, {
      headers: {
        ...corsHeaders,
        'Cache-Control': cacheControl,
      },
    });
  }

  if (url.pathname === '/api/alerts/feed' && method === 'GET') {
    if (!deps.isFeatureEnabled(env, 'FEATURE_ENABLE_ALERTS_IN_APP', 'ENABLE_ALERTS_IN_APP', true)) {
      const payload: AlertsFeedResponsePayload = {
        as_of: new Date().toISOString(),
        alerts: [],
        degraded_reason: 'feature_disabled',
      };

      return Response.json(payload, {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'no-store',
        },
      });
    }

    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Alerts feed schema guard failed:', err);
      const payload: AlertsFeedResponsePayload = {
        as_of: new Date().toISOString(),
        alerts: [],
        degraded_reason: 'migration_guard_failed',
      };

      return Response.json(payload, {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'no-store',
        },
      });
    }

    const limit = Math.min(200, Math.max(1, parseInt(url.searchParams.get('limit') || '50', 10)));
    const since = url.searchParams.get('since');
    const rawTypes = (url.searchParams.get('types') || '')
      .split(',')
      .map((value) => value.trim())
      .filter((value) => value.length > 0);
    const types = rawTypes.filter((value): value is MarketAlertType => VALID_ALERT_TYPES.includes(value as MarketAlertType));

    let query = `
      SELECT id, event_type, severity, title, body, entity_type, entity_id, created_at
      FROM market_alert_events
      WHERE 1 = 1
    `;
    const params: Array<string | number> = [];

    if (since) {
      query += ` AND datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime(?)`;
      params.push(since);
    }

    if (types.length > 0) {
      query += ` AND event_type IN (${types.map(() => '?').join(',')})`;
      params.push(...types);
    }

    query += ' ORDER BY created_at DESC LIMIT ?';
    params.push(limit);

    let events: any;
    try {
      events = await env.DB.prepare(query).bind(...params).all();
    } catch (err) {
      console.error('Failed to load in-app alerts feed:', err);
      const payload: AlertsFeedResponsePayload = {
        as_of: new Date().toISOString(),
        alerts: [],
        degraded_reason: 'query_failed',
      };

      return Response.json(payload, {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'no-store',
        },
      });
    }

    const payload: AlertsFeedResponsePayload = {
      as_of: new Date().toISOString(),
      alerts: events.results || [],
      degraded_reason: null,
    };

    return Response.json(payload, {
      headers: {
        ...corsHeaders,
        'Cache-Control': 'public, max-age=30',
      },
    });
  }

  if (url.pathname === '/api/alerts/subscribe/start' && method === 'POST') {
    if (!deps.isFeatureEnabled(env, 'FEATURE_ENABLE_ALERTS_EMAIL', 'ENABLE_ALERTS_EMAIL', true)) {
      return Response.json({ error: 'Email alerts disabled' }, { status: 404, headers: corsHeaders });
    }
    if (!deps.canSendEmail(env)) {
      return Response.json({ error: 'Email service unavailable' }, { status: 503, headers: corsHeaders });
    }
    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Subscribe-start schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    const body = await deps.parseJsonBody(request) as { email?: string; types?: string[]; cadence?: string } | null;
    const email = String(body?.email || '').trim().toLowerCase();
    if (!deps.validateEmail(email)) {
      return Response.json({ error: 'Invalid email' }, { status: 400, headers: corsHeaders });
    }

    const secret = deps.getAlertsSigningSecret(env);
    if (!secret) {
      return Response.json({ error: 'Signing secret unavailable' }, { status: 503, headers: corsHeaders });
    }

    const subscriberId = `sub_${deps.stableHash(`${email}:${Date.now()}:${deps.generateToken(4)}`)}`;
    const cadence = deps.normalizeCadence(body?.cadence);
    const types = deps.normalizeAlertTypes(body?.types);
    const verifyToken = deps.generateToken(18);
    const tokenHash = await deps.hashToken(secret, verifyToken);
    const expiresAt = new Date(Date.now() + (15 * 60 * 1000)).toISOString();

    await env.DB.prepare(`
      INSERT INTO email_subscribers (id, email, status, cadence, types_json, timezone, created_at, updated_at)
      VALUES (?, ?, 'pending', ?, ?, 'America/New_York', datetime('now'), datetime('now'))
      ON CONFLICT(email) DO UPDATE SET
        status = 'pending',
        cadence = excluded.cadence,
        types_json = excluded.types_json,
        updated_at = datetime('now')
    `).bind(subscriberId, email, cadence, JSON.stringify(types)).run();

    await env.DB.prepare(`
      INSERT INTO email_verification_tokens (email, token_hash, expires_at, created_at)
      VALUES (?, ?, ?, datetime('now'))
    `).bind(email, tokenHash, expiresAt).run();

    const verifyUrl = `https://pxicommand.com/inbox?verify_token=${encodeURIComponent(verifyToken)}`;
    const verificationEmail = await deps.sendCloudflareEmail(env, {
      to: email,
      subject: 'Verify your PXI alert subscription',
      text: `Verify your PXI alerts subscription by opening: ${verifyUrl}\n\nThis link expires in 15 minutes.`,
      html: `
        <div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;padding:20px;color:#111;">
          <h2>Verify your PXI alerts subscription</h2>
          <p>Confirm your email to receive daily 8:00 AM ET digest emails.</p>
          <p><a href="${verifyUrl}">Verify subscription</a></p>
          <p style="font-size:12px;color:#555;">This link expires in 15 minutes.</p>
        </div>
      `,
    });

    if (!verificationEmail.ok) {
      return Response.json({ error: verificationEmail.error || 'Verification email failed' }, { status: 503, headers: corsHeaders });
    }

    return Response.json({ ok: true }, { headers: corsHeaders });
  }

  if (url.pathname === '/api/alerts/subscribe/verify' && method === 'POST') {
    if (!deps.isFeatureEnabled(env, 'FEATURE_ENABLE_ALERTS_EMAIL', 'ENABLE_ALERTS_EMAIL', true)) {
      return Response.json({ error: 'Email alerts disabled' }, { status: 404, headers: corsHeaders });
    }
    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Subscribe-verify schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    const body = await deps.parseJsonBody(request) as { token?: string } | null;
    const token = String(body?.token || '').trim();
    if (!token) {
      return Response.json({ error: 'Token required' }, { status: 400, headers: corsHeaders });
    }

    const secret = deps.getAlertsSigningSecret(env);
    if (!secret) {
      return Response.json({ error: 'Signing secret unavailable' }, { status: 503, headers: corsHeaders });
    }

    const tokenHash = await deps.hashToken(secret, token);
    const tokenRecord = await env.DB.prepare(`
      SELECT id, email
      FROM email_verification_tokens
      WHERE token_hash = ?
        AND used_at IS NULL
        AND expires_at > datetime('now')
      ORDER BY id DESC
      LIMIT 1
    `).bind(tokenHash).first<{ id: number; email: string }>();

    if (!tokenRecord) {
      return Response.json({ error: 'Invalid or expired token' }, { status: 400, headers: corsHeaders });
    }

    await env.DB.prepare(`
      UPDATE email_verification_tokens
      SET used_at = datetime('now')
      WHERE id = ?
    `).bind(tokenRecord.id).run();

    await env.DB.prepare(`
      UPDATE email_subscribers
      SET status = 'active', updated_at = datetime('now')
      WHERE email = ?
    `).bind(tokenRecord.email).run();

    const subscriber = await env.DB.prepare(`
      SELECT id
      FROM email_subscribers
      WHERE email = ?
      LIMIT 1
    `).bind(tokenRecord.email).first<{ id: string }>();

    if (subscriber?.id) {
      const unsubscribeToken = `${subscriber.id}.${deps.generateToken(8)}`;
      const unsubscribeHash = await deps.hashToken(secret, unsubscribeToken);
      await env.DB.prepare(`
        INSERT OR REPLACE INTO email_unsubscribe_tokens (subscriber_id, token_hash, created_at)
        VALUES (?, ?, datetime('now'))
      `).bind(subscriber.id, unsubscribeHash).run();
    }

    return Response.json({
      ok: true,
      subscriber_status: 'active',
    }, { headers: corsHeaders });
  }

  if (url.pathname === '/api/alerts/unsubscribe' && method === 'POST') {
    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Unsubscribe schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    const body = await deps.parseJsonBody(request) as { token?: string } | null;
    const token = String(body?.token || '').trim();
    const subscriberId = token.split('.')[0];

    if (!token || !subscriberId) {
      return Response.json({ error: 'Token required' }, { status: 400, headers: corsHeaders });
    }

    const secret = deps.getAlertsSigningSecret(env);
    if (!secret) {
      return Response.json({ error: 'Signing secret unavailable' }, { status: 503, headers: corsHeaders });
    }

    const record = await env.DB.prepare(`
      SELECT token_hash
      FROM email_unsubscribe_tokens
      WHERE subscriber_id = ?
    `).bind(subscriberId).first<{ token_hash: string }>();

    if (!record) {
      return Response.json({ error: 'Invalid unsubscribe token' }, { status: 400, headers: corsHeaders });
    }

    const tokenHash = await deps.hashToken(secret, token);
    if (!deps.constantTimeEquals(tokenHash, record.token_hash)) {
      return Response.json({ error: 'Invalid unsubscribe token' }, { status: 400, headers: corsHeaders });
    }

    await env.DB.prepare(`
      UPDATE email_subscribers
      SET status = 'unsubscribed', updated_at = datetime('now')
      WHERE id = ?
    `).bind(subscriberId).run();

    return Response.json({ ok: true }, { headers: corsHeaders });
  }

  return null;
}
