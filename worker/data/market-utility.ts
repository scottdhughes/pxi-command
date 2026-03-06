import { toNumber } from '../lib/market-primitives';
import type { UtilityEventInsertPayload, UtilityFunnelSummary } from '../types';

export async function insertUtilityEvent(db: D1Database, payload: UtilityEventInsertPayload): Promise<void> {
  await db.prepare(`
    INSERT INTO market_utility_events
      (session_id, event_type, route, actionability_state, payload_json, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `).bind(
    payload.session_id,
    payload.event_type,
    payload.route,
    payload.actionability_state,
    payload.payload_json,
    payload.created_at,
  ).run();
}

export async function computeUtilityFunnelSummary(
  db: D1Database,
  windowDays: number,
): Promise<UtilityFunnelSummary> {
  const boundedWindowDays = Math.max(1, Math.floor(windowDays));
  const lookbackExpr = `-${Math.max(0, boundedWindowDays - 1)} days`;

  const [aggregate, daysRow] = await Promise.all([
    db.prepare(`
      SELECT
        COUNT(*) as total_events,
        COUNT(DISTINCT session_id) as unique_sessions,
        SUM(CASE WHEN event_type = 'plan_view' THEN 1 ELSE 0 END) as plan_views,
        SUM(CASE WHEN event_type = 'opportunities_view' THEN 1 ELSE 0 END) as opportunities_views,
        SUM(CASE WHEN event_type = 'decision_actionable_view' THEN 1 ELSE 0 END) as decision_actionable_views,
        SUM(CASE WHEN event_type = 'decision_watch_view' THEN 1 ELSE 0 END) as decision_watch_views,
        SUM(CASE WHEN event_type = 'decision_no_action_view' THEN 1 ELSE 0 END) as decision_no_action_views,
        SUM(CASE WHEN event_type = 'no_action_unlock_view' THEN 1 ELSE 0 END) as no_action_unlock_views,
        SUM(CASE WHEN event_type = 'cta_action_click' THEN 1 ELSE 0 END) as cta_action_clicks,
        COUNT(DISTINCT CASE WHEN event_type = 'cta_action_click' THEN session_id END) as cta_action_sessions,
        COUNT(DISTINCT CASE WHEN event_type = 'decision_actionable_view' THEN session_id END) as actionable_view_sessions,
        COUNT(DISTINCT CASE WHEN event_type IN ('decision_actionable_view', 'cta_action_click') THEN session_id END) as actionable_sessions,
        MAX(created_at) as last_event_at
      FROM market_utility_events
      WHERE datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
    `).bind(lookbackExpr).first<{
      total_events: number | null;
      unique_sessions: number | null;
      plan_views: number | null;
      opportunities_views: number | null;
      decision_actionable_views: number | null;
      decision_watch_views: number | null;
      decision_no_action_views: number | null;
      no_action_unlock_views: number | null;
      cta_action_clicks: number | null;
      cta_action_sessions: number | null;
      actionable_view_sessions: number | null;
      actionable_sessions: number | null;
      last_event_at: string | null;
    }>(),
    db.prepare(`
      SELECT COUNT(DISTINCT substr(replace(created_at, 'T', ' '), 1, 10)) as days_observed
      FROM market_utility_events
      WHERE datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
    `).bind(lookbackExpr).first<{ days_observed: number | null }>(),
  ]);

  const totalEvents = Math.max(0, Math.floor(toNumber(aggregate?.total_events, 0)));
  const uniqueSessions = Math.max(0, Math.floor(toNumber(aggregate?.unique_sessions, 0)));
  const planViews = Math.max(0, Math.floor(toNumber(aggregate?.plan_views, 0)));
  const opportunitiesViews = Math.max(0, Math.floor(toNumber(aggregate?.opportunities_views, 0)));
  const decisionActionableViews = Math.max(0, Math.floor(toNumber(aggregate?.decision_actionable_views, 0)));
  const decisionWatchViews = Math.max(0, Math.floor(toNumber(aggregate?.decision_watch_views, 0)));
  const decisionNoActionViews = Math.max(0, Math.floor(toNumber(aggregate?.decision_no_action_views, 0)));
  const noActionUnlockViews = Math.max(0, Math.floor(toNumber(aggregate?.no_action_unlock_views, 0)));
  const ctaActionClicks = Math.max(0, Math.floor(toNumber(aggregate?.cta_action_clicks, 0)));
  const ctaActionSessions = Math.max(0, Math.floor(toNumber(aggregate?.cta_action_sessions, 0)));
  const actionableViewSessions = Math.max(0, Math.floor(toNumber(aggregate?.actionable_view_sessions, 0)));
  const actionableSessions = Math.max(0, Math.floor(toNumber(aggregate?.actionable_sessions, 0)));
  const decisionEventsTotal = decisionActionableViews + decisionWatchViews + decisionNoActionViews;
  const decisionEventsPerSession = uniqueSessions > 0
    ? Number((decisionEventsTotal / uniqueSessions).toFixed(4))
    : 0;
  const ctaActionRatePct = actionableSessions > 0
    ? Number(((ctaActionSessions / actionableSessions) * 100).toFixed(2))
    : 0;
  const noActionUnlockCoverage = decisionNoActionViews > 0
    ? Number(((noActionUnlockViews / decisionNoActionViews) * 100).toFixed(2))
    : 0;

  return {
    window_days: boundedWindowDays,
    days_observed: Math.max(0, Math.floor(toNumber(daysRow?.days_observed, 0))),
    total_events: totalEvents,
    unique_sessions: uniqueSessions,
    plan_views: planViews,
    opportunities_views: opportunitiesViews,
    decision_actionable_views: decisionActionableViews,
    decision_watch_views: decisionWatchViews,
    decision_no_action_views: decisionNoActionViews,
    no_action_unlock_views: noActionUnlockViews,
    cta_action_clicks: ctaActionClicks,
    actionable_view_sessions: actionableViewSessions,
    actionable_sessions: actionableSessions,
    cta_action_rate_pct: ctaActionRatePct,
    decision_events_total: decisionEventsTotal,
    decision_events_per_session: decisionEventsPerSession,
    no_action_unlock_coverage_pct: noActionUnlockCoverage,
    last_event_at: aggregate?.last_event_at || null,
  };
}
