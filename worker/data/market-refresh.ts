import { asIsoDateTime, toNumber } from '../lib/market-primitives';
import { fetchLatestMarketProductSnapshotWrite } from './market-snapshots';

const DEFAULT_REFRESH_RUN_WINDOW_MINUTES = 90;

interface FreshnessImpactEventSummary {
  created_at: string;
  severity: 'warning' | 'critical';
  title: string;
  body: string;
}

interface FreshnessSloImpactSummary {
  state: 'none' | 'monitor' | 'degraded';
  stale_days: number;
  warning_events: number;
  critical_events: number;
  estimated_suppressed_days: number;
  latest_warning_event: FreshnessImpactEventSummary | null;
  latest_critical_event: FreshnessImpactEventSummary | null;
}

interface FreshnessSloWindowSummary {
  days_observed: number;
  days_with_critical_stale: number;
  slo_attainment_pct: number;
  recent_incidents: Array<{
    as_of: string;
    completed_at: string | null;
    trigger: string | null;
    stale_count: number;
    critical_stale_count: number;
  }>;
  incident_impact: FreshnessSloImpactSummary;
}

export type MarketRefreshRunClaim =
  | {
      status: 'claimed';
      run_id: number | null;
      refresh_trigger: string;
    }
  | {
      status: 'skipped';
      run_id: number | null;
      refresh_trigger: string;
      reason: 'refresh_in_progress';
    };

function normalizeRefreshTrigger(trigger: string): string {
  const trimmed = trigger.trim();
  return trimmed.length > 0 ? trimmed.slice(0, 128) : 'unknown';
}

export async function claimMarketRefreshRun(
  db: D1Database,
  trigger: string,
  maxAgeMinutes = DEFAULT_REFRESH_RUN_WINDOW_MINUTES,
): Promise<MarketRefreshRunClaim> {
  const normalizedTrigger = normalizeRefreshTrigger(trigger);
  const boundedWindowMinutes = Math.max(1, Math.floor(maxAgeMinutes));
  const lookbackExpr = `-${boundedWindowMinutes} minutes`;
  const nowIso = asIsoDateTime(new Date());

  try {
    await db.prepare(`
      UPDATE market_refresh_runs
      SET completed_at = ?,
          status = 'failed',
          error = 'abandoned_run'
      WHERE status = 'running'
        AND "trigger" = ?
        AND datetime(replace(replace(started_at, 'T', ' '), 'Z', '')) < datetime('now', ?)
    `).bind(nowIso, normalizedTrigger, lookbackExpr).run();

    const existing = await db.prepare(`
      SELECT id
      FROM market_refresh_runs
      WHERE status = 'running'
        AND "trigger" = ?
        AND datetime(replace(replace(started_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
      ORDER BY datetime(replace(replace(started_at, 'T', ' '), 'Z', '')) DESC, id DESC
      LIMIT 1
    `).bind(normalizedTrigger, lookbackExpr).first<{ id: number | null }>();

    if (existing?.id) {
      return {
        status: 'skipped',
        run_id: existing.id,
        refresh_trigger: normalizedTrigger,
        reason: 'refresh_in_progress',
      };
    }

    const result = await db.prepare(`
      INSERT INTO market_refresh_runs (started_at, status, "trigger", created_at)
      SELECT ?, 'running', ?, datetime('now')
      WHERE NOT EXISTS (
        SELECT 1
        FROM market_refresh_runs
        WHERE status = 'running'
          AND "trigger" = ?
          AND datetime(replace(replace(started_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
      )
    `).bind(nowIso, normalizedTrigger, normalizedTrigger, lookbackExpr).run();

    if (typeof result.meta?.changes === 'number' && result.meta.changes > 0) {
      return {
        status: 'claimed',
        run_id: typeof result.meta?.last_row_id === 'number' ? result.meta.last_row_id : null,
        refresh_trigger: normalizedTrigger,
      };
    }

    const active = await db.prepare(`
      SELECT id
      FROM market_refresh_runs
      WHERE status = 'running'
        AND "trigger" = ?
        AND datetime(replace(replace(started_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
      ORDER BY datetime(replace(replace(started_at, 'T', ' '), 'Z', '')) DESC, id DESC
      LIMIT 1
    `).bind(normalizedTrigger, lookbackExpr).first<{ id: number | null }>();

    if (active?.id) {
      return {
        status: 'skipped',
        run_id: active.id,
        refresh_trigger: normalizedTrigger,
        reason: 'refresh_in_progress',
      };
    }
  } catch (err) {
    console.error('Failed to claim market refresh run:', err);
  }

  return {
    status: 'claimed',
    run_id: await recordMarketRefreshRunStart(db, normalizedTrigger),
    refresh_trigger: normalizedTrigger,
  };
}

export async function recordMarketRefreshRunStart(db: D1Database, trigger: string): Promise<number | null> {
  try {
    const result = await db.prepare(`
      INSERT INTO market_refresh_runs (started_at, status, "trigger", created_at)
      VALUES (?, 'running', ?, datetime('now'))
    `).bind(asIsoDateTime(new Date()), normalizeRefreshTrigger(trigger)).run();

    if (typeof result.meta?.last_row_id === 'number') {
      return result.meta.last_row_id;
    }
    return null;
  } catch (err) {
    console.error('Failed to record market refresh run start:', err);
    return null;
  }
}

export async function recordMarketRefreshRunFinish(
  db: D1Database,
  runId: number | null,
  payload: {
    status: 'success' | 'failed' | 'blocked';
    brief_generated?: number;
    opportunities_generated?: number;
    calibrations_generated?: number;
    alerts_generated?: number;
    stale_count?: number | null;
    critical_stale_count?: number | null;
    as_of?: string | null;
    error?: string | null;
  },
): Promise<void> {
  if (!runId) return;

  try {
    await db.prepare(`
      UPDATE market_refresh_runs
      SET completed_at = ?,
          status = ?,
          brief_generated = ?,
          opportunities_generated = ?,
          calibrations_generated = ?,
          alerts_generated = ?,
          stale_count = ?,
          critical_stale_count = ?,
          as_of = ?,
          error = ?
      WHERE id = ?
    `).bind(
      asIsoDateTime(new Date()),
      payload.status,
      payload.brief_generated ?? 0,
      payload.opportunities_generated ?? 0,
      payload.calibrations_generated ?? 0,
      payload.alerts_generated ?? 0,
      payload.stale_count ?? null,
      payload.critical_stale_count ?? null,
      payload.as_of ?? null,
      payload.error ?? null,
      runId,
    ).run();
  } catch (err) {
    console.error('Failed to finalize market refresh run:', err);
  }
}

export async function fetchLatestSuccessfulMarketRefreshRun(db: D1Database): Promise<{
  completed_at: string | null;
  trigger: string | null;
  stale_count: number | null;
  critical_stale_count: number | null;
  as_of: string | null;
} | null> {
  try {
    const row = await db.prepare(`
      SELECT completed_at, "trigger" as trigger, stale_count, critical_stale_count, as_of
      FROM market_refresh_runs
      WHERE status = 'success'
        AND completed_at IS NOT NULL
      ORDER BY completed_at DESC, id DESC
      LIMIT 1
    `).first<{
      completed_at: string | null;
      trigger: string | null;
      stale_count: number | null;
      critical_stale_count: number | null;
      as_of: string | null;
    }>();

    return row || null;
  } catch {
    return null;
  }
}

export async function fetchLatestObservedMarketRefreshRun(db: D1Database): Promise<{
  completed_at: string | null;
  trigger: string | null;
  stale_count: number | null;
  critical_stale_count: number | null;
  as_of: string | null;
} | null> {
  try {
    const row = await db.prepare(`
      SELECT completed_at, "trigger" as trigger, stale_count, critical_stale_count, as_of
      FROM market_refresh_runs
      WHERE status IN ('success', 'blocked')
        AND completed_at IS NOT NULL
      ORDER BY completed_at DESC, id DESC
      LIMIT 1
    `).first<{
      completed_at: string | null;
      trigger: string | null;
      stale_count: number | null;
      critical_stale_count: number | null;
      as_of: string | null;
    }>();

    return row || null;
  } catch {
    return null;
  }
}

export async function computeFreshnessSloWindow(
  db: D1Database,
  windowDays: number,
): Promise<FreshnessSloWindowSummary> {
  const boundedDays = Math.max(1, Math.floor(windowDays));
  const lookbackExpr = `-${Math.max(0, boundedDays - 1)} days`;

  const [grouped, impactCounts, latestWarningEvent, latestCriticalEvent] = await Promise.all([
    db.prepare(`
      SELECT
        COALESCE(as_of, substr(completed_at, 1, 10)) as run_date,
        MAX(completed_at) as completed_at,
        MAX("trigger") as trigger,
        MAX(COALESCE(stale_count, 0)) as stale_count,
        MAX(COALESCE(critical_stale_count, 0)) as critical_stale_count
      FROM market_refresh_runs
      WHERE status IN ('success', 'blocked')
        AND completed_at IS NOT NULL
        AND completed_at >= datetime('now', ?)
      GROUP BY run_date
      ORDER BY run_date DESC
    `).bind(lookbackExpr).all<{
      run_date: string | null;
      completed_at: string | null;
      trigger: string | null;
      stale_count: number | null;
      critical_stale_count: number | null;
    }>(),
    db.prepare(`
      SELECT
        SUM(CASE WHEN severity = 'warning' THEN 1 ELSE 0 END) as warning_events,
        SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical_events
      FROM market_alert_events
      WHERE event_type = 'freshness_warning'
        AND datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
    `).bind(lookbackExpr).first<{
      warning_events: number | null;
      critical_events: number | null;
    }>(),
    db.prepare(`
      SELECT created_at, severity, title, body
      FROM market_alert_events
      WHERE event_type = 'freshness_warning'
        AND severity = 'warning'
        AND datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
      ORDER BY created_at DESC
      LIMIT 1
    `).bind(lookbackExpr).first<{
      created_at: string | null;
      severity: 'warning' | 'critical' | null;
      title: string | null;
      body: string | null;
    }>(),
    db.prepare(`
      SELECT created_at, severity, title, body
      FROM market_alert_events
      WHERE event_type = 'freshness_warning'
        AND severity = 'critical'
        AND datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
      ORDER BY created_at DESC
      LIMIT 1
    `).bind(lookbackExpr).first<{
      created_at: string | null;
      severity: 'warning' | 'critical' | null;
      title: string | null;
      body: string | null;
    }>(),
  ]);

  const rows = (grouped.results || [])
    .filter((row) => typeof row.run_date === 'string' && row.run_date.length >= 10)
    .map((row) => ({
      as_of: String(row.run_date).slice(0, 10),
      completed_at: row.completed_at || null,
      trigger: row.trigger || null,
      stale_count: Math.max(0, Math.floor(toNumber(row.stale_count, 0))),
      critical_stale_count: Math.max(0, Math.floor(toNumber(row.critical_stale_count, 0))),
    }));

  const daysObserved = rows.length;
  const staleDays = rows.filter((row) => row.stale_count > 0).length;
  const incidentRows = rows.filter((row) => row.critical_stale_count > 0);
  const daysWithCriticalStale = incidentRows.length;
  const sloAttainment = daysObserved > 0
    ? Number((((daysObserved - daysWithCriticalStale) / daysObserved) * 100).toFixed(2))
    : 0;
  const warningEvents = Math.max(0, Math.floor(toNumber(impactCounts?.warning_events, 0)));
  const criticalEvents = Math.max(0, Math.floor(toNumber(impactCounts?.critical_events, 0)));
  const impactState: FreshnessSloImpactSummary['state'] =
    (daysWithCriticalStale > 0 || criticalEvents > 0)
      ? 'degraded'
      : (staleDays > 0 || warningEvents > 0)
        ? 'monitor'
        : 'none';

  const mapImpactEvent = (event: {
    created_at: string | null;
    severity: 'warning' | 'critical' | null;
    title: string | null;
    body: string | null;
  } | null): FreshnessImpactEventSummary | null => {
    if (!event?.created_at || !event.severity || !event.title || !event.body) {
      return null;
    }

    return {
      created_at: event.created_at,
      severity: event.severity,
      title: event.title,
      body: event.body,
    };
  };

  return {
    days_observed: daysObserved,
    days_with_critical_stale: daysWithCriticalStale,
    slo_attainment_pct: sloAttainment,
    recent_incidents: incidentRows.slice(0, 20),
    incident_impact: {
      state: impactState,
      stale_days: staleDays,
      warning_events: warningEvents,
      critical_events: criticalEvents,
      estimated_suppressed_days: daysWithCriticalStale,
      latest_warning_event: mapImpactEvent(latestWarningEvent),
      latest_critical_event: mapImpactEvent(latestCriticalEvent),
    },
  };
}

export async function resolveLatestRefreshTimestamp(db: D1Database): Promise<{
  last_refresh_at_utc: string | null;
  source: 'market_refresh_runs' | 'market_product_snapshots' | 'fetch_logs' | 'unknown';
}> {
  const [latestRefreshRun, latestProductSnapshotWrite, lastFetchLogRow] = await Promise.all([
    fetchLatestSuccessfulMarketRefreshRun(db),
    fetchLatestMarketProductSnapshotWrite(db),
    db.prepare(`
      SELECT MAX(completed_at) as last_refresh_at
      FROM fetch_logs
      WHERE completed_at IS NOT NULL
    `).first<{ last_refresh_at: string | null }>(),
  ]);

  if (latestRefreshRun?.completed_at) {
    return {
      last_refresh_at_utc: latestRefreshRun.completed_at,
      source: 'market_refresh_runs',
    };
  }

  if (latestProductSnapshotWrite) {
    return {
      last_refresh_at_utc: latestProductSnapshotWrite,
      source: 'market_product_snapshots',
    };
  }

  if (lastFetchLogRow?.last_refresh_at) {
    return {
      last_refresh_at_utc: lastFetchLogRow.last_refresh_at,
      source: 'fetch_logs',
    };
  }

  return {
    last_refresh_at_utc: null,
    source: 'unknown',
  };
}

export async function resolveLatestObservedRefreshTimestamp(db: D1Database): Promise<{
  last_refresh_at_utc: string | null;
  source: 'market_refresh_runs' | 'market_product_snapshots' | 'fetch_logs' | 'unknown';
}> {
  const [latestRefreshRun, latestProductSnapshotWrite, lastFetchLogRow] = await Promise.all([
    fetchLatestObservedMarketRefreshRun(db),
    fetchLatestMarketProductSnapshotWrite(db),
    db.prepare(`
      SELECT MAX(completed_at) as last_refresh_at
      FROM fetch_logs
      WHERE completed_at IS NOT NULL
    `).first<{ last_refresh_at: string | null }>(),
  ]);

  if (latestRefreshRun?.completed_at) {
    return {
      last_refresh_at_utc: latestRefreshRun.completed_at,
      source: 'market_refresh_runs',
    };
  }

  if (latestProductSnapshotWrite) {
    return {
      last_refresh_at_utc: latestProductSnapshotWrite,
      source: 'market_product_snapshots',
    };
  }

  if (lastFetchLogRow?.last_refresh_at) {
    return {
      last_refresh_at_utc: lastFetchLogRow.last_refresh_at,
      source: 'fetch_logs',
    };
  }

  return {
    last_refresh_at_utc: null,
    source: 'unknown',
  };
}
