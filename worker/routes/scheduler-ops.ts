import { isUsEquityTradingDay } from '../config/refresh-schedule';
import {
  fetchRefreshSchedulerHealth,
  type RefreshSchedulerHealth,
} from '../data/refresh-scheduler';
import {
  claimRefreshMutationLock,
  releaseRefreshMutationLock,
  type RefreshMutationHolderType,
} from '../data/refresh-mutation-lock';
import { currentNewYorkDate } from '../lib/history-provenance';
import { enforceAdminAuth } from '../lib/security';
import type { NativeRefreshSummary } from '../services/scheduled-refresh';
import type { Env, WorkerRouteContext } from '../types';

type AdminAuth = (
  request: Request,
  env: Env,
  corsHeaders: Record<string, string>,
  clientIP: string,
) => Promise<Response | null>;

interface SchedulerOpsDeps {
  now?: () => Date;
  enforceAdminAuth?: AdminAuth;
  fetchRefreshSchedulerHealth?: typeof fetchRefreshSchedulerHealth;
  runNativeRefreshPipeline?: typeof import('../services/scheduled-refresh')['runNativeRefreshPipeline'];
  claimRefreshMutationLock?: typeof claimRefreshMutationLock;
  releaseRefreshMutationLock?: typeof releaseRefreshMutationLock;
}

const HEALTHY_STATES = new Set<RefreshSchedulerHealth['state']>([
  'healthy',
  'pending',
  'not_expected',
]);
const EXTERNAL_MUTATION_HOLDERS = new Set<RefreshMutationHolderType>([
  'github_daily_refresh',
  'history_reconstruction',
  'deploy',
]);

function responseHeaders(corsHeaders: Record<string, string>): Record<string, string> {
  return {
    ...corsHeaders,
    'Cache-Control': 'no-store',
  };
}

function unknownHealth(now: Date, decisionDate: string | null): RefreshSchedulerHealth {
  return {
    state: 'unknown',
    checked_at: now.toISOString(),
    decision_date: decisionDate,
    market_day_expected: decisionDate ? isUsEquityTradingDay(decisionDate) : null,
    latest_daily_close: null,
    latest_incident: null,
  };
}

/** Public scheduler health plus a narrowly scoped authenticated deploy smoke. */
export async function tryHandleSchedulerOpsRoute(
  route: WorkerRouteContext,
  deps: SchedulerOpsDeps = {},
): Promise<Response | null> {
  const { request, env, url, method, corsHeaders, clientIP } = route;
  const now = deps.now?.() ?? new Date();
  const headers = responseHeaders(corsHeaders);

  if (url.pathname === '/health/refresh' && method === 'GET') {
    let decisionDate: string | null = null;
    let health: RefreshSchedulerHealth;
    try {
      decisionDate = currentNewYorkDate(now);
      health = await (deps.fetchRefreshSchedulerHealth ?? fetchRefreshSchedulerHealth)(env.DB, {
        now,
        decisionDate,
        isUsMarketDay: isUsEquityTradingDay(decisionDate),
      });
    } catch (error) {
      console.error('Scheduler health check failed:', error instanceof Error ? error.message : error);
      health = unknownHealth(now, decisionDate);
    }

    return Response.json(health, {
      status: HEALTHY_STATES.has(health.state) ? 200 : 503,
      headers,
    });
  }

  if (url.pathname === '/api/admin/refresh/run' && method === 'POST') {
    const authFailure = await (deps.enforceAdminAuth ?? enforceAdminAuth)(
      request,
      env,
      corsHeaders,
      clientIP,
    );
    if (authFailure) return authFailure;

    const refreshPipeline = deps.runNativeRefreshPipeline
      ?? (await import('../services/scheduled-refresh')).runNativeRefreshPipeline;
    const summary: NativeRefreshSummary = await refreshPipeline(
      env,
      {
        schedule_id: 'deploy_smoke',
        record_research_evidence: false,
      },
      now.getTime(),
    );

    return Response.json(summary, { headers });
  }

  if (url.pathname === '/api/admin/refresh/lease/acquire' && method === 'POST') {
    const authFailure = await (deps.enforceAdminAuth ?? enforceAdminAuth)(
      request,
      env,
      corsHeaders,
      clientIP,
    );
    if (authFailure) return authFailure;

    let body: { holder_id?: unknown; holder_type?: unknown; lease_minutes?: unknown };
    try {
      body = await request.json();
    } catch {
      return Response.json({ error: 'Invalid JSON body' }, { status: 400, headers });
    }
    const holderId = typeof body.holder_id === 'string' ? body.holder_id.trim() : '';
    const holderType = body.holder_type as RefreshMutationHolderType;
    const leaseMinutes = body.lease_minutes;
    if (
      !holderId
      || holderId.length > 200
      || !EXTERNAL_MUTATION_HOLDERS.has(holderType)
      || typeof leaseMinutes !== 'number'
      || !Number.isInteger(leaseMinutes)
      || leaseMinutes < 1
      || leaseMinutes > 60
    ) {
      return Response.json({ error: 'Invalid mutation lease request' }, { status: 400, headers });
    }

    const claim = await (deps.claimRefreshMutationLock ?? claimRefreshMutationLock)(env.DB, {
      holderId,
      holderType,
      leaseMinutes,
      now,
    });
    return Response.json(claim, {
      status: claim.status === 'claimed' ? 200 : 409,
      headers,
    });
  }

  if (url.pathname === '/api/admin/refresh/lease/release' && method === 'POST') {
    const authFailure = await (deps.enforceAdminAuth ?? enforceAdminAuth)(
      request,
      env,
      corsHeaders,
      clientIP,
    );
    if (authFailure) return authFailure;

    let body: { holder_id?: unknown };
    try {
      body = await request.json();
    } catch {
      return Response.json({ error: 'Invalid JSON body' }, { status: 400, headers });
    }
    const holderId = typeof body.holder_id === 'string' ? body.holder_id.trim() : '';
    if (!holderId || holderId.length > 200) {
      return Response.json({ error: 'Invalid mutation lease release' }, { status: 400, headers });
    }
    const released = await (deps.releaseRefreshMutationLock ?? releaseRefreshMutationLock)(env.DB, {
      holderId,
    });
    return Response.json({ released }, { headers });
  }

  return null;
}
