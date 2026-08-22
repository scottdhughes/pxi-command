import assert from 'node:assert/strict';
import test from 'node:test';

import type { RefreshSchedulerHealth } from '../data/refresh-scheduler';
import type { NativeRefreshSummary } from '../services/scheduled-refresh';
import { tryHandleSchedulerOpsRoute } from './scheduler-ops';

function createRoute(
  path: string,
  init: RequestInit = {},
  envOverrides: Record<string, unknown> = {},
  clientIP = 'scheduler-ops-test',
) {
  const request = new Request(`https://pxi.test${path}`, init);
  return {
    request,
    env: {
      DB: {},
      AI: {},
      VECTORIZE: {},
      ...envOverrides,
    },
    url: new URL(request.url),
    method: request.method,
    corsHeaders: { 'Access-Control-Allow-Origin': 'https://pxicommand.com' },
    clientIP,
  };
}

function schedulerHealth(
  state: RefreshSchedulerHealth['state'],
  decisionDate = '2026-08-21',
): RefreshSchedulerHealth {
  return {
    state,
    checked_at: '2026-08-21T14:00:00.000Z',
    decision_date: decisionDate,
    market_day_expected: state === 'not_expected' ? false : true,
    latest_daily_close: null,
    latest_incident: null,
  };
}

test('GET /health/refresh uses the current New York market date and disables caching', async () => {
  const now = new Date('2026-08-21T14:00:00.000Z');
  const received: { options?: Record<string, unknown> } = {};
  const route = createRoute('/health/refresh');

  const response = await tryHandleSchedulerOpsRoute(route as any, {
    now: () => now,
    fetchRefreshSchedulerHealth: async (_db, options) => {
      received.options = options as unknown as Record<string, unknown>;
      return schedulerHealth('pending');
    },
  });

  assert.ok(response);
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('Cache-Control'), 'no-store');
  assert.equal(response.headers.get('Access-Control-Allow-Origin'), 'https://pxicommand.com');
  assert.equal(received.options?.decisionDate, '2026-08-21');
  assert.equal(received.options?.isUsMarketDay, true);
  assert.equal(received.options?.now, now);
  assert.deepEqual(await response.json(), schedulerHealth('pending'));
});

test('GET /health/refresh maps only missed and unknown states to 503', async () => {
  for (const [state, expectedStatus] of [
    ['healthy', 200],
    ['pending', 200],
    ['not_expected', 200],
    ['missed', 503],
    ['unknown', 503],
  ] as const) {
    const route = createRoute('/health/refresh');
    const response = await tryHandleSchedulerOpsRoute(route as any, {
      now: () => new Date('2026-08-21T14:00:00.000Z'),
      fetchRefreshSchedulerHealth: async () => schedulerHealth(state),
    });
    assert.equal(response?.status, expectedStatus, state);
  }
});

test('GET /health/refresh passes a non-trading New York date to scheduler health', async () => {
  const route = createRoute('/health/refresh');
  let marketDay: boolean | undefined;
  const response = await tryHandleSchedulerOpsRoute(route as any, {
    now: () => new Date('2026-08-22T14:00:00.000Z'),
    fetchRefreshSchedulerHealth: async (_db, options) => {
      marketDay = options?.isUsMarketDay;
      return schedulerHealth('not_expected', '2026-08-22');
    },
  });

  assert.equal(response?.status, 200);
  assert.equal(marketDay, false);
});

test('GET /health/refresh fails closed with a public-safe unknown payload', async () => {
  const route = createRoute('/health/refresh');
  const originalConsoleError = console.error;
  console.error = () => undefined;
  try {
    const response = await tryHandleSchedulerOpsRoute(route as any, {
      now: () => new Date('2026-08-21T14:00:00.000Z'),
      fetchRefreshSchedulerHealth: async () => {
        throw new Error('private database detail');
      },
    });

    assert.equal(response?.status, 503);
    assert.equal(response?.headers.get('Cache-Control'), 'no-store');
    const payload = await response!.json() as Record<string, unknown>;
    assert.equal(payload.state, 'unknown');
    assert.equal(payload.decision_date, '2026-08-21');
    assert.doesNotMatch(JSON.stringify(payload), /private database detail/);
  } finally {
    console.error = originalConsoleError;
  }
});

test('POST /api/admin/refresh/run enforces existing admin authentication', async () => {
  let pipelineCalls = 0;
  const route = createRoute(
    '/api/admin/refresh/run',
    { method: 'POST' },
    { WRITE_API_KEY: 'test-secret' },
    'scheduler-ops-unauthorized',
  );

  const response = await tryHandleSchedulerOpsRoute(route as any, {
    runNativeRefreshPipeline: async () => {
      pipelineCalls += 1;
      throw new Error('must not run');
    },
  });

  assert.equal(response?.status, 401);
  assert.equal(pipelineCalls, 0);
});

test('POST /api/admin/refresh/run invokes a non-evidence deploy smoke at current time', async () => {
  const now = new Date('2026-08-21T14:00:00.000Z');
  const summary: NativeRefreshSummary = {
    decision_date: '2026-08-21',
    schedule_id: 'deploy_smoke',
    indicators_fetched: 12,
    indicators_written: 12,
    sla: {
      checked: 12,
      critical_failures: [],
      non_critical_failures: 0,
    },
    recalculate: { ok: true, evidence_status: 'not_requested' },
    evaluation: { evaluated: 1 },
    products: { publication_status: 'refreshed' },
  };
  let invocation: { schedule: unknown; scheduledTime: number } | null = null;
  const route = createRoute(
    '/api/admin/refresh/run',
    {
      method: 'POST',
      headers: { Authorization: 'Bearer test-secret' },
    },
    { WRITE_API_KEY: 'test-secret' },
    'scheduler-ops-authorized',
  );

  const response = await tryHandleSchedulerOpsRoute(route as any, {
    now: () => now,
    runNativeRefreshPipeline: async (_env, schedule, scheduledTime) => {
      invocation = { schedule, scheduledTime };
      return summary;
    },
  });

  assert.equal(response?.status, 200);
  assert.equal(response?.headers.get('Cache-Control'), 'no-store');
  assert.deepEqual(invocation, {
    schedule: {
      schedule_id: 'deploy_smoke',
      record_research_evidence: false,
    },
    scheduledTime: now.getTime(),
  });
  assert.deepEqual(await response!.json(), summary);
});

test('POST /api/admin/refresh/run returns only a stable failure code', async () => {
  const route = createRoute(
    '/api/admin/refresh/run',
    {
      method: 'POST',
      headers: { Authorization: 'Bearer test-secret' },
    },
    { WRITE_API_KEY: 'test-secret' },
  );
  const originalConsoleError = console.error;
  console.error = () => undefined;
  try {
    const response = await tryHandleSchedulerOpsRoute(route as any, {
      now: () => new Date('2026-08-21T14:00:00.000Z'),
      runNativeRefreshPipeline: async () => {
        throw new Error('FRED_API_KEY not configured: private detail');
      },
    });

    assert.equal(response?.status, 503);
    assert.deepEqual(await response!.json(), {
      error: 'Refresh smoke failed',
      failure_code: 'missing_fred_api_key',
    });
  } finally {
    console.error = originalConsoleError;
  }
});

test('scheduler operations route ignores unrelated paths and methods', async () => {
  const unrelated = await tryHandleSchedulerOpsRoute(
    createRoute('/health') as any,
  );
  const wrongMethod = await tryHandleSchedulerOpsRoute(
    createRoute('/health/refresh', { method: 'POST' }) as any,
  );
  assert.equal(unrelated, null);
  assert.equal(wrongMethod, null);
});

test('mutation lease acquisition is authenticated, strict, and public-safe', async () => {
  const route = createRoute('/api/admin/refresh/lease/acquire', {
    method: 'POST',
    headers: {
      Authorization: 'Bearer test-secret',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      holder_id: 'github:123:1',
      holder_type: 'github_daily_refresh',
      lease_minutes: 45,
    }),
  }, { WRITE_API_KEY: 'test-secret' });
  const receivedOptions: Array<Record<string, unknown>> = [];
  const response = await tryHandleSchedulerOpsRoute(route as any, {
    now: () => new Date('2026-08-21T14:00:00.000Z'),
    claimRefreshMutationLock: async (_db, received) => {
      receivedOptions.push(received as unknown as Record<string, unknown>);
      return {
        status: 'claimed',
        lock_name: 'indicator_score_mutation',
        holder_type: 'github_daily_refresh',
        acquired_at: '2026-08-21T14:00:00.000Z',
        expires_at: '2026-08-21T14:45:00.000Z',
        lease_version: 1,
        reclaimed: false,
      };
    },
  });
  assert.equal(response?.status, 200);
  assert.equal(response?.headers.get('Cache-Control'), 'no-store');
  assert.equal(receivedOptions[0]?.holderId, 'github:123:1');
  assert.equal(receivedOptions[0]?.holderType, 'github_daily_refresh');
  assert.equal(receivedOptions[0]?.leaseMinutes, 45);
  assert.doesNotMatch(JSON.stringify(await response!.json()), /github:123:1/);
});

test('mutation lease acquisition accepts the manual market-backfill holder', async () => {
  const route = createRoute('/api/admin/refresh/lease/acquire', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      holder_id: 'market_backfill:123:1',
      holder_type: 'market_backfill',
      lease_minutes: 60,
    }),
  });
  let receivedHolderType = '';
  const response = await tryHandleSchedulerOpsRoute(route as any, {
    enforceAdminAuth: async () => null,
    claimRefreshMutationLock: async (_db, received) => {
      receivedHolderType = received.holderType;
      return {
        status: 'claimed',
        lock_name: 'indicator_score_mutation',
        holder_type: 'market_backfill',
        acquired_at: '2026-08-21T12:00:00.000Z',
        expires_at: '2026-08-21T13:00:00.000Z',
        lease_version: 1,
        reclaimed: false,
      };
    },
  });

  assert.equal(response?.status, 200);
  assert.equal(receivedHolderType, 'market_backfill');
});

test('mutation lease reports contention and exact-holder release', async () => {
  const acquire = createRoute('/api/admin/refresh/lease/acquire', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      holder_id: 'history:123',
      holder_type: 'history_reconstruction',
      lease_minutes: 45,
    }),
  });
  const blocked = await tryHandleSchedulerOpsRoute(acquire as any, {
    enforceAdminAuth: async () => null,
    claimRefreshMutationLock: async () => ({
      status: 'not_claimed',
      lock_name: 'indicator_score_mutation',
      holder_type: 'cloudflare_cron',
      expires_at: '2026-08-21T14:30:00.000Z',
      reason: 'active_lease',
    }),
  });
  assert.equal(blocked?.status, 409);

  const release = createRoute('/api/admin/refresh/lease/release', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ holder_id: 'history:123' }),
  });
  let releasedHolder = '';
  const released = await tryHandleSchedulerOpsRoute(release as any, {
    enforceAdminAuth: async () => null,
    releaseRefreshMutationLock: async (_db, received) => {
      releasedHolder = received.holderId;
      return true;
    },
  });
  assert.equal(released?.status, 200);
  assert.deepEqual(await released!.json(), { released: true });
  assert.equal(releasedHolder, 'history:123');
});
