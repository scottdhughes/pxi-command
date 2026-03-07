import assert from 'node:assert/strict';
import test from 'node:test';

import {
  claimMarketRefreshRun,
  computeFreshnessSloWindow,
  resolveLatestObservedRefreshTimestamp,
} from './market-refresh.js';

type RefreshRunRow = {
  id: number;
  started_at: string;
  completed_at: string | null;
  status: 'running' | 'success' | 'failed' | 'blocked';
  trigger: string;
  error: string | null;
  created_at: string;
  stale_count?: number | null;
  critical_stale_count?: number | null;
  as_of?: string | null;
};

function createRefreshDb(initialRuns: RefreshRunRow[] = []) {
  const runs = [...initialRuns];
  let nextId = runs.reduce((max, row) => Math.max(max, row.id), 0) + 1;

  const cutoffFromExpr = (expr: unknown): number => {
    const match = /-(\d+)\s+minutes/.exec(String(expr || ''));
    const minutes = match ? Number.parseInt(match[1], 10) : 90;
    return Date.now() - (minutes * 60_000);
  };

  const buildStatement = (sql: string, args: unknown[] = []) => ({
    bind: (...boundArgs: unknown[]) => buildStatement(sql, boundArgs),
    first: async <T>() => {
      if (sql.includes('SELECT completed_at, "trigger" as trigger') && sql.includes("WHERE status IN ('success', 'blocked')")) {
        const row = [...runs]
          .filter((candidate) => (candidate.status === 'success' || candidate.status === 'blocked') && candidate.completed_at)
          .sort((a, b) => new Date(String(b.completed_at)).getTime() - new Date(String(a.completed_at)).getTime() || b.id - a.id)[0];
        if (!row) {
          return null as T | null;
        }
        return {
          completed_at: row.completed_at,
          trigger: row.trigger,
          stale_count: row.stale_count ?? null,
          critical_stale_count: row.critical_stale_count ?? null,
          as_of: row.as_of ?? null,
        } as T;
      }

      if (sql.includes('SELECT id') && sql.includes('FROM market_refresh_runs') && sql.includes("status = 'running'")) {
        const [trigger, lookbackExpr] = args as [string, string];
        const cutoff = cutoffFromExpr(lookbackExpr);
        const row = [...runs]
          .filter((candidate) =>
            candidate.status === 'running' &&
            candidate.trigger === trigger &&
            new Date(candidate.started_at).getTime() >= cutoff,
          )
          .sort((a, b) => new Date(b.started_at).getTime() - new Date(a.started_at).getTime() || b.id - a.id)[0];
        return (row ? { id: row.id } : null) as T | null;
      }

      return null as T | null;
    },
    all: async <T>() => ({ results: [] as T[] }),
    run: async () => {
      if (sql.includes("SET completed_at = ?") && sql.includes("error = 'abandoned_run'")) {
        const [completedAt, trigger, lookbackExpr] = args as [string, string, string];
        const cutoff = cutoffFromExpr(lookbackExpr);
        let changes = 0;
        for (const row of runs) {
          if (
            row.status === 'running' &&
            row.trigger === trigger &&
            new Date(row.started_at).getTime() < cutoff
          ) {
            row.status = 'failed';
            row.completed_at = completedAt;
            row.error = 'abandoned_run';
            changes += 1;
          }
        }
        return { success: true, meta: { changes } };
      }

      if (sql.includes('INSERT INTO market_refresh_runs') && sql.includes('WHERE NOT EXISTS')) {
        const [startedAt, trigger, existingTrigger, lookbackExpr] = args as [string, string, string, string];
        const cutoff = cutoffFromExpr(lookbackExpr);
        const hasFreshRunning = runs.some((row) =>
          row.status === 'running' &&
          row.trigger === existingTrigger &&
          new Date(row.started_at).getTime() >= cutoff,
        );
        if (hasFreshRunning) {
          return { success: true, meta: { changes: 0 } };
        }

        const row: RefreshRunRow = {
          id: nextId,
          started_at: startedAt,
          completed_at: null,
          status: 'running',
          trigger,
          error: null,
          created_at: startedAt,
        };
        runs.push(row);
        nextId += 1;
        return { success: true, meta: { changes: 1, last_row_id: row.id } };
      }

      if (sql.includes('INSERT INTO market_refresh_runs') && sql.includes("VALUES (?, 'running', ?, datetime('now'))")) {
        const [startedAt, trigger] = args as [string, string];
        const row: RefreshRunRow = {
          id: nextId,
          started_at: startedAt,
          completed_at: null,
          status: 'running',
          trigger,
          error: null,
          created_at: startedAt,
        };
        runs.push(row);
        nextId += 1;
        return { success: true, meta: { changes: 1, last_row_id: row.id } };
      }

      throw new Error(`Unhandled SQL in market-refresh test db: ${sql}`);
    },
  });

  return {
    runs,
    prepare(sql: string) {
      return buildStatement(sql);
    },
  };
}

test('claimMarketRefreshRun skips when a fresh running row already exists for the same trigger', async () => {
  const db = createRefreshDb([
    {
      id: 7,
      started_at: new Date(Date.now() - 15 * 60_000).toISOString(),
      completed_at: null,
      status: 'running',
      trigger: 'github_actions',
      error: null,
      created_at: new Date(Date.now() - 15 * 60_000).toISOString(),
    },
  ]);

  const result = await claimMarketRefreshRun(db as unknown as D1Database, 'github_actions');

  assert.deepEqual(result, {
    status: 'skipped',
    run_id: 7,
    refresh_trigger: 'github_actions',
    reason: 'refresh_in_progress',
  });
  assert.equal(db.runs.length, 1);
  assert.equal(db.runs[0]?.status, 'running');
});

test('claimMarketRefreshRun marks stale running rows as failed and starts a replacement run', async () => {
  const staleStartedAt = new Date(Date.now() - 2 * 60 * 60_000).toISOString();
  const db = createRefreshDb([
    {
      id: 4,
      started_at: staleStartedAt,
      completed_at: null,
      status: 'running',
      trigger: 'github_actions',
      error: null,
      created_at: staleStartedAt,
    },
  ]);

  const result = await claimMarketRefreshRun(db as unknown as D1Database, 'github_actions');

  assert.equal(result.status, 'claimed');
  assert.equal(result.refresh_trigger, 'github_actions');
  assert.equal(db.runs.length, 2);
  assert.equal(db.runs[0]?.status, 'failed');
  assert.equal(db.runs[0]?.error, 'abandoned_run');
  assert.equal(db.runs[1]?.status, 'running');
  assert.equal(db.runs[1]?.trigger, 'github_actions');
});

test('claimMarketRefreshRun starts a new run when none exists', async () => {
  const db = createRefreshDb();

  const result = await claimMarketRefreshRun(db as unknown as D1Database, 'manual');

  assert.deepEqual(result, {
    status: 'claimed',
    run_id: 1,
    refresh_trigger: 'manual',
  });
  assert.equal(db.runs.length, 1);
  assert.equal(db.runs[0]?.status, 'running');
  assert.equal(db.runs[0]?.trigger, 'manual');
});

test('resolveLatestObservedRefreshTimestamp prefers blocked refresh runs over snapshot fallback', async () => {
  const completedAt = '2026-03-05T14:00:00.000Z';
  const db = createRefreshDb([
    {
      id: 9,
      started_at: '2026-03-05T13:30:00.000Z',
      completed_at: completedAt,
      status: 'blocked',
      trigger: 'github_actions',
      error: 'decision_impact_enforcement_failed:market_30d_hit_rate_breach',
      created_at: '2026-03-05T13:30:00.000Z',
      stale_count: 0,
      critical_stale_count: 0,
      as_of: '2026-03-05T00:00:00.000Z',
    },
  ]);

  const result = await resolveLatestObservedRefreshTimestamp(db as unknown as D1Database);

  assert.deepEqual(result, {
    last_refresh_at_utc: completedAt,
    source: 'market_refresh_runs',
  });
});

test('computeFreshnessSloWindow counts blocked runs as observed refreshes', async () => {
  const db = {
    prepare(sql: string) {
      const buildStatement = (args: unknown[] = []) => ({
        bind: (...boundArgs: unknown[]) => buildStatement(boundArgs),
        all: async <T>() => {
          if (sql.includes('FROM market_refresh_runs') && sql.includes("status IN ('success', 'blocked')")) {
            return {
              results: [
                {
                  run_date: '2026-03-05',
                  completed_at: '2026-03-05T14:00:00.000Z',
                  trigger: 'github_actions',
                  stale_count: 0,
                  critical_stale_count: 0,
                },
                {
                  run_date: '2026-03-04',
                  completed_at: '2026-03-04T14:00:00.000Z',
                  trigger: 'github_actions',
                  stale_count: 1,
                  critical_stale_count: 1,
                },
              ] as T[],
            };
          }
          return { results: [] as T[] };
        },
        first: async <T>() => {
          if (sql.includes('SUM(CASE WHEN severity =')) {
            return { warning_events: 1, critical_events: 0 } as T;
          }
          return null as T | null;
        },
      });

      return buildStatement();
    },
  };

  const result = await computeFreshnessSloWindow(db as unknown as D1Database, 30);

  assert.equal(result.days_observed, 2);
  assert.equal(result.days_with_critical_stale, 1);
  assert.equal(result.recent_incidents.length, 1);
  assert.equal(result.recent_incidents[0]?.as_of, '2026-03-04');
});
