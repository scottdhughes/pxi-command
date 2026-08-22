import assert from 'node:assert/strict';
import test from 'node:test';

import { consumeRequestBudget } from './request-rate-limits.js';

function createDb(consumedCounts: Array<number | null>, queries: string[]): D1Database {
  const statement = (sql: string) => ({
    bind: () => statement(sql),
    first: async () => {
      queries.push(sql);
      const count = consumedCounts.shift();
      return count === null || count === undefined ? null : { count };
    },
    run: async () => {
      queries.push(sql);
      return { success: true };
    },
  });
  return { prepare: (sql: string) => statement(sql) } as unknown as D1Database;
}

test('a new global budget window prunes old buckets once', async () => {
  const queries: string[] = [];
  const allowed = await consumeRequestBudget(createDb([1], queries), 'email-global', 'all', 5, 3600, 1_800_000);
  assert.equal(allowed, true);
  assert.match(queries[0], /ON CONFLICT\(scope, subject_hash, window_start\)/);
  assert.match(queries[0], /WHERE request_rate_limit_buckets\.count < \?/);
  assert.match(queries[1], /DELETE FROM request_rate_limit_buckets/);
});

test('atomic request budgets reject a window with no returned slot', async () => {
  const queries: string[] = [];
  const allowed = await consumeRequestBudget(createDb([null], queries), 'email', '203.0.113.1', 5, 3600, 1_800_000);
  assert.equal(allowed, false);
  assert.equal(queries.length, 1);
});
