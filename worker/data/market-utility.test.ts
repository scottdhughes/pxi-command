import assert from 'node:assert/strict';
import test from 'node:test';

import { computeUtilityFunnelSummary, insertUtilityEvent } from './market-utility.js';

function createDb(queries: Array<{ sql: string; args: unknown[] }>): D1Database {
  const statement = (sql: string, args: unknown[] = []) => ({
    bind: (...boundArgs: unknown[]) => statement(sql, boundArgs),
    run: async () => {
      queries.push({ sql, args });
      return { success: true };
    },
    first: async () => {
      queries.push({ sql, args });
      return {};
    },
  });
  return { prepare: (sql: string) => statement(sql) } as unknown as D1Database;
}

test('utility event insertion prunes expired rows before the durable write', async () => {
  const queries: Array<{ sql: string; args: unknown[] }> = [];
  await insertUtilityEvent(createDb(queries), {
    session_id: 'session-1',
    event_type: 'plan_view',
    route: '/',
    actionability_state: null,
    payload_json: null,
    created_at: '2026-08-21T18:00:00.000Z',
  });

  assert.equal(queries.length, 2);
  assert.match(queries[0].sql, /DELETE FROM market_utility_events/);
  assert.match(String(queries[0].args[0]), /^\d{4}-\d{2}-\d{2}T/);
  assert.match(queries[1].sql, /INSERT INTO market_utility_events/);
});

test('utility funnel uses index-friendly canonical timestamp predicates', async () => {
  const queries: Array<{ sql: string; args: unknown[] }> = [];
  await computeUtilityFunnelSummary(createDb(queries), 30);

  assert.equal(queries.length, 2);
  for (const query of queries) {
    assert.match(query.sql, /WHERE created_at >= \?/);
    assert.doesNotMatch(query.sql, /datetime\(replace/);
    assert.match(String(query.args[0]), /^\d{4}-\d{2}-\d{2}T/);
  }
});
