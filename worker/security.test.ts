import assert from 'node:assert/strict';
import test from 'node:test';

import { checkPublicRateLimit, checkRouteRateLimit } from './lib/security.js';

function createKv() {
  const values = new Map<string, string>();
  return {
    values,
    namespace: {
      async get(key: string) {
        return values.get(key) ?? null;
      },
      async put(key: string, value: string) {
        values.set(key, value);
      },
    } as KVNamespace,
  };
}

test('public rate limiting persists counters in the configured KV namespace', async () => {
  const kv = createKv();
  const env = { RATE_LIMIT_KV: kv.namespace } as any;
  assert.equal(await checkPublicRateLimit('203.0.113.10', env), true);
  assert.ok([...kv.values.keys()].some((key) => key.startsWith('rate_limit:public:')));
});

test('route rate limiting blocks requests after the scoped durable budget', async () => {
  const kv = createKv();
  const env = { RATE_LIMIT_KV: kv.namespace } as any;
  const scope = `security-test-${Date.now()}`;

  assert.equal(await checkRouteRateLimit(scope, 'client', 2, 60_000, env), true);
  assert.equal(await checkRouteRateLimit(scope, 'client', 2, 60_000, env), true);
  assert.equal(await checkRouteRateLimit(scope, 'client', 2, 60_000, env), false);
});
