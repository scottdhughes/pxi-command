import assert from 'node:assert/strict';
import test from 'node:test';

import { checkPublicRateLimit, checkRouteRateLimit } from './lib/security.js';

function createKv() {
  const values = new Map<string, string>();
  const puts: Array<{ key: string; expirationTtl: number | undefined }> = [];
  return {
    values,
    puts,
    namespace: {
      async get(key: string) {
        return values.get(key) ?? null;
      },
      async put(key: string, value: string, options?: { expirationTtl?: number }) {
        values.set(key, value);
        puts.push({ key, expirationTtl: options?.expirationTtl });
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

test('KV rate-limit writes respect Cloudflare minimum TTL', async () => {
  const kv = createKv();
  const env = { RATE_LIMIT_KV: kv.namespace } as any;
  const key = 'near-window-end';
  kv.values.set(`rate_limit:route:test:${key}`, JSON.stringify({
    count: 1,
    resetTime: Date.now() + 1_000,
  }));

  assert.equal(await checkRouteRateLimit('test', key, 10, 60_000, env), true);
  assert.equal(kv.puts.at(-1)?.expirationTtl, 60);
});
