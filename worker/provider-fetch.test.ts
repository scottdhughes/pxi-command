import assert from 'node:assert/strict';
import test from 'node:test';

type ResolveHook = (
  specifier: string,
  context: unknown,
  nextResolve: (specifier: string, context: unknown) => unknown,
) => unknown;

const { registerHooks } = await import('node:module') as unknown as {
  registerHooks: (hooks: { resolve: ResolveHook }) => void;
};
const cloudflareEmailStub = `data:text/javascript,${encodeURIComponent('export class EmailMessage {}')}`;
registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === 'cloudflare:email') {
      return { url: cloudflareEmailStub, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});

const {
  fetchAllIndicators,
  fetchWithRetry,
  mapWithConcurrency,
} = await import('./runtime/legacy.js');

test('retryable upstream responses are canceled before retrying', async () => {
  let calls = 0;
  let cancellations = 0;
  const fetchImpl = (async () => {
    calls += 1;
    if (calls === 1) {
      return new Response(new ReadableStream({
        cancel() {
          cancellations += 1;
        },
      }), { status: 503 });
    }
    return new Response('ok', { status: 200 });
  }) as unknown as typeof fetch;

  const response = await fetchWithRetry(
    'https://provider.test/data',
    {},
    2,
    { fetchImpl, requestTimeoutMs: 100 },
  );

  assert.equal(await response.text(), 'ok');
  assert.equal(calls, 2);
  assert.equal(cancellations, 1);
});

test('non-retryable upstream responses are canceled before provider fallback', async () => {
  let calls = 0;
  let cancellations = 0;
  const fetchImpl = (async () => {
    calls += 1;
    return new Response(new ReadableStream({
      cancel() {
        cancellations += 1;
      },
    }), { status: 404 });
  }) as unknown as typeof fetch;

  const response = await fetchWithRetry(
    'https://provider.test/missing',
    {},
    3,
    { fetchImpl, requestTimeoutMs: 100 },
  );

  assert.equal(response.status, 404);
  assert.equal(calls, 1);
  assert.equal(cancellations, 1);
});

test('per-request timeout aborts each attempt and preserves the retry bound', async () => {
  let calls = 0;
  let aborts = 0;
  const fetchImpl = ((_input: RequestInfo | URL, init?: RequestInit) => {
    calls += 1;
    return new Promise<Response>((_resolve, reject) => {
      const signal = init?.signal;
      assert.ok(signal);
      const onAbort = () => {
        aborts += 1;
        reject(signal.reason);
      };
      if (signal.aborted) onAbort();
      else signal.addEventListener('abort', onAbort, { once: true });
    });
  }) as unknown as typeof fetch;

  await assert.rejects(fetchWithRetry(
    'https://provider.test/hangs',
    {},
    2,
    { fetchImpl, requestTimeoutMs: 20 },
  ));

  assert.equal(calls, 2);
  assert.equal(aborts, 2);
});

test('provider task pool never exceeds four concurrent operations', async () => {
  let active = 0;
  let maximumActive = 0;
  const items = Array.from({ length: 12 }, (_, index) => index);

  const results = await mapWithConcurrency(items, 99, async (item) => {
    active += 1;
    maximumActive = Math.max(maximumActive, active);
    await new Promise((resolve) => setTimeout(resolve, 10));
    active -= 1;
    return item * 2;
  });

  assert.equal(maximumActive, 4);
  assert.deepEqual(results, items.map((item) => item * 2));
});

test('fetch-stage deadline aborts outstanding work and rejects normally', async () => {
  let active = 0;
  let maximumActive = 0;
  let aborts = 0;
  const fetchImpl = ((_input: RequestInfo | URL, init?: RequestInit) => {
    active += 1;
    maximumActive = Math.max(maximumActive, active);
    return new Promise<Response>((_resolve, reject) => {
      const signal = init?.signal;
      assert.ok(signal);
      const onAbort = () => {
        active -= 1;
        aborts += 1;
        reject(signal.reason);
      };
      if (signal.aborted) onAbort();
      else signal.addEventListener('abort', onAbort, { once: true });
    });
  }) as unknown as typeof fetch;

  const startedAt = Date.now();
  await assert.rejects(
    fetchAllIndicators('test-fred-key', {
      fetchImpl,
      stageTimeoutMs: 40,
      requestTimeoutMs: 5_000,
      concurrency: 99,
    }),
    /Provider fetch stage exceeded 40ms deadline/,
  );

  assert.ok(Date.now() - startedAt < 1_000);
  assert.equal(maximumActive, 4);
  assert.equal(active, 0);
  assert.equal(aborts, 4);
});
