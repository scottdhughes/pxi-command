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

const { fetchLatestSignalsThemes } = await import('./runtime/legacy.js');

function requestFrom(input: RequestInfo | URL, init?: RequestInit): Request {
  return input instanceof Request ? input : new Request(input, init);
}

test('signals themes use the service binding when it is available', async (t) => {
  t.mock.method(globalThis, 'fetch', async () => {
    throw new Error('public fetch must not run when SIGNALS_SERVICE is bound');
  });

  const requests: Request[] = [];
  const signalsService: Pick<Fetcher, 'fetch'> = {
    async fetch(input, init) {
      const request = requestFrom(input, init);
      requests.push(request);

      if (request.url.endsWith('/signals/api/runs?status=ok')) {
        return Response.json({ runs: [{ id: '20260820-test-run' }] });
      }
      return Response.json({
        themes: [{
          theme_id: 'defensive-assets',
          theme_name: 'Defensive Assets',
          score: 72,
          key_tickers: ['GLD', 'TLT'],
          classification: {
            signal_type: 'bullish',
            confidence: 'high',
            timing: 'current',
          },
        }],
      });
    },
  };

  const themes = await fetchLatestSignalsThemes({ signals_service: signalsService });

  assert.equal(requests.length, 2);
  assert.equal(requests[0].headers.get('Accept'), 'application/json');
  assert.equal(requests[0].url, 'https://pxicommand.com/signals/api/runs?status=ok');
  assert.equal(requests[1].url, 'https://pxicommand.com/signals/api/runs/20260820-test-run');
  assert.deepEqual(themes, [{
    theme_id: 'defensive-assets',
    theme_name: 'Defensive Assets',
    score: 72,
    key_tickers: ['GLD', 'TLT'],
    classification: {
      signal_type: 'bullish',
      confidence: 'high',
      timing: 'current',
    },
  }]);
});

test('signals themes fall back to the public URL when the binding is absent', async (t) => {
  const urls: string[] = [];
  t.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = requestFrom(input, init);
    urls.push(request.url);
    if (request.url.endsWith('/signals/api/runs?status=ok')) {
      return Response.json({ runs: [{ id: '20260820-public-run' }] });
    }
    return Response.json({
      themes: [{ theme_id: 'growth', theme_name: 'Growth', score: 61, key_tickers: ['SPY'] }],
    });
  });

  const themes = await fetchLatestSignalsThemes();

  assert.deepEqual(urls, [
    'https://pxicommand.com/signals/api/runs?status=ok',
    'https://pxicommand.com/signals/api/runs/20260820-public-run',
  ]);
  assert.equal(themes[0]?.theme_id, 'growth');
});

test('signals themes fail closed when the service returns non-JSON', async () => {
  let calls = 0;
  const signalsService: Pick<Fetcher, 'fetch'> = {
    async fetch() {
      calls += 1;
      return new Response('<!doctype html><title>Unexpected route</title>', {
        status: 200,
        headers: { 'Content-Type': 'text/html; charset=UTF-8' },
      });
    },
  };

  const themes = await fetchLatestSignalsThemes({ signals_service: signalsService });

  assert.deepEqual(themes, []);
  assert.equal(calls, 1);
});
