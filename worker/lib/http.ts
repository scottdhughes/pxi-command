const ALLOWED_ORIGINS = [
  'https://pxicommand.com',
  'https://www.pxicommand.com',
  'https://pxi-command.pages.dev',
  'https://pxi-frontend.pages.dev',
];

export function getCorsHeaders(origin: string | null): Record<string, string> {
  const allowedOrigin = origin && ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];

  return {
    'Access-Control-Allow-Origin': allowedOrigin,
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS, HEAD',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Admin-Token',
    'Access-Control-Max-Age': '86400',
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'Referrer-Policy': 'strict-origin-when-cross-origin',
    'Content-Security-Policy': "default-src 'none'; frame-ancestors 'none'",
  };
}

export function jsonResponse(body: unknown, init?: ResponseInit): Response {
  return Response.json(body, init);
}

export function htmlResponse(html: string, init?: ResponseInit): Response {
  return new Response(html, {
    ...init,
    headers: {
      'Content-Type': 'text/html; charset=utf-8',
      ...(init?.headers || {}),
    },
  });
}

export function markdownResponse(markdown: string, init?: ResponseInit): Response {
  return new Response(markdown, {
    ...init,
    headers: {
      'Content-Type': 'text/markdown; charset=utf-8',
      ...(init?.headers || {}),
    },
  });
}

export function unauthorizedResponse(corsHeaders: Record<string, string>): Response {
  return jsonResponse({ error: 'Unauthorized' }, { status: 401, headers: corsHeaders });
}

export function methodNotAllowedResponse(corsHeaders: Record<string, string>): Response {
  return jsonResponse({ error: 'Method not allowed' }, { status: 405, headers: corsHeaders });
}

export function preflightResponse(corsHeaders: Record<string, string>): Response {
  return new Response(null, { headers: corsHeaders });
}
