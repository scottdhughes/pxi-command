// Cloudflare Worker API for PXI
// Connects to Neon PostgreSQL via serverless driver

import { neon } from '@neondatabase/serverless';

interface Env {
  DATABASE_URL: string;
}

interface PXIRow {
  date: string;
  score: string;
  label: string;
  status: string;
  delta_1d: string | null;
  delta_7d: string | null;
  delta_30d: string | null;
}

interface CategoryRow {
  category: string;
  score: string;
  weight: string;
}

// Allowed origins for CORS
const ALLOWED_ORIGINS = [
  'https://pxicommand.com',
  'https://www.pxicommand.com',
  'https://pxi-command.pages.dev',
];

// Rate limiting: simple in-memory store (resets on worker restart)
const rateLimitStore = new Map<string, { count: number; resetTime: number }>();
const RATE_LIMIT = 100; // requests per window
const RATE_WINDOW = 60 * 1000; // 1 minute

function checkRateLimit(ip: string): boolean {
  const now = Date.now();
  const record = rateLimitStore.get(ip);

  if (!record || now > record.resetTime) {
    rateLimitStore.set(ip, { count: 1, resetTime: now + RATE_WINDOW });
    return true;
  }

  if (record.count >= RATE_LIMIT) {
    return false;
  }

  record.count++;
  return true;
}

// Security headers
function getCorsHeaders(origin: string | null): Record<string, string> {
  const allowedOrigin = origin && ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];

  return {
    'Access-Control-Allow-Origin': allowedOrigin,
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '86400',
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'Referrer-Policy': 'strict-origin-when-cross-origin',
    'Content-Security-Policy': "default-src 'none'; frame-ancestors 'none'",
  };
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const origin = request.headers.get('Origin');
    const corsHeaders = getCorsHeaders(origin);

    // Rate limiting
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    if (!checkRateLimit(clientIP)) {
      return Response.json(
        { error: 'Too many requests' },
        { status: 429, headers: { ...corsHeaders, 'Retry-After': '60' } }
      );
    }

    // Only allow GET and OPTIONS methods
    if (request.method !== 'GET' && request.method !== 'OPTIONS') {
      return Response.json(
        { error: 'Method not allowed' },
        { status: 405, headers: corsHeaders }
      );
    }

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // Health check (no cache, always fresh)
    if (url.pathname === '/health') {
      return Response.json({ status: 'healthy', timestamp: new Date().toISOString() }, {
        headers: { ...corsHeaders, 'Cache-Control': 'no-store' },
      });
    }

    // OG Image endpoint for social sharing
    if (url.pathname === '/og-image.svg') {
      try {
        const sql = neon(env.DATABASE_URL);
        const rows = await sql`
          SELECT score, label, status FROM pxi_scores ORDER BY date DESC LIMIT 1
        ` as { score: string; label: string; status: string }[];

        const pxi = rows[0];
        if (!pxi) {
          return new Response('No data', { status: 404 });
        }

        const score = Math.round(parseFloat(pxi.score));
        const label = pxi.label;
        const statusColors: Record<string, string> = {
          max_pamp: '#00a3ff',
          pamping: '#00a3ff',
          neutral: '#949ba5',
          soft: '#949ba5',
          dumping: '#949ba5',
        };
        const color = statusColors[pxi.status] || '#949ba5';
        const isLight = pxi.status === 'neutral' || pxi.status === 'soft';

        const svg = `<svg width="1200" height="630" xmlns="http://www.w3.org/2000/svg">
  <rect width="1200" height="630" fill="#000000"/>
  <text x="600" y="340" text-anchor="middle" font-family="system-ui, -apple-system, sans-serif" font-weight="300" font-size="220" fill="#f3f3f3">${score}</text>
  <rect x="475" y="400" width="250" height="44" rx="4" fill="${color}" fill-opacity="${isLight ? '0.2' : '1'}"/>
  <text x="600" y="432" text-anchor="middle" font-family="monospace" font-weight="500" font-size="16" fill="${isLight ? '#f3f3f3' : '#000000'}" letter-spacing="2">${label}</text>
  <text x="600" y="100" text-anchor="middle" font-family="monospace" font-weight="500" font-size="18" fill="#949ba5" letter-spacing="4">PXI/COMMAND</text>
  <text x="600" y="580" text-anchor="middle" font-family="system-ui, sans-serif" font-size="14" fill="#949ba5" opacity="0.5">MACRO MARKET STRENGTH INDEX</text>
</svg>`;

        return new Response(svg, {
          headers: {
            'Content-Type': 'image/svg+xml',
            'Cache-Control': 'public, max-age=300',
            ...corsHeaders,
          },
        });
      } catch (err: unknown) {
        // Log error internally but don't expose details to client
        console.error('OG image error:', err instanceof Error ? err.message : err);
        return new Response('Service unavailable', { status: 500, headers: corsHeaders });
      }
    }

    // Main PXI endpoint
    if (url.pathname === '/api/pxi') {
      try {
        const sql = neon(env.DATABASE_URL);

        // Get latest PXI score
        const pxiRows = await sql`
          SELECT date, score, label, status, delta_1d, delta_7d, delta_30d
          FROM pxi_scores ORDER BY date DESC LIMIT 1
        ` as PXIRow[];

        const pxi = pxiRows[0];

        if (!pxi) {
          return Response.json({ error: 'No data' }, { status: 404, headers: corsHeaders });
        }

        // Get categories for the same date
        const catRows = await sql`
          SELECT category, score, weight
          FROM category_scores WHERE date = ${pxi.date}
        ` as CategoryRow[];

        // Get sparkline (last 30 days)
        const sparkRows = await sql`
          SELECT date, score FROM pxi_scores ORDER BY date DESC LIMIT 30
        ` as { date: string; score: string }[];

        const response = {
          date: pxi.date,
          score: parseFloat(pxi.score),
          label: pxi.label,
          status: pxi.status,
          delta: {
            d1: pxi.delta_1d ? parseFloat(pxi.delta_1d) : null,
            d7: pxi.delta_7d ? parseFloat(pxi.delta_7d) : null,
            d30: pxi.delta_30d ? parseFloat(pxi.delta_30d) : null,
          },
          categories: catRows.map((c: CategoryRow) => ({
            name: c.category,
            score: parseFloat(c.score),
            weight: parseFloat(c.weight),
          })),
          sparkline: sparkRows.reverse().map((r: { date: string; score: string }) => ({
            date: r.date,
            score: parseFloat(r.score),
          })),
        };

        return Response.json(response, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'public, max-age=60', // Cache for 1 minute
          },
        });
      } catch (err: unknown) {
        // Log error internally but don't expose details to client
        console.error('API error:', err instanceof Error ? err.message : err);
        return Response.json({ error: 'Service unavailable' }, { status: 500, headers: corsHeaders });
      }
    }

    // 404 for unknown routes
    return Response.json({ error: 'Not found' }, { status: 404, headers: corsHeaders });
  },
};
