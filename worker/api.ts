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

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // Health check
    if (url.pathname === '/health') {
      return Response.json({ status: 'healthy', timestamp: new Date().toISOString() }, {
        headers: corsHeaders,
      });
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

        return Response.json(response, { headers: corsHeaders });
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : 'Unknown error';
        return Response.json({ error: message }, { status: 500, headers: corsHeaders });
      }
    }

    return Response.json({ error: 'Not found' }, { status: 404, headers: corsHeaders });
  },
};
