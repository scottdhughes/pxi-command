// Cloudflare Worker API for PXI
// Connects to Neon PostgreSQL

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

// Simple PostgreSQL query helper using fetch (Neon HTTP API)
async function query(env: Env, sql: string, params: unknown[] = []) {
  const url = env.DATABASE_URL.replace('postgresql://', 'https://').replace(/\/[^/]+$/, '/sql');

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Neon-Connection-String': env.DATABASE_URL,
    },
    body: JSON.stringify({
      query: sql,
      params,
    }),
  });

  if (!response.ok) {
    throw new Error(`Database error: ${response.statusText}`);
  }

  return response.json();
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
        // Use Neon's serverless driver approach with fetch
        const dbUrl = env.DATABASE_URL;

        // Parse connection string
        const connMatch = dbUrl.match(/postgresql:\/\/([^:]+):([^@]+)@([^/]+)\/(.+)/);
        if (!connMatch) {
          throw new Error('Invalid DATABASE_URL');
        }

        const [, user, password, host, database] = connMatch;
        const neonHost = host.replace('-pooler', '');

        // Use Neon HTTP API
        const apiUrl = `https://${neonHost}/sql`;

        const pxiQuery = `
          SELECT date, score, label, status, delta_1d, delta_7d, delta_30d
          FROM pxi_scores ORDER BY date DESC LIMIT 1
        `;

        const pxiRes = await fetch(apiUrl, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${password}`,
          },
          body: JSON.stringify({ query: pxiQuery }),
        });

        if (!pxiRes.ok) {
          const text = await pxiRes.text();
          throw new Error(`PXI query failed: ${text}`);
        }

        const pxiData = await pxiRes.json() as { rows: PXIRow[] };
        const pxi = pxiData.rows[0];

        if (!pxi) {
          return Response.json({ error: 'No data' }, { status: 404, headers: corsHeaders });
        }

        // Get categories
        const catQuery = `
          SELECT category, score, weight
          FROM category_scores WHERE date = '${pxi.date}'
        `;

        const catRes = await fetch(apiUrl, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${password}`,
          },
          body: JSON.stringify({ query: catQuery }),
        });

        const catData = await catRes.json() as { rows: CategoryRow[] };

        // Get sparkline
        const sparkQuery = `
          SELECT date, score FROM pxi_scores ORDER BY date DESC LIMIT 30
        `;

        const sparkRes = await fetch(apiUrl, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${password}`,
          },
          body: JSON.stringify({ query: sparkQuery }),
        });

        const sparkData = await sparkRes.json() as { rows: { date: string; score: string }[] };

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
          categories: catData.rows.map((c: CategoryRow) => ({
            name: c.category,
            score: parseFloat(c.score),
            weight: parseFloat(c.weight),
          })),
          sparkline: sparkData.rows.reverse().map((r: { date: string; score: string }) => ({
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
