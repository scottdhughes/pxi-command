// Cloudflare Worker API for PXI
// Uses D1 (SQLite), Vectorize, and Workers AI

interface Env {
  DB: D1Database;
  VECTORIZE: VectorizeIndex;
  AI: Ai;
}

interface PXIRow {
  date: string;
  score: number;
  label: string;
  status: string;
  delta_1d: number | null;
  delta_7d: number | null;
  delta_30d: number | null;
}

interface CategoryRow {
  category: string;
  score: number;
  weight: number;
}

interface SparklineRow {
  date: string;
  score: number;
}

interface IndicatorRow {
  indicator_id: string;
  value: number;
}

// Allowed origins for CORS
const ALLOWED_ORIGINS = [
  'https://pxicommand.com',
  'https://www.pxicommand.com',
  'https://pxi-command.pages.dev',
  'https://pxi-frontend.pages.dev',
];

// Rate limiting: simple in-memory store
const rateLimitStore = new Map<string, { count: number; resetTime: number }>();
const RATE_LIMIT = 100;
const RATE_WINDOW = 60 * 1000;

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

function getCorsHeaders(origin: string | null): Record<string, string> {
  const allowedOrigin = origin && ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];

  return {
    'Access-Control-Allow-Origin': allowedOrigin,
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
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

    // Only allow GET, POST, and OPTIONS
    if (!['GET', 'POST', 'OPTIONS'].includes(request.method)) {
      return Response.json(
        { error: 'Method not allowed' },
        { status: 405, headers: corsHeaders }
      );
    }

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    try {
      // Health check
      if (url.pathname === '/health') {
        const result = await env.DB.prepare('SELECT 1 as ok').first();
        return Response.json(
          { status: 'healthy', db: result?.ok === 1, timestamp: new Date().toISOString() },
          { headers: { ...corsHeaders, 'Cache-Control': 'no-store' } }
        );
      }

      // OG Image endpoint
      if (url.pathname === '/og-image.svg') {
        const pxi = await env.DB.prepare(
          'SELECT score, label, status FROM pxi_scores ORDER BY date DESC LIMIT 1'
        ).first<{ score: number; label: string; status: string }>();

        if (!pxi) {
          return new Response('No data', { status: 404, headers: corsHeaders });
        }

        const score = Math.round(pxi.score);
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
      }

      // Main PXI endpoint
      if (url.pathname === '/api/pxi') {
        // Get latest PXI score
        const pxi = await env.DB.prepare(
          'SELECT date, score, label, status, delta_1d, delta_7d, delta_30d FROM pxi_scores ORDER BY date DESC LIMIT 1'
        ).first<PXIRow>();

        if (!pxi) {
          return Response.json({ error: 'No data' }, { status: 404, headers: corsHeaders });
        }

        // Get categories for the same date
        const catResult = await env.DB.prepare(
          'SELECT category, score, weight FROM category_scores WHERE date = ?'
        ).bind(pxi.date).all<CategoryRow>();

        // Get sparkline (last 30 days)
        const sparkResult = await env.DB.prepare(
          'SELECT date, score FROM pxi_scores ORDER BY date DESC LIMIT 30'
        ).all<SparklineRow>();

        const response = {
          date: pxi.date,
          score: pxi.score,
          label: pxi.label,
          status: pxi.status,
          delta: {
            d1: pxi.delta_1d,
            d7: pxi.delta_7d,
            d30: pxi.delta_30d,
          },
          categories: (catResult.results || []).map((c) => ({
            name: c.category,
            score: c.score,
            weight: c.weight,
          })),
          sparkline: (sparkResult.results || []).reverse().map((r) => ({
            date: r.date,
            score: r.score,
          })),
        };

        return Response.json(response, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'public, max-age=60',
          },
        });
      }

      // AI: Find similar market regimes
      if (url.pathname === '/api/similar' && request.method === 'GET') {
        // Get today's indicator snapshot
        const latestDate = await env.DB.prepare(
          'SELECT date FROM pxi_scores ORDER BY date DESC LIMIT 1'
        ).first<{ date: string }>();

        if (!latestDate) {
          return Response.json({ error: 'No data' }, { status: 404, headers: corsHeaders });
        }

        // Get indicator values for embedding
        const indicators = await env.DB.prepare(`
          SELECT indicator_id, value FROM indicator_values
          WHERE date = ? ORDER BY indicator_id
        `).bind(latestDate.date).all<IndicatorRow>();

        if (!indicators.results || indicators.results.length === 0) {
          return Response.json({ error: 'No indicators' }, { status: 404, headers: corsHeaders });
        }

        // Create text representation for embedding
        const indicatorText = indicators.results
          .map((i) => `${i.indicator_id}: ${i.value.toFixed(2)}`)
          .join(', ');

        // Generate embedding using Workers AI
        const embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
          text: indicatorText,
        });

        // Query Vectorize for similar days
        const similar = await env.VECTORIZE.query(embedding.data[0], {
          topK: 5,
          returnMetadata: 'all',
        });

        // Get PXI scores for similar dates
        const similarDates = similar.matches
          .filter((m) => m.metadata?.date)
          .map((m) => m.metadata!.date as string);

        if (similarDates.length === 0) {
          return Response.json({
            current_date: latestDate.date,
            similar_periods: [],
            message: 'No historical embeddings yet. Run /api/embed to generate.',
          }, { headers: corsHeaders });
        }

        const historicalScores = await env.DB.prepare(`
          SELECT date, score, label, status FROM pxi_scores
          WHERE date IN (${similarDates.map(() => '?').join(',')})
        `).bind(...similarDates).all<PXIRow>();

        return Response.json({
          current_date: latestDate.date,
          similar_periods: similar.matches.map((m, i) => ({
            date: m.metadata?.date,
            similarity: m.score,
            pxi: historicalScores.results?.find((s) => s.date === m.metadata?.date),
          })),
        }, { headers: corsHeaders });
      }

      // AI: Generate embeddings for historical data
      if (url.pathname === '/api/embed' && request.method === 'POST') {
        const dates = await env.DB.prepare(
          'SELECT DISTINCT date FROM indicator_values ORDER BY date'
        ).all<{ date: string }>();

        let embedded = 0;
        const batchSize = 10;

        for (let i = 0; i < (dates.results?.length || 0); i += batchSize) {
          const batch = dates.results!.slice(i, i + batchSize);

          for (const { date } of batch) {
            const indicators = await env.DB.prepare(`
              SELECT indicator_id, value FROM indicator_values
              WHERE date = ? ORDER BY indicator_id
            `).bind(date).all<IndicatorRow>();

            if (!indicators.results || indicators.results.length < 10) continue;

            const indicatorText = indicators.results
              .map((ind) => `${ind.indicator_id}: ${ind.value.toFixed(2)}`)
              .join(', ');

            const embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
              text: indicatorText,
            });

            await env.VECTORIZE.upsert([{
              id: date,
              values: embedding.data[0],
              metadata: { date },
            }]);

            embedded++;
          }
        }

        return Response.json({
          success: true,
          embedded_dates: embedded,
        }, { headers: corsHeaders });
      }

      // Write endpoint for fetchers (requires API key)
      if (url.pathname === '/api/write' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization');
        const apiKey = authHeader?.replace('Bearer ', '');

        // Simple API key check (set via wrangler secret)
        if (!apiKey || apiKey !== (env as any).WRITE_API_KEY) {
          return Response.json({ error: 'Unauthorized' }, { status: 401, headers: corsHeaders });
        }

        const body = await request.json() as {
          type: 'indicator' | 'category' | 'pxi';
          data: any;
        };

        if (body.type === 'indicator') {
          const { indicator_id, date, value, source } = body.data;
          await env.DB.prepare(`
            INSERT OR REPLACE INTO indicator_values (indicator_id, date, value, source)
            VALUES (?, ?, ?, ?)
          `).bind(indicator_id, date, value, source).run();
        } else if (body.type === 'category') {
          const { category, date, score, weight, weighted_score } = body.data;
          await env.DB.prepare(`
            INSERT OR REPLACE INTO category_scores (category, date, score, weight, weighted_score)
            VALUES (?, ?, ?, ?, ?)
          `).bind(category, date, score, weight, weighted_score).run();
        } else if (body.type === 'pxi') {
          const { date, score, label, status, delta_1d, delta_7d, delta_30d } = body.data;
          await env.DB.prepare(`
            INSERT OR REPLACE INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d)
            VALUES (?, ?, ?, ?, ?, ?, ?)
          `).bind(date, score, label, status, delta_1d, delta_7d, delta_30d).run();

          // Generate embedding for this date
          const indicators = await env.DB.prepare(`
            SELECT indicator_id, value FROM indicator_values
            WHERE date = ? ORDER BY indicator_id
          `).bind(date).all<IndicatorRow>();

          if (indicators.results && indicators.results.length >= 10) {
            const indicatorText = indicators.results
              .map((i) => `${i.indicator_id}: ${i.value.toFixed(2)}`)
              .join(', ');

            const embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
              text: indicatorText,
            });

            await env.VECTORIZE.upsert([{
              id: date,
              values: embedding.data[0],
              metadata: { date, score },
            }]);
          }
        }

        return Response.json({ success: true }, { headers: corsHeaders });
      }

      // AI: Analyze current regime
      if (url.pathname === '/api/analyze' && request.method === 'GET') {
        // Get latest PXI and categories
        const pxi = await env.DB.prepare(
          'SELECT date, score, label, status FROM pxi_scores ORDER BY date DESC LIMIT 1'
        ).first<PXIRow>();

        const categories = await env.DB.prepare(
          'SELECT category, score FROM category_scores WHERE date = ? ORDER BY score DESC'
        ).bind(pxi?.date).all<CategoryRow>();

        if (!pxi || !categories.results) {
          return Response.json({ error: 'No data' }, { status: 404, headers: corsHeaders });
        }

        // Create analysis prompt
        const prompt = `Analyze this market regime in 2-3 sentences. Be specific about what's driving conditions.

PXI Score: ${pxi.score.toFixed(1)} (${pxi.label})
Category Breakdown:
${categories.results.map((c) => `- ${c.category}: ${c.score.toFixed(1)}/100`).join('\n')}

Focus on: What's strong? What's weak? What does this suggest for risk appetite?`;

        const analysis = await env.AI.run('@cf/meta/llama-3.1-8b-instruct', {
          prompt,
          max_tokens: 200,
        });

        return Response.json({
          date: pxi.date,
          score: pxi.score,
          label: pxi.label,
          status: pxi.status,
          categories: categories.results,
          analysis: (analysis as { response: string }).response,
        }, { headers: corsHeaders });
      }

      return Response.json({ error: 'Not found' }, { status: 404, headers: corsHeaders });
    } catch (err: unknown) {
      console.error('API error:', err instanceof Error ? err.message : err);
      return Response.json({ error: 'Service unavailable' }, { status: 500, headers: corsHeaders });
    }
  },
};
