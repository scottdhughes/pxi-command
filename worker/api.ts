// Cloudflare Worker API for PXI
// Uses D1 (SQLite), Vectorize, and Workers AI
// Includes scheduled cron handler for daily data refresh

interface Env {
  DB: D1Database;
  VECTORIZE: VectorizeIndex;
  AI: Ai;
  FRED_API_KEY?: string;
  WRITE_API_KEY?: string;
}

// ============== Data Fetchers ==============

interface IndicatorValue {
  indicator_id: string;
  date: string;
  value: number;
  source: string;
}

function formatDate(date: Date): string {
  return date.toISOString().split('T')[0];
}

function subYears(date: Date, years: number): Date {
  const d = new Date(date);
  d.setFullYear(d.getFullYear() - years);
  return d;
}

// FRED API fetcher
async function fetchFredSeries(seriesId: string, indicatorId: string, apiKey: string): Promise<IndicatorValue[]> {
  const startDate = formatDate(subYears(new Date(), 3));
  const endDate = formatDate(new Date());

  const url = `https://api.stlouisfed.org/fred/series/observations?series_id=${seriesId}&api_key=${apiKey}&file_type=json&observation_start=${startDate}&observation_end=${endDate}&sort_order=desc&limit=100`;

  const response = await fetch(url);
  if (!response.ok) throw new Error(`FRED API error: ${response.status}`);

  const data = await response.json() as { observations: { date: string; value: string }[] };
  return (data.observations || [])
    .filter(obs => obs.value !== '.')
    .map(obs => ({
      indicator_id: indicatorId,
      date: obs.date,
      value: parseFloat(obs.value),
      source: 'fred',
    }));
}

// Yahoo Finance fetcher (using query2.finance.yahoo.com)
async function fetchYahooSeries(symbol: string, indicatorId: string): Promise<IndicatorValue[]> {
  const period1 = Math.floor(subYears(new Date(), 3).getTime() / 1000);
  const period2 = Math.floor(Date.now() / 1000);

  const url = `https://query2.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?period1=${period1}&period2=${period2}&interval=1d`;

  const response = await fetch(url, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
  });
  if (!response.ok) throw new Error(`Yahoo API error: ${response.status}`);

  const data = await response.json() as any;
  const result = data.chart?.result?.[0];
  if (!result) return [];

  const timestamps = result.timestamp || [];
  const closes = result.indicators?.quote?.[0]?.close || [];

  return timestamps.map((ts: number, i: number) => ({
    indicator_id: indicatorId,
    date: formatDate(new Date(ts * 1000)),
    value: closes[i],
    source: 'yahoo',
  })).filter((v: IndicatorValue) => v.value !== null && v.value !== undefined);
}

// DeFiLlama stablecoin fetcher
async function fetchStablecoinMcap(): Promise<IndicatorValue[]> {
  const response = await fetch('https://stablecoins.llama.fi/stablecoincharts/all');
  if (!response.ok) throw new Error(`DeFiLlama API error: ${response.status}`);

  const data = await response.json() as any[];
  if (!Array.isArray(data)) return [];

  const results: IndicatorValue[] = [];
  for (let i = 30; i < data.length; i++) {
    const current = data[i];
    const past = data[i - 30];
    const currentVal = current.totalCirculating?.peggedUSD || 0;
    const pastVal = past.totalCirculating?.peggedUSD || 0;
    if (pastVal > 0) {
      const roc = ((currentVal - pastVal) / pastVal) * 100;
      results.push({
        indicator_id: 'stablecoin_mcap',
        date: formatDate(new Date(current.date * 1000)),
        value: roc,
        source: 'defillama',
      });
    }
  }
  return results;
}

// CNN Fear & Greed
async function fetchFearGreed(): Promise<IndicatorValue[]> {
  try {
    const response = await fetch('https://production.dataviz.cnn.io/index/fearandgreed/graphdata');
    if (!response.ok) return [];

    const data = await response.json() as any;
    if (data?.fear_and_greed?.score) {
      return [{
        indicator_id: 'fear_greed',
        date: formatDate(new Date()),
        value: data.fear_and_greed.score,
        source: 'cnn',
      }];
    }
  } catch { }
  return [];
}

// Fetch all indicator data
async function fetchAllIndicators(fredApiKey: string): Promise<IndicatorValue[]> {
  const all: IndicatorValue[] = [];

  // FRED indicators
  const fredIndicators = [
    { ticker: 'WALCL', id: 'fed_balance_sheet' },
    { ticker: 'RRPONTSYD', id: 'reverse_repo' },
    { ticker: 'WTREGEN', id: 'treasury_general_account' },
    { ticker: 'BAMLH0A0HYM2', id: 'high_yield_spread' },
    { ticker: 'BAMLC0A4CBBB', id: 'investment_grade_spread' },
    { ticker: 'T10Y2Y', id: 'yield_curve' },
    { ticker: 'DGS10', id: 'ten_year_yield' },
    { ticker: 'DTWEXBGS', id: 'dollar_index' },
  ];

  for (const { ticker, id } of fredIndicators) {
    try {
      const data = await fetchFredSeries(ticker, id, fredApiKey);
      all.push(...data);
      console.log(`FRED ${id}: ${data.length} values`);
    } catch (e) {
      console.error(`FRED ${id} failed:`, e);
    }
  }

  // Calculate net liquidity
  const walcl = all.filter(i => i.indicator_id === 'fed_balance_sheet');
  const tga = all.filter(i => i.indicator_id === 'treasury_general_account');
  const rrp = all.filter(i => i.indicator_id === 'reverse_repo');
  const tgaMap = new Map(tga.map(t => [t.date, t.value]));
  const rrpMap = new Map(rrp.map(r => [r.date, r.value]));

  for (const w of walcl) {
    const t = tgaMap.get(w.date);
    const r = rrpMap.get(w.date);
    if (t !== undefined && r !== undefined) {
      all.push({ indicator_id: 'net_liquidity', date: w.date, value: w.value - t - r, source: 'fred' });
    }
  }

  // Yahoo indicators
  const yahooIndicators = [
    { ticker: '^VIX', id: 'vix' },
    { ticker: 'HYG', id: 'hyg' },
    { ticker: 'LQD', id: 'lqd' },
    { ticker: 'TLT', id: 'tlt' },
    { ticker: 'GLD', id: 'gold' },
    { ticker: 'BTC-USD', id: 'btc_price' },
  ];

  for (const { ticker, id } of yahooIndicators) {
    try {
      const data = await fetchYahooSeries(ticker, id);
      all.push(...data);
      console.log(`Yahoo ${id}: ${data.length} values`);
    } catch (e) {
      console.error(`Yahoo ${id} failed:`, e);
    }
  }

  // Computed Yahoo indicators
  try {
    const [vix, vix3m] = await Promise.all([
      fetchYahooSeries('^VIX', 'vix_temp'),
      fetchYahooSeries('^VIX3M', 'vix3m_temp'),
    ]);
    const vix3mMap = new Map(vix3m.map(v => [v.date, v.value]));
    for (const v of vix) {
      const v3m = vix3mMap.get(v.date);
      if (v3m) {
        all.push({ indicator_id: 'vix_term_structure', date: v.date, value: v.value - v3m, source: 'yahoo' });
      }
    }
    console.log('VIX term structure: calculated');
  } catch (e) {
    console.error('VIX term structure failed:', e);
  }

  try {
    const [rsp, spy] = await Promise.all([
      fetchYahooSeries('RSP', 'rsp_temp'),
      fetchYahooSeries('SPY', 'spy_temp'),
    ]);
    const spyMap = new Map(spy.map(s => [s.date, s.value]));
    for (const r of rsp) {
      const s = spyMap.get(r.date);
      if (s) {
        all.push({ indicator_id: 'rsp_spy_ratio', date: r.date, value: r.value / s, source: 'yahoo' });
      }
    }
    console.log('RSP/SPY ratio: calculated');
  } catch (e) {
    console.error('RSP/SPY ratio failed:', e);
  }

  // Crypto
  try {
    const stableData = await fetchStablecoinMcap();
    all.push(...stableData);
    console.log(`Stablecoin mcap: ${stableData.length} values`);
  } catch (e) {
    console.error('Stablecoin mcap failed:', e);
  }

  // Alternative
  try {
    const fgData = await fetchFearGreed();
    all.push(...fgData);
    console.log(`Fear & Greed: ${fgData.length} values`);
  } catch (e) {
    console.error('Fear & Greed failed:', e);
  }

  return all;
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

// ============== PXI Calculation ==============

interface IndicatorConfig {
  id: string;
  category: string;
  weight: number;
  invert: boolean;
}

// Full 28-indicator configuration matching src/config/indicators.ts
// Total weights sum to 1.0 (100%)
const INDICATOR_CONFIG: IndicatorConfig[] = [
  // ============== LIQUIDITY (22%) ==============
  { id: 'fed_balance_sheet', category: 'liquidity', weight: 0.07, invert: false },
  { id: 'treasury_general_account', category: 'liquidity', weight: 0.05, invert: true },
  { id: 'reverse_repo', category: 'liquidity', weight: 0.05, invert: true },
  { id: 'net_liquidity', category: 'liquidity', weight: 0.05, invert: false },

  // ============== CREDIT (18%) ==============
  { id: 'hy_oas_spread', category: 'credit', weight: 0.05, invert: true },
  { id: 'ig_oas_spread', category: 'credit', weight: 0.04, invert: true },
  { id: 'yield_curve_2s10s', category: 'credit', weight: 0.05, invert: false },
  { id: 'bbb_aaa_spread', category: 'credit', weight: 0.04, invert: true },

  // ============== VOLATILITY (18%) ==============
  { id: 'vix', category: 'volatility', weight: 0.08, invert: true },
  { id: 'vix_term_structure', category: 'volatility', weight: 0.05, invert: true },
  { id: 'aaii_sentiment', category: 'volatility', weight: 0.05, invert: false },

  // ============== BREADTH (12%) ==============
  { id: 'rsp_spy_ratio', category: 'breadth', weight: 0.04, invert: false },
  { id: 'sector_breadth', category: 'breadth', weight: 0.04, invert: false },
  { id: 'small_cap_strength', category: 'breadth', weight: 0.02, invert: false },
  { id: 'midcap_strength', category: 'breadth', weight: 0.02, invert: false },

  // ============== MACRO (10%) ==============
  { id: 'ism_manufacturing', category: 'macro', weight: 0.03, invert: false },
  { id: 'jobless_claims', category: 'macro', weight: 0.04, invert: true },
  { id: 'cfnai', category: 'macro', weight: 0.03, invert: false },

  // ============== GLOBAL (10%) ==============
  { id: 'dxy', category: 'global', weight: 0.03, invert: true },
  { id: 'copper_gold_ratio', category: 'global', weight: 0.03, invert: false },
  { id: 'em_spread', category: 'global', weight: 0.02, invert: true },
  { id: 'audjpy', category: 'global', weight: 0.02, invert: false },

  // ============== CRYPTO (10%) ==============
  { id: 'btc_vs_200dma', category: 'crypto', weight: 0.04, invert: false },
  { id: 'stablecoin_mcap', category: 'crypto', weight: 0.03, invert: false },
  { id: 'btc_price', category: 'crypto', weight: 0.03, invert: false },
];

// Proper empirical percentile calculation
function calculatePercentile(value: number, history: number[]): number {
  if (history.length === 0) return 50;
  const sorted = [...history].sort((a, b) => a - b);

  // Count values less than current value
  let below = 0;
  let equal = 0;
  for (const v of sorted) {
    if (v < value) below++;
    else if (v === value) equal++;
  }

  // Use midpoint for ties (standard practice)
  const rank = below + equal / 2;
  return (rank / history.length) * 100;
}

function getLabel(score: number): string {
  if (score >= 80) return 'MAX PAMP';
  if (score >= 65) return 'PAMPING';
  if (score >= 50) return 'NEUTRAL';
  if (score >= 35) return 'SOFT';
  return 'DUMPING';
}

function getStatus(score: number): string {
  if (score >= 80) return 'max_pamp';
  if (score >= 65) return 'pamping';
  if (score >= 50) return 'neutral';
  if (score >= 35) return 'soft';
  return 'dumping';
}

async function calculatePXI(db: D1Database, targetDate: string): Promise<{
  pxi: { date: string; score: number; label: string; status: string; delta_1d: number | null; delta_7d: number | null; delta_30d: number | null };
  categories: { category: string; date: string; score: number; weight: number; weighted_score: number }[];
} | null> {
  // Get indicator values for target date
  const latestValues = await db.prepare(`
    SELECT indicator_id, value FROM indicator_values WHERE date = ?
  `).bind(targetDate).all<{ indicator_id: string; value: number }>();

  if (!latestValues.results || latestValues.results.length === 0) {
    return null;
  }

  const valueMap = new Map(latestValues.results.map(r => [r.indicator_id, r.value]));

  // Get historical values for percentile calculation (3 years)
  const historyResult = await db.prepare(`
    SELECT indicator_id, value FROM indicator_values
    WHERE date >= date(?, '-3 years')
  `).bind(targetDate).all<{ indicator_id: string; value: number }>();

  const historyMap = new Map<string, number[]>();
  for (const row of historyResult.results || []) {
    if (!historyMap.has(row.indicator_id)) {
      historyMap.set(row.indicator_id, []);
    }
    historyMap.get(row.indicator_id)!.push(row.value);
  }

  // Calculate category scores
  const categoryScores = new Map<string, { total: number; weight: number }>();

  for (const config of INDICATOR_CONFIG) {
    const value = valueMap.get(config.id);
    const history = historyMap.get(config.id) || [];

    if (value === undefined || history.length < 10) continue;

    let percentile = calculatePercentile(value, history);
    if (config.invert) {
      percentile = 100 - percentile;
    }

    const weighted = percentile * config.weight;

    if (!categoryScores.has(config.category)) {
      categoryScores.set(config.category, { total: 0, weight: 0 });
    }
    const cat = categoryScores.get(config.category)!;
    cat.total += weighted;
    cat.weight += config.weight;
  }

  // Calculate final PXI score
  let totalScore = 0;
  let totalWeight = 0;
  const categories: { category: string; date: string; score: number; weight: number; weighted_score: number }[] = [];

  for (const [category, data] of categoryScores) {
    if (data.weight > 0) {
      const categoryScore = (data.total / data.weight);
      totalScore += data.total;
      totalWeight += data.weight;
      categories.push({
        category,
        date: targetDate,
        score: categoryScore,
        weight: data.weight,
        weighted_score: data.total,
      });
    }
  }

  if (totalWeight === 0) return null;

  const pxiScore = totalScore / totalWeight;

  // Get historical PXI for deltas
  const historicalPxi = await db.prepare(`
    SELECT date, score FROM pxi_scores
    WHERE date < ? ORDER BY date DESC LIMIT 30
  `).bind(targetDate).all<{ date: string; score: number }>();

  const pxiHistory = historicalPxi.results || [];
  const delta_1d = pxiHistory.length >= 1 ? pxiScore - pxiHistory[0].score : null;
  const delta_7d = pxiHistory.length >= 7 ? pxiScore - pxiHistory[6].score : null;
  const delta_30d = pxiHistory.length >= 30 ? pxiScore - pxiHistory[29].score : null;

  return {
    pxi: {
      date: targetDate,
      score: pxiScore,
      label: getLabel(pxiScore),
      status: getStatus(pxiScore),
      delta_1d,
      delta_7d,
      delta_30d,
    },
    categories,
  };
}

// ============== Scheduled Handler ==============

async function handleScheduled(env: Env): Promise<void> {
  console.log('🕐 Starting scheduled PXI refresh...');

  if (!env.FRED_API_KEY) {
    throw new Error('FRED_API_KEY not configured');
  }

  // Fetch all indicator data
  console.log('📊 Fetching indicator data...');
  const indicators = await fetchAllIndicators(env.FRED_API_KEY);
  console.log(`📊 Fetched ${indicators.length} indicator values`);

  // Write indicators to D1 in batches
  const BATCH_SIZE = 100;
  let written = 0;

  for (let i = 0; i < indicators.length; i += BATCH_SIZE) {
    const batch = indicators.slice(i, i + BATCH_SIZE);
    const stmts = batch.map(ind =>
      env.DB.prepare(`
        INSERT OR REPLACE INTO indicator_values (indicator_id, date, value, source)
        VALUES (?, ?, ?, ?)
      `).bind(ind.indicator_id, ind.date, ind.value, ind.source)
    );
    await env.DB.batch(stmts);
    written += batch.length;
  }
  console.log(`💾 Wrote ${written} indicator values to D1`);

  // Calculate and store PXI score for today
  const today = formatDate(new Date());
  console.log(`🧮 Calculating PXI for ${today}...`);

  const result = await calculatePXI(env.DB, today);

  if (result) {
    // Write PXI score
    await env.DB.prepare(`
      INSERT OR REPLACE INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).bind(
      result.pxi.date,
      result.pxi.score,
      result.pxi.label,
      result.pxi.status,
      result.pxi.delta_1d,
      result.pxi.delta_7d,
      result.pxi.delta_30d
    ).run();

    // Write category scores
    const catStmts = result.categories.map(cat =>
      env.DB.prepare(`
        INSERT OR REPLACE INTO category_scores (category, date, score, weight, weighted_score)
        VALUES (?, ?, ?, ?, ?)
      `).bind(cat.category, cat.date, cat.score, cat.weight, cat.weighted_score)
    );
    if (catStmts.length > 0) {
      await env.DB.batch(catStmts);
    }

    // Generate embedding for vector similarity
    try {
      const indicatorText = indicators
        .filter(i => i.date === today)
        .map(i => `${i.indicator_id}: ${i.value.toFixed(2)}`)
        .join(', ');

      if (indicatorText) {
        const embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
          text: indicatorText,
        });

        await env.VECTORIZE.upsert([{
          id: today,
          values: embedding.data[0],
          metadata: { date: today, score: result.pxi.score },
        }]);
        console.log('🔮 Generated and stored embedding');
      }
    } catch (e) {
      console.error('Embedding generation failed:', e);
    }

    console.log(`✅ PXI refresh complete: ${result.pxi.score.toFixed(1)} (${result.pxi.label})`);
  } else {
    console.log('⚠️ Could not calculate PXI - insufficient data');
  }
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
        // Find the most recent date with at least 3 categories (skip incomplete weekend data)
        const recentScores = await env.DB.prepare(
          'SELECT date, score, label, status, delta_1d, delta_7d, delta_30d FROM pxi_scores ORDER BY date DESC LIMIT 10'
        ).all<PXIRow>();

        let pxi: PXIRow | null = null;
        let catResult: D1Result<CategoryRow> | null = null;

        for (const candidate of recentScores.results || []) {
          const cats = await env.DB.prepare(
            'SELECT category, score, weight FROM category_scores WHERE date = ?'
          ).bind(candidate.date).all<CategoryRow>();

          if ((cats.results?.length || 0) >= 3) {
            pxi = candidate;
            catResult = cats;
            break;
          }
        }

        // Fallback to latest if no date has 3+ categories
        if (!pxi) {
          pxi = recentScores.results?.[0] || null;
          if (pxi) {
            catResult = await env.DB.prepare(
              'SELECT category, score, weight FROM category_scores WHERE date = ?'
            ).bind(pxi.date).all<CategoryRow>();
          }
        }

        if (!pxi) {
          return Response.json({ error: 'No data' }, { status: 404, headers: corsHeaders });
        }

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
          categories: (catResult?.results || []).map((c) => ({
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

      // Write endpoint for fetchers (requires API key) - supports batch writes
      if (url.pathname === '/api/write' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization');
        const apiKey = authHeader?.replace('Bearer ', '');

        // Simple API key check (set via wrangler secret)
        if (!apiKey || apiKey !== (env as any).WRITE_API_KEY) {
          return Response.json({ error: 'Unauthorized' }, { status: 401, headers: corsHeaders });
        }

        const body = await request.json() as {
          // Legacy single-record format
          type?: 'indicator' | 'category' | 'pxi';
          data?: any;
          // New batch format
          indicators?: { indicator_id: string; date: string; value: number; source: string }[];
          categories?: { category: string; date: string; score: number; weight: number; weighted_score: number }[];
          pxi?: { date: string; score: number; label: string; status: string; delta_1d: number | null; delta_7d: number | null; delta_30d: number | null };
        };

        const stmts: D1PreparedStatement[] = [];

        // Handle legacy single-record format
        if (body.type) {
          if (body.type === 'indicator') {
            const { indicator_id, date, value, source } = body.data;
            stmts.push(env.DB.prepare(`
              INSERT OR REPLACE INTO indicator_values (indicator_id, date, value, source)
              VALUES (?, ?, ?, ?)
            `).bind(indicator_id, date, value, source));
          } else if (body.type === 'category') {
            const { category, date, score, weight, weighted_score } = body.data;
            stmts.push(env.DB.prepare(`
              INSERT OR REPLACE INTO category_scores (category, date, score, weight, weighted_score)
              VALUES (?, ?, ?, ?, ?)
            `).bind(category, date, score, weight, weighted_score));
          } else if (body.type === 'pxi') {
            const { date, score, label, status, delta_1d, delta_7d, delta_30d } = body.data;
            stmts.push(env.DB.prepare(`
              INSERT OR REPLACE INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d)
              VALUES (?, ?, ?, ?, ?, ?, ?)
            `).bind(date, score, label, status, delta_1d, delta_7d, delta_30d));
          }
        }

        // Handle batch indicator values
        for (const ind of body.indicators || []) {
          stmts.push(env.DB.prepare(`
            INSERT OR REPLACE INTO indicator_values (indicator_id, date, value, source)
            VALUES (?, ?, ?, ?)
          `).bind(ind.indicator_id, ind.date, ind.value, ind.source));
        }

        // Handle batch category scores
        for (const cat of body.categories || []) {
          stmts.push(env.DB.prepare(`
            INSERT OR REPLACE INTO category_scores (category, date, score, weight, weighted_score)
            VALUES (?, ?, ?, ?, ?)
          `).bind(cat.category, cat.date, cat.score, cat.weight, cat.weighted_score));
        }

        // Handle PXI score
        if (body.pxi) {
          const p = body.pxi;
          stmts.push(env.DB.prepare(`
            INSERT OR REPLACE INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d)
            VALUES (?, ?, ?, ?, ?, ?, ?)
          `).bind(p.date, p.score, p.label, p.status, p.delta_1d, p.delta_7d, p.delta_30d));
        }

        // Execute in batches (D1 batch limit is ~100 statements)
        const BATCH_SIZE = 100;
        let totalWritten = 0;

        for (let i = 0; i < stmts.length; i += BATCH_SIZE) {
          const batch = stmts.slice(i, i + BATCH_SIZE);
          await env.DB.batch(batch);
          totalWritten += batch.length;
        }

        // Generate embedding if we have PXI data
        if (body.pxi) {
          const indicators = await env.DB.prepare(`
            SELECT indicator_id, value FROM indicator_values
            WHERE date = ? ORDER BY indicator_id
          `).bind(body.pxi.date).all<IndicatorRow>();

          if (indicators.results && indicators.results.length >= 10) {
            try {
              const indicatorText = indicators.results
                .map((i) => `${i.indicator_id}: ${i.value.toFixed(2)}`)
                .join(', ');

              const embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
                text: indicatorText,
              });

              await env.VECTORIZE.upsert([{
                id: body.pxi.date,
                values: embedding.data[0],
                metadata: { date: body.pxi.date, score: body.pxi.score },
              }]);
            } catch (e) {
              // Embedding generation is best-effort, don't fail the whole write
              console.error('Embedding generation failed:', e);
            }
          }
        }

        return Response.json({ success: true, written: totalWritten }, { headers: corsHeaders });
      }

      // Recalculate PXI for a given date (requires auth)
      if (url.pathname === '/api/recalculate' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization');
        const apiKey = authHeader?.replace('Bearer ', '');

        if (!apiKey || apiKey !== (env as any).WRITE_API_KEY) {
          return Response.json({ error: 'Unauthorized' }, { status: 401, headers: corsHeaders });
        }

        const body = await request.json() as { date?: string };
        const targetDate = body.date || formatDate(new Date());

        const result = await calculatePXI(env.DB, targetDate);

        if (!result) {
          return Response.json({ error: 'Insufficient data for calculation', date: targetDate }, { status: 400, headers: corsHeaders });
        }

        // Write PXI score
        await env.DB.prepare(`
          INSERT OR REPLACE INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `).bind(
          result.pxi.date,
          result.pxi.score,
          result.pxi.label,
          result.pxi.status,
          result.pxi.delta_1d,
          result.pxi.delta_7d,
          result.pxi.delta_30d
        ).run();

        // Write category scores
        const catStmts = result.categories.map(cat =>
          env.DB.prepare(`
            INSERT OR REPLACE INTO category_scores (category, date, score, weight, weighted_score)
            VALUES (?, ?, ?, ?, ?)
          `).bind(cat.category, cat.date, cat.score, cat.weight, cat.weighted_score)
        );
        if (catStmts.length > 0) {
          await env.DB.batch(catStmts);
        }

        return Response.json({
          success: true,
          date: targetDate,
          score: result.pxi.score,
          label: result.pxi.label,
          categories: result.categories.length,
        }, { headers: corsHeaders });
      }

      // Backfill historical PXI scores and embeddings (requires auth)
      if (url.pathname === '/api/backfill' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization');
        const apiKey = authHeader?.replace('Bearer ', '');

        if (!apiKey || apiKey !== (env as any).WRITE_API_KEY) {
          return Response.json({ error: 'Unauthorized' }, { status: 401, headers: corsHeaders });
        }

        const body = await request.json() as { start?: string; end?: string; limit?: number };
        const limit = body.limit || 50; // Process 50 dates per request to avoid timeout

        // Get all unique dates with indicator data that don't have PXI scores yet
        const datesResult = await env.DB.prepare(`
          SELECT DISTINCT iv.date
          FROM indicator_values iv
          LEFT JOIN pxi_scores ps ON iv.date = ps.date
          WHERE ps.date IS NULL
          ${body.start ? `AND iv.date >= '${body.start}'` : ''}
          ${body.end ? `AND iv.date <= '${body.end}'` : ''}
          ORDER BY iv.date DESC
          LIMIT ?
        `).bind(limit).all<{ date: string }>();

        const dates = datesResult.results || [];
        let processed = 0;
        let succeeded = 0;
        let embedded = 0;
        const results: { date: string; score?: number; categories?: number; error?: string }[] = [];

        for (const { date } of dates) {
          processed++;
          try {
            const result = await calculatePXI(env.DB, date);

            if (!result || result.categories.length < 2) {
              results.push({ date, error: 'Insufficient data' });
              continue;
            }

            // Write PXI score
            await env.DB.prepare(`
              INSERT OR REPLACE INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d)
              VALUES (?, ?, ?, ?, ?, ?, ?)
            `).bind(
              result.pxi.date,
              result.pxi.score,
              result.pxi.label,
              result.pxi.status,
              result.pxi.delta_1d,
              result.pxi.delta_7d,
              result.pxi.delta_30d
            ).run();

            // Write category scores
            const catStmts = result.categories.map(cat =>
              env.DB.prepare(`
                INSERT OR REPLACE INTO category_scores (category, date, score, weight, weighted_score)
                VALUES (?, ?, ?, ?, ?)
              `).bind(cat.category, cat.date, cat.score, cat.weight, cat.weighted_score)
            );
            if (catStmts.length > 0) {
              await env.DB.batch(catStmts);
            }

            succeeded++;
            results.push({ date, score: result.pxi.score, categories: result.categories.length });

            // Generate embedding for Vectorize
            try {
              const indicators = await env.DB.prepare(`
                SELECT indicator_id, value FROM indicator_values WHERE date = ? ORDER BY indicator_id
              `).bind(date).all<{ indicator_id: string; value: number }>();

              if (indicators.results && indicators.results.length >= 5) {
                const indicatorText = indicators.results
                  .map(i => `${i.indicator_id}: ${i.value.toFixed(4)}`)
                  .join(', ');

                const embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
                  text: indicatorText,
                });

                await env.VECTORIZE.upsert([{
                  id: date,
                  values: embedding.data[0],
                  metadata: { date, score: result.pxi.score, label: result.pxi.label },
                }]);
                embedded++;
              }
            } catch (e) {
              // Embedding is best-effort
              console.error(`Embedding failed for ${date}:`, e);
            }
          } catch (e) {
            results.push({ date, error: e instanceof Error ? e.message : 'Unknown error' });
          }
        }

        // Check remaining dates
        const remainingResult = await env.DB.prepare(`
          SELECT COUNT(DISTINCT iv.date) as cnt
          FROM indicator_values iv
          LEFT JOIN pxi_scores ps ON iv.date = ps.date
          WHERE ps.date IS NULL
        `).first<{ cnt: number }>();

        return Response.json({
          success: true,
          processed,
          succeeded,
          embedded,
          remaining: remainingResult?.cnt || 0,
          results,
        }, { headers: corsHeaders });
      }

      // Predict PXI direction based on similar historical regimes
      if (url.pathname === '/api/predict' && request.method === 'GET') {
        // Get latest indicator values
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
        `).bind(latestDate.date).all<{ indicator_id: string; value: number }>();

        if (!indicators.results || indicators.results.length < 5) {
          return Response.json({ error: 'Insufficient indicators' }, { status: 400, headers: corsHeaders });
        }

        // Create embedding for current state
        const indicatorText = indicators.results
          .map(i => `${i.indicator_id}: ${i.value.toFixed(4)}`)
          .join(', ');

        const embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
          text: indicatorText,
        });

        // Find similar historical periods (excluding recent 30 days)
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
        const cutoffDate = formatDate(thirtyDaysAgo);

        const similar = await env.VECTORIZE.query(embedding.data[0], {
          topK: 10,
          filter: { date: { $lt: cutoffDate } },
          returnMetadata: 'all',
        });

        if (!similar.matches || similar.matches.length === 0) {
          return Response.json({
            error: 'No similar historical periods found',
            hint: 'Run /api/backfill to populate historical embeddings'
          }, { status: 400, headers: corsHeaders });
        }

        // For each similar period, look at what happened next
        const outcomes: { date: string; similarity: number; score: number; d7_change: number | null; d30_change: number | null }[] = [];

        for (const match of similar.matches) {
          const histDate = match.id;
          const histScore = (match.metadata as any)?.score || 0;

          // Get PXI 7 and 30 days after this date
          const futureScores = await env.DB.prepare(`
            SELECT date, score,
              julianday(date) - julianday(?) as days_after
            FROM pxi_scores
            WHERE date > ? AND date <= date(?, '+35 days')
            ORDER BY date
          `).bind(histDate, histDate, histDate).all<{ date: string; score: number; days_after: number }>();

          let d7_change: number | null = null;
          let d30_change: number | null = null;

          for (const fs of futureScores.results || []) {
            if (fs.days_after >= 5 && fs.days_after <= 10 && d7_change === null) {
              d7_change = fs.score - histScore;
            }
            if (fs.days_after >= 25 && fs.days_after <= 35 && d30_change === null) {
              d30_change = fs.score - histScore;
            }
          }

          outcomes.push({
            date: histDate,
            similarity: match.score,
            score: histScore,
            d7_change,
            d30_change,
          });
        }

        // Calculate weighted predictions
        const validD7 = outcomes.filter(o => o.d7_change !== null);
        const validD30 = outcomes.filter(o => o.d30_change !== null);

        const weightedAvg = (items: typeof outcomes, key: 'd7_change' | 'd30_change') => {
          const valid = items.filter(i => i[key] !== null);
          if (valid.length === 0) return null;
          const totalWeight = valid.reduce((sum, i) => sum + i.similarity, 0);
          const weightedSum = valid.reduce((sum, i) => sum + (i[key]! * i.similarity), 0);
          return weightedSum / totalWeight;
        };

        const d7_prediction = weightedAvg(outcomes, 'd7_change');
        const d30_prediction = weightedAvg(outcomes, 'd30_change');

        // Calculate confidence (based on agreement of similar periods)
        const d7_directions = validD7.map(o => o.d7_change! > 0 ? 1 : -1);
        const d7_agreement = d7_directions.length > 0
          ? Math.abs(d7_directions.reduce((a, b) => a + b, 0)) / d7_directions.length
          : 0;

        const d30_directions = validD30.map(o => o.d30_change! > 0 ? 1 : -1);
        const d30_agreement = d30_directions.length > 0
          ? Math.abs(d30_directions.reduce((a, b) => a + b, 0)) / d30_directions.length
          : 0;

        // Get current PXI
        const currentPxi = await env.DB.prepare(
          'SELECT score, label FROM pxi_scores WHERE date = ?'
        ).first<{ score: number; label: string }>(latestDate.date);

        return Response.json({
          current: {
            date: latestDate.date,
            score: currentPxi?.score,
            label: currentPxi?.label,
          },
          prediction: {
            d7: d7_prediction !== null ? {
              expected_change: d7_prediction,
              direction: d7_prediction > 0 ? 'UP' : 'DOWN',
              confidence: d7_agreement,
              based_on: validD7.length,
            } : null,
            d30: d30_prediction !== null ? {
              expected_change: d30_prediction,
              direction: d30_prediction > 0 ? 'UP' : 'DOWN',
              confidence: d30_agreement,
              based_on: validD30.length,
            } : null,
          },
          similar_periods: outcomes.slice(0, 5).map(o => ({
            date: o.date,
            similarity: (o.similarity * 100).toFixed(1) + '%',
            pxi_then: o.score.toFixed(1),
            d7_change: o.d7_change?.toFixed(1),
            d30_change: o.d30_change?.toFixed(1),
          })),
        }, { headers: corsHeaders });
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

  // Cron trigger handler - runs daily at 6:00 AM UTC
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(handleScheduled(env));
  },
};
