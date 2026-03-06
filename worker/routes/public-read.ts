import type {
  AlertsApiResponsePayload,
  CategoryDetailResponsePayload,
  WorkerRouteContext,
} from '../types';

type PublicReadDeps = Record<string, any>;

export async function tryHandlePublicReadRoute(
  route: WorkerRouteContext,
  deps: PublicReadDeps,
): Promise<Response | null> {
  const { env, url, method, corsHeaders } = route;

  if (url.pathname === '/api/regime') {
    const regime = await deps.detectRegime(env.DB);
    if (!regime) {
      return Response.json({ error: 'Could not detect regime' }, { status: 500, headers: corsHeaders });
    }

    const recentDates = await env.DB.prepare(
      'SELECT date FROM pxi_scores ORDER BY date DESC LIMIT 30'
    ).all<{ date: string }>();

    const regimeHistory: Array<{ date: string; regime: string }> = [];
    for (const row of (recentDates.results || []).slice(0, 10)) {
      const historyRegime = await deps.detectRegime(env.DB, row.date);
      if (historyRegime) {
        regimeHistory.push({ date: historyRegime.date, regime: historyRegime.regime });
      }
    }

    let regimeChanges = 0;
    for (let index = 1; index < regimeHistory.length; index += 1) {
      if (regimeHistory[index].regime !== regimeHistory[index - 1].regime) {
        regimeChanges += 1;
      }
    }

    return Response.json({
      current: regime,
      history: regimeHistory,
      stability: regimeChanges <= 1 ? 'STABLE' : regimeChanges <= 3 ? 'MODERATE' : 'VOLATILE',
      regime_changes_10d: regimeChanges,
    }, { headers: corsHeaders });
  }

  if (url.pathname === '/api/history') {
    const days = Math.min(365, Math.max(7, parseInt(url.searchParams.get('days') || '90', 10)));
    const historyResult = await env.DB.prepare(`
      SELECT p.date, p.score, p.label, p.status
      FROM pxi_scores p
      ORDER BY p.date DESC
      LIMIT ?
    `).bind(days).all<{
      date: string;
      score: number;
      label: string;
      status: string;
    }>();

    if (!historyResult.results || historyResult.results.length === 0) {
      return Response.json({ error: 'No historical data' }, { status: 404, headers: corsHeaders });
    }

    const dataWithRegimes = historyResult.results.map((row) => ({
      date: row.date,
      score: row.score,
      label: row.label,
      status: row.status,
      regime: row.score >= 60 ? 'RISK_ON' : row.score <= 40 ? 'RISK_OFF' : 'TRANSITION',
    }));

    return Response.json({
      data: dataWithRegimes.reverse(),
      count: dataWithRegimes.length,
    }, {
      headers: {
        ...corsHeaders,
        'Cache-Control': 'public, max-age=300',
      },
    });
  }

  if (url.pathname === '/api/alerts') {
    const limit = Math.min(100, Math.max(10, parseInt(url.searchParams.get('limit') || '50', 10)));
    const alertType = url.searchParams.get('type');
    const severity = url.searchParams.get('severity');

    let queryStr = `SELECT id, date, alert_type, message, severity, acknowledged,
                           pxi_score, forward_return_7d, forward_return_30d, created_at
                    FROM alerts WHERE 1=1`;
    const params: Array<string | number> = [];

    if (alertType) {
      queryStr += ` AND alert_type = ?`;
      params.push(alertType);
    }
    if (severity) {
      queryStr += ` AND severity = ?`;
      params.push(severity);
    }

    queryStr += ` ORDER BY date DESC LIMIT ?`;
    params.push(limit);

    const alertsResult = await env.DB.prepare(queryStr).bind(...params).all<{
      id: number;
      date: string;
      alert_type: string;
      message: string;
      severity: 'info' | 'warning' | 'critical';
      acknowledged: number;
      pxi_score: number | null;
      forward_return_7d: number | null;
      forward_return_30d: number | null;
      created_at: string;
    }>();

    const typeCounts = await env.DB.prepare(`
      SELECT alert_type, COUNT(*) as count FROM alerts
      GROUP BY alert_type ORDER BY count DESC
    `).all<{ alert_type: string; count: number }>();

    const accuracyStats = await env.DB.prepare(`
      SELECT
        alert_type,
        COUNT(*) as total,
        SUM(CASE WHEN
          (alert_type LIKE '%bullish%' AND forward_return_7d > 0) OR
          (alert_type LIKE '%bearish%' AND forward_return_7d < 0) OR
          (alert_type = 'extreme_high' AND forward_return_7d < 0) OR
          (alert_type = 'extreme_low' AND forward_return_7d > 0)
        THEN 1 ELSE 0 END) as correct_7d,
        AVG(forward_return_7d) as avg_return_7d
      FROM alerts
      WHERE forward_return_7d IS NOT NULL
      GROUP BY alert_type
    `).all<{ alert_type: string; total: number; correct_7d: number; avg_return_7d: number }>();

    const payload: AlertsApiResponsePayload = {
      alerts: (alertsResult.results || []).map((alert) => ({
        ...alert,
        acknowledged: alert.acknowledged === 1,
      })),
      count: alertsResult.results?.length || 0,
      filters: {
        types: (typeCounts.results || []).map((typeRow) => ({
          type: typeRow.alert_type,
          count: typeRow.count,
        })),
      },
      accuracy: (accuracyStats.results || []).reduce((acc, stat) => {
        acc[stat.alert_type] = {
          total: stat.total,
          accuracy_7d: stat.total > 0 ? (stat.correct_7d / stat.total) * 100 : null,
          avg_return_7d: stat.avg_return_7d,
        };
        return acc;
      }, {} as Record<string, { total: number; accuracy_7d: number | null; avg_return_7d: number }>),
    };

    return Response.json(payload, {
      headers: {
        ...corsHeaders,
        'Cache-Control': 'public, max-age=60',
      },
    });
  }

  if (url.pathname.startsWith('/api/category/')) {
    const category = url.pathname.split('/api/category/')[1];
    const validCategories = ['positioning', 'credit', 'volatility', 'breadth', 'macro', 'global', 'crypto'];

    if (!category || !validCategories.includes(category)) {
      return Response.json({ error: 'Invalid category' }, { status: 400, headers: corsHeaders });
    }

    const latestPxi = await env.DB.prepare(
      'SELECT date FROM pxi_scores ORDER BY date DESC LIMIT 1'
    ).first<{ date: string }>();

    if (!latestPxi) {
      return Response.json({ error: 'No data' }, { status: 404, headers: corsHeaders });
    }

    const categoryScore = await env.DB.prepare(
      'SELECT score, weight FROM category_scores WHERE category = ? AND date = ?'
    ).bind(category, latestPxi.date).first<{ score: number; weight: number }>();

    const indicatorCategories: Record<string, string> = {
      fed_balance_sheet: 'positioning', treasury_general_account: 'positioning',
      reverse_repo: 'positioning', m2_yoy: 'positioning',
      hy_oas_spread: 'credit', ig_oas_spread: 'credit', yield_curve_2s10s: 'credit',
      vix: 'volatility', vix_term_structure: 'volatility', skew: 'volatility',
      put_call_ratio: 'volatility', move_index: 'volatility',
      sp500_adline: 'breadth', sp500_pct_above_200: 'breadth', sp500_pct_above_50: 'breadth',
      nyse_new_highs_lows: 'breadth',
      ism_manufacturing: 'macro', ism_services: 'macro', unemployment_claims: 'macro',
      consumer_sentiment: 'macro', aaii_sentiment: 'macro',
      dxy: 'global', copper_gold_ratio: 'global', btc_flows: 'global',
      stablecoin_mcap: 'crypto', btc_funding_rate: 'crypto',
    };

    const categoryIndicatorIds = Object.entries(indicatorCategories)
      .filter(([, mappedCategory]) => mappedCategory === category)
      .map(([indicatorId]) => indicatorId);

    const indicatorScoresResult = await env.DB.prepare(`
      SELECT indicator_id, raw_value, normalized_value
      FROM indicator_scores
      WHERE indicator_id IN (${categoryIndicatorIds.map(() => '?').join(',')})
        AND date = ?
    `).bind(...categoryIndicatorIds, latestPxi.date).all<{
      indicator_id: string;
      raw_value: number;
      normalized_value: number;
    }>();

    const historyResult = await env.DB.prepare(`
      SELECT date, score FROM category_scores
      WHERE category = ?
      ORDER BY date DESC
      LIMIT 90
    `).bind(category).all<{ date: string; score: number }>();

    const scores = (historyResult.results || []).map((row) => row.score);
    const currentScore = categoryScore?.score || 0;
    const percentileRank = scores.length > 0
      ? (scores.filter((score) => score < currentScore).length / scores.length) * 100
      : 50;

    const indicatorNames: Record<string, string> = {
      fed_balance_sheet: 'Fed Balance Sheet', treasury_general_account: 'Treasury General Account',
      reverse_repo: 'Reverse Repo Facility', m2_yoy: 'M2 Money Supply YoY',
      hy_oas_spread: 'High Yield Spread', ig_oas_spread: 'Investment Grade Spread',
      yield_curve_2s10s: '2s10s Yield Curve',
      vix: 'VIX', vix_term_structure: 'VIX Term Structure', skew: 'SKEW Index',
      put_call_ratio: 'Put/Call Ratio', move_index: 'MOVE Index',
      sp500_adline: 'S&P 500 A/D Line', sp500_pct_above_200: '% Above 200 DMA',
      sp500_pct_above_50: '% Above 50 DMA', nyse_new_highs_lows: 'NYSE New Highs-Lows',
      ism_manufacturing: 'ISM Manufacturing', ism_services: 'ISM Services',
      unemployment_claims: 'Initial Claims', consumer_sentiment: 'Consumer Sentiment',
      aaii_sentiment: 'AAII Bull/Bear',
      dxy: 'Dollar Index', copper_gold_ratio: 'Copper/Gold Ratio', btc_flows: 'BTC ETF Flows',
      stablecoin_mcap: 'Stablecoin Mcap RoC', btc_funding_rate: 'BTC Funding Rate',
    };

    const payload: CategoryDetailResponsePayload = {
      category,
      date: latestPxi.date,
      score: currentScore,
      weight: categoryScore?.weight || 0,
      percentile_rank: Math.round(percentileRank),
      indicators: (indicatorScoresResult.results || []).map((indicator) => ({
        id: indicator.indicator_id,
        name: indicatorNames[indicator.indicator_id] || indicator.indicator_id,
        raw_value: indicator.raw_value,
        normalized_value: indicator.normalized_value,
      })),
      history: (historyResult.results || []).reverse(),
    };

    return Response.json(payload, {
      headers: {
        ...corsHeaders,
        'Cache-Control': 'public, max-age=300',
      },
    });
  }

  if (url.pathname === '/api/analyze' && method === 'GET') {
    const pxi = await env.DB.prepare(
      'SELECT date, score, label, status FROM pxi_scores ORDER BY date DESC LIMIT 1'
    ).first<{
      date: string;
      score: number;
      label: string;
      status: string;
    }>();

    const categories = await env.DB.prepare(
      'SELECT category, score FROM category_scores WHERE date = ? ORDER BY score DESC'
    ).bind(pxi?.date).all<{ category: string; score: number }>();

    if (!pxi || !categories.results) {
      return Response.json({ error: 'No data' }, { status: 404, headers: corsHeaders });
    }

    const prompt = `Analyze this market regime in 2-3 sentences. Be specific about what's driving conditions.

PXI Score: ${pxi.score.toFixed(1)} (${pxi.label})
Category Breakdown:
${categories.results.map((category) => `- ${category.category}: ${category.score.toFixed(1)}/100`).join('\n')}

Focus on: What's strong? What's weak? What does this suggest for risk appetite?`;

    const analysis = await (env.AI as any).run('@cf/meta/llama-3.1-8b-instruct', {
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

  return null;
}
