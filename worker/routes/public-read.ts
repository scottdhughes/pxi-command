import type {
  AlertsApiResponsePayload,
  CategoryDetailResponsePayload,
  WorkerRouteContext,
} from '../types';
import { INDICATORS } from '../../src/config/indicators.js';

type PublicReadDeps = Record<string, any>;

type HistoryOrigin =
  | 'legacy_unclassified'
  | 'live_recorded'
  | 'retrospective_reconstruction';

interface HistoryRow {
  date: string;
  score: number;
  label: string;
  status: string;
  history_origin: HistoryOrigin;
  reconstructed_at: string | null;
  reconstruction_method: string | null;
  reconstruction_build_sha: string | null;
  source_data_as_of: string | null;
}

interface HistoryGap {
  start_date: string;
  end_date: string;
  missing_days: number;
}

const DAY_MS = 24 * 60 * 60 * 1000;

function utcDay(date: string): number | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return null;
  const timestamp = Date.parse(`${date}T00:00:00.000Z`);
  return Number.isFinite(timestamp) ? Math.floor(timestamp / DAY_MS) : null;
}

function isoDateFromUtcDay(day: number): string {
  return new Date(day * DAY_MS).toISOString().slice(0, 10);
}

export function summarizeHistoryContinuity(rows: Array<Pick<HistoryRow, 'date'>>) {
  if (rows.length === 0) {
    return {
      is_contiguous: true,
      start_date: '',
      end_date: '',
      observed_days: 0,
      expected_days: 0,
      missing_days: 0,
      gap_count: 0,
      gaps: [] as HistoryGap[],
    };
  }

  const datedRows = rows
    .map((row) => ({ date: row.date, day: utcDay(row.date) }))
    .filter((row): row is { date: string; day: number } => row.day !== null)
    .sort((left, right) => left.day - right.day);

  if (datedRows.length === 0) {
    return {
      is_contiguous: false,
      start_date: rows[0]?.date || '',
      end_date: rows[rows.length - 1]?.date || '',
      observed_days: rows.length,
      expected_days: rows.length,
      missing_days: 0,
      gap_count: 0,
      gaps: [] as HistoryGap[],
    };
  }

  const gaps: HistoryGap[] = [];
  for (let index = 1; index < datedRows.length; index += 1) {
    const previous = datedRows[index - 1].day;
    const current = datedRows[index].day;
    if (current - previous <= 1) continue;
    gaps.push({
      start_date: isoDateFromUtcDay(previous + 1),
      end_date: isoDateFromUtcDay(current - 1),
      missing_days: current - previous - 1,
    });
  }

  const first = datedRows[0];
  const last = datedRows[datedRows.length - 1];
  const expectedDays = last.day - first.day + 1;
  const missingDays = gaps.reduce((total, gap) => total + gap.missing_days, 0);

  return {
    is_contiguous: gaps.length === 0 && datedRows.length === rows.length,
    start_date: first.date,
    end_date: last.date,
    observed_days: rows.length,
    expected_days: expectedDays,
    missing_days: missingDays,
    gap_count: gaps.length,
    gaps,
  };
}

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
      SELECT
        history.date,
        history.score,
        history.label,
        history.status,
        history.history_origin,
        history.reconstructed_at,
        history.reconstruction_method,
        history.reconstruction_build_sha,
        history.source_data_as_of
      FROM (
        SELECT
          p.date,
          p.score,
          p.label,
          p.status,
          p.history_origin,
          NULL AS reconstructed_at,
          NULL AS reconstruction_method,
          NULL AS reconstruction_build_sha,
          NULL AS source_data_as_of
        FROM pxi_scores p
        UNION ALL
        SELECT
          reconstruction.date,
          reconstruction.score,
          reconstruction.label,
          reconstruction.status,
          reconstruction.history_origin,
          reconstruction.reconstructed_at,
          reconstruction.reconstruction_method,
          reconstruction.reconstruction_build_sha,
          reconstruction.source_data_as_of
        FROM pxi_score_reconstructions reconstruction
        WHERE NOT EXISTS (
          SELECT 1
          FROM pxi_scores live
          WHERE live.date = reconstruction.date
        )
      ) history
      ORDER BY history.date DESC
      LIMIT ?
    `).bind(days).all<HistoryRow>();

    if (!historyResult.results || historyResult.results.length === 0) {
      return Response.json({ error: 'No historical data' }, { status: 404, headers: corsHeaders });
    }

    const dataWithRegimes = historyResult.results.map((row) => ({
      date: row.date,
      score: row.score,
      label: row.label,
      status: row.status,
      regime: row.score >= 60 ? 'RISK_ON' : row.score <= 40 ? 'RISK_OFF' : 'TRANSITION',
      history_origin: row.history_origin,
      reconstructed_at: row.reconstructed_at,
      reconstruction_method: row.reconstruction_method,
      reconstruction_build_sha: row.reconstruction_build_sha,
      source_data_as_of: row.source_data_as_of,
    }));

    const data = [...dataWithRegimes].reverse();
    const provenanceCounts = data.reduce<Record<HistoryOrigin, number>>((counts, row) => {
      counts[row.history_origin] += 1;
      return counts;
    }, {
      legacy_unclassified: 0,
      live_recorded: 0,
      retrospective_reconstruction: 0,
    });

    return Response.json({
      data,
      count: data.length,
      provenance_counts: provenanceCounts,
      continuity: summarizeHistoryContinuity(data),
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

    const categoryIndicators = INDICATORS.filter((indicator) => indicator.category === category);
    const categoryIndicatorIds = categoryIndicators.map((indicator) => indicator.id);

    const indicatorScoresResult = await env.DB.prepare(`
      WITH latest AS (
        SELECT iv.indicator_id, iv.value AS raw_value
        FROM indicator_values iv
        INNER JOIN (
          SELECT indicator_id, MAX(date) AS max_date
          FROM indicator_values
          WHERE indicator_id IN (${categoryIndicatorIds.map(() => '?').join(',')})
            AND date <= ?
          GROUP BY indicator_id
        ) current
          ON current.indicator_id = iv.indicator_id
         AND current.max_date = iv.date
      )
      SELECT
        latest.indicator_id,
        latest.raw_value,
        100.0 * (
          SUM(CASE WHEN history.value < latest.raw_value THEN 1 ELSE 0 END) +
          (0.5 * SUM(CASE WHEN history.value = latest.raw_value THEN 1 ELSE 0 END))
        ) / COUNT(history.value) AS normalized_value
      FROM latest
      INNER JOIN indicator_values history
        ON history.indicator_id = latest.indicator_id
       AND history.date >= date(?, '-5 years')
       AND history.date <= ?
      GROUP BY latest.indicator_id, latest.raw_value
      HAVING COUNT(history.value) >= 10
    `).bind(
      ...categoryIndicatorIds,
      latestPxi.date,
      latestPxi.date,
      latestPxi.date,
    ).all<{
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

    const indicatorNames = new Map(categoryIndicators.map((indicator) => [indicator.id, indicator.name]));

    const payload: CategoryDetailResponsePayload = {
      category,
      date: latestPxi.date,
      score: currentScore,
      weight: categoryScore?.weight || 0,
      percentile_rank: Math.round(percentileRank),
      indicators: (indicatorScoresResult.results || []).map((indicator) => ({
        id: indicator.indicator_id,
        name: indicatorNames.get(indicator.indicator_id) || indicator.indicator_id,
        raw_value: indicator.raw_value,
        normalized_value: categoryIndicators.find((definition) => definition.id === indicator.indicator_id)?.inverted
          ? 100 - indicator.normalized_value
          : indicator.normalized_value,
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
