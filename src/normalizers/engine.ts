import { format, subDays } from 'date-fns';
import { query } from '../db/connection.js';
import { INDICATORS, getIndicatorById } from '../config/indicators.js';
import { CATEGORY_WEIGHTS, type Category } from '../types/indicators.js';

const LOOKBACK_DAYS = 504; // ~2 years of trading days

interface HistoricalData {
  values: number[];
  min: number;
  max: number;
  mean: number;
  stdDev: number;
}

// Get historical data for percentile calculations
async function getHistoricalData(
  indicatorId: string,
  endDate: Date
): Promise<HistoricalData> {
  const startDate = subDays(endDate, LOOKBACK_DAYS);

  const result = await query(
    `SELECT value FROM indicator_values
     WHERE indicator_id = $1
       AND date >= $2
       AND date <= $3
     ORDER BY date ASC`,
    [indicatorId, format(startDate, 'yyyy-MM-dd'), format(endDate, 'yyyy-MM-dd')]
  );

  const values = result.rows.map((r) => parseFloat(r.value));

  if (values.length === 0) {
    return { values: [], min: 0, max: 0, mean: 0, stdDev: 1 };
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const variance =
    values.reduce((sum, v) => sum + Math.pow(v - mean, 2), 0) / values.length;
  const stdDev = Math.sqrt(variance);

  return { values, min, max, mean, stdDev };
}

// Calculate percentile rank (0-100)
function percentileRank(value: number, historicalValues: number[]): number {
  if (historicalValues.length === 0) return 50;

  const sorted = [...historicalValues].sort((a, b) => a - b);
  let count = 0;

  for (const v of sorted) {
    if (v < value) count++;
    else if (v === value) count += 0.5;
  }

  return (count / sorted.length) * 100;
}

// Z-score normalization mapped to 0-100
function zscoreNormalize(
  value: number,
  mean: number,
  stdDev: number
): number {
  if (stdDev === 0) return 50;

  const zscore = (value - mean) / stdDev;
  // Map z-score to 0-100, capping at ±3 std dev
  const capped = Math.max(-3, Math.min(3, zscore));
  return 50 + capped * (50 / 3);
}

// Bell curve normalization for funding rates
function bellCurveNormalize(value: number): number {
  // Optimal range: 0.005% to 0.03%
  // Returns 100 at optimal center, declining at extremes
  const absValue = Math.abs(value);

  if (absValue >= 0.005 && absValue <= 0.03) {
    const center = 0.015;
    const distance = Math.abs(absValue - center);
    const maxDistance = 0.015;
    return 100 - (distance / maxDistance) * 30;
  } else if (absValue < 0.005) {
    return 50 + (absValue / 0.005) * 20;
  } else {
    const excess = absValue - 0.03;
    const maxExcess = 0.1;
    return Math.max(0, 70 - (excess / maxExcess) * 70);
  }
}

// Direct normalization for percentage values (already 0-100 scale)
function directNormalize(value: number): number {
  return Math.max(0, Math.min(100, value));
}

// PMI normalization (30-70 scale mapped to 0-100)
function pmiNormalize(value: number): number {
  // PMI: 50 = neutral, 30 = very contractionary, 70 = very expansionary
  const clamped = Math.max(30, Math.min(70, value));
  return ((clamped - 30) / 40) * 100;
}

// Main normalization function
export async function normalizeIndicator(
  indicatorId: string,
  rawValue: number,
  date: Date
): Promise<number> {
  const indicator = getIndicatorById(indicatorId);
  if (!indicator) {
    console.warn(`Unknown indicator: ${indicatorId}`);
    return 50;
  }

  const historical = await getHistoricalData(indicatorId, date);
  let normalized: number;

  switch (indicator.normalization) {
    case 'percentile':
      normalized = percentileRank(rawValue, historical.values);
      break;

    case 'percentile_inverted':
      normalized = 100 - percentileRank(rawValue, historical.values);
      break;

    case 'zscore':
      normalized = zscoreNormalize(rawValue, historical.mean, historical.stdDev);
      if (indicator.inverted) {
        normalized = 100 - normalized;
      }
      break;

    case 'bellcurve':
      normalized = bellCurveNormalize(rawValue);
      break;

    case 'direct':
      // Check if it's a PMI-type indicator
      if (
        indicator.id.includes('ism_') ||
        indicator.id === 'ism_manufacturing' ||
        indicator.id === 'ism_services'
      ) {
        normalized = pmiNormalize(rawValue);
      } else {
        normalized = directNormalize(rawValue);
      }
      break;

    default:
      normalized = 50;
  }

  return Math.max(0, Math.min(100, normalized));
}

// Calculate all indicator scores for a given date
export async function calculateIndicatorScores(
  targetDate: Date
): Promise<Map<string, { raw: number; normalized: number }>> {
  const scores = new Map<string, { raw: number; normalized: number }>();
  const dateStr = format(targetDate, 'yyyy-MM-dd');

  for (const indicator of INDICATORS) {
    try {
      // Get the most recent value on or before target date
      const result = await query(
        `SELECT value, date FROM indicator_values
         WHERE indicator_id = $1 AND date <= $2
         ORDER BY date DESC
         LIMIT 1`,
        [indicator.id, dateStr]
      );

      if (result.rows.length === 0) {
        console.warn(`No data for ${indicator.id} on or before ${dateStr}`);
        continue;
      }

      const rawValue = parseFloat(result.rows[0].value);
      const normalized = await normalizeIndicator(indicator.id, rawValue, targetDate);

      scores.set(indicator.id, { raw: rawValue, normalized });

      // Save to indicator_scores table
      await query(
        `INSERT INTO indicator_scores (indicator_id, date, raw_value, normalized_value, lookback_days)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (indicator_id, date)
         DO UPDATE SET raw_value = EXCLUDED.raw_value,
                       normalized_value = EXCLUDED.normalized_value,
                       calculated_at = NOW()`,
        [indicator.id, dateStr, rawValue, normalized, LOOKBACK_DAYS]
      );
    } catch (err) {
      console.error(`Error processing ${indicator.id}:`, err);
    }
  }

  return scores;
}

// Calculate category scores
export async function calculateCategoryScores(
  targetDate: Date,
  indicatorScores: Map<string, { raw: number; normalized: number }>
): Promise<Map<Category, number>> {
  const categoryScores = new Map<Category, number>();
  const dateStr = format(targetDate, 'yyyy-MM-dd');

  const categories: Category[] = [
    'liquidity',
    'credit',
    'volatility',
    'breadth',
    'macro',
    'global',
    'crypto',
  ];

  for (const category of categories) {
    const categoryIndicators = INDICATORS.filter((i) => i.category === category);
    const scores: number[] = [];

    for (const indicator of categoryIndicators) {
      const score = indicatorScores.get(indicator.id);
      if (score) {
        scores.push(score.normalized);
      }
    }

    if (scores.length === 0) {
      console.warn(`No scores for category: ${category}`);
      categoryScores.set(category, 50);
      continue;
    }

    // Equal weight average within category
    const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;
    categoryScores.set(category, avgScore);

    const weight = CATEGORY_WEIGHTS[category];
    const weightedScore = avgScore * weight;

    // Save category score
    await query(
      `INSERT INTO category_scores (category, date, score, weight, weighted_score)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (category, date)
       DO UPDATE SET score = EXCLUDED.score,
                     weighted_score = EXCLUDED.weighted_score,
                     calculated_at = NOW()`,
      [category, dateStr, avgScore, weight, weightedScore]
    );
  }

  return categoryScores;
}

// Calculate final PXI score
export async function calculatePXI(targetDate: Date): Promise<{
  score: number;
  label: string;
  status: string;
}> {
  const dateStr = format(targetDate, 'yyyy-MM-dd');

  // Get indicator scores
  const indicatorScores = await calculateIndicatorScores(targetDate);

  // Get category scores
  const categoryScores = await calculateCategoryScores(targetDate, indicatorScores);

  // Calculate weighted composite
  let totalScore = 0;
  for (const [category, score] of categoryScores) {
    totalScore += score * CATEGORY_WEIGHTS[category];
  }

  // Determine label and status
  let label: string;
  let status: string;

  if (totalScore >= 80) {
    label = 'MAX PAMP';
    status = 'max_pamp';
  } else if (totalScore >= 65) {
    label = 'PAMPING';
    status = 'pamping';
  } else if (totalScore >= 50) {
    label = 'NEUTRAL';
    status = 'neutral';
  } else if (totalScore >= 35) {
    label = 'SOFT';
    status = 'soft';
  } else {
    label = 'DUMPING';
    status = 'dumping';
  }

  // Calculate deltas
  const delta1d = await getDelta(dateStr, 1);
  const delta7d = await getDelta(dateStr, 7);
  const delta30d = await getDelta(dateStr, 30);

  // Save PXI score
  await query(
    `INSERT INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (date)
     DO UPDATE SET score = EXCLUDED.score,
                   label = EXCLUDED.label,
                   status = EXCLUDED.status,
                   delta_1d = EXCLUDED.delta_1d,
                   delta_7d = EXCLUDED.delta_7d,
                   delta_30d = EXCLUDED.delta_30d,
                   calculated_at = NOW()`,
    [dateStr, totalScore, label, status, delta1d, delta7d, delta30d]
  );

  return { score: totalScore, label, status };
}

async function getDelta(
  currentDateStr: string,
  daysAgo: number
): Promise<number | null> {
  const pastDate = format(subDays(new Date(currentDateStr), daysAgo), 'yyyy-MM-dd');

  const result = await query(
    `SELECT score FROM pxi_scores WHERE date = $1`,
    [pastDate]
  );

  if (result.rows.length === 0) return null;

  const currentResult = await query(
    `SELECT score FROM pxi_scores WHERE date = $1`,
    [currentDateStr]
  );

  if (currentResult.rows.length === 0) return null;

  return parseFloat(currentResult.rows[0].score) - parseFloat(result.rows[0].score);
}
