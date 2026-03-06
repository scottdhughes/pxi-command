import type {
  MLAccuracyApiResponsePayload,
  WorkerRouteContext,
} from '../types';

type ModelingDeps = Record<string, any>;

const DIRECTION_THRESHOLDS = {
  strongUp: 5,
  up: 2,
  down: -2,
  strongDown: -5,
} as const;

function asDateKey(date: Date): string {
  return date.toISOString().split('T')[0];
}

function interpretDirectionalPrediction(prediction: number | null): string | null {
  if (prediction === null) return null;
  if (prediction > DIRECTION_THRESHOLDS.strongUp) return 'STRONG_UP';
  if (prediction > DIRECTION_THRESHOLDS.up) return 'UP';
  if (prediction > DIRECTION_THRESHOLDS.down) return 'FLAT';
  if (prediction > DIRECTION_THRESHOLDS.strongDown) return 'DOWN';
  return 'STRONG_DOWN';
}

function formatDirectionalMetrics(metrics: {
  d7_correct: number;
  d7_total: number;
  d7_mae: number;
  d30_correct: number;
  d30_total: number;
  d30_mae: number;
}) {
  return {
    d7: metrics.d7_total > 0 ? {
      direction_accuracy: `${((metrics.d7_correct / metrics.d7_total) * 100).toFixed(1)}%`,
      mean_absolute_error: (metrics.d7_mae / metrics.d7_total).toFixed(2),
      sample_size: metrics.d7_total,
    } : null,
    d30: metrics.d30_total > 0 ? {
      direction_accuracy: `${((metrics.d30_correct / metrics.d30_total) * 100).toFixed(1)}%`,
      mean_absolute_error: (metrics.d30_mae / metrics.d30_total).toFixed(2),
      sample_size: metrics.d30_total,
    } : null,
  };
}

function buildPriceMap(rows: Array<{ date: string; value: number }>): Map<string, number> {
  const map = new Map<string, number>();
  for (const row of rows) {
    map.set(row.date, row.value);
  }
  return map;
}

function getPriceOnOrAfter(priceMap: Map<string, number>, dateStr: string, maxDaysForward = 5): number | null {
  const date = new Date(dateStr);
  for (let offset = 0; offset <= maxDaysForward; offset += 1) {
    const checkDate = new Date(date);
    checkDate.setDate(checkDate.getDate() + offset);
    const price = priceMap.get(asDateKey(checkDate));
    if (price !== undefined) {
      return price;
    }
  }
  return null;
}

function calculateMedian(values: number[]): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

function calculateAverage(values: number[]): number | null {
  return values.length > 0 ? values.reduce((sum, value) => sum + value, 0) / values.length : null;
}

function calculateWinRate(values: number[]): number | null {
  return values.length > 0 ? (values.filter((value) => value > 0).length / values.length) * 100 : null;
}

function calculateR2(predicted: number[], actual: number[]): number {
  if (predicted.length === 0) return 0;
  const meanActual = actual.reduce((sum, value) => sum + value, 0) / actual.length;
  const ssRes = predicted.reduce((sum, value, index) => sum + Math.pow(actual[index] - value, 2), 0);
  const ssTot = actual.reduce((sum, value) => sum + Math.pow(value - meanActual, 2), 0);
  return ssTot > 0 ? 1 - (ssRes / ssTot) : 0;
}

async function fetchCurrentFeatureInputs(env: WorkerRouteContext['env'], date: string) {
  const [categories, indicators, recentScores] = await Promise.all([
    env.DB.prepare(`
      SELECT category, score FROM category_scores WHERE date = ?
    `).bind(date).all<{ category: string; score: number }>(),
    env.DB.prepare(`
      SELECT indicator_id, value FROM indicator_values WHERE date = ?
    `).bind(date).all<{ indicator_id: string; value: number }>(),
    env.DB.prepare(`
      SELECT score FROM pxi_scores ORDER BY date DESC LIMIT 20
    `).all<{ score: number }>(),
  ]);

  const scores = (recentScores.results || []).map((row) => row.score);
  const pxi_ma_5 = scores.slice(0, 5).reduce((sum, value) => sum + value, 0) / Math.min(5, scores.length);
  const pxi_ma_20 = scores.reduce((sum, value) => sum + value, 0) / scores.length;
  const pxi_std_20 = Math.sqrt(scores.reduce((sum, value) => sum + Math.pow(value - pxi_ma_20, 2), 0) / scores.length);

  const categoryMap: Record<string, number> = {};
  for (const category of categories.results || []) {
    categoryMap[category.category] = category.score;
  }

  const indicatorMap: Record<string, number> = {};
  for (const indicator of indicators.results || []) {
    indicatorMap[indicator.indicator_id] = indicator.value;
  }

  return {
    categoryMap,
    indicatorMap,
    pxi_ma_5,
    pxi_ma_20,
    pxi_std_20,
  };
}

async function fetchLstmSequenceInputs(
  env: WorkerRouteContext['env'],
  seqLength: number,
): Promise<{
  currentDate: string;
  currentScore: number;
  sequence: number[][];
} | null> {
  const pxiHistory = await env.DB.prepare(
    'SELECT date, score, delta_7d FROM pxi_scores ORDER BY date DESC LIMIT ?'
  ).bind(seqLength).all<{ date: string; score: number; delta_7d: number | null }>();

  if (!pxiHistory.results || pxiHistory.results.length < seqLength) {
    return null;
  }

  const dates = pxiHistory.results.map((row) => row.date);
  const [categoryData, vixData] = await Promise.all([
    env.DB.prepare(`
      SELECT date, category, score
      FROM category_scores
      WHERE date IN (${dates.map(() => '?').join(',')})
    `).bind(...dates).all<{ date: string; category: string; score: number }>(),
    env.DB.prepare(`
      SELECT date, value
      FROM indicator_values
      WHERE indicator_id = 'vix' AND date IN (${dates.map(() => '?').join(',')})
    `).bind(...dates).all<{ date: string; value: number }>(),
  ]);

  const byDate = new Map<string, { score: number; delta_7d: number | null; categories: Record<string, number> }>();
  for (const row of pxiHistory.results) {
    byDate.set(row.date, { score: row.score, delta_7d: row.delta_7d, categories: {} });
  }

  for (const row of categoryData.results || []) {
    if (byDate.has(row.date)) {
      byDate.get(row.date)!.categories[row.category] = row.score;
    }
  }

  const vixMap: Record<string, number> = {};
  for (const row of vixData.results || []) {
    vixMap[row.date] = row.value;
  }

  const sortedDates = [...dates].sort();
  const currentDate = dates[0];
  const currentPxi = byDate.get(currentDate);
  if (!currentPxi) {
    return null;
  }

  return {
    currentDate,
    currentScore: currentPxi.score,
    sequence: sortedDates.map((date) => {
      const day = byDate.get(date)!;
      return { day, vix: vixMap[date] };
    }) as unknown as number[][],
  };
}

async function handlePredictRoute(route: WorkerRouteContext): Promise<Response> {
  const { env, corsHeaders } = route;

  const currentPxi = await env.DB.prepare(
    'SELECT date, score, label FROM pxi_scores ORDER BY date DESC LIMIT 1'
  ).first<{ date: string; score: number; label: string }>();

  if (!currentPxi) {
    return Response.json({ error: 'No PXI data' }, { status: 404, headers: corsHeaders });
  }

  const [pxiScores, spyPrices, thresholdParams] = await Promise.all([
    env.DB.prepare('SELECT date, score FROM pxi_scores ORDER BY date ASC').all<{ date: string; score: number }>(),
    env.DB.prepare(`
      SELECT date, value FROM indicator_values
      WHERE indicator_id = 'spy_close' ORDER BY date ASC
    `).all<{ date: string; value: number }>(),
    env.DB.prepare(`
      SELECT param_key, param_value FROM model_params
      WHERE param_key LIKE 'bucket_threshold_%'
    `).all<{ param_key: string; param_value: number }>(),
  ]);

  const spyMap = buildPriceMap(spyPrices.results || []);
  const thresholds = { t1: 20, t2: 40, t3: 60, t4: 80 };
  for (const param of thresholdParams.results || []) {
    if (param.param_key === 'bucket_threshold_1') thresholds.t1 = param.param_value;
    if (param.param_key === 'bucket_threshold_2') thresholds.t2 = param.param_value;
    if (param.param_key === 'bucket_threshold_3') thresholds.t3 = param.param_value;
    if (param.param_key === 'bucket_threshold_4') thresholds.t4 = param.param_value;
  }

  const getBucket = (score: number): string => {
    if (score < thresholds.t1) return `0-${thresholds.t1}`;
    if (score < thresholds.t2) return `${thresholds.t1}-${thresholds.t2}`;
    if (score < thresholds.t3) return `${thresholds.t2}-${thresholds.t3}`;
    if (score < thresholds.t4) return `${thresholds.t3}-${thresholds.t4}`;
    return `${thresholds.t4}-100`;
  };

  const bucket = getBucket(currentPxi.score);
  const bucketReturns7d: number[] = [];
  const bucketReturns30d: number[] = [];

  for (const pxi of pxiScores.results || []) {
    if (getBucket(pxi.score) !== bucket) continue;

    const spyNow = getPriceOnOrAfter(spyMap, pxi.date);
    if (spyNow === null) continue;

    const startDate = new Date(pxi.date);
    const date7d = new Date(startDate);
    date7d.setDate(date7d.getDate() + 7);
    const date30d = new Date(startDate);
    date30d.setDate(date30d.getDate() + 30);

    const spy7d = getPriceOnOrAfter(spyMap, asDateKey(date7d));
    const spy30d = getPriceOnOrAfter(spyMap, asDateKey(date30d));

    if (spy7d !== null) bucketReturns7d.push(((spy7d - spyNow) / spyNow) * 100);
    if (spy30d !== null) bucketReturns30d.push(((spy30d - spyNow) / spyNow) * 100);
  }

  const extremeLowThreshold = thresholds.t1 * 1.25;
  const extremeHighThreshold = thresholds.t4 - (100 - thresholds.t4) * 0.25;
  const isExtremeLow = currentPxi.score < extremeLowThreshold;
  const isExtremeHigh = currentPxi.score > extremeHighThreshold;

  let extremeStats: Record<string, unknown> | null = null;
  if (isExtremeLow || isExtremeHigh) {
    const extremeReturns7d: number[] = [];
    const extremeReturns30d: number[] = [];

    for (const pxi of pxiScores.results || []) {
      const inRange = isExtremeLow ? pxi.score < extremeLowThreshold : pxi.score > extremeHighThreshold;
      if (!inRange) continue;

      const spyNow = getPriceOnOrAfter(spyMap, pxi.date);
      if (spyNow === null) continue;

      const startDate = new Date(pxi.date);
      const date7d = new Date(startDate);
      date7d.setDate(date7d.getDate() + 7);
      const date30d = new Date(startDate);
      date30d.setDate(date30d.getDate() + 30);

      const spy7d = getPriceOnOrAfter(spyMap, asDateKey(date7d));
      const spy30d = getPriceOnOrAfter(spyMap, asDateKey(date30d));

      if (spy7d !== null) extremeReturns7d.push(((spy7d - spyNow) / spyNow) * 100);
      if (spy30d !== null) extremeReturns30d.push(((spy30d - spyNow) / spyNow) * 100);
    }

    extremeStats = {
      type: isExtremeLow ? 'OVERSOLD' : 'OVERBOUGHT',
      threshold: isExtremeLow ? `<${extremeLowThreshold.toFixed(0)}` : `>${extremeHighThreshold.toFixed(0)}`,
      historical_count: extremeReturns7d.length,
      avg_return_7d: calculateAverage(extremeReturns7d),
      avg_return_30d: calculateAverage(extremeReturns30d),
      win_rate_7d: calculateWinRate(extremeReturns7d),
      win_rate_30d: calculateWinRate(extremeReturns30d),
      signal: isExtremeLow ? 'BULLISH' : 'BEARISH',
    };
  }

  const mlPrediction = await env.DB.prepare(`
    SELECT predicted_change_7d, predicted_change_30d, confidence_7d, confidence_30d, similar_periods
    FROM prediction_log
    WHERE prediction_date = ?
  `).bind(currentPxi.date).first<{
    predicted_change_7d: number | null;
    predicted_change_30d: number | null;
    confidence_7d: number | null;
    confidence_30d: number | null;
    similar_periods: string | null;
  }>();

  const getConfidenceLabel = (confidence: number | null) => {
    if (confidence === null) return 'N/A';
    if (confidence >= 0.7) return 'HIGH';
    if (confidence >= 0.4) return 'MEDIUM';
    return 'LOW';
  };

  const bucketWinRate7d = calculateWinRate(bucketReturns7d) || 50;

  return Response.json({
    current: {
      date: currentPxi.date,
      score: currentPxi.score,
      label: currentPxi.label,
      bucket,
    },
    adaptive_thresholds: {
      buckets: [
        `0-${thresholds.t1}`,
        `${thresholds.t1}-${thresholds.t2}`,
        `${thresholds.t2}-${thresholds.t3}`,
        `${thresholds.t3}-${thresholds.t4}`,
        `${thresholds.t4}-100`,
      ],
      values: thresholds,
    },
    prediction: {
      method: 'empirical_backtest',
      d7: {
        avg_return: calculateAverage(bucketReturns7d),
        median_return: calculateMedian(bucketReturns7d),
        win_rate: calculateWinRate(bucketReturns7d),
        sample_size: bucketReturns7d.length,
      },
      d30: {
        avg_return: calculateAverage(bucketReturns30d),
        median_return: calculateMedian(bucketReturns30d),
        win_rate: calculateWinRate(bucketReturns30d),
        sample_size: bucketReturns30d.length,
      },
    },
    ml_prediction: mlPrediction ? {
      method: 'similar_period_weighted',
      d7: {
        predicted_change: mlPrediction.predicted_change_7d,
        confidence: mlPrediction.confidence_7d,
        confidence_label: getConfidenceLabel(mlPrediction.confidence_7d),
      },
      d30: {
        predicted_change: mlPrediction.predicted_change_30d,
        confidence: mlPrediction.confidence_30d,
        confidence_label: getConfidenceLabel(mlPrediction.confidence_30d),
      },
      similar_periods_count: mlPrediction.similar_periods ? JSON.parse(mlPrediction.similar_periods).length : 0,
    } : null,
    extreme_reading: extremeStats,
    interpretation: {
      bias: bucketWinRate7d > 55 ? 'BULLISH' : bucketWinRate7d < 45 ? 'BEARISH' : 'NEUTRAL',
      confidence: bucketReturns7d.length >= 50 ? 'HIGH' : bucketReturns7d.length >= 20 ? 'MEDIUM' : 'LOW',
      ml_confidence: mlPrediction ? getConfidenceLabel(mlPrediction.confidence_7d) : null,
      note: isExtremeLow
        ? 'Oversold readings have historically preceded rallies'
        : isExtremeHigh
          ? 'Extended readings often see mean reversion'
          : bucketWinRate7d > 55
            ? `At this level, markets rose ${Math.round(bucketWinRate7d)}% of the time`
            : bucketWinRate7d < 45
              ? `At this level, markets fell ${Math.round(100 - bucketWinRate7d)}% of the time`
              : 'Mixed historical outcomes at this level',
    },
  }, { headers: corsHeaders });
}

async function handleEvaluateRoute(route: WorkerRouteContext, deps: ModelingDeps): Promise<Response> {
  const { request, env, corsHeaders, clientIP } = route;

  const adminAuthFailure = await deps.enforceAdminAuth(request, env, corsHeaders, clientIP);
  if (adminAuthFailure) {
    return adminAuthFailure;
  }

  const pendingPredictions = await env.DB.prepare(`
    SELECT id, prediction_date, target_date_7d, target_date_30d, current_score,
           predicted_change_7d, predicted_change_30d, actual_change_7d, actual_change_30d
    FROM prediction_log
    WHERE evaluated_at IS NULL
    ORDER BY prediction_date ASC
  `).all<{
    id: number;
    prediction_date: string;
    target_date_7d: string | null;
    target_date_30d: string | null;
    current_score: number;
    predicted_change_7d: number | null;
    predicted_change_30d: number | null;
    actual_change_7d: number | null;
    actual_change_30d: number | null;
  }>();

  const today = asDateKey(new Date());
  let evaluated = 0;

  for (const prediction of pendingPredictions.results || []) {
    let needsUpdate = false;
    let actual7d = prediction.actual_change_7d;
    let actual30d = prediction.actual_change_30d;

    if (prediction.target_date_7d && prediction.target_date_7d <= today && actual7d === null) {
      const score7d = await env.DB.prepare(
        'SELECT score FROM pxi_scores WHERE date = ?'
      ).bind(prediction.target_date_7d).first<{ score: number }>();

      if (score7d) {
        actual7d = score7d.score - prediction.current_score;
        needsUpdate = true;
      }
    }

    if (prediction.target_date_30d && prediction.target_date_30d <= today && actual30d === null) {
      const score30d = await env.DB.prepare(
        'SELECT score FROM pxi_scores WHERE date = ?'
      ).bind(prediction.target_date_30d).first<{ score: number }>();

      if (score30d) {
        actual30d = score30d.score - prediction.current_score;
        needsUpdate = true;
      }
    }

    if (needsUpdate) {
      const fullyEvaluated = (actual7d !== null || prediction.predicted_change_7d === null)
        && (actual30d !== null || prediction.predicted_change_30d === null);

      await env.DB.prepare(`
        UPDATE prediction_log
        SET actual_change_7d = ?, actual_change_30d = ?, evaluated_at = ?
        WHERE id = ?
      `).bind(
        actual7d,
        actual30d,
        fullyEvaluated ? new Date().toISOString() : null,
        prediction.id,
      ).run();
      evaluated += 1;
    }
  }

  return Response.json({
    success: true,
    pending: pendingPredictions.results?.length || 0,
    evaluated,
  }, { headers: corsHeaders });
}

async function handleMlPredictRoute(route: WorkerRouteContext, deps: ModelingDeps): Promise<Response> {
  const { env, corsHeaders } = route;

  const model = await deps.loadMLModel(env.ML_MODELS);
  if (!model) {
    return Response.json({
      error: 'ML model not loaded',
      message: 'Model has not been uploaded to KV yet',
    }, { status: 503, headers: corsHeaders });
  }

  const currentPxi = await env.DB.prepare(`
    SELECT date, score, delta_1d, delta_7d, delta_30d
    FROM pxi_scores ORDER BY date DESC LIMIT 1
  `).first<{
    date: string;
    score: number;
    delta_1d: number | null;
    delta_7d: number | null;
    delta_30d: number | null;
  }>();

  if (!currentPxi) {
    return Response.json({ error: 'No PXI data' }, { status: 404, headers: corsHeaders });
  }

  const featureInputs = await fetchCurrentFeatureInputs(env, currentPxi.date);
  const features = deps.extractMLFeatures({
    pxi_score: currentPxi.score,
    pxi_delta_1d: currentPxi.delta_1d,
    pxi_delta_7d: currentPxi.delta_7d,
    pxi_delta_30d: currentPxi.delta_30d,
    categories: featureInputs.categoryMap,
    indicators: featureInputs.indicatorMap,
    pxi_ma_5: featureInputs.pxi_ma_5,
    pxi_ma_20: featureInputs.pxi_ma_20,
    pxi_std_20: featureInputs.pxi_std_20,
  });

  const prediction7d = model.m['7d'] ? deps.xgbPredict(model.m['7d'], features, model.f) : null;
  const prediction30d = model.m['30d'] ? deps.xgbPredict(model.m['30d'], features, model.f) : null;

  return Response.json({
    date: currentPxi.date,
    current_score: currentPxi.score,
    model_version: model.v,
    predictions: {
      pxi_change_7d: {
        value: prediction7d,
        direction: interpretDirectionalPrediction(prediction7d),
      },
      pxi_change_30d: {
        value: prediction30d,
        direction: interpretDirectionalPrediction(prediction30d),
      },
    },
    features_used: Object.keys(features).length,
    key_features: {
      extreme_low: features.extreme_low,
      extreme_high: features.extreme_high,
      pxi_vs_ma_20: features.pxi_vs_ma_20,
      category_dispersion: features.category_dispersion,
      weak_categories_count: features.weak_categories_count,
    },
  }, { headers: corsHeaders });
}

async function handleMlLstmRoute(route: WorkerRouteContext, deps: ModelingDeps): Promise<Response> {
  const { env, corsHeaders } = route;

  const pxiHistory = await env.DB.prepare(
    'SELECT date, score, delta_7d FROM pxi_scores ORDER BY date DESC LIMIT 20'
  ).all<{ date: string; score: number; delta_7d: number | null }>();

  const model = await deps.loadLSTMModel(env.ML_MODELS);
  if (!model) {
    return Response.json({
      error: 'LSTM model not loaded',
      message: 'Model has not been uploaded to KV yet',
    }, { status: 503, headers: corsHeaders });
  }

  const seqLength = model.c.s;
  if (!pxiHistory.results || pxiHistory.results.length < seqLength) {
    return Response.json({
      error: 'Insufficient history',
      message: `Need ${seqLength} days, have ${pxiHistory.results?.length || 0}`,
    }, { status: 400, headers: corsHeaders });
  }

  const dates = pxiHistory.results.map((row) => row.date);
  const byDate = new Map<string, {
    score: number;
    delta_7d: number | null;
    categories: Record<string, number>;
  }>();
  for (const row of pxiHistory.results) {
    byDate.set(row.date, { score: row.score, delta_7d: row.delta_7d, categories: {} });
  }

  const [categoryData, vixData] = await Promise.all([
    env.DB.prepare(`
      SELECT date, category, score
      FROM category_scores
      WHERE date IN (${dates.map(() => '?').join(',')})
    `).bind(...dates).all<{ date: string; category: string; score: number }>(),
    env.DB.prepare(`
      SELECT date, value FROM indicator_values
      WHERE indicator_id = 'vix' AND date IN (${dates.map(() => '?').join(',')})
    `).bind(...dates).all<{ date: string; value: number }>(),
  ]);

  for (const row of categoryData.results || []) {
    if (byDate.has(row.date)) {
      byDate.get(row.date)!.categories[row.category] = row.score;
    }
  }

  const vixMap: Record<string, number> = {};
  for (const row of vixData.results || []) {
    vixMap[row.date] = row.value;
  }

  const sortedDates = [...dates].sort();
  const sequence = sortedDates.map((date) => {
    const day = byDate.get(date)!;
    return deps.extractLSTMFeatures(
      { ...day, vix: vixMap[date] },
      model.n,
      model.c.f,
    );
  });

  const pred7d = model.m['7d']
    ? deps.lstmForward(sequence, model.m['7d'].lstm, model.m['7d'].fc, model.c.h)
    : null;
  const pred30d = model.m['30d']
    ? deps.lstmForward(sequence, model.m['30d'].lstm, model.m['30d'].fc, model.c.h)
    : null;

  const currentDate = dates[0];
  const currentPxi = byDate.get(currentDate)!;

  return Response.json({
    date: currentDate,
    current_score: currentPxi.score,
    model_type: 'lstm',
    model_version: model.v,
    sequence_length: seqLength,
    predictions: {
      pxi_change_7d: {
        value: pred7d,
        direction: interpretDirectionalPrediction(pred7d),
      },
      pxi_change_30d: {
        value: pred30d,
        direction: interpretDirectionalPrediction(pred30d),
      },
    },
    features_used: model.c.f.length,
    feature_names: model.c.f,
  }, { headers: corsHeaders });
}

async function handlePredictReturnsRoute(route: WorkerRouteContext, deps: ModelingDeps): Promise<Response> {
  const { env, corsHeaders } = route;

  const model = await deps.loadSPYReturnModel(env.ML_MODELS);
  if (!model) {
    return Response.json({
      error: 'SPY return model not loaded',
      message: 'Model has not been uploaded to KV yet',
    }, { status: 503, headers: corsHeaders });
  }

  const currentPxi = await env.DB.prepare(`
    SELECT date, score, delta_1d, delta_7d, delta_30d
    FROM pxi_scores ORDER BY date DESC LIMIT 1
  `).first<{
    date: string;
    score: number;
    delta_1d: number | null;
    delta_7d: number | null;
    delta_30d: number | null;
  }>();

  if (!currentPxi) {
    return Response.json({ error: 'No PXI data' }, { status: 404, headers: corsHeaders });
  }

  const [categories, indicators, recentScores, vixHistory] = await Promise.all([
    env.DB.prepare(`
      SELECT category, score FROM category_scores WHERE date = ?
    `).bind(currentPxi.date).all<{ category: string; score: number }>(),
    env.DB.prepare(`
      SELECT indicator_id, value FROM indicator_values WHERE date = ?
    `).bind(currentPxi.date).all<{ indicator_id: string; value: number }>(),
    env.DB.prepare(`
      SELECT score FROM pxi_scores ORDER BY date DESC LIMIT 20
    `).all<{ score: number }>(),
    env.DB.prepare(`
      SELECT value FROM indicator_values WHERE indicator_id = 'vix' ORDER BY date DESC LIMIT 20
    `).all<{ value: number }>(),
  ]);

  const scores = (recentScores.results || []).map((row) => row.score);
  const pxi_ma_5 = scores.slice(0, 5).reduce((sum, value) => sum + value, 0) / Math.min(5, scores.length);
  const pxi_ma_20 = scores.reduce((sum, value) => sum + value, 0) / scores.length;
  const pxi_std_20 = Math.sqrt(scores.reduce((sum, value) => sum + Math.pow(value - pxi_ma_20, 2), 0) / scores.length);
  const pxi_vs_ma_20 = currentPxi.score - pxi_ma_20;

  const vixValues = (vixHistory.results || []).map((row) => row.value);
  const vix = vixValues[0] || 0;
  const vix_ma_20 = vixValues.length > 0 ? vixValues.reduce((sum, value) => sum + value, 0) / vixValues.length : 0;
  const vix_vs_ma = vix - vix_ma_20;

  const categoryMap: Record<string, number> = {};
  for (const category of categories.results || []) {
    categoryMap[category.category] = category.score;
  }

  const indicatorMap: Record<string, number> = {};
  for (const indicator of indicators.results || []) {
    indicatorMap[indicator.indicator_id] = indicator.value;
  }

  const categoryValues = Object.values(categoryMap);
  const category_mean = categoryValues.length > 0
    ? categoryValues.reduce((sum, value) => sum + value, 0) / categoryValues.length
    : 50;
  const category_max = categoryValues.length > 0 ? Math.max(...categoryValues) : 50;
  const category_min = categoryValues.length > 0 ? Math.min(...categoryValues) : 50;
  const category_dispersion = category_max - category_min;
  const category_std = categoryValues.length > 0
    ? Math.sqrt(categoryValues.reduce((sum, value) => sum + Math.pow(value - category_mean, 2), 0) / categoryValues.length)
    : 0;
  const strong_categories = categoryValues.filter((value) => value > 70).length;
  const weak_categories = categoryValues.filter((value) => value < 30).length;

  const features: Record<string, number> = {
    pxi_score: currentPxi.score,
    pxi_delta_1d: currentPxi.delta_1d ?? 0,
    pxi_delta_7d: currentPxi.delta_7d ?? 0,
    pxi_delta_30d: currentPxi.delta_30d ?? 0,
    pxi_bucket: currentPxi.score < 20 ? 0 : currentPxi.score < 40 ? 1 : currentPxi.score < 60 ? 2 : currentPxi.score < 80 ? 3 : 4,
    momentum_7d_signal: (currentPxi.delta_7d ?? 0) > 5 ? 2 : (currentPxi.delta_7d ?? 0) > 2 ? 1 : (currentPxi.delta_7d ?? 0) > -2 ? 0 : (currentPxi.delta_7d ?? 0) > -5 ? -1 : -2,
    momentum_30d_signal: (currentPxi.delta_30d ?? 0) > 10 ? 2 : (currentPxi.delta_30d ?? 0) > 4 ? 1 : (currentPxi.delta_30d ?? 0) > -4 ? 0 : (currentPxi.delta_30d ?? 0) > -10 ? -1 : -2,
    acceleration: (currentPxi.delta_7d ?? 0) - ((currentPxi.delta_30d ?? 0) / 4.3),
    acceleration_signal: 0,
    cat_breadth: categoryMap.breadth ?? 50,
    cat_credit: categoryMap.credit ?? 50,
    cat_crypto: categoryMap.crypto ?? 50,
    cat_global: categoryMap.global ?? 50,
    cat_liquidity: categoryMap.liquidity ?? 50,
    cat_macro: categoryMap.macro ?? 50,
    cat_positioning: categoryMap.positioning ?? 50,
    cat_volatility: categoryMap.volatility ?? 50,
    category_mean,
    category_dispersion,
    category_std,
    strong_categories,
    weak_categories,
    vix,
    hy_spread: indicatorMap.hy_oas ?? 0,
    ig_spread: indicatorMap.ig_oas ?? 0,
    breadth_ratio: indicatorMap.rsp_spy_ratio ?? 1,
    yield_curve: indicatorMap.yield_curve_2s10s ?? 0,
    dxy: indicatorMap.dxy ?? 100,
    btc_vs_200d: indicatorMap.btc_vs_200d ?? 0,
    vix_high: vix > 25 ? 1 : 0,
    vix_low: vix < 15 ? 1 : 0,
    vix_ma_20,
    vix_vs_ma,
    pxi_ma_5,
    pxi_ma_20,
    pxi_std_20,
    pxi_vs_ma_20,
    above_50: currentPxi.score > 50 ? 1 : 0,
    extreme_low: currentPxi.score < 25 ? 1 : 0,
    extreme_high: currentPxi.score > 75 ? 1 : 0,
    spread_widening: 0,
  };

  features.acceleration_signal = features.acceleration > 2 ? 1 : features.acceleration < -2 ? -1 : 0;

  const return7d = deps.predictSPYReturn(model, features, '7d');
  const return30d = deps.predictSPYReturn(model, features, '30d');

  const interpretReturn = (value: number) => {
    if (value > 3) return 'STRONG_BULLISH';
    if (value > 1) return 'BULLISH';
    if (value > -1) return 'NEUTRAL';
    if (value > -3) return 'BEARISH';
    return 'STRONG_BEARISH';
  };

  return Response.json({
    date: currentPxi.date,
    current_pxi: currentPxi.score,
    model_created: model.created_at,
    predictions: {
      spy_return_7d: {
        value: Math.round(return7d * 100) / 100,
        outlook: interpretReturn(return7d),
        unit: '%',
      },
      spy_return_30d: {
        value: Math.round(return30d * 100) / 100,
        outlook: interpretReturn(return30d),
        unit: '%',
      },
    },
    model_accuracy: {
      direction_acc_7d: model.metrics['7d'].cv_direction_acc,
      direction_acc_30d: model.metrics['30d'].cv_direction_acc,
      mae_7d: model.metrics['7d'].cv_mae_mean,
      mae_30d: model.metrics['30d'].cv_mae_mean,
    },
    disclaimer: 'Alpha model - predicts SPY returns, not PXI changes. Accuracy is modest (~53% 7d, ~65% 30d direction).',
  }, { headers: corsHeaders });
}

async function handleMlEnsembleRoute(route: WorkerRouteContext, deps: ModelingDeps): Promise<Response> {
  const { env, corsHeaders, executionContext } = route;

  const [xgboostModel, lstmModel] = await Promise.all([
    deps.loadMLModel(env.ML_MODELS),
    deps.loadLSTMModel(env.ML_MODELS),
  ]);

  const currentPxi = await env.DB.prepare(
    'SELECT date, score, delta_1d, delta_7d, delta_30d FROM pxi_scores ORDER BY date DESC LIMIT 1'
  ).first<{ date: string; score: number; delta_1d: number | null; delta_7d: number | null; delta_30d: number | null }>();

  if (!currentPxi) {
    return Response.json({ error: 'No PXI data available' }, { status: 404, headers: corsHeaders });
  }

  const currentDate = currentPxi.date;
  const currentScore = currentPxi.score;

  let xgboost: { pred_7d: number | null; pred_30d: number | null; dir_7d: string | null; dir_30d: string | null } | null = null;
  if (xgboostModel) {
    const featureInputs = await fetchCurrentFeatureInputs(env, currentDate);
    const features = deps.extractMLFeatures({
      pxi_score: currentPxi.score,
      pxi_delta_1d: currentPxi.delta_1d,
      pxi_delta_7d: currentPxi.delta_7d,
      pxi_delta_30d: currentPxi.delta_30d,
      categories: featureInputs.categoryMap,
      indicators: featureInputs.indicatorMap,
      pxi_ma_5: featureInputs.pxi_ma_5,
      pxi_ma_20: featureInputs.pxi_ma_20,
      pxi_std_20: featureInputs.pxi_std_20,
    });

    const pred7d = xgboostModel.m['7d'] ? deps.xgbPredict(xgboostModel.m['7d'], features, xgboostModel.f) : null;
    const pred30d = xgboostModel.m['30d'] ? deps.xgbPredict(xgboostModel.m['30d'], features, xgboostModel.f) : null;
    xgboost = {
      pred_7d: pred7d,
      pred_30d: pred30d,
      dir_7d: interpretDirectionalPrediction(pred7d),
      dir_30d: interpretDirectionalPrediction(pred30d),
    };
  }

  let lstm: { pred_7d: number | null; pred_30d: number | null; dir_7d: string | null; dir_30d: string | null } | null = null;
  if (lstmModel) {
    const seqLength = lstmModel.c.s;
    const pxiHistory = await env.DB.prepare(
      'SELECT date, score, delta_7d FROM pxi_scores ORDER BY date DESC LIMIT ?'
    ).bind(seqLength).all<{ date: string; score: number; delta_7d: number | null }>();

    if (pxiHistory.results && pxiHistory.results.length >= seqLength) {
      const dates = pxiHistory.results.map((row) => row.date);
      const [categoryData, vixData] = await Promise.all([
        env.DB.prepare(`
          SELECT date, category, score FROM category_scores
          WHERE date IN (${dates.map(() => '?').join(',')})
        `).bind(...dates).all<{ date: string; category: string; score: number }>(),
        env.DB.prepare(`
          SELECT date, value FROM indicator_values
          WHERE indicator_id = 'vix' AND date IN (${dates.map(() => '?').join(',')})
        `).bind(...dates).all<{ date: string; value: number }>(),
      ]);

      const byDate = new Map<string, { score: number; delta_7d: number | null; categories: Record<string, number> }>();
      for (const row of pxiHistory.results) {
        byDate.set(row.date, { score: row.score, delta_7d: row.delta_7d, categories: {} });
      }
      for (const row of categoryData.results || []) {
        if (byDate.has(row.date)) {
          byDate.get(row.date)!.categories[row.category] = row.score;
        }
      }

      const vixMap: Record<string, number> = {};
      for (const row of vixData.results || []) {
        vixMap[row.date] = row.value;
      }

      const sortedDates = [...dates].sort();
      const sequence = sortedDates.map((date) => {
        const day = byDate.get(date)!;
        return deps.extractLSTMFeatures({ ...day, vix: vixMap[date] }, lstmModel.n, lstmModel.c.f);
      });

      if (sequence.length === seqLength) {
        const pred7d = lstmModel.m['7d']
          ? deps.lstmForward(sequence, lstmModel.m['7d'].lstm, lstmModel.m['7d'].fc, lstmModel.c.h)
          : null;
        const pred30d = lstmModel.m['30d']
          ? deps.lstmForward(sequence, lstmModel.m['30d'].lstm, lstmModel.m['30d'].fc, lstmModel.c.h)
          : null;

        lstm = {
          pred_7d: pred7d,
          pred_30d: pred30d,
          dir_7d: interpretDirectionalPrediction(pred7d),
          dir_30d: interpretDirectionalPrediction(pred30d),
        };
      }
    }
  }

  if (!xgboost && !lstm) {
    return Response.json({
      error: 'No models available',
      message: 'Neither XGBoost nor LSTM models could be loaded',
    }, { status: 503, headers: corsHeaders });
  }

  const ensemblePredict = (
    xgValue: number | null,
    lstmValue: number | null,
  ): { value: number | null; xgboost_contrib: number | null; lstm_contrib: number | null } => {
    if (xgValue !== null && lstmValue !== null) {
      return {
        value: xgValue * 0.6 + lstmValue * 0.4,
        xgboost_contrib: xgValue * 0.6,
        lstm_contrib: lstmValue * 0.4,
      };
    }
    if (xgValue !== null) {
      return { value: xgValue, xgboost_contrib: xgValue, lstm_contrib: null };
    }
    if (lstmValue !== null) {
      return { value: lstmValue, xgboost_contrib: null, lstm_contrib: lstmValue };
    }
    return { value: null, xgboost_contrib: null, lstm_contrib: null };
  };

  const calcAgreement = (
    xgDirection: string | null,
    lstmDirection: string | null,
  ): { agreement: 'HIGH' | 'MEDIUM' | 'LOW' | null; note: string } => {
    if (!xgDirection || !lstmDirection) {
      return { agreement: null, note: 'Single model only' };
    }

    const upDirections = ['STRONG_UP', 'UP'];
    const downDirections = ['STRONG_DOWN', 'DOWN'];

    const xgUp = upDirections.includes(xgDirection);
    const xgDown = downDirections.includes(xgDirection);
    const lstmUp = upDirections.includes(lstmDirection);
    const lstmDown = downDirections.includes(lstmDirection);

    if (xgDirection === lstmDirection) {
      return { agreement: 'HIGH', note: 'Models agree on direction and magnitude' };
    }
    if ((xgUp && lstmUp) || (xgDown && lstmDown)) {
      return { agreement: 'MEDIUM', note: 'Models agree on direction' };
    }
    if (xgDirection === 'FLAT' || lstmDirection === 'FLAT') {
      return { agreement: 'MEDIUM', note: 'One model neutral' };
    }
    return { agreement: 'LOW', note: 'Models disagree on direction' };
  };

  const xg7d = xgboost?.pred_7d ?? null;
  const xg30d = xgboost?.pred_30d ?? null;
  const lstm7d = lstm?.pred_7d ?? null;
  const lstm30d = lstm?.pred_30d ?? null;
  const ensemble7d = ensemblePredict(xg7d, lstm7d);
  const ensemble30d = ensemblePredict(xg30d, lstm30d);
  const agreement7d = calcAgreement(xgboost?.dir_7d ?? null, lstm?.dir_7d ?? null);
  const agreement30d = calcAgreement(xgboost?.dir_30d ?? null, lstm?.dir_30d ?? null);

  const logPrediction = async () => {
    try {
      const predDate = new Date(currentDate);
      const target7d = new Date(predDate);
      target7d.setDate(target7d.getDate() + 7);
      const target30d = new Date(predDate);
      target30d.setDate(target30d.getDate() + 30);

      await env.DB.prepare(`
        INSERT OR REPLACE INTO ensemble_predictions (
          prediction_date, target_date_7d, target_date_30d, current_score,
          xgboost_7d, xgboost_30d, lstm_7d, lstm_30d,
          ensemble_7d, ensemble_30d, confidence_7d, confidence_30d
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).bind(
        currentDate,
        asDateKey(target7d),
        asDateKey(target30d),
        currentScore,
        xg7d,
        xg30d,
        lstm7d,
        lstm30d,
        ensemble7d.value,
        ensemble30d.value,
        agreement7d.agreement,
        agreement30d.agreement,
      ).run();
    } catch (error) {
      console.error('Failed to log ensemble prediction:', error);
    }
  };

  if (executionContext) {
    executionContext.waitUntil(logPrediction());
  } else {
    void logPrediction();
  }

  return Response.json({
    date: currentDate,
    current_score: currentScore,
    ensemble: {
      weights: { xgboost: 0.6, lstm: 0.4 },
      predictions: {
        pxi_change_7d: {
          value: ensemble7d.value,
          direction: interpretDirectionalPrediction(ensemble7d.value),
          confidence: agreement7d.agreement,
          components: {
            xgboost: xg7d,
            lstm: lstm7d,
          },
        },
        pxi_change_30d: {
          value: ensemble30d.value,
          direction: interpretDirectionalPrediction(ensemble30d.value),
          confidence: agreement30d.agreement,
          components: {
            xgboost: xg30d,
            lstm: lstm30d,
          },
        },
      },
    },
    models: {
      xgboost: xgboost ? {
        available: true,
        predictions: {
          pxi_change_7d: { value: xgboost.pred_7d, direction: xgboost.dir_7d },
          pxi_change_30d: { value: xgboost.pred_30d, direction: xgboost.dir_30d },
        },
      } : { available: false },
      lstm: lstm ? {
        available: true,
        predictions: {
          pxi_change_7d: { value: lstm.pred_7d, direction: lstm.dir_7d },
          pxi_change_30d: { value: lstm.pred_30d, direction: lstm.dir_30d },
        },
      } : { available: false },
    },
    interpretation: {
      d7: agreement7d,
      d30: agreement30d,
    },
  }, { headers: corsHeaders });
}

async function handleAccuracyRoute(route: WorkerRouteContext, deps: ModelingDeps): Promise<Response> {
  const { env, url, corsHeaders } = route;
  const includePending = url.searchParams.get('include_pending') === 'true';
  const minimumReliableSample = deps.MINIMUM_RELIABLE_SAMPLE ?? 30;

  const predictions = await env.DB.prepare(`
    SELECT prediction_date, predicted_change_7d, predicted_change_30d,
           actual_change_7d, actual_change_30d, confidence_7d, confidence_30d
    FROM prediction_log
    ${includePending ? '' : 'WHERE actual_change_7d IS NOT NULL OR actual_change_30d IS NOT NULL'}
    ORDER BY prediction_date DESC
    LIMIT 100
  `).all<{
    prediction_date: string;
    predicted_change_7d: number | null;
    predicted_change_30d: number | null;
    actual_change_7d: number | null;
    actual_change_30d: number | null;
    confidence_7d: number | null;
    confidence_30d: number | null;
  }>();

  if (!predictions.results || predictions.results.length === 0) {
    const unavailableReasons = [
      includePending ? 'no_predictions' : 'no_evaluated_predictions',
      'no_7d_evaluated_predictions',
      'no_30d_evaluated_predictions',
      'insufficient_sample',
    ];
    const coverage = { total_predictions: 0, evaluated_count: 0, pending_count: 0 };
    return Response.json({
      message: includePending ? 'No predictions logged yet' : 'No evaluated predictions yet',
      as_of: deps.asIsoDateTime(new Date()),
      coverage,
      coverage_quality: 'INSUFFICIENT',
      minimum_reliable_sample: minimumReliableSample,
      unavailable_reasons: unavailableReasons,
      total_predictions: 0,
      evaluated_count: 0,
      pending_count: 0,
      metrics: null,
    }, { headers: corsHeaders });
  }

  const pendingCount = predictions.results.filter((prediction) =>
    prediction.actual_change_7d === null && prediction.actual_change_30d === null
  ).length;
  const evaluatedCount = predictions.results.length - pendingCount;

  let d7_correct_direction = 0;
  let d7_total = 0;
  let d7_mae = 0;
  let d30_correct_direction = 0;
  let d30_total = 0;
  let d30_mae = 0;

  const recentPredictions: Array<{
    date: string;
    predicted_7d: number | null;
    actual_7d: number | null;
    error_7d: number | null;
    predicted_30d: number | null;
    actual_30d: number | null;
    error_30d: number | null;
  }> = [];

  for (const prediction of predictions.results) {
    const pred7d = prediction.predicted_change_7d;
    const act7d = prediction.actual_change_7d;
    const pred30d = prediction.predicted_change_30d;
    const act30d = prediction.actual_change_30d;

    let error7d: number | null = null;
    let error30d: number | null = null;

    if (pred7d !== null && act7d !== null) {
      d7_total += 1;
      error7d = Math.abs(pred7d - act7d);
      d7_mae += error7d;
      if ((pred7d > 0 && act7d > 0) || (pred7d < 0 && act7d < 0) || (pred7d === 0 && act7d === 0)) {
        d7_correct_direction += 1;
      }
    }

    if (pred30d !== null && act30d !== null) {
      d30_total += 1;
      error30d = Math.abs(pred30d - act30d);
      d30_mae += error30d;
      if ((pred30d > 0 && act30d > 0) || (pred30d < 0 && act30d < 0) || (pred30d === 0 && act30d === 0)) {
        d30_correct_direction += 1;
      }
    }

    if (recentPredictions.length < 10) {
      recentPredictions.push({
        date: prediction.prediction_date,
        predicted_7d: pred7d,
        actual_7d: act7d,
        error_7d: error7d,
        predicted_30d: pred30d,
        actual_30d: act30d,
        error_30d: error30d,
      });
    }
  }

  const unavailableReasons: string[] = [];
  if (d7_total === 0) unavailableReasons.push('no_7d_evaluated_predictions');
  if (d30_total === 0) unavailableReasons.push('no_30d_evaluated_predictions');
  if (evaluatedCount === 0) unavailableReasons.push('no_evaluated_predictions');
  if (evaluatedCount < minimumReliableSample) unavailableReasons.push('insufficient_sample');

  const asOfPredictionDate = predictions.results[0]?.prediction_date;
  const asOf = asOfPredictionDate ? `${asOfPredictionDate}T00:00:00.000Z` : deps.asIsoDateTime(new Date());
  const coverage = {
    total_predictions: predictions.results.length,
    evaluated_count: evaluatedCount,
    pending_count: pendingCount,
  };

  return Response.json({
    as_of: asOf,
    coverage,
    coverage_quality: deps.calibrationQualityForSampleSize(evaluatedCount),
    minimum_reliable_sample: minimumReliableSample,
    unavailable_reasons: unavailableReasons,
    total_predictions: predictions.results.length,
    evaluated_count: evaluatedCount,
    pending_count: pendingCount,
    metrics: {
      d7: d7_total > 0 ? {
        direction_accuracy: `${((d7_correct_direction / d7_total) * 100).toFixed(1)}%`,
        mean_absolute_error: (d7_mae / d7_total).toFixed(2),
        sample_size: d7_total,
      } : null,
      d30: d30_total > 0 ? {
        direction_accuracy: `${((d30_correct_direction / d30_total) * 100).toFixed(1)}%`,
        mean_absolute_error: (d30_mae / d30_total).toFixed(2),
        sample_size: d30_total,
      } : null,
    },
    recent_predictions: recentPredictions,
  }, { headers: corsHeaders });
}

async function handleMlAccuracyRoute(route: WorkerRouteContext, deps: ModelingDeps): Promise<Response> {
  const { env, url, corsHeaders } = route;
  const includePending = url.searchParams.get('include_pending') === 'true';
  const minimumReliableSample = deps.MINIMUM_RELIABLE_SAMPLE ?? 30;

  const predictions = await env.DB.prepare(`
    SELECT prediction_date, target_date_7d, target_date_30d, current_score,
           xgboost_7d, xgboost_30d, lstm_7d, lstm_30d,
           ensemble_7d, ensemble_30d, confidence_7d, confidence_30d,
           actual_change_7d, actual_change_30d,
           direction_correct_7d, direction_correct_30d, evaluated_at
    FROM ensemble_predictions
    ${includePending ? '' : 'WHERE evaluated_at IS NOT NULL'}
    ORDER BY prediction_date DESC
    LIMIT 100
  `).all<{
    prediction_date: string;
    target_date_7d: string;
    target_date_30d: string;
    current_score: number;
    xgboost_7d: number | null;
    xgboost_30d: number | null;
    lstm_7d: number | null;
    lstm_30d: number | null;
    ensemble_7d: number | null;
    ensemble_30d: number | null;
    confidence_7d: string | null;
    confidence_30d: string | null;
    actual_change_7d: number | null;
    actual_change_30d: number | null;
    direction_correct_7d: number | null;
    direction_correct_30d: number | null;
    evaluated_at: string | null;
  }>();

  if (!predictions.results || predictions.results.length === 0) {
    const unavailableReasons = [
      includePending ? 'no_predictions' : 'no_evaluated_predictions',
      'no_7d_evaluated_predictions',
      'no_30d_evaluated_predictions',
      'insufficient_sample',
    ];
    const coverage = { total_predictions: 0, evaluated_count: 0, pending_count: 0 };
    const payload: MLAccuracyApiResponsePayload = {
      message: includePending ? 'No ensemble predictions logged yet' : 'No evaluated ensemble predictions yet',
      as_of: deps.asIsoDateTime(new Date()),
      coverage,
      coverage_quality: 'INSUFFICIENT',
      minimum_reliable_sample: minimumReliableSample,
      unavailable_reasons: unavailableReasons,
      total_predictions: 0,
      evaluated_count: 0,
      pending_count: 0,
      metrics: null,
    };

    return Response.json(payload, { headers: corsHeaders });
  }

  const pendingCount = predictions.results.filter((prediction) => prediction.evaluated_at === null).length;
  const evaluatedCount = predictions.results.length - pendingCount;

  const metrics = {
    xgboost: { d7_correct: 0, d7_total: 0, d7_mae: 0, d30_correct: 0, d30_total: 0, d30_mae: 0 },
    lstm: { d7_correct: 0, d7_total: 0, d7_mae: 0, d30_correct: 0, d30_total: 0, d30_mae: 0 },
    ensemble: { d7_correct: 0, d7_total: 0, d7_mae: 0, d30_correct: 0, d30_total: 0, d30_mae: 0 },
  };

  const recentPredictions: Array<{
    date: string;
    current_score: number;
    xgboost_7d: number | null;
    lstm_7d: number | null;
    ensemble_7d: number | null;
    actual_7d: number | null;
    xgboost_30d: number | null;
    lstm_30d: number | null;
    ensemble_30d: number | null;
    actual_30d: number | null;
    confidence_7d: string | null;
    confidence_30d: string | null;
  }> = [];

  for (const prediction of predictions.results) {
    const act7d = prediction.actual_change_7d;
    const act30d = prediction.actual_change_30d;

    if (act7d !== null) {
      if (prediction.xgboost_7d !== null) {
        metrics.xgboost.d7_total += 1;
        metrics.xgboost.d7_mae += Math.abs(prediction.xgboost_7d - act7d);
        if ((prediction.xgboost_7d > 0 && act7d > 0) || (prediction.xgboost_7d < 0 && act7d < 0)) {
          metrics.xgboost.d7_correct += 1;
        }
      }
      if (prediction.lstm_7d !== null) {
        metrics.lstm.d7_total += 1;
        metrics.lstm.d7_mae += Math.abs(prediction.lstm_7d - act7d);
        if ((prediction.lstm_7d > 0 && act7d > 0) || (prediction.lstm_7d < 0 && act7d < 0)) {
          metrics.lstm.d7_correct += 1;
        }
      }
      if (prediction.ensemble_7d !== null) {
        metrics.ensemble.d7_total += 1;
        metrics.ensemble.d7_mae += Math.abs(prediction.ensemble_7d - act7d);
        if ((prediction.ensemble_7d > 0 && act7d > 0) || (prediction.ensemble_7d < 0 && act7d < 0)) {
          metrics.ensemble.d7_correct += 1;
        }
      }
    }

    if (act30d !== null) {
      if (prediction.xgboost_30d !== null) {
        metrics.xgboost.d30_total += 1;
        metrics.xgboost.d30_mae += Math.abs(prediction.xgboost_30d - act30d);
        if ((prediction.xgboost_30d > 0 && act30d > 0) || (prediction.xgboost_30d < 0 && act30d < 0)) {
          metrics.xgboost.d30_correct += 1;
        }
      }
      if (prediction.lstm_30d !== null) {
        metrics.lstm.d30_total += 1;
        metrics.lstm.d30_mae += Math.abs(prediction.lstm_30d - act30d);
        if ((prediction.lstm_30d > 0 && act30d > 0) || (prediction.lstm_30d < 0 && act30d < 0)) {
          metrics.lstm.d30_correct += 1;
        }
      }
      if (prediction.ensemble_30d !== null) {
        metrics.ensemble.d30_total += 1;
        metrics.ensemble.d30_mae += Math.abs(prediction.ensemble_30d - act30d);
        if ((prediction.ensemble_30d > 0 && act30d > 0) || (prediction.ensemble_30d < 0 && act30d < 0)) {
          metrics.ensemble.d30_correct += 1;
        }
      }
    }

    if (recentPredictions.length < 10) {
      recentPredictions.push({
        date: prediction.prediction_date,
        current_score: prediction.current_score,
        xgboost_7d: prediction.xgboost_7d,
        lstm_7d: prediction.lstm_7d,
        ensemble_7d: prediction.ensemble_7d,
        actual_7d: act7d,
        xgboost_30d: prediction.xgboost_30d,
        lstm_30d: prediction.lstm_30d,
        ensemble_30d: prediction.ensemble_30d,
        actual_30d: act30d,
        confidence_7d: prediction.confidence_7d,
        confidence_30d: prediction.confidence_30d,
      });
    }
  }

  const unavailableReasons: string[] = [];
  if (metrics.ensemble.d7_total === 0) unavailableReasons.push('no_7d_evaluated_predictions');
  if (metrics.ensemble.d30_total === 0) unavailableReasons.push('no_30d_evaluated_predictions');
  if (evaluatedCount === 0) unavailableReasons.push('no_evaluated_predictions');
  if (evaluatedCount < minimumReliableSample) unavailableReasons.push('insufficient_sample');

  const asOfPredictionDate = predictions.results[0]?.prediction_date;
  const asOf = asOfPredictionDate ? `${asOfPredictionDate}T00:00:00.000Z` : deps.asIsoDateTime(new Date());
  const coverage = {
    total_predictions: predictions.results.length,
    evaluated_count: evaluatedCount,
    pending_count: pendingCount,
  };

  const payload: MLAccuracyApiResponsePayload = {
    as_of: asOf,
    coverage,
    coverage_quality: deps.calibrationQualityForSampleSize(evaluatedCount),
    minimum_reliable_sample: minimumReliableSample,
    unavailable_reasons: unavailableReasons,
    total_predictions: predictions.results.length,
    evaluated_count: evaluatedCount,
    pending_count: pendingCount,
    metrics: {
      xgboost: formatDirectionalMetrics(metrics.xgboost),
      lstm: formatDirectionalMetrics(metrics.lstm),
      ensemble: formatDirectionalMetrics(metrics.ensemble),
    },
    recent_predictions: recentPredictions,
  };

  return Response.json(payload, { headers: corsHeaders });
}

async function handleMlBacktestRoute(route: WorkerRouteContext, deps: ModelingDeps): Promise<Response> {
  const { env, url, corsHeaders } = route;
  const limit = parseInt(url.searchParams.get('limit') || '500', 10);
  const offset = parseInt(url.searchParams.get('offset') || '0', 10);

  const [xgboostModel, lstmModel] = await Promise.all([
    deps.loadMLModel(env.ML_MODELS),
    deps.loadLSTMModel(env.ML_MODELS),
  ]);

  if (!xgboostModel && !lstmModel) {
    return Response.json({
      error: 'No models available for backtesting',
    }, { status: 503, headers: corsHeaders });
  }

  const pxiHistory = await env.DB.prepare(`
    SELECT
      p.date, p.score, p.delta_1d, p.delta_7d, p.delta_30d,
      (SELECT score FROM pxi_scores p2 WHERE p2.date > p.date ORDER BY p2.date LIMIT 1 OFFSET 6) as actual_score_7d,
      (SELECT score FROM pxi_scores p2 WHERE p2.date > p.date ORDER BY p2.date LIMIT 1 OFFSET 29) as actual_score_30d
    FROM pxi_scores p
    WHERE p.date <= date('now', '-37 days')
    ORDER BY p.date DESC
    LIMIT ? OFFSET ?
  `).bind(limit, offset).all<{
    date: string;
    score: number;
    delta_1d: number | null;
    delta_7d: number | null;
    delta_30d: number | null;
    actual_score_7d: number | null;
    actual_score_30d: number | null;
  }>();

  if (!pxiHistory.results || pxiHistory.results.length === 0) {
    return Response.json({
      error: 'No historical data available for backtesting',
      hint: 'Need at least 37 days of historical PXI scores',
    }, { status: 400, headers: corsHeaders });
  }

  const dates = pxiHistory.results.map((row) => row.date);
  const [categoryData, indicatorData] = await Promise.all([
    env.DB.prepare(`
      SELECT date, category, score FROM category_scores
      WHERE date IN (${dates.map(() => '?').join(',')})
    `).bind(...dates).all<{ date: string; category: string; score: number }>(),
    env.DB.prepare(`
      SELECT date, indicator_id, value FROM indicator_values
      WHERE date IN (${dates.map(() => '?').join(',')})
    `).bind(...dates).all<{ date: string; indicator_id: string; value: number }>(),
  ]);

  const categoryMap = new Map<string, Record<string, number>>();
  for (const row of categoryData.results || []) {
    if (!categoryMap.has(row.date)) categoryMap.set(row.date, {});
    categoryMap.get(row.date)![row.category] = row.score;
  }

  const indicatorMap = new Map<string, Record<string, number>>();
  for (const row of indicatorData.results || []) {
    if (!indicatorMap.has(row.date)) indicatorMap.set(row.date, {});
    indicatorMap.get(row.date)![row.indicator_id] = row.value;
  }

  const sortedHistory = [...pxiHistory.results].sort((left, right) => left.date.localeCompare(right.date));
  const rollingStats = new Map<string, { ma5: number; ma20: number; std20: number }>();
  for (let index = 0; index < sortedHistory.length; index += 1) {
    const recent5 = sortedHistory.slice(Math.max(0, index - 4), index + 1).map((row) => row.score);
    const recent20 = sortedHistory.slice(Math.max(0, index - 19), index + 1).map((row) => row.score);
    const ma5 = recent5.reduce((sum, value) => sum + value, 0) / recent5.length;
    const ma20 = recent20.reduce((sum, value) => sum + value, 0) / recent20.length;
    const std20 = Math.sqrt(recent20.reduce((sum, value) => sum + Math.pow(value - ma20, 2), 0) / recent20.length);
    rollingStats.set(sortedHistory[index].date, { ma5, ma20, std20 });
  }

  const results: Array<{
    date: string;
    pxi_score: number;
    xgboost_7d: number | null;
    xgboost_30d: number | null;
    lstm_7d: number | null;
    lstm_30d: number | null;
    ensemble_7d: number | null;
    ensemble_30d: number | null;
    actual_7d: number | null;
    actual_30d: number | null;
  }> = [];

  const metrics = {
    xgboost: { d7_correct: 0, d7_total: 0, d7_mae: 0, d30_correct: 0, d30_total: 0, d30_mae: 0 },
    lstm: { d7_correct: 0, d7_total: 0, d7_mae: 0, d30_correct: 0, d30_total: 0, d30_mae: 0 },
    ensemble: { d7_correct: 0, d7_total: 0, d7_mae: 0, d30_correct: 0, d30_total: 0, d30_mae: 0 },
  };

  for (const row of pxiHistory.results) {
    const categories = categoryMap.get(row.date) || {};
    const indicators = indicatorMap.get(row.date) || {};
    const rolling = rollingStats.get(row.date) || { ma5: row.score, ma20: row.score, std20: 10 };
    const actual7d = row.actual_score_7d !== null ? row.actual_score_7d - row.score : null;
    const actual30d = row.actual_score_30d !== null ? row.actual_score_30d - row.score : null;

    let xg7d: number | null = null;
    let xg30d: number | null = null;
    let lstm7d: number | null = null;
    let lstm30d: number | null = null;

    if (xgboostModel) {
      const features = deps.extractMLFeatures({
        pxi_score: row.score,
        pxi_delta_1d: row.delta_1d,
        pxi_delta_7d: row.delta_7d,
        pxi_delta_30d: row.delta_30d,
        categories,
        indicators,
        pxi_ma_5: rolling.ma5,
        pxi_ma_20: rolling.ma20,
        pxi_std_20: rolling.std20,
      });
      xg7d = xgboostModel.m['7d'] ? deps.xgbPredict(xgboostModel.m['7d'], features, xgboostModel.f) : null;
      xg30d = xgboostModel.m['30d'] ? deps.xgbPredict(xgboostModel.m['30d'], features, xgboostModel.f) : null;
    }

    if (lstmModel && Object.keys(categories).length > 0) {
      const vix = indicators.vix ?? 20;
      deps.extractLSTMFeatures(
        { score: row.score, delta_7d: row.delta_7d, categories, vix },
        lstmModel.n,
        lstmModel.c.f,
      );
    }

    const ensemble7d = xg7d;
    const ensemble30d = xg30d;

    if (actual7d !== null) {
      if (xg7d !== null) {
        metrics.xgboost.d7_mae += Math.abs(xg7d - actual7d);
        metrics.xgboost.d7_total += 1;
        if ((xg7d > 0 && actual7d > 0) || (xg7d < 0 && actual7d < 0) || (xg7d === 0 && actual7d === 0)) {
          metrics.xgboost.d7_correct += 1;
        }
      }
      if (ensemble7d !== null) {
        metrics.ensemble.d7_mae += Math.abs(ensemble7d - actual7d);
        metrics.ensemble.d7_total += 1;
        if ((ensemble7d > 0 && actual7d > 0) || (ensemble7d < 0 && actual7d < 0)) {
          metrics.ensemble.d7_correct += 1;
        }
      }
    }

    if (actual30d !== null) {
      if (xg30d !== null) {
        metrics.xgboost.d30_mae += Math.abs(xg30d - actual30d);
        metrics.xgboost.d30_total += 1;
        if ((xg30d > 0 && actual30d > 0) || (xg30d < 0 && actual30d < 0)) {
          metrics.xgboost.d30_correct += 1;
        }
      }
      if (ensemble30d !== null) {
        metrics.ensemble.d30_mae += Math.abs(ensemble30d - actual30d);
        metrics.ensemble.d30_total += 1;
        if ((ensemble30d > 0 && actual30d > 0) || (ensemble30d < 0 && actual30d < 0)) {
          metrics.ensemble.d30_correct += 1;
        }
      }
    }

    results.push({
      date: row.date,
      pxi_score: row.score,
      xgboost_7d: xg7d,
      xgboost_30d: xg30d,
      lstm_7d: lstm7d,
      lstm_30d: lstm30d,
      ensemble_7d: ensemble7d,
      ensemble_30d: ensemble30d,
      actual_7d: actual7d,
      actual_30d: actual30d,
    });
  }

  const validResults7d = results.filter((result) => result.actual_7d !== null && result.xgboost_7d !== null);
  const validResults30d = results.filter((result) => result.actual_30d !== null && result.xgboost_30d !== null);
  const r2_7d = calculateR2(
    validResults7d.map((result) => result.xgboost_7d!),
    validResults7d.map((result) => result.actual_7d!),
  );
  const r2_30d = calculateR2(
    validResults30d.map((result) => result.xgboost_30d!),
    validResults30d.map((result) => result.actual_30d!),
  );

  return Response.json({
    summary: {
      total_observations: results.length,
      date_range: {
        start: results[results.length - 1]?.date,
        end: results[0]?.date,
      },
      models_tested: {
        xgboost: !!xgboostModel,
        lstm: false,
      },
      note: 'This is IN-SAMPLE performance (models trained on this data). True OOS metrics come from live predictions.',
    },
    metrics: {
      xgboost: {
        ...formatDirectionalMetrics(metrics.xgboost),
        r2_7d: r2_7d.toFixed(4),
        r2_30d: r2_30d.toFixed(4),
      },
      ensemble: formatDirectionalMetrics(metrics.ensemble),
    },
    by_pxi_bucket: [
      { name: '0-20', min: 0, max: 20 },
      { name: '20-40', min: 20, max: 40 },
      { name: '40-60', min: 40, max: 60 },
      { name: '60-80', min: 60, max: 80 },
      { name: '80-100', min: 80, max: 100 },
    ].map((bucket) => {
      const bucketResults = validResults7d.filter(
        (result) => result.pxi_score >= bucket.min && result.pxi_score < bucket.max,
      );
      if (bucketResults.length === 0) return { bucket: bucket.name, count: 0 };

      const correctDir = bucketResults.filter(
        (result) => (result.xgboost_7d! > 0 && result.actual_7d! > 0) || (result.xgboost_7d! < 0 && result.actual_7d! < 0),
      ).length;

      return {
        bucket: bucket.name,
        count: bucketResults.length,
        direction_accuracy_7d: `${((correctDir / bucketResults.length) * 100).toFixed(1)}%`,
        avg_predicted_7d: (bucketResults.reduce((sum, result) => sum + result.xgboost_7d!, 0) / bucketResults.length).toFixed(2),
        avg_actual_7d: (bucketResults.reduce((sum, result) => sum + result.actual_7d!, 0) / bucketResults.length).toFixed(2),
      };
    }),
    recent_predictions: results.slice(0, 20).map((result) => ({
      date: result.date,
      pxi: result.pxi_score.toFixed(1),
      xgb_7d: result.xgboost_7d?.toFixed(2),
      actual_7d: result.actual_7d?.toFixed(2),
      xgb_30d: result.xgboost_30d?.toFixed(2),
      actual_30d: result.actual_30d?.toFixed(2),
    })),
  }, { headers: corsHeaders });
}

async function handleRetrainRoute(route: WorkerRouteContext, deps: ModelingDeps): Promise<Response> {
  const { request, env, corsHeaders, clientIP } = route;

  const adminAuthFailure = await deps.enforceAdminAuth(request, env, corsHeaders, clientIP);
  if (adminAuthFailure) {
    return adminAuthFailure;
  }

  const evaluatedPredictions = await env.DB.prepare(`
    SELECT prediction_date, similar_periods, predicted_change_7d, predicted_change_30d,
           actual_change_7d, actual_change_30d
    FROM prediction_log
    WHERE (actual_change_7d IS NOT NULL OR actual_change_30d IS NOT NULL)
      AND similar_periods IS NOT NULL
  `).all<{
    prediction_date: string;
    similar_periods: string;
    predicted_change_7d: number | null;
    predicted_change_30d: number | null;
    actual_change_7d: number | null;
    actual_change_30d: number | null;
  }>();

  if (!evaluatedPredictions.results || evaluatedPredictions.results.length === 0) {
    return Response.json({
      message: 'No evaluated predictions to learn from',
      periods_updated: 0,
    }, { headers: corsHeaders });
  }

  const periodStats: Record<string, {
    times_used: number;
    correct_7d: number;
    total_7d: number;
    errors_7d: number[];
    correct_30d: number;
    total_30d: number;
    errors_30d: number[];
  }> = {};

  for (const prediction of evaluatedPredictions.results) {
    let periods: string[] = [];
    try {
      periods = JSON.parse(prediction.similar_periods);
    } catch {
      continue;
    }

    for (const periodDate of periods) {
      if (!periodStats[periodDate]) {
        periodStats[periodDate] = {
          times_used: 0,
          correct_7d: 0,
          total_7d: 0,
          errors_7d: [],
          correct_30d: 0,
          total_30d: 0,
          errors_30d: [],
        };
      }

      const stats = periodStats[periodDate];
      stats.times_used += 1;

      if (prediction.predicted_change_7d !== null && prediction.actual_change_7d !== null) {
        stats.total_7d += 1;
        const correctDir = (prediction.predicted_change_7d > 0 && prediction.actual_change_7d > 0)
          || (prediction.predicted_change_7d < 0 && prediction.actual_change_7d < 0)
          || (prediction.predicted_change_7d === 0 && prediction.actual_change_7d === 0);
        if (correctDir) stats.correct_7d += 1;
        stats.errors_7d.push(Math.abs(prediction.predicted_change_7d - prediction.actual_change_7d));
      }

      if (prediction.predicted_change_30d !== null && prediction.actual_change_30d !== null) {
        stats.total_30d += 1;
        const correctDir = (prediction.predicted_change_30d > 0 && prediction.actual_change_30d > 0)
          || (prediction.predicted_change_30d < 0 && prediction.actual_change_30d < 0)
          || (prediction.predicted_change_30d === 0 && prediction.actual_change_30d === 0);
        if (correctDir) stats.correct_30d += 1;
        stats.errors_30d.push(Math.abs(prediction.predicted_change_30d - prediction.actual_change_30d));
      }
    }
  }

  let periodsUpdated = 0;
  for (const [periodDate, stats] of Object.entries(periodStats)) {
    const dir7d = stats.total_7d > 0 ? stats.correct_7d / stats.total_7d : 0.5;
    const dir30d = stats.total_30d > 0 ? stats.correct_30d / stats.total_30d : 0.5;
    const accuracyScore = stats.total_30d > 0 ? (dir7d * 0.6 + dir30d * 0.4) : dir7d;
    const avgError7d = stats.errors_7d.length > 0 ? stats.errors_7d.reduce((sum, value) => sum + value, 0) / stats.errors_7d.length : null;
    const avgError30d = stats.errors_30d.length > 0 ? stats.errors_30d.reduce((sum, value) => sum + value, 0) / stats.errors_30d.length : null;

    await env.DB.prepare(`
      INSERT OR REPLACE INTO period_accuracy
      (period_date, times_used, correct_direction_7d, correct_direction_30d,
       total_7d_predictions, total_30d_predictions, avg_error_7d, avg_error_30d,
       accuracy_score, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    `).bind(
      periodDate,
      stats.times_used,
      stats.correct_7d,
      stats.correct_30d,
      stats.total_7d,
      stats.total_30d,
      avgError7d,
      avgError30d,
      accuracyScore,
    ).run();
    periodsUpdated += 1;
  }

  const totalPredictions = evaluatedPredictions.results.length;
  let total7dCorrect = 0;
  let total7d = 0;
  for (const prediction of evaluatedPredictions.results) {
    if (prediction.predicted_change_7d !== null && prediction.actual_change_7d !== null) {
      total7d += 1;
      if (
        (prediction.predicted_change_7d > 0 && prediction.actual_change_7d > 0)
        || (prediction.predicted_change_7d < 0 && prediction.actual_change_7d < 0)
      ) {
        total7dCorrect += 1;
      }
    }
  }

  const overallAccuracy = total7d > 0 ? total7dCorrect / total7d : 0.5;
  let newAccuracyWeight = 0.3;
  if (overallAccuracy > 0.65) newAccuracyWeight = 0.5;
  else if (overallAccuracy > 0.55) newAccuracyWeight = 0.4;
  else if (overallAccuracy < 0.45) newAccuracyWeight = 0.2;

  await env.DB.prepare(`
    UPDATE model_params SET param_value = ?, updated_at = datetime('now'),
    notes = ? WHERE param_key = 'accuracy_weight'
  `).bind(
    newAccuracyWeight,
    `Auto-tuned based on ${total7d} predictions (${(overallAccuracy * 100).toFixed(1)}% accuracy)`,
  ).run();

  const thresholdParams = await env.DB.prepare(`
    SELECT param_key, param_value FROM model_params
    WHERE param_key LIKE 'bucket_threshold_%'
  `).all<{ param_key: string; param_value: number }>();

  const currentThresholds = { t1: 20, t2: 40, t3: 60, t4: 80 };
  for (const param of thresholdParams.results || []) {
    if (param.param_key === 'bucket_threshold_1') currentThresholds.t1 = param.param_value;
    if (param.param_key === 'bucket_threshold_2') currentThresholds.t2 = param.param_value;
    if (param.param_key === 'bucket_threshold_3') currentThresholds.t3 = param.param_value;
    if (param.param_key === 'bucket_threshold_4') currentThresholds.t4 = param.param_value;
  }

  const pxiScoresForPredictions = await env.DB.prepare(`
    SELECT pl.prediction_date, ps.score, pl.predicted_change_7d, pl.actual_change_7d
    FROM prediction_log pl
    JOIN pxi_scores ps ON pl.prediction_date = ps.date
    WHERE pl.actual_change_7d IS NOT NULL
  `).all<{ prediction_date: string; score: number; predicted_change_7d: number; actual_change_7d: number }>();

  const getBucketIndex = (score: number, thresholds: typeof currentThresholds): number => {
    if (score < thresholds.t1) return 0;
    if (score < thresholds.t2) return 1;
    if (score < thresholds.t3) return 2;
    if (score < thresholds.t4) return 3;
    return 4;
  };

  const bucketStats = [
    { correct: 0, total: 0 },
    { correct: 0, total: 0 },
    { correct: 0, total: 0 },
    { correct: 0, total: 0 },
    { correct: 0, total: 0 },
  ];

  for (const prediction of pxiScoresForPredictions.results || []) {
    const bucketIndex = getBucketIndex(prediction.score, currentThresholds);
    bucketStats[bucketIndex].total += 1;
    if (
      (prediction.predicted_change_7d > 0 && prediction.actual_change_7d > 0)
      || (prediction.predicted_change_7d < 0 && prediction.actual_change_7d < 0)
    ) {
      bucketStats[bucketIndex].correct += 1;
    }
  }

  const bucketAccuracies = bucketStats.map((bucket) => bucket.total >= 3 ? bucket.correct / bucket.total : null);
  const newThresholds = { ...currentThresholds };
  const minDiff = 8;

  if (bucketStats.reduce((sum, bucket) => sum + bucket.total, 0) >= 10) {
    if (bucketAccuracies[0] !== null && bucketAccuracies[1] !== null) {
      const diff = bucketAccuracies[0] - bucketAccuracies[1];
      if (diff > 0.15) newThresholds.t1 = Math.min(currentThresholds.t1 + 2, currentThresholds.t2 - minDiff);
      else if (diff < -0.15) newThresholds.t1 = Math.max(currentThresholds.t1 - 2, 10);
    }
    if (bucketAccuracies[1] !== null && bucketAccuracies[2] !== null) {
      const diff = bucketAccuracies[1] - bucketAccuracies[2];
      if (diff > 0.15) newThresholds.t2 = Math.min(currentThresholds.t2 + 2, currentThresholds.t3 - minDiff);
      else if (diff < -0.15) newThresholds.t2 = Math.max(currentThresholds.t2 - 2, newThresholds.t1 + minDiff);
    }
    if (bucketAccuracies[2] !== null && bucketAccuracies[3] !== null) {
      const diff = bucketAccuracies[2] - bucketAccuracies[3];
      if (diff > 0.15) newThresholds.t3 = Math.min(currentThresholds.t3 + 2, currentThresholds.t4 - minDiff);
      else if (diff < -0.15) newThresholds.t3 = Math.max(currentThresholds.t3 - 2, newThresholds.t2 + minDiff);
    }
    if (bucketAccuracies[3] !== null && bucketAccuracies[4] !== null) {
      const diff = bucketAccuracies[3] - bucketAccuracies[4];
      if (diff > 0.15) newThresholds.t4 = Math.min(currentThresholds.t4 + 2, 90);
      else if (diff < -0.15) newThresholds.t4 = Math.max(currentThresholds.t4 - 2, newThresholds.t3 + minDiff);
    }

    const thresholdUpdates = [
      { key: 'bucket_threshold_1', value: newThresholds.t1, old: currentThresholds.t1 },
      { key: 'bucket_threshold_2', value: newThresholds.t2, old: currentThresholds.t2 },
      { key: 'bucket_threshold_3', value: newThresholds.t3, old: currentThresholds.t3 },
      { key: 'bucket_threshold_4', value: newThresholds.t4, old: currentThresholds.t4 },
    ];

    for (const threshold of thresholdUpdates) {
      if (threshold.value !== threshold.old) {
        await env.DB.prepare(`
          UPDATE model_params SET param_value = ?, updated_at = datetime('now'),
          notes = ? WHERE param_key = ?
        `).bind(
          threshold.value,
          `Tuned from ${threshold.old} (bucket accuracies: ${bucketAccuracies.map((accuracy) => accuracy ? `${(accuracy * 100).toFixed(0)}%` : 'N/A').join(', ')})`,
          threshold.key,
        ).run();
      }
    }
  }

  return Response.json({
    success: true,
    predictions_analyzed: totalPredictions,
    periods_updated: periodsUpdated,
    overall_accuracy: `${(overallAccuracy * 100).toFixed(1)}%`,
    new_accuracy_weight: newAccuracyWeight,
    bucket_tuning: {
      samples_per_bucket: bucketStats.map((bucket) => bucket.total),
      accuracy_per_bucket: bucketAccuracies.map((accuracy) => accuracy !== null ? `${(accuracy * 100).toFixed(0)}%` : 'N/A'),
      old_thresholds: currentThresholds,
      new_thresholds: newThresholds,
      changed: JSON.stringify(currentThresholds) !== JSON.stringify(newThresholds),
    },
    top_periods: Object.entries(periodStats)
      .sort((left, right) => right[1].times_used - left[1].times_used)
      .slice(0, 5)
      .map(([date, stats]) => ({
        date,
        times_used: stats.times_used,
        accuracy_7d: stats.total_7d > 0 ? `${((stats.correct_7d / stats.total_7d) * 100).toFixed(0)}%` : 'N/A',
      })),
  }, { headers: corsHeaders });
}

async function handleModelRoute(route: WorkerRouteContext): Promise<Response> {
  const { env, corsHeaders } = route;

  const [params, periodAccuracy] = await Promise.all([
    env.DB.prepare(
      'SELECT param_key, param_value, updated_at, notes FROM model_params ORDER BY param_key'
    ).all<{ param_key: string; param_value: number; updated_at: string; notes: string }>(),
    env.DB.prepare(`
      SELECT period_date, accuracy_score, times_used, avg_error_7d
      FROM period_accuracy
      WHERE times_used >= 2
      ORDER BY accuracy_score DESC
      LIMIT 10
    `).all<{ period_date: string; accuracy_score: number; times_used: number; avg_error_7d: number }>(),
  ]);

  return Response.json({
    params: params.results,
    top_accurate_periods: periodAccuracy.results,
  }, { headers: corsHeaders });
}

async function handleBacktestRoute(route: WorkerRouteContext): Promise<Response> {
  const { env, url, corsHeaders } = route;

  const [pxiScores, spyPrices] = await Promise.all([
    env.DB.prepare(`
      SELECT date, score FROM pxi_scores ORDER BY date ASC
    `).all<{ date: string; score: number }>(),
    env.DB.prepare(`
      SELECT date, value FROM indicator_values
      WHERE indicator_id = 'spy_close'
      ORDER BY date ASC
    `).all<{ date: string; value: number }>(),
  ]);

  if (!spyPrices.results || spyPrices.results.length === 0) {
    return Response.json({
      error: 'No SPY price data available. Run /api/refresh to fetch SPY prices.',
      hint: 'POST /api/refresh with Authorization header',
    }, { status: 400, headers: corsHeaders });
  }

  const spyMap = buildPriceMap(spyPrices.results);
  const results = (pxiScores.results || []).map((pxi) => {
    const spyNow = getPriceOnOrAfter(spyMap, pxi.date);
    const date = new Date(pxi.date);
    const date7d = new Date(date);
    date7d.setDate(date7d.getDate() + 7);
    const date30d = new Date(date);
    date30d.setDate(date30d.getDate() + 30);
    const spy7d = getPriceOnOrAfter(spyMap, asDateKey(date7d));
    const spy30d = getPriceOnOrAfter(spyMap, asDateKey(date30d));
    const return7d = spyNow && spy7d ? ((spy7d - spyNow) / spyNow) * 100 : null;
    const return30d = spyNow && spy30d ? ((spy30d - spyNow) / spyNow) * 100 : null;

    let bucket = '80-100';
    if (pxi.score < 20) bucket = '0-20';
    else if (pxi.score < 40) bucket = '20-40';
    else if (pxi.score < 60) bucket = '40-60';
    else if (pxi.score < 80) bucket = '60-80';

    return {
      date: pxi.date,
      pxi_score: pxi.score,
      pxi_bucket: bucket,
      spy_price: spyNow,
      spy_7d: spy7d,
      spy_30d: spy30d,
      return_7d: return7d,
      return_30d: return30d,
    };
  });

  const bucketStats = ['0-20', '20-40', '40-60', '60-80', '80-100'].map((bucket) => {
    const bucketResults = results.filter((result) => result.pxi_bucket === bucket);
    const returns7d = bucketResults.map((result) => result.return_7d).filter((value): value is number => value !== null);
    const returns30d = bucketResults.map((result) => result.return_30d).filter((value): value is number => value !== null);

    return {
      bucket,
      count: bucketResults.length,
      avg_return_7d: calculateAverage(returns7d),
      avg_return_30d: calculateAverage(returns30d),
      win_rate_7d: calculateWinRate(returns7d),
      win_rate_30d: calculateWinRate(returns30d),
      median_return_7d: calculateMedian(returns7d),
      median_return_30d: calculateMedian(returns30d),
      min_return_7d: returns7d.length > 0 ? Math.min(...returns7d) : null,
      max_return_7d: returns7d.length > 0 ? Math.max(...returns7d) : null,
      min_return_30d: returns30d.length > 0 ? Math.min(...returns30d) : null,
      max_return_30d: returns30d.length > 0 ? Math.max(...returns30d) : null,
    };
  });

  const allReturns7d = results.map((result) => result.return_7d).filter((value): value is number => value !== null);
  const allReturns30d = results.map((result) => result.return_30d).filter((value): value is number => value !== null);
  const extremeLow = results.filter((result) => result.pxi_score < 25);
  const extremeHigh = results.filter((result) => result.pxi_score > 75);
  const extremeLowReturns7d = extremeLow.map((result) => result.return_7d).filter((value): value is number => value !== null);
  const extremeLowReturns30d = extremeLow.map((result) => result.return_30d).filter((value): value is number => value !== null);
  const extremeHighReturns7d = extremeHigh.map((result) => result.return_7d).filter((value): value is number => value !== null);
  const extremeHighReturns30d = extremeHigh.map((result) => result.return_30d).filter((value): value is number => value !== null);

  return Response.json({
    summary: {
      total_observations: results.length,
      with_7d_return: allReturns7d.length,
      with_30d_return: allReturns30d.length,
      date_range: {
        start: results[0]?.date,
        end: results[results.length - 1]?.date,
      },
      spy_data_points: spyPrices.results.length,
    },
    bucket_analysis: bucketStats,
    extreme_readings: {
      low_pxi: {
        threshold: '<25',
        count: extremeLow.length,
        avg_return_7d: calculateAverage(extremeLowReturns7d),
        avg_return_30d: calculateAverage(extremeLowReturns30d),
        win_rate_7d: calculateWinRate(extremeLowReturns7d),
        win_rate_30d: calculateWinRate(extremeLowReturns30d),
      },
      high_pxi: {
        threshold: '>75',
        count: extremeHigh.length,
        avg_return_7d: calculateAverage(extremeHighReturns7d),
        avg_return_30d: calculateAverage(extremeHighReturns30d),
        win_rate_7d: calculateWinRate(extremeHighReturns7d),
        win_rate_30d: calculateWinRate(extremeHighReturns30d),
      },
    },
    raw_data: url.searchParams.get('raw') === 'true' ? results : undefined,
  }, { headers: corsHeaders });
}

async function handleBacktestSignalRoute(route: WorkerRouteContext, deps: ModelingDeps): Promise<Response> {
  const { env, corsHeaders } = route;

  const [signals, spyPrices] = await Promise.all([
    env.DB.prepare(`
      SELECT date, pxi_level, risk_allocation, signal_type, regime
      FROM pxi_signal ORDER BY date ASC
    `).all<{ date: string; pxi_level: number; risk_allocation: number; signal_type: string; regime: string }>(),
    env.DB.prepare(`
      SELECT date, value FROM indicator_values
      WHERE indicator_id = 'spy_close'
      ORDER BY date ASC
    `).all<{ date: string; value: number }>(),
  ]);

  if (!signals.results || signals.results.length < 30) {
    return Response.json({
      error: 'Insufficient signal data. Run historical recalculation first.',
      signal_count: signals.results?.length || 0,
      hint: 'POST /api/recalculate-all-signals with Authorization header',
    }, { status: 400, headers: corsHeaders });
  }

  if (!spyPrices.results || spyPrices.results.length === 0) {
    return Response.json({
      error: 'No SPY price data available.',
      hint: 'POST /api/refresh to fetch SPY prices',
    }, { status: 400, headers: corsHeaders });
  }

  const spyMap = buildPriceMap(spyPrices.results);
  const spy200dma = new Map<string, number>();
  const spyDates = spyPrices.results.map((row) => row.date).sort();
  for (let index = 199; index < spyDates.length; index += 1) {
    const window = spyDates.slice(index - 199, index + 1);
    const average = window.reduce((sum, date) => sum + (spyMap.get(date) || 0), 0) / 200;
    spy200dma.set(spyDates[index], average);
  }

  const dailyReturns: Array<{
    date: string;
    spy_return: number;
    pxi_signal_return: number;
    dma200_return: number;
    buy_hold_return: number;
    allocation: number;
    signal_type: string;
  }> = [];

  let previousDate: string | null = null;
  for (const signal of signals.results) {
    if (!previousDate) {
      previousDate = signal.date;
      continue;
    }

    const prevPrice = spyMap.get(previousDate);
    const currPrice = spyMap.get(signal.date);

    if (prevPrice && currPrice) {
      const dailyReturn = (currPrice - prevPrice) / prevPrice;
      const prevDma = spy200dma.get(previousDate);
      const isDmaRiskOn = prevDma ? prevPrice > prevDma : true;
      dailyReturns.push({
        date: signal.date,
        spy_return: dailyReturn,
        pxi_signal_return: dailyReturn * signal.risk_allocation,
        dma200_return: isDmaRiskOn ? dailyReturn : 0,
        buy_hold_return: dailyReturn,
        allocation: signal.risk_allocation,
        signal_type: signal.signal_type,
      });
    }

    previousDate = signal.date;
  }

  const calculateMetrics = (returns: number[], name: string) => {
    if (returns.length === 0) return null;

    let cumulative = 1;
    let peak = 1;
    let maxDrawdown = 0;
    for (const value of returns) {
      cumulative *= (1 + value);
      if (cumulative > peak) peak = cumulative;
      const drawdown = (peak - cumulative) / peak;
      if (drawdown > maxDrawdown) maxDrawdown = drawdown;
    }

    const years = returns.length / 252;
    const cagr = years > 0 ? Math.pow(cumulative, 1 / years) - 1 : 0;
    const avgReturn = returns.reduce((sum, value) => sum + value, 0) / returns.length;
    const variance = returns.reduce((sum, value) => sum + Math.pow(value - avgReturn, 2), 0) / returns.length;
    const dailyVol = Math.sqrt(variance);
    const annualVol = dailyVol * Math.sqrt(252);
    const sharpe = annualVol > 0 ? cagr / annualVol : 0;
    const winRate = (returns.filter((value) => value > 0).length / returns.length) * 100;

    return {
      strategy: name,
      total_return_pct: (cumulative - 1) * 100,
      cagr_pct: cagr * 100,
      volatility_pct: annualVol * 100,
      sharpe_ratio: Math.round(sharpe * 100) / 100,
      max_drawdown_pct: maxDrawdown * 100,
      win_rate_pct: winRate,
      trading_days: returns.length,
    };
  };

  const pxiSignalMetrics = calculateMetrics(dailyReturns.map((row) => row.pxi_signal_return), 'PXI-Signal');
  const dma200Metrics = calculateMetrics(dailyReturns.map((row) => row.dma200_return), '200DMA');
  const buyHoldMetrics = calculateMetrics(dailyReturns.map((row) => row.buy_hold_return), 'Buy-and-Hold');

  const signalDistribution: Record<string, number> = {};
  const avgAllocationBySignal: Record<string, number> = {};
  const signalReturns: Record<string, number[]> = {};

  for (const row of dailyReturns) {
    signalDistribution[row.signal_type] = (signalDistribution[row.signal_type] || 0) + 1;
    if (!avgAllocationBySignal[row.signal_type]) {
      avgAllocationBySignal[row.signal_type] = 0;
      signalReturns[row.signal_type] = [];
    }
    avgAllocationBySignal[row.signal_type] += row.allocation;
    signalReturns[row.signal_type].push(row.pxi_signal_return);
  }

  for (const signalType of Object.keys(avgAllocationBySignal)) {
    avgAllocationBySignal[signalType] = avgAllocationBySignal[signalType] / signalDistribution[signalType];
  }

  const runDate = deps.formatDate(new Date());
  if (pxiSignalMetrics) {
    await env.DB.prepare(`
      INSERT OR REPLACE INTO backtest_results
      (run_date, strategy, lookback_start, lookback_end, cagr, volatility, sharpe, max_drawdown, total_trades, win_rate, baseline_comparison)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      runDate,
      'PXI-Signal',
      dailyReturns[0]?.date || '',
      dailyReturns[dailyReturns.length - 1]?.date || '',
      pxiSignalMetrics.cagr_pct,
      pxiSignalMetrics.volatility_pct,
      pxiSignalMetrics.sharpe_ratio,
      pxiSignalMetrics.max_drawdown_pct,
      dailyReturns.length,
      pxiSignalMetrics.win_rate_pct,
      JSON.stringify({
        vs_buy_hold: buyHoldMetrics ? {
          cagr_diff: pxiSignalMetrics.cagr_pct - buyHoldMetrics.cagr_pct,
          vol_diff: pxiSignalMetrics.volatility_pct - buyHoldMetrics.volatility_pct,
          sharpe_diff: pxiSignalMetrics.sharpe_ratio - buyHoldMetrics.sharpe_ratio,
        } : null,
        vs_200dma: dma200Metrics ? {
          cagr_diff: pxiSignalMetrics.cagr_pct - dma200Metrics.cagr_pct,
          vol_diff: pxiSignalMetrics.volatility_pct - dma200Metrics.volatility_pct,
          sharpe_diff: pxiSignalMetrics.sharpe_ratio - dma200Metrics.sharpe_ratio,
        } : null,
      }),
    ).run();
  }

  return Response.json({
    summary: {
      date_range: {
        start: dailyReturns[0]?.date,
        end: dailyReturns[dailyReturns.length - 1]?.date,
      },
      trading_days: dailyReturns.length,
      signal_data_points: signals.results.length,
    },
    strategies: {
      pxi_signal: pxiSignalMetrics,
      dma_200: dma200Metrics,
      buy_and_hold: buyHoldMetrics,
    },
    signal_analysis: {
      distribution: signalDistribution,
      avg_allocation_by_signal: avgAllocationBySignal,
    },
    comparison: {
      pxi_vs_buy_hold: pxiSignalMetrics && buyHoldMetrics ? {
        cagr_advantage: Math.round((pxiSignalMetrics.cagr_pct - buyHoldMetrics.cagr_pct) * 100) / 100,
        volatility_reduction: Math.round((buyHoldMetrics.volatility_pct - pxiSignalMetrics.volatility_pct) * 100) / 100,
        sharpe_improvement: Math.round((pxiSignalMetrics.sharpe_ratio - buyHoldMetrics.sharpe_ratio) * 100) / 100,
        max_dd_improvement: Math.round((buyHoldMetrics.max_drawdown_pct - pxiSignalMetrics.max_drawdown_pct) * 100) / 100,
      } : null,
      pxi_vs_200dma: pxiSignalMetrics && dma200Metrics ? {
        cagr_advantage: Math.round((pxiSignalMetrics.cagr_pct - dma200Metrics.cagr_pct) * 100) / 100,
        volatility_diff: Math.round((pxiSignalMetrics.volatility_pct - dma200Metrics.volatility_pct) * 100) / 100,
        sharpe_improvement: Math.round((pxiSignalMetrics.sharpe_ratio - dma200Metrics.sharpe_ratio) * 100) / 100,
      } : null,
    },
  }, { headers: corsHeaders });
}

async function handleBacktestHistoryRoute(route: WorkerRouteContext): Promise<Response> {
  const { env, corsHeaders } = route;
  const results = await env.DB.prepare(`
    SELECT * FROM backtest_results ORDER BY run_date DESC LIMIT 10
  `).all();

  return Response.json({
    history: results.results || [],
  }, { headers: corsHeaders });
}

async function handleExportHistoryRoute(route: WorkerRouteContext): Promise<Response> {
  const { env, url, corsHeaders } = route;
  const days = Math.min(730, Math.max(7, parseInt(url.searchParams.get('days') || '365', 10)));
  const format = url.searchParams.get('format') || 'csv';

  const historyResult = await env.DB.prepare(`
    SELECT
      p.date,
      p.score,
      p.label,
      p.status,
      p.delta_1d,
      p.delta_7d,
      p.delta_30d
    FROM pxi_scores p
    ORDER BY p.date DESC
    LIMIT ?
  `).bind(days).all<{
    date: string;
    score: number;
    label: string;
    status: string;
    delta_1d: number | null;
    delta_7d: number | null;
    delta_30d: number | null;
  }>();

  const data = (historyResult.results || []).reverse();
  if (format === 'csv') {
    const headers = ['date', 'score', 'label', 'status', 'delta_1d', 'delta_7d', 'delta_30d'];
    const rows = [headers.join(',')];
    for (const row of data) {
      rows.push([
        row.date,
        row.score.toFixed(2),
        row.label,
        row.status,
        row.delta_1d?.toFixed(2) ?? '',
        row.delta_7d?.toFixed(2) ?? '',
        row.delta_30d?.toFixed(2) ?? '',
      ].join(','));
    }

    return new Response(rows.join('\n'), {
      headers: {
        ...corsHeaders,
        'Content-Type': 'text/csv',
        'Content-Disposition': `attachment; filename="pxi-history-${asDateKey(new Date())}.csv"`,
      },
    });
  }

  return Response.json({
    data,
    count: data.length,
    exported_at: new Date().toISOString(),
  }, { headers: corsHeaders });
}

async function handleExportTrainingDataRoute(route: WorkerRouteContext, deps: ModelingDeps): Promise<Response> {
  const { request, env, corsHeaders, clientIP } = route;

  const adminAuthFailure = await deps.enforceAdminAuth(request, env, corsHeaders, clientIP);
  if (adminAuthFailure) {
    return adminAuthFailure;
  }

  const [pxiData, spyPrices, categoryData, indicatorData] = await Promise.all([
    env.DB.prepare(`
      SELECT
        p.date,
        p.score,
        p.delta_1d,
        p.delta_7d,
        p.delta_30d,
        p.label,
        p.status
      FROM pxi_scores p
      ORDER BY p.date ASC
    `).all<{
      date: string;
      score: number;
      delta_1d: number | null;
      delta_7d: number | null;
      delta_30d: number | null;
      label: string;
      status: string;
    }>(),
    env.DB.prepare(`
      SELECT date, value FROM indicator_values
      WHERE indicator_id = 'spy_close'
      ORDER BY date ASC
    `).all<{ date: string; value: number }>(),
    env.DB.prepare(`
      SELECT date, category, score FROM category_scores ORDER BY date, category
    `).all<{ date: string; category: string; score: number }>(),
    env.DB.prepare(`
      SELECT date, indicator_id, value FROM indicator_values ORDER BY date, indicator_id
    `).all<{ date: string; indicator_id: string; value: number }>(),
  ]);

  const spyMap = buildPriceMap(spyPrices.results || []);
  const categoryMap = new Map<string, Record<string, number>>();
  const indicatorMap = new Map<string, Record<string, number>>();

  for (const row of categoryData.results || []) {
    if (!categoryMap.has(row.date)) categoryMap.set(row.date, {});
    categoryMap.get(row.date)![row.category] = row.score;
  }
  for (const row of indicatorData.results || []) {
    if (!indicatorMap.has(row.date)) indicatorMap.set(row.date, {});
    indicatorMap.get(row.date)![row.indicator_id] = row.value;
  }

  const trainingData = (pxiData.results || []).map((pxi) => {
    const spyNow = getPriceOnOrAfter(spyMap, pxi.date);
    let spyReturn7d: number | null = null;
    let spyReturn30d: number | null = null;

    if (spyNow !== null) {
      const baseDate = new Date(pxi.date);
      const date7d = new Date(baseDate);
      date7d.setDate(date7d.getDate() + 7);
      const spy7d = getPriceOnOrAfter(spyMap, asDateKey(date7d));
      if (spy7d !== null) {
        spyReturn7d = ((spy7d - spyNow) / spyNow) * 100;
      }

      const date30d = new Date(baseDate);
      date30d.setDate(date30d.getDate() + 30);
      const spy30d = getPriceOnOrAfter(spyMap, asDateKey(date30d));
      if (spy30d !== null) {
        spyReturn30d = ((spy30d - spyNow) / spyNow) * 100;
      }
    }

    return {
      date: pxi.date,
      spy_return_7d: spyReturn7d,
      spy_return_30d: spyReturn30d,
      pxi_score: pxi.score,
      pxi_delta_1d: pxi.delta_1d,
      pxi_delta_7d: pxi.delta_7d,
      pxi_delta_30d: pxi.delta_30d,
      pxi_label: pxi.label,
      pxi_status: pxi.status,
      categories: categoryMap.get(pxi.date) || {},
      indicators: indicatorMap.get(pxi.date) || {},
    };
  });

  return Response.json({
    count: trainingData.length,
    spy_data_points: spyPrices.results?.length || 0,
    data: trainingData,
  }, { headers: corsHeaders });
}

export async function tryHandleModelingRoute(
  route: WorkerRouteContext,
  deps: ModelingDeps,
): Promise<Response | null> {
  const { url, method } = route;

  if (url.pathname === '/api/predict' && method === 'GET') {
    return handlePredictRoute(route);
  }

  if (url.pathname === '/api/evaluate' && method === 'POST') {
    return handleEvaluateRoute(route, deps);
  }

  if (url.pathname === '/api/ml/predict' && method === 'GET') {
    return handleMlPredictRoute(route, deps);
  }

  if (url.pathname === '/api/ml/lstm' && method === 'GET') {
    return handleMlLstmRoute(route, deps);
  }

  if (url.pathname === '/api/predict/returns' && method === 'GET') {
    return handlePredictReturnsRoute(route, deps);
  }

  if (url.pathname === '/api/ml/ensemble' && method === 'GET') {
    return handleMlEnsembleRoute(route, deps);
  }

  if (url.pathname === '/api/accuracy' && method === 'GET') {
    return handleAccuracyRoute(route, deps);
  }

  if (url.pathname === '/api/ml/accuracy' && method === 'GET') {
    return handleMlAccuracyRoute(route, deps);
  }

  if (url.pathname === '/api/ml/backtest' && method === 'GET') {
    return handleMlBacktestRoute(route, deps);
  }

  if (url.pathname === '/api/retrain' && method === 'POST') {
    return handleRetrainRoute(route, deps);
  }

  if (url.pathname === '/api/model' && method === 'GET') {
    return handleModelRoute(route);
  }

  if (url.pathname === '/api/backtest' && method === 'GET') {
    return handleBacktestRoute(route);
  }

  if (url.pathname === '/api/backtest/signal' && method === 'GET') {
    return handleBacktestSignalRoute(route, deps);
  }

  if (url.pathname === '/api/backtest/history' && method === 'GET') {
    return handleBacktestHistoryRoute(route);
  }

  if (url.pathname === '/api/export/history' && method === 'GET') {
    return handleExportHistoryRoute(route);
  }

  if (url.pathname === '/api/export/training-data' && method === 'GET') {
    return handleExportTrainingDataRoute(route, deps);
  }

  return null;
}
