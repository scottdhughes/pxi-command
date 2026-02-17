// Cloudflare Worker API for PXI
// Uses D1 (SQLite), Vectorize, and Workers AI
// Includes scheduled cron handler for daily data refresh

import { getStaleThresholdDays } from '../src/config/indicator-sla';

interface Env {
  DB: D1Database;
  VECTORIZE: VectorizeIndex;
  AI: Ai;
  ML_MODELS: KVNamespace;
  RATE_LIMIT_KV?: KVNamespace;
  FRED_API_KEY?: string;
  WRITE_API_KEY?: string;
  RESEND_API_KEY?: string;
  RESEND_FROM_EMAIL?: string;
  ALERTS_SIGNING_SECRET?: string;
  FEATURE_ENABLE_BRIEF?: string;
  FEATURE_ENABLE_OPPORTUNITIES?: string;
  FEATURE_ENABLE_PLAN?: string;
  FEATURE_ENABLE_ALERTS_EMAIL?: string;
  FEATURE_ENABLE_ALERTS_IN_APP?: string;
  ENABLE_BRIEF?: string;
  ENABLE_OPPORTUNITIES?: string;
  ENABLE_PLAN?: string;
  ENABLE_ALERTS_EMAIL?: string;
  ENABLE_ALERTS_IN_APP?: string;
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

// Generate rich embedding text with engineered features
interface EmbeddingContext {
  indicators: { indicator_id: string; value: number }[];
  pxi?: { score: number; delta_7d: number | null; delta_30d: number | null };
  categories?: { category: string; score: number }[];
}

function generateEmbeddingText(ctx: EmbeddingContext): string {
  const parts: string[] = [];

  // 1. Raw indicator values (base features)
  if (ctx.indicators.length > 0) {
    const indicatorText = ctx.indicators
      .map(i => `${i.indicator_id}: ${i.value.toFixed(2)}`)
      .join(', ');
    parts.push(`indicators: ${indicatorText}`);
  }

  // 2. PXI score bucket and momentum (engineered features)
  if (ctx.pxi) {
    // Score bucket (0-20, 20-40, 40-60, 60-80, 80-100)
    const bucket = ctx.pxi.score < 20 ? 'very_low' :
                   ctx.pxi.score < 40 ? 'low' :
                   ctx.pxi.score < 60 ? 'neutral' :
                   ctx.pxi.score < 80 ? 'high' : 'very_high';
    parts.push(`pxi_bucket: ${bucket}`);
    parts.push(`pxi_score: ${ctx.pxi.score.toFixed(1)}`);

    // Momentum signals
    if (ctx.pxi.delta_7d !== null) {
      const momentum7d = ctx.pxi.delta_7d > 5 ? 'strong_up' :
                         ctx.pxi.delta_7d > 2 ? 'up' :
                         ctx.pxi.delta_7d > -2 ? 'flat' :
                         ctx.pxi.delta_7d > -5 ? 'down' : 'strong_down';
      parts.push(`momentum_7d: ${momentum7d} (${ctx.pxi.delta_7d.toFixed(1)})`);
    }

    if (ctx.pxi.delta_30d !== null) {
      const momentum30d = ctx.pxi.delta_30d > 10 ? 'strong_up' :
                          ctx.pxi.delta_30d > 4 ? 'up' :
                          ctx.pxi.delta_30d > -4 ? 'flat' :
                          ctx.pxi.delta_30d > -10 ? 'down' : 'strong_down';
      parts.push(`momentum_30d: ${momentum30d} (${ctx.pxi.delta_30d.toFixed(1)})`);
    }

    // Rate of change acceleration (7d vs 30d normalized)
    if (ctx.pxi.delta_7d !== null && ctx.pxi.delta_30d !== null) {
      const weeklyRate = ctx.pxi.delta_7d;
      const monthlyWeeklyRate = ctx.pxi.delta_30d / 4.3; // Normalize to weekly
      const acceleration = weeklyRate - monthlyWeeklyRate;
      const accelSignal = acceleration > 2 ? 'accelerating' :
                          acceleration < -2 ? 'decelerating' : 'steady';
      parts.push(`acceleration: ${accelSignal}`);
    }
  }

  // 3. Category dispersion and extremes
  if (ctx.categories && ctx.categories.length > 0) {
    const scores = ctx.categories.map(c => c.score);
    const maxScore = Math.max(...scores);
    const minScore = Math.min(...scores);
    const dispersion = maxScore - minScore;

    // Dispersion signal (high dispersion = mixed signals)
    const dispersionSignal = dispersion > 40 ? 'high_dispersion' :
                             dispersion > 20 ? 'moderate_dispersion' : 'low_dispersion';
    parts.push(`category_dispersion: ${dispersionSignal} (${dispersion.toFixed(0)})`);

    // Identify extreme categories
    const extremeHigh = ctx.categories.filter(c => c.score > 70).map(c => c.category);
    const extremeLow = ctx.categories.filter(c => c.score < 30).map(c => c.category);

    if (extremeHigh.length > 0) {
      parts.push(`strong_categories: ${extremeHigh.join(', ')}`);
    }
    if (extremeLow.length > 0) {
      parts.push(`weak_categories: ${extremeLow.join(', ')}`);
    }

    // Category scores
    const catText = ctx.categories
      .map(c => `${c.category}: ${c.score.toFixed(0)}`)
      .join(', ');
    parts.push(`categories: ${catText}`);
  }

  // 4. Extract volatility regime from indicators if available
  const vixIndicator = ctx.indicators.find(i => i.indicator_id === 'vix' || i.indicator_id === 'VIX');
  if (vixIndicator) {
    const volRegime = vixIndicator.value < 15 ? 'low_vol' :
                      vixIndicator.value < 20 ? 'normal_vol' :
                      vixIndicator.value < 30 ? 'elevated_vol' : 'high_vol';
    parts.push(`vol_regime: ${volRegime}`);
  }

  return parts.join(' | ');
}

// ============== XGBoost Inference ==============

interface XGBTreeNode {
  nodeid: number;
  depth?: number;
  split?: string;
  split_condition?: number;
  yes?: number;
  no?: number;
  missing?: number;
  leaf?: number;
  children?: XGBTreeNode[];
}

interface XGBModel {
  b: number; // base_score
  t: XGBTreeNode[]; // trees
}

interface MLModel {
  v: string;
  f: string[]; // feature_names
  m: {
    '7d': XGBModel;
    '30d': XGBModel;
  };
}

// Cache for loaded model
let cachedModel: MLModel | null = null;
let modelLoadTime = 0;
const MODEL_CACHE_TTL = 3600000; // 1 hour

async function loadMLModel(kv: KVNamespace): Promise<MLModel | null> {
  const now = Date.now();
  if (cachedModel && (now - modelLoadTime) < MODEL_CACHE_TTL) {
    return cachedModel;
  }

  try {
    const modelJson = await kv.get('pxi_model', 'json');
    if (modelJson) {
      cachedModel = modelJson as MLModel;
      modelLoadTime = now;
      return cachedModel;
    }
  } catch (e) {
    console.error('Failed to load ML model from KV:', e);
  }
  return null;
}

// Traverse XGBoost tree to get prediction
function traverseTree(node: XGBTreeNode, features: Record<string, number>, featureNames: string[]): number {
  // Leaf node
  if (node.leaf !== undefined) {
    return node.leaf;
  }

  // Get feature value - handle both indexed (f0, f1) and named features
  let featureValue = 0;
  const splitName = node.split!;

  if (splitName.startsWith('f')) {
    // Indexed feature name (e.g., "f9") - convert to actual name
    const featureIndex = parseInt(splitName.slice(1), 10);
    if (featureIndex >= 0 && featureIndex < featureNames.length) {
      const actualName = featureNames[featureIndex];
      featureValue = features[actualName] ?? 0;
    }
  } else {
    // Named feature
    featureValue = features[splitName] ?? 0;
  }

  // Decision: go left (yes) if value < split_condition, else right (no)
  // Missing values go to the 'missing' branch (usually yes)
  const goLeft = featureValue < node.split_condition!;

  // Find the child node
  const targetNodeId = goLeft ? node.yes : node.no;

  // Children are in the 'children' array
  if (node.children) {
    const child = node.children.find(c => c.nodeid === targetNodeId);
    if (child) {
      return traverseTree(child, features, featureNames);
    }
  }

  // Fallback: return 0 if structure is unexpected
  return 0;
}

// XGBoost prediction: sum of all tree predictions + base score
function xgbPredict(model: XGBModel, features: Record<string, number>, featureNames?: string[]): number {
  let prediction = model.b; // base score

  const names = featureNames || [];
  for (const tree of model.t) {
    prediction += traverseTree(tree, features, names);
  }

  return prediction;
}

// Extract features matching the Python training script
interface MLFeatures {
  pxi_score: number;
  pxi_delta_1d: number | null;
  pxi_delta_7d: number | null;
  pxi_delta_30d: number | null;
  categories: Record<string, number>;
  indicators: Record<string, number>;
  pxi_ma_5?: number;
  pxi_ma_20?: number;
  pxi_std_20?: number;
}

function extractMLFeatures(data: MLFeatures): Record<string, number> {
  const features: Record<string, number> = {};

  // PXI features
  features['pxi_score'] = data.pxi_score;
  features['pxi_delta_1d'] = data.pxi_delta_1d ?? 0;
  features['pxi_delta_7d'] = data.pxi_delta_7d ?? 0;
  features['pxi_delta_30d'] = data.pxi_delta_30d ?? 0;

  // Momentum signals
  const d7 = data.pxi_delta_7d ?? 0;
  features['momentum_7d_signal'] = d7 > 5 ? 2 : d7 > 2 ? 1 : d7 > -2 ? 0 : d7 > -5 ? -1 : -2;

  const d30 = data.pxi_delta_30d ?? 0;
  features['momentum_30d_signal'] = d30 > 10 ? 2 : d30 > 4 ? 1 : d30 > -4 ? 0 : d30 > -10 ? -1 : -2;

  // Acceleration
  features['acceleration'] = d7 - (d30 / 4.3);
  features['acceleration_signal'] = features['acceleration'] > 2 ? 1 : features['acceleration'] < -2 ? -1 : 0;

  // Category features
  const catScores: number[] = [];
  for (const cat of ['breadth', 'credit', 'crypto', 'global', 'liquidity', 'macro', 'positioning', 'volatility']) {
    const score = data.categories[cat] ?? 0;
    features[`cat_${cat}`] = score;
    if (score > 0) catScores.push(score);
  }

  // Category dispersion and mean
  if (catScores.length > 0) {
    features['category_dispersion'] = Math.max(...catScores) - Math.min(...catScores);
    const mean = catScores.reduce((a, b) => a + b, 0) / catScores.length;
    features['category_std'] = Math.sqrt(catScores.reduce((sum, s) => sum + Math.pow(s - mean, 2), 0) / catScores.length);
    features['category_mean'] = mean;
  } else {
    features['category_dispersion'] = 0;
    features['category_std'] = 0;
    features['category_mean'] = 50;
  }

  // Extreme category counts (use both naming conventions for compatibility)
  features['strong_categories'] = catScores.filter(s => s > 70).length;
  features['weak_categories'] = catScores.filter(s => s < 30).length;
  features['strong_categories_count'] = features['strong_categories'];
  features['weak_categories_count'] = features['weak_categories'];

  // Indicator features
  features['vix'] = data.indicators['vix'] ?? 0;
  features['hy_spread'] = data.indicators['hy_oas'] ?? data.indicators['hy_oas_spread'] ?? 0;
  features['ig_spread'] = data.indicators['ig_oas'] ?? data.indicators['ig_oas_spread'] ?? 0;
  features['breadth_ratio'] = data.indicators['rsp_spy_ratio'] ?? 0;
  features['yield_curve'] = data.indicators['yield_curve_2s10s'] ?? 0;
  features['dxy'] = data.indicators['dxy'] ?? 0;
  features['btc_vs_200d'] = data.indicators['btc_vs_200d'] ?? 0;

  // Derived features (from rolling windows - use provided or estimate)
  features['pxi_ma_5'] = data.pxi_ma_5 ?? data.pxi_score;
  features['pxi_ma_20'] = data.pxi_ma_20 ?? data.pxi_score;
  features['pxi_std_20'] = data.pxi_std_20 ?? 10;
  features['pxi_vs_ma_20'] = data.pxi_score - features['pxi_ma_20'];

  // Binary and bucket features
  features['above_50'] = data.pxi_score > 50 ? 1 : 0;
  features['extreme_low'] = data.pxi_score < 25 ? 1 : 0;

  // PXI bucket (numeric: 0=very_low, 1=low, 2=neutral, 3=high, 4=very_high)
  features['pxi_bucket'] = data.pxi_score < 20 ? 0 : data.pxi_score < 40 ? 1 : data.pxi_score < 60 ? 2 : data.pxi_score < 80 ? 3 : 4;

  // VIX features
  const vix = features['vix'];
  features['vix_high'] = vix > 25 ? 1 : 0;
  features['vix_low'] = vix < 15 ? 1 : 0;
  features['vix_ma_20'] = vix; // Approximate
  features['vix_vs_ma'] = 0;
  features['extreme_high'] = data.pxi_score > 75 ? 1 : 0;

  return features;
}

// ============== LSTM Inference ==============

interface LSTMWeights {
  weight_ih: number[][];  // (4*hidden, input)
  weight_hh: number[][];  // (4*hidden, hidden)
  bias_ih: number[];      // (4*hidden)
  bias_hh: number[];      // (4*hidden)
}

interface LSTMModel {
  v: string;  // version
  c: {
    s: number;  // sequence_length
    h: number;  // hidden_size
    f: string[]; // feature_names
  };
  n: Record<string, { mean: number; std: number }>;  // normalization
  m: {
    '7d': { lstm: LSTMWeights; fc: { weight: number[][]; bias: number[] } };
    '30d': { lstm: LSTMWeights; fc: { weight: number[][]; bias: number[] } };
  };
}

// Cache for LSTM model
let cachedLSTM: LSTMModel | null = null;
let lstmLoadTime = 0;

async function loadLSTMModel(kv: KVNamespace): Promise<LSTMModel | null> {
  const now = Date.now();
  if (cachedLSTM && (now - lstmLoadTime) < MODEL_CACHE_TTL) {
    return cachedLSTM;
  }

  try {
    const modelJson = await kv.get('pxi_lstm', 'json');
    if (modelJson) {
      cachedLSTM = modelJson as LSTMModel;
      lstmLoadTime = now;
      return cachedLSTM;
    }
  } catch (e) {
    console.error('Failed to load LSTM model from KV:', e);
  }
  return null;
}

// SPY Return Prediction Model (predicts actual market returns, not PXI changes)
interface SPYReturnModel {
  created_at: string;
  model_type: string;
  feature_names: string[];
  models: {
    '7d': {
      type: string;
      version: string;
      n_estimators: number;
      base_score: number;
      feature_names: string[];
      trees: XGBTreeNode[];
    };
    '30d': {
      type: string;
      version: string;
      n_estimators: number;
      base_score: number;
      feature_names: string[];
      trees: XGBTreeNode[];
    };
  };
  metrics: {
    '7d': { cv_direction_acc: number; cv_mae_mean: number; cv_r2_mean: number };
    '30d': { cv_direction_acc: number; cv_mae_mean: number; cv_r2_mean: number };
  };
}

let cachedSPYModel: SPYReturnModel | null = null;
let spyModelLoadTime = 0;

async function loadSPYReturnModel(kv: KVNamespace): Promise<SPYReturnModel | null> {
  const now = Date.now();
  if (cachedSPYModel && (now - spyModelLoadTime) < MODEL_CACHE_TTL) {
    return cachedSPYModel;
  }

  try {
    const modelJson = await kv.get('spy_return_model', 'json');
    if (modelJson) {
      cachedSPYModel = modelJson as SPYReturnModel;
      spyModelLoadTime = now;
      return cachedSPYModel;
    }
  } catch (e) {
    console.error('Failed to load SPY return model from KV:', e);
  }
  return null;
}

// Predict SPY returns using XGBoost model
function predictSPYReturn(
  model: SPYReturnModel,
  features: Record<string, number>,
  horizon: '7d' | '30d'
): number {
  const horizonModel = model.models[horizon];
  const featureNames = horizonModel.feature_names;

  let prediction = horizonModel.base_score;
  for (const tree of horizonModel.trees) {
    prediction += traverseTree(tree, features, featureNames);
  }

  return prediction;
}

// Sigmoid activation
function sigmoid(x: number): number {
  return 1 / (1 + Math.exp(-Math.max(-500, Math.min(500, x))));
}

// Tanh activation
function tanh(x: number): number {
  const ex = Math.exp(2 * Math.max(-500, Math.min(500, x)));
  return (ex - 1) / (ex + 1);
}

// Matrix-vector multiplication
function matVec(mat: number[][], vec: number[]): number[] {
  return mat.map(row => row.reduce((sum, val, i) => sum + val * vec[i], 0));
}

// Vector addition
function vecAdd(a: number[], b: number[]): number[] {
  return a.map((v, i) => v + b[i]);
}

// LSTM cell forward pass
function lstmCell(
  x: number[],
  h_prev: number[],
  c_prev: number[],
  weights: LSTMWeights
): { h: number[]; c: number[] } {
  const hiddenSize = h_prev.length;

  // Compute input and hidden contributions
  const ih = matVec(weights.weight_ih, x);
  const hh = matVec(weights.weight_hh, h_prev);

  // Add biases and combine
  const gates = vecAdd(vecAdd(ih, hh), vecAdd(weights.bias_ih, weights.bias_hh));

  // Split into gates (PyTorch order: i, f, g, o)
  const i_gate = gates.slice(0, hiddenSize).map(sigmoid);
  const f_gate = gates.slice(hiddenSize, 2 * hiddenSize).map(sigmoid);
  const g_gate = gates.slice(2 * hiddenSize, 3 * hiddenSize).map(tanh);
  const o_gate = gates.slice(3 * hiddenSize, 4 * hiddenSize).map(sigmoid);

  // Update cell state: c = f * c_prev + i * g
  const c = f_gate.map((f, idx) => f * c_prev[idx] + i_gate[idx] * g_gate[idx]);

  // Update hidden state: h = o * tanh(c)
  const h = o_gate.map((o, idx) => o * tanh(c[idx]));

  return { h, c };
}

// LSTM forward pass over sequence
function lstmForward(
  sequence: number[][],  // (seq_len, input_size)
  lstm: LSTMWeights,
  fc: { weight: number[][]; bias: number[] },
  hiddenSize: number
): number {
  // Initialize hidden and cell states to zeros
  let h = new Array(hiddenSize).fill(0);
  let c = new Array(hiddenSize).fill(0);

  // Process each timestep
  for (const x of sequence) {
    const result = lstmCell(x, h, c, lstm);
    h = result.h;
    c = result.c;
  }

  // Final linear layer: output = W * h + b
  const output = fc.weight[0].reduce((sum, w, i) => sum + w * h[i], 0) + fc.bias[0];

  return output;
}

// Extract and normalize features for LSTM
function extractLSTMFeatures(
  data: { pxi_score: number; pxi_delta_7d: number | null; categories: Record<string, number>; vix?: number },
  norm: Record<string, { mean: number; std: number }>,
  featureNames: string[]
): number[] {
  const features: Record<string, number> = {
    pxi_score: data.pxi_score,
    pxi_delta_7d: data.pxi_delta_7d ?? 0,
    cat_breadth: data.categories['breadth'] ?? 0,
    cat_credit: data.categories['credit'] ?? 0,
    cat_crypto: data.categories['crypto'] ?? 0,
    cat_global: data.categories['global'] ?? 0,
    cat_liquidity: data.categories['liquidity'] ?? 0,
    cat_macro: data.categories['macro'] ?? 0,
    cat_positioning: data.categories['positioning'] ?? 0,
    cat_volatility: data.categories['volatility'] ?? 0,
    vix: data.vix ?? 20,
  };

  // Category dispersion
  const catScores = Object.entries(features)
    .filter(([k]) => k.startsWith('cat_'))
    .map(([, v]) => v);
  features['category_dispersion'] = catScores.length > 0
    ? Math.max(...catScores) - Math.min(...catScores)
    : 0;

  // Normalize and return in correct order
  return featureNames.map(name => {
    const value = features[name] ?? 0;
    const params = norm[name];
    if (params && params.std > 0) {
      return (value - params.mean) / params.std;
    }
    return value;
  });
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
    { ticker: 'SPY', id: 'spy_close' },  // For backtesting forward returns
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

  // Small cap strength (IWM vs SPY)
  try {
    const [iwm, spy] = await Promise.all([
      fetchYahooSeries('IWM', 'iwm_temp'),
      fetchYahooSeries('SPY', 'spy_temp2'),
    ]);
    const spyMap = new Map(spy.map(s => [s.date, s.value]));
    for (const i of iwm) {
      const s = spyMap.get(i.date);
      if (s) {
        all.push({ indicator_id: 'small_cap_strength', date: i.date, value: (i.value / s) * 100, source: 'yahoo' });
      }
    }
    console.log('Small cap strength: calculated');
  } catch (e) {
    console.error('Small cap strength failed:', e);
  }

  // Mid cap strength (IJH vs SPY)
  try {
    const [ijh, spy] = await Promise.all([
      fetchYahooSeries('IJH', 'ijh_temp'),
      fetchYahooSeries('SPY', 'spy_temp3'),
    ]);
    const spyMap = new Map(spy.map(s => [s.date, s.value]));
    for (const i of ijh) {
      const s = spyMap.get(i.date);
      if (s) {
        all.push({ indicator_id: 'midcap_strength', date: i.date, value: (i.value / s) * 100, source: 'yahoo' });
      }
    }
    console.log('Midcap strength: calculated');
  } catch (e) {
    console.error('Midcap strength failed:', e);
  }

  // Sector breadth (% of sector ETFs above their 50-day MA)
  try {
    const sectorETFs = ['XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY'];
    const sectorData = await Promise.all(sectorETFs.map(t => fetchYahooSeries(t, t.toLowerCase())));

    // Get all unique dates across all sectors
    const allDates = new Set<string>();
    for (const data of sectorData) {
      for (const d of data) allDates.add(d.date);
    }

    // For each date, calculate % of sectors above 50-day MA
    const sectorMaps = sectorData.map(data => new Map(data.map(d => [d.date, d.value])));
    const sortedDates = [...allDates].sort();

    for (let i = 50; i < sortedDates.length; i++) {
      const date = sortedDates[i];
      let above = 0;
      let total = 0;

      for (let s = 0; s < sectorData.length; s++) {
        const currentVal = sectorMaps[s].get(date);
        if (currentVal === undefined) continue;

        // Calculate 50-day MA
        let sum = 0;
        let count = 0;
        for (let j = i - 50; j < i; j++) {
          const val = sectorMaps[s].get(sortedDates[j]);
          if (val !== undefined) { sum += val; count++; }
        }

        if (count >= 40) { // Need at least 40 days of data
          const ma50 = sum / count;
          if (currentVal > ma50) above++;
          total++;
        }
      }

      if (total >= 8) { // Need at least 8 sectors
        all.push({ indicator_id: 'sector_breadth', date, value: (above / total) * 100, source: 'yahoo' });
      }
    }
    console.log('Sector breadth: calculated');
  } catch (e) {
    console.error('Sector breadth failed:', e);
  }

  // AAII Sentiment (bulls - bears spread) - use CNN Fear & Greed as proxy
  try {
    const fgValues = all.filter(i => i.indicator_id === 'fear_greed');
    for (const fg of fgValues) {
      // Convert 0-100 fear/greed to -50 to +50 sentiment spread
      all.push({ indicator_id: 'aaii_sentiment', date: fg.date, value: fg.value - 50, source: 'derived' });
    }
    console.log('AAII sentiment (proxy): calculated');
  } catch (e) {
    console.error('AAII sentiment failed:', e);
  }

  // BTC vs 200-day MA
  try {
    const btcData = all.filter(i => i.indicator_id === 'btc_price').sort((a, b) => a.date.localeCompare(b.date));
    for (let i = 200; i < btcData.length; i++) {
      let sum = 0;
      for (let j = i - 200; j < i; j++) {
        sum += btcData[j].value;
      }
      const ma200 = sum / 200;
      const pctAbove = ((btcData[i].value - ma200) / ma200) * 100;
      all.push({ indicator_id: 'btc_vs_200dma', date: btcData[i].date, value: pctAbove, source: 'yahoo' });
    }
    console.log('BTC vs 200dma: calculated');
  } catch (e) {
    console.error('BTC vs 200dma failed:', e);
  }

  // AUD/JPY (risk sentiment indicator)
  try {
    const audjpy = await fetchYahooSeries('AUDJPY=X', 'audjpy');
    all.push(...audjpy);
    console.log(`AUDJPY: ${audjpy.length} values`);
  } catch (e) {
    console.error('AUDJPY failed:', e);
  }

  // DXY (dollar index)
  try {
    const dxy = await fetchYahooSeries('DX-Y.NYB', 'dxy');
    all.push(...dxy);
    console.log(`DXY: ${dxy.length} values`);
  } catch (e) {
    console.error('DXY failed:', e);
  }

  // Credit spreads from FRED (map to expected IDs)
  try {
    const hySpread = await fetchFredSeries('BAMLH0A0HYM2', 'hy_oas_spread', fredApiKey);
    all.push(...hySpread);
    console.log(`HY OAS Spread: ${hySpread.length} values`);
  } catch (e) {
    console.error('HY OAS Spread failed:', e);
  }

  try {
    const igSpread = await fetchFredSeries('BAMLC0A4CBBBEY', 'ig_oas_spread', fredApiKey);
    all.push(...igSpread);
    console.log(`IG OAS Spread: ${igSpread.length} values`);
  } catch (e) {
    console.error('IG OAS Spread failed:', e);
  }

  try {
    const yieldCurve = await fetchFredSeries('T10Y2Y', 'yield_curve_2s10s', fredApiKey);
    all.push(...yieldCurve);
    console.log(`Yield curve 2s10s: ${yieldCurve.length} values`);
  } catch (e) {
    console.error('Yield curve failed:', e);
  }

  // BBB-AAA spread
  try {
    const [bbb, aaa] = await Promise.all([
      fetchFredSeries('BAMLC0A4CBBBEY', 'bbb_temp', fredApiKey),
      fetchFredSeries('BAMLC0A1CAAAEY', 'aaa_temp', fredApiKey),
    ]);
    const aaaMap = new Map(aaa.map(a => [a.date, a.value]));
    for (const b of bbb) {
      const a = aaaMap.get(b.date);
      if (a !== undefined) {
        all.push({ indicator_id: 'bbb_aaa_spread', date: b.date, value: b.value - a, source: 'fred' });
      }
    }
    console.log('BBB-AAA spread: calculated');
  } catch (e) {
    console.error('BBB-AAA spread failed:', e);
  }

  // EM Spread
  try {
    const emSpread = await fetchFredSeries('BAMLEMCBPIOAS', 'em_spread', fredApiKey);
    all.push(...emSpread);
    console.log(`EM Spread: ${emSpread.length} values`);
  } catch (e) {
    console.error('EM Spread failed:', e);
  }

  // ISM Manufacturing PMI
  try {
    const ism = await fetchFredSeries('MANEMP', 'ism_manufacturing', fredApiKey);
    all.push(...ism);
    console.log(`ISM Manufacturing: ${ism.length} values`);
  } catch (e) {
    console.error('ISM Manufacturing failed:', e);
  }

  // Initial Jobless Claims
  try {
    const claims = await fetchFredSeries('ICSA', 'jobless_claims', fredApiKey);
    all.push(...claims);
    console.log(`Jobless claims: ${claims.length} values`);
  } catch (e) {
    console.error('Jobless claims failed:', e);
  }

  // CFNAI
  try {
    const cfnai = await fetchFredSeries('CFNAI', 'cfnai', fredApiKey);
    all.push(...cfnai);
    console.log(`CFNAI: ${cfnai.length} values`);
  } catch (e) {
    console.error('CFNAI failed:', e);
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

type RegimeDelta = 'UNCHANGED' | 'SHIFTED' | 'STRENGTHENED' | 'WEAKENED';
type RiskPosture = 'risk_on' | 'neutral' | 'risk_off';
type OpportunityDirection = 'bullish' | 'bearish' | 'neutral';
type MarketAlertType = 'regime_change' | 'threshold_cross' | 'opportunity_spike' | 'freshness_warning';
type AlertSeverity = 'info' | 'warning' | 'critical';
type ConflictState = 'ALIGNED' | 'MIXED' | 'CONFLICT';
type EdgeQualityLabel = 'HIGH' | 'MEDIUM' | 'LOW';

interface BriefSnapshot {
  as_of: string;
  summary: string;
  regime_delta: RegimeDelta;
  top_changes: string[];
  risk_posture: RiskPosture;
  explainability: {
    category_movers: Array<{ category: string; score_change: number }>;
    indicator_movers: Array<{ indicator_id: string; value_change: number; z_impact: number }>;
  };
  freshness_status: {
    has_stale_data: boolean;
    stale_count: number;
  };
  updated_at: string;
}

interface OpportunityItem {
  id: string;
  symbol: string | null;
  theme_id: string;
  theme_name: string;
  direction: OpportunityDirection;
  conviction_score: number;
  rationale: string;
  supporting_factors: string[];
  historical_hit_rate: number;
  sample_size: number;
  updated_at: string;
}

interface OpportunitySnapshot {
  as_of: string;
  horizon: '7d' | '30d';
  items: OpportunityItem[];
}

interface MarketAlertEvent {
  id: string;
  event_type: MarketAlertType;
  severity: AlertSeverity;
  title: string;
  body: string;
  entity_type: 'market' | 'theme' | 'indicator';
  entity_id: string | null;
  dedupe_key: string;
  payload_json: string;
  created_at: string;
}

interface SignalsThemeRecord {
  theme_id: string;
  theme_name: string;
  score: number;
  key_tickers: string[];
  classification?: {
    signal_type?: string;
    confidence?: string;
    timing?: string;
  };
}

interface EdgeQualitySnapshot {
  score: number;
  label: EdgeQualityLabel;
  breakdown: {
    data_quality: number;
    model_agreement: number;
    regime_stability: number;
  };
  stale_count: number;
  ml_sample_size: number;
  conflict_state: ConflictState;
}

interface PlanRiskBand {
  bear: number | null;
  base: number | null;
  bull: number | null;
  sample_size: number;
}

interface PlanPayload {
  as_of: string;
  setup_summary: string;
  action_now: {
    risk_allocation_target: number;
    horizon_bias: string;
    primary_signal: SignalType;
  };
  edge_quality: EdgeQualitySnapshot;
  risk_band: {
    d7: PlanRiskBand;
    d30: PlanRiskBand;
  };
  invalidation_rules: string[];
  degraded_reason: string | null;
}

// Allowed origins for CORS
const ALLOWED_ORIGINS = [
  'https://pxicommand.com',
  'https://www.pxicommand.com',
  'https://pxi-command.pages.dev',
  'https://pxi-frontend.pages.dev',
];

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

// Rate limiting: simple in-memory store
const publicRateLimitStore = new Map<string, { count: number; resetTime: number }>();
const adminRateLimitStore = new Map<string, { count: number; resetTime: number }>();
const RATE_LIMIT = 100;
const RATE_WINDOW = 60 * 1000;
const ADMIN_RATE_LIMIT = 20;
const ADMIN_RATE_WINDOW = 60 * 1000;
const MAX_BACKFILL_LIMIT = 365;

function checkRateLimitStore(
  ip: string,
  limit: number,
  windowMs: number,
  store: Map<string, { count: number; resetTime: number }>
): boolean {
  const now = Date.now();
  const record = store.get(ip);

  if (!record || now > record.resetTime) {
    store.set(ip, { count: 1, resetTime: now + windowMs });
    return true;
  }

  if (record.count >= limit) {
    return false;
  }

  record.count++;
  return true;
}

async function checkRateLimitKV(
  ip: string,
  limit: number,
  windowMs: number,
  kv?: KVNamespace
): Promise<boolean> {
  if (!kv) {
    return true;
  }

  const now = Date.now();
  const key = `admin_rate_limit:${ip}`;
  let count = 1;
  let resetTime = now + windowMs;

  const raw = await kv.get(key);
  if (raw) {
    try {
      const parsed = JSON.parse(raw) as { count: number; resetTime: number };
      if (Number.isFinite(parsed.count) && Number.isFinite(parsed.resetTime)) {
        count = parsed.count;
        resetTime = parsed.resetTime;
      }
    } catch (err) {
      console.error('Failed to parse admin rate limit KV record', err);
    }
  }

  if (count >= limit && resetTime > now) {
    return false;
  }

  const nextResetTime = resetTime > now ? resetTime : now + windowMs;
  const nextTtl = Math.max(1, Math.ceil((nextResetTime - now) / 1000));
  const nextCount = count >= limit ? 1 : count + 1;

  await kv.put(key, JSON.stringify({ count: nextCount, resetTime: nextResetTime }), {
    expirationTtl: nextTtl,
  });

  return true;
}

function checkPublicRateLimit(ip: string): boolean {
  return checkRateLimitStore(ip, RATE_LIMIT, RATE_WINDOW, publicRateLimitStore);
}

async function checkAdminRateLimit(ip: string, env: Env): Promise<boolean> {
  const nowOk = checkRateLimitStore(ip, ADMIN_RATE_LIMIT, ADMIN_RATE_WINDOW, adminRateLimitStore);
  if (!nowOk) {
    return false;
  }

  if (!env.RATE_LIMIT_KV) {
    return true;
  }

  try {
    return await checkRateLimitKV(ip, ADMIN_RATE_LIMIT, ADMIN_RATE_WINDOW, env.RATE_LIMIT_KV);
  } catch (err) {
    console.error('Admin KV rate limit check failed, using in-memory fallback', err);
    return true;
  }
}

function getCorsHeaders(origin: string | null): Record<string, string> {
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

function getRequestToken(request: Request): string {
  const authHeader = request.headers.get('Authorization');
  if (authHeader?.startsWith('Bearer ')) {
    return authHeader.slice(7).trim();
  }

  const adminToken = request.headers.get('X-Admin-Token');
  return adminToken?.trim() || '';
}

function constantTimeEquals(a: string, b: string): boolean {
  const left = new TextEncoder().encode(a || '');
  const right = new TextEncoder().encode(b || '');
  const maxLen = Math.max(left.length, right.length);
  let diff = left.length ^ right.length;

  for (let i = 0; i < maxLen; i += 1) {
    const lv = left[i] ?? 0;
    const rv = right[i] ?? 0;
    diff |= lv ^ rv;
  }

  return diff === 0;
}

function hasWriteAccess(request: Request, env: Env): boolean {
  const expected = env.WRITE_API_KEY || '';
  const token = getRequestToken(request);
  if (!expected || token.length === 0) return false;
  return constantTimeEquals(token, expected);
}

function unauthorizedResponse(corsHeaders: Record<string, string>) {
  return Response.json({ error: 'Unauthorized' }, { status: 401, headers: corsHeaders });
}

async function enforceAdminAuth(
  request: Request,
  env: Env,
  corsHeaders: Record<string, string>,
  clientIP: string
): Promise<Response | null> {
  if (!(await checkAdminRateLimit(clientIP, env))) {
    return Response.json(
      { error: 'Too many requests' },
      { status: 429, headers: { ...corsHeaders, 'Retry-After': '60' } }
    );
  }

  if (!hasWriteAccess(request, env)) {
    return unauthorizedResponse(corsHeaders);
  }

  return null;
}

function parseIsoDate(value: string): string | null {
  if (!ISO_DATE_RE.test(value)) return null;
  const date = new Date(`${value}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString().slice(0, 10);
}

function parseBackfillLimit(rawLimit: unknown): number {
  const candidate = Number(rawLimit);
  if (!Number.isFinite(candidate)) {
    return 50;
  }
  return Math.max(1, Math.min(MAX_BACKFILL_LIMIT, Math.floor(candidate)));
}

function parseBackfillDateRange(start?: string, end?: string): {
  start: string | null;
  end: string | null;
} {
  const parsedStart = start ? parseIsoDate(start) : null;
  const parsedEnd = end ? parseIsoDate(end) : null;

  if (start && !parsedStart) {
    throw new Error('Invalid start date. Use YYYY-MM-DD format.');
  }
  if (end && !parsedEnd) {
    throw new Error('Invalid end date. Use YYYY-MM-DD format.');
  }
  if (parsedStart && parsedEnd && parsedStart > parsedEnd) {
    throw new Error('Invalid date range. start must be <= end.');
  }

  return {
    start: parsedStart,
    end: parsedEnd,
  };
}

const TRUE_FLAG_VALUES = new Set(['1', 'true', 'yes', 'on']);
const VALID_ALERT_TYPES: MarketAlertType[] = ['regime_change', 'threshold_cross', 'opportunity_spike', 'freshness_warning'];
const VALID_CADENCE = new Set(['daily_8am_et']);

function isFeatureEnabled(
  env: Env,
  primary: keyof Env,
  fallback: keyof Env,
  defaultValue: boolean
): boolean {
  const raw = env[primary] ?? env[fallback];
  if (raw === undefined || raw === null || raw === '') {
    return defaultValue;
  }
  return TRUE_FLAG_VALUES.has(String(raw).toLowerCase());
}

function clamp(min: number, max: number, value: number): number {
  return Math.max(min, Math.min(max, value));
}

function asIsoDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function asIsoDateTime(date: Date): string {
  return date.toISOString();
}

function toNumber(value: unknown, fallback = 0): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function stableHash(value: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = (hash * 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, '0');
}

function normalizeAlertTypes(rawTypes: unknown): MarketAlertType[] {
  if (!Array.isArray(rawTypes)) return VALID_ALERT_TYPES;
  const normalized = rawTypes
    .map((value) => String(value).trim())
    .filter((value): value is MarketAlertType => VALID_ALERT_TYPES.includes(value as MarketAlertType));
  return normalized.length > 0 ? [...new Set(normalized)] : VALID_ALERT_TYPES;
}

function normalizeCadence(raw: unknown): string {
  const value = String(raw || 'daily_8am_et').trim().toLowerCase();
  if (VALID_CADENCE.has(value)) {
    return value;
  }
  return 'daily_8am_et';
}

function validateEmail(email: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function parseSignalDirection(value: number): OpportunityDirection {
  if (value > 1) return 'bullish';
  if (value < -1) return 'bearish';
  return 'neutral';
}

function confidenceTextToScore(value: string | null | undefined): number {
  const normalized = String(value || '').toUpperCase();
  if (normalized === 'HIGH' || normalized === 'VERY HIGH') return 90;
  if (normalized === 'MEDIUM-HIGH') return 75;
  if (normalized === 'MEDIUM') return 65;
  if (normalized === 'MEDIUM-LOW') return 55;
  if (normalized === 'LOW') return 45;
  return 50;
}

function mapRiskPosture(regime: RegimeResult | null, score: number): RiskPosture {
  if (regime?.regime === 'RISK_OFF' || score <= 40) return 'risk_off';
  if (regime?.regime === 'RISK_ON' || score >= 60) return 'risk_on';
  return 'neutral';
}

function resolveRegimeDelta(
  currentRegime: RegimeResult | null,
  previousRegime: RegimeResult | null,
  scoreDelta: number | null
): RegimeDelta {
  if (currentRegime && previousRegime && currentRegime.regime !== previousRegime.regime) {
    return 'SHIFTED';
  }
  if (scoreDelta !== null && scoreDelta >= 5) {
    return 'STRENGTHENED';
  }
  if (scoreDelta !== null && scoreDelta <= -5) {
    return 'WEAKENED';
  }
  return 'UNCHANGED';
}

function getAlertsSigningSecret(env: Env): string {
  return (env.ALERTS_SIGNING_SECRET || env.WRITE_API_KEY || '').trim();
}

async function hashToken(secret: string, token: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    'SHA-256',
    new TextEncoder().encode(`${secret}:${token}`)
  );
  const bytes = new Uint8Array(digest);
  let output = '';
  for (const byte of bytes) {
    output += byte.toString(16).padStart(2, '0');
  }
  return output;
}

function generateToken(byteLength = 24): string {
  const bytes = new Uint8Array(byteLength);
  crypto.getRandomValues(bytes);
  let out = '';
  for (const byte of bytes) {
    out += byte.toString(16).padStart(2, '0');
  }
  return out;
}

async function parseJsonBody<T>(request: Request): Promise<T | null> {
  try {
    return (await request.json()) as T;
  } catch {
    return null;
  }
}

function indicatorName(indicatorId: string): string {
  const map: Record<string, string> = {
    vix: 'VIX',
    spy_close: 'SPY close',
    dxy: 'Dollar index',
    copper_gold_ratio: 'Copper/Gold ratio',
    aaii_sentiment: 'AAII sentiment',
    fear_greed: 'Fear/Greed',
    hyg: 'HYG',
    lqd: 'LQD',
  };
  return map[indicatorId] || indicatorId.replace(/_/g, ' ');
}

function regimeLabel(regime: RegimeResult | null): string {
  if (!regime) return 'unknown';
  return regime.regime.replace('_', ' ').toLowerCase();
}

function buildOpportunityId(asOfDate: string, themeId: string, horizon: '7d' | '30d'): string {
  return `${asOfDate}-${themeId}-${horizon}-${stableHash(`${asOfDate}:${themeId}:${horizon}`)}`;
}

async function computeFreshnessStatus(db: D1Database): Promise<{ has_stale_data: boolean; stale_count: number }> {
  const freshnessResult = await db.prepare(`
    SELECT indicator_id, MAX(date) as last_date,
           julianday('now') - julianday(MAX(date)) as days_old
    FROM indicator_values
    GROUP BY indicator_id
  `).all<{ indicator_id: string; last_date: string; days_old: number }>();

  const staleCount = (freshnessResult.results || []).filter((row) => {
    const threshold = getStaleThresholdDays(row.indicator_id);
    return Number.isFinite(row.days_old) && row.days_old > threshold;
  }).length;

  return {
    has_stale_data: staleCount > 0,
    stale_count: staleCount,
  };
}

function resolveConflictState(regime: RegimeResult | null, signal: PXISignal): ConflictState {
  if (!regime || regime.regime === 'TRANSITION') {
    return 'MIXED';
  }

  if (regime.regime === 'RISK_ON') {
    if (signal.signal_type === 'RISK_OFF' || signal.signal_type === 'DEFENSIVE' || signal.risk_allocation < 0.5) {
      return 'CONFLICT';
    }
    return 'ALIGNED';
  }

  if (signal.signal_type === 'FULL_RISK' || signal.risk_allocation > 0.75) {
    return 'CONFLICT';
  }

  return 'ALIGNED';
}

function edgeQualityLabel(score: number): EdgeQualityLabel {
  if (score >= 75) return 'HIGH';
  if (score >= 50) return 'MEDIUM';
  return 'LOW';
}

async function fetchPredictionEvaluationSampleSize(db: D1Database): Promise<number> {
  const row = await db.prepare(`
    SELECT COUNT(*) as n
    FROM prediction_log
    WHERE actual_change_7d IS NOT NULL
       OR actual_change_30d IS NOT NULL
  `).first<{ n: number }>();
  return row?.n ?? 0;
}

function computeEdgeQualitySnapshot(params: {
  staleCount: number;
  mlSampleSize: number;
  regime: RegimeResult | null;
  conflictState: ConflictState;
  divergenceCount: number;
}): EdgeQualitySnapshot {
  const { staleCount, mlSampleSize, regime, conflictState, divergenceCount } = params;

  const dataQuality = Math.round(clamp(0, 100, 100 - staleCount * 4));

  let modelAgreement = 78;
  if (mlSampleSize < 5) modelAgreement -= 35;
  else if (mlSampleSize < 20) modelAgreement -= 25;
  else if (mlSampleSize < 50) modelAgreement -= 15;
  else if (mlSampleSize < 100) modelAgreement -= 8;
  modelAgreement = Math.round(clamp(0, 100, modelAgreement));

  let regimeStability = regime?.regime === 'TRANSITION' ? 58 : 82;
  if (divergenceCount >= 3) regimeStability -= 12;
  else if (divergenceCount >= 1) regimeStability -= 6;

  if (conflictState === 'CONFLICT') regimeStability -= 22;
  if (conflictState === 'MIXED') regimeStability -= 10;
  regimeStability = Math.round(clamp(0, 100, regimeStability));

  const score = Math.round(clamp(
    0,
    100,
    dataQuality * 0.4 + modelAgreement * 0.35 + regimeStability * 0.25,
  ));

  return {
    score,
    label: edgeQualityLabel(score),
    breakdown: {
      data_quality: dataQuality,
      model_agreement: modelAgreement,
      regime_stability: regimeStability,
    },
    stale_count: staleCount,
    ml_sample_size: mlSampleSize,
    conflict_state: conflictState,
  };
}

function quantile(values: number[], q: number): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const pos = (sorted.length - 1) * q;
  const low = Math.floor(pos);
  const high = Math.ceil(pos);
  if (low === high) return sorted[low];
  const weight = pos - low;
  return sorted[low] * (1 - weight) + sorted[high] * weight;
}

async function buildCurrentBucketRiskBands(db: D1Database, currentScore: number): Promise<{
  d7: PlanRiskBand;
  d30: PlanRiskBand;
}> {
  const thresholdParams = await db.prepare(`
    SELECT param_key, param_value FROM model_params
    WHERE param_key LIKE 'bucket_threshold_%'
  `).all<{ param_key: string; param_value: number }>();

  const thresholds = { t1: 20, t2: 40, t3: 60, t4: 80 };
  for (const p of thresholdParams.results || []) {
    if (p.param_key === 'bucket_threshold_1') thresholds.t1 = toNumber(p.param_value, 20);
    if (p.param_key === 'bucket_threshold_2') thresholds.t2 = toNumber(p.param_value, 40);
    if (p.param_key === 'bucket_threshold_3') thresholds.t3 = toNumber(p.param_value, 60);
    if (p.param_key === 'bucket_threshold_4') thresholds.t4 = toNumber(p.param_value, 80);
  }

  const bucketFor = (score: number): string => {
    if (score < thresholds.t1) return `0-${thresholds.t1}`;
    if (score < thresholds.t2) return `${thresholds.t1}-${thresholds.t2}`;
    if (score < thresholds.t3) return `${thresholds.t2}-${thresholds.t3}`;
    if (score < thresholds.t4) return `${thresholds.t3}-${thresholds.t4}`;
    return `${thresholds.t4}-100`;
  };

  const currentBucket = bucketFor(currentScore);

  const [pxiScores, spyPrices] = await Promise.all([
    db.prepare(`SELECT date, score FROM pxi_scores ORDER BY date ASC`).all<{ date: string; score: number }>(),
    db.prepare(`
      SELECT date, value FROM indicator_values
      WHERE indicator_id = 'spy_close'
      ORDER BY date ASC
    `).all<{ date: string; value: number }>(),
  ]);

  const spyMap = new Map<string, number>();
  for (const row of spyPrices.results || []) {
    spyMap.set(row.date, row.value);
  }

  const getSpyPrice = (dateStr: string, maxDaysForward = 5): number | null => {
    const date = new Date(dateStr);
    for (let i = 0; i <= maxDaysForward; i += 1) {
      const checkDate = new Date(date);
      checkDate.setDate(checkDate.getDate() + i);
      const checkKey = checkDate.toISOString().split('T')[0];
      const value = spyMap.get(checkKey);
      if (value !== undefined) return value;
    }
    return null;
  };

  const returns7d: number[] = [];
  const returns30d: number[] = [];

  for (const row of pxiScores.results || []) {
    if (bucketFor(row.score) !== currentBucket) continue;

    const spot = getSpyPrice(row.date);
    if (!spot) continue;

    const start = new Date(row.date);
    const date7d = new Date(start);
    date7d.setDate(date7d.getDate() + 7);
    const date30d = new Date(start);
    date30d.setDate(date30d.getDate() + 30);

    const spy7d = getSpyPrice(date7d.toISOString().split('T')[0]);
    const spy30d = getSpyPrice(date30d.toISOString().split('T')[0]);

    if (spy7d !== null) returns7d.push(((spy7d - spot) / spot) * 100);
    if (spy30d !== null) returns30d.push(((spy30d - spot) / spot) * 100);
  }

  const bandFor = (values: number[]): PlanRiskBand => ({
    bear: quantile(values, 0.25),
    base: quantile(values, 0.5),
    bull: quantile(values, 0.75),
    sample_size: values.length,
  });

  return {
    d7: bandFor(returns7d),
    d30: bandFor(returns30d),
  };
}

function resolveHorizonBias(signal: PXISignal, regime: RegimeResult | null, edgeQualityScore: number): string {
  if (edgeQualityScore < 45) return 'capital_preservation';
  if (signal.risk_allocation < 0.45) return '7d_defensive_30d_neutral';
  if (regime?.regime === 'RISK_OFF') return '7d_defensive_30d_defensive';
  if (regime?.regime === 'RISK_ON' && signal.risk_allocation >= 0.7) return '7d_risk_on_30d_risk_on';
  return '7d_neutral_30d_neutral';
}

function buildInvalidationRules(params: {
  pxi: PXIRow;
  freshness: { stale_count: number };
  regime: RegimeResult | null;
  edgeQuality: EdgeQualitySnapshot;
}): string[] {
  const { pxi, freshness, regime, edgeQuality } = params;
  const rules: string[] = [];

  if (pxi.score < 50) {
    rules.push('If PXI closes above 50 for two consecutive sessions, increase risk by one tier.');
  } else {
    rules.push('If PXI closes below 45 for two consecutive sessions, reduce risk by one tier.');
  }

  if (freshness.stale_count > 0) {
    const capThreshold = Math.max(3, freshness.stale_count);
    rules.push(`If stale indicator count remains above ${capThreshold}, keep risk allocation capped until freshness improves.`);
  }

  if (regime?.regime === 'RISK_ON') {
    rules.push('If regime flips to RISK_OFF, immediately move to defensive allocation.');
  } else if (regime?.regime === 'RISK_OFF') {
    rules.push('Only increase risk after regime returns to RISK_ON with edge quality at MEDIUM or HIGH.');
  } else {
    rules.push('During TRANSITION regime, keep sizing reduced until regime stabilizes.');
  }

  if (edgeQuality.label === 'LOW') {
    rules.push('Do not add new risk while edge quality remains LOW.');
  }

  return rules.slice(0, 3);
}

function buildBriefFallbackSnapshot(reason: string): BriefSnapshot & { degraded_reason: string } {
  const now = asIsoDateTime(new Date());
  return {
    as_of: now,
    summary: 'Daily market brief is temporarily unavailable. Showing neutral fallback context.',
    regime_delta: 'UNCHANGED',
    top_changes: [`degraded: ${reason}`],
    risk_posture: 'neutral',
    explainability: {
      category_movers: [],
      indicator_movers: [],
    },
    freshness_status: {
      has_stale_data: false,
      stale_count: 0,
    },
    updated_at: now,
    degraded_reason: reason,
  };
}

function buildOpportunityFallbackSnapshot(
  horizon: '7d' | '30d',
  reason: string
): OpportunitySnapshot & { degraded_reason: string } {
  return {
    as_of: asIsoDateTime(new Date()),
    horizon,
    items: [],
    degraded_reason: reason,
  };
}

function buildPlanFallbackPayload(reason: string): PlanPayload {
  return {
    as_of: asIsoDateTime(new Date()),
    setup_summary: 'Plan service is in degraded mode. Use neutral sizing until full context is restored.',
    action_now: {
      risk_allocation_target: 0.5,
      horizon_bias: '7d_neutral_30d_neutral',
      primary_signal: 'REDUCED_RISK',
    },
    edge_quality: {
      score: 50,
      label: 'MEDIUM',
      breakdown: {
        data_quality: 50,
        model_agreement: 50,
        regime_stability: 50,
      },
      stale_count: 0,
      ml_sample_size: 0,
      conflict_state: 'MIXED',
    },
    risk_band: {
      d7: { bear: null, base: null, bull: null, sample_size: 0 },
      d30: { bear: null, base: null, bull: null, sample_size: 0 },
    },
    invalidation_rules: [
      'Hold neutral risk until plan data is fully available.',
    ],
    degraded_reason: reason,
  };
}

async function fetchLatestSignalsThemes(): Promise<SignalsThemeRecord[]> {
  try {
    const runsRes = await fetch('https://pxicommand.com/signals/api/runs?status=ok');
    if (!runsRes.ok) {
      return [];
    }
    const runsJson = await runsRes.json() as { runs?: Array<{ id?: string }> };
    const latestRunId = runsJson.runs?.[0]?.id;
    if (!latestRunId) {
      return [];
    }

    const detailRes = await fetch(`https://pxicommand.com/signals/api/runs/${encodeURIComponent(latestRunId)}`);
    if (!detailRes.ok) {
      return [];
    }

    const detailJson = await detailRes.json() as { themes?: Array<Record<string, unknown>> };
    const themes = detailJson.themes || [];
    return themes.map((theme) => ({
      theme_id: String(theme.theme_id || 'unknown_theme'),
      theme_name: String(theme.theme_name || theme.theme_id || 'Unknown Theme'),
      score: toNumber(theme.score, 50),
      key_tickers: Array.isArray(theme.key_tickers)
        ? theme.key_tickers.map((ticker) => String(ticker))
        : [],
      classification: typeof theme.classification === 'object' && theme.classification
        ? {
            signal_type: String((theme.classification as Record<string, unknown>).signal_type || ''),
            confidence: String((theme.classification as Record<string, unknown>).confidence || ''),
            timing: String((theme.classification as Record<string, unknown>).timing || ''),
          }
        : undefined,
    }));
  } catch (err) {
    console.error('Failed to fetch signals themes:', err);
    return [];
  }
}

async function buildBriefSnapshot(db: D1Database): Promise<BriefSnapshot | null> {
  const latestAndPrevious = await db.prepare(`
    SELECT date, score, label, status
    FROM pxi_scores
    ORDER BY date DESC
    LIMIT 2
  `).all<{ date: string; score: number; label: string; status: string }>();

  const latest = latestAndPrevious.results?.[0];
  if (!latest) return null;
  const previous = latestAndPrevious.results?.[1] || null;

  const [currentRegime, previousRegime, freshness] = await Promise.all([
    detectRegime(db, latest.date),
    previous ? detectRegime(db, previous.date) : Promise.resolve(null),
    computeFreshnessStatus(db),
  ]);

  const categoryRows = await db.prepare(`
    SELECT c.category as category,
           c.score as current_score,
           p.score as previous_score
    FROM category_scores c
    LEFT JOIN category_scores p
      ON p.category = c.category
      AND p.date = ?
    WHERE c.date = ?
  `).bind(previous?.date || '', latest.date).all<{ category: string; current_score: number; previous_score: number | null }>();

  const categoryMovers = (categoryRows.results || [])
    .map((row) => ({
      category: row.category,
      score_change: row.current_score - (row.previous_score ?? row.current_score),
    }))
    .sort((a, b) => Math.abs(b.score_change) - Math.abs(a.score_change))
    .slice(0, 5);

  const indicatorRows = await db.prepare(`
    SELECT i.indicator_id as indicator_id,
           i.raw_value as current_value,
           p.raw_value as previous_value,
           i.normalized_value as current_norm,
           p.normalized_value as previous_norm
    FROM indicator_scores i
    LEFT JOIN indicator_scores p
      ON p.indicator_id = i.indicator_id
      AND p.date = ?
    WHERE i.date = ?
  `).bind(previous?.date || '', latest.date).all<{
    indicator_id: string;
    current_value: number;
    previous_value: number | null;
    current_norm: number;
    previous_norm: number | null;
  }>();

  const indicatorMovers = (indicatorRows.results || [])
    .map((row) => ({
      indicator_id: row.indicator_id,
      value_change: row.current_value - (row.previous_value ?? row.current_value),
      z_impact: row.current_norm - (row.previous_norm ?? row.current_norm),
    }))
    .sort((a, b) => Math.abs(b.z_impact) - Math.abs(a.z_impact))
    .slice(0, 5);

  const scoreDelta = previous ? latest.score - previous.score : null;
  const regimeDelta = resolveRegimeDelta(currentRegime, previousRegime, scoreDelta);
  const riskPosture = mapRiskPosture(currentRegime, latest.score);
  const deltaText = scoreDelta !== null ? `${scoreDelta >= 0 ? '+' : ''}${scoreDelta.toFixed(1)}` : 'n/a';

  const topChanges: string[] = [];
  for (const category of categoryMovers.slice(0, 3)) {
    topChanges.push(
      `${category.category} ${category.score_change >= 0 ? '+' : ''}${category.score_change.toFixed(1)}`
    );
  }
  for (const indicator of indicatorMovers.slice(0, 2)) {
    topChanges.push(
      `${indicatorName(indicator.indicator_id)} ${indicator.value_change >= 0 ? '+' : ''}${indicator.value_change.toFixed(2)}`
    );
  }

  const summary = `PXI ${latest.score.toFixed(1)} (${latest.label}), ${deltaText} vs prior reading. Regime ${regimeLabel(currentRegime)}; posture ${riskPosture.replace('_', '-')}.${
    freshness.has_stale_data ? ` ${freshness.stale_count} indicator(s) stale.` : ''
  }`;

  return {
    as_of: `${latest.date}T00:00:00.000Z`,
    summary,
    regime_delta: regimeDelta,
    top_changes: topChanges.slice(0, 5),
    risk_posture: riskPosture,
    explainability: {
      category_movers: categoryMovers,
      indicator_movers: indicatorMovers,
    },
    freshness_status: freshness,
    updated_at: asIsoDateTime(new Date()),
  };
}

async function computeHistoricalHitStats(
  db: D1Database,
  horizon: '7d' | '30d'
): Promise<{ hitRate: number; sampleSize: number }> {
  const rows = horizon === '7d'
    ? await db.prepare(`
      SELECT predicted_change_7d as predicted_change, actual_change_7d as actual_change
      FROM prediction_log
      WHERE predicted_change_7d IS NOT NULL
        AND actual_change_7d IS NOT NULL
      ORDER BY prediction_date DESC
      LIMIT 500
    `).all<{ predicted_change: number; actual_change: number }>()
    : await db.prepare(`
      SELECT predicted_change_30d as predicted_change, actual_change_30d as actual_change
      FROM prediction_log
      WHERE predicted_change_30d IS NOT NULL
        AND actual_change_30d IS NOT NULL
      ORDER BY prediction_date DESC
      LIMIT 500
    `).all<{ predicted_change: number; actual_change: number }>();

  const samples = rows.results || [];
  if (samples.length === 0) {
    return { hitRate: 0.5, sampleSize: 0 };
  }

  let correct = 0;
  for (const sample of samples) {
    if ((sample.predicted_change >= 0 && sample.actual_change >= 0) || (sample.predicted_change < 0 && sample.actual_change < 0)) {
      correct += 1;
    }
  }

  return {
    hitRate: correct / samples.length,
    sampleSize: samples.length,
  };
}

async function buildOpportunitySnapshot(
  db: D1Database,
  horizon: '7d' | '30d'
): Promise<OpportunitySnapshot | null> {
  const latestPxi = await db.prepare(`
    SELECT date, score, delta_7d, delta_30d
    FROM pxi_scores
    ORDER BY date DESC
    LIMIT 1
  `).first<{ date: string; score: number; delta_7d: number | null; delta_30d: number | null }>();

  if (!latestPxi) {
    return null;
  }

  const [latestSignal, latestEnsemble, hitStats, themes] = await Promise.all([
    db.prepare(`
      SELECT date, risk_allocation, signal_type, regime
      FROM pxi_signal
      ORDER BY date DESC
      LIMIT 1
    `).first<{ date: string; risk_allocation: number; signal_type: string; regime: string }>(),
    db.prepare(`
      SELECT prediction_date, ensemble_7d, ensemble_30d, confidence_7d, confidence_30d
      FROM ensemble_predictions
      ORDER BY prediction_date DESC
      LIMIT 1
    `).first<{
      prediction_date: string;
      ensemble_7d: number | null;
      ensemble_30d: number | null;
      confidence_7d: string | null;
      confidence_30d: string | null;
    }>(),
    computeHistoricalHitStats(db, horizon),
    fetchLatestSignalsThemes(),
  ]);

  const themeSource = themes.length > 0
    ? themes
    : (await db.prepare(`
      SELECT category as theme_id, category as theme_name, score
      FROM category_scores
      WHERE date = ?
      ORDER BY score DESC
      LIMIT 12
    `).bind(latestPxi.date).all<{ theme_id: string; theme_name: string; score: number }>())
      .results
      ?.map((row) => ({
        theme_id: row.theme_id,
        theme_name: row.theme_name,
        score: row.score,
        key_tickers: [],
      })) || [];

  const ensembleValue = horizon === '7d' ? (latestEnsemble?.ensemble_7d ?? 0) : (latestEnsemble?.ensemble_30d ?? 0);
  const ensembleConfidence = horizon === '7d' ? latestEnsemble?.confidence_7d : latestEnsemble?.confidence_30d;
  const deltaBias = horizon === '7d' ? (latestPxi.delta_7d ?? 0) : (latestPxi.delta_30d ?? 0);

  const mlComponent = clamp(0, 100, 50 + ensembleValue * 6 + confidenceTextToScore(ensembleConfidence) * 0.3 + deltaBias * 0.4);
  const similarComponent = clamp(0, 100, hitStats.hitRate * 100 + Math.min(20, Math.log10(hitStats.sampleSize + 1) * 10));
  const signalComponent = clamp(0, 100, (latestSignal?.risk_allocation ?? 0.5) * 100);

  const items: OpportunityItem[] = themeSource.map((theme) => {
    const confidenceScore = confidenceTextToScore(theme.classification?.confidence);
    const themeComponent = clamp(0, 100, theme.score * 0.75 + confidenceScore * 0.25);
    const conviction = clamp(0, 100, Math.round(
      0.35 * mlComponent +
      0.25 * similarComponent +
      0.20 * signalComponent +
      0.20 * themeComponent
    ));

    const directionalSignal = parseSignalDirection(ensembleValue + (theme.score - 50) * 0.06 + deltaBias * 0.08);
    const supportingFactors = [
      ...(theme.classification?.signal_type ? [theme.classification.signal_type] : []),
      ...(theme.key_tickers.slice(0, 3)),
      latestSignal?.signal_type || 'signal_context',
    ].filter(Boolean);

    const id = buildOpportunityId(latestPxi.date, theme.theme_id, horizon);
    const rationale = `${theme.theme_name}: ${directionalSignal} setup with conviction ${conviction}/100, combining ensemble trend, historical analog hit-rate, and current signal regime.`;

    return {
      id,
      symbol: theme.key_tickers[0] || null,
      theme_id: theme.theme_id,
      theme_name: theme.theme_name,
      direction: directionalSignal,
      conviction_score: conviction,
      rationale,
      supporting_factors: supportingFactors.slice(0, 6),
      historical_hit_rate: hitStats.hitRate,
      sample_size: hitStats.sampleSize,
      updated_at: asIsoDateTime(new Date()),
    };
  });

  items.sort((a, b) => {
    if (b.conviction_score !== a.conviction_score) return b.conviction_score - a.conviction_score;
    if (b.sample_size !== a.sample_size) return b.sample_size - a.sample_size;
    return a.theme_id.localeCompare(b.theme_id);
  });

  return {
    as_of: `${latestPxi.date}T00:00:00.000Z`,
    horizon,
    items,
  };
}

function buildMarketEvent(
  type: MarketAlertType,
  runDate: string,
  severity: AlertSeverity,
  title: string,
  body: string,
  entityType: 'market' | 'theme' | 'indicator',
  entityId: string | null,
  payload: Record<string, unknown>
): MarketAlertEvent {
  const dedupeKey = `${type}:${runDate}:${entityId || 'market'}`;
  return {
    id: `${runDate}-${type}-${stableHash(dedupeKey + asIsoDateTime(new Date()))}`,
    event_type: type,
    severity,
    title,
    body,
    entity_type: entityType,
    entity_id: entityId,
    dedupe_key: dedupeKey,
    payload_json: JSON.stringify(payload),
    created_at: asIsoDateTime(new Date()),
  };
}

async function generateMarketEvents(
  db: D1Database,
  brief: BriefSnapshot,
  opportunities: OpportunitySnapshot
): Promise<MarketAlertEvent[]> {
  const events: MarketAlertEvent[] = [];
  const runDate = brief.as_of.slice(0, 10);

  if (brief.regime_delta === 'SHIFTED') {
    events.push(buildMarketEvent(
      'regime_change',
      runDate,
      'warning',
      'Market regime shifted',
      `PXI regime changed as of ${runDate}. Current posture is ${brief.risk_posture.replace('_', '-')}.`,
      'market',
      'pxi',
      { regime_delta: brief.regime_delta, risk_posture: brief.risk_posture }
    ));
  }

  const latestPxiRows = await db.prepare(`
    SELECT date, score FROM pxi_scores ORDER BY date DESC LIMIT 2
  `).all<{ date: string; score: number }>();
  const current = latestPxiRows.results?.[0];
  const previous = latestPxiRows.results?.[1];
  if (current && previous) {
    for (const threshold of [30, 45, 65, 80]) {
      const crossedUp = previous.score < threshold && current.score >= threshold;
      const crossedDown = previous.score > threshold && current.score <= threshold;
      if (crossedUp || crossedDown) {
        events.push(buildMarketEvent(
          'threshold_cross',
          runDate,
          threshold >= 65 ? 'warning' : 'info',
          `PXI crossed ${threshold}`,
          `PXI moved ${crossedUp ? 'above' : 'below'} ${threshold} (${previous.score.toFixed(1)} → ${current.score.toFixed(1)}).`,
          'indicator',
          `pxi_${threshold}`,
          { threshold, from: previous.score, to: current.score, direction: crossedUp ? 'up' : 'down' }
        ));
      }
    }
  }

  const topOpportunity = opportunities.items[0];
  if (topOpportunity) {
    const previousSnapshot = await db.prepare(`
      SELECT payload_json
      FROM opportunity_snapshots
      WHERE horizon = ?
      ORDER BY as_of DESC
      LIMIT 1 OFFSET 1
    `).bind(opportunities.horizon).first<{ payload_json: string }>();

    let previousTopConviction: number | null = null;
    if (previousSnapshot?.payload_json) {
      try {
        const previousPayload = JSON.parse(previousSnapshot.payload_json) as OpportunitySnapshot;
        previousTopConviction = previousPayload.items?.[0]?.conviction_score ?? null;
      } catch {
        previousTopConviction = null;
      }
    }

    if (previousTopConviction !== null && (topOpportunity.conviction_score - previousTopConviction) >= 12) {
      events.push(buildMarketEvent(
        'opportunity_spike',
        runDate,
        'info',
        'Opportunity conviction spike',
        `${topOpportunity.theme_name} conviction jumped from ${previousTopConviction} to ${topOpportunity.conviction_score}.`,
        'theme',
        topOpportunity.theme_id,
        {
          theme_id: topOpportunity.theme_id,
          previous_conviction: previousTopConviction,
          current_conviction: topOpportunity.conviction_score,
        }
      ));
    }
  }

  if (brief.freshness_status.has_stale_data || brief.freshness_status.stale_count > 0) {
    events.push(buildMarketEvent(
      'freshness_warning',
      runDate,
      'critical',
      'Data freshness warning',
      `${brief.freshness_status.stale_count} indicator(s) are stale and may impact confidence.`,
      'market',
      'data_freshness',
      { stale_count: brief.freshness_status.stale_count }
    ));
  }

  return events;
}

async function storeBriefSnapshot(db: D1Database, brief: BriefSnapshot): Promise<void> {
  await db.prepare(`
    INSERT OR REPLACE INTO market_brief_snapshots (as_of, payload_json, created_at)
    VALUES (?, ?, datetime('now'))
  `).bind(brief.as_of, JSON.stringify(brief)).run();
}

async function storeOpportunitySnapshot(db: D1Database, snapshot: OpportunitySnapshot): Promise<void> {
  await db.prepare(`
    INSERT OR REPLACE INTO opportunity_snapshots (as_of, horizon, payload_json, created_at)
    VALUES (?, ?, ?, datetime('now'))
  `).bind(snapshot.as_of, snapshot.horizon, JSON.stringify(snapshot)).run();
}

async function insertMarketEvents(db: D1Database, events: MarketAlertEvent[], inAppEnabled: boolean): Promise<number> {
  let inserted = 0;
  for (const event of events) {
    const result = await db.prepare(`
      INSERT OR IGNORE INTO market_alert_events
      (id, event_type, severity, title, body, entity_type, entity_id, dedupe_key, payload_json, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      event.id,
      event.event_type,
      event.severity,
      event.title,
      event.body,
      event.entity_type,
      event.entity_id,
      event.dedupe_key,
      event.payload_json,
      event.created_at
    ).run();

    if (result.meta?.changes && result.meta.changes > 0) {
      inserted += 1;
      if (inAppEnabled) {
        await db.prepare(`
          INSERT INTO market_alert_deliveries (event_id, channel, status, attempted_at)
          VALUES (?, 'in_app', 'sent', datetime('now'))
        `).bind(event.id).run();
      }
    }
  }
  return inserted;
}

async function sendResendEmail(
  env: Env,
  payload: { to: string; subject: string; html: string; text: string }
): Promise<{ ok: boolean; providerId?: string; error?: string }> {
  if (!env.RESEND_API_KEY || !env.RESEND_FROM_EMAIL) {
    return { ok: false, error: 'Resend not configured' };
  }

  try {
    const response = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${env.RESEND_API_KEY}`,
      },
      body: JSON.stringify({
        from: env.RESEND_FROM_EMAIL,
        to: [payload.to],
        subject: payload.subject,
        html: payload.html,
        text: payload.text,
      }),
    });

    const body = await response.json().catch(() => ({} as Record<string, unknown>));
    if (!response.ok) {
      const message = String((body as { message?: string }).message || `Resend API ${response.status}`);
      return { ok: false, error: message };
    }

    const providerId = String((body as { id?: string }).id || '');
    return { ok: true, providerId };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : String(err) };
  }
}

async function sendDigestToSubscriber(
  env: Env,
  email: string,
  brief: BriefSnapshot | null,
  opportunities: OpportunitySnapshot | null,
  events: MarketAlertEvent[],
  unsubscribeToken: string
): Promise<{ ok: boolean; providerId?: string; error?: string }> {
  const topOpportunities = opportunities?.items.slice(0, 3) || [];
  const recentEvents = events.slice(0, 10);
  const unsubscribeUrl = `https://pxicommand.com/inbox?unsubscribe_token=${encodeURIComponent(unsubscribeToken)}`;

  const summaryText = brief?.summary || 'PXI market brief unavailable for this run.';
  const eventLines = recentEvents.length > 0
    ? recentEvents.map((event) => `- [${event.severity}] ${event.title}: ${event.body}`).join('\n')
    : '- No market alerts in the last 24 hours.';
  const opportunityLines = topOpportunities.length > 0
    ? topOpportunities.map((item) => `- ${item.theme_name}: ${item.direction} (${item.conviction_score}/100)`).join('\n')
    : '- No opportunities available.';

  const text = [
    'PXI Daily Digest',
    '',
    `As of: ${brief?.as_of || asIsoDateTime(new Date())}`,
    '',
    'Market Brief',
    summaryText,
    '',
    'Top Opportunities',
    opportunityLines,
    '',
    'Last 24h Alerts',
    eventLines,
    '',
    `Unsubscribe: ${unsubscribeUrl}`,
  ].join('\n');

  const html = `
    <div style="font-family:Arial,sans-serif;max-width:640px;margin:0 auto;padding:20px;color:#111;">
      <h2>PXI Daily Digest</h2>
      <p><strong>As of:</strong> ${brief?.as_of || asIsoDateTime(new Date())}</p>
      <h3>Market Brief</h3>
      <p>${summaryText}</p>
      <h3>Top Opportunities</h3>
      <ul>${topOpportunities.map((item) => `<li><strong>${item.theme_name}</strong>: ${item.direction} (${item.conviction_score}/100)</li>`).join('')}</ul>
      <h3>Last 24h Alerts</h3>
      <ul>${recentEvents.map((event) => `<li><strong>[${event.severity}] ${event.title}</strong> - ${event.body}</li>`).join('')}</ul>
      <p><a href="${unsubscribeUrl}">Unsubscribe</a></p>
      <p style="font-size:12px;color:#555;">Not investment advice.</p>
    </div>
  `;

  const subject = `PXI Daily Digest • ${asIsoDate(new Date())}`;
  return sendResendEmail(env, { to: email, subject, html, text });
}

// ============== PXI Calculation ==============

interface IndicatorConfig {
  id: string;
  category: string;
  weight: number;
  invert: boolean;
}

// Full 28-indicator configuration - v1.1 weights
// Total weights sum to 1.0 (100%)
const INDICATOR_CONFIG: IndicatorConfig[] = [
  // ============== POSITIONING (15%) - was Liquidity 22% ==============
  { id: 'fed_balance_sheet', category: 'positioning', weight: 0.05, invert: false },
  { id: 'treasury_general_account', category: 'positioning', weight: 0.04, invert: true },
  { id: 'reverse_repo', category: 'positioning', weight: 0.03, invert: true },
  { id: 'net_liquidity', category: 'positioning', weight: 0.03, invert: false },

  // ============== CREDIT (20%) - was 18% ==============
  { id: 'hy_oas_spread', category: 'credit', weight: 0.06, invert: true },
  { id: 'ig_oas_spread', category: 'credit', weight: 0.04, invert: true },
  { id: 'yield_curve_2s10s', category: 'credit', weight: 0.06, invert: false },
  { id: 'bbb_aaa_spread', category: 'credit', weight: 0.04, invert: true },

  // ============== VOLATILITY (20%) - was 18% ==============
  { id: 'vix', category: 'volatility', weight: 0.07, invert: true },
  { id: 'vix_term_structure', category: 'volatility', weight: 0.05, invert: true },
  { id: 'aaii_sentiment', category: 'volatility', weight: 0.04, invert: false },
  { id: 'gex', category: 'volatility', weight: 0.04, invert: false },

  // ============== BREADTH (15%) - was 12% ==============
  { id: 'rsp_spy_ratio', category: 'breadth', weight: 0.05, invert: false },
  { id: 'sector_breadth', category: 'breadth', weight: 0.05, invert: false },
  { id: 'small_cap_strength', category: 'breadth', weight: 0.03, invert: false },
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
  // Get most recent indicator values for each indicator (up to target date)
  // This allows using lagging data when today's data isn't available yet
  const latestValues = await db.prepare(`
    SELECT iv.indicator_id, iv.value, iv.date
    FROM indicator_values iv
    INNER JOIN (
      SELECT indicator_id, MAX(date) as max_date
      FROM indicator_values
      WHERE date <= ?
      GROUP BY indicator_id
    ) latest ON iv.indicator_id = latest.indicator_id AND iv.date = latest.max_date
  `).bind(targetDate).all<{ indicator_id: string; value: number; date: string }>();

  if (!latestValues.results || latestValues.results.length === 0) {
    return null;
  }

  const valueMap = new Map(latestValues.results.map(r => [r.indicator_id, r.value]));

  // Get historical values for percentile calculation (5 years per v1.1 spec)
  const historyResult = await db.prepare(`
    SELECT indicator_id, value FROM indicator_values
    WHERE date >= date(?, '-5 years')
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

  // Get historical PXI for deltas using actual calendar dates
  const targetDateObj = new Date(targetDate);
  const date1d = new Date(targetDateObj);
  date1d.setDate(date1d.getDate() - 1);
  const date7d = new Date(targetDateObj);
  date7d.setDate(date7d.getDate() - 7);
  const date30d = new Date(targetDateObj);
  date30d.setDate(date30d.getDate() - 30);

  const formatDate = (d: Date) => d.toISOString().split('T')[0];

  // Get closest available scores to target dates
  const [hist1d, hist7d, hist30d] = await Promise.all([
    db.prepare(`SELECT score FROM pxi_scores WHERE date <= ? ORDER BY date DESC LIMIT 1`).bind(formatDate(date1d)).first<{ score: number }>(),
    db.prepare(`SELECT score FROM pxi_scores WHERE date <= ? ORDER BY date DESC LIMIT 1`).bind(formatDate(date7d)).first<{ score: number }>(),
    db.prepare(`SELECT score FROM pxi_scores WHERE date <= ? ORDER BY date DESC LIMIT 1`).bind(formatDate(date30d)).first<{ score: number }>(),
  ]);

  const delta_1d = hist1d ? pxiScore - hist1d.score : null;
  const delta_7d = hist7d ? pxiScore - hist7d.score : null;
  const delta_30d = hist30d ? pxiScore - hist30d.score : null;

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

    // Generate embedding for vector similarity with engineered features
    const todayIndicators = indicators.filter(i => i.date === today);
    const embeddingText = generateEmbeddingText({
      indicators: todayIndicators,
      pxi: {
        score: result.pxi.score,
        delta_7d: result.pxi.delta_7d,
        delta_30d: result.pxi.delta_30d,
      },
      categories: result.categories.map(c => ({ category: c.category, score: c.score })),
    });

    try {
      if (embeddingText) {
        const embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
          text: embeddingText,
        });

        await env.VECTORIZE.upsert([{
          id: today,
          values: embedding.data[0],
          metadata: { date: today, score: result.pxi.score },
        }]);
        console.log('🔮 Generated and stored embedding with engineered features');
      }
    } catch (e) {
      console.error('Embedding generation failed:', e);
    }

    console.log(`✅ PXI refresh complete: ${result.pxi.score.toFixed(1)} (${result.pxi.label})`);

    // Generate and store prediction for tracking
    try {
      const thirtyDaysAgo = new Date();
      thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
      const cutoffDate = formatDate(thirtyDaysAgo);

      // Get embedding for today (reuse the same embedding text)
      const todayEmbedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
        text: embeddingText,
      });

      const similar = await env.VECTORIZE.query(todayEmbedding.data[0], {
        topK: 50,
        returnMetadata: 'all',
      });

      const filteredMatches = (similar.matches || []).filter(m => {
        const matchDate = (m.metadata as { date?: string })?.date || m.id;
        return matchDate < cutoffDate;
      }).slice(0, 5);

      if (filteredMatches.length > 0) {
        // Fetch accuracy scores for similar periods
        const periodDates = filteredMatches.map(m => (m.metadata as { date?: string })?.date || m.id);
        const accuracyScores = await env.DB.prepare(`
          SELECT period_date, accuracy_score, times_used FROM period_accuracy
          WHERE period_date IN (${periodDates.map(() => '?').join(',')})
        `).bind(...periodDates).all<{ period_date: string; accuracy_score: number; times_used: number }>();

        const accuracyMap = new Map(
          (accuracyScores.results || []).map(a => [a.period_date, { score: a.accuracy_score, used: a.times_used }])
        );

        // Calculate predictions from similar periods with enhanced weighting
        const outcomes: { d7: number | null; d30: number | null; weight: number; breakdown: { similarity: number; recency: number; accuracy: number } }[] = [];
        const todayMs = new Date().getTime();

        for (const match of filteredMatches) {
          const histDate = (match.metadata as { date?: string })?.date || match.id;
          const histScore = (match.metadata as { score?: number })?.score;
          if (!histScore) continue;

          // Weight components:
          // 1. Similarity (from Vectorize) - already 0-1
          const similarityWeight = match.score;

          // 2. Recency - exponential decay, half-life of 365 days
          const histDateMs = new Date(histDate).getTime();
          const daysSince = (todayMs - histDateMs) / (1000 * 60 * 60 * 24);
          const recencyWeight = Math.exp(-daysSince / 365);

          // 3. Accuracy - from period_accuracy table (default 0.5 if unknown)
          const periodAccuracy = accuracyMap.get(histDate);
          const accuracyWeight = periodAccuracy && periodAccuracy.used >= 2
            ? periodAccuracy.score
            : 0.5; // Default for unproven periods

          // Combined weight (geometric mean preserves scale)
          const combinedWeight = similarityWeight * (0.4 + 0.3 * recencyWeight + 0.3 * accuracyWeight);

          const endDate = new Date(histDate);
          endDate.setDate(endDate.getDate() + 35);
          const endDateStr = formatDate(endDate);

          const futureScores = await env.DB.prepare(`
            SELECT date, score FROM pxi_scores WHERE date > ? AND date <= ? ORDER BY date LIMIT 35
          `).bind(histDate, endDateStr).all<{ date: string; score: number }>();

          let d7_change: number | null = null;
          let d30_change: number | null = null;

          for (const fs of futureScores.results || []) {
            const daysAfter = Math.round((new Date(fs.date).getTime() - histDateMs) / (1000 * 60 * 60 * 24));
            if (daysAfter >= 5 && daysAfter <= 10 && d7_change === null) {
              d7_change = fs.score - histScore;
            }
            if (daysAfter >= 25 && daysAfter <= 35 && d30_change === null) {
              d30_change = fs.score - histScore;
            }
          }

          outcomes.push({
            d7: d7_change,
            d30: d30_change,
            weight: combinedWeight,
            breakdown: { similarity: similarityWeight, recency: recencyWeight, accuracy: accuracyWeight }
          });
        }

        // Calculate weighted predictions
        const validD7 = outcomes.filter(o => o.d7 !== null);
        const validD30 = outcomes.filter(o => o.d30 !== null);

        const d7_prediction = validD7.length > 0
          ? validD7.reduce((sum, o) => sum + o.d7! * o.weight, 0) / validD7.reduce((sum, o) => sum + o.weight, 0)
          : null;
        const d30_prediction = validD30.length > 0
          ? validD30.reduce((sum, o) => sum + o.d30! * o.weight, 0) / validD30.reduce((sum, o) => sum + o.weight, 0)
          : null;

        // Enhanced confidence scoring
        const calculateConfidence = (
          values: number[],
          weights: number[],
          prediction: number | null
        ) => {
          if (values.length === 0 || prediction === null) return { score: 0, components: null };

          const totalWeight = weights.reduce((sum, w) => sum + w, 0);

          // 1. Directional agreement (weighted) - do periods agree on direction?
          let weightedDirection = 0;
          for (let i = 0; i < values.length; i++) {
            const direction = values[i] > 0 ? 1 : -1;
            weightedDirection += direction * weights[i];
          }
          weightedDirection /= totalWeight;
          const directionScore = Math.abs(weightedDirection); // 0-1, 1 = perfect agreement

          // 2. Magnitude consistency - low variance = high confidence
          const mean = values.reduce((a, b) => a + b, 0) / values.length;
          const variance = values.reduce((sum, v) => sum + Math.pow(v - mean, 2), 0) / values.length;
          const stdDev = Math.sqrt(variance);
          // Normalize: stdDev of 0 = perfect, stdDev of 20+ = low confidence
          const consistencyScore = Math.max(0, 1 - stdDev / 20);

          // 3. Sample size factor - more samples = more confident (capped at 5)
          const sampleScore = Math.min(values.length / 5, 1);

          // 4. Weight quality - average combined weight of samples
          const avgWeight = totalWeight / values.length;
          const weightScore = avgWeight; // Already 0-1 range

          // Combined confidence (weighted average of factors)
          const confidence = (
            directionScore * 0.35 +    // Direction agreement most important
            consistencyScore * 0.25 +  // Magnitude consistency
            sampleScore * 0.20 +       // Sample size
            weightScore * 0.20         // Weight quality
          );

          return {
            score: confidence,
            components: {
              direction: directionScore,
              consistency: consistencyScore,
              sample_size: sampleScore,
              weight_quality: weightScore,
            }
          };
        };

        const d7_values = validD7.map(o => o.d7!);
        const d7_weights = validD7.map(o => o.weight);
        const d30_values = validD30.map(o => o.d30!);
        const d30_weights = validD30.map(o => o.weight);

        const d7_conf = calculateConfidence(d7_values, d7_weights, d7_prediction);
        const d30_conf = calculateConfidence(d30_values, d30_weights, d30_prediction);
        const d7_confidence = d7_conf.score;
        const d30_confidence = d30_conf.score;

        // Calculate target dates
        const target7d = new Date();
        target7d.setDate(target7d.getDate() + 7);
        const target30d = new Date();
        target30d.setDate(target30d.getDate() + 30);

        await env.DB.prepare(`
          INSERT OR REPLACE INTO prediction_log
          (prediction_date, target_date_7d, target_date_30d, current_score, predicted_change_7d, predicted_change_30d, confidence_7d, confidence_30d, similar_periods)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).bind(
          today,
          d7_prediction !== null ? formatDate(target7d) : null,
          d30_prediction !== null ? formatDate(target30d) : null,
          result.pxi.score,
          d7_prediction,
          d30_prediction,
          d7_confidence,
          d30_confidence,
          JSON.stringify(filteredMatches.slice(0, 5).map(m => m.id))
        ).run();

        console.log(`📈 Logged prediction: 7d=${d7_prediction?.toFixed(1) || 'N/A'}, 30d=${d30_prediction?.toFixed(1) || 'N/A'}`);
      }
    } catch (predErr) {
      console.error('Prediction logging failed:', predErr);
    }

    // Evaluate past predictions
    try {
      const pendingPredictions = await env.DB.prepare(`
        SELECT id, prediction_date, target_date_7d, target_date_30d, current_score,
               predicted_change_7d, predicted_change_30d, actual_change_7d, actual_change_30d
        FROM prediction_log
        WHERE evaluated_at IS NULL
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

      let evaluated = 0;
      for (const pred of pendingPredictions.results || []) {
        let needsUpdate = false;
        let actual7d = pred.actual_change_7d;
        let actual30d = pred.actual_change_30d;

        if (pred.target_date_7d && pred.target_date_7d <= today && actual7d === null) {
          const score7d = await env.DB.prepare(
            'SELECT score FROM pxi_scores WHERE date = ?'
          ).bind(pred.target_date_7d).first<{ score: number }>();
          if (score7d) {
            actual7d = score7d.score - pred.current_score;
            needsUpdate = true;
          }
        }

        if (pred.target_date_30d && pred.target_date_30d <= today && actual30d === null) {
          const score30d = await env.DB.prepare(
            'SELECT score FROM pxi_scores WHERE date = ?'
          ).bind(pred.target_date_30d).first<{ score: number }>();
          if (score30d) {
            actual30d = score30d.score - pred.current_score;
            needsUpdate = true;
          }
        }

        if (needsUpdate) {
          const fullyEvaluated = (actual7d !== null || pred.predicted_change_7d === null) &&
                                 (actual30d !== null || pred.predicted_change_30d === null);
          await env.DB.prepare(`
            UPDATE prediction_log SET actual_change_7d = ?, actual_change_30d = ?, evaluated_at = ? WHERE id = ?
          `).bind(actual7d, actual30d, fullyEvaluated ? new Date().toISOString() : null, pred.id).run();
          evaluated++;
        }
      }

      if (evaluated > 0) {
        console.log(`✅ Evaluated ${evaluated} past predictions`);
      }
    } catch (evalErr) {
      console.error('Prediction evaluation failed:', evalErr);
    }

    // Retrain model periodically (update period accuracy scores and tune params)
    try {
      const evaluatedPreds = await env.DB.prepare(`
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

      if (evaluatedPreds.results && evaluatedPreds.results.length >= 3) {
        // Track accuracy for each period
        const periodStats: Record<string, {
          times_used: number;
          correct_7d: number;
          total_7d: number;
          errors_7d: number[];
        }> = {};

        for (const pred of evaluatedPreds.results) {
          let periods: string[] = [];
          try { periods = JSON.parse(pred.similar_periods); } catch { continue; }

          for (const periodDate of periods) {
            if (!periodStats[periodDate]) {
              periodStats[periodDate] = { times_used: 0, correct_7d: 0, total_7d: 0, errors_7d: [] };
            }
            const stats = periodStats[periodDate];
            stats.times_used++;

            if (pred.predicted_change_7d !== null && pred.actual_change_7d !== null) {
              stats.total_7d++;
              const p = pred.predicted_change_7d, a = pred.actual_change_7d;
              if ((p > 0 && a > 0) || (p < 0 && a < 0)) stats.correct_7d++;
              stats.errors_7d.push(Math.abs(p - a));
            }
          }
        }

        // Update period_accuracy table
        for (const [periodDate, stats] of Object.entries(periodStats)) {
          const accuracyScore = stats.total_7d > 0 ? stats.correct_7d / stats.total_7d : 0.5;
          const avgError = stats.errors_7d.length > 0
            ? stats.errors_7d.reduce((a, b) => a + b, 0) / stats.errors_7d.length : null;

          await env.DB.prepare(`
            INSERT OR REPLACE INTO period_accuracy
            (period_date, times_used, correct_direction_7d, total_7d_predictions, avg_error_7d, accuracy_score, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
          `).bind(periodDate, stats.times_used, stats.correct_7d, stats.total_7d, avgError, accuracyScore).run();
        }

        // Auto-tune accuracy_weight
        let total7dCorrect = 0, total7d = 0;
        for (const pred of evaluatedPreds.results) {
          if (pred.predicted_change_7d !== null && pred.actual_change_7d !== null) {
            total7d++;
            const p = pred.predicted_change_7d, a = pred.actual_change_7d;
            if ((p > 0 && a > 0) || (p < 0 && a < 0)) total7dCorrect++;
          }
        }
        const overallAccuracy = total7d > 0 ? total7dCorrect / total7d : 0.5;
        let newWeight = 0.3;
        if (overallAccuracy > 0.65) newWeight = 0.5;
        else if (overallAccuracy > 0.55) newWeight = 0.4;
        else if (overallAccuracy < 0.45) newWeight = 0.2;

        await env.DB.prepare(`
          UPDATE model_params SET param_value = ?, updated_at = datetime('now'),
          notes = ? WHERE param_key = 'accuracy_weight'
        `).bind(newWeight, `Auto-tuned: ${(overallAccuracy * 100).toFixed(0)}% accuracy on ${total7d} predictions`).run();

        console.log(`🧠 Retrained model: ${Object.keys(periodStats).length} periods, ${(overallAccuracy * 100).toFixed(0)}% accuracy, weight=${newWeight}`);
      }
    } catch (retrainErr) {
      console.error('Model retrain failed:', retrainErr);
    }

    // Evaluate ensemble (ML) predictions
    try {
      const pendingEnsemble = await env.DB.prepare(`
        SELECT id, prediction_date, target_date_7d, target_date_30d, current_score,
               ensemble_7d, ensemble_30d, xgboost_7d, xgboost_30d, lstm_7d, lstm_30d,
               actual_change_7d, actual_change_30d
        FROM ensemble_predictions
        WHERE evaluated_at IS NULL
      `).all<{
        id: number;
        prediction_date: string;
        target_date_7d: string;
        target_date_30d: string;
        current_score: number;
        ensemble_7d: number | null;
        ensemble_30d: number | null;
        xgboost_7d: number | null;
        xgboost_30d: number | null;
        lstm_7d: number | null;
        lstm_30d: number | null;
        actual_change_7d: number | null;
        actual_change_30d: number | null;
      }>();

      let ensembleEvaluated = 0;
      for (const pred of pendingEnsemble.results || []) {
        let needsUpdate = false;
        let actual7d = pred.actual_change_7d;
        let actual30d = pred.actual_change_30d;
        let dir7d: number | null = null;
        let dir30d: number | null = null;

        // Check 7d prediction
        if (pred.target_date_7d <= today && actual7d === null) {
          const score7d = await env.DB.prepare(
            'SELECT score FROM pxi_scores WHERE date = ?'
          ).bind(pred.target_date_7d).first<{ score: number }>();
          if (score7d) {
            actual7d = score7d.score - pred.current_score;
            needsUpdate = true;
            // Check if direction was correct
            if (pred.ensemble_7d !== null) {
              const predictedUp = pred.ensemble_7d > 0;
              const actualUp = actual7d > 0;
              dir7d = (predictedUp === actualUp) ? 1 : 0;
            }
          }
        }

        // Check 30d prediction
        if (pred.target_date_30d <= today && actual30d === null) {
          const score30d = await env.DB.prepare(
            'SELECT score FROM pxi_scores WHERE date = ?'
          ).bind(pred.target_date_30d).first<{ score: number }>();
          if (score30d) {
            actual30d = score30d.score - pred.current_score;
            needsUpdate = true;
            // Check if direction was correct
            if (pred.ensemble_30d !== null) {
              const predictedUp = pred.ensemble_30d > 0;
              const actualUp = actual30d > 0;
              dir30d = (predictedUp === actualUp) ? 1 : 0;
            }
          }
        }

        if (needsUpdate) {
          const fullyEvaluated = (actual7d !== null) && (actual30d !== null);
          await env.DB.prepare(`
            UPDATE ensemble_predictions
            SET actual_change_7d = ?, actual_change_30d = ?,
                direction_correct_7d = ?, direction_correct_30d = ?,
                evaluated_at = ?
            WHERE id = ?
          `).bind(
            actual7d, actual30d,
            dir7d, dir30d,
            fullyEvaluated ? new Date().toISOString() : null,
            pred.id
          ).run();
          ensembleEvaluated++;
        }
      }

      if (ensembleEvaluated > 0) {
        console.log(`🤖 Evaluated ${ensembleEvaluated} ensemble predictions`);
      }
    } catch (ensembleErr) {
      console.error('Ensemble evaluation failed:', ensembleErr);
    }
  } else {
    console.log('⚠️ Could not calculate PXI - insufficient data');
  }
}

// ============== Regime Detection (v1.1 - Percentile-Based) ==============

type RegimeType = 'RISK_ON' | 'RISK_OFF' | 'TRANSITION';

interface RegimeSignal {
  indicator: string;
  value: number | null;
  percentile: number | null;  // v1.1: actual percentile rank
  threshold_low_pct: number;  // v1.1: percentile threshold
  threshold_high_pct: number; // v1.1: percentile threshold
  signal: 'RISK_ON' | 'RISK_OFF' | 'NEUTRAL';
  description: string;
}

interface RegimeResult {
  regime: RegimeType;
  confidence: number;
  signals: RegimeSignal[];
  description: string;
  date: string;
}

async function detectRegime(db: D1Database, targetDate?: string): Promise<RegimeResult | null> {
  // Get the date to analyze
  const dateToUse = targetDate || (await db.prepare(
    'SELECT date FROM pxi_scores ORDER BY date DESC LIMIT 1'
  ).first<{ date: string }>())?.date;

  if (!dateToUse) return null;

  // Helper to get current value and 5-year percentile for an indicator
  const getIndicatorWithPercentile = async (indicatorId: string): Promise<{ value: number; percentile: number } | null> => {
    // Get current value
    const current = await db.prepare(`
      SELECT value FROM indicator_values
      WHERE indicator_id = ? AND date <= ?
      ORDER BY date DESC LIMIT 1
    `).bind(indicatorId, dateToUse).first<{ value: number }>();

    if (!current) return null;

    // Get 5-year history for percentile calculation
    const history = await db.prepare(`
      SELECT value FROM indicator_values
      WHERE indicator_id = ? AND date >= date(?, '-5 years') AND date <= ?
    `).bind(indicatorId, dateToUse, dateToUse).all<{ value: number }>();

    if (!history.results || history.results.length < 10) {
      // Fall back to all available data if less than 5 years
      const allHistory = await db.prepare(`
        SELECT value FROM indicator_values WHERE indicator_id = ? AND date <= ?
      `).bind(indicatorId, dateToUse).all<{ value: number }>();

      if (!allHistory.results || allHistory.results.length < 10) return null;

      const values = allHistory.results.map(r => r.value).sort((a, b) => a - b);
      const rank = values.filter(v => v < current.value).length +
                   values.filter(v => v === current.value).length / 2;
      return { value: current.value, percentile: (rank / values.length) * 100 };
    }

    const values = history.results.map(r => r.value).sort((a, b) => a - b);
    const rank = values.filter(v => v < current.value).length +
                 values.filter(v => v === current.value).length / 2;

    return { value: current.value, percentile: (rank / values.length) * 100 };
  };

  // Fetch key regime indicators with percentiles
  const [vix, hySpread, sectorBreadth, yieldCurve, dollarIndex] = await Promise.all([
    getIndicatorWithPercentile('vix'),
    getIndicatorWithPercentile('hy_oas_spread'),  // v1.1: use hy_oas_spread
    getIndicatorWithPercentile('sector_breadth'),
    getIndicatorWithPercentile('yield_curve_2s10s'),  // v1.1: use yield_curve_2s10s
    getIndicatorWithPercentile('dxy'),  // v1.1: use dxy
  ]);

  const signals: RegimeSignal[] = [];

  // VIX signal: <30th percentile = risk-on, >70th percentile = risk-off
  // (Lower VIX values = lower percentile = complacency/risk-on)
  if (vix !== null) {
    let vixSignal: 'RISK_ON' | 'RISK_OFF' | 'NEUTRAL' = 'NEUTRAL';
    let vixDesc = `VIX at ${vix.percentile.toFixed(0)}th percentile`;
    if (vix.percentile < 30) {
      vixSignal = 'RISK_ON';
      vixDesc = `Low volatility (${vix.percentile.toFixed(0)}th pct) - complacency`;
    } else if (vix.percentile > 70) {
      vixSignal = 'RISK_OFF';
      vixDesc = `Elevated fear (${vix.percentile.toFixed(0)}th pct)`;
    }
    signals.push({
      indicator: 'VIX',
      value: vix.value,
      percentile: vix.percentile,
      threshold_low_pct: 30,
      threshold_high_pct: 70,
      signal: vixSignal,
      description: vixDesc,
    });
  }

  // High Yield OAS Spread: <30th percentile = risk-on, >70th percentile = risk-off
  // (Lower spreads = lower percentile = risk appetite strong)
  if (hySpread !== null) {
    let hySignal: 'RISK_ON' | 'RISK_OFF' | 'NEUTRAL' = 'NEUTRAL';
    let hyDesc = `HY spreads at ${hySpread.percentile.toFixed(0)}th percentile`;
    if (hySpread.percentile < 30) {
      hySignal = 'RISK_ON';
      hyDesc = `Tight spreads (${hySpread.percentile.toFixed(0)}th pct) - risk appetite`;
    } else if (hySpread.percentile > 70) {
      hySignal = 'RISK_OFF';
      hyDesc = `Wide spreads (${hySpread.percentile.toFixed(0)}th pct) - credit stress`;
    }
    signals.push({
      indicator: 'HY_OAS',
      value: hySpread.value,
      percentile: hySpread.percentile,
      threshold_low_pct: 30,
      threshold_high_pct: 70,
      signal: hySignal,
      description: hyDesc,
    });
  }

  // Sector Breadth: >60% = risk-on, <40% = risk-off (direct thresholds, not percentile)
  if (sectorBreadth !== null) {
    let breadthSignal: 'RISK_ON' | 'RISK_OFF' | 'NEUTRAL' = 'NEUTRAL';
    let breadthDesc = `Sector breadth at ${sectorBreadth.value.toFixed(0)}%`;
    if (sectorBreadth.value > 60) {
      breadthSignal = 'RISK_ON';
      breadthDesc = `Broad participation (${sectorBreadth.value.toFixed(0)}%)`;
    } else if (sectorBreadth.value < 40) {
      breadthSignal = 'RISK_OFF';
      breadthDesc = `Narrow leadership (${sectorBreadth.value.toFixed(0)}%)`;
    }
    signals.push({
      indicator: 'BREADTH',
      value: sectorBreadth.value,
      percentile: sectorBreadth.percentile,
      threshold_low_pct: 40,  // Direct threshold (not percentile)
      threshold_high_pct: 60, // Direct threshold (not percentile)
      signal: breadthSignal,
      description: breadthDesc,
    });
  }

  // Yield Curve (2s10s): >60th percentile = risk-on, <20th percentile = risk-off
  // (Higher/steeper curve = higher percentile = growth expectations)
  if (yieldCurve !== null) {
    let ycSignal: 'RISK_ON' | 'RISK_OFF' | 'NEUTRAL' = 'NEUTRAL';
    let ycDesc = `Yield curve at ${yieldCurve.percentile.toFixed(0)}th percentile`;
    if (yieldCurve.percentile > 60) {
      ycSignal = 'RISK_ON';
      ycDesc = `Steep curve (${yieldCurve.percentile.toFixed(0)}th pct) - growth`;
    } else if (yieldCurve.percentile < 20) {
      ycSignal = 'RISK_OFF';
      ycDesc = `Flat/inverted (${yieldCurve.percentile.toFixed(0)}th pct) - caution`;
    }
    signals.push({
      indicator: 'YIELD_CURVE',
      value: yieldCurve.value,
      percentile: yieldCurve.percentile,
      threshold_low_pct: 20,
      threshold_high_pct: 60,
      signal: ycSignal,
      description: ycDesc,
    });
  }

  // Dollar Index: <40th percentile = risk-on (weak dollar), >70th percentile = risk-off
  // (Lower DXY = lower percentile = weak dollar = risk-on)
  if (dollarIndex !== null) {
    let dxySignal: 'RISK_ON' | 'RISK_OFF' | 'NEUTRAL' = 'NEUTRAL';
    let dxyDesc = `DXY at ${dollarIndex.percentile.toFixed(0)}th percentile`;
    if (dollarIndex.percentile < 40) {
      dxySignal = 'RISK_ON';
      dxyDesc = `Weak dollar (${dollarIndex.percentile.toFixed(0)}th pct) - risk appetite`;
    } else if (dollarIndex.percentile > 70) {
      dxySignal = 'RISK_OFF';
      dxyDesc = `Strong dollar (${dollarIndex.percentile.toFixed(0)}th pct) - safe haven`;
    }
    signals.push({
      indicator: 'DXY',
      value: dollarIndex.value,
      percentile: dollarIndex.percentile,
      threshold_low_pct: 40,
      threshold_high_pct: 70,
      signal: dxySignal,
      description: dxyDesc,
    });
  }

  // Count votes
  const riskOnVotes = signals.filter(s => s.signal === 'RISK_ON').length;
  const riskOffVotes = signals.filter(s => s.signal === 'RISK_OFF').length;
  const totalSignals = signals.length;

  // Determine regime
  let regime: RegimeType = 'TRANSITION';
  let description = 'Mixed signals - market in transition';
  let confidence = 0.5;

  if (totalSignals > 0) {
    if (riskOnVotes >= 3 || (riskOnVotes >= 2 && riskOffVotes === 0)) {
      regime = 'RISK_ON';
      description = 'Risk-on environment - favorable for equities';
      confidence = riskOnVotes / totalSignals;
    } else if (riskOffVotes >= 3 || (riskOffVotes >= 2 && riskOnVotes === 0)) {
      regime = 'RISK_OFF';
      description = 'Risk-off environment - defensive positioning recommended';
      confidence = riskOffVotes / totalSignals;
    } else {
      confidence = 1 - Math.abs(riskOnVotes - riskOffVotes) / totalSignals;
    }
  }

  return {
    regime,
    confidence,
    signals,
    description,
    date: dateToUse,
  };
}

// ============== Divergence Detection (v1.1 with Alert Metrics) ==============

interface AlertMetrics {
  historical_frequency: number;  // % of days this condition occurred
  median_return_7d: number | null;  // Median 7-day forward SPY return
  median_return_30d: number | null; // Median 30-day forward SPY return
  false_positive_rate: number | null; // % of times followed by positive returns (for bearish alerts)
}

interface DivergenceAlert {
  type: 'PXI_REGIME' | 'PXI_MOMENTUM' | 'REGIME_SHIFT';
  severity: 'LOW' | 'MEDIUM' | 'HIGH';
  title: string;
  description: string;
  actionable: boolean;
  metrics?: AlertMetrics;  // v1.1: historical performance data
}

interface DivergenceResult {
  has_divergence: boolean;
  alerts: DivergenceAlert[];
}

// v1.1: Calculate historical metrics for alert types
async function calculateAlertMetrics(
  db: D1Database,
  alertType: string,
  conditionFn: (pxi: number, vix: number | null, regime: string | null) => boolean
): Promise<AlertMetrics> {
  // Get historical data with forward returns
  const historicalData = await db.prepare(`
    SELECT
      p.date,
      p.score as pxi,
      (SELECT value FROM indicator_values iv WHERE iv.indicator_id = 'vix' AND iv.date <= p.date ORDER BY iv.date DESC LIMIT 1) as vix,
      me.forward_return_7d,
      me.forward_return_30d
    FROM pxi_scores p
    LEFT JOIN market_embeddings me ON p.date = me.date
    WHERE p.date >= date('now', '-3 years')
    ORDER BY p.date DESC
  `).all<{
    date: string;
    pxi: number;
    vix: number | null;
    forward_return_7d: number | null;
    forward_return_30d: number | null;
  }>();

  if (!historicalData.results || historicalData.results.length < 30) {
    return { historical_frequency: 0, median_return_7d: null, median_return_30d: null, false_positive_rate: null };
  }

  const totalDays = historicalData.results.length;
  const triggerDays: { r7d: number | null; r30d: number | null }[] = [];

  for (const row of historicalData.results) {
    // For now, we'll use a simplified condition check
    // In production, this would be more sophisticated
    if (conditionFn(row.pxi, row.vix, null)) {
      triggerDays.push({ r7d: row.forward_return_7d, r30d: row.forward_return_30d });
    }
  }

  if (triggerDays.length === 0) {
    return { historical_frequency: 0, median_return_7d: null, median_return_30d: null, false_positive_rate: null };
  }

  const frequency = (triggerDays.length / totalDays) * 100;

  // Calculate medians
  const valid7d = triggerDays.filter(t => t.r7d !== null).map(t => t.r7d!).sort((a, b) => a - b);
  const valid30d = triggerDays.filter(t => t.r30d !== null).map(t => t.r30d!).sort((a, b) => a - b);

  const median7d = valid7d.length > 0 ? valid7d[Math.floor(valid7d.length / 2)] : null;
  const median30d = valid30d.length > 0 ? valid30d[Math.floor(valid30d.length / 2)] : null;

  // False positive rate: % of times followed by positive returns (for bearish alerts)
  const positiveReturns = valid7d.filter(r => r > 0).length;
  const fpr = valid7d.length > 0 ? (positiveReturns / valid7d.length) * 100 : null;

  return {
    historical_frequency: Math.round(frequency * 10) / 10,
    median_return_7d: median7d !== null ? Math.round(median7d * 100) / 100 : null,
    median_return_30d: median30d !== null ? Math.round(median30d * 100) / 100 : null,
    false_positive_rate: fpr !== null ? Math.round(fpr) : null,
  };
}

async function detectDivergence(
  db: D1Database,
  currentPxi: number,
  regime: RegimeResult | null
): Promise<DivergenceResult> {
  const alerts: DivergenceAlert[] = [];

  // Get VIX for more granular analysis
  const vix = await db.prepare(`
    SELECT value FROM indicator_values
    WHERE indicator_id = 'vix'
    ORDER BY date DESC LIMIT 1
  `).first<{ value: number }>();

  const vixValue = vix?.value ?? null;
  const isLowVol = vixValue !== null && vixValue < 18;
  const isHighVol = vixValue !== null && vixValue > 25;

  // v1.1: Calculate metrics for each alert type in parallel
  const [stealthMetrics, resilientMetrics, hiddenRiskMetrics, rapidDetMetrics] = await Promise.all([
    calculateAlertMetrics(db, 'STEALTH_WEAKNESS', (pxi, vix) => pxi < 40 && vix !== null && vix < 18),
    calculateAlertMetrics(db, 'RESILIENT_STRENGTH', (pxi, vix) => pxi > 60 && vix !== null && vix > 25),
    calculateAlertMetrics(db, 'HIDDEN_RISK', (pxi) => pxi < 40), // Simplified - would need regime in prod
    calculateAlertMetrics(db, 'RAPID_DETERIORATION', (pxi) => pxi < 40), // Simplified
  ]);

  // Divergence 1: PXI LOW but volatility is low (unusual - weakness without panic)
  if (currentPxi < 40 && isLowVol) {
    alerts.push({
      type: 'PXI_REGIME',
      severity: 'HIGH',
      title: 'Stealth Weakness',
      description: `PXI at ${currentPxi.toFixed(0)} signals weakness, but VIX at ${vixValue?.toFixed(1)} shows no fear. Unusual - watch for delayed volatility spike.`,
      actionable: true,
      metrics: stealthMetrics,
    });
  }

  // Divergence 2: PXI HIGH but volatility elevated (rare - strength despite fear)
  if (currentPxi > 60 && isHighVol) {
    alerts.push({
      type: 'PXI_REGIME',
      severity: 'MEDIUM',
      title: 'Resilient Strength',
      description: `PXI at ${currentPxi.toFixed(0)} shows strength despite VIX at ${vixValue?.toFixed(1)}. Market shrugging off fear - potentially bullish.`,
      actionable: true,
      metrics: resilientMetrics,
    });
  }

  // Divergence 3: Regime is RISK_OFF but PXI is high
  if (regime?.regime === 'RISK_OFF' && currentPxi > 50) {
    alerts.push({
      type: 'PXI_REGIME',
      severity: 'MEDIUM',
      title: 'Regime Divergence',
      description: `Regime signals RISK_OFF but PXI at ${currentPxi.toFixed(0)} remains elevated. Conflicting signals - proceed with caution.`,
      actionable: true,
    });
  }

  // Divergence 4: Regime is RISK_ON but PXI is low
  if (regime?.regime === 'RISK_ON' && currentPxi < 40) {
    alerts.push({
      type: 'PXI_REGIME',
      severity: 'HIGH',
      title: 'Hidden Risk',
      description: `Regime appears RISK_ON but PXI at ${currentPxi.toFixed(0)} shows underlying weakness. Structure looks OK but something's off.`,
      actionable: true,
      metrics: hiddenRiskMetrics,
    });
  }

  // Divergence 5: Check PXI momentum - falling sharply while regime stable
  // Use calendar date (7 days ago) not index-based lookup
  const latestPxi = await db.prepare(`
    SELECT date, score FROM pxi_scores ORDER BY date DESC LIMIT 1
  `).first<{ date: string; score: number }>();

  if (latestPxi) {
    const latestDate = new Date(latestPxi.date);
    const date7dAgo = new Date(latestDate);
    date7dAgo.setDate(date7dAgo.getDate() - 7);
    const date7dStr = date7dAgo.toISOString().split('T')[0];

    const pxi7dAgo = await db.prepare(`
      SELECT score FROM pxi_scores WHERE date <= ? ORDER BY date DESC LIMIT 1
    `).bind(date7dStr).first<{ score: number }>();

    if (pxi7dAgo) {
      const weekChange = latestPxi.score - pxi7dAgo.score;

      // Sharp drop (>15 points) while regime is still RISK_ON
      if (weekChange < -15 && regime?.regime === 'RISK_ON') {
        alerts.push({
          type: 'PXI_MOMENTUM',
          severity: 'HIGH',
          title: 'Rapid Deterioration',
          description: `PXI dropped ${Math.abs(weekChange).toFixed(0)} points in 7 days but regime still shows RISK_ON. Leading indicator of regime change?`,
          actionable: true,
          metrics: rapidDetMetrics,
        });
      }

      // Sharp rise (>15 points) while regime is RISK_OFF
      if (weekChange > 15 && regime?.regime === 'RISK_OFF') {
        alerts.push({
          type: 'PXI_MOMENTUM',
          severity: 'MEDIUM',
          title: 'Potential Regime Shift',
          description: `PXI rose ${weekChange.toFixed(0)} points in 7 days despite RISK_OFF regime. Early signs of improvement.`,
          actionable: true,
        });
      }
    }
  }

  // Divergence 6: Check for regime instability (multiple changes recently)
  const recentDates = await db.prepare(
    'SELECT date FROM pxi_scores ORDER BY date DESC LIMIT 15'
  ).all<{ date: string }>();

  let regimeChanges = 0;
  let lastRegime: RegimeType | null = null;

  for (const d of (recentDates.results || []).slice(0, 10)) {
    const r = await detectRegime(db, d.date);
    if (r && lastRegime && r.regime !== lastRegime) {
      regimeChanges++;
    }
    if (r) lastRegime = r.regime;
  }

  if (regimeChanges >= 3) {
    alerts.push({
      type: 'REGIME_SHIFT',
      severity: 'MEDIUM',
      title: 'Unstable Regime',
      description: `${regimeChanges} regime changes in last 10 days. Market in transition - signals less reliable.`,
      actionable: false,
    });
  }

  return {
    has_divergence: alerts.length > 0,
    alerts,
  };
}

// ============== PXI-Signal Layer (v1.1) ==============

type SignalType = 'FULL_RISK' | 'REDUCED_RISK' | 'RISK_OFF' | 'DEFENSIVE';

interface PXISignal {
  pxi_level: number;
  delta_pxi_7d: number | null;
  delta_pxi_30d: number | null;
  category_dispersion: number;  // Std dev of category scores
  regime: RegimeType;
  volatility_percentile: number | null;
  risk_allocation: number;  // 0-1 scale
  signal_type: SignalType;
  adjustments: string[];  // Explanations for allocation adjustments
}

async function calculatePXISignal(
  db: D1Database,
  pxi: { score: number; delta_7d: number | null; delta_30d: number | null },
  regime: RegimeResult | null,
  categoryScores: { score: number }[]
): Promise<PXISignal> {
  // Calculate category dispersion (standard deviation of category scores)
  const scores = categoryScores.map(c => c.score);
  const mean = scores.reduce((a, b) => a + b, 0) / scores.length;
  const variance = scores.reduce((sum, s) => sum + Math.pow(s - mean, 2), 0) / scores.length;
  const dispersion = Math.sqrt(variance);

  // Get VIX percentile for volatility adjustment
  const vixData = await db.prepare(`
    SELECT value FROM indicator_values
    WHERE indicator_id = 'vix'
    ORDER BY date DESC LIMIT 1
  `).first<{ value: number }>();

  let volatilityPercentile: number | null = null;
  if (vixData) {
    const vixHistory = await db.prepare(`
      SELECT value FROM indicator_values
      WHERE indicator_id = 'vix' AND date >= date('now', '-5 years')
    `).all<{ value: number }>();

    if (vixHistory.results && vixHistory.results.length > 0) {
      const values = vixHistory.results.map(r => r.value).sort((a, b) => a - b);
      const rank = values.filter(v => v < vixData.value).length +
                   values.filter(v => v === vixData.value).length / 2;
      volatilityPercentile = (rank / values.length) * 100;
    }
  }

  // Base allocation from PXI score (0-100 maps to 0.3-1.0)
  let baseAllocation = 0.3 + (pxi.score / 100) * 0.7;
  const adjustments: string[] = [];

  // Apply canonical trading policy adjustments
  let allocation = baseAllocation;

  // Rule 1: If Regime = RISK_OFF → allocation * 0.5
  if (regime?.regime === 'RISK_OFF') {
    allocation *= 0.5;
    adjustments.push('RISK_OFF regime: -50%');
  }

  // Rule 2: If Regime = TRANSITION → allocation * 0.75
  if (regime?.regime === 'TRANSITION') {
    allocation *= 0.75;
    adjustments.push('TRANSITION regime: -25%');
  }

  // Rule 3: If Δ7d < -10 → allocation * 0.8
  if (pxi.delta_7d !== null && pxi.delta_7d < -10) {
    allocation *= 0.8;
    adjustments.push(`7d deterioration (${pxi.delta_7d.toFixed(0)}pts): -20%`);
  }

  // Rule 4: If vol_percentile > 80 → allocation * 0.7
  if (volatilityPercentile !== null && volatilityPercentile > 80) {
    allocation *= 0.7;
    adjustments.push(`High volatility (${volatilityPercentile.toFixed(0)}th pct): -30%`);
  }

  // Determine signal type
  let signalType: SignalType = 'FULL_RISK';
  if (allocation < 0.3) {
    signalType = 'DEFENSIVE';
  } else if (allocation < 0.5) {
    signalType = 'RISK_OFF';
  } else if (allocation < 0.8) {
    signalType = 'REDUCED_RISK';
  }

  return {
    pxi_level: pxi.score,
    delta_pxi_7d: pxi.delta_7d,
    delta_pxi_30d: pxi.delta_30d,
    category_dispersion: Math.round(dispersion * 10) / 10,
    regime: regime?.regime || 'TRANSITION',
    volatility_percentile: volatilityPercentile !== null ? Math.round(volatilityPercentile) : null,
    risk_allocation: Math.round(allocation * 100) / 100,
    signal_type: signalType,
    adjustments,
  };
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (url.hostname === 'www.pxicommand.com') {
      return Response.redirect(`https://pxicommand.com${url.pathname}${url.search}`, 301);
    }

    const origin = request.headers.get('Origin');
    const corsHeaders = getCorsHeaders(origin);
    const method = request.method === 'HEAD' ? 'GET' : request.method;

    // Rate limiting
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
    if (!checkPublicRateLimit(clientIP)) {
      return Response.json(
        { error: 'Too many requests' },
        { status: 429, headers: { ...corsHeaders, 'Retry-After': '60' } }
      );
    }

    // Only allow GET, POST, and OPTIONS (+HEAD mapped to GET for compatibility)
    if (!['GET', 'POST', 'OPTIONS', 'HEAD'].includes(request.method)) {
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

      // Database migration - create any missing tables (requires auth)
      if (url.pathname === '/api/migrate' && method === 'POST') {
        const adminAuthFailure = await enforceAdminAuth(request, env, corsHeaders, clientIP);
        if (adminAuthFailure) {
          return adminAuthFailure;
        }

        const migrations: string[] = [];

        // Create prediction_log table if it doesn't exist
        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS prediction_log (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              prediction_date TEXT NOT NULL UNIQUE,
              target_date_7d TEXT,
              target_date_30d TEXT,
              current_score REAL NOT NULL,
              predicted_change_7d REAL,
              predicted_change_30d REAL,
              actual_change_7d REAL,
              actual_change_30d REAL,
              confidence_7d REAL,
              confidence_30d REAL,
              similar_periods TEXT,
              evaluated_at TEXT,
              created_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_prediction_log_date ON prediction_log(prediction_date DESC)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_prediction_log_evaluated ON prediction_log(evaluated_at)`).run();
          migrations.push('prediction_log');
        } catch (e) {
          console.error('prediction_log migration failed:', e);
        }

        // Create model_params table if it doesn't exist
        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS model_params (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              param_key TEXT NOT NULL UNIQUE,
              param_value REAL NOT NULL,
              notes TEXT,
              updated_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          migrations.push('model_params');

          // Insert default params if they don't exist
          await env.DB.prepare(`
            INSERT OR IGNORE INTO model_params (param_key, param_value, notes)
            VALUES ('accuracy_weight', 0.3, 'Weight given to period accuracy vs similarity')
          `).run();
          await env.DB.prepare(`
            INSERT OR IGNORE INTO model_params (param_key, param_value, notes)
            VALUES ('similarity_threshold', 0.8, 'Minimum cosine similarity to include period')
          `).run();

          // Adaptive bucket thresholds (default: 20, 40, 60, 80)
          await env.DB.prepare(`
            INSERT OR IGNORE INTO model_params (param_key, param_value, notes)
            VALUES ('bucket_threshold_1', 20, 'Threshold between bucket 1 (0-X) and bucket 2')
          `).run();
          await env.DB.prepare(`
            INSERT OR IGNORE INTO model_params (param_key, param_value, notes)
            VALUES ('bucket_threshold_2', 40, 'Threshold between bucket 2 and bucket 3')
          `).run();
          await env.DB.prepare(`
            INSERT OR IGNORE INTO model_params (param_key, param_value, notes)
            VALUES ('bucket_threshold_3', 60, 'Threshold between bucket 3 and bucket 4')
          `).run();
          await env.DB.prepare(`
            INSERT OR IGNORE INTO model_params (param_key, param_value, notes)
            VALUES ('bucket_threshold_4', 80, 'Threshold between bucket 4 and bucket 5 (X-100)')
          `).run();
        } catch (e) {
          console.error('model_params migration failed:', e);
        }

        // Create period_accuracy table if it doesn't exist
        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS period_accuracy (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              period_date TEXT NOT NULL UNIQUE,
              times_used INTEGER DEFAULT 0,
              correct_predictions INTEGER DEFAULT 0,
              total_predictions INTEGER DEFAULT 0,
              mean_absolute_error REAL,
              accuracy_score REAL,
              updated_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_period_accuracy_date ON period_accuracy(period_date DESC)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_period_accuracy_score ON period_accuracy(accuracy_score DESC)`).run();
          migrations.push('period_accuracy');
        } catch (e) {
          console.error('period_accuracy migration failed:', e);
        }

        // Create alerts table if it doesn't exist
        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS alerts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              date TEXT NOT NULL,
              alert_type TEXT NOT NULL,
              message TEXT NOT NULL,
              severity TEXT NOT NULL DEFAULT 'info',
              acknowledged INTEGER DEFAULT 0,
              pxi_score REAL,
              forward_return_7d REAL,
              forward_return_30d REAL,
              created_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_alerts_date ON alerts(date DESC)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_alerts_type ON alerts(alert_type)`).run();

          // Add columns to existing table if they don't exist (SQLite doesn't have IF NOT EXISTS for columns)
          try {
            await env.DB.prepare(`ALTER TABLE alerts ADD COLUMN pxi_score REAL`).run();
          } catch (e) { /* Column already exists */ }
          try {
            await env.DB.prepare(`ALTER TABLE alerts ADD COLUMN forward_return_7d REAL`).run();
          } catch (e) { /* Column already exists */ }
          try {
            await env.DB.prepare(`ALTER TABLE alerts ADD COLUMN forward_return_30d REAL`).run();
          } catch (e) { /* Column already exists */ }

          migrations.push('alerts');
        } catch (e) {
          console.error('alerts migration failed:', e);
        }

        // Create market product layer tables
        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS email_subscribers (
              id TEXT PRIMARY KEY,
              email TEXT UNIQUE NOT NULL,
              status TEXT NOT NULL CHECK(status IN ('pending', 'active', 'unsubscribed', 'bounced')),
              cadence TEXT NOT NULL DEFAULT 'daily_8am_et',
              types_json TEXT NOT NULL,
              timezone TEXT NOT NULL DEFAULT 'America/New_York',
              created_at TEXT DEFAULT (datetime('now')),
              updated_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_email_subscribers_status ON email_subscribers(status)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_email_subscribers_updated ON email_subscribers(updated_at DESC)`).run();
          migrations.push('email_subscribers');
        } catch (e) {
          console.error('email_subscribers migration failed:', e);
        }

        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS email_verification_tokens (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              email TEXT NOT NULL,
              token_hash TEXT NOT NULL,
              expires_at TEXT NOT NULL,
              used_at TEXT,
              created_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_email_verification_email_expires ON email_verification_tokens(email, expires_at DESC)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_email_verification_hash ON email_verification_tokens(token_hash)`).run();
          migrations.push('email_verification_tokens');
        } catch (e) {
          console.error('email_verification_tokens migration failed:', e);
        }

        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS email_unsubscribe_tokens (
              subscriber_id TEXT PRIMARY KEY,
              token_hash TEXT NOT NULL,
              created_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_email_unsubscribe_hash ON email_unsubscribe_tokens(token_hash)`).run();
          migrations.push('email_unsubscribe_tokens');
        } catch (e) {
          console.error('email_unsubscribe_tokens migration failed:', e);
        }

        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS market_brief_snapshots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              as_of TEXT NOT NULL UNIQUE,
              payload_json TEXT NOT NULL,
              created_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_brief_as_of ON market_brief_snapshots(as_of DESC)`).run();
          migrations.push('market_brief_snapshots');
        } catch (e) {
          console.error('market_brief_snapshots migration failed:', e);
        }

        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS opportunity_snapshots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              as_of TEXT NOT NULL,
              horizon TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_at TEXT DEFAULT (datetime('now')),
              UNIQUE(as_of, horizon)
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_opportunity_snapshots_lookup ON opportunity_snapshots(as_of DESC, horizon)`).run();
          migrations.push('opportunity_snapshots');
        } catch (e) {
          console.error('opportunity_snapshots migration failed:', e);
        }

        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS market_alert_events (
              id TEXT PRIMARY KEY,
              event_type TEXT NOT NULL,
              severity TEXT NOT NULL CHECK(severity IN ('info', 'warning', 'critical')),
              title TEXT NOT NULL,
              body TEXT NOT NULL,
              entity_type TEXT NOT NULL CHECK(entity_type IN ('market', 'theme', 'indicator')),
              entity_id TEXT,
              dedupe_key TEXT NOT NULL UNIQUE,
              payload_json TEXT,
              created_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_events_created ON market_alert_events(created_at DESC)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_events_type ON market_alert_events(event_type, created_at DESC)`).run();
          migrations.push('market_alert_events');
        } catch (e) {
          console.error('market_alert_events migration failed:', e);
        }

        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS market_alert_deliveries (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              event_id TEXT NOT NULL,
              channel TEXT NOT NULL CHECK(channel IN ('in_app', 'email')),
              subscriber_id TEXT,
              status TEXT NOT NULL CHECK(status IN ('queued', 'sent', 'failed')),
              provider_id TEXT,
              error TEXT,
              attempted_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_deliveries_event ON market_alert_deliveries(event_id, attempted_at DESC)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_deliveries_subscriber ON market_alert_deliveries(subscriber_id, attempted_at DESC)`).run();
          migrations.push('market_alert_deliveries');
        } catch (e) {
          console.error('market_alert_deliveries migration failed:', e);
        }

        return Response.json({
          success: true,
          tables_created: migrations,
          message: `Migration complete. Created/verified: ${migrations.join(', ')}`,
        }, { headers: corsHeaders });
      }

      // Regime detection endpoint
      if (url.pathname === '/api/regime') {
        const regime = await detectRegime(env.DB);
        if (!regime) {
          return Response.json({ error: 'Could not detect regime' }, { status: 500, headers: corsHeaders });
        }

        // Get regime history for context
        const recentDates = await env.DB.prepare(
          'SELECT date FROM pxi_scores ORDER BY date DESC LIMIT 30'
        ).all<{ date: string }>();

        const regimeHistory: { date: string; regime: RegimeType }[] = [];
        for (const d of (recentDates.results || []).slice(0, 10)) {
          const r = await detectRegime(env.DB, d.date);
          if (r) {
            regimeHistory.push({ date: r.date, regime: r.regime });
          }
        }

        // Count regime changes in last 30 days
        let regimeChanges = 0;
        for (let i = 1; i < regimeHistory.length; i++) {
          if (regimeHistory[i].regime !== regimeHistory[i - 1].regime) {
            regimeChanges++;
          }
        }

        return Response.json({
          current: regime,
          history: regimeHistory,
          stability: regimeChanges <= 1 ? 'STABLE' : regimeChanges <= 3 ? 'MODERATE' : 'VOLATILE',
          regime_changes_10d: regimeChanges,
        }, { headers: corsHeaders });
      }

      // OG Image endpoint
      if (url.pathname === '/og-image.svg') {
        const pxi = await env.DB.prepare(
          'SELECT score, label, status, delta_7d FROM pxi_scores ORDER BY date DESC LIMIT 1'
        ).first<{ score: number; label: string; status: string; delta_7d: number | null }>();

        if (!pxi) {
          return new Response('No data', { status: 404, headers: corsHeaders });
        }

        // Fetch categories for the bar visualization
        const categories = await env.DB.prepare(
          'SELECT category, score FROM category_scores WHERE date = (SELECT date FROM pxi_scores ORDER BY date DESC LIMIT 1) ORDER BY score DESC'
        ).all<{ category: string; score: number }>();

        const score = Math.round(pxi.score);
        const label = pxi.label;
        const delta7d = pxi.delta_7d;
        const deltaStr = delta7d !== null ? `${delta7d >= 0 ? '+' : ''}${delta7d.toFixed(1)}` : '';

        const statusColors: Record<string, string> = {
          max_pamp: '#00a3ff',
          pamping: '#00a3ff',
          neutral: '#949ba5',
          soft: '#949ba5',
          dumping: '#949ba5',
        };
        const color = statusColors[pxi.status] || '#949ba5';
        const isAccent = pxi.status === 'max_pamp' || pxi.status === 'pamping';

        // Build category bars SVG
        const cats = (categories.results || []).slice(0, 7);
        const catBarY = 440;
        const catBarHeight = 3;
        const catBarGap = 26;
        const catLabelX = 540;
        const catBarX = 640;
        const catBarMaxW = 360;

        const catBars = cats.map((c, i) => {
          const y = catBarY + i * catBarGap;
          const w = Math.max(4, (c.score / 100) * catBarMaxW);
          const isHigh = c.score >= 70;
          const displayName = c.category.replace(/_/g, ' ');
          return `
    <text x="${catLabelX}" y="${y + 4}" text-anchor="end" font-family="'SF Mono', 'Menlo', monospace" font-size="11" fill="#949ba5" letter-spacing="1" text-transform="uppercase">${displayName}</text>
    <rect x="${catBarX}" y="${y - 1}" width="${catBarMaxW}" height="${catBarHeight}" rx="1.5" fill="#26272b"/>
    <rect x="${catBarX}" y="${y - 1}" width="${w}" height="${catBarHeight}" rx="1.5" fill="${isHigh ? '#00a3ff' : 'rgba(148,155,165,0.5)'}"/>
    <text x="${catBarX + catBarMaxW + 12}" y="${y + 4}" font-family="'SF Mono', 'Menlo', monospace" font-size="11" fill="#949ba5">${Math.round(c.score)}</text>`;
        }).join('');

        // Score position
        const scoreX = 200;
        const scoreY = 360;

        const svg = `<svg width="1200" height="630" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <!-- Subtle radial glow behind score -->
    <radialGradient id="glow" cx="0.3" cy="0.5" r="0.4">
      <stop offset="0%" stop-color="#00a3ff" stop-opacity="0.04"/>
      <stop offset="100%" stop-color="#000000" stop-opacity="0"/>
    </radialGradient>
  </defs>

  <!-- Background -->
  <rect width="1200" height="630" fill="#000000"/>
  <rect width="1200" height="630" fill="url(#glow)"/>

  <!-- Top border accent line -->
  <rect x="0" y="0" width="1200" height="1" fill="#26272b"/>

  <!-- Header: PXI/COMMAND -->
  <text x="60" y="56" font-family="'SF Mono', 'Menlo', monospace" font-weight="500" font-size="14" fill="#949ba5" letter-spacing="3">PXI<tspan fill="#00a3ff">/</tspan>COMMAND</text>

  <!-- Subtitle -->
  <text x="60" y="80" font-family="'SF Mono', 'Menlo', monospace" font-size="10" fill="#949ba5" opacity="0.4" letter-spacing="2">MACRO MARKET STRENGTH INDEX</text>

  <!-- Hero Score -->
  <text x="${scoreX}" y="${scoreY}" text-anchor="middle" font-family="system-ui, -apple-system, 'Helvetica Neue', sans-serif" font-weight="200" font-size="200" fill="#f3f3f3" letter-spacing="-8">${score}</text>

  <!-- 7d Delta -->
  ${deltaStr ? `<text x="${scoreX}" y="${scoreY + 44}" text-anchor="middle" font-family="'SF Mono', 'Menlo', monospace" font-size="16" fill="${delta7d && delta7d >= 0 ? '#00a3ff' : '#949ba5'}" letter-spacing="1">${deltaStr}<tspan fill="#949ba5" opacity="0.5" font-size="11"> 7D</tspan></text>` : ''}

  <!-- Status Badge -->
  <rect x="${scoreX - 60}" y="${scoreY + 60}" width="120" height="32" rx="3" fill="${color}" fill-opacity="${isAccent ? '1' : '0.15'}"/>
  <text x="${scoreX}" y="${scoreY + 82}" text-anchor="middle" font-family="'SF Mono', 'Menlo', monospace" font-weight="500" font-size="11" fill="${isAccent ? '#000000' : '#f3f3f3'}" letter-spacing="2">${label}</text>

  <!-- Divider -->
  <line x1="440" y1="430" x2="440" y2="${catBarY + cats.length * catBarGap - 10}" stroke="#26272b" stroke-width="1" stroke-dasharray="4,4"/>

  <!-- Category Bars -->
  ${catBars}

  <!-- Bottom border accent line -->
  <rect x="0" y="629" width="1200" height="1" fill="#26272b"/>

  <!-- Footer -->
  <text x="1140" y="612" text-anchor="end" font-family="'SF Mono', 'Menlo', monospace" font-size="10" fill="#949ba5" opacity="0.3" letter-spacing="2">PXICOMMAND.COM</text>
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

        // Detect current regime
        const regime = await detectRegime(env.DB, pxi.date);

        // Detect divergences
        const divergence = await detectDivergence(env.DB, pxi.score, regime);

        // v1.4: Check data freshness - get stale indicators
        const freshnessResult = await env.DB.prepare(`
          SELECT indicator_id, MAX(date) as last_date,
                 julianday('now') - julianday(MAX(date)) as days_old
          FROM indicator_values
          GROUP BY indicator_id
          ORDER BY days_old DESC
        `).all<{ indicator_id: string; last_date: string; days_old: number }>();

        const staleIndicators = (freshnessResult.results || []).filter((r) => {
          const threshold = getStaleThresholdDays(r.indicator_id);
          return r.days_old > threshold;
        });
        const hasStaleData = staleIndicators.length > 0;

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
          regime: regime ? {
            type: regime.regime,
            confidence: regime.confidence,
            description: regime.description,
          } : null,
          divergence: divergence.has_divergence ? {
            alerts: divergence.alerts,
          } : null,
          // v1.4: Data freshness info
          dataFreshness: {
            hasStaleData,
            staleCount: staleIndicators.length,
            staleIndicators: hasStaleData ? staleIndicators.slice(0, 5).map(s => ({
              id: s.indicator_id,
              lastUpdate: s.last_date,
              daysOld: Math.round(s.days_old),
            })) : [],
          },
        };

        return Response.json(response, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'public, max-age=60',
          },
        });
      }

      if (url.pathname === '/api/brief' && method === 'GET') {
        if (!isFeatureEnabled(env, 'FEATURE_ENABLE_BRIEF', 'ENABLE_BRIEF', true)) {
          return Response.json(buildBriefFallbackSnapshot('feature_disabled'), {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'no-store',
            },
          });
        }

        const scope = (url.searchParams.get('scope') || 'market').trim().toLowerCase();
        if (scope !== 'market') {
          return Response.json({ error: 'Only scope=market is supported in phase 1' }, { status: 400, headers: corsHeaders });
        }

        let snapshot: BriefSnapshot | null = null;
        const stored = await env.DB.prepare(`
          SELECT payload_json
          FROM market_brief_snapshots
          ORDER BY as_of DESC
          LIMIT 1
        `).first<{ payload_json: string }>();

        if (stored?.payload_json) {
          try {
            snapshot = JSON.parse(stored.payload_json) as BriefSnapshot;
          } catch {
            snapshot = null;
          }
        }

        if (!snapshot) {
          try {
            snapshot = await buildBriefSnapshot(env.DB);
          } catch (err) {
            console.error('Failed to build brief snapshot:', err);
            snapshot = null;
          }
          if (!snapshot) {
            return Response.json(buildBriefFallbackSnapshot('snapshot_unavailable'), {
              headers: {
                ...corsHeaders,
                'Cache-Control': 'no-store',
              },
            });
          }
          await storeBriefSnapshot(env.DB, snapshot);
        }

        return Response.json(snapshot, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'public, max-age=60',
          },
        });
      }

      if (url.pathname === '/api/opportunities' && method === 'GET') {
        const horizon = (url.searchParams.get('horizon') || '7d').trim() === '30d' ? '30d' : '7d';
        const limit = Math.min(50, Math.max(1, parseInt(url.searchParams.get('limit') || '20', 10)));
        if (!isFeatureEnabled(env, 'FEATURE_ENABLE_OPPORTUNITIES', 'ENABLE_OPPORTUNITIES', true)) {
          const fallback = buildOpportunityFallbackSnapshot(horizon, 'feature_disabled');
          return Response.json({
            as_of: fallback.as_of,
            horizon: fallback.horizon,
            items: fallback.items,
            degraded_reason: fallback.degraded_reason,
          }, {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'no-store',
            },
          });
        }

        let snapshot: OpportunitySnapshot | null = null;
        const stored = await env.DB.prepare(`
          SELECT payload_json
          FROM opportunity_snapshots
          WHERE horizon = ?
          ORDER BY as_of DESC
          LIMIT 1
        `).bind(horizon).first<{ payload_json: string }>();

        if (stored?.payload_json) {
          try {
            snapshot = JSON.parse(stored.payload_json) as OpportunitySnapshot;
          } catch {
            snapshot = null;
          }
        }

        if (!snapshot) {
          try {
            snapshot = await buildOpportunitySnapshot(env.DB, horizon);
          } catch (err) {
            console.error('Failed to build opportunity snapshot:', err);
            snapshot = null;
          }
          if (!snapshot) {
            const fallback = buildOpportunityFallbackSnapshot(horizon, 'snapshot_unavailable');
            return Response.json({
              as_of: fallback.as_of,
              horizon: fallback.horizon,
              items: fallback.items,
              degraded_reason: fallback.degraded_reason,
            }, {
              headers: {
                ...corsHeaders,
                'Cache-Control': 'no-store',
              },
            });
          }
          await storeOpportunitySnapshot(env.DB, snapshot);
        }

        return Response.json({
          as_of: snapshot.as_of,
          horizon: snapshot.horizon,
          items: snapshot.items.slice(0, limit),
        }, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'public, max-age=60',
          },
        });
      }

      if (url.pathname === '/api/alerts/feed' && method === 'GET') {
        if (!isFeatureEnabled(env, 'FEATURE_ENABLE_ALERTS_IN_APP', 'ENABLE_ALERTS_IN_APP', true)) {
          return Response.json({
            as_of: new Date().toISOString(),
            alerts: [],
            degraded_reason: 'feature_disabled',
          }, {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'no-store',
            },
          });
        }

        const limit = Math.min(200, Math.max(1, parseInt(url.searchParams.get('limit') || '50', 10)));
        const since = url.searchParams.get('since');
        const rawTypes = (url.searchParams.get('types') || '')
          .split(',')
          .map((value) => value.trim())
          .filter((value) => value.length > 0);
        const types = rawTypes.filter((value): value is MarketAlertType => VALID_ALERT_TYPES.includes(value as MarketAlertType));

        let query = `
          SELECT id, event_type, severity, title, body, entity_type, entity_id, created_at
          FROM market_alert_events
          WHERE 1 = 1
        `;
        const params: (string | number)[] = [];

        if (since) {
          query += ' AND created_at >= ?';
          params.push(since);
        }

        if (types.length > 0) {
          query += ` AND event_type IN (${types.map(() => '?').join(',')})`;
          params.push(...types);
        }

        query += ' ORDER BY created_at DESC LIMIT ?';
        params.push(limit);

        let events: D1Result<{
          id: string;
          event_type: MarketAlertType;
          severity: AlertSeverity;
          title: string;
          body: string;
          entity_type: 'market' | 'theme' | 'indicator';
          entity_id: string | null;
          created_at: string;
        }>;
        try {
          events = await env.DB.prepare(query).bind(...params).all<{
            id: string;
            event_type: MarketAlertType;
            severity: AlertSeverity;
            title: string;
            body: string;
            entity_type: 'market' | 'theme' | 'indicator';
            entity_id: string | null;
            created_at: string;
          }>();
        } catch (err) {
          console.error('Failed to load in-app alerts feed:', err);
          return Response.json({
            as_of: new Date().toISOString(),
            alerts: [],
            degraded_reason: 'query_failed',
          }, {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'no-store',
            },
          });
        }

        return Response.json({
          as_of: new Date().toISOString(),
          alerts: events.results || [],
          degraded_reason: null,
        }, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'public, max-age=30',
          },
        });
      }

      if (url.pathname === '/api/alerts/subscribe/start' && method === 'POST') {
        if (!isFeatureEnabled(env, 'FEATURE_ENABLE_ALERTS_EMAIL', 'ENABLE_ALERTS_EMAIL', true)) {
          return Response.json({ error: 'Email alerts disabled' }, { status: 404, headers: corsHeaders });
        }
        if (!env.RESEND_API_KEY || !env.RESEND_FROM_EMAIL) {
          return Response.json({ error: 'Email service unavailable' }, { status: 503, headers: corsHeaders });
        }

        const body = await parseJsonBody<{ email?: string; types?: string[]; cadence?: string }>(request);
        const email = String(body?.email || '').trim().toLowerCase();
        if (!validateEmail(email)) {
          return Response.json({ error: 'Invalid email' }, { status: 400, headers: corsHeaders });
        }

        const secret = getAlertsSigningSecret(env);
        if (!secret) {
          return Response.json({ error: 'Signing secret unavailable' }, { status: 503, headers: corsHeaders });
        }

        const subscriberId = `sub_${stableHash(`${email}:${Date.now()}:${generateToken(4)}`)}`;
        const cadence = normalizeCadence(body?.cadence);
        const types = normalizeAlertTypes(body?.types);
        const verifyToken = generateToken(18);
        const tokenHash = await hashToken(secret, verifyToken);
        const expiresAt = new Date(Date.now() + (15 * 60 * 1000)).toISOString();

        await env.DB.prepare(`
          INSERT INTO email_subscribers (id, email, status, cadence, types_json, timezone, created_at, updated_at)
          VALUES (?, ?, 'pending', ?, ?, 'America/New_York', datetime('now'), datetime('now'))
          ON CONFLICT(email) DO UPDATE SET
            status = 'pending',
            cadence = excluded.cadence,
            types_json = excluded.types_json,
            updated_at = datetime('now')
        `).bind(subscriberId, email, cadence, JSON.stringify(types)).run();

        await env.DB.prepare(`
          INSERT INTO email_verification_tokens (email, token_hash, expires_at, created_at)
          VALUES (?, ?, ?, datetime('now'))
        `).bind(email, tokenHash, expiresAt).run();

        const verifyUrl = `https://pxicommand.com/inbox?verify_token=${encodeURIComponent(verifyToken)}`;
        const verificationEmail = await sendResendEmail(env, {
          to: email,
          subject: 'Verify your PXI alert subscription',
          text: `Verify your PXI alerts subscription by opening: ${verifyUrl}\n\nThis link expires in 15 minutes.`,
          html: `
            <div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;padding:20px;color:#111;">
              <h2>Verify your PXI alerts subscription</h2>
              <p>Confirm your email to receive daily 8:00 AM ET digest emails.</p>
              <p><a href="${verifyUrl}">Verify subscription</a></p>
              <p style="font-size:12px;color:#555;">This link expires in 15 minutes.</p>
            </div>
          `,
        });

        if (!verificationEmail.ok) {
          return Response.json({ error: verificationEmail.error || 'Verification email failed' }, { status: 503, headers: corsHeaders });
        }

        return Response.json({ ok: true }, { headers: corsHeaders });
      }

      if (url.pathname === '/api/alerts/subscribe/verify' && method === 'POST') {
        if (!isFeatureEnabled(env, 'FEATURE_ENABLE_ALERTS_EMAIL', 'ENABLE_ALERTS_EMAIL', true)) {
          return Response.json({ error: 'Email alerts disabled' }, { status: 404, headers: corsHeaders });
        }

        const body = await parseJsonBody<{ token?: string }>(request);
        const token = String(body?.token || '').trim();
        if (!token) {
          return Response.json({ error: 'Token required' }, { status: 400, headers: corsHeaders });
        }

        const secret = getAlertsSigningSecret(env);
        if (!secret) {
          return Response.json({ error: 'Signing secret unavailable' }, { status: 503, headers: corsHeaders });
        }

        const tokenHash = await hashToken(secret, token);
        const tokenRecord = await env.DB.prepare(`
          SELECT id, email
          FROM email_verification_tokens
          WHERE token_hash = ?
            AND used_at IS NULL
            AND expires_at > datetime('now')
          ORDER BY id DESC
          LIMIT 1
        `).bind(tokenHash).first<{ id: number; email: string }>();

        if (!tokenRecord) {
          return Response.json({ error: 'Invalid or expired token' }, { status: 400, headers: corsHeaders });
        }

        await env.DB.prepare(`
          UPDATE email_verification_tokens
          SET used_at = datetime('now')
          WHERE id = ?
        `).bind(tokenRecord.id).run();

        await env.DB.prepare(`
          UPDATE email_subscribers
          SET status = 'active', updated_at = datetime('now')
          WHERE email = ?
        `).bind(tokenRecord.email).run();

        const subscriber = await env.DB.prepare(`
          SELECT id
          FROM email_subscribers
          WHERE email = ?
          LIMIT 1
        `).bind(tokenRecord.email).first<{ id: string }>();

        if (subscriber?.id) {
          const unsubscribeToken = `${subscriber.id}.${generateToken(8)}`;
          const unsubscribeHash = await hashToken(secret, unsubscribeToken);
          await env.DB.prepare(`
            INSERT OR REPLACE INTO email_unsubscribe_tokens (subscriber_id, token_hash, created_at)
            VALUES (?, ?, datetime('now'))
          `).bind(subscriber.id, unsubscribeHash).run();
        }

        return Response.json({
          ok: true,
          subscriber_status: 'active',
        }, { headers: corsHeaders });
      }

      if (url.pathname === '/api/alerts/unsubscribe' && method === 'POST') {
        const body = await parseJsonBody<{ token?: string }>(request);
        const token = String(body?.token || '').trim();
        const subscriberId = token.split('.')[0];

        if (!token || !subscriberId) {
          return Response.json({ error: 'Token required' }, { status: 400, headers: corsHeaders });
        }

        const secret = getAlertsSigningSecret(env);
        if (!secret) {
          return Response.json({ error: 'Signing secret unavailable' }, { status: 503, headers: corsHeaders });
        }

        const record = await env.DB.prepare(`
          SELECT token_hash
          FROM email_unsubscribe_tokens
          WHERE subscriber_id = ?
        `).bind(subscriberId).first<{ token_hash: string }>();

        if (!record) {
          return Response.json({ error: 'Invalid unsubscribe token' }, { status: 400, headers: corsHeaders });
        }

        const tokenHash = await hashToken(secret, token);
        if (!constantTimeEquals(tokenHash, record.token_hash)) {
          return Response.json({ error: 'Invalid unsubscribe token' }, { status: 400, headers: corsHeaders });
        }

        await env.DB.prepare(`
          UPDATE email_subscribers
          SET status = 'unsubscribed', updated_at = datetime('now')
          WHERE id = ?
        `).bind(subscriberId).run();

        return Response.json({ ok: true }, { headers: corsHeaders });
      }

      if (url.pathname === '/api/market/refresh-products' && method === 'POST') {
        const adminAuthFailure = await enforceAdminAuth(request, env, corsHeaders, clientIP);
        if (adminAuthFailure) {
          return adminAuthFailure;
        }

        const briefEnabled = isFeatureEnabled(env, 'FEATURE_ENABLE_BRIEF', 'ENABLE_BRIEF', true);
        const opportunitiesEnabled = isFeatureEnabled(env, 'FEATURE_ENABLE_OPPORTUNITIES', 'ENABLE_OPPORTUNITIES', true);
        const inAppAlertsEnabled = isFeatureEnabled(env, 'FEATURE_ENABLE_ALERTS_IN_APP', 'ENABLE_ALERTS_IN_APP', true);

        let brief: BriefSnapshot | null = null;
        if (briefEnabled) {
          brief = await buildBriefSnapshot(env.DB);
          if (brief) {
            await storeBriefSnapshot(env.DB, brief);
          }
        }

        let opportunities7d: OpportunitySnapshot | null = null;
        let opportunities30d: OpportunitySnapshot | null = null;
        if (opportunitiesEnabled) {
          opportunities7d = await buildOpportunitySnapshot(env.DB, '7d');
          opportunities30d = await buildOpportunitySnapshot(env.DB, '30d');
          if (opportunities7d) await storeOpportunitySnapshot(env.DB, opportunities7d);
          if (opportunities30d) await storeOpportunitySnapshot(env.DB, opportunities30d);
        }

        let alertsGenerated = 0;
        if (brief && opportunities7d) {
          const generated = await generateMarketEvents(env.DB, brief, opportunities7d);
          alertsGenerated = await insertMarketEvents(env.DB, generated, inAppAlertsEnabled);
        }

        return Response.json({
          ok: true,
          brief_generated: brief ? 1 : 0,
          opportunities_generated: (opportunities7d ? 1 : 0) + (opportunities30d ? 1 : 0),
          alerts_generated: alertsGenerated,
          as_of: brief?.as_of || opportunities7d?.as_of || null,
        }, { headers: corsHeaders });
      }

      if (url.pathname === '/api/market/send-digest' && method === 'POST') {
        const adminAuthFailure = await enforceAdminAuth(request, env, corsHeaders, clientIP);
        if (adminAuthFailure) {
          return adminAuthFailure;
        }

        if (!isFeatureEnabled(env, 'FEATURE_ENABLE_ALERTS_EMAIL', 'ENABLE_ALERTS_EMAIL', true)) {
          return Response.json({
            ok: true,
            skipped: true,
            reason: 'Email alerts disabled',
          }, { headers: corsHeaders });
        }

        if (!env.RESEND_API_KEY || !env.RESEND_FROM_EMAIL) {
          return Response.json({ error: 'Email service unavailable' }, { status: 503, headers: corsHeaders });
        }

        const secret = getAlertsSigningSecret(env);
        if (!secret) {
          return Response.json({ error: 'Signing secret unavailable' }, { status: 503, headers: corsHeaders });
        }

        const [briefRow, opportunityRow, alertRows, subscribers] = await Promise.all([
          env.DB.prepare(`SELECT payload_json FROM market_brief_snapshots ORDER BY as_of DESC LIMIT 1`).first<{ payload_json: string }>(),
          env.DB.prepare(`SELECT payload_json FROM opportunity_snapshots WHERE horizon = '7d' ORDER BY as_of DESC LIMIT 1`).first<{ payload_json: string }>(),
          env.DB.prepare(`
            SELECT id, event_type, severity, title, body, entity_type, entity_id, dedupe_key, payload_json, created_at
            FROM market_alert_events
            WHERE created_at >= datetime('now', '-24 hours')
            ORDER BY created_at DESC
            LIMIT 200
          `).all<MarketAlertEvent>(),
          env.DB.prepare(`
            SELECT id, email, types_json, cadence, status
            FROM email_subscribers
            WHERE status = 'active'
              AND cadence = 'daily_8am_et'
          `).all<{ id: string; email: string; types_json: string; cadence: string; status: string }>(),
        ]);

        let brief: BriefSnapshot | null = null;
        let opportunities: OpportunitySnapshot | null = null;
        try {
          brief = briefRow?.payload_json ? (JSON.parse(briefRow.payload_json) as BriefSnapshot) : null;
        } catch {
          brief = null;
        }
        try {
          opportunities = opportunityRow?.payload_json ? (JSON.parse(opportunityRow.payload_json) as OpportunitySnapshot) : null;
        } catch {
          opportunities = null;
        }
        const events = alertRows.results || [];

        let sentCount = 0;
        let failCount = 0;
        let bounceCount = 0;

        for (const subscriber of subscribers.results || []) {
          let types: MarketAlertType[] = VALID_ALERT_TYPES;
          try {
            types = normalizeAlertTypes(JSON.parse(subscriber.types_json));
          } catch {
            types = VALID_ALERT_TYPES;
          }

          const filteredEvents = events.filter((event) => types.includes(event.event_type));
          const unsubscribeToken = `${subscriber.id}.${generateToken(8)}`;
          const unsubscribeHash = await hashToken(secret, unsubscribeToken);
          await env.DB.prepare(`
            INSERT OR REPLACE INTO email_unsubscribe_tokens (subscriber_id, token_hash, created_at)
            VALUES (?, ?, datetime('now'))
          `).bind(subscriber.id, unsubscribeHash).run();

          let result: { ok: boolean; providerId?: string; error?: string } = { ok: false, error: 'Unknown error' };
          for (let attempt = 1; attempt <= 3; attempt += 1) {
            result = await sendDigestToSubscriber(env, subscriber.email, brief, opportunities, filteredEvents, unsubscribeToken);
            if (result.ok) break;
            const delayMs = attempt * attempt * 300;
            await new Promise((resolve) => setTimeout(resolve, delayMs));
          }

          if (result.ok) {
            sentCount += 1;
            await env.DB.prepare(`
              INSERT INTO market_alert_deliveries
              (event_id, channel, subscriber_id, status, provider_id, attempted_at)
              VALUES (?, 'email', ?, 'sent', ?, datetime('now'))
            `).bind(`digest-${asIsoDate(new Date())}`, subscriber.id, result.providerId || null).run();
          } else {
            failCount += 1;
            const errorText = result.error || 'Delivery failed';
            if (errorText.toLowerCase().includes('bounce')) {
              bounceCount += 1;
              await env.DB.prepare(`
                UPDATE email_subscribers SET status = 'bounced', updated_at = datetime('now')
                WHERE id = ?
              `).bind(subscriber.id).run();
            }
            await env.DB.prepare(`
              INSERT INTO market_alert_deliveries
              (event_id, channel, subscriber_id, status, error, attempted_at)
              VALUES (?, 'email', ?, 'failed', ?, datetime('now'))
            `).bind(`digest-${asIsoDate(new Date())}`, subscriber.id, errorText).run();
          }
        }

        return Response.json({
          ok: true,
          sent_count: sentCount,
          fail_count: failCount,
          bounce_count: bounceCount,
          active_subscribers: subscribers.results?.length || 0,
        }, { headers: corsHeaders });
      }

      // v1.4: Historical data endpoint for charts
      if (url.pathname === '/api/history') {
        const days = Math.min(365, Math.max(7, parseInt(url.searchParams.get('days') || '90')));

        // Get historical PXI scores
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

        // Derive regime from score (fast heuristic to avoid N+1 queries)
        // Score >= 60: RISK_ON, Score <= 40: RISK_OFF, otherwise: TRANSITION
        const dataWithRegimes = historyResult.results.map((row) => ({
          date: row.date,
          score: row.score,
          label: row.label,
          status: row.status,
          regime: row.score >= 60 ? 'RISK_ON' : row.score <= 40 ? 'RISK_OFF' : 'TRANSITION',
        }));

        return Response.json({
          data: dataWithRegimes.reverse(),  // Chronological order
          count: dataWithRegimes.length,
        }, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'public, max-age=300',  // 5 min cache
          },
        });
      }

      // v1.4: Alert history endpoint
      if (url.pathname === '/api/alerts') {
        const limit = Math.min(100, Math.max(10, parseInt(url.searchParams.get('limit') || '50')));
        const alertType = url.searchParams.get('type'); // Optional filter
        const severity = url.searchParams.get('severity'); // Optional filter

        // Build query with optional filters
        let queryStr = `SELECT id, date, alert_type, message, severity, acknowledged,
                               pxi_score, forward_return_7d, forward_return_30d, created_at
                        FROM alerts WHERE 1=1`;
        const params: (string | number)[] = [];

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
          severity: string;
          acknowledged: number;
          pxi_score: number | null;
          forward_return_7d: number | null;
          forward_return_30d: number | null;
          created_at: string;
        }>();

        // Get alert type counts for filters
        const typeCounts = await env.DB.prepare(`
          SELECT alert_type, COUNT(*) as count FROM alerts
          GROUP BY alert_type ORDER BY count DESC
        `).all<{ alert_type: string; count: number }>();

        // Calculate accuracy stats for alerts that have forward returns
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

        return Response.json({
          alerts: (alertsResult.results || []).map(a => ({
            ...a,
            acknowledged: a.acknowledged === 1,
          })),
          count: alertsResult.results?.length || 0,
          filters: {
            types: (typeCounts.results || []).map(t => ({
              type: t.alert_type,
              count: t.count,
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
        }, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'public, max-age=60',
          },
        });
      }

      // v1.5: Category deep-dive endpoint
      if (url.pathname.startsWith('/api/category/')) {
        const category = url.pathname.split('/api/category/')[1];
        const validCategories = ['positioning', 'credit', 'volatility', 'breadth', 'macro', 'global', 'crypto'];

        if (!category || !validCategories.includes(category)) {
          return Response.json({ error: 'Invalid category' }, { status: 400, headers: corsHeaders });
        }

        // Get latest date
        const latestPxi = await env.DB.prepare(
          'SELECT date FROM pxi_scores ORDER BY date DESC LIMIT 1'
        ).first<{ date: string }>();

        if (!latestPxi) {
          return Response.json({ error: 'No data' }, { status: 404, headers: corsHeaders });
        }

        // Get category score
        const categoryScore = await env.DB.prepare(
          'SELECT score, weight FROM category_scores WHERE category = ? AND date = ?'
        ).bind(category, latestPxi.date).first<{ score: number; weight: number }>();

        // Get all indicator scores for this category (need to map indicator_id to category)
        // Indicator to category mapping
        const indicatorCategories: Record<string, string> = {
          'fed_balance_sheet': 'positioning', 'treasury_general_account': 'positioning',
          'reverse_repo': 'positioning', 'm2_yoy': 'positioning',
          'hy_oas_spread': 'credit', 'ig_oas_spread': 'credit', 'yield_curve_2s10s': 'credit',
          'vix': 'volatility', 'vix_term_structure': 'volatility', 'skew': 'volatility',
          'put_call_ratio': 'volatility', 'move_index': 'volatility',
          'sp500_adline': 'breadth', 'sp500_pct_above_200': 'breadth', 'sp500_pct_above_50': 'breadth',
          'nyse_new_highs_lows': 'breadth',
          'ism_manufacturing': 'macro', 'ism_services': 'macro', 'unemployment_claims': 'macro',
          'consumer_sentiment': 'macro', 'aaii_sentiment': 'macro',
          'dxy': 'global', 'copper_gold_ratio': 'global', 'btc_flows': 'global',
          'stablecoin_mcap': 'crypto', 'btc_funding_rate': 'crypto',
        };

        const categoryIndicatorIds = Object.entries(indicatorCategories)
          .filter(([, cat]) => cat === category)
          .map(([id]) => id);

        // Get indicator scores
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

        // Get 90-day category history
        const historyResult = await env.DB.prepare(`
          SELECT date, score FROM category_scores
          WHERE category = ?
          ORDER BY date DESC
          LIMIT 90
        `).bind(category).all<{ date: string; score: number }>();

        // Calculate percentile of current score vs history
        const scores = (historyResult.results || []).map(r => r.score);
        const currentScore = categoryScore?.score || 0;
        const percentileRank = scores.length > 0
          ? (scores.filter(s => s < currentScore).length / scores.length) * 100
          : 50;

        // Indicator display names
        const indicatorNames: Record<string, string> = {
          'fed_balance_sheet': 'Fed Balance Sheet', 'treasury_general_account': 'Treasury General Account',
          'reverse_repo': 'Reverse Repo Facility', 'm2_yoy': 'M2 Money Supply YoY',
          'hy_oas_spread': 'High Yield Spread', 'ig_oas_spread': 'Investment Grade Spread',
          'yield_curve_2s10s': '2s10s Yield Curve',
          'vix': 'VIX', 'vix_term_structure': 'VIX Term Structure', 'skew': 'SKEW Index',
          'put_call_ratio': 'Put/Call Ratio', 'move_index': 'MOVE Index',
          'sp500_adline': 'S&P 500 A/D Line', 'sp500_pct_above_200': '% Above 200 DMA',
          'sp500_pct_above_50': '% Above 50 DMA', 'nyse_new_highs_lows': 'NYSE New Highs-Lows',
          'ism_manufacturing': 'ISM Manufacturing', 'ism_services': 'ISM Services',
          'unemployment_claims': 'Initial Claims', 'consumer_sentiment': 'Consumer Sentiment',
          'aaii_sentiment': 'AAII Bull/Bear',
          'dxy': 'Dollar Index', 'copper_gold_ratio': 'Copper/Gold Ratio', 'btc_flows': 'BTC ETF Flows',
          'stablecoin_mcap': 'Stablecoin Mcap RoC', 'btc_funding_rate': 'BTC Funding Rate',
        };

        return Response.json({
          category,
          date: latestPxi.date,
          score: currentScore,
          weight: categoryScore?.weight || 0,
          percentile_rank: Math.round(percentileRank),
          indicators: (indicatorScoresResult.results || []).map(ind => ({
            id: ind.indicator_id,
            name: indicatorNames[ind.indicator_id] || ind.indicator_id,
            raw_value: ind.raw_value,
            normalized_value: ind.normalized_value,
          })),
          history: (historyResult.results || []).reverse(),
        }, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'public, max-age=300',
          },
        });
      }

      // v1.1: PXI-Signal endpoint - two-layer architecture
      if (url.pathname === '/api/signal') {
        // Get latest PXI data
        const pxi = await env.DB.prepare(
          'SELECT date, score, label, status, delta_1d, delta_7d, delta_30d FROM pxi_scores ORDER BY date DESC LIMIT 1'
        ).first<PXIRow>();

        if (!pxi) {
          return Response.json({ error: 'No data' }, { status: 404, headers: corsHeaders });
        }

        // Get categories
        const cats = await env.DB.prepare(
          'SELECT category, score, weight FROM category_scores WHERE date = ?'
        ).bind(pxi.date).all<CategoryRow>();

        // Get regime
        const regime = await detectRegime(env.DB, pxi.date);

        // Calculate signal
        const signal = await calculatePXISignal(
          env.DB,
          { score: pxi.score, delta_7d: pxi.delta_7d, delta_30d: pxi.delta_30d },
          regime,
          cats.results || []
        );

        const [divergence, freshness, mlSampleSize] = await Promise.all([
          detectDivergence(env.DB, pxi.score, regime),
          computeFreshnessStatus(env.DB),
          fetchPredictionEvaluationSampleSize(env.DB),
        ]);
        const conflictState = resolveConflictState(regime, signal);
        const edgeQuality = computeEdgeQualitySnapshot({
          staleCount: freshness.stale_count,
          mlSampleSize,
          regime,
          conflictState,
          divergenceCount: divergence.alerts.length,
        });

        return Response.json({
          date: pxi.date,
          // PXI-State layer (monitoring)
          state: {
            score: pxi.score,
            label: pxi.label,
            status: pxi.status,
            delta: {
              d1: pxi.delta_1d,
              d7: pxi.delta_7d,
              d30: pxi.delta_30d,
            },
            categories: (cats.results || []).map(c => ({
              name: c.category,
              score: c.score,
              weight: c.weight,
            })),
          },
          // PXI-Signal layer (trading)
          signal: {
            type: signal.signal_type,
            risk_allocation: signal.risk_allocation,
            volatility_percentile: signal.volatility_percentile,
            category_dispersion: signal.category_dispersion,
            adjustments: signal.adjustments,
            conflict_state: conflictState,
          },
          // Regime info
          regime: regime ? {
            type: regime.regime,
            confidence: regime.confidence,
            description: regime.description,
          } : null,
          // Active alerts
          divergence: divergence.has_divergence ? {
            alerts: divergence.alerts,
          } : null,
          edge_quality: edgeQuality,
          freshness_status: freshness,
        }, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'public, max-age=60',
          },
        });
      }

      if (url.pathname === '/api/plan' && method === 'GET') {
        if (!isFeatureEnabled(env, 'FEATURE_ENABLE_PLAN', 'ENABLE_PLAN', true)) {
          return Response.json(buildPlanFallbackPayload('feature_disabled'), {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'no-store',
            },
          });
        }

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

        if (!pxi) {
          pxi = recentScores.results?.[0] || null;
          if (pxi) {
            catResult = await env.DB.prepare(
              'SELECT category, score, weight FROM category_scores WHERE date = ?'
            ).bind(pxi.date).all<CategoryRow>();
          }
        }

        if (!pxi) {
          return Response.json(buildPlanFallbackPayload('no_pxi_data'), {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'no-store',
            },
          });
        }

        const categories = (catResult?.results || []).map((row) => ({ score: row.score }));
        const [regime, freshness, mlSampleSize, riskBand] = await Promise.all([
          detectRegime(env.DB, pxi.date),
          computeFreshnessStatus(env.DB),
          fetchPredictionEvaluationSampleSize(env.DB),
          buildCurrentBucketRiskBands(env.DB, pxi.score),
        ]);

        const signal = await calculatePXISignal(
          env.DB,
          { score: pxi.score, delta_7d: pxi.delta_7d, delta_30d: pxi.delta_30d },
          regime,
          categories,
        );
        const divergence = await detectDivergence(env.DB, pxi.score, regime);
        const conflictState = resolveConflictState(regime, signal);
        const edgeQuality = computeEdgeQualitySnapshot({
          staleCount: freshness.stale_count,
          mlSampleSize,
          regime,
          conflictState,
          divergenceCount: divergence.alerts.length,
        });

        const setupSummary = `PXI ${pxi.score.toFixed(1)} (${pxi.label}); ${signal.signal_type.replace('_', ' ')} posture at ${Math.round(signal.risk_allocation * 100)}% risk budget.${
          freshness.stale_count > 0 ? ` ${freshness.stale_count} stale indicator(s) are penalizing confidence.` : ''
        }`;

        const degradedReasons: string[] = [];
        if (riskBand.d7.sample_size < 20 || riskBand.d30.sample_size < 20) {
          degradedReasons.push('limited_scenario_sample');
        }
        if (freshness.stale_count > 0) degradedReasons.push('stale_inputs');
        if (edgeQuality.label === 'LOW') degradedReasons.push('low_edge_quality');

        const payload: PlanPayload = {
          as_of: `${pxi.date}T00:00:00.000Z`,
          setup_summary: setupSummary,
          action_now: {
            risk_allocation_target: signal.risk_allocation,
            horizon_bias: resolveHorizonBias(signal, regime, edgeQuality.score),
            primary_signal: signal.signal_type,
          },
          edge_quality: edgeQuality,
          risk_band: riskBand,
          invalidation_rules: buildInvalidationRules({
            pxi,
            freshness,
            regime,
            edgeQuality,
          }),
          degraded_reason: degradedReasons.length > 0 ? degradedReasons.join(',') : null,
        };

        return Response.json(payload, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'no-store',
          },
        });
      }

      // AI: Find similar market regimes
      if (url.pathname === '/api/similar' && method === 'GET') {
        try {
          // Get today's PXI data including deltas
          const latestPxi = await env.DB.prepare(
            'SELECT date, score, delta_7d, delta_30d FROM pxi_scores ORDER BY date DESC LIMIT 1'
          ).first<{ date: string; score: number; delta_7d: number | null; delta_30d: number | null }>();

          if (!latestPxi) {
            return Response.json({ error: 'No data' }, { status: 404, headers: corsHeaders });
          }

          // Get indicator values and category scores in parallel
          const [indicators, categories] = await Promise.all([
            env.DB.prepare(`
              SELECT indicator_id, value FROM indicator_values
              WHERE date = ? ORDER BY indicator_id
            `).bind(latestPxi.date).all<IndicatorRow>(),
            env.DB.prepare(`
              SELECT category, score FROM category_scores
              WHERE date = ? ORDER BY category
            `).bind(latestPxi.date).all<{ category: string; score: number }>(),
          ]);

          if (!indicators.results || indicators.results.length === 0) {
            return Response.json({ error: 'No indicators' }, { status: 404, headers: corsHeaders });
          }

          // Create rich embedding text with engineered features
          const embeddingText = generateEmbeddingText({
            indicators: indicators.results,
            pxi: {
              score: latestPxi.score,
              delta_7d: latestPxi.delta_7d,
              delta_30d: latestPxi.delta_30d,
            },
            categories: categories.results || [],
          });

          // Generate embedding using Workers AI
          let embedding;
          try {
            embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
              text: embeddingText,
            });
          } catch (aiError) {
            console.error('Workers AI embedding error:', aiError);
            return Response.json({
              error: 'AI embedding failed',
              details: aiError instanceof Error ? aiError.message : String(aiError)
            }, { status: 503, headers: corsHeaders });
          }

          if (!embedding?.data?.[0]) {
            return Response.json({ error: 'Empty embedding response' }, { status: 500, headers: corsHeaders });
          }

          // Query Vectorize for similar days - get more candidates for filtering
          // Note: max topK is 50 when returnMetadata='all'
          let similar;
          try {
            similar = await env.VECTORIZE.query(embedding.data[0], {
              topK: 50,
              returnMetadata: 'all',
            });
          } catch (vecError) {
            console.error('Vectorize query error:', vecError);
            return Response.json({
              error: 'Vectorize query failed',
              details: vecError instanceof Error ? vecError.message : String(vecError)
            }, { status: 503, headers: corsHeaders });
          }

          // Calculate cutoff date (30 days ago) to exclude recent periods
          const cutoffDate = new Date();
          cutoffDate.setDate(cutoffDate.getDate() - 30);
          const cutoffDateStr = cutoffDate.toISOString().split('T')[0];

          // Filter out recent dates and take top 5
          const filteredMatches = (similar.matches || [])
            .filter((m) => {
              const matchDate = m.metadata?.date as string;
              return matchDate && matchDate < cutoffDateStr;
            })
            .slice(0, 5);

          // Get PXI scores for similar dates
          const similarDates = filteredMatches
            .filter((m) => m.metadata?.date)
            .map((m) => m.metadata!.date as string);

          if (similarDates.length === 0) {
            return Response.json({
              current_date: latestPxi.date,
              cutoff_date: cutoffDateStr,
              similar_periods: [],
              total_matches: similar.matches?.length || 0,
              message: 'No historical periods found (excluding last 30 days). Need more historical data.',
            }, { headers: corsHeaders });
          }

          // Get PXI scores for similar dates
          const historicalScores = await env.DB.prepare(`
            SELECT date, score, label, status
            FROM pxi_scores
            WHERE date IN (${similarDates.map(() => '?').join(',')})
          `).bind(...similarDates).all<PXIRow>();

          // Embedding-era forward returns may be missing for recent dates.
          // Keep them as a secondary fallback only.
          const embeddingReturns = await env.DB.prepare(`
            SELECT date, forward_return_7d, forward_return_30d
            FROM market_embeddings
            WHERE date IN (${similarDates.map(() => '?').join(',')})
          `).bind(...similarDates).all<{ date: string; forward_return_7d: number | null; forward_return_30d: number | null }>();

          const embeddingReturnMap = new Map(
            (embeddingReturns.results || []).map((r) => [r.date, r])
          );

          // Build SPY price lookup for on-demand forward return calculation.
          const spyPrices = await env.DB.prepare(`
            SELECT date, value FROM indicator_values
            WHERE indicator_id = 'spy_close'
            ORDER BY date ASC
          `).all<{ date: string; value: number }>();

          const spyMap = new Map<string, number>();
          for (const p of spyPrices.results || []) {
            spyMap.set(p.date, p.value);
          }

          const getSpyPrice = (dateStr: string, maxDaysForward: number = 5): number | null => {
            const date = new Date(dateStr);
            for (let i = 0; i <= maxDaysForward; i++) {
              const checkDate = new Date(date);
              checkDate.setDate(checkDate.getDate() + i);
              const checkStr = checkDate.toISOString().split('T')[0];
              const price = spyMap.get(checkStr);
              if (price !== undefined) {
                return price;
              }
            }
            return null;
          };

          const calculateForwardReturn = (startDate: string, horizonDays: number): number | null => {
            const start = getSpyPrice(startDate);
            if (start === null) return null;

            const target = new Date(startDate);
            target.setDate(target.getDate() + horizonDays);
            const targetStr = target.toISOString().split('T')[0];
            const end = getSpyPrice(targetStr);
            if (end === null) return null;

            return ((end - start) / start) * 100;
          };

          // Get accuracy scores for similar periods
          const accuracyScores = await env.DB.prepare(`
            SELECT period_date, accuracy_score, times_used FROM period_accuracy
            WHERE period_date IN (${similarDates.map(() => '?').join(',')})
          `).bind(...similarDates).all<{ period_date: string; accuracy_score: number; times_used: number }>();

          const accuracyMap = new Map(
            (accuracyScores.results || []).map(a => [a.period_date, { score: a.accuracy_score, used: a.times_used }])
          );

          const todayMs = new Date().getTime();

          return Response.json({
            current_date: latestPxi.date,
            cutoff_date: cutoffDateStr,
            similar_periods: filteredMatches.map((m) => {
              const hist = historicalScores.results?.find((s) => s.date === m.metadata?.date);
              const matchDate = m.metadata?.date as string;

              // Calculate weight components
              const similarityWeight = m.score;
              const daysSince = matchDate ? (todayMs - new Date(matchDate).getTime()) / (1000 * 60 * 60 * 24) : 0;
              const recencyWeight = Math.exp(-daysSince / 365);
              const periodAccuracy = matchDate ? accuracyMap.get(matchDate) : null;
              const accuracyWeight = periodAccuracy && periodAccuracy.used >= 2 ? periodAccuracy.score : 0.5;
              const combinedWeight = similarityWeight * (0.4 + 0.3 * recencyWeight + 0.3 * accuracyWeight);

              const embeddingReturn = matchDate ? embeddingReturnMap.get(matchDate) : null;
              const computed7d = matchDate ? calculateForwardReturn(matchDate, 7) : null;
              const computed30d = matchDate ? calculateForwardReturn(matchDate, 30) : null;

              return {
                date: matchDate,
                similarity: m.score,
                weights: {
                  combined: combinedWeight,
                  similarity: similarityWeight,
                  recency: recencyWeight,
                  accuracy: accuracyWeight,
                  accuracy_sample: periodAccuracy?.used || 0,
                },
                pxi: hist ? {
                  date: hist.date,
                  score: hist.score,
                  label: hist.label,
                  status: hist.status,
                } : null,
                forward_returns: hist ? {
                  d7: computed7d ?? embeddingReturn?.forward_return_7d ?? null,
                  d30: computed30d ?? embeddingReturn?.forward_return_30d ?? null,
                } : null,
              };
            }),
          }, { headers: corsHeaders });
        } catch (err) {
          console.error('Similar endpoint error:', err);
          return Response.json({
            error: 'Similar search failed',
            details: err instanceof Error ? err.message : String(err)
          }, { status: 500, headers: corsHeaders });
        }
      }

      // AI: Generate embeddings for historical data
      if (url.pathname === '/api/embed' && method === 'POST') {
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
      if (url.pathname === '/api/write' && method === 'POST') {
        const adminAuthFailure = await enforceAdminAuth(request, env, corsHeaders, clientIP);
        if (adminAuthFailure) {
          return adminAuthFailure;
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

      // Manual refresh - fetch fresh data and recalculate (requires auth)
      if (url.pathname === '/api/refresh' && method === 'POST') {
        const adminAuthFailure = await enforceAdminAuth(request, env, corsHeaders, clientIP);
        if (adminAuthFailure) {
          return adminAuthFailure;
        }

        if (!env.FRED_API_KEY) {
          return Response.json({ error: 'FRED_API_KEY not configured' }, { status: 500, headers: corsHeaders });
        }

        // Fetch all indicator data
        const indicators = await fetchAllIndicators(env.FRED_API_KEY);

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

        // Calculate PXI for today
        const today = formatDate(new Date());
        const result = await calculatePXI(env.DB, today);

        if (result) {
          await env.DB.prepare(`
            INSERT OR REPLACE INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d)
            VALUES (?, ?, ?, ?, ?, ?, ?)
          `).bind(
            result.pxi.date, result.pxi.score, result.pxi.label, result.pxi.status,
            result.pxi.delta_1d, result.pxi.delta_7d, result.pxi.delta_30d
          ).run();

          const catStmts = result.categories.map(cat =>
            env.DB.prepare(`
              INSERT OR REPLACE INTO category_scores (category, date, score, weight, weighted_score)
              VALUES (?, ?, ?, ?, ?)
            `).bind(cat.category, cat.date, cat.score, cat.weight, cat.weighted_score)
          );
          if (catStmts.length > 0) {
            await env.DB.batch(catStmts);
          }
        }

        return Response.json({
          success: true,
          indicators_fetched: indicators.length,
          indicators_written: written,
          pxi: result ? { date: today, score: result.pxi.score, label: result.pxi.label, categories: result.categories.length } : null,
        }, { headers: corsHeaders });
      }

      // Recalculate PXI for a given date (requires auth)
      if (url.pathname === '/api/recalculate' && method === 'POST') {
        const adminAuthFailure = await enforceAdminAuth(request, env, corsHeaders, clientIP);
        if (adminAuthFailure) {
          return adminAuthFailure;
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

        // Generate embedding for Vectorize (for similarity search) with engineered features
        let embedded = false;
        try {
          const indicators = await env.DB.prepare(`
            SELECT indicator_id, value FROM indicator_values WHERE date = ? ORDER BY indicator_id
          `).bind(targetDate).all<{ indicator_id: string; value: number }>();

          if (indicators.results && indicators.results.length >= 5) {
            // Generate rich embedding text with engineered features
            const embeddingText = generateEmbeddingText({
              indicators: indicators.results,
              pxi: {
                score: result.pxi.score,
                delta_7d: result.pxi.delta_7d,
                delta_30d: result.pxi.delta_30d,
              },
              categories: result.categories.map(c => ({ category: c.category, score: c.score })),
            });

            const embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
              text: embeddingText,
            });

            await env.VECTORIZE.upsert([{
              id: targetDate,
              values: embedding.data[0],
              metadata: { date: targetDate, score: result.pxi.score, label: result.pxi.label },
            }]);
            embedded = true;

            // Generate and store prediction for tracking
            try {
              const thirtyDaysAgo = new Date();
              thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
              const cutoffDate = formatDate(thirtyDaysAgo);

              const similar = await env.VECTORIZE.query(embedding.data[0], {
                topK: 50,
                returnMetadata: 'all',
              });

              const filteredMatches = (similar.matches || []).filter(m => {
                const matchDate = (m.metadata as any)?.date || m.id;
                return matchDate < cutoffDate;
              }).slice(0, 5);

              if (filteredMatches.length > 0) {
                // Calculate predictions from similar periods
                const outcomes: { d7: number | null; d30: number | null }[] = [];
                for (const match of filteredMatches) {
                  const histDate = match.id;
                  const histScore = (match.metadata as any)?.score || 0;
                  const histDateObj = new Date(histDate);
                  histDateObj.setDate(histDateObj.getDate() + 35);
                  const endDate = formatDate(histDateObj);

                  const futureScores = await env.DB.prepare(`
                    SELECT date, score FROM pxi_scores WHERE date > ? AND date <= ? ORDER BY date LIMIT 35
                  `).bind(histDate, endDate).all<{ date: string; score: number }>();

                  const histDateMs = new Date(histDate).getTime();
                  let d7 = null, d30 = null;
                  for (const fs of futureScores.results || []) {
                    const daysAfter = Math.round((new Date(fs.date).getTime() - histDateMs) / (1000 * 60 * 60 * 24));
                    if (daysAfter >= 5 && daysAfter <= 10 && d7 === null) d7 = fs.score - histScore;
                    if (daysAfter >= 25 && daysAfter <= 35 && d30 === null) d30 = fs.score - histScore;
                  }
                  outcomes.push({ d7, d30 });
                }

                const valid7 = outcomes.filter(o => o.d7 !== null);
                const valid30 = outcomes.filter(o => o.d30 !== null);
                const pred7 = valid7.length > 0 ? valid7.reduce((s, o) => s + o.d7!, 0) / valid7.length : null;
                const pred30 = valid30.length > 0 ? valid30.reduce((s, o) => s + o.d30!, 0) / valid30.length : null;
                const conf7 = valid7.length > 0 ? Math.abs(valid7.filter(o => o.d7! > 0).length - valid7.filter(o => o.d7! < 0).length) / valid7.length : null;
                const conf30 = valid30.length > 0 ? Math.abs(valid30.filter(o => o.d30! > 0).length - valid30.filter(o => o.d30! < 0).length) / valid30.length : null;

                // Calculate target dates
                const targetDate7d = new Date(targetDate);
                targetDate7d.setDate(targetDate7d.getDate() + 7);
                const targetDate30d = new Date(targetDate);
                targetDate30d.setDate(targetDate30d.getDate() + 30);

                await env.DB.prepare(`
                  INSERT OR REPLACE INTO prediction_log
                  (prediction_date, target_date_7d, target_date_30d, current_score, predicted_change_7d, predicted_change_30d, confidence_7d, confidence_30d, similar_periods)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                `).bind(
                  targetDate,
                  formatDate(targetDate7d),
                  formatDate(targetDate30d),
                  result.pxi.score,
                  pred7,
                  pred30,
                  conf7,
                  conf30,
                  JSON.stringify(filteredMatches.map(m => m.id))
                ).run();
              }
            } catch (predErr) {
              console.error('Prediction logging failed:', predErr);
            }
          }
        } catch (e) {
          console.error('Embedding generation failed:', e);
        }

        return Response.json({
          success: true,
          date: targetDate,
          score: result.pxi.score,
          label: result.pxi.label,
          categories: result.categories.length,
          embedded,
        }, { headers: corsHeaders });
      }

      // Backfill historical PXI scores and embeddings (requires auth)
      if (url.pathname === '/api/backfill' && method === 'POST') {
        const adminAuthFailure = await enforceAdminAuth(request, env, corsHeaders, clientIP);
        if (adminAuthFailure) {
          return adminAuthFailure;
        }

        let body: { start?: string; end?: string; limit?: number };
        try {
          body = await request.json() as { start?: string; end?: string; limit?: number };
        } catch {
          return Response.json(
            { error: 'Invalid JSON body' },
            { status: 400, headers: corsHeaders }
          );
        }
        const limit = parseBackfillLimit(body.limit);
        let dateFilter: ReturnType<typeof parseBackfillDateRange>;
        try {
          dateFilter = parseBackfillDateRange(body.start, body.end);
        } catch (err) {
          return Response.json({
            error: err instanceof Error ? err.message : 'Invalid date range',
          }, { status: 400, headers: corsHeaders });
        }

        // Get all unique dates with indicator data that don't have PXI scores yet
        const dateClauses: string[] = [];
        const dateParams: string[] = [];

        if (dateFilter.start) {
          dateClauses.push('iv.date >= ?');
          dateParams.push(dateFilter.start);
        }

        if (dateFilter.end) {
          dateClauses.push('iv.date <= ?');
          dateParams.push(dateFilter.end);
        }

        const remainingFilterClause = dateClauses.length > 0
          ? `AND ${dateClauses.join(' AND ')}`
          : '';

        const remainingParams = [...dateParams];

        const datesResult = await env.DB.prepare(`
          SELECT DISTINCT iv.date
          FROM indicator_values iv
          LEFT JOIN pxi_scores ps ON iv.date = ps.date
          WHERE ps.date IS NULL
          ${remainingFilterClause}
          ORDER BY iv.date DESC
          LIMIT ?
        `).bind(...remainingParams, limit).all<{ date: string }>();

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

            // Generate embedding for Vectorize with engineered features
            try {
              const indicators = await env.DB.prepare(`
                SELECT indicator_id, value FROM indicator_values WHERE date = ? ORDER BY indicator_id
              `).bind(date).all<{ indicator_id: string; value: number }>();

              if (indicators.results && indicators.results.length >= 5) {
                // Generate rich embedding text with engineered features
                const embeddingText = generateEmbeddingText({
                  indicators: indicators.results,
                  pxi: {
                    score: result.pxi.score,
                    delta_7d: result.pxi.delta_7d,
                    delta_30d: result.pxi.delta_30d,
                  },
                  categories: result.categories.map(c => ({ category: c.category, score: c.score })),
                });

                const embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
                  text: embeddingText,
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
          ${remainingFilterClause}
        `).bind(...remainingParams).first<{ cnt: number }>();

        return Response.json({
          success: true,
          processed,
          succeeded,
          embedded,
          remaining: remainingResult?.cnt || 0,
          results,
        }, { headers: corsHeaders });
      }

      // Predict SPY returns based on empirical backtest data
      if (url.pathname === '/api/predict' && method === 'GET') {
        // Get current PXI score
        const currentPxi = await env.DB.prepare(
          'SELECT date, score, label FROM pxi_scores ORDER BY date DESC LIMIT 1'
        ).first<{ date: string; score: number; label: string }>();

        if (!currentPxi) {
          return Response.json({ error: 'No PXI data' }, { status: 404, headers: corsHeaders });
        }

        // Get all historical PXI scores with SPY forward returns
        const pxiScores = await env.DB.prepare(
          'SELECT date, score FROM pxi_scores ORDER BY date ASC'
        ).all<{ date: string; score: number }>();

        const spyPrices = await env.DB.prepare(`
          SELECT date, value FROM indicator_values
          WHERE indicator_id = 'spy_close' ORDER BY date ASC
        `).all<{ date: string; value: number }>();

        // Build SPY price map
        const spyMap = new Map<string, number>();
        for (const p of spyPrices.results || []) {
          spyMap.set(p.date, p.value);
        }

        // Helper to get SPY price (handling weekends)
        const getSpyPrice = (dateStr: string, maxDays = 5): number | null => {
          const date = new Date(dateStr);
          for (let i = 0; i <= maxDays; i++) {
            const check = new Date(date);
            check.setDate(check.getDate() + i);
            const key = check.toISOString().split('T')[0];
            if (spyMap.has(key)) return spyMap.get(key)!;
          }
          return null;
        };

        // Fetch adaptive bucket thresholds from model_params
        const thresholdParams = await env.DB.prepare(`
          SELECT param_key, param_value FROM model_params
          WHERE param_key LIKE 'bucket_threshold_%'
        `).all<{ param_key: string; param_value: number }>();

        const thresholds = {
          t1: 20, t2: 40, t3: 60, t4: 80 // defaults
        };
        for (const p of thresholdParams.results || []) {
          if (p.param_key === 'bucket_threshold_1') thresholds.t1 = p.param_value;
          if (p.param_key === 'bucket_threshold_2') thresholds.t2 = p.param_value;
          if (p.param_key === 'bucket_threshold_3') thresholds.t3 = p.param_value;
          if (p.param_key === 'bucket_threshold_4') thresholds.t4 = p.param_value;
        }

        // Helper to get bucket from score using adaptive thresholds
        const getBucket = (s: number): string => {
          if (s < thresholds.t1) return `0-${thresholds.t1}`;
          if (s < thresholds.t2) return `${thresholds.t1}-${thresholds.t2}`;
          if (s < thresholds.t3) return `${thresholds.t2}-${thresholds.t3}`;
          if (s < thresholds.t4) return `${thresholds.t3}-${thresholds.t4}`;
          return `${thresholds.t4}-100`;
        };

        // Determine current bucket using adaptive thresholds
        const score = currentPxi.score;
        const bucket = getBucket(score);

        // Calculate stats for current bucket from historical data
        const bucketReturns7d: number[] = [];
        const bucketReturns30d: number[] = [];

        for (const pxi of pxiScores.results || []) {
          // Check if this PXI is in the same bucket
          const pxiBucket = getBucket(pxi.score);

          if (pxiBucket !== bucket) continue;

          // Calculate forward returns
          const spyNow = getSpyPrice(pxi.date);
          if (!spyNow) continue;

          const date = new Date(pxi.date);
          const date7d = new Date(date); date7d.setDate(date7d.getDate() + 7);
          const date30d = new Date(date); date30d.setDate(date30d.getDate() + 30);

          const spy7d = getSpyPrice(date7d.toISOString().split('T')[0]);
          const spy30d = getSpyPrice(date30d.toISOString().split('T')[0]);

          if (spy7d) bucketReturns7d.push(((spy7d - spyNow) / spyNow) * 100);
          if (spy30d) bucketReturns30d.push(((spy30d - spyNow) / spyNow) * 100);
        }

        // Calculate bucket statistics
        const avg = (arr: number[]) => arr.length > 0 ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
        const winRate = (arr: number[]) => arr.length > 0 ? (arr.filter(r => r > 0).length / arr.length) * 100 : null;
        const median = (arr: number[]) => {
          if (arr.length === 0) return null;
          const sorted = [...arr].sort((a, b) => a - b);
          const mid = Math.floor(sorted.length / 2);
          return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
        };

        // Check for extreme readings using adaptive thresholds
        // Extreme low = below midpoint of first bucket, Extreme high = above midpoint of last bucket
        const extremeLowThreshold = thresholds.t1 * 1.25; // e.g., 25 for t1=20
        const extremeHighThreshold = thresholds.t4 - (100 - thresholds.t4) * 0.25; // e.g., 75 for t4=80
        const isExtremeLow = score < extremeLowThreshold;
        const isExtremeHigh = score > extremeHighThreshold;

        // Calculate extreme stats if applicable
        let extremeStats = null;
        if (isExtremeLow || isExtremeHigh) {
          const extremeReturns7d: number[] = [];
          const extremeReturns30d: number[] = [];

          for (const pxi of pxiScores.results || []) {
            const inRange = isExtremeLow ? pxi.score < extremeLowThreshold : pxi.score > extremeHighThreshold;
            if (!inRange) continue;

            const spyNow = getSpyPrice(pxi.date);
            if (!spyNow) continue;

            const date = new Date(pxi.date);
            const date7d = new Date(date); date7d.setDate(date7d.getDate() + 7);
            const date30d = new Date(date); date30d.setDate(date30d.getDate() + 30);

            const spy7d = getSpyPrice(date7d.toISOString().split('T')[0]);
            const spy30d = getSpyPrice(date30d.toISOString().split('T')[0]);

            if (spy7d) extremeReturns7d.push(((spy7d - spyNow) / spyNow) * 100);
            if (spy30d) extremeReturns30d.push(((spy30d - spyNow) / spyNow) * 100);
          }

          extremeStats = {
            type: isExtremeLow ? 'OVERSOLD' : 'OVERBOUGHT',
            threshold: isExtremeLow ? `<${extremeLowThreshold.toFixed(0)}` : `>${extremeHighThreshold.toFixed(0)}`,
            historical_count: extremeReturns7d.length,
            avg_return_7d: avg(extremeReturns7d),
            avg_return_30d: avg(extremeReturns30d),
            win_rate_7d: winRate(extremeReturns7d),
            win_rate_30d: winRate(extremeReturns30d),
            signal: isExtremeLow ? 'BULLISH' : 'BEARISH',
          };
        }

        // Get ML prediction from prediction_log (if available)
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

        // Calculate confidence label
        const getConfidenceLabel = (conf: number | null) => {
          if (conf === null) return 'N/A';
          if (conf >= 0.7) return 'HIGH';
          if (conf >= 0.4) return 'MEDIUM';
          return 'LOW';
        };

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
              `${thresholds.t4}-100`
            ],
            values: thresholds,
          },
          prediction: {
            method: 'empirical_backtest',
            d7: {
              avg_return: avg(bucketReturns7d),
              median_return: median(bucketReturns7d),
              win_rate: winRate(bucketReturns7d),
              sample_size: bucketReturns7d.length,
            },
            d30: {
              avg_return: avg(bucketReturns30d),
              median_return: median(bucketReturns30d),
              win_rate: winRate(bucketReturns30d),
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
            similar_periods_count: mlPrediction.similar_periods
              ? JSON.parse(mlPrediction.similar_periods).length
              : 0,
          } : null,
          extreme_reading: extremeStats,
          interpretation: {
            bias: (winRate(bucketReturns7d) || 50) > 55 ? 'BULLISH' : (winRate(bucketReturns7d) || 50) < 45 ? 'BEARISH' : 'NEUTRAL',
            confidence: bucketReturns7d.length >= 50 ? 'HIGH' : bucketReturns7d.length >= 20 ? 'MEDIUM' : 'LOW',
            ml_confidence: mlPrediction ? getConfidenceLabel(mlPrediction.confidence_7d) : null,
            note: isExtremeLow
              ? 'Oversold readings have historically preceded rallies'
              : isExtremeHigh
              ? 'Extended readings often see mean reversion'
              : (winRate(bucketReturns7d) || 50) > 55
              ? `At this level, markets rose ${Math.round(winRate(bucketReturns7d) || 0)}% of the time`
              : (winRate(bucketReturns7d) || 50) < 45
              ? `At this level, markets fell ${Math.round(100 - (winRate(bucketReturns7d) || 0))}% of the time`
              : 'Mixed historical outcomes at this level',
          },
        }, { headers: corsHeaders });
      }

      // AI: Analyze current regime
      if (url.pathname === '/api/analyze' && method === 'GET') {
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

      // Evaluate past predictions against actual results
      if (url.pathname === '/api/evaluate' && method === 'POST') {
        const adminAuthFailure = await enforceAdminAuth(request, env, corsHeaders, clientIP);
        if (adminAuthFailure) {
          return adminAuthFailure;
        }

        // Get all predictions that haven't been evaluated yet
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

        const today = new Date().toISOString().split('T')[0];
        let evaluated = 0;

        for (const pred of pendingPredictions.results || []) {
          let needsUpdate = false;
          let actual7d = pred.actual_change_7d;
          let actual30d = pred.actual_change_30d;

          // Check if we can evaluate 7d prediction
          if (pred.target_date_7d && pred.target_date_7d <= today && actual7d === null) {
            const score7d = await env.DB.prepare(
              'SELECT score FROM pxi_scores WHERE date = ?'
            ).bind(pred.target_date_7d).first<{ score: number }>();

            if (score7d) {
              actual7d = score7d.score - pred.current_score;
              needsUpdate = true;
            }
          }

          // Check if we can evaluate 30d prediction
          if (pred.target_date_30d && pred.target_date_30d <= today && actual30d === null) {
            const score30d = await env.DB.prepare(
              'SELECT score FROM pxi_scores WHERE date = ?'
            ).bind(pred.target_date_30d).first<{ score: number }>();

            if (score30d) {
              actual30d = score30d.score - pred.current_score;
              needsUpdate = true;
            }
          }

          // Update if we have new actual values
          if (needsUpdate) {
            const fullyEvaluated = (actual7d !== null || pred.predicted_change_7d === null) &&
                                   (actual30d !== null || pred.predicted_change_30d === null);

            await env.DB.prepare(`
              UPDATE prediction_log
              SET actual_change_7d = ?, actual_change_30d = ?, evaluated_at = ?
              WHERE id = ?
            `).bind(
              actual7d,
              actual30d,
              fullyEvaluated ? new Date().toISOString() : null,
              pred.id
            ).run();
            evaluated++;
          }
        }

        return Response.json({
          success: true,
          pending: pendingPredictions.results?.length || 0,
          evaluated,
        }, { headers: corsHeaders });
      }

      // XGBoost ML prediction endpoint
      if (url.pathname === '/api/ml/predict' && method === 'GET') {
        // Load ML model from KV
        const model = await loadMLModel(env.ML_MODELS);
        if (!model) {
          return Response.json({
            error: 'ML model not loaded',
            message: 'Model has not been uploaded to KV yet',
          }, { status: 503, headers: corsHeaders });
        }

        // Get current PXI data with deltas
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

        // Get category scores and indicators
        const [categories, indicators, recentScores] = await Promise.all([
          env.DB.prepare(`
            SELECT category, score FROM category_scores WHERE date = ?
          `).bind(currentPxi.date).all<{ category: string; score: number }>(),
          env.DB.prepare(`
            SELECT indicator_id, value FROM indicator_values WHERE date = ?
          `).bind(currentPxi.date).all<{ indicator_id: string; value: number }>(),
          env.DB.prepare(`
            SELECT score FROM pxi_scores ORDER BY date DESC LIMIT 20
          `).all<{ score: number }>(),
        ]);

        // Calculate rolling features
        const scores = (recentScores.results || []).map(r => r.score);
        const pxi_ma_5 = scores.slice(0, 5).reduce((a, b) => a + b, 0) / Math.min(5, scores.length);
        const pxi_ma_20 = scores.reduce((a, b) => a + b, 0) / scores.length;
        const mean = pxi_ma_20;
        const pxi_std_20 = Math.sqrt(scores.reduce((sum, s) => sum + Math.pow(s - mean, 2), 0) / scores.length);

        // Build category and indicator maps
        const categoryMap: Record<string, number> = {};
        for (const c of categories.results || []) {
          categoryMap[c.category] = c.score;
        }

        const indicatorMap: Record<string, number> = {};
        for (const i of indicators.results || []) {
          indicatorMap[i.indicator_id] = i.value;
        }

        // Extract ML features
        const features = extractMLFeatures({
          pxi_score: currentPxi.score,
          pxi_delta_1d: currentPxi.delta_1d,
          pxi_delta_7d: currentPxi.delta_7d,
          pxi_delta_30d: currentPxi.delta_30d,
          categories: categoryMap,
          indicators: indicatorMap,
          pxi_ma_5,
          pxi_ma_20,
          pxi_std_20,
        });

        // Run predictions
        const prediction_7d = model.m['7d'] ? xgbPredict(model.m['7d'], features, model.f) : null;
        const prediction_30d = model.m['30d'] ? xgbPredict(model.m['30d'], features, model.f) : null;

        // Interpret predictions
        const interpret = (pred: number | null) => {
          if (pred === null) return null;
          if (pred > 5) return 'STRONG_UP';
          if (pred > 2) return 'UP';
          if (pred > -2) return 'FLAT';
          if (pred > -5) return 'DOWN';
          return 'STRONG_DOWN';
        };

        return Response.json({
          date: currentPxi.date,
          current_score: currentPxi.score,
          model_version: model.v,
          predictions: {
            pxi_change_7d: {
              value: prediction_7d,
              direction: interpret(prediction_7d),
            },
            pxi_change_30d: {
              value: prediction_30d,
              direction: interpret(prediction_30d),
            },
          },
          features_used: Object.keys(features).length,
          // Include key features for transparency
          key_features: {
            extreme_low: features['extreme_low'],
            extreme_high: features['extreme_high'],
            pxi_vs_ma_20: features['pxi_vs_ma_20'],
            category_dispersion: features['category_dispersion'],
            weak_categories_count: features['weak_categories_count'],
          },
        }, { headers: corsHeaders });
      }

      // LSTM sequence model prediction endpoint
      if (url.pathname === '/api/ml/lstm' && method === 'GET') {
        // Get recent PXI scores using a fresh prepare
        const pxiHistory = await env.DB.prepare(
          'SELECT date, score, delta_7d FROM pxi_scores ORDER BY date DESC LIMIT 20'
        ).all<{
          date: string;
          score: number;
          delta_7d: number | null;
        }>();

        // Load LSTM model from KV
        const model = await loadLSTMModel(env.ML_MODELS);
        if (!model) {
          return Response.json({
            error: 'LSTM model not loaded',
            message: 'Model has not been uploaded to KV yet',
          }, { status: 503, headers: corsHeaders });
        }

        const seqLength = model.c.s;  // Usually 20

        if (!pxiHistory.results || pxiHistory.results.length < seqLength) {
          return Response.json({
            error: 'Insufficient history',
            message: `Need ${seqLength} days, have ${pxiHistory.results?.length || 0}`,
          }, { status: 400, headers: corsHeaders });
        }

        // Get dates in order (most recent first)
        const dates = pxiHistory.results.map(r => r.date);

        // Build initial byDate map
        const byDate = new Map<string, {
          score: number;
          delta_7d: number | null;
          categories: Record<string, number>;
        }>();

        for (const row of pxiHistory.results) {
          byDate.set(row.date, { score: row.score, delta_7d: row.delta_7d, categories: {} });
        }

        // Fetch categories for these dates
        const categoryData = await env.DB.prepare(`
          SELECT date, category, score
          FROM category_scores
          WHERE date IN (${dates.map(() => '?').join(',')})
        `).bind(...dates).all<{ date: string; category: string; score: number }>();

        for (const row of categoryData.results || []) {
          if (byDate.has(row.date)) {
            byDate.get(row.date)!.categories[row.category] = row.score;
          }
        }

        // Get VIX for each date
        const vixData = await env.DB.prepare(`
          SELECT date, value FROM indicator_values
          WHERE indicator_id = 'vix' AND date IN (${dates.map(() => '?').join(',')})
        `).bind(...dates).all<{ date: string; value: number }>();

        const vixMap: Record<string, number> = {};
        for (const v of vixData.results || []) {
          vixMap[v.date] = v.value;
        }

        // Build sequence (oldest to newest for LSTM)
        const sortedDates = [...dates].sort();  // Chronological (copy to avoid mutating original)
        const sequence: number[][] = [];

        for (const date of sortedDates) {
          const day = byDate.get(date)!;
          const features = extractLSTMFeatures(
            { ...day, vix: vixMap[date] },
            model.n,
            model.c.f
          );
          sequence.push(features);
        }

        // Run predictions
        const pred_7d = model.m['7d']
          ? lstmForward(sequence, model.m['7d'].lstm, model.m['7d'].fc, model.c.h)
          : null;
        const pred_30d = model.m['30d']
          ? lstmForward(sequence, model.m['30d'].lstm, model.m['30d'].fc, model.c.h)
          : null;

        // Interpret predictions
        const interpret = (pred: number | null) => {
          if (pred === null) return null;
          if (pred > 5) return 'STRONG_UP';
          if (pred > 2) return 'UP';
          if (pred > -2) return 'FLAT';
          if (pred > -5) return 'DOWN';
          return 'STRONG_DOWN';
        };

        const currentDate = dates[0];  // Most recent
        const currentPxi = byDate.get(currentDate)!;

        return Response.json({
          date: currentDate,
          current_score: currentPxi.score,
          model_type: 'lstm',
          model_version: model.v,
          sequence_length: seqLength,
          predictions: {
            pxi_change_7d: {
              value: pred_7d,
              direction: interpret(pred_7d),
            },
            pxi_change_30d: {
              value: pred_30d,
              direction: interpret(pred_30d),
            },
          },
          features_used: model.c.f.length,
          feature_names: model.c.f,
        }, { headers: corsHeaders });
      }

      // SPY Return Prediction endpoint - predicts actual market returns (ALPHA model)
      if (url.pathname === '/api/predict/returns' && method === 'GET') {
        const model = await loadSPYReturnModel(env.ML_MODELS);
        if (!model) {
          return Response.json({
            error: 'SPY return model not loaded',
            message: 'Model has not been uploaded to KV yet',
          }, { status: 503, headers: corsHeaders });
        }

        // Get current PXI data with deltas
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

        // Get category scores, indicators, and recent PXI scores
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

        // Calculate rolling features
        const scores = (recentScores.results || []).map(r => r.score);
        const pxi_ma_5 = scores.slice(0, 5).reduce((a, b) => a + b, 0) / Math.min(5, scores.length);
        const pxi_ma_20 = scores.reduce((a, b) => a + b, 0) / scores.length;
        const pxi_std_20 = Math.sqrt(scores.reduce((sum, s) => sum + Math.pow(s - pxi_ma_20, 2), 0) / scores.length);
        const pxi_vs_ma_20 = currentPxi.score - pxi_ma_20;

        // VIX features
        const vixValues = (vixHistory.results || []).map(r => r.value);
        const vix = vixValues[0] || 0;
        const vix_ma_20 = vixValues.length > 0 ? vixValues.reduce((a, b) => a + b, 0) / vixValues.length : 0;
        const vix_vs_ma = vix - vix_ma_20;

        // Build category map
        const categoryMap: Record<string, number> = {};
        for (const c of categories.results || []) {
          categoryMap[c.category] = c.score;
        }

        // Build indicator map
        const indicatorMap: Record<string, number> = {};
        for (const i of indicators.results || []) {
          indicatorMap[i.indicator_id] = i.value;
        }

        // Category stats
        const catValues = Object.values(categoryMap);
        const category_mean = catValues.length > 0 ? catValues.reduce((a, b) => a + b, 0) / catValues.length : 50;
        const category_max = catValues.length > 0 ? Math.max(...catValues) : 50;
        const category_min = catValues.length > 0 ? Math.min(...catValues) : 50;
        const category_dispersion = category_max - category_min;
        const category_std = catValues.length > 0 ? Math.sqrt(catValues.reduce((sum, v) => sum + Math.pow(v - category_mean, 2), 0) / catValues.length) : 0;
        const strong_categories = catValues.filter(v => v > 70).length;
        const weak_categories = catValues.filter(v => v < 30).length;

        // Build features matching the Python training script
        const features: Record<string, number> = {
          // PXI features
          pxi_score: currentPxi.score,
          pxi_delta_1d: currentPxi.delta_1d ?? 0,
          pxi_delta_7d: currentPxi.delta_7d ?? 0,
          pxi_delta_30d: currentPxi.delta_30d ?? 0,
          pxi_bucket: currentPxi.score < 20 ? 0 : currentPxi.score < 40 ? 1 : currentPxi.score < 60 ? 2 : currentPxi.score < 80 ? 3 : 4,

          // Momentum signals
          momentum_7d_signal: (currentPxi.delta_7d ?? 0) > 5 ? 2 : (currentPxi.delta_7d ?? 0) > 2 ? 1 : (currentPxi.delta_7d ?? 0) > -2 ? 0 : (currentPxi.delta_7d ?? 0) > -5 ? -1 : -2,
          momentum_30d_signal: (currentPxi.delta_30d ?? 0) > 10 ? 2 : (currentPxi.delta_30d ?? 0) > 4 ? 1 : (currentPxi.delta_30d ?? 0) > -4 ? 0 : (currentPxi.delta_30d ?? 0) > -10 ? -1 : -2,

          // Acceleration
          acceleration: (currentPxi.delta_7d ?? 0) - ((currentPxi.delta_30d ?? 0) / 4.3),
          acceleration_signal: 0,

          // Category features
          cat_breadth: categoryMap['breadth'] ?? 50,
          cat_credit: categoryMap['credit'] ?? 50,
          cat_crypto: categoryMap['crypto'] ?? 50,
          cat_global: categoryMap['global'] ?? 50,
          cat_liquidity: categoryMap['liquidity'] ?? 50,
          cat_macro: categoryMap['macro'] ?? 50,
          cat_positioning: categoryMap['positioning'] ?? 50,
          cat_volatility: categoryMap['volatility'] ?? 50,
          category_mean,
          category_dispersion,
          category_std,
          strong_categories,
          weak_categories,

          // Indicators
          vix,
          hy_spread: indicatorMap['hy_oas'] ?? 0,
          ig_spread: indicatorMap['ig_oas'] ?? 0,
          breadth_ratio: indicatorMap['rsp_spy_ratio'] ?? 1,
          yield_curve: indicatorMap['yield_curve_2s10s'] ?? 0,
          dxy: indicatorMap['dxy'] ?? 100,
          btc_vs_200d: indicatorMap['btc_vs_200d'] ?? 0,

          // Derived features
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

        // Set acceleration signal
        features.acceleration_signal = features.acceleration > 2 ? 1 : features.acceleration < -2 ? -1 : 0;

        // Run predictions
        const return_7d = predictSPYReturn(model, features, '7d');
        const return_30d = predictSPYReturn(model, features, '30d');

        // Interpret predictions (for market returns)
        const interpretReturn = (ret: number) => {
          if (ret > 3) return 'STRONG_BULLISH';
          if (ret > 1) return 'BULLISH';
          if (ret > -1) return 'NEUTRAL';
          if (ret > -3) return 'BEARISH';
          return 'STRONG_BEARISH';
        };

        return Response.json({
          date: currentPxi.date,
          current_pxi: currentPxi.score,
          model_created: model.created_at,
          predictions: {
            spy_return_7d: {
              value: Math.round(return_7d * 100) / 100,
              outlook: interpretReturn(return_7d),
              unit: '%',
            },
            spy_return_30d: {
              value: Math.round(return_30d * 100) / 100,
              outlook: interpretReturn(return_30d),
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

      // Ensemble prediction endpoint - combines XGBoost and LSTM
      if (url.pathname === '/api/ml/ensemble' && method === 'GET') {
        // Load both models
        const [xgboostModel, lstmModel] = await Promise.all([
          loadMLModel(env.ML_MODELS),
          loadLSTMModel(env.ML_MODELS),
        ]);

        // Get current PXI and recent history
        const currentPxi = await env.DB.prepare(
          'SELECT date, score, delta_1d, delta_7d, delta_30d FROM pxi_scores ORDER BY date DESC LIMIT 1'
        ).first<{ date: string; score: number; delta_1d: number | null; delta_7d: number | null; delta_30d: number | null }>();

        if (!currentPxi) {
          return Response.json({ error: 'No PXI data available' }, { status: 404, headers: corsHeaders });
        }

        const currentDate = currentPxi.date;
        const currentScore = currentPxi.score;

        // XGBoost prediction
        let xgboost: { pred_7d: number | null; pred_30d: number | null; dir_7d: string | null; dir_30d: string | null } | null = null;
        if (xgboostModel) {
          const [categories, indicators, recentScores] = await Promise.all([
            env.DB.prepare('SELECT category, score FROM category_scores WHERE date = ?').bind(currentDate).all<{ category: string; score: number }>(),
            env.DB.prepare('SELECT indicator_id, value FROM indicator_values WHERE date = ?').bind(currentDate).all<{ indicator_id: string; value: number }>(),
            env.DB.prepare('SELECT score FROM pxi_scores ORDER BY date DESC LIMIT 20').all<{ score: number }>(),
          ]);

          const scores = (recentScores.results || []).map(r => r.score);
          const pxi_ma_5 = scores.slice(0, 5).reduce((a, b) => a + b, 0) / Math.min(5, scores.length);
          const pxi_ma_20 = scores.reduce((a, b) => a + b, 0) / scores.length;
          const pxi_std_20 = Math.sqrt(scores.reduce((sum, s) => sum + Math.pow(s - pxi_ma_20, 2), 0) / scores.length);

          const categoryMap: Record<string, number> = {};
          for (const c of categories.results || []) categoryMap[c.category] = c.score;

          const indicatorMap: Record<string, number> = {};
          for (const i of indicators.results || []) indicatorMap[i.indicator_id] = i.value;

          const features = extractMLFeatures({
            pxi_score: currentPxi.score,
            pxi_delta_1d: currentPxi.delta_1d,
            pxi_delta_7d: currentPxi.delta_7d,
            pxi_delta_30d: currentPxi.delta_30d,
            categories: categoryMap,
            indicators: indicatorMap,
            pxi_ma_5,
            pxi_ma_20,
            pxi_std_20,
          });

          const pred_7d = xgboostModel.m['7d'] ? xgbPredict(xgboostModel.m['7d'], features, xgboostModel.f) : null;
          const pred_30d = xgboostModel.m['30d'] ? xgbPredict(xgboostModel.m['30d'], features, xgboostModel.f) : null;

          const interpretDir = (p: number | null) => {
            if (p === null) return null;
            if (p > 5) return 'STRONG_UP';
            if (p > 2) return 'UP';
            if (p > -2) return 'FLAT';
            if (p > -5) return 'DOWN';
            return 'STRONG_DOWN';
          };

          xgboost = { pred_7d, pred_30d, dir_7d: interpretDir(pred_7d), dir_30d: interpretDir(pred_30d) };
        }

        // LSTM prediction
        let lstm: { pred_7d: number | null; pred_30d: number | null; dir_7d: string | null; dir_30d: string | null } | null = null;
        if (lstmModel) {
          const seqLength = lstmModel.c.s;

          const pxiHistory = await env.DB.prepare(
            'SELECT date, score, delta_7d FROM pxi_scores ORDER BY date DESC LIMIT ?'
          ).bind(seqLength).all<{ date: string; score: number; delta_7d: number | null }>();

          if (pxiHistory.results && pxiHistory.results.length >= seqLength) {
            const dates = pxiHistory.results.map(r => r.date);

            const [categoryData, vixData] = await Promise.all([
              env.DB.prepare(`SELECT date, category, score FROM category_scores WHERE date IN (${dates.map(() => '?').join(',')})`).bind(...dates).all<{ date: string; category: string; score: number }>(),
              env.DB.prepare(`SELECT date, value FROM indicator_values WHERE indicator_id = 'vix' AND date IN (${dates.map(() => '?').join(',')})`).bind(...dates).all<{ date: string; value: number }>(),
            ]);

            const byDate = new Map<string, { score: number; delta_7d: number | null; categories: Record<string, number> }>();
            for (const row of pxiHistory.results) {
              byDate.set(row.date, { score: row.score, delta_7d: row.delta_7d, categories: {} });
            }
            for (const row of categoryData.results || []) {
              if (byDate.has(row.date)) byDate.get(row.date)!.categories[row.category] = row.score;
            }

            const vixMap: Record<string, number> = {};
            for (const v of vixData.results || []) vixMap[v.date] = v.value;

            const sortedDates = [...dates].sort();
            const sequence: number[][] = [];

            for (const date of sortedDates) {
              const day = byDate.get(date);
              if (day) {
                const features = extractLSTMFeatures({ ...day, vix: vixMap[date] }, lstmModel.n, lstmModel.c.f);
                sequence.push(features);
              }
            }

            if (sequence.length === seqLength) {
              const pred_7d = lstmModel.m['7d'] ? lstmForward(sequence, lstmModel.m['7d'].lstm, lstmModel.m['7d'].fc, lstmModel.c.h) : null;
              const pred_30d = lstmModel.m['30d'] ? lstmForward(sequence, lstmModel.m['30d'].lstm, lstmModel.m['30d'].fc, lstmModel.c.h) : null;

              const interpretDir = (p: number | null) => {
                if (p === null) return null;
                if (p > 5) return 'STRONG_UP';
                if (p > 2) return 'UP';
                if (p > -2) return 'FLAT';
                if (p > -5) return 'DOWN';
                return 'STRONG_DOWN';
              };

              lstm = { pred_7d, pred_30d, dir_7d: interpretDir(pred_7d), dir_30d: interpretDir(pred_30d) };
            }
          }
        }

        if (!xgboost && !lstm) {
          return Response.json({
            error: 'No models available',
            message: 'Neither XGBoost nor LSTM models could be loaded',
          }, { status: 503, headers: corsHeaders });
        }

        // Ensemble weights (can be tuned based on historical accuracy)
        const XGBOOST_WEIGHT = 0.6;  // XGBoost has more features
        const LSTM_WEIGHT = 0.4;     // LSTM captures temporal patterns

        const ensemblePredict = (
          xgVal: number | null,
          lstmVal: number | null
        ): { value: number | null; xgboost_contrib: number | null; lstm_contrib: number | null } => {
          if (xgVal !== null && lstmVal !== null) {
            // Weighted average
            return {
              value: xgVal * XGBOOST_WEIGHT + lstmVal * LSTM_WEIGHT,
              xgboost_contrib: xgVal * XGBOOST_WEIGHT,
              lstm_contrib: lstmVal * LSTM_WEIGHT,
            };
          } else if (xgVal !== null) {
            return { value: xgVal, xgboost_contrib: xgVal, lstm_contrib: null };
          } else if (lstmVal !== null) {
            return { value: lstmVal, xgboost_contrib: null, lstm_contrib: lstmVal };
          }
          return { value: null, xgboost_contrib: null, lstm_contrib: null };
        };

        const interpret = (pred: number | null) => {
          if (pred === null) return null;
          if (pred > 5) return 'STRONG_UP';
          if (pred > 2) return 'UP';
          if (pred > -2) return 'FLAT';
          if (pred > -5) return 'DOWN';
          return 'STRONG_DOWN';
        };

        // Calculate model agreement (confidence indicator)
        const calcAgreement = (
          xgDir: string | null,
          lstmDir: string | null
        ): { agreement: 'HIGH' | 'MEDIUM' | 'LOW' | null; note: string } => {
          if (!xgDir || !lstmDir) {
            return { agreement: null, note: 'Single model only' };
          }

          const upDirs = ['STRONG_UP', 'UP'];
          const downDirs = ['STRONG_DOWN', 'DOWN'];

          const xgUp = upDirs.includes(xgDir);
          const xgDown = downDirs.includes(xgDir);
          const lstmUp = upDirs.includes(lstmDir);
          const lstmDown = downDirs.includes(lstmDir);

          if (xgDir === lstmDir) {
            return { agreement: 'HIGH', note: 'Models agree on direction and magnitude' };
          } else if ((xgUp && lstmUp) || (xgDown && lstmDown)) {
            return { agreement: 'MEDIUM', note: 'Models agree on direction' };
          } else if (xgDir === 'FLAT' || lstmDir === 'FLAT') {
            return { agreement: 'MEDIUM', note: 'One model neutral' };
          } else {
            return { agreement: 'LOW', note: 'Models disagree on direction' };
          }
        };

        const xg7d = xgboost?.pred_7d ?? null;
        const xg30d = xgboost?.pred_30d ?? null;
        const lstm7d = lstm?.pred_7d ?? null;
        const lstm30d = lstm?.pred_30d ?? null;

        const ensemble7d = ensemblePredict(xg7d, lstm7d);
        const ensemble30d = ensemblePredict(xg30d, lstm30d);

        const agreement7d = calcAgreement(
          xgboost?.dir_7d ?? null,
          lstm?.dir_7d ?? null
        );
        const agreement30d = calcAgreement(
          xgboost?.dir_30d ?? null,
          lstm?.dir_30d ?? null
        );

        // Log ensemble prediction to database (non-blocking)
        const logPrediction = async () => {
          try {
            // Calculate target dates
            const predDate = new Date(currentDate);
            const target7d = new Date(predDate);
            target7d.setDate(target7d.getDate() + 7);
            const target30d = new Date(predDate);
            target30d.setDate(target30d.getDate() + 30);

            const formatDate = (d: Date) => d.toISOString().split('T')[0];

            await env.DB.prepare(`
              INSERT OR REPLACE INTO ensemble_predictions (
                prediction_date, target_date_7d, target_date_30d, current_score,
                xgboost_7d, xgboost_30d, lstm_7d, lstm_30d,
                ensemble_7d, ensemble_30d, confidence_7d, confidence_30d
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `).bind(
              currentDate,
              formatDate(target7d),
              formatDate(target30d),
              currentScore,
              xg7d,
              xg30d,
              lstm7d,
              lstm30d,
              ensemble7d.value,
              ensemble30d.value,
              agreement7d.agreement,
              agreement30d.agreement
            ).run();
          } catch (e) {
            console.error('Failed to log ensemble prediction:', e);
          }
        };

        // Execute logging in background (don't block response)
        ctx.waitUntil(logPrediction());

        return Response.json({
          date: currentDate,
          current_score: currentScore,
          ensemble: {
            weights: { xgboost: XGBOOST_WEIGHT, lstm: LSTM_WEIGHT },
            predictions: {
              pxi_change_7d: {
                value: ensemble7d.value,
                direction: interpret(ensemble7d.value),
                confidence: agreement7d.agreement,
                components: {
                  xgboost: xg7d,
                  lstm: lstm7d,
                },
              },
              pxi_change_30d: {
                value: ensemble30d.value,
                direction: interpret(ensemble30d.value),
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

      // Get prediction accuracy metrics
      if (url.pathname === '/api/accuracy' && method === 'GET') {
        const includePending = url.searchParams.get('include_pending') === 'true';

        // Get all predictions (optionally including pending)
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
          return Response.json({
            message: includePending ? 'No predictions logged yet' : 'No evaluated predictions yet',
            total_predictions: 0,
            metrics: null,
          }, { headers: corsHeaders });
        }

        // Count pending vs evaluated
        const pendingCount = predictions.results.filter(p =>
          p.actual_change_7d === null && p.actual_change_30d === null
        ).length;
        const evaluatedCount = predictions.results.length - pendingCount;

        // Calculate accuracy metrics
        let d7_correct_direction = 0;
        let d7_total = 0;
        let d7_mae = 0; // Mean Absolute Error
        let d30_correct_direction = 0;
        let d30_total = 0;
        let d30_mae = 0;

        const recentPredictions: {
          date: string;
          predicted_7d: number | null;
          actual_7d: number | null;
          error_7d: number | null;
          predicted_30d: number | null;
          actual_30d: number | null;
          error_30d: number | null;
        }[] = [];

        for (const p of predictions.results) {
          const pred7d = p.predicted_change_7d;
          const act7d = p.actual_change_7d;
          const pred30d = p.predicted_change_30d;
          const act30d = p.actual_change_30d;

          let error7d: number | null = null;
          let error30d: number | null = null;

          if (pred7d !== null && act7d !== null) {
            d7_total++;
            error7d = Math.abs(pred7d - act7d);
            d7_mae += error7d;
            // Direction accuracy: both positive or both negative
            if ((pred7d > 0 && act7d > 0) || (pred7d < 0 && act7d < 0) || (pred7d === 0 && act7d === 0)) {
              d7_correct_direction++;
            }
          }

          if (pred30d !== null && act30d !== null) {
            d30_total++;
            error30d = Math.abs(pred30d - act30d);
            d30_mae += error30d;
            if ((pred30d > 0 && act30d > 0) || (pred30d < 0 && act30d < 0) || (pred30d === 0 && act30d === 0)) {
              d30_correct_direction++;
            }
          }

          if (recentPredictions.length < 10) {
            recentPredictions.push({
              date: p.prediction_date,
              predicted_7d: pred7d,
              actual_7d: act7d,
              error_7d: error7d,
              predicted_30d: pred30d,
              actual_30d: act30d,
              error_30d: error30d,
            });
          }
        }

        return Response.json({
          total_predictions: predictions.results.length,
          evaluated_count: evaluatedCount,
          pending_count: pendingCount,
          metrics: {
            d7: d7_total > 0 ? {
              direction_accuracy: (d7_correct_direction / d7_total * 100).toFixed(1) + '%',
              mean_absolute_error: (d7_mae / d7_total).toFixed(2),
              sample_size: d7_total,
            } : null,
            d30: d30_total > 0 ? {
              direction_accuracy: (d30_correct_direction / d30_total * 100).toFixed(1) + '%',
              mean_absolute_error: (d30_mae / d30_total).toFixed(2),
              sample_size: d30_total,
            } : null,
          },
          recent_predictions: recentPredictions,
        }, { headers: corsHeaders });
      }

      // Get ML ensemble prediction accuracy metrics
      if (url.pathname === '/api/ml/accuracy' && method === 'GET') {
        const includePending = url.searchParams.get('include_pending') === 'true';

        // Get all ensemble predictions
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
          return Response.json({
            message: includePending ? 'No ensemble predictions logged yet' : 'No evaluated ensemble predictions yet',
            total_predictions: 0,
            metrics: null,
          }, { headers: corsHeaders });
        }

        // Count pending vs evaluated
        const pendingCount = predictions.results.filter(p => p.evaluated_at === null).length;
        const evaluatedCount = predictions.results.length - pendingCount;

        // Calculate accuracy metrics for each model
        const metrics = {
          xgboost: { d7_correct: 0, d7_total: 0, d7_mae: 0, d30_correct: 0, d30_total: 0, d30_mae: 0 },
          lstm: { d7_correct: 0, d7_total: 0, d7_mae: 0, d30_correct: 0, d30_total: 0, d30_mae: 0 },
          ensemble: { d7_correct: 0, d7_total: 0, d7_mae: 0, d30_correct: 0, d30_total: 0, d30_mae: 0 },
        };

        const recentPredictions: {
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
        }[] = [];

        for (const p of predictions.results) {
          const act7d = p.actual_change_7d;
          const act30d = p.actual_change_30d;

          // 7-day metrics
          if (act7d !== null) {
            // XGBoost
            if (p.xgboost_7d !== null) {
              metrics.xgboost.d7_total++;
              metrics.xgboost.d7_mae += Math.abs(p.xgboost_7d - act7d);
              if ((p.xgboost_7d > 0 && act7d > 0) || (p.xgboost_7d < 0 && act7d < 0)) {
                metrics.xgboost.d7_correct++;
              }
            }
            // LSTM
            if (p.lstm_7d !== null) {
              metrics.lstm.d7_total++;
              metrics.lstm.d7_mae += Math.abs(p.lstm_7d - act7d);
              if ((p.lstm_7d > 0 && act7d > 0) || (p.lstm_7d < 0 && act7d < 0)) {
                metrics.lstm.d7_correct++;
              }
            }
            // Ensemble
            if (p.ensemble_7d !== null) {
              metrics.ensemble.d7_total++;
              metrics.ensemble.d7_mae += Math.abs(p.ensemble_7d - act7d);
              if ((p.ensemble_7d > 0 && act7d > 0) || (p.ensemble_7d < 0 && act7d < 0)) {
                metrics.ensemble.d7_correct++;
              }
            }
          }

          // 30-day metrics
          if (act30d !== null) {
            // XGBoost
            if (p.xgboost_30d !== null) {
              metrics.xgboost.d30_total++;
              metrics.xgboost.d30_mae += Math.abs(p.xgboost_30d - act30d);
              if ((p.xgboost_30d > 0 && act30d > 0) || (p.xgboost_30d < 0 && act30d < 0)) {
                metrics.xgboost.d30_correct++;
              }
            }
            // LSTM
            if (p.lstm_30d !== null) {
              metrics.lstm.d30_total++;
              metrics.lstm.d30_mae += Math.abs(p.lstm_30d - act30d);
              if ((p.lstm_30d > 0 && act30d > 0) || (p.lstm_30d < 0 && act30d < 0)) {
                metrics.lstm.d30_correct++;
              }
            }
            // Ensemble
            if (p.ensemble_30d !== null) {
              metrics.ensemble.d30_total++;
              metrics.ensemble.d30_mae += Math.abs(p.ensemble_30d - act30d);
              if ((p.ensemble_30d > 0 && act30d > 0) || (p.ensemble_30d < 0 && act30d < 0)) {
                metrics.ensemble.d30_correct++;
              }
            }
          }

          if (recentPredictions.length < 10) {
            recentPredictions.push({
              date: p.prediction_date,
              current_score: p.current_score,
              xgboost_7d: p.xgboost_7d,
              lstm_7d: p.lstm_7d,
              ensemble_7d: p.ensemble_7d,
              actual_7d: act7d,
              xgboost_30d: p.xgboost_30d,
              lstm_30d: p.lstm_30d,
              ensemble_30d: p.ensemble_30d,
              actual_30d: act30d,
              confidence_7d: p.confidence_7d,
              confidence_30d: p.confidence_30d,
            });
          }
        }

        const formatMetrics = (m: typeof metrics.xgboost) => ({
          d7: m.d7_total > 0 ? {
            direction_accuracy: (m.d7_correct / m.d7_total * 100).toFixed(1) + '%',
            mean_absolute_error: (m.d7_mae / m.d7_total).toFixed(2),
            sample_size: m.d7_total,
          } : null,
          d30: m.d30_total > 0 ? {
            direction_accuracy: (m.d30_correct / m.d30_total * 100).toFixed(1) + '%',
            mean_absolute_error: (m.d30_mae / m.d30_total).toFixed(2),
            sample_size: m.d30_total,
          } : null,
        });

        return Response.json({
          total_predictions: predictions.results.length,
          evaluated_count: evaluatedCount,
          pending_count: pendingCount,
          metrics: {
            xgboost: formatMetrics(metrics.xgboost),
            lstm: formatMetrics(metrics.lstm),
            ensemble: formatMetrics(metrics.ensemble),
          },
          recent_predictions: recentPredictions,
        }, { headers: corsHeaders });
      }

      // ML Backtest: Run models against historical data
      if (url.pathname === '/api/ml/backtest' && method === 'GET') {
        const limit = parseInt(url.searchParams.get('limit') || '500');
        const offset = parseInt(url.searchParams.get('offset') || '0');

        // Load models
        const [xgboostModel, lstmModel] = await Promise.all([
          loadMLModel(env.ML_MODELS),
          loadLSTMModel(env.ML_MODELS),
        ]);

        if (!xgboostModel && !lstmModel) {
          return Response.json({
            error: 'No models available for backtesting',
          }, { status: 503, headers: corsHeaders });
        }

        // Get historical PXI scores with forward returns
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

        // Get all dates for batch queries
        const dates = pxiHistory.results.map(r => r.date);

        // Batch fetch category scores
        const categoryData = await env.DB.prepare(`
          SELECT date, category, score FROM category_scores
          WHERE date IN (${dates.map(() => '?').join(',')})
        `).bind(...dates).all<{ date: string; category: string; score: number }>();

        // Batch fetch indicator values
        const indicatorData = await env.DB.prepare(`
          SELECT date, indicator_id, value FROM indicator_values
          WHERE date IN (${dates.map(() => '?').join(',')})
        `).bind(...dates).all<{ date: string; indicator_id: string; value: number }>();

        // Build lookup maps
        const categoryMap = new Map<string, Record<string, number>>();
        for (const c of categoryData.results || []) {
          if (!categoryMap.has(c.date)) categoryMap.set(c.date, {});
          categoryMap.get(c.date)![c.category] = c.score;
        }

        const indicatorMap = new Map<string, Record<string, number>>();
        for (const i of indicatorData.results || []) {
          if (!indicatorMap.has(i.date)) indicatorMap.set(i.date, {});
          indicatorMap.get(i.date)![i.indicator_id] = i.value;
        }

        // Compute rolling averages for each date (need surrounding data)
        const sortedHistory = [...pxiHistory.results].sort((a, b) => a.date.localeCompare(b.date));
        const rollingStats = new Map<string, { ma5: number; ma20: number; std20: number }>();

        for (let i = 0; i < sortedHistory.length; i++) {
          const recent5 = sortedHistory.slice(Math.max(0, i - 4), i + 1).map(r => r.score);
          const recent20 = sortedHistory.slice(Math.max(0, i - 19), i + 1).map(r => r.score);

          const ma5 = recent5.reduce((a, b) => a + b, 0) / recent5.length;
          const ma20 = recent20.reduce((a, b) => a + b, 0) / recent20.length;
          const std20 = Math.sqrt(recent20.reduce((sum, s) => sum + Math.pow(s - ma20, 2), 0) / recent20.length);

          rollingStats.set(sortedHistory[i].date, { ma5, ma20, std20 });
        }

        // Run predictions and calculate metrics
        const results: {
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
        }[] = [];

        const metrics = {
          xgboost: { d7_mae: 0, d7_correct: 0, d7_total: 0, d30_mae: 0, d30_correct: 0, d30_total: 0 },
          lstm: { d7_mae: 0, d7_correct: 0, d7_total: 0, d30_mae: 0, d30_correct: 0, d30_total: 0 },
          ensemble: { d7_mae: 0, d7_correct: 0, d7_total: 0, d30_mae: 0, d30_correct: 0, d30_total: 0 },
        };

        const XGBOOST_WEIGHT = 0.6;
        const LSTM_WEIGHT = 0.4;

        for (const row of pxiHistory.results) {
          const categories = categoryMap.get(row.date) || {};
          const indicators = indicatorMap.get(row.date) || {};
          const rolling = rollingStats.get(row.date) || { ma5: row.score, ma20: row.score, std20: 10 };

          // Calculate actual forward changes
          const actual_7d = row.actual_score_7d !== null ? row.actual_score_7d - row.score : null;
          const actual_30d = row.actual_score_30d !== null ? row.actual_score_30d - row.score : null;

          let xg7d: number | null = null;
          let xg30d: number | null = null;
          let lstm7d: number | null = null;
          let lstm30d: number | null = null;

          // XGBoost prediction
          if (xgboostModel) {
            const features = extractMLFeatures({
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

            xg7d = xgboostModel.m['7d'] ? xgbPredict(xgboostModel.m['7d'], features, xgboostModel.f) : null;
            xg30d = xgboostModel.m['30d'] ? xgbPredict(xgboostModel.m['30d'], features, xgboostModel.f) : null;
          }

          // LSTM prediction (simplified - uses single row features, not full sequence)
          // For proper LSTM backtest, we'd need sequence history which is computationally expensive
          if (lstmModel && categories && Object.keys(categories).length > 0) {
            const vix = indicators['vix'] ?? 20;
            const lstmFeatures = extractLSTMFeatures(
              { score: row.score, delta_7d: row.delta_7d, categories, vix },
              lstmModel.n,
              lstmModel.c.f
            );
            // Note: This is a simplified single-step LSTM, not a proper sequence prediction
            // Real LSTM backtest would require sequence data for each historical date
          }

          // Ensemble (XGBoost only for now, LSTM requires sequence data)
          const ens7d = xg7d;
          const ens30d = xg30d;

          // Track metrics for 7-day
          if (actual_7d !== null) {
            if (xg7d !== null) {
              metrics.xgboost.d7_mae += Math.abs(xg7d - actual_7d);
              metrics.xgboost.d7_total++;
              if ((xg7d > 0 && actual_7d > 0) || (xg7d < 0 && actual_7d < 0) || (xg7d === 0 && actual_7d === 0)) {
                metrics.xgboost.d7_correct++;
              }
            }
            if (ens7d !== null) {
              metrics.ensemble.d7_mae += Math.abs(ens7d - actual_7d);
              metrics.ensemble.d7_total++;
              if ((ens7d > 0 && actual_7d > 0) || (ens7d < 0 && actual_7d < 0)) {
                metrics.ensemble.d7_correct++;
              }
            }
          }

          // Track metrics for 30-day
          if (actual_30d !== null) {
            if (xg30d !== null) {
              metrics.xgboost.d30_mae += Math.abs(xg30d - actual_30d);
              metrics.xgboost.d30_total++;
              if ((xg30d > 0 && actual_30d > 0) || (xg30d < 0 && actual_30d < 0)) {
                metrics.xgboost.d30_correct++;
              }
            }
            if (ens30d !== null) {
              metrics.ensemble.d30_mae += Math.abs(ens30d - actual_30d);
              metrics.ensemble.d30_total++;
              if ((ens30d > 0 && actual_30d > 0) || (ens30d < 0 && actual_30d < 0)) {
                metrics.ensemble.d30_correct++;
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
            ensemble_7d: ens7d,
            ensemble_30d: ens30d,
            actual_7d,
            actual_30d,
          });
        }

        const formatMetrics = (m: typeof metrics.xgboost) => ({
          d7: m.d7_total > 0 ? {
            direction_accuracy: ((m.d7_correct / m.d7_total) * 100).toFixed(1) + '%',
            mean_absolute_error: (m.d7_mae / m.d7_total).toFixed(2),
            sample_size: m.d7_total,
          } : null,
          d30: m.d30_total > 0 ? {
            direction_accuracy: ((m.d30_correct / m.d30_total) * 100).toFixed(1) + '%',
            mean_absolute_error: (m.d30_mae / m.d30_total).toFixed(2),
            sample_size: m.d30_total,
          } : null,
        });

        // Calculate additional stats
        const validResults7d = results.filter(r => r.actual_7d !== null && r.xgboost_7d !== null);
        const validResults30d = results.filter(r => r.actual_30d !== null && r.xgboost_30d !== null);

        // Compute R² for XGBoost
        const calcR2 = (predicted: number[], actual: number[]): number => {
          if (predicted.length === 0) return 0;
          const meanActual = actual.reduce((a, b) => a + b, 0) / actual.length;
          const ssRes = predicted.reduce((sum, p, i) => sum + Math.pow(actual[i] - p, 2), 0);
          const ssTot = actual.reduce((sum, a) => sum + Math.pow(a - meanActual, 2), 0);
          return ssTot > 0 ? 1 - (ssRes / ssTot) : 0;
        };

        const r2_7d = calcR2(
          validResults7d.map(r => r.xgboost_7d!),
          validResults7d.map(r => r.actual_7d!)
        );
        const r2_30d = calcR2(
          validResults30d.map(r => r.xgboost_30d!),
          validResults30d.map(r => r.actual_30d!)
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
              lstm: false, // LSTM requires sequence data for proper backtest
            },
            note: 'This is IN-SAMPLE performance (models trained on this data). True OOS metrics come from live predictions.',
          },
          metrics: {
            xgboost: {
              ...formatMetrics(metrics.xgboost),
              r2_7d: r2_7d.toFixed(4),
              r2_30d: r2_30d.toFixed(4),
            },
            ensemble: formatMetrics(metrics.ensemble),
          },
          by_pxi_bucket: (() => {
            const buckets = [
              { name: '0-20', min: 0, max: 20 },
              { name: '20-40', min: 20, max: 40 },
              { name: '40-60', min: 40, max: 60 },
              { name: '60-80', min: 60, max: 80 },
              { name: '80-100', min: 80, max: 100 },
            ];

            return buckets.map(bucket => {
              const bucketResults = validResults7d.filter(
                r => r.pxi_score >= bucket.min && r.pxi_score < bucket.max
              );
              if (bucketResults.length === 0) return { bucket: bucket.name, count: 0 };

              const correctDir = bucketResults.filter(
                r => (r.xgboost_7d! > 0 && r.actual_7d! > 0) || (r.xgboost_7d! < 0 && r.actual_7d! < 0)
              ).length;

              return {
                bucket: bucket.name,
                count: bucketResults.length,
                direction_accuracy_7d: ((correctDir / bucketResults.length) * 100).toFixed(1) + '%',
                avg_predicted_7d: (bucketResults.reduce((s, r) => s + r.xgboost_7d!, 0) / bucketResults.length).toFixed(2),
                avg_actual_7d: (bucketResults.reduce((s, r) => s + r.actual_7d!, 0) / bucketResults.length).toFixed(2),
              };
            });
          })(),
          recent_predictions: results.slice(0, 20).map(r => ({
            date: r.date,
            pxi: r.pxi_score.toFixed(1),
            xgb_7d: r.xgboost_7d?.toFixed(2),
            actual_7d: r.actual_7d?.toFixed(2),
            xgb_30d: r.xgboost_30d?.toFixed(2),
            actual_30d: r.actual_30d?.toFixed(2),
          })),
        }, { headers: corsHeaders });
      }

      // Retrain: Update period accuracy scores and tune model parameters
      if (url.pathname === '/api/retrain' && method === 'POST') {
        const adminAuthFailure = await enforceAdminAuth(request, env, corsHeaders, clientIP);
        if (adminAuthFailure) {
          return adminAuthFailure;
        }

        // Get all evaluated predictions with their similar periods
        const evaluatedPreds = await env.DB.prepare(`
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

        if (!evaluatedPreds.results || evaluatedPreds.results.length === 0) {
          return Response.json({
            message: 'No evaluated predictions to learn from',
            periods_updated: 0,
          }, { headers: corsHeaders });
        }

        // Track accuracy for each period
        const periodStats: Record<string, {
          times_used: number;
          correct_7d: number;
          total_7d: number;
          errors_7d: number[];
          correct_30d: number;
          total_30d: number;
          errors_30d: number[];
        }> = {};

        for (const pred of evaluatedPreds.results) {
          let periods: string[] = [];
          try {
            periods = JSON.parse(pred.similar_periods);
          } catch { continue; }

          const pred7d = pred.predicted_change_7d;
          const act7d = pred.actual_change_7d;
          const pred30d = pred.predicted_change_30d;
          const act30d = pred.actual_change_30d;

          for (const periodDate of periods) {
            if (!periodStats[periodDate]) {
              periodStats[periodDate] = {
                times_used: 0,
                correct_7d: 0, total_7d: 0, errors_7d: [],
                correct_30d: 0, total_30d: 0, errors_30d: [],
              };
            }
            const stats = periodStats[periodDate];
            stats.times_used++;

            // 7d accuracy
            if (pred7d !== null && act7d !== null) {
              stats.total_7d++;
              const correctDir = (pred7d > 0 && act7d > 0) || (pred7d < 0 && act7d < 0) || (pred7d === 0 && act7d === 0);
              if (correctDir) stats.correct_7d++;
              stats.errors_7d.push(Math.abs(pred7d - act7d));
            }

            // 30d accuracy
            if (pred30d !== null && act30d !== null) {
              stats.total_30d++;
              const correctDir = (pred30d > 0 && act30d > 0) || (pred30d < 0 && act30d < 0) || (pred30d === 0 && act30d === 0);
              if (correctDir) stats.correct_30d++;
              stats.errors_30d.push(Math.abs(pred30d - act30d));
            }
          }
        }

        // Update period_accuracy table
        let periodsUpdated = 0;
        for (const [periodDate, stats] of Object.entries(periodStats)) {
          // Calculate accuracy score (0-1) based on direction accuracy
          const dir7d = stats.total_7d > 0 ? stats.correct_7d / stats.total_7d : 0.5;
          const dir30d = stats.total_30d > 0 ? stats.correct_30d / stats.total_30d : 0.5;
          // Weight 7d more since we have more data
          const accuracyScore = stats.total_30d > 0
            ? (dir7d * 0.6 + dir30d * 0.4)
            : dir7d;

          const avgError7d = stats.errors_7d.length > 0
            ? stats.errors_7d.reduce((a, b) => a + b, 0) / stats.errors_7d.length
            : null;
          const avgError30d = stats.errors_30d.length > 0
            ? stats.errors_30d.reduce((a, b) => a + b, 0) / stats.errors_30d.length
            : null;

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
            accuracyScore
          ).run();
          periodsUpdated++;
        }

        // Tune model parameters based on overall accuracy
        const totalPreds = evaluatedPreds.results.length;
        let total7dCorrect = 0, total7d = 0;
        for (const pred of evaluatedPreds.results) {
          if (pred.predicted_change_7d !== null && pred.actual_change_7d !== null) {
            total7d++;
            const p = pred.predicted_change_7d;
            const a = pred.actual_change_7d;
            if ((p > 0 && a > 0) || (p < 0 && a < 0)) total7dCorrect++;
          }
        }
        const overallAccuracy = total7d > 0 ? total7dCorrect / total7d : 0.5;

        // Adjust accuracy_weight based on how well accuracy-weighted predictions are doing
        // If overall accuracy > 60%, increase weight; if < 50%, decrease
        let newAccuracyWeight = 0.3; // default
        if (overallAccuracy > 0.65) {
          newAccuracyWeight = 0.5; // Accuracy weighting is working well
        } else if (overallAccuracy > 0.55) {
          newAccuracyWeight = 0.4;
        } else if (overallAccuracy < 0.45) {
          newAccuracyWeight = 0.2; // Fall back more to similarity
        }

        await env.DB.prepare(`
          UPDATE model_params SET param_value = ?, updated_at = datetime('now'),
          notes = ? WHERE param_key = 'accuracy_weight'
        `).bind(
          newAccuracyWeight,
          `Auto-tuned based on ${total7d} predictions (${(overallAccuracy * 100).toFixed(1)}% accuracy)`
        ).run();

        // Adaptive bucket threshold tuning
        // Get current thresholds
        const thresholdParams = await env.DB.prepare(`
          SELECT param_key, param_value FROM model_params
          WHERE param_key LIKE 'bucket_threshold_%'
        `).all<{ param_key: string; param_value: number }>();

        const currentThresholds = { t1: 20, t2: 40, t3: 60, t4: 80 };
        for (const p of thresholdParams.results || []) {
          if (p.param_key === 'bucket_threshold_1') currentThresholds.t1 = p.param_value;
          if (p.param_key === 'bucket_threshold_2') currentThresholds.t2 = p.param_value;
          if (p.param_key === 'bucket_threshold_3') currentThresholds.t3 = p.param_value;
          if (p.param_key === 'bucket_threshold_4') currentThresholds.t4 = p.param_value;
        }

        // Get PXI scores for each prediction to determine bucket
        const pxiScoresForPreds = await env.DB.prepare(`
          SELECT pl.prediction_date, ps.score, pl.predicted_change_7d, pl.actual_change_7d
          FROM prediction_log pl
          JOIN pxi_scores ps ON pl.prediction_date = ps.date
          WHERE pl.actual_change_7d IS NOT NULL
        `).all<{ prediction_date: string; score: number; predicted_change_7d: number; actual_change_7d: number }>();

        // Helper to get bucket index (0-4) from score
        const getBucketIdx = (s: number, t: typeof currentThresholds): number => {
          if (s < t.t1) return 0;
          if (s < t.t2) return 1;
          if (s < t.t3) return 2;
          if (s < t.t4) return 3;
          return 4;
        };

        // Calculate accuracy by bucket
        const bucketStats = [
          { correct: 0, total: 0 },
          { correct: 0, total: 0 },
          { correct: 0, total: 0 },
          { correct: 0, total: 0 },
          { correct: 0, total: 0 },
        ];

        for (const pred of pxiScoresForPreds.results || []) {
          const bucketIdx = getBucketIdx(pred.score, currentThresholds);
          bucketStats[bucketIdx].total++;
          const p = pred.predicted_change_7d;
          const a = pred.actual_change_7d;
          if ((p > 0 && a > 0) || (p < 0 && a < 0)) {
            bucketStats[bucketIdx].correct++;
          }
        }

        // Calculate accuracy per bucket
        const bucketAccuracies = bucketStats.map(b =>
          b.total >= 3 ? b.correct / b.total : null // Need at least 3 samples
        );

        // Adjust thresholds: if adjacent buckets have very different accuracies,
        // shift the boundary toward the more accurate bucket
        const newThresholds = { ...currentThresholds };
        const minDiff = 8; // Minimum 8 points between thresholds

        // Only tune if we have enough data
        if (bucketStats.reduce((sum, b) => sum + b.total, 0) >= 10) {
          // Tune t1 based on bucket 0 vs bucket 1 accuracy
          if (bucketAccuracies[0] !== null && bucketAccuracies[1] !== null) {
            const diff = bucketAccuracies[0] - bucketAccuracies[1];
            if (diff > 0.15) newThresholds.t1 = Math.min(currentThresholds.t1 + 2, currentThresholds.t2 - minDiff);
            else if (diff < -0.15) newThresholds.t1 = Math.max(currentThresholds.t1 - 2, 10);
          }

          // Tune t2 based on bucket 1 vs bucket 2 accuracy
          if (bucketAccuracies[1] !== null && bucketAccuracies[2] !== null) {
            const diff = bucketAccuracies[1] - bucketAccuracies[2];
            if (diff > 0.15) newThresholds.t2 = Math.min(currentThresholds.t2 + 2, currentThresholds.t3 - minDiff);
            else if (diff < -0.15) newThresholds.t2 = Math.max(currentThresholds.t2 - 2, newThresholds.t1 + minDiff);
          }

          // Tune t3 based on bucket 2 vs bucket 3 accuracy
          if (bucketAccuracies[2] !== null && bucketAccuracies[3] !== null) {
            const diff = bucketAccuracies[2] - bucketAccuracies[3];
            if (diff > 0.15) newThresholds.t3 = Math.min(currentThresholds.t3 + 2, currentThresholds.t4 - minDiff);
            else if (diff < -0.15) newThresholds.t3 = Math.max(currentThresholds.t3 - 2, newThresholds.t2 + minDiff);
          }

          // Tune t4 based on bucket 3 vs bucket 4 accuracy
          if (bucketAccuracies[3] !== null && bucketAccuracies[4] !== null) {
            const diff = bucketAccuracies[3] - bucketAccuracies[4];
            if (diff > 0.15) newThresholds.t4 = Math.min(currentThresholds.t4 + 2, 90);
            else if (diff < -0.15) newThresholds.t4 = Math.max(currentThresholds.t4 - 2, newThresholds.t3 + minDiff);
          }

          // Update thresholds in database
          const thresholdUpdates = [
            { key: 'bucket_threshold_1', val: newThresholds.t1, old: currentThresholds.t1 },
            { key: 'bucket_threshold_2', val: newThresholds.t2, old: currentThresholds.t2 },
            { key: 'bucket_threshold_3', val: newThresholds.t3, old: currentThresholds.t3 },
            { key: 'bucket_threshold_4', val: newThresholds.t4, old: currentThresholds.t4 },
          ];

          for (const t of thresholdUpdates) {
            if (t.val !== t.old) {
              await env.DB.prepare(`
                UPDATE model_params SET param_value = ?, updated_at = datetime('now'),
                notes = ? WHERE param_key = ?
              `).bind(t.val, `Tuned from ${t.old} (bucket accuracies: ${bucketAccuracies.map(a => a ? (a * 100).toFixed(0) + '%' : 'N/A').join(', ')})`, t.key).run();
            }
          }
        }

        return Response.json({
          success: true,
          predictions_analyzed: totalPreds,
          periods_updated: periodsUpdated,
          overall_accuracy: (overallAccuracy * 100).toFixed(1) + '%',
          new_accuracy_weight: newAccuracyWeight,
          bucket_tuning: {
            samples_per_bucket: bucketStats.map(b => b.total),
            accuracy_per_bucket: bucketAccuracies.map(a => a !== null ? (a * 100).toFixed(0) + '%' : 'N/A'),
            old_thresholds: currentThresholds,
            new_thresholds: newThresholds,
            changed: JSON.stringify(currentThresholds) !== JSON.stringify(newThresholds),
          },
          top_periods: Object.entries(periodStats)
            .sort((a, b) => b[1].times_used - a[1].times_used)
            .slice(0, 5)
            .map(([date, s]) => ({
              date,
              times_used: s.times_used,
              accuracy_7d: s.total_7d > 0 ? (s.correct_7d / s.total_7d * 100).toFixed(0) + '%' : 'N/A',
            })),
        }, { headers: corsHeaders });
      }

      // Get model parameters (for debugging/monitoring)
      if (url.pathname === '/api/model' && method === 'GET') {
        const params = await env.DB.prepare(
          'SELECT param_key, param_value, updated_at, notes FROM model_params ORDER BY param_key'
        ).all<{ param_key: string; param_value: number; updated_at: string; notes: string }>();

        const periodAccuracy = await env.DB.prepare(`
          SELECT period_date, accuracy_score, times_used, avg_error_7d
          FROM period_accuracy
          WHERE times_used >= 2
          ORDER BY accuracy_score DESC
          LIMIT 10
        `).all<{ period_date: string; accuracy_score: number; times_used: number; avg_error_7d: number }>();

        return Response.json({
          params: params.results,
          top_accurate_periods: periodAccuracy.results,
        }, { headers: corsHeaders });
      }

      // Backtest endpoint - calculate conditional return distributions
      if (url.pathname === '/api/backtest' && method === 'GET') {
        // Get all PXI scores with their dates
        const pxiScores = await env.DB.prepare(`
          SELECT date, score FROM pxi_scores ORDER BY date ASC
        `).all<{ date: string; score: number }>();

        // Get all SPY prices
        const spyPrices = await env.DB.prepare(`
          SELECT date, value FROM indicator_values
          WHERE indicator_id = 'spy_close'
          ORDER BY date ASC
        `).all<{ date: string; value: number }>();

        if (!spyPrices.results || spyPrices.results.length === 0) {
          return Response.json({
            error: 'No SPY price data available. Run /api/refresh to fetch SPY prices.',
            hint: 'POST /api/refresh with Authorization header'
          }, { status: 400, headers: corsHeaders });
        }

        // Build SPY price lookup map
        const spyMap = new Map<string, number>();
        for (const p of spyPrices.results) {
          spyMap.set(p.date, p.value);
        }

        // Helper to get SPY price on or after a date (handle weekends/holidays)
        const getSpyPrice = (dateStr: string, maxDaysForward: number = 5): number | null => {
          const date = new Date(dateStr);
          for (let i = 0; i <= maxDaysForward; i++) {
            const checkDate = new Date(date);
            checkDate.setDate(checkDate.getDate() + i);
            const checkStr = checkDate.toISOString().split('T')[0];
            if (spyMap.has(checkStr)) {
              return spyMap.get(checkStr)!;
            }
          }
          return null;
        };

        // Calculate forward returns for each PXI score
        interface BacktestResult {
          date: string;
          pxi_score: number;
          pxi_bucket: string;
          spy_price: number | null;
          spy_7d: number | null;
          spy_30d: number | null;
          return_7d: number | null;
          return_30d: number | null;
        }

        const results: BacktestResult[] = [];

        for (const pxi of pxiScores.results || []) {
          const spyNow = getSpyPrice(pxi.date);

          // Get future SPY prices
          const date = new Date(pxi.date);
          const date7d = new Date(date);
          date7d.setDate(date7d.getDate() + 7);
          const date30d = new Date(date);
          date30d.setDate(date30d.getDate() + 30);

          const spy7d = getSpyPrice(date7d.toISOString().split('T')[0]);
          const spy30d = getSpyPrice(date30d.toISOString().split('T')[0]);

          // Calculate returns
          const return7d = spyNow && spy7d ? ((spy7d - spyNow) / spyNow) * 100 : null;
          const return30d = spyNow && spy30d ? ((spy30d - spyNow) / spyNow) * 100 : null;

          // Determine PXI bucket
          let bucket = 'unknown';
          if (pxi.score < 20) bucket = '0-20';
          else if (pxi.score < 40) bucket = '20-40';
          else if (pxi.score < 60) bucket = '40-60';
          else if (pxi.score < 80) bucket = '60-80';
          else bucket = '80-100';

          results.push({
            date: pxi.date,
            pxi_score: pxi.score,
            pxi_bucket: bucket,
            spy_price: spyNow,
            spy_7d: spy7d,
            spy_30d: spy30d,
            return_7d: return7d,
            return_30d: return30d,
          });
        }

        // Aggregate by bucket
        interface BucketStats {
          bucket: string;
          count: number;
          avg_return_7d: number | null;
          avg_return_30d: number | null;
          win_rate_7d: number | null;  // % of positive 7d returns
          win_rate_30d: number | null; // % of positive 30d returns
          median_return_7d: number | null;
          median_return_30d: number | null;
          min_return_7d: number | null;
          max_return_7d: number | null;
          min_return_30d: number | null;
          max_return_30d: number | null;
        }

        const buckets = ['0-20', '20-40', '40-60', '60-80', '80-100'];
        const bucketStats: BucketStats[] = [];

        for (const bucket of buckets) {
          const bucketResults = results.filter(r => r.pxi_bucket === bucket);
          const returns7d = bucketResults.map(r => r.return_7d).filter((r): r is number => r !== null);
          const returns30d = bucketResults.map(r => r.return_30d).filter((r): r is number => r !== null);

          const median = (arr: number[]): number | null => {
            if (arr.length === 0) return null;
            const sorted = [...arr].sort((a, b) => a - b);
            const mid = Math.floor(sorted.length / 2);
            return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
          };

          bucketStats.push({
            bucket,
            count: bucketResults.length,
            avg_return_7d: returns7d.length > 0 ? returns7d.reduce((a, b) => a + b, 0) / returns7d.length : null,
            avg_return_30d: returns30d.length > 0 ? returns30d.reduce((a, b) => a + b, 0) / returns30d.length : null,
            win_rate_7d: returns7d.length > 0 ? (returns7d.filter(r => r > 0).length / returns7d.length) * 100 : null,
            win_rate_30d: returns30d.length > 0 ? (returns30d.filter(r => r > 0).length / returns30d.length) * 100 : null,
            median_return_7d: median(returns7d),
            median_return_30d: median(returns30d),
            min_return_7d: returns7d.length > 0 ? Math.min(...returns7d) : null,
            max_return_7d: returns7d.length > 0 ? Math.max(...returns7d) : null,
            min_return_30d: returns30d.length > 0 ? Math.min(...returns30d) : null,
            max_return_30d: returns30d.length > 0 ? Math.max(...returns30d) : null,
          });
        }

        // Overall stats
        const allReturns7d = results.map(r => r.return_7d).filter((r): r is number => r !== null);
        const allReturns30d = results.map(r => r.return_30d).filter((r): r is number => r !== null);

        // Calculate extreme readings analysis
        const extremeLow = results.filter(r => r.pxi_score < 25);
        const extremeHigh = results.filter(r => r.pxi_score > 75);

        const extremeLowReturns7d = extremeLow.map(r => r.return_7d).filter((r): r is number => r !== null);
        const extremeLowReturns30d = extremeLow.map(r => r.return_30d).filter((r): r is number => r !== null);
        const extremeHighReturns7d = extremeHigh.map(r => r.return_7d).filter((r): r is number => r !== null);
        const extremeHighReturns30d = extremeHigh.map(r => r.return_30d).filter((r): r is number => r !== null);

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
              avg_return_7d: extremeLowReturns7d.length > 0
                ? extremeLowReturns7d.reduce((a, b) => a + b, 0) / extremeLowReturns7d.length
                : null,
              avg_return_30d: extremeLowReturns30d.length > 0
                ? extremeLowReturns30d.reduce((a, b) => a + b, 0) / extremeLowReturns30d.length
                : null,
              win_rate_7d: extremeLowReturns7d.length > 0
                ? (extremeLowReturns7d.filter(r => r > 0).length / extremeLowReturns7d.length) * 100
                : null,
              win_rate_30d: extremeLowReturns30d.length > 0
                ? (extremeLowReturns30d.filter(r => r > 0).length / extremeLowReturns30d.length) * 100
                : null,
            },
            high_pxi: {
              threshold: '>75',
              count: extremeHigh.length,
              avg_return_7d: extremeHighReturns7d.length > 0
                ? extremeHighReturns7d.reduce((a, b) => a + b, 0) / extremeHighReturns7d.length
                : null,
              avg_return_30d: extremeHighReturns30d.length > 0
                ? extremeHighReturns30d.reduce((a, b) => a + b, 0) / extremeHighReturns30d.length
                : null,
              win_rate_7d: extremeHighReturns7d.length > 0
                ? (extremeHighReturns7d.filter(r => r > 0).length / extremeHighReturns7d.length) * 100
                : null,
              win_rate_30d: extremeHighReturns30d.length > 0
                ? (extremeHighReturns30d.filter(r => r > 0).length / extremeHighReturns30d.length) * 100
                : null,
            },
          },
          // Include raw data for detailed analysis (optional query param)
          raw_data: url.searchParams.get('raw') === 'true' ? results : undefined,
        }, { headers: corsHeaders });
      }

      // ============== v1.1: Walk-Forward Signal Backtest ==============
      // Compares PXI-Signal strategy vs baselines (200DMA, buy-and-hold)
      if (url.pathname === '/api/backtest/signal' && method === 'GET') {
        // Get all historical signal data
        const signals = await env.DB.prepare(`
          SELECT date, pxi_level, risk_allocation, signal_type, regime
          FROM pxi_signal ORDER BY date ASC
        `).all<{ date: string; pxi_level: number; risk_allocation: number; signal_type: string; regime: string }>();

        // Get SPY prices for return calculation
        const spyPrices = await env.DB.prepare(`
          SELECT date, value FROM indicator_values
          WHERE indicator_id = 'spy_close'
          ORDER BY date ASC
        `).all<{ date: string; value: number }>();

        if (!signals.results || signals.results.length < 30) {
          return Response.json({
            error: 'Insufficient signal data. Run historical recalculation first.',
            signal_count: signals.results?.length || 0,
            hint: 'POST /api/recalculate-all-signals with Authorization header'
          }, { status: 400, headers: corsHeaders });
        }

        if (!spyPrices.results || spyPrices.results.length === 0) {
          return Response.json({
            error: 'No SPY price data available.',
            hint: 'POST /api/refresh to fetch SPY prices'
          }, { status: 400, headers: corsHeaders });
        }

        // Build price lookup maps
        const spyMap = new Map<string, number>();
        for (const p of spyPrices.results) {
          spyMap.set(p.date, p.value);
        }

        // Calculate 200DMA for baseline comparison
        const spy200dma = new Map<string, number>();
        const spyDates = spyPrices.results.map(p => p.date).sort();
        for (let i = 199; i < spyDates.length; i++) {
          const window = spyDates.slice(i - 199, i + 1);
          const avg = window.reduce((sum, d) => sum + (spyMap.get(d) || 0), 0) / 200;
          spy200dma.set(spyDates[i], avg);
        }

        // Calculate strategy returns
        interface DailyReturn {
          date: string;
          spy_return: number;
          pxi_signal_return: number;   // PXI-Signal strategy (allocation-weighted)
          dma200_return: number;       // 200DMA strategy (100% when SPY > 200DMA, 0% otherwise)
          buy_hold_return: number;     // Buy and hold (always 100%)
          allocation: number;
          signal_type: string;
        }

        const dailyReturns: DailyReturn[] = [];
        let prevDate: string | null = null;

        for (const signal of signals.results) {
          if (!prevDate) {
            prevDate = signal.date;
            continue;
          }

          const prevPrice = spyMap.get(prevDate);
          const currPrice = spyMap.get(signal.date);

          if (prevPrice && currPrice) {
            const dailyReturn = (currPrice - prevPrice) / prevPrice;
            const prevDma = spy200dma.get(prevDate);
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

          prevDate = signal.date;
        }

        // Calculate cumulative returns and metrics
        const calculateMetrics = (returns: number[], name: string) => {
          if (returns.length === 0) return null;

          // Cumulative return
          let cumulative = 1;
          let peak = 1;
          let maxDrawdown = 0;

          for (const r of returns) {
            cumulative *= (1 + r);
            if (cumulative > peak) peak = cumulative;
            const drawdown = (peak - cumulative) / peak;
            if (drawdown > maxDrawdown) maxDrawdown = drawdown;
          }

          // Annualized metrics (assuming ~252 trading days/year)
          const years = returns.length / 252;
          const cagr = years > 0 ? Math.pow(cumulative, 1 / years) - 1 : 0;

          // Volatility (annualized)
          const avgReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
          const variance = returns.reduce((sum, r) => sum + Math.pow(r - avgReturn, 2), 0) / returns.length;
          const dailyVol = Math.sqrt(variance);
          const annualVol = dailyVol * Math.sqrt(252);

          // Sharpe ratio (assuming 0% risk-free rate for simplicity)
          const sharpe = annualVol > 0 ? cagr / annualVol : 0;

          // Win rate
          const winningDays = returns.filter(r => r > 0).length;
          const winRate = (winningDays / returns.length) * 100;

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

        const pxiSignalMetrics = calculateMetrics(dailyReturns.map(r => r.pxi_signal_return), 'PXI-Signal');
        const dma200Metrics = calculateMetrics(dailyReturns.map(r => r.dma200_return), '200DMA');
        const buyHoldMetrics = calculateMetrics(dailyReturns.map(r => r.buy_hold_return), 'Buy-and-Hold');

        // Calculate signal type distribution
        const signalDistribution: Record<string, number> = {};
        for (const r of dailyReturns) {
          signalDistribution[r.signal_type] = (signalDistribution[r.signal_type] || 0) + 1;
        }

        // Average allocation by signal type
        const avgAllocationBySignal: Record<string, number> = {};
        const signalReturns: Record<string, number[]> = {};

        for (const r of dailyReturns) {
          if (!avgAllocationBySignal[r.signal_type]) {
            avgAllocationBySignal[r.signal_type] = 0;
            signalReturns[r.signal_type] = [];
          }
          avgAllocationBySignal[r.signal_type] += r.allocation;
          signalReturns[r.signal_type].push(r.pxi_signal_return);
        }

        for (const type of Object.keys(avgAllocationBySignal)) {
          avgAllocationBySignal[type] = avgAllocationBySignal[type] / signalDistribution[type];
        }

        // Store results in backtest_results table
        const runDate = formatDate(new Date());
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
            })
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

      // ============== v1.1: Historical Signal Recalculation ==============
      // Generates signal layer data for all historical PXI scores
      if (url.pathname === '/api/recalculate-all-signals' && method === 'POST') {
        const adminAuthFailure = await enforceAdminAuth(request, env, corsHeaders, clientIP);
        if (adminAuthFailure) {
          return adminAuthFailure;
        }

        try {
          // Get all PXI scores
          const pxiScores = await env.DB.prepare(`
            SELECT date, score, delta_7d, delta_30d FROM pxi_scores ORDER BY date ASC
          `).all<{ date: string; score: number; delta_7d: number | null; delta_30d: number | null }>();

          if (!pxiScores.results || pxiScores.results.length === 0) {
            return Response.json({ error: 'No PXI scores found' }, { status: 400, headers: corsHeaders });
          }

          // Pre-load ALL category scores in one query
          const allCatScores = await env.DB.prepare(`
            SELECT date, category, score FROM category_scores ORDER BY date
          `).all<{ date: string; category: string; score: number }>();

          // Build lookup map by date
          const catScoresByDate = new Map<string, { score: number }[]>();
          for (const cs of allCatScores.results || []) {
            if (!catScoresByDate.has(cs.date)) {
              catScoresByDate.set(cs.date, []);
            }
            catScoresByDate.get(cs.date)!.push({ score: cs.score });
          }

          // Get all VIX values for historical percentile calculation
          const vixHistory = await env.DB.prepare(`
            SELECT date, value FROM indicator_values
            WHERE indicator_id = 'vix'
            ORDER BY date ASC
          `).all<{ date: string; value: number }>();

          const vixMap = new Map<string, number>();
          const vixValues: number[] = [];
          for (const v of vixHistory.results || []) {
            vixMap.set(v.date, v.value);
            vixValues.push(v.value);
          }
          const sortedVix = [...vixValues].sort((a, b) => a - b);

          // Build all signals in memory first
          const signals: Array<{
            date: string; pxi_level: number; delta_pxi_7d: number | null;
            delta_pxi_30d: number | null; category_dispersion: number;
            regime: string; volatility_percentile: number | null;
            risk_allocation: number; signal_type: string;
          }> = [];

          for (const pxi of pxiScores.results) {
            // Get VIX percentile for this date
            const vix = vixMap.get(pxi.date);
            let vixPercentile: number | null = null;
            if (vix !== undefined && sortedVix.length > 0) {
              const rank = sortedVix.filter(v => v < vix).length +
                          sortedVix.filter(v => v === vix).length / 2;
              vixPercentile = (rank / sortedVix.length) * 100;
            }

            // Get category scores for this date
            const catScores = catScoresByDate.get(pxi.date) || [];

            // Calculate category dispersion
            const scores = catScores.map(c => c.score);
            const mean = scores.length > 0 ? scores.reduce((a, b) => a + b, 0) / scores.length : 50;
            const variance = scores.length > 0
              ? scores.reduce((sum, s) => sum + Math.pow(s - mean, 2), 0) / scores.length
              : 0;
            const dispersion = Math.sqrt(variance);

            // Determine regime from PXI level (simplified for historical)
            let regime: string = 'TRANSITION';
            if (pxi.score >= 65) regime = 'RISK_ON';
            else if (pxi.score <= 35) regime = 'RISK_OFF';

            // Base allocation from PXI score
            let allocation = 0.3 + (pxi.score / 100) * 0.7;

            // Rule 1: If regime = RISK_OFF → allocation * 0.5
            if (regime === 'RISK_OFF') allocation *= 0.5;

            // Rule 2: If regime = TRANSITION → allocation * 0.75
            if (regime === 'TRANSITION') allocation *= 0.75;

            // Rule 3: If Δ7d < -10 → allocation * 0.8
            if (pxi.delta_7d !== null && pxi.delta_7d < -10) allocation *= 0.8;

            // Rule 4: If vol_percentile > 80 → allocation * 0.7
            if (vixPercentile !== null && vixPercentile > 80) allocation *= 0.7;

            // Determine signal type
            let signalType: string = 'FULL_RISK';
            if (allocation < 0.3) signalType = 'DEFENSIVE';
            else if (allocation < 0.5) signalType = 'RISK_OFF';
            else if (allocation < 0.8) signalType = 'REDUCED_RISK';

            signals.push({
              date: pxi.date,
              pxi_level: pxi.score,
              delta_pxi_7d: pxi.delta_7d,
              delta_pxi_30d: pxi.delta_30d,
              category_dispersion: Math.round(dispersion * 10) / 10,
              regime,
              volatility_percentile: vixPercentile !== null ? Math.round(vixPercentile) : null,
              risk_allocation: Math.round(allocation * 100) / 100,
              signal_type: signalType,
            });
          }

          // Batch insert all signals (100 at a time)
          const batchSize = 100;
          let processed = 0;

          for (let i = 0; i < signals.length; i += batchSize) {
            const batch = signals.slice(i, i + batchSize);
            const statements = batch.map(s =>
              env.DB.prepare(`
                INSERT OR REPLACE INTO pxi_signal
                (date, pxi_level, delta_pxi_7d, delta_pxi_30d, category_dispersion, regime, volatility_percentile, risk_allocation, signal_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
              `).bind(
                s.date, s.pxi_level, s.delta_pxi_7d, s.delta_pxi_30d,
                s.category_dispersion, s.regime, s.volatility_percentile,
                s.risk_allocation, s.signal_type
              )
            );
            await env.DB.batch(statements);
            processed += batch.length;
          }

          return Response.json({
            success: true,
            processed,
            total: pxiScores.results.length,
            message: `Generated signal data for ${processed} dates`,
          }, { headers: corsHeaders });

        } catch (err) {
          console.error('Signal recalculation error:', err);
          return Response.json({
            error: 'Signal recalculation failed',
            details: err instanceof Error ? err.message : String(err)
          }, { status: 500, headers: corsHeaders });
        }
      }

      // Get backtest history
      if (url.pathname === '/api/backtest/history' && method === 'GET') {
        const results = await env.DB.prepare(`
          SELECT * FROM backtest_results ORDER BY run_date DESC LIMIT 10
        `).all();

        return Response.json({
          history: results.results || [],
        }, { headers: corsHeaders });
      }

      // v1.6: Export PXI history as CSV
      if (url.pathname === '/api/export/history' && method === 'GET') {
        const days = Math.min(730, Math.max(7, parseInt(url.searchParams.get('days') || '365')));
        const format = url.searchParams.get('format') || 'csv';

        // Get historical PXI scores with category breakdown
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
          // Build CSV
          const headers = ['date', 'score', 'label', 'status', 'delta_1d', 'delta_7d', 'delta_30d'];
          const csvRows = [headers.join(',')];

          for (const row of data) {
            csvRows.push([
              row.date,
              row.score.toFixed(2),
              row.label,
              row.status,
              row.delta_1d?.toFixed(2) ?? '',
              row.delta_7d?.toFixed(2) ?? '',
              row.delta_30d?.toFixed(2) ?? '',
            ].join(','));
          }

          const csv = csvRows.join('\n');

          return new Response(csv, {
            headers: {
              ...corsHeaders,
              'Content-Type': 'text/csv',
              'Content-Disposition': `attachment; filename="pxi-history-${new Date().toISOString().split('T')[0]}.csv"`,
            },
          });
        }

        // JSON format
        return Response.json({
          data,
          count: data.length,
          exported_at: new Date().toISOString(),
        }, { headers: corsHeaders });
      }

      // Export training data for ML model (requires auth)
      if (url.pathname === '/api/export/training-data' && method === 'GET') {
        const adminAuthFailure = await enforceAdminAuth(request, env, corsHeaders, clientIP);
        if (adminAuthFailure) {
          return adminAuthFailure;
        }

        // Get all PXI scores with deltas
        const pxiData = await env.DB.prepare(`
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
        }>();

        // Get SPY prices for return calculation
        const spyPrices = await env.DB.prepare(`
          SELECT date, value FROM indicator_values
          WHERE indicator_id = 'spy_close'
          ORDER BY date ASC
        `).all<{ date: string; value: number }>();

        // Build SPY price lookup map
        const spyMap = new Map<string, number>();
        for (const p of spyPrices.results || []) {
          spyMap.set(p.date, p.value);
        }

        // Helper to get SPY price on or after a date (handle weekends)
        const getSpyPrice = (dateStr: string, maxDays: number = 5): number | null => {
          const date = new Date(dateStr);
          for (let i = 0; i <= maxDays; i++) {
            const check = new Date(date);
            check.setDate(check.getDate() + i);
            const key = check.toISOString().split('T')[0];
            if (spyMap.has(key)) return spyMap.get(key)!;
          }
          return null;
        };

        // Get category scores for each date
        const categoryData = await env.DB.prepare(`
          SELECT date, category, score FROM category_scores ORDER BY date, category
        `).all<{ date: string; category: string; score: number }>();

        // Get indicator values for each date
        const indicatorData = await env.DB.prepare(`
          SELECT date, indicator_id, value FROM indicator_values ORDER BY date, indicator_id
        `).all<{ date: string; indicator_id: string; value: number }>();

        // Group categories and indicators by date
        const categoryMap = new Map<string, Record<string, number>>();
        for (const c of categoryData.results || []) {
          if (!categoryMap.has(c.date)) categoryMap.set(c.date, {});
          categoryMap.get(c.date)![c.category] = c.score;
        }

        const indicatorMap = new Map<string, Record<string, number>>();
        for (const i of indicatorData.results || []) {
          if (!indicatorMap.has(i.date)) indicatorMap.set(i.date, {});
          indicatorMap.get(i.date)![i.indicator_id] = i.value;
        }

        // Build training records with calculated SPY returns
        const trainingData = (pxiData.results || []).map(p => {
          // Calculate SPY forward returns
          const spyNow = getSpyPrice(p.date);
          let spyReturn7d: number | null = null;
          let spyReturn30d: number | null = null;

          if (spyNow) {
            const date = new Date(p.date);

            // 7-day forward return
            const date7d = new Date(date);
            date7d.setDate(date7d.getDate() + 7);
            const spy7d = getSpyPrice(date7d.toISOString().split('T')[0]);
            if (spy7d) {
              spyReturn7d = ((spy7d - spyNow) / spyNow) * 100;
            }

            // 30-day forward return
            const date30d = new Date(date);
            date30d.setDate(date30d.getDate() + 30);
            const spy30d = getSpyPrice(date30d.toISOString().split('T')[0]);
            if (spy30d) {
              spyReturn30d = ((spy30d - spyNow) / spyNow) * 100;
            }
          }

          return {
            date: p.date,
            // Target variables - actual SPY returns (%)
            spy_return_7d: spyReturn7d,
            spy_return_30d: spyReturn30d,
            // PXI features
            pxi_score: p.score,
            pxi_delta_1d: p.delta_1d,
            pxi_delta_7d: p.delta_7d,
            pxi_delta_30d: p.delta_30d,
            pxi_label: p.label,
            pxi_status: p.status,
            // Category scores
            categories: categoryMap.get(p.date) || {},
            // Raw indicators
            indicators: indicatorMap.get(p.date) || {},
          };
        });

        return Response.json({
          count: trainingData.length,
          spy_data_points: spyPrices.results?.length || 0,
          data: trainingData,
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
