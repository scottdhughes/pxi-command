// Cloudflare Worker API for PXI
// Uses D1 (SQLite), Vectorize, and Workers AI
// Includes scheduled cron handler for daily data refresh

import { EmailMessage } from 'cloudflare:email';
import {
  MONITORED_SLA_INDICATORS,
  evaluateSla,
  isChronicStaleness,
  resolveIndicatorSla,
  resolveStalePolicy,
} from '../src/config/indicator-sla';
import { INDICATORS } from '../src/config/indicators.js';

interface Env {
  DB: D1Database;
  VECTORIZE: VectorizeIndex;
  AI: Ai;
  ML_MODELS: KVNamespace;
  RATE_LIMIT_KV?: KVNamespace;
  FRED_API_KEY?: string;
  WRITE_API_KEY?: string;
  EMAIL_OUTBOUND?: {
    send(message: EmailMessage): Promise<void>;
  };
  ALERTS_FROM_EMAIL?: string;
  ALERTS_SIGNING_SECRET?: string;
  FEATURE_ENABLE_BRIEF?: string;
  FEATURE_ENABLE_OPPORTUNITIES?: string;
  FEATURE_ENABLE_PLAN?: string;
  FEATURE_ENABLE_ALERTS_EMAIL?: string;
  FEATURE_ENABLE_ALERTS_IN_APP?: string;
  FEATURE_ENABLE_OPPORTUNITY_COHERENCE_GATE?: string;
  FEATURE_ENABLE_CALIBRATION_DIAGNOSTICS?: string;
  FEATURE_ENABLE_EDGE_DIAGNOSTICS?: string;
  FEATURE_ENABLE_SIGNALS_SANITIZER?: string;
  FEATURE_ENABLE_DECISION_IMPACT?: string;
  FEATURE_ENABLE_DECISION_IMPACT_ENFORCE?: string;
  FEATURE_ENABLE_CTA_INTENT_TRACKING?: string;
  ENABLE_BRIEF?: string;
  ENABLE_OPPORTUNITIES?: string;
  ENABLE_PLAN?: string;
  ENABLE_ALERTS_EMAIL?: string;
  ENABLE_ALERTS_IN_APP?: string;
  ENABLE_OPPORTUNITY_COHERENCE_GATE?: string;
  ENABLE_CALIBRATION_DIAGNOSTICS?: string;
  ENABLE_EDGE_DIAGNOSTICS?: string;
  ENABLE_SIGNALS_SANITIZER?: string;
  ENABLE_DECISION_IMPACT?: string;
  ENABLE_DECISION_IMPACT_ENFORCE?: string;
  ENABLE_CTA_INTENT_TRACKING?: string;
  DECISION_IMPACT_ENFORCE_MIN_SAMPLE?: string;
  DECISION_IMPACT_ENFORCE_MIN_ACTIONABLE_SESSIONS?: string;
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
type PolicyStance = 'RISK_ON' | 'RISK_OFF' | 'MIXED';
type ConsistencyState = 'PASS' | 'WARN' | 'FAIL';
type OpportunityDirection = 'bullish' | 'bearish' | 'neutral';
type MarketAlertType = 'regime_change' | 'threshold_cross' | 'opportunity_spike' | 'freshness_warning';
type AlertSeverity = 'info' | 'warning' | 'critical';
type ConflictState = 'ALIGNED' | 'MIXED' | 'CONFLICT';
type EdgeQualityLabel = 'HIGH' | 'MEDIUM' | 'LOW';
type CalibrationQuality = 'ROBUST' | 'LIMITED' | 'INSUFFICIENT';
type CoverageQuality = CalibrationQuality;
type PlanActionabilityState = 'ACTIONABLE' | 'WATCH' | 'NO_ACTION';
type OpportunityTtlState = 'fresh' | 'stale' | 'overdue' | 'unknown';
type UtilityEventType =
  | 'session_start'
  | 'plan_view'
  | 'opportunities_view'
  | 'decision_actionable_view'
  | 'decision_watch_view'
  | 'decision_no_action_view'
  | 'no_action_unlock_view'
  | 'cta_action_click';
type PlanActionabilityReasonCode =
  | 'critical_data_quality_block'
  | 'consistency_fail_block'
  | 'opportunity_reference_unavailable'
  | 'no_eligible_opportunities'
  | 'high_edge_override_no_eligible'
  | 'high_edge_with_eligible_opportunities'
  | 'medium_edge_watch'
  | 'low_edge_watch'
  | 'cross_horizon_conflict_watch'
  | 'cross_horizon_insufficient_watch'
  | 'fallback_degraded_mode'
  | `opportunity_${string}`;
type OpportunityExpectancyBasis = 'theme_direction' | 'theme_direction_shrunk_prior' | 'direction_prior_proxy' | 'none';
type OpportunityConfidenceBand = 'high' | 'medium' | 'low';
type OpportunityEligibilityCheck =
  | 'neutral_direction_not_actionable'
  | 'calibration_probability_below_threshold'
  | 'expectancy_sign_conflict'
  | 'incomplete_contract';
type OpportunityCtaDisabledReason =
  | 'no_eligible_opportunities'
  | 'suppressed_data_quality'
  | 'calibration_quality_not_robust'
  | 'calibration_ece_unavailable'
  | 'ece_above_threshold'
  | 'refresh_ttl_overdue'
  | 'refresh_ttl_unknown';

interface OpportunityEligibility {
  passed: boolean;
  failed_checks: OpportunityEligibilityCheck[];
}

interface OpportunityDecisionContract {
  coherent: boolean;
  confidence_band: OpportunityConfidenceBand;
  rationale_codes: string[];
}

interface EdgeQualityCalibration {
  bin: string | null;
  probability_correct_7d: number | null;
  ci95_low_7d: number | null;
  ci95_high_7d: number | null;
  sample_size_7d: number;
  quality: CalibrationQuality;
}

interface OpportunityCalibration {
  probability_correct_direction: number | null;
  ci95_low: number | null;
  ci95_high: number | null;
  sample_size: number;
  quality: CalibrationQuality;
  basis: 'conviction_decile';
  window: string | null;
  unavailable_reason: string | null;
}

interface CalibrationBinSnapshot {
  bin: string;
  correct_count: number;
  probability_correct: number | null;
  ci95_low: number | null;
  ci95_high: number | null;
  sample_size: number;
  quality: CalibrationQuality;
}

interface MarketCalibrationSnapshotPayload {
  as_of: string;
  metric: 'edge_quality' | 'conviction';
  horizon: '7d' | '30d' | null;
  basis: 'edge_quality_decile' | 'conviction_decile';
  bins: CalibrationBinSnapshot[];
  total_samples: number;
}

interface CalibrationDiagnosticsSnapshot {
  brier_score: number | null;
  ece: number | null;
  log_loss: number | null;
  quality_band: CalibrationQuality;
  minimum_reliable_sample: number;
  insufficient_reasons: string[];
}

type EdgeDiagnosticsHorizon = '7d' | '30d';

interface EdgeLeakageSentinel {
  pass: boolean;
  violation_count: number;
  reasons: string[];
}

interface EdgeDiagnosticsWindow {
  horizon: EdgeDiagnosticsHorizon;
  as_of: string | null;
  sample_size: number;
  model_direction_accuracy: number | null;
  baseline_direction_accuracy: number | null;
  uplift_vs_baseline: number | null;
  uplift_ci95_low: number | null;
  uplift_ci95_high: number | null;
  lower_bound_positive: boolean;
  minimum_reliable_sample: number;
  quality_band: CalibrationQuality;
  baseline_strategy: 'lagged_actual_direction';
  leakage_sentinel: EdgeLeakageSentinel;
  calibration_diagnostics: CalibrationDiagnosticsSnapshot;
}

interface EdgeDiagnosticsReport {
  as_of: string;
  basis: string;
  windows: EdgeDiagnosticsWindow[];
  promotion_gate: {
    pass: boolean;
    reasons: string[];
  };
}

interface FreshnessStatus {
  has_stale_data: boolean;
  stale_count: number;
  critical_stale_count: number;
}

interface FreshnessSloIncident {
  as_of: string;
  completed_at: string | null;
  trigger: string | null;
  stale_count: number;
  critical_stale_count: number;
}

interface FreshnessImpactEventSummary {
  created_at: string;
  severity: AlertSeverity;
  title: string;
  body: string;
}

interface FreshnessSloImpactSummary {
  state: 'none' | 'monitor' | 'degraded';
  stale_days: number;
  warning_events: number;
  critical_events: number;
  estimated_suppressed_days: number;
  latest_warning_event: FreshnessImpactEventSummary | null;
  latest_critical_event: FreshnessImpactEventSummary | null;
}

interface FreshnessSloWindowSummary {
  days_observed: number;
  days_with_critical_stale: number;
  slo_attainment_pct: number;
  recent_incidents: FreshnessSloIncident[];
  incident_impact: FreshnessSloImpactSummary;
}

interface BriefSnapshot {
  as_of: string;
  summary: string;
  regime_delta: RegimeDelta;
  top_changes: string[];
  risk_posture: RiskPosture;
  policy_state: PolicyStateSnapshot;
  source_plan_as_of: string;
  contract_version: string;
  consistency: ConsistencySnapshot;
  explainability: {
    category_movers: Array<{ category: string; score_change: number }>;
    indicator_movers: Array<{ indicator_id: string; value_change: number; z_impact: number }>;
  };
  freshness_status: {
    has_stale_data: boolean;
    stale_count: number;
    critical_stale_count: number;
  };
  updated_at: string;
  degraded_reason: string | null;
}

interface OpportunityExpectancy {
  expected_move_pct: number | null;
  max_adverse_move_pct: number | null;
  sample_size: number;
  basis: OpportunityExpectancyBasis;
  quality: CoverageQuality;
  unavailable_reason: string | null;
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
  calibration: OpportunityCalibration;
  expectancy: OpportunityExpectancy;
  eligibility: OpportunityEligibility;
  decision_contract: OpportunityDecisionContract;
  updated_at: string;
}

interface OpportunitySnapshot {
  as_of: string;
  horizon: '7d' | '30d';
  items: OpportunityItem[];
}

interface OpportunitySuppressionByReason {
  coherence_failed: number;
  quality_filtered: number;
  data_quality_suppressed: number;
}

interface OpportunityTtlMetadata {
  data_age_seconds: number | null;
  ttl_state: OpportunityTtlState;
  next_expected_refresh_at: string | null;
  overdue_seconds: number | null;
}

interface UtilityEventInsertPayload {
  session_id: string;
  event_type: UtilityEventType;
  route: string | null;
  actionability_state: PlanActionabilityState | null;
  payload_json: string | null;
  created_at: string;
}

interface UtilityFunnelSummary {
  window_days: number;
  days_observed: number;
  total_events: number;
  unique_sessions: number;
  plan_views: number;
  opportunities_views: number;
  decision_actionable_views: number;
  decision_watch_views: number;
  decision_no_action_views: number;
  no_action_unlock_views: number;
  cta_action_clicks: number;
  actionable_view_sessions: number;
  actionable_sessions: number;
  cta_action_rate_pct: number;
  decision_events_total: number;
  decision_events_per_session: number;
  no_action_unlock_coverage_pct: number;
  last_event_at: string | null;
}

type DecisionImpactOutcomeBasis = 'spy_forward_proxy' | 'theme_proxy_blend';

interface DecisionImpactMarketStats {
  sample_size: number;
  hit_rate: number;
  avg_forward_return_pct: number;
  avg_signed_return_pct: number;
  win_rate: number;
  downside_p10_pct: number;
  max_loss_pct: number;
  quality_band: CalibrationQuality;
}

interface DecisionImpactThemeStats {
  theme_id: string;
  theme_name: string;
  sample_size: number;
  hit_rate: number;
  avg_signed_return_pct: number;
  avg_forward_return_pct: number;
  win_rate: number;
  quality_band: CalibrationQuality;
  last_as_of: string;
}

interface DecisionImpactCoverage {
  matured_items: number;
  eligible_items: number;
  coverage_ratio: number;
  insufficient_reasons: string[];
  theme_proxy_eligible_items?: number;
  spy_fallback_items?: number;
}

interface DecisionImpactResponsePayload {
  as_of: string;
  horizon: '7d' | '30d';
  scope: 'market' | 'theme';
  window_days: 30 | 90;
  outcome_basis: DecisionImpactOutcomeBasis;
  market: DecisionImpactMarketStats;
  themes: DecisionImpactThemeStats[];
  coverage: DecisionImpactCoverage;
}

interface DecisionImpactObserveSnapshot {
  enabled: true;
  mode: 'observe' | 'enforce';
  thresholds: {
    market_7d_hit_rate_min: number;
    market_30d_hit_rate_min: number;
    market_7d_avg_signed_return_min: number;
    market_30d_avg_signed_return_min: number;
    cta_action_rate_pct_min: number;
  };
  minimum_samples_required: number;
  minimum_actionable_sessions_required: number;
  enforce_ready: boolean;
  enforce_breaches: string[];
  enforce_breach_count: number;
  breaches: string[];
  breach_count: number;
}

interface DecisionImpactOpsResponsePayload {
  as_of: string;
  window_days: 30 | 90;
  market_7d: DecisionImpactMarketStats;
  market_30d: DecisionImpactMarketStats;
  theme_summary: {
    themes_with_samples: number;
    themes_robust: number;
    top_positive: DecisionImpactThemeStats[];
    top_negative: DecisionImpactThemeStats[];
  };
  utility_attribution: {
    actionable_views: number;
    actionable_sessions: number;
    cta_action_clicks: number;
    cta_action_rate_pct: number;
    no_action_unlock_views: number;
    decision_events_total: number;
  };
  observe_mode: DecisionImpactObserveSnapshot;
}

interface OpportunityItemLedgerInsertPayload {
  refresh_run_id: number | null;
  as_of: string;
  horizon: '7d' | '30d';
  opportunity_id: string;
  theme_id: string;
  theme_name: string;
  direction: OpportunityDirection;
  conviction_score: number;
  published: 0 | 1;
  suppression_reason: 'coherence_failed' | 'quality_filtered' | 'suppressed_data_quality' | null;
}

interface OpportunityLedgerInsertPayload {
  refresh_run_id: number | null;
  as_of: string;
  horizon: '7d' | '30d';
  candidate_count: number;
  published_count: number;
  suppressed_count: number;
  quality_filtered_count: number;
  coherence_suppressed_count: number;
  data_quality_suppressed_count: number;
  degraded_reason: string | null;
  top_direction_candidate: OpportunityDirection | null;
  top_direction_published: OpportunityDirection | null;
}

interface OpportunityLedgerRow extends OpportunityLedgerInsertPayload {
  created_at: string;
}

interface OpportunityLedgerWindowMetrics {
  window_days: number;
  rows_observed: number;
  candidate_count_total: number;
  published_count_total: number;
  publish_rate_pct: number;
  suppressed_count_total: number;
  over_suppressed_rows: number;
  over_suppression_rate_pct: number;
  paired_days: number;
  cross_horizon_conflict_days: number;
  cross_horizon_conflict_rate_pct: number;
  conflict_persistence_days: number;
  last_as_of: string | null;
}

type DecisionGradeComponentStatus = 'pass' | 'watch' | 'fail' | 'insufficient';

interface DecisionGradeResponse {
  as_of: string;
  window_days: number;
  score: number;
  grade: 'GREEN' | 'YELLOW' | 'RED';
  go_live_ready: boolean;
  components: {
    freshness: {
      score: number;
      status: DecisionGradeComponentStatus;
      slo_attainment_pct: number;
      days_with_critical_stale: number;
      days_observed: number;
    };
    consistency: {
      score: number;
      status: DecisionGradeComponentStatus;
      pass_count: number;
      warn_count: number;
      fail_count: number;
      total: number;
    };
    calibration: {
      score: number;
      status: DecisionGradeComponentStatus;
      conviction_7d: CalibrationQuality;
      conviction_30d: CalibrationQuality;
      edge_quality: CalibrationQuality;
    };
    edge: {
      score: number;
      status: DecisionGradeComponentStatus;
      promotion_gate_pass: boolean;
      lower_bound_positive_horizons: number;
      horizons_observed: number;
      reasons: string[];
    };
    opportunity_hygiene: {
      score: number;
      status: DecisionGradeComponentStatus;
      publish_rate_pct: number;
      over_suppression_rate_pct: number;
      cross_horizon_conflict_rate_pct: number;
      conflict_persistence_days: number;
      rows_observed: number;
    };
    utility: {
      score: number;
      status: DecisionGradeComponentStatus;
      decision_events_total: number;
      no_action_unlock_coverage_pct: number;
      unique_sessions: number;
    };
  };
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
  calibration: EdgeQualityCalibration;
}

interface PolicyStateSnapshot {
  stance: PolicyStance;
  risk_posture: RiskPosture;
  conflict_state: ConflictState;
  base_signal: SignalType;
  regime_context: RegimeType;
  rationale: string;
  rationale_codes: string[];
}

interface UncertaintySnapshot {
  headline: string | null;
  flags: {
    stale_inputs: boolean;
    limited_calibration: boolean;
    limited_scenario_sample: boolean;
  };
}

interface ConsistencySnapshot {
  score: number;
  state: ConsistencyState;
  violations: string[];
  components: {
    base_score: number;
    structural_penalty: number;
    reliability_penalty: number;
  };
}

interface TraderPlaybookSnapshot {
  recommended_size_pct: {
    min: number;
    target: number;
    max: number;
  };
  scenarios: Array<{
    condition: string;
    action: string;
    invalidation: string;
  }>;
  benchmark_follow_through_7d: {
    hit_rate: number | null;
    sample_size: number;
    unavailable_reason: string | null;
  };
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
  policy_state: PolicyStateSnapshot;
  actionability_state: PlanActionabilityState;
  actionability_reason_codes: PlanActionabilityReasonCode[];
  action_now: {
    risk_allocation_target: number;
    raw_signal_allocation_target: number;
    risk_allocation_basis: 'penalized_playbook_target' | 'fallback_neutral';
    horizon_bias: string;
    primary_signal: SignalType;
  };
  edge_quality: EdgeQualitySnapshot;
  risk_band: {
    d7: PlanRiskBand;
    d30: PlanRiskBand;
  };
  uncertainty: UncertaintySnapshot;
  consistency: ConsistencySnapshot;
  trader_playbook: TraderPlaybookSnapshot;
  brief_ref?: {
    as_of: string;
    regime_delta: RegimeDelta;
    risk_posture: RiskPosture;
  };
  opportunity_ref?: {
    as_of: string;
    horizon: '7d' | '30d';
    eligible_count: number;
    suppressed_count: number;
    degraded_reason: string | null;
  };
  alerts_ref?: {
    as_of: string;
    warning_count_24h: number;
    critical_count_24h: number;
  };
  cross_horizon?: {
    as_of: string;
    state: 'ALIGNED' | 'MIXED' | 'CONFLICT' | 'INSUFFICIENT';
    eligible_7d: number;
    eligible_30d: number;
    top_direction_7d: OpportunityDirection | null;
    top_direction_30d: OpportunityDirection | null;
    rationale_codes: string[];
    invalidation_note: string | null;
  };
  decision_stack?: {
    what_changed: string;
    what_to_do: string;
    why_now: string;
    confidence: string;
    cta_state: PlanActionabilityState;
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

const BRIEF_CONTRACT_VERSION = '2026-02-17-v2';
const CONSISTENCY_PASS_MIN = 90;
const CONSISTENCY_WARN_MIN = 80;
const REFRESH_HOURS_UTC = [6, 14, 18, 22];
const MINIMUM_RELIABLE_SAMPLE = 30;
const CALIBRATION_DIAGNOSTICS_MIN_SAMPLE = 30;
const CALIBRATION_ROBUST_MIN_SAMPLE = 50;
const CALIBRATION_LIMITED_MIN_SAMPLE = 20;
const CALIBRATION_POOL_TARGET_SAMPLE = CALIBRATION_ROBUST_MIN_SAMPLE;
const OPPORTUNITY_COHERENCE_MIN_PROBABILITY = 0.5;
const OPPORTUNITY_CTA_MAX_ECE = 0.08;
const OPPORTUNITY_REFRESH_TTL_GRACE_SECONDS = 90 * 60;
const EDGE_DIAGNOSTICS_MAX_ROWS = 5000;
const SIGNALS_TICKER_REGEX = /^[A-Z]{2,5}$/;
const SIGNALS_TICKER_LIMIT = 4;
const SIGNALS_TICKER_STOPWORDS = new Set<string>([
  'GPT',
  'LLM',
  'GPU',
  'ASIC',
  'HBM',
  'SOFR',
  'IORB',
  'METR',
  'WI',
  'SRF',
]);
const INDICATOR_FREQUENCY_HINTS = new Map<string, string>(
  INDICATORS.map((indicator) => [indicator.id, indicator.frequency])
);
const FRESHNESS_MONITORED_INDICATORS = new Set<string>([
  ...INDICATORS.map((indicator) => indicator.id),
  ...MONITORED_SLA_INDICATORS,
]);

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

// Rate limiting: simple in-memory store
const publicRateLimitStore = new Map<string, { count: number; resetTime: number }>();
const adminRateLimitStore = new Map<string, { count: number; resetTime: number }>();
const RATE_LIMIT = 100;
const RATE_WINDOW = 60 * 1000;
const ADMIN_RATE_LIMIT = 20;
const ADMIN_RATE_WINDOW = 60 * 1000;
const MAX_BACKFILL_LIMIT = 365;
const UTILITY_EVENT_TYPES: UtilityEventType[] = [
  'session_start',
  'plan_view',
  'opportunities_view',
  'decision_actionable_view',
  'decision_watch_view',
  'decision_no_action_view',
  'no_action_unlock_view',
  'cta_action_click',
];
const UTILITY_ROUTE_ALLOWLIST = new Set<string>([
  '/',
  '/brief',
  '/opportunities',
  '/inbox',
  '/alerts',
  '/spec',
  '/guide',
]);
const UTILITY_SESSION_ID_RE = /^[A-Za-z0-9_-]{12,96}$/;
const UTILITY_PAYLOAD_MAX_CHARS = 2000;
const UTILITY_WINDOW_DAY_OPTIONS = new Set<number>([7, 30]);
const DECISION_IMPACT_WINDOW_DAY_OPTIONS = new Set<number>([30, 90]);
const DECISION_IMPACT_OBSERVE_THRESHOLDS = {
  market_7d_hit_rate_min: 0.52,
  market_30d_hit_rate_min: 0.50,
  market_7d_avg_signed_return_min: 0,
  market_30d_avg_signed_return_min: 0,
  cta_action_rate_pct_min: 2.0,
} as const;
const DECISION_IMPACT_ENFORCE_MIN_SAMPLE_DEFAULT = 30;
const DECISION_IMPACT_ENFORCE_MIN_ACTIONABLE_SESSIONS_DEFAULT = 10;

interface ThemeProxyRule {
  indicator_id: string;
  weight: number;
  invert: boolean;
}

const THEME_PROXY_RULES = new Map<string, ThemeProxyRule[]>([
  ['credit', [
    { indicator_id: 'hyg', weight: 0.65, invert: false },
    { indicator_id: 'lqd', weight: 0.35, invert: false },
  ]],
  ['volatility', [
    { indicator_id: 'vix', weight: 1, invert: true },
  ]],
  ['breadth', [
    { indicator_id: 'spy_close', weight: 1, invert: false },
  ]],
  ['positioning', [
    { indicator_id: 'spy_close', weight: 1, invert: false },
  ]],
  ['macro', [
    { indicator_id: 'spy_close', weight: 0.7, invert: false },
    { indicator_id: 'wti_crude', weight: 0.3, invert: false },
  ]],
  ['global', [
    { indicator_id: 'copper_gold_ratio', weight: 0.6, invert: false },
    { indicator_id: 'dxy', weight: 0.4, invert: true },
  ]],
  ['crypto', [
    { indicator_id: 'btc_price', weight: 1, invert: false },
  ]],
]);

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

function resolveDecisionImpactGovernance(env: Env): DecisionImpactGovernanceOptions {
  return {
    enforce_enabled: isFeatureEnabled(
      env,
      'FEATURE_ENABLE_DECISION_IMPACT_ENFORCE',
      'ENABLE_DECISION_IMPACT_ENFORCE',
      false,
    ),
    min_sample_size: parseEnvIntInRange(
      env.DECISION_IMPACT_ENFORCE_MIN_SAMPLE,
      DECISION_IMPACT_ENFORCE_MIN_SAMPLE_DEFAULT,
      10,
      1000,
    ),
    min_actionable_sessions: parseEnvIntInRange(
      env.DECISION_IMPACT_ENFORCE_MIN_ACTIONABLE_SESSIONS,
      DECISION_IMPACT_ENFORCE_MIN_ACTIONABLE_SESSIONS_DEFAULT,
      1,
      1000,
    ),
  };
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

function toIntInRange(
  value: unknown,
  fallback: number,
  min: number,
  max: number,
): number {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function parseEnvIntInRange(
  raw: unknown,
  fallback: number,
  min: number,
  max: number,
): number {
  if (raw === undefined || raw === null || raw === '') {
    return fallback;
  }
  return toIntInRange(raw, fallback, min, max);
}

function normalizeThemeIdForProxy(themeId: string): string {
  return String(themeId || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function resolveThemeProxyRules(themeId: string): ThemeProxyRule[] {
  const normalized = normalizeThemeIdForProxy(themeId);
  if (!normalized) {
    return [{ indicator_id: 'spy_close', weight: 1, invert: false }];
  }

  const direct = THEME_PROXY_RULES.get(normalized);
  if (direct && direct.length > 0) {
    return direct;
  }

  if (normalized.includes('credit')) {
    return THEME_PROXY_RULES.get('credit') || [{ indicator_id: 'spy_close', weight: 1, invert: false }];
  }
  if (normalized.includes('vol')) {
    return THEME_PROXY_RULES.get('volatility') || [{ indicator_id: 'spy_close', weight: 1, invert: false }];
  }
  if (normalized.includes('breadth')) {
    return THEME_PROXY_RULES.get('breadth') || [{ indicator_id: 'spy_close', weight: 1, invert: false }];
  }
  if (normalized.includes('macro') || normalized.includes('growth') || normalized.includes('liquidity')) {
    return THEME_PROXY_RULES.get('macro') || [{ indicator_id: 'spy_close', weight: 1, invert: false }];
  }
  if (normalized.includes('global') || normalized.includes('fx')) {
    return THEME_PROXY_RULES.get('global') || [{ indicator_id: 'spy_close', weight: 1, invert: false }];
  }
  if (normalized.includes('crypto') || normalized.includes('btc') || normalized.includes('eth')) {
    return THEME_PROXY_RULES.get('crypto') || [{ indicator_id: 'spy_close', weight: 1, invert: false }];
  }

  return [{ indicator_id: 'spy_close', weight: 1, invert: false }];
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

function signalTypeFromValue(
  rawSignalType: string | null | undefined,
  predictedChange: number
): 'FULL_RISK' | 'REDUCED_RISK' | 'RISK_OFF' | 'DEFENSIVE' {
  const normalized = String(rawSignalType || '').toUpperCase();
  if (normalized === 'FULL_RISK') return 'FULL_RISK';
  if (normalized === 'REDUCED_RISK') return 'REDUCED_RISK';
  if (normalized === 'RISK_OFF') return 'RISK_OFF';
  if (normalized === 'DEFENSIVE') return 'DEFENSIVE';
  if (predictedChange < -0.5) return 'RISK_OFF';
  if (predictedChange > 0.5) return 'FULL_RISK';
  return 'REDUCED_RISK';
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

function buildDecileBinLabel(score: number): string {
  const normalized = Math.round(clamp(0, 100, score));
  const binStart = normalized === 100 ? 90 : Math.floor(normalized / 10) * 10;
  const binEnd = binStart === 90 ? 100 : binStart + 9;
  return `${binStart}-${binEnd}`;
}

function calibrationQualityForSampleSize(sampleSize: number): CalibrationQuality {
  if (sampleSize >= CALIBRATION_ROBUST_MIN_SAMPLE) return 'ROBUST';
  if (sampleSize >= CALIBRATION_LIMITED_MIN_SAMPLE) return 'LIMITED';
  return 'INSUFFICIENT';
}

function computeWilson95(correct: number, sampleSize: number): {
  probability: number;
  ci95_low: number;
  ci95_high: number;
} {
  if (sampleSize <= 0) {
    return { probability: 0, ci95_low: 0, ci95_high: 0 };
  }
  const z = 1.96;
  const p = correct / sampleSize;
  const z2OverN = (z * z) / sampleSize;
  const denom = 1 + z2OverN;
  const center = (p + (z * z) / (2 * sampleSize)) / denom;
  const spread = (z * Math.sqrt((p * (1 - p) + (z * z) / (4 * sampleSize)) / sampleSize)) / denom;
  return {
    probability: clamp(0, 1, p),
    ci95_low: clamp(0, 1, center - spread),
    ci95_high: clamp(0, 1, center + spread),
  };
}

function buildEdgeCalibrationFallback(bin: string | null): EdgeQualityCalibration {
  return {
    bin,
    probability_correct_7d: null,
    ci95_low_7d: null,
    ci95_high_7d: null,
    sample_size_7d: 0,
    quality: 'INSUFFICIENT',
  };
}

function buildOpportunityCalibrationFallback(): OpportunityCalibration {
  return {
    probability_correct_direction: null,
    ci95_low: null,
    ci95_high: null,
    sample_size: 0,
    quality: 'INSUFFICIENT',
    basis: 'conviction_decile',
    window: null,
    unavailable_reason: 'insufficient_sample',
  };
}

function parseCalibrationSnapshotPayload(raw: string | null | undefined): MarketCalibrationSnapshotPayload | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as Partial<MarketCalibrationSnapshotPayload>;
    if (!parsed || typeof parsed !== 'object') return null;
    if (!Array.isArray(parsed.bins)) return null;
    const bins = parsed.bins
      .map((bin): CalibrationBinSnapshot | null => {
        if (!bin || typeof bin !== 'object') return null;
        const candidate = bin as Partial<CalibrationBinSnapshot>;
        if (typeof candidate.bin !== 'string') return null;
        const sampleSize = toNumber(candidate.sample_size, 0);
        const inferredCorrectCount = Math.round(
          sampleSize > 0 && candidate.probability_correct !== null && candidate.probability_correct !== undefined
            ? clamp(0, 1, toNumber(candidate.probability_correct, 0)) * sampleSize
            : 0
        );
        const correctCount = Math.max(0, Math.min(Math.floor(sampleSize), Math.floor(toNumber(candidate.correct_count, inferredCorrectCount))));
        const probability = candidate.probability_correct === null || candidate.probability_correct === undefined
          ? null
          : clamp(0, 1, toNumber(candidate.probability_correct, 0));
        const ci95Low = candidate.ci95_low === null || candidate.ci95_low === undefined
          ? null
          : clamp(0, 1, toNumber(candidate.ci95_low, 0));
        const ci95High = candidate.ci95_high === null || candidate.ci95_high === undefined
          ? null
          : clamp(0, 1, toNumber(candidate.ci95_high, 0));
        return {
          bin: candidate.bin,
          correct_count: correctCount,
          probability_correct: probability,
          ci95_low: ci95Low,
          ci95_high: ci95High,
          sample_size: Math.max(0, Math.floor(sampleSize)),
          quality: calibrationQualityForSampleSize(sampleSize),
        };
      })
      .filter((value): value is CalibrationBinSnapshot => Boolean(value));
    return {
      as_of: String(parsed.as_of || ''),
      metric: parsed.metric === 'conviction' ? 'conviction' : 'edge_quality',
      horizon: parsed.horizon === '30d' ? '30d' : (parsed.horizon === '7d' ? '7d' : null),
      basis: parsed.basis === 'conviction_decile' ? 'conviction_decile' : 'edge_quality_decile',
      bins,
      total_samples: Math.max(0, Math.floor(toNumber(parsed.total_samples, 0))),
    };
  } catch {
    return null;
  }
}

function parseBinStart(bin: string): number | null {
  const match = /^(\d+)-(\d+)$/.exec(bin);
  if (!match) return null;
  const start = Number(match[1]);
  return Number.isFinite(start) ? start : null;
}

function parseBinMidpointProbability(bin: string): number | null {
  const match = /^(\d+)-(\d+)$/.exec(bin);
  if (!match) return null;
  const start = Number(match[1]);
  const end = Number(match[2]);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
  const midpointPct = (start + end) / 2;
  return clamp(0, 1, midpointPct / 100);
}

function computeCalibrationDiagnostics(
  snapshot: MarketCalibrationSnapshotPayload | null
): CalibrationDiagnosticsSnapshot {
  if (!snapshot || snapshot.total_samples < CALIBRATION_DIAGNOSTICS_MIN_SAMPLE) {
    const reasons = !snapshot
      ? ['snapshot_unavailable']
      : [`insufficient_sample_${snapshot?.total_samples ?? 0}`];
    return {
      brier_score: null,
      ece: null,
      log_loss: null,
      quality_band: 'INSUFFICIENT',
      minimum_reliable_sample: CALIBRATION_DIAGNOSTICS_MIN_SAMPLE,
      insufficient_reasons: reasons,
    };
  }

  let total = 0;
  let brierSum = 0;
  let logLossSum = 0;
  let eceSum = 0;
  let invalidBins = 0;

  for (const bin of snapshot.bins) {
    if (!bin || bin.sample_size <= 0 || bin.probability_correct === null) continue;
    const nominal = parseBinMidpointProbability(bin.bin);
    if (nominal === null) {
      invalidBins += 1;
      continue;
    }
    const observed = clamp(0, 1, toNumber(bin.probability_correct, 0));
    const weight = Math.max(0, Math.floor(toNumber(bin.sample_size, 0)));
    if (weight <= 0) continue;
    total += weight;
    brierSum += (nominal - observed) * (nominal - observed) * weight;
    eceSum += Math.abs(nominal - observed) * weight;
    const clippedObserved = clamp(1e-6, 1 - 1e-6, observed);
    const clippedNominal = clamp(1e-6, 1 - 1e-6, nominal);
    logLossSum += (
      -(
        clippedObserved * Math.log(clippedNominal) +
        (1 - clippedObserved) * Math.log(1 - clippedNominal)
      )
    ) * weight;
  }

  if (total < CALIBRATION_DIAGNOSTICS_MIN_SAMPLE) {
    const reasons = [`insufficient_scored_sample_${total}`];
    if (invalidBins > 0) reasons.push(`invalid_bins_${invalidBins}`);
    return {
      brier_score: null,
      ece: null,
      log_loss: null,
      quality_band: 'INSUFFICIENT',
      minimum_reliable_sample: CALIBRATION_DIAGNOSTICS_MIN_SAMPLE,
      insufficient_reasons: reasons,
    };
  }

  const qualityBand = calibrationQualityForSampleSize(total);
  const reasons: string[] = [];
  if (invalidBins > 0) reasons.push(`invalid_bins_${invalidBins}`);
  return {
    brier_score: brierSum / total,
    ece: eceSum / total,
    log_loss: logLossSum / total,
    quality_band: qualityBand,
    minimum_reliable_sample: CALIBRATION_DIAGNOSTICS_MIN_SAMPLE,
    insufficient_reasons: reasons,
  };
}

function directionSign(value: number): -1 | 0 | 1 {
  if (value > 0) return 1;
  if (value < 0) return -1;
  return 0;
}

function computeDifferenceCi95(
  modelCorrect: number,
  modelTotal: number,
  baselineCorrect: number,
  baselineTotal: number
): {
  uplift: number | null;
  ci95_low: number | null;
  ci95_high: number | null;
  lower_bound_positive: boolean;
} {
  if (modelTotal <= 0 || baselineTotal <= 0) {
    return {
      uplift: null,
      ci95_low: null,
      ci95_high: null,
      lower_bound_positive: false,
    };
  }

  const modelRate = clamp(0, 1, modelCorrect / modelTotal);
  const baselineRate = clamp(0, 1, baselineCorrect / baselineTotal);
  const uplift = modelRate - baselineRate;
  const variance =
    (modelRate * (1 - modelRate)) / modelTotal +
    (baselineRate * (1 - baselineRate)) / baselineTotal;
  const stdErr = Math.sqrt(Math.max(0, variance));
  const margin = 1.96 * stdErr;
  const ciLow = uplift - margin;
  const ciHigh = uplift + margin;

  return {
    uplift,
    ci95_low: ciLow,
    ci95_high: ciHigh,
    lower_bound_positive: ciLow > 0,
  };
}

function resolvePredictionColumns(horizon: EdgeDiagnosticsHorizon): {
  predicted: 'predicted_change_7d' | 'predicted_change_30d';
  actual: 'actual_change_7d' | 'actual_change_30d';
  target: 'target_date_7d' | 'target_date_30d';
} {
  if (horizon === '30d') {
    return {
      predicted: 'predicted_change_30d',
      actual: 'actual_change_30d',
      target: 'target_date_30d',
    };
  }
  return {
    predicted: 'predicted_change_7d',
    actual: 'actual_change_7d',
    target: 'target_date_7d',
  };
}

async function computeEdgeLeakageSentinel(
  db: D1Database,
  horizon: EdgeDiagnosticsHorizon
): Promise<EdgeLeakageSentinel> {
  const columns = resolvePredictionColumns(horizon);
  const row = await db.prepare(`
    SELECT
      SUM(CASE
        WHEN ${columns.actual} IS NOT NULL
         AND ${columns.target} IS NOT NULL
         AND ${columns.target} > date('now')
        THEN 1 ELSE 0 END) AS future_target_evaluated,
      SUM(CASE
        WHEN ${columns.actual} IS NOT NULL
         AND ${columns.target} IS NOT NULL
         AND prediction_date >= ${columns.target}
        THEN 1 ELSE 0 END) AS non_forward_target,
      SUM(CASE
        WHEN ${columns.actual} IS NOT NULL
         AND ${columns.target} IS NOT NULL
         AND evaluated_at IS NOT NULL
         AND datetime(replace(replace(evaluated_at, 'T', ' '), 'Z', '')) < datetime(${columns.target} || ' 00:00:00')
        THEN 1 ELSE 0 END) AS evaluated_before_target
    FROM prediction_log
    WHERE ${columns.actual} IS NOT NULL
  `).first<{
    future_target_evaluated: number | null;
    non_forward_target: number | null;
    evaluated_before_target: number | null;
  }>();

  const futureTargetEvaluated = Math.max(0, Math.floor(toNumber(row?.future_target_evaluated, 0)));
  const nonForwardTarget = Math.max(0, Math.floor(toNumber(row?.non_forward_target, 0)));
  const evaluatedBeforeTarget = Math.max(0, Math.floor(toNumber(row?.evaluated_before_target, 0)));
  const reasons: string[] = [];
  if (futureTargetEvaluated > 0) reasons.push(`future_target_evaluated_${futureTargetEvaluated}`);
  if (nonForwardTarget > 0) reasons.push(`non_forward_target_${nonForwardTarget}`);
  if (evaluatedBeforeTarget > 0) reasons.push(`evaluated_before_target_${evaluatedBeforeTarget}`);
  const violationCount = futureTargetEvaluated + nonForwardTarget + evaluatedBeforeTarget;

  return {
    pass: violationCount === 0,
    violation_count: violationCount,
    reasons,
  };
}

async function buildEdgeDiagnosticsWindow(
  db: D1Database,
  horizon: EdgeDiagnosticsHorizon
): Promise<EdgeDiagnosticsWindow> {
  const columns = resolvePredictionColumns(horizon);
  const rows = await db.prepare(`
    SELECT prediction_date, ${columns.predicted} AS predicted_change, ${columns.actual} AS actual_change
    FROM prediction_log
    WHERE ${columns.predicted} IS NOT NULL
      AND ${columns.actual} IS NOT NULL
    ORDER BY prediction_date ASC
    LIMIT ${EDGE_DIAGNOSTICS_MAX_ROWS}
  `).all<{
    prediction_date: string;
    predicted_change: number;
    actual_change: number;
  }>();

  let modelCorrect = 0;
  let baselineCorrect = 0;
  let comparableSamples = 0;
  let previousActualDirection: -1 | 0 | 1 | null = null;

  for (const row of rows.results || []) {
    const predicted = toNumber(row.predicted_change, 0);
    const actual = toNumber(row.actual_change, 0);
    if (!Number.isFinite(predicted) || !Number.isFinite(actual)) continue;
    const actualDirection = directionSign(actual);
    const predictedDirection = directionSign(predicted);

    if (previousActualDirection !== null) {
      comparableSamples += 1;
      if (predictedDirection === actualDirection) modelCorrect += 1;
      if (previousActualDirection === actualDirection) baselineCorrect += 1;
    }

    previousActualDirection = actualDirection;
  }

  const delta = computeDifferenceCi95(
    modelCorrect,
    comparableSamples,
    baselineCorrect,
    comparableSamples,
  );
  const calibrationSnapshot = await fetchLatestCalibrationSnapshot(db, 'conviction', horizon);
  const calibrationDiagnostics = computeCalibrationDiagnostics(calibrationSnapshot);
  const leakageSentinel = await computeEdgeLeakageSentinel(db, horizon);
  const lastRow = rows.results && rows.results.length > 0
    ? rows.results[rows.results.length - 1]
    : null;

  return {
    horizon,
    as_of: lastRow ? `${lastRow.prediction_date}T00:00:00.000Z` : null,
    sample_size: comparableSamples,
    model_direction_accuracy: comparableSamples > 0 ? modelCorrect / comparableSamples : null,
    baseline_direction_accuracy: comparableSamples > 0 ? baselineCorrect / comparableSamples : null,
    uplift_vs_baseline: delta.uplift,
    uplift_ci95_low: delta.ci95_low,
    uplift_ci95_high: delta.ci95_high,
    lower_bound_positive: delta.lower_bound_positive,
    minimum_reliable_sample: MINIMUM_RELIABLE_SAMPLE,
    quality_band: calibrationQualityForSampleSize(comparableSamples),
    baseline_strategy: 'lagged_actual_direction',
    leakage_sentinel: leakageSentinel,
    calibration_diagnostics: calibrationDiagnostics,
  };
}

async function buildEdgeDiagnosticsReport(
  db: D1Database,
  horizons: EdgeDiagnosticsHorizon[]
): Promise<EdgeDiagnosticsReport> {
  const uniqueHorizons = Array.from(new Set(horizons));
  const windows = await Promise.all(uniqueHorizons.map((horizon) => buildEdgeDiagnosticsWindow(db, horizon)));
  const gateReasons = windows.flatMap((window) =>
    window.leakage_sentinel.pass
      ? []
      : window.leakage_sentinel.reasons.map((reason) => `${window.horizon}:${reason}`)
  );
  const asOfCandidates = windows
    .map((window) => window.as_of)
    .filter((value): value is string => Boolean(value))
    .sort();

  return {
    as_of: asOfCandidates.length > 0 ? asOfCandidates[asOfCandidates.length - 1] : asIsoDateTime(new Date()),
    basis: 'prediction_log_forward_chain_vs_lagged_actual_baseline',
    windows,
    promotion_gate: {
      pass: gateReasons.length === 0,
      reasons: gateReasons,
    },
  };
}

function buildEdgeQualityCalibrationFromSnapshot(
  snapshot: MarketCalibrationSnapshotPayload | null,
  edgeScore: number
): EdgeQualityCalibration {
  const binLabel = buildDecileBinLabel(edgeScore);
  if (!snapshot || !Array.isArray(snapshot.bins) || snapshot.bins.length === 0) {
    return buildEdgeCalibrationFallback(null);
  }

  const targetStart = parseBinStart(binLabel) ?? 0;
  const candidates = snapshot.bins
    .map((entry) => ({
      ...entry,
      start: parseBinStart(entry.bin),
    }))
    .filter((entry): entry is CalibrationBinSnapshot & { start: number } => entry.start !== null)
    .sort((a, b) => {
      const distanceA = Math.abs(a.start - targetStart);
      const distanceB = Math.abs(b.start - targetStart);
      if (distanceA !== distanceB) return distanceA - distanceB;
      return a.start - b.start;
    });

  let pooledSamples = 0;
  let pooledCorrect = 0;
  let minStart: number | null = null;
  let maxStart: number | null = null;
  for (const candidate of candidates) {
    if (candidate.sample_size <= 0) continue;
    pooledSamples += candidate.sample_size;
    pooledCorrect += Math.max(0, Math.min(candidate.sample_size, candidate.correct_count));
    minStart = minStart === null ? candidate.start : Math.min(minStart, candidate.start);
    maxStart = maxStart === null ? candidate.start : Math.max(maxStart, candidate.start);
    if (pooledSamples >= CALIBRATION_POOL_TARGET_SAMPLE) {
      break;
    }
  }

  if (pooledSamples <= 0) {
    return buildEdgeCalibrationFallback(binLabel);
  }

  const interval = computeWilson95(pooledCorrect, pooledSamples);
  const pooledBin = minStart !== null && maxStart !== null
    ? `${minStart}-${maxStart === 90 ? 100 : maxStart + 9}`
    : binLabel;
  return {
    bin: pooledBin,
    probability_correct_7d: interval.probability,
    ci95_low_7d: interval.ci95_low,
    ci95_high_7d: interval.ci95_high,
    sample_size_7d: pooledSamples,
    quality: calibrationQualityForSampleSize(pooledSamples),
  };
}

function buildOpportunityCalibrationFromSnapshot(
  snapshot: MarketCalibrationSnapshotPayload | null,
  convictionScore: number,
  direction: OpportunityDirection
): OpportunityCalibration {
  if (direction === 'neutral') {
    return {
      probability_correct_direction: null,
      ci95_low: null,
      ci95_high: null,
      sample_size: 0,
      quality: 'INSUFFICIENT',
      basis: 'conviction_decile',
      window: null,
      unavailable_reason: 'neutral_direction',
    };
  }
  if (!snapshot || !Array.isArray(snapshot.bins) || snapshot.bins.length === 0) {
    return buildOpportunityCalibrationFallback();
  }

  const targetBin = buildDecileBinLabel(convictionScore);
  const targetStart = parseBinStart(targetBin) ?? 0;
  const candidates = snapshot.bins
    .map((entry) => ({
      ...entry,
      start: parseBinStart(entry.bin),
    }))
    .filter((entry): entry is CalibrationBinSnapshot & { start: number } => entry.start !== null)
    .sort((a, b) => {
      const distanceA = Math.abs(a.start - targetStart);
      const distanceB = Math.abs(b.start - targetStart);
      if (distanceA !== distanceB) return distanceA - distanceB;
      return a.start - b.start;
    });

  let pooledSamples = 0;
  let pooledCorrect = 0;
  let minStart: number | null = null;
  let maxStart: number | null = null;
  for (const candidate of candidates) {
    if (candidate.sample_size <= 0) continue;
    pooledSamples += candidate.sample_size;
    pooledCorrect += Math.max(0, Math.min(candidate.sample_size, candidate.correct_count));
    minStart = minStart === null ? candidate.start : Math.min(minStart, candidate.start);
    maxStart = maxStart === null ? candidate.start : Math.max(maxStart, candidate.start);
    if (pooledSamples >= CALIBRATION_POOL_TARGET_SAMPLE) {
      break;
    }
  }

  if (pooledSamples <= 0) {
    return {
      probability_correct_direction: null,
      ci95_low: null,
      ci95_high: null,
      sample_size: 0,
      quality: 'INSUFFICIENT',
      basis: 'conviction_decile',
      window: null,
      unavailable_reason: 'insufficient_sample',
    };
  }

  const interval = computeWilson95(pooledCorrect, pooledSamples);
  const quality = calibrationQualityForSampleSize(pooledSamples);
  const window = minStart !== null && maxStart !== null
    ? `${minStart}-${maxStart === 90 ? 100 : maxStart + 9}`
    : targetBin;

  return {
    probability_correct_direction: interval.probability,
    ci95_low: interval.ci95_low,
    ci95_high: interval.ci95_high,
    sample_size: pooledSamples,
    quality,
    basis: 'conviction_decile',
    window,
    unavailable_reason: pooledSamples < MINIMUM_RELIABLE_SAMPLE ? 'insufficient_sample' : null,
  };
}

function signalTypeToPolicyStance(signalType: SignalType): PolicyStance {
  if (signalType === 'RISK_OFF' || signalType === 'DEFENSIVE') {
    return 'RISK_OFF';
  }
  return 'RISK_ON';
}

function policyStanceToRiskPosture(stance: PolicyStance): RiskPosture {
  if (stance === 'RISK_ON') return 'risk_on';
  if (stance === 'RISK_OFF') return 'risk_off';
  return 'neutral';
}

function freshnessPenaltyCount(freshness: FreshnessStatus): number {
  const critical = Math.max(0, Math.floor(toNumber(freshness.critical_stale_count, 0)));
  const nonCritical = Math.max(0, Math.floor(toNumber(freshness.stale_count, 0)) - critical);
  // One non-critical stale input (typically source-lagged monthly) should not degrade plan quality.
  const nonCriticalPenalty = nonCritical <= 1 ? 0 : Math.ceil((nonCritical - 1) * 0.35);
  return critical + nonCriticalPenalty;
}

function buildPolicyStateSnapshot(params: {
  signal: PXISignal;
  regime: RegimeResult | null;
  edgeQuality: EdgeQualitySnapshot;
  freshness: FreshnessStatus;
  calibrationQuality?: CalibrationQuality | null;
}): PolicyStateSnapshot {
  const { signal, regime, edgeQuality, freshness, calibrationQuality } = params;
  const baseStance = signalTypeToPolicyStance(signal.signal_type);
  const reasons: string[] = [];

  if (edgeQuality.conflict_state === 'CONFLICT') {
    reasons.push('regime_signal_conflict');
  }
  if (edgeQuality.label === 'LOW') {
    reasons.push('low_edge_quality');
  }
  if (freshnessPenaltyCount(freshness) > 0) {
    reasons.push('stale_inputs');
  }
  if (calibrationQuality && calibrationQuality !== 'ROBUST') {
    reasons.push('limited_calibration');
  }

  const stance: PolicyStance = reasons.length > 0 ? 'MIXED' : baseStance;
  const rationale = reasons.length > 0
    ? `mixed:${reasons.join(',')}`
    : `aligned:${signal.signal_type.toLowerCase()}`;
  const rationaleCodes = reasons.length > 0 ? reasons : ['aligned'];

  return {
    stance,
    risk_posture: policyStanceToRiskPosture(stance),
    conflict_state: edgeQuality.conflict_state,
    base_signal: signal.signal_type,
    regime_context: regime?.regime || 'TRANSITION',
    rationale,
    rationale_codes: rationaleCodes,
  };
}

function buildUncertaintySnapshot(degradedReasons: string[]): UncertaintySnapshot {
  const reasonSet = new Set(degradedReasons);
  const staleInputs = reasonSet.has('stale_inputs');
  const limitedCalibration = reasonSet.has('limited_calibration_sample');
  const limitedScenarioSample = reasonSet.has('limited_scenario_sample');

  let headline: string | null = null;
  if (staleInputs && limitedCalibration) {
    headline = 'Signal quality reduced: stale inputs + limited calibration.';
  } else if (staleInputs) {
    headline = 'Signal quality reduced: stale inputs detected.';
  } else if (limitedCalibration) {
    headline = 'Signal quality reduced: limited calibration sample.';
  } else if (limitedScenarioSample) {
    headline = 'Signal quality reduced: limited scenario sample.';
  }

  return {
    headline,
    flags: {
      stale_inputs: staleInputs,
      limited_calibration: limitedCalibration,
      limited_scenario_sample: limitedScenarioSample,
    },
  };
}

interface ConsistencyReliabilityInputs {
  stale_count: number;
  calibration_quality: CalibrationQuality;
  limited_scenario_sample: boolean;
  conflict_state: ConflictState;
  allocation_target_mismatch: boolean;
}

interface RiskSizingSnapshot {
  raw_signal_allocation_target: number;
  target_pct: number;
  min_pct: number;
  max_pct: number;
}

function computeRiskSizingSnapshot(params: {
  signal: PXISignal;
  policyState: PolicyStateSnapshot;
  edgeQuality: EdgeQualitySnapshot;
  freshness: FreshnessStatus;
}): RiskSizingSnapshot {
  const { signal, policyState, edgeQuality, freshness } = params;
  const rawTarget = clamp(0, 1, signal.risk_allocation);
  const baseTargetPct = Math.round(rawTarget * 100);
  const conflictPenalty = policyState.conflict_state === 'CONFLICT' ? 8 : 0;
  const freshnessPenaltyUnits = freshnessPenaltyCount(freshness);
  const freshnessPenalty = freshnessPenaltyUnits > 0 ? Math.min(15, Math.round(freshnessPenaltyUnits * 0.6)) : 0;
  const calibrationPenalty = edgeQuality.calibration.quality === 'ROBUST'
    ? 0
    : edgeQuality.calibration.quality === 'LIMITED' ? 5 : 10;
  const target = Math.max(0, Math.min(100, baseTargetPct - conflictPenalty - freshnessPenalty - calibrationPenalty));
  const min = Math.max(0, target - 12);
  const max = Math.min(100, target + 12);

  return {
    raw_signal_allocation_target: rawTarget,
    target_pct: target,
    min_pct: min,
    max_pct: max,
  };
}

function buildConsistencySnapshot(
  policyState: PolicyStateSnapshot,
  reliability?: ConsistencyReliabilityInputs
): ConsistencySnapshot {
  const violations: string[] = [];

  if (policyState.conflict_state === 'CONFLICT' && policyState.stance !== 'MIXED') {
    violations.push('conflict_state_requires_mixed_stance');
  }

  if (
    policyState.regime_context === 'RISK_ON' &&
    (policyState.base_signal === 'RISK_OFF' || policyState.base_signal === 'DEFENSIVE') &&
    policyState.stance !== 'MIXED'
  ) {
    violations.push('risk_on_regime_with_defensive_signal_requires_mixed_stance');
  }

  if (
    policyState.regime_context === 'RISK_OFF' &&
    (policyState.base_signal === 'FULL_RISK' || policyState.base_signal === 'REDUCED_RISK') &&
    policyState.stance !== 'MIXED'
  ) {
    violations.push('risk_off_regime_with_risk_on_signal_requires_mixed_stance');
  }

  const expectedRiskPosture = policyStanceToRiskPosture(policyState.stance);
  if (policyState.risk_posture !== expectedRiskPosture) {
    violations.push('risk_posture_stance_mismatch');
  }

  const structuralPenalty = violations.length * 12;
  let reliabilityPenalty = 0;

  if (reliability) {
    if (reliability.stale_count > 0) {
      violations.push('stale_inputs_penalty');
      reliabilityPenalty += 3;
    }
    if (reliability.calibration_quality === 'LIMITED') {
      violations.push('limited_calibration_penalty');
      reliabilityPenalty += 3;
    }
    if (reliability.calibration_quality === 'INSUFFICIENT') {
      violations.push('insufficient_calibration_penalty');
      reliabilityPenalty += 6;
    }
    if (reliability.limited_scenario_sample) {
      violations.push('limited_scenario_sample_penalty');
      reliabilityPenalty += 2;
    }
    if (reliability.conflict_state === 'CONFLICT') {
      violations.push('conflict_state_penalty');
      reliabilityPenalty += 1;
    }
    if (reliability.allocation_target_mismatch) {
      violations.push('allocation_target_mismatch');
      reliabilityPenalty += 20;
    }
  }

  const score = Math.max(0, Math.round(100 - structuralPenalty - reliabilityPenalty));
  const state: ConsistencyState = score >= CONSISTENCY_PASS_MIN
    ? 'PASS'
    : score >= CONSISTENCY_WARN_MIN
      ? 'WARN'
      : 'FAIL';

  return {
    score,
    state,
    violations,
    components: {
      base_score: 100,
      structural_penalty: structuralPenalty,
      reliability_penalty: reliabilityPenalty,
    },
  };
}

async function buildBenchmarkFollowThrough7d(db: D1Database): Promise<{
  hit_rate: number | null;
  sample_size: number;
  unavailable_reason: string | null;
}> {
  const rows = await db.prepare(`
    SELECT predicted_change_7d, actual_change_7d
    FROM prediction_log
    WHERE predicted_change_7d IS NOT NULL
      AND actual_change_7d IS NOT NULL
    ORDER BY prediction_date DESC
    LIMIT 500
  `).all<{ predicted_change_7d: number; actual_change_7d: number }>();

  const samples = rows.results || [];
  if (samples.length < 20) {
    return {
      hit_rate: null,
      sample_size: samples.length,
      unavailable_reason: 'insufficient_sample',
    };
  }

  let correct = 0;
  for (const row of samples) {
    if (
      (row.predicted_change_7d > 0 && row.actual_change_7d > 0) ||
      (row.predicted_change_7d < 0 && row.actual_change_7d < 0) ||
      (row.predicted_change_7d === 0 && row.actual_change_7d === 0)
    ) {
      correct += 1;
    }
  }

  return {
    hit_rate: correct / samples.length,
    sample_size: samples.length,
    unavailable_reason: null,
  };
}

async function buildTraderPlaybookSnapshot(
  db: D1Database,
  params: {
    signal: PXISignal;
    policyState: PolicyStateSnapshot;
    edgeQuality: EdgeQualitySnapshot;
    freshness: FreshnessStatus;
    sizing?: RiskSizingSnapshot;
  }
): Promise<TraderPlaybookSnapshot> {
  const { signal, policyState, edgeQuality, freshness } = params;
  const sizing = params.sizing || computeRiskSizingSnapshot({
    signal,
    policyState,
    edgeQuality,
    freshness,
  });
  const target = sizing.target_pct;
  const min = sizing.min_pct;
  const max = sizing.max_pct;
  const staleThreshold = Math.max(2, freshnessPenaltyCount(freshness));

  const benchmark = await buildBenchmarkFollowThrough7d(db);
  const scenarios: TraderPlaybookSnapshot['scenarios'] = [
    {
      condition: 'Conflict persists or volatility percentile remains above 85.',
      action: `Hold allocation near ${min}% and prioritize downside hedges.`,
      invalidation: 'Exit scenario when conflict clears and volatility normalizes.',
    },
    {
      condition: `Calibration improves to ROBUST and stale-input pressure drops below ${staleThreshold}.`,
      action: `Increase toward ${max}% risk in one tier increments.`,
      invalidation: 'Stop increasing if edge quality drops to LOW.',
    },
    {
      condition: 'Regime flips to RISK_OFF or primary signal shifts to DEFENSIVE.',
      action: 'Move to defensive allocation immediately.',
      invalidation: 'Only re-risk after two consecutive non-defensive plan updates.',
    },
  ];

  return {
    recommended_size_pct: { min, target, max },
    scenarios,
    benchmark_follow_through_7d: benchmark,
  };
}

async function selectLatestPxiWithCategories(db: D1Database): Promise<{
  pxi: PXIRow | null;
  categories: CategoryRow[];
}> {
  const recentScores = await db.prepare(
    'SELECT date, score, label, status, delta_1d, delta_7d, delta_30d FROM pxi_scores ORDER BY date DESC LIMIT 10'
  ).all<PXIRow>();

  let selected: PXIRow | null = null;
  let selectedCategories: CategoryRow[] = [];

  for (const candidate of recentScores.results || []) {
    const cats = await db.prepare(
      'SELECT category, score, weight FROM category_scores WHERE date = ?'
    ).bind(candidate.date).all<CategoryRow>();
    if ((cats.results?.length || 0) >= 3) {
      selected = candidate;
      selectedCategories = cats.results || [];
      break;
    }
  }

  if (!selected) {
    selected = recentScores.results?.[0] || null;
    if (selected) {
      const cats = await db.prepare(
        'SELECT category, score, weight FROM category_scores WHERE date = ?'
      ).bind(selected.date).all<CategoryRow>();
      selectedCategories = cats.results || [];
    }
  }

  return {
    pxi: selected,
    categories: selectedCategories,
  };
}

async function buildCanonicalMarketDecision(
  db: D1Database,
  options?: {
    pxi?: PXIRow;
    categories?: CategoryRow[];
  }
): Promise<{
  as_of: string;
  pxi: PXIRow;
  categories: CategoryRow[];
  signal: PXISignal;
  risk_sizing: RiskSizingSnapshot;
  regime: RegimeResult | null;
  freshness: FreshnessStatus;
  risk_band: { d7: PlanRiskBand; d30: PlanRiskBand };
  edge_quality: EdgeQualitySnapshot;
  policy_state: PolicyStateSnapshot;
  degraded_reasons: string[];
  uncertainty: UncertaintySnapshot;
  consistency: ConsistencySnapshot;
  trader_playbook: TraderPlaybookSnapshot;
}> {
  let pxi = options?.pxi || null;
  let categories = options?.categories || [];

  if (!pxi) {
    const selected = await selectLatestPxiWithCategories(db);
    pxi = selected.pxi;
    categories = selected.categories;
  }

  if (!pxi) {
    throw new Error('no_pxi_data');
  }

  const categoryScores = categories.map((row) => ({ score: row.score }));
  const [regime, freshness, mlSampleSize, riskBand, edgeCalibrationSnapshot] = await Promise.all([
    detectRegime(db, pxi.date),
    computeFreshnessStatus(db),
    fetchPredictionEvaluationSampleSize(db),
    buildCurrentBucketRiskBands(db, pxi.score),
    fetchLatestCalibrationSnapshot(db, 'edge_quality', null),
  ]);

  const signal = await calculatePXISignal(
    db,
    { score: pxi.score, delta_7d: pxi.delta_7d, delta_30d: pxi.delta_30d },
    regime,
    categoryScores,
  );

  const divergence = await detectDivergence(db, pxi.score, regime);
  const conflictState = resolveConflictState(regime, signal);
  const staleCountRaw = Math.max(0, Math.floor(toNumber(freshness.stale_count, 0)));
  const stalePenaltyUnits = freshnessPenaltyCount(freshness);
  const edgeQuality = computeEdgeQualitySnapshot({
    staleCount: staleCountRaw,
    mlSampleSize,
    regime,
    conflictState,
    divergenceCount: divergence.alerts.length,
  });
  const edgeQualityWithCalibration: EdgeQualitySnapshot = {
    ...edgeQuality,
    calibration: buildEdgeQualityCalibrationFromSnapshot(edgeCalibrationSnapshot, edgeQuality.score),
  };

  const policyState = buildPolicyStateSnapshot({
    signal,
    regime,
    edgeQuality: edgeQualityWithCalibration,
    freshness,
    calibrationQuality: edgeQualityWithCalibration.calibration.quality,
  });

  const degradedReasons: string[] = [];
  if (riskBand.d7.sample_size < 20 || riskBand.d30.sample_size < 20) degradedReasons.push('limited_scenario_sample');
  if (stalePenaltyUnits > 0) degradedReasons.push('stale_inputs');
  if (edgeQualityWithCalibration.label === 'LOW') degradedReasons.push('low_edge_quality');
  if (
    edgeQualityWithCalibration.calibration.quality === 'INSUFFICIENT' ||
    (edgeQualityWithCalibration.calibration.quality === 'LIMITED' && stalePenaltyUnits > 0)
  ) {
    degradedReasons.push('limited_calibration_sample');
  }

  const uncertainty = buildUncertaintySnapshot(degradedReasons);
  const riskSizing = computeRiskSizingSnapshot({
    signal,
    policyState,
    edgeQuality: edgeQualityWithCalibration,
    freshness,
  });
  const traderPlaybook = await buildTraderPlaybookSnapshot(db, {
    signal,
    policyState,
    edgeQuality: edgeQualityWithCalibration,
    freshness,
    sizing: riskSizing,
  });
  const limitedScenarioSample = riskBand.d7.sample_size < 20 || riskBand.d30.sample_size < 20;
  const allocationTargetMismatch = Math.abs(traderPlaybook.recommended_size_pct.target - riskSizing.target_pct) > 0.5;
  const consistency = buildConsistencySnapshot(policyState, {
    stale_count: stalePenaltyUnits,
    calibration_quality: edgeQualityWithCalibration.calibration.quality,
    limited_scenario_sample: limitedScenarioSample,
    conflict_state: policyState.conflict_state,
    allocation_target_mismatch: allocationTargetMismatch,
  });

  return {
    as_of: `${pxi.date}T00:00:00.000Z`,
    pxi,
    categories,
    signal,
    risk_sizing: riskSizing,
    regime,
    freshness,
    risk_band: riskBand,
    edge_quality: edgeQualityWithCalibration,
    policy_state: policyState,
    degraded_reasons: degradedReasons,
    uncertainty,
    consistency,
    trader_playbook: traderPlaybook,
  };
}

function isBriefSnapshotCompatible(snapshot: BriefSnapshot | null): boolean {
  if (!snapshot) return false;
  if (snapshot.contract_version !== BRIEF_CONTRACT_VERSION) return false;
  if (!snapshot.policy_state || !snapshot.source_plan_as_of || !snapshot.consistency) return false;
  if (!snapshot.policy_state.stance || !snapshot.policy_state.risk_posture) return false;
  if (typeof snapshot.consistency.score !== 'number' || typeof snapshot.consistency.state !== 'string') return false;
  if (!snapshot.consistency.components || typeof snapshot.consistency.components !== 'object') return false;
  return true;
}

function computeNextExpectedRefresh(now = new Date()): { at: string; in_minutes: number } {
  const nowMs = now.getTime();
  const candidateDates: Date[] = [];

  for (let dayOffset = 0; dayOffset <= 2; dayOffset += 1) {
    for (const hour of REFRESH_HOURS_UTC) {
      const d = new Date(Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate() + dayOffset,
        hour,
        0,
        0,
        0,
      ));
      if (d.getTime() >= nowMs) {
        candidateDates.push(d);
      }
    }
  }

  const next = candidateDates.length > 0
    ? candidateDates.sort((a, b) => a.getTime() - b.getTime())[0]
    : new Date(nowMs + (6 * 60 * 60 * 1000));

  return {
    at: next.toISOString(),
    in_minutes: Math.max(0, Math.round((next.getTime() - nowMs) / 60000)),
  };
}

function parseIsoTimestamp(value: string | null | undefined): Date | null {
  if (!value) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function computeOpportunityTtlMetadata(
  lastRefreshAtUtc: string | null,
  now = new Date(),
): OpportunityTtlMetadata {
  const lastRefreshDate = parseIsoTimestamp(lastRefreshAtUtc);
  if (!lastRefreshDate) {
    return {
      data_age_seconds: null,
      ttl_state: 'unknown',
      next_expected_refresh_at: null,
      overdue_seconds: null,
    };
  }

  const ageSeconds = Math.max(0, Math.floor((now.getTime() - lastRefreshDate.getTime()) / 1000));
  const nextExpected = computeNextExpectedRefresh(lastRefreshDate);
  const nextExpectedDate = parseIsoTimestamp(nextExpected.at);
  if (!nextExpectedDate) {
    return {
      data_age_seconds: ageSeconds,
      ttl_state: 'unknown',
      next_expected_refresh_at: null,
      overdue_seconds: null,
    };
  }

  const overdueSecondsRaw = Math.floor((now.getTime() - nextExpectedDate.getTime()) / 1000);
  const overdueSeconds = Math.max(0, overdueSecondsRaw);
  let ttlState: OpportunityTtlState = 'fresh';
  if (overdueSecondsRaw > 0 && overdueSeconds <= OPPORTUNITY_REFRESH_TTL_GRACE_SECONDS) {
    ttlState = 'stale';
  } else if (overdueSeconds > OPPORTUNITY_REFRESH_TTL_GRACE_SECONDS) {
    ttlState = 'overdue';
  }

  return {
    data_age_seconds: ageSeconds,
    ttl_state: ttlState,
    next_expected_refresh_at: nextExpectedDate.toISOString(),
    overdue_seconds: overdueSeconds,
  };
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

function normalizeUtilityEventType(value: unknown): UtilityEventType | null {
  if (typeof value !== 'string') return null;
  const candidate = value.trim() as UtilityEventType;
  if (!UTILITY_EVENT_TYPES.includes(candidate)) {
    return null;
  }
  return candidate;
}

function normalizeUtilitySessionId(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const sessionId = value.trim();
  if (!UTILITY_SESSION_ID_RE.test(sessionId)) {
    return null;
  }
  return sessionId;
}

function normalizeUtilityRoute(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const route = value.trim();
  if (!route || route.length > 64) return null;
  if (!UTILITY_ROUTE_ALLOWLIST.has(route)) return null;
  return route;
}

function normalizeUtilityActionabilityState(value: unknown): PlanActionabilityState | null {
  if (value === 'ACTIONABLE' || value === 'WATCH' || value === 'NO_ACTION') {
    return value;
  }
  return null;
}

function sanitizeUtilityPayload(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  try {
    const serialized = JSON.stringify(value);
    if (serialized.length > UTILITY_PAYLOAD_MAX_CHARS) {
      return serialized.slice(0, UTILITY_PAYLOAD_MAX_CHARS);
    }
    return serialized;
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

interface FreshnessIndicatorStatus {
  indicator_id: string;
  latest_date: string | null;
  days_old: number | null;
  max_age_days: number;
  critical: boolean;
  status: 'stale' | 'missing';
}

interface FreshnessDiagnostics {
  status: FreshnessStatus;
  stale_indicators: FreshnessIndicatorStatus[];
}

async function computeFreshnessDiagnostics(db: D1Database): Promise<FreshnessDiagnostics> {
  const latestResult = await db.prepare(`
    SELECT indicator_id, MAX(date) as last_date
    FROM indicator_values
    GROUP BY indicator_id
  `).all<{ indicator_id: string; last_date: string | null }>();

  const latestByIndicator = new Map<string, string | null>();
  for (const row of latestResult.results || []) {
    latestByIndicator.set(row.indicator_id, row.last_date ?? null);
  }

  const now = new Date();
  const staleIndicators: FreshnessIndicatorStatus[] = [];
  for (const indicatorId of [...FRESHNESS_MONITORED_INDICATORS].sort()) {
    const frequency = INDICATOR_FREQUENCY_HINTS.get(indicatorId) ?? null;
    const policy = resolveIndicatorSla(indicatorId, frequency);
    const evaluation = evaluateSla(latestByIndicator.get(indicatorId) ?? null, now, policy);
    if (!evaluation.stale && !evaluation.missing) {
      continue;
    }
    staleIndicators.push({
      indicator_id: indicatorId,
      latest_date: evaluation.latest_date,
      days_old: evaluation.days_old,
      max_age_days: evaluation.max_age_days,
      critical: evaluation.critical,
      status: evaluation.missing ? 'missing' : 'stale',
    });
  }

  staleIndicators.sort((a, b) => {
    if (a.critical !== b.critical) return a.critical ? -1 : 1;
    if (a.status !== b.status) return a.status === 'missing' ? -1 : 1;
    const aDays = a.days_old ?? Number.POSITIVE_INFINITY;
    const bDays = b.days_old ?? Number.POSITIVE_INFINITY;
    if (aDays !== bDays) return bDays - aDays;
    return a.indicator_id.localeCompare(b.indicator_id);
  });

  const staleCount = staleIndicators.length;
  const criticalStaleCount = staleIndicators.filter((indicator) => indicator.critical).length;
  return {
    status: {
      has_stale_data: staleCount > 0,
      stale_count: staleCount,
      critical_stale_count: criticalStaleCount,
    },
    stale_indicators: staleIndicators,
  };
}

async function computeFreshnessStatus(db: D1Database): Promise<FreshnessStatus> {
  const diagnostics = await computeFreshnessDiagnostics(db);
  return diagnostics.status;
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

  const stalePenaltyUnits = staleCount <= 1 ? 0 : staleCount - 1;
  const dataQuality = Math.round(clamp(0, 100, 100 - stalePenaltyUnits * 4));

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
    calibration: buildEdgeCalibrationFallback(buildDecileBinLabel(score)),
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
  freshness: FreshnessStatus;
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

  const stalePenalty = freshnessPenaltyCount(freshness);
  if (stalePenalty > 0) {
    const capThreshold = Math.max(2, stalePenalty);
    rules.push(`If stale-input pressure remains above ${capThreshold}, keep risk allocation capped until freshness improves.`);
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
  const policyState: PolicyStateSnapshot = {
    stance: 'MIXED',
    risk_posture: 'neutral',
    conflict_state: 'MIXED',
    base_signal: 'REDUCED_RISK',
    regime_context: 'TRANSITION',
    rationale: `fallback:${reason}`,
    rationale_codes: ['fallback'],
  };
  const consistency = buildConsistencySnapshot(policyState);
  return {
    as_of: now,
    summary: 'Daily market brief is temporarily unavailable. Showing neutral fallback context.',
    regime_delta: 'UNCHANGED',
    top_changes: [`degraded: ${reason}`],
    risk_posture: 'neutral',
    policy_state: policyState,
    source_plan_as_of: now,
    contract_version: BRIEF_CONTRACT_VERSION,
    consistency,
    explainability: {
      category_movers: [],
      indicator_movers: [],
    },
    freshness_status: {
      has_stale_data: false,
      stale_count: 0,
      critical_stale_count: 0,
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
  const policyState: PolicyStateSnapshot = {
    stance: 'MIXED',
    risk_posture: 'neutral',
    conflict_state: 'MIXED',
    base_signal: 'REDUCED_RISK',
    regime_context: 'TRANSITION',
    rationale: `fallback:${reason}`,
    rationale_codes: ['fallback'],
  };
  const consistency = buildConsistencySnapshot(policyState);
  return {
    as_of: asIsoDateTime(new Date()),
    setup_summary: 'Plan service is in degraded mode. Use neutral sizing until full context is restored.',
    policy_state: policyState,
    actionability_state: 'NO_ACTION',
    actionability_reason_codes: ['fallback_degraded_mode', `opportunity_${reason}`],
    action_now: {
      risk_allocation_target: 0.5,
      raw_signal_allocation_target: 0.5,
      risk_allocation_basis: 'fallback_neutral',
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
      calibration: buildEdgeCalibrationFallback('50-59'),
    },
    risk_band: {
      d7: { bear: null, base: null, bull: null, sample_size: 0 },
      d30: { bear: null, base: null, bull: null, sample_size: 0 },
    },
    uncertainty: {
      headline: 'Signal quality reduced: fallback mode.',
      flags: {
        stale_inputs: false,
        limited_calibration: true,
        limited_scenario_sample: true,
      },
    },
    consistency,
    trader_playbook: {
      recommended_size_pct: { min: 25, target: 50, max: 65 },
      scenarios: [
        {
          condition: 'Fallback mode active.',
          action: 'Hold neutral risk and avoid directional concentration.',
          invalidation: 'Replace with live plan once service health recovers.',
        },
      ],
      benchmark_follow_through_7d: {
        hit_rate: null,
        sample_size: 0,
        unavailable_reason: 'insufficient_sample',
      },
    },
    invalidation_rules: [
      'Hold neutral risk until plan data is fully available.',
    ],
    degraded_reason: reason,
  };
}

const MARKET_SCHEMA_CACHE_MS = 5 * 60 * 1000;
let marketSchemaInitPromise: Promise<void> | null = null;
let marketSchemaInitializedAt = 0;

async function tableHasColumn(db: D1Database, tableName: string, columnName: string): Promise<boolean> {
  const rows = await db.prepare(`PRAGMA table_info(${tableName})`).all<{
    name: string;
  }>();
  return (rows.results || []).some((row) => row.name === columnName);
}

async function ensureMarketProductSchema(db: D1Database): Promise<void> {
  const now = Date.now();
  if (marketSchemaInitializedAt > 0 && (now - marketSchemaInitializedAt) < MARKET_SCHEMA_CACHE_MS) {
    return;
  }

  if (marketSchemaInitPromise) {
    await marketSchemaInitPromise;
    return;
  }

  marketSchemaInitPromise = (async () => {
    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_brief_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT NOT NULL UNIQUE,
        contract_version TEXT NOT NULL DEFAULT '${BRIEF_CONTRACT_VERSION}',
        payload_json TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    const hasContractVersion = await tableHasColumn(db, 'market_brief_snapshots', 'contract_version');
    if (!hasContractVersion) {
      await db.prepare(
        `ALTER TABLE market_brief_snapshots ADD COLUMN contract_version TEXT NOT NULL DEFAULT '${BRIEF_CONTRACT_VERSION}'`
      ).run();
    }
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_brief_as_of ON market_brief_snapshots(as_of DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS opportunity_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT NOT NULL,
        horizon TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(as_of, horizon)
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_opportunity_snapshots_lookup ON opportunity_snapshots(as_of DESC, horizon)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_opportunity_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        refresh_run_id INTEGER,
        as_of TEXT NOT NULL,
        horizon TEXT NOT NULL CHECK(horizon IN ('7d', '30d')),
        candidate_count INTEGER NOT NULL DEFAULT 0,
        published_count INTEGER NOT NULL DEFAULT 0,
        suppressed_count INTEGER NOT NULL DEFAULT 0,
        quality_filtered_count INTEGER NOT NULL DEFAULT 0,
        coherence_suppressed_count INTEGER NOT NULL DEFAULT 0,
        data_quality_suppressed_count INTEGER NOT NULL DEFAULT 0,
        degraded_reason TEXT,
        top_direction_candidate TEXT CHECK(top_direction_candidate IN ('bullish', 'bearish', 'neutral')),
        top_direction_published TEXT CHECK(top_direction_published IN ('bullish', 'bearish', 'neutral')),
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_created ON market_opportunity_ledger(created_at DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_as_of ON market_opportunity_ledger(as_of DESC, horizon)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_run ON market_opportunity_ledger(refresh_run_id, horizon)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_opportunity_item_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        refresh_run_id INTEGER,
        as_of TEXT NOT NULL,
        horizon TEXT NOT NULL CHECK(horizon IN ('7d', '30d')),
        opportunity_id TEXT NOT NULL,
        theme_id TEXT NOT NULL,
        theme_name TEXT NOT NULL,
        direction TEXT NOT NULL CHECK(direction IN ('bullish', 'bearish', 'neutral')),
        conviction_score INTEGER NOT NULL,
        published INTEGER NOT NULL,
        suppression_reason TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(as_of, horizon, opportunity_id)
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_asof_horizon ON market_opportunity_item_ledger(as_of DESC, horizon)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_theme_horizon_asof ON market_opportunity_item_ledger(theme_id, horizon, as_of DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_published_created ON market_opportunity_item_ledger(published, created_at DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_decision_impact_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT NOT NULL,
        horizon TEXT NOT NULL CHECK(horizon IN ('7d', '30d')),
        scope TEXT NOT NULL CHECK(scope IN ('market', 'theme')),
        window_days INTEGER NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(as_of, horizon, scope, window_days)
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_decision_impact_lookup ON market_decision_impact_snapshots(scope, horizon, window_days, as_of DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_calibration_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT NOT NULL,
        metric TEXT NOT NULL,
        horizon TEXT,
        payload_json TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(as_of, metric, horizon)
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_calibration_lookup ON market_calibration_snapshots(metric, horizon, as_of DESC)`).run();

    await db.prepare(`
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
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_events_created ON market_alert_events(created_at DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_events_type ON market_alert_events(event_type, created_at DESC)`).run();

    await db.prepare(`
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
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_deliveries_event ON market_alert_deliveries(event_id, attempted_at DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_deliveries_subscriber ON market_alert_deliveries(subscriber_id, attempted_at DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_consistency_checks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT NOT NULL UNIQUE,
        score REAL NOT NULL,
        state TEXT NOT NULL,
        violations_json TEXT NOT NULL,
        components_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    const hasComponentsJson = await tableHasColumn(db, 'market_consistency_checks', 'components_json');
    if (!hasComponentsJson) {
      await db.prepare(
        `ALTER TABLE market_consistency_checks ADD COLUMN components_json TEXT NOT NULL DEFAULT '{}'`
      ).run();
    }
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_consistency_created ON market_consistency_checks(created_at DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_refresh_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at TEXT NOT NULL,
        completed_at TEXT,
        status TEXT NOT NULL CHECK(status IN ('running', 'success', 'failed')),
        "trigger" TEXT NOT NULL DEFAULT 'unknown',
        brief_generated INTEGER DEFAULT 0,
        opportunities_generated INTEGER DEFAULT 0,
        calibrations_generated INTEGER DEFAULT 0,
        alerts_generated INTEGER DEFAULT 0,
        stale_count INTEGER,
        critical_stale_count INTEGER,
        as_of TEXT,
        error TEXT,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    const hasCriticalStaleCount = await tableHasColumn(db, 'market_refresh_runs', 'critical_stale_count');
    if (!hasCriticalStaleCount) {
      await db.prepare(
        `ALTER TABLE market_refresh_runs ADD COLUMN critical_stale_count INTEGER`
      ).run();
    }
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_refresh_runs_completed ON market_refresh_runs(status, completed_at DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_refresh_runs_created ON market_refresh_runs(created_at DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_utility_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        route TEXT,
        actionability_state TEXT,
        payload_json TEXT,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_utility_events_created ON market_utility_events(created_at DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_utility_events_type ON market_utility_events(event_type, created_at DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_utility_events_session ON market_utility_events(session_id, created_at DESC)`).run();

    await db.prepare(`
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
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_email_subscribers_status ON email_subscribers(status)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_email_subscribers_updated ON email_subscribers(updated_at DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS email_verification_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL,
        token_hash TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        used_at TEXT,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_email_verification_email_expires ON email_verification_tokens(email, expires_at DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_email_verification_hash ON email_verification_tokens(token_hash)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS email_unsubscribe_tokens (
        subscriber_id TEXT PRIMARY KEY,
        token_hash TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_email_unsubscribe_hash ON email_unsubscribe_tokens(token_hash)`).run();

    marketSchemaInitializedAt = Date.now();
  })();

  try {
    await marketSchemaInitPromise;
  } finally {
    marketSchemaInitPromise = null;
  }
}

function sanitizeSignalsTickers(values: unknown[]): string[] {
  const sanitized: string[] = [];
  const seen = new Set<string>();

  for (const raw of values) {
    const token = String(raw || '').trim().toUpperCase();
    if (!token || seen.has(token)) continue;
    if (!SIGNALS_TICKER_REGEX.test(token)) continue;
    if (SIGNALS_TICKER_STOPWORDS.has(token)) continue;
    seen.add(token);
    sanitized.push(token);
    if (sanitized.length >= SIGNALS_TICKER_LIMIT) break;
  }

  return sanitized;
}

async function fetchLatestSignalsThemes(
  options?: { sanitize_tickers?: boolean }
): Promise<SignalsThemeRecord[]> {
  const sanitizeTickers = options?.sanitize_tickers !== false;
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
        ? (sanitizeTickers
            ? sanitizeSignalsTickers(theme.key_tickers)
            : theme.key_tickers.map((ticker) => String(ticker).trim().toUpperCase()))
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
    SELECT date, score, label, status, delta_7d, delta_30d
    FROM pxi_scores
    ORDER BY date DESC
    LIMIT 2
  `).all<{ date: string; score: number; label: string; status: string; delta_7d: number | null; delta_30d: number | null }>();

  const latest = latestAndPrevious.results?.[0];
  if (!latest) return null;
  const previous = latestAndPrevious.results?.[1] || null;

  const [currentRegime, previousRegime] = await Promise.all([
    detectRegime(db, latest.date),
    previous ? detectRegime(db, previous.date) : Promise.resolve(null),
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

  const canonical = await buildCanonicalMarketDecision(db, {
    pxi: {
      date: latest.date,
      score: latest.score,
      label: latest.label,
      status: latest.status,
      delta_1d: null,
      delta_7d: latest.delta_7d,
      delta_30d: latest.delta_30d,
    },
    categories: (categoryRows.results || []).map((row) => ({
      category: row.category,
      score: row.current_score,
      weight: 0,
    })),
  });

  const freshness = canonical.freshness;

  let indicatorMovers = (indicatorRows.results || [])
    .map((row) => ({
      indicator_id: row.indicator_id,
      value_change: row.current_value - (row.previous_value ?? row.current_value),
      z_impact: row.current_norm - (row.previous_norm ?? row.current_norm),
    }))
    .filter((row) => Number.isFinite(row.value_change) && Number.isFinite(row.z_impact))
    .filter((row) => Math.abs(row.value_change) > 1e-12 || Math.abs(row.z_impact) > 1e-12)
    .sort((a, b) => Math.abs(b.z_impact) - Math.abs(a.z_impact))
    .slice(0, 5);

  if (indicatorMovers.length === 0) {
    const indicatorValueRows = await db.prepare(`
      WITH latest_dates AS (
        SELECT date
        FROM indicator_values
        GROUP BY date
        ORDER BY date DESC
        LIMIT 2
      )
      SELECT cur.indicator_id as indicator_id,
             cur.value as current_value,
             prev.value as previous_value
      FROM indicator_values cur
      LEFT JOIN indicator_values prev
        ON prev.indicator_id = cur.indicator_id
        AND prev.date = (SELECT MIN(date) FROM latest_dates)
      WHERE cur.date = (SELECT MAX(date) FROM latest_dates)
    `).all<{
      indicator_id: string;
      current_value: number;
      previous_value: number | null;
    }>();

    indicatorMovers = (indicatorValueRows.results || [])
      .map((row) => {
        const prev = row.previous_value ?? row.current_value;
        const valueChange = row.current_value - prev;
        const denominator = Math.max(1e-6, Math.abs(prev));
        const pctImpact = valueChange / denominator;
        return {
          indicator_id: row.indicator_id,
          value_change: valueChange,
          z_impact: clamp(-10, 10, pctImpact),
        };
      })
      .filter((row) => Number.isFinite(row.value_change) && Number.isFinite(row.z_impact))
      .filter((row) => Math.abs(row.value_change) > 1e-12 || Math.abs(row.z_impact) > 1e-12)
      .sort((a, b) => Math.abs(b.z_impact) - Math.abs(a.z_impact))
      .slice(0, 5);
  }

  const scoreDelta = previous ? latest.score - previous.score : null;
  const regimeDelta = resolveRegimeDelta(currentRegime, previousRegime, scoreDelta);
  const riskPosture = canonical.policy_state.risk_posture;
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

  const summary = `PXI ${latest.score.toFixed(1)} (${latest.label}), ${deltaText} vs prior reading. Regime ${regimeLabel(currentRegime)}; posture ${riskPosture.replace('_', '-')} (${canonical.policy_state.stance.replace('_', ' ')}).${
    freshness.has_stale_data
      ? ` ${freshness.stale_count} indicator(s) stale (${freshness.critical_stale_count} critical).`
      : ''
  }`;

  return {
    as_of: canonical.as_of,
    summary,
    regime_delta: regimeDelta,
    top_changes: topChanges.slice(0, 5),
    risk_posture: riskPosture,
    policy_state: canonical.policy_state,
    source_plan_as_of: canonical.as_of,
    contract_version: BRIEF_CONTRACT_VERSION,
    consistency: canonical.consistency,
    explainability: {
      category_movers: categoryMovers,
      indicator_movers: indicatorMovers,
    },
    freshness_status: freshness,
    updated_at: asIsoDateTime(new Date()),
    degraded_reason: canonical.degraded_reasons.length > 0 ? canonical.degraded_reasons.join(',') : null,
  };
}

function addCalendarDays(isoDate: string, days: number): string {
  const d = new Date(`${isoDate}T00:00:00.000Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return asIsoDate(d);
}

function allDecileLabels(): string[] {
  const labels: string[] = [];
  for (let binStart = 0; binStart <= 90; binStart += 10) {
    const binEnd = binStart === 90 ? 100 : binStart + 9;
    labels.push(`${binStart}-${binEnd}`);
  }
  return labels;
}

function baselineRiskAllocationForSignal(
  signalType: 'FULL_RISK' | 'REDUCED_RISK' | 'RISK_OFF' | 'DEFENSIVE'
): number {
  if (signalType === 'FULL_RISK') return 0.9;
  if (signalType === 'REDUCED_RISK') return 0.65;
  if (signalType === 'RISK_OFF') return 0.35;
  return 0.15;
}

async function resolveCalibrationAsOf(db: D1Database): Promise<string> {
  const latest = await db.prepare(`
    SELECT date
    FROM pxi_scores
    ORDER BY date DESC
    LIMIT 1
  `).first<{ date: string }>();
  if (!latest?.date) {
    return asIsoDateTime(new Date());
  }
  return `${latest.date}T00:00:00.000Z`;
}

async function fetchLatestCalibrationSnapshot(
  db: D1Database,
  metric: 'edge_quality' | 'conviction',
  horizon: '7d' | '30d' | null
): Promise<MarketCalibrationSnapshotPayload | null> {
  try {
    let row: { payload_json: string } | null = null;
    if (horizon === null) {
      row = await db.prepare(`
        SELECT payload_json
        FROM market_calibration_snapshots
        WHERE metric = ?
          AND horizon IS NULL
        ORDER BY as_of DESC
        LIMIT 1
      `).bind(metric).first<{ payload_json: string }>();
    } else {
      row = await db.prepare(`
        SELECT payload_json
        FROM market_calibration_snapshots
        WHERE metric = ?
          AND horizon = ?
        ORDER BY as_of DESC
        LIMIT 1
      `).bind(metric, horizon).first<{ payload_json: string }>();
    }
    return parseCalibrationSnapshotPayload(row?.payload_json) || null;
  } catch (err) {
    console.warn('Calibration snapshot lookup failed:', err);
    return null;
  }
}

async function fetchCalibrationSnapshotAtOrBefore(
  db: D1Database,
  metric: 'edge_quality' | 'conviction',
  horizon: '7d' | '30d' | null,
  asOf?: string | null
): Promise<MarketCalibrationSnapshotPayload | null> {
  try {
    const asOfFilter = asOf && parseIsoDate(asOf) ? `${asOf.slice(0, 10)}T23:59:59.999Z` : null;
    let row: { payload_json: string } | null = null;

    if (horizon === null) {
      if (asOfFilter) {
        row = await db.prepare(`
          SELECT payload_json
          FROM market_calibration_snapshots
          WHERE metric = ?
            AND horizon IS NULL
            AND as_of <= ?
          ORDER BY as_of DESC
          LIMIT 1
        `).bind(metric, asOfFilter).first<{ payload_json: string }>();
      } else {
        row = await db.prepare(`
          SELECT payload_json
          FROM market_calibration_snapshots
          WHERE metric = ?
            AND horizon IS NULL
          ORDER BY as_of DESC
          LIMIT 1
        `).bind(metric).first<{ payload_json: string }>();
      }
    } else if (asOfFilter) {
      row = await db.prepare(`
        SELECT payload_json
        FROM market_calibration_snapshots
        WHERE metric = ?
          AND horizon = ?
          AND as_of <= ?
        ORDER BY as_of DESC
        LIMIT 1
      `).bind(metric, horizon, asOfFilter).first<{ payload_json: string }>();
    } else {
      row = await db.prepare(`
        SELECT payload_json
        FROM market_calibration_snapshots
        WHERE metric = ?
          AND horizon = ?
        ORDER BY as_of DESC
        LIMIT 1
      `).bind(metric, horizon).first<{ payload_json: string }>();
    }

    return parseCalibrationSnapshotPayload(row?.payload_json) || null;
  } catch (err) {
    console.warn('Calibration snapshot lookup failed:', err);
    return null;
  }
}

async function storeCalibrationSnapshot(
  db: D1Database,
  snapshot: MarketCalibrationSnapshotPayload
): Promise<void> {
  if (snapshot.horizon === null) {
    await db.prepare(`
      DELETE FROM market_calibration_snapshots
      WHERE as_of = ?
        AND metric = ?
        AND horizon IS NULL
    `).bind(snapshot.as_of, snapshot.metric).run();
  }
  await db.prepare(`
    INSERT OR REPLACE INTO market_calibration_snapshots
      (as_of, metric, horizon, payload_json, created_at)
    VALUES (?, ?, ?, ?, datetime('now'))
  `).bind(
    snapshot.as_of,
    snapshot.metric,
    snapshot.horizon,
    JSON.stringify(snapshot),
  ).run();
}

async function buildEdgeQualityCalibrationSnapshot(db: D1Database): Promise<MarketCalibrationSnapshotPayload> {
  const asOf = await resolveCalibrationAsOf(db);
  const rows = await db.prepare(`
    SELECT
      pl.prediction_date,
      pl.predicted_change_7d,
      pl.actual_change_7d,
      ps.regime,
      ps.signal_type,
      ps.risk_allocation
    FROM prediction_log pl
    LEFT JOIN pxi_signal ps
      ON ps.date = pl.prediction_date
    WHERE pl.predicted_change_7d IS NOT NULL
      AND pl.actual_change_7d IS NOT NULL
    ORDER BY pl.prediction_date ASC
    LIMIT 5000
  `).all<{
    prediction_date: string;
    predicted_change_7d: number;
    actual_change_7d: number;
    regime: string | null;
    signal_type: string | null;
    risk_allocation: number | null;
  }>();

  const byBin = new Map<string, { correct: number; total: number }>();
  let mlSampleSize = 0;

  for (const row of rows.results || []) {
    const predicted = toNumber(row.predicted_change_7d, 0);
    const actual = toNumber(row.actual_change_7d, 0);
    if (!Number.isFinite(predicted) || !Number.isFinite(actual) || predicted === 0) {
      continue;
    }

    mlSampleSize += 1;
    const regimeType = row.regime === 'RISK_ON' || row.regime === 'RISK_OFF' || row.regime === 'TRANSITION'
      ? row.regime
      : null;
    const regime: RegimeResult | null = regimeType
      ? {
          regime: regimeType,
          confidence: 0.5,
          signals: [],
          description: 'historical_reconstruction',
          date: row.prediction_date,
        }
      : null;

    const signalType = signalTypeFromValue(row.signal_type, predicted);
    const riskAllocationRaw = row.risk_allocation !== null ? toNumber(row.risk_allocation, baselineRiskAllocationForSignal(signalType)) : baselineRiskAllocationForSignal(signalType);
    const syntheticSignal: PXISignal = {
      pxi_level: 50,
      delta_pxi_7d: null,
      delta_pxi_30d: null,
      category_dispersion: 0,
      regime: regime?.regime || 'TRANSITION',
      volatility_percentile: null,
      risk_allocation: clamp(0, 1, riskAllocationRaw),
      signal_type: signalType,
      adjustments: [],
    };
    const conflictState = resolveConflictState(regime, syntheticSignal);
    const edgeQuality = computeEdgeQualitySnapshot({
      staleCount: 0,
      mlSampleSize,
      regime,
      conflictState,
      divergenceCount: 0,
    });

    const bin = buildDecileBinLabel(edgeQuality.score);
    const entry = byBin.get(bin) || { correct: 0, total: 0 };
    entry.total += 1;
    if ((predicted > 0 && actual > 0) || (predicted < 0 && actual < 0)) {
      entry.correct += 1;
    }
    byBin.set(bin, entry);
  }

  const bins: CalibrationBinSnapshot[] = allDecileLabels().map((bin) => {
    const entry = byBin.get(bin) || { correct: 0, total: 0 };
    const interval = computeWilson95(entry.correct, entry.total);
    return {
      bin,
      correct_count: entry.correct,
      probability_correct: entry.total > 0 ? interval.probability : null,
      ci95_low: entry.total > 0 ? interval.ci95_low : null,
      ci95_high: entry.total > 0 ? interval.ci95_high : null,
      sample_size: entry.total,
      quality: calibrationQualityForSampleSize(entry.total),
    };
  });

  return {
    as_of: asOf,
    metric: 'edge_quality',
    horizon: null,
    basis: 'edge_quality_decile',
    bins,
    total_samples: bins.reduce((sum, bin) => sum + bin.sample_size, 0),
  };
}

async function buildConvictionCalibrationSnapshot(
  db: D1Database,
  horizon: '7d' | '30d'
): Promise<MarketCalibrationSnapshotPayload> {
  const asOf = await resolveCalibrationAsOf(db);
  const horizonDays = horizon === '7d' ? 7 : 30;
  const [snapshots, spyRows] = await Promise.all([
    db.prepare(`
      SELECT as_of, payload_json
      FROM opportunity_snapshots
      WHERE horizon = ?
      ORDER BY as_of DESC
      LIMIT 730
    `).bind(horizon).all<{ as_of: string; payload_json: string }>(),
    db.prepare(`
      SELECT date, value
      FROM indicator_values
      WHERE indicator_id = 'spy_close'
      ORDER BY date ASC
    `).all<{ date: string; value: number }>(),
  ]);

  const spyMap = new Map<string, number>();
  for (const row of spyRows.results || []) {
    spyMap.set(row.date, row.value);
  }

  const priceOnOrAfter = (date: string, toleranceDays = 3): number | null => {
    for (let offset = 0; offset <= toleranceDays; offset += 1) {
      const candidateDate = addCalendarDays(date, offset);
      const price = spyMap.get(candidateDate);
      if (price !== undefined) {
        return price;
      }
    }
    return null;
  };

  const byBin = new Map<string, { correct: number; total: number }>();
  for (const row of snapshots.results || []) {
    let payload: OpportunitySnapshot | null = null;
    try {
      payload = JSON.parse(row.payload_json) as OpportunitySnapshot;
    } catch {
      payload = null;
    }
    if (!payload?.items || payload.items.length === 0) continue;

    const asOfDate = (payload.as_of || row.as_of).slice(0, 10);
    const spot = priceOnOrAfter(asOfDate, 3);
    const forward = priceOnOrAfter(addCalendarDays(asOfDate, horizonDays), 3);
    if (spot === null || forward === null || spot === 0) continue;
    const forwardReturn = (forward - spot) / spot;

    for (const item of payload.items) {
      if (item.direction !== 'bullish' && item.direction !== 'bearish') {
        continue;
      }
      const conviction = clamp(0, 100, toNumber(item.conviction_score, 50));
      const bin = buildDecileBinLabel(conviction);
      const entry = byBin.get(bin) || { correct: 0, total: 0 };
      entry.total += 1;
      if ((item.direction === 'bullish' && forwardReturn > 0) || (item.direction === 'bearish' && forwardReturn < 0)) {
        entry.correct += 1;
      }
      byBin.set(bin, entry);
    }
  }

  const bins: CalibrationBinSnapshot[] = allDecileLabels().map((bin) => {
    const entry = byBin.get(bin) || { correct: 0, total: 0 };
    const interval = computeWilson95(entry.correct, entry.total);
    return {
      bin,
      correct_count: entry.correct,
      probability_correct: entry.total > 0 ? interval.probability : null,
      ci95_low: entry.total > 0 ? interval.ci95_low : null,
      ci95_high: entry.total > 0 ? interval.ci95_high : null,
      sample_size: entry.total,
      quality: calibrationQualityForSampleSize(entry.total),
    };
  });

  return {
    as_of: asOf,
    metric: 'conviction',
    horizon,
    basis: 'conviction_decile',
    bins,
    total_samples: bins.reduce((sum, bin) => sum + bin.sample_size, 0),
  };
}

function applyOpportunityCalibration(
  item: OpportunityItem,
  snapshot: MarketCalibrationSnapshotPayload | null
): OpportunityItem {
  return {
    ...item,
    calibration: buildOpportunityCalibrationFromSnapshot(snapshot, item.conviction_score, item.direction),
  };
}

function normalizeOpportunityItemsForPublishing(
  items: OpportunityItem[],
  convictionCalibration: MarketCalibrationSnapshotPayload | null
): OpportunityItem[] {
  return items.map((item) => {
    const calibrated = applyOpportunityCalibration(item, convictionCalibration);
    return withOpportunityCoherenceMetadata({
      ...calibrated,
      expectancy: normalizeOpportunityExpectancy(calibrated.expectancy, calibrated.direction),
    });
  });
}

async function computeHistoricalHitStats(
  db: D1Database,
  horizon: '7d' | '30d',
  asOfDate?: string
): Promise<{ hitRate: number; sampleSize: number }> {
  const dateFilter = asOfDate ? 'AND prediction_date <= ?' : '';
  const rows = horizon === '7d'
    ? await db.prepare(`
      SELECT predicted_change_7d as predicted_change, actual_change_7d as actual_change
      FROM prediction_log
      WHERE predicted_change_7d IS NOT NULL
        AND actual_change_7d IS NOT NULL
        ${dateFilter}
      ORDER BY prediction_date DESC
      LIMIT 500
    `).bind(...(asOfDate ? [asOfDate] : [])).all<{ predicted_change: number; actual_change: number }>()
    : await db.prepare(`
      SELECT predicted_change_30d as predicted_change, actual_change_30d as actual_change
      FROM prediction_log
      WHERE predicted_change_30d IS NOT NULL
        AND actual_change_30d IS NOT NULL
        ${dateFilter}
      ORDER BY prediction_date DESC
      LIMIT 500
    `).bind(...(asOfDate ? [asOfDate] : [])).all<{ predicted_change: number; actual_change: number }>();

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

function buildExpectancyUnavailable(
  direction: OpportunityDirection,
  reason: string,
  basis: OpportunityExpectancyBasis = 'none',
  sampleSize = 0
): OpportunityExpectancy {
  if (direction === 'neutral') {
    return {
      expected_move_pct: null,
      max_adverse_move_pct: null,
      sample_size: 0,
      basis: 'none',
      quality: 'INSUFFICIENT',
      unavailable_reason: 'neutral_direction',
    };
  }

  return {
    expected_move_pct: null,
    max_adverse_move_pct: null,
    sample_size: sampleSize,
    basis,
    quality: calibrationQualityForSampleSize(sampleSize),
    unavailable_reason: reason,
  };
}

function resolveMaxAdverseMove(outcomes: number[], direction: OpportunityDirection): number {
  return direction === 'bullish' ? Math.min(...outcomes) : Math.max(...outcomes);
}

function buildExpectancyFromOutcomes(
  outcomes: number[],
  direction: OpportunityDirection,
  basis: OpportunityExpectancyBasis
): OpportunityExpectancy {
  if (direction !== 'bullish' && direction !== 'bearish') {
    return buildExpectancyUnavailable(direction, 'neutral_direction');
  }
  if (outcomes.length === 0) {
    return buildExpectancyUnavailable(direction, 'insufficient_sample', basis, 0);
  }
  const expectedMove = outcomes.reduce((sum, value) => sum + value, 0) / outcomes.length;
  const maxAdverse = resolveMaxAdverseMove(outcomes, direction);
  const quality = calibrationQualityForSampleSize(outcomes.length);
  return {
    expected_move_pct: expectedMove,
    max_adverse_move_pct: maxAdverse,
    sample_size: outcomes.length,
    basis,
    quality,
    unavailable_reason: outcomes.length < MINIMUM_RELIABLE_SAMPLE ? 'insufficient_sample' : null,
  };
}

async function computeOpportunityOutcomeHistory(
  db: D1Database,
  horizon: '7d' | '30d'
): Promise<{
  byThemeDirection: Map<string, number[]>;
  byDirection: Record<OpportunityDirection, number[]>;
}> {
  const horizonDays = horizon === '7d' ? 7 : 30;
  const [snapshots, spyRows] = await Promise.all([
    db.prepare(`
      SELECT as_of, payload_json
      FROM opportunity_snapshots
      WHERE horizon = ?
      ORDER BY as_of DESC
      LIMIT 730
    `).bind(horizon).all<{ as_of: string; payload_json: string }>(),
    db.prepare(`
      SELECT date, value
      FROM indicator_values
      WHERE indicator_id = 'spy_close'
      ORDER BY date ASC
    `).all<{ date: string; value: number }>(),
  ]);

  const spyMap = new Map<string, number>();
  for (const row of spyRows.results || []) {
    spyMap.set(row.date, row.value);
  }

  const priceOnOrAfter = (date: string, toleranceDays = 3): number | null => {
    for (let offset = 0; offset <= toleranceDays; offset += 1) {
      const candidateDate = addCalendarDays(date, offset);
      const price = spyMap.get(candidateDate);
      if (price !== undefined) return price;
    }
    return null;
  };

  const byThemeDirection = new Map<string, number[]>();
  const byDirection: Record<OpportunityDirection, number[]> = {
    bullish: [],
    bearish: [],
    neutral: [],
  };

  for (const row of snapshots.results || []) {
    let payload: OpportunitySnapshot | null = null;
    try {
      payload = JSON.parse(row.payload_json) as OpportunitySnapshot;
    } catch {
      payload = null;
    }
    if (!payload?.items || payload.items.length === 0) continue;

    const asOfDate = (payload.as_of || row.as_of).slice(0, 10);
    const spot = priceOnOrAfter(asOfDate, 3);
    const forward = priceOnOrAfter(addCalendarDays(asOfDate, horizonDays), 3);
    if (spot === null || forward === null || spot === 0) continue;
    const forwardReturnPct = ((forward - spot) / spot) * 100;

    for (const item of payload.items) {
      if (item.direction !== 'bullish' && item.direction !== 'bearish') {
        continue;
      }
      const key = `${item.theme_id}::${item.direction}`;
      const outcomes = byThemeDirection.get(key) || [];
      outcomes.push(forwardReturnPct);
      byThemeDirection.set(key, outcomes);
      byDirection[item.direction].push(forwardReturnPct);
    }
  }

  return {
    byThemeDirection,
    byDirection,
  };
}

function buildOpportunityExpectancy(
  history: {
    byThemeDirection: Map<string, number[]>;
    byDirection: Record<OpportunityDirection, number[]>;
  },
  item: Pick<OpportunityItem, 'theme_id' | 'direction'>
): OpportunityExpectancy {
  if (item.direction !== 'bullish' && item.direction !== 'bearish') {
    return buildExpectancyUnavailable(item.direction, 'neutral_direction');
  }

  const themeOutcomes = history.byThemeDirection.get(`${item.theme_id}::${item.direction}`) || [];
  const directionPrior = history.byDirection[item.direction] || [];

  if (themeOutcomes.length >= 20) {
    return buildExpectancyFromOutcomes(themeOutcomes, item.direction, 'theme_direction');
  }

  if (themeOutcomes.length >= 5 && directionPrior.length >= 20) {
    const themeExpected = themeOutcomes.reduce((sum, value) => sum + value, 0) / themeOutcomes.length;
    const priorExpected = directionPrior.reduce((sum, value) => sum + value, 0) / directionPrior.length;
    const themeAdverse = resolveMaxAdverseMove(themeOutcomes, item.direction);
    const priorAdverse = resolveMaxAdverseMove(directionPrior, item.direction);
    const w = themeOutcomes.length / (themeOutcomes.length + 20);
    const shrunkExpected = w * themeExpected + (1 - w) * priorExpected;
    const shrunkAdverse = w * themeAdverse + (1 - w) * priorAdverse;
    const sampleSize = themeOutcomes.length + directionPrior.length;
    return {
      expected_move_pct: shrunkExpected,
      max_adverse_move_pct: shrunkAdverse,
      sample_size: sampleSize,
      basis: 'theme_direction_shrunk_prior',
      quality: calibrationQualityForSampleSize(sampleSize),
      unavailable_reason: sampleSize < MINIMUM_RELIABLE_SAMPLE ? 'insufficient_sample' : null,
    };
  }

  if (themeOutcomes.length < 5 && directionPrior.length >= 20) {
    return buildExpectancyFromOutcomes(directionPrior, item.direction, 'direction_prior_proxy');
  }

  return buildExpectancyUnavailable(item.direction, 'insufficient_sample', 'none', themeOutcomes.length);
}

function normalizeOpportunityExpectancy(
  expectancy: Partial<OpportunityExpectancy> | null | undefined,
  direction: OpportunityDirection
): OpportunityExpectancy {
  if (direction === 'neutral') {
    return buildExpectancyUnavailable(direction, 'neutral_direction');
  }

  if (!expectancy || typeof expectancy !== 'object') {
    return buildExpectancyUnavailable(direction, 'insufficient_sample');
  }

  const sampleSize = Math.max(0, Math.floor(toNumber(expectancy.sample_size, 0)));
  const expectedMove = expectancy.expected_move_pct === null || expectancy.expected_move_pct === undefined
    ? null
    : toNumber(expectancy.expected_move_pct, 0);
  const maxAdverse = expectancy.max_adverse_move_pct === null || expectancy.max_adverse_move_pct === undefined
    ? null
    : toNumber(expectancy.max_adverse_move_pct, 0);
  const basis = (() => {
    const candidate = String(expectancy.basis || '');
    if (candidate === 'theme_direction' || candidate === 'theme_direction_shrunk_prior' || candidate === 'direction_prior_proxy' || candidate === 'none') {
      return candidate as OpportunityExpectancyBasis;
    }
    return expectedMove !== null || maxAdverse !== null ? 'theme_direction' : 'none';
  })();
  const quality = (() => {
    const candidate = String(expectancy.quality || '');
    if (candidate === 'ROBUST' || candidate === 'LIMITED' || candidate === 'INSUFFICIENT') {
      return candidate as CoverageQuality;
    }
    return calibrationQualityForSampleSize(sampleSize);
  })();
  const unavailableReason = expectancy.unavailable_reason === null || expectancy.unavailable_reason === undefined
    ? (sampleSize < MINIMUM_RELIABLE_SAMPLE ? 'insufficient_sample' : null)
    : String(expectancy.unavailable_reason);

  return {
    expected_move_pct: expectedMove,
    max_adverse_move_pct: maxAdverse,
    sample_size: sampleSize,
    basis,
    quality,
    unavailable_reason: unavailableReason,
  };
}

function evaluateOpportunityCoherence(item: OpportunityItem): OpportunityEligibility {
  const failed = new Set<OpportunityEligibilityCheck>();
  const calibrationProbability = item.calibration.probability_correct_direction;
  const expectedMove = item.expectancy.expected_move_pct;

  if (item.direction === 'neutral') {
    failed.add('neutral_direction_not_actionable');
  }

  if (calibrationProbability === null) {
    if (item.calibration.quality !== 'INSUFFICIENT') {
      failed.add('incomplete_contract');
    }
  } else if (calibrationProbability < OPPORTUNITY_COHERENCE_MIN_PROBABILITY) {
    failed.add('calibration_probability_below_threshold');
  }

  if (expectedMove === null) {
    if (item.expectancy.quality !== 'INSUFFICIENT') {
      failed.add('incomplete_contract');
    }
  } else if (item.direction === 'bullish' && expectedMove <= 0) {
    failed.add('expectancy_sign_conflict');
  } else if (item.direction === 'bearish' && expectedMove >= 0) {
    failed.add('expectancy_sign_conflict');
  }

  return {
    passed: failed.size === 0,
    failed_checks: [...failed],
  };
}

function inferOpportunityConfidenceBand(
  item: OpportunityItem,
  eligibility: OpportunityEligibility
): OpportunityConfidenceBand {
  if (!eligibility.passed) return 'low';
  const probability = item.calibration.probability_correct_direction;
  if (probability === null || item.expectancy.expected_move_pct === null) return 'low';
  if (
    item.calibration.quality === 'ROBUST' &&
    item.expectancy.quality === 'ROBUST' &&
    probability >= 0.6
  ) {
    return 'high';
  }
  if (probability >= OPPORTUNITY_COHERENCE_MIN_PROBABILITY) {
    return 'medium';
  }
  return 'low';
}

function buildOpportunityDecisionContract(
  item: OpportunityItem,
  eligibility: OpportunityEligibility
): OpportunityDecisionContract {
  const qualityCodes = [
    `calibration_${item.calibration.quality.toLowerCase()}`,
    `expectancy_${item.expectancy.quality.toLowerCase()}`,
  ];

  return {
    coherent: eligibility.passed,
    confidence_band: inferOpportunityConfidenceBand(item, eligibility),
    rationale_codes: eligibility.passed
      ? ['coherent_contract', ...qualityCodes]
      : [...eligibility.failed_checks, ...qualityCodes],
  };
}

function withOpportunityCoherenceMetadata(item: OpportunityItem): OpportunityItem {
  const eligibility = evaluateOpportunityCoherence(item);
  return {
    ...item,
    eligibility,
    decision_contract: buildOpportunityDecisionContract(item, eligibility),
  };
}

function applyOpportunityCoherenceGate(
  items: OpportunityItem[],
  enabled: boolean
): {
  items: OpportunityItem[];
  suppressed_count: number;
} {
  const decorated = items.map(withOpportunityCoherenceMetadata);
  if (!enabled) {
    return {
      items: decorated,
      suppressed_count: 0,
    };
  }

  const eligible = decorated.filter((item) => item.eligibility.passed);
  return {
    items: eligible,
    suppressed_count: Math.max(0, decorated.length - eligible.length),
  };
}

interface OpportunityFeedProjection {
  items: OpportunityItem[];
  suppressed_count: number;
  degraded_reason: string | null;
  quality_filtered_count: number;
  coherence_suppressed_count: number;
  suppressed_data_quality: boolean;
  suppression_by_reason: OpportunitySuppressionByReason;
  total_candidates: number;
  quality_filter_rate: number;
  coherence_fail_rate: number;
}

function projectOpportunityFeed(
  items: OpportunityItem[],
  options: {
    coherence_gate_enabled: boolean;
    freshness: FreshnessStatus;
    consistency_state: ConsistencyState;
  }
): OpportunityFeedProjection {
  const totalCandidates = items.length;
  const qualityFiltered = removeLowInformationOpportunities(items);
  const coherenceFiltered = applyOpportunityCoherenceGate(
    qualityFiltered.items,
    options.coherence_gate_enabled
  );
  const suppressForDataQuality = options.freshness.critical_stale_count > 0 || options.consistency_state === 'FAIL';
  const dataQualitySuppressedCount = suppressForDataQuality ? coherenceFiltered.items.length : 0;
  const suppressionByReason: OpportunitySuppressionByReason = {
    coherence_failed: coherenceFiltered.suppressed_count,
    quality_filtered: qualityFiltered.filtered_count,
    data_quality_suppressed: dataQualitySuppressedCount,
  };
  const qualityFilterRate = totalCandidates > 0
    ? Number((qualityFiltered.filtered_count / totalCandidates).toFixed(4))
    : 0;
  const coherenceFailRate = totalCandidates > 0
    ? Number((coherenceFiltered.suppressed_count / totalCandidates).toFixed(4))
    : 0;

  if (suppressForDataQuality) {
    return {
      items: [],
      suppressed_count: qualityFiltered.filtered_count + coherenceFiltered.suppressed_count + coherenceFiltered.items.length,
      degraded_reason: 'suppressed_data_quality',
      quality_filtered_count: qualityFiltered.filtered_count,
      coherence_suppressed_count: coherenceFiltered.suppressed_count,
      suppressed_data_quality: true,
      suppression_by_reason: suppressionByReason,
      total_candidates: totalCandidates,
      quality_filter_rate: qualityFilterRate,
      coherence_fail_rate: coherenceFailRate,
    };
  }

  let degradedReason: string | null = null;
  if (coherenceFiltered.suppressed_count > 0 && options.coherence_gate_enabled) {
    degradedReason = 'coherence_gate_failed';
  } else if (qualityFiltered.filtered_count > 0) {
    degradedReason = 'quality_filtered';
  }

  return {
    items: coherenceFiltered.items,
    suppressed_count: qualityFiltered.filtered_count + coherenceFiltered.suppressed_count,
    degraded_reason: degradedReason,
    quality_filtered_count: qualityFiltered.filtered_count,
    coherence_suppressed_count: coherenceFiltered.suppressed_count,
    suppressed_data_quality: false,
    suppression_by_reason: suppressionByReason,
    total_candidates: totalCandidates,
    quality_filter_rate: qualityFilterRate,
    coherence_fail_rate: coherenceFailRate,
  };
}

function resolvePlanActionability(args: {
  opportunity_ref?: PlanPayload['opportunity_ref'];
  edge_quality: EdgeQualitySnapshot;
  freshness: FreshnessStatus;
  consistency: ConsistencySnapshot;
}): {
  state: PlanActionabilityState;
  reason_codes: PlanActionabilityReasonCode[];
} {
  const reasonCodes: PlanActionabilityReasonCode[] = [];

  if (args.freshness.critical_stale_count > 0) {
    reasonCodes.push('critical_data_quality_block');
    return { state: 'NO_ACTION', reason_codes: reasonCodes };
  }
  if (args.consistency.state === 'FAIL') {
    reasonCodes.push('consistency_fail_block');
    return { state: 'NO_ACTION', reason_codes: reasonCodes };
  }

  const opportunityRef = args.opportunity_ref;
  if (!opportunityRef) {
    reasonCodes.push('opportunity_reference_unavailable');
    return { state: 'NO_ACTION', reason_codes: reasonCodes };
  }

  if (opportunityRef.eligible_count <= 0) {
    reasonCodes.push('no_eligible_opportunities');
    if (opportunityRef.degraded_reason) {
      reasonCodes.push(`opportunity_${opportunityRef.degraded_reason}`);
    }
    if (args.edge_quality.label === 'HIGH') {
      reasonCodes.push('high_edge_override_no_eligible');
    }
    return { state: 'NO_ACTION', reason_codes: reasonCodes };
  }

  if (args.edge_quality.label === 'HIGH') {
    reasonCodes.push('high_edge_with_eligible_opportunities');
    return { state: 'ACTIONABLE', reason_codes: reasonCodes };
  }

  if (args.edge_quality.label === 'MEDIUM') {
    reasonCodes.push('medium_edge_watch');
    return { state: 'WATCH', reason_codes: reasonCodes };
  }

  reasonCodes.push('low_edge_watch');
  return { state: 'WATCH', reason_codes: reasonCodes };
}

function summarizeCrossHorizonCoherence(args: {
  projected_7d: OpportunityFeedProjection | null;
  projected_30d: OpportunityFeedProjection | null;
  as_of_7d: string | null;
  as_of_30d: string | null;
}): {
  as_of: string;
  state: 'ALIGNED' | 'MIXED' | 'CONFLICT' | 'INSUFFICIENT';
  eligible_7d: number;
  eligible_30d: number;
  top_direction_7d: OpportunityDirection | null;
  top_direction_30d: OpportunityDirection | null;
  rationale_codes: string[];
  invalidation_note: string | null;
} | null {
  const projected7 = args.projected_7d;
  const projected30 = args.projected_30d;
  if (!projected7 && !projected30) {
    return null;
  }

  const eligible7 = projected7?.items.length || 0;
  const eligible30 = projected30?.items.length || 0;
  const top7 = projected7?.items[0]?.direction ?? null;
  const top30 = projected30?.items[0]?.direction ?? null;
  const reasons: string[] = [];
  let state: 'ALIGNED' | 'MIXED' | 'CONFLICT' | 'INSUFFICIENT' = 'INSUFFICIENT';
  let invalidationNote: string | null = null;

  if (eligible7 <= 0 && eligible30 <= 0) {
    state = 'INSUFFICIENT';
    reasons.push('no_eligible_both_horizons');
    invalidationNote = 'Wait for both horizons to repopulate with eligible opportunities.';
  } else if (eligible7 > 0 && eligible30 > 0) {
    if (top7 && top30 && top7 !== top30) {
      state = 'CONFLICT';
      reasons.push('direction_conflict');
      invalidationNote = 'Stand down until 7d and 30d directions re-align.';
    } else {
      state = 'ALIGNED';
      reasons.push('direction_aligned');
    }
  } else {
    state = 'MIXED';
    reasons.push('single_horizon_signal');
    invalidationNote = 'Treat the active horizon as tactical only until cross-horizon confirmation appears.';
  }

  const asOfCandidates = [args.as_of_7d, args.as_of_30d]
    .filter((value): value is string => Boolean(value))
    .sort();

  return {
    as_of: asOfCandidates.length > 0 ? asOfCandidates[asOfCandidates.length - 1] : asIsoDateTime(new Date()),
    state,
    eligible_7d: eligible7,
    eligible_30d: eligible30,
    top_direction_7d: top7,
    top_direction_30d: top30,
    rationale_codes: reasons,
    invalidation_note: invalidationNote,
  };
}

function applyCrossHorizonActionabilityOverride(
  base: { state: PlanActionabilityState; reason_codes: PlanActionabilityReasonCode[] },
  coherence: {
    state: 'ALIGNED' | 'MIXED' | 'CONFLICT' | 'INSUFFICIENT';
  } | null
): { state: PlanActionabilityState; reason_codes: PlanActionabilityReasonCode[] } {
  if (!coherence) return base;
  if (base.state === 'NO_ACTION') return base;

  const reasonCodes = [...base.reason_codes];
  if (coherence.state === 'CONFLICT') {
    reasonCodes.push('cross_horizon_conflict_watch');
    return {
      state: 'WATCH',
      reason_codes: Array.from(new Set(reasonCodes)),
    };
  }
  if (coherence.state === 'INSUFFICIENT') {
    reasonCodes.push('cross_horizon_insufficient_watch');
    return {
      state: 'WATCH',
      reason_codes: Array.from(new Set(reasonCodes)),
    };
  }
  return base;
}

function buildDecisionStack(args: {
  actionability_state: PlanActionabilityState;
  setup_summary: string;
  edge_quality: EdgeQualitySnapshot;
  consistency: ConsistencySnapshot;
  opportunity_ref?: PlanPayload['opportunity_ref'];
  brief_ref?: PlanPayload['brief_ref'];
  alerts_ref?: PlanPayload['alerts_ref'];
  cross_horizon?: PlanPayload['cross_horizon'];
}): NonNullable<PlanPayload['decision_stack']> {
  const regimeDelta = args.brief_ref?.regime_delta?.toLowerCase() || 'unchanged';
  const eligibleCount = args.opportunity_ref?.eligible_count ?? 0;
  const warning24h = args.alerts_ref?.warning_count_24h ?? 0;
  const critical24h = args.alerts_ref?.critical_count_24h ?? 0;
  const crossHorizonState = args.cross_horizon?.state || 'INSUFFICIENT';

  const whatChanged = `${regimeDelta} regime delta · ${eligibleCount} eligible opportunities · ${warning24h} warning / ${critical24h} critical alerts (24h).`;
  let whatToDo = 'Monitor and wait for cleaner setup.';
  if (args.actionability_state === 'ACTIONABLE') {
    whatToDo = 'Execute the playbook target with standard risk controls and invalidation checks.';
  } else if (args.actionability_state === 'WATCH') {
    whatToDo = 'Maintain watch posture and require confirmation before adding risk.';
  }

  const whyNow = `Edge ${args.edge_quality.label.toLowerCase()} · consistency ${args.consistency.state.toLowerCase()} · cross-horizon ${crossHorizonState.toLowerCase()}.`;
  const confidence = `edge=${args.edge_quality.label} | consistency=${args.consistency.state} | cross_horizon=${crossHorizonState}`;

  return {
    what_changed: whatChanged,
    what_to_do: whatToDo,
    why_now: whyNow,
    confidence,
    cta_state: args.actionability_state,
  };
}

function evaluateOpportunityCtaState(
  projectedFeed: OpportunityFeedProjection,
  diagnostics: CalibrationDiagnosticsSnapshot,
  ttl: OpportunityTtlMetadata,
  degradedReason: string | null,
): {
  cta_enabled: boolean;
  cta_disabled_reasons: OpportunityCtaDisabledReason[];
  actionability_state: PlanActionabilityState;
} {
  const disabledReasons: OpportunityCtaDisabledReason[] = [];

  const actionabilityState: PlanActionabilityState = projectedFeed.items.length > 0
    ? (degradedReason === null ? 'ACTIONABLE' : 'WATCH')
    : 'NO_ACTION';

  if (projectedFeed.items.length === 0) {
    disabledReasons.push('no_eligible_opportunities');
  }
  if (projectedFeed.degraded_reason === 'suppressed_data_quality') {
    disabledReasons.push('suppressed_data_quality');
  }
  if (diagnostics.quality_band !== 'ROBUST') {
    disabledReasons.push('calibration_quality_not_robust');
  }
  if (diagnostics.ece === null) {
    disabledReasons.push('calibration_ece_unavailable');
  } else if (diagnostics.ece > OPPORTUNITY_CTA_MAX_ECE) {
    disabledReasons.push('ece_above_threshold');
  }
  if (ttl.ttl_state === 'overdue') {
    disabledReasons.push('refresh_ttl_overdue');
  } else if (ttl.ttl_state === 'unknown') {
    disabledReasons.push('refresh_ttl_unknown');
  }

  return {
    cta_enabled: disabledReasons.length === 0,
    cta_disabled_reasons: Array.from(new Set(disabledReasons)),
    actionability_state: actionabilityState,
  };
}

function calibrationQualityRank(quality: CalibrationQuality): number {
  if (quality === 'ROBUST') return 3;
  if (quality === 'LIMITED') return 2;
  return 1;
}

function worstCalibrationQuality(...qualities: CalibrationQuality[]): CalibrationQuality {
  if (qualities.length === 0) return 'INSUFFICIENT';
  return [...qualities].sort((a, b) => calibrationQualityRank(a) - calibrationQualityRank(b))[0];
}

function calibrationPenaltyPoints(quality: CalibrationQuality): number {
  if (quality === 'ROBUST') return 0;
  if (quality === 'LIMITED') return 8;
  return 16;
}

function removeLowInformationOpportunities(items: OpportunityItem[]): {
  items: OpportunityItem[];
  filtered_count: number;
} {
  const filtered = items.filter((item) => {
    const lowInformation = item.calibration.quality === 'INSUFFICIENT' && item.expectancy.quality === 'INSUFFICIENT';
    return !(item.direction === 'neutral' && lowInformation);
  });

  if (filtered.length > 0) {
    return {
      items: filtered,
      filtered_count: Math.max(0, items.length - filtered.length),
    };
  }

  const fallbackCount = Math.min(3, items.length);
  return {
    items: items.slice(0, fallbackCount),
    filtered_count: Math.max(0, items.length - fallbackCount),
  };
}

function applyQualityGateToConviction(rawConviction: number, qualityFactor: number): number {
  const bounded = clamp(0, 100, rawConviction);
  const factor = clamp(0.55, 1, qualityFactor);
  // Shrink conviction toward neutral when data quality/calibration is degraded.
  return Math.round(clamp(0, 100, 50 + (bounded - 50) * factor));
}

async function buildOpportunitySnapshot(
  db: D1Database,
  horizon: '7d' | '30d',
  calibrationSnapshot?: MarketCalibrationSnapshotPayload | null,
  options?: {
    sanitize_signals_tickers?: boolean;
  }
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

  const [latestSignal, latestEnsemble, hitStats, themes, convictionCalibration, opportunityHistory, freshness, edgeCalibration] = await Promise.all([
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
    fetchLatestSignalsThemes({ sanitize_tickers: options?.sanitize_signals_tickers !== false }),
    calibrationSnapshot ? Promise.resolve(calibrationSnapshot) : fetchLatestCalibrationSnapshot(db, 'conviction', horizon),
    computeOpportunityOutcomeHistory(db, horizon),
    computeFreshnessStatus(db),
    fetchLatestCalibrationSnapshot(db, 'edge_quality', null),
  ]);

  const freshnessPenalty = freshnessPenaltyCount(freshness);
  const convictionCalibrationQuality = calibrationQualityForSampleSize(convictionCalibration?.total_samples ?? 0);
  const edgeCalibrationQuality = calibrationQualityForSampleSize(edgeCalibration?.total_samples ?? 0);
  const qualityFloor = worstCalibrationQuality(convictionCalibrationQuality, edgeCalibrationQuality);
  const qualityPenalty = Math.min(
    30,
    freshnessPenalty * 3 + calibrationPenaltyPoints(qualityFloor)
  );
  const convictionQualityFactor = clamp(0.55, 1, 1 - qualityPenalty / 100);

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
    const rawConviction = clamp(0, 100, Math.round(
      0.35 * mlComponent +
      0.25 * similarComponent +
      0.20 * signalComponent +
      0.20 * themeComponent
    ));
    const conviction = applyQualityGateToConviction(rawConviction, convictionQualityFactor);

    const directionalSignal = parseSignalDirection(ensembleValue + (theme.score - 50) * 0.06 + deltaBias * 0.08);
    const supportingFactors = [
      ...(theme.classification?.signal_type ? [theme.classification.signal_type] : []),
      ...(theme.key_tickers.slice(0, 3)),
      latestSignal?.signal_type || 'signal_context',
      conviction < rawConviction ? 'quality_gate_applied' : null,
    ].filter(Boolean);

    const id = buildOpportunityId(latestPxi.date, theme.theme_id, horizon);
    const rationale = `${theme.theme_name}: ${directionalSignal} setup with conviction ${conviction}/100, combining ensemble trend, historical analog hit-rate, and current signal regime.`;

    const baseItem: OpportunityItem = {
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
      calibration: buildOpportunityCalibrationFromSnapshot(convictionCalibration, conviction, directionalSignal),
      expectancy: buildOpportunityExpectancy(opportunityHistory, {
        theme_id: theme.theme_id,
        direction: directionalSignal,
      }),
      eligibility: { passed: false, failed_checks: ['incomplete_contract'] },
      decision_contract: {
        coherent: false,
        confidence_band: 'low',
        rationale_codes: ['incomplete_contract'],
      },
      updated_at: asIsoDateTime(new Date()),
    };
    return withOpportunityCoherenceMetadata(baseItem);
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

async function buildHistoricalOpportunitySnapshot(
  db: D1Database,
  asOfDate: string,
  horizon: '7d' | '30d'
): Promise<OpportunitySnapshot | null> {
  const latestPxi = await db.prepare(`
    SELECT date, score, delta_7d, delta_30d
    FROM pxi_scores
    WHERE date = ?
    LIMIT 1
  `).bind(asOfDate).first<{
    date: string;
    score: number;
    delta_7d: number | null;
    delta_30d: number | null;
  }>();

  if (!latestPxi) {
    return null;
  }

  const [latestSignal, latestEnsemble, hitStats, themeRows] = await Promise.all([
    db.prepare(`
      SELECT date, risk_allocation, signal_type, regime
      FROM pxi_signal
      WHERE date <= ?
      ORDER BY date DESC
      LIMIT 1
    `).bind(asOfDate).first<{
      date: string;
      risk_allocation: number;
      signal_type: string;
      regime: string;
    }>(),
    db.prepare(`
      SELECT prediction_date, ensemble_7d, ensemble_30d, confidence_7d, confidence_30d
      FROM ensemble_predictions
      WHERE prediction_date <= ?
      ORDER BY prediction_date DESC
      LIMIT 1
    `).bind(asOfDate).first<{
      prediction_date: string;
      ensemble_7d: number | null;
      ensemble_30d: number | null;
      confidence_7d: string | null;
      confidence_30d: string | null;
    }>(),
    computeHistoricalHitStats(db, horizon, asOfDate),
    db.prepare(`
      SELECT category as theme_id, category as theme_name, score
      FROM category_scores
      WHERE date = ?
      ORDER BY score DESC
      LIMIT 12
    `).bind(asOfDate).all<{ theme_id: string; theme_name: string; score: number }>(),
  ]);

  const themeSource = (themeRows.results || [])
    .map((row) => ({
      theme_id: row.theme_id,
      theme_name: row.theme_name,
      score: row.score,
      key_tickers: [] as string[],
    }));

  if (themeSource.length === 0) {
    return null;
  }

  const ensembleValue = horizon === '7d' ? (latestEnsemble?.ensemble_7d ?? 0) : (latestEnsemble?.ensemble_30d ?? 0);
  const ensembleConfidence = horizon === '7d' ? latestEnsemble?.confidence_7d : latestEnsemble?.confidence_30d;
  const deltaBias = horizon === '7d' ? (latestPxi.delta_7d ?? 0) : (latestPxi.delta_30d ?? 0);

  const mlComponent = clamp(0, 100, 50 + ensembleValue * 6 + confidenceTextToScore(ensembleConfidence) * 0.3 + deltaBias * 0.4);
  const similarComponent = clamp(0, 100, hitStats.hitRate * 100 + Math.min(20, Math.log10(hitStats.sampleSize + 1) * 10));
  const signalComponent = clamp(0, 100, (latestSignal?.risk_allocation ?? 0.5) * 100);

  const items: OpportunityItem[] = themeSource.map((theme) => {
    const themeComponent = clamp(0, 100, theme.score * 0.85 + 50 * 0.15);
    const conviction = clamp(0, 100, Math.round(
      0.35 * mlComponent +
      0.25 * similarComponent +
      0.20 * signalComponent +
      0.20 * themeComponent
    ));
    const directionalSignal = parseSignalDirection(ensembleValue + (theme.score - 50) * 0.06 + deltaBias * 0.08);
    const id = buildOpportunityId(asOfDate, theme.theme_id, horizon);
    const rationale = `${theme.theme_name}: ${directionalSignal} setup from historical backfill seed model (${conviction}/100).`;

    const baseItem: OpportunityItem = {
      id,
      symbol: null,
      theme_id: theme.theme_id,
      theme_name: theme.theme_name,
      direction: directionalSignal,
      conviction_score: conviction,
      rationale,
      supporting_factors: [
        latestSignal?.signal_type || 'signal_context',
        'historical_backfill_seed',
      ],
      historical_hit_rate: hitStats.hitRate,
      sample_size: hitStats.sampleSize,
      calibration: buildOpportunityCalibrationFallback(),
      expectancy: buildExpectancyUnavailable(directionalSignal, 'historical_backfill_seed', 'none', 0),
      eligibility: { passed: false, failed_checks: ['incomplete_contract'] },
      decision_contract: {
        coherent: false,
        confidence_band: 'low',
        rationale_codes: ['incomplete_contract'],
      },
      updated_at: asIsoDateTime(new Date()),
    };
    return withOpportunityCoherenceMetadata(baseItem);
  });

  items.sort((a, b) => {
    if (b.conviction_score !== a.conviction_score) return b.conviction_score - a.conviction_score;
    if (b.sample_size !== a.sample_size) return b.sample_size - a.sample_size;
    return a.theme_id.localeCompare(b.theme_id);
  });

  return {
    as_of: `${asOfDate}T00:00:00.000Z`,
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
    id: `${runDate}-${type}-${stableHash(dedupeKey)}`,
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

  const staleCount = Math.max(0, Math.floor(toNumber(brief.freshness_status.stale_count, 0)));
  const criticalStaleCount = Math.max(0, Math.floor(toNumber(brief.freshness_status.critical_stale_count, 0)));
  if (brief.freshness_status.has_stale_data || staleCount > 0) {
    const severity: AlertSeverity = criticalStaleCount > 0 ? 'critical' : 'warning';
    events.push(buildMarketEvent(
      'freshness_warning',
      runDate,
      severity,
      'Data freshness warning',
      criticalStaleCount > 0
        ? `${staleCount} indicator(s) are stale (${criticalStaleCount} critical) and may impact confidence.`
        : `${staleCount} non-critical indicator(s) are stale and may impact confidence.`,
      'market',
      'data_freshness',
      { stale_count: staleCount, critical_stale_count: criticalStaleCount }
    ));
  }

  if (brief.consistency.state === 'WARN' || brief.consistency.state === 'FAIL') {
    events.push(buildMarketEvent(
      'threshold_cross',
      runDate,
      brief.consistency.state === 'FAIL' ? 'critical' : 'warning',
      'Consistency warning',
      `Public decision consistency is ${brief.consistency.state} (score ${brief.consistency.score}).`,
      'market',
      'consistency',
      {
        consistency_state: brief.consistency.state,
        consistency_score: brief.consistency.score,
        violations: brief.consistency.violations,
      }
    ));
  }

  return events;
}

async function storeBriefSnapshot(db: D1Database, brief: BriefSnapshot): Promise<void> {
  await db.prepare(`
    INSERT OR REPLACE INTO market_brief_snapshots (as_of, contract_version, payload_json, created_at)
    VALUES (?, ?, ?, datetime('now'))
  `).bind(brief.as_of, brief.contract_version || BRIEF_CONTRACT_VERSION, JSON.stringify(brief)).run();
}

async function storeConsistencyCheck(
  db: D1Database,
  asOf: string,
  consistency: ConsistencySnapshot
): Promise<void> {
  await db.prepare(`
    INSERT OR REPLACE INTO market_consistency_checks (as_of, score, state, violations_json, components_json, created_at)
    VALUES (?, ?, ?, ?, ?, datetime('now'))
  `).bind(
    asOf,
    consistency.score,
    consistency.state,
    JSON.stringify(consistency.violations),
    JSON.stringify(consistency.components),
  ).run();
}

async function fetchLatestConsistencyCheck(db: D1Database): Promise<{
  as_of: string;
  score: number;
  state: ConsistencyState;
  violations: string[];
  components: ConsistencySnapshot['components'];
  created_at: string;
} | null> {
  const row = await db.prepare(`
    SELECT as_of, score, state, violations_json, components_json, created_at
    FROM market_consistency_checks
    ORDER BY as_of DESC
    LIMIT 1
  `).first<{
    as_of: string;
    score: number;
    state: string;
    violations_json: string;
    components_json?: string | null;
    created_at: string;
  }>();
  if (!row) return null;
  let violations: string[] = [];
  let components: ConsistencySnapshot['components'] = {
    base_score: 100,
    structural_penalty: 0,
    reliability_penalty: 0,
  };
  try {
    const parsed = JSON.parse(row.violations_json) as unknown;
    if (Array.isArray(parsed)) {
      violations = parsed.map((value) => String(value));
    }
  } catch {
    violations = [];
  }
  try {
    if (row.components_json) {
      const parsed = JSON.parse(row.components_json) as Partial<ConsistencySnapshot['components']>;
      if (parsed && typeof parsed === 'object') {
        components = {
          base_score: toNumber(parsed.base_score, 100),
          structural_penalty: toNumber(parsed.structural_penalty, 0),
          reliability_penalty: toNumber(parsed.reliability_penalty, 0),
        };
      }
    }
  } catch {
    components = {
      base_score: 100,
      structural_penalty: 0,
      reliability_penalty: 0,
    };
  }
  const state: ConsistencyState = row.state === 'FAIL' ? 'FAIL' : row.state === 'WARN' ? 'WARN' : 'PASS';
  return {
    as_of: row.as_of,
    score: toNumber(row.score, 0),
    state,
    violations,
    components,
    created_at: row.created_at,
  };
}

async function recordMarketRefreshRunStart(db: D1Database, trigger: string): Promise<number | null> {
  try {
    const result = await db.prepare(`
      INSERT INTO market_refresh_runs (started_at, status, "trigger", created_at)
      VALUES (?, 'running', ?, datetime('now'))
    `).bind(asIsoDateTime(new Date()), trigger || 'unknown').run();

    if (typeof result.meta?.last_row_id === 'number') {
      return result.meta.last_row_id;
    }
    return null;
  } catch (err) {
    console.error('Failed to record market refresh run start:', err);
    return null;
  }
}

async function recordMarketRefreshRunFinish(
  db: D1Database,
  runId: number | null,
  payload: {
    status: 'success' | 'failed';
    brief_generated?: number;
    opportunities_generated?: number;
    calibrations_generated?: number;
    alerts_generated?: number;
    stale_count?: number | null;
    critical_stale_count?: number | null;
    as_of?: string | null;
    error?: string | null;
  }
): Promise<void> {
  if (!runId) return;
  try {
    await db.prepare(`
      UPDATE market_refresh_runs
      SET completed_at = ?,
          status = ?,
          brief_generated = ?,
          opportunities_generated = ?,
          calibrations_generated = ?,
          alerts_generated = ?,
          stale_count = ?,
          critical_stale_count = ?,
          as_of = ?,
          error = ?
      WHERE id = ?
    `).bind(
      asIsoDateTime(new Date()),
      payload.status,
      payload.brief_generated ?? 0,
      payload.opportunities_generated ?? 0,
      payload.calibrations_generated ?? 0,
      payload.alerts_generated ?? 0,
      payload.stale_count ?? null,
      payload.critical_stale_count ?? null,
      payload.as_of ?? null,
      payload.error ?? null,
      runId,
    ).run();
  } catch (err) {
    console.error('Failed to finalize market refresh run:', err);
  }
}

async function fetchLatestSuccessfulMarketRefreshRun(db: D1Database): Promise<{
  completed_at: string | null;
  trigger: string | null;
  stale_count: number | null;
  critical_stale_count: number | null;
  as_of: string | null;
} | null> {
  try {
    const row = await db.prepare(`
      SELECT completed_at, "trigger" as trigger, stale_count, critical_stale_count, as_of
      FROM market_refresh_runs
      WHERE status = 'success'
        AND completed_at IS NOT NULL
      ORDER BY completed_at DESC, id DESC
      LIMIT 1
    `).first<{
      completed_at: string | null;
      trigger: string | null;
      stale_count: number | null;
      critical_stale_count: number | null;
      as_of: string | null;
    }>();
    return row || null;
  } catch {
    return null;
  }
}

async function computeFreshnessSloWindow(
  db: D1Database,
  windowDays: number
): Promise<FreshnessSloWindowSummary> {
  const boundedDays = Math.max(1, Math.floor(windowDays));
  const lookbackExpr = `-${Math.max(0, boundedDays - 1)} days`;

  const [grouped, impactCounts, latestWarningEvent, latestCriticalEvent] = await Promise.all([
    db.prepare(`
      SELECT
        COALESCE(as_of, substr(completed_at, 1, 10)) as run_date,
        MAX(completed_at) as completed_at,
        MAX("trigger") as trigger,
        MAX(COALESCE(stale_count, 0)) as stale_count,
        MAX(COALESCE(critical_stale_count, 0)) as critical_stale_count
      FROM market_refresh_runs
      WHERE status = 'success'
        AND completed_at IS NOT NULL
        AND completed_at >= datetime('now', ?)
      GROUP BY run_date
      ORDER BY run_date DESC
    `).bind(lookbackExpr).all<{
      run_date: string | null;
      completed_at: string | null;
      trigger: string | null;
      stale_count: number | null;
      critical_stale_count: number | null;
    }>(),
    db.prepare(`
      SELECT
        SUM(CASE WHEN severity = 'warning' THEN 1 ELSE 0 END) as warning_events,
        SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical_events
      FROM market_alert_events
      WHERE event_type = 'freshness_warning'
        AND datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
    `).bind(lookbackExpr).first<{
      warning_events: number | null;
      critical_events: number | null;
    }>(),
    db.prepare(`
      SELECT created_at, severity, title, body
      FROM market_alert_events
      WHERE event_type = 'freshness_warning'
        AND severity = 'warning'
        AND datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
      ORDER BY created_at DESC
      LIMIT 1
    `).bind(lookbackExpr).first<{
      created_at: string | null;
      severity: AlertSeverity | null;
      title: string | null;
      body: string | null;
    }>(),
    db.prepare(`
      SELECT created_at, severity, title, body
      FROM market_alert_events
      WHERE event_type = 'freshness_warning'
        AND severity = 'critical'
        AND datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
      ORDER BY created_at DESC
      LIMIT 1
    `).bind(lookbackExpr).first<{
      created_at: string | null;
      severity: AlertSeverity | null;
      title: string | null;
      body: string | null;
    }>(),
  ]);

  const rows = (grouped.results || [])
    .filter((row) => typeof row.run_date === 'string' && row.run_date.length >= 10)
    .map((row) => ({
      as_of: String(row.run_date).slice(0, 10),
      completed_at: row.completed_at || null,
      trigger: row.trigger || null,
      stale_count: Math.max(0, Math.floor(toNumber(row.stale_count, 0))),
      critical_stale_count: Math.max(0, Math.floor(toNumber(row.critical_stale_count, 0))),
    }));

  const daysObserved = rows.length;
  const staleDays = rows.filter((row) => row.stale_count > 0).length;
  const incidentRows = rows.filter((row) => row.critical_stale_count > 0);
  const daysWithCriticalStale = incidentRows.length;
  const sloAttainment = daysObserved > 0
    ? Number((((daysObserved - daysWithCriticalStale) / daysObserved) * 100).toFixed(2))
    : 0;
  const warningEvents = Math.max(0, Math.floor(toNumber(impactCounts?.warning_events, 0)));
  const criticalEvents = Math.max(0, Math.floor(toNumber(impactCounts?.critical_events, 0)));
  const impactState: FreshnessSloImpactSummary['state'] = (daysWithCriticalStale > 0 || criticalEvents > 0)
    ? 'degraded'
    : ((staleDays > 0 || warningEvents > 0) ? 'monitor' : 'none');

  const mapImpactEvent = (event: {
    created_at: string | null;
    severity: AlertSeverity | null;
    title: string | null;
    body: string | null;
  } | null): FreshnessImpactEventSummary | null => {
    if (!event?.created_at || !event.severity || !event.title || !event.body) {
      return null;
    }
    return {
      created_at: event.created_at,
      severity: event.severity,
      title: event.title,
      body: event.body,
    };
  };

  return {
    days_observed: daysObserved,
    days_with_critical_stale: daysWithCriticalStale,
    slo_attainment_pct: sloAttainment,
    recent_incidents: incidentRows.slice(0, 20),
    incident_impact: {
      state: impactState,
      stale_days: staleDays,
      warning_events: warningEvents,
      critical_events: criticalEvents,
      estimated_suppressed_days: daysWithCriticalStale,
      latest_warning_event: mapImpactEvent(latestWarningEvent),
      latest_critical_event: mapImpactEvent(latestCriticalEvent),
    },
  };
}

async function insertUtilityEvent(db: D1Database, payload: UtilityEventInsertPayload): Promise<void> {
  await db.prepare(`
    INSERT INTO market_utility_events
      (session_id, event_type, route, actionability_state, payload_json, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `).bind(
    payload.session_id,
    payload.event_type,
    payload.route,
    payload.actionability_state,
    payload.payload_json,
    payload.created_at,
  ).run();
}

async function computeUtilityFunnelSummary(
  db: D1Database,
  windowDays: number,
): Promise<UtilityFunnelSummary> {
  const boundedWindowDays = Math.max(1, Math.floor(windowDays));
  const lookbackExpr = `-${Math.max(0, boundedWindowDays - 1)} days`;

  const [aggregate, daysRow] = await Promise.all([
    db.prepare(`
      SELECT
        COUNT(*) as total_events,
        COUNT(DISTINCT session_id) as unique_sessions,
        SUM(CASE WHEN event_type = 'plan_view' THEN 1 ELSE 0 END) as plan_views,
        SUM(CASE WHEN event_type = 'opportunities_view' THEN 1 ELSE 0 END) as opportunities_views,
        SUM(CASE WHEN event_type = 'decision_actionable_view' THEN 1 ELSE 0 END) as decision_actionable_views,
        SUM(CASE WHEN event_type = 'decision_watch_view' THEN 1 ELSE 0 END) as decision_watch_views,
        SUM(CASE WHEN event_type = 'decision_no_action_view' THEN 1 ELSE 0 END) as decision_no_action_views,
        SUM(CASE WHEN event_type = 'no_action_unlock_view' THEN 1 ELSE 0 END) as no_action_unlock_views,
        SUM(CASE WHEN event_type = 'cta_action_click' THEN 1 ELSE 0 END) as cta_action_clicks,
        COUNT(DISTINCT CASE WHEN event_type = 'cta_action_click' THEN session_id END) as cta_action_sessions,
        COUNT(DISTINCT CASE WHEN event_type = 'decision_actionable_view' THEN session_id END) as actionable_view_sessions,
        COUNT(DISTINCT CASE WHEN event_type IN ('decision_actionable_view', 'cta_action_click') THEN session_id END) as actionable_sessions,
        MAX(created_at) as last_event_at
      FROM market_utility_events
      WHERE datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
    `).bind(lookbackExpr).first<{
      total_events: number | null;
      unique_sessions: number | null;
      plan_views: number | null;
      opportunities_views: number | null;
      decision_actionable_views: number | null;
      decision_watch_views: number | null;
      decision_no_action_views: number | null;
      no_action_unlock_views: number | null;
      cta_action_clicks: number | null;
      cta_action_sessions: number | null;
      actionable_view_sessions: number | null;
      actionable_sessions: number | null;
      last_event_at: string | null;
    }>(),
    db.prepare(`
      SELECT COUNT(DISTINCT substr(replace(created_at, 'T', ' '), 1, 10)) as days_observed
      FROM market_utility_events
      WHERE datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
    `).bind(lookbackExpr).first<{ days_observed: number | null }>(),
  ]);

  const totalEvents = Math.max(0, Math.floor(toNumber(aggregate?.total_events, 0)));
  const uniqueSessions = Math.max(0, Math.floor(toNumber(aggregate?.unique_sessions, 0)));
  const planViews = Math.max(0, Math.floor(toNumber(aggregate?.plan_views, 0)));
  const opportunitiesViews = Math.max(0, Math.floor(toNumber(aggregate?.opportunities_views, 0)));
  const decisionActionableViews = Math.max(0, Math.floor(toNumber(aggregate?.decision_actionable_views, 0)));
  const decisionWatchViews = Math.max(0, Math.floor(toNumber(aggregate?.decision_watch_views, 0)));
  const decisionNoActionViews = Math.max(0, Math.floor(toNumber(aggregate?.decision_no_action_views, 0)));
  const noActionUnlockViews = Math.max(0, Math.floor(toNumber(aggregate?.no_action_unlock_views, 0)));
  const ctaActionClicks = Math.max(0, Math.floor(toNumber(aggregate?.cta_action_clicks, 0)));
  const ctaActionSessions = Math.max(0, Math.floor(toNumber(aggregate?.cta_action_sessions, 0)));
  const actionableViewSessions = Math.max(0, Math.floor(toNumber(aggregate?.actionable_view_sessions, 0)));
  const actionableSessions = Math.max(0, Math.floor(toNumber(aggregate?.actionable_sessions, 0)));
  const decisionEventsTotal = decisionActionableViews + decisionWatchViews + decisionNoActionViews;
  const decisionEventsPerSession = uniqueSessions > 0
    ? Number((decisionEventsTotal / uniqueSessions).toFixed(4))
    : 0;
  const ctaActionRatePct = actionableSessions > 0
    ? Number(((ctaActionSessions / actionableSessions) * 100).toFixed(2))
    : 0;
  const noActionUnlockCoverage = decisionNoActionViews > 0
    ? Number(((noActionUnlockViews / decisionNoActionViews) * 100).toFixed(2))
    : 0;

  return {
    window_days: boundedWindowDays,
    days_observed: Math.max(0, Math.floor(toNumber(daysRow?.days_observed, 0))),
    total_events: totalEvents,
    unique_sessions: uniqueSessions,
    plan_views: planViews,
    opportunities_views: opportunitiesViews,
    decision_actionable_views: decisionActionableViews,
    decision_watch_views: decisionWatchViews,
    decision_no_action_views: decisionNoActionViews,
    no_action_unlock_views: noActionUnlockViews,
    cta_action_clicks: ctaActionClicks,
    actionable_view_sessions: actionableViewSessions,
    actionable_sessions: actionableSessions,
    cta_action_rate_pct: ctaActionRatePct,
    decision_events_total: decisionEventsTotal,
    decision_events_per_session: decisionEventsPerSession,
    no_action_unlock_coverage_pct: noActionUnlockCoverage,
    last_event_at: aggregate?.last_event_at || null,
  };
}

async function insertOpportunityLedgerRow(
  db: D1Database,
  payload: OpportunityLedgerInsertPayload,
): Promise<void> {
  await db.prepare(`
    INSERT INTO market_opportunity_ledger (
      refresh_run_id,
      as_of,
      horizon,
      candidate_count,
      published_count,
      suppressed_count,
      quality_filtered_count,
      coherence_suppressed_count,
      data_quality_suppressed_count,
      degraded_reason,
      top_direction_candidate,
      top_direction_published,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
  `).bind(
    payload.refresh_run_id,
    payload.as_of,
    payload.horizon,
    payload.candidate_count,
    payload.published_count,
    payload.suppressed_count,
    payload.quality_filtered_count,
    payload.coherence_suppressed_count,
    payload.data_quality_suppressed_count,
    payload.degraded_reason,
    payload.top_direction_candidate,
    payload.top_direction_published,
  ).run();
}

async function insertOpportunityItemLedgerRow(
  db: D1Database,
  payload: OpportunityItemLedgerInsertPayload,
): Promise<void> {
  await db.prepare(`
    INSERT OR REPLACE INTO market_opportunity_item_ledger (
      refresh_run_id,
      as_of,
      horizon,
      opportunity_id,
      theme_id,
      theme_name,
      direction,
      conviction_score,
      published,
      suppression_reason,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
  `).bind(
    payload.refresh_run_id,
    payload.as_of,
    payload.horizon,
    payload.opportunity_id,
    payload.theme_id,
    payload.theme_name,
    payload.direction,
    payload.conviction_score,
    payload.published,
    payload.suppression_reason,
  ).run();
}

interface OpportunityLedgerBuildResult {
  ledger_row: OpportunityLedgerInsertPayload;
  item_rows: OpportunityItemLedgerInsertPayload[];
  projected: OpportunityFeedProjection;
  normalized_items: OpportunityItem[];
}

function buildOpportunityLedgerProjection(args: {
  refresh_run_id: number | null;
  snapshot: OpportunitySnapshot;
  calibration: MarketCalibrationSnapshotPayload | null;
  coherence_gate_enabled: boolean;
  freshness: FreshnessStatus;
  consistency_state: ConsistencyState;
}): OpportunityLedgerBuildResult {
  const normalized = normalizeOpportunityItemsForPublishing(args.snapshot.items, args.calibration);
  const qualityGateResult = removeLowInformationOpportunities(normalized);
  const coherenceGateResult = applyOpportunityCoherenceGate(
    qualityGateResult.items,
    args.coherence_gate_enabled,
  );
  const projected = projectOpportunityFeed(normalized, {
    coherence_gate_enabled: args.coherence_gate_enabled,
    freshness: args.freshness,
    consistency_state: args.consistency_state,
  });
  const qualityRetainedIds = new Set(qualityGateResult.items.map((item) => item.id));
  const coherenceEligibleIds = new Set(coherenceGateResult.items.map((item) => item.id));
  const publishedIds = new Set(projected.items.map((item) => item.id));

  const itemRows: OpportunityItemLedgerInsertPayload[] = normalized.map((item) => {
    const isPublished = publishedIds.has(item.id);
    let suppressionReason: OpportunityItemLedgerInsertPayload['suppression_reason'] = null;
    if (!isPublished) {
      if (!qualityRetainedIds.has(item.id)) {
        suppressionReason = 'quality_filtered';
      } else if (!coherenceEligibleIds.has(item.id)) {
        suppressionReason = 'coherence_failed';
      } else if (projected.suppressed_data_quality || projected.degraded_reason === 'suppressed_data_quality') {
        suppressionReason = 'suppressed_data_quality';
      } else if (projected.degraded_reason === 'coherence_gate_failed') {
        suppressionReason = 'coherence_failed';
      } else if (projected.degraded_reason === 'quality_filtered') {
        suppressionReason = 'quality_filtered';
      }
    }

    return {
      refresh_run_id: args.refresh_run_id,
      as_of: args.snapshot.as_of,
      horizon: args.snapshot.horizon,
      opportunity_id: item.id,
      theme_id: item.theme_id,
      theme_name: item.theme_name,
      direction: item.direction,
      conviction_score: Math.max(0, Math.min(100, Math.round(toNumber(item.conviction_score, 0)))),
      published: isPublished ? 1 : 0,
      suppression_reason: suppressionReason,
    };
  });

  return {
    ledger_row: {
      refresh_run_id: args.refresh_run_id,
      as_of: args.snapshot.as_of,
      horizon: args.snapshot.horizon,
      candidate_count: projected.total_candidates,
      published_count: projected.items.length,
      suppressed_count: projected.suppressed_count,
      quality_filtered_count: projected.quality_filtered_count,
      coherence_suppressed_count: projected.coherence_suppressed_count,
      data_quality_suppressed_count: projected.suppression_by_reason.data_quality_suppressed,
      degraded_reason: projected.degraded_reason,
      top_direction_candidate: normalized[0]?.direction ?? null,
      top_direction_published: projected.items[0]?.direction ?? null,
    },
    item_rows: itemRows,
    projected,
    normalized_items: normalized,
  };
}

function roundMetric(value: number, digits = 4): number {
  if (!Number.isFinite(value)) return 0;
  return Number(value.toFixed(digits));
}

function decisionImpactQualityBand(sampleSize: number): CalibrationQuality {
  if (sampleSize >= 80) return 'ROBUST';
  if (sampleSize >= 30) return 'LIMITED';
  return 'INSUFFICIENT';
}

interface DecisionImpactObservation {
  theme_id: string;
  theme_name: string;
  as_of: string;
  forward_return_pct: number;
  signed_return_pct: number;
  basis_kind: 'spy_market' | 'theme_proxy' | 'spy_fallback';
}

interface IndicatorSeriesPoint {
  date: string;
  value: number;
}

function latestDateInSeries(series: IndicatorSeriesPoint[]): string | null {
  if (series.length <= 0) return null;
  return series[series.length - 1].date;
}

function priceOnOrAfterSeries(
  seriesMap: Map<string, number> | undefined,
  date: string,
  toleranceDays = 3,
): number | null {
  if (!seriesMap) return null;
  for (let offset = 0; offset <= toleranceDays; offset += 1) {
    const candidate = addCalendarDays(date, offset);
    const price = seriesMap.get(candidate);
    if (price !== undefined) return price;
  }
  return null;
}

function buildDecisionImpactMarketStats(
  observations: DecisionImpactObservation[],
): DecisionImpactMarketStats {
  const sampleSize = observations.length;
  if (sampleSize <= 0) {
    return {
      sample_size: 0,
      hit_rate: 0,
      avg_forward_return_pct: 0,
      avg_signed_return_pct: 0,
      win_rate: 0,
      downside_p10_pct: 0,
      max_loss_pct: 0,
      quality_band: 'INSUFFICIENT',
    };
  }

  const signed = observations.map((row) => row.signed_return_pct);
  const forward = observations.map((row) => row.forward_return_pct);
  const correctCount = signed.filter((value) => value > 0).length;
  const hitRate = correctCount / sampleSize;
  const avgForward = forward.reduce((acc, value) => acc + value, 0) / sampleSize;
  const avgSigned = signed.reduce((acc, value) => acc + value, 0) / sampleSize;
  const downsideP10 = quantile(signed, 0.1) ?? 0;
  const maxLoss = Math.min(...signed);

  return {
    sample_size: sampleSize,
    hit_rate: roundMetric(hitRate),
    avg_forward_return_pct: roundMetric(avgForward),
    avg_signed_return_pct: roundMetric(avgSigned),
    win_rate: roundMetric(hitRate),
    downside_p10_pct: roundMetric(downsideP10),
    max_loss_pct: roundMetric(maxLoss),
    quality_band: decisionImpactQualityBand(sampleSize),
  };
}

function buildDecisionImpactThemeStats(
  observations: DecisionImpactObservation[],
  limit: number,
): DecisionImpactThemeStats[] {
  const grouped = new Map<string, {
    theme_id: string;
    theme_name: string;
    last_as_of: string;
    forward: number[];
    signed: number[];
  }>();

  for (const row of observations) {
    const key = `${row.theme_id}::${row.theme_name}`;
    const current = grouped.get(key) || {
      theme_id: row.theme_id,
      theme_name: row.theme_name,
      last_as_of: row.as_of,
      forward: [],
      signed: [],
    };
    current.forward.push(row.forward_return_pct);
    current.signed.push(row.signed_return_pct);
    if (row.as_of > current.last_as_of) {
      current.last_as_of = row.as_of;
    }
    grouped.set(key, current);
  }

  const rows = [...grouped.values()].map((entry) => {
    const sampleSize = entry.signed.length;
    const correctCount = entry.signed.filter((value) => value > 0).length;
    const hitRate = sampleSize > 0 ? (correctCount / sampleSize) : 0;
    const avgSigned = sampleSize > 0
      ? entry.signed.reduce((acc, value) => acc + value, 0) / sampleSize
      : 0;
    const avgForward = sampleSize > 0
      ? entry.forward.reduce((acc, value) => acc + value, 0) / sampleSize
      : 0;

    return {
      theme_id: entry.theme_id,
      theme_name: entry.theme_name,
      sample_size: sampleSize,
      hit_rate: roundMetric(hitRate),
      avg_signed_return_pct: roundMetric(avgSigned),
      avg_forward_return_pct: roundMetric(avgForward),
      win_rate: roundMetric(hitRate),
      quality_band: decisionImpactQualityBand(sampleSize),
      last_as_of: entry.last_as_of,
    } as DecisionImpactThemeStats;
  });

  rows.sort((a, b) => {
    if (b.avg_signed_return_pct !== a.avg_signed_return_pct) {
      return b.avg_signed_return_pct - a.avg_signed_return_pct;
    }
    if (b.sample_size !== a.sample_size) {
      return b.sample_size - a.sample_size;
    }
    return a.theme_id.localeCompare(b.theme_id);
  });

  return rows.slice(0, Math.max(1, Math.min(50, Math.floor(limit))));
}

async function fetchDecisionImpactSnapshotAtOrBefore(
  db: D1Database,
  horizon: '7d' | '30d',
  scope: 'market' | 'theme',
  windowDays: 30 | 90,
  asOf?: string | null,
): Promise<DecisionImpactResponsePayload | null> {
  const asOfFilter = asOf && parseIsoDate(asOf) ? `${asOf.slice(0, 10)}T23:59:59.999Z` : null;
  let row: { payload_json: string } | null = null;
  if (asOfFilter) {
    row = await db.prepare(`
      SELECT payload_json
      FROM market_decision_impact_snapshots
      WHERE horizon = ?
        AND scope = ?
        AND window_days = ?
        AND as_of <= ?
      ORDER BY as_of DESC
      LIMIT 1
    `).bind(horizon, scope, windowDays, asOfFilter).first<{ payload_json: string }>();
  } else {
    row = await db.prepare(`
      SELECT payload_json
      FROM market_decision_impact_snapshots
      WHERE horizon = ?
        AND scope = ?
        AND window_days = ?
      ORDER BY as_of DESC
      LIMIT 1
    `).bind(horizon, scope, windowDays).first<{ payload_json: string }>();
  }

  if (!row?.payload_json) return null;
  try {
    const parsed = JSON.parse(row.payload_json) as DecisionImpactResponsePayload;
    if (!parsed || typeof parsed !== 'object') return null;
    if (parsed.horizon !== horizon || parsed.scope !== scope || parsed.window_days !== windowDays) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

async function storeDecisionImpactSnapshot(
  db: D1Database,
  payload: DecisionImpactResponsePayload,
): Promise<void> {
  await db.prepare(`
    INSERT OR REPLACE INTO market_decision_impact_snapshots (
      as_of,
      horizon,
      scope,
      window_days,
      payload_json,
      created_at
    ) VALUES (?, ?, ?, ?, ?, datetime('now'))
  `).bind(
    payload.as_of,
    payload.horizon,
    payload.scope,
    payload.window_days,
    JSON.stringify(payload),
  ).run();
}

async function computeDecisionImpact(
  db: D1Database,
  args: {
    horizon: '7d' | '30d';
    scope: 'market' | 'theme';
    window_days: 30 | 90;
    limit?: number;
    as_of?: string | null;
  },
): Promise<DecisionImpactResponsePayload> {
  const windowDays = DECISION_IMPACT_WINDOW_DAY_OPTIONS.has(args.window_days) ? args.window_days : 30;
  const asOfNow = asIsoDateTime(new Date());
  const requestedAsOf = String(args.as_of || '');
  const effectiveAsOfDate =
    parseIsoDate(requestedAsOf) ||
    parseIsoDate(requestedAsOf.slice(0, 10)) ||
    asIsoDate(new Date());
  const startDate = addCalendarDays(effectiveAsOfDate, -(windowDays - 1));
  const horizonDays = args.horizon === '7d' ? 7 : 30;

  const ledgerRows = await db.prepare(`
    SELECT as_of, theme_id, theme_name, direction
    FROM market_opportunity_item_ledger
    WHERE horizon = ?
      AND published = 1
      AND substr(as_of, 1, 10) >= ?
      AND substr(as_of, 1, 10) <= ?
    ORDER BY as_of DESC, id DESC
  `).bind(args.horizon, startDate, effectiveAsOfDate).all<{
    as_of: string;
    theme_id: string;
    theme_name: string;
    direction: string;
  }>();

  const proxyIndicatorIds = new Set<string>(['spy_close']);
  if (args.scope === 'theme') {
    for (const row of ledgerRows.results || []) {
      for (const rule of resolveThemeProxyRules(row.theme_id)) {
        proxyIndicatorIds.add(rule.indicator_id);
      }
    }
  }

  const indicatorIdList = [...proxyIndicatorIds];
  const placeholders = indicatorIdList.map(() => '?').join(',');
  const indicatorRows = await db.prepare(`
    SELECT indicator_id, date, value
    FROM indicator_values
    WHERE indicator_id IN (${placeholders})
    ORDER BY indicator_id ASC, date ASC
  `).bind(...indicatorIdList).all<{
    indicator_id: string;
    date: string;
    value: number;
  }>();

  const seriesByIndicator = new Map<string, IndicatorSeriesPoint[]>();
  for (const row of indicatorRows.results || []) {
    const bucket = seriesByIndicator.get(row.indicator_id) || [];
    bucket.push({ date: row.date, value: row.value });
    seriesByIndicator.set(row.indicator_id, bucket);
  }
  const seriesMapByIndicator = new Map<string, Map<string, number>>();
  const latestDateByIndicator = new Map<string, string>();
  for (const [indicatorId, series] of seriesByIndicator.entries()) {
    const map = new Map<string, number>();
    for (const point of series) {
      map.set(point.date, point.value);
    }
    seriesMapByIndicator.set(indicatorId, map);
    const latestDate = latestDateInSeries(series);
    if (latestDate) {
      latestDateByIndicator.set(indicatorId, latestDate <= effectiveAsOfDate ? latestDate : effectiveAsOfDate);
    }
  }

  const latestReferenceDate = (() => {
    const spyLatest = latestDateByIndicator.get('spy_close');
    if (spyLatest) {
      return spyLatest;
    }
    const dates = [...latestDateByIndicator.values()];
    if (dates.length <= 0) return null;
    return dates.sort((a, b) => a.localeCompare(b))[dates.length - 1];
  })();

  let maturedItems = 0;
  let eligibleItems = 0;
  let themeProxyEligibleItems = 0;
  let spyFallbackItems = 0;
  const observations: DecisionImpactObservation[] = [];

  for (const row of ledgerRows.results || []) {
    const direction = row.direction === 'bearish' || row.direction === 'bullish' || row.direction === 'neutral'
      ? row.direction
      : null;
    if (!direction) continue;

    const asOfDate = String(row.as_of || '').slice(0, 10);
    if (!parseIsoDate(asOfDate)) continue;

    if (!latestReferenceDate) {
      continue;
    }

    const maturityDate = addCalendarDays(asOfDate, horizonDays);
    if (maturityDate > latestReferenceDate) {
      continue;
    }
    maturedItems += 1;

    if (direction === 'neutral') {
      continue;
    }

    let forwardReturnPct: number | null = null;
    let basisKind: DecisionImpactObservation['basis_kind'] = 'spy_market';

    if (args.scope === 'theme') {
      const proxyRules = resolveThemeProxyRules(row.theme_id);
      let weightedReturn = 0;
      let totalWeight = 0;
      for (const rule of proxyRules) {
        const series = seriesMapByIndicator.get(rule.indicator_id);
        const spot = priceOnOrAfterSeries(series, asOfDate, 3);
        const forward = priceOnOrAfterSeries(series, maturityDate, 3);
        if (spot === null || forward === null || spot === 0) {
          continue;
        }
        const rawReturn = ((forward - spot) / spot) * 100;
        const orientedReturn = rule.invert ? -rawReturn : rawReturn;
        const weight = Number.isFinite(rule.weight) && rule.weight > 0 ? rule.weight : 1;
        weightedReturn += orientedReturn * weight;
        totalWeight += weight;
      }
      if (totalWeight > 0) {
        forwardReturnPct = weightedReturn / totalWeight;
        basisKind = 'theme_proxy';
        themeProxyEligibleItems += 1;
      } else {
        const spySeries = seriesMapByIndicator.get('spy_close');
        const spot = priceOnOrAfterSeries(spySeries, asOfDate, 3);
        const forward = priceOnOrAfterSeries(spySeries, maturityDate, 3);
        if (spot === null || forward === null || spot === 0) {
          continue;
        }
        forwardReturnPct = ((forward - spot) / spot) * 100;
        basisKind = 'spy_fallback';
        spyFallbackItems += 1;
      }
    } else {
      const spySeries = seriesMapByIndicator.get('spy_close');
      const spot = priceOnOrAfterSeries(spySeries, asOfDate, 3);
      const forward = priceOnOrAfterSeries(spySeries, maturityDate, 3);
      if (spot === null || forward === null || spot === 0) {
        continue;
      }
      forwardReturnPct = ((forward - spot) / spot) * 100;
      basisKind = 'spy_market';
    }

    if (forwardReturnPct === null) {
      continue;
    }

    const signedReturnPct = direction === 'bullish' ? forwardReturnPct : -forwardReturnPct;
    eligibleItems += 1;
    observations.push({
      theme_id: row.theme_id,
      theme_name: row.theme_name,
      as_of: row.as_of,
      forward_return_pct: forwardReturnPct,
      signed_return_pct: signedReturnPct,
      basis_kind: basisKind,
    });
  }

  const coverageRatio = maturedItems > 0 ? (eligibleItems / maturedItems) : 0;
  const insufficientReasons: string[] = [];
  if (!latestReferenceDate) {
    insufficientReasons.push('missing_spy_close_series');
  }
  if (maturedItems === 0) {
    insufficientReasons.push('no_matured_items');
  } else if (eligibleItems === 0) {
    insufficientReasons.push(args.scope === 'theme' ? 'no_eligible_items_with_proxy' : 'no_eligible_items_with_spy_proxy');
  } else if (coverageRatio < 0.6) {
    insufficientReasons.push(args.scope === 'theme' ? 'low_proxy_coverage' : 'low_spy_proxy_coverage');
  }
  if (args.scope === 'theme' && eligibleItems > 0) {
    if (themeProxyEligibleItems === 0) {
      insufficientReasons.push('theme_proxy_unavailable_using_spy_fallback');
    } else {
      const themeProxyCoverage = themeProxyEligibleItems / eligibleItems;
      if (themeProxyCoverage < 0.6) {
        insufficientReasons.push('low_theme_proxy_coverage');
      }
      if (spyFallbackItems > 0) {
        insufficientReasons.push('partial_theme_proxy_fallback');
      }
    }
  }

  const marketStats = buildDecisionImpactMarketStats(observations);
  const themeStats = args.scope === 'theme'
    ? buildDecisionImpactThemeStats(observations, args.limit ?? 10)
    : [];
  const asOfOutput = args.as_of
    ? asIsoDateTime(new Date(`${effectiveAsOfDate}T00:00:00.000Z`))
    : asOfNow;
  const outcomeBasis: DecisionImpactOutcomeBasis =
    args.scope === 'theme' && themeProxyEligibleItems > 0
      ? 'theme_proxy_blend'
      : 'spy_forward_proxy';

  return {
    as_of: asOfOutput,
    horizon: args.horizon,
    scope: args.scope,
    window_days: windowDays,
    outcome_basis: outcomeBasis,
    market: marketStats,
    themes: themeStats,
    coverage: {
      matured_items: maturedItems,
      eligible_items: eligibleItems,
      coverage_ratio: roundMetric(coverageRatio),
      insufficient_reasons: insufficientReasons,
      theme_proxy_eligible_items: args.scope === 'theme' ? themeProxyEligibleItems : undefined,
      spy_fallback_items: args.scope === 'theme' ? spyFallbackItems : undefined,
    },
  };
}

interface DecisionImpactGovernanceOptions {
  enforce_enabled: boolean;
  min_sample_size: number;
  min_actionable_sessions: number;
}

function evaluateDecisionImpactObserveMode(
  market7: DecisionImpactMarketStats,
  market30: DecisionImpactMarketStats,
  utilityFunnel: UtilityFunnelSummary,
  governance: DecisionImpactGovernanceOptions,
): DecisionImpactObserveSnapshot {
  const breaches: string[] = [];
  const enforceBreaches: string[] = [];

  if (market7.sample_size <= 0) {
    breaches.push('market_7d_insufficient_samples');
  } else {
    if (market7.hit_rate < DECISION_IMPACT_OBSERVE_THRESHOLDS.market_7d_hit_rate_min) {
      breaches.push('market_7d_hit_rate_breach');
    }
    if (market7.avg_signed_return_pct <= DECISION_IMPACT_OBSERVE_THRESHOLDS.market_7d_avg_signed_return_min) {
      breaches.push('market_7d_avg_signed_return_breach');
    }
  }
  if (market7.sample_size >= governance.min_sample_size) {
    if (market7.hit_rate < DECISION_IMPACT_OBSERVE_THRESHOLDS.market_7d_hit_rate_min) {
      enforceBreaches.push('market_7d_hit_rate_breach');
    }
    if (market7.avg_signed_return_pct <= DECISION_IMPACT_OBSERVE_THRESHOLDS.market_7d_avg_signed_return_min) {
      enforceBreaches.push('market_7d_avg_signed_return_breach');
    }
  }

  if (market30.sample_size <= 0) {
    breaches.push('market_30d_insufficient_samples');
  } else {
    if (market30.hit_rate < DECISION_IMPACT_OBSERVE_THRESHOLDS.market_30d_hit_rate_min) {
      breaches.push('market_30d_hit_rate_breach');
    }
    if (market30.avg_signed_return_pct <= DECISION_IMPACT_OBSERVE_THRESHOLDS.market_30d_avg_signed_return_min) {
      breaches.push('market_30d_avg_signed_return_breach');
    }
  }
  if (market30.sample_size >= governance.min_sample_size) {
    if (market30.hit_rate < DECISION_IMPACT_OBSERVE_THRESHOLDS.market_30d_hit_rate_min) {
      enforceBreaches.push('market_30d_hit_rate_breach');
    }
    if (market30.avg_signed_return_pct <= DECISION_IMPACT_OBSERVE_THRESHOLDS.market_30d_avg_signed_return_min) {
      enforceBreaches.push('market_30d_avg_signed_return_breach');
    }
  }

  if (utilityFunnel.actionable_sessions <= 0) {
    breaches.push('cta_action_insufficient_sessions');
  } else if (utilityFunnel.cta_action_rate_pct < DECISION_IMPACT_OBSERVE_THRESHOLDS.cta_action_rate_pct_min) {
    breaches.push('cta_action_rate_breach');
  }
  if (utilityFunnel.actionable_sessions >= governance.min_actionable_sessions) {
    if (utilityFunnel.cta_action_rate_pct < DECISION_IMPACT_OBSERVE_THRESHOLDS.cta_action_rate_pct_min) {
      enforceBreaches.push('cta_action_rate_breach');
    }
  }

  const enforceReady =
    market7.sample_size >= governance.min_sample_size &&
    market30.sample_size >= governance.min_sample_size &&
    utilityFunnel.actionable_sessions >= governance.min_actionable_sessions;

  return {
    enabled: true,
    mode: governance.enforce_enabled ? 'enforce' : 'observe',
    thresholds: {
      market_7d_hit_rate_min: DECISION_IMPACT_OBSERVE_THRESHOLDS.market_7d_hit_rate_min,
      market_30d_hit_rate_min: DECISION_IMPACT_OBSERVE_THRESHOLDS.market_30d_hit_rate_min,
      market_7d_avg_signed_return_min: DECISION_IMPACT_OBSERVE_THRESHOLDS.market_7d_avg_signed_return_min,
      market_30d_avg_signed_return_min: DECISION_IMPACT_OBSERVE_THRESHOLDS.market_30d_avg_signed_return_min,
      cta_action_rate_pct_min: DECISION_IMPACT_OBSERVE_THRESHOLDS.cta_action_rate_pct_min,
    },
    minimum_samples_required: governance.min_sample_size,
    minimum_actionable_sessions_required: governance.min_actionable_sessions,
    enforce_ready: enforceReady,
    enforce_breaches: Array.from(new Set(enforceBreaches)),
    enforce_breach_count: Array.from(new Set(enforceBreaches)).length,
    breaches: Array.from(new Set(breaches)),
    breach_count: Array.from(new Set(breaches)).length,
  };
}

async function buildDecisionImpactOpsResponse(
  db: D1Database,
  windowDays: 30 | 90,
  governance?: DecisionImpactGovernanceOptions,
): Promise<DecisionImpactOpsResponsePayload> {
  const appliedGovernance: DecisionImpactGovernanceOptions = governance || {
    enforce_enabled: false,
    min_sample_size: DECISION_IMPACT_ENFORCE_MIN_SAMPLE_DEFAULT,
    min_actionable_sessions: DECISION_IMPACT_ENFORCE_MIN_ACTIONABLE_SESSIONS_DEFAULT,
  };
  const [market7, market30, themes7, utilityFunnel] = await Promise.all([
    computeDecisionImpact(db, {
      horizon: '7d',
      scope: 'market',
      window_days: windowDays,
    }),
    computeDecisionImpact(db, {
      horizon: '30d',
      scope: 'market',
      window_days: windowDays,
    }),
    computeDecisionImpact(db, {
      horizon: '7d',
      scope: 'theme',
      window_days: windowDays,
      limit: 50,
    }),
    computeUtilityFunnelSummary(db, windowDays),
  ]);

  const themesWithSamples = themes7.themes.filter((theme) => theme.sample_size > 0);
  const topPositive = [...themesWithSamples].slice(0, 5);
  const topNegative = [...themesWithSamples]
    .sort((a, b) => {
      if (a.avg_signed_return_pct !== b.avg_signed_return_pct) {
        return a.avg_signed_return_pct - b.avg_signed_return_pct;
      }
      if (b.sample_size !== a.sample_size) {
        return b.sample_size - a.sample_size;
      }
      return a.theme_id.localeCompare(b.theme_id);
    })
    .slice(0, 5);
  const observeMode = evaluateDecisionImpactObserveMode(
    market7.market,
    market30.market,
    utilityFunnel,
    appliedGovernance,
  );

  return {
    as_of: asIsoDateTime(new Date()),
    window_days: windowDays,
    market_7d: market7.market,
    market_30d: market30.market,
    theme_summary: {
      themes_with_samples: themesWithSamples.length,
      themes_robust: themesWithSamples.filter((theme) => theme.quality_band === 'ROBUST').length,
      top_positive: topPositive,
      top_negative: topNegative,
    },
    utility_attribution: {
      actionable_views: utilityFunnel.decision_actionable_views,
      actionable_sessions: utilityFunnel.actionable_sessions,
      cta_action_clicks: utilityFunnel.cta_action_clicks,
      cta_action_rate_pct: utilityFunnel.cta_action_rate_pct,
      no_action_unlock_views: utilityFunnel.no_action_unlock_views,
      decision_events_total: utilityFunnel.decision_events_total,
    },
    observe_mode: observeMode,
  };
}

function calibrationQualityScore(quality: CalibrationQuality): number {
  if (quality === 'ROBUST') return 100;
  if (quality === 'LIMITED') return 70;
  return 35;
}

function decisionGradeFromScore(score: number): 'GREEN' | 'YELLOW' | 'RED' {
  if (score >= 85) return 'GREEN';
  if (score >= 70) return 'YELLOW';
  return 'RED';
}

async function computeOpportunityLedgerWindowMetrics(
  db: D1Database,
  windowDays: number,
): Promise<OpportunityLedgerWindowMetrics> {
  const boundedWindowDays = UTILITY_WINDOW_DAY_OPTIONS.has(windowDays) ? windowDays : 30;
  const lookbackExpr = `-${Math.max(0, boundedWindowDays - 1)} days`;
  const rows = await db.prepare(`
    SELECT
      as_of,
      horizon,
      candidate_count,
      published_count,
      suppressed_count,
      quality_filtered_count,
      coherence_suppressed_count,
      data_quality_suppressed_count,
      degraded_reason,
      top_direction_candidate,
      top_direction_published,
      created_at
    FROM market_opportunity_ledger
    WHERE datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
    ORDER BY as_of ASC, created_at ASC, id ASC
  `).bind(lookbackExpr).all<{
    as_of: string;
    horizon: string;
    candidate_count: number | null;
    published_count: number | null;
    suppressed_count: number | null;
    quality_filtered_count: number | null;
    coherence_suppressed_count: number | null;
    data_quality_suppressed_count: number | null;
    degraded_reason: string | null;
    top_direction_candidate: string | null;
    top_direction_published: string | null;
    created_at: string | null;
  }>();

  const latestByDateHorizon = new Map<string, OpportunityLedgerRow>();
  for (const row of rows.results || []) {
    const horizon = row.horizon === '30d' ? '30d' : row.horizon === '7d' ? '7d' : null;
    if (!horizon || !row.as_of) continue;
    const asOfDate = row.as_of.slice(0, 10);
    const key = `${asOfDate}:${horizon}`;
    latestByDateHorizon.set(key, {
      refresh_run_id: null,
      as_of: row.as_of,
      horizon,
      candidate_count: Math.max(0, Math.floor(toNumber(row.candidate_count, 0))),
      published_count: Math.max(0, Math.floor(toNumber(row.published_count, 0))),
      suppressed_count: Math.max(0, Math.floor(toNumber(row.suppressed_count, 0))),
      quality_filtered_count: Math.max(0, Math.floor(toNumber(row.quality_filtered_count, 0))),
      coherence_suppressed_count: Math.max(0, Math.floor(toNumber(row.coherence_suppressed_count, 0))),
      data_quality_suppressed_count: Math.max(0, Math.floor(toNumber(row.data_quality_suppressed_count, 0))),
      degraded_reason: row.degraded_reason || null,
      top_direction_candidate:
        row.top_direction_candidate === 'bullish' || row.top_direction_candidate === 'bearish' || row.top_direction_candidate === 'neutral'
          ? row.top_direction_candidate
          : null,
      top_direction_published:
        row.top_direction_published === 'bullish' || row.top_direction_published === 'bearish' || row.top_direction_published === 'neutral'
          ? row.top_direction_published
          : null,
      created_at: row.created_at || row.as_of,
    });
  }

  const normalizedRows = [...latestByDateHorizon.values()];
  const candidateCountTotal = normalizedRows.reduce((acc, row) => acc + row.candidate_count, 0);
  const publishedCountTotal = normalizedRows.reduce((acc, row) => acc + row.published_count, 0);
  const suppressedCountTotal = normalizedRows.reduce((acc, row) => acc + row.suppressed_count, 0);
  const overSuppressedRows = normalizedRows.filter((row) =>
    row.candidate_count > 0 &&
    row.published_count === 0 &&
    row.data_quality_suppressed_count === 0
  ).length;

  const byDate = new Map<string, { d7: OpportunityLedgerRow | null; d30: OpportunityLedgerRow | null }>();
  for (const row of normalizedRows) {
    const dateKey = row.as_of.slice(0, 10);
    const current = byDate.get(dateKey) || { d7: null, d30: null };
    if (row.horizon === '7d') current.d7 = row;
    if (row.horizon === '30d') current.d30 = row;
    byDate.set(dateKey, current);
  }

  const orderedDates = [...byDate.keys()].sort();
  let pairedDays = 0;
  let crossHorizonConflictDays = 0;
  let conflictPersistenceDays = 0;
  for (const dateKey of orderedDates) {
    const pair = byDate.get(dateKey);
    const isPaired = Boolean(pair?.d7 && pair?.d30);
    const hasDirectionalConflict = Boolean(
      pair?.d7 &&
      pair?.d30 &&
      pair.d7.published_count > 0 &&
      pair.d30.published_count > 0 &&
      pair.d7.top_direction_published &&
      pair.d30.top_direction_published &&
      pair.d7.top_direction_published !== pair.d30.top_direction_published
    );
    if (isPaired) {
      pairedDays += 1;
    }
    if (hasDirectionalConflict) {
      crossHorizonConflictDays += 1;
      conflictPersistenceDays += 1;
    } else {
      conflictPersistenceDays = 0;
    }
  }

  return {
    window_days: boundedWindowDays,
    rows_observed: normalizedRows.length,
    candidate_count_total: candidateCountTotal,
    published_count_total: publishedCountTotal,
    publish_rate_pct: candidateCountTotal > 0
      ? Number(((publishedCountTotal / candidateCountTotal) * 100).toFixed(2))
      : 0,
    suppressed_count_total: suppressedCountTotal,
    over_suppressed_rows: overSuppressedRows,
    over_suppression_rate_pct: normalizedRows.length > 0
      ? Number(((overSuppressedRows / normalizedRows.length) * 100).toFixed(2))
      : 0,
    paired_days: pairedDays,
    cross_horizon_conflict_days: crossHorizonConflictDays,
    cross_horizon_conflict_rate_pct: pairedDays > 0
      ? Number(((crossHorizonConflictDays / pairedDays) * 100).toFixed(2))
      : 0,
    conflict_persistence_days: conflictPersistenceDays,
    last_as_of: normalizedRows.length > 0
      ? [...normalizedRows].sort((a, b) => a.as_of.localeCompare(b.as_of))[normalizedRows.length - 1].as_of
      : null,
  };
}

async function computeDecisionGradeScorecard(
  db: D1Database,
  windowDays: number,
): Promise<DecisionGradeResponse> {
  const boundedWindowDays = UTILITY_WINDOW_DAY_OPTIONS.has(windowDays) ? windowDays : 30;
  const lookbackExpr = `-${Math.max(0, boundedWindowDays - 1)} days`;

  const [freshnessWindow, utilityFunnel, opportunityMetrics, consistencyRows, conviction7d, conviction30d, edgeCalibration] = await Promise.all([
    computeFreshnessSloWindow(db, boundedWindowDays),
    computeUtilityFunnelSummary(db, boundedWindowDays),
    computeOpportunityLedgerWindowMetrics(db, boundedWindowDays),
    db.prepare(`
      SELECT state, COUNT(*) as count
      FROM market_consistency_checks
      WHERE datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
      GROUP BY state
    `).bind(lookbackExpr).all<{ state: string; count: number | null }>(),
    fetchLatestCalibrationSnapshot(db, 'conviction', '7d'),
    fetchLatestCalibrationSnapshot(db, 'conviction', '30d'),
    fetchLatestCalibrationSnapshot(db, 'edge_quality', null),
  ]);

  let edgeReport: EdgeDiagnosticsReport | null = null;
  try {
    edgeReport = await buildEdgeDiagnosticsReport(db, ['7d', '30d']);
  } catch (err) {
    console.warn('Decision-grade edge diagnostics unavailable:', err);
    edgeReport = null;
  }

  const freshnessScore = Number(clamp(0, 100, freshnessWindow.slo_attainment_pct).toFixed(2));
  const freshnessStatus: DecisionGradeComponentStatus =
    freshnessWindow.days_observed <= 0
      ? 'insufficient'
      : freshnessWindow.days_with_critical_stale === 0 && freshnessWindow.slo_attainment_pct >= 95
        ? 'pass'
        : freshnessWindow.days_with_critical_stale <= 1 && freshnessWindow.slo_attainment_pct >= 90
          ? 'watch'
          : 'fail';

  let passCount = 0;
  let warnCount = 0;
  let failCount = 0;
  for (const row of consistencyRows.results || []) {
    const count = Math.max(0, Math.floor(toNumber(row.count, 0)));
    if (row.state === 'PASS') passCount += count;
    else if (row.state === 'WARN') warnCount += count;
    else if (row.state === 'FAIL') failCount += count;
  }
  const consistencyTotal = passCount + warnCount + failCount;
  const consistencyScore = consistencyTotal > 0
    ? Number(clamp(
      0,
      100,
      100 - ((failCount / consistencyTotal) * 100) - ((warnCount / consistencyTotal) * 35)
    ).toFixed(2))
    : 40;
  const consistencyStatus: DecisionGradeComponentStatus =
    consistencyTotal <= 0
      ? 'insufficient'
      : failCount > 0
        ? 'fail'
        : warnCount > 0
          ? 'watch'
          : 'pass';

  const conviction7dQuality = computeCalibrationDiagnostics(conviction7d).quality_band;
  const conviction30dQuality = computeCalibrationDiagnostics(conviction30d).quality_band;
  const edgeCalibrationQuality = computeCalibrationDiagnostics(edgeCalibration).quality_band;
  const calibrationScore = Number((
    (calibrationQualityScore(conviction7dQuality) +
      calibrationQualityScore(conviction30dQuality) +
      calibrationQualityScore(edgeCalibrationQuality)) / 3
  ).toFixed(2));
  const calibrationStatus: DecisionGradeComponentStatus =
    conviction7dQuality === 'INSUFFICIENT' || conviction30dQuality === 'INSUFFICIENT' || edgeCalibrationQuality === 'INSUFFICIENT'
      ? 'fail'
      : conviction7dQuality === 'LIMITED' || conviction30dQuality === 'LIMITED' || edgeCalibrationQuality === 'LIMITED'
        ? 'watch'
        : 'pass';

  let edgeScore = 35;
  let edgeStatus: DecisionGradeComponentStatus = 'insufficient';
  let lowerBoundPositiveHorizons = 0;
  let horizonsObserved = 0;
  let edgeReasons: string[] = [];
  if (edgeReport) {
    horizonsObserved = edgeReport.windows.length;
    lowerBoundPositiveHorizons = edgeReport.windows.filter((window) => window.lower_bound_positive).length;
    const leakageFailures = edgeReport.windows.filter((window) => !window.leakage_sentinel.pass).length;
    edgeScore = Number(clamp(
      0,
      100,
      50 +
      (edgeReport.promotion_gate.pass ? 25 : -20) +
      (lowerBoundPositiveHorizons * 12) -
      (leakageFailures * 10)
    ).toFixed(2));
    edgeReasons = edgeReport.promotion_gate.reasons;
    edgeStatus = edgeReport.promotion_gate.pass && lowerBoundPositiveHorizons > 0
      ? 'pass'
      : edgeReport.promotion_gate.pass
        ? 'watch'
        : 'fail';
  }

  let opportunityHygieneScore = 100;
  if (opportunityMetrics.publish_rate_pct < 10) opportunityHygieneScore -= 35;
  else if (opportunityMetrics.publish_rate_pct < 20) opportunityHygieneScore -= 20;
  if (opportunityMetrics.over_suppression_rate_pct > 35) opportunityHygieneScore -= 35;
  else if (opportunityMetrics.over_suppression_rate_pct > 20) opportunityHygieneScore -= 20;
  else if (opportunityMetrics.over_suppression_rate_pct > 10) opportunityHygieneScore -= 10;
  if (opportunityMetrics.cross_horizon_conflict_rate_pct > 50) opportunityHygieneScore -= 25;
  else if (opportunityMetrics.cross_horizon_conflict_rate_pct > 30) opportunityHygieneScore -= 15;
  else if (opportunityMetrics.cross_horizon_conflict_rate_pct > 15) opportunityHygieneScore -= 8;
  if (opportunityMetrics.conflict_persistence_days >= 3) opportunityHygieneScore -= 15;
  else if (opportunityMetrics.conflict_persistence_days >= 2) opportunityHygieneScore -= 8;
  const boundedOpportunityHygieneScore = Number(clamp(0, 100, opportunityHygieneScore).toFixed(2));
  const opportunityHygieneStatus: DecisionGradeComponentStatus =
    opportunityMetrics.rows_observed <= 0
      ? 'insufficient'
      : opportunityMetrics.over_suppression_rate_pct > 35 || opportunityMetrics.cross_horizon_conflict_rate_pct > 45
        ? 'fail'
        : opportunityMetrics.over_suppression_rate_pct > 20 || opportunityMetrics.cross_horizon_conflict_rate_pct > 30 || opportunityMetrics.publish_rate_pct < 10
          ? 'watch'
          : 'pass';

  const utilityTarget = boundedWindowDays === 30 ? 25 : 8;
  const utilityScoreRaw = Math.min(70, utilityFunnel.decision_events_total * 2) + (utilityFunnel.no_action_unlock_coverage_pct * 0.3);
  const utilityScore = Number(clamp(0, 100, utilityScoreRaw).toFixed(2));
  const utilityStatus: DecisionGradeComponentStatus =
    utilityFunnel.decision_events_total <= 0
      ? 'insufficient'
      : utilityFunnel.decision_events_total >= utilityTarget && utilityFunnel.no_action_unlock_coverage_pct >= 80
        ? 'pass'
        : utilityFunnel.decision_events_total >= Math.ceil(utilityTarget / 2) && utilityFunnel.no_action_unlock_coverage_pct >= 60
          ? 'watch'
          : 'fail';

  const weightedScore = Number((
    freshnessScore * 0.25 +
    consistencyScore * 0.20 +
    calibrationScore * 0.20 +
    edgeScore * 0.15 +
    boundedOpportunityHygieneScore * 0.15 +
    utilityScore * 0.05
  ).toFixed(2));
  const grade = decisionGradeFromScore(weightedScore);
  const goLiveReady = weightedScore >= 85
    && freshnessStatus === 'pass'
    && consistencyStatus !== 'fail'
    && calibrationStatus !== 'fail'
    && edgeStatus === 'pass'
    && opportunityHygieneStatus !== 'fail';

  return {
    as_of: asIsoDateTime(new Date()),
    window_days: boundedWindowDays,
    score: weightedScore,
    grade,
    go_live_ready: goLiveReady,
    components: {
      freshness: {
        score: freshnessScore,
        status: freshnessStatus,
        slo_attainment_pct: freshnessWindow.slo_attainment_pct,
        days_with_critical_stale: freshnessWindow.days_with_critical_stale,
        days_observed: freshnessWindow.days_observed,
      },
      consistency: {
        score: consistencyScore,
        status: consistencyStatus,
        pass_count: passCount,
        warn_count: warnCount,
        fail_count: failCount,
        total: consistencyTotal,
      },
      calibration: {
        score: calibrationScore,
        status: calibrationStatus,
        conviction_7d: conviction7dQuality,
        conviction_30d: conviction30dQuality,
        edge_quality: edgeCalibrationQuality,
      },
      edge: {
        score: edgeScore,
        status: edgeStatus,
        promotion_gate_pass: edgeReport?.promotion_gate.pass ?? false,
        lower_bound_positive_horizons: lowerBoundPositiveHorizons,
        horizons_observed: horizonsObserved,
        reasons: edgeReasons,
      },
      opportunity_hygiene: {
        score: boundedOpportunityHygieneScore,
        status: opportunityHygieneStatus,
        publish_rate_pct: opportunityMetrics.publish_rate_pct,
        over_suppression_rate_pct: opportunityMetrics.over_suppression_rate_pct,
        cross_horizon_conflict_rate_pct: opportunityMetrics.cross_horizon_conflict_rate_pct,
        conflict_persistence_days: opportunityMetrics.conflict_persistence_days,
        rows_observed: opportunityMetrics.rows_observed,
      },
      utility: {
        score: utilityScore,
        status: utilityStatus,
        decision_events_total: utilityFunnel.decision_events_total,
        no_action_unlock_coverage_pct: utilityFunnel.no_action_unlock_coverage_pct,
        unique_sessions: utilityFunnel.unique_sessions,
      },
    },
  };
}

async function fetchLatestMarketProductSnapshotWrite(db: D1Database): Promise<string | null> {
  try {
    const row = await db.prepare(`
      SELECT MAX(created_at) as latest_created_at
      FROM (
        SELECT created_at FROM market_brief_snapshots
        UNION ALL
        SELECT created_at FROM opportunity_snapshots
        UNION ALL
        SELECT created_at FROM market_calibration_snapshots
      )
    `).first<{ latest_created_at: string | null }>();
    return row?.latest_created_at || null;
  } catch {
    return null;
  }
}

async function resolveLatestRefreshTimestamp(db: D1Database): Promise<{
  last_refresh_at_utc: string | null;
  source: 'market_refresh_runs' | 'market_product_snapshots' | 'fetch_logs' | 'unknown';
}> {
  const [latestRefreshRun, latestProductSnapshotWrite, lastFetchLogRow] = await Promise.all([
    fetchLatestSuccessfulMarketRefreshRun(db),
    fetchLatestMarketProductSnapshotWrite(db),
    db.prepare(`
      SELECT MAX(completed_at) as last_refresh_at
      FROM fetch_logs
      WHERE completed_at IS NOT NULL
    `).first<{ last_refresh_at: string | null }>(),
  ]);

  if (latestRefreshRun?.completed_at) {
    return {
      last_refresh_at_utc: latestRefreshRun.completed_at,
      source: 'market_refresh_runs',
    };
  }
  if (latestProductSnapshotWrite) {
    return {
      last_refresh_at_utc: latestProductSnapshotWrite,
      source: 'market_product_snapshots',
    };
  }
  if (lastFetchLogRow?.last_refresh_at) {
    return {
      last_refresh_at_utc: lastFetchLogRow.last_refresh_at,
      source: 'fetch_logs',
    };
  }
  return {
    last_refresh_at_utc: null,
    source: 'unknown',
  };
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
    const existing = await db.prepare(`
      SELECT id
      FROM market_alert_events
      WHERE dedupe_key = ?
      LIMIT 1
    `).bind(event.dedupe_key).first<{ id: string }>();

    const eventId = existing?.id || event.id;
    const isNew = !existing?.id;

    await db.prepare(`
      INSERT INTO market_alert_events
      (id, event_type, severity, title, body, entity_type, entity_id, dedupe_key, payload_json, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(dedupe_key) DO UPDATE SET
        event_type = excluded.event_type,
        severity = excluded.severity,
        title = excluded.title,
        body = excluded.body,
        entity_type = excluded.entity_type,
        entity_id = excluded.entity_id,
        payload_json = excluded.payload_json,
        created_at = excluded.created_at
    `).bind(
      eventId,
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

    if (isNew) {
      inserted += 1;
      if (inAppEnabled) {
        await db.prepare(`
          INSERT INTO market_alert_deliveries (event_id, channel, status, attempted_at)
          VALUES (?, 'in_app', 'sent', datetime('now'))
        `).bind(eventId).run();
      }
    }
  }
  return inserted;
}

function canSendEmail(env: Env): boolean {
  return Boolean(env.EMAIL_OUTBOUND);
}

function resolveFromAddress(env: Env): string {
  return (env.ALERTS_FROM_EMAIL || 'alerts@pxicommand.com').trim();
}

function buildMimeMessage(payload: { from: string; to: string; subject: string; html: string; text: string }): string {
  const boundary = `pxi-${stableHash(`${payload.to}:${payload.subject}:${Date.now()}`)}`;
  return [
    `From: ${payload.from}`,
    `To: ${payload.to}`,
    `Subject: ${payload.subject}`,
    'MIME-Version: 1.0',
    `Content-Type: multipart/alternative; boundary="${boundary}"`,
    '',
    `--${boundary}`,
    'Content-Type: text/plain; charset=UTF-8',
    'Content-Transfer-Encoding: 8bit',
    '',
    payload.text,
    '',
    `--${boundary}`,
    'Content-Type: text/html; charset=UTF-8',
    'Content-Transfer-Encoding: 8bit',
    '',
    payload.html,
    '',
    `--${boundary}--`,
    '',
  ].join('\r\n');
}

async function sendCloudflareEmail(
  env: Env,
  payload: { to: string; subject: string; html: string; text: string }
): Promise<{ ok: boolean; providerId?: string; error?: string }> {
  if (!env.EMAIL_OUTBOUND) {
    return { ok: false, error: 'Cloudflare email binding not configured' };
  }

  try {
    const from = resolveFromAddress(env);
    const rawMime = buildMimeMessage({
      from,
      to: payload.to,
      subject: payload.subject,
      html: payload.html,
      text: payload.text,
    });
    const message = new EmailMessage(from, payload.to, rawMime);
    await env.EMAIL_OUTBOUND.send(message);
    const providerId = `cf-email:${stableHash(`${from}:${payload.to}:${payload.subject}`)}`;
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
  return sendCloudflareEmail(env, { to: email, subject, html, text });
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
              contract_version TEXT NOT NULL DEFAULT '${BRIEF_CONTRACT_VERSION}',
              payload_json TEXT NOT NULL,
              created_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          try {
            await env.DB.prepare(
              `ALTER TABLE market_brief_snapshots ADD COLUMN contract_version TEXT NOT NULL DEFAULT '${BRIEF_CONTRACT_VERSION}'`
            ).run();
          } catch (e) { /* Column already exists */ }
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
            CREATE TABLE IF NOT EXISTS market_opportunity_ledger (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              refresh_run_id INTEGER,
              as_of TEXT NOT NULL,
              horizon TEXT NOT NULL CHECK(horizon IN ('7d', '30d')),
              candidate_count INTEGER NOT NULL DEFAULT 0,
              published_count INTEGER NOT NULL DEFAULT 0,
              suppressed_count INTEGER NOT NULL DEFAULT 0,
              quality_filtered_count INTEGER NOT NULL DEFAULT 0,
              coherence_suppressed_count INTEGER NOT NULL DEFAULT 0,
              data_quality_suppressed_count INTEGER NOT NULL DEFAULT 0,
              degraded_reason TEXT,
              top_direction_candidate TEXT CHECK(top_direction_candidate IN ('bullish', 'bearish', 'neutral')),
              top_direction_published TEXT CHECK(top_direction_published IN ('bullish', 'bearish', 'neutral')),
              created_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_created ON market_opportunity_ledger(created_at DESC)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_as_of ON market_opportunity_ledger(as_of DESC, horizon)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_run ON market_opportunity_ledger(refresh_run_id, horizon)`).run();
          migrations.push('market_opportunity_ledger');
        } catch (e) {
          console.error('market_opportunity_ledger migration failed:', e);
        }

        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS market_opportunity_item_ledger (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              refresh_run_id INTEGER,
              as_of TEXT NOT NULL,
              horizon TEXT NOT NULL CHECK(horizon IN ('7d', '30d')),
              opportunity_id TEXT NOT NULL,
              theme_id TEXT NOT NULL,
              theme_name TEXT NOT NULL,
              direction TEXT NOT NULL CHECK(direction IN ('bullish', 'bearish', 'neutral')),
              conviction_score INTEGER NOT NULL,
              published INTEGER NOT NULL,
              suppression_reason TEXT,
              created_at TEXT DEFAULT (datetime('now')),
              UNIQUE(as_of, horizon, opportunity_id)
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_asof_horizon ON market_opportunity_item_ledger(as_of DESC, horizon)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_theme_horizon_asof ON market_opportunity_item_ledger(theme_id, horizon, as_of DESC)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_published_created ON market_opportunity_item_ledger(published, created_at DESC)`).run();
          migrations.push('market_opportunity_item_ledger');
        } catch (e) {
          console.error('market_opportunity_item_ledger migration failed:', e);
        }

        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS market_decision_impact_snapshots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              as_of TEXT NOT NULL,
              horizon TEXT NOT NULL CHECK(horizon IN ('7d', '30d')),
              scope TEXT NOT NULL CHECK(scope IN ('market', 'theme')),
              window_days INTEGER NOT NULL,
              payload_json TEXT NOT NULL,
              created_at TEXT DEFAULT (datetime('now')),
              UNIQUE(as_of, horizon, scope, window_days)
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_decision_impact_lookup ON market_decision_impact_snapshots(scope, horizon, window_days, as_of DESC)`).run();
          migrations.push('market_decision_impact_snapshots');
        } catch (e) {
          console.error('market_decision_impact_snapshots migration failed:', e);
        }

        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS market_calibration_snapshots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              as_of TEXT NOT NULL,
              metric TEXT NOT NULL,
              horizon TEXT,
              payload_json TEXT NOT NULL,
              created_at TEXT DEFAULT (datetime('now')),
              UNIQUE(as_of, metric, horizon)
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_calibration_lookup ON market_calibration_snapshots(metric, horizon, as_of DESC)`).run();
          migrations.push('market_calibration_snapshots');
        } catch (e) {
          console.error('market_calibration_snapshots migration failed:', e);
        }

        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS market_consistency_checks (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              as_of TEXT NOT NULL UNIQUE,
              score REAL NOT NULL,
              state TEXT NOT NULL CHECK(state IN ('PASS', 'WARN', 'FAIL')),
              violations_json TEXT NOT NULL,
              components_json TEXT NOT NULL DEFAULT '{}',
              created_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          try {
            await env.DB.prepare(
              `ALTER TABLE market_consistency_checks ADD COLUMN components_json TEXT NOT NULL DEFAULT '{}'`
            ).run();
          } catch (e) { /* Column already exists */ }
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_consistency_created ON market_consistency_checks(created_at DESC)`).run();
          migrations.push('market_consistency_checks');
        } catch (e) {
          console.error('market_consistency_checks migration failed:', e);
        }

        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS market_refresh_runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              started_at TEXT NOT NULL,
              completed_at TEXT,
              status TEXT NOT NULL CHECK(status IN ('running', 'success', 'failed')),
              "trigger" TEXT NOT NULL DEFAULT 'unknown',
              brief_generated INTEGER DEFAULT 0,
              opportunities_generated INTEGER DEFAULT 0,
              calibrations_generated INTEGER DEFAULT 0,
              alerts_generated INTEGER DEFAULT 0,
              stale_count INTEGER,
              critical_stale_count INTEGER,
              as_of TEXT,
              error TEXT,
              created_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          try {
            await env.DB.prepare(
              `ALTER TABLE market_refresh_runs ADD COLUMN critical_stale_count INTEGER`
            ).run();
          } catch (e) { /* Column already exists */ }
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_refresh_runs_completed ON market_refresh_runs(status, completed_at DESC)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_refresh_runs_created ON market_refresh_runs(created_at DESC)`).run();
          migrations.push('market_refresh_runs');
        } catch (e) {
          console.error('market_refresh_runs migration failed:', e);
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

        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS market_utility_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              session_id TEXT NOT NULL,
              event_type TEXT NOT NULL,
              route TEXT,
              actionability_state TEXT,
              payload_json TEXT,
              created_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_utility_events_created ON market_utility_events(created_at DESC)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_utility_events_type ON market_utility_events(event_type, created_at DESC)`).run();
          await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_utility_events_session ON market_utility_events(session_id, created_at DESC)`).run();
          migrations.push('market_utility_events');
        } catch (e) {
          console.error('market_utility_events migration failed:', e);
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

        // v1.4: Check data freshness - SLA-driven, monitored indicator set only.
        const freshnessDiagnostics = await computeFreshnessDiagnostics(env.DB);
        const staleIndicators = freshnessDiagnostics.stale_indicators;
        const hasStaleData = freshnessDiagnostics.status.has_stale_data;
        const topOffenders = staleIndicators
          .map((stale) => {
            const stalePolicy = resolveStalePolicy(
              stale.indicator_id,
              INDICATOR_FREQUENCY_HINTS.get(stale.indicator_id) ?? null
            );
            return {
              id: stale.indicator_id,
              status: stale.status,
              critical: stale.critical,
              lastUpdate: stale.latest_date,
              daysOld: stale.days_old === null || !Number.isFinite(stale.days_old) ? null : Math.round(stale.days_old),
              maxAgeDays: stale.max_age_days,
              chronic: isChronicStaleness(stale.days_old, stale.max_age_days),
              owner: stalePolicy.owner,
              escalation: stalePolicy.escalation,
              priorityScore: (stale.critical ? 1000 : 0) +
                (stale.status === 'missing' ? 500 : 0) +
                (stale.days_old === null || !Number.isFinite(stale.days_old) ? 250 : stale.days_old - stale.max_age_days),
            };
          })
          .sort((a, b) => b.priorityScore - a.priorityScore)
          .slice(0, 3)
          .map(({ priorityScore: _priorityScore, ...offender }) => offender);

        const latestRefresh = await resolveLatestRefreshTimestamp(env.DB);
        const lastRefreshAtUtc = latestRefresh.last_refresh_at_utc;
        const lastRefreshSource = latestRefresh.source;
        const nextRefresh = computeNextExpectedRefresh(new Date());

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
            staleCount: freshnessDiagnostics.status.stale_count,
            criticalStaleCount: freshnessDiagnostics.status.critical_stale_count,
            staleIndicators: hasStaleData ? staleIndicators.slice(0, 5).map(s => ({
              id: s.indicator_id,
              status: s.status,
              critical: s.critical,
              lastUpdate: s.latest_date,
              daysOld: s.days_old === null || !Number.isFinite(s.days_old) ? null : Math.round(s.days_old),
              maxAgeDays: s.max_age_days,
            })) : [],
            topOffenders,
            lastRefreshAtUtc,
            lastRefreshSource,
            nextExpectedRefreshAtUtc: nextRefresh.at,
            nextExpectedRefreshInMinutes: nextRefresh.in_minutes,
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

        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Brief schema guard failed:', err);
          return Response.json(buildBriefFallbackSnapshot('migration_guard_failed'), {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'no-store',
            },
          });
        }

        let snapshot: BriefSnapshot | null = null;
        let stored: { payload_json: string } | null = null;
        try {
          stored = await env.DB.prepare(`
            SELECT payload_json
            FROM market_brief_snapshots
            ORDER BY as_of DESC
            LIMIT 1
          `).first<{ payload_json: string }>();
        } catch (err) {
          console.error('Brief snapshot lookup failed:', err);
          stored = null;
        }

        if (stored?.payload_json) {
          try {
            snapshot = JSON.parse(stored.payload_json) as BriefSnapshot;
          } catch {
            snapshot = null;
          }
        }

        if (snapshot && !isBriefSnapshotCompatible(snapshot)) {
          snapshot = null;
        }

        if (snapshot) {
          const latestDateRow = await env.DB.prepare(`
            SELECT date
            FROM pxi_scores
            ORDER BY date DESC
            LIMIT 1
          `).first<{ date: string }>();
          const expectedPlanAsOf = latestDateRow?.date ? `${latestDateRow.date}T00:00:00.000Z` : null;
          if (expectedPlanAsOf && snapshot.source_plan_as_of !== expectedPlanAsOf) {
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
          try {
            await storeBriefSnapshot(env.DB, snapshot);
          } catch (err) {
            console.error('Brief snapshot store failed:', err);
          }
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
        const coherenceGateEnabled = isFeatureEnabled(
          env,
          'FEATURE_ENABLE_OPPORTUNITY_COHERENCE_GATE',
          'ENABLE_OPPORTUNITY_COHERENCE_GATE',
          true,
        );
        const signalsSanitizerEnabled = isFeatureEnabled(
          env,
          'FEATURE_ENABLE_SIGNALS_SANITIZER',
          'ENABLE_SIGNALS_SANITIZER',
          true,
        );
        const unknownTtlMetadata: OpportunityTtlMetadata = {
          data_age_seconds: null,
          ttl_state: 'unknown',
          next_expected_refresh_at: null,
          overdue_seconds: null,
        };

        if (!isFeatureEnabled(env, 'FEATURE_ENABLE_OPPORTUNITIES', 'ENABLE_OPPORTUNITIES', true)) {
          const fallback = buildOpportunityFallbackSnapshot(horizon, 'feature_disabled');
          return Response.json({
            as_of: fallback.as_of,
            horizon: fallback.horizon,
            items: fallback.items,
            suppressed_count: 0,
            quality_filtered_count: 0,
            coherence_suppressed_count: 0,
            suppression_by_reason: {
              coherence_failed: 0,
              quality_filtered: 0,
              data_quality_suppressed: 0,
            },
            quality_filter_rate: 0,
            coherence_fail_rate: 0,
            degraded_reason: fallback.degraded_reason,
            actionability_state: 'NO_ACTION',
            actionability_reason_codes: ['no_eligible_opportunities', `opportunity_${fallback.degraded_reason}`],
            cta_enabled: false,
            cta_disabled_reasons: ['no_eligible_opportunities'],
            data_age_seconds: unknownTtlMetadata.data_age_seconds,
            ttl_state: unknownTtlMetadata.ttl_state,
            next_expected_refresh_at: unknownTtlMetadata.next_expected_refresh_at,
            overdue_seconds: unknownTtlMetadata.overdue_seconds,
          }, {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'no-store',
            },
          });
        }

        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Opportunities schema guard failed:', err);
          const fallback = buildOpportunityFallbackSnapshot(horizon, 'migration_guard_failed');
          return Response.json({
            as_of: fallback.as_of,
            horizon: fallback.horizon,
            items: fallback.items,
            suppressed_count: 0,
            quality_filtered_count: 0,
            coherence_suppressed_count: 0,
            suppression_by_reason: {
              coherence_failed: 0,
              quality_filtered: 0,
              data_quality_suppressed: 0,
            },
            quality_filter_rate: 0,
            coherence_fail_rate: 0,
            degraded_reason: fallback.degraded_reason,
            actionability_state: 'NO_ACTION',
            actionability_reason_codes: ['no_eligible_opportunities', `opportunity_${fallback.degraded_reason}`],
            cta_enabled: false,
            cta_disabled_reasons: ['no_eligible_opportunities'],
            data_age_seconds: unknownTtlMetadata.data_age_seconds,
            ttl_state: unknownTtlMetadata.ttl_state,
            next_expected_refresh_at: unknownTtlMetadata.next_expected_refresh_at,
            overdue_seconds: unknownTtlMetadata.overdue_seconds,
          }, {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'no-store',
            },
          });
        }

        let snapshot: OpportunitySnapshot | null = null;
        let stored: { payload_json: string } | null = null;
        try {
          stored = await env.DB.prepare(`
            SELECT payload_json
            FROM opportunity_snapshots
            WHERE horizon = ?
            ORDER BY as_of DESC
            LIMIT 1
          `).bind(horizon).first<{ payload_json: string }>();
        } catch (err) {
          console.error('Opportunity snapshot lookup failed:', err);
          stored = null;
        }

        if (stored?.payload_json) {
          try {
            snapshot = JSON.parse(stored.payload_json) as OpportunitySnapshot;
          } catch {
            snapshot = null;
          }
        }

        if (!snapshot) {
          try {
            snapshot = await buildOpportunitySnapshot(env.DB, horizon, undefined, {
              sanitize_signals_tickers: signalsSanitizerEnabled,
            });
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
              suppressed_count: 0,
              quality_filtered_count: 0,
              coherence_suppressed_count: 0,
              suppression_by_reason: {
                coherence_failed: 0,
                quality_filtered: 0,
                data_quality_suppressed: 0,
              },
              quality_filter_rate: 0,
              coherence_fail_rate: 0,
              degraded_reason: fallback.degraded_reason,
              actionability_state: 'NO_ACTION',
              actionability_reason_codes: ['no_eligible_opportunities', `opportunity_${fallback.degraded_reason}`],
              cta_enabled: false,
              cta_disabled_reasons: ['no_eligible_opportunities'],
              data_age_seconds: unknownTtlMetadata.data_age_seconds,
              ttl_state: unknownTtlMetadata.ttl_state,
              next_expected_refresh_at: unknownTtlMetadata.next_expected_refresh_at,
              overdue_seconds: unknownTtlMetadata.overdue_seconds,
            }, {
              headers: {
                ...corsHeaders,
                'Cache-Control': 'no-store',
              },
            });
          }
          try {
            await storeOpportunitySnapshot(env.DB, snapshot);
          } catch (err) {
            console.error('Opportunity snapshot store failed:', err);
          }
        }

        const [convictionCalibration, freshness, latestConsistencyCheck, latestRefresh] = await Promise.all([
          fetchLatestCalibrationSnapshot(env.DB, 'conviction', horizon),
          computeFreshnessStatus(env.DB),
          fetchLatestConsistencyCheck(env.DB),
          resolveLatestRefreshTimestamp(env.DB),
        ]);

        let consistencyState: ConsistencyState = latestConsistencyCheck?.state ?? 'PASS';
        if (!latestConsistencyCheck) {
          try {
            consistencyState = (await buildCanonicalMarketDecision(env.DB)).consistency.state;
          } catch {
            consistencyState = 'PASS';
          }
        }

        const normalizedItems = normalizeOpportunityItemsForPublishing(snapshot.items, convictionCalibration);
        const projectedFeed = projectOpportunityFeed(normalizedItems, {
          coherence_gate_enabled: coherenceGateEnabled,
          freshness,
          consistency_state: consistencyState,
        });
        const ttlMetadata = computeOpportunityTtlMetadata(latestRefresh.last_refresh_at_utc, new Date());
        let effectiveDegradedReason = projectedFeed.degraded_reason;
        if (!effectiveDegradedReason && ttlMetadata.ttl_state === 'overdue') {
          effectiveDegradedReason = 'refresh_ttl_overdue';
        } else if (!effectiveDegradedReason && ttlMetadata.ttl_state === 'unknown') {
          effectiveDegradedReason = 'refresh_ttl_unknown';
        }
        const diagnostics = computeCalibrationDiagnostics(convictionCalibration);
        const ctaState = evaluateOpportunityCtaState(projectedFeed, diagnostics, ttlMetadata, effectiveDegradedReason);
        const actionabilityReasonCodes = Array.from(new Set([
          ...(projectedFeed.items.length === 0 ? ['no_eligible_opportunities'] : []),
          ...(effectiveDegradedReason ? [`opportunity_${effectiveDegradedReason}`] : []),
          ...(ctaState.actionability_state === 'WATCH' ? ['watch_state'] : []),
          ...(ctaState.actionability_state === 'ACTIONABLE' ? ['eligible_opportunities_available'] : []),
          ...ctaState.cta_disabled_reasons.map((reason) => `cta_${reason}`),
        ]));
        const responseItems = projectedFeed.items.slice(0, limit);
        const cacheControl = ttlMetadata.ttl_state === 'overdue' || ttlMetadata.ttl_state === 'unknown'
          ? 'no-store'
          : 'public, max-age=60';

        return Response.json({
          as_of: snapshot.as_of,
          horizon: snapshot.horizon,
          items: responseItems,
          suppressed_count: projectedFeed.suppressed_count,
          quality_filtered_count: projectedFeed.quality_filtered_count,
          coherence_suppressed_count: projectedFeed.coherence_suppressed_count,
          suppression_by_reason: projectedFeed.suppression_by_reason,
          quality_filter_rate: projectedFeed.quality_filter_rate,
          coherence_fail_rate: projectedFeed.coherence_fail_rate,
          degraded_reason: effectiveDegradedReason,
          actionability_state: ctaState.actionability_state,
          actionability_reason_codes: actionabilityReasonCodes,
          cta_enabled: ctaState.cta_enabled,
          cta_disabled_reasons: ctaState.cta_disabled_reasons,
          data_age_seconds: ttlMetadata.data_age_seconds,
          ttl_state: ttlMetadata.ttl_state,
          next_expected_refresh_at: ttlMetadata.next_expected_refresh_at,
          overdue_seconds: ttlMetadata.overdue_seconds,
        }, {
          headers: {
            ...corsHeaders,
            'Cache-Control': cacheControl,
          },
        });
      }

      if (url.pathname === '/api/decision-impact' && method === 'GET') {
        if (!isFeatureEnabled(env, 'FEATURE_ENABLE_DECISION_IMPACT', 'ENABLE_DECISION_IMPACT', true)) {
          return Response.json({ error: 'Decision impact disabled' }, { status: 404, headers: corsHeaders });
        }

        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Decision-impact schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
        }

        const horizonRaw = (url.searchParams.get('horizon') || '7d').trim().toLowerCase();
        if (horizonRaw !== '7d' && horizonRaw !== '30d') {
          return Response.json({
            error: 'Invalid horizon. Use horizon=7d or horizon=30d',
          }, { status: 400, headers: corsHeaders });
        }
        const horizon = horizonRaw as '7d' | '30d';

        const scopeRaw = (url.searchParams.get('scope') || 'market').trim().toLowerCase();
        if (scopeRaw !== 'market' && scopeRaw !== 'theme') {
          return Response.json({
            error: 'Invalid scope. Use scope=market or scope=theme',
          }, { status: 400, headers: corsHeaders });
        }
        const scope = scopeRaw as 'market' | 'theme';

        const windowRaw = Number.parseInt((url.searchParams.get('window') || '30').trim(), 10);
        if (!DECISION_IMPACT_WINDOW_DAY_OPTIONS.has(windowRaw)) {
          return Response.json({
            error: 'Invalid window. Supported values: 30, 90',
          }, { status: 400, headers: corsHeaders });
        }
        const windowDays = windowRaw as 30 | 90;
        const limitRaw = Number.parseInt((url.searchParams.get('limit') || '10').trim(), 10);
        const limit = Math.max(1, Math.min(50, Number.isFinite(limitRaw) ? limitRaw : 10));

        const asOfRaw = (url.searchParams.get('as_of') || '').trim();
        let asOfDate: string | null = null;
        if (asOfRaw) {
          asOfDate = parseIsoDate(asOfRaw) || parseIsoDate(asOfRaw.slice(0, 10));
          if (!asOfDate) {
            return Response.json({
              error: 'Invalid as_of. Use YYYY-MM-DD or ISO date-time.',
            }, { status: 400, headers: corsHeaders });
          }
        }

        let payload = await fetchDecisionImpactSnapshotAtOrBefore(
          env.DB,
          horizon,
          scope,
          windowDays,
          asOfDate,
        );

        if (!payload) {
          payload = await computeDecisionImpact(env.DB, {
            horizon,
            scope,
            window_days: windowDays,
            limit,
          });
        }

        const responsePayload: DecisionImpactResponsePayload = {
          ...payload,
          scope,
          horizon,
          window_days: windowDays,
          themes: scope === 'theme'
            ? (payload.themes || []).slice(0, limit)
            : [],
        };

        return Response.json(responsePayload, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'public, max-age=120',
          },
        });
      }

      if (url.pathname === '/api/ops/decision-impact' && method === 'GET') {
        if (!isFeatureEnabled(env, 'FEATURE_ENABLE_DECISION_IMPACT', 'ENABLE_DECISION_IMPACT', true)) {
          return Response.json({ error: 'Decision impact disabled' }, { status: 404, headers: corsHeaders });
        }

        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Ops decision-impact schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
        }

        const windowRaw = Number.parseInt((url.searchParams.get('window') || '30').trim(), 10);
        if (!DECISION_IMPACT_WINDOW_DAY_OPTIONS.has(windowRaw)) {
          return Response.json({
            error: 'Invalid window. Supported values: 30, 90',
          }, { status: 400, headers: corsHeaders });
        }
        const windowDays = windowRaw as 30 | 90;
        const decisionImpactGovernance = resolveDecisionImpactGovernance(env);

        try {
          const payload = await buildDecisionImpactOpsResponse(env.DB, windowDays, decisionImpactGovernance);
          return Response.json(payload, {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'public, max-age=120',
            },
          });
        } catch (err) {
          console.error('Ops decision-impact computation failed:', err);
          return Response.json({ error: 'Decision impact ops unavailable' }, { status: 503, headers: corsHeaders });
        }
      }

      if (url.pathname === '/api/diagnostics/calibration' && method === 'GET') {
        if (!isFeatureEnabled(env, 'FEATURE_ENABLE_CALIBRATION_DIAGNOSTICS', 'ENABLE_CALIBRATION_DIAGNOSTICS', true)) {
          return Response.json({
            error: 'Calibration diagnostics disabled',
          }, { status: 503, headers: corsHeaders });
        }

        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Calibration diagnostics schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
        }

        const metricRaw = (url.searchParams.get('metric') || 'conviction').trim().toLowerCase();
        if (metricRaw !== 'conviction' && metricRaw !== 'edge_quality') {
          return Response.json({
            error: 'Invalid metric. Use metric=conviction or metric=edge_quality',
          }, { status: 400, headers: corsHeaders });
        }
        const metric = metricRaw as 'conviction' | 'edge_quality';

        const horizonRaw = (url.searchParams.get('horizon') || '').trim().toLowerCase();
        let horizon: '7d' | '30d' | null = null;
        if (metric === 'conviction') {
          if (horizonRaw !== '7d' && horizonRaw !== '30d') {
            return Response.json({
              error: 'horizon is required for metric=conviction (7d or 30d)',
            }, { status: 400, headers: corsHeaders });
          }
          horizon = horizonRaw;
        }

        const asOfRaw = (url.searchParams.get('as_of') || '').trim();
        let asOfDate: string | null = null;
        if (asOfRaw) {
          asOfDate = parseIsoDate(asOfRaw) || parseIsoDate(asOfRaw.slice(0, 10));
          if (!asOfDate) {
            return Response.json({
              error: 'Invalid as_of. Use YYYY-MM-DD or ISO date-time.',
            }, { status: 400, headers: corsHeaders });
          }
        }

        const snapshot = await fetchCalibrationSnapshotAtOrBefore(env.DB, metric, horizon, asOfDate);
        const diagnostics = computeCalibrationDiagnostics(snapshot);
        const response = snapshot || {
          as_of: asOfDate ? `${asOfDate}T00:00:00.000Z` : asIsoDateTime(new Date()),
          metric,
          horizon,
          basis: metric === 'conviction' ? 'conviction_decile' : 'edge_quality_decile',
          bins: [],
          total_samples: 0,
        };

        return Response.json({
          as_of: response.as_of,
          metric: response.metric,
          horizon: response.horizon,
          basis: response.basis,
          total_samples: response.total_samples,
          bins: response.bins,
          diagnostics,
        }, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'public, max-age=60',
          },
        });
      }

      if (url.pathname === '/api/diagnostics/edge' && method === 'GET') {
        if (!isFeatureEnabled(env, 'FEATURE_ENABLE_EDGE_DIAGNOSTICS', 'ENABLE_EDGE_DIAGNOSTICS', true)) {
          return Response.json({
            error: 'Edge diagnostics disabled',
          }, { status: 503, headers: corsHeaders });
        }

        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Edge diagnostics schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
        }

        const horizonParam = (url.searchParams.get('horizon') || 'all').trim().toLowerCase();
        let horizons: EdgeDiagnosticsHorizon[];
        if (horizonParam === 'all') {
          horizons = ['7d', '30d'];
        } else if (horizonParam === '7d' || horizonParam === '30d') {
          horizons = [horizonParam];
        } else {
          return Response.json({
            error: 'Invalid horizon. Use horizon=7d, horizon=30d, or horizon=all',
          }, { status: 400, headers: corsHeaders });
        }

        try {
          const report = await buildEdgeDiagnosticsReport(env.DB, horizons);
          return Response.json(report, {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'public, max-age=60',
            },
          });
        } catch (err) {
          console.error('Edge diagnostics failed:', err);
          return Response.json({ error: 'Edge diagnostics unavailable' }, { status: 503, headers: corsHeaders });
        }
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

        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Alerts feed schema guard failed:', err);
          return Response.json({
            as_of: new Date().toISOString(),
            alerts: [],
            degraded_reason: 'migration_guard_failed',
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
          query += ` AND datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime(?)`;
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
        if (!canSendEmail(env)) {
          return Response.json({ error: 'Email service unavailable' }, { status: 503, headers: corsHeaders });
        }
        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Subscribe-start schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
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
        const verificationEmail = await sendCloudflareEmail(env, {
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
        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Subscribe-verify schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
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
        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Unsubscribe schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
        }

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

      if (url.pathname === '/api/metrics/utility-event' && method === 'POST') {
        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Utility-event schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
        }

        const body = await parseJsonBody<{
          session_id?: unknown;
          event_type?: unknown;
          route?: unknown;
          actionability_state?: unknown;
          metadata?: unknown;
        }>(request);

        const sessionId = normalizeUtilitySessionId(body?.session_id);
        if (!sessionId) {
          return Response.json({ error: 'Invalid session_id' }, { status: 400, headers: corsHeaders });
        }

        const eventType = normalizeUtilityEventType(body?.event_type);
        if (!eventType) {
          return Response.json({ error: 'Invalid event_type' }, { status: 400, headers: corsHeaders });
        }

        const route = normalizeUtilityRoute(body?.route);
        const actionabilityState = normalizeUtilityActionabilityState(body?.actionability_state);
        const payloadJson = sanitizeUtilityPayload(body?.metadata);
        const createdAt = asIsoDateTime(new Date());
        const ctaIntentEnabled = isFeatureEnabled(
          env,
          'FEATURE_ENABLE_CTA_INTENT_TRACKING',
          'ENABLE_CTA_INTENT_TRACKING',
          true,
        );

        if (eventType === 'cta_action_click' && !ctaIntentEnabled) {
          return Response.json({
            ok: true,
            stored: false,
            ignored_reason: 'cta_intent_tracking_disabled',
            accepted: {
              event_type: eventType,
              route,
              actionability_state: actionabilityState,
            },
          }, {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'no-store',
            },
          });
        }

        try {
          await insertUtilityEvent(env.DB, {
            session_id: sessionId,
            event_type: eventType,
            route,
            actionability_state: actionabilityState,
            payload_json: payloadJson,
            created_at: createdAt,
          });
        } catch (err) {
          console.error('Utility-event insert failed:', err);
          return Response.json({ error: 'Failed to record utility event' }, { status: 503, headers: corsHeaders });
        }

        return Response.json({
          ok: true,
          accepted: {
            event_type: eventType,
            route,
            actionability_state: actionabilityState,
          },
        }, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'no-store',
          },
        });
      }

      if (url.pathname === '/api/ops/utility-funnel' && method === 'GET') {
        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Utility-funnel schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
        }

        const windowRaw = Number.parseInt((url.searchParams.get('window') || '7').trim(), 10);
        const windowDays = UTILITY_WINDOW_DAY_OPTIONS.has(windowRaw) ? windowRaw : 7;

        try {
          const funnel = await computeUtilityFunnelSummary(env.DB, windowDays);
          return Response.json({
            as_of: asIsoDateTime(new Date()),
            funnel,
          }, {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'public, max-age=120',
            },
          });
        } catch (err) {
          console.error('Utility-funnel computation failed:', err);
          return Response.json({ error: 'Utility funnel unavailable' }, { status: 503, headers: corsHeaders });
        }
      }

      if (url.pathname === '/api/market/consistency' && method === 'GET') {
        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Consistency schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
        }

        const latest = await fetchLatestConsistencyCheck(env.DB);
        if (latest) {
          return Response.json({
            as_of: latest.as_of,
            score: latest.score,
            state: latest.state,
            violations: latest.violations,
            components: latest.components,
            created_at: latest.created_at,
          }, { headers: corsHeaders });
        }

        try {
          const canonical = await buildCanonicalMarketDecision(env.DB);
          return Response.json({
            as_of: canonical.as_of,
            score: canonical.consistency.score,
            state: canonical.consistency.state,
            violations: canonical.consistency.violations,
            components: canonical.consistency.components,
            created_at: asIsoDateTime(new Date()),
          }, { headers: corsHeaders });
        } catch (err) {
          console.error('Consistency fallback computation failed:', err);
          return Response.json({ error: 'Consistency unavailable' }, { status: 503, headers: corsHeaders });
        }
      }

      if (url.pathname === '/api/ops/freshness-slo' && method === 'GET') {
        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Freshness SLO schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
        }

        try {
          const [window7d, window30d] = await Promise.all([
            computeFreshnessSloWindow(env.DB, 7),
            computeFreshnessSloWindow(env.DB, 30),
          ]);

          return Response.json({
            as_of: asIsoDateTime(new Date()),
            windows: {
              '7d': window7d,
              '30d': window30d,
            },
          }, {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'public, max-age=300',
            },
          });
        } catch (err) {
          console.error('Freshness SLO computation failed:', err);
          return Response.json({ error: 'Freshness SLO unavailable' }, { status: 503, headers: corsHeaders });
        }
      }

      if (url.pathname === '/api/ops/decision-grade' && method === 'GET') {
        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Decision-grade schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
        }

        const windowRaw = Number.parseInt((url.searchParams.get('window') || '30').trim(), 10);
        if (!UTILITY_WINDOW_DAY_OPTIONS.has(windowRaw)) {
          return Response.json({
            error: 'Invalid window. Supported values: 7, 30',
          }, { status: 400, headers: corsHeaders });
        }

        try {
          const scorecard = await computeDecisionGradeScorecard(env.DB, windowRaw);
          return Response.json(scorecard, {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'public, max-age=300',
            },
          });
        } catch (err) {
          console.error('Decision-grade computation failed:', err);
          return Response.json({ error: 'Decision grade unavailable' }, { status: 503, headers: corsHeaders });
        }
      }

      if (url.pathname === '/api/market/refresh-products' && method === 'POST') {
        const adminAuthFailure = await enforceAdminAuth(request, env, corsHeaders, clientIP);
        if (adminAuthFailure) {
          return adminAuthFailure;
        }

        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Refresh-products schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
        }

        const briefEnabled = isFeatureEnabled(env, 'FEATURE_ENABLE_BRIEF', 'ENABLE_BRIEF', true);
        const opportunitiesEnabled = isFeatureEnabled(env, 'FEATURE_ENABLE_OPPORTUNITIES', 'ENABLE_OPPORTUNITIES', true);
        const inAppAlertsEnabled = isFeatureEnabled(env, 'FEATURE_ENABLE_ALERTS_IN_APP', 'ENABLE_ALERTS_IN_APP', true);
        const coherenceGateEnabled = isFeatureEnabled(
          env,
          'FEATURE_ENABLE_OPPORTUNITY_COHERENCE_GATE',
          'ENABLE_OPPORTUNITY_COHERENCE_GATE',
          true,
        );
        const signalsSanitizerEnabled = isFeatureEnabled(
          env,
          'FEATURE_ENABLE_SIGNALS_SANITIZER',
          'ENABLE_SIGNALS_SANITIZER',
          true,
        );
        const calibrationDiagnosticsEnabled = isFeatureEnabled(
          env,
          'FEATURE_ENABLE_CALIBRATION_DIAGNOSTICS',
          'ENABLE_CALIBRATION_DIAGNOSTICS',
          true,
        );
        const edgeDiagnosticsEnabled = isFeatureEnabled(
          env,
          'FEATURE_ENABLE_EDGE_DIAGNOSTICS',
          'ENABLE_EDGE_DIAGNOSTICS',
          true,
        );
        const decisionImpactEnabled = isFeatureEnabled(
          env,
          'FEATURE_ENABLE_DECISION_IMPACT',
          'ENABLE_DECISION_IMPACT',
          true,
        );
        const decisionImpactGovernance = resolveDecisionImpactGovernance(env);
        const triggerHeader = request.headers.get('X-Refresh-Trigger');
        const refreshTrigger = triggerHeader && triggerHeader.trim().length > 0
          ? triggerHeader.trim().slice(0, 128)
          : 'unknown';
        const refreshRunId = await recordMarketRefreshRunStart(env.DB, refreshTrigger);

        try {
          let edgeDiagnosticsReport: EdgeDiagnosticsReport | null = null;
          if (edgeDiagnosticsEnabled) {
            edgeDiagnosticsReport = await buildEdgeDiagnosticsReport(env.DB, ['7d', '30d']);
            if (!edgeDiagnosticsReport.promotion_gate.pass) {
              throw new Error(`promotion_gate_failed:${edgeDiagnosticsReport.promotion_gate.reasons.join(',')}`);
            }
          }

          let edgeCalibrationSnapshot: MarketCalibrationSnapshotPayload | null = null;
          let convictionCalibration7d: MarketCalibrationSnapshotPayload | null = null;
          let convictionCalibration30d: MarketCalibrationSnapshotPayload | null = null;
          let calibrationsGenerated = 0;

          try {
            edgeCalibrationSnapshot = await buildEdgeQualityCalibrationSnapshot(env.DB);
            await storeCalibrationSnapshot(env.DB, edgeCalibrationSnapshot);
            calibrationsGenerated += 1;
          } catch (err) {
            console.error('Edge calibration refresh failed:', err);
            edgeCalibrationSnapshot = null;
          }

          if (opportunitiesEnabled) {
            try {
              convictionCalibration7d = await buildConvictionCalibrationSnapshot(env.DB, '7d');
              await storeCalibrationSnapshot(env.DB, convictionCalibration7d);
              calibrationsGenerated += 1;
            } catch (err) {
              console.error('7d conviction calibration refresh failed:', err);
              convictionCalibration7d = null;
            }

            try {
              convictionCalibration30d = await buildConvictionCalibrationSnapshot(env.DB, '30d');
              await storeCalibrationSnapshot(env.DB, convictionCalibration30d);
              calibrationsGenerated += 1;
            } catch (err) {
              console.error('30d conviction calibration refresh failed:', err);
              convictionCalibration30d = null;
            }
          }

          let brief: BriefSnapshot | null = null;
          let consistencyStored = false;
          if (briefEnabled) {
            brief = await buildBriefSnapshot(env.DB);
            if (brief) {
              await storeBriefSnapshot(env.DB, brief);
              await storeConsistencyCheck(env.DB, brief.source_plan_as_of, brief.consistency);
              consistencyStored = true;
            }
          }

          if (!consistencyStored) {
            try {
              const canonical = await buildCanonicalMarketDecision(env.DB);
              await storeConsistencyCheck(env.DB, canonical.as_of, canonical.consistency);
              consistencyStored = true;
            } catch (err) {
              console.error('Consistency snapshot fallback store failed:', err);
            }
          }

          let opportunities7d: OpportunitySnapshot | null = null;
          let opportunities30d: OpportunitySnapshot | null = null;
          if (opportunitiesEnabled) {
            opportunities7d = await buildOpportunitySnapshot(env.DB, '7d', convictionCalibration7d, {
              sanitize_signals_tickers: signalsSanitizerEnabled,
            });
            opportunities30d = await buildOpportunitySnapshot(env.DB, '30d', convictionCalibration30d, {
              sanitize_signals_tickers: signalsSanitizerEnabled,
            });
            if (opportunities7d) await storeOpportunitySnapshot(env.DB, opportunities7d);
            if (opportunities30d) await storeOpportunitySnapshot(env.DB, opportunities30d);
          }

          let alertsGenerated = 0;
          if (brief && opportunities7d) {
            const projectedForAlerts = projectOpportunityFeed(
              normalizeOpportunityItemsForPublishing(opportunities7d.items, convictionCalibration7d),
              {
                coherence_gate_enabled: coherenceGateEnabled,
                freshness: brief.freshness_status,
                consistency_state: brief.consistency.state,
              }
            );
            const generated = await generateMarketEvents(env.DB, brief, {
              ...opportunities7d,
              items: projectedForAlerts.items,
            });
            alertsGenerated = await insertMarketEvents(env.DB, generated, inAppAlertsEnabled);
          }

          const freshnessForRun = brief?.freshness_status || await computeFreshnessStatus(env.DB);
          let consistencyStateForRun: ConsistencyState = brief?.consistency.state || 'PASS';
          if (!brief) {
            const latestConsistency = await fetchLatestConsistencyCheck(env.DB);
            if (latestConsistency) {
              consistencyStateForRun = latestConsistency.state;
            } else {
              try {
                consistencyStateForRun = (await buildCanonicalMarketDecision(env.DB)).consistency.state;
              } catch {
                consistencyStateForRun = 'PASS';
              }
            }
          }

          let qualityFilteredCount = 0;
          let coherenceSuppressedCount = 0;
          let suppressedDataQualityCount = 0;
          const ledgerRows: OpportunityLedgerInsertPayload[] = [];
          const itemLedgerRows: OpportunityItemLedgerInsertPayload[] = [];
          const projectionTargets: Array<{
            snapshot: OpportunitySnapshot | null;
            calibration: MarketCalibrationSnapshotPayload | null;
          }> = [
            { snapshot: opportunities7d, calibration: convictionCalibration7d },
            { snapshot: opportunities30d, calibration: convictionCalibration30d },
          ];
          for (const target of projectionTargets) {
            if (!target.snapshot) continue;
            const ledgerBuild = buildOpportunityLedgerProjection({
              refresh_run_id: refreshRunId,
              snapshot: target.snapshot,
              calibration: target.calibration,
              coherence_gate_enabled: coherenceGateEnabled,
              freshness: freshnessForRun,
              consistency_state: consistencyStateForRun,
            });
            qualityFilteredCount += ledgerBuild.projected.quality_filtered_count;
            coherenceSuppressedCount += ledgerBuild.projected.coherence_suppressed_count;
            suppressedDataQualityCount += ledgerBuild.projected.suppression_by_reason.data_quality_suppressed;
            ledgerRows.push(ledgerBuild.ledger_row);
            itemLedgerRows.push(...ledgerBuild.item_rows);
          }
          for (const row of ledgerRows) {
            try {
              await insertOpportunityLedgerRow(env.DB, row);
            } catch (err) {
              console.error('Opportunity ledger insert failed:', err);
            }
          }
          for (const row of itemLedgerRows) {
            try {
              await insertOpportunityItemLedgerRow(env.DB, row);
            } catch (err) {
              console.error('Opportunity item ledger insert failed:', err);
            }
          }
          const overSuppressedCount = ledgerRows.filter((row) =>
            row.candidate_count > 0 &&
            row.published_count === 0 &&
            row.data_quality_suppressed_count === 0
          ).length;
          const horizon7 = ledgerRows.find((row) => row.horizon === '7d') || null;
          const horizon30 = ledgerRows.find((row) => row.horizon === '30d') || null;
          const crossHorizonState: 'ALIGNED' | 'MIXED' | 'CONFLICT' | 'INSUFFICIENT' = (
            horizon7 &&
            horizon30 &&
            horizon7.published_count > 0 &&
            horizon30.published_count > 0 &&
            horizon7.top_direction_published &&
            horizon30.top_direction_published
          )
            ? (horizon7.top_direction_published === horizon30.top_direction_published ? 'ALIGNED' : 'CONFLICT')
            : (
              horizon7 &&
              horizon30 &&
              horizon7.published_count === 0 &&
              horizon30.published_count === 0
            )
              ? 'INSUFFICIENT'
              : (
                horizon7 || horizon30
              )
                ? 'MIXED'
                : 'INSUFFICIENT';

          let decisionImpactSnapshotsGenerated = 0;
          let decisionImpactSummary: {
            market_7d_hit_rate: number;
            market_7d_sample_size: number;
            market_30d_hit_rate: number;
            market_30d_sample_size: number;
            actionable_sessions: number;
            cta_action_rate_pct: number;
            governance_mode: 'observe' | 'enforce';
            enforce_ready: boolean;
            enforce_breach_count: number;
            enforce_breaches: string[];
            minimum_samples_required: number;
            minimum_actionable_sessions_required: number;
            observe_breach_count: number;
            observe_breaches: string[];
          } | null = null;
          let decisionImpactGenerationError: string | null = null;
          let decisionImpactEnforcementError: string | null = null;
          if (decisionImpactEnabled) {
            try {
              const asOfFallback = brief?.as_of || opportunities7d?.as_of || opportunities30d?.as_of || asIsoDateTime(new Date());
              const asOfByHorizon: Record<'7d' | '30d', string> = {
                '7d': opportunities7d?.as_of || asOfFallback,
                '30d': opportunities30d?.as_of || opportunities7d?.as_of || asOfFallback,
              };
              for (const horizon of ['7d', '30d'] as const) {
                for (const scope of ['market', 'theme'] as const) {
                  for (const windowDays of [30, 90] as const) {
                    const snapshot = await computeDecisionImpact(env.DB, {
                      horizon,
                      scope,
                      window_days: windowDays,
                      limit: scope === 'theme' ? 50 : 10,
                      as_of: asOfByHorizon[horizon],
                    });
                    await storeDecisionImpactSnapshot(env.DB, snapshot);
                    decisionImpactSnapshotsGenerated += 1;
                  }
                }
              }

              const opsDecisionImpact = await buildDecisionImpactOpsResponse(env.DB, 30, decisionImpactGovernance);
              decisionImpactSummary = {
                market_7d_hit_rate: opsDecisionImpact.market_7d.hit_rate,
                market_7d_sample_size: opsDecisionImpact.market_7d.sample_size,
                market_30d_hit_rate: opsDecisionImpact.market_30d.hit_rate,
                market_30d_sample_size: opsDecisionImpact.market_30d.sample_size,
                actionable_sessions: opsDecisionImpact.utility_attribution.actionable_sessions,
                cta_action_rate_pct: opsDecisionImpact.utility_attribution.cta_action_rate_pct,
                governance_mode: opsDecisionImpact.observe_mode.mode,
                enforce_ready: opsDecisionImpact.observe_mode.enforce_ready,
                enforce_breach_count: opsDecisionImpact.observe_mode.enforce_breach_count,
                enforce_breaches: opsDecisionImpact.observe_mode.enforce_breaches,
                minimum_samples_required: opsDecisionImpact.observe_mode.minimum_samples_required,
                minimum_actionable_sessions_required: opsDecisionImpact.observe_mode.minimum_actionable_sessions_required,
                observe_breach_count: opsDecisionImpact.observe_mode.breach_count,
                observe_breaches: opsDecisionImpact.observe_mode.breaches,
              };
              if (
                decisionImpactGovernance.enforce_enabled &&
                opsDecisionImpact.observe_mode.enforce_ready &&
                opsDecisionImpact.observe_mode.enforce_breach_count > 0
              ) {
                decisionImpactEnforcementError = `decision_impact_enforcement_failed:${opsDecisionImpact.observe_mode.enforce_breaches.join(',')}`;
              }
            } catch (err) {
              decisionImpactGenerationError = err instanceof Error ? err.message : String(err);
              console.error('Decision impact snapshot generation failed:', err);
            }
          }
          if (decisionImpactEnforcementError) {
            throw new Error(decisionImpactEnforcementError);
          }

          let decisionGradeSnapshot: {
            score: number;
            grade: 'GREEN' | 'YELLOW' | 'RED';
            go_live_ready: boolean;
            opportunity_hygiene: {
              over_suppression_rate_pct: number;
              cross_horizon_conflict_rate_pct: number;
              conflict_persistence_days: number;
            };
          } | null = null;
          try {
            const scorecard = await computeDecisionGradeScorecard(env.DB, 30);
            decisionGradeSnapshot = {
              score: scorecard.score,
              grade: scorecard.grade,
              go_live_ready: scorecard.go_live_ready,
              opportunity_hygiene: {
                over_suppression_rate_pct: scorecard.components.opportunity_hygiene.over_suppression_rate_pct,
                cross_horizon_conflict_rate_pct: scorecard.components.opportunity_hygiene.cross_horizon_conflict_rate_pct,
                conflict_persistence_days: scorecard.components.opportunity_hygiene.conflict_persistence_days,
              },
            };
          } catch (err) {
            console.warn('Decision-grade snapshot unavailable during refresh-products:', err);
          }

          let diagnosticsSummary: {
            edge_quality: CalibrationQuality;
            conviction_7d: CalibrationQuality;
            conviction_30d: CalibrationQuality;
          } | null = null;
          if (calibrationDiagnosticsEnabled) {
            diagnosticsSummary = {
              edge_quality: computeCalibrationDiagnostics(edgeCalibrationSnapshot).quality_band,
              conviction_7d: computeCalibrationDiagnostics(convictionCalibration7d).quality_band,
              conviction_30d: computeCalibrationDiagnostics(convictionCalibration30d).quality_band,
            };
          }

          await recordMarketRefreshRunFinish(env.DB, refreshRunId, {
            status: 'success',
            brief_generated: brief ? 1 : 0,
            opportunities_generated: (opportunities7d ? 1 : 0) + (opportunities30d ? 1 : 0),
            calibrations_generated: calibrationsGenerated,
            alerts_generated: alertsGenerated,
            stale_count: freshnessForRun.stale_count,
            critical_stale_count: freshnessForRun.critical_stale_count,
            as_of: brief?.as_of || opportunities7d?.as_of || null,
            error: null,
          });

          return Response.json({
            ok: true,
            brief_generated: brief ? 1 : 0,
            opportunities_generated: (opportunities7d ? 1 : 0) + (opportunities30d ? 1 : 0),
            calibrations_generated: calibrationsGenerated,
            alerts_generated: alertsGenerated,
            consistency_stored: consistencyStored ? 1 : 0,
            consistency_state: consistencyStateForRun,
            consistency_score: brief?.consistency.score ?? null,
            as_of: brief?.as_of || opportunities7d?.as_of || null,
            stale_count: freshnessForRun.stale_count,
            critical_stale_count: freshnessForRun.critical_stale_count,
            quality_filtered_count: qualityFilteredCount,
            coherence_suppressed_count: coherenceSuppressedCount,
            suppressed_data_quality_count: suppressedDataQualityCount,
            over_suppressed_count: overSuppressedCount,
            cross_horizon_state: crossHorizonState,
            opportunity_ledger_rows: ledgerRows.length,
            opportunity_item_ledger_rows: itemLedgerRows.length,
            decision_impact_snapshots_generated: decisionImpactSnapshotsGenerated,
            decision_impact: decisionImpactSummary,
            decision_impact_error: decisionImpactGenerationError,
            calibration_diagnostics: diagnosticsSummary,
            decision_grade_snapshot: decisionGradeSnapshot,
            edge_diagnostics: edgeDiagnosticsReport
              ? {
                  as_of: edgeDiagnosticsReport.as_of,
                  promotion_gate: edgeDiagnosticsReport.promotion_gate,
                  windows: edgeDiagnosticsReport.windows.map((window) => ({
                    horizon: window.horizon,
                    sample_size: window.sample_size,
                    model_direction_accuracy: window.model_direction_accuracy,
                    baseline_direction_accuracy: window.baseline_direction_accuracy,
                    uplift_vs_baseline: window.uplift_vs_baseline,
                    uplift_ci95_low: window.uplift_ci95_low,
                    uplift_ci95_high: window.uplift_ci95_high,
                    lower_bound_positive: window.lower_bound_positive,
                    leakage_sentinel: window.leakage_sentinel,
                    quality_band: window.quality_band,
                  })),
                }
              : null,
            refresh_trigger: refreshTrigger,
            refresh_run_id: refreshRunId,
          }, { headers: corsHeaders });
        } catch (err) {
          const errorMessage = err instanceof Error ? err.message : String(err);
          await recordMarketRefreshRunFinish(env.DB, refreshRunId, {
            status: 'failed',
            error: errorMessage.slice(0, 1000),
          });
          console.error('Refresh-products failed:', err);
          return Response.json({ error: 'Refresh products failed', detail: errorMessage }, { status: 500, headers: corsHeaders });
        }
      }

      if (url.pathname === '/api/market/backfill-products' && method === 'POST') {
        const adminAuthFailure = await enforceAdminAuth(request, env, corsHeaders, clientIP);
        if (adminAuthFailure) {
          return adminAuthFailure;
        }

        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Backfill-products schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
        }

        const signalsSanitizerEnabled = isFeatureEnabled(
          env,
          'FEATURE_ENABLE_SIGNALS_SANITIZER',
          'ENABLE_SIGNALS_SANITIZER',
          true,
        );

        let body: {
          start?: string;
          end?: string;
          limit?: number;
          overwrite?: boolean;
          dry_run?: boolean;
          recalibrate?: boolean;
          rebuild_ledgers?: boolean;
        } = {};
        try {
          body = await request.json() as {
            start?: string;
            end?: string;
            limit?: number;
            overwrite?: boolean;
            dry_run?: boolean;
            recalibrate?: boolean;
            rebuild_ledgers?: boolean;
          };
        } catch {
          body = {};
        }

        let dateFilter: ReturnType<typeof parseBackfillDateRange>;
        try {
          dateFilter = parseBackfillDateRange(body.start, body.end);
        } catch (err) {
          return Response.json({
            error: err instanceof Error ? err.message : 'Invalid date range',
          }, { status: 400, headers: corsHeaders });
        }

        const limit = parseBackfillLimit(body.limit);
        const today = asIsoDate(new Date());
        const startDate = dateFilter.start || addCalendarDays(today, -540);
        const endDate = dateFilter.end || today;
        const overwrite = body.overwrite === true;
        const dryRun = body.dry_run === true;
        const recalibrate = body.recalibrate !== false;
        const rebuildLedgers = body.rebuild_ledgers !== false;

        const dateRows = await env.DB.prepare(`
          SELECT p.date as date, COUNT(c.category) as category_count
          FROM pxi_scores p
          JOIN category_scores c ON c.date = p.date
          WHERE p.date >= ?
            AND p.date <= ?
          GROUP BY p.date
          HAVING COUNT(c.category) >= 3
          ORDER BY p.date DESC
          LIMIT ?
        `).bind(startDate, endDate, limit).all<{ date: string; category_count: number }>();

        const candidateDates = (dateRows.results || []).map((row) => row.date);
        const existingByDate = new Map<string, Set<'7d' | '30d'>>();
        if (!overwrite && candidateDates.length > 0) {
          const asOfDates = candidateDates.map((date) => `${date}T00:00:00.000Z`);
          const placeholders = asOfDates.map(() => '?').join(',');
          const existingRows = await env.DB.prepare(`
            SELECT as_of, horizon
            FROM opportunity_snapshots
            WHERE as_of IN (${placeholders})
              AND horizon IN ('7d', '30d')
          `).bind(...asOfDates).all<{ as_of: string; horizon: '7d' | '30d' }>();

          for (const row of existingRows.results || []) {
            const date = row.as_of.slice(0, 10);
            const set = existingByDate.get(date) || new Set<'7d' | '30d'>();
            set.add(row.horizon);
            existingByDate.set(date, set);
          }
        }

        const processDates = [...candidateDates].sort((a, b) => a.localeCompare(b));
        let seededSnapshots = 0;
        let processedDates = 0;
        let skippedDates = 0;
        const skippedExisting: string[] = [];
        const failedDates: Array<{ date: string; error: string }> = [];

        for (const date of processDates) {
          const existing = existingByDate.get(date);
          if (!overwrite && existing?.has('7d') && existing?.has('30d')) {
            skippedDates += 1;
            if (skippedExisting.length < 20) {
              skippedExisting.push(date);
            }
            continue;
          }

          processedDates += 1;
          try {
            const [snapshot7d, snapshot30d] = await Promise.all([
              buildHistoricalOpportunitySnapshot(env.DB, date, '7d'),
              buildHistoricalOpportunitySnapshot(env.DB, date, '30d'),
            ]);

            if (!dryRun) {
              if (snapshot7d) {
                await storeOpportunitySnapshot(env.DB, snapshot7d);
                seededSnapshots += 1;
              }
              if (snapshot30d) {
                await storeOpportunitySnapshot(env.DB, snapshot30d);
                seededSnapshots += 1;
              }
            } else {
              if (snapshot7d) seededSnapshots += 1;
              if (snapshot30d) seededSnapshots += 1;
            }
          } catch (err) {
            const errorMessage = err instanceof Error ? err.message : String(err);
            failedDates.push({ date, error: errorMessage.slice(0, 300) });
          }
        }

        let calibrationsGenerated = 0;
        let convictionCalibration7d: MarketCalibrationSnapshotPayload | null = null;
        let convictionCalibration30d: MarketCalibrationSnapshotPayload | null = null;
        let edgeCalibration: MarketCalibrationSnapshotPayload | null = null;
        let ledgerRowsGenerated = 0;
        let itemLedgerRowsGenerated = 0;
        let decisionImpactSnapshotsGenerated = 0;
        let decisionImpactSummary: {
          market_7d_hit_rate: number;
          market_7d_sample_size: number;
          market_30d_hit_rate: number;
          market_30d_sample_size: number;
          actionable_sessions: number;
          cta_action_rate_pct: number;
          governance_mode: 'observe' | 'enforce';
          enforce_ready: boolean;
          enforce_breach_count: number;
          enforce_breaches: string[];
          observe_breach_count: number;
          observe_breaches: string[];
        } | null = null;
        if (!dryRun && recalibrate) {
          try {
            convictionCalibration7d = await buildConvictionCalibrationSnapshot(env.DB, '7d');
            await storeCalibrationSnapshot(env.DB, convictionCalibration7d);
            calibrationsGenerated += 1;
          } catch (err) {
            console.error('Backfill 7d conviction calibration failed:', err);
          }

          try {
            convictionCalibration30d = await buildConvictionCalibrationSnapshot(env.DB, '30d');
            await storeCalibrationSnapshot(env.DB, convictionCalibration30d);
            calibrationsGenerated += 1;
          } catch (err) {
            console.error('Backfill 30d conviction calibration failed:', err);
          }

          try {
            edgeCalibration = await buildEdgeQualityCalibrationSnapshot(env.DB);
            await storeCalibrationSnapshot(env.DB, edgeCalibration);
            calibrationsGenerated += 1;
          } catch (err) {
            console.error('Backfill edge calibration failed:', err);
          }

          try {
            const latest7d = await buildOpportunitySnapshot(env.DB, '7d', convictionCalibration7d, {
              sanitize_signals_tickers: signalsSanitizerEnabled,
            });
            const latest30d = await buildOpportunitySnapshot(env.DB, '30d', convictionCalibration30d, {
              sanitize_signals_tickers: signalsSanitizerEnabled,
            });
            if (latest7d) await storeOpportunitySnapshot(env.DB, latest7d);
            if (latest30d) await storeOpportunitySnapshot(env.DB, latest30d);
          } catch (err) {
            console.error('Backfill latest opportunity refresh failed:', err);
          }
        }

        if (!dryRun && rebuildLedgers) {
          const snapshotLimit = Math.max(
            1,
            Math.min(MAX_BACKFILL_LIMIT * 2, candidateDates.length * 2 + 10),
          );
          const snapshotRows = await env.DB.prepare(`
            SELECT as_of, horizon, payload_json
            FROM opportunity_snapshots
            WHERE substr(as_of, 1, 10) >= ?
              AND substr(as_of, 1, 10) <= ?
              AND horizon IN ('7d', '30d')
            ORDER BY as_of ASC, horizon ASC
            LIMIT ?
          `).bind(startDate, endDate, snapshotLimit).all<{
            as_of: string;
            horizon: '7d' | '30d';
            payload_json: string;
          }>();

          for (const row of snapshotRows.results || []) {
            let snapshot: OpportunitySnapshot | null = null;
            try {
              snapshot = JSON.parse(row.payload_json) as OpportunitySnapshot;
            } catch {
              snapshot = null;
            }
            if (!snapshot || !Array.isArray(snapshot.items)) {
              continue;
            }

            try {
              const calibration = row.horizon === '7d' ? convictionCalibration7d : convictionCalibration30d;
              const ledgerBuild = buildOpportunityLedgerProjection({
                refresh_run_id: null,
                snapshot,
                calibration,
                coherence_gate_enabled: true,
                freshness: {
                  has_stale_data: false,
                  stale_count: 0,
                  critical_stale_count: 0,
                },
                consistency_state: 'PASS',
              });

              await insertOpportunityLedgerRow(env.DB, ledgerBuild.ledger_row);
              ledgerRowsGenerated += 1;

              for (const itemRow of ledgerBuild.item_rows) {
                await insertOpportunityItemLedgerRow(env.DB, itemRow);
                itemLedgerRowsGenerated += 1;
              }
            } catch (err) {
              const errorMessage = err instanceof Error ? err.message : String(err);
              failedDates.push({
                date: row.as_of.slice(0, 10),
                error: `ledger:${errorMessage.slice(0, 260)}`,
              });
            }
          }

          const asOfForImpact = `${endDate}T00:00:00.000Z`;
          try {
            for (const horizon of ['7d', '30d'] as const) {
              for (const scope of ['market', 'theme'] as const) {
                for (const windowDays of [30, 90] as const) {
                  const snapshot = await computeDecisionImpact(env.DB, {
                    horizon,
                    scope,
                    window_days: windowDays,
                    limit: scope === 'theme' ? 50 : 10,
                    as_of: asOfForImpact,
                  });
                  await storeDecisionImpactSnapshot(env.DB, snapshot);
                  decisionImpactSnapshotsGenerated += 1;
                }
              }
            }

            const governance = resolveDecisionImpactGovernance(env);
            const opsDecisionImpact = await buildDecisionImpactOpsResponse(env.DB, 30, governance);
            decisionImpactSummary = {
              market_7d_hit_rate: opsDecisionImpact.market_7d.hit_rate,
              market_7d_sample_size: opsDecisionImpact.market_7d.sample_size,
              market_30d_hit_rate: opsDecisionImpact.market_30d.hit_rate,
              market_30d_sample_size: opsDecisionImpact.market_30d.sample_size,
              actionable_sessions: opsDecisionImpact.utility_attribution.actionable_sessions,
              cta_action_rate_pct: opsDecisionImpact.utility_attribution.cta_action_rate_pct,
              governance_mode: opsDecisionImpact.observe_mode.mode,
              enforce_ready: opsDecisionImpact.observe_mode.enforce_ready,
              enforce_breach_count: opsDecisionImpact.observe_mode.enforce_breach_count,
              enforce_breaches: opsDecisionImpact.observe_mode.enforce_breaches,
              observe_breach_count: opsDecisionImpact.observe_mode.breach_count,
              observe_breaches: opsDecisionImpact.observe_mode.breaches,
            };
          } catch (err) {
            console.error('Backfill decision-impact snapshot regeneration failed:', err);
          }
        }

        return Response.json({
          ok: true,
          dry_run: dryRun,
          requested: {
            start: startDate,
            end: endDate,
            limit,
            overwrite,
            recalibrate,
            rebuild_ledgers: rebuildLedgers,
          },
          scanned_dates: candidateDates.length,
          processed_dates: processedDates,
          skipped_dates: skippedDates,
          seeded_snapshots: seededSnapshots,
          calibrations_generated: calibrationsGenerated,
          opportunity_ledger_rows_generated: ledgerRowsGenerated,
          opportunity_item_ledger_rows_generated: itemLedgerRowsGenerated,
          decision_impact_snapshots_generated: decisionImpactSnapshotsGenerated,
          decision_impact: decisionImpactSummary,
          calibration_samples: {
            edge_total_samples: edgeCalibration?.total_samples ?? null,
            conviction_7d_total_samples: convictionCalibration7d?.total_samples ?? null,
            conviction_30d_total_samples: convictionCalibration30d?.total_samples ?? null,
          },
          skipped_existing_dates: skippedExisting,
          failed_dates: failedDates.slice(0, 20),
        }, { headers: corsHeaders });
      }

      if (url.pathname === '/api/market/send-digest' && method === 'POST') {
        const adminAuthFailure = await enforceAdminAuth(request, env, corsHeaders, clientIP);
        if (adminAuthFailure) {
          return adminAuthFailure;
        }

        try {
          await ensureMarketProductSchema(env.DB);
        } catch (err) {
          console.error('Send-digest schema guard failed:', err);
          return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
        }

        if (!isFeatureEnabled(env, 'FEATURE_ENABLE_ALERTS_EMAIL', 'ENABLE_ALERTS_EMAIL', true)) {
          return Response.json({
            ok: true,
            skipped: true,
            reason: 'Email alerts disabled',
          }, { headers: corsHeaders });
        }

        if (!canSendEmail(env)) {
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
            WHERE datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', '-24 hours')
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

        const [divergence, freshness, mlSampleSize, edgeCalibrationSnapshot] = await Promise.all([
          detectDivergence(env.DB, pxi.score, regime),
          computeFreshnessStatus(env.DB),
          fetchPredictionEvaluationSampleSize(env.DB),
          fetchLatestCalibrationSnapshot(env.DB, 'edge_quality', null),
        ]);
        const conflictState = resolveConflictState(regime, signal);
        const stalePenaltyUnits = freshnessPenaltyCount(freshness);
        const edgeQuality = computeEdgeQualitySnapshot({
          staleCount: stalePenaltyUnits,
          mlSampleSize,
          regime,
          conflictState,
          divergenceCount: divergence.alerts.length,
        });
        const edgeQualityWithCalibration: EdgeQualitySnapshot = {
          ...edgeQuality,
          calibration: buildEdgeQualityCalibrationFromSnapshot(edgeCalibrationSnapshot, edgeQuality.score),
        };

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
          edge_quality: edgeQualityWithCalibration,
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

        let canonical: Awaited<ReturnType<typeof buildCanonicalMarketDecision>> | null = null;
        try {
          canonical = await buildCanonicalMarketDecision(env.DB);
        } catch (err) {
          console.error('Failed to build canonical market decision:', err);
          return Response.json(buildPlanFallbackPayload('no_pxi_data'), {
            headers: {
              ...corsHeaders,
              'Cache-Control': 'no-store',
            },
          });
        }

        const { pxi, signal, regime, freshness, risk_band, edge_quality, policy_state, degraded_reasons, risk_sizing } = canonical;
        const stalePenaltyUnits = freshnessPenaltyCount(freshness);
        const setupSummary = `PXI ${pxi.score.toFixed(1)} (${pxi.label}); ${policy_state.stance.replace('_', ' ')} stance with ${signal.signal_type.replace('_', ' ')} tactical posture at ${risk_sizing.target_pct}% risk budget (raw ${Math.round(signal.risk_allocation * 100)}%).${
          stalePenaltyUnits > 0
            ? ` stale-input pressure ${stalePenaltyUnits} (critical stale: ${freshness.critical_stale_count}).`
            : ''
        }`;

        let briefRef: PlanPayload['brief_ref'] | undefined;
        let opportunityRef: PlanPayload['opportunity_ref'] | undefined;
        let alertsRef: PlanPayload['alerts_ref'] | undefined;
        let crossHorizonRef: PlanPayload['cross_horizon'] | undefined;
        try {
          await ensureMarketProductSchema(env.DB);

          const [briefRow, alertsCounts] = await Promise.all([
            env.DB.prepare(`
              SELECT payload_json
              FROM market_brief_snapshots
              ORDER BY as_of DESC
              LIMIT 1
            `).first<{ payload_json: string }>(),
            env.DB.prepare(`
              SELECT
                MAX(created_at) as latest_as_of,
                SUM(CASE WHEN severity = 'warning' THEN 1 ELSE 0 END) as warning_count,
                SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical_count
              FROM market_alert_events
              WHERE datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', '-24 hours')
            `).first<{
              latest_as_of: string | null;
              warning_count: number | null;
              critical_count: number | null;
            }>(),
          ]);

          if (briefRow?.payload_json) {
            try {
              const briefSnapshot = JSON.parse(briefRow.payload_json) as BriefSnapshot;
              if (isBriefSnapshotCompatible(briefSnapshot)) {
                briefRef = {
                  as_of: briefSnapshot.as_of,
                  regime_delta: briefSnapshot.regime_delta,
                  risk_posture: briefSnapshot.risk_posture,
                };
              }
            } catch {
              briefRef = undefined;
            }
          }

          const opportunityRows = await env.DB.prepare(`
            SELECT horizon, as_of, payload_json
            FROM opportunity_snapshots
            WHERE horizon IN ('7d', '30d')
              AND as_of = (
                SELECT MAX(s2.as_of)
                FROM opportunity_snapshots s2
                WHERE s2.horizon = opportunity_snapshots.horizon
              )
          `).all<{ horizon: '7d' | '30d'; as_of: string; payload_json: string }>();

          const coherenceGateEnabled = isFeatureEnabled(
            env,
            'FEATURE_ENABLE_OPPORTUNITY_COHERENCE_GATE',
            'ENABLE_OPPORTUNITY_COHERENCE_GATE',
            true,
          );
          const projectedByHorizon: Partial<Record<'7d' | '30d', OpportunityFeedProjection>> = {};
          const asOfByHorizon: Partial<Record<'7d' | '30d', string>> = {};

          for (const row of opportunityRows.results || []) {
            try {
              const opportunitySnapshot = JSON.parse(row.payload_json) as OpportunitySnapshot;
              if (
                !opportunitySnapshot ||
                (opportunitySnapshot.horizon !== '7d' && opportunitySnapshot.horizon !== '30d') ||
                !Array.isArray(opportunitySnapshot.items)
              ) {
                continue;
              }

              const calibration = await fetchLatestCalibrationSnapshot(
                env.DB,
                'conviction',
                opportunitySnapshot.horizon
              );
              const normalized = normalizeOpportunityItemsForPublishing(
                opportunitySnapshot.items,
                calibration
              );
              const projected = projectOpportunityFeed(normalized, {
                coherence_gate_enabled: coherenceGateEnabled,
                freshness,
                consistency_state: canonical.consistency.state,
              });

              projectedByHorizon[opportunitySnapshot.horizon] = projected;
              asOfByHorizon[opportunitySnapshot.horizon] = opportunitySnapshot.as_of;
            } catch {
              // Skip malformed snapshot payloads.
            }
          }

          if (projectedByHorizon['7d']) {
            opportunityRef = {
              as_of: asOfByHorizon['7d'] || canonical.as_of,
              horizon: '7d',
              eligible_count: projectedByHorizon['7d'].items.length,
              suppressed_count: projectedByHorizon['7d'].suppressed_count,
              degraded_reason: projectedByHorizon['7d'].degraded_reason,
            };
          } else if (projectedByHorizon['30d']) {
            opportunityRef = {
              as_of: asOfByHorizon['30d'] || canonical.as_of,
              horizon: '30d',
              eligible_count: projectedByHorizon['30d'].items.length,
              suppressed_count: projectedByHorizon['30d'].suppressed_count,
              degraded_reason: projectedByHorizon['30d'].degraded_reason,
            };
          }

          crossHorizonRef = summarizeCrossHorizonCoherence({
            projected_7d: projectedByHorizon['7d'] || null,
            projected_30d: projectedByHorizon['30d'] || null,
            as_of_7d: asOfByHorizon['7d'] || null,
            as_of_30d: asOfByHorizon['30d'] || null,
          }) || undefined;

          alertsRef = {
            as_of: alertsCounts?.latest_as_of || canonical.as_of,
            warning_count_24h: Math.max(0, Math.floor(toNumber(alertsCounts?.warning_count, 0))),
            critical_count_24h: Math.max(0, Math.floor(toNumber(alertsCounts?.critical_count, 0))),
          };
        } catch (err) {
          console.warn('Failed to attach plan reference blocks:', err);
        }

        const actionability = resolvePlanActionability({
          opportunity_ref: opportunityRef,
          edge_quality,
          freshness,
          consistency: canonical.consistency,
        });
        const actionabilityWithCrossHorizon = applyCrossHorizonActionabilityOverride(actionability, crossHorizonRef);
        const decisionStack = buildDecisionStack({
          actionability_state: actionabilityWithCrossHorizon.state,
          setup_summary: setupSummary,
          edge_quality,
          consistency: canonical.consistency,
          opportunity_ref: opportunityRef,
          brief_ref: briefRef,
          alerts_ref: alertsRef,
          cross_horizon: crossHorizonRef,
        });
        const invalidationRules = buildInvalidationRules({
          pxi,
          freshness,
          regime,
          edgeQuality: edge_quality,
        });
        if (crossHorizonRef?.invalidation_note) {
          invalidationRules.unshift(crossHorizonRef.invalidation_note);
        }
        const finalInvalidationRules = Array.from(new Set(invalidationRules));

        const payload: PlanPayload = {
          as_of: canonical.as_of,
          setup_summary: setupSummary,
          policy_state,
          actionability_state: actionabilityWithCrossHorizon.state,
          actionability_reason_codes: actionabilityWithCrossHorizon.reason_codes,
          action_now: {
            risk_allocation_target: risk_sizing.target_pct / 100,
            raw_signal_allocation_target: risk_sizing.raw_signal_allocation_target,
            risk_allocation_basis: 'penalized_playbook_target',
            horizon_bias: resolveHorizonBias(signal, regime, edge_quality.score),
            primary_signal: signal.signal_type,
          },
          edge_quality,
          risk_band,
          uncertainty: canonical.uncertainty,
          consistency: canonical.consistency,
          trader_playbook: canonical.trader_playbook,
          invalidation_rules: finalInvalidationRules,
          ...(briefRef ? { brief_ref: briefRef } : {}),
          ...(opportunityRef ? { opportunity_ref: opportunityRef } : {}),
          ...(alertsRef ? { alerts_ref: alertsRef } : {}),
          ...(crossHorizonRef ? { cross_horizon: crossHorizonRef } : {}),
          decision_stack: decisionStack,
          degraded_reason: degraded_reasons.length > 0 ? degraded_reasons.join(',') : null,
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
          const unavailableReasons = [
            includePending ? 'no_predictions' : 'no_evaluated_predictions',
            'no_7d_evaluated_predictions',
            'no_30d_evaluated_predictions',
            'insufficient_sample',
          ];
          const coverage = {
            total_predictions: 0,
            evaluated_count: 0,
            pending_count: 0,
          };
          return Response.json({
            message: includePending ? 'No predictions logged yet' : 'No evaluated predictions yet',
            as_of: asIsoDateTime(new Date()),
            coverage,
            coverage_quality: 'INSUFFICIENT',
            minimum_reliable_sample: MINIMUM_RELIABLE_SAMPLE,
            unavailable_reasons: unavailableReasons,
            total_predictions: 0,
            evaluated_count: 0,
            pending_count: 0,
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

        const unavailableReasons: string[] = [];
        if (d7_total === 0) unavailableReasons.push('no_7d_evaluated_predictions');
        if (d30_total === 0) unavailableReasons.push('no_30d_evaluated_predictions');
        if (evaluatedCount === 0) unavailableReasons.push('no_evaluated_predictions');
        if (evaluatedCount < MINIMUM_RELIABLE_SAMPLE) unavailableReasons.push('insufficient_sample');
        const asOfPredictionDate = predictions.results[0]?.prediction_date;
        const asOf = asOfPredictionDate ? `${asOfPredictionDate}T00:00:00.000Z` : asIsoDateTime(new Date());
        const coverage = {
          total_predictions: predictions.results.length,
          evaluated_count: evaluatedCount,
          pending_count: pendingCount,
        };
        const coverageQuality = calibrationQualityForSampleSize(evaluatedCount);

        return Response.json({
          as_of: asOf,
          coverage,
          coverage_quality: coverageQuality,
          minimum_reliable_sample: MINIMUM_RELIABLE_SAMPLE,
          unavailable_reasons: unavailableReasons,
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
          const unavailableReasons = [
            includePending ? 'no_predictions' : 'no_evaluated_predictions',
            'no_7d_evaluated_predictions',
            'no_30d_evaluated_predictions',
            'insufficient_sample',
          ];
          const coverage = {
            total_predictions: 0,
            evaluated_count: 0,
            pending_count: 0,
          };
          return Response.json({
            message: includePending ? 'No ensemble predictions logged yet' : 'No evaluated ensemble predictions yet',
            as_of: asIsoDateTime(new Date()),
            coverage,
            coverage_quality: 'INSUFFICIENT',
            minimum_reliable_sample: MINIMUM_RELIABLE_SAMPLE,
            unavailable_reasons: unavailableReasons,
            total_predictions: 0,
            evaluated_count: 0,
            pending_count: 0,
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

        const unavailableReasons: string[] = [];
        if (metrics.ensemble.d7_total === 0) unavailableReasons.push('no_7d_evaluated_predictions');
        if (metrics.ensemble.d30_total === 0) unavailableReasons.push('no_30d_evaluated_predictions');
        if (evaluatedCount === 0) unavailableReasons.push('no_evaluated_predictions');
        if (evaluatedCount < MINIMUM_RELIABLE_SAMPLE) unavailableReasons.push('insufficient_sample');
        const asOfPredictionDate = predictions.results[0]?.prediction_date;
        const asOf = asOfPredictionDate ? `${asOfPredictionDate}T00:00:00.000Z` : asIsoDateTime(new Date());
        const coverage = {
          total_predictions: predictions.results.length,
          evaluated_count: evaluatedCount,
          pending_count: pendingCount,
        };
        const coverageQuality = calibrationQualityForSampleSize(evaluatedCount);

        return Response.json({
          as_of: asOf,
          coverage,
          coverage_quality: coverageQuality,
          minimum_reliable_sample: MINIMUM_RELIABLE_SAMPLE,
          unavailable_reasons: unavailableReasons,
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
