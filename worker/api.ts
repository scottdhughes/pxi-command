// Cloudflare Worker API for PXI
// Uses D1 (SQLite), Vectorize, and Workers AI
// Includes scheduled cron handler for daily data refresh

interface Env {
  DB: D1Database;
  VECTORIZE: VectorizeIndex;
  AI: Ai;
  ML_MODELS: KVNamespace;
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

      // Database migration - create any missing tables (requires auth)
      if (url.pathname === '/api/migrate' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader || authHeader !== `Bearer ${env.WRITE_API_KEY}`) {
          return Response.json({ error: 'Unauthorized' }, { status: 401, headers: corsHeaders });
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

        // Detect current regime
        const regime = await detectRegime(env.DB, pxi.date);

        // Detect divergences
        const divergence = await detectDivergence(env.DB, pxi.score, regime);

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
        };

        return Response.json(response, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'public, max-age=60',
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

        // Get divergence
        const divergence = await detectDivergence(env.DB, pxi.score, regime);

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
        }, {
          headers: {
            ...corsHeaders,
            'Cache-Control': 'public, max-age=60',
          },
        });
      }

      // AI: Find similar market regimes
      if (url.pathname === '/api/similar' && request.method === 'GET') {
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

          // Get PXI scores and forward returns for similar dates
          const historicalScores = await env.DB.prepare(`
            SELECT p.date, p.score, p.label, p.status,
                   me.forward_return_7d, me.forward_return_30d
            FROM pxi_scores p
            LEFT JOIN market_embeddings me ON p.date = me.date
            WHERE p.date IN (${similarDates.map(() => '?').join(',')})
          `).bind(...similarDates).all<PXIRow & { forward_return_7d: number | null; forward_return_30d: number | null }>();

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
                  d7: hist.forward_return_7d,
                  d30: hist.forward_return_30d,
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

      // Manual refresh - fetch fresh data and recalculate (requires auth)
      if (url.pathname === '/api/refresh' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader || authHeader !== `Bearer ${env.WRITE_API_KEY}`) {
          return Response.json({ error: 'Unauthorized' }, { status: 401, headers: corsHeaders });
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

      // Predict SPY returns based on empirical backtest data
      if (url.pathname === '/api/predict' && request.method === 'GET') {
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
              ? 'PXI in oversold territory - historically bullish setup'
              : isExtremeHigh
              ? 'PXI in overbought territory - expect mean reversion'
              : `PXI in ${bucket} range - typical market conditions`,
          },
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

      // Evaluate past predictions against actual results
      if (url.pathname === '/api/evaluate' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader || authHeader !== `Bearer ${env.WRITE_API_KEY}`) {
          return Response.json({ error: 'Unauthorized' }, { status: 401, headers: corsHeaders });
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
      if (url.pathname === '/api/ml/predict' && request.method === 'GET') {
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
      if (url.pathname === '/api/ml/lstm' && request.method === 'GET') {
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

      // Ensemble prediction endpoint - combines XGBoost and LSTM
      if (url.pathname === '/api/ml/ensemble' && request.method === 'GET') {
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
      if (url.pathname === '/api/accuracy' && request.method === 'GET') {
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
      if (url.pathname === '/api/ml/accuracy' && request.method === 'GET') {
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
      if (url.pathname === '/api/ml/backtest' && request.method === 'GET') {
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
      if (url.pathname === '/api/retrain' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader || authHeader !== `Bearer ${env.WRITE_API_KEY}`) {
          return Response.json({ error: 'Unauthorized' }, { status: 401, headers: corsHeaders });
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
      if (url.pathname === '/api/model' && request.method === 'GET') {
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
      if (url.pathname === '/api/backtest' && request.method === 'GET') {
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
      if (url.pathname === '/api/backtest/signal' && request.method === 'GET') {
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
      if (url.pathname === '/api/recalculate-all-signals' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization');
        const apiKey = authHeader?.replace('Bearer ', '');

        if (!apiKey || apiKey !== (env as any).WRITE_API_KEY) {
          return Response.json({ error: 'Unauthorized' }, { status: 401, headers: corsHeaders });
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
      if (url.pathname === '/api/backtest/history' && request.method === 'GET') {
        const results = await env.DB.prepare(`
          SELECT * FROM backtest_results ORDER BY run_date DESC LIMIT 10
        `).all();

        return Response.json({
          history: results.results || [],
        }, { headers: corsHeaders });
      }

      // Export training data for ML model (requires auth)
      if (url.pathname === '/api/export/training-data' && request.method === 'GET') {
        const authHeader = request.headers.get('Authorization');
        const apiKey = authHeader?.replace('Bearer ', '');
        if (!apiKey || apiKey !== (env as any).WRITE_API_KEY) {
          return Response.json({ error: 'Unauthorized' }, { status: 401, headers: corsHeaders });
        }

        // Get all PXI scores with deltas and forward returns
        const pxiData = await env.DB.prepare(`
          SELECT
            p.date,
            p.score,
            p.delta_1d,
            p.delta_7d,
            p.delta_30d,
            p.label,
            p.status,
            me.forward_return_7d,
            me.forward_return_30d
          FROM pxi_scores p
          LEFT JOIN market_embeddings me ON p.date = me.date
          ORDER BY p.date ASC
        `).all<{
          date: string;
          score: number;
          delta_1d: number | null;
          delta_7d: number | null;
          delta_30d: number | null;
          label: string;
          status: string;
          forward_return_7d: number | null;
          forward_return_30d: number | null;
        }>();

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

        // Build training records
        const trainingData = (pxiData.results || []).map(p => ({
          date: p.date,
          // Target variables
          forward_return_7d: p.forward_return_7d,
          forward_return_30d: p.forward_return_30d,
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
        }));

        return Response.json({
          count: trainingData.length,
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
