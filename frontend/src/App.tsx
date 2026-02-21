import { useEffect, useState, useRef } from 'react'
import './App.css'

type AppRoute = '/' | '/spec' | '/alerts' | '/guide' | '/brief' | '/opportunities' | '/inbox'

interface RouteMeta {
  title: string
  description: string
  canonical: string
  ogTitle: string
  ogDescription: string
  jsonLd: Record<string, unknown>
}

const ROUTE_META: Record<AppRoute, RouteMeta> = {
  '/': {
    title: 'PXI /COMMAND - Macro Market Strength Index',
    description: 'Real-time composite macro indicator synthesizing volatility, credit spreads, market breadth, positioning, and global risk signals into a single 0-100 score.',
    canonical: 'https://pxicommand.com/',
    ogTitle: 'PXI /COMMAND - Macro Market Strength Index',
    ogDescription: 'Real-time composite macro indicator: volatility, credit, breadth, positioning, and global risk signals in a single 0-100 score.',
    jsonLd: {
      '@context': 'https://schema.org',
      '@type': 'WebSite',
      name: 'PXI /COMMAND',
      url: 'https://pxicommand.com/',
      description: 'Real-time macro market strength index',
      potentialAction: {
        '@type': 'SearchAction',
        target: 'https://pxicommand.com/?q={search_term_string}',
        'query-input': 'required name=search_term_string',
      },
    },
  },
  '/spec': {
    title: 'PXI /COMMAND Protocol Specification',
    description: 'PXI methodology, indicators, and risk architecture. Learn the full framework behind the macro market strength index.',
    canonical: 'https://pxicommand.com/spec',
    ogTitle: 'PXI /COMMAND Protocol Specification',
    ogDescription: 'Detailed PXI methodology, category weights, and modeling assumptions for transparency.',
    jsonLd: {
      '@context': 'https://schema.org',
      '@type': 'WebPage',
      name: 'PXI /COMMAND Protocol Specification',
      url: 'https://pxicommand.com/spec',
      description: 'Detailed explanation of PXI indicators, methodology, and backtest assumptions.',
    },
  },
  '/alerts': {
    title: 'PXI /COMMAND Alert History',
    description: 'Historical alert signal log for PXI with forward return attribution and regime-specific accuracy context.',
    canonical: 'https://pxicommand.com/alerts',
    ogTitle: 'PXI /COMMAND Alert History',
    ogDescription: 'Review historical divergence alerts and outcome attribution from the PXI /COMMAND index.',
    jsonLd: {
      '@context': 'https://schema.org',
      '@type': 'WebPage',
      name: 'PXI /COMMAND Alert History',
      url: 'https://pxicommand.com/alerts',
      description: 'Historical divergence alerts and regime-based risk signal archive.',
    },
  },
  '/guide': {
    title: 'PXI /COMMAND Guide',
    description: 'How to interpret PXI score levels, regimes, and allocation guidance for macro and risk environments.',
    canonical: 'https://pxicommand.com/guide',
    ogTitle: 'PXI /COMMAND Guide',
    ogDescription: 'A practical guide to reading PXI scores and regime signals.',
    jsonLd: {
      '@context': 'https://schema.org',
      '@type': 'HowTo',
      name: 'PXI /COMMAND Guide',
      url: 'https://pxicommand.com/guide',
      description: 'How to interpret PXI scores, regimes, and risk guidance.',
      step: [],
    },
  },
  '/brief': {
    title: 'PXI Daily Brief',
    description: 'Daily market brief with regime delta, explainability movers, and data freshness context.',
    canonical: 'https://pxicommand.com/brief',
    ogTitle: 'PXI Daily Brief',
    ogDescription: 'Daily macro brief: what changed, why it matters, and current risk posture.',
    jsonLd: {
      '@context': 'https://schema.org',
      '@type': 'WebPage',
      name: 'PXI Daily Brief',
      url: 'https://pxicommand.com/brief',
      description: 'Daily PXI brief with explainability and freshness context.',
    },
  },
  '/opportunities': {
    title: 'PXI Opportunities',
    description: 'Ranked market opportunities blending PXI, ML, and Signals theme context.',
    canonical: 'https://pxicommand.com/opportunities',
    ogTitle: 'PXI Opportunities',
    ogDescription: 'Deterministic opportunity feed with conviction, rationale, and historical hit-rate context.',
    jsonLd: {
      '@context': 'https://schema.org',
      '@type': 'WebPage',
      name: 'PXI Opportunities',
      url: 'https://pxicommand.com/opportunities',
      description: 'Opportunity feed for 7d and 30d horizons.',
    },
  },
  '/inbox': {
    title: 'PXI Alert Inbox',
    description: 'Market-wide alert feed with severity, event metadata, and email digest subscription.',
    canonical: 'https://pxicommand.com/inbox',
    ogTitle: 'PXI Alert Inbox',
    ogDescription: 'In-app feed for regime changes, threshold events, opportunity spikes, and freshness warnings.',
    jsonLd: {
      '@context': 'https://schema.org',
      '@type': 'WebPage',
      name: 'PXI Alert Inbox',
      url: 'https://pxicommand.com/inbox',
      description: 'In-app market alerts feed for PXI.',
    },
  },
}

function normalizeRoute(pathname: string): AppRoute {
  const path = (pathname || '/').replace(/\/+$/, '')
  if (path === '/spec') return '/spec'
  if (path === '/alerts') return '/alerts'
  if (path === '/guide') return '/guide'
  if (path === '/brief') return '/brief'
  if (path === '/opportunities') return '/opportunities'
  if (path === '/inbox') return '/inbox'
  return '/'
}

function setMeta(name: string, content: string, attr: 'name' | 'property' = 'name') {
  const selector = attr === 'name' ? `meta[name="${name}"]` : `meta[property="${name}"]`
  let tag = document.querySelector(selector) as HTMLMetaElement | null
  if (!tag) {
    tag = document.createElement('meta')
    tag.setAttribute(attr, name)
    document.head.appendChild(tag)
  }
  tag.setAttribute('content', content)
}

function setJsonLd(meta: Record<string, unknown>) {
  let tag = document.getElementById('route-jsonld') as HTMLScriptElement | null
  if (!tag) {
    tag = document.createElement('script')
    tag.setAttribute('type', 'application/ld+json')
    tag.id = 'route-jsonld'
    document.head.appendChild(tag)
  }
  tag.text = JSON.stringify(meta, null, 2)
}

function applyRouteMetadata(route: AppRoute) {
  const meta = ROUTE_META[route]
  document.title = meta.title

  const setCanonical = (href: string) => {
    let tag = document.querySelector('link[rel="canonical"]') as HTMLLinkElement | null
    if (!tag) {
      tag = document.createElement('link')
      tag.setAttribute('rel', 'canonical')
      document.head.appendChild(tag)
    }
    tag.setAttribute('href', href)
  }

  setMeta('description', meta.description, 'name')
  setMeta('og:title', meta.ogTitle, 'property')
  setMeta('og:description', meta.ogDescription, 'property')
  setMeta('og:url', meta.canonical, 'property')
  setMeta('twitter:title', meta.ogTitle, 'name')
  setMeta('twitter:description', meta.ogDescription, 'name')
  setCanonical(meta.canonical)
  setJsonLd(meta.jsonLd)
}

interface RouteFetchPlan {
  poll: boolean
  pxi: boolean
  signal: boolean
  plan: boolean
  prediction: boolean
  ensemble: boolean
  accuracy: boolean
  history: boolean
  alertsHistory: boolean
  brief: boolean
  opportunities: boolean
  inbox: boolean
  signalsDetail: boolean
  similar: boolean
  backtest: boolean
}

const EMPTY_FETCH_PLAN: RouteFetchPlan = {
  poll: false,
  pxi: false,
  signal: false,
  plan: false,
  prediction: false,
  ensemble: false,
  accuracy: false,
  history: false,
  alertsHistory: false,
  brief: false,
  opportunities: false,
  inbox: false,
  signalsDetail: false,
  similar: false,
  backtest: false,
}

function getRouteFetchPlan(route: AppRoute): RouteFetchPlan {
  if (route === '/') {
    return {
      poll: true,
      pxi: true,
      signal: true,
      plan: true,
      prediction: true,
      ensemble: true,
      accuracy: true,
      history: true,
      alertsHistory: true,
      brief: true,
      opportunities: true,
      inbox: true,
      signalsDetail: true,
      similar: true,
      backtest: true,
    }
  }

  if (route === '/alerts') {
    return {
      ...EMPTY_FETCH_PLAN,
      poll: true,
      alertsHistory: true,
    }
  }

  if (route === '/brief') {
    return {
      ...EMPTY_FETCH_PLAN,
      poll: true,
      brief: true,
    }
  }

  if (route === '/opportunities') {
    return {
      ...EMPTY_FETCH_PLAN,
      poll: true,
      opportunities: true,
    }
  }

  if (route === '/inbox') {
    return {
      ...EMPTY_FETCH_PLAN,
      poll: true,
      inbox: true,
    }
  }

  return EMPTY_FETCH_PLAN
}

const API_PRIMARY = 'https://api.pxicommand.com'
const API_ROLLBACK = 'https://pxi-api.novoamorx1.workers.dev'
let pinnedApiBase: string | null = null

function getApiUrlCandidates() {
  const configured = import.meta.env.VITE_API_URL?.trim()
  let candidates: string[]

  if (import.meta.env.DEV) {
    candidates = [configured || 'http://localhost:3000']
  } else if (configured) {
    candidates = configured === API_ROLLBACK ? [configured] : [configured, API_ROLLBACK]
  } else {
    candidates = [API_PRIMARY, API_ROLLBACK]
  }

  if (pinnedApiBase && candidates.includes(pinnedApiBase)) {
    return [pinnedApiBase, ...candidates.filter(c => c !== pinnedApiBase)]
  }
  return candidates
}

async function fetchApi(path: string, init?: RequestInit): Promise<Response> {
  const candidates = getApiUrlCandidates()
  let lastError: unknown

  for (let i = 0; i < candidates.length; i += 1) {
    const base = candidates[i]
    try {
      const response = await fetch(`${base}${path}`, init)
      pinnedApiBase = base
      return response
    } catch (err) {
      lastError = err
      if (i < candidates.length - 1) {
        console.warn(`API host unreachable (${base}), trying fallback`, err)
      }
    }
  }

  throw lastError instanceof Error ? lastError : new Error('API fetch failed')
}

// ============== ML Accuracy Data Interface ==============
// Matches the /api/ml/accuracy response format
interface MLAccuracyApiResponse {
  as_of?: string
  coverage?: {
    total_predictions: number
    evaluated_count: number
    pending_count: number
  }
  coverage_quality?: 'ROBUST' | 'LIMITED' | 'INSUFFICIENT'
  minimum_reliable_sample?: number
  unavailable_reasons?: string[]
  total_predictions: number
  evaluated_count: number
  pending_count: number
  error?: string
  metrics: {
    ensemble: {
      d7: { direction_accuracy: string; mean_absolute_error: string; sample_size: number } | null
      d30: { direction_accuracy: string; mean_absolute_error: string; sample_size: number } | null
    } | null
  } | null
}

// Parsed format for display
interface MLAccuracyData {
  as_of: string | null
  coverage: {
    total_predictions: number
    evaluated_count: number
    pending_count: number
  }
  coverage_quality: 'ROBUST' | 'LIMITED' | 'INSUFFICIENT'
  minimum_reliable_sample: number
  unavailable_reasons: string[]
  rolling_7d: {
    direction_accuracy: number | null
    sample_size: number
    mae: number | null
  }
  rolling_30d: {
    direction_accuracy: number | null
    sample_size: number
    mae: number | null
  }
  all_time: {
    direction_accuracy_7d: number | null
    direction_accuracy_30d: number | null
    total_predictions: number
  }
}

// Parse API response to display format
function parseMLAccuracy(api: MLAccuracyApiResponse): MLAccuracyData | null {
  const parsePercent = (value?: string | null): number | null => {
    if (!value || typeof value !== 'string') return null
    const parsed = parseFloat(value.replace('%', ''))
    return Number.isFinite(parsed) ? parsed : null
  }

  const d7 = api.metrics?.ensemble?.d7 ?? null
  const d30 = api.metrics?.ensemble?.d30 ?? null

  return {
    as_of: api.as_of || null,
    coverage: api.coverage || {
      total_predictions: api.total_predictions || 0,
      evaluated_count: api.evaluated_count || 0,
      pending_count: api.pending_count || 0,
    },
    coverage_quality: api.coverage_quality || 'INSUFFICIENT',
    minimum_reliable_sample: typeof api.minimum_reliable_sample === 'number' ? api.minimum_reliable_sample : 30,
    unavailable_reasons: Array.isArray(api.unavailable_reasons) ? api.unavailable_reasons : [],
    rolling_7d: {
      direction_accuracy: parsePercent(d7?.direction_accuracy),
      sample_size: d7?.sample_size || 0,
      mae: d7 ? parseFloat(d7.mean_absolute_error) : null
    },
    rolling_30d: {
      direction_accuracy: parsePercent(d30?.direction_accuracy),
      sample_size: d30?.sample_size || 0,
      mae: d30 ? parseFloat(d30.mean_absolute_error) : null
    },
    all_time: {
      direction_accuracy_7d: parsePercent(d7?.direction_accuracy),
      direction_accuracy_30d: parsePercent(d30?.direction_accuracy),
      total_predictions: api.total_predictions
    }
  }
}

// ============== History Data Interface ==============
interface HistoryDataPoint {
  date: string
  score: number
  regime?: 'RISK_ON' | 'RISK_OFF' | 'TRANSITION'
}

// ============== Alerts Interface ==============
interface AlertData {
  id: number
  date: string
  alert_type: string
  message: string
  severity: 'info' | 'warning' | 'critical'
  acknowledged: boolean
  pxi_score: number | null
  forward_return_7d: number | null
  forward_return_30d: number | null
}

interface AlertsApiResponse {
  alerts: AlertData[]
  count: number
  filters: {
    types: { type: string; count: number }[]
  }
  accuracy: Record<string, {
    total: number
    accuracy_7d: number | null
    avg_return_7d: number
  }>
}

interface BriefData {
  as_of: string
  summary: string
  regime_delta: 'UNCHANGED' | 'SHIFTED' | 'STRENGTHENED' | 'WEAKENED'
  top_changes: string[]
  risk_posture: 'risk_on' | 'neutral' | 'risk_off'
  policy_state: {
    stance: 'RISK_ON' | 'RISK_OFF' | 'MIXED'
    risk_posture: 'risk_on' | 'neutral' | 'risk_off'
    conflict_state: 'ALIGNED' | 'MIXED' | 'CONFLICT'
    base_signal: string
    regime_context: 'RISK_ON' | 'RISK_OFF' | 'TRANSITION'
    rationale: string
    rationale_codes: string[]
  }
  source_plan_as_of: string
  contract_version: string
  consistency: {
    score: number
    state: 'PASS' | 'WARN' | 'FAIL'
    violations: string[]
  }
  explainability: {
    category_movers: { category: string; score_change: number }[]
    indicator_movers: { indicator_id: string; value_change: number; z_impact: number }[]
  }
  freshness_status: {
    has_stale_data: boolean
    stale_count: number
  }
  updated_at: string
  degraded_reason: string | null
}

interface OpportunityItem {
  id: string
  symbol: string | null
  theme_id: string
  theme_name: string
  direction: 'bullish' | 'bearish' | 'neutral'
  conviction_score: number
  rationale: string
  supporting_factors: string[]
  historical_hit_rate: number
  sample_size: number
  calibration?: {
    probability_correct_direction: number | null
    ci95_low: number | null
    ci95_high: number | null
    sample_size: number
    quality: 'ROBUST' | 'LIMITED' | 'INSUFFICIENT'
    basis: 'conviction_decile'
    window: string | null
    unavailable_reason: string | null
  }
  expectancy?: {
    expected_move_pct: number | null
    max_adverse_move_pct: number | null
    sample_size: number
    basis: 'theme_direction' | 'theme_direction_shrunk_prior' | 'direction_prior_proxy' | 'none'
    quality: 'ROBUST' | 'LIMITED' | 'INSUFFICIENT'
    unavailable_reason: string | null
  }
  eligibility?: {
    passed: boolean
    failed_checks: string[]
  }
  decision_contract?: {
    coherent: boolean
    confidence_band: 'high' | 'medium' | 'low'
    rationale_codes: string[]
  }
  updated_at: string
}

interface OpportunitiesResponse {
  as_of: string
  horizon: '7d' | '30d'
  items: OpportunityItem[]
  suppressed_count: number
  quality_filtered_count?: number
  coherence_suppressed_count?: number
  degraded_reason?: string | null
  suppression_by_reason?: {
    coherence_failed: number
    quality_filtered: number
    data_quality_suppressed: number
  }
  quality_filter_rate?: number
  coherence_fail_rate?: number
  actionability_state?: 'ACTIONABLE' | 'WATCH' | 'NO_ACTION'
  actionability_reason_codes?: string[]
  cta_enabled?: boolean
  cta_disabled_reasons?: string[]
}

interface CalibrationDiagnosticsResponse {
  as_of: string
  metric: 'conviction' | 'edge_quality'
  horizon: '7d' | '30d' | null
  basis: string
  total_samples: number
  bins: Array<{
    bin: string
    correct_count: number
    probability_correct: number | null
    ci95_low: number | null
    ci95_high: number | null
    sample_size: number
    quality: 'ROBUST' | 'LIMITED' | 'INSUFFICIENT'
  }>
  diagnostics: {
    brier_score: number | null
    ece: number | null
    log_loss: number | null
    quality_band: 'ROBUST' | 'LIMITED' | 'INSUFFICIENT'
    minimum_reliable_sample: number
    insufficient_reasons: string[]
  }
}

interface MarketFeedAlert {
  id: string
  event_type: 'regime_change' | 'threshold_cross' | 'opportunity_spike' | 'freshness_warning'
  severity: 'info' | 'warning' | 'critical'
  title: string
  body: string
  entity_type: 'market' | 'theme' | 'indicator'
  entity_id: string | null
  created_at: string
}

interface AlertsFeedResponse {
  as_of: string
  alerts: MarketFeedAlert[]
  degraded_reason?: string | null
}

interface PlanData {
  as_of: string
  setup_summary: string
  actionability_state?: 'ACTIONABLE' | 'WATCH' | 'NO_ACTION'
  actionability_reason_codes?: string[]
  policy_state?: {
    stance: 'RISK_ON' | 'RISK_OFF' | 'MIXED'
    risk_posture: 'risk_on' | 'neutral' | 'risk_off'
    conflict_state: 'ALIGNED' | 'MIXED' | 'CONFLICT'
    base_signal: 'FULL_RISK' | 'REDUCED_RISK' | 'RISK_OFF' | 'DEFENSIVE' | string
    regime_context: 'RISK_ON' | 'RISK_OFF' | 'TRANSITION'
    rationale: string
    rationale_codes: string[]
  }
  action_now: {
    risk_allocation_target: number
    raw_signal_allocation_target: number
    risk_allocation_basis: 'penalized_playbook_target' | 'fallback_neutral'
    horizon_bias: string
    primary_signal: 'FULL_RISK' | 'REDUCED_RISK' | 'RISK_OFF' | 'DEFENSIVE' | string
  }
  edge_quality: {
    score: number
    label: 'HIGH' | 'MEDIUM' | 'LOW'
    breakdown: {
      data_quality: number
      model_agreement: number
      regime_stability: number
    }
    stale_count: number
    ml_sample_size: number
    conflict_state: 'ALIGNED' | 'MIXED' | 'CONFLICT'
    calibration?: {
      bin: string | null
      probability_correct_7d: number | null
      ci95_low_7d: number | null
      ci95_high_7d: number | null
      sample_size_7d: number
      quality: 'ROBUST' | 'LIMITED' | 'INSUFFICIENT'
    }
  }
  risk_band: {
    d7: { bear: number | null; base: number | null; bull: number | null; sample_size: number }
    d30: { bear: number | null; base: number | null; bull: number | null; sample_size: number }
  }
  uncertainty: {
    headline: string | null
    flags: {
      stale_inputs: boolean
      limited_calibration: boolean
      limited_scenario_sample: boolean
    }
  }
  consistency: {
    score: number
    state: 'PASS' | 'WARN' | 'FAIL'
    violations: string[]
    components?: {
      base_score: number
      structural_penalty: number
      reliability_penalty: number
    }
  }
  trader_playbook: {
    recommended_size_pct: { min: number; target: number; max: number }
    scenarios: Array<{ condition: string; action: string; invalidation: string }>
    benchmark_follow_through_7d: {
      hit_rate: number | null
      sample_size: number
      unavailable_reason: string | null
    }
  }
  brief_ref?: {
    as_of: string
    regime_delta: 'UNCHANGED' | 'SHIFTED' | 'STRENGTHENED' | 'WEAKENED'
    risk_posture: 'risk_on' | 'neutral' | 'risk_off'
  }
  opportunity_ref?: {
    as_of: string
    horizon: '7d' | '30d'
    eligible_count: number
    suppressed_count: number
    degraded_reason: string | null
  }
  alerts_ref?: {
    as_of: string
    warning_count_24h: number
    critical_count_24h: number
  }
  invalidation_rules: string[]
  degraded_reason: string | null
}

// ============== Category Details Interface ==============
interface CategoryDetailData {
  category: string
  date: string
  score: number
  weight: number
  percentile_rank: number
  indicators: {
    id: string
    name: string
    raw_value: number
    normalized_value: number
  }[]
  history: {
    date: string
    score: number
  }[]
}

// ============== Signals Theme Interface ==============
interface SignalTheme {
  rank: number
  theme_id: string
  theme_name: string
  score: number
  classification: {
    signal_type: string  // 'Rotation' | 'Momentum' | 'Divergence' | 'Mean Reversion'
    confidence: string   // 'Very High' | 'High' | 'Medium-High' | 'Medium' | 'Medium-Low'
    timing: string       // 'Now' | 'Building' | 'Ongoing' | 'Early'
    stars: number        // 1-5
  }
  key_tickers: string[]
}

interface SignalsData {
  run_id: string
  generated_at_utc: string
  themes: SignalTheme[]
}

interface SignalsRunSummary {
  id: string
  status?: "ok" | "error"
}

// ============== Similar Periods Interface ==============
interface SimilarPeriod {
  date: string
  similarity: number
  weights: {
    combined: number
    similarity: number
    recency: number
    accuracy: number
    accuracy_sample: number
  }
  pxi: {
    date: string
    score: number
    label: string
    status: string
  } | null
  forward_returns: {
    d7: number | null
    d30: number | null
  } | null
}

interface SimilarPeriodsData {
  current_date: string
  similar_periods: SimilarPeriod[]
}

// ============== Backtest Data Interface ==============
interface BacktestData {
  summary: {
    total_observations: number
    with_7d_return: number
    with_30d_return: number
    date_range: {
      start: string
      end: string
    }
  }
  bucket_analysis: {
    bucket: string
    count: number
    avg_return_7d: number | null
    avg_return_30d: number | null
    win_rate_7d: number | null
    win_rate_30d: number | null
  }[]
  extreme_readings?: {
    low_pxi: { count: number; avg_return_30d: number | null; win_rate_30d: number | null }
    high_pxi: { count: number; avg_return_30d: number | null; win_rate_30d: number | null }
  }
}

interface PXIData {
  date: string
  score: number
  label: string
  status: string
  delta: {
    d1: number | null
    d7: number | null
    d30: number | null
  }
  categories: {
    name: string
    score: number
    weight: number
  }[]
  sparkline: {
    date: string
    score: number
  }[]
  regime: {
    type: 'RISK_ON' | 'RISK_OFF' | 'TRANSITION'
    confidence: number
    description: string
  } | null
  divergence: {
    alerts: {
      type: 'PXI_REGIME' | 'PXI_MOMENTUM' | 'REGIME_SHIFT'
      severity: 'LOW' | 'MEDIUM' | 'HIGH'
      title: string
      description: string
      actionable: boolean
      metrics?: {
        historical_frequency: number
        median_return_7d: number | null
        median_return_30d: number | null
        false_positive_rate: number | null
      }
    }[]
  } | null
  // v1.4: Data freshness info
  dataFreshness?: {
    hasStaleData: boolean
    staleCount: number
    staleIndicators: {
      id: string
      lastUpdate: string | null
      daysOld: number | null
    }[]
    topOffenders?: Array<{
      id: string
      lastUpdate: string | null
      daysOld: number | null
      maxAgeDays: number
      chronic: boolean
      owner: 'market_data' | 'macro_data' | 'risk_ops'
      escalation: 'observe' | 'retry_source' | 'escalate_ops'
    }>
    lastRefreshAtUtc?: string | null
    lastRefreshSource?: string
    nextExpectedRefreshAtUtc?: string
    nextExpectedRefreshInMinutes?: number
  }
}

// v1.1: Signal layer data
interface SignalData {
  date: string
  state: {
    score: number
    label: string
    status: string
    delta: { d1: number | null; d7: number | null; d30: number | null }
    categories: { name: string; score: number; weight: number }[]
  }
  signal: {
    type: 'FULL_RISK' | 'REDUCED_RISK' | 'RISK_OFF' | 'DEFENSIVE'
    risk_allocation: number
    volatility_percentile: number | null
    category_dispersion: number
    adjustments: string[]
    conflict_state?: 'ALIGNED' | 'MIXED' | 'CONFLICT'
  }
  regime: {
    type: 'RISK_ON' | 'RISK_OFF' | 'TRANSITION'
    confidence: number
    description: string
  } | null
  divergence: PXIData['divergence']
  edge_quality?: PlanData['edge_quality']
  freshness_status?: {
    has_stale_data: boolean
    stale_count: number
  }
}

interface PredictionData {
  current: {
    date: string
    score: number
    label: string
    bucket: string
  }
  prediction: {
    method: string
    d7: {
      avg_return: number | null
      median_return: number | null
      win_rate: number | null
      sample_size: number
    }
    d30: {
      avg_return: number | null
      median_return: number | null
      win_rate: number | null
      sample_size: number
    }
  }
  extreme_reading: {
    type: 'OVERSOLD' | 'OVERBOUGHT'
    threshold: string
    historical_count: number
    avg_return_7d: number | null
    avg_return_30d: number | null
    win_rate_7d: number | null
    win_rate_30d: number | null
    signal: 'BULLISH' | 'BEARISH'
  } | null
  interpretation: {
    bias: 'BULLISH' | 'BEARISH' | 'NEUTRAL'
    confidence: 'HIGH' | 'MEDIUM' | 'LOW'
    note: string
  }
}

// Ensemble prediction
interface EnsemblePrediction {
  value: number | null
  direction: 'STRONG_UP' | 'UP' | 'FLAT' | 'DOWN' | 'STRONG_DOWN' | null
  confidence: 'HIGH' | 'MEDIUM' | 'LOW' | null
  components: {
    xgboost: number | null
    lstm: number | null
  }
}

interface EnsembleData {
  date: string
  current_score: number
  ensemble: {
    weights: { xgboost: number; lstm: number }
    predictions: {
      pxi_change_7d: EnsemblePrediction
      pxi_change_30d: EnsemblePrediction
    }
  }
  interpretation: {
    d7: { agreement: string | null; note: string }
    d30: { agreement: string | null; note: string }
  }
}

function Sparkline({ data }: { data: { score: number }[] }) {
  if (data.length < 2) return null  // Need at least 2 points to draw a line

  const min = Math.min(...data.map(d => d.score))
  const max = Math.max(...data.map(d => d.score))
  const range = max - min || 1

  const width = 240
  const height = 48
  const padding = 4

  const points = data.map((d, i) => {
    const x = padding + (i / (data.length - 1)) * (width - padding * 2)
    const y = padding + (height - padding * 2) - ((d.score - min) / range) * (height - padding * 2)
    return `${x},${y}`
  }).join(' ')

  const lastPoint = data[data.length - 1]
  const firstPoint = data[0]
  const isUp = lastPoint.score >= firstPoint.score

  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      className="w-48 sm:w-60 h-10 sm:h-12 opacity-70"
      preserveAspectRatio="none"
    >
      <defs>
        <linearGradient id="lineGradient" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor="#949ba5" stopOpacity="0.3" />
          <stop offset="100%" stopColor={isUp ? '#00a3ff' : '#949ba5'} stopOpacity="1" />
        </linearGradient>
      </defs>
      <polyline
        fill="none"
        stroke="url(#lineGradient)"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        points={points}
      />
    </svg>
  )
}

function StatusBadge({ status, label }: { status: string; label: string }) {
  const styles: Record<string, string> = {
    max_pamp: 'bg-[#00a3ff] text-black',
    pamping: 'bg-[#00a3ff]/80 text-black',
    neutral: 'bg-[#949ba5]/20 text-[#f3f3f3] border border-[#26272b]',
    soft: 'bg-[#949ba5]/10 text-[#949ba5] border border-[#26272b]',
    dumping: 'bg-[#26272b] text-[#949ba5]',
  }

  return (
    <span className={`${styles[status] || styles.neutral} px-4 py-1.5 rounded text-[11px] font-medium tracking-[0.1em] uppercase`}>
      {label}
    </span>
  )
}

function RegimeBadge({ regime }: { regime: PXIData['regime'] }) {
  if (!regime) return null

  const styles: Record<string, { bg: string; text: string; icon: string }> = {
    RISK_ON: { bg: 'bg-[#00a3ff]/10 border-[#00a3ff]/30', text: 'text-[#00a3ff]', icon: '↗' },
    RISK_OFF: { bg: 'bg-[#ff6b6b]/10 border-[#ff6b6b]/30', text: 'text-[#ff6b6b]', icon: '↘' },
    TRANSITION: { bg: 'bg-[#f59e0b]/10 border-[#f59e0b]/30', text: 'text-[#f59e0b]', icon: '↔' },
  }

  const style = styles[regime.type] || styles.TRANSITION
  const label = regime.type.replace('_', ' ')

  return (
    <div className={`${style.bg} border rounded px-3 py-1.5 flex items-center gap-2`}>
      <span className={`${style.text} text-sm`}>{style.icon}</span>
      <span className={`${style.text} text-[10px] font-medium tracking-wider uppercase`}>
        {label}
      </span>
      <span className="text-[9px] text-[#949ba5]/50">
        {Math.round(regime.confidence * 100)}%
      </span>
    </div>
  )
}

function formatBand(value: number | null): string {
  return value === null ? '—' : `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`
}

function formatProbability(value: number | null, digits = 0): string {
  if (value === null || Number.isNaN(value)) return '—'
  return `${(value * 100).toFixed(digits)}%`
}

function formatMaybePercent(value: number | null, digits = 1): string {
  if (value === null || Number.isNaN(value)) return '—'
  return `${value >= 0 ? '+' : ''}${value.toFixed(digits)}%`
}

function formatUnavailableReason(reason: string | null | undefined): string {
  if (!reason) return ''
  return reason.replace(/_/g, ' ')
}

function formatOpportunityDegradedReason(reason: string | null | undefined): string {
  if (!reason) return ''
  if (reason === 'suppressed_data_quality') return 'Opportunity feed is suppressed due to critical stale inputs or consistency failure.'
  if (reason === 'coherence_gate_failed') return 'No eligible opportunities (contract gate).'
  if (reason === 'quality_filtered') return 'Low-information opportunities were filtered from this feed.'
  return reason.replace(/_/g, ' ')
}

function formatActionabilityState(state: 'ACTIONABLE' | 'WATCH' | 'NO_ACTION' | null | undefined): string {
  if (!state) return 'watch'
  if (state === 'ACTIONABLE') return 'actionable'
  if (state === 'NO_ACTION') return 'no action'
  return 'watch'
}

function actionabilityClass(state: 'ACTIONABLE' | 'WATCH' | 'NO_ACTION' | null | undefined): string {
  if (state === 'ACTIONABLE') return 'border-[#00c896]/40 text-[#00c896]'
  if (state === 'NO_ACTION') return 'border-[#f59e0b]/40 text-[#f59e0b]'
  return 'border-[#949ba5]/40 text-[#949ba5]'
}

function formatCtaDisabledReason(reason: string): string {
  if (reason === 'no_eligible_opportunities') return 'no eligible opportunities'
  if (reason === 'suppressed_data_quality') return 'suppressed data quality'
  if (reason === 'calibration_quality_not_robust') return 'calibration quality not robust'
  if (reason === 'calibration_ece_unavailable') return 'calibration ECE unavailable'
  if (reason === 'ece_above_threshold') return 'ECE above threshold'
  return reason.replace(/_/g, ' ')
}

function deriveNoActionUnlockConditions(args: {
  actionabilityReasonCodes?: string[]
  ctaDisabledReasons?: string[]
  diagnostics?: CalibrationDiagnosticsResponse | null
}): string[] {
  const reasonCodes = new Set((args.actionabilityReasonCodes || []).filter(Boolean))
  const ctaReasons = new Set((args.ctaDisabledReasons || []).filter(Boolean))
  const hasAny = (...codes: string[]): boolean => codes.some((code) => reasonCodes.has(code) || ctaReasons.has(code) || reasonCodes.has(`cta_${code}`))

  const unlock: string[] = []

  if (hasAny('no_eligible_opportunities', 'opportunity_coherence_gate_failed', 'high_edge_override_no_eligible')) {
    unlock.push('At least one opportunity must pass coherence (p(correct) >= 50% and aligned expectancy sign).')
  }
  if (hasAny('critical_data_quality_block', 'consistency_fail_block', 'suppressed_data_quality', 'opportunity_suppressed_data_quality')) {
    unlock.push('Critical stale inputs must be zero and consistency must remain PASS.')
  }
  if (hasAny('calibration_quality_not_robust')) {
    unlock.push('Calibration quality must be ROBUST for action CTA.')
  }
  if (hasAny('ece_above_threshold')) {
    const eceNow = args.diagnostics?.diagnostics?.ece
    unlock.push(`Calibration ECE must be <= 0.08${typeof eceNow === 'number' ? ` (current ${eceNow.toFixed(3)})` : ''}.`)
  }
  if (hasAny('calibration_ece_unavailable')) {
    unlock.push('Calibration diagnostics must publish a valid ECE estimate.')
  }

  return unlock.length > 0
    ? unlock
    : ['Wait for the next refresh cycle and recheck actionability state.']
}

function calibrationQualityClass(quality: 'ROBUST' | 'LIMITED' | 'INSUFFICIENT'): string {
  if (quality === 'ROBUST') return 'border-[#00c896]/40 text-[#00c896]'
  if (quality === 'LIMITED') return 'border-[#f59e0b]/40 text-[#f59e0b]'
  return 'border-[#ff6b6b]/40 text-[#ff6b6b]'
}

function fallbackEdgeCalibration(score: number): NonNullable<PlanData['edge_quality']['calibration']> {
  const clampedScore = Math.max(0, Math.min(100, Math.round(score)))
  const binStart = clampedScore === 100 ? 90 : Math.floor(clampedScore / 10) * 10
  const binEnd = binStart === 90 ? 100 : binStart + 9
  return {
    bin: `${binStart}-${binEnd}`,
    probability_correct_7d: null,
    ci95_low_7d: null,
    ci95_high_7d: null,
    sample_size_7d: 0,
    quality: 'INSUFFICIENT',
  }
}

function fallbackOpportunityCalibration(): NonNullable<OpportunityItem['calibration']> {
  return {
    probability_correct_direction: null,
    ci95_low: null,
    ci95_high: null,
    sample_size: 0,
    quality: 'INSUFFICIENT',
    basis: 'conviction_decile',
    window: null,
    unavailable_reason: 'insufficient_sample',
  }
}

function fallbackOpportunityExpectancy(): NonNullable<OpportunityItem['expectancy']> {
  return {
    expected_move_pct: null,
    max_adverse_move_pct: null,
    sample_size: 0,
    basis: 'none',
    quality: 'INSUFFICIENT',
    unavailable_reason: 'insufficient_sample',
  }
}

function derivePolicyStance(plan: PlanData): 'RISK_ON' | 'RISK_OFF' | 'MIXED' {
  if (plan.policy_state?.stance) return plan.policy_state.stance

  if (plan.edge_quality.conflict_state === 'CONFLICT') {
    return 'MIXED'
  }

  return (plan.action_now.primary_signal === 'RISK_OFF' || plan.action_now.primary_signal === 'DEFENSIVE')
    ? 'RISK_OFF'
    : 'RISK_ON'
}

function policyStanceClass(stance: 'RISK_ON' | 'RISK_OFF' | 'MIXED'): string {
  if (stance === 'RISK_ON') return 'border-[#00c896]/40 text-[#00c896]'
  if (stance === 'RISK_OFF') return 'border-[#ff6b6b]/40 text-[#ff6b6b]'
  return 'border-[#f59e0b]/40 text-[#f59e0b]'
}

function TodayPlanCard({ plan }: { plan: PlanData | null }) {
  if (!plan) return null

  const policyStance = derivePolicyStance(plan)
  const actionabilityState = plan.actionability_state || (plan.opportunity_ref?.eligible_count === 0 ? 'NO_ACTION' : 'WATCH')
  const actionabilityReasons = (plan.actionability_reason_codes || []).filter(Boolean)
  const noActionUnlockConditions = actionabilityState === 'NO_ACTION'
    ? deriveNoActionUnlockConditions({ actionabilityReasonCodes: actionabilityReasons })
    : []
  const targetPct = Math.round(plan.action_now.risk_allocation_target * 100)
  const rawTargetPct = Math.round((plan.action_now.raw_signal_allocation_target ?? plan.action_now.risk_allocation_target) * 100)
  const qualityColor =
    plan.edge_quality.label === 'HIGH' ? 'text-[#00c896]' :
    plan.edge_quality.label === 'MEDIUM' ? 'text-[#f59e0b]' :
    'text-[#ff6b6b]'

  const conflictColor =
    plan.edge_quality.conflict_state === 'ALIGNED' ? 'text-[#00c896]' :
    plan.edge_quality.conflict_state === 'MIXED' ? 'text-[#f59e0b]' :
    'text-[#ff6b6b]'

  const bars = [
    { label: 'data', value: plan.edge_quality.breakdown.data_quality },
    { label: 'model', value: plan.edge_quality.breakdown.model_agreement },
    { label: 'regime', value: plan.edge_quality.breakdown.regime_stability },
  ]
  const calibration = plan.edge_quality.calibration ?? fallbackEdgeCalibration(plan.edge_quality.score)
  const consistencyClass =
    plan.consistency.state === 'PASS' ? 'border-[#00c896]/40 text-[#00c896]' :
    plan.consistency.state === 'WARN' ? 'border-[#f59e0b]/40 text-[#f59e0b]' :
    'border-[#ff6b6b]/40 text-[#ff6b6b]'
  const shouldShowUncertaintyBanner =
    Boolean(plan.uncertainty?.headline) ||
    Boolean(plan.degraded_reason) ||
    plan.uncertainty?.flags.stale_inputs ||
    plan.uncertainty?.flags.limited_calibration ||
    plan.uncertainty?.flags.limited_scenario_sample
  const opportunitySuppressed = Boolean(
    plan.opportunity_ref?.degraded_reason === 'suppressed_data_quality' ||
    plan.opportunity_ref?.degraded_reason === 'coherence_gate_failed'
  )

  return (
    <section className="w-full mb-6 rounded border border-[#26272b] bg-[#0a0a0a]/80 p-4">
      {shouldShowUncertaintyBanner && (
        <div className="mb-3 rounded border border-[#f59e0b]/40 bg-[#f59e0b]/10 px-3 py-2">
          <p className="text-[9px] uppercase tracking-wider text-[#f59e0b]">Uncertainty</p>
          <p className="mt-1 text-[11px] text-[#f3e3c2]">
            {plan.uncertainty?.headline || plan.degraded_reason?.replace(/,/g, ', ') || 'Signals are in degraded mode.'}
          </p>
        </div>
      )}

      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-[9px] uppercase tracking-[0.25em] text-[#949ba5]">Decision</p>
          <div className="mt-2 flex flex-wrap items-center gap-2 text-[9px] uppercase tracking-wide">
            <span className={`rounded border px-2 py-1 ${policyStanceClass(policyStance)}`}>
              stance {policyStance.replace('_', ' ')}
            </span>
            <span className={`rounded border px-2 py-1 ${actionabilityClass(actionabilityState)}`}>
              {formatActionabilityState(actionabilityState)}
            </span>
            <span className="rounded border border-[#26272b] px-2 py-1 text-[#949ba5]">
              tactical {plan.action_now.primary_signal.replace('_', ' ')}
            </span>
            <span className="rounded border border-[#26272b] px-2 py-1 text-[#d7dbe1]">
              target {targetPct}%
            </span>
          </div>
          {rawTargetPct !== targetPct && (
            <p className="mt-1 text-[9px] text-[#949ba5]/75">
              raw {rawTargetPct}% · {plan.action_now.risk_allocation_basis.replace(/_/g, ' ')}
            </p>
          )}
          <p className="mt-2 text-[12px] leading-relaxed text-[#e4e8ee]">{plan.setup_summary}</p>
          {opportunitySuppressed && (
            <p className="mt-2 text-[10px] text-[#f59e0b]">
              Opportunity feed currently suppressed: {formatOpportunityDegradedReason(plan.opportunity_ref?.degraded_reason)}
            </p>
          )}
          {actionabilityState === 'NO_ACTION' && (
            <div className="mt-2 rounded border border-[#f59e0b]/30 bg-[#f59e0b]/5 px-2 py-2">
              <p className="text-[10px] uppercase tracking-wider text-[#f59e0b]">No-action unlock conditions</p>
              {actionabilityReasons.length > 0 && (
                <p className="mt-1 text-[9px] text-[#f3e3c2]/80">
                  reasons: {actionabilityReasons.slice(0, 3).map((reason) => reason.replace(/_/g, ' ')).join(' · ')}
                </p>
              )}
              <div className="mt-1 space-y-1">
                {noActionUnlockConditions.map((line) => (
                  <p key={line} className="text-[10px] text-[#f3e3c2]">- {line}</p>
                ))}
              </div>
            </div>
          )}
        </div>
        <div className="shrink-0 text-right">
          <p className={`text-[11px] font-medium uppercase tracking-wide ${qualityColor}`}>
            {plan.edge_quality.label}
          </p>
          <p className="text-[10px] text-[#949ba5]">edge {plan.edge_quality.score}</p>
        </div>
      </div>

      <div className="mt-4 rounded border border-[#26272b] px-2 py-2">
        <div className="flex items-center justify-between gap-2">
          <p className="text-[9px] uppercase tracking-wide text-[#949ba5]">Confidence</p>
          <span className={`rounded border px-2 py-0.5 text-[8px] uppercase tracking-wider ${consistencyClass}`}>
            consistency {plan.consistency.state} {plan.consistency.score}
          </span>
        </div>
        <div className="mt-2 flex flex-wrap gap-2 text-[10px]">
          <span className="rounded border border-[#26272b] px-2 py-1 text-[#949ba5]">
            {plan.action_now.horizon_bias.replace(/_/g, ' ')}
          </span>
          <span className={`rounded border border-[#26272b] px-2 py-1 ${conflictColor}`}>
            {plan.edge_quality.conflict_state.toLowerCase()}
          </span>
          <span className={`rounded border px-2 py-1 ${calibrationQualityClass(calibration.quality)}`}>
            calibration {calibration.quality.toLowerCase()}
          </span>
        </div>
        {plan.consistency.components && (
          <p className="mt-2 text-[9px] text-[#949ba5]/70">
            score build: base {plan.consistency.components.base_score} - structural {plan.consistency.components.structural_penalty} - reliability {plan.consistency.components.reliability_penalty}
          </p>
        )}
        <div className="mt-3 grid grid-cols-3 gap-2">
          {bars.map((bar) => (
            <div key={bar.label}>
              <div className="mb-1 flex items-center justify-between text-[9px] uppercase tracking-wide text-[#949ba5]">
                <span>{bar.label}</span>
                <span className="text-[#d7dbe1]">{bar.value}</span>
              </div>
              <div className="h-1.5 rounded bg-[#15161a]">
                <div
                  className="h-1.5 rounded bg-[#00a3ff]"
                  style={{ width: `${Math.max(0, Math.min(100, bar.value))}%` }}
                />
              </div>
            </div>
          ))}
        </div>
        <p className="mt-2 text-[10px] text-[#d7dbe1]">
          p(correct) {formatProbability(calibration.probability_correct_7d)} ·
          {' '}95% CI {formatProbability(calibration.ci95_low_7d)}-{formatProbability(calibration.ci95_high_7d)} ·
          {' '}bin {calibration.bin || 'n/a'} · n={calibration.sample_size_7d}
        </p>
        {calibration.quality !== 'ROBUST' && (
          <p className="mt-1 text-[9px] text-[#f59e0b]">
            Limited calibration sample; size down and prefer faster invalidation checks.
          </p>
        )}
      </div>

      <div className="mt-3 rounded border border-[#26272b] px-2 py-2">
        <p className="text-[9px] uppercase tracking-wide text-[#949ba5]">Why</p>
        <p className="mt-1 text-[10px] text-[#d7dbe1]">
          {plan.policy_state?.rationale ? plan.policy_state.rationale.replace(/_/g, ' ') : 'No rationale available.'}
        </p>
        {plan.policy_state?.rationale_codes?.length ? (
          <div className="mt-2 flex flex-wrap gap-1.5">
            {plan.policy_state.rationale_codes.slice(0, 6).map((code) => (
              <span key={code} className="rounded border border-[#26272b] px-2 py-0.5 text-[8px] uppercase tracking-wider text-[#949ba5]">
                {code.replace(/_/g, ' ')}
              </span>
            ))}
          </div>
        ) : null}
      </div>

      <div className="mt-3 rounded border border-[#26272b] px-2 py-2">
        <p className="text-[9px] uppercase tracking-wide text-[#949ba5]">Risk Limits</p>
        <div className="mt-2 grid grid-cols-2 gap-2 text-[10px]">
          <div className="rounded border border-[#26272b] px-2 py-2">
            <p className="text-[9px] uppercase tracking-wide text-[#949ba5]">7d band</p>
            <p className="text-[#d7dbe1]">
              {formatBand(plan.risk_band.d7.bear)} / {formatBand(plan.risk_band.d7.base)} / {formatBand(plan.risk_band.d7.bull)}
            </p>
            <p className="text-[#949ba5]/70">n={plan.risk_band.d7.sample_size}</p>
          </div>
          <div className="rounded border border-[#26272b] px-2 py-2">
            <p className="text-[9px] uppercase tracking-wide text-[#949ba5]">30d band</p>
            <p className="text-[#d7dbe1]">
              {formatBand(plan.risk_band.d30.bear)} / {formatBand(plan.risk_band.d30.base)} / {formatBand(plan.risk_band.d30.bull)}
            </p>
            <p className="text-[#949ba5]/70">n={plan.risk_band.d30.sample_size}</p>
          </div>
        </div>
        <div className="mt-2 rounded border border-[#26272b] px-2 py-2">
          <p className="text-[9px] uppercase tracking-wide text-[#949ba5]">Sizing Playbook</p>
          <p className="mt-1 text-[10px] text-[#d7dbe1]">
            size range {plan.trader_playbook.recommended_size_pct.min}%-{plan.trader_playbook.recommended_size_pct.max}%
            {' '}· target {plan.trader_playbook.recommended_size_pct.target}%
          </p>
          <p className="mt-1 text-[9px] text-[#949ba5]/70">
            7d follow-through {formatProbability(plan.trader_playbook.benchmark_follow_through_7d.hit_rate)}
            {' '}· n={plan.trader_playbook.benchmark_follow_through_7d.sample_size}
            {plan.trader_playbook.benchmark_follow_through_7d.unavailable_reason
              ? ` · ${formatUnavailableReason(plan.trader_playbook.benchmark_follow_through_7d.unavailable_reason)}`
              : ''}
          </p>
          <div className="mt-2 space-y-1">
            {plan.trader_playbook.scenarios.slice(0, 3).map((scenario) => (
              <div key={`${scenario.condition}-${scenario.action}`} className="text-[9px] text-[#cfd5de]">
                <span className="text-[#949ba5]">if</span> {scenario.condition} <span className="text-[#949ba5]">then</span> {scenario.action}
              </div>
            ))}
          </div>
        </div>
        {plan.invalidation_rules.length > 0 && (
          <div className="mt-2">
            <p className="text-[9px] uppercase tracking-wide text-[#949ba5]">Invalidation</p>
            <ul className="mt-1 space-y-1 text-[10px] text-[#d7dbe1]">
              {plan.invalidation_rules.slice(0, 3).map((rule) => (
                <li key={rule}>• {rule}</li>
              ))}
            </ul>
          </div>
        )}
      </div>

      {plan.consistency.violations.length > 0 && (
        <div className="mt-2 text-[9px] text-[#ff6b6b]">
          Violations: {plan.consistency.violations.join(', ').replace(/_/g, ' ')}
        </div>
      )}
      {plan.degraded_reason && (
        <div className="mt-1 text-[9px] text-[#949ba5]/80">
          degraded: {plan.degraded_reason.replace(/,/g, ', ')}
        </div>
      )}
    </section>
  )
}

// v1.1: Signal indicator component
function SignalIndicator({ signal }: { signal: SignalData['signal'] | null }) {
  if (!signal) return null

  const signalColors: Record<string, string> = {
    'FULL_RISK': '#00c896',
    'REDUCED_RISK': '#f59e0b',
    'RISK_OFF': '#ff6b6b',
    'DEFENSIVE': '#dc2626',
  }

  const signalLabels: Record<string, string> = {
    'FULL_RISK': 'Full Risk',
    'REDUCED_RISK': 'Reduced',
    'RISK_OFF': 'Risk Off',
    'DEFENSIVE': 'Defensive',
  }

  const color = signalColors[signal.type] || '#949ba5'
  const allocationPct = Math.round(signal.risk_allocation * 100)

  return (
    <div className="flex items-center gap-2 sm:gap-4 mb-6">
      <div className="w-20 sm:w-28 shrink-0 text-right">
        <span className="text-[9px] text-[#949ba5]/50 uppercase tracking-widest">Signal</span>
      </div>
      <div className="flex-1 flex items-center gap-3">
        <div
          className="bg-[#0a0a0a]/80 backdrop-blur-sm rounded px-3 py-2"
          style={{ borderLeft: `2px solid ${color}` }}
        >
          <div className="flex items-center gap-3">
            <span
              className="text-[11px] font-medium uppercase tracking-wide"
              style={{ color }}
            >
              {signalLabels[signal.type]}
            </span>
            <span className="text-[11px] text-[#f3f3f3]/80 font-mono">
              {allocationPct}%
            </span>
            {signal.conflict_state && (
              <span className={`text-[8px] uppercase tracking-widest ${
                signal.conflict_state === 'ALIGNED'
                  ? 'text-[#00c896]'
                  : signal.conflict_state === 'MIXED'
                    ? 'text-[#f59e0b]'
                    : 'text-[#ff6b6b]'
              }`}>
                {signal.conflict_state.toLowerCase()}
              </span>
            )}
          </div>
          {signal.adjustments.length > 0 && (
            <p className="text-[9px] text-[#949ba5]/50 mt-1">
              {signal.adjustments.join(' · ')}
            </p>
          )}
        </div>
        {signal.volatility_percentile !== null && (
          <span className="text-[9px] text-[#949ba5]/40">
            Vol: {signal.volatility_percentile}th pct
          </span>
        )}
      </div>
      <div className="w-6 sm:w-8 shrink-0" />
    </div>
  )
}

function DivergenceAlerts({ divergence }: { divergence: PXIData['divergence'] }) {
  if (!divergence || divergence.alerts.length === 0) return null

  const severityColors: Record<string, string> = {
    HIGH: '#ff6b6b',
    MEDIUM: '#f59e0b',
    LOW: '#949ba5',
  }

  return (
    <div className="w-full mt-6 space-y-3">
      {divergence.alerts.map((alert, i) => {
        const color = severityColors[alert.severity] || severityColors.LOW
        return (
          <div key={i} className="flex items-start gap-2 sm:gap-4">
            <div className="w-20 sm:w-28 shrink-0" />
            <div
              className="flex-1 bg-[#0a0a0a]/80 backdrop-blur-sm rounded px-3 py-2.5"
              style={{ borderLeft: `2px solid ${color}` }}
            >
              <div className="flex items-center justify-between gap-4">
                <span
                  className="text-[10px] font-medium uppercase tracking-wider"
                  style={{ color }}
                >
                  {alert.title}
                </span>
                {alert.actionable && (
                  <span className="text-[8px] text-[#949ba5]/50 uppercase tracking-widest shrink-0">
                    Actionable
                  </span>
                )}
              </div>
              <p className="text-[10px] text-[#949ba5]/60 leading-relaxed mt-1">
                {alert.description}
              </p>
              {alert.metrics && alert.metrics.historical_frequency > 0 && (
                <p className="text-[8px] text-[#949ba5]/40 mt-1">
                  Historically occurred {alert.metrics.historical_frequency.toFixed(1)}% of days
                </p>
              )}
            </div>
            <div className="w-6 sm:w-8 shrink-0" />
          </div>
        )
      })}
    </div>
  )
}

function CategoryBar({
  name,
  score,
  onClick
}: {
  name: string
  score: number
  onClick?: () => void
}) {
  const isHigh = score >= 70
  const displayName = name.replace(/_/g, ' ')

  return (
    <button
      onClick={onClick}
      className="flex items-center gap-2 sm:gap-4 w-full text-left hover:bg-[#0a0a0a]/40 rounded px-1 py-0.5 -mx-1 transition-colors group"
    >
      <span className="w-20 sm:w-28 text-right text-[#949ba5] text-[11px] sm:text-[13px] tracking-wide capitalize group-hover:text-[#f3f3f3] transition-colors">
        {displayName}
      </span>
      <div className="flex-1 h-[3px] bg-[#26272b] rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all duration-700 ease-out ${
            isHigh ? 'bg-[#00a3ff]' : 'bg-[#949ba5]/50'
          }`}
          style={{ width: `${score}%` }}
        />
      </div>
      <span className="w-6 sm:w-8 text-right font-mono text-[11px] sm:text-[12px] text-[#949ba5]">
        {Math.round(score)}
      </span>
      <span className="text-[#949ba5]/30 text-[10px] opacity-0 group-hover:opacity-100 transition-opacity">
        ›
      </span>
    </button>
  )
}

function PredictionCard({ prediction }: { prediction: PredictionData }) {
  const { d7, d30 } = prediction.prediction
  const extreme = prediction.extreme_reading
  const { bias, confidence, note } = prediction.interpretation

  const formatReturn = (val: number | null) => {
    if (val === null) return '—'
    return `${val >= 0 ? '+' : ''}${val.toFixed(2)}%`
  }

  const formatWinRate = (val: number | null) => {
    if (val === null) return '—'
    return `${Math.round(val)}%`
  }

  const biasColor = bias === 'BULLISH' ? 'text-[#00a3ff]' : bias === 'BEARISH' ? 'text-[#ff6b6b]' : 'text-[#949ba5]'

  // Map technical bias terms to clearer outlook labels
  const outlookLabel = bias === 'BULLISH' ? 'Favorable' : bias === 'BEARISH' ? 'Unfavorable' : 'Mixed'
  const extremeLabel = (signal: string) => signal === 'BULLISH' ? 'Favorable Setup' : 'Caution'

  return (
    <div className="w-full mt-6 sm:mt-10">
      {/* Extreme reading alert */}
      {extreme && (
        <div className={`mb-4 px-4 py-2 rounded text-center ${
          extreme.signal === 'BULLISH' ? 'bg-[#00a3ff]/10 border border-[#00a3ff]/30' : 'bg-[#ff6b6b]/10 border border-[#ff6b6b]/30'
        }`}>
          <div className={`text-[11px] font-medium uppercase tracking-wider ${
            extreme.signal === 'BULLISH' ? 'text-[#00a3ff]' : 'text-[#ff6b6b]'
          }`}>
            {extreme.type} — {extremeLabel(extreme.signal)}
          </div>
          <div className="text-[9px] text-[#949ba5]/50 mt-1">
            {extreme.historical_count} similar readings → {formatWinRate(extreme.win_rate_30d)} win rate
          </div>
        </div>
      )}

      <div className="text-[10px] sm:text-[11px] text-[#949ba5]/50 uppercase tracking-widest mb-4 text-center">
        Historical Outlook
      </div>

      {/* Main stats */}
      <div className="flex justify-center gap-8 sm:gap-12">
        <div className="text-center">
          <div className="text-[10px] text-[#949ba5]/50 uppercase tracking-wider mb-2">7 Day</div>
          <div className={`text-2xl sm:text-3xl font-light ${
            (d7.avg_return ?? 0) >= 0 ? 'text-[#00a3ff]' : 'text-[#949ba5]'
          }`}>
            {formatReturn(d7.avg_return)}
          </div>
          <div className="text-[10px] text-[#949ba5]/60 mt-1">
            avg return
          </div>
          <div className="text-[13px] font-mono text-[#f3f3f3]/80 mt-2">
            {formatWinRate(d7.win_rate)}
          </div>
          <div className="text-[9px] text-[#949ba5]/40">
            win rate
          </div>
        </div>

        <div className="text-center">
          <div className="text-[10px] text-[#949ba5]/50 uppercase tracking-wider mb-2">30 Day</div>
          <div className={`text-2xl sm:text-3xl font-light ${
            (d30.avg_return ?? 0) >= 0 ? 'text-[#00a3ff]' : 'text-[#949ba5]'
          }`}>
            {formatReturn(d30.avg_return)}
          </div>
          <div className="text-[10px] text-[#949ba5]/60 mt-1">
            avg return
          </div>
          <div className="text-[13px] font-mono text-[#f3f3f3]/80 mt-2">
            {formatWinRate(d30.win_rate)}
          </div>
          <div className="text-[9px] text-[#949ba5]/40">
            win rate
          </div>
        </div>
      </div>

      {/* Interpretation */}
      <div className="mt-6 text-center">
        <div className="text-[9px] text-[#949ba5]/50 uppercase tracking-widest mb-1">
          Forward Outlook
        </div>
        <div className={`text-[11px] font-medium uppercase tracking-wider ${biasColor}`}>
          {outlookLabel}
        </div>
        <div className="text-[9px] text-[#949ba5]/40 mt-2">
          {note}
        </div>
        <div className="text-[8px] text-[#949ba5]/30 mt-1">
          {d7.sample_size} observations at similar levels • {confidence.toLowerCase()} confidence
        </div>
      </div>
    </div>
  )
}

// ============== Onboarding / Guide ==============
function OnboardingModal({
  onClose,
  inPage = false,
  exampleScore,
}: {
  onClose: () => void;
  inPage?: boolean;
  exampleScore?: number;
}) {
  const [step, setStep] = useState(0)
  const hasLiveScore = typeof exampleScore === 'number' && Number.isFinite(exampleScore)
  const displayScore = hasLiveScore ? Math.round(exampleScore) : 53
  const scoreLabel = hasLiveScore ? 'Live Score' : 'Example Score'

  const steps = [
    {
      title: 'Welcome to PXI',
      content: (
        <div className="space-y-4">
          <p className="text-[13px] text-[#949ba5] leading-relaxed">
            PXI (Pamp Index) is a composite indicator measuring <span className="text-[#f3f3f3]">macro market strength</span> across seven dimensions.
          </p>
          <div className="bg-[#0a0a0a] rounded-lg p-4 border border-[#26272b]">
            <div className="text-center">
              <div className="text-6xl font-extralight text-[#f3f3f3] mb-2">{displayScore}</div>
              <div className="text-[10px] text-[#949ba5]/60 uppercase tracking-widest">{scoreLabel}</div>
            </div>
          </div>
          <p className="text-[11px] text-[#949ba5]/70">
            The score ranges from 0-100, synthesizing volatility, credit, breadth, positioning, macro, global, and crypto signals.
          </p>
        </div>
      )
    },
    {
      title: 'Score Interpretation',
      content: (
        <div className="space-y-3">
          <p className="text-[12px] text-[#949ba5] mb-4">
            Higher scores indicate stronger risk-on conditions. Lower scores suggest caution.
          </p>
          <div className="space-y-2">
            <div className="flex items-center gap-3 bg-[#0a0a0a] rounded px-3 py-2 border-l-2 border-[#ff6b6b]">
              <div className="w-16 text-[11px] font-mono text-[#ff6b6b]">0–30</div>
              <div className="flex-1">
                <div className="text-[11px] text-[#f3f3f3]">Weak / Dumping</div>
                <div className="text-[9px] text-[#949ba5]/60">Historically favorable for forward returns (mean reversion)</div>
              </div>
            </div>
            <div className="flex items-center gap-3 bg-[#0a0a0a] rounded px-3 py-2 border-l-2 border-[#949ba5]">
              <div className="w-16 text-[11px] font-mono text-[#949ba5]">30–60</div>
              <div className="flex-1">
                <div className="text-[11px] text-[#f3f3f3]">Neutral / Soft</div>
                <div className="text-[9px] text-[#949ba5]/60">Typical market conditions, no strong directional bias</div>
              </div>
            </div>
            <div className="flex items-center gap-3 bg-[#0a0a0a] rounded px-3 py-2 border-l-2 border-[#00a3ff]">
              <div className="w-16 text-[11px] font-mono text-[#00a3ff]">60–80</div>
              <div className="flex-1">
                <div className="text-[11px] text-[#f3f3f3]">Strong / Pamping</div>
                <div className="text-[9px] text-[#949ba5]/60">Risk-on conditions, favorable environment</div>
              </div>
            </div>
            <div className="flex items-center gap-3 bg-[#0a0a0a] rounded px-3 py-2 border-l-2 border-[#f59e0b]">
              <div className="w-16 text-[11px] font-mono text-[#f59e0b]">80–100</div>
              <div className="flex-1">
                <div className="text-[11px] text-[#f3f3f3]">Extended / Max Pamp</div>
                <div className="text-[9px] text-[#949ba5]/60">Historically poor forward returns, elevated reversal risk</div>
              </div>
            </div>
          </div>
        </div>
      )
    },
    {
      title: 'Market Regimes',
      content: (
        <div className="space-y-4">
          <p className="text-[12px] text-[#949ba5]">
            Market regime is classified using a voting system across key indicators.
          </p>
          <div className="space-y-2">
            <div className="flex items-center gap-3 bg-[#00a3ff]/10 border border-[#00a3ff]/30 rounded px-3 py-2">
              <span className="text-[#00a3ff] text-lg">↗</span>
              <div>
                <div className="text-[11px] text-[#00a3ff] uppercase tracking-wide">Risk On</div>
                <div className="text-[9px] text-[#949ba5]/60">Favorable for equities, credit tight, volatility low</div>
              </div>
            </div>
            <div className="flex items-center gap-3 bg-[#f59e0b]/10 border border-[#f59e0b]/30 rounded px-3 py-2">
              <span className="text-[#f59e0b] text-lg">↔</span>
              <div>
                <div className="text-[11px] text-[#f59e0b] uppercase tracking-wide">Transition</div>
                <div className="text-[9px] text-[#949ba5]/60">Mixed signals, regime unclear or shifting</div>
              </div>
            </div>
            <div className="flex items-center gap-3 bg-[#ff6b6b]/10 border border-[#ff6b6b]/30 rounded px-3 py-2">
              <span className="text-[#ff6b6b] text-lg">↘</span>
              <div>
                <div className="text-[11px] text-[#ff6b6b] uppercase tracking-wide">Risk Off</div>
                <div className="text-[9px] text-[#949ba5]/60">Defensive positioning recommended, stress signals</div>
              </div>
            </div>
          </div>
        </div>
      )
    },
    {
      title: 'Categories & Signals',
      content: (
        <div className="space-y-4">
          <p className="text-[12px] text-[#949ba5]">
            PXI aggregates 7 category scores, each with specific weights:
          </p>
          <div className="grid grid-cols-2 gap-2 text-[10px]">
            {[
              { name: 'Credit', weight: '20%', desc: 'HY/IG spreads, curve' },
              { name: 'Volatility', weight: '20%', desc: 'VIX, term structure' },
              { name: 'Breadth', weight: '15%', desc: 'RSP/SPY, sectors' },
              { name: 'Positioning', weight: '15%', desc: 'Fed, TGA, RRP' },
              { name: 'Macro', weight: '10%', desc: 'ISM, claims' },
              { name: 'Global', weight: '10%', desc: 'DXY, EM spreads' },
              { name: 'Crypto', weight: '10%', desc: 'BTC, stables' },
            ].map(cat => (
              <div key={cat.name} className="bg-[#0a0a0a] rounded px-2 py-1.5">
                <div className="flex justify-between">
                  <span className="text-[#f3f3f3]">{cat.name}</span>
                  <span className="text-[#00a3ff]">{cat.weight}</span>
                </div>
                <div className="text-[8px] text-[#949ba5]/50">{cat.desc}</div>
              </div>
            ))}
          </div>
          <p className="text-[9px] text-[#949ba5]/50 text-center">
            See /spec for full methodology and backtest results
          </p>
        </div>
      )
    }
  ]

  const header = inPage ? (
    <div className="sticky top-0 bg-black/95 backdrop-blur border-b border-[#26272b]">
      <div className="max-w-2xl mx-auto px-4 py-4 flex justify-between items-center">
        <h1 className="text-[10px] sm:text-[11px] text-[#949ba5] font-mono uppercase tracking-[0.3em]">PXI/guide</h1>
        <button onClick={onClose} className="text-[#949ba5] text-[10px] uppercase tracking-widest">
          Exit
        </button>
      </div>
    </div>
  ) : null

  return (
    <div className={inPage ? 'min-h-screen bg-black text-[#f3f3f3] overflow-y-auto' : 'fixed inset-0 z-[100] flex items-center justify-center p-4'}>
      {!inPage ? (
        <>
          {/* Backdrop */}
          <div
            className="absolute inset-0 bg-black/90 backdrop-blur-sm"
            onClick={onClose}
          />

          {/* Modal */}
          <div className="relative bg-[#0a0a0a] border border-[#26272b] rounded-lg max-w-md w-full overflow-hidden shadow-2xl">
            {header}
            <div className="px-6 py-4">
              <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-3">
                {steps[step].title}
              </h2>
              <div className="min-h-[280px]">{steps[step].content}</div>
            </div>

            <div className="flex justify-between items-center px-6 py-4 border-t border-[#26272b]">
              <button
                onClick={() => step > 0 && setStep(step - 1)}
                className={`text-[10px] uppercase tracking-widest transition-colors ${
                  step > 0 ? 'text-[#949ba5] hover:text-[#f3f3f3]' : 'text-transparent pointer-events-none'
                }`}
              >
                Back
              </button>
              {step < steps.length - 1 ? (
                <button
                  onClick={() => setStep(step + 1)}
                  className="text-[10px] uppercase tracking-widest text-[#00a3ff] hover:text-[#f3f3f3] transition-colors"
                >
                  Next
                </button>
              ) : (
                <button
                  onClick={onClose}
                  className="text-[10px] uppercase tracking-widest bg-[#00a3ff] text-black px-4 py-1.5 rounded hover:bg-[#00a3ff]/80 transition-colors"
                >
                  Get Started
                </button>
              )}
            </div>
          </div>
        </>
      ) : (
        <div className="min-h-screen bg-black/95">
          {header}
          <div className="max-w-2xl mx-auto px-4 py-8 sm:py-12">
            <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">
              {steps[step].title}
            </h2>
            <div className="min-h-[300px]">{steps[step].content}</div>

            <div className="mt-8 flex justify-between items-center border-t border-[#26272b] pt-4">
              <button
                onClick={() => step > 0 && setStep(step - 1)}
                className={`text-[10px] uppercase tracking-widest transition-colors ${
                  step > 0 ? 'text-[#949ba5] hover:text-[#f3f3f3]' : 'text-transparent pointer-events-none'
                }`}
              >
                Back
              </button>
              {step < steps.length - 1 ? (
                <button
                  onClick={() => setStep(step + 1)}
                  className="text-[10px] uppercase tracking-widest text-[#00a3ff] hover:text-[#f3f3f3] transition-colors"
                >
                  Next
                </button>
              ) : (
                <button
                  onClick={onClose}
                  className="text-[10px] uppercase tracking-widest bg-[#00a3ff] text-black px-4 py-1.5 rounded hover:bg-[#00a3ff]/80 transition-colors"
                >
                  Exit
                </button>
              )}
            </div>
          </div>
        </div>
      )}
      {/* Progress dots */}
      <div className={inPage ? 'max-w-2xl mx-auto px-4 pb-4 flex justify-center gap-2' : 'absolute top-6 left-1/2 -translate-x-1/2 flex justify-center gap-2 pt-4'}>
        {steps.map((_, i) => (
          <button
            key={i}
            onClick={() => setStep(i)}
            className={`w-1.5 h-1.5 rounded-full transition-all ${
              i === step ? 'bg-[#00a3ff] w-4' : 'bg-[#26272b] hover:bg-[#949ba5]/30'
            }`}
          />
        ))}
      </div>
    </div>
  )
}

// ============== Tooltip Component ==============
function Tooltip({ children, content }: { children: React.ReactNode; content: string }) {
  const [show, setShow] = useState(false)

  return (
    <span
      className="relative inline-flex items-center cursor-help"
      onMouseEnter={() => setShow(true)}
      onMouseLeave={() => setShow(false)}
    >
      {children}
      <span className="ml-1 text-[#949ba5]/40 text-[8px]">ⓘ</span>
      {show && (
        <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-3 py-2 bg-[#1a1a1a] border border-[#26272b] rounded text-[10px] text-[#949ba5] whitespace-nowrap z-50 shadow-lg">
          {content}
          <span className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-[#1a1a1a]" />
        </span>
      )}
    </span>
  )
}

// ============== Historical Chart Component ==============
function HistoricalChart({
  data,
  range,
  onRangeChange
}: {
  data: HistoryDataPoint[]
  range: '7d' | '30d' | '90d'
  onRangeChange: (range: '7d' | '30d' | '90d') => void
}) {
  const [hoveredPoint, setHoveredPoint] = useState<HistoryDataPoint | null>(null)
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null)

  if (!data || data.length < 2) return null  // Need at least 2 points

  const rangeMap = { '7d': 7, '30d': 30, '90d': 90 }
  const displayData = data.slice(-rangeMap[range])

  if (displayData.length < 2) return null  // Guard after slicing

  const min = Math.min(...displayData.map(d => d.score))
  const max = Math.max(...displayData.map(d => d.score))
  const range_ = max - min || 1

  const width = 100
  const height = 100
  const padding = { top: 8, right: 4, bottom: 20, left: 4 }
  const chartWidth = width - padding.left - padding.right
  const chartHeight = height - padding.top - padding.bottom

  const getRegimeColor = (regime?: string) => {
    switch(regime) {
      case 'RISK_ON': return '#00a3ff'
      case 'RISK_OFF': return '#ff6b6b'
      case 'TRANSITION': return '#f59e0b'
      default: return '#949ba5'
    }
  }

  // Create path with gradient stops
  const points = displayData.map((d, i) => {
    const x = padding.left + (i / (displayData.length - 1)) * chartWidth
    const y = padding.top + chartHeight - ((d.score - min) / range_) * chartHeight
    return { x, y, data: d }
  })

  const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x},${p.y}`).join(' ')

  // Area path for gradient fill
  const areaD = `${pathD} L${points[points.length - 1].x},${height - padding.bottom} L${points[0].x},${height - padding.bottom} Z`

  return (
    <div className="w-full mt-8">
      <div className="flex justify-between items-center mb-4">
        <div className="text-[10px] text-[#949ba5]/50 uppercase tracking-widest">
          Historical Trend
        </div>
        <div className="flex gap-1">
          {(['7d', '30d', '90d'] as const).map(r => (
            <button
              key={r}
              onClick={() => onRangeChange(r)}
              className={`px-2 py-0.5 text-[9px] uppercase tracking-wider rounded transition-all ${
                range === r
                  ? 'bg-[#00a3ff]/20 text-[#00a3ff] border border-[#00a3ff]/30'
                  : 'text-[#949ba5]/50 hover:text-[#949ba5] border border-transparent'
              }`}
            >
              {r}
            </button>
          ))}
        </div>
      </div>

      <div className="relative bg-[#0a0a0a]/50 rounded-lg border border-[#1a1a1a] p-3">
        <svg
          viewBox={`0 0 ${width} ${height}`}
          className="w-full h-32"
          preserveAspectRatio="none"
          onMouseLeave={() => {
            setHoveredPoint(null)
            setHoveredIndex(null)
          }}
        >
          <defs>
            {/* Gradient for line */}
            <linearGradient id="historyGradient" x1="0%" y1="0%" x2="100%" y2="0%">
              {displayData.map((d, i) => (
                <stop
                  key={i}
                  offset={`${(i / (displayData.length - 1)) * 100}%`}
                  stopColor={getRegimeColor(d.regime)}
                />
              ))}
            </linearGradient>

            {/* Area fill gradient */}
            <linearGradient id="areaGradient" x1="0%" y1="0%" x2="0%" y2="100%">
              <stop offset="0%" stopColor="#00a3ff" stopOpacity="0.15" />
              <stop offset="100%" stopColor="#00a3ff" stopOpacity="0" />
            </linearGradient>
          </defs>

          {/* Grid lines */}
          {[25, 50, 75].map(v => {
            const y = padding.top + chartHeight - ((v - min) / range_) * chartHeight
            if (y < padding.top || y > height - padding.bottom) return null
            return (
              <line
                key={v}
                x1={padding.left}
                y1={y}
                x2={width - padding.right}
                y2={y}
                stroke="#26272b"
                strokeWidth="0.5"
                strokeDasharray="2,2"
              />
            )
          })}

          {/* Area fill */}
          <path
            d={areaD}
            fill="url(#areaGradient)"
          />

          {/* Main line */}
          <path
            d={pathD}
            fill="none"
            stroke="url(#historyGradient)"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />

          {/* Interactive points */}
          {points.map((p, i) => (
            <g key={i}>
              {/* Invisible hitbox */}
              <rect
                x={p.x - chartWidth / displayData.length / 2}
                y={padding.top}
                width={chartWidth / displayData.length}
                height={chartHeight}
                fill="transparent"
                onMouseEnter={() => {
                  setHoveredPoint(p.data)
                  setHoveredIndex(i)
                }}
              />
              {/* Visible dot on hover */}
              {hoveredIndex === i && (
                <>
                  <line
                    x1={p.x}
                    y1={padding.top}
                    x2={p.x}
                    y2={height - padding.bottom}
                    stroke="#26272b"
                    strokeWidth="1"
                  />
                  <circle
                    cx={p.x}
                    cy={p.y}
                    r="3"
                    fill={getRegimeColor(p.data.regime)}
                    stroke="#0a0a0a"
                    strokeWidth="1.5"
                  />
                </>
              )}
            </g>
          ))}
        </svg>

        {/* Hover tooltip */}
        {hoveredPoint && hoveredIndex !== null && (
          <div
            className="absolute top-2 left-1/2 -translate-x-1/2 bg-[#1a1a1a] border border-[#26272b] rounded px-3 py-2 text-center shadow-lg pointer-events-none z-10"
          >
            <div className="text-[9px] text-[#949ba5]/60 mb-1">
              {new Date(hoveredPoint.date + 'T12:00:00').toLocaleDateString('en-US', {
                month: 'short',
                day: 'numeric',
                year: displayData.length > 60 ? '2-digit' : undefined
              })}
            </div>
            <div className="text-lg font-light text-[#f3f3f3]">
              {Math.round(hoveredPoint.score)}
            </div>
            {hoveredPoint.regime && (
              <div
                className="text-[8px] uppercase tracking-wider mt-1"
                style={{ color: getRegimeColor(hoveredPoint.regime) }}
              >
                {hoveredPoint.regime.replace('_', ' ')}
              </div>
            )}
          </div>
        )}

        {/* Stats bar */}
        <div className="flex justify-between mt-3 pt-3 border-t border-[#1a1a1a]">
          <div className="text-center">
            <div className="text-[9px] text-[#949ba5]/40 uppercase tracking-wider">Low</div>
            <div className="text-[11px] font-mono text-[#949ba5]">{Math.round(min)}</div>
          </div>
          <div className="text-center">
            <div className="text-[9px] text-[#949ba5]/40 uppercase tracking-wider">Avg</div>
            <div className="text-[11px] font-mono text-[#949ba5]">
              {Math.round(displayData.reduce((a, b) => a + b.score, 0) / displayData.length)}
            </div>
          </div>
          <div className="text-center">
            <div className="text-[9px] text-[#949ba5]/40 uppercase tracking-wider">High</div>
            <div className="text-[11px] font-mono text-[#949ba5]">{Math.round(max)}</div>
          </div>
          <div className="text-center">
            <div className="text-[9px] text-[#949ba5]/40 uppercase tracking-wider">Change</div>
            <div className={`text-[11px] font-mono ${
              displayData[displayData.length - 1].score >= displayData[0].score
                ? 'text-[#00a3ff]'
                : 'text-[#ff6b6b]'
            }`}>
              {displayData[displayData.length - 1].score >= displayData[0].score ? '+' : ''}
              {(displayData[displayData.length - 1].score - displayData[0].score).toFixed(1)}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

// ============== Stale Data Warning ==============
function StaleDataWarning({ freshness }: { freshness: PXIData['dataFreshness'] }) {
  const [expanded, setExpanded] = useState(false)

  if (!freshness) return null

  const topOffenders = freshness.topOffenders || []
  const hasOffenders = freshness.hasStaleData || topOffenders.length > 0

  if (!hasOffenders) return null

  return (
    <div className="w-full mt-4">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-center gap-2 py-2 px-4 bg-[#f59e0b]/10 border border-[#f59e0b]/30 rounded text-[10px] text-[#f59e0b] uppercase tracking-wider hover:bg-[#f59e0b]/20 transition-colors"
      >
        <span>operator panel</span>
        <span>{freshness.staleCount} stale input{freshness.staleCount === 1 ? '' : 's'}</span>
        <span className="text-[#f59e0b]/50">{expanded ? '▲' : '▼'}</span>
      </button>
      {expanded && (
        <div className="mt-2 p-3 bg-[#0a0a0a]/60 border border-[#26272b] rounded">
          <div className="flex flex-wrap gap-3 text-[9px] text-[#949ba5]/70 uppercase tracking-wider mb-3">
            <div>
              last refresh {freshness.lastRefreshAtUtc ? new Date(freshness.lastRefreshAtUtc).toLocaleString() : 'unknown'}
              {freshness.lastRefreshSource ? ` (${freshness.lastRefreshSource.replace(/_/g, ' ')})` : ''}
            </div>
            <div>
              next refresh {freshness.nextExpectedRefreshAtUtc ? new Date(freshness.nextExpectedRefreshAtUtc).toLocaleString() : 'unknown'}
              {typeof freshness.nextExpectedRefreshInMinutes === 'number' ? ` (${freshness.nextExpectedRefreshInMinutes}m)` : ''}
            </div>
          </div>
          <div className="space-y-2">
            {(topOffenders.length > 0 ? topOffenders : freshness.staleIndicators.slice(0, 3).map((s) => ({
              id: s.id,
              lastUpdate: s.lastUpdate,
              daysOld: s.daysOld,
              maxAgeDays: 0,
              chronic: false,
              owner: 'market_data' as const,
              escalation: 'observe' as const,
            }))).map((ind) => (
              <div key={ind.id} className="border border-[#26272b] rounded px-2 py-2">
                <div className="flex justify-between items-center gap-2 text-[10px]">
                  <span className="text-[#d7dbe1]">{ind.id.replace(/_/g, ' ')}</span>
                  <span className="text-[#f59e0b]/80">
                    {ind.daysOld === null ? 'unknown age' : `${ind.daysOld}d old`}
                    {ind.maxAgeDays > 0 ? ` / max ${ind.maxAgeDays}d` : ''}
                  </span>
                </div>
                <div className="mt-1 flex flex-wrap gap-2 text-[8px] uppercase tracking-wider">
                  <span className="px-1.5 py-0.5 border border-[#26272b] rounded text-[#949ba5]">{ind.owner}</span>
                  <span className="px-1.5 py-0.5 border border-[#26272b] rounded text-[#949ba5]">{ind.escalation.replace(/_/g, ' ')}</span>
                  {ind.chronic && (
                    <span className="px-1.5 py-0.5 border border-[#ff6b6b]/40 rounded text-[#ff6b6b]">chronic</span>
                  )}
                </div>
                {ind.lastUpdate && (
                  <div className="mt-1 text-[8px] text-[#949ba5]/60">
                    last update {new Date(ind.lastUpdate).toLocaleDateString()}
                  </div>
                )}
              </div>
            ))}
          </div>
          {freshness.staleCount > 3 && (
            <div className="text-[8px] text-[#949ba5]/40 mt-2">
              +{freshness.staleCount - 3} more stale indicators
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ============== ML Accuracy Display ==============
function MLAccuracyBadge({ accuracy }: { accuracy: MLAccuracyData | null }) {
  if (!accuracy) return null

  const acc7d = accuracy.rolling_7d.direction_accuracy
  const acc30d = accuracy.rolling_30d.direction_accuracy
  const insufficientSample = accuracy.coverage_quality === 'INSUFFICIENT' || accuracy.coverage.evaluated_count < accuracy.minimum_reliable_sample

  const getColor = (acc: number | null) => {
    if (insufficientSample) return 'text-[#949ba5]'
    if (acc === null) return 'text-[#949ba5]'
    if (acc >= 70) return 'text-[#00c896]'
    if (acc >= 55) return 'text-[#f59e0b]'
    return 'text-[#ff6b6b]'
  }

  const renderValue = (acc: number | null) => (acc === null ? 'N/A' : `${Math.round(acc)}%`)

  return (
    <div className="flex justify-center gap-4 mt-3 mb-2">
      <Tooltip content="Rolling 30-day directional accuracy of ML predictions">
        <div className="flex items-center gap-2 bg-[#0a0a0a]/60 rounded px-2.5 py-1 border border-[#1a1a1a]">
          <span className="text-[8px] text-[#949ba5]/50 uppercase tracking-wider">7d Acc</span>
          <span className={`text-[10px] font-mono ${getColor(acc7d)}`}>
            {renderValue(acc7d)}
          </span>
        </div>
      </Tooltip>
      <Tooltip content="Rolling 90-day directional accuracy of ML predictions">
        <div className="flex items-center gap-2 bg-[#0a0a0a]/60 rounded px-2.5 py-1 border border-[#1a1a1a]">
          <span className="text-[8px] text-[#949ba5]/50 uppercase tracking-wider">30d Acc</span>
          <span className={`text-[10px] font-mono ${getColor(acc30d)}`}>
            {renderValue(acc30d)}
          </span>
        </div>
      </Tooltip>
      {accuracy.unavailable_reasons.length > 0 && (
        <div className="text-[8px] text-[#949ba5]/60 self-center">
          {accuracy.unavailable_reasons.slice(0, 1).map((reason) => formatUnavailableReason(reason)).join(', ')}
        </div>
      )}
      {insufficientSample && (
        <div className="text-[8px] text-[#949ba5]/60 self-center">
          low sample (n={accuracy.coverage.evaluated_count}/{accuracy.minimum_reliable_sample})
        </div>
      )}
    </div>
  )
}

// ============== Category Modal Component ==============
function CategoryModal({
  category,
  onClose
}: {
  category: string
  onClose: () => void
}) {
  const [data, setData] = useState<CategoryDetailData | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const fetchData = async () => {
      try {
        const res = await fetchApi(`/api/category/${category}`)
        if (res.ok) {
          const json = await res.json()
          setData(json)
        }
      } catch (err) {
        console.error('Failed to fetch category details:', err)
      } finally {
        setLoading(false)
      }
    }
    fetchData()
  }, [category])

  const formatDisplayName = (name: string) =>
    name.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())

  // Category sparkline
  const renderSparkline = () => {
    if (!data || data.history.length < 2) return null  // Need at least 2 points

    const values = data.history.map(h => h.score)
    const min = Math.min(...values)
    const max = Math.max(...values)
    const range = max - min || 1
    const width = 320
    const height = 80

    const points = data.history.map((h, i) => {
      const x = (i / (data.history.length - 1)) * width
      const y = height - ((h.score - min) / range) * (height - 10) - 5
      return `${x},${y}`
    }).join(' ')

    const currentScore = data.score
    const avgScore = values.reduce((a, b) => a + b, 0) / values.length

    return (
      <div className="mb-6">
        <div className="flex justify-between items-center mb-2">
          <span className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider">90-Day History</span>
          <span className="text-[10px] text-[#949ba5]/60">
            Avg: <span className="font-mono text-[#f3f3f3]">{avgScore.toFixed(1)}</span>
          </span>
        </div>
        <svg viewBox={`0 0 ${width} ${height}`} className="w-full h-20">
          <defs>
            <linearGradient id={`gradient-${category}`} x1="0%" y1="0%" x2="0%" y2="100%">
              <stop offset="0%" stopColor="#00a3ff" stopOpacity="0.2" />
              <stop offset="100%" stopColor="#00a3ff" stopOpacity="0" />
            </linearGradient>
          </defs>
          {/* Fill area */}
          <polygon
            points={`0,${height} ${points} ${width},${height}`}
            fill={`url(#gradient-${category})`}
          />
          {/* Line */}
          <polyline
            fill="none"
            stroke="#00a3ff"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
            points={points}
          />
          {/* Current value dot */}
          {data.history.length > 0 && (
            <circle
              cx={width}
              cy={height - ((currentScore - min) / range) * (height - 10) - 5}
              r="4"
              fill="#00a3ff"
            />
          )}
        </svg>
        <div className="flex justify-between text-[9px] text-[#949ba5]/40 mt-1">
          <span>{data.history[0]?.date}</span>
          <span>{data.history[data.history.length - 1]?.date}</span>
        </div>
      </div>
    )
  }

  const getScoreColor = (normalized: number) => {
    if (normalized >= 70) return 'text-[#00a3ff]'
    if (normalized >= 50) return 'text-[#f3f3f3]'
    if (normalized >= 30) return 'text-[#f59e0b]'
    return 'text-[#ff6b6b]'
  }

  return (
    <div className="fixed inset-0 bg-black/90 z-50 flex items-center justify-center p-4">
      <div className="bg-[#0a0a0a] border border-[#26272b] rounded-lg max-w-md w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-[#26272b]">
          <div>
            <h2 className="text-[#f3f3f3] text-lg font-light tracking-wider uppercase">
              {formatDisplayName(category)}
            </h2>
            <p className="text-[#949ba5]/60 text-[10px] tracking-wide mt-0.5">
              Category Deep Dive
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-[#949ba5] hover:text-[#f3f3f3] transition-colors p-2"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Content */}
        <div className="p-4">
          {loading ? (
            <div className="text-center py-8 text-[#949ba5]/50 text-sm">Loading...</div>
          ) : data ? (
            <>
              {/* Score Overview */}
              <div className="flex items-center justify-between mb-6">
                <div>
                  <div className="text-4xl font-light text-[#f3f3f3]">{Math.round(data.score)}</div>
                  <div className="text-[10px] text-[#949ba5]/60 mt-1">
                    Weight: {(data.weight * 100).toFixed(0)}%
                  </div>
                </div>
                <div className="text-right">
                  <div className={`text-2xl font-mono ${data.percentile_rank >= 70 ? 'text-[#00a3ff]' : data.percentile_rank <= 30 ? 'text-[#ff6b6b]' : 'text-[#949ba5]'}`}>
                    {data.percentile_rank}<span className="text-sm">th</span>
                  </div>
                  <div className="text-[10px] text-[#949ba5]/60 mt-1">
                    90-day percentile
                  </div>
                </div>
              </div>

              {/* Sparkline */}
              {renderSparkline()}

              {/* Indicators */}
              <div>
                <div className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider mb-3">
                  Component Indicators
                </div>
                <div className="space-y-2">
                  {data.indicators.map(ind => (
                    <div key={ind.id} className="flex items-center justify-between py-2 border-b border-[#1a1a1a]">
                      <div>
                        <div className="text-[11px] text-[#f3f3f3]">{ind.name}</div>
                        <div className="text-[9px] text-[#949ba5]/50 font-mono mt-0.5">
                          Raw: {typeof ind.raw_value === 'number' ? ind.raw_value.toFixed(2) : '—'}
                        </div>
                      </div>
                      <div className="text-right">
                        <div className={`text-lg font-mono ${getScoreColor(ind.normalized_value)}`}>
                          {Math.round(ind.normalized_value)}
                        </div>
                        <div className="w-12 h-1 bg-[#26272b] rounded-full overflow-hidden mt-1">
                          <div
                            className="h-full bg-[#00a3ff]/60 rounded-full"
                            style={{ width: `${ind.normalized_value}%` }}
                          />
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Percentile Context */}
              <div className="mt-6 p-3 bg-[#0f0f0f] border border-[#1a1a1a] rounded">
                <div className="text-[10px] text-[#949ba5]">
                  <span className="text-[#00a3ff]">{formatDisplayName(category)}</span> at{' '}
                  <span className="font-mono text-[#f3f3f3]">{Math.round(data.score)}</span> is in the{' '}
                  <span className={`font-mono ${data.percentile_rank >= 70 ? 'text-[#00a3ff]' : data.percentile_rank <= 30 ? 'text-[#ff6b6b]' : 'text-[#f3f3f3]'}`}>
                    {data.percentile_rank}th percentile
                  </span>{' '}
                  of the last 90 days.
                  {data.percentile_rank >= 80 && ' This is an elevated reading.'}
                  {data.percentile_rank <= 20 && ' This is a depressed reading.'}
                </div>
              </div>
            </>
          ) : (
            <div className="text-center py-8 text-[#949ba5]/50 text-sm">Failed to load data</div>
          )}
        </div>
      </div>
    </div>
  )
}

// ============== Top Themes Widget ==============
function TopThemesWidget({ data, regime }: { data: SignalsData | null; regime?: 'RISK_ON' | 'RISK_OFF' | 'TRANSITION' }) {
  if (!data || !data.themes || data.themes.length === 0) return null

  const topThemes = data.themes.slice(0, 3)

  const getConfidenceColor = (confidence: string) => {
    if (confidence === 'Very High' || confidence === 'High') return 'text-[#00c896]'
    if (confidence === 'Medium-Low' || confidence === 'Low') return 'text-[#ff6b6b]'
    return 'text-[#f5a524]' // Medium-High, Medium
  }

  const getTimingIcon = (timing: string) => {
    if (timing === 'Now') return '↑'
    if (timing === 'Building') return '→'
    if (timing === 'Ongoing') return '◆'
    return '○' // Early
  }

  const getSignalTypeColor = (signalType: string) => {
    if (signalType === 'Momentum') return 'bg-[#00c896]/20 text-[#00c896]'
    if (signalType === 'Rotation') return 'bg-[#00a3ff]/20 text-[#00a3ff]'
    if (signalType === 'Divergence') return 'bg-[#f5a524]/20 text-[#f5a524]'
    return 'bg-[#949ba5]/20 text-[#949ba5]' // Mean Reversion
  }

  const renderStars = (stars: number) => {
    return '★'.repeat(stars) + '☆'.repeat(5 - stars)
  }

  // Check if top theme aligns with current regime based on signal type
  const isAligned = regime && topThemes[0] &&
    ((regime === 'RISK_ON' && topThemes[0].classification.signal_type === 'Momentum') ||
     (regime === 'RISK_OFF' && topThemes[0].classification.signal_type === 'Mean Reversion'))

  return (
    <div className="w-full mt-6 sm:mt-8 p-4 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg">
      <div className="flex items-center justify-between mb-3">
        <div className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider">
          Trending Themes
        </div>
        <a
          href="/signals"
          className="text-[9px] text-[#00a3ff] hover:text-[#00a3ff]/80 transition-colors uppercase tracking-wider"
        >
          View All →
        </a>
      </div>

      {isAligned && (
        <div className="mb-3 px-3 py-1.5 bg-[#00a3ff]/10 border border-[#00a3ff]/20 rounded text-center">
          <span className="text-[9px] text-[#00a3ff]">
            {regime?.replace('_', ' ')} regime + {topThemes[0].theme_name} trending = Aligned signal
          </span>
        </div>
      )}

      <div className="space-y-2">
        {topThemes.map((theme, idx) => (
          <div
            key={theme.theme_id}
            className="flex items-center justify-between py-2 border-b border-[#1a1a1a] last:border-0"
          >
            <div className="flex items-center gap-2">
              <span className="text-[10px] text-[#949ba5]/40 w-4">#{idx + 1}</span>
              <div>
                <div className="flex items-center gap-2">
                  <span className="text-[11px] text-[#f3f3f3]">{theme.theme_name}</span>
                  <span className={`text-[8px] px-1.5 py-0.5 rounded ${getSignalTypeColor(theme.classification.signal_type)}`}>
                    {theme.classification.signal_type}
                  </span>
                </div>
                <div className="text-[9px] text-[#949ba5]/50 font-mono mt-0.5">
                  {theme.key_tickers.slice(0, 3).join(', ')}
                </div>
              </div>
            </div>
            <div className="flex flex-col items-end gap-0.5">
              <div className="flex items-center gap-2">
                <span className={`text-[10px] ${getConfidenceColor(theme.classification.confidence)}`}>
                  {getTimingIcon(theme.classification.timing)}
                </span>
                <span className="text-[11px] font-mono text-[#f3f3f3]">
                  {theme.score.toFixed(1)}
                </span>
              </div>
              <span className="text-[9px] text-[#f5a524]/80">
                {renderStars(theme.classification.stars)}
              </span>
            </div>
          </div>
        ))}
      </div>

      <div className="mt-3 text-[8px] text-[#949ba5]/30 text-center">
        Updated: {new Date(data.generated_at_utc).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
      </div>
    </div>
  )
}

// ============== Similar Periods Card ==============
function SimilarPeriodsCard({ data }: { data: SimilarPeriodsData | null }) {
  if (!data || !data.similar_periods || data.similar_periods.length === 0) return null

  const formatReturn = (val: number | null) => {
    if (val === null) return '—'
    const color = val >= 0 ? 'text-[#00c896]' : 'text-[#ff6b6b]'
    return <span className={color}>{val >= 0 ? '+' : ''}{val.toFixed(2)}%</span>
  }

  // Only compute outlook from periods with real forward return values.
  const weightedForwardReturn = (horizon: 'd7' | 'd30') => {
    let weightedSum = 0
    let totalWeight = 0
    let sampleCount = 0

    for (const period of data.similar_periods) {
      const value = period.forward_returns?.[horizon]
      if (typeof value === 'number') {
        weightedSum += value * period.weights.combined
        totalWeight += period.weights.combined
        sampleCount += 1
      }
    }

    if (sampleCount === 0 || totalWeight === 0) {
      return { value: null as number | null, sampleCount: 0 }
    }

    return { value: weightedSum / totalWeight, sampleCount }
  }

  const outlook7d = weightedForwardReturn('d7')
  const outlook30d = weightedForwardReturn('d30')

  const valid30dReturns = data.similar_periods
    .map(period => period.forward_returns?.d30)
    .filter((value): value is number => typeof value === 'number')

  const positiveCount = valid30dReturns.filter(value => value > 0).length
  const winRate = valid30dReturns.length > 0
    ? (positiveCount / valid30dReturns.length) * 100
    : null

  const metricColor = (value: number | null, threshold = 0) => {
    if (value === null) return 'text-[#949ba5]/60'
    return value >= threshold ? 'text-[#00c896]' : 'text-[#ff6b6b]'
  }

  return (
    <div className="w-full mt-6 sm:mt-8 p-4 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg">
      <div className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider mb-4">
        Similar Historical Periods
      </div>

      {/* Probability-Weighted Outlook */}
      <div className="mb-4 p-3 bg-[#0f0f0f] border border-[#1a1a1a] rounded">
        <div className="text-[8px] text-[#949ba5]/40 uppercase tracking-wider mb-2">
          Probability-Weighted Outlook
        </div>
        <div className="flex justify-between items-center">
          <div className="text-center">
            <div className="text-[10px] text-[#949ba5]/60">7d</div>
            <div className={`text-lg font-mono ${metricColor(outlook7d.value)}`}>
              {outlook7d.value === null ? '—' : `${outlook7d.value >= 0 ? '+' : ''}${outlook7d.value.toFixed(2)}%`}
            </div>
          </div>
          <div className="text-center">
            <div className="text-[10px] text-[#949ba5]/60">30d</div>
            <div className={`text-lg font-mono ${metricColor(outlook30d.value)}`}>
              {outlook30d.value === null ? '—' : `${outlook30d.value >= 0 ? '+' : ''}${outlook30d.value.toFixed(2)}%`}
            </div>
          </div>
          <div className="text-center">
            <div className="text-[10px] text-[#949ba5]/60">Win Rate</div>
            <div className={`text-lg font-mono ${metricColor(winRate, 50)}`}>
              {winRate === null ? '—' : `${Math.round(winRate)}%`}
            </div>
          </div>
        </div>
        {(outlook7d.sampleCount < data.similar_periods.length || outlook30d.sampleCount < data.similar_periods.length) && (
          <div className="mt-2 text-[8px] text-[#949ba5]/40 text-center">
            sample coverage: 7d {outlook7d.sampleCount}/{data.similar_periods.length}, 30d {outlook30d.sampleCount}/{data.similar_periods.length}
          </div>
        )}
      </div>

      {/* Similar Periods List */}
      <div className="space-y-2">
        {data.similar_periods.map((period, idx) => (
          <div
            key={period.date}
            className="flex items-center justify-between py-2 border-b border-[#1a1a1a] last:border-0"
          >
            <div className="flex items-center gap-3">
              <span className="text-[10px] text-[#949ba5]/40 w-4">#{idx + 1}</span>
              <div>
                <div className="text-[11px] text-[#f3f3f3]">
                  {new Date(period.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}
                </div>
                <div className="text-[9px] text-[#949ba5]/50">
                  PXI: <span className="font-mono">{period.pxi?.score?.toFixed(0) ?? '—'}</span>
                  <span className="mx-1">•</span>
                  {Math.round(period.similarity * 100)}% similar
                </div>
              </div>
            </div>
            <div className="text-right text-[10px]">
              <div>7d: {formatReturn(period.forward_returns?.d7 ?? null)}</div>
              <div>30d: {formatReturn(period.forward_returns?.d30 ?? null)}</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ============== Backtest Performance Card ==============
function BacktestCard({ data }: { data: BacktestData | null }) {
  if (!data || !data.bucket_analysis || data.bucket_analysis.length === 0) return null

  const formatPercent = (val: number | null) => {
    if (val === null) return '—'
    return `${val >= 0 ? '+' : ''}${val.toFixed(1)}%`
  }

  return (
    <div className="w-full mt-6 sm:mt-8 p-4 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg">
      <div className="flex items-center justify-between mb-4">
        <div className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider">
          Historical Backtest
        </div>
        <div className="text-[8px] text-[#949ba5]/30">
          {data.summary.date_range?.start} → {data.summary.date_range?.end}
        </div>
      </div>

      {/* Extreme Readings Summary */}
      {data.extreme_readings && (
        <div className="mb-4 grid grid-cols-2 gap-3">
          <div className="p-2 bg-[#0f0f0f] rounded border border-[#1a1a1a]">
            <div className="text-[8px] text-[#00c896]/60 uppercase tracking-wider mb-1">Low PXI (&lt;25)</div>
            <div className="flex items-center justify-between">
              <span className="text-sm font-mono text-[#00c896]">
                {formatPercent(data.extreme_readings.low_pxi.avg_return_30d)}
              </span>
              <span className="text-[9px] text-[#949ba5]/40">
                n={data.extreme_readings.low_pxi.count}
              </span>
            </div>
          </div>
          <div className="p-2 bg-[#0f0f0f] rounded border border-[#1a1a1a]">
            <div className="text-[8px] text-[#ff6b6b]/60 uppercase tracking-wider mb-1">High PXI (&gt;75)</div>
            <div className="flex items-center justify-between">
              <span className="text-sm font-mono text-[#ff6b6b]">
                {formatPercent(data.extreme_readings.high_pxi.avg_return_30d)}
              </span>
              <span className="text-[9px] text-[#949ba5]/40">
                n={data.extreme_readings.high_pxi.count}
              </span>
            </div>
          </div>
        </div>
      )}

      {/* Bucket Performance */}
      <div className="text-[8px] text-[#949ba5]/40 uppercase tracking-wider mb-2">
        30-Day Returns by PXI Bucket
      </div>
      <div className="space-y-1.5">
        {data.bucket_analysis.map(bucket => (
          <div key={bucket.bucket} className="flex items-center gap-2">
            <span className="text-[10px] text-[#949ba5] w-16 text-right font-mono">
              {bucket.bucket}
            </span>
            <div className="flex-1 h-2 bg-[#1a1a1a] rounded-full overflow-hidden">
              <div
                className={`h-full rounded-full ${(bucket.avg_return_30d ?? 0) >= 0 ? 'bg-[#00c896]' : 'bg-[#ff6b6b]'}`}
                style={{ width: `${Math.min(100, Math.abs(bucket.avg_return_30d ?? 0) * 10)}%` }}
              />
            </div>
            <span className={`text-[10px] font-mono w-14 text-right ${(bucket.avg_return_30d ?? 0) >= 0 ? 'text-[#00c896]' : 'text-[#ff6b6b]'}`}>
              {formatPercent(bucket.avg_return_30d)}
            </span>
            <span className="text-[9px] text-[#949ba5]/40 w-10 text-right">
              n={bucket.count}
            </span>
          </div>
        ))}
      </div>

      <div className="mt-4 text-center">
        <span className="text-[9px] text-[#949ba5]/40">
          Why trust PXI? {data.summary.total_observations.toLocaleString()} observations backtested
        </span>
      </div>
    </div>
  )
}

function BriefCompactCard({
  brief,
  onOpen,
  className,
}: {
  brief: BriefData | null
  onOpen: () => void
  className?: string
}) {
  if (!brief) return null

  return (
    <div className={className || "w-full mt-6 p-4 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg"}>
      <div className="flex items-center justify-between mb-2">
        <div className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider">Today&apos;s Brief</div>
        <button
          onClick={onOpen}
          className="text-[9px] uppercase tracking-[0.2em] text-[#00a3ff] hover:text-[#7ccfff]"
        >
          open /brief
        </button>
      </div>
      <p className="text-[12px] text-[#d7dbe1] leading-relaxed">{brief.summary}</p>
      <div className="mt-3 flex items-center gap-2 text-[9px] uppercase tracking-wider">
        <span className="px-2 py-1 border border-[#26272b] rounded text-[#949ba5]">{brief.regime_delta}</span>
        <span className="px-2 py-1 border border-[#26272b] rounded text-[#949ba5]">{brief.risk_posture.replace('_', '-')}</span>
        <span className={`px-2 py-1 border rounded ${
          brief.consistency.state === 'PASS'
            ? 'border-[#00c896]/40 text-[#00c896]'
            : brief.consistency.state === 'WARN'
              ? 'border-[#f59e0b]/40 text-[#f59e0b]'
              : 'border-[#ff6b6b]/40 text-[#ff6b6b]'
        }`}>
          {brief.consistency.state} {brief.consistency.score}
        </span>
        {brief.freshness_status.has_stale_data && (
          <span className="px-2 py-1 border border-[#ff6b6b]/40 rounded text-[#ff6b6b]">
            stale: {brief.freshness_status.stale_count}
          </span>
        )}
      </div>
      <div className="mt-2 text-[9px] uppercase tracking-wider text-[#949ba5]/70">
        policy {brief.policy_state.stance.replace('_', ' ')} · {brief.policy_state.base_signal.replace(/_/g, ' ')}
      </div>
      {brief.degraded_reason && (
        <div className="mt-1 text-[9px] text-[#f59e0b]">
          degraded: {brief.degraded_reason.replace(/_/g, ' ')}
        </div>
      )}
    </div>
  )
}

function OpportunityPreview({ data, onOpen }: { data: OpportunitiesResponse | null; onOpen: () => void }) {
  if (!data) return null
  const top = (data.items || []).slice(0, 3)
  const suppressedCount = Math.max(0, data.suppressed_count || 0)
  const suppressionByReason = data.suppression_by_reason || {
    coherence_failed: Math.max(0, data.coherence_suppressed_count || 0),
    quality_filtered: Math.max(0, data.quality_filtered_count || 0),
    data_quality_suppressed: data.degraded_reason === 'suppressed_data_quality' ? suppressedCount : 0,
  }
  const qualityFilterRate = Number.isFinite(data.quality_filter_rate as number) ? Number(data.quality_filter_rate) : 0
  const coherenceFailRate = Number.isFinite(data.coherence_fail_rate as number) ? Number(data.coherence_fail_rate) : 0
  const actionabilityState = data.actionability_state || (top.length === 0 ? 'NO_ACTION' : 'WATCH')
  const ctaDisabledReasons = data.cta_disabled_reasons || []
  const ctaEnabled = typeof data.cta_enabled === 'boolean'
    ? data.cta_enabled
    : (top.length > 0 && ctaDisabledReasons.length === 0)
  const hasFeedState = top.length > 0 || suppressedCount > 0 || Boolean(data.degraded_reason)
  if (!hasFeedState) return null

  return (
    <div className="w-full mt-6 p-4 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg">
      <div className="flex items-center justify-between mb-3">
        <div className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider">Top Opportunities</div>
        <button
          onClick={onOpen}
          className="text-[9px] uppercase tracking-[0.2em] text-[#00a3ff] hover:text-[#7ccfff]"
        >
          open /opportunities
        </button>
      </div>

      {data.degraded_reason && (
        <p className="mb-3 text-[10px] text-[#f59e0b]">
          {formatOpportunityDegradedReason(data.degraded_reason)}
        </p>
      )}
      {suppressedCount > 0 && (
        <div className="mb-3 space-y-1">
          <p className="text-[9px] text-[#949ba5]/70 uppercase tracking-wider">
            suppressed {suppressedCount} · {formatActionabilityState(actionabilityState)}
          </p>
          <p className="text-[9px] text-[#949ba5]/60">
            coherence {suppressionByReason.coherence_failed} ({(coherenceFailRate * 100).toFixed(0)}%) ·
            {' '}quality {suppressionByReason.quality_filtered} ({(qualityFilterRate * 100).toFixed(0)}%) ·
            {' '}data-quality {suppressionByReason.data_quality_suppressed}
          </p>
        </div>
      )}
      {!ctaEnabled && (
        <p className="mb-3 text-[9px] text-[#f59e0b]">
          action CTA disabled: {(ctaDisabledReasons.length > 0 ? ctaDisabledReasons : ['no_eligible_opportunities']).map(formatCtaDisabledReason).join(' · ')}
        </p>
      )}

      {top.length === 0 ? (
        <div className="text-[10px] text-[#949ba5]">No eligible opportunities currently published.</div>
      ) : (
        <div className="space-y-2">
          {top.map((item) => {
            const calibration = item.calibration ?? fallbackOpportunityCalibration()
            const expectancy = item.expectancy ?? fallbackOpportunityExpectancy()
            return (
            <div key={item.id} className="p-3 bg-[#0f0f0f] border border-[#1a1a1a] rounded">
              <div className="flex items-start justify-between gap-2">
                <div>
                  <div className="text-[12px] text-[#f3f3f3]">{item.theme_name}</div>
                  <div className="text-[9px] text-[#949ba5]/60 uppercase tracking-wider">
                    {item.direction} · {item.sample_size} samples
                  </div>
                </div>
                <div className="text-right">
                  <div className="text-[18px] leading-none font-mono text-[#f3f3f3]">{item.conviction_score}</div>
                  <div className="text-[8px] text-[#949ba5]/50 uppercase tracking-wider">conviction</div>
                  <div className={`mt-1 inline-block rounded border px-1.5 py-0.5 text-[8px] uppercase tracking-wider ${calibrationQualityClass(calibration.quality)}`}>
                    {calibration.quality.toLowerCase()}
                  </div>
                </div>
              </div>
              <p className="mt-2 text-[10px] text-[#b8bec8]">{item.rationale}</p>
              <p className="mt-1 text-[9px] text-[#949ba5]/70">
                calibrated hit {formatProbability(calibration.probability_correct_direction)} ·
                {' '}95% CI {formatProbability(calibration.ci95_low)}-{formatProbability(calibration.ci95_high)} ·
                {' '}n={calibration.sample_size} · window {calibration.window || 'n/a'}
              </p>
              <p className="mt-1 text-[9px] text-[#949ba5]/70">
                expectancy {formatMaybePercent(expectancy.expected_move_pct)} ·
                {' '}max adverse {formatMaybePercent(expectancy.max_adverse_move_pct)} ·
                {' '}n={expectancy.sample_size} · {expectancy.basis.replace(/_/g, ' ')} · {expectancy.quality.toLowerCase()}
              </p>
              {(calibration.unavailable_reason || expectancy.unavailable_reason) && (
                <p className="mt-1 text-[9px] text-[#949ba5]/70">
                  unavailable: {[calibration.unavailable_reason, expectancy.unavailable_reason].filter(Boolean).map((r) => formatUnavailableReason(r)).join(' · ')}
                </p>
              )}
              {calibration.quality !== 'ROBUST' && (
                <p className="mt-1 text-[9px] text-[#f59e0b]">Use reduced sizing until calibration quality improves.</p>
              )}
            </div>
          )})}
        </div>
      )}
    </div>
  )
}

function BriefPage({ brief, onBack }: { brief: BriefData | null; onBack: () => void }) {
  return (
    <div className="min-h-screen bg-black text-[#f3f3f3] px-4 sm:px-8 py-10">
      <div className="max-w-4xl mx-auto">
        <div className="flex items-center justify-between mb-8">
          <h1 className="text-[11px] uppercase tracking-[0.3em] text-[#949ba5]">PXI /brief</h1>
          <button
            onClick={onBack}
            className="text-[10px] uppercase tracking-[0.2em] border border-[#26272b] px-3 py-1.5 rounded text-[#949ba5] hover:text-[#f3f3f3]"
          >
            home
          </button>
        </div>

        {!brief ? (
          <div className="text-[#949ba5]">No brief snapshot available yet.</div>
        ) : (
          <div className="space-y-6">
            <div className="p-5 bg-[#0a0a0a]/70 border border-[#26272b] rounded-lg">
              <div className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider mb-2">
                Market Summary
              </div>
              <p className="text-[14px] leading-relaxed text-[#e4e8ee]">{brief.summary}</p>
              <div className="mt-3 flex flex-wrap items-center gap-2 text-[9px] uppercase tracking-wider">
                <span className="px-2 py-1 border border-[#26272b] rounded text-[#949ba5]">{brief.regime_delta}</span>
                <span className="px-2 py-1 border border-[#26272b] rounded text-[#949ba5]">{brief.risk_posture.replace('_', '-')}</span>
                <span className="px-2 py-1 border border-[#26272b] rounded text-[#949ba5]">
                  stance {brief.policy_state.stance.replace('_', ' ')}
                </span>
                <span className={`px-2 py-1 border rounded ${
                  brief.consistency.state === 'PASS'
                    ? 'border-[#00c896]/40 text-[#00c896]'
                    : brief.consistency.state === 'WARN'
                      ? 'border-[#f59e0b]/40 text-[#f59e0b]'
                      : 'border-[#ff6b6b]/40 text-[#ff6b6b]'
                }`}>
                  consistency {brief.consistency.state} {brief.consistency.score}
                </span>
                <span className="px-2 py-1 border border-[#26272b] rounded text-[#949ba5]">
                  as of {new Date(brief.as_of).toLocaleString()}
                </span>
              </div>
              <div className="mt-2 text-[10px] text-[#949ba5]/80">
                source plan {new Date(brief.source_plan_as_of).toLocaleString()} · contract {brief.contract_version}
              </div>
              {brief.policy_state.rationale && (
                <div className="mt-2 text-[10px] text-[#cfd5de]">
                  {brief.policy_state.rationale.replace(/_/g, ' ')}
                </div>
              )}
              {brief.degraded_reason && (
                <div className="mt-2 text-[9px] text-[#f59e0b]">
                  degraded: {brief.degraded_reason.replace(/_/g, ' ')}
                </div>
              )}
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="p-4 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg">
                <div className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider mb-3">Category Movers</div>
                <div className="space-y-2">
                  {brief.explainability.category_movers.slice(0, 5).map((row) => (
                    <div key={row.category} className="flex items-center justify-between text-[11px]">
                      <span className="text-[#cfd5de]">{row.category}</span>
                      <span className={row.score_change >= 0 ? 'text-[#00c896] font-mono' : 'text-[#ff6b6b] font-mono'}>
                        {row.score_change >= 0 ? '+' : ''}{row.score_change.toFixed(1)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
              <div className="p-4 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg">
                <div className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider mb-3">Indicator Movers</div>
                <div className="space-y-2">
                  {brief.explainability.indicator_movers.slice(0, 5).map((row) => (
                    <div key={row.indicator_id} className="flex items-center justify-between text-[11px]">
                      <span className="text-[#cfd5de]">{row.indicator_id}</span>
                      <span className={row.z_impact >= 0 ? 'text-[#00c896] font-mono' : 'text-[#ff6b6b] font-mono'}>
                        {row.z_impact >= 0 ? '+' : ''}{row.z_impact.toFixed(1)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            <div className="p-4 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg">
              <div className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider mb-2">Top Changes</div>
              <ul className="space-y-1">
                {brief.top_changes.map((change, idx) => (
                  <li key={`${change}-${idx}`} className="text-[11px] text-[#cfd5de]">{change}</li>
                ))}
              </ul>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

function OpportunitiesPage({
  data,
  diagnostics,
  horizon,
  onHorizonChange,
  onBack,
}: {
  data: OpportunitiesResponse | null
  diagnostics: CalibrationDiagnosticsResponse | null
  horizon: '7d' | '30d'
  onHorizonChange: (h: '7d' | '30d') => void
  onBack: () => void
}) {
  const suppressedCount = Math.max(0, data?.suppressed_count || 0)
  const degradedReason = data?.degraded_reason || null
  const suppressionByReason = data?.suppression_by_reason || {
    coherence_failed: Math.max(0, data?.coherence_suppressed_count || 0),
    quality_filtered: Math.max(0, data?.quality_filtered_count || 0),
    data_quality_suppressed: degradedReason === 'suppressed_data_quality' ? suppressedCount : 0,
  }
  const qualityFilterRate = Number.isFinite(data?.quality_filter_rate as number) ? Number(data?.quality_filter_rate) : 0
  const coherenceFailRate = Number.isFinite(data?.coherence_fail_rate as number) ? Number(data?.coherence_fail_rate) : 0
  const actionabilityState = data?.actionability_state || (data?.items?.length ? 'WATCH' : 'NO_ACTION')
  const actionabilityReasonCodes = data?.actionability_reason_codes || []
  const ctaDisabledReasons = data?.cta_disabled_reasons || []
  const ctaEnabled = typeof data?.cta_enabled === 'boolean'
    ? Boolean(data.cta_enabled)
    : (Boolean(data?.items?.length) && ctaDisabledReasons.length === 0)
  const noActionUnlockConditions = actionabilityState === 'NO_ACTION'
    ? deriveNoActionUnlockConditions({
        actionabilityReasonCodes,
        ctaDisabledReasons,
        diagnostics,
      })
    : []
  const hasContractGateSuppression = degradedReason === 'coherence_gate_failed'
  const hasDataQualitySuppression = degradedReason === 'suppressed_data_quality'
  const hasQualityFilter = degradedReason === 'quality_filtered'

  return (
    <div className="min-h-screen bg-black text-[#f3f3f3] px-4 sm:px-8 py-10">
      <div className="max-w-5xl mx-auto">
        <div className="flex items-center justify-between mb-8">
          <h1 className="text-[11px] uppercase tracking-[0.3em] text-[#949ba5]">PXI /opportunities</h1>
          <button
            onClick={onBack}
            className="text-[10px] uppercase tracking-[0.2em] border border-[#26272b] px-3 py-1.5 rounded text-[#949ba5] hover:text-[#f3f3f3]"
          >
            home
          </button>
        </div>

        <div className="mb-6 flex items-center gap-2">
          <button
            onClick={() => onHorizonChange('7d')}
            className={`px-3 py-1.5 text-[10px] uppercase tracking-[0.2em] border rounded ${
              horizon === '7d' ? 'border-[#00a3ff] text-[#00a3ff]' : 'border-[#26272b] text-[#949ba5]'
            }`}
          >
            7d
          </button>
          <button
            onClick={() => onHorizonChange('30d')}
            className={`px-3 py-1.5 text-[10px] uppercase tracking-[0.2em] border rounded ${
              horizon === '30d' ? 'border-[#00a3ff] text-[#00a3ff]' : 'border-[#26272b] text-[#949ba5]'
            }`}
          >
            30d
          </button>
        </div>

        {diagnostics && (
          <div className="mb-4 p-3 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg">
            <div className="flex items-center justify-between gap-2">
              <div className="text-[9px] uppercase tracking-wider text-[#949ba5]/60">Calibration diagnostics</div>
              <span className={`rounded border px-2 py-1 text-[8px] uppercase tracking-wider ${calibrationQualityClass(diagnostics.diagnostics.quality_band)}`}>
                {diagnostics.diagnostics.quality_band.toLowerCase()}
              </span>
            </div>
            <div className="mt-2 text-[10px] text-[#d7dbe1]">
              samples {diagnostics.total_samples} · as_of {new Date(diagnostics.as_of).toLocaleString()}
            </div>
            {diagnostics.diagnostics.quality_band !== 'INSUFFICIENT' ? (
              <div className="mt-1 text-[10px] text-[#949ba5]">
                brier {diagnostics.diagnostics.brier_score?.toFixed(4)} · ece {diagnostics.diagnostics.ece?.toFixed(4)}
              </div>
            ) : (
              <div className="mt-1 text-[10px] text-[#f59e0b]">
                Insufficient sample for stable numeric calibration diagnostics.
              </div>
            )}
          </div>
        )}

        {degradedReason && (
          <div className={`mb-4 text-[10px] ${hasDataQualitySuppression || hasContractGateSuppression ? 'text-[#f59e0b]' : 'text-[#949ba5]'}`}>
            {formatOpportunityDegradedReason(degradedReason)}
          </div>
        )}
        {suppressedCount > 0 && (
          <div className="mb-4 space-y-1 text-[10px] text-[#949ba5]/80">
            <div className="uppercase tracking-wider">
              suppressed {suppressedCount} · {formatActionabilityState(actionabilityState)}
            </div>
            <div className="text-[#949ba5]/70">
              coherence {suppressionByReason.coherence_failed} ({(coherenceFailRate * 100).toFixed(0)}%) ·
              {' '}quality {suppressionByReason.quality_filtered} ({(qualityFilterRate * 100).toFixed(0)}%) ·
              {' '}data-quality {suppressionByReason.data_quality_suppressed}
            </div>
            {actionabilityReasonCodes.length > 0 && (
              <div className="text-[#949ba5]/60">
                reasons: {actionabilityReasonCodes.slice(0, 4).map((reason) => reason.replace(/_/g, ' ')).join(' · ')}
              </div>
            )}
          </div>
        )}

        {!ctaEnabled && (
          <div className="mb-4 text-[10px] text-[#f59e0b]">
            action CTA disabled: {(ctaDisabledReasons.length > 0 ? ctaDisabledReasons : ['no_eligible_opportunities']).map(formatCtaDisabledReason).join(' · ')}
          </div>
        )}
        {actionabilityState === 'NO_ACTION' && (
          <div className="mb-4 rounded border border-[#f59e0b]/30 bg-[#f59e0b]/5 px-3 py-2">
            <p className="text-[10px] uppercase tracking-wider text-[#f59e0b]">No-action unlock conditions</p>
            <div className="mt-1 space-y-1">
              {noActionUnlockConditions.map((line) => (
                <p key={line} className="text-[10px] text-[#f3e3c2]">- {line}</p>
              ))}
            </div>
          </div>
        )}

        {!data || data.items.length === 0 ? (
          <div className="text-[#949ba5]">
            {hasContractGateSuppression
              ? 'No eligible opportunities (contract gate).'
              : hasDataQualitySuppression
                ? 'Opportunities are suppressed until critical data quality recovers.'
                : hasQualityFilter
                  ? 'No opportunities available after quality filtering.'
                  : 'No opportunities available yet.'}
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {data.items.map((item) => {
              const calibration = item.calibration ?? fallbackOpportunityCalibration()
              const expectancy = item.expectancy ?? fallbackOpportunityExpectancy()
              return (
              <div key={item.id} className="p-4 bg-[#0a0a0a]/65 border border-[#26272b] rounded-lg">
                <div className="flex items-start justify-between gap-2">
                  <div>
                    <div className="text-[13px] text-[#f3f3f3]">{item.theme_name}</div>
                    <div className="text-[9px] text-[#949ba5]/60 uppercase tracking-wider">
                      {item.direction} · {item.symbol || 'theme-level'}
                    </div>
                  </div>
                  <div className="px-2 py-1 rounded border border-[#1d2f3f] bg-[#081521]">
                    <div className="text-[16px] leading-none font-mono text-[#00a3ff]">{item.conviction_score}</div>
                    <div className="text-[8px] text-[#7fa8c7] uppercase tracking-wider">conviction</div>
                    <div className={`mt-1 inline-block rounded border px-1.5 py-0.5 text-[8px] uppercase tracking-wider ${calibrationQualityClass(calibration.quality)}`}>
                      {calibration.quality.toLowerCase()}
                    </div>
                  </div>
                </div>
                <p className="mt-3 text-[11px] text-[#cfd5de] leading-relaxed">{item.rationale}</p>
                <div className="mt-3 flex flex-wrap gap-1.5">
                  {item.supporting_factors.slice(0, 5).map((factor) => (
                    <span key={factor} className="px-2 py-1 text-[8px] uppercase tracking-wider border border-[#26272b] text-[#949ba5] rounded">
                      {factor}
                    </span>
                  ))}
                </div>
                <div className="mt-3 text-[9px] text-[#949ba5]/60">
                  Hit rate {(item.historical_hit_rate * 100).toFixed(0)}% · n={item.sample_size}
                </div>
                <div className="mt-2 text-[9px] text-[#949ba5]/70">
                  Calibrated p(correct) {formatProbability(calibration.probability_correct_direction)} ·
                  {' '}95% CI {formatProbability(calibration.ci95_low)}-{formatProbability(calibration.ci95_high)} ·
                  {' '}n={calibration.sample_size} · window {calibration.window || 'n/a'}
                </div>
                <div className="mt-1 text-[9px] text-[#949ba5]/70">
                  Expectancy {formatMaybePercent(expectancy.expected_move_pct)} ·
                  {' '}max adverse {formatMaybePercent(expectancy.max_adverse_move_pct)} ·
                  {' '}n={expectancy.sample_size} · {expectancy.basis.replace(/_/g, ' ')} · {expectancy.quality.toLowerCase()}
                </div>
                {(calibration.unavailable_reason || expectancy.unavailable_reason) && (
                  <div className="mt-1 text-[9px] text-[#949ba5]/70">
                    unavailable: {[calibration.unavailable_reason, expectancy.unavailable_reason].filter(Boolean).map((r) => formatUnavailableReason(r)).join(' · ')}
                  </div>
                )}
                {calibration.quality !== 'ROBUST' && (
                  <div className="mt-1 text-[9px] text-[#f59e0b]">
                    Limited calibration quality; treat this as exploratory risk.
                  </div>
                )}
              </div>
            )})}
          </div>
        )}
      </div>
    </div>
  )
}

function severityClass(severity: MarketFeedAlert['severity']) {
  if (severity === 'critical') return 'text-[#ff6b6b] border-[#ff6b6b]/40'
  if (severity === 'warning') return 'text-[#f59e0b] border-[#f59e0b]/40'
  return 'text-[#00a3ff] border-[#00a3ff]/40'
}

function InboxPage({
  alerts,
  onBack,
  onOpenSubscribe,
  notice,
}: {
  alerts: MarketFeedAlert[]
  onBack: () => void
  onOpenSubscribe: () => void
  notice: string | null
}) {
  return (
    <div className="min-h-screen bg-black text-[#f3f3f3] px-4 sm:px-8 py-10">
      <div className="max-w-4xl mx-auto">
        <div className="flex items-center justify-between mb-8">
          <h1 className="text-[11px] uppercase tracking-[0.3em] text-[#949ba5]">PXI /inbox</h1>
          <button
            onClick={onBack}
            className="text-[10px] uppercase tracking-[0.2em] border border-[#26272b] px-3 py-1.5 rounded text-[#949ba5] hover:text-[#f3f3f3]"
          >
            home
          </button>
        </div>

        {notice && (
          <div className="mb-4 p-3 border border-[#1f3e56] bg-[#091825] rounded text-[11px] text-[#9ec5e2]">
            {notice}
          </div>
        )}

        <div className="mb-4 p-4 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg flex items-center justify-between gap-3">
          <div>
            <div className="text-[10px] uppercase tracking-wider text-[#949ba5]/60">Email Digest</div>
            <div className="text-[12px] text-[#d7dbe1]">Subscribe for the daily 8:00 AM ET market digest.</div>
          </div>
          <button
            onClick={onOpenSubscribe}
            className="px-3 py-2 border border-[#00a3ff] text-[#00a3ff] rounded text-[10px] uppercase tracking-[0.2em]"
          >
            subscribe
          </button>
        </div>

        {alerts.length === 0 ? (
          <div className="text-[#949ba5]">No alerts recorded yet.</div>
        ) : (
          <div className="space-y-3">
            {alerts.map((alert) => (
              <div key={alert.id} className="p-4 bg-[#0a0a0a]/65 border border-[#26272b] rounded-lg">
                <div className="flex items-start justify-between gap-2">
                  <div>
                    <div className="text-[13px] text-[#f3f3f3]">{alert.title}</div>
                    <div className="text-[10px] text-[#949ba5]/60 uppercase tracking-wider">
                      {alert.event_type.replace('_', ' ')} · {new Date(alert.created_at).toLocaleString()}
                    </div>
                  </div>
                  <span className={`px-2 py-1 border rounded text-[8px] uppercase tracking-wider ${severityClass(alert.severity)}`}>
                    {alert.severity}
                  </span>
                </div>
                <p className="mt-2 text-[11px] text-[#cfd5de]">{alert.body}</p>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function EmailSubscribeModal({
  onClose,
  onSuccess,
}: {
  onClose: () => void
  onSuccess: (message: string) => void
}) {
  const [email, setEmail] = useState('')
  const [types, setTypes] = useState<Array<MarketFeedAlert['event_type']>>([
    'regime_change',
    'threshold_cross',
    'opportunity_spike',
    'freshness_warning',
  ])
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const toggleType = (type: MarketFeedAlert['event_type']) => {
    setTypes((prev) => {
      if (prev.includes(type)) return prev.filter((item) => item !== type)
      return [...prev, type]
    })
  }

  const submit = async () => {
    setSubmitting(true)
    setError(null)
    try {
      const res = await fetchApi('/api/alerts/subscribe/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email,
          types,
          cadence: 'daily_8am_et',
        }),
      })

      const payload = await res.json().catch(() => ({} as { error?: string }))
      if (!res.ok) {
        throw new Error(payload.error || 'Subscription failed')
      }

      onSuccess('Verification email sent. Use the link in your inbox to activate.')
      onClose()
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Subscription failed'
      setError(message)
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="fixed inset-0 z-[80] bg-black/80 backdrop-blur-sm flex items-center justify-center px-4">
      <div className="w-full max-w-md p-5 bg-[#090909] border border-[#26272b] rounded-lg">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-[11px] uppercase tracking-[0.25em] text-[#949ba5]">Email Alerts</h2>
          <button onClick={onClose} className="text-[10px] uppercase tracking-[0.2em] text-[#949ba5] hover:text-[#f3f3f3]">
            close
          </button>
        </div>

        <label className="block text-[9px] uppercase tracking-wider text-[#949ba5]/60 mb-1">Email</label>
        <input
          type="email"
          value={email}
          onChange={(event) => setEmail(event.target.value)}
          className="w-full bg-[#111] border border-[#26272b] rounded px-3 py-2 text-[12px] text-[#f3f3f3] outline-none focus:border-[#00a3ff]"
          placeholder="you@example.com"
        />

        <div className="mt-4">
          <div className="text-[9px] uppercase tracking-wider text-[#949ba5]/60 mb-2">Event types</div>
          <div className="grid grid-cols-2 gap-2">
            {(['regime_change', 'threshold_cross', 'opportunity_spike', 'freshness_warning'] as const).map((type) => (
              <button
                key={type}
                onClick={() => toggleType(type)}
                className={`px-2 py-1.5 text-[9px] uppercase tracking-wider border rounded ${
                  types.includes(type)
                    ? 'border-[#00a3ff] text-[#00a3ff]'
                    : 'border-[#26272b] text-[#949ba5]'
                }`}
              >
                {type.replace('_', ' ')}
              </button>
            ))}
          </div>
        </div>

        {error && <p className="mt-3 text-[10px] text-[#ff6b6b]">{error}</p>}

        <div className="mt-4 flex justify-end">
          <button
            onClick={submit}
            disabled={submitting || !email || types.length === 0}
            className="px-3 py-2 rounded border border-[#00a3ff] text-[#00a3ff] text-[10px] uppercase tracking-[0.2em] disabled:opacity-40"
          >
            {submitting ? 'sending...' : 'send verify link'}
          </button>
        </div>
      </div>
    </div>
  )
}

// ============== CSV Export Button ==============
function ExportButton() {
  const [exporting, setExporting] = useState(false)

  const handleExport = async () => {
    setExporting(true)
    try {
      const response = await fetchApi('/api/export/history?format=csv&days=365')
      if (!response.ok) throw new Error('Export failed')

      const blob = await response.blob()
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `pxi-history-${new Date().toISOString().split('T')[0]}.csv`
      document.body.appendChild(a)
      a.click()
      window.URL.revokeObjectURL(url)
      document.body.removeChild(a)
    } catch (err) {
      console.error('Export failed:', err)
    } finally {
      setExporting(false)
    }
  }

  return (
    <button
      onClick={handleExport}
      disabled={exporting}
      className="px-3 py-1.5 bg-[#0a0a0a] border border-[#26272b] rounded text-[10px] text-[#949ba5] uppercase tracking-wider hover:border-[#949ba5]/50 hover:text-[#f3f3f3] transition-colors disabled:opacity-50"
    >
      {exporting ? 'Exporting...' : 'Export CSV'}
    </button>
  )
}

// ============== Alert History Component ==============
function AlertHistoryPanel({
  alerts,
  accuracy,
  onClose,
  inPage = false
}: {
  alerts: AlertData[]
  accuracy: Record<string, { total: number; accuracy_7d: number | null; avg_return_7d: number }>
  onClose: () => void
  inPage?: boolean
}) {
  const [filterType, setFilterType] = useState<string | null>(null)

  const filteredAlerts = filterType
    ? alerts.filter(a => a.alert_type === filterType)
    : alerts

  const alertTypes = [...new Set(alerts.map(a => a.alert_type))]

  const getSeverityStyles = (severity: string) => {
    switch (severity) {
      case 'critical': return 'border-[#ff6b6b]/40 bg-[#ff6b6b]/5'
      case 'warning': return 'border-[#f59e0b]/40 bg-[#f59e0b]/5'
      default: return 'border-[#00a3ff]/20 bg-[#00a3ff]/5'
    }
  }

  const getSeverityDot = (severity: string) => {
    switch (severity) {
      case 'critical': return 'bg-[#ff6b6b]'
      case 'warning': return 'bg-[#f59e0b]'
      default: return 'bg-[#00a3ff]'
    }
  }

  const formatReturn = (val: number | null) => {
    if (val === null) return '—'
    const color = val >= 0 ? 'text-[#00c896]' : 'text-[#ff6b6b]'
    return <span className={color}>{val >= 0 ? '+' : ''}{val.toFixed(2)}%</span>
  }

  const formatType = (type: string) =>
    type.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())

  const containerClass = inPage
    ? 'min-h-screen bg-black text-[#f3f3f3] px-4 pt-8 pb-10'
    : 'fixed inset-0 bg-black/90 z-50 overflow-y-auto'

  return (
    <div className={containerClass}>
      <div className={inPage ? 'max-w-4xl mx-auto' : 'min-h-screen px-4 py-8 max-w-4xl mx-auto'}>
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-[#f3f3f3] text-lg font-light tracking-wider">ALERT HISTORY</h2>
            <p className="text-[#949ba5]/60 text-[10px] tracking-wide mt-1">
              Historical alerts with forward return analysis
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-[#949ba5] hover:text-[#f3f3f3] transition-colors p-2"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Alert Type Accuracy Summary */}
        {Object.keys(accuracy).length > 0 && (
          <div className="mb-6 p-4 bg-[#0a0a0a]/80 border border-[#1a1a1a] rounded-lg">
            <h3 className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider mb-3">
              Alert Accuracy by Type
            </h3>
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
              {Object.entries(accuracy).map(([type, stats]) => (
                <div key={type} className="bg-[#0f0f0f] rounded px-3 py-2 border border-[#1a1a1a]">
                  <div className="text-[10px] text-[#949ba5] mb-1">{formatType(type)}</div>
                  <div className="flex items-center gap-2">
                    {stats.accuracy_7d !== null ? (
                      <span className={`text-sm font-mono ${stats.accuracy_7d >= 60 ? 'text-[#00c896]' : stats.accuracy_7d >= 50 ? 'text-[#f59e0b]' : 'text-[#ff6b6b]'}`}>
                        {Math.round(stats.accuracy_7d)}%
                      </span>
                    ) : (
                      <span className="text-sm font-mono text-[#949ba5]/50">—</span>
                    )}
                    <span className="text-[8px] text-[#949ba5]/40">n={stats.total}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Filters */}
        <div className="flex flex-wrap gap-2 mb-4">
          <button
            onClick={() => setFilterType(null)}
            className={`px-3 py-1 text-[10px] uppercase tracking-wider rounded transition-colors ${
              !filterType
                ? 'bg-[#00a3ff]/20 text-[#00a3ff] border border-[#00a3ff]/30'
                : 'bg-[#1a1a1a] text-[#949ba5] border border-[#26272b] hover:border-[#949ba5]/30'
            }`}
          >
            All
          </button>
          {alertTypes.map(type => (
            <button
              key={type}
              onClick={() => setFilterType(type)}
              className={`px-3 py-1 text-[10px] uppercase tracking-wider rounded transition-colors ${
                filterType === type
                  ? 'bg-[#00a3ff]/20 text-[#00a3ff] border border-[#00a3ff]/30'
                  : 'bg-[#1a1a1a] text-[#949ba5] border border-[#26272b] hover:border-[#949ba5]/30'
              }`}
            >
              {formatType(type)}
            </button>
          ))}
        </div>

        {/* Alerts List */}
        <div className="space-y-2">
          {filteredAlerts.length === 0 ? (
            <div className="text-center py-12 text-[#949ba5]/50 text-sm">
              No alerts found
            </div>
          ) : (
            filteredAlerts.map(alert => (
              <div
                key={alert.id}
                className={`p-4 border rounded-lg ${getSeverityStyles(alert.severity)}`}
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      <span className={`w-2 h-2 rounded-full ${getSeverityDot(alert.severity)}`} />
                      <span className="text-[10px] text-[#949ba5]/60 uppercase tracking-wider">
                        {formatType(alert.alert_type)}
                      </span>
                      <span className="text-[9px] text-[#949ba5]/40">
                        {new Date(alert.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}
                      </span>
                    </div>
                    <p className="text-[#f3f3f3] text-sm leading-relaxed">
                      {alert.message}
                    </p>
                    {alert.pxi_score !== null && (
                      <div className="mt-2 text-[10px] text-[#949ba5]/60">
                        PXI at alert: <span className="text-[#00a3ff] font-mono">{alert.pxi_score.toFixed(1)}</span>
                      </div>
                    )}
                  </div>
                  {(alert.forward_return_7d !== null || alert.forward_return_30d !== null) && (
                    <div className="flex-shrink-0 text-right">
                      <div className="text-[8px] text-[#949ba5]/40 uppercase tracking-wider mb-1">
                        Forward Returns
                      </div>
                      {alert.forward_return_7d !== null && (
                        <div className="text-[10px]">
                          <span className="text-[#949ba5]/50">7d:</span> {formatReturn(alert.forward_return_7d)}
                        </div>
                      )}
                      {alert.forward_return_30d !== null && (
                        <div className="text-[10px]">
                          <span className="text-[#949ba5]/50">30d:</span> {formatReturn(alert.forward_return_30d)}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>
            ))
          )}
        </div>

        {/* Subscribe CTA */}
        <div className="mt-8 p-4 border border-dashed border-[#26272b] rounded-lg text-center">
          <p className="text-[#949ba5]/60 text-sm mb-2">
            Get alerts delivered to your inbox
          </p>
          <button className="px-4 py-2 bg-[#00a3ff]/10 border border-[#00a3ff]/30 rounded text-[10px] text-[#00a3ff] uppercase tracking-wider hover:bg-[#00a3ff]/20 transition-colors">
            Coming Soon
          </button>
        </div>
      </div>
    </div>
  )
}

function MLPredictionsCard({ ensemble, accuracy }: { ensemble: EnsembleData | null; accuracy?: MLAccuracyData | null }) {
  if (!ensemble) return null

  const formatChange = (val: number | null) => {
    if (val === null) return '—'
    return `${val >= 0 ? '+' : ''}${val.toFixed(1)}`
  }

  const getDirectionColor = (dir: string | null) => {
    if (!dir) return 'text-[#949ba5]'
    if (dir === 'STRONG_UP' || dir === 'UP') return 'text-[#00a3ff]'
    if (dir === 'STRONG_DOWN' || dir === 'DOWN') return 'text-[#ff6b6b]'
    return 'text-[#949ba5]'
  }

  const getDirectionIcon = (dir: string | null) => {
    if (!dir || dir === 'FLAT') return '→'
    if (dir === 'STRONG_UP') return '↑↑'
    if (dir === 'UP') return '↑'
    if (dir === 'DOWN') return '↓'
    if (dir === 'STRONG_DOWN') return '↓↓'
    return '→'
  }

  const getConfidenceColor = (conf: string | null) => {
    if (conf === 'HIGH') return 'text-[#00c896]'
    if (conf === 'MEDIUM') return 'text-[#f59e0b]'
    if (conf === 'LOW') return 'text-[#ff6b6b]'
    return 'text-[#949ba5]'
  }

  const { pxi_change_7d, pxi_change_30d } = ensemble.ensemble.predictions

  return (
    <div className="w-full mt-6 sm:mt-8">
      <div className="text-[10px] sm:text-[11px] text-[#949ba5]/50 uppercase tracking-widest mb-4 text-center">
        ML Ensemble Prediction
      </div>

      {/* Main ensemble predictions */}
      <div className="flex justify-center gap-8 sm:gap-12 mb-4">
        <div className="text-center">
          <div className="text-[10px] text-[#949ba5]/50 uppercase tracking-wider mb-2">7 Day</div>
          <div className={`text-2xl sm:text-3xl font-light flex items-center justify-center gap-2 ${getDirectionColor(pxi_change_7d.direction)}`}>
            {formatChange(pxi_change_7d.value)}
            <span className="text-lg">{getDirectionIcon(pxi_change_7d.direction)}</span>
          </div>
          <div className={`text-[9px] mt-1 ${getConfidenceColor(pxi_change_7d.confidence)}`}>
            {pxi_change_7d.confidence || '—'} confidence
          </div>
        </div>

        <div className="text-center">
          <div className="text-[10px] text-[#949ba5]/50 uppercase tracking-wider mb-2">30 Day</div>
          <div className={`text-2xl sm:text-3xl font-light flex items-center justify-center gap-2 ${getDirectionColor(pxi_change_30d.direction)}`}>
            {formatChange(pxi_change_30d.value)}
            <span className="text-lg">{getDirectionIcon(pxi_change_30d.direction)}</span>
          </div>
          <div className={`text-[9px] mt-1 ${getConfidenceColor(pxi_change_30d.confidence)}`}>
            {pxi_change_30d.confidence || '—'} confidence
          </div>
        </div>
      </div>

      {/* Component breakdown */}
      <div className="grid grid-cols-2 gap-3 mt-4">
        <div className="bg-[#0a0a0a]/40 rounded px-3 py-2 border border-[#1a1a1a]">
          <div className="text-[8px] text-[#949ba5]/40 uppercase tracking-wider mb-1">XGBoost (60%)</div>
          <div className="flex justify-between text-[10px]">
            <span className="text-[#949ba5]/50">7d</span>
            <span className={getDirectionColor(pxi_change_7d.components.xgboost !== null && pxi_change_7d.components.xgboost > 0 ? 'UP' : pxi_change_7d.components.xgboost !== null && pxi_change_7d.components.xgboost < 0 ? 'DOWN' : 'FLAT')}>
              {formatChange(pxi_change_7d.components.xgboost)}
            </span>
          </div>
          <div className="flex justify-between text-[10px]">
            <span className="text-[#949ba5]/50">30d</span>
            <span className={getDirectionColor(pxi_change_30d.components.xgboost !== null && pxi_change_30d.components.xgboost > 0 ? 'UP' : pxi_change_30d.components.xgboost !== null && pxi_change_30d.components.xgboost < 0 ? 'DOWN' : 'FLAT')}>
              {formatChange(pxi_change_30d.components.xgboost)}
            </span>
          </div>
        </div>
        <div className="bg-[#0a0a0a]/40 rounded px-3 py-2 border border-[#1a1a1a]">
          <div className="text-[8px] text-[#949ba5]/40 uppercase tracking-wider mb-1">LSTM (40%)</div>
          <div className="flex justify-between text-[10px]">
            <span className="text-[#949ba5]/50">7d</span>
            <span className={getDirectionColor(pxi_change_7d.components.lstm !== null && pxi_change_7d.components.lstm > 0 ? 'UP' : pxi_change_7d.components.lstm !== null && pxi_change_7d.components.lstm < 0 ? 'DOWN' : 'FLAT')}>
              {formatChange(pxi_change_7d.components.lstm)}
            </span>
          </div>
          <div className="flex justify-between text-[10px]">
            <span className="text-[#949ba5]/50">30d</span>
            <span className={getDirectionColor(pxi_change_30d.components.lstm !== null && pxi_change_30d.components.lstm > 0 ? 'UP' : pxi_change_30d.components.lstm !== null && pxi_change_30d.components.lstm < 0 ? 'DOWN' : 'FLAT')}>
              {formatChange(pxi_change_30d.components.lstm)}
            </span>
          </div>
        </div>
      </div>

      {/* Accuracy display */}
      <MLAccuracyBadge accuracy={accuracy ?? null} />

      <div className="text-[8px] text-[#949ba5]/20 text-center mt-1">
        Weighted ensemble • {ensemble.interpretation.d7.note}
      </div>
    </div>
  )
}

function SpecPage({ onClose, inPage = false }: { onClose: () => void; inPage?: boolean }) {
  const wrapperClass = inPage
    ? 'min-h-screen bg-black text-[#f3f3f3] px-4 sm:px-8 py-16 overflow-auto'
    : 'min-h-screen bg-black text-[#f3f3f3] px-4 sm:px-8 py-16 overflow-auto'

  return (
    <div className={wrapperClass}>
      <div className="max-w-2xl mx-auto">
        {/* Header */}
        <div className="flex justify-between items-center mb-12">
          <div className="text-[10px] sm:text-[11px] font-mono tracking-[0.2em] text-[#949ba5] uppercase">
            PXI<span className="text-[#00a3ff]">/</span>spec
          </div>
          <button
            onClick={onClose}
            className="text-[10px] text-[#949ba5]/50 hover:text-[#f3f3f3] uppercase tracking-widest transition-colors"
          >
            Close
          </button>
        </div>

        {/* Title */}
        <h1 className="text-2xl sm:text-3xl font-extralight mb-2 tracking-tight">
          Protocol Specification
        </h1>
        <p className="text-[11px] text-[#949ba5]/60 mb-12 uppercase tracking-widest">
          Macro Market Strength Index — Quantitative Framework v1.3
        </p>

        {/* Two-Layer Architecture */}
        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Two-Layer Architecture</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            Two-layer system separating descriptive state from actionable signals.
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3 border-l-2 border-[#00a3ff]">
              <div className="text-[11px] font-medium uppercase tracking-wide mb-2">PXI-State</div>
              <p className="text-[10px] text-[#949ba5]/70 mb-2">Descriptive layer for monitoring macro conditions</p>
              <ul className="text-[9px] text-[#949ba5]/60 space-y-1">
                <li>• Composite score (0-100)</li>
                <li>• Category breakdowns</li>
                <li>• Market regime classification</li>
                <li>• Divergence alerts</li>
              </ul>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3 border-l-2 border-[#00c896]">
              <div className="text-[11px] font-medium uppercase tracking-wide mb-2">PXI-Signal</div>
              <p className="text-[10px] text-[#949ba5]/70 mb-2">Actionable layer for risk allocation</p>
              <ul className="text-[9px] text-[#949ba5]/60 space-y-1">
                <li>• Risk allocation (0-100%)</li>
                <li>• Signal type classification</li>
                <li>• Volatility percentile</li>
                <li>• Adjustment explanations</li>
              </ul>
            </div>
          </div>
        </section>

        {/* Definition */}
        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Definition</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            PXI (Pamp Index) is a composite indicator measuring macro market strength across seven
            dimensions. It synthesizes volatility, credit conditions, breadth, positioning, macro,
            global, and crypto signals into a single 0-100 score using 5-year rolling percentiles.
          </p>
          <div className="bg-[#0a0a0a] rounded px-4 py-3 font-mono text-[12px] text-[#f3f3f3]/80">
            PXI = Σ(Cᵢ × Wᵢ) / Σ(Wᵢ)
          </div>
          <p className="text-[10px] text-[#949ba5]/50 mt-2">
            Where Cᵢ = category score [0,100] and Wᵢ = category weight
          </p>
        </section>

        {/* Categories */}
        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Category Composition</h2>
          <div className="space-y-4">
            {[
              { name: 'Volatility', weight: 20, indicators: 'VIX, VIX term structure, AAII sentiment, GEX (gamma exposure)', formula: '100 - percentile(VIX, 5yr)' },
              { name: 'Credit', weight: 20, indicators: 'HY OAS, IG OAS, 2s10s curve, BBB-AAA spread', formula: '100 - percentile(HY_spread, 5yr)' },
              { name: 'Breadth', weight: 15, indicators: 'RSP/SPY ratio, sector breadth, small/mid cap strength', formula: 'percentile(breadth_composite, 5yr)' },
              { name: 'Positioning', weight: 15, indicators: 'Fed balance sheet, TGA, reverse repo, net liquidity', formula: 'percentile(net_liq, 5yr)' },
              { name: 'Macro', weight: 10, indicators: 'ISM manufacturing, jobless claims, CFNAI', formula: 'percentile(macro_composite, 5yr)' },
              { name: 'Global', weight: 10, indicators: 'DXY, copper/gold ratio, EM spreads, AUD/JPY', formula: 'percentile(global_composite, 5yr)' },
              { name: 'Crypto', weight: 10, indicators: 'BTC vs 200DMA, stablecoin mcap, BTC ETF flows, funding rates', formula: 'percentile(crypto_composite, 5yr)' },
            ].map((cat) => (
              <div key={cat.name} className="bg-[#0a0a0a]/60 rounded px-4 py-3">
                <div className="flex justify-between items-center mb-2">
                  <span className="text-[11px] font-medium uppercase tracking-wide">{cat.name}</span>
                  <span className="text-[10px] text-[#949ba5]/50">w = {cat.weight}%</span>
                </div>
                <p className="text-[10px] text-[#949ba5]/70 mb-1">{cat.indicators}</p>
                <code className="text-[9px] text-[#00a3ff]/60 font-mono">{cat.formula}</code>
              </div>
            ))}
          </div>
        </section>

        {/* PXI-Signal Trading Policy */}
        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">PXI-Signal Trading Policy</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            The signal layer applies a canonical trading policy to convert PXI state into risk allocation.
            Base allocation scales linearly from 30% (PXI=0) to 100% (PXI=100).
          </p>
          <div className="bg-[#0a0a0a] rounded px-4 py-3 font-mono text-[11px] text-[#f3f3f3]/70 space-y-1 mb-4">
            <div>Base = 0.3 + (PXI / 100) × 0.7</div>
            <div className="text-[#ff6b6b]">If Regime = RISK_OFF → allocation × 0.5</div>
            <div className="text-[#f59e0b]">If Regime = TRANSITION → allocation × 0.75</div>
            <div className="text-[#f59e0b]">If Δ7d &lt; -10 → allocation × 0.8</div>
            <div className="text-[#f59e0b]">If VIX_percentile &gt; 80 → allocation × 0.7</div>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-[10px]">
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-2 text-center">
              <div className="text-[#00c896] font-medium">FULL_RISK</div>
              <div className="text-[#949ba5]/60">≥80% alloc</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-2 text-center">
              <div className="text-[#f59e0b] font-medium">REDUCED</div>
              <div className="text-[#949ba5]/60">50-80% alloc</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-2 text-center">
              <div className="text-[#ff6b6b] font-medium">RISK_OFF</div>
              <div className="text-[#949ba5]/60">30-50% alloc</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-2 text-center">
              <div className="text-[#dc2626] font-medium">DEFENSIVE</div>
              <div className="text-[#949ba5]/60">&lt;30% alloc</div>
            </div>
          </div>
        </section>

        {/* Regime Detection */}
        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Regime Detection</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            Market regime is classified using a voting system with percentile-based thresholds
            calculated over a 5-year rolling window. Each indicator votes RISK_ON, RISK_OFF, or NEUTRAL.
          </p>
          <div className="bg-[#0a0a0a] rounded px-4 py-3 font-mono text-[11px] text-[#f3f3f3]/70 space-y-1">
            <div>VIX &lt; 30th pct → RISK_ON | &gt; 70th pct → RISK_OFF</div>
            <div>HY_OAS &lt; 30th pct → RISK_ON | &gt; 70th pct → RISK_OFF</div>
            <div>Breadth &gt; 60% → RISK_ON | &lt; 40% → RISK_OFF (direct)</div>
            <div>Yield_curve &gt; 60th pct → RISK_ON | &lt; 20th pct → RISK_OFF</div>
            <div>DXY &lt; 40th pct → RISK_ON | &gt; 70th pct → RISK_OFF</div>
          </div>
          <p className="text-[10px] text-[#949ba5]/50 mt-2">
            Regime = RISK_ON if votes ≥ 3 (or 2 with 0 RISK_OFF) | RISK_OFF if votes ≥ 3 (or 2 with 0 RISK_ON) | else TRANSITION
          </p>
        </section>

        {/* Backtest Results */}
        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Backtest Results (2022-2024)</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            Walk-forward backtest comparing PXI-Signal strategy against baseline strategies.
            583 trading days, returns measured on SPY.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-[10px]">
              <thead>
                <tr className="text-[#949ba5]/50 uppercase tracking-wider">
                  <th className="text-left py-2">Strategy</th>
                  <th className="text-right py-2">CAGR</th>
                  <th className="text-right py-2">Vol</th>
                  <th className="text-right py-2">Sharpe</th>
                  <th className="text-right py-2">Max DD</th>
                </tr>
              </thead>
              <tbody className="text-[#f3f3f3]/80 font-mono">
                <tr className="border-t border-[#26272b]">
                  <td className="py-2 text-[#00c896]">PXI-Signal</td>
                  <td className="text-right">12.3%</td>
                  <td className="text-right text-[#00c896]">6.2%</td>
                  <td className="text-right text-[#00c896]">2.00</td>
                  <td className="text-right text-[#00c896]">6.3%</td>
                </tr>
                <tr className="border-t border-[#26272b]">
                  <td className="py-2">200DMA</td>
                  <td className="text-right">15.3%</td>
                  <td className="text-right">12.4%</td>
                  <td className="text-right">1.24</td>
                  <td className="text-right">13.8%</td>
                </tr>
                <tr className="border-t border-[#26272b]">
                  <td className="py-2 text-[#949ba5]">Buy-and-Hold</td>
                  <td className="text-right text-[#949ba5]">17.0%</td>
                  <td className="text-right text-[#ff6b6b]">16.0%</td>
                  <td className="text-right text-[#949ba5]">1.06</td>
                  <td className="text-right text-[#ff6b6b]">17.5%</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div className="mt-4 grid grid-cols-3 gap-2 text-[10px]">
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-2 text-center">
              <div className="text-[#00c896] font-mono text-lg">+0.94</div>
              <div className="text-[#949ba5]/50">Sharpe vs B&H</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-2 text-center">
              <div className="text-[#00c896] font-mono text-lg">-9.9%</div>
              <div className="text-[#949ba5]/50">Vol reduction</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-2 text-center">
              <div className="text-[#00c896] font-mono text-lg">-11.2%</div>
              <div className="text-[#949ba5]/50">Max DD improve</div>
            </div>
          </div>
        </section>

        {/* Empirical Model */}
        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Forward Return Model</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            Conditional probability distributions derived from historical data.
            Returns bucketed by PXI score at observation time.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-[10px]">
              <thead>
                <tr className="text-[#949ba5]/50 uppercase tracking-wider">
                  <th className="text-left py-2">PXI Range</th>
                  <th className="text-right py-2">n</th>
                  <th className="text-right py-2">E[R₇ᵈ]</th>
                  <th className="text-right py-2">E[R₃₀ᵈ]</th>
                  <th className="text-right py-2">P(R₃₀ᵈ &gt; 0)</th>
                </tr>
              </thead>
              <tbody className="text-[#f3f3f3]/80 font-mono">
                <tr className="border-t border-[#26272b]">
                  <td className="py-2">0–20</td>
                  <td className="text-right">44</td>
                  <td className="text-right text-[#00a3ff]">+0.59%</td>
                  <td className="text-right text-[#00a3ff]">+1.64%</td>
                  <td className="text-right">70%</td>
                </tr>
                <tr className="border-t border-[#26272b]">
                  <td className="py-2">20–40</td>
                  <td className="text-right">222</td>
                  <td className="text-right text-[#00a3ff]">+0.70%</td>
                  <td className="text-right text-[#00a3ff]">+2.66%</td>
                  <td className="text-right">75%</td>
                </tr>
                <tr className="border-t border-[#26272b]">
                  <td className="py-2">40–60</td>
                  <td className="text-right">580</td>
                  <td className="text-right text-[#00a3ff]">+0.38%</td>
                  <td className="text-right text-[#00a3ff]">+1.64%</td>
                  <td className="text-right">75%</td>
                </tr>
                <tr className="border-t border-[#26272b]">
                  <td className="py-2">60–80</td>
                  <td className="text-right">226</td>
                  <td className="text-right">+0.17%</td>
                  <td className="text-right">+0.84%</td>
                  <td className="text-right">73%</td>
                </tr>
                <tr className="border-t border-[#26272b]">
                  <td className="py-2 text-[#949ba5]">80–100</td>
                  <td className="text-right text-[#949ba5]">27</td>
                  <td className="text-right text-[#ff6b6b]">−0.23%</td>
                  <td className="text-right text-[#949ba5]">+0.13%</td>
                  <td className="text-right text-[#949ba5]">63%</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>

        {/* ML Predictions */}
        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">ML Ensemble (v1.3)</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            Weighted ensemble combining two models for 7-day and 30-day PXI predictions.
            Models trained locally, deployed as JSON weights for edge inference.
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-4">
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3 border-l-2 border-[#00a3ff]">
              <div className="text-[11px] font-medium uppercase tracking-wide mb-2">XGBoost (60%)</div>
              <p className="text-[10px] text-[#949ba5]/70 mb-2">Gradient boosted trees for tabular features</p>
              <ul className="text-[9px] text-[#949ba5]/60 space-y-1">
                <li>• 36 engineered features</li>
                <li>• Momentum, dispersion, extremes</li>
                <li>• Rolling statistics (7d, 14d, 30d)</li>
                <li>• Point-in-time prediction</li>
              </ul>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3 border-l-2 border-[#f59e0b]">
              <div className="text-[11px] font-medium uppercase tracking-wide mb-2">LSTM (40%)</div>
              <p className="text-[10px] text-[#949ba5]/70 mb-2">Recurrent neural network for sequences</p>
              <ul className="text-[9px] text-[#949ba5]/60 space-y-1">
                <li>• 20-day input sequences</li>
                <li>• 12 features per timestep</li>
                <li>• 32 hidden units, single layer</li>
                <li>• Temporal pattern recognition</li>
              </ul>
            </div>
          </div>
          <div className="bg-[#0a0a0a] rounded px-4 py-3 font-mono text-[11px] text-[#f3f3f3]/70 space-y-1">
            <div>Ensemble = 0.6 × XGBoost + 0.4 × LSTM</div>
            <div>Confidence: HIGH (same direction+magnitude) | MEDIUM (same direction) | LOW (disagree)</div>
            <div>Direction: STRONG_UP | UP | FLAT | DOWN | STRONG_DOWN</div>
          </div>
          <p className="text-[10px] text-[#949ba5]/50 mt-2">
            Thresholds: |Δ| &gt; 5 = STRONG, |Δ| &gt; 2 = directional, else FLAT
          </p>

          {/* XGBoost Backtest Metrics */}
          <div className="mt-4 bg-[#0a0a0a]/60 rounded px-4 py-3 border border-[#1a1a1a]">
            <div className="text-[10px] text-[#00a3ff]/80 uppercase tracking-wider mb-2">XGBoost In-Sample Backtest</div>
            <div className="grid grid-cols-2 gap-4 text-[10px]">
              <div>
                <div className="text-[#949ba5]/60 mb-1">7-Day Predictions</div>
                <div className="space-y-0.5 text-[#949ba5]">
                  <div>Direction Accuracy: <span className="text-[#22c55e]">77%</span></div>
                  <div>R²: <span className="text-[#949ba5]/80">0.64</span></div>
                  <div>MAE: <span className="text-[#949ba5]/80">3.2 pts</span></div>
                </div>
              </div>
              <div>
                <div className="text-[#949ba5]/60 mb-1">30-Day Predictions</div>
                <div className="space-y-0.5 text-[#949ba5]">
                  <div>Direction Accuracy: <span className="text-[#22c55e]">90%</span></div>
                  <div>R²: <span className="text-[#949ba5]/80">0.88</span></div>
                  <div>MAE: <span className="text-[#949ba5]/80">4.8 pts</span></div>
                </div>
              </div>
            </div>
            <p className="text-[9px] text-[#949ba5]/40 mt-2">
              ⚠️ In-sample metrics (trained on this data). Live OOS accuracy tracked via /api/ml/accuracy
            </p>
          </div>
        </section>

        {/* Divergence Detection */}
        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Divergence Alerts</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            Divergence signals flag conflicts between PXI score and market regime. Alerts include
            historical metrics: frequency, median forward returns, and false positive rates.
          </p>
          <div className="space-y-2 text-[11px]">
            {[
              { name: 'Stealth Weakness', condition: 'PXI < 30 ∧ VIX < 15', desc: 'Low index despite low vol — complacency risk' },
              { name: 'Resilient Strength', condition: 'PXI > 70 ∧ VIX > 25', desc: 'High index despite elevated vol — market absorbing fear' },
              { name: 'Rapid Deterioration', condition: 'ΔPXI₇ᵈ < -15 ∧ Regime = RISK_ON', desc: 'Sharp drop while regime stable — leading indicator' },
              { name: 'Hidden Risk', condition: 'Regime = RISK_ON ∧ PXI < 40', desc: 'Regime looks fine but underlying weakness' },
            ].map((alert) => (
              <div key={alert.name} className="bg-[#0a0a0a]/60 rounded px-4 py-2">
                <div className="flex justify-between items-center">
                  <span className="font-medium">{alert.name}</span>
                  <code className="text-[9px] text-[#00a3ff]/60 font-mono">{alert.condition}</code>
                </div>
                <p className="text-[10px] text-[#949ba5]/60 mt-1">{alert.desc}</p>
              </div>
            ))}
          </div>
        </section>

        {/* Signal Distribution */}
        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Historical Signal Distribution</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            Distribution of signal types over the backtest period (1100 observations).
          </p>
          <div className="grid grid-cols-4 gap-2 text-[10px]">
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-3 text-center">
              <div className="text-[#ff6b6b] font-mono text-lg">38%</div>
              <div className="text-[#949ba5]/50 text-[9px]">RISK_OFF</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-3 text-center">
              <div className="text-[#f59e0b] font-mono text-lg">36%</div>
              <div className="text-[#949ba5]/50 text-[9px]">REDUCED</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-3 text-center">
              <div className="text-[#dc2626] font-mono text-lg">18%</div>
              <div className="text-[#949ba5]/50 text-[9px]">DEFENSIVE</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-3 text-center">
              <div className="text-[#00c896] font-mono text-lg">8%</div>
              <div className="text-[#949ba5]/50 text-[9px]">FULL_RISK</div>
            </div>
          </div>
        </section>

        {/* Interpretation */}
        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Interpretation Guide</h2>
          <div className="grid grid-cols-2 gap-4 text-[11px]">
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3">
              <div className="text-[#00a3ff] font-medium mb-1">0–40: Weak</div>
              <p className="text-[10px] text-[#949ba5]/70">Risk-off conditions. Historically favorable for forward returns (mean reversion).</p>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3">
              <div className="text-[#949ba5] font-medium mb-1">40–60: Neutral</div>
              <p className="text-[10px] text-[#949ba5]/70">Typical conditions. No strong directional bias.</p>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3">
              <div className="text-[#f59e0b] font-medium mb-1">60–80: Strong</div>
              <p className="text-[10px] text-[#949ba5]/70">Risk-on conditions. Watch for overextension.</p>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3">
              <div className="text-[#ff6b6b] font-medium mb-1">80–100: Extended</div>
              <p className="text-[10px] text-[#949ba5]/70">Historically poor forward returns. Elevated reversal risk.</p>
            </div>
          </div>
        </section>

        {/* Footer */}
        <div className="text-[9px] text-[#949ba5]/30 font-mono tracking-wider text-center uppercase pt-8 border-t border-[#26272b]">
          PXI/COMMAND Protocol v1.3
        </div>
      </div>
    </div>
  )
}

function App() {
  const [route, setRoute] = useState<AppRoute>(() => {
    if (typeof window === 'undefined') return '/'
    return normalizeRoute(window.location.pathname)
  })

  const [data, setData] = useState<PXIData | null>(null)
  const [prediction, setPrediction] = useState<PredictionData | null>(null)
  const [signal, setSignal] = useState<SignalData | null>(null)  // v1.1
  const [ensemble, setEnsemble] = useState<EnsembleData | null>(null)  // ML ensemble
  const [mlAccuracy, setMlAccuracy] = useState<MLAccuracyData | null>(null)  // v1.4: ML accuracy tracking
  const [historyData, setHistoryData] = useState<HistoryDataPoint[]>([])  // v1.4: Historical chart data
  const [historyRange, setHistoryRange] = useState<'7d' | '30d' | '90d'>('30d')  // v1.4: Chart range
  const [showOnboarding, setShowOnboarding] = useState(false)  // v1.4: Onboarding modal
  const [alertsData, setAlertsData] = useState<AlertsApiResponse | null>(null)  // v1.5: Alert history
  const [selectedCategory, setSelectedCategory] = useState<string | null>(null)  // v1.5: Category deep-dive
  const [signalsData, setSignalsData] = useState<SignalsData | null>(null)  // v1.5: PXI + Signals integration
  const [similarData, setSimilarData] = useState<SimilarPeriodsData | null>(null)  // v1.6: Similar periods
  const [backtestData, setBacktestData] = useState<BacktestData | null>(null)  // v1.6: Backtest performance
  const [planData, setPlanData] = useState<PlanData | null>(null)
  const [briefData, setBriefData] = useState<BriefData | null>(null)
  const [opportunitiesData, setOpportunitiesData] = useState<OpportunitiesResponse | null>(null)
  const [opportunityDiagnostics, setOpportunityDiagnostics] = useState<CalibrationDiagnosticsResponse | null>(null)
  const [opportunityHorizon, setOpportunityHorizon] = useState<'7d' | '30d'>('7d')
  const [alertsFeed, setAlertsFeed] = useState<AlertsFeedResponse | null>(null)
  const [showSubscribeModal, setShowSubscribeModal] = useState(false)
  const [subscriptionNotice, setSubscriptionNotice] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [menuOpen, setMenuOpen] = useState(false)
  const menuRef = useRef<HTMLDivElement>(null)

  const navigateTo = (nextRoute: AppRoute) => {
    const path = normalizeRoute(nextRoute)
    if (typeof window !== 'undefined') {
      if (normalizeRoute(window.location.pathname) !== path) {
        window.history.pushState({}, '', path)
      }
      setRoute(path)
    }
    setMenuOpen(false)
  }

  useEffect(() => {
    applyRouteMetadata(route)
  }, [route])

  // Keep onboarding opt-in via /guide instead of forcing a modal on first visit.
  useEffect(() => {
    try {
      const hasSeenOnboarding = localStorage.getItem('pxi_onboarding_complete')
      if (!hasSeenOnboarding) {
        localStorage.setItem('pxi_onboarding_complete', 'true')
      }
    } catch {
      // Ignore localStorage failures in private browsing environments.
    }
  }, [])

  // Close menu when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setMenuOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  // Support browser back/forward for route-based rendering
  useEffect(() => {
    const syncRoute = () => setRoute(normalizeRoute(window.location.pathname))
    window.addEventListener('popstate', syncRoute)
    return () => window.removeEventListener('popstate', syncRoute)
  }, [])

  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const verifyToken = params.get('verify_token')
    const unsubscribeToken = params.get('unsubscribe_token')
    if (!verifyToken && !unsubscribeToken) {
      return
    }

    const finalize = () => {
      const cleanUrl = `${window.location.pathname}`
      window.history.replaceState({}, '', cleanUrl)
      setRoute(normalizeRoute(window.location.pathname))
    }

    const run = async () => {
      try {
        if (verifyToken) {
          const response = await fetchApi('/api/alerts/subscribe/verify', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ token: verifyToken }),
          })
          if (!response.ok) {
            const payload = await response.json().catch(() => ({ error: 'Verification failed' }))
            throw new Error((payload as { error?: string }).error || 'Verification failed')
          }
          setSubscriptionNotice('Email alerts verified. Daily digest is active.')
        } else if (unsubscribeToken) {
          const response = await fetchApi('/api/alerts/unsubscribe', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ token: unsubscribeToken }),
          })
          if (!response.ok) {
            const payload = await response.json().catch(() => ({ error: 'Unsubscribe failed' }))
            throw new Error((payload as { error?: string }).error || 'Unsubscribe failed')
          }
          setSubscriptionNotice('You have been unsubscribed from PXI digest emails.')
        }
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : 'Token action failed'
        setSubscriptionNotice(message)
      } finally {
        finalize()
      }
    }

    run()
  }, [])

  useEffect(() => {
    const fetchPlan = getRouteFetchPlan(route)
    const hasWork = Object.values(fetchPlan).some(Boolean)
    const isHomeRoute = route === '/'

    if (!hasWork) {
      setLoading(false)
      setError(null)
      return
    }

    const fetchData = async () => {
      try {
        if (isHomeRoute) {
          setLoading(true)
        }

        const [pxiRes, signalRes, planRes, predRes, ensembleRes, accuracyRes, historyRes, alertsRes, briefRes, oppRes, oppDiagRes, inboxRes] = await Promise.all([
          fetchPlan.pxi ? fetchApi('/api/pxi') : Promise.resolve(null),
          fetchPlan.signal ? fetchApi('/api/signal').catch(() => null) : Promise.resolve(null),
          fetchPlan.plan ? fetchApi('/api/plan').catch(() => null) : Promise.resolve(null),
          fetchPlan.prediction ? fetchApi('/api/predict').catch(() => null) : Promise.resolve(null),
          fetchPlan.ensemble ? fetchApi('/api/ml/ensemble').catch(() => null) : Promise.resolve(null),
          fetchPlan.accuracy ? fetchApi('/api/ml/accuracy').catch(() => null) : Promise.resolve(null),
          fetchPlan.history ? fetchApi('/api/history?days=90').catch(() => null) : Promise.resolve(null),
          fetchPlan.alertsHistory ? fetchApi('/api/alerts?limit=50').catch(() => null) : Promise.resolve(null),
          fetchPlan.brief ? fetchApi('/api/brief?scope=market').catch(() => null) : Promise.resolve(null),
          fetchPlan.opportunities ? fetchApi(`/api/opportunities?horizon=${opportunityHorizon}&limit=20`).catch(() => null) : Promise.resolve(null),
          fetchPlan.opportunities ? fetchApi(`/api/diagnostics/calibration?metric=conviction&horizon=${opportunityHorizon}`).catch(() => null) : Promise.resolve(null),
          fetchPlan.inbox ? fetchApi('/api/alerts/feed?limit=50').catch(() => null) : Promise.resolve(null),
        ])

        if (fetchPlan.pxi) {
          if (!pxiRes?.ok) throw new Error('Failed to fetch')
          const pxiJson = await pxiRes.json()
          setData(pxiJson)
        }

        if (signalRes?.ok) {
          const signalJson = await signalRes.json()
          if (!signalJson.error) {
            setSignal(signalJson)
          }
        }

        if (planRes?.ok) {
          const planJson = await planRes.json() as PlanData
          if (planJson.setup_summary && planJson.action_now) {
            setPlanData({
              ...planJson,
              actionability_state: planJson.actionability_state || (planJson.opportunity_ref?.eligible_count === 0 ? 'NO_ACTION' : 'WATCH'),
              actionability_reason_codes: Array.isArray(planJson.actionability_reason_codes) ? planJson.actionability_reason_codes : [],
              action_now: {
                ...planJson.action_now,
                raw_signal_allocation_target: planJson.action_now.raw_signal_allocation_target ?? planJson.action_now.risk_allocation_target,
                risk_allocation_basis: planJson.action_now.risk_allocation_basis || 'penalized_playbook_target',
              },
              policy_state: planJson.policy_state ? {
                ...planJson.policy_state,
                rationale_codes: planJson.policy_state.rationale_codes || [],
              } : undefined,
              uncertainty: planJson.uncertainty || {
                headline: null,
                flags: {
                  stale_inputs: false,
                  limited_calibration: false,
                  limited_scenario_sample: false,
                },
              },
              consistency: planJson.consistency
                ? {
                    ...planJson.consistency,
                    components: planJson.consistency.components || {
                      base_score: 100,
                      structural_penalty: 0,
                      reliability_penalty: 0,
                    },
                  }
                : {
                    score: 100,
                    state: 'PASS',
                    violations: [],
                    components: {
                      base_score: 100,
                      structural_penalty: 0,
                      reliability_penalty: 0,
                    },
                  },
              trader_playbook: planJson.trader_playbook || {
                recommended_size_pct: { min: 25, target: 50, max: 65 },
                scenarios: [],
                benchmark_follow_through_7d: {
                  hit_rate: null,
                  sample_size: 0,
                  unavailable_reason: 'insufficient_sample',
                },
              },
            })
          }
        }

        if (predRes?.ok) {
          const predJson = await predRes.json()
          if (!predJson.error) {
            setPrediction(predJson)
          }
        }

        if (ensembleRes?.ok) {
          const ensembleJson = await ensembleRes.json()
          if (!ensembleJson.error) {
            setEnsemble(ensembleJson)
          }
        }

        if (accuracyRes?.ok) {
          const accuracyJson = await accuracyRes.json() as MLAccuracyApiResponse
          if (!accuracyJson.error) {
            const parsed = parseMLAccuracy(accuracyJson)
            if (parsed) {
              setMlAccuracy(parsed)
            }
          }
        }

        if (historyRes?.ok) {
          const historyJson = await historyRes.json() as { data?: HistoryDataPoint[]; error?: string }
          if (historyJson.data && Array.isArray(historyJson.data)) {
            setHistoryData(historyJson.data)
          }
        }

        if (alertsRes?.ok) {
          const alertsJson = await alertsRes.json() as AlertsApiResponse
          if (alertsJson.alerts) {
            setAlertsData(alertsJson)
          }
        }

        if (briefRes?.ok) {
          const briefJson = await briefRes.json() as BriefData
          if (briefJson.summary) {
            setBriefData({
              ...briefJson,
              policy_state: briefJson.policy_state || {
                stance: 'MIXED',
                risk_posture: 'neutral',
                conflict_state: 'MIXED',
                base_signal: 'REDUCED_RISK',
                regime_context: 'TRANSITION',
                rationale: 'fallback',
                rationale_codes: ['fallback'],
              },
              source_plan_as_of: briefJson.source_plan_as_of || briefJson.as_of,
              contract_version: briefJson.contract_version || 'legacy',
              consistency: briefJson.consistency || {
                score: 100,
                state: 'PASS',
                violations: [],
              },
              degraded_reason: briefJson.degraded_reason || null,
            })
          }
        }

        if (oppRes?.ok) {
          const oppJson = await oppRes.json() as OpportunitiesResponse
          if (Array.isArray(oppJson.items)) {
            const qualityFilteredCount = Number.isFinite(oppJson.quality_filtered_count as number) ? Number(oppJson.quality_filtered_count) : 0
            const coherenceSuppressedCount = Number.isFinite(oppJson.coherence_suppressed_count as number) ? Number(oppJson.coherence_suppressed_count) : 0
            const suppressedCount = Number.isFinite(oppJson.suppressed_count) ? oppJson.suppressed_count : 0
            setOpportunitiesData({
              ...oppJson,
              suppressed_count: suppressedCount,
              quality_filtered_count: qualityFilteredCount,
              coherence_suppressed_count: coherenceSuppressedCount,
              suppression_by_reason: oppJson.suppression_by_reason || {
                coherence_failed: coherenceSuppressedCount,
                quality_filtered: qualityFilteredCount,
                data_quality_suppressed: oppJson.degraded_reason === 'suppressed_data_quality' ? suppressedCount : 0,
              },
              quality_filter_rate: Number.isFinite(oppJson.quality_filter_rate as number) ? Number(oppJson.quality_filter_rate) : 0,
              coherence_fail_rate: Number.isFinite(oppJson.coherence_fail_rate as number) ? Number(oppJson.coherence_fail_rate) : 0,
              actionability_state: oppJson.actionability_state || (oppJson.items.length > 0 ? 'WATCH' : 'NO_ACTION'),
              actionability_reason_codes: Array.isArray(oppJson.actionability_reason_codes) ? oppJson.actionability_reason_codes : [],
              cta_enabled: typeof oppJson.cta_enabled === 'boolean' ? oppJson.cta_enabled : oppJson.items.length > 0,
              cta_disabled_reasons: Array.isArray(oppJson.cta_disabled_reasons) ? oppJson.cta_disabled_reasons : [],
            })
          }
        }

        if (oppDiagRes?.ok) {
          const diagnosticsJson = await oppDiagRes.json() as CalibrationDiagnosticsResponse
          if (diagnosticsJson?.diagnostics && typeof diagnosticsJson.total_samples === 'number') {
            setOpportunityDiagnostics(diagnosticsJson)
          }
        } else if (fetchPlan.opportunities) {
          setOpportunityDiagnostics(null)
        }

        if (inboxRes?.ok) {
          const inboxJson = await inboxRes.json() as AlertsFeedResponse
          if (Array.isArray(inboxJson.alerts)) {
            setAlertsFeed(inboxJson)
          }
        }

        if (fetchPlan.signalsDetail) {
          try {
            const signalsApiUrl = '/signals/api/runs'
            const signalsRunsRes = await fetch(`${signalsApiUrl}?status=ok`)
            if (signalsRunsRes.ok) {
              const runsJson = await signalsRunsRes.json() as { runs: SignalsRunSummary[] }
              if (runsJson.runs && runsJson.runs.length > 0) {
                const latestRunId = runsJson.runs[0].id
                const signalsDetailRes = await fetch(`${signalsApiUrl}/${latestRunId}`)
                if (signalsDetailRes.ok) {
                  const signalsJson = await signalsDetailRes.json() as SignalsData
                  setSignalsData(signalsJson)
                }
              }
            }
          } catch {
            // Signals are optional, don't fail
          }
        }

        if (fetchPlan.similar) {
          try {
            const similarRes = await fetchApi('/api/similar')
            if (similarRes.ok) {
              const similarJson = await similarRes.json() as SimilarPeriodsData
              if (similarJson.similar_periods) {
                setSimilarData(similarJson)
              }
            }
          } catch {
            // Similar periods are optional
          }
        }

        if (fetchPlan.backtest) {
          try {
            const backtestRes = await fetchApi('/api/backtest')
            if (backtestRes.ok) {
              const backtestJson = await backtestRes.json() as BacktestData
              if (backtestJson.bucket_analysis) {
                setBacktestData(backtestJson)
              }
            }
          } catch {
            // Backtest is optional
          }
        }

        setError(null)
      } catch (err: unknown) {
        if (isHomeRoute) {
          const message = err instanceof Error ? err.message : 'Unknown error'
          setError(message)
        }
      } finally {
        if (isHomeRoute) {
          setLoading(false)
        }
      }
    }

    fetchData()
    if (!fetchPlan.poll) {
      return
    }

    const interval = setInterval(fetchData, 5 * 60 * 1000)
    return () => clearInterval(interval)
  }, [route, opportunityHorizon])

  if (route === '/spec') {
    return <SpecPage onClose={() => navigateTo('/')} inPage />
  }

  // Show alerts page
  if (route === '/alerts') {
    return alertsData ? (
      <AlertHistoryPanel
        alerts={alertsData.alerts}
        accuracy={alertsData.accuracy}
        inPage
        onClose={() => navigateTo('/')}
      />
    ) : (
      <div className="min-h-screen bg-black text-[#949ba5] flex flex-col items-center justify-center px-4">
        <p className="text-sm uppercase tracking-widest">No alert history available yet.</p>
        <button
          onClick={() => navigateTo('/')}
          className="mt-4 text-[10px] uppercase tracking-[0.25em] border border-[#26272b] px-4 py-2 rounded"
        >
          Return Home
        </button>
      </div>
    )
  }

  if (route === '/guide') {
    return <OnboardingModal onClose={() => navigateTo('/')} inPage exampleScore={data?.score} />
  }

  if (route === '/brief') {
    return <BriefPage brief={briefData} onBack={() => navigateTo('/')} />
  }

  if (route === '/opportunities') {
    return (
      <OpportunitiesPage
        data={opportunitiesData}
        diagnostics={opportunityDiagnostics}
        horizon={opportunityHorizon}
        onHorizonChange={setOpportunityHorizon}
        onBack={() => navigateTo('/')}
      />
    )
  }

  if (route === '/inbox') {
    return (
      <InboxPage
        alerts={alertsFeed?.alerts || []}
        onBack={() => navigateTo('/')}
        onOpenSubscribe={() => setShowSubscribeModal(true)}
        notice={subscriptionNotice}
      />
    )
  }

  if (loading) {
    return (
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="text-[#949ba5] text-sm tracking-widest uppercase animate-pulse">
          loading
        </div>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="text-[#949ba5] text-sm">
          {error || 'No data available'}
        </div>
      </div>
    )
  }

  // v1.4: Handler to close onboarding and persist preference
  const handleCloseOnboarding = () => {
    setShowOnboarding(false)
    localStorage.setItem('pxi_onboarding_complete', 'true')
  }

  // v1.4: Prepare history data - use sparkline as fallback if no dedicated history endpoint
  const chartData: HistoryDataPoint[] = historyData.length > 0
    ? historyData
    : data.sparkline.map(s => ({
        date: s.date || data.date,
        score: s.score,
        regime: data.regime?.type
      }))

  const delta7d = data.delta.d7
  const deltaDisplay = delta7d !== null
    ? `${delta7d >= 0 ? '+' : ''}${delta7d.toFixed(1)}`
    : null

  return (
    <div className="min-h-screen bg-black text-[#f3f3f3] flex flex-col items-center justify-center px-4 sm:px-8 py-12 sm:py-16">
      {/* v1.4: Onboarding Modal */}
      {showOnboarding && <OnboardingModal onClose={handleCloseOnboarding} exampleScore={data.score} />}

      {/* v1.5: Category Deep-Dive Modal */}
      {selectedCategory && (
        <CategoryModal
          category={selectedCategory}
          onClose={() => setSelectedCategory(null)}
        />
      )}

      {showSubscribeModal && (
        <EmailSubscribeModal
          onClose={() => setShowSubscribeModal(false)}
          onSuccess={(message) => setSubscriptionNotice(message)}
        />
      )}

      {/* Header */}
      <header className="fixed top-0 left-0 right-0 p-4 sm:p-6 flex justify-between items-center z-50">
        <div className="relative flex items-center gap-2 sm:gap-3" ref={menuRef}>
          <button
            onClick={() => navigateTo('/')}
            className="text-[9px] sm:text-[10px] font-mono tracking-[0.3em] text-[#949ba5] uppercase hover:text-[#f3f3f3] transition-colors"
          >
            PXI<span className="text-[#00a3ff]">/</span>COMMAND
          </button>

          <button
            onClick={() => setMenuOpen(!menuOpen)}
            className="text-[10px] sm:text-[11px] font-mono tracking-[0.2em] text-[#949ba5] uppercase hover:text-[#f3f3f3] transition-colors"
            aria-label="Open navigation menu"
          >
            {menuOpen ? '▲' : '▼'}
          </button>
          {menuOpen && (
            <div className="absolute top-full left-0 mt-2 bg-[#0a0a0a] border border-[#26272b] rounded shadow-lg min-w-[130px]">
              <button
                onClick={() => {
                  navigateTo('/spec')
                  setMenuOpen(false)
                }}
                className="w-full text-left px-4 py-2 text-[10px] font-mono uppercase tracking-wider text-[#949ba5] hover:text-[#f3f3f3] hover:bg-[#26272b]/50 transition-colors"
              >
                /spec
              </button>
              <a
                href="/signals"
                className="block w-full text-left px-4 py-2 text-[10px] font-mono uppercase tracking-wider text-[#949ba5] hover:text-[#f3f3f3] hover:bg-[#26272b]/50 transition-colors"
              >
                /signals
              </a>
              <button
                onClick={() => {
                  navigateTo('/alerts')
                  setMenuOpen(false)
                }}
                className="w-full text-left px-4 py-2 text-[10px] font-mono uppercase tracking-wider text-[#949ba5] hover:text-[#f3f3f3] hover:bg-[#26272b]/50 transition-colors"
              >
                /alerts
                {alertsData && alertsData.count > 0 && (
                  <span className="ml-2 text-[8px] text-[#00a3ff]">({alertsData.count})</span>
                )}
              </button>
              <button
                onClick={() => {
                  navigateTo('/brief')
                  setMenuOpen(false)
                }}
                className="w-full text-left px-4 py-2 text-[10px] font-mono uppercase tracking-wider text-[#949ba5] hover:text-[#f3f3f3] hover:bg-[#26272b]/50 transition-colors"
              >
                /brief
              </button>
              <button
                onClick={() => {
                  navigateTo('/opportunities')
                  setMenuOpen(false)
                }}
                className="w-full text-left px-4 py-2 text-[10px] font-mono uppercase tracking-wider text-[#949ba5] hover:text-[#f3f3f3] hover:bg-[#26272b]/50 transition-colors"
              >
                /opportunities
              </button>
              <button
                onClick={() => {
                  navigateTo('/inbox')
                  setMenuOpen(false)
                }}
                className="w-full text-left px-4 py-2 text-[10px] font-mono uppercase tracking-wider text-[#949ba5] hover:text-[#f3f3f3] hover:bg-[#26272b]/50 transition-colors"
              >
                /inbox
                {alertsFeed?.alerts?.length ? (
                  <span className="ml-2 text-[8px] text-[#00a3ff]">({alertsFeed.alerts.length})</span>
                ) : null}
              </button>
              <button
                onClick={() => {
                  navigateTo('/guide')
                  setMenuOpen(false)
                }}
                className="w-full text-left px-4 py-2 text-[10px] font-mono uppercase tracking-wider text-[#949ba5] hover:text-[#f3f3f3] hover:bg-[#26272b]/50 transition-colors border-t border-[#26272b]"
              >
                /guide
              </button>
            </div>
          )}
        </div>
        <div className="text-[10px] sm:text-[11px] font-mono text-[#949ba5]/50">
          {new Date(data.date + 'T12:00:00').toLocaleDateString('en-US', {
            month: 'short',
            day: 'numeric',
          })}
        </div>
      </header>

      {/* Main Content */}
      <main className="flex flex-col items-center max-w-lg w-full pt-8 sm:pt-0">
        {/* Status Badge */}
        <div className="mb-6 sm:mb-8 flex flex-col items-center gap-3">
          <StatusBadge status={data.status} label={data.label} />
        </div>

        <TodayPlanCard plan={planData} />

        {/* Hero Score */}
        <div className="text-center mb-4 sm:mb-6">
          <div className="text-[120px] sm:text-[180px] md:text-[220px] font-extralight leading-none tracking-[-0.04em] tabular-nums">
            {Math.round(data.score)}
          </div>
        </div>

        {/* Delta */}
        {deltaDisplay && (
          <div className="mb-8 sm:mb-10 flex items-center gap-2">
            <span className={`font-mono text-base sm:text-lg tracking-tight ${
              delta7d && delta7d >= 0 ? 'text-[#00a3ff]' : 'text-[#949ba5]'
            }`}>
              {deltaDisplay}
            </span>
            <span className="text-[10px] sm:text-[11px] text-[#949ba5]/50 uppercase tracking-widest">
              7d
            </span>
          </div>
        )}

        {/* Sparkline */}
        <div className="mb-10 sm:mb-16">
          <Sparkline data={data.sparkline} />
        </div>

        {/* Divider */}
        <div className="w-full border-t border-dashed border-[#26272b] mb-6 sm:mb-10" />

        {/* Categories */}
        <div className="w-full space-y-2 sm:space-y-3">
          {data.categories
            .sort((a, b) => b.score - a.score)
            .map((cat) => (
              <CategoryBar
                key={cat.name}
                name={cat.name}
                score={cat.score}
                onClick={() => setSelectedCategory(cat.name)}
              />
            ))}
        </div>

        {/* v1.4: Historical Chart */}
        {chartData.length > 0 && (
          <HistoricalChart
            data={chartData}
            range={historyRange}
            onRangeChange={setHistoryRange}
          />
        )}

        {/* Divergence Alerts */}
        {data.divergence && <DivergenceAlerts divergence={data.divergence} />}

        {/* Predictions */}
        {prediction && <PredictionCard prediction={prediction} />}

        {/* ML Ensemble Predictions */}
        <MLPredictionsCard ensemble={ensemble} accuracy={mlAccuracy} />

        {/* v1.5: Top Themes from Signals */}
        <TopThemesWidget data={signalsData} regime={data.regime?.type} />

        <OpportunityPreview data={opportunitiesData} onOpen={() => navigateTo('/opportunities')} />

        <details className="w-full mt-6 p-4 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg">
          <summary className="cursor-pointer text-[9px] text-[#949ba5]/60 uppercase tracking-wider">
            Context Tools
          </summary>
          <div className="mt-3 space-y-3">
            {data.regime ? (
              <div className="flex justify-start">
                <RegimeBadge regime={data.regime} />
              </div>
            ) : null}
            {signal && <SignalIndicator signal={signal.signal} />}
            <BriefCompactCard brief={briefData} onOpen={() => navigateTo('/brief')} className="w-full p-4 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg" />
          </div>
        </details>

        {subscriptionNotice && (
          <div className="w-full mt-4 p-3 border border-[#1f3e56] bg-[#091825] rounded text-[11px] text-[#9ec5e2]">
            {subscriptionNotice}
          </div>
        )}

        {/* v1.6: Similar Periods */}
        <SimilarPeriodsCard data={similarData} />

        {/* v1.6: Backtest Performance */}
        <BacktestCard data={backtestData} />

        {/* v1.4: Stale Data Warning */}
        <StaleDataWarning freshness={data.dataFreshness} />

        {/* v1.6: Export Button */}
        <div className="w-full mt-6 flex justify-center">
          <ExportButton />
        </div>
      </main>

      {/* Footer */}
      <footer className="fixed bottom-0 left-0 right-0 p-4 sm:p-6">
        <div className="text-[9px] sm:text-[10px] text-[#949ba5]/30 font-mono tracking-wider text-center uppercase">
          Macro Market Strength Index
        </div>
      </footer>
    </div>
  )
}

export default App
