import { useEffect, useState, useRef } from 'react'

type AppRoute = '/' | '/spec' | '/alerts' | '/guide'

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
}

function normalizeRoute(pathname: string): AppRoute {
  const path = (pathname || '/').replace(/\/+$/, '')
  if (path === '/spec') return '/spec'
  if (path === '/alerts') return '/alerts'
  if (path === '/guide') return '/guide'
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

// ============== ML Accuracy Data Interface ==============
// Matches the /api/ml/accuracy response format
interface MLAccuracyApiResponse {
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
  rolling_7d: {
    direction_accuracy: number
    sample_size: number
    mae: number | null
  }
  rolling_30d: {
    direction_accuracy: number
    sample_size: number
    mae: number | null
  }
  all_time: {
    direction_accuracy_7d: number
    direction_accuracy_30d: number
    total_predictions: number
  }
}

// Parse API response to display format
function parseMLAccuracy(api: MLAccuracyApiResponse): MLAccuracyData | null {
  if (!api.metrics?.ensemble) return null

  const d7 = api.metrics.ensemble.d7
  const d30 = api.metrics.ensemble.d30

  return {
    rolling_7d: {
      direction_accuracy: d7 ? parseFloat(d7.direction_accuracy.replace('%', '')) : 50,
      sample_size: d7?.sample_size || 0,
      mae: d7 ? parseFloat(d7.mean_absolute_error) : null
    },
    rolling_30d: {
      direction_accuracy: d30 ? parseFloat(d30.direction_accuracy.replace('%', '')) : 50,
      sample_size: d30?.sample_size || 0,
      mae: d30 ? parseFloat(d30.mean_absolute_error) : null
    },
    all_time: {
      direction_accuracy_7d: d7 ? parseFloat(d7.direction_accuracy.replace('%', '')) : 50,
      direction_accuracy_30d: d30 ? parseFloat(d30.direction_accuracy.replace('%', '')) : 50,
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
      lastUpdate: string
      daysOld: number
    }[]
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
  }
  regime: {
    type: 'RISK_ON' | 'RISK_OFF' | 'TRANSITION'
    confidence: number
    description: string
  } | null
  divergence: PXIData['divergence']
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
function OnboardingModal({ onClose, inPage = false }: { onClose: () => void; inPage?: boolean }) {
  const [step, setStep] = useState(0)

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
              <div className="text-6xl font-extralight text-[#f3f3f3] mb-2">53</div>
              <div className="text-[10px] text-[#949ba5]/60 uppercase tracking-widest">Example Score</div>
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

  if (!freshness?.hasStaleData) return null

  return (
    <div className="w-full mt-4">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-center gap-2 py-2 px-4 bg-[#f59e0b]/10 border border-[#f59e0b]/30 rounded text-[10px] text-[#f59e0b] uppercase tracking-wider hover:bg-[#f59e0b]/20 transition-colors"
      >
        <span>⚠</span>
        <span>{freshness.staleCount} indicator{freshness.staleCount > 1 ? 's' : ''} may be stale</span>
        <span className="text-[#f59e0b]/50">{expanded ? '▲' : '▼'}</span>
      </button>
      {expanded && freshness.staleIndicators.length > 0 && (
        <div className="mt-2 p-3 bg-[#0a0a0a]/60 border border-[#26272b] rounded">
          <div className="text-[9px] text-[#949ba5]/50 uppercase tracking-wider mb-2">
            Last updated more than 2 days ago
          </div>
          <div className="space-y-1">
            {freshness.staleIndicators.map(ind => (
              <div key={ind.id} className="flex justify-between text-[10px]">
                <span className="text-[#949ba5]">{ind.id.replace(/_/g, ' ')}</span>
                <span className="text-[#f59e0b]/70">{ind.daysOld}d ago</span>
              </div>
            ))}
          </div>
          {freshness.staleCount > 5 && (
            <div className="text-[8px] text-[#949ba5]/40 mt-2">
              +{freshness.staleCount - 5} more
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

  const getColor = (acc: number) => {
    if (acc >= 70) return 'text-[#00c896]'
    if (acc >= 55) return 'text-[#f59e0b]'
    return 'text-[#ff6b6b]'
  }

  return (
    <div className="flex justify-center gap-4 mt-3 mb-2">
      <Tooltip content="Rolling 30-day directional accuracy of ML predictions">
        <div className="flex items-center gap-2 bg-[#0a0a0a]/60 rounded px-2.5 py-1 border border-[#1a1a1a]">
          <span className="text-[8px] text-[#949ba5]/50 uppercase tracking-wider">7d Acc</span>
          <span className={`text-[10px] font-mono ${getColor(acc7d)}`}>
            {Math.round(acc7d)}%
          </span>
        </div>
      </Tooltip>
      <Tooltip content="Rolling 90-day directional accuracy of ML predictions">
        <div className="flex items-center gap-2 bg-[#0a0a0a]/60 rounded px-2.5 py-1 border border-[#1a1a1a]">
          <span className="text-[8px] text-[#949ba5]/50 uppercase tracking-wider">30d Acc</span>
          <span className={`text-[10px] font-mono ${getColor(acc30d)}`}>
            {Math.round(acc30d)}%
          </span>
        </div>
      </Tooltip>
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
        const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:3000'
        const res = await fetch(`${apiUrl}/api/category/${category}`)
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

  // Calculate probability-weighted outlook (guard against division by zero)
  const totalWeight = data.similar_periods.reduce((sum, p) => sum + p.weights.combined, 0)
  const safeWeight = totalWeight || 1  // Prevent division by zero

  const weightedReturn7d = data.similar_periods.reduce((sum, p) => {
    const ret = p.forward_returns?.d7 ?? 0
    return sum + ret * p.weights.combined
  }, 0) / safeWeight

  const weightedReturn30d = data.similar_periods.reduce((sum, p) => {
    const ret = p.forward_returns?.d30 ?? 0
    return sum + ret * p.weights.combined
  }, 0) / safeWeight

  const positiveCount = data.similar_periods.filter(p => (p.forward_returns?.d30 ?? 0) > 0).length
  const winRate = data.similar_periods.length > 0
    ? (positiveCount / data.similar_periods.length) * 100
    : 0

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
            <div className={`text-lg font-mono ${weightedReturn7d >= 0 ? 'text-[#00c896]' : 'text-[#ff6b6b]'}`}>
              {weightedReturn7d >= 0 ? '+' : ''}{weightedReturn7d.toFixed(2)}%
            </div>
          </div>
          <div className="text-center">
            <div className="text-[10px] text-[#949ba5]/60">30d</div>
            <div className={`text-lg font-mono ${weightedReturn30d >= 0 ? 'text-[#00c896]' : 'text-[#ff6b6b]'}`}>
              {weightedReturn30d >= 0 ? '+' : ''}{weightedReturn30d.toFixed(2)}%
            </div>
          </div>
          <div className="text-center">
            <div className="text-[10px] text-[#949ba5]/60">Win Rate</div>
            <div className={`text-lg font-mono ${winRate >= 50 ? 'text-[#00c896]' : 'text-[#ff6b6b]'}`}>
              {Math.round(winRate)}%
            </div>
          </div>
        </div>
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

// ============== CSV Export Button ==============
function ExportButton() {
  const [exporting, setExporting] = useState(false)

  const handleExport = async () => {
    setExporting(true)
    try {
      const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:3000'
      const response = await fetch(`${apiUrl}/api/export/history?format=csv&days=365`)
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

  // v1.4: Check for first visit on mount
  useEffect(() => {
    const hasSeenOnboarding = localStorage.getItem('pxi_onboarding_complete')
    if (!hasSeenOnboarding) {
      setShowOnboarding(true)
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
    const fetchData = async () => {
      try {
        const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:3000'

        // Fetch PXI data, signal data, predictions, ML ensemble, accuracy, history, and alerts in parallel
        const [pxiRes, signalRes, predRes, ensembleRes, accuracyRes, historyRes, alertsRes] = await Promise.all([
          fetch(`${apiUrl}/api/pxi`),
          fetch(`${apiUrl}/api/signal`).catch(() => null),  // v1.1
          fetch(`${apiUrl}/api/predict`).catch(() => null),
          fetch(`${apiUrl}/api/ml/ensemble`).catch(() => null),  // Ensemble
          fetch(`${apiUrl}/api/ml/accuracy`).catch(() => null),  // v1.4: ML accuracy
          fetch(`${apiUrl}/api/history?days=90`).catch(() => null),  // v1.4: Historical data
          fetch(`${apiUrl}/api/alerts?limit=50`).catch(() => null)  // v1.5: Alert history
        ])

        if (!pxiRes.ok) throw new Error('Failed to fetch')
        const pxiJson = await pxiRes.json()
        setData(pxiJson)

        // v1.1: Signal data
        if (signalRes?.ok) {
          const signalJson = await signalRes.json()
          if (!signalJson.error) {
            setSignal(signalJson)
          }
        }

        // Predictions are optional - don't fail if unavailable
        if (predRes?.ok) {
          const predJson = await predRes.json()
          if (!predJson.error) {
            setPrediction(predJson)
          }
        }

        // ML Ensemble predictions
        if (ensembleRes?.ok) {
          const ensembleJson = await ensembleRes.json()
          if (!ensembleJson.error) {
            setEnsemble(ensembleJson)
          }
        }

        // v1.4: ML accuracy data - parse API response to display format
        if (accuracyRes?.ok) {
          const accuracyJson = await accuracyRes.json() as MLAccuracyApiResponse
          if (!accuracyJson.error && accuracyJson.metrics) {
            const parsed = parseMLAccuracy(accuracyJson)
            if (parsed) {
              setMlAccuracy(parsed)
            }
          }
        }

        // v1.4: Historical data for chart - use sparkline as fallback
        if (historyRes?.ok) {
          const historyJson = await historyRes.json() as { data?: HistoryDataPoint[]; error?: string }
          if (historyJson.data && Array.isArray(historyJson.data)) {
            setHistoryData(historyJson.data)
          }
        }

        // v1.5: Alert history data
        if (alertsRes?.ok) {
          const alertsJson = await alertsRes.json() as AlertsApiResponse
          if (alertsJson.alerts) {
            setAlertsData(alertsJson)
          }
        }

        // v1.5: Signals data - fetch latest run from signals API
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

        // v1.6: Similar periods data
        try {
          const similarRes = await fetch(`${apiUrl}/api/similar`)
          if (similarRes.ok) {
            const similarJson = await similarRes.json() as SimilarPeriodsData
            if (similarJson.similar_periods) {
              setSimilarData(similarJson)
            }
          }
        } catch {
          // Similar periods are optional
        }

        // v1.6: Backtest data
        try {
          const backtestRes = await fetch(`${apiUrl}/api/backtest`)
          if (backtestRes.ok) {
            const backtestJson = await backtestRes.json() as BacktestData
            if (backtestJson.bucket_analysis) {
              setBacktestData(backtestJson)
            }
          }
        } catch {
          // Backtest is optional
        }

        setError(null)
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : 'Unknown error'
        setError(message)
      } finally {
        setLoading(false)
      }
    }

    fetchData()
    const interval = setInterval(fetchData, 5 * 60 * 1000)
    return () => clearInterval(interval)
  }, [])

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
    return <OnboardingModal onClose={() => navigateTo('/')} inPage />
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
      {showOnboarding && <OnboardingModal onClose={handleCloseOnboarding} />}

      {/* v1.5: Category Deep-Dive Modal */}
      {selectedCategory && (
        <CategoryModal
          category={selectedCategory}
          onClose={() => setSelectedCategory(null)}
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
        {/* Status Badge & Regime */}
        <div className="mb-6 sm:mb-8 flex flex-col items-center gap-3">
          <StatusBadge status={data.status} label={data.label} />
          {data.regime && <RegimeBadge regime={data.regime} />}
        </div>

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

        {/* v1.1: Signal Indicator */}
        {signal && <SignalIndicator signal={signal.signal} />}

        {/* Divergence Alerts */}
        {data.divergence && <DivergenceAlerts divergence={data.divergence} />}

        {/* Predictions */}
        {prediction && <PredictionCard prediction={prediction} />}

        {/* ML Ensemble Predictions */}
        <MLPredictionsCard ensemble={ensemble} accuracy={mlAccuracy} />

        {/* v1.5: Top Themes from Signals */}
        <TopThemesWidget data={signalsData} regime={data.regime?.type} />

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
