import { useEffect, useState, useRef } from 'react'

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

// ML Model predictions (XGBoost and LSTM)
interface MLPrediction {
  value: number | null
  direction: 'STRONG_UP' | 'UP' | 'FLAT' | 'DOWN' | 'STRONG_DOWN' | null
}

interface MLPredictData {
  date: string
  current_score: number
  model_type?: string  // 'lstm' for LSTM endpoint
  model_version: string
  predictions: {
    pxi_change_7d: MLPrediction
    pxi_change_30d: MLPrediction
  }
  features_used: number
}

function Sparkline({ data }: { data: { score: number }[] }) {
  if (data.length === 0) return null

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

function CategoryBar({ name, score }: { name: string; score: number }) {
  const isHigh = score >= 70
  const displayName = name.replace(/_/g, ' ')

  return (
    <div className="flex items-center gap-2 sm:gap-4">
      <span className="w-20 sm:w-28 text-right text-[#949ba5] text-[11px] sm:text-[13px] tracking-wide capitalize">
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
    </div>
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
            {extreme.type} — {extreme.signal}
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
        <div className={`text-[11px] font-medium uppercase tracking-wider ${biasColor}`}>
          {bias}
        </div>
        <div className="text-[9px] text-[#949ba5]/40 mt-1">
          {note}
        </div>
        <div className="text-[8px] text-[#949ba5]/30 mt-2">
          Based on {d7.sample_size} observations • {confidence.toLowerCase()} confidence
        </div>
      </div>
    </div>
  )
}

function MLPredictionsCard({ xgboost, lstm }: { xgboost: MLPredictData | null; lstm: MLPredictData | null }) {
  if (!xgboost && !lstm) return null

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

  return (
    <div className="w-full mt-6 sm:mt-8">
      <div className="text-[10px] sm:text-[11px] text-[#949ba5]/50 uppercase tracking-widest mb-4 text-center">
        ML Predictions
      </div>

      <div className="grid grid-cols-2 gap-4">
        {/* XGBoost Model */}
        <div className="bg-[#0a0a0a]/40 rounded-lg p-4 border border-[#1a1a1a]">
          <div className="text-[9px] text-[#949ba5]/40 uppercase tracking-wider mb-3 text-center">
            XGBoost
          </div>
          {xgboost ? (
            <div className="space-y-3">
              <div className="flex justify-between items-center">
                <span className="text-[10px] text-[#949ba5]/60">7d</span>
                <div className="flex items-center gap-1">
                  <span className={`text-[13px] font-mono ${getDirectionColor(xgboost.predictions.pxi_change_7d.direction)}`}>
                    {formatChange(xgboost.predictions.pxi_change_7d.value)}
                  </span>
                  <span className={`text-[11px] ${getDirectionColor(xgboost.predictions.pxi_change_7d.direction)}`}>
                    {getDirectionIcon(xgboost.predictions.pxi_change_7d.direction)}
                  </span>
                </div>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-[10px] text-[#949ba5]/60">30d</span>
                <div className="flex items-center gap-1">
                  <span className={`text-[13px] font-mono ${getDirectionColor(xgboost.predictions.pxi_change_30d.direction)}`}>
                    {formatChange(xgboost.predictions.pxi_change_30d.value)}
                  </span>
                  <span className={`text-[11px] ${getDirectionColor(xgboost.predictions.pxi_change_30d.direction)}`}>
                    {getDirectionIcon(xgboost.predictions.pxi_change_30d.direction)}
                  </span>
                </div>
              </div>
              <div className="text-[8px] text-[#949ba5]/30 text-center pt-1">
                {xgboost.features_used} features
              </div>
            </div>
          ) : (
            <div className="text-[10px] text-[#949ba5]/30 text-center py-2">unavailable</div>
          )}
        </div>

        {/* LSTM Model */}
        <div className="bg-[#0a0a0a]/40 rounded-lg p-4 border border-[#1a1a1a]">
          <div className="text-[9px] text-[#949ba5]/40 uppercase tracking-wider mb-3 text-center">
            LSTM
          </div>
          {lstm ? (
            <div className="space-y-3">
              <div className="flex justify-between items-center">
                <span className="text-[10px] text-[#949ba5]/60">7d</span>
                <div className="flex items-center gap-1">
                  <span className={`text-[13px] font-mono ${getDirectionColor(lstm.predictions.pxi_change_7d.direction)}`}>
                    {formatChange(lstm.predictions.pxi_change_7d.value)}
                  </span>
                  <span className={`text-[11px] ${getDirectionColor(lstm.predictions.pxi_change_7d.direction)}`}>
                    {getDirectionIcon(lstm.predictions.pxi_change_7d.direction)}
                  </span>
                </div>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-[10px] text-[#949ba5]/60">30d</span>
                <div className="flex items-center gap-1">
                  <span className={`text-[13px] font-mono ${getDirectionColor(lstm.predictions.pxi_change_30d.direction)}`}>
                    {formatChange(lstm.predictions.pxi_change_30d.value)}
                  </span>
                  <span className={`text-[11px] ${getDirectionColor(lstm.predictions.pxi_change_30d.direction)}`}>
                    {getDirectionIcon(lstm.predictions.pxi_change_30d.direction)}
                  </span>
                </div>
              </div>
              <div className="text-[8px] text-[#949ba5]/30 text-center pt-1">
                20-day sequence
              </div>
            </div>
          ) : (
            <div className="text-[10px] text-[#949ba5]/30 text-center py-2">unavailable</div>
          )}
        </div>
      </div>

      <div className="text-[8px] text-[#949ba5]/20 text-center mt-3">
        Predicted PXI change • Updated with data refresh
      </div>
    </div>
  )
}

function SpecPage({ onClose }: { onClose: () => void }) {
  return (
    <div className="min-h-screen bg-black text-[#f3f3f3] px-4 sm:px-8 py-16 overflow-auto">
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
          Macro Market Strength Index — Quantitative Framework v1.1
        </p>

        {/* Two-Layer Architecture */}
        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Two-Layer Architecture</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            v1.1 introduces a two-layer system separating descriptive state from actionable signals.
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
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Category Composition (v1.1)</h2>
          <div className="space-y-4">
            {[
              { name: 'Volatility', weight: 20, indicators: 'VIX, VIX term structure, AAII sentiment', formula: '100 - percentile(VIX, 5yr)' },
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
          PXI/COMMAND Protocol v1.1
        </div>
      </div>
    </div>
  )
}

function App() {
  const [data, setData] = useState<PXIData | null>(null)
  const [prediction, setPrediction] = useState<PredictionData | null>(null)
  const [signal, setSignal] = useState<SignalData | null>(null)  // v1.1
  const [mlXgboost, setMlXgboost] = useState<MLPredictData | null>(null)  // ML predictions
  const [mlLstm, setMlLstm] = useState<MLPredictData | null>(null)  // LSTM predictions
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [showSpec, setShowSpec] = useState(false)
  const [menuOpen, setMenuOpen] = useState(false)
  const menuRef = useRef<HTMLDivElement>(null)

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

  useEffect(() => {
    const fetchData = async () => {
      try {
        const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:3000'

        // Fetch PXI data, signal data, predictions, and ML models in parallel
        const [pxiRes, signalRes, predRes, mlXgboostRes, mlLstmRes] = await Promise.all([
          fetch(`${apiUrl}/api/pxi`),
          fetch(`${apiUrl}/api/signal`).catch(() => null),  // v1.1
          fetch(`${apiUrl}/api/predict`).catch(() => null),
          fetch(`${apiUrl}/api/ml/predict`).catch(() => null),  // XGBoost
          fetch(`${apiUrl}/api/ml/lstm`).catch(() => null)  // LSTM
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

        // ML XGBoost predictions
        if (mlXgboostRes?.ok) {
          const mlJson = await mlXgboostRes.json()
          if (!mlJson.error) {
            setMlXgboost(mlJson)
          }
        }

        // ML LSTM predictions
        if (mlLstmRes?.ok) {
          const lstmJson = await mlLstmRes.json()
          if (!lstmJson.error) {
            setMlLstm(lstmJson)
          }
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

  // Show spec page
  if (showSpec) {
    return <SpecPage onClose={() => setShowSpec(false)} />
  }

  const delta7d = data.delta.d7
  const deltaDisplay = delta7d !== null
    ? `${delta7d >= 0 ? '+' : ''}${delta7d.toFixed(1)}`
    : null

  return (
    <div className="min-h-screen bg-black text-[#f3f3f3] flex flex-col items-center justify-center px-4 sm:px-8 py-12 sm:py-16">
      {/* Header */}
      <header className="fixed top-0 left-0 right-0 p-4 sm:p-6 flex justify-between items-center z-50">
        <div className="relative" ref={menuRef}>
          <button
            onClick={() => setMenuOpen(!menuOpen)}
            className="text-[10px] sm:text-[11px] font-mono tracking-[0.2em] text-[#949ba5] uppercase hover:text-[#f3f3f3] transition-colors"
          >
            PXI<span className="text-[#00a3ff]">/</span>command
            <span className="ml-2 text-[#949ba5]/50">{menuOpen ? '▲' : '▼'}</span>
          </button>
          {menuOpen && (
            <div className="absolute top-full left-0 mt-2 bg-[#0a0a0a] border border-[#26272b] rounded shadow-lg min-w-[120px]">
              <button
                onClick={() => {
                  setShowSpec(true)
                  setMenuOpen(false)
                }}
                className="w-full text-left px-4 py-2 text-[10px] font-mono uppercase tracking-wider text-[#949ba5] hover:text-[#f3f3f3] hover:bg-[#26272b]/50 transition-colors"
              >
                /spec
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
              <CategoryBar key={cat.name} name={cat.name} score={cat.score} />
            ))}
        </div>

        {/* v1.1: Signal Indicator */}
        {signal && <SignalIndicator signal={signal.signal} />}

        {/* Divergence Alerts */}
        {data.divergence && <DivergenceAlerts divergence={data.divergence} />}

        {/* Predictions */}
        {prediction && <PredictionCard prediction={prediction} />}

        {/* ML Model Predictions */}
        <MLPredictionsCard xgboost={mlXgboost} lstm={mlLstm} />
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
