import { useEffect, useState } from 'react'

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

function App() {
  const [data, setData] = useState<PXIData | null>(null)
  const [prediction, setPrediction] = useState<PredictionData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchData = async () => {
      try {
        const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:3000'

        // Fetch PXI data and predictions in parallel
        const [pxiRes, predRes] = await Promise.all([
          fetch(`${apiUrl}/api/pxi`),
          fetch(`${apiUrl}/api/predict`).catch(() => null)
        ])

        if (!pxiRes.ok) throw new Error('Failed to fetch')
        const pxiJson = await pxiRes.json()
        setData(pxiJson)

        // Predictions are optional - don't fail if unavailable
        if (predRes?.ok) {
          const predJson = await predRes.json()
          if (!predJson.error) {
            setPrediction(predJson)
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

  const delta7d = data.delta.d7
  const deltaDisplay = delta7d !== null
    ? `${delta7d >= 0 ? '+' : ''}${delta7d.toFixed(1)}`
    : null

  return (
    <div className="min-h-screen bg-black text-[#f3f3f3] flex flex-col items-center justify-center px-4 sm:px-8 py-12 sm:py-16">
      {/* Header */}
      <header className="fixed top-0 left-0 right-0 p-4 sm:p-6 flex justify-between items-center">
        <div className="text-[10px] sm:text-[11px] font-mono tracking-[0.2em] text-[#949ba5] uppercase">
          PXI<span className="text-[#00a3ff]">/</span>command
        </div>
        <div className="text-[10px] sm:text-[11px] font-mono text-[#949ba5]/50">
          {new Date(data.date).toLocaleDateString('en-US', {
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

        {/* Predictions */}
        {prediction && <PredictionCard prediction={prediction} />}
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
