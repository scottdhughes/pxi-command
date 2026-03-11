import type { RefObject } from 'react'

import { EmailSubscribeModal } from '../components/EmailSubscribeModal'
import { SiteDisclaimer } from '../components/SiteDisclaimer'
import {
  ExportButton,
  MLPredictionsCard,
  OpportunityPreview,
  BriefCompactCard,
  BacktestCard,
  SimilarPeriodsCard,
  TopThemesWidget,
} from '../components/home/InsightCards'
import {
  CategoryModal,
  HistoricalChart,
  StaleDataWarning,
} from '../components/home/MarketCharts'
import { OnboardingModal } from '../components/home/OnboardingModal'
import { TodayPlanCard } from '../components/home/PlanCard'
import type { AppRoute } from '../lib/routes'
import type {
  AlertsApiResponse,
  AlertsFeedResponse,
  BacktestData,
  BriefData,
  EnsembleData,
  HistoryDataPoint,
  MLAccuracyData,
  OpportunitiesResponse,
  PlanData,
  PredictionData,
  PXIData,
  SignalData,
  SignalsData,
  SimilarPeriodsData,
} from '../lib/types'

function Sparkline({ data }: { data: { score: number }[] }) {
  if (data.length < 2) return null

  const min = Math.min(...data.map((d) => d.score))
  const max = Math.max(...data.map((d) => d.score))
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

function SignalIndicator({ signal }: { signal: SignalData['signal'] | null }) {
  if (!signal) return null

  const signalColors: Record<string, string> = {
    FULL_RISK: '#00c896',
    REDUCED_RISK: '#f59e0b',
    RISK_OFF: '#ff6b6b',
    DEFENSIVE: '#dc2626',
  }

  const signalLabels: Record<string, string> = {
    FULL_RISK: 'Full Risk',
    REDUCED_RISK: 'Reduced',
    RISK_OFF: 'Risk Off',
    DEFENSIVE: 'Defensive',
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
  onClick,
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
  const outlookLabel = bias === 'BULLISH' ? 'Favorable' : bias === 'BEARISH' ? 'Unfavorable' : 'Mixed'
  const extremeLabel = (signal: string) => signal === 'BULLISH' ? 'Favorable Setup' : 'Caution'

  return (
    <div className="w-full mt-6 sm:mt-10">
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

type HomePageProps = {
  alertsData: AlertsApiResponse | null
  alertsFeed: AlertsFeedResponse | null
  backtestData: BacktestData | null
  briefData: BriefData | null
  data: PXIData
  ensemble: EnsembleData | null
  historyData: HistoryDataPoint[]
  historyRange: '7d' | '30d' | '90d'
  menuOpen: boolean
  menuRef: RefObject<HTMLDivElement | null>
  mlAccuracy: MLAccuracyData | null
  opportunitiesData: OpportunitiesResponse | null
  planData: PlanData | null
  prediction: PredictionData | null
  selectedCategory: string | null
  setHistoryRange: (range: '7d' | '30d' | '90d') => void
  setMenuOpen: (open: boolean) => void
  setSelectedCategory: (category: string | null) => void
  setShowOnboarding: (show: boolean) => void
  setShowSubscribeModal: (show: boolean) => void
  setSubscriptionNotice: (message: string | null) => void
  showOnboarding: boolean
  showSubscribeModal: boolean
  signal: SignalData | null
  signalsData: SignalsData | null
  similarData: SimilarPeriodsData | null
  subscriptionNotice: string | null
  navigateTo: (route: AppRoute) => void
}

export function HomePage({
  alertsData,
  alertsFeed,
  backtestData,
  briefData,
  data,
  ensemble,
  historyData,
  historyRange,
  menuOpen,
  menuRef,
  mlAccuracy,
  opportunitiesData,
  planData,
  prediction,
  selectedCategory,
  setHistoryRange,
  setMenuOpen,
  setSelectedCategory,
  setShowOnboarding,
  setShowSubscribeModal,
  setSubscriptionNotice,
  showOnboarding,
  showSubscribeModal,
  signal,
  signalsData,
  similarData,
  subscriptionNotice,
  navigateTo,
}: HomePageProps) {
  const handleCloseOnboarding = () => {
    setShowOnboarding(false)
    localStorage.setItem('pxi_onboarding_complete', 'true')
  }

  const chartData: HistoryDataPoint[] = historyData.length > 0
    ? historyData
    : data.sparkline.map((s) => ({
        date: s.date || data.date,
        score: s.score,
        regime: data.regime?.type,
      }))

  const delta7d = data.delta.d7
  const deltaDisplay = delta7d !== null
    ? `${delta7d >= 0 ? '+' : ''}${delta7d.toFixed(1)}`
    : null

  return (
    <div className="min-h-screen bg-black text-[#f3f3f3] flex flex-col items-center justify-center px-4 sm:px-8 py-12 sm:py-16">
      {showOnboarding && <OnboardingModal onClose={handleCloseOnboarding} exampleScore={data.score} />}

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

      <main className="flex flex-col items-center max-w-lg w-full pt-8 sm:pt-0">
        <div className="mb-6 sm:mb-8 flex flex-col items-center gap-3">
          <StatusBadge status={data.status} label={data.label} />
        </div>

        <div className="text-center mb-4 sm:mb-6">
          <div className="text-[120px] sm:text-[180px] md:text-[220px] font-extralight leading-none tracking-[-0.04em] tabular-nums">
            {Math.round(data.score)}
          </div>
        </div>

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

        <div className="mb-10 sm:mb-16">
          <Sparkline data={data.sparkline} />
        </div>

        <div className="w-full border-t border-dashed border-[#26272b] mb-6 sm:mb-10" />

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

        <TodayPlanCard plan={planData} />

        {chartData.length > 0 && (
          <HistoricalChart
            data={chartData}
            range={historyRange}
            onRangeChange={setHistoryRange}
          />
        )}

        {data.divergence && <DivergenceAlerts divergence={data.divergence} />}

        {prediction && <PredictionCard prediction={prediction} />}

        <MLPredictionsCard ensemble={ensemble} accuracy={mlAccuracy} />

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

        <SimilarPeriodsCard data={similarData} />

        <BacktestCard data={backtestData} />

        <StaleDataWarning freshness={data.dataFreshness} />

        <div className="w-full mt-6 flex justify-center">
          <ExportButton />
        </div>
      </main>

      <footer className="fixed bottom-0 left-0 right-0 p-4 sm:p-6">
        <div className="space-y-1">
          <div className="text-[9px] sm:text-[10px] text-[#949ba5]/30 font-mono tracking-wider text-center uppercase">
            Macro Market Strength Index
          </div>
          <SiteDisclaimer />
        </div>
      </footer>
    </div>
  )
}

export default HomePage
