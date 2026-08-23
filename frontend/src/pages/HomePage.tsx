import type { RefObject } from 'react'

import { EmailSubscribeModal } from '../components/EmailSubscribeModal'
import { SiteDisclaimer } from '../components/SiteDisclaimer'
import {
  ExportButton,
  OpportunityPreview,
  BriefCompactCard,
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
  HistoryMetadata,
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

function SignalIndicator({ data }: { data: SignalData | null }) {
  if (!data) return null

  const { signal, decision_contract: decisionContract } = data

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

  const researchPosture = decisionContract?.descriptive_context.research_posture || signal.type
  const color = signalColors[researchPosture] || '#949ba5'
  const evidenceStatus = decisionContract?.evidence.status || 'BLOCKED'

  return (
    <div className="flex items-center gap-2 sm:gap-4 mb-6">
      <div className="w-20 sm:w-28 shrink-0 text-right">
        <span className="text-[9px] text-[#949ba5]/50 uppercase tracking-widest">
          Research posture
        </span>
      </div>
      <div className="min-w-0 flex-1 flex flex-wrap items-start gap-2 sm:gap-3">
        <div
          className="bg-[#0a0a0a]/80 backdrop-blur-sm rounded px-3 py-2"
          style={{ borderLeft: `2px solid ${color}` }}
        >
          <div className="flex flex-wrap items-center gap-2 sm:gap-3">
            <span
              className="text-[11px] font-medium uppercase tracking-wide"
              style={{ color }}
            >
              {signalLabels[researchPosture]}
            </span>
            <span className="text-[10px] font-mono text-[#f59e0b]">
              allocation withheld
            </span>
            <span className={`text-[8px] uppercase tracking-widest ${evidenceStatus === 'PASSED' ? 'text-[#00c896]' : 'text-[#f59e0b]'}`}>
              evidence {evidenceStatus.toLowerCase()}
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
          <p className="text-[9px] text-[#949ba5]/60 mt-1">
            {decisionContract?.headline || 'No actionable signal'} · research context only; the Plan is the sole allocation authority.
          </p>
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
                <span className="text-[8px] text-[#949ba5]/50 uppercase tracking-widest shrink-0">
                  Research flag
                </span>
              </div>
              <p className="text-[10px] text-[#949ba5]/60 leading-relaxed mt-1">
                {alert.description}
              </p>
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

type HomePageProps = {
  alertsData: AlertsApiResponse | null
  alertsFeed: AlertsFeedResponse | null
  backtestData: BacktestData | null
  briefData: BriefData | null
  data: PXIData
  ensemble: EnsembleData | null
  historyData: HistoryDataPoint[]
  historyMetadata: HistoryMetadata | null
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
  briefData,
  data,
  historyData,
  historyMetadata,
  historyRange,
  menuOpen,
  menuRef,
  opportunitiesData,
  planData,
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

  const navigationItems: Array<{
    href: '/' | '/brief' | '/opportunities' | '/signals' | '/alerts' | '/inbox' | '/guide' | '/spec'
    label: string
    badge?: number
  }> = [
    { href: '/', label: 'Home' },
    { href: '/brief', label: 'Daily Brief' },
    { href: '/opportunities', label: 'Opportunities' },
    { href: '/signals', label: 'Signals' },
    { href: '/alerts', label: 'Alert History', badge: alertsData?.count },
    { href: '/inbox', label: 'Alert Inbox', badge: alertsFeed?.alerts?.length },
    { href: '/guide', label: 'Guide' },
    { href: '/spec', label: 'Methodology' },
  ]

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
            className="rounded border border-[#26272b] px-2 py-1 text-[9px] sm:text-[10px] font-mono tracking-[0.16em] text-[#949ba5] uppercase hover:border-[#4a4d54] hover:text-[#f3f3f3] transition-colors"
            aria-label="Open navigation menu"
            aria-expanded={menuOpen}
          >
            {menuOpen ? 'close' : 'menu'}
          </button>
          {menuOpen && (
            <nav
              aria-label="Primary navigation"
              className="absolute top-full left-0 z-[70] mt-2 w-56 max-h-[calc(100vh-5rem)] overflow-y-auto rounded-md border border-[#26272b] bg-[#0a0a0a]/98 p-1 shadow-2xl backdrop-blur"
            >
              {navigationItems.map((item, index) => {
                const className = `flex w-full items-center justify-between rounded px-3 py-2.5 text-left text-[10px] font-mono uppercase tracking-[0.12em] text-[#949ba5] transition-colors hover:bg-[#26272b]/60 hover:text-[#f3f3f3] ${index === 6 ? 'mt-1 border-t border-[#26272b] pt-3' : ''}`
                const content = (
                  <>
                    <span><span className="mr-2 text-[#00a3ff]/70">/</span>{item.label}</span>
                    {item.badge ? <span className="text-[8px] text-[#00a3ff]">{item.badge}</span> : null}
                  </>
                )

                return item.href === '/signals' ? (
                  <a key={item.href} href={item.href} className={className}>{content}</a>
                ) : (
                  <button
                    key={item.href}
                    onClick={() => {
                      navigateTo(item.href as AppRoute)
                      setMenuOpen(false)
                    }}
                    className={className}
                  >
                    {content}
                  </button>
                )
              })}
            </nav>
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
            metadata={historyData.length > 0 ? historyMetadata : null}
            range={historyRange}
            onRangeChange={setHistoryRange}
          />
        )}

        {data.divergence && <DivergenceAlerts divergence={data.divergence} />}

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
            {signal && <SignalIndicator data={signal} />}
            <BriefCompactCard brief={briefData} onOpen={() => navigateTo('/brief')} className="w-full p-4 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg" />
          </div>
        </details>

        {subscriptionNotice && (
          <div className="w-full mt-4 p-3 border border-[#1f3e56] bg-[#091825] rounded text-[11px] text-[#9ec5e2]">
            {subscriptionNotice}
          </div>
        )}

        <div className="w-full mt-6 p-3 border border-[#26272b] rounded text-[10px] leading-relaxed text-[#949ba5]/70">
          Legacy forward-return, similarity, ML, and backtest panels are withheld until they are reproduced on the prospective evidence stream.
        </div>

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
