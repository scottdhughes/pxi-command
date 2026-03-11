import { useState } from 'react'

import { fetchApi } from '../../lib/api'
import {
  calibrationQualityClass,
  formatActionabilityState,
  formatCtaDisabledReason,
  formatDataAgeSeconds,
  formatMaybePercent,
  formatOpportunityDegradedReason,
  formatProbability,
  formatTtlState,
  formatUnavailableReason,
  ttlStateClass,
  fallbackOpportunityCalibration,
  fallbackOpportunityExpectancy,
} from '../../lib/display'
import type {
  BacktestData,
  BriefData,
  EnsembleData,
  MLAccuracyData,
  OpportunitiesResponse,
  SignalsData,
  SimilarPeriodsData,
} from '../../lib/types'
import { MLAccuracyBadge } from './MarketCharts'

export function TopThemesWidget({ data, regime }: { data: SignalsData | null; regime?: 'RISK_ON' | 'RISK_OFF' | 'TRANSITION' }) {
  if (!data || !data.themes || data.themes.length === 0) return null

  const topThemes = data.themes.slice(0, 3)

  const getConfidenceColor = (confidence: string) => {
    if (confidence === 'Very High' || confidence === 'High') return 'text-[#00c896]'
    if (confidence === 'Medium-Low' || confidence === 'Low') return 'text-[#ff6b6b]'
    return 'text-[#f5a524]'
  }

  const getTimingIcon = (timing: string) => {
    if (timing === 'Now') return '↑'
    if (timing === 'Building') return '→'
    if (timing === 'Ongoing') return '◆'
    return '○'
  }

  const getSignalTypeColor = (signalType: string) => {
    if (signalType === 'Momentum') return 'bg-[#00c896]/20 text-[#00c896]'
    if (signalType === 'Rotation') return 'bg-[#00a3ff]/20 text-[#00a3ff]'
    if (signalType === 'Divergence') return 'bg-[#f5a524]/20 text-[#f5a524]'
    return 'bg-[#949ba5]/20 text-[#949ba5]'
  }

  const renderStars = (stars: number) => '★'.repeat(stars) + '☆'.repeat(5 - stars)

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

export function SimilarPeriodsCard({ data }: { data: SimilarPeriodsData | null }) {
  if (!data || !data.similar_periods || data.similar_periods.length === 0) return null

  const formatReturn = (val: number | null) => {
    if (val === null) return '—'
    const color = val >= 0 ? 'text-[#00c896]' : 'text-[#ff6b6b]'
    return <span className={color}>{val >= 0 ? '+' : ''}{val.toFixed(2)}%</span>
  }

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
    .map((period) => period.forward_returns?.d30)
    .filter((value): value is number => typeof value === 'number')

  const positiveCount = valid30dReturns.filter((value) => value > 0).length
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

export function BacktestCard({ data }: { data: BacktestData | null }) {
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

      <div className="text-[8px] text-[#949ba5]/40 uppercase tracking-wider mb-2">
        30-Day Returns by PXI Bucket
      </div>
      <div className="space-y-1.5">
        {data.bucket_analysis.map((bucket) => (
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

export function BriefCompactCard({
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
    <div className={className || 'w-full mt-6 p-4 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg'}>
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

export function OpportunityPreview({ data, onOpen }: { data: OpportunitiesResponse | null; onOpen: () => void }) {
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
  const ttlState = data.ttl_state || 'unknown'
  const dataAgeText = formatDataAgeSeconds(data.data_age_seconds)
  const nextExpectedRefresh = data.next_expected_refresh_at ? new Date(data.next_expected_refresh_at).toLocaleString() : 'unknown'
  const hasFeedState = top.length > 0 || suppressedCount > 0 || Boolean(data.degraded_reason)
  const noEligibleContractGate = data.degraded_reason === 'coherence_gate_failed' && top.length === 0

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
          {noEligibleContractGate
            ? 'Opportunity feed currently suppressed: No eligible opportunities (contract gate).'
            : formatOpportunityDegradedReason(data.degraded_reason)}
        </p>
      )}
      <div className="mb-3 flex flex-wrap items-center gap-2 text-[9px] uppercase tracking-wider">
        <span className={`px-2 py-1 border rounded ${ttlStateClass(ttlState)}`}>
          ttl {formatTtlState(ttlState)}
        </span>
        <span className="px-2 py-1 border border-[#26272b] rounded text-[#949ba5]">
          data age {dataAgeText}
        </span>
        <span className="px-2 py-1 border border-[#26272b] rounded text-[#949ba5]">
          next scheduled refresh {nextExpectedRefresh}
        </span>
      </div>
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
        <div className="text-[10px] text-[#949ba5]">
          {noEligibleContractGate
            ? 'No eligible opportunities are currently published.'
            : 'No eligible opportunities currently published.'}
        </div>
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
            )
          })}
        </div>
      )}
    </div>
  )
}

export function ExportButton() {
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

export function MLPredictionsCard({ ensemble, accuracy }: { ensemble: EnsembleData | null; accuracy?: MLAccuracyData | null }) {
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

      <MLAccuracyBadge accuracy={accuracy ?? null} />

      <div className="text-[8px] text-[#949ba5]/20 text-center mt-1">
        Weighted ensemble • {ensemble.interpretation.d7.note}
      </div>
    </div>
  )
}
