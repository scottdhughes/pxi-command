import { useState } from 'react'

import {
  calibrationQualityClass,
  deriveNoActionUnlockConditions,
  fallbackOpportunityCalibration,
  fallbackOpportunityExpectancy,
  formatActionabilityState,
  formatCtaDisabledReason,
  formatDataAgeSeconds,
  formatDecisionImpactBasis,
  formatMaybePercent,
  formatOpportunityDegradedReason,
  formatProbability,
  formatTtlState,
  formatUnavailableReason,
  ttlStateClass,
} from '../lib/display'
import type {
  CalibrationDiagnosticsResponse,
  DecisionImpactResponse,
  EdgeDiagnosticsResponse,
  OpportunitiesResponse,
  OpsDecisionImpactResponse,
} from '../lib/types'

export function OpportunitiesPage({
  data,
  decisionImpact,
  opsDecisionImpact,
  diagnostics,
  edgeDiagnostics,
  horizon,
  onHorizonChange,
  onLogActionIntent,
  onBack,
}: {
  data: OpportunitiesResponse | null
  decisionImpact: DecisionImpactResponse | null
  opsDecisionImpact: OpsDecisionImpactResponse | null
  diagnostics: CalibrationDiagnosticsResponse | null
  edgeDiagnostics: EdgeDiagnosticsResponse | null
  horizon: '7d' | '30d'
  onHorizonChange: (h: '7d' | '30d') => void
  onLogActionIntent: (args: {
    asOf: string
    horizon: '7d' | '30d'
    actionabilityState: 'ACTIONABLE' | 'WATCH' | 'NO_ACTION'
  }) => void
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
  const ttlState = data?.ttl_state || 'unknown'
  const dataAgeText = formatDataAgeSeconds(data?.data_age_seconds)
  const nextExpectedRefresh = data?.next_expected_refresh_at ? new Date(data.next_expected_refresh_at).toLocaleString() : 'unknown'
  const overdueSeconds = typeof data?.overdue_seconds === 'number' ? Math.max(0, data.overdue_seconds) : null
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
  const hasRefreshTtlSuppression = degradedReason === 'refresh_ttl_overdue' || degradedReason === 'refresh_ttl_unknown'
  const edgeWindow = edgeDiagnostics?.windows.find((window) => window.horizon === horizon) || edgeDiagnostics?.windows[0] || null
  const [ctaLoggedKey, setCtaLoggedKey] = useState<string | null>(null)
  const currentCtaKey = data ? `${data.as_of}:${horizon}` : null
  const ctaLogged = currentCtaKey !== null && ctaLoggedKey === currentCtaKey

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

        {edgeDiagnostics && edgeWindow && (
          <div className="mb-4 p-3 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg">
            <div className="flex items-center justify-between gap-2">
              <div className="text-[9px] uppercase tracking-wider text-[#949ba5]/60">Edge diagnostics</div>
              <span className={`rounded border px-2 py-1 text-[8px] uppercase tracking-wider ${
                edgeDiagnostics.promotion_gate.pass
                  ? 'border-[#00c896]/40 text-[#00c896]'
                  : 'border-[#f59e0b]/40 text-[#f59e0b]'
              }`}>
                {edgeDiagnostics.promotion_gate.pass ? 'gate pass' : 'gate blocked'}
              </span>
            </div>
            <div className="mt-2 text-[10px] text-[#d7dbe1]">
              n={edgeWindow.sample_size} · model {formatProbability(edgeWindow.model_direction_accuracy)} ·
              {' '}baseline {formatProbability(edgeWindow.baseline_direction_accuracy)}
            </div>
            <div className="mt-1 text-[10px] text-[#949ba5]">
              uplift {(edgeWindow.uplift_vs_baseline === null ? 'n/a' : `${(edgeWindow.uplift_vs_baseline * 100).toFixed(2)}%`)} ·
              {' '}ci95 [{edgeWindow.uplift_ci95_low === null ? 'n/a' : `${(edgeWindow.uplift_ci95_low * 100).toFixed(2)}%`},
              {' '}{edgeWindow.uplift_ci95_high === null ? 'n/a' : `${(edgeWindow.uplift_ci95_high * 100).toFixed(2)}%`}] ·
              {' '}lower-bound {edgeWindow.lower_bound_positive ? '>0' : '<=0'}
            </div>
            {!edgeWindow.leakage_sentinel.pass && (
              <div className="mt-1 text-[10px] text-[#f59e0b]">
                leakage sentinel: {edgeWindow.leakage_sentinel.reasons.join(', ')}
              </div>
            )}
          </div>
        )}

        {decisionImpact && (
          <div className="impact-card mb-4 p-4 border rounded-lg">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="text-[9px] uppercase tracking-wider text-[#949ba5]/60">
                What happened after similar setups ({formatDecisionImpactBasis(decisionImpact.outcome_basis)})
              </div>
              <span className={`impact-quality-chip rounded border px-2 py-1 text-[8px] uppercase tracking-wider ${calibrationQualityClass(decisionImpact.market.quality_band)}`}>
                market {decisionImpact.market.quality_band.toLowerCase()}
              </span>
            </div>
            <div className="mt-2 grid grid-cols-2 md:grid-cols-4 gap-2 text-[10px]">
              <div className="impact-row">
                <span className="text-[#949ba5]">hit rate</span>
                <span className="font-mono text-[#d7dbe1]">{formatProbability(decisionImpact.market.hit_rate)}</span>
              </div>
              <div className="impact-row">
                <span className="text-[#949ba5]">avg signed</span>
                <span className={`font-mono ${decisionImpact.market.avg_signed_return_pct >= 0 ? 'text-[#00c896]' : 'text-[#ff6b6b]'}`}>
                  {formatMaybePercent(decisionImpact.market.avg_signed_return_pct)}
                </span>
              </div>
              <div className="impact-row">
                <span className="text-[#949ba5]">sample</span>
                <span className="font-mono text-[#d7dbe1]">{decisionImpact.market.sample_size}</span>
              </div>
              <div className="impact-row">
                <span className="text-[#949ba5]">coverage</span>
                <span className="font-mono text-[#d7dbe1]">{formatProbability(decisionImpact.coverage.coverage_ratio)}</span>
              </div>
            </div>
            {decisionImpact.scope === 'theme' && (
              <div className="mt-2 text-[9px] text-[#949ba5]/70">
                theme-proxy eligible {decisionImpact.coverage.theme_proxy_eligible_items ?? 0} ·
                {' '}SPY fallback {decisionImpact.coverage.spy_fallback_items ?? 0}
              </div>
            )}
            {decisionImpact.themes.length > 0 && (
              <div className="mt-3">
                <div className="text-[9px] uppercase tracking-wider text-[#949ba5]/55">Top themes</div>
                <div className="mt-2 space-y-1.5">
                  {decisionImpact.themes.slice(0, 5).map((theme) => (
                    <div key={theme.theme_id} className="impact-row text-[10px]">
                      <span className="text-[#cfd5de]">{theme.theme_name}</span>
                      <span className="font-mono text-[#949ba5]">
                        hit {formatProbability(theme.hit_rate)} · signed {formatMaybePercent(theme.avg_signed_return_pct)} · n={theme.sample_size}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {opsDecisionImpact && (
          <div className="mb-4 text-[10px] text-[#949ba5]/80">
            {opsDecisionImpact.observe_mode.mode} mode ·
            {' '}observe breaches {opsDecisionImpact.observe_mode.breach_count} ·
            {' '}enforce breaches {opsDecisionImpact.observe_mode.enforce_breach_count} ·
            {' '}enforce ready {opsDecisionImpact.observe_mode.enforce_ready ? 'yes' : 'no'} ·
            {' '}cta action rate {opsDecisionImpact.utility_attribution.cta_action_rate_pct.toFixed(2)}%
            {' '}({opsDecisionImpact.utility_attribution.cta_action_clicks}/{opsDecisionImpact.utility_attribution.actionable_sessions} sessions)
          </div>
        )}

        {degradedReason && (
          <div className={`mb-4 text-[10px] ${hasDataQualitySuppression || hasContractGateSuppression ? 'text-[#f59e0b]' : 'text-[#949ba5]'}`}>
            {formatOpportunityDegradedReason(degradedReason)}
          </div>
        )}

        <div className="mb-4 flex flex-wrap items-center gap-2 text-[9px] uppercase tracking-wider">
          <span className={`px-2 py-1 border rounded ${ttlStateClass(ttlState)}`}>
            ttl {formatTtlState(ttlState)}
          </span>
          <span className="px-2 py-1 border border-[#26272b] rounded text-[#949ba5]">
            data age {dataAgeText}
          </span>
          <span className="px-2 py-1 border border-[#26272b] rounded text-[#949ba5]">
            next refresh {nextExpectedRefresh}
          </span>
          {ttlState === 'overdue' && overdueSeconds !== null && (
            <span className="px-2 py-1 border border-[#ff6b6b]/40 rounded text-[#ff6b6b]">
              overdue {(overdueSeconds / 3600).toFixed(1)}h
            </span>
          )}
        </div>

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

        {ctaEnabled && actionabilityState !== 'NO_ACTION' && data && (
          <div className="mb-4 flex flex-wrap items-center gap-3 rounded border border-[#1f3e56] bg-[#081521]/70 px-3 py-2">
            <button
              onClick={() => {
                onLogActionIntent({
                  asOf: data.as_of,
                  horizon,
                  actionabilityState,
                })
                setCtaLoggedKey(currentCtaKey)
              }}
              className="px-3 py-1.5 border border-[#00a3ff] text-[#00a3ff] rounded text-[10px] uppercase tracking-[0.18em] hover:text-[#7ccfff] hover:border-[#7ccfff]"
            >
              Log Action Intent
            </button>
            <span className="text-[10px] text-[#9ec5e2]">
              {ctaLogged ? 'Action intent logged for this session/horizon.' : 'Use this when you act on the current opportunity setup.'}
            </span>
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
                  : hasRefreshTtlSuppression
                    ? 'No opportunities available while refresh recency is outside TTL policy.'
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
                      unavailable: {[calibration.unavailable_reason, expectancy.unavailable_reason].filter(Boolean).map((reason) => formatUnavailableReason(reason)).join(' · ')}
                    </div>
                  )}
                  {calibration.quality !== 'ROBUST' && (
                    <div className="mt-1 text-[9px] text-[#f59e0b]">
                      Limited calibration quality; treat this as exploratory risk.
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}
