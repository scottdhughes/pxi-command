import { SiteDisclaimer } from '../components/SiteDisclaimer'
import {
  calibrationQualityClass,
  formatDecisionImpactBasis,
  formatMaybePercent,
  formatProbability,
} from '../lib/display'
import type { BriefData, DecisionImpactResponse } from '../lib/types'

export function BriefPage({
  brief,
  decisionImpact,
  onBack,
}: {
  brief: BriefData | null
  decisionImpact: DecisionImpactResponse | null
  onBack: () => void
}) {
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

            {decisionImpact && (
              <div className="impact-card p-4 border rounded-lg">
                <div className="flex items-center justify-between gap-2">
                  <div className="text-[9px] uppercase tracking-wider text-[#949ba5]/60">
                    Market impact ({formatDecisionImpactBasis(decisionImpact.outcome_basis)})
                  </div>
                  <span className={`impact-quality-chip rounded border px-2 py-1 text-[8px] uppercase tracking-wider ${calibrationQualityClass(decisionImpact.market.quality_band)}`}>
                    {decisionImpact.market.quality_band.toLowerCase()}
                  </span>
                </div>
                <div className="mt-2 grid grid-cols-2 gap-2 text-[10px]">
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
                    <span className="text-[#949ba5]">sample size</span>
                    <span className="font-mono text-[#d7dbe1]">{decisionImpact.market.sample_size}</span>
                  </div>
                  <div className="impact-row">
                    <span className="text-[#949ba5]">coverage</span>
                    <span className="font-mono text-[#d7dbe1]">{formatProbability(decisionImpact.coverage.coverage_ratio)}</span>
                  </div>
                </div>
              </div>
            )}

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
      <SiteDisclaimer className="mt-8 pb-2" />
    </div>
  )
}
