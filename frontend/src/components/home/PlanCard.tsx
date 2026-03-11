import {
  actionabilityClass,
  calibrationQualityClass,
  deriveNoActionUnlockConditions,
  fallbackEdgeCalibration,
  formatActionabilityState,
  formatBand,
  formatOpportunityDegradedReason,
  formatProbability,
  formatUnavailableReason,
} from '../../lib/display'
import type { PlanData } from '../../lib/types'

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

export function TodayPlanCard({ plan }: { plan: PlanData | null }) {
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
  const noEligibleContractGate =
    plan.opportunity_ref?.degraded_reason === 'coherence_gate_failed' &&
    plan.opportunity_ref?.eligible_count === 0
  const crossHorizonState = plan.cross_horizon?.state || null
  const crossHorizonClass =
    crossHorizonState === 'ALIGNED' ? 'border-[#00c896]/40 text-[#00c896]' :
    crossHorizonState === 'MIXED' ? 'border-[#f59e0b]/40 text-[#f59e0b]' :
    crossHorizonState === 'CONFLICT' ? 'border-[#ff6b6b]/40 text-[#ff6b6b]' :
    'border-[#26272b] text-[#949ba5]'
  const decisionStack = plan.decision_stack || {
    what_changed: plan.setup_summary,
    what_to_do: actionabilityState === 'ACTIONABLE'
      ? 'Execute with playbook risk controls.'
      : actionabilityState === 'WATCH'
        ? 'Hold watch posture until confirmation.'
        : 'No action; wait for unlock conditions.',
    why_now: `${plan.edge_quality.label} edge with ${plan.consistency.state} consistency.`,
    confidence: `edge=${plan.edge_quality.label} | consistency=${plan.consistency.state}`,
    cta_state: actionabilityState,
  }

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

      <div className="mb-3 rounded border border-[#26272b] bg-[#050608]/70 px-3 py-2">
        <div className="flex items-center justify-between gap-2">
          <p className="text-[9px] uppercase tracking-wider text-[#949ba5]">Decision Stack</p>
          <span className={`rounded border px-2 py-1 text-[8px] uppercase tracking-wider ${actionabilityClass(decisionStack.cta_state)}`}>
            {formatActionabilityState(decisionStack.cta_state)}
          </span>
        </div>
        <p className="mt-1 text-[10px] text-[#d7dbe1]"><span className="text-[#949ba5]">changed:</span> {decisionStack.what_changed}</p>
        <p className="mt-1 text-[10px] text-[#d7dbe1]"><span className="text-[#949ba5]">do:</span> {decisionStack.what_to_do}</p>
        <p className="mt-1 text-[10px] text-[#d7dbe1]"><span className="text-[#949ba5]">why:</span> {decisionStack.why_now}</p>
        <p className="mt-1 text-[9px] text-[#949ba5]/80">{decisionStack.confidence}</p>
      </div>

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
            {crossHorizonState && (
              <span className={`rounded border px-2 py-1 ${crossHorizonClass}`}>
                cross-horizon {crossHorizonState.toLowerCase()}
              </span>
            )}
          </div>
          {rawTargetPct !== targetPct && (
            <p className="mt-1 text-[9px] text-[#949ba5]/75">
              raw {rawTargetPct}% · {plan.action_now.risk_allocation_basis.replace(/_/g, ' ')}
            </p>
          )}
          {plan.cross_horizon?.invalidation_note && (
            <p className="mt-1 text-[10px] text-[#f59e0b]">
              {plan.cross_horizon.invalidation_note}
            </p>
          )}
          <p className="mt-2 text-[12px] leading-relaxed text-[#e4e8ee]">{plan.setup_summary}</p>
          {opportunitySuppressed && (
            <p className="mt-2 text-[10px] text-[#f59e0b]">
              {noEligibleContractGate
                ? 'Opportunity feed currently suppressed: No eligible opportunities (contract gate).'
                : formatOpportunityDegradedReason(plan.opportunity_ref?.degraded_reason)}
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
