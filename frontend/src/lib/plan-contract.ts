import type { PlanData } from './types'

function isFiniteInRange(value: unknown, min: number, max: number): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= min && value <= max
}

export function isPlanActionAuthorized(plan: PlanData | null | undefined): boolean {
  const decisionContract = plan?.decision_contract
  const actionNow = plan?.action_now
  const evidenceGate = plan?.edge_evidence_gate
  const opportunityRef = plan?.opportunity_ref
  const playbook = plan?.trader_playbook
  const sizing = playbook?.recommended_size_pct
  const allocationTarget = actionNow?.risk_allocation_target

  if (!isFiniteInRange(allocationTarget, 0, 1)) return false
  if (!isFiniteInRange(sizing?.min, 0, 100)) return false
  if (!isFiniteInRange(sizing?.target, 0, 100)) return false
  if (!isFiniteInRange(sizing?.max, 0, 100)) return false
  if (sizing.min > sizing.target || sizing.target > sizing.max) return false
  if (Math.abs(sizing.target - (allocationTarget * 100)) > 0.5) return false

  const consistencyState = plan?.consistency?.state
  const ttlState = opportunityRef?.ttl_state
  return Boolean(
    decisionContract?.contract_version === '2026-08-23-v1' &&
    decisionContract.headline === 'Actionable plan' &&
    decisionContract.action_authorized === true &&
    decisionContract.actionability_state === 'ACTIONABLE' &&
    decisionContract.evidence.pass === true &&
    decisionContract.evidence.status === 'PASSED' &&
    decisionContract.consistency_scope === 'INTERNAL_COHERENCE' &&
    plan?.actionability_state === 'ACTIONABLE' &&
    actionNow?.action_authorized === true &&
    actionNow.risk_allocation_basis === 'penalized_playbook_target' &&
    evidenceGate?.pass === true &&
    plan?.edge_quality?.validated_edge === true &&
    (consistencyState === 'PASS' || consistencyState === 'WARN') &&
    opportunityRef?.cta_enabled === true &&
    opportunityRef.degraded_reason === null &&
    Array.isArray(opportunityRef.cta_disabled_reasons) &&
    opportunityRef.cta_disabled_reasons.length === 0 &&
    (ttlState === 'fresh' || ttlState === 'stale') &&
    playbook?.authorization === 'AUTHORIZED'
  )
}
