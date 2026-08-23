import assert from 'node:assert/strict'
import test from 'node:test'

import { isPlanActionAuthorized } from '../src/lib/plan-contract.js'
import type { PlanData } from '../src/lib/types.js'

function authorizedPlan(): PlanData {
  return {
    decision_contract: {
      contract_version: '2026-08-23-v1',
      headline: 'Actionable plan',
      actionability_state: 'ACTIONABLE',
      action_authorized: true,
      actionability_reason_codes: ['candidate_actionable'],
      evidence: { status: 'PASSED', pass: true, reason_codes: [] },
      consistency_scope: 'INTERNAL_COHERENCE',
    },
    actionability_state: 'ACTIONABLE',
    action_now: {
      action_authorized: true,
      risk_allocation_target: 0.6,
      risk_allocation_basis: 'penalized_playbook_target',
    },
    edge_evidence_gate: { pass: true, reasons: [] },
    edge_quality: { validated_edge: true },
    consistency: { state: 'PASS' },
    opportunity_ref: {
      cta_enabled: true,
      cta_disabled_reasons: [],
      degraded_reason: null,
      ttl_state: 'fresh',
    },
    trader_playbook: {
      authorization: 'AUTHORIZED',
      recommended_size_pct: { min: 50, target: 60, max: 70 },
    },
  } as PlanData
}

test('Plan authorization accepts only a coherent current decision contract', () => {
  assert.equal(isPlanActionAuthorized(authorizedPlan()), true)

  const staleWithinGrace = authorizedPlan()
  staleWithinGrace.opportunity_ref!.ttl_state = 'stale'
  assert.equal(isPlanActionAuthorized(staleWithinGrace), true)
})

test('Plan authorization fails closed for malformed or contradictory authority fields', () => {
  const cases: Array<[string, (plan: PlanData) => void]> = [
    ['wrong contract version', (plan) => { (plan.decision_contract.contract_version as string) = 'legacy' }],
    ['blocked evidence status with pass true', (plan) => { plan.decision_contract.evidence.status = 'BLOCKED' }],
    ['unknown consistency state', (plan) => { (plan.consistency.state as string) = 'UNKNOWN' }],
    ['overdue opportunity TTL', (plan) => { plan.opportunity_ref!.ttl_state = 'overdue' }],
    ['unknown opportunity TTL', (plan) => { plan.opportunity_ref!.ttl_state = 'unknown' }],
    ['missing opportunity TTL', (plan) => { delete (plan.opportunity_ref as { ttl_state?: string }).ttl_state }],
    ['non-finite allocation', (plan) => { plan.action_now.risk_allocation_target = Number.NaN }],
    ['allocation below zero', (plan) => { plan.action_now.risk_allocation_target = -0.01 }],
    ['allocation above one', (plan) => { plan.action_now.risk_allocation_target = 1.01 }],
    ['negative playbook minimum', (plan) => { plan.trader_playbook.recommended_size_pct.min = -1 }],
    ['playbook maximum above 100', (plan) => { plan.trader_playbook.recommended_size_pct.max = 101 }],
    ['unordered playbook sizing', (plan) => { plan.trader_playbook.recommended_size_pct.min = 65 }],
    ['playbook target mismatch', (plan) => { plan.trader_playbook.recommended_size_pct.target = 80 }],
  ]

  for (const [name, mutate] of cases) {
    const plan = authorizedPlan()
    mutate(plan)
    assert.equal(isPlanActionAuthorized(plan), false, name)
  }
})
