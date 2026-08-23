import assert from 'node:assert/strict'
import test from 'node:test'

import {
  deriveNoActionUnlockConditions,
  summarizeEvidenceBlock,
} from '../src/lib/display.js'

const WARMUP_REASONS = [
  'integrity:7d:current_model_rows_unavailable',
  '7d:evidence:eligibility:actual_observation_freshness_unavailable',
  '7d:performance:direction_evidence_unavailable',
  'policy_alignment:validated_spy_forecast_not_bound_to_plan_sizing_or_theme_policy',
]

test('Plan evidence reasons explain warm-up, performance, and policy gates', () => {
  assert.equal(
    summarizeEvidenceBlock(WARMUP_REASONS),
    'Why: current-model outcomes are not yet available for evaluation, prospective performance evidence is not yet available, and the validated SPY forecast is not yet bound to a tested Plan sizing and theme policy.',
  )

  assert.deepEqual(
    deriveNoActionUnlockConditions({
      actionabilityReasonCodes: ['edge_evidence_gate_block'],
      evidenceReasonCodes: WARMUP_REASONS,
    }),
    [
      'Current-model predictions must mature into enough evaluated outcomes, with adequate calendar span and weekday coverage; another refresh alone will not clear this gate.',
      'Prospective direction accuracy and after-cost return uplift must clear their confidence thresholds.',
      'A versioned forecast-to-sizing and theme policy must be implemented, tested, and validated.',
    ],
  )
})

test('unknown no-action reasons stay fail-closed without promising a next-refresh unlock', () => {
  assert.equal(
    summarizeEvidenceBlock(['unexpected_gate_reason']),
    'Why: required prospective evidence has not yet passed the allocation gate.',
  )
  assert.deepEqual(
    deriveNoActionUnlockConditions({ actionabilityReasonCodes: ['unexpected_gate_reason'] }),
    ['Required prospective evidence and policy gates must pass before allocation can be authorized.'],
  )
})

test('live prefixed outcome-freshness reason is translated when it is the only blocker', () => {
  const reason = '7d:evidence:eligibility:actual_observation_freshness_unavailable'

  assert.equal(
    summarizeEvidenceBlock([reason]),
    'Why: prediction and outcome freshness cannot yet be established.',
  )
  assert.deepEqual(
    deriveNoActionUnlockConditions({ evidenceReasonCodes: [reason] }),
    ['Current prediction and outcome observations must satisfy the evidence freshness checks.'],
  )
})
