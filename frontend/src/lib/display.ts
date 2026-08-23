import type {
  CalibrationDiagnosticsResponse,
  DecisionImpactResponse,
  OpportunitiesResponse,
  OpportunityItem,
  PlanData,
} from './types'

export function formatBand(value: number | null): string {
  return value === null ? '—' : `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`
}

export function formatProbability(value: number | null, digits = 0): string {
  if (value === null || Number.isNaN(value)) return '—'
  return `${(value * 100).toFixed(digits)}%`
}

export function formatMaybePercent(value: number | null, digits = 1): string {
  if (value === null || Number.isNaN(value)) return '—'
  return `${value >= 0 ? '+' : ''}${value.toFixed(digits)}%`
}

export function formatDecisionImpactBasis(value: DecisionImpactResponse['outcome_basis'] | null | undefined): string {
  if (value === 'theme_proxy_blend') return 'theme proxy blend'
  return 'SPY proxy'
}

export function formatUnavailableReason(reason: string | null | undefined): string {
  if (!reason) return ''
  return reason.replace(/_/g, ' ')
}

export function formatOpportunityDegradedReason(reason: string | null | undefined): string {
  if (!reason) return ''
  if (reason === 'suppressed_data_quality') return 'Opportunity feed is suppressed due to critical stale inputs or consistency failure.'
  if (reason === 'coherence_gate_failed') return 'Some setups were suppressed by the contract gate.'
  if (reason === 'quality_filtered') return 'Low-information opportunities were filtered from this feed.'
  if (reason === 'refresh_ttl_overdue') return 'Opportunity feed is in watch mode because refresh data is overdue.'
  if (reason === 'refresh_ttl_unknown') return 'Opportunity feed cannot verify refresh recency yet.'
  return reason.replace(/_/g, ' ')
}

export function formatDataAgeSeconds(value: number | null | undefined): string {
  if (typeof value !== 'number' || Number.isNaN(value)) return 'unknown'
  if (value < 60) return `${Math.max(0, Math.round(value))}s`
  const minutes = value / 60
  if (minutes < 60) return `${minutes.toFixed(0)}m`
  const hours = minutes / 60
  if (hours < 48) return `${hours.toFixed(1)}h`
  const days = hours / 24
  return `${days.toFixed(1)}d`
}

export function formatTtlState(state: OpportunitiesResponse['ttl_state']): string {
  if (!state) return 'unknown'
  return state
}

export function ttlStateClass(state: OpportunitiesResponse['ttl_state']): string {
  if (state === 'fresh') return 'border-[#00c896]/40 text-[#00c896]'
  if (state === 'stale') return 'border-[#f59e0b]/40 text-[#f59e0b]'
  if (state === 'overdue') return 'border-[#ff6b6b]/40 text-[#ff6b6b]'
  return 'border-[#949ba5]/40 text-[#949ba5]'
}

export function formatActionabilityState(state: 'ACTIONABLE' | 'WATCH' | 'NO_ACTION' | null | undefined): string {
  if (!state) return 'watch'
  if (state === 'ACTIONABLE') return 'actionable'
  if (state === 'NO_ACTION') return 'no action'
  return 'watch'
}

export function actionabilityClass(state: 'ACTIONABLE' | 'WATCH' | 'NO_ACTION' | null | undefined): string {
  if (state === 'ACTIONABLE') return 'border-[#00c896]/40 text-[#00c896]'
  if (state === 'NO_ACTION') return 'border-[#f59e0b]/40 text-[#f59e0b]'
  return 'border-[#949ba5]/40 text-[#949ba5]'
}

export function formatCtaDisabledReason(reason: string): string {
  if (reason === 'no_eligible_opportunities') return 'no eligible opportunities'
  if (reason === 'suppressed_data_quality') return 'suppressed data quality'
  if (reason === 'calibration_quality_not_robust') return 'calibration quality not robust'
  if (reason === 'calibration_ece_unavailable') return 'calibration ECE unavailable'
  if (reason === 'ece_above_threshold') return 'ECE above threshold'
  if (reason === 'refresh_ttl_overdue') return 'refresh data overdue'
  if (reason === 'refresh_ttl_unknown') return 'refresh recency unknown'
  return reason.replace(/_/g, ' ')
}

export function deriveNoActionUnlockConditions(args: {
  actionabilityReasonCodes?: string[]
  ctaDisabledReasons?: string[]
  evidenceReasonCodes?: string[]
  diagnostics?: CalibrationDiagnosticsResponse | null
}): string[] {
  const reasonCodes = new Set((args.actionabilityReasonCodes || []).filter(Boolean))
  const ctaReasons = new Set((args.ctaDisabledReasons || []).filter(Boolean))
  const evidenceReasons = new Set((args.evidenceReasonCodes || []).filter(Boolean))
  const hasAny = (...codes: string[]): boolean => codes.some((code) => reasonCodes.has(code) || ctaReasons.has(code) || reasonCodes.has(`cta_${code}`))
  const evidenceHasFragment = (...fragments: string[]): boolean => (
    [...evidenceReasons].some((reason) => fragments.some((fragment) => reason.includes(fragment)))
  )
  const hasSampleMaturityBlock = evidenceHasFragment(
    'current_model_rows_unavailable',
    'insufficient_paired_sample',
    'insufficient_discordant_pairs',
    'insufficient_calendar_span',
    'insufficient_weekday_coverage',
  )
  const hasFreshnessBlock = evidenceHasFragment(
    'prediction_freshness_unavailable',
    'actual_freshness_unavailable',
    'actual_observation_freshness_unavailable',
  )

  const unlock: string[] = []

  if (hasAny('no_eligible_opportunities', 'opportunity_coherence_gate_failed', 'high_edge_override_no_eligible')) {
    unlock.push('At least one opportunity must pass coherence (p(correct) >= 50% and aligned expectancy sign).')
  }
  if (hasAny('critical_data_quality_block', 'consistency_fail_block', 'suppressed_data_quality', 'opportunity_suppressed_data_quality')) {
    unlock.push('Critical stale inputs must be zero and consistency must remain PASS.')
  }
  if (hasAny('calibration_quality_not_robust')) {
    unlock.push('Calibration quality must be ROBUST for action CTA.')
  }
  if (hasAny('ece_above_threshold')) {
    const eceNow = args.diagnostics?.diagnostics?.ece
    unlock.push(`Calibration ECE must be <= 0.08${typeof eceNow === 'number' ? ` (current ${eceNow.toFixed(3)})` : ''}.`)
  }
  if (hasAny('calibration_ece_unavailable')) {
    unlock.push('Calibration diagnostics must publish a valid ECE estimate.')
  }
  if (hasAny('refresh_ttl_overdue', 'refresh_ttl_unknown', 'opportunity_refresh_ttl_overdue', 'opportunity_refresh_ttl_unknown')) {
    unlock.push('Latest successful refresh must be within the scheduled TTL window before action CTA unlocks.')
  }
  if (hasSampleMaturityBlock) {
    unlock.push('Current-model predictions must mature into enough evaluated outcomes, with adequate calendar span and weekday coverage; another refresh alone will not clear this gate.')
  }
  if (!hasSampleMaturityBlock && hasFreshnessBlock) {
    unlock.push('Current prediction and outcome observations must satisfy the evidence freshness checks.')
  }
  if (evidenceHasFragment(
    'direction_evidence_unavailable',
    'signed_return_evidence_unavailable',
    'direction_hac_lower_bound_not_positive',
    'signed_return_hac_lower_bound_not_positive',
  )) {
    unlock.push('Prospective direction accuracy and after-cost return uplift must clear their confidence thresholds.')
  }
  if (evidenceHasFragment('validated_spy_forecast_not_bound_to_plan_sizing_or_theme_policy')) {
    unlock.push('A versioned forecast-to-sizing and theme policy must be implemented, tested, and validated.')
  }

  return unlock.length > 0
    ? unlock
    : ['Required prospective evidence and policy gates must pass before allocation can be authorized.']
}

export function summarizeEvidenceBlock(reasonCodes: string[] | null | undefined): string {
  const reasons = (reasonCodes || []).filter(Boolean)
  const hasFragment = (...fragments: string[]): boolean => (
    reasons.some((reason) => fragments.some((fragment) => reason.includes(fragment)))
  )
  const causes: string[] = []
  const currentModelRowsUnavailable = hasFragment('current_model_rows_unavailable')
  const sampleMaturityBlocked = hasFragment(
    'insufficient_paired_sample',
    'insufficient_discordant_pairs',
    'insufficient_calendar_span',
    'insufficient_weekday_coverage',
  )
  const freshnessUnavailable = hasFragment(
    'prediction_freshness_unavailable',
    'actual_freshness_unavailable',
    'actual_observation_freshness_unavailable',
  )

  if (currentModelRowsUnavailable) {
    causes.push('current-model outcomes are not yet available for evaluation')
  } else if (sampleMaturityBlocked) {
    causes.push('current-model outcomes are still accumulating')
  } else if (freshnessUnavailable) {
    causes.push('prediction and outcome freshness cannot yet be established')
  }
  if (hasFragment('direction_evidence_unavailable', 'signed_return_evidence_unavailable')) {
    causes.push('prospective performance evidence is not yet available')
  } else if (hasFragment('direction_hac_lower_bound_not_positive', 'signed_return_hac_lower_bound_not_positive')) {
    causes.push('prospective performance has not cleared its confidence thresholds')
  }
  if (hasFragment('validated_spy_forecast_not_bound_to_plan_sizing_or_theme_policy')) {
    causes.push('the validated SPY forecast is not yet bound to a tested Plan sizing and theme policy')
  }

  if (causes.length === 0) {
    return 'Why: required prospective evidence has not yet passed the allocation gate.'
  }

  const explanation = causes.length === 1
    ? causes[0]
    : `${causes.slice(0, -1).join(', ')}, and ${causes.at(-1)}`
  return `Why: ${explanation}.`
}

export function calibrationQualityClass(quality: 'ROBUST' | 'LIMITED' | 'INSUFFICIENT'): string {
  if (quality === 'ROBUST') return 'border-[#00c896]/40 text-[#00c896]'
  if (quality === 'LIMITED') return 'border-[#f59e0b]/40 text-[#f59e0b]'
  return 'border-[#ff6b6b]/40 text-[#ff6b6b]'
}

export function fallbackEdgeCalibration(score: number): NonNullable<PlanData['edge_quality']['calibration']> {
  const clampedScore = Math.max(0, Math.min(100, Math.round(score)))
  const binStart = clampedScore === 100 ? 90 : Math.floor(clampedScore / 10) * 10
  const binEnd = binStart === 90 ? 100 : binStart + 9
  return {
    bin: `${binStart}-${binEnd}`,
    probability_correct_7d: null,
    ci95_low_7d: null,
    ci95_high_7d: null,
    sample_size_7d: 0,
    quality: 'INSUFFICIENT',
  }
}

export function fallbackOpportunityCalibration(): NonNullable<OpportunityItem['calibration']> {
  return {
    probability_correct_direction: null,
    ci95_low: null,
    ci95_high: null,
    sample_size: 0,
    quality: 'INSUFFICIENT',
    basis: 'conviction_decile',
    window: null,
    unavailable_reason: 'insufficient_sample',
  }
}

export function fallbackOpportunityExpectancy(): NonNullable<OpportunityItem['expectancy']> {
  return {
    expected_move_pct: null,
    max_adverse_move_pct: null,
    sample_size: 0,
    basis: 'none',
    quality: 'INSUFFICIENT',
    unavailable_reason: 'insufficient_sample',
  }
}
