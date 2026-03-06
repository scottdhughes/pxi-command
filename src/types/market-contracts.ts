export type RegimeDelta = 'UNCHANGED' | 'SHIFTED' | 'STRENGTHENED' | 'WEAKENED'
export type RiskPosture = 'risk_on' | 'neutral' | 'risk_off'
export type PolicyStance = 'RISK_ON' | 'RISK_OFF' | 'MIXED'
export type ConsistencyState = 'PASS' | 'WARN' | 'FAIL'
export type OpportunityDirection = 'bullish' | 'bearish' | 'neutral'
export type ConflictState = 'ALIGNED' | 'MIXED' | 'CONFLICT'
export type CalibrationQuality = 'ROBUST' | 'LIMITED' | 'INSUFFICIENT'
export type PlanActionabilityState = 'ACTIONABLE' | 'WATCH' | 'NO_ACTION'
export type OpportunityTtlState = 'fresh' | 'stale' | 'overdue' | 'unknown'
export type RegimeType = 'RISK_ON' | 'RISK_OFF' | 'TRANSITION'
export type SignalType = 'FULL_RISK' | 'REDUCED_RISK' | 'RISK_OFF' | 'DEFENSIVE'
export type DecisionImpactOutcomeBasis = 'spy_forward_proxy' | 'theme_proxy_blend'

export interface AlertData {
  id: number
  date: string
  alert_type: string
  message: string
  severity: 'info' | 'warning' | 'critical'
  acknowledged: boolean
  pxi_score: number | null
  forward_return_7d: number | null
  forward_return_30d: number | null
}

export interface AlertsApiResponse {
  alerts: AlertData[]
  count: number
  filters: {
    types: Array<{
      type: string
      count: number
    }>
  }
  accuracy: Record<string, {
    total: number
    accuracy_7d: number | null
    avg_return_7d: number
  }>
}

export interface PolicyStateContract {
  stance: PolicyStance
  risk_posture: RiskPosture
  conflict_state: ConflictState
  base_signal: SignalType | string
  regime_context: RegimeType
  rationale: string
  rationale_codes: string[]
}

export interface ConsistencyComponents {
  base_score: number
  structural_penalty: number
  reliability_penalty: number
}

export interface ConsistencySnapshotContract {
  score: number
  state: ConsistencyState
  violations: string[]
  components?: ConsistencyComponents
}

export interface FreshnessStatusContract {
  has_stale_data: boolean
  stale_count: number
  critical_stale_count: number
}

export interface BriefData {
  as_of: string
  summary: string
  regime_delta: RegimeDelta
  top_changes: string[]
  risk_posture: RiskPosture
  policy_state: PolicyStateContract
  source_plan_as_of: string
  contract_version: string
  consistency: ConsistencySnapshotContract
  explainability: {
    category_movers: Array<{
      category: string
      score_change: number
    }>
    indicator_movers: Array<{
      indicator_id: string
      value_change: number
      z_impact: number
    }>
  }
  freshness_status: FreshnessStatusContract
  updated_at: string
  degraded_reason: string | null
}

export interface OpportunityCalibration {
  probability_correct_direction: number | null
  ci95_low: number | null
  ci95_high: number | null
  sample_size: number
  quality: CalibrationQuality
  basis: 'conviction_decile'
  window: string | null
  unavailable_reason: string | null
}

export interface OpportunityExpectancy {
  expected_move_pct: number | null
  max_adverse_move_pct: number | null
  sample_size: number
  basis: 'theme_direction' | 'theme_direction_shrunk_prior' | 'direction_prior_proxy' | 'none'
  quality: CalibrationQuality
  unavailable_reason: string | null
}

export interface OpportunityEligibility {
  passed: boolean
  failed_checks: string[]
}

export interface OpportunityDecisionContract {
  coherent: boolean
  confidence_band: 'high' | 'medium' | 'low'
  rationale_codes: string[]
}

export interface OpportunityItem {
  id: string
  symbol: string | null
  theme_id: string
  theme_name: string
  direction: OpportunityDirection
  conviction_score: number
  rationale: string
  supporting_factors: string[]
  historical_hit_rate: number
  sample_size: number
  calibration?: OpportunityCalibration
  expectancy?: OpportunityExpectancy
  eligibility?: OpportunityEligibility
  decision_contract?: OpportunityDecisionContract
  updated_at: string
}

export interface OpportunitySuppressionByReason {
  coherence_failed: number
  quality_filtered: number
  data_quality_suppressed: number
}

export interface OpportunitiesResponse {
  as_of: string
  horizon: '7d' | '30d'
  items: OpportunityItem[]
  suppressed_count: number
  quality_filtered_count?: number
  coherence_suppressed_count?: number
  degraded_reason?: string | null
  suppression_by_reason?: OpportunitySuppressionByReason
  quality_filter_rate?: number
  coherence_fail_rate?: number
  actionability_state?: PlanActionabilityState
  actionability_reason_codes?: string[]
  cta_enabled?: boolean
  cta_disabled_reasons?: string[]
  data_age_seconds?: number | null
  ttl_state?: OpportunityTtlState
  next_expected_refresh_at?: string | null
  overdue_seconds?: number | null
}

export interface DecisionImpactMarketStats {
  sample_size: number
  hit_rate: number
  avg_forward_return_pct: number
  avg_signed_return_pct: number
  win_rate: number
  downside_p10_pct: number
  max_loss_pct: number
  quality_band: CalibrationQuality
}

export interface DecisionImpactThemeStats {
  theme_id: string
  theme_name: string
  sample_size: number
  hit_rate: number
  avg_signed_return_pct: number
  avg_forward_return_pct: number
  win_rate: number
  quality_band: CalibrationQuality
  last_as_of: string
}

export interface DecisionImpactCoverage {
  matured_items: number
  eligible_items: number
  coverage_ratio: number
  insufficient_reasons: string[]
  theme_proxy_eligible_items?: number
  spy_fallback_items?: number
}

export interface DecisionImpactResponse {
  as_of: string
  horizon: '7d' | '30d'
  scope: 'market' | 'theme'
  window_days: 30 | 90
  outcome_basis: DecisionImpactOutcomeBasis
  market: DecisionImpactMarketStats
  themes: DecisionImpactThemeStats[]
  coverage: DecisionImpactCoverage
}

export interface DecisionImpactObserveMode {
  enabled: boolean
  mode: 'observe' | 'enforce'
  thresholds: {
    market_7d_hit_rate_min: number
    market_30d_hit_rate_min: number
    market_7d_avg_signed_return_min: number
    market_30d_avg_signed_return_min: number
    cta_action_rate_pct_min: number
  }
  minimum_samples_required: number
  minimum_actionable_sessions_required: number
  configured_minimum_actionable_sessions_required?: number
  enforce_ready: boolean
  enforce_breaches: string[]
  enforce_breach_count: number
  breaches: string[]
  breach_count: number
}

export interface OpsDecisionImpactResponse {
  as_of: string
  window_days: 30 | 90
  market_7d: DecisionImpactMarketStats
  market_30d: DecisionImpactMarketStats
  theme_summary: {
    themes_with_samples: number
    themes_robust: number
    top_positive: DecisionImpactThemeStats[]
    top_negative: DecisionImpactThemeStats[]
  }
  utility_attribution: {
    actionable_views: number
    actionable_sessions: number
    cta_action_clicks: number
    cta_action_rate_pct: number
    no_action_unlock_views: number
    decision_events_total: number
  }
  observe_mode: DecisionImpactObserveMode
}

export interface CalibrationDiagnosticsResponse {
  as_of: string
  metric: 'conviction' | 'edge_quality'
  horizon: '7d' | '30d' | null
  basis: string
  total_samples: number
  bins: Array<{
    bin: string
    correct_count: number
    probability_correct: number | null
    ci95_low: number | null
    ci95_high: number | null
    sample_size: number
    quality: CalibrationQuality
  }>
  diagnostics: {
    brier_score: number | null
    ece: number | null
    log_loss: number | null
    quality_band: CalibrationQuality
    minimum_reliable_sample: number
    insufficient_reasons: string[]
  }
}

export interface EdgeDiagnosticsResponse {
  as_of: string
  basis: string
  windows: Array<{
    horizon: '7d' | '30d'
    as_of: string | null
    sample_size: number
    model_direction_accuracy: number | null
    baseline_direction_accuracy: number | null
    uplift_vs_baseline: number | null
    uplift_ci95_low: number | null
    uplift_ci95_high: number | null
    lower_bound_positive: boolean
    minimum_reliable_sample: number
    quality_band: CalibrationQuality
    baseline_strategy: 'lagged_actual_direction'
    leakage_sentinel: {
      pass: boolean
      violation_count: number
      reasons: string[]
    }
    calibration_diagnostics: CalibrationDiagnosticsResponse['diagnostics']
  }>
  promotion_gate: {
    pass: boolean
    reasons: string[]
  }
}

export interface MarketFeedAlert {
  id: string
  event_type: 'regime_change' | 'threshold_cross' | 'opportunity_spike' | 'freshness_warning'
  severity: 'info' | 'warning' | 'critical'
  title: string
  body: string
  entity_type: 'market' | 'theme' | 'indicator'
  entity_id: string | null
  created_at: string
}

export interface AlertsFeedResponse {
  as_of: string
  alerts: MarketFeedAlert[]
  degraded_reason?: string | null
}

export interface PlanData {
  as_of: string
  setup_summary: string
  actionability_state?: PlanActionabilityState
  actionability_reason_codes?: string[]
  policy_state?: PolicyStateContract
  action_now: {
    risk_allocation_target: number
    raw_signal_allocation_target: number
    risk_allocation_basis: 'penalized_playbook_target' | 'fallback_neutral'
    horizon_bias: string
    primary_signal: SignalType | string
  }
  edge_quality: {
    score: number
    label: 'HIGH' | 'MEDIUM' | 'LOW'
    breakdown: {
      data_quality: number
      model_agreement: number
      regime_stability: number
    }
    stale_count: number
    ml_sample_size: number
    conflict_state: ConflictState
    calibration?: {
      bin: string | null
      probability_correct_7d: number | null
      ci95_low_7d: number | null
      ci95_high_7d: number | null
      sample_size_7d: number
      quality: CalibrationQuality
    }
  }
  risk_band: {
    d7: { bear: number | null; base: number | null; bull: number | null; sample_size: number }
    d30: { bear: number | null; base: number | null; bull: number | null; sample_size: number }
  }
  uncertainty: {
    headline: string | null
    flags: {
      stale_inputs: boolean
      limited_calibration: boolean
      limited_scenario_sample: boolean
    }
  }
  consistency: ConsistencySnapshotContract
  trader_playbook: {
    recommended_size_pct: { min: number; target: number; max: number }
    scenarios: Array<{ condition: string; action: string; invalidation: string }>
    benchmark_follow_through_7d: {
      hit_rate: number | null
      sample_size: number
      unavailable_reason: string | null
    }
  }
  brief_ref?: {
    as_of: string
    regime_delta: RegimeDelta
    risk_posture: RiskPosture
  }
  opportunity_ref?: {
    as_of: string
    horizon: '7d' | '30d'
    eligible_count: number
    suppressed_count: number
    degraded_reason: string | null
  }
  alerts_ref?: {
    as_of: string
    warning_count_24h: number
    critical_count_24h: number
  }
  cross_horizon?: {
    as_of: string
    state: 'ALIGNED' | 'MIXED' | 'CONFLICT' | 'INSUFFICIENT'
    eligible_7d: number
    eligible_30d: number
    top_direction_7d: OpportunityDirection | null
    top_direction_30d: OpportunityDirection | null
    rationale_codes: string[]
    invalidation_note: string | null
  }
  decision_stack?: {
    what_changed: string
    what_to_do: string
    why_now: string
    confidence: string
    cta_state: PlanActionabilityState
  }
  invalidation_rules: string[]
  degraded_reason: string | null
}

export interface CategoryDetailData {
  category: string
  date: string
  score: number
  weight: number
  percentile_rank: number
  indicators: Array<{
    id: string
    name: string
    raw_value: number
    normalized_value: number
  }>
  history: Array<{
    date: string
    score: number
  }>
}

export interface PXIData {
  date: string
  score: number
  label: string
  status: string
  delta: {
    d1: number | null
    d7: number | null
    d30: number | null
  }
  categories: Array<{
    name: string
    score: number
    weight: number
  }>
  sparkline: Array<{
    date: string
    score: number
  }>
  regime: {
    type: RegimeType
    confidence: number
    description: string
  } | null
  divergence: {
    alerts: Array<{
      type: 'PXI_REGIME' | 'PXI_MOMENTUM' | 'REGIME_SHIFT'
      severity: 'LOW' | 'MEDIUM' | 'HIGH'
      title: string
      description: string
      actionable: boolean
      metrics?: {
        historical_frequency: number
        median_return_7d: number | null
        median_return_30d: number | null
        false_positive_rate: number | null
      }
    }>
  } | null
  dataFreshness?: {
    hasStaleData: boolean
    staleCount: number
    criticalStaleCount?: number
    staleIndicators: Array<{
      id: string
      status?: string
      critical?: boolean
      lastUpdate: string | null
      daysOld: number | null
      maxAgeDays?: number
    }>
    topOffenders?: Array<{
      id: string
      lastUpdate: string | null
      daysOld: number | null
      maxAgeDays: number
      chronic: boolean
      owner: 'market_data' | 'macro_data' | 'risk_ops'
      escalation: 'observe' | 'retry_source' | 'escalate_ops'
    }>
    lastRefreshAtUtc?: string | null
    lastRefreshSource?: string | null
    nextExpectedRefreshAtUtc?: string | null
    nextExpectedRefreshInMinutes?: number
  }
}

export interface SignalData {
  date: string
  state: {
    score: number
    label: string
    status: string
    delta: {
      d1: number | null
      d7: number | null
      d30: number | null
    }
    categories: Array<{
      name: string
      score: number
      weight: number
    }>
  }
  signal: {
    type: SignalType
    risk_allocation: number
    volatility_percentile: number | null
    category_dispersion: number
    adjustments: string[]
    conflict_state?: ConflictState
  }
  regime: {
    type: RegimeType
    confidence: number
    description: string
  } | null
  divergence: PXIData['divergence']
  edge_quality?: PlanData['edge_quality']
  freshness_status?: FreshnessStatusContract
}

export interface MlAccuracyDirectionalMetrics {
  direction_accuracy: string
  mean_absolute_error: string
  sample_size: number
}

export interface MLAccuracyApiResponse {
  as_of?: string
  message?: string
  coverage?: {
    total_predictions: number
    evaluated_count: number
    pending_count: number
  }
  coverage_quality?: CalibrationQuality
  minimum_reliable_sample?: number
  unavailable_reasons?: string[]
  total_predictions: number
  evaluated_count: number
  pending_count: number
  error?: string
  recent_predictions?: Array<Record<string, unknown>>
  metrics: {
    xgboost?: {
      d7: MlAccuracyDirectionalMetrics | null
      d30: MlAccuracyDirectionalMetrics | null
    } | null
    lstm?: {
      d7: MlAccuracyDirectionalMetrics | null
      d30: MlAccuracyDirectionalMetrics | null
    } | null
    ensemble: {
      d7: MlAccuracyDirectionalMetrics | null
      d30: MlAccuracyDirectionalMetrics | null
    } | null
  } | null
}
