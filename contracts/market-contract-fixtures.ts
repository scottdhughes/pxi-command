import type {
  AlertsApiResponse,
  AlertsFeedResponse,
  BriefData,
  CalibrationDiagnosticsResponse,
  CategoryDetailData,
  DecisionImpactResponse,
  EdgeDiagnosticsResponse,
  MLAccuracyApiResponse,
  OpportunitiesResponse,
  OpsDecisionImpactResponse,
  PlanData,
  PXIData,
  SignalData,
} from '../src/types/market-contracts';

export const pxiFixture = {
  date: '2026-03-05',
  score: 67.4,
  label: 'Constructive',
  status: 'GREEN',
  delta: {
    d1: 1.2,
    d7: 4.1,
    d30: -2.6,
  },
  categories: [
    { name: 'Macro', score: 71.2, weight: 0.35 },
    { name: 'Credit', score: 64.8, weight: 0.25 },
    { name: 'Breadth', score: 60.1, weight: 0.2 },
  ],
  sparkline: [
    { date: '2026-03-01', score: 61.5 },
    { date: '2026-03-02', score: 62.9 },
    { date: '2026-03-03', score: 64.2 },
    { date: '2026-03-04', score: 66.2 },
    { date: '2026-03-05', score: 67.4 },
  ],
  regime: {
    type: 'RISK_ON',
    confidence: 0.78,
    description: 'Breadth and credit remain supportive.',
  },
  divergence: {
    alerts: [
      {
        type: 'PXI_REGIME',
        severity: 'MEDIUM',
        title: 'PXI diverges from prior regime pacing',
        description: 'Momentum improved faster than the trailing regime baseline.',
        actionable: true,
        metrics: {
          historical_frequency: 0.18,
          median_return_7d: 1.7,
          median_return_30d: 3.9,
          false_positive_rate: 0.21,
        },
      },
    ],
  },
  dataFreshness: {
    hasStaleData: true,
    staleCount: 2,
    criticalStaleCount: 1,
    staleIndicators: [
      {
        id: 'fed-balance-sheet',
        status: 'STALE',
        critical: true,
        lastUpdate: '2026-03-02T14:00:00.000Z',
        daysOld: 3,
        maxAgeDays: 2,
      },
    ],
    topOffenders: [
      {
        id: 'fed-balance-sheet',
        lastUpdate: '2026-03-02T14:00:00.000Z',
        daysOld: 3,
        maxAgeDays: 2,
        chronic: false,
        owner: 'macro_data',
        escalation: 'retry_source',
      },
    ],
    lastRefreshAtUtc: '2026-03-05T13:30:00.000Z',
    lastRefreshSource: 'github_actions',
    nextExpectedRefreshAtUtc: '2026-03-05T18:00:00.000Z',
    nextExpectedRefreshInMinutes: 240,
  },
} satisfies PXIData;

export const signalFixture = {
  date: '2026-03-05',
  state: {
    score: 67.4,
    label: 'Constructive',
    status: 'GREEN',
    delta: {
      d1: 1.2,
      d7: 4.1,
      d30: -2.6,
    },
    categories: pxiFixture.categories,
  },
  signal: {
    type: 'FULL_RISK',
    risk_allocation: 0.85,
    volatility_percentile: 0.44,
    category_dispersion: 0.18,
    adjustments: ['policy_tailwind', 'breadth_confirmation'],
    conflict_state: 'ALIGNED',
  },
  regime: pxiFixture.regime,
  divergence: pxiFixture.divergence,
  edge_quality: {
    score: 74,
    label: 'HIGH',
    breakdown: {
      data_quality: 22,
      model_agreement: 28,
      regime_stability: 24,
    },
    stale_count: 2,
    ml_sample_size: 126,
    conflict_state: 'ALIGNED',
    calibration: {
      bin: '0.7-0.8',
      probability_correct_7d: 0.67,
      ci95_low_7d: 0.58,
      ci95_high_7d: 0.75,
      sample_size_7d: 83,
      quality: 'ROBUST',
    },
  },
  freshness_status: {
    has_stale_data: true,
    stale_count: 2,
    critical_stale_count: 1,
  },
} satisfies SignalData;

export const planFixture = {
  as_of: '2026-03-05',
  setup_summary: 'Risk remains supported, but stale macro inputs still limit conviction.',
  actionability_state: 'ACTIONABLE',
  actionability_reason_codes: [
    'high_edge_with_eligible_opportunities',
    'opportunity_theme-semiconductors',
  ],
  policy_state: {
    stance: 'RISK_ON',
    risk_posture: 'risk_on',
    conflict_state: 'ALIGNED',
    base_signal: 'FULL_RISK',
    regime_context: 'RISK_ON',
    rationale: 'Macro and breadth remain aligned.',
    rationale_codes: ['breadth_confirmation', 'credit_support'],
  },
  action_now: {
    risk_allocation_target: 0.85,
    raw_signal_allocation_target: 0.9,
    risk_allocation_basis: 'penalized_playbook_target',
    horizon_bias: '7d stronger than 30d',
    primary_signal: 'FULL_RISK',
  },
  edge_quality: signalFixture.edge_quality!,
  risk_band: {
    d7: { bear: -2.4, base: 1.8, bull: 4.2, sample_size: 83 },
    d30: { bear: -5.1, base: 4.7, bull: 9.8, sample_size: 67 },
  },
  uncertainty: {
    headline: 'One critical stale macro series is still unresolved.',
    flags: {
      stale_inputs: true,
      limited_calibration: false,
      limited_scenario_sample: false,
    },
  },
  consistency: {
    score: 81,
    state: 'PASS',
    violations: [],
    components: {
      base_score: 88,
      structural_penalty: 4,
      reliability_penalty: 3,
    },
  },
  trader_playbook: {
    recommended_size_pct: { min: 35, target: 55, max: 75 },
    scenarios: [
      {
        condition: 'Breadth keeps expanding while credit spreads stay contained.',
        action: 'Hold core risk and add to strongest aligned themes.',
        invalidation: 'Trim if spreads widen and breadth rolls over together.',
      },
    ],
    benchmark_follow_through_7d: {
      hit_rate: 0.63,
      sample_size: 52,
      unavailable_reason: null,
    },
  },
  brief_ref: {
    as_of: '2026-03-05',
    regime_delta: 'STRENGTHENED',
    risk_posture: 'risk_on',
  },
  opportunity_ref: {
    as_of: '2026-03-05',
    horizon: '7d',
    eligible_count: 2,
    suppressed_count: 1,
    degraded_reason: null,
  },
  alerts_ref: {
    as_of: '2026-03-05',
    warning_count_24h: 1,
    critical_count_24h: 1,
  },
  cross_horizon: {
    as_of: '2026-03-05',
    state: 'ALIGNED',
    eligible_7d: 2,
    eligible_30d: 1,
    top_direction_7d: 'bullish',
    top_direction_30d: 'bullish',
    rationale_codes: ['cross_horizon_alignment'],
    invalidation_note: null,
  },
  decision_stack: {
    what_changed: 'Macro breadth improved and coherence passed.',
    what_to_do: 'Stay risk-on with selective additions.',
    why_now: 'Both edge quality and opportunity eligibility are supportive.',
    confidence: 'High confidence with one stale-data watch item.',
    cta_state: 'ACTIONABLE',
  },
  invalidation_rules: [
    'If critical stale inputs persist through next refresh cycle, downgrade to watch.',
    'If cross-horizon alignment breaks, reduce target size.',
  ],
  degraded_reason: null,
} satisfies PlanData;

export const briefFixture = {
  as_of: '2026-03-05',
  summary: 'Risk posture improved as breadth and credit stayed constructive.',
  regime_delta: 'STRENGTHENED',
  top_changes: [
    'Credit spreads narrowed versus the prior session.',
    'Breadth participation broadened across cyclicals.',
  ],
  risk_posture: 'risk_on',
  policy_state: planFixture.policy_state!,
  source_plan_as_of: '2026-03-05',
  contract_version: 'v1',
  consistency: planFixture.consistency,
  explainability: {
    category_movers: [
      { category: 'Breadth', score_change: 4.8 },
      { category: 'Credit', score_change: 3.1 },
    ],
    indicator_movers: [
      { indicator_id: 'high_yield_oas', value_change: -0.22, z_impact: 0.61 },
      { indicator_id: 'advance_decline_line', value_change: 1.4, z_impact: 0.58 },
    ],
  },
  freshness_status: signalFixture.freshness_status!,
  updated_at: '2026-03-05T13:35:00.000Z',
  degraded_reason: null,
} satisfies BriefData;

export const opportunitiesFixture = {
  as_of: '2026-03-05',
  horizon: '7d',
  items: [
    {
      id: 'opp-semiconductors-2026-03-05',
      symbol: 'SMH',
      theme_id: 'theme-semiconductors',
      theme_name: 'Semiconductors',
      direction: 'bullish',
      conviction_score: 82,
      rationale: 'Leadership remains aligned with improving breadth.',
      supporting_factors: ['positive earnings revisions', 'relative strength breakout'],
      historical_hit_rate: 0.64,
      sample_size: 47,
      calibration: {
        probability_correct_direction: 0.66,
        ci95_low: 0.53,
        ci95_high: 0.77,
        sample_size: 47,
        quality: 'ROBUST',
        basis: 'conviction_decile',
        window: 'rolling_180d',
        unavailable_reason: null,
      },
      expectancy: {
        expected_move_pct: 3.8,
        max_adverse_move_pct: -1.7,
        sample_size: 47,
        basis: 'theme_direction_shrunk_prior',
        quality: 'ROBUST',
        unavailable_reason: null,
      },
      eligibility: {
        passed: true,
        failed_checks: [],
      },
      decision_contract: {
        coherent: true,
        confidence_band: 'high',
        rationale_codes: ['trend_alignment', 'robust_calibration'],
      },
      updated_at: '2026-03-05T13:36:00.000Z',
    },
  ],
  suppressed_count: 1,
  quality_filtered_count: 0,
  coherence_suppressed_count: 1,
  degraded_reason: null,
  suppression_by_reason: {
    coherence_failed: 1,
    quality_filtered: 0,
    data_quality_suppressed: 0,
  },
  quality_filter_rate: 0,
  coherence_fail_rate: 0.5,
  actionability_state: 'ACTIONABLE',
  actionability_reason_codes: ['high_edge_with_eligible_opportunities'],
  cta_enabled: true,
  cta_disabled_reasons: [],
  data_age_seconds: 420,
  ttl_state: 'fresh',
  next_expected_refresh_at: '2026-03-05T18:00:00.000Z',
  overdue_seconds: null,
} satisfies OpportunitiesResponse;

export const decisionImpactFixture = {
  as_of: '2026-03-05',
  horizon: '7d',
  scope: 'market',
  window_days: 30,
  outcome_basis: 'spy_forward_proxy',
  market: {
    sample_size: 28,
    hit_rate: 0.64,
    avg_forward_return_pct: 1.9,
    avg_signed_return_pct: 1.3,
    win_rate: 0.61,
    downside_p10_pct: -1.8,
    max_loss_pct: -4.4,
    quality_band: 'ROBUST',
  },
  themes: [
    {
      theme_id: 'theme-semiconductors',
      theme_name: 'Semiconductors',
      sample_size: 18,
      hit_rate: 0.67,
      avg_signed_return_pct: 2.1,
      avg_forward_return_pct: 2.6,
      win_rate: 0.61,
      quality_band: 'ROBUST',
      last_as_of: '2026-03-05',
    },
  ],
  coverage: {
    matured_items: 28,
    eligible_items: 31,
    coverage_ratio: 0.9,
    insufficient_reasons: [],
    theme_proxy_eligible_items: 22,
    spy_fallback_items: 6,
  },
} satisfies DecisionImpactResponse;

export const opsDecisionImpactFixture = {
  as_of: '2026-03-05',
  window_days: 30,
  market_7d: decisionImpactFixture.market,
  market_30d: {
    sample_size: 19,
    hit_rate: 0.58,
    avg_forward_return_pct: 3.7,
    avg_signed_return_pct: 2.2,
    win_rate: 0.58,
    downside_p10_pct: -3.6,
    max_loss_pct: -7.1,
    quality_band: 'LIMITED',
  },
  theme_summary: {
    themes_with_samples: 8,
    themes_robust: 3,
    top_positive: decisionImpactFixture.themes,
    top_negative: [
      {
        theme_id: 'theme-utilities',
        theme_name: 'Utilities',
        sample_size: 12,
        hit_rate: 0.42,
        avg_signed_return_pct: -1.1,
        avg_forward_return_pct: 0.3,
        win_rate: 0.42,
        quality_band: 'LIMITED',
        last_as_of: '2026-03-05',
      },
    ],
  },
  utility_attribution: {
    actionable_views: 124,
    actionable_sessions: 88,
    cta_action_clicks: 23,
    cta_action_rate_pct: 26.1,
    no_action_unlock_views: 9,
    decision_events_total: 241,
  },
  observe_mode: {
    enabled: true,
    mode: 'observe',
    thresholds: {
      market_7d_hit_rate_min: 0.55,
      market_30d_hit_rate_min: 0.54,
      market_7d_avg_signed_return_min: 0.5,
      market_30d_avg_signed_return_min: 0.75,
      cta_action_rate_pct_min: 20,
    },
    minimum_samples_required: 20,
    minimum_actionable_sessions_required: 30,
    configured_minimum_actionable_sessions_required: 30,
    enforce_ready: false,
    enforce_breaches: ['market_30d_sample_size'],
    enforce_breach_count: 1,
    breaches: ['market_30d_sample_size'],
    breach_count: 1,
  },
} satisfies OpsDecisionImpactResponse;

export const alertsFeedFixture = {
  as_of: '2026-03-05T13:37:00.000Z',
  alerts: [
    {
      id: 'alert-regime-1',
      event_type: 'regime_change',
      severity: 'warning',
      title: 'Regime strengthened into risk-on',
      body: 'Breadth and credit aligned on the latest refresh.',
      entity_type: 'market',
      entity_id: null,
      created_at: '2026-03-05T13:37:00.000Z',
    },
    {
      id: 'alert-freshness-1',
      event_type: 'freshness_warning',
      severity: 'critical',
      title: 'Critical macro input is stale',
      body: 'One critical macro indicator missed its freshness SLA.',
      entity_type: 'indicator',
      entity_id: 'fed-balance-sheet',
      created_at: '2026-03-05T13:38:00.000Z',
    },
  ],
  degraded_reason: null,
} satisfies AlertsFeedResponse;

export const alertsApiFixture = {
  alerts: [
    {
      id: 1001,
      date: '2026-03-05',
      alert_type: 'REGIME_SHIFT',
      message: 'PXI crossed into a constructive regime.',
      severity: 'warning',
      acknowledged: false,
      pxi_score: 67.4,
      forward_return_7d: 1.8,
      forward_return_30d: 4.2,
    },
  ],
  count: 1,
  filters: {
    types: [
      {
        type: 'REGIME_SHIFT',
        count: 1,
      },
    ],
  },
  accuracy: {
    REGIME_SHIFT: {
      total: 12,
      accuracy_7d: 0.58,
      avg_return_7d: 1.2,
    },
  },
} satisfies AlertsApiResponse;

export const categoryDetailFixture = {
  category: 'macro',
  date: '2026-03-05',
  score: 71.2,
  weight: 0.35,
  percentile_rank: 0.82,
  indicators: [
    {
      id: 'fed-balance-sheet',
      name: 'Fed Balance Sheet',
      raw_value: 7.2,
      normalized_value: 0.61,
    },
    {
      id: 'yield-curve-3m10y',
      name: 'Yield Curve 3m10y',
      raw_value: 0.42,
      normalized_value: 0.54,
    },
  ],
  history: [
    { date: '2026-03-03', score: 68.1 },
    { date: '2026-03-04', score: 69.7 },
    { date: '2026-03-05', score: 71.2 },
  ],
} satisfies CategoryDetailData;

export const calibrationDiagnosticsFixture = {
  as_of: '2026-03-05',
  metric: 'conviction',
  horizon: '7d',
  basis: 'conviction_decile',
  total_samples: 83,
  bins: [
    {
      bin: '0.7-0.8',
      correct_count: 24,
      probability_correct: 0.67,
      ci95_low: 0.58,
      ci95_high: 0.75,
      sample_size: 36,
      quality: 'ROBUST',
    },
  ],
  diagnostics: {
    brier_score: 0.19,
    ece: 0.04,
    log_loss: 0.57,
    quality_band: 'ROBUST',
    minimum_reliable_sample: 30,
    insufficient_reasons: [],
  },
} satisfies CalibrationDiagnosticsResponse;

export const edgeDiagnosticsFixture = {
  as_of: '2026-03-05',
  basis: 'edge_quality_v2',
  windows: [
    {
      horizon: '7d',
      as_of: '2026-03-05',
      sample_size: 83,
      model_direction_accuracy: 0.64,
      baseline_direction_accuracy: 0.55,
      uplift_vs_baseline: 0.09,
      uplift_ci95_low: 0.02,
      uplift_ci95_high: 0.16,
      lower_bound_positive: true,
      minimum_reliable_sample: 30,
      quality_band: 'ROBUST',
      baseline_strategy: 'lagged_actual_direction',
      leakage_sentinel: {
        pass: true,
        violation_count: 0,
        reasons: [],
      },
      calibration_diagnostics: calibrationDiagnosticsFixture.diagnostics,
    },
  ],
  promotion_gate: {
    pass: true,
    reasons: [],
  },
} satisfies EdgeDiagnosticsResponse;

export const mlAccuracyFixture = {
  as_of: '2026-03-05',
  message: 'Coverage is sufficient for the rolling 7d ensemble view.',
  coverage: {
    total_predictions: 142,
    evaluated_count: 96,
    pending_count: 46,
  },
  coverage_quality: 'ROBUST',
  minimum_reliable_sample: 30,
  unavailable_reasons: [],
  total_predictions: 142,
  evaluated_count: 96,
  pending_count: 46,
  recent_predictions: [
    {
      date: '2026-03-05',
      model: 'ensemble',
      target: 'd7',
      predicted_direction: 'UP',
      realized_direction: 'UP',
    },
  ],
  metrics: {
    xgboost: {
      d7: {
        direction_accuracy: '0.61',
        mean_absolute_error: '2.14',
        sample_size: 96,
      },
      d30: {
        direction_accuracy: '0.57',
        mean_absolute_error: '4.82',
        sample_size: 73,
      },
    },
    lstm: {
      d7: {
        direction_accuracy: '0.58',
        mean_absolute_error: '2.36',
        sample_size: 96,
      },
      d30: null,
    },
    ensemble: {
      d7: {
        direction_accuracy: '0.64',
        mean_absolute_error: '1.98',
        sample_size: 96,
      },
      d30: {
        direction_accuracy: '0.59',
        mean_absolute_error: '4.44',
        sample_size: 73,
      },
    },
  },
} satisfies MLAccuracyApiResponse;

export const contractFixtures = {
  alertsApi: alertsApiFixture,
  alertsFeed: alertsFeedFixture,
  brief: briefFixture,
  calibrationDiagnostics: calibrationDiagnosticsFixture,
  categoryDetail: categoryDetailFixture,
  decisionImpact: decisionImpactFixture,
  edgeDiagnostics: edgeDiagnosticsFixture,
  mlAccuracy: mlAccuracyFixture,
  opportunities: opportunitiesFixture,
  opsDecisionImpact: opsDecisionImpactFixture,
  plan: planFixture,
  pxi: pxiFixture,
  signal: signalFixture,
} as const;
