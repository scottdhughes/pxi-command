import type { EmailMessage } from 'cloudflare:email';
import type {
  AlertsApiResponse as SharedAlertsApiResponse,
  AlertsFeedResponse as SharedAlertsFeedResponse,
  BriefData as SharedBriefData,
  CalibrationDiagnosticsResponse as SharedCalibrationDiagnosticsResponse,
  CategoryDetailData as SharedCategoryDetailData,
  DecisionImpactCoverage as SharedDecisionImpactCoverage,
  DecisionImpactMarketStats as SharedDecisionImpactMarketStats,
  DecisionImpactObserveMode as SharedDecisionImpactObserveMode,
  DecisionImpactResponse as SharedDecisionImpactResponse,
  DecisionImpactThemeStats as SharedDecisionImpactThemeStats,
  EdgeDiagnosticsResponse as SharedEdgeDiagnosticsResponse,
  MLAccuracyApiResponse as SharedMLAccuracyApiResponse,
  OpportunitiesResponse as SharedOpportunitiesResponse,
  OpsDecisionImpactResponse as SharedOpsDecisionImpactResponse,
  PlanData as SharedPlanData,
  PXIData as SharedPXIData,
  SignalData as SharedSignalData,
} from '../src/types/market-contracts';

export interface Env {
  DB: D1Database;
  VECTORIZE: VectorizeIndex;
  AI: Ai;
  ML_MODELS: KVNamespace;
  RATE_LIMIT_KV?: KVNamespace;
  BUILD_SHA?: string;
  BUILD_TIMESTAMP?: string;
  WORKER_VERSION?: string;
  DEPLOY_ENV?: string;
  FRED_API_KEY?: string;
  WRITE_API_KEY?: string;
  EMAIL_OUTBOUND?: {
    send(message: EmailMessage): Promise<void>;
  };
  ALERTS_FROM_EMAIL?: string;
  ALERTS_SIGNING_SECRET?: string;
  FEATURE_ENABLE_BRIEF?: string;
  FEATURE_ENABLE_OPPORTUNITIES?: string;
  FEATURE_ENABLE_PLAN?: string;
  FEATURE_ENABLE_ALERTS_EMAIL?: string;
  FEATURE_ENABLE_ALERTS_IN_APP?: string;
  FEATURE_ENABLE_OPPORTUNITY_COHERENCE_GATE?: string;
  FEATURE_ENABLE_CALIBRATION_DIAGNOSTICS?: string;
  FEATURE_ENABLE_EDGE_DIAGNOSTICS?: string;
  FEATURE_ENABLE_SIGNALS_SANITIZER?: string;
  FEATURE_ENABLE_DECISION_IMPACT?: string;
  FEATURE_ENABLE_DECISION_IMPACT_ENFORCE?: string;
  FEATURE_ENABLE_CTA_INTENT_TRACKING?: string;
  ENABLE_BRIEF?: string;
  ENABLE_OPPORTUNITIES?: string;
  ENABLE_PLAN?: string;
  ENABLE_ALERTS_EMAIL?: string;
  ENABLE_ALERTS_IN_APP?: string;
  ENABLE_OPPORTUNITY_COHERENCE_GATE?: string;
  ENABLE_CALIBRATION_DIAGNOSTICS?: string;
  ENABLE_EDGE_DIAGNOSTICS?: string;
  ENABLE_SIGNALS_SANITIZER?: string;
  ENABLE_DECISION_IMPACT?: string;
  ENABLE_DECISION_IMPACT_ENFORCE?: string;
  ENABLE_CTA_INTENT_TRACKING?: string;
  DECISION_IMPACT_ENFORCE_MIN_SAMPLE?: string;
  DECISION_IMPACT_ENFORCE_MIN_ACTIONABLE_SESSIONS?: string;
}

export interface WorkerRouteContext {
  request: Request;
  env: Env;
  url: URL;
  method: string;
  corsHeaders: Record<string, string>;
  clientIP: string;
  executionContext?: ExecutionContext;
}

export interface WorkerHealthResponsePayload {
  status: 'healthy';
  db: boolean;
  timestamp: string;
  environment: string;
  build_sha: string;
  build_timestamp: string;
  worker_version: string;
}

export interface PXIRow {
  date: string;
  score: number;
  label: string;
  status: string;
  delta_1d: number | null;
  delta_7d: number | null;
  delta_30d: number | null;
}

export interface CategoryRow {
  category: string;
  score: number;
  weight: number;
}

export interface SparklineRow {
  date: string;
  score: number;
}

export interface IndicatorRow {
  indicator_id: string;
  value: number;
}

export type RegimeDelta = 'UNCHANGED' | 'SHIFTED' | 'STRENGTHENED' | 'WEAKENED';
export type RiskPosture = 'risk_on' | 'neutral' | 'risk_off';
export type PolicyStance = 'RISK_ON' | 'RISK_OFF' | 'MIXED';
export type ConsistencyState = 'PASS' | 'WARN' | 'FAIL';
export type OpportunityDirection = 'bullish' | 'bearish' | 'neutral';
export type MarketAlertType = 'regime_change' | 'threshold_cross' | 'opportunity_spike' | 'freshness_warning';
export type AlertSeverity = 'info' | 'warning' | 'critical';
export type ConflictState = 'ALIGNED' | 'MIXED' | 'CONFLICT';
export type EdgeQualityLabel = 'HIGH' | 'MEDIUM' | 'LOW';
export type CalibrationQuality = 'ROBUST' | 'LIMITED' | 'INSUFFICIENT';
export type CoverageQuality = CalibrationQuality;
export type PlanActionabilityState = 'ACTIONABLE' | 'WATCH' | 'NO_ACTION';
export type OpportunityTtlState = 'fresh' | 'stale' | 'overdue' | 'unknown';
export type UtilityEventType =
  | 'session_start'
  | 'plan_view'
  | 'opportunities_view'
  | 'decision_actionable_view'
  | 'decision_watch_view'
  | 'decision_no_action_view'
  | 'no_action_unlock_view'
  | 'cta_action_click';
export type PlanActionabilityReasonCode =
  | 'critical_data_quality_block'
  | 'consistency_fail_block'
  | 'opportunity_reference_unavailable'
  | 'no_eligible_opportunities'
  | 'high_edge_override_no_eligible'
  | 'high_edge_with_eligible_opportunities'
  | 'medium_edge_watch'
  | 'low_edge_watch'
  | 'cross_horizon_conflict_watch'
  | 'cross_horizon_insufficient_watch'
  | 'fallback_degraded_mode'
  | `opportunity_${string}`;
export type OpportunityExpectancyBasis =
  | 'theme_direction'
  | 'theme_direction_shrunk_prior'
  | 'direction_prior_proxy'
  | 'none';
export type OpportunityConfidenceBand = 'high' | 'medium' | 'low';
export type OpportunityEligibilityCheck =
  | 'neutral_direction_not_actionable'
  | 'calibration_probability_below_threshold'
  | 'expectancy_sign_conflict'
  | 'incomplete_contract';
export type OpportunityCtaDisabledReason =
  | 'no_eligible_opportunities'
  | 'suppressed_data_quality'
  | 'calibration_quality_not_robust'
  | 'calibration_ece_unavailable'
  | 'ece_above_threshold'
  | 'refresh_ttl_overdue'
  | 'refresh_ttl_unknown';
export type RegimeType = 'RISK_ON' | 'RISK_OFF' | 'TRANSITION';
export type SignalType = 'FULL_RISK' | 'REDUCED_RISK' | 'RISK_OFF' | 'DEFENSIVE';
export type EdgeDiagnosticsHorizon = '7d' | '30d';
export type DecisionImpactOutcomeBasis = 'spy_forward_proxy' | 'theme_proxy_blend';
export type DecisionGradeComponentStatus = 'pass' | 'watch' | 'fail' | 'insufficient';

export interface OpportunityEligibility {
  passed: boolean;
  failed_checks: OpportunityEligibilityCheck[];
}

export interface OpportunityDecisionContract {
  coherent: boolean;
  confidence_band: OpportunityConfidenceBand;
  rationale_codes: string[];
}

export interface EdgeQualityCalibration {
  bin: string | null;
  probability_correct_7d: number | null;
  ci95_low_7d: number | null;
  ci95_high_7d: number | null;
  sample_size_7d: number;
  quality: CalibrationQuality;
}

export interface OpportunityCalibration {
  probability_correct_direction: number | null;
  ci95_low: number | null;
  ci95_high: number | null;
  sample_size: number;
  quality: CalibrationQuality;
  basis: 'conviction_decile';
  window: string | null;
  unavailable_reason: string | null;
}

export interface OpportunityExpectancy {
  expected_move_pct: number | null;
  max_adverse_move_pct: number | null;
  sample_size: number;
  basis: OpportunityExpectancyBasis;
  quality: CoverageQuality;
  unavailable_reason: string | null;
}

export interface CalibrationBinSnapshot {
  bin: string;
  correct_count: number;
  probability_correct: number | null;
  ci95_low: number | null;
  ci95_high: number | null;
  sample_size: number;
  quality: CalibrationQuality;
}

export interface MarketCalibrationSnapshotPayload {
  as_of: string;
  metric: 'edge_quality' | 'conviction';
  horizon: '7d' | '30d' | null;
  basis: 'edge_quality_decile' | 'conviction_decile';
  bins: CalibrationBinSnapshot[];
  total_samples: number;
}

export interface CalibrationDiagnosticsSnapshot {
  brier_score: number | null;
  ece: number | null;
  log_loss: number | null;
  quality_band: CalibrationQuality;
  minimum_reliable_sample: number;
  insufficient_reasons: string[];
}

export interface EdgeLeakageSentinel {
  pass: boolean;
  violation_count: number;
  reasons: string[];
}

export interface EdgeDiagnosticsWindow {
  horizon: EdgeDiagnosticsHorizon;
  as_of: string | null;
  sample_size: number;
  model_direction_accuracy: number | null;
  baseline_direction_accuracy: number | null;
  uplift_vs_baseline: number | null;
  uplift_ci95_low: number | null;
  uplift_ci95_high: number | null;
  lower_bound_positive: boolean;
  minimum_reliable_sample: number;
  quality_band: CalibrationQuality;
  baseline_strategy: 'lagged_actual_direction';
  leakage_sentinel: EdgeLeakageSentinel;
  calibration_diagnostics: CalibrationDiagnosticsSnapshot;
}

export interface EdgeDiagnosticsReport {
  as_of: string;
  basis: string;
  windows: EdgeDiagnosticsWindow[];
  promotion_gate: {
    pass: boolean;
    reasons: string[];
  };
}

export interface FreshnessStatus {
  has_stale_data: boolean;
  stale_count: number;
  critical_stale_count: number;
}

export type BriefSnapshot = SharedBriefData;

export interface OpportunityItem {
  id: string;
  symbol: string | null;
  theme_id: string;
  theme_name: string;
  direction: OpportunityDirection;
  conviction_score: number;
  rationale: string;
  supporting_factors: string[];
  historical_hit_rate: number;
  sample_size: number;
  calibration: OpportunityCalibration;
  expectancy: OpportunityExpectancy;
  eligibility: OpportunityEligibility;
  decision_contract: OpportunityDecisionContract;
  updated_at: string;
}

export interface OpportunitySnapshot {
  as_of: string;
  horizon: '7d' | '30d';
  items: OpportunityItem[];
}

export interface OpportunitySuppressionByReason {
  coherence_failed: number;
  quality_filtered: number;
  data_quality_suppressed: number;
}

export interface OpportunityTtlMetadata {
  data_age_seconds: number | null;
  ttl_state: OpportunityTtlState;
  next_expected_refresh_at: string | null;
  overdue_seconds: number | null;
}

export interface UtilityEventInsertPayload {
  session_id: string;
  event_type: UtilityEventType;
  route: string | null;
  actionability_state: PlanActionabilityState | null;
  payload_json: string | null;
  created_at: string;
}

export interface UtilityFunnelSummary {
  window_days: number;
  days_observed: number;
  total_events: number;
  unique_sessions: number;
  plan_views: number;
  opportunities_views: number;
  decision_actionable_views: number;
  decision_watch_views: number;
  decision_no_action_views: number;
  no_action_unlock_views: number;
  cta_action_clicks: number;
  actionable_view_sessions: number;
  actionable_sessions: number;
  cta_action_rate_pct: number;
  decision_events_total: number;
  decision_events_per_session: number;
  no_action_unlock_coverage_pct: number;
  last_event_at: string | null;
}

export interface MigrationResponsePayload {
  success: true;
  tables_created: string[];
  message: string;
}

export interface WriteResponsePayload {
  success: true;
  written: number;
}

export interface RefreshIngestionResponsePayload {
  success: true;
  indicators_fetched: number;
  indicators_written: number;
  pxi: {
    date: string;
    score: number;
    label: string;
    categories: number;
  } | null;
}

export interface RecalculateResponsePayload {
  success: true;
  date: string;
  pxi: {
    date: string;
    score: number;
    label: string;
    status: string;
    delta_1d: number | null;
    delta_7d: number | null;
    delta_30d: number | null;
  };
  categories: number;
  embedded: boolean;
}

export interface BackfillRunResultItem {
  date: string;
  error?: string;
}

export interface BackfillResponsePayload {
  success: true;
  run_id: number | null;
  start: string;
  end: string;
  requested_limit: number;
  refresh_products: boolean;
  include_decision_impact: boolean;
  include_decision_grade: boolean;
  succeeded: number;
  embedded: number;
  results: BackfillRunResultItem[];
}

export interface RecalculateAllSignalsResponsePayload {
  success: true;
  processed: number;
  total: number;
  message: string;
}

export interface UtilityEventAcceptedPayload {
  event_type: UtilityEventType;
  route: string | null;
  actionability_state: PlanActionabilityState | null;
}

export interface UtilityEventResponsePayload {
  ok: true;
  stored?: boolean;
  ignored_reason?: 'cta_intent_tracking_disabled';
  accepted: UtilityEventAcceptedPayload;
}

export interface UtilityFunnelResponsePayload {
  as_of: string;
  funnel: UtilityFunnelSummary;
}

export interface MarketDecisionImpactSummary {
  market_7d_hit_rate: number;
  market_7d_sample_size: number;
  market_30d_hit_rate: number;
  market_30d_sample_size: number;
  actionable_sessions: number;
  cta_action_rate_pct: number;
  governance_mode: DecisionImpactObserveSnapshot['mode'];
  enforce_ready: boolean;
  enforce_breach_count: number;
  enforce_breaches: string[];
  observe_breach_count: number;
  observe_breaches: string[];
  minimum_samples_required?: number;
  minimum_actionable_sessions_required?: number;
}

export interface MarketCalibrationDiagnosticsSummary {
  edge_quality: CalibrationQuality;
  conviction_7d: CalibrationQuality;
  conviction_30d: CalibrationQuality;
}

export interface MarketDecisionGradeSnapshot {
  score: number;
  grade: DecisionGradeResponse['grade'];
  go_live_ready: boolean;
  go_live_blockers: string[];
  readiness: DecisionGradeResponse['readiness'];
  opportunity_hygiene: {
    over_suppression_rate_pct: number;
    cross_horizon_conflict_rate_pct: number;
    conflict_persistence_days: number;
  };
}

export interface MarketEdgeDiagnosticsWindowSummary {
  horizon: '7d' | '30d';
  sample_size: number;
  model_direction_accuracy: number | null;
  baseline_direction_accuracy: number | null;
  uplift_vs_baseline: number | null;
  uplift_ci95_low: number | null;
  uplift_ci95_high: number | null;
  lower_bound_positive: boolean;
  leakage_sentinel: {
    pass: boolean;
    violation_count: number;
    reasons: string[];
  };
  quality_band: CalibrationQuality;
}

export interface MarketEdgeDiagnosticsSummary {
  as_of: string;
  promotion_gate: {
    pass: boolean;
    reasons: string[];
  };
  windows: MarketEdgeDiagnosticsWindowSummary[];
}

export interface RefreshProductsCompletedResponsePayload {
  ok: true;
  brief_generated: number;
  opportunities_generated: number;
  calibrations_generated: number;
  alerts_generated: number;
  consistency_stored: number;
  consistency_state: ConsistencyState | 'INSUFFICIENT';
  consistency_score: number | null;
  as_of: string | null;
  stale_count: number;
  critical_stale_count: number;
  quality_filtered_count: number;
  coherence_suppressed_count: number;
  suppressed_data_quality_count: number;
  over_suppressed_count: number;
  cross_horizon_state: 'ALIGNED' | 'MIXED' | 'CONFLICT' | 'INSUFFICIENT';
  opportunity_ledger_rows: number;
  opportunity_item_ledger_rows: number;
  decision_impact_snapshots_generated: number;
  decision_impact: MarketDecisionImpactSummary | null;
  decision_impact_error: string | null;
  calibration_diagnostics: MarketCalibrationDiagnosticsSummary | null;
  decision_grade_snapshot: MarketDecisionGradeSnapshot | null;
  edge_diagnostics: MarketEdgeDiagnosticsSummary | null;
  refresh_trigger: string;
  refresh_run_id: number | null;
}

export interface RefreshProductsSkippedResponsePayload {
  ok: true;
  skipped: true;
  reason: 'refresh_in_progress';
  refresh_run_id: number | null;
  refresh_trigger: string;
}

export type RefreshProductsResponsePayload =
  | RefreshProductsCompletedResponsePayload
  | RefreshProductsSkippedResponsePayload;

export interface BackfillProductsResponsePayload {
  ok: true;
  dry_run: boolean;
  requested: {
    start: string;
    end: string;
    limit: number;
    overwrite: boolean;
    recalibrate: boolean;
    rebuild_ledgers: boolean;
  };
  scanned_dates: number;
  processed_dates: number;
  skipped_dates: number;
  seeded_snapshots: number;
  calibrations_generated: number;
  opportunity_ledger_rows_generated: number;
  opportunity_item_ledger_rows_generated: number;
  decision_impact_snapshots_generated: number;
  decision_impact: MarketDecisionImpactSummary | null;
  calibration_samples: {
    edge_total_samples: number | null;
    conviction_7d_total_samples: number | null;
    conviction_30d_total_samples: number | null;
  };
  skipped_existing_dates: string[];
  failed_dates: Array<{
    date: string;
    error: string;
  }>;
}

export interface SendDigestDeliveredResponsePayload {
  ok: true;
  sent_count: number;
  fail_count: number;
  bounce_count: number;
  active_subscribers: number;
}

export interface SendDigestSkippedResponsePayload {
  ok: true;
  skipped: true;
  reason: string;
}

export type SendDigestResponsePayload =
  | SendDigestDeliveredResponsePayload
  | SendDigestSkippedResponsePayload;

export type DecisionImpactMarketStats = SharedDecisionImpactMarketStats;
export type DecisionImpactThemeStats = SharedDecisionImpactThemeStats;
export type DecisionImpactCoverage = SharedDecisionImpactCoverage;
export type DecisionImpactResponsePayload = SharedDecisionImpactResponse;
export type DecisionImpactObserveSnapshot = SharedDecisionImpactObserveMode;
export type DecisionImpactOpsResponsePayload = SharedOpsDecisionImpactResponse;

export interface OpportunityItemLedgerInsertPayload {
  refresh_run_id: number | null;
  as_of: string;
  horizon: '7d' | '30d';
  opportunity_id: string;
  theme_id: string;
  theme_name: string;
  direction: OpportunityDirection;
  conviction_score: number;
  published: 0 | 1;
  suppression_reason: 'coherence_failed' | 'quality_filtered' | 'suppressed_data_quality' | null;
}

export interface OpportunityLedgerInsertPayload {
  refresh_run_id: number | null;
  as_of: string;
  horizon: '7d' | '30d';
  candidate_count: number;
  published_count: number;
  suppressed_count: number;
  quality_filtered_count: number;
  coherence_suppressed_count: number;
  data_quality_suppressed_count: number;
  degraded_reason: string | null;
  top_direction_candidate: OpportunityDirection | null;
  top_direction_published: OpportunityDirection | null;
}

export interface OpportunityLedgerRow extends OpportunityLedgerInsertPayload {
  created_at: string;
}

export interface OpportunityLedgerWindowMetrics {
  window_days: number;
  rows_observed: number;
  candidate_count_total: number;
  published_count_total: number;
  publish_rate_pct: number;
  suppressed_count_total: number;
  over_suppressed_rows: number;
  over_suppression_rate_pct: number;
  paired_days: number;
  cross_horizon_conflict_days: number;
  cross_horizon_conflict_rate_pct: number;
  conflict_persistence_days: number;
  last_as_of: string | null;
}

export interface DecisionGradeResponse {
  as_of: string;
  window_days: number;
  score: number;
  grade: 'GREEN' | 'YELLOW' | 'RED';
  go_live_ready: boolean;
  go_live_blockers: string[];
  readiness: {
    decision_impact_window_days: 30 | 90;
    decision_impact_enforce_ready: boolean;
    decision_impact_breaches: string[];
    decision_impact_market_7d_sample_size: number;
    decision_impact_market_30d_sample_size: number;
    decision_impact_actionable_sessions: number;
    minimum_samples_required: number;
    minimum_actionable_sessions_required: number;
  };
  components: {
    freshness: {
      score: number;
      status: DecisionGradeComponentStatus;
      slo_attainment_pct: number;
      days_with_critical_stale: number;
      days_observed: number;
    };
    consistency: {
      score: number;
      status: DecisionGradeComponentStatus;
      pass_count: number;
      warn_count: number;
      fail_count: number;
      total: number;
    };
    calibration: {
      score: number;
      status: DecisionGradeComponentStatus;
      conviction_7d: CalibrationQuality;
      conviction_30d: CalibrationQuality;
      edge_quality: CalibrationQuality;
    };
    edge: {
      score: number;
      status: DecisionGradeComponentStatus;
      promotion_gate_pass: boolean;
      lower_bound_positive_horizons: number;
      horizons_observed: number;
      reasons: string[];
    };
    opportunity_hygiene: {
      score: number;
      status: DecisionGradeComponentStatus;
      publish_rate_pct: number;
      over_suppression_rate_pct: number;
      cross_horizon_conflict_rate_pct: number;
      conflict_persistence_days: number;
      rows_observed: number;
    };
    utility: {
      score: number;
      status: DecisionGradeComponentStatus;
      decision_events_total: number;
      no_action_unlock_coverage_pct: number;
      unique_sessions: number;
    };
  };
}

export interface MarketAlertEvent {
  id: string;
  event_type: MarketAlertType;
  severity: AlertSeverity;
  title: string;
  body: string;
  entity_type: 'market' | 'theme' | 'indicator';
  entity_id: string | null;
  dedupe_key: string;
  payload_json: string;
  created_at: string;
}

export interface SignalsThemeRecord {
  theme_id: string;
  theme_name: string;
  score: number;
  key_tickers: string[];
  classification?: {
    signal_type?: string;
    confidence?: string;
    timing?: string;
  };
}

export interface EdgeQualitySnapshot {
  score: number;
  label: EdgeQualityLabel;
  breakdown: {
    data_quality: number;
    model_agreement: number;
    regime_stability: number;
  };
  stale_count: number;
  ml_sample_size: number;
  conflict_state: ConflictState;
  calibration: EdgeQualityCalibration;
}

export interface PolicyStateSnapshot {
  stance: PolicyStance;
  risk_posture: RiskPosture;
  conflict_state: ConflictState;
  base_signal: SignalType;
  regime_context: RegimeType;
  rationale: string;
  rationale_codes: string[];
}

export interface UncertaintySnapshot {
  headline: string | null;
  flags: {
    stale_inputs: boolean;
    limited_calibration: boolean;
    limited_scenario_sample: boolean;
  };
}

export interface ConsistencySnapshot {
  score: number;
  state: ConsistencyState;
  violations: string[];
  components: {
    base_score: number;
    structural_penalty: number;
    reliability_penalty: number;
  };
}

export interface TraderPlaybookSnapshot {
  recommended_size_pct: {
    min: number;
    target: number;
    max: number;
  };
  scenarios: Array<{
    condition: string;
    action: string;
    invalidation: string;
  }>;
  benchmark_follow_through_7d: {
    hit_rate: number | null;
    sample_size: number;
    unavailable_reason: string | null;
  };
}

export interface PlanRiskBand {
  bear: number | null;
  base: number | null;
  bull: number | null;
  sample_size: number;
}

export interface RiskSizingSnapshot {
  raw_signal_allocation_target: number;
  target_pct: number;
  min_pct: number;
  max_pct: number;
}

export type PlanPayload = SharedPlanData;
export type PXIResponsePayload = SharedPXIData;
export type SignalResponsePayload = SharedSignalData;
export type OpportunityFeedResponsePayload = SharedOpportunitiesResponse;
export type AlertsFeedResponsePayload = SharedAlertsFeedResponse;
export type AlertsApiResponsePayload = SharedAlertsApiResponse;
export type CategoryDetailResponsePayload = SharedCategoryDetailData;
export type CalibrationDiagnosticsResponsePayload = SharedCalibrationDiagnosticsResponse;
export type EdgeDiagnosticsResponsePayload = SharedEdgeDiagnosticsResponse;
export type MLAccuracyApiResponsePayload = SharedMLAccuracyApiResponse;

export interface RegimeSignal {
  indicator: string;
  value: number | null;
  percentile: number | null;
  threshold_low_pct: number;
  threshold_high_pct: number;
  signal: 'RISK_ON' | 'RISK_OFF' | 'NEUTRAL';
  description: string;
}

export interface RegimeResult {
  regime: RegimeType;
  confidence: number;
  signals: RegimeSignal[];
  description: string;
  date: string;
}

export interface DivergenceAlert {
  type: 'PXI_REGIME' | 'PXI_MOMENTUM' | 'REGIME_SHIFT';
  severity: 'LOW' | 'MEDIUM' | 'HIGH';
  title: string;
  description: string;
  actionable: boolean;
  metrics?: {
    historical_frequency: number;
    median_return_7d: number | null;
    median_return_30d: number | null;
    false_positive_rate: number | null;
  };
}

export interface DivergenceResult {
  has_divergence: boolean;
  alerts: DivergenceAlert[];
}

export interface PXISignal {
  pxi_level: number;
  delta_pxi_7d: number | null;
  delta_pxi_30d: number | null;
  category_dispersion: number;
  regime: RegimeType;
  volatility_percentile: number | null;
  risk_allocation: number;
  signal_type: SignalType;
  adjustments: string[];
}

export interface OpportunityFeedProjection {
  items: OpportunityItem[];
  suppressed_count: number;
  degraded_reason: string | null;
  quality_filtered_count: number;
  coherence_suppressed_count: number;
  suppressed_data_quality: boolean;
  suppression_by_reason: OpportunitySuppressionByReason;
  total_candidates: number;
  quality_filter_rate: number;
  coherence_fail_rate: number;
}

export interface DecisionImpactGovernanceOptions {
  enforce_enabled: boolean;
  min_sample_size: number;
  min_actionable_sessions: number;
}

export interface CanonicalMarketDecision {
  as_of: string;
  pxi: PXIRow;
  categories: CategoryRow[];
  signal: PXISignal;
  risk_sizing: RiskSizingSnapshot;
  regime: RegimeResult | null;
  freshness: FreshnessStatus;
  risk_band: { d7: PlanRiskBand; d30: PlanRiskBand };
  edge_quality: EdgeQualitySnapshot;
  policy_state: PolicyStateSnapshot;
  degraded_reasons: string[];
  uncertainty: UncertaintySnapshot;
  consistency: ConsistencySnapshot;
  trader_playbook: TraderPlaybookSnapshot;
}
