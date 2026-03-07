import type {
  BackfillProductsResponsePayload,
  BackfillResponsePayload,
  MigrationResponsePayload,
  RecalculateAllSignalsResponsePayload,
  RecalculateResponsePayload,
  RefreshIngestionResponsePayload,
  RefreshProductsCompletedResponsePayload,
  RefreshProductsResponsePayload,
  SendDigestResponsePayload,
  UtilityEventResponsePayload,
  UtilityFunnelResponsePayload,
  WriteResponsePayload,
} from '../worker/types';

export const migrationFixture = {
  success: true,
  tables_created: ['prediction_log', 'market_refresh_runs', 'market_utility_events'],
  message: 'Migration complete. Created/verified: prediction_log, market_refresh_runs, market_utility_events',
} satisfies MigrationResponsePayload;

export const writeFixture = {
  success: true,
  written: 42,
} satisfies WriteResponsePayload;

export const refreshIngestionFixture = {
  success: true,
  indicators_fetched: 128,
  indicators_written: 128,
  pxi: {
    date: '2026-03-05',
    score: 67.4,
    label: 'Constructive',
    categories: 8,
  },
} satisfies RefreshIngestionResponsePayload;

export const recalculateFixture = {
  success: true,
  date: '2026-03-05',
  pxi: {
    date: '2026-03-05',
    score: 67.4,
    label: 'Constructive',
    status: 'GREEN',
    delta_1d: 1.2,
    delta_7d: 4.1,
    delta_30d: -2.6,
  },
  categories: 8,
  embedded: true,
} satisfies RecalculateResponsePayload;

export const backfillFixture = {
  success: true,
  run_id: 91,
  start: '2026-03-01',
  end: '2026-03-05',
  requested_limit: 5,
  refresh_products: true,
  include_decision_impact: true,
  include_decision_grade: true,
  succeeded: 4,
  embedded: 3,
  results: [
    { date: '2026-03-01' },
    { date: '2026-03-02', error: 'Insufficient data for calculation' },
  ],
} satisfies BackfillResponsePayload;

export const recalculateAllSignalsFixture = {
  success: true,
  processed: 128,
  total: 128,
  message: 'Generated signal data for 128 dates',
} satisfies RecalculateAllSignalsResponsePayload;

export const utilityEventFixture = {
  ok: true,
  accepted: {
    event_type: 'plan_view',
    route: '/',
    actionability_state: 'ACTIONABLE',
  },
} satisfies UtilityEventResponsePayload;

export const ignoredUtilityEventFixture = {
  ok: true,
  stored: false,
  ignored_reason: 'cta_intent_tracking_disabled',
  accepted: {
    event_type: 'cta_action_click',
    route: '/',
    actionability_state: 'ACTIONABLE',
  },
} satisfies UtilityEventResponsePayload;

export const utilityFunnelFixture = {
  as_of: '2026-03-05T14:00:00.000Z',
  funnel: {
    window_days: 7,
    days_observed: 5,
    total_events: 48,
    unique_sessions: 17,
    plan_views: 12,
    opportunities_views: 9,
    decision_actionable_views: 11,
    decision_watch_views: 6,
    decision_no_action_views: 5,
    no_action_unlock_views: 3,
    cta_action_clicks: 4,
    actionable_view_sessions: 8,
    actionable_sessions: 9,
    cta_action_rate_pct: 44.44,
    decision_events_total: 22,
    decision_events_per_session: 1.2941,
    no_action_unlock_coverage_pct: 60,
    last_event_at: '2026-03-05T13:58:00.000Z',
  },
} satisfies UtilityFunnelResponsePayload;

export const refreshProductsFixture = {
  ok: true,
  publication_status: 'published',
  brief_generated: 1,
  opportunities_generated: 2,
  calibrations_generated: 3,
  alerts_generated: 2,
  consistency_stored: 1,
  consistency_state: 'PASS',
  consistency_score: 83,
  as_of: '2026-03-05T00:00:00.000Z',
  stale_count: 2,
  critical_stale_count: 1,
  quality_filtered_count: 1,
  coherence_suppressed_count: 1,
  suppressed_data_quality_count: 0,
  over_suppressed_count: 0,
  cross_horizon_state: 'ALIGNED',
  opportunity_ledger_rows: 2,
  opportunity_item_ledger_rows: 5,
  decision_impact_snapshots_generated: 8,
  decision_impact: {
    market_7d_hit_rate: 0.64,
    market_7d_sample_size: 28,
    market_30d_hit_rate: 0.58,
    market_30d_sample_size: 19,
    actionable_sessions: 88,
    cta_action_rate_pct: 26.1,
    governance_mode: 'observe',
    enforce_ready: false,
    enforce_breach_count: 1,
    enforce_breaches: ['market_30d_sample_size'],
    minimum_samples_required: 20,
    minimum_actionable_sessions_required: 30,
    observe_breach_count: 1,
    observe_breaches: ['market_30d_sample_size'],
  },
  decision_impact_error: null,
  calibration_diagnostics: {
    edge_quality: 'ROBUST',
    conviction_7d: 'ROBUST',
    conviction_30d: 'LIMITED',
  },
  decision_grade_snapshot: {
    score: 91.4,
    grade: 'GREEN',
    go_live_ready: false,
    go_live_blockers: ['market_30d_sample_size'],
    readiness: {
      decision_impact_window_days: 30,
      decision_impact_enforce_ready: false,
      decision_impact_breaches: ['market_30d_sample_size'],
      decision_impact_market_7d_sample_size: 28,
      decision_impact_market_30d_sample_size: 19,
      decision_impact_actionable_sessions: 88,
      minimum_samples_required: 20,
      minimum_actionable_sessions_required: 30,
    },
    opportunity_hygiene: {
      over_suppression_rate_pct: 0,
      cross_horizon_conflict_rate_pct: 0,
      conflict_persistence_days: 0,
    },
  },
  edge_diagnostics: {
    as_of: '2026-03-05',
    promotion_gate: {
      pass: true,
      reasons: [],
    },
    windows: [
      {
        horizon: '7d',
        sample_size: 83,
        model_direction_accuracy: 0.64,
        baseline_direction_accuracy: 0.55,
        uplift_vs_baseline: 0.09,
        uplift_ci95_low: 0.02,
        uplift_ci95_high: 0.16,
        lower_bound_positive: true,
        leakage_sentinel: {
          pass: true,
          violation_count: 0,
          reasons: [],
        },
        quality_band: 'ROBUST',
      },
    ],
  },
  refresh_trigger: 'github_actions',
  refresh_run_id: 101,
} satisfies RefreshProductsCompletedResponsePayload;

export const refreshProductsBlockedFixture = {
  ok: true,
  blocked: true,
  reason: 'decision_impact_enforcement_failed',
  publication_status: 'blocked',
  governance_breaches: [
    'market_30d_hit_rate_breach',
    'market_30d_avg_signed_return_breach',
  ],
  brief_generated: 0,
  opportunities_generated: 0,
  calibrations_generated: 3,
  alerts_generated: 0,
  consistency_stored: 1,
  consistency_state: 'PASS',
  consistency_score: 83,
  as_of: '2026-03-05T00:00:00.000Z',
  stale_count: 2,
  critical_stale_count: 1,
  quality_filtered_count: 1,
  coherence_suppressed_count: 1,
  suppressed_data_quality_count: 0,
  over_suppressed_count: 0,
  cross_horizon_state: 'INSUFFICIENT',
  opportunity_ledger_rows: 2,
  opportunity_item_ledger_rows: 5,
  decision_impact_snapshots_generated: 8,
  decision_impact: {
    market_7d_hit_rate: 0.44,
    market_7d_sample_size: 31,
    market_30d_hit_rate: 0.29,
    market_30d_sample_size: 33,
    actionable_sessions: 88,
    cta_action_rate_pct: 26.1,
    governance_mode: 'enforce',
    enforce_ready: true,
    enforce_breach_count: 2,
    enforce_breaches: ['market_30d_hit_rate_breach', 'market_30d_avg_signed_return_breach'],
    minimum_samples_required: 20,
    minimum_actionable_sessions_required: 30,
    observe_breach_count: 2,
    observe_breaches: ['market_30d_hit_rate_breach', 'market_30d_avg_signed_return_breach'],
  },
  decision_impact_error: null,
  calibration_diagnostics: {
    edge_quality: 'ROBUST',
    conviction_7d: 'ROBUST',
    conviction_30d: 'LIMITED',
  },
  decision_grade_snapshot: {
    score: 78.5,
    grade: 'YELLOW',
    go_live_ready: false,
    go_live_blockers: ['decision_impact_market_30d_hit_rate_breach'],
    readiness: {
      decision_impact_window_days: 30,
      decision_impact_enforce_ready: true,
      decision_impact_breaches: ['market_30d_hit_rate_breach', 'market_30d_avg_signed_return_breach'],
      decision_impact_market_7d_sample_size: 31,
      decision_impact_market_30d_sample_size: 33,
      decision_impact_actionable_sessions: 88,
      minimum_samples_required: 20,
      minimum_actionable_sessions_required: 30,
    },
    opportunity_hygiene: {
      over_suppression_rate_pct: 100,
      cross_horizon_conflict_rate_pct: 0,
      conflict_persistence_days: 0,
    },
  },
  edge_diagnostics: {
    as_of: '2026-03-05',
    promotion_gate: {
      pass: true,
      reasons: [],
    },
    windows: [
      {
        horizon: '7d',
        sample_size: 83,
        model_direction_accuracy: 0.64,
        baseline_direction_accuracy: 0.55,
        uplift_vs_baseline: 0.09,
        uplift_ci95_low: 0.02,
        uplift_ci95_high: 0.16,
        lower_bound_positive: true,
        leakage_sentinel: {
          pass: true,
          violation_count: 0,
          reasons: [],
        },
        quality_band: 'ROBUST',
      },
    ],
  },
  refresh_trigger: 'github_actions',
  refresh_run_id: 101,
} satisfies RefreshProductsResponsePayload;

export const refreshProductsSkippedFixture = {
  ok: true,
  skipped: true,
  publication_status: 'skipped',
  reason: 'refresh_in_progress',
  refresh_run_id: 101,
  refresh_trigger: 'github_actions',
} satisfies RefreshProductsResponsePayload;

export const backfillProductsFixture = {
  ok: true,
  dry_run: false,
  requested: {
    start: '2025-01-01',
    end: '2025-01-31',
    limit: 31,
    overwrite: false,
    recalibrate: true,
    rebuild_ledgers: true,
  },
  scanned_dates: 21,
  processed_dates: 17,
  skipped_dates: 4,
  seeded_snapshots: 34,
  calibrations_generated: 3,
  opportunity_ledger_rows_generated: 17,
  opportunity_item_ledger_rows_generated: 49,
  decision_impact_snapshots_generated: 8,
  decision_impact: {
    market_7d_hit_rate: 0.61,
    market_7d_sample_size: 49,
    market_30d_hit_rate: 0.56,
    market_30d_sample_size: 37,
    actionable_sessions: 88,
    cta_action_rate_pct: 26.1,
    governance_mode: 'observe',
    enforce_ready: false,
    enforce_breach_count: 1,
    enforce_breaches: ['market_30d_sample_size'],
    observe_breach_count: 1,
    observe_breaches: ['market_30d_sample_size'],
  },
  calibration_samples: {
    edge_total_samples: 83,
    conviction_7d_total_samples: 71,
    conviction_30d_total_samples: 44,
  },
  skipped_existing_dates: ['2025-01-09', '2025-01-10'],
  failed_dates: [
    { date: '2025-01-16', error: 'ledger:malformed_snapshot' },
  ],
} satisfies BackfillProductsResponsePayload;

export const sendDigestFixture = {
  ok: true,
  sent_count: 12,
  fail_count: 1,
  bounce_count: 0,
  active_subscribers: 13,
} satisfies SendDigestResponsePayload;

export const skippedDigestFixture = {
  ok: true,
  skipped: true,
  reason: 'Email alerts disabled',
} satisfies SendDigestResponsePayload;

export const workerOperationalFixtures = {
  backfill: backfillFixture,
  backfillProducts: backfillProductsFixture,
  ignoredUtilityEvent: ignoredUtilityEventFixture,
  migration: migrationFixture,
  recalculate: recalculateFixture,
  recalculateAllSignals: recalculateAllSignalsFixture,
  refreshIngestion: refreshIngestionFixture,
  refreshProducts: refreshProductsFixture,
  refreshProductsBlocked: refreshProductsBlockedFixture,
  refreshProductsSkipped: refreshProductsSkippedFixture,
  sendDigest: sendDigestFixture,
  skippedDigest: skippedDigestFixture,
  utilityEvent: utilityEventFixture,
  utilityFunnel: utilityFunnelFixture,
  write: writeFixture,
} as const;
