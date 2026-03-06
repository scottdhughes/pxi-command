export type {
  AlertData,
  AlertsApiResponse,
  AlertsFeedResponse,
  BriefData,
  CalibrationDiagnosticsResponse,
  CategoryDetailData,
  DecisionImpactMarketStats,
  DecisionImpactResponse,
  DecisionImpactThemeStats,
  EdgeDiagnosticsResponse,
  MarketFeedAlert,
  MLAccuracyApiResponse,
  OpportunitiesResponse,
  OpportunityItem,
  OpsDecisionImpactResponse,
  PlanActionabilityState,
  PlanData,
  PXIData,
  SignalData,
} from '../../../src/types/market-contracts'

export type UtilityEventType =
  | 'session_start'
  | 'plan_view'
  | 'opportunities_view'
  | 'decision_actionable_view'
  | 'decision_watch_view'
  | 'decision_no_action_view'
  | 'no_action_unlock_view'
  | 'cta_action_click'

export interface MLAccuracyData {
  as_of: string | null
  coverage: {
    total_predictions: number
    evaluated_count: number
    pending_count: number
  }
  coverage_quality: 'ROBUST' | 'LIMITED' | 'INSUFFICIENT'
  minimum_reliable_sample: number
  unavailable_reasons: string[]
  rolling_7d: {
    direction_accuracy: number | null
    sample_size: number
    mae: number | null
  }
  rolling_30d: {
    direction_accuracy: number | null
    sample_size: number
    mae: number | null
  }
  all_time: {
    direction_accuracy_7d: number | null
    direction_accuracy_30d: number | null
    total_predictions: number
  }
}

export interface HistoryDataPoint {
  date: string
  score: number
  regime?: 'RISK_ON' | 'RISK_OFF' | 'TRANSITION'
}

export interface SignalTheme {
  rank: number
  theme_id: string
  theme_name: string
  score: number
  classification: {
    signal_type: string
    confidence: string
    timing: string
    stars: number
  }
  key_tickers: string[]
}

export interface SignalsData {
  run_id: string
  generated_at_utc: string
  themes: SignalTheme[]
}

export interface SignalsRunSummary {
  id: string
  status?: 'ok' | 'error'
}

export interface SimilarPeriod {
  date: string
  similarity: number
  weights: {
    combined: number
    similarity: number
    recency: number
    accuracy: number
    accuracy_sample: number
  }
  pxi: {
    date: string
    score: number
    label: string
    status: string
  } | null
  forward_returns: {
    d7: number | null
    d30: number | null
  } | null
}

export interface SimilarPeriodsData {
  current_date: string
  similar_periods: SimilarPeriod[]
}

export interface BacktestData {
  summary: {
    total_observations: number
    with_7d_return: number
    with_30d_return: number
    date_range: {
      start: string
      end: string
    }
  }
  bucket_analysis: Array<{
    bucket: string
    count: number
    avg_return_7d: number | null
    avg_return_30d: number | null
    win_rate_7d: number | null
    win_rate_30d: number | null
  }>
  extreme_readings?: {
    low_pxi: { count: number; avg_return_30d: number | null; win_rate_30d: number | null }
    high_pxi: { count: number; avg_return_30d: number | null; win_rate_30d: number | null }
  }
}

export interface PredictionData {
  current: {
    date: string
    score: number
    label: string
    bucket: string
  }
  prediction: {
    method: string
    d7: {
      avg_return: number | null
      median_return: number | null
      win_rate: number | null
      sample_size: number
    }
    d30: {
      avg_return: number | null
      median_return: number | null
      win_rate: number | null
      sample_size: number
    }
  }
  extreme_reading: {
    type: 'OVERSOLD' | 'OVERBOUGHT'
    threshold: string
    historical_count: number
    avg_return_7d: number | null
    avg_return_30d: number | null
    win_rate_7d: number | null
    win_rate_30d: number | null
    signal: 'BULLISH' | 'BEARISH'
  } | null
  interpretation: {
    bias: 'BULLISH' | 'BEARISH' | 'NEUTRAL'
    confidence: 'HIGH' | 'MEDIUM' | 'LOW'
    note: string
  }
}

export interface EnsemblePrediction {
  value: number | null
  direction: 'STRONG_UP' | 'UP' | 'FLAT' | 'DOWN' | 'STRONG_DOWN' | null
  confidence: 'HIGH' | 'MEDIUM' | 'LOW' | null
  components: {
    xgboost: number | null
    lstm: number | null
  }
}

export interface EnsembleData {
  date: string
  current_score: number
  ensemble: {
    weights: { xgboost: number; lstm: number }
    predictions: {
      pxi_change_7d: EnsemblePrediction
      pxi_change_30d: EnsemblePrediction
    }
  }
  interpretation: {
    d7: { agreement: string | null; note: string }
    d30: { agreement: string | null; note: string }
  }
}
