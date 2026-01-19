import type { ThemeMetrics } from "../analysis/metrics"
import type { ThemeScore } from "../analysis/scoring"
import type { ThemeClassification } from "../analysis/classify"

export interface ReportConfig {
  lookback_days: number
  baseline_days: number
  top_n: number
  price_provider: string
  enable_comments: boolean
  enable_rss: boolean
}

export interface ThemeReportItem {
  rank: number
  theme_id: string
  theme_name: string
  score: number
  classification: ThemeClassification
  metrics: ThemeMetrics
  scoring: ThemeScore
  evidence_links: string[]
  key_tickers: string[]
}

export interface ReportJson {
  run_id: string
  generated_at_utc: string
  config: ReportConfig
  summary: {
    total_docs: number
    total_themes: number
  }
  themes: ThemeReportItem[]
}

export function renderJson(runId: string, generatedAt: string, config: ReportConfig, totalDocs: number, themes: ThemeReportItem[]): ReportJson {
  return {
    run_id: runId,
    generated_at_utc: generatedAt,
    config,
    summary: {
      total_docs: totalDocs,
      total_themes: themes.length,
    },
    themes,
  }
}
