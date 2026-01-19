import type { ReportJson } from "./render_json"
import { renderHtml as buildHtml } from "./template"
import type { AccuracyStats } from "../db"

export function renderHtml(
  report: ReportJson,
  takeaways: { data_shows: string[]; actionable_signals: string[]; risk_factors: string[] },
  accuracy: AccuracyStats | null = null
) {
  return buildHtml(report, takeaways, accuracy)
}
