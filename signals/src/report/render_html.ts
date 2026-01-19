import type { ReportJson } from "./render_json"
import { renderHtml as buildHtml } from "./template"

export function renderHtml(report: ReportJson, takeaways: { data_shows: string[]; actionable_signals: string[]; risk_factors: string[] }) {
  return buildHtml(report, takeaways)
}
