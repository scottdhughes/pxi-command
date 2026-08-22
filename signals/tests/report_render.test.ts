import { describe, it, expect } from "vitest"
import sampleData from "../data/sample_reddit.json" assert { type: "json" }
import type { RedditDataset } from "../src/reddit/types"
import { THEMES } from "../src/analysis/themes"
import { computeMetrics } from "../src/analysis/metrics"
import { scoreThemes } from "../src/analysis/scoring"
import { classifyTheme } from "../src/analysis/classify"
import { buildTakeaways } from "../src/analysis/takeaways"
import { renderJson } from "../src/report/render_json"
import { renderHtml } from "../src/report/render_html"
import { nowUtcIso } from "../src/utils/time"
import { upgradeStoredReportNavigation } from "../src/routes"

describe("report rendering", () => {
  it("includes required sections", () => {
    const dataset = sampleData as RedditDataset
    const result = computeMetrics(dataset, THEMES, 7, 30, false)
    const scores = scoreThemes(result.metrics)
    const ranked = scores.slice(0, 10).map((s, idx) => {
      const m = result.metrics.find((mm) => mm.theme_id === s.theme_id)!
      const classification = classifyTheme(m, s, idx + 1, 10)
      return {
        rank: idx + 1,
        theme_id: s.theme_id,
        theme_name: s.theme_name,
        score: s.score,
        classification,
        metrics: m,
        scoring: s,
        evidence_links: m.evidence_links,
        key_tickers: m.key_tickers,
      }
    })
    const takeaways = buildTakeaways(ranked.map((r) => ({ metrics: r.metrics, score: r.scoring, classification: r.classification })))
    const reportJson = renderJson(
      "offline-demo",
      nowUtcIso(),
      {
        lookback_days: 7,
        baseline_days: 30,
        top_n: 10,
        price_provider: "none",
        enable_comments: false,
        enable_rss: false,
      },
      result.docs.length,
      ranked
    )
    const html = renderHtml(reportJson, takeaways)
    expect(html).toContain("Signal Distribution")
    expect(html).toContain("Key Takeaways")
    expect(html).toContain("Not investment advice.")
    expect(html).toContain("Research Timing Flags — Not Actionable")
    expect(html).toContain("Research-only observation layer")
    expect(html).not.toContain("Ready to act")
    expect(html).not.toContain("Top 10 Opportunities")
    expect(html).toContain("Top Signal")
    for (const href of ["/", "/brief", "/opportunities", "/signals", "/alerts", "/inbox", "/guide", "/spec"]) {
      expect(html).toContain(`href="${href}"`)
    }

    const jsonLd = html.match(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/)?.[1]
    expect(jsonLd).toBeDefined()
    const structuredData = JSON.parse(jsonLd!)
    expect(structuredData.mainEntity["@type"]).toBe("Report")
    expect(JSON.stringify(structuredData)).not.toContain('"@type":"Dataset"')
  })

  it("upgrades stored report snapshots", () => {
    const legacy = `<html><head><script type="application/ld+json">{"@context":"https://schema.org","@type":"WebPage","mainEntity":{"@type":"Dataset","name":"Legacy signals","description":"Legacy sector rotation research derived from discussion patterns."}}</script></head><body><div class="nav-dropdown-menu"><a href="/">/</a><a href="/spec">/SPEC</a><a href="/signals">/SIGNALS</a></div><header></header><!-- Summary Dashboard --><div class="takeaway-title">Actionable Signals</div><span class="section-title">Top 8 Opportunities</span><div class="stat-label">"Now" Signals</div><div class="stat-sub">Ready to act</div></body></html>`
    const upgraded = upgradeStoredReportNavigation(legacy)
    for (const href of ["/", "/brief", "/opportunities", "/signals", "/alerts", "/inbox", "/guide", "/spec"]) {
      expect(upgraded).toContain(`href="${href}"`)
    }
    expect(upgraded).toContain(".nav-dropdown-menu{width:224px")
    expect(upgraded).toContain("Research Timing Flags — Not Actionable")
    expect(upgraded).toContain("Top 8 Observed Themes")
    expect(upgraded).toContain('"Now" Activity Flags')
    expect(upgraded).toContain("Research only")
    expect(upgraded).toContain("Research-only observation layer")
    expect(upgraded).not.toContain("Ready to act")

    const jsonLd = upgraded.match(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/)?.[1]
    expect(jsonLd).toBeDefined()
    const structuredData = JSON.parse(jsonLd!)
    expect(structuredData.mainEntity["@type"]).toBe("Report")
    expect(JSON.stringify(structuredData)).not.toContain('"@type":"Dataset"')
  })
})
