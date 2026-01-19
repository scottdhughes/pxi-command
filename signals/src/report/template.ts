import type { ReportJson, ThemeReportItem } from "./render_json"
import { REPORT_CSS } from "./css"
import type { AccuracyStats } from "../db"

// ─────────────────────────────────────────────────────────────────────────────
// Security Utilities
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Escapes HTML special characters to prevent XSS attacks.
 * Must be applied to all user-derived content before rendering.
 */
function escapeHtml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;")
}

/**
 * Validates and sanitizes URLs for use in href attributes.
 * Only allows https:// URLs to Reddit domains.
 */
function sanitizeUrl(url: string): string {
  const escaped = escapeHtml(url)
  // Only allow https URLs to reddit.com
  if (escaped.startsWith("https://reddit.com/") || escaped.startsWith("https://www.reddit.com/")) {
    return escaped
  }
  // Return empty for invalid URLs (link will be broken but safe)
  return "#"
}

// ─────────────────────────────────────────────────────────────────────────────
// Sparkline Visualization
// ─────────────────────────────────────────────────────────────────────────────

const SPARK_BLOCKS = ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"]

/**
 * Renders a sparkline from an array of daily counts using Unicode block characters.
 * Normalizes values to fit the available block range.
 */
function renderSparkline(dailyCounts: number[]): { sparkline: string; isNegative: boolean } {
  if (!dailyCounts || dailyCounts.length === 0) {
    return { sparkline: "▁▁▁▁▁▁▁", isNegative: false }
  }

  const max = Math.max(...dailyCounts)
  const min = Math.min(...dailyCounts)
  const range = max - min || 1

  const sparkline = dailyCounts
    .map((val) => {
      const normalized = (val - min) / range
      const index = Math.min(Math.floor(normalized * SPARK_BLOCKS.length), SPARK_BLOCKS.length - 1)
      return SPARK_BLOCKS[index]
    })
    .join("")

  // Trend is negative if later values are lower than earlier values
  const firstHalf = dailyCounts.slice(0, Math.floor(dailyCounts.length / 2))
  const secondHalf = dailyCounts.slice(Math.floor(dailyCounts.length / 2))
  const firstAvg = firstHalf.reduce((a, b) => a + b, 0) / firstHalf.length
  const secondAvg = secondHalf.reduce((a, b) => a + b, 0) / secondHalf.length

  return { sparkline, isNegative: secondAvg < firstAvg * 0.8 }
}

// ─────────────────────────────────────────────────────────────────────────────
// Classification Pill Helpers
// ─────────────────────────────────────────────────────────────────────────────

function getSignalTypeClass(signalType: string): string {
  const normalized = signalType.toLowerCase().replace(/\s+/g, "-")
  if (normalized.includes("rotation")) return "pill-rotation"
  if (normalized.includes("momentum")) return "pill-momentum"
  if (normalized.includes("divergence")) return "pill-divergence"
  if (normalized.includes("reversion")) return "pill-reversion"
  return "pill-rotation"
}

function getConfidenceClass(confidence: string): string {
  const normalized = confidence.toLowerCase().replace(/\s+/g, "-")
  if (normalized.includes("very-high")) return "pill-conf-very-high"
  if (normalized.includes("high")) return "pill-conf-high"
  if (normalized.includes("medium-high")) return "pill-conf-medium-high"
  if (normalized.includes("medium")) return "pill-conf-medium"
  return "pill-conf-low"
}

function getTimingClass(timing: string): string {
  const normalized = timing.toLowerCase()
  if (normalized.includes("now")) return "pill-timing-now"
  if (normalized.includes("building")) return "pill-timing-building"
  if (normalized.includes("early")) return "pill-timing-early"
  if (normalized.includes("ongoing")) return "pill-timing-ongoing"
  return "pill-timing-early"
}

// ─────────────────────────────────────────────────────────────────────────────
// Star Rating
// ─────────────────────────────────────────────────────────────────────────────

function renderStars(count: number): string {
  const filled = "★".repeat(count)
  const empty = `<span class="empty">${"■".repeat(Math.max(0, 5 - count))}</span>`
  return `${filled}${empty}`
}

// ─────────────────────────────────────────────────────────────────────────────
// Summary Dashboard
// ─────────────────────────────────────────────────────────────────────────────

function renderSummaryDashboard(themes: ThemeReportItem[], totalDocs: number): string {
  const topSignal = themes[0]?.theme_name || "N/A"

  // Calculate average confidence score (map confidence strings to numbers)
  const confMap: Record<string, number> = {
    "Very High": 5,
    "High": 4,
    "Medium-High": 3,
    "Medium": 2,
    "Medium-Low": 1,
  }
  const avgConfScore =
    themes.reduce((sum, t) => sum + (confMap[t.classification.confidence] || 2), 0) / themes.length
  const avgConfLabel =
    avgConfScore >= 4 ? "High" : avgConfScore >= 3 ? "Medium-High" : avgConfScore >= 2 ? "Medium" : "Low"

  // Count "Now" timing signals
  const nowCount = themes.filter((t) => t.classification.timing.toLowerCase().includes("now")).length

  return `
    <div class="summary-grid">
      <div class="stat-card highlight">
        <div class="stat-label">Top Signal</div>
        <div class="stat-value accent">#1</div>
        <div class="stat-sub">${escapeHtml(topSignal)}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Avg Confidence</div>
        <div class="stat-value">${avgConfScore.toFixed(1)}</div>
        <div class="stat-sub">${avgConfLabel}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">"Now" Signals</div>
        <div class="stat-value accent">${nowCount}</div>
        <div class="stat-sub">Ready to act</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Data Points</div>
        <div class="stat-value">${totalDocs.toLocaleString()}</div>
        <div class="stat-sub">Reddit posts analyzed</div>
      </div>
    </div>
  `
}

// ─────────────────────────────────────────────────────────────────────────────
// Signal Distribution Bar
// ─────────────────────────────────────────────────────────────────────────────

function renderDistributionBar(themes: ThemeReportItem[]): string {
  const distribution: Record<string, number> = {
    Rotation: 0,
    Momentum: 0,
    Divergence: 0,
    "Mean Reversion": 0,
  }

  themes.forEach((t) => {
    const type = t.classification.signal_type
    if (type.includes("Rotation")) distribution["Rotation"]++
    else if (type.includes("Momentum")) distribution["Momentum"]++
    else if (type.includes("Divergence")) distribution["Divergence"]++
    else if (type.includes("Reversion")) distribution["Mean Reversion"]++
    else distribution["Rotation"]++ // default
  })

  const segments = Object.entries(distribution)
    .filter(([_, count]) => count > 0)
    .map(([type, count]) => {
      const cssClass = type.toLowerCase().replace(/\s+/g, "").replace("mean", "")
      return `<div class="dist-segment ${cssClass}" style="flex: ${count}">${count > 1 ? `${count}` : ""}</div>`
    })
    .join("")

  const legendItems = Object.entries(distribution)
    .filter(([_, count]) => count > 0)
    .map(([type, count]) => {
      const cssClass = type.toLowerCase().replace(/\s+/g, "").replace("mean", "")
      return `
        <div class="legend-item">
          <div class="legend-dot ${cssClass}"></div>
          <span>${escapeHtml(type)} (${count})</span>
        </div>
      `
    })
    .join("")

  return `
    <div class="distribution-section">
      <div class="section-header">
        <span class="section-title">Signal Distribution</span>
        <div class="section-line"></div>
      </div>
      <div class="distribution-bar">
        ${segments}
      </div>
      <div class="dist-legend">
        ${legendItems}
      </div>
    </div>
  `
}

// ─────────────────────────────────────────────────────────────────────────────
// Classification Pills
// ─────────────────────────────────────────────────────────────────────────────

function renderClassificationPills(classification: ThemeReportItem["classification"]): string {
  return `
    <div class="theme-pills">
      <span class="pill ${getSignalTypeClass(classification.signal_type)}">${escapeHtml(classification.signal_type)}</span>
      <span class="pill ${getConfidenceClass(classification.confidence)}">${escapeHtml(classification.confidence)}</span>
      <span class="pill ${getTimingClass(classification.timing)}">${escapeHtml(classification.timing)}</span>
    </div>
  `
}

// ─────────────────────────────────────────────────────────────────────────────
// Metrics Grid
// ─────────────────────────────────────────────────────────────────────────────

function renderMetricsGrid(item: ThemeReportItem): string {
  const m = item.metrics

  const velocityClass = m.growth_ratio >= 1.5 ? "positive" : m.growth_ratio < 1 ? "negative" : "neutral"
  const sentimentClass = m.sentiment_shift > 0 ? "positive" : m.sentiment_shift < 0 ? "negative" : "neutral"
  const confirmClass = m.confirmation_score >= 0.5 ? "positive" : m.confirmation_score < 0.3 ? "negative" : "neutral"

  return `
    <div class="metrics-grid">
      <div class="metric-item">
        <div class="metric-label">Velocity</div>
        <div class="metric-value ${velocityClass}">${m.growth_ratio.toFixed(2)}x</div>
      </div>
      <div class="metric-item">
        <div class="metric-label">Sentiment</div>
        <div class="metric-value ${sentimentClass}">${m.sentiment_shift >= 0 ? "+" : ""}${m.sentiment_shift.toFixed(2)}</div>
      </div>
      <div class="metric-item">
        <div class="metric-label">Confirmation</div>
        <div class="metric-value ${confirmClass}">${Math.round(m.confirmation_score * 100)}%</div>
      </div>
    </div>
  `
}

// ─────────────────────────────────────────────────────────────────────────────
// Ticker Tags
// ─────────────────────────────────────────────────────────────────────────────

function renderTickerTags(tickers: string[]): string {
  if (!tickers || tickers.length === 0) return ""

  const tags = tickers.map((t) => `<span class="ticker-tag">${escapeHtml(t)}</span>`).join("")

  return `
    <div class="tickers-row">
      <div class="tickers-label">Key Tickers</div>
      <div class="ticker-tags">${tags}</div>
    </div>
  `
}

// ─────────────────────────────────────────────────────────────────────────────
// Narrative
// ─────────────────────────────────────────────────────────────────────────────

function renderNarrative(item: ThemeReportItem): string {
  const m = item.metrics
  const parts: string[] = []

  parts.push(`Mentions are running ${m.growth_ratio.toFixed(2)}x the baseline rate with coverage across ${m.unique_subreddits} subreddits.`)

  const sentiment = m.sentiment_shift >= 0 ? "improving" : "softening"
  parts.push(`Sentiment is ${sentiment} versus baseline (${m.sentiment_shift >= 0 ? "+" : ""}${m.sentiment_shift.toFixed(2)}).`)

  if (m.concentration > 0.6) {
    parts.push("Activity is concentrated in a small set of posts, which can amplify noise.")
  }

  return `<div class="narrative">${escapeHtml(parts.join(" "))}</div>`
}

// ─────────────────────────────────────────────────────────────────────────────
// Caution Banner
// ─────────────────────────────────────────────────────────────────────────────

function renderCautionBanner(item: ThemeReportItem): string {
  const m = item.metrics
  const reasons: string[] = []

  if (m.concentration > 0.6) reasons.push("high concentration")
  if (m.mentions_L < 3) reasons.push("low sample size")

  if (reasons.length === 0) return ""

  return `
    <div class="caution-banner">
      <span class="caution-icon">&#9888;</span>
      <span class="caution-text">Caution: ${escapeHtml(reasons.join(" and "))} detected</span>
    </div>
  `
}

// ─────────────────────────────────────────────────────────────────────────────
// Evidence Section (Collapsible)
// ─────────────────────────────────────────────────────────────────────────────

function renderEvidenceSection(item: ThemeReportItem): string {
  const links = item.evidence_links.slice(0, 5)
  if (links.length === 0) return ""

  const evidenceItems = links
    .map((l) => {
      const safeUrl = sanitizeUrl(l)
      // Extract a shorter display text from the URL
      const displayText = l.replace(/^https?:\/\/(www\.)?reddit\.com/, "").slice(0, 80)
      return `
        <div class="evidence-item">
          <a href="${safeUrl}" class="evidence-link" target="_blank" rel="noopener noreferrer">${escapeHtml(displayText)}...</a>
        </div>
      `
    })
    .join("")

  return `
    <div class="evidence-section" id="evidence-${item.rank}">
      <button class="evidence-toggle" onclick="this.parentElement.classList.toggle('open')">
        <span>Evidence (${links.length} sources)</span>
        <span class="evidence-toggle-icon">&#9660;</span>
      </button>
      <div class="evidence-list">
        ${evidenceItems}
      </div>
    </div>
  `
}

// ─────────────────────────────────────────────────────────────────────────────
// Theme Card
// ─────────────────────────────────────────────────────────────────────────────

function renderThemeCard(item: ThemeReportItem): string {
  const { sparkline, isNegative } = renderSparkline(item.metrics.daily_counts_L)
  const sparklineClass = isNegative ? "sparkline negative" : "sparkline"

  return `
    <div class="theme-card">
      <div class="theme-card-header">
        <div class="theme-rank-badge">
          <span class="rank-number">${item.rank}</span>
          <span class="rank-stars">${renderStars(item.classification.stars)}</span>
        </div>
        <div class="theme-info">
          <div class="theme-name">${escapeHtml(item.theme_name)}</div>
          ${renderClassificationPills(item.classification)}
        </div>
        <div class="theme-sparkline">
          <span class="sparkline-label">7-Day Trend</span>
          <span class="${sparklineClass}">${sparkline}</span>
        </div>
      </div>
      <div class="theme-card-body">
        ${renderMetricsGrid(item)}
        ${renderTickerTags(item.key_tickers)}
        ${renderNarrative(item)}
        ${renderCautionBanner(item)}
        ${renderEvidenceSection(item)}
      </div>
    </div>
  `
}

// ─────────────────────────────────────────────────────────────────────────────
// Key Takeaways
// ─────────────────────────────────────────────────────────────────────────────

function renderTakeaways(takeaways: {
  data_shows: string[]
  actionable_signals: string[]
  risk_factors: string[]
}): string {
  const renderList = (items: string[]) =>
    items.map((t) => `<li>${escapeHtml(t)}</li>`).join("")

  return `
    <div class="section">
      <div class="section-header">
        <span class="section-title">Key Takeaways</span>
        <div class="section-line"></div>
      </div>
      <div class="takeaways-grid">
        <div class="takeaway-card data">
          <div class="takeaway-title">What the Data Shows</div>
          <ul class="takeaway-list">
            ${renderList(takeaways.data_shows)}
          </ul>
        </div>
        <div class="takeaway-card actionable">
          <div class="takeaway-title">Actionable Signals</div>
          <ul class="takeaway-list">
            ${renderList(takeaways.actionable_signals)}
          </ul>
        </div>
        <div class="takeaway-card risks">
          <div class="takeaway-title">Risk Factors</div>
          <ul class="takeaway-list">
            ${renderList(takeaways.risk_factors)}
          </ul>
        </div>
      </div>
    </div>
  `
}

// ─────────────────────────────────────────────────────────────────────────────
// Accuracy Track Record Card
// ─────────────────────────────────────────────────────────────────────────────

function renderAccuracyCard(accuracy: AccuracyStats | null): string {
  // No data state - minimal design
  if (!accuracy || accuracy.overall.total === 0) {
    return `
      <div class="accuracy-card">
        <div class="accuracy-header">
          <div class="accuracy-title">Signal Performance</div>
        </div>
        <div class="accuracy-no-data">
          <div class="accuracy-no-data-text">Performance data collecting...</div>
          <div class="accuracy-no-data-sub">First accuracy report available after 7 days of predictions</div>
        </div>
      </div>
    `
  }

  const hitRate = Math.round(accuracy.overall.hit_rate)
  const avgReturn = accuracy.overall.avg_return
  const avgReturnClass = avgReturn >= 0 ? "positive" : "negative"
  const avgReturnSign = avgReturn >= 0 ? "+" : ""

  // Get timing breakdown
  const timingEntries = Object.entries(accuracy.by_timing)
  const getTimingData = (key: string) => {
    const found = timingEntries.find(([k]) => k.toLowerCase().includes(key.toLowerCase()))
    return found ? found[1] : null
  }

  const nowData = getTimingData("now")
  const buildingData = getTimingData("building")
  const earlyData = getTimingData("early") || getTimingData("ongoing")

  // Render a row in the breakdown
  const renderTimingRow = (
    label: string,
    cssClass: string,
    data: { total: number; hit_rate: number; avg_return: number } | null
  ) => {
    if (!data || data.total === 0) return ""

    const rate = Math.round(data.hit_rate)
    return `
      <div class="accuracy-row">
        <div class="accuracy-row-label">${escapeHtml(label)}</div>
        <div class="accuracy-row-bar">
          <div class="accuracy-row-fill ${cssClass}" style="width: ${rate}%"></div>
        </div>
        <div class="accuracy-row-values">
          <div class="accuracy-row-rate">${rate}%</div>
          <div class="accuracy-row-count">${data.total}</div>
        </div>
      </div>
    `
  }

  return `
    <div class="accuracy-card">
      <div class="accuracy-header">
        <div class="accuracy-title">Signal Performance</div>
        <div class="accuracy-sample">
          <span class="accuracy-sample-dot"></span>
          <span>${accuracy.overall.total} evaluated</span>
        </div>
      </div>
      <div class="accuracy-body">
        <div class="accuracy-main">
          <div class="accuracy-hero-value">${hitRate}<span class="unit">%</span></div>
          <div class="accuracy-hero-label">Hit Rate</div>
          <div class="accuracy-hero-sub">
            Avg Return: <span class="value ${avgReturnClass}">${avgReturnSign}${avgReturn.toFixed(1)}%</span>
          </div>
        </div>
        <div class="accuracy-breakdown">
          ${renderTimingRow("now", "now", nowData)}
          ${renderTimingRow("building", "building", buildingData)}
          ${renderTimingRow("early", "early", earlyData)}
        </div>
      </div>
    </div>
  `
}

// ─────────────────────────────────────────────────────────────────────────────
// Calculate Next Update Date
// ─────────────────────────────────────────────────────────────────────────────

function getNextUpdateDate(): string {
  const now = new Date()
  const dayOfWeek = now.getUTCDay() // 0 = Sunday, 1 = Monday, etc.

  // Find next Monday
  let daysUntilMonday = (8 - dayOfWeek) % 7
  if (daysUntilMonday === 0) daysUntilMonday = 7 // If today is Monday, next Monday

  const nextMonday = new Date(now)
  nextMonday.setUTCDate(now.getUTCDate() + daysUntilMonday)
  nextMonday.setUTCHours(15, 0, 0, 0) // 15:00 UTC = 10 AM EST

  return nextMonday.toISOString().split("T")[0]
}

// ─────────────────────────────────────────────────────────────────────────────
// Main Render Function
// ─────────────────────────────────────────────────────────────────────────────

export function renderHtml(
  report: ReportJson,
  takeaways: { data_shows: string[]; actionable_signals: string[]; risk_factors: string[] },
  accuracy: AccuracyStats | null = null
): string {
  const themeCards = report.themes.map(renderThemeCard).join("")
  const analysisDate = report.generated_at_utc.split("T")[0]
  const nextUpdate = getNextUpdateDate()

  // Dynamic SEO content
  const topTheme = report.themes[0]
  const topThemeName = topTheme?.theme_name || "Market Themes"
  const pageTitle = `Early Sector Rotation Signals | PXI Command`
  const pageDescription = `Weekly analysis of ${report.summary.total_docs} Reddit discussions. Top signal: ${topThemeName}. Updated ${analysisDate}.`
  const pageUrl = "https://pxicommand.com/signals"
  const ogImage = "https://pxicommand.com/signals/og-image.png"

  // JSON-LD structured data
  const structuredData = {
    "@context": "https://schema.org",
    "@type": "WebPage",
    "name": pageTitle,
    "description": pageDescription,
    "url": pageUrl,
    "dateModified": report.generated_at_utc,
    "publisher": {
      "@type": "Organization",
      "name": "PXI Command",
      "url": "https://pxicommand.com"
    },
    "mainEntity": {
      "@type": "Dataset",
      "name": "Early Sector Rotation Signals",
      "description": "Investment theme analysis derived from Reddit discussion patterns",
      "dateModified": report.generated_at_utc,
      "creator": {
        "@type": "Organization",
        "name": "PXI Command"
      }
    }
  }

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />

  <!-- Primary Meta Tags -->
  <title>${escapeHtml(pageTitle)}</title>
  <meta name="title" content="${escapeHtml(pageTitle)}" />
  <meta name="description" content="${escapeHtml(pageDescription)}" />
  <meta name="robots" content="index, follow" />
  <meta name="author" content="PXI Command" />
  <link rel="canonical" href="${pageUrl}" />

  <!-- Theme Color for Browser UI -->
  <meta name="theme-color" content="#000000" />
  <meta name="color-scheme" content="dark" />

  <!-- Open Graph / Facebook -->
  <meta property="og:type" content="website" />
  <meta property="og:url" content="${pageUrl}" />
  <meta property="og:title" content="${escapeHtml(pageTitle)}" />
  <meta property="og:description" content="${escapeHtml(pageDescription)}" />
  <meta property="og:image" content="${ogImage}" />
  <meta property="og:image:width" content="1200" />
  <meta property="og:image:height" content="630" />
  <meta property="og:site_name" content="PXI Command" />
  <meta property="og:locale" content="en_US" />

  <!-- Twitter -->
  <meta name="twitter:card" content="summary_large_image" />
  <meta name="twitter:url" content="${pageUrl}" />
  <meta name="twitter:title" content="${escapeHtml(pageTitle)}" />
  <meta name="twitter:description" content="${escapeHtml(pageDescription)}" />
  <meta name="twitter:image" content="${ogImage}" />

  <!-- Structured Data -->
  <script type="application/ld+json">${JSON.stringify(structuredData)}</script>

  <style>${REPORT_CSS}</style>
</head>
<body>
  <!-- Site Navigation - Matching pxicommand.com -->
  <nav class="site-nav">
    <a href="/" class="site-nav-brand">PXI<span class="brand-slash">/</span>COMMAND<span class="brand-caret">▼</span></a>
    <div class="site-nav-links">
      <a href="/signals" class="nav-link">Signals</a>
    </div>
  </nav>

  <div class="container">
    <!-- Hero Header -->
    <header class="hero">
      <h1 class="hero-title">Early Sector <span class="accent">Rotation</span> Signals</h1>
      <div class="hero-meta">
        <div class="hero-meta-item">
          <span class="hero-meta-label">Analysis Date</span>
          <span class="hero-meta-value">${escapeHtml(analysisDate)}</span>
        </div>
        <div class="hero-meta-item">
          <span class="hero-meta-label">Next Update</span>
          <span class="hero-meta-value">${escapeHtml(nextUpdate)}</span>
        </div>
        <div class="hero-meta-item">
          <span class="hero-meta-label">Themes Analyzed</span>
          <span class="hero-meta-value">${report.themes.length}</span>
        </div>
      </div>
    </header>

    <!-- Summary Dashboard -->
    ${renderSummaryDashboard(report.themes, report.summary.total_docs)}

    <!-- Accuracy Track Record -->
    ${renderAccuracyCard(accuracy)}

    <!-- Signal Distribution -->
    ${renderDistributionBar(report.themes)}

    <!-- Key Takeaways -->
    ${renderTakeaways(takeaways)}

    <!-- Top Opportunities -->
    <div class="section">
      <div class="section-header">
        <span class="section-title">Top ${report.themes.length} Opportunities</span>
        <div class="section-line"></div>
      </div>
      ${themeCards}
    </div>

    <!-- Footer -->
    <footer class="footer">
      <div class="footer-text">PXI Command Signals</div>
      <div class="footer-disclaimer">
        Analysis derived from public Reddit discussions. Not investment advice.
        Methodology: Mention Velocity, Sentiment Shift, Cross-Platform Confirmation.
      </div>
    </footer>
  </div>
</body>
</html>`
}
