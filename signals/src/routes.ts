import type { Env } from "./config"
import { getConfig } from "./config"
import { listRuns, getRun, insertRun, getAccuracyStats, listPredictions } from "./db"
import { getObjectText, getLatestRunId } from "./storage"
import { runPipeline } from "./scheduled"
import sampleData from "../data/sample_reddit.json" assert { type: "json" }
import type { RedditDataset } from "./reddit/types"
import { THEMES } from "./analysis/themes"
import { computeMetrics } from "./analysis/metrics"
import { scoreThemes } from "./analysis/scoring"
import { classifyTheme } from "./analysis/classify"
import { buildTakeaways } from "./analysis/takeaways"
import { renderJson } from "./report/render_json"
import { renderHtml } from "./report/render_html"
import { nowUtcIso } from "./utils/time"
import { ulid } from "ulidx"

// ─────────────────────────────────────────────────────────────────────────────
// Security Constants
// ─────────────────────────────────────────────────────────────────────────────

/** Rate limit window in seconds */
const RATE_LIMIT_WINDOW = 60

/** Maximum requests per rate limit window */
const RATE_LIMIT_MAX = 5

/** Pattern for validating run IDs: YYYYMMDD-ULID */
const RUNID_PATTERN = /^\d{8}-[0-9A-Z]{26}$/

// ─────────────────────────────────────────────────────────────────────────────
// Security Utilities
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Performs constant-time string comparison to prevent timing attacks.
 * Always compares all characters regardless of where mismatches occur.
 */
function secureCompare(a: string, b: string): boolean {
  // Use constant-time comparison to prevent timing attacks
  // Always compare max length to avoid early exit timing leaks
  const maxLen = Math.max(a.length, b.length)
  let result = a.length ^ b.length // Non-zero if lengths differ

  for (let i = 0; i < maxLen; i++) {
    const charA = i < a.length ? a.charCodeAt(i) : 0
    const charB = i < b.length ? b.charCodeAt(i) : 0
    result |= charA ^ charB
  }

  return result === 0
}

/**
 * Validates that a run ID matches the expected format (YYYYMMDD-ULID).
 */
function isValidRunId(id: string): boolean {
  return RUNID_PATTERN.test(id)
}

/**
 * Checks rate limit for a given key using KV storage.
 * Returns true if the request is allowed, false if rate limited.
 */
async function checkRateLimit(env: Env, key: string): Promise<boolean> {
  if (!env.SIGNALS_KV) return true // Skip if KV not configured

  const rateLimitKey = `ratelimit:${key}`
  const countStr = await env.SIGNALS_KV.get(rateLimitKey)
  const count = countStr ? parseInt(countStr, 10) : 0

  if (count >= RATE_LIMIT_MAX) {
    return false
  }

  await env.SIGNALS_KV.put(rateLimitKey, String(count + 1), {
    expirationTtl: RATE_LIMIT_WINDOW,
  })
  return true
}

/**
 * Extracts client identifier from request for rate limiting.
 * Uses CF-Connecting-IP header if available, falls back to generic key.
 */
function getClientKey(request: Request): string {
  return request.headers.get("CF-Connecting-IP") || "anonymous"
}

// ─────────────────────────────────────────────────────────────────────────────
// Response Helpers
// ─────────────────────────────────────────────────────────────────────────────

function jsonResponse(data: unknown, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "Content-Type": "application/json" },
  })
}

function htmlResponse(html: string, status = 200) {
  return new Response(html, {
    status,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  })
}

function fallbackHtml() {
  return `<!doctype html>
  <html lang="en">
  <head><meta charset="utf-8"/><title>Signals</title></head>
  <body><p>Signals report is temporarily unavailable.</p><p>Not investment advice.</p></body></html>`
}

async function buildStubReport(env: Env) {
  const cfg = getConfig(env)
  const dataset = sampleData as RedditDataset
  const metricResult = computeMetrics(dataset, THEMES, cfg.lookbackDays, cfg.baselineDays, cfg.enableComments)
  const scores = scoreThemes(metricResult.metrics)
  const eligible = scores.filter((s) => {
    const m = metricResult.metrics.find((mm) => mm.theme_id === s.theme_id)
    return m ? m.evidence_links.length >= 3 : false
  })
  if (eligible.length < cfg.topN) {
    throw new Error("insufficient_evidence")
  }
  const ranked = eligible.slice(0, cfg.topN).map((s, idx) => {
    const m = metricResult.metrics.find((mm) => mm.theme_id === s.theme_id)!
    const classification = classifyTheme(m, s, idx + 1, cfg.topN)
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
      lookback_days: cfg.lookbackDays,
      baseline_days: cfg.baselineDays,
      top_n: cfg.topN,
      price_provider: cfg.priceProvider,
      enable_comments: cfg.enableComments,
      enable_rss: cfg.enableRss,
    },
    metricResult.docs.length,
    ranked
  )
  return renderHtml(reportJson, takeaways, null)
}

export async function handleRequest(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url)
  const cfg = getConfig(env)
  const base = cfg.publicBasePath

  if (!url.pathname.startsWith(base)) {
    return new Response("Not found", { status: 404 })
  }

  const path = url.pathname.slice(base.length) || "/"

  if (request.method === "GET" && (path === "/" || path === "")) {
    return new Response(null, {
      status: 302,
      headers: { Location: `${base}/latest` },
    })
  }

  if (request.method === "GET" && path === "/latest") {
    try {
      const latestId = await getLatestRunId(env)
      if (!latestId) {
        return htmlResponse(await buildStubReport(env))
      }
      const run = await getRun(env, latestId)
      if (!run) return htmlResponse(await buildStubReport(env))
      const html = await getObjectText(env, run.report_html_key)
      if (!html) return htmlResponse(await buildStubReport(env))
      return htmlResponse(html)
    } catch {
      return htmlResponse(fallbackHtml())
    }
  }

  if (request.method === "GET" && path.startsWith("/run/")) {
    const runId = path.replace("/run/", "")
    if (!isValidRunId(runId)) {
      return new Response("Invalid run ID format", { status: 400 })
    }
    const run = await getRun(env, runId)
    if (!run) return new Response("Not found", { status: 404 })
    const html = await getObjectText(env, run.report_html_key)
    if (!html) return new Response("Not found", { status: 404 })
    return htmlResponse(html)
  }

  if (request.method === "GET" && path === "/api/runs") {
    const runs = await listRuns(env, 50)
    return jsonResponse({ runs })
  }

  if (request.method === "GET" && path.startsWith("/api/runs/")) {
    const runId = path.replace("/api/runs/", "")
    if (!isValidRunId(runId)) {
      return jsonResponse({ error: "Invalid run ID format" }, 400)
    }
    const run = await getRun(env, runId)
    if (!run) return jsonResponse({ error: "Not found" }, 404)
    const json = await getObjectText(env, run.results_json_key)
    if (!json) return jsonResponse({ error: "Not found" }, 404)
    return new Response(json, { headers: { "Content-Type": "application/json" } })
  }

  if (request.method === "POST" && path === "/api/run") {
    // Rate limit check
    const clientKey = getClientKey(request)
    const allowed = await checkRateLimit(env, `admin:${clientKey}`)
    if (!allowed) {
      return jsonResponse({ error: "Rate limit exceeded" }, 429)
    }

    // Token validation with constant-time comparison
    const token = request.headers.get("X-Admin-Token") || ""
    if (!env.ADMIN_RUN_TOKEN || !secureCompare(token, env.ADMIN_RUN_TOKEN)) {
      return jsonResponse({ error: "Unauthorized" }, 401)
    }

    try {
      const result = await runPipeline(env)
      return jsonResponse({ ok: true, run_id: result.runId })
    } catch (err: unknown) {
      const error = err instanceof Error ? err : new Error(String(err))
      const runId = `${new Date().toISOString().slice(0, 10).replace(/-/g, "")}-${ulid()}`
      await insertRun(env, {
        id: runId,
        created_at_utc: nowUtcIso(),
        lookback_days: Number(env.DEFAULT_LOOKBACK_DAYS) || 7,
        baseline_days: Number(env.DEFAULT_BASELINE_DAYS) || 30,
        status: "error",
        summary_json: null,
        report_html_key: "",
        results_json_key: "",
        raw_json_key: null,
        error_message: error.message || "admin_run_failed",
      })
      return jsonResponse({ ok: false, error: error.message || "run_failed" }, 500)
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Signal Accuracy and Predictions API
  // ─────────────────────────────────────────────────────────────────────────────

  if (request.method === "GET" && path === "/api/accuracy") {
    try {
      const stats = await getAccuracyStats(env)
      return jsonResponse({
        generated_at: nowUtcIso(),
        sample_size: stats.overall.total,
        overall: {
          hit_rate: `${stats.overall.hit_rate.toFixed(1)}%`,
          avg_return: `${stats.overall.avg_return >= 0 ? "+" : ""}${stats.overall.avg_return.toFixed(1)}%`,
        },
        by_timing: Object.fromEntries(
          Object.entries(stats.by_timing).map(([timing, data]) => [
            timing,
            {
              hit_rate: `${data.hit_rate.toFixed(1)}%`,
              count: data.total,
              avg_return: `${data.avg_return >= 0 ? "+" : ""}${data.avg_return.toFixed(1)}%`,
            },
          ])
        ),
        by_confidence: Object.fromEntries(
          Object.entries(stats.by_confidence).map(([confidence, data]) => [
            confidence,
            {
              hit_rate: `${data.hit_rate.toFixed(1)}%`,
              count: data.total,
              avg_return: `${data.avg_return >= 0 ? "+" : ""}${data.avg_return.toFixed(1)}%`,
            },
          ])
        ),
      })
    } catch {
      return jsonResponse({ error: "Failed to fetch accuracy stats" }, 500)
    }
  }

  if (request.method === "GET" && path === "/api/predictions") {
    try {
      const evaluatedParam = url.searchParams.get("evaluated")
      const limitParam = url.searchParams.get("limit")
      const limit = limitParam ? Math.min(parseInt(limitParam, 10), 100) : 50

      const opts: { limit: number; evaluated?: boolean } = { limit }
      if (evaluatedParam === "true") opts.evaluated = true
      else if (evaluatedParam === "false") opts.evaluated = false

      const predictions = await listPredictions(env, opts)

      return jsonResponse({
        predictions: predictions.map((p) => ({
          signal_date: p.signal_date,
          target_date: p.target_date,
          theme_id: p.theme_id,
          theme_name: p.theme_name,
          rank: p.rank,
          score: Math.round(p.score * 100) / 100,
          signal_type: p.signal_type,
          confidence: p.confidence,
          timing: p.timing,
          stars: p.stars,
          proxy_etf: p.proxy_etf,
          entry_price: p.entry_price,
          exit_price: p.exit_price,
          return_pct: p.return_pct,
          hit: p.hit === 1 ? true : p.hit === 0 ? false : null,
          status: p.evaluated_at ? "evaluated" : "pending",
          evaluated_at: p.evaluated_at,
        })),
      })
    } catch {
      return jsonResponse({ error: "Failed to fetch predictions" }, 500)
    }
  }

  // OG Image for social sharing
  if (request.method === "GET" && path === "/og-image.png") {
    try {
      const latestId = await getLatestRunId(env)
      let topTheme = "Market Themes"
      let totalDocs = 0
      let analysisDate = new Date().toISOString().split("T")[0]

      if (latestId) {
        const run = await getRun(env, latestId)
        if (run?.summary_json) {
          const summary = JSON.parse(run.summary_json)
          topTheme = summary.top_themes?.[0]?.theme || "Market Themes"
          totalDocs = summary.total_docs || 0
          analysisDate = run.created_at_utc?.split("T")[0] || analysisDate
        }
      }

      const svg = generateOgImageSvg(topTheme, totalDocs, analysisDate)
      return new Response(svg, {
        headers: {
          "Content-Type": "image/svg+xml",
          "Cache-Control": "public, max-age=3600",
        },
      })
    } catch {
      // Return a fallback OG image
      const svg = generateOgImageSvg("Sector Rotation Signals", 0, new Date().toISOString().split("T")[0])
      return new Response(svg, {
        headers: { "Content-Type": "image/svg+xml" },
      })
    }
  }

  return new Response("Not found", { status: 404 })
}

/**
 * Generate an OG image as SVG for social sharing
 */
function generateOgImageSvg(topTheme: string, totalDocs: number, analysisDate: string): string {
  return `<svg width="1200" height="630" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="bgGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" style="stop-color:#000000"/>
      <stop offset="100%" style="stop-color:#0a0a0a"/>
    </linearGradient>
    <linearGradient id="accentGrad" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" style="stop-color:#00a3ff"/>
      <stop offset="100%" style="stop-color:#0066aa"/>
    </linearGradient>
  </defs>

  <!-- Background -->
  <rect width="1200" height="630" fill="url(#bgGrad)"/>

  <!-- Accent line at top -->
  <rect x="0" y="0" width="1200" height="4" fill="url(#accentGrad)"/>

  <!-- Grid pattern overlay -->
  <pattern id="grid" width="40" height="40" patternUnits="userSpaceOnUse">
    <path d="M 40 0 L 0 0 0 40" fill="none" stroke="#26272b" stroke-width="0.5"/>
  </pattern>
  <rect width="1200" height="630" fill="url(#grid)" opacity="0.3"/>

  <!-- Brand -->
  <text x="80" y="100" font-family="ui-monospace, SF Mono, monospace" font-size="24" fill="#949ba5" letter-spacing="0.2em">
    PXI<tspan fill="#00a3ff">/</tspan>COMMAND
  </text>

  <!-- Main Title -->
  <text x="80" y="220" font-family="ui-monospace, SF Mono, monospace" font-size="56" font-weight="600" fill="#f3f3f3">
    Early Sector
  </text>
  <text x="80" y="290" font-family="ui-monospace, SF Mono, monospace" font-size="56" font-weight="600" fill="#00a3ff">
    Rotation Signals
  </text>

  <!-- Top Signal Box -->
  <rect x="80" y="340" width="1040" height="120" rx="8" fill="#0a0a0a" stroke="#26272b" stroke-width="1"/>
  <text x="110" y="385" font-family="ui-monospace, SF Mono, monospace" font-size="14" fill="#949ba5" letter-spacing="0.1em">
    TOP SIGNAL
  </text>
  <text x="110" y="430" font-family="ui-monospace, SF Mono, monospace" font-size="32" font-weight="600" fill="#f3f3f3">
    ${escapeXml(topTheme)}
  </text>

  <!-- Stats -->
  <text x="80" y="530" font-family="ui-monospace, SF Mono, monospace" font-size="18" fill="#949ba5">
    ${totalDocs > 0 ? `${totalDocs} discussions analyzed` : "Weekly analysis"} · Updated ${analysisDate}
  </text>

  <!-- URL -->
  <text x="80" y="580" font-family="ui-monospace, SF Mono, monospace" font-size="20" fill="#00a3ff">
    pxicommand.com/signals
  </text>
</svg>`
}

/**
 * Escape XML special characters for SVG text content
 */
function escapeXml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;")
}
