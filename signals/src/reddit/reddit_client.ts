import type { Env } from "../config"
import { getConfig } from "../config"
import type { RedditComment, RedditDataset, RedditPost } from "./types"
import { nowUtcIso } from "../utils/time"
import { parseRedditListing, parseOAuthResponse } from "./schemas"
import { logWarn } from "../utils/logger"

const REDDIT_BASE = "https://oauth.reddit.com"
const REDDIT_PUBLIC_BASES = ["https://api.reddit.com", "https://www.reddit.com", "https://old.reddit.com"]

// Reddit-compliant User-Agent format: <platform>:<app_id>:<version> (by /u/<username>)
const DEFAULT_USER_AGENT = "web:pxi-signals:1.0.0 (by /u/pxi_command)"
const RETRYABLE_HTTP_STATUSES = new Set([429, 500, 502, 503, 504, 522, 523, 524])
const MAX_RETRY_DELAY_MS = 15_000

// Browser-like headers to avoid bot detection (Reddit 2026 requirements)
function getBrowserHeaders(userAgent: string): Record<string, string> {
  return {
    "User-Agent": userAgent,
    "Accept": "application/json, text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Sec-CH-UA": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"macOS"',
    "Upgrade-Insecure-Requests": "1",
  }
}

async function sleep(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

// Add jitter to delays to appear more human-like
function sleepWithJitter(baseMs: number): Promise<void> {
  const jitter = Math.random() * baseMs * 0.5 // 0-50% jitter
  return sleep(baseMs + jitter)
}

function parseRetryAfterMs(headers: Headers): number | null {
  const raw = headers.get("Retry-After")
  if (!raw) return null

  const seconds = Number(raw)
  if (Number.isFinite(seconds) && seconds >= 0) {
    return Math.min(MAX_RETRY_DELAY_MS, Math.round(seconds * 1000))
  }

  const retryAt = Date.parse(raw)
  if (!Number.isFinite(retryAt)) return null
  return Math.min(MAX_RETRY_DELAY_MS, Math.max(0, retryAt - Date.now()))
}

function getRetryDelayMs(attempt: number, headers?: Headers): number {
  const retryAfter = headers ? parseRetryAfterMs(headers) : null
  if (retryAfter !== null) return retryAfter
  const base = Math.min(MAX_RETRY_DELAY_MS, 500 * Math.pow(2, attempt))
  return Math.round(base + Math.random() * 250)
}

async function fetchWithBackoff(input: RequestInfo, init: RequestInit, maxRetries = 3) {
  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    try {
      const res = await fetch(input, init)
      if (!RETRYABLE_HTTP_STATUSES.has(res.status)) {
        return res
      }

      if (attempt >= maxRetries) {
        return res
      }

      const waitMs = getRetryDelayMs(attempt, res.headers)
      logWarn("Retrying Reddit request after retryable status", {
        status: res.status,
        attempt: attempt + 1,
        maxRetries,
        waitMs,
      })
      await sleep(waitMs)
    } catch (err) {
      if (attempt >= maxRetries) {
        throw err
      }

      const waitMs = getRetryDelayMs(attempt)
      logWarn("Retrying Reddit request after network error", {
        attempt: attempt + 1,
        maxRetries,
        waitMs,
        error: String(err),
      })
      await sleep(waitMs)
    }
  }

  throw new Error("unreachable_backoff_state")
}

async function getOAuthToken(env: Env): Promise<string | null> {
  const id = env.REDDIT_CLIENT_ID
  const secret = env.REDDIT_CLIENT_SECRET
  const ua = env.REDDIT_USER_AGENT || DEFAULT_USER_AGENT
  if (!id || !secret) {
    logWarn("Reddit OAuth credentials not configured, will use public API with browser headers")
    return null
  }

  const body = new URLSearchParams({
    grant_type: "client_credentials",
  })

  const basic = btoa(`${id}:${secret}`)
  const browserHeaders = getBrowserHeaders(ua)
  const res = await fetch("https://www.reddit.com/api/v1/access_token", {
    method: "POST",
    headers: {
      ...browserHeaders,
      Authorization: `Basic ${basic}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: body.toString(),
  })

  if (!res.ok) {
    logWarn("OAuth token request failed", { status: res.status, statusText: res.statusText })
    return null
  }

  const json = await res.json()
  const parsed = parseOAuthResponse(json)
  if (!parsed) {
    // Log only non-sensitive fields - never log tokens or full OAuth responses
    logWarn("OAuth response validation failed", {
      hasAccessToken: typeof json === "object" && json !== null && "access_token" in json,
      responseKeys: typeof json === "object" && json !== null ? Object.keys(json) : [],
    })
    return null
  }

  return parsed.access_token
}

function getRedditPathFromPermalink(permalink: string): string {
  try {
    return new URL(permalink).pathname
  } catch {
    return permalink.startsWith("/") ? permalink : `/${permalink}`
  }
}

function getAuthHeaders(base: string, ua: string, token?: string | null): Record<string, string> {
  const headers = getBrowserHeaders(ua)
  if (token && base === REDDIT_BASE) {
    headers.Authorization = `Bearer ${token}`
  }
  return headers
}

function resolveListingUrl(base: string, sub: string, limit: number, after: string | null): string {
  return `${base}/r/${sub}/new.json?limit=${limit}${after ? `&after=${after}` : ""}`
}

function resolveCommentUrl(base: string, permalink: string, maxComments: number): string {
  const path = getRedditPathFromPermalink(permalink)
  return `${base}${path}.json?limit=${maxComments}`
}

async function fetchListingWithFallback(
  sub: string,
  limit: number,
  after: string | null,
  ua: string,
  token: string | null,
  publicBases: string[]
) {
  let lastStatus: string | number = "unknown"
  const baseCandidates = token ? [REDDIT_BASE, ...publicBases] : [...publicBases]
  for (const base of baseCandidates) {
    const headers = getAuthHeaders(base, ua, token)
    const url = resolveListingUrl(base, sub, limit, after)
    let res: Response
    try {
      res = await fetchWithBackoff(url, { headers })
    } catch (err) {
      lastStatus = "network-error"
      logWarn("Reddit listing request failed with network error", { base, sub, error: String(err) })
      continue
    }

    if (!res.ok) {
      lastStatus = res.status
      logWarn(`Reddit listing request failed`, { base, status: res.status, sub })
      continue
    }

    let json: unknown
    try {
      json = await res.json()
    } catch {
      lastStatus = "invalid-json"
      logWarn("Reddit listing response body parse failed", { base, sub })
      continue
    }

    const parsed = parseRedditListing(json)
    if (!parsed) {
      lastStatus = "invalid-schema"
      logWarn("Reddit listing validation failed, trying next source", { base, sub })
      continue
    }

    return parsed
  }

  throw new Error(`Reddit fetch failed: ${lastStatus}`)
}

async function fetchCommentsWithFallback(
  permalink: string,
  maxComments: number,
  ua: string,
  token: string | null,
  publicBases: string[]
) {
  const errors: string[] = []
  const baseCandidates = token ? [REDDIT_BASE, ...publicBases] : [...publicBases]
  for (const base of baseCandidates) {
    const headers = getAuthHeaders(base, ua, token)
    const url = resolveCommentUrl(base, permalink, maxComments)
    let res: Response
    try {
      res = await fetchWithBackoff(url, { headers })
    } catch (err) {
      errors.push(`${base}:network-error`)
      logWarn("Comment request failed with network error", { base, permalink, error: String(err) })
      continue
    }

    if (!res.ok) {
      const error = `${base}:${res.status}`
      errors.push(error)
      continue
    }

    let json: unknown
    try {
      json = await res.json()
    } catch {
      errors.push(`${base}:invalid-json`)
      continue
    }

    if (!Array.isArray(json) || json.length < 2) {
      errors.push(`${base}:unexpected-comment-format`)
      logWarn("Unexpected comment response format", { base, permalink })
      continue
    }

    const comments: RedditComment[] = []
    const listing = (json[1] as { data?: { children?: unknown[] } })?.data?.children || []
    for (const child of listing) {
      const c = (child as { data?: { id?: string; created_utc?: number; body?: string; permalink?: string } })?.data
      if (!c || !c.body || !c.id || !c.created_utc || !c.permalink) continue
      comments.push({
        id: c.id,
        created_utc: c.created_utc,
        body: c.body,
        permalink: `https://reddit.com${c.permalink}`,
      })
      if (comments.length >= maxComments) break
    }
    return comments
  }

  if (errors.length > 0) {
    logWarn("Failed to fetch comments", { permalink, errors })
  }
  return []
}

function mapPost(child: any, subreddit: string): RedditPost | null {
  const data = child?.data
  if (!data) return null
  return {
    id: data.id,
    subreddit,
    created_utc: data.created_utc,
    title: data.title || "",
    selftext: data.selftext || "",
    permalink: `https://reddit.com${data.permalink}`,
    score: data.score || 0,
    num_comments: data.num_comments || 0,
  }
}

export async function fetchRedditDataset(env: Env, subreddits: string[]): Promise<RedditDataset> {
  const cfg = getConfig(env)
  const ua = env.REDDIT_USER_AGENT || DEFAULT_USER_AGENT
  const token = await getOAuthToken(env)

  const publicBases = REDDIT_PUBLIC_BASES

  const posts: RedditPost[] = []
  const failedSubreddits: Array<{ subreddit: string; reason: string }> = []
  for (const sub of subreddits) {
    let after: string | null = null
    let fetched = 0
    while (fetched < cfg.maxPostsPerSubreddit) {
      const limit = Math.min(100, cfg.maxPostsPerSubreddit - fetched)
      let json: Awaited<ReturnType<typeof fetchListingWithFallback>>
      try {
        json = await fetchListingWithFallback(sub, limit, after, ua, token, publicBases)
      } catch (err) {
        const reason = err instanceof Error ? err.message : String(err)
        failedSubreddits.push({ subreddit: sub, reason })
        logWarn("Skipping subreddit after repeated listing failures", { sub, reason })
        break
      }

      const children = json?.data?.children || []
      for (const child of children) {
        const post = mapPost(child, sub)
        if (post) posts.push(post)
      }
      fetched += children.length
      after = json?.data?.after || null
      if (!after || children.length === 0) break
      await sleepWithJitter(500) // Increased delay with jitter to avoid detection
    }
  }

  if (posts.length === 0) {
    const failureSummary = failedSubreddits.map((entry) => `${entry.subreddit}:${entry.reason}`).join(", ")
    throw new Error(
      failureSummary.length > 0
        ? `Reddit dataset fetch produced no posts (${failureSummary})`
        : "Reddit dataset fetch produced no posts"
    )
  }

  if (failedSubreddits.length > 0) {
    logWarn("Reddit dataset completed with partial subreddit coverage", {
      failedSubreddits: failedSubreddits.map((entry) => entry.subreddit),
      requestedSubreddits: subreddits.length,
      collectedPosts: posts.length,
    })
  }

  if (cfg.enableComments) {
    for (const post of posts) {
      post.comments = await fetchCommentsWithFallback(post.permalink, cfg.maxCommentsPerPost, ua, token, publicBases)
      await sleepWithJitter(400) // Increased delay with jitter
    }
  }

  return {
    generated_at_utc: nowUtcIso(),
    subreddits,
    posts,
  }
}
