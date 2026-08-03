import type { Env } from "../config"
import { getConfig } from "../config"
import type { RedditComment, RedditDataset, RedditPost } from "./types"
import { nowUtcIso } from "../utils/time"
import { parseRedditListing, parseOAuthResponse } from "./schemas"
import { logWarn } from "../utils/logger"
import { RedditAPIError } from "../errors"

const REDDIT_BASE = "https://oauth.reddit.com"
const REDDIT_PUBLIC = "https://www.reddit.com"

// Reddit-compliant User-Agent format: <platform>:<app_id>:<version> (by /u/<username>)
const DEFAULT_USER_AGENT = "web:pxi-signals:1.0.0 (by /u/pxi_command)"
const RETRYABLE_HTTP_STATUSES = new Set([429, 500, 502, 503, 504, 522, 523, 524])
const MAX_RETRY_DELAY_MS = 15_000
const MAX_RSS_RESPONSE_BYTES = 1_500_000
const MAX_RSS_POSTS_PER_SUBREDDIT = 100
const RSS_REQUEST_SPACING_MS = 6_500
const REDDIT_HOSTNAMES = new Set(["reddit.com", "www.reddit.com"])

function getApiHeaders(userAgent: string, token?: string): Record<string, string> {
  return {
    "User-Agent": userAgent,
    Accept: "application/json",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
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

function getRetryDelayMs(attempt: number, headers?: Headers, status?: number): number {
  const retryAfter = headers ? parseRetryAfterMs(headers) : null
  if (retryAfter !== null) return retryAfter
  if (status === 429) {
    return Math.min(MAX_RETRY_DELAY_MS, 6_000 * Math.pow(2, attempt))
  }
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

      const waitMs = getRetryDelayMs(attempt, res.headers, res.status)
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

async function getOAuthToken(env: Env): Promise<string> {
  const id = env.REDDIT_CLIENT_ID
  const secret = env.REDDIT_CLIENT_SECRET
  const ua = env.REDDIT_USER_AGENT || DEFAULT_USER_AGENT
  if (!id || !secret) {
    throw new RedditAPIError("Reddit OAuth credentials are not configured", {
      missingBindings: [
        ...(!id ? ["REDDIT_CLIENT_ID"] : []),
        ...(!secret ? ["REDDIT_CLIENT_SECRET"] : []),
      ],
    })
  }

  const body = new URLSearchParams({
    grant_type: "client_credentials",
  })

  const basic = btoa(`${id}:${secret}`)
  const res = await fetchWithBackoff("https://www.reddit.com/api/v1/access_token", {
    method: "POST",
    headers: {
      ...getApiHeaders(ua),
      Authorization: `Basic ${basic}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: body.toString(),
  })

  if (!res.ok) {
    throw new RedditAPIError(`Reddit OAuth token request failed: ${res.status}`, {
      status: res.status,
      statusText: res.statusText,
    })
  }

  const json = await res.json()
  const parsed = parseOAuthResponse(json)
  if (!parsed) {
    // Log only non-sensitive fields - never log tokens or full OAuth responses
    logWarn("OAuth response validation failed", {
      hasAccessToken: typeof json === "object" && json !== null && "access_token" in json,
      responseKeys: typeof json === "object" && json !== null ? Object.keys(json) : [],
    })
    throw new RedditAPIError("Reddit OAuth response validation failed")
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

function resolveListingUrl(sub: string, limit: number, after: string | null): string {
  return `${REDDIT_BASE}/r/${sub}/new.json?limit=${limit}${after ? `&after=${after}` : ""}`
}

function resolveCommentUrl(permalink: string, maxComments: number): string {
  const path = getRedditPathFromPermalink(permalink)
  return `${REDDIT_BASE}${path}.json?limit=${maxComments}`
}

function decodeXmlEntities(value: string): string {
  const named: Record<string, string> = {
    amp: "&",
    apos: "'",
    gt: ">",
    lt: "<",
    quot: '"',
  }

  return value.replace(/&(#x[0-9a-f]+|#\d+|amp|apos|gt|lt|quot);/gi, (match, entity: string) => {
    if (entity.startsWith("#x")) {
      const codePoint = Number.parseInt(entity.slice(2), 16)
      return Number.isFinite(codePoint) && codePoint <= 0x10ffff ? String.fromCodePoint(codePoint) : match
    }
    if (entity.startsWith("#")) {
      const codePoint = Number.parseInt(entity.slice(1), 10)
      return Number.isFinite(codePoint) && codePoint <= 0x10ffff ? String.fromCodePoint(codePoint) : match
    }
    return named[entity.toLowerCase()] ?? match
  })
}

function readAtomElement(entry: string, name: string): string | null {
  const match = entry.match(new RegExp(`<${name}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${name}>`, "i"))
  return match ? match[1] : null
}

function stripHtml(value: string): string {
  const decoded = decodeXmlEntities(value)
  const withoutAttribution = decoded.split(/\s+submitted by\s+/i, 1)[0]
  return decodeXmlEntities(
    withoutAttribution
      .replace(/<!--[\s\S]*?-->/g, " ")
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<\/p>/gi, "\n")
      .replace(/<[^>]+>/g, " ")
  )
    .replace(/[ \t]+/g, " ")
    .replace(/\n\s+/g, "\n")
    .trim()
}

function parseRssPosts(xml: string, subreddit: string): RedditPost[] {
  const posts: RedditPost[] = []
  const entries = xml.match(/<entry>[\s\S]*?<\/entry>/gi) ?? []

  for (const entry of entries) {
    const rawId = readAtomElement(entry, "id")?.trim()
    const rawTitle = readAtomElement(entry, "title")
    const rawPublished = readAtomElement(entry, "published") ?? readAtomElement(entry, "updated")
    const rawContent = readAtomElement(entry, "content") ?? ""
    const linkMatch = entry.match(/<link\s+[^>]*href=["']([^"']+)["'][^>]*\/?\s*>/i)
    if (!rawId || rawTitle === null || !rawPublished || !linkMatch) continue

    const createdMs = Date.parse(rawPublished.trim())
    if (!Number.isFinite(createdMs)) continue

    let link: URL
    try {
      link = new URL(decodeXmlEntities(linkMatch[1]))
    } catch {
      continue
    }

    const expectedPrefix = `/r/${subreddit}/comments/`
    if (!REDDIT_HOSTNAMES.has(link.hostname) || !link.pathname.startsWith(expectedPrefix)) {
      continue
    }

    posts.push({
      id: rawId.replace(/^t3_/, ""),
      subreddit,
      created_utc: Math.floor(createdMs / 1000),
      title: decodeXmlEntities(rawTitle).trim(),
      selftext: stripHtml(rawContent),
      permalink: `https://reddit.com${link.pathname}`,
      score: 0,
      num_comments: 0,
    })
  }

  return posts
}

async function fetchRssPosts(subreddit: string, userAgent: string, limit: number): Promise<RedditPost[]> {
  const boundedLimit = Math.min(Math.max(1, limit), MAX_RSS_POSTS_PER_SUBREDDIT)
  const url = `${REDDIT_PUBLIC}/r/${subreddit}/new.rss?limit=${boundedLimit}`
  const res = await fetchWithBackoff(url, {
    headers: {
      "User-Agent": userAgent,
      Accept: "application/atom+xml, application/xml;q=0.9",
    },
  })
  if (!res.ok) {
    throw new RedditAPIError(`Reddit RSS request failed: ${res.status}`, {
      status: res.status,
      subreddit,
    })
  }

  const contentLength = Number(res.headers.get("Content-Length"))
  if (Number.isFinite(contentLength) && contentLength > MAX_RSS_RESPONSE_BYTES) {
    throw new RedditAPIError("Reddit RSS response exceeded size limit", { subreddit, contentLength })
  }

  const xml = await res.text()
  if (new TextEncoder().encode(xml).byteLength > MAX_RSS_RESPONSE_BYTES) {
    throw new RedditAPIError("Reddit RSS response exceeded size limit", { subreddit })
  }

  return parseRssPosts(xml, subreddit)
}

async function fetchRedditRssDataset(
  env: Env,
  subreddits: string[],
  userAgent: string
): Promise<RedditDataset> {
  const cfg = getConfig(env)
  const posts: RedditPost[] = []
  const failedSubreddits: Array<{ subreddit: string; reason: string }> = []

  for (const [index, sub] of subreddits.entries()) {
    try {
      posts.push(...(await fetchRssPosts(sub, userAgent, cfg.maxPostsPerSubreddit)))
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err)
      failedSubreddits.push({ subreddit: sub, reason })
      logWarn("Skipping subreddit after RSS failure", { sub, reason })
    }
    if (index < subreddits.length - 1) {
      await sleepWithJitter(RSS_REQUEST_SPACING_MS)
    }
  }

  if (posts.length === 0) {
    const failureSummary = failedSubreddits.map((entry) => `${entry.subreddit}:${entry.reason}`).join(", ")
    throw new Error(
      failureSummary.length > 0
        ? `Reddit RSS dataset fetch produced no posts (${failureSummary})`
        : "Reddit RSS dataset fetch produced no posts"
    )
  }

  if (failedSubreddits.length > 0) {
    logWarn("Reddit RSS dataset completed with partial subreddit coverage", {
      failedSubreddits: failedSubreddits.map((entry) => entry.subreddit),
      requestedSubreddits: subreddits.length,
      collectedPosts: posts.length,
    })
  }

  if (cfg.enableComments) {
    logWarn("Reddit comments are unavailable in RSS ingestion mode")
  }

  return {
    generated_at_utc: nowUtcIso(),
    subreddits,
    posts,
  }
}

async function fetchListing(
  sub: string,
  limit: number,
  after: string | null,
  ua: string,
  token: string
) {
  const url = resolveListingUrl(sub, limit, after)
  const res = await fetchWithBackoff(url, { headers: getApiHeaders(ua, token) })
  if (!res.ok) {
    throw new RedditAPIError(`Reddit listing request failed: ${res.status}`, {
      status: res.status,
      subreddit: sub,
    })
  }

  let json: unknown
  try {
    json = await res.json()
  } catch {
    throw new RedditAPIError("Reddit listing response was not valid JSON", { subreddit: sub })
  }

  const parsed = parseRedditListing(json)
  if (!parsed) {
    throw new RedditAPIError("Reddit listing response validation failed", { subreddit: sub })
  }
  return parsed
}

async function fetchComments(
  permalink: string,
  maxComments: number,
  ua: string,
  token: string
) {
  const url = resolveCommentUrl(permalink, maxComments)
  let res: Response
  try {
    res = await fetchWithBackoff(url, { headers: getApiHeaders(ua, token) })
  } catch (err) {
    logWarn("Comment request failed with network error", { permalink, error: String(err) })
    return []
  }

  if (!res.ok) {
    logWarn("Comment request failed", { permalink, status: res.status })
    return []
  }

  let json: unknown
  try {
    json = await res.json()
  } catch {
    logWarn("Comment response body parse failed", { permalink })
    return []
  }

  if (!Array.isArray(json) || json.length < 2) {
    logWarn("Unexpected comment response format", { permalink })
    return []
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
  if (!env.REDDIT_CLIENT_ID && !env.REDDIT_CLIENT_SECRET && cfg.enableRss) {
    return fetchRedditRssDataset(env, subreddits, ua)
  }
  const token = await getOAuthToken(env)

  const posts: RedditPost[] = []
  const failedSubreddits: Array<{ subreddit: string; reason: string }> = []
  for (const sub of subreddits) {
    let after: string | null = null
    let fetched = 0
    while (fetched < cfg.maxPostsPerSubreddit) {
      const limit = Math.min(100, cfg.maxPostsPerSubreddit - fetched)
      let json: Awaited<ReturnType<typeof fetchListing>>
      try {
        json = await fetchListing(sub, limit, after, ua, token)
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
      await sleepWithJitter(500) // Pace paginated API requests.
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
      post.comments = await fetchComments(post.permalink, cfg.maxCommentsPerPost, ua, token)
      await sleepWithJitter(400) // Pace comment API requests.
    }
  }

  return {
    generated_at_utc: nowUtcIso(),
    subreddits,
    posts,
  }
}
