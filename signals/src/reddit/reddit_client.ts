import type { Env } from "../config"
import { getConfig } from "../config"
import type { RedditComment, RedditDataset, RedditPost } from "./types"
import { nowUtcIso } from "../utils/time"
import { parseRedditListing, parseOAuthResponse } from "./schemas"
import { logWarn } from "../utils/logger"

const REDDIT_BASE = "https://oauth.reddit.com"
const REDDIT_PUBLIC = "https://www.reddit.com"

async function sleep(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

async function fetchWithBackoff(input: RequestInfo, init: RequestInit, maxRetries = 3) {
  let attempt = 0
  while (attempt <= maxRetries) {
    const res = await fetch(input, init)
    if (res.status !== 429 && res.status < 500) {
      return res
    }
    const waitMs = 500 * Math.pow(2, attempt)
    await sleep(waitMs)
    attempt += 1
  }
  return fetch(input, init)
}

async function getOAuthToken(env: Env): Promise<string | null> {
  const id = env.REDDIT_CLIENT_ID
  const secret = env.REDDIT_CLIENT_SECRET
  const ua = env.REDDIT_USER_AGENT
  if (!id || !secret || !ua) return null

  const body = new URLSearchParams({
    grant_type: "client_credentials",
  })

  const basic = btoa(`${id}:${secret}`)
  const res = await fetch("https://www.reddit.com/api/v1/access_token", {
    method: "POST",
    headers: {
      Authorization: `Basic ${basic}`,
      "User-Agent": ua,
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
    logWarn("OAuth response validation failed", { response: json })
    return null
  }

  return parsed.access_token
}

async function fetchListing(url: string, headers: Record<string, string>) {
  const res = await fetchWithBackoff(url, { headers })
  if (!res.ok) {
    throw new Error(`Reddit fetch failed: ${res.status}`)
  }
  const json = await res.json()
  const parsed = parseRedditListing(json)
  if (!parsed) {
    logWarn("Reddit listing validation failed, returning empty result", { url })
    return { data: { children: [], after: null } }
  }
  return parsed
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

async function fetchComments(permalink: string, headers: Record<string, string>, maxComments: number): Promise<RedditComment[]> {
  const url = `${permalink}.json?limit=${maxComments}`
  const res = await fetchWithBackoff(url, { headers })
  if (!res.ok) {
    logWarn("Failed to fetch comments", {
      permalink,
      status: res.status,
      statusText: res.statusText,
    })
    return []
  }
  const json = (await res.json()) as unknown[]
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

export async function fetchRedditDataset(env: Env, subreddits: string[]): Promise<RedditDataset> {
  const cfg = getConfig(env)
  const ua = env.REDDIT_USER_AGENT || "pxi-signals/0.1"
  const token = await getOAuthToken(env)
  const headers: Record<string, string> = {
    "User-Agent": ua,
  }
  const base = token ? REDDIT_BASE : REDDIT_PUBLIC
  if (token) headers.Authorization = `Bearer ${token}`

  const posts: RedditPost[] = []
  for (const sub of subreddits) {
    let after: string | null = null
    let fetched = 0
    while (fetched < cfg.maxPostsPerSubreddit) {
      const limit = Math.min(100, cfg.maxPostsPerSubreddit - fetched)
      const url = `${base}/r/${sub}/new.json?limit=${limit}${after ? `&after=${after}` : ""}`
      const json = await fetchListing(url, headers)
      const children = json?.data?.children || []
      for (const child of children) {
        const post = mapPost(child, sub)
        if (post) posts.push(post)
      }
      fetched += children.length
      after = json?.data?.after || null
      if (!after || children.length === 0) break
      await sleep(250)
    }
  }

  if (cfg.enableComments) {
    for (const post of posts) {
      post.comments = await fetchComments(post.permalink, headers, cfg.maxCommentsPerPost)
      await sleep(200)
    }
  }

  return {
    generated_at_utc: nowUtcIso(),
    subreddits,
    posts,
  }
}
