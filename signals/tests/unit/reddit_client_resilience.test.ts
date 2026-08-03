import { afterEach, describe, expect, it, vi } from "vitest"
import { fetchRedditDataset } from "../../src/reddit/reddit_client"
import { createMockEnv } from "../fixtures/mock_env"

const realFetch = globalThis.fetch

function resolveUrl(input: RequestInfo | URL): string {
  if (typeof input === "string") return input
  if (input instanceof URL) return input.toString()
  return input.url
}

function listingResponse(subreddit: string, id: string): Response {
  return new Response(
    JSON.stringify({
      kind: "Listing",
      data: {
        children: [
          {
            kind: "t3",
            data: {
              id,
              created_utc: 1_700_000_000,
              title: `${subreddit} post`,
              selftext: "",
              permalink: `/r/${subreddit}/comments/${id}/post/`,
              score: 1,
              num_comments: 0,
            },
          },
        ],
        after: null,
        before: null,
      },
    }),
    {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }
  )
}

function oauthResponse(): Response {
  return new Response(
    JSON.stringify({
      access_token: "test-access-token",
      token_type: "bearer",
      expires_in: 3600,
      scope: "read",
    }),
    { status: 200, headers: { "Content-Type": "application/json" } }
  )
}

function createRedditEnv() {
  return createMockEnv({
    REDDIT_CLIENT_ID: "test-client-id",
    REDDIT_CLIENT_SECRET: "test-client-secret",
    REDDIT_USER_AGENT: "web:pxi-signals:test (by /u/pxi_command)",
  })
}

afterEach(() => {
  globalThis.fetch = realFetch
})

describe("reddit client resilience", () => {
  it("retries rate-limited listing requests and succeeds", async () => {
    const env = createRedditEnv()
    let attempts = 0

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = resolveUrl(input)
      if (url.endsWith("/api/v1/access_token")) return oauthResponse()
      if (!url.includes("/r/stocks/new.json")) {
        throw new Error(`Unexpected URL in rate-limit test: ${url}`)
      }

      expect(url.startsWith("https://oauth.reddit.com/"), url).toBe(true)
      expect(new Headers(init?.headers).get("Authorization")).toBe("Bearer test-access-token")
      expect(new Headers(init?.headers).get("Sec-CH-UA")).toBeNull()

      attempts += 1
      if (attempts === 1) {
        return new Response("rate limited", {
          status: 429,
          headers: { "Retry-After": "0" },
        })
      }

      return listingResponse("stocks", "abc123")
    })

    globalThis.fetch = fetchMock as unknown as typeof fetch

    const dataset = await fetchRedditDataset(env, ["stocks"])
    expect(dataset.posts).toHaveLength(1)
    expect(dataset.posts[0]?.subreddit).toBe("stocks")
    expect(fetchMock).toHaveBeenCalledTimes(3)
  })

  it("continues processing when one subreddit repeatedly fails", async () => {
    const env = createRedditEnv()

    const fetchMock = vi.fn(async (input: RequestInfo | URL): Promise<Response> => {
      const url = resolveUrl(input)
      if (url.endsWith("/api/v1/access_token")) return oauthResponse()

      if (url.includes("/r/fail/new.json")) {
        return new Response("forbidden", { status: 403 })
      }

      if (url.includes("/r/stocks/new.json")) {
        return listingResponse("stocks", "ok456")
      }

      throw new Error(`Unexpected URL in partial-failure test: ${url}`)
    })

    globalThis.fetch = fetchMock as unknown as typeof fetch

    const dataset = await fetchRedditDataset(env, ["fail", "stocks"])
    expect(dataset.posts).toHaveLength(1)
    expect(dataset.posts[0]?.subreddit).toBe("stocks")
    expect(fetchMock).toHaveBeenCalledTimes(3)
  })

  it("throws a dataset error when all subreddits fail", async () => {
    const env = createRedditEnv()

    const fetchMock = vi.fn(async (input: RequestInfo | URL): Promise<Response> => {
      if (resolveUrl(input).endsWith("/api/v1/access_token")) return oauthResponse()
      return new Response("forbidden", { status: 403 })
    })

    globalThis.fetch = fetchMock as unknown as typeof fetch

    await expect(fetchRedditDataset(env, ["fail-a", "fail-b"])).rejects.toThrow(
      "Reddit dataset fetch produced no posts"
    )
    expect(fetchMock).toHaveBeenCalled()
  })

  it("fails clearly when OAuth credentials are missing", async () => {
    const fetchMock = vi.fn()
    globalThis.fetch = fetchMock as unknown as typeof fetch

    await expect(fetchRedditDataset(createMockEnv(), ["stocks"])).rejects.toMatchObject({
      code: "REDDIT_API_ERROR",
      message: "Reddit OAuth credentials are not configured",
      context: {
        missingBindings: ["REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET"],
      },
    })
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it("does not fall back to public scraping when OAuth fails", async () => {
    const fetchMock = vi.fn(async (): Promise<Response> => {
      return new Response("unauthorized", { status: 401 })
    })
    globalThis.fetch = fetchMock as unknown as typeof fetch

    await expect(fetchRedditDataset(createRedditEnv(), ["stocks"])).rejects.toMatchObject({
      code: "REDDIT_API_ERROR",
      message: "Reddit OAuth token request failed: 401",
    })
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })
})
