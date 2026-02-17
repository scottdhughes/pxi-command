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

afterEach(() => {
  globalThis.fetch = realFetch
})

describe("reddit client resilience", () => {
  it("retries rate-limited listing requests and succeeds", async () => {
    const env = createMockEnv()
    let attempts = 0

    const fetchMock = vi.fn(async (input: RequestInfo | URL): Promise<Response> => {
      const url = resolveUrl(input)
      if (!url.includes("/r/stocks/new.json")) {
        throw new Error(`Unexpected URL in rate-limit test: ${url}`)
      }

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
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it("continues processing when one subreddit repeatedly fails", async () => {
    const env = createMockEnv()

    const fetchMock = vi.fn(async (input: RequestInfo | URL): Promise<Response> => {
      const url = resolveUrl(input)

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
    expect(fetchMock.mock.calls.length).toBeGreaterThanOrEqual(4)
  })

  it("throws a dataset error when all subreddits fail", async () => {
    const env = createMockEnv()

    const fetchMock = vi.fn(async (): Promise<Response> => {
      return new Response("forbidden", { status: 403 })
    })

    globalThis.fetch = fetchMock as unknown as typeof fetch

    await expect(fetchRedditDataset(env, ["fail-a", "fail-b"])).rejects.toThrow(
      "Reddit dataset fetch produced no posts"
    )
    expect(fetchMock).toHaveBeenCalled()
  })
})
