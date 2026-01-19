import { describe, it, expect } from "vitest"
import {
  RedditPostDataSchema,
  RedditCommentDataSchema,
  RedditListingSchema,
  RedditOAuthResponseSchema,
  parseRedditListing,
  parseOAuthResponse,
} from "../../src/reddit/schemas"

describe("RedditPostDataSchema", () => {
  describe("valid input", () => {
    it("parses minimal valid post", () => {
      const input = {
        id: "abc123",
        created_utc: 1700000000,
        title: "Test Post",
        selftext: "Post body",
        permalink: "/r/stocks/comments/abc123/test_post",
        score: 42,
        num_comments: 10,
      }

      const result = RedditPostDataSchema.safeParse(input)

      expect(result.success).toBe(true)
      if (result.success) {
        expect(result.data.id).toBe("abc123")
        expect(result.data.title).toBe("Test Post")
      }
    })

    it("applies defaults for optional fields", () => {
      const input = {
        id: "abc123",
        created_utc: 1700000000,
        title: "Test",
        permalink: "/r/stocks/comments/abc123/test",
      }

      const result = RedditPostDataSchema.safeParse(input)

      expect(result.success).toBe(true)
      if (result.success) {
        expect(result.data.selftext).toBe("")
        expect(result.data.score).toBe(0)
        expect(result.data.num_comments).toBe(0)
      }
    })

    it("handles empty selftext", () => {
      const input = {
        id: "abc123",
        created_utc: 1700000000,
        title: "Test",
        selftext: "",
        permalink: "/r/stocks/comments/abc123/test",
        score: 0,
        num_comments: 0,
      }

      const result = RedditPostDataSchema.safeParse(input)
      expect(result.success).toBe(true)
    })
  })

  describe("invalid input", () => {
    it("rejects missing id", () => {
      const input = {
        created_utc: 1700000000,
        title: "Test",
        permalink: "/r/stocks/test",
      }

      const result = RedditPostDataSchema.safeParse(input)
      expect(result.success).toBe(false)
    })

    it("rejects missing created_utc", () => {
      const input = {
        id: "abc123",
        title: "Test",
        permalink: "/r/stocks/test",
      }

      const result = RedditPostDataSchema.safeParse(input)
      expect(result.success).toBe(false)
    })

    it("rejects invalid permalink format", () => {
      const input = {
        id: "abc123",
        created_utc: 1700000000,
        title: "Test",
        permalink: "https://example.com/not-reddit", // doesn't start with /r/
      }

      const result = RedditPostDataSchema.safeParse(input)
      expect(result.success).toBe(false)
    })

    it("rejects non-numeric created_utc", () => {
      const input = {
        id: "abc123",
        created_utc: "not a number",
        title: "Test",
        permalink: "/r/stocks/test",
      }

      const result = RedditPostDataSchema.safeParse(input)
      expect(result.success).toBe(false)
    })
  })
})

describe("RedditCommentDataSchema", () => {
  it("parses valid comment", () => {
    const input = {
      id: "xyz789",
      created_utc: 1700000000,
      body: "Great analysis!",
      permalink: "/r/stocks/comments/abc123/test/xyz789",
    }

    const result = RedditCommentDataSchema.safeParse(input)

    expect(result.success).toBe(true)
    if (result.success) {
      expect(result.data.body).toBe("Great analysis!")
    }
  })

  it("rejects missing body", () => {
    const input = {
      id: "xyz789",
      created_utc: 1700000000,
      permalink: "/r/stocks/comments/abc123/test/xyz789",
    }

    const result = RedditCommentDataSchema.safeParse(input)
    expect(result.success).toBe(false)
  })
})

describe("RedditListingSchema", () => {
  it("parses valid listing", () => {
    const input = {
      kind: "Listing",
      data: {
        children: [
          {
            kind: "t3",
            data: {
              id: "abc123",
              created_utc: 1700000000,
              title: "Test Post",
              selftext: "Body",
              permalink: "/r/stocks/comments/abc123/test",
              score: 10,
              num_comments: 5,
            },
          },
        ],
        after: "t3_nextpage",
      },
    }

    const result = RedditListingSchema.safeParse(input)

    expect(result.success).toBe(true)
    if (result.success) {
      expect(result.data.data.children).toHaveLength(1)
      expect(result.data.data.after).toBe("t3_nextpage")
    }
  })

  it("parses listing without kind field", () => {
    const input = {
      data: {
        children: [],
        after: null,
      },
    }

    const result = RedditListingSchema.safeParse(input)
    expect(result.success).toBe(true)
  })

  it("parses listing with null after", () => {
    const input = {
      data: {
        children: [],
        after: null,
      },
    }

    const result = RedditListingSchema.safeParse(input)

    expect(result.success).toBe(true)
    if (result.success) {
      expect(result.data.data.after).toBeNull()
    }
  })

  it("parses listing with multiple children", () => {
    const input = {
      data: {
        children: [
          { kind: "t3", data: { id: "1", created_utc: 1700000000, title: "A", permalink: "/r/a/b", score: 0, num_comments: 0 } },
          { kind: "t3", data: { id: "2", created_utc: 1700000001, title: "B", permalink: "/r/a/c", score: 0, num_comments: 0 } },
          { kind: "t3", data: { id: "3", created_utc: 1700000002, title: "C", permalink: "/r/a/d", score: 0, num_comments: 0 } },
        ],
        after: null,
      },
    }

    const result = RedditListingSchema.safeParse(input)

    expect(result.success).toBe(true)
    if (result.success) {
      expect(result.data.data.children).toHaveLength(3)
    }
  })
})

describe("RedditOAuthResponseSchema", () => {
  it("parses valid OAuth response", () => {
    const input = {
      access_token: "abc123xyz",
      token_type: "bearer",
      expires_in: 86400,
      scope: "read",
    }

    const result = RedditOAuthResponseSchema.safeParse(input)

    expect(result.success).toBe(true)
    if (result.success) {
      expect(result.data.access_token).toBe("abc123xyz")
      expect(result.data.token_type).toBe("bearer")
    }
  })

  it("rejects missing access_token", () => {
    const input = {
      token_type: "bearer",
      expires_in: 86400,
    }

    const result = RedditOAuthResponseSchema.safeParse(input)
    expect(result.success).toBe(false)
  })

  it("rejects missing token_type", () => {
    const input = {
      access_token: "abc123",
      expires_in: 86400,
    }

    const result = RedditOAuthResponseSchema.safeParse(input)
    expect(result.success).toBe(false)
  })
})

describe("parseRedditListing helper", () => {
  it("returns parsed data for valid input", () => {
    const input = {
      data: {
        children: [],
        after: null,
      },
    }

    const result = parseRedditListing(input)

    expect(result).not.toBeNull()
    expect(result?.data.children).toEqual([])
  })

  it("returns null for invalid input", () => {
    const result = parseRedditListing({ invalid: "data" })
    expect(result).toBeNull()
  })

  it("returns null for non-object input", () => {
    expect(parseRedditListing(null)).toBeNull()
    expect(parseRedditListing(undefined)).toBeNull()
    expect(parseRedditListing("string")).toBeNull()
    expect(parseRedditListing(123)).toBeNull()
  })
})

describe("parseOAuthResponse helper", () => {
  it("returns parsed data for valid input", () => {
    const input = {
      access_token: "token123",
      token_type: "bearer",
      expires_in: 3600,
      scope: "read",
    }

    const result = parseOAuthResponse(input)

    expect(result).not.toBeNull()
    expect(result?.access_token).toBe("token123")
  })

  it("returns null for invalid input", () => {
    const result = parseOAuthResponse({ invalid: "data" })
    expect(result).toBeNull()
  })

  it("returns null for error response", () => {
    // Reddit returns error responses like this
    const errorResponse = {
      error: "invalid_grant",
      message: "Invalid credentials",
    }

    const result = parseOAuthResponse(errorResponse)
    expect(result).toBeNull()
  })
})

describe("real Reddit API response simulation", () => {
  it("handles typical r/stocks listing response", () => {
    const mockApiResponse = {
      kind: "Listing",
      data: {
        modhash: "",
        dist: 2,
        children: [
          {
            kind: "t3",
            data: {
              id: "1a2b3c",
              created_utc: 1700000000,
              title: "$TSLA Analysis - My DD",
              selftext: "Here's why I think Tesla will moon...",
              permalink: "/r/stocks/comments/1a2b3c/tsla_analysis_my_dd/",
              score: 150,
              num_comments: 42,
              author: "investor123",
              subreddit: "stocks",
            },
          },
          {
            kind: "t3",
            data: {
              id: "4d5e6f",
              created_utc: 1700000100,
              title: "Weekly Discussion Thread",
              selftext: "",
              permalink: "/r/stocks/comments/4d5e6f/weekly_discussion_thread/",
              score: 50,
              num_comments: 200,
              author: "AutoModerator",
              subreddit: "stocks",
            },
          },
        ],
        after: "t3_7g8h9i",
        before: null,
      },
    }

    const result = RedditListingSchema.safeParse(mockApiResponse)

    expect(result.success).toBe(true)
    if (result.success) {
      expect(result.data.data.children).toHaveLength(2)
      expect(result.data.data.children[0].data.title).toBe("$TSLA Analysis - My DD")
      expect(result.data.data.after).toBe("t3_7g8h9i")
    }
  })

  it("handles empty subreddit response", () => {
    const emptyResponse = {
      kind: "Listing",
      data: {
        children: [],
        after: null,
        before: null,
      },
    }

    const result = RedditListingSchema.safeParse(emptyResponse)

    expect(result.success).toBe(true)
    if (result.success) {
      expect(result.data.data.children).toHaveLength(0)
      expect(result.data.data.after).toBeNull()
    }
  })
})
