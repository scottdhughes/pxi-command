import { describe, it, expect } from "vitest"
import { extractTickers, scoreTickers, STOPLIST, type TickerStats } from "../../../src/analysis/tickers"
import { LIMITS } from "../../../src/analysis/constants"
import type { Doc } from "../../../src/analysis/metrics"
import { createDoc } from "../../fixtures/sample_data"

describe("STOPLIST", () => {
  describe("contains expected categories", () => {
    it("filters common English words", () => {
      expect(STOPLIST.has("THE")).toBe(true)
      expect(STOPLIST.has("AND")).toBe(true)
      expect(STOPLIST.has("FOR")).toBe(true)
      expect(STOPLIST.has("WITH")).toBe(true)
    })

    it("filters financial terms", () => {
      expect(STOPLIST.has("BUY")).toBe(true)
      expect(STOPLIST.has("SELL")).toBe(true)
      expect(STOPLIST.has("HOLD")).toBe(true)
      expect(STOPLIST.has("YOLO")).toBe(true)
      expect(STOPLIST.has("FOMO")).toBe(true)
    })

    it("filters market abbreviations", () => {
      expect(STOPLIST.has("ETF")).toBe(true)
      expect(STOPLIST.has("IPO")).toBe(true)
      expect(STOPLIST.has("SEC")).toBe(true)
      expect(STOPLIST.has("FOMC")).toBe(true)
    })

    it("filters currency codes", () => {
      expect(STOPLIST.has("USD")).toBe(true)
      expect(STOPLIST.has("EUR")).toBe(true)
      expect(STOPLIST.has("GBP")).toBe(true)
    })

    it("filters tech abbreviations", () => {
      expect(STOPLIST.has("AI")).toBe(true)
      expect(STOPLIST.has("CEO")).toBe(true)
      expect(STOPLIST.has("API")).toBe(true)
    })

    it("filters social media terms", () => {
      expect(STOPLIST.has("DD")).toBe(true)
      expect(STOPLIST.has("TLDR")).toBe(true)
      expect(STOPLIST.has("IMO")).toBe(true)
    })
  })

  describe("does not filter valid tickers", () => {
    it("allows common stock tickers", () => {
      expect(STOPLIST.has("TSLA")).toBe(false)
      expect(STOPLIST.has("AAPL")).toBe(false)
      expect(STOPLIST.has("NVDA")).toBe(false)
      expect(STOPLIST.has("MSFT")).toBe(false)
      expect(STOPLIST.has("AMZN")).toBe(false)
      expect(STOPLIST.has("GOOGL")).toBe(false)
    })

    it("allows sector-specific tickers", () => {
      expect(STOPLIST.has("XOM")).toBe(false)   // Oil
      expect(STOPLIST.has("JPM")).toBe(false)   // Banking
      expect(STOPLIST.has("PFE")).toBe(false)   // Pharma
      expect(STOPLIST.has("BA")).toBe(false)    // Aerospace
    })
  })
})

describe("extractTickers", () => {
  describe("basic extraction", () => {
    it("extracts $TICKER format", () => {
      const docs: Doc[] = [
        createDoc({ id: "1", text: "$TSLA is up today" }),
      ]
      const { stats } = extractTickers(docs)

      expect(stats.has("TSLA")).toBe(true)
      expect(stats.get("TSLA")?.count).toBe(1)
    })

    it("extracts plain TICKER format", () => {
      const docs: Doc[] = [
        createDoc({ id: "1", text: "TSLA beats earnings" }),
      ]
      const { stats } = extractTickers(docs)

      expect(stats.has("TSLA")).toBe(true)
    })

    it("extracts (TICKER) format", () => {
      const docs: Doc[] = [
        createDoc({ id: "1", text: "Tesla (TSLA) looks strong" }),
      ]
      const { stats } = extractTickers(docs)

      expect(stats.has("TSLA")).toBe(true)
    })

    it("counts multiple mentions across documents", () => {
      const docs: Doc[] = [
        createDoc({ id: "1", text: "$TSLA is up", subreddit: "stocks" }),
        createDoc({ id: "2", text: "TSLA beats", subreddit: "stocks" }),
        createDoc({ id: "3", text: "(TSLA) looks strong", subreddit: "investing" }),
      ]
      const { stats } = extractTickers(docs)

      expect(stats.get("TSLA")?.count).toBe(3)
    })

    it("tracks unique subreddits", () => {
      const docs: Doc[] = [
        createDoc({ id: "1", text: "$TSLA", subreddit: "stocks" }),
        createDoc({ id: "2", text: "$TSLA", subreddit: "stocks" }),
        createDoc({ id: "3", text: "$TSLA", subreddit: "investing" }),
        createDoc({ id: "4", text: "$TSLA", subreddit: "wallstreetbets" }),
      ]
      const { stats } = extractTickers(docs)

      expect(stats.get("TSLA")?.subreddits.size).toBe(3)
    })
  })

  describe("filtering", () => {
    it("filters out stopwords", () => {
      const docs: Doc[] = [
        createDoc({ id: "1", text: "THE BUY signal for ETF is strong" }),
      ]
      const { stats } = extractTickers(docs)

      expect(stats.has("THE")).toBe(false)
      expect(stats.has("BUY")).toBe(false)
      expect(stats.has("ETF")).toBe(false)
    })

    it("keeps valid tickers mixed with stopwords", () => {
      const docs: Doc[] = [
        createDoc({ id: "1", text: "THE $TSLA BUY signal is strong" }),
      ]
      const { stats } = extractTickers(docs)

      expect(stats.has("THE")).toBe(false)
      expect(stats.has("BUY")).toBe(false)
      expect(stats.has("TSLA")).toBe(true)
    })
  })

  describe("perDoc tracking", () => {
    it("tracks tickers per document", () => {
      const docs: Doc[] = [
        createDoc({ id: "doc1", text: "$TSLA $NVDA" }),
        createDoc({ id: "doc2", text: "$AAPL" }),
      ]
      const { perDoc } = extractTickers(docs)

      expect(perDoc.get("doc1")).toContain("TSLA")
      expect(perDoc.get("doc1")).toContain("NVDA")
      expect(perDoc.get("doc2")).toContain("AAPL")
    })

    it("returns empty array for docs with no tickers", () => {
      const docs: Doc[] = [
        createDoc({ id: "1", text: "No tickers here" }),
      ]
      const { perDoc } = extractTickers(docs)

      expect(perDoc.get("1")).toEqual([])
    })
  })

  describe("edge cases", () => {
    it("handles empty docs array", () => {
      const { stats, perDoc } = extractTickers([])
      expect(stats.size).toBe(0)
      expect(perDoc.size).toBe(0)
    })

    it("handles empty text", () => {
      const docs: Doc[] = [createDoc({ id: "1", text: "" })]
      const { stats } = extractTickers(docs)
      expect(stats.size).toBe(0)
    })

    it("handles text with no matches", () => {
      const docs: Doc[] = [createDoc({ id: "1", text: "lowercase only 123 !@#" })]
      const { stats } = extractTickers(docs)
      expect(stats.size).toBe(0)
    })

    it("handles duplicate tickers in same document", () => {
      const docs: Doc[] = [
        createDoc({ id: "1", text: "$TSLA is great, I love $TSLA, TSLA forever" }),
      ]
      const { stats, perDoc } = extractTickers(docs)

      // Count should include all mentions
      expect(stats.get("TSLA")?.count).toBe(3)
      // perDoc should include all mentions too
      expect(perDoc.get("1")?.filter(t => t === "TSLA").length).toBe(3)
    })

    it("handles 2-character tickers", () => {
      const docs: Doc[] = [
        createDoc({ id: "1", text: "$BA Boeing is up" }), // BA = Boeing
      ]
      const { stats } = extractTickers(docs)

      expect(stats.has("BA")).toBe(true)
    })

    it("handles 5-character tickers", () => {
      const docs: Doc[] = [
        createDoc({ id: "1", text: "$GOOGL is the class A shares" }),
      ]
      const { stats } = extractTickers(docs)

      expect(stats.has("GOOGL")).toBe(true)
    })

    it("ignores tickers longer than 5 characters", () => {
      const docs: Doc[] = [
        createDoc({ id: "1", text: "TOOLONG is not a ticker" }),
      ]
      const { stats } = extractTickers(docs)

      expect(stats.has("TOOLONG")).toBe(false)
    })

    it("ignores single character matches", () => {
      const docs: Doc[] = [
        createDoc({ id: "1", text: "I like A and B" }),
      ]
      const { stats } = extractTickers(docs)

      expect(stats.has("A")).toBe(false)
      expect(stats.has("B")).toBe(false)
    })
  })
})

describe("scoreTickers", () => {
  describe("scoring formula", () => {
    it("calculates score as count + (subreddit_count * weight)", () => {
      const stats = new Map<string, TickerStats>()
      stats.set("TSLA", {
        ticker: "TSLA",
        count: 10,
        subreddits: new Set(["stocks", "investing", "wallstreetbets"]),
      })

      const result = scoreTickers(stats)

      // score = 10 + (3 * LIMITS.subredditScoreWeight)
      expect(result[0].score).toBe(10 + 3 * LIMITS.subredditScoreWeight)
      expect(result[0].count).toBe(10)
      expect(result[0].subreddit_count).toBe(3)
    })

    it("weighs subreddit diversity", () => {
      const stats = new Map<string, TickerStats>()
      // High count, single subreddit
      stats.set("AAPL", {
        ticker: "AAPL",
        count: 20,
        subreddits: new Set(["stocks"]),
      })
      // Lower count, multiple subreddits
      stats.set("TSLA", {
        ticker: "TSLA",
        count: 10,
        subreddits: new Set(["stocks", "investing", "wsb", "options", "spacs"]),
      })

      const result = scoreTickers(stats)

      // TSLA: 10 + (5 * 2) = 20
      // AAPL: 20 + (1 * 2) = 22
      // AAPL should still be higher with these specific numbers
      expect(result[0].ticker).toBe("AAPL")
    })
  })

  describe("sorting", () => {
    it("sorts by score descending", () => {
      const stats = new Map<string, TickerStats>()
      stats.set("LOW", { ticker: "LOW", count: 1, subreddits: new Set(["a"]) })
      stats.set("HIGH", { ticker: "HIGH", count: 100, subreddits: new Set(["a", "b", "c"]) })
      stats.set("MED", { ticker: "MED", count: 10, subreddits: new Set(["a", "b"]) })

      const result = scoreTickers(stats)

      expect(result[0].ticker).toBe("HIGH")
      expect(result[1].ticker).toBe("MED")
      expect(result[2].ticker).toBe("LOW")
    })

    it("maintains stable order for equal scores", () => {
      const stats = new Map<string, TickerStats>()
      stats.set("A", { ticker: "A", count: 5, subreddits: new Set(["x"]) })
      stats.set("B", { ticker: "B", count: 5, subreddits: new Set(["y"]) })

      const result1 = scoreTickers(stats)
      const result2 = scoreTickers(stats)

      expect(result1.map(r => r.ticker)).toEqual(result2.map(r => r.ticker))
    })
  })

  describe("return structure", () => {
    it("includes all required fields", () => {
      const stats = new Map<string, TickerStats>()
      stats.set("TSLA", { ticker: "TSLA", count: 5, subreddits: new Set(["stocks"]) })

      const result = scoreTickers(stats)

      expect(result[0]).toHaveProperty("ticker")
      expect(result[0]).toHaveProperty("score")
      expect(result[0]).toHaveProperty("count")
      expect(result[0]).toHaveProperty("subreddit_count")
    })

    it("returns array", () => {
      const stats = new Map<string, TickerStats>()
      const result = scoreTickers(stats)
      expect(Array.isArray(result)).toBe(true)
    })

    it("returns empty array for empty stats", () => {
      const stats = new Map<string, TickerStats>()
      const result = scoreTickers(stats)
      expect(result).toEqual([])
    })
  })

  describe("edge cases", () => {
    it("handles single ticker", () => {
      const stats = new Map<string, TickerStats>()
      stats.set("ONLY", { ticker: "ONLY", count: 1, subreddits: new Set(["a"]) })

      const result = scoreTickers(stats)

      expect(result).toHaveLength(1)
      expect(result[0].ticker).toBe("ONLY")
    })

    it("handles ticker with zero count", () => {
      const stats = new Map<string, TickerStats>()
      stats.set("ZERO", { ticker: "ZERO", count: 0, subreddits: new Set() })

      const result = scoreTickers(stats)

      expect(result[0].score).toBe(0)
    })

    it("handles large number of tickers", () => {
      const stats = new Map<string, TickerStats>()
      for (let i = 0; i < 1000; i++) {
        stats.set(`T${i}`, { ticker: `T${i}`, count: i, subreddits: new Set(["a"]) })
      }

      const result = scoreTickers(stats)

      expect(result).toHaveLength(1000)
      expect(result[0].ticker).toBe("T999") // Highest count
    })
  })
})

describe("integration: extractTickers + scoreTickers", () => {
  it("processes real-world-like data correctly", () => {
    const docs: Doc[] = [
      createDoc({ id: "1", text: "$TSLA rallying today!", subreddit: "stocks" }),
      createDoc({ id: "2", text: "NVDA and TSLA looking good", subreddit: "stocks" }),
      createDoc({ id: "3", text: "Time to buy $TSLA?", subreddit: "investing" }),
      createDoc({ id: "4", text: "AMD vs NVDA comparison", subreddit: "wallstreetbets" }),
      createDoc({ id: "5", text: "$NVDA earnings soon", subreddit: "options" }),
    ]

    const { stats } = extractTickers(docs)
    const scored = scoreTickers(stats)

    // TSLA: 3 mentions, 2 subreddits = 3 + 2*2 = 7
    // NVDA: 3 mentions, 3 subreddits = 3 + 3*2 = 9
    // AMD: 1 mention, 1 subreddit = 1 + 1*2 = 3

    expect(scored[0].ticker).toBe("NVDA")
    expect(scored[1].ticker).toBe("TSLA")
    expect(scored[2].ticker).toBe("AMD")
  })
})
