import { describe, it, expect } from "vitest"
import { extractTickers } from "../src/analysis/tickers"
import type { Doc } from "../src/analysis/metrics"

describe("ticker extraction", () => {
  it("extracts $TSLA, TSLA, (TSLA) patterns", () => {
    const docs: Doc[] = [
      { id: "1", subreddit: "stocks", created_utc: 0, text: "$TSLA is up", permalink: "", score: 0, num_comments: 0, post_id: "1", is_comment: false },
      { id: "2", subreddit: "stocks", created_utc: 0, text: "TSLA beats", permalink: "", score: 0, num_comments: 0, post_id: "2", is_comment: false },
      { id: "3", subreddit: "stocks", created_utc: 0, text: "(TSLA) looks strong", permalink: "", score: 0, num_comments: 0, post_id: "3", is_comment: false },
    ]
    const { stats } = extractTickers(docs)
    expect(stats.get("TSLA")?.count).toBe(3)
  })
})
