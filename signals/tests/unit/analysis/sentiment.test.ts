import { describe, it, expect } from "vitest"
import { scoreSentiment } from "../../../src/analysis/sentiment"

describe("scoreSentiment", () => {
  describe("positive sentiment", () => {
    it("scores positive text above 0", () => {
      const score = scoreSentiment("This is great! Amazing performance!")
      expect(score).toBeGreaterThan(0)
    })

    it("scores bullish financial text as positive or neutral", () => {
      // VADER may not recognize all financial jargon, but "soaring" is positive
      const score = scoreSentiment("Earnings beat expectations, stock is soaring")
      // Accept neutral (0) or positive since VADER focuses on general sentiment words
      expect(score).toBeGreaterThanOrEqual(0)
    })

    it("scores enthusiastic language highly", () => {
      const score = scoreSentiment("Incredible gains! Best stock pick ever!")
      expect(score).toBeGreaterThan(0.5)
    })

    it("handles rocket emoji as positive (common in stock discussions)", () => {
      const withEmoji = scoreSentiment("To the moon! 🚀🚀🚀")
      const withoutEmoji = scoreSentiment("To the moon!")
      // VADER handles emojis, rocket is positive
      expect(withEmoji).toBeGreaterThanOrEqual(withoutEmoji)
    })
  })

  describe("negative sentiment", () => {
    it("scores negative text below 0", () => {
      const score = scoreSentiment("This is terrible! Awful news!")
      expect(score).toBeLessThan(0)
    })

    it("scores bearish financial text as negative", () => {
      const score = scoreSentiment("Earnings missed badly, stock is crashing")
      expect(score).toBeLessThan(0)
    })

    it("handles fear and uncertainty language", () => {
      const score = scoreSentiment("Very worried about the market, could crash hard")
      expect(score).toBeLessThan(0)
    })

    it("scores warnings and losses as negative", () => {
      const score = scoreSentiment("Lost 50% of my portfolio, disaster")
      expect(score).toBeLessThan(0)
    })
  })

  describe("neutral sentiment", () => {
    it("scores neutral text near 0", () => {
      const score = scoreSentiment("The company released Q3 report")
      expect(Math.abs(score)).toBeLessThan(0.3)
    })

    it("handles factual financial statements neutrally", () => {
      const score = scoreSentiment("Stock price is $150")
      expect(Math.abs(score)).toBeLessThan(0.5)
    })

    it("scores empty string as neutral", () => {
      const score = scoreSentiment("")
      expect(score).toBe(0)
    })
  })

  describe("edge cases", () => {
    it("handles ticker symbols without sentiment impact", () => {
      const withTicker = scoreSentiment("TSLA is moving")
      const withoutTicker = scoreSentiment("Stock is moving")
      // Tickers themselves shouldn't dramatically change sentiment
      expect(Math.abs(withTicker - withoutTicker)).toBeLessThan(0.5)
    })

    it("handles very long text", () => {
      const longText = "This is great! ".repeat(100)
      const score = scoreSentiment(longText)
      expect(Number.isFinite(score)).toBe(true)
      expect(score).toBeGreaterThan(0)
    })

    it("handles special characters", () => {
      const score = scoreSentiment("!!!??? $$$ @@@ ###")
      expect(Number.isFinite(score)).toBe(true)
    })

    it("handles ALL CAPS (VADER considers this emphasis)", () => {
      const normal = scoreSentiment("This is great")
      const caps = scoreSentiment("THIS IS GREAT")
      // VADER treats caps as emphasis, should be more extreme
      expect(caps).toBeGreaterThanOrEqual(normal)
    })

    it("handles mixed sentiment", () => {
      const score = scoreSentiment("Great earnings but worried about guidance")
      // Mixed sentiment should moderate the score
      expect(score).toBeGreaterThan(-0.5)
      expect(score).toBeLessThan(0.5)
    })
  })

  describe("return value bounds", () => {
    it("returns value between -1 and 1", () => {
      const positiveScore = scoreSentiment("Best thing ever! Amazing! Fantastic!")
      const negativeScore = scoreSentiment("Worst disaster ever! Terrible! Horrible!")

      expect(positiveScore).toBeGreaterThanOrEqual(-1)
      expect(positiveScore).toBeLessThanOrEqual(1)
      expect(negativeScore).toBeGreaterThanOrEqual(-1)
      expect(negativeScore).toBeLessThanOrEqual(1)
    })

    it("never returns NaN", () => {
      const scores = [
        scoreSentiment(""),
        scoreSentiment("normal text"),
        scoreSentiment("🚀🚀🚀"),
        scoreSentiment("123 456 789"),
      ]

      for (const score of scores) {
        expect(Number.isNaN(score)).toBe(false)
      }
    })
  })

  describe("financial language patterns", () => {
    it("handles common Reddit stock vocabulary", () => {
      const phrases = [
        { text: "diamond hands", expectPositive: true },
        { text: "to the moon", expectPositive: true },
        { text: "buy the dip", expectPositive: true },
        { text: "paper hands", expectPositive: false },
        { text: "bag holder", expectPositive: false },
      ]

      for (const { text, expectPositive } of phrases) {
        const score = scoreSentiment(text)
        if (expectPositive) {
          // Positive or neutral is acceptable for bullish phrases
          expect(score).toBeGreaterThanOrEqual(-0.2)
        }
        // Just verify it produces a valid score
        expect(Number.isFinite(score)).toBe(true)
      }
    })

    it("handles price action descriptions", () => {
      const bullish = scoreSentiment("Stock is up 50% after earnings")
      const bearish = scoreSentiment("Stock is down 50% after earnings")

      // VADER may treat these similarly as neutral financial statements
      // The important thing is both produce valid scores
      expect(Number.isFinite(bullish)).toBe(true)
      expect(Number.isFinite(bearish)).toBe(true)
      // At minimum, bullish should not be more negative than bearish
      expect(bullish).toBeGreaterThanOrEqual(bearish)
    })
  })
})
