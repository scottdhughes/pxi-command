import { describe, it, expect } from "vitest"
import { classifyTheme } from "../../../src/analysis/classify"
import {
  createThemeMetrics,
  createThemeScore,
  highConfidenceMetrics,
  lowConfidenceMetrics,
  buildingTimingMetrics,
  meanReversionMetrics,
} from "../../fixtures/sample_data"
import { CONFIDENCE_THRESHOLDS, TIMING_THRESHOLDS, SIGNAL_TYPE_THRESHOLDS } from "../../../src/analysis/constants"

describe("classifyTheme", () => {
  describe("signal_type classification", () => {
    it("defaults to Rotation when no momentum/divergence scores", () => {
      const metrics = createThemeMetrics({
        momentum_score: null,
        divergence_score: null,
        sentiment_shift: 0.1,
        growth_ratio: 1.5,
      })
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result.signal_type).toBe("Rotation")
    })

    it("classifies as Momentum when momentum_score >= divergence_score", () => {
      const metrics = createThemeMetrics({
        momentum_score: 0.8,
        divergence_score: 0.3,
      })
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result.signal_type).toBe("Momentum")
    })

    it("classifies as Divergence when divergence_score > momentum_score", () => {
      const metrics = createThemeMetrics({
        momentum_score: 0.3,
        divergence_score: 0.8,
      })
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result.signal_type).toBe("Divergence")
    })

    it("classifies as Mean Reversion with negative sentiment and declining growth", () => {
      const metrics = meanReversionMetrics
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result.signal_type).toBe("Mean Reversion")
    })

    it("requires sentiment below threshold for Mean Reversion", () => {
      const metrics = createThemeMetrics({
        momentum_score: null,
        divergence_score: null,
        sentiment_shift: SIGNAL_TYPE_THRESHOLDS.meanReversionSentiment, // exactly at threshold
        growth_ratio: 0.8,
      })
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      // At threshold, should NOT be Mean Reversion
      expect(result.signal_type).toBe("Rotation")
    })
  })

  describe("confidence classification", () => {
    it("classifies as Very High with 4 confidence points", () => {
      const metrics = highConfidenceMetrics
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result.confidence).toBe("Very High")
    })

    it("classifies as Medium-Low with 0 confidence points", () => {
      const metrics = lowConfidenceMetrics
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result.confidence).toBe("Medium-Low")
    })

    it("awards point for mentions >= 8", () => {
      const metricsLow = createThemeMetrics({
        mentions_L: CONFIDENCE_THRESHOLDS.minMentions - 1,
        unique_subreddits: 1,
        concentration: 0.9,
        price_available: false,
      })
      const metricsHigh = createThemeMetrics({
        mentions_L: CONFIDENCE_THRESHOLDS.minMentions,
        unique_subreddits: 1,
        concentration: 0.9,
        price_available: false,
      })
      const score = createThemeScore()

      const resultLow = classifyTheme(metricsLow, score, 1, 10)
      const resultHigh = classifyTheme(metricsHigh, score, 1, 10)

      // resultHigh should have higher confidence
      expect(resultLow.confidence).toBe("Medium-Low")
      expect(resultHigh.confidence).toBe("Medium")
    })

    it("awards point for unique_subreddits >= 3", () => {
      const metricsLow = createThemeMetrics({
        mentions_L: 1,
        unique_subreddits: CONFIDENCE_THRESHOLDS.minSubreddits - 1,
        concentration: 0.9,
        price_available: false,
      })
      const metricsHigh = createThemeMetrics({
        mentions_L: 1,
        unique_subreddits: CONFIDENCE_THRESHOLDS.minSubreddits,
        concentration: 0.9,
        price_available: false,
      })
      const score = createThemeScore()

      const resultLow = classifyTheme(metricsLow, score, 1, 10)
      const resultHigh = classifyTheme(metricsHigh, score, 1, 10)

      expect(resultLow.confidence).toBe("Medium-Low")
      expect(resultHigh.confidence).toBe("Medium")
    })

    it("awards point for concentration <= 0.5", () => {
      const metricsHigh = createThemeMetrics({
        mentions_L: 1,
        unique_subreddits: 1,
        concentration: CONFIDENCE_THRESHOLDS.maxConcentration,
        price_available: false,
      })
      const metricsLow = createThemeMetrics({
        mentions_L: 1,
        unique_subreddits: 1,
        concentration: CONFIDENCE_THRESHOLDS.maxConcentration + 0.01,
        price_available: false,
      })
      const score = createThemeScore()

      const resultHigh = classifyTheme(metricsHigh, score, 1, 10)
      const resultLow = classifyTheme(metricsLow, score, 1, 10)

      expect(resultHigh.confidence).toBe("Medium")
      expect(resultLow.confidence).toBe("Medium-Low")
    })

    it("awards point for price_available", () => {
      const metricsWithPrice = createThemeMetrics({
        mentions_L: 1,
        unique_subreddits: 1,
        concentration: 0.9,
        price_available: true,
      })
      const metricsNoPrice = createThemeMetrics({
        mentions_L: 1,
        unique_subreddits: 1,
        concentration: 0.9,
        price_available: false,
      })
      const score = createThemeScore()

      const resultWithPrice = classifyTheme(metricsWithPrice, score, 1, 10)
      const resultNoPrice = classifyTheme(metricsNoPrice, score, 1, 10)

      expect(resultWithPrice.confidence).toBe("Medium")
      expect(resultNoPrice.confidence).toBe("Medium-Low")
    })

    it("classifies High with 3 points", () => {
      const metrics = createThemeMetrics({
        mentions_L: CONFIDENCE_THRESHOLDS.minMentions,
        unique_subreddits: CONFIDENCE_THRESHOLDS.minSubreddits,
        concentration: CONFIDENCE_THRESHOLDS.maxConcentration,
        price_available: false, // 3 points, not 4
      })
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result.confidence).toBe("High")
    })

    it("classifies Medium-High with 2 points", () => {
      const metrics = createThemeMetrics({
        mentions_L: CONFIDENCE_THRESHOLDS.minMentions,
        unique_subreddits: CONFIDENCE_THRESHOLDS.minSubreddits,
        concentration: 0.9, // loses this point
        price_available: false, // loses this point
      })
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result.confidence).toBe("Medium-High")
    })
  })

  describe("timing classification", () => {
    it("classifies as Now with high growth and slope", () => {
      const metrics = createThemeMetrics({
        growth_ratio: TIMING_THRESHOLDS.growthNow,
        slope: TIMING_THRESHOLDS.slopeNow + 0.01,
        concentration: 0.3,
      })
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result.timing).toBe("Now")
    })

    it("classifies as Now (volatile) with high growth and concentration", () => {
      const metrics = createThemeMetrics({
        growth_ratio: TIMING_THRESHOLDS.growthNow,
        slope: 0.1, // below slopeNow threshold
        concentration: TIMING_THRESHOLDS.concentrationVolatile + 0.01,
      })
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result.timing).toBe("Now (volatile)")
    })

    it("classifies as Building with moderate growth", () => {
      const metrics = buildingTimingMetrics
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result.timing).toBe("Building")
    })

    it("classifies as Ongoing with lower growth", () => {
      const metrics = createThemeMetrics({
        growth_ratio: TIMING_THRESHOLDS.growthOngoing,
        slope: 0.1,
        concentration: 0.3,
      })
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result.timing).toBe("Ongoing")
    })

    it("classifies as Early with lowest growth", () => {
      const metrics = createThemeMetrics({
        growth_ratio: TIMING_THRESHOLDS.growthOngoing - 0.01,
        slope: 0.1,
        concentration: 0.3,
      })
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result.timing).toBe("Early")
    })
  })

  describe("stars calculation", () => {
    it("gives 5 stars to rank 1 of 10", () => {
      const metrics = createThemeMetrics()
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result.stars).toBe(5)
    })

    it("gives 1 star to rank 10 of 10", () => {
      const metrics = createThemeMetrics()
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 10, 10)

      expect(result.stars).toBe(1)
    })

    it("gives 3 stars to middle rank", () => {
      const metrics = createThemeMetrics()
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 5, 10)

      expect(result.stars).toBe(3)
    })

    it("clamps stars to minimum 1", () => {
      const metrics = createThemeMetrics()
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 100, 100)

      expect(result.stars).toBeGreaterThanOrEqual(1)
    })

    it("clamps stars to maximum 5", () => {
      const metrics = createThemeMetrics()
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 1)

      expect(result.stars).toBeLessThanOrEqual(5)
    })

    it("handles edge case of total=1", () => {
      const metrics = createThemeMetrics()
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 1)

      expect(result.stars).toBe(5)
    })
  })

  describe("return type structure", () => {
    it("returns all required fields", () => {
      const metrics = createThemeMetrics()
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(result).toHaveProperty("signal_type")
      expect(result).toHaveProperty("confidence")
      expect(result).toHaveProperty("timing")
      expect(result).toHaveProperty("stars")
    })

    it("signal_type is one of valid values", () => {
      const validTypes = ["Rotation", "Momentum", "Divergence", "Mean Reversion"]
      const metrics = createThemeMetrics()
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(validTypes).toContain(result.signal_type)
    })

    it("confidence is one of valid values", () => {
      const validConfidences = ["Very High", "High", "Medium-High", "Medium", "Medium-Low"]
      const metrics = createThemeMetrics()
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(validConfidences).toContain(result.confidence)
    })

    it("timing is one of valid values", () => {
      const validTimings = ["Now", "Now (volatile)", "Building", "Ongoing", "Early"]
      const metrics = createThemeMetrics()
      const score = createThemeScore()
      const result = classifyTheme(metrics, score, 1, 10)

      expect(validTimings).toContain(result.timing)
    })
  })
})
