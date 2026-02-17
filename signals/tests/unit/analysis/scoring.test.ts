import { describe, it, expect } from "vitest"
import { scoreThemes } from "../../../src/analysis/scoring"
import type { ThemeMetrics } from "../../../src/analysis/metrics"
import { WEIGHTS } from "../../../src/analysis/constants"
import { createThemeMetrics } from "../../fixtures/sample_data"

function createMinimalMetrics(overrides: Partial<ThemeMetrics> = {}): ThemeMetrics {
  return createThemeMetrics(overrides)
}

describe("scoreThemes", () => {
  describe("basic scoring", () => {
    it("returns array of ThemeScore objects", () => {
      const metrics = [createMinimalMetrics({ theme_id: "test", theme_name: "Test" })]
      const result = scoreThemes(metrics)

      expect(Array.isArray(result)).toBe(true)
      expect(result).toHaveLength(1)
    })

    it("includes all required fields in ThemeScore", () => {
      const metrics = [createMinimalMetrics()]
      const result = scoreThemes(metrics)

      expect(result[0]).toHaveProperty("theme_id")
      expect(result[0]).toHaveProperty("theme_name")
      expect(result[0]).toHaveProperty("score")
      expect(result[0]).toHaveProperty("components")
      expect(result[0]).toHaveProperty("raw")
    })

    it("components includes all z-score values", () => {
      const metrics = [createMinimalMetrics()]
      const result = scoreThemes(metrics)

      expect(result[0].components).toHaveProperty("velocity")
      expect(result[0].components).toHaveProperty("sentiment_shift")
      expect(result[0].components).toHaveProperty("confirmation")
      expect(result[0].components).toHaveProperty("price")
    })

    it("raw includes all raw values", () => {
      const metrics = [createMinimalMetrics()]
      const result = scoreThemes(metrics)

      expect(result[0].raw).toHaveProperty("velocity")
      expect(result[0].raw).toHaveProperty("sentiment_shift")
      expect(result[0].raw).toHaveProperty("confirmation")
      expect(result[0].raw).toHaveProperty("price")
    })

    it("preserves theme_id and theme_name", () => {
      const metrics = [
        createMinimalMetrics({ theme_id: "nuclear", theme_name: "Nuclear/Uranium" }),
      ]
      const result = scoreThemes(metrics)

      expect(result[0].theme_id).toBe("nuclear")
      expect(result[0].theme_name).toBe("Nuclear/Uranium")
    })
  })

  describe("z-score normalization", () => {
    it("produces z-scores with mean approximately 0", () => {
      const metrics = [
        createMinimalMetrics({ growth_ratio: 1.5, slope: 0.1 }),
        createMinimalMetrics({ growth_ratio: 2.0, slope: 0.2 }),
        createMinimalMetrics({ growth_ratio: 2.5, slope: 0.3 }),
      ]

      const result = scoreThemes(metrics)
      const velocitySum = result.reduce((sum, r) => sum + r.components.velocity, 0)
      const meanVelocity = velocitySum / result.length

      expect(meanVelocity).toBeCloseTo(0, 5)
    })

    it("single item gets z-score of 0", () => {
      const metrics = [createMinimalMetrics()]
      const result = scoreThemes(metrics)

      expect(result[0].components.velocity).toBe(0)
      expect(result[0].components.sentiment_shift).toBe(0)
      expect(result[0].components.confirmation).toBe(0)
    })
  })

  describe("sorting", () => {
    it("sorts by composite score descending", () => {
      const metrics = [
        createMinimalMetrics({ theme_id: "low", growth_ratio: 0.5, slope: 0, confirmation_score: 0.1 }),
        createMinimalMetrics({ theme_id: "high", growth_ratio: 3.0, slope: 0.5, confirmation_score: 0.9 }),
        createMinimalMetrics({ theme_id: "mid", growth_ratio: 1.5, slope: 0.2, confirmation_score: 0.5 }),
      ]

      const result = scoreThemes(metrics)

      expect(result[0].theme_id).toBe("high")
      expect(result[1].theme_id).toBe("mid")
      expect(result[2].theme_id).toBe("low")
    })

    it("scores are monotonically decreasing", () => {
      const metrics = Array.from({ length: 10 }, (_, i) =>
        createMinimalMetrics({
          theme_id: `theme-${i}`,
          growth_ratio: 1 + Math.random() * 2,
          slope: Math.random() * 0.5,
          confirmation_score: Math.random(),
        })
      )

      const result = scoreThemes(metrics)

      for (let i = 1; i < result.length; i++) {
        expect(result[i - 1].score).toBeGreaterThanOrEqual(result[i].score)
      }
    })
  })

  describe("weight application", () => {
    it("uses configured weights from constants", () => {
      // Verify weights are accessible and reasonable
      expect(WEIGHTS.velocity).toBe(0.4)
      expect(WEIGHTS.sentiment).toBe(0.2)
      expect(WEIGHTS.confirmation).toBe(0.3)
      expect(WEIGHTS.price).toBe(0.1)
    })

    it("weight total equals 1.0", () => {
      const total = WEIGHTS.velocity + WEIGHTS.sentiment + WEIGHTS.confirmation + WEIGHTS.price
      expect(total).toBeCloseTo(1.0, 5)
    })

    it("redistributes price weight when no price data available", () => {
      const metricsNoPrice = [
        createMinimalMetrics({
          price_available: false,
          momentum_score: null,
          divergence_score: null,
        }),
        createMinimalMetrics({
          price_available: false,
          momentum_score: null,
          divergence_score: null,
        }),
      ]

      const result = scoreThemes(metricsNoPrice)

      // Should still produce valid scores
      expect(Number.isFinite(result[0].score)).toBe(true)
      // Price component should still be 0 since z-score of [0,0] is [0,0]
      expect(result[0].components.price).toBe(0)
    })

    it("includes price component when price data available", () => {
      const metricsWithPrice = [
        createMinimalMetrics({
          price_available: true,
          momentum_score: 0.8,
          divergence_score: 0.2,
        }),
        createMinimalMetrics({
          price_available: true,
          momentum_score: 0.2,
          divergence_score: 0.1,
        }),
      ]

      const result = scoreThemes(metricsWithPrice)

      // The one with higher momentum+divergence should have higher price z-score
      const highPriceIndex = result.findIndex(r =>
        r.raw.price === 1.0 // 0.8 + 0.2
      )
      expect(result[highPriceIndex].components.price).toBeGreaterThan(0)
    })
  })

  describe("raw value calculations", () => {
    it("velocity_raw = log(growth_ratio) + slope", () => {
      const metrics = [createMinimalMetrics({ growth_ratio: 2.0, slope: 0.3 })]
      const result = scoreThemes(metrics)

      const expected = Math.log(2.0) + 0.3
      expect(result[0].raw.velocity).toBeCloseTo(expected, 5)
    })

    it("sentiment_raw = sentiment_shift", () => {
      const metrics = [createMinimalMetrics({ sentiment_shift: 0.15 })]
      const result = scoreThemes(metrics)

      expect(result[0].raw.sentiment_shift).toBe(0.15)
    })

    it("confirmation_raw = confirmation_score", () => {
      const metrics = [createMinimalMetrics({ confirmation_score: 0.75 })]
      const result = scoreThemes(metrics)

      expect(result[0].raw.confirmation).toBe(0.75)
    })

    it("price_raw = momentum_score + divergence_score", () => {
      const metrics = [createMinimalMetrics({ momentum_score: 0.5, divergence_score: 0.3 })]
      const result = scoreThemes(metrics)

      expect(result[0].raw.price).toBe(0.8)
    })

    it("price_raw handles null momentum/divergence as 0", () => {
      const metrics = [createMinimalMetrics({ momentum_score: null, divergence_score: null })]
      const result = scoreThemes(metrics)

      expect(result[0].raw.price).toBe(0)
    })
  })

  describe("edge cases", () => {
    it("handles empty metrics array", () => {
      const result = scoreThemes([])
      expect(result).toEqual([])
    })

    it("handles single metric", () => {
      const metrics = [createMinimalMetrics({ theme_name: "Only Theme" })]
      const result = scoreThemes(metrics)

      expect(result).toHaveLength(1)
      expect(result[0].theme_name).toBe("Only Theme")
      expect(result[0].score).toBe(0) // z-score of single value is 0
    })

    it("handles metrics with same values", () => {
      // When all metrics have identical raw values, z-scores should all be 0
      // and final scores should be equal
      const metrics = [
        createMinimalMetrics({ theme_id: "a" }),
        createMinimalMetrics({ theme_id: "b" }),
        createMinimalMetrics({ theme_id: "c" }),
      ]

      const result = scoreThemes(metrics)

      // All should have the same score
      expect(result[0].score).toBe(result[1].score)
      expect(result[1].score).toBe(result[2].score)
    })

    it("handles very large growth ratios", () => {
      const metrics = [
        createMinimalMetrics({ growth_ratio: 1000 }),
        createMinimalMetrics({ growth_ratio: 1 }),
      ]

      const result = scoreThemes(metrics)

      expect(Number.isFinite(result[0].score)).toBe(true)
      expect(Number.isFinite(result[1].score)).toBe(true)
    })

    it("handles negative sentiment shift", () => {
      const metrics = [
        createMinimalMetrics({ sentiment_shift: -0.5 }),
        createMinimalMetrics({ sentiment_shift: 0.5 }),
      ]

      const result = scoreThemes(metrics)

      // Positive sentiment should score higher
      const positiveSentimentScore = result.find(r => r.raw.sentiment_shift === 0.5)
      const negativeSentimentScore = result.find(r => r.raw.sentiment_shift === -0.5)

      expect(positiveSentimentScore!.components.sentiment_shift).toBeGreaterThan(
        negativeSentimentScore!.components.sentiment_shift
      )
    })

    it("handles zero growth ratio gracefully", () => {
      // log(0) would be -Infinity without clamping
      const metrics = [
        createMinimalMetrics({ growth_ratio: 0 }),
        createMinimalMetrics({ growth_ratio: 1.0 }),
      ]

      const result = scoreThemes(metrics)

      expect(result).toHaveLength(2)
      expect(Number.isFinite(result[0].score)).toBe(true)
      expect(Number.isFinite(result[1].score)).toBe(true)
    })

    it("handles negative or non-finite growth ratios gracefully", () => {
      const metrics = [
        createMinimalMetrics({ theme_id: "neg", growth_ratio: -5 }),
        createMinimalMetrics({ theme_id: "nan", growth_ratio: Number.NaN }),
        createMinimalMetrics({ theme_id: "inf", growth_ratio: Number.POSITIVE_INFINITY }),
        createMinimalMetrics({ theme_id: "ok", growth_ratio: 1.2 }),
      ]

      const result = scoreThemes(metrics)

      expect(result).toHaveLength(4)
      for (const row of result) {
        expect(Number.isFinite(row.score)).toBe(true)
        expect(Number.isFinite(row.components.velocity)).toBe(true)
      }
    })
  })

  describe("score stability", () => {
    it("produces same results for same input", () => {
      const metrics = [
        createMinimalMetrics({ theme_id: "a", growth_ratio: 2.0 }),
        createMinimalMetrics({ theme_id: "b", growth_ratio: 1.5 }),
      ]

      const result1 = scoreThemes(metrics)
      const result2 = scoreThemes(metrics)

      expect(result1[0].theme_id).toBe(result2[0].theme_id)
      expect(result1[0].score).toBe(result2[0].score)
    })

    it("produces non-NaN scores", () => {
      const metrics = Array.from({ length: 20 }, (_, i) =>
        createMinimalMetrics({
          theme_id: `theme-${i}`,
          growth_ratio: 0.5 + i * 0.2,
          slope: i * 0.05,
          confirmation_score: i * 0.05,
          sentiment_shift: (i - 10) * 0.02,
        })
      )

      const result = scoreThemes(metrics)

      for (const r of result) {
        expect(Number.isNaN(r.score)).toBe(false)
        expect(Number.isNaN(r.components.velocity)).toBe(false)
        expect(Number.isNaN(r.components.sentiment_shift)).toBe(false)
        expect(Number.isNaN(r.components.confirmation)).toBe(false)
        expect(Number.isNaN(r.components.price)).toBe(false)
      }
    })
  })
})
