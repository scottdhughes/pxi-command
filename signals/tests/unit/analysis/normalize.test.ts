import { describe, it, expect } from "vitest"
import { zScore } from "../../../src/analysis/normalize"

describe("zScore", () => {
  describe("basic functionality", () => {
    it("returns empty array for empty input", () => {
      expect(zScore([])).toEqual([])
    })

    it("returns [0] for single value", () => {
      const result = zScore([5])
      expect(result).toHaveLength(1)
      expect(result[0]).toBe(0)
    })

    it("normalizes values to mean=0 approximately", () => {
      const result = zScore([1, 2, 3, 4, 5])
      const mean = result.reduce((a, b) => a + b, 0) / result.length
      expect(mean).toBeCloseTo(0, 10)
    })

    it("produces expected z-scores for known distribution", () => {
      // For [1, 2, 3, 4, 5]: mean=3, std=sqrt(2)≈1.414
      const result = zScore([1, 2, 3, 4, 5])

      expect(result[0]).toBeCloseTo(-1.414, 2)  // (1-3)/1.414
      expect(result[2]).toBeCloseTo(0, 10)      // (3-3)/1.414 = 0
      expect(result[4]).toBeCloseTo(1.414, 2)   // (5-3)/1.414
    })
  })

  describe("edge cases", () => {
    it("handles all identical values (zero variance)", () => {
      const result = zScore([5, 5, 5, 5])
      // When std=0, it defaults to 1, so all values become 0
      expect(result).toEqual([0, 0, 0, 0])
    })

    it("handles negative values", () => {
      const result = zScore([-5, -3, -1, 1, 3, 5])
      const mean = result.reduce((a, b) => a + b, 0) / result.length
      expect(mean).toBeCloseTo(0, 10)
    })

    it("handles very large values", () => {
      const result = zScore([1e10, 2e10, 3e10])
      expect(result).toHaveLength(3)
      expect(Number.isFinite(result[0])).toBe(true)
      expect(Number.isFinite(result[1])).toBe(true)
      expect(Number.isFinite(result[2])).toBe(true)
    })

    it("handles very small values", () => {
      const result = zScore([1e-10, 2e-10, 3e-10])
      expect(result).toHaveLength(3)
      expect(Number.isFinite(result[0])).toBe(true)
    })

    it("handles mixed positive and negative large values", () => {
      const result = zScore([-1000, 0, 1000])
      expect(result[0]).toBeCloseTo(-1.22, 1)
      expect(result[1]).toBeCloseTo(0, 10)
      expect(result[2]).toBeCloseTo(1.22, 1)
    })
  })

  describe("properties", () => {
    it("preserves array length", () => {
      const input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      const result = zScore(input)
      expect(result.length).toBe(input.length)
    })

    it("preserves relative ordering", () => {
      const input = [3, 1, 4, 1, 5, 9, 2, 6]
      const result = zScore(input)

      // Original min is at indices 1, 3; max at index 5
      // Relative ordering should be preserved
      const minIdx = result.indexOf(Math.min(...result))
      const maxIdx = result.indexOf(Math.max(...result))

      expect([1, 3]).toContain(minIdx)
      expect(maxIdx).toBe(5)
    })

    it("produces standard deviation approximately 1 for varied input", () => {
      const result = zScore([10, 20, 30, 40, 50])
      const mean = result.reduce((a, b) => a + b, 0) / result.length
      const variance = result.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / result.length
      const std = Math.sqrt(variance)
      expect(std).toBeCloseTo(1, 5)
    })

    it("does not modify the input array", () => {
      const input = [1, 2, 3, 4, 5]
      const original = [...input]
      zScore(input)
      expect(input).toEqual(original)
    })
  })

  describe("realistic scenarios", () => {
    it("normalizes mention counts across themes", () => {
      const mentionCounts = [5, 12, 8, 45, 22, 3, 18]
      const result = zScore(mentionCounts)

      // Highest count (45) should have highest z-score
      const maxZIndex = result.indexOf(Math.max(...result))
      expect(maxZIndex).toBe(3)

      // Lowest count (3) should have lowest z-score
      const minZIndex = result.indexOf(Math.min(...result))
      expect(minZIndex).toBe(5)
    })

    it("normalizes sentiment scores", () => {
      const sentiments = [-0.5, 0.2, 0.8, 0.1, -0.2, 0.5]
      const result = zScore(sentiments)

      // Most positive (0.8) should have highest z-score
      const maxZIndex = result.indexOf(Math.max(...result))
      expect(maxZIndex).toBe(2)
    })
  })
})
