import { describe, it, expect } from "vitest"
import { buildTakeaways } from "../../../src/analysis/takeaways"
import { createThemeMetrics, createThemeScore, createThemeClassification } from "../../fixtures/sample_data"
import { LIMITS, RISK_THRESHOLDS } from "../../../src/analysis/constants"

function createItem(
  metricsOverrides: Partial<ReturnType<typeof createThemeMetrics>> = {},
  classificationOverrides: Partial<ReturnType<typeof createThemeClassification>> = {}
) {
  return {
    metrics: createThemeMetrics(metricsOverrides),
    score: createThemeScore(),
    classification: createThemeClassification(classificationOverrides),
  }
}

describe("buildTakeaways", () => {
  describe("data_shows section", () => {
    it("includes top 3 themes by default", () => {
      const items = [
        createItem({ theme_name: "Theme A", growth_ratio: 2.0, unique_subreddits: 5 }),
        createItem({ theme_name: "Theme B", growth_ratio: 1.8, unique_subreddits: 4 }),
        createItem({ theme_name: "Theme C", growth_ratio: 1.5, unique_subreddits: 3 }),
        createItem({ theme_name: "Theme D", growth_ratio: 1.2, unique_subreddits: 2 }),
      ]

      const result = buildTakeaways(items)

      expect(result.data_shows).toHaveLength(LIMITS.topTakeaways)
      expect(result.data_shows[0]).toContain("Theme A")
      expect(result.data_shows[1]).toContain("Theme B")
      expect(result.data_shows[2]).toContain("Theme C")
    })

    it("includes growth ratio in data_shows", () => {
      const items = [
        createItem({ theme_name: "Tech", growth_ratio: 2.5, unique_subreddits: 4 }),
      ]

      const result = buildTakeaways(items)

      expect(result.data_shows[0]).toContain("2.50x")
    })

    it("includes subreddit count in data_shows", () => {
      const items = [
        createItem({ theme_name: "Tech", growth_ratio: 1.5, unique_subreddits: 7 }),
      ]

      const result = buildTakeaways(items)

      expect(result.data_shows[0]).toContain("7 subreddits")
    })

    it("handles fewer than 3 items", () => {
      const items = [
        createItem({ theme_name: "Only Theme", growth_ratio: 2.0, unique_subreddits: 3 }),
      ]

      const result = buildTakeaways(items)

      expect(result.data_shows).toHaveLength(1)
      expect(result.data_shows[0]).toContain("Only Theme")
    })

    it("handles empty items array", () => {
      const result = buildTakeaways([])

      expect(result.data_shows).toEqual([])
    })
  })

  describe("actionable_signals section", () => {
    it("groups themes by timing - Immediate for Now", () => {
      const items = [
        createItem({ theme_name: "Urgent Theme" }, { timing: "Now" }),
      ]

      const result = buildTakeaways(items)

      expect(result.actionable_signals.some(s => s.startsWith("Immediate:"))).toBe(true)
      expect(result.actionable_signals.find(s => s.startsWith("Immediate:"))).toContain("Urgent Theme")
    })

    it("groups Now (volatile) under Immediate", () => {
      const items = [
        createItem({ theme_name: "Volatile Theme" }, { timing: "Now (volatile)" }),
      ]

      const result = buildTakeaways(items)

      expect(result.actionable_signals.some(s => s.startsWith("Immediate:"))).toBe(true)
    })

    it("groups Building themes separately", () => {
      const items = [
        createItem({ theme_name: "Building Theme" }, { timing: "Building" }),
      ]

      const result = buildTakeaways(items)

      expect(result.actionable_signals.some(s => s.startsWith("Building:"))).toBe(true)
      expect(result.actionable_signals.find(s => s.startsWith("Building:"))).toContain("Building Theme")
    })

    it("groups Early and Ongoing under Watch", () => {
      const items = [
        createItem({ theme_name: "Early Theme" }, { timing: "Early" }),
        createItem({ theme_name: "Ongoing Theme" }, { timing: "Ongoing" }),
      ]

      const result = buildTakeaways(items)

      expect(result.actionable_signals.some(s => s.startsWith("Watch:"))).toBe(true)
      const watchLine = result.actionable_signals.find(s => s.startsWith("Watch:"))!
      expect(watchLine).toContain("Early Theme")
      expect(watchLine).toContain("Ongoing Theme")
    })

    it("handles all timing types in one response", () => {
      const items = [
        createItem({ theme_name: "Now Theme" }, { timing: "Now" }),
        createItem({ theme_name: "Build Theme" }, { timing: "Building" }),
        createItem({ theme_name: "Early Theme" }, { timing: "Early" }),
      ]

      const result = buildTakeaways(items)

      expect(result.actionable_signals).toHaveLength(3)
      expect(result.actionable_signals.some(s => s.startsWith("Immediate:"))).toBe(true)
      expect(result.actionable_signals.some(s => s.startsWith("Building:"))).toBe(true)
      expect(result.actionable_signals.some(s => s.startsWith("Watch:"))).toBe(true)
    })

    it("limits each category to top 3", () => {
      const items = [
        createItem({ theme_name: "Now 1" }, { timing: "Now" }),
        createItem({ theme_name: "Now 2" }, { timing: "Now" }),
        createItem({ theme_name: "Now 3" }, { timing: "Now" }),
        createItem({ theme_name: "Now 4" }, { timing: "Now" }),
        createItem({ theme_name: "Now 5" }, { timing: "Now" }),
      ]

      const result = buildTakeaways(items)
      const immediateLine = result.actionable_signals.find(s => s.startsWith("Immediate:"))!

      // Should only contain first 3
      expect(immediateLine).toContain("Now 1")
      expect(immediateLine).toContain("Now 2")
      expect(immediateLine).toContain("Now 3")
      expect(immediateLine).not.toContain("Now 4")
      expect(immediateLine).not.toContain("Now 5")
    })

    it("omits empty categories", () => {
      const items = [
        createItem({ theme_name: "Early Only" }, { timing: "Early" }),
      ]

      const result = buildTakeaways(items)

      expect(result.actionable_signals).toHaveLength(1)
      expect(result.actionable_signals[0].startsWith("Watch:")).toBe(true)
    })
  })

  describe("risk_factors section", () => {
    it("identifies high concentration as risk", () => {
      const items = [
        createItem({
          theme_name: "Concentrated Theme",
          concentration: RISK_THRESHOLDS.highConcentration + 0.01,
          mentions_L: 10,
        }),
      ]

      const result = buildTakeaways(items)

      expect(result.risk_factors.some(r => r.includes("high concentration"))).toBe(true)
      expect(result.risk_factors.find(r => r.includes("high concentration"))).toContain("Concentrated Theme")
    })

    it("identifies low sample size as risk", () => {
      const items = [
        createItem({
          theme_name: "Small Sample Theme",
          concentration: 0.3,
          mentions_L: RISK_THRESHOLDS.lowMentions - 1,
        }),
      ]

      const result = buildTakeaways(items)

      expect(result.risk_factors.some(r => r.includes("low sample size"))).toBe(true)
    })

    it("identifies both risks when present", () => {
      const items = [
        createItem({
          theme_name: "Risky Theme",
          concentration: RISK_THRESHOLDS.highConcentration + 0.1,
          mentions_L: RISK_THRESHOLDS.lowMentions - 1,
        }),
      ]

      const result = buildTakeaways(items)

      const riskLine = result.risk_factors.find(r => r.includes("Risky Theme"))!
      expect(riskLine).toContain("high concentration")
      expect(riskLine).toContain("low sample size")
    })

    it("limits risk factors to top 3", () => {
      const items = Array.from({ length: 5 }, (_, i) =>
        createItem({
          theme_name: `Risky ${i + 1}`,
          concentration: RISK_THRESHOLDS.highConcentration + 0.1,
          mentions_L: 10,
        })
      )

      const result = buildTakeaways(items)

      expect(result.risk_factors).toHaveLength(LIMITS.topTakeaways)
    })

    it("returns default message when no risk factors", () => {
      const items = [
        createItem({
          theme_name: "Safe Theme",
          concentration: 0.3, // below threshold
          mentions_L: 10, // above threshold
        }),
      ]

      const result = buildTakeaways(items)

      expect(result.risk_factors).toHaveLength(1)
      expect(result.risk_factors[0]).toContain("No outsized risk factors")
    })
  })

  describe("return type structure", () => {
    it("returns all required sections", () => {
      const items = [createItem()]
      const result = buildTakeaways(items)

      expect(result).toHaveProperty("data_shows")
      expect(result).toHaveProperty("actionable_signals")
      expect(result).toHaveProperty("risk_factors")
    })

    it("all sections are arrays of strings", () => {
      const items = [createItem()]
      const result = buildTakeaways(items)

      expect(Array.isArray(result.data_shows)).toBe(true)
      expect(Array.isArray(result.actionable_signals)).toBe(true)
      expect(Array.isArray(result.risk_factors)).toBe(true)

      for (const line of [...result.data_shows, ...result.actionable_signals, ...result.risk_factors]) {
        expect(typeof line).toBe("string")
      }
    })

    it("each line ends with period", () => {
      const items = [
        createItem({ theme_name: "Test Theme" }, { timing: "Now" }),
      ]
      const result = buildTakeaways(items)

      for (const line of result.data_shows) {
        expect(line).toMatch(/\.$/)
      }
      for (const line of result.actionable_signals) {
        expect(line).toMatch(/\.$/)
      }
    })
  })

  describe("edge cases", () => {
    it("handles theme names with special characters", () => {
      const items = [
        createItem({ theme_name: "Oil & Gas" }),
      ]

      const result = buildTakeaways(items)

      expect(result.data_shows[0]).toContain("Oil & Gas")
    })

    it("handles very long theme names", () => {
      const longName = "A".repeat(100)
      const items = [createItem({ theme_name: longName })]

      const result = buildTakeaways(items)

      expect(result.data_shows[0]).toContain(longName)
    })

    it("handles zero growth ratio", () => {
      const items = [
        createItem({ theme_name: "Static Theme", growth_ratio: 0 }),
      ]

      const result = buildTakeaways(items)

      expect(result.data_shows[0]).toContain("0.00x")
    })

    it("handles negative growth ratio", () => {
      const items = [
        createItem({ theme_name: "Declining Theme", growth_ratio: -0.5 }),
      ]

      const result = buildTakeaways(items)

      expect(result.data_shows[0]).toContain("-0.50x")
    })
  })
})
