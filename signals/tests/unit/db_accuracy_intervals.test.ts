import { describe, expect, it } from "vitest"
import { computeWilsonInterval } from "../../src/db"

describe("computeWilsonInterval", () => {
  it("returns deterministic 95% interval bounds for a typical sample", () => {
    const interval = computeWilsonInterval(24, 40)

    expect(interval.low).toBeCloseTo(0.446, 3)
    expect(interval.high).toBeCloseTo(0.737, 3)
  })

  it("returns 0-0 interval for zero-sample input", () => {
    expect(computeWilsonInterval(0, 0)).toEqual({ low: 0, high: 0 })
  })

  it("clamps invalid hit counts and interval bounds safely", () => {
    const belowZero = computeWilsonInterval(-5, 10)
    const aboveTotal = computeWilsonInterval(99, 10)

    expect(belowZero.low).toBeGreaterThanOrEqual(0)
    expect(belowZero.high).toBeLessThanOrEqual(1)
    expect(aboveTotal.low).toBeGreaterThanOrEqual(0)
    expect(aboveTotal.high).toBeLessThanOrEqual(1)

    expect(belowZero).toEqual(computeWilsonInterval(0, 10))
    expect(aboveTotal).toEqual(computeWilsonInterval(10, 10))
  })
})
