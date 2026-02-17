import { describe, expect, it } from "vitest"
import { calculateTargetDate } from "../../src/evaluation"
import { addTradingDays } from "../../src/utils/calendar"

describe("evaluation target date uses trading-day horizon", () => {
  it("adds 7 trading days from a standard weekday signal date", () => {
    expect(calculateTargetDate("2026-02-17", 7)).toBe("2026-02-26")
  })

  it("skips weekends and Monday market holidays", () => {
    // 2026-02-16 is Presidents' Day (NYSE closed).
    expect(calculateTargetDate("2026-02-13", 1)).toBe("2026-02-17")

    // 2026-09-07 is Labor Day (NYSE closed).
    expect(calculateTargetDate("2026-09-04", 1)).toBe("2026-09-08")
  })

  it("handles weekend start dates deterministically", () => {
    expect(addTradingDays("2026-02-14", 1)).toBe("2026-02-17")
  })

  it("skips Good Friday when computing trading-day horizons", () => {
    // Good Friday in 2026 is 2026-04-03.
    expect(calculateTargetDate("2026-03-31", 3)).toBe("2026-04-06")
  })
})
