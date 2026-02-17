import { describe, expect, it } from "vitest"
import { getNyseHolidaySet, isNyseHoliday, shouldRunScheduledPipeline } from "../../src/worker"
import { isNyseHolidayDate } from "../../src/utils/calendar"

function decide(scheduledAtUtc: string, lastSuccessfulRunAtUtc: string | null) {
  return shouldRunScheduledPipeline({
    scheduledAt: new Date(scheduledAtUtc),
    lastSuccessfulRunAtUtc,
  })
}

describe("NYSE holiday calendar", () => {
  it("matches expected Monday NYSE closures for 2025-2032", () => {
    const expectedByYear: Record<number, string[]> = {
      2025: ["2025-01-20", "2025-02-17", "2025-05-26", "2025-09-01"],
      2026: ["2026-01-19", "2026-02-16", "2026-05-25", "2026-09-07"],
      2027: ["2027-01-18", "2027-02-15", "2027-05-31", "2027-07-05", "2027-09-06"],
      2028: ["2028-01-17", "2028-02-21", "2028-05-29", "2028-06-19", "2028-09-04", "2028-12-25"],
      2029: ["2029-01-01", "2029-01-15", "2029-02-19", "2029-05-28", "2029-09-03"],
      2030: ["2030-01-21", "2030-02-18", "2030-05-27", "2030-09-02"],
      2031: ["2031-01-20", "2031-02-17", "2031-05-26", "2031-09-01"],
      2032: ["2032-01-19", "2032-02-16", "2032-05-31", "2032-07-05", "2032-09-06"],
    }

    for (const [yearText, expected] of Object.entries(expectedByYear)) {
      const year = Number(yearText)
      const mondayHolidays = [...getNyseHolidaySet(year)]
        .filter((date) => new Date(`${date}T00:00:00.000Z`).getUTCDay() === 1)
        .sort()

      expect(mondayHolidays).toEqual(expected)
    }
  })

  it("recognizes non-Monday NYSE holidays (Good Friday)", () => {
    expect(isNyseHoliday(new Date("2026-04-03T15:00:00.000Z"))).toBe(true)
    expect(isNyseHoliday(new Date("2026-04-06T15:00:00.000Z"))).toBe(false)
  })

  it("recognizes observed fixed-date Monday holiday (Independence Day observed)", () => {
    expect(isNyseHoliday(new Date("2027-07-05T15:00:00.000Z"))).toBe(true)
  })

  it("keeps worker holiday helper parity with shared calendar util", () => {
    const date = new Date("2028-12-25T15:00:00.000Z")
    expect(isNyseHoliday(date)).toBe(isNyseHolidayDate(date))
  })
})

describe("scheduled pipeline decisioning", () => {
  it("skips Monday on NYSE holidays", () => {
    const result = decide("2026-02-16T15:00:00.000Z", "2026-02-12T22:41:09.394Z")
    expect(result.shouldRun).toBe(false)
    expect(result.reason).toContain("holiday")
  })

  it("runs Tuesday when Monday was a holiday", () => {
    const result = decide("2026-02-17T15:00:00.000Z", "2026-02-12T22:41:09.394Z")
    expect(result.shouldRun).toBe(true)
    expect(result.reason).toContain("was a holiday")
  })

  it("runs Tuesday after observed fixed-date Monday holiday", () => {
    const result = decide("2027-07-06T15:00:00.000Z", "2027-06-28T15:00:00.000Z")
    expect(result.shouldRun).toBe(true)
    expect(result.reason).toContain("was a holiday")
  })

  it("skips Tuesday when Monday was not a holiday and data is fresh", () => {
    const result = decide("2026-02-10T15:00:00.000Z", "2026-02-09T15:00:00.000Z")
    expect(result.shouldRun).toBe(false)
    expect(result.reason).toContain("Monday was not a holiday")
  })

  it("runs Tuesday catch-up when data is stale", () => {
    const result = decide("2026-02-10T15:00:00.000Z", "2026-02-01T15:00:00.000Z")
    expect(result.shouldRun).toBe(true)
    expect(result.reason).toContain("catch-up")
  })

  it("runs Wednesday catch-up when data is stale", () => {
    const result = decide("2026-02-11T15:00:00.000Z", "2026-02-01T15:00:00.000Z")
    expect(result.shouldRun).toBe(true)
    expect(result.reason).toContain("stale-data catch-up")
  })

  it("skips Wednesday catch-up when data is fresh", () => {
    const result = decide("2026-02-11T15:00:00.000Z", "2026-02-10T15:00:00.000Z")
    expect(result.shouldRun).toBe(false)
    expect(result.reason).toContain("fresh")
  })
})
