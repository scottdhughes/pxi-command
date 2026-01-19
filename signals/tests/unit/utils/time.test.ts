import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { nowUtcIso, toUtcDateString, daysAgoUtc, dateToYmd, getUtcDayKey } from "../../../src/utils/time"

describe("time utilities", () => {
  describe("nowUtcIso", () => {
    it("returns ISO 8601 formatted string", () => {
      const result = nowUtcIso()
      expect(result).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
    })

    it("returns current time", () => {
      const before = new Date().toISOString()
      const result = nowUtcIso()
      const after = new Date().toISOString()

      expect(result >= before).toBe(true)
      expect(result <= after).toBe(true)
    })

    it("ends with Z indicating UTC", () => {
      const result = nowUtcIso()
      expect(result).toMatch(/Z$/)
    })
  })

  describe("toUtcDateString", () => {
    it("converts Unix timestamp to YYYY-MM-DD", () => {
      // 2024-01-15 00:00:00 UTC
      const ts = 1705276800
      const result = toUtcDateString(ts)
      expect(result).toBe("2024-01-15")
    })

    it("handles timestamp 0 (Unix epoch)", () => {
      const result = toUtcDateString(0)
      expect(result).toBe("1970-01-01")
    })

    it("handles timestamps at day boundaries", () => {
      // End of 2024-01-15 (23:59:59 UTC)
      const tsEndOfDay = 1705363199
      const result = toUtcDateString(tsEndOfDay)
      expect(result).toBe("2024-01-15")
    })

    it("handles timestamps in different years", () => {
      const ts2020 = 1577836800 // 2020-01-01
      const ts2030 = 1893456000 // 2030-01-01

      expect(toUtcDateString(ts2020)).toBe("2020-01-01")
      expect(toUtcDateString(ts2030)).toBe("2030-01-01")
    })

    it("pads month and day with leading zeros", () => {
      // 2024-01-05
      const ts = 1704412800
      const result = toUtcDateString(ts)
      expect(result).toMatch(/^\d{4}-0\d-0\d$/)
    })
  })

  describe("daysAgoUtc", () => {
    beforeEach(() => {
      vi.useFakeTimers()
      vi.setSystemTime(new Date("2024-06-15T12:00:00Z"))
    })

    afterEach(() => {
      vi.useRealTimers()
    })

    it("returns Date object for 0 days ago (today)", () => {
      const result = daysAgoUtc(0)
      expect(result).toBeInstanceOf(Date)
      expect(result.toISOString()).toContain("2024-06-15")
    })

    it("returns correct date for 1 day ago", () => {
      const result = daysAgoUtc(1)
      expect(result.toISOString()).toContain("2024-06-14")
    })

    it("returns correct date for 7 days ago", () => {
      const result = daysAgoUtc(7)
      expect(result.toISOString()).toContain("2024-06-08")
    })

    it("returns correct date for 30 days ago", () => {
      const result = daysAgoUtc(30)
      expect(result.toISOString()).toContain("2024-05-16")
    })

    it("handles crossing month boundary", () => {
      vi.setSystemTime(new Date("2024-03-05T12:00:00Z"))
      const result = daysAgoUtc(10)
      expect(result.toISOString()).toContain("2024-02-24")
    })

    it("handles crossing year boundary", () => {
      vi.setSystemTime(new Date("2024-01-05T12:00:00Z"))
      const result = daysAgoUtc(10)
      expect(result.toISOString()).toContain("2023-12-26")
    })

    it("handles leap year February", () => {
      vi.setSystemTime(new Date("2024-03-01T12:00:00Z"))
      const result = daysAgoUtc(1)
      // 2024 is a leap year, so Feb 29 exists
      expect(result.toISOString()).toContain("2024-02-29")
    })
  })

  describe("dateToYmd", () => {
    it("converts Date to YYYY-MM-DD string", () => {
      const date = new Date("2024-06-15T12:30:45Z")
      const result = dateToYmd(date)
      expect(result).toBe("2024-06-15")
    })

    it("uses UTC timezone", () => {
      // This date at 23:00 UTC would be next day in UTC+1
      const date = new Date("2024-06-15T23:00:00Z")
      const result = dateToYmd(date)
      expect(result).toBe("2024-06-15")
    })

    it("handles dates at start of day", () => {
      const date = new Date("2024-06-15T00:00:00Z")
      const result = dateToYmd(date)
      expect(result).toBe("2024-06-15")
    })

    it("handles Unix epoch", () => {
      const date = new Date(0)
      const result = dateToYmd(date)
      expect(result).toBe("1970-01-01")
    })
  })

  describe("getUtcDayKey", () => {
    it("returns same result as toUtcDateString", () => {
      const ts = 1705276800
      expect(getUtcDayKey(ts)).toBe(toUtcDateString(ts))
    })

    it("can be used as Map/object key", () => {
      const map = new Map<string, number>()
      const key1 = getUtcDayKey(1705276800)
      const key2 = getUtcDayKey(1705276800 + 3600) // 1 hour later, same day

      map.set(key1, 1)
      expect(map.get(key2)).toBe(1) // Same key
    })

    it("different days produce different keys", () => {
      const key1 = getUtcDayKey(1705276800) // 2024-01-15
      const key2 = getUtcDayKey(1705363200) // 2024-01-16

      expect(key1).not.toBe(key2)
    })
  })
})
