import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import type { Env } from "../../src/config"
import type { ThemeReportItem } from "../../src/report/render_json"

vi.mock("../../src/db", () => ({
  getPendingPredictions: vi.fn(),
  updatePredictionOutcome: vi.fn(),
  insertSignalPredictions: vi.fn(),
  getExistingPredictionThemesForDate: vi.fn(),
}))

vi.mock("../../src/utils/price", () => ({
  fetchMultipleETFPrices: vi.fn(),
  fetchMultipleHistoricalETFPrices: vi.fn(),
  historicalPriceRequestKey: (symbol: string, targetDate: string) => `${symbol.toUpperCase()}|${targetDate}`,
  calculateReturn: vi.fn(),
  isHit: vi.fn(),
}))

import { storePredictions } from "../../src/evaluation"
import { getExistingPredictionThemesForDate, insertSignalPredictions } from "../../src/db"
import { fetchMultipleETFPrices } from "../../src/utils/price"

const getExistingPredictionThemesForDateMock = vi.mocked(getExistingPredictionThemesForDate)
const insertSignalPredictionsMock = vi.mocked(insertSignalPredictions)
const fetchMultipleETFPricesMock = vi.mocked(fetchMultipleETFPrices)

function makeThemeItem(themeId: string, rank: number): ThemeReportItem {
  return {
    rank,
    theme_id: themeId,
    theme_name: themeId,
    score: 1,
    classification: {
      signal_type: "Rotation",
      confidence: "High",
      timing: "Building",
      stars: 5,
    },
    metrics: {} as ThemeReportItem["metrics"],
    scoring: {} as ThemeReportItem["scoring"],
    evidence_links: [],
    key_tickers: [],
  }
}

describe("storePredictions duplicate protection", () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date("2026-02-17T15:00:00.000Z"))

    getExistingPredictionThemesForDateMock.mockResolvedValue(new Set())
    insertSignalPredictionsMock.mockResolvedValue()

    fetchMultipleETFPricesMock.mockResolvedValue(
      new Map([
        ["URNM", { symbol: "URNM", price: 10, currency: "USD", error: null }],
        ["ITA", { symbol: "ITA", price: 20, currency: "USD", error: null }],
      ])
    )
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.clearAllMocks()
  })

  it("stores only themes that do not already exist for the signal date", async () => {
    const env = {} as Env
    const ranked: ThemeReportItem[] = [
      makeThemeItem("nuclear_uranium", 1),
      makeThemeItem("defense_aerospace", 2),
    ]

    getExistingPredictionThemesForDateMock.mockResolvedValue(new Set(["nuclear_uranium"]))

    const stored = await storePredictions(env, "run-1", ranked)

    expect(getExistingPredictionThemesForDateMock).toHaveBeenCalledWith(env, "2026-02-17")
    expect(insertSignalPredictionsMock).toHaveBeenCalledTimes(1)

    const inserted = insertSignalPredictionsMock.mock.calls[0]?.[1] ?? []
    expect(inserted).toHaveLength(1)
    expect(inserted[0]?.theme_id).toBe("defense_aerospace")
    expect(stored).toBe(1)
  })

  it("skips insertion when all ranked themes already exist for the signal date", async () => {
    const env = {} as Env
    const ranked: ThemeReportItem[] = [
      makeThemeItem("nuclear_uranium", 1),
      makeThemeItem("defense_aerospace", 2),
    ]

    getExistingPredictionThemesForDateMock.mockResolvedValue(
      new Set(["nuclear_uranium", "defense_aerospace"])
    )

    const stored = await storePredictions(env, "run-2", ranked)

    expect(insertSignalPredictionsMock).not.toHaveBeenCalled()
    expect(stored).toBe(0)
  })
})
