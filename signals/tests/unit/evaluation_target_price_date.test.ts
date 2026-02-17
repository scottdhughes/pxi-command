import { beforeEach, describe, expect, it, vi } from "vitest"
import type { Env } from "../../src/config"

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

import { evaluatePendingPredictions } from "../../src/evaluation"
import { getPendingPredictions, updatePredictionOutcome } from "../../src/db"
import {
  fetchMultipleHistoricalETFPrices,
  calculateReturn,
  isHit,
} from "../../src/utils/price"

const getPendingPredictionsMock = vi.mocked(getPendingPredictions)
const updatePredictionOutcomeMock = vi.mocked(updatePredictionOutcome)
const fetchMultipleHistoricalETFPricesMock = vi.mocked(fetchMultipleHistoricalETFPrices)
const calculateReturnMock = vi.mocked(calculateReturn)
const isHitMock = vi.mocked(isHit)

describe("evaluatePendingPredictions target-date anchored exits", () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it("evaluates using historical close anchored to target_date", async () => {
    const env = {} as Env
    getPendingPredictionsMock.mockResolvedValue([
      {
        id: 7,
        run_id: "run-1",
        signal_date: "2026-02-17",
        target_date: "2026-02-26",
        theme_id: "nuclear_uranium",
        theme_name: "Nuclear Uranium",
        rank: 1,
        score: 9.1,
        signal_type: "Rotation",
        confidence: "High",
        timing: "Now",
        stars: 5,
        proxy_etf: "URNM",
        entry_price: 100,
        exit_price: null,
        exit_price_date: null,
        return_pct: null,
        evaluated_at: null,
        hit: null,
        evaluation_note: null,
        created_at: "2026-02-17T15:00:00.000Z",
      },
    ])

    fetchMultipleHistoricalETFPricesMock.mockResolvedValue(
      new Map([
        [
          "URNM|2026-02-26",
          {
            symbol: "URNM",
            targetDate: "2026-02-26",
            priceDate: "2026-02-26",
            price: 110,
            currency: "USD",
            error: null,
          },
        ],
      ])
    )

    calculateReturnMock.mockReturnValue(10)
    isHitMock.mockReturnValue(1)

    const result = await evaluatePendingPredictions(env)

    expect(fetchMultipleHistoricalETFPricesMock).toHaveBeenCalledWith([
      { symbol: "URNM", targetDate: "2026-02-26" },
    ])

    expect(updatePredictionOutcomeMock).toHaveBeenCalledWith(env, 7, {
      exitPrice: 110,
      exitPriceDate: "2026-02-26",
      returnPct: 10,
      hit: 1,
      evaluationNote: null,
    })

    expect(result).toEqual({ evaluated: 1, hits: 1, errors: 0 })
  })

  it("marks unresolved historical prices with null hit and evaluation note", async () => {
    const env = {} as Env
    getPendingPredictionsMock.mockResolvedValue([
      {
        id: 8,
        run_id: "run-2",
        signal_date: "2026-02-18",
        target_date: "2026-02-27",
        theme_id: "defense_aerospace",
        theme_name: "Defense Aerospace",
        rank: 2,
        score: 8.2,
        signal_type: "Rotation",
        confidence: "Medium",
        timing: "Building",
        stars: 4,
        proxy_etf: "ITA",
        entry_price: 90,
        exit_price: null,
        exit_price_date: null,
        return_pct: null,
        evaluated_at: null,
        hit: null,
        evaluation_note: null,
        created_at: "2026-02-18T15:00:00.000Z",
      },
    ])

    fetchMultipleHistoricalETFPricesMock.mockResolvedValue(
      new Map([
        [
          "ITA|2026-02-27",
          {
            symbol: "ITA",
            targetDate: "2026-02-27",
            priceDate: null,
            price: null,
            currency: "USD",
            error: "No close price on/after 2026-02-27 within +10 calendar days",
          },
        ],
      ])
    )

    const result = await evaluatePendingPredictions(env)

    expect(updatePredictionOutcomeMock).toHaveBeenCalledWith(env, 8, {
      exitPrice: null,
      exitPriceDate: null,
      returnPct: null,
      hit: null,
      evaluationNote: "No close price on/after 2026-02-27 within +10 calendar days",
    })

    expect(result).toEqual({ evaluated: 1, hits: 0, errors: 1 })
  })
})
