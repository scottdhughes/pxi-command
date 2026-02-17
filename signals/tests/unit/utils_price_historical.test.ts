import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import {
  fetchHistoricalETFPriceOnOrAfter,
  fetchMultipleHistoricalETFPrices,
  historicalPriceRequestKey,
} from "../../src/utils/price"

function mockFetchJson(payload: unknown, opts: { ok?: boolean; status?: number; statusText?: string } = {}) {
  const ok = opts.ok ?? true
  const status = opts.status ?? 200
  const statusText = opts.statusText ?? "OK"

  return vi.fn(async () => ({
    ok,
    status,
    statusText,
    json: async () => payload,
  }))
}

describe("historical ETF price fetching", () => {
  beforeEach(() => {
    vi.restoreAllMocks()
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it("resolves the first valid close on/after target date", async () => {
    const fetchMock = mockFetchJson({
      chart: {
        result: [
          {
            meta: { currency: "USD", symbol: "URNM" },
            timestamp: [
              1772064000, // 2026-02-26
              1772150400, // 2026-02-27
            ],
            indicators: {
              quote: [
                {
                  close: [49.876, 50.2],
                },
              ],
            },
          },
        ],
        error: null,
      },
    })

    vi.stubGlobal("fetch", fetchMock)

    const result = await fetchHistoricalETFPriceOnOrAfter("URNM", "2026-02-26")

    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(result).toMatchObject({
      symbol: "URNM",
      targetDate: "2026-02-26",
      priceDate: "2026-02-26",
      price: 49.88,
      currency: "USD",
      error: null,
    })
  })

  it("returns deterministic error for invalid target date format", async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal("fetch", fetchMock)

    const result = await fetchHistoricalETFPriceOnOrAfter("URNM", "2026/02/26")

    expect(fetchMock).not.toHaveBeenCalled()
    expect(result.error).toBe("Invalid target date format: 2026/02/26")
    expect(result.price).toBeNull()
    expect(result.priceDate).toBeNull()
  })

  it("deduplicates (symbol,targetDate) requests in batch fetch", async () => {
    const fetchMock = mockFetchJson({
      chart: {
        result: [
          {
            meta: { currency: "USD", symbol: "ITA" },
            timestamp: [1772064000],
            indicators: {
              quote: [
                {
                  close: [100],
                },
              ],
            },
          },
        ],
        error: null,
      },
    })

    vi.stubGlobal("fetch", fetchMock)

    const result = await fetchMultipleHistoricalETFPrices([
      { symbol: "ITA", targetDate: "2026-02-26" },
      { symbol: "ITA", targetDate: "2026-02-26" },
    ])

    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(result.size).toBe(1)
    expect(result.get(historicalPriceRequestKey("ITA", "2026-02-26"))?.price).toBe(100)
  })
})
