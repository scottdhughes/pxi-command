/**
 * Yahoo Finance price fetching utility
 *
 * Provides functions to fetch ETF/stock prices for signal evaluation.
 * Uses the Yahoo Finance v8 chart API endpoint.
 */

interface YahooChartResponse {
  chart: {
    result: Array<{
      meta: {
        regularMarketPrice: number
        currency: string
        symbol: string
      }
      indicators: {
        quote: Array<{
          close: (number | null)[]
        }>
      }
    }> | null
    error: {
      code: string
      description: string
    } | null
  }
}

export interface PriceResult {
  symbol: string
  price: number | null
  currency: string | null
  error: string | null
}

/**
 * Fetches the latest closing price for a symbol from Yahoo Finance.
 *
 * @param symbol - Ticker symbol (e.g., "URNM", "XLU")
 * @returns PriceResult with price if successful, error message if failed
 */
export async function fetchETFPrice(symbol: string): Promise<PriceResult> {
  const url = `https://query2.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=5d&interval=1d`

  try {
    const response = await fetch(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; PXI-Signals/1.0)",
        Accept: "application/json",
      },
    })

    if (!response.ok) {
      return {
        symbol,
        price: null,
        currency: null,
        error: `HTTP ${response.status}: ${response.statusText}`,
      }
    }

    const data = (await response.json()) as YahooChartResponse

    if (data.chart.error) {
      return {
        symbol,
        price: null,
        currency: null,
        error: data.chart.error.description || data.chart.error.code,
      }
    }

    if (!data.chart.result || data.chart.result.length === 0) {
      return {
        symbol,
        price: null,
        currency: null,
        error: "No data returned",
      }
    }

    const result = data.chart.result[0]
    const price = result.meta.regularMarketPrice

    if (typeof price !== "number" || isNaN(price)) {
      return {
        symbol,
        price: null,
        currency: null,
        error: "Invalid price data",
      }
    }

    return {
      symbol,
      price: Math.round(price * 100) / 100, // Round to 2 decimal places
      currency: result.meta.currency || "USD",
      error: null,
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    return {
      symbol,
      price: null,
      currency: null,
      error: `Fetch failed: ${message}`,
    }
  }
}

/**
 * Fetches prices for multiple symbols in parallel.
 *
 * @param symbols - Array of ticker symbols
 * @returns Map of symbol to PriceResult
 */
export async function fetchMultipleETFPrices(
  symbols: string[]
): Promise<Map<string, PriceResult>> {
  const uniqueSymbols = [...new Set(symbols.filter(Boolean))]
  const results = await Promise.all(uniqueSymbols.map(fetchETFPrice))

  const priceMap = new Map<string, PriceResult>()
  for (const result of results) {
    priceMap.set(result.symbol, result)
  }

  return priceMap
}

/**
 * Calculates percentage return between entry and exit prices.
 *
 * @param entryPrice - Price at signal generation
 * @param exitPrice - Price at evaluation time
 * @returns Percentage return (positive = gain, negative = loss)
 */
export function calculateReturn(entryPrice: number, exitPrice: number): number {
  if (entryPrice <= 0) return 0
  const returnPct = ((exitPrice - entryPrice) / entryPrice) * 100
  return Math.round(returnPct * 100) / 100 // Round to 2 decimal places
}

/**
 * Determines if a prediction was a "hit" based on return direction.
 * A hit means the ETF moved in the expected direction (positive return).
 *
 * @param returnPct - Calculated percentage return
 * @returns 1 for hit, 0 for miss
 */
export function isHit(returnPct: number): number {
  return returnPct > 0 ? 1 : 0
}
