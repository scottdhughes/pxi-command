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
        regularMarketPrice?: number
        currency?: string
        symbol?: string
      }
      timestamp?: number[]
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

const YAHOO_HEADERS = {
  "User-Agent": "Mozilla/5.0 (compatible; PXI-Signals/1.0)",
  Accept: "application/json",
}

function parseIsoDateUtc(isoDate: string): number | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(isoDate)
  if (!match) return null

  const year = Number(match[1])
  const month = Number(match[2])
  const day = Number(match[3])

  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return null
  }

  const utcMs = Date.UTC(year, month - 1, day)
  const roundTrip = new Date(utcMs).toISOString().slice(0, 10)
  return roundTrip === isoDate ? utcMs : null
}

function toIsoDateFromUnixSeconds(unixSeconds: number): string {
  return new Date(unixSeconds * 1000).toISOString().slice(0, 10)
}

export interface PriceResult {
  symbol: string
  price: number | null
  currency: string | null
  error: string | null
}

export interface HistoricalPriceRequest {
  symbol: string
  targetDate: string
}

export interface HistoricalPriceResult extends PriceResult {
  targetDate: string
  priceDate: string | null
}

export function historicalPriceRequestKey(symbol: string, targetDate: string): string {
  return `${symbol.toUpperCase()}|${targetDate}`
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
      headers: YAHOO_HEADERS,
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
      price: Math.round(price * 100) / 100,
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
 * Fetches a historical close for the first market session on/after targetDate.
 *
 * This anchors evaluation exits to the intended prediction horizon rather than
 * the runtime spot price.
 */
export async function fetchHistoricalETFPriceOnOrAfter(
  symbol: string,
  targetDate: string,
  opts: { maxCalendarDaysForward?: number } = {}
): Promise<HistoricalPriceResult> {
  const maxCalendarDaysForward = opts.maxCalendarDaysForward ?? 10
  const targetUtcMs = parseIsoDateUtc(targetDate)

  if (targetUtcMs === null) {
    return {
      symbol,
      targetDate,
      priceDate: null,
      price: null,
      currency: null,
      error: `Invalid target date format: ${targetDate}`,
    }
  }

  const period1 = Math.floor(targetUtcMs / 1000)
  const period2 = period1 + (maxCalendarDaysForward + 1) * 24 * 60 * 60
  const url = `https://query2.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?period1=${period1}&period2=${period2}&interval=1d&events=history&includePrePost=false`

  try {
    const response = await fetch(url, {
      headers: YAHOO_HEADERS,
    })

    if (!response.ok) {
      return {
        symbol,
        targetDate,
        priceDate: null,
        price: null,
        currency: null,
        error: `HTTP ${response.status}: ${response.statusText}`,
      }
    }

    const data = (await response.json()) as YahooChartResponse

    if (data.chart.error) {
      return {
        symbol,
        targetDate,
        priceDate: null,
        price: null,
        currency: null,
        error: data.chart.error.description || data.chart.error.code,
      }
    }

    if (!data.chart.result || data.chart.result.length === 0) {
      return {
        symbol,
        targetDate,
        priceDate: null,
        price: null,
        currency: null,
        error: "No data returned",
      }
    }

    const result = data.chart.result[0]
    const timestamps = result.timestamp || []
    const closes = result.indicators.quote?.[0]?.close || []

    let resolvedDate: string | null = null
    let resolvedPrice: number | null = null

    for (let i = 0; i < timestamps.length; i++) {
      const isoDate = toIsoDateFromUnixSeconds(timestamps[i])
      const close = closes[i]

      if (isoDate < targetDate) continue
      if (typeof close !== "number" || Number.isNaN(close)) continue

      resolvedDate = isoDate
      resolvedPrice = Math.round(close * 100) / 100
      break
    }

    if (resolvedDate === null || resolvedPrice === null) {
      return {
        symbol,
        targetDate,
        priceDate: null,
        price: null,
        currency: result.meta.currency || "USD",
        error: `No close price on/after ${targetDate} within +${maxCalendarDaysForward} calendar days`,
      }
    }

    return {
      symbol,
      targetDate,
      priceDate: resolvedDate,
      price: resolvedPrice,
      currency: result.meta.currency || "USD",
      error: null,
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    return {
      symbol,
      targetDate,
      priceDate: null,
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
 * Fetches historical prices for multiple (symbol, targetDate) requests in parallel.
 *
 * @returns Map keyed by historicalPriceRequestKey(symbol, targetDate)
 */
export async function fetchMultipleHistoricalETFPrices(
  requests: HistoricalPriceRequest[]
): Promise<Map<string, HistoricalPriceResult>> {
  const deduped = new Map<string, HistoricalPriceRequest>()
  for (const request of requests) {
    if (!request.symbol || !request.targetDate) continue
    deduped.set(historicalPriceRequestKey(request.symbol, request.targetDate), request)
  }

  const entries = await Promise.all(
    [...deduped.entries()].map(async ([key, request]) => {
      const result = await fetchHistoricalETFPriceOnOrAfter(request.symbol, request.targetDate)
      return [key, result] as const
    })
  )

  return new Map(entries)
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
