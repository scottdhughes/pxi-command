import type { Doc } from "./metrics"
import { TICKER_REGEX, LIMITS } from "./constants"

// ─────────────────────────────────────────────────────────────────────────────
// STOPLIST: Terms that match ticker patterns but are not stock symbols
// ─────────────────────────────────────────────────────────────────────────────

/** Common English words that match ticker patterns (2-5 uppercase letters) */
const COMMON_WORDS = [
  "THE", "AND", "FOR", "WITH", "FROM", "THIS", "THAT", "ABOUT",
  "WILL", "YOUR", "HAVE", "HAS", "HAD", "ARE", "WAS", "WERE",
  "NOT", "ALL", "ANY", "NEW", "NOW", "LOW", "HIGH", "CAN",
  "MAY", "JUST", "ALSO", "VERY", "MUCH", "BEEN", "THAN",
  "OVER", "THEY", "INTO", "ONLY", "SOME", "WHEN", "WHAT",
  "WHICH", "WHO", "HOW", "WHY", "EACH", "BOTH", "THESE",
]

/** Financial/trading terminology that appears frequently in discussions */
const FINANCIAL_TERMS = [
  "BUY", "SELL", "HOLD", "LONG", "SHORT", "CALL", "PUT",
  "PUMP", "DUMP", "YOLO", "FOMO", "HODL", "MOON", "BEAR",
  "BULL", "GAIN", "LOSS", "RISK", "BOND", "FUND", "CASH",
  "DEBT", "LOAN", "RATE", "RISE", "FALL", "DROP", "SPIKE",
  "DIP", "ATH", "ATL", "ROI", "APY", "APR", "NAV", "P/E",
  "EPS", "DIV", "DRIP", "OTC", "ITM", "OTM", "IV", "DTE",
]

/** Market abbreviations, indices, and economic terms */
const MARKET_ABBREVS = [
  "ETF", "ETFS", "IPO", "IPOS", "SEC", "FOMC", "FED",
  "NYSE", "DOW", "SP", "GDP", "CPI", "PPI", "PCE",
  "SPAC", "REIT", "ADR", "AUM", "EBITDA", "PE", "PB",
  "RSI", "MACD", "EMA", "SMA", "VWAP", "TA", "FA",
]

/** Currency codes and geographical abbreviations */
const GEO_CODES = [
  "USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY",
  "US", "UK", "EU", "USA", "ASIA", "EMEA",
]

/** Technology and general business abbreviations */
const TECH_ABBREVS = [
  "AI", "ML", "IT", "CEO", "CFO", "COO", "CTO", "VP",
  "HR", "PR", "API", "SDK", "SaaS", "PaaS", "IaaS",
  "B2B", "B2C", "D2C", "CAGR", "MRR", "ARR", "LTV",
  "CAC", "NPS", "OKR", "KPI", "QA", "R&D", "M&A",
  "ESG", "DEI", "WFH", "RTO", "BYOD",
]

/** Social media and Reddit-specific terms */
const SOCIAL_TERMS = [
  "IMO", "IMHO", "TBH", "AFAIK", "IIRC", "TIL", "ELI",
  "OP", "DD", "TLDR", "PSA", "AMA", "ETA", "FAQ",
]

/**
 * Combined set of all stopwords that should be filtered from ticker extraction.
 * Using a Set for O(1) lookup performance.
 */
export const STOPLIST = new Set([
  ...COMMON_WORDS,
  ...FINANCIAL_TERMS,
  ...MARKET_ABBREVS,
  ...GEO_CODES,
  ...TECH_ABBREVS,
  ...SOCIAL_TERMS,
])

// ─────────────────────────────────────────────────────────────────────────────
// Ticker Extraction
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Extracts potential ticker candidates from raw text.
 * Matches patterns like $TSLA, TSLA, (TSLA) and filters out stopwords.
 *
 * @param text - Raw text to search for tickers
 * @returns Array of unique ticker symbols (uppercase, without $ prefix)
 */
function extractCandidates(text: string): string[] {
  const matches = text.match(TICKER_REGEX) || []
  const tickers: string[] = []
  for (const m of matches) {
    const t = m.startsWith("$") ? m.slice(1) : m
    if (STOPLIST.has(t)) continue
    tickers.push(t)
  }
  return tickers
}

/**
 * Statistics for a single ticker symbol across all documents.
 */
export interface TickerStats {
  /** The ticker symbol (uppercase) */
  ticker: string
  /** Total number of mentions across all documents */
  count: number
  /** Set of unique subreddits where this ticker was mentioned */
  subreddits: Set<string>
}

/**
 * Extracts all ticker symbols from a collection of documents.
 * Returns both per-document tickers and aggregated statistics.
 *
 * @param docs - Array of documents to analyze
 * @returns Object containing:
 *   - stats: Map of ticker → TickerStats with counts and subreddits
 *   - perDoc: Map of document ID → array of tickers found in that document
 */
export function extractTickers(docs: Doc[]): {
  stats: Map<string, TickerStats>
  perDoc: Map<string, string[]>
} {
  const stats = new Map<string, TickerStats>()
  const perDoc = new Map<string, string[]>()

  for (const doc of docs) {
    const tickers = extractCandidates(doc.text)
    perDoc.set(doc.id, tickers)
    for (const t of tickers) {
      const existing = stats.get(t) || { ticker: t, count: 0, subreddits: new Set<string>() }
      existing.count += 1
      existing.subreddits.add(doc.subreddit)
      stats.set(t, existing)
    }
  }

  return { stats, perDoc }
}

/**
 * Scores and ranks tickers by their prominence in the dataset.
 * Score = mention count + (subreddit count × weight)
 *
 * @param stats - Map of ticker statistics from extractTickers()
 * @returns Array of scored tickers, sorted by score descending
 */
export function scoreTickers(stats: Map<string, TickerStats>): Array<{
  ticker: string
  score: number
  count: number
  subreddit_count: number
}> {
  const scored = Array.from(stats.values()).map((s) => ({
    ticker: s.ticker,
    score: s.count + s.subreddits.size * LIMITS.subredditScoreWeight,
    count: s.count,
    subreddit_count: s.subreddits.size,
  }))
  scored.sort((a, b) => b.score - a.score)
  return scored
}
