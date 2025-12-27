import axios from 'axios';
import { format } from 'date-fns';
import { query } from '../db/connection.js';
import type { IndicatorValue } from '../types/indicators.js';

// ============== Alternative Breadth Data ==============
// Using free Yahoo Finance data to calculate breadth ourselves

const SP500_SECTORS = [
  'XLK', // Technology
  'XLF', // Financials
  'XLV', // Healthcare
  'XLE', // Energy
  'XLI', // Industrials
  'XLY', // Consumer Discretionary
  'XLP', // Consumer Staples
  'XLU', // Utilities
  'XLB', // Materials
  'XLRE', // Real Estate
  'XLC', // Communication
];

// Major components for breadth approximation
const BREADTH_TICKERS = [
  'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B', 'UNH', 'JNJ',
  'JPM', 'V', 'PG', 'XOM', 'HD', 'CVX', 'MA', 'ABBV', 'MRK', 'PEP',
  'KO', 'COST', 'AVGO', 'TMO', 'WMT', 'MCD', 'CSCO', 'ACN', 'ABT', 'DHR',
  'NEE', 'LLY', 'VZ', 'ADBE', 'NKE', 'TXN', 'PM', 'WFC', 'RTX', 'BMY',
  'COP', 'QCOM', 'UPS', 'MS', 'HON', 'ORCL', 'INTC', 'IBM', 'AMD', 'CAT',
];

interface YahooQuote {
  symbol: string;
  regularMarketPrice: number;
  fiftyDayAverage: number;
  twoHundredDayAverage: number;
}

export async function fetchBreadthFromYahoo(): Promise<{
  above50dma: number;
  above200dma: number;
}> {
  try {
    // Fetch quotes for breadth tickers
    const symbols = BREADTH_TICKERS.join(',');
    const response = await axios.get(
      `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${symbols}`,
      {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        },
      }
    );

    const quotes = response.data?.quoteResponse?.result || [];

    let above50 = 0;
    let above200 = 0;
    let total = 0;

    for (const q of quotes as YahooQuote[]) {
      if (q.regularMarketPrice && q.fiftyDayAverage && q.twoHundredDayAverage) {
        total++;
        if (q.regularMarketPrice > q.fiftyDayAverage) above50++;
        if (q.regularMarketPrice > q.twoHundredDayAverage) above200++;
      }
    }

    return {
      above50dma: total > 0 ? (above50 / total) * 100 : 50,
      above200dma: total > 0 ? (above200 / total) * 100 : 50,
    };
  } catch (err: any) {
    console.error('Yahoo breadth fetch error:', err.message);
    throw err;
  }
}

// ============== CBOE Put/Call from alternative source ==============

export async function fetchPutCallFromYahoo(): Promise<number | null> {
  try {
    // Use VIX as a volatility proxy for put/call sentiment
    // When VIX is high, P/C ratio tends to be high
    // This is a rough approximation
    const response = await axios.get(
      'https://query1.finance.yahoo.com/v7/finance/quote?symbols=^VIX',
      {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        },
      }
    );

    const vix = response.data?.quoteResponse?.result?.[0]?.regularMarketPrice;
    if (!vix) return null;

    // Approximate P/C ratio from VIX
    // VIX 12 -> P/C ~0.6, VIX 20 -> P/C ~0.85, VIX 30 -> P/C ~1.1
    const pcRatio = 0.4 + (vix / 50);
    return Math.min(1.5, Math.max(0.4, pcRatio));
  } catch (err: any) {
    console.error('Put/Call approximation error:', err.message);
    return null;
  }
}

// ============== AAII Sentiment approximation ==============

export async function fetchSentimentFromFearGreed(): Promise<number | null> {
  try {
    // CNN Fear & Greed Index as sentiment proxy
    const response = await axios.get(
      'https://production.dataviz.cnn.io/index/fearandgreed/graphdata',
      {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        },
      }
    );

    const score = response.data?.fear_and_greed?.score;
    if (score === undefined) return null;

    // Convert 0-100 Fear/Greed to bull-bear spread (-50 to +50)
    // F&G 0 = extreme fear = bearish, F&G 100 = extreme greed = bullish
    const bullBearSpread = (score - 50);
    return bullBearSpread;
  } catch (err: any) {
    console.error('Fear & Greed fetch error:', err.message);
    return null;
  }
}

// ============== NYSE Highs/Lows from WSJ ==============

export async function fetchHighsLowsApprox(): Promise<number | null> {
  try {
    // Use sector ETF momentum as proxy for highs/lows
    const symbols = SP500_SECTORS.join(',');
    const response = await axios.get(
      `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${symbols}`,
      {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        },
      }
    );

    const quotes = response.data?.quoteResponse?.result || [];

    let bullish = 0;
    let bearish = 0;

    for (const q of quotes) {
      const change = q.regularMarketChangePercent || 0;
      const above200 = q.regularMarketPrice > q.twoHundredDayAverage;

      if (change > 0 && above200) bullish++;
      else if (change < 0 && !above200) bearish++;
    }

    // Scale to approximate highs-lows range (-200 to +200)
    return (bullish - bearish) * 20;
  } catch (err: any) {
    console.error('Highs/Lows approximation error:', err.message);
    return null;
  }
}

// ============== Fetch and save all alternative data ==============

export async function fetchAlternativeIndicators(): Promise<{
  success: string[];
  failed: string[];
}> {
  const success: string[] = [];
  const failed: string[] = [];
  const today = format(new Date(), 'yyyy-MM-dd');

  console.log('\n📡 Fetching alternative data sources...\n');

  // Breadth data
  try {
    console.log('  Calculating breadth from Yahoo quotes...');
    const breadth = await fetchBreadthFromYahoo();

    await query(
      `INSERT INTO indicator_values (indicator_id, date, value, source)
       VALUES ('sp500_above_50dma', $1, $2, 'yahoo_calc')
       ON CONFLICT (indicator_id, date) DO UPDATE SET value = EXCLUDED.value`,
      [today, breadth.above50dma]
    );
    success.push('sp500_above_50dma');

    await query(
      `INSERT INTO indicator_values (indicator_id, date, value, source)
       VALUES ('sp500_above_200dma', $1, $2, 'yahoo_calc')
       ON CONFLICT (indicator_id, date) DO UPDATE SET value = EXCLUDED.value`,
      [today, breadth.above200dma]
    );
    success.push('sp500_above_200dma');

    console.log(`  ✓ breadth: 50DMA=${breadth.above50dma.toFixed(1)}%, 200DMA=${breadth.above200dma.toFixed(1)}%`);
  } catch (err: any) {
    console.error(`  ✗ breadth: ${err.message}`);
    failed.push('breadth');
  }

  // Put/Call approximation
  try {
    console.log('  Approximating put/call ratio...');
    const pcRatio = await fetchPutCallFromYahoo();
    if (pcRatio !== null) {
      await query(
        `INSERT INTO indicator_values (indicator_id, date, value, source)
         VALUES ('put_call_ratio', $1, $2, 'yahoo_calc')
         ON CONFLICT (indicator_id, date) DO UPDATE SET value = EXCLUDED.value`,
        [today, pcRatio]
      );
      success.push('put_call_ratio');
      console.log(`  ✓ put_call_ratio: ${pcRatio.toFixed(2)}`);
    }
  } catch (err: any) {
    console.error(`  ✗ put_call_ratio: ${err.message}`);
    failed.push('put_call_ratio');
  }

  // Sentiment from Fear & Greed
  try {
    console.log('  Fetching CNN Fear & Greed as sentiment proxy...');
    const sentiment = await fetchSentimentFromFearGreed();
    if (sentiment !== null) {
      await query(
        `INSERT INTO indicator_values (indicator_id, date, value, source)
         VALUES ('aaii_sentiment', $1, $2, 'cnn_fg')
         ON CONFLICT (indicator_id, date) DO UPDATE SET value = EXCLUDED.value`,
        [today, sentiment]
      );
      success.push('aaii_sentiment');
      console.log(`  ✓ aaii_sentiment (proxy): ${sentiment.toFixed(1)}`);
    }
  } catch (err: any) {
    console.error(`  ✗ aaii_sentiment: ${err.message}`);
    failed.push('aaii_sentiment');
  }

  // Highs/Lows approximation
  try {
    console.log('  Approximating NYSE highs/lows...');
    const highsLows = await fetchHighsLowsApprox();
    if (highsLows !== null) {
      await query(
        `INSERT INTO indicator_values (indicator_id, date, value, source)
         VALUES ('nyse_highs_lows', $1, $2, 'yahoo_calc')
         ON CONFLICT (indicator_id, date) DO UPDATE SET value = EXCLUDED.value`,
        [today, highsLows]
      );
      success.push('nyse_highs_lows');
      console.log(`  ✓ nyse_highs_lows: ${highsLows}`);
    }
  } catch (err: any) {
    console.error(`  ✗ nyse_highs_lows: ${err.message}`);
    failed.push('nyse_highs_lows');
  }

  return { success, failed };
}
