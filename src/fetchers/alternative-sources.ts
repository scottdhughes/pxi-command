import axios from 'axios';
import yahooFinance from 'yahoo-finance2';
import { format } from 'date-fns';
import { query } from '../db/connection.js';
import type { IndicatorValue } from '../types/indicators.js';

// ============== Alternative Breadth Data ==============
// Using yahoo-finance2 package to calculate breadth

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

// Calculate sector breadth - % of sectors with positive 20-day momentum
export async function fetchSectorBreadth(): Promise<number> {
  try {
    let bullishSectors = 0;

    for (const symbol of SP500_SECTORS) {
      try {
        const quote = await yahooFinance.quote(symbol);
        const price = quote.regularMarketPrice || 0;
        const fiftyDayAvg = quote.fiftyDayAverage || price;

        // Sector is bullish if above 50-day MA
        if (price > fiftyDayAvg) {
          bullishSectors++;
        }
      } catch (e) {
        // Skip failed fetches
      }
    }

    // Return as percentage (0-100)
    return (bullishSectors / SP500_SECTORS.length) * 100;
  } catch (err: any) {
    console.error('Sector breadth error:', err.message);
    throw err;
  }
}

// Calculate small cap relative strength (IWM/SPY)
export async function fetchSmallCapStrength(): Promise<number> {
  try {
    const iwm = await yahooFinance.quote('IWM');
    const spy = await yahooFinance.quote('SPY');

    const iwmPrice = iwm.regularMarketPrice || 0;
    const spyPrice = spy.regularMarketPrice || 0;

    if (spyPrice === 0) return 0;

    // Return ratio * 100 for easier percentile calc
    return (iwmPrice / spyPrice) * 100;
  } catch (err: any) {
    console.error('Small cap strength error:', err.message);
    throw err;
  }
}

// Calculate mid cap relative strength (IJH/SPY)
export async function fetchMidCapStrength(): Promise<number> {
  try {
    const ijh = await yahooFinance.quote('IJH');
    const spy = await yahooFinance.quote('SPY');

    const ijhPrice = ijh.regularMarketPrice || 0;
    const spyPrice = spy.regularMarketPrice || 0;

    if (spyPrice === 0) return 0;

    // Return ratio * 100 for easier percentile calc
    return (ijhPrice / spyPrice) * 100;
  } catch (err: any) {
    console.error('Mid cap strength error:', err.message);
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

// ============== Fear & Greed with fallback ==============

// CNN Fear & Greed Index (primary source)
async function fetchFearGreedFromCNN(): Promise<number | null> {
  try {
    const response = await axios.get(
      'https://production.dataviz.cnn.io/index/fearandgreed/graphdata',
      {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'application/json, text/plain, */*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'gzip, deflate, br',
          'Referer': 'https://www.cnn.com/markets/fear-and-greed',
          'Origin': 'https://www.cnn.com',
          'Connection': 'keep-alive',
          'Sec-Fetch-Dest': 'empty',
          'Sec-Fetch-Mode': 'cors',
          'Sec-Fetch-Site': 'same-site',
        },
        timeout: 10000,
      }
    );

    const score = response.data?.fear_and_greed?.score;
    if (score === undefined) return null;

    // Convert 0-100 Fear/Greed to bull-bear spread (-50 to +50)
    return score - 50;
  } catch (err: any) {
    console.error('CNN Fear & Greed error:', err.message);
    return null;
  }
}

// Alternative.me Crypto Fear & Greed (fallback source)
async function fetchFearGreedFromAlternative(): Promise<number | null> {
  try {
    const response = await axios.get(
      'https://api.alternative.me/fng/',
      { timeout: 10000 }
    );

    const value = response.data?.data?.[0]?.value;
    const score = parseInt(value, 10);
    if (isNaN(score)) return null;

    // Convert 0-100 to bull-bear spread (-50 to +50)
    return score - 50;
  } catch (err: any) {
    console.error('Alternative.me Fear & Greed error:', err.message);
    return null;
  }
}

// Main export: tries CNN first, then Alternative.me
export async function fetchSentimentFromFearGreed(): Promise<number | null> {
  // Try CNN first (traditional market sentiment)
  const cnnResult = await fetchFearGreedFromCNN();
  if (cnnResult !== null) {
    return cnnResult;
  }

  console.log('  CNN Fear & Greed failed, trying Alternative.me...');

  // Fallback to Alternative.me (crypto-focused but correlated)
  const altResult = await fetchFearGreedFromAlternative();
  if (altResult !== null) {
    return altResult;
  }

  console.error('  All Fear & Greed sources failed');
  return null;
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

  // Sector Breadth
  try {
    console.log('  Calculating sector breadth...');
    const sectorBreadth = await fetchSectorBreadth();

    await query(
      `INSERT INTO indicator_values (indicator_id, date, value, source)
       VALUES ('sector_breadth', $1, $2, 'yahoo_calc')
       ON CONFLICT (indicator_id, date) DO UPDATE SET value = EXCLUDED.value`,
      [today, sectorBreadth]
    );
    success.push('sector_breadth');
    console.log(`  ✓ sector_breadth: ${sectorBreadth.toFixed(1)}%`);
  } catch (err: any) {
    console.error(`  ✗ sector_breadth: ${err.message}`);
    failed.push('sector_breadth');
  }

  // Small Cap Strength
  try {
    console.log('  Calculating small cap relative strength...');
    const smallCapStrength = await fetchSmallCapStrength();

    await query(
      `INSERT INTO indicator_values (indicator_id, date, value, source)
       VALUES ('small_cap_strength', $1, $2, 'yahoo_calc')
       ON CONFLICT (indicator_id, date) DO UPDATE SET value = EXCLUDED.value`,
      [today, smallCapStrength]
    );
    success.push('small_cap_strength');
    console.log(`  ✓ small_cap_strength: ${smallCapStrength.toFixed(2)}`);
  } catch (err: any) {
    console.error(`  ✗ small_cap_strength: ${err.message}`);
    failed.push('small_cap_strength');
  }

  // Mid Cap Strength
  try {
    console.log('  Calculating mid cap relative strength...');
    const midCapStrength = await fetchMidCapStrength();

    await query(
      `INSERT INTO indicator_values (indicator_id, date, value, source)
       VALUES ('midcap_strength', $1, $2, 'yahoo_calc')
       ON CONFLICT (indicator_id, date) DO UPDATE SET value = EXCLUDED.value`,
      [today, midCapStrength]
    );
    success.push('midcap_strength');
    console.log(`  ✓ midcap_strength: ${midCapStrength.toFixed(2)}`);
  } catch (err: any) {
    console.error(`  ✗ midcap_strength: ${err.message}`);
    failed.push('midcap_strength');
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

  return { success, failed };
}
