import axios from 'axios';
import * as cheerio from 'cheerio';
import { format, parse, subDays } from 'date-fns';
import { query } from '../db/connection.js';
import type { IndicatorValue } from '../types/indicators.js';
import {
  resilientFetch,
  isCircuitOpen,
  createFetchSummary,
  type FetchSummary,
} from '../utils/resilience.js';

// ============== AAII Sentiment Survey ==============

interface AAIISentiment {
  date: Date;
  bullish: number;
  neutral: number;
  bearish: number;
  bullBearSpread: number;
}

export async function fetchAAIISentiment(): Promise<IndicatorValue[]> {
  const SOURCE_NAME = 'aaii';

  // Check circuit breaker
  if (isCircuitOpen(SOURCE_NAME)) {
    console.log('  ⚡ Circuit open for AAII, skipping');
    return [];
  }

  return resilientFetch(
    async () => {
      // AAII provides historical data in a table format
      const response = await axios.get(
        'https://www.aaii.com/sentimentsurvey/sent_results',
        {
          headers: {
            'User-Agent':
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
          },
          timeout: 15000,
        }
      );

      const $ = cheerio.load(response.data);
      const results: IndicatorValue[] = [];

      // Parse the sentiment table
      $('table.sentimenttable tbody tr').each((_, row) => {
        const cols = $(row).find('td');
        if (cols.length >= 4) {
          const dateStr = $(cols[0]).text().trim();
          const bullish = parseFloat($(cols[1]).text().replace('%', ''));
          const neutral = parseFloat($(cols[2]).text().replace('%', ''));
          const bearish = parseFloat($(cols[3]).text().replace('%', ''));

          if (!isNaN(bullish) && !isNaN(bearish)) {
            try {
              const date = parse(dateStr, 'MM/dd/yyyy', new Date());
              results.push({
                indicatorId: 'aaii_sentiment',
                date,
                value: bullish - bearish, // Bull-Bear spread
              });
            } catch {
              // Skip invalid dates
            }
          }
        }
      });

      // If table parsing fails, try JSON endpoint
      if (results.length === 0) {
        console.log('  Trying AAII JSON endpoint...');
        const jsonResponse = await axios.get(
          'https://www.aaii.com/files/surveys/sentiment.xls',
          { responseType: 'text', timeout: 15000 }
        );
        // Parse XLS/CSV format if available
      }

      return results;
    },
    {
      sourceName: SOURCE_NAME,
      maxAttempts: 3,
      baseDelayMs: 1000,
      timeoutMs: 30000,
    }
  );
}

// ============== Farside BTC ETF Flows ==============

interface FarsideFlow {
  date: Date;
  totalFlow: number;
}

// Primary: Try Farside scraper
async function fetchBtcEtfFlowsFromFarside(): Promise<IndicatorValue[]> {
  // Farside publishes daily ETF flow data
  const response = await axios.get(
    'https://farside.co.uk/bitcoin-etf-flow-all-data/',
    {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
      },
      timeout: 15000,
    }
  );

  // Check for Cloudflare challenge
  if (response.data.includes('challenge-platform') || response.data.includes('Just a moment')) {
    throw new Error('Cloudflare challenge detected');
  }

  const $ = cheerio.load(response.data);
  const dailyFlows: Map<string, number> = new Map();

  // Parse the flow table
  $('table tbody tr').each((_, row) => {
    const cols = $(row).find('td');
    if (cols.length >= 2) {
      const dateStr = $(cols[0]).text().trim();
      const totalCol = $(cols).last().text().trim();

      // Parse total flow (last column usually)
      const flowMatch = totalCol.match(/-?[\d.]+/);
      if (flowMatch && dateStr) {
        try {
          // Try different date formats
          let date: Date;
          if (dateStr.includes('/')) {
            date = parse(dateStr, 'dd/MM/yyyy', new Date());
          } else {
            date = parse(dateStr, 'd MMM yyyy', new Date());
          }

          const flow = parseFloat(flowMatch[0]);
          if (!isNaN(flow)) {
            dailyFlows.set(format(date, 'yyyy-MM-dd'), flow);
          }
        } catch {
          // Skip invalid dates
        }
      }
    }
  });

  // Calculate 7-day rolling sum
  const sortedDates = Array.from(dailyFlows.keys()).sort();
  const results: IndicatorValue[] = [];

  for (let i = 6; i < sortedDates.length; i++) {
    let sum = 0;
    for (let j = i - 6; j <= i; j++) {
      sum += dailyFlows.get(sortedDates[j]) || 0;
    }

    results.push({
      indicatorId: 'btc_etf_flows',
      date: new Date(sortedDates[i]),
      value: sum,
    });
  }

  return results;
}

// Fallback: Calculate implied flows from Yahoo Finance BTC ETF data
async function fetchBtcEtfFlowsFromYahoo(): Promise<IndicatorValue[]> {
  // Import yahoo-finance2 dynamically to avoid issues if not installed
  const yahooFinance = await import('yahoo-finance2').then(m => m.default);

  // Major BTC ETFs
  const etfSymbols = ['IBIT', 'FBTC', 'GBTC', 'ARKB', 'BITB'];
  const endDate = new Date();
  const startDate = subDays(endDate, 45); // Get ~45 days of data

  const dailyVolumes: Map<string, number> = new Map();

  for (const symbol of etfSymbols) {
    try {
      const history = await yahooFinance.historical(symbol, {
        period1: startDate,
        period2: endDate,
        interval: '1d',
      });

      for (const day of history) {
        const dateStr = format(day.date, 'yyyy-MM-dd');
        // Use volume * close price as a proxy for dollar flow
        // Positive volume = inflow approximation
        const dollarVolume = (day.volume || 0) * (day.close || 0);
        const priceChange = day.close - day.open;
        // If price went up with volume, likely inflow; if down, likely outflow
        const impliedFlow = priceChange >= 0 ? dollarVolume / 1e6 : -dollarVolume / 1e6;

        const existing = dailyVolumes.get(dateStr) || 0;
        dailyVolumes.set(dateStr, existing + impliedFlow);
      }
    } catch (err) {
      console.warn(`  Warning: Could not fetch ${symbol} ETF data`);
    }
  }

  // Calculate 7-day rolling sum
  const sortedDates = Array.from(dailyVolumes.keys()).sort();
  const results: IndicatorValue[] = [];

  for (let i = 6; i < sortedDates.length; i++) {
    let sum = 0;
    for (let j = i - 6; j <= i; j++) {
      sum += dailyVolumes.get(sortedDates[j]) || 0;
    }

    results.push({
      indicatorId: 'btc_etf_flows',
      date: new Date(sortedDates[i]),
      value: sum,
    });
  }

  return results;
}

export async function fetchBtcEtfFlows(): Promise<IndicatorValue[]> {
  const SOURCE_NAME = 'farside';

  // Check circuit breaker
  if (isCircuitOpen(SOURCE_NAME)) {
    console.log('  ⚡ Circuit open for Farside, trying Yahoo fallback');
    try {
      return await fetchBtcEtfFlowsFromYahoo();
    } catch (yahooErr: any) {
      console.error('  ✗ Yahoo fallback failed:', yahooErr.message);
      return [];
    }
  }

  return resilientFetch(
    async () => {
      try {
        // Try Farside first
        const farsideData = await fetchBtcEtfFlowsFromFarside();
        if (farsideData.length > 0) {
          console.log('  ✓ Farside scraper succeeded');
          return farsideData;
        }
        throw new Error('No data from Farside');
      } catch (farsideErr: any) {
        console.warn(`  ⚠ Farside failed (${farsideErr.message}), trying Yahoo fallback...`);

        // Fallback to Yahoo Finance implied flows
        try {
          const yahooData = await fetchBtcEtfFlowsFromYahoo();
          if (yahooData.length > 0) {
            console.log('  ✓ Yahoo fallback succeeded');
            return yahooData;
          }
        } catch (yahooErr: any) {
          console.error('  ✗ Yahoo fallback also failed:', yahooErr.message);
        }

        // Re-throw original error if both fail
        throw farsideErr;
      }
    },
    {
      sourceName: SOURCE_NAME,
      maxAttempts: 2,
      baseDelayMs: 1000,
      timeoutMs: 60000,  // Longer timeout for Yahoo fallback
    }
  );
}

// ============== CBOE Put/Call Ratio ==============

export async function fetchCboePutCallRatio(): Promise<IndicatorValue[]> {
  const SOURCE_NAME = 'cboe';

  // Check circuit breaker
  if (isCircuitOpen(SOURCE_NAME)) {
    console.log('  ⚡ Circuit open for CBOE, skipping');
    return [];
  }

  return resilientFetch(
    async () => {
      // CBOE provides historical P/C data
      const response = await axios.get(
        'https://www.cboe.com/us/options/market_statistics/daily/',
        {
          headers: {
            'User-Agent':
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
          },
          timeout: 15000,
        }
      );

      const $ = cheerio.load(response.data);
      const results: IndicatorValue[] = [];

      // Try to find equity put/call ratio data
      // CBOE format varies - may need adjustment
      $('table tbody tr').each((_, row) => {
        const cols = $(row).find('td');
        const text = $(row).text();

        if (text.toLowerCase().includes('equity') && cols.length >= 4) {
          const dateStr = $(cols[0]).text().trim();
          const ratio = parseFloat($(cols[3]).text().trim());

          if (!isNaN(ratio) && dateStr) {
            try {
              const date = parse(dateStr, 'MM/dd/yyyy', new Date());
              results.push({
                indicatorId: 'put_call_ratio',
                date,
                value: ratio,
              });
            } catch {
              // Skip
            }
          }
        }
      });

      // Fallback: try the CSV download
      if (results.length === 0) {
        console.log('  Trying CBOE CSV endpoint...');
        const csvResponse = await axios.get(
          'https://www.cboe.com/us/options/market_statistics/daily/equity_put_call_ratio_data.csv',
          { responseType: 'text', timeout: 15000 }
        );

        const lines = csvResponse.data.split('\n');
        for (let i = 1; i < lines.length; i++) {
          const parts = lines[i].split(',');
          if (parts.length >= 2) {
            const dateStr = parts[0].trim();
            const ratio = parseFloat(parts[1]);

            if (!isNaN(ratio) && dateStr) {
              try {
                const date = new Date(dateStr);
                if (!isNaN(date.getTime())) {
                  results.push({
                    indicatorId: 'put_call_ratio',
                    date,
                    value: ratio,
                  });
                }
              } catch {
                // Skip
              }
            }
          }
        }
      }

      return results;
    },
    {
      sourceName: SOURCE_NAME,
      maxAttempts: 3,
      baseDelayMs: 1000,
      timeoutMs: 30000,
    }
  );
}

// ============== Barchart Breadth Data ==============

export async function fetchBarchartBreadth(): Promise<{
  above200dma: IndicatorValue[];
  above50dma: IndicatorValue[];
  highsLows: IndicatorValue[];
}> {
  const SOURCE_NAME = 'barchart';

  // Note: Barchart requires API key for full historical data
  // This fetches current values from their public pages

  const results = {
    above200dma: [] as IndicatorValue[],
    above50dma: [] as IndicatorValue[],
    highsLows: [] as IndicatorValue[],
  };

  // Check circuit breaker
  if (isCircuitOpen(SOURCE_NAME)) {
    console.log('  ⚡ Circuit open for Barchart, skipping');
    return results;
  }

  try {
    await resilientFetch(
      async () => {
        // S&P 500 stocks above moving averages
        const response = await axios.get(
          'https://www.barchart.com/stocks/indices/sp-500',
          {
            headers: {
              'User-Agent':
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            },
            timeout: 15000,
          }
        );

        const $ = cheerio.load(response.data);
        const today = new Date();

        // Look for breadth stats in the page
        $('[data-ng-bind*="above"]').each((_, el) => {
          const text = $(el).text();
          const value = parseFloat(text.replace('%', ''));

          if (!isNaN(value)) {
            const context = $(el).parent().text().toLowerCase();
            if (context.includes('200')) {
              results.above200dma.push({
                indicatorId: 'sp500_above_200dma',
                date: today,
                value,
              });
            } else if (context.includes('50')) {
              results.above50dma.push({
                indicatorId: 'sp500_above_50dma',
                date: today,
                value,
              });
            }
          }
        });

        // NYSE Highs-Lows
        const nyseResponse = await axios.get(
          'https://www.barchart.com/stocks/highs-lows/nyse',
          {
            headers: {
              'User-Agent':
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            },
            timeout: 15000,
          }
        );

        const $nyse = cheerio.load(nyseResponse.data);
        const highsText = $nyse('[data-ng-bind*="highs"]').first().text();
        const lowsText = $nyse('[data-ng-bind*="lows"]').first().text();

        const highs = parseInt(highsText) || 0;
        const lows = parseInt(lowsText) || 0;

        if (highs > 0 || lows > 0) {
          results.highsLows.push({
            indicatorId: 'nyse_highs_lows',
            date: today,
            value: highs - lows,
          });
        }

        return results;
      },
      {
        sourceName: SOURCE_NAME,
        maxAttempts: 3,
        baseDelayMs: 1000,
        timeoutMs: 30000,
      }
    );
  } catch (err: any) {
    console.error('Barchart fetch error:', err.message);
  }

  return results;
}

// ============== Combined Scrapers ==============

export async function saveScrapedData(
  indicatorId: string,
  values: IndicatorValue[],
  source: string
): Promise<number> {
  if (values.length === 0) return 0;

  let inserted = 0;

  for (const val of values) {
    try {
      await query(
        `INSERT INTO indicator_values (indicator_id, date, value, source)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (indicator_id, date)
         DO UPDATE SET value = EXCLUDED.value, fetched_at = NOW()`,
        [indicatorId, format(val.date, 'yyyy-MM-dd'), val.value, source]
      );
      inserted++;
    } catch (err) {
      console.error(`Error saving ${indicatorId}:`, err);
    }
  }

  return inserted;
}

export async function fetchAllScrapedIndicators(): Promise<{
  success: string[];
  failed: string[];
  skipped: string[];
  summaries: FetchSummary[];
}> {
  const success: string[] = [];
  const failed: string[] = [];
  const skipped: string[] = [];
  const summaries: FetchSummary[] = [];

  console.log('\n🕸️ Fetching scraped indicators...\n');

  // AAII Sentiment
  try {
    console.log('  Fetching AAII sentiment...');
    const aaii = await fetchAAIISentiment();
    if (aaii.length === 0 && isCircuitOpen('aaii')) {
      skipped.push('aaii_sentiment');
      summaries.push(createFetchSummary('aaii', false, 0, 'Circuit breaker open'));
    } else {
      const saved = await saveScrapedData('aaii_sentiment', aaii, 'aaii');
      console.log(`  ✓ aaii_sentiment: ${saved} records saved`);
      success.push('aaii_sentiment');
      summaries.push(createFetchSummary('aaii', true, saved));
    }
  } catch (err: any) {
    console.error(`  ✗ aaii_sentiment: ${err.message}`);
    failed.push('aaii_sentiment');
    summaries.push(createFetchSummary('aaii', false, 0, err.message));
  }

  // BTC ETF Flows
  try {
    console.log('  Fetching BTC ETF flows...');
    const flows = await fetchBtcEtfFlows();
    if (flows.length === 0 && isCircuitOpen('farside')) {
      skipped.push('btc_etf_flows');
      summaries.push(createFetchSummary('farside', false, 0, 'Circuit breaker open'));
    } else {
      const saved = await saveScrapedData('btc_etf_flows', flows, 'farside');
      console.log(`  ✓ btc_etf_flows: ${saved} records saved`);
      success.push('btc_etf_flows');
      summaries.push(createFetchSummary('farside', true, saved));
    }
  } catch (err: any) {
    console.error(`  ✗ btc_etf_flows: ${err.message}`);
    failed.push('btc_etf_flows');
    summaries.push(createFetchSummary('farside', false, 0, err.message));
  }

  // CBOE Put/Call
  try {
    console.log('  Fetching CBOE put/call ratio...');
    const pcRatio = await fetchCboePutCallRatio();
    if (pcRatio.length === 0 && isCircuitOpen('cboe')) {
      skipped.push('put_call_ratio');
      summaries.push(createFetchSummary('cboe', false, 0, 'Circuit breaker open'));
    } else {
      const saved = await saveScrapedData('put_call_ratio', pcRatio, 'cboe');
      console.log(`  ✓ put_call_ratio: ${saved} records saved`);
      success.push('put_call_ratio');
      summaries.push(createFetchSummary('cboe', true, saved));
    }
  } catch (err: any) {
    console.error(`  ✗ put_call_ratio: ${err.message}`);
    failed.push('put_call_ratio');
    summaries.push(createFetchSummary('cboe', false, 0, err.message));
  }

  // Barchart Breadth
  try {
    console.log('  Fetching Barchart breadth data...');
    const breadth = await fetchBarchartBreadth();

    let breadthSaved = 0;
    if (breadth.above200dma.length > 0) {
      breadthSaved += await saveScrapedData('sp500_above_200dma', breadth.above200dma, 'barchart');
      success.push('sp500_above_200dma');
    }
    if (breadth.above50dma.length > 0) {
      breadthSaved += await saveScrapedData('sp500_above_50dma', breadth.above50dma, 'barchart');
      success.push('sp500_above_50dma');
    }
    if (breadth.highsLows.length > 0) {
      breadthSaved += await saveScrapedData('nyse_highs_lows', breadth.highsLows, 'barchart');
      success.push('nyse_highs_lows');
    }

    if (breadthSaved === 0 && isCircuitOpen('barchart')) {
      skipped.push('breadth');
      summaries.push(createFetchSummary('barchart', false, 0, 'Circuit breaker open'));
    } else {
      console.log(`  ✓ breadth data saved (${breadthSaved} records)`);
      summaries.push(createFetchSummary('barchart', true, breadthSaved));
    }
  } catch (err: any) {
    console.error(`  ✗ breadth: ${err.message}`);
    failed.push('breadth');
    summaries.push(createFetchSummary('barchart', false, 0, err.message));
  }

  // Log summary
  console.log('\n📊 Scraper Summary:');
  console.log(`  ✓ Success: ${success.length} indicators`);
  console.log(`  ✗ Failed: ${failed.length} indicators`);
  console.log(`  ⚡ Skipped (circuit open): ${skipped.length} indicators`);

  return { success, failed, skipped, summaries };
}
