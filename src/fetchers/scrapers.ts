import axios from 'axios';
import * as cheerio from 'cheerio';
import { format, parse, subDays } from 'date-fns';
import { query } from '../db/connection.js';
import type { IndicatorValue } from '../types/indicators.js';

// ============== AAII Sentiment Survey ==============

interface AAIISentiment {
  date: Date;
  bullish: number;
  neutral: number;
  bearish: number;
  bullBearSpread: number;
}

export async function fetchAAIISentiment(): Promise<IndicatorValue[]> {
  try {
    // AAII provides historical data in a table format
    const response = await axios.get(
      'https://www.aaii.com/sentimentsurvey/sent_results',
      {
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        },
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
        { responseType: 'text' }
      );
      // Parse XLS/CSV format if available
    }

    return results;
  } catch (err: any) {
    console.error('AAII fetch error:', err.message);
    throw err;
  }
}

// ============== Farside BTC ETF Flows ==============

interface FarsideFlow {
  date: Date;
  totalFlow: number;
}

export async function fetchBtcEtfFlows(): Promise<IndicatorValue[]> {
  try {
    // Farside publishes daily ETF flow data
    const response = await axios.get(
      'https://farside.co.uk/bitcoin-etf-flow-all-data/',
      {
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        },
      }
    );

    const $ = cheerio.load(response.data);
    const results: IndicatorValue[] = [];
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
  } catch (err: any) {
    console.error('Farside fetch error:', err.message);
    throw err;
  }
}

// ============== CBOE Put/Call Ratio ==============

export async function fetchCboePutCallRatio(): Promise<IndicatorValue[]> {
  try {
    // CBOE provides historical P/C data
    const response = await axios.get(
      'https://www.cboe.com/us/options/market_statistics/daily/',
      {
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        },
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
        { responseType: 'text' }
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
  } catch (err: any) {
    console.error('CBOE fetch error:', err.message);
    throw err;
  }
}

// ============== Barchart Breadth Data ==============

export async function fetchBarchartBreadth(): Promise<{
  above200dma: IndicatorValue[];
  above50dma: IndicatorValue[];
  highsLows: IndicatorValue[];
}> {
  // Note: Barchart requires API key for full historical data
  // This fetches current values from their public pages

  const results = {
    above200dma: [] as IndicatorValue[],
    above50dma: [] as IndicatorValue[],
    highsLows: [] as IndicatorValue[],
  };

  try {
    // S&P 500 stocks above moving averages
    const response = await axios.get(
      'https://www.barchart.com/stocks/indices/sp-500',
      {
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        },
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
}> {
  const success: string[] = [];
  const failed: string[] = [];

  console.log('\n🕸️ Fetching scraped indicators...\n');

  // AAII Sentiment
  try {
    console.log('  Fetching AAII sentiment...');
    const aaii = await fetchAAIISentiment();
    const saved = await saveScrapedData('aaii_sentiment', aaii, 'aaii');
    console.log(`  ✓ aaii_sentiment: ${saved} records saved`);
    success.push('aaii_sentiment');
  } catch (err: any) {
    console.error(`  ✗ aaii_sentiment: ${err.message}`);
    failed.push('aaii_sentiment');
  }

  // BTC ETF Flows
  try {
    console.log('  Fetching BTC ETF flows...');
    const flows = await fetchBtcEtfFlows();
    const saved = await saveScrapedData('btc_etf_flows', flows, 'farside');
    console.log(`  ✓ btc_etf_flows: ${saved} records saved`);
    success.push('btc_etf_flows');
  } catch (err: any) {
    console.error(`  ✗ btc_etf_flows: ${err.message}`);
    failed.push('btc_etf_flows');
  }

  // CBOE Put/Call
  try {
    console.log('  Fetching CBOE put/call ratio...');
    const pcRatio = await fetchCboePutCallRatio();
    const saved = await saveScrapedData('put_call_ratio', pcRatio, 'cboe');
    console.log(`  ✓ put_call_ratio: ${saved} records saved`);
    success.push('put_call_ratio');
  } catch (err: any) {
    console.error(`  ✗ put_call_ratio: ${err.message}`);
    failed.push('put_call_ratio');
  }

  // Barchart Breadth
  try {
    console.log('  Fetching Barchart breadth data...');
    const breadth = await fetchBarchartBreadth();

    if (breadth.above200dma.length > 0) {
      await saveScrapedData('sp500_above_200dma', breadth.above200dma, 'barchart');
      success.push('sp500_above_200dma');
    }
    if (breadth.above50dma.length > 0) {
      await saveScrapedData('sp500_above_50dma', breadth.above50dma, 'barchart');
      success.push('sp500_above_50dma');
    }
    if (breadth.highsLows.length > 0) {
      await saveScrapedData('nyse_highs_lows', breadth.highsLows, 'barchart');
      success.push('nyse_highs_lows');
    }

    console.log(`  ✓ breadth data saved`);
  } catch (err: any) {
    console.error(`  ✗ breadth: ${err.message}`);
    failed.push('breadth');
  }

  return { success, failed };
}
