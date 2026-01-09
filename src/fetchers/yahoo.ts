import yahooFinance from 'yahoo-finance2';
import { format, subYears, subDays } from 'date-fns';
import { query } from '../db/connection.js';
import { getIndicatorsBySource } from '../config/indicators.js';
import type { IndicatorValue } from '../types/indicators.js';

interface YahooQuote {
  date: Date;
  close: number | null;
  adjclose?: number | null;
}

export async function fetchYahooSeries(
  symbol: string,
  startDate?: Date,
  endDate?: Date
): Promise<IndicatorValue[]> {
  const start = startDate || subYears(new Date(), 3);
  const end = endDate || new Date();

  try {
    const result = await yahooFinance.chart(symbol, {
      period1: start,
      period2: end,
      interval: '1d',
    });

    if (!result.quotes || result.quotes.length === 0) {
      console.warn(`No data returned for ${symbol}`);
      return [];
    }

    return result.quotes
      .filter((q) => q.close !== null && q.date !== null)
      .map((q) => ({
        indicatorId: symbol,
        date: q.date!,
        value: q.adjclose ?? q.close!,
      }));
  } catch (err: any) {
    console.error(`Yahoo fetch error for ${symbol}:`, err.message);
    throw err;
  }
}

export async function saveYahooData(
  indicatorId: string,
  values: IndicatorValue[]
): Promise<number> {
  if (values.length === 0) return 0;

  let inserted = 0;

  for (const val of values) {
    try {
      await query(
        `INSERT INTO indicator_values (indicator_id, date, value, source)
         VALUES ($1, $2, $3, 'yahoo')
         ON CONFLICT (indicator_id, date)
         DO UPDATE SET value = EXCLUDED.value, fetched_at = NOW()`,
        [indicatorId, format(val.date, 'yyyy-MM-dd'), val.value]
      );
      inserted++;
    } catch (err) {
      console.error(`Error saving ${indicatorId}:`, err);
    }
  }

  return inserted;
}

// Fetch VIX and VIX3M for term structure
export async function fetchVixTermStructure(): Promise<IndicatorValue[]> {
  const [vix, vix3m] = await Promise.all([
    fetchYahooSeries('^VIX'),
    fetchYahooSeries('^VIX3M'),
  ]);

  const vix3mMap = new Map(
    vix3m.map((v) => [format(v.date, 'yyyy-MM-dd'), v.value])
  );

  return vix
    .filter((v) => vix3mMap.has(format(v.date, 'yyyy-MM-dd')))
    .map((v) => {
      const dateStr = format(v.date, 'yyyy-MM-dd');
      const spread = v.value - (vix3mMap.get(dateStr) || 0);
      return {
        indicatorId: 'vix_term_structure',
        date: v.date,
        value: spread,
      };
    });
}

// Fetch RSP/SPY ratio
export async function fetchRspSpyRatio(): Promise<IndicatorValue[]> {
  const [rsp, spy] = await Promise.all([
    fetchYahooSeries('RSP'),
    fetchYahooSeries('SPY'),
  ]);

  const spyMap = new Map(
    spy.map((v) => [format(v.date, 'yyyy-MM-dd'), v.value])
  );

  return rsp
    .filter((v) => spyMap.has(format(v.date, 'yyyy-MM-dd')))
    .map((v) => {
      const dateStr = format(v.date, 'yyyy-MM-dd');
      const ratio = v.value / (spyMap.get(dateStr) || 1);
      return {
        indicatorId: 'rsp_spy_ratio',
        date: v.date,
        value: ratio,
      };
    });
}

// Fetch Copper/Gold ratio
export async function fetchCopperGoldRatio(): Promise<IndicatorValue[]> {
  const [copper, gold] = await Promise.all([
    fetchYahooSeries('HG=F'),
    fetchYahooSeries('GC=F'),
  ]);

  const goldMap = new Map(
    gold.map((v) => [format(v.date, 'yyyy-MM-dd'), v.value])
  );

  return copper
    .filter((v) => goldMap.has(format(v.date, 'yyyy-MM-dd')))
    .map((v) => {
      const dateStr = format(v.date, 'yyyy-MM-dd');
      // Copper is per pound, gold per oz - ratio scaled for readability
      const ratio = (v.value / (goldMap.get(dateStr) || 1)) * 1000;
      return {
        indicatorId: 'copper_gold_ratio',
        date: v.date,
        value: ratio,
      };
    });
}

// Calculate BTC vs 200 DMA
export async function fetchBtcVs200Dma(): Promise<IndicatorValue[]> {
  const btc = await fetchYahooSeries('BTC-USD', subYears(new Date(), 3));

  // Sort by date ascending for MA calculation
  const sorted = [...btc].sort((a, b) => a.date.getTime() - b.date.getTime());

  const results: IndicatorValue[] = [];

  for (let i = 199; i < sorted.length; i++) {
    // Calculate 200-day SMA
    let sum = 0;
    for (let j = i - 199; j <= i; j++) {
      sum += sorted[j].value;
    }
    const sma200 = sum / 200;
    const current = sorted[i];

    // Percentage above/below 200 DMA, capped at ±50%
    const pctAbove = ((current.value - sma200) / sma200) * 100;
    const capped = Math.max(-50, Math.min(50, pctAbove));

    // Convert to 0-100 scale: -50% = 0, 0% = 50, +50% = 100
    const normalized = (capped + 50);

    results.push({
      indicatorId: 'btc_vs_200dma',
      date: current.date,
      value: normalized,
    });
  }

  return results;
}

export async function fetchAllYahooIndicators(): Promise<{
  success: string[];
  failed: string[];
}> {
  const indicators = getIndicatorsBySource('yahoo');
  const success: string[] = [];
  const failed: string[] = [];

  console.log(`\n📈 Fetching ${indicators.length} Yahoo Finance indicators...\n`);

  // Simple single-ticker indicators
  const simpleTickers = indicators.filter(
    (i) => !i.ticker.includes(',') && i.id !== 'btc_vs_200dma'
  );

  for (const indicator of simpleTickers) {
    try {
      console.log(`  Fetching ${indicator.name} (${indicator.ticker})...`);
      const values = await fetchYahooSeries(indicator.ticker);
      const saved = await saveYahooData(indicator.id, values);
      console.log(`  ✓ ${indicator.id}: ${saved} records saved`);
      success.push(indicator.id);

      await query(
        `INSERT INTO fetch_logs (source, indicator_id, status, records_fetched, started_at)
         VALUES ('yahoo', $1, 'success', $2, NOW())`,
        [indicator.id, saved]
      );

      await new Promise((r) => setTimeout(r, 500));
    } catch (err: any) {
      console.error(`  ✗ ${indicator.id}: ${err.message}`);
      failed.push(indicator.id);
    }
  }

  // Computed indicators
  const computedFetchers = [
    { id: 'vix_term_structure', fn: fetchVixTermStructure },
    { id: 'rsp_spy_ratio', fn: fetchRspSpyRatio },
    { id: 'copper_gold_ratio', fn: fetchCopperGoldRatio },
    { id: 'btc_vs_200dma', fn: fetchBtcVs200Dma },
  ];

  for (const { id, fn } of computedFetchers) {
    try {
      console.log(`  Computing ${id}...`);
      const values = await fn();
      const saved = await saveYahooData(id, values);
      console.log(`  ✓ ${id}: ${saved} records saved`);
      success.push(id);
    } catch (err: any) {
      console.error(`  ✗ ${id}: ${err.message}`);
      failed.push(id);
    }
  }

  return { success, failed };
}
