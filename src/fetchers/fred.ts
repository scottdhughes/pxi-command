import axios from 'axios';
import { format, subYears } from 'date-fns';
import { query } from '../db/connection.js';
import { getIndicatorsBySource } from '../config/indicators.js';
import type { IndicatorValue } from '../types/indicators.js';

const FRED_BASE_URL = 'https://api.stlouisfed.org/fred/series/observations';

interface FredObservation {
  date: string;
  value: string;
}

interface FredResponse {
  observations: FredObservation[];
}

export async function fetchFredSeries(
  seriesId: string,
  startDate?: Date,
  endDate?: Date
): Promise<IndicatorValue[]> {
  const apiKey = process.env.FRED_API_KEY;
  if (!apiKey) {
    throw new Error('FRED_API_KEY not set in environment');
  }

  const start = startDate || subYears(new Date(), 3);
  const end = endDate || new Date();

  const response = await axios.get<FredResponse>(FRED_BASE_URL, {
    params: {
      series_id: seriesId,
      api_key: apiKey,
      file_type: 'json',
      observation_start: format(start, 'yyyy-MM-dd'),
      observation_end: format(end, 'yyyy-MM-dd'),
      sort_order: 'desc',
    },
  });

  return response.data.observations
    .filter((obs) => obs.value !== '.')
    .map((obs) => ({
      indicatorId: seriesId,
      date: new Date(obs.date),
      value: parseFloat(obs.value),
    }));
}

export async function saveFredData(
  indicatorId: string,
  ticker: string,
  values: IndicatorValue[]
): Promise<number> {
  if (values.length === 0) return 0;

  let inserted = 0;

  for (const val of values) {
    try {
      await query(
        `INSERT INTO indicator_values (indicator_id, date, value, source)
         VALUES ($1, $2, $3, 'fred')
         ON CONFLICT (indicator_id, date)
         DO UPDATE SET value = EXCLUDED.value, fetched_at = NOW()`,
        [indicatorId, format(val.date, 'yyyy-MM-dd'), val.value]
      );
      inserted++;
    } catch (err) {
      console.error(`Error saving ${indicatorId} for ${val.date}:`, err);
    }
  }

  return inserted;
}

export async function fetchAllFredIndicators(): Promise<{
  success: string[];
  failed: string[];
}> {
  const indicators = getIndicatorsBySource('fred');
  const success: string[] = [];
  const failed: string[] = [];

  console.log(`\n📊 Fetching ${indicators.length} FRED indicators...\n`);

  for (const indicator of indicators) {
    try {
      console.log(`  Fetching ${indicator.name} (${indicator.ticker})...`);

      const values = await fetchFredSeries(indicator.ticker);
      const saved = await saveFredData(indicator.id, indicator.ticker, values);

      console.log(`  ✓ ${indicator.id}: ${saved} records saved`);
      success.push(indicator.id);

      // Log the fetch
      await query(
        `INSERT INTO fetch_logs (source, indicator_id, status, records_fetched, started_at)
         VALUES ('fred', $1, 'success', $2, NOW())`,
        [indicator.id, saved]
      );

      // Rate limit - FRED allows 120 requests/minute
      await new Promise((r) => setTimeout(r, 600));
    } catch (err: any) {
      console.error(`  ✗ ${indicator.id}: ${err.message}`);
      failed.push(indicator.id);

      await query(
        `INSERT INTO fetch_logs (source, indicator_id, status, error_message, started_at)
         VALUES ('fred', $1, 'error', $2, NOW())`,
        [indicator.id, err.message]
      );
    }
  }

  return { success, failed };
}

// Special handling for computed FRED indicators
export async function fetchNetLiquidity(): Promise<IndicatorValue[]> {
  // Net Liquidity = Fed Balance Sheet - TGA - RRP
  const [walcl, tga, rrp] = await Promise.all([
    fetchFredSeries('WALCL'),
    fetchFredSeries('WTREGEN'),
    fetchFredSeries('RRPONTSYD'),
  ]);

  // Create date map for each series
  const walclMap = new Map(walcl.map((v) => [format(v.date, 'yyyy-MM-dd'), v.value]));
  const tgaMap = new Map(tga.map((v) => [format(v.date, 'yyyy-MM-dd'), v.value]));
  const rrpMap = new Map(rrp.map((v) => [format(v.date, 'yyyy-MM-dd'), v.value]));

  // Calculate net liquidity for dates where all three exist
  const results: IndicatorValue[] = [];

  for (const [dateStr, walclVal] of walclMap) {
    const tgaVal = tgaMap.get(dateStr);
    const rrpVal = rrpMap.get(dateStr);

    if (tgaVal !== undefined && rrpVal !== undefined) {
      results.push({
        indicatorId: 'net_liquidity',
        date: new Date(dateStr),
        value: walclVal - tgaVal - rrpVal,
      });
    }
  }

  return results;
}

// M2 YoY growth rate calculation
export async function fetchM2YoY(): Promise<IndicatorValue[]> {
  const m2 = await fetchFredSeries('M2SL', subYears(new Date(), 4));

  const results: IndicatorValue[] = [];
  const valueMap = new Map(m2.map((v) => [format(v.date, 'yyyy-MM-dd'), v.value]));

  for (const current of m2) {
    const currentDate = current.date;
    const yearAgoDate = subYears(currentDate, 1);
    const yearAgoStr = format(yearAgoDate, 'yyyy-MM-dd');

    // Find closest date to year ago
    const yearAgoValue = valueMap.get(yearAgoStr);

    if (yearAgoValue) {
      const yoyGrowth = ((current.value - yearAgoValue) / yearAgoValue) * 100;
      results.push({
        indicatorId: 'm2_yoy',
        date: currentDate,
        value: yoyGrowth,
      });
    }
  }

  return results;
}
