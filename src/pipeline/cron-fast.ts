// Fast cron pipeline that uses Worker API instead of wrangler CLI.
// Fetches all indicator data, validates SLA, and only then writes to Worker API.

import dotenv from 'dotenv';
dotenv.config();

import { writeFile } from 'node:fs/promises';
import { basename } from 'node:path';
import { pathToFileURL } from 'node:url';

import axios from 'axios';
import { format, subYears } from 'date-fns';
import yahooFinance from 'yahoo-finance2';

import { INDICATORS } from '../config/indicators.js';
import {
  CRITICAL_INDICATORS,
  MONITORED_SLA_INDICATORS,
  evaluateSla,
  isChronicStaleness,
  resolveStalePolicy,
  resolveIndicatorSla,
  type IndicatorSlaEvaluation,
} from '../config/indicator-sla.js';
import { fetchGEX } from '../fetchers/gex.js';

const WRITE_API_URL = process.env.WRITE_API_URL ?? '';
const WRITE_API_KEY = process.env.WRITE_API_KEY ?? '';
const FRED_API_KEY = process.env.FRED_API_KEY ?? '';
const SLA_SUMMARY_PATH = process.env.SLA_SUMMARY_PATH ?? '/tmp/pxi-sla-summary.json';
const STALE_TOP_OFFENDERS_PATH = process.env.STALE_TOP_OFFENDERS_PATH ?? '/tmp/pxi-stale-top-offenders.json';
const MARKET_SUMMARY_PATH = process.env.MARKET_SUMMARY_PATH ?? '/tmp/pxi-market-summary.json';

interface IndicatorValue {
  indicator_id: string;
  date: string;
  value: number;
  source: string;
}

interface RetryBackoffOptions {
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  shouldRetry?: (error: unknown) => boolean;
}

interface SlaCheck extends IndicatorSlaEvaluation {
  indicator_id: string;
  frequency: string | null;
  status: 'ok' | 'stale' | 'missing';
}

interface StaleTopOffender {
  indicator_id: string;
  status: 'stale' | 'missing';
  days_old: number | null;
  max_age_days: number;
  critical: boolean;
  chronic: boolean;
  policy: {
    retry_attempts: number;
    retry_backoff_minutes: number;
    escalation: 'observe' | 'retry_source' | 'escalate_ops';
    owner: 'market_data' | 'macro_data' | 'risk_ops';
  };
}

interface SlaSummary {
  generated_at_utc: string;
  summary: {
    checked: number;
    critical_failures: number;
    non_critical_failures: number;
    stale_or_missing_total: number;
    chronic_total: number;
  };
  checks: SlaCheck[];
  top_offenders: StaleTopOffender[];
}

interface MarketRefreshSummary {
  generated_at_utc: string;
  result: {
    ok?: boolean;
    brief_generated?: number;
    opportunities_generated?: number;
    calibrations_generated?: number;
    alerts_generated?: number;
    as_of?: string | null;
    [key: string]: unknown;
  };
}

interface YahooChartPayload {
  chart?: {
    result?: Array<{
      timestamp?: number[];
      indicators?: {
        quote?: Array<{ close?: Array<number | null> }>;
        adjclose?: Array<{ adjclose?: Array<number | null> }>;
      };
    }>;
    error?: { description?: string } | null;
  };
}

const indicatorFrequencyMap = new Map<string, string>(
  INDICATORS.map((indicator) => [indicator.id, indicator.frequency])
);
const monitoredIndicators = new Set<string>([
  ...INDICATORS.map((indicator) => indicator.id),
  ...MONITORED_SLA_INDICATORS,
]);

// Collected indicator values for this run.
const allIndicators: IndicatorValue[] = [];

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clamp(min: number, max: number, value: number): number {
  return Math.max(min, Math.min(max, value));
}

export function isRetryableYahooError(error: unknown): boolean {
  if (!error) return false;

  if (axios.isAxiosError(error)) {
    const status = error.response?.status;
    if (status === 429) return true;
    if (status !== undefined && status >= 500) return true;
    if (!status) return true;
  }

  const message = error instanceof Error ? error.message : String(error);
  const normalized = message.toLowerCase();
  return (
    normalized.includes('too many requests') ||
    normalized.includes('429') ||
    normalized.includes('timeout') ||
    normalized.includes('econnreset') ||
    normalized.includes('enotfound') ||
    normalized.includes('network')
  );
}

export async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  options: RetryBackoffOptions = {}
): Promise<T> {
  const maxAttempts = options.maxAttempts ?? 4;
  const baseDelayMs = options.baseDelayMs ?? 300;
  const maxDelayMs = options.maxDelayMs ?? 5000;
  const shouldRetry = options.shouldRetry ?? (() => true);

  let attempt = 0;
  let lastError: unknown;

  while (attempt < maxAttempts) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      attempt += 1;

      if (attempt >= maxAttempts || !shouldRetry(error)) {
        throw error;
      }

      const exponentialDelay = Math.min(baseDelayMs * Math.pow(2, attempt - 1), maxDelayMs);
      const jitter = Math.floor(Math.random() * Math.max(1, Math.floor(exponentialDelay * 0.2)));
      await sleep(exponentialDelay + jitter);
    }
  }

  throw lastError instanceof Error ? lastError : new Error('Retry exhausted');
}

export function parseYahooChartResponse(
  payload: unknown,
  indicatorId: string,
  source: string = 'yahoo_direct'
): IndicatorValue[] {
  const parsed = payload as YahooChartPayload;
  const result = parsed?.chart?.result?.[0];
  if (!result || !Array.isArray(result.timestamp)) {
    return [];
  }

  const timestamps = result.timestamp;
  const adjustedSeries = result.indicators?.adjclose?.[0]?.adjclose;
  const closeSeries = result.indicators?.quote?.[0]?.close;

  const values: IndicatorValue[] = [];
  for (let i = 0; i < timestamps.length; i += 1) {
    const ts = timestamps[i];
    if (!Number.isFinite(ts)) continue;

    const adj = Array.isArray(adjustedSeries) ? adjustedSeries[i] : null;
    const close = Array.isArray(closeSeries) ? closeSeries[i] : null;
    const value = Number.isFinite(adj as number) ? (adj as number) : (close as number);

    if (!Number.isFinite(value)) continue;

    values.push({
      indicator_id: indicatorId,
      date: format(new Date(ts * 1000), 'yyyy-MM-dd'),
      value,
      source,
    });
  }

  return values;
}

function recordIndicatorValues(values: IndicatorValue[]): number {
  let written = 0;
  for (const value of values) {
    if (!Number.isFinite(value.value)) continue;
    allIndicators.push(value);
    written += 1;
  }
  return written;
}

async function fetchSentimentProxy(): Promise<number | null> {
  const { fetchSentimentFromFearGreed } = await import('../fetchers/alternative-sources.js');
  return fetchSentimentFromFearGreed();
}

async function fetchPutCallProxy(): Promise<number | null> {
  const { fetchPutCallFromYahoo } = await import('../fetchers/alternative-sources.js');
  return fetchPutCallFromYahoo();
}

async function fetchAaiiPrimary(): Promise<Array<{ date: Date; value: number }>> {
  const { fetchAAIISentiment } = await import('../fetchers/scrapers.js');
  return fetchAAIISentiment();
}

async function fetchCboePutCallPrimary(): Promise<Array<{ date: Date; value: number }>> {
  const { fetchCboePutCallRatio } = await import('../fetchers/scrapers.js');
  return fetchCboePutCallRatio();
}

function getWriteConfig(): { writeApiUrl: string; writeApiKey: string; baseUrl: string } {
  if (!WRITE_API_URL) {
    throw new Error('WRITE_API_URL environment variable is required');
  }
  if (!WRITE_API_KEY) {
    throw new Error('WRITE_API_KEY environment variable is required');
  }

  return {
    writeApiUrl: WRITE_API_URL,
    writeApiKey: WRITE_API_KEY,
    baseUrl: WRITE_API_URL.replace('/api/write', ''),
  };
}

async function fetchFredSeries(seriesId: string, indicatorId: string): Promise<IndicatorValue[]> {
  if (!FRED_API_KEY) {
    console.warn('FRED_API_KEY not set, skipping FRED data');
    return [];
  }

  const response = await axios.get('https://api.stlouisfed.org/fred/series/observations', {
    params: {
      series_id: seriesId,
      api_key: FRED_API_KEY,
      file_type: 'json',
      observation_start: format(subYears(new Date(), 3), 'yyyy-MM-dd'),
      observation_end: format(new Date(), 'yyyy-MM-dd'),
      sort_order: 'desc',
      limit: 120,
    },
    timeout: 20000,
  });

  const values: IndicatorValue[] = [];
  for (const obs of response.data.observations || []) {
    if (obs.value === '.') continue;
    const numericValue = Number.parseFloat(obs.value);
    if (!Number.isFinite(numericValue)) continue;

    values.push({
      indicator_id: indicatorId,
      date: obs.date,
      value: numericValue,
      source: 'fred',
    });
  }
  return values;
}

async function fetchFredSeriesWithFallback(
  seriesIds: string[],
  indicatorId: string
): Promise<{ values: IndicatorValue[]; seriesId: string }> {
  let lastError: unknown = null;
  for (const seriesId of seriesIds) {
    try {
      const values = await fetchFredSeries(seriesId, indicatorId);
      if (values.length > 0) {
        return { values, seriesId };
      }
      lastError = new Error(`FRED series ${seriesId} returned no rows`);
    } catch (error) {
      lastError = error;
    }
  }

  const message = lastError instanceof Error ? lastError.message : String(lastError);
  throw new Error(message || `Failed to fetch ${indicatorId} from FRED fallback series`);
}

async function fetchAllFred(): Promise<void> {
  console.log('\n━━━ FRED Data ━━━');

  const fredIndicators = [
    { ticker: 'WALCL', id: 'fed_balance_sheet' },
    { ticker: 'RRPONTSYD', id: 'reverse_repo' },
    { ticker: 'WTREGEN', id: 'treasury_general_account' },
    { ticker: 'BAMLH0A0HYM2', id: 'hy_oas_spread' },
    { ticker: 'BAMLC0A4CBBBEY', id: 'ig_oas_spread' },
    { ticker: 'T10Y2Y', id: 'yield_curve_2s10s' },
    { ticker: 'DCOILWTICO', id: 'wti_crude' },
    { ticker: 'DTWEXBGS', id: 'dollar_index' },
    { ticker: 'IC4WSA', id: 'jobless_claims' },
    { ticker: 'BAMLEMCBPIOAS', id: 'em_spread' },
    { ticker: 'CFNAI', id: 'cfnai' },
    { ticker: 'MANEMP', id: 'ism_manufacturing' },
  ];

  for (const { ticker, id } of fredIndicators) {
    try {
      const values = await fetchFredSeries(ticker, id);
      const written = recordIndicatorValues(values);
      console.log(`  ✓ ${id}: ${written} values`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`  ✗ ${id}: ${message}`);
    }
    await sleep(200);
  }

  // ISM services can drift between FRED aliases; try known fallbacks.
  try {
    const { values, seriesId } = await fetchFredSeriesWithFallback(
      ['NAPMNOI', 'NMFCI'],
      'ism_services'
    );
    const written = recordIndicatorValues(values);
    console.log(`  ✓ ism_services: ${written} values (${seriesId})`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.log(`  ⚠ ism_services primary failed: ${message}`);
    const latestIsmManufacturing = [...allIndicators]
      .filter((value) => value.indicator_id === 'ism_manufacturing')
      .sort((a, b) => b.date.localeCompare(a.date))[0];

    if (latestIsmManufacturing && Number.isFinite(latestIsmManufacturing.value)) {
      const written = recordIndicatorValues([{
        indicator_id: 'ism_services',
        date: latestIsmManufacturing.date,
        value: latestIsmManufacturing.value,
        source: 'ism_manufacturing_proxy',
      }]);
      console.log(`  ✓ ism_services: ${written} value (ism_manufacturing_proxy)`);
    } else {
      console.error('  ✗ ism_services: unavailable from both primary and proxy');
    }
  }

  // Derive BBB-AAA spread from BAA/AAA effective yields.
  try {
    const [bbbSeries, aaaSeries] = await Promise.all([
      fetchFredSeries('BAMLC0A4CBBBEY', 'bbb_temp'),
      fetchFredSeries('BAMLC0A1CAAAEY', 'aaa_temp'),
    ]);

    const aaaByDate = new Map(aaaSeries.map((row) => [row.date, row.value]));
    const derived: IndicatorValue[] = [];
    for (const bbb of bbbSeries) {
      const aaa = aaaByDate.get(bbb.date);
      if (aaa === undefined) continue;
      derived.push({
        indicator_id: 'bbb_aaa_spread',
        date: bbb.date,
        value: bbb.value - aaa,
        source: 'fred',
      });
    }

    const written = recordIndicatorValues(derived);
    console.log(`  ✓ bbb_aaa_spread: ${written} values`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`  ✗ bbb_aaa_spread: ${message}`);
  }

  // Derive M2 YoY from M2SL series.
  try {
    const m2Series = await fetchFredSeries('M2SL', 'm2_raw');
    const sorted = [...m2Series].sort((a, b) => a.date.localeCompare(b.date));
    const m2Yoy: IndicatorValue[] = [];

    for (let i = 12; i < sorted.length; i += 1) {
      const current = sorted[i];
      const prior = sorted[i - 12];
      if (!prior || prior.value === 0) continue;

      const yoy = ((current.value - prior.value) / prior.value) * 100;
      if (!Number.isFinite(yoy)) continue;

      m2Yoy.push({
        indicator_id: 'm2_yoy',
        date: current.date,
        value: yoy,
        source: 'fred',
      });
    }

    const written = recordIndicatorValues(m2Yoy);
    console.log(`  ✓ m2_yoy: ${written} values`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`  ✗ m2_yoy: ${message}`);
  }

  // Derive net liquidity from core components.
  const walcl = allIndicators.filter((i) => i.indicator_id === 'fed_balance_sheet');
  const tga = allIndicators.filter((i) => i.indicator_id === 'treasury_general_account');
  const rrp = allIndicators.filter((i) => i.indicator_id === 'reverse_repo');

  const tgaByDate = new Map(tga.map((item) => [item.date, item.value]));
  const rrpByDate = new Map(rrp.map((item) => [item.date, item.value]));

  const netLiquidity: IndicatorValue[] = [];
  for (const row of walcl) {
    const tgaValue = tgaByDate.get(row.date);
    const rrpValue = rrpByDate.get(row.date);
    if (tgaValue === undefined || rrpValue === undefined) continue;

    netLiquidity.push({
      indicator_id: 'net_liquidity',
      date: row.date,
      value: row.value - tgaValue - rrpValue,
      source: 'fred',
    });
  }

  const written = recordIndicatorValues(netLiquidity);
  console.log(`  ✓ net_liquidity: ${written} values`);
}

async function fetchYahooSeriesViaLibrary(symbol: string, indicatorId: string): Promise<IndicatorValue[]> {
  const chart = await yahooFinance.chart(symbol, {
    period1: subYears(new Date(), 3),
    period2: new Date(),
    interval: '1d',
  });

  const values: IndicatorValue[] = [];
  for (const quote of chart.quotes || []) {
    if (!quote.date) continue;
    const value = quote.adjclose ?? quote.close;
    if (!Number.isFinite(value as number)) continue;

    values.push({
      indicator_id: indicatorId,
      date: format(quote.date, 'yyyy-MM-dd'),
      value: value as number,
      source: 'yahoo',
    });
  }

  return values;
}

async function fetchYahooSeriesViaDirectApi(symbol: string, indicatorId: string): Promise<IndicatorValue[]> {
  const response = await axios.get(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}`, {
    params: {
      range: '3y',
      interval: '1d',
    },
    timeout: 20000,
    headers: {
      'User-Agent': 'Mozilla/5.0',
      'Accept': 'application/json',
    },
    validateStatus: () => true,
  });

  if (response.status === 429) {
    throw new Error('Yahoo direct API rate limited (429)');
  }
  if (response.status >= 400) {
    throw new Error(`Yahoo direct API error ${response.status}`);
  }

  const values = parseYahooChartResponse(response.data, indicatorId, 'yahoo_direct');
  if (values.length === 0) {
    const errorText = (response.data as YahooChartPayload)?.chart?.error?.description;
    throw new Error(errorText || `No chart data for ${symbol}`);
  }

  return values;
}

async function fetchYahooSeriesWithFallback(symbol: string, indicatorId: string): Promise<IndicatorValue[]> {
  try {
    const values = await retryWithBackoff(
      () => fetchYahooSeriesViaLibrary(symbol, indicatorId),
      {
        maxAttempts: 3,
        baseDelayMs: 250,
        maxDelayMs: 2000,
        shouldRetry: isRetryableYahooError,
      }
    );

    if (values.length > 0) {
      return values;
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(`  ⚠ ${indicatorId}: yahoo-finance2 failed (${message}), trying direct API`);
  }

  return retryWithBackoff(
    () => fetchYahooSeriesViaDirectApi(symbol, indicatorId),
    {
      maxAttempts: 4,
      baseDelayMs: 300,
      maxDelayMs: 3000,
      shouldRetry: isRetryableYahooError,
    }
  );
}

function toDateSourceMap(values: IndicatorValue[]): Map<string, { value: number; source: string }> {
  const map = new Map<string, { value: number; source: string }>();
  for (const value of values) {
    const current = map.get(value.date);
    if (!current || (current.source !== 'yahoo_direct' && value.source === 'yahoo_direct')) {
      map.set(value.date, { value: value.value, source: value.source });
    }
  }
  return map;
}

async function fetchAllYahoo(): Promise<void> {
  console.log('\n━━━ Yahoo Finance Data ━━━');

  const yahooIndicators = [
    { ticker: '^VIX', id: 'vix' },
    { ticker: 'HYG', id: 'hyg' },
    { ticker: 'LQD', id: 'lqd' },
    { ticker: 'TLT', id: 'tlt' },
    { ticker: 'GLD', id: 'gold' },
    { ticker: 'BTC-USD', id: 'btc_price' },
    { ticker: 'SPY', id: 'spy_close' },
    { ticker: 'DX-Y.NYB', id: 'dxy' },
    { ticker: 'AUDJPY=X', id: 'audjpy' },
  ];

  for (const { ticker, id } of yahooIndicators) {
    try {
      const values = await fetchYahooSeriesWithFallback(ticker, id);
      const written = recordIndicatorValues(values);
      const source = values.some((v) => v.source === 'yahoo_direct') ? 'yahoo_direct' : 'yahoo';
      console.log(`  ✓ ${id}: ${written} values (${source})`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`  ✗ ${id}: ${message}`);
    }
    await sleep(250);
  }

  // Small cap strength (IWM vs SPY).
  try {
    const [iwm, spy] = await Promise.all([
      fetchYahooSeriesWithFallback('IWM', 'iwm_temp'),
      Promise.resolve(allIndicators.filter((v) => v.indicator_id === 'spy_close')),
    ]);

    const spyMap = toDateSourceMap(spy);
    const derived: IndicatorValue[] = [];
    for (const row of iwm) {
      const spyValue = spyMap.get(row.date);
      if (!spyValue || spyValue.value === 0) continue;
      derived.push({
        indicator_id: 'small_cap_strength',
        date: row.date,
        value: (row.value / spyValue.value) * 100,
        source: row.source === 'yahoo_direct' || spyValue.source === 'yahoo_direct' ? 'yahoo_direct' : 'yahoo',
      });
    }
    const written = recordIndicatorValues(derived);
    console.log(`  ✓ small_cap_strength: ${written} values`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`  ✗ small_cap_strength: ${message}`);
  }

  // Mid cap strength (IJH vs SPY).
  try {
    const [ijh, spy] = await Promise.all([
      fetchYahooSeriesWithFallback('IJH', 'ijh_temp'),
      Promise.resolve(allIndicators.filter((v) => v.indicator_id === 'spy_close')),
    ]);

    const spyMap = toDateSourceMap(spy);
    const derived: IndicatorValue[] = [];
    for (const row of ijh) {
      const spyValue = spyMap.get(row.date);
      if (!spyValue || spyValue.value === 0) continue;
      derived.push({
        indicator_id: 'midcap_strength',
        date: row.date,
        value: (row.value / spyValue.value) * 100,
        source: row.source === 'yahoo_direct' || spyValue.source === 'yahoo_direct' ? 'yahoo_direct' : 'yahoo',
      });
    }
    const written = recordIndicatorValues(derived);
    console.log(`  ✓ midcap_strength: ${written} values`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`  ✗ midcap_strength: ${message}`);
  }

  // Sector breadth (% sector ETFs above 50-day moving average).
  try {
    const sectorEtfs = ['XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY'];
    const sectorSeries = await Promise.all(
      sectorEtfs.map((ticker) => fetchYahooSeriesWithFallback(ticker, `${ticker.toLowerCase()}_temp`))
    );

    const allDates = new Set<string>();
    const sectorMaps = sectorSeries.map((series) => {
      const dateMap = toDateSourceMap(series);
      for (const date of dateMap.keys()) allDates.add(date);
      return dateMap;
    });
    const sortedDates = [...allDates].sort();

    const derived: IndicatorValue[] = [];
    for (let i = 50; i < sortedDates.length; i += 1) {
      const date = sortedDates[i];
      let above = 0;
      let total = 0;
      let hasDirectSource = false;

      for (const sectorMap of sectorMaps) {
        const current = sectorMap.get(date);
        if (!current) continue;

        let sum = 0;
        let count = 0;
        let sectorDirect = current.source === 'yahoo_direct';
        for (let j = i - 50; j < i; j += 1) {
          const past = sectorMap.get(sortedDates[j]);
          if (!past) continue;
          sum += past.value;
          count += 1;
          if (past.source === 'yahoo_direct') sectorDirect = true;
        }

        if (count >= 40) {
          const ma50 = sum / count;
          if (current.value > ma50) above += 1;
          total += 1;
          hasDirectSource = hasDirectSource || sectorDirect;
        }
      }

      if (total >= 8) {
        derived.push({
          indicator_id: 'sector_breadth',
          date,
          value: (above / total) * 100,
          source: hasDirectSource ? 'yahoo_direct' : 'yahoo',
        });
      }
    }

    const written = recordIndicatorValues(derived);
    console.log(`  ✓ sector_breadth: ${written} values`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`  ✗ sector_breadth: ${message}`);
  }

  // BTC vs 200-day moving average.
  try {
    const btc = [...allIndicators]
      .filter((value) => value.indicator_id === 'btc_price')
      .sort((a, b) => a.date.localeCompare(b.date));
    const derived: IndicatorValue[] = [];

    for (let i = 200; i < btc.length; i += 1) {
      let rolling = 0;
      for (let j = i - 200; j < i; j += 1) {
        rolling += btc[j].value;
      }
      const ma200 = rolling / 200;
      if (!Number.isFinite(ma200) || ma200 === 0) continue;

      const pctAbove = ((btc[i].value - ma200) / ma200) * 100;
      if (!Number.isFinite(pctAbove)) continue;

      derived.push({
        indicator_id: 'btc_vs_200dma',
        date: btc[i].date,
        value: pctAbove,
        source: btc[i].source,
      });
    }

    const written = recordIndicatorValues(derived);
    console.log(`  ✓ btc_vs_200dma: ${written} values`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`  ✗ btc_vs_200dma: ${message}`);
  }

  // VIX term structure.
  try {
    const [vix, vix3m] = await Promise.all([
      Promise.resolve(allIndicators.filter((v) => v.indicator_id === 'vix')),
      fetchYahooSeriesWithFallback('^VIX3M', 'vix3m_temp'),
    ]);

    const vix3mMap = toDateSourceMap(vix3m);
    const derived: IndicatorValue[] = [];

    for (const row of vix) {
      const secondLeg = vix3mMap.get(row.date);
      if (!secondLeg) continue;
      derived.push({
        indicator_id: 'vix_term_structure',
        date: row.date,
        value: row.value - secondLeg.value,
        source: row.source === 'yahoo_direct' || secondLeg.source === 'yahoo_direct' ? 'yahoo_direct' : 'yahoo',
      });
    }

    const written = recordIndicatorValues(derived);
    console.log(`  ✓ vix_term_structure: ${written} values`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`  ✗ vix_term_structure: ${message}`);
  }

  // RSP/SPY ratio.
  try {
    const [rsp, spy] = await Promise.all([
      fetchYahooSeriesWithFallback('RSP', 'rsp_temp'),
      Promise.resolve(allIndicators.filter((v) => v.indicator_id === 'spy_close')),
    ]);

    const spyMap = toDateSourceMap(spy);
    const derived: IndicatorValue[] = [];

    for (const row of rsp) {
      const spyValue = spyMap.get(row.date);
      if (!spyValue || spyValue.value === 0) continue;
      derived.push({
        indicator_id: 'rsp_spy_ratio',
        date: row.date,
        value: row.value / spyValue.value,
        source: row.source === 'yahoo_direct' || spyValue.source === 'yahoo_direct' ? 'yahoo_direct' : 'yahoo',
      });
    }

    const written = recordIndicatorValues(derived);
    console.log(`  ✓ rsp_spy_ratio: ${written} values`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`  ✗ rsp_spy_ratio: ${message}`);
  }

  // Copper/Gold ratio.
  try {
    const [copper, gold] = await Promise.all([
      fetchYahooSeriesWithFallback('HG=F', 'copper_temp'),
      fetchYahooSeriesWithFallback('GC=F', 'gold_temp'),
    ]);

    const goldMap = toDateSourceMap(gold);
    const derived: IndicatorValue[] = [];

    for (const row of copper) {
      const goldValue = goldMap.get(row.date);
      if (!goldValue || goldValue.value === 0) continue;
      derived.push({
        indicator_id: 'copper_gold_ratio',
        date: row.date,
        value: (row.value / goldValue.value) * 1000,
        source: row.source === 'yahoo_direct' || goldValue.source === 'yahoo_direct' ? 'yahoo_direct' : 'yahoo',
      });
    }

    const written = recordIndicatorValues(derived);
    const source = derived.some((v) => v.source === 'yahoo_direct') ? 'yahoo_direct' : 'yahoo';
    console.log(`  ✓ copper_gold_ratio: ${written} values (${source})`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`  ✗ copper_gold_ratio: ${message}`);
  }
}

async function fetchCrypto(): Promise<void> {
  console.log('\n━━━ Crypto Data ━━━');

  try {
    const response = await axios.get('https://stablecoins.llama.fi/stablecoincharts/all', { timeout: 20000 });
    const data = response.data;

    if (Array.isArray(data)) {
      const values: IndicatorValue[] = [];
      for (let i = 30; i < data.length; i += 1) {
        const current = data[i];
        const past = data[i - 30];
        const currentValue = current.totalCirculating?.peggedUSD;
        const pastValue = past.totalCirculating?.peggedUSD;
        if (!currentValue || !pastValue) continue;

        const roc = ((currentValue - pastValue) / pastValue) * 100;
        if (!Number.isFinite(roc)) continue;

        values.push({
          indicator_id: 'stablecoin_mcap',
          date: format(new Date(current.date * 1000), 'yyyy-MM-dd'),
          value: roc,
          source: 'defillama',
        });
      }

      const written = recordIndicatorValues(values);
      console.log(`  ✓ stablecoin_mcap: ${written} values`);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`  ✗ stablecoin_mcap: ${message}`);
  }

  const writeFundingRate = (ratePct: number, source: string): number => {
    return recordIndicatorValues([{
      indicator_id: 'btc_funding_rate',
      date: format(new Date(), 'yyyy-MM-dd'),
      value: ratePct,
      source,
    }]);
  };

  let wroteFunding = false;
  try {
    const response = await axios.get('https://open-api.coinglass.com/public/v2/funding', {
      params: { symbol: 'BTC' },
      timeout: 20000,
    });

    if (response.data?.data?.uMarginList) {
      const rates = response.data.data.uMarginList;
      const avgRate = rates.reduce((sum: number, row: { rate?: number }) => sum + (row.rate || 0), 0) /
        Math.max(rates.length, 1);
      const written = writeFundingRate(avgRate * 100, 'coinglass');
      wroteFunding = written > 0;
      if (wroteFunding) {
        console.log(`  ✓ btc_funding_rate: ${written} value (coinglass)`);
      }
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.log(`  ⚠ btc_funding_rate coinglass failed: ${message}`);
  }

  if (!wroteFunding) {
    try {
      const response = await axios.get('https://fapi.binance.com/fapi/v1/fundingRate', {
        params: {
          symbol: 'BTCUSDT',
          limit: 40,
        },
        timeout: 20000,
        validateStatus: () => true,
      });

      if (response.status >= 400) {
        throw new Error(`Binance funding API error ${response.status}`);
      }

      const rows = Array.isArray(response.data)
        ? response.data as Array<{ fundingRate?: string }>
        : [];
      const rates = rows
        .map((row) => Number.parseFloat(String(row.fundingRate ?? '')))
        .filter((rate) => Number.isFinite(rate));

      if (rates.length === 0) {
        throw new Error('No Binance funding rows');
      }

      const avgRatePct = (rates.reduce((sum, rate) => sum + rate, 0) / rates.length) * 100;
      const written = writeFundingRate(avgRatePct, 'binance_futures');
      wroteFunding = written > 0;
      if (wroteFunding) {
        console.log(`  ✓ btc_funding_rate: ${written} value (binance_futures)`);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.log(`  ⚠ btc_funding_rate binance failed: ${message}`);
    }
  }

  if (!wroteFunding) {
    const btcSeries = [...allIndicators]
      .filter((value) => value.indicator_id === 'btc_price')
      .sort((a, b) => b.date.localeCompare(a.date));
    const current = btcSeries[0];
    const prior = btcSeries[3];

    if (current && prior && prior.value !== 0) {
      const pctMove3d = ((current.value - prior.value) / prior.value) * 100;
      const proxyRatePct = clamp(-0.2, 0.2, pctMove3d * 0.01);
      const written = writeFundingRate(proxyRatePct, 'btc_momentum_proxy');
      wroteFunding = written > 0;
      if (wroteFunding) {
        console.log(`  ✓ btc_funding_rate: ${written} value (btc_momentum_proxy)`);
      }
    }
  }

  if (!wroteFunding) {
    console.error('  ✗ btc_funding_rate: unavailable from all sources');
  }
}

async function fetchBtcEtfFlows(): Promise<void> {
  console.log('\n━━━ BTC ETF Flows ━━━');

  try {
    const response = await axios.get(
      'https://api.coingecko.com/api/v3/coins/bitcoin/market_chart',
      {
        params: {
          vs_currency: 'usd',
          days: 90,
          interval: 'daily',
        },
        timeout: 20000,
        headers: {
          accept: 'application/json',
        },
      }
    );

    if (response.data?.prices && response.data?.total_volumes) {
      const prices = response.data.prices as Array<[number, number]>;
      const volumes = response.data.total_volumes as Array<[number, number]>;

      const values: IndicatorValue[] = [];
      for (let i = 7; i < prices.length; i += 1) {
        const currentPrice = prices[i][1];
        const weekAgoPrice = prices[i - 7][1];
        const currentVolume = volumes[i]?.[1] || 0;
        const avgVolume = volumes
          .slice(Math.max(0, i - 30), i)
          .reduce((sum: number, row: [number, number]) => sum + (row[1] || 0), 0) / 30;

        const momentum = ((currentPrice - weekAgoPrice) / weekAgoPrice) * 100;
        const volumeRatio = avgVolume > 0 ? currentVolume / avgVolume : 1;
        const flowProxy = momentum * Math.log1p(volumeRatio);

        if (!Number.isFinite(flowProxy)) continue;

        values.push({
          indicator_id: 'btc_etf_flows',
          date: format(new Date(prices[i][0]), 'yyyy-MM-dd'),
          value: flowProxy,
          source: 'coingecko_proxy',
        });
      }

      const written = recordIndicatorValues(values);
      console.log(`  ✓ btc_etf_flows: ${written} values (CoinGecko proxy)`);
      return;
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.log(`  ⚠ CoinGecko API: ${message}`);
  }

  try {
    const btcData = allIndicators
      .filter((i) => i.indicator_id === 'btc_price')
      .sort((a, b) => b.date.localeCompare(a.date));

    if (btcData.length >= 8) {
      const values: IndicatorValue[] = [];
      for (let i = 0; i < Math.min(30, btcData.length - 7); i += 1) {
        const current = btcData[i];
        const weekAgo = btcData[i + 7];
        if (!current || !weekAgo || weekAgo.value === 0) continue;

        const momentum = ((current.value - weekAgo.value) / weekAgo.value) * 100;
        if (!Number.isFinite(momentum)) continue;

        values.push({
          indicator_id: 'btc_etf_flows',
          date: current.date,
          value: momentum,
          source: 'btc_momentum_proxy',
        });
      }

      const written = recordIndicatorValues(values);
      console.log(`  ✓ btc_etf_flows: ${written} values (momentum proxy)`);
      return;
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.log(`  ⚠ BTC momentum fallback: ${message}`);
  }

  console.error('  ✗ btc_etf_flows: all sources failed');
}

async function fetchAaiiSentimentWithFallback(): Promise<void> {
  console.log('\n━━━ AAII Sentiment ━━━');

  try {
    const aaiiValues = await fetchAaiiPrimary();
    if (aaiiValues.length > 0) {
      const normalized: IndicatorValue[] = aaiiValues
        .filter((value) => Number.isFinite(value.value) && value.date instanceof Date)
        .map((value) => ({
          indicator_id: 'aaii_sentiment',
          date: format(value.date, 'yyyy-MM-dd'),
          value: value.value,
          source: 'aaii',
        }));

      const written = recordIndicatorValues(normalized);
      if (written > 0) {
        console.log(`  ✓ aaii_sentiment: ${written} values (AAII)`);
        return;
      }
    }

    console.log('  ⚠ AAII returned no values, using controlled proxy fallback');
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.log(`  ⚠ AAII scrape failed (${message}), using controlled proxy fallback`);
  }

  try {
    const sentimentSpread = await fetchSentimentProxy();
    if (sentimentSpread !== null) {
      const written = recordIndicatorValues([{
        indicator_id: 'aaii_sentiment',
        date: format(new Date(), 'yyyy-MM-dd'),
        value: sentimentSpread,
        source: 'cnn_fg_proxy',
      }]);

      if (written > 0) {
        console.log(`  ✓ aaii_sentiment: ${written} value (proxy)`);
        return;
      }
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`  ✗ aaii_sentiment proxy fallback failed: ${message}`);
    return;
  }

  console.error('  ✗ aaii_sentiment: unavailable from both real and proxy sources');
}

async function fetchAlternative(): Promise<void> {
  console.log('\n━━━ Alternative Data ━━━');

  try {
    const sentimentSpread = await fetchSentimentProxy();
    if (sentimentSpread !== null) {
      const rawScore = sentimentSpread + 50;
      const written = recordIndicatorValues([{
        indicator_id: 'fear_greed',
        date: format(new Date(), 'yyyy-MM-dd'),
        value: rawScore,
        source: 'cnn_or_alt',
      }]);
      console.log(`  ✓ fear_greed: ${written} value`);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`  ✗ fear_greed: ${message}`);
  }

  let latestPutCall: number | null = null;
  try {
    const cboeValues = await fetchCboePutCallPrimary();
    if (cboeValues.length > 0) {
      const latest = [...cboeValues]
        .filter((value) => value.date instanceof Date && Number.isFinite(value.value))
        .sort((a, b) => b.date.getTime() - a.date.getTime())[0];
      if (latest) {
        latestPutCall = latest.value;
        const written = recordIndicatorValues([{
          indicator_id: 'put_call_ratio',
          date: format(latest.date, 'yyyy-MM-dd'),
          value: latest.value,
          source: 'cboe',
        }]);
        if (written > 0) {
          console.log(`  ✓ put_call_ratio: ${written} value (cboe)`);
        }
      }
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.log(`  ⚠ put_call_ratio CBOE scrape failed: ${message}`);
  }

  if (latestPutCall === null) {
    try {
      const putCall = await fetchPutCallProxy();
      if (putCall !== null && Number.isFinite(putCall)) {
        latestPutCall = putCall;
        const written = recordIndicatorValues([{
          indicator_id: 'put_call_ratio',
          date: format(new Date(), 'yyyy-MM-dd'),
          value: putCall,
          source: 'yahoo_proxy',
        }]);
        console.log(`  ✓ put_call_ratio: ${written} value (proxy)`);
      } else {
        console.log('  ⚠ put_call_ratio proxy returned no value');
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.log(`  ⚠ put_call_ratio proxy failed: ${message}`);
    }
  }

  if (latestPutCall === null) {
    const latestVix = [...allIndicators]
      .filter((value) => value.indicator_id === 'vix')
      .sort((a, b) => b.date.localeCompare(a.date))[0];
    if (latestVix && Number.isFinite(latestVix.value)) {
      const proxyRatio = clamp(0.45, 1.35, 0.45 + (latestVix.value / 45));
      const written = recordIndicatorValues([{
        indicator_id: 'put_call_ratio',
        date: latestVix.date,
        value: proxyRatio,
        source: 'vix_proxy',
      }]);
      if (written > 0) {
        latestPutCall = proxyRatio;
        console.log(`  ✓ put_call_ratio: ${written} value (vix_proxy)`);
      }
    }
  }

  if (latestPutCall === null) {
    console.error('  ✗ put_call_ratio: unavailable from all sources');
  }

  let gexWritten = false;
  try {
    const gex = await fetchGEX();
    if (gex !== null && Number.isFinite(gex)) {
      const written = recordIndicatorValues([{
        indicator_id: 'gex',
        date: format(new Date(), 'yyyy-MM-dd'),
        value: gex,
        source: 'cboe',
      }]);
      gexWritten = written > 0;
      if (gexWritten) {
        console.log(`  ✓ gex: ${written} value (cboe)`);
      }
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.log(`  ⚠ gex primary failed: ${message}`);
  }

  if (!gexWritten) {
    const latestVix = [...allIndicators]
      .filter((value) => value.indicator_id === 'vix')
      .sort((a, b) => b.date.localeCompare(a.date))[0];
    const latestDate = latestVix?.date || format(new Date(), 'yyyy-MM-dd');
    if (latestVix && Number.isFinite(latestVix.value)) {
      const putCallForProxy = latestPutCall ?? 0.9;
      // Positive in calmer regimes, negative in stressed regimes.
      const proxy = clamp(-25, 25, ((20 - latestVix.value) * 1.2) + ((1 - putCallForProxy) * 18));
      const written = recordIndicatorValues([{
        indicator_id: 'gex',
        date: latestDate,
        value: proxy,
        source: 'proxy_vol_surface',
      }]);
      if (written > 0) {
        gexWritten = true;
        console.log(`  ✓ gex: ${written} value (proxy_vol_surface)`);
      }
    }
  }

  if (!gexWritten) {
    console.error('  ✗ gex: unavailable from both primary and proxy fallback');
  }
}

function buildLatestDateByIndicator(values: IndicatorValue[]): Map<string, string> {
  const map = new Map<string, string>();
  for (const value of values) {
    const current = map.get(value.indicator_id);
    if (!current || value.date > current) {
      map.set(value.indicator_id, value.date);
    }
  }
  return map;
}

function formatDays(daysOld: number | null): string {
  if (daysOld === null) return '—';
  return daysOld.toFixed(1);
}

function buildSlaChecks(now: Date): SlaCheck[] {
  const latestDates = buildLatestDateByIndicator(allIndicators);
  const checks: SlaCheck[] = [];

  for (const indicatorId of [...monitoredIndicators].sort()) {
    const frequency = indicatorFrequencyMap.get(indicatorId) ?? null;
    const policy = resolveIndicatorSla(indicatorId, frequency);
    const evaluation = evaluateSla(latestDates.get(indicatorId) ?? null, now, policy);

    checks.push({
      indicator_id: indicatorId,
      frequency,
      status: evaluation.missing ? 'missing' : evaluation.stale ? 'stale' : 'ok',
      ...evaluation,
    });
  }

  return checks;
}

function logSlaChecks(checks: SlaCheck[]): void {
  console.log('\n━━━ SLA Gate ━━━');
  console.log('  indicator                 latest      days   max   class         critical  status');

  for (const check of checks) {
    const indicator = check.indicator_id.padEnd(24, ' ');
    const latest = (check.latest_date ?? 'missing').padEnd(10, ' ');
    const days = formatDays(check.days_old).padStart(5, ' ');
    const max = String(check.max_age_days).padStart(3, ' ');
    const slaClass = check.sla_class.padEnd(12, ' ');
    const critical = String(check.critical).padEnd(8, ' ');

    console.log(`  ${indicator} ${latest} ${days} ${max}   ${slaClass} ${critical} ${check.status}`);
  }
}

function buildStaleTopOffenders(checks: SlaCheck[], limit = 10): StaleTopOffender[] {
  return checks
    .filter((check) => check.status === 'stale' || check.status === 'missing')
    .map((check) => {
      const status: 'stale' | 'missing' = check.status === 'missing' ? 'missing' : 'stale';
      const policy = resolveStalePolicy(check.indicator_id, check.frequency);
      const chronic = isChronicStaleness(check.days_old, check.max_age_days);
      return {
        indicator_id: check.indicator_id,
        status,
        days_old: check.days_old,
        max_age_days: check.max_age_days,
        critical: check.critical,
        chronic,
        policy,
      };
    })
    .sort((a, b) => {
      if (a.critical !== b.critical) return a.critical ? -1 : 1;
      if (a.chronic !== b.chronic) return a.chronic ? -1 : 1;
      if (a.status !== b.status) return a.status === 'missing' ? -1 : 1;
      const aDays = a.days_old ?? Number.POSITIVE_INFINITY;
      const bDays = b.days_old ?? Number.POSITIVE_INFINITY;
      return bDays - aDays;
    })
    .slice(0, limit);
}

async function writeSlaSummary(checks: SlaCheck[]): Promise<void> {
  const criticalFailures = checks.filter(
    (check) => check.critical && (check.status === 'stale' || check.status === 'missing')
  ).length;
  const nonCriticalFailures = checks.filter(
    (check) => !check.critical && (check.status === 'stale' || check.status === 'missing')
  ).length;
  const topOffenders = buildStaleTopOffenders(checks);
  const chronicTotal = topOffenders.filter((item) => item.chronic).length;

  const summary: SlaSummary = {
    generated_at_utc: new Date().toISOString(),
    summary: {
      checked: checks.length,
      critical_failures: criticalFailures,
      non_critical_failures: nonCriticalFailures,
      stale_or_missing_total: topOffenders.length,
      chronic_total: chronicTotal,
    },
    checks,
    top_offenders: topOffenders,
  };

  await writeFile(SLA_SUMMARY_PATH, `${JSON.stringify(summary, null, 2)}\n`, 'utf8');
  await writeFile(STALE_TOP_OFFENDERS_PATH, `${JSON.stringify({
    generated_at_utc: summary.generated_at_utc,
    top_offenders: topOffenders,
  }, null, 2)}\n`, 'utf8');
  console.log(`\n📝 SLA summary written to ${SLA_SUMMARY_PATH}`);
  console.log(`📝 Stale top offenders written to ${STALE_TOP_OFFENDERS_PATH}`);
}

async function enforceSlaGate(now: Date): Promise<void> {
  const checks = buildSlaChecks(now);
  logSlaChecks(checks);
  await writeSlaSummary(checks);
  const topOffenders = buildStaleTopOffenders(checks, 5);

  if (topOffenders.length > 0) {
    console.log('\n━━━ Stale Top Offenders ━━━');
    for (const offender of topOffenders) {
      const days = offender.days_old === null ? 'n/a' : offender.days_old.toFixed(1);
      console.log(
        `  ${offender.indicator_id}: ${offender.status}, ${days}d old, ` +
        `policy=${offender.policy.escalation} owner=${offender.policy.owner}`
      );
    }
  }

  const criticalViolations = checks.filter(
    (check) => check.critical && (check.status === 'stale' || check.status === 'missing')
  );

  if (criticalViolations.length > 0) {
    const names = criticalViolations.map((check) => check.indicator_id).join(', ');
    throw new Error(`Critical SLA violation(s): ${names}`);
  }
}

async function postToWorkerAPI(): Promise<void> {
  const { writeApiKey, writeApiUrl } = getWriteConfig();

  console.log('\n━━━ Posting to Worker API ━━━');
  console.log(`  Total indicator values: ${allIndicators.length}`);

  const requestedBatchSize = Number.parseInt(process.env.CRON_WRITE_BATCH_SIZE ?? '1000', 10);
  const batchSize = Number.isFinite(requestedBatchSize)
    ? Math.min(5000, Math.max(100, requestedBatchSize))
    : 1000;
  let totalWritten = 0;
  let failedBatches = 0;

  for (let i = 0; i < allIndicators.length; i += batchSize) {
    const batch = allIndicators.slice(i, i + batchSize);

    try {
      const response = await fetch(writeApiUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${writeApiKey}`,
        },
        body: JSON.stringify({ indicators: batch }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`API error ${response.status}: ${errorText}`);
      }

      const result = (await response.json()) as { written?: number };
      const written = Number(result.written ?? 0);
      totalWritten += written;
      console.log(`  Batch ${Math.floor(i / batchSize) + 1}: wrote ${written} records`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      failedBatches += 1;
      console.error(`  Batch ${Math.floor(i / batchSize) + 1} failed: ${message}`);
    }
  }

  console.log(`\n✅ Total written: ${totalWritten} records`);
  if (failedBatches > 0) {
    throw new Error(`${failedBatches} batch(es) failed while writing indicator data`);
  }
}

async function triggerRecalculation(): Promise<void> {
  const { writeApiKey, baseUrl } = getWriteConfig();

  const today = new Date().toISOString().split('T')[0];
  console.log('\n━━━ Recalculating PXI ━━━');
  console.log(`  Date: ${today}`);

  const response = await fetch(`${baseUrl}/api/recalculate`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${writeApiKey}`,
    },
    body: JSON.stringify({ date: today }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Recalculate error ${response.status}: ${errorText}`);
  }

  const result = (await response.json()) as {
    score: number;
    label: string;
    categories: number;
    embedded?: boolean;
  };

  console.log(`  ✓ PXI: ${result.score.toFixed(1)} (${result.label})`);
  console.log(`  ✓ Categories: ${result.categories}`);
  console.log(`  ✓ Embedding: ${result.embedded ? 'generated' : 'skipped'}`);
}

async function evaluatePredictions(): Promise<void> {
  const { writeApiKey, baseUrl } = getWriteConfig();

  console.log('\n━━━ Evaluating Past Predictions ━━━');

  const response = await fetch(`${baseUrl}/api/evaluate`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${writeApiKey}`,
    },
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Evaluate error ${response.status}: ${errorText}`);
  }

  const result = (await response.json()) as { pending: number; evaluated: number };
  console.log(`  ✓ Pending predictions: ${result.pending}`);
  console.log(`  ✓ Evaluated: ${result.evaluated}`);
}

async function triggerMarketProductsRefresh(): Promise<void> {
  const { writeApiKey, baseUrl } = getWriteConfig();
  console.log('\n━━━ Refreshing Market Products ━━━');

  const response = await fetch(`${baseUrl}/api/market/refresh-products`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${writeApiKey}`,
      'X-Refresh-Trigger': 'cron_fast_pipeline',
    },
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Market refresh error ${response.status}: ${errorText}`);
  }

  const result = (await response.json()) as MarketRefreshSummary['result'];
  const summary: MarketRefreshSummary = {
    generated_at_utc: new Date().toISOString(),
    result,
  };

  await writeFile(MARKET_SUMMARY_PATH, `${JSON.stringify(summary, null, 2)}\n`, 'utf8');
  console.log(`  ✓ Brief generated: ${result.brief_generated ?? 0}`);
  console.log(`  ✓ Opportunities generated: ${result.opportunities_generated ?? 0}`);
  console.log(`  ✓ Calibrations generated: ${result.calibrations_generated ?? 0}`);
  console.log(`  ✓ Alerts generated: ${result.alerts_generated ?? 0}`);
  console.log(`  ✓ Summary written: ${MARKET_SUMMARY_PATH}`);
}

export async function main(): Promise<void> {
  const startTime = Date.now();
  allIndicators.length = 0;

  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║                 PXI DAILY REFRESH (FAST)                   ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  console.log(`\n📅 ${new Date().toISOString()}`);

  try {
    await fetchAllFred();
    await fetchAllYahoo();
    await fetchCrypto();
    await fetchBtcEtfFlows();
    await fetchAaiiSentimentWithFallback();
    await fetchAlternative();

    await enforceSlaGate(new Date());

    await postToWorkerAPI();
    await triggerRecalculation();
    await evaluatePredictions();
    await triggerMarketProductsRefresh();

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n✅ Daily refresh complete in ${duration}s`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`\n❌ Daily refresh failed: ${message}`);
    process.exitCode = 1;
    throw error;
  }
}

function isDirectExecution(): boolean {
  const entry = process.argv[1];
  if (!entry) return false;

  const entryBaseName = basename(entry);
  if (entryBaseName !== 'cron-fast.ts' && entryBaseName !== 'cron-fast.js') {
    return false;
  }

  return import.meta.url === pathToFileURL(entry).href;
}

if (isDirectExecution()) {
  main().catch(() => {
    process.exit(1);
  });
}
