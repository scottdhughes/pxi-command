// Fast cron pipeline that uses Worker API instead of wrangler CLI
// Fetches all indicator data → posts to Worker API in batch

import dotenv from 'dotenv';
dotenv.config();

import { format, subYears } from 'date-fns';
import axios from 'axios';
import yahooFinance from 'yahoo-finance2';

const WRITE_API_URL = process.env.WRITE_API_URL || 'https://pxi-api.novoamorx1.workers.dev/api/write';
const WRITE_API_KEY = process.env.WRITE_API_KEY;
const FRED_API_KEY = process.env.FRED_API_KEY;

interface IndicatorValue {
  indicator_id: string;
  date: string;
  value: number;
  source: string;
}

// Collected indicator values
const allIndicators: IndicatorValue[] = [];

// ============== FRED Fetchers ==============

async function fetchFredSeries(seriesId: string, indicatorId: string): Promise<void> {
  if (!FRED_API_KEY) {
    console.warn('FRED_API_KEY not set, skipping FRED data');
    return;
  }

  try {
    const response = await axios.get('https://api.stlouisfed.org/fred/series/observations', {
      params: {
        series_id: seriesId,
        api_key: FRED_API_KEY,
        file_type: 'json',
        observation_start: format(subYears(new Date(), 3), 'yyyy-MM-dd'),
        observation_end: format(new Date(), 'yyyy-MM-dd'),
        sort_order: 'desc',
        limit: 100, // Only recent data for daily refresh
      },
    });

    for (const obs of response.data.observations || []) {
      if (obs.value !== '.') {
        allIndicators.push({
          indicator_id: indicatorId,
          date: obs.date,
          value: parseFloat(obs.value),
          source: 'fred',
        });
      }
    }
    console.log(`  ✓ ${indicatorId}: ${response.data.observations?.length || 0} values`);
  } catch (err: any) {
    console.error(`  ✗ ${indicatorId}: ${err.message}`);
  }
}

async function fetchAllFred(): Promise<void> {
  console.log('\n━━━ FRED Data ━━━');

  const fredIndicators = [
    { ticker: 'WALCL', id: 'fed_balance_sheet' },
    { ticker: 'RRPONTSYD', id: 'reverse_repo' },
    { ticker: 'WTREGEN', id: 'treasury_general_account' },
    { ticker: 'BAMLH0A0HYM2', id: 'high_yield_spread' },
    { ticker: 'BAMLC0A4CBBB', id: 'investment_grade_spread' },
    { ticker: 'T10Y2Y', id: 'yield_curve' },
    { ticker: 'DGS10', id: 'ten_year_yield' },
    { ticker: 'DCOILWTICO', id: 'wti_crude' },
    { ticker: 'DTWEXBGS', id: 'dollar_index' },
  ];

  for (const { ticker, id } of fredIndicators) {
    await fetchFredSeries(ticker, id);
    await new Promise(r => setTimeout(r, 200)); // Rate limit
  }

  // Calculate net liquidity
  const walcl = allIndicators.filter(i => i.indicator_id === 'fed_balance_sheet');
  const tga = allIndicators.filter(i => i.indicator_id === 'treasury_general_account');
  const rrp = allIndicators.filter(i => i.indicator_id === 'reverse_repo');

  const tgaMap = new Map(tga.map(t => [t.date, t.value]));
  const rrpMap = new Map(rrp.map(r => [r.date, r.value]));

  for (const w of walcl) {
    const t = tgaMap.get(w.date);
    const r = rrpMap.get(w.date);
    if (t !== undefined && r !== undefined) {
      allIndicators.push({
        indicator_id: 'net_liquidity',
        date: w.date,
        value: w.value - t - r,
        source: 'fred',
      });
    }
  }
  console.log(`  ✓ net_liquidity: calculated from components`);
}

// ============== Yahoo Finance Fetchers ==============

async function fetchYahooSeries(symbol: string, indicatorId: string): Promise<void> {
  try {
    const result = await yahooFinance.chart(symbol, {
      period1: subYears(new Date(), 3),
      period2: new Date(),
      interval: '1d',
    });

    for (const q of result.quotes || []) {
      if (q.close !== null && q.date) {
        allIndicators.push({
          indicator_id: indicatorId,
          date: format(q.date, 'yyyy-MM-dd'),
          value: q.adjClose ?? q.close,
          source: 'yahoo',
        });
      }
    }
    console.log(`  ✓ ${indicatorId}: ${result.quotes?.length || 0} values`);
  } catch (err: any) {
    console.error(`  ✗ ${indicatorId}: ${err.message}`);
  }
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
  ];

  for (const { ticker, id } of yahooIndicators) {
    await fetchYahooSeries(ticker, id);
    await new Promise(r => setTimeout(r, 300)); // Rate limit
  }

  // VIX term structure
  try {
    const [vix, vix3m] = await Promise.all([
      yahooFinance.chart('^VIX', { period1: subYears(new Date(), 3), period2: new Date(), interval: '1d' }),
      yahooFinance.chart('^VIX3M', { period1: subYears(new Date(), 3), period2: new Date(), interval: '1d' }),
    ]);

    const vix3mMap = new Map((vix3m.quotes || []).map(q => [format(q.date!, 'yyyy-MM-dd'), q.close!]));

    for (const q of vix.quotes || []) {
      if (q.close && q.date) {
        const dateStr = format(q.date, 'yyyy-MM-dd');
        const v3m = vix3mMap.get(dateStr);
        if (v3m) {
          allIndicators.push({
            indicator_id: 'vix_term_structure',
            date: dateStr,
            value: q.close - v3m,
            source: 'yahoo',
          });
        }
      }
    }
    console.log(`  ✓ vix_term_structure: calculated`);
  } catch (err: any) {
    console.error(`  ✗ vix_term_structure: ${err.message}`);
  }

  // RSP/SPY ratio
  try {
    const [rsp, spy] = await Promise.all([
      yahooFinance.chart('RSP', { period1: subYears(new Date(), 3), period2: new Date(), interval: '1d' }),
      yahooFinance.chart('SPY', { period1: subYears(new Date(), 3), period2: new Date(), interval: '1d' }),
    ]);

    const spyMap = new Map((spy.quotes || []).map(q => [format(q.date!, 'yyyy-MM-dd'), q.close!]));

    for (const q of rsp.quotes || []) {
      if (q.close && q.date) {
        const dateStr = format(q.date, 'yyyy-MM-dd');
        const s = spyMap.get(dateStr);
        if (s) {
          allIndicators.push({
            indicator_id: 'rsp_spy_ratio',
            date: dateStr,
            value: q.close / s,
            source: 'yahoo',
          });
        }
      }
    }
    console.log(`  ✓ rsp_spy_ratio: calculated`);
  } catch (err: any) {
    console.error(`  ✗ rsp_spy_ratio: ${err.message}`);
  }

  // Copper/Gold ratio
  try {
    const [copper, gold] = await Promise.all([
      yahooFinance.chart('HG=F', { period1: subYears(new Date(), 3), period2: new Date(), interval: '1d' }),
      yahooFinance.chart('GC=F', { period1: subYears(new Date(), 3), period2: new Date(), interval: '1d' }),
    ]);

    const goldMap = new Map((gold.quotes || []).map(q => [format(q.date!, 'yyyy-MM-dd'), q.close!]));

    for (const q of copper.quotes || []) {
      if (q.close && q.date) {
        const dateStr = format(q.date, 'yyyy-MM-dd');
        const g = goldMap.get(dateStr);
        if (g) {
          allIndicators.push({
            indicator_id: 'copper_gold_ratio',
            date: dateStr,
            value: (q.close / g) * 1000,
            source: 'yahoo',
          });
        }
      }
    }
    console.log(`  ✓ copper_gold_ratio: calculated`);
  } catch (err: any) {
    console.error(`  ✗ copper_gold_ratio: ${err.message}`);
  }
}

// ============== Crypto Fetchers ==============

async function fetchCrypto(): Promise<void> {
  console.log('\n━━━ Crypto Data ━━━');

  // Stablecoin market cap from DeFiLlama
  try {
    const response = await axios.get('https://stablecoins.llama.fi/stablecoincharts/all');
    const data = response.data;

    if (Array.isArray(data)) {
      for (let i = 30; i < data.length; i++) {
        const current = data[i];
        const past = data[i - 30];
        const roc = ((current.totalCirculating?.peggedUSD - past.totalCirculating?.peggedUSD) / past.totalCirculating?.peggedUSD) * 100;

        allIndicators.push({
          indicator_id: 'stablecoin_mcap',
          date: format(new Date(current.date * 1000), 'yyyy-MM-dd'),
          value: roc,
          source: 'defillama',
        });
      }
      console.log(`  ✓ stablecoin_mcap: ${data.length - 30} values`);
    }
  } catch (err: any) {
    console.error(`  ✗ stablecoin_mcap: ${err.message}`);
  }

  // BTC funding rate from CoinGlass
  try {
    const response = await axios.get('https://open-api.coinglass.com/public/v2/funding', {
      params: { symbol: 'BTC' },
    });

    if (response.data?.data?.uMarginList) {
      const rates = response.data.data.uMarginList;
      const avgRate = rates.reduce((sum: number, r: any) => sum + (r.rate || 0), 0) / Math.max(rates.length, 1);

      allIndicators.push({
        indicator_id: 'btc_funding_rate',
        date: format(new Date(), 'yyyy-MM-dd'),
        value: avgRate * 100,
        source: 'coinglass',
      });
      console.log(`  ✓ btc_funding_rate: current value`);
    }
  } catch (err: any) {
    console.error(`  ✗ btc_funding_rate: ${err.message}`);
  }
}

// ============== Alternative Indicators ==============

async function fetchAlternative(): Promise<void> {
  console.log('\n━━━ Alternative Data ━━━');

  // CNN Fear & Greed Index
  try {
    const response = await axios.get('https://production.dataviz.cnn.io/index/fearandgreed/graphdata');
    const data = response.data;

    if (data?.fear_and_greed?.score) {
      allIndicators.push({
        indicator_id: 'fear_greed',
        date: format(new Date(), 'yyyy-MM-dd'),
        value: data.fear_and_greed.score,
        source: 'cnn',
      });
      console.log(`  ✓ fear_greed: ${data.fear_and_greed.score}`);
    }
  } catch (err: any) {
    console.error(`  ✗ fear_greed: ${err.message}`);
  }
}

// ============== POST to Worker API ==============

async function postToWorkerAPI(): Promise<void> {
  if (!WRITE_API_KEY) {
    throw new Error('WRITE_API_KEY not set in environment');
  }

  console.log(`\n━━━ Posting to Worker API ━━━`);
  console.log(`  Total indicator values: ${allIndicators.length}`);

  // Chunk into batches to avoid payload limits
  const BATCH_SIZE = 500;
  let totalWritten = 0;

  for (let i = 0; i < allIndicators.length; i += BATCH_SIZE) {
    const batch = allIndicators.slice(i, i + BATCH_SIZE);

    try {
      const response = await fetch(WRITE_API_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${WRITE_API_KEY}`,
        },
        body: JSON.stringify({ indicators: batch }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`API error ${response.status}: ${errorText}`);
      }

      const result = await response.json() as { success: boolean; written: number };
      totalWritten += result.written;
      console.log(`  Batch ${Math.floor(i / BATCH_SIZE) + 1}: wrote ${result.written} records`);
    } catch (err: any) {
      console.error(`  Batch ${Math.floor(i / BATCH_SIZE) + 1} failed: ${err.message}`);
    }
  }

  console.log(`\n✅ Total written: ${totalWritten} records`);
}

// ============== Main ==============

async function main(): Promise<void> {
  const startTime = Date.now();

  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║                 PXI DAILY REFRESH (FAST)                   ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  console.log(`\n📅 ${new Date().toISOString()}`);

  try {
    // Fetch all data sources
    await fetchAllFred();
    await fetchAllYahoo();
    await fetchCrypto();
    await fetchAlternative();

    // Post to Worker API
    await postToWorkerAPI();

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n✅ Daily refresh complete in ${duration}s`);
  } catch (err: any) {
    console.error('\n❌ Daily refresh failed:', err.message);
    process.exit(1);
  }
}

main();
