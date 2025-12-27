import axios from 'axios';
import { format, subDays, fromUnixTime } from 'date-fns';
import { query } from '../db/connection.js';
import type { IndicatorValue } from '../types/indicators.js';

// ============== DeFiLlama ==============

interface DefiLlamaStablecoin {
  date: number;
  totalCirculating: { peggedUSD: number };
}

export async function fetchStablecoinMcap(): Promise<IndicatorValue[]> {
  try {
    // Get total stablecoin market cap over time
    const response = await axios.get(
      'https://stablecoins.llama.fi/stablecoincharts/all',
      { params: { stablecoin: 'USDT,USDC,DAI,BUSD,TUSD,USDP,GUSD,FRAX' } }
    );

    // Alternative: get aggregated data
    const aggResponse = await axios.get(
      'https://stablecoins.llama.fi/stablecoincharts/all'
    );

    if (!Array.isArray(aggResponse.data)) {
      console.warn('Unexpected DeFiLlama response format');
      return [];
    }

    return aggResponse.data.map((d: DefiLlamaStablecoin) => ({
      indicatorId: 'stablecoin_mcap',
      date: fromUnixTime(d.date),
      value: d.totalCirculating?.peggedUSD || 0,
    }));
  } catch (err: any) {
    console.error('DeFiLlama fetch error:', err.message);
    throw err;
  }
}

// Calculate 30-day rate of change for stablecoin mcap
export async function calculateStablecoinRoc(): Promise<IndicatorValue[]> {
  const data = await fetchStablecoinMcap();

  // Sort by date ascending
  const sorted = [...data].sort((a, b) => a.date.getTime() - b.date.getTime());

  const results: IndicatorValue[] = [];

  for (let i = 30; i < sorted.length; i++) {
    const current = sorted[i];
    const past = sorted[i - 30];

    const roc = ((current.value - past.value) / past.value) * 100;

    results.push({
      indicatorId: 'stablecoin_mcap',
      date: current.date,
      value: roc, // Store RoC, not absolute value
    });
  }

  return results;
}

// ============== CoinGlass ==============

interface CoinGlassFundingRate {
  symbol: string;
  uMarginList: Array<{
    exchangeName: string;
    rate: number;
    nextFundingTime: number;
  }>;
}

export async function fetchBtcFundingRate(): Promise<IndicatorValue[]> {
  try {
    // CoinGlass public API for funding rates
    const response = await axios.get(
      'https://open-api.coinglass.com/public/v2/funding',
      {
        params: { symbol: 'BTC' },
        headers: {
          accept: 'application/json',
        },
      }
    );

    if (!response.data?.data) {
      // Try alternative endpoint
      const altResponse = await axios.get(
        'https://fapi.coinglass.com/api/fundingRate/v2/home'
      );

      if (altResponse.data?.data) {
        const btcData = altResponse.data.data.find(
          (d: any) => d.symbol === 'BTC'
        );
        if (btcData) {
          // Average funding rate across exchanges
          const rates = btcData.uMarginList || [];
          const avgRate =
            rates.reduce((sum: number, r: any) => sum + (r.rate || 0), 0) /
            Math.max(rates.length, 1);

          return [
            {
              indicatorId: 'btc_funding_rate',
              date: new Date(),
              value: avgRate * 100, // Convert to percentage
            },
          ];
        }
      }
      return [];
    }

    const data = response.data.data as CoinGlassFundingRate;
    const rates = data.uMarginList || [];
    const avgRate =
      rates.reduce((sum, r) => sum + (r.rate || 0), 0) /
      Math.max(rates.length, 1);

    return [
      {
        indicatorId: 'btc_funding_rate',
        date: new Date(),
        value: avgRate * 100,
      },
    ];
  } catch (err: any) {
    console.error('CoinGlass fetch error:', err.message);
    throw err;
  }
}

// Normalize funding rate using bell curve (optimal range 0.01-0.03%)
export function normalizeFundingRate(rate: number): number {
  // rate is already in percentage form (e.g., 0.01 = 0.01%)
  const absRate = Math.abs(rate);

  // Optimal range: 0.005% to 0.03%
  // Score 100 at 0.01%, decline towards extremes
  if (absRate >= 0.005 && absRate <= 0.03) {
    // In healthy range - score 70-100 based on how centered
    const center = 0.015;
    const distance = Math.abs(absRate - center);
    const maxDistance = 0.015;
    return 100 - (distance / maxDistance) * 30;
  } else if (absRate < 0.005) {
    // Too low (no conviction) - score 50-70
    return 50 + (absRate / 0.005) * 20;
  } else {
    // Too high (overheated) - score declines from 70 to 0
    const excess = absRate - 0.03;
    const maxExcess = 0.1; // 0.13% would be 0 score
    return Math.max(0, 70 - (excess / maxExcess) * 70);
  }
}

// ============== Combined Crypto Fetcher ==============

export async function saveCryptoData(
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

export async function fetchAllCryptoIndicators(): Promise<{
  success: string[];
  failed: string[];
}> {
  const success: string[] = [];
  const failed: string[] = [];

  console.log('\n🪙 Fetching crypto indicators...\n');

  // Stablecoin market cap
  try {
    console.log('  Fetching stablecoin market cap...');
    const stableData = await calculateStablecoinRoc();
    const saved = await saveCryptoData('stablecoin_mcap', stableData, 'defillama');
    console.log(`  ✓ stablecoin_mcap: ${saved} records saved`);
    success.push('stablecoin_mcap');
  } catch (err: any) {
    console.error(`  ✗ stablecoin_mcap: ${err.message}`);
    failed.push('stablecoin_mcap');
  }

  // BTC funding rate
  try {
    console.log('  Fetching BTC funding rate...');
    const fundingData = await fetchBtcFundingRate();
    const saved = await saveCryptoData('btc_funding_rate', fundingData, 'coinglass');
    console.log(`  ✓ btc_funding_rate: ${saved} records saved`);
    success.push('btc_funding_rate');
  } catch (err: any) {
    console.error(`  ✗ btc_funding_rate: ${err.message}`);
    failed.push('btc_funding_rate');
  }

  return { success, failed };
}
