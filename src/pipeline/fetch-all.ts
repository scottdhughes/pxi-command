import dotenv from 'dotenv';
dotenv.config();

import { pool } from '../db/connection.js';
import { fetchAllFredIndicators, fetchM2YoY, fetchNetLiquidity } from '../fetchers/fred.js';
import { fetchAllYahooIndicators } from '../fetchers/yahoo.js';
import { fetchAllCryptoIndicators } from '../fetchers/crypto.js';
import { fetchAllScrapedIndicators } from '../fetchers/scrapers.js';
import { fetchAlternativeIndicators } from '../fetchers/alternative-sources.js';
import { fetchGEX } from '../fetchers/gex.js';
import { format } from 'date-fns';
import { query } from '../db/connection.js';

interface FetchResult {
  source: string;
  success: string[];
  failed: string[];
  duration: number;
}

async function runFetchPipeline(): Promise<void> {
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║                    PXI DATA PIPELINE                       ║');
  console.log('║                     Fetch All Sources                      ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  console.log(`\n📅 Run started at: ${new Date().toISOString()}\n`);

  const results: FetchResult[] = [];
  const startTime = Date.now();

  // 1. FRED (most indicators)
  {
    const start = Date.now();
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    const { success, failed } = await fetchAllFredIndicators();

    // Computed FRED indicators
    try {
      console.log('\n  Computing derived FRED indicators...');

      const netLiq = await fetchNetLiquidity();
      if (netLiq.length > 0) {
        for (const val of netLiq) {
          await query(
            `INSERT INTO indicator_values (indicator_id, date, value, source)
             VALUES ('net_liquidity', $1, $2, 'fred')
             ON CONFLICT (indicator_id, date) DO UPDATE SET value = EXCLUDED.value`,
            [format(val.date, 'yyyy-MM-dd'), val.value]
          );
        }
        console.log(`  ✓ net_liquidity: ${netLiq.length} records`);
        success.push('net_liquidity');
      }

      const m2yoy = await fetchM2YoY();
      if (m2yoy.length > 0) {
        for (const val of m2yoy) {
          await query(
            `INSERT INTO indicator_values (indicator_id, date, value, source)
             VALUES ('m2_yoy', $1, $2, 'fred')
             ON CONFLICT (indicator_id, date) DO UPDATE SET value = EXCLUDED.value`,
            [format(val.date, 'yyyy-MM-dd'), val.value]
          );
        }
        console.log(`  ✓ m2_yoy: ${m2yoy.length} records`);
        success.push('m2_yoy');
      }
    } catch (err: any) {
      console.error(`  ✗ computed indicators: ${err.message}`);
    }

    results.push({
      source: 'FRED',
      success,
      failed,
      duration: Date.now() - start,
    });
  }

  // 2. Yahoo Finance
  {
    const start = Date.now();
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    const { success, failed } = await fetchAllYahooIndicators();
    results.push({
      source: 'Yahoo Finance',
      success,
      failed,
      duration: Date.now() - start,
    });
  }

  // 3. Crypto sources
  {
    const start = Date.now();
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    const { success, failed } = await fetchAllCryptoIndicators();
    results.push({
      source: 'Crypto',
      success,
      failed,
      duration: Date.now() - start,
    });
  }

  // 4. Scraped sources
  {
    const start = Date.now();
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    const { success, failed } = await fetchAllScrapedIndicators();
    results.push({
      source: 'Scraped',
      success,
      failed,
      duration: Date.now() - start,
    });
  }

  // 5. Alternative sources (fill gaps)
  {
    const start = Date.now();
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    const { success, failed } = await fetchAlternativeIndicators();
    results.push({
      source: 'Alternative',
      success,
      failed,
      duration: Date.now() - start,
    });
  }

  // 6. GEX (Gamma Exposure) from CBOE
  {
    const start = Date.now();
    const success: string[] = [];
    const failed: string[] = [];
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('📊 CBOE GEX...\n');
    try {
      const gex = await fetchGEX();
      if (gex !== null) {
        const today = format(new Date(), 'yyyy-MM-dd');
        await query(
          `INSERT INTO indicator_values (indicator_id, date, value, source)
           VALUES ('gex', $1, $2, 'cboe')
           ON CONFLICT (indicator_id, date) DO UPDATE SET value = EXCLUDED.value`,
          [today, gex]
        );
        success.push('gex');
        console.log(`  ✓ gex: ${gex.toFixed(2)}B`);
      } else {
        failed.push('gex');
        console.error('  ✗ gex: No data returned');
      }
    } catch (err: any) {
      console.error(`  ✗ gex: ${err.message}`);
      failed.push('gex');
    }
    results.push({
      source: 'CBOE GEX',
      success,
      failed,
      duration: Date.now() - start,
    });
  }

  // Summary
  console.log('\n\n╔════════════════════════════════════════════════════════════╗');
  console.log('║                       FETCH SUMMARY                        ║');
  console.log('╚════════════════════════════════════════════════════════════╝\n');

  let totalSuccess = 0;
  let totalFailed = 0;

  for (const result of results) {
    const status =
      result.failed.length === 0 ? '✅' : result.failed.length < result.success.length ? '⚠️' : '❌';
    console.log(
      `${status} ${result.source.padEnd(15)} | Success: ${result.success.length
        .toString()
        .padStart(2)} | Failed: ${result.failed.length.toString().padStart(2)} | ${(
        result.duration / 1000
      ).toFixed(1)}s`
    );
    totalSuccess += result.success.length;
    totalFailed += result.failed.length;
  }

  const totalDuration = (Date.now() - startTime) / 1000;
  console.log('─'.repeat(60));
  console.log(
    `📊 Total: ${totalSuccess} succeeded, ${totalFailed} failed in ${totalDuration.toFixed(1)}s`
  );

  if (totalFailed > 0) {
    console.log('\n⚠️  Failed indicators:');
    for (const result of results) {
      for (const failed of result.failed) {
        console.log(`   - ${failed} (${result.source})`);
      }
    }
  }

  console.log('\n✅ Fetch pipeline complete!\n');

  await pool.end();
}

runFetchPipeline().catch((err) => {
  console.error('Pipeline failed:', err);
  process.exit(1);
});
