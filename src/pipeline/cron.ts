import dotenv from 'dotenv';
dotenv.config();

import { pool } from '../db/connection.js';
import { fetchAllFredIndicators, fetchM2YoY, fetchNetLiquidity } from '../fetchers/fred.js';
import { fetchAllYahooIndicators } from '../fetchers/yahoo.js';
import { fetchAllCryptoIndicators } from '../fetchers/crypto.js';
import { fetchAlternativeIndicators } from '../fetchers/alternative-sources.js';
import { calculatePXI } from '../normalizers/engine.js';
import { processAlerts } from '../alerts/engine.js';
import { query } from '../db/connection.js';
import { format } from 'date-fns';

// ============== Daily Refresh Job ==============

async function dailyRefresh(): Promise<void> {
  const startTime = Date.now();
  const today = new Date();

  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║                    PXI DAILY REFRESH                       ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  console.log(`\n📅 ${today.toISOString()}\n`);

  try {
    // 1. Fetch FRED data
    console.log('━━━ FRED Data ━━━');
    await fetchAllFredIndicators();
    await fetchNetLiquidity().then((data) =>
      Promise.all(
        data.map((v) =>
          query(
            `INSERT INTO indicator_values (indicator_id, date, value, source)
             VALUES ('net_liquidity', $1, $2, 'fred')
             ON CONFLICT (indicator_id, date) DO UPDATE SET value = EXCLUDED.value`,
            [format(v.date, 'yyyy-MM-dd'), v.value]
          )
        )
      )
    );

    // 2. Fetch Yahoo data
    console.log('\n━━━ Yahoo Data ━━━');
    await fetchAllYahooIndicators();

    // 3. Fetch Crypto data
    console.log('\n━━━ Crypto Data ━━━');
    await fetchAllCryptoIndicators();

    // 4. Fetch Alternative sources
    console.log('\n━━━ Alternative Data ━━━');
    await fetchAlternativeIndicators();

    // 5. Calculate PXI
    console.log('\n━━━ Calculate PXI ━━━');
    const result = await calculatePXI(today);
    console.log(`\n✅ PXI: ${result.score.toFixed(1)} (${result.label})`);

    // 6. Check for alerts
    await processAlerts(today);

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n✅ Daily refresh complete in ${duration}s\n`);

    // Log success
    await query(
      `INSERT INTO fetch_logs (source, status, records_fetched, started_at)
       VALUES ('daily_refresh', 'success', 1, NOW())`
    );
  } catch (err: any) {
    console.error('\n❌ Daily refresh failed:', err.message);

    await query(
      `INSERT INTO fetch_logs (source, status, error_message, started_at)
       VALUES ('daily_refresh', 'error', $1, NOW())`,
      [err.message]
    );
  } finally {
    await pool.end();
  }
}

// ============== Lightweight Intraday Update ==============

async function intradayUpdate(): Promise<void> {
  console.log('⚡ Intraday update...');

  try {
    // Only fetch real-time indicators
    await fetchAlternativeIndicators();

    // Recalculate today's PXI
    await calculatePXI(new Date());

    console.log('✅ Intraday update complete');
  } catch (err: any) {
    console.error('❌ Intraday update failed:', err.message);
  } finally {
    await pool.end();
  }
}

// ============== CLI ==============

const args = process.argv.slice(2);

if (args.includes('--intraday')) {
  intradayUpdate();
} else {
  dailyRefresh();
}
