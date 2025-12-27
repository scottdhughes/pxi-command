import dotenv from 'dotenv';
dotenv.config();

import { pool, query, healthCheck } from './db/connection.js';
import { format } from 'date-fns';
import { INDICATORS } from './config/indicators.js';
import { CATEGORY_WEIGHTS } from './types/indicators.js';

async function showLatestPXI(): Promise<void> {
  // Check DB connection
  const healthy = await healthCheck();
  if (!healthy) {
    console.error('❌ Database connection failed. Check your .env configuration.');
    process.exit(1);
  }

  // Get latest PXI
  const pxiResult = await query(`SELECT * FROM pxi_scores ORDER BY date DESC LIMIT 1`);

  if (pxiResult.rows.length === 0) {
    console.log('\n📊 No PXI data found. Run the pipeline first:');
    console.log('   npm run fetch    # Fetch all indicator data');
    console.log('   npm run calculate # Calculate PXI score\n');
    return;
  }

  const pxi = pxiResult.rows[0];
  const dateStr = format(new Date(pxi.date), 'yyyy-MM-dd');

  // Get category scores
  const catResult = await query(
    `SELECT * FROM category_scores WHERE date = $1 ORDER BY category`,
    [dateStr]
  );

  // Status display
  const statusEmoji =
    pxi.status === 'max_pamp'
      ? '🟢🟢'
      : pxi.status === 'pamping'
      ? '🟢'
      : pxi.status === 'neutral'
      ? '🟡'
      : pxi.status === 'soft'
      ? '🟠'
      : '🔴';

  console.log('\n╔══════════════════════════════════════════════════════════════╗');
  console.log('║                        PXI INDEX                             ║');
  console.log(`║                       ${parseFloat(pxi.score).toFixed(1).padStart(5)} / 100                            ║`);
  console.log(`║                      ${statusEmoji} ${pxi.label.padEnd(10)}                         ║`);
  console.log(`║                      ${dateStr}                            ║`);
  console.log('╠══════════════════════════════════════════════════════════════╣');

  for (const cat of catResult.rows) {
    const score = parseFloat(cat.score);
    const barLength = Math.round(score / 6.67);
    const bar = '█'.repeat(barLength) + '░'.repeat(15 - barLength);
    console.log(
      `║  ${cat.category.padEnd(12)} ${bar}  ${score.toFixed(0).padStart(3)}                    ║`
    );
  }

  console.log('╠══════════════════════════════════════════════════════════════╣');

  const d1 = pxi.delta_1d
    ? `${parseFloat(pxi.delta_1d) >= 0 ? '+' : ''}${parseFloat(pxi.delta_1d).toFixed(1)}`
    : 'N/A';
  const d7 = pxi.delta_7d
    ? `${parseFloat(pxi.delta_7d) >= 0 ? '+' : ''}${parseFloat(pxi.delta_7d).toFixed(1)}`
    : 'N/A';
  const d30 = pxi.delta_30d
    ? `${parseFloat(pxi.delta_30d) >= 0 ? '+' : ''}${parseFloat(pxi.delta_30d).toFixed(1)}`
    : 'N/A';

  console.log(`║  1d: ${d1.padStart(5)}  |  7d: ${d7.padStart(5)}  |  30d: ${d30.padStart(5)}                    ║`);
  console.log('╚══════════════════════════════════════════════════════════════╝\n');
}

async function showStatus(): Promise<void> {
  console.log('\n╔════════════════════════════════════════════════════════════╗');
  console.log('║                      PXI STATUS                            ║');
  console.log('╚════════════════════════════════════════════════════════════╝\n');

  // DB connection
  const healthy = await healthCheck();
  console.log(`Database: ${healthy ? '✅ Connected' : '❌ Not connected'}`);

  if (!healthy) {
    console.log('\nConfigure your database in .env:');
    console.log('  PG_HOST=localhost');
    console.log('  PG_DATABASE=pxi');
    console.log('  PG_USER=postgres');
    console.log('  PG_PASSWORD=yourpassword\n');
    return;
  }

  // Data counts
  const valueCount = await query(`SELECT COUNT(*) FROM indicator_values`);
  const pxiCount = await query(`SELECT COUNT(*) FROM pxi_scores`);
  const latestFetch = await query(
    `SELECT MAX(fetched_at) as latest FROM indicator_values`
  );

  console.log(`Indicator values: ${valueCount.rows[0].count}`);
  console.log(`PXI scores:       ${pxiCount.rows[0].count}`);
  console.log(
    `Latest fetch:     ${
      latestFetch.rows[0].latest
        ? format(new Date(latestFetch.rows[0].latest), 'yyyy-MM-dd HH:mm')
        : 'Never'
    }`
  );

  // Coverage by source
  console.log('\nData coverage by source:');
  const coverage = await query(`
    SELECT source, COUNT(DISTINCT indicator_id) as indicators, COUNT(*) as records
    FROM indicator_values
    GROUP BY source
    ORDER BY source
  `);

  for (const row of coverage.rows) {
    console.log(`  ${row.source.padEnd(12)} ${row.indicators} indicators, ${row.records} records`);
  }

  console.log('\nConfiguration:');
  console.log(`  Total indicators: ${INDICATORS.length}`);
  console.log(`  Categories:       ${Object.keys(CATEGORY_WEIGHTS).length}`);
  console.log(`  FRED API Key:     ${process.env.FRED_API_KEY ? '✅ Set' : '❌ Not set'}`);

  console.log('\nCommands:');
  console.log('  npm run migrate   # Create database tables');
  console.log('  npm run fetch     # Fetch all indicator data');
  console.log('  npm run calculate # Calculate PXI score');
  console.log('  npm run dev       # Show latest PXI\n');
}

// CLI
const args = process.argv.slice(2);

if (args.includes('--status')) {
  showStatus()
    .then(() => pool.end())
    .catch(console.error);
} else {
  showLatestPXI()
    .then(() => pool.end())
    .catch(console.error);
}
