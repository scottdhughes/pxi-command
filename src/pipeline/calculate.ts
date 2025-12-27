import dotenv from 'dotenv';
dotenv.config();

import { pool, query } from '../db/connection.js';
import { calculatePXI } from '../normalizers/engine.js';
import { format, subDays, eachDayOfInterval, isWeekend } from 'date-fns';
import { INDICATORS } from '../config/indicators.js';
import { CATEGORY_WEIGHTS, type Category } from '../types/indicators.js';

interface PXIOutput {
  date: string;
  score: number;
  label: string;
  status: string;
  categories: {
    name: Category;
    score: number;
    weight: number;
  }[];
}

async function displayPXI(date: Date): Promise<void> {
  const dateStr = format(date, 'yyyy-MM-dd');

  // Get PXI score
  const pxiResult = await query(
    `SELECT * FROM pxi_scores WHERE date = $1`,
    [dateStr]
  );

  if (pxiResult.rows.length === 0) {
    console.log(`No PXI data for ${dateStr}`);
    return;
  }

  const pxi = pxiResult.rows[0];

  // Get category scores
  const catResult = await query(
    `SELECT * FROM category_scores WHERE date = $1 ORDER BY category`,
    [dateStr]
  );

  // Display
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
  console.log('╠══════════════════════════════════════════════════════════════╣');

  for (const cat of catResult.rows) {
    const score = parseFloat(cat.score);
    const barLength = Math.round(score / 6.67); // 15 chars max
    const bar = '█'.repeat(barLength) + '░'.repeat(15 - barLength);
    const delta = ''; // Would need historical comparison
    console.log(
      `║  ${cat.category.padEnd(12)} ${bar}  ${score.toFixed(0).padStart(3)}  ${delta.padStart(5)}                 ║`
    );
  }

  console.log('╠══════════════════════════════════════════════════════════════╣');

  const d1 = pxi.delta_1d ? `${pxi.delta_1d >= 0 ? '+' : ''}${parseFloat(pxi.delta_1d).toFixed(1)}` : 'N/A';
  const d7 = pxi.delta_7d ? `${pxi.delta_7d >= 0 ? '+' : ''}${parseFloat(pxi.delta_7d).toFixed(1)}` : 'N/A';
  const d30 = pxi.delta_30d ? `${pxi.delta_30d >= 0 ? '+' : ''}${parseFloat(pxi.delta_30d).toFixed(1)}` : 'N/A';

  console.log(`║  1d: ${d1.padStart(5)}  |  7d: ${d7.padStart(5)}  |  30d: ${d30.padStart(5)}                    ║`);
  console.log('╚══════════════════════════════════════════════════════════════╝');
}

async function runCalculation(targetDate?: Date): Promise<void> {
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║                    PXI CALCULATION                         ║');
  console.log('╚════════════════════════════════════════════════════════════╝');

  const date = targetDate || new Date();
  const dateStr = format(date, 'yyyy-MM-dd');

  console.log(`\n📅 Calculating PXI for: ${dateStr}\n`);

  // Check data coverage
  console.log('📊 Checking data coverage...\n');

  let available = 0;
  let missing = 0;
  const missingIndicators: string[] = [];

  for (const indicator of INDICATORS) {
    const result = await query(
      `SELECT 1 FROM indicator_values
       WHERE indicator_id = $1 AND date <= $2
       ORDER BY date DESC LIMIT 1`,
      [indicator.id, dateStr]
    );

    if (result.rows.length > 0) {
      available++;
    } else {
      missing++;
      missingIndicators.push(indicator.id);
    }
  }

  console.log(`   Available: ${available}/${INDICATORS.length}`);
  console.log(`   Missing:   ${missing}/${INDICATORS.length}`);

  if (missingIndicators.length > 0 && missingIndicators.length <= 5) {
    console.log(`   Missing:   ${missingIndicators.join(', ')}`);
  } else if (missingIndicators.length > 5) {
    console.log(`   Missing:   ${missingIndicators.slice(0, 5).join(', ')}...`);
  }

  if (available < INDICATORS.length * 0.5) {
    console.log('\n⚠️  Warning: Less than 50% data coverage. Results may be unreliable.');
  }

  // Calculate PXI
  console.log('\n🔢 Running normalization and scoring...\n');

  try {
    const result = await calculatePXI(date);
    console.log(`\n✅ PXI calculated: ${result.score.toFixed(1)} (${result.label})`);

    // Display full output
    await displayPXI(date);
  } catch (err: any) {
    console.error('❌ Calculation failed:', err.message);
    throw err;
  }
}

// Backfill historical PXI scores
async function backfillPXI(startDate: Date, endDate: Date): Promise<void> {
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║                    PXI BACKFILL                            ║');
  console.log('╚════════════════════════════════════════════════════════════╝');

  const days = eachDayOfInterval({ start: startDate, end: endDate }).filter(
    (d) => !isWeekend(d)
  );

  console.log(`\n📅 Backfilling ${days.length} trading days...\n`);

  let processed = 0;
  let failed = 0;

  for (const day of days) {
    try {
      await calculatePXI(day);
      processed++;
      if (processed % 20 === 0) {
        console.log(`   Processed ${processed}/${days.length}...`);
      }
    } catch {
      failed++;
    }
  }

  console.log(`\n✅ Backfill complete: ${processed} succeeded, ${failed} failed`);
}

// CLI handling
const args = process.argv.slice(2);

if (args.includes('--backfill')) {
  const daysArg = args.find((a) => a.startsWith('--days='));
  const days = daysArg ? parseInt(daysArg.split('=')[1]) : 30;

  backfillPXI(subDays(new Date(), days), new Date())
    .then(() => pool.end())
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
} else {
  const dateArg = args.find((a) => a.match(/^\d{4}-\d{2}-\d{2}$/));
  const targetDate = dateArg ? new Date(dateArg) : new Date();

  runCalculation(targetDate)
    .then(() => pool.end())
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
}
