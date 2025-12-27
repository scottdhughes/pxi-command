// Sync calculated data to D1 via Worker API
// Run after calculate step: npx tsx scripts/sync-to-d1.ts

import { pool } from '../src/db/connection.js';
import { format } from 'date-fns';

const API_URL = process.env.D1_API_URL || 'https://pxi-api.novoamorx1.workers.dev';
const WRITE_KEY = process.env.D1_WRITE_KEY;

if (!WRITE_KEY) {
  console.error('Error: D1_WRITE_KEY environment variable required');
  process.exit(1);
}

async function writeToD1(type: string, data: any): Promise<boolean> {
  try {
    const response = await fetch(`${API_URL}/api/write`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${WRITE_KEY}`,
      },
      body: JSON.stringify({ type, data }),
    });

    if (!response.ok) {
      const error = await response.text();
      console.error(`  Failed to write ${type}:`, error);
      return false;
    }
    return true;
  } catch (err: any) {
    console.error(`  Error writing ${type}:`, err.message);
    return false;
  }
}

async function syncToD1() {
  const today = format(new Date(), 'yyyy-MM-dd');
  console.log(`\n📤 Syncing ${today} data to D1...\n`);

  // Sync indicator values for today
  const indicators = await pool.query(`
    SELECT indicator_id, date::text, value, source
    FROM indicator_values
    WHERE date = $1
  `, [today]);

  console.log(`  Syncing ${indicators.rows.length} indicator values...`);
  let indicatorSuccess = 0;
  for (const row of indicators.rows) {
    const success = await writeToD1('indicator', {
      indicator_id: row.indicator_id,
      date: row.date.split('T')[0],
      value: parseFloat(row.value),
      source: row.source,
    });
    if (success) indicatorSuccess++;
  }
  console.log(`  ✓ ${indicatorSuccess}/${indicators.rows.length} indicators synced`);

  // Sync category scores for today
  const categories = await pool.query(`
    SELECT category, date::text, score, weight, weighted_score
    FROM category_scores
    WHERE date = $1
  `, [today]);

  console.log(`  Syncing ${categories.rows.length} category scores...`);
  for (const row of categories.rows) {
    await writeToD1('category', {
      category: row.category,
      date: row.date.split('T')[0],
      score: parseFloat(row.score),
      weight: parseFloat(row.weight),
      weighted_score: parseFloat(row.weighted_score),
    });
  }
  console.log(`  ✓ ${categories.rows.length} categories synced`);

  // Sync PXI score for today
  const pxi = await pool.query(`
    SELECT date::text, score, label, status, delta_1d, delta_7d, delta_30d
    FROM pxi_scores
    WHERE date = $1
  `, [today]);

  if (pxi.rows.length > 0) {
    const row = pxi.rows[0];
    console.log(`  Syncing PXI score...`);
    await writeToD1('pxi', {
      date: row.date.split('T')[0],
      score: parseFloat(row.score),
      label: row.label,
      status: row.status,
      delta_1d: row.delta_1d ? parseFloat(row.delta_1d) : null,
      delta_7d: row.delta_7d ? parseFloat(row.delta_7d) : null,
      delta_30d: row.delta_30d ? parseFloat(row.delta_30d) : null,
    });
    console.log(`  ✓ PXI score synced (${row.score})`);
  }

  console.log('\n✅ D1 sync complete!\n');
  await pool.end();
}

syncToD1().catch(console.error);
