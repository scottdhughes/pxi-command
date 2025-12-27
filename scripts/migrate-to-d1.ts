// Migration script: Neon PostgreSQL → Cloudflare D1
// Run with: npx tsx scripts/migrate-to-d1.ts

import { pool } from '../src/db/connection.js';
import { writeFileSync } from 'fs';

interface IndicatorValue {
  indicator_id: string;
  date: string;
  value: number;
  source: string;
}

interface CategoryScore {
  category: string;
  date: string;
  score: number;
  weight: number;
  weighted_score: number;
}

interface PXIScore {
  date: string;
  score: number;
  label: string;
  status: string;
  delta_1d: number | null;
  delta_7d: number | null;
  delta_30d: number | null;
}

async function exportFromNeon() {
  console.log('📤 Exporting data from Neon...\n');

  // Export indicator_values
  const indicatorValues = await pool.query<IndicatorValue>(`
    SELECT indicator_id, date::text, value, source
    FROM indicator_values
    ORDER BY date
  `);
  console.log(`  indicator_values: ${indicatorValues.rows.length} rows`);

  // Export category_scores
  const categoryScores = await pool.query<CategoryScore>(`
    SELECT category, date::text, score, weight, weighted_score
    FROM category_scores
    ORDER BY date
  `);
  console.log(`  category_scores: ${categoryScores.rows.length} rows`);

  // Export pxi_scores
  const pxiScores = await pool.query<PXIScore>(`
    SELECT date::text, score, label, status, delta_1d, delta_7d, delta_30d
    FROM pxi_scores
    ORDER BY date
  `);
  console.log(`  pxi_scores: ${pxiScores.rows.length} rows`);

  // Generate SQL inserts for D1
  let sql = '-- Data migration from Neon to D1\n\n';

  // Indicator values
  if (indicatorValues.rows.length > 0) {
    sql += '-- indicator_values\n';
    for (const row of indicatorValues.rows) {
      const date = row.date.split('T')[0]; // Ensure YYYY-MM-DD format
      sql += `INSERT OR REPLACE INTO indicator_values (indicator_id, date, value, source) VALUES ('${row.indicator_id}', '${date}', ${row.value}, '${row.source}');\n`;
    }
    sql += '\n';
  }

  // Category scores
  if (categoryScores.rows.length > 0) {
    sql += '-- category_scores\n';
    for (const row of categoryScores.rows) {
      const date = row.date.split('T')[0];
      sql += `INSERT OR REPLACE INTO category_scores (category, date, score, weight, weighted_score) VALUES ('${row.category}', '${date}', ${row.score}, ${row.weight}, ${row.weighted_score});\n`;
    }
    sql += '\n';
  }

  // PXI scores
  if (pxiScores.rows.length > 0) {
    sql += '-- pxi_scores\n';
    for (const row of pxiScores.rows) {
      const date = row.date.split('T')[0];
      const d1 = row.delta_1d !== null ? row.delta_1d : 'NULL';
      const d7 = row.delta_7d !== null ? row.delta_7d : 'NULL';
      const d30 = row.delta_30d !== null ? row.delta_30d : 'NULL';
      sql += `INSERT OR REPLACE INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d) VALUES ('${date}', ${row.score}, '${row.label}', '${row.status}', ${d1}, ${d7}, ${d30});\n`;
    }
  }

  // Write to file
  const outputPath = 'scripts/d1-data.sql';
  writeFileSync(outputPath, sql);
  console.log(`\n✅ Exported to ${outputPath}`);
  console.log(`\nRun: npx wrangler d1 execute pxi-db --file=${outputPath} --remote`);

  await pool.end();
}

exportFromNeon().catch(console.error);
