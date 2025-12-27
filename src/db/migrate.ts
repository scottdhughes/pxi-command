import { pool, query } from './connection.js';

async function migrate() {
  console.log('🚀 Running PXI database migrations...\n');

  try {
    // Create tables
    await query(`
      CREATE TABLE IF NOT EXISTS indicator_values (
        id SERIAL PRIMARY KEY,
        indicator_id VARCHAR(50) NOT NULL,
        date DATE NOT NULL,
        value DECIMAL(20, 6) NOT NULL,
        source VARCHAR(20) NOT NULL,
        fetched_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        UNIQUE(indicator_id, date)
      )
    `);
    console.log('✓ Created indicator_values table');

    await query(`CREATE INDEX IF NOT EXISTS idx_indicator_values_date ON indicator_values(date DESC)`);
    await query(`CREATE INDEX IF NOT EXISTS idx_indicator_values_indicator ON indicator_values(indicator_id)`);
    await query(`CREATE INDEX IF NOT EXISTS idx_indicator_values_lookup ON indicator_values(indicator_id, date DESC)`);
    console.log('✓ Created indicator_values indexes');

    await query(`
      CREATE TABLE IF NOT EXISTS indicator_scores (
        id SERIAL PRIMARY KEY,
        indicator_id VARCHAR(50) NOT NULL,
        date DATE NOT NULL,
        raw_value DECIMAL(20, 6) NOT NULL,
        normalized_value DECIMAL(5, 2) NOT NULL,
        percentile_rank DECIMAL(5, 4),
        lookback_days INTEGER DEFAULT 504,
        calculated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        UNIQUE(indicator_id, date)
      )
    `);
    console.log('✓ Created indicator_scores table');

    await query(`CREATE INDEX IF NOT EXISTS idx_indicator_scores_date ON indicator_scores(date DESC)`);
    await query(`CREATE INDEX IF NOT EXISTS idx_indicator_scores_lookup ON indicator_scores(indicator_id, date DESC)`);
    console.log('✓ Created indicator_scores indexes');

    await query(`
      CREATE TABLE IF NOT EXISTS category_scores (
        id SERIAL PRIMARY KEY,
        category VARCHAR(20) NOT NULL,
        date DATE NOT NULL,
        score DECIMAL(5, 2) NOT NULL,
        weight DECIMAL(4, 2) NOT NULL,
        weighted_score DECIMAL(5, 2) NOT NULL,
        calculated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        UNIQUE(category, date)
      )
    `);
    console.log('✓ Created category_scores table');

    await query(`CREATE INDEX IF NOT EXISTS idx_category_scores_date ON category_scores(date DESC)`);
    console.log('✓ Created category_scores indexes');

    await query(`
      CREATE TABLE IF NOT EXISTS pxi_scores (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL UNIQUE,
        score DECIMAL(5, 2) NOT NULL,
        label VARCHAR(20) NOT NULL,
        status VARCHAR(20) NOT NULL,
        delta_1d DECIMAL(5, 2),
        delta_7d DECIMAL(5, 2),
        delta_30d DECIMAL(5, 2),
        calculated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      )
    `);
    console.log('✓ Created pxi_scores table');

    await query(`CREATE INDEX IF NOT EXISTS idx_pxi_scores_date ON pxi_scores(date DESC)`);
    console.log('✓ Created pxi_scores indexes');

    await query(`
      CREATE TABLE IF NOT EXISTS fetch_logs (
        id SERIAL PRIMARY KEY,
        source VARCHAR(20) NOT NULL,
        indicator_id VARCHAR(50),
        status VARCHAR(20) NOT NULL,
        records_fetched INTEGER DEFAULT 0,
        error_message TEXT,
        started_at TIMESTAMP WITH TIME ZONE NOT NULL,
        completed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      )
    `);
    console.log('✓ Created fetch_logs table');

    await query(`CREATE INDEX IF NOT EXISTS idx_fetch_logs_source ON fetch_logs(source, started_at DESC)`);
    console.log('✓ Created fetch_logs indexes');

    await query(`
      CREATE TABLE IF NOT EXISTS alerts (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        alert_type VARCHAR(50) NOT NULL,
        message TEXT NOT NULL,
        severity VARCHAR(20) NOT NULL,
        acknowledged BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      )
    `);
    console.log('✓ Created alerts table');

    await query(`CREATE INDEX IF NOT EXISTS idx_alerts_date ON alerts(date DESC)`);
    console.log('✓ Created alerts indexes');

    // Create views
    await query(`
      CREATE OR REPLACE VIEW latest_indicator_values AS
      SELECT DISTINCT ON (indicator_id)
        indicator_id,
        date,
        value,
        source,
        fetched_at
      FROM indicator_values
      ORDER BY indicator_id, date DESC
    `);
    console.log('✓ Created latest_indicator_values view');

    await query(`
      CREATE OR REPLACE VIEW pxi_latest AS
      SELECT
        p.date,
        p.score,
        p.label,
        p.status,
        p.delta_1d,
        p.delta_7d,
        p.delta_30d,
        json_agg(
          json_build_object(
            'category', c.category,
            'score', c.score,
            'weight', c.weight,
            'weighted_score', c.weighted_score
          )
        ) as categories
      FROM pxi_scores p
      LEFT JOIN category_scores c ON p.date = c.date
      WHERE p.date = (SELECT MAX(date) FROM pxi_scores)
      GROUP BY p.id, p.date, p.score, p.label, p.status, p.delta_1d, p.delta_7d, p.delta_30d
    `);
    console.log('✓ Created pxi_latest view');

    console.log('\n✅ Migration complete!');
  } catch (error) {
    console.error('❌ Migration failed:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

migrate();
