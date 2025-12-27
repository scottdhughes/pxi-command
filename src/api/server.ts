import express, { Request, Response } from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { query, healthCheck } from '../db/connection.js';
import { INDICATORS } from '../config/indicators.js';
import { CATEGORY_WEIGHTS } from '../types/indicators.js';

dotenv.config();

const app = express();
const PORT = parseInt(process.env.PORT || '3000');

app.use(cors());
app.use(express.json());

// ============== Health Check ==============
app.get('/health', async (_req: Request, res: Response) => {
  const dbHealthy = await healthCheck();
  res.json({
    status: dbHealthy ? 'healthy' : 'degraded',
    database: dbHealthy ? 'connected' : 'disconnected',
    timestamp: new Date().toISOString(),
  });
});

// ============== Current PXI ==============
app.get('/api/pxi', async (_req: Request, res: Response) => {
  try {
    const pxiResult = await query(`
      SELECT * FROM pxi_scores ORDER BY date DESC LIMIT 1
    `);

    if (pxiResult.rows.length === 0) {
      res.status(404).json({ error: 'No PXI data available' });
      return;
    }

    const pxi = pxiResult.rows[0];
    const dateStr = pxi.date;

    // Get category scores
    const catResult = await query(
      `SELECT category, score, weight, weighted_score
       FROM category_scores WHERE date = $1`,
      [dateStr]
    );

    // Get sparkline data (last 30 days)
    const sparklineResult = await query(`
      SELECT date, score FROM pxi_scores
      ORDER BY date DESC LIMIT 30
    `);

    res.json({
      date: pxi.date,
      score: parseFloat(pxi.score),
      label: pxi.label,
      status: pxi.status,
      delta: {
        d1: pxi.delta_1d ? parseFloat(pxi.delta_1d) : null,
        d7: pxi.delta_7d ? parseFloat(pxi.delta_7d) : null,
        d30: pxi.delta_30d ? parseFloat(pxi.delta_30d) : null,
      },
      categories: catResult.rows.map((c) => ({
        name: c.category,
        score: parseFloat(c.score),
        weight: parseFloat(c.weight),
      })),
      sparkline: sparklineResult.rows.reverse().map((r) => ({
        date: r.date,
        score: parseFloat(r.score),
      })),
    });
  } catch (err: any) {
    console.error('API error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============== Historical PXI ==============
app.get('/api/pxi/history', async (req: Request, res: Response) => {
  try {
    const days = parseInt(req.query.days as string) || 30;
    const limit = Math.min(days, 365);

    const result = await query(
      `SELECT date, score, label, status, delta_1d, delta_7d
       FROM pxi_scores ORDER BY date DESC LIMIT $1`,
      [limit]
    );

    res.json({
      count: result.rows.length,
      data: result.rows.map((r) => ({
        date: r.date,
        score: parseFloat(r.score),
        label: r.label,
        status: r.status,
        delta1d: r.delta_1d ? parseFloat(r.delta_1d) : null,
        delta7d: r.delta_7d ? parseFloat(r.delta_7d) : null,
      })),
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ============== Category Detail ==============
app.get('/api/pxi/category/:name', async (req: Request, res: Response) => {
  try {
    const { name } = req.params;
    const days = parseInt(req.query.days as string) || 30;

    // Validate category
    if (!CATEGORY_WEIGHTS[name as keyof typeof CATEGORY_WEIGHTS]) {
      res.status(400).json({ error: 'Invalid category' });
      return;
    }

    // Get category history
    const catResult = await query(
      `SELECT date, score FROM category_scores
       WHERE category = $1 ORDER BY date DESC LIMIT $2`,
      [name, days]
    );

    // Get indicators for this category
    const categoryIndicators = INDICATORS.filter((i) => i.category === name);

    // Get latest indicator values
    const indicatorData = await Promise.all(
      categoryIndicators.map(async (ind) => {
        const valResult = await query(
          `SELECT date, value FROM indicator_values
           WHERE indicator_id = $1 ORDER BY date DESC LIMIT 1`,
          [ind.id]
        );
        const scoreResult = await query(
          `SELECT normalized_value FROM indicator_scores
           WHERE indicator_id = $1 ORDER BY date DESC LIMIT 1`,
          [ind.id]
        );

        return {
          id: ind.id,
          name: ind.name,
          value: valResult.rows[0]?.value ? parseFloat(valResult.rows[0].value) : null,
          score: scoreResult.rows[0]?.normalized_value
            ? parseFloat(scoreResult.rows[0].normalized_value)
            : null,
          date: valResult.rows[0]?.date || null,
        };
      })
    );

    res.json({
      category: name,
      weight: CATEGORY_WEIGHTS[name as keyof typeof CATEGORY_WEIGHTS],
      history: catResult.rows.reverse().map((r) => ({
        date: r.date,
        score: parseFloat(r.score),
      })),
      indicators: indicatorData,
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ============== All Indicators ==============
app.get('/api/indicators', async (_req: Request, res: Response) => {
  try {
    const result = await query(`
      SELECT
        iv.indicator_id,
        iv.date,
        iv.value,
        iv.source,
        isc.normalized_value
      FROM indicator_values iv
      LEFT JOIN indicator_scores isc
        ON iv.indicator_id = isc.indicator_id AND iv.date = isc.date
      WHERE iv.date = (SELECT MAX(date) FROM indicator_values)
      ORDER BY iv.indicator_id
    `);

    const indicators = INDICATORS.map((ind) => {
      const data = result.rows.find((r) => r.indicator_id === ind.id);
      return {
        id: ind.id,
        name: ind.name,
        category: ind.category,
        source: ind.source,
        frequency: ind.frequency,
        value: data?.value ? parseFloat(data.value) : null,
        score: data?.normalized_value ? parseFloat(data.normalized_value) : null,
        date: data?.date || null,
      };
    });

    res.json({ indicators });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ============== Alerts ==============
app.get('/api/alerts', async (req: Request, res: Response) => {
  try {
    const unacknowledgedOnly = req.query.unacknowledged === 'true';

    const result = await query(
      unacknowledgedOnly
        ? `SELECT * FROM alerts WHERE acknowledged = FALSE ORDER BY created_at DESC LIMIT 50`
        : `SELECT * FROM alerts ORDER BY created_at DESC LIMIT 50`
    );

    res.json({ alerts: result.rows });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ============== Status Summary ==============
app.get('/api/status', async (_req: Request, res: Response) => {
  try {
    const [pxiCount, valueCount, lastFetch] = await Promise.all([
      query(`SELECT COUNT(*) FROM pxi_scores`),
      query(`SELECT COUNT(*) FROM indicator_values`),
      query(`SELECT MAX(fetched_at) as latest FROM indicator_values`),
    ]);

    res.json({
      pxiDays: parseInt(pxiCount.rows[0].count),
      indicatorValues: parseInt(valueCount.rows[0].count),
      lastFetch: lastFetch.rows[0].latest,
      indicatorCount: INDICATORS.length,
      categories: Object.keys(CATEGORY_WEIGHTS).length,
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ============== Start Server ==============
export function startServer() {
  app.listen(PORT, () => {
    console.log(`\n🚀 PXI API server running on http://localhost:${PORT}\n`);
    console.log('Endpoints:');
    console.log('  GET /health           - Health check');
    console.log('  GET /api/pxi          - Current PXI score');
    console.log('  GET /api/pxi/history  - Historical scores');
    console.log('  GET /api/pxi/category/:name - Category detail');
    console.log('  GET /api/indicators   - All indicators');
    console.log('  GET /api/alerts       - Active alerts');
    console.log('  GET /api/status       - System status\n');
  });
}

// Run if called directly
const isMainModule = process.argv[1]?.includes('server');
if (isMainModule) {
  startServer();
}

export default app;
