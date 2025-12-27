// D1 Database client for Node.js
// Collects writes in memory and batch-POSTs to Worker API
// Compatible with the old pg-style query interface

const WRITE_API_URL = process.env.WRITE_API_URL || 'https://pxi-api.novoamorx1.workers.dev/api/write';
const WRITE_API_KEY = process.env.WRITE_API_KEY;

interface QueryResult<T = any> {
  rows: T[];
  rowCount: number;
}

// In-memory storage for batch writes
const pendingIndicators: { indicator_id: string; date: string; value: number; source: string }[] = [];
const pendingCategories: { category: string; date: string; score: number; weight: number; weighted_score: number }[] = [];
let pendingPxi: { date: string; score: number; label: string; status: string; delta_1d: number | null; delta_7d: number | null; delta_30d: number | null } | null = null;

// Track if we're in batch mode
let batchMode = true;

// Parse INSERT statement and extract data
function parseInsert(sql: string, params?: any[]): { table: string; data: Record<string, any> } | null {
  const insertMatch = sql.match(/INSERT\s+(?:OR\s+REPLACE\s+)?INTO\s+(\w+)\s*\(([^)]+)\)/i);
  if (!insertMatch) return null;

  const table = insertMatch[1];
  const columns = insertMatch[2].split(',').map(c => c.trim());

  // Extract values from the SQL
  const valuesMatch = sql.match(/VALUES\s*\(([^)]+)\)/i);
  if (!valuesMatch) return null;

  const data: Record<string, any> = {};

  // Map parameters to columns (handle $1, $2 style or ? style)
  let paramIndex = 0;
  columns.forEach((col, i) => {
    if (params && paramIndex < params.length) {
      data[col] = params[paramIndex];
      paramIndex++;
    }
  });

  return { table, data };
}

// Execute a query (compatible with pg.query interface)
export async function query<T = any>(
  sql: string,
  params?: any[]
): Promise<QueryResult<T>> {
  const sqlLower = sql.toLowerCase().trim();

  // Handle SELECT queries - we need to fetch from the API
  if (sqlLower.startsWith('select')) {
    // For now, return empty results for SELECTs during batch mode
    // The calculator needs historical data, so we'll handle this specially
    console.warn('SELECT queries not supported in batch mode:', sql.substring(0, 50));
    return { rows: [], rowCount: 0 };
  }

  // Handle INSERT/UPSERT queries - collect them for batch write
  if (sqlLower.includes('insert')) {
    const parsed = parseInsert(sql, params);
    if (!parsed) {
      console.warn('Could not parse INSERT:', sql.substring(0, 100));
      return { rows: [], rowCount: 0 };
    }

    const { table, data } = parsed;

    if (table === 'indicator_values') {
      pendingIndicators.push({
        indicator_id: data.indicator_id,
        date: data.date,
        value: parseFloat(data.value),
        source: data.source || 'unknown',
      });
    } else if (table === 'category_scores') {
      pendingCategories.push({
        category: data.category,
        date: data.date,
        score: parseFloat(data.score),
        weight: parseFloat(data.weight),
        weighted_score: parseFloat(data.weighted_score),
      });
    } else if (table === 'pxi_scores') {
      pendingPxi = {
        date: data.date,
        score: parseFloat(data.score),
        label: data.label,
        status: data.status,
        delta_1d: data.delta_1d !== null ? parseFloat(data.delta_1d) : null,
        delta_7d: data.delta_7d !== null ? parseFloat(data.delta_7d) : null,
        delta_30d: data.delta_30d !== null ? parseFloat(data.delta_30d) : null,
      };
    } else if (table === 'indicator_scores') {
      // Indicator scores are intermediate - we can skip them for now
    } else if (table === 'fetch_logs') {
      // Fetch logs are for monitoring - skip in batch mode
    } else {
      console.warn(`Unknown table: ${table}`);
    }

    return { rows: [], rowCount: 1 };
  }

  // Other queries (UPDATE, DELETE, etc.) - warn and skip
  console.warn('Unsupported query type:', sql.substring(0, 50));
  return { rows: [], rowCount: 0 };
}

// Flush all pending writes to the Worker API
export async function flush(): Promise<{ success: boolean; written: number }> {
  if (!WRITE_API_KEY) {
    throw new Error('WRITE_API_KEY not set in environment');
  }

  const indicatorCount = pendingIndicators.length;
  const categoryCount = pendingCategories.length;
  const hasPxi = pendingPxi !== null;

  if (indicatorCount === 0 && categoryCount === 0 && !hasPxi) {
    console.log('No pending writes to flush');
    return { success: true, written: 0 };
  }

  console.log(`\nFlushing to Worker API:`);
  console.log(`  - ${indicatorCount} indicator values`);
  console.log(`  - ${categoryCount} category scores`);
  console.log(`  - ${hasPxi ? '1' : '0'} PXI score`);

  try {
    const response = await fetch(WRITE_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${WRITE_API_KEY}`,
      },
      body: JSON.stringify({
        indicators: pendingIndicators,
        categories: pendingCategories,
        pxi: pendingPxi,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`API error ${response.status}: ${errorText}`);
    }

    const result = await response.json() as { success: boolean; written: number };
    console.log(`✅ Wrote ${result.written} records to D1`);

    // Clear pending data
    pendingIndicators.length = 0;
    pendingCategories.length = 0;
    pendingPxi = null;

    return result;
  } catch (err: any) {
    console.error('Failed to flush to Worker API:', err.message);
    throw err;
  }
}

// Get pending counts for monitoring
export function getPendingCounts(): { indicators: number; categories: number; pxi: boolean } {
  return {
    indicators: pendingIndicators.length,
    categories: pendingCategories.length,
    pxi: pendingPxi !== null,
  };
}

// Pool end - triggers flush
export const pool = {
  end: async () => {
    await flush();
  },
};

// Health check
export async function healthCheck(): Promise<boolean> {
  try {
    const response = await fetch('https://pxi-api.novoamorx1.workers.dev/health');
    return response.ok;
  } catch {
    return false;
  }
}

// Batch execute (for compatibility)
export async function batchExecute(statements: string[]): Promise<void> {
  for (const stmt of statements) {
    await query(stmt);
  }
}
