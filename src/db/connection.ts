// D1 Database client for Node.js
// Uses wrangler CLI to execute queries against remote D1
// Drop-in replacement for the PostgreSQL connection.ts

import { execSync } from 'child_process';

const DB_NAME = 'pxi-db';

interface QueryResult<T = any> {
  rows: T[];
  rowCount: number;
}

// Convert PostgreSQL $1, $2, $3 placeholders to SQLite ?
function convertPlaceholders(sql: string): string {
  return sql.replace(/\$\d+/g, '?');
}

// Escape a value for SQL
function escapeValue(val: any): string {
  if (val === null || val === undefined) return 'NULL';
  if (typeof val === 'number') return String(val);
  if (typeof val === 'boolean') return val ? '1' : '0';
  if (val instanceof Date) return `'${val.toISOString().split('T')[0]}'`;
  // Escape single quotes
  return `'${String(val).replace(/'/g, "''")}'`;
}

// Build SQL with parameters inlined (for wrangler CLI)
function buildSql(sql: string, params?: any[]): string {
  let converted = convertPlaceholders(sql);

  // Replace NOW() with SQLite equivalent
  converted = converted.replace(/NOW\(\)/gi, "datetime('now')");

  // Replace EXCLUDED with SQLite syntax (already works in SQLite)
  // ON CONFLICT ... DO UPDATE SET x = EXCLUDED.x works in SQLite 3.24+

  if (!params || params.length === 0) return converted;

  // Replace ? with actual values
  let paramIndex = 0;
  return converted.replace(/\?/g, () => {
    if (paramIndex >= params.length) {
      throw new Error('Not enough parameters provided');
    }
    return escapeValue(params[paramIndex++]);
  });
}

// Execute a query (compatible with pg.query interface)
export async function query<T = any>(
  sql: string,
  params?: any[]
): Promise<QueryResult<T>> {
  const finalSql = buildSql(sql, params);

  try {
    // Pass SQL via environment variable to avoid shell escaping issues
    // Use bash -c with single quotes so $D1_SQL is expanded from environment
    const result = execSync(
      `bash -c 'npx wrangler d1 execute ${DB_NAME} --remote --json --command "$D1_SQL"'`,
      {
        encoding: 'utf-8',
        maxBuffer: 50 * 1024 * 1024,
        stdio: ['pipe', 'pipe', 'pipe'],
        env: { ...process.env, D1_SQL: finalSql },
      }
    );

    const parsed = JSON.parse(result);
    const results = parsed[0]?.results || [];

    return {
      rows: results,
      rowCount: results.length,
    };
  } catch (err: any) {
    // Check if it's just a non-SELECT query (no results expected)
    if (err.stdout) {
      try {
        const parsed = JSON.parse(err.stdout);
        if (parsed[0]?.success) {
          return { rows: [], rowCount: 0 };
        }
      } catch {}
    }
    console.error('D1 query error:', err.message);
    console.error('SQL:', sql);
    throw err;
  }
}

// Batch execute multiple SQL statements
export async function batchExecute(statements: string[]): Promise<void> {
  // Execute each statement individually
  for (const stmt of statements) {
    await query(stmt);
  }
}

// Health check
export async function healthCheck(): Promise<boolean> {
  try {
    await query('SELECT 1');
    return true;
  } catch {
    return false;
  }
}

// Pool end (no-op for D1, but needed for compatibility)
export const pool = {
  end: async () => {},
};
