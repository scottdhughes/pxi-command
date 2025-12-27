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
  try {
    const finalSql = buildSql(sql, params);

    // Escape for shell - use base64 to avoid escaping issues
    const base64Sql = Buffer.from(finalSql).toString('base64');

    const result = execSync(
      `echo "${base64Sql}" | base64 -d | npx wrangler d1 execute ${DB_NAME} --remote --json`,
      {
        encoding: 'utf-8',
        maxBuffer: 50 * 1024 * 1024,
        stdio: ['pipe', 'pipe', 'pipe'],
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
    if (err.message?.includes('results')) {
      return { rows: [], rowCount: 0 };
    }
    console.error('D1 query error:', err.message);
    console.error('SQL:', sql);
    throw err;
  }
}

// Batch execute multiple SQL statements
export async function batchExecute(statements: string[]): Promise<void> {
  if (statements.length === 0) return;

  const sql = statements.join('\n');
  const base64Sql = Buffer.from(sql).toString('base64');

  try {
    execSync(
      `echo "${base64Sql}" | base64 -d | npx wrangler d1 execute ${DB_NAME} --remote`,
      {
        encoding: 'utf-8',
        stdio: 'inherit',
      }
    );
  } catch (err: any) {
    console.error('D1 batch execute error:', err.message);
    throw err;
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
