import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import type { Env } from "../../src/config"
import {
  getAccuracyStats,
  getPendingPredictions,
  listPredictions,
} from "../../src/db"

type QueryResult = {
  results?: unknown[]
  first?: unknown
}

function createQueryEnv(resolver: (sql: string, params: unknown[]) => QueryResult): Env {
  const db = {
    prepare: (sql: string) => ({
      bind: (...params: unknown[]) => ({
        all: async <T>() => ({ success: true, results: (resolver(sql, params).results || []) as T[] }),
        first: async <T>() => (resolver(sql, params).first as T | null),
        run: async () => ({ success: true }),
      }),
      all: async <T>() => ({ success: true, results: (resolver(sql, []).results || []) as T[] }),
      first: async <T>() => (resolver(sql, []).first as T | null),
      run: async () => ({ success: true }),
    }),
    batch: async () => [],
    exec: async () => ({ count: 0, duration: 0 }),
    dump: async () => new ArrayBuffer(0),
  } as unknown as D1Database

  return {
    SIGNALS_DB: db,
    SIGNALS_BUCKET: {} as R2Bucket,
    SIGNALS_KV: {} as KVNamespace,
    PUBLIC_BASE_PATH: "/signals",
    DEFAULT_LOOKBACK_DAYS: 7,
    DEFAULT_BASELINE_DAYS: 30,
    DEFAULT_TOP_N: 10,
    ENABLE_COMMENTS: 0,
    ENABLE_RSS: 0,
    PRICE_PROVIDER: "none",
    ADMIN_RUN_TOKEN: "test-token",
  }
}

describe("db canonical prediction read paths", () => {
  const sqlSeen: string[] = []

  beforeEach(() => {
    sqlSeen.length = 0
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it("uses canonical CTE + window function for pending prediction reads", async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date("2026-02-20T12:00:00.000Z"))

    const env = createQueryEnv((sql, params) => {
      sqlSeen.push(sql)
      if (sql.includes("FROM canonical_predictions") && sql.includes("target_date <= ?")) {
        expect(params).toEqual(["2026-02-20"])
        return {
          results: [
            {
              id: 1,
              run_id: "run-a",
              signal_date: "2026-02-10",
              target_date: "2026-02-17",
              theme_id: "nuclear_uranium",
              theme_name: "Nuclear",
              rank: 1,
              score: 9,
              signal_type: "Rotation",
              confidence: "High",
              timing: "Now",
              stars: 5,
              proxy_etf: "URNM",
              entry_price: 100,
              exit_price: null,
              return_pct: null,
              evaluated_at: null,
              hit: null,
              created_at: "2026-02-10T15:00:00.000Z",
            },
          ],
        }
      }
      return { results: [] }
    })

    const pending = await getPendingPredictions(env)

    expect(pending).toHaveLength(1)
    expect(pending[0]?.theme_id).toBe("nuclear_uranium")

    const pendingQuery = sqlSeen.find((sql) => sql.includes("target_date <= ?"))
    expect(pendingQuery).toBeDefined()
    expect(pendingQuery).toContain("ROW_NUMBER() OVER")
    expect(pendingQuery).toContain("PARTITION BY sp.signal_date, sp.theme_id")
    expect(pendingQuery).toContain("canonical_predictions")
  })

  it("computes accuracy stats from canonical predictions CTE", async () => {
    const env = createQueryEnv((sql) => {
      sqlSeen.push(sql)

      if (sql.includes("COUNT(*) as total") && !sql.includes("GROUP BY")) {
        return {
          first: {
            total: 2,
            hits: 1,
            avg_return: 0.5,
          },
        }
      }

      if (sql.includes("GROUP BY timing")) {
        return {
          results: [
            { timing: "Now", total: 1, hits: 0, avg_return: -1 },
            { timing: "Building", total: 1, hits: 1, avg_return: 2 },
          ],
        }
      }

      if (sql.includes("GROUP BY confidence")) {
        return {
          results: [
            { confidence: "High", total: 1, hits: 0, avg_return: -1 },
            { confidence: "Medium", total: 1, hits: 1, avg_return: 2 },
          ],
        }
      }

      if (sql.includes("COUNT(*) as evaluated_total")) {
        return {
          first: {
            evaluated_total: 3,
            unresolved_total: 1,
          },
        }
      }

      return {}
    })

    const stats = await getAccuracyStats(env)

    expect(stats.overall.total).toBe(2)
    expect(stats.overall.hits).toBe(1)
    expect(stats.overall.hit_rate).toBe(50)
    expect(stats.overall.avg_return).toBe(0.5)

    expect(stats.evaluated_total).toBe(3)
    expect(stats.resolved_total).toBe(2)
    expect(stats.unresolved_total).toBe(1)
    expect(stats.unresolved_rate).toBeCloseTo(33.3333, 4)

    expect(stats.by_timing.Now?.total).toBe(1)
    expect(stats.by_confidence.High?.hits).toBe(0)

    const queriedSql = sqlSeen.join("\n")
    expect(queriedSql).toContain("ROW_NUMBER() OVER")
    expect(queriedSql).toContain("PARTITION BY sp.signal_date, sp.theme_id")
    expect(queriedSql).toContain("FROM canonical_predictions")
    expect(queriedSql).toContain("hit IS NOT NULL")
    expect(queriedSql).toContain("COUNT(*) as evaluated_total")
    expect(queriedSql).toContain("CASE WHEN hit IS NULL THEN 1 ELSE 0 END")
  })

  it("applies evaluated filter on canonical predictions in listPredictions", async () => {
    const env = createQueryEnv((sql, params) => {
      sqlSeen.push(sql)

      if (sql.includes("WHERE evaluated_at IS NULL")) {
        expect(params).toEqual([10])
        return {
          results: [
            {
              id: 3,
              run_id: "run-a",
              signal_date: "2026-02-11",
              target_date: "2026-02-18",
              theme_id: "defense_aerospace",
              theme_name: "Defense",
              rank: 1,
              score: 7,
              signal_type: "Rotation",
              confidence: "Medium",
              timing: "Building",
              stars: 4,
              proxy_etf: "ITA",
              entry_price: 50,
              exit_price: null,
              return_pct: null,
              evaluated_at: null,
              hit: null,
              created_at: "2026-02-11T15:00:00.000Z",
            },
          ],
        }
      }

      return { results: [] }
    })

    const rows = await listPredictions(env, { limit: 10, evaluated: false })

    expect(rows).toHaveLength(1)
    expect(rows[0]?.theme_id).toBe("defense_aerospace")

    const query = sqlSeen.find((sql) => sql.includes("ORDER BY signal_date DESC"))
    expect(query).toBeDefined()
    expect(query).toContain("ROW_NUMBER() OVER")
    expect(query).toContain("FROM canonical_predictions")
    expect(query).toContain("WHERE evaluated_at IS NULL")
  })
})
