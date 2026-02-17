import { describe, expect, it } from "vitest"
import type { Env } from "../../src/config"
import { getPipelineFreshness } from "../../src/db"

type QueryResult = {
  first?: unknown
}

function createQueryEnv(resolver: (sql: string, params: unknown[]) => QueryResult): Env {
  const db = {
    prepare: (sql: string) => ({
      bind: (...params: unknown[]) => ({
        all: async <T>() => ({ success: true, results: [] as T[] }),
        first: async <T>() => (resolver(sql, params).first as T | null),
        run: async () => ({ success: true }),
      }),
      all: async <T>() => ({ success: true, results: [] as T[] }),
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

describe("getPipelineFreshness", () => {
  it("returns ok when latest successful run is within threshold", async () => {
    const env = createQueryEnv((sql) => {
      if (sql.includes("FROM runs") && sql.includes("status = 'ok'")) {
        return {
          first: {
            id: "20260216-01",
            created_at_utc: "2026-02-16T20:00:00.000Z",
          },
        }
      }
      return {}
    })

    const freshness = await getPipelineFreshness(env, {
      now: new Date("2026-02-17T06:00:00.000Z"),
      thresholdDays: 8,
    })

    expect(freshness).toEqual({
      latest_success_at: "2026-02-16T20:00:00.000Z",
      hours_since_success: 10,
      threshold_days: 8,
      is_stale: false,
      status: "ok",
    })
  })

  it("returns stale when latest successful run exceeds threshold", async () => {
    const env = createQueryEnv((sql) => {
      if (sql.includes("FROM runs") && sql.includes("status = 'ok'")) {
        return {
          first: {
            id: "20260101-01",
            created_at_utc: "2026-01-01T00:00:00.000Z",
          },
        }
      }
      return {}
    })

    const freshness = await getPipelineFreshness(env, {
      now: new Date("2026-02-17T00:00:00.000Z"),
      thresholdDays: 8,
    })

    expect(freshness.is_stale).toBe(true)
    expect(freshness.status).toBe("stale")
    expect(freshness.hours_since_success).toBeGreaterThan(8 * 24)
  })

  it("returns no_history when no successful run exists", async () => {
    const env = createQueryEnv(() => ({ first: null }))

    const freshness = await getPipelineFreshness(env)

    expect(freshness).toEqual({
      latest_success_at: null,
      hours_since_success: null,
      threshold_days: 8,
      is_stale: true,
      status: "no_history",
    })
  })

  it("returns stale when run timestamp is invalid", async () => {
    const env = createQueryEnv((sql) => {
      if (sql.includes("FROM runs") && sql.includes("status = 'ok'")) {
        return {
          first: {
            id: "20260216-01",
            created_at_utc: "not-a-date",
          },
        }
      }
      return {}
    })

    const freshness = await getPipelineFreshness(env)

    expect(freshness).toEqual({
      latest_success_at: "not-a-date",
      hours_since_success: null,
      threshold_days: 8,
      is_stale: true,
      status: "stale",
    })
  })
})
