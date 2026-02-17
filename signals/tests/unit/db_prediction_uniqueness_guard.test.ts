import { describe, expect, it } from "vitest"
import type { Env } from "../../src/config"
import {
  insertSignalPrediction,
  insertSignalPredictions,
  type SignalPredictionInput,
} from "../../src/db"

type BoundStmt = {
  __sql: string
  __params: unknown[]
  run: () => Promise<{ success: boolean }>
  all: <T>() => Promise<{ success: boolean; results: T[] }>
  first: <T>() => Promise<T | null>
}

function createEnvForInsertTests() {
  const preparedSql: string[] = []
  const batchCalls: BoundStmt[][] = []

  const db = {
    prepare: (sql: string) => {
      preparedSql.push(sql)
      return {
        bind: (...params: unknown[]): BoundStmt => ({
          __sql: sql,
          __params: params,
          run: async () => ({ success: true }),
          all: async <T>() => ({ success: true, results: [] as T[] }),
          first: async <T>() => null as T | null,
        }),
      }
    },
    batch: async (stmts: BoundStmt[]) => {
      batchCalls.push(stmts)
      return stmts.map(() => ({ success: true }))
    },
    exec: async () => ({ count: 0, duration: 0 }),
    dump: async () => new ArrayBuffer(0),
  } as unknown as D1Database

  const env = {
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
  } satisfies Env

  return { env, preparedSql, batchCalls }
}

function makePrediction(overrides: Partial<SignalPredictionInput> = {}): SignalPredictionInput {
  return {
    run_id: "run-1",
    signal_date: "2026-02-17",
    target_date: "2026-02-26",
    theme_id: "nuclear_uranium",
    theme_name: "Nuclear Uranium",
    rank: 1,
    score: 9.2,
    signal_type: "Rotation",
    confidence: "High",
    timing: "Building",
    stars: 5,
    proxy_etf: "URNM",
    entry_price: 10,
    ...overrides,
  }
}

describe("db prediction uniqueness guard", () => {
  it("uses conflict-safe insert for single prediction writes", async () => {
    const { env, preparedSql } = createEnvForInsertTests()

    await insertSignalPrediction(env, makePrediction())

    expect(preparedSql).toHaveLength(1)
    expect(preparedSql[0]).toContain("ON CONFLICT(signal_date, theme_id) DO NOTHING")
  })

  it("uses conflict-safe insert for batched prediction writes", async () => {
    const { env, preparedSql, batchCalls } = createEnvForInsertTests()

    await insertSignalPredictions(env, [
      makePrediction({ run_id: "run-a", theme_id: "nuclear_uranium" }),
      makePrediction({ run_id: "run-b", theme_id: "defense_aerospace", theme_name: "Defense Aerospace" }),
    ])

    expect(preparedSql).toHaveLength(1)
    expect(preparedSql[0]).toContain("ON CONFLICT(signal_date, theme_id) DO NOTHING")

    expect(batchCalls).toHaveLength(1)
    expect(batchCalls[0]).toHaveLength(2)
    expect(batchCalls[0][0].__params[3]).toBe("nuclear_uranium")
    expect(batchCalls[0][1].__params[3]).toBe("defense_aerospace")
    expect(batchCalls[0][0].__sql).toContain("ON CONFLICT(signal_date, theme_id) DO NOTHING")
  })
})
