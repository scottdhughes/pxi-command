import { beforeEach, describe, expect, it, vi } from "vitest"
import type { Env } from "../../src/config"

vi.mock("../../src/db", () => ({
  listRuns: vi.fn(),
  getRun: vi.fn(),
  insertRun: vi.fn(),
  getAccuracyStats: vi.fn(),
  listPredictions: vi.fn(),
  getPipelineFreshness: vi.fn(),
}))

vi.mock("../../src/storage", () => ({
  getObjectText: vi.fn(),
  getLatestRunId: vi.fn(),
}))

vi.mock("../../src/scheduled", () => ({
  runPipeline: vi.fn(),
}))

import { handleRequest } from "../../src/routes"
import { listPredictions } from "../../src/db"

const listPredictionsMock = vi.mocked(listPredictions)

function createEnv(): Env {
  return {
    SIGNALS_DB: {} as D1Database,
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

function makeRequest(query = ""): Request {
  return new Request(`https://example.com/signals/api/predictions${query}`)
}

describe("/api/predictions query validation", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    listPredictionsMock.mockResolvedValue([])
  })

  it("uses default limit when query params are omitted", async () => {
    const env = createEnv()

    const response = await handleRequest(makeRequest(), env)

    expect(response.status).toBe(200)
    expect(response.headers.get("Cache-Control")).toBe("no-store")
    expect(listPredictionsMock).toHaveBeenCalledWith(env, { limit: 50 })
  })

  it("clamps huge limit values to 100", async () => {
    const env = createEnv()

    const response = await handleRequest(makeRequest("?limit=9999"), env)

    expect(response.status).toBe(200)
    expect(listPredictionsMock).toHaveBeenCalledWith(env, { limit: 100 })
  })

  it.each(["0", "-5"])("clamps non-positive integer limit %s to 1", async (value) => {
    const env = createEnv()

    const response = await handleRequest(makeRequest(`?limit=${value}`), env)

    expect(response.status).toBe(200)
    expect(listPredictionsMock).toHaveBeenCalledWith(env, { limit: 1 })
  })

  it.each(["abc", "10.5", "", "1e2"])("rejects malformed limit value %s with 400", async (value) => {
    const env = createEnv()

    const response = await handleRequest(makeRequest(`?limit=${value}`), env)

    expect(response.status).toBe(400)
    expect(await response.json()).toEqual({ error: "Invalid limit. Must be an integer." })
    expect(listPredictionsMock).not.toHaveBeenCalled()
  })

  it("rejects malformed evaluated filter with 400", async () => {
    const env = createEnv()

    const response = await handleRequest(makeRequest("?evaluated=yes"), env)

    expect(response.status).toBe(400)
    expect(await response.json()).toEqual({ error: "Invalid evaluated filter. Use true or false." })
    expect(listPredictionsMock).not.toHaveBeenCalled()
  })

  it("passes valid evaluated=true and limit values through", async () => {
    const env = createEnv()

    const response = await handleRequest(makeRequest("?evaluated=true&limit=12"), env)

    expect(response.status).toBe(200)
    expect(listPredictionsMock).toHaveBeenCalledWith(env, { limit: 12, evaluated: true })
  })

  it("surfaces exit_price_date and evaluation_note fields for auditability", async () => {
    const env = createEnv()
    listPredictionsMock.mockResolvedValue([
      {
        id: 1,
        run_id: "run-1",
        signal_date: "2026-02-17",
        target_date: "2026-02-26",
        theme_id: "nuclear_uranium",
        theme_name: "Nuclear Uranium",
        rank: 1,
        score: 9.1,
        signal_type: "Rotation",
        confidence: "High",
        timing: "Now",
        stars: 5,
        proxy_etf: "URNM",
        entry_price: 100,
        exit_price: null,
        exit_price_date: null,
        return_pct: null,
        evaluated_at: "2026-03-01T14:00:00.000Z",
        hit: null,
        evaluation_note: "historical_price_unavailable",
        created_at: "2026-02-17T15:00:00.000Z",
      },
    ])

    const response = await handleRequest(makeRequest("?evaluated=true&limit=1"), env)

    expect(response.status).toBe(200)
    const body = await response.json()

    expect(body.predictions[0]).toMatchObject({
      exit_price_date: null,
      evaluation_note: "historical_price_unavailable",
      status: "evaluated",
      hit: null,
    })
  })
})
