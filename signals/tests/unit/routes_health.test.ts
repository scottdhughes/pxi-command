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
import { getPipelineFreshness } from "../../src/db"

const getPipelineFreshnessMock = vi.mocked(getPipelineFreshness)

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

function makeRequest(): Request {
  return new Request("https://example.com/signals/api/health")
}

describe("/api/health", () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it("returns fresh status when latest successful run is within threshold", async () => {
    const env = createEnv()
    getPipelineFreshnessMock.mockResolvedValue({
      latest_success_at: "2026-02-17T05:00:00.000Z",
      hours_since_success: 1.5,
      threshold_days: 8,
      is_stale: false,
      status: "ok",
    })

    const response = await handleRequest(makeRequest(), env)

    expect(response.status).toBe(200)
    expect(response.headers.get("Cache-Control")).toBe("no-store")
    expect(getPipelineFreshnessMock).toHaveBeenCalledWith(env, { thresholdDays: 8 })

    const body = await response.json()
    expect(body).toMatchObject({
      latest_success_at: "2026-02-17T05:00:00.000Z",
      hours_since_success: 1.5,
      threshold_days: 8,
      is_stale: false,
      status: "ok",
    })
    expect(typeof body.generated_at).toBe("string")
  })

  it("returns stale status when latest successful run is older than threshold", async () => {
    const env = createEnv()
    getPipelineFreshnessMock.mockResolvedValue({
      latest_success_at: "2026-02-01T05:00:00.000Z",
      hours_since_success: 384,
      threshold_days: 8,
      is_stale: true,
      status: "stale",
    })

    const response = await handleRequest(makeRequest(), env)

    expect(response.status).toBe(200)
    expect(await response.json()).toMatchObject({
      latest_success_at: "2026-02-01T05:00:00.000Z",
      hours_since_success: 384,
      threshold_days: 8,
      is_stale: true,
      status: "stale",
    })
  })

  it("returns no_history state when no successful run exists", async () => {
    const env = createEnv()
    getPipelineFreshnessMock.mockResolvedValue({
      latest_success_at: null,
      hours_since_success: null,
      threshold_days: 8,
      is_stale: true,
      status: "no_history",
    })

    const response = await handleRequest(makeRequest(), env)

    expect(response.status).toBe(200)
    expect(await response.json()).toMatchObject({
      latest_success_at: null,
      hours_since_success: null,
      threshold_days: 8,
      is_stale: true,
      status: "no_history",
    })
  })

  it("returns 500 when health lookup fails", async () => {
    const env = createEnv()
    getPipelineFreshnessMock.mockRejectedValue(new Error("db down"))

    const response = await handleRequest(makeRequest(), env)

    expect(response.status).toBe(500)
    expect(await response.json()).toEqual({ error: "Failed to fetch pipeline health" })
  })
})
