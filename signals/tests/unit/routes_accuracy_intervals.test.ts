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
import { getAccuracyStats } from "../../src/db"

const getAccuracyStatsMock = vi.mocked(getAccuracyStats)

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
  return new Request("https://example.com/signals/api/accuracy")
}

describe("/api/accuracy intervals", () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it("returns Wilson confidence intervals and no warning for sufficiently large samples", async () => {
    const env = createEnv()
    getAccuracyStatsMock.mockResolvedValue({
      overall: {
        total: 40,
        hits: 24,
        hit_rate: 60,
        hit_rate_ci_low: 44.6,
        hit_rate_ci_high: 73.7,
        avg_return: 1.23,
      },
      by_timing: {
        Now: {
          total: 31,
          hits: 20,
          hit_rate: 64.5,
          hit_rate_ci_low: 46.9,
          hit_rate_ci_high: 78.9,
          avg_return: 1.5,
        },
      },
      by_confidence: {
        High: {
          total: 35,
          hits: 23,
          hit_rate: 65.7,
          hit_rate_ci_low: 48.1,
          hit_rate_ci_high: 80.0,
          avg_return: 1.8,
        },
      },
      evaluated_total: 43,
      resolved_total: 40,
      unresolved_total: 3,
      unresolved_rate: 6.976744186,
    })

    const response = await handleRequest(makeRequest(), env)

    expect(response.status).toBe(200)
    expect(response.headers.get("Cache-Control")).toBe("no-store")
    const body = await response.json()

    expect(body.minimum_recommended_sample_size).toBe(30)
    expect(body.evaluated_count).toBe(43)
    expect(body.resolved_count).toBe(40)
    expect(body.unresolved_count).toBe(3)
    expect(body.unresolved_rate).toBe("7.0%")
    expect(body.overall).toMatchObject({
      hit_rate: "60.0%",
      hit_rate_ci_low: "44.6%",
      hit_rate_ci_high: "73.7%",
      count: 40,
      sample_size_warning: false,
      avg_return: "+1.2%",
    })

    expect(body.by_timing.Now.sample_size_warning).toBe(false)
    expect(body.by_confidence.High.sample_size_warning).toBe(false)
  })

  it("flags low-sample groups to avoid false confidence", async () => {
    const env = createEnv()
    getAccuracyStatsMock.mockResolvedValue({
      overall: {
        total: 12,
        hits: 8,
        hit_rate: 66.7,
        hit_rate_ci_low: 39.1,
        hit_rate_ci_high: 86.2,
        avg_return: -0.4,
      },
      by_timing: {
        Building: {
          total: 8,
          hits: 5,
          hit_rate: 62.5,
          hit_rate_ci_low: 30.6,
          hit_rate_ci_high: 86.3,
          avg_return: -0.3,
        },
      },
      by_confidence: {
        Medium: {
          total: 4,
          hits: 3,
          hit_rate: 75,
          hit_rate_ci_low: 30.1,
          hit_rate_ci_high: 95.4,
          avg_return: -0.8,
        },
      },
      evaluated_total: 15,
      resolved_total: 12,
      unresolved_total: 3,
      unresolved_rate: 20,
    })

    const response = await handleRequest(makeRequest(), env)

    expect(response.status).toBe(200)
    const body = await response.json()

    expect(body.overall.sample_size_warning).toBe(true)
    expect(body.by_timing.Building.sample_size_warning).toBe(true)
    expect(body.by_confidence.Medium.sample_size_warning).toBe(true)
    expect(body.unresolved_count).toBe(3)
    expect(body.unresolved_rate).toBe("20.0%")
    expect(body.overall.avg_return).toBe("-0.4%")
  })

  it("returns explicit zero-sample interval fields deterministically", async () => {
    const env = createEnv()
    getAccuracyStatsMock.mockResolvedValue({
      overall: {
        total: 0,
        hits: 0,
        hit_rate: 0,
        hit_rate_ci_low: 0,
        hit_rate_ci_high: 0,
        avg_return: 0,
      },
      by_timing: {},
      by_confidence: {},
      evaluated_total: 0,
      resolved_total: 0,
      unresolved_total: 0,
      unresolved_rate: 0,
    })

    const response = await handleRequest(makeRequest(), env)

    expect(response.status).toBe(200)
    expect(await response.json()).toMatchObject({
      sample_size: 0,
      evaluated_count: 0,
      resolved_count: 0,
      unresolved_count: 0,
      unresolved_rate: "0.0%",
      overall: {
        hit_rate: "0.0%",
        hit_rate_ci_low: "0.0%",
        hit_rate_ci_high: "0.0%",
        count: 0,
        sample_size_warning: true,
        avg_return: "+0.0%",
      },
      by_timing: {},
      by_confidence: {},
    })
  })

  it("returns 500 when accuracy lookup fails", async () => {
    const env = createEnv()
    getAccuracyStatsMock.mockRejectedValue(new Error("db down"))

    const response = await handleRequest(makeRequest(), env)

    expect(response.status).toBe(500)
    expect(await response.json()).toEqual({ error: "Failed to fetch accuracy stats" })
  })
})
