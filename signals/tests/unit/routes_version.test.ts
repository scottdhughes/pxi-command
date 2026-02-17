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
  isPipelineLockError: vi.fn(),
}))

import { handleRequest } from "../../src/routes"

function createEnv(overrides: Partial<Env> = {}): Env {
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
    BUILD_SHA: "abc123def456",
    BUILD_TIMESTAMP: "2026-02-17T09:00:00Z",
    WORKER_VERSION: "signals-prod-20260217-0900",
    ...overrides,
  }
}

describe("GET /api/version", () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it("returns deploy manifest metadata", async () => {
    const env = createEnv()
    const request = new Request("https://example.com/signals/api/version")

    const response = await handleRequest(request, env)

    expect(response.status).toBe(200)
    expect(response.headers.get("Cache-Control")).toBe("no-store")

    const body = await response.json()
    expect(body).toMatchObject({
      api_contract_version: "2026-02-17",
      worker_version: "signals-prod-20260217-0900",
      build_sha: "abc123def456",
      build_timestamp: "2026-02-17T09:00:00.000Z",
    })
    expect(typeof body.generated_at).toBe("string")
  })

  it("returns null build timestamp when metadata is not parseable", async () => {
    const env = createEnv({ BUILD_TIMESTAMP: "not-a-timestamp" })
    const request = new Request("https://example.com/signals/api/version")

    const response = await handleRequest(request, env)
    const body = await response.json()

    expect(response.status).toBe(200)
    expect(body.build_timestamp).toBeNull()
  })
})
