import { beforeEach, describe, expect, it, vi } from "vitest"
import type { Env } from "../../src/config"

const { PipelineLockError } = vi.hoisted(() => ({
  PipelineLockError: class PipelineLockError extends Error {},
}))

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
  isPipelineLockError: vi.fn((error: unknown) => error instanceof PipelineLockError),
}))

import { handleRequest } from "../../src/routes"
import { runPipeline, isPipelineLockError } from "../../src/scheduled"
import { insertRun } from "../../src/db"

const runPipelineMock = vi.mocked(runPipeline)
const isPipelineLockErrorMock = vi.mocked(isPipelineLockError)
const insertRunMock = vi.mocked(insertRun)

function createEnv(): Env {
  return {
    SIGNALS_DB: {} as D1Database,
    SIGNALS_BUCKET: {} as R2Bucket,
    SIGNALS_KV: {
      get: vi.fn().mockResolvedValue(null),
      put: vi.fn().mockResolvedValue(undefined),
    } as unknown as KVNamespace,
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
  return new Request("https://example.com/signals/api/run", {
    method: "POST",
    headers: {
      "X-Admin-Token": "test-token",
    },
  })
}

describe("POST /api/run lock conflict handling", () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it("returns 409 and does not write error run when pipeline is locked", async () => {
    const env = createEnv()
    runPipelineMock.mockRejectedValue(new PipelineLockError("pipeline_locked"))

    const response = await handleRequest(makeRequest(), env)

    expect(response.status).toBe(409)
    expect(response.headers.get("Cache-Control")).toBe("no-store")
    expect(await response.json()).toEqual({ ok: false, error: "pipeline_locked" })
    expect(isPipelineLockErrorMock).toHaveBeenCalledTimes(1)
    expect(insertRunMock).not.toHaveBeenCalled()
  })

  it("returns sanitized 500 error and writes internal error details for non-lock failures", async () => {
    const env = createEnv()
    runPipelineMock.mockRejectedValue(new Error("upstream failed"))

    const response = await handleRequest(makeRequest(), env)

    expect(response.status).toBe(500)
    expect(response.headers.get("Cache-Control")).toBe("no-store")
    expect(await response.json()).toEqual({ ok: false, error: "run_failed" })
    expect(insertRunMock).toHaveBeenCalledTimes(1)
    expect(insertRunMock.mock.calls[0]?.[1]).toMatchObject({
      status: "error",
      error_message: "upstream failed",
    })
  })
})
