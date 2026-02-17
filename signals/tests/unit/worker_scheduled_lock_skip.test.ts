import { beforeEach, describe, expect, it, vi } from "vitest"
import type { Env } from "../../src/config"

const { PipelineLockError } = vi.hoisted(() => ({
  PipelineLockError: class PipelineLockError extends Error {},
}))

vi.mock("../../src/routes", () => ({
  handleRequest: vi.fn(),
}))

vi.mock("../../src/scheduled", () => ({
  runPipeline: vi.fn(),
  isPipelineLockError: vi.fn((error: unknown) => error instanceof PipelineLockError),
}))

vi.mock("../../src/db", () => ({
  getLatestSuccessfulRun: vi.fn(),
  insertRun: vi.fn(),
}))

import worker from "../../src/worker"
import { runPipeline } from "../../src/scheduled"
import { getLatestSuccessfulRun, insertRun } from "../../src/db"

const runPipelineMock = vi.mocked(runPipeline)
const getLatestSuccessfulRunMock = vi.mocked(getLatestSuccessfulRun)
const insertRunMock = vi.mocked(insertRun)

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

function createScheduledEvent(isoTime: string): ScheduledEvent {
  return {
    cron: "0 15 * * MON",
    scheduledTime: Date.parse(isoTime),
    type: "scheduled",
  }
}

describe("worker scheduled lock conflict handling", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    getLatestSuccessfulRunMock.mockResolvedValue({
      id: "20260202-01ARZ3NDEKTSV4RRFFQ69G5FAV",
      created_at_utc: "2026-02-02T15:00:00.000Z",
    })
  })

  it("skips error run insert when lock conflict occurs", async () => {
    const env = createEnv()
    runPipelineMock.mockRejectedValue(new PipelineLockError("pipeline_locked"))

    await worker.scheduled(createScheduledEvent("2026-02-09T15:00:00.000Z"), env, {} as ExecutionContext)

    expect(insertRunMock).not.toHaveBeenCalled()
  })

  it("records error run for non-lock failures", async () => {
    const env = createEnv()
    runPipelineMock.mockRejectedValue(new Error("pipeline_crashed"))

    await worker.scheduled(createScheduledEvent("2026-02-09T15:00:00.000Z"), env, {} as ExecutionContext)

    expect(insertRunMock).toHaveBeenCalledTimes(1)
  })
})
