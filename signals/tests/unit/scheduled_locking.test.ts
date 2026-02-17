import { beforeEach, describe, expect, it, vi } from "vitest"
import type { Env } from "../../src/config"
import type { RedditDataset } from "../../src/reddit/types"

vi.mock("../../src/db", () => ({
  insertRun: vi.fn(),
  getAccuracyStats: vi.fn(),
  acquirePipelineLock: vi.fn(),
  releasePipelineLock: vi.fn(),
}))

vi.mock("../../src/reddit/reddit_client", () => ({
  fetchRedditDataset: vi.fn(),
}))

vi.mock("../../src/analysis/metrics", () => ({
  computeMetrics: vi.fn(),
}))

vi.mock("../../src/analysis/scoring", () => ({
  scoreThemes: vi.fn(),
}))

vi.mock("../../src/analysis/classify", () => ({
  classifyTheme: vi.fn(),
}))

vi.mock("../../src/analysis/takeaways", () => ({
  buildTakeaways: vi.fn(),
}))

vi.mock("../../src/report/render_json", () => ({
  renderJson: vi.fn(),
}))

vi.mock("../../src/report/render_html", () => ({
  renderHtml: vi.fn(),
}))

vi.mock("../../src/storage", () => ({
  putObject: vi.fn(),
  setLatestRun: vi.fn(),
}))

vi.mock("../../src/evaluation", () => ({
  evaluatePendingPredictions: vi.fn(),
  storePredictions: vi.fn(),
}))

import { runPipeline, PipelineLockError } from "../../src/scheduled"
import {
  acquirePipelineLock,
  releasePipelineLock,
  getAccuracyStats,
  insertRun,
} from "../../src/db"
import { computeMetrics } from "../../src/analysis/metrics"
import { scoreThemes } from "../../src/analysis/scoring"
import { classifyTheme } from "../../src/analysis/classify"
import { buildTakeaways } from "../../src/analysis/takeaways"
import { renderJson } from "../../src/report/render_json"
import { renderHtml } from "../../src/report/render_html"
import { putObject, setLatestRun } from "../../src/storage"
import { evaluatePendingPredictions, storePredictions } from "../../src/evaluation"

const acquirePipelineLockMock = vi.mocked(acquirePipelineLock)
const releasePipelineLockMock = vi.mocked(releasePipelineLock)
const getAccuracyStatsMock = vi.mocked(getAccuracyStats)
const insertRunMock = vi.mocked(insertRun)
const computeMetricsMock = vi.mocked(computeMetrics)
const scoreThemesMock = vi.mocked(scoreThemes)
const classifyThemeMock = vi.mocked(classifyTheme)
const buildTakeawaysMock = vi.mocked(buildTakeaways)
const renderJsonMock = vi.mocked(renderJson)
const renderHtmlMock = vi.mocked(renderHtml)
const putObjectMock = vi.mocked(putObject)
const setLatestRunMock = vi.mocked(setLatestRun)
const evaluatePendingPredictionsMock = vi.mocked(evaluatePendingPredictions)
const storePredictionsMock = vi.mocked(storePredictions)

function createEnv(): Env {
  return {
    SIGNALS_DB: {} as D1Database,
    SIGNALS_BUCKET: {} as R2Bucket,
    SIGNALS_KV: {} as KVNamespace,
    PUBLIC_BASE_PATH: "/signals",
    DEFAULT_LOOKBACK_DAYS: 7,
    DEFAULT_BASELINE_DAYS: 30,
    DEFAULT_TOP_N: 1,
    ENABLE_COMMENTS: 0,
    ENABLE_RSS: 0,
    PRICE_PROVIDER: "none",
    ADMIN_RUN_TOKEN: "test-token",
  }
}

function createDataset(): RedditDataset {
  return {
    generated_at: "2026-02-17T08:00:00.000Z",
    docs: [],
  }
}

describe("runPipeline lock guard", () => {
  beforeEach(() => {
    vi.clearAllMocks()

    acquirePipelineLockMock.mockResolvedValue({ acquired: true })
    releasePipelineLockMock.mockResolvedValue()

    evaluatePendingPredictionsMock.mockResolvedValue({ evaluated: 0, hits: 0 })
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

    computeMetricsMock.mockReturnValue({
      docs: [{ id: "doc-1" }],
      metrics: [
        {
          theme_id: "ai_infra",
          evidence_links: ["a", "b", "c"],
          key_tickers: ["NVDA"],
        },
      ],
    } as never)

    scoreThemesMock.mockReturnValue([
      {
        theme_id: "ai_infra",
        theme_name: "AI Infra",
        score: 12.5,
      },
    ] as never)

    classifyThemeMock.mockReturnValue({ direction: "long" } as never)
    buildTakeawaysMock.mockReturnValue([])
    renderJsonMock.mockReturnValue({ ok: true } as never)
    renderHtmlMock.mockReturnValue("<html>ok</html>")
    storePredictionsMock.mockResolvedValue(1)
  })

  it("throws PipelineLockError when lock is already held", async () => {
    const env = createEnv()
    acquirePipelineLockMock.mockResolvedValue({ acquired: false, reason: "already_locked" })

    await expect(runPipeline(env, { dataset: createDataset() })).rejects.toBeInstanceOf(PipelineLockError)

    expect(releasePipelineLockMock).not.toHaveBeenCalled()
    expect(insertRunMock).not.toHaveBeenCalled()
    expect(putObjectMock).not.toHaveBeenCalled()
    expect(setLatestRunMock).not.toHaveBeenCalled()
  })

  it("acquires and releases lock on successful run", async () => {
    const env = createEnv()

    const result = await runPipeline(env, { dataset: createDataset() })

    expect(result.runId).toMatch(/^\d{8}-[0-9A-Z]{26}$/)
    expect(acquirePipelineLockMock).toHaveBeenCalledWith(
      env,
      "signals_pipeline_global",
      expect.any(String),
      expect.any(String),
      10800
    )

    const lockToken = acquirePipelineLockMock.mock.calls[0]?.[2]
    expect(releasePipelineLockMock).toHaveBeenCalledWith(env, "signals_pipeline_global", lockToken)
    expect(putObjectMock).toHaveBeenCalledTimes(3)
    expect(insertRunMock).toHaveBeenCalledTimes(1)
  })

  it("releases lock even when pipeline body throws", async () => {
    const env = createEnv()
    computeMetricsMock.mockImplementationOnce(() => {
      throw new Error("metrics_failed")
    })

    await expect(runPipeline(env, { dataset: createDataset() })).rejects.toThrow("metrics_failed")

    const lockToken = acquirePipelineLockMock.mock.calls[0]?.[2]
    expect(releasePipelineLockMock).toHaveBeenCalledWith(env, "signals_pipeline_global", lockToken)
  })
})
