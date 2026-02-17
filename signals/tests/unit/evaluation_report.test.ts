import { describe, expect, it } from "vitest"
import {
  buildEvaluationReport,
  computeTwoSidedBinomialPValue,
  normalizeEvaluationRows,
  parseEvaluationReportArgs,
} from "../../src/ops/evaluation_report"

describe("evaluation report CLI arg parsing", () => {
  it("uses deterministic defaults", () => {
    const args = parseEvaluationReportArgs(["node", "evaluation_report.ts"])

    expect(args).toEqual({
      inputPath: "data/evaluation_sample_predictions.json",
      outDir: "out/evaluation",
      minTrainSize: 20,
      testSize: 5,
      stepSize: 5,
      expandingWindow: true,
      maxSlices: undefined,
      familywiseAlpha: 0.05,
      minimumRecommendedSampleSize: 30,
      minResolvedObservations: 30,
      maxUnresolvedRatePct: 20,
      minSliceCount: 3,
    })
  })

  it("parses explicit overrides", () => {
    const args = parseEvaluationReportArgs([
      "node",
      "evaluation_report.ts",
      "--input",
      "fixtures/preds.json",
      "--out",
      "out/custom",
      "--min-train",
      "8",
      "--test-size",
      "2",
      "--step-size",
      "1",
      "--max-slices",
      "3",
      "--rolling",
      "--alpha",
      "0.1",
      "--minimum-sample",
      "12",
      "--min-resolved",
      "25",
      "--max-unresolved-rate",
      "15",
      "--min-slices",
      "4",
    ])

    expect(args).toEqual({
      inputPath: "fixtures/preds.json",
      outDir: "out/custom",
      minTrainSize: 8,
      testSize: 2,
      stepSize: 1,
      expandingWindow: false,
      maxSlices: 3,
      familywiseAlpha: 0.1,
      minimumRecommendedSampleSize: 12,
      minResolvedObservations: 25,
      maxUnresolvedRatePct: 15,
      minSliceCount: 4,
    })
  })

  it("rejects unknown flags", () => {
    expect(() =>
      parseEvaluationReportArgs(["node", "evaluation_report.ts", "--bad-flag"])
    ).toThrow("Unknown flag: --bad-flag")
  })
})

describe("evaluation report calculations", () => {
  it("computes exact two-sided binomial p-values", () => {
    const p = computeTwoSidedBinomialPValue(10, 10, 0.5)
    expect(p).toBeCloseTo(0.001953125, 10)

    const balanced = computeTwoSidedBinomialPValue(5, 10, 0.5)
    expect(balanced).toBeCloseTo(1, 10)
  })

  it("normalizes prediction payload and builds report", () => {
    const payload = {
      predictions: [
        { signal_date: "2026-01-01", rank: 1, return_pct: 2.2, hit: true, confidence: "High", timing: "Now" },
        { signal_date: "2026-01-01", rank: 2, return_pct: -1.1, hit: false, confidence: "High", timing: "Now" },
        { signal_date: "2026-01-01", rank: 3, return_pct: 0.4, hit: true, confidence: "Medium", timing: "Building" },
        { signal_date: "2026-01-02", rank: 1, return_pct: 1.8, hit: true, confidence: "High", timing: "Now" },
        { signal_date: "2026-01-02", rank: 2, return_pct: -0.5, hit: false, confidence: "Medium", timing: "Building" },
        { signal_date: "2026-01-02", rank: 3, return_pct: 0.2, hit: true, confidence: "Low", timing: "Ongoing" },
        { signal_date: "2026-01-05", rank: 1, return_pct: 2.4, hit: true, confidence: "High", timing: "Now" },
        { signal_date: "2026-01-05", rank: 2, return_pct: -0.9, hit: false, confidence: "Medium", timing: "Building" },
        { signal_date: "2026-01-05", rank: 3, return_pct: 0.3, hit: true, confidence: "Low", timing: "Ongoing" },
        { signal_date: "2026-01-06", rank: 1, return_pct: 2.0, hit: true, confidence: "High", timing: "Now" },
        { signal_date: "2026-01-06", rank: 2, return_pct: -1.2, hit: false, confidence: "Medium", timing: "Building" },
        { signal_date: "2026-01-06", rank: 3, return_pct: 0.5, hit: true, confidence: "Low", timing: "Ongoing" },
        { signal_date: "2026-01-07", rank: 1, return_pct: 2.1, hit: true, confidence: "High", timing: "Now" },
        { signal_date: "2026-01-07", rank: 2, return_pct: -1.0, hit: false, confidence: "Medium", timing: "Building" },
        { signal_date: "2026-01-07", rank: 3, return_pct: 0.1, hit: true, confidence: "Low", timing: "Ongoing" },
        { signal_date: "2026-01-08", rank: 1, return_pct: 1.7, hit: true, confidence: "High", timing: "Now" },
        { signal_date: "2026-01-08", rank: 2, return_pct: -0.8, hit: false, confidence: "Medium", timing: "Building" },
        { signal_date: "2026-01-08", rank: 3, return_pct: 0.2, hit: true, confidence: "Low", timing: "Ongoing" },
        { signal_date: "2026-01-09", rank: 1, return_pct: 2.5, hit: true, confidence: "High", timing: "Now" },
        { signal_date: "2026-01-09", rank: 2, return_pct: -1.4, hit: false, confidence: "Medium", timing: "Building" },
        { signal_date: "2026-01-09", rank: 3, return_pct: 0.6, hit: true, confidence: "Low", timing: "Ongoing" },
        { signal_date: "2026-01-12", rank: 1, return_pct: 2.3, hit: true, confidence: "High", timing: "Now" },
        { signal_date: "2026-01-12", rank: 2, return_pct: -0.7, hit: false, confidence: "Medium", timing: "Building" },
        { signal_date: "2026-01-12", rank: 3, return_pct: 0.4, hit: true, confidence: "Low", timing: "Ongoing" },
        { signal_date: "2026-01-13", rank: 1, return_pct: 1.9, hit: true, confidence: "High", timing: "Now" },
        { signal_date: "2026-01-13", rank: 2, return_pct: -0.6, hit: false, confidence: "Medium", timing: "Building" },
        { signal_date: "2026-01-13", rank: 3, return_pct: 0.2, hit: true, confidence: "Low", timing: "Ongoing" },
        { signal_date: "2026-01-14", rank: 1, return_pct: 2.2, hit: true, confidence: "High", timing: "Now" },
        { signal_date: "2026-01-14", rank: 2, return_pct: -1.1, hit: false, confidence: "Medium", timing: "Building" },
        { signal_date: "2026-01-14", rank: 3, return_pct: null, hit: null, confidence: "Low", timing: "Ongoing" },
        { signal_date: "", rank: 1, return_pct: 1.0, hit: true },
      ],
    }

    const rows = normalizeEvaluationRows(payload)
    expect(rows).toHaveLength(30)

    const report = buildEvaluationReport(rows, {
      minTrainSize: 4,
      testSize: 2,
      stepSize: 2,
      expandingWindow: true,
      maxSlices: 3,
      familywiseAlpha: 0.05,
      minimumRecommendedSampleSize: 10,
      minResolvedObservations: 20,
      maxUnresolvedRatePct: 10,
      minSliceCount: 3,
    })

    expect(report.source.observations).toBe(30)
    expect(report.source.unique_signal_dates).toBe(10)
    expect(report.source.resolved_observations).toBe(29)
    expect(report.walk_forward.slice_count).toBe(3)

    expect(report.walk_forward.slices[0].p_value_adjusted).not.toBeNull()
    expect(report.multiple_testing.hypotheses.length).toBeGreaterThan(3)
    expect(report.governance_status.status).toBe("pass")
    expect(report.governance_status.reasons).toHaveLength(0)
    expect(report.references).toHaveLength(5)
  })

  it("marks governance status as fail when thresholds are breached", () => {
    const rows = normalizeEvaluationRows({
      predictions: [
        { signal_date: "2026-01-01", rank: 1, return_pct: 1.0, hit: true },
        { signal_date: "2026-01-02", rank: 1, return_pct: -1.0, hit: null },
      ],
    })

    const report = buildEvaluationReport(rows, {
      minTrainSize: 1,
      testSize: 1,
      stepSize: 1,
      expandingWindow: true,
      maxSlices: 1,
      familywiseAlpha: 0.05,
      minimumRecommendedSampleSize: 5,
      minResolvedObservations: 2,
      maxUnresolvedRatePct: 10,
      minSliceCount: 2,
    })

    expect(report.governance_status.status).toBe("fail")
    expect(report.governance_status.reasons).toContain("resolved observations 1 below minimum 2")
    expect(report.governance_status.reasons).toContain("unresolved rate 50.0% exceeds maximum 10.0%")
    expect(report.governance_status.reasons).toContain("walk-forward slices 1 below minimum 2")
  })
})
