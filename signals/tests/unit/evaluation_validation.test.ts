import { describe, expect, it } from "vitest"
import {
  computeHitRateIntervals,
  computeMultipleTestingAdjustedPvalues,
  computeRankICSeries,
  computeSpearmanCorrelation,
  computeWalkForwardSlices,
} from "../../src/evaluation_validation"

describe("computeWalkForwardSlices", () => {
  it("builds deterministic expanding-window slices with no train/test overlap", () => {
    const dates = [
      "2026-01-01",
      "2026-01-02",
      "2026-01-05",
      "2026-01-06",
      "2026-01-07",
      "2026-01-08",
      "2026-01-09",
      "2026-01-12",
      "2026-01-13",
      "2026-01-14",
    ]

    const slices = computeWalkForwardSlices(dates, {
      minTrainSize: 4,
      testSize: 2,
      stepSize: 2,
    })

    expect(slices).toHaveLength(3)
    expect(slices[0]).toMatchObject({
      slice_id: 1,
      train_start: "2026-01-01",
      train_end: "2026-01-06",
      test_start: "2026-01-07",
      test_end: "2026-01-08",
    })

    for (const slice of slices) {
      expect(slice.train_dates.at(-1)! < slice.test_dates[0]).toBe(true)
    }
  })

  it("returns empty slice set when there is insufficient data", () => {
    const slices = computeWalkForwardSlices(["2026-01-01", "2026-01-02"], {
      minTrainSize: 2,
      testSize: 2,
    })

    expect(slices).toEqual([])
  })
})

describe("computeSpearmanCorrelation", () => {
  it("computes tie-aware Spearman correlation", () => {
    const rho = computeSpearmanCorrelation([1, 2, 3, 4], [4, 3, 2, 1])
    expect(rho).toBeCloseTo(1, 8)

    const tied = computeSpearmanCorrelation([1, 2, 2, 4], [3, 2, 2, 1])
    expect(Number.isFinite(tied)).toBe(true)
    expect(tied).toBeGreaterThan(0)
  })
})

describe("computeRankICSeries", () => {
  it("returns per-date rank IC points and skips invalid/undersampled groups", () => {
    const points = computeRankICSeries([
      { signal_date: "2026-01-05", rank: 1, return_pct: 3 },
      { signal_date: "2026-01-05", rank: 2, return_pct: 2 },
      { signal_date: "2026-01-05", rank: 3, return_pct: 1 },
      { signal_date: "2026-01-12", rank: 1, return_pct: 1 },
      { signal_date: "2026-01-12", rank: 2, return_pct: 2 },
      { signal_date: "2026-01-12", rank: 3, return_pct: 3 },
      { signal_date: "2026-01-19", rank: 1, return_pct: null },
    ])

    expect(points).toHaveLength(2)

    expect(points[0]).toMatchObject({
      signal_date: "2026-01-05",
      sample_size: 3,
    })
    expect(points[0].spearman_rho).toBeCloseTo(1, 8)
    expect(points[0].rank_ic).toBeCloseTo(1, 8)

    expect(points[1].spearman_rho).toBeCloseTo(-1, 8)
    expect(points[1].rank_ic).toBeCloseTo(-1, 8)
  })
})

describe("computeHitRateIntervals", () => {
  it("returns Wilson CI-backed hit-rate summary with sample warning", () => {
    const summary = computeHitRateIntervals(24, 40, 30)

    expect(summary.hit_rate).toBeCloseTo(60, 6)
    expect(summary.hit_rate_ci_low).toBeCloseTo(44.6, 1)
    expect(summary.hit_rate_ci_high).toBeCloseTo(73.7, 1)
    expect(summary.sample_size_warning).toBe(false)

    const lowSample = computeHitRateIntervals(2, 5, 30)
    expect(lowSample.sample_size_warning).toBe(true)
  })
})

describe("computeMultipleTestingAdjustedPvalues", () => {
  it("applies Holm step-down adjusted p-values in original hypothesis order", () => {
    const adjusted = computeMultipleTestingAdjustedPvalues([
      { hypothesis_id: "h2", p_value: 0.03 },
      { hypothesis_id: "h1", p_value: 0.01 },
      { hypothesis_id: "h3", p_value: 0.2 },
    ])

    expect(adjusted).toHaveLength(3)

    const byId = new Map(adjusted.map((row) => [row.hypothesis_id, row]))
    expect(byId.get("h1")?.p_value_adjusted).toBeCloseTo(0.03, 8)
    expect(byId.get("h2")?.p_value_adjusted).toBeCloseTo(0.06, 8)
    expect(byId.get("h3")?.p_value_adjusted).toBeCloseTo(0.2, 8)
    expect(byId.get("h1")?.reject).toBe(true)
    expect(byId.get("h2")?.reject).toBe(false)
  })

  it("rejects duplicate hypothesis IDs and invalid p-values", () => {
    expect(() =>
      computeMultipleTestingAdjustedPvalues([
        { hypothesis_id: "dup", p_value: 0.1 },
        { hypothesis_id: "dup", p_value: 0.2 },
      ])
    ).toThrow(/duplicate hypothesis_id/)

    expect(() =>
      computeMultipleTestingAdjustedPvalues([
        { hypothesis_id: "bad", p_value: 1.2 },
      ])
    ).toThrow(/invalid p_value/)
  })
})
