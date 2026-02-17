import { describe, expect, it } from "vitest"
import {
  findDuplicatePredictionKeys,
  parseDeployParityArgs,
  parsePredictionRows,
  validateAccuracyPayload,
  validateHealthPayload,
  validateMigrationMarkers,
  validateVersionPayload,
} from "../../src/ops/deploy_parity"

describe("deploy parity validators", () => {
  it("accepts valid version payload", () => {
    const result = validateVersionPayload({
      generated_at: "2026-02-17T08:30:00.000Z",
      api_contract_version: "2026-02-17",
      worker_version: "signals-prod-20260217-0830",
      build_sha: "a1b2c3d4e5f6",
      build_timestamp: "2026-02-17T08:29:12.000Z",
    })

    expect(result.ok).toBe(true)
    expect(result.errors).toHaveLength(0)
  })

  it("flags invalid version payload fields", () => {
    const result = validateVersionPayload({
      generated_at: "not-a-date",
      api_contract_version: "",
      worker_version: "   ",
      build_sha: null,
      build_timestamp: "bad",
    })

    expect(result.ok).toBe(false)
    expect(result.errors).toContain("version.generated_at must be a valid ISO timestamp")
    expect(result.errors).toContain("version.api_contract_version must be a non-empty string")
    expect(result.errors).toContain("version.worker_version must be a non-empty string")
    expect(result.errors).toContain("version.build_sha must be a non-empty string")
    expect(result.errors).toContain("version.build_timestamp must be a valid ISO timestamp")
  })

  it("flags strict-mode placeholder version metadata", () => {
    const result = validateVersionPayload(
      {
        generated_at: "2026-02-17T08:30:00.000Z",
        api_contract_version: "placeholder",
        worker_version: "signals-dev",
        build_sha: "000000000000",
        build_timestamp: "1970-01-01T00:00:00.000Z",
      },
      { strictVersion: true }
    )

    expect(result.ok).toBe(false)
    expect(result.errors).toContain("version.api_contract_version cannot be a placeholder in strict mode")
    expect(result.errors).toContain("version.worker_version cannot be a placeholder in strict mode")
    expect(result.errors).toContain("version.build_sha cannot be all zeros in strict mode")
    expect(result.errors).toContain("version.build_timestamp is implausibly old in strict mode")
  })

  it("accepts strict-mode realistic version metadata", () => {
    const result = validateVersionPayload(
      {
        generated_at: "2026-02-17T08:30:00.000Z",
        api_contract_version: "2026-02-17",
        worker_version: "signals-a1b2c3d4e5f6-2026-02-17T08:30:00Z",
        build_sha: "a1b2c3d4e5f6",
        build_timestamp: "2026-02-17T08:29:12.000Z",
      },
      { strictVersion: true }
    )

    expect(result.ok).toBe(true)
    expect(result.errors).toHaveLength(0)
  })

  it("accepts matching expected release metadata", () => {
    const result = validateVersionPayload(
      {
        generated_at: "2026-02-17T08:30:00.000Z",
        api_contract_version: "2026-02-17",
        worker_version: "signals-a1b2c3d4e5f6-2026-02-17T08:30:00Z",
        build_sha: "A1B2C3D4E5F6",
        build_timestamp: "2026-02-17T08:29:12.000Z",
      },
      {
        expectedWorkerVersion: "signals-a1b2c3d4e5f6-2026-02-17T08:30:00Z",
        expectedBuildSha: "a1b2c3d4e5f6",
      }
    )

    expect(result.ok).toBe(true)
    expect(result.errors).toHaveLength(0)
  })

  it("flags mismatched expected release metadata", () => {
    const result = validateVersionPayload(
      {
        generated_at: "2026-02-17T08:30:00.000Z",
        api_contract_version: "2026-02-17",
        worker_version: "signals-a1b2c3d4e5f6-2026-02-17T08:30:00Z",
        build_sha: "a1b2c3d4e5f6",
        build_timestamp: "2026-02-17T08:29:12.000Z",
      },
      {
        expectedWorkerVersion: "signals-ffffeeee1111-2026-02-17T08:30:00Z",
        expectedBuildSha: "ffffeeee1111",
      }
    )

    expect(result.ok).toBe(false)
    expect(result.errors).toContain(
      "version.worker_version mismatch: expected 'signals-ffffeeee1111-2026-02-17T08:30:00Z' but got 'signals-a1b2c3d4e5f6-2026-02-17T08:30:00Z'"
    )
    expect(result.errors).toContain("version.build_sha mismatch: expected 'ffffeeee1111' but got 'a1b2c3d4e5f6'")
  })

  it("accepts valid health payload", () => {
    const result = validateHealthPayload({
      generated_at: "2026-02-17T08:00:00.000Z",
      latest_success_at: "2026-02-17T07:00:00.000Z",
      hours_since_success: 1,
      threshold_days: 8,
      is_stale: false,
      status: "ok",
    })

    expect(result.ok).toBe(true)
    expect(result.errors).toHaveLength(0)
  })

  it("flags missing health fields", () => {
    const result = validateHealthPayload({ status: "ok" })

    expect(result.ok).toBe(false)
    expect(result.errors).toContain("health.generated_at must be a string")
    expect(result.errors).toContain("health.threshold_days must be a number")
    expect(result.errors).toContain("health.is_stale must be a boolean")
  })

  it("accepts accuracy payload with CI fields", () => {
    const result = validateAccuracyPayload({
      generated_at: "2026-02-17T08:00:00.000Z",
      as_of: "2026-02-17T08:00:00.000Z",
      sample_size: 70,
      total_predictions: 70,
      minimum_recommended_sample_size: 30,
      evaluated_count: 75,
      resolved_count: 70,
      resolved_predictions: 70,
      unresolved_count: 5,
      unresolved_predictions: 5,
      unresolved_rate: "6.7%",
      governance_status: "PASS",
      overall: {
        hit_rate: "50.0%",
        hit_rate_ci_low: "38.6%",
        hit_rate_ci_high: "61.4%",
        count: 70,
        sample_size_warning: false,
        avg_return: "+1.1%",
      },
    })

    expect(result.ok).toBe(true)
    expect(result.errors).toHaveLength(0)
  })

  it("flags accuracy payload missing CI and completeness fields", () => {
    const result = validateAccuracyPayload({
      generated_at: "2026-02-17T08:00:00.000Z",
      sample_size: 70,
      overall: {
        hit_rate: "50.0%",
        count: 70,
        avg_return: "+1.1%",
      },
    })

    expect(result.ok).toBe(false)
    expect(result.errors).toContain("accuracy missing field: minimum_recommended_sample_size")
    expect(result.errors).toContain("accuracy missing field: evaluated_count")
    expect(result.errors).toContain("accuracy missing field: resolved_count")
    expect(result.errors).toContain("accuracy missing field: unresolved_count")
    expect(result.errors).toContain("accuracy missing field: governance_status")
    expect(result.errors).toContain("accuracy missing field: as_of")
    expect(result.errors).toContain("accuracy missing field: unresolved_rate")
    expect(result.errors).toContain("accuracy.overall missing field: hit_rate_ci_low")
    expect(result.errors).toContain("accuracy.overall missing field: hit_rate_ci_high")
  })

  it("detects duplicate prediction logical keys", () => {
    const duplicates = findDuplicatePredictionKeys([
      { signal_date: "2026-02-17", theme_id: "defense_aerospace" },
      { signal_date: "2026-02-17", theme_id: "defense_aerospace" },
      { signal_date: "2026-02-17", theme_id: "copper_critical" },
    ])

    expect(duplicates).toEqual(["2026-02-17::defense_aerospace"])
  })

  it("parses prediction rows defensively", () => {
    const rows = parsePredictionRows({
      predictions: [
        { signal_date: "2026-02-17", theme_id: "defense_aerospace" },
        { signal_date: "2026-02-17", theme_id: 123 },
        null,
      ],
    })

    expect(rows).toEqual([{ signal_date: "2026-02-17", theme_id: "defense_aerospace" }])
  })
})

describe("deploy parity CLI arg parsing", () => {
  it("uses default base with strict mode disabled", () => {
    const result = parseDeployParityArgs(["node", "deploy_parity_check.ts"])

    expect(result).toEqual({
      base: "https://pxicommand.com/signals",
      strictVersion: false,
      expectedWorkerVersion: undefined,
      expectedBuildSha: undefined,
      checkMigrations: false,
      migrationEnv: "production",
      migrationDatabase: "SIGNALS_DB",
    })
  })

  it("parses custom base and strict mode", () => {
    const result = parseDeployParityArgs([
      "node",
      "deploy_parity_check.ts",
      "https://staging.pxicommand.com/signals/",
      "--strict-version",
    ])

    expect(result).toEqual({
      base: "https://staging.pxicommand.com/signals",
      strictVersion: true,
      expectedWorkerVersion: undefined,
      expectedBuildSha: undefined,
      checkMigrations: false,
      migrationEnv: "production",
      migrationDatabase: "SIGNALS_DB",
    })
  })

  it("parses expected release metadata flags", () => {
    const result = parseDeployParityArgs([
      "node",
      "deploy_parity_check.ts",
      "--expect-worker-version",
      "signals-a1b2c3d4e5f6-2026-02-17T08:30:00Z",
      "--expect-build-sha",
      "a1b2c3d4e5f6",
    ])

    expect(result).toEqual({
      base: "https://pxicommand.com/signals",
      strictVersion: false,
      expectedWorkerVersion: "signals-a1b2c3d4e5f6-2026-02-17T08:30:00Z",
      expectedBuildSha: "a1b2c3d4e5f6",
      checkMigrations: false,
      migrationEnv: "production",
      migrationDatabase: "SIGNALS_DB",
    })
  })

  it("supports equals-style expected release metadata flags", () => {
    const result = parseDeployParityArgs([
      "node",
      "deploy_parity_check.ts",
      "--expect-worker-version=signals-a1b2c3d4e5f6-2026-02-17T08:30:00Z",
      "--expect-build-sha=a1b2c3d4e5f6",
    ])

    expect(result).toEqual({
      base: "https://pxicommand.com/signals",
      strictVersion: false,
      expectedWorkerVersion: "signals-a1b2c3d4e5f6-2026-02-17T08:30:00Z",
      expectedBuildSha: "a1b2c3d4e5f6",
      checkMigrations: false,
      migrationEnv: "production",
      migrationDatabase: "SIGNALS_DB",
    })
  })

  it("parses migration-check flags", () => {
    const result = parseDeployParityArgs([
      "node",
      "deploy_parity_check.ts",
      "--check-migrations",
      "--migration-env",
      "staging",
      "--migration-db",
      "SIGNALS_DB_STAGING",
    ])

    expect(result).toEqual({
      base: "https://pxicommand.com/signals",
      strictVersion: false,
      expectedWorkerVersion: undefined,
      expectedBuildSha: undefined,
      checkMigrations: true,
      migrationEnv: "staging",
      migrationDatabase: "SIGNALS_DB_STAGING",
    })
  })

  it("supports equals-style migration-check flags", () => {
    const result = parseDeployParityArgs([
      "node",
      "deploy_parity_check.ts",
      "--check-migrations",
      "--migration-env=production",
      "--migration-db=SIGNALS_DB",
    ])

    expect(result).toEqual({
      base: "https://pxicommand.com/signals",
      strictVersion: false,
      expectedWorkerVersion: undefined,
      expectedBuildSha: undefined,
      checkMigrations: true,
      migrationEnv: "production",
      migrationDatabase: "SIGNALS_DB",
    })
  })

  it("throws when expected metadata flag values are missing", () => {
    expect(() =>
      parseDeployParityArgs(["node", "deploy_parity_check.ts", "--expect-worker-version"])
    ).toThrow("Missing value for --expect-worker-version")

    expect(() => parseDeployParityArgs(["node", "deploy_parity_check.ts", "--expect-build-sha"]))
      .toThrow("Missing value for --expect-build-sha")
  })

  it("throws when migration flag values are missing", () => {
    expect(() => parseDeployParityArgs(["node", "deploy_parity_check.ts", "--migration-env"])).toThrow(
      "Missing value for --migration-env"
    )

    expect(() => parseDeployParityArgs(["node", "deploy_parity_check.ts", "--migration-db"])).toThrow(
      "Missing value for --migration-db"
    )
  })

  it("throws on unknown flags", () => {
    expect(() =>
      parseDeployParityArgs(["node", "deploy_parity_check.ts", "--strict-versions"])
    ).toThrow("Unknown flag: --strict-versions")
  })

  it("throws on multiple positional args", () => {
    expect(() =>
      parseDeployParityArgs([
        "node",
        "deploy_parity_check.ts",
        "https://pxicommand.com/signals",
        "https://staging.pxicommand.com/signals",
      ])
    ).toThrow("Unexpected extra positional argument: https://staging.pxicommand.com/signals")
  })
})

describe("migration marker validation", () => {
  it("accepts required migration markers", () => {
    const result = validateMigrationMarkers({
      tables: ["pipeline_locks", "signal_predictions"],
      indexes: ["idx_signal_predictions_signal_theme_unique"],
      columnsByTable: {
        signal_predictions: ["id", "exit_price_date", "evaluation_note"],
      },
    })

    expect(result.ok).toBe(true)
    expect(result.errors).toHaveLength(0)
  })

  it("flags missing required migration markers", () => {
    const result = validateMigrationMarkers({
      tables: ["signal_predictions"],
      indexes: [],
      columnsByTable: {
        signal_predictions: ["id", "exit_price_date"],
      },
    })

    expect(result.ok).toBe(false)
    expect(result.errors).toContain("migration marker missing table: pipeline_locks")
    expect(result.errors).toContain("migration marker missing index: idx_signal_predictions_signal_theme_unique")
    expect(result.errors).toContain("migration marker missing column: signal_predictions.evaluation_note")
  })
})
