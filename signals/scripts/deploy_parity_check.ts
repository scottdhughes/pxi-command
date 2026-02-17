import {
  findDuplicatePredictionKeys,
  parseDeployParityArgs,
  parsePredictionRows,
  validateAccuracyPayload,
  validateHealthPayload,
  validateMigrationMarkers,
  validateVersionPayload,
} from "../src/ops/deploy_parity"
import { execFile } from "node:child_process"
import { promisify } from "node:util"

const execFileAsync = promisify(execFile)
const WRANGLER_JSON_MAX_BUFFER = 10 * 1024 * 1024

interface JsonRecord {
  [key: string]: unknown
}

function asRecord(value: unknown): JsonRecord | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null
  }
  return value as JsonRecord
}

function extractRows(payload: unknown): JsonRecord[] {
  const rows: JsonRecord[] = []
  const rowContainerKeys = ["results", "result", "rows"]

  const visit = (value: unknown) => {
    if (Array.isArray(value)) {
      for (const item of value) {
        visit(item)
      }
      return
    }

    const record = asRecord(value)
    if (!record) {
      return
    }

    for (const key of rowContainerKeys) {
      const container = record[key]
      if (!Array.isArray(container)) {
        continue
      }

      for (const item of container) {
        const row = asRecord(item)
        if (row) {
          rows.push(row)
        }
      }
    }
  }

  visit(payload)
  return rows
}

function extractStringField(rows: JsonRecord[], field: string): string[] {
  const values: string[] = []
  for (const row of rows) {
    const value = row[field]
    if (typeof value === "string" && value.trim().length > 0) {
      values.push(value.trim())
    }
  }
  return values
}

function formatExecFailure(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error)
  const details = error as { stdout?: unknown; stderr?: unknown }
  const stdout = typeof details.stdout === "string" ? details.stdout.trim() : ""
  const stderr = typeof details.stderr === "string" ? details.stderr.trim() : ""

  if (stdout.length === 0 && stderr.length === 0) {
    return message
  }

  return `${message} (stdout: ${stdout || "n/a"}; stderr: ${stderr || "n/a"})`
}

async function runWranglerD1Query(database: string, envName: string, sql: string): Promise<JsonRecord[]> {
  const args = [
    "wrangler",
    "d1",
    "execute",
    database,
    "--remote",
    "--env",
    envName,
    "--command",
    sql,
    "--json",
  ]

  try {
    const { stdout } = await execFileAsync("npx", args, {
      encoding: "utf8",
      maxBuffer: WRANGLER_JSON_MAX_BUFFER,
      env: process.env,
    })

    const parsed = JSON.parse(stdout) as unknown
    return extractRows(parsed)
  } catch (error: unknown) {
    throw new Error(`wrangler D1 query failed: ${formatExecFailure(error)}`)
  }
}

async function fetchMigrationSnapshot(database: string, envName: string) {
  const [tableRows, indexRows, signalPredictionColumns] = await Promise.all([
    runWranglerD1Query(
      database,
      envName,
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('pipeline_locks', 'signal_predictions');"
    ),
    runWranglerD1Query(
      database,
      envName,
      "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_signal_predictions_signal_theme_unique';"
    ),
    runWranglerD1Query(database, envName, "PRAGMA table_info(signal_predictions);"),
  ])

  return {
    tables: extractStringField(tableRows, "name"),
    indexes: extractStringField(indexRows, "name"),
    columnsByTable: {
      signal_predictions: extractStringField(signalPredictionColumns, "name"),
    },
  }
}

async function fetchJson(url: string): Promise<{ status: number; body: unknown }> {
  const response = await fetch(url, {
    headers: { "Cache-Control": "no-cache" },
  })

  const text = await response.text()
  let body: unknown = null

  try {
    body = JSON.parse(text)
  } catch {
    body = text
  }

  return { status: response.status, body }
}

async function run() {
  const {
    base,
    strictVersion,
    expectedWorkerVersion,
    expectedBuildSha,
    checkMigrations,
    migrationEnv,
    migrationDatabase,
  } = parseDeployParityArgs(process.argv)
  const versionUrl = `${base}/api/version`
  const healthUrl = `${base}/api/health`
  const accuracyUrl = `${base}/api/accuracy`
  const predictionsUrl = `${base}/api/predictions?limit=100`

  const [versionRes, healthRes, accuracyRes, predictionsRes] = await Promise.all([
    fetchJson(versionUrl),
    fetchJson(healthUrl),
    fetchJson(accuracyUrl),
    fetchJson(predictionsUrl),
  ])

  const failures: string[] = []

  if (versionRes.status !== 200) {
    failures.push(`version endpoint returned HTTP ${versionRes.status}`)
  } else {
    const validation = validateVersionPayload(versionRes.body, {
      strictVersion,
      expectedWorkerVersion,
      expectedBuildSha,
    })
    failures.push(...validation.errors)
  }

  if (healthRes.status !== 200) {
    failures.push(`health endpoint returned HTTP ${healthRes.status}`)
  } else {
    const validation = validateHealthPayload(healthRes.body)
    failures.push(...validation.errors)
  }

  if (accuracyRes.status !== 200) {
    failures.push(`accuracy endpoint returned HTTP ${accuracyRes.status}`)
  } else {
    const validation = validateAccuracyPayload(accuracyRes.body)
    failures.push(...validation.errors)
  }

  if (predictionsRes.status !== 200) {
    failures.push(`predictions endpoint returned HTTP ${predictionsRes.status}`)
  } else {
    const rows = parsePredictionRows(predictionsRes.body)
    const duplicates = findDuplicatePredictionKeys(rows)
    if (duplicates.length > 0) {
      failures.push(`duplicate signal_date/theme_id keys found: ${duplicates.join(", ")}`)
    }
  }

  const migrationFailures: string[] = []
  if (checkMigrations) {
    try {
      const snapshot = await fetchMigrationSnapshot(migrationDatabase, migrationEnv)
      const migrationValidation = validateMigrationMarkers(snapshot)
      migrationFailures.push(...migrationValidation.errors)
      failures.push(...migrationValidation.errors)
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error)
      const wrapped = `migration marker check failed: ${message}`
      migrationFailures.push(wrapped)
      failures.push(wrapped)
    }
  }

  const summary = {
    checked_at_utc: new Date().toISOString(),
    base,
    strict_version: strictVersion,
    expected_worker_version: expectedWorkerVersion ?? null,
    expected_build_sha: expectedBuildSha ?? null,
    migration_check: {
      enabled: checkMigrations,
      env: checkMigrations ? migrationEnv : null,
      database: checkMigrations ? migrationDatabase : null,
      failures: migrationFailures,
    },
    endpoints: {
      version: { url: versionUrl, status: versionRes.status },
      health: { url: healthUrl, status: healthRes.status },
      accuracy: { url: accuracyUrl, status: accuracyRes.status },
      predictions: { url: predictionsUrl, status: predictionsRes.status },
    },
    pass: failures.length === 0,
    failures,
  }

  console.log(JSON.stringify(summary, null, 2))

  if (failures.length > 0) {
    process.exitCode = 1
  }
}

run().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error)
  console.error(`[deploy_parity_check] failed: ${message}`)
  process.exit(1)
})
