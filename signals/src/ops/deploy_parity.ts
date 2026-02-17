export interface DeployParityValidation {
  ok: boolean
  errors: string[]
}

interface JsonObject {
  [key: string]: unknown
}

export interface VersionValidationOptions {
  strictVersion?: boolean
  expectedWorkerVersion?: string
  expectedBuildSha?: string
}

export interface DeployParityCliArgs {
  base: string
  strictVersion: boolean
  expectedWorkerVersion?: string
  expectedBuildSha?: string
  checkMigrations: boolean
  migrationEnv: string
  migrationDatabase: string
}

export interface MigrationMarkerSnapshot {
  tables: string[]
  indexes: string[]
  columnsByTable: Record<string, string[]>
}

export interface MigrationMarkerRequirements {
  tables: string[]
  indexes: string[]
  columnsByTable: Record<string, string[]>
}

const STRICT_PLACEHOLDER_VALUES = new Set([
  "local-dev",
  "signals-dev",
  "dev",
  "test",
  "placeholder",
  "unknown",
  "unset",
  "n/a",
  "na",
  "none",
  "null",
  "undefined",
])

const STRICT_MIN_BUILD_TIMESTAMP_MS = Date.UTC(2010, 0, 1, 0, 0, 0, 0)
const DEFAULT_MIGRATION_ENV = "production"
const DEFAULT_MIGRATION_DATABASE = "SIGNALS_DB"

export const REQUIRED_MIGRATION_MARKERS: MigrationMarkerRequirements = {
  tables: ["pipeline_locks"],
  indexes: ["idx_signal_predictions_signal_theme_unique"],
  columnsByTable: {
    signal_predictions: ["exit_price_date", "evaluation_note"],
  },
}

function asRecord(value: unknown): JsonObject | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null
  }
  return value as JsonObject
}

function normalizeString(value: unknown): string | null {
  if (typeof value !== "string") return null
  const normalized = value.trim()
  return normalized.length > 0 ? normalized : null
}

function isPlaceholderValue(value: string): boolean {
  const normalized = value.trim().toLowerCase()
  return STRICT_PLACEHOLDER_VALUES.has(normalized) || normalized.includes("placeholder")
}

function isLikelyGitSha(value: string): boolean {
  return /^[0-9a-f]{7,40}$/i.test(value)
}

export function parseDeployParityArgs(argv: string[]): DeployParityCliArgs {
  const defaultBase = "https://pxicommand.com/signals"
  let base = defaultBase
  let strictVersion = false
  let expectedWorkerVersion: string | undefined
  let expectedBuildSha: string | undefined
  let checkMigrations = false
  let migrationEnv = DEFAULT_MIGRATION_ENV
  let migrationDatabase = DEFAULT_MIGRATION_DATABASE
  let hasPositionalBase = false

  const args = argv.slice(2)

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i].trim()

    if (arg.length === 0) continue

    if (arg === "--strict-version") {
      strictVersion = true
      continue
    }

    if (arg === "--expect-worker-version") {
      const value = normalizeString(args[i + 1])
      if (!value || value.startsWith("--")) {
        throw new Error("Missing value for --expect-worker-version")
      }
      expectedWorkerVersion = value
      i += 1
      continue
    }

    if (arg.startsWith("--expect-worker-version=")) {
      const value = normalizeString(arg.split("=").slice(1).join("="))
      if (!value) {
        throw new Error("Missing value for --expect-worker-version")
      }
      expectedWorkerVersion = value
      continue
    }

    if (arg === "--expect-build-sha") {
      const value = normalizeString(args[i + 1])
      if (!value || value.startsWith("--")) {
        throw new Error("Missing value for --expect-build-sha")
      }
      expectedBuildSha = value
      i += 1
      continue
    }

    if (arg.startsWith("--expect-build-sha=")) {
      const value = normalizeString(arg.split("=").slice(1).join("="))
      if (!value) {
        throw new Error("Missing value for --expect-build-sha")
      }
      expectedBuildSha = value
      continue
    }

    if (arg === "--check-migrations") {
      checkMigrations = true
      continue
    }

    if (arg === "--migration-env") {
      const value = normalizeString(args[i + 1])
      if (!value || value.startsWith("--")) {
        throw new Error("Missing value for --migration-env")
      }
      migrationEnv = value
      i += 1
      continue
    }

    if (arg.startsWith("--migration-env=")) {
      const value = normalizeString(arg.split("=").slice(1).join("="))
      if (!value) {
        throw new Error("Missing value for --migration-env")
      }
      migrationEnv = value
      continue
    }

    if (arg === "--migration-db") {
      const value = normalizeString(args[i + 1])
      if (!value || value.startsWith("--")) {
        throw new Error("Missing value for --migration-db")
      }
      migrationDatabase = value
      i += 1
      continue
    }

    if (arg.startsWith("--migration-db=")) {
      const value = normalizeString(arg.split("=").slice(1).join("="))
      if (!value) {
        throw new Error("Missing value for --migration-db")
      }
      migrationDatabase = value
      continue
    }

    if (arg.startsWith("--")) {
      throw new Error(`Unknown flag: ${arg}`)
    }

    if (hasPositionalBase) {
      throw new Error(`Unexpected extra positional argument: ${arg}`)
    }

    base = arg
    hasPositionalBase = true
  }

  return {
    base: base.endsWith("/") ? base.slice(0, -1) : base,
    strictVersion,
    expectedWorkerVersion,
    expectedBuildSha,
    checkMigrations,
    migrationEnv,
    migrationDatabase,
  }
}

export function validateHealthPayload(payload: unknown): DeployParityValidation {
  const errors: string[] = []
  const record = asRecord(payload)

  if (!record) {
    return { ok: false, errors: ["health payload is not an object"] }
  }

  const requiredStringFields = ["generated_at", "status"]
  for (const field of requiredStringFields) {
    if (typeof record[field] !== "string") {
      errors.push(`health.${field} must be a string`)
    }
  }

  if (typeof record.latest_success_at !== "string" && record.latest_success_at !== null) {
    errors.push("health.latest_success_at must be string|null")
  }

  if (typeof record.hours_since_success !== "number" && record.hours_since_success !== null) {
    errors.push("health.hours_since_success must be number|null")
  }

  if (typeof record.threshold_days !== "number") {
    errors.push("health.threshold_days must be a number")
  }

  if (typeof record.is_stale !== "boolean") {
    errors.push("health.is_stale must be a boolean")
  }

  const status = record.status
  if (status !== "ok" && status !== "stale" && status !== "no_history") {
    errors.push("health.status must be one of: ok|stale|no_history")
  }

  return { ok: errors.length === 0, errors }
}

export function validateAccuracyPayload(payload: unknown): DeployParityValidation {
  const errors: string[] = []
  const record = asRecord(payload)

  if (!record) {
    return { ok: false, errors: ["accuracy payload is not an object"] }
  }

  const rootRequiredFields = [
    "generated_at",
    "as_of",
    "sample_size",
    "total_predictions",
    "minimum_recommended_sample_size",
    "evaluated_count",
    "resolved_count",
    "resolved_predictions",
    "unresolved_count",
    "unresolved_predictions",
    "unresolved_rate",
    "governance_status",
    "overall",
  ]
  for (const field of rootRequiredFields) {
    if (!(field in record)) {
      errors.push(`accuracy missing field: ${field}`)
    }
  }

  if (typeof record.generated_at !== "string") {
    errors.push("accuracy.generated_at must be a string")
  }

  if (typeof record.as_of !== "string") {
    errors.push("accuracy.as_of must be a string")
  }

  if (typeof record.sample_size !== "number") {
    errors.push("accuracy.sample_size must be a number")
  }

  if (typeof record.total_predictions !== "number") {
    errors.push("accuracy.total_predictions must be a number")
  }

  if (typeof record.minimum_recommended_sample_size !== "number") {
    errors.push("accuracy.minimum_recommended_sample_size must be a number")
  }

  if (typeof record.evaluated_count !== "number") {
    errors.push("accuracy.evaluated_count must be a number")
  }

  if (typeof record.resolved_count !== "number") {
    errors.push("accuracy.resolved_count must be a number")
  }

  if (typeof record.resolved_predictions !== "number") {
    errors.push("accuracy.resolved_predictions must be a number")
  }

  if (typeof record.unresolved_count !== "number") {
    errors.push("accuracy.unresolved_count must be a number")
  }

  if (typeof record.unresolved_predictions !== "number") {
    errors.push("accuracy.unresolved_predictions must be a number")
  }

  if (typeof record.unresolved_rate !== "string") {
    errors.push("accuracy.unresolved_rate must be a percent string")
  }

  if (typeof record.governance_status !== "string") {
    errors.push("accuracy.governance_status must be a string")
  } else if (!["PASS", "WARN", "FAIL", "INSUFFICIENT"].includes(record.governance_status)) {
    errors.push("accuracy.governance_status must be one of: PASS|WARN|FAIL|INSUFFICIENT")
  }

  if (typeof record.sample_size === "number" && typeof record.total_predictions === "number" && record.sample_size !== record.total_predictions) {
    errors.push("accuracy.total_predictions must match sample_size")
  }

  if (
    typeof record.resolved_count === "number" &&
    typeof record.resolved_predictions === "number" &&
    record.resolved_count !== record.resolved_predictions
  ) {
    errors.push("accuracy.resolved_predictions must match resolved_count")
  }

  if (
    typeof record.unresolved_count === "number" &&
    typeof record.unresolved_predictions === "number" &&
    record.unresolved_count !== record.unresolved_predictions
  ) {
    errors.push("accuracy.unresolved_predictions must match unresolved_count")
  }

  const overall = asRecord(record.overall)
  if (!overall) {
    errors.push("accuracy.overall must be an object")
  } else {
    const requiredOverallFields = [
      "hit_rate",
      "hit_rate_ci_low",
      "hit_rate_ci_high",
      "count",
      "sample_size_warning",
      "avg_return",
    ]

    for (const field of requiredOverallFields) {
      if (!(field in overall)) {
        errors.push(`accuracy.overall missing field: ${field}`)
      }
    }
  }

  return { ok: errors.length === 0, errors }
}

export function validateVersionPayload(
  payload: unknown,
  options: VersionValidationOptions = {}
): DeployParityValidation {
  const errors: string[] = []
  const record = asRecord(payload)

  if (!record) {
    return { ok: false, errors: ["version payload is not an object"] }
  }

  const requiredStringFields = ["generated_at", "api_contract_version", "worker_version", "build_sha", "build_timestamp"]

  for (const field of requiredStringFields) {
    if (typeof record[field] !== "string" || (record[field] as string).trim().length === 0) {
      errors.push(`version.${field} must be a non-empty string`)
    }
  }

  if (typeof record.generated_at === "string" && Number.isNaN(Date.parse(record.generated_at))) {
    errors.push("version.generated_at must be a valid ISO timestamp")
  }

  if (typeof record.build_timestamp === "string" && Number.isNaN(Date.parse(record.build_timestamp))) {
    errors.push("version.build_timestamp must be a valid ISO timestamp")
  }

  const normalizedWorkerVersion = normalizeString(record.worker_version)
  const normalizedBuildSha = normalizeString(record.build_sha)

  const expectedWorkerVersion = normalizeString(options.expectedWorkerVersion)
  if (expectedWorkerVersion && normalizedWorkerVersion && normalizedWorkerVersion !== expectedWorkerVersion) {
    errors.push(
      `version.worker_version mismatch: expected '${expectedWorkerVersion}' but got '${normalizedWorkerVersion}'`
    )
  }

  const expectedBuildSha = normalizeString(options.expectedBuildSha)
  if (expectedBuildSha && normalizedBuildSha && normalizedBuildSha.toLowerCase() !== expectedBuildSha.toLowerCase()) {
    errors.push(`version.build_sha mismatch: expected '${expectedBuildSha}' but got '${normalizedBuildSha}'`)
  }

  if (options.strictVersion) {
    const apiContractVersion = normalizeString(record.api_contract_version)
    if (apiContractVersion && isPlaceholderValue(apiContractVersion)) {
      errors.push("version.api_contract_version cannot be a placeholder in strict mode")
    }

    if (normalizedWorkerVersion && isPlaceholderValue(normalizedWorkerVersion)) {
      errors.push("version.worker_version cannot be a placeholder in strict mode")
    }

    if (normalizedBuildSha) {
      if (isPlaceholderValue(normalizedBuildSha)) {
        errors.push("version.build_sha cannot be a placeholder in strict mode")
      }

      if (/^0+$/i.test(normalizedBuildSha)) {
        errors.push("version.build_sha cannot be all zeros in strict mode")
      }

      if (!isLikelyGitSha(normalizedBuildSha)) {
        errors.push("version.build_sha must look like a git SHA in strict mode")
      }
    }

    const buildTimestamp = normalizeString(record.build_timestamp)
    if (buildTimestamp) {
      const parsedTimestamp = Date.parse(buildTimestamp)
      if (!Number.isNaN(parsedTimestamp) && parsedTimestamp < STRICT_MIN_BUILD_TIMESTAMP_MS) {
        errors.push("version.build_timestamp is implausibly old in strict mode")
      }
    }
  }

  return { ok: errors.length === 0, errors }
}

function normalizeName(value: string): string {
  return value.trim().toLowerCase()
}

function normalizeNameSet(values: string[]): Set<string> {
  const normalized = new Set<string>()
  for (const value of values) {
    const name = normalizeName(value)
    if (name.length > 0) {
      normalized.add(name)
    }
  }
  return normalized
}

export function validateMigrationMarkers(
  snapshot: MigrationMarkerSnapshot,
  requirements: MigrationMarkerRequirements = REQUIRED_MIGRATION_MARKERS
): DeployParityValidation {
  const errors: string[] = []

  const observedTables = normalizeNameSet(snapshot.tables)
  const observedIndexes = normalizeNameSet(snapshot.indexes)
  const observedColumnsByTable = new Map<string, Set<string>>()

  for (const [tableName, columns] of Object.entries(snapshot.columnsByTable)) {
    observedColumnsByTable.set(normalizeName(tableName), normalizeNameSet(columns))
  }

  for (const requiredTable of requirements.tables) {
    if (!observedTables.has(normalizeName(requiredTable))) {
      errors.push(`migration marker missing table: ${requiredTable}`)
    }
  }

  for (const requiredIndex of requirements.indexes) {
    if (!observedIndexes.has(normalizeName(requiredIndex))) {
      errors.push(`migration marker missing index: ${requiredIndex}`)
    }
  }

  for (const [tableName, requiredColumns] of Object.entries(requirements.columnsByTable)) {
    const observedColumns = observedColumnsByTable.get(normalizeName(tableName)) ?? new Set<string>()
    for (const requiredColumn of requiredColumns) {
      if (!observedColumns.has(normalizeName(requiredColumn))) {
        errors.push(`migration marker missing column: ${tableName}.${requiredColumn}`)
      }
    }
  }

  return { ok: errors.length === 0, errors }
}

export interface PredictionKey {
  signal_date: string
  theme_id: string
}

export function findDuplicatePredictionKeys(predictions: PredictionKey[]): string[] {
  const seen = new Set<string>()
  const duplicates = new Set<string>()

  for (const row of predictions) {
    const key = `${row.signal_date}::${row.theme_id}`
    if (seen.has(key)) {
      duplicates.add(key)
      continue
    }
    seen.add(key)
  }

  return Array.from(duplicates).sort()
}

export function parsePredictionRows(payload: unknown): PredictionKey[] {
  const record = asRecord(payload)
  if (!record) return []

  const rows = record.predictions
  if (!Array.isArray(rows)) return []

  return rows
    .map((row) => asRecord(row))
    .filter((row): row is JsonObject => Boolean(row))
    .filter((row) => typeof row.signal_date === "string" && typeof row.theme_id === "string")
    .map((row) => ({
      signal_date: row.signal_date as string,
      theme_id: row.theme_id as string,
    }))
}
