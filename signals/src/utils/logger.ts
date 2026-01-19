/**
 * Structured logging utilities for the PXI-Signals pipeline.
 *
 * All log output is JSON-formatted for easy parsing in Cloudflare's
 * logging dashboard and external log aggregators.
 */

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

export type LogLevel = "debug" | "info" | "warn" | "error"

export interface LogContext {
  /** Optional run ID for correlating logs across a pipeline execution */
  runId?: string
  /** Component name (e.g., "reddit_client", "metrics", "scheduled") */
  component?: string
  /** Additional structured data */
  [key: string]: unknown
}

interface LogEntry {
  timestamp: string
  level: LogLevel
  message: string
  [key: string]: unknown
}

// ─────────────────────────────────────────────────────────────────────────────
// Core Logger
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Logs a structured message at the specified level.
 *
 * @param level - Log severity level
 * @param message - Human-readable log message
 * @param context - Optional structured context data
 */
export function log(level: LogLevel, message: string, context?: LogContext): void {
  const timestamp = new Date().toISOString()
  const entry: LogEntry = {
    timestamp,
    level,
    message,
    ...context,
  }

  const json = JSON.stringify(entry)

  switch (level) {
    case "error":
      console.error(json)
      break
    case "warn":
      console.warn(json)
      break
    case "debug":
      // Only output debug logs if explicitly enabled
      if (process.env.DEBUG === "true") {
        console.log(json)
      }
      break
    default:
      console.log(json)
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Convenience Functions
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Logs an info-level message.
 */
export function logInfo(message: string, context?: LogContext): void {
  log("info", message, context)
}

/**
 * Logs a warning-level message.
 */
export function logWarn(message: string, context?: LogContext): void {
  log("warn", message, context)
}

/**
 * Logs an error-level message.
 */
export function logError(message: string, context?: LogContext): void {
  log("error", message, context)
}

/**
 * Logs a debug-level message (only when DEBUG=true).
 */
export function logDebug(message: string, context?: LogContext): void {
  log("debug", message, context)
}

// ─────────────────────────────────────────────────────────────────────────────
// Error Logging
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Logs an error with full context, including stack trace if available.
 * Extracts structured information from PXISignalsError instances.
 *
 * @param message - Description of what failed
 * @param error - The error that was caught
 * @param context - Additional context
 */
export function logErrorWithStack(
  message: string,
  error: unknown,
  context?: LogContext
): void {
  const errorInfo: Record<string, unknown> = {}

  if (error instanceof Error) {
    errorInfo.errorMessage = error.message
    errorInfo.errorName = error.name
    if (error.stack) {
      // Only include first 5 lines of stack to avoid log bloat
      errorInfo.stack = error.stack.split("\n").slice(0, 5).join("\n")
    }
    // Check for PXISignalsError properties
    if ("code" in error && typeof error.code === "string") {
      errorInfo.errorCode = error.code
    }
    if ("context" in error && typeof error.context === "object") {
      errorInfo.errorContext = error.context
    }
  } else {
    errorInfo.errorMessage = String(error)
  }

  log("error", message, { ...context, ...errorInfo })
}

// ─────────────────────────────────────────────────────────────────────────────
// Performance Logging
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Logs the duration of an operation.
 *
 * @param operation - Name of the operation being timed
 * @param startTime - Start time from Date.now()
 * @param context - Additional context
 */
export function logDuration(
  operation: string,
  startTime: number,
  context?: LogContext
): void {
  const durationMs = Date.now() - startTime
  logInfo(`${operation} completed`, {
    ...context,
    durationMs,
  })
}
