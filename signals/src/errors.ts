/**
 * Structured error types for the PXI-Signals pipeline.
 *
 * These errors provide:
 * - Machine-readable error codes for programmatic handling
 * - Structured context for debugging and logging
 * - Consistent error format across the application
 */

// ─────────────────────────────────────────────────────────────────────────────
// Base Error Class
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Base error class for all PXI-Signals errors.
 * Provides structured error code and optional context.
 */
export class PXISignalsError extends Error {
  /** Machine-readable error code */
  public readonly code: string

  /** Additional context for debugging (no sensitive data!) */
  public readonly context?: Record<string, unknown>

  /** ISO timestamp when the error occurred */
  public readonly timestamp: string

  constructor(
    message: string,
    code: string,
    context?: Record<string, unknown>
  ) {
    super(message)
    this.name = "PXISignalsError"
    this.code = code
    this.context = context
    this.timestamp = new Date().toISOString()

    // Maintains proper stack trace in V8
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, this.constructor)
    }
  }

  /**
   * Serializes the error for logging or API responses.
   */
  toJSON(): Record<string, unknown> {
    return {
      name: this.name,
      code: this.code,
      message: this.message,
      context: this.context,
      timestamp: this.timestamp,
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Specific Error Types
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Errors from Reddit API interactions.
 */
export class RedditAPIError extends PXISignalsError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "REDDIT_API_ERROR", context)
    this.name = "RedditAPIError"
  }
}

/**
 * Errors when there's insufficient data to produce a valid report.
 */
export class InsufficientDataError extends PXISignalsError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "INSUFFICIENT_DATA", context)
    this.name = "InsufficientDataError"
  }
}

/**
 * Errors from R2/KV/D1 storage operations.
 */
export class StorageError extends PXISignalsError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "STORAGE_ERROR", context)
    this.name = "StorageError"
  }
}

/**
 * Errors from authentication/authorization checks.
 */
export class AuthenticationError extends PXISignalsError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "AUTHENTICATION_ERROR", context)
    this.name = "AuthenticationError"
  }
}

/**
 * Errors from rate limiting.
 */
export class RateLimitError extends PXISignalsError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "RATE_LIMIT_ERROR", context)
    this.name = "RateLimitError"
  }
}

/**
 * Errors from input validation.
 */
export class ValidationError extends PXISignalsError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "VALIDATION_ERROR", context)
    this.name = "ValidationError"
  }
}

/**
 * Errors from the analysis pipeline.
 */
export class PipelineError extends PXISignalsError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "PIPELINE_ERROR", context)
    this.name = "PipelineError"
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Error Helpers
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Converts unknown error types to a consistent Error object.
 * Useful in catch blocks with `unknown` error type.
 */
export function toError(err: unknown): Error {
  if (err instanceof Error) {
    return err
  }
  if (typeof err === "string") {
    return new Error(err)
  }
  return new Error(String(err))
}

/**
 * Extracts a safe error message for logging or user display.
 * Never exposes stack traces or internal details.
 */
export function safeErrorMessage(err: unknown): string {
  if (err instanceof PXISignalsError) {
    return err.message
  }
  if (err instanceof Error) {
    return err.message
  }
  if (typeof err === "string") {
    return err
  }
  return "An unknown error occurred"
}

/**
 * Type guard to check if error is a PXISignalsError.
 */
export function isPXISignalsError(err: unknown): err is PXISignalsError {
  return err instanceof PXISignalsError
}
