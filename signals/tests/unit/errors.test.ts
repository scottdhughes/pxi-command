import { describe, it, expect } from "vitest"
import {
  PXISignalsError,
  RedditAPIError,
  InsufficientDataError,
  StorageError,
  AuthenticationError,
  RateLimitError,
  ValidationError,
  PipelineError,
  toError,
  safeErrorMessage,
  isPXISignalsError,
} from "../../src/errors"

describe("PXISignalsError", () => {
  describe("constructor", () => {
    it("sets message correctly", () => {
      const error = new PXISignalsError("Test message", "TEST_CODE")
      expect(error.message).toBe("Test message")
    })

    it("sets code correctly", () => {
      const error = new PXISignalsError("Test message", "TEST_CODE")
      expect(error.code).toBe("TEST_CODE")
    })

    it("sets context when provided", () => {
      const context = { key: "value", num: 42 }
      const error = new PXISignalsError("Test", "CODE", context)
      expect(error.context).toEqual(context)
    })

    it("context is undefined when not provided", () => {
      const error = new PXISignalsError("Test", "CODE")
      expect(error.context).toBeUndefined()
    })

    it("sets timestamp as ISO string", () => {
      const before = new Date().toISOString()
      const error = new PXISignalsError("Test", "CODE")
      const after = new Date().toISOString()

      expect(error.timestamp).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
      expect(error.timestamp >= before).toBe(true)
      expect(error.timestamp <= after).toBe(true)
    })

    it("sets name to PXISignalsError", () => {
      const error = new PXISignalsError("Test", "CODE")
      expect(error.name).toBe("PXISignalsError")
    })

    it("is instance of Error", () => {
      const error = new PXISignalsError("Test", "CODE")
      expect(error).toBeInstanceOf(Error)
    })

    it("has stack trace", () => {
      const error = new PXISignalsError("Test", "CODE")
      expect(error.stack).toBeDefined()
      expect(error.stack).toContain("PXISignalsError")
    })
  })

  describe("toJSON", () => {
    it("serializes all fields", () => {
      const error = new PXISignalsError("Test message", "TEST_CODE", { extra: "data" })
      const json = error.toJSON()

      expect(json.name).toBe("PXISignalsError")
      expect(json.code).toBe("TEST_CODE")
      expect(json.message).toBe("Test message")
      expect(json.context).toEqual({ extra: "data" })
      expect(json.timestamp).toBeDefined()
    })

    it("produces valid JSON", () => {
      const error = new PXISignalsError("Test", "CODE", { key: "value" })
      const jsonString = JSON.stringify(error.toJSON())
      const parsed = JSON.parse(jsonString)

      expect(parsed.name).toBe("PXISignalsError")
      expect(parsed.code).toBe("CODE")
    })
  })
})

describe("Specific error types", () => {
  describe("RedditAPIError", () => {
    it("has correct name and code", () => {
      const error = new RedditAPIError("API failed")
      expect(error.name).toBe("RedditAPIError")
      expect(error.code).toBe("REDDIT_API_ERROR")
    })

    it("is instance of PXISignalsError", () => {
      const error = new RedditAPIError("API failed")
      expect(error).toBeInstanceOf(PXISignalsError)
    })

    it("accepts context", () => {
      const error = new RedditAPIError("API failed", { status: 429 })
      expect(error.context).toEqual({ status: 429 })
    })
  })

  describe("InsufficientDataError", () => {
    it("has correct name and code", () => {
      const error = new InsufficientDataError("Not enough data")
      expect(error.name).toBe("InsufficientDataError")
      expect(error.code).toBe("INSUFFICIENT_DATA")
    })
  })

  describe("StorageError", () => {
    it("has correct name and code", () => {
      const error = new StorageError("Storage failed")
      expect(error.name).toBe("StorageError")
      expect(error.code).toBe("STORAGE_ERROR")
    })
  })

  describe("AuthenticationError", () => {
    it("has correct name and code", () => {
      const error = new AuthenticationError("Auth failed")
      expect(error.name).toBe("AuthenticationError")
      expect(error.code).toBe("AUTHENTICATION_ERROR")
    })
  })

  describe("RateLimitError", () => {
    it("has correct name and code", () => {
      const error = new RateLimitError("Rate limited")
      expect(error.name).toBe("RateLimitError")
      expect(error.code).toBe("RATE_LIMIT_ERROR")
    })
  })

  describe("ValidationError", () => {
    it("has correct name and code", () => {
      const error = new ValidationError("Invalid input")
      expect(error.name).toBe("ValidationError")
      expect(error.code).toBe("VALIDATION_ERROR")
    })
  })

  describe("PipelineError", () => {
    it("has correct name and code", () => {
      const error = new PipelineError("Pipeline failed")
      expect(error.name).toBe("PipelineError")
      expect(error.code).toBe("PIPELINE_ERROR")
    })
  })
})

describe("toError", () => {
  it("returns Error unchanged", () => {
    const original = new Error("Original")
    const result = toError(original)
    expect(result).toBe(original)
  })

  it("returns PXISignalsError unchanged", () => {
    const original = new PXISignalsError("Test", "CODE")
    const result = toError(original)
    expect(result).toBe(original)
  })

  it("converts string to Error", () => {
    const result = toError("string error")
    expect(result).toBeInstanceOf(Error)
    expect(result.message).toBe("string error")
  })

  it("converts number to Error", () => {
    const result = toError(404)
    expect(result).toBeInstanceOf(Error)
    expect(result.message).toBe("404")
  })

  it("converts null to Error", () => {
    const result = toError(null)
    expect(result).toBeInstanceOf(Error)
    expect(result.message).toBe("null")
  })

  it("converts undefined to Error", () => {
    const result = toError(undefined)
    expect(result).toBeInstanceOf(Error)
    expect(result.message).toBe("undefined")
  })

  it("converts object to Error", () => {
    const result = toError({ foo: "bar" })
    expect(result).toBeInstanceOf(Error)
    expect(result.message).toBe("[object Object]")
  })
})

describe("safeErrorMessage", () => {
  it("extracts message from PXISignalsError", () => {
    const error = new PXISignalsError("Test message", "CODE")
    expect(safeErrorMessage(error)).toBe("Test message")
  })

  it("extracts message from Error", () => {
    const error = new Error("Error message")
    expect(safeErrorMessage(error)).toBe("Error message")
  })

  it("returns string as-is", () => {
    expect(safeErrorMessage("string error")).toBe("string error")
  })

  it("returns default for unknown types", () => {
    expect(safeErrorMessage(null)).toBe("An unknown error occurred")
    expect(safeErrorMessage(undefined)).toBe("An unknown error occurred")
    expect(safeErrorMessage(42)).toBe("An unknown error occurred")
    expect(safeErrorMessage({})).toBe("An unknown error occurred")
  })

  it("does not expose stack traces", () => {
    const error = new Error("Message")
    const result = safeErrorMessage(error)
    expect(result).not.toContain("at ")
    expect(result).not.toContain("Error:")
  })
})

describe("isPXISignalsError", () => {
  it("returns true for PXISignalsError", () => {
    const error = new PXISignalsError("Test", "CODE")
    expect(isPXISignalsError(error)).toBe(true)
  })

  it("returns true for subclasses", () => {
    expect(isPXISignalsError(new RedditAPIError("Test"))).toBe(true)
    expect(isPXISignalsError(new StorageError("Test"))).toBe(true)
    expect(isPXISignalsError(new PipelineError("Test"))).toBe(true)
  })

  it("returns false for regular Error", () => {
    const error = new Error("Regular error")
    expect(isPXISignalsError(error)).toBe(false)
  })

  it("returns false for non-errors", () => {
    expect(isPXISignalsError(null)).toBe(false)
    expect(isPXISignalsError(undefined)).toBe(false)
    expect(isPXISignalsError("string")).toBe(false)
    expect(isPXISignalsError(42)).toBe(false)
    expect(isPXISignalsError({})).toBe(false)
  })

  it("works as type guard", () => {
    const error: unknown = new PXISignalsError("Test", "CODE")
    if (isPXISignalsError(error)) {
      // TypeScript should now know error has .code
      expect(error.code).toBe("CODE")
    }
  })
})
