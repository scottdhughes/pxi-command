/**
 * Mock environment for testing Cloudflare Workers code.
 */

import type { Env } from "../../src/config"

/**
 * Creates a mock D1 database for testing.
 */
export function createMockD1(): D1Database {
  const results: unknown[] = []
  return {
    prepare: () => ({
      bind: () => ({
        run: async () => ({ success: true, results }),
        all: async () => ({ success: true, results }),
        first: async () => null,
      }),
      run: async () => ({ success: true, results }),
      all: async () => ({ success: true, results }),
      first: async () => null,
    }),
    batch: async () => [],
    exec: async () => ({ count: 0, duration: 0 }),
    dump: async () => new ArrayBuffer(0),
  } as unknown as D1Database
}

/**
 * Creates a mock R2 bucket for testing.
 */
export function createMockR2Bucket(): R2Bucket {
  const storage = new Map<string, string>()
  return {
    put: async (key: string, value: string | ReadableStream | ArrayBuffer) => {
      storage.set(key, typeof value === "string" ? value : "")
      return {} as R2Object
    },
    get: async (key: string) => {
      const value = storage.get(key)
      if (!value) return null
      return {
        text: async () => value,
        json: async () => JSON.parse(value),
        body: null,
      } as R2ObjectBody
    },
    delete: async () => {},
    list: async () => ({ objects: [], truncated: false, cursor: undefined }),
    head: async () => null,
    createMultipartUpload: async () => ({} as R2MultipartUpload),
    resumeMultipartUpload: () => ({} as R2MultipartUpload),
  } as unknown as R2Bucket
}

/**
 * Creates a mock KV namespace for testing.
 */
export function createMockKV(): KVNamespace {
  const storage = new Map<string, string>()
  return {
    get: async (key: string) => storage.get(key) ?? null,
    put: async (key: string, value: string) => {
      storage.set(key, value)
    },
    delete: async (key: string) => {
      storage.delete(key)
    },
    list: async () => ({ keys: [], list_complete: true, cursor: undefined }),
    getWithMetadata: async () => ({ value: null, metadata: null }),
  } as unknown as KVNamespace
}

/**
 * Creates a complete mock environment for testing.
 */
export function createMockEnv(overrides: Partial<Env> = {}): Env {
  return {
    SIGNALS_DB: createMockD1(),
    SIGNALS_BUCKET: createMockR2Bucket(),
    SIGNALS_KV: createMockKV(),
    PUBLIC_BASE_PATH: "/signals",
    DEFAULT_LOOKBACK_DAYS: 7,
    DEFAULT_BASELINE_DAYS: 30,
    DEFAULT_TOP_N: 10,
    ENABLE_COMMENTS: 0,
    ENABLE_RSS: 0,
    PRICE_PROVIDER: "none",
    ADMIN_RUN_TOKEN: "test-admin-token",
    ...overrides,
  }
}
