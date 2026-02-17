import { describe, it, expect } from "vitest"
import { getConfig, DEFAULTS } from "../../src/config"
import { createMockEnv } from "../fixtures/mock_env"

describe("config", () => {
  describe("DEFAULTS", () => {
    it("has all expected default values", () => {
      expect(DEFAULTS.lookbackDays).toBe(7)
      expect(DEFAULTS.baselineDays).toBe(30)
      expect(DEFAULTS.topN).toBe(10)
      expect(DEFAULTS.maxPostsPerSubreddit).toBe(150)
      expect(DEFAULTS.maxCommentsPerPost).toBe(50)
      expect(DEFAULTS.eps).toBe(1e-6)
    })

    it("default values are reasonable", () => {
      expect(DEFAULTS.lookbackDays).toBeGreaterThan(0)
      expect(DEFAULTS.baselineDays).toBeGreaterThan(DEFAULTS.lookbackDays)
      expect(DEFAULTS.topN).toBeGreaterThan(0)
      expect(DEFAULTS.maxPostsPerSubreddit).toBeGreaterThan(0)
      expect(DEFAULTS.maxCommentsPerPost).toBeGreaterThan(0)
      expect(DEFAULTS.eps).toBeGreaterThan(0)
      expect(DEFAULTS.eps).toBeLessThan(0.001)
    })
  })

  describe("getConfig", () => {
    describe("publicBasePath", () => {
      it("uses PUBLIC_BASE_PATH from env", () => {
        const env = createMockEnv({ PUBLIC_BASE_PATH: "/custom" })
        const config = getConfig(env)
        expect(config.publicBasePath).toBe("/custom")
      })

      it("defaults to /signals when not set", () => {
        const env = createMockEnv({ PUBLIC_BASE_PATH: "" as any })
        const config = getConfig(env)
        expect(config.publicBasePath).toBe("/signals")
      })
    })

    describe("lookbackDays", () => {
      it("uses DEFAULT_LOOKBACK_DAYS from env", () => {
        const env = createMockEnv({ DEFAULT_LOOKBACK_DAYS: 14 })
        const config = getConfig(env)
        expect(config.lookbackDays).toBe(14)
      })

      it("defaults to 7 when not set", () => {
        const env = createMockEnv({ DEFAULT_LOOKBACK_DAYS: 0 as any })
        const config = getConfig(env)
        expect(config.lookbackDays).toBe(DEFAULTS.lookbackDays)
      })

      it("converts string to number", () => {
        const env = createMockEnv({ DEFAULT_LOOKBACK_DAYS: "14" as any })
        const config = getConfig(env)
        expect(config.lookbackDays).toBe(14)
      })
    })

    describe("baselineDays", () => {
      it("uses DEFAULT_BASELINE_DAYS from env", () => {
        const env = createMockEnv({ DEFAULT_BASELINE_DAYS: 60 })
        const config = getConfig(env)
        expect(config.baselineDays).toBe(60)
      })

      it("defaults to 30 when not set", () => {
        const env = createMockEnv({ DEFAULT_BASELINE_DAYS: 0 as any })
        const config = getConfig(env)
        expect(config.baselineDays).toBe(DEFAULTS.baselineDays)
      })
    })

    describe("topN", () => {
      it("uses DEFAULT_TOP_N from env", () => {
        const env = createMockEnv({ DEFAULT_TOP_N: 5 })
        const config = getConfig(env)
        expect(config.topN).toBe(5)
      })

      it("defaults to 10 when not set", () => {
        const env = createMockEnv({ DEFAULT_TOP_N: 0 as any })
        const config = getConfig(env)
        expect(config.topN).toBe(DEFAULTS.topN)
      })
    })

    describe("enableComments", () => {
      it("returns true when ENABLE_COMMENTS is 1", () => {
        const env = createMockEnv({ ENABLE_COMMENTS: 1 })
        const config = getConfig(env)
        expect(config.enableComments).toBe(true)
      })

      it("returns false when ENABLE_COMMENTS is 0", () => {
        const env = createMockEnv({ ENABLE_COMMENTS: 0 })
        const config = getConfig(env)
        expect(config.enableComments).toBe(false)
      })

      it("returns false for other values", () => {
        const env = createMockEnv({ ENABLE_COMMENTS: 2 as any })
        const config = getConfig(env)
        expect(config.enableComments).toBe(false)
      })
    })

    describe("enableRss", () => {
      it("returns true when ENABLE_RSS is 1", () => {
        const env = createMockEnv({ ENABLE_RSS: 1 })
        const config = getConfig(env)
        expect(config.enableRss).toBe(true)
      })

      it("returns false when ENABLE_RSS is 0", () => {
        const env = createMockEnv({ ENABLE_RSS: 0 })
        const config = getConfig(env)
        expect(config.enableRss).toBe(false)
      })
    })

    describe("priceProvider", () => {
      it("uses PRICE_PROVIDER from env", () => {
        const env = createMockEnv({ PRICE_PROVIDER: "yahoo" })
        const config = getConfig(env)
        expect(config.priceProvider).toBe("yahoo")
      })

      it("defaults to 'none' when not set", () => {
        const env = createMockEnv({ PRICE_PROVIDER: "" })
        const config = getConfig(env)
        expect(config.priceProvider).toBe("none")
      })
    })

    describe("deploy metadata", () => {
      it("normalizes build metadata when provided", () => {
        const env = createMockEnv({
          BUILD_SHA: "abc123",
          BUILD_TIMESTAMP: "2026-02-17T09:00:00Z",
          WORKER_VERSION: "signals-prod-20260217-0900",
        })

        const config = getConfig(env)

        expect(config.buildSha).toBe("abc123")
        expect(config.buildTimestamp).toBe("2026-02-17T09:00:00.000Z")
        expect(config.workerVersion).toBe("signals-prod-20260217-0900")
      })

      it("returns null metadata fields when values are invalid", () => {
        const env = createMockEnv({
          BUILD_SHA: "   " as any,
          BUILD_TIMESTAMP: "not-a-date" as any,
          WORKER_VERSION: "" as any,
        })

        const config = getConfig(env)

        expect(config.buildSha).toBeNull()
        expect(config.buildTimestamp).toBeNull()
        expect(config.workerVersion).toBeNull()
      })
    })

    describe("static config values", () => {
      it("always uses DEFAULTS.maxPostsPerSubreddit", () => {
        const env = createMockEnv()
        const config = getConfig(env)
        expect(config.maxPostsPerSubreddit).toBe(DEFAULTS.maxPostsPerSubreddit)
      })

      it("always uses DEFAULTS.maxCommentsPerPost", () => {
        const env = createMockEnv()
        const config = getConfig(env)
        expect(config.maxCommentsPerPost).toBe(DEFAULTS.maxCommentsPerPost)
      })

      it("always uses DEFAULTS.eps", () => {
        const env = createMockEnv()
        const config = getConfig(env)
        expect(config.eps).toBe(DEFAULTS.eps)
      })
    })

    describe("config immutability", () => {
      it("returns new object on each call", () => {
        const env = createMockEnv()
        const config1 = getConfig(env)
        const config2 = getConfig(env)
        expect(config1).not.toBe(config2)
      })

      it("changes to returned config do not affect future calls", () => {
        const env = createMockEnv({ DEFAULT_LOOKBACK_DAYS: 7 })
        const config1 = getConfig(env) as any
        config1.lookbackDays = 999

        const config2 = getConfig(env)
        expect(config2.lookbackDays).toBe(7)
      })
    })
  })
})
