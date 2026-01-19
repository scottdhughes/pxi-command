import type { Env } from "./config"
import { handleRequest } from "./routes"
import { runPipeline } from "./scheduled"
import { insertRun } from "./db"
import { nowUtcIso } from "./utils/time"
import { logErrorWithStack } from "./utils/logger"
import { toError } from "./errors"
import { ulid } from "ulidx"

// ─────────────────────────────────────────────────────────────────────────────
// US Monday Market Holidays (NYSE/NASDAQ closed)
// These are the standard Monday holidays when markets are closed.
// Update this list annually.
// ─────────────────────────────────────────────────────────────────────────────

const US_MONDAY_HOLIDAYS = new Set([
  // 2025
  "2025-01-20", // MLK Day
  "2025-02-17", // Presidents Day
  "2025-05-26", // Memorial Day
  "2025-09-01", // Labor Day
  // 2026
  "2026-01-19", // MLK Day
  "2026-02-16", // Presidents Day
  "2026-05-25", // Memorial Day
  "2026-09-07", // Labor Day
  // 2027
  "2027-01-18", // MLK Day
  "2027-02-15", // Presidents Day
  "2027-05-31", // Memorial Day
  "2027-09-06", // Labor Day
])

/**
 * Determines if the scheduled pipeline should run based on the current day.
 *
 * Logic:
 * - Monday: Run if NOT a US market holiday
 * - Tuesday: Run only if the previous Monday WAS a US market holiday
 * - Other days: Do not run (cron shouldn't trigger, but guard anyway)
 *
 * @returns { shouldRun: boolean, reason: string }
 */
function shouldRunScheduledPipeline(): { shouldRun: boolean; reason: string } {
  const now = new Date()
  const dayOfWeek = now.getUTCDay() // 0=Sun, 1=Mon, 2=Tue, ...
  const todayIso = now.toISOString().slice(0, 10) // YYYY-MM-DD

  // Calculate yesterday (for Tuesday to check Monday)
  const yesterday = new Date(now)
  yesterday.setUTCDate(now.getUTCDate() - 1)
  const yesterdayIso = yesterday.toISOString().slice(0, 10)

  if (dayOfWeek === 1) {
    // Monday
    if (US_MONDAY_HOLIDAYS.has(todayIso)) {
      return {
        shouldRun: false,
        reason: `Skipping: Monday ${todayIso} is a US market holiday. Will run Tuesday.`,
      }
    }
    return {
      shouldRun: true,
      reason: `Running: Monday ${todayIso} is a trading day.`,
    }
  }

  if (dayOfWeek === 2) {
    // Tuesday
    if (US_MONDAY_HOLIDAYS.has(yesterdayIso)) {
      return {
        shouldRun: true,
        reason: `Running: Tuesday ${todayIso} (Monday ${yesterdayIso} was a holiday).`,
      }
    }
    return {
      shouldRun: false,
      reason: `Skipping: Tuesday ${todayIso} - Monday was not a holiday.`,
    }
  }

  // Should not happen given our cron config, but guard anyway
  return {
    shouldRun: false,
    reason: `Skipping: Day ${dayOfWeek} is not a scheduled run day.`,
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Worker Export
// ─────────────────────────────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env) {
    return handleRequest(request, env)
  },

  async scheduled(_event: ScheduledEvent, env: Env, _ctx: ExecutionContext) {
    const runId = `${new Date().toISOString().slice(0, 10).replace(/-/g, "")}-${ulid()}`

    // Check if we should run based on day/holiday logic
    const { shouldRun, reason } = shouldRunScheduledPipeline()
    console.log(`[pxi-signals] ${reason}`)

    if (!shouldRun) {
      // Log skip decision but don't create a run record
      console.log(`[pxi-signals] Run skipped for ${runId}`)
      return
    }

    try {
      console.log(`[pxi-signals] Starting pipeline run: ${runId}`)
      await runPipeline(env)
      console.log(`[pxi-signals] Pipeline completed: ${runId}`)
    } catch (err: unknown) {
      const error = toError(err)

      logErrorWithStack("Scheduled pipeline failed", error, {
        runId,
        component: "scheduled",
      })

      await insertRun(env, {
        id: runId,
        created_at_utc: nowUtcIso(),
        lookback_days: Number(env.DEFAULT_LOOKBACK_DAYS) || 7,
        baseline_days: Number(env.DEFAULT_BASELINE_DAYS) || 30,
        status: "error",
        summary_json: null,
        report_html_key: "",
        results_json_key: "",
        raw_json_key: null,
        error_message: error.message || "scheduled_run_failed",
      })
    }
  },
}
