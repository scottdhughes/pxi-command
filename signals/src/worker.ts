import type { Env } from "./config"
import { handleRequest } from "./routes"
import { runPipeline, isPipelineLockError } from "./scheduled"
import { getLatestSuccessfulRun, insertRun } from "./db"
import { nowUtcIso } from "./utils/time"
import { logErrorWithStack, logWarn } from "./utils/logger"
import { toError } from "./errors"
import { getNyseHolidaySet, isNyseHolidayDate, toIsoDateUtc } from "./utils/calendar"
import { ulid } from "ulidx"

export { getNyseHolidaySet }
export const isNyseHoliday = isNyseHolidayDate

const DAY_MS = 24 * 60 * 60 * 1000
const CATCHUP_STALE_DAYS = 6

interface ScheduleDecisionInput {
  scheduledAt: Date
  lastSuccessfulRunAtUtc: string | null
}

function getDaysSinceLastSuccess(scheduledAt: Date, lastSuccessfulRunAtUtc: string | null): number {
  if (!lastSuccessfulRunAtUtc) return Number.POSITIVE_INFINITY
  const last = new Date(lastSuccessfulRunAtUtc)
  if (Number.isNaN(last.getTime())) return Number.POSITIVE_INFINITY
  return (scheduledAt.getTime() - last.getTime()) / DAY_MS
}

/**
 * Determines if the scheduled pipeline should run based on the current day.
 *
 * Logic:
 * - Monday: Run if NOT a US market holiday
 * - Tuesday: Run only if the previous Monday WAS a US market holiday
 * - Tuesday/Wednesday: Catch up automatically if last successful run is stale
 * - Other days: Do not run (cron shouldn't trigger, but guard anyway)
 *
 * @returns { shouldRun: boolean, reason: string }
 */
export function shouldRunScheduledPipeline(input: ScheduleDecisionInput): { shouldRun: boolean; reason: string } {
  const { scheduledAt, lastSuccessfulRunAtUtc } = input
  const dayOfWeek = scheduledAt.getUTCDay() // 0=Sun, 1=Mon, 2=Tue, ...
  const todayIso = toIsoDateUtc(scheduledAt)
  const daysSinceLastSuccess = getDaysSinceLastSuccess(scheduledAt, lastSuccessfulRunAtUtc)
  const staleForCatchup = !Number.isFinite(daysSinceLastSuccess) || daysSinceLastSuccess >= CATCHUP_STALE_DAYS

  // Calculate yesterday (for Tuesday to check Monday)
  const yesterday = new Date(scheduledAt)
  yesterday.setUTCDate(scheduledAt.getUTCDate() - 1)
  const yesterdayIso = toIsoDateUtc(yesterday)

  if (dayOfWeek === 1) {
    // Monday
    if (isNyseHoliday(scheduledAt)) {
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
    if (isNyseHoliday(yesterday)) {
      return {
        shouldRun: true,
        reason: `Running: Tuesday ${todayIso} (Monday ${yesterdayIso} was a holiday).`,
      }
    }

    if (staleForCatchup) {
      const staleReason = Number.isFinite(daysSinceLastSuccess)
        ? `${daysSinceLastSuccess.toFixed(1)} days since last successful run`
        : "no previous successful run found"
      return {
        shouldRun: true,
        reason: `Running: Tuesday ${todayIso} catch-up (${staleReason}).`,
      }
    }

    return {
      shouldRun: false,
      reason: `Skipping: Tuesday ${todayIso} - Monday was not a holiday.`,
    }
  }

  if (dayOfWeek === 3) {
    // Wednesday catch-up fallback in case earlier scheduled events were missed.
    if (staleForCatchup) {
      const staleReason = Number.isFinite(daysSinceLastSuccess)
        ? `${daysSinceLastSuccess.toFixed(1)} days since last successful run`
        : "no previous successful run found"
      return {
        shouldRun: true,
        reason: `Running: Wednesday ${todayIso} stale-data catch-up (${staleReason}).`,
      }
    }

    return {
      shouldRun: false,
      reason: `Skipping: Wednesday ${todayIso} - latest run is fresh.`,
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

  async scheduled(event: ScheduledEvent, env: Env, _ctx: ExecutionContext) {
    const scheduledAt = typeof event.scheduledTime === "number" ? new Date(event.scheduledTime) : new Date()
    const runId = `${toIsoDateUtc(scheduledAt).replace(/-/g, "")}-${ulid()}`

    let lastSuccessfulRunAtUtc: string | null = null
    try {
      const latest = await getLatestSuccessfulRun(env)
      lastSuccessfulRunAtUtc = latest?.created_at_utc ?? null
    } catch (err) {
      logWarn("Failed to fetch latest successful run before schedule decision", {
        component: "scheduled",
        error: String(err),
      })
    }

    // Check if we should run based on day/holiday logic
    const { shouldRun, reason } = shouldRunScheduledPipeline({
      scheduledAt,
      lastSuccessfulRunAtUtc,
    })
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

      if (isPipelineLockError(error)) {
        logWarn("Scheduled run skipped: pipeline already locked by another execution", {
          runId,
          component: "scheduled",
        })
        return
      }

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
