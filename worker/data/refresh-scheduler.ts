import {
  expectedDailyCloseSlotKey,
  scheduledSlotKey,
  type RefreshScheduleId,
} from '../config/refresh-schedule.js';

const DEFAULT_STALE_AFTER_MINUTES = 90;
const DAILY_CLOSE_WATCHDOG_HOUR_UTC = 23;
const DAILY_CLOSE_WATCHDOG_MINUTE_UTC = 30;
const MISSED_DAILY_CLOSE_INCIDENT = 'missed_daily_close';

type SchedulerRunStatus = 'running' | 'success' | 'failed';
export type RefreshSchedulerHealthState =
  | 'healthy'
  | 'pending'
  | 'not_expected'
  | 'missed'
  | 'unknown';

interface SchedulerRunRow {
  slot_key: string;
  schedule_id: RefreshScheduleId;
  scheduled_at: string;
  decision_date: string;
  status: SchedulerRunStatus;
  attempt_count: number;
  claimed_at: string;
  completed_at: string | null;
}

interface SchedulerIncidentRow {
  incident_id: string;
  incident_type: typeof MISSED_DAILY_CLOSE_INCIDENT;
  decision_date: string;
  status: 'open' | 'resolved';
  opened_at: string;
  last_checked_at: string;
  resolved_at: string | null;
}

export type RefreshScheduleSlotClaim =
  | {
      status: 'claimed';
      slot_key: string;
      schedule_id: RefreshScheduleId;
      scheduled_at: string;
      decision_date: string;
      attempt: number;
      reclaimed: boolean;
    }
  | {
      status: 'skipped';
      slot_key: string;
      schedule_id: RefreshScheduleId;
      scheduled_at: string;
      decision_date: string;
      attempt: number;
      reason: 'already_succeeded' | 'in_progress';
    };

export interface RefreshSchedulerHealth {
  state: RefreshSchedulerHealthState;
  checked_at: string;
  decision_date: string | null;
  market_day_expected: boolean | null;
  latest_daily_close: {
    scheduled_at: string;
    decision_date: string;
    status: SchedulerRunStatus;
    attempt_count: number;
    completed_at: string | null;
  } | null;
  latest_incident: {
    decision_date: string;
    status: 'open' | 'resolved';
    opened_at: string;
    resolved_at: string | null;
  } | null;
}

export interface ClaimRefreshScheduleSlotOptions {
  scheduleId: RefreshScheduleId;
  scheduledTime: number;
  now?: Date;
  decisionDate?: string;
  staleAfterMinutes?: number;
}

export interface FinishRefreshScheduleSlotOptions {
  slotKey: string;
  attempt: number;
  status: 'success' | 'failed';
  error?: string | null;
  completedAt?: Date;
}

export interface EvaluateDailyCloseWatchdogOptions {
  decisionDate: string;
  isUsMarketDay: boolean;
  now?: Date;
}

export interface FetchRefreshSchedulerHealthOptions {
  now?: Date;
  decisionDate?: string;
  isUsMarketDay?: boolean;
}

function isoDateTime(value: Date): string {
  if (Number.isNaN(value.getTime())) throw new Error('Invalid scheduler timestamp');
  return value.toISOString();
}

function isIsoDateKey(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00.000Z`);
  return !Number.isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}

/** Return the New York calendar date associated with a Cloudflare scheduled event. */
export function newYorkDecisionDate(scheduledTime: number): string {
  const value = new Date(scheduledTime);
  if (Number.isNaN(value.getTime())) throw new Error('Invalid scheduled time');
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(value);
  const year = parts.find((part) => part.type === 'year')?.value;
  const month = parts.find((part) => part.type === 'month')?.value;
  const day = parts.find((part) => part.type === 'day')?.value;
  if (!year || !month || !day) throw new Error('Unable to resolve scheduler decision date');
  return `${year}-${month}-${day}`;
}

function watchdogCutoff(decisionDate: string): number {
  return Date.parse(
    `${decisionDate}T${String(DAILY_CLOSE_WATCHDOG_HOUR_UTC).padStart(2, '0')}:${String(DAILY_CLOSE_WATCHDOG_MINUTE_UTC).padStart(2, '0')}:00.000Z`,
  );
}

function publicRun(row: SchedulerRunRow | null): RefreshSchedulerHealth['latest_daily_close'] {
  if (!row) return null;
  return {
    scheduled_at: row.scheduled_at,
    decision_date: row.decision_date,
    status: row.status,
    attempt_count: row.attempt_count,
    completed_at: row.completed_at,
  };
}

function publicIncident(row: SchedulerIncidentRow | null): RefreshSchedulerHealth['latest_incident'] {
  if (!row) return null;
  return {
    decision_date: row.decision_date,
    status: row.status,
    opened_at: row.opened_at,
    resolved_at: row.resolved_at,
  };
}

/**
 * Atomically claim a deterministic Cloudflare Cron slot.
 *
 * A successful slot is permanently deduplicated. Failed attempts and running
 * attempts whose lease expired reuse the same slot row with a fenced attempt
 * number, so a late prior worker cannot complete a reclaimed attempt.
 */
export async function claimRefreshScheduleSlot(
  db: D1Database,
  options: ClaimRefreshScheduleSlotOptions,
): Promise<RefreshScheduleSlotClaim> {
  const scheduledAt = isoDateTime(new Date(options.scheduledTime));
  const canonicalDecisionDate = newYorkDecisionDate(options.scheduledTime);
  const decisionDate = options.decisionDate ?? canonicalDecisionDate;
  if (!isIsoDateKey(decisionDate)) throw new Error('Invalid scheduler decision date');
  if (options.scheduleId === 'daily_close' && decisionDate !== canonicalDecisionDate) {
    throw new Error('Daily-close decision date must match the scheduled New York date');
  }

  const now = options.now ?? new Date();
  const claimedAt = isoDateTime(now);
  const staleMinutes = Math.max(1, Math.floor(options.staleAfterMinutes ?? DEFAULT_STALE_AFTER_MINUTES));
  const staleBefore = new Date(now.getTime() - staleMinutes * 60_000).toISOString();
  const slotKey = scheduledSlotKey(options.scheduleId, options.scheduledTime);

  const claimed = await db.prepare(`
    INSERT INTO refresh_scheduler_runs (
      slot_key,
      schedule_id,
      scheduled_at,
      decision_date,
      status,
      attempt_count,
      claimed_at,
      completed_at,
      last_error,
      created_at,
      updated_at
    ) VALUES (?, ?, ?, ?, 'running', 1, ?, NULL, NULL, ?, ?)
    ON CONFLICT(slot_key) DO UPDATE SET
      status = 'running',
      attempt_count = refresh_scheduler_runs.attempt_count + 1,
      claimed_at = excluded.claimed_at,
      completed_at = NULL,
      last_error = NULL,
      updated_at = excluded.updated_at
    WHERE refresh_scheduler_runs.status = 'failed'
       OR (
         refresh_scheduler_runs.status = 'running'
         AND refresh_scheduler_runs.claimed_at <= ?
       )
    RETURNING
      slot_key,
      schedule_id,
      scheduled_at,
      decision_date,
      status,
      attempt_count,
      claimed_at,
      completed_at
  `).bind(
    slotKey,
    options.scheduleId,
    scheduledAt,
    decisionDate,
    claimedAt,
    claimedAt,
    claimedAt,
    staleBefore,
  ).first<SchedulerRunRow>();

  if (claimed) {
    return {
      status: 'claimed',
      slot_key: claimed.slot_key,
      schedule_id: claimed.schedule_id,
      scheduled_at: claimed.scheduled_at,
      decision_date: claimed.decision_date,
      attempt: claimed.attempt_count,
      reclaimed: claimed.attempt_count > 1,
    };
  }

  const existing = await db.prepare(`
    SELECT
      slot_key,
      schedule_id,
      scheduled_at,
      decision_date,
      status,
      attempt_count,
      claimed_at,
      completed_at
    FROM refresh_scheduler_runs
    WHERE slot_key = ?
    LIMIT 1
  `).bind(slotKey).first<SchedulerRunRow>();

  if (!existing) throw new Error('Scheduler slot claim lost without a persisted row');
  return {
    status: 'skipped',
    slot_key: existing.slot_key,
    schedule_id: existing.schedule_id,
    scheduled_at: existing.scheduled_at,
    decision_date: existing.decision_date,
    attempt: existing.attempt_count,
    reason: existing.status === 'success' ? 'already_succeeded' : 'in_progress',
  };
}

/** Finish only the claimed attempt; the attempt number is a stale-worker fence. */
export async function finishRefreshScheduleSlot(
  db: D1Database,
  options: FinishRefreshScheduleSlotOptions,
): Promise<boolean> {
  const completedAt = isoDateTime(options.completedAt ?? new Date());
  const error = options.status === 'failed'
    ? (options.error?.trim().slice(0, 1000) || 'scheduled_refresh_failed')
    : null;

  const completed = await db.prepare(`
    UPDATE refresh_scheduler_runs
    SET status = ?,
        completed_at = ?,
        last_error = ?,
        updated_at = ?
    WHERE slot_key = ?
      AND status = 'running'
      AND attempt_count = ?
    RETURNING
      slot_key,
      schedule_id,
      scheduled_at,
      decision_date,
      status,
      attempt_count,
      claimed_at,
      completed_at
  `).bind(
    options.status,
    completedAt,
    error,
    completedAt,
    options.slotKey,
    Math.max(1, Math.floor(options.attempt)),
  ).first<SchedulerRunRow>();

  if (!completed) return false;

  if (options.status === 'success' && completed.schedule_id === 'daily_close') {
    await db.prepare(`
      UPDATE refresh_scheduler_incidents
      SET status = 'resolved',
          resolved_at = ?,
          last_checked_at = ?,
          resolution_slot_key = ?
      WHERE incident_type = ?
        AND status = 'open'
        AND decision_date = ?
    `).bind(
      completedAt,
      completedAt,
      completed.slot_key,
      MISSED_DAILY_CLOSE_INCIDENT,
      completed.decision_date,
    ).run();
  }

  return true;
}

/** Evaluate and persist the daily-close missed-refresh watchdog state. */
export async function evaluateDailyCloseWatchdog(
  db: D1Database,
  options: EvaluateDailyCloseWatchdogOptions,
): Promise<RefreshSchedulerHealth> {
  if (!isIsoDateKey(options.decisionDate)) throw new Error('Invalid watchdog decision date');
  const now = options.now ?? new Date();
  const checkedAt = isoDateTime(now);

  if (!options.isUsMarketDay) {
    return {
      state: 'not_expected',
      checked_at: checkedAt,
      decision_date: options.decisionDate,
      market_day_expected: false,
      latest_daily_close: null,
      latest_incident: null,
    };
  }

  const expectedSlot = expectedDailyCloseSlotKey(options.decisionDate);
  const run = await db.prepare(`
    SELECT
      slot_key,
      schedule_id,
      scheduled_at,
      decision_date,
      status,
      attempt_count,
      claimed_at,
      completed_at
    FROM refresh_scheduler_runs
    WHERE slot_key = ?
      AND schedule_id = 'daily_close'
    LIMIT 1
  `).bind(expectedSlot).first<SchedulerRunRow>();

  if (run?.status === 'success') {
    const incident = await resolveDailyCloseIncident(db, options.decisionDate, run.slot_key, checkedAt);
    return {
      state: 'healthy',
      checked_at: checkedAt,
      decision_date: options.decisionDate,
      market_day_expected: true,
      latest_daily_close: publicRun(run),
      latest_incident: publicIncident(incident),
    };
  }

  if (now.getTime() < watchdogCutoff(options.decisionDate)) {
    return {
      state: 'pending',
      checked_at: checkedAt,
      decision_date: options.decisionDate,
      market_day_expected: true,
      latest_daily_close: publicRun(run),
      latest_incident: null,
    };
  }

  const incidentId = `${MISSED_DAILY_CLOSE_INCIDENT}:${options.decisionDate}`;
  const incident = await db.prepare(`
    INSERT INTO refresh_scheduler_incidents (
      incident_id,
      incident_type,
      decision_date,
      status,
      expected_slot_key,
      opened_at,
      last_checked_at,
      resolved_at,
      resolution_slot_key,
      details_json
    ) VALUES (?, ?, ?, 'open', ?, ?, ?, NULL, NULL, ?)
    ON CONFLICT(incident_type, decision_date) DO UPDATE SET
      status = 'open',
      last_checked_at = excluded.last_checked_at,
      resolved_at = NULL,
      resolution_slot_key = NULL,
      details_json = excluded.details_json
    RETURNING
      incident_id,
      incident_type,
      decision_date,
      status,
      opened_at,
      last_checked_at,
      resolved_at
  `).bind(
    incidentId,
    MISSED_DAILY_CLOSE_INCIDENT,
    options.decisionDate,
    expectedSlot,
    checkedAt,
    checkedAt,
    JSON.stringify({ reason: 'daily_close_refresh_not_successful_by_watchdog_cutoff' }),
  ).first<SchedulerIncidentRow>();

  if (!incident) throw new Error('Failed to persist missed-refresh incident');
  return {
    state: 'missed',
    checked_at: checkedAt,
    decision_date: options.decisionDate,
    market_day_expected: true,
    latest_daily_close: publicRun(run),
    latest_incident: publicIncident(incident),
  };
}

async function resolveDailyCloseIncident(
  db: D1Database,
  decisionDate: string,
  resolutionSlotKey: string,
  resolvedAt: string,
): Promise<SchedulerIncidentRow | null> {
  const resolved = await db.prepare(`
    UPDATE refresh_scheduler_incidents
    SET status = 'resolved',
        resolved_at = ?,
        last_checked_at = ?,
        resolution_slot_key = ?
    WHERE incident_type = ?
      AND decision_date = ?
      AND status = 'open'
    RETURNING
      incident_id,
      incident_type,
      decision_date,
      status,
      opened_at,
      last_checked_at,
      resolved_at
  `).bind(
    resolvedAt,
    resolvedAt,
    resolutionSlotKey,
    MISSED_DAILY_CLOSE_INCIDENT,
    decisionDate,
  ).first<SchedulerIncidentRow>();
  if (resolved) return resolved;

  return db.prepare(`
    SELECT
      incident_id,
      incident_type,
      decision_date,
      status,
      opened_at,
      last_checked_at,
      resolved_at
    FROM refresh_scheduler_incidents
    WHERE incident_type = ?
      AND decision_date = ?
    LIMIT 1
  `).bind(MISSED_DAILY_CLOSE_INCIDENT, decisionDate).first<SchedulerIncidentRow>();
}

/** Read the latest public-safe scheduler health without exposing error payloads. */
export async function fetchRefreshSchedulerHealth(
  db: D1Database,
  options: FetchRefreshSchedulerHealthOptions = {},
): Promise<RefreshSchedulerHealth> {
  const now = options.now ?? new Date();
  const checkedAt = isoDateTime(now);
  if (options.decisionDate !== undefined && !isIsoDateKey(options.decisionDate)) {
    throw new Error('Invalid health decision date');
  }

  const runQuery = options.decisionDate
    ? db.prepare(`
        SELECT
          slot_key,
          schedule_id,
          scheduled_at,
          decision_date,
          status,
          attempt_count,
          claimed_at,
          completed_at
        FROM refresh_scheduler_runs
        WHERE schedule_id = 'daily_close'
          AND decision_date = ?
        ORDER BY scheduled_at DESC
        LIMIT 1
      `).bind(options.decisionDate)
    : db.prepare(`
        SELECT
          slot_key,
          schedule_id,
          scheduled_at,
          decision_date,
          status,
          attempt_count,
          claimed_at,
          completed_at
        FROM refresh_scheduler_runs
        WHERE schedule_id = 'daily_close'
        ORDER BY scheduled_at DESC
        LIMIT 1
      `);

  const [run, openIncident, latestIncident] = await Promise.all([
    runQuery.first<SchedulerRunRow>(),
    db.prepare(`
      SELECT
        incident_id,
        incident_type,
        decision_date,
        status,
        opened_at,
        last_checked_at,
        resolved_at
      FROM refresh_scheduler_incidents
      WHERE incident_type = ?
        AND status = 'open'
      ORDER BY decision_date DESC, opened_at DESC
      LIMIT 1
    `).bind(MISSED_DAILY_CLOSE_INCIDENT).first<SchedulerIncidentRow>(),
    db.prepare(`
      SELECT
        incident_id,
        incident_type,
        decision_date,
        status,
        opened_at,
        last_checked_at,
        resolved_at
      FROM refresh_scheduler_incidents
      WHERE incident_type = ?
      ORDER BY decision_date DESC, opened_at DESC
      LIMIT 1
    `).bind(MISSED_DAILY_CLOSE_INCIDENT).first<SchedulerIncidentRow>(),
  ]);

  let state: RefreshSchedulerHealthState = 'unknown';
  if (openIncident && (!options.decisionDate || openIncident.decision_date <= options.decisionDate)) {
    state = 'missed';
  } else if (options.decisionDate && options.isUsMarketDay === false) {
    state = 'not_expected';
  } else if (run?.status === 'success') {
    state = 'healthy';
  } else if (run) {
    state = now.getTime() < watchdogCutoff(run.decision_date) ? 'pending' : 'missed';
  } else if (options.decisionDate && options.isUsMarketDay === true) {
    state = now.getTime() < watchdogCutoff(options.decisionDate) ? 'pending' : 'missed';
  }

  return {
    state,
    checked_at: checkedAt,
    decision_date: options.decisionDate ?? run?.decision_date ?? openIncident?.decision_date ?? null,
    market_day_expected: options.isUsMarketDay ?? null,
    latest_daily_close: publicRun(run ?? null),
    latest_incident: publicIncident(openIncident ?? latestIncident ?? null),
  };
}
