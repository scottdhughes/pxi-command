export const OVERNIGHT_REFRESH_CRON = '0 6 * * *';
export const PREMARKET_REFRESH_CRON = '0 14 * * *';
export const MIDDAY_REFRESH_CRON = '0 18 * * *';
export const DAILY_CLOSE_REFRESH_CRON = '0 22 * * 1-5';
export const MISSED_REFRESH_WATCHDOG_CRON = '30 23 * * 1-5';

export const PRODUCTION_CRON_EXPRESSIONS = [
  OVERNIGHT_REFRESH_CRON,
  PREMARKET_REFRESH_CRON,
  MIDDAY_REFRESH_CRON,
  DAILY_CLOSE_REFRESH_CRON,
  MISSED_REFRESH_WATCHDOG_CRON,
] as const;

export type RefreshScheduleId = 'overnight' | 'premarket' | 'midday' | 'daily_close';

export interface RefreshScheduleSpec {
  cron: string;
  schedule_id: RefreshScheduleId;
  record_research_evidence: boolean;
}

export const REFRESH_SCHEDULES: readonly RefreshScheduleSpec[] = [
  {
    cron: OVERNIGHT_REFRESH_CRON,
    schedule_id: 'overnight',
    record_research_evidence: false,
  },
  {
    cron: PREMARKET_REFRESH_CRON,
    schedule_id: 'premarket',
    record_research_evidence: false,
  },
  {
    cron: MIDDAY_REFRESH_CRON,
    schedule_id: 'midday',
    record_research_evidence: false,
  },
  {
    cron: DAILY_CLOSE_REFRESH_CRON,
    schedule_id: 'daily_close',
    record_research_evidence: true,
  },
] as const;

export function findRefreshSchedule(cron: string): RefreshScheduleSpec | null {
  return REFRESH_SCHEDULES.find((schedule) => schedule.cron === cron) ?? null;
}

function parseIsoCalendarDate(dateKey: string): {
  year: number;
  month: number;
  day: number;
  weekday: number;
} | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(dateKey);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year
    || date.getUTCMonth() !== month - 1
    || date.getUTCDate() !== day
  ) {
    return null;
  }
  return { year, month, day, weekday: date.getUTCDay() };
}

function isoDate(year: number, monthIndex: number, day: number): string {
  return new Date(Date.UTC(year, monthIndex, day)).toISOString().slice(0, 10);
}

function observedFixedHoliday(year: number, monthIndex: number, day: number): string {
  const date = new Date(Date.UTC(year, monthIndex, day));
  const weekday = date.getUTCDay();
  if (weekday === 6) date.setUTCDate(date.getUTCDate() - 1);
  if (weekday === 0) date.setUTCDate(date.getUTCDate() + 1);
  return date.toISOString().slice(0, 10);
}

function nthWeekdayOfMonth(
  year: number,
  monthIndex: number,
  weekday: number,
  occurrence: number,
): string {
  const first = new Date(Date.UTC(year, monthIndex, 1));
  const offset = (weekday - first.getUTCDay() + 7) % 7;
  return isoDate(year, monthIndex, 1 + offset + (occurrence - 1) * 7);
}

function lastWeekdayOfMonth(year: number, monthIndex: number, weekday: number): string {
  const last = new Date(Date.UTC(year, monthIndex + 1, 0));
  const offset = (last.getUTCDay() - weekday + 7) % 7;
  return isoDate(year, monthIndex, last.getUTCDate() - offset);
}

function goodFriday(year: number): string {
  const a = year % 19;
  const b = Math.floor(year / 100);
  const c = year % 100;
  const d = Math.floor(b / 4);
  const e = b % 4;
  const f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4);
  const k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31);
  const day = ((h + l - 7 * m + 114) % 31) + 1;
  const easter = new Date(Date.UTC(year, month - 1, day));
  easter.setUTCDate(easter.getUTCDate() - 2);
  return easter.toISOString().slice(0, 10);
}

/** Full-day NYSE calendar used by both canonical capture and its watchdog. */
export function isUsEquityTradingDay(dateKey: string): boolean {
  const parsed = parseIsoCalendarDate(dateKey);
  if (!parsed || parsed.weekday === 0 || parsed.weekday === 6) return false;

  const { year } = parsed;
  const holidays = new Set<string>([
    observedFixedHoliday(year, 0, 1),
    observedFixedHoliday(year + 1, 0, 1),
    nthWeekdayOfMonth(year, 0, 1, 3),
    nthWeekdayOfMonth(year, 1, 1, 3),
    goodFriday(year),
    lastWeekdayOfMonth(year, 4, 1),
    observedFixedHoliday(year, 6, 4),
    nthWeekdayOfMonth(year, 8, 1, 1),
    nthWeekdayOfMonth(year, 10, 4, 4),
    observedFixedHoliday(year, 11, 25),
  ]);
  if (year >= 2022) holidays.add(observedFixedHoliday(year, 5, 19));
  return !holidays.has(dateKey);
}

export function scheduledSlotKey(scheduleId: RefreshScheduleId, scheduledTime: number): string {
  return `pxi-refresh:${scheduleId}:${new Date(scheduledTime).toISOString()}`;
}

export function expectedDailyCloseSlotKey(decisionDate: string): string {
  return scheduledSlotKey('daily_close', Date.parse(`${decisionDate}T22:00:00.000Z`));
}

export function isPastDailyCloseWatchdogCutoff(now: Date): boolean {
  const hour = now.getUTCHours();
  return hour > 23 || (hour === 23 && now.getUTCMinutes() >= 30);
}

/** Next configured refresh slot; the canonical 22:00 slot is weekdays only. */
export function nextScheduledRefreshAt(now = new Date()): Date {
  if (Number.isNaN(now.getTime())) throw new Error('Invalid refresh schedule timestamp');
  const alwaysHours = [6, 14, 18] as const;

  for (let dayOffset = 0; dayOffset <= 7; dayOffset += 1) {
    const day = new Date(Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate() + dayOffset,
    ));
    const weekday = day.getUTCDay();
    const hours = weekday >= 1 && weekday <= 5
      ? [...alwaysHours, 22]
      : [...alwaysHours];
    for (const hour of hours) {
      const candidate = new Date(Date.UTC(
        day.getUTCFullYear(),
        day.getUTCMonth(),
        day.getUTCDate(),
        hour,
      ));
      if (candidate.getTime() >= now.getTime()) return candidate;
    }
  }

  throw new Error('Unable to resolve next configured refresh');
}
