// ─────────────────────────────────────────────────────────────────────────────
// NYSE Holiday Calendar + Trading-Day Utilities
//
// Primary source:
// https://www.nyse.com/trade/hours-calendars
// ─────────────────────────────────────────────────────────────────────────────

const NYSE_EXCEPTIONAL_CLOSURES = new Set<string>([
  // National days of mourning / major disruptions (historical)
  "2001-09-11",
  "2001-09-12",
  "2001-09-13",
  "2001-09-14",
  "2004-06-11",
  "2007-01-02",
  "2012-10-29",
  "2012-10-30",
  "2018-12-05",
])

const nyseHolidayCache = new Map<number, Set<string>>()

export function toIsoDateUtc(date: Date): string {
  return date.toISOString().slice(0, 10)
}

function makeUtcDate(year: number, monthIndex: number, day: number): Date {
  return new Date(Date.UTC(year, monthIndex, day))
}

function addUtcDays(date: Date, days: number): Date {
  const next = new Date(date)
  next.setUTCDate(next.getUTCDate() + days)
  return next
}

function nthWeekdayOfMonthUtc(year: number, monthIndex: number, weekday: number, nth: number): Date {
  const firstDay = makeUtcDate(year, monthIndex, 1)
  const offset = (weekday - firstDay.getUTCDay() + 7) % 7
  return makeUtcDate(year, monthIndex, 1 + offset + (nth - 1) * 7)
}

function lastWeekdayOfMonthUtc(year: number, monthIndex: number, weekday: number): Date {
  const lastDay = makeUtcDate(year, monthIndex + 1, 0)
  const offset = (lastDay.getUTCDay() - weekday + 7) % 7
  return makeUtcDate(year, monthIndex, lastDay.getUTCDate() - offset)
}

function observedFixedHolidayUtc(year: number, monthIndex: number, day: number): Date {
  const holiday = makeUtcDate(year, monthIndex, day)
  const dayOfWeek = holiday.getUTCDay()

  // NYSE observed-holiday convention for fixed-date holidays:
  // Saturday -> Friday, Sunday -> Monday
  if (dayOfWeek === 6) return addUtcDays(holiday, -1)
  if (dayOfWeek === 0) return addUtcDays(holiday, 1)
  return holiday
}

// Meeus/Jones/Butcher Gregorian Easter algorithm
function easterSundayUtc(year: number): Date {
  const a = year % 19
  const b = Math.floor(year / 100)
  const c = year % 100
  const d = Math.floor(b / 4)
  const e = b % 4
  const f = Math.floor((b + 8) / 25)
  const g = Math.floor((b - f + 1) / 3)
  const h = (19 * a + b - d - g + 15) % 30
  const i = Math.floor(c / 4)
  const k = c % 4
  const l = (32 + 2 * e + 2 * i - h - k) % 7
  const m = Math.floor((a + 11 * h + 22 * l) / 451)
  const month = Math.floor((h + l - 7 * m + 114) / 31)
  const day = ((h + l - 7 * m + 114) % 31) + 1
  return makeUtcDate(year, month - 1, day)
}

export function getNyseHolidaySet(year: number): Set<string> {
  const cached = nyseHolidayCache.get(year)
  if (cached) return cached

  const holidays = new Set<string>()

  // New Year's Day (observed)
  holidays.add(toIsoDateUtc(observedFixedHolidayUtc(year, 0, 1)))
  // Cross-year edge: Jan 1 (next year) observed on Dec 31 of current year
  const nextYearObservedNewYears = observedFixedHolidayUtc(year + 1, 0, 1)
  if (nextYearObservedNewYears.getUTCFullYear() === year) {
    holidays.add(toIsoDateUtc(nextYearObservedNewYears))
  }

  // Martin Luther King Jr. Day: third Monday in January
  holidays.add(toIsoDateUtc(nthWeekdayOfMonthUtc(year, 0, 1, 3)))

  // Presidents' Day (Washington's Birthday): third Monday in February
  holidays.add(toIsoDateUtc(nthWeekdayOfMonthUtc(year, 1, 1, 3)))

  // Good Friday: Friday before Easter Sunday
  holidays.add(toIsoDateUtc(addUtcDays(easterSundayUtc(year), -2)))

  // Memorial Day: last Monday in May
  holidays.add(toIsoDateUtc(lastWeekdayOfMonthUtc(year, 4, 1)))

  // Juneteenth National Independence Day (NYSE since 2022), observed
  if (year >= 2022) {
    holidays.add(toIsoDateUtc(observedFixedHolidayUtc(year, 5, 19)))
  }

  // Independence Day, observed
  holidays.add(toIsoDateUtc(observedFixedHolidayUtc(year, 6, 4)))

  // Labor Day: first Monday in September
  holidays.add(toIsoDateUtc(nthWeekdayOfMonthUtc(year, 8, 1, 1)))

  // Thanksgiving Day: fourth Thursday in November
  holidays.add(toIsoDateUtc(nthWeekdayOfMonthUtc(year, 10, 4, 4)))

  // Christmas Day, observed
  holidays.add(toIsoDateUtc(observedFixedHolidayUtc(year, 11, 25)))

  // Exceptional closures that do not follow standard recurrence rules
  for (const date of NYSE_EXCEPTIONAL_CLOSURES) {
    if (date.startsWith(`${year}-`)) holidays.add(date)
  }

  nyseHolidayCache.set(year, holidays)
  return holidays
}

export function isNyseHolidayDate(date: Date): boolean {
  const isoDate = toIsoDateUtc(date)
  return getNyseHolidaySet(date.getUTCFullYear()).has(isoDate)
}

function parseIsoDateUtc(isoDate: string): Date {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(isoDate)
  if (!match) {
    throw new Error(`Invalid ISO date format: ${isoDate}`)
  }

  const year = Number(match[1])
  const monthIndex = Number(match[2]) - 1
  const day = Number(match[3])
  return makeUtcDate(year, monthIndex, day)
}

function isWeekend(date: Date): boolean {
  const day = date.getUTCDay()
  return day === 0 || day === 6
}

export function addTradingDays(startDate: string, tradingDays: number): string {
  if (!Number.isInteger(tradingDays) || tradingDays < 0) {
    throw new Error(`tradingDays must be a non-negative integer; received ${tradingDays}`)
  }

  let date = parseIsoDateUtc(startDate)
  let daysAdded = 0

  while (daysAdded < tradingDays) {
    date = addUtcDays(date, 1)
    if (isWeekend(date) || isNyseHolidayDate(date)) {
      continue
    }
    daysAdded += 1
  }

  return toIsoDateUtc(date)
}
