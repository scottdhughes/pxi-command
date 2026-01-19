export function nowUtcIso(): string {
  return new Date().toISOString()
}

export function toUtcDateString(tsSeconds: number): string {
  const d = new Date(tsSeconds * 1000)
  return d.toISOString().slice(0, 10)
}

export function daysAgoUtc(days: number): Date {
  const d = new Date()
  d.setUTCDate(d.getUTCDate() - days)
  return d
}

export function dateToYmd(date: Date): string {
  return date.toISOString().slice(0, 10)
}

export function getUtcDayKey(tsSeconds: number): string {
  return toUtcDateString(tsSeconds)
}
