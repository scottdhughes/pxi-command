export const HISTORY_RECONSTRUCTION_CONTRACT = 'isolated-missing-only-v1' as const;

export function currentNewYorkDate(now = new Date()): string {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(now);
  const value = (type: Intl.DateTimeFormatPartTypes) =>
    parts.find((part) => part.type === type)?.value;
  const year = value('year');
  const month = value('month');
  const day = value('day');
  if (!year || !month || !day) {
    throw new Error('Could not resolve the current America/New_York date');
  }
  return `${year}-${month}-${day}`;
}
