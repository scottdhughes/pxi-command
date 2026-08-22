import { INDICATORS } from '../../src/config/indicators.js';
import {
  MONITORED_SLA_INDICATORS,
  evaluateSla,
  resolveIndicatorSla,
} from '../../src/config/indicator-sla.js';

export interface IndicatorCandidate {
  indicator_id: string;
  date: string;
  value: number;
  source: string;
}

export interface CandidateSlaSummary {
  checked: number;
  critical_failures: Array<{
    indicator_id: string;
    latest_date: string | null;
    status: 'stale' | 'missing';
  }>;
  non_critical_failures: number;
}

const indicatorFrequency = new Map(
  INDICATORS.map((indicator) => [indicator.id, indicator.frequency]),
);
const monitoredIndicators = new Set([
  ...INDICATORS.map((indicator) => indicator.id),
  ...MONITORED_SLA_INDICATORS,
]);

function newYorkDateKey(now: Date): string {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(now);
  const year = parts.find((part) => part.type === 'year')?.value;
  const month = parts.find((part) => part.type === 'month')?.value;
  const day = parts.find((part) => part.type === 'day')?.value;
  if (!year || !month || !day) throw new Error('Unable to resolve candidate cutoff date');
  return `${year}-${month}-${day}`;
}

function isUsableCandidate(candidate: IndicatorCandidate, maxDate: string): boolean {
  if (!candidate.indicator_id || !candidate.source || !Number.isFinite(candidate.value)) return false;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(candidate.date)) return false;
  const parsed = new Date(`${candidate.date}T00:00:00.000Z`);
  return !Number.isNaN(parsed.getTime())
    && parsed.toISOString().slice(0, 10) === candidate.date
    && candidate.date <= maxDate;
}

export function evaluateCandidateSla(
  candidates: IndicatorCandidate[],
  now = new Date(),
): CandidateSlaSummary {
  const maxDate = newYorkDateKey(now);
  const latestDates = new Map<string, string>();
  for (const candidate of candidates) {
    if (!isUsableCandidate(candidate, maxDate)) continue;
    const current = latestDates.get(candidate.indicator_id);
    if (!current || candidate.date > current) {
      latestDates.set(candidate.indicator_id, candidate.date);
    }
  }

  const criticalFailures: CandidateSlaSummary['critical_failures'] = [];
  let nonCriticalFailures = 0;
  for (const indicatorId of [...monitoredIndicators].sort()) {
    const policy = resolveIndicatorSla(indicatorId, indicatorFrequency.get(indicatorId));
    const evaluation = evaluateSla(latestDates.get(indicatorId) ?? null, now, policy);
    if (!evaluation.stale && !evaluation.missing) continue;
    if (evaluation.critical) {
      criticalFailures.push({
        indicator_id: indicatorId,
        latest_date: evaluation.latest_date,
        status: evaluation.missing ? 'missing' : 'stale',
      });
    } else {
      nonCriticalFailures += 1;
    }
  }

  return {
    checked: monitoredIndicators.size,
    critical_failures: criticalFailures,
    non_critical_failures: nonCriticalFailures,
  };
}

/**
 * A scheduled refresh needs the full fetched history for derived indicators and
 * freshness checks, but only the newest observation per series is a live write.
 * Historical ingestion remains a separate bounded workflow.
 */
export function selectLatestIndicatorCandidates(
  candidates: IndicatorCandidate[],
  maxDate = newYorkDateKey(new Date()),
): IndicatorCandidate[] {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(maxDate)) throw new Error('Invalid candidate cutoff date');
  const latest = new Map<string, IndicatorCandidate>();
  for (const candidate of candidates) {
    if (!isUsableCandidate(candidate, maxDate)) continue;
    const current = latest.get(candidate.indicator_id);
    if (!current || candidate.date >= current.date) {
      latest.set(candidate.indicator_id, candidate);
    }
  }
  return [...latest.values()].sort((left, right) => (
    left.indicator_id.localeCompare(right.indicator_id)
  ));
}
