import { INDICATORS } from './indicators.js';

export type IndicatorSlaClass = 'daily' | 'weekly' | 'monthly' | 'source_lagged';

export interface IndicatorSlaPolicy {
  class: IndicatorSlaClass;
  max_age_days: number;
  critical: boolean;
}

export interface IndicatorSlaEvaluation {
  latest_date: string | null;
  days_old: number | null;
  stale: boolean;
  missing: boolean;
  max_age_days: number;
  critical: boolean;
  sla_class: IndicatorSlaClass;
}

export type StaleEscalationTier = 'observe' | 'retry_source' | 'escalate_ops';

export interface IndicatorStalePolicy {
  retry_attempts: number;
  retry_backoff_minutes: number;
  escalation: StaleEscalationTier;
  owner: 'market_data' | 'macro_data' | 'risk_ops';
}

const DAILY_MAX_AGE_DAYS = 4;
const WEEKLY_MAX_AGE_DAYS = 10;
const MONTHLY_MAX_AGE_DAYS = 45;

const SOURCE_LAGGED_MAX_AGE_BY_INDICATOR: Record<string, number> = {
  wti_crude: 7,
  dollar_index: 10,
};

const MAX_AGE_OVERRIDE_BY_INDICATOR: Record<string, number> = {
  cfnai: 120,
  m2_yoy: 120,
  fed_balance_sheet: 14,
  treasury_general_account: 14,
  reverse_repo: 14,
  net_liquidity: 14,
};

const DEFAULT_STALE_POLICY_BY_CLASS: Record<IndicatorSlaClass, IndicatorStalePolicy> = {
  daily: {
    retry_attempts: 2,
    retry_backoff_minutes: 30,
    escalation: 'retry_source',
    owner: 'market_data',
  },
  weekly: {
    retry_attempts: 1,
    retry_backoff_minutes: 180,
    escalation: 'observe',
    owner: 'macro_data',
  },
  monthly: {
    retry_attempts: 1,
    retry_backoff_minutes: 720,
    escalation: 'observe',
    owner: 'macro_data',
  },
  source_lagged: {
    retry_attempts: 0,
    retry_backoff_minutes: 0,
    escalation: 'observe',
    owner: 'macro_data',
  },
};

const INDICATOR_STALE_POLICY_OVERRIDES: Record<string, Partial<IndicatorStalePolicy>> = {
  vix: {
    retry_attempts: 3,
    retry_backoff_minutes: 15,
    escalation: 'escalate_ops',
    owner: 'risk_ops',
  },
  spy_close: {
    retry_attempts: 3,
    retry_backoff_minutes: 15,
    escalation: 'escalate_ops',
    owner: 'risk_ops',
  },
  dxy: {
    retry_attempts: 2,
    retry_backoff_minutes: 30,
    escalation: 'escalate_ops',
    owner: 'risk_ops',
  },
  aaii_sentiment: {
    retry_attempts: 1,
    retry_backoff_minutes: 180,
    escalation: 'retry_source',
    owner: 'macro_data',
  },
};

const INDICATOR_FREQUENCY_HINTS: Record<string, string> = Object.fromEntries(
  INDICATORS.map((indicator) => [indicator.id, indicator.frequency])
);

const KNOWN_FREQUENCY_HINTS: Record<string, string> = {
  ...INDICATOR_FREQUENCY_HINTS,
  // Internal/computed series used by cron-fast but not part of canonical indicator definitions.
  fear_greed: 'daily',
  spy_close: 'daily',
  hyg: 'daily',
  lqd: 'daily',
  net_liquidity: 'weekly',
};

export const CRITICAL_INDICATORS = new Set<string>([
  'aaii_sentiment',
  'copper_gold_ratio',
  'vix',
  'spy_close',
  'dxy',
  'hyg',
  'lqd',
  'fear_greed',
]);

export const MONITORED_SLA_INDICATORS = new Set<string>([
  ...CRITICAL_INDICATORS,
  ...Object.keys(SOURCE_LAGGED_MAX_AGE_BY_INDICATOR),
  ...Object.keys(MAX_AGE_OVERRIDE_BY_INDICATOR),
]);

function normalizeFrequency(frequency: string | null | undefined): 'daily' | 'weekly' | 'monthly' | 'realtime' {
  const normalized = frequency?.trim().toLowerCase();
  if (normalized === 'weekly' || normalized === 'monthly' || normalized === 'realtime') {
    return normalized;
  }
  return 'daily';
}

function frequencyToSlaClass(frequency: 'daily' | 'weekly' | 'monthly' | 'realtime'): IndicatorSlaClass {
  if (frequency === 'weekly') return 'weekly';
  if (frequency === 'monthly') return 'monthly';
  return 'daily';
}

function defaultMaxAgeDaysForFrequency(frequency: 'daily' | 'weekly' | 'monthly' | 'realtime'): number {
  if (frequency === 'weekly') return WEEKLY_MAX_AGE_DAYS;
  if (frequency === 'monthly') return MONTHLY_MAX_AGE_DAYS;
  return DAILY_MAX_AGE_DAYS;
}

function normalizeIsoDateString(value: string): string {
  return value.slice(0, 10);
}

function parseLatestDate(value: string | Date): Date | null {
  if (value instanceof Date) {
    return Number.isFinite(value.getTime()) ? value : null;
  }

  const iso = normalizeIsoDateString(value);
  const parsed = new Date(`${iso}T00:00:00Z`);
  if (!Number.isFinite(parsed.getTime())) {
    return null;
  }
  return parsed;
}

export function resolveIndicatorSla(
  indicatorId: string,
  frequency?: string | null
): IndicatorSlaPolicy {
  if (SOURCE_LAGGED_MAX_AGE_BY_INDICATOR[indicatorId] !== undefined) {
    return {
      class: 'source_lagged',
      max_age_days: SOURCE_LAGGED_MAX_AGE_BY_INDICATOR[indicatorId],
      critical: CRITICAL_INDICATORS.has(indicatorId),
    };
  }

  const inferredFrequency = normalizeFrequency(frequency ?? KNOWN_FREQUENCY_HINTS[indicatorId]);
  const defaultMaxAgeDays = defaultMaxAgeDaysForFrequency(inferredFrequency);
  const maxAgeDays = MAX_AGE_OVERRIDE_BY_INDICATOR[indicatorId] ?? defaultMaxAgeDays;

  return {
    class: frequencyToSlaClass(inferredFrequency),
    max_age_days: maxAgeDays,
    critical: CRITICAL_INDICATORS.has(indicatorId),
  };
}

export function getStaleThresholdDays(
  indicatorId: string,
  frequency?: string | null
): number {
  return resolveIndicatorSla(indicatorId, frequency).max_age_days;
}

export function resolveStalePolicy(
  indicatorId: string,
  frequency?: string | null
): IndicatorStalePolicy {
  const policy = resolveIndicatorSla(indicatorId, frequency);
  const defaults = DEFAULT_STALE_POLICY_BY_CLASS[policy.class];
  const override = INDICATOR_STALE_POLICY_OVERRIDES[indicatorId];
  return {
    retry_attempts: override?.retry_attempts ?? defaults.retry_attempts,
    retry_backoff_minutes: override?.retry_backoff_minutes ?? defaults.retry_backoff_minutes,
    escalation: override?.escalation ?? defaults.escalation,
    owner: override?.owner ?? defaults.owner,
  };
}

export function isChronicStaleness(daysOld: number | null, maxAgeDays: number): boolean {
  if (daysOld === null) return true;
  return daysOld >= Math.max(maxAgeDays + 1, maxAgeDays * 2);
}

export function evaluateSla(
  latestDate: string | Date | null | undefined,
  now: Date,
  policy: IndicatorSlaPolicy
): IndicatorSlaEvaluation {
  if (!latestDate) {
    return {
      latest_date: null,
      days_old: null,
      stale: true,
      missing: true,
      max_age_days: policy.max_age_days,
      critical: policy.critical,
      sla_class: policy.class,
    };
  }

  const parsedDate = parseLatestDate(latestDate);
  if (!parsedDate) {
    return {
      latest_date: typeof latestDate === 'string' ? normalizeIsoDateString(latestDate) : null,
      days_old: null,
      stale: true,
      missing: true,
      max_age_days: policy.max_age_days,
      critical: policy.critical,
      sla_class: policy.class,
    };
  }

  const daysOld = (now.getTime() - parsedDate.getTime()) / (24 * 60 * 60 * 1000);
  return {
    latest_date: normalizeIsoDateString(parsedDate.toISOString()),
    days_old: daysOld,
    stale: daysOld > policy.max_age_days,
    missing: false,
    max_age_days: policy.max_age_days,
    critical: policy.critical,
    sla_class: policy.class,
  };
}
