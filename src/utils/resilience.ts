/**
 * Resilience utilities for data fetching
 * - Exponential backoff retry
 * - Circuit breaker pattern
 * - Request timeout handling
 */

// ============== Retry with Exponential Backoff ==============

interface RetryOptions {
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  backoffMultiplier?: number;
  onRetry?: (attempt: number, error: Error, delayMs: number) => void;
}

const DEFAULT_RETRY_OPTIONS: Required<Omit<RetryOptions, 'onRetry'>> = {
  maxAttempts: 3,
  baseDelayMs: 1000,
  maxDelayMs: 10000,
  backoffMultiplier: 2,
};

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export async function withRetry<T>(
  fn: () => Promise<T>,
  options: RetryOptions = {}
): Promise<T> {
  const opts = { ...DEFAULT_RETRY_OPTIONS, ...options };
  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= opts.maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));

      if (attempt === opts.maxAttempts) {
        break;
      }

      // Calculate delay with exponential backoff + jitter
      const exponentialDelay = opts.baseDelayMs * Math.pow(opts.backoffMultiplier, attempt - 1);
      const jitter = Math.random() * 0.3 * exponentialDelay; // 0-30% jitter
      const delay = Math.min(exponentialDelay + jitter, opts.maxDelayMs);

      if (opts.onRetry) {
        opts.onRetry(attempt, lastError, delay);
      }

      await sleep(delay);
    }
  }

  throw lastError;
}

// ============== Circuit Breaker ==============

interface CircuitBreakerState {
  failures: number;
  lastFailure: number;
  isOpen: boolean;
}

interface CircuitBreakerOptions {
  failureThreshold?: number;  // failures before opening
  resetTimeoutMs?: number;    // time before trying again
  onStateChange?: (sourceName: string, isOpen: boolean) => void;
}

const DEFAULT_CIRCUIT_OPTIONS: Required<Omit<CircuitBreakerOptions, 'onStateChange'>> = {
  failureThreshold: 5,
  resetTimeoutMs: 5 * 60 * 1000, // 5 minutes
};

// In-memory circuit state (per source)
const circuitStates = new Map<string, CircuitBreakerState>();

export function getCircuitState(sourceName: string): CircuitBreakerState {
  if (!circuitStates.has(sourceName)) {
    circuitStates.set(sourceName, {
      failures: 0,
      lastFailure: 0,
      isOpen: false,
    });
  }
  return circuitStates.get(sourceName)!;
}

export function recordSuccess(sourceName: string): void {
  const state = getCircuitState(sourceName);
  state.failures = 0;
  state.isOpen = false;
}

export function recordFailure(
  sourceName: string,
  options: CircuitBreakerOptions = {}
): boolean {
  const opts = { ...DEFAULT_CIRCUIT_OPTIONS, ...options };
  const state = getCircuitState(sourceName);

  state.failures++;
  state.lastFailure = Date.now();

  if (state.failures >= opts.failureThreshold) {
    if (!state.isOpen) {
      state.isOpen = true;
      opts.onStateChange?.(sourceName, true);
      console.warn(`Circuit OPEN for ${sourceName} after ${state.failures} failures`);
    }
    return true; // Circuit is now open
  }

  return false;
}

export function isCircuitOpen(
  sourceName: string,
  options: CircuitBreakerOptions = {}
): boolean {
  const opts = { ...DEFAULT_CIRCUIT_OPTIONS, ...options };
  const state = getCircuitState(sourceName);

  if (!state.isOpen) {
    return false;
  }

  // Check if reset timeout has passed
  const timeSinceLastFailure = Date.now() - state.lastFailure;
  if (timeSinceLastFailure >= opts.resetTimeoutMs) {
    // Half-open: allow one request through
    state.isOpen = false;
    state.failures = Math.floor(state.failures / 2); // Reduce failure count
    opts.onStateChange?.(sourceName, false);
    console.log(`Circuit HALF-OPEN for ${sourceName}, allowing retry`);
    return false;
  }

  return true;
}

export function resetCircuit(sourceName: string): void {
  circuitStates.delete(sourceName);
}

// ============== Combined Resilient Fetch ==============

interface ResilientFetchOptions extends RetryOptions, CircuitBreakerOptions {
  sourceName: string;
  timeoutMs?: number;
}

export async function resilientFetch<T>(
  fn: () => Promise<T>,
  options: ResilientFetchOptions
): Promise<T> {
  const { sourceName, timeoutMs = 30000, ...retryOpts } = options;

  // Check circuit breaker first
  if (isCircuitOpen(sourceName, options)) {
    throw new Error(`Circuit breaker open for ${sourceName}, skipping request`);
  }

  try {
    // Wrap with timeout
    const timeoutPromise = new Promise<never>((_, reject) => {
      setTimeout(() => reject(new Error(`Timeout after ${timeoutMs}ms`)), timeoutMs);
    });

    const result = await withRetry(
      () => Promise.race([fn(), timeoutPromise]),
      {
        ...retryOpts,
        onRetry: (attempt, error, delay) => {
          console.log(`  ↻ Retry ${attempt} for ${sourceName} in ${Math.round(delay)}ms: ${error.message}`);
        },
      }
    );

    recordSuccess(sourceName);
    return result;
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    recordFailure(sourceName, options);
    throw error;
  }
}

// ============== Staleness Check ==============

export interface StalenessInfo {
  isStale: boolean;
  daysSinceUpdate: number;
  lastUpdate: Date | null;
}

export function checkStaleness(
  lastUpdateDate: Date | string | null,
  thresholdDays: number = 2
): StalenessInfo {
  if (!lastUpdateDate) {
    return { isStale: true, daysSinceUpdate: Infinity, lastUpdate: null };
  }

  const lastUpdate = typeof lastUpdateDate === 'string'
    ? new Date(lastUpdateDate)
    : lastUpdateDate;

  const now = new Date();
  const diffMs = now.getTime() - lastUpdate.getTime();
  const daysSinceUpdate = diffMs / (1000 * 60 * 60 * 24);

  return {
    isStale: daysSinceUpdate > thresholdDays,
    daysSinceUpdate,
    lastUpdate,
  };
}

// ============== Gap Detection ==============

export interface GapInfo {
  hasGaps: boolean;
  gaps: Array<{ startDate: string; endDate: string; daysMissing: number }>;
  totalMissingDays: number;
}

export function detectGaps(
  dates: string[],
  maxGapDays: number = 5
): GapInfo {
  if (dates.length < 2) {
    return { hasGaps: false, gaps: [], totalMissingDays: 0 };
  }

  const sorted = [...dates].sort();
  const gaps: GapInfo['gaps'] = [];
  let totalMissingDays = 0;

  for (let i = 1; i < sorted.length; i++) {
    const prevDate = new Date(sorted[i - 1]);
    const currDate = new Date(sorted[i]);
    const diffDays = Math.round((currDate.getTime() - prevDate.getTime()) / (1000 * 60 * 60 * 24));

    // Weekends (2-day gaps) are expected, only flag larger gaps
    if (diffDays > 3) { // More than a weekend
      const daysMissing = diffDays - 1;
      if (daysMissing > 0) {
        gaps.push({
          startDate: sorted[i - 1],
          endDate: sorted[i],
          daysMissing,
        });
        totalMissingDays += daysMissing;
      }
    }
  }

  return {
    hasGaps: gaps.length > 0,
    gaps,
    totalMissingDays,
  };
}

// ============== Fetch Status Summary ==============

export interface FetchSummary {
  source: string;
  success: boolean;
  recordsCount: number;
  error?: string;
  retryCount?: number;
  circuitOpen?: boolean;
  staleness?: StalenessInfo;
}

export function createFetchSummary(
  source: string,
  success: boolean,
  recordsCount: number = 0,
  error?: string
): FetchSummary {
  const state = getCircuitState(source);
  return {
    source,
    success,
    recordsCount,
    error,
    retryCount: state.failures,
    circuitOpen: state.isOpen,
  };
}
