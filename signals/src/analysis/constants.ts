/**
 * Centralized constants for the PXI-Signals analysis pipeline.
 *
 * Grouping magic numbers here improves maintainability and makes
 * the scoring/classification logic easier to tune and understand.
 */

// ─────────────────────────────────────────────────────────────────────────────
// Time Constants
// ─────────────────────────────────────────────────────────────────────────────

/** Number of seconds in a day (86400 = 24 * 60 * 60) */
export const SECONDS_PER_DAY = 86400;

/** Small epsilon to prevent division by zero in rate calculations */
export const RATE_EPSILON = 1e-6;

// ─────────────────────────────────────────────────────────────────────────────
// Scoring Weights
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Weights for combining z-score normalized components into final theme score.
 * Sum should equal 1.0 when all components are available.
 */
export const WEIGHTS = {
  /** Mention velocity: growth ratio + slope (most important signal) */
  velocity: 0.4,
  /** Sentiment shift: VADER compound difference between periods */
  sentiment: 0.2,
  /** Confirmation: cross-subreddit validation minus concentration penalty */
  confirmation: 0.3,
  /** Price metrics: momentum + divergence (optional, redistributed if unavailable) */
  price: 0.1,
} as const;

// ─────────────────────────────────────────────────────────────────────────────
// Classification Thresholds
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Thresholds for computing confidence score (0-4 points).
 * Each condition met adds 1 point to confidence.
 */
export const CONFIDENCE_THRESHOLDS = {
  /** Minimum mentions in lookback period to earn 1 confidence point */
  minMentions: 8,
  /** Minimum unique subreddits to earn 1 confidence point */
  minSubreddits: 3,
  /** Maximum concentration ratio to earn 1 confidence point */
  maxConcentration: 0.5,
} as const;

/**
 * Thresholds for determining timing classification.
 * Timing indicates whether a signal is early, building, or actionable now.
 */
export const TIMING_THRESHOLDS = {
  /** Minimum growth ratio for "Now" timing */
  growthNow: 2.0,
  /** Minimum slope for "Now" timing (not volatile) */
  slopeNow: 0.2,
  /** Concentration above this triggers "Now (volatile)" instead of "Now" */
  concentrationVolatile: 0.6,
  /** Minimum growth ratio for "Building" timing */
  growthBuilding: 1.4,
  /** Minimum growth ratio for "Ongoing" timing */
  growthOngoing: 1.0,
} as const;

/**
 * Thresholds for signal type classification.
 */
export const SIGNAL_TYPE_THRESHOLDS = {
  /** Sentiment shift below this (negative) suggests mean reversion */
  meanReversionSentiment: -0.05,
} as const;

/**
 * Thresholds for identifying risk factors in takeaways.
 */
export const RISK_THRESHOLDS = {
  /** Concentration above this triggers a risk warning */
  highConcentration: 0.6,
  /** Mentions below this triggers a low sample size warning */
  lowMentions: 3,
} as const;

// ─────────────────────────────────────────────────────────────────────────────
// Extraction & Display Limits
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Limits for extracting and displaying data in reports.
 */
export const LIMITS = {
  /** Maximum number of key tickers to display per theme */
  maxTickers: 8,
  /** Maximum number of evidence links to include per theme */
  maxEvidenceLinks: 5,
  /** Number of top themes to feature in takeaways */
  topTakeaways: 3,
  /** Number of top posts used to calculate concentration */
  topPostsForConcentration: 3,
  /** Subreddit weight multiplier in ticker scoring */
  subredditScoreWeight: 2,
} as const;

// ─────────────────────────────────────────────────────────────────────────────
// Ticker Extraction
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Regex pattern for extracting potential ticker symbols from text.
 * Matches 2-5 uppercase letters, optionally prefixed with $.
 */
export const TICKER_REGEX = /\$?[A-Z]{2,5}/g;

/** Minimum length for a valid ticker symbol */
export const TICKER_MIN_LENGTH = 2;

/** Maximum length for a valid ticker symbol */
export const TICKER_MAX_LENGTH = 5;

// ─────────────────────────────────────────────────────────────────────────────
// Confirmation Score
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Divisor for normalizing unique subreddit count in confirmation score.
 * confirmation_score = min(1, max(0, (unique_subreddits / divisor) - concentration))
 */
export const CONFIRMATION_SUBREDDIT_DIVISOR = 3;
