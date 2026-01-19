export interface Env {
  SIGNALS_DB: D1Database
  SIGNALS_BUCKET: R2Bucket
  SIGNALS_KV: KVNamespace
  PUBLIC_BASE_PATH: string
  DEFAULT_LOOKBACK_DAYS: number
  DEFAULT_BASELINE_DAYS: number
  DEFAULT_TOP_N: number
  ENABLE_COMMENTS: number
  ENABLE_RSS: number
  PRICE_PROVIDER: string
  REDDIT_CLIENT_ID?: string
  REDDIT_CLIENT_SECRET?: string
  REDDIT_USER_AGENT?: string
  ADMIN_RUN_TOKEN?: string
}

export const DEFAULTS = {
  lookbackDays: 7,
  baselineDays: 30,
  topN: 10,
  maxPostsPerSubreddit: 150,
  maxCommentsPerPost: 50,
  eps: 1e-6,
}

export function getConfig(env: Env) {
  return {
    publicBasePath: env.PUBLIC_BASE_PATH || "/signals",
    lookbackDays: Number(env.DEFAULT_LOOKBACK_DAYS) || DEFAULTS.lookbackDays,
    baselineDays: Number(env.DEFAULT_BASELINE_DAYS) || DEFAULTS.baselineDays,
    topN: Number(env.DEFAULT_TOP_N) || DEFAULTS.topN,
    enableComments: Number(env.ENABLE_COMMENTS) === 1,
    enableRss: Number(env.ENABLE_RSS) === 1,
    priceProvider: env.PRICE_PROVIDER || "none",
    maxPostsPerSubreddit: DEFAULTS.maxPostsPerSubreddit,
    maxCommentsPerPost: DEFAULTS.maxCommentsPerPost,
    eps: DEFAULTS.eps,
  }
}
