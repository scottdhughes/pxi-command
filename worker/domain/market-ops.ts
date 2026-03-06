import type {
  CalibrationDiagnosticsResponsePayload,
  DecisionGradeComponentStatus,
  DecisionImpactObserveSnapshot,
  DecisionGradeResponse,
  DecisionImpactGovernanceOptions,
  DecisionImpactOpsResponsePayload,
  DecisionImpactResponsePayload,
  EdgeDiagnosticsResponsePayload,
  WorkerRouteContext,
} from '../types';

type MarketOpsDeps = Record<string, any>;

const UTILITY_WINDOW_DAY_OPTIONS = new Set<number>([7, 30]);
const DECISION_IMPACT_WINDOW_DAY_OPTIONS = new Set<number>([30, 90]);
const DECISION_IMPACT_OBSERVE_THRESHOLDS = {
  market_7d_hit_rate_min: 0.52,
  market_30d_hit_rate_min: 0.50,
  market_7d_avg_signed_return_min: 0,
  market_30d_avg_signed_return_min: 0,
  cta_action_rate_pct_min: 2.0,
} as const;
const DECISION_IMPACT_ENFORCE_MIN_SAMPLE_DEFAULT = 30;
const DECISION_IMPACT_ENFORCE_MIN_ACTIONABLE_SESSIONS_DEFAULT = 10;

export async function computeDecisionImpact(
  db: D1Database,
  deps: MarketOpsDeps,
  args: {
    horizon: '7d' | '30d';
    scope: 'market' | 'theme';
    window_days: 30 | 90;
    limit?: number;
    as_of?: string | null;
  },
): Promise<DecisionImpactResponsePayload> {
  const windowDays = DECISION_IMPACT_WINDOW_DAY_OPTIONS.has(args.window_days) ? args.window_days : 30;
  const asOfNow = deps.asIsoDateTime(new Date());
  const requestedAsOf = String(args.as_of || '');
  const effectiveAsOfDate =
    deps.parseIsoDate(requestedAsOf) ||
    deps.parseIsoDate(requestedAsOf.slice(0, 10)) ||
    deps.asIsoDate(new Date());
  const startDate = deps.addCalendarDays(effectiveAsOfDate, -(windowDays - 1));
  const horizonDays = args.horizon === '7d' ? 7 : 30;
  const asOfStartDate = deps.addCalendarDays(startDate, -horizonDays);

  const ledgerRows = await db.prepare(`
    SELECT as_of, theme_id, theme_name, direction
    FROM market_opportunity_item_ledger
    WHERE horizon = ?
      AND published = 1
      AND substr(as_of, 1, 10) >= ?
      AND substr(as_of, 1, 10) <= ?
    ORDER BY as_of DESC, id DESC
  `).bind(args.horizon, asOfStartDate, effectiveAsOfDate).all<{
    as_of: string;
    theme_id: string;
    theme_name: string;
    direction: string;
  }>();

  const proxyIndicatorIds = new Set<string>(['spy_close']);
  if (args.scope === 'theme') {
    for (const row of ledgerRows.results || []) {
      for (const rule of deps.resolveThemeProxyRules(row.theme_id)) {
        proxyIndicatorIds.add(rule.indicator_id);
      }
    }
  }

  const indicatorIdList = [...proxyIndicatorIds];
  const placeholders = indicatorIdList.map(() => '?').join(',');
  const indicatorRows = await db.prepare(`
    SELECT indicator_id, date, value
    FROM indicator_values
    WHERE indicator_id IN (${placeholders})
    ORDER BY indicator_id ASC, date ASC
  `).bind(...indicatorIdList).all<{
    indicator_id: string;
    date: string;
    value: number;
  }>();

  const seriesByIndicator = new Map<string, Array<{ date: string; value: number }>>();
  for (const row of indicatorRows.results || []) {
    const bucket = seriesByIndicator.get(row.indicator_id) || [];
    bucket.push({ date: row.date, value: row.value });
    seriesByIndicator.set(row.indicator_id, bucket);
  }
  const seriesMapByIndicator = new Map<string, Map<string, number>>();
  const latestDateByIndicator = new Map<string, string>();
  for (const [indicatorId, series] of seriesByIndicator.entries()) {
    const map = new Map<string, number>();
    for (const point of series) {
      map.set(point.date, point.value);
    }
    seriesMapByIndicator.set(indicatorId, map);
    const latestDate = deps.latestDateInSeries(series);
    if (latestDate) {
      latestDateByIndicator.set(indicatorId, latestDate <= effectiveAsOfDate ? latestDate : effectiveAsOfDate);
    }
  }

  const latestReferenceDate = (() => {
    const spyLatest = latestDateByIndicator.get('spy_close');
    if (spyLatest) {
      return spyLatest;
    }
    const dates = [...latestDateByIndicator.values()];
    if (dates.length <= 0) return null;
    return dates.sort((a, b) => a.localeCompare(b))[dates.length - 1];
  })();

  let maturedItems = 0;
  let eligibleItems = 0;
  let themeProxyEligibleItems = 0;
  let spyFallbackItems = 0;
  const observations: any[] = [];

  for (const row of ledgerRows.results || []) {
    const direction = row.direction === 'bearish' || row.direction === 'bullish' || row.direction === 'neutral'
      ? row.direction
      : null;
    if (!direction) continue;

    const asOfDate = String(row.as_of || '').slice(0, 10);
    if (!deps.parseIsoDate(asOfDate)) continue;
    if (!latestReferenceDate) continue;

    const maturityDate = deps.addCalendarDays(asOfDate, horizonDays);
    if (maturityDate < startDate || maturityDate > effectiveAsOfDate) continue;
    if (maturityDate > latestReferenceDate) continue;
    maturedItems += 1;

    if (direction === 'neutral') continue;

    let forwardReturnPct: number | null = null;
    let basisKind: 'spy_market' | 'theme_proxy' | 'spy_fallback' = 'spy_market';

    if (args.scope === 'theme') {
      const proxyRules = deps.resolveThemeProxyRules(row.theme_id);
      let weightedReturn = 0;
      let totalWeight = 0;
      for (const rule of proxyRules) {
        const series = seriesMapByIndicator.get(rule.indicator_id);
        const spot = deps.priceOnOrAfterSeries(series, asOfDate, 3);
        const forward = deps.priceOnOrAfterSeries(series, maturityDate, 3);
        if (spot === null || forward === null || spot === 0) {
          continue;
        }
        const rawReturn = ((forward - spot) / spot) * 100;
        const orientedReturn = rule.invert ? -rawReturn : rawReturn;
        const weight = Number.isFinite(rule.weight) && rule.weight > 0 ? rule.weight : 1;
        weightedReturn += orientedReturn * weight;
        totalWeight += weight;
      }
      if (totalWeight > 0) {
        forwardReturnPct = weightedReturn / totalWeight;
        basisKind = 'theme_proxy';
        themeProxyEligibleItems += 1;
      } else {
        const spySeries = seriesMapByIndicator.get('spy_close');
        const spot = deps.priceOnOrAfterSeries(spySeries, asOfDate, 3);
        const forward = deps.priceOnOrAfterSeries(spySeries, maturityDate, 3);
        if (spot === null || forward === null || spot === 0) {
          continue;
        }
        forwardReturnPct = ((forward - spot) / spot) * 100;
        basisKind = 'spy_fallback';
        spyFallbackItems += 1;
      }
    } else {
      const spySeries = seriesMapByIndicator.get('spy_close');
      const spot = deps.priceOnOrAfterSeries(spySeries, asOfDate, 3);
      const forward = deps.priceOnOrAfterSeries(spySeries, maturityDate, 3);
      if (spot === null || forward === null || spot === 0) {
        continue;
      }
      forwardReturnPct = ((forward - spot) / spot) * 100;
      basisKind = 'spy_market';
    }

    if (forwardReturnPct === null) {
      continue;
    }

    const signedReturnPct = direction === 'bullish' ? forwardReturnPct : -forwardReturnPct;
    eligibleItems += 1;
    observations.push({
      theme_id: row.theme_id,
      theme_name: row.theme_name,
      as_of: row.as_of,
      forward_return_pct: forwardReturnPct,
      signed_return_pct: signedReturnPct,
      basis_kind: basisKind,
    });
  }

  const coverageRatio = maturedItems > 0 ? (eligibleItems / maturedItems) : 0;
  const insufficientReasons: string[] = [];
  if (!latestReferenceDate) {
    insufficientReasons.push('missing_spy_close_series');
  }
  if (maturedItems === 0) {
    insufficientReasons.push('no_matured_items');
  } else if (eligibleItems === 0) {
    insufficientReasons.push(args.scope === 'theme' ? 'no_eligible_items_with_proxy' : 'no_eligible_items_with_spy_proxy');
  } else if (coverageRatio < 0.6) {
    insufficientReasons.push(args.scope === 'theme' ? 'low_proxy_coverage' : 'low_spy_proxy_coverage');
  }
  if (args.scope === 'theme' && eligibleItems > 0) {
    if (themeProxyEligibleItems === 0) {
      insufficientReasons.push('theme_proxy_unavailable_using_spy_fallback');
    } else {
      const themeProxyCoverage = themeProxyEligibleItems / eligibleItems;
      if (themeProxyCoverage < 0.6) {
        insufficientReasons.push('low_theme_proxy_coverage');
      }
      if (spyFallbackItems > 0) {
        insufficientReasons.push('partial_theme_proxy_fallback');
      }
    }
  }

  const marketStats = deps.buildDecisionImpactMarketStats(observations);
  const themeStats = args.scope === 'theme'
    ? deps.buildDecisionImpactThemeStats(observations, args.limit ?? 10)
    : [];
  const asOfOutput = args.as_of
    ? deps.asIsoDateTime(new Date(`${effectiveAsOfDate}T00:00:00.000Z`))
    : asOfNow;
  const outcomeBasis = args.scope === 'theme' && themeProxyEligibleItems > 0
    ? 'theme_proxy_blend'
    : 'spy_forward_proxy';

  return {
    as_of: asOfOutput,
    horizon: args.horizon,
    scope: args.scope,
    window_days: windowDays,
    outcome_basis: outcomeBasis,
    market: marketStats,
    themes: themeStats,
    coverage: {
      matured_items: maturedItems,
      eligible_items: eligibleItems,
      coverage_ratio: deps.roundMetric(coverageRatio),
      insufficient_reasons: insufficientReasons,
      theme_proxy_eligible_items: args.scope === 'theme' ? themeProxyEligibleItems : undefined,
      spy_fallback_items: args.scope === 'theme' ? spyFallbackItems : undefined,
    },
  };
}

function evaluateDecisionImpactObserveMode(
  market7: any,
  market30: any,
  utilityFunnel: any,
  governance: DecisionImpactGovernanceOptions,
): DecisionImpactObserveSnapshot {
  const breaches: string[] = [];
  const enforceBreaches: string[] = [];
  const observedDays = Math.max(
    1,
    Math.min(utilityFunnel.window_days, Math.max(0, utilityFunnel.days_observed)),
  );
  const effectiveMinActionableSessions = Math.max(
    1,
    Math.ceil((governance.min_actionable_sessions * observedDays) / utilityFunnel.window_days),
  );

  if (market7.sample_size <= 0) {
    breaches.push('market_7d_insufficient_samples');
  } else if (market7.sample_size < governance.min_sample_size) {
    breaches.push('market_7d_below_enforce_min_sample');
  } else {
    if (market7.hit_rate < DECISION_IMPACT_OBSERVE_THRESHOLDS.market_7d_hit_rate_min) {
      breaches.push('market_7d_hit_rate_breach');
    }
    if (market7.avg_signed_return_pct <= DECISION_IMPACT_OBSERVE_THRESHOLDS.market_7d_avg_signed_return_min) {
      breaches.push('market_7d_avg_signed_return_breach');
    }
  }
  if (market7.sample_size >= governance.min_sample_size) {
    if (market7.hit_rate < DECISION_IMPACT_OBSERVE_THRESHOLDS.market_7d_hit_rate_min) {
      enforceBreaches.push('market_7d_hit_rate_breach');
    }
    if (market7.avg_signed_return_pct <= DECISION_IMPACT_OBSERVE_THRESHOLDS.market_7d_avg_signed_return_min) {
      enforceBreaches.push('market_7d_avg_signed_return_breach');
    }
  }

  if (market30.sample_size <= 0) {
    breaches.push('market_30d_insufficient_samples');
  } else if (market30.sample_size < governance.min_sample_size) {
    breaches.push('market_30d_below_enforce_min_sample');
  } else {
    if (market30.hit_rate < DECISION_IMPACT_OBSERVE_THRESHOLDS.market_30d_hit_rate_min) {
      breaches.push('market_30d_hit_rate_breach');
    }
    if (market30.avg_signed_return_pct <= DECISION_IMPACT_OBSERVE_THRESHOLDS.market_30d_avg_signed_return_min) {
      breaches.push('market_30d_avg_signed_return_breach');
    }
  }
  if (market30.sample_size >= governance.min_sample_size) {
    if (market30.hit_rate < DECISION_IMPACT_OBSERVE_THRESHOLDS.market_30d_hit_rate_min) {
      enforceBreaches.push('market_30d_hit_rate_breach');
    }
    if (market30.avg_signed_return_pct <= DECISION_IMPACT_OBSERVE_THRESHOLDS.market_30d_avg_signed_return_min) {
      enforceBreaches.push('market_30d_avg_signed_return_breach');
    }
  }

  if (utilityFunnel.actionable_sessions <= 0) {
    breaches.push('cta_action_insufficient_sessions');
  } else if (utilityFunnel.actionable_sessions < effectiveMinActionableSessions) {
    breaches.push('cta_action_below_enforce_min_sessions');
  } else if (utilityFunnel.cta_action_rate_pct < DECISION_IMPACT_OBSERVE_THRESHOLDS.cta_action_rate_pct_min) {
    breaches.push('cta_action_rate_breach');
  }
  if (utilityFunnel.actionable_sessions >= effectiveMinActionableSessions) {
    if (utilityFunnel.cta_action_rate_pct < DECISION_IMPACT_OBSERVE_THRESHOLDS.cta_action_rate_pct_min) {
      enforceBreaches.push('cta_action_rate_breach');
    }
  }

  const enforceReady =
    market7.sample_size >= governance.min_sample_size &&
    market30.sample_size >= governance.min_sample_size &&
    utilityFunnel.actionable_sessions >= effectiveMinActionableSessions;

  return {
    enabled: true,
    mode: governance.enforce_enabled ? 'enforce' : 'observe',
    thresholds: {
      market_7d_hit_rate_min: DECISION_IMPACT_OBSERVE_THRESHOLDS.market_7d_hit_rate_min,
      market_30d_hit_rate_min: DECISION_IMPACT_OBSERVE_THRESHOLDS.market_30d_hit_rate_min,
      market_7d_avg_signed_return_min: DECISION_IMPACT_OBSERVE_THRESHOLDS.market_7d_avg_signed_return_min,
      market_30d_avg_signed_return_min: DECISION_IMPACT_OBSERVE_THRESHOLDS.market_30d_avg_signed_return_min,
      cta_action_rate_pct_min: DECISION_IMPACT_OBSERVE_THRESHOLDS.cta_action_rate_pct_min,
    },
    minimum_samples_required: governance.min_sample_size,
    minimum_actionable_sessions_required: effectiveMinActionableSessions,
    configured_minimum_actionable_sessions_required: governance.min_actionable_sessions,
    enforce_ready: enforceReady,
    enforce_breaches: Array.from(new Set(enforceBreaches)),
    enforce_breach_count: Array.from(new Set(enforceBreaches)).length,
    breaches: Array.from(new Set(breaches)),
    breach_count: Array.from(new Set(breaches)).length,
  };
}

export async function buildDecisionImpactOpsResponse(
  db: D1Database,
  deps: MarketOpsDeps,
  windowDays: 30 | 90,
  governance?: DecisionImpactGovernanceOptions,
): Promise<DecisionImpactOpsResponsePayload> {
  const appliedGovernance: DecisionImpactGovernanceOptions = governance || {
    enforce_enabled: false,
    min_sample_size: DECISION_IMPACT_ENFORCE_MIN_SAMPLE_DEFAULT,
    min_actionable_sessions: DECISION_IMPACT_ENFORCE_MIN_ACTIONABLE_SESSIONS_DEFAULT,
  };
  const [market7, market30, themes7, utilityFunnel] = await Promise.all([
    computeDecisionImpact(db, deps, {
      horizon: '7d',
      scope: 'market',
      window_days: windowDays,
    }),
    computeDecisionImpact(db, deps, {
      horizon: '30d',
      scope: 'market',
      window_days: windowDays,
    }),
    computeDecisionImpact(db, deps, {
      horizon: '7d',
      scope: 'theme',
      window_days: windowDays,
      limit: 50,
    }),
    deps.computeUtilityFunnelSummary(db, windowDays),
  ]);

  const themesWithSamples = themes7.themes.filter((theme: any) => theme.sample_size > 0);
  const topPositive = [...themesWithSamples].slice(0, 5);
  const topNegative = [...themesWithSamples]
    .sort((a: any, b: any) => {
      if (a.avg_signed_return_pct !== b.avg_signed_return_pct) {
        return a.avg_signed_return_pct - b.avg_signed_return_pct;
      }
      if (b.sample_size !== a.sample_size) {
        return b.sample_size - a.sample_size;
      }
      return a.theme_id.localeCompare(b.theme_id);
    })
    .slice(0, 5);
  const observeMode = evaluateDecisionImpactObserveMode(
    market7.market,
    market30.market,
    utilityFunnel,
    appliedGovernance,
  );

  return {
    as_of: deps.asIsoDateTime(new Date()),
    window_days: windowDays,
    market_7d: market7.market,
    market_30d: market30.market,
    theme_summary: {
      themes_with_samples: themesWithSamples.length,
      themes_robust: themesWithSamples.filter((theme: any) => theme.quality_band === 'ROBUST').length,
      top_positive: topPositive,
      top_negative: topNegative,
    },
    utility_attribution: {
      actionable_views: utilityFunnel.decision_actionable_views,
      actionable_sessions: utilityFunnel.actionable_sessions,
      cta_action_clicks: utilityFunnel.cta_action_clicks,
      cta_action_rate_pct: utilityFunnel.cta_action_rate_pct,
      no_action_unlock_views: utilityFunnel.no_action_unlock_views,
      decision_events_total: utilityFunnel.decision_events_total,
    },
    observe_mode: observeMode,
  };
}

function calibrationQualityScore(quality: string): number {
  if (quality === 'ROBUST') return 100;
  if (quality === 'LIMITED') return 70;
  return 35;
}

function decisionGradeFromScore(score: number): 'GREEN' | 'YELLOW' | 'RED' {
  if (score >= 85) return 'GREEN';
  if (score >= 70) return 'YELLOW';
  return 'RED';
}

async function computeOpportunityLedgerWindowMetrics(
  db: D1Database,
  deps: MarketOpsDeps,
  windowDays: number,
) {
  const boundedWindowDays = UTILITY_WINDOW_DAY_OPTIONS.has(windowDays) ? windowDays : 30;
  const lookbackExpr = `-${Math.max(0, boundedWindowDays - 1)} days`;
  const rows = await db.prepare(`
    SELECT
      as_of,
      horizon,
      candidate_count,
      published_count,
      suppressed_count,
      quality_filtered_count,
      coherence_suppressed_count,
      data_quality_suppressed_count,
      degraded_reason,
      top_direction_candidate,
      top_direction_published,
      created_at
    FROM market_opportunity_ledger
    WHERE datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
    ORDER BY as_of ASC, created_at ASC, id ASC
  `).bind(lookbackExpr).all<any>();

  const latestByDateHorizon = new Map<string, any>();
  for (const row of rows.results || []) {
    const horizon = row.horizon === '30d' ? '30d' : row.horizon === '7d' ? '7d' : null;
    if (!horizon || !row.as_of) continue;
    const asOfDate = row.as_of.slice(0, 10);
    const key = `${asOfDate}:${horizon}`;
    latestByDateHorizon.set(key, {
      refresh_run_id: null,
      as_of: row.as_of,
      horizon,
      candidate_count: Math.max(0, Math.floor(deps.toNumber(row.candidate_count, 0))),
      published_count: Math.max(0, Math.floor(deps.toNumber(row.published_count, 0))),
      suppressed_count: Math.max(0, Math.floor(deps.toNumber(row.suppressed_count, 0))),
      quality_filtered_count: Math.max(0, Math.floor(deps.toNumber(row.quality_filtered_count, 0))),
      coherence_suppressed_count: Math.max(0, Math.floor(deps.toNumber(row.coherence_suppressed_count, 0))),
      data_quality_suppressed_count: Math.max(0, Math.floor(deps.toNumber(row.data_quality_suppressed_count, 0))),
      degraded_reason: row.degraded_reason || null,
      top_direction_candidate:
        row.top_direction_candidate === 'bullish' || row.top_direction_candidate === 'bearish' || row.top_direction_candidate === 'neutral'
          ? row.top_direction_candidate
          : null,
      top_direction_published:
        row.top_direction_published === 'bullish' || row.top_direction_published === 'bearish' || row.top_direction_published === 'neutral'
          ? row.top_direction_published
          : null,
      created_at: row.created_at || row.as_of,
    });
  }

  const normalizedRows = [...latestByDateHorizon.values()];
  const candidateCountTotal = normalizedRows.reduce((acc: number, row: any) => acc + row.candidate_count, 0);
  const publishedCountTotal = normalizedRows.reduce((acc: number, row: any) => acc + row.published_count, 0);
  const suppressedCountTotal = normalizedRows.reduce((acc: number, row: any) => acc + row.suppressed_count, 0);
  const overSuppressedRows = normalizedRows.filter((row: any) =>
    row.candidate_count > 0 &&
    row.published_count === 0 &&
    row.data_quality_suppressed_count === 0
  ).length;

  const byDate = new Map<string, { d7: any; d30: any }>();
  for (const row of normalizedRows) {
    const dateKey = row.as_of.slice(0, 10);
    const current = byDate.get(dateKey) || { d7: null, d30: null };
    if (row.horizon === '7d') current.d7 = row;
    if (row.horizon === '30d') current.d30 = row;
    byDate.set(dateKey, current);
  }

  const orderedDates = [...byDate.keys()].sort();
  let pairedDays = 0;
  let crossHorizonConflictDays = 0;
  let conflictPersistenceDays = 0;
  for (const dateKey of orderedDates) {
    const pair = byDate.get(dateKey);
    const isPaired = Boolean(pair?.d7 && pair?.d30);
    const hasDirectionalConflict = Boolean(
      pair?.d7 &&
      pair?.d30 &&
      pair.d7.published_count > 0 &&
      pair.d30.published_count > 0 &&
      pair.d7.top_direction_published &&
      pair.d30.top_direction_published &&
      pair.d7.top_direction_published !== pair.d30.top_direction_published
    );
    if (isPaired) {
      pairedDays += 1;
    }
    if (hasDirectionalConflict) {
      crossHorizonConflictDays += 1;
      conflictPersistenceDays += 1;
    } else {
      conflictPersistenceDays = 0;
    }
  }

  return {
    window_days: boundedWindowDays,
    rows_observed: normalizedRows.length,
    candidate_count_total: candidateCountTotal,
    published_count_total: publishedCountTotal,
    publish_rate_pct: candidateCountTotal > 0
      ? Number(((publishedCountTotal / candidateCountTotal) * 100).toFixed(2))
      : 0,
    suppressed_count_total: suppressedCountTotal,
    over_suppressed_rows: overSuppressedRows,
    over_suppression_rate_pct: normalizedRows.length > 0
      ? Number(((overSuppressedRows / normalizedRows.length) * 100).toFixed(2))
      : 0,
    paired_days: pairedDays,
    cross_horizon_conflict_days: crossHorizonConflictDays,
    cross_horizon_conflict_rate_pct: pairedDays > 0
      ? Number(((crossHorizonConflictDays / pairedDays) * 100).toFixed(2))
      : 0,
    conflict_persistence_days: conflictPersistenceDays,
    last_as_of: normalizedRows.length > 0
      ? [...normalizedRows].sort((a: any, b: any) => a.as_of.localeCompare(b.as_of))[normalizedRows.length - 1].as_of
      : null,
  };
}

export async function computeDecisionGradeScorecard(
  db: D1Database,
  deps: MarketOpsDeps,
  windowDays: number,
  governance?: DecisionImpactGovernanceOptions,
): Promise<DecisionGradeResponse> {
  const boundedWindowDays = UTILITY_WINDOW_DAY_OPTIONS.has(windowDays) ? windowDays : 30;
  const lookbackExpr = `-${Math.max(0, boundedWindowDays - 1)} days`;
  const appliedGovernance: DecisionImpactGovernanceOptions = governance || {
    enforce_enabled: false,
    min_sample_size: DECISION_IMPACT_ENFORCE_MIN_SAMPLE_DEFAULT,
    min_actionable_sessions: DECISION_IMPACT_ENFORCE_MIN_ACTIONABLE_SESSIONS_DEFAULT,
  };

  const [freshnessWindow, utilityFunnel, opportunityMetrics, consistencyRows, conviction7d, conviction30d, edgeCalibration, decisionImpactOps] = await Promise.all([
    deps.computeFreshnessSloWindow(db, boundedWindowDays),
    deps.computeUtilityFunnelSummary(db, boundedWindowDays),
    computeOpportunityLedgerWindowMetrics(db, deps, boundedWindowDays),
    db.prepare(`
      SELECT state, COUNT(*) as count
      FROM market_consistency_checks
      WHERE datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', ?)
      GROUP BY state
    `).bind(lookbackExpr).all<{ state: string; count: number | null }>(),
    deps.fetchLatestCalibrationSnapshot(db, 'conviction', '7d'),
    deps.fetchLatestCalibrationSnapshot(db, 'conviction', '30d'),
    deps.fetchLatestCalibrationSnapshot(db, 'edge_quality', null),
    buildDecisionImpactOpsResponse(db, deps, 30, appliedGovernance),
  ]);

  let edgeReport: any = null;
  try {
    edgeReport = await deps.buildEdgeDiagnosticsReport(db, ['7d', '30d']);
  } catch (err) {
    console.warn('Decision-grade edge diagnostics unavailable:', err);
    edgeReport = null;
  }

  const freshnessScore = Number(deps.clamp(0, 100, freshnessWindow.slo_attainment_pct).toFixed(2));
  const freshnessStatus =
    freshnessWindow.days_observed <= 0
      ? 'insufficient'
      : freshnessWindow.days_with_critical_stale === 0 && freshnessWindow.slo_attainment_pct >= 95
        ? 'pass'
        : freshnessWindow.days_with_critical_stale <= 1 && freshnessWindow.slo_attainment_pct >= 90
          ? 'watch'
          : 'fail';

  let passCount = 0;
  let warnCount = 0;
  let failCount = 0;
  for (const row of consistencyRows.results || []) {
    const count = Math.max(0, Math.floor(deps.toNumber(row.count, 0)));
    if (row.state === 'PASS') passCount += count;
    else if (row.state === 'WARN') warnCount += count;
    else if (row.state === 'FAIL') failCount += count;
  }
  const consistencyTotal = passCount + warnCount + failCount;
  const consistencyScore = consistencyTotal > 0
    ? Number(deps.clamp(
      0,
      100,
      100 - ((failCount / consistencyTotal) * 100) - ((warnCount / consistencyTotal) * 35)
    ).toFixed(2))
    : 40;
  const consistencyStatus =
    consistencyTotal <= 0
      ? 'insufficient'
      : failCount > 0
        ? 'fail'
        : warnCount > 0
          ? 'watch'
          : 'pass';

  const conviction7dQuality = deps.computeCalibrationDiagnostics(conviction7d).quality_band;
  const conviction30dQuality = deps.computeCalibrationDiagnostics(conviction30d).quality_band;
  const edgeCalibrationQuality = deps.computeCalibrationDiagnostics(edgeCalibration).quality_band;
  const calibrationScore = Number((
    (calibrationQualityScore(conviction7dQuality) +
      calibrationQualityScore(conviction30dQuality) +
      calibrationQualityScore(edgeCalibrationQuality)) / 3
  ).toFixed(2));
  const calibrationStatus =
    conviction7dQuality === 'INSUFFICIENT' || conviction30dQuality === 'INSUFFICIENT' || edgeCalibrationQuality === 'INSUFFICIENT'
      ? 'fail'
      : conviction7dQuality === 'LIMITED' || conviction30dQuality === 'LIMITED' || edgeCalibrationQuality === 'LIMITED'
        ? 'watch'
        : 'pass';

  let edgeScore = 35;
  let edgeStatus: DecisionGradeComponentStatus = 'insufficient';
  let lowerBoundPositiveHorizons = 0;
  let horizonsObserved = 0;
  let edgeReasons: string[] = [];
  if (edgeReport) {
    horizonsObserved = edgeReport.windows.length;
    lowerBoundPositiveHorizons = edgeReport.windows.filter((window: any) => window.lower_bound_positive).length;
    const leakageFailures = edgeReport.windows.filter((window: any) => !window.leakage_sentinel.pass).length;
    edgeScore = Number(deps.clamp(
      0,
      100,
      50 +
      (edgeReport.promotion_gate.pass ? 25 : -20) +
      (lowerBoundPositiveHorizons * 12) -
      (leakageFailures * 10)
    ).toFixed(2));
    edgeReasons = edgeReport.promotion_gate.reasons;
    edgeStatus = edgeReport.promotion_gate.pass && lowerBoundPositiveHorizons > 0
      ? 'pass'
      : edgeReport.promotion_gate.pass
        ? 'watch'
        : 'fail';
  }

  let opportunityHygieneScore = 100;
  if (opportunityMetrics.publish_rate_pct < 10) opportunityHygieneScore -= 35;
  else if (opportunityMetrics.publish_rate_pct < 20) opportunityHygieneScore -= 20;
  if (opportunityMetrics.over_suppression_rate_pct > 35) opportunityHygieneScore -= 35;
  else if (opportunityMetrics.over_suppression_rate_pct > 20) opportunityHygieneScore -= 20;
  else if (opportunityMetrics.over_suppression_rate_pct > 10) opportunityHygieneScore -= 10;
  if (opportunityMetrics.cross_horizon_conflict_rate_pct > 50) opportunityHygieneScore -= 25;
  else if (opportunityMetrics.cross_horizon_conflict_rate_pct > 30) opportunityHygieneScore -= 15;
  else if (opportunityMetrics.cross_horizon_conflict_rate_pct > 15) opportunityHygieneScore -= 8;
  if (opportunityMetrics.conflict_persistence_days >= 3) opportunityHygieneScore -= 15;
  else if (opportunityMetrics.conflict_persistence_days >= 2) opportunityHygieneScore -= 8;
  const boundedOpportunityHygieneScore = Number(deps.clamp(0, 100, opportunityHygieneScore).toFixed(2));
  const opportunityHygieneStatus =
    opportunityMetrics.rows_observed <= 0
      ? 'insufficient'
      : opportunityMetrics.over_suppression_rate_pct > 35 || opportunityMetrics.cross_horizon_conflict_rate_pct > 45
        ? 'fail'
        : opportunityMetrics.over_suppression_rate_pct > 20 || opportunityMetrics.cross_horizon_conflict_rate_pct > 30 || opportunityMetrics.publish_rate_pct < 10
          ? 'watch'
          : 'pass';

  const utilityTargetFullWindow = boundedWindowDays === 30 ? 25 : 8;
  const observedUtilityDays = Math.max(
    1,
    Math.min(boundedWindowDays, Math.max(0, utilityFunnel.days_observed)),
  );
  const utilityTarget = Math.max(
    1,
    Math.ceil((utilityTargetFullWindow * observedUtilityDays) / boundedWindowDays),
  );
  const utilityProgressPct = Math.min(
    100,
    (utilityFunnel.decision_events_total / utilityTarget) * 100,
  );
  const utilityScoreRaw = (utilityProgressPct * 0.7) + (utilityFunnel.no_action_unlock_coverage_pct * 0.3);
  const utilityScore = Number(deps.clamp(0, 100, utilityScoreRaw).toFixed(2));
  const utilityStatus =
    utilityFunnel.decision_events_total <= 0
      ? 'insufficient'
      : utilityFunnel.decision_events_total >= utilityTarget && utilityFunnel.no_action_unlock_coverage_pct >= 80
        ? 'pass'
        : utilityFunnel.decision_events_total >= Math.ceil(utilityTarget * 0.6) && utilityFunnel.no_action_unlock_coverage_pct >= 60
          ? 'watch'
          : 'fail';

  const weightedScore = Number((
    freshnessScore * 0.25 +
    consistencyScore * 0.20 +
    calibrationScore * 0.20 +
    edgeScore * 0.15 +
    boundedOpportunityHygieneScore * 0.15 +
    utilityScore * 0.05
  ).toFixed(2));
  const grade = decisionGradeFromScore(weightedScore);
  const goLiveBlockers = new Set<string>();
  if (weightedScore < 85) goLiveBlockers.add('score_below_threshold');
  if (freshnessStatus !== 'pass') goLiveBlockers.add('freshness_not_pass');
  if (consistencyStatus === 'fail') goLiveBlockers.add('consistency_fail');
  if (calibrationStatus === 'fail') goLiveBlockers.add('calibration_fail');
  if (!edgeReport) {
    goLiveBlockers.add('edge_diagnostics_unavailable');
  } else if (!edgeReport.promotion_gate.pass) {
    goLiveBlockers.add('edge_promotion_gate_fail');
  }
  if (opportunityHygieneStatus === 'fail') goLiveBlockers.add('opportunity_hygiene_fail');
  if (utilityStatus === 'fail') goLiveBlockers.add('utility_signal_weak');
  if (utilityStatus === 'insufficient') goLiveBlockers.add('utility_signal_insufficient');
  if (!decisionImpactOps.observe_mode.enforce_ready) {
    goLiveBlockers.add('decision_impact_not_enforce_ready');
  }
  for (const breach of decisionImpactOps.observe_mode.breaches) {
    goLiveBlockers.add(`decision_impact_${breach}`);
  }

  const blockers = [...goLiveBlockers];
  const goLiveReady = blockers.length === 0;

  return {
    as_of: deps.asIsoDateTime(new Date()),
    window_days: boundedWindowDays,
    score: weightedScore,
    grade,
    go_live_ready: goLiveReady,
    go_live_blockers: blockers,
    readiness: {
      decision_impact_window_days: decisionImpactOps.window_days,
      decision_impact_enforce_ready: decisionImpactOps.observe_mode.enforce_ready,
      decision_impact_breaches: decisionImpactOps.observe_mode.breaches,
      decision_impact_market_7d_sample_size: decisionImpactOps.market_7d.sample_size,
      decision_impact_market_30d_sample_size: decisionImpactOps.market_30d.sample_size,
      decision_impact_actionable_sessions: decisionImpactOps.utility_attribution.actionable_sessions,
      minimum_samples_required: decisionImpactOps.observe_mode.minimum_samples_required,
      minimum_actionable_sessions_required: decisionImpactOps.observe_mode.minimum_actionable_sessions_required,
    },
    components: {
      freshness: {
        score: freshnessScore,
        status: freshnessStatus,
        slo_attainment_pct: freshnessWindow.slo_attainment_pct,
        days_with_critical_stale: freshnessWindow.days_with_critical_stale,
        days_observed: freshnessWindow.days_observed,
      },
      consistency: {
        score: consistencyScore,
        status: consistencyStatus,
        pass_count: passCount,
        warn_count: warnCount,
        fail_count: failCount,
        total: consistencyTotal,
      },
      calibration: {
        score: calibrationScore,
        status: calibrationStatus,
        conviction_7d: conviction7dQuality,
        conviction_30d: conviction30dQuality,
        edge_quality: edgeCalibrationQuality,
      },
      edge: {
        score: edgeScore,
        status: edgeStatus,
        promotion_gate_pass: edgeReport?.promotion_gate.pass ?? false,
        lower_bound_positive_horizons: lowerBoundPositiveHorizons,
        horizons_observed: horizonsObserved,
        reasons: edgeReasons,
      },
      opportunity_hygiene: {
        score: boundedOpportunityHygieneScore,
        status: opportunityHygieneStatus,
        publish_rate_pct: opportunityMetrics.publish_rate_pct,
        over_suppression_rate_pct: opportunityMetrics.over_suppression_rate_pct,
        cross_horizon_conflict_rate_pct: opportunityMetrics.cross_horizon_conflict_rate_pct,
        conflict_persistence_days: opportunityMetrics.conflict_persistence_days,
        rows_observed: opportunityMetrics.rows_observed,
      },
      utility: {
        score: utilityScore,
        status: utilityStatus,
        decision_events_total: utilityFunnel.decision_events_total,
        no_action_unlock_coverage_pct: utilityFunnel.no_action_unlock_coverage_pct,
        unique_sessions: utilityFunnel.unique_sessions,
      },
    },
  };
}

export async function tryHandleMarketOpsRoute(
  route: WorkerRouteContext,
  deps: MarketOpsDeps,
): Promise<Response | null> {
  const { request, env, url, method, corsHeaders } = route;

  if (url.pathname === '/api/decision-impact' && method === 'GET') {
    if (!deps.isFeatureEnabled(env, 'FEATURE_ENABLE_DECISION_IMPACT', 'ENABLE_DECISION_IMPACT', true)) {
      return Response.json({ error: 'Decision impact disabled' }, { status: 404, headers: corsHeaders });
    }

    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Decision-impact schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    const horizonRaw = (url.searchParams.get('horizon') || '7d').trim().toLowerCase();
    if (horizonRaw !== '7d' && horizonRaw !== '30d') {
      return Response.json({
        error: 'Invalid horizon. Use horizon=7d or horizon=30d',
      }, { status: 400, headers: corsHeaders });
    }
    const horizon = horizonRaw as '7d' | '30d';

    const scopeRaw = (url.searchParams.get('scope') || 'market').trim().toLowerCase();
    if (scopeRaw !== 'market' && scopeRaw !== 'theme') {
      return Response.json({
        error: 'Invalid scope. Use scope=market or scope=theme',
      }, { status: 400, headers: corsHeaders });
    }
    const scope = scopeRaw as 'market' | 'theme';

    const windowRaw = Number.parseInt((url.searchParams.get('window') || '30').trim(), 10);
    if (!DECISION_IMPACT_WINDOW_DAY_OPTIONS.has(windowRaw)) {
      return Response.json({
        error: 'Invalid window. Supported values: 30, 90',
      }, { status: 400, headers: corsHeaders });
    }
    const windowDays = windowRaw as 30 | 90;
    const limitRaw = Number.parseInt((url.searchParams.get('limit') || '10').trim(), 10);
    const limit = Math.max(1, Math.min(50, Number.isFinite(limitRaw) ? limitRaw : 10));

    const asOfRaw = (url.searchParams.get('as_of') || '').trim();
    let asOfDate: string | null = null;
    if (asOfRaw) {
      asOfDate = deps.parseIsoDate(asOfRaw) || deps.parseIsoDate(asOfRaw.slice(0, 10));
      if (!asOfDate) {
        return Response.json({
          error: 'Invalid as_of. Use YYYY-MM-DD or ISO date-time.',
        }, { status: 400, headers: corsHeaders });
      }
    }

    let payload: DecisionImpactResponsePayload | null = null;
    if (asOfDate) {
      payload = await deps.fetchDecisionImpactSnapshotAtOrBefore(
        env.DB,
        horizon,
        scope,
        windowDays,
        asOfDate,
      );
    }

    if (!payload) {
      payload = await computeDecisionImpact(env.DB, deps, {
        horizon,
        scope,
        window_days: windowDays,
        limit,
        as_of: asOfDate,
      });
    }

    const responsePayload: DecisionImpactResponsePayload = {
      ...payload,
      scope,
      horizon,
      window_days: windowDays,
      themes: scope === 'theme'
        ? (payload.themes || []).slice(0, limit)
        : [],
    };

    return Response.json(responsePayload, {
      headers: {
        ...corsHeaders,
        'Cache-Control': 'public, max-age=120',
      },
    });
  }

  if (url.pathname === '/api/ops/decision-impact' && method === 'GET') {
    if (!deps.isFeatureEnabled(env, 'FEATURE_ENABLE_DECISION_IMPACT', 'ENABLE_DECISION_IMPACT', true)) {
      return Response.json({ error: 'Decision impact disabled' }, { status: 404, headers: corsHeaders });
    }

    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Ops decision-impact schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    const windowRaw = Number.parseInt((url.searchParams.get('window') || '30').trim(), 10);
    if (!DECISION_IMPACT_WINDOW_DAY_OPTIONS.has(windowRaw)) {
      return Response.json({
        error: 'Invalid window. Supported values: 30, 90',
      }, { status: 400, headers: corsHeaders });
    }
    const windowDays = windowRaw as 30 | 90;
    const decisionImpactGovernance = deps.resolveDecisionImpactGovernance(env);

    try {
      const payload = await buildDecisionImpactOpsResponse(env.DB, deps, windowDays, decisionImpactGovernance);
      return Response.json(payload, {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'public, max-age=120',
        },
      });
    } catch (err) {
      console.error('Ops decision-impact computation failed:', err);
      return Response.json({ error: 'Decision impact ops unavailable' }, { status: 503, headers: corsHeaders });
    }
  }

  if (url.pathname === '/api/diagnostics/calibration' && method === 'GET') {
    if (!deps.isFeatureEnabled(env, 'FEATURE_ENABLE_CALIBRATION_DIAGNOSTICS', 'ENABLE_CALIBRATION_DIAGNOSTICS', true)) {
      return Response.json({
        error: 'Calibration diagnostics disabled',
      }, { status: 503, headers: corsHeaders });
    }

    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Calibration diagnostics schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    const metricRaw = (url.searchParams.get('metric') || 'conviction').trim().toLowerCase();
    if (metricRaw !== 'conviction' && metricRaw !== 'edge_quality') {
      return Response.json({
        error: 'Invalid metric. Use metric=conviction or metric=edge_quality',
      }, { status: 400, headers: corsHeaders });
    }
    const metric = metricRaw as 'conviction' | 'edge_quality';

    const horizonRaw = (url.searchParams.get('horizon') || '').trim().toLowerCase();
    let horizon: '7d' | '30d' | null = null;
    if (metric === 'conviction') {
      if (horizonRaw !== '7d' && horizonRaw !== '30d') {
        return Response.json({
          error: 'horizon is required for metric=conviction (7d or 30d)',
        }, { status: 400, headers: corsHeaders });
      }
      horizon = horizonRaw;
    }

    const asOfRaw = (url.searchParams.get('as_of') || '').trim();
    let asOfDate: string | null = null;
    if (asOfRaw) {
      asOfDate = deps.parseIsoDate(asOfRaw) || deps.parseIsoDate(asOfRaw.slice(0, 10));
      if (!asOfDate) {
        return Response.json({
          error: 'Invalid as_of. Use YYYY-MM-DD or ISO date-time.',
        }, { status: 400, headers: corsHeaders });
      }
    }

    const snapshot = await deps.fetchCalibrationSnapshotAtOrBefore(env.DB, metric, horizon, asOfDate);
    const diagnostics = deps.computeCalibrationDiagnostics(snapshot);
    const response = snapshot || {
      as_of: asOfDate ? `${asOfDate}T00:00:00.000Z` : deps.asIsoDateTime(new Date()),
      metric,
      horizon,
      basis: metric === 'conviction' ? 'conviction_decile' : 'edge_quality_decile',
      bins: [],
      total_samples: 0,
    };

    const responsePayload: CalibrationDiagnosticsResponsePayload = {
      as_of: response.as_of,
      metric: response.metric,
      horizon: response.horizon,
      basis: response.basis,
      total_samples: response.total_samples,
      bins: response.bins,
      diagnostics,
    };

    return Response.json(responsePayload, {
      headers: {
        ...corsHeaders,
        'Cache-Control': 'public, max-age=60',
      },
    });
  }

  if (url.pathname === '/api/diagnostics/edge' && method === 'GET') {
    if (!deps.isFeatureEnabled(env, 'FEATURE_ENABLE_EDGE_DIAGNOSTICS', 'ENABLE_EDGE_DIAGNOSTICS', true)) {
      return Response.json({
        error: 'Edge diagnostics disabled',
      }, { status: 503, headers: corsHeaders });
    }

    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Edge diagnostics schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    const horizonParam = (url.searchParams.get('horizon') || 'all').trim().toLowerCase();
    let horizons: Array<'7d' | '30d'>;
    if (horizonParam === 'all') {
      horizons = ['7d', '30d'];
    } else if (horizonParam === '7d' || horizonParam === '30d') {
      horizons = [horizonParam];
    } else {
      return Response.json({
        error: 'Invalid horizon. Use horizon=7d, horizon=30d, or horizon=all',
      }, { status: 400, headers: corsHeaders });
    }

    try {
      const report = await deps.buildEdgeDiagnosticsReport(env.DB, horizons) as EdgeDiagnosticsResponsePayload;
      return Response.json(report, {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'public, max-age=60',
        },
      });
    } catch (err) {
      console.error('Edge diagnostics failed:', err);
      return Response.json({ error: 'Edge diagnostics unavailable' }, { status: 503, headers: corsHeaders });
    }
  }

  if (url.pathname === '/api/ops/decision-grade' && method === 'GET') {
    if (request.method !== 'GET') {
      return null;
    }
    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Decision-grade schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    const windowRaw = Number.parseInt((url.searchParams.get('window') || '30').trim(), 10);
    if (!UTILITY_WINDOW_DAY_OPTIONS.has(windowRaw)) {
      return Response.json({
        error: 'Invalid window. Supported values: 7, 30',
      }, { status: 400, headers: corsHeaders });
    }

    try {
      const scorecard = await computeDecisionGradeScorecard(
        env.DB,
        deps,
        windowRaw,
        deps.resolveDecisionImpactGovernance(env),
      );
      return Response.json(scorecard, {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'public, max-age=300',
        },
      });
    } catch (err) {
      console.error('Decision-grade computation failed:', err);
      return Response.json({ error: 'Decision grade unavailable' }, { status: 503, headers: corsHeaders });
    }
  }

  if (url.pathname === '/api/ops/go-live-readiness' && method === 'GET') {
    try {
      await deps.ensureMarketProductSchema(env.DB);
    } catch (err) {
      console.error('Go-live readiness schema guard failed:', err);
      return Response.json({ error: 'Schema initialization failed' }, { status: 503, headers: corsHeaders });
    }

    const windowRaw = Number.parseInt((url.searchParams.get('window') || '30').trim(), 10);
    if (windowRaw !== 30) {
      return Response.json({
        error: 'Invalid window. Supported values: 30',
      }, { status: 400, headers: corsHeaders });
    }

    const scoreWindow = 30;
    try {
      const scorecard = await computeDecisionGradeScorecard(
        env.DB,
        deps,
        scoreWindow,
        deps.resolveDecisionImpactGovernance(env),
      );
      return Response.json({
        as_of: deps.asIsoDateTime(new Date()),
        window_days: windowRaw,
        score_window_days: scoreWindow,
        go_live_ready: scorecard.go_live_ready,
        blockers: scorecard.go_live_blockers,
        grade: {
          score: scorecard.score,
          grade: scorecard.grade,
        },
        readiness: scorecard.readiness,
        components: scorecard.components,
      }, {
        headers: {
          ...corsHeaders,
          'Cache-Control': 'public, max-age=300',
        },
      });
    } catch (err) {
      console.error('Go-live readiness computation failed:', err);
      return Response.json({ error: 'Go-live readiness unavailable' }, { status: 503, headers: corsHeaders });
    }
  }

  return null;
}
