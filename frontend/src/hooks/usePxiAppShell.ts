import { useCallback, useEffect, useRef, useState } from 'react'

import { fetchApi, getOrCreateUtilitySessionId, utilityDecisionEventForState } from '../lib/api'
import { applyRouteMetadata, getRouteFetchPlan, normalizeRoute, type AppRoute } from '../lib/routes'
import type {
  AlertsApiResponse,
  AlertsFeedResponse,
  BacktestData,
  BriefData,
  CalibrationDiagnosticsResponse,
  DecisionImpactResponse,
  EdgeDiagnosticsResponse,
  EnsembleData,
  HistoryDataPoint,
  MLAccuracyApiResponse,
  MLAccuracyData,
  OpsDecisionImpactResponse,
  OpportunitiesResponse,
  PXIData,
  PlanData,
  PredictionData,
  SignalData,
  SignalsData,
  SignalsRunSummary,
  SimilarPeriodsData,
  UtilityEventType,
} from '../lib/types'

function parseMLAccuracy(api: MLAccuracyApiResponse): MLAccuracyData | null {
  const parsePercent = (value?: string | null): number | null => {
    if (!value || typeof value !== 'string') return null
    const parsed = parseFloat(value.replace('%', ''))
    return Number.isFinite(parsed) ? parsed : null
  }

  const d7 = api.metrics?.ensemble?.d7 ?? null
  const d30 = api.metrics?.ensemble?.d30 ?? null

  return {
    as_of: api.as_of || null,
    coverage: api.coverage || {
      total_predictions: api.total_predictions || 0,
      evaluated_count: api.evaluated_count || 0,
      pending_count: api.pending_count || 0,
    },
    coverage_quality: api.coverage_quality || 'INSUFFICIENT',
    minimum_reliable_sample: typeof api.minimum_reliable_sample === 'number' ? api.minimum_reliable_sample : 30,
    unavailable_reasons: Array.isArray(api.unavailable_reasons) ? api.unavailable_reasons : [],
    rolling_7d: {
      direction_accuracy: parsePercent(d7?.direction_accuracy),
      sample_size: d7?.sample_size || 0,
      mae: d7 ? parseFloat(d7.mean_absolute_error) : null,
    },
    rolling_30d: {
      direction_accuracy: parsePercent(d30?.direction_accuracy),
      sample_size: d30?.sample_size || 0,
      mae: d30 ? parseFloat(d30.mean_absolute_error) : null,
    },
    all_time: {
      direction_accuracy_7d: parsePercent(d7?.direction_accuracy),
      direction_accuracy_30d: parsePercent(d30?.direction_accuracy),
      total_predictions: api.total_predictions,
    },
  }
}

export function usePxiAppShell() {
  const [route, setRoute] = useState<AppRoute>(() => {
    if (typeof window === 'undefined') return '/'
    return normalizeRoute(window.location.pathname)
  })

  const [data, setData] = useState<PXIData | null>(null)
  const [prediction, setPrediction] = useState<PredictionData | null>(null)
  const [signal, setSignal] = useState<SignalData | null>(null)
  const [ensemble, setEnsemble] = useState<EnsembleData | null>(null)
  const [mlAccuracy, setMlAccuracy] = useState<MLAccuracyData | null>(null)
  const [historyData, setHistoryData] = useState<HistoryDataPoint[]>([])
  const [historyRange, setHistoryRange] = useState<'7d' | '30d' | '90d'>('30d')
  const [showOnboarding, setShowOnboarding] = useState(false)
  const [alertsData, setAlertsData] = useState<AlertsApiResponse | null>(null)
  const [selectedCategory, setSelectedCategory] = useState<string | null>(null)
  const [signalsData, setSignalsData] = useState<SignalsData | null>(null)
  const [similarData, setSimilarData] = useState<SimilarPeriodsData | null>(null)
  const [backtestData, setBacktestData] = useState<BacktestData | null>(null)
  const [planData, setPlanData] = useState<PlanData | null>(null)
  const [briefData, setBriefData] = useState<BriefData | null>(null)
  const [briefDecisionImpact, setBriefDecisionImpact] = useState<DecisionImpactResponse | null>(null)
  const [opportunitiesData, setOpportunitiesData] = useState<OpportunitiesResponse | null>(null)
  const [opportunitiesDecisionImpact, setOpportunitiesDecisionImpact] = useState<DecisionImpactResponse | null>(null)
  const [opsDecisionImpact, setOpsDecisionImpact] = useState<OpsDecisionImpactResponse | null>(null)
  const [opportunityDiagnostics, setOpportunityDiagnostics] = useState<CalibrationDiagnosticsResponse | null>(null)
  const [edgeDiagnostics, setEdgeDiagnostics] = useState<EdgeDiagnosticsResponse | null>(null)
  const [opportunityHorizon, setOpportunityHorizon] = useState<'7d' | '30d'>('7d')
  const [alertsFeed, setAlertsFeed] = useState<AlertsFeedResponse | null>(null)
  const [showSubscribeModal, setShowSubscribeModal] = useState(false)
  const [subscriptionNotice, setSubscriptionNotice] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [menuOpen, setMenuOpen] = useState(false)
  const menuRef = useRef<HTMLDivElement>(null)
  const utilitySessionIdRef = useRef<string>(getOrCreateUtilitySessionId())
  const utilitySeenRef = useRef<Set<string>>(new Set())

  const trackUtilityEvent = useCallback((eventType: UtilityEventType, args?: {
    route?: AppRoute
    actionabilityState?: 'ACTIONABLE' | 'WATCH' | 'NO_ACTION' | null
    metadata?: Record<string, unknown>
    dedupeKey?: string
  }) => {
    const dedupeKey = args?.dedupeKey?.trim()
    if (dedupeKey && utilitySeenRef.current.has(dedupeKey)) {
      return
    }
    if (dedupeKey) {
      utilitySeenRef.current.add(dedupeKey)
    }

    const payload = {
      session_id: utilitySessionIdRef.current,
      event_type: eventType,
      route: args?.route || route,
      actionability_state: args?.actionabilityState ?? null,
      metadata: args?.metadata || null,
    }

    fetchApi('/api/metrics/utility-event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      keepalive: true,
    }).catch(() => {
      if (dedupeKey) {
        utilitySeenRef.current.delete(dedupeKey)
      }
    })
  }, [route])

  const handleOpportunityCtaIntent = useCallback((args: {
    asOf: string
    horizon: '7d' | '30d'
    actionabilityState: 'ACTIONABLE' | 'WATCH' | 'NO_ACTION'
  }) => {
    const dedupeKey = `cta_action_click:${utilitySessionIdRef.current}:${args.asOf}:${args.horizon}`
    trackUtilityEvent('cta_action_click', {
      route: '/opportunities',
      actionabilityState: args.actionabilityState,
      metadata: {
        as_of: args.asOf,
        horizon: args.horizon,
        route: '/opportunities',
        actionability_state: args.actionabilityState,
        scope: 'market_theme',
        source: 'opportunities',
      },
      dedupeKey,
    })
  }, [trackUtilityEvent])

  const navigateTo = useCallback((nextRoute: AppRoute) => {
    const path = normalizeRoute(nextRoute)
    if (typeof window !== 'undefined') {
      if (normalizeRoute(window.location.pathname) !== path) {
        window.history.pushState({}, '', path)
      }
      setRoute(path)
    }
    setMenuOpen(false)
  }, [])

  useEffect(() => {
    applyRouteMetadata(route)
  }, [route])

  useEffect(() => {
    trackUtilityEvent('session_start', {
      route,
      metadata: { entry_route: route },
      dedupeKey: 'session_start',
    })
  }, [route, trackUtilityEvent])

  useEffect(() => {
    if (route !== '/' || !planData) {
      return
    }

    const actionabilityState = planData.actionability_state || (planData.opportunity_ref?.eligible_count === 0 ? 'NO_ACTION' : 'WATCH')
    const dedupeBase = `${planData.as_of}:${actionabilityState}:${route}`

    trackUtilityEvent('plan_view', {
      route,
      actionabilityState,
      metadata: {
        as_of: planData.as_of,
        cross_horizon_state: planData.cross_horizon?.state || null,
        consistency_state: planData.consistency.state,
      },
      dedupeKey: `plan_view:${dedupeBase}`,
    })

    trackUtilityEvent(utilityDecisionEventForState(actionabilityState), {
      route,
      actionabilityState,
      metadata: {
        as_of: planData.as_of,
        source: 'plan',
      },
      dedupeKey: `decision_view:${dedupeBase}`,
    })

    if (actionabilityState === 'NO_ACTION') {
      trackUtilityEvent('no_action_unlock_view', {
        route,
        actionabilityState,
        metadata: {
          as_of: planData.as_of,
          source: 'plan',
          thresholds_communicated: true,
          reason_codes: planData.actionability_reason_codes || [],
        },
        dedupeKey: `unlock_view:${dedupeBase}`,
      })
    }
  }, [planData, route, trackUtilityEvent])

  useEffect(() => {
    if (route !== '/opportunities' || !opportunitiesData) {
      return
    }

    const actionabilityState = opportunitiesData.actionability_state || (opportunitiesData.items.length > 0 ? 'WATCH' : 'NO_ACTION')
    const dedupeBase = `${opportunitiesData.as_of}:${opportunityHorizon}:${actionabilityState}:${route}`

    trackUtilityEvent('opportunities_view', {
      route,
      actionabilityState,
      metadata: {
        as_of: opportunitiesData.as_of,
        horizon: opportunityHorizon,
        published_count: opportunitiesData.items.length,
        suppressed_count: opportunitiesData.suppressed_count,
        ttl_state: opportunitiesData.ttl_state || 'unknown',
      },
      dedupeKey: `opportunities_view:${dedupeBase}`,
    })

    trackUtilityEvent(utilityDecisionEventForState(actionabilityState), {
      route,
      actionabilityState,
      metadata: {
        as_of: opportunitiesData.as_of,
        horizon: opportunityHorizon,
        source: 'opportunities',
      },
      dedupeKey: `decision_view:${dedupeBase}`,
    })

    if (actionabilityState === 'NO_ACTION') {
      trackUtilityEvent('no_action_unlock_view', {
        route,
        actionabilityState,
        metadata: {
          as_of: opportunitiesData.as_of,
          horizon: opportunityHorizon,
          source: 'opportunities',
          thresholds_communicated: true,
          reason_codes: opportunitiesData.actionability_reason_codes || [],
        },
        dedupeKey: `unlock_view:${dedupeBase}`,
      })
    }
  }, [opportunitiesData, opportunityHorizon, route, trackUtilityEvent])

  useEffect(() => {
    try {
      const hasSeenOnboarding = localStorage.getItem('pxi_onboarding_complete')
      if (!hasSeenOnboarding) {
        localStorage.setItem('pxi_onboarding_complete', 'true')
      }
    } catch {
      // Ignore localStorage failures in private browsing environments.
    }
  }, [])

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setMenuOpen(false)
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  useEffect(() => {
    const syncRoute = () => setRoute(normalizeRoute(window.location.pathname))
    window.addEventListener('popstate', syncRoute)
    return () => window.removeEventListener('popstate', syncRoute)
  }, [])

  useEffect(() => {
    if (typeof window === 'undefined') {
      return
    }

    const params = new URLSearchParams(window.location.search)
    const verifyToken = params.get('verify_token')
    const unsubscribeToken = params.get('unsubscribe_token')
    if (!verifyToken && !unsubscribeToken) {
      return
    }

    const finalize = () => {
      const cleanUrl = `${window.location.pathname}`
      window.history.replaceState({}, '', cleanUrl)
      setRoute(normalizeRoute(window.location.pathname))
    }

    const run = async () => {
      try {
        if (verifyToken) {
          const response = await fetchApi('/api/alerts/subscribe/verify', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ token: verifyToken }),
          })
          if (!response.ok) {
            const payload = await response.json().catch(() => ({ error: 'Verification failed' }))
            throw new Error((payload as { error?: string }).error || 'Verification failed')
          }
          setSubscriptionNotice('Email alerts verified. Daily digest is active.')
        } else if (unsubscribeToken) {
          const response = await fetchApi('/api/alerts/unsubscribe', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ token: unsubscribeToken }),
          })
          if (!response.ok) {
            const payload = await response.json().catch(() => ({ error: 'Unsubscribe failed' }))
            throw new Error((payload as { error?: string }).error || 'Unsubscribe failed')
          }
          setSubscriptionNotice('You have been unsubscribed from PXI digest emails.')
        }
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : 'Token action failed'
        setSubscriptionNotice(message)
      } finally {
        finalize()
      }
    }

    void run()
  }, [])

  useEffect(() => {
    const fetchPlan = getRouteFetchPlan(route)
    const hasWork = Object.values(fetchPlan).some(Boolean)
    const isHomeRoute = route === '/'

    if (!hasWork) {
      setLoading(false)
      setError(null)
      return
    }

    const fetchData = async () => {
      try {
        if (isHomeRoute) {
          setLoading(true)
        }

        const [
          pxiRes,
          signalRes,
          planRes,
          predRes,
          ensembleRes,
          accuracyRes,
          historyRes,
          alertsRes,
          briefRes,
          briefImpactRes,
          oppRes,
          oppImpactRes,
          oppOpsImpactRes,
          oppDiagRes,
          edgeDiagRes,
          inboxRes,
        ] = await Promise.all([
          fetchPlan.pxi ? fetchApi('/api/pxi') : Promise.resolve(null),
          fetchPlan.signal ? fetchApi('/api/signal').catch(() => null) : Promise.resolve(null),
          fetchPlan.plan ? fetchApi('/api/plan').catch(() => null) : Promise.resolve(null),
          fetchPlan.prediction ? fetchApi('/api/predict').catch(() => null) : Promise.resolve(null),
          fetchPlan.ensemble ? fetchApi('/api/ml/ensemble').catch(() => null) : Promise.resolve(null),
          fetchPlan.accuracy ? fetchApi('/api/ml/accuracy').catch(() => null) : Promise.resolve(null),
          fetchPlan.history ? fetchApi('/api/history?days=90').catch(() => null) : Promise.resolve(null),
          fetchPlan.alertsHistory ? fetchApi('/api/alerts?limit=50').catch(() => null) : Promise.resolve(null),
          fetchPlan.brief ? fetchApi('/api/brief?scope=market').catch(() => null) : Promise.resolve(null),
          fetchPlan.brief ? fetchApi('/api/decision-impact?horizon=7d&scope=market&window=30').catch(() => null) : Promise.resolve(null),
          fetchPlan.opportunities ? fetchApi(`/api/opportunities?horizon=${opportunityHorizon}&limit=20`).catch(() => null) : Promise.resolve(null),
          fetchPlan.opportunities ? fetchApi(`/api/decision-impact?horizon=${opportunityHorizon}&scope=theme&window=30&limit=10`).catch(() => null) : Promise.resolve(null),
          fetchPlan.opportunities ? fetchApi('/api/ops/decision-impact?window=30').catch(() => null) : Promise.resolve(null),
          fetchPlan.opportunities ? fetchApi(`/api/diagnostics/calibration?metric=conviction&horizon=${opportunityHorizon}`).catch(() => null) : Promise.resolve(null),
          fetchPlan.opportunities ? fetchApi(`/api/diagnostics/edge?horizon=${opportunityHorizon}`).catch(() => null) : Promise.resolve(null),
          fetchPlan.inbox ? fetchApi('/api/alerts/feed?limit=50').catch(() => null) : Promise.resolve(null),
        ])

        if (fetchPlan.pxi) {
          if (!pxiRes?.ok) throw new Error('Failed to fetch')
          const pxiJson = await pxiRes.json() as PXIData
          setData(pxiJson)
        }

        if (signalRes?.ok) {
          const signalJson = await signalRes.json() as SignalData & { error?: string }
          if (!signalJson.error) {
            setSignal(signalJson)
          }
        }

        if (planRes?.ok) {
          const planJson = await planRes.json() as PlanData
          if (planJson.setup_summary && planJson.action_now) {
            setPlanData({
              ...planJson,
              actionability_state: planJson.actionability_state || (planJson.opportunity_ref?.eligible_count === 0 ? 'NO_ACTION' : 'WATCH'),
              actionability_reason_codes: Array.isArray(planJson.actionability_reason_codes) ? planJson.actionability_reason_codes : [],
              action_now: {
                ...planJson.action_now,
                raw_signal_allocation_target: planJson.action_now.raw_signal_allocation_target ?? planJson.action_now.risk_allocation_target,
                risk_allocation_basis: planJson.action_now.risk_allocation_basis || 'penalized_playbook_target',
              },
              policy_state: planJson.policy_state ? {
                ...planJson.policy_state,
                rationale_codes: planJson.policy_state.rationale_codes || [],
              } : undefined,
              uncertainty: planJson.uncertainty || {
                headline: null,
                flags: {
                  stale_inputs: false,
                  limited_calibration: false,
                  limited_scenario_sample: false,
                },
              },
              consistency: planJson.consistency
                ? {
                    ...planJson.consistency,
                    components: planJson.consistency.components || {
                      base_score: 100,
                      structural_penalty: 0,
                      reliability_penalty: 0,
                    },
                  }
                : {
                    score: 100,
                    state: 'PASS',
                    violations: [],
                    components: {
                      base_score: 100,
                      structural_penalty: 0,
                      reliability_penalty: 0,
                    },
                  },
              trader_playbook: planJson.trader_playbook || {
                recommended_size_pct: { min: 25, target: 50, max: 65 },
                scenarios: [],
                benchmark_follow_through_7d: {
                  hit_rate: null,
                  sample_size: 0,
                  unavailable_reason: 'insufficient_sample',
                },
              },
              cross_horizon: planJson.cross_horizon
                ? {
                    ...planJson.cross_horizon,
                    rationale_codes: planJson.cross_horizon.rationale_codes || [],
                  }
                : undefined,
              decision_stack: planJson.decision_stack
                ? {
                    ...planJson.decision_stack,
                    cta_state: planJson.decision_stack.cta_state || (planJson.opportunity_ref?.eligible_count === 0 ? 'NO_ACTION' : 'WATCH'),
                  }
                : undefined,
            })
          }
        }

        if (predRes?.ok) {
          const predJson = await predRes.json() as PredictionData & { error?: string }
          if (!predJson.error) {
            setPrediction(predJson)
          }
        }

        if (ensembleRes?.ok) {
          const ensembleJson = await ensembleRes.json() as EnsembleData & { error?: string }
          if (!ensembleJson.error) {
            setEnsemble(ensembleJson)
          }
        }

        if (accuracyRes?.ok) {
          const accuracyJson = await accuracyRes.json() as MLAccuracyApiResponse
          if (!accuracyJson.error) {
            const parsed = parseMLAccuracy(accuracyJson)
            if (parsed) {
              setMlAccuracy(parsed)
            }
          }
        }

        if (historyRes?.ok) {
          const historyJson = await historyRes.json() as { data?: HistoryDataPoint[]; error?: string }
          if (historyJson.data && Array.isArray(historyJson.data)) {
            setHistoryData(historyJson.data)
          }
        }

        if (alertsRes?.ok) {
          const alertsJson = await alertsRes.json() as AlertsApiResponse
          if (alertsJson.alerts) {
            setAlertsData(alertsJson)
          }
        }

        if (briefRes?.ok) {
          const briefJson = await briefRes.json() as BriefData
          if (briefJson.summary) {
            setBriefData({
              ...briefJson,
              policy_state: briefJson.policy_state || {
                stance: 'MIXED',
                risk_posture: 'neutral',
                conflict_state: 'MIXED',
                base_signal: 'REDUCED_RISK',
                regime_context: 'TRANSITION',
                rationale: 'fallback',
                rationale_codes: ['fallback'],
              },
              source_plan_as_of: briefJson.source_plan_as_of || briefJson.as_of,
              contract_version: briefJson.contract_version || 'legacy',
              consistency: briefJson.consistency || {
                score: 100,
                state: 'PASS',
                violations: [],
              },
              degraded_reason: briefJson.degraded_reason || null,
            })
          }
        }

        if (briefImpactRes?.ok) {
          const impactJson = await briefImpactRes.json() as DecisionImpactResponse
          if (impactJson?.market && impactJson.scope === 'market') {
            setBriefDecisionImpact(impactJson)
          }
        } else if (fetchPlan.brief) {
          setBriefDecisionImpact(null)
        }

        if (oppRes?.ok) {
          const oppJson = await oppRes.json() as OpportunitiesResponse
          if (Array.isArray(oppJson.items)) {
            const qualityFilteredCount = Number.isFinite(oppJson.quality_filtered_count as number) ? Number(oppJson.quality_filtered_count) : 0
            const coherenceSuppressedCount = Number.isFinite(oppJson.coherence_suppressed_count as number) ? Number(oppJson.coherence_suppressed_count) : 0
            const suppressedCount = Number.isFinite(oppJson.suppressed_count) ? oppJson.suppressed_count : 0
            const dataAgeSeconds = Number.isFinite(oppJson.data_age_seconds as number) ? Number(oppJson.data_age_seconds) : null
            const overdueSeconds = Number.isFinite(oppJson.overdue_seconds as number) ? Number(oppJson.overdue_seconds) : null

            setOpportunitiesData({
              ...oppJson,
              suppressed_count: suppressedCount,
              quality_filtered_count: qualityFilteredCount,
              coherence_suppressed_count: coherenceSuppressedCount,
              suppression_by_reason: oppJson.suppression_by_reason || {
                coherence_failed: coherenceSuppressedCount,
                quality_filtered: qualityFilteredCount,
                data_quality_suppressed: oppJson.degraded_reason === 'suppressed_data_quality' ? suppressedCount : 0,
              },
              quality_filter_rate: Number.isFinite(oppJson.quality_filter_rate as number) ? Number(oppJson.quality_filter_rate) : 0,
              coherence_fail_rate: Number.isFinite(oppJson.coherence_fail_rate as number) ? Number(oppJson.coherence_fail_rate) : 0,
              actionability_state: oppJson.actionability_state || (oppJson.items.length > 0 ? 'WATCH' : 'NO_ACTION'),
              actionability_reason_codes: Array.isArray(oppJson.actionability_reason_codes) ? oppJson.actionability_reason_codes : [],
              cta_enabled: typeof oppJson.cta_enabled === 'boolean' ? oppJson.cta_enabled : oppJson.items.length > 0,
              cta_disabled_reasons: Array.isArray(oppJson.cta_disabled_reasons) ? oppJson.cta_disabled_reasons : [],
              data_age_seconds: dataAgeSeconds,
              ttl_state: oppJson.ttl_state || 'unknown',
              next_expected_refresh_at: typeof oppJson.next_expected_refresh_at === 'string' ? oppJson.next_expected_refresh_at : null,
              overdue_seconds: overdueSeconds,
            })
          }
        }

        if (oppImpactRes?.ok) {
          const impactJson = await oppImpactRes.json() as DecisionImpactResponse
          if (impactJson?.market && impactJson.scope === 'theme') {
            setOpportunitiesDecisionImpact(impactJson)
          }
        } else if (fetchPlan.opportunities) {
          setOpportunitiesDecisionImpact(null)
        }

        if (oppOpsImpactRes?.ok) {
          const opsImpactJson = await oppOpsImpactRes.json() as OpsDecisionImpactResponse
          if (opsImpactJson?.observe_mode && opsImpactJson?.utility_attribution) {
            setOpsDecisionImpact(opsImpactJson)
          }
        } else if (fetchPlan.opportunities) {
          setOpsDecisionImpact(null)
        }

        if (oppDiagRes?.ok) {
          const diagnosticsJson = await oppDiagRes.json() as CalibrationDiagnosticsResponse
          if (diagnosticsJson?.diagnostics && typeof diagnosticsJson.total_samples === 'number') {
            setOpportunityDiagnostics(diagnosticsJson)
          }
        } else if (fetchPlan.opportunities) {
          setOpportunityDiagnostics(null)
        }

        if (edgeDiagRes?.ok) {
          const edgeJson = await edgeDiagRes.json() as EdgeDiagnosticsResponse
          if (Array.isArray(edgeJson.windows) && edgeJson.promotion_gate) {
            setEdgeDiagnostics(edgeJson)
          }
        } else if (fetchPlan.opportunities) {
          setEdgeDiagnostics(null)
        }

        if (inboxRes?.ok) {
          const inboxJson = await inboxRes.json() as AlertsFeedResponse
          if (Array.isArray(inboxJson.alerts)) {
            setAlertsFeed(inboxJson)
          }
        }

        if (fetchPlan.signalsDetail) {
          try {
            const signalsApiUrl = '/signals/api/runs'
            const signalsRunsRes = await fetch(`${signalsApiUrl}?status=ok`)
            if (signalsRunsRes.ok) {
              const runsJson = await signalsRunsRes.json() as { runs: SignalsRunSummary[] }
              if (runsJson.runs && runsJson.runs.length > 0) {
                const latestRunId = runsJson.runs[0].id
                const signalsDetailRes = await fetch(`${signalsApiUrl}/${latestRunId}`)
                if (signalsDetailRes.ok) {
                  const signalsJson = await signalsDetailRes.json() as SignalsData
                  setSignalsData(signalsJson)
                }
              }
            }
          } catch {
            // Signals are optional, don't fail
          }
        }

        if (fetchPlan.similar) {
          try {
            const similarRes = await fetchApi('/api/similar')
            if (similarRes.ok) {
              const similarJson = await similarRes.json() as SimilarPeriodsData
              if (similarJson.similar_periods) {
                setSimilarData(similarJson)
              }
            }
          } catch {
            // Similar periods are optional
          }
        }

        if (fetchPlan.backtest) {
          try {
            const backtestRes = await fetchApi('/api/backtest')
            if (backtestRes.ok) {
              const backtestJson = await backtestRes.json() as BacktestData
              if (backtestJson.bucket_analysis) {
                setBacktestData(backtestJson)
              }
            }
          } catch {
            // Backtest is optional
          }
        }

        setError(null)
      } catch (err: unknown) {
        if (isHomeRoute) {
          const message = err instanceof Error ? err.message : 'Unknown error'
          setError(message)
        }
      } finally {
        if (isHomeRoute) {
          setLoading(false)
        }
      }
    }

    void fetchData()
    if (!fetchPlan.poll) {
      return
    }

    const interval = setInterval(() => {
      void fetchData()
    }, 5 * 60 * 1000)

    return () => clearInterval(interval)
  }, [opportunityHorizon, route])

  return {
    route,
    data,
    prediction,
    signal,
    ensemble,
    mlAccuracy,
    historyData,
    historyRange,
    setHistoryRange,
    showOnboarding,
    setShowOnboarding,
    alertsData,
    selectedCategory,
    setSelectedCategory,
    signalsData,
    similarData,
    backtestData,
    planData,
    briefData,
    briefDecisionImpact,
    opportunitiesData,
    opportunitiesDecisionImpact,
    opsDecisionImpact,
    opportunityDiagnostics,
    edgeDiagnostics,
    opportunityHorizon,
    setOpportunityHorizon,
    alertsFeed,
    showSubscribeModal,
    setShowSubscribeModal,
    subscriptionNotice,
    setSubscriptionNotice,
    loading,
    error,
    menuOpen,
    setMenuOpen,
    menuRef,
    navigateTo,
    handleOpportunityCtaIntent,
  }
}
