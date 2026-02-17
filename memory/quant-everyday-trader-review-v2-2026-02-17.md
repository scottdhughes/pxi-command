# Quant Product Review v2: Everyday Trader Compounding Plan
**Date:** 2026-02-17 10:21 EST  
**Workspace:** `/Users/scott/pxi`  
**Owner:** Taylor (Aineko Quant)  
**Objective:** Make PXI Command more interesting, compelling, and useful for everyday traders without increasing hidden risk.

## 1) Live System Snapshot (ground truth first)

Sampled at 2026-02-17 10:21 EST from public API:

- `GET /api/pxi`
  - `score`: `42.96` (`SOFT`)
  - stale indicators: `18` (`dataFreshness.hasStaleData = true`)
- `GET /api/signal`
  - `signal.risk_allocation`: `0.42`
  - `signal.type`: `null` (missing explicit trading state)
  - `regime.type`: `RISK_ON`
- `GET /api/predict`
  - bucket `40-60`
  - 7d historical win rate: `64.37%` (`n=623`)
  - 30d historical win rate: `74.71%` (`n=601`)
  - `prediction.confidence`: not explicit as a single normalized user-facing metric
- `GET /api/ml/accuracy`
  - total predictions evaluated: `1`
  - model-level sample sizes are extremely small (`n=1`)
- `GET /api/brief`, `/api/opportunities`, `/api/alerts/feed`
  - currently returning `404` on live API

Interpretation:
- Core quant substrate is strong, but user trust is constrained by missing/fragmented decision UX and contract inconsistency.
- Everyday traders need a single decision object plus explicit edge-quality accounting.

## 2) What Will Make PXI More Compelling

### Priority Order (Impact x Feasibility x Trust)

| Rank | Priority | Opportunity | Why it matters for everyday traders | Core risk |
|---|---|---|---|---|
| 1 | P0 | Daily Trade Plan API + card | Converts many endpoints into one "what do I do now?" answer | Over-simplified guidance |
| 2 | P0 | Edge Quality score + confidence penalties | Prevents false certainty when data is stale/conflicted | Too conservative during volatility |
| 3 | P0 | Route consistency for `/brief`, `/opportunities`, `/alerts/feed` | Removes broken-flow friction and restores trust | Schema drift during rollout |
| 4 | P1 | Scenario bands and invalidation rules | Gives downside framing and stop/adjust logic | Bands not calibrated initially |
| 5 | P1 | Signals narrative normalization | Makes reports understandable without dumbing down | Loss of raw metric transparency |
| 6 | P1 | Personal watchlist mapping | Increases repeat usage and relevance | Mapping noise for multi-theme names |

## 3) Detailed Feature Specs (top 3)

### P0-A: Daily Trade Plan (`/api/plan`)
Goal: a single, deterministic payload that answers setup, action, conviction, and invalidation.

Suggested response contract:

```json
{
  "as_of": "2026-02-17T10:21:00Z",
  "setup_summary": "Soft tape, risk budget reduced, stale-data penalty active.",
  "action_now": {
    "horizon": "7d",
    "risk_budget": 0.42,
    "size_label": "reduced",
    "bias": "neutral_to_defensive"
  },
  "edge_quality": 0.54,
  "confidence_breakdown": {
    "data_quality": 0.35,
    "model_agreement": 0.52,
    "regime_stability": 0.74
  },
  "scenario_bands": {
    "d7": {"bear": -2.1, "base": 0.5, "bull": 2.3},
    "d30": {"bear": -4.4, "base": 2.2, "bull": 5.9}
  },
  "invalidation_rules": [
    "If PXI closes above 50 for 2 sessions, upgrade risk budget by one tier.",
    "If stale indicator count remains above threshold, cap risk budget at 0.5."
  ]
}
```

Implementation touchpoints:
- `worker/api.ts`: add `/api/plan` endpoint using current `/api/pxi`, `/api/signal`, `/api/predict` internals.
- `frontend/src/App.tsx`: add top-of-page plan card.
- `frontend/src/App.css`: add clear action and invalidation visual hierarchy.

Acceptance criteria:
- Endpoint returns non-null `setup_summary`, `action_now`, `edge_quality`, and at least one invalidation rule.
- Plan card visible in first viewport on desktop and mobile.
- Existing endpoints remain backward-compatible.

### P0-B: Edge Quality Engine (honest confidence)
Goal: communicate "how trustworthy today’s signal is", not just "direction".

Proposed formula (v1):

```
edge_quality = base_confidence
  - freshness_penalty(stale_count, stale_age_weighted)
  - conflict_penalty(regime_vs_signal_disagreement)
  - small_sample_penalty(ml_sample_size)
```

Implementation touchpoints:
- `worker/api.ts`: add `edge_quality`, `confidence_breakdown`, and explicit `conflict_state`.
- `src/config/indicator-sla.ts`: expose configurable stale thresholds/penalties.
- `frontend/src/App.tsx`: add three-bar decomposition (data/model/regime).

Acceptance criteria:
- `edge_quality` decreases monotonically when stale count rises (all else equal).
- `conflict_state` is explicit and not inferred by users from mixed fields.
- Confidence UI distinguishes "strong signal, low edge quality" from "strong signal, high edge quality".

### P0-C: Route Consistency and Fallback Contracts
Goal: no visible dead paths in core app journeys.

Implementation touchpoints:
- `worker/api.ts`: ensure `/api/brief`, `/api/opportunities`, `/api/alerts/feed` are consistently available or return typed fallback payloads instead of opaque 404.
- `worker/wrangler.toml`: make feature flags explicit per env to avoid accidental drift.
- `frontend/src/App.tsx`: resilient rendering for partial data states.

Acceptance criteria:
- Route 404 rate for the three endpoints goes to zero in staging and production.
- Frontend can render a valid "degraded but usable" state if upstream data is missing.
- Daily digest/alerts flow does not fail when one component payload is missing.

## 4) KPIs and Success Metrics

Primary KPIs:
- Plan-card engagement rate (`plan_card_views / active_sessions`)
- Repeat daily active users (D1 -> D7 retention for logged-in or session cohort)
- Route reliability (`/api/brief`, `/api/opportunities`, `/api/alerts/feed` non-404 success rate)

Quant quality KPIs:
- Edge quality calibration (Brier / reliability)
- Coverage of scenario bands (realized returns within band)
- Signal conflict precision (did flagged conflicts coincide with unstable periods)

Risk controls:
- Never increase risk budget when edge quality is below a threshold.
- Always show sample size next to probabilistic claims.
- Keep old payload fields until clients are migrated.

## 5) 7-Day Delivery Plan (execution-ready)

Day 1:
- Add `/api/plan` backend contract and typed fallback behavior.
- Add smoke checks for new endpoint.

Day 2:
- Ship frontend plan card with action, risk budget, and invalidation rules.
- Add mobile-first layout verification.

Day 3:
- Implement edge quality decomposition + penalties.
- Expose `conflict_state` in `/api/signal`.

Day 4:
- Route consistency hardening for `/api/brief`, `/api/opportunities`, `/api/alerts/feed`.
- Add fallback payload schema guards.

Day 5:
- Add scenario bands to plan and predict payloads.
- Add uncertainty copy in UI.

Day 6:
- Staging validation pass and acceptance review.
- Tune penalties and scenario cut points from staging telemetry.

Day 7:
- Production rollout with rollback checklist.
- Publish first post-rollout KPI snapshot.

## 6) 30-Day Compounding Roadmap

Week 1:
- Deliver P0-A/B/C and stabilize.

Week 2:
- Add scenario calibration tables and monitor coverage drift.

Week 3:
- Signals narrative normalization (percentile-first language + raw-value tooltip).

Week 4:
- Add watchlist-to-theme relevance and begin personalized opportunity ranking.

Expected compounding effect:
- Better clarity -> better trust -> better repeat usage -> better feedback loops for model and product calibration.

## 7) SHIP_QUEUE alignment

Opened/refreshed queue entries:
- `P0-7`: Daily Trade Plan endpoint + UI card
- `P0-8`: Edge Quality engine and explicit conflict-state semantics
- `P0-9`: Route consistency and fallback contracts for brief/opportunities/alerts

These three are the highest-leverage sequence for making PXI feel like a daily trader tool instead of a dashboard of disconnected outputs.
