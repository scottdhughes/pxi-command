# Quant Everyday Trader Review (v2)
**Date:** 2026-02-17 (EST)  
**Workspace:** `/Users/scott/pxi`  
**Prior report reviewed:** `memory/quant-everyday-trader-review-2026-02-17.md`

## Context Re-check (what changed since v1)
This v2 re-check confirms the same core opportunity, with clearer execution priorities:

- **Live tension remains visible:**
  - `/api/pxi` currently reports `score=42.96`, `label=SOFT`, `staleCount=18`.
  - `/api/signal` reports `signal.type=RISK_OFF`, `risk_allocation=0.42`, while `regime.type=RISK_ON`.
- **Signals UI still shows extreme velocity values** (e.g., `1000.00x`) on `/signals/latest`.
- **Frontend has product pages (`/brief`, `/opportunities`, `/inbox`) in `frontend/src/App.tsx`, but live API route consistency is not reliable from user perspective.**
- **Quant signal depth is strong**, but user trust/clarity and action framing are still the bottleneck.

Bottom line: the highest-ROI work is to improve **decision clarity + confidence honesty + route consistency**, then compound into calibration and personalization.

---

## Priority Framework (Impact vs Implementation Risk)
Scoring scale: 1 (low) to 5 (high)

| Rank | Opportunity | User Impact | Impl. Risk | Priority Score (Impact × (6-Risk)) | Why now |
|---|---|---:|---:|---:|---|
| 1 | Decision Engine API (`/api/plan`) + “Today’s Plan” card | 5 | 2 | 20 | Fastest path to clarity/actionability; directly answers “what do I do now?” |
| 2 | Confidence Quality Layer (staleness + disagreement penalties) | 5 | 3 | 15 | Converts model output into trustworthy signal quality, reducing false certainty |
| 3 | Product Surface Consistency Gate (brief/opportunities/inbox) | 5 | 3 | 15 | Broken/partial routes erode trust immediately; this is a reliability UX fix |
| 4 | Scenario Bands (P25/P50/P75 for 7d/30d) | 4 | 2 | 16 | Adds practical risk framing with minimal model complexity |
| 5 | Opportunity Conviction Calibration | 4 | 3 | 12 | Makes conviction interpretable and reduces arbitrary score perception |
| 6 | Signals Outlier Communication (percentile-first) | 4 | 2 | 16 | Keeps alpha while making metrics understandable for retail users |
| 7 | Invalidation Rules Engine | 4 | 3 | 12 | Gives explicit “when I’m wrong” triggers; improves discipline |
| 8 | Alert Severity by Edge Quality | 3 | 3 | 9 | Better signal-to-noise in inbox and digests |
| 9 | Watchlist-to-Theme Mapping | 3 | 3 | 9 | Raises personalization and retention |
| 10 | Product API Contract CI Suite | 3 | 2 | 12 | Prevents future drift between frontend promises and live API behavior |

---

## Top Product Opportunities (signal quality + UX clarity + risk control)

### 1) **Decision Engine API + “Today’s Plan” UX** (Top Priority)
**User value:** A trader gets one coherent output instead of reconciling 5 widgets manually.  
**Quant rationale:** Plan generation is a deterministic aggregation layer over existing signal components; no new alpha claim required.

#### Feature spec
- Add endpoint: `GET /api/plan`
- Return fields:
  - `setup_summary` (1-line narrative)
  - `action_now` (`risk_allocation_target`, horizon bias 7d/30d)
  - `confidence_quality` (`high|medium|low` + numeric score)
  - `risk_band` (P25/P50/P75 expected move)
  - `invalidation_rules` (max 3 explicit conditions)

#### API/UI touchpoints
- API: `worker/api.ts`
- UI: `frontend/src/App.tsx` (new top “Today’s Plan” card)
- Optional schema support: `worker/schema.sql` (if plan snapshots persisted)

#### Risk / failure modes
- Oversimplification can hide nuance in edge cases.
- Mitigation: include “Why” expander with component-level breakdown.

#### KPI + acceptance criteria
- **KPI:** `% of sessions with plan-card interaction` (target +20% vs baseline engagement with top section).
- **KPI:** time-to-first-action proxy (target -30% click path depth to opportunities/signals).
- **Acceptance:** endpoint returns non-null plan fields for latest trading day; plan card renders without blocking legacy cards.

---

### 2) **Confidence Quality Layer (honest confidence)**
**User value:** Avoids overconfidence when data is stale or models disagree.  
**Quant rationale:** Confidence should reflect expected reliability, not just directional strength.

#### Feature spec
Compute `confidence_quality_score` from:
- `data_quality_penalty` (stale indicators / critical stale indicators)
- `model_agreement_penalty` (e.g., xgboost vs lstm divergence)
- `regime_signal_conflict_penalty`
- optional `sample_size_penalty` for low N metrics

#### API/UI touchpoints
- API: `worker/api.ts` (`/api/pxi`, `/api/signal`, new `/api/plan`)
- SLA utility: `src/config/indicator-sla.ts`
- UI: `frontend/src/App.tsx` (confidence badge + breakdown)

#### Risk / failure modes
- Over-penalization can make confidence perpetually low.
- Mitigation: cap penalties and track calibration over rolling windows.

#### KPI + acceptance criteria
- **KPI:** reliability calibration error (ECE) improves by >=15% relative baseline.
- **KPI:** false-high-confidence events (high confidence followed by opposite 7d move) reduced.
- **Acceptance:** monotonic test cases pass (`more stale` => `lower confidence`), conflict state always reduces score by configured minimum.

---

### 3) **Product Surface Consistency Gate**
**User value:** If UI links exist, corresponding API must be available and non-empty.  
**Quant rationale:** Reliability and consistency are preconditions for trust in quant output.

#### Feature spec
- Add CI route-contract checks for:
  - `/api/brief?scope=market`
  - `/api/opportunities?horizon=7d`
  - `/api/alerts/feed`
- Verify expected status, content type, and required fields.
- Add frontend fallback messaging when feature endpoint unavailable.

#### API/UI touchpoints
- API runtime + route config: `worker/api.ts`, `worker/wrangler.toml`
- CI: `.github/workflows/ci.yml`
- UI fallback handling: `frontend/src/App.tsx`

#### Risk / failure modes
- False negatives in CI if route intentionally disabled.
- Mitigation: explicit feature-flag aware checks and staged environment contract profile.

#### KPI + acceptance criteria
- **KPI:** 404 rate on product endpoints -> 0 in production.
- **KPI:** endpoint non-empty payload rate >95% for market days.
- **Acceptance:** CI fails on route drift; release blocked until fixed.

---

### 4) Scenario Bands in Predictive Surfaces
**User value:** Better risk framing than point estimates.

- **Touchpoints:** `worker/api.ts` (`/api/predict`, `/api/plan`), `frontend/src/App.tsx`
- **KPI:** realized return within published band frequency (coverage calibration)
- **Acceptance:** publish P25/P50/P75 for 7d/30d where sample threshold met

### 5) Opportunity Conviction Calibration
**User value:** “Conviction 78” becomes interpretable probability bracket.

- **Touchpoints:** `worker/api.ts` (`buildOpportunitySnapshot`), `worker/schema.sql` (calibration bins/table)
- **KPI:** monotonic hit rate by conviction decile
- **Acceptance:** calibration table generated and used in payload, with fallback when sample too thin

### 6) Signals Communication Refactor (percentile-first)
**User value:** avoids confusion from extreme raw ratios while preserving transparency.

- **Touchpoints:** `signals/src/report/template.ts`, `signals/src/analysis/constants.ts`
- **KPI:** user engagement with signals evidence links; reduction in “what does 1000x mean?” support confusion
- **Acceptance:** UI shows percentile label + raw metric tooltip

### 7) Invalidation Rules Engine
**User value:** explicit “what breaks this setup” discipline.

- **Touchpoints:** `worker/api.ts` (`/api/plan` rule generation), `frontend/src/App.tsx`
- **KPI:** % plans with explicit invalidation criteria (target 100%)
- **Acceptance:** rule templates generated from measurable thresholds only (no vague language)

### 8) Alert Severity Weighted by Edge Quality
**User value:** less noisy inbox and email digest.

- **Touchpoints:** `worker/api.ts` (`generateMarketEvents`), `worker/schema.sql`
- **KPI:** alert precision proxy (user interactions per alert), reduced low-value alert volume
- **Acceptance:** severity score combines event type + confidence quality + freshness status

---

## 7-Day Delivery Plan (practical, production-safe)

### Day 1 — Contract and scope lock
- Define `/api/plan` JSON contract and confidence-quality formula (weights + caps).
- Add implementation checklist and rollback notes.
- Exit criteria: reviewed contract doc committed in `memory/` + task list opened.

### Day 2 — API implementation (phase 1)
- Implement `/api/plan` in `worker/api.ts` (using existing PXI/signal/predict data).
- Add confidence-quality fields to `/api/pxi` and `/api/signal` payloads.
- Exit criteria: local endpoint smoke responses valid.

### Day 3 — UI integration (phase 1)
- Add “Today’s Plan” card at top of home route in `frontend/src/App.tsx`.
- Add confidence breakdown and conflict-state badges.
- Exit criteria: UI loads with fallback behavior if `/api/plan` unavailable.

### Day 4 — Route consistency hardening
- Add CI contract checks for brief/opportunities/alerts feed availability.
- Add frontend route fallback notices for disabled/unavailable product endpoints.
- Exit criteria: CI fails on route drift; graceful UX when unavailable.

### Day 5 — Scenario bands + invalidation rules
- Extend `/api/plan` and/or `/api/predict` with P25/P50/P75 + invalidation rules.
- Exit criteria: payload includes bands and rules with sample-size guardrails.

### Day 6 — Signals communication cleanup
- Add percentile-first labels for velocity extremes on signals report template.
- Exit criteria: `/signals/latest` shows normalized communication with raw-value transparency.

### Day 7 — Verify, canary, release
- Stage deployment + smoke checks + manual UX review.
- Production rollout with rollback checkpoint.
- Exit criteria: post-deploy endpoint checks green and plan card live.

---

## 30-Day Compounding Roadmap

### Week 1 (Foundation)
- Ship top-3 priorities: `/api/plan`, confidence quality, route consistency gate.
- Outcome: clarity + trust baseline materially improved.

### Week 2 (Risk communication)
- Add scenario bands, invalidation rules, and confidence decomposition UX.
- Outcome: better risk framing; fewer ambiguous calls.

### Week 3 (Calibration)
- Implement conviction calibration and monitor decile monotonicity.
- Outcome: probabilities become interpretable and measurable.

### Week 4 (Retention and quality loops)
- Add watchlist mapping + uncertainty-aware alerts.
- Add weekly KPI review process and thresholds for rollback/tuning.
- Outcome: more personalized and sticky trader workflow.

---

## KPI Dashboard (recommended minimum)

### User outcomes
- Plan-card engagement rate
- Opportunity-card CTR from home
- Return visit rate (7-day)

### Trust/reliability outcomes
- Product endpoint 404 rate
- Data freshness penalty incidence
- Regime-signal conflict frequency + resolution lag

### Quant quality outcomes
- Confidence calibration (ECE/Brier)
- Scenario band coverage rates
- Conviction decile monotonicity

---

## Acceptance Criteria Summary (must-pass before calling this “done”)
1. `/api/plan` returns valid payload for latest day and is consumed by frontend without regressions.
2. Confidence quality visibly decreases under stale-data and model-disagreement stress tests.
3. Product route contract checks in CI prevent deploys with missing `/api/brief`, `/api/opportunities`, `/api/alerts/feed`.
4. Signals report no longer relies on raw extreme values alone for user interpretation.
5. Rollback instructions documented for each shipped change.

---

## Expected Value and Failure Modes (explicit)

### Expected value (conservative)
- **Clarity uplift:** measurable increase in first-action clicks due to single plan card.
- **Trust uplift:** fewer contradictory interpretations from explicit conflict + confidence quality.
- **Risk control uplift:** reduced decision errors from confidence overstatement and missing downside framing.

### Failure modes
- Formula overfitting in confidence penalties.
- UX overload if too many new widgets are added at once.
- Route contract checks blocking legitimate feature-flag states.

### Controls
- Keep formulas simple + test monotonicity.
- Prefer one prominent card over many minor panels.
- Make CI checks environment/feature-flag aware.
