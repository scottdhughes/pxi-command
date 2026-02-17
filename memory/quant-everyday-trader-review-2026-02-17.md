# Quant Product Review: Making PXI Compelling for Everyday Traders
**Date:** 2026-02-17  
**Workspace:** `/Users/scott/pxi`  
**Focus:** Everyday trader utility (clarity, actionability, risk framing, trust, consistency)

## Method and System Snapshot
I reviewed the live product surface plus implementation files across PXI core and Signals.

**Inspected live endpoints (sample):**
- `https://api.pxicommand.com/api/pxi`
- `https://api.pxicommand.com/api/signal`
- `https://api.pxicommand.com/api/predict`
- `https://api.pxicommand.com/api/accuracy`
- `https://api.pxicommand.com/api/ml/accuracy`
- `https://api.pxicommand.com/api/similar`
- `https://api.pxicommand.com/api/backtest`
- `https://pxicommand.com/signals/latest`

**Inspected implementation/docs:**
- `frontend/src/App.tsx`
- `worker/api.ts`
- `worker/schema.sql`
- `worker/wrangler.toml`
- `signals/src/{routes.ts,scheduled.ts,analysis/constants.ts,report/template.ts}`
- `signals/docs/{ARCHITECTURE.md,DEPLOYMENT.md,SCORING.md}`
- `README.md`

---

## Executive Summary
1. PXI has strong quantitative surface area, but everyday-trader UX is fragmented across multiple endpoints and pages without a single “today’s plan” decision output.
2. Current live state already shows trust-friction: `/api/pxi` reports **SOFT** + stale data (`staleCount=18`) while `/api/signal` returns **RISK_OFF** allocation (`0.42`) despite regime showing **RISK_ON**.
3. Signals communication still exposes extreme raw values (e.g., **Velocity 1000.00x**) that are mathematically explainable but cognitively unhelpful for retail traders.
4. Product consistency gap: frontend contains `/brief`, `/opportunities`, `/inbox` flows, but live API responses currently return 404 for corresponding routes on `api.pxicommand.com`.
5. Confidence signaling is inconsistent: `/api/predict` may show high confidence while `/api/ml/accuracy` has very small sample support (e.g., sample size 1 for model-level metrics).
6. Highest-impact near-term work is not new model complexity; it is communication calibration, consistency hardening, and confidence/risk normalization.
7. Build a **single actionable daily trade plan** endpoint and card that reconciles PXI state, signal allocation, ML horizon bias, and data freshness penalties.
8. Convert conviction/score outputs into calibrated probabilities and scenario bands using existing backtest and prediction logs.
9. Add explicit disagreement and stale-data penalties so users can see “edge quality,” not just directional bias.
10. If you execute the top 12 moves below in order, PXI should become materially more understandable, credible, and habit-forming for everyday traders.

---

## 1) What Everyday Traders Care About Most (and current gaps)

### A. Clarity
**Trader question:** “What is the setup today in one sentence?”

**Current gap:** Data is rich, but decision output is split across PXI score, signal type, ML forecasts, similar periods, and signals themes.

### B. Actionability
**Trader question:** “What should I do with position size and timing?”

**Current gap:** `risk_allocation` exists but isn’t consistently framed as an actionable plan with invalidation conditions.

### C. Risk Framing
**Trader question:** “What can go wrong and how bad could it be?”

**Current gap:** Backtests exist but downside framing is not front-and-center in daily decision UX.

### D. Trust
**Trader question:** “Can I trust this today?”

**Current gap (live):** stale data count is visible in `/api/pxi`, but confidence is not systematically discounted for staleness/disagreement.

### E. Consistency
**Trader question:** “Why do pages and messages disagree?”

**Current gap (live):** signal/regime mismatch and feature endpoint availability mismatch create avoidable credibility leakage.

---

## 2) Top 12 Product/Quant Upgrades (prioritized by impact and effort)

| # | Priority | Upgrade | Impact | Effort | User Value | Quant Rationale | Implementation Scope (files/components) | Key Risk | Validation Metric |
|---|---|---|---|---|---|---|---|---|---|
| 1 | P0 | **Daily Trade Plan (single decision object)** | High | M | One-glance “what to do now” | Reconciles state + signal + horizon into one policy output | `worker/api.ts` (new `/api/plan`), `frontend/src/App.tsx` (Plan card) | Over-simplification | % sessions viewing plan card; reduction in conflicting state/signal interpretation support tickets |
| 2 | P0 | **Confidence Engine with penalties** (stale data, model disagreement, regime conflict) | High | M | Confidence becomes honest, not cosmetic | Confidence should be reliability-weighted, not raw directional score | `worker/api.ts` (`calculatePXISignal`, confidence helper), `src/config/indicator-sla.ts`, `frontend/src/App.tsx` | Too conservative in noisy periods | Brier score, reliability curve slope/intercept, calibration drift |
| 3 | P0 | **Regime–Signal Conflict State** (explicit) | High | S | Users understand contradiction immediately | Divergent indicators should be represented as a distinct state | `worker/api.ts` (`/api/signal` payload), `frontend/src/App.tsx` (`SignalIndicator`, badges) | False conflict triggers | Conflict precision (% true instability episodes), user comprehension click-through |
| 4 | P0 | **Ship/enable product endpoints consistently** (`/api/brief`, `/api/opportunities`, `/api/alerts/feed`) | High | M | Removes broken-path friction | Feature availability consistency is prerequisite to trust | `worker/api.ts` route deployment, `worker/wrangler.toml` vars/flags, frontend fallback handling in `App.tsx` | Partial rollout mismatch | 404 rate for these endpoints → 0; API uptime + non-empty payload rate |
| 5 | P1 | **Opportunity Conviction Calibration** (score -> probability band) | High | M | “Conviction 78” becomes interpretable probability | Current weighted blend is useful but not calibrated | `worker/api.ts` (`buildOpportunitySnapshot`), `worker/schema.sql` (calibration table), DB queries from `prediction_log` | Overfit to short regime window | Calibration error (ECE), hit-rate by conviction decile monotonicity |
| 6 | P1 | **Scenario Bands** (best/base/worst for 7d/30d) | High | M | Adds concrete downside context | Use empirical quantiles from historical bucket + similar periods | `worker/api.ts` (`/api/predict` or `/api/plan`), `frontend/src/App.tsx` (scenario panel) | Interval too wide/too narrow | Realized return coverage vs predicted intervals |
| 7 | P1 | **Signals Narrative Normalization** (percentile labels vs raw 1000x velocity emphasis) | Med-High | S-M | Signals become understandable to non-quants | Preserve rank info while reducing outlier cognitive distortion | `signals/src/report/template.ts`, `signals/src/analysis/metrics.ts`, `signals/src/analysis/constants.ts` | Loss of transparency if over-abstracted | Reduced outlier metric displays + improved engagement with evidence links |
| 8 | P1 | **Explainability Delta Card** (“what changed since yesterday”) | Med-High | S | Helps users connect movement to cause | Existing category/indicator movers can be surfaced as causal summary | `worker/api.ts` (`buildBriefSnapshot`, `/api/pxi` payload), `frontend/src/App.tsx` | Noisy mover attribution | % users opening explainability card; lower bounce on volatile days |
| 9 | P1 | **Uncertainty-aware Alerts** (severity weighted by confidence quality) | Med | M | Fewer noisy alerts, better attention allocation | Alert quality should combine signal strength + reliability | `worker/api.ts` (`generateMarketEvents`, `insertMarketEvents`), `worker/schema.sql` (quality score field) | Under-alerting true events | Alert precision/recall proxy, open/click rates, false alert rate |
| 10 | P2 | **Watchlist-to-Theme Mapping** | Med | M | Makes PXI personal to everyday portfolios | Map user tickers to theme/regime context to improve relevance | `frontend/src/App.tsx` (watchlist UI), `worker/api.ts` (mapping endpoint), optional local storage | Entity mapping errors | Watchlist retention, repeat usage, alert relevance feedback |
| 11 | P2 | **Product API Contract Test Suite** | Med | M | Prevents silent frontend/backend drift | Hardens deploy confidence and consistency | New tests in worker package; CI workflow updates in `.github/workflows/ci.yml` | Initial setup time | Zero unexpected API contract regressions reaching production |
| 12 | P2 | **Research-to-Production Quant Pipeline** (hypothesis cards + replay gate) | Med | M | Better future edge with less fragility | Separates idea quality from shipping urgency | `memory/` templates, `signals/tests/fixtures`, replay scripts in `signals/scripts` | Process overhead | % hypotheses that ship; post-ship out-of-sample delta vs baseline |

---

## 3) Everyday Trader UX + Signal Communication

### Communication principle
Every screen should answer, in order:
1. **What is the setup now?**
2. **What action is implied?**
3. **How strong is the edge?**
4. **What invalidates the setup?**

### Concrete UI/content upgrades
1. **Top-of-screen one-liner (always visible):**
   - Example: `Today: Defensive tilt (42% risk). Momentum weakening; high volatility penalty active.`
2. **Action box with explicit sizing language:**
   - `Base risk size: 1.0x → Suggested today: 0.4x`.
3. **Invalidation triggers:**
   - `If PXI closes >50 and VIX percentile <70 for 2 sessions, upgrade from Defensive to Reduced Risk.`
4. **Confidence decomposition (3 bars):**
   - Data quality, model agreement, regime stability.
5. **Horizon separation (7d vs 30d):**
   - Prevent users mixing tactical and swing horizons.
6. **Percentile-first labeling for Signals metrics:**
   - Replace raw emphasis like `1000x` with `Top 1% velocity event vs 12-month history` while retaining raw value in tooltip.
7. **Always show sample size + uncertainty near every probabilistic claim.**
8. **Consistency colors and semantics:**
   - Same color and wording for risk states across PXI, opportunities, and alerts.

### Proposed payload contract for communication
- `setup_summary` (one sentence)
- `action_now` (sizing + horizon)
- `confidence_breakdown` (data/model/regime)
- `risk_band` (best/base/worst)
- `invalidation_rules` (up to 3)

---

## 4) 7 Quick Wins in 14 Days (exact tasks)

1. **Add conflict flag to `/api/signal`**
   - Task: Add `conflict_state` when regime and signal type disagree materially.
   - Files: `worker/api.ts`, `frontend/src/App.tsx`.
   - Validation: unit test for conflict logic + UI rendering check.

2. **Add freshness-adjusted confidence to `/api/pxi` and `/api/signal`**
   - Task: derive confidence discount from `dataFreshness.staleCount` and disagreement penalties.
   - Files: `worker/api.ts`, `src/config/indicator-sla.ts`.
   - Validation: synthetic cases (0 stale vs high stale) produce expected monotonic confidence changes.

3. **Expose a minimal `/api/plan` endpoint**
   - Task: merge existing fields into one plan response with `setup_summary`, `risk_allocation`, `horizon_bias`.
   - Files: `worker/api.ts`, `frontend/src/App.tsx`.
   - Validation: endpoint contract test + frontend integration snapshot.

4. **Normalize Signals display language for outliers**
   - Task: add percentile bucket copy and tooltip raw value for velocity.
   - Files: `signals/src/report/template.ts`, `signals/src/analysis/metrics.ts`.
   - Validation: report render tests + manual smoke on `/signals/latest`.

5. **Add API contract smoke for product routes**
   - Task: CI check for `/api/brief`, `/api/opportunities`, `/api/alerts/feed` status/content-type schema.
   - Files: `.github/workflows/ci.yml`, worker tests/scripts.
   - Validation: CI fails on route mismatch/404.

6. **Add scenario ranges to `/api/predict`**
   - Task: publish 25/50/75th percentile return bands per horizon from historical buckets.
   - Files: `worker/api.ts` (`/api/predict`), potentially `worker/schema.sql` if caching needed.
   - Validation: interval coverage backtest + endpoint schema tests.

7. **Update onboarding/guide to “30-second playbook” mode**
   - Task: add one step focused on concrete daily decisions and invalidation rules.
   - Files: `frontend/src/App.tsx` (`OnboardingModal`).
   - Validation: reduced drop-off through onboarding steps; increased click-through to action pages.

---

## 5) Production-Safe Delivery Guardrails
- Ship all new logic behind feature flags first (existing `isFeatureEnabled` pattern in `worker/api.ts`).
- Stage-first rollout with explicit endpoint contract checks before production deploy.
- Use additive DB migrations only (`worker/schema.sql`) before read/write path changes.
- Require rollback-ready deploy evidence (build SHA + route smoke + parity checks).
- Keep communication changes non-destructive: preserve existing fields, add new fields.

---

## 6) Suggested Validation Stack (for this roadmap)
- **Functional:** endpoint contract tests + frontend render tests for plan/conflict/scenario cards.
- **Quant:** calibration (ECE/Brier), confidence monotonicity under stale/disagreement stress tests.
- **Product:** CTR on plan card, repeat visits, alert open rate, feature adoption by route.
- **Reliability:** API 404 rate for product routes, stale-data incidence trend, conflict-state frequency trend.

---

## 7) Recommended Sequence (practical)
1. Consistency hardening (routes + conflict + confidence penalties).
2. Communication hardening (plan card + scenario bands + signals language).
3. Calibration and personalization (conviction calibration + watchlist mapping).
4. Process hardening (contract tests + research-to-production gate).

This sequence gives the fastest visible user-value uplift while reducing trust leakage before deeper model work.
