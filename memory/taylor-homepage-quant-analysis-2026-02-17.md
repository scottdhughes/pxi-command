# Taylor Quant Review: PXI Homepage Decision Clarity
Date: 2026-02-17 11:59 EST
Scope: `/Users/scott/pxi` homepage (`/`) + live API behavior

## Executive Take
Yes, we should change the homepage. The current page is information-rich but decision-inconsistent.
Right now users can see:
- `RISK_ON` regime (from `/api/pxi`),
- `RISK_OFF` action signal (from `/api/plan` + `/api/signal`),
- `risk_on` brief posture (from `/api/brief`),
- mostly `bullish` opportunities (from `/api/opportunities`).

That creates high cognitive load and low trust, even when the quant logic is individually defensible.

## Ground Truth (Current Live Snapshot)
Observed on 2026-02-17:
- `/api/plan`: `primary_signal=RISK_OFF`, `risk_allocation_target=0.42`, `conflict_state=CONFLICT`, `degraded_reason=stale_inputs,limited_calibration_sample`.
- `/api/signal`: `signal.type=RISK_OFF`, `regime.type=RISK_ON`, `volatility_percentile=87`, adjustment includes high-volatility penalty.
- `/api/brief?scope=market`: `risk_posture=risk_on` with summary text “Regime risk on; posture risk-on.”
- `/api/opportunities?horizon=7d`: top opportunities are `bullish` with `INSUFFICIENT` calibration quality.

## Why This Happens (Code-Level)
1. Homepage renders multiple independent decision systems at once:
- `RegimeBadge` from `/api/pxi`: `/Users/scott/pxi/frontend/src/App.tsx:3941`
- `TodayPlanCard` from `/api/plan`: `/Users/scott/pxi/frontend/src/App.tsx:3944`
- `SignalIndicator` from `/api/signal`: `/Users/scott/pxi/frontend/src/App.tsx:3999`
- `BriefCompactCard` + `OpportunityPreview`: `/Users/scott/pxi/frontend/src/App.tsx:4013`, `/Users/scott/pxi/frontend/src/App.tsx:4014`
- All fetched in one parallel batch: `/Users/scott/pxi/frontend/src/App.tsx:3568`

2. Brief posture logic is not signal-consistent.
- `mapRiskPosture` uses regime OR score thresholds (not tactical allocation): `/Users/scott/pxi/worker/api.ts:1623`
- Brief uses that posture directly: `/Users/scott/pxi/worker/api.ts:2213`, `/Users/scott/pxi/worker/api.ts:2237`

3. Signal logic applies volatility and regime penalties separately.
- `calculatePXISignal` applies high-volatility haircut and sets `signal_type` from final allocation: `/Users/scott/pxi/worker/api.ts:4179`

Result: the page is mixing a structural regime lens, a tactical signal lens, and a narrative brief lens without a canonical arbitration layer.

## What To Show On Home (Best Tool Stack)
Use a 3-tier model. Keep power, reduce conflict.

### Tier 1 (Above the Fold): Decision Tools Only
1. **Canonical Stance Card** (single source of truth)
- Fields: stance (`RISK_ON`/`RISK_OFF`/`MIXED`), risk allocation target, horizon bias, top 2 invalidation triggers.
- If conflict exists, show `MIXED` (not simultaneous risk-on and risk-off labels).
- Source should be `/api/plan` + explicit arbitration output.

2. **Edge Reliability Card**
- Fields: edge quality label/score, calibration quality, CI band, sample size, stale-input penalty.
- Always display uncertainty and sample size next to confidence claims.

3. **Scenario + Downside Card**
- Fields: 7d/30d bear-base-bull + expected shortfall style downside summary.
- Focus on “what can hurt me” first, then upside.

### Tier 2 (Collapsed by Default): Context Tools
4. **Regime Diagnostics** (expanded view)
- Keep indicator-vote details and rationale.
- Do not make this the default action cue.

5. **Opportunity Radar**
- Show only top 1-2 opportunities initially.
- Grey down recommendations when calibration quality is `INSUFFICIENT`.

6. **Freshness/Health**
- Keep stale indicator list and SLA status visible but compact.

### Tier 3 (Research / Power User)
7. Similar periods
8. Historical backtest buckets
9. Full ML model decomposition

These remain accessible, but not first-screen decision drivers.

## What To Remove Or Reframe On Home
1. Remove simultaneous primary labels from multiple models (regime, brief posture, signal action) in first view.
2. Reframe brief posture as context text, not a competing action directive.
3. Prevent bullish opportunities from appearing as strong calls when calibration quality is insufficient.
4. Demote low-sample ML accuracy metrics (`n=1`) from prominent placement.

## Quant Research Basis (Primary Sources)
1. **Attention drives suboptimal retail trading behavior**
- Barber, Odean. “All That Glitters: The Effect of Attention and News on the Buying Behavior of Individual and Institutional Investors.”
- Link: https://doi.org/10.1093/rfs/hhn035
- Implication: avoid salience-heavy, conflicting action cues on the main screen.

2. **Frequent evaluation increases myopic risk behavior**
- Benartzi, Thaler. “Myopic Loss Aversion and the Equity Premium Puzzle.”
- Link: https://doi.org/10.2307/2118511
- Implication: separate tactical vs strategic horizons and avoid noisy over-updates in primary panel.

3. **Volatility-managed exposure improves risk-adjusted outcomes**
- Moreira, Muir. “Volatility-Managed Portfolios.”
- Link: https://www.nber.org/papers/w22208
- Implication: retain explicit volatility/risk-budget sizing as a core tool.

4. **Decision support improves when uncertainty is explicitly represented**
- Miller et al. “Conformal Prediction Sets Improve Human Decision-Making through Better Uncertainty Communication.”
- Link: https://proceedings.mlr.press/v266/miller25a.html
- Implication: show calibrated intervals and coverage, not point estimates alone.

5. **Probability forecasts should be judged with proper scoring rules**
- Gneiting, Raftery. “Strictly Proper Scoring Rules, Prediction, and Estimation.”
- Link: https://sites.stat.washington.edu/raftery/Research/PDF/Gneiting2007jasa.pdf
- Implication: calibrate and monitor confidence outputs formally (Brier/reliability).

6. **Tail-risk coherent risk framing**
- Acerbi, Tasche. “On the coherence of expected shortfall.”
- Link: https://arxiv.org/abs/cond-mat/0104295
- Implication: include downside-tail framing in primary risk panel.

## Recommended Homepage Arbitration Model
Add a canonical `policy_state` object in backend and drive homepage from it.

Proposed logic:
1. Start from tactical signal allocation (`calculatePXISignal`).
2. Apply reliability caps:
- If `edge_quality.label=LOW` OR stale count above threshold, cap allocation and label `MIXED`.
- If calibration quality is `INSUFFICIENT`, cap conviction display strength and add explicit warning.
3. Keep regime as contextual (slow-state) field only.
4. Ensure brief posture mirrors policy_state, not separate rule set.

## Implementation Plan (Targeted)
1. Add `policy_state` to `/api/plan` and use it as homepage canonical stance.
- File: `/Users/scott/pxi/worker/api.ts`

2. Make brief posture derive from policy_state (or at minimum signal+edge quality), not `mapRiskPosture` OR-rule.
- File: `/Users/scott/pxi/worker/api.ts`

3. On homepage, show one top action banner and move regime/brief/opportunities into secondary modules.
- File: `/Users/scott/pxi/frontend/src/App.tsx`

4. Add UI guardrails for low-confidence recommendations:
- no “strong” visual treatment when calibration quality `!= ROBUST`.
- File: `/Users/scott/pxi/frontend/src/App.tsx`

5. Add a consistency contract test:
- fail if homepage payload simultaneously exposes contradictory primary stance labels without `MIXED` conflict state.
- Files: `/Users/scott/pxi/scripts/api-product-contract-check.sh`, `/Users/scott/pxi/.github/workflows/ci.yml`

## Success Criteria
- First-screen contradiction rate (risk-on + risk-off simultaneously) -> 0 unless explicitly labeled `MIXED`.
- Users can answer “what do I do now?” in <10 seconds from homepage.
- Increase in repeat visits to `/` with lower bounce.
- Maintain or improve risk-adjusted performance while reducing action noise.
