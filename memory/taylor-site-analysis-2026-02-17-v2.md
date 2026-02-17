# Taylor Site Analysis (Quant/Product Pass) — 2026-02-17

## Scope
Live review of:
- https://pxicommand.com
- https://api.pxicommand.com/api/{pxi,plan,signal,brief,opportunities,alerts/feed,accuracy,ml/accuracy}
- https://pxicommand.com/signals/api/{version,health,accuracy,predictions}

Run time:
- 2026-02-17T17:47:36Z to 2026-02-17T17:48:22Z

## Current State (What Users See)
1. Home decision state is internally mixed:
- `/api/plan`: `policy_state.stance=MIXED`, `risk_posture=neutral`, `primary_signal=RISK_OFF`, `risk_allocation_target=0.42`.
- `rationale`: `regime_signal_conflict,stale_inputs,limited_calibration`.

2. Regime and tactical disagree:
- `/api/pxi`: `regime.type=RISK_ON`.
- `/api/signal`: `signal.type=RISK_OFF`, `conflict_state=CONFLICT`, volatility percentile 87.

3. Freshness drag is material:
- `/api/pxi`: `staleCount=17`.
- `/api/plan`: `degraded_reason=stale_inputs,limited_calibration_sample`.
- `/api/alerts/feed`: only one alert and it is a critical freshness warning.

4. Confidence scaffolding exists but is weakly informative today:
- `/api/plan` edge calibration `quality=INSUFFICIENT`, `sample_size_7d=0`.
- `/api/opportunities` calibration largely `INSUFFICIENT`, sample size 0.
- Expected/adverse move fields in opportunities are null.

5. Cross-surface coherence gap still visible:
- `/api/plan` says neutral/MIXED.
- `/api/brief` still says `risk_posture=risk_on` and summary text says "Regime risk on; posture risk-on.".
- This can still read as contradiction to users even after the homepage UI cleanup.

6. Performance/analytics credibility fields are sparse:
- `/api/accuracy`: key top-level fields null.
- `/api/ml/accuracy`: overall metrics null, no periods.
- `/signals/api/accuracy`: several top-level metadata fields null though `overall` block is populated.

## Taylor’s Quant View
Core issue is no longer UI clutter; it is **state coherence and confidence quality**.

Users can handle complex markets, but they cannot trust a product when:
- posture changes by endpoint,
- confidence is mostly null/insufficient,
- and freshness warnings dominate without explicit action hierarchy.

The platform should act as a decision engine with one canonical answer and explicit uncertainty, not many partial answers.

## Highest-Impact Recommendations

### P0 (Do now)
1. Establish one canonical decision object and enforce it across home widgets:
- Source all posture/stance chips from `/api/plan.policy_state` only.
- Force `/api/brief` to mirror plan stance/posture after each refresh.
- Add hard contract assertion: if plan stance is MIXED/neutral, brief cannot publish `risk_on` unless rationale explicitly indicates override.

2. Add explicit uncertainty headline on homepage when degraded:
- If `degraded_reason` includes `stale_inputs` or calibration is not ROBUST, show first-screen subtitle:
  - "Signal quality reduced: stale inputs + limited calibration."
- Make this the top contextual sentence so users understand why mixed posture exists.

3. Convert freshness warning into an operator status panel:
- Surface stale count, top 3 stale indicators, and age.
- Show ETA or next scheduled refresh time.
- This turns a scary warning into an operationally interpretable signal.

### P1 (Next)
4. Replace null-heavy fields with explicit `unavailable_reason` payloads:
- For opportunities expected move, calibration gaps, and ML accuracy.
- Avoid empty/null ambiguity in trader-facing UI.

5. Normalize calibration output semantics:
- For zero sample bins, return null probabilities and CI instead of numeric zeros.
- Keep quality `INSUFFICIENT`, sample_size=0, and provide reason `insufficient_sample`.

6. Add compact "Decision Card" hierarchy:
- Decision (stance + allocation) -> Confidence (quality + CI + sample) -> Why (3 drivers) -> Risk limits (invalidation rules).
- Keep all other panels secondary.

### P2 (Compounding value)
7. Introduce a trader utility layer:
- Position-sizing suggestion by volatility regime.
- Scenario response table (if VIX percentile > 85, do X; if conflict clears, do Y).
- Optional benchmark overlay: SPY realized follow-through vs prior plan stance.

8. Add consistency score and alert:
- Internal metric across endpoints (`plan`, `brief`, `signal`, `pxi`) to detect contradictory public states.
- Publish score internally and gate release if below threshold.

## Suggested Success Criteria
- 0 contradictory stance/posture messages across home surface for 14 consecutive days.
- Freshness stale count median < 5 over 14 days.
- Calibration bins with `ROBUST` quality appear for top decision bins.
- < 5% of user-visible numeric fields are null without `unavailable_reason`.

## Bottom Line
The product direction is strong. The immediate unlock is to tighten coherence + uncertainty communication so the homepage behaves like a disciplined PM/quant memo, not a collection of independent widgets.
