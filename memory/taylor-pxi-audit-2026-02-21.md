A) **Executive verdict (max 200 words)**  
**Would I use this daily as a quant?** **No (not yet).**  
**Overall score: 58/100.**

PXI is interesting and closer to a real quant workflow than most “AI market dashboards,” but it is not yet decision-grade for daily capital allocation. What’s good: coherent regime framing (`/api/pxi` + `/api/brief`), explicit freshness flags, and attempts at calibration metadata in opportunities. What breaks trust: internal contradictions in the opportunity stack (bullish calls with low `probability_correct_direction` and one negative expectancy), unstable data-quality history (repeated critical stale alerts), and weakly interpretable signal UX on `/signals/latest` (ticker extraction/noise artifacts).  

For a serious user, this is currently **an idea generator + risk context overlay**, not a primary execution engine. Daily habit potential exists if you tighten semantics (what conviction means in probabilistic terms), enforce eligibility gates (don’t publish opportunities that fail basic expected-value tests), and prove out-of-sample value versus baselines.  

Right now: promising architecture, insufficient earned evidence.

---

B) **Scorecard (0–10)**

| Dimension | Score |
|---|---:|
| Signal quality | 6 |
| Calibration reliability | 4 |
| Robustness to stale/missing data | 5 |
| Decision usefulness | 5 |
| Explainability | 7 |
| Trustworthiness | 5 |
| Novelty/differentiation | 7 |
| Habit potential | 6 |

---

C) **Evidence-backed findings**

### High severity
1) **Opportunity semantics are internally inconsistent (can induce wrong trades).**  
- Evidence: `/api/opportunities?horizon=7d` shows bullish opportunities with `conviction_score` 71–72, but `calibration.probability_correct_direction = 0.3378` (well below 0.5) and for `global` `expectancy.expected_move_pct = -0.069%` while still labeled bullish.  
- Why high: violates basic sign consistency between direction, calibrated success probability, and expectancy.

2) **Data freshness reliability has recent instability.**  
- Evidence: `/api/alerts/feed` has multiple recent **critical** freshness warnings (10–13 stale indicators on Feb 17–20), now downgraded to warning (1 stale non-critical).  
- Why high: users can’t separate model edge from pipeline health noise without strong reliability SLAs.

3) **Signals page is noisy/fragile as a quant input.**  
- Evidence: `/signals/latest` emits extreme repeated velocity values (e.g., `1000.00x`) and concatenated ticker strings (`AVGOCPOLDOCO...`, `NVDAGOOGL...`) suggesting extraction/format quality issues.  
- Why high: low parsing quality destroys confidence and actionability.

### Medium severity
4) **Risk language coherence gap (SOFT vs RISK_ON posture).**  
- Evidence: `/api/pxi` score 46.59 labeled `SOFT`; `/api/brief` policy state shows `stance: RISK_ON`, `risk_posture: risk_on`, `base_signal: REDUCED_RISK`.  
- Why medium: may be logically valid, but needs explicit mapping rules to avoid “story beats data” perception.

5) **Calibration claims are underpowered for user trust.**  
- Evidence: opportunities expose calibration fields, but no public reliability curves/Brier by horizon/decile in surfaced endpoints; current displayed prob (0.338) conflicts with bullish framing.  
- Why medium: calibration data exists but is not operationally coherent at decision point.

### Low severity
6) **Home page fetch readability gives little evaluable content for hierarchy audit.**  
- Evidence: `pxicommand.com` extract largely title-only in this run.  
- Why low: may be tooling/extraction limitation, but suggests need for machine-readable status/summary block.

---

D) **Prioritized roadmap (strict order)**

1) **Hard-gate opportunity publication on coherence constraints**  
- Rule: require directional sign agreement across (`direction`, `probability_correct_direction > 0.5`, `expected_move_pct` sign), else suppress or relabel “watchlist/inconclusive.”  
- Impact: Very high | Complexity: Low-Med | Risk: Low | Owner: **Backend + Product**

2) **Ship public calibration diagnostics endpoint + UI card**  
- Include rolling Brier, log loss, ECE by horizon/theme/conviction decile + confidence intervals.  
- Impact: Very high | Complexity: Med | Risk: Low | Owner: **Data + Backend + Frontend**

3) **Freshness reliability SLO + degraded mode contract**  
- SLO: e.g., critical stale count ≤1 for 95% of days; auto downgrade confidence and hide affected opportunity types during breaches.  
- Impact: High | Complexity: Med | Risk: Med | Owner: **Data Eng + Backend**

4) **Signals sanitation and confidence scoring revamp**  
- Fix ticker tokenization, cap/winsorize velocity ratios, add anomaly filters and source-quality weighting.  
- Impact: High | Complexity: Med | Risk: Med | Owner: **Data + Backend**

5) **Decision stack unification layer (“What changed / What to do / Why now”)**  
- One object joining brief + opportunities + alerts with explicit action class (risk add/reduce/hold), confidence band, and invalidation conditions.  
- Impact: High | Complexity: Med-High | Risk: Med | Owner: **Product + Frontend + Backend**

---

E) **Quant reality check**

**Claims not yet earned**
- “Decision-grade calibrated forecasts” — contradicted by current opportunity coherence issues.  
- “Robust live reliability” — recent repeated critical freshness episodes weaken this.  
- “Actionable signals feed” — current `/signals/latest` formatting/noise undermines execution utility.

**What must be true to justify stronger claims**
1) Out-of-sample improvement vs baselines (e.g., simple regime/MA baseline) across 7d and 30d with pre-registered eval.  
2) Stable calibration: ECE/Brier within target bands for 8+ consecutive weeks, published by segment.  
3) Reliability SLO attainment with transparent incident log and measured impact on model outputs.  
4) Opportunity coherence pass rate near 100% under automated QA checks.  
5) Demonstrated user-level utility: tracked decisions influenced + realized uplift vs control workflow.
