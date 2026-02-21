A) **Top 12 backlog table**

| Rank | Title | Problem statement | Why it matters quantitatively | Scope | Effort | Dependencies | Rollback plan |
|---:|---|---|---|---|---|---|---|
| 1 | Opportunity Coherence Gate v1 | `/api/opportunities` can publish bullish calls with `probability_correct_direction < 0.5` and/or negative expectancy. | Prevents sign-incoherent signals; should cut false-positive action rate immediately. | backend/data/model | S | Existing opportunity generation pipeline | Feature flag `oppty_coherence_gate=false`; revert to legacy publish logic. |
| 2 | Publish Eligibility + Degraded Suppression | Opportunities still appear during freshness/reliability stress without strict eligibility policy. | Reduces stale-induced decision error; lowers bad-action exposure during data incidents. | backend/ops/data | M | Freshness status + alerts feed | Toggle `oppty_publish_on_degraded=true` to restore prior behavior. |
| 3 | Calibration Transparency API (`/api/diagnostics/calibration`) | Calibration exists but is not surfaced as decision-grade evidence. | Enables objective trust via Brier/ECE/log-loss by horizon/decile; supports model governance. | backend/data | M | Historical resolved outcomes store | Keep endpoint additive; if unstable, hide UI consumer and keep API dark. |
| 4 | Opportunity Semantics Contract v1 | “Conviction” lacks deterministic mapping to probability/expectancy semantics. | Eliminates interpretation drift; improves user action consistency and backtest reproducibility. | product/backend/data | M | Item 1 + schema docs | Keep old fields, add new `decision_contract`; UI can ignore via flag. |
| 5 | Signals Sanitization Pipeline | `/signals/latest` shows noisy ticker concatenations + extreme velocity artifacts. | Improves precision of signal extraction; raises downstream conversion from signal-to-action. | data/backend | M | NLP/tokenization rules, anomaly filters | Fallback to raw feed + banner “sanitization unavailable.” |
| 6 | Decision Stack API (`/api/decision-stack`) | Brief/opportunities/alerts are separate; user must manually reconcile. | Cuts cognitive load; increases habit formation and action latency quality. | backend/frontend/product | M | Items 1–4 | Endpoint additive + UI flag; disable card if errors > threshold. |
| 7 | Freshness SLO + Incident Ledger | Recurrent critical stale bursts reduce trust; weak post-incident accountability. | Quantifies operational quality and its PnL impact proxy; supports SLA-driven ops. | ops/backend/data | S | Existing alerts + refresh runs | If noisy, keep internal-only and disable public ledger view. |
| 8 | Horizon Quality Split (7d vs 30d contract) | Horizon quality/utility not explicitly separated with confidence discipline. | Prevents users over-weighting weak horizons; improves forecast selection efficiency. | data/backend/frontend | M | Calibration diagnostics | Preserve existing horizon endpoints; add quality bands. |
| 9 | Consistency Regression Tests in CI | Consistency can drift (historical WARN in feed). | Detects contract drift before prod; lowers silent logic regressions. | backend/ops | S | Test harness + fixtures | Mark tests non-blocking if flaky, then tighten once stable. |
| 10 | Alert Severity Logic Upgrade | Current severity wording can be too coarse (`warning` vs prior `critical` streak). | Better risk communication reduces decision overconfidence during partial outages. | backend/product | S | Freshness classifier | Revert severity mapper to prior rules via config version pin. |
| 11 | Explainability Delta Cards v2 | Movers exist, but not tightly tied to action deltas/invalidation. | Improves interpretability-action linkage; reduces discretionary misuse. | frontend/backend/product | M | Decision stack API | Keep old cards available under toggle. |
| 12 | Reliability-Aware Caching + Timebox | Missing explicit timebox on stale but “healthy” states. | Reduces silent data aging and stale reads under partial pipeline lag. | backend/ops | S | Refresh scheduler + cache layer | Disable strict TTL enforcement and fall back to current cache. |

---

B) **Acceptance-test checklist**

## 1) Opportunity Coherence Gate v1 (Critical)
- [ ] **API pass:** `GET /api/opportunities?horizon=7d` returns zero items where `(direction=bullish AND probability_correct_direction<0.5)` OR `(direction=bullish AND expected_move_pct<0)`; mirrored rules for bearish.
- [ ] **API pass:** each item includes `eligibility: {passed:true, failed_checks:[]}`.
- [ ] **Failure-path:** inject fixture with sign mismatch; item must be suppressed or relabeled `status=inconclusive`, and alert emitted to internal log.

## 2) Publish Eligibility + Degraded Suppression (Critical)
- [ ] **API pass:** when `critical_stale_count>0`, `/api/opportunities` returns empty `items` + `degraded_reason` populated.
- [ ] **API/UI pass:** homepage/opportunities panel shows deterministic “suppressed due to data quality” state.
- [ ] **Failure-path:** simulate stale spike (10 indicators stale); ensure no new opportunities publish until freshness clears.

## 3) Calibration Transparency API
- [ ] **API pass:** `GET /api/diagnostics/calibration?horizon=7d` returns Brier/ECE/log-loss + CI + sample size by decile.
- [ ] **API pass:** schema versioned and additive; old endpoints unchanged.
- [ ] **Failure-path:** sample size below minimum returns `quality=INSUFFICIENT` and hides score values (no fake precision).

## 4) Opportunity Semantics Contract v1
- [ ] **API pass:** opportunity payload includes `decision_contract: {direction, p_correct, expectancy_sign, confidence_band}` with deterministic derivation.
- [ ] **UI pass:** tooltip explains exact mapping from conviction band to probability semantics.
- [ ] **Failure-path:** if derived fields conflict, response marks `contract_state=INVALID` and suppresses CTA.

## 5) Signals Sanitization Pipeline (Critical)
- [ ] **API/UI pass:** no concatenated ticker artifacts; tickers are tokenized array with valid symbol regex.
- [ ] **API pass:** velocity capped/winsorized and includes raw vs adjusted value.
- [ ] **Failure-path:** parser failure routes to fallback block with explicit “low confidence extraction” tag.

## 6) Decision Stack API
- [ ] **API pass:** `GET /api/decision-stack` returns joined brief + opportunities + alerts with one `recommended_action`.
- [ ] **UI pass:** single card renders action + confidence + invalidation conditions.
- [ ] **Failure-path:** if any upstream endpoint fails, stack returns partial with `completeness` score and no hard CTA.

## 7) Freshness SLO + Incident Ledger
- [ ] **API pass:** `/api/ops/freshness-slo` exposes daily SLO attainment and incidents.
- [ ] **UI pass:** status badge reflects last 7d reliability class.
- [ ] **Failure-path:** ingestion outage still records synthetic incident entry (no silent gap).

## 8) Horizon Quality Split
- [ ] **API pass:** 7d/30d responses include `quality_band` (ROBUST/THIN/INSUFFICIENT) with min-sample logic.
- [ ] **UI pass:** low-quality horizon shows muted action state.
- [ ] **Failure-path:** insufficient sample blocks confidence rendering.

## 9) Consistency Regression Tests in CI
- [ ] **CI pass:** fixture set reproduces `consistency.score` and `state` deterministically.
- [ ] **CI pass:** any schema-breaking change fails pipeline.
- [ ] **Failure-path:** injected conflict (policy/risk mismatch) must fail CI with explicit reason.

## 10) Alert Severity Logic Upgrade
- [ ] **API pass:** severity derives from codified thresholds (`critical_stale_count`, duration, recurrence).
- [ ] **UI pass:** severity color/text matches API value exactly.
- [ ] **Failure-path:** stale recurrence escalation triggers warning→critical after threshold days.

## 11) Explainability Delta Cards v2
- [ ] **UI pass:** each action links to top 3 drivers and signed contribution.
- [ ] **API pass:** drivers include timestamp + source indicator.
- [ ] **Failure-path:** missing driver data disables “why now” CTA (no fabricated reasons).

## 12) Reliability-Aware Caching + Timebox
- [ ] **API pass:** responses include `data_age_seconds` and `ttl_state`.
- [ ] **Ops pass:** cache invalidates when `nextExpectedRefreshAtUtc` exceeded by threshold.
- [ ] **Failure-path:** forced delayed refresh marks endpoint degraded and blocks stale hard-CTA states.

---

C) **Metrics and guardrails matrix**

| Item | Success metric | Guardrail metric | Monitoring window | Rollback/disable trigger |
|---|---|---|---|---|
| 1 | Coherence violation rate <0.5% of published opps | Opportunity count collapse <40% vs 28d avg | Daily, 14d | Violations >2% for 2 days or publish collapse >60% |
| 2 | Degraded-state false publish = 0 | Suppression overfire days <10% | Daily, 21d | Any publish during critical stale OR overfire >20% |
| 3 | Calibration endpoint uptime >99% | Metric drift vs offline calc <1bp tolerance | Daily, 30d | Drift >5bp for 2 consecutive runs |
| 4 | Contract-valid opportunities >99% | User confusion proxy (support tickets/tagged) non-increasing | Weekly, 4w | Invalid contract >1% |
| 5 | Sanitized ticker precision >95% (sampled) | Recall drop <10% vs baseline | Daily, 14d | Precision <90% or recall drop >20% |
| 6 | Decision-stack render success >99% | Wrong-action contradiction count = 0 | Daily, 14d | Contradiction >0 in prod |
| 7 | Freshness SLO attainment ≥95% days | Incident logging completeness 100% | Weekly, 6w | SLO <85% two weeks |
| 8 | Horizon-specific hit-rate delta stable by quality band | Overconfidence rate in THIN/INSUFFICIENT = 0 | Weekly, 6w | Any confidence shown for insufficient band |
| 9 | CI catches all seeded consistency faults | Flake rate <2% | Per PR, rolling 2w | Flake >5% or missed seeded fault |
| 10 | Severity precision/recall >90% vs incident labels | Alert fatigue (alerts/user/day) < threshold | Weekly, 4w | Precision <80% |
| 11 | “Why now” card CTR +20% | Misleading explanation reports = 0 | Weekly, 4w | Any confirmed fabricated driver |
| 12 | Stale hard-CTA exposure = 0 | Endpoint latency increase <15% | Daily, 14d | Latency +30% sustained or stale CTA leak |

---

D) **Experiment plan (4)**

## Exp 1 — Coherence Gate utility uplift
- **Hypothesis:** enforcing sign coherence reduces bad calls and improves realized directional hit rate.
- **Control/Treatment:** Control = legacy opp publish; Treatment = Item 1 gate.
- **Required sample:** ≥400 opportunities total or 4 weeks, whichever later.
- **Decision threshold:** +3pp directional hit rate and no >25% drop in actionable count.
- **Stop conditions:** actionable count drops >50% for 7 days or contradiction bug found.

## Exp 2 — Decision Stack habit lift
- **Hypothesis:** unified decision stack increases 4+ weekly return usage among quant users.
- **Control/Treatment:** old multi-panel flow vs `decision-stack` card with explicit action/invalidation.
- **Required sample:** ≥120 active users, 3-week run.
- **Decision threshold:** +15% relative increase in users returning ≥4 times/week.
- **Stop conditions:** trust proxy declines (thumbs-down/report) >20% relative.

## Exp 3 — Signals sanitization actionability
- **Hypothesis:** sanitized signals improve analyst-rated actionability and reduce false triggers.
- **Control/Treatment:** raw signals vs sanitized + confidence tags.
- **Required sample:** ≥300 signal events with blind human adjudication.
- **Decision threshold:** precision +10pp at <=10% recall loss.
- **Stop conditions:** recall loss >20% or precision not improved by week 2 midpoint.

## Exp 4 — Horizon quality banding risk control
- **Hypothesis:** quality-banded confidence display reduces overtrading in low-quality horizons.
- **Control/Treatment:** current horizon display vs quality-gated confidence/CTA suppression.
- **Required sample:** ≥200 horizon-specific decision events.
- **Decision threshold:** -20% actions taken in THIN/INSUFFICIENT with no degradation in ROBUST action rate.
- **Stop conditions:** ROBUST action rate drops >15%.

---

E) **6-week ship plan**

## Week 1 (No-regrets fast track, ship in ≤7 days)
- Ship **Item 1 (Coherence Gate v1)** behind flag.
- Ship API field additions: `eligibility`, `failed_checks`, `contract_state`.
- Add smoke tests for sign-consistency.
- **Output:** production-protected coherent opportunities.

## Week 2
- Ship **Item 2 (Degraded Suppression)** + **Item 10 (Severity upgrade)**.
- Add deterministic stale thresholds and suppression reasoning.
- **Output:** no-opportunity publish during critical stale; clearer risk communication.

## Week 3
- Ship **Item 3 (Calibration diagnostics API)** + initial dashboard internal view.
- Start Exp 1 data collection formally.
- **Output:** measurable calibration transparency.

## Week 4
- Ship **Item 5 (Signals sanitization)**.
- Begin Exp 3 A/B and adjudication pipeline.
- **Output:** cleaner signal feed with confidence annotations.

## Week 5
- Ship **Item 6 (Decision Stack API + UI)** + **Item 11 (Explainability v2)**.
- Launch Exp 2.
- **Output:** integrated decision surface with “why now / invalidation”.

## Week 6
- Ship **Item 7 (Freshness SLO + incident ledger)**, **Item 8 (horizon quality split)**, **Item 12 (cache timebox)**, and CI hardening (**Item 9**).
- Launch Exp 4 and publish first 6-week quality report.
- **Output:** reliability-governed system with explicit quality bands and governance checks.

---

F) **Risks and unknowns**

1) **Sample sufficiency risk:** some segments (30d, niche themes) may remain underpowered; enforce INSUFFICIENT labels, don’t backfill confidence.  
2) **Suppression overfire risk:** strict gating may starve opportunity flow; monitor publish volume guardrail and iterate thresholds conservatively.  
3) **Schema drift risk:** additive changes can still break fragile clients; provide versioned contract docs + fixtures.  
4) **Evaluation leakage risk:** calibration gains may be overstated if validation windows are not strictly forward-chained.  
5) **UX trust risk:** better metrics can still confuse users unless action semantics are deterministic and visible at point of decision.
