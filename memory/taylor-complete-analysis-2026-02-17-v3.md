# Taylor Complete Analysis (2026-02-17)

Generated at: 2026-02-17T20:19:00Z
Scope: live production review of decision coherence, trader utility, data freshness, calibration quality, model utility, and operator reliability for pxicommand.com.

## 1) Executive Summary

Current state is materially improved versus prior baseline: contracts are live, canonical plan/brief coherence is additive and typed, and the homepage now has explicit uncertainty and consistency surfaces.

However, from a quant/trader decision-quality perspective, there are still critical coherence and trust gaps:

1. Decision target mismatch inside the same plan payload (`action_now` 42% vs `trader_playbook` target 16%).
2. Freshness operator timestamp is not operationally trustworthy (`lastRefreshAtUtc` is stale and disconnected from latest successful refresh cycle).
3. Alerts feed can serve stale stale-count text due dedupe + insert-ignore behavior.
4. Consistency score is logically valid but economically weak (can remain `PASS 100` during high uncertainty/conflict conditions).
5. Opportunity expectancy/calibration are mostly unavailable and currently not theme-conditional, reducing trader utility.
6. ML accuracy semantics are formally typed but still overstate reliability at low sample counts.

Conclusion: system is deploy-stable and contract-stable, but not yet decision-optimal for everyday traders. Next phase should focus on decision unification, operational freshness truth, and statistically honest utility metrics.

## 2) Evidence Snapshot (Live)

### 2.1 Contract/Deployment Health
- Product contract gate: PASS
  - `bash scripts/api-product-contract-check.sh https://api.pxicommand.com`
- Live signals worker metadata:
  - `worker_version: signals-bb834d0c8c0f-2026-02-17T20:12:07Z`
  - `build_sha: bb834d0c8c0f`

### 2.2 Canonical Plan/Brief State
- `/api/plan`:
  - `policy_state.stance = MIXED`
  - `action_now.risk_allocation_target = 0.42`
  - `trader_playbook.recommended_size_pct.target = 16`
  - `uncertainty.headline = "Signal quality reduced: stale inputs + limited calibration."`
  - `consistency = PASS, score 100`
- `/api/brief?scope=market`:
  - coherent `policy_state` and `contract_version = 2026-02-17-v2`

### 2.3 Freshness and Alerts
- `/api/pxi`:
  - `staleCount = 15`
  - `lastRefreshAtUtc = "2025-12-28 01:34:17"` (stale timestamp)
  - `nextExpectedRefreshAtUtc` is populated correctly
- `/api/alerts/feed` latest warning body:
  - `"17 indicator(s) are stale and may impact confidence."`
- Cross-surface comparison at same time:
  - plan stale = 15
  - brief stale = 15
  - pxi stale = 15
  - alerts stale text = 17

### 2.4 Opportunities / Calibration Utility
- `/api/opportunities?horizon=7d&limit=5`:
  - all calibrations `quality = INSUFFICIENT`
  - expectancy mostly unavailable (`insufficient_sample` / `neutral_direction`)
- `/api/opportunities?horizon=30d&limit=5`:
  - all calibrations `INSUFFICIENT`
  - all expectancy unavailable

### 2.5 Accuracy Surfaces
- `/api/accuracy`:
  - `coverage.total_predictions = 50`, 7d sample 50, 30d sample 27 (usable)
- `/api/ml/accuracy`:
  - `coverage.total_predictions = 1`
  - currently no `unavailable_reasons` despite effectively insufficient sample

## 3) Findings (Ordered by Severity)

## P0-1 Decision contradiction inside canonical plan payload
- Symptom:
  - `action_now.risk_allocation_target = 42%`
  - `trader_playbook.recommended_size_pct.target = 16%`
- Why this matters:
  - This is two conflicting position-size directives in the same payload and same screen.
  - A trader cannot know which target is canonical for execution.
- Probable root cause:
  - `action_now` reflects raw signal output; playbook applies additional penalties.
- Required fix:
  - Promote one canonical executable target (`recommended_size_pct.target`) and relabel raw signal as `base_allocation_target`.
  - Add explicit derivation fields (`penalty_conflict`, `penalty_freshness`, `penalty_calibration`).

## P0-2 Freshness operator panel is partially non-truthful
- Symptom:
  - `lastRefreshAtUtc` remains `2025-12-28` despite successful refresh run at `2026-02-17T20:07Z`.
- Why this matters:
  - Operator panel becomes misleading and weakens trust in freshness status.
- Probable root cause:
  - `lastRefreshAtUtc` sourced from `fetch_logs`, but current write path does not update `fetch_logs` in worker-side flow.
- Required fix:
  - Source last refresh from a canonical runtime artifact written by refresh path (`market_refresh_runs` table or snapshot metadata), not `fetch_logs`.

## P1-3 Alert feed can drift from current truth
- Symptom:
  - alerts feed stale body still says 17 while plan/brief/pxi report 15.
- Why this matters:
  - Feed messaging appears stale/degraded when core decision state updated.
- Root cause:
  - Event dedupe key is date/entity and insertion uses `INSERT OR IGNORE`; same-day event body is not updated.
- Required fix:
  - Use UPSERT on `dedupe_key` to refresh body/payload/severity/created_at (or introduce `updated_at`).

## P1-4 Consistency score is structurally too permissive
- Symptom:
  - `consistency = PASS 100` while system simultaneously reports conflict + stale inputs + insufficient calibration.
- Why this matters:
  - Score communicates "all clear" despite elevated uncertainty.
- Root cause:
  - Current consistency only checks logical contradictions, not statistical/operational confidence.
- Required fix:
  - Split into two dimensions:
    - `logical_consistency_score`
    - `decision_reliability_score` (penalize stale inputs, low calibration sample, unresolved uncertainty flags)
  - Gate homepage "green" state on both.

## P1-5 Opportunity utility remains statistically weak for actioning
- Symptom:
  - calibration bins for opportunities are all `INSUFFICIENT`; expectancy unavailable in most rows.
- Why this matters:
  - Opportunity table cannot support sizing or conviction differentiation when all rows are effectively "unknown".
- Root cause:
  - low sample + expectancy/hit-rate computed from broad prediction log rather than theme-conditional outcomes.
- Required fix:
  - Build theme-conditioned expectancy and hit-rate from opportunity history + forward outcomes per theme/direction/horizon.
  - Add minimum sample badge and suppress expectancy until threshold met.

## P2-6 ML accuracy surface is typed but not yet statistically honest
- Symptom:
  - `/api/ml/accuracy` coverage is 1 sample with no unavailable reason.
- Why this matters:
  - Users can overweight a single observation.
- Required fix:
  - Add low-sample reasoning (`insufficient_sample`) and explicit confidence class for ML accuracy.
  - Enforce floor thresholds before showing directional percentages as primary UI values.

## P2-7 Multi-card narrative still risks cognitive overload
- Observation:
  - Plan is clearer now, but predict/backtest/ML cards can still imply different directional tone than decision card.
- Why this matters:
  - Everyday traders need one actionable narrative with supporting context, not multiple competing narratives.
- Required fix:
  - Add a routing rule: non-canonical cards must map to plan stance and display as supporting evidence, not independent recommendations.

## 4) Quant Product Recommendations (What to Build Next)

1. Canonical Position Sizing Ladder (required)
- Expose `base_target`, `penalties`, `final_target` and make `final_target` the only actionable size.

2. Reliability-Weighted Decision Score (required)
- Add `decision_reliability_score` that can degrade independently of logical consistency.

3. Freshness Truth Table (required)
- Persist refresh runs with `completed_at`, `records_written`, `stale_count`, `run_status`.
- Drive operator panel from this table.

4. Alert State Upsert (required)
- Upsert on dedupe key; keep current alert text aligned with latest snapshot.

5. Theme-Conditional Opportunity Metrics (high value)
- Compute expectancy and hit-rate by theme/direction/horizon with rolling windows.

6. Sample-Aware UI Guardrails (high value)
- If sample below threshold, replace percentages with `N/A` + explicit reason chip.

7. Trader Workflow Enhancements (medium)
- Add watchlist integration from opportunities to inbox with trigger criteria and invalidation reminders.

## 5) 7-Day Execution Plan (Decision-Complete)

Day 1-2
- Fix canonical sizing contradiction:
  - add `action_now.base_risk_allocation_target`
  - set `action_now.risk_allocation_target = trader_playbook.recommended_size_pct.target / 100`
  - expose penalty breakdown.

Day 2-3
- Freshness truth refactor:
  - add `market_refresh_runs` table.
  - write row in `/api/market/refresh-products`.
  - switch `/api/pxi.dataFreshness.lastRefreshAtUtc` to `market_refresh_runs`.

Day 3-4
- Alert upsert fix:
  - replace `INSERT OR IGNORE` with UPSERT on `dedupe_key` updating body/payload/severity timestamps.

Day 4-5
- Reliability score:
  - add `decision_reliability` object to `/api/plan` and `/api/brief`.
  - adjust contract gate to enforce reliability threshold floors.

Day 5-6
- Opportunity metric hardening:
  - build theme-conditioned expectancy/hit-rate job.
  - only render expectancy where sample threshold satisfied.

Day 6-7
- UI narrative routing:
  - ensure predict/backtest/ML cards are subordinate and tagged relative to canonical stance.

## 6) Acceptance Criteria for Next Review

- No payload-level size contradiction between plan action and playbook.
- `/api/pxi.dataFreshness.lastRefreshAtUtc` updates within one refresh cycle of successful run.
- `/api/alerts/feed` stale-count text matches plan/brief stale count after refresh.
- Consistency dashboard includes both logical and reliability dimensions.
- At least 2 top opportunities have non-null expectancy with sample >= threshold, else explicitly marked unavailable.
- ML accuracy low-sample state rendered as unavailable, not as actionable percent.

## 7) Overall Rating (Taylor)

- System stability: 8.0/10
- Contract maturity: 8.5/10
- Decision coherence for execution: 6.5/10
- Statistical honesty in trader-facing utility: 6.0/10
- Operator observability truthfulness: 6.0/10

Composite: 7.0/10

The platform is now structurally strong; the next gains are in decision singularity and truthful uncertainty presentation.
