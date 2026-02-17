# Taylor Thought Full Closeout Evidence (2026-02-17)

## Scope
Implemented additive-only closeout for homepage decision coherence, uncertainty framing, freshness operator visibility, null/unavailable semantics, trader utility, and consistency contract gating.

## Backend/API changes
- `/api/plan`
  - Added `policy_state.rationale_codes`, `uncertainty`, `consistency`, `trader_playbook`.
- `/api/brief?scope=market`
  - Added/validated `policy_state`, `source_plan_as_of`, `contract_version`, `consistency`, `degraded_reason`.
  - Legacy snapshots auto-rebuild when contract mismatch is detected.
- `/api/opportunities`
  - Added `expectancy` object and calibration `unavailable_reason`.
- `/api/pxi`
  - Added freshness operator fields:
    - `dataFreshness.topOffenders`
    - `dataFreshness.lastRefreshAtUtc`
    - `dataFreshness.nextExpectedRefreshAtUtc`
    - `dataFreshness.nextExpectedRefreshInMinutes`
- `/api/accuracy` + `/api/ml/accuracy`
  - Added `as_of`, `coverage`, `unavailable_reasons`.
- `/api/market/consistency`
  - Added endpoint returning latest consistency score/state/violations.

## Schema/migration changes
- `worker/schema.sql`
  - Added `market_brief_snapshots.contract_version` default `2026-02-17-v2`.
  - Added `market_consistency_checks` + `idx_market_consistency_created`.
- `worker/api.ts` migration path (`/api/migrate` + runtime schema guard)
  - Added/guarded `contract_version` column.
  - Added/guarded `market_consistency_checks` table and index.

## Frontend changes
- Homepage decision surface (`frontend/src/App.tsx`)
  - Enforced hierarchy: Decision → Confidence → Why → Risk Limits.
  - Added uncertainty banner.
  - Added consistency state/score and violations visibility.
  - Added trader playbook rendering (size range, scenarios, follow-through).
- Brief surfaces
  - Added policy-state + consistency + contract metadata context.
- Freshness operator panel
  - Added top offenders with owner/escalation/chronic and refresh ETA.
- Opportunities
  - Added expectancy display and unavailable-reason handling.
- ML accuracy
  - Removed silent fallback-to-50 behavior; now displays explicit unavailable state.

## Signals compatibility/parity changes
- `signals/src/routes.ts`
  - Added `/signals/api/accuracy` aliases:
    - `as_of`, `total_predictions`, `resolved_predictions`, `unresolved_predictions`
    - `governance_status`
- `signals/src/ops/deploy_parity.ts`
  - Added validator requirements for aliases and governance status.
- `signals/tests/unit/deploy_parity.test.ts`
  - Updated fixtures/assertions for alias contract.

## Contract gate changes
- `scripts/api-product-contract-check.sh`
  - Added checks for:
    - new `/api/plan` uncertainty/consistency/trader_playbook fields
    - `/api/brief` policy-state coherence fields
    - `/api/opportunities` expectancy + unavailable reasons
    - `/api/pxi` freshness operator payload
    - `/api/market/consistency`
  - Added coherence assertions:
    - `plan.policy_state.stance == brief.policy_state.stance`
    - `plan.policy_state.risk_posture == brief.policy_state.risk_posture`
  - Added hard gate:
    - fail if consistency state `FAIL`
    - fail if consistency score `< 90`

## Validation runs
- `cd /Users/scott/pxi && npm run build` ✅
- `cd /Users/scott/pxi/frontend && npm run build` ✅
- `cd /Users/scott/pxi/signals && npm test -- tests/unit/deploy_parity.test.ts` ✅

- `cd /Users/scott/pxi && bash -n scripts/api-product-contract-check.sh` ✅
- `cd /Users/scott/pxi && bash scripts/api-product-contract-check.sh https://api.pxicommand.com` ❌ (expected until worker/frontend deploy; live `/api/plan` still old contract without `uncertainty`, `consistency`, `trader_playbook`, `policy_state.rationale_codes`)
