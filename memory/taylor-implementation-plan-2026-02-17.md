# Taylor Implementation Plan — PXI Command
**Date:** 2026-02-17  
**Repo:** `/Users/scott/pxi`  
**Objective:** Practical next-step execution plan after P0-7/8/9-class shipping, maximizing everyday trader utility.

---

## 1) Verified Current Status (code + live)

## A. Codebase verification (local)
Confirmed shipped components are present in code:

- **Plan API + plan payload logic** in `worker/api.ts`
  - `/api/plan` route present.
  - payload fields present: `setup_summary`, `action_now`, `edge_quality`, `risk_band`, `invalidation_rules`, `degraded_reason`.
- **Edge quality + conflict state** in `worker/api.ts`
  - `resolveConflictState(...)`, `computeEdgeQualitySnapshot(...)` present.
  - `/api/signal` includes `signal.conflict_state`, `edge_quality`, `freshness_status`.
- **Frontend integration** in `frontend/src/App.tsx`
  - plan model/state present (`horizon_bias`, `conflict_state`, `invalidation_rules`).
  - `fetchApi('/api/plan')` integrated.
  - plan/edge rendering logic present.
- **Daily workflow refresh-products trigger** present:
  - `.github/workflows/daily-refresh.yml` includes POST to `/api/market/refresh-products` after `npm run cron:daily`.
  - `src/pipeline/cron-fast.ts` includes refresh-products request path.

## B. Live endpoint verification
Observed at `api.pxicommand.com`:

- `GET /api/plan` → **200** with full payload.
- `GET /api/signal` → **200** with `conflict_state` and `edge_quality`.
- `GET /api/brief?scope=market` → **200**.
- `GET /api/opportunities?horizon=7d&limit=5` → **200**.
- `GET /api/alerts/feed?limit=10` → **200**.

Observed at `pxicommand.com/signals`:

- `/signals/api/health` → **200**, status `ok`, latest run fresh.
- `/signals/api/accuracy` → **200**, completeness fields present.

## C. Local quality checks (run now)
- `cd /Users/scott/pxi && npm run build` ✅
- `cd /Users/scott/pxi/signals && npm test` ✅ (35 files, 363 tests)
- `cd /Users/scott/pxi/signals && npm run lint` ✅
- `cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals --strict-version` ✅
- `cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals-staging --strict-version --check-migrations --migration-env staging --migration-db SIGNALS_DB` ✅

**Note:** production migration-marker check from this shell failed due missing `CLOUDFLARE_API_TOKEN` in non-interactive environment (endpoint parity still passed).

---

## 2) Remaining Highest-Impact Work (everyday trader utility)

### Priority 1 — Confidence calibration + reliability scoring hardening
**Why:** Edge quality is now visible; next step is making it statistically trustworthy.
- Calibrate `edge_quality.score` against realized outcomes (reliability curve / Brier).
- Add monotonic constraints so stale/conflict conditions never produce high confidence by accident.

### Priority 2 — Opportunity conviction calibration (score -> probability)
**Why:** Conviction number is useful but still heuristic-heavy; needs interpretability.
- Map conviction deciles to realized hit rates and uncertainty ranges.
- Show confidence intervals when sample size is small.

### Priority 3 — Scenario/risk bands promotion into primary UX
**Why:** Traders need downside framing, not just direction.
- Elevate `risk_band` into first-screen decision UX.
- Add explicit “low sample / degraded confidence” warning logic.

### Priority 4 — Data freshness remediation loop (reduce stale count)
**Why:** Current stale indicator count materially drags edge quality and trust.
- Triage chronic stale indicators and source lag.
- Improve ingest fallback behavior and freshness telemetry by indicator class.

### Priority 5 — Product contract + fallback observability gates
**Why:** P0 routes are now live; keep them live and coherent under deploy churn.
- Add API contract tests for `/api/plan`, `/api/brief`, `/api/opportunities`, `/api/alerts/feed`.
- Add degraded mode observability metrics so failures are explainable to ops and users.

---

## 3) 14-Day Execution Plan

## Phase 1 (Days 1–3): Calibration foundations
### Tasks
1. Add edge-quality calibration job/report from historical outcomes.
2. Define score-to-label thresholds using realized reliability bins.
3. Add confidence monotonicity unit tests.

### Touchpoints
- `worker/api.ts` (edge quality scoring helpers)
- `worker/schema.sql` (optional calibration table)
- `src/pipeline/*` (if batch calibration script added)
- `memory/` (calibration report artifact)

### Acceptance criteria
- Edge quality bins have measurable realized performance deltas.
- `stale_count↑` or `conflict_state=CONFLICT` cannot increase quality score.
- Calibration report generated and versioned.

### Risk controls
- Keep formula simple and bounded.
- Gate threshold changes behind explicit config constants.

---

## Phase 2 (Days 4–7): Opportunity and risk framing upgrade
### Tasks
1. Add conviction calibration layer (decile mapping + CI).
2. Upgrade plan/opportunity payloads with calibrated probability fields.
3. Surface risk bands prominently in frontend decision card and opportunities page.

### Touchpoints
- `worker/api.ts` (`buildOpportunitySnapshot`, `/api/plan`)
- `frontend/src/App.tsx` (plan + opportunities rendering)
- `worker/schema.sql` (if storing calibration bins)

### Acceptance criteria
- Conviction deciles show monotonic realized hit-rate trend.
- UI displays probability + sample size + warning when unstable.
- No regression in existing endpoint fields.

### Risk controls
- Preserve existing fields for backward compatibility.
- Add fallback path when calibration sample is insufficient.

---

## Phase 3 (Days 8–10): Freshness and ingest reliability
### Tasks
1. Identify top stale indicators by recurrence and impact.
2. Add source-specific fallback/escalation policy for stale-critical indicators.
3. Add daily stale breakdown to ops output.

### Touchpoints
- `src/config/indicator-sla.ts`
- `src/pipeline/cron-fast.ts`
- `.github/workflows/daily-refresh.yml`
- `worker/api.ts` (`computeFreshnessStatus` presentation)

### Acceptance criteria
- Critical stale indicators trend down over 7-day window.
- SLA summary surfaces top stale causes clearly.
- Edge quality data-quality component improves measurably.

### Risk controls
- Do not relax critical thresholds to “improve score”.
- Keep source-lagged policy explicit and documented.

---

## Phase 4 (Days 11–14): Production hardening + gates
### Tasks
1. Add contract tests for plan/brief/opportunities/alerts payloads.
2. Add release checklist enforcement in CI (route + schema + freshness checks).
3. Run staged canary, then production promotion with evidence artifact.

### Touchpoints
- `.github/workflows/ci.yml`
- `worker/api.ts` (contract consistency)
- `signals/scripts/deploy_parity_check.ts` (if extending cross-product checks)

### Acceptance criteria
- CI blocks on contract drift.
- Staging passes route and payload checks before prod.
- Production deploy has attached validation artifact bundle.

### Risk controls
- Feature-flag risky UX/model changes.
- Keep rollback commands pre-staged before deploy.

---

## 4) Exact Validation Command Set

## Local build + quality
```bash
cd /Users/scott/pxi && npm run build
cd /Users/scott/pxi/signals && npm test
cd /Users/scott/pxi/signals && npm run lint
```

## Live endpoint sanity
```bash
curl -sS https://api.pxicommand.com/api/plan | jq .
curl -sS https://api.pxicommand.com/api/signal | jq .
curl -sS "https://api.pxicommand.com/api/brief?scope=market" | jq .
curl -sS "https://api.pxicommand.com/api/opportunities?horizon=7d&limit=5" | jq .
curl -sS "https://api.pxicommand.com/api/alerts/feed?limit=10" | jq .
```

## Signals parity gates
```bash
cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals --strict-version
cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals-staging --strict-version --check-migrations --migration-env staging --migration-db SIGNALS_DB
```

## Production migration marker check (requires credentials)
```bash
export CLOUDFLARE_API_TOKEN=...
export CLOUDFLARE_ACCOUNT_ID=...
cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals --strict-version --check-migrations --migration-env production --migration-db SIGNALS_DB
```

---

## 5) Rollback Plan

If any phase regresses trust/accuracy/availability:

1. **API rollback:** redeploy previous known-good worker artifact.
2. **UI rollback:** hide new components behind feature flags; preserve legacy cards.
3. **Schema rollback:** only additive migrations in rollout window; avoid destructive changes in same release.
4. **Confidence/calibration rollback:** revert to last stable threshold constants and label mapping.
5. **Workflow rollback:** disable new CI blocking checks temporarily (`workflow_dispatch` override), then patch and re-enable.

Rollback trigger conditions:
- endpoint contract failure in production,
- confidence calibration inversion (bad monotonicity),
- stale-count spike from ingest regression,
- significant drop in plan/opportunity render success.

---

## 6) Production Gating Checklist (must pass)

- [ ] `npm run build` succeeds at repo root.
- [ ] Signals test/lint suite green.
- [ ] `/api/plan`, `/api/signal`, `/api/brief`, `/api/opportunities`, `/api/alerts/feed` all return contract-valid payloads.
- [ ] `signals` strict parity check passes in prod and staging.
- [ ] Production migration markers verified with Cloudflare credentials.
- [ ] Feature flags reviewed for safe fallback behavior.
- [ ] Rollback artifact/version documented before promotion.
- [ ] Post-deploy sanity commands run and attached to release notes.

---

## 7) Practical Next 5 Priorities (ordered)
1. Edge-quality calibration + monotonic reliability tests.
2. Opportunity conviction calibration with uncertainty bands.
3. Promote risk-band framing into top-level plan/opportunities UX.
4. Freshness remediation sprint for chronic stale indicators.
5. CI contract gates for plan/brief/opportunities/alerts + staged production checklist enforcement.
