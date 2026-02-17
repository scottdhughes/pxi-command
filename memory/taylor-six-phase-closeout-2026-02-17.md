# Taylor Six-Phase Closeout Evidence (2026-02-17)

## Code + Deploy Status
- Worker deployed to staging: `pxi-api-staging` version `86b5429e-aedb-4ae2-a10e-c741b4a46e5f`.
- Worker deployed to production: `pxi-api-production` version `28def0db-37b9-46af-aa55-a437b90b949e`.
- Frontend deployed to Pages project `pxi-frontend` (branch `main`) at deploy URL:
  - `https://0b1cb3ee.pxi-frontend.pages.dev`
- Manual refresh endpoint auth token was not available locally; refresh was executed via GitHub Actions workflow dispatch:
  - Workflow: `Daily PXI Refresh`
  - Run ID: `22115072101`
  - Result: `success`

## Production Contract Gate
Command:
```bash
bash scripts/api-product-contract-check.sh https://api.pxicommand.com
```

Result:
- **FAILED**
- Failure reason:
  - `Consistency gate failed: score 89 is below minimum 90`

Returned `/api/market/consistency` payload at failure time:
```json
{
  "as_of": "2026-02-17T00:00:00.000Z",
  "score": 89,
  "state": "WARN",
  "violations": [
    "stale_inputs_penalty",
    "insufficient_calibration_penalty",
    "conflict_state_penalty"
  ],
  "components": {
    "base_score": 100,
    "structural_penalty": 0,
    "reliability_penalty": 11
  },
  "created_at": "2026-02-17 20:47:31"
}
```

## Key Post-Deploy Endpoint Evidence

### `/api/plan`
- `action_now.risk_allocation_target = 0.16`
- `action_now.raw_signal_allocation_target = 0.42`
- `action_now.risk_allocation_basis = "penalized_playbook_target"`
- `trader_playbook.recommended_size_pct.target = 16`
- `consistency.score = 89`, `state = "WARN"`, `components.reliability_penalty = 11`

### `/api/pxi`
- `dataFreshness.staleCount = 15`
- `dataFreshness.lastRefreshAtUtc = "2026-02-17T20:47:33.245Z"`
- `dataFreshness.lastRefreshSource = "market_refresh_runs"`

### `/api/alerts/feed?limit=5`
- latest `freshness_warning` body: `"15 indicator(s) are stale and may impact confidence."`
- parity with plan/brief/pxi stale count now aligned at `15`

### `/api/opportunities?horizon=7d&limit=3`
- calibration includes additive `window` field
- expectancy includes additive `basis` and `quality` fields

### `/api/ml/accuracy`
- `coverage.total_predictions = 1`
- `coverage_quality = "INSUFFICIENT"`
- `minimum_reliable_sample = 30`
- `unavailable_reasons` includes `"insufficient_sample"`

## Local Validation
- `frontend` build: success (`npm run build`)
- root TypeScript build + unit tests: success (`npm run build` + node tests)
- worker deploy dry run and production deploy: success
- contract script syntax check: success (`bash -n scripts/api-product-contract-check.sh`)

