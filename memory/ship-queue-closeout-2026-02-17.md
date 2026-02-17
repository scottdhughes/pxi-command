# Ship Queue Closeout Evidence (2026-02-17)

Generated: 2026-02-17 11:44 EST
Branch: `main`

## 1) Baseline product/API evidence

### Product API contract gate
Command:
```bash
cd /Users/scott/pxi && bash scripts/api-product-contract-check.sh https://api.pxicommand.com
```
Result:
- `Product API contract checks passed against https://api.pxicommand.com`

### Signals strict parity + migration markers
Command:
```bash
cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals --strict-version --check-migrations
```
Result summary:
- `pass: true`
- `/api/version` status `200`
- `/api/health` status `200`
- `/api/accuracy` status `200`
- `/api/predictions` status `200`
- migration check enabled (`production`, `SIGNALS_DB`) with zero failures.

### Calibration payload checks
Commands:
```bash
curl -sS https://api.pxicommand.com/api/plan | jq '.edge_quality.calibration'
curl -sS "https://api.pxicommand.com/api/opportunities?horizon=7d&limit=3" | jq '.items[].calibration'
curl -sS https://api.pxicommand.com/api/signal | jq '.edge_quality.calibration'
```
Observed shape:
- `/api/plan` calibration has `bin`, `probability_correct_7d`, `ci95_low_7d`, `ci95_high_7d`, `sample_size_7d`, `quality`.
- `/api/opportunities` item calibration has `probability_correct_direction`, `ci95_low`, `ci95_high`, `sample_size`, `quality`, `basis`.
- `/api/signal` calibration shape matches plan edge calibration.
- Current quality values are `INSUFFICIENT` with zero samples (typed null-safe fallback behavior is active).

### Targeted unit tests
Command:
```bash
cd /Users/scott/pxi/signals && npm test -- tests/unit/deploy_parity.test.ts tests/unit/evaluation_report.test.ts
```
Result:
- `2` files passed, `30` tests passed.

## 2) P0-10 calibration rollout closeout evidence

### Implementation anchors verified
- `/Users/scott/pxi/worker/schema.sql:295`
- `/Users/scott/pxi/worker/api.ts:2341`
- `/Users/scott/pxi/worker/api.ts:5307`
- `/Users/scott/pxi/worker/api.ts:5846`
- `/Users/scott/pxi/frontend/src/App.tsx:785`

### Refresh execution
Direct token call was not possible on this machine because `PXI_ADMIN_TOKEN` was not present in shell env.
Fallback trigger (same production refresh path with configured secrets) was used:
```bash
cd /Users/scott/pxi && gh workflow run "Daily PXI Refresh" --ref main
cd /Users/scott/pxi && gh run watch 22107152356 --exit-status
```
Workflow run:
- run id: `22107152356`
- status: `completed`
- conclusion: `success`
- URL: https://github.com/scottdhughes/pxi-command/actions/runs/22107152356
- step `Generate market products and in-app alerts` completed successfully.

## 3) P0-11 contract gate closeout evidence

### Implementation anchors verified
- `/Users/scott/pxi/scripts/api-product-contract-check.sh:1`
- `/Users/scott/pxi/.github/workflows/ci.yml:170`

### Gate behavior confirmed
- Script executes in CI smoke job after route/status checks.
- Script passes against production API base.

## 4) P0-6 production drift closure evidence

### Live artifact metadata
Command:
```bash
curl -sS https://pxicommand.com/signals/api/version | jq .
```
Observed:
- `worker_version`: `signals-7413701b0960-2026-02-17T16:35:30Z`
- `build_sha`: `7413701b0960`
- `build_timestamp`: `2026-02-17T16:35:30.000Z`

### Artifact-pinned strict parity + migration gate
Command:
```bash
cd /Users/scott/pxi/signals
npm run smoke:deploy -- https://pxicommand.com/signals --strict-version \
  --expect-worker-version "signals-7413701b0960-2026-02-17T16:35:30Z" \
  --expect-build-sha "7413701b0960" \
  --check-migrations --migration-env production --migration-db SIGNALS_DB
```
Result:
- `pass: true`
- no failures.

### Duplicate/index proof queries
Commands:
```bash
cd /Users/scott/pxi/signals
npx wrangler d1 execute SIGNALS_DB --remote --env production --command "SELECT signal_date, theme_id, COUNT(*) AS c FROM signal_predictions GROUP BY signal_date, theme_id HAVING c > 1 LIMIT 5;"
npx wrangler d1 execute SIGNALS_DB --remote --env production --command "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_signal_predictions_signal_theme_unique';"
```
Result:
- duplicate query returned empty `results`.
- index query returned `idx_signal_predictions_signal_theme_unique`.

## 5) P1-14 migration parity gate evidence

### Implementation anchors verified
- `/Users/scott/pxi/signals/src/ops/deploy_parity.ts:57`
- `/Users/scott/pxi/signals/scripts/deploy_parity_check.ts:1`
- `/Users/scott/pxi/signals/tests/unit/deploy_parity.test.ts:283`
- `/Users/scott/pxi/signals/docs/DEPLOYMENT.md:128`

### Validation
- `tests/unit/deploy_parity.test.ts` passed.
- `smoke:deploy --check-migrations` passed in production strict mode.

## 6) P2-3 governance threshold evidence

### Implementation anchors verified
- `/Users/scott/pxi/signals/src/ops/evaluation_report.ts:9`
- `/Users/scott/pxi/signals/src/ops/evaluation_report.ts:618`
- `/Users/scott/pxi/signals/tests/unit/evaluation_report.test.ts:1`
- `/Users/scott/pxi/signals/docs/SCORING.md:329`

### Deterministic fail-path report runs
Commands:
```bash
cd /Users/scott/pxi/signals
npm run report:evaluation -- --input data/evaluation_sample_predictions.json --out out/evaluation
npm run report:evaluation -- --input data/evaluation_sample_predictions.json --out out/evaluation-strict --min-resolved 999 --max-unresolved-rate 0 --min-slices 999
```
Observed governance status (`report.json`):
- default thresholds: `status=fail` with reasons:
  - `resolved observations 24 below minimum 30`
  - `walk-forward slices 0 below minimum 3`
- strict thresholds: `status=fail` with reasons:
  - `resolved observations 24 below minimum 999`
  - `walk-forward slices 0 below minimum 999`

Interpretation:
- Governance gates are active and produce machine-checkable, reproducible no-go reasons.
