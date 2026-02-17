# SHIP QUEUE (Quant Review)

Last updated: 2026-02-17 13:15 EST

## Queue Hygiene Sync (2026-02-17)
- Marked `P0-ET1`, `P0-ET2`, `P0-ET3`, `P0-7`, `P0-8`, and `P0-9` as closed to reflect shipped production behavior.
- Code references: `worker/api.ts` (`/api/plan`, `/api/signal`, `/api/brief`, `/api/opportunities`, `/api/alerts/feed`), `frontend/src/App.tsx` (Today Plan + opportunities rendering), `.github/workflows/ci.yml` + `scripts/api-product-contract-check.sh` (contract gates).
- Run references: production endpoint checks on 2026-02-17 for `/api/plan`, `/api/brief?scope=market`, `/api/opportunities?horizon=7d&limit=5`, and `/api/alerts/feed?limit=10` returned HTTP 200 with JSON payloads.
- Closeout sync: marked `P0-10`, `P0-11`, `P0-6`, `P1-14`, and `P2-3` closed after parity, contract, migration-marker, and governance gate verification.
- Evidence report: `memory/ship-queue-closeout-2026-02-17.md`.

## Taylor Thought Full Closeout (2026-02-17)
- Implemented canonical-homepage alignment additions across worker + frontend:
  - `/api/plan`: `policy_state.rationale_codes`, `uncertainty`, `consistency`, `trader_playbook`.
  - `/api/brief`: coherence fields (`policy_state`, `source_plan_as_of`, `contract_version`, `consistency`, `degraded_reason`) enforced with legacy snapshot auto-rebuild.
  - `/api/opportunities`: `expectancy` + calibration `unavailable_reason`.
  - `/api/pxi`: freshness operator payload (`topOffenders`, `lastRefreshAtUtc`, `nextExpectedRefreshAtUtc`, `nextExpectedRefreshInMinutes`).
  - `/api/market/consistency`: latest score/state/violations endpoint.
- Added additive schema/migration support:
  - `worker/schema.sql`: `market_brief_snapshots.contract_version`, `market_consistency_checks` table/index.
  - `worker/api.ts` migration path: same table/column/index guards.
- Updated quality gates and parity checks:
  - `scripts/api-product-contract-check.sh` enforces plan/brief coherence + consistency threshold (`state != FAIL`, `score >= 90`) and validates new product fields.
  - `signals/src/routes.ts` accuracy aliases + `governance_status`.
  - `signals/src/ops/deploy_parity.ts` + `signals/tests/unit/deploy_parity.test.ts` validate new alias contract.
- Validation run set:
  - `cd /Users/scott/pxi && npm run build` ✅
  - `cd /Users/scott/pxi/frontend && npm run build` ✅
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/deploy_parity.test.ts` ✅
  - `cd /Users/scott/pxi && bash -n scripts/api-product-contract-check.sh` ✅
- Production contract gate note:
  - `bash scripts/api-product-contract-check.sh https://api.pxicommand.com` currently fails until this change-set is deployed (live `/api/plan` still on previous contract).

## P0

### P0-ET1 (CLOSED 2026-02-17) — Daily Decision Engine (`/api/plan`) + home "Today’s Plan" card
- **Impact / Confidence / Effort / Risk:** Very High / High / Medium / Low-Medium
- **Scope (exact):**
  - `worker/api.ts`
    - add `GET /api/plan` payload with: `setup_summary`, `action_now`, `confidence_quality`, `risk_band`, `invalidation_rules`.
  - `frontend/src/App.tsx`
    - add top-priority `Today’s Plan` card on `/` route.
    - consume `/api/plan` and render fallback when unavailable.
- **Precise technical next actions:**
  1. Define strict response contract for `/api/plan` (no nullable core fields on trading days).
  2. Implement endpoint by composing existing PXI, signal, and predict data paths.
  3. Add UI card with one-line setup + risk allocation + horizon bias.
  4. Add non-blocking fallback (existing page still loads if `/api/plan` fails).
  5. Add endpoint smoke checks in local verification notes.
- **Validation commands:**
  - `cd /Users/scott/pxi && npm run build`
  - `cd /Users/scott/pxi/frontend && npm run lint && npm run build`
  - `curl -sS https://api.pxicommand.com/api/plan | jq .`
- **Expected pass criteria:**
  - Home page surfaces single actionable plan consistently.
  - `/api/plan` returns coherent payload with no contradictory action fields.
- **Rollback plan:**
  - remove `/api/plan` route and hide card behind feature flag fallback.

### P0-ET2 (CLOSED 2026-02-17) — Confidence Quality Layer + explicit regime/signal conflict state
- **Impact / Confidence / Effort / Risk:** Very High / High / Medium / Medium
- **Scope (exact):**
  - `worker/api.ts`
    - add confidence-quality computation with penalties for stale data, model disagreement, and regime/signal conflict.
    - expose `conflict_state` on `/api/signal`.
  - `src/config/indicator-sla.ts`
    - expose helper for confidence penalty contribution from freshness state.
  - `frontend/src/App.tsx`
    - render confidence breakdown + conflict badge in primary decision area.
- **Precise technical next actions:**
  1. Define bounded confidence formula with capped penalties and monotonic guarantees.
  2. Implement `conflict_state` trigger conditions in `/api/signal`.
  3. Add confidence-quality fields to `/api/pxi` + `/api/signal` (+ `/api/plan` once ET1 lands).
  4. Add deterministic tests for stale/disagreement/conflict edge cases.
  5. Update UI to show quality decomposition (data/model/regime).
- **Validation commands:**
  - `cd /Users/scott/pxi && npm run build`
  - `cd /Users/scott/pxi && npx tsx src/config/indicator-sla.test.ts`
  - `curl -sS https://api.pxicommand.com/api/signal | jq .`
- **Expected pass criteria:**
  - confidence quality always declines as stale/disagreement penalties rise.
  - conflict state is explicit when regime and signal are materially misaligned.
- **Rollback plan:**
  - disable confidence penalties/conflict field and fall back to legacy confidence labels.

### P0-ET3 (CLOSED 2026-02-17) — Product surface consistency gate (`/api/brief`, `/api/opportunities`, `/api/alerts/feed`)
- **Impact / Confidence / Effort / Risk:** Very High / High / Medium / Medium
- **Scope (exact):**
  - `.github/workflows/ci.yml`
    - add contract checks for status + content type + required fields on product endpoints.
  - `worker/api.ts`
    - ensure route handlers return deterministic shape and non-empty payload contracts (or explicit disabled-state payload).
  - `frontend/src/App.tsx`
    - strengthen fallback UI when product endpoints unavailable.
- **Precise technical next actions:**
  1. Define required JSON contract per endpoint (`brief`, `opportunities`, `alerts/feed`).
  2. Add CI smoke assertions against production-like API target.
  3. Harmonize route responses (avoid accidental HTML fallback / ambiguous 404s).
  4. Add frontend message states for disabled/unavailable products.
  5. Gate deployment on contract pass.
- **Validation commands:**
  - `cd /Users/scott/pxi && npm run build`
  - `curl -sS https://api.pxicommand.com/api/brief?scope=market | jq .`
  - `curl -sS "https://api.pxicommand.com/api/opportunities?horizon=7d&limit=5" | jq .`
  - `curl -sS "https://api.pxicommand.com/api/alerts/feed?limit=10" | jq .`
- **Expected pass criteria:**
  - no unexpected 404s on surfaced product routes.
  - CI blocks releases when product contracts drift.
- **Rollback plan:**
  - revert CI gate and route-contract changes; preserve UI fallback warnings.

### P0-10 (CLOSED 2026-02-17) — Calibration snapshots + confidence payload rollout
- **Impact / Confidence / Effort / Risk:** Very High / Medium-High / Medium / Medium
- **Scope (exact):**
  - `worker/schema.sql`
    - add `market_calibration_snapshots` table + lookup index.
  - `worker/api.ts`
    - build/store edge-quality and conviction calibration snapshots during `/api/market/refresh-products`.
    - add calibration blocks to `/api/plan`, `/api/opportunities`, and `/api/signal`.
  - `frontend/src/App.tsx`
    - render calibration quality chips, CI bands, and warning state when quality is not robust.
- **Implementation steps (completed):**
  1. Verified calibration storage schema/table/index and snapshot generation paths.
  2. Verified `/api/market/refresh-products` calibration generation path and endpoint payload wiring.
  3. Verified frontend rendering for calibration quality chips, CI ranges, and non-robust warnings.
  4. Triggered production refresh via `Daily PXI Refresh` workflow dispatch path (run `22107152356`) because local `PXI_ADMIN_TOKEN` was not set.
- **Validation commands (run):**
  - `curl -sS https://api.pxicommand.com/api/plan | jq '.edge_quality.calibration'`
  - `curl -sS "https://api.pxicommand.com/api/opportunities?horizon=7d&limit=3" | jq '.items[].calibration'`
  - `curl -sS https://api.pxicommand.com/api/signal | jq '.edge_quality.calibration'`
  - `cd /Users/scott/pxi && gh workflow run "Daily PXI Refresh" --ref main` ✅
  - `cd /Users/scott/pxi && gh run watch 22107152356 --exit-status` ✅
- **Expected pass criteria (met):**
  - calibration blocks are always present and typed; null-safe when sample size is insufficient.
  - refresh path generates market products/calibrations and completes successfully in production workflow execution.
  - current live calibration quality can degrade to `INSUFFICIENT` without contract breakage.
- **Rollback plan:**
  - Revert calibration snapshot writes/reads in `worker/api.ts` and calibration rendering blocks in `frontend/src/App.tsx`; keep additive table in place.

### P0-11 (CLOSED 2026-02-17) — Product API contract gate hardening in CI
- **Impact / Confidence / Effort / Risk:** High / High / Low / Low
- **Scope (exact):**
  - `/Users/scott/pxi/scripts/api-product-contract-check.sh`
    - enforce required-field contracts for `/api/plan`, `/api/brief`, `/api/opportunities`, `/api/alerts/feed`.
  - `.github/workflows/ci.yml`
    - execute contract script in smoke job after route/status checks.
- **Validation commands (run):**
  - `cd /Users/scott/pxi && bash scripts/api-product-contract-check.sh https://api.pxicommand.com`
- **Expected pass criteria (met):**
  - CI fails on missing required fields or non-JSON responses for surfaced product routes.
  - degraded responses remain typed JSON (`degraded_reason`) and do not regress to HTML/404.
- **Rollback plan:**
  - Revert contract assertions in `scripts/api-product-contract-check.sh` and smoke workflow invocation in `.github/workflows/ci.yml`.

### P0-5 (CLOSED this cycle) — Anchor evaluation exits to target-date historical close (eliminate delayed-run horizon drift)
- **Impact / Confidence / Effort / Risk:** High / Medium-High / Medium-High / Medium
- **Scope (exact):**
  - `src/utils/price.ts`
    - added `fetchHistoricalETFPriceOnOrAfter(...)`,
    - added batched `fetchMultipleHistoricalETFPrices(...)`,
    - added deterministic request key helper (`historicalPriceRequestKey`).
  - `src/evaluation.ts`
    - `evaluatePendingPredictions()` now resolves exits by `(proxy_etf, target_date)` historical close,
    - persists unresolved states with explicit `evaluationNote` instead of implicit null-only outcomes,
    - keeps target-date alignment in log output (`exit_price_date`).
  - `src/db.ts`
    - extended prediction schema typing with `exit_price_date` + `evaluation_note`,
    - upgraded `updatePredictionOutcome(...)` to structured payload,
    - excluded unresolved rows from accuracy denominators (`AND hit IS NOT NULL`).
  - `migrations/0006_prediction_eval_price_date.sql` (new)
    - adds `exit_price_date` and `evaluation_note` columns,
    - adds evaluated-row index on `exit_price_date`.
  - `src/routes.ts`
    - `/api/predictions` now exposes `exit_price_date` and `evaluation_note` for auditability.
  - Tests:
    - `tests/unit/evaluation_target_price_date.test.ts` (new),
    - `tests/unit/utils_price_historical.test.ts` (new),
    - `tests/unit/db_prediction_dedupe.test.ts` assertion update for unresolved exclusion.
  - Docs:
    - `docs/API.md`, `docs/SCORING.md`, `docs/DEPLOYMENT.md`.
- **Implementation steps (completed):**
  1. Added historical close fetch path anchored to `target_date` with bounded forward lookup.
  2. Switched evaluation phase to use historical exits and persist resolved `exit_price_date`.
  3. Added explicit unresolved-note handling (`evaluation_note`) and kept `hit = null` for unresolved rows.
  4. Updated accuracy aggregation filters to exclude unresolved rows from hit-rate denominators.
  5. Added deterministic unit coverage and updated docs/runbook.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/utils_price_historical.test.ts tests/unit/evaluation_target_price_date.test.ts tests/unit/evaluation_store_predictions.test.ts tests/unit/db_prediction_dedupe.test.ts` ✅
  - `cd /Users/scott/pxi/signals && npm test` ✅ (33 files, 339 tests)
  - `cd /Users/scott/pxi/signals && npm run lint` ✅
  - `cd /Users/scott/pxi/signals && npm run offline` ✅
  - `cd /Users/scott/pxi/signals && npx wrangler d1 migrations apply SIGNALS_DB --local --env production` ✅
- **Expected pass criteria (met locally):**
  - Returns are computed against intended horizon-aligned historical closes.
  - API/database expose exact exit-price date and unresolved reason.
  - Unresolved market-data rows no longer bias hit-rate denominator.
- **Rollback plan:**
  - Revert `0006_prediction_eval_price_date.sql`, `src/evaluation.ts`, `src/utils/price.ts`, `src/db.ts`, `src/routes.ts`, related tests/docs; redeploy prior worker revision.

### P0-6 (CLOSED 2026-02-17) — Production drift closure + parity-green release execution
- **Impact / Confidence / Effort / Risk:** Very High / High / Medium / Medium
- **Scope (exact):**
  - Operational release path only (no code changes expected):
    - `wrangler.toml` env bindings,
    - remote D1 migrations (`0004`, `0005`, `0006`) on `SIGNALS_DB` production,
    - release metadata vars (`BUILD_SHA`, `BUILD_TIMESTAMP`, `WORKER_VERSION`).
  - Verification:
    - `scripts/deploy_parity_check.ts` strict run,
    - duplicate-key SQL and unique-index verification queries.
- **Implementation steps (completed):**
  1. Pulled live deploy metadata from `/api/version` (`worker_version=signals-7413701b0960-2026-02-17T16:35:30Z`, `build_sha=7413701b0960`).
  2. Ran strict parity gate with expected artifact pinning + migration checks.
  3. Ran explicit remote duplicate-key and unique-index proof SQL against production D1.
  4. Confirmed endpoint contract parity for `/api/version`, `/api/health`, `/api/accuracy`, and `/api/predictions`.
- **Validation commands (runbook + run evidence):**
  - `cd /Users/scott/pxi/signals && BUILD_SHA=$(git rev-parse --short=12 HEAD) && BUILD_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ") && WORKER_VERSION="signals-${BUILD_SHA}-${BUILD_TIMESTAMP}" && npx wrangler deploy --env staging --var BUILD_SHA:${BUILD_SHA} --var BUILD_TIMESTAMP:${BUILD_TIMESTAMP} --var WORKER_VERSION:${WORKER_VERSION}`
  - `cd /Users/scott/pxi/signals && BUILD_SHA=$(git rev-parse --short=12 HEAD) && BUILD_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ") && WORKER_VERSION="signals-${BUILD_SHA}-${BUILD_TIMESTAMP}" && npx wrangler deploy --env production --var BUILD_SHA:${BUILD_SHA} --var BUILD_TIMESTAMP:${BUILD_TIMESTAMP} --var WORKER_VERSION:${WORKER_VERSION}`
  - `cd /Users/scott/pxi/signals && npx wrangler d1 migrations apply SIGNALS_DB --remote --env production`
  - `cd /Users/scott/pxi/signals && npx wrangler d1 execute SIGNALS_DB --remote --env production --command "SELECT signal_date, theme_id, COUNT(*) AS c FROM signal_predictions GROUP BY signal_date, theme_id HAVING c > 1 LIMIT 5;"`
  - `cd /Users/scott/pxi/signals && npx wrangler d1 execute SIGNALS_DB --remote --env production --command "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_signal_predictions_signal_theme_unique';"`
  - `cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals --strict-version`
  - `cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals --strict-version --expect-worker-version signals-7413701b0960-2026-02-17T16:35:30Z --expect-build-sha 7413701b0960 --check-migrations --migration-env production --migration-db SIGNALS_DB` ✅
- **Expected pass criteria (met):**
  - strict parity returns success with no schema drift and no duplicate logical keys.
  - `/api/version` emits non-placeholder build metadata.
  - `/api/health` and `/api/accuracy` expose current tested contracts.
- **Confidence:** High.
- **Rollback plan:**
  - Redeploy previous worker revision and restore from backup/migration checkpoint if regression appears.

### P0-7 (CLOSED 2026-02-17) — Daily Trade Plan command center (`/api/plan` + first-screen card)
- **Impact / Confidence / Effort / Risk:** Very High / High / Medium / Medium
- **Scope (exact):**
  - `worker/api.ts`
    - add `GET /api/plan` endpoint that consolidates PXI state, signal allocation, prediction context, and invalidation rules.
    - return typed fallback payload when one upstream component is unavailable.
  - `frontend/src/App.tsx`
    - add a top-of-screen "Today Plan" card (setup summary, risk budget, invalidation rules, horizon split).
  - `frontend/src/App.css`
    - add compact mobile-friendly styling for plan card and confidence chips.
- **Implementation steps (ship plan):**
  1. Compose deterministic plan payload from existing `/api/pxi`, `/api/signal`, and `/api/predict` internals.
  2. Add explicit `action_now` and `invalidation_rules` fields so the trader knows what changes posture.
  3. Render plan card in first viewport on both mobile and desktop.
  4. Gate behind `FEATURE_ENABLE_PLAN` while preserving backward compatibility.
- **Validation commands:**
  - `curl -sS https://api.pxicommand.com/api/plan | jq '{as_of, setup_summary, action_now, edge_quality, invalidation_rules}'`
  - `curl -sS https://api.pxicommand.com/api/plan | jq '.invalidation_rules | length'`
  - `cd /Users/scott/pxi && npm run build`
- **Expected pass criteria:**
  - `/api/plan` returns non-null `setup_summary`, `action_now`, and `edge_quality`.
  - Plan card is visible by default in the first viewport.
  - Existing endpoint contracts remain additive-only.
- **Confidence:** High.
- **Rollback plan:**
  - Revert `/api/plan` route and plan-card UI additions, then redeploy prior worker/frontend revisions.

### P0-8 (CLOSED 2026-02-17) — Edge Quality engine (freshness/conflict/sample penalties)
- **Impact / Confidence / Effort / Risk:** Very High / Medium-High / Medium / Medium
- **Scope (exact):**
  - `worker/api.ts`
    - add `edge_quality` and `confidence_breakdown` fields to signal/plan responses.
    - add explicit `conflict_state` when regime posture and signal posture disagree.
  - `src/config/indicator-sla.ts`
    - define stale-data penalty thresholds and weights.
  - `frontend/src/App.tsx`
    - add confidence decomposition view (`data_quality`, `model_agreement`, `regime_stability`).
- **Implementation steps (ship plan):**
  1. Define baseline confidence and deterministic penalties (stale count/age, disagreement, tiny sample sizes).
  2. Compute normalized `edge_quality` in `[0, 1]` and expose decomposition fields.
  3. Add UI semantics for low edge quality even when directional signal is present.
  4. Add guardrails to avoid increasing risk budget when edge quality is below threshold.
- **Validation commands:**
  - `curl -sS https://api.pxicommand.com/api/pxi | jq '.dataFreshness.staleCount'`
  - `curl -sS https://api.pxicommand.com/api/signal | jq '{signal: .signal, regime: .regime, edge_quality, confidence_breakdown, conflict_state}'`
  - `cd /Users/scott/pxi && npm run build`
- **Expected pass criteria:**
  - `edge_quality` moves monotonically down under higher stale-data conditions.
  - `conflict_state` is explicit for disagreement states instead of inferred by users.
  - UI presents decomposed confidence, not a single opaque confidence label.
- **Confidence:** Medium-High.
- **Rollback plan:**
  - Revert confidence-penalty and conflict-state fields and restore prior signal payload semantics.

### P0-9 (CLOSED 2026-02-17) — Route consistency hardening for brief/opportunities/alerts feed
- **Impact / Confidence / Effort / Risk:** High / High / Low-Medium / Low-Medium
- **Scope (exact):**
  - `worker/api.ts`
    - enforce stable behavior for `GET /api/brief`, `GET /api/opportunities`, `GET /api/alerts/feed`.
    - return typed fallback responses instead of opaque `404` where possible.
  - `worker/wrangler.toml`
    - make feature flags explicit by environment for brief/opportunity/in-app alerts.
  - `frontend/src/App.tsx`
    - add graceful degraded-state rendering when optional data is unavailable.
- **Implementation steps (ship plan):**
  1. Verify production route handlers are deployed and mapped to current worker revision.
  2. Replace "not found" user experience with typed empty/degraded payloads.
  3. Add contract smoke checks to fail deploy if routes regress to 404.
  4. Wire degraded UI copy so traders still get a usable daily plan.
- **Validation commands:**
  - `for u in /api/brief /api/opportunities /api/alerts/feed; do curl -sS -o /tmp/out -w \"%{http_code} $u\\n\" https://api.pxicommand.com$u; done`
  - `curl -sS https://api.pxicommand.com/api/brief | jq '{as_of, summary}'`
  - `curl -sS https://api.pxicommand.com/api/opportunities?horizon=7d | jq '{as_of, horizon, item_count: (.items | length)}'`
- **Expected pass criteria:**
  - 404 rate for the three routes drops to zero.
  - Response payloads are schema-stable and consumable by frontend.
  - Frontend remains functional under partial-data states.
- **Confidence:** High.
- **Rollback plan:**
  - Revert route-contract hardening and restore prior feature-flag behavior if regressions appear.

### P0-1 (CLOSED previous cycle) — Prevent same-day duplicate prediction rows from reruns
- **Impact / Confidence / Effort / Risk:** High / High / Low / Low
- **Scope (exact):**
  - `src/evaluation.ts` → `storePredictions()`
  - `src/db.ts` → `getExistingPredictionThemesForDate()`
  - `tests/unit/evaluation_store_predictions.test.ts`
- **Validation commands (already run):**
  - `cd /Users/scott/pxi/signals && npx vitest run tests/unit/evaluation_store_predictions.test.ts`
  - `cd /Users/scott/pxi/signals && npm test`
  - `cd /Users/scott/pxi/signals && npm run lint`
- **Rollback plan:**
  - Revert `src/evaluation.ts`, `src/db.ts`, and `tests/unit/evaluation_store_predictions.test.ts`.

### P0-2 (CLOSED this cycle) — De-bias read paths from historical duplicate same-day rows
- **Impact / Confidence / Effort / Risk:** High / High / Medium / Low-Medium
- **Scope (exact):**
  - `src/db.ts`
    - Added canonical dedupe CTE (`ROW_NUMBER() OVER (PARTITION BY signal_date, theme_id ORDER BY created_at, id)`)
    - Updated:
      - `getPendingPredictions()`
      - `getAccuracyStats()`
      - `listPredictions()`
      - `getExistingPredictionThemesForDate()`
  - `tests/unit/db_prediction_dedupe.test.ts`
- **Implementation steps (completed):**
  1. Defined canonical-row policy for duplicate logical signals (`signal_date + theme_id`).
  2. Applied canonicalized CTE to all prediction read paths and aggregations.
  3. Added unit tests asserting canonical query shape and filtered read behavior.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/db_prediction_dedupe.test.ts`
  - `cd /Users/scott/pxi/signals && npm test`
  - `cd /Users/scott/pxi/signals && npm run lint`
- **Expected pass criteria (met):**
  - Read APIs and evaluation read path consume canonical rows only.
  - Accuracy stats are based on canonicalized predictions.
  - Full suite and lint pass.
- **Rollback plan:**
  - Revert `src/db.ts` and `tests/unit/db_prediction_dedupe.test.ts`.

### P0-3 (CLOSED this cycle) — Hard constraint + cleanup migration for duplicate prevention at DB layer
- **Impact / Confidence / Effort / Risk:** High / Medium-High / Medium / Medium
- **Scope (exact):**
  - `migrations/0005_signal_predictions_uniqueness.sql` (new)
    - backup (`signal_predictions_backup_0005`),
    - canonical cleanup (keep earliest `(created_at, id)` per `(signal_date, theme_id)`),
    - DB-level uniqueness index on `(signal_date, theme_id)`.
  - `src/db.ts`
    - `insertSignalPrediction()` and `insertSignalPredictions()` now use conflict-safe insert:
      - `ON CONFLICT(signal_date, theme_id) DO NOTHING`.
  - `tests/unit/db_prediction_uniqueness_guard.test.ts` (new)
    - asserts single + batch insert paths are conflict-safe.
  - `docs/DEPLOYMENT.md`
    - migration verification SQL for duplicate-count and index presence.
- **Implementation steps (completed):**
  1. Added migration backup and deterministic canonicalization delete for historical duplicate logical keys.
  2. Added DB-level uniqueness enforcement via `idx_signal_predictions_signal_theme_unique`.
  3. Hardened insert paths to ignore logical-key collisions deterministically.
  4. Added focused unit tests for single/batch conflict-safe insertion SQL.
  5. Documented remote verification SQL checks in deploy runbook.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/db_prediction_uniqueness_guard.test.ts` ✅
  - `cd /Users/scott/pxi/signals && npm test` ✅
  - `cd /Users/scott/pxi/signals && npm run lint` ✅
  - `cd /Users/scott/pxi/signals && npx wrangler d1 migrations apply SIGNALS_DB --local --env production` ✅
- **Expected pass criteria (met locally):**
  - Historical duplicate logical rows are removable before constraint activation.
  - New duplicate logical rows are blocked/ignored at DB write layer.
  - Insert path remains idempotent under reruns.
- **Rollback plan:**
  - Restore from `signal_predictions_backup_0005`, drop unique index, revert `src/db.ts` insert SQL and related tests.

### P0-4 (CLOSED this cycle) — Add deploy-parity smoke gate to detect production drift before/after release
- **Impact / Confidence / Effort / Risk:** High / High / Low / Low
- **Scope (exact):**
  - `src/ops/deploy_parity.ts` (new)
    - payload validators for `/api/health` and `/api/accuracy` contracts,
    - duplicate logical-key detector for `/api/predictions` (`signal_date`, `theme_id`).
  - `scripts/deploy_parity_check.ts` (new)
    - one-shot staging/production endpoint gate.
  - `tests/unit/deploy_parity.test.ts` (new)
    - validator + duplicate-detection coverage.
  - `package.json`
    - added `smoke:deploy` script.
  - `docs/DEPLOYMENT.md`
    - post-deploy parity gate command + failure semantics.
- **Implementation steps (completed):**
  1. Added deterministic API-contract validators for freshness and accuracy endpoints.
  2. Added duplicate logical-key detector for prediction feed integrity.
  3. Added executable smoke script: `npm run smoke:deploy -- <base-url>`.
  4. Added unit coverage for success/failure payload shapes and duplicate detection.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/deploy_parity.test.ts` ✅
  - `cd /Users/scott/pxi/signals && npm test` ✅
  - `cd /Users/scott/pxi/signals && npm run lint` ✅
  - `cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals` ❌ (expected currently: deploy drift detected)
- **Expected pass criteria (met for local code):**
  - Release gate fails deterministically when deployed surface is missing required health/CI fields or has duplicate logical predictions.
  - Unit coverage locks validator behavior.
- **Rollback plan:**
  - Revert `src/ops/deploy_parity.ts`, `scripts/deploy_parity_check.ts`, `tests/unit/deploy_parity.test.ts`, `package.json`, and `docs/DEPLOYMENT.md`.

## P1

### P1-13 (CLOSED this cycle) — Add release-candidate artifact checks to prevent partial deploys
- **Impact / Confidence / Effort / Risk:** High / High / Low-Medium / Low
- **Scope (exact):**
  - `src/ops/deploy_parity.ts`
    - extended `VersionValidationOptions` with expected metadata (`expectedWorkerVersion`, `expectedBuildSha`),
    - added deterministic mismatch failures when `/api/version` metadata does not match operator-provided expectations,
    - extended CLI arg parser with:
      - `--expect-worker-version <value>` / `--expect-worker-version=<value>`
      - `--expect-build-sha <value>` / `--expect-build-sha=<value>`
      - explicit missing-value failures for both flags.
  - `scripts/deploy_parity_check.ts`
    - now passes expected metadata into version validator,
    - emits expected metadata in JSON summary (`expected_worker_version`, `expected_build_sha`).
  - `tests/unit/deploy_parity.test.ts`
    - added expected-metadata match + mismatch validator tests,
    - added parser coverage for both new flags (positional and equals forms) and missing-value failures.
  - `docs/DEPLOYMENT.md`
    - added pinned release-candidate parity command using strict mode + expected metadata flags.
- **Implementation steps (completed):**
  1. Added optional expected-artifact metadata checks in parity validator without changing default behavior.
  2. Added robust CLI flag parsing for expected worker/build identifiers.
  3. Wired expected metadata through parity script output for operator auditability.
  4. Added deterministic test coverage for pass/fail parser and validator cases.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/deploy_parity.test.ts` ✅
  - `cd /Users/scott/pxi/signals && npm test` ✅
  - `cd /Users/scott/pxi/signals && npm run lint` ✅
  - `cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals --strict-version --expect-worker-version signals-d69e1d11d936-2026-02-17T13:33:40Z --expect-build-sha d69e1d11d936` ❌ expected fail (live drift still unresolved; `/api/version` and `/api/health` 404)
- **Expected pass criteria (met locally):**
  - Parity check can fail on artifact mismatch even when endpoint schema itself is valid.
  - Existing command behavior remains backward-compatible when expectation flags are omitted.
- **Rollback plan:**
  - Revert expected-artifact parser/validator wiring and related tests/docs if operational friction emerges.

### P1-14 (CLOSED 2026-02-17) — Add DB schema-version parity gate for migration completeness proof
- **Impact / Confidence / Effort / Risk:** High / Medium-High / Medium / Medium
- **Scope (exact):**
  - `scripts/deploy_parity_check.ts`
    - optional `--check-migrations` mode that executes remote verification command(s) for required indexes/columns via Wrangler and appends result into summary.
  - `src/ops/deploy_parity.ts`
    - add parser + evaluator for migration verification output (`idx_signal_predictions_signal_theme_unique`, `exit_price_date`, `evaluation_note`, `pipeline_locks` table).
  - `tests/unit/deploy_parity.test.ts`
    - add parser/validator tests for migration-check pass/fail outputs.
  - `docs/DEPLOYMENT.md`
    - add one-command release gate that combines API parity + migration parity.
- **Implementation steps (ship plan):**
  1. Define deterministic required migration markers (index/table/column names).
  2. Parse remote SQL result payloads into machine-checkable parity evidence.
  3. Fail deploy gate when API surface is green but schema markers are missing.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/deploy_parity.test.ts`
  - `cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals --strict-version`
  - `cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals --strict-version --check-migrations --migration-env production --migration-db SIGNALS_DB` ✅
- **Expected pass criteria (met):**
  - Release gate proves both worker artifact parity and DB migration completeness.
  - Eliminates class of false-greens where code deploys but migration set is partial.
- **Confidence:** Medium-High.
- **Rollback plan:**
  - Keep migration checks optional and revert checker changes if remote command parsing proves unstable.

### P1-12 (CLOSED this cycle) — Expose evaluation completeness metrics in `/api/accuracy` (quantify unresolved-exit attrition)
- **Impact / Confidence / Effort / Risk:** Medium-High / High / Low-Medium / Low
- **Scope (exact):**
  - `src/db.ts`
    - extended `AccuracyStats` with:
      - `evaluated_total`
      - `resolved_total`
      - `unresolved_total`
      - `unresolved_rate`
    - `getAccuracyStats()` now runs an explicit completeness query (`COUNT(*) evaluated_total`, unresolved `CASE WHEN hit IS NULL`) over canonical predictions.
  - `src/routes.ts`
    - `GET /api/accuracy` now returns:
      - `evaluated_count`, `resolved_count`, `unresolved_count`, `unresolved_rate`
    - preserves existing hit-rate/CI payload contract.
  - `src/ops/deploy_parity.ts`
    - `validateAccuracyPayload()` now requires and type-checks completeness fields.
  - Tests:
    - `tests/unit/routes_accuracy_intervals.test.ts`
    - `tests/unit/db_prediction_dedupe.test.ts`
    - `tests/unit/deploy_parity.test.ts`
    - `tests/unit/scheduled_locking.test.ts` (mock shape alignment)
  - Docs:
    - `docs/API.md`, `docs/SCORING.md`
- **Implementation steps (completed):**
  1. Added evaluated/resolved/unresolved accounting in DB accuracy aggregation path.
  2. Exposed completeness counters and unresolved-rate percent in `/api/accuracy`.
  3. Hardened deploy parity validation to detect missing completeness fields in live/staging payloads.
  4. Updated tests and docs to lock contract + interpretation.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/routes_accuracy_intervals.test.ts tests/unit/db_prediction_dedupe.test.ts tests/unit/deploy_parity.test.ts tests/unit/scheduled_locking.test.ts` ✅
  - `cd /Users/scott/pxi/signals && npm test` ✅ (33 files, 340 tests)
  - `cd /Users/scott/pxi/signals && npm run lint` ✅
  - `cd /Users/scott/pxi/signals && npm run offline` ✅
- **Expected pass criteria (met locally):**
  - Accuracy API exposes denominator attrition transparently.
  - Operators can distinguish genuine hit-rate changes from unresolved-exit coverage changes.
  - Parity gate fails when deployed accuracy payload omits completeness fields.
- **Rollback plan:**
  - Revert `src/db.ts`, `src/routes.ts`, `src/ops/deploy_parity.ts`, related tests/docs.

### P1-8 (CLOSED this cycle) — Sanitize `POST /api/run` 500 errors to avoid internal failure leakage
- **Impact / Confidence / Effort / Risk:** Medium / High / Low / Low
- **Scope (exact):**
  - `src/routes.ts`
    - `POST /api/run` non-lock failure path now returns fixed external error code (`run_failed`) instead of surfacing raw exception messages.
    - Internal diagnostic detail remains captured via `insertRun(... error_message)`.
  - `tests/unit/routes_run_lock_conflict.test.ts`
    - updated non-lock failure assertion to require sanitized response,
    - added assertion that internal run-row capture preserves original error message for operators.
  - `docs/API.md`
    - updated admin endpoint error contract and common error table.
- **Implementation steps (completed):**
  1. Replaced raw exception passthrough in admin-run 500 responses with deterministic `run_failed` code.
  2. Preserved internal observability by keeping original exception message in persisted `runs.error_message`.
  3. Updated unit tests to enforce public-sanitized/private-detailed split.
  4. Updated API docs to make sanitized contract explicit.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/routes_run_lock_conflict.test.ts` ✅
  - `cd /Users/scott/pxi/signals && npm test` ✅
  - `cd /Users/scott/pxi/signals && npm run lint` ✅
- **Expected pass criteria (met):**
  - API callers do not receive raw internal exception strings on admin-trigger failures.
  - Operators retain full failure context in internal run metadata for debugging.
  - Lock-contention behavior (`409 pipeline_locked`) remains unchanged.
- **Rollback plan:**
  - Revert `src/routes.ts`, `tests/unit/routes_run_lock_conflict.test.ts`, and `docs/API.md`.

### P1-9 (CLOSED this cycle) — Add explicit no-store cache headers for operational API endpoints
- **Impact / Confidence / Effort / Risk:** Medium / High / Low / Low
- **Scope (exact):**
  - `src/routes.ts`
    - extended `jsonResponse(...)` helper to support explicit response headers,
    - applied `Cache-Control: no-store` to:
      - `GET /api/health` (success + error),
      - `GET /api/accuracy` (success + error),
      - `GET /api/predictions` (success + validation/error paths),
      - `POST /api/run` (success + `401` + `409` + `429` + `500`).
  - `tests/unit/routes_health.test.ts`
    - asserts `Cache-Control: no-store` on success path.
  - `tests/unit/routes_accuracy_intervals.test.ts`
    - asserts `Cache-Control: no-store` on success path.
  - `tests/unit/routes_predictions_query_validation.test.ts`
    - asserts `Cache-Control: no-store` on success path.
  - `tests/unit/routes_run_lock_conflict.test.ts`
    - asserts `Cache-Control: no-store` on `409` and `500` responses.
  - `docs/API.md`
    - added explicit cache policy section for operational/admin endpoints.
- **Implementation steps (completed):**
  1. Extended the JSON response helper with optional headers.
  2. Added deterministic no-store cache control to operational/admin responses.
  3. Added focused route-header assertions in existing test suites.
  4. Documented endpoint cache semantics in API reference.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/routes_health.test.ts tests/unit/routes_accuracy_intervals.test.ts tests/unit/routes_predictions_query_validation.test.ts tests/unit/routes_run_lock_conflict.test.ts` ✅
  - `cd /Users/scott/pxi/signals && npm test` ✅
  - `cd /Users/scott/pxi/signals && npm run lint` ✅
- **Expected pass criteria (met):**
  - Operational status/accuracy/prediction reads are not intermediary-cacheable.
  - Admin trigger responses (including lock/failure/error paths) are not cacheable.
  - Existing payload contracts remain unchanged.
- **Rollback plan:**
  - Revert `src/routes.ts`, route test updates, and API docs cache section.

### P1-10 (CLOSED this cycle) — Publish a deploy artifact manifest endpoint for verifiable release provenance
- **Impact / Confidence / Effort / Risk:** Medium-High / Medium-High / Medium / Low-Medium
- **Scope (exact):**
  - `src/routes.ts`
    - added `GET /api/version` with immutable release metadata fields.
  - `src/config.ts`
    - added deploy metadata parsing/normalization for `BUILD_SHA`, `BUILD_TIMESTAMP`, `WORKER_VERSION`.
  - `wrangler.toml`
    - wired metadata vars in staging + production env blocks.
  - `src/ops/deploy_parity.ts`
    - added `validateVersionPayload()` validator.
  - `scripts/deploy_parity_check.ts`
    - parity gate now checks `/api/version` contract in addition to health/accuracy/predictions.
  - `tests/unit/routes_version.test.ts` (new)
  - `tests/unit/deploy_parity.test.ts`
  - `tests/unit/config.test.ts`
  - `docs/API.md`, `docs/DEPLOYMENT.md`
- **Implementation steps (completed):**
  1. Added read-only manifest endpoint (`/api/version`) with `api_contract_version`, `worker_version`, `build_sha`, `build_timestamp`.
  2. Extended deploy parity validator/smoke script to fail on missing or malformed manifest contract.
  3. Added route + validator + config normalization tests.
  4. Updated API/deployment docs with metadata contract and deploy var injection commands.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/routes_version.test.ts tests/unit/deploy_parity.test.ts tests/unit/config.test.ts` ✅
  - `cd /Users/scott/pxi/signals && npm test` ✅
  - `cd /Users/scott/pxi/signals && npm run lint` ✅
  - `cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals` ❌ expected fail (live deploy drift remains)
- **Expected pass criteria (met locally):**
  - Manifest endpoint exists with stable schema and no-store cache policy.
  - Parity gate now detects manifest drift/missing metadata in addition to existing API contract checks.
  - No production-path pipeline logic change.
- **Rollback plan:**
  - Revert manifest route/config/parity/test/doc changes; restore previous parity checker scope.

### P1-11 (CLOSED this cycle) — Enforce non-placeholder deploy metadata quality at release time
- **Impact / Confidence / Effort / Risk:** Medium / High / Low-Medium / Low
- **Scope (exact):**
  - `src/ops/deploy_parity.ts`
    - added strict validation options to `validateVersionPayload(...)`,
    - added `parseDeployParityArgs(...)` for deterministic CLI parsing.
  - `scripts/deploy_parity_check.ts`
    - added `--strict-version` support,
    - strict-mode toggle now passed into version payload validator,
    - parity summary now reports `strict_version` mode.
  - `tests/unit/deploy_parity.test.ts`
    - added strict-mode pass/fail coverage for placeholder metadata,
    - added CLI arg parsing coverage (default, strict, unknown flag, extra positional).
  - `docs/DEPLOYMENT.md`
    - documented strict production parity gate command.
- **Implementation steps (completed):**
  1. Implemented strict-mode parity flag (`--strict-version`) without changing default smoke behavior.
  2. Added semantic placeholder rejection for version metadata in strict mode (`signals-dev`, `local-dev`, all-zero SHA, epoch-like timestamps).
  3. Added deterministic CLI parsing with explicit erroring on unknown flags/extra args.
  4. Added focused validator + parser test coverage.
  5. Updated deployment runbook with strict production gate command.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/deploy_parity.test.ts` ✅
  - `cd /Users/scott/pxi/signals && npm test` ✅
  - `cd /Users/scott/pxi/signals && npm run lint` ✅
- **Expected pass criteria (met):**
  - Production parity checks can now fail on semantically-placeholder release metadata even when JSON schema is otherwise valid.
  - Local/dev smoke checks remain backward-compatible when strict flag is omitted.
- **Rollback plan:**
  - Revert strict validation/parser changes in `src/ops/deploy_parity.ts`, `scripts/deploy_parity_check.ts`, test updates, and deployment docs.

### P1-1 (CLOSED this cycle) — Replace static Monday holiday list with algorithmic US market holiday calendar + yearly tests
- **Impact / Confidence / Effort / Risk:** Medium-High / High / Medium / Low
- **Scope (exact):**
  - `src/worker.ts`
    - Added algorithmic NYSE holiday engine:
      - `getNyseHolidaySet(year)`
      - `isNyseHoliday(date)`
      - Observed-date handling for fixed holidays
      - Good Friday via Gregorian Easter calculation
      - Exceptional-closure override set
    - Updated `shouldRunScheduledPipeline()` to use computed holiday checks instead of static date set.
  - `tests/unit/worker_schedule.test.ts`
    - Added deterministic yearly coverage for 2025–2032 Monday closures.
    - Added Good Friday and observed fixed-date holiday tests.
- **Implementation steps (completed):**
  1. Replaced static date list with rule-based NYSE holiday computation.
  2. Added observed-date logic and cross-year New Year edge handling.
  3. Added explicit exceptional closure override list.
  4. Expanded unit tests to lock expected dates and scheduler behavior.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/worker_schedule.test.ts`
  - `cd /Users/scott/pxi/signals && npm test`
  - `cd /Users/scott/pxi/signals && npm run lint`
- **Expected pass criteria (met):**
  - No annual manual holiday list maintenance required.
  - Tuesday fallback triggers correctly after all relevant Monday market closures (including observed fixed-date closures).
  - Full test suite and lint pass.
- **Rollback plan:**
  - Revert `src/worker.ts` and `tests/unit/worker_schedule.test.ts` to previous static-list logic.

### P1-2 (CLOSED this cycle) — Harden query-param bounds on read APIs (`limit`, booleans)
- **Impact / Confidence / Effort / Risk:** Medium / High / Low / Low
- **Scope (exact):**
  - `src/routes.ts`
    - Added strict query parser for `/api/predictions`:
      - integer-only `limit` parsing,
      - clamp `limit` to `[1, 100]`,
      - deterministic `400` for malformed `limit` values,
      - deterministic `400` for malformed `evaluated` values.
  - `tests/unit/routes_predictions_query_validation.test.ts` (new)
- **Implementation steps (completed):**
  1. Added `parsePredictionsQuery()` helper for explicit query validation and normalization.
  2. Rejected malformed `evaluated` values with explicit `400` (`true|false` only).
  3. Rejected malformed `limit` values (non-integer formats), and clamped valid integer values to `[1, 100]`.
  4. Added route tests for defaults, clamp behavior, and invalid input handling.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/routes_predictions_query_validation.test.ts`
  - `cd /Users/scott/pxi/signals && npm test`
  - `cd /Users/scott/pxi/signals && npm run lint`
- **Expected pass criteria (met):**
  - API does not pass ambiguous pagination values into DB query layer.
  - Invalid boolean/filter input returns deterministic `400` without hitting DB path.
  - Full test suite and lint pass.
- **Rollback plan:**
  - Revert `src/routes.ts` and `tests/unit/routes_predictions_query_validation.test.ts`.

### P1-3 (CLOSED this cycle) — Expose explicit pipeline freshness health signal
- **Impact / Confidence / Effort / Risk:** Medium-High / High / Low-Medium / Low
- **Scope (exact):**
  - `src/db.ts`
    - Added `PipelineFreshness` type + `getPipelineFreshness()` helper (reuses `getLatestSuccessfulRun()`).
  - `src/routes.ts`
    - Added read-only endpoint `GET /api/health` returning freshness fields.
    - Added fixed threshold constant (`HEALTH_STALE_THRESHOLD_DAYS = 8`).
  - `tests/unit/routes_health.test.ts` (new)
    - fresh/stale/no-history/error cases for `/api/health`.
  - `tests/unit/db_pipeline_freshness.test.ts` (new)
    - deterministic helper tests for fresh/stale/no-history/invalid timestamp.
  - `docs/API.md`, `docs/DEPLOYMENT.md`
    - endpoint contract + deployment smoke check.
- **Implementation steps (completed):**
  1. Added typed freshness helper returning deterministic states (`ok | stale | no_history`).
  2. Exposed `/api/health` with `latest_success_at`, `hours_since_success`, `threshold_days`, `is_stale`, `status`.
  3. Added route + helper unit coverage.
  4. Documented API behavior and deployment verification step.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/routes_health.test.ts tests/unit/db_pipeline_freshness.test.ts`
  - `cd /Users/scott/pxi/signals && npm test`
  - `cd /Users/scott/pxi/signals && npm run lint`
- **Expected pass criteria (met):**
  - Operators can detect stale/no-history pipeline state without manual DB queries.
  - Freshness semantics are deterministic and test-covered.
- **Rollback plan:**
  - Revert `src/db.ts`, `src/routes.ts`, `tests/unit/routes_health.test.ts`, `tests/unit/db_pipeline_freshness.test.ts`, and doc updates.

### P1-4 (CLOSED this cycle) — Add uncertainty bands to `/api/accuracy` to reduce false confidence
- **Impact / Confidence / Effort / Risk:** Medium-High / High / Medium / Low-Medium
- **Scope (exact):**
  - `src/db.ts`
    - Added Wilson interval helper (`computeWilsonInterval`) and CI-enriched `AccuracyBucketStats`.
    - `getAccuracyStats()` now returns `hit_rate_ci_low` / `hit_rate_ci_high` for overall and grouped buckets.
  - `src/routes.ts`
    - Extended `GET /api/accuracy` payload with interval fields, `minimum_recommended_sample_size`, and `sample_size_warning`.
  - `tests/unit/routes_accuracy_intervals.test.ts` (new)
    - normal/low-sample/zero-sample/error route coverage.
  - `tests/unit/db_accuracy_intervals.test.ts` (new)
    - deterministic Wilson interval helper coverage + bounds clamping.
  - `docs/API.md`, `docs/SCORING.md`
    - endpoint contract + Wilson interval interpretation guidance.
- **Implementation steps (completed):**
  1. Implemented 95% Wilson interval computation for hit-rate proportions.
  2. Added CI fields to accuracy aggregates in DB layer.
  3. Exposed CI + sample-size warnings in API response with deterministic one-decimal formatting.
  4. Added route tests for high-sample, low-sample, zero-sample, and failure cases.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/db_accuracy_intervals.test.ts tests/unit/routes_accuracy_intervals.test.ts`
  - `cd /Users/scott/pxi/signals && npm test`
  - `cd /Users/scott/pxi/signals && npm run lint`
- **Expected pass criteria (met):**
  - API consumers can distinguish statistically weak from robust hit-rate estimates.
  - Zero/low-sample scenarios are explicit and non-misleading.
- **Rollback plan:**
  - Revert `src/db.ts`, `src/routes.ts`, `tests/unit/db_accuracy_intervals.test.ts`, `tests/unit/routes_accuracy_intervals.test.ts`, and doc updates.

### P1-5 (CLOSED this cycle) — Use trading-day target dates for evaluation horizon (remove weekend/holiday drift)
- **Impact / Confidence / Effort / Risk:** Medium-High / High / Medium / Low-Medium
- **Scope (exact):**
  - `src/utils/calendar.ts` (new)
    - extracted shared NYSE holiday + trading-day helpers:
      - `getNyseHolidaySet()`
      - `isNyseHolidayDate()`
      - `addTradingDays()`
  - `src/worker.ts`
    - removed duplicated in-file holiday engine,
    - imported shared helper module,
    - preserved worker exports for schedule tests (`getNyseHolidaySet`, `isNyseHoliday`).
  - `src/evaluation.ts`
    - replaced calendar-day target date logic with trading-day-aware `calculateTargetDate()` via `addTradingDays()`.
  - `tests/unit/evaluation_target_date.test.ts` (new)
    - +7 trading-day horizon,
    - weekend + Monday-holiday skips,
    - Good Friday skip behavior.
  - `tests/unit/worker_schedule.test.ts`
    - added parity assertion for worker helper vs shared calendar util.
  - `docs/SCORING.md`
    - documented +7 trading-day evaluation horizon policy.
- **Implementation steps (completed):**
  1. Extracted holiday/calendar logic into shared utility module with deterministic UTC date parsing.
  2. Switched prediction `target_date` calculation to +7 NYSE trading days.
  3. Added holiday-adjacent horizon tests (Presidents Day, Labor Day, Good Friday).
  4. Verified scheduler logic parity after helper extraction.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/evaluation_target_date.test.ts tests/unit/worker_schedule.test.ts`
  - `cd /Users/scott/pxi/signals && npm test`
  - `cd /Users/scott/pxi/signals && npm run lint`
- **Expected pass criteria (met):**
  - Prediction evaluation horizon reflects actual trading sessions.
  - No weekend/holiday calendar drift in measured holding period.
  - Scheduler holiday behavior remains unchanged.
- **Rollback plan:**
  - Revert `src/utils/calendar.ts`, `src/evaluation.ts`, `src/worker.ts`, and test/doc updates.

### P1-6 (CLOSED this cycle) — Add run-level concurrency guard to prevent overlapping pipeline executions
- **Impact / Confidence / Effort / Risk:** High / Medium-High / Medium / Medium
- **Scope (exact):**
  - `migrations/0004_pipeline_lock.sql` (new)
    - added `pipeline_locks` table + `idx_pipeline_locks_acquired_at` index.
  - `src/db.ts`
    - added lock helpers:
      - `acquirePipelineLock(env, lockKey, lockToken, nowIso, ttlSeconds)`
      - `releasePipelineLock(env, lockKey, lockToken)`
  - `src/scheduled.ts`
    - wrapped `runPipeline()` with lock acquire/release (`try/finally`),
    - added typed `PipelineLockError` + `isPipelineLockError()`.
  - `src/routes.ts`
    - lock contention on `POST /api/run` now returns `409 { ok:false, error:"pipeline_locked" }`.
  - `src/worker.ts`
    - scheduled lock contention now logs warning and exits without writing an error run row.
  - `tests/unit/scheduled_locking.test.ts` (new)
  - `tests/unit/routes_run_lock_conflict.test.ts` (new)
  - `tests/unit/worker_scheduled_lock_skip.test.ts` (new)
  - `docs/API.md`, `docs/DEPLOYMENT.md`
    - documented `409 pipeline_locked` and lock migration application command.
- **Implementation steps (completed):**
  1. Added D1 migration for lock table/index.
  2. Implemented stale-lock cleanup + unique-key acquire semantics in DB helper layer.
  3. Wrapped pipeline execution with lock lifecycle and safe release logging.
  4. Mapped admin-run lock contention to deterministic `409`.
  5. Prevented scheduled lock-contention from generating false error telemetry.
  6. Added focused unit coverage for lock acquire/release and conflict handling paths.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/scheduled_locking.test.ts tests/unit/routes_run_lock_conflict.test.ts tests/unit/worker_scheduled_lock_skip.test.ts` ✅
  - `cd /Users/scott/pxi/signals && npm test` ✅ (29 files, 320 tests)
  - `cd /Users/scott/pxi/signals && npm run lint` ✅
  - `cd /Users/scott/pxi/signals && npx wrangler d1 migrations apply SIGNALS_DB --local --env production` ✅
- **Expected pass criteria (met):**
  - Concurrent cron/manual triggers cannot execute full pipeline simultaneously.
  - Lock releases on success/failure with stale-lock auto-recovery policy.
  - Scheduled contention no longer emits false `runs.status=error` rows.
- **Rollback plan:**
  - Revert migration + lock helpers + route/scheduler handling; roll forward with prior schema and behavior.

### P1-7 (MOVED) — Deploy drift closure execution has been re-ranked as **P0-6**
- Maintained for historical continuity; use P0-6 as canonical execution item.

## P2

### P2-2 (CLOSED this cycle) — Build deterministic validation primitives for walk-forward, rank-IC, and multiple-testing control
- **Impact / Confidence / Effort / Risk:** Medium-High / High / Medium / Low
- **Scope (exact):**
  - `src/evaluation_validation.ts` (new)
    - `computeWalkForwardSlices()`
    - `computeSpearmanCorrelation()` (tie-aware rank handling)
    - `computeRankICSeries()`
    - `computeHitRateIntervals()`
    - `computeMultipleTestingAdjustedPvalues()` (Holm step-down baseline, Reality-Check/SPA-ready interface)
  - `tests/unit/evaluation_validation.test.ts` (new)
    - walk-forward leakage guard + deterministic split coverage,
    - rank-IC/spearman behavior coverage,
    - hit-rate interval summary coverage,
    - multiple-testing adjusted p-value correctness + validation failures.
  - `docs/SCORING.md`
    - added validation protocol primitives section + primary-source links.
- **Implementation steps (completed):**
  1. Added pure, non-production-coupled evaluation-validation module for temporal slicing and inference hygiene.
  2. Added tie-aware Spearman + per-date rank-IC series output to quantify temporal ranking stability.
  3. Added hit-rate interval helper wrapping Wilson CI with explicit sample-size warning semantics.
  4. Added Holm step-down adjusted p-value path as conservative multiple-testing baseline.
  5. Added deterministic unit tests across success and failure paths.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/evaluation_validation.test.ts` ✅
  - `cd /Users/scott/pxi/signals && npm test` ✅
  - `cd /Users/scott/pxi/signals && npm run lint` ✅
- **Expected pass criteria (met):**
  - Validation primitives produce deterministic outputs and reject malformed hypothesis inputs.
  - Temporal split utility guarantees train/test non-overlap.
  - Multiple-testing adjustment path is test-covered and ready for report integration.
- **Rollback plan:**
  - Revert `src/evaluation_validation.ts`, `tests/unit/evaluation_validation.test.ts`, and `docs/SCORING.md` updates.

### P2-1 (CLOSED this cycle) — Add robust offline evaluation report protocol (walk-forward + multiple-testing controls)
- **Impact / Confidence / Effort / Risk:** High / Medium-High / Medium / Low-Medium
- **Scope (exact):**
  - `src/ops/evaluation_report.ts` (new)
    - CLI parsing for deterministic offline report runs,
    - defensive payload normalization (`predictions[]` or array input),
    - exact two-sided binomial p-value helper,
    - full-sample + walk-forward slice report assembly,
    - Holm-adjusted multiple-testing table for slice + subgroup hypotheses,
    - markdown renderer with primary-source citation links.
  - `scripts/evaluation_report.ts` (new)
    - reads offline outcomes JSON,
    - emits `out/evaluation/report.json` and `out/evaluation/report.md`.
  - `tests/unit/evaluation_report.test.ts` (new)
    - CLI parser defaults/overrides/unknown-flag behavior,
    - binomial p-value determinism,
    - end-to-end report assembly on deterministic sample payload.
  - `data/evaluation_sample_predictions.json` (new)
    - deterministic sample input fixture for script dry runs.
  - `package.json`
    - added `report:evaluation` script.
  - `docs/SCORING.md`
    - added report command usage + output contract notes.
- **Implementation steps (completed):**
  1. Added pure offline report logic to isolate research/evaluation from production request paths.
  2. Wired deterministic CLI + script outputs for machine-readable and human-readable artifacts.
  3. Added exact-binomial + Holm-adjusted inference path for basic multiple-testing hygiene.
  4. Added focused unit coverage and sample fixture for reproducible runs.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/evaluation_report.test.ts` ✅
  - `cd /Users/scott/pxi/signals && npm run report:evaluation -- --input data/evaluation_sample_predictions.json --out out/evaluation --min-train 4 --test-size 2 --step-size 2 --minimum-sample 6` ✅
  - `cd /Users/scott/pxi/signals && npm test` ✅
  - `cd /Users/scott/pxi/signals && npm run lint` ✅
- **Expected pass criteria (met):**
  - Script emits deterministic JSON + markdown artifacts with walk-forward metrics and adjusted p-values.
  - No production runtime behavior changes.
  - Unit tests cover parser, statistical helper, and report assembly edge cases.
- **Rollback plan:**
  - Revert `src/ops/evaluation_report.ts`, `scripts/evaluation_report.ts`, fixture, tests, docs, and `package.json` script entry.

### P2-3 (CLOSED 2026-02-17) — Add minimum-history governance thresholds to report promotion policy
- **Impact / Confidence / Effort / Risk:** Medium / Medium / Low-Medium / Low
- **Scope (exact):**
  - `src/ops/evaluation_report.ts`
    - add configurable no-go gates (e.g., min resolved count, max unresolved rate, min slices).
  - `docs/SCORING.md`
    - codify hard fail / advisory-only thresholds for model-promotion decisions.
  - `tests/unit/evaluation_report.test.ts`
    - add gate behavior tests.
- **Implementation steps (ship plan):**
  1. Define explicit thresholds for report readiness and promotion safety.
  2. Emit deterministic `governance_status` block in report artifacts.
  3. Fail/flag with actionable reasons when thresholds are not met.
- **Validation commands (run):**
  - `cd /Users/scott/pxi/signals && npm test -- tests/unit/evaluation_report.test.ts`
  - `cd /Users/scott/pxi/signals && npm run report:evaluation -- --input data/evaluation_sample_predictions.json --out out/evaluation --min-train 4 --test-size 2 --step-size 2`
  - `cd /Users/scott/pxi/signals && npm run report:evaluation -- --input data/evaluation_sample_predictions.json --out out/evaluation` ❌ expected governance no-go with deterministic reasons (`resolved observations 24 below minimum 30`, `walk-forward slices 0 below minimum 3`)
  - `cd /Users/scott/pxi/signals && npm run report:evaluation -- --input data/evaluation_sample_predictions.json --out out/evaluation-strict --min-resolved 999 --max-unresolved-rate 0 --min-slices 999` ❌ expected governance no-go with stricter deterministic reasons
- **Expected pass criteria (met):**
  - Report includes machine-checkable go/no-go status for promotion.
  - Threshold breaches are explicit and reproducible.
- **Confidence:** Medium.
- **Rollback plan:**
  - Revert governance-status additions in report module/tests/docs if thresholds prove misleading.
