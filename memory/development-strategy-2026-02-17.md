# Development Strategy Report — PXI Signals
Date: 2026-02-17
Scope: `/Users/scott/pxi` with primary focus on `/Users/scott/pxi/signals`

## Executive Summary
- The Signals stack is currently operationally healthy: tests/lint pass, deploy parity passes for both production and staging, and live health is fresh.
- Core reliability risk remains external-source fragility (historical Reddit 403/429 error runs still visible in run history).
- The most important architectural gap is **environment isolation**: staging and production currently point to the same D1, R2, and KV resources in `signals/wrangler.toml`.
- Release validation is strong (strict version + migration marker checks), but promotion flow can be tightened to reduce schema/version race risk.
- Prediction accuracy now has enough initial sample to monitor (n=40, hit rate 52.5%), but confidence intervals are still wide for subgroup slices.
- Development should prioritize reliability and data-quality guardrails before adding model complexity.
- The next 30 days should center on five ship-ready PRs: infra isolation, ingestion fail-soft behavior, release-order hardening, signal quality gates, and evaluation diagnostics.
- Research-to-code flow should be formalized into hypothesis cards, offline replay tests, staging canary, and explicit promotion gates.
- Add richer health telemetry (source coverage, failed subreddits, degraded-mode flag) so incidents are diagnosable from API surface alone.
- Keep changes backward-compatible and rollout-safe: additive migrations first, then code paths, then strict parity gate, then production promotion.
- Treat ranking quality as a monitored product metric (not just a model output): track hit rate, unresolved rate, and rank-vs-return correlation.
- If executed, this plan should reduce run-failure risk, improve confidence in alpha quality, and make releases auditable and reversible.

## Current System Health (with evidence)
### 1) Local quality gates (required commands)
- `cd /Users/scott/pxi/signals && npm test` ✅
  - Result: **35 test files passed, 363 tests passed, 0 failed**.
- `cd /Users/scott/pxi/signals && npm run lint` ✅
  - Result: **pass** (`eslint .` exited successfully).

### 2) Deploy parity checks (required commands)
- `cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals --strict-version --check-migrations --migration-env production --migration-db SIGNALS_DB` ✅
  - Result: **pass=true**, endpoint checks all 200 (`/api/version`, `/api/health`, `/api/accuracy`, `/api/predictions`), migration failures empty.
- `cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals-staging --strict-version --check-migrations --migration-env staging --migration-db SIGNALS_DB` ✅
  - Result: **pass=true**, same endpoint and migration checks passed.

### 3) Live API state snapshots
- Production health (`/signals/api/health`):
  - `latest_success_at=2026-02-17T15:00:04.725Z`
  - `hours_since_success=0.13`
  - `status=ok`, `is_stale=false`
- Production accuracy (`/signals/api/accuracy`):
  - `sample_size=40`, `hit_rate=52.5%`, Wilson CI `37.5%–67.1%`, `unresolved_rate=0.0%`
- Production error history (`/signals/api/runs?status=error`):
  - Includes `Reddit fetch failed: 403` (2026-02-09) and `429` (2026-01-20)

### 4) Architecture and deployment context inspected
- `signals/docs/ARCHITECTURE.md`, `signals/docs/DEPLOYMENT.md`, `signals/docs/SCORING.md`, `signals/README.md`
- `signals/src/{worker.ts,scheduled.ts,routes.ts,db.ts,reddit/reddit_client.ts,evaluation.ts}`
- `.github/workflows/{ci.yml,deploy-signals.yml}`

### 5) Material finding
- In `signals/wrangler.toml`, staging and production currently share:
  - same D1 database id,
  - same R2 bucket,
  - same KV namespace.
- This removes true pre-prod isolation and increases blast radius of staging experiments.

## Top 10 Prioritized Development Moves (P0/P1/P2, impact, effort, risk)
| Priority | Move | Impact | Effort | Risk | Why now |
|---|---|---:|---:|---:|---|
| P0 | Split staging/prod D1, R2, KV resources | High | M | Low-Med | Current shared resources undermine safe staging and can contaminate prod data. |
| P0 | Add ingest fail-soft mode with explicit source quorum + cached fallback | High | M | Med | Historical 403/429 failures are the dominant observed incident mode. |
| P0 | Enforce release ordering: migration-safe flow + pinned artifact parity | High | M | Low | Prevent schema/version drift during deploy and make rollback deterministic. |
| P0 | Add run quality gates (min docs, min subreddit coverage, min evidence) | High | M | Med | Prevent low-quality/noisy runs from being published as actionable signals. |
| P1 | Extend health endpoint with source diagnostics (`failed_subreddits`, `coverage_pct`, `degraded`) | Med-High | S-M | Low | Faster incident triage; enables automated freshness/quality watchdogs. |
| P1 | Add deterministic replay regression set from saved `raw.json` artifacts | Med-High | M | Low | Locks alpha behavior against accidental scoring/threshold regressions. |
| P1 | Add ranking-quality diagnostics (Spearman rank-vs-return + cohort drift) | Med | M | Low-Med | Converts model claims into measurable, ongoing validation. |
| P1 | Stage-first promotion workflow (staging deploy + parity + manual prod approval) | Med-High | M | Low | Production safety improves without changing analysis logic. |
| P2 | Introduce price-provider abstraction + provider fallback policy | Med | M-L | Med | Reduces single-provider dependency in evaluation step. |
| P2 | Theme governance + research registry (change log, rationale, expiry review) | Med | M | Low | Keeps alpha taxonomy coherent as research ideas scale. |

## 30-Day Delivery Plan (week-by-week)
### Week 1 — Reliability Foundation
- Ship infra isolation PR (distinct staging bindings in Wrangler + docs update).
- Ship release-process PR (staging-first gate + pinned SHA/version checks in CI path).
- Add/verify runbook updates for deploy + rollback with parity commands.
- Exit criteria:
  - staging/prod parity commands pass against independent resources,
  - no shared D1/R2/KV IDs,
  - production deploy still passes strict parity.

### Week 2 — Ingestion and Incident Resilience
- Implement Reddit fail-soft policy:
  - define minimum source quorum,
  - permit degraded run with explicit status metadata,
  - avoid hard failure when partial signal quality remains above threshold.
- Add health diagnostics fields and tests.
- Exit criteria:
  - new tests for all-fail, partial-fail, degraded-pass scenarios,
  - `/api/health` exposes coverage + degradation flags,
  - simulated 403/429 scenario no longer causes opaque failure mode.

### Week 3 — Alpha Quality Guardrails
- Add run-quality acceptance gates (docs count, source coverage, evidence-link floor).
- Add replay regression suite with representative historical `raw.json` snapshots.
- Add scoring-change safety tests (distribution drift tolerance bounds).
- Exit criteria:
  - poor-quality inputs are blocked or flagged,
  - regression suite green in CI,
  - report payload includes quality metadata.

### Week 4 — Quant Validation and Release Hardening
- Add rank-quality diagnostics endpoint/report (Spearman, per-timing/per-confidence quality deltas).
- Add weekly model-quality review cadence and checklist artifact in `memory/`.
- Complete promotion playbook and “go/no-go” checklist for production.
- Exit criteria:
  - score quality trends visible via API/report,
  - production release checklist required and documented,
  - first full cycle completed with staging->prod promotion discipline.

## Quality and Release Process Upgrades
1. **Staging-first promotion pipeline**
   - Split deployment workflow into staging deploy + strict parity + manual prod promotion.
   - Require exact `build_sha`/`worker_version` pinning on prod parity checks.
2. **Schema-safe deploy protocol**
   - Enforce additive migration compatibility.
   - Run migration checks before enabling new code paths that require new columns/indexes.
3. **Artifacted release evidence**
   - Persist parity JSON summaries and health snapshots as CI artifacts.
   - Attach to release notes for auditability.
4. **Deterministic regression gate**
   - Add replay suite over frozen `raw.json` datasets.
   - Block merges if rank order or key metrics drift outside tolerance.
5. **Mandatory release checklist**
   - Test, lint, deploy parity (prod + staging), run-quality check, rollback command prepared.

## Security and Reliability Hardening Checklist
- [ ] Separate staging and production data/storage bindings (D1/R2/KV).
- [ ] Keep admin run endpoint token rotation schedule documented and enforced.
- [ ] Add optional IP allowlist / signed timestamp for `/api/run` trigger path.
- [ ] Add structured incident payload in `/api/health` (last error category/time).
- [ ] Add degraded-mode indicator for partial-source runs.
- [ ] Add source-level timeout/retry budget metrics and expose counts in run summary.
- [ ] Add circuit-breaker policy when source failures exceed threshold across runs.
- [ ] Add replay-based canary gate before production promotion.
- [ ] Add backup/restore drill for D1 and retention checks for R2 artifacts.
- [ ] Add synthetic probe cron for `/api/version`, `/api/health`, `/api/accuracy` with alerting.

## Quant Research Integration Plan (how research becomes shipped code)
1. **Research Intake (Hypothesis Card)**
   - For each idea: thesis, expected edge, affected themes, lookback assumptions, failure modes.
2. **Design-to-Code Mapping**
   - Map hypothesis to one of: metrics, scoring weights, classification thresholds, theme definitions.
   - Define exact files and test surfaces before coding.
3. **Offline Validation First**
   - Replay against fixed historical datasets (`raw.json`) and compare against baseline metrics.
   - Require pre-registered acceptance criteria (e.g., hit-rate uplift, narrower unresolved rate).
4. **Staging Canary**
   - Deploy to staging only; run parity + quality diagnostics over at least one full schedule cycle.
5. **Promotion Gate**
   - Promote only if: quality gates pass, no reliability regressions, and confidence intervals are acceptable.
6. **Post-Deploy Evaluation**
   - Weekly review of hit rate, CI width, timing/cohort drift, and rank-vs-return correlation.
   - Revert or tune if performance degrades beyond agreed tolerance.

## Immediate Next 5 PRs (title, scope, files, validation commands)
### PR 1 — Isolate staging data plane from production
- **Scope:** Give staging its own D1 database, R2 bucket, KV namespace; update docs and parity expectations.
- **Files:**
  - `signals/wrangler.toml`
  - `signals/docs/DEPLOYMENT.md`
  - `signals/docs/ARCHITECTURE.md`
- **Validation commands:**
  - `cd /Users/scott/pxi/signals && npm test`
  - `cd /Users/scott/pxi/signals && npm run lint`
  - `cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals-staging --strict-version --check-migrations --migration-env staging --migration-db SIGNALS_DB`

### PR 2 — Reddit ingest quorum + degraded-mode resilience
- **Scope:** Prevent hard-fail on transient source failures; publish degraded status with diagnostics.
- **Files:**
  - `signals/src/reddit/reddit_client.ts`
  - `signals/src/scheduled.ts`
  - `signals/src/routes.ts`
  - `signals/tests/unit/reddit_client_resilience.test.ts`
  - `signals/tests/unit/routes_health.test.ts`
- **Validation commands:**
  - `cd /Users/scott/pxi/signals && npm test`
  - `cd /Users/scott/pxi/signals && npm run lint`

### PR 3 — Deploy promotion hardening (staging -> prod, pinned artifact checks)
- **Scope:** Make release path deterministic and auditable; require artifact pinning on prod checks.
- **Files:**
  - `.github/workflows/deploy-signals.yml`
  - `signals/scripts/deploy_parity_check.ts`
  - `signals/src/ops/deploy_parity.ts`
  - `signals/docs/DEPLOYMENT.md`
- **Validation commands:**
  - `cd /Users/scott/pxi/signals && npm test`
  - `cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals --strict-version --expect-worker-version <WORKER_VERSION> --expect-build-sha <BUILD_SHA> --check-migrations --migration-env production --migration-db SIGNALS_DB`

### PR 4 — Signal quality acceptance gates
- **Scope:** Add explicit minimum-quality requirements before publishing a run.
- **Files:**
  - `signals/src/scheduled.ts`
  - `signals/src/analysis/metrics.ts`
  - `signals/src/report/render_json.ts`
  - `signals/tests/unit/analysis/metrics.test.ts`
  - `signals/tests/unit/worker_schedule.test.ts`
- **Validation commands:**
  - `cd /Users/scott/pxi/signals && npm test`
  - `cd /Users/scott/pxi/signals && npm run lint`

### PR 5 — Evaluation diagnostics for ranking quality
- **Scope:** Expose rank-vs-return quality metrics (including Spearman) for ongoing alpha calibration.
- **Files:**
  - `signals/src/evaluation.ts`
  - `signals/src/db.ts`
  - `signals/src/routes.ts`
  - `signals/tests/unit/evaluation_target_price_date.test.ts`
  - `signals/tests/unit/routes_accuracy_intervals.test.ts`
- **Validation commands:**
  - `cd /Users/scott/pxi/signals && npm test`
  - `cd /Users/scott/pxi/signals && npm run lint`
  - `cd /Users/scott/pxi/signals && npm run report:evaluation`

## Open Decisions for Scott (binary yes/no with recommendation)
| Decision | Yes/No | Recommendation | Why |
|---|---|---|---|
| Split staging and production data/storage resources now? | Yes | **Yes** | Highest leverage safety move; removes staging blast radius to prod data. |
| Keep Wednesday catch-up trigger active? | Yes | **Yes** | Good protection against missed Mon/Tue runs and already implemented cleanly. |
| Allow degraded runs when Reddit partially fails but quorum is met? | Yes | **Yes** | Better continuity than hard-fail; can still mark quality explicitly. |
| Continue hard-failing when all sources fail? | Yes | **Yes** (with explicit degraded/no-signal metadata fallback path) | Prevent publishing fabricated/empty alpha while preserving observability. |
| Turn on comments ingestion by default (`ENABLE_COMMENTS=1`)? | No | **No** (for now) | Reliability and latency first; enable after source resilience and cost profiling. |
| Require prod parity to pin exact `build_sha` + `worker_version` every deploy? | Yes | **Yes** | Eliminates ambiguity about what artifact is live. |
| Add synthetic external watchdog alerting on `/api/health` + freshness? | Yes | **Yes** | Shortens MTTR and catches stale pipeline before user-facing impact. |
| Prioritize model/feature expansion before reliability hardening? | No | **No** | Current bottleneck is pipeline robustness and quality gating, not feature breadth. |
