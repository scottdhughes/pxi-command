# Production refresh and score-history isolation

Cloudflare Cron on `pxi-api-production` is the sole automatic PXI refresh
scheduler. Its four native refresh slots and 23:30 UTC missed-close watchdog
are configured only in `worker/wrangler.toml`. `daily-refresh.yml` must remain
manual-only; do not add a GitHub `schedule` trigger or run two automatic
schedulers.

The D1 `refresh_mutation_locks` row named `indicator_score_mutation` is the
cross-control-plane source of truth. Cloudflare Cron, deploy smoke, the manual
GitHub fallback, production deployment, score-history reconstruction, and
manual market-product backfill must claim that lease before mutating state and
release it after the bounded operation. An unexpired lease makes a competing
operation fail closed.

`daily-refresh.yml`, production runs of `deploy-worker.yml`,
`score-history-reconstruction.yml`, and `market-backfill.yml` additionally
share the exact GitHub Actions concurrency group
`pxi-production-indicator-score-mutation` with
`cancel-in-progress: false`. This is a secondary queue for GitHub-owned paths;
it does not replace the D1 lease.

The manual workflow splits an authorized total range into sequential requests
of at most three calendar days and waits four seconds between requests. This
keeps each Worker invocation below the D1 Free query budget (two range reads,
then at most three calculations and eight immutable inserts per date) and
avoids crowding the admin-auth rate window.

This serializes the manual production indicator/score fallback and Worker code
rollovers with retrospective score reconstruction. Reconstruction reads the
current indicator store, so allowing those workflows to overlap could record a
row from a moving source snapshot. Non-cancelling behavior also prevents a
partially completed immutable reconstruction from being interrupted; a later
missing-only run may safely resume after a fail-stop.

The native scheduler records deterministic Cloudflare slots in
`refresh_scheduler_runs`. Attempt fencing prevents a stale invocation from
finishing a newer claim. The 23:30 UTC in-Worker watchdog writes one
`refresh_scheduler_incidents` row per missed decision date, and a later
successful close resolves only the matching date. The independently scheduled
GitHub `refresh-watchdog.yml` check at 23:50 UTC is the operator-visible alarm
for failures that also affect the Cloudflare Cron control plane.

The only authorized mutation path is the versioned
`POST /api/history/reconstruct-missing-v1`; the old `/api/backfill` path is
permanently closed. Before any reconstruction POST, the manual workflow must poll `/health` and
require both the triggering commit's 12-character build SHA and the
`isolated-missing-only-v1` history reconstruction capability. The capability is
available only when the Worker can see the isolated tables and all forward,
reverse, and immutability database guards.

The GitHub concurrency group covers only the four named GitHub workflow paths;
the D1 lease is what coordinates them with native Cloudflare Cron. The
application's New York current-date checks, deterministic scheduler slots,
attempt fences, and D1 bidirectional triggers remain the fail-closed controls
for other callers and races. Retrospective rows are continuity reconstructions
only and are never point-in-time research evidence.
