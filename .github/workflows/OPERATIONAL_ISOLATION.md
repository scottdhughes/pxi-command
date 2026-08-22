# Production score-history isolation

`daily-refresh.yml`, production runs of `deploy-worker.yml`, and
`score-history-reconstruction.yml` must share the exact
GitHub Actions concurrency group `pxi-production-indicator-score-mutation` with
`cancel-in-progress: false`.

The manual workflow splits an authorized total range into sequential requests
of at most three calendar days and waits four seconds between requests. This
keeps each Worker invocation below the D1 Free query budget (two range reads,
then at most three calculations and eight immutable inserts per date) and
avoids crowding the admin-auth rate window.

This serializes the normal production indicator/score mutation job and Worker
code rollovers with the manual retrospective score reconstruction job.
Reconstruction reads the current indicator store, so allowing those workflows
to overlap could record a row from a moving source snapshot. Non-cancelling
behavior also prevents a
partially completed immutable reconstruction from being interrupted by the
scheduled refresh; a later missing-only run may safely resume after a
fail-stop.

The only authorized mutation path is the versioned
`POST /api/history/reconstruct-missing-v1`; the old `/api/backfill` path is
permanently closed. Before any reconstruction POST, the manual workflow must poll `/health` and
require both the triggering commit's 12-character build SHA and the
`isolated-missing-only-v1` history reconstruction capability. The capability is
available only when the Worker can see the isolated tables and all forward,
reverse, and immutability database guards.

This concurrency group covers only the three named GitHub workflow paths above. The
application's New York current-date checks and D1 bidirectional triggers remain
the fail-closed controls for other callers and races. Retrospective rows are
continuity reconstructions only and are never point-in-time research evidence.
