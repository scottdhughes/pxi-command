# Deployment

Do not run these commands unless you have Cloudflare credentials configured.

## Create D1 Database + Migrations

```
wrangler d1 create pxi_signals
wrangler d1 migrations apply SIGNALS_DB --remote --env production
```

For local validation before deploy:

```bash
cd /Users/scott/pxi/signals
npx wrangler d1 migrations apply SIGNALS_DB --local --env production
```

This includes:
- `0004_pipeline_lock.sql` (run-level overlap protection),
- `0005_signal_predictions_uniqueness.sql` (historical duplicate cleanup + DB-level uniqueness on `(signal_date, theme_id)`),
- `0006_prediction_eval_price_date.sql` (evaluation audit fields: `exit_price_date`, `evaluation_note`).

After applying migrations, verify prediction uniqueness:

```bash
cd /Users/scott/pxi/signals
npx wrangler d1 execute SIGNALS_DB --remote --env production --command "SELECT signal_date, theme_id, COUNT(*) AS c FROM signal_predictions GROUP BY signal_date, theme_id HAVING c > 1 LIMIT 5;"
npx wrangler d1 execute SIGNALS_DB --remote --env production --command "SELECT name, sql FROM sqlite_master WHERE type='index' AND name='idx_signal_predictions_signal_theme_unique';"
npx wrangler d1 execute SIGNALS_DB --remote --env production --command "PRAGMA table_info(signal_predictions);"
```

Expected results:
- duplicate query returns zero rows,
- unique index query returns one row,
- table info includes `exit_price_date` and `evaluation_note` columns.

## Create R2 Bucket

```
wrangler r2 bucket create pxi-signals
```

## Create KV Namespace

```
wrangler kv:namespace create SIGNALS_KV
```

## Set Secrets

```
wrangler secret put REDDIT_CLIENT_ID
wrangler secret put REDDIT_CLIENT_SECRET
wrangler secret put REDDIT_USER_AGENT
wrangler secret put ADMIN_RUN_TOKEN
```

## Deploy Worker

Use deploy metadata vars so `/api/version` can prove release provenance:

```bash
BUILD_SHA=$(git rev-parse --short=12 HEAD)
BUILD_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
WORKER_VERSION="signals-${BUILD_SHA}-${BUILD_TIMESTAMP}"

wrangler deploy --env staging --var BUILD_SHA:${BUILD_SHA} --var BUILD_TIMESTAMP:${BUILD_TIMESTAMP} --var WORKER_VERSION:${WORKER_VERSION}
wrangler deploy --env production --var BUILD_SHA:${BUILD_SHA} --var BUILD_TIMESTAMP:${BUILD_TIMESTAMP} --var WORKER_VERSION:${WORKER_VERSION}
```

## Verify Routing

- Confirm the worker route pattern is `pxicommand.com/signals` and `pxicommand.com/signals/*`.
- Ensure the main site remains served by Cloudflare Pages for all other paths.

## Checklist

- D1 database is bound as `SIGNALS_DB`.
- R2 bucket is bound as `SIGNALS_BUCKET`.
- KV namespace is bound as `SIGNALS_KV`.
- Secrets are set.
- `/signals/latest` serves a report.
- `/signals/api/version` returns build metadata (`build_sha`, `build_timestamp`, `worker_version`).
- `/signals/api/health` returns freshness JSON with `status` and `is_stale`.

## Post-Deploy Parity Gate

Run this after every staging/production deploy:

```bash
cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals
```

For production release validation, enforce strict non-placeholder metadata:

```bash
cd /Users/scott/pxi/signals && npm run smoke:deploy -- https://pxicommand.com/signals --strict-version
```

For release-candidate pinning, assert the exact artifact you intended to ship:

```bash
cd /Users/scott/pxi/signals
BUILD_SHA=$(git rev-parse --short=12 HEAD)
BUILD_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
WORKER_VERSION="signals-${BUILD_SHA}-${BUILD_TIMESTAMP}"

npm run smoke:deploy -- https://pxicommand.com/signals --strict-version --expect-worker-version "${WORKER_VERSION}" --expect-build-sha "${BUILD_SHA}"
```

For schema-aware parity (recommended for production), include migration-marker checks:

```bash
cd /Users/scott/pxi/signals
npm run smoke:deploy -- \
  https://pxicommand.com/signals \
  --strict-version \
  --expect-worker-version "${WORKER_VERSION}" \
  --expect-build-sha "${BUILD_SHA}" \
  --check-migrations \
  --migration-env production \
  --migration-db SIGNALS_DB
```

This check fails fast when:
- `/api/version` is missing/malformed deploy metadata,
- strict mode detects placeholder provenance metadata (`signals-dev`, `local-dev`, all-zero `build_sha`, epoch-like timestamps),
- expected-artifact pinning detects `worker_version` or `build_sha` mismatches,
- required migration markers are missing (`pipeline_locks` table, prediction uniqueness index, evaluation audit columns),
- `/api/health` is missing or malformed,
- `/api/accuracy` does not expose required CI/completeness fields,
- `/api/predictions` contains duplicate logical keys (`signal_date`, `theme_id`).
