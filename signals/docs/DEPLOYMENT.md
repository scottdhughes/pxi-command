# Deployment

Do not run these commands unless you have Cloudflare credentials configured.

## Create D1 Database + Migrations

```
wrangler d1 create pxi_signals
wrangler d1 migrations apply pxi_signals --remote
```

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

```
wrangler deploy
```

## Verify Routing

- Confirm the worker route pattern is `pxicommand.com/signals*`.
- Ensure the main site remains served by Cloudflare Pages for all other paths.

## Checklist

- D1 database is bound as `SIGNALS_DB`.
- R2 bucket is bound as `SIGNALS_BUCKET`.
- KV namespace is bound as `SIGNALS_KV`.
- Secrets are set.
- `/signals/latest` serves a report.
