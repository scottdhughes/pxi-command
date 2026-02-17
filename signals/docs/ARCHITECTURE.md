# Architecture

This worker is mounted only on the `pxicommand.com/signals` and `pxicommand.com/signals/*` routes and does not touch the rest of the site.

## Components

- Worker: HTTP routes + scheduled pipeline.
- D1: run index (`runs` table) and minimal metadata.
- R2: report artifacts (`report.html`, `results.json`, optional `raw.json`).
- KV: pointers and cache (`latest_run_id`, `latest_run_at_utc`).

## Data Flow

1) Scheduled cron triggers pipeline.
2) Reddit data is fetched (or offline data is used).
3) Metrics + scoring produce ranked themes.
4) HTML + JSON artifacts are written to R2.
5) Run metadata is written to D1.
6) KV latest pointers are updated on success.

## Storage Keys

- `reports/<run_id>/report.html`
- `reports/<run_id>/results.json`
- `reports/<run_id>/raw.json` (optional)

## D1 Data Model

Only the `runs` table is required for MVP. Theme details live in R2 `results.json`.

## Offline Demo

`npm run offline` reads `data/sample_reddit.json` and produces the same artifacts to `out/offline/`.
