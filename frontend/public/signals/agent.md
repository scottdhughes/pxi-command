# PXI Signals Agent Spec

## Purpose
This document defines the stable read surface for PXI Signals report and prediction data.

## Canonical Routes
- Latest report HTML: `https://pxicommand.com/signals/latest`
- Runs list: `https://pxicommand.com/signals/api/runs`
- Accuracy: `https://pxicommand.com/signals/api/accuracy`
- Predictions: `https://pxicommand.com/signals/api/predictions`

## Report Flow
1. `GET /signals` responds with a redirect to `/signals/latest`.
2. `GET /signals/latest` returns the latest report HTML.
3. `GET /signals/api/runs` returns run metadata JSON.
4. `GET /signals/api/runs/{id}` returns a specific run detail JSON.

## Filtering Rules (`/signals/api/runs`)
- Allowed `status` values: `ok`, `error`
- Example valid filter: `https://pxicommand.com/signals/api/runs?status=ok`
- Invalid filter behavior: `https://pxicommand.com/signals/api/runs?status=foo` returns `400` JSON

## Accuracy and Predictions

### `GET /signals/api/accuracy`
- Returns aggregate hit-rate and summary metrics as JSON
- Expected status: `200`

### `GET /signals/api/predictions`
- Returns prediction rows as JSON
- Expected status: `200`

## Cadence
- Scheduler cadence: Monday and Tuesday at 15:00 UTC.
- Tuesday run is a holiday fallback path for US market closures.

## Safety Notes
- Signals APIs above are read-only.
- Manual run triggering requires admin token and should not be called autonomously.
