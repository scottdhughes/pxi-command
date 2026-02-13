# PXI /COMMAND Agent Spec

## Purpose
This document defines the stable, agent-facing read surface for PXI /COMMAND.
Use this for market-state retrieval and signals context.

## Canonical Hosts
- App: `https://pxicommand.com`
- API: `https://api.pxicommand.com`
- Signals API: `https://pxicommand.com/signals/api`

## Core Endpoint Contracts

### `GET /api/pxi`
- Full URL: `https://api.pxicommand.com/api/pxi`
- Access: public, read-only
- Returns: current PXI score payload as JSON
- Expected status: `200`
- Content type: `application/json`

### `GET /api/alerts?limit=...`
- Full URL: `https://api.pxicommand.com/api/alerts?limit=5`
- Access: public, read-only
- Query parameter:
  - `limit` integer, practical small values recommended
- Returns: alert history JSON
- Expected status: `200`
- Content type: `application/json`

## Response Semantics
- `2xx`: successful read
- `4xx`: invalid request or unauthorized for restricted endpoints
- `5xx`: service-side error; retry with backoff
- JSON fields may evolve with additive changes; avoid brittle schema coupling

## Freshness Cadence
- PXI data refresh is scheduled by GitHub Actions multiple times per day.
- Signals runs are scheduled weekly (Monday and Tuesday at 15:00 UTC).

## Error Handling Guidance
- Treat non-`2xx` responses as non-authoritative.
- Retry idempotent reads with exponential backoff.
- Respect endpoint-specific validation errors instead of retrying malformed requests.

## Safe Usage Guidance
- `/api/pxi` and `/api/alerts` are read-only agent endpoints.
- Admin/write routes require auth and are not safe for autonomous execution.
- Never perform write/admin operations unless explicitly instructed and authorized.
