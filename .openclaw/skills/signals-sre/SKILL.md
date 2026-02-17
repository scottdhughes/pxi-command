---
name: signals-sre
description: Operate and recover the PXI Signals pipeline using a fast diagnosis and remediation runbook.
metadata:
  {
    "openclaw":
      {
        "emoji": "S",
      },
  }
---

# Signals SRE Runbook

Use this skill for stale `/signals` pages, failed runs, or scheduler drift.

## Fast Health Checks

1. Check latest run metadata:
   - `curl -sS https://pxicommand.com/signals/api/runs`
2. Check rendered page freshness:
   - `curl -sS https://pxicommand.com/signals/latest`
3. Verify newest run status and timestamp.

## Common Failure Modes

- Missing recent run despite cron day.
- Latest run status `error` (for example Reddit fetch failures).
- Endpoint returns 5xx due to bad deployment target/env bindings.

## Recovery Sequence

1. Trigger manual run (token-gated endpoint):
   - `POST /signals/api/run` with `X-Admin-Token`.
2. Verify run persisted in D1 and page updated.
3. Validate cron schedules are present in Cloudflare.
4. If deployment issue:
   - redeploy explicitly to production env with proper bindings.

## Post-Incident Checklist

- Confirm `/signals/api/runs` returns HTTP 200.
- Confirm latest run is `ok`.
- Confirm page metadata reflects current date.
- Document root cause and prevention changes.
