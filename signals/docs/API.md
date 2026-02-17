# API Reference

PXI Signals exposes a RESTful API for accessing signal reports and triggering pipeline runs.

## Base URL

All endpoints are prefixed with the configured `PUBLIC_BASE_PATH` (default: `/signals`).

```
https://pxicommand.com/signals
```

## Authentication

Most endpoints are public and read-only. The admin endpoint (`POST /api/run`) requires the `X-Admin-Token` header.

## Cache Policy (Operational/Admin Endpoints)

To prevent stale intermediary reads and cached admin responses, the following endpoints return:

- `Cache-Control: no-store`

Applied to:
- `GET /api/version`
- `GET /api/health`
- `GET /api/accuracy`
- `GET /api/predictions`
- `POST /api/run` (including `409` lock and `500` failure responses)

---

## Public Endpoints

### GET /latest

Returns the latest signal report as an HTML page.

**Response**
- `200 OK` - HTML report
- Falls back to stub report if no runs exist

**Example**
```bash
curl https://pxicommand.com/signals/latest
```

---

### GET /api/runs

Returns a list of pipeline runs with metadata.

**Response**
```json
{
  "runs": [
    {
      "id": "20250118-01HQXYZABC123DEF456GHI789",
      "created_at_utc": "2025-01-18T15:30:00Z",
      "lookback_days": 7,
      "baseline_days": 30,
      "status": "ok",
      "summary_json": null,
      "report_html_key": "reports/20250118-.../report.html",
      "results_json_key": "reports/20250118-.../results.json",
      "raw_json_key": null,
      "error_message": null
    }
  ]
}
```

Optional query parameters:

- `status=ok` to return only successful runs
- `status=error` to return only failed runs
- Without status, returns mixed runs (most recent first)

**Example**
```bash
curl https://pxicommand.com/signals/api/runs
```

---

### GET /run/:id

Returns a specific run's HTML report.

**Parameters**
| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | string | Run ID in format `YYYYMMDD-ULID` |

**Response**
- `200 OK` - HTML report
- `400 Bad Request` - Invalid run ID format
- `404 Not Found` - Run does not exist

**Example**
```bash
curl https://pxicommand.com/signals/run/20250118-01HQXYZABC123DEF456GHI789
```

---

### GET /api/runs/:id

Returns a specific run's structured JSON data.

**Parameters**
| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | string | Run ID in format `YYYYMMDD-ULID` |

**Response**
```json
{
  "run_id": "20250118-01HQXYZABC123DEF456GHI789",
  "generated_at_utc": "2025-01-18T15:30:00Z",
  "pipeline_config": {
    "lookback_days": 7,
    "baseline_days": 30,
    "top_n": 10,
    "price_provider": "none",
    "enable_comments": false,
    "enable_rss": false
  },
  "doc_count": 1523,
  "themes": [
    {
      "rank": 1,
      "theme_id": "nuclear",
      "theme_name": "Nuclear/Uranium",
      "score": 2.34,
      "classification": {
        "signal_type": "Rotation",
        "confidence": "High",
        "timing": "Now",
        "stars": 5
      },
      "metrics": { /* ThemeMetrics */ },
      "scoring": { /* ThemeScore */ },
      "evidence_links": [
        "https://reddit.com/r/stocks/..."
      ],
      "key_tickers": ["CCJ", "URA", "UUUU"]
    }
  ]
}
```

**Error Response**
```json
{
  "error": "Not found"
}
```

**Example**
```bash
curl https://pxicommand.com/signals/api/runs/20250118-01HQXYZABC123DEF456GHI789
```

---

### GET /api/version

Returns immutable deploy metadata for release provenance and parity checks.

**Response**
```json
{
  "generated_at": "2026-02-17T06:00:00.000Z",
  "api_contract_version": "2026-02-17",
  "worker_version": "signals-prod-20260217-0600",
  "build_sha": "a1b2c3d4e5f6",
  "build_timestamp": "2026-02-17T05:58:41.000Z"
}
```

Notes:
- `generated_at` is response generation time.
- `build_timestamp` is normalized to ISO format.
- If deploy metadata is missing/malformed, fields may be `null` and parity checks should fail.

**Example**
```bash
curl https://pxicommand.com/signals/api/version
```

---

### GET /api/health

Returns pipeline freshness health based on the most recent successful run.

**Response**
```json
{
  "generated_at": "2026-02-17T06:00:00.000Z",
  "latest_success_at": "2026-02-17T05:00:00.000Z",
  "hours_since_success": 1,
  "threshold_days": 8,
  "is_stale": false,
  "status": "ok"
}
```

`status` values:
- `ok`: latest successful run is within threshold
- `stale`: latest successful run exists but is older than threshold
- `no_history`: no successful run exists yet

**Example**
```bash
curl https://pxicommand.com/signals/api/health
```

---

### GET /api/accuracy

Returns evaluated prediction performance with uncertainty bands.

**Response**
```json
{
  "generated_at": "2026-02-17T06:10:00.000Z",
  "as_of": "2026-02-17T06:10:00.000Z",
  "sample_size": 40,
  "total_predictions": 40,
  "minimum_recommended_sample_size": 30,
  "evaluated_count": 43,
  "resolved_count": 40,
  "resolved_predictions": 40,
  "unresolved_count": 3,
  "unresolved_predictions": 3,
  "unresolved_rate": "7.0%",
  "governance_status": "PASS",
  "overall": {
    "hit_rate": "60.0%",
    "hit_rate_ci_low": "44.6%",
    "hit_rate_ci_high": "73.7%",
    "count": 40,
    "sample_size_warning": false,
    "avg_return": "+1.2%"
  },
  "by_timing": {
    "Now": {
      "hit_rate": "64.5%",
      "hit_rate_ci_low": "46.9%",
      "hit_rate_ci_high": "78.9%",
      "count": 31,
      "sample_size_warning": false,
      "avg_return": "+1.5%"
    }
  },
  "by_confidence": {
    "High": {
      "hit_rate": "65.7%",
      "hit_rate_ci_low": "48.1%",
      "hit_rate_ci_high": "80.0%",
      "count": 35,
      "sample_size_warning": false,
      "avg_return": "+1.8%"
    }
  }
}
```

Notes:
- Confidence intervals are 95% Wilson score intervals on hit-rate proportions.
- `as_of` aliases `generated_at` for cross-surface compatibility.
- `total_predictions`, `resolved_predictions`, and `unresolved_predictions` are aliases of `sample_size`, `resolved_count`, and `unresolved_count`.
- `governance_status` is one of `PASS|WARN|FAIL|INSUFFICIENT`.
- `sample_size_warning = true` when subgroup `count` is below `minimum_recommended_sample_size`.
- `evaluated_count` includes all evaluated rows with `proxy_etf` (resolved + unresolved).
- `resolved_count` is the denominator used for hit-rate/return aggregates (`hit` is non-null).
- `unresolved_count` and `unresolved_rate` quantify evaluation attrition from unresolved exits (`hit = null`).
- For zero-sample cases, interval fields return `"0.0%"` deterministically.

**Example**
```bash
curl https://pxicommand.com/signals/api/accuracy
```

---

### GET /api/predictions

Returns recent prediction rows with optional filtering.

Optional query parameters:
- `limit` (integer, clamped to `[1, 100]`, default `50`)
- `evaluated=true|false`

Malformed query values return `400`.

**Response (example)**
```json
{
  "predictions": [
    {
      "signal_date": "2026-02-17",
      "target_date": "2026-02-26",
      "theme_id": "nuclear_uranium",
      "theme_name": "Nuclear Uranium",
      "rank": 1,
      "score": 9.12,
      "signal_type": "Rotation",
      "confidence": "High",
      "timing": "Now",
      "stars": 5,
      "proxy_etf": "URNM",
      "entry_price": 97.44,
      "exit_price": 101.2,
      "exit_price_date": "2026-02-26",
      "return_pct": 3.86,
      "hit": true,
      "status": "evaluated",
      "evaluated_at": "2026-03-01T14:00:00.000Z",
      "evaluation_note": null
    }
  ]
}
```

Evaluation alignment notes:
- `exit_price_date` is the actual market date used for exit pricing.
- `evaluation_note` is populated when no valid historical close could be resolved (e.g., data gap), in which case `hit` remains `null`.

**Example**
```bash
curl "https://pxicommand.com/signals/api/predictions?limit=25&evaluated=true"
```

---

## Admin Endpoints

### POST /api/run

Triggers a new pipeline run. Requires authentication and is rate-limited.

**Headers**
| Header | Required | Description |
|--------|----------|-------------|
| `X-Admin-Token` | Yes | Admin authentication token |

**Rate Limiting**
- 5 requests per 60-second window per IP address
- Uses `CF-Connecting-IP` header for client identification

**Response (Success)**
```json
{
  "ok": true,
  "run_id": "20250118-01HQXYZABC123DEF456GHI789"
}
```

**Response (Error)**
```json
{
  "ok": false,
  "error": "run_failed"
}
```

Notes:
- `500` responses intentionally return the sanitized code `run_failed` to avoid leaking internal pipeline details.
- Detailed failure context is still persisted in the internal `runs.error_message` field for operators.

If another run is already active, the endpoint returns:

```json
{
  "ok": false,
  "error": "pipeline_locked"
}
```

**Status Codes**
| Code | Description |
|------|-------------|
| `200` | Pipeline completed successfully |
| `401` | Invalid or missing admin token |
| `409` | Pipeline already running (`pipeline_locked`) |
| `429` | Rate limit exceeded |
| `500` | Pipeline execution failed |

**Example**
```bash
curl -X POST \
  -H "X-Admin-Token: your-secret-token" \
  https://pxicommand.com/signals/api/run
```

---

## Data Types

### ThemeMetrics

Computed metrics for an investment theme.

```typescript
interface ThemeMetrics {
  theme_id: string
  theme_name: string
  mentions_L: number           // Mentions in lookback window
  mentions_B: number           // Mentions in baseline window
  unique_subreddits: number    // Subreddit dispersion
  growth_ratio: number         // Velocity ratio
  slope: number                // Linear regression slope
  sentiment_L: number          // Avg sentiment in lookback
  sentiment_B: number          // Avg sentiment in baseline
  sentiment_shift: number      // Sentiment change
  concentration: number        // Top-3 post concentration
  momentum_score: number | null
  divergence_score: number | null
  confirmation_score: number
  price_available: boolean
  price_metrics: object | null
  key_tickers: string[]
  evidence_links: string[]
}
```

### ThemeScore

Normalized scoring components.

```typescript
interface ThemeScore {
  theme_id: string
  theme_name: string
  score: number              // Composite score
  components: {
    velocity: number         // Z-score
    sentiment_shift: number  // Z-score
    confirmation: number     // Z-score
    price: number            // Z-score
  }
  raw: {
    velocity: number         // Raw value
    sentiment_shift: number  // Raw value
    confirmation: number     // Raw value
    price: number            // Raw value
  }
}
```

### ThemeClassification

Signal classification output.

```typescript
interface ThemeClassification {
  signal_type: "Rotation" | "Momentum" | "Mean Reversion" | "Contrarian"
  confidence: "High" | "Medium" | "Low"
  timing: "Now" | "Now (volatile)" | "Building" | "Early" | "Ongoing"
  stars: 1 | 2 | 3 | 4 | 5
}
```

---

## Run ID Format

Run IDs follow the format `YYYYMMDD-ULID`:

- `YYYYMMDD`: Date of the run (e.g., `20250118`)
- `ULID`: 26-character Universally Unique Lexicographically Sortable Identifier

Example: `20250118-01HQXYZABC123DEF456GHI789`

This format ensures:
- Chronological sorting
- Uniqueness across distributed systems
- Human-readable date prefix

---

## Error Handling

All error responses include a JSON body with an `error` field:

```json
{
  "error": "Description of the error"
}
```

### Common Errors

| Error | Status | Description |
|-------|--------|-------------|
| `Invalid run ID format` | 400 | Run ID doesn't match expected pattern |
| `Unauthorized` | 401 | Missing or invalid admin token |
| `Not found` | 404 | Requested resource doesn't exist |
| `Rate limit exceeded` | 429 | Too many requests |
| `run_failed` | 500 | Pipeline execution failed (sanitized public error code) |
| `pipeline_locked` | 409 | Another run currently holds the pipeline lock |

---

## CORS

CORS headers are not currently configured. For cross-origin access, deploy a proxy or configure the worker to include appropriate headers.

---

## Versioning

The API does not currently use URL path versioning. Deploy provenance and contract compatibility are exposed via `GET /api/version` (`api_contract_version`, `worker_version`, `build_sha`, `build_timestamp`).
