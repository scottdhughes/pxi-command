# API Reference

PXI Signals exposes a RESTful API for accessing signal reports and triggering pipeline runs.

## Base URL

All endpoints are prefixed with the configured `PUBLIC_BASE_PATH` (default: `/signals`).

```
https://pxicommand.com/signals
```

## Authentication

Most endpoints are public and read-only. The admin endpoint (`POST /api/run`) requires the `X-Admin-Token` header.

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
      "status": "success",
      "summary_json": null,
      "report_html_key": "reports/20250118-.../report.html",
      "results_json_key": "reports/20250118-.../results.json",
      "raw_json_key": null,
      "error_message": null
    }
  ]
}
```

**Parameters**
- Limit: Returns up to 50 most recent runs

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
  "error": "insufficient_data"
}
```

**Status Codes**
| Code | Description |
|------|-------------|
| `200` | Pipeline completed successfully |
| `401` | Invalid or missing admin token |
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
| `insufficient_data` | 500 | Not enough data to generate report |
| `insufficient_evidence` | 500 | Themes lack required evidence links |

---

## CORS

CORS headers are not currently configured. For cross-origin access, deploy a proxy or configure the worker to include appropriate headers.

---

## Versioning

The API does not currently use versioning. Breaking changes will be documented in release notes.
