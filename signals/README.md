# PXI Signals

[![Live](https://img.shields.io/badge/Live-pxicommand.com/signals-00a3ff)](https://pxicommand.com/signals)
[![Tests](https://img.shields.io/badge/Tests-258%20passing-10b981)](tests/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0-3178C6)](https://www.typescriptlang.org/)

Weekly sector rotation signals from Reddit investment discussions. Analyzes social sentiment, mention velocity, and dispersion patterns to identify emerging investment themes.

**Live:** [pxicommand.com/signals](https://pxicommand.com/signals)

## Features

- **Weekly Analysis**: Runs every Monday (or Tuesday if Monday is a US market holiday)
- **Mention Velocity Tracking**: Detects accelerating discussion around investment themes
- **Sentiment Analysis**: VADER-based sentiment scoring with shift detection
- **Multi-Subreddit Dispersion**: Identifies themes spreading across communities
- **Automated Classification**: Categorizes signals by confidence, timing, and type
- **20 Sector Themes**: Nuclear, Automation, Defense, Copper, Financials, and more
- **SEO Optimized**: Open Graph, Twitter Cards, JSON-LD structured data
- **Dynamic OG Image**: Auto-generated social preview showing top signal

## Quick Start

```bash
# Install dependencies
npm install

# Generate a demo report from sample data
npm run offline

# Outputs:
#   out/offline/report.html  - Visual report
#   out/offline/results.json - Structured data
```

## Development

```bash
# Start local development server
npm run dev

# Run tests (258 test cases)
npm test

# Lint code
npm run lint

# Format code
npm run format
```

## API Endpoints

| Route | Method | Description |
|-------|--------|-------------|
| `/signals/latest` | GET | Latest report (HTML) |
| `/signals/latest.json` | GET | Latest report (JSON) |
| `/signals/runs` | GET | List of all pipeline runs |
| `/signals/runs/:id` | GET | Specific run report (HTML) |
| `/signals/runs/:id.json` | GET | Specific run report (JSON) |
| `/signals/api/run` | POST | Trigger pipeline run (requires `X-Admin-Token`) |

See [docs/API.md](docs/API.md) for detailed API documentation.

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ADMIN_RUN_TOKEN` | Yes | - | Token for manual run endpoint |
| `REDDIT_CLIENT_ID` | No | - | Reddit API client ID (falls back to public endpoints) |
| `REDDIT_CLIENT_SECRET` | No | - | Reddit API client secret |
| `REDDIT_USER_AGENT` | No | - | User agent for Reddit requests |
| `DEFAULT_LOOKBACK_DAYS` | No | 7 | Days for recent activity window |
| `DEFAULT_BASELINE_DAYS` | No | 30 | Days for baseline comparison |
| `DEFAULT_TOP_N` | No | 10 | Number of themes in report |
| `PUBLIC_BASE_PATH` | No | /signals | Base path for all routes |

> **Note:** Reddit API credentials are optional. Without them, the system falls back to public Reddit endpoints (`old.reddit.com/.json`) which don't require authentication.

### Cloudflare Bindings

| Binding | Type | Description |
|---------|------|-------------|
| `SIGNALS_DB` | D1 | Run metadata storage |
| `SIGNALS_BUCKET` | R2 | Report artifact storage |
| `SIGNALS_KV` | KV | Latest run pointers and cache |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Cloudflare Worker                      │
├─────────────────────────────────────────────────────────┤
│  HTTP Routes          │  Scheduled Trigger               │
│  - /signals/*         │  - Cron-based pipeline           │
└───────────┬───────────┴───────────────┬─────────────────┘
            │                           │
            ▼                           ▼
┌───────────────────┐       ┌───────────────────────────┐
│   Reddit API      │       │   Analysis Pipeline        │
│   - OAuth2        │──────▶│   - Metrics computation    │
│   - Rate limited  │       │   - Z-score normalization  │
└───────────────────┘       │   - Theme classification   │
                            └─────────────┬─────────────┘
                                          │
            ┌─────────────────────────────┼─────────────────┐
            │                             │                 │
            ▼                             ▼                 ▼
    ┌───────────────┐           ┌───────────────┐   ┌──────────────┐
    │   D1 (SQLite) │           │   R2 Storage  │   │   KV Store   │
    │   runs table  │           │   HTML/JSON   │   │   pointers   │
    └───────────────┘           └───────────────┘   └──────────────┘
```

## Analysis Pipeline

1. **Data Collection**: Fetch posts/comments from target subreddits
2. **Theme Matching**: Match content against theme keywords
3. **Metrics Computation**: Calculate velocity, sentiment, confirmation scores
4. **Z-Score Normalization**: Normalize across themes for fair comparison
5. **Composite Scoring**: Weight and combine component scores
6. **Classification**: Assign signal type, confidence, timing, and star rating
7. **Report Generation**: Produce HTML and JSON artifacts

See [docs/SCORING.md](docs/SCORING.md) for algorithm details.

## Project Structure

```
src/
├── analysis/           # Core analysis logic
│   ├── constants.ts    # Weights, thresholds, limits
│   ├── metrics.ts      # Metrics computation
│   ├── scoring.ts      # Composite scoring
│   ├── classify.ts     # Theme classification
│   ├── sentiment.ts    # VADER sentiment
│   ├── normalize.ts    # Z-score normalization
│   ├── tickers.ts      # Ticker extraction
│   ├── takeaways.ts    # Report summaries
│   └── themes.ts       # Theme definitions
├── reddit/             # Reddit API integration
│   ├── reddit_client.ts
│   ├── schemas.ts      # Zod validation
│   └── types.ts
├── report/             # Report generation
│   ├── render_html.ts
│   ├── render_json.ts
│   └── template.ts
├── utils/              # Utilities
│   ├── time.ts
│   └── logger.ts
├── config.ts           # Configuration
├── errors.ts           # Error types
├── routes.ts           # HTTP handlers
├── scheduled.ts        # Pipeline logic
└── worker.ts           # Entry point

tests/
├── unit/               # Unit tests
├── integration/        # Integration tests
└── fixtures/           # Test data
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) - System design and data flow
- [Scoring](docs/SCORING.md) - Algorithm and methodology
- [Deployment](docs/DEPLOYMENT.md) - Production setup guide
- [API Reference](docs/API.md) - HTTP endpoint details

## Security

- **XSS Prevention**: All user-derived content is HTML-escaped
- **Constant-Time Comparison**: Admin token verification uses timing-safe comparison
- **Rate Limiting**: Admin endpoint is rate-limited (5 requests/minute)
- **Input Validation**: Reddit API responses validated with Zod schemas
- **Run ID Validation**: Strict format enforcement (YYYYMMDD-ULID)

## License

MIT
