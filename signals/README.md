# PXI Signals

A Cloudflare Workers-powered pipeline for detecting early sector rotation signals from Reddit investment discussions. Analyzes social sentiment, mention velocity, and dispersion patterns to identify emerging investment themes.

## Features

- **Mention Velocity Tracking**: Detects accelerating discussion around investment themes
- **Sentiment Analysis**: VADER-based sentiment scoring with shift detection
- **Multi-Subreddit Dispersion**: Identifies themes spreading across communities
- **Automated Classification**: Categorizes signals by confidence, timing, and type
- **HTML/JSON Reports**: Generates both human-readable and machine-parseable outputs

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
| `REDDIT_CLIENT_ID` | Yes | - | Reddit API client ID |
| `REDDIT_CLIENT_SECRET` | Yes | - | Reddit API client secret |
| `REDDIT_USER_AGENT` | Yes | - | User agent for Reddit requests |
| `ADMIN_RUN_TOKEN` | Yes | - | Token for admin endpoints |
| `DEFAULT_LOOKBACK_DAYS` | No | 7 | Days for recent activity window |
| `DEFAULT_BASELINE_DAYS` | No | 30 | Days for baseline comparison |
| `DEFAULT_TOP_N` | No | 10 | Number of themes in report |
| `PUBLIC_BASE_PATH` | No | /signals | Base path for all routes |

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
