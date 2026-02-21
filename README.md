# PXI /COMMAND

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Cloudflare Workers](https://img.shields.io/badge/Cloudflare-Workers-F38020?logo=cloudflare&logoColor=white)](https://workers.cloudflare.com/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![React](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=black)](https://react.dev/)

A suite of quantitative market tools:
- **PXI Index** - Real-time macro market strength (28 indicators → single 0-100 score)
- **Sector Signals** - Weekly sector rotation signals from Reddit discussion analysis

**Live:** [pxicommand.com](https://pxicommand.com) · [/signals](https://pxicommand.com/signals)

## What is PXI?

PXI (Positioning Index) synthesizes signals from liquidity, credit spreads, volatility, market breadth, macro economic data, global risk appetite, and crypto markets into one score:

| Score | Status | Meaning |
|-------|--------|---------|
| 80+ | MAX PAMP | Extremely bullish conditions |
| 65-79 | PAMPING | Bullish conditions |
| 45-64 | NEUTRAL | Mixed signals |
| 30-44 | SOFT | Bearish conditions |
| <30 | DUMPING | Extremely bearish conditions |

## Features

### Core Index
- **28 indicators** across 7 categories normalized to 0-100 scale
- **Real-time updates** via GitHub Actions cron (4x daily)
- **Historical backtest** with 1000+ observations since Dec 2022

### ML & Predictions (v1.2)
- **Ensemble Predictions** - Weighted combination of XGBoost (60%) and LSTM (40%) with confidence scoring
- **XGBoost Model** - Gradient boosted trees using 36 engineered features (momentum, dispersion, extremes)
- **LSTM Model** - Recurrent neural network using 20-day sequences of 12 features for temporal patterns
- **Similar Period Detection** - Find historically similar market regimes using vector embeddings
- **Prediction Tracking** - Log predictions and evaluate against actual outcomes
- **Historical Outlook** - Forward returns and win rates by PXI bucket with adaptive thresholds
- **Signal Layer** - Risk allocation signals based on PXI, momentum, and volatility

### Backtesting
- **PXI-Signal Strategy** - Dynamic risk allocation (Sharpe ~2.0)
- **Comparison** - vs 200DMA and buy-and-hold baselines
- **Walk-forward validation** - Out-of-sample testing

### MCP Server (AI Agent Integration)
- **Model Context Protocol** - Enables Claude and other LLM agents to query PXI
- **7 Tools** - get_pxi, get_predictions, get_similar_periods, get_signal, get_regime, get_market_context, get_history
- **Agent-Optimized** - Structured responses with suggested actions and risk assessments

### Sector Rotation Signals (`/signals`)
- **Weekly Analysis** - Runs every Monday (or Tuesday if Monday is a US market holiday)
- **Reddit Sentiment** - Analyzes 750+ posts from investing subreddits
- **20 Sector Themes** - Nuclear, Automation, Defense, Copper, Financials, and more
- **Classification System** - Signal type (Rotation/Momentum/Divergence/Reversion), confidence, timing
- **Evidence-Based** - Links to source discussions for each theme

## Categories & Weights

| Category | Weight | Key Indicators |
|----------|--------|----------------|
| Credit | 20% | HY spreads, IG spreads, yield curve |
| Liquidity | 15% | Fed balance sheet, TGA, reverse repo, net liquidity |
| Volatility | 15% | VIX, VIX term structure |
| Breadth | 15% | RSP/SPY ratio, advance-decline |
| Positioning | 15% | Put/call ratio, fear & greed |
| Macro | 10% | 10Y yield, dollar index, oil |
| Global | 10% | Copper/gold ratio, BTC, stablecoin flows |

## Architecture

```
                              PXI INDEX                                    SIGNALS
                    ┌─────────────────────────┐                 ┌─────────────────────────┐
                    │                         │                 │                         │
┌─────────────────┐ │  ┌──────────────────┐   │  ┌───────────┐  │  ┌──────────────────┐   │
│   Data Sources  │───▶│  GitHub Actions  │   │  │  Reddit   │────▶│  Cloudflare Cron │   │
│  FRED, Yahoo,   │ │  │   (Daily Cron)   │   │  │   API     │  │  │   (Weekly)       │   │
│  DeFiLlama...   │ │  └────────┬─────────┘   │  └───────────┘  │  └────────┬─────────┘   │
└─────────────────┘ │           │             │                 │           │             │
                    │           ▼             │                 │           ▼             │
                    │  ┌──────────────────┐   │                 │  ┌──────────────────┐   │
                    │  │  Cloudflare D1   │   │                 │  │  Cloudflare D1   │   │
                    │  │   (SQLite)       │   │                 │  │  + R2 Storage    │   │
                    │  └────────┬─────────┘   │                 │  └────────┬─────────┘   │
                    │           │             │                 │           │             │
                    └───────────┼─────────────┘                 └───────────┼─────────────┘
                                │                                           │
                                ▼                                           ▼
                    ┌──────────────────────────────────────────────────────────────────┐
                    │                     React Frontend (CF Pages)                     │
                    │                        pxicommand.com                             │
                    └──────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
pxi-command/
├── frontend/        # React app (pxicommand.com)
├── worker/          # Main PXI API worker
├── signals/         # Sector rotation signals worker (/signals)
├── ml/              # ML model training (XGBoost, LSTM)
├── mcp-server/      # MCP server for AI agents
├── src/             # Shared data collection scripts
└── ai/              # AI analysis prompts
```

## Tech Stack

- **Backend:** TypeScript, Node.js
- **Database:** Cloudflare D1 (SQLite at edge)
- **Storage:** Cloudflare R2 (report storage for signals)
- **Vector Store:** Cloudflare Vectorize (768-dim embeddings)
- **ML Models:** XGBoost + LSTM (trained locally, inference at edge via KV)
- **AI:** Cloudflare Workers AI (BGE embeddings, Llama analysis)
- **API:** Cloudflare Workers
- **Frontend:** React 19, Vite, Tailwind CSS
- **Hosting:** Cloudflare Pages
- **Scheduler:** GitHub Actions (PXI daily) + Cloudflare Cron (Signals weekly)
- **Agent Integration:** MCP Server (Model Context Protocol)

## API Endpoints

### Core Data
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/pxi` | GET | Current PXI score, categories, sparkline, regime, and freshness operator payload (`topOffenders`, refresh ETA) |
| `/api/history` | GET | Historical PXI scores |
| `/api/regime` | GET | Current market regime detection |
| `/api/signal` | GET | PXI-Signal layer with risk allocation |
| `/api/plan` | GET | Canonical decision object for homepage (policy state, uncertainty, consistency, trader playbook) |
| `/api/market/consistency` | GET | Latest decision consistency score/state/violations |
| `/api/ops/freshness-slo` | GET | Rolling 7d/30d freshness SLO attainment and recent critical stale incidents |

### Product Layer (Phase 1)
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/brief` | GET | Daily market brief coherent with `/api/plan` policy state (includes contract version + consistency) |
| `/api/opportunities` | GET | Ranked opportunities for `7d` or `30d` horizon with calibration + expectancy + unavailable reasons |
| `/api/diagnostics/calibration` | GET | Calibration diagnostics (Brier/ECE/log loss + quality band) for conviction and edge-quality snapshots |
| `/api/diagnostics/edge` | GET | Forward-chaining edge evidence (model vs lagged baseline uplift, CI, leakage sentinel, promotion gate) |
| `/api/alerts/feed` | GET | In-app alert timeline (`regime_change`, `threshold_cross`, `opportunity_spike`, `freshness_warning`) |
| `/api/alerts/subscribe/start` | POST | Start email digest subscription with verification token |
| `/api/alerts/subscribe/verify` | POST | Verify subscription token and activate email digest |
| `/api/alerts/unsubscribe` | POST | Unsubscribe via token |

### ML & Predictions
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/similar` | GET | Find similar historical periods (vector search with weighted scoring) |
| `/api/predict` | GET | Historical outlook by PXI bucket with adaptive thresholds |
| `/api/ml/predict` | GET | XGBoost model predictions for 7d/30d PXI changes |
| `/api/ml/lstm` | GET | LSTM neural network predictions using 20-day sequences |
| `/api/ml/ensemble` | GET | Weighted ensemble (60% XGBoost + 40% LSTM) with confidence |
| `/api/accuracy` | GET | Prediction accuracy metrics |
| `/api/analyze` | GET | AI-generated market analysis |

### Backtesting
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/backtest` | GET | PXI bucket forward returns analysis |
| `/api/backtest/signal` | GET | Signal strategy vs baselines |

### Admin (requires auth)
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/write` | POST | Write indicator data |
| `/api/recalculate` | POST | Recalculate PXI for date |
| `/api/migrate` | POST | Create missing database tables |
| `/api/evaluate` | POST | Evaluate past predictions |
| `/api/retrain` | POST | Retrain adaptive bucket thresholds |
| `/api/export/training-data` | GET | Export data for ML model training |
| `/api/market/refresh-products` | POST | Generate/store latest brief, opportunities, and market alerts |
| `/api/market/send-digest` | POST | Send daily digest to active subscribers |

### Signals (`/signals/*`)
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/signals/latest` | GET | Latest weekly report (HTML) |
| `/signals/run/{id}` | GET | Specific report by run ID |
| `/signals/api/runs` | GET | List all runs with metadata |
| `/signals/api/runs/{id}` | GET | Run details as JSON |
| `/signals/api/run` | POST | Trigger manual run (requires X-Admin-Token) |
| `/signals/og-image.png` | GET | Dynamic OG image for social sharing |

## Agent Docs

Agent-discovery and markdown docs are published as static frontend endpoints:

- `https://pxicommand.com/llms.txt`
- `https://pxicommand.com/agent.md`
- `https://pxicommand.com/signals/agent.md`

Canonical read endpoints referenced by `llms.txt`:

- `https://api.pxicommand.com/health`
- `https://api.pxicommand.com/api/pxi`
- `https://pxicommand.com/signals/api/runs`
- `https://pxicommand.com/signals/api/accuracy`
- `https://pxicommand.com/signals/api/predictions`

Write/admin endpoints remain auth-protected and are not intended for autonomous agent writes.

## Data Freshness SLA

`npm run cron:daily` enforces per-indicator freshness SLAs before any write/recalculate calls.

### SLA Classes

| Class | Max age (days) |
|---|---:|
| daily | 4 |
| realtime | 4 |
| weekly | 10 |
| monthly | 45 |
| source-lagged (`wti_crude`) | 7 |
| source-lagged (`dollar_index`) | 10 |

### Explicit Threshold Overrides

| Indicator | Max age (days) |
|---|---:|
| `cfnai` | 120 |
| `m2_yoy` | 120 |
| `fed_balance_sheet` | 14 |
| `treasury_general_account` | 14 |
| `reverse_repo` | 14 |
| `net_liquidity` | 14 |

### Critical SLA Gate

The daily refresh fails if any of these are stale or missing after fetch stage:

- `aaii_sentiment`
- `copper_gold_ratio`
- `vix`
- `spy_close`
- `dxy`
- `hyg`
- `lqd`
- `fear_greed`

Run summary output is written to `/tmp/pxi-sla-summary.json` and published to GitHub Actions step summary.

## Market Product Automation

- `Daily PXI Refresh` now runs ingestion + SLA gate + post-refresh product generation (`/api/market/refresh-products`).
- Product summary output is written to `/tmp/pxi-market-summary.json` and appended to the Actions job summary.
- A separate `Daily PXI Digest` workflow runs at `13:00 UTC` (8:00 AM ET standard) and calls `/api/market/send-digest`.
- Digest summary output is written to `/tmp/pxi-digest-summary.json`.

## PXI Audit Findings Remediation

Taylor’s PXI audit findings are being remediated with trust-first controls:

1. **Opportunity coherence gate**
- Directional opportunities are only published when contract checks are coherent.
- Bullish items must have non-negative expectancy and `p_correct >= 0.50` when available.
- Bearish items must have non-positive expectancy and `p_correct >= 0.50` when available.
- Neutral items are not published in phase 1 actionable feed.

2. **Degraded-mode suppression**
- `/api/opportunities` is hard-suppressed (`items: []`) when:
  - `critical_stale_count > 0`, or
  - market consistency state is `FAIL`.
- Response includes `suppressed_count` and `degraded_reason` (for example `suppressed_data_quality`, `coherence_gate_failed`, `quality_filtered`).

3. **Calibration diagnostics transparency**
- New endpoint: `GET /api/diagnostics/calibration`
- Includes Brier score, ECE, log loss, quality band, and insufficient-sample reasons.

4. **Freshness reliability governance**
- New endpoint: `GET /api/ops/freshness-slo`
- Reports rolling 7d and 30d SLO attainment using `market_refresh_runs`, including recent critical stale incidents.

5. **Edge-proof diagnostics + promotion gate**
- New endpoint: `GET /api/diagnostics/edge`
- Reports forward-chaining directional uplift vs lagged baseline, CI bounds, and leakage sentinel health per horizon.
- `/api/market/refresh-products` now fails closed when leakage sentinel checks fail, blocking publish/promotion.

6. **Decision-stack unification + cross-horizon coherence**
- `/api/plan` now includes additive `decision_stack` and `cross_horizon` blocks.
- `cross_horizon.state` (`ALIGNED|MIXED|CONFLICT|INSUFFICIENT`) downgrades `ACTIONABLE -> WATCH` on conflict/insufficient evidence.
- Additional invalidation guidance is attached when horizons conflict or one horizon is missing.

7. **Signals sanitation + stability**
- Adapter-side ticker sanitation (allowlist regex, jargon stopwords, dedupe/cap) before PXI opportunity blending.
- Signals source velocity cap tightened (`GROWTH_RATIO_CAP = 25`) with explicit `growth_ratio_capped` flag in metrics payload.

## Scheduler Ownership Runbook

Target ownership is GitHub Actions (`Daily PXI Refresh`) only. Keep Cloudflare cron enabled during hardening, then cut over after stability validation.

### 72-hour Shadow Procedure

1. Keep Cloudflare and GitHub schedulers active.
2. Require 12 consecutive successful scheduled GitHub runs (4/day over 72 hours) with zero critical SLA violations.
3. Disable Cloudflare cron triggers for:
   - `pxi-api-production`
   - `pxi-api`
4. Re-verify Cloudflare schedules are empty and GitHub continues refreshing data.

## Development

```bash
# Install dependencies
npm install

# Set up environment
cp .env.example .env
# Edit .env with your credentials

# Run daily data refresh
npm run cron:daily

# Start frontend dev server
cd frontend && npm run dev

# Deploy main worker
cd worker && npx wrangler deploy --env production

# Deploy signals worker
cd signals && npm install && npx wrangler deploy --env production
```

### Launch Readiness Smoke Checks

Use these checks after deployment or before releases:

```bash
# Frontend route checks
curl -I https://pxicommand.com/
curl -I https://pxicommand.com/spec
curl -I https://pxicommand.com/alerts
curl -I https://pxicommand.com/guide
curl -I https://pxicommand.com/signals
curl -I https://pxicommand.com/signals/latest

# API checks (canonical host)
curl -I https://api.pxicommand.com/health
curl -I https://api.pxicommand.com/api/pxi
curl -I https://api.pxicommand.com/api/alerts
curl -I https://api.pxicommand.com/api/signal
curl -I https://api.pxicommand.com/api/diagnostics/edge?horizon=all

# CORS preflight check
curl -X OPTIONS https://api.pxicommand.com/api/refresh \
  -H "Origin: https://pxicommand.com" \
  -H "Access-Control-Request-Method: POST" \
  -H "Access-Control-Request-Headers: Content-Type, Authorization, X-Admin-Token"

# Backfill validation check (expect 400/401 depending on auth mode)
curl -X POST https://api.pxicommand.com/api/backfill \
  -H "Content-Type: application/json" \
  -d '{"start":"invalid-date","limit":"bad"}'
```

Temporary rollback path remains available via `https://pxi-api.novoamorx1.workers.dev` during cutover.

### Signals Worker Route Check

Confirm the route is attached and active:

```bash
cd /Users/scott/pxi/signals
npx wrangler whoami
npx wrangler deployments list --name pxi-signals --env production
```

If `/signals` is unreachable (404), verify:

1. `signals/wrangler.toml` has `pxicommand.com/signals` and `pxicommand.com/signals/*` route entries.
2. The worker deploy succeeds after route changes.
3. The `pxi-signals` worker has active production bindings (`d1`, `r2`, `kv`) for `pxicommand.com`.

### Signals Development

```bash
cd signals

# Run tests (258 tests)
npm test

# Generate offline report (uses sample data)
npm run offline

# Deploy to Cloudflare
npx wrangler deploy --env production

# Set admin token for manual runs
npx wrangler secret put ADMIN_RUN_TOKEN
```

### ML Model Training

```bash
# Install Python dependencies
cd ml && pip install -r requirements.txt

# Train XGBoost models (requires WRITE_API_KEY for data export)
export WRITE_API_KEY=your_key
python train_xgboost.py

# Train LSTM models (requires PyTorch)
pip install torch
python train_lstm.py

# Upload models to Cloudflare KV
npx wrangler kv key put "pxi_model" --path=pxi_model_compact.json \
  --namespace-id=88901ff0216a484eb81b8004be0f5aea --remote
npx wrangler kv key put "pxi_lstm_model" --path=pxi_lstm_compact.json \
  --namespace-id=88901ff0216a484eb81b8004be0f5aea --remote
```

**XGBoost** (`train_xgboost.py`):
- Engineers 36 features (momentum, dispersion, extremes, rolling stats)
- Gradient boosted trees for 7d/30d PXI change prediction
- Exports to JSON (318KB compact)

**LSTM** (`train_lstm.py`):
- Uses 20-day sequences of 12 features (PXI, categories, VIX, dispersion)
- Single-layer LSTM with 32 hidden units
- Exports weights to JSON for edge inference (239KB compact)

### MCP Server Setup

```bash
# Build the MCP server
cd mcp-server && npm install && npm run build
```

**Claude Desktop** - add to `~/Library/Application Support/Claude/claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "pxi": {
      "command": "node",
      "args": ["/path/to/pxi/mcp-server/dist/index.js"]
    }
  }
}
```

**Available Tools:**
| Tool | Description |
|------|-------------|
| `get_pxi` | Current score, categories, trend |
| `get_predictions` | ML ensemble forecasts (7d/30d) |
| `get_similar_periods` | Vector similarity search |
| `get_signal` | Risk allocation signal |
| `get_regime` | Market regime analysis |
| `get_market_context` | Comprehensive agent context |
| `get_history` | Historical scores |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `FRED_API_KEY` | FRED API key ([get one free](https://fred.stlouisfed.org/docs/api/api_key.html)) |
| `WRITE_API_KEY` | API key for write operations |
| `WRITE_API_URL` | Worker API URL for cron |

### Cloudflare Secrets (set via wrangler)
```bash
npx wrangler secret put WRITE_API_KEY
npx wrangler secret put FRED_API_KEY
```

## Database Schema

### Core Tables
- `indicator_values` - Raw indicator data from sources
- `indicator_scores` - Normalized 0-100 scores
- `category_scores` - Category-level aggregations
- `pxi_scores` - Final composite scores

### ML Tables
- `prediction_log` - Predictions vs actual outcomes
- `model_params` - Tunable model parameters
- `period_accuracy` - Historical period prediction quality
- `market_embeddings` - Vector embeddings for similarity search

### Signal Tables
- `pxi_signal` - Risk allocation signals
- `alert_history` - Generated alerts
- `backtest_results` - Strategy performance

## Data Sources

- **FRED** - Federal Reserve economic data (liquidity, credit spreads, yields)
- **Yahoo Finance** - Market prices, ETFs, VIX, currencies
- **DeFiLlama** - Stablecoin market cap
- **Coinglass** - BTC funding rates
- **CNN** - Fear & Greed index

## License

MIT
