# PXI /COMMAND

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Cloudflare Workers](https://img.shields.io/badge/Cloudflare-Workers-F38020?logo=cloudflare&logoColor=white)](https://workers.cloudflare.com/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![React](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=black)](https://react.dev/)

A real-time macro market strength index that aggregates 28 indicators across 7 categories to provide a single composite score (0-100) for market conditions, with ML-powered similar period detection and prediction tracking.

**Live:** [pxicommand.com](https://pxicommand.com)

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
- **Similar Period Detection** - Find historically similar market regimes using vector embeddings with engineered features (momentum, dispersion, volatility regime)
- **XGBoost Predictions** - Trained ML model predicting 7d/30d PXI changes using 36 features
- **LSTM Predictions** - Recurrent neural network using 20-day sequences of 12 features for temporal pattern recognition
- **Prediction Tracking** - Log predictions and evaluate against actual outcomes
- **Historical Outlook** - Forward returns and win rates by PXI bucket with adaptive thresholds
- **Signal Layer** - Risk allocation signals based on PXI, momentum, and volatility
- **Confidence Scoring** - Multi-factor confidence (direction agreement, consistency, sample size, weight quality)

### Backtesting
- **PXI-Signal Strategy** - Dynamic risk allocation (Sharpe ~2.0)
- **Comparison** - vs 200DMA and buy-and-hold baselines
- **Walk-forward validation** - Out-of-sample testing

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
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Data Sources  │────▶│  GitHub Actions  │────▶│  Cloudflare D1  │
│  FRED, Yahoo,   │     │   (Daily Cron)   │     │   (SQLite)      │
│  DeFiLlama...   │     └──────────────────┘     └────────┬────────┘
└─────────────────┘                                       │
                                                          ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  React Frontend │◀────│ Cloudflare Worker│◀────│  Workers AI     │
│  (CF Pages)     │     │    (Edge API)    │     │  + Vectorize    │
└─────────────────┘     └────────┬─────────┘     └─────────────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │  Cloudflare KV  │
                        │  (ML Models)    │
                        └─────────────────┘
```

## Tech Stack

- **Backend:** TypeScript, Node.js
- **Database:** Cloudflare D1 (SQLite at edge)
- **Vector Store:** Cloudflare Vectorize (768-dim embeddings)
- **ML Models:** XGBoost + LSTM (trained locally, inference at edge via KV)
- **AI:** Cloudflare Workers AI (BGE embeddings, Llama analysis)
- **API:** Cloudflare Workers
- **Frontend:** React 19, Vite, Tailwind CSS
- **Hosting:** Cloudflare Pages
- **Scheduler:** GitHub Actions (4x daily cron)

## API Endpoints

### Core Data
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/pxi` | GET | Current PXI score, categories, sparkline, regime |
| `/api/history` | GET | Historical PXI scores |
| `/api/regime` | GET | Current market regime detection |
| `/api/signal` | GET | PXI-Signal layer with risk allocation |

### ML & Predictions
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/similar` | GET | Find similar historical periods (vector search with weighted scoring) |
| `/api/predict` | GET | Historical outlook by PXI bucket with adaptive thresholds |
| `/api/ml/predict` | GET | XGBoost model predictions for 7d/30d PXI changes |
| `/api/ml/lstm` | GET | LSTM neural network predictions using 20-day sequences |
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

# Deploy worker
cd worker && npx wrangler deploy
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
