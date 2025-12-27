# PXI /COMMAND

A real-time macro market strength index that aggregates 28 indicators across 7 categories to provide a single composite score (0-100) for market conditions.

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

## Categories & Weights

| Category | Weight | Key Indicators |
|----------|--------|----------------|
| Liquidity | 22% | Fed balance sheet, TGA, reverse repo, M2 |
| Credit | 18% | HY spreads, IG spreads, yield curve |
| Volatility | 18% | VIX, VIX term structure, put/call ratio |
| Breadth | 12% | RSP/SPY ratio, sector breadth, small/mid cap strength |
| Macro | 10% | ISM PMI, jobless claims, CFNAI |
| Global | 10% | DXY, copper/gold ratio, EM spreads, AUD/JPY |
| Crypto | 10% | BTC vs 200 DMA, ETF flows, stablecoin mcap, funding rates |

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Data Sources  │────▶│  Node.js Backend │────▶│  Neon Postgres  │
│  FRED, Yahoo,   │     │  Fetchers & Calc │     │   (Serverless)  │
│  DeFiLlama...   │     └──────────────────┘     └────────┬────────┘
└─────────────────┘                                       │
                                                          ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  React Frontend │◀────│ Cloudflare Worker│◀────│   API Layer     │
│  (CF Pages)     │     │    (Edge API)    │     │                 │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

## Tech Stack

- **Backend:** TypeScript, Node.js
- **Database:** Neon PostgreSQL (serverless)
- **API:** Cloudflare Workers
- **Frontend:** React, Vite, Tailwind CSS
- **Hosting:** Cloudflare Pages
- **Scheduler:** GitHub Actions (daily cron)

## Development

```bash
# Install dependencies
npm install

# Set up environment
cp .env.example .env
# Edit .env with your credentials

# Run database migrations
npm run migrate

# Fetch indicator data
npm run fetch

# Calculate PXI score
npm run calculate

# Start frontend dev server
cd frontend && npm run dev
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `PG_HOST` | Neon database host |
| `PG_DATABASE` | Database name |
| `PG_USER` | Database user |
| `PG_PASSWORD` | Database password |
| `FRED_API_KEY` | FRED API key ([get one free](https://fred.stlouisfed.org/docs/api/api_key.html)) |

## Data Sources

- **FRED** - Federal Reserve economic data
- **Yahoo Finance** - Market prices, ETFs, currencies
- **DeFiLlama** - Stablecoin market cap
- **Coinglass** - BTC funding rates
- **Farside** - BTC ETF flows
- **CNN** - Fear & Greed index

## License

MIT
