# PXI /COMMAND - AI Context

> Use this file as ground truth. Follow its conventions. Don't invent commands.

## What This System Does

- Aggregates 28 macro/market indicators into a single 0-100 "market strength" score
- Fetches data daily from FRED, Yahoo Finance, DeFiLlama, Coinglass, CNN Fear & Greed
- Stores historical values in Neon PostgreSQL (serverless)
- Serves a read-only JSON API via Cloudflare Workers
- Displays score + sparkline + category breakdown on a minimal React frontend

## Architecture

```
Data Sources (FRED, Yahoo, etc.)
        │
        ▼
┌──────────────────────────┐
│  Node.js Fetchers        │  ← src/fetchers/*.ts
│  (runs daily via cron)   │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│  Neon PostgreSQL         │  ← Tables: indicator_values, pxi_scores, category_scores
│  (serverless, us-east-1) │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│  Cloudflare Worker       │  ← worker/api.ts
│  (Edge API)              │
│  pxi-api.novoamorx1.workers.dev
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│  React Frontend          │  ← frontend/src/App.tsx
│  (Cloudflare Pages)      │
│  pxicommand.com
└──────────────────────────┘
```

## Key Modules

| Path | Responsibility |
|------|----------------|
| `src/fetchers/fred.ts` | Fetches FRED data (liquidity, credit, macro) |
| `src/fetchers/yahoo.ts` | Fetches Yahoo Finance (VIX, ETFs, currencies) |
| `src/fetchers/crypto.ts` | Fetches crypto data (DeFiLlama, Coinglass) |
| `src/fetchers/alternative-sources.ts` | Calculated breadth indicators, CNN F&G |
| `src/config/indicators.ts` | **Master list of all 28 indicators** - weights, sources, normalization |
| `src/calculators/pxi.ts` | Normalizes values, calculates category & composite scores |
| `src/db/connection.ts` | PostgreSQL connection pool |
| `worker/api.ts` | Cloudflare Worker API (read-only, rate-limited) |
| `frontend/src/App.tsx` | Single-page React dashboard |

## Local Dev Commands

```bash
# Install dependencies (from repo root)
npm install
cd frontend && npm install && cd ..

# Run database migrations
npm run migrate

# Fetch all indicator data (takes ~5 min)
npm run fetch

# Calculate PXI score from fetched data
npm run calculate

# Run full daily pipeline (fetch + calculate)
npm run cron:daily

# Start frontend dev server (port 5173)
cd frontend && npm run dev

# Build frontend for production
cd frontend && VITE_API_URL=https://pxi-api.novoamorx1.workers.dev npm run build

# Deploy Cloudflare Worker
cd worker && npx wrangler deploy

# Deploy frontend to Cloudflare Pages
npx wrangler pages deploy frontend/dist --project-name pxi-frontend
```

## Test Commands

```bash
# No test suite yet - this is a known gap
# To manually verify:
npm run fetch    # Check for fetch errors in console
npm run calculate # Should output new PXI score
curl https://pxi-api.novoamorx1.workers.dev/api/pxi | jq .
```

## Code Style & Conventions

- **TypeScript** everywhere (strict mode)
- **ES Modules** - use `.js` extensions in imports (for Node ESM)
- **No semicolons** in frontend, **semicolons** in backend (legacy inconsistency)
- **Tailwind CSS** for styling - no separate CSS files
- **Functional components** only in React
- **No classes** - prefer functions and plain objects
- Indicator IDs are `snake_case` (e.g., `fed_balance_sheet`)
- All dates stored as `YYYY-MM-DD` strings in database

## Do Not Break (Invariants)

1. **`/api/pxi` response shape** - Frontend depends on exact structure:
   ```typescript
   { date, score, label, status, delta: {d1, d7, d30}, categories: [], sparkline: [] }
   ```

2. **Indicator IDs in `src/config/indicators.ts`** - Changing IDs breaks historical data joins

3. **Category names** - Must match between `indicators.ts` and calculator: `liquidity`, `credit`, `volatility`, `breadth`, `macro`, `global`, `crypto`

4. **CORS whitelist in `worker/api.ts`** - Only allows `pxicommand.com` origins

5. **Rate limit: 100 req/min per IP** - Hardcoded in worker, don't remove

## Known Sharp Edges / Gotchas

- **Yahoo Finance rate limits** - Fetcher adds delays between requests; don't parallelize aggressively
- **FRED API** - Some series update weekly/monthly; missing data returns null, not error
- **Neon cold starts** - First query after idle can be slow (~500ms)
- **Wrangler v4** - Use `compatibility_flags = ["nodejs_compat"]`, not deprecated `node_compat = true`
- **Frontend build** - Must pass `VITE_API_URL` env var at build time, not runtime
- **GitHub Actions cron** - Runs at 6:00 AM UTC daily; secrets stored in repo settings
- **Coinglass/Farside scrapers** - Fragile; may break if sites change HTML structure

## Dependency Constraints

| Package | Version | Notes |
|---------|---------|-------|
| Node.js | 20+ | Required for ESM + fetch |
| `@neondatabase/serverless` | ^1.0.0 | Must use this for Worker, not `pg` |
| `yahoo-finance2` | ^2.x | Handles Yahoo auth; don't use raw axios |
| `wrangler` | 4.x | CLI for Cloudflare Workers/Pages |
| React | 19.x | Using new JSX transform |
| Vite | 7.x | Frontend build tool |

## Security Constraints

- **No secrets in code** - All credentials via environment variables
- **Secrets locations:**
  - Local: `.env` file (gitignored)
  - Worker: Cloudflare Worker secrets (`npx wrangler secret put KEY`)
  - CI: GitHub Actions secrets
- **Required secrets:**
  - `DATABASE_URL` - Neon connection string (postgres://...)
  - `PG_HOST`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD` - For local Node scripts
  - `FRED_API_KEY` - Free from FRED website
- **API is read-only** - No mutations exposed; safe to be public
- **Never commit `.env`** - Verify with `git status` before pushing

## Environment Variables

```bash
# .env (local development)
PG_HOST=ep-xyz.us-east-1.aws.neon.tech
PG_PORT=5432
PG_DATABASE=pxi
PG_USER=pxi_owner
PG_PASSWORD=<secret>
FRED_API_KEY=<secret>
DATABASE_URL=postgres://pxi_owner:<password>@ep-xyz.us-east-1.aws.neon.tech/pxi?sslmode=require
```

## URLs

- **Production site:** https://pxicommand.com
- **API endpoint:** https://pxi-api.novoamorx1.workers.dev/api/pxi
- **Health check:** https://pxi-api.novoamorx1.workers.dev/health
- **OG image:** https://pxi-api.novoamorx1.workers.dev/og-image.svg
- **GitHub repo:** https://github.com/scottdhughes/pxi-command
- **Cloudflare dashboard:** https://dash.cloudflare.com (Workers & Pages)
- **Neon dashboard:** https://console.neon.tech
