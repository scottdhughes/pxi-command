# PXI /COMMAND - AI Context

> Use this file as ground truth. Follow its conventions. Don't invent commands.

## What This System Does

- Aggregates 28 macro/market indicators into a single 0-100 "market strength" score
- Fetches data daily from FRED, Yahoo Finance, DeFiLlama, Coinglass, CNN Fear & Greed
- Stores data in **Cloudflare D1** (SQLite) - no external database
- Uses **Vectorize** for market regime embeddings (similarity search)
- Uses **Workers AI** for regime analysis and text generation
- Serves a read-only JSON API via Cloudflare Workers
- Displays score + sparkline + category breakdown on a minimal React frontend

## Architecture

```
Data Sources (FRED, Yahoo, etc.)
        │
        ▼
┌──────────────────────────┐
│  Node.js Fetchers        │  ← src/fetchers/*.ts
│  (GitHub Actions cron)   │  ← Uses wrangler CLI to write to D1
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│  Cloudflare D1           │  ← SQLite database (no external DB)
│  (pxi-db)                │  ← Tables: indicator_values, pxi_scores, category_scores
└──────────┬───────────────┘
           │
           ├──────────────────────────┐
           ▼                          ▼
┌──────────────────────────┐  ┌──────────────────┐
│  Vectorize               │  │  Workers AI      │
│  (pxi-embeddings)        │  │  (LLM + embed)   │
│  768-dim cosine index    │  │  BGE + Llama 3.1 │
└──────────────────────────┘  └──────────────────┘
           │                          │
           └──────────┬───────────────┘
                      ▼
┌──────────────────────────────────────────────────┐
│  Cloudflare Worker API                           │
│  <your-worker>.workers.dev                       │
│  Endpoints:                                      │
│    GET  /api/pxi      - Current score + history  │
│    GET  /api/analyze  - AI regime analysis       │
│    GET  /api/similar  - Find similar periods     │
│    POST /api/write    - Write data (auth req)    │
└──────────────────────────────────────────────────┘
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
| `src/db/connection.ts` | **D1 client using wrangler CLI** (replaced pg) |
| `worker/api.ts` | Cloudflare Worker API with D1, Vectorize, Workers AI |
| `worker/schema.sql` | D1 database schema |
| `frontend/src/App.tsx` | Single-page React dashboard |

## Local Dev Commands

```bash
# Install dependencies (from repo root)
npm install
cd frontend && npm install && cd ..

# Fetch all indicator data (writes to D1 via wrangler)
npm run fetch

# Calculate PXI score from fetched data
npm run calculate

# Run full daily pipeline (fetch + calculate)
npm run cron:daily

# Start frontend dev server (port 5173)
cd frontend && npm run dev

# Build frontend for production
cd frontend && VITE_API_URL=https://<your-worker>.workers.dev npm run build

# Deploy Cloudflare Worker
cd worker && npx wrangler deploy

# Deploy frontend to Cloudflare Pages
npx wrangler pages deploy frontend/dist --project-name pxi-frontend

# Execute D1 queries directly
npx wrangler d1 execute pxi-db --command "SELECT * FROM pxi_scores ORDER BY date DESC LIMIT 5" --remote
```

## API Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/pxi` | GET | No | Current PXI score, categories, sparkline |
| `/api/analyze` | GET | No | AI-generated regime analysis |
| `/api/similar` | GET | No | Find historically similar market regimes |
| `/api/write` | POST | Yes | Write indicator/category/pxi data |
| `/health` | GET | No | Health check with DB status |
| `/og-image.svg` | GET | No | Dynamic OG image for social sharing |

## Code Style & Conventions

- **TypeScript** everywhere (strict mode)
- **ES Modules** - use `.js` extensions in imports (for Node ESM)
- **No semicolons** in frontend, **semicolons** in backend
- **Tailwind CSS** for styling - no separate CSS files
- **Functional components** only in React
- **No classes** - prefer functions and plain objects
- Indicator IDs are `snake_case` (e.g., `fed_balance_sheet`)
- All dates stored as `YYYY-MM-DD` strings in D1

## Do Not Break (Invariants)

1. **`/api/pxi` response shape** - Frontend depends on exact structure:
   ```typescript
   { date, score, label, status, delta: {d1, d7, d30}, categories: [], sparkline: [] }
   ```

2. **Indicator IDs in `src/config/indicators.ts`** - Changing IDs breaks historical data joins

3. **Category names** - Must match between `indicators.ts` and calculator: `liquidity`, `credit`, `volatility`, `breadth`, `macro`, `global`, `crypto`

4. **CORS whitelist in `worker/api.ts`** - Only allows `pxicommand.com` origins

5. **Rate limit: 100 req/min per IP** - Hardcoded in worker

6. **D1 database ID** - `2254a52c-11b6-40a6-8881-ef6880f70d21` in wrangler.toml

## Known Sharp Edges / Gotchas

- **Yahoo Finance rate limits** - Fetcher adds delays between requests
- **FRED API** - Some series update weekly/monthly; missing data returns null
- **D1 is SQLite** - No array types, no PostgreSQL features
- **D1 client uses wrangler CLI** - Requires wrangler auth in environment
- **Vectorize embeddings** - 768 dimensions using BGE model
- **Workers AI rate limits** - Can't generate all embeddings in one request
- **Wrangler v4** - Use `compatibility_flags = ["nodejs_compat"]`
- **Frontend build** - Must pass `VITE_API_URL` env var at build time
- **GitHub Actions cron** - Runs at 6:00 AM UTC daily

## Dependency Constraints

| Package | Version | Notes |
|---------|---------|-------|
| Node.js | 20+ | Required for ESM + fetch |
| `wrangler` | 4.x | CLI for Cloudflare Workers/Pages/D1 |
| `yahoo-finance2` | ^2.x | Handles Yahoo auth |
| React | 19.x | Using new JSX transform |
| Vite | 7.x | Frontend build tool |

**Removed:** `pg`, `@types/pg` (no longer using PostgreSQL)

## Security Constraints

- **No secrets in code** - All credentials via environment variables
- **Secrets locations:**
  - Local: Wrangler OAuth (auto via `wrangler login`)
  - CI: GitHub Actions secrets (`CLOUDFLARE_API_TOKEN`, `FRED_API_KEY`)
  - Worker: Cloudflare Worker secrets (`WRITE_API_KEY`)
- **Required secrets:**
  - `CLOUDFLARE_API_TOKEN` - For wrangler CLI in GitHub Actions
  - `FRED_API_KEY` - Free from FRED website
  - `WRITE_API_KEY` - For /api/write endpoint (Worker secret)
- **Write endpoint requires auth** - Bearer token in Authorization header
- **Never commit `.env`**

## Cloudflare Resources

| Resource | Name/ID | Binding |
|----------|---------|---------|
| D1 Database | pxi-db / `2254a52c-11b6-40a6-8881-ef6880f70d21` | `DB` |
| Vectorize Index | pxi-embeddings | `VECTORIZE` |
| Workers AI | - | `AI` |
| Worker | pxi-api | - |
| Pages | pxi-frontend | - |

## URLs

After deployment, your API will be available at:
- **API endpoint:** `https://<your-worker>.workers.dev/api/pxi`
- **AI analysis:** `https://<your-worker>.workers.dev/api/analyze`
- **Similar periods:** `https://<your-worker>.workers.dev/api/similar`
- **Health check:** `https://<your-worker>.workers.dev/health`

Configure your own domain and Pages site as needed.
