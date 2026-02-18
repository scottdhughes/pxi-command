# Taylor Deep Familiarization Brief: Polybot
**Date:** 2026-02-18  
**Repo under review:** `/Users/scott/polymarket-monitor`  
**GitHub remote:** `https://github.com/scottdhughes/polybot.git` (branch `main`, commit `e896914`)  
**Fly app:** `polybot-whale` (`iad`)

---

## 1) System architecture map

## 1.1 Runtime topology

```text
PolymarketMonitor (__main__.py)
  ├─ Data clients
  │   ├─ GammaClient (Polymarket market metadata/holders)
  │   ├─ DataApiClient (prices/trades)
  │   ├─ DomeClient (optional; Kalshi + wallet analytics + x-platform matches)
  │   └─ KalshiClient (optional; direct RSA-PSS Kalshi mark-to-market)
  │
  ├─ Shared cache
  │   └─ MarketCache(ttl=300s)
  │
  ├─ Detectors
  │   ├─ Polymarket parallel set: whale, arbitrage, volume, tail, momentum
  │   ├─ Kalshi detector (if Dome enabled; returns signals + kalshi market cache)
  │   ├─ Cross-platform arb (if enabled; consumes both caches)
  │   └─ Semantic detector (hourly throttle)
  │
  ├─ Signal processing
  │   ├─ PaperTrader.create_trade_from_signal(signal)  [always attempted]
  │   └─ Executor.execute(signal) if auto_execute AND confidence >= 0.75
  │
  ├─ P&L maintenance
  │   ├─ Kalshi direct API mark-to-market (open Kalshi tickers)
  │   ├─ check_resolutions()
  │   └─ update_unrealized_pnl()
  │
  └─ Health server (port 8080)
      ├─ /health
      ├─ /status
      ├─ /journal
      └─ /journal.csv
```

## 1.2 Control-loop behavior (actual)
- Scan cadence is tied to `holder_scan_interval` (default 300s).  
- Semantic scan is explicitly throttled to once/hour (`SEMANTIC_SCAN_INTERVAL = 1h`).  
- Per-signal Discord alerts are currently disabled in code; daily summary flow is active.

## 1.3 Persistence and deployment substrate
- State is file-based (`/data/paper_trades.json`, `/data/relationships.json`) rather than DB.  
- Fly volume mount persists `/data` across restarts.  
- Fly health check targets `/health`.

### Evidence (architecture)
- Main loop + detector composition + semantic throttle: `polymarket_monitor/__main__.py:38-39, 84-109, 237-243, 272-287`  
- Paper trading always attempted per signal: `polymarket_monitor/__main__.py:313-316`  
- Auto-exec gating in main loop: `polymarket_monitor/__main__.py:318-322`  
- Kalshi mark-to-market path: `polymarket_monitor/__main__.py:335-356`  
- Fly persistence + health: `fly.toml:19-21, 35-40`

---

## 2) Strategy intent vs implementation mapping

Intent source used: `/Users/scott/pxi/memory/polybot-trading-strategies-2026-02-18.txt`.

| Strategy | Intent (doc) | Actual implementation | Parity | Notes |
|---|---|---|---|---|
| Tail Event (Poly) | Enabled, <5% / >95%, $5k min volume, max size $15 | Config + execution align with enabled + size cap 15 | ✅ Mostly aligned | `config.py:73-76, 197-202` |
| Momentum (Poly) | 3 consecutive 5% moves, bounded range, enabled | **Implemented as 2 intervals**, range widened to 5%-95% | ⚠️ Drift | `momentum_detector.py:45, 52-53` vs intent `...strategies...txt:56-59` |
| Whale (Poly) | New-account + correlated + obscure market pattern | Core thresholds and Dome boost present | ✅ Aligned | `config.py:35-46, 114-116` |
| Arbitrage (Poly/NegRisk) | Enabled, fee-buffered min profit | Enabled in config and detector path | ✅ Mostly aligned | Execution layer still has order-token TODOs (see risks) |
| Volume Spike | Document says disabled | Disabled for auto-exec, but detector still runs and paper trades can still be created | ⚠️ Semantic mismatch | `config.py:203-208`, `__main__.py:240`, `paper_trader.py:368-487` |
| Semantic Relationship | Enabled, hourly clustering, 75% similarity/80% confidence | Enabled, hourly throttled in main loop | ✅ Aligned | `config.py:82-88`, `__main__.py:272-287` |
| Price Spike (Poly) | Watch-only mode | Detector exists, but **not wired into main loop** | ❌ Gap | Detector file exists `price_spike_detector.py`, but missing from `__main__.py` detector setup/tasks (`84-92`, `237-243`) |
| Kalshi signals | Watch-only (with cross-platform monitoring) | Mixed: many watch-only/disabled, but CROSS_PLATFORM_ARB is auto_execute true | ⚠️ Mixed | `config.py:226-274` (notably `CROSS_PLATFORM_ARB auto_execute: True`) |
| Risk Management | Hard checks: halt/cooldown/loss/exposure/liquidity | Logic exists, but liquidity check requires nonzero `market_liquidity` and call-sites pass default 0 | ⚠️ Partially effective | `risk_manager.py:197-202`; executor call path uses default `market_liquidity=0` |
| Position Sizing | Confidence tiers + quarter Kelly | Implemented as described | ✅ Aligned | `position_sizer.py:141-163`, `102-139` |

## Additional high-signal drift not obvious in intent doc
1. **Per-signal Discord alerting drift**: README/docs imply per-signal alerts, code comments disable per-signal alerts in favor of daily digest.  
2. **Cross-platform execution realism**: signals can request Kalshi-side actions, but executor is Polymarket `OrderManager` only.

### Evidence (intent mapping)
- Intent strategy states: `/Users/scott/pxi/memory/polybot-trading-strategies-2026-02-18.txt:54-65, 108-112, 134-141, 142-173`  
- Runtime detector set excludes price spike: `polymarket_monitor/__main__.py:84-92, 237-243`  
- Price spike detector exists: `polymarket_monitor/detectors/price_spike_detector.py:30-47, 61-69`  
- Kalshi config watch-only/mixed: `polymarket_monitor/config.py:226-274`

---

## 3) Local runbook + GitHub/Fly deployment model

## 3.1 Local runbook (operator-ready)
1. **Clone + install**
   ```bash
   git clone https://github.com/scottdhughes/polybot.git
   cd polybot
   pip install -e .
   cp .env.example .env
   ```
2. **Start in paper mode first**
   ```bash
   python -m polymarket_monitor --no-execute
   ```
3. **Optional live mode (requires key)**
   ```bash
   python -m polymarket_monitor
   ```
4. **Health checks**
   - `GET /health`
   - `GET /status`
   - `GET /journal`
   - `GET /journal.csv`

## 3.2 GitHub model (current)
- Remote is configured to `scottdhughes/polybot`.
- No GitHub Actions workflows were found under `.github/workflows` in this repo snapshot.
- Practical implication: CI/CD guardrails are currently manual/local.

## 3.3 Fly deployment model (current)
- App: `polybot-whale`, region `iad`.
- Health endpoint `/health` with 30s interval checks.
- Persistent volume mounted to `/data` (`polymarket_data`).
- Deploy appears manual (`fly deploy`), with secrets set via `fly secrets set ...`.

### Evidence (runbook/deploy)
- Quickstart + deploy commands: `README.md:65-82, 296-308`  
- Monitoring endpoints: `README.md:273-281`  
- Fly config: `fly.toml:6-7, 19-21, 23-40`  
- GitHub linkage observed via command:
  ```text
  git remote -v
  origin https://github.com/scottdhughes/polybot.git (fetch)
  origin https://github.com/scottdhughes/polybot.git (push)
  ```
- No workflows observed via command:
  ```text
  find .github/workflows -maxdepth 2 -type f
  # (no output)
  ```

---

## 4) Data/model dependencies and operational risks

## 4.1 Data + model dependency graph
- **Mandatory for baseline operation**
  - Polymarket Gamma API + Data API (`config.py:11-14`)  
- **Feature-gated/optional**
  - Dome API for enhanced whale + Kalshi + cross-platform (`config.py:110-126`)  
  - Direct Kalshi API creds for mark-to-market (`config.py:158-160`)  
  - Discord webhook for external alerting (`config.py:24`)  
- **Local ML/runtime dependencies**
  - `sentence-transformers`, `chromadb` for semantic pipeline
  - `py-clob-client` for live execution path
  - Dev tools (`pytest`, `pytest-asyncio`, `ruff`) are optional extras

## 4.2 Operational risk register (highest impact first)

### R1 — Live execution direction/token correctness risk (**critical**)
- `_execute_directional()` defaults to `outcome="YES"`, uses `token_id = signal.market_id` with TODO for real token resolution.  
- Cross-platform signals can recommend Kalshi-side action, but executor path is Polymarket order manager only.
- **Impact:** wrong side or wrong instrument in live mode.

### R2 — Liquidity control is effectively bypassed at runtime (**critical**)
- Risk manager has liquidity check, but only if `market_liquidity > 0`.  
- Current call sites pass default 0 through executor path.
- **Impact:** liquidity protection not actually enforced.

### R3 — Circuit breaker is configured but not implemented (**high**)
- `circuit_breaker_pct` and enum exist, but no check path in risk logic.
- **Impact:** intended adverse-move stop is non-functional.

### R4 — Strategy wiring drift: Price Spike not running (**high**)
- Intent says watch-only; detector exists; main loop never invokes it.
- **Impact:** silent blind spot + documentation mismatch.

### R5 — Volume-spike “disabled” semantics are ambiguous (**medium**)
- Disabled for auto-execution, not disabled for detection/paper-trade generation.
- **Impact:** performance interpretation can be confused; “disabled” is operationally ambiguous.

### R6 — Deployment reproducibility/guardrails gap (**medium**)
- No in-repo GH workflows observed; tests exist but dev extras absent in current shell (`No module named pytest`).
- **Impact:** drift and regressions can reach deploy path undetected.

### R7 — Cross-platform settlement/execution basis risk (**medium**)
- Detector acknowledges settlement differences and dual-leg timing risk; current execution path is single-stack.
- **Impact:** false “arb” assumptions if treated as executable without a dual-leg engine.

### Evidence (dependencies/risks)
- Dependency manifest: `pyproject.toml:7-35`  
- Direction/token TODO + default YES: `execution/executor.py:202-205`  
- Liquidity guard condition: `execution/risk_manager.py:197-202`  
- Call path with default liquidity: `execution/executor.py:104-108, 162-164` and `__main__.py:321`  
- Circuit breaker only declared: `config.py:101`, `risk_manager.py:31, 104`  
- Price spike detector unhooked: `__main__.py:84-92, 237-243` vs `detectors/price_spike_detector.py:30-47`

---

## 5) Prioritized next steps with acceptance criteria

## P0 (safety/correctness before further optimization)

### P0.1 Fix execution correctness and side/token mapping
**Scope**
- Implement deterministic mapping from condition/outcome to tradable token IDs.
- Honor `recommended_action` (`BUY_YES` vs `BUY_NO`) in executor.
- Block unsupported signal types from live execution (especially cross-platform/Kalshi until dual-leg support exists).

**Acceptance criteria**
- Unit tests prove `BUY_NO` routes to NO token and `BUY_YES` to YES token.
- Any signal lacking resolvable token IDs is hard-rejected pre-order.
- CROSS_PLATFORM_ARB live execution is either explicitly disabled or uses a validated dual-leg executor.

### P0.2 Enforce liquidity and circuit-breaker controls end-to-end
**Scope**
- Pass real market liquidity into `Executor.execute(...)` from cached market context.
- Implement circuit-breaker adverse-move logic in risk layer.

**Acceptance criteria**
- Test: low-liquidity market correctly rejects with `REJECTED_LOW_LIQUIDITY`.
- Test: adverse move beyond threshold triggers halt and blocks subsequent trades.
- `/status` exposes both controls as active checks.

## P1 (strategy parity + documentation reliability)

### P1.1 Wire Price Spike detector into scan loop (watch-only)
**Scope**
- Instantiate `PriceSpikeDetector` and add it to parallel detector tasks.
- Add explicit `EXECUTION_CONFIG["PRICE_SPIKE"]` as watch-only (`auto_execute: false`).

**Acceptance criteria**
- Scan logs show price spike detector execution.
- PRICE_SPIKE signals appear in paper journal/status; no live execution attempts.
- Strategy doc and runtime behavior align.

### P1.2 Resolve momentum spec drift explicitly
**Scope**
- Decide canonical momentum rules (2 vs 3 intervals; 5-95 vs 10-90) and encode in config (not hardcoded).
- Align docs + strategy text + implementation.

**Acceptance criteria**
- One authoritative setting source for intervals/range.
- Regression tests cover chosen thresholds/range.
- No contradictory references remain in README/docs/code comments.

## P2 (operational quality and deployment discipline)

### P2.1 Add CI guardrails and deploy gates
**Scope**
- Add GitHub Actions for lint + unit tests + smoke startup.
- Optional gated deploy workflow to Fly (manual approval for production).

**Acceptance criteria**
- PRs fail on lint/test regressions.
- Tagged/main deploy path runs through a consistent checklist.
- Reproducible `make test`/`python -m pytest` path documented.

### P2.2 Complete research-validation backlog
**Scope**
- Execute outstanding runtime checks listed in `docs/RESEARCH_NOTES.md`.
- Publish periodic calibration metrics by signal type (frequency, win rate, realized edge).

**Acceptance criteria**
- Remaining unchecked validation items are closed or explicitly deprioritized.
- `/status` includes per-signal calibration stats for real monitoring.

---

## Command evidence snapshot (this review)

```text
$ cd /Users/scott/polymarket-monitor && git remote -v
origin  https://github.com/scottdhughes/polybot.git (fetch)
origin  https://github.com/scottdhughes/polybot.git (push)

$ git branch --show-current && git rev-parse --short HEAD
main
e896914

$ find .github/workflows -maxdepth 2 -type f
# (no output)

$ ls -la tests
tests/test_thresholds.py present

$ python3 -m pytest -q
/Library/Developer/CommandLineTools/usr/bin/python3: No module named pytest
```

---

## Bottom line
Polybot has strong detector breadth and good research intent, but the highest-risk execution/control paths need hardening before trusting live automation behavior. The fastest high-value move is **P0.1 + P0.2** (execution correctness + enforced risk controls), then strategy parity cleanup (notably Price Spike and momentum spec drift).