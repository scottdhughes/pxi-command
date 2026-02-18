# Taylor Performance Execution Report — 2026-02-18

## Scope completed
Implemented requested quick wins in `/Users/scott/polymarket-monitor` and ran one bounded monitor pass (`--no-execute --once`, DOME/KALSHI disabled).

## Implemented changes

1. **Per-scan caps with config knobs**
   - Added in `polymarket_monitor/config.py`:
     - `max_signals_per_scan: int = 250`
     - `max_tail_signals_per_scan: int = 100`
   - Applied post-aggregation in `PolymarketMonitor._apply_signal_caps(...)`.

2. **Per-market TAIL_EVENT cooldown knob (default 2h)**
   - Added `tail_event_cooldown_seconds: int = 7200` in config.
   - Wired in `detectors/tail_event_detector.py` via `timedelta(seconds=settings.tail_event_cooldown_seconds)`.

3. **Compact logging default + verbose table flag**
   - Added CLI flag `--verbose-signals`.
   - Default path now prints compact summaries (`Signals: total=... | by_type`), full table only when verbose flag is set.

4. **Final perf summary line**
   - Added `Perf:` line per scan including:
     - scan duration
     - detector counts
     - accepted/rejected signal counts
     - paper trade delta

5. **Expanded CI lint scope beyond tests**
   - Updated `.github/workflows/ci.yml`:
     - keep full lint on tests
     - added source lint scope (`--select F`) for hardened modules:
       - `__main__.py`, `tail_event_detector.py`, `executor.py`, `risk_manager.py`, `health.py`, `market.py`

---

## Validation results

- `ruff check tests` -> pass
- `ruff check --select F <hardened modules>` -> pass
- `pytest -q tests` -> **26 passed**
- `python -m polymarket_monitor --help` -> pass (includes new `--verbose-signals`)

---

## Bounded monitoring run (exactly one pass)

Command profile:
- DOME/KALSHI disabled via empty env vars
- `--no-execute --once`
- Log: `/tmp/polybot-perf-20260217-203244.log`
- Data dir: `/tmp/polybot-perf-data-20260217-203244`
- Exit code: `0`

Observed metrics:
- **Scan timing:** `9.93s`
- **Signals by type:** `TAIL_EVENT=100`
- **Paper trade delta:** `+15`
- **Perf line:** `duration=9.93s | detectors=tail_event=1079 | accepted=100 | rejected=979 | paper_delta=+15`
- **Log size:** `532,782 bytes` (~520 KB)

---

## Before/After comparison

| Metric | Baseline (prior run) | After quick wins | Change |
|---|---:|---:|---:|
| Scan duration | 23.64s | 9.93s | **-58.0%** |
| TAIL_EVENT signals emitted | 1083 | 100 | **-90.8%** |
| Paper trade delta | +452 | +15 | **-96.7%** |
| Log size | ~843 KB | ~520 KB | **-38.3%** |

## Did performance improve?

**Yes — materially improved.**

- Throughput and scan latency improved strongly.
- Tail-event flood was capped as intended.
- Paper-trade growth was sharply reduced.
- Log volume dropped, but remains larger than ideal due to remaining per-signal internal logging paths (next target: further log compaction/sampling).

---

## Files changed for this execution

- `/Users/scott/polymarket-monitor/polymarket_monitor/config.py`
- `/Users/scott/polymarket-monitor/polymarket_monitor/detectors/tail_event_detector.py`
- `/Users/scott/polymarket-monitor/polymarket_monitor/__main__.py`
- `/Users/scott/polymarket-monitor/polymarket_monitor/health.py`
- `/Users/scott/polymarket-monitor/.github/workflows/ci.yml`
