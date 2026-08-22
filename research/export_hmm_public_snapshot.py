#!/usr/bin/env python3
"""Build a mutable public-data smoke snapshot for the PXI HMM challenger.

Yahoo adjusted closes and PXI public history are current reconstructions, not
immutable historical vintages. The resulting snapshot therefore sets
``point_in_time_guarantee=false`` and can never pass the evidence gate. Network
access is confined to this explicit exporter; unit tests do not call it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SYMBOLS = ("SPY", "TLT", "GLD", "^VIX")
HISTORY_ORIGINS = ("legacy_unclassified", "live_recorded", "retrospective_reconstruction")
PXI_HISTORY_URL = "https://api.pxicommand.com/api/history?days=365"
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def fetch_bytes(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 PXI-HMM-Research/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read()


def yahoo_url(symbol: str, period1: int, period2: int) -> str:
    query = urllib.parse.urlencode(
        {
            "period1": period1,
            "period2": period2,
            "interval": "1d",
            "events": "div,splits",
            "includeAdjustedClose": "true",
        }
    )
    return f"{YAHOO_CHART_URL.format(symbol=urllib.parse.quote(symbol, safe=''))}?{query}"


def parse_yahoo(payload: bytes) -> dict[str, float]:
    parsed = json.loads(payload)
    error = parsed.get("chart", {}).get("error")
    if error:
        raise ValueError(f"Yahoo chart error: {error}")
    result = parsed["chart"]["result"][0]
    timestamps = result["timestamp"]
    adjusted = result["indicators"]["adjclose"][0]["adjclose"]
    output: dict[str, float] = {}
    for timestamp, value in zip(timestamps, adjusted, strict=True):
        if isinstance(value, (int, float)) and math.isfinite(float(value)) and float(value) > 0:
            observed_on = datetime.fromtimestamp(timestamp, tz=timezone.utc).date().isoformat()
            output[observed_on] = float(value)
    return output


def history_origin_for_row(row: dict[str, Any]) -> str:
    """Return the API provenance label, defaulting only a missing legacy field."""
    origin = row.get("history_origin", "legacy_unclassified")
    if origin not in HISTORY_ORIGINS:
        raise ValueError(f"PXI public history returned invalid history_origin {origin!r}")
    return origin


def build_snapshot() -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    # Yahoo treats period2 as exclusive; add one day to include the current day.
    period2 = int(now.timestamp()) + 86_400
    period1 = period2 - (25 * 366 * 86_400)
    fetched_at = now.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    artifacts: dict[str, dict[str, str]] = {}
    price_series: dict[str, dict[str, float]] = {}
    for symbol in SYMBOLS:
        url = yahoo_url(symbol, period1, period2)
        payload = fetch_bytes(url)
        price_series[symbol] = parse_yahoo(payload)
        artifacts[f"yahoo:{symbol}"] = {
            "url": url,
            "fetched_at": fetched_at,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "mutability": "current provider response; not a historical vintage",
        }

    pxi_payload = fetch_bytes(PXI_HISTORY_URL)
    pxi_parsed = json.loads(pxi_payload)
    pxi_rows = pxi_parsed.get("data", [])
    if not isinstance(pxi_rows, list):
        raise ValueError("PXI public history data must be an array")
    pxi_by_date: dict[str, dict[str, Any]] = {}
    for row_index, row in enumerate(pxi_rows):
        if not isinstance(row, dict):
            raise ValueError(f"PXI public history row {row_index} must be an object")
        if row.get("regime") not in {"RISK_ON", "TRANSITION", "RISK_OFF"}:
            continue
        if not isinstance(row.get("date"), str):
            raise ValueError(f"PXI public history row {row_index} requires a date")
        normalized_row = dict(row)
        normalized_row["history_origin"] = history_origin_for_row(row)
        pxi_by_date[row["date"]] = normalized_row
    if not pxi_by_date:
        raise ValueError("PXI public history returned no usable regime rows")
    artifacts["pxi:history"] = {
        "url": PXI_HISTORY_URL,
        "fetched_at": fetched_at,
        "sha256": hashlib.sha256(pxi_payload).hexdigest(),
        "mutability": "current public history response; not an immutable vintage",
    }

    pxi_start, pxi_end = min(pxi_by_date), max(pxi_by_date)
    full_spy_dates = sorted(price_series["SPY"])
    spy_session_ordinals = {observed_on: index for index, observed_on in enumerate(full_spy_dates)}
    expected_dates = [observed_on for observed_on in full_spy_dates if pxi_start <= observed_on <= pxi_end]
    common_dates = sorted(set(pxi_by_date).intersection(*(set(price_series[symbol]) for symbol in SYMBOLS)))
    common_dates = [observed_on for observed_on in common_dates if pxi_start <= observed_on <= pxi_end]
    if len(common_dates) < 100:
        raise ValueError(f"only {len(common_dates)} exact-date rows were available; at least 100 are required for a smoke run")

    rows = []
    for observed_on in common_dates:
        rows.append(
            {
                "snapshot_id": f"mutable-public-{observed_on}",
                "decision_date": observed_on,
                "session_ordinal": spy_session_ordinals[observed_on],
                # This nominal end-of-day timestamp is not asserted as historical
                # evidence; the snapshot is explicitly non-point-in-time.
                "available_at": f"{observed_on}T23:59:59Z",
                "immutable_snapshot": False,
                "adjusted_closes": {symbol: price_series[symbol][observed_on] for symbol in SYMBOLS},
                "price_observation_dates": {symbol: observed_on for symbol in SYMBOLS},
                "price_available_at": {symbol: f"{observed_on}T23:59:59Z" for symbol in SYMBOLS},
                "price_sources": {symbol: f"yahoo:{symbol}" for symbol in SYMBOLS},
                "pxi_regime": pxi_by_date[observed_on]["regime"],
                "pxi_regime_observation_date": observed_on,
                "pxi_regime_available_at": f"{observed_on}T23:59:59Z",
                "pxi_regime_source": "pxi:history",
                "history_origin": pxi_by_date[observed_on]["history_origin"],
            }
        )

    return {
        "schema_version": "pxi-hmm-market-snapshot/v1",
        "dataset_id": f"pxi-hmm-public-smoke-{fetched_at[:10]}",
        "generated_at": fetched_at,
        "point_in_time_guarantee": False,
        "storage_contract": "mutable-public-download/v1",
        "price_contract": {
            "portfolio_assets": ["SPY", "TLT", "GLD"],
            "regime_indicator": "^VIX",
            "price_field": "adjusted_close",
            "total_return_adjusted_assets": True,
            "corporate_action_policy": "Use provider adjusted close for ETF splits and distributions; current reconstruction only.",
            "currency": "USD",
        },
        "cadence_contract": {
            "trading_session": "SPY",
            "calendar_id": "yahoo-spy-observed-session-ordinal/v1-mutable-proxy",
            "annualization_periods": 252,
            "session_ordinal_source": "yahoo:SPY",
            "consecutive_required": True,
            "expected_decision_dates": expected_dates,
        },
        "source_artifacts": artifacts,
        "source_notes": [
            "Yahoo adjusted-close history and PXI public history were downloaded as current mutable responses.",
            "PXI public history is capped at 365 calendar rows, so the exact-date join is only a recent wiring smoke test.",
            "PXI history_origin is propagated row by row; an older API response without that field is labeled legacy_unclassified.",
            "Expected sessions and ordinals use the full Yahoo SPY response over the PXI-history date range as a mutable proxy calendar.",
            "No PXI regimes are fabricated for the longer Yahoo-only history.",
            "Historical first-availability timestamps and immutable vintages are not available.",
            "This snapshot is suitable only for an end-to-end smoke run and is unconditionally NO_GO.",
        ],
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="research/out/hmm_public_snapshot.json")
    args = parser.parse_args()

    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot = build_snapshot()
    output_path.write_text(json.dumps(snapshot, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(f"Wrote {len(snapshot['rows'])} rows to {output_path}")
    expected_dates = snapshot["cadence_contract"]["expected_decision_dates"]
    actual_dates = {row["decision_date"] for row in snapshot["rows"]}
    missing = [value for value in expected_dates if value not in actual_dates]
    print(f"Expected SPY sessions: {len(expected_dates)}; joined rows: {len(actual_dates)}; current gap: {len(missing)}")
    provenance_counts = {origin: 0 for origin in HISTORY_ORIGINS}
    for row in snapshot["rows"]:
        provenance_counts[row["history_origin"]] += 1
    print(f"PXI history provenance counts: {json.dumps(provenance_counts, sort_keys=True)}")
    if missing:
        print(f"Missing expected sessions: {', '.join(missing)}")
    print("PXI history is capped at 365 calendar rows; this is a recent wiring smoke, not a long-history backtest")
    print("point_in_time_guarantee=false; governance is forced to NO_GO")


if __name__ == "__main__":
    main()
