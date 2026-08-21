#!/usr/bin/env python3
"""Build a non-vintage-safe PXI research smoke snapshot from public sources.

This exporter intentionally sets point_in_time_guarantee=false. It is useful for
testing the research machinery, not for establishing investment evidence.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import statistics
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

PXI_HISTORY_URL = "https://api.pxicommand.com/api/history?days=365"
FRED_SP500_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=SP500"


def fetch_text(url: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "PXI-Research-Lab/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8")


def finite_or_none(value: float | None) -> float | None:
    return value if value is not None and math.isfinite(value) else None


def lag_delta(values: list[float], index: int, lag: int) -> float | None:
    if index < lag:
        return None
    return values[index] - values[index - lag]


def rolling_stat(values: list[float], index: int, window: int, mode: str) -> float | None:
    start = max(0, index - window + 1)
    sample = values[start : index + 1]
    if len(sample) < min(window, 5):
        return None
    if mode == "mean":
        return statistics.fmean(sample)
    if mode == "std":
        return statistics.pstdev(sample)
    raise ValueError(f"unsupported rolling mode: {mode}")


def build_snapshot() -> dict:
    pxi_payload = json.loads(fetch_text(PXI_HISTORY_URL))
    pxi_rows = sorted(pxi_payload.get("data", []), key=lambda row: row["date"])

    price_rows = csv.DictReader(io.StringIO(fetch_text(FRED_SP500_URL)))
    prices: dict[str, float] = {}
    for row in price_rows:
        raw = (row.get("SP500") or "").strip()
        if raw and raw != ".":
            prices[row["observation_date"]] = float(raw)

    scores = [float(row["score"]) for row in pxi_rows]
    output_rows = []
    for index, row in enumerate(pxi_rows):
        decision_date = row["date"]
        close = prices.get(decision_date)
        if close is None:
            continue

        features = {
            "pxi_score": scores[index],
            "pxi_delta_1": finite_or_none(lag_delta(scores, index, 1)),
            "pxi_delta_7": finite_or_none(lag_delta(scores, index, 7)),
            "pxi_delta_30": finite_or_none(lag_delta(scores, index, 30)),
            "pxi_mean_20": finite_or_none(rolling_stat(scores, index, 20, "mean")),
            "pxi_std_20": finite_or_none(rolling_stat(scores, index, 20, "std")),
        }
        output_rows.append(
            {
                "decision_date": decision_date,
                "available_at": f"{decision_date}T23:59:59Z",
                "features": features,
                "benchmark_close": close,
            }
        )

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "schema_version": "pxi-research-snapshot/v1",
        "dataset_id": f"pxi-public-smoke-{generated_at[:10]}",
        "generated_at": generated_at,
        "point_in_time_guarantee": False,
        "feature_version": "pxi-public-history-v1",
        "benchmark": {"symbol": "SP500", "price_source": "FRED:SP500"},
        "source_notes": [
            "PXI history is the current public history response, not an immutable historical vintage.",
            "Only dates with an exact same-day FRED S&P 500 close are included.",
            "This snapshot is a smoke-test input and cannot satisfy the research go-live gate.",
        ],
        "rows": output_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="research/out/public_snapshot.json")
    args = parser.parse_args()

    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot = build_snapshot()
    output_path.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(snapshot['rows'])} rows to {output_path}")
    print("point_in_time_guarantee=false (smoke test only)")


if __name__ == "__main__":
    main()
