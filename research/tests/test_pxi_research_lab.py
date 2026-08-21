from __future__ import annotations

import math
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pxi_research_lab import Config, attach_targets, build_purged_slices, run, validate_snapshot


def synthetic_snapshot(point_in_time: bool = True, rows: int = 260) -> dict:
    rng = np.random.default_rng(7)
    start = date(2024, 1, 2)
    benchmark = 100.0
    output = []
    latent_values = []
    for index in range(rows):
        latent = math.sin(index / 13.0) + rng.normal(0, 0.15)
        latent_values.append(latent)
        if index > 0:
            benchmark *= 1 + ((0.0015 * latent_values[index - 1]) + rng.normal(0, 0.001))
        decision = start + timedelta(days=index)
        output.append(
            {
                "decision_date": decision.isoformat(),
                "available_at": f"{decision.isoformat()}T23:00:00Z",
                "features": {
                    "pxi_score": 50 + rng.normal(0, 4),
                    "stable_signal": latent,
                    "redundant_signal": latent * 0.99,
                },
                "benchmark_close": benchmark,
            }
        )
    return {
        "schema_version": "pxi-research-snapshot/v1",
        "dataset_id": "synthetic",
        "generated_at": "2026-08-21T00:00:00Z",
        "point_in_time_guarantee": point_in_time,
        "feature_version": "test-v1",
        "benchmark": {"symbol": "TEST", "price_source": "synthetic"},
        "rows": output,
    }


class ResearchLabTests(unittest.TestCase):
    def test_rejects_obvious_target_leakage(self) -> None:
        snapshot = synthetic_snapshot()
        snapshot["rows"][0]["features"]["future_return_7d"] = 1.0
        self.assertTrue(any("forbidden" in error for error in validate_snapshot(snapshot)))

    def test_walk_forward_slices_purge_overlapping_targets(self) -> None:
        snapshot = synthetic_snapshot(rows=220)
        rows = snapshot["rows"]
        targets, target_dates = attach_targets(rows, 7)
        config = Config(min_train_rows=80, test_rows=20, step_rows=20)
        slices = build_purged_slices(rows, targets, target_dates, config)
        self.assertGreaterEqual(len(slices), 3)
        for train_indices, test_indices in slices:
            test_start = date.fromisoformat(rows[int(test_indices[0])]["decision_date"])
            self.assertTrue(all(target_dates[int(index)] < test_start for index in train_indices))

    def test_non_vintage_snapshot_is_always_no_go(self) -> None:
        snapshot = synthetic_snapshot(point_in_time=False)
        config = Config(
            horizons_calendar_days=(7,),
            min_train_rows=80,
            test_rows=20,
            step_rows=20,
            minimum_oos_rows=40,
            minimum_walk_forward_slices=2,
        )
        report = run(snapshot, config)
        self.assertEqual(report["governance"]["status"], "NO_GO")
        self.assertTrue(any("point-in-time" in reason for reason in report["horizons"][0]["gate_reasons"]))

    def test_ridge_uses_signal_without_leaking_across_slices(self) -> None:
        snapshot = synthetic_snapshot()
        config = Config(
            horizons_calendar_days=(7,),
            min_train_rows=80,
            test_rows=20,
            step_rows=20,
            ridge_alpha=5,
            minimum_oos_rows=40,
            minimum_walk_forward_slices=2,
        )
        report = run(snapshot, config)
        horizon = report["horizons"][0]
        self.assertGreaterEqual(horizon["walk_forward_slices"], 3)
        self.assertLess(horizon["ridge"]["rmse_pct"], horizon["baseline"]["rmse_pct"])
        self.assertTrue(all(item["purge_verified"] for item in horizon["slices"]))
        self.assertTrue(report["redundancy_diagnostics"]["high_correlation_pairs"])


if __name__ == "__main__":
    unittest.main()
