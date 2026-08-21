#!/usr/bin/env python3
"""Leakage-aware offline PXI walk-forward research harness.

The harness never writes to Cloudflare or production model storage. It compares
the current fixed PXI composite (a train-only linear calibration of pxi_score)
with a regularized linear model over the supplied feature snapshot.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np

SCHEMA_VERSION = "pxi-research-snapshot/v1"
FORBIDDEN_FEATURE_TOKENS = ("target", "forward", "future", "return", "benchmark_close", "spy_close")


@dataclass(frozen=True)
class Config:
    horizons_calendar_days: tuple[int, ...] = (7, 30)
    min_train_rows: int = 126
    test_rows: int = 21
    step_rows: int = 21
    ridge_alpha: float = 10.0
    round_trip_cost_bps: float = 10.0
    minimum_oos_rows: int = 60
    minimum_walk_forward_slices: int = 3
    minimum_feature_coverage: float = 0.9
    maximum_pairwise_correlation: float = 0.9

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> "Config":
        return cls(
            horizons_calendar_days=tuple(int(value) for value in values.get("horizons_calendar_days", (7, 30))),
            min_train_rows=int(values.get("min_train_rows", 126)),
            test_rows=int(values.get("test_rows", 21)),
            step_rows=int(values.get("step_rows", 21)),
            ridge_alpha=float(values.get("ridge_alpha", 10.0)),
            round_trip_cost_bps=float(values.get("round_trip_cost_bps", 10.0)),
            minimum_oos_rows=int(values.get("minimum_oos_rows", 60)),
            minimum_walk_forward_slices=int(values.get("minimum_walk_forward_slices", 3)),
            minimum_feature_coverage=float(values.get("minimum_feature_coverage", 0.9)),
            maximum_pairwise_correlation=float(values.get("maximum_pairwise_correlation", 0.9)),
        )


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def validate_snapshot(snapshot: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if snapshot.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must equal {SCHEMA_VERSION}")
    rows = snapshot.get("rows")
    if not isinstance(rows, list) or not rows:
        errors.append("rows must be a non-empty list")
        return errors

    point_in_time = snapshot.get("point_in_time_guarantee", False) is True
    if point_in_time and snapshot.get("storage_contract") != "append-only-d1-research-snapshots/v1":
        errors.append("point-in-time snapshots require the append-only D1 storage contract")

    seen_dates: set[str] = set()
    feature_names: set[str] = set()
    for index, row in enumerate(rows):
        try:
            decision_date = parse_date(row["decision_date"])
            available_at = parse_datetime(row["available_at"])
        except (KeyError, TypeError, ValueError) as error:
            errors.append(f"row {index}: invalid date metadata ({error})")
            continue
        if row["decision_date"] in seen_dates:
            errors.append(f"row {index}: duplicate decision_date {row['decision_date']}")
        seen_dates.add(row["decision_date"])
        decision_end = datetime.combine(decision_date, datetime.max.time(), tzinfo=timezone.utc)
        if available_at > decision_end:
            errors.append(f"row {index}: features became available after decision_date")
        features = row.get("features")
        if not isinstance(features, dict) or not features:
            errors.append(f"row {index}: features must be a non-empty object")
            continue
        feature_names.update(features)
        if point_in_time:
            if not isinstance(row.get("snapshot_id"), str) or not row["snapshot_id"].strip():
                errors.append(f"row {index}: point-in-time row requires snapshot_id")
            if row.get("immutable_snapshot") is not True:
                errors.append(f"row {index}: point-in-time row must be marked immutable_snapshot")
            observation_dates = row.get("feature_observation_dates")
            sources = row.get("feature_sources")
            if not isinstance(observation_dates, dict) or set(observation_dates) != set(features):
                errors.append(f"row {index}: feature observation-date provenance is incomplete")
            if not isinstance(sources, dict) or set(sources) != set(features):
                errors.append(f"row {index}: feature source provenance is incomplete")
            if isinstance(observation_dates, dict):
                for name, observed_on in observation_dates.items():
                    try:
                        if parse_date(observed_on) > decision_date:
                            errors.append(f"row {index}: feature {name} was observed after decision_date")
                    except (TypeError, ValueError):
                        errors.append(f"row {index}: feature {name} has invalid observation date")
        close = row.get("benchmark_close")
        if not isinstance(close, (int, float)) or not math.isfinite(close) or close <= 0:
            errors.append(f"row {index}: benchmark_close must be positive and finite")

    for name in sorted(feature_names):
        normalized = name.lower()
        if any(token in normalized for token in FORBIDDEN_FEATURE_TOKENS):
            errors.append(f"forbidden potentially leaky feature name: {name}")
    if "pxi_score" not in feature_names:
        errors.append("features must include pxi_score for the fixed-composite baseline")
    return errors


def feature_matrix(snapshot: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str], np.ndarray]:
    rows = sorted(snapshot["rows"], key=lambda row: row["decision_date"])
    names = sorted({name for row in rows for name in row["features"]})
    matrix = np.array(
        [[float(row["features"].get(name)) if row["features"].get(name) is not None else np.nan for name in names] for row in rows],
        dtype=float,
    )
    return rows, names, matrix


def attach_targets(rows: list[dict[str, Any]], horizon_days: int) -> tuple[np.ndarray, list[date | None]]:
    dates = [parse_date(row["decision_date"]) for row in rows]
    closes = np.array([float(row["benchmark_close"]) for row in rows])
    targets = np.full(len(rows), np.nan)
    target_dates: list[date | None] = [None] * len(rows)
    for index, decision_date in enumerate(dates):
        threshold = decision_date + timedelta(days=horizon_days)
        future_index = next((candidate for candidate in range(index + 1, len(rows)) if dates[candidate] >= threshold), None)
        if future_index is None:
            continue
        targets[index] = ((closes[future_index] / closes[index]) - 1.0) * 100.0
        target_dates[index] = dates[future_index]
    return targets, target_dates


def build_purged_slices(
    rows: list[dict[str, Any]],
    targets: np.ndarray,
    target_dates: list[date | None],
    config: Config,
) -> list[tuple[np.ndarray, np.ndarray]]:
    valid_indices = np.array([index for index, value in enumerate(targets) if math.isfinite(float(value))], dtype=int)
    slices: list[tuple[np.ndarray, np.ndarray]] = []
    for test_offset in range(config.min_train_rows, len(valid_indices) - config.test_rows + 1, config.step_rows):
        test_indices = valid_indices[test_offset : test_offset + config.test_rows]
        test_start = parse_date(rows[int(test_indices[0])]["decision_date"])
        candidate_train = valid_indices[:test_offset]
        train_indices = np.array(
            [index for index in candidate_train if target_dates[int(index)] is not None and target_dates[int(index)] < test_start],
            dtype=int,
        )
        if len(train_indices) < config.min_train_rows:
            continue
        slices.append((train_indices, test_indices))
    return slices


def standardize_train_test(x_train: np.ndarray, x_test: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    medians = np.nanmedian(x_train, axis=0)
    medians = np.where(np.isfinite(medians), medians, 0.0)
    train_imputed = np.where(np.isnan(x_train), medians, x_train)
    test_imputed = np.where(np.isnan(x_test), medians, x_test)
    means = np.mean(train_imputed, axis=0)
    scale = np.std(train_imputed, axis=0)
    scale = np.where(scale > 1e-12, scale, 1.0)
    return (train_imputed - means) / scale, (test_imputed - means) / scale


def fit_ridge(x: np.ndarray, y: np.ndarray, alpha: float) -> tuple[np.ndarray, float]:
    y_mean = float(np.mean(y))
    x_mean = np.mean(x, axis=0)
    centered_x = x - x_mean
    centered_y = y - y_mean
    gram = centered_x.T @ centered_x
    coefficients = np.linalg.pinv(gram + (alpha * np.eye(x.shape[1]))) @ centered_x.T @ centered_y
    intercept = y_mean - float(x_mean @ coefficients)
    return coefficients, intercept


def predict_ridge(x: np.ndarray, coefficients: np.ndarray, intercept: float) -> np.ndarray:
    return x @ coefficients + intercept


def rankdata(values: np.ndarray) -> np.ndarray:
    order = np.argsort(values, kind="mergesort")
    ranks = np.empty(len(values), dtype=float)
    cursor = 0
    while cursor < len(values):
        end = cursor
        while end + 1 < len(values) and values[order[end + 1]] == values[order[cursor]]:
            end += 1
        ranks[order[cursor : end + 1]] = (cursor + end + 2) / 2.0
        cursor = end + 1
    return ranks


def correlation(left: np.ndarray, right: np.ndarray) -> float | None:
    if len(left) < 2 or np.std(left) <= 1e-12 or np.std(right) <= 1e-12:
        return None
    return float(np.corrcoef(left, right)[0, 1])


def wilson_interval(hits: int, total: int, z: float = 1.959963984540054) -> tuple[float | None, float | None]:
    if total <= 0:
        return None, None
    proportion = hits / total
    denominator = 1 + (z * z / total)
    center = (proportion + (z * z / (2 * total))) / denominator
    margin = z * math.sqrt((proportion * (1 - proportion) / total) + (z * z / (4 * total * total))) / denominator
    return max(0.0, center - margin), min(1.0, center + margin)


def metrics(actual: np.ndarray, predicted: np.ndarray, cost_bps: float) -> dict[str, Any]:
    residual = actual - predicted
    rmse = float(math.sqrt(np.mean(residual**2)))
    mae = float(np.mean(np.abs(residual)))
    variance = float(np.sum((actual - np.mean(actual)) ** 2))
    r2 = None if variance <= 1e-12 else float(1 - (np.sum(residual**2) / variance))
    correct = np.sign(actual) == np.sign(predicted)
    hits = int(np.sum(correct))
    ci_low, ci_high = wilson_interval(hits, len(actual))
    rank_ic = correlation(rankdata(predicted), rankdata(actual))
    directional_gross = np.sign(predicted) * actual
    cost_pct = cost_bps / 100.0
    directional_net = directional_gross - cost_pct
    return {
        "observations": int(len(actual)),
        "rmse_pct": rmse,
        "mae_pct": mae,
        "r2": r2,
        "direction_accuracy": hits / len(actual),
        "direction_accuracy_ci95": [ci_low, ci_high],
        "rank_ic": rank_ic,
        "directional_edge_gross_pct": float(np.mean(directional_gross)),
        "directional_edge_after_cost_pct": float(np.mean(directional_net)),
        "cost_assumption_bps_per_signal": cost_bps,
        "portfolio_metric_warning": "Directional edge is a per-signal proxy with overlapping horizons; it is not CAGR or Sharpe.",
    }


def redundancy_diagnostics(names: list[str], matrix: np.ndarray, threshold: float) -> dict[str, Any]:
    coverage = np.mean(np.isfinite(matrix), axis=0)
    medians = np.nanmedian(matrix, axis=0)
    medians = np.where(np.isfinite(medians), medians, 0.0)
    imputed = np.where(np.isnan(matrix), medians, matrix)
    high_pairs = []
    for left in range(len(names)):
        for right in range(left + 1, len(names)):
            value = correlation(imputed[:, left], imputed[:, right])
            if value is not None and abs(value) >= threshold:
                high_pairs.append({"left": names[left], "right": names[right], "correlation": value})

    centered = imputed - np.mean(imputed, axis=0)
    scale = np.std(centered, axis=0)
    usable = scale > 1e-12
    explained: list[float] = []
    if np.sum(usable) >= 2:
        standardized = centered[:, usable] / scale[usable]
        singular_values = np.linalg.svd(standardized, full_matrices=False, compute_uv=False)
        eigenvalues = singular_values**2
        explained = (eigenvalues / np.sum(eigenvalues)).tolist()

    return {
        "feature_coverage": {name: float(coverage[index]) for index, name in enumerate(names)},
        "minimum_feature_coverage": float(np.min(coverage)),
        "high_correlation_pairs": high_pairs,
        "pca_explained_variance_ratio": explained,
        "components_for_80pct_variance": next(
            (index + 1 for index, value in enumerate(np.cumsum(explained)) if value >= 0.8),
            None,
        ),
        "note": "PCA is diagnostic only and is fit on the supplied snapshot, not used for predictions.",
    }


def evaluate_horizon(
    snapshot: dict[str, Any],
    rows: list[dict[str, Any]],
    names: list[str],
    matrix: np.ndarray,
    horizon_days: int,
    config: Config,
) -> dict[str, Any]:
    targets, target_dates = attach_targets(rows, horizon_days)
    slices = build_purged_slices(rows, targets, target_dates, config)
    baseline_column = names.index("pxi_score")
    actual_parts: list[np.ndarray] = []
    baseline_parts: list[np.ndarray] = []
    ridge_parts: list[np.ndarray] = []
    slice_reports = []

    for slice_id, (train_indices, test_indices) in enumerate(slices, start=1):
        y_train = targets[train_indices]
        y_test = targets[test_indices]

        baseline_train, baseline_test = standardize_train_test(
            matrix[train_indices][:, [baseline_column]], matrix[test_indices][:, [baseline_column]]
        )
        baseline_coef, baseline_intercept = fit_ridge(baseline_train, y_train, 1e-8)
        baseline_pred = predict_ridge(baseline_test, baseline_coef, baseline_intercept)

        ridge_train, ridge_test = standardize_train_test(matrix[train_indices], matrix[test_indices])
        ridge_coef, ridge_intercept = fit_ridge(ridge_train, y_train, config.ridge_alpha)
        ridge_pred = predict_ridge(ridge_test, ridge_coef, ridge_intercept)

        actual_parts.append(y_test)
        baseline_parts.append(baseline_pred)
        ridge_parts.append(ridge_pred)
        slice_reports.append(
            {
                "slice_id": slice_id,
                "train_start": rows[int(train_indices[0])]["decision_date"],
                "train_end": rows[int(train_indices[-1])]["decision_date"],
                "latest_train_target_date": target_dates[int(train_indices[-1])].isoformat(),
                "test_start": rows[int(test_indices[0])]["decision_date"],
                "test_end": rows[int(test_indices[-1])]["decision_date"],
                "train_rows": int(len(train_indices)),
                "test_rows": int(len(test_indices)),
                "purge_verified": target_dates[int(train_indices[-1])] < parse_date(rows[int(test_indices[0])]["decision_date"]),
            }
        )

    if not actual_parts:
        return {
            "horizon_calendar_days": horizon_days,
            "walk_forward_slices": 0,
            "oos_observations": 0,
            "baseline": None,
            "ridge": None,
            "gate_status": "NO_GO",
            "gate_reasons": ["no valid purged walk-forward slices"],
            "slices": [],
        }

    actual = np.concatenate(actual_parts)
    baseline_pred = np.concatenate(baseline_parts)
    ridge_pred = np.concatenate(ridge_parts)
    baseline_metrics = metrics(actual, baseline_pred, config.round_trip_cost_bps)
    ridge_metrics = metrics(actual, ridge_pred, config.round_trip_cost_bps)

    reasons = []
    if not snapshot.get("point_in_time_guarantee", False):
        reasons.append("snapshot does not guarantee point-in-time feature vintages")
    if len(actual) < config.minimum_oos_rows:
        reasons.append(f"OOS observations {len(actual)} below minimum {config.minimum_oos_rows}")
    if len(slices) < config.minimum_walk_forward_slices:
        reasons.append(f"walk-forward slices {len(slices)} below minimum {config.minimum_walk_forward_slices}")
    if any(not report["purge_verified"] for report in slice_reports):
        reasons.append("train/test target-overlap purge failed")
    if ridge_metrics["rmse_pct"] >= baseline_metrics["rmse_pct"]:
        reasons.append("ridge RMSE did not beat the fixed-composite baseline")
    if ridge_metrics["direction_accuracy_ci95"][0] is None or ridge_metrics["direction_accuracy_ci95"][0] <= 0.5:
        reasons.append("ridge direction-accuracy 95% lower bound did not exceed 50%")
    if ridge_metrics["directional_edge_after_cost_pct"] <= 0:
        reasons.append("ridge directional edge proxy was not positive after assumed cost")

    return {
        "horizon_calendar_days": horizon_days,
        "walk_forward_slices": len(slices),
        "oos_observations": int(len(actual)),
        "baseline": baseline_metrics,
        "ridge": ridge_metrics,
        "ridge_minus_baseline": {
            "rmse_pct": ridge_metrics["rmse_pct"] - baseline_metrics["rmse_pct"],
            "direction_accuracy": ridge_metrics["direction_accuracy"] - baseline_metrics["direction_accuracy"],
            "rank_ic": None
            if ridge_metrics["rank_ic"] is None or baseline_metrics["rank_ic"] is None
            else ridge_metrics["rank_ic"] - baseline_metrics["rank_ic"],
        },
        "gate_status": "GO" if not reasons else "NO_GO",
        "gate_reasons": reasons,
        "slices": slice_reports,
    }


def run(snapshot: dict[str, Any], config: Config) -> dict[str, Any]:
    validation_errors = validate_snapshot(snapshot)
    if validation_errors:
        raise ValueError("; ".join(validation_errors))

    rows, names, matrix = feature_matrix(snapshot)
    redundancy = redundancy_diagnostics(names, matrix, config.maximum_pairwise_correlation)
    horizons = [evaluate_horizon(snapshot, rows, names, matrix, horizon, config) for horizon in config.horizons_calendar_days]
    global_reasons = []
    if redundancy["minimum_feature_coverage"] < config.minimum_feature_coverage:
        global_reasons.append(
            f"minimum feature coverage {redundancy['minimum_feature_coverage']:.3f} below {config.minimum_feature_coverage:.3f}"
        )
    for horizon in horizons:
        if horizon["gate_status"] != "GO":
            global_reasons.append(f"{horizon['horizon_calendar_days']}d horizon is NO_GO")

    return {
        "report_version": "pxi-research-report/v1",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "dataset": {
            "dataset_id": snapshot["dataset_id"],
            "feature_version": snapshot["feature_version"],
            "rows": len(rows),
            "date_range": [rows[0]["decision_date"], rows[-1]["decision_date"]],
            "point_in_time_guarantee": bool(snapshot.get("point_in_time_guarantee", False)),
            "benchmark": snapshot["benchmark"],
        },
        "config": {
            "horizons_calendar_days": list(config.horizons_calendar_days),
            "min_train_rows": config.min_train_rows,
            "test_rows": config.test_rows,
            "step_rows": config.step_rows,
            "ridge_alpha": config.ridge_alpha,
            "round_trip_cost_bps": config.round_trip_cost_bps,
        },
        "features": names,
        "redundancy_diagnostics": redundancy,
        "horizons": horizons,
        "governance": {
            "status": "GO" if not global_reasons else "NO_GO",
            "reasons": global_reasons,
            "production_effect": "none; this report cannot modify live scoring or models",
        },
    }


def fmt(value: Any, digits: int = 4) -> str:
    return "null" if value is None else f"{value:.{digits}f}"


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# PXI Research Lab Report",
        "",
        f"**Governance status: {report['governance']['status']}**",
        "",
        f"- Dataset: `{report['dataset']['dataset_id']}`",
        f"- Rows: {report['dataset']['rows']} ({report['dataset']['date_range'][0]} to {report['dataset']['date_range'][1]})",
        f"- Point-in-time guarantee: `{str(report['dataset']['point_in_time_guarantee']).lower()}`",
        f"- Production effect: {report['governance']['production_effect']}",
        "",
    ]
    if report["governance"]["reasons"]:
        lines.extend(["## Global blockers", ""] + [f"- {reason}" for reason in report["governance"]["reasons"]] + [""])
    lines.extend(["## Walk-forward results", ""])
    for horizon in report["horizons"]:
        lines.extend(
            [
                f"### {horizon['horizon_calendar_days']}-calendar-day target - {horizon['gate_status']}",
                "",
                f"OOS observations: {horizon['oos_observations']}; purged slices: {horizon['walk_forward_slices']}",
                "",
            ]
        )
        if horizon["baseline"] and horizon["ridge"]:
            lines.extend(
                [
                    "| Model | RMSE | Direction accuracy | 95% CI | Rank IC | Net directional edge proxy |",
                    "| --- | ---: | ---: | ---: | ---: | ---: |",
                    f"| Fixed PXI baseline | {fmt(horizon['baseline']['rmse_pct'])}% | {fmt(horizon['baseline']['direction_accuracy'])} | {fmt(horizon['baseline']['direction_accuracy_ci95'][0])} to {fmt(horizon['baseline']['direction_accuracy_ci95'][1])} | {fmt(horizon['baseline']['rank_ic'])} | {fmt(horizon['baseline']['directional_edge_after_cost_pct'])}% |",
                    f"| Ridge features | {fmt(horizon['ridge']['rmse_pct'])}% | {fmt(horizon['ridge']['direction_accuracy'])} | {fmt(horizon['ridge']['direction_accuracy_ci95'][0])} to {fmt(horizon['ridge']['direction_accuracy_ci95'][1])} | {fmt(horizon['ridge']['rank_ic'])} | {fmt(horizon['ridge']['directional_edge_after_cost_pct'])}% |",
                    "",
                ]
            )
        if horizon["gate_reasons"]:
            lines.extend(["Blockers:", ""] + [f"- {reason}" for reason in horizon["gate_reasons"]] + [""])
    lines.extend(
        [
            "## Evidence boundary",
            "",
            "Metrics are out-of-sample within purged expanding windows, but the directional edge value is a per-signal proxy with overlapping horizons. It is not a portfolio backtest, Sharpe ratio, or investable performance claim.",
            "",
        ]
    )
    return "\n".join(lines)


def load_config(path: Path) -> Config:
    return Config.from_dict(json.loads(path.read_text(encoding="utf-8")))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", default="research/config.json")
    parser.add_argument("--out", default="research/out/latest")
    args = parser.parse_args()

    snapshot = json.loads(Path(args.input).read_text(encoding="utf-8"))
    report = run(snapshot, load_config(Path(args.config)))
    output_dir = Path(args.out)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (output_dir / "report.md").write_text(render_markdown(report), encoding="utf-8")
    print(render_markdown(report))
    print(f"Reports written to {output_dir}")


if __name__ == "__main__":
    main()
