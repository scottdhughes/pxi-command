#!/usr/bin/env python3
"""Research-only, leakage-resistant Gaussian HMM challenger for PXI.

This module is intentionally isolated from production. It compares a small,
transparent regime-conditioned portfolio rule with three frozen benchmarks.
It never writes to Cloudflare, D1, KV, production routes, or model weights.

Important evidence boundary:

* HMM state-count selection is repeated inside each expanding training window.
* Test-time state probabilities use the forward filter only. Backward-smoothed
  or Viterbi states are never used for an out-of-sample decision.
* A close-to-close observation through date t executes at close t+1 and may
  affect only the close-t+1 to close-t+2 portfolio return.
* Reinforcement learning is not implemented. With action-independent market
  transitions, the paper's tabular Bellman problem reduces to a state-wise
  immediate-reward lookup; this challenger makes that lookup explicit.
* Retrospective output is always governance ``NO_GO``. Passing the statistical
  screen is not production authorization.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import os
import re
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import NormalDist
from typing import Any

import numpy as np


SCHEMA_VERSION = "pxi-hmm-market-snapshot/v1"
REPORT_VERSION = "pxi-hmm-challenger-report/v1"
POINT_IN_TIME_STORAGE_CONTRACT = "append-only-market-research-snapshots/v1"
ASSET_SYMBOLS = ("SPY", "TLT", "GLD")
INDICATOR_SYMBOL = "^VIX"
ALL_SYMBOLS = ASSET_SYMBOLS + (INDICATOR_SYMBOL,)
PXI_REGIMES = ("RISK_ON", "TRANSITION", "RISK_OFF")
HISTORY_ORIGINS = ("legacy_unclassified", "live_recorded", "retrospective_reconstruction")

CANONICAL_CONFIG_DEFAULTS: dict[str, Any] = {
    "profile": "canonical_evidence",
    "state_counts": (2, 3),
    "min_train_rows": 1260,
    "test_rows": 252,
    "step_rows": 252,
    "hmm_restarts": 7,
    "hmm_max_iterations": 200,
    "hmm_tolerance": 1e-6,
    "variance_floor": 1e-4,
    "random_seed": 20260822,
    "policy_shrinkage_rows": 20.0,
    "minimum_state_effective_rows": 30.0,
    "minimum_state_fraction": 0.02,
    "minimum_eligible_restarts": 3,
    "minimum_restart_state_agreement": 0.80,
    "cost_scenarios_bps": (10.0, 25.0),
    "paired_hac_lag": 5,
    "paired_familywise_alpha": 0.05,
    "annualization_periods": 252,
    "minimum_input_rows": 3780,
    "minimum_input_years": 15.0,
    "minimum_oos_rows": 1260,
    "minimum_oos_years": 5.0,
    "minimum_walk_forward_slices": 5,
    "minimum_excess_sharpe": 0.10,
}

# The action set is frozen and deliberately tiny. It is a transparent lookup,
# not reinforcement learning and not an optimizer over arbitrary weights.
ACTION_ORDER = ("equal_weight", "spy", "defensive_tlt_gold")
ACTION_WEIGHTS = {
    "equal_weight": np.array([1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0]),
    "spy": np.array([1.0, 0.0, 0.0]),
    "defensive_tlt_gold": np.array([0.0, 0.5, 0.5]),
}
PXI_BENCHMARK_WEIGHTS = {
    "RISK_ON": ACTION_WEIGHTS["spy"],
    "TRANSITION": ACTION_WEIGHTS["equal_weight"],
    "RISK_OFF": ACTION_WEIGHTS["defensive_tlt_gold"],
}


@dataclass(frozen=True)
class ChallengerConfig:
    profile: str = "canonical_evidence"
    state_counts: tuple[int, ...] = (2, 3)
    min_train_rows: int = 1260
    test_rows: int = 252
    step_rows: int = 252
    hmm_restarts: int = 7
    hmm_max_iterations: int = 200
    hmm_tolerance: float = 1e-6
    variance_floor: float = 1e-4
    random_seed: int = 20260822
    policy_shrinkage_rows: float = 20.0
    minimum_state_effective_rows: float = 30.0
    minimum_state_fraction: float = 0.02
    minimum_eligible_restarts: int = 3
    minimum_restart_state_agreement: float = 0.80
    cost_scenarios_bps: tuple[float, ...] = (10.0, 25.0)
    paired_hac_lag: int = 5
    paired_familywise_alpha: float = 0.05
    annualization_periods: int = 252
    minimum_input_rows: int = 3780
    minimum_input_years: float = 15.0
    minimum_oos_rows: int = 1260
    minimum_oos_years: float = 5.0
    minimum_walk_forward_slices: int = 5
    minimum_excess_sharpe: float = 0.10

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> "ChallengerConfig":
        if not isinstance(values, dict):
            raise ValueError("challenger config must be an object")
        allowed_keys = {field.name for field in fields(cls)}
        unknown_keys = sorted(set(values) - allowed_keys)
        if unknown_keys:
            raise ValueError(f"unknown challenger config keys: {', '.join(unknown_keys)}")

        def integer(name: str, default: int) -> int:
            value = values.get(name, default)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{name} must be an integer")
            return value

        def number(name: str, default: float) -> float:
            value = values.get(name, default)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be numeric")
            return float(value)

        try:
            raw_state_counts = values.get("state_counts", (2, 3))
            raw_costs = values.get("cost_scenarios_bps", (10.0, 25.0))
            if not isinstance(raw_state_counts, (list, tuple)) or not isinstance(raw_costs, (list, tuple)):
                raise ValueError("state_counts and cost_scenarios_bps must be arrays")
            if any(isinstance(item, bool) or not isinstance(item, (int, float)) for item in raw_costs):
                raise ValueError("cost_scenarios_bps entries must be numeric")
            config = cls(
                profile=values.get("profile", "canonical_evidence"),
                state_counts=tuple(integer(f"state_counts[{index}]", item) for index, item in enumerate(raw_state_counts)),
                min_train_rows=integer("min_train_rows", 1260),
                test_rows=integer("test_rows", 252),
                step_rows=integer("step_rows", 252),
                hmm_restarts=integer("hmm_restarts", 7),
                hmm_max_iterations=integer("hmm_max_iterations", 200),
                hmm_tolerance=number("hmm_tolerance", 1e-6),
                variance_floor=number("variance_floor", 1e-4),
                random_seed=integer("random_seed", 20260822),
                policy_shrinkage_rows=number("policy_shrinkage_rows", 20.0),
                minimum_state_effective_rows=number("minimum_state_effective_rows", 30.0),
                minimum_state_fraction=number("minimum_state_fraction", 0.02),
                minimum_eligible_restarts=integer("minimum_eligible_restarts", 3),
                minimum_restart_state_agreement=number("minimum_restart_state_agreement", 0.80),
                cost_scenarios_bps=tuple(float(item) for item in raw_costs),
                paired_hac_lag=integer("paired_hac_lag", 5),
                paired_familywise_alpha=number("paired_familywise_alpha", 0.05),
                annualization_periods=integer("annualization_periods", 252),
                minimum_input_rows=integer("minimum_input_rows", 3780),
                minimum_input_years=number("minimum_input_years", 15.0),
                minimum_oos_rows=integer("minimum_oos_rows", 1260),
                minimum_oos_years=number("minimum_oos_years", 5.0),
                minimum_walk_forward_slices=integer("minimum_walk_forward_slices", 5),
                minimum_excess_sharpe=number("minimum_excess_sharpe", 0.10),
            )
        except (TypeError, ValueError, OverflowError) as error:
            raise ValueError(f"invalid challenger config: {error}") from error
        config.validate()
        return config

    def validate(self) -> None:
        if self.profile not in {"canonical_evidence", "exploratory_smoke"}:
            raise ValueError("profile must be canonical_evidence or exploratory_smoke")
        if self.state_counts != (2, 3):
            raise ValueError("state_counts must be exactly [2, 3] for the frozen challenger")
        if self.min_train_rows < 30:
            raise ValueError("min_train_rows must be at least 30")
        if self.test_rows <= 0 or self.step_rows != self.test_rows:
            raise ValueError("step_rows must equal test_rows so OOS dates are contiguous and non-duplicated")
        if self.hmm_restarts <= 0 or self.hmm_max_iterations <= 0:
            raise ValueError("HMM restarts and iterations must be positive")
        finite_values = {
            "hmm_tolerance": self.hmm_tolerance,
            "variance_floor": self.variance_floor,
            "policy_shrinkage_rows": self.policy_shrinkage_rows,
            "minimum_state_effective_rows": self.minimum_state_effective_rows,
            "minimum_state_fraction": self.minimum_state_fraction,
            "minimum_restart_state_agreement": self.minimum_restart_state_agreement,
            "paired_familywise_alpha": self.paired_familywise_alpha,
            "minimum_input_years": self.minimum_input_years,
            "minimum_oos_years": self.minimum_oos_years,
            "minimum_excess_sharpe": self.minimum_excess_sharpe,
        }
        if any(not math.isfinite(value) for value in finite_values.values()):
            raise ValueError("all floating-point configuration values must be finite")
        if self.hmm_tolerance <= 0 or self.variance_floor <= 0:
            raise ValueError("HMM numerical controls must be positive")
        if self.policy_shrinkage_rows < 0:
            raise ValueError("policy_shrinkage_rows cannot be negative")
        if self.minimum_state_effective_rows <= 0 or not 0 < self.minimum_state_fraction < 1:
            raise ValueError("state-degeneracy thresholds must be positive and minimum_state_fraction must be below 1")
        if self.minimum_eligible_restarts < 2 or self.minimum_eligible_restarts > self.hmm_restarts:
            raise ValueError("minimum_eligible_restarts must be between 2 and hmm_restarts")
        if not 0 < self.minimum_restart_state_agreement <= 1:
            raise ValueError("minimum_restart_state_agreement must be in (0, 1]")
        if any(not math.isfinite(value) or value < 0 for value in self.cost_scenarios_bps):
            raise ValueError("cost scenarios must be finite and non-negative")
        if self.cost_scenarios_bps != (10.0, 25.0):
            raise ValueError("cost_scenarios_bps must remain frozen at [10, 25]")
        if self.paired_hac_lag < 0 or not 0 < self.paired_familywise_alpha < 1:
            raise ValueError("paired HAC lag and familywise alpha are invalid")
        if self.annualization_periods != 252:
            raise ValueError("annualization_periods must equal the SPY session contract value 252")
        if (
            self.annualization_periods <= 0
            or self.minimum_input_rows <= 0
            or self.minimum_input_years <= 0
            or self.minimum_oos_rows <= 0
            or self.minimum_oos_years <= 0
            or self.minimum_walk_forward_slices <= 0
        ):
            raise ValueError("annualization and minimum evidence counts must be positive")
        if self.minimum_excess_sharpe < 0:
            raise ValueError("minimum_excess_sharpe cannot be negative")
        if self.profile == "canonical_evidence":
            actual = asdict(self)
            deviations = [
                f"{name}={actual[name]!r} (expected {expected!r})"
                for name, expected in CANONICAL_CONFIG_DEFAULTS.items()
                if actual[name] != expected
            ]
            if deviations:
                raise ValueError(
                    "canonical_evidence config must exactly match checked-in controls: " + "; ".join(deviations)
                )


@dataclass(frozen=True)
class GaussianHMM:
    state_count: int
    start_probability: np.ndarray
    transition: np.ndarray
    means: np.ndarray
    variances: np.ndarray
    log_likelihood: float
    iterations: int
    seed: int
    converged: bool
    likelihood_monotone: bool
    maximum_likelihood_drop: float
    state_effective_rows: np.ndarray


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _finite_positive(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(float(value)) and float(value) > 0


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def config_payload(config: ChallengerConfig) -> dict[str, Any]:
    payload = asdict(config)
    payload["state_counts"] = list(config.state_counts)
    payload["cost_scenarios_bps"] = list(config.cost_scenarios_bps)
    return payload


def implementation_provenance(build_revision: str | None = None) -> dict[str, Any]:
    source_path = Path(__file__).resolve()
    source_sha256 = hashlib.sha256(source_path.read_bytes()).hexdigest()
    if build_revision is not None and not isinstance(build_revision, str):
        raise ValueError("build_revision must be a string or null")
    normalized_revision = build_revision.strip() if isinstance(build_revision, str) else None
    normalized_revision = normalized_revision or None
    return {
        "source_file": source_path.name,
        "source_sha256": source_sha256,
        "source_hash_scope": "exact bytes of the executed hmm_challenger.py module",
        "build_revision": normalized_revision,
        "build_revision_status": "supplied" if normalized_revision else "absent_not_supplied",
        "build_revision_source": "explicit --build-revision argument or PXI_BUILD_REVISION environment value; the library never invokes git",
    }


def validate_snapshot(snapshot: dict[str, Any]) -> list[str]:
    """Validate the adjusted-total-return and provenance input contract."""

    errors: list[str] = []
    if not isinstance(snapshot, dict):
        return ["snapshot must be an object"]
    if snapshot.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must equal {SCHEMA_VERSION}")
    if not isinstance(snapshot.get("dataset_id"), str) or not snapshot["dataset_id"].strip():
        errors.append("dataset_id is required")
    generated_at: datetime | None = None
    try:
        generated_at = parse_datetime(snapshot["generated_at"])
    except (KeyError, TypeError, ValueError):
        errors.append("generated_at must be a valid timestamp")

    contract = snapshot.get("price_contract")
    if not isinstance(contract, dict):
        errors.append("price_contract must be an object")
    else:
        portfolio_assets = contract.get("portfolio_assets")
        if not isinstance(portfolio_assets, list) or tuple(portfolio_assets) != ASSET_SYMBOLS:
            errors.append(f"price_contract.portfolio_assets must equal {list(ASSET_SYMBOLS)} in order")
        if contract.get("regime_indicator") != INDICATOR_SYMBOL:
            errors.append(f"price_contract.regime_indicator must equal {INDICATOR_SYMBOL}")
        if contract.get("price_field") != "adjusted_close":
            errors.append("price_contract.price_field must equal adjusted_close")
        if contract.get("total_return_adjusted_assets") is not True:
            errors.append("portfolio asset inputs must be explicitly total-return adjusted")
        if not isinstance(contract.get("corporate_action_policy"), str) or not contract["corporate_action_policy"].strip():
            errors.append("price_contract.corporate_action_policy is required")

    point_in_time = snapshot.get("point_in_time_guarantee") is True
    if point_in_time and snapshot.get("storage_contract") != POINT_IN_TIME_STORAGE_CONTRACT:
        errors.append(f"point-in-time data requires storage_contract={POINT_IN_TIME_STORAGE_CONTRACT}")

    artifacts = snapshot.get("source_artifacts")
    if not isinstance(artifacts, dict) or not artifacts:
        errors.append("source_artifacts with immutable content hashes are required")
    else:
        hashes = [item.get("sha256") for item in artifacts.values() if isinstance(item, dict)]
        if len(hashes) != len(artifacts) or any(not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value) for value in hashes):
            errors.append("every source artifact requires a lowercase SHA-256 content hash")
        for artifact_id, item in artifacts.items():
            try:
                fetched_at = parse_datetime(item["fetched_at"])
                if point_in_time and generated_at is not None and fetched_at > generated_at:
                    errors.append(f"source artifact {artifact_id} was fetched after generated_at")
            except (KeyError, TypeError, ValueError):
                errors.append(f"source artifact {artifact_id} requires a valid fetched_at timestamp")

    cadence = snapshot.get("cadence_contract")
    if not isinstance(cadence, dict):
        errors.append("cadence_contract must be an object")
    else:
        if cadence.get("trading_session") != "SPY":
            errors.append("cadence_contract.trading_session must equal SPY")
        if not isinstance(cadence.get("calendar_id"), str) or not cadence["calendar_id"].strip():
            errors.append("cadence_contract.calendar_id is required")
        if cadence.get("annualization_periods") != 252:
            errors.append("cadence_contract.annualization_periods must equal 252")
        if cadence.get("consecutive_required") is not True:
            errors.append("cadence_contract.consecutive_required must be true")
        ordinal_source = cadence.get("session_ordinal_source")
        if not isinstance(ordinal_source, str) or not ordinal_source.strip():
            errors.append("cadence_contract.session_ordinal_source is required")
        elif isinstance(artifacts, dict) and ordinal_source not in artifacts:
            errors.append("cadence_contract.session_ordinal_source is not bound in source_artifacts")
        expected_dates = cadence.get("expected_decision_dates")
        if not isinstance(expected_dates, list) or not expected_dates:
            errors.append("cadence_contract.expected_decision_dates must be a non-empty array")
        else:
            parsed_expected = []
            for index, expected_date in enumerate(expected_dates):
                try:
                    parsed_expected.append(parse_date(expected_date))
                except (TypeError, ValueError):
                    errors.append(f"cadence expected date {index} is invalid")
            if len(parsed_expected) == len(expected_dates):
                if parsed_expected != sorted(parsed_expected) or len(set(parsed_expected)) != len(parsed_expected):
                    errors.append("cadence expected dates must be strictly increasing and unique")

    rows = snapshot.get("rows")
    if not isinstance(rows, list) or len(rows) < 3:
        errors.append("rows must contain at least three observations")
        return errors

    seen_dates: set[str] = set()
    seen_snapshot_ids: set[str] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            errors.append(f"row {index}: row must be an object")
            continue
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
        if available_at.date() != decision_date:
            errors.append(f"row {index}: combined availability timestamp must be in the decision-date session")
        if available_at > decision_end:
            errors.append(f"row {index}: data became available after decision_date")
        if generated_at is not None and point_in_time and available_at > generated_at:
            errors.append(f"row {index}: combined availability is after generated_at")

        session_ordinal = row.get("session_ordinal")
        if isinstance(session_ordinal, bool) or not isinstance(session_ordinal, int) or session_ordinal < 0:
            errors.append(f"row {index}: session_ordinal must be a non-negative integer")

        closes = row.get("adjusted_closes")
        if not isinstance(closes, dict) or set(closes) != set(ALL_SYMBOLS):
            errors.append(f"row {index}: adjusted_closes must contain exactly {list(ALL_SYMBOLS)}")
        elif any(not _finite_positive(closes[symbol]) for symbol in ALL_SYMBOLS):
            errors.append(f"row {index}: adjusted closes must be positive and finite")

        observation_dates = row.get("price_observation_dates")
        price_available_at = row.get("price_available_at")
        sources = row.get("price_sources")
        if not isinstance(observation_dates, dict) or set(observation_dates) != set(ALL_SYMBOLS):
            errors.append(f"row {index}: price observation-date provenance is incomplete")
        if not isinstance(sources, dict) or set(sources) != set(ALL_SYMBOLS):
            errors.append(f"row {index}: price source provenance is incomplete")
        elif any(not isinstance(value, str) or not value.strip() for value in sources.values()):
            errors.append(f"row {index}: price source provenance contains an empty value")
        elif isinstance(artifacts, dict):
            for symbol, artifact_id in sources.items():
                if artifact_id not in artifacts:
                    errors.append(f"row {index}: {symbol} source artifact {artifact_id!r} is not bound in source_artifacts")
        if not isinstance(price_available_at, dict) or set(price_available_at) != set(ALL_SYMBOLS):
            errors.append(f"row {index}: price first-availability provenance is incomplete")
        else:
            component_availability = []
            for symbol, first_available in price_available_at.items():
                try:
                    parsed_available = parse_datetime(first_available)
                    component_availability.append(parsed_available)
                    if parsed_available.date() != decision_date:
                        errors.append(f"row {index}: {symbol} first-availability timestamp must be in the decision-date session")
                    if parsed_available > decision_end:
                        errors.append(f"row {index}: {symbol} first became available after decision_date")
                except (TypeError, ValueError):
                    errors.append(f"row {index}: {symbol} has an invalid first-availability timestamp")
            if component_availability and available_at < max(component_availability):
                errors.append(f"row {index}: combined availability precedes a price component")
            if generated_at is not None and point_in_time and any(value > generated_at for value in component_availability):
                errors.append(f"row {index}: a price component is available after generated_at")
        if isinstance(observation_dates, dict):
            for symbol, observed_on in observation_dates.items():
                try:
                    if parse_date(observed_on) != decision_date:
                        errors.append(f"row {index}: {symbol} observation date must equal decision_date; stale or forward-filled prices are forbidden")
                except (TypeError, ValueError):
                    errors.append(f"row {index}: {symbol} has an invalid observation date")

        regime = row.get("pxi_regime")
        if regime not in PXI_REGIMES:
            errors.append(f"row {index}: pxi_regime must be one of {list(PXI_REGIMES)}")
        history_origin = row.get("history_origin")
        if history_origin not in HISTORY_ORIGINS:
            errors.append(f"row {index}: history_origin must be one of {list(HISTORY_ORIGINS)}")
        elif point_in_time and history_origin != "live_recorded":
            errors.append(f"row {index}: point-in-time PXI regime history must be live_recorded, got {history_origin}")
        try:
            if parse_date(row["pxi_regime_observation_date"]) != decision_date:
                errors.append(f"row {index}: PXI regime observation date must equal decision_date")
        except (KeyError, TypeError, ValueError):
            errors.append(f"row {index}: invalid PXI regime observation date")
        if not isinstance(row.get("pxi_regime_source"), str) or not row["pxi_regime_source"].strip():
            errors.append(f"row {index}: pxi_regime_source is required")
        elif isinstance(artifacts, dict) and row["pxi_regime_source"] not in artifacts:
            errors.append(f"row {index}: PXI regime source artifact is not bound in source_artifacts")
        try:
            regime_available_at = parse_datetime(row["pxi_regime_available_at"])
            if regime_available_at.date() != decision_date:
                errors.append(f"row {index}: PXI regime first-availability timestamp must be in the decision-date session")
            if regime_available_at > decision_end:
                errors.append(f"row {index}: PXI regime first became available after decision_date")
            if available_at < regime_available_at:
                errors.append(f"row {index}: combined availability precedes the PXI regime component")
            if generated_at is not None and point_in_time and regime_available_at > generated_at:
                errors.append(f"row {index}: PXI regime component is available after generated_at")
        except (KeyError, TypeError, ValueError):
            errors.append(f"row {index}: invalid PXI regime first-availability timestamp")

        snapshot_id = row.get("snapshot_id")
        if not isinstance(snapshot_id, str) or not snapshot_id.strip():
            errors.append(f"row {index}: snapshot_id is required")
        else:
            if snapshot_id in seen_snapshot_ids:
                errors.append(f"row {index}: duplicate snapshot_id {snapshot_id}")
            seen_snapshot_ids.add(snapshot_id)
        if point_in_time:
            if row.get("immutable_snapshot") is not True:
                errors.append(f"row {index}: point-in-time row must be immutable")

    return errors


def cadence_diagnostics(snapshot: dict[str, Any], rows: list[dict[str, Any]], config: ChallengerConfig) -> dict[str, Any]:
    cadence = snapshot["cadence_contract"]
    expected = list(cadence["expected_decision_dates"])
    actual = [row["decision_date"] for row in rows]
    expected_set = set(expected)
    actual_set = set(actual)
    missing = [value for value in expected if value not in actual_set]
    unexpected = [value for value in actual if value not in expected_set]
    if actual:
        first_actual, last_actual = actual[0], actual[-1]
        interior_missing = [value for value in missing if first_actual < value < last_actual]
    else:
        interior_missing = missing

    ordinal_gaps = []
    for previous, current in zip(rows, rows[1:]):
        difference = current["session_ordinal"] - previous["session_ordinal"]
        if difference != 1:
            ordinal_gaps.append(
                {
                    "previous_date": previous["decision_date"],
                    "current_date": current["decision_date"],
                    "previous_ordinal": previous["session_ordinal"],
                    "current_ordinal": current["session_ordinal"],
                    "ordinal_difference": difference,
                    "missing_sessions": max(0, difference - 1),
                }
            )

    blockers = []
    if interior_missing:
        blockers.append(f"{len(interior_missing)} expected interior trading sessions are missing")
    if ordinal_gaps:
        blockers.append(f"{len(ordinal_gaps)} non-consecutive session-ordinal transitions were detected")
    if unexpected:
        blockers.append(f"{len(unexpected)} decision dates are absent from the expected-session calendar")
    if cadence["annualization_periods"] != config.annualization_periods:
        blockers.append("cadence annualization_periods does not match the frozen research config")

    return {
        "metrics_valid": not blockers,
        "trading_session": cadence["trading_session"],
        "calendar_id": cadence["calendar_id"],
        "session_ordinal_source": cadence["session_ordinal_source"],
        "consecutive_required": cadence["consecutive_required"],
        "annualization_periods": cadence["annualization_periods"],
        "expected_session_count": len(expected),
        "actual_row_count": len(actual),
        "expected_session_coverage": len(expected_set & actual_set) / len(expected),
        "missing_expected_sessions": missing,
        "interior_missing_expected_sessions": interior_missing,
        "unexpected_decision_dates": unexpected,
        "ordinal_gaps": ordinal_gaps,
        "blockers": blockers,
        "evidence_boundary": "Session ordinals and expected dates are ingestion assertions bound to the declared source artifact hash.",
    }


def market_arrays(snapshot: dict[str, Any]) -> tuple[list[dict[str, Any]], np.ndarray, np.ndarray]:
    """Return sorted rows, HMM observations, and asset simple returns.

    Observation q ends on rows[q + 1]. The target executes at rows[q + 2], and
    its portfolio outcome is asset_returns[q + 2], ending on rows[q + 3]. This
    deliberately excludes the close-t to close-(t+1) interval.
    """

    rows = sorted(snapshot["rows"], key=lambda row: row["decision_date"])
    prices = np.array([[float(row["adjusted_closes"][symbol]) for symbol in ALL_SYMBOLS] for row in rows])
    asset_returns = (prices[1:, : len(ASSET_SYMBOLS)] / prices[:-1, : len(ASSET_SYMBOLS)]) - 1.0
    asset_log_returns_pct = np.log(prices[1:, : len(ASSET_SYMBOLS)] / prices[:-1, : len(ASSET_SYMBOLS)]) * 100.0
    vix_delta = prices[1:, -1] - prices[:-1, -1]
    observations = np.column_stack((asset_log_returns_pct, vix_delta))
    if not np.all(np.isfinite(observations)) or not np.all(np.isfinite(asset_returns)):
        raise ValueError("derived observations or returns are not finite")
    return rows, observations, asset_returns


def fit_standardizer(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    means = np.mean(values, axis=0)
    scales = np.std(values, axis=0)
    scales = np.where(scales > 1e-12, scales, 1.0)
    return means, scales


def apply_standardizer(values: np.ndarray, means: np.ndarray, scales: np.ndarray) -> np.ndarray:
    return (values - means) / scales


def _logsumexp(values: np.ndarray, axis: int | None = None) -> np.ndarray | float:
    maximum = np.max(values, axis=axis, keepdims=True)
    total = maximum + np.log(np.sum(np.exp(values - maximum), axis=axis, keepdims=True))
    squeezed = np.squeeze(total, axis=axis) if axis is not None else np.squeeze(total)
    return float(squeezed) if np.ndim(squeezed) == 0 else squeezed


def _emission_log_probability(values: np.ndarray, means: np.ndarray, variances: np.ndarray) -> np.ndarray:
    difference = values[:, None, :] - means[None, :, :]
    return -0.5 * np.sum(np.log(2.0 * math.pi * variances)[None, :, :] + (difference**2 / variances[None, :, :]), axis=2)


def _forward_backward(
    values: np.ndarray,
    start_probability: np.ndarray,
    transition: np.ndarray,
    means: np.ndarray,
    variances: np.ndarray,
) -> tuple[float, np.ndarray, np.ndarray]:
    emission = _emission_log_probability(values, means, variances)
    log_start = np.log(np.maximum(start_probability, 1e-300))
    log_transition = np.log(np.maximum(transition, 1e-300))
    count, states = emission.shape
    alpha = np.empty((count, states))
    alpha[0] = log_start + emission[0]
    for index in range(1, count):
        alpha[index] = emission[index] + _logsumexp(alpha[index - 1][:, None] + log_transition, axis=0)
    log_likelihood = float(_logsumexp(alpha[-1]))

    beta = np.zeros((count, states))
    for index in range(count - 2, -1, -1):
        beta[index] = _logsumexp(log_transition + emission[index + 1][None, :] + beta[index + 1][None, :], axis=1)

    log_gamma = alpha + beta - log_likelihood
    log_gamma -= _logsumexp(log_gamma, axis=1)[:, None]
    gamma = np.exp(log_gamma)
    xi_sum = np.zeros((states, states))
    for index in range(count - 1):
        log_xi = alpha[index][:, None] + log_transition + emission[index + 1][None, :] + beta[index + 1][None, :] - log_likelihood
        log_xi -= float(_logsumexp(log_xi))
        xi_sum += np.exp(log_xi)
    return log_likelihood, gamma, xi_sum


def _initial_parameters(values: np.ndarray, state_count: int, seed: int, variance_floor: float) -> tuple[np.ndarray, ...]:
    rng = np.random.default_rng(seed)
    sample_count, dimensions = values.shape
    centers = values[rng.choice(sample_count, size=state_count, replace=False)].copy()
    for _ in range(12):
        distance = np.sum((values[:, None, :] - centers[None, :, :]) ** 2, axis=2)
        assignments = np.argmin(distance, axis=1)
        for state in range(state_count):
            members = values[assignments == state]
            if len(members):
                centers[state] = np.mean(members, axis=0)
            else:
                nearest = np.min(distance, axis=1)
                centers[state] = values[int(np.argmax(nearest))]
    centers += rng.normal(0.0, 0.01, size=centers.shape)

    global_variance = np.maximum(np.var(values, axis=0), variance_floor)
    variances = np.tile(global_variance, (state_count, 1)) * rng.uniform(0.9, 1.1, size=(state_count, dimensions))
    transition = np.full((state_count, state_count), 0.08 / max(1, state_count - 1))
    np.fill_diagonal(transition, 0.92)
    transition *= rng.uniform(0.98, 1.02, size=transition.shape)
    transition /= np.sum(transition, axis=1, keepdims=True)
    start_probability = rng.dirichlet(np.ones(state_count))
    return start_probability, transition, centers, np.maximum(variances, variance_floor)


def fit_gaussian_hmm(
    values: np.ndarray,
    state_count: int,
    seed: int,
    max_iterations: int,
    tolerance: float,
    variance_floor: float,
) -> GaussianHMM:
    """Fit a diagonal Gaussian HMM by deterministic-seed EM."""

    if values.ndim != 2 or len(values) < max(10, state_count * 3):
        raise ValueError("insufficient observations for HMM fit")
    start, transition, means, variances = _initial_parameters(values, state_count, seed, variance_floor)
    log_likelihood, gamma, xi_sum = _forward_backward(values, start, transition, means, variances)
    iterations = 0
    converged = False
    likelihood_monotone = True
    maximum_likelihood_drop = 0.0
    for iterations in range(1, max_iterations + 1):
        start = gamma[0] / np.sum(gamma[0])
        transition_mass = np.sum(xi_sum, axis=1, keepdims=True)
        if np.any(transition_mass <= 1e-15):
            likelihood_monotone = False
            break
        transition = xi_sum / transition_mass
        state_mass = np.maximum(np.sum(gamma, axis=0), 1e-12)
        means = (gamma.T @ values) / state_mass[:, None]
        difference = values[:, None, :] - means[None, :, :]
        variances = np.sum(gamma[:, :, None] * difference**2, axis=0) / state_mass[:, None]
        variances = np.maximum(variances, variance_floor)
        next_likelihood, next_gamma, next_xi_sum = _forward_backward(values, start, transition, means, variances)
        improvement = next_likelihood - log_likelihood
        maximum_likelihood_drop = max(maximum_likelihood_drop, -improvement)
        material_drop = tolerance * 10.0 * (1.0 + abs(log_likelihood))
        if improvement < -material_drop:
            likelihood_monotone = False
            log_likelihood, gamma, xi_sum = next_likelihood, next_gamma, next_xi_sum
            break
        log_likelihood, gamma, xi_sum = next_likelihood, next_gamma, next_xi_sum
        if abs(improvement) <= tolerance * (1.0 + abs(log_likelihood)):
            converged = True
            break

    state_effective_rows = np.sum(gamma, axis=0)
    return GaussianHMM(
        state_count=state_count,
        start_probability=start,
        transition=transition,
        means=means,
        variances=variances,
        log_likelihood=log_likelihood,
        iterations=iterations,
        seed=seed,
        converged=converged,
        likelihood_monotone=likelihood_monotone,
        maximum_likelihood_drop=maximum_likelihood_drop,
        state_effective_rows=state_effective_rows,
    )


def parameter_count(state_count: int, dimensions: int) -> int:
    return (state_count - 1) + (state_count * (state_count - 1)) + (2 * state_count * dimensions)


def model_fingerprint(model: GaussianHMM, standardizer_mean: np.ndarray, standardizer_scale: np.ndarray) -> str:
    payload = {
        "state_count": model.state_count,
        "start": model.start_probability.tolist(),
        "transition": model.transition.tolist(),
        "means": model.means.tolist(),
        "variances": model.variances.tolist(),
        "standardizer_mean": standardizer_mean.tolist(),
        "standardizer_scale": standardizer_scale.tolist(),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def aligned_restart_stability(
    reference: GaussianHMM,
    eligible_models: list[GaussianHMM],
    values: np.ndarray,
    minimum_eligible_restarts: int,
    minimum_agreement: float,
) -> dict[str, Any]:
    reference_labels = np.argmax(filter_probabilities(reference, values), axis=1)
    agreements = []
    for model in eligible_models:
        labels = np.argmax(filter_probabilities(model, values), axis=1)
        best_agreement = 0.0
        for permutation in itertools.permutations(range(reference.state_count)):
            mapping = np.array(permutation, dtype=int)
            agreement = float(np.mean(mapping[labels] == reference_labels))
            best_agreement = max(best_agreement, agreement)
        agreements.append({"seed": model.seed, "aligned_state_agreement": best_agreement})
    minimum_observed = min((item["aligned_state_agreement"] for item in agreements), default=None)
    mean_observed = float(np.mean([item["aligned_state_agreement"] for item in agreements])) if agreements else None
    passed = (
        len(eligible_models) >= minimum_eligible_restarts
        and minimum_observed is not None
        and minimum_observed >= minimum_agreement
    )
    return {
        "method": "Brute-force label permutation maximizing agreement of forward-filtered training-state argmax paths.",
        "reference_seed": reference.seed,
        "eligible_restarts": len(eligible_models),
        "minimum_required_restarts": minimum_eligible_restarts,
        "minimum_aligned_state_agreement": minimum_observed,
        "mean_aligned_state_agreement": mean_observed,
        "required_minimum_agreement": minimum_agreement,
        "per_restart": agreements,
        "passed": passed,
    }


def select_hmm_model(values: np.ndarray, config: ChallengerConfig) -> tuple[GaussianHMM, list[dict[str, Any]]]:
    """Select two versus three states using training values only."""

    candidates: list[tuple[GaussianHMM, float]] = []
    reports: list[dict[str, Any]] = []
    for state_count in config.state_counts:
        fits = []
        for restart in range(config.hmm_restarts):
            seed = config.random_seed + (state_count * 1009) + restart
            fits.append(
                fit_gaussian_hmm(
                    values,
                    state_count,
                    seed,
                    config.hmm_max_iterations,
                    config.hmm_tolerance,
                    config.variance_floor,
                )
            )
        eligible = [
            model
            for model in fits
            if model.converged
            and model.likelihood_monotone
            and float(np.min(model.state_effective_rows)) >= config.minimum_state_effective_rows
            and float(np.min(model.state_effective_rows) / len(values)) >= config.minimum_state_fraction
        ]
        best = max(eligible or fits, key=lambda model: (model.log_likelihood, -model.seed))
        stability = aligned_restart_stability(
            best,
            eligible,
            values,
            config.minimum_eligible_restarts,
            config.minimum_restart_state_agreement,
        )
        parameters = parameter_count(state_count, values.shape[1])
        bic = (-2.0 * best.log_likelihood) + (parameters * math.log(len(values)))
        selectable = bool(eligible)
        if selectable:
            candidates.append((best, bic))
        reports.append(
            {
                "state_count": state_count,
                "bic": bic,
                "log_likelihood": best.log_likelihood,
                "parameter_count": parameters,
                "winning_restart_seed": best.seed,
                "iterations": best.iterations,
                "restarts": config.hmm_restarts,
                "converged": best.converged,
                "likelihood_monotone": best.likelihood_monotone,
                "maximum_likelihood_drop": best.maximum_likelihood_drop,
                "state_effective_rows": best.state_effective_rows.tolist(),
                "minimum_state_fraction": float(np.min(best.state_effective_rows) / len(values)),
                "eligible_restart_count": len(eligible),
                "rejected_restart_count": len(fits) - len(eligible),
                "selectable": selectable,
                "restart_stability": stability,
            }
        )
    if not candidates:
        raise ValueError("no converged, non-degenerate HMM candidate was available")
    selected, _ = min(candidates, key=lambda item: (item[1], item[0].state_count))
    return selected, reports


def filter_probabilities(model: GaussianHMM, values: np.ndarray) -> np.ndarray:
    """One-sided HMM filtering; output at t is invariant to observations after t."""

    emission = _emission_log_probability(values, model.means, model.variances)
    output = np.empty((len(values), model.state_count))
    previous: np.ndarray | None = None
    for index in range(len(values)):
        prior = model.start_probability if previous is None else previous @ model.transition
        log_posterior = np.log(np.maximum(prior, 1e-300)) + emission[index]
        log_posterior -= float(_logsumexp(log_posterior))
        previous = np.exp(log_posterior)
        output[index] = previous
    return output


def filtered_state_diagnostics(model: GaussianHMM, probabilities: np.ndarray, latest_date: str) -> dict[str, Any]:
    safe = np.maximum(probabilities, 1e-300)
    entropy = -np.sum(safe * np.log(safe), axis=1) / math.log(model.state_count)
    diagonal = np.diag(model.transition)
    durations = [None if value >= 1.0 - 1e-12 else float(1.0 / (1.0 - value)) for value in diagonal]
    return {
        "latest_date": latest_date,
        "latest_filtered_posterior": {str(index): float(value) for index, value in enumerate(probabilities[-1])},
        "latest_normalized_entropy": float(entropy[-1]),
        "mean_normalized_entropy": float(np.mean(entropy)),
        "maximum_normalized_entropy": float(np.max(entropy)),
        "mean_filtered_state_probability": {
            str(index): float(value) for index, value in enumerate(np.mean(probabilities, axis=0))
        },
        "transition_self_probabilities": {str(index): float(value) for index, value in enumerate(diagonal)},
        "mean_transition_persistence": float(np.mean(diagonal)),
        "implied_expected_duration_periods": {str(index): value for index, value in enumerate(durations)},
        "interpretation": "Entropy is normalized to [0,1]; higher values mean greater regime uncertainty.",
    }


def fit_state_policy(
    filtered_probabilities: np.ndarray,
    next_asset_returns: np.ndarray,
    shrinkage_rows: float,
) -> tuple[np.ndarray, list[dict[str, Any]]]:
    """Fit the frozen state/action lookup using forward-filtered training states."""

    if len(filtered_probabilities) != len(next_asset_returns):
        raise ValueError("state probabilities and policy rewards must align")
    action_returns = np.column_stack([next_asset_returns @ ACTION_WEIGHTS[name] for name in ACTION_ORDER])
    unconditional = np.mean(action_returns, axis=0)
    state_action_weights = np.empty((filtered_probabilities.shape[1], len(ASSET_SYMBOLS)))
    reports = []
    for state in range(filtered_probabilities.shape[1]):
        state_probability = filtered_probabilities[:, state]
        effective_rows = float(np.sum(state_probability))
        numerator = state_probability @ action_returns
        shrunken = (numerator + (shrinkage_rows * unconditional)) / (effective_rows + shrinkage_rows)
        action_index = int(np.argmax(shrunken))
        action = ACTION_ORDER[action_index]
        state_action_weights[state] = ACTION_WEIGHTS[action]
        reports.append(
            {
                "state": state,
                "effective_training_rows": effective_rows,
                "selected_action": action,
                "selected_weights": dict(zip(ASSET_SYMBOLS, ACTION_WEIGHTS[action].tolist(), strict=True)),
                "shrunken_expected_daily_returns": dict(zip(ACTION_ORDER, shrunken.tolist(), strict=True)),
            }
        )
    return state_action_weights, reports


def net_return_series(weights: np.ndarray, asset_returns: np.ndarray, cost_bps: float) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if len(weights) != len(asset_returns) or not len(weights):
        raise ValueError("weights and asset returns must be non-empty and aligned")
    gross = np.sum(weights * asset_returns, axis=1)
    turnover = np.empty(len(weights))
    turnover[0] = 1.0
    for index in range(1, len(weights)):
        grown_holdings = weights[index - 1] * (1.0 + asset_returns[index - 1])
        gross_value = float(np.sum(grown_holdings))
        if not math.isfinite(gross_value) or gross_value <= 0:
            raise ValueError("prior portfolio cannot be normalized after asset returns")
        drifted_weights = grown_holdings / gross_value
        turnover[index] = 0.5 * float(np.sum(np.abs(weights[index] - drifted_weights)))
    costs = turnover * (cost_bps / 10_000.0)
    net = ((1.0 - costs) * (1.0 + gross)) - 1.0
    return net, turnover, costs


def portfolio_metrics(weights: np.ndarray, asset_returns: np.ndarray, cost_bps: float, annualization: int) -> dict[str, Any]:
    net, turnover, costs = net_return_series(weights, asset_returns, cost_bps)
    wealth = np.cumprod(1.0 + net)
    peak = np.maximum.accumulate(np.concatenate(([1.0], wealth)))
    drawdowns = np.concatenate(([1.0], wealth)) / peak - 1.0
    sample_volatility = float(np.std(net, ddof=1)) if len(net) > 1 else 0.0
    volatility = sample_volatility * math.sqrt(annualization)
    sharpe = None if sample_volatility <= 1e-12 else float(np.mean(net) / sample_volatility * math.sqrt(annualization))
    annualized_return = float(wealth[-1] ** (annualization / len(net)) - 1.0) if wealth[-1] > 0 else None
    return {
        "observations": len(net),
        "cumulative_return": float(wealth[-1] - 1.0),
        "annualized_return": annualized_return,
        "annualized_volatility": volatility,
        "sharpe_zero_risk_free": sharpe,
        "maximum_drawdown": float(np.min(drawdowns)),
        "total_turnover": float(np.sum(turnover)),
        "average_daily_turnover": float(np.mean(turnover)),
        "transaction_cost_paid": float(np.sum(costs)),
        "cost_bps_per_one_way_turnover": cost_bps,
    }


def paired_hac_mean_lower_bound(
    differences: np.ndarray,
    lag: int,
    familywise_alpha: float,
    comparison_count: int,
) -> dict[str, Any]:
    """Newey-West/Bartlett lower bound for a paired daily mean difference."""

    count = len(differences)
    if count < 2:
        return {
            "observations": count,
            "mean_daily_uplift": None,
            "hac_standard_error": None,
            "lower_confidence_bound_daily": None,
            "valid": False,
        }
    effective_lag = min(lag, count - 1)
    mean = float(np.mean(differences))
    centered = differences - mean
    long_run_variance = float(centered @ centered / count)
    for offset in range(1, effective_lag + 1):
        covariance = float(centered[offset:] @ centered[:-offset] / count)
        bartlett_weight = 1.0 - (offset / (effective_lag + 1.0))
        long_run_variance += 2.0 * bartlett_weight * covariance
    # Roundoff can make a theoretically non-negative Bartlett estimate tiny
    # and negative. A material negative estimate is invalid and fails closed.
    numerical_tolerance = 1e-18
    valid = math.isfinite(long_run_variance) and long_run_variance >= -numerical_tolerance
    variance_of_mean = max(0.0, long_run_variance) / count if valid else math.nan
    standard_error = math.sqrt(variance_of_mean) if valid else None
    adjusted_alpha = familywise_alpha / comparison_count
    z_value = NormalDist().inv_cdf(1.0 - adjusted_alpha)
    lower = mean - (z_value * standard_error) if standard_error is not None else None
    return {
        "observations": count,
        "mean_daily_uplift": mean,
        "hac_lag": effective_lag,
        "hac_long_run_variance": long_run_variance,
        "hac_standard_error": standard_error,
        "one_sided_bonferroni_z": z_value,
        "familywise_alpha": familywise_alpha,
        "comparison_count": comparison_count,
        "lower_confidence_bound_daily": lower,
        "valid": valid,
    }


def run(
    snapshot: dict[str, Any],
    config: ChallengerConfig,
    build_revision: str | None = None,
) -> dict[str, Any]:
    config.validate()
    errors = validate_snapshot(snapshot)
    if errors:
        raise ValueError("; ".join(errors))
    snapshot_hash = canonical_sha256(snapshot)
    frozen_config = config_payload(config)
    frozen_config_hash = canonical_sha256(frozen_config)
    implementation = implementation_provenance(build_revision)
    rows = sorted(snapshot["rows"], key=lambda row: row["decision_date"])
    history_origin_counts = {origin: 0 for origin in HISTORY_ORIGINS}
    for row in rows:
        history_origin_counts[row["history_origin"]] += 1
    all_history_live_recorded = history_origin_counts["live_recorded"] == len(rows)
    cadence = cadence_diagnostics(snapshot, rows, config)
    metrics_valid = cadence["metrics_valid"]
    if metrics_valid:
        rows, observations, asset_returns = market_arrays(snapshot)
    else:
        observations = np.empty((0, len(ALL_SYMBOLS)))
        asset_returns = np.empty((0, len(ASSET_SYMBOLS)))
    # Observation q can make a decision only if the post-execution return
    # asset_returns[q + 2] exists.
    reward_capable_observations = len(observations) - 2
    if reward_capable_observations <= config.min_train_rows:
        training_ends: list[int] = []
    else:
        training_ends = list(
            range(
                config.min_train_rows,
                reward_capable_observations + 1,
                config.step_rows,
            )
        )

    strategy_names = ("hmm_challenger", "spy", "equal_weight", "pxi_regime_rule")
    weights_parts: dict[str, list[np.ndarray]] = {name: [] for name in strategy_names}
    returns_parts: list[np.ndarray] = []
    slice_reports: list[dict[str, Any]] = []
    oos_alignment: list[dict[str, str]] = []
    numerical_fit_issues: list[str] = []

    for slice_id, train_end in enumerate(training_ends, start=1):
        q_start = train_end - 1
        q_end = min(q_start + config.test_rows, reward_capable_observations)

        standardizer_mean, standardizer_scale = fit_standardizer(observations[:train_end])
        standardized_prefix = apply_standardizer(observations[:q_end], standardizer_mean, standardizer_scale)
        model, selection = select_hmm_model(standardized_prefix[:train_end], config)
        forward_probabilities = filter_probabilities(model, standardized_prefix)

        # The last training reward ends on the first OOS decision date. Policy
        # rewards therefore cannot include an OOS post-execution outcome.
        policy_probabilities = forward_probabilities[: train_end - 2]
        policy_rewards = asset_returns[2:train_end]
        state_weights, policy_report = fit_state_policy(policy_probabilities, policy_rewards, config.policy_shrinkage_rows)

        decision_probabilities = forward_probabilities[q_start:q_end]
        hmm_weights = decision_probabilities @ state_weights
        outcome_returns = asset_returns[q_start + 2 : q_end + 2]
        decision_rows = rows[q_start + 1 : q_end + 1]
        execution_rows = rows[q_start + 2 : q_end + 2]
        return_rows = rows[q_start + 3 : q_end + 3]
        pxi_weights = np.array([PXI_BENCHMARK_WEIGHTS[row["pxi_regime"]] for row in decision_rows])

        slice_weights = {
            "hmm_challenger": hmm_weights,
            "spy": np.tile(ACTION_WEIGHTS["spy"], (len(hmm_weights), 1)),
            "equal_weight": np.tile(ACTION_WEIGHTS["equal_weight"], (len(hmm_weights), 1)),
            "pxi_regime_rule": pxi_weights,
        }
        for name in strategy_names:
            weights_parts[name].append(slice_weights[name])
        returns_parts.append(outcome_returns)
        oos_alignment.extend(
            {
                "decision_date": decision["decision_date"],
                "execution_date": execution["decision_date"],
                "return_date": outcome["decision_date"],
            }
            for decision, execution, outcome in zip(decision_rows, execution_rows, return_rows, strict=True)
        )

        for candidate in selection:
            if not candidate["selectable"]:
                numerical_fit_issues.append(
                    f"slice {slice_id} state-count {candidate['state_count']} had no converged non-degenerate restart"
                )
            elif candidate["rejected_restart_count"]:
                numerical_fit_issues.append(
                    f"slice {slice_id} state-count {candidate['state_count']} rejected {candidate['rejected_restart_count']} of {candidate['restarts']} restarts for convergence, likelihood, or state-mass failure"
                )
            if not candidate["restart_stability"]["passed"]:
                numerical_fit_issues.append(
                    f"slice {slice_id} state-count {candidate['state_count']} failed aligned cross-restart state stability"
                )

        diagnostics = filtered_state_diagnostics(model, decision_probabilities, decision_rows[-1]["decision_date"])

        slice_reports.append(
            {
                "slice_id": slice_id,
                "train_start": rows[1]["decision_date"],
                "train_end": rows[train_end]["decision_date"],
                "training_observations": train_end,
                "model_selection_scope": "training observations only",
                "bic_candidates": selection,
                "selected_state_count": model.state_count,
                "model_fingerprint": model_fingerprint(model, standardizer_mean, standardizer_scale),
                "policy_fit_scope": "forward-filtered training probabilities and rewards ending no later than the first decision date",
                "latest_policy_reward_date": rows[train_end]["decision_date"],
                "state_policy": policy_report,
                "test_decision_start": decision_rows[0]["decision_date"],
                "test_decision_end": decision_rows[-1]["decision_date"],
                "first_execution_date": execution_rows[0]["decision_date"],
                "last_execution_date": execution_rows[-1]["decision_date"],
                "first_return_date": return_rows[0]["decision_date"],
                "last_return_date": return_rows[-1]["decision_date"],
                "test_rows": len(decision_rows),
                "partial_final_test_slice": len(decision_rows) < config.test_rows,
                "oos_inference": "one-sided forward filter only",
                "decision_to_execution_periods": 1,
                "decision_to_return_end_periods": 2,
                "filtered_state_diagnostics": diagnostics,
            }
        )

    combined_weights: dict[str, np.ndarray] = {}
    if returns_parts:
        combined_returns = np.concatenate(returns_parts)
        combined_weights = {name: np.concatenate(parts) for name, parts in weights_parts.items()}
        results = {
            f"{cost:g}_bps": {
                name: portfolio_metrics(combined_weights[name], combined_returns, cost, config.annualization_periods)
                for name in strategy_names
            }
            for cost in config.cost_scenarios_bps
        }
    else:
        combined_returns = np.empty((0, len(ASSET_SYMBOLS)))
        results = {f"{cost:g}_bps": {} for cost in config.cost_scenarios_bps}

    paired_uncertainty: dict[str, Any] = {}
    if len(combined_returns):
        high_cost_net = {
            name: net_return_series(combined_weights[name], combined_returns, 25.0)[0]
            for name in strategy_names
        }
        comparison_names = strategy_names[1:]
        paired_uncertainty = {
            benchmark: paired_hac_mean_lower_bound(
                high_cost_net["hmm_challenger"] - high_cost_net[benchmark],
                config.paired_hac_lag,
                config.paired_familywise_alpha,
                len(comparison_names),
            )
            for benchmark in comparison_names
        }

    screen_reasons = []
    if not metrics_valid:
        screen_reasons.extend([f"cadence invalid: {reason}" for reason in cadence["blockers"]])
        screen_reasons.append("all portfolio, annualized, Sharpe, and HAC metrics were suppressed")
    if config.profile == "exploratory_smoke":
        screen_reasons.append("exploratory_smoke config is wiring-only and can never yield an evidence PASS")
    if snapshot.get("point_in_time_guarantee") is not True:
        screen_reasons.append("input is a current mutable reconstruction without point-in-time guarantees")
    retrospective_count = history_origin_counts["retrospective_reconstruction"]
    if retrospective_count:
        screen_reasons.append(
            f"PXI history contains {retrospective_count} retrospective_reconstruction regime rows; retrospective reconstructions cannot support the evidence screen"
        )
    legacy_count = history_origin_counts["legacy_unclassified"]
    if legacy_count:
        screen_reasons.append(
            f"PXI history contains {legacy_count} legacy_unclassified regime rows; unclassified historical provenance cannot support the evidence screen"
        )
    input_years = (parse_date(rows[-1]["decision_date"]) - parse_date(rows[0]["decision_date"])).days / 365.2425
    if len(rows) < config.minimum_input_rows:
        screen_reasons.append(f"input rows {len(rows)} below minimum {config.minimum_input_rows}")
    if input_years < config.minimum_input_years:
        screen_reasons.append(f"input span {input_years:.3f} years below minimum {config.minimum_input_years:.3f}")
    if len(combined_returns) < config.minimum_oos_rows:
        screen_reasons.append(f"OOS rows {len(combined_returns)} below minimum {config.minimum_oos_rows}")
    if len(slice_reports) < config.minimum_walk_forward_slices:
        screen_reasons.append(f"walk-forward slices {len(slice_reports)} below minimum {config.minimum_walk_forward_slices}")
    screen_reasons.extend(numerical_fit_issues)

    high_cost_results = results.get("25_bps", {})
    if high_cost_results:
        candidate_sharpe = high_cost_results["hmm_challenger"]["sharpe_zero_risk_free"]
        benchmark_sharpes = [high_cost_results[name]["sharpe_zero_risk_free"] for name in strategy_names[1:]]
        usable_benchmarks = [value for value in benchmark_sharpes if value is not None]
        if candidate_sharpe is None or not usable_benchmarks or candidate_sharpe < max(usable_benchmarks) + config.minimum_excess_sharpe:
            screen_reasons.append(f"25 bps Sharpe did not exceed every frozen benchmark by {config.minimum_excess_sharpe:.2f}")
    else:
        screen_reasons.append("no valid OOS portfolio results")
    for benchmark in strategy_names[1:]:
        uncertainty = paired_uncertainty.get(benchmark)
        lower = uncertainty.get("lower_confidence_bound_daily") if uncertainty else None
        if uncertainty is None or not uncertainty.get("valid") or lower is None or lower <= 0:
            screen_reasons.append(
                f"25 bps paired HAC lower confidence bound versus {benchmark} did not exceed zero"
            )

    selected_counts = {str(count): sum(item["selected_state_count"] == count for item in slice_reports) for count in config.state_counts}
    evaluated_through = oos_alignment[-1]["return_date"] if oos_alignment else None
    oos_years = (
        (parse_date(oos_alignment[-1]["return_date"]) - parse_date(oos_alignment[0]["return_date"])).days / 365.2425
        if len(oos_alignment) >= 2
        else 0.0
    )
    if oos_years < config.minimum_oos_years:
        screen_reasons.append(f"OOS span {oos_years:.3f} years below minimum {config.minimum_oos_years:.3f}")
    unused_tail_rows = (
        sum(row["decision_date"] > evaluated_through for row in rows)
        if evaluated_through is not None
        else len(rows)
    )
    return {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "implementation_provenance": implementation,
        "dataset": {
            "dataset_id": snapshot.get("dataset_id"),
            "rows": len(rows),
            "date_range": [rows[0]["decision_date"], rows[-1]["decision_date"]],
            "point_in_time_guarantee": snapshot.get("point_in_time_guarantee") is True,
            "point_in_time_label": "ingestion assertion only; contract validation binds source-artifact ids to declared hashes but does not independently prove provider historical availability",
            "snapshot_sha256": snapshot_hash,
            "price_field": "adjusted_close",
            "total_return_adjusted_assets": True,
            "portfolio_assets": list(ASSET_SYMBOLS),
            "regime_indicator": INDICATOR_SYMBOL,
            "input_span_years": input_years,
        },
        "pxi_history_provenance": {
            "counts": history_origin_counts,
            "all_rows_live_recorded": all_history_live_recorded,
            "point_in_time_requirement": "Every PXI regime row must be live_recorded when point_in_time_guarantee=true.",
            "legacy_default": "An exporter reading an older API response without history_origin must label the row legacy_unclassified.",
        },
        "methodology": {
            "emissions": ["SPY_log_return_pct", "TLT_log_return_pct", "GLD_log_return_pct", "VIX_level_change"],
            "state_selection": "BIC comparison of 2 and 3 states, repeated independently inside every training slice",
            "hmm_fit": "Diagonal-Gaussian maximum-likelihood EM with a variance floor; no transition pseudocount is used in the BIC fit. Nonconverged, materially decreasing, or degenerate restarts are not selectable.",
            "test_inference": "forward filter only; no backward smoothing and no Viterbi path",
            "execution_timing": "observation through close t; target executes at close t+1; evaluated return is close t+1 to close t+2",
            "decision_to_execution_periods": 1,
            "decision_to_return_end_periods": 2,
            "action_set": {name: dict(zip(ASSET_SYMBOLS, ACTION_WEIGHTS[name].tolist(), strict=True)) for name in ACTION_ORDER},
            "pxi_regime_rule": {name: dict(zip(ASSET_SYMBOLS, weights.tolist(), strict=True)) for name, weights in PXI_BENCHMARK_WEIGHTS.items()},
            "reinforcement_learning": {
                "status": "REJECTED_NON_DEFAULT",
                "implemented": False,
                "reason": "With action-independent market transitions, tabular RL collapses to a state-wise immediate-reward lookup; the lookup is explicit here.",
            },
            "sharpe_convention": "annualized arithmetic mean divided by sample volatility; zero risk-free rate",
            "turnover_convention": "initial allocation costs 100% turnover; later turnover is one-half L1 distance from prior post-return drifted holdings; holdings carry continuously across refit boundaries",
            "paired_uncertainty": "Newey-West/Bartlett HAC on paired 25 bps net daily uplift with a one-sided Bonferroni familywise 95% lower bound across three frozen benchmarks",
            "cross_restart_stability": "For each K, restart state labels are brute-force aligned to the selected fit and forward-filtered training path agreement must meet the frozen threshold.",
        },
        "config": frozen_config,
        "config_sha256": frozen_config_hash,
        "walk_forward": {
            "slices": slice_reports,
            "slice_count": len(slice_reports),
            "oos_observations": len(combined_returns),
            "oos_span_years": oos_years,
            "selected_state_counts": selected_counts,
            "alignment": {
                "decision_to_execution_periods": 1,
                "decision_to_return_end_periods": 2,
                "first": oos_alignment[0] if oos_alignment else None,
                "last": oos_alignment[-1] if oos_alignment else None,
            },
            "latest_evaluated_filtered_state": slice_reports[-1]["filtered_state_diagnostics"] if slice_reports else None,
            "evaluated_through_return_date": evaluated_through,
            "input_last_date": rows[-1]["decision_date"],
            "unused_tail_rows_after_evaluated_return": unused_tail_rows,
            "cadence": cadence,
            "metrics_valid": metrics_valid,
        },
        "results": results,
        "paired_uncertainty_25_bps": {
            "comparisons": paired_uncertainty,
            "convention": "Paired candidate-minus-benchmark net daily returns; Newey-West/Bartlett HAC with frozen lag; one-sided asymptotic-normal lower bounds with Bonferroni familywise alpha across the three frozen benchmarks; lower bounds are not annualized.",
            "finite_sample_guarantee": False,
            "approximation": "asymptotic normal",
            "gate": "Every valid lower confidence bound must be strictly greater than zero.",
        },
        "retrospective_screen": {
            "status": "PASS" if not screen_reasons else "NO_GO",
            "metrics_valid": metrics_valid,
            "reasons": screen_reasons,
            "meaning": "A PASS is only permission to continue research; it is not an allocation recommendation.",
        },
        "governance": {
            "status": "NO_GO",
            "actionability": "NONE",
            "production_effect": "none; this module has no production imports, routes, storage writes, or model activation path",
            "reasons": [
                "retrospective challenger evidence cannot authorize production",
                "promotion requires an independently reviewed prospective shadow ledger and an explicit deployment decision",
            ] + (["retrospective robustness screen did not pass"] if screen_reasons else []),
        },
    }


def _fmt_percent(value: float | None) -> str:
    return "null" if value is None else f"{value * 100:.2f}%"


def render_markdown(report: dict[str, Any]) -> str:
    build_revision = report["implementation_provenance"]["build_revision"]
    build_revision_display = build_revision if build_revision is not None else "null"
    lines = [
        "# PXI HMM Challenger",
        "",
        f"**Governance: {report['governance']['status']} - actionability {report['governance']['actionability']}**",
        "",
        f"- Dataset: `{report['dataset']['dataset_id']}`",
        f"- Dataset SHA-256: `{report['dataset']['snapshot_sha256']}`",
        f"- Config SHA-256: `{report['config_sha256']}`",
        f"- Implementation SHA-256: `{report['implementation_provenance']['source_sha256']}`",
        f"- Build revision: `{build_revision_display}` ({report['implementation_provenance']['build_revision_status']})",
        f"- Point-in-time guarantee: `{str(report['dataset']['point_in_time_guarantee']).lower()}`",
        f"- PXI history provenance counts: `{json.dumps(report['pxi_history_provenance']['counts'], sort_keys=True)}`",
        f"- All PXI history live-recorded: `{str(report['pxi_history_provenance']['all_rows_live_recorded']).lower()}`",
        f"- Config profile: `{report['config']['profile']}`",
        f"- Metrics valid: `{str(report['walk_forward']['metrics_valid']).lower()}`",
        f"- Expected-session coverage: {report['walk_forward']['cadence']['expected_session_coverage']:.3f}",
        f"- Interior missing sessions: {len(report['walk_forward']['cadence']['interior_missing_expected_sessions'])}",
        f"- Walk-forward slices: {report['walk_forward']['slice_count']}",
        f"- OOS observations: {report['walk_forward']['oos_observations']}",
        f"- Evaluated through return date: `{report['walk_forward']['evaluated_through_return_date']}`",
        f"- Unused input tail rows: {report['walk_forward']['unused_tail_rows_after_evaluated_return']}",
        f"- Retrospective screen: `{report['retrospective_screen']['status']}`",
        "",
        "## Results",
        "",
    ]
    for scenario, strategies in report["results"].items():
        lines.extend([f"### {scenario.replace('_', ' ')}", ""])
        if not strategies:
            message = (
                "Metrics suppressed because the session cadence is invalid."
                if not report["walk_forward"]["metrics_valid"]
                else "No valid walk-forward slices."
            )
            lines.extend([message, ""])
            continue
        lines.extend(
            [
                "| Strategy | CAGR | Volatility | Sharpe | Max drawdown | Turnover |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for name, metrics in strategies.items():
            sharpe = "null" if metrics["sharpe_zero_risk_free"] is None else f"{metrics['sharpe_zero_risk_free']:.3f}"
            lines.append(
                f"| {name} | {_fmt_percent(metrics['annualized_return'])} | {_fmt_percent(metrics['annualized_volatility'])} | {sharpe} | {_fmt_percent(metrics['maximum_drawdown'])} | {metrics['total_turnover']:.2f} |"
            )
        lines.append("")
    comparisons = report["paired_uncertainty_25_bps"]["comparisons"]
    lines.extend(["## Paired uncertainty at 25 bps", "", "Asymptotic normal approximation; no finite-sample guarantee.", ""])
    if comparisons:
        lines.extend(
            [
                "| Benchmark | Mean daily uplift | Familywise lower bound | HAC lag | Valid |",
                "| --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for benchmark, item in comparisons.items():
            mean = item["mean_daily_uplift"]
            lower = item["lower_confidence_bound_daily"]
            lines.append(
                f"| {benchmark} | {_fmt_percent(mean)} | {_fmt_percent(lower)} | {item['hac_lag']} | `{str(item['valid']).lower()}` |"
            )
        lines.append("")
    else:
        message = (
            "HAC metrics suppressed because the session cadence is invalid."
            if not report["walk_forward"]["metrics_valid"]
            else "No valid paired OOS comparisons."
        )
        lines.extend([message, ""])
    latest = report["walk_forward"]["latest_evaluated_filtered_state"]
    if latest:
        posterior = ", ".join(f"state {state}: {value:.3f}" for state, value in latest["latest_filtered_posterior"].items())
        lines.extend(
            [
                "## Latest evaluated filtered state",
                "",
                f"- Decision date: `{latest['latest_date']}`",
                f"- Posterior: {posterior}",
                f"- Normalized entropy: {latest['latest_normalized_entropy']:.3f}",
                f"- Mean transition persistence: {latest['mean_transition_persistence']:.3f}",
                "",
            ]
        )
    if report["retrospective_screen"]["reasons"]:
        lines.extend(["## Screen blockers", ""] + [f"- {reason}" for reason in report["retrospective_screen"]["reasons"]] + [""])
    evidence_sentence = (
        "Session cadence failed, so no HMM fit or performance, annualized, Sharpe, or HAC metric was computed."
        if not report["walk_forward"]["metrics_valid"]
        else "States were selected inside each expanding training window and OOS decisions use one-sided filtering only."
    )
    lines.extend(
        [
            "## Evidence boundary",
            "",
            f"{evidence_sentence} This is a retrospective challenger, not a trading recommendation. Governance remains NO_GO even if the retrospective screen passes.",
            "",
        ]
    )
    return "\n".join(lines)


def load_config(path: Path) -> ChallengerConfig:
    return ChallengerConfig.from_dict(json.loads(path.read_text(encoding="utf-8")))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", default="research/hmm_config.json")
    parser.add_argument("--out", default="research/out/hmm_challenger")
    parser.add_argument("--build-revision", default=os.environ.get("PXI_BUILD_REVISION"))
    args = parser.parse_args()

    snapshot = json.loads(Path(args.input).read_text(encoding="utf-8"))
    report = run(snapshot, load_config(Path(args.config)), build_revision=args.build_revision)
    output_dir = Path(args.out)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "report.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    (output_dir / "report.md").write_text(render_markdown(report), encoding="utf-8")
    print(render_markdown(report))
    print(f"Reports written to {output_dir}")


if __name__ == "__main__":
    main()
