from __future__ import annotations

import copy
import hashlib
import json
import math
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hmm_challenger import (
    ChallengerConfig,
    apply_standardizer,
    filter_probabilities,
    fit_standardizer,
    implementation_provenance,
    load_config,
    market_arrays,
    net_return_series,
    run,
    select_hmm_model,
    validate_snapshot,
)
from export_hmm_public_snapshot import history_origin_for_row


def synthetic_snapshot(rows: int = 300, point_in_time: bool = True) -> dict:
    rng = np.random.default_rng(112358)
    transition = np.array([[0.96, 0.04], [0.10, 0.90]])
    state = 0
    prices = np.array([100.0, 100.0, 100.0])
    vix = 16.0
    output = []
    start = date(2024, 1, 2)
    for index in range(rows):
        if index:
            state = int(rng.choice(2, p=transition[state]))
            if state == 0:
                daily = np.array([0.0008, 0.0001, 0.0002]) + rng.normal(0, [0.006, 0.004, 0.005])
                vix = max(9.0, 0.96 * vix + 0.64 + rng.normal(0, 0.45))
            else:
                daily = np.array([-0.0012, 0.0005, 0.0007]) + rng.normal(0, [0.018, 0.009, 0.010])
                vix = max(12.0, 0.91 * vix + 2.8 + rng.normal(0, 1.5))
            prices *= 1.0 + daily
        observed_on = (start + timedelta(days=index)).isoformat()
        regime = "RISK_ON" if state == 0 else "RISK_OFF"
        output.append(
            {
                "snapshot_id": f"synthetic-{index}",
                "decision_date": observed_on,
                "session_ordinal": index,
                "available_at": f"{observed_on}T23:00:00Z",
                "immutable_snapshot": point_in_time,
                "adjusted_closes": {
                    "SPY": float(prices[0]),
                    "TLT": float(prices[1]),
                    "GLD": float(prices[2]),
                    "^VIX": float(vix),
                },
                "price_observation_dates": {symbol: observed_on for symbol in ("SPY", "TLT", "GLD", "^VIX")},
                "price_available_at": {symbol: f"{observed_on}T23:00:00Z" for symbol in ("SPY", "TLT", "GLD", "^VIX")},
                "price_sources": {symbol: "synthetic" for symbol in ("SPY", "TLT", "GLD", "^VIX")},
                "pxi_regime": regime,
                "history_origin": "live_recorded",
                "pxi_regime_observation_date": observed_on,
                "pxi_regime_available_at": f"{observed_on}T23:00:00Z",
                "pxi_regime_source": "synthetic",
            }
        )
    digest = hashlib.sha256(b"synthetic-hmm-fixture-v1").hexdigest()
    return {
        "schema_version": "pxi-hmm-market-snapshot/v1",
        "dataset_id": "synthetic-hmm-v1",
        "generated_at": "2026-08-22T00:00:00Z",
        "point_in_time_guarantee": point_in_time,
        "storage_contract": "append-only-market-research-snapshots/v1" if point_in_time else "mutable-test/v1",
        "price_contract": {
            "portfolio_assets": ["SPY", "TLT", "GLD"],
            "regime_indicator": "^VIX",
            "price_field": "adjusted_close",
            "total_return_adjusted_assets": True,
            "corporate_action_policy": "Synthetic series already represents total returns.",
            "currency": "USD",
        },
        "cadence_contract": {
            "trading_session": "SPY",
            "calendar_id": "synthetic-consecutive-sessions/v1",
            "annualization_periods": 252,
            "session_ordinal_source": "synthetic",
            "consecutive_required": True,
            "expected_decision_dates": [row["decision_date"] for row in output],
        },
        "source_artifacts": {
            "synthetic": {
                "sha256": digest,
                "fetched_at": "2026-08-22T00:00:00Z",
            }
        },
        "rows": output,
    }


def test_config(**overrides: object) -> ChallengerConfig:
    values = {
        "profile": "exploratory_smoke",
        "state_counts": [2, 3],
        "min_train_rows": 80,
        "test_rows": 30,
        "step_rows": 30,
        "hmm_restarts": 2,
        "hmm_max_iterations": 35,
        "hmm_tolerance": 1e-5,
        "variance_floor": 1e-4,
        "random_seed": 20260822,
        "policy_shrinkage_rows": 10,
        "minimum_state_effective_rows": 5,
        "minimum_state_fraction": 0.01,
        "minimum_eligible_restarts": 2,
        "minimum_restart_state_agreement": 0.6,
        "cost_scenarios_bps": [10, 25],
        "paired_hac_lag": 5,
        "paired_familywise_alpha": 0.05,
        "annualization_periods": 252,
        "minimum_input_rows": 90,
        "minimum_input_years": 0.2,
        "minimum_oos_rows": 90,
        "minimum_oos_years": 0.2,
        "minimum_walk_forward_slices": 3,
        "minimum_excess_sharpe": 0.1,
    }
    values.update(overrides)
    return ChallengerConfig.from_dict(values)


class HMMChallengerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.snapshot = synthetic_snapshot()
        cls.config = test_config()
        cls.report = run(cls.snapshot, cls.config)

    def test_contract_rejects_non_adjusted_or_incomplete_provenance(self) -> None:
        snapshot = synthetic_snapshot(rows=20)
        snapshot["price_contract"]["total_return_adjusted_assets"] = False
        snapshot["rows"][0]["price_sources"].pop("GLD")
        errors = validate_snapshot(snapshot)
        self.assertTrue(any("total-return adjusted" in error for error in errors))
        self.assertTrue(any("source provenance" in error for error in errors))

    def test_contract_rejects_stale_prices_and_malformed_rows(self) -> None:
        snapshot = synthetic_snapshot(rows=20)
        snapshot["rows"][3]["price_observation_dates"]["TLT"] = snapshot["rows"][2]["decision_date"]
        snapshot["rows"][4]["price_available_at"]["GLD"] = f"{snapshot['rows'][3]['decision_date']}T23:00:00Z"
        snapshot["rows"].append(None)
        errors = validate_snapshot(snapshot)
        self.assertTrue(any("stale or forward-filled" in error for error in errors))
        self.assertTrue(any("first-availability timestamp must be in" in error for error in errors))
        self.assertTrue(any("row must be an object" in error for error in errors))

        malformed_contract = synthetic_snapshot(rows=20)
        malformed_contract["price_contract"]["portfolio_assets"] = None
        self.assertTrue(any("portfolio_assets" in error for error in validate_snapshot(malformed_contract)))

    def test_contract_rejects_duplicate_snapshot_ids_and_unbound_sources(self) -> None:
        snapshot = synthetic_snapshot(rows=20)
        snapshot["rows"][1]["snapshot_id"] = snapshot["rows"][0]["snapshot_id"]
        snapshot["rows"][2]["price_sources"]["GLD"] = "missing-artifact"
        errors = validate_snapshot(snapshot)
        self.assertTrue(any("duplicate snapshot_id" in error for error in errors))
        self.assertTrue(any("not bound" in error for error in errors))

    def test_history_origin_contract_and_legacy_export_default(self) -> None:
        missing = synthetic_snapshot(rows=20)
        del missing["rows"][0]["history_origin"]
        self.assertTrue(any("history_origin must be one of" in error for error in validate_snapshot(missing)))
        invalid = synthetic_snapshot(rows=20)
        invalid["rows"][0]["history_origin"] = "unknown"
        self.assertTrue(any("history_origin must be one of" in error for error in validate_snapshot(invalid)))
        self.assertEqual(history_origin_for_row({}), "legacy_unclassified")
        self.assertEqual(history_origin_for_row({"history_origin": "live_recorded"}), "live_recorded")
        with self.assertRaisesRegex(ValueError, "invalid history_origin"):
            history_origin_for_row({"history_origin": "unknown"})

    def test_point_in_time_requires_all_history_live_recorded(self) -> None:
        for origin in ("retrospective_reconstruction", "legacy_unclassified"):
            with self.subTest(origin=origin):
                snapshot = synthetic_snapshot(rows=20)
                snapshot["rows"][3]["history_origin"] = origin
                self.assertTrue(
                    any("point-in-time PXI regime history must be live_recorded" in error for error in validate_snapshot(snapshot))
                )

    def test_config_rejects_nonfinite_numeric_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "finite"):
            test_config(hmm_tolerance=float("nan"))
        with self.assertRaisesRegex(ValueError, "finite"):
            test_config(minimum_excess_sharpe=float("inf"))
        with self.assertRaises(ValueError):
            test_config(cost_scenarios_bps=[10, float("nan")])

    def test_config_rejects_unknown_keys(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown challenger config keys"):
            test_config(minimum_oos_rowz=90)

    def test_canonical_config_freezes_evidence_strength_thresholds(self) -> None:
        config_path = Path(__file__).resolve().parents[1] / "hmm_config.json"
        config = load_config(config_path)
        checked_in = json.loads(config_path.read_text(encoding="utf-8"))
        self.assertEqual(config.profile, "canonical_evidence")
        self.assertGreaterEqual(config.min_train_rows, 1260)
        self.assertGreaterEqual(config.minimum_oos_rows, 1260)
        self.assertGreaterEqual(config.minimum_walk_forward_slices, 5)
        self.assertGreaterEqual(config.minimum_oos_years, 5)
        self.assertGreaterEqual(config.minimum_input_years, 15)
        self.assertGreaterEqual(config.minimum_input_rows, 3780)
        self.assertGreaterEqual(config.minimum_state_effective_rows, 30)
        self.assertGreaterEqual(config.test_rows, 252)
        normalized = {
            **config.__dict__,
            "state_counts": list(config.state_counts),
            "cost_scenarios_bps": list(config.cost_scenarios_bps),
        }
        self.assertEqual(normalized, checked_in)

    def test_every_canonical_control_is_exactly_frozen(self) -> None:
        config_path = Path(__file__).resolve().parents[1] / "hmm_config.json"
        canonical = json.loads(config_path.read_text(encoding="utf-8"))
        mutations = {
            "state_counts": [2, 4],
            "min_train_rows": 1259,
            "test_rows": 251,
            "step_rows": 253,
            "hmm_restarts": 6,
            "hmm_max_iterations": 199,
            "hmm_tolerance": 0.000002,
            "variance_floor": 0.0002,
            "random_seed": 20260823,
            "policy_shrinkage_rows": 19,
            "minimum_state_effective_rows": 29,
            "minimum_state_fraction": 0.03,
            "minimum_eligible_restarts": 2,
            "minimum_restart_state_agreement": 0.79,
            "cost_scenarios_bps": [10, 20],
            "paired_hac_lag": 4,
            "paired_familywise_alpha": 0.06,
            "annualization_periods": 251,
            "minimum_input_rows": 3779,
            "minimum_input_years": 14,
            "minimum_oos_rows": 1259,
            "minimum_oos_years": 4,
            "minimum_walk_forward_slices": 4,
            "minimum_excess_sharpe": 0.09,
        }
        self.assertEqual(set(mutations), set(canonical) - {"profile"})
        for name, weakened in mutations.items():
            with self.subTest(control=name):
                candidate = copy.deepcopy(canonical)
                candidate[name] = weakened
                if name == "test_rows":
                    candidate["step_rows"] = weakened
                with self.assertRaises(ValueError):
                    ChallengerConfig.from_dict(candidate)

    def test_named_canonical_controls_reach_exact_freeze_guard(self) -> None:
        config_path = Path(__file__).resolve().parents[1] / "hmm_config.json"
        canonical = json.loads(config_path.read_text(encoding="utf-8"))
        named_mutations = {
            "hmm_restarts": 6,
            "paired_familywise_alpha": 0.06,
            "paired_hac_lag": 4,
            "minimum_excess_sharpe": 0.09,
            "minimum_restart_state_agreement": 0.79,
            "policy_shrinkage_rows": 19,
            "random_seed": 20260823,
            "hmm_max_iterations": 199,
            "hmm_tolerance": 0.000002,
            "variance_floor": 0.0002,
        }
        for name, weakened in named_mutations.items():
            with self.subTest(control=name):
                candidate = copy.deepcopy(canonical)
                candidate[name] = weakened
                with self.assertRaisesRegex(ValueError, "must exactly match checked-in controls"):
                    ChallengerConfig.from_dict(candidate)

    def test_implementation_provenance_hashes_executed_source(self) -> None:
        provenance = implementation_provenance()
        source_path = Path(__file__).resolve().parents[1] / "hmm_challenger.py"
        self.assertEqual(provenance["source_sha256"], hashlib.sha256(source_path.read_bytes()).hexdigest())
        self.assertIsNone(provenance["build_revision"])
        self.assertEqual(provenance["build_revision_status"], "absent_not_supplied")
        supplied = implementation_provenance("  build-abc123  ")
        self.assertEqual(supplied["build_revision"], "build-abc123")
        self.assertEqual(supplied["build_revision_status"], "supplied")

    def test_point_in_time_generated_at_cannot_precede_artifacts(self) -> None:
        snapshot = synthetic_snapshot(rows=20)
        snapshot["source_artifacts"]["synthetic"]["fetched_at"] = "2026-08-23T00:00:00Z"
        self.assertTrue(any("fetched after generated_at" in error for error in validate_snapshot(snapshot)))

        component_snapshot = synthetic_snapshot(rows=20)
        component_snapshot["generated_at"] = "2024-01-05T00:00:00Z"
        component_snapshot["source_artifacts"]["synthetic"]["fetched_at"] = "2024-01-02T00:00:00Z"
        self.assertTrue(any("component is available after generated_at" in error for error in validate_snapshot(component_snapshot)))

    def test_combined_availability_cannot_precede_components(self) -> None:
        snapshot = synthetic_snapshot(rows=20)
        snapshot["rows"][2]["available_at"] = f"{snapshot['rows'][2]['decision_date']}T22:00:00Z"
        self.assertTrue(any("combined availability precedes" in error for error in validate_snapshot(snapshot)))

    def test_filter_is_prefix_invariant_to_future_observations(self) -> None:
        _, observations, _ = market_arrays(self.snapshot)
        mean, scale = fit_standardizer(observations[:120])
        standardized = apply_standardizer(observations, mean, scale)
        model, _ = select_hmm_model(
            standardized[:120],
            test_config(hmm_restarts=2, minimum_eligible_restarts=2, hmm_max_iterations=25),
        )
        prefix = filter_probabilities(model, standardized[:150])
        mutated = standardized.copy()
        mutated[150:] = mutated[150:] * -17.0 + 23.0
        full = filter_probabilities(model, mutated)
        np.testing.assert_allclose(prefix, full[:150], rtol=0, atol=1e-12)

    def test_nonconverged_or_degenerate_fits_are_not_selectable(self) -> None:
        _, observations, _ = market_arrays(self.snapshot)
        mean, scale = fit_standardizer(observations[:100])
        standardized = apply_standardizer(observations[:100], mean, scale)
        with self.assertRaisesRegex(ValueError, "no converged"):
            select_hmm_model(
                standardized,
                test_config(hmm_restarts=2, minimum_eligible_restarts=2, hmm_max_iterations=1, hmm_tolerance=1e-12),
            )
        with self.assertRaisesRegex(ValueError, "no converged"):
            select_hmm_model(
                standardized,
                test_config(minimum_state_effective_rows=1_000),
            )

    def test_walk_forward_selects_two_vs_three_states_inside_each_train_slice(self) -> None:
        slices = self.report["walk_forward"]["slices"]
        self.assertGreaterEqual(len(slices), 3)
        for item in slices:
            self.assertEqual(item["model_selection_scope"], "training observations only")
            self.assertEqual({candidate["state_count"] for candidate in item["bic_candidates"]}, {2, 3})
            self.assertIn(item["selected_state_count"], {2, 3})
            self.assertEqual(item["oos_inference"], "one-sided forward filter only")
            selected = next(candidate for candidate in item["bic_candidates"] if candidate["state_count"] == item["selected_state_count"])
            self.assertTrue(selected["converged"])
            self.assertTrue(selected["likelihood_monotone"])
            self.assertTrue(selected["selectable"])
            self.assertTrue(selected["restart_stability"]["passed"])
            diagnostics = item["filtered_state_diagnostics"]
            self.assertAlmostEqual(sum(diagnostics["latest_filtered_posterior"].values()), 1.0, places=10)
            self.assertGreaterEqual(diagnostics["latest_normalized_entropy"], 0.0)
            self.assertLessEqual(diagnostics["latest_normalized_entropy"], 1.0)

    def test_model_fingerprint_is_unchanged_by_future_prices(self) -> None:
        small_config = test_config(test_rows=30, step_rows=30, minimum_walk_forward_slices=1, minimum_oos_rows=30)
        short_snapshot = copy.deepcopy(self.snapshot)
        short_snapshot["rows"] = short_snapshot["rows"][:112]
        baseline = run(short_snapshot, small_config)
        modified = copy.deepcopy(short_snapshot)
        # The first model has 80 training observations ending at row 80. Only
        # mutate rows beyond its first test interval.
        for row in modified["rows"][105:]:
            row["adjusted_closes"]["SPY"] *= 3.0
            row["adjusted_closes"]["^VIX"] += 40.0
        future_changed = run(modified, small_config)
        self.assertEqual(
            baseline["walk_forward"]["slices"][0]["model_fingerprint"],
            future_changed["walk_forward"]["slices"][0]["model_fingerprint"],
        )

    def test_one_period_lag_and_cost_scenarios_are_explicit(self) -> None:
        alignment = self.report["walk_forward"]["alignment"]
        self.assertEqual(alignment["decision_to_execution_periods"], 1)
        self.assertEqual(alignment["decision_to_return_end_periods"], 2)
        self.assertLess(alignment["first"]["decision_date"], alignment["first"]["execution_date"])
        self.assertLess(alignment["first"]["execution_date"], alignment["first"]["return_date"])
        ten = self.report["results"]["10_bps"]["hmm_challenger"]
        twenty_five = self.report["results"]["25_bps"]["hmm_challenger"]
        self.assertGreater(twenty_five["transaction_cost_paid"], ten["transaction_cost_paid"])
        self.assertLessEqual(twenty_five["cumulative_return"], ten["cumulative_return"])
        self.assertAlmostEqual(self.report["results"]["10_bps"]["spy"]["total_turnover"], 1.0)
        uncertainty = self.report["paired_uncertainty_25_bps"]["comparisons"]
        self.assertEqual(set(uncertainty), {"spy", "equal_weight", "pxi_regime_rule"})
        self.assertTrue(all(item["hac_lag"] == 5 for item in uncertainty.values()))
        self.assertFalse(self.report["paired_uncertainty_25_bps"]["finite_sample_guarantee"])
        self.assertEqual(self.report["paired_uncertainty_25_bps"]["approximation"], "asymptotic normal")

    def test_turnover_uses_post_return_drifted_holdings(self) -> None:
        weights = np.array([[0.5, 0.5, 0.0], [0.5, 0.5, 0.0]])
        returns = np.array([[0.10, 0.0, 0.0], [0.0, 0.0, 0.0]])
        _, turnover, _ = net_return_series(weights, returns, 10.0)
        drifted_spy = 0.55 / 1.05
        expected = 0.5 * (abs(0.5 - drifted_spy) + abs(0.5 - (0.5 / 1.05)))
        self.assertAlmostEqual(turnover[0], 1.0)
        self.assertAlmostEqual(turnover[1], expected)
        self.assertGreater(turnover[1], 0.0)

    def test_research_output_is_never_actionable(self) -> None:
        self.assertEqual(self.report["governance"]["status"], "NO_GO")
        self.assertEqual(self.report["governance"]["actionability"], "NONE")
        self.assertFalse(self.report["methodology"]["reinforcement_learning"]["implemented"])
        self.assertEqual(self.report["methodology"]["reinforcement_learning"]["status"], "REJECTED_NON_DEFAULT")
        self.assertRegex(self.report["dataset"]["snapshot_sha256"], r"^[0-9a-f]{64}$")
        self.assertRegex(self.report["config_sha256"], r"^[0-9a-f]{64}$")
        self.assertRegex(self.report["implementation_provenance"]["source_sha256"], r"^[0-9a-f]{64}$")
        self.assertIsNone(self.report["implementation_provenance"]["build_revision"])
        self.assertEqual(
            self.report["implementation_provenance"]["build_revision_status"],
            "absent_not_supplied",
        )
        self.assertIn("ingestion assertion", self.report["dataset"]["point_in_time_label"])

    def test_mutable_public_style_input_is_a_screen_blocker(self) -> None:
        snapshot = synthetic_snapshot(point_in_time=False)
        report = run(snapshot, self.config)
        self.assertEqual(report["retrospective_screen"]["status"], "NO_GO")
        self.assertTrue(any("point-in-time" in reason for reason in report["retrospective_screen"]["reasons"]))
        self.assertEqual(report["governance"]["status"], "NO_GO")

    def test_non_live_history_is_counted_and_explicitly_blocked(self) -> None:
        snapshot = synthetic_snapshot(point_in_time=False)
        snapshot["rows"][0]["history_origin"] = "retrospective_reconstruction"
        snapshot["rows"][1]["history_origin"] = "legacy_unclassified"
        report = run(snapshot, self.config)
        provenance = report["pxi_history_provenance"]
        self.assertEqual(provenance["counts"]["retrospective_reconstruction"], 1)
        self.assertEqual(provenance["counts"]["legacy_unclassified"], 1)
        self.assertEqual(provenance["counts"]["live_recorded"], len(snapshot["rows"]) - 2)
        self.assertFalse(provenance["all_rows_live_recorded"])
        reasons = report["retrospective_screen"]["reasons"]
        self.assertTrue(any("retrospective_reconstruction" in reason for reason in reasons))
        self.assertTrue(any("legacy_unclassified" in reason for reason in reasons))

    def test_exploratory_profile_can_never_pass_evidence_screen(self) -> None:
        report = run(self.snapshot, self.config)
        self.assertEqual(report["retrospective_screen"]["status"], "NO_GO")
        self.assertTrue(any("exploratory_smoke" in reason for reason in report["retrospective_screen"]["reasons"]))

    def test_missing_interior_session_suppresses_all_metrics(self) -> None:
        snapshot = synthetic_snapshot()
        del snapshot["rows"][150]
        report = run(snapshot, self.config)
        self.assertFalse(report["walk_forward"]["metrics_valid"])
        self.assertTrue(report["walk_forward"]["cadence"]["interior_missing_expected_sessions"])
        self.assertTrue(report["walk_forward"]["cadence"]["ordinal_gaps"])
        self.assertTrue(all(not strategies for strategies in report["results"].values()))
        self.assertFalse(report["paired_uncertainty_25_bps"]["comparisons"])
        self.assertEqual(report["retrospective_screen"]["status"], "NO_GO")


if __name__ == "__main__":
    unittest.main()
