from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "worker" / "migrations"
SCHEMA_PATH = REPO_ROOT / "worker" / "schema.sql"

EXPECTED_TABLES = {
    "indicator_values",
    "pxi_scores",
    "category_scores",
    "market_brief_snapshots",
    "opportunity_snapshots",
    "market_refresh_runs",
    "market_alert_events",
    "market_alert_deliveries",
    "market_decision_impact_snapshots",
    "market_calibration_snapshots",
    "market_utility_events",
    "research_feature_snapshots",
    "market_prediction_evidence",
    "request_rate_limit_buckets",
}

EXPECTED_INDEXES = {
    "idx_market_alert_deliveries_email_unique",
    "idx_market_refresh_runs_completed",
    "idx_market_decision_impact_lookup",
    "idx_research_feature_snapshots_decision",
    "idx_research_feature_snapshots_canonical_slot",
    "idx_market_prediction_evidence_model_date",
    "idx_request_rate_limit_window",
}

EXPECTED_TRIGGERS = {
    "research_feature_snapshots_no_update",
    "research_feature_snapshots_no_delete",
    "market_prediction_evidence_prediction_immutable",
    "market_prediction_evidence_outcomes_write_once",
    "market_prediction_evidence_no_delete",
}


def migration_files() -> list[Path]:
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not files:
        raise SystemExit("No worker migrations found in worker/migrations")
    return files


def apply_sql_file(connection: sqlite3.Connection, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    if not sql.strip():
        raise SystemExit(f"Migration file is empty: {path}")
    connection.executescript(sql)


def assert_expected_schema(connection: sqlite3.Connection) -> None:
    rows = connection.execute(
        "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'index', 'trigger')"
    ).fetchall()
    names_by_type: dict[str, set[str]] = {"table": set(), "index": set()}
    for name, obj_type in rows:
        names_by_type.setdefault(obj_type, set()).add(name)

    missing_tables = sorted(EXPECTED_TABLES - names_by_type["table"])
    missing_indexes = sorted(EXPECTED_INDEXES - names_by_type["index"])
    missing_triggers = sorted(EXPECTED_TRIGGERS - names_by_type.get("trigger", set()))
    if missing_tables:
        raise SystemExit(f"Missing expected tables after migration apply: {', '.join(missing_tables)}")
    if missing_indexes:
        raise SystemExit(f"Missing expected indexes after migration apply: {', '.join(missing_indexes)}")
    if missing_triggers:
        raise SystemExit(f"Missing expected triggers after migration apply: {', '.join(missing_triggers)}")

    evidence_foreign_keys = connection.execute(
        "PRAGMA foreign_key_list(market_prediction_evidence)"
    ).fetchall()
    if not any(
        row[2] == "research_feature_snapshots"
        and row[3] == "feature_snapshot_id"
        and row[4] == "snapshot_id"
        for row in evidence_foreign_keys
    ):
        raise SystemExit("market_prediction_evidence is missing its research snapshot foreign key")

    connection.execute(
        """
        INSERT OR IGNORE INTO research_feature_snapshots (
          snapshot_id, decision_date, available_at, feature_version,
          storage_contract, capture_source, benchmark_close,
          benchmark_observation_date, payload_json
        ) VALUES (
          'immutability-test', '2026-03-05', '2026-03-05T20:00:00.000Z',
          'test-v1', 'append-only-d1-research-snapshots/v1', 'migration-test',
          100.0, '2026-03-05', '{}'
        )
        """
    )
    try:
        connection.execute(
            "UPDATE research_feature_snapshots SET benchmark_close = 101 WHERE snapshot_id = 'immutability-test'"
        )
    except sqlite3.IntegrityError:
        pass
    else:
        raise SystemExit("research_feature_snapshots allowed an update despite immutability trigger")

    connection.execute(
        """
        INSERT INTO research_feature_snapshots (
          snapshot_id, decision_date, available_at, feature_version,
          storage_contract, capture_source, canonical_slot, benchmark_close,
          benchmark_observation_date, payload_json
        ) VALUES (
          'canonical-test', '2026-03-05', '2026-03-05T22:00:00.000Z',
          'test-v1', 'append-only-d1-research-snapshots/v1', 'migration-test',
          'daily_close_22z', 100.0, '2026-03-05',
          '{"canonical_slot":"daily_close_22z"}'
        )
        """
    )
    try:
        connection.execute(
            """
            INSERT INTO research_feature_snapshots (
              snapshot_id, decision_date, available_at, feature_version,
              storage_contract, capture_source, canonical_slot, benchmark_close,
              benchmark_observation_date, payload_json
            ) VALUES (
              'canonical-test-duplicate', '2026-03-05', '2026-03-05T22:01:00.000Z',
              'test-v1', 'append-only-d1-research-snapshots/v1', 'migration-test',
              'daily_close_22z', 101.0, '2026-03-05',
              '{"canonical_slot":"daily_close_22z"}'
            )
            """
        )
    except sqlite3.IntegrityError:
        pass
    else:
        raise SystemExit("research_feature_snapshots allowed a duplicate canonical slot")

    connection.execute(
        """
        INSERT INTO market_prediction_evidence (
          evidence_id, prediction_date, prediction_available_at, canonical_slot,
          feature_snapshot_id, model_family, model_version, target_metric,
          current_pxi_score, pxi_bucket, bucket_lower, bucket_upper,
          benchmark_close, benchmark_observation_date, target_date_7d,
          target_date_30d, predicted_return_7d, predicted_return_30d,
          sample_size_7d, sample_size_30d, training_cutoff_date, methodology_json
        ) VALUES (
          'evidence-test-v1', '2026-03-05', '2026-03-05T22:05:00.000Z',
          'daily_close_22z', 'canonical-test', 'empirical_bucket_spy_return',
          'empirical-bucket-spy-return/v1', 'spy_return_pct', 72.0, '60-80',
          60.0, 80.0, 100.0, '2026-03-05', '2026-03-12', '2026-04-04',
          1.0, 2.0, 10, 8, '2026-03-04', '{}'
        )
        """
    )
    try:
        connection.execute(
            "UPDATE market_prediction_evidence SET predicted_return_7d = 9 WHERE evidence_id = 'evidence-test-v1'"
        )
    except sqlite3.IntegrityError:
        pass
    else:
        raise SystemExit("market_prediction_evidence allowed a forecast mutation")

    connection.execute(
        """
        UPDATE market_prediction_evidence
        SET outcome_status_7d = 'observed',
            actual_return_7d = 1.5,
            actual_observation_date_7d = '2026-03-12',
            evaluated_at_7d = '2026-03-12T22:00:00.000Z'
        WHERE evidence_id = 'evidence-test-v1'
        """
    )
    try:
        connection.execute(
            """
            UPDATE market_prediction_evidence
            SET evaluated_at = '2026-03-12T22:00:00.000Z'
            WHERE evidence_id = 'evidence-test-v1'
            """
        )
    except sqlite3.IntegrityError:
        pass
    else:
        raise SystemExit("market_prediction_evidence allowed completion before both horizons existed")
    try:
        connection.execute(
            "UPDATE market_prediction_evidence SET actual_return_7d = 1.6 WHERE evidence_id = 'evidence-test-v1'"
        )
    except sqlite3.IntegrityError:
        pass
    else:
        raise SystemExit("market_prediction_evidence allowed an outcome rewrite")

    connection.execute(
        """
        UPDATE market_prediction_evidence
        SET outcome_status_30d = 'unavailable',
            outcome_unavailable_reason_30d = 'canonical_close_missing_within_tolerance',
            evaluated_at_30d = '2026-04-08T22:00:00.000Z'
        WHERE evidence_id = 'evidence-test-v1'
        """
    )
    connection.execute(
        """
        UPDATE market_prediction_evidence
        SET evaluated_at = '2026-04-08T22:00:00.000Z'
        WHERE evidence_id = 'evidence-test-v1'
        """
    )
    try:
        connection.execute(
            """
            UPDATE market_prediction_evidence
            SET outcome_status_30d = 'pending',
                outcome_unavailable_reason_30d = NULL,
                evaluated_at_30d = NULL
            WHERE evidence_id = 'evidence-test-v1'
            """
        )
    except sqlite3.IntegrityError:
        pass
    else:
        raise SystemExit("market_prediction_evidence allowed a terminal unavailable outcome to reopen")

    try:
        connection.execute(
            "DELETE FROM market_prediction_evidence WHERE evidence_id = 'evidence-test-v1'"
        )
    except sqlite3.IntegrityError:
        pass
    else:
        raise SystemExit("market_prediction_evidence allowed a delete")

    connection.execute(
        """
        INSERT INTO market_prediction_evidence (
          evidence_id, prediction_date, prediction_available_at, canonical_slot,
          feature_snapshot_id, model_family, model_version, target_metric,
          current_pxi_score, pxi_bucket, bucket_lower, bucket_upper,
          benchmark_close, benchmark_observation_date, target_date_7d,
          target_date_30d, sample_size_7d, sample_size_30d, methodology_json
        ) VALUES (
          'evidence-test-v2', '2026-03-05', '2026-03-05T22:05:00.000Z',
          'daily_close_22z', 'canonical-test', 'empirical_bucket_spy_return',
          'empirical-bucket-spy-return/v2', 'spy_return_pct', 72.0, '60-80',
          60.0, 80.0, 100.0, '2026-03-05', '2026-03-12', '2026-04-04',
          0, 0, '{}'
        )
        """
    )

    refresh_runs_sql_row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'market_refresh_runs'"
    ).fetchone()
    refresh_runs_sql = refresh_runs_sql_row[0] if refresh_runs_sql_row else ""
    if "'blocked'" not in refresh_runs_sql:
        raise SystemExit("market_refresh_runs schema does not allow status='blocked' after migration apply")

    consume_sql = """
        INSERT INTO request_rate_limit_buckets
          (scope, subject_hash, window_start, count, updated_at)
        VALUES ('test', 'subject', 1000, 1, datetime('now'))
        ON CONFLICT(scope, subject_hash, window_start) DO UPDATE SET
          count = request_rate_limit_buckets.count + 1,
          updated_at = datetime('now')
        WHERE request_rate_limit_buckets.count < 2
        RETURNING count
    """
    first = connection.execute(consume_sql).fetchone()
    second = connection.execute(consume_sql).fetchone()
    blocked = connection.execute(consume_sql).fetchone()
    if first != (1,) or second != (2,) or blocked is not None:
        raise SystemExit("request_rate_limit_buckets did not enforce its atomic window budget")


def apply_migrations_to_empty_db() -> None:
    with tempfile.NamedTemporaryFile(suffix=".sqlite3") as handle:
        connection = sqlite3.connect(handle.name)
        try:
            for path in migration_files():
                apply_sql_file(connection, path)
            assert_expected_schema(connection)
        finally:
            connection.close()


def apply_migrations_to_current_schema_db() -> None:
    with tempfile.NamedTemporaryFile(suffix=".sqlite3") as handle:
        connection = sqlite3.connect(handle.name)
        try:
            apply_sql_file(connection, SCHEMA_PATH)
            connection.execute(
                """
                INSERT INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d)
                VALUES ('2026-03-05', 72.0, 'risk-on', 'pamping', 1.1, 3.2, 6.8)
                """
            )
            connection.execute(
                """
                INSERT INTO market_refresh_runs (
                  started_at,
                  completed_at,
                  status,
                  "trigger",
                  brief_generated,
                  opportunities_generated,
                  calibrations_generated,
                  alerts_generated,
                  stale_count,
                  critical_stale_count,
                  as_of,
                  error
                )
                VALUES (
                  '2026-03-05T12:00:00.000Z',
                  '2026-03-05T12:05:00.000Z',
                  'success',
                  'migration_test',
                  1,
                  2,
                  3,
                  4,
                  0,
                  0,
                  '2026-03-05T12:05:00.000Z',
                  NULL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO research_feature_snapshots (
                  snapshot_id, decision_date, available_at, feature_version,
                  storage_contract, capture_source, canonical_slot, benchmark_close,
                  benchmark_observation_date, payload_json
                ) VALUES (
                  'legacy-raw-snapshot', '2026-03-05', '2026-03-05T18:00:00.000Z',
                  'test-v1', 'append-only-d1-research-snapshots/v1', 'migration-test',
                  NULL, 100.0, '2026-03-04',
                  '{"canonical_slot":"daily_close_22z","legacy_raw":true}'
                )
                """
            )
            connection.execute(
                """
                INSERT INTO market_opportunity_ledger (
                  refresh_run_id, as_of, horizon, candidate_count, published_count,
                  suppressed_count, degraded_reason, top_direction_candidate,
                  top_direction_published
                ) VALUES (
                  NULL, '2026-02-20T00:00:00.000Z', '7d', 2, 1, 1,
                  NULL, 'bullish', 'bullish'
                )
                """
            )
            connection.execute(
                """
                INSERT INTO market_opportunity_item_ledger (
                  refresh_run_id, as_of, horizon, opportunity_id, theme_id,
                  theme_name, direction, conviction_score, published,
                  suppression_reason
                ) VALUES (
                  NULL, '2026-02-20T00:00:00.000Z', '7d', 'historical-test',
                  'test-theme', 'Test Theme', 'bullish', 70, 1, NULL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO market_decision_impact_snapshots (
                  as_of, horizon, scope, window_days, payload_json
                ) VALUES (
                  '2026-03-05T00:00:00.000Z', '7d', 'market', 30, '{}'
                )
                """
            )
            connection.commit()

            for path in migration_files():
                apply_sql_file(connection, path)

            assert_expected_schema(connection)

            row = connection.execute(
                "SELECT status, brief_generated FROM market_refresh_runs WHERE \"trigger\" = 'migration_test'"
            ).fetchone()
            if row != ("success", 1):
                raise SystemExit("Current-schema migration apply did not preserve existing market_refresh_runs rows")

            legacy_snapshot = connection.execute(
                """
                SELECT canonical_slot, benchmark_observation_date, payload_json
                FROM research_feature_snapshots
                WHERE snapshot_id = 'legacy-raw-snapshot'
                """
            ).fetchone()
            if legacy_snapshot != (
                None,
                "2026-03-04",
                '{"canonical_slot":"daily_close_22z","legacy_raw":true}',
            ):
                raise SystemExit("Canonical-slot migration did not preserve the legacy snapshot as raw")

            historical_item = connection.execute(
                """
                SELECT published, suppression_reason
                FROM market_opportunity_item_ledger
                WHERE opportunity_id = 'historical-test'
                """
            ).fetchone()
            if historical_item != (0, "historical_backfill_nonprospective"):
                raise SystemExit("Historical backfill item was not excluded from prospective publication")

            historical_ledger = connection.execute(
                """
                SELECT published_count, suppressed_count, degraded_reason, top_direction_published
                FROM market_opportunity_ledger
                WHERE refresh_run_id IS NULL AND as_of = '2026-02-20T00:00:00.000Z'
                """
            ).fetchone()
            if historical_ledger != (0, 2, "historical_backfill_nonprospective", None):
                raise SystemExit("Historical backfill parent ledger was not made non-prospective")

            impact_snapshot_count = connection.execute(
                "SELECT COUNT(*) FROM market_decision_impact_snapshots"
            ).fetchone()
            if impact_snapshot_count != (0,):
                raise SystemExit("Derived decision-impact snapshots were not cleared after provenance repair")
        finally:
            connection.close()


if __name__ == "__main__":
    apply_migrations_to_empty_db()
    apply_migrations_to_current_schema_db()
