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
    "request_rate_limit_buckets",
}

EXPECTED_INDEXES = {
    "idx_market_alert_deliveries_email_unique",
    "idx_market_refresh_runs_completed",
    "idx_market_decision_impact_lookup",
    "idx_research_feature_snapshots_decision",
    "idx_request_rate_limit_window",
}

EXPECTED_TRIGGERS = {
    "research_feature_snapshots_no_update",
    "research_feature_snapshots_no_delete",
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
            connection.commit()

            for path in migration_files():
                apply_sql_file(connection, path)

            assert_expected_schema(connection)

            row = connection.execute(
                "SELECT status, brief_generated FROM market_refresh_runs WHERE \"trigger\" = 'migration_test'"
            ).fetchone()
            if row != ("success", 1):
                raise SystemExit("Current-schema migration apply did not preserve existing market_refresh_runs rows")
        finally:
            connection.close()


if __name__ == "__main__":
    apply_migrations_to_empty_db()
    apply_migrations_to_current_schema_db()
