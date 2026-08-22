from __future__ import annotations

import re
import sqlite3
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "worker" / "migrations"
SCHEMA_PATH = REPO_ROOT / "worker" / "schema.sql"
ADMIN_INGESTION_PATH = REPO_ROOT / "worker" / "routes" / "admin-ingestion.ts"
PUBLIC_READ_PATH = REPO_ROOT / "worker" / "routes" / "public-read.ts"
SYSTEM_ROUTE_PATH = REPO_ROOT / "worker" / "routes" / "system.ts"
LEGACY_RUNTIME_PATH = REPO_ROOT / "worker" / "runtime" / "legacy.ts"
DAILY_REFRESH_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "daily-refresh.yml"
DEPLOY_WORKER_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "deploy-worker.yml"
OPERATIONAL_ISOLATION_PATH = REPO_ROOT / ".github" / "workflows" / "OPERATIONAL_ISOLATION.md"
RECONSTRUCTION_WORKFLOW_PATH = (
    REPO_ROOT / ".github" / "workflows" / "score-history-reconstruction.yml"
)

EXPECTED_TABLES = {
    "indicator_values",
    "pxi_scores",
    "category_scores",
    "pxi_score_reconstructions",
    "category_score_reconstructions",
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
    "refresh_scheduler_runs",
    "refresh_scheduler_incidents",
    "refresh_mutation_locks",
}

EXPECTED_INDEXES = {
    "idx_market_alert_deliveries_email_unique",
    "idx_market_refresh_runs_completed",
    "idx_market_decision_impact_lookup",
    "idx_research_feature_snapshots_decision",
    "idx_research_feature_snapshots_canonical_slot",
    "idx_market_prediction_evidence_model_date",
    "idx_request_rate_limit_window",
    "idx_pxi_scores_origin_date",
    "idx_category_scores_origin_date",
    "idx_pxi_score_reconstructions_date",
    "idx_category_score_reconstructions_date",
    "idx_refresh_scheduler_runs_decision",
    "idx_refresh_scheduler_runs_status",
    "idx_refresh_scheduler_incidents_status",
}

EXPECTED_TRIGGERS = {
    "research_feature_snapshots_no_update",
    "research_feature_snapshots_no_delete",
    "market_prediction_evidence_prediction_immutable",
    "market_prediction_evidence_outcomes_write_once",
    "market_prediction_evidence_no_delete",
    "pxi_scores_no_reconstruction_insert",
    "pxi_scores_no_reconstruction_update",
    "category_scores_no_reconstruction_insert",
    "category_scores_no_reconstruction_update",
    "pxi_score_reconstructions_missing_only",
    "category_score_reconstructions_missing_only",
    "pxi_score_reconstructions_no_update",
    "pxi_score_reconstructions_no_delete",
    "category_score_reconstructions_no_update",
    "category_score_reconstructions_no_delete",
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


def assert_score_history_code_isolation() -> None:
    production_sources = [
        path
        for path in (REPO_ROOT / "worker").rglob("*.ts")
        if not path.name.endswith(".test.ts") and path.name != "worker-configuration.d.ts"
    ]

    isolated_table_allowlist = {
        ADMIN_INGESTION_PATH.resolve(),
        PUBLIC_READ_PATH.resolve(),
        SYSTEM_ROUTE_PATH.resolve(),
        LEGACY_RUNTIME_PATH.resolve(),
    }
    for path in production_sources:
        source = path.read_text(encoding="utf-8")
        if re.search(
            r"(?:INSERT\s+OR\s+REPLACE|REPLACE)\s+INTO\s+"
            r"(?:pxi_score_reconstructions|category_score_reconstructions)\b",
            source,
            flags=re.IGNORECASE,
        ):
            raise SystemExit(f"Production code may not REPLACE immutable reconstruction rows: {path}")
        if not re.search(r"\b(?:pxi|category)_score_reconstructions\b", source):
            continue
        if path.resolve() not in isolated_table_allowlist:
            raise SystemExit(
                f"Retrospective score table escaped the audited history/backfill boundary: {path}"
            )

    system_source = SYSTEM_ROUTE_PATH.read_text(encoding="utf-8")
    if re.search(
        r"\b(?:INSERT|UPDATE|DELETE|REPLACE)\b[\s\S]{0,80}"
        r"\b(?:pxi|category)_score_reconstructions\b",
        system_source,
        flags=re.IGNORECASE,
    ):
        raise SystemExit("The health capability route may only inspect reconstruction schema read-only")
    for marker in (
        "HISTORY_RECONSTRUCTION_CONTRACT",
        "history_reconstruction_contract:",
        "pxi_scores_no_reconstruction_insert",
        "category_score_reconstructions_no_delete",
        "'unavailable'",
    ):
        if marker not in system_source:
            raise SystemExit(f"The health capability route is missing fail-closed marker: {marker}")

    for path in (ADMIN_INGESTION_PATH, LEGACY_RUNTIME_PATH):
        source = path.read_text(encoding="utf-8")
        for match in re.finditer(
            r"INSERT\s+OR\s+REPLACE\s+INTO\s+(pxi_scores|category_scores)\s*"
            r"\((?P<columns>[^)]*)\)\s*VALUES\s*\((?P<values>[^)]*)\)",
            source,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            columns = match.group("columns")
            values = match.group("values")
            if "history_origin" not in columns or "'live_recorded'" not in values:
                line = source.count("\n", 0, match.start()) + 1
                raise SystemExit(f"Live score write lacks explicit live_recorded provenance: {path}:{line}")

    admin_source = ADMIN_INGESTION_PATH.read_text(encoding="utf-8")
    legacy_backfill_start = admin_source.index(
        "if (url.pathname === '/api/backfill' && method === 'POST')"
    )
    reconstruction_start = admin_source.index(
        "if (url.pathname === '/api/history/reconstruct-missing-v1' && method === 'POST')",
        legacy_backfill_start,
    )
    legacy_backfill_source = admin_source[legacy_backfill_start:reconstruction_start]
    if "status: 410" not in legacy_backfill_source or "permanently disabled" not in legacy_backfill_source:
        raise SystemExit("The modular unversioned /api/backfill route is not permanently fail-closed")
    for forbidden_marker in ("enforceAdminAuth", "env.DB", "parseJsonBody", "INSERT", "UPDATE", "DELETE"):
        if forbidden_marker in legacy_backfill_source:
            raise SystemExit(
                f"The unversioned /api/backfill kill switch must not authenticate or touch state: {forbidden_marker}"
            )

    reconstruction_end = admin_source.index(
        "if (url.pathname === '/api/recalculate-all-signals' && method === 'POST')",
        reconstruction_start,
    )
    backfill_source = admin_source[reconstruction_start:reconstruction_end]
    required_markers = (
        "INSERT INTO pxi_score_reconstructions",
        "INSERT INTO category_score_reconstructions",
        "point_in_time_guarantee: false",
        "research_evidence_captured: false",
        "market_products_refreshed: false",
        "decision_impact_refreshed: false",
        "embeddings_generated: 0",
        "status: 'conflict'",
        "stopped_early: stoppedEarly",
        "unprocessed: dates.length - results.length",
        "const today = deps.currentNewYorkDate();",
        "PXI_SCORE_CATEGORIES",
        "hasCompleteCategorySet",
        "expected_build_sha",
        "limit > 3",
        "replaced: 0",
    )
    for marker in required_markers:
        if marker not in backfill_source:
            raise SystemExit(f"Backfill provenance guard is missing: {marker}")
    if backfill_source.index("INSERT INTO category_score_reconstructions") >= backfill_source.index(
        "INSERT INTO pxi_score_reconstructions"
    ):
        raise SystemExit("Reconstruction batch must insert all categories before the PXI aggregate seal")
    if backfill_source.index("expectedBuildSha.toLowerCase() !== buildSha.toLowerCase()") >= backfill_source.index(
        "SELECT MAX(fetched_at) AS source_data_as_of"
    ):
        raise SystemExit("Expected build SHA mismatch must fail before any reconstruction D1 access")
    forbidden_markers = (
        "INSERT OR REPLACE INTO pxi_scores",
        "INSERT OR REPLACE INTO category_scores",
        "recordMarketRefreshRun",
        "captureResearchFeatureSnapshot",
        "captureCanonicalMarketPredictionEvidence",
        "VECTORIZE",
        "env.AI",
        "ensureMarketProductSchema",
    )
    for marker in forbidden_markers:
        if marker in backfill_source:
            raise SystemExit(f"Backfill contains a forbidden evidence/product side effect: {marker}")

    legacy_source = LEGACY_RUNTIME_PATH.read_text(encoding="utf-8")
    if "Legacy backfill is disabled" not in legacy_source:
        raise SystemExit("The unused monolithic worker's destructive backfill route is not fail-closed")
    legacy_route_start = legacy_source.index(
        "if (url.pathname === '/api/backfill' && method === 'POST')"
    )
    legacy_route_end = legacy_source.index("// Predict SPY returns", legacy_route_start)
    legacy_route_source = legacy_source[legacy_route_start:legacy_route_end]
    if "status: 410" not in legacy_route_source:
        raise SystemExit("The monolithic legacy backfill route does not return 410")
    for forbidden_marker in ("enforceAdminAuth", "env.DB", "request.json", "INSERT", "UPDATE", "DELETE"):
        if forbidden_marker in legacy_route_source:
            raise SystemExit(
                f"The monolithic legacy backfill kill switch must not authenticate or touch state: {forbidden_marker}"
            )
    if "options.includeRetrospectiveHistory" not in legacy_source:
        raise SystemExit("calculatePXI does not explicitly gate retrospective delta history")
    if legacy_source.count("const currentDate = currentNewYorkDate();") < 2:
        raise SystemExit("Legacy live write/recalculate paths do not share the New York date guard")
    if "const today = currentNewYorkDate();" not in legacy_source:
        raise SystemExit("The scheduled live score path does not use the New York decision date")
    if admin_source.count("const currentDate = deps.currentNewYorkDate();") < 2:
        raise SystemExit("Modular live write/recalculate paths do not share the New York date guard")

    workflow = RECONSTRUCTION_WORKFLOW_PATH.read_text(encoding="utf-8")
    workflow_required = (
        '"missing_only": True',
        '"overwrite": False',
        '"record_evidence": False',
        '"refresh_products": False',
        '"include_decision_impact": False',
        '"generate_embeddings": False',
        ".replaced == 0",
        ".point_in_time_guarantee == false",
        ".stopped_early == false",
        ".unprocessed == 0",
        "((.results | length) == $limit)",
        "([.results[].date] == $expected_dates)",
        'history_reconstruction_contract == $contract',
        'EXPECTED_BUILD_SHA="${GITHUB_SHA:0:12}"',
        '--arg build_sha "${GITHUB_SHA:0:12}"',
        '== ($build_sha | ascii_downcase)',
        'ZoneInfo("America/New_York")',
        '"expected_build_sha": build_sha',
        "dt.timedelta(days=2)",
        '"expected_dates": [',
        "del(.expected_dates)",
        "pxi-score-reconstruction-chunks.json",
        "for ((index = 0; index < CHUNK_COUNT; index += 1)); do",
        "if ((index + 1 < CHUNK_COUNT)); then",
        "sleep 4",
        "/api/history/reconstruct-missing-v1",
    )
    for marker in workflow_required:
        if marker not in workflow:
            raise SystemExit(f"Manual reconstruction workflow is missing safety assertion: {marker}")
    for forbidden_endpoint in ("/api/market/refresh-products", "/api/market/backfill-products"):
        if forbidden_endpoint in workflow:
            raise SystemExit(
                f"Score reconstruction workflow must not invoke market-product endpoint {forbidden_endpoint}"
            )
    if "/api/backfill" in workflow:
        raise SystemExit("Score reconstruction workflow may only call the versioned reconstruction endpoint")

    health_gate = workflow.index("Require matching deployed reconstruction contract")
    reconstruction_post = workflow.index(
        '-X POST "${BASE_URL}/api/history/reconstruct-missing-v1"'
    )
    if health_gate >= reconstruction_post:
        raise SystemExit("The deployed-build health/capability gate must run before reconstruction POST")

    daily_workflow = DAILY_REFRESH_WORKFLOW_PATH.read_text(encoding="utf-8")
    deploy_workflow = DEPLOY_WORKER_WORKFLOW_PATH.read_text(encoding="utf-8")
    concurrency_group = "group: pxi-production-indicator-score-mutation"
    for path, source in (
        (DAILY_REFRESH_WORKFLOW_PATH, daily_workflow),
        (RECONSTRUCTION_WORKFLOW_PATH, workflow),
    ):
        if concurrency_group not in source or "cancel-in-progress: false" not in source:
            raise SystemExit(f"Operational isolation concurrency contract is missing: {path}")
    for marker in (
        "inputs.environment == 'production'",
        "pxi-production-indicator-score-mutation",
        "inputs.environment != 'production'",
        "Verify legacy backfill kill switch",
        '"${TARGET_BASE_URL}/api/backfill"',
        '"${http_code}" != "410"',
    ):
        if marker not in deploy_workflow:
            raise SystemExit(f"Production deploy isolation/kill-switch gate is missing: {marker}")

    isolation_contract = OPERATIONAL_ISOLATION_PATH.read_text(encoding="utf-8")
    for marker in (
        "pxi-production-indicator-score-mutation",
        "cancel-in-progress: false",
        "isolated-missing-only-v1",
        "never point-in-time research evidence",
    ):
        if marker not in isolation_contract:
            raise SystemExit(f"Operational isolation documentation is missing: {marker}")


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

    for table in ("pxi_scores", "category_scores"):
        columns = {
            row[1]: {"not_null": row[3], "default": row[4]}
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        origin = columns.get("history_origin")
        if origin != {"not_null": 1, "default": "'legacy_unclassified'"}:
            raise SystemExit(f"{table}.history_origin does not default fail-closed to legacy_unclassified")

    canonical_categories = (
        "breadth",
        "credit",
        "crypto",
        "global",
        "macro",
        "positioning",
        "volatility",
    )

    def insert_reconstruction_categories(
        date: str,
        categories: tuple[str, ...] = canonical_categories,
        *,
        reconstructed_at: str = "2026-08-22T18:00:00.000Z",
        method: str = "test-method",
        build_sha: str = "a1b2c3d4e5f6",
        source_data_as_of: str = "2026-08-22T17:55:00.000Z",
    ) -> None:
        for category in categories:
            connection.execute(
                """
                INSERT INTO category_score_reconstructions (
                  category, date, score, weight, weighted_score, history_origin,
                  reconstructed_at, reconstruction_method,
                  reconstruction_build_sha, source_data_as_of
                ) VALUES (?, ?, 62, ?, ?, 'retrospective_reconstruction', ?, ?, ?, ?)
                """,
                (
                    category,
                    date,
                    1 / len(canonical_categories),
                    62 / len(canonical_categories),
                    reconstructed_at,
                    method,
                    build_sha,
                    source_data_as_of,
                ),
            )

    def insert_reconstruction_pxi(
        date: str,
        *,
        method: str = "test-method",
        build_sha: str = "a1b2c3d4e5f6",
        source_data_as_of: str = "2026-08-22T17:55:00.000Z",
    ) -> None:
        connection.execute(
            """
            INSERT INTO pxi_score_reconstructions (
              date, score, label, status, delta_1d, delta_7d, delta_30d,
              history_origin, reconstructed_at, reconstruction_method,
              reconstruction_build_sha, source_data_as_of
            ) VALUES (
              ?, 61, 'neutral', 'neutral', 1, 2, 3,
              'retrospective_reconstruction', '2026-08-22T18:00:00.000Z',
              ?, ?, ?
            )
            """,
            (date, method, build_sha, source_data_as_of),
        )

    def expect_integrity_error(label: str, operation) -> None:
        try:
            operation()
        except sqlite3.IntegrityError:
            pass
        else:
            raise SystemExit(label)

    expect_integrity_error(
        "category_score_reconstructions accepted incomplete provenance metadata",
        lambda: insert_reconstruction_categories(
            "2026-03-07", ("breadth",), reconstructed_at=""
        ),
    )
    expect_integrity_error(
        "category_score_reconstructions accepted a non-canonical extra category",
        lambda: insert_reconstruction_categories("2026-03-11", ("not_canonical",)),
    )
    expect_integrity_error(
        "pxi_score_reconstructions accepted an aggregate with no category rows",
        lambda: insert_reconstruction_pxi("2026-03-12"),
    )

    insert_reconstruction_categories("2026-03-13", canonical_categories[:-1])
    expect_integrity_error(
        "pxi_score_reconstructions accepted an aggregate missing a canonical category",
        lambda: insert_reconstruction_pxi("2026-03-13"),
    )

    insert_reconstruction_categories("2026-03-14", ("breadth",), method="method-a")
    expect_integrity_error(
        "category_score_reconstructions accepted mixed reconstruction metadata",
        lambda: insert_reconstruction_categories("2026-03-14", ("credit",), method="method-b"),
    )

    insert_reconstruction_categories("2026-03-15", method="method-a")
    expect_integrity_error(
        "pxi_score_reconstructions accepted metadata incompatible with its categories",
        lambda: insert_reconstruction_pxi("2026-03-15", method="method-b"),
    )

    connection.execute(
        """
        INSERT OR IGNORE INTO pxi_scores (date, score, label, status)
        VALUES ('2026-03-09', 62, 'neutral', 'neutral')
        """
    )
    expect_integrity_error(
        "pxi_score_reconstructions replaced an existing live-history date",
        lambda: insert_reconstruction_pxi("2026-03-09"),
    )

    insert_reconstruction_categories("2026-03-08")
    insert_reconstruction_pxi("2026-03-08")
    aggregate_category_count = connection.execute(
        "SELECT COUNT(*) FROM category_score_reconstructions WHERE date = '2026-03-08'"
    ).fetchone()
    if aggregate_category_count != (7,):
        raise SystemExit("Valid reconstruction aggregate did not contain exactly seven categories")

    connection.execute("PRAGMA recursive_triggers = OFF")
    reconstruction_replace_cases = (
        (
            "pxi_score_reconstructions",
            """
            INSERT OR REPLACE INTO pxi_score_reconstructions (
              date, score, label, status, history_origin,
              reconstructed_at, reconstruction_method,
              reconstruction_build_sha, source_data_as_of
            ) VALUES (
              '2026-03-08', 99, 'replacement', 'replacement',
              'retrospective_reconstruction', '2026-08-22T19:00:00.000Z',
              'replacement-method', 'ffeeddccbbaa', '2026-08-22T18:55:00.000Z'
            )
            """,
        ),
        (
            "category_score_reconstructions",
            """
            INSERT OR REPLACE INTO category_score_reconstructions (
              category, date, score, weight, weighted_score, history_origin,
              reconstructed_at, reconstruction_method,
              reconstruction_build_sha, source_data_as_of
            ) VALUES (
              'macro', '2026-03-08', 99, 0.25, 24.75,
              'retrospective_reconstruction', '2026-08-22T19:00:00.000Z',
              'replacement-method', 'ffeeddccbbaa', '2026-08-22T18:55:00.000Z'
            )
            """,
        ),
    )
    for table, statement in reconstruction_replace_cases:
        try:
            connection.execute(statement)
        except sqlite3.IntegrityError:
            pass
        else:
            raise SystemExit(
                f"{table} allowed INSERT OR REPLACE with recursive_triggers disabled"
            )
    preserved_pxi_score = connection.execute(
        "SELECT score FROM pxi_score_reconstructions WHERE date = '2026-03-08'"
    ).fetchone()
    preserved_category_score = connection.execute(
        """
        SELECT score FROM category_score_reconstructions
        WHERE category = 'macro' AND date = '2026-03-08'
        """
    ).fetchone()
    if preserved_pxi_score != (61.0,) or preserved_category_score != (62.0,):
        raise SystemExit("Rejected reconstruction REPLACE did not preserve original immutable rows")

    reverse_guard_cases = (
        (
            "pxi_scores INSERT",
            """
            INSERT OR REPLACE INTO pxi_scores (date, score, label, status)
            VALUES ('2026-03-08', 64, 'neutral', 'neutral')
            """,
        ),
        (
            "category_scores INSERT",
            """
            INSERT OR REPLACE INTO category_scores
              (category, date, score, weight, weighted_score)
            VALUES ('growth', '2026-03-08', 64, 0.25, 16)
            """,
        ),
        (
            "pxi_scores UPDATE",
            "UPDATE pxi_scores SET date = '2026-03-08' WHERE date = '2026-03-09'",
        ),
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO category_scores
          (category, date, score, weight, weighted_score)
        VALUES ('macro', '2026-03-10', 63, 0.25, 15.75)
        """
    )
    reverse_guard_cases += ((
        "category_scores UPDATE",
        "UPDATE category_scores SET date = '2026-03-08' WHERE date = '2026-03-10'",
    ),)
    for label, statement in reverse_guard_cases:
        try:
            connection.execute(statement)
        except sqlite3.IntegrityError:
            pass
        else:
            raise SystemExit(f"{label} silently superseded retrospective reconstruction history")
    connection.execute("DELETE FROM category_scores WHERE date = '2026-03-10'")

    for table in ("pxi_score_reconstructions", "category_score_reconstructions"):
        try:
            connection.execute(f"UPDATE {table} SET score = score + 1 WHERE date = '2026-03-08'")
        except sqlite3.IntegrityError:
            pass
        else:
            raise SystemExit(f"{table} allowed an update despite immutability trigger")
        try:
            connection.execute(f"DELETE FROM {table} WHERE date = '2026-03-08'")
        except sqlite3.IntegrityError:
            pass
        else:
            raise SystemExit(f"{table} allowed a delete despite immutability trigger")

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
                if path.name == "0006_score_history_provenance.sql":
                    connection.execute(
                        """
                        INSERT INTO pxi_scores (date, score, label, status)
                        VALUES
                          ('2026-03-05', 60, 'neutral', 'neutral'),
                          ('2026-03-06', 61, 'neutral', 'neutral')
                        """
                    )
                    connection.execute(
                        """
                        INSERT INTO category_scores (category, date, score, weight, weighted_score)
                        VALUES
                          ('macro', '2026-03-05', 60, 0.25, 15),
                          ('macro', '2026-03-06', 61, 0.25, 15.25)
                        """
                    )
                    connection.execute(
                        """
                        INSERT INTO market_refresh_runs (
                          started_at, completed_at, status, "trigger", as_of
                        ) VALUES
                          ('2026-03-05T22:00:00.000Z', '2026-03-05T22:01:00.000Z',
                           'success', 'manual_refresh', '2026-03-05T22:01:00.000Z'),
                          ('2026-03-06T22:00:00.000Z', '2026-03-06T22:01:00.000Z',
                           'success', 'cron_fast_pipeline', '2026-03-06T22:01:00.000Z')
                        """
                    )
                apply_sql_file(connection, path)
            assert_expected_schema(connection)
            promoted = connection.execute(
                """
                SELECT date, history_origin FROM pxi_scores
                WHERE date IN ('2026-03-05', '2026-03-06')
                ORDER BY date
                """
            ).fetchall()
            if promoted != [
                ("2026-03-05", "legacy_unclassified"),
                ("2026-03-06", "live_recorded"),
            ]:
                raise SystemExit(f"Provenance migration promotion was not fail-closed: {promoted!r}")
            promoted_categories = connection.execute(
                "SELECT date, history_origin FROM category_scores ORDER BY date"
            ).fetchall()
            if promoted_categories != [
                ("2026-03-05", "legacy_unclassified"),
                ("2026-03-06", "live_recorded"),
            ]:
                raise SystemExit(
                    f"Category provenance migration promotion was not fail-closed: {promoted_categories!r}"
                )
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
                # schema.sql already carries the 0006 columns and isolated
                # reconstruction tables; SQLite has no ADD COLUMN IF NOT EXISTS.
                if path.name != "0006_score_history_provenance.sql":
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
    assert_score_history_code_isolation()
    apply_migrations_to_empty_db()
    apply_migrations_to_current_schema_db()
