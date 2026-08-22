-- Include the manual market-product backfill in the sole production mutation
-- lease. SQLite CHECK constraints require rebuilding this otherwise tiny table.
CREATE TABLE refresh_mutation_locks_v2 (
    lock_name TEXT PRIMARY KEY CHECK(lock_name = 'indicator_score_mutation'),
    holder_id TEXT NOT NULL CHECK(length(trim(holder_id)) BETWEEN 1 AND 200),
    holder_type TEXT NOT NULL CHECK(holder_type IN (
        'cloudflare_cron',
        'deploy_smoke',
        'github_daily_refresh',
        'history_reconstruction',
        'market_backfill',
        'deploy'
    )),
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    lease_version INTEGER NOT NULL DEFAULT 1 CHECK(lease_version >= 1),
    updated_at TEXT NOT NULL,
    CHECK(expires_at > acquired_at)
);

INSERT INTO refresh_mutation_locks_v2 (
    lock_name,
    holder_id,
    holder_type,
    acquired_at,
    expires_at,
    lease_version,
    updated_at
)
SELECT
    lock_name,
    holder_id,
    holder_type,
    acquired_at,
    expires_at,
    lease_version,
    updated_at
FROM refresh_mutation_locks;

DROP TABLE refresh_mutation_locks;
ALTER TABLE refresh_mutation_locks_v2 RENAME TO refresh_mutation_locks;
