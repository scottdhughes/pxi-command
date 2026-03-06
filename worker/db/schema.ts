const BRIEF_CONTRACT_VERSION = '2026-02-17-v2';
const MARKET_SCHEMA_CACHE_MS = 5 * 60 * 1000;

let marketSchemaInitPromise: Promise<void> | null = null;
let marketSchemaInitializedAt = 0;

export async function tableHasColumn(
  db: D1Database,
  tableName: string,
  columnName: string
): Promise<boolean> {
  const rows = await db.prepare(`PRAGMA table_info(${tableName})`).all<{ name: string }>();
  return (rows.results || []).some((row) => row.name === columnName);
}

async function indexExists(
  db: D1Database,
  indexName: string,
): Promise<boolean> {
  const row = await db.prepare(`
    SELECT name
    FROM sqlite_master
    WHERE type = 'index'
      AND name = ?
    LIMIT 1
  `).bind(indexName).first<{ name: string }>();
  return Boolean(row?.name);
}

async function dedupeMarketAlertDeliveries(db: D1Database): Promise<void> {
  const duplicateGroups = await db.prepare(`
    SELECT event_id, channel, subscriber_id, COUNT(*) as duplicate_count
    FROM market_alert_deliveries
    WHERE subscriber_id IS NOT NULL
      AND channel = 'email'
    GROUP BY event_id, channel, subscriber_id
    HAVING COUNT(*) > 1
  `).all<{
    event_id: string;
    channel: 'email';
    subscriber_id: string;
    duplicate_count: number | null;
  }>();

  for (const group of duplicateGroups.results || []) {
    const rows = await db.prepare(`
      SELECT id, status, attempted_at
      FROM market_alert_deliveries
      WHERE event_id = ?
        AND channel = ?
        AND subscriber_id = ?
      ORDER BY
        CASE WHEN status = 'sent' THEN 0 ELSE 1 END,
        datetime(replace(replace(attempted_at, 'T', ' '), 'Z', '')) DESC,
        id DESC
    `).bind(group.event_id, group.channel, group.subscriber_id).all<{
      id: number;
      status: 'queued' | 'sent' | 'failed';
      attempted_at: string | null;
    }>();

    const keep = rows.results?.[0];
    if (!keep) continue;

    for (const row of rows.results?.slice(1) || []) {
      await db.prepare(`
        DELETE FROM market_alert_deliveries
        WHERE id = ?
      `).bind(row.id).run();
    }
  }
}

export async function ensureEmailAlertDeliveryUniqueness(db: D1Database): Promise<void> {
  const hasChannel = await tableHasColumn(db, 'market_alert_deliveries', 'channel');
  if (!hasChannel) {
    await db.prepare(`
      ALTER TABLE market_alert_deliveries
      ADD COLUMN channel TEXT NOT NULL DEFAULT 'email'
    `).run();
  }

  const uniqueIndexName = 'idx_market_alert_deliveries_email_unique';
  if (!(await indexExists(db, uniqueIndexName))) {
    await dedupeMarketAlertDeliveries(db);
  }

  await db.prepare(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_market_alert_deliveries_email_unique
    ON market_alert_deliveries(event_id, channel, subscriber_id)
    WHERE subscriber_id IS NOT NULL
  `).run();
}

export async function ensureMarketProductSchema(db: D1Database): Promise<void> {
  const now = Date.now();
  if (marketSchemaInitializedAt > 0 && (now - marketSchemaInitializedAt) < MARKET_SCHEMA_CACHE_MS) {
    return;
  }

  if (marketSchemaInitPromise) {
    await marketSchemaInitPromise;
    return;
  }

  marketSchemaInitPromise = (async () => {
    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_brief_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT NOT NULL UNIQUE,
        contract_version TEXT NOT NULL DEFAULT '${BRIEF_CONTRACT_VERSION}',
        payload_json TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    const hasContractVersion = await tableHasColumn(db, 'market_brief_snapshots', 'contract_version');
    if (!hasContractVersion) {
      await db.prepare(
        `ALTER TABLE market_brief_snapshots ADD COLUMN contract_version TEXT NOT NULL DEFAULT '${BRIEF_CONTRACT_VERSION}'`
      ).run();
    }
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_brief_as_of ON market_brief_snapshots(as_of DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS opportunity_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT NOT NULL,
        horizon TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(as_of, horizon)
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_opportunity_snapshots_lookup ON opportunity_snapshots(as_of DESC, horizon)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_opportunity_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        refresh_run_id INTEGER,
        as_of TEXT NOT NULL,
        horizon TEXT NOT NULL CHECK(horizon IN ('7d', '30d')),
        candidate_count INTEGER NOT NULL DEFAULT 0,
        published_count INTEGER NOT NULL DEFAULT 0,
        suppressed_count INTEGER NOT NULL DEFAULT 0,
        quality_filtered_count INTEGER NOT NULL DEFAULT 0,
        coherence_suppressed_count INTEGER NOT NULL DEFAULT 0,
        data_quality_suppressed_count INTEGER NOT NULL DEFAULT 0,
        degraded_reason TEXT,
        top_direction_candidate TEXT CHECK(top_direction_candidate IN ('bullish', 'bearish', 'neutral')),
        top_direction_published TEXT CHECK(top_direction_published IN ('bullish', 'bearish', 'neutral')),
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_created ON market_opportunity_ledger(created_at DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_as_of ON market_opportunity_ledger(as_of DESC, horizon)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_run ON market_opportunity_ledger(refresh_run_id, horizon)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_opportunity_item_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        refresh_run_id INTEGER,
        as_of TEXT NOT NULL,
        horizon TEXT NOT NULL CHECK(horizon IN ('7d', '30d')),
        opportunity_id TEXT NOT NULL,
        theme_id TEXT NOT NULL,
        theme_name TEXT NOT NULL,
        direction TEXT NOT NULL CHECK(direction IN ('bullish', 'bearish', 'neutral')),
        conviction_score INTEGER NOT NULL,
        published INTEGER NOT NULL,
        suppression_reason TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(as_of, horizon, opportunity_id)
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_asof_horizon ON market_opportunity_item_ledger(as_of DESC, horizon)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_theme_horizon_asof ON market_opportunity_item_ledger(theme_id, horizon, as_of DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_published_created ON market_opportunity_item_ledger(published, created_at DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_decision_impact_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT NOT NULL,
        horizon TEXT NOT NULL CHECK(horizon IN ('7d', '30d')),
        scope TEXT NOT NULL CHECK(scope IN ('market', 'theme')),
        window_days INTEGER NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(as_of, horizon, scope, window_days)
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_decision_impact_lookup ON market_decision_impact_snapshots(scope, horizon, window_days, as_of DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_calibration_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT NOT NULL,
        metric TEXT NOT NULL,
        horizon TEXT,
        payload_json TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(as_of, metric, horizon)
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_calibration_lookup ON market_calibration_snapshots(metric, horizon, as_of DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_alert_events (
        id TEXT PRIMARY KEY,
        event_type TEXT NOT NULL,
        severity TEXT NOT NULL CHECK(severity IN ('info', 'warning', 'critical')),
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        entity_type TEXT NOT NULL CHECK(entity_type IN ('market', 'theme', 'indicator')),
        entity_id TEXT,
        dedupe_key TEXT NOT NULL UNIQUE,
        payload_json TEXT,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_events_created ON market_alert_events(created_at DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_events_type ON market_alert_events(event_type, created_at DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_alert_deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL,
        channel TEXT NOT NULL CHECK(channel IN ('in_app', 'email')),
        subscriber_id TEXT,
        status TEXT NOT NULL CHECK(status IN ('queued', 'sent', 'failed')),
        provider_id TEXT,
        error TEXT,
        attempted_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_deliveries_event ON market_alert_deliveries(event_id, attempted_at DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_deliveries_subscriber ON market_alert_deliveries(subscriber_id, attempted_at DESC)`).run();
    await ensureEmailAlertDeliveryUniqueness(db);

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_consistency_checks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT NOT NULL UNIQUE,
        score REAL NOT NULL,
        state TEXT NOT NULL,
        violations_json TEXT NOT NULL,
        components_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    const hasComponentsJson = await tableHasColumn(db, 'market_consistency_checks', 'components_json');
    if (!hasComponentsJson) {
      await db.prepare(
        `ALTER TABLE market_consistency_checks ADD COLUMN components_json TEXT NOT NULL DEFAULT '{}'`
      ).run();
    }
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_consistency_created ON market_consistency_checks(created_at DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_refresh_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at TEXT NOT NULL,
        completed_at TEXT,
        status TEXT NOT NULL CHECK(status IN ('running', 'success', 'failed')),
        "trigger" TEXT NOT NULL DEFAULT 'unknown',
        brief_generated INTEGER DEFAULT 0,
        opportunities_generated INTEGER DEFAULT 0,
        calibrations_generated INTEGER DEFAULT 0,
        alerts_generated INTEGER DEFAULT 0,
        stale_count INTEGER,
        critical_stale_count INTEGER,
        as_of TEXT,
        error TEXT,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    const hasCriticalStaleCount = await tableHasColumn(db, 'market_refresh_runs', 'critical_stale_count');
    if (!hasCriticalStaleCount) {
      await db.prepare(
        `ALTER TABLE market_refresh_runs ADD COLUMN critical_stale_count INTEGER`
      ).run();
    }
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_refresh_runs_completed ON market_refresh_runs(status, completed_at DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_refresh_runs_created ON market_refresh_runs(created_at DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS market_utility_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        route TEXT,
        actionability_state TEXT,
        payload_json TEXT,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_utility_events_created ON market_utility_events(created_at DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_utility_events_type ON market_utility_events(event_type, created_at DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_market_utility_events_session ON market_utility_events(session_id, created_at DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS email_subscribers (
        id TEXT PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('pending', 'active', 'unsubscribed', 'bounced')),
        cadence TEXT NOT NULL DEFAULT 'daily_8am_et',
        types_json TEXT NOT NULL,
        timezone TEXT NOT NULL DEFAULT 'America/New_York',
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_email_subscribers_status ON email_subscribers(status)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_email_subscribers_updated ON email_subscribers(updated_at DESC)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS email_verification_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL,
        token_hash TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        used_at TEXT,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_email_verification_email_expires ON email_verification_tokens(email, expires_at DESC)`).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_email_verification_hash ON email_verification_tokens(token_hash)`).run();

    await db.prepare(`
      CREATE TABLE IF NOT EXISTS email_unsubscribe_tokens (
        subscriber_id TEXT PRIMARY KEY,
        token_hash TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `).run();
    await db.prepare(`CREATE INDEX IF NOT EXISTS idx_email_unsubscribe_hash ON email_unsubscribe_tokens(token_hash)`).run();

    marketSchemaInitializedAt = Date.now();
  })();

  try {
    await marketSchemaInitPromise;
  } finally {
    marketSchemaInitPromise = null;
  }
}
