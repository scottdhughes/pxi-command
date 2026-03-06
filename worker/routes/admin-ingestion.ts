import { ensureEmailAlertDeliveryUniqueness } from '../db/schema';
import type {
  BackfillResponsePayload,
  MigrationResponsePayload,
  RecalculateAllSignalsResponsePayload,
  RecalculateResponsePayload,
  RefreshIngestionResponsePayload,
  WorkerRouteContext,
  WriteResponsePayload,
} from '../types';

type AdminIngestionDeps = Record<string, any>;

export async function tryHandleAdminIngestionRoute(
  route: WorkerRouteContext,
  deps: AdminIngestionDeps,
): Promise<Response | null> {
  const { request, env, url, method, corsHeaders, clientIP } = route;

  if (url.pathname === '/api/migrate' && method === 'POST') {
    const adminAuthFailure = await deps.enforceAdminAuth(request, env, corsHeaders, clientIP);
    if (adminAuthFailure) {
      return adminAuthFailure;
    }

    const migrations: string[] = [];

    try {
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS prediction_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          prediction_date TEXT NOT NULL UNIQUE,
          target_date_7d TEXT,
          target_date_30d TEXT,
          current_score REAL NOT NULL,
          predicted_change_7d REAL,
          predicted_change_30d REAL,
          actual_change_7d REAL,
          actual_change_30d REAL,
          confidence_7d REAL,
          confidence_30d REAL,
          similar_periods TEXT,
          evaluated_at TEXT,
          created_at TEXT DEFAULT (datetime('now'))
        )
      `).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_prediction_log_date ON prediction_log(prediction_date DESC)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_prediction_log_evaluated ON prediction_log(evaluated_at)`).run();
      migrations.push('prediction_log');
    } catch (error) {
      console.error('prediction_log migration failed:', error);
    }

    try {
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS model_params (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          param_key TEXT NOT NULL UNIQUE,
          param_value REAL NOT NULL,
          notes TEXT,
          updated_at TEXT DEFAULT (datetime('now'))
        )
      `).run();
      migrations.push('model_params');
      await env.DB.prepare(`
        INSERT OR IGNORE INTO model_params (param_key, param_value, notes)
        VALUES ('accuracy_weight', 0.3, 'Weight given to period accuracy vs similarity')
      `).run();
      await env.DB.prepare(`
        INSERT OR IGNORE INTO model_params (param_key, param_value, notes)
        VALUES ('similarity_threshold', 0.8, 'Minimum cosine similarity to include period')
      `).run();
      await env.DB.prepare(`
        INSERT OR IGNORE INTO model_params (param_key, param_value, notes)
        VALUES ('bucket_threshold_1', 20, 'Threshold between bucket 1 (0-X) and bucket 2')
      `).run();
      await env.DB.prepare(`
        INSERT OR IGNORE INTO model_params (param_key, param_value, notes)
        VALUES ('bucket_threshold_2', 40, 'Threshold between bucket 2 and bucket 3')
      `).run();
      await env.DB.prepare(`
        INSERT OR IGNORE INTO model_params (param_key, param_value, notes)
        VALUES ('bucket_threshold_3', 60, 'Threshold between bucket 3 and bucket 4')
      `).run();
      await env.DB.prepare(`
        INSERT OR IGNORE INTO model_params (param_key, param_value, notes)
        VALUES ('bucket_threshold_4', 80, 'Threshold between bucket 4 and bucket 5 (X-100)')
      `).run();
    } catch (error) {
      console.error('model_params migration failed:', error);
    }

    try {
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS period_accuracy (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          period_date TEXT NOT NULL UNIQUE,
          times_used INTEGER DEFAULT 0,
          correct_predictions INTEGER DEFAULT 0,
          total_predictions INTEGER DEFAULT 0,
          mean_absolute_error REAL,
          accuracy_score REAL,
          updated_at TEXT DEFAULT (datetime('now'))
        )
      `).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_period_accuracy_date ON period_accuracy(period_date DESC)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_period_accuracy_score ON period_accuracy(accuracy_score DESC)`).run();
      migrations.push('period_accuracy');
    } catch (error) {
      console.error('period_accuracy migration failed:', error);
    }

    try {
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS alerts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          date TEXT NOT NULL,
          alert_type TEXT NOT NULL,
          message TEXT NOT NULL,
          severity TEXT NOT NULL DEFAULT 'info',
          acknowledged INTEGER DEFAULT 0,
          pxi_score REAL,
          forward_return_7d REAL,
          forward_return_30d REAL,
          created_at TEXT DEFAULT (datetime('now'))
        )
      `).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_alerts_date ON alerts(date DESC)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_alerts_type ON alerts(alert_type)`).run();
      try {
        await env.DB.prepare(`ALTER TABLE alerts ADD COLUMN pxi_score REAL`).run();
      } catch {}
      try {
        await env.DB.prepare(`ALTER TABLE alerts ADD COLUMN forward_return_7d REAL`).run();
      } catch {}
      try {
        await env.DB.prepare(`ALTER TABLE alerts ADD COLUMN forward_return_30d REAL`).run();
      } catch {}
      migrations.push('alerts');
    } catch (error) {
      console.error('alerts migration failed:', error);
    }

    try {
      await env.DB.prepare(`
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
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_email_subscribers_status ON email_subscribers(status)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_email_subscribers_updated ON email_subscribers(updated_at DESC)`).run();
      migrations.push('email_subscribers');
    } catch (error) {
      console.error('email_subscribers migration failed:', error);
    }

    try {
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS email_verification_tokens (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email TEXT NOT NULL,
          token_hash TEXT NOT NULL,
          expires_at TEXT NOT NULL,
          used_at TEXT,
          created_at TEXT DEFAULT (datetime('now'))
        )
      `).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_email_verification_email_expires ON email_verification_tokens(email, expires_at DESC)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_email_verification_hash ON email_verification_tokens(token_hash)`).run();
      migrations.push('email_verification_tokens');
    } catch (error) {
      console.error('email_verification_tokens migration failed:', error);
    }

    try {
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS email_unsubscribe_tokens (
          subscriber_id TEXT PRIMARY KEY,
          token_hash TEXT NOT NULL,
          created_at TEXT DEFAULT (datetime('now'))
        )
      `).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_email_unsubscribe_hash ON email_unsubscribe_tokens(token_hash)`).run();
      migrations.push('email_unsubscribe_tokens');
    } catch (error) {
      console.error('email_unsubscribe_tokens migration failed:', error);
    }

    try {
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS market_brief_snapshots (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          as_of TEXT NOT NULL UNIQUE,
          contract_version TEXT NOT NULL DEFAULT '2026-02-17-v2',
          payload_json TEXT NOT NULL,
          created_at TEXT DEFAULT (datetime('now'))
        )
      `).run();
      try {
        await env.DB.prepare(
          `ALTER TABLE market_brief_snapshots ADD COLUMN contract_version TEXT NOT NULL DEFAULT '2026-02-17-v2'`
        ).run();
      } catch {}
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_brief_as_of ON market_brief_snapshots(as_of DESC)`).run();
      migrations.push('market_brief_snapshots');
    } catch (error) {
      console.error('market_brief_snapshots migration failed:', error);
    }

    try {
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS opportunity_snapshots (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          as_of TEXT NOT NULL,
          horizon TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          created_at TEXT DEFAULT (datetime('now')),
          UNIQUE(as_of, horizon)
        )
      `).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_opportunity_snapshots_lookup ON opportunity_snapshots(as_of DESC, horizon)`).run();
      migrations.push('opportunity_snapshots');
    } catch (error) {
      console.error('opportunity_snapshots migration failed:', error);
    }

    try {
      await env.DB.prepare(`
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
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_created ON market_opportunity_ledger(created_at DESC)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_as_of ON market_opportunity_ledger(as_of DESC, horizon)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opportunity_ledger_run ON market_opportunity_ledger(refresh_run_id, horizon)`).run();
      migrations.push('market_opportunity_ledger');
    } catch (error) {
      console.error('market_opportunity_ledger migration failed:', error);
    }

    try {
      await env.DB.prepare(`
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
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_asof_horizon ON market_opportunity_item_ledger(as_of DESC, horizon)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_theme_horizon_asof ON market_opportunity_item_ledger(theme_id, horizon, as_of DESC)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_opp_item_ledger_published_created ON market_opportunity_item_ledger(published, created_at DESC)`).run();
      migrations.push('market_opportunity_item_ledger');
    } catch (error) {
      console.error('market_opportunity_item_ledger migration failed:', error);
    }

    try {
      await env.DB.prepare(`
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
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_refresh_runs_completed ON market_refresh_runs(status, completed_at DESC)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_refresh_runs_created ON market_refresh_runs(created_at DESC)`).run();
      migrations.push('market_refresh_runs');
    } catch (error) {
      console.error('market_refresh_runs migration failed:', error);
    }

    try {
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS market_consistency_checks (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          as_of TEXT NOT NULL,
          state TEXT NOT NULL CHECK(state IN ('PASS', 'WARN', 'FAIL')),
          score REAL NOT NULL,
          payload_json TEXT NOT NULL,
          created_at TEXT DEFAULT (datetime('now'))
        )
      `).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_consistency_checks_as_of ON market_consistency_checks(as_of DESC)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_consistency_checks_created ON market_consistency_checks(created_at DESC)`).run();
      migrations.push('market_consistency_checks');
    } catch (error) {
      console.error('market_consistency_checks migration failed:', error);
    }

    try {
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS calibration_snapshots (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          as_of TEXT NOT NULL,
          metric TEXT NOT NULL,
          horizon TEXT,
          payload_json TEXT NOT NULL,
          created_at TEXT DEFAULT (datetime('now'))
        )
      `).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_calibration_snapshots_lookup ON calibration_snapshots(metric, horizon, as_of DESC)`).run();
      migrations.push('calibration_snapshots');
    } catch (error) {
      console.error('calibration_snapshots migration failed:', error);
    }

    try {
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS decision_impact_snapshots (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          as_of TEXT NOT NULL,
          horizon TEXT NOT NULL CHECK(horizon IN ('7d', '30d')),
          scope TEXT NOT NULL CHECK(scope IN ('market', 'theme')),
          window_days INTEGER NOT NULL CHECK(window_days IN (30, 90)),
          payload_json TEXT NOT NULL,
          created_at TEXT DEFAULT (datetime('now')),
          UNIQUE(as_of, horizon, scope, window_days)
        )
      `).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_decision_impact_snapshots_lookup ON decision_impact_snapshots(horizon, scope, window_days, as_of DESC)`).run();
      migrations.push('decision_impact_snapshots');
    } catch (error) {
      console.error('decision_impact_snapshots migration failed:', error);
    }

    try {
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS market_alert_events (
          id TEXT PRIMARY KEY,
          event_type TEXT NOT NULL,
          severity TEXT NOT NULL,
          title TEXT NOT NULL,
          body TEXT NOT NULL,
          entity_type TEXT NOT NULL,
          entity_id TEXT,
          dedupe_key TEXT NOT NULL UNIQUE,
          payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
      `).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_events_created ON market_alert_events(created_at DESC)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_events_type ON market_alert_events(event_type, created_at DESC)`).run();
      migrations.push('market_alert_events');
    } catch (error) {
      console.error('market_alert_events migration failed:', error);
    }

    try {
      await env.DB.prepare(`
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
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_deliveries_event ON market_alert_deliveries(event_id, attempted_at DESC)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_alert_deliveries_subscriber ON market_alert_deliveries(subscriber_id, attempted_at DESC)`).run();
      await ensureEmailAlertDeliveryUniqueness(env.DB);
      migrations.push('market_alert_deliveries');
    } catch (error) {
      console.error('market_alert_deliveries migration failed:', error);
    }

    try {
      await env.DB.prepare(`
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
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_utility_events_created ON market_utility_events(created_at DESC)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_utility_events_type ON market_utility_events(event_type, created_at DESC)`).run();
      await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_market_utility_events_session ON market_utility_events(session_id, created_at DESC)`).run();
      migrations.push('market_utility_events');
    } catch (error) {
      console.error('market_utility_events migration failed:', error);
    }

    const payload: MigrationResponsePayload = {
      success: true,
      tables_created: migrations,
      message: `Migration complete. Created/verified: ${migrations.join(', ')}`,
    };
    return Response.json(payload, { headers: corsHeaders });
  }

  if (url.pathname === '/api/write' && method === 'POST') {
    const adminAuthFailure = await deps.enforceAdminAuth(request, env, corsHeaders, clientIP);
    if (adminAuthFailure) {
      return adminAuthFailure;
    }

    const body = await request.json() as {
      type?: 'indicator' | 'category' | 'pxi';
      data?: any;
      indicators?: { indicator_id: string; date: string; value: number; source: string }[];
      categories?: { category: string; date: string; score: number; weight: number; weighted_score: number }[];
      pxi?: { date: string; score: number; label: string; status: string; delta_1d: number | null; delta_7d: number | null; delta_30d: number | null };
    };

    const stmts: D1PreparedStatement[] = [];

    if (body.type) {
      if (body.type === 'indicator') {
        const { indicator_id, date, value, source } = body.data;
        stmts.push(env.DB.prepare(`
          INSERT OR REPLACE INTO indicator_values (indicator_id, date, value, source)
          VALUES (?, ?, ?, ?)
        `).bind(indicator_id, date, value, source));
      } else if (body.type === 'category') {
        const { category, date, score, weight, weighted_score } = body.data;
        stmts.push(env.DB.prepare(`
          INSERT OR REPLACE INTO category_scores (category, date, score, weight, weighted_score)
          VALUES (?, ?, ?, ?, ?)
        `).bind(category, date, score, weight, weighted_score));
      } else if (body.type === 'pxi') {
        const { date, score, label, status, delta_1d, delta_7d, delta_30d } = body.data;
        stmts.push(env.DB.prepare(`
          INSERT OR REPLACE INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `).bind(date, score, label, status, delta_1d, delta_7d, delta_30d));
      }
    }

    for (const indicator of body.indicators || []) {
      stmts.push(env.DB.prepare(`
        INSERT OR REPLACE INTO indicator_values (indicator_id, date, value, source)
        VALUES (?, ?, ?, ?)
      `).bind(indicator.indicator_id, indicator.date, indicator.value, indicator.source));
    }

    for (const category of body.categories || []) {
      stmts.push(env.DB.prepare(`
        INSERT OR REPLACE INTO category_scores (category, date, score, weight, weighted_score)
        VALUES (?, ?, ?, ?, ?)
      `).bind(category.category, category.date, category.score, category.weight, category.weighted_score));
    }

    if (body.pxi) {
      stmts.push(env.DB.prepare(`
        INSERT OR REPLACE INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `).bind(
        body.pxi.date,
        body.pxi.score,
        body.pxi.label,
        body.pxi.status,
        body.pxi.delta_1d,
        body.pxi.delta_7d,
        body.pxi.delta_30d,
      ));
    }

    const batchSize = 100;
    let totalWritten = 0;
    for (let index = 0; index < stmts.length; index += batchSize) {
      const batch = stmts.slice(index, index + batchSize);
      await env.DB.batch(batch);
      totalWritten += batch.length;
    }

    if (body.pxi) {
      const indicators = await env.DB.prepare(`
        SELECT indicator_id, value FROM indicator_values
        WHERE date = ? ORDER BY indicator_id
      `).bind(body.pxi.date).all<{ indicator_id: string; value: number }>();

      if (indicators.results && indicators.results.length >= 10) {
        try {
          const indicatorText = indicators.results
            .map((indicator) => `${indicator.indicator_id}: ${indicator.value.toFixed(2)}`)
            .join(', ');

          const embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
            text: indicatorText,
          });
          const embeddingVector = deps.getEmbeddingVector(embedding);

          await env.VECTORIZE.upsert([{
            id: body.pxi.date,
            values: embeddingVector,
            metadata: { date: body.pxi.date, score: body.pxi.score },
          }]);
        } catch (error) {
          console.error('Embedding generation failed:', error);
        }
      }
    }

    const payload: WriteResponsePayload = { success: true, written: totalWritten };
    return Response.json(payload, { headers: corsHeaders });
  }

  if (url.pathname === '/api/refresh' && method === 'POST') {
    const adminAuthFailure = await deps.enforceAdminAuth(request, env, corsHeaders, clientIP);
    if (adminAuthFailure) {
      return adminAuthFailure;
    }

    if (!env.FRED_API_KEY) {
      return Response.json({ error: 'FRED_API_KEY not configured' }, { status: 500, headers: corsHeaders });
    }

    const indicators = await deps.fetchAllIndicators(env.FRED_API_KEY);

    const batchSize = 100;
    let written = 0;
    for (let index = 0; index < indicators.length; index += batchSize) {
      const batch = indicators.slice(index, index + batchSize);
      const stmts = batch.map((indicator: any) =>
        env.DB.prepare(`
          INSERT OR REPLACE INTO indicator_values (indicator_id, date, value, source)
          VALUES (?, ?, ?, ?)
        `).bind(indicator.indicator_id, indicator.date, indicator.value, indicator.source)
      );
      await env.DB.batch(stmts);
      written += batch.length;
    }

    const today = deps.formatDate(new Date());
    const result = await deps.calculatePXI(env.DB, today);

    if (result) {
      await env.DB.prepare(`
        INSERT OR REPLACE INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `).bind(
        result.pxi.date, result.pxi.score, result.pxi.label, result.pxi.status,
        result.pxi.delta_1d, result.pxi.delta_7d, result.pxi.delta_30d,
      ).run();

      const catStmts = result.categories.map((category: any) =>
        env.DB.prepare(`
          INSERT OR REPLACE INTO category_scores (category, date, score, weight, weighted_score)
          VALUES (?, ?, ?, ?, ?)
        `).bind(category.category, category.date, category.score, category.weight, category.weighted_score)
      );
      if (catStmts.length > 0) {
        await env.DB.batch(catStmts);
      }
    }

    const payload: RefreshIngestionResponsePayload = {
      success: true,
      indicators_fetched: indicators.length,
      indicators_written: written,
      pxi: result ? { date: today, score: result.pxi.score, label: result.pxi.label, categories: result.categories.length } : null,
    };
    return Response.json(payload, { headers: corsHeaders });
  }

  if (url.pathname === '/api/recalculate' && method === 'POST') {
    const adminAuthFailure = await deps.enforceAdminAuth(request, env, corsHeaders, clientIP);
    if (adminAuthFailure) {
      return adminAuthFailure;
    }

    const body = await request.json() as { date?: string };
    const targetDate = body.date || deps.formatDate(new Date());
    const result = await deps.calculatePXI(env.DB, targetDate);

    if (!result) {
      return Response.json({ error: 'Insufficient data for calculation', date: targetDate }, { status: 400, headers: corsHeaders });
    }

    await env.DB.prepare(`
      INSERT OR REPLACE INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).bind(
      result.pxi.date,
      result.pxi.score,
      result.pxi.label,
      result.pxi.status,
      result.pxi.delta_1d,
      result.pxi.delta_7d,
      result.pxi.delta_30d,
    ).run();

    const catStmts = result.categories.map((category: any) =>
      env.DB.prepare(`
        INSERT OR REPLACE INTO category_scores (category, date, score, weight, weighted_score)
        VALUES (?, ?, ?, ?, ?)
      `).bind(category.category, category.date, category.score, category.weight, category.weighted_score)
    );
    if (catStmts.length > 0) {
      await env.DB.batch(catStmts);
    }

    let embedded = false;
    try {
      const indicators = await env.DB.prepare(`
        SELECT indicator_id, value FROM indicator_values WHERE date = ? ORDER BY indicator_id
      `).bind(targetDate).all<{ indicator_id: string; value: number }>();

      if (indicators.results && indicators.results.length >= 5) {
        const embeddingText = deps.generateEmbeddingText({
          indicators: indicators.results,
          pxi: {
            score: result.pxi.score,
            delta_7d: result.pxi.delta_7d,
            delta_30d: result.pxi.delta_30d,
          },
          categories: result.categories.map((category: any) => ({ category: category.category, score: category.score })),
        });

        const embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
          text: embeddingText,
        });
        const embeddingVector = deps.getEmbeddingVector(embedding);

        await env.VECTORIZE.upsert([{
          id: targetDate,
          values: embeddingVector,
          metadata: { date: targetDate, score: result.pxi.score, label: result.pxi.label },
        }]);
        embedded = true;
      }
    } catch (error) {
      console.error('Embedding generation failed during recalculation:', error);
    }

    const payload: RecalculateResponsePayload = {
      success: true,
      date: targetDate,
      pxi: result.pxi,
      categories: result.categories.length,
      embedded,
    };
    return Response.json(payload, { headers: corsHeaders });
  }

  if (url.pathname === '/api/backfill' && method === 'POST') {
    const adminAuthFailure = await deps.enforceAdminAuth(request, env, corsHeaders, clientIP);
    if (adminAuthFailure) {
      return adminAuthFailure;
    }

    const body = await deps.parseJsonBody(request);
    if (!body) {
      return Response.json({ error: 'Invalid JSON body' }, { status: 400, headers: corsHeaders });
    }

    const range = deps.parseBackfillDateRange(body.start, body.end);
    if (!range) {
      return Response.json({
        error: 'Invalid date range. Use ISO dates and ensure start <= end.',
      }, { status: 400, headers: corsHeaders });
    }

    const limit = deps.parseBackfillLimit(body.limit);
    const refreshProducts = body.refresh_products !== false;
    const includeDecisionImpact = body.include_decision_impact !== false;
    const includeDecisionGrade = body.include_decision_grade !== false;

    const refreshRunId = await deps.recordMarketRefreshRunStart(
      env.DB,
      `backfill ${range.start} -> ${range.end}`,
    );

    try {
      const results: Array<{ date: string; error?: string }> = [];
      let succeeded = 0;
      let embedded = 0;

      const startDate = new Date(`${range.start}T00:00:00.000Z`);
      const endDate = new Date(`${range.end}T00:00:00.000Z`);
      const dates: string[] = [];
      for (let cursor = new Date(startDate); cursor <= endDate; cursor.setUTCDate(cursor.getUTCDate() + 1)) {
        dates.push(deps.formatDate(cursor));
        if (dates.length >= limit) break;
      }

      for (const date of dates) {
        try {
          const result = await deps.calculatePXI(env.DB, date);
          if (!result) {
            results.push({ date, error: 'Insufficient data for calculation' });
            continue;
          }

          await env.DB.prepare(`
            INSERT OR REPLACE INTO pxi_scores (date, score, label, status, delta_1d, delta_7d, delta_30d)
            VALUES (?, ?, ?, ?, ?, ?, ?)
          `).bind(
            result.pxi.date,
            result.pxi.score,
            result.pxi.label,
            result.pxi.status,
            result.pxi.delta_1d,
            result.pxi.delta_7d,
            result.pxi.delta_30d,
          ).run();

          const catStmts = result.categories.map((category: any) =>
            env.DB.prepare(`
              INSERT OR REPLACE INTO category_scores (category, date, score, weight, weighted_score)
              VALUES (?, ?, ?, ?, ?)
            `).bind(category.category, category.date, category.score, category.weight, category.weighted_score)
          );
          if (catStmts.length > 0) {
            await env.DB.batch(catStmts);
          }

          const indicators = await env.DB.prepare(`
            SELECT indicator_id, value FROM indicator_values WHERE date = ? ORDER BY indicator_id
          `).bind(date).all<{ indicator_id: string; value: number }>();

          if (indicators.results && indicators.results.length >= 5) {
            try {
              const embeddingText = deps.generateEmbeddingText({
                indicators: indicators.results,
                pxi: {
                  score: result.pxi.score,
                  delta_7d: result.pxi.delta_7d,
                  delta_30d: result.pxi.delta_30d,
                },
                categories: result.categories.map((category: any) => ({ category: category.category, score: category.score })),
              });

              const embedding = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
                text: embeddingText,
              });
              const embeddingVector = deps.getEmbeddingVector(embedding);

              await env.VECTORIZE.upsert([{
                id: date,
                values: embeddingVector,
                metadata: { date, score: result.pxi.score, label: result.pxi.label },
              }]);
              embedded += 1;
            } catch (error) {
              console.error(`Embedding failed for ${date}:`, error);
            }
          }

          succeeded += 1;
          results.push({ date });
        } catch (error) {
          results.push({ date, error: error instanceof Error ? error.message : 'Unknown error' });
        }
      }

      if (refreshProducts) {
        try {
          await deps.ensureMarketProductSchema(env.DB);
        } catch (error) {
          console.warn('Backfill product schema guard failed:', error);
        }
      }

      await deps.recordMarketRefreshRunFinish(env.DB, refreshRunId, {
        status: 'success',
        as_of: range.end,
        error: null,
      });

      const payload: BackfillResponsePayload = {
        success: true,
        run_id: refreshRunId,
        start: range.start,
        end: range.end,
        requested_limit: limit,
        refresh_products: refreshProducts,
        include_decision_impact: includeDecisionImpact,
        include_decision_grade: includeDecisionGrade,
        succeeded,
        embedded,
        results,
      };

      return Response.json(payload, { headers: corsHeaders });
    } catch (error) {
      await deps.recordMarketRefreshRunFinish(
        env.DB,
        refreshRunId,
        {
          status: 'failed',
          as_of: range.end,
          error: error instanceof Error ? error.message : 'Unknown backfill failure',
        },
      );
      throw error;
    }
  }

  if (url.pathname === '/api/recalculate-all-signals' && method === 'POST') {
    const adminAuthFailure = await deps.enforceAdminAuth(request, env, corsHeaders, clientIP);
    if (adminAuthFailure) {
      return adminAuthFailure;
    }

    try {
      const pxiScores = await env.DB.prepare(`
        SELECT date, score, delta_7d, delta_30d FROM pxi_scores ORDER BY date ASC
      `).all<{ date: string; score: number; delta_7d: number | null; delta_30d: number | null }>();

      if (!pxiScores.results || pxiScores.results.length === 0) {
        return Response.json({ error: 'No PXI scores found' }, { status: 400, headers: corsHeaders });
      }

      const allCatScores = await env.DB.prepare(`
        SELECT date, category, score FROM category_scores ORDER BY date
      `).all<{ date: string; category: string; score: number }>();

      const catScoresByDate = new Map<string, Array<{ score: number }>>();
      for (const row of allCatScores.results || []) {
        if (!catScoresByDate.has(row.date)) {
          catScoresByDate.set(row.date, []);
        }
        catScoresByDate.get(row.date)!.push({ score: row.score });
      }

      const vixHistory = await env.DB.prepare(`
        SELECT date, value FROM indicator_values
        WHERE indicator_id = 'vix'
        ORDER BY date ASC
      `).all<{ date: string; value: number }>();

      const vixMap = new Map<string, number>();
      const vixValues: number[] = [];
      for (const row of vixHistory.results || []) {
        vixMap.set(row.date, row.value);
        vixValues.push(row.value);
      }
      const sortedVix = [...vixValues].sort((left, right) => left - right);

      const signals: Array<{
        date: string;
        pxi_level: number;
        delta_pxi_7d: number | null;
        delta_pxi_30d: number | null;
        category_dispersion: number;
        regime: string;
        volatility_percentile: number | null;
        risk_allocation: number;
        signal_type: string;
      }> = [];

      for (const pxi of pxiScores.results) {
        const vix = vixMap.get(pxi.date);
        let vixPercentile: number | null = null;
        if (vix !== undefined && sortedVix.length > 0) {
          const rank = sortedVix.filter((value) => value < vix).length +
            sortedVix.filter((value) => value === vix).length / 2;
          vixPercentile = (rank / sortedVix.length) * 100;
        }

        const catScores = catScoresByDate.get(pxi.date) || [];
        const scores = catScores.map((category) => category.score);
        const mean = scores.length > 0 ? scores.reduce((sum, value) => sum + value, 0) / scores.length : 50;
        const variance = scores.length > 0
          ? scores.reduce((sum, value) => sum + Math.pow(value - mean, 2), 0) / scores.length
          : 0;
        const dispersion = Math.sqrt(variance);

        let regime = 'TRANSITION';
        if (pxi.score >= 65) regime = 'RISK_ON';
        else if (pxi.score <= 35) regime = 'RISK_OFF';

        let allocation = 0.3 + (pxi.score / 100) * 0.7;
        if (regime === 'RISK_OFF') allocation *= 0.5;
        if (regime === 'TRANSITION') allocation *= 0.75;
        if (pxi.delta_7d !== null && pxi.delta_7d < -10) allocation *= 0.8;
        if (vixPercentile !== null && vixPercentile > 80) allocation *= 0.7;

        let signalType = 'FULL_RISK';
        if (allocation < 0.3) signalType = 'DEFENSIVE';
        else if (allocation < 0.5) signalType = 'RISK_OFF';
        else if (allocation < 0.8) signalType = 'REDUCED_RISK';

        signals.push({
          date: pxi.date,
          pxi_level: pxi.score,
          delta_pxi_7d: pxi.delta_7d,
          delta_pxi_30d: pxi.delta_30d,
          category_dispersion: Math.round(dispersion * 10) / 10,
          regime,
          volatility_percentile: vixPercentile !== null ? Math.round(vixPercentile) : null,
          risk_allocation: Math.round(allocation * 100) / 100,
          signal_type: signalType,
        });
      }

      const batchSize = 100;
      let processed = 0;
      for (let index = 0; index < signals.length; index += batchSize) {
        const batch = signals.slice(index, index + batchSize);
        const statements = batch.map((signal) =>
          env.DB.prepare(`
            INSERT OR REPLACE INTO pxi_signal
            (date, pxi_level, delta_pxi_7d, delta_pxi_30d, category_dispersion, regime, volatility_percentile, risk_allocation, signal_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
          `).bind(
            signal.date,
            signal.pxi_level,
            signal.delta_pxi_7d,
            signal.delta_pxi_30d,
            signal.category_dispersion,
            signal.regime,
            signal.volatility_percentile,
            signal.risk_allocation,
            signal.signal_type,
          )
        );
        await env.DB.batch(statements);
        processed += batch.length;
      }

      const payload: RecalculateAllSignalsResponsePayload = {
        success: true,
        processed,
        total: pxiScores.results.length,
        message: `Generated signal data for ${processed} dates`,
      };

      return Response.json(payload, { headers: corsHeaders });
    } catch (error) {
      console.error('Signal recalculation error:', error);
      return Response.json({
        error: 'Signal recalculation failed',
        details: error instanceof Error ? error.message : String(error),
      }, { status: 500, headers: corsHeaders });
    }
  }

  return null;
}
