import type { MarketAlertEvent } from '../types';

export async function insertMarketEvents(
  db: D1Database,
  events: MarketAlertEvent[],
  inAppEnabled: boolean,
): Promise<number> {
  let inserted = 0;

  for (const event of events) {
    const existing = await db.prepare(`
      SELECT id
      FROM market_alert_events
      WHERE dedupe_key = ?
      LIMIT 1
    `).bind(event.dedupe_key).first<{ id: string }>();

    const eventId = existing?.id || event.id;
    const isNew = !existing?.id;

    await db.prepare(`
      INSERT INTO market_alert_events
      (id, event_type, severity, title, body, entity_type, entity_id, dedupe_key, payload_json, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(dedupe_key) DO UPDATE SET
        event_type = excluded.event_type,
        severity = excluded.severity,
        title = excluded.title,
        body = excluded.body,
        entity_type = excluded.entity_type,
        entity_id = excluded.entity_id,
        payload_json = excluded.payload_json,
        created_at = excluded.created_at
    `).bind(
      eventId,
      event.event_type,
      event.severity,
      event.title,
      event.body,
      event.entity_type,
      event.entity_id,
      event.dedupe_key,
      event.payload_json,
      event.created_at,
    ).run();

    if (isNew) {
      inserted += 1;
      if (inAppEnabled) {
        await db.prepare(`
          INSERT INTO market_alert_deliveries (event_id, channel, status, attempted_at)
          VALUES (?, 'in_app', 'sent', datetime('now'))
        `).bind(eventId).run();
      }
    }
  }

  return inserted;
}
