import assert from 'node:assert/strict';
import test from 'node:test';

import {
  finalizeDigestDeliverySent,
  reserveDigestDeliveryForSubscriber,
} from './market-digest.js';

type DeliveryRow = {
  id: number;
  event_id: string;
  channel: 'email';
  subscriber_id: string;
  status: 'queued' | 'sent' | 'failed';
  provider_id: string | null;
  error: string | null;
  attempted_at: string;
};

function createDigestDb(initialRows: DeliveryRow[] = []) {
  const rows = [...initialRows];
  let nextId = rows.reduce((max, row) => Math.max(max, row.id), 0) + 1;

  const buildStatement = (sql: string, args: unknown[] = []) => ({
    bind: (...boundArgs: unknown[]) => buildStatement(sql, boundArgs),
    first: async <T>() => {
      if (sql.includes('FROM market_alert_deliveries') && sql.includes('subscriber_id = ?')) {
        const [eventId, subscriberId] = args as [string, string];
        const row = rows.find((candidate) =>
          candidate.event_id === eventId &&
          candidate.channel === 'email' &&
          candidate.subscriber_id === subscriberId,
        );
        if (!row) return null as T | null;
        if (sql.includes('SELECT id, status')) {
          return { id: row.id, status: row.status } as T;
        }
        return { status: row.status } as T;
      }

      return null as T | null;
    },
    all: async <T>() => ({ results: [] as T[] }),
    run: async () => {
      if (sql.includes('INSERT INTO market_alert_deliveries') && sql.includes("VALUES (?, 'email', ?, 'queued'")) {
        const [eventId, subscriberId] = args as [string, string];
        const existing = rows.find((candidate) =>
          candidate.event_id === eventId &&
          candidate.channel === 'email' &&
          candidate.subscriber_id === subscriberId,
        );
        if (existing) {
          throw new Error('UNIQUE constraint failed: market_alert_deliveries.event_id, channel, subscriber_id');
        }

        rows.push({
          id: nextId,
          event_id: eventId,
          channel: 'email',
          subscriber_id: subscriberId,
          status: 'queued',
          provider_id: null,
          error: null,
          attempted_at: new Date().toISOString(),
        });
        nextId += 1;
        return { success: true, meta: { changes: 1 } };
      }

      if (sql.includes("SET status = 'queued'") && sql.includes("WHERE id = ?")) {
        const [attemptedAt, id] = args as [string, number];
        const row = rows.find((candidate) => candidate.id === id && candidate.status === 'failed');
        if (!row) {
          return { success: true, meta: { changes: 0 } };
        }
        row.status = 'queued';
        row.provider_id = null;
        row.error = null;
        row.attempted_at = attemptedAt;
        return { success: true, meta: { changes: 1 } };
      }

      if (sql.includes("SET status = 'sent'")) {
        const [providerId, attemptedAt, eventId, subscriberId] = args as [string | null, string, string, string];
        const row = rows.find((candidate) =>
          candidate.event_id === eventId &&
          candidate.channel === 'email' &&
          candidate.subscriber_id === subscriberId,
        );
        assert.ok(row);
        row.status = 'sent';
        row.provider_id = providerId;
        row.error = null;
        row.attempted_at = attemptedAt;
        return { success: true, meta: { changes: 1 } };
      }

      throw new Error(`Unhandled SQL in market-digest test db: ${sql}`);
    },
  });

  return {
    rows,
    prepare(sql: string) {
      return buildStatement(sql);
    },
  };
}

test('reserveDigestDeliveryForSubscriber sends once for a new subscriber/day and skips after sent', async () => {
  const db = createDigestDb();

  const first = await reserveDigestDeliveryForSubscriber(db as unknown as D1Database, 'digest-2026-03-05', 'sub-1');
  assert.deepEqual(first, { action: 'send' });
  assert.equal(db.rows.length, 1);
  assert.equal(db.rows[0]?.status, 'queued');

  await finalizeDigestDeliverySent(db as unknown as D1Database, 'digest-2026-03-05', 'sub-1', 'provider-1');
  assert.equal(db.rows[0]?.status, 'sent');

  const second = await reserveDigestDeliveryForSubscriber(db as unknown as D1Database, 'digest-2026-03-05', 'sub-1');
  assert.deepEqual(second, { action: 'skip', status: 'sent' });
});

test('reserveDigestDeliveryForSubscriber skips when a sent row already exists', async () => {
  const db = createDigestDb([
    {
      id: 1,
      event_id: 'digest-2026-03-05',
      channel: 'email',
      subscriber_id: 'sub-1',
      status: 'sent',
      provider_id: 'provider-1',
      error: null,
      attempted_at: new Date().toISOString(),
    },
  ]);

  const result = await reserveDigestDeliveryForSubscriber(db as unknown as D1Database, 'digest-2026-03-05', 'sub-1');
  assert.deepEqual(result, { action: 'skip', status: 'sent' });
});

test('reserveDigestDeliveryForSubscriber skips when a queued row already exists', async () => {
  const db = createDigestDb([
    {
      id: 1,
      event_id: 'digest-2026-03-05',
      channel: 'email',
      subscriber_id: 'sub-1',
      status: 'queued',
      provider_id: null,
      error: null,
      attempted_at: new Date().toISOString(),
    },
  ]);

  const result = await reserveDigestDeliveryForSubscriber(db as unknown as D1Database, 'digest-2026-03-05', 'sub-1');
  assert.deepEqual(result, { action: 'skip', status: 'queued' });
});

test('reserveDigestDeliveryForSubscriber reclaims failed rows for one retry', async () => {
  const db = createDigestDb([
    {
      id: 2,
      event_id: 'digest-2026-03-05',
      channel: 'email',
      subscriber_id: 'sub-1',
      status: 'failed',
      provider_id: null,
      error: 'smtp timeout',
      attempted_at: new Date(Date.now() - 3_600_000).toISOString(),
    },
  ]);

  const result = await reserveDigestDeliveryForSubscriber(db as unknown as D1Database, 'digest-2026-03-05', 'sub-1');
  assert.deepEqual(result, { action: 'send' });
  assert.equal(db.rows[0]?.status, 'queued');

  const queuedRetry = await reserveDigestDeliveryForSubscriber(db as unknown as D1Database, 'digest-2026-03-05', 'sub-1');
  assert.deepEqual(queuedRetry, { action: 'skip', status: 'queued' });
});
