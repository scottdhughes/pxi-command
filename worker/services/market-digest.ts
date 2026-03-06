import { asIsoDate, asIsoDateTime, stableHash } from '../lib/market-primitives';
import type { BriefSnapshot, Env, MarketAlertEvent, OpportunitySnapshot } from '../types';

export type DigestDeliveryReservation =
  | {
      action: 'send';
    }
  | {
      action: 'skip';
      status: 'queued' | 'sent';
    };

export async function reserveDigestDeliveryForSubscriber(
  db: D1Database,
  eventId: string,
  subscriberId: string,
): Promise<DigestDeliveryReservation> {
  const current = await db.prepare(`
    SELECT id, status
    FROM market_alert_deliveries
    WHERE event_id = ?
      AND channel = 'email'
      AND subscriber_id = ?
    LIMIT 1
  `).bind(eventId, subscriberId).first<{
    id: number;
    status: 'queued' | 'sent' | 'failed';
  }>();

  if (!current) {
    try {
      await db.prepare(`
        INSERT INTO market_alert_deliveries
        (event_id, channel, subscriber_id, status, attempted_at)
        VALUES (?, 'email', ?, 'queued', datetime('now'))
      `).bind(eventId, subscriberId).run();
      return { action: 'send' };
    } catch (err) {
      console.warn('Digest delivery reservation insert raced or failed:', err);
      const retried = await db.prepare(`
        SELECT status
        FROM market_alert_deliveries
        WHERE event_id = ?
          AND channel = 'email'
          AND subscriber_id = ?
        LIMIT 1
      `).bind(eventId, subscriberId).first<{ status: 'queued' | 'sent' | 'failed' }>();

      if (retried?.status === 'queued' || retried?.status === 'sent') {
        return { action: 'skip', status: retried.status };
      }

      if (retried?.status === 'failed') {
        return reserveDigestDeliveryForSubscriber(db, eventId, subscriberId);
      }

      throw err;
    }
  }

  if (current.status === 'sent' || current.status === 'queued') {
    return {
      action: 'skip',
      status: current.status,
    };
  }

  const reclaimed = await db.prepare(`
    UPDATE market_alert_deliveries
    SET status = 'queued',
        provider_id = NULL,
        error = NULL,
        attempted_at = ?
    WHERE id = ?
      AND status = 'failed'
  `).bind(asIsoDateTime(new Date()), current.id).run();

  if (typeof reclaimed.meta?.changes === 'number' && reclaimed.meta.changes > 0) {
    return { action: 'send' };
  }

  const latest = await db.prepare(`
    SELECT status
    FROM market_alert_deliveries
    WHERE event_id = ?
      AND channel = 'email'
      AND subscriber_id = ?
    LIMIT 1
  `).bind(eventId, subscriberId).first<{ status: 'queued' | 'sent' | 'failed' }>();

  if (latest?.status === 'queued' || latest?.status === 'sent') {
    return {
      action: 'skip',
      status: latest.status,
    };
  }

  return { action: 'send' };
}

export async function finalizeDigestDeliverySent(
  db: D1Database,
  eventId: string,
  subscriberId: string,
  providerId?: string | null,
): Promise<void> {
  await db.prepare(`
    UPDATE market_alert_deliveries
    SET status = 'sent',
        provider_id = ?,
        error = NULL,
        attempted_at = ?
    WHERE event_id = ?
      AND channel = 'email'
      AND subscriber_id = ?
  `).bind(providerId || null, asIsoDateTime(new Date()), eventId, subscriberId).run();
}

export async function finalizeDigestDeliveryFailure(
  db: D1Database,
  eventId: string,
  subscriberId: string,
  error: string,
): Promise<void> {
  await db.prepare(`
    UPDATE market_alert_deliveries
    SET status = 'failed',
        provider_id = NULL,
        error = ?,
        attempted_at = ?
    WHERE event_id = ?
      AND channel = 'email'
      AND subscriber_id = ?
  `).bind(error, asIsoDateTime(new Date()), eventId, subscriberId).run();
}

function resolveFromAddress(env: Env): string {
  return (env.ALERTS_FROM_EMAIL || 'alerts@pxicommand.com').trim();
}

function buildMimeMessage(payload: { from: string; to: string; subject: string; html: string; text: string }): string {
  const boundary = `pxi-${stableHash(`${payload.to}:${payload.subject}:${Date.now()}`)}`;
  return [
    `From: ${payload.from}`,
    `To: ${payload.to}`,
    `Subject: ${payload.subject}`,
    'MIME-Version: 1.0',
    `Content-Type: multipart/alternative; boundary="${boundary}"`,
    '',
    `--${boundary}`,
    'Content-Type: text/plain; charset=UTF-8',
    'Content-Transfer-Encoding: 8bit',
    '',
    payload.text,
    '',
    `--${boundary}`,
    'Content-Type: text/html; charset=UTF-8',
    'Content-Transfer-Encoding: 8bit',
    '',
    payload.html,
    '',
    `--${boundary}--`,
    '',
  ].join('\r\n');
}

async function sendCloudflareEmail(
  env: Env,
  payload: { to: string; subject: string; html: string; text: string },
): Promise<{ ok: boolean; providerId?: string; error?: string }> {
  if (!env.EMAIL_OUTBOUND) {
    return { ok: false, error: 'Cloudflare email binding not configured' };
  }

  try {
    const { EmailMessage } = await import('cloudflare:email');
    const from = resolveFromAddress(env);
    const rawMime = buildMimeMessage({
      from,
      to: payload.to,
      subject: payload.subject,
      html: payload.html,
      text: payload.text,
    });
    const message = new EmailMessage(from, payload.to, rawMime);
    await env.EMAIL_OUTBOUND.send(message);
    const providerId = `cf-email:${stableHash(`${from}:${payload.to}:${payload.subject}`)}`;
    return { ok: true, providerId };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : String(err) };
  }
}

export async function sendDigestToSubscriber(
  env: Env,
  email: string,
  brief: BriefSnapshot | null,
  opportunities: OpportunitySnapshot | null,
  events: MarketAlertEvent[],
  unsubscribeToken: string,
): Promise<{ ok: boolean; providerId?: string; error?: string }> {
  const topOpportunities = opportunities?.items.slice(0, 3) || [];
  const recentEvents = events.slice(0, 10);
  const unsubscribeUrl = `https://pxicommand.com/inbox?unsubscribe_token=${encodeURIComponent(unsubscribeToken)}`;

  const summaryText = brief?.summary || 'PXI market brief unavailable for this run.';
  const eventLines = recentEvents.length > 0
    ? recentEvents.map((event) => `- [${event.severity}] ${event.title}: ${event.body}`).join('\n')
    : '- No market alerts in the last 24 hours.';
  const opportunityLines = topOpportunities.length > 0
    ? topOpportunities.map((item) => `- ${item.theme_name}: ${item.direction} (${item.conviction_score}/100)`).join('\n')
    : '- No opportunities available.';

  const text = [
    'PXI Daily Digest',
    '',
    `As of: ${brief?.as_of || asIsoDateTime(new Date())}`,
    '',
    'Market Brief',
    summaryText,
    '',
    'Top Opportunities',
    opportunityLines,
    '',
    'Last 24h Alerts',
    eventLines,
    '',
    `Unsubscribe: ${unsubscribeUrl}`,
  ].join('\n');

  const html = `
    <div style="font-family:Arial,sans-serif;max-width:640px;margin:0 auto;padding:20px;color:#111;">
      <h2>PXI Daily Digest</h2>
      <p><strong>As of:</strong> ${brief?.as_of || asIsoDateTime(new Date())}</p>
      <h3>Market Brief</h3>
      <p>${summaryText}</p>
      <h3>Top Opportunities</h3>
      <ul>${topOpportunities.map((item) => `<li><strong>${item.theme_name}</strong>: ${item.direction} (${item.conviction_score}/100)</li>`).join('')}</ul>
      <h3>Last 24h Alerts</h3>
      <ul>${recentEvents.map((event) => `<li><strong>[${event.severity}] ${event.title}</strong> - ${event.body}</li>`).join('')}</ul>
      <p><a href="${unsubscribeUrl}">Unsubscribe</a></p>
      <p style="font-size:12px;color:#555;">Not investment advice.</p>
    </div>
  `;

  const subject = `PXI Daily Digest • ${asIsoDate(new Date())}`;
  return sendCloudflareEmail(env, { to: email, subject, html, text });
}
