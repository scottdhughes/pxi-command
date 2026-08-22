const RATE_LIMIT_RETENTION_SECONDS = 7 * 24 * 60 * 60;

async function hashSubject(scope: string, subject: string): Promise<string> {
  const bytes = new TextEncoder().encode(`${scope}:${subject}`);
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

export async function consumeRequestBudget(
  db: D1Database,
  scope: string,
  subject: string,
  limit: number,
  windowSeconds: number,
  nowMs = Date.now(),
): Promise<boolean> {
  const boundedLimit = Math.max(1, Math.floor(limit));
  const boundedWindow = Math.max(1, Math.floor(windowSeconds));
  const nowSeconds = Math.floor(nowMs / 1000);
  const windowStart = Math.floor(nowSeconds / boundedWindow) * boundedWindow;
  const subjectHash = await hashSubject(scope, subject);

  const consumed = await db.prepare(`
    INSERT INTO request_rate_limit_buckets
      (scope, subject_hash, window_start, count, updated_at)
    VALUES (?, ?, ?, 1, datetime('now'))
    ON CONFLICT(scope, subject_hash, window_start) DO UPDATE SET
      count = request_rate_limit_buckets.count + 1,
      updated_at = datetime('now')
    WHERE request_rate_limit_buckets.count < ?
    RETURNING count
  `).bind(scope, subjectHash, windowStart, boundedLimit).first<{ count: number }>();

  if (consumed?.count === 1 && subject === 'all') {
    const retentionCutoff = nowSeconds - RATE_LIMIT_RETENTION_SECONDS;
    await db.prepare(`
      DELETE FROM request_rate_limit_buckets
      WHERE window_start < ?
    `).bind(retentionCutoff).run();
  }

  return Boolean(consumed && consumed.count <= boundedLimit);
}
