export const PRODUCTION_MUTATION_LOCK_NAME = 'indicator_score_mutation' as const;

export type RefreshMutationHolderType =
  | 'cloudflare_cron'
  | 'deploy_smoke'
  | 'github_daily_refresh'
  | 'history_reconstruction'
  | 'deploy';

const DEFAULT_LEASE_MINUTES = 30;
const MIN_LEASE_MINUTES = 1;
const MAX_LEASE_MINUTES = 60;
const HOLDER_TYPES = new Set<RefreshMutationHolderType>([
  'cloudflare_cron',
  'deploy_smoke',
  'github_daily_refresh',
  'history_reconstruction',
  'deploy',
]);

interface RefreshMutationLockRow {
  lock_name: typeof PRODUCTION_MUTATION_LOCK_NAME;
  holder_id: string;
  holder_type: RefreshMutationHolderType;
  acquired_at: string;
  expires_at: string;
  lease_version: number;
}

export interface ClaimRefreshMutationLockOptions {
  holderId: string;
  holderType: RefreshMutationHolderType;
  now?: Date;
  leaseMinutes?: number;
}

export interface ReleaseRefreshMutationLockOptions {
  holderId: string;
}

export type RefreshMutationLockClaim =
  | {
      status: 'claimed';
      lock_name: typeof PRODUCTION_MUTATION_LOCK_NAME;
      holder_type: RefreshMutationHolderType;
      acquired_at: string;
      expires_at: string;
      lease_version: number;
      reclaimed: boolean;
    }
  | {
      status: 'not_claimed';
      lock_name: typeof PRODUCTION_MUTATION_LOCK_NAME;
      holder_type: RefreshMutationHolderType;
      expires_at: string;
      reason: 'active_lease';
    };

export interface PublicRefreshMutationLock {
  lock_name: typeof PRODUCTION_MUTATION_LOCK_NAME;
  holder_type: RefreshMutationHolderType;
  acquired_at: string;
  expires_at: string;
}

function isoDateTime(value: Date): string {
  if (Number.isNaN(value.getTime())) throw new Error('Invalid mutation-lock timestamp');
  return value.toISOString();
}

function normalizeHolderId(holderId: string): string {
  const normalized = holderId.trim();
  if (normalized.length === 0 || normalized.length > 200) {
    throw new Error('Mutation-lock holder ID must contain 1 to 200 characters');
  }
  return normalized;
}

function boundedLeaseMinutes(value: number | undefined): number {
  const finite = value !== undefined && Number.isFinite(value)
    ? Math.floor(value)
    : DEFAULT_LEASE_MINUTES;
  return Math.min(MAX_LEASE_MINUTES, Math.max(MIN_LEASE_MINUTES, finite));
}

function publicLock(row: RefreshMutationLockRow): PublicRefreshMutationLock {
  return {
    lock_name: row.lock_name,
    holder_type: row.holder_type,
    acquired_at: row.acquired_at,
    expires_at: row.expires_at,
  };
}

async function attemptMutationLockClaim(
  db: D1Database,
  holderId: string,
  holderType: RefreshMutationHolderType,
  acquiredAt: string,
  expiresAt: string,
): Promise<RefreshMutationLockRow | null> {
  return db.prepare(`
    INSERT INTO refresh_mutation_locks (
      lock_name,
      holder_id,
      holder_type,
      acquired_at,
      expires_at,
      lease_version,
      updated_at
    ) VALUES (?, ?, ?, ?, ?, 1, ?)
    ON CONFLICT(lock_name) DO UPDATE SET
      holder_id = excluded.holder_id,
      holder_type = excluded.holder_type,
      acquired_at = excluded.acquired_at,
      expires_at = excluded.expires_at,
      lease_version = refresh_mutation_locks.lease_version + 1,
      updated_at = excluded.updated_at
    WHERE refresh_mutation_locks.expires_at <= excluded.acquired_at
    RETURNING
      lock_name,
      holder_id,
      holder_type,
      acquired_at,
      expires_at,
      lease_version
  `).bind(
    PRODUCTION_MUTATION_LOCK_NAME,
    holderId,
    holderType,
    acquiredAt,
    expiresAt,
    acquiredAt,
  ).first<RefreshMutationLockRow>();
}

/**
 * Atomically claim the sole production indicator/score mutation lease.
 * Active leases disclose only their holder class and expiry, never holder IDs.
 */
export async function claimRefreshMutationLock(
  db: D1Database,
  options: ClaimRefreshMutationLockOptions,
): Promise<RefreshMutationLockClaim> {
  const holderId = normalizeHolderId(options.holderId);
  if (!HOLDER_TYPES.has(options.holderType)) throw new Error('Invalid mutation-lock holder type');

  const now = options.now ?? new Date();
  const acquiredAt = isoDateTime(now);
  const expiresAt = new Date(
    now.getTime() + boundedLeaseMinutes(options.leaseMinutes) * 60_000,
  ).toISOString();

  let claimed = await attemptMutationLockClaim(
    db,
    holderId,
    options.holderType,
    acquiredAt,
    expiresAt,
  );
  if (claimed) {
    return {
      status: 'claimed',
      ...publicLock(claimed),
      lease_version: claimed.lease_version,
      reclaimed: claimed.lease_version > 1,
    };
  }

  let existing = await db.prepare(`
    SELECT
      lock_name,
      holder_id,
      holder_type,
      acquired_at,
      expires_at,
      lease_version
    FROM refresh_mutation_locks
    WHERE lock_name = ?
      AND expires_at > ?
    LIMIT 1
  `).bind(PRODUCTION_MUTATION_LOCK_NAME, acquiredAt).first<RefreshMutationLockRow>();

  // A holder can release between the failed insert and the read. Retry once so
  // that ordinary release/claim races do not surface as operational errors.
  if (!existing) {
    claimed = await attemptMutationLockClaim(
      db,
      holderId,
      options.holderType,
      acquiredAt,
      expiresAt,
    );
    if (claimed) {
      return {
        status: 'claimed',
        ...publicLock(claimed),
        lease_version: claimed.lease_version,
        reclaimed: claimed.lease_version > 1,
      };
    }
    existing = await db.prepare(`
      SELECT
        lock_name,
        holder_id,
        holder_type,
        acquired_at,
        expires_at,
        lease_version
      FROM refresh_mutation_locks
      WHERE lock_name = ?
        AND expires_at > ?
      LIMIT 1
    `).bind(PRODUCTION_MUTATION_LOCK_NAME, acquiredAt).first<RefreshMutationLockRow>();
  }

  if (!existing) throw new Error('Mutation-lock claim lost without an active lease');
  return {
    status: 'not_claimed',
    lock_name: PRODUCTION_MUTATION_LOCK_NAME,
    holder_type: existing.holder_type,
    expires_at: existing.expires_at,
    reason: 'active_lease',
  };
}

/** Release only when the caller still owns the exact opaque holder ID. */
export async function releaseRefreshMutationLock(
  db: D1Database,
  options: ReleaseRefreshMutationLockOptions,
): Promise<boolean> {
  const holderId = normalizeHolderId(options.holderId);
  const result = await db.prepare(`
    DELETE FROM refresh_mutation_locks
    WHERE lock_name = ?
      AND holder_id = ?
  `).bind(PRODUCTION_MUTATION_LOCK_NAME, holderId).run();
  return typeof result.meta?.changes === 'number' && result.meta.changes > 0;
}

/** Read an active lease in a public-safe form; expired or absent rows are null. */
export async function readRefreshMutationLock(
  db: D1Database,
  now = new Date(),
): Promise<PublicRefreshMutationLock | null> {
  const nowIso = isoDateTime(now);
  const row = await db.prepare(`
    SELECT
      lock_name,
      holder_id,
      holder_type,
      acquired_at,
      expires_at,
      lease_version
    FROM refresh_mutation_locks
    WHERE lock_name = ?
      AND expires_at > ?
    LIMIT 1
  `).bind(PRODUCTION_MUTATION_LOCK_NAME, nowIso).first<RefreshMutationLockRow>();
  return row ? publicLock(row) : null;
}
