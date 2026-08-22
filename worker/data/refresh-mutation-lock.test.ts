import assert from 'node:assert/strict';
import test from 'node:test';

import {
  claimRefreshMutationLock,
  PRODUCTION_MUTATION_LOCK_NAME,
  readRefreshMutationLock,
  releaseRefreshMutationLock,
  type RefreshMutationHolderType,
} from './refresh-mutation-lock.js';

type LockRow = {
  lock_name: typeof PRODUCTION_MUTATION_LOCK_NAME;
  holder_id: string;
  holder_type: RefreshMutationHolderType;
  acquired_at: string;
  expires_at: string;
  lease_version: number;
};

function createMutationLockDb() {
  let lock: LockRow | null = null;

  function statement(sql: string, args: unknown[] = []): D1PreparedStatement {
    return {
      bind: (...boundArgs: unknown[]) => statement(sql, boundArgs),
      first: async <T>() => {
        if (sql.includes('INSERT INTO refresh_mutation_locks')) {
          const [lockName, holderId, holderType, acquiredAt, expiresAt] = args as [
            typeof PRODUCTION_MUTATION_LOCK_NAME,
            string,
            RefreshMutationHolderType,
            string,
            string,
          ];
          if (lock && lock.expires_at > acquiredAt) return null as T | null;
          const leaseVersion = lock ? lock.lease_version + 1 : 1;
          lock = {
            lock_name: lockName,
            holder_id: holderId,
            holder_type: holderType,
            acquired_at: acquiredAt,
            expires_at: expiresAt,
            lease_version: leaseVersion,
          };
          return { ...lock } as T;
        }

        if (sql.includes('FROM refresh_mutation_locks')) {
          if (!lock) return null as T | null;
          if (sql.includes('AND expires_at > ?') && lock.expires_at <= String(args[1])) {
            return null as T | null;
          }
          return { ...lock } as T;
        }

        throw new Error(`Unhandled mutation-lock first SQL: ${sql}`);
      },
      run: async () => {
        if (sql.includes('DELETE FROM refresh_mutation_locks')) {
          const [lockName, holderId] = args as [string, string];
          const matches = lock?.lock_name === lockName && lock.holder_id === holderId;
          if (matches) lock = null;
          return { success: true, meta: { changes: matches ? 1 : 0 } } as D1Result;
        }
        throw new Error(`Unhandled mutation-lock run SQL: ${sql}`);
      },
    } as unknown as D1PreparedStatement;
  }

  return {
    db: { prepare: (sql: string) => statement(sql) } as unknown as D1Database,
    get lock() {
      return lock;
    },
  };
}

test('first claimant acquires the one production mutation lease', async () => {
  const state = createMutationLockDb();
  const claim = await claimRefreshMutationLock(state.db, {
    holderId: 'cron:2026-08-21T22:00:00Z',
    holderType: 'cloudflare_cron',
    now: new Date('2026-08-21T22:00:00.000Z'),
    leaseMinutes: 30,
  });

  assert.deepEqual(claim, {
    status: 'claimed',
    lock_name: PRODUCTION_MUTATION_LOCK_NAME,
    holder_type: 'cloudflare_cron',
    acquired_at: '2026-08-21T22:00:00.000Z',
    expires_at: '2026-08-21T22:30:00.000Z',
    lease_version: 1,
    reclaimed: false,
  });
  assert.equal(state.lock?.holder_id, 'cron:2026-08-21T22:00:00Z');
});

test('an active lease blocks every holder type without exposing its holder ID', async () => {
  const state = createMutationLockDb();
  await claimRefreshMutationLock(state.db, {
    holderId: 'private-cloudflare-invocation-id',
    holderType: 'cloudflare_cron',
    now: new Date('2026-08-21T22:00:00.000Z'),
    leaseMinutes: 30,
  });

  const blocked = await claimRefreshMutationLock(state.db, {
    holderId: 'reconstruction-run-123',
    holderType: 'history_reconstruction',
    now: new Date('2026-08-21T22:29:59.000Z'),
  });

  assert.deepEqual(blocked, {
    status: 'not_claimed',
    lock_name: PRODUCTION_MUTATION_LOCK_NAME,
    holder_type: 'cloudflare_cron',
    expires_at: '2026-08-21T22:30:00.000Z',
    reason: 'active_lease',
  });
  assert.equal('holder_id' in blocked, false);
});

test('an expired lease is atomically reclaimed in the same row', async () => {
  const state = createMutationLockDb();
  await claimRefreshMutationLock(state.db, {
    holderId: 'daily-refresh-old',
    holderType: 'github_daily_refresh',
    now: new Date('2026-08-21T14:00:00.000Z'),
    leaseMinutes: 1,
  });

  const reclaimed = await claimRefreshMutationLock(state.db, {
    holderId: 'deploy-new',
    holderType: 'deploy',
    now: new Date('2026-08-21T14:01:00.000Z'),
    leaseMinutes: 60,
  });

  assert.equal(reclaimed.status, 'claimed');
  assert.equal(reclaimed.status === 'claimed' && reclaimed.reclaimed, true);
  assert.equal(reclaimed.status === 'claimed' && reclaimed.lease_version, 2);
  assert.equal(state.lock?.holder_id, 'deploy-new');
  assert.equal(state.lock?.expires_at, '2026-08-21T15:01:00.000Z');
});

test('lease duration is bounded to one through sixty minutes', async () => {
  const shortState = createMutationLockDb();
  const short = await claimRefreshMutationLock(shortState.db, {
    holderId: 'short',
    holderType: 'deploy_smoke',
    now: new Date('2026-08-21T12:00:00.000Z'),
    leaseMinutes: -10,
  });
  assert.equal(short.status === 'claimed' && short.expires_at, '2026-08-21T12:01:00.000Z');

  const longState = createMutationLockDb();
  const long = await claimRefreshMutationLock(longState.db, {
    holderId: 'long',
    holderType: 'deploy',
    now: new Date('2026-08-21T12:00:00.000Z'),
    leaseMinutes: 1000,
  });
  assert.equal(long.status === 'claimed' && long.expires_at, '2026-08-21T13:00:00.000Z');
});

test('release requires the exact holder and active reads remain public-safe', async () => {
  const state = createMutationLockDb();
  await claimRefreshMutationLock(state.db, {
    holderId: 'exact-holder',
    holderType: 'history_reconstruction',
    now: new Date('2026-08-21T12:00:00.000Z'),
    leaseMinutes: 10,
  });

  assert.equal(await releaseRefreshMutationLock(state.db, { holderId: 'wrong-holder' }), false);
  const visible = await readRefreshMutationLock(
    state.db,
    new Date('2026-08-21T12:05:00.000Z'),
  );
  assert.deepEqual(visible, {
    lock_name: PRODUCTION_MUTATION_LOCK_NAME,
    holder_type: 'history_reconstruction',
    acquired_at: '2026-08-21T12:00:00.000Z',
    expires_at: '2026-08-21T12:10:00.000Z',
  });
  assert.equal(visible && 'holder_id' in visible, false);

  assert.equal(await releaseRefreshMutationLock(state.db, { holderId: 'exact-holder' }), true);
  assert.equal(await readRefreshMutationLock(state.db), null);
});

test('expired leases are not reported as active', async () => {
  const state = createMutationLockDb();
  await claimRefreshMutationLock(state.db, {
    holderId: 'expired',
    holderType: 'deploy_smoke',
    now: new Date('2026-08-21T12:00:00.000Z'),
    leaseMinutes: 1,
  });

  assert.equal(
    await readRefreshMutationLock(state.db, new Date('2026-08-21T12:01:00.000Z')),
    null,
  );
});
