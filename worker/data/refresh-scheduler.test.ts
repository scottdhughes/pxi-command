import assert from 'node:assert/strict';
import test from 'node:test';

import { scheduledSlotKey } from '../config/refresh-schedule.js';
import {
  claimRefreshScheduleSlot,
  evaluateDailyCloseWatchdog,
  fetchRefreshSchedulerHealth,
  finishRefreshScheduleSlot,
  newYorkDecisionDate,
} from './refresh-scheduler.js';

type RunRow = {
  slot_key: string;
  schedule_id: 'overnight' | 'premarket' | 'midday' | 'daily_close';
  scheduled_at: string;
  decision_date: string;
  status: 'running' | 'success' | 'failed';
  attempt_count: number;
  claimed_at: string;
  completed_at: string | null;
  last_error: string | null;
};

type IncidentRow = {
  incident_id: string;
  incident_type: 'missed_daily_close';
  decision_date: string;
  status: 'open' | 'resolved';
  expected_slot_key: string;
  opened_at: string;
  last_checked_at: string;
  resolved_at: string | null;
  resolution_slot_key: string | null;
  details_json: string;
};

function createSchedulerDb() {
  const runs: RunRow[] = [];
  const incidents: IncidentRow[] = [];

  function statement(sql: string, args: unknown[] = []): D1PreparedStatement {
    const boundStatement = {
      bind: (...boundArgs: unknown[]) => statement(sql, boundArgs),
      first: async <T>() => {
        if (sql.includes('INSERT INTO refresh_scheduler_runs')) {
          const [
            slotKey,
            scheduleId,
            scheduledAt,
            decisionDate,
            claimedAt,
            ,
            ,
            staleBefore,
          ] = args as [string, RunRow['schedule_id'], string, string, string, string, string, string];
          const existing = runs.find((row) => row.slot_key === slotKey);
          if (!existing) {
            const inserted: RunRow = {
              slot_key: slotKey,
              schedule_id: scheduleId,
              scheduled_at: scheduledAt,
              decision_date: decisionDate,
              status: 'running',
              attempt_count: 1,
              claimed_at: claimedAt,
              completed_at: null,
              last_error: null,
            };
            runs.push(inserted);
            return { ...inserted } as T;
          }
          if (existing.status === 'failed' || (existing.status === 'running' && existing.claimed_at <= staleBefore)) {
            existing.status = 'running';
            existing.attempt_count += 1;
            existing.claimed_at = claimedAt;
            existing.completed_at = null;
            existing.last_error = null;
            return { ...existing } as T;
          }
          return null as T | null;
        }

        if (sql.includes('UPDATE refresh_scheduler_runs')) {
          const [status, completedAt, error, , slotKey, attempt] = args as [
            RunRow['status'], string, string | null, string, string, number,
          ];
          const row = runs.find((candidate) =>
            candidate.slot_key === slotKey
            && candidate.status === 'running'
            && candidate.attempt_count === attempt,
          );
          if (!row) return null as T | null;
          row.status = status;
          row.completed_at = completedAt;
          row.last_error = error;
          return { ...row } as T;
        }

        if (sql.includes('INSERT INTO refresh_scheduler_incidents')) {
          const [incidentId, incidentType, decisionDate, expectedSlotKey, openedAt, lastCheckedAt, detailsJson] = args as [
            string, 'missed_daily_close', string, string, string, string, string,
          ];
          let row = incidents.find((candidate) =>
            candidate.incident_type === incidentType && candidate.decision_date === decisionDate,
          );
          if (!row) {
            row = {
              incident_id: incidentId,
              incident_type: incidentType,
              decision_date: decisionDate,
              status: 'open',
              expected_slot_key: expectedSlotKey,
              opened_at: openedAt,
              last_checked_at: lastCheckedAt,
              resolved_at: null,
              resolution_slot_key: null,
              details_json: detailsJson,
            };
            incidents.push(row);
          } else {
            row.status = 'open';
            row.last_checked_at = lastCheckedAt;
            row.resolved_at = null;
            row.resolution_slot_key = null;
            row.details_json = detailsJson;
          }
          return { ...row } as T;
        }

        if (sql.includes('UPDATE refresh_scheduler_incidents')) {
          const [resolvedAt, , resolutionSlotKey, incidentType, decisionDate] = args as [
            string, string, string, string, string,
          ];
          const row = incidents.find((candidate) =>
            candidate.incident_type === incidentType
            && candidate.decision_date === decisionDate
            && candidate.status === 'open',
          );
          if (!row) return null as T | null;
          row.status = 'resolved';
          row.resolved_at = resolvedAt;
          row.last_checked_at = resolvedAt;
          row.resolution_slot_key = resolutionSlotKey;
          return { ...row } as T;
        }

        if (sql.includes('FROM refresh_scheduler_runs')) {
          if (sql.includes('WHERE slot_key = ?')) {
            const row = runs.find((candidate) => candidate.slot_key === args[0]);
            return (row ? { ...row } : null) as T | null;
          }
          const decisionDate = sql.includes('AND decision_date = ?') ? String(args[0]) : null;
          const row = [...runs]
            .filter((candidate) => candidate.schedule_id === 'daily_close')
            .filter((candidate) => !decisionDate || candidate.decision_date === decisionDate)
            .sort((left, right) => right.scheduled_at.localeCompare(left.scheduled_at))[0];
          return (row ? { ...row } : null) as T | null;
        }

        if (sql.includes('FROM refresh_scheduler_incidents')) {
          const wantsOpen = sql.includes("status = 'open'");
          const decisionDate = sql.includes('AND decision_date = ?') ? String(args.at(-1)) : null;
          const row = [...incidents]
            .filter((candidate) => !wantsOpen || candidate.status === 'open')
            .filter((candidate) => !decisionDate || candidate.decision_date === decisionDate)
            .sort((left, right) => right.decision_date.localeCompare(left.decision_date))[0];
          return (row ? { ...row } : null) as T | null;
        }

        throw new Error(`Unhandled scheduler first SQL: ${sql}`);
      },
      run: async () => {
        if (sql.includes('UPDATE refresh_scheduler_incidents')) {
          const [resolvedAt, , resolutionSlotKey, incidentType, decisionDate] = args as [
            string, string, string, string, string,
          ];
          let changes = 0;
          for (const row of incidents) {
            if (
              row.incident_type === incidentType
              && row.status === 'open'
              && row.decision_date <= decisionDate
            ) {
              row.status = 'resolved';
              row.resolved_at = resolvedAt;
              row.last_checked_at = resolvedAt;
              row.resolution_slot_key = resolutionSlotKey;
              changes += 1;
            }
          }
          return { success: true, meta: { changes } } as D1Result;
        }
        throw new Error(`Unhandled scheduler run SQL: ${sql}`);
      },
    };
    return boundStatement as unknown as D1PreparedStatement;
  }

  return {
    runs,
    incidents,
    db: { prepare: (sql: string) => statement(sql) } as unknown as D1Database,
  };
}

test('daily-close claims use a deterministic slot and canonical New York decision date', async () => {
  const scheduledTime = Date.parse('2026-08-21T22:00:00.000Z');
  const now = new Date('2026-08-21T22:00:03.000Z');
  const state = createSchedulerDb();

  const claimed = await claimRefreshScheduleSlot(state.db, {
    scheduleId: 'daily_close',
    scheduledTime,
    now,
  });

  assert.deepEqual(claimed, {
    status: 'claimed',
    slot_key: scheduledSlotKey('daily_close', scheduledTime),
    schedule_id: 'daily_close',
    scheduled_at: '2026-08-21T22:00:00.000Z',
    decision_date: '2026-08-21',
    attempt: 1,
    reclaimed: false,
  });
  assert.equal(newYorkDecisionDate(scheduledTime), '2026-08-21');
});

test('successful and active duplicate deliveries are deduplicated', async () => {
  const scheduledTime = Date.parse('2026-08-21T18:00:00.000Z');
  const state = createSchedulerDb();
  const first = await claimRefreshScheduleSlot(state.db, {
    scheduleId: 'midday',
    scheduledTime,
    now: new Date('2026-08-21T18:00:01.000Z'),
  });
  assert.equal(first.status, 'claimed');

  const activeDuplicate = await claimRefreshScheduleSlot(state.db, {
    scheduleId: 'midday',
    scheduledTime,
    now: new Date('2026-08-21T18:10:00.000Z'),
  });
  assert.equal(activeDuplicate.status, 'skipped');
  assert.equal(activeDuplicate.status === 'skipped' && activeDuplicate.reason, 'in_progress');

  assert.equal(first.status === 'claimed' && await finishRefreshScheduleSlot(state.db, {
    slotKey: first.slot_key,
    attempt: first.attempt,
    status: 'success',
    completedAt: new Date('2026-08-21T18:15:00.000Z'),
  }), true);

  const successfulDuplicate = await claimRefreshScheduleSlot(state.db, {
    scheduleId: 'midday',
    scheduledTime,
    now: new Date('2026-08-21T20:00:00.000Z'),
  });
  assert.equal(successfulDuplicate.status, 'skipped');
  assert.equal(successfulDuplicate.status === 'skipped' && successfulDuplicate.reason, 'already_succeeded');
  assert.equal(state.runs.length, 1);
});

test('failed and stale attempts are reclaimed with fencing against late completion', async () => {
  const scheduledTime = Date.parse('2026-08-21T14:00:00.000Z');
  const state = createSchedulerDb();
  const first = await claimRefreshScheduleSlot(state.db, {
    scheduleId: 'premarket',
    scheduledTime,
    now: new Date('2026-08-21T14:00:00.000Z'),
  });
  assert.equal(first.status, 'claimed');

  const reclaimed = await claimRefreshScheduleSlot(state.db, {
    scheduleId: 'premarket',
    scheduledTime,
    now: new Date('2026-08-21T16:00:01.000Z'),
    staleAfterMinutes: 90,
  });
  assert.equal(reclaimed.status, 'claimed');
  assert.equal(reclaimed.status === 'claimed' && reclaimed.attempt, 2);
  assert.equal(reclaimed.status === 'claimed' && reclaimed.reclaimed, true);

  const staleFinish = first.status === 'claimed' && await finishRefreshScheduleSlot(state.db, {
    slotKey: first.slot_key,
    attempt: first.attempt,
    status: 'success',
    completedAt: new Date('2026-08-21T16:01:00.000Z'),
  });
  assert.equal(staleFinish, false);

  assert.equal(reclaimed.status === 'claimed' && await finishRefreshScheduleSlot(state.db, {
    slotKey: reclaimed.slot_key,
    attempt: reclaimed.attempt,
    status: 'failed',
    error: 'upstream unavailable',
    completedAt: new Date('2026-08-21T16:02:00.000Z'),
  }), true);

  const retry = await claimRefreshScheduleSlot(state.db, {
    scheduleId: 'premarket',
    scheduledTime,
    now: new Date('2026-08-21T16:03:00.000Z'),
  });
  assert.equal(retry.status, 'claimed');
  assert.equal(retry.status === 'claimed' && retry.attempt, 3);
  assert.equal(state.runs.length, 1);
});

test('watchdog records one missed-date incident, then daily-close success resolves it', async () => {
  const state = createSchedulerDb();
  const decisionDate = '2026-08-21';

  const notExpected = await evaluateDailyCloseWatchdog(state.db, {
    decisionDate: '2026-08-22',
    isUsMarketDay: false,
    now: new Date('2026-08-22T23:30:00.000Z'),
  });
  assert.equal(notExpected.state, 'not_expected');
  assert.equal(state.incidents.length, 0);

  const pending = await evaluateDailyCloseWatchdog(state.db, {
    decisionDate,
    isUsMarketDay: true,
    now: new Date('2026-08-21T23:29:59.000Z'),
  });
  assert.equal(pending.state, 'pending');

  const missed = await evaluateDailyCloseWatchdog(state.db, {
    decisionDate,
    isUsMarketDay: true,
    now: new Date('2026-08-21T23:30:00.000Z'),
  });
  assert.equal(missed.state, 'missed');
  assert.equal(missed.latest_incident?.status, 'open');

  await evaluateDailyCloseWatchdog(state.db, {
    decisionDate,
    isUsMarketDay: true,
    now: new Date('2026-08-21T23:45:00.000Z'),
  });
  assert.equal(state.incidents.length, 1);

  const scheduledTime = Date.parse('2026-08-21T22:00:00.000Z');
  const claim = await claimRefreshScheduleSlot(state.db, {
    scheduleId: 'daily_close',
    scheduledTime,
    now: new Date('2026-08-21T23:46:00.000Z'),
  });
  assert.equal(claim.status, 'claimed');
  assert.equal(claim.status === 'claimed' && await finishRefreshScheduleSlot(state.db, {
    slotKey: claim.slot_key,
    attempt: claim.attempt,
    status: 'success',
    completedAt: new Date('2026-08-21T23:50:00.000Z'),
  }), true);
  assert.equal(state.incidents[0]?.status, 'resolved');

  const healthy = await evaluateDailyCloseWatchdog(state.db, {
    decisionDate,
    isUsMarketDay: true,
    now: new Date('2026-08-21T23:55:00.000Z'),
  });
  assert.equal(healthy.state, 'healthy');
  assert.equal(healthy.latest_incident?.status, 'resolved');
});

test('latest health is public-safe and distinguishes missed from healthy', async () => {
  const state = createSchedulerDb();
  await evaluateDailyCloseWatchdog(state.db, {
    decisionDate: '2026-08-21',
    isUsMarketDay: true,
    now: new Date('2026-08-21T23:30:00.000Z'),
  });

  const missed = await fetchRefreshSchedulerHealth(state.db, {
    decisionDate: '2026-08-21',
    isUsMarketDay: true,
    now: new Date('2026-08-21T23:31:00.000Z'),
  });
  assert.equal(missed.state, 'missed');
  assert.equal('last_error' in missed, false);
  assert.equal('details_json' in (missed.latest_incident || {}), false);

  const weekendWithOpenIncident = await fetchRefreshSchedulerHealth(state.db, {
    decisionDate: '2026-08-22',
    isUsMarketDay: false,
    now: new Date('2026-08-22T12:00:00.000Z'),
  });
  assert.equal(weekendWithOpenIncident.state, 'missed');

  const scheduledTime = Date.parse('2026-08-22T22:00:00.000Z');
  const claim = await claimRefreshScheduleSlot(state.db, {
    scheduleId: 'daily_close',
    scheduledTime,
    now: new Date('2026-08-22T22:00:00.000Z'),
  });
  assert.equal(claim.status, 'claimed');
  if (claim.status === 'claimed') {
    await finishRefreshScheduleSlot(state.db, {
      slotKey: claim.slot_key,
      attempt: claim.attempt,
      status: 'success',
      completedAt: new Date('2026-08-22T22:10:00.000Z'),
    });
  }

  const healthy = await fetchRefreshSchedulerHealth(state.db, {
    decisionDate: '2026-08-22',
    isUsMarketDay: true,
    now: new Date('2026-08-22T22:11:00.000Z'),
  });
  assert.equal(healthy.state, 'healthy');
  assert.equal(healthy.latest_daily_close?.status, 'success');
});
