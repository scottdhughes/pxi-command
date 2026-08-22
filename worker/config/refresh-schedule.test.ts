import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DAILY_CLOSE_REFRESH_CRON,
  MISSED_REFRESH_WATCHDOG_CRON,
  PRODUCTION_CRON_EXPRESSIONS,
  expectedDailyCloseSlotKey,
  findRefreshSchedule,
  isPastDailyCloseWatchdogCutoff,
  isUsEquityTradingDay,
  nextScheduledRefreshAt,
  scheduledSlotKey,
} from './refresh-schedule';

test('production cron contract is bounded and routes exact expressions', () => {
  assert.equal(PRODUCTION_CRON_EXPRESSIONS.length, 5);
  assert.equal(new Set(PRODUCTION_CRON_EXPRESSIONS).size, 5);
  assert.equal(findRefreshSchedule(DAILY_CLOSE_REFRESH_CRON)?.schedule_id, 'daily_close');
  assert.equal(findRefreshSchedule(DAILY_CLOSE_REFRESH_CRON)?.record_research_evidence, true);
  assert.equal(findRefreshSchedule(MISSED_REFRESH_WATCHDOG_CRON), null);
  assert.equal(findRefreshSchedule('0 22 * * *'), null);
});

test('scheduled slot keys use the nominal Cloudflare scheduled time', () => {
  const scheduledTime = Date.parse('2026-08-21T22:00:00.000Z');
  assert.equal(
    scheduledSlotKey('daily_close', scheduledTime),
    'pxi-refresh:daily_close:2026-08-21T22:00:00.000Z',
  );
  assert.equal(
    expectedDailyCloseSlotKey('2026-08-21'),
    'pxi-refresh:daily_close:2026-08-21T22:00:00.000Z',
  );
});

test('US equity calendar handles weekends and recurring full-day holidays', () => {
  assert.equal(isUsEquityTradingDay('2026-08-21'), true);
  assert.equal(isUsEquityTradingDay('2026-08-22'), false);
  assert.equal(isUsEquityTradingDay('2026-01-01'), false);
  assert.equal(isUsEquityTradingDay('2026-01-19'), false);
  assert.equal(isUsEquityTradingDay('2026-04-03'), false);
  assert.equal(isUsEquityTradingDay('2026-06-19'), false);
  assert.equal(isUsEquityTradingDay('2026-11-26'), false);
  assert.equal(isUsEquityTradingDay('not-a-date'), false);
});

test('daily-close watchdog cutoff begins at 23:30 UTC', () => {
  assert.equal(isPastDailyCloseWatchdogCutoff(new Date('2026-08-21T23:29:59Z')), false);
  assert.equal(isPastDailyCloseWatchdogCutoff(new Date('2026-08-21T23:30:00Z')), true);
});

test('next refresh excludes the weekday-only close slot on weekends', () => {
  assert.equal(
    nextScheduledRefreshAt(new Date('2026-08-21T21:00:00.000Z')).toISOString(),
    '2026-08-21T22:00:00.000Z',
  );
  assert.equal(
    nextScheduledRefreshAt(new Date('2026-08-22T19:00:00.000Z')).toISOString(),
    '2026-08-23T06:00:00.000Z',
  );
});
