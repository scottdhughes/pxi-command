import { createRouteDeps } from '../bootstrap/create-route-deps';
import {
  MISSED_REFRESH_WATCHDOG_CRON,
  findRefreshSchedule,
  isUsEquityTradingDay,
  type RefreshScheduleSpec,
} from '../config/refresh-schedule';
import {
  claimRefreshScheduleSlot,
  evaluateDailyCloseWatchdog,
  finishRefreshScheduleSlot,
} from '../data/refresh-scheduler';
import {
  claimRefreshMutationLock,
  releaseRefreshMutationLock,
} from '../data/refresh-mutation-lock';
import { tryHandleAdminIngestionRoute } from '../routes/admin-ingestion';
import { tryHandleMarketLifecycleRoute } from '../routes/market-lifecycle';
import { tryHandleModelingRoute } from '../routes/modeling';
import type { Env, WorkerRouteContext } from '../types';
import {
  evaluateCandidateSla,
  selectLatestIndicatorCandidates,
  type CandidateSlaSummary,
  type IndicatorCandidate,
} from './candidate-sla';

export { evaluateCandidateSla } from './candidate-sla';

export interface NativeRefreshSummary {
  decision_date: string;
  schedule_id: string;
  indicators_fetched: number;
  indicators_written: number;
  sla: CandidateSlaSummary;
  recalculate: Record<string, unknown>;
  evaluation: Record<string, unknown>;
  products: Record<string, unknown>;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function boundedFailureDetail(error: unknown): string {
  return errorMessage(error).replace(/[\r\n\t]+/g, ' ').slice(0, 500);
}

function failureCode(error: unknown): string {
  const message = errorMessage(error).toLowerCase();
  if (message.includes('fred_api_key')) return 'missing_fred_api_key';
  if (message.includes('write_api_key')) return 'missing_write_api_key';
  if (message.includes('critical sla')) return 'critical_sla_failure';
  if (message.includes('canonical evidence')) return 'canonical_evidence_failure';
  if (message.includes('date drift')) return 'scheduled_date_drift';
  if (message.includes('mutation lock')) return 'mutation_lock_busy';
  if (message.includes('scheduler fence')) return 'scheduler_fence_lost';
  if (message.includes('deadline') || message.includes('timed out')) return 'provider_deadline_exceeded';
  if (message.includes('already in progress')) return 'refresh_in_progress';
  return 'refresh_pipeline_failure';
}

function internalRoute(
  env: Env,
  path: string,
  body: Record<string, unknown>,
  extraHeaders: Record<string, string> = {},
): WorkerRouteContext {
  if (!env.WRITE_API_KEY) {
    throw new Error('WRITE_API_KEY not configured');
  }
  const request = new Request(`https://pxi.internal${path}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${env.WRITE_API_KEY}`,
      'Content-Type': 'application/json',
      ...extraHeaders,
    },
    body: JSON.stringify(body),
  });
  return {
    request,
    env,
    url: new URL(request.url),
    method: 'POST',
    corsHeaders: {},
    clientIP: 'cloudflare-cron',
  };
}

async function requireJsonResponse(
  label: string,
  response: Response | null,
): Promise<Record<string, unknown>> {
  if (!response) throw new Error(`${label} route was not handled`);
  const text = await response.text();
  let payload: Record<string, unknown> = {};
  try {
    payload = text ? JSON.parse(text) as Record<string, unknown> : {};
  } catch {
    throw new Error(`${label} returned non-JSON status ${response.status}`);
  }
  if (!response.ok) {
    const reason = typeof payload.error === 'string' ? payload.error : `status ${response.status}`;
    throw new Error(`${label} failed: ${reason}`);
  }
  return payload;
}

async function writeIndicatorCandidates(
  env: Env,
  candidates: IndicatorCandidate[],
  decisionDate: string,
): Promise<number> {
  const valid = selectLatestIndicatorCandidates(candidates, decisionDate);
  const batchSize = 100;
  let written = 0;
  for (let index = 0; index < valid.length; index += batchSize) {
    const batch = valid.slice(index, index + batchSize);
    const statements = batch.map((candidate) => env.DB.prepare(`
      INSERT OR REPLACE INTO indicator_values (indicator_id, date, value, source)
      VALUES (?, ?, ?, ?)
    `).bind(
      candidate.indicator_id,
      candidate.date,
      candidate.value,
      candidate.source,
    ));
    await env.DB.batch(statements);
    written += batch.length;
  }
  return written;
}

export async function runNativeRefreshPipeline(
  env: Env,
  schedule: Pick<RefreshScheduleSpec, 'schedule_id' | 'record_research_evidence'> | {
    schedule_id: string;
    record_research_evidence: boolean;
  },
  scheduledTime: number,
): Promise<NativeRefreshSummary> {
  if (env.DEPLOY_ENV !== 'production') {
    throw new Error('Scheduled refresh is production-only');
  }
  if (!env.FRED_API_KEY) {
    throw new Error('FRED_API_KEY not configured');
  }

  const deps = createRouteDeps();
  const nominalDecisionDate = deps.currentNewYorkDate(new Date(scheduledTime));
  const actualDecisionDate = deps.currentNewYorkDate();
  if (nominalDecisionDate !== actualDecisionDate) {
    throw new Error(`Scheduled date drift: nominal ${nominalDecisionDate}, actual ${actualDecisionDate}`);
  }

  const mutationHolderId = `${schedule.schedule_id}:${scheduledTime}:${crypto.randomUUID()}`;
  const mutationLock = await claimRefreshMutationLock(env.DB, {
    holderId: mutationHolderId,
    holderType: schedule.schedule_id === 'deploy_smoke' ? 'deploy_smoke' : 'cloudflare_cron',
    leaseMinutes: 30,
  });
  if (mutationLock.status !== 'claimed') {
    throw new Error(
      `Production mutation lock held by ${mutationLock.holder_type} until ${mutationLock.expires_at}`,
    );
  }

  try {
    console.log(JSON.stringify({
      event: 'pxi_refresh_started',
      schedule_id: schedule.schedule_id,
      scheduled_at: new Date(scheduledTime).toISOString(),
      decision_date: actualDecisionDate,
      build_sha: env.BUILD_SHA || 'unknown',
    }));

    const candidates = await deps.fetchAllIndicators(env.FRED_API_KEY) as IndicatorCandidate[];
    const sla = evaluateCandidateSla(candidates, new Date());
    if (sla.critical_failures.length > 0) {
      const names = sla.critical_failures.map((failure) => failure.indicator_id).join(', ');
      throw new Error(`Critical SLA violation(s): ${names}`);
    }

    const indicatorsWritten = await writeIndicatorCandidates(env, candidates, actualDecisionDate);

    const recalculate = await requireJsonResponse(
      'recalculate',
      await tryHandleAdminIngestionRoute(
        internalRoute(env, '/api/recalculate', {
          date: actualDecisionDate,
          record_evidence: schedule.record_research_evidence,
        }),
        deps,
      ),
    );
    if (schedule.record_research_evidence) {
      const evidenceStatus = recalculate.evidence_status;
      const tradingDayExpected = isUsEquityTradingDay(actualDecisionDate);
      const accepted = evidenceStatus === 'inserted'
        || evidenceStatus === 'existing'
        || (!tradingDayExpected && evidenceStatus === 'skipped_canonical_capture');
      if (!accepted) {
        throw new Error(`Canonical evidence capture failed: ${String(evidenceStatus)}`);
      }
    }

    const evaluation = await requireJsonResponse(
      'evaluate',
      await tryHandleModelingRoute(internalRoute(env, '/api/evaluate', {}), deps),
    );

    const products = await requireJsonResponse(
      'refresh-products',
      await tryHandleMarketLifecycleRoute(
        internalRoute(
          env,
          '/api/market/refresh-products',
          {},
          {
            'X-Refresh-Trigger': `cloudflare_cron_${schedule.schedule_id}_${scheduledTime}`,
          },
        ),
        deps,
      ),
    );

    const summary: NativeRefreshSummary = {
      decision_date: actualDecisionDate,
      schedule_id: schedule.schedule_id,
      indicators_fetched: candidates.length,
      indicators_written: indicatorsWritten,
      sla,
      recalculate,
      evaluation,
      products,
    };
    console.log(JSON.stringify({
      event: 'pxi_refresh_completed',
      schedule_id: schedule.schedule_id,
      decision_date: actualDecisionDate,
      indicators_fetched: candidates.length,
      indicators_written: indicatorsWritten,
      critical_sla_failures: 0,
      evidence_status: recalculate.evidence_status ?? 'not_requested',
      product_publication_status: products.publication_status ?? 'unknown',
    }));
    return summary;
  } finally {
    const released = await releaseRefreshMutationLock(env.DB, { holderId: mutationHolderId });
    if (!released) {
      console.error(JSON.stringify({
        event: 'pxi_refresh_mutation_lock_release_failed',
        schedule_id: schedule.schedule_id,
      }));
    }
  }
}

export async function runScheduledRefresh(
  controller: ScheduledController,
  env: Env,
): Promise<void> {
  if (env.DEPLOY_ENV !== 'production') {
    throw new Error('Cron invocation rejected outside production');
  }

  if (controller.cron === MISSED_REFRESH_WATCHDOG_CRON) {
    const decisionDate = createRouteDeps().currentNewYorkDate(new Date(controller.scheduledTime));
    const watchdog = await evaluateDailyCloseWatchdog(env.DB, {
      decisionDate,
      now: new Date(controller.scheduledTime),
      isUsMarketDay: isUsEquityTradingDay(decisionDate),
    });
    console.log(JSON.stringify({ event: 'pxi_refresh_watchdog', ...watchdog }));
    if (watchdog.state === 'missed') {
      throw new Error(`Missed canonical daily refresh for ${watchdog.decision_date}`);
    }
    return;
  }

  const schedule = findRefreshSchedule(controller.cron);
  if (!schedule) {
    throw new Error(`Unknown Cron Trigger: ${controller.cron}`);
  }

  const decisionDate = createRouteDeps().currentNewYorkDate(new Date(controller.scheduledTime));
  const claim = await claimRefreshScheduleSlot(env.DB, {
    scheduleId: schedule.schedule_id,
    scheduledTime: controller.scheduledTime,
    decisionDate,
  });
  if (claim.status === 'skipped') {
    console.log(JSON.stringify({
      event: 'pxi_refresh_duplicate',
      schedule_id: schedule.schedule_id,
      scheduled_at: new Date(controller.scheduledTime).toISOString(),
      reason: claim.reason,
      attempt: claim.attempt,
    }));
    return;
  }

  try {
    await runNativeRefreshPipeline(env, schedule, controller.scheduledTime);
    const recorded = await finishRefreshScheduleSlot(env.DB, {
      slotKey: claim.slot_key,
      attempt: claim.attempt,
      status: 'success',
    });
    if (!recorded) {
      throw new Error('Scheduler fence lost after refresh pipeline completion');
    }
  } catch (error) {
    const recorded = await finishRefreshScheduleSlot(env.DB, {
      slotKey: claim.slot_key,
      attempt: claim.attempt,
      status: 'failed',
      error: `${failureCode(error)}: ${boundedFailureDetail(error)}`,
    });
    if (!recorded) {
      console.error(JSON.stringify({
        event: 'pxi_refresh_scheduler_fence_lost',
        schedule_id: schedule.schedule_id,
        scheduled_at: new Date(controller.scheduledTime).toISOString(),
        attempt: claim.attempt,
      }));
    }
    console.error(JSON.stringify({
      event: 'pxi_refresh_failed',
      schedule_id: schedule.schedule_id,
      scheduled_at: new Date(controller.scheduledTime).toISOString(),
      failure_code: failureCode(error),
      detail: boundedFailureDetail(error),
    }));
    throw error;
  }
}
