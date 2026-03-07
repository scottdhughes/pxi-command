import {
  normalizeOpportunityItemsForPublishing,
  projectOpportunityFeed,
  toNumber,
} from '../runtime/legacy';
import type {
  ConsistencyState,
  FreshnessStatus,
  MarketCalibrationSnapshotPayload,
  OpportunityFeedProjection,
  OpportunityItem,
  OpportunityItemLedgerInsertPayload,
  OpportunityLedgerInsertPayload,
  OpportunitySnapshot,
} from '../types';

interface OpportunityLedgerBuildResult {
  ledger_row: OpportunityLedgerInsertPayload;
  item_rows: OpportunityItemLedgerInsertPayload[];
  projected: OpportunityFeedProjection;
  normalized_items: OpportunityItem[];
}

function removeLowInformationOpportunities(items: OpportunityItem[]): {
  items: OpportunityItem[];
  filtered_count: number;
} {
  const filtered = items.filter((item) => {
    const lowInformation = item.calibration.quality === 'INSUFFICIENT' && item.expectancy.quality === 'INSUFFICIENT';
    return !(item.direction === 'neutral' && lowInformation);
  });

  if (filtered.length > 0) {
    return {
      items: filtered,
      filtered_count: Math.max(0, items.length - filtered.length),
    };
  }

  const fallbackCount = Math.min(3, items.length);
  return {
    items: items.slice(0, fallbackCount),
    filtered_count: Math.max(0, items.length - fallbackCount),
  };
}

function applyOpportunityCoherenceGate(
  items: OpportunityItem[],
  enabled: boolean,
): {
  items: OpportunityItem[];
  suppressed_count: number;
} {
  if (!enabled) {
    return {
      items,
      suppressed_count: 0,
    };
  }

  const eligible = items.filter((item) => item.eligibility?.passed === true);
  return {
    items: eligible,
    suppressed_count: Math.max(0, items.length - eligible.length),
  };
}

export async function insertOpportunityLedgerRow(
  db: D1Database,
  payload: OpportunityLedgerInsertPayload,
): Promise<void> {
  await db.prepare(`
    INSERT INTO market_opportunity_ledger (
      refresh_run_id,
      as_of,
      horizon,
      candidate_count,
      published_count,
      suppressed_count,
      quality_filtered_count,
      coherence_suppressed_count,
      data_quality_suppressed_count,
      degraded_reason,
      top_direction_candidate,
      top_direction_published,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
  `).bind(
    payload.refresh_run_id,
    payload.as_of,
    payload.horizon,
    payload.candidate_count,
    payload.published_count,
    payload.suppressed_count,
    payload.quality_filtered_count,
    payload.coherence_suppressed_count,
    payload.data_quality_suppressed_count,
    payload.degraded_reason,
    payload.top_direction_candidate,
    payload.top_direction_published,
  ).run();
}

export async function insertOpportunityItemLedgerRow(
  db: D1Database,
  payload: OpportunityItemLedgerInsertPayload,
): Promise<void> {
  await db.prepare(`
    INSERT OR REPLACE INTO market_opportunity_item_ledger (
      refresh_run_id,
      as_of,
      horizon,
      opportunity_id,
      theme_id,
      theme_name,
      direction,
      conviction_score,
      published,
      suppression_reason,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
  `).bind(
    payload.refresh_run_id,
    payload.as_of,
    payload.horizon,
    payload.opportunity_id,
    payload.theme_id,
    payload.theme_name,
    payload.direction,
    payload.conviction_score,
    payload.published,
    payload.suppression_reason,
  ).run();
}

export function buildOpportunityLedgerProjection(args: {
  refresh_run_id: number | null;
  snapshot: OpportunitySnapshot;
  calibration: MarketCalibrationSnapshotPayload | null;
  coherence_gate_enabled: boolean;
  freshness: FreshnessStatus;
  consistency_state: ConsistencyState;
  publication_allowed?: boolean;
  publication_blocked_reason?: string | null;
}): OpportunityLedgerBuildResult {
  const publicationAllowed = args.publication_allowed !== false;
  const publicationBlockedReason = args.publication_blocked_reason || 'governance_blocked';
  const normalized = normalizeOpportunityItemsForPublishing(args.snapshot.items, args.calibration);
  const qualityGateResult = removeLowInformationOpportunities(normalized);
  const coherenceGateResult = applyOpportunityCoherenceGate(
    qualityGateResult.items,
    args.coherence_gate_enabled,
  );
  const projected = projectOpportunityFeed(normalized, {
    coherence_gate_enabled: args.coherence_gate_enabled,
    freshness: args.freshness,
    consistency_state: args.consistency_state,
  });
  const qualityRetainedIds = new Set(qualityGateResult.items.map((item) => item.id));
  const coherenceEligibleIds = new Set(coherenceGateResult.items.map((item) => item.id));
  const projectedPublishedIds = new Set(projected.items.map((item) => item.id));
  const blockedPublishedIds = publicationAllowed ? new Set<string>() : new Set(projected.items.map((item) => item.id));
  const publishedIds = publicationAllowed ? projectedPublishedIds : new Set<string>();

  const itemRows: OpportunityItemLedgerInsertPayload[] = normalized.map((item) => {
    const isPublished = publishedIds.has(item.id);
    let suppressionReason: OpportunityItemLedgerInsertPayload['suppression_reason'] = null;

    if (!isPublished) {
      if (!qualityRetainedIds.has(item.id)) {
        suppressionReason = 'quality_filtered';
      } else if (!coherenceEligibleIds.has(item.id)) {
        suppressionReason = 'coherence_failed';
      } else if (blockedPublishedIds.has(item.id)) {
        suppressionReason = 'governance_blocked';
      } else if (projected.suppressed_data_quality || projected.degraded_reason === 'suppressed_data_quality') {
        suppressionReason = 'suppressed_data_quality';
      } else if (projected.degraded_reason === 'coherence_gate_failed') {
        suppressionReason = 'coherence_failed';
      } else if (projected.degraded_reason === 'quality_filtered') {
        suppressionReason = 'quality_filtered';
      }
    }

    return {
      refresh_run_id: args.refresh_run_id,
      as_of: args.snapshot.as_of,
      horizon: args.snapshot.horizon,
      opportunity_id: item.id,
      theme_id: item.theme_id,
      theme_name: item.theme_name,
      direction: item.direction,
      conviction_score: Math.max(0, Math.min(100, Math.round(toNumber(item.conviction_score, 0)))),
      published: isPublished ? 1 : 0,
      suppression_reason: suppressionReason,
    };
  });

  return {
    ledger_row: {
      refresh_run_id: args.refresh_run_id,
      as_of: args.snapshot.as_of,
      horizon: args.snapshot.horizon,
      candidate_count: projected.total_candidates,
      published_count: publicationAllowed ? projected.items.length : 0,
      suppressed_count: publicationAllowed ? projected.suppressed_count : (projected.suppressed_count + blockedPublishedIds.size),
      quality_filtered_count: projected.quality_filtered_count,
      coherence_suppressed_count: projected.coherence_suppressed_count,
      data_quality_suppressed_count: projected.suppression_by_reason.data_quality_suppressed,
      degraded_reason: publicationAllowed ? projected.degraded_reason : publicationBlockedReason,
      top_direction_candidate: normalized[0]?.direction ?? null,
      top_direction_published: publicationAllowed ? (projected.items[0]?.direction ?? null) : null,
    },
    item_rows: itemRows,
    projected,
    normalized_items: normalized,
  };
}
