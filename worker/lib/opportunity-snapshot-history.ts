export const HISTORICAL_BACKFILL_SEED_MARKER = 'historical_backfill_seed';

export function isHistoricalBackfillOpportunitySnapshot(snapshot: unknown): boolean {
  if (!snapshot || typeof snapshot !== 'object') return false;
  const items = (snapshot as { items?: unknown }).items;
  if (!Array.isArray(items)) return false;

  return items.some((item) => {
    if (!item || typeof item !== 'object') return false;
    const candidate = item as {
      supporting_factors?: unknown;
      expectancy?: { unavailable_reason?: unknown } | null;
    };
    const supportingFactors = Array.isArray(candidate.supporting_factors)
      ? candidate.supporting_factors
      : [];
    return supportingFactors.some((factor) => factor === HISTORICAL_BACKFILL_SEED_MARKER)
      || candidate.expectancy?.unavailable_reason === HISTORICAL_BACKFILL_SEED_MARKER;
  });
}

export function parseProspectiveOpportunitySnapshot<T extends { items: unknown[] }>(
  raw: string | null | undefined,
): T | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== 'object') return null;
    if (!Array.isArray((parsed as { items?: unknown }).items)) return null;
    if (isHistoricalBackfillOpportunitySnapshot(parsed)) return null;
    return parsed as T;
  } catch {
    return null;
  }
}
