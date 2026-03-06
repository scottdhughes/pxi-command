import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import test from 'node:test';

test('createRouteDeps wires the plan-critical decision helpers from legacy', () => {
  const routeDepsPath = path.resolve(process.cwd(), 'worker/bootstrap/create-route-deps.ts');
  const source = readFileSync(routeDepsPath, 'utf8');

  assert.match(source, /buildUncertaintySnapshot: legacy\.buildUncertaintySnapshot/);
  assert.match(source, /computeRiskSizingSnapshot: legacy\.computeRiskSizingSnapshot/);
  assert.match(source, /buildPlanFallbackPayload: legacy\.buildPlanFallbackPayload/);
});
