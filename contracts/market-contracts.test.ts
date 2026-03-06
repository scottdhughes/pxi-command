import assert from 'node:assert/strict';
import test from 'node:test';

import type {
  AlertsApiResponse as FrontendAlertsApiResponse,
  AlertsFeedResponse as FrontendAlertsFeedResponse,
  BriefData as FrontendBriefData,
  CalibrationDiagnosticsResponse as FrontendCalibrationDiagnosticsResponse,
  CategoryDetailData as FrontendCategoryDetailData,
  DecisionImpactResponse as FrontendDecisionImpactResponse,
  EdgeDiagnosticsResponse as FrontendEdgeDiagnosticsResponse,
  MLAccuracyApiResponse as FrontendMLAccuracyApiResponse,
  OpportunitiesResponse as FrontendOpportunitiesResponse,
  OpsDecisionImpactResponse as FrontendOpsDecisionImpactResponse,
  PlanData as FrontendPlanData,
  PXIData as FrontendPXIData,
  SignalData as FrontendSignalData,
} from '../frontend/src/lib/types';
import type {
  AlertsApiResponsePayload as WorkerAlertsApiResponse,
  AlertsFeedResponsePayload as WorkerAlertsFeedResponse,
  BriefSnapshot as WorkerBriefData,
  CalibrationDiagnosticsResponsePayload as WorkerCalibrationDiagnosticsResponse,
  CategoryDetailResponsePayload as WorkerCategoryDetailData,
  DecisionImpactOpsResponsePayload as WorkerOpsDecisionImpactResponse,
  DecisionImpactResponsePayload as WorkerDecisionImpactResponse,
  EdgeDiagnosticsResponsePayload as WorkerEdgeDiagnosticsResponse,
  MLAccuracyApiResponsePayload as WorkerMLAccuracyApiResponse,
  OpportunityFeedResponsePayload as WorkerOpportunitiesResponse,
  PlanPayload as WorkerPlanData,
  PXIResponsePayload as WorkerPXIData,
  SignalResponsePayload as WorkerSignalData,
} from '../worker/types';
import {
  alertsApiFixture,
  alertsFeedFixture,
  briefFixture,
  calibrationDiagnosticsFixture,
  categoryDetailFixture,
  contractFixtures,
  decisionImpactFixture,
  edgeDiagnosticsFixture,
  mlAccuracyFixture,
  opportunitiesFixture,
  opsDecisionImpactFixture,
  planFixture,
  pxiFixture,
  signalFixture,
} from './market-contract-fixtures';

type IsEqual<A, B> =
  (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2)
    ? ((<T>() => T extends B ? 1 : 2) extends (<T>() => T extends A ? 1 : 2) ? true : false)
    : false;

type AssertTrue<T extends true> = T;

type _AlertsApiMatch = AssertTrue<IsEqual<FrontendAlertsApiResponse, WorkerAlertsApiResponse>>;
type _AlertsFeedMatch = AssertTrue<IsEqual<FrontendAlertsFeedResponse, WorkerAlertsFeedResponse>>;
type _BriefMatch = AssertTrue<IsEqual<FrontendBriefData, WorkerBriefData>>;
type _CalibrationMatch = AssertTrue<
  IsEqual<FrontendCalibrationDiagnosticsResponse, WorkerCalibrationDiagnosticsResponse>
>;
type _CategoryMatch = AssertTrue<IsEqual<FrontendCategoryDetailData, WorkerCategoryDetailData>>;
type _DecisionImpactMatch = AssertTrue<IsEqual<FrontendDecisionImpactResponse, WorkerDecisionImpactResponse>>;
type _EdgeDiagnosticsMatch = AssertTrue<IsEqual<FrontendEdgeDiagnosticsResponse, WorkerEdgeDiagnosticsResponse>>;
type _MlAccuracyMatch = AssertTrue<IsEqual<FrontendMLAccuracyApiResponse, WorkerMLAccuracyApiResponse>>;
type _OpportunitiesMatch = AssertTrue<IsEqual<FrontendOpportunitiesResponse, WorkerOpportunitiesResponse>>;
type _OpsDecisionImpactMatch = AssertTrue<
  IsEqual<FrontendOpsDecisionImpactResponse, WorkerOpsDecisionImpactResponse>
>;
type _PlanMatch = AssertTrue<IsEqual<FrontendPlanData, WorkerPlanData>>;
type _PxiMatch = AssertTrue<IsEqual<FrontendPXIData, WorkerPXIData>>;
type _SignalMatch = AssertTrue<IsEqual<FrontendSignalData, WorkerSignalData>>;

function expectType<T>(value: T): T {
  return value;
}

const crossSurfaceAssignments = {
  alertsApi: expectType<FrontendAlertsApiResponse>(expectType<WorkerAlertsApiResponse>(alertsApiFixture)),
  alertsFeed: expectType<FrontendAlertsFeedResponse>(expectType<WorkerAlertsFeedResponse>(alertsFeedFixture)),
  brief: expectType<FrontendBriefData>(expectType<WorkerBriefData>(briefFixture)),
  calibrationDiagnostics: expectType<FrontendCalibrationDiagnosticsResponse>(
    expectType<WorkerCalibrationDiagnosticsResponse>(calibrationDiagnosticsFixture),
  ),
  categoryDetail: expectType<FrontendCategoryDetailData>(expectType<WorkerCategoryDetailData>(categoryDetailFixture)),
  decisionImpact: expectType<FrontendDecisionImpactResponse>(expectType<WorkerDecisionImpactResponse>(decisionImpactFixture)),
  edgeDiagnostics: expectType<FrontendEdgeDiagnosticsResponse>(
    expectType<WorkerEdgeDiagnosticsResponse>(edgeDiagnosticsFixture),
  ),
  mlAccuracy: expectType<FrontendMLAccuracyApiResponse>(expectType<WorkerMLAccuracyApiResponse>(mlAccuracyFixture)),
  opportunities: expectType<FrontendOpportunitiesResponse>(expectType<WorkerOpportunitiesResponse>(opportunitiesFixture)),
  opsDecisionImpact: expectType<FrontendOpsDecisionImpactResponse>(
    expectType<WorkerOpsDecisionImpactResponse>(opsDecisionImpactFixture),
  ),
  plan: expectType<FrontendPlanData>(expectType<WorkerPlanData>(planFixture)),
  pxi: expectType<FrontendPXIData>(expectType<WorkerPXIData>(pxiFixture)),
  signal: expectType<FrontendSignalData>(expectType<WorkerSignalData>(signalFixture)),
};

void crossSurfaceAssignments;

test('contract fixtures cover the shared worker/frontend route surface', () => {
  assert.deepEqual(Object.keys(contractFixtures).sort(), [
    'alertsApi',
    'alertsFeed',
    'brief',
    'calibrationDiagnostics',
    'categoryDetail',
    'decisionImpact',
    'edgeDiagnostics',
    'mlAccuracy',
    'opportunities',
    'opsDecisionImpact',
    'plan',
    'pxi',
    'signal',
  ]);
});

test('contract fixtures preserve cross-route invariants used by the frontend', () => {
  assert.equal(planFixture.as_of, briefFixture.as_of);
  assert.equal(planFixture.as_of, opportunitiesFixture.as_of);
  assert.equal(signalFixture.date, pxiFixture.date);
  assert.equal(briefFixture.policy_state.base_signal, signalFixture.signal.type);
  assert.equal(alertsApiFixture.count, alertsApiFixture.alerts.length);
  assert.equal(mlAccuracyFixture.coverage?.evaluated_count, mlAccuracyFixture.evaluated_count);
  assert.equal(decisionImpactFixture.market.sample_size > 0, true);
  assert.equal(opsDecisionImpactFixture.theme_summary.top_positive[0]?.theme_id, opportunitiesFixture.items[0]?.theme_id);
  assert.equal(categoryDetailFixture.category, 'macro');
  assert.equal(edgeDiagnosticsFixture.windows[0]?.calibration_diagnostics.quality_band, 'ROBUST');
});
