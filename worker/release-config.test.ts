import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import test from 'node:test';

function extractEnvBlock(config: string, envName: string, nextEnvName?: string): string {
  const startToken = `[env.${envName}]`;
  const startIndex = config.indexOf(startToken);
  assert.notEqual(startIndex, -1, `Missing ${startToken} block`);

  const endIndex = nextEnvName
    ? config.indexOf(`[env.${nextEnvName}]`, startIndex + startToken.length)
    : config.length;

  return config.slice(startIndex, endIndex === -1 ? config.length : endIndex);
}

function firstCapture(block: string, pattern: RegExp, label: string): string {
  const match = block.match(pattern);
  assert.ok(match?.[1], `Missing ${label}`);
  return match[1];
}

function captures(block: string, pattern: RegExp): string[] {
  return Array.from(block.matchAll(pattern), (match) => match[1]);
}

test('worker staging bindings are isolated from production', () => {
  const configPath = path.resolve(process.cwd(), 'worker/wrangler.toml');
  const config = readFileSync(configPath, 'utf8');

  const staging = extractEnvBlock(config, 'staging', 'production');
  const production = extractEnvBlock(config, 'production', 'fallback');

  const stagingDb = firstCapture(staging, /database_id = "([^"]+)"/, 'staging database_id');
  const productionDb = firstCapture(production, /database_id = "([^"]+)"/, 'production database_id');
  assert.notEqual(stagingDb, productionDb);

  const stagingVectorize = firstCapture(staging, /index_name = "([^"]+)"/, 'staging vectorize index');
  const productionVectorize = firstCapture(production, /index_name = "([^"]+)"/, 'production vectorize index');
  assert.notEqual(stagingVectorize, productionVectorize);

  const stagingKvIds = captures(staging, /id = "([0-9a-f]{32})"/g);
  const productionKvIds = captures(production, /id = "([0-9a-f]{32})"/g);
  assert.equal(stagingKvIds.length, 2);
  assert.equal(productionKvIds.length, 2);
  assert.notDeepEqual(stagingKvIds, productionKvIds);

  assert.match(staging, /name = "pxi-api-staging"/);
  assert.match(production, /name = "pxi-api-production"/);
  assert.doesNotMatch(staging, /\[\[env\.staging\.send_email\]\]/);
  assert.match(staging, /FEATURE_ENABLE_ALERTS_EMAIL = "false"/);
  assert.match(staging, /DEPLOY_ENV = "staging"/);
  assert.match(production, /DEPLOY_ENV = "production"/);
});

test('worker deploy workflow targets the configured production and staging script names', () => {
  const workflowPath = path.resolve(process.cwd(), '.github/workflows/deploy-worker.yml');
  const workflow = readFileSync(workflowPath, 'utf8');

  assert.match(workflow, /worker_script_name="pxi-api-production"/);
  assert.match(workflow, /worker_script_name="pxi-api-staging"/);
  assert.match(workflow, /--name "\$\{WORKER_SCRIPT_NAME\}"/);
});

test('frontend production verify workflow tolerates non-JSON build responses while polling', () => {
  const workflowPath = path.resolve(process.cwd(), '.github/workflows/verify-frontend-production.yml');
  const workflow = readFileSync(workflowPath, 'utf8');

  assert.match(workflow, /curl -sS -D \/tmp\/pxi-build\.headers -o \/tmp\/pxi-build\.json/);
  assert.match(workflow, /grep -qi "\^content-type: application\/json"/);
  assert.match(workflow, /try \{/);
  assert.match(workflow, /catch \{/);
});

test('score reconstruction workflow is versioned, bounded, and serialized with production mutation', () => {
  const reconstructionPath = path.resolve(
    process.cwd(),
    '.github/workflows/score-history-reconstruction.yml',
  );
  const dailyPath = path.resolve(process.cwd(), '.github/workflows/daily-refresh.yml');
  const deployPath = path.resolve(process.cwd(), '.github/workflows/deploy-worker.yml');
  const reconstruction = readFileSync(reconstructionPath, 'utf8');
  const daily = readFileSync(dailyPath, 'utf8');
  const deploy = readFileSync(deployPath, 'utf8');

  assert.match(reconstruction, /\/api\/history\/reconstruct-missing-v1/);
  assert.doesNotMatch(reconstruction, /-X POST "\$\{BASE_URL\}\/api\/backfill"/);
  assert.match(reconstruction, /dt\.timedelta\(days=2\)/);
  assert.match(reconstruction, /\[\.results\[\]\.date\] == \$expected_dates/);
  assert.match(reconstruction, /if \(\(index \+ 1 < CHUNK_COUNT\)\); then\s+sleep 4/);
  assert.match(reconstruction, /"expected_build_sha": build_sha/);

  for (const workflow of [reconstruction, daily]) {
    assert.match(workflow, /group: pxi-production-indicator-score-mutation/);
    assert.match(workflow, /cancel-in-progress: false/);
  }
  assert.match(deploy, /inputs\.environment == 'production'/);
  assert.match(deploy, /pxi-production-indicator-score-mutation/);
  assert.match(deploy, /Verify legacy backfill kill switch/);
  assert.match(deploy, /"\$\{http_code\}" != "410"/);
});
