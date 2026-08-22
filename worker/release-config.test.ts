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

const EXPECTED_PRODUCTION_CRONS = [
  '0 6 * * *',
  '0 14 * * *',
  '0 18 * * *',
  '0 22 * * 1-5',
  '30 23 * * 1-5',
] as const;

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
  assert.match(
    production,
    /\[\[env\.production\.services\]\]\s+binding = "SIGNALS_SERVICE"\s+service = "pxi-signals"/,
  );
  assert.doesNotMatch(staging, /SIGNALS_SERVICE/);
  assert.doesNotMatch(staging, /\[\[env\.staging\.send_email\]\]/);
  assert.match(staging, /FEATURE_ENABLE_ALERTS_EMAIL = "false"/);
  assert.match(staging, /DEPLOY_ENV = "staging"/);
  assert.match(production, /DEPLOY_ENV = "production"/);
});

test('Cloudflare exclusively owns the production automatic refresh schedule', () => {
  const configPath = path.resolve(process.cwd(), 'worker/wrangler.toml');
  const dailyPath = path.resolve(process.cwd(), '.github/workflows/daily-refresh.yml');
  const watchdogPath = path.resolve(process.cwd(), '.github/workflows/refresh-watchdog.yml');
  const config = readFileSync(configPath, 'utf8');
  const daily = readFileSync(dailyPath, 'utf8');
  const watchdog = readFileSync(watchdogPath, 'utf8');

  const staging = extractEnvBlock(config, 'staging', 'production');
  const production = extractEnvBlock(config, 'production', 'fallback');
  const fallback = extractEnvBlock(config, 'fallback');
  const cronBlock = production.match(
    /^\[env\.production\.triggers\]\s*$\n(?:#.*\n)*crons\s*=\s*\[(?<values>[\s\S]*?)\]/m,
  );
  assert.ok(cronBlock?.groups?.values, 'Missing production Cron Trigger list');
  assert.deepEqual(captures(cronBlock.groups.values, /"([^"]+)"/g), EXPECTED_PRODUCTION_CRONS);
  assert.doesNotMatch(staging, /^\[env\.staging\.triggers\]\s*$/m);
  assert.doesNotMatch(fallback, /^\[env\.fallback\.triggers\]\s*$/m);
  assert.doesNotMatch(config.slice(0, config.indexOf('[env.staging]')), /^\[triggers\]\s*$/m);

  assert.doesNotMatch(daily, /^\s{2}schedule:\s*$/m);
  assert.doesNotMatch(daily, /^\s*-\s*cron:\s*/m);
  assert.match(daily, /^\s{2}workflow_dispatch:\s*$/m);
  assert.match(daily, /^\s{6}record_research_evidence:\s*$/m);

  assert.match(watchdog, /^\s{2}schedule:\s*$/m);
  assert.deepEqual(captures(watchdog, /^\s*-\s*cron:\s*'([^']+)'/gm), ['50 23 * * 1-5']);
  assert.match(watchdog, /^\s{2}workflow_dispatch:\s*$/m);
  assert.match(watchdog, /https:\/\/api\.pxicommand\.com\/health\/refresh/);
});

test('worker deploy workflow targets the configured production and staging script names', () => {
  const workflowPath = path.resolve(process.cwd(), '.github/workflows/deploy-worker.yml');
  const workflow = readFileSync(workflowPath, 'utf8');

  assert.match(workflow, /worker_script_name="pxi-api-production"/);
  assert.match(workflow, /worker_script_name="pxi-api-staging"/);
  assert.match(workflow, /--name "\$\{WORKER_SCRIPT_NAME\}"/);

  const secretSync = workflow.slice(
    workflow.indexOf('Sync production FRED secret to Worker'),
    workflow.indexOf('Acquire production mutation lease'),
  );
  assert.match(secretSync, /wrangler secret put FRED_API_KEY/);
  assert.match(secretSync, /--env production/);
  assert.doesNotMatch(secretSync, /--name/);
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
  const marketBackfillPath = path.resolve(process.cwd(), '.github/workflows/market-backfill.yml');
  const deployPath = path.resolve(process.cwd(), '.github/workflows/deploy-worker.yml');
  const reconstruction = readFileSync(reconstructionPath, 'utf8');
  const daily = readFileSync(dailyPath, 'utf8');
  const marketBackfill = readFileSync(marketBackfillPath, 'utf8');
  const deploy = readFileSync(deployPath, 'utf8');

  assert.match(reconstruction, /\/api\/history\/reconstruct-missing-v1/);
  assert.doesNotMatch(reconstruction, /-X POST "\$\{BASE_URL\}\/api\/backfill"/);
  assert.match(reconstruction, /dt\.timedelta\(days=2\)/);
  assert.match(reconstruction, /\[\.results\[\]\.date\] == \$expected_dates/);
  assert.match(reconstruction, /if \(\(index \+ 1 < CHUNK_COUNT\)\); then\s+sleep 4/);
  assert.match(reconstruction, /"expected_build_sha": build_sha/);

  for (const workflow of [reconstruction, daily, marketBackfill]) {
    assert.match(workflow, /group: pxi-production-indicator-score-mutation/);
    assert.match(workflow, /cancel-in-progress: false/);
  }
  assert.match(deploy, /inputs\.environment == 'production'/);
  assert.match(deploy, /pxi-production-indicator-score-mutation/);
  assert.match(deploy, /Verify legacy backfill kill switch/);
  assert.match(deploy, /"\$\{http_code\}" != "410"/);

  assert.match(daily, /HOLDER_ID="github_daily_refresh:\$\{GITHUB_RUN_ID\}:\$\{GITHUB_RUN_ATTEMPT\}"/);
  assert.match(daily, /--arg holder_type 'github_daily_refresh'/);
  assert.match(daily, /timeout-minutes: 55/);
  assert.match(daily, /\/api\/admin\/refresh\/lease\/acquire/);
  assert.match(daily, /\/api\/admin\/refresh\/lease\/release/);
  assert.ok(daily.indexOf('Acquire production mutation lease') < daily.indexOf('npm run cron:daily'));
  assert.ok(daily.indexOf('npm run cron:daily') < daily.indexOf('Release production mutation lease'));
  const dailyRelease = daily.slice(
    daily.indexOf('Release production mutation lease'),
    daily.indexOf('Verify immutable research snapshot capture'),
  );
  assert.match(dailyRelease, /if: \$\{\{ always\(\) \}\}/);

  assert.match(reconstruction, /HOLDER_ID="history_reconstruction:\$\{GITHUB_RUN_ID\}:\$\{GITHUB_RUN_ATTEMPT\}"/);
  assert.match(reconstruction, /--arg holder_type 'history_reconstruction'/);
  assert.ok(
    reconstruction.indexOf('Acquire production mutation lease')
      < reconstruction.indexOf('-X POST "${BASE_URL}/api/history/reconstruct-missing-v1"'),
  );
  assert.ok(
    reconstruction.indexOf('-X POST "${BASE_URL}/api/history/reconstruct-missing-v1"')
      < reconstruction.indexOf('Release production mutation lease'),
  );
  const reconstructionRelease = reconstruction.slice(
    reconstruction.indexOf('Release production mutation lease'),
    reconstruction.indexOf('Append audit summary'),
  );
  assert.match(reconstructionRelease, /if: \$\{\{ always\(\) \}\}/);

  assert.match(deploy, /HOLDER_ID="deploy:\$\{GITHUB_RUN_ID\}:\$\{GITHUB_RUN_ATTEMPT\}"/);
  assert.match(deploy, /--arg holder_type 'deploy'/);
  assert.ok(
    deploy.indexOf('Acquire production mutation lease')
      < deploy.indexOf('Apply remote D1 migrations'),
  );
  assert.ok(
    deploy.indexOf('Release production mutation lease before refresh smoke')
      < deploy.indexOf('- name: Run refresh smoke'),
  );
  assert.match(deploy, /Best-effort production mutation lease cleanup/);

  assert.match(marketBackfill, /HOLDER_ID="market_backfill:\$\{GITHUB_RUN_ID\}:\$\{GITHUB_RUN_ATTEMPT\}"/);
  assert.match(marketBackfill, /--arg holder_type 'market_backfill'/);
  assert.match(marketBackfill, /timeout-minutes: 55/);
  assert.ok(
    marketBackfill.indexOf('Acquire production mutation lease')
      < marketBackfill.indexOf('-X POST "${BASE_URL}/api/market/backfill-products"'),
  );
  assert.ok(
    marketBackfill.indexOf('-X POST "${BASE_URL}/api/market/refresh-products"')
      < marketBackfill.indexOf('Release production mutation lease'),
  );
  assert.match(marketBackfill, /always\(\) && github\.event\.inputs\.dry_run != 'true'/);

  for (const workflow of [daily, reconstruction, deploy, marketBackfill]) {
    const leaseMinutes = Array.from(
      workflow.matchAll(/--argjson lease_minutes (\d+)/g),
      (match) => Number(match[1]),
    );
    assert.ok(leaseMinutes.length > 0);
    assert.ok(leaseMinutes.every((minutes) => minutes >= 1 && minutes <= 60));
  }
});
