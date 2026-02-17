import dotenv from 'dotenv';
dotenv.config();

import { writeFile } from 'node:fs/promises';

const WRITE_API_URL = process.env.WRITE_API_URL ?? '';
const WRITE_API_KEY = process.env.WRITE_API_KEY ?? '';
const MARKET_SUMMARY_PATH = process.env.MARKET_SUMMARY_PATH ?? '/tmp/pxi-market-summary.json';
const REFRESH_TRIGGER = process.env.REFRESH_TRIGGER ?? 'postrefresh_market_script';

function getBaseUrl(): string {
  if (!WRITE_API_URL) {
    throw new Error('WRITE_API_URL environment variable is required');
  }
  if (!WRITE_API_KEY) {
    throw new Error('WRITE_API_KEY environment variable is required');
  }
  return WRITE_API_URL.replace('/api/write', '');
}

async function main(): Promise<void> {
  const baseUrl = getBaseUrl();
  const response = await fetch(`${baseUrl}/api/market/refresh-products`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${WRITE_API_KEY}`,
      'Content-Type': 'application/json',
      'X-Refresh-Trigger': REFRESH_TRIGGER,
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Post-refresh market generation failed (${response.status}): ${text}`);
  }

  const result = await response.json();
  const output = {
    generated_at_utc: new Date().toISOString(),
    result,
  };

  await writeFile(MARKET_SUMMARY_PATH, `${JSON.stringify(output, null, 2)}\n`, 'utf8');
  console.log(`Market products refreshed. Summary written to ${MARKET_SUMMARY_PATH}`);
}

main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(message);
  process.exit(1);
});
