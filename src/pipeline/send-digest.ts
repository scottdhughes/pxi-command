import dotenv from 'dotenv';
dotenv.config();

import { writeFile } from 'node:fs/promises';

const WRITE_API_URL = process.env.WRITE_API_URL ?? '';
const WRITE_API_KEY = process.env.WRITE_API_KEY ?? '';
const DIGEST_SUMMARY_PATH = process.env.DIGEST_SUMMARY_PATH ?? '/tmp/pxi-digest-summary.json';
const MAX_ATTEMPTS = 4;
const RETRYABLE_STATUS = new Set([429, 500, 502, 503, 504]);

function getBaseUrl(): string {
  if (!WRITE_API_URL) {
    throw new Error('WRITE_API_URL environment variable is required');
  }
  if (!WRITE_API_KEY) {
    throw new Error('WRITE_API_KEY environment variable is required');
  }
  return WRITE_API_URL.replace('/api/write', '');
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function requestDigest(baseUrl: string): Promise<Response> {
  return fetch(`${baseUrl}/api/market/send-digest`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${WRITE_API_KEY}`,
      'Content-Type': 'application/json',
    },
  });
}

async function main(): Promise<void> {
  const baseUrl = getBaseUrl();

  let lastErrorMessage = 'Daily digest dispatch failed';
  let response: Response | null = null;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1) {
    try {
      response = await requestDigest(baseUrl);
    } catch (error) {
      lastErrorMessage = error instanceof Error ? error.message : String(error);
      if (attempt < MAX_ATTEMPTS) {
        const waitMs = attempt * 1000;
        console.warn(`Digest request network error (attempt ${attempt}/${MAX_ATTEMPTS}): ${lastErrorMessage}. Retrying in ${waitMs}ms...`);
        await wait(waitMs);
        continue;
      }
      throw new Error(`Daily digest dispatch failed after ${MAX_ATTEMPTS} attempts: ${lastErrorMessage}`);
    }

    if (response.ok) {
      break;
    }

    const text = await response.text();
    lastErrorMessage = `Daily digest dispatch failed (${response.status}): ${text}`;
    if (attempt >= MAX_ATTEMPTS || !RETRYABLE_STATUS.has(response.status)) {
      throw new Error(lastErrorMessage);
    }

    const waitMs = attempt * 1000;
    console.warn(`${lastErrorMessage}. Retrying in ${waitMs}ms...`);
    await wait(waitMs);
  }

  if (!response || !response.ok) {
    throw new Error(lastErrorMessage);
  }

  const result = await response.json();
  const output = {
    generated_at_utc: new Date().toISOString(),
    result,
  };

  await writeFile(DIGEST_SUMMARY_PATH, `${JSON.stringify(output, null, 2)}\n`, 'utf8');
  console.log(`Daily digest dispatched. Summary written to ${DIGEST_SUMMARY_PATH}`);
}

main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(message);
  process.exit(1);
});
