import type { Env } from "./config"

export async function putObject(env: Env, key: string, body: string, contentType: string) {
  await env.SIGNALS_BUCKET.put(key, body, {
    httpMetadata: { contentType },
  })
}

export async function getObjectText(env: Env, key: string): Promise<string | null> {
  const obj = await env.SIGNALS_BUCKET.get(key)
  if (!obj) return null
  return await obj.text()
}

export async function setLatestRun(env: Env, runId: string, ts: string) {
  await env.SIGNALS_KV.put("latest_run_id", runId)
  await env.SIGNALS_KV.put("latest_run_at_utc", ts)
}

export async function getLatestRunId(env: Env): Promise<string | null> {
  return await env.SIGNALS_KV.get("latest_run_id")
}
