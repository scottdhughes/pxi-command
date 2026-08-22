import { unauthorizedResponse } from './http';
import type { Env } from '../types';

const publicRateLimitStore = new Map<string, { count: number; resetTime: number }>();
const adminRateLimitStore = new Map<string, { count: number; resetTime: number }>();
const routeRateLimitStore = new Map<string, { count: number; resetTime: number }>();
const RATE_LIMIT = 100;
const RATE_WINDOW = 60 * 1000;
const ADMIN_RATE_LIMIT = 20;
const ADMIN_RATE_WINDOW = 60 * 1000;

export function checkRateLimitStore(
  ip: string,
  limit: number,
  windowMs: number,
  store: Map<string, { count: number; resetTime: number }>
): boolean {
  const now = Date.now();
  const record = store.get(ip);

  if (!record || now > record.resetTime) {
    store.set(ip, { count: 1, resetTime: now + windowMs });
    return true;
  }

  if (record.count >= limit) {
    return false;
  }

  record.count += 1;
  return true;
}

export async function checkRateLimitKV(
  keySuffix: string,
  limit: number,
  windowMs: number,
  kv?: KVNamespace,
  namespace = 'admin',
): Promise<boolean> {
  if (!kv) {
    return true;
  }

  const now = Date.now();
  const key = `rate_limit:${namespace}:${keySuffix}`;
  let count = 0;
  let resetTime = now + windowMs;

  const raw = await kv.get(key);
  if (raw) {
    try {
      const parsed = JSON.parse(raw) as { count: number; resetTime: number };
      if (Number.isFinite(parsed.count) && Number.isFinite(parsed.resetTime) && parsed.resetTime > now) {
        count = parsed.count;
        resetTime = parsed.resetTime;
      }
    } catch (err) {
      console.error('Failed to parse admin rate limit KV record', err);
    }
  }

  if (count >= limit && resetTime > now) {
    return false;
  }

  const nextResetTime = resetTime > now ? resetTime : now + windowMs;
  // Workers KV rejects expiration_ttl values below 60 seconds. The logical
  // reset timestamp remains authoritative if the record outlives its window.
  const nextTtl = Math.max(60, Math.ceil((nextResetTime - now) / 1000));
  const nextCount = count + 1;

  await kv.put(key, JSON.stringify({ count: nextCount, resetTime: nextResetTime }), {
    expirationTtl: nextTtl,
  });

  return true;
}

export async function checkPublicRateLimit(ip: string, env: Env): Promise<boolean> {
  const localOk = checkRateLimitStore(ip, RATE_LIMIT, RATE_WINDOW, publicRateLimitStore);
  if (!localOk) return false;
  if (!env.RATE_LIMIT_KV) return true;

  try {
    return await checkRateLimitKV(ip, RATE_LIMIT, RATE_WINDOW, env.RATE_LIMIT_KV, 'public');
  } catch (err) {
    console.error('Public KV rate limit check failed, using in-memory fallback', err);
    return true;
  }
}

export async function checkRouteRateLimit(
  scope: string,
  key: string,
  limit: number,
  windowMs: number,
  env: Env,
): Promise<boolean> {
  const localKey = `${scope}:${key}`;
  const localOk = checkRateLimitStore(localKey, limit, windowMs, routeRateLimitStore);
  if (!localOk) return false;
  if (!env.RATE_LIMIT_KV) return true;

  try {
    return await checkRateLimitKV(key, limit, windowMs, env.RATE_LIMIT_KV, `route:${scope}`);
  } catch (err) {
    console.error(`Route KV rate limit check failed for ${scope}, using in-memory fallback`, err);
    return true;
  }
}

export async function checkAdminRateLimit(ip: string, env: Env): Promise<boolean> {
  const nowOk = checkRateLimitStore(ip, ADMIN_RATE_LIMIT, ADMIN_RATE_WINDOW, adminRateLimitStore);
  if (!nowOk) {
    return false;
  }

  if (!env.RATE_LIMIT_KV) {
    return true;
  }

  try {
    return await checkRateLimitKV(ip, ADMIN_RATE_LIMIT, ADMIN_RATE_WINDOW, env.RATE_LIMIT_KV);
  } catch (err) {
    console.error('Admin KV rate limit check failed, using in-memory fallback', err);
    return true;
  }
}

export function getRequestToken(request: Request): string {
  const authHeader = request.headers.get('Authorization');
  if (authHeader?.startsWith('Bearer ')) {
    return authHeader.slice(7).trim();
  }

  const adminToken = request.headers.get('X-Admin-Token');
  return adminToken?.trim() || '';
}

export function constantTimeEquals(a: string, b: string): boolean {
  const left = new TextEncoder().encode(a || '');
  const right = new TextEncoder().encode(b || '');
  const maxLen = Math.max(left.length, right.length);
  let diff = left.length ^ right.length;

  for (let i = 0; i < maxLen; i += 1) {
    const lv = left[i] ?? 0;
    const rv = right[i] ?? 0;
    diff |= lv ^ rv;
  }

  return diff === 0;
}

export function hasWriteAccess(request: Request, env: Env): boolean {
  const expected = env.WRITE_API_KEY || '';
  const token = getRequestToken(request);
  if (!expected || token.length === 0) return false;
  return constantTimeEquals(token, expected);
}

export async function enforceAdminAuth(
  request: Request,
  env: Env,
  corsHeaders: Record<string, string>,
  clientIP: string
): Promise<Response | null> {
  if (!(await checkAdminRateLimit(clientIP, env))) {
    return Response.json(
      { error: 'Too many requests' },
      { status: 429, headers: { ...corsHeaders, 'Retry-After': '60' } }
    );
  }

  if (!hasWriteAccess(request, env)) {
    return unauthorizedResponse(corsHeaders);
  }

  return null;
}
