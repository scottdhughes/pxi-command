import { handleScheduled } from './runtime/legacy';
import {
  getCorsHeaders as buildCorsHeaders,
  methodNotAllowedResponse,
  preflightResponse,
} from './lib/http';
import {
  checkPublicRateLimit as checkPublicRequestRateLimit,
  checkRouteRateLimit,
} from './lib/security';
import { createRouteDeps } from './bootstrap/create-route-deps';
import { tryHandleMarketCoreRoute } from './domain/market-core';
import { tryHandleMarketProductsRoute } from './domain/market-products';
import { tryHandleMarketOpsRoute } from './domain/market-ops';
import { tryHandleMarketLifecycleRoute } from './routes/market-lifecycle';
import { tryHandleSystemRoute } from './routes/system';
import { tryHandlePublicReadRoute } from './routes/public-read';
import { tryHandleSimilarityRoute } from './routes/similarity';
import { tryHandleAdminIngestionRoute } from './routes/admin-ingestion';
import { tryHandleModelingRoute } from './routes/modeling';
import { consumeRequestBudget } from './data/request-rate-limits';
import type { Env, WorkerRouteContext } from './types';

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (url.hostname === 'www.pxicommand.com') {
      return Response.redirect(`https://pxicommand.com${url.pathname}${url.search}`, 301);
    }

    const origin = request.headers.get('Origin');
    const corsHeaders = buildCorsHeaders(origin);
    const method = request.method === 'HEAD' ? 'GET' : request.method;
    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';

    if (!(await checkPublicRequestRateLimit(clientIP, env))) {
      return Response.json(
        { error: 'Too many requests' },
        { status: 429, headers: { ...corsHeaders, 'Retry-After': '60' } },
      );
    }

    const routeLimits: Record<string, { limit: number; windowMs: number }> = {
      '/api/predict': { limit: 20, windowMs: 60_000 },
      '/api/ml/backtest': { limit: 10, windowMs: 60_000 },
      '/api/backtest': { limit: 10, windowMs: 60_000 },
      '/api/backtest/signal': { limit: 10, windowMs: 60_000 },
      '/api/similar': { limit: 20, windowMs: 60_000 },
    };
    const routeLimit = routeLimits[url.pathname];
    if (method === 'GET' && routeLimit && !(await checkRouteRateLimit(
      url.pathname,
      clientIP,
      routeLimit.limit,
      routeLimit.windowMs,
      env,
    ))) {
      return Response.json(
        { error: 'Too many requests' },
        { status: 429, headers: { ...corsHeaders, 'Retry-After': '60' } },
      );
    }

    if (method === 'POST' && url.pathname === '/api/alerts/subscribe/start') {
      const sourceAllowed = await consumeRequestBudget(env.DB, 'subscribe-source', clientIP, 5, 60 * 60);
      if (!sourceAllowed) {
        return Response.json(
          { error: 'Too many requests' },
          { status: 429, headers: { ...corsHeaders, 'Retry-After': '3600' } },
        );
      }
      const globalAllowed = await consumeRequestBudget(env.DB, 'subscribe-global', 'all', 200, 60 * 60);
      if (!globalAllowed) {
        return Response.json(
          { error: 'Too many requests' },
          { status: 429, headers: { ...corsHeaders, 'Retry-After': '3600' } },
        );
      }
    }

    if (method === 'POST' && url.pathname === '/api/metrics/utility-event') {
      const sourceAllowed = await consumeRequestBudget(env.DB, 'utility-source', clientIP, 300, 60 * 60);
      if (!sourceAllowed) {
        return Response.json(
          { error: 'Too many requests' },
          { status: 429, headers: { ...corsHeaders, 'Retry-After': '3600' } },
        );
      }
      const globalAllowed = await consumeRequestBudget(env.DB, 'utility-global', 'all', 5000, 60 * 60);
      if (!globalAllowed) {
        return Response.json(
          { error: 'Too many requests' },
          { status: 429, headers: { ...corsHeaders, 'Retry-After': '3600' } },
        );
      }
    }

    if (!['GET', 'POST', 'OPTIONS', 'HEAD'].includes(request.method)) {
      return methodNotAllowedResponse(corsHeaders);
    }

    if (request.method === 'OPTIONS') {
      return preflightResponse(corsHeaders);
    }

    const routeContext: WorkerRouteContext = {
      request,
      env,
      url,
      method,
      corsHeaders,
      clientIP,
      executionContext: ctx,
    };
    const routeDeps = createRouteDeps();

    try {
      const marketCoreResponse = await tryHandleMarketCoreRoute(routeContext, routeDeps);
      if (marketCoreResponse) return marketCoreResponse;

      const marketProductsResponse = await tryHandleMarketProductsRoute(routeContext, routeDeps);
      if (marketProductsResponse) return marketProductsResponse;

      const marketLifecycleResponse = await tryHandleMarketLifecycleRoute(routeContext, routeDeps);
      if (marketLifecycleResponse) return marketLifecycleResponse;

      const marketOpsResponse = await tryHandleMarketOpsRoute(routeContext, routeDeps);
      if (marketOpsResponse) return marketOpsResponse;

      const systemResponse = await tryHandleSystemRoute(routeContext);
      if (systemResponse) return systemResponse;

      const publicReadResponse = await tryHandlePublicReadRoute(routeContext, routeDeps);
      if (publicReadResponse) return publicReadResponse;

      const similarityResponse = await tryHandleSimilarityRoute(routeContext, routeDeps);
      if (similarityResponse) return similarityResponse;

      const adminIngestionResponse = await tryHandleAdminIngestionRoute(routeContext, routeDeps);
      if (adminIngestionResponse) return adminIngestionResponse;

      const modelingResponse = await tryHandleModelingRoute(routeContext, routeDeps);
      if (modelingResponse) return modelingResponse;

      return Response.json({ error: 'Not found' }, { status: 404, headers: corsHeaders });
    } catch (err: unknown) {
      console.error('API error:', err instanceof Error ? err.message : err);
      return Response.json({ error: 'Service unavailable' }, { status: 500, headers: corsHeaders });
    }
  },

  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(handleScheduled(env));
  },
};
