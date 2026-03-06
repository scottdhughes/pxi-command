import { handleScheduled } from './runtime/legacy';
import {
  getCorsHeaders as buildCorsHeaders,
  methodNotAllowedResponse,
  preflightResponse,
} from './lib/http';
import { checkPublicRateLimit as checkPublicRequestRateLimit } from './lib/security';
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

    if (!checkPublicRequestRateLimit(clientIP)) {
      return Response.json(
        { error: 'Too many requests' },
        { status: 429, headers: { ...corsHeaders, 'Retry-After': '60' } },
      );
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
