import type { WorkerHealthResponsePayload, WorkerRouteContext } from '../types';

export async function tryHandleSystemRoute(route: WorkerRouteContext): Promise<Response | null> {
  const { env, url, corsHeaders } = route;

  if (url.pathname === '/health') {
    const result = await env.DB.prepare('SELECT 1 as ok').first<{ ok: number }>();
    const payload: WorkerHealthResponsePayload = {
      status: 'healthy',
      db: result?.ok === 1,
      timestamp: new Date().toISOString(),
      environment: env.DEPLOY_ENV || 'development',
      build_sha: env.BUILD_SHA || 'local-dev',
      build_timestamp: env.BUILD_TIMESTAMP || '1970-01-01T00:00:00.000Z',
      worker_version: env.WORKER_VERSION || 'pxi-dev',
    };
    return Response.json(
      payload,
      { headers: { ...corsHeaders, 'Cache-Control': 'no-store' } },
    );
  }

  if (url.pathname === '/og-image.svg') {
    const pxi = await env.DB.prepare(
      'SELECT score, label, status, delta_7d FROM pxi_scores ORDER BY date DESC LIMIT 1'
    ).first<{ score: number; label: string; status: string; delta_7d: number | null }>();

    if (!pxi) {
      return new Response('No data', { status: 404, headers: corsHeaders });
    }

    const categories = await env.DB.prepare(
      'SELECT category, score FROM category_scores WHERE date = (SELECT date FROM pxi_scores ORDER BY date DESC LIMIT 1) ORDER BY score DESC'
    ).all<{ category: string; score: number }>();

    const score = Math.round(pxi.score);
    const label = pxi.label;
    const delta7d = pxi.delta_7d;
    const deltaStr = delta7d !== null ? `${delta7d >= 0 ? '+' : ''}${delta7d.toFixed(1)}` : '';

    const statusColors: Record<string, string> = {
      max_pamp: '#00a3ff',
      pamping: '#00a3ff',
      neutral: '#949ba5',
      soft: '#949ba5',
      dumping: '#949ba5',
    };
    const color = statusColors[pxi.status] || '#949ba5';
    const isAccent = pxi.status === 'max_pamp' || pxi.status === 'pamping';

    const cats = (categories.results || []).slice(0, 7);
    const catBarY = 440;
    const catBarHeight = 3;
    const catBarGap = 26;
    const catLabelX = 540;
    const catBarX = 640;
    const catBarMaxW = 360;

    const catBars = cats.map((category, index) => {
      const y = catBarY + index * catBarGap;
      const width = Math.max(4, (category.score / 100) * catBarMaxW);
      const isHigh = category.score >= 70;
      const displayName = category.category.replace(/_/g, ' ');
      return `
    <text x="${catLabelX}" y="${y + 4}" text-anchor="end" font-family="'SF Mono', 'Menlo', monospace" font-size="11" fill="#949ba5" letter-spacing="1" text-transform="uppercase">${displayName}</text>
    <rect x="${catBarX}" y="${y - 1}" width="${catBarMaxW}" height="${catBarHeight}" rx="1.5" fill="#26272b"/>
    <rect x="${catBarX}" y="${y - 1}" width="${width}" height="${catBarHeight}" rx="1.5" fill="${isHigh ? '#00a3ff' : 'rgba(148,155,165,0.5)'}"/>
    <text x="${catBarX + catBarMaxW + 12}" y="${y + 4}" font-family="'SF Mono', 'Menlo', monospace" font-size="11" fill="#949ba5">${Math.round(category.score)}</text>`;
    }).join('');

    const scoreX = 200;
    const scoreY = 360;

    const svg = `<svg width="1200" height="630" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <radialGradient id="glow" cx="0.3" cy="0.5" r="0.4">
      <stop offset="0%" stop-color="#00a3ff" stop-opacity="0.04"/>
      <stop offset="100%" stop-color="#000000" stop-opacity="0"/>
    </radialGradient>
  </defs>
  <rect width="1200" height="630" fill="#000000"/>
  <rect width="1200" height="630" fill="url(#glow)"/>
  <rect width="1200" height="630" fill="url(#glow)"/>
  <rect x="0" y="0" width="1200" height="1" fill="#26272b"/>
  <text x="60" y="56" font-family="'SF Mono', 'Menlo', monospace" font-weight="500" font-size="14" fill="#949ba5" letter-spacing="3">PXI<tspan fill="#00a3ff">/</tspan>COMMAND</text>
  <text x="60" y="80" font-family="'SF Mono', 'Menlo', monospace" font-size="10" fill="#949ba5" opacity="0.4" letter-spacing="2">MACRO MARKET STRENGTH INDEX</text>
  <text x="${scoreX}" y="${scoreY}" text-anchor="middle" font-family="system-ui, -apple-system, 'Helvetica Neue', sans-serif" font-weight="200" font-size="200" fill="#f3f3f3" letter-spacing="-8">${score}</text>
  ${deltaStr ? `<text x="${scoreX}" y="${scoreY + 44}" text-anchor="middle" font-family="'SF Mono', 'Menlo', monospace" font-size="16" fill="${delta7d && delta7d >= 0 ? '#00a3ff' : '#949ba5'}" letter-spacing="1">${deltaStr}<tspan fill="#949ba5" opacity="0.5" font-size="11"> 7D</tspan></text>` : ''}
  <rect x="${scoreX - 60}" y="${scoreY + 60}" width="120" height="32" rx="3" fill="${color}" fill-opacity="${isAccent ? '1' : '0.15'}"/>
  <text x="${scoreX}" y="${scoreY + 82}" text-anchor="middle" font-family="'SF Mono', 'Menlo', monospace" font-weight="500" font-size="11" fill="${isAccent ? '#000000' : '#f3f3f3'}" letter-spacing="2">${label}</text>
  <line x1="440" y1="430" x2="440" y2="${catBarY + cats.length * catBarGap - 10}" stroke="#26272b" stroke-width="1" stroke-dasharray="4,4"/>
  ${catBars}
  <rect x="0" y="629" width="1200" height="1" fill="#26272b"/>
  <text x="1140" y="612" text-anchor="end" font-family="'SF Mono', 'Menlo', monospace" font-size="10" fill="#949ba5" opacity="0.3" letter-spacing="2">PXICOMMAND.COM</text>
</svg>`;

    return new Response(svg, {
      headers: {
        'Content-Type': 'image/svg+xml',
        'Cache-Control': 'public, max-age=300',
        ...corsHeaders,
      },
    });
  }

  return null;
}
