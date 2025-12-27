// Cloudflare Worker to generate dynamic OG images for PXI
// Returns SVG image with current score and status

import { neon } from '@neondatabase/serverless';

interface Env {
  DATABASE_URL: string;
}

const STATUS_COLORS: Record<string, string> = {
  max_pamp: '#00a3ff',
  pamping: '#00a3ff',
  neutral: '#949ba5',
  soft: '#949ba5',
  dumping: '#949ba5',
};

export async function generateOGImage(env: Env): Promise<Response> {
  const sql = neon(env.DATABASE_URL);

  // Get latest PXI score
  const rows = await sql`
    SELECT score, label, status FROM pxi_scores ORDER BY date DESC LIMIT 1
  ` as { score: string; label: string; status: string }[];

  const pxi = rows[0];
  if (!pxi) {
    return new Response('No data', { status: 404 });
  }

  const score = Math.round(parseFloat(pxi.score));
  const label = pxi.label;
  const color = STATUS_COLORS[pxi.status] || '#949ba5';

  // Generate SVG
  const svg = `
<svg width="1200" height="630" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;500&amp;family=JetBrains+Mono:wght@500&amp;display=swap');
    </style>
  </defs>

  <!-- Background -->
  <rect width="1200" height="630" fill="#000000"/>

  <!-- Score -->
  <text x="600" y="340" text-anchor="middle" font-family="Inter, sans-serif" font-weight="300" font-size="220" fill="#f3f3f3">${score}</text>

  <!-- Status Badge -->
  <rect x="475" y="400" width="250" height="44" rx="4" fill="${color}" fill-opacity="${pxi.status === 'neutral' || pxi.status === 'soft' ? '0.2' : '1'}"/>
  <text x="600" y="432" text-anchor="middle" font-family="JetBrains Mono, monospace" font-weight="500" font-size="16" fill="${pxi.status === 'neutral' || pxi.status === 'soft' ? '#f3f3f3' : '#000000'}" letter-spacing="2">${label}</text>

  <!-- Header -->
  <text x="600" y="100" text-anchor="middle" font-family="JetBrains Mono, monospace" font-weight="500" font-size="18" fill="#949ba5" letter-spacing="4">PXI/COMMAND</text>

  <!-- Footer -->
  <text x="600" y="580" text-anchor="middle" font-family="Inter, sans-serif" font-weight="400" font-size="14" fill="#949ba5" opacity="0.5">MACRO MARKET STRENGTH INDEX</text>
</svg>`;

  return new Response(svg, {
    headers: {
      'Content-Type': 'image/svg+xml',
      'Cache-Control': 'public, max-age=300', // Cache for 5 minutes
    },
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    return generateOGImage(env);
  },
};
