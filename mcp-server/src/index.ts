#!/usr/bin/env node
/**
 * PXI MCP Server
 *
 * Provides AI agents with tools to query macro market conditions.
 * Enables Claude and other LLM agents to make data-driven financial decisions.
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  Tool,
} from '@modelcontextprotocol/sdk/types.js';

const API_URL = process.env.PXI_API_URL || 'https://pxi-api.novoamorx1.workers.dev';

// Tool definitions
const TOOLS: Tool[] = [
  {
    name: 'get_pxi',
    description: `Get the current PXI (Positioning Index) score and market regime.

PXI synthesizes 28 indicators across 7 categories (credit, liquidity, volatility, breadth, positioning, macro, global) into a single 0-100 score:
- 80+: MAX PAMP (extremely bullish)
- 65-79: PAMPING (bullish)
- 45-64: NEUTRAL (mixed signals)
- 30-44: SOFT (bearish)
- <30: DUMPING (extremely bearish)

Use this to understand current macro market conditions before making investment decisions.`,
    inputSchema: {
      type: 'object',
      properties: {},
      required: [],
    },
  },
  {
    name: 'get_predictions',
    description: `Get ML ensemble predictions for future PXI changes.

Returns weighted predictions from XGBoost (60%) and LSTM (40%) models:
- 7-day prediction: Short-term PXI change forecast
- 30-day prediction: Medium-term PXI change forecast

Each prediction includes:
- value: Expected change in PXI points
- direction: STRONG_UP, UP, FLAT, DOWN, STRONG_DOWN
- confidence: HIGH (models agree), MEDIUM (partial agreement), LOW (models disagree)

Use this to anticipate market regime changes and adjust positioning.`,
    inputSchema: {
      type: 'object',
      properties: {},
      required: [],
    },
  },
  {
    name: 'get_similar_periods',
    description: `Find historically similar market periods using vector embeddings.

Identifies past periods with similar PXI scores and category distributions, then shows what happened next. Useful for:
- Understanding potential forward returns
- Identifying historical analogs
- Assessing risk based on precedent

Returns similar periods with their dates, similarity scores, and subsequent market performance.`,
    inputSchema: {
      type: 'object',
      properties: {},
      required: [],
    },
  },
  {
    name: 'get_signal',
    description: `Get the PXI risk allocation signal.

Returns a recommended risk exposure level (0-100%) based on:
- Current PXI score and trend
- Momentum indicators
- Volatility conditions

Use this for portfolio risk management and position sizing decisions.`,
    inputSchema: {
      type: 'object',
      properties: {},
      required: [],
    },
  },
  {
    name: 'get_regime',
    description: `Get detailed market regime analysis.

Returns the current market regime classification with:
- Regime type (risk-on, risk-off, transition)
- Category breakdown (which factors are bullish/bearish)
- Key drivers of current conditions

Use this to understand the "why" behind the PXI score.`,
    inputSchema: {
      type: 'object',
      properties: {},
      required: [],
    },
  },
  {
    name: 'get_market_context',
    description: `Get comprehensive market context optimized for AI agent decision-making.

Returns a single payload with everything an agent needs:
- Current PXI score and regime
- ML predictions with confidence
- Suggested action (increase/decrease/hold exposure)
- Key risk factors
- Recent changes and trends

This is the recommended tool for agents that need to make portfolio decisions.`,
    inputSchema: {
      type: 'object',
      properties: {},
      required: [],
    },
  },
  {
    name: 'get_history',
    description: `Get historical PXI scores for trend analysis.

Returns PXI scores over a specified time period. Useful for:
- Analyzing trends
- Backtesting strategies
- Understanding market cycles

Defaults to last 30 days if no parameters specified.`,
    inputSchema: {
      type: 'object',
      properties: {
        days: {
          type: 'number',
          description: 'Number of days of history to retrieve (default: 30, max: 365)',
        },
      },
      required: [],
    },
  },
];

// API fetch helper
async function fetchAPI(endpoint: string): Promise<any> {
  const response = await fetch(`${API_URL}${endpoint}`);
  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`);
  }
  return response.json();
}

// Tool handlers
async function getPxi(): Promise<string> {
  const data = await fetchAPI('/api/pxi');

  // Categories is an array of {name, score, weight}
  const categoryLines = Array.isArray(data.categories)
    ? data.categories.map((c: any) => `- ${c.name}: ${c.score?.toFixed(1)}`).join('\n')
    : 'N/A';

  const summary = `## Current PXI: ${data.score?.toFixed(1)} (${data.label})

**Date:** ${data.date}
**Status:** ${data.status}

### Category Scores
${categoryLines}

### Recent Trend
- 1-day change: ${data.delta?.d1?.toFixed(1) || 'N/A'} points
- 7-day change: ${data.delta?.d7?.toFixed(1) || 'N/A'} points
- 30-day change: ${data.delta?.d30?.toFixed(1) || 'N/A'} points`;

  return summary;
}

async function getPredictions(): Promise<string> {
  const data = await fetchAPI('/api/ml/ensemble');

  if (!data.ensemble?.predictions) {
    return 'Predictions not available';
  }

  const p7 = data.ensemble.predictions.pxi_change_7d;
  const p30 = data.ensemble.predictions.pxi_change_30d;

  return `## ML Ensemble Predictions

**Current PXI:** ${data.current_score?.toFixed(1)}
**Date:** ${data.date}

### 7-Day Forecast
- **Predicted Change:** ${p7?.value?.toFixed(2) || 'N/A'}%
- **Direction:** ${p7?.direction || 'N/A'}
- **Confidence:** ${p7?.confidence || 'N/A'}
- Components: XGBoost ${p7?.components?.xgboost?.toFixed(2) || 'N/A'}% | LSTM ${p7?.components?.lstm?.toFixed(2) || 'N/A'}%

### 30-Day Forecast
- **Predicted Change:** ${p30?.value?.toFixed(2) || 'N/A'}%
- **Direction:** ${p30?.direction || 'N/A'}
- **Confidence:** ${p30?.confidence || 'N/A'}
- Components: XGBoost ${p30?.components?.xgboost?.toFixed(2) || 'N/A'}% | LSTM ${p30?.components?.lstm?.toFixed(2) || 'N/A'}%

*Models: XGBoost (${(data.ensemble.weights?.xgboost * 100) || 60}% weight) + LSTM (${(data.ensemble.weights?.lstm * 100) || 40}% weight)*`;
}

async function getSimilarPeriods(): Promise<string> {
  const [similar, pxi] = await Promise.all([
    fetchAPI('/api/similar'),
    fetchAPI('/api/pxi').catch(() => null),
  ]);

  if (!similar.similar_periods?.length) {
    return 'No similar periods found';
  }

  const periods = similar.similar_periods.slice(0, 5);

  // Calculate average forward returns from similar periods that have data
  const periodsWithReturns = similar.similar_periods.filter((p: any) =>
    p.forward_returns?.d7 !== null || p.forward_returns?.d30 !== null
  );

  let avgD7 = 'N/A';
  let avgD30 = 'N/A';
  if (periodsWithReturns.length > 0) {
    const d7Returns = periodsWithReturns.filter((p: any) => p.forward_returns?.d7 !== null);
    const d30Returns = periodsWithReturns.filter((p: any) => p.forward_returns?.d30 !== null);
    if (d7Returns.length > 0) {
      avgD7 = (d7Returns.reduce((sum: number, p: any) => sum + p.forward_returns.d7, 0) / d7Returns.length).toFixed(1);
    }
    if (d30Returns.length > 0) {
      avgD30 = (d30Returns.reduce((sum: number, p: any) => sum + p.forward_returns.d30, 0) / d30Returns.length).toFixed(1);
    }
  }

  return `## Similar Historical Periods

**Current Date:** ${similar.current_date}
**Current PXI:** ${pxi?.score?.toFixed(1) || 'N/A'} (${pxi?.label || 'Unknown'})
**Cutoff Date:** ${similar.cutoff_date} (periods before this have full forward data)

### Top ${periods.length} Similar Periods
${periods.map((p: any, i: number) =>
  `${i + 1}. **${p.date}** (similarity: ${(p.similarity * 100).toFixed(0)}%)
   - PXI: ${p.pxi?.score?.toFixed(1)} (${p.pxi?.status})
   - Forward 7d: ${p.forward_returns?.d7?.toFixed(1) || 'N/A'}%
   - Forward 30d: ${p.forward_returns?.d30?.toFixed(1) || 'N/A'}%`
).join('\n\n')}

### Historical Outlook (from similar periods)
- Avg 7-day forward return: ${avgD7}%
- Avg 30-day forward return: ${avgD30}%`;
}

async function getSignal(): Promise<string> {
  const data = await fetchAPI('/api/signal');

  return `## PXI Risk Signal

**Signal:** ${data.signal || 'N/A'}
**Risk Exposure:** ${data.exposure ? (data.exposure * 100).toFixed(0) : 'N/A'}%
**PXI Score:** ${data.pxi_score?.toFixed(1) || 'N/A'}

### Components
- PXI Component: ${data.components?.pxi || 'N/A'}
- Momentum Component: ${data.components?.momentum || 'N/A'}
- Volatility Component: ${data.components?.volatility || 'N/A'}

### Recommendation
${data.recommendation || 'No specific recommendation available'}`;
}

async function getRegime(): Promise<string> {
  const data = await fetchAPI('/api/regime');

  return `## Market Regime Analysis

**Regime:** ${data.regime || 'Unknown'}
**Description:** ${data.description || 'N/A'}

### Category Analysis
${data.categories ? Object.entries(data.categories)
  .map(([cat, info]: [string, any]) => `- **${cat}:** ${info.status} (${info.score?.toFixed(1)})`)
  .join('\n') : 'N/A'}

### Key Drivers
${data.drivers?.length ? data.drivers.map((d: string) => `- ${d}`).join('\n') : 'No specific drivers identified'}`;
}

async function getMarketContext(): Promise<string> {
  // Fetch multiple endpoints in parallel
  const [pxi, predictions, predict] = await Promise.all([
    fetchAPI('/api/pxi').catch(() => null),
    fetchAPI('/api/ml/ensemble').catch(() => null),
    fetchAPI('/api/predict').catch(() => null),
  ]);

  // Determine suggested action
  let action = 'HOLD_CURRENT_EXPOSURE';
  let reasoning = '';

  if (pxi && predictions?.ensemble?.predictions) {
    const score = pxi.score;
    const p30 = predictions.ensemble.predictions.pxi_change_30d;

    if (score < 30 && p30?.direction?.includes('UP')) {
      action = 'CONSIDER_INCREASING_EXPOSURE';
      reasoning = 'PXI extremely low but predictions suggest recovery';
    } else if (score > 70 && p30?.direction?.includes('DOWN')) {
      action = 'CONSIDER_REDUCING_EXPOSURE';
      reasoning = 'PXI elevated and predictions suggest decline';
    } else if (score < 40) {
      action = 'DEFENSIVE_POSITIONING';
      reasoning = 'PXI in bearish territory';
    } else if (score > 65) {
      action = 'RISK_ON_POSITIONING';
      reasoning = 'PXI in bullish territory';
    } else {
      action = 'NEUTRAL_POSITIONING';
      reasoning = 'PXI in neutral territory, mixed signals';
    }
  }

  // Helper to get category score from array
  const getCatScore = (cats: any[], name: string) => {
    if (!Array.isArray(cats)) return null;
    const cat = cats.find((c: any) => c.name === name);
    return cat?.score ?? null;
  };

  const keyRisks: string[] = [];
  if (pxi?.delta?.d7 && pxi.delta.d7 < -10) {
    keyRisks.push('Rapid deterioration: PXI down ' + Math.abs(pxi.delta.d7).toFixed(0) + ' points in 7 days');
  }
  const volScore = getCatScore(pxi?.categories, 'volatility');
  if (volScore !== null && volScore < 40) {
    keyRisks.push('Elevated volatility conditions');
  }
  const creditScore = getCatScore(pxi?.categories, 'credit');
  if (creditScore !== null && creditScore < 40) {
    keyRisks.push('Credit spreads widening');
  }
  if (predictions?.ensemble?.predictions?.pxi_change_7d?.confidence === 'LOW') {
    keyRisks.push('Low model confidence - high uncertainty');
  }

  return `## Market Context for Agent Decision-Making

### Current State
- **PXI Score:** ${pxi?.score?.toFixed(1) || 'N/A'} (${pxi?.label || 'Unknown'})
- **Status:** ${pxi?.status || 'Unknown'}
- **Date:** ${pxi?.date || 'Unknown'}

### ML Predictions
- **7-day:** ${predictions?.ensemble?.predictions?.pxi_change_7d?.value?.toFixed(1) || 'N/A'}% (${predictions?.ensemble?.predictions?.pxi_change_7d?.direction || 'N/A'}, ${predictions?.ensemble?.predictions?.pxi_change_7d?.confidence || 'N/A'} confidence)
- **30-day:** ${predictions?.ensemble?.predictions?.pxi_change_30d?.value?.toFixed(1) || 'N/A'}% (${predictions?.ensemble?.predictions?.pxi_change_30d?.direction || 'N/A'}, ${predictions?.ensemble?.predictions?.pxi_change_30d?.confidence || 'N/A'} confidence)

### Historical Context (${predict?.current?.bucket || 'N/A'} bucket)
- Avg 7-day forward return: ${predict?.prediction?.d7?.avg_return?.toFixed(1) || 'N/A'}%
- Avg 30-day forward return: ${predict?.prediction?.d30?.avg_return?.toFixed(1) || 'N/A'}%
- Historical win rate (30d): ${predict?.prediction?.d30?.win_rate?.toFixed(0) || 'N/A'}%

### Suggested Action
**${action}**
${reasoning}

### Key Risks
${keyRisks.length ? keyRisks.map(r => `- ${r}`).join('\n') : '- No significant risks identified'}

### Recent Changes
- 1-day change: ${pxi?.delta?.d1?.toFixed(1) || 'N/A'} points
- 7-day change: ${pxi?.delta?.d7?.toFixed(1) || 'N/A'} points
- 30-day change: ${pxi?.delta?.d30?.toFixed(1) || 'N/A'} points

---
*Data updated: ${pxi?.date || 'Unknown'} | Source: PXI /COMMAND*`;
}

async function getHistory(days: number = 30): Promise<string> {
  const limit = Math.min(Math.max(days, 1), 365);
  const data = await fetchAPI(`/api/history?limit=${limit}`);

  if (!data.history?.length) {
    return 'No historical data available';
  }

  const history = data.history.slice(0, 10); // Show last 10 for readability
  const oldest = data.history[data.history.length - 1];
  const newest = data.history[0];

  const change = newest.score - oldest.score;
  const avgScore = data.history.reduce((sum: number, h: any) => sum + h.score, 0) / data.history.length;

  return `## PXI History (Last ${data.history.length} days)

### Summary
- **Current:** ${newest.score?.toFixed(1)} (${newest.status})
- **${data.history.length} days ago:** ${oldest.score?.toFixed(1)}
- **Change:** ${change >= 0 ? '+' : ''}${change.toFixed(1)} points
- **Average:** ${avgScore.toFixed(1)}

### Recent Data
${history.map((h: any) => `- ${h.date}: ${h.score?.toFixed(1)} (${h.status})`).join('\n')}

${data.history.length > 10 ? `\n*Showing 10 of ${data.history.length} records*` : ''}`;
}

// Main server setup
async function main() {
  const server = new Server(
    {
      name: 'pxi-mcp-server',
      version: '1.0.0',
    },
    {
      capabilities: {
        tools: {},
      },
    }
  );

  // List tools handler
  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: TOOLS,
  }));

  // Call tool handler
  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;

    try {
      let result: string;

      switch (name) {
        case 'get_pxi':
          result = await getPxi();
          break;
        case 'get_predictions':
          result = await getPredictions();
          break;
        case 'get_similar_periods':
          result = await getSimilarPeriods();
          break;
        case 'get_signal':
          result = await getSignal();
          break;
        case 'get_regime':
          result = await getRegime();
          break;
        case 'get_market_context':
          result = await getMarketContext();
          break;
        case 'get_history':
          result = await getHistory((args as any)?.days);
          break;
        default:
          throw new Error(`Unknown tool: ${name}`);
      }

      return {
        content: [{ type: 'text', text: result }],
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return {
        content: [{ type: 'text', text: `Error: ${message}` }],
        isError: true,
      };
    }
  });

  // Start server
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error('PXI MCP Server running on stdio');
}

main().catch(console.error);
