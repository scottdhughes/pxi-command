# PXI MCP Server

MCP (Model Context Protocol) server for the PXI macro market index. Enables AI agents to query real-time market conditions and make data-driven investment decisions.

## Installation

### For Claude Desktop

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "pxi": {
      "command": "npx",
      "args": ["-y", "pxi-mcp-server"]
    }
  }
}
```

### For Claude Code

Add to your Claude Code MCP settings:

```json
{
  "mcpServers": {
    "pxi": {
      "command": "npx",
      "args": ["-y", "pxi-mcp-server"]
    }
  }
}
```

### Local Development

```bash
cd mcp-server
npm install
npm run build
npm start
```

## Available Tools

| Tool | Description |
|------|-------------|
| `get_pxi` | Current PXI score (0-100) and market regime |
| `get_predictions` | ML ensemble predictions for 7d/30d |
| `get_similar_periods` | Find historically similar market periods |
| `get_signal` | Risk allocation signal (0-100% exposure) |
| `get_regime` | Detailed market regime analysis |
| `get_market_context` | Comprehensive context for agent decisions |
| `get_history` | Historical PXI scores for trend analysis |

## Tool Details

### get_pxi
Returns the current PXI score with category breakdown:
- Score: 0-100 composite index
- Status: MAX PAMP (80+), PAMPING (65-79), NEUTRAL (45-64), SOFT (30-44), DUMPING (<30)
- Categories: credit, liquidity, volatility, breadth, positioning, macro, global

### get_predictions
ML ensemble combining XGBoost (60%) and LSTM (40%):
- 7-day and 30-day forecasts
- Direction: STRONG_UP, UP, FLAT, DOWN, STRONG_DOWN
- Confidence: HIGH (models agree), MEDIUM, LOW (models disagree)

### get_market_context
Recommended for agent decision-making. Single payload with:
- Current state and regime
- ML predictions with confidence
- Suggested action (RISK_ON, NEUTRAL, DEFENSIVE, etc.)
- Key risks and recent changes

## Example Usage

```
Agent: "What are current market conditions?"

[Uses get_pxi tool]

Response: "PXI is currently at 50.6 (NEUTRAL). The market regime is
mixed with credit spreads widening slightly but volatility contained.
Key categories: Credit 45.2, Liquidity 58.1, Volatility 52.3..."
```

```
Agent: "Should I increase equity exposure?"

[Uses get_market_context tool]

Response: "Based on PXI analysis:
- Current score: 50.6 (NEUTRAL)
- 30-day prediction: +8.4% (STRONG_UP, MEDIUM confidence)
- Suggested action: NEUTRAL_POSITIONING
- Key risk: Rapid deterioration - down 15 points in 7 days

Recommendation: Hold current exposure. While predictions are bullish,
the recent rapid decline suggests waiting for stabilization."
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PXI_API_URL` | `https://pxi-api.novoamorx1.workers.dev` | PXI API endpoint |

## Data Sources

PXI aggregates 28 indicators across 7 categories:
- **Credit**: HY spreads, IG spreads, yield curve
- **Liquidity**: Fed balance sheet, TGA, reverse repo
- **Volatility**: VIX, VIX term structure
- **Breadth**: RSP/SPY ratio, advance-decline
- **Positioning**: Put/call ratio, fear & greed
- **Macro**: 10Y yield, dollar index, oil
- **Global**: Copper/gold ratio, BTC, stablecoin flows

## License

MIT
