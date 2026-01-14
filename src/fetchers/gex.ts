// GEX (Gamma Exposure) Fetcher
// Calculates SPX dealer gamma exposure from CBOE options data
// Positive GEX = dealers dampen volatility (buy dips, sell rips)
// Negative GEX = dealers amplify volatility (sell dips, buy rips)

import axios from 'axios';
import yahooFinance from 'yahoo-finance2';

interface ParsedOption {
  strike: number;
  expiration: string;
  gamma: number;
  openInterest: number;
  type: 'call' | 'put';
}

interface CBOEOptionQuote {
  option: string; // e.g., "SPX260116C00200000"
  gamma: number;
  open_interest: number;
  delta?: number;
  iv?: number;
  bid?: number;
  ask?: number;
}

interface CBOEResponse {
  timestamp: string;
  data: {
    options: CBOEOptionQuote[];
  };
}

// Parse CBOE option symbol: SPX{YYMMDD}{C/P}{STRIKE*1000}
function parseOptionSymbol(symbol: string): { expiration: string; type: 'call' | 'put'; strike: number } | null {
  // Example: SPX260116C00200000 = SPX Jan 16 2026 Call @ $200
  const match = symbol.match(/^SPX(\d{6})(C|P)(\d+)$/);
  if (!match) return null;

  const [, dateStr, typeChar, strikeStr] = match;
  const year = parseInt('20' + dateStr.slice(0, 2));
  const month = parseInt(dateStr.slice(2, 4));
  const day = parseInt(dateStr.slice(4, 6));

  return {
    expiration: `${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`,
    type: typeChar === 'C' ? 'call' : 'put',
    strike: parseInt(strikeStr) / 1000, // Convert from cents to dollars
  };
}

// Fetch SPX options chain from CBOE (underscore prefix required for public access)
async function fetchCBOEOptionsChain(): Promise<ParsedOption[]> {
  const response = await axios.get<CBOEResponse>(
    'https://cdn.cboe.com/api/global/delayed_quotes/options/_SPX.json',
    {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
      },
      timeout: 30000,
    }
  );

  const options: ParsedOption[] = [];
  const rawOptions = response.data?.data?.options || [];

  for (const opt of rawOptions) {
    const parsed = parseOptionSymbol(opt.option);
    if (!parsed) continue;

    // Skip options with zero gamma or zero OI
    if (opt.gamma === 0 || opt.open_interest === 0) continue;

    options.push({
      strike: parsed.strike,
      expiration: parsed.expiration,
      gamma: opt.gamma,
      openInterest: opt.open_interest,
      type: parsed.type,
    });
  }

  return options;
}

// Get current SPX spot price from Yahoo Finance
async function getSPXSpot(): Promise<number> {
  const quote = await yahooFinance.quote('^SPX');
  const price = quote.regularMarketPrice;
  if (!price) {
    throw new Error('Could not get SPX spot price');
  }
  return price;
}

// Calculate total GEX in $ billions
// Formula: GEX = Gamma × OI × ContractMultiplier × Spot² × 0.01
function calculateGEX(options: ParsedOption[], spotPrice: number): number {
  let totalGEX = 0;
  const contractMultiplier = 100; // SPX options are 100x

  for (const opt of options) {
    // GEX contribution for this option
    const gex =
      opt.gamma *
      opt.openInterest *
      contractMultiplier *
      spotPrice *
      spotPrice *
      0.01;

    // Dealers are typically:
    // - Long gamma on calls (customers buy calls, dealers sell them, then hedge)
    // - Short gamma on puts (customers buy puts, dealers sell them, then hedge)
    // So: call GEX is positive, put GEX is negative
    totalGEX += opt.type === 'call' ? gex : -gex;
  }

  // Convert to billions for readability
  return totalGEX / 1e9;
}

// Main export: fetch and calculate GEX
export async function fetchGEX(): Promise<number | null> {
  try {
    console.log('  Fetching SPX options chain from CBOE...');
    const [spotPrice, options] = await Promise.all([
      getSPXSpot(),
      fetchCBOEOptionsChain(),
    ]);

    console.log(`  SPX spot: $${spotPrice.toFixed(2)}, Options: ${options.length}`);

    if (options.length === 0) {
      console.warn('  No valid options data found');
      return null;
    }

    const gex = calculateGEX(options, spotPrice);
    return gex;
  } catch (err: any) {
    console.error('GEX fetch error:', err.message);
    return null;
  }
}

// CLI test
if (import.meta.url === `file://${process.argv[1]}`) {
  fetchGEX().then((gex) => {
    if (gex !== null) {
      console.log(`\nGEX: ${gex.toFixed(2)}B`);
      console.log(
        gex > 5
          ? 'High positive GEX → Low vol regime'
          : gex > 0
            ? 'Positive GEX → Stable'
            : gex > -5
              ? 'Neutral/Negative GEX → Normal vol'
              : 'Deep negative GEX → High vol risk'
      );
    } else {
      console.log('Failed to fetch GEX');
    }
  });
}
