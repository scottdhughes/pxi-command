export function SpecPage({ onClose, inPage = false }: { onClose: () => void; inPage?: boolean }) {
  const wrapperClass = inPage
    ? 'min-h-screen bg-black text-[#f3f3f3] px-4 sm:px-8 py-16 overflow-auto'
    : 'min-h-screen bg-black text-[#f3f3f3] px-4 sm:px-8 py-16 overflow-auto'

  return (
    <div className={wrapperClass}>
      <div className="max-w-2xl mx-auto">
        <div className="flex justify-between items-center mb-12">
          <div className="text-[10px] sm:text-[11px] font-mono tracking-[0.2em] text-[#949ba5] uppercase">
            PXI<span className="text-[#00a3ff]">/</span>spec
          </div>
          <button
            onClick={onClose}
            className="text-[10px] text-[#949ba5]/50 hover:text-[#f3f3f3] uppercase tracking-widest transition-colors"
          >
            Close
          </button>
        </div>

        <h1 className="text-2xl sm:text-3xl font-extralight mb-2 tracking-tight">
          Protocol Specification
        </h1>
        <p className="text-[11px] text-[#949ba5]/60 mb-12 uppercase tracking-widest">
          Macro Market Strength Index — Quantitative Framework v1.3
        </p>

        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Two-Layer Architecture</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            Two-layer system separating descriptive state from actionable signals.
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3 border-l-2 border-[#00a3ff]">
              <div className="text-[11px] font-medium uppercase tracking-wide mb-2">PXI-State</div>
              <p className="text-[10px] text-[#949ba5]/70 mb-2">Descriptive layer for monitoring macro conditions</p>
              <ul className="text-[9px] text-[#949ba5]/60 space-y-1">
                <li>• Composite score (0-100)</li>
                <li>• Category breakdowns</li>
                <li>• Market regime classification</li>
                <li>• Divergence alerts</li>
              </ul>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3 border-l-2 border-[#00c896]">
              <div className="text-[11px] font-medium uppercase tracking-wide mb-2">PXI-Signal</div>
              <p className="text-[10px] text-[#949ba5]/70 mb-2">Actionable layer for risk allocation</p>
              <ul className="text-[9px] text-[#949ba5]/60 space-y-1">
                <li>• Risk allocation (0-100%)</li>
                <li>• Signal type classification</li>
                <li>• Volatility percentile</li>
                <li>• Adjustment explanations</li>
              </ul>
            </div>
          </div>
        </section>

        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Definition</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            PXI (Pamp Index) is a composite indicator measuring macro market strength across seven
            dimensions. It synthesizes volatility, credit conditions, breadth, positioning, macro,
            global, and crypto signals into a single 0-100 score using 5-year rolling percentiles.
          </p>
          <div className="bg-[#0a0a0a] rounded px-4 py-3 font-mono text-[12px] text-[#f3f3f3]/80">
            PXI = Σ(Cᵢ × Wᵢ) / Σ(Wᵢ)
          </div>
          <p className="text-[10px] text-[#949ba5]/50 mt-2">
            Where Cᵢ = category score [0,100] and Wᵢ = category weight
          </p>
        </section>

        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Category Composition</h2>
          <div className="space-y-4">
            {[
              { name: 'Volatility', weight: 20, indicators: 'VIX, VIX term structure, AAII sentiment, GEX (gamma exposure)', formula: '100 - percentile(VIX, 5yr)' },
              { name: 'Credit', weight: 20, indicators: 'HY OAS, IG OAS, 2s10s curve, BBB-AAA spread', formula: '100 - percentile(HY_spread, 5yr)' },
              { name: 'Breadth', weight: 15, indicators: 'RSP/SPY ratio, sector breadth, small/mid cap strength', formula: 'percentile(breadth_composite, 5yr)' },
              { name: 'Positioning', weight: 15, indicators: 'Fed balance sheet, TGA, reverse repo, net liquidity', formula: 'percentile(net_liq, 5yr)' },
              { name: 'Macro', weight: 10, indicators: 'ISM manufacturing, jobless claims, CFNAI', formula: 'percentile(macro_composite, 5yr)' },
              { name: 'Global', weight: 10, indicators: 'DXY, copper/gold ratio, EM spreads, AUD/JPY', formula: 'percentile(global_composite, 5yr)' },
              { name: 'Crypto', weight: 10, indicators: 'BTC vs 200DMA, stablecoin mcap, BTC ETF flows, funding rates', formula: 'percentile(crypto_composite, 5yr)' },
            ].map((cat) => (
              <div key={cat.name} className="bg-[#0a0a0a]/60 rounded px-4 py-3">
                <div className="flex justify-between items-center mb-2">
                  <span className="text-[11px] font-medium uppercase tracking-wide">{cat.name}</span>
                  <span className="text-[10px] text-[#949ba5]/50">w = {cat.weight}%</span>
                </div>
                <p className="text-[10px] text-[#949ba5]/70 mb-1">{cat.indicators}</p>
                <code className="text-[9px] text-[#00a3ff]/60 font-mono">{cat.formula}</code>
              </div>
            ))}
          </div>
        </section>

        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">PXI-Signal Trading Policy</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            The signal layer applies a canonical trading policy to convert PXI state into risk allocation.
            Base allocation scales linearly from 30% (PXI=0) to 100% (PXI=100).
          </p>
          <div className="bg-[#0a0a0a] rounded px-4 py-3 font-mono text-[11px] text-[#f3f3f3]/70 space-y-1 mb-4">
            <div>Base = 0.3 + (PXI / 100) × 0.7</div>
            <div className="text-[#ff6b6b]">If Regime = RISK_OFF → allocation × 0.5</div>
            <div className="text-[#f59e0b]">If Regime = TRANSITION → allocation × 0.75</div>
            <div className="text-[#f59e0b]">If Δ7d &lt; -10 → allocation × 0.8</div>
            <div className="text-[#f59e0b]">If VIX_percentile &gt; 80 → allocation × 0.7</div>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-[10px]">
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-2 text-center">
              <div className="text-[#00c896] font-medium">FULL_RISK</div>
              <div className="text-[#949ba5]/60">≥80% alloc</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-2 text-center">
              <div className="text-[#f59e0b] font-medium">REDUCED</div>
              <div className="text-[#949ba5]/60">50-80% alloc</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-2 text-center">
              <div className="text-[#ff6b6b] font-medium">RISK_OFF</div>
              <div className="text-[#949ba5]/60">30-50% alloc</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-2 text-center">
              <div className="text-[#dc2626] font-medium">DEFENSIVE</div>
              <div className="text-[#949ba5]/60">&lt;30% alloc</div>
            </div>
          </div>
        </section>

        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Regime Detection</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            Market regime is classified using a voting system with percentile-based thresholds
            calculated over a 5-year rolling window. Each indicator votes RISK_ON, RISK_OFF, or NEUTRAL.
          </p>
          <div className="bg-[#0a0a0a] rounded px-4 py-3 font-mono text-[11px] text-[#f3f3f3]/70 space-y-1">
            <div>VIX &lt; 30th pct → RISK_ON | &gt; 70th pct → RISK_OFF</div>
            <div>HY_OAS &lt; 30th pct → RISK_ON | &gt; 70th pct → RISK_OFF</div>
            <div>Breadth &gt; 60% → RISK_ON | &lt; 40% → RISK_OFF (direct)</div>
            <div>Yield_curve &gt; 60th pct → RISK_ON | &lt; 20th pct → RISK_OFF</div>
            <div>DXY &lt; 40th pct → RISK_ON | &gt; 70th pct → RISK_OFF</div>
          </div>
          <p className="text-[10px] text-[#949ba5]/50 mt-2">
            Regime = RISK_ON if votes ≥ 3 (or 2 with 0 RISK_OFF) | RISK_OFF if votes ≥ 3 (or 2 with 0 RISK_ON) | else TRANSITION
          </p>
        </section>

        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Backtest Results (2022-2024)</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            Walk-forward backtest comparing PXI-Signal strategy against baseline strategies.
            583 trading days, returns measured on SPY.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-[10px]">
              <thead>
                <tr className="text-[#949ba5]/50 uppercase tracking-wider">
                  <th className="text-left py-2">Strategy</th>
                  <th className="text-right py-2">CAGR</th>
                  <th className="text-right py-2">Vol</th>
                  <th className="text-right py-2">Sharpe</th>
                  <th className="text-right py-2">Max DD</th>
                </tr>
              </thead>
              <tbody className="text-[#f3f3f3]/80 font-mono">
                <tr className="border-t border-[#26272b]">
                  <td className="py-2 text-[#00c896]">PXI-Signal</td>
                  <td className="text-right">12.3%</td>
                  <td className="text-right text-[#00c896]">6.2%</td>
                  <td className="text-right text-[#00c896]">2.00</td>
                  <td className="text-right text-[#00c896]">6.3%</td>
                </tr>
                <tr className="border-t border-[#26272b]">
                  <td className="py-2">200DMA</td>
                  <td className="text-right">15.3%</td>
                  <td className="text-right">12.4%</td>
                  <td className="text-right">1.24</td>
                  <td className="text-right">13.8%</td>
                </tr>
                <tr className="border-t border-[#26272b]">
                  <td className="py-2 text-[#949ba5]">Buy-and-Hold</td>
                  <td className="text-right text-[#949ba5]">17.0%</td>
                  <td className="text-right text-[#ff6b6b]">16.0%</td>
                  <td className="text-right text-[#949ba5]">1.06</td>
                  <td className="text-right text-[#ff6b6b]">17.5%</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div className="mt-4 grid grid-cols-3 gap-2 text-[10px]">
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-2 text-center">
              <div className="text-[#00c896] font-mono text-lg">+0.94</div>
              <div className="text-[#949ba5]/50">Sharpe vs B&H</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-2 text-center">
              <div className="text-[#00c896] font-mono text-lg">-9.9%</div>
              <div className="text-[#949ba5]/50">Vol reduction</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-2 text-center">
              <div className="text-[#00c896] font-mono text-lg">-11.2%</div>
              <div className="text-[#949ba5]/50">Max DD improve</div>
            </div>
          </div>
        </section>

        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Forward Return Model</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            Conditional probability distributions derived from historical data.
            Returns bucketed by PXI score at observation time.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-[10px]">
              <thead>
                <tr className="text-[#949ba5]/50 uppercase tracking-wider">
                  <th className="text-left py-2">PXI Range</th>
                  <th className="text-right py-2">n</th>
                  <th className="text-right py-2">E[R₇ᵈ]</th>
                  <th className="text-right py-2">E[R₃₀ᵈ]</th>
                  <th className="text-right py-2">P(R₃₀ᵈ &gt; 0)</th>
                </tr>
              </thead>
              <tbody className="text-[#f3f3f3]/80 font-mono">
                <tr className="border-t border-[#26272b]">
                  <td className="py-2">0–20</td>
                  <td className="text-right">44</td>
                  <td className="text-right text-[#00a3ff]">+0.59%</td>
                  <td className="text-right text-[#00a3ff]">+1.64%</td>
                  <td className="text-right">70%</td>
                </tr>
                <tr className="border-t border-[#26272b]">
                  <td className="py-2">20–40</td>
                  <td className="text-right">222</td>
                  <td className="text-right text-[#00a3ff]">+0.70%</td>
                  <td className="text-right text-[#00a3ff]">+2.66%</td>
                  <td className="text-right">75%</td>
                </tr>
                <tr className="border-t border-[#26272b]">
                  <td className="py-2">40–60</td>
                  <td className="text-right">580</td>
                  <td className="text-right text-[#00a3ff]">+0.38%</td>
                  <td className="text-right text-[#00a3ff]">+1.64%</td>
                  <td className="text-right">75%</td>
                </tr>
                <tr className="border-t border-[#26272b]">
                  <td className="py-2">60–80</td>
                  <td className="text-right">226</td>
                  <td className="text-right">+0.17%</td>
                  <td className="text-right">+0.84%</td>
                  <td className="text-right">73%</td>
                </tr>
                <tr className="border-t border-[#26272b]">
                  <td className="py-2 text-[#949ba5]">80–100</td>
                  <td className="text-right text-[#949ba5]">27</td>
                  <td className="text-right text-[#ff6b6b]">−0.23%</td>
                  <td className="text-right text-[#949ba5]">+0.13%</td>
                  <td className="text-right text-[#949ba5]">63%</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>

        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">ML Ensemble (v1.3)</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            Weighted ensemble combining two models for 7-day and 30-day PXI predictions.
            Models trained locally, deployed as JSON weights for edge inference.
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-4">
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3 border-l-2 border-[#00a3ff]">
              <div className="text-[11px] font-medium uppercase tracking-wide mb-2">XGBoost (60%)</div>
              <p className="text-[10px] text-[#949ba5]/70 mb-2">Gradient boosted trees for tabular features</p>
              <ul className="text-[9px] text-[#949ba5]/60 space-y-1">
                <li>• 36 engineered features</li>
                <li>• Momentum, dispersion, extremes</li>
                <li>• Rolling statistics (7d, 14d, 30d)</li>
                <li>• Point-in-time prediction</li>
              </ul>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3 border-l-2 border-[#f59e0b]">
              <div className="text-[11px] font-medium uppercase tracking-wide mb-2">LSTM (40%)</div>
              <p className="text-[10px] text-[#949ba5]/70 mb-2">Recurrent neural network for sequences</p>
              <ul className="text-[9px] text-[#949ba5]/60 space-y-1">
                <li>• 20-day input sequences</li>
                <li>• 12 features per timestep</li>
                <li>• 32 hidden units, single layer</li>
                <li>• Temporal pattern recognition</li>
              </ul>
            </div>
          </div>
          <div className="bg-[#0a0a0a] rounded px-4 py-3 font-mono text-[11px] text-[#f3f3f3]/70 space-y-1">
            <div>Ensemble = 0.6 × XGBoost + 0.4 × LSTM</div>
            <div>Confidence: HIGH (same direction+magnitude) | MEDIUM (same direction) | LOW (disagree)</div>
            <div>Direction: STRONG_UP | UP | FLAT | DOWN | STRONG_DOWN</div>
          </div>
          <p className="text-[10px] text-[#949ba5]/50 mt-2">
            Thresholds: |Δ| &gt; 5 = STRONG, |Δ| &gt; 2 = directional, else FLAT
          </p>

          <div className="mt-4 bg-[#0a0a0a]/60 rounded px-4 py-3 border border-[#1a1a1a]">
            <div className="text-[10px] text-[#00a3ff]/80 uppercase tracking-wider mb-2">XGBoost In-Sample Backtest</div>
            <div className="grid grid-cols-2 gap-4 text-[10px]">
              <div>
                <div className="text-[#949ba5]/60 mb-1">7-Day Predictions</div>
                <div className="space-y-0.5 text-[#949ba5]">
                  <div>Direction Accuracy: <span className="text-[#22c55e]">77%</span></div>
                  <div>R²: <span className="text-[#949ba5]/80">0.64</span></div>
                  <div>MAE: <span className="text-[#949ba5]/80">3.2 pts</span></div>
                </div>
              </div>
              <div>
                <div className="text-[#949ba5]/60 mb-1">30-Day Predictions</div>
                <div className="space-y-0.5 text-[#949ba5]">
                  <div>Direction Accuracy: <span className="text-[#22c55e]">90%</span></div>
                  <div>R²: <span className="text-[#949ba5]/80">0.88</span></div>
                  <div>MAE: <span className="text-[#949ba5]/80">4.8 pts</span></div>
                </div>
              </div>
            </div>
            <p className="text-[9px] text-[#949ba5]/40 mt-2">
              ⚠️ In-sample metrics (trained on this data). Live OOS accuracy tracked via /api/ml/accuracy
            </p>
          </div>
        </section>

        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Divergence Alerts</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            Divergence signals flag conflicts between PXI score and market regime. Alerts include
            historical metrics: frequency, median forward returns, and false positive rates.
          </p>
          <div className="space-y-2 text-[11px]">
            {[
              { name: 'Stealth Weakness', condition: 'PXI < 30 ∧ VIX < 15', desc: 'Low index despite low vol — complacency risk' },
              { name: 'Resilient Strength', condition: 'PXI > 70 ∧ VIX > 25', desc: 'High index despite elevated vol — market absorbing fear' },
              { name: 'Rapid Deterioration', condition: 'ΔPXI₇ᵈ < -15 ∧ Regime = RISK_ON', desc: 'Sharp drop while regime stable — leading indicator' },
              { name: 'Hidden Risk', condition: 'Regime = RISK_ON ∧ PXI < 40', desc: 'Regime looks fine but underlying weakness' },
            ].map((alert) => (
              <div key={alert.name} className="bg-[#0a0a0a]/60 rounded px-4 py-2">
                <div className="flex justify-between items-center">
                  <span className="font-medium">{alert.name}</span>
                  <code className="text-[9px] text-[#00a3ff]/60 font-mono">{alert.condition}</code>
                </div>
                <p className="text-[10px] text-[#949ba5]/60 mt-1">{alert.desc}</p>
              </div>
            ))}
          </div>
        </section>

        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Historical Signal Distribution</h2>
          <p className="text-[13px] text-[#949ba5] leading-relaxed mb-4">
            Distribution of signal types over the backtest period (1100 observations).
          </p>
          <div className="grid grid-cols-4 gap-2 text-[10px]">
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-3 text-center">
              <div className="text-[#ff6b6b] font-mono text-lg">38%</div>
              <div className="text-[#949ba5]/50 text-[9px]">RISK_OFF</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-3 text-center">
              <div className="text-[#f59e0b] font-mono text-lg">36%</div>
              <div className="text-[#949ba5]/50 text-[9px]">REDUCED</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-3 text-center">
              <div className="text-[#dc2626] font-mono text-lg">18%</div>
              <div className="text-[#949ba5]/50 text-[9px]">DEFENSIVE</div>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-3 py-3 text-center">
              <div className="text-[#00c896] font-mono text-lg">8%</div>
              <div className="text-[#949ba5]/50 text-[9px]">FULL_RISK</div>
            </div>
          </div>
        </section>

        <section className="mb-12">
          <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">Interpretation Guide</h2>
          <div className="grid grid-cols-2 gap-4 text-[11px]">
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3">
              <div className="text-[#00a3ff] font-medium mb-1">0–40: Weak</div>
              <p className="text-[10px] text-[#949ba5]/70">Risk-off conditions. Historically favorable for forward returns (mean reversion).</p>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3">
              <div className="text-[#949ba5] font-medium mb-1">40–60: Neutral</div>
              <p className="text-[10px] text-[#949ba5]/70">Typical conditions. No strong directional bias.</p>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3">
              <div className="text-[#f59e0b] font-medium mb-1">60–80: Strong</div>
              <p className="text-[10px] text-[#949ba5]/70">Risk-on conditions. Watch for overextension.</p>
            </div>
            <div className="bg-[#0a0a0a]/60 rounded px-4 py-3">
              <div className="text-[#ff6b6b] font-medium mb-1">80–100: Extended</div>
              <p className="text-[10px] text-[#949ba5]/70">Historically poor forward returns. Elevated reversal risk.</p>
            </div>
          </div>
        </section>

        <div className="text-[9px] text-[#949ba5]/30 font-mono tracking-wider text-center uppercase pt-8 border-t border-[#26272b]">
          PXI/COMMAND Protocol v1.3
        </div>
      </div>
    </div>
  )
}
