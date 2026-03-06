import { useState } from 'react'

export function OnboardingModal({
  onClose,
  inPage = false,
  exampleScore,
}: {
  onClose: () => void
  inPage?: boolean
  exampleScore?: number
}) {
  const [step, setStep] = useState(0)
  const hasLiveScore = typeof exampleScore === 'number' && Number.isFinite(exampleScore)
  const displayScore = hasLiveScore ? Math.round(exampleScore) : 53
  const scoreLabel = hasLiveScore ? 'Live Score' : 'Example Score'

  const steps = [
    {
      title: 'Welcome to PXI',
      content: (
        <div className="space-y-4">
          <p className="text-[13px] text-[#949ba5] leading-relaxed">
            PXI (Pamp Index) is a composite indicator measuring <span className="text-[#f3f3f3]">macro market strength</span> across seven dimensions.
          </p>
          <div className="bg-[#0a0a0a] rounded-lg p-4 border border-[#26272b]">
            <div className="text-center">
              <div className="text-6xl font-extralight text-[#f3f3f3] mb-2">{displayScore}</div>
              <div className="text-[10px] text-[#949ba5]/60 uppercase tracking-widest">{scoreLabel}</div>
            </div>
          </div>
          <p className="text-[11px] text-[#949ba5]/70">
            The score ranges from 0-100, synthesizing volatility, credit, breadth, positioning, macro, global, and crypto signals.
          </p>
        </div>
      ),
    },
    {
      title: 'Score Interpretation',
      content: (
        <div className="space-y-3">
          <p className="text-[12px] text-[#949ba5] mb-4">
            Higher scores indicate stronger risk-on conditions. Lower scores suggest caution.
          </p>
          <div className="space-y-2">
            <div className="flex items-center gap-3 bg-[#0a0a0a] rounded px-3 py-2 border-l-2 border-[#ff6b6b]">
              <div className="w-16 text-[11px] font-mono text-[#ff6b6b]">0–30</div>
              <div className="flex-1">
                <div className="text-[11px] text-[#f3f3f3]">Weak / Dumping</div>
                <div className="text-[9px] text-[#949ba5]/60">Historically favorable for forward returns (mean reversion)</div>
              </div>
            </div>
            <div className="flex items-center gap-3 bg-[#0a0a0a] rounded px-3 py-2 border-l-2 border-[#949ba5]">
              <div className="w-16 text-[11px] font-mono text-[#949ba5]">30–60</div>
              <div className="flex-1">
                <div className="text-[11px] text-[#f3f3f3]">Neutral / Soft</div>
                <div className="text-[9px] text-[#949ba5]/60">Typical market conditions, no strong directional bias</div>
              </div>
            </div>
            <div className="flex items-center gap-3 bg-[#0a0a0a] rounded px-3 py-2 border-l-2 border-[#00a3ff]">
              <div className="w-16 text-[11px] font-mono text-[#00a3ff]">60–80</div>
              <div className="flex-1">
                <div className="text-[11px] text-[#f3f3f3]">Strong / Pamping</div>
                <div className="text-[9px] text-[#949ba5]/60">Risk-on conditions, favorable environment</div>
              </div>
            </div>
            <div className="flex items-center gap-3 bg-[#0a0a0a] rounded px-3 py-2 border-l-2 border-[#f59e0b]">
              <div className="w-16 text-[11px] font-mono text-[#f59e0b]">80–100</div>
              <div className="flex-1">
                <div className="text-[11px] text-[#f3f3f3]">Extended / Max Pamp</div>
                <div className="text-[9px] text-[#949ba5]/60">Historically poor forward returns, elevated reversal risk</div>
              </div>
            </div>
          </div>
        </div>
      ),
    },
    {
      title: 'Market Regimes',
      content: (
        <div className="space-y-4">
          <p className="text-[12px] text-[#949ba5]">
            Market regime is classified using a voting system across key indicators.
          </p>
          <div className="space-y-2">
            <div className="flex items-center gap-3 bg-[#00a3ff]/10 border border-[#00a3ff]/30 rounded px-3 py-2">
              <span className="text-[#00a3ff] text-lg">↗</span>
              <div>
                <div className="text-[11px] text-[#00a3ff] uppercase tracking-wide">Risk On</div>
                <div className="text-[9px] text-[#949ba5]/60">Favorable for equities, credit tight, volatility low</div>
              </div>
            </div>
            <div className="flex items-center gap-3 bg-[#f59e0b]/10 border border-[#f59e0b]/30 rounded px-3 py-2">
              <span className="text-[#f59e0b] text-lg">↔</span>
              <div>
                <div className="text-[11px] text-[#f59e0b] uppercase tracking-wide">Transition</div>
                <div className="text-[9px] text-[#949ba5]/60">Mixed signals, regime unclear or shifting</div>
              </div>
            </div>
            <div className="flex items-center gap-3 bg-[#ff6b6b]/10 border border-[#ff6b6b]/30 rounded px-3 py-2">
              <span className="text-[#ff6b6b] text-lg">↘</span>
              <div>
                <div className="text-[11px] text-[#ff6b6b] uppercase tracking-wide">Risk Off</div>
                <div className="text-[9px] text-[#949ba5]/60">Defensive positioning recommended, stress signals</div>
              </div>
            </div>
          </div>
        </div>
      ),
    },
    {
      title: 'Categories & Signals',
      content: (
        <div className="space-y-4">
          <p className="text-[12px] text-[#949ba5]">
            PXI aggregates 7 category scores, each with specific weights:
          </p>
          <div className="grid grid-cols-2 gap-2 text-[10px]">
            {[
              { name: 'Credit', weight: '20%', desc: 'HY/IG spreads, curve' },
              { name: 'Volatility', weight: '20%', desc: 'VIX, term structure' },
              { name: 'Breadth', weight: '15%', desc: 'RSP/SPY, sectors' },
              { name: 'Positioning', weight: '15%', desc: 'Fed, TGA, RRP' },
              { name: 'Macro', weight: '10%', desc: 'ISM, claims' },
              { name: 'Global', weight: '10%', desc: 'DXY, EM spreads' },
              { name: 'Crypto', weight: '10%', desc: 'BTC, stables' },
            ].map((cat) => (
              <div key={cat.name} className="bg-[#0a0a0a] rounded px-2 py-1.5">
                <div className="flex justify-between">
                  <span className="text-[#f3f3f3]">{cat.name}</span>
                  <span className="text-[#00a3ff]">{cat.weight}</span>
                </div>
                <div className="text-[8px] text-[#949ba5]/50">{cat.desc}</div>
              </div>
            ))}
          </div>
          <p className="text-[9px] text-[#949ba5]/50 text-center">
            See /spec for full methodology and backtest results
          </p>
        </div>
      ),
    },
  ]

  const header = inPage ? (
    <div className="sticky top-0 bg-black/95 backdrop-blur border-b border-[#26272b]">
      <div className="max-w-2xl mx-auto px-4 py-4 flex justify-between items-center">
        <h1 className="text-[10px] sm:text-[11px] text-[#949ba5] font-mono uppercase tracking-[0.3em]">PXI/guide</h1>
        <button onClick={onClose} className="text-[#949ba5] text-[10px] uppercase tracking-widest">
          Exit
        </button>
      </div>
    </div>
  ) : null

  return (
    <div className={inPage ? 'min-h-screen bg-black text-[#f3f3f3] overflow-y-auto' : 'fixed inset-0 z-[100] flex items-center justify-center p-4'}>
      {!inPage ? (
        <>
          <div
            className="absolute inset-0 bg-black/90 backdrop-blur-sm"
            onClick={onClose}
          />

          <div className="relative bg-[#0a0a0a] border border-[#26272b] rounded-lg max-w-md w-full overflow-hidden shadow-2xl">
            {header}
            <div className="px-6 py-4">
              <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-3">
                {steps[step].title}
              </h2>
              <div className="min-h-[280px]">{steps[step].content}</div>
            </div>

            <div className="flex justify-between items-center px-6 py-4 border-t border-[#26272b]">
              <button
                onClick={() => step > 0 && setStep(step - 1)}
                className={`text-[10px] uppercase tracking-widest transition-colors ${
                  step > 0 ? 'text-[#949ba5] hover:text-[#f3f3f3]' : 'text-transparent pointer-events-none'
                }`}
              >
                Back
              </button>
              {step < steps.length - 1 ? (
                <button
                  onClick={() => setStep(step + 1)}
                  className="text-[10px] uppercase tracking-widest text-[#00a3ff] hover:text-[#f3f3f3] transition-colors"
                >
                  Next
                </button>
              ) : (
                <button
                  onClick={onClose}
                  className="text-[10px] uppercase tracking-widest bg-[#00a3ff] text-black px-4 py-1.5 rounded hover:bg-[#00a3ff]/80 transition-colors"
                >
                  Get Started
                </button>
              )}
            </div>
          </div>
        </>
      ) : (
        <div className="min-h-screen bg-black/95">
          {header}
          <div className="max-w-2xl mx-auto px-4 py-8 sm:py-12">
            <h2 className="text-[10px] text-[#00a3ff] uppercase tracking-widest mb-4">
              {steps[step].title}
            </h2>
            <div className="min-h-[300px]">{steps[step].content}</div>

            <div className="mt-8 flex justify-between items-center border-t border-[#26272b] pt-4">
              <button
                onClick={() => step > 0 && setStep(step - 1)}
                className={`text-[10px] uppercase tracking-widest transition-colors ${
                  step > 0 ? 'text-[#949ba5] hover:text-[#f3f3f3]' : 'text-transparent pointer-events-none'
                }`}
              >
                Back
              </button>
              {step < steps.length - 1 ? (
                <button
                  onClick={() => setStep(step + 1)}
                  className="text-[10px] uppercase tracking-widest text-[#00a3ff] hover:text-[#f3f3f3] transition-colors"
                >
                  Next
                </button>
              ) : (
                <button
                  onClick={onClose}
                  className="text-[10px] uppercase tracking-widest bg-[#00a3ff] text-black px-4 py-1.5 rounded hover:bg-[#00a3ff]/80 transition-colors"
                >
                  Exit
                </button>
              )}
            </div>
          </div>
        </div>
      )}
      <div className={inPage ? 'max-w-2xl mx-auto px-4 pb-4 flex justify-center gap-2' : 'absolute top-6 left-1/2 -translate-x-1/2 flex justify-center gap-2 pt-4'}>
        {steps.map((_, i) => (
          <button
            key={i}
            onClick={() => setStep(i)}
            className={`w-1.5 h-1.5 rounded-full transition-all ${
              i === step ? 'bg-[#00a3ff] w-4' : 'bg-[#26272b] hover:bg-[#949ba5]/30'
            }`}
          />
        ))}
      </div>
    </div>
  )
}
