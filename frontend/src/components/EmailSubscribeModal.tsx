import { useState } from 'react'

import { fetchApi } from '../lib/api'
import type { MarketFeedAlert } from '../lib/types'

export function EmailSubscribeModal({
  onClose,
  onSuccess,
}: {
  onClose: () => void
  onSuccess: (message: string) => void
}) {
  const [email, setEmail] = useState('')
  const [types, setTypes] = useState<Array<MarketFeedAlert['event_type']>>([
    'regime_change',
    'threshold_cross',
    'opportunity_spike',
    'freshness_warning',
  ])
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const toggleType = (type: MarketFeedAlert['event_type']) => {
    setTypes((prev) => {
      if (prev.includes(type)) return prev.filter((item) => item !== type)
      return [...prev, type]
    })
  }

  const submit = async () => {
    setSubmitting(true)
    setError(null)
    try {
      const res = await fetchApi('/api/alerts/subscribe/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email,
          types,
          cadence: 'daily_8am_et',
        }),
      })

      const payload = await res.json().catch(() => ({} as { error?: string }))
      if (!res.ok) {
        throw new Error(payload.error || 'Subscription failed')
      }

      onSuccess('Verification email sent. Use the link in your inbox to activate.')
      onClose()
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Subscription failed'
      setError(message)
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="fixed inset-0 z-[80] bg-black/80 backdrop-blur-sm flex items-center justify-center px-4">
      <div className="w-full max-w-md p-5 bg-[#090909] border border-[#26272b] rounded-lg">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-[11px] uppercase tracking-[0.25em] text-[#949ba5]">Email Alerts</h2>
          <button onClick={onClose} className="text-[10px] uppercase tracking-[0.2em] text-[#949ba5] hover:text-[#f3f3f3]">
            close
          </button>
        </div>

        <label className="block text-[9px] uppercase tracking-wider text-[#949ba5]/60 mb-1">Email</label>
        <input
          type="email"
          value={email}
          onChange={(event) => setEmail(event.target.value)}
          className="w-full bg-[#111] border border-[#26272b] rounded px-3 py-2 text-[12px] text-[#f3f3f3] outline-none focus:border-[#00a3ff]"
          placeholder="you@example.com"
        />

        <div className="mt-4">
          <div className="text-[9px] uppercase tracking-wider text-[#949ba5]/60 mb-2">Event types</div>
          <div className="grid grid-cols-2 gap-2">
            {(['regime_change', 'threshold_cross', 'opportunity_spike', 'freshness_warning'] as const).map((type) => (
              <button
                key={type}
                onClick={() => toggleType(type)}
                className={`px-2 py-1.5 text-[9px] uppercase tracking-wider border rounded ${
                  types.includes(type)
                    ? 'border-[#00a3ff] text-[#00a3ff]'
                    : 'border-[#26272b] text-[#949ba5]'
                }`}
              >
                {type.replace('_', ' ')}
              </button>
            ))}
          </div>
        </div>

        {error && <p className="mt-3 text-[10px] text-[#ff6b6b]">{error}</p>}

        <div className="mt-4 flex justify-end">
          <button
            onClick={submit}
            disabled={submitting || !email || types.length === 0}
            className="px-3 py-2 rounded border border-[#00a3ff] text-[#00a3ff] text-[10px] uppercase tracking-[0.2em] disabled:opacity-40"
          >
            {submitting ? 'sending...' : 'send verify link'}
          </button>
        </div>
      </div>
    </div>
  )
}
