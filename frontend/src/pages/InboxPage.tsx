import type { MarketFeedAlert } from '../lib/types'

function severityClass(severity: MarketFeedAlert['severity']) {
  if (severity === 'critical') return 'text-[#ff6b6b] border-[#ff6b6b]/40'
  if (severity === 'warning') return 'text-[#f59e0b] border-[#f59e0b]/40'
  return 'text-[#00a3ff] border-[#00a3ff]/40'
}

export function InboxPage({
  alerts,
  onBack,
  onOpenSubscribe,
  notice,
}: {
  alerts: MarketFeedAlert[]
  onBack: () => void
  onOpenSubscribe: () => void
  notice: string | null
}) {
  return (
    <div className="min-h-screen bg-black text-[#f3f3f3] px-4 sm:px-8 py-10">
      <div className="max-w-4xl mx-auto">
        <div className="flex items-center justify-between mb-8">
          <h1 className="text-[11px] uppercase tracking-[0.3em] text-[#949ba5]">PXI /inbox</h1>
          <button
            onClick={onBack}
            className="text-[10px] uppercase tracking-[0.2em] border border-[#26272b] px-3 py-1.5 rounded text-[#949ba5] hover:text-[#f3f3f3]"
          >
            home
          </button>
        </div>

        {notice && (
          <div className="mb-4 p-3 border border-[#1f3e56] bg-[#091825] rounded text-[11px] text-[#9ec5e2]">
            {notice}
          </div>
        )}

        <div className="mb-4 p-4 bg-[#0a0a0a]/60 border border-[#26272b] rounded-lg flex items-center justify-between gap-3">
          <div>
            <div className="text-[10px] uppercase tracking-wider text-[#949ba5]/60">Email Digest</div>
            <div className="text-[12px] text-[#d7dbe1]">Subscribe for the daily 8:00 AM ET market digest.</div>
          </div>
          <button
            onClick={onOpenSubscribe}
            className="px-3 py-2 border border-[#00a3ff] text-[#00a3ff] rounded text-[10px] uppercase tracking-[0.2em]"
          >
            subscribe
          </button>
        </div>

        {alerts.length === 0 ? (
          <div className="text-[#949ba5]">No alerts recorded yet.</div>
        ) : (
          <div className="space-y-3">
            {alerts.map((alert) => (
              <div key={alert.id} className="p-4 bg-[#0a0a0a]/65 border border-[#26272b] rounded-lg">
                <div className="flex items-start justify-between gap-2">
                  <div>
                    <div className="text-[13px] text-[#f3f3f3]">{alert.title}</div>
                    <div className="text-[10px] text-[#949ba5]/60 uppercase tracking-wider">
                      {alert.event_type.replace('_', ' ')} · {new Date(alert.created_at).toLocaleString()}
                    </div>
                  </div>
                  <span className={`px-2 py-1 border rounded text-[8px] uppercase tracking-wider ${severityClass(alert.severity)}`}>
                    {alert.severity}
                  </span>
                </div>
                <p className="mt-2 text-[11px] text-[#cfd5de]">{alert.body}</p>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
